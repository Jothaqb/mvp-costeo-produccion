import asyncio
import base64
import hashlib
import hmac
import json
import os
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest import mock

from fastapi import HTTPException
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from starlette.requests import Request

from app import database as database_module
from app.database import Base
from app.models import (
    AuditLog,
    B2CCustomer,
    B2CSalesOrder,
    Channel,
    InventoryTransaction,
    Product,
    ShopifyCustomerMapping,
    ShopifyOrderIntegration,
    ShopifyWebhookEvent,
)
from app.services.shopify_phase1a_service import register_shopify_webhook_event_in_session
from app.services.shopify_phase1b_service import (
    ShopifyAccessTokenProvider,
    ShopifyConfigurationError,
    ShopifyFinancialSnapshot,
    ShopifyGraphQLClient,
    ShopifyPhase1BError,
    ShopifyRuntimeConfig,
    ShopifyTransientError,
    ShopifyWebhookSecurityError,
    build_shopify_retry_csrf_token,
    claim_next_shopify_event,
    detect_missing_shopify_orders,
    mark_shopify_event_retry,
    load_shopify_runtime_config,
    parse_shipping_method_map,
    process_claimed_shopify_event_in_session,
    queue_shopify_event_retry_in_session,
    run_shopify_worker_cycle,
    validate_shopify_retry_csrf_token,
    validate_shopify_webhook_request,
    verify_shopify_webhook_hmac,
)


class ShopifyPhase1BTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)
        with self.Session.begin() as db:
            db.add(Channel(name="Online Web", active=True, applies_to_b2c=True))
            db.add_all(
                (
                    Product(
                        sku="P-100",
                        name="Producto físico",
                        active=True,
                        available_for_sale_gc=True,
                        b2c_price=Decimal("1000"),
                    ),
                    Product(
                        sku="10219",
                        name="Envío GAM",
                        active=True,
                        available_for_sale_gc=True,
                        b2c_price=Decimal("2500"),
                    ),
                    Product(
                        sku="10258",
                        name="Envío fuera GAM",
                        active=True,
                        available_for_sale_gc=True,
                        b2c_price=Decimal("3500"),
                    ),
                )
            )
        self.config = ShopifyRuntimeConfig(
            shop_domain="green-corner.myshopify.com",
            webhook_secret="test-secret",
            api_version="2026-10",
            shipping_method_map={
                "shopify|gam": "10219",
                "shopify|outside-gam": "10258",
                "shopify|pickup": "pickup",
            },
            max_webhook_bytes=2 * 1024 * 1024,
        )

    def tearDown(self) -> None:
        Base.metadata.drop_all(self.engine)
        self.engine.dispose()

    def _payload(
        self,
        *,
        order_id: int = 1001,
        customer_gid: str | None = "gid://shopify/Customer/501",
        phone: str | None = "8888-7777",
        email: str | None = "cliente@example.com",
        sku: str = "P-100",
        shipping_code: str = "gam",
        shipping_price: str = "2500",
        shipping_discount: str = "500",
        product_discount: str = "200",
        total: str = "3800",
        currency: str = "CRC",
        tax: str = "0",
        include_shipping: bool = True,
        requires_shipping: bool = True,
    ) -> dict:
        customer = {
            "id": 501,
            "admin_graphql_api_id": customer_gid,
            "first_name": "Ana",
            "last_name": "Cliente",
            "phone": phone,
            "email": email,
        }
        shipping_lines = []
        if include_shipping:
            shipping_lines.append(
                {
                    "id": 801,
                    "title": "Entrega",
                    "source": "shopify",
                    "code": shipping_code,
                    "price": shipping_price,
                    "discount_allocations": [{"amount": shipping_discount}],
                }
            )
        return {
            "id": order_id,
            "admin_graphql_api_id": f"gid://shopify/Order/{order_id}",
            "name": f"#{order_id}",
            "created_at": "2026-10-25T18:00:00-06:00",
            "updated_at": "2026-10-25T18:05:00-06:00",
            "currency": currency,
            "current_total_tax": tax,
            "current_total_price": total,
            "current_total_discounts": str(Decimal(product_discount) + Decimal(shipping_discount)),
            "financial_status": "paid",
            "fulfillment_status": None,
            "total_received": total,
            "total_outstanding": "0",
            "total_refunded": "0",
            "phone": phone,
            "email": email,
            "customer": customer,
            "shipping_address": {
                "name": "Ana Cliente",
                "phone": phone,
                "address1": "Calle 1",
                "city": "San José",
                "province": "San José",
                "zip": "10101",
                "country": "Costa Rica",
            },
            "line_items": [
                {
                    "id": 701,
                    "admin_graphql_api_id": "gid://shopify/LineItem/701",
                    "variant_id": 601,
                    "sku": sku,
                    "name": "Producto físico",
                    "quantity": 2,
                    "price": "1000",
                    "requires_shipping": requires_shipping,
                    "discount_allocations": [{"amount": product_discount}],
                }
            ],
            "shipping_lines": shipping_lines,
        }

    @staticmethod
    def _body(payload: dict) -> bytes:
        return json.dumps(payload, separators=(",", ":")).encode("utf-8")

    @staticmethod
    def _main_attribute(name: str):
        with mock.patch("zoneinfo.ZoneInfo", return_value=timezone(timedelta(hours=-6))):
            import app.main as main_module

        return getattr(main_module, name)

    @classmethod
    def _webhook_route(cls):
        return cls._main_attribute("receive_shopify_orders_create_webhook")

    def _register(self, payload: dict, *, webhook_id: str = "wh-1") -> int:
        with self.Session() as db:
            event, _ = register_shopify_webhook_event_in_session(
                db,
                shop_domain=self.config.shop_domain,
                webhook_id=webhook_id,
                topic="orders/create",
                raw_body=self._body(payload),
                api_version="2026-10",
            )
            db.commit()
            return event.id

    def _claim_and_process(self, event_id: int):
        with self.Session() as db:
            claimed = claim_next_shopify_event(db)
            self.assertEqual(claimed, event_id)
            db.commit()
        with self.Session() as db:
            result = process_claimed_shopify_event_in_session(
                db,
                event_id=event_id,
                config=self.config,
            )
            db.commit()
            return result

    def _financial_client(self, *, order_id: int = 1001) -> mock.Mock:
        client = mock.Mock(spec=ShopifyGraphQLClient)
        client.shop_domain = self.config.shop_domain
        client.get_order_financial_snapshot.return_value = ShopifyFinancialSnapshot(
            order_gid=f"gid://shopify/Order/{order_id}",
            financial_status="PAID",
            currency_code="CRC",
            total_amount=Decimal("3800.0000"),
            tax_amount=Decimal("0.0000"),
            total_received_amount=Decimal("3800.0000"),
            outstanding_amount=Decimal("0.0000"),
            refunded_amount=Decimal("0.0000"),
            updated_at=datetime(2026, 10, 26),
        )
        return client

    def _headers(self, body: bytes, **overrides: str) -> dict[str, str]:
        signature = base64.b64encode(
            hmac.new(self.config.webhook_secret.encode(), body, hashlib.sha256).digest()
        ).decode()
        headers = {
            "x-shopify-shop-domain": self.config.shop_domain,
            "x-shopify-topic": "orders/create",
            "x-shopify-webhook-id": "wh-1",
            "x-shopify-hmac-sha256": signature,
            "x-shopify-api-version": "2026-10",
            "x-shopify-triggered-at": "2026-10-26T00:00:00Z",
        }
        headers.update(overrides)
        return headers

    def test_shipping_map_requires_explicit_exact_source_and_code(self) -> None:
        mapping = parse_shipping_method_map(
            '{"shopify|gam": "10219", "shopify|outside-gam": "10258", "shopify|pickup": "pickup"}'
        )
        self.assertEqual(mapping["shopify|gam"], "10219")
        with self.assertRaises(Exception):
            parse_shipping_method_map('{"gam": "10219"}')

    def test_webhook_secret_must_match_client_secret_for_the_same_app(self) -> None:
        base_env = {
            "SHOPIFY_SHOP_DOMAIN": self.config.shop_domain,
            "SHOPIFY_CLIENT_SECRET": "the-app-client-secret",
        }
        with mock.patch.dict(os.environ, base_env, clear=True):
            config = load_shopify_runtime_config()
        self.assertEqual(config.webhook_secret, "the-app-client-secret")

        with mock.patch.dict(
            os.environ,
            {**base_env, "SHOPIFY_WEBHOOK_SECRET": "different-secret"},
            clear=True,
        ):
            with self.assertRaises(ShopifyConfigurationError):
                load_shopify_runtime_config()

    def test_hmac_shop_topic_and_body_are_validated_before_persistence(self) -> None:
        body = self._body(self._payload())
        self.assertTrue(verify_shopify_webhook_hmac(body, self._headers(body)["x-shopify-hmac-sha256"], "test-secret"))
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ShopifyWebhookSecurityError):
                validate_shopify_webhook_request(
                    raw_body=body,
                    headers=self._headers(body),
                    config=self.config,
                )
        with mock.patch.dict(os.environ, {"SHOPIFY_INTEGRATION_ENABLED": "true"}, clear=False):
            result = validate_shopify_webhook_request(
                raw_body=body,
                headers=self._headers(body),
                config=self.config,
            )
            self.assertEqual(result[0], self.config.shop_domain)
            with self.assertRaises(ShopifyWebhookSecurityError):
                validate_shopify_webhook_request(
                    raw_body=body,
                    headers=self._headers(body, **{"x-shopify-hmac-sha256": "bad"}),
                    config=self.config,
                )
            with self.assertRaises(ShopifyWebhookSecurityError):
                validate_shopify_webhook_request(
                    raw_body=body,
                    headers=self._headers(body, **{"x-shopify-shop-domain": "other.myshopify.com"}),
                    config=self.config,
                )

    def test_valid_paid_shopify_order_creates_one_draft_with_exact_totals(self) -> None:
        event_id = self._register(self._payload())
        result = self._claim_and_process(event_id)
        self.assertEqual(result.status, "processed")
        with self.Session() as db:
            order = db.query(B2CSalesOrder).one()
            integration = db.query(ShopifyOrderIntegration).one()
            self.assertEqual(order.status, "draft")
            self.assertEqual(order.channel, "Online Web")
            self.assertEqual(order.order_date.isoformat(), "2026-10-25")
            self.assertEqual(order.total_amount, Decimal("3800.0000"))
            self.assertEqual(order.discount_amount, Decimal("700.0000"))
            self.assertEqual([line.line_type for line in order.lines], ["product", "shipping"])
            self.assertEqual([line.net_line_total_snapshot for line in order.lines], [Decimal("1800.0000"), Decimal("2000.0000")])
            self.assertEqual(integration.financial_status, "PAID")
            self.assertEqual(integration.total_received_amount, Decimal("3800.0000"))
            self.assertEqual(integration.outstanding_amount, Decimal("0.0000"))
            self.assertEqual(db.query(InventoryTransaction).count(), 0)
            self.assertEqual(db.query(AuditLog).filter(AuditLog.action == "shopify_b2c_draft_created").count(), 1)

    def test_existing_customer_is_reused_by_phone_without_master_update(self) -> None:
        with self.Session.begin() as db:
            customer = B2CCustomer(name="Nombre maestro", phone="+50688887777", email=None, active=True)
            db.add(customer)
            db.flush()
            customer_id = customer.id
        self._claim_and_process(self._register(self._payload(email=None, customer_gid=None)))
        with self.Session() as db:
            self.assertEqual(db.query(B2CCustomer).count(), 1)
            customer = db.get(B2CCustomer, customer_id)
            self.assertEqual(customer.name, "Nombre maestro")
            self.assertIsNone(customer.email)
            self.assertEqual(db.query(B2CSalesOrder).one().b2c_customer_id, customer_id)

    def test_existing_customer_is_reused_by_email(self) -> None:
        with self.Session.begin() as db:
            customer = B2CCustomer(name="Correo maestro", phone=None, email="CLIENTE@example.com", active=True)
            db.add(customer)
            db.flush()
            customer_id = customer.id
        self._claim_and_process(self._register(self._payload(phone=None, customer_gid=None)))
        with self.Session() as db:
            self.assertEqual(db.query(B2CCustomer).count(), 1)
            self.assertEqual(db.query(B2CSalesOrder).one().b2c_customer_id, customer_id)

    def test_new_customer_without_email_is_created_with_gid_mapping(self) -> None:
        self._claim_and_process(self._register(self._payload(email=None)))
        with self.Session() as db:
            customer = db.query(B2CCustomer).one()
            mapping = db.query(ShopifyCustomerMapping).one()
            self.assertIsNone(customer.email)
            self.assertEqual(mapping.b2c_customer_id, customer.id)

    def test_ambiguous_customer_stays_review_pending_without_partial_order(self) -> None:
        with self.Session.begin() as db:
            db.add_all(
                (
                    B2CCustomer(name="Uno", phone="88887777", active=True),
                    B2CCustomer(name="Dos", phone="+506 8888 7777", active=True),
                )
            )
        result = self._claim_and_process(self._register(self._payload(customer_gid=None)))
        self.assertEqual(result.status, "review_pending")
        with self.Session() as db:
            self.assertEqual(db.query(B2CSalesOrder).count(), 0)
            self.assertEqual(db.query(ShopifyOrderIntegration).one().review_code, "customer_match")

    def test_missing_sku_stays_review_pending_without_customer_or_order(self) -> None:
        result = self._claim_and_process(self._register(self._payload(sku="MISSING")))
        self.assertEqual(result.status, "review_pending")
        with self.Session() as db:
            self.assertEqual(db.query(B2CSalesOrder).count(), 0)
            self.assertEqual(db.query(B2CCustomer).count(), 0)
            self.assertEqual(db.query(ShopifyOrderIntegration).one().review_code, "product_sku")

    def test_webhook_api_version_mismatch_stays_in_review_without_partial_order(self) -> None:
        with self.Session() as db:
            event, _ = register_shopify_webhook_event_in_session(
                db,
                shop_domain=self.config.shop_domain,
                webhook_id="wrong-api-version",
                topic="orders/create",
                raw_body=self._body(self._payload()),
                api_version="2026-07",
            )
            db.commit()
            event_id = event.id
        result = self._claim_and_process(event_id)
        self.assertEqual(result.status, "review_pending")
        with self.Session() as db:
            event = db.get(ShopifyWebhookEvent, event_id)
            self.assertEqual(event.error_code, "api_version_mismatch")
            self.assertEqual(db.query(B2CSalesOrder).count(), 0)

    def test_outside_gam_shipping_uses_confirmed_sku(self) -> None:
        payload = self._payload(
            shipping_code="outside-gam",
            shipping_price="3500",
            shipping_discount="0",
            total="5300",
        )
        payload["current_total_discounts"] = "200"
        self._claim_and_process(self._register(payload))
        with self.Session() as db:
            shipping_line = next(line for line in db.query(B2CSalesOrder).one().lines if line.line_type == "shipping")
            self.assertEqual(shipping_line.sku_snapshot, "10258")

    def test_free_shipping_and_pickup_are_not_duplicated(self) -> None:
        free_payload = self._payload(shipping_discount="2500", total="1800")
        free_payload["current_total_discounts"] = "2700"
        self._claim_and_process(self._register(free_payload, webhook_id="free"))
        pickup = self._payload(
            order_id=1002,
            shipping_code="pickup",
            shipping_price="0",
            shipping_discount="0",
            total="1800",
        )
        pickup["current_total_discounts"] = "200"
        self._claim_and_process(self._register(pickup, webhook_id="pickup"))
        with self.Session() as db:
            orders = db.query(B2CSalesOrder).order_by(B2CSalesOrder.id).all()
            free_shipping = next(line for line in orders[0].lines if line.line_type == "shipping")
            self.assertEqual(free_shipping.net_line_total_snapshot, Decimal("0.0000"))
            self.assertEqual(len(orders[1].lines), 1)
            pickup_integration = db.query(ShopifyOrderIntegration).filter(ShopifyOrderIntegration.shopify_order_gid.endswith("1002")).one()
            self.assertEqual(pickup_integration.delivery_method_type, "pickup")

    def test_unknown_shipping_method_and_total_mismatch_require_review(self) -> None:
        unknown = self._claim_and_process(self._register(self._payload(shipping_code="unknown"), webhook_id="unknown"))
        self.assertEqual(unknown.status, "review_pending")
        mismatch_payload = self._payload(order_id=1002, total="9999")
        mismatch = self._claim_and_process(self._register(mismatch_payload, webhook_id="mismatch"))
        self.assertEqual(mismatch.status, "review_pending")
        with self.Session() as db:
            self.assertEqual(db.query(B2CSalesOrder).count(), 0)

    def test_non_crc_and_nonzero_tax_require_review(self) -> None:
        currency_result = self._claim_and_process(
            self._register(self._payload(currency="USD"), webhook_id="currency")
        )
        tax_result = self._claim_and_process(
            self._register(self._payload(order_id=1002, tax="130"), webhook_id="tax")
        )
        self.assertEqual(currency_result.status, "review_pending")
        self.assertEqual(tax_result.status, "review_pending")
        with self.Session() as db:
            self.assertEqual(db.query(B2CSalesOrder).count(), 0)
            self.assertEqual(
                {row.review_code for row in db.query(ShopifyOrderIntegration).all()},
                {"unsupported_currency", "nonzero_tax"},
            )

    def test_duplicate_delivery_and_duplicate_order_never_create_two_drafts(self) -> None:
        payload = self._payload()
        event_id = self._register(payload)
        self._claim_and_process(event_id)
        with self.Session() as db:
            duplicate, created = register_shopify_webhook_event_in_session(
                db,
                shop_domain=self.config.shop_domain,
                webhook_id="wh-1",
                topic="orders/create",
                raw_body=self._body(payload),
            )
            self.assertFalse(created)
            self.assertEqual(duplicate.id, event_id)
            db.rollback()
        second_id = self._register(payload, webhook_id="wh-2")
        second_result = self._claim_and_process(second_id)
        self.assertEqual(second_result.message, "Shopify order was already imported.")
        with self.Session() as db:
            self.assertEqual(db.query(B2CSalesOrder).count(), 1)

    def test_transaction_rolls_back_customer_order_mapping_and_audit_on_failure(self) -> None:
        event_id = self._register(self._payload())
        with self.Session() as db:
            claim_next_shopify_event(db)
            db.commit()
        with mock.patch(
            "app.services.shopify_phase1b_service.add_audit_event_in_session",
            side_effect=RuntimeError("audit unavailable"),
        ):
            with self.Session() as db:
                with self.assertRaises(RuntimeError):
                    process_claimed_shopify_event_in_session(db, event_id=event_id, config=self.config)
                db.rollback()
        with self.Session() as db:
            self.assertEqual(db.query(B2CCustomer).count(), 0)
            self.assertEqual(db.query(B2CSalesOrder).count(), 0)
            self.assertEqual(db.query(ShopifyCustomerMapping).count(), 0)
            self.assertEqual(db.query(ShopifyOrderIntegration).count(), 0)
            mark_shopify_event_retry(db, event_id=event_id, error=RuntimeError("audit unavailable"))
            db.commit()
            self.assertEqual(db.get(ShopifyWebhookEvent, event_id).processing_status, "retry")

    def test_worker_is_disabled_by_default_and_recovers_persisted_event_when_enabled(self) -> None:
        event_id = self._register(self._payload())
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertEqual(run_shopify_worker_cycle(self.Session, config=self.config), [])
        with mock.patch.dict(
            os.environ,
            {
                "SHOPIFY_INTEGRATION_ENABLED": "true",
                "SHOPIFY_BACKGROUND_PROCESSOR_ENABLED": "true",
            },
            clear=True,
        ):
            results = run_shopify_worker_cycle(
                self.Session,
                config=self.config,
                graph_ql_client=self._financial_client(),
            )
        self.assertEqual([result.event_id for result in results], [event_id])
        with self.Session() as db:
            self.assertEqual(db.query(B2CSalesOrder).count(), 1)

    def test_graphql_financial_snapshot_is_persisted_without_creating_payment(self) -> None:
        event_id = self._register(self._payload())
        with self.Session() as db:
            claim_next_shopify_event(db)
            db.commit()
        client = self._financial_client()
        client.get_order_financial_snapshot.return_value = ShopifyFinancialSnapshot(
            order_gid="gid://shopify/Order/1001",
            financial_status="PARTIALLY_PAID",
            currency_code="CRC",
            total_amount=Decimal("3800.0000"),
            tax_amount=Decimal("0.0000"),
            total_received_amount=Decimal("2000.0000"),
            outstanding_amount=Decimal("1800.0000"),
            refunded_amount=Decimal("0.0000"),
            updated_at=datetime(2026, 10, 26),
        )
        with self.Session() as db:
            process_claimed_shopify_event_in_session(
                db,
                event_id=event_id,
                config=self.config,
                graph_ql_client=client,
            )
            db.commit()
        with self.Session() as db:
            integration = db.query(ShopifyOrderIntegration).one()
            self.assertEqual(integration.financial_status, "PARTIALLY_PAID")
            self.assertEqual(integration.total_received_amount, Decimal("2000.0000"))
            self.assertEqual(integration.outstanding_amount, Decimal("1800.0000"))
            self.assertEqual(db.query(B2CSalesOrder).one().status, "draft")
            self.assertEqual(db.query(InventoryTransaction).count(), 0)

    def test_b2c_detail_loads_the_linked_shopify_snapshot(self) -> None:
        self._claim_and_process(self._register(self._payload()))
        scope = {
            "type": "http",
            "http_version": "1.1",
            "method": "GET",
            "scheme": "https",
            "path": "/b2c/orders/1",
            "raw_path": b"/b2c/orders/1",
            "query_string": b"",
            "headers": [(b"cookie", b"erp_session=session-a")],
            "client": ("127.0.0.1", 1234),
            "server": ("erp.example", 443),
        }
        request = Request(scope)
        route = self._main_attribute("b2c_order_detail")
        with self.Session() as db, mock.patch("app.main.require_permission"), mock.patch(
            "app.main.templates.TemplateResponse", return_value=SimpleNamespace(status_code=200)
        ) as render:
            order = db.query(B2CSalesOrder).one()
            response = route(order.id, request, db)
        self.assertEqual(response.status_code, 200)
        context = render.call_args.kwargs["context"]
        self.assertEqual(context["shopify_integration"].shopify_order_gid, "gid://shopify/Order/1001")

    def test_graphql_failure_is_retried_without_partial_order(self) -> None:
        event_id = self._register(self._payload())
        client = self._financial_client()
        client.get_order_financial_snapshot.side_effect = ShopifyTransientError("API unavailable")
        with mock.patch.dict(
            os.environ,
            {
                "SHOPIFY_INTEGRATION_ENABLED": "true",
                "SHOPIFY_BACKGROUND_PROCESSOR_ENABLED": "true",
            },
            clear=True,
        ):
            results = run_shopify_worker_cycle(
                self.Session,
                config=self.config,
                graph_ql_client=client,
            )
        self.assertEqual(results, [])
        with self.Session() as db:
            event = db.get(ShopifyWebhookEvent, event_id)
            self.assertEqual(event.processing_status, "retry")
            self.assertEqual(db.query(B2CSalesOrder).count(), 0)

    def test_retry_csrf_is_session_and_event_bound_and_processed_event_cannot_repeat(self) -> None:
        token = build_shopify_retry_csrf_token("session-a", 10)
        validate_shopify_retry_csrf_token("session-a", 10, token)
        with self.assertRaises(ShopifyWebhookSecurityError):
            validate_shopify_retry_csrf_token("session-b", 10, token)
        event_id = self._register(self._payload())
        self._claim_and_process(event_id)
        with self.Session() as db:
            with self.assertRaises(ShopifyPhase1BError):
                queue_shopify_event_retry_in_session(db, event_id=event_id)

    def test_incremental_phase1b_schema_is_idempotent_and_preserves_phase1a_rows(self) -> None:
        legacy_engine = create_engine("sqlite://", poolclass=StaticPool)
        with legacy_engine.begin() as connection:
            connection.execute(text("CREATE TABLE shopify_webhook_events (id INTEGER PRIMARY KEY, shop_domain VARCHAR(255), webhook_id VARCHAR(255), topic VARCHAR(100), payload_sha256 VARCHAR(64), processing_status VARCHAR(50), attempt_count INTEGER, received_at DATETIME)"))
            connection.execute(text("CREATE TABLE shopify_order_integrations (id INTEGER PRIMARY KEY, shop_domain VARCHAR(255), shopify_order_gid VARCHAR(255), import_status VARCHAR(50), created_at DATETIME, updated_at DATETIME)"))
            connection.execute(text("INSERT INTO shopify_webhook_events (id, shop_domain, webhook_id, topic, payload_sha256, processing_status, attempt_count) VALUES (1, 'green-corner.myshopify.com', 'old', 'orders/create', 'abc', 'received', 0)"))
        with mock.patch.object(database_module, "engine", legacy_engine):
            database_module.ensure_shopify_phase1b_schema()
            database_module.ensure_shopify_phase1b_schema()
        columns = {column["name"] for column in inspect(legacy_engine).get_columns("shopify_webhook_events")}
        self.assertIn("payload_body", columns)
        self.assertIn("next_attempt_at", columns)
        with legacy_engine.connect() as connection:
            self.assertEqual(connection.execute(text("SELECT webhook_id FROM shopify_webhook_events WHERE id=1")).scalar_one(), "old")
        legacy_engine.dispose()

    def test_postgresql_phase1b_schema_uses_only_idempotent_incremental_sql(self) -> None:
        fake_engine = mock.MagicMock()
        fake_engine.dialect.name = "postgresql"
        connection = fake_engine.begin.return_value.__enter__.return_value
        with mock.patch.object(database_module, "engine", fake_engine):
            database_module.ensure_shopify_phase1b_schema()
        sql = "\n".join(call.args[0] for call in connection.exec_driver_sql.call_args_list)
        self.assertIn("ADD COLUMN IF NOT EXISTS payload_body TEXT", sql)
        self.assertIn("ADD COLUMN IF NOT EXISTS total_received_amount NUMERIC(12, 4)", sql)
        self.assertIn("CREATE INDEX IF NOT EXISTS ix_shopify_webhook_events_next_attempt_at", sql)
        self.assertNotIn("DROP ", sql.upper())

    def test_token_provider_supports_static_and_renewable_client_credentials(self) -> None:
        static = ShopifyAccessTokenProvider(
            shop_domain=self.config.shop_domain,
            static_access_token="static-secret",
        )
        self.assertEqual(static.get_token(), "static-secret")
        calls = []

        def fake_post(url, body, headers):
            calls.append((url, body, headers))
            return json.dumps({"access_token": f"token-{len(calls)}", "expires_in": 3600}).encode()

        renewable = ShopifyAccessTokenProvider(
            shop_domain=self.config.shop_domain,
            client_id="client",
            client_secret="secret",
            http_post=fake_post,
        )
        now = datetime(2026, 10, 25, tzinfo=timezone.utc)
        self.assertEqual(renewable.get_token(now=now), "token-1")
        self.assertEqual(renewable.get_token(now=now + timedelta(minutes=30)), "token-1")
        self.assertEqual(renewable.get_token(now=now + timedelta(minutes=56)), "token-2")
        self.assertEqual(len(calls), 2)
        self.assertNotIn(b"static-secret", calls[0][1])

    def test_graphql_client_rejects_missing_or_ambiguous_admin_credentials_before_work(self) -> None:
        base_env = {
            "SHOPIFY_SHOP_DOMAIN": self.config.shop_domain,
            "SHOPIFY_API_VERSION": self.config.api_version,
        }
        with mock.patch.dict(os.environ, base_env, clear=True):
            with self.assertRaises(ShopifyConfigurationError):
                ShopifyGraphQLClient.from_environment()
        with mock.patch.dict(
            os.environ,
            {
                **base_env,
                "SHOPIFY_ADMIN_ACCESS_TOKEN": "static-token",
                "SHOPIFY_CLIENT_ID": "client-id",
                "SHOPIFY_CLIENT_SECRET": "client-secret",
            },
            clear=True,
        ):
            with self.assertRaises(ShopifyConfigurationError):
                ShopifyGraphQLClient.from_environment()

    def test_graphql_client_reads_complete_financial_snapshot(self) -> None:
        response = {
            "data": {
                "order": {
                    "id": "gid://shopify/Order/1001",
                    "displayFinancialStatus": "PAID",
                    "updatedAt": "2026-10-26T01:00:00Z",
                    "currentTotalPriceSet": {"shopMoney": {"amount": "3800", "currencyCode": "CRC"}},
                    "currentTotalTaxSet": {"shopMoney": {"amount": "0", "currencyCode": "CRC"}},
                    "totalReceivedSet": {"shopMoney": {"amount": "3800", "currencyCode": "CRC"}},
                    "totalOutstandingSet": {"shopMoney": {"amount": "0", "currencyCode": "CRC"}},
                    "totalRefundedSet": {"shopMoney": {"amount": "0", "currencyCode": "CRC"}},
                }
            }
        }
        client = ShopifyGraphQLClient(
            shop_domain=self.config.shop_domain,
            api_version=self.config.api_version,
            token_provider=ShopifyAccessTokenProvider(
                shop_domain=self.config.shop_domain,
                static_access_token="token",
            ),
            http_post=lambda _url, _body, _headers: json.dumps(response).encode(),
        )
        snapshot = client.get_order_financial_snapshot("gid://shopify/Order/1001")
        self.assertEqual(snapshot.financial_status, "PAID")
        self.assertEqual(snapshot.total_received_amount, Decimal("3800.0000"))
        self.assertEqual(snapshot.currency_code, "CRC")

    def test_read_only_reconciliation_reports_missing_without_writes(self) -> None:
        with self.Session.begin() as db:
            db.add(
                ShopifyOrderIntegration(
                    shop_domain=self.config.shop_domain,
                    shopify_order_gid="gid://shopify/Order/1",
                    import_status="imported",
                )
            )
        client = mock.Mock(spec=ShopifyGraphQLClient)
        client.shop_domain = self.config.shop_domain
        client.list_order_gids_created_since.return_value = [
            "gid://shopify/Order/1",
            "gid://shopify/Order/2",
        ]
        with self.Session() as db:
            before = db.query(ShopifyOrderIntegration).count()
            result = detect_missing_shopify_orders(
                db,
                client=client,
                since=datetime(2026, 10, 25, tzinfo=timezone.utc),
            )
            after = db.query(ShopifyOrderIntegration).count()
        self.assertEqual(result.missing_order_gids, ("gid://shopify/Order/2",))
        self.assertEqual(before, after)

    def test_webhook_endpoint_persists_and_acknowledges_before_processing(self) -> None:
        payload = self._payload()
        body = self._body(payload)
        headers = self._headers(body)
        headers["content-type"] = "application/json"
        headers["content-length"] = str(len(body))
        scope = {
            "type": "http",
            "http_version": "1.1",
            "method": "POST",
            "scheme": "https",
            "path": "/integrations/shopify/webhooks/orders-create",
            "raw_path": b"/integrations/shopify/webhooks/orders-create",
            "query_string": b"",
            "headers": [(key.encode(), value.encode()) for key, value in headers.items()],
            "client": ("127.0.0.1", 1234),
            "server": ("erp.example", 443),
        }
        sent = False

        async def receive():
            nonlocal sent
            if sent:
                return {"type": "http.request", "body": b"", "more_body": False}
            sent = True
            return {"type": "http.request", "body": body, "more_body": False}

        request = Request(scope, receive)
        env = {
            "SHOPIFY_INTEGRATION_ENABLED": "true",
            "SHOPIFY_SHOP_DOMAIN": self.config.shop_domain,
            "SHOPIFY_WEBHOOK_SECRET": self.config.webhook_secret,
            "SHOPIFY_API_VERSION": self.config.api_version,
            "SHOPIFY_SHIPPING_METHOD_MAP": json.dumps(self.config.shipping_method_map),
        }
        with mock.patch.dict(os.environ, env, clear=True), self.Session() as db:
            response = asyncio.run(self._webhook_route()(request, db))
        self.assertEqual(response.status_code, 202)
        with self.Session() as db:
            event = db.query(ShopifyWebhookEvent).one()
            self.assertEqual(event.processing_status, "received")
            self.assertEqual(db.query(B2CSalesOrder).count(), 0)

    def test_webhook_endpoint_database_failure_never_returns_success(self) -> None:
        payload = self._payload()
        body = self._body(payload)
        headers = self._headers(body)
        headers["content-type"] = "application/json"
        scope = {
            "type": "http",
            "http_version": "1.1",
            "method": "POST",
            "scheme": "https",
            "path": "/integrations/shopify/webhooks/orders-create",
            "raw_path": b"/integrations/shopify/webhooks/orders-create",
            "query_string": b"",
            "headers": [(key.encode(), value.encode()) for key, value in headers.items()],
            "client": ("127.0.0.1", 1234),
            "server": ("erp.example", 443),
        }
        sent = False

        async def receive():
            nonlocal sent
            if sent:
                return {"type": "http.request", "body": b"", "more_body": False}
            sent = True
            return {"type": "http.request", "body": body, "more_body": False}

        request = Request(scope, receive)
        env = {
            "SHOPIFY_INTEGRATION_ENABLED": "true",
            "SHOPIFY_SHOP_DOMAIN": self.config.shop_domain,
            "SHOPIFY_WEBHOOK_SECRET": self.config.webhook_secret,
            "SHOPIFY_API_VERSION": self.config.api_version,
            "SHOPIFY_SHIPPING_METHOD_MAP": json.dumps(self.config.shipping_method_map),
        }
        with mock.patch.dict(os.environ, env, clear=True), self.Session() as db:
            with mock.patch.object(db, "commit", side_effect=RuntimeError("database unavailable")):
                with self.assertRaises(RuntimeError):
                    asyncio.run(self._webhook_route()(request, db))
            db.rollback()
        with self.Session() as db:
            self.assertEqual(db.query(ShopifyWebhookEvent).count(), 0)

    def test_retry_endpoint_requires_permission_same_origin_and_session_bound_csrf(self) -> None:
        event_id = self._register(self._payload())
        with self.Session.begin() as db:
            db.get(ShopifyWebhookEvent, event_id).processing_status = "review_pending"
        route = self._main_attribute("retry_shopify_order_import")
        token = build_shopify_retry_csrf_token("session-a", event_id)

        def request_with_headers(*headers: tuple[bytes, bytes]) -> Request:
            return Request(
                {
                    "type": "http",
                    "http_version": "1.1",
                    "method": "POST",
                    "scheme": "https",
                    "path": f"/b2c/orders/shopify-events/{event_id}/retry",
                    "raw_path": f"/b2c/orders/shopify-events/{event_id}/retry".encode(),
                    "query_string": b"",
                    "headers": [(b"cookie", b"erp_session=session-a"), *headers],
                    "client": ("127.0.0.1", 1234),
                    "server": ("erp.example", 443),
                }
            )

        authorized_user = SimpleNamespace(id=1, username="authorized")
        env = {"APP_BASE_URL": "https://erp.example"}
        with mock.patch.dict(os.environ, env, clear=True), mock.patch(
            "app.main.require_permission", return_value=authorized_user
        ), self.Session() as db:
            with self.assertRaises(HTTPException) as missing_origin:
                route(event_id, request_with_headers(), token, db)
            self.assertEqual(missing_origin.exception.status_code, 403)

        with mock.patch.dict(os.environ, env, clear=True), mock.patch(
            "app.main.require_permission", side_effect=HTTPException(status_code=403)
        ), self.Session() as db:
            with self.assertRaises(HTTPException):
                route(
                    event_id,
                    request_with_headers((b"origin", b"https://erp.example")),
                    token,
                    db,
                )

        with mock.patch.dict(os.environ, env, clear=True), mock.patch(
            "app.main.require_permission", return_value=authorized_user
        ), mock.patch("app.main.add_audit_event_in_session"), self.Session() as db:
            response = route(
                event_id,
                request_with_headers((b"origin", b"https://erp.example")),
                token,
                db,
            )
            self.assertEqual(response.status_code, 303)
        with self.Session() as db:
            self.assertEqual(db.get(ShopifyWebhookEvent, event_id).processing_status, "received")

    def test_webhook_endpoint_rejects_invalid_hmac_without_writes(self) -> None:
        body = self._body(self._payload())
        headers = self._headers(body, **{"x-shopify-hmac-sha256": "invalid"})
        scope = {
            "type": "http",
            "http_version": "1.1",
            "method": "POST",
            "scheme": "https",
            "path": "/integrations/shopify/webhooks/orders-create",
            "raw_path": b"/integrations/shopify/webhooks/orders-create",
            "query_string": b"",
            "headers": [(key.encode(), value.encode()) for key, value in headers.items()],
            "client": ("127.0.0.1", 1234),
            "server": ("erp.example", 443),
        }

        async def receive():
            return {"type": "http.request", "body": body, "more_body": False}

        request = Request(scope, receive)
        env = {
            "SHOPIFY_INTEGRATION_ENABLED": "true",
            "SHOPIFY_SHOP_DOMAIN": self.config.shop_domain,
            "SHOPIFY_WEBHOOK_SECRET": self.config.webhook_secret,
        }
        with mock.patch.dict(os.environ, env, clear=True), self.Session() as db:
            with self.assertRaises(HTTPException) as rejected:
                asyncio.run(self._webhook_route()(request, db))
            self.assertEqual(rejected.exception.status_code, 401)
        with self.Session() as db:
            self.assertEqual(db.query(ShopifyWebhookEvent).count(), 0)

    def test_webhook_endpoint_stream_limit_rejects_body_without_content_length(self) -> None:
        body = b"x" * 1025
        scope = {
            "type": "http",
            "http_version": "1.1",
            "method": "POST",
            "scheme": "https",
            "path": "/integrations/shopify/webhooks/orders-create",
            "raw_path": b"/integrations/shopify/webhooks/orders-create",
            "query_string": b"",
            "headers": [],
            "client": ("127.0.0.1", 1234),
            "server": ("erp.example", 443),
        }
        sent = False

        async def receive():
            nonlocal sent
            if sent:
                return {"type": "http.request", "body": b"", "more_body": False}
            sent = True
            return {"type": "http.request", "body": body, "more_body": False}

        request = Request(scope, receive)
        env = {
            "SHOPIFY_INTEGRATION_ENABLED": "true",
            "SHOPIFY_SHOP_DOMAIN": self.config.shop_domain,
            "SHOPIFY_WEBHOOK_SECRET": self.config.webhook_secret,
            "SHOPIFY_MAX_WEBHOOK_BYTES": "1024",
        }
        with mock.patch.dict(os.environ, env, clear=True), self.Session() as db:
            with self.assertRaises(HTTPException) as rejected:
                asyncio.run(self._webhook_route()(request, db))
            self.assertEqual(rejected.exception.status_code, 413)
        with self.Session() as db:
            self.assertEqual(db.query(ShopifyWebhookEvent).count(), 0)


if __name__ == "__main__":
    unittest.main()
