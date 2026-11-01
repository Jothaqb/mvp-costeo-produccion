import json
import unittest
from datetime import datetime
from decimal import Decimal
from unittest import mock

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.database import Base
from app.models import (
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
    ShopifyFinancialSnapshot,
    ShopifyGraphQLClient,
    ShopifyRuntimeConfig,
)
from tools import shopify_process_single_event as single_event_tool


class ShopifyProcessSingleEventTests(unittest.TestCase):
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
                        sku="10265",
                        name="Producto Shopify controlado",
                        active=True,
                        available_for_sale_gc=True,
                        b2c_price=Decimal("3300"),
                    ),
                    Product(
                        sku="10219",
                        name="Envío GAM",
                        active=True,
                        available_for_sale_gc=True,
                        b2c_price=Decimal("2500"),
                    ),
                )
            )
        self.config = ShopifyRuntimeConfig(
            shop_domain="green-corner.myshopify.com",
            webhook_secret="test-only-secret",
            api_version="2026-10",
            shipping_method_map={"shopify|gam menos de 20000": "10219"},
            max_webhook_bytes=2 * 1024 * 1024,
        )
        self.environment = mock.patch.dict(
            "os.environ",
            {
                "SHOPIFY_INTEGRATION_ENABLED": "true",
                "SHOPIFY_BACKGROUND_PROCESSOR_ENABLED": "false",
            },
        )
        self.environment.start()

    def tearDown(self) -> None:
        self.environment.stop()
        Base.metadata.drop_all(self.engine)
        self.engine.dispose()

    @staticmethod
    def _payload(*, order_id: int = 8198807126309, order_name: str = "#1679") -> dict:
        return {
            "id": order_id,
            "admin_graphql_api_id": f"gid://shopify/Order/{order_id}",
            "name": order_name,
            "source_name": "shopify_draft_order",
            "created_at": "2026-10-30T10:00:00-06:00",
            "updated_at": "2026-10-30T10:05:00-06:00",
            "currency": "CRC",
            "current_total_tax": "0",
            "current_total_price": "5800",
            "current_total_discounts": "0",
            "financial_status": "pending",
            "fulfillment_status": None,
            "total_received": "0",
            "total_outstanding": "5800",
            "total_refunded": "0",
            "phone": "88887777",
            "email": "shopify-test@example.invalid",
            "customer": {
                "id": 901,
                "admin_graphql_api_id": "gid://shopify/Customer/901",
                "first_name": "Cliente",
                "last_name": "Sintético",
                "phone": "88887777",
                "email": "shopify-test@example.invalid",
            },
            "shipping_address": {
                "name": "Cliente Sintético",
                "phone": "88887777",
                "address1": "Dirección sintética",
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
                    "sku": "10265",
                    "name": "Producto Shopify controlado",
                    "quantity": 1,
                    "price": "3300",
                    "requires_shipping": True,
                    "discount_allocations": [],
                }
            ],
            "shipping_lines": [
                {
                    "id": 801,
                    "admin_graphql_api_id": "gid://shopify/ShippingLine/801",
                    "title": "GAM menos de 20000",
                    "source": "shopify",
                    "code": "GAM menos de 20000",
                    "price": "2500",
                    "discount_allocations": [],
                }
            ],
        }

    @staticmethod
    def _body(payload: dict) -> bytes:
        return json.dumps(payload, separators=(",", ":")).encode("utf-8")

    def _register(self, *, payload: dict | None = None, webhook_id: str = "single-event-1") -> int:
        with self.Session.begin() as db:
            event, _ = register_shopify_webhook_event_in_session(
                db,
                shop_domain=self.config.shop_domain,
                webhook_id=webhook_id,
                topic="orders/create",
                raw_body=self._body(payload or self._payload()),
                api_version="2026-10",
            )
            return event.id

    def _financial_client(self) -> mock.Mock:
        client = mock.Mock(spec=ShopifyGraphQLClient)
        client.get_order_financial_snapshot.return_value = ShopifyFinancialSnapshot(
            order_gid=single_event_tool.APPROVED_ORDER_GID,
            financial_status="PENDING",
            currency_code="CRC",
            total_amount=Decimal("5800.0000"),
            tax_amount=Decimal("0.0000"),
            total_received_amount=Decimal("0.0000"),
            outstanding_amount=Decimal("5800.0000"),
            refunded_amount=Decimal("0.0000"),
            updated_at=datetime(2026, 10, 30, 16, 5),
        )
        return client

    def _dry_run(self, event_id: int, *, expected_gid: str | None = None) -> dict:
        return single_event_tool.dry_run_single_event(
            self.Session,
            event_id=event_id,
            expected_order_gid=expected_gid or single_event_tool.APPROVED_ORDER_GID,
            config=self.config,
            graph_ql_client=self._financial_client(),
        )

    def test_cli_requires_one_mode_and_execute_confirmation(self) -> None:
        with self.assertRaises(SystemExit):
            single_event_tool.parse_args(
                ["--event-id", "1", "--expected-order-gid", single_event_tool.APPROVED_ORDER_GID]
            )
        with self.assertRaises(SystemExit):
            single_event_tool.parse_args(
                [
                    "--event-id",
                    "1",
                    "--expected-order-gid",
                    single_event_tool.APPROVED_ORDER_GID,
                    "--execute",
                ]
            )
        args = single_event_tool.parse_args(
            [
                "--event-id",
                "1",
                "--expected-order-gid",
                single_event_tool.APPROVED_ORDER_GID,
                "--dry-run",
            ]
        )
        self.assertTrue(args.dry_run)

    def test_dry_run_is_read_only_and_reports_approved_plan(self) -> None:
        event_id = self._register()

        plan = self._dry_run(event_id)

        self.assertEqual(plan["status"], single_event_tool.READY)
        self.assertEqual(plan["channel"], "Online Web")
        self.assertEqual(plan["technical_origin"], "shopify_draft_order")
        self.assertEqual(plan["product_sku"], "10265")
        self.assertEqual(plan["shipping_sku"], "10219")
        self.assertEqual(plan["total"], "5800.0000")
        with self.Session() as db:
            event = db.get(ShopifyWebhookEvent, event_id)
            self.assertEqual(event.processing_status, "received")
            self.assertEqual(event.attempt_count, 0)
            self.assertEqual(db.query(B2CSalesOrder).count(), 0)
            self.assertEqual(db.query(B2CCustomer).count(), 0)
            self.assertEqual(db.query(ShopifyCustomerMapping).count(), 0)
            self.assertEqual(db.query(ShopifyOrderIntegration).count(), 0)

    def test_dry_run_blocks_wrong_gid_non_received_duplicate_and_graphql_failure(self) -> None:
        event_id = self._register()
        wrong_gid = "gid://shopify/Order/999"
        self.assertEqual(self._dry_run(event_id, expected_gid=wrong_gid)["status"], "BLOCKED")

        with self.Session.begin() as db:
            db.get(ShopifyWebhookEvent, event_id).processing_status = "processing"
        self.assertEqual(self._dry_run(event_id)["status"], "BLOCKED")

        with self.Session.begin() as db:
            db.get(ShopifyWebhookEvent, event_id).processing_status = "retry"
        self.assertEqual(self._dry_run(event_id)["status"], "BLOCKED")

        with self.Session.begin() as db:
            event = db.get(ShopifyWebhookEvent, event_id)
            event.processing_status = "received"
            db.add(
                ShopifyOrderIntegration(
                    shop_domain=self.config.shop_domain,
                    shopify_order_gid=single_event_tool.APPROVED_ORDER_GID,
                    source_webhook_event_id=event_id,
                )
            )
        self.assertEqual(self._dry_run(event_id)["status"], "BLOCKED")

        with self.Session.begin() as db:
            db.query(ShopifyOrderIntegration).delete()
        failed_client = self._financial_client()
        failed_client.get_order_financial_snapshot.side_effect = RuntimeError("synthetic failure")
        plan = single_event_tool.dry_run_single_event(
            self.Session,
            event_id=event_id,
            expected_order_gid=single_event_tool.APPROVED_ORDER_GID,
            config=self.config,
            graph_ql_client=failed_client,
        )
        self.assertEqual(plan["status"], "BLOCKED")
        with self.Session() as db:
            self.assertEqual(db.get(ShopifyWebhookEvent, event_id).processing_status, "received")
            self.assertEqual(db.query(B2CSalesOrder).count(), 0)

    def test_worker_must_remain_disabled(self) -> None:
        event_id = self._register()
        with mock.patch.dict("os.environ", {"SHOPIFY_BACKGROUND_PROCESSOR_ENABLED": "true"}):
            plan = self._dry_run(event_id)
        self.assertEqual(plan["status"], "BLOCKED")
        self.assertTrue(any("must remain false" in reason for reason in plan["reasons"]))

    def test_execute_processes_only_exact_event_and_second_execution_is_idempotent(self) -> None:
        event_id = self._register()
        other_event_id = self._register(
            payload=self._payload(order_id=8198807126310, order_name="#1680"),
            webhook_id="single-event-other",
        )
        plan = self._dry_run(event_id)

        result = single_event_tool.execute_single_event(
            self.Session,
            event_id=event_id,
            expected_order_gid=single_event_tool.APPROVED_ORDER_GID,
            confirmation=plan["confirmation"],
            config=self.config,
            graph_ql_client=self._financial_client(),
        )

        self.assertEqual(result["status"], "PROCESSED")
        with self.Session() as db:
            order = db.query(B2CSalesOrder).one()
            self.assertEqual(order.status, "draft")
            self.assertEqual(order.channel, "Online Web")
            self.assertEqual(order.total_amount, Decimal("5800.0000"))
            self.assertEqual(order.discount_amount, Decimal("0.0000"))
            self.assertEqual(
                [(line.sku_snapshot, line.line_type) for line in order.lines],
                [("10265", "product"), ("10219", "shipping")],
            )
            self.assertEqual(db.query(InventoryTransaction).count(), 0)
            self.assertEqual(db.get(ShopifyWebhookEvent, other_event_id).processing_status, "received")

        repeated = single_event_tool.execute_single_event(
            self.Session,
            event_id=event_id,
            expected_order_gid=single_event_tool.APPROVED_ORDER_GID,
            confirmation=plan["confirmation"],
            config=self.config,
            graph_ql_client=self._financial_client(),
        )
        self.assertEqual(repeated["status"], "ALREADY_PROCESSED")
        with self.Session() as db:
            self.assertEqual(db.query(B2CSalesOrder).count(), 1)
            self.assertEqual(db.query(ShopifyOrderIntegration).count(), 1)

    def test_wrong_confirmation_and_processing_status_block_concurrent_execution(self) -> None:
        event_id = self._register()
        plan = self._dry_run(event_id)
        with self.assertRaises(single_event_tool.SingleShopifyEventError):
            single_event_tool.execute_single_event(
                self.Session,
                event_id=event_id,
                expected_order_gid=single_event_tool.APPROVED_ORDER_GID,
                confirmation="incorrect",
                config=self.config,
                graph_ql_client=self._financial_client(),
            )
        with self.Session.begin() as db:
            db.get(ShopifyWebhookEvent, event_id).processing_status = "processing"
        with self.assertRaises(single_event_tool.SingleShopifyEventError):
            single_event_tool.execute_single_event(
                self.Session,
                event_id=event_id,
                expected_order_gid=single_event_tool.APPROVED_ORDER_GID,
                confirmation=plan["confirmation"],
                config=self.config,
                graph_ql_client=self._financial_client(),
            )
        with self.Session() as db:
            self.assertEqual(db.query(B2CSalesOrder).count(), 0)

    def test_processing_failure_rolls_back_and_does_not_leave_event_processing(self) -> None:
        event_id = self._register()
        plan = self._dry_run(event_id)
        with mock.patch.object(
            single_event_tool,
            "process_claimed_shopify_event_in_session",
            side_effect=RuntimeError("synthetic database failure"),
        ):
            with self.assertRaises(RuntimeError):
                single_event_tool.execute_single_event(
                    self.Session,
                    event_id=event_id,
                    expected_order_gid=single_event_tool.APPROVED_ORDER_GID,
                    confirmation=plan["confirmation"],
                    config=self.config,
                    graph_ql_client=self._financial_client(),
                )
        with self.Session() as db:
            event = db.get(ShopifyWebhookEvent, event_id)
            self.assertEqual(event.processing_status, "retry")
            self.assertIsNone(event.processing_started_at)
            self.assertEqual(db.query(B2CSalesOrder).count(), 0)
            self.assertEqual(db.query(ShopifyOrderIntegration).count(), 0)
            self.assertEqual(db.query(B2CCustomer).count(), 0)

    def test_postgresql_event_query_requests_a_row_lock(self) -> None:
        db = mock.Mock()
        query = db.query.return_value.filter.return_value
        db.get_bind.return_value.dialect.name = "postgresql"
        single_event_tool._event_query(db, 1, for_update=True)
        query.with_for_update.assert_called_once_with(of=ShopifyWebhookEvent)


if __name__ == "__main__":
    unittest.main()
