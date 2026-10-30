import os
import unittest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch
from pathlib import Path

from jinja2 import Environment, FileSystemLoader
from sqlalchemy import create_engine
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.database import Base
from app.models import (
    AuditLog,
    B2BCustomer,
    B2BLoyverseInvoiceDispatch,
    B2BSalesOrder,
    B2BSalesOrderLine,
    Channel,
    LoyverseCustomerMapping,
    LoyversePaymentTypeMapping,
    LoyverseVariantMapping,
    User,
)
from app.services.b2b_loyverse_invoicing_workflow_service import (
    B2BLoyverseInvoicingSecurityError,
    B2BLoyverseInvoicingStalePlanError,
    build_b2b_loyverse_invoicing_confirmation_token,
    build_b2b_loyverse_invoicing_csrf_token,
    build_b2b_loyverse_invoicing_preview,
    queue_b2b_loyverse_invoicing_preview,
    run_b2b_loyverse_invoice_worker_cycle,
    validate_b2b_loyverse_invoicing_confirmation_token,
    validate_b2b_loyverse_invoicing_csrf_token,
)
from app.services.b2b_loyverse_mapping_service import (
    B2B_CHANNEL_TO_LOYVERSE_PAYMENT_TYPE_NAME,
    build_b2b_invoice_readiness,
    resolve_b2b_loyverse_payment_type_mapping,
)


class B2BLoyverseInvoicingWorkflowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, autocommit=False, autoflush=False)
        self.db = self.Session()
        self.user = User(
            username="invoice-admin",
            full_name="Invoice Admin",
            email="invoice-admin@example.test",
            password_hash="test-only",
            is_active=True,
        )
        self.default_channel = Channel(
            name="Tiendas Naturales (Supermercados)",
            active=True,
            applies_to_b2b=True,
            applies_to_b2c=False,
        )
        self.db.add_all([self.user, self.default_channel])
        self.db.flush()
        self.customer = B2BCustomer(
            customer_name="Cliente de prueba",
            phone="88888888",
            loyverse_customer_id="loyverse-customer-1",
            channel_id=self.default_channel.id,
            active=True,
        )
        self.db.add(self.customer)
        self.db.flush()
        refreshed_at = datetime(2026, 10, 1)
        self.db.add_all(
            [
                LoyversePaymentTypeMapping(
                    loyverse_payment_type_id="payment-type-1",
                    name="Tiendas Naturales",
                    payment_type="OTHER",
                    active=True,
                    last_refreshed_at=refreshed_at,
                ),
                LoyversePaymentTypeMapping(
                    loyverse_payment_type_id="payment-type-horeca",
                    name="Horeca",
                    payment_type="OTHER",
                    active=True,
                    last_refreshed_at=refreshed_at,
                ),
                LoyversePaymentTypeMapping(
                    loyverse_payment_type_id="payment-type-maquila",
                    name="Maquila",
                    payment_type="OTHER",
                    active=True,
                    last_refreshed_at=refreshed_at,
                ),
                LoyverseVariantMapping(
                    sku="SKU-1",
                    loyverse_variant_id="variant-1",
                    loyverse_item_id="item-1",
                    item_name="Producto",
                    active=True,
                    last_refreshed_at=refreshed_at,
                ),
            ]
        )
        self.db.commit()
        self.environment = patch.dict(
            os.environ,
            {
                "LOYVERSE_STORE_ID": "store-1",
                "LOYVERSE_API_TOKEN": "test-token-never-sent",
            },
            clear=False,
        )
        self.environment.start()

    def tearDown(self) -> None:
        self.environment.stop()
        self.db.close()
        Base.metadata.drop_all(self.engine)
        self.engine.dispose()

    def _order(
        self,
        number: str,
        *,
        status: str = "invoiced",
        sync_status: str | None = None,
        receipt_id: str | None = None,
        receipt_number: str | None = None,
        delivery_date: date = date(2026, 10, 15),
        invoice_date: date | None = None,
        invoiced_at: datetime | None = None,
        channel_id: int | None = None,
        without_channel: bool = False,
        loyverse_customer_id_snapshot: str | None = "loyverse-customer-1",
        phone_snapshot: str | None = None,
    ) -> B2BSalesOrder:
        selected_channel_id = (
            None
            if without_channel
            else (channel_id if channel_id is not None else self.default_channel.id)
        )
        selected_channel = (
            self.db.get(Channel, selected_channel_id)
            if selected_channel_id is not None
            else None
        )
        order = B2BSalesOrder(
            order_number=number,
            customer_id=self.customer.id,
            customer_name_snapshot=self.customer.customer_name,
            phone_snapshot=(
                phone_snapshot if phone_snapshot is not None else self.customer.phone
            ),
            loyverse_customer_id_snapshot=loyverse_customer_id_snapshot,
            channel_id=selected_channel_id,
            delivery_date=delivery_date,
            status=status,
            total_amount=Decimal("2000"),
            b2b_channel_name_snapshot=(selected_channel.name if selected_channel else None),
            loyverse_invoice_sync_status=sync_status,
            loyverse_receipt_id=receipt_id,
            loyverse_receipt_number=receipt_number,
            invoice_date=(
                invoice_date
                if invoice_date is not None
                else (delivery_date if status == "invoiced" and invoiced_at is None else None)
            ),
            invoiced_at=invoiced_at,
        )
        order.lines.append(
            B2BSalesOrderLine(
                line_number=1,
                sku_snapshot="SKU-1",
                description_snapshot="Producto de prueba",
                unit_price_snapshot=Decimal("1000"),
                quantity=Decimal("2"),
                line_total=Decimal("2000"),
            )
        )
        self.db.add(order)
        self.db.commit()
        self.db.refresh(order)
        return order

    def _preview(self):
        return build_b2b_loyverse_invoicing_preview(
            self.db,
            start_date=date(2026, 10, 1),
            end_date=date(2026, 10, 29),
        )

    def _request(self):
        return SimpleNamespace(
            url=SimpleNamespace(path="/b2b/orders/loyverse-invoicing/confirm"),
            method="POST",
            client=None,
            headers={},
            state=SimpleNamespace(current_user=self.user),
        )

    def _channel(self, name: str, *, applies_to_b2b: bool = True) -> Channel:
        channel = Channel(
            name=name,
            active=True,
            applies_to_b2b=applies_to_b2b,
            applies_to_b2c=not applies_to_b2b,
        )
        self.db.add(channel)
        self.db.commit()
        self.db.refresh(channel)
        return channel

    def test_all_six_approved_channels_resolve_many_to_one_payment_types(self) -> None:
        expected_ids = {
            "Horeca": "payment-type-horeca",
            "Maquila": "payment-type-maquila",
            "Tiendas Naturales (Minisuper)": "payment-type-1",
            "Tiendas Naturales (Supermercados)": "payment-type-1",
            "Tiendas Naturas (Macobioticas)": "payment-type-1",
            "Tienda Online (Terceros)": "payment-type-1",
        }
        channels = {self.default_channel.name: self.default_channel}
        for channel_name in expected_ids:
            if channel_name not in channels:
                channels[channel_name] = self._channel(channel_name)

        resolutions = {}
        for index, (channel_name, expected_payment_type_id) in enumerate(
            expected_ids.items(),
            start=1,
        ):
            order = self._order(
                f"B2B-CHANNEL-{index}",
                channel_id=channels[channel_name].id,
            )
            resolution = resolve_b2b_loyverse_payment_type_mapping(self.db, order)
            resolutions[channel_name] = resolution
            self.assertTrue(resolution["ready"])
            self.assertEqual(
                resolution["loyverse_payment_type_id"],
                expected_payment_type_id,
            )
            self.assertEqual(
                resolution["loyverse_payment_type_name"],
                B2B_CHANNEL_TO_LOYVERSE_PAYMENT_TYPE_NAME[channel_name],
            )

        natural_ids = {
            resolutions[channel_name]["loyverse_payment_type_id"]
            for channel_name in (
                "Tiendas Naturales (Minisuper)",
                "Tiendas Naturales (Supermercados)",
                "Tiendas Naturas (Macobioticas)",
                "Tienda Online (Terceros)",
            )
        }
        self.assertEqual(natural_ids, {"payment-type-1"})

    def test_b2b1244_uses_channel_master_even_with_empty_or_legacy_snapshot(self) -> None:
        order = self._order("B2B1244")
        order.loyverse_payment_type_id_snapshot = "legacy-payment-type-must-not-be-used"
        self.db.commit()

        preview = self._preview()
        row = next(item for item in preview.rows if item.order_number == "B2B1244")
        readiness = build_b2b_invoice_readiness(self.db, order.id)

        self.assertTrue(row.eligible)
        self.assertFalse(row.missing_payment_type_mapping)
        self.assertTrue(readiness["channel"]["ready"])
        self.assertEqual(
            readiness["channel"]["channel_name"],
            "Tiendas Naturales (Supermercados)",
        )
        self.assertEqual(
            readiness["channel"]["loyverse_payment_type_id"],
            "payment-type-1",
        )

    def test_missing_unapproved_or_unavailable_channel_mapping_is_specific(self) -> None:
        without_channel = self._order("B2B-NO-CHANNEL", without_channel=True)
        without_channel.b2b_channel_name_snapshot = "Tiendas Naturales (Supermercados)"
        without_channel.loyverse_payment_type_id_snapshot = "payment-type-1"
        unmapped_channel = self._channel("Canal B2B sin mapeo")
        unmapped = self._order("B2B-UNMAPPED-CHANNEL", channel_id=unmapped_channel.id)
        wrong_scope_channel = self._channel("Canal solo B2C", applies_to_b2b=False)
        wrong_scope = self._order("B2B-WRONG-SCOPE", channel_id=wrong_scope_channel.id)

        missing_resolution = resolve_b2b_loyverse_payment_type_mapping(
            self.db,
            without_channel,
        )
        unmapped_resolution = resolve_b2b_loyverse_payment_type_mapping(self.db, unmapped)
        wrong_scope_resolution = resolve_b2b_loyverse_payment_type_mapping(
            self.db,
            wrong_scope,
        )

        self.assertFalse(missing_resolution["ready"])
        self.assertIn("no Channel Master", missing_resolution["message"])
        self.assertFalse(unmapped_resolution["ready"])
        self.assertIn("no approved", unmapped_resolution["message"])
        self.assertFalse(wrong_scope_resolution["ready"])
        self.assertIn("not applicable to B2B", wrong_scope_resolution["message"])

        natural_mapping = (
            self.db.query(LoyversePaymentTypeMapping)
            .filter(LoyversePaymentTypeMapping.name == "Tiendas Naturales")
            .one()
        )
        natural_mapping.active = False
        self.db.commit()
        unavailable = resolve_b2b_loyverse_payment_type_mapping(
            self.db,
            self._order("B2B-PAYMENT-TYPE-UNAVAILABLE"),
        )
        self.assertFalse(unavailable["ready"])
        self.assertIn("not active", unavailable["message"])

    def test_receipt_payload_has_one_payment_for_exact_order_total(self) -> None:
        from tools import b2b_loyverse_monthly_invoice_job as monthly

        order = self._order("B2B-PAYMENT-PAYLOAD")
        evaluation = monthly.evaluate_order(self.db, order, "store-1")

        self.assertTrue(evaluation.eligible)
        self.assertIsNotNone(evaluation.payload)
        payments = evaluation.payload["payments"]
        self.assertEqual(len(payments), 1)
        self.assertEqual(payments[0]["payment_type_id"], "payment-type-1")
        self.assertEqual(payments[0]["money_amount"], 2000.0)
        self.assertEqual(evaluation.total_amount, Decimal("2000"))

    def test_missing_or_ambiguous_customer_mapping_stays_blocked(self) -> None:
        self.customer.loyverse_customer_id = None
        refreshed_at = datetime(2026, 10, 1)
        self.db.add_all(
            [
                LoyverseCustomerMapping(
                    loyverse_customer_id="ambiguous-customer-1",
                    customer_name="Ambiguous one",
                    phone="71111111",
                    active=True,
                    last_refreshed_at=refreshed_at,
                ),
                LoyverseCustomerMapping(
                    loyverse_customer_id="ambiguous-customer-2",
                    customer_name="Ambiguous two",
                    phone="7111-1111",
                    active=True,
                    last_refreshed_at=refreshed_at,
                ),
                LoyverseCustomerMapping(
                    loyverse_customer_id="unique-customer",
                    customer_name="Unique customer",
                    phone="7222-2222",
                    active=True,
                    last_refreshed_at=refreshed_at,
                ),
            ]
        )
        self.db.commit()
        self._order(
            "B2B-CUSTOMER-NOT-FOUND",
            loyverse_customer_id_snapshot=None,
            phone_snapshot="70000000",
        )
        self._order(
            "B2B-CUSTOMER-AMBIGUOUS",
            loyverse_customer_id_snapshot=None,
            phone_snapshot="71111111",
        )
        self._order(
            "B2B-CUSTOMER-UNIQUE-PHONE",
            loyverse_customer_id_snapshot=None,
            phone_snapshot="72222222",
        )

        preview = self._preview()
        rows = {row.order_number: row for row in preview.rows}

        self.assertEqual(preview.missing_customer_mapping_count, 2)
        self.assertTrue(rows["B2B-CUSTOMER-NOT-FOUND"].missing_customer_mapping)
        self.assertIn(
            "No local Loyverse customer mapping found",
            rows["B2B-CUSTOMER-NOT-FOUND"].reason,
        )
        self.assertTrue(rows["B2B-CUSTOMER-AMBIGUOUS"].missing_customer_mapping)
        self.assertIn(
            "Multiple local Loyverse customer mappings",
            rows["B2B-CUSTOMER-AMBIGUOUS"].reason,
        )
        self.assertTrue(rows["B2B-CUSTOMER-UNIQUE-PHONE"].eligible)

    def test_preview_only_makes_invoiced_orders_eligible(self) -> None:
        self._order("B2B-ELIGIBLE")
        self._order("B2B-DRAFT", status="draft")
        self._order("B2B-CANCELLED", status="cancelled")
        self._order("B2B-SENT", sync_status="success", receipt_number="R-100")
        self._order("B2B-UNKNOWN", sync_status="unknown")

        preview = self._preview()
        rows = {row.order_number: row for row in preview.rows}

        self.assertEqual(preview.total_found, 3)
        self.assertEqual(preview.eligible_count, 1)
        self.assertEqual(preview.already_sent_count, 1)
        self.assertEqual(preview.excluded_count, 0)
        self.assertEqual(preview.blocked_count, 1)
        self.assertEqual(preview.eligible_amount, Decimal("2000"))
        self.assertTrue(rows["B2B-ELIGIBLE"].eligible)
        self.assertNotIn("B2B-DRAFT", rows)
        self.assertNotIn("B2B-CANCELLED", rows)
        self.assertEqual(rows["B2B-SENT"].classification, "already_sent")
        self.assertEqual(rows["B2B-UNKNOWN"].classification, "blocked")

    def test_preview_uses_real_invoice_date_and_never_delivery_date(self) -> None:
        included = self._order(
            "B2B-INVOICE-DATE-IN",
            delivery_date=date(2026, 9, 15),
            invoice_date=date(2026, 10, 15),
        )
        self._order(
            "B2B-DELIVERY-DATE-ONLY-IN",
            delivery_date=date(2026, 10, 15),
            invoice_date=date(2026, 9, 30),
        )
        legacy = self._order(
            "B2B-INVOICED-AT-IN",
            delivery_date=date(2026, 9, 1),
            invoiced_at=datetime(2026, 10, 20, 10, 0),
        )

        preview = self._preview()
        rows = {row.order_number: row for row in preview.rows}

        self.assertEqual(set(rows), {included.order_number, legacy.order_number})
        self.assertEqual(rows[included.order_number].invoice_date_source, "invoice_date")
        self.assertEqual(rows[legacy.order_number].invoice_date_source, "invoiced_at")

    def test_duplicate_receipt_references_are_blocked(self) -> None:
        self._order("B2B-DUP-1", sync_status="success", receipt_id="same-receipt")
        self._order("B2B-DUP-2", sync_status="success", receipt_id="same-receipt")

        preview = self._preview()

        self.assertEqual(preview.blocked_count, 2)
        self.assertTrue(all("repetida" in row.reason for row in preview.rows))

    def test_repeated_preview_reports_button_success_as_sent_and_only_adds_new_pending(self) -> None:
        sent = self._order(
            "B2B-BUTTON-SENT",
            sync_status="success",
            receipt_id="receipt-button-1",
            receipt_number="R-BUTTON-1",
        )
        self.db.add(
            B2BLoyverseInvoiceDispatch(
                sales_order_id=sent.id,
                status="success",
                start_date=date(2026, 10, 1),
                end_date=date(2026, 10, 20),
                plan_fingerprint="a" * 64,
                payload_fingerprint="b" * 64,
                queued_by_user_id=self.user.id,
                completed_at=datetime.utcnow(),
            )
        )
        self.db.commit()

        repeated = self._preview()
        self.assertEqual(repeated.eligible_count, 0)
        self.assertEqual(repeated.already_sent_count, 1)
        self.assertEqual(repeated.blocked_count, 0)

        self._order("B2B-NEW-PENDING", delivery_date=date(2026, 10, 29))
        incremental = self._preview()
        rows = {row.order_number: row for row in incremental.rows}

        self.assertEqual(incremental.eligible_count, 1)
        self.assertEqual(incremental.already_sent_count, 1)
        self.assertFalse(rows["B2B-BUTTON-SENT"].eligible)
        self.assertEqual(rows["B2B-BUTTON-SENT"].classification, "already_sent")
        self.assertTrue(rows["B2B-NEW-PENDING"].eligible)

    def test_missing_mapping_is_reported_without_writes(self) -> None:
        order = self._order("B2B-MISSING-MAPPING")
        order.lines[0].sku_snapshot = "SKU-SIN-MAPEO"
        self.db.commit()

        preview = self._preview()

        self.assertEqual(preview.blocked_count, 1)
        self.assertEqual(preview.missing_variant_mapping_count, 1)
        self.db.refresh(order)
        self.assertIsNone(order.loyverse_invoice_sync_status)
        self.assertEqual(self.db.query(B2BLoyverseInvoiceDispatch).count(), 0)
        self.assertEqual(self.db.query(AuditLog).count(), 0)

    def test_definite_failure_requires_new_preview_but_can_be_requeued(self) -> None:
        order = self._order("B2B-DEFINITE-FAILURE", sync_status="failed")
        dispatch = B2BLoyverseInvoiceDispatch(
            sales_order_id=order.id,
            status="failed",
            start_date=date(2026, 9, 1),
            end_date=date(2026, 9, 30),
            plan_fingerprint="a" * 64,
            payload_fingerprint="b" * 64,
            queued_by_user_id=self.user.id,
            completed_at=datetime.utcnow(),
            last_error="Definite rejected request",
        )
        self.db.add(dispatch)
        self.db.commit()

        preview = self._preview()
        self.assertEqual(preview.eligible_count, 1)
        queue_b2b_loyverse_invoicing_preview(
            self.db,
            preview=preview,
            user=self.user,
            request=self._request(),
        )
        self.db.commit()

        self.db.expire_all()
        retried = self.db.query(B2BLoyverseInvoiceDispatch).one()
        self.assertEqual(retried.status, "queued")
        self.assertEqual(retried.start_date, date(2026, 10, 1))
        self.assertEqual(retried.end_date, date(2026, 10, 29))
        self.assertIsNone(retried.completed_at)

    def test_csrf_and_plan_tokens_are_bound_to_session_and_plan(self) -> None:
        csrf = build_b2b_loyverse_invoicing_csrf_token("session-a")
        validate_b2b_loyverse_invoicing_csrf_token("session-a", csrf)
        with self.assertRaises(B2BLoyverseInvoicingSecurityError):
            validate_b2b_loyverse_invoicing_csrf_token("session-b", csrf)

        token = build_b2b_loyverse_invoicing_confirmation_token(
            "session-a",
            start_date=date(2026, 10, 1),
            end_date=date(2026, 10, 29),
            plan_fingerprint="a" * 64,
        )
        validate_b2b_loyverse_invoicing_confirmation_token(
            "session-a",
            token,
            start_date=date(2026, 10, 1),
            end_date=date(2026, 10, 29),
            plan_fingerprint="a" * 64,
        )
        with self.assertRaises(B2BLoyverseInvoicingStalePlanError):
            validate_b2b_loyverse_invoicing_confirmation_token(
                "session-a",
                token,
                start_date=date(2026, 10, 1),
                end_date=date(2026, 10, 29),
                plan_fingerprint="b" * 64,
            )

    def test_confirmation_queues_only_approved_rows_and_is_audited_atomically(self) -> None:
        eligible = self._order("B2B-QUEUE")
        excluded = self._order("B2B-NO-QUEUE", status="draft")
        preview = self._preview()

        queued = queue_b2b_loyverse_invoicing_preview(
            self.db,
            preview=preview,
            user=self.user,
            request=self._request(),
        )
        self.db.commit()

        self.assertEqual(queued, 1)
        self.db.refresh(eligible)
        self.db.refresh(excluded)
        self.assertEqual(eligible.loyverse_invoice_sync_status, "queued")
        self.assertIsNone(excluded.loyverse_invoice_sync_status)
        dispatch = self.db.query(B2BLoyverseInvoiceDispatch).one()
        self.assertEqual(dispatch.sales_order_id, eligible.id)
        self.assertEqual(dispatch.status, "queued")
        self.assertEqual(dispatch.plan_fingerprint, preview.plan_fingerprint)
        self.assertEqual(
            self.db.query(AuditLog)
            .filter(AuditLog.action == "b2b_loyverse_invoice_queued")
            .count(),
            1,
        )
        self.assertEqual(eligible.status, "invoiced")

    def test_confirmation_can_rollback_queue_and_audit_together(self) -> None:
        order = self._order("B2B-ROLLBACK")
        preview = self._preview()

        with patch(
            "app.services.b2b_loyverse_invoicing_workflow_service.add_audit_event_in_session",
            side_effect=RuntimeError("audit failed"),
        ):
            with self.assertRaisesRegex(RuntimeError, "audit failed"):
                queue_b2b_loyverse_invoicing_preview(
                    self.db,
                    preview=preview,
                    user=self.user,
                    request=self._request(),
                )
        self.db.rollback()
        self.db.expire_all()

        stored = self.db.query(B2BSalesOrder).filter(B2BSalesOrder.id == order.id).one()
        self.assertIsNone(stored.loyverse_invoice_sync_status)
        self.assertEqual(self.db.query(B2BLoyverseInvoiceDispatch).count(), 0)
        self.assertEqual(self.db.query(AuditLog).count(), 0)

    def test_second_preview_cannot_queue_same_order_again(self) -> None:
        self._order("B2B-ONCE")
        first = self._preview()
        queue_b2b_loyverse_invoicing_preview(
            self.db,
            preview=first,
            user=self.user,
            request=self._request(),
        )
        self.db.commit()

        second = self._preview()

        self.assertEqual(second.eligible_count, 0)
        self.assertEqual(second.blocked_count, 1)
        self.assertNotEqual(first.plan_fingerprint, second.plan_fingerprint)

    def test_repeated_schema_initialization_preserves_dispatch_data(self) -> None:
        order = self._order("B2B-SCHEMA-IDEMPOTENT")
        preview = self._preview()
        queue_b2b_loyverse_invoicing_preview(
            self.db,
            preview=preview,
            user=self.user,
            request=self._request(),
        )
        self.db.commit()
        dispatch_id = self.db.query(B2BLoyverseInvoiceDispatch.id).scalar()

        Base.metadata.create_all(self.engine)

        self.assertEqual(self.db.query(B2BLoyverseInvoiceDispatch).count(), 1)
        self.assertEqual(self.db.query(B2BLoyverseInvoiceDispatch.id).scalar(), dispatch_id)
        self.assertEqual(
            self.db.query(B2BLoyverseInvoiceDispatch).one().sales_order_id,
            order.id,
        )

    def test_worker_persists_success_reference_without_changing_erp_invoice(self) -> None:
        order = self._order("B2B-WORKER")
        preview = self._preview()
        queue_b2b_loyverse_invoicing_preview(
            self.db,
            preview=preview,
            user=self.user,
            request=self._request(),
        )
        self.db.commit()

        def fake_execute_one(session_factory, evaluation, **_kwargs):
            db = session_factory()
            try:
                stored = db.query(B2BSalesOrder).filter(B2BSalesOrder.id == evaluation.order_id).one()
                stored.loyverse_receipt_id = "receipt-id-1"
                stored.loyverse_receipt_number = "receipt-number-1"
                stored.loyverse_invoice_sync_status = "success"
                stored.loyverse_invoice_synced_at = datetime.utcnow()
                db.commit()
            finally:
                db.close()
            return (
                {
                    "result": "success",
                    "message": "Receipt created and local reference committed.",
                },
                None,
            )

        with patch(
            "app.services.b2b_loyverse_invoicing_workflow_service.execute_one",
            side_effect=fake_execute_one,
        ), patch(
            "app.services.b2b_loyverse_invoicing_workflow_service.safe_log_audit_event"
        ) as audit_result:
            result = run_b2b_loyverse_invoice_worker_cycle(self.Session, max_orders=1)

        self.assertEqual(result["success"], 1)
        audit_result.assert_called_once()
        self.db.expire_all()
        stored = self.db.query(B2BSalesOrder).filter(B2BSalesOrder.id == order.id).one()
        dispatch = self.db.query(B2BLoyverseInvoiceDispatch).one()
        self.assertEqual(stored.status, "invoiced")
        self.assertEqual(stored.loyverse_receipt_number, "receipt-number-1")
        self.assertEqual(dispatch.status, "success")

    def test_changed_payload_is_blocked_before_external_call(self) -> None:
        order = self._order("B2B-CHANGED")
        preview = self._preview()
        queue_b2b_loyverse_invoicing_preview(
            self.db,
            preview=preview,
            user=self.user,
            request=self._request(),
        )
        self.db.commit()
        order.lines[0].unit_price_snapshot = Decimal("1100")
        order.lines[0].line_total = Decimal("2200")
        self.db.commit()

        with patch(
            "app.services.b2b_loyverse_invoicing_workflow_service.execute_one"
        ) as execute:
            result = run_b2b_loyverse_invoice_worker_cycle(self.Session, max_orders=1)

        execute.assert_not_called()
        self.assertEqual(result["failed"], 1)
        self.db.expire_all()
        self.assertEqual(self.db.query(B2BLoyverseInvoiceDispatch).one().status, "failed")
        self.assertEqual(
            self.db.query(B2BSalesOrder).filter(B2BSalesOrder.id == order.id).one().status,
            "invoiced",
        )
        refreshed_preview = self._preview()
        self.assertEqual(refreshed_preview.eligible_count, 1)

    def test_stale_processing_with_unknown_result_is_never_retried(self) -> None:
        order = self._order("B2B-UNKNOWN-RECOVERY", sync_status="unknown")
        dispatch = B2BLoyverseInvoiceDispatch(
            sales_order_id=order.id,
            status="processing",
            start_date=date(2026, 10, 1),
            end_date=date(2026, 10, 29),
            plan_fingerprint="a" * 64,
            payload_fingerprint="b" * 64,
            queued_by_user_id=self.user.id,
            processing_started_at=datetime.utcnow() - timedelta(minutes=20),
        )
        self.db.add(dispatch)
        self.db.commit()

        with patch(
            "app.services.b2b_loyverse_invoicing_workflow_service.execute_one"
        ) as execute:
            result = run_b2b_loyverse_invoice_worker_cycle(self.Session, max_orders=1)

        execute.assert_not_called()
        self.assertEqual(result["recovered"], 1)
        self.assertEqual(result["processed"], 0)
        self.db.expire_all()
        self.assertEqual(self.db.query(B2BLoyverseInvoiceDispatch).one().status, "unknown")

    def test_required_invoiced_status_is_revalidated_inside_legacy_executor(self) -> None:
        from tools import b2b_loyverse_monthly_invoice_job as monthly

        order = self._order("B2B-STATUS-RACE")
        evaluation = monthly.evaluate_order(self.db, order, "store-1")
        order.status = "draft"
        self.db.commit()

        with patch.object(monthly, "_create_loyverse_receipt") as create_receipt:
            result, error = monthly.execute_one(
                self.Session,
                evaluation,
                start_date=date(2026, 10, 1),
                end_date_exclusive=date(2026, 10, 30),
                token="test-token",
                store_id="store-1",
                timezone=timezone.utc,
                fallback_payment_type_id="",
                fallback_payment_type_name="",
                customer_override_erp_id=None,
                customer_override_loyverse_id="",
                customer_override_loyverse_name="",
                excluded_order_numbers=set(),
                required_order_status="invoiced",
            )

        create_receipt.assert_not_called()
        self.assertEqual(result["result"], "failed")
        self.assertIn("required status", error["error"])

    def test_legacy_executor_revalidates_approved_invoice_date_range(self) -> None:
        from tools import b2b_loyverse_monthly_invoice_job as monthly

        order = self._order(
            "B2B-INVOICE-DATE-RACE",
            delivery_date=date(2026, 10, 15),
            invoice_date=date(2026, 10, 15),
        )
        evaluation = monthly.evaluate_order(self.db, order, "store-1")
        order.invoice_date = date(2026, 9, 30)
        self.db.commit()

        with patch.object(monthly, "_create_loyverse_receipt") as create_receipt:
            result, error = monthly.execute_one(
                self.Session,
                evaluation,
                start_date=date(2026, 10, 1),
                end_date_exclusive=date(2026, 10, 30),
                token="test-token",
                store_id="store-1",
                timezone=timezone.utc,
                fallback_payment_type_id="",
                fallback_payment_type_name="",
                customer_override_erp_id=None,
                customer_override_loyverse_id="",
                customer_override_loyverse_name="",
                excluded_order_numbers=set(),
                required_order_status="invoiced",
                approved_date_field="invoice_date",
            )

        create_receipt.assert_not_called()
        self.assertEqual(result["result"], "failed")
        self.assertIn("approved invoice_date range", error["error"])


class B2BLoyverseInvoicingIntegrationSurfaceTests(unittest.TestCase):
    def test_dispatch_table_ddl_is_postgresql_compatible_and_unique_per_order(self) -> None:
        ddl = str(
            CreateTable(B2BLoyverseInvoiceDispatch.__table__).compile(
                dialect=postgresql.dialect()
            )
        )
        self.assertIn("b2b_loyverse_invoice_dispatches", ddl)
        self.assertIn("uq_b2b_loyverse_invoice_dispatches_order", ddl)
        self.assertIn("ck_b2b_loyverse_invoice_dispatches_status", ddl)

    def test_routes_enforce_backend_permission_csrf_origin_and_revalidation(self) -> None:
        main_source = Path("app/main.py").read_text(encoding="utf-8")
        get_start = main_source.index("def b2b_loyverse_invoicing_preview_page(")
        post_start = main_source.index("async def confirm_b2b_loyverse_invoicing(")
        next_route = main_source.index('@app.get("/b2b/orders/import"', post_start)
        get_source = main_source[get_start:post_start]
        post_source = main_source[post_start:next_route]
        self.assertIn('require_permission(request, "sales.invoice")', get_source)
        self.assertIn('require_permission(request, "sales.invoice")', post_source)
        self.assertIn("validate_role_permissions_request_origin(", post_source)
        self.assertIn("validate_b2b_loyverse_invoicing_csrf_token(", post_source)
        self.assertIn("lock_b2b_loyverse_invoicing_range(", post_source)
        self.assertIn("build_b2b_loyverse_invoicing_preview(", post_source)
        self.assertIn("compare_digest", post_source)

    def test_templates_parse_and_expose_only_existing_permission(self) -> None:
        environment = Environment(loader=FileSystemLoader("app/templates"))
        environment.get_template("b2b_loyverse_invoicing.html")
        environment.get_template("b2b_orders_list.html")
        environment.get_template("b2b_order_detail.html")
        template = Path("app/templates/b2b_orders_list.html").read_text(encoding="utf-8")
        self.assertIn('can(request, "sales.invoice")', template)
        self.assertIn("Facturar en Loyverse", template)

    def test_worker_is_disabled_by_default_and_has_no_startup_side_effect(self) -> None:
        from app.services.b2b_loyverse_invoicing_workflow_service import (
            b2b_loyverse_invoice_worker_enabled,
        )

        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(b2b_loyverse_invoice_worker_enabled())


if __name__ == "__main__":
    unittest.main()
