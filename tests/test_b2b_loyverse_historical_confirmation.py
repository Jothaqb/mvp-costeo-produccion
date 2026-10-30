import csv
import json
import tempfile
import unittest
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.database import Base
from app.models import (
    AuditLog,
    B2BCustomer,
    B2BSalesOrder,
    Permission,
    Role,
    RolePermission,
    User,
    UserRole,
)
from app.services.b2b_loyverse_historical_confirmation_service import (
    HISTORICAL_CONFIRMATION_TEXT,
    B2BLoyverseHistoricalConfirmationError,
    build_historical_confirmation_plan,
    execute_historical_confirmation,
)
from app.services.b2b_loyverse_invoice_service import (
    SYNC_STATUS_HISTORICAL_CONFIRMED,
)
from app.services.b2b_loyverse_invoicing_workflow_service import loyverse_integration_status
from tools.b2b_loyverse_daily_invoice_job import evaluate_daily_order
from tools.b2b_loyverse_monthly_invoice_job import evaluate_order
from tools.mark_b2b_loyverse_historical_invoices import _write_evidence


class B2BLoyverseHistoricalConfirmationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, autocommit=False, autoflush=False)
        self.db = self.Session()
        self.customer = B2BCustomer(
            customer_name="Cliente histórico",
            phone="88888888",
            active=True,
        )
        self.user = User(
            username="historical-admin",
            full_name="Historical Admin",
            email="historical@example.test",
            password_hash="test-only",
            is_active=True,
        )
        permission = Permission(
            code="sales.invoice",
            module="sales",
            action="invoice",
            description="Invoice sales",
            active=True,
        )
        role = Role(code="historical-role", name="Historical role", active=True)
        role.permission_links.append(RolePermission(permission=permission))
        self.user.user_role_links.append(UserRole(role=role))
        self.db.add_all([self.customer, self.user])
        self.db.commit()

    def tearDown(self) -> None:
        self.db.close()
        Base.metadata.drop_all(self.engine)
        self.engine.dispose()

    def _order(
        self,
        number: str,
        *,
        invoice_date: date | None,
        invoiced_at: datetime | None = None,
        sync_status: str | None = None,
        receipt_id: str | None = None,
        receipt_number: str | None = None,
        observations: str | None = None,
        status: str = "invoiced",
    ) -> B2BSalesOrder:
        order = B2BSalesOrder(
            order_number=number,
            customer_id=self.customer.id,
            customer_name_snapshot=self.customer.customer_name,
            delivery_date=date(2026, 9, 15),
            invoice_date=invoice_date,
            invoiced_at=invoiced_at,
            status=status,
            total_amount=Decimal("1000"),
            observations=observations,
            loyverse_invoice_sync_status=sync_status,
            loyverse_receipt_id=receipt_id,
            loyverse_receipt_number=receipt_number,
        )
        self.db.add(order)
        self.db.commit()
        self.db.refresh(order)
        return order

    def test_dry_plan_uses_only_reliable_invoice_dates_and_reports_exceptions(self) -> None:
        self._order("HIST-EXPLICIT", invoice_date=date(2026, 9, 1))
        self._order(
            "HIST-INVOICED-AT",
            invoice_date=None,
            invoiced_at=datetime(2026, 9, 2, 10, 0),
        )
        self._order("HIST-UNRELIABLE", invoice_date=None)
        self._order(
            "HIST-DATE-CONFLICT",
            invoice_date=date(2026, 9, 4),
            invoiced_at=datetime(2026, 9, 5, 10, 0),
        )
        self._order("CURRENT", invoice_date=date(2026, 10, 1))
        self._order("HIST-UNKNOWN", invoice_date=date(2026, 9, 3), sync_status="unknown")

        before_audits = self.db.query(AuditLog).count()
        plan = build_historical_confirmation_plan(self.db)
        self.db.rollback()
        rows = {row.order_number: row for row in plan.rows}

        self.assertEqual(rows["HIST-EXPLICIT"].classification, "candidate_dated_historical")
        self.assertEqual(rows["HIST-EXPLICIT"].invoice_date_source, "invoice_date")
        self.assertEqual(rows["HIST-INVOICED-AT"].classification, "candidate_dated_historical")
        self.assertEqual(rows["HIST-INVOICED-AT"].invoice_date_source, "invoiced_at")
        self.assertEqual(rows["HIST-UNRELIABLE"].classification, "review_unreliable_date")
        self.assertEqual(rows["HIST-DATE-CONFLICT"].classification, "review_date_conflict")
        self.assertEqual(rows["CURRENT"].classification, "outside_cutoff")
        self.assertEqual(rows["HIST-UNKNOWN"].classification, "review_unknown")
        self.assertEqual(self.db.query(AuditLog).count(), before_audits)

    def test_migrated_import_marker_is_required_for_missing_date_exception(self) -> None:
        migrated = self._order(
            "MIGRATED-NO-DATE",
            invoice_date=None,
            observations="Historical CSV import\nlegacy source 1594",
            receipt_id="existing-receipt-id",
            receipt_number="EXISTING-1594",
        )
        unmarked = self._order(
            "UNMARKED-NO-DATE",
            invoice_date=None,
            observations="Normal administrative note",
        )
        october_import = self._order(
            "MIGRATED-OCTOBER",
            invoice_date=date(2026, 10, 1),
            observations="Historical CSV import",
        )
        unknown_import = self._order(
            "MIGRATED-UNKNOWN",
            invoice_date=None,
            observations="Historical CSV import 1600",
            sync_status="unknown",
        )

        plan = build_historical_confirmation_plan(self.db)
        rows = {row.order_number: row for row in plan.rows}

        self.assertEqual(rows[migrated.order_number].classification, "candidate_migrated_import")
        self.assertEqual(rows[unmarked.order_number].classification, "review_unreliable_date")
        self.assertEqual(rows[october_import.order_number].classification, "outside_cutoff")
        self.assertEqual(rows[unknown_import.order_number].classification, "review_unknown")
        self.assertEqual(plan.migrated_import_candidate_count, 1)
        self.assertEqual(plan.dated_historical_candidate_count, 0)

        executed = execute_historical_confirmation(
            self.Session,
            confirmed_by_user_id=self.user.id,
            expected_fingerprint=plan.fingerprint,
            business_confirmation=HISTORICAL_CONFIRMATION_TEXT,
        )

        self.assertEqual(executed.candidate_count, 1)
        self.db.expire_all()
        stored_migrated = self.db.get(B2BSalesOrder, migrated.id)
        self.assertEqual(stored_migrated.status, "invoiced")
        self.assertEqual(stored_migrated.total_amount, Decimal("1000"))
        self.assertIsNone(stored_migrated.invoice_date)
        self.assertIsNone(stored_migrated.invoiced_at)
        self.assertEqual(stored_migrated.loyverse_receipt_id, "existing-receipt-id")
        self.assertEqual(stored_migrated.loyverse_receipt_number, "EXISTING-1594")
        self.assertEqual(
            stored_migrated.loyverse_invoice_sync_status,
            SYNC_STATUS_HISTORICAL_CONFIRMED,
        )
        self.assertIsNone(
            self.db.get(B2BSalesOrder, unmarked.id).loyverse_invoice_sync_status
        )
        self.assertIsNone(
            self.db.get(B2BSalesOrder, october_import.id).loyverse_invoice_sync_status
        )
        self.assertEqual(
            self.db.get(B2BSalesOrder, unknown_import.id).loyverse_invoice_sync_status,
            "unknown",
        )

    def test_dry_run_exports_migrated_dated_review_and_october_separately(self) -> None:
        self._order(
            "EXPORT-MIGRATED",
            invoice_date=None,
            observations="Historical CSV import",
        )
        self._order("EXPORT-DATED", invoice_date=date(2026, 9, 1))
        self._order("EXPORT-REVIEW", invoice_date=None)
        self._order("EXPORT-OCTOBER", invoice_date=date(2026, 10, 1))
        plan = build_historical_confirmation_plan(self.db)

        with tempfile.TemporaryDirectory() as directory:
            export_dir = Path(directory) / "evidence"
            _write_evidence(export_dir, plan)
            summary = json.loads((export_dir / "summary.json").read_text(encoding="utf-8"))

            self.assertEqual(summary["migrated_import_candidate_count"], 1)
            self.assertEqual(summary["dated_historical_candidate_count"], 1)
            expected = {
                "orders_migrated_identified.csv": "EXPORT-MIGRATED",
                "orders_historical_normal.csv": "EXPORT-DATED",
                "orders_review.csv": "EXPORT-REVIEW",
                "orders_from_october.csv": "EXPORT-OCTOBER",
            }
            for file_name, expected_order_number in expected.items():
                with (export_dir / file_name).open(encoding="utf-8-sig", newline="") as source:
                    rows = list(csv.DictReader(source))
                self.assertEqual([row["order_number"] for row in rows], [expected_order_number])

    def test_execute_marks_candidates_preserves_receipts_and_audits_business_confirmation(self) -> None:
        no_receipt = self._order("HIST-NO-RECEIPT", invoice_date=date(2026, 9, 1))
        with_receipt = self._order(
            "HIST-WITH-RECEIPT",
            invoice_date=date(2026, 9, 2),
            sync_status="success",
            receipt_id="real-receipt-id",
            receipt_number="R-REAL",
        )
        unknown = self._order(
            "HIST-UNKNOWN",
            invoice_date=date(2026, 9, 3),
            sync_status="unknown",
        )
        plan = build_historical_confirmation_plan(self.db)

        executed = execute_historical_confirmation(
            self.Session,
            confirmed_by_user_id=self.user.id,
            expected_fingerprint=plan.fingerprint,
            business_confirmation=HISTORICAL_CONFIRMATION_TEXT,
        )

        self.assertEqual(executed.candidate_count, 2)
        self.db.expire_all()
        stored_no_receipt = self.db.get(B2BSalesOrder, no_receipt.id)
        stored_with_receipt = self.db.get(B2BSalesOrder, with_receipt.id)
        stored_unknown = self.db.get(B2BSalesOrder, unknown.id)
        self.assertEqual(stored_no_receipt.status, "invoiced")
        self.assertEqual(
            stored_no_receipt.loyverse_invoice_sync_status,
            SYNC_STATUS_HISTORICAL_CONFIRMED,
        )
        self.assertIsNone(stored_no_receipt.loyverse_receipt_id)
        self.assertEqual(
            stored_with_receipt.loyverse_invoice_sync_status,
            SYNC_STATUS_HISTORICAL_CONFIRMED,
        )
        self.assertEqual(stored_with_receipt.loyverse_receipt_id, "real-receipt-id")
        self.assertEqual(stored_with_receipt.loyverse_receipt_number, "R-REAL")
        self.assertEqual(stored_unknown.loyverse_invoice_sync_status, "unknown")
        self.assertEqual(
            loyverse_integration_status(stored_no_receipt)["label"],
            "Facturado en Loyverse — Histórico",
        )
        self.assertEqual(
            self.db.query(AuditLog)
            .filter(AuditLog.action == "b2b_loyverse_historical_invoice_confirmed")
            .count(),
            2,
        )
        audit = (
            self.db.query(AuditLog)
            .filter(AuditLog.action == "b2b_loyverse_historical_invoice_confirmed")
            .first()
        )
        self.assertEqual(audit.notes, HISTORICAL_CONFIRMATION_TEXT)
        self.assertEqual(audit.user_id, self.user.id)

    def test_stale_plan_and_audit_failure_roll_back_all_status_changes(self) -> None:
        first = self._order("HIST-ROLLBACK-1", invoice_date=date(2026, 9, 1))
        second = self._order("HIST-ROLLBACK-2", invoice_date=date(2026, 9, 2))
        plan = build_historical_confirmation_plan(self.db)

        with self.assertRaises(B2BLoyverseHistoricalConfirmationError):
            execute_historical_confirmation(
                self.Session,
                confirmed_by_user_id=self.user.id,
                expected_fingerprint="0" * 64,
                business_confirmation=HISTORICAL_CONFIRMATION_TEXT,
            )

        with patch(
            "app.services.b2b_loyverse_historical_confirmation_service.add_audit_event_in_session",
            side_effect=RuntimeError("audit unavailable"),
        ):
            with self.assertRaisesRegex(RuntimeError, "audit unavailable"):
                execute_historical_confirmation(
                    self.Session,
                    confirmed_by_user_id=self.user.id,
                    expected_fingerprint=plan.fingerprint,
                    business_confirmation=HISTORICAL_CONFIRMATION_TEXT,
                )

        self.db.expire_all()
        self.assertIsNone(self.db.get(B2BSalesOrder, first.id).loyverse_invoice_sync_status)
        self.assertIsNone(self.db.get(B2BSalesOrder, second.id).loyverse_invoice_sync_status)
        self.assertEqual(self.db.query(AuditLog).count(), 0)

    def test_historical_status_blocks_legacy_jobs_without_a_receipt(self) -> None:
        order = self._order(
            "HIST-BLOCK-LEGACY",
            invoice_date=date(2026, 9, 1),
            sync_status=SYNC_STATUS_HISTORICAL_CONFIRMED,
        )

        monthly = evaluate_order(self.db, order, "store-not-used")
        daily = evaluate_daily_order(
            self.db,
            order,
            "store-not-used",
            excluded_order_numbers=set(),
        )

        self.assertEqual(monthly.classification, "already_sent")
        self.assertEqual(daily.classification, "already_sent")
        self.assertFalse(monthly.eligible)
        self.assertFalse(daily.eligible)

    def test_repeating_confirmation_with_fresh_plan_makes_no_additional_writes(self) -> None:
        order = self._order("HIST-IDEMPOTENT", invoice_date=date(2026, 9, 1))
        first_plan = build_historical_confirmation_plan(self.db)
        execute_historical_confirmation(
            self.Session,
            confirmed_by_user_id=self.user.id,
            expected_fingerprint=first_plan.fingerprint,
            business_confirmation=HISTORICAL_CONFIRMATION_TEXT,
        )

        self.db.expire_all()
        second_plan = build_historical_confirmation_plan(self.db)
        self.assertEqual(second_plan.candidate_count, 0)
        self.assertEqual(second_plan.already_historical_count, 1)
        audit_count_before = self.db.query(AuditLog).count()
        status_before = self.db.get(B2BSalesOrder, order.id).loyverse_invoice_sync_status

        repeated = execute_historical_confirmation(
            self.Session,
            confirmed_by_user_id=self.user.id,
            expected_fingerprint=second_plan.fingerprint,
            business_confirmation=HISTORICAL_CONFIRMATION_TEXT,
        )

        self.db.expire_all()
        self.assertEqual(repeated.candidate_count, 0)
        self.assertEqual(self.db.query(AuditLog).count(), audit_count_before)
        self.assertEqual(
            self.db.get(B2BSalesOrder, order.id).loyverse_invoice_sync_status,
            status_before,
        )


if __name__ == "__main__":
    unittest.main()
