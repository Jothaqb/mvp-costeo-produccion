import csv
import io
import unittest
from datetime import date, datetime
from decimal import Decimal
from unittest import mock

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.database import Base
from app.models import B2BAROpeningBalance, B2BARPayment, B2BCustomer, B2BSalesOrder
from app.services import b2b_sales_service
from app.services.accounts_receivable_service import (
    _build_row,
    resolve_b2b_invoice_date,
)
from app.services.b2b_sales_historical_import_service import (
    EXPECTED_HEADERS,
    import_b2b_historical_sales_csv,
)
from tools import backfill_b2b_historical_invoice_dates as backfill


class B2BInvoiceDateTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)
        with self.Session.begin() as db:
            customer = B2BCustomer(customer_name="Spazio Verde", credit_days=30)
            db.add(customer)
            db.flush()
            self.customer_id = customer.id

    def tearDown(self) -> None:
        Base.metadata.drop_all(self.engine)
        self.engine.dispose()

    def _add_order(
        self,
        order_number: str,
        *,
        delivery_date: date = date(2026, 4, 30),
        observations: str | None = "Historical CSV import 1594",
        status: str = "invoiced",
        invoice_date: date | None = None,
        invoiced_at: datetime | None = None,
        created_at: datetime = datetime(2026, 5, 5, 2, 30, 12, 825492),
    ) -> int:
        with self.Session.begin() as db:
            order = B2BSalesOrder(
                order_number=order_number,
                customer_id=self.customer_id,
                customer_name_snapshot="Spazio Verde",
                delivery_date=delivery_date,
                observations=observations,
                status=status,
                invoice_date=invoice_date,
                invoiced_at=invoiced_at,
                total_amount=Decimal("100.0000"),
                created_at=created_at,
                updated_at=created_at,
            )
            db.add(order)
            db.flush()
            return order.id

    def test_invoice_date_resolver_precedence_and_negative_normal_case(self) -> None:
        with self.Session() as db:
            normal = B2BSalesOrder(
                order_number="NORMAL",
                customer_id=self.customer_id,
                customer_name_snapshot="Spazio Verde",
                delivery_date=date(2026, 4, 1),
                observations="Normal order",
                status="invoiced",
                total_amount=Decimal("10"),
                created_at=datetime(2026, 5, 5, 8, 0),
            )
            resolved, source = resolve_b2b_invoice_date(normal)
            self.assertEqual(resolved, date(2026, 5, 5))
            self.assertEqual(source, "created_at")
            self.assertNotEqual(resolved, normal.delivery_date)

            normal.invoiced_at = datetime(2026, 5, 6, 15, 0)
            resolved, source = resolve_b2b_invoice_date(normal)
            self.assertEqual((resolved, source), (date(2026, 5, 6), "invoiced_at"))

            normal.invoice_date = date(2026, 5, 7)
            resolved, source = resolve_b2b_invoice_date(normal)
            self.assertEqual((resolved, source), (date(2026, 5, 7), "invoice_date"))

    def test_b2b1055_uses_delivery_date_and_ar_calculations(self) -> None:
        order_id = self._add_order("B2B1055")
        with self.Session() as db:
            order = db.get(B2BSalesOrder, order_id)
            order.ar_opening_balance = B2BAROpeningBalance(
                sales_order_id=order.id,
                outstanding_amount=Decimal("100"),
                created_at=datetime(2026, 5, 5, 3, 0),
            )
            resolved, source = resolve_b2b_invoice_date(order)
            self.assertEqual(resolved, date(2026, 4, 30))
            self.assertEqual(source, "historical_delivery_date")
            self.assertNotEqual(resolved, date(2026, 5, 5))

            row = _build_row(order, date(2026, 6, 1))
            self.assertEqual(row.invoice_date, date(2026, 4, 30))
            self.assertEqual(row.days_remaining, -2)
            self.assertEqual(row.status_code, "overdue")

    def test_normal_invoicing_sets_invoice_date_from_invoiced_at(self) -> None:
        order_id = self._add_order(
            "NORMAL-INVOICE",
            status="in_process",
            observations="Normal order",
            invoice_date=None,
            invoiced_at=None,
        )
        with mock.patch.object(b2b_sales_service, "_prepare_b2b_invoice_lines", return_value=[object()]), mock.patch.object(
            b2b_sales_service, "_post_b2b_invoice_line"
        ), mock.patch.object(b2b_sales_service, "_snapshot_b2b_order_invoice_margin"):
            with self.Session() as db:
                order = b2b_sales_service.invoice_b2b_order_in_erp(db, order_id)
                self.assertIsNotNone(order.invoiced_at)
                self.assertEqual(order.invoice_date, order.invoiced_at.date())

    def test_future_historical_import_sets_invoice_date_from_delivery_date(self) -> None:
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=EXPECTED_HEADERS)
        writer.writeheader()
        writer.writerow(
            {
                "order_number": "HIST-FUTURE",
                "delivery_date": "2026-04-20",
                "customer_name": "Spazio Verde",
                "channel": "",
                "sku": "SKU-1",
                "description": "Product 1",
                "quantity": "1",
                "unit_price": "10",
                "line_total": "10",
                "cost_unit": "5",
                "cost_total": "5",
                "gross_profit": "5",
                "gross_profit_percent": "0.5",
                "observations": "1594",
            }
        )
        with self.Session() as db:
            result = import_b2b_historical_sales_csv(
                db,
                file_name="historical.csv",
                file_bytes=buffer.getvalue().encode("utf-8"),
            )
            self.assertEqual(result.created_orders, 1)
            order = db.query(B2BSalesOrder).filter_by(order_number="HIST-FUTURE").one()
            self.assertEqual(order.invoice_date, date(2026, 4, 20))
            self.assertIsNone(order.invoiced_at)

    def test_backfill_scope_conflicts_b2b1055_and_idempotence(self) -> None:
        b2b1055_id = self._add_order("B2B1055")
        normal_id = self._add_order("NORMAL", observations="Normal order")
        invoiced_at_id = self._add_order(
            "HIST-INVOICED-AT",
            invoiced_at=datetime(2026, 4, 30, 12, 0),
        )
        post_limit_id = self._add_order("HIST-JUNE", delivery_date=date(2026, 6, 1))
        existing_date_id = self._add_order("HIST-DATED", invoice_date=date(2026, 4, 29))
        conflict_id = self._add_order("HIST-CONFLICT")
        matching_override_id = self._add_order("HIST-MATCH")

        with self.Session.begin() as db:
            db.add_all(
                [
                    B2BAROpeningBalance(
                        sales_order_id=conflict_id,
                        outstanding_amount=Decimal("50"),
                        invoice_date_override=date(2026, 4, 29),
                        created_at=datetime(2026, 5, 5, 4, 0),
                    ),
                    B2BAROpeningBalance(
                        sales_order_id=matching_override_id,
                        outstanding_amount=Decimal("60"),
                        invoice_date_override=date(2026, 4, 30),
                        created_at=datetime(2026, 5, 5, 4, 0),
                    ),
                    B2BARPayment(
                        sales_order_id=matching_override_id,
                        payment_date=date(2026, 5, 1),
                        amount=Decimal("10"),
                        created_at=datetime(2026, 5, 5, 5, 0),
                    ),
                ]
            )

        dry_plan = backfill.dry_run_backfill(self.Session)
        self.assertEqual(dry_plan.candidate_count, 3)
        self.assertEqual(dry_plan.updatable_count, 2)
        self.assertEqual(dry_plan.anomaly_count, 1)
        self.assertEqual(dry_plan.anomalies[0].order_number, "HIST-CONFLICT")
        with self.Session() as db:
            self.assertIsNone(db.get(B2BSalesOrder, b2b1055_id).invoice_date)
            self.assertIsNone(db.get(B2BSalesOrder, matching_override_id).invoice_date)

        with self.Session() as db:
            created_at_before = db.get(B2BSalesOrder, b2b1055_id).created_at
            payment_before = db.query(B2BARPayment).one().amount
            balance_before = db.get(B2BAROpeningBalance, 1).outstanding_amount

        executed_plan = backfill.execute_backfill(self.Session)
        self.assertEqual(executed_plan.updatable_count, 2)

        with self.Session() as db:
            self.assertEqual(db.get(B2BSalesOrder, b2b1055_id).invoice_date, date(2026, 4, 30))
            self.assertEqual(db.get(B2BSalesOrder, b2b1055_id).created_at, created_at_before)
            self.assertIsNone(db.get(B2BSalesOrder, normal_id).invoice_date)
            self.assertIsNone(db.get(B2BSalesOrder, invoiced_at_id).invoice_date)
            self.assertIsNone(db.get(B2BSalesOrder, post_limit_id).invoice_date)
            self.assertEqual(db.get(B2BSalesOrder, existing_date_id).invoice_date, date(2026, 4, 29))
            self.assertIsNone(db.get(B2BSalesOrder, conflict_id).invoice_date)
            self.assertEqual(db.get(B2BSalesOrder, matching_override_id).invoice_date, date(2026, 4, 30))
            self.assertEqual(db.query(B2BARPayment).one().amount, payment_before)
            self.assertEqual(db.get(B2BAROpeningBalance, 1).outstanding_amount, balance_before)

        second_plan = backfill.execute_backfill(self.Session)
        self.assertEqual(second_plan.updatable_count, 0)
        self.assertEqual(second_plan.anomaly_count, 1)

    def test_backfill_rolls_back_completely_on_validation_failure(self) -> None:
        order_id = self._add_order("B2B1055")
        with mock.patch.object(
            backfill,
            "_validate_execution",
            side_effect=backfill.BackfillValidationError("forced validation failure"),
        ):
            with self.assertRaises(backfill.BackfillValidationError):
                backfill.execute_backfill(self.Session)

        with self.Session() as db:
            order = db.get(B2BSalesOrder, order_id)
            self.assertIsNone(order.invoice_date)
            self.assertEqual(order.created_at, datetime(2026, 5, 5, 2, 30, 12, 825492))


if __name__ == "__main__":
    unittest.main()
