import unittest
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.database import Base
from app.models import B2BAROpeningBalance, B2BARPayment, B2BCustomer, B2BSalesOrder
from app.services.accounts_receivable_service import (
    AccountsReceivableValidationError,
    _build_row,
    record_accounts_receivable_payment,
)


class AccountsReceivableBalanceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)
        with self.Session.begin() as db:
            customer = B2BCustomer(customer_name="Cliente CxC", credit_days=30)
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
        total_amount: Decimal = Decimal("100.0000"),
        invoiced_at: datetime | None = datetime(2026, 8, 10, 12, 0),
        opening_balance: Decimal | None = None,
        payments: tuple[Decimal, ...] = (),
    ) -> int:
        with self.Session.begin() as db:
            order = B2BSalesOrder(
                order_number=order_number,
                customer_id=self.customer_id,
                customer_name_snapshot="Cliente CxC",
                delivery_date=date(2026, 8, 10),
                status="invoiced",
                total_amount=total_amount,
                invoice_date=date(2026, 8, 10) if invoiced_at is not None else date(2026, 5, 1),
                invoiced_at=invoiced_at,
                observations="Normal order" if invoiced_at is not None else "Historical CSV import",
            )
            db.add(order)
            db.flush()
            if opening_balance is not None:
                db.add(
                    B2BAROpeningBalance(
                        sales_order_id=order.id,
                        outstanding_amount=opening_balance,
                    )
                )
            for index, amount in enumerate(payments, start=1):
                db.add(
                    B2BARPayment(
                        sales_order_id=order.id,
                        payment_date=date(2026, 8, 10 + index),
                        amount=amount,
                    )
                )
            db.flush()
            return order.id

    def _row(self, order_id: int, *, today: date = date(2026, 8, 10)):
        with self.Session() as db:
            order = db.get(B2BSalesOrder, order_id)
            return _build_row(order, today)

    def test_new_invoice_without_opening_balance_starts_fully_pending(self) -> None:
        order_id = self._add_order("B2B-NEW")

        row = self._row(order_id)

        self.assertEqual(row.paid_amount, Decimal("0.0000"))
        self.assertEqual(row.pending_amount, Decimal("100.0000"))
        self.assertNotEqual(row.status_code, "paid")
        self.assertFalse(row.has_opening_balance)
        self.assertTrue(row.can_register_payment)

    def test_partial_payment_reduces_pending_amount(self) -> None:
        order_id = self._add_order("B2B-PARTIAL", payments=(Decimal("35.0000"),))

        row = self._row(order_id)

        self.assertEqual(row.paid_amount, Decimal("35.0000"))
        self.assertEqual(row.pending_amount, Decimal("65.0000"))
        self.assertNotEqual(row.status_code, "paid")

    def test_full_payment_marks_invoice_paid(self) -> None:
        order_id = self._add_order("B2B-PAID", payments=(Decimal("100.0000"),))

        row = self._row(order_id)

        self.assertEqual(row.paid_amount, Decimal("100.0000"))
        self.assertEqual(row.pending_amount, Decimal("0.0000"))
        self.assertEqual(row.status_code, "paid")

    def test_payment_can_be_recorded_without_opening_balance(self) -> None:
        order_id = self._add_order("B2B-RECORD")
        with self.Session.begin() as db:
            order = db.get(B2BSalesOrder, order_id)

            payment, pending_before, pending_after = record_accounts_receivable_payment(
                db,
                order=order,
                payment_date=date(2026, 8, 11),
                amount=Decimal("25.0000"),
                comment="Pago manual",
                acting_user=None,
            )

            self.assertEqual(payment.amount, Decimal("25.0000"))
            self.assertEqual(pending_before, Decimal("100.0000"))
            self.assertEqual(pending_after, Decimal("75.0000"))

        row = self._row(order_id)
        self.assertEqual(row.paid_amount, Decimal("25.0000"))
        self.assertEqual(row.pending_amount, Decimal("75.0000"))

    def test_historical_opening_balance_keeps_existing_behavior(self) -> None:
        order_id = self._add_order(
            "B2B-HIST",
            invoiced_at=None,
            opening_balance=Decimal("60.0000"),
            payments=(Decimal("10.0000"),),
        )

        row = self._row(order_id, today=date(2026, 6, 15))

        self.assertEqual(row.paid_amount, Decimal("50.0000"))
        self.assertEqual(row.pending_amount, Decimal("50.0000"))

    def test_payment_greater_than_pending_remains_blocked(self) -> None:
        order_id = self._add_order("B2B-OVERPAY")
        with self.Session.begin() as db:
            order = db.get(B2BSalesOrder, order_id)
            with self.assertRaises(AccountsReceivableValidationError):
                record_accounts_receivable_payment(
                    db,
                    order=order,
                    payment_date=date(2026, 8, 11),
                    amount=Decimal("100.0001"),
                    comment=None,
                    acting_user=None,
                )


if __name__ == "__main__":
    unittest.main()
