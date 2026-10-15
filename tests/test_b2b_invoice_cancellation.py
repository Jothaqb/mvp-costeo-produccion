import unittest
from unittest import mock
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from fastapi import HTTPException
from starlette.requests import Request

from app.models import (
    Base,
    AuditLog,
    B2BAROpeningBalance,
    B2BARPayment,
    B2BCustomer,
    B2BSalesOrder,
    B2BSalesOrderLine,
    InventoryBalance,
    InventoryTransaction,
    Permission,
    Product,
    Role,
    RolePermission,
    User,
)
from app.services.accounts_receivable_service import build_accounts_receivable_dashboard
from app.services.auth_service import ensure_auth_seed_state, require_permission
from app.services.b2b_sales_service import (
    B2BValidationError,
    cancel_b2b_invoice,
    invoice_b2b_order_in_erp,
)
from app import database as database_module


class B2BInvoiceCancellationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)
        with self.Session.begin() as db:
            ensure_auth_seed_state(db)
            user = User(
                username="invoice.canceller",
                full_name="Invoice Canceller",
                password_hash="test-only",
                is_active=True,
            )
            customer = B2BCustomer(customer_name="Rganic", credit_days=30)
            product = Product(sku="SKU-CANCEL", name="Cancellation product", active=True)
            db.add_all([user, customer, product])
            db.flush()
            self.user_id = user.id
            self.customer_id = customer.id
            self.product_id = product.id
            db.add(
                InventoryBalance(
                    product_id=product.id,
                    on_hand_qty=Decimal("20"),
                    average_unit_cost=Decimal("5"),
                    inventory_value=Decimal("100"),
                )
            )

    def tearDown(self) -> None:
        Base.metadata.drop_all(self.engine)
        self.engine.dispose()

    def _create_order(self, *, status: str = "in_process", number: str = "B2B1160") -> int:
        with self.Session.begin() as db:
            order = B2BSalesOrder(
                order_number=number,
                customer_id=self.customer_id,
                customer_name_snapshot="Rganic",
                delivery_date=date(2026, 7, 17),
                status=status,
                total_amount=Decimal("14100"),
                loyverse_receipt_id="receipt-uuid-1682",
                loyverse_receipt_number="1682",
                loyverse_invoice_sync_status="success",
            )
            order.lines.append(
                B2BSalesOrderLine(
                    line_number=1,
                    sku_snapshot="SKU-CANCEL",
                    description_snapshot="Cancellation product",
                    unit_price_snapshot=Decimal("2350"),
                    quantity=Decimal("6"),
                    line_total=Decimal("14100"),
                )
            )
            db.add(order)
            db.flush()
            order_id = order.id
        if status != "invoiced":
            with self.Session() as db:
                invoice_b2b_order_in_erp(db, order_id)
        return order_id

    def _permission_codes(self, role_code: str) -> set[str]:
        with self.Session() as db:
            return {
                code
                for (code,) in (
                    db.query(Permission.code)
                    .join(RolePermission, RolePermission.permission_id == Permission.id)
                    .join(Role, Role.id == RolePermission.role_id)
                    .filter(Role.code == role_code)
                    .all()
                )
            }

    def test_admin_and_sales_manager_have_permission_but_general_roles_do_not(self) -> None:
        permission = "b2b_sales.cancel_invoice"
        self.assertIn(permission, self._permission_codes("admin"))
        self.assertIn(permission, self._permission_codes("sales_manager"))
        self.assertNotIn(permission, self._permission_codes("general_operator"))
        self.assertNotIn(permission, self._permission_codes("general_approver"))

        for role_code in ("admin", "sales_manager"):
            request = self._request_with_permissions(self._permission_codes(role_code))
            self.assertIsNotNone(require_permission(request, permission))
        for role_code in ("general_operator", "general_approver"):
            request = self._request_with_permissions(self._permission_codes(role_code))
            with self.assertRaises(HTTPException) as raised:
                require_permission(request, permission)
            self.assertEqual(raised.exception.status_code, 403)

    def _request_with_permissions(self, permissions: set[str]) -> Request:
        request = Request(
            {
                "type": "http",
                "method": "POST",
                "path": "/b2b/orders/1/cancel",
                "headers": [],
                "query_string": b"",
                "scheme": "http",
                "server": ("testserver", 80),
                "client": ("testclient", 123),
            }
        )
        request.state.current_user = object()
        request.state.current_permissions = permissions
        return request

    def test_existing_sqlite_schema_upgrade_is_idempotent_and_preserves_rows(self) -> None:
        migration_engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        try:
            with migration_engine.begin() as connection:
                connection.exec_driver_sql("CREATE TABLE users (id INTEGER PRIMARY KEY)")
                connection.exec_driver_sql(
                    """
                    CREATE TABLE b2b_sales_orders (
                        id INTEGER NOT NULL PRIMARY KEY,
                        order_number VARCHAR(100) NOT NULL UNIQUE,
                        status VARCHAR(50) NOT NULL,
                        CONSTRAINT ck_b2b_sales_orders_status
                        CHECK (status IN ('draft', 'in_process', 'invoiced'))
                    )
                    """
                )
                for table_name in (
                    "b2b_sales_order_lines",
                    "b2b_ar_payments",
                    "b2b_ar_opening_balances",
                ):
                    connection.exec_driver_sql(
                        f"CREATE TABLE {table_name} ("
                        "id INTEGER PRIMARY KEY, sales_order_id INTEGER, "
                        "FOREIGN KEY(sales_order_id) REFERENCES b2b_sales_orders(id))"
                    )
                connection.exec_driver_sql(
                    "INSERT INTO b2b_sales_orders (id, order_number, status) "
                    "VALUES (1, 'B2B1160', 'invoiced')"
                )
                connection.exec_driver_sql(
                    "INSERT INTO b2b_ar_opening_balances (id, sales_order_id) VALUES (1, 1)"
                )
            with mock.patch.object(database_module, "engine", migration_engine):
                database_module.ensure_b2b_invoice_cancellation_columns()
                database_module.ensure_b2b_invoice_cancellation_columns()
            with migration_engine.begin() as connection:
                columns = {
                    row[1]
                    for row in connection.exec_driver_sql(
                        "PRAGMA table_info('b2b_sales_orders')"
                    ).fetchall()
                }
                self.assertTrue(
                    {"cancelled_at", "cancelled_by_user_id", "cancellation_reason"}.issubset(columns)
                )
                connection.exec_driver_sql(
                    "UPDATE b2b_sales_orders SET status = 'cancelled', "
                    "cancellation_reason = 'test' WHERE id = 1"
                )
                self.assertEqual(
                    connection.exec_driver_sql(
                        "SELECT order_number, status, cancellation_reason "
                        "FROM b2b_sales_orders WHERE id = 1"
                    ).fetchone(),
                    ("B2B1160", "cancelled", "test"),
                )
                self.assertEqual(
                    connection.exec_driver_sql(
                        "SELECT sales_order_id FROM b2b_ar_opening_balances WHERE id = 1"
                    ).scalar_one(),
                    1,
                )
        finally:
            migration_engine.dispose()

    def test_sqlite_schema_upgrade_adds_status_check_when_missing(self) -> None:
        migration_engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        try:
            with migration_engine.begin() as connection:
                connection.exec_driver_sql("CREATE TABLE users (id INTEGER PRIMARY KEY)")
                connection.exec_driver_sql(
                    "CREATE TABLE b2b_sales_orders ("
                    "id INTEGER NOT NULL PRIMARY KEY, order_number VARCHAR(100) NOT NULL UNIQUE, "
                    "status VARCHAR(50) NOT NULL)"
                )
                for table_name in (
                    "b2b_sales_order_lines",
                    "b2b_ar_payments",
                    "b2b_ar_opening_balances",
                ):
                    connection.exec_driver_sql(
                        f"CREATE TABLE {table_name} ("
                        "id INTEGER PRIMARY KEY, sales_order_id INTEGER, "
                        "FOREIGN KEY(sales_order_id) REFERENCES b2b_sales_orders(id))"
                    )
                connection.exec_driver_sql(
                    "INSERT INTO b2b_sales_orders (id, order_number, status) "
                    "VALUES (1, 'NO-CHECK', 'invoiced')"
                )
            with mock.patch.object(database_module, "engine", migration_engine):
                database_module.ensure_b2b_invoice_cancellation_columns()
            with migration_engine.begin() as connection:
                connection.exec_driver_sql(
                    "UPDATE b2b_sales_orders SET status = 'cancelled' WHERE id = 1"
                )
                with self.assertRaises(IntegrityError):
                    connection.exec_driver_sql(
                        "UPDATE b2b_sales_orders SET status = 'invalid' WHERE id = 1"
                    )
        finally:
            migration_engine.dispose()

    def test_cancel_reverses_inventory_preserves_history_and_removes_active_ar(self) -> None:
        order_id = self._create_order()
        with self.Session.begin() as db:
            db.add(
                B2BAROpeningBalance(
                    sales_order_id=order_id,
                    outstanding_amount=Decimal("14100"),
                    invoice_date_override=date(2026, 7, 17),
                    comment="Historical opening balance",
                )
            )

        with self.Session() as db:
            order = cancel_b2b_invoice(db, order_id, self.user_id, "Customer invoice voided")
            self.assertEqual(order.status, "cancelled")
            self.assertIsNotNone(order.cancelled_at)
            self.assertEqual(order.cancelled_by_user_id, self.user_id)
            self.assertEqual(order.cancellation_reason, "Customer invoice voided")
            self.assertEqual(order.loyverse_receipt_id, "receipt-uuid-1682")
            self.assertEqual(order.loyverse_receipt_number, "1682")
            self.assertEqual(order.loyverse_invoice_sync_status, "success")
            self.assertEqual(len(order.lines), 1)
            self.assertEqual(order.lines[0].cost_unit_snapshot, Decimal("5.0000"))
            self.assertEqual(order.lines[0].cost_total_snapshot, Decimal("30.0000"))

        with self.Session() as db:
            originals = db.query(InventoryTransaction).filter_by(
                transaction_type="b2b_invoice_sale", source_type="b2b_order", source_id=order_id
            ).all()
            reversals = db.query(InventoryTransaction).filter_by(
                transaction_type="b2b_invoice_cancellation", source_type="b2b_order", source_id=order_id
            ).all()
            self.assertEqual(len(originals), 1)
            self.assertEqual(len(reversals), 1)
            self.assertEqual(reversals[0].quantity_in, originals[0].quantity_out)
            self.assertEqual(reversals[0].unit_cost, originals[0].unit_cost)
            self.assertEqual(reversals[0].total_cost, originals[0].total_cost)
            balance = db.query(InventoryBalance).filter_by(product_id=self.product_id).one()
            self.assertEqual(balance.on_hand_qty, Decimal("20"))
            self.assertEqual(balance.inventory_value, Decimal("100"))
            self.assertIsNotNone(db.query(B2BAROpeningBalance).filter_by(sales_order_id=order_id).one_or_none())
            self.assertEqual(
                db.query(AuditLog)
                .filter_by(action="b2b_sales_order_cancelled", entity_id=str(order_id))
                .count(),
                1,
            )
            dashboard = build_accounts_receivable_dashboard(db, today=date(2026, 10, 10))
            self.assertNotIn(order_id, [row.order_id for row in dashboard.rows])

    def test_invoiced_order_without_invoiced_at_can_be_cancelled(self) -> None:
        order_id = self._create_order(number="B2B-NO-INVOICED-AT")
        with self.Session.begin() as db:
            db.get(B2BSalesOrder, order_id).invoiced_at = None
        with self.Session() as db:
            order = cancel_b2b_invoice(db, order_id, self.user_id, "Legacy invoice cancellation")
            self.assertEqual(order.status, "cancelled")
            self.assertIsNone(order.invoiced_at)

    def test_commit_failure_rolls_back_status_reversal_and_audit(self) -> None:
        order_id = self._create_order(number="B2B-ROLLBACK")
        with self.Session() as db, mock.patch.object(
            db, "commit", side_effect=RuntimeError("forced commit failure")
        ):
            with self.assertRaisesRegex(RuntimeError, "forced commit failure"):
                cancel_b2b_invoice(db, order_id, self.user_id, "Rollback test")

        with self.Session() as db:
            self.assertEqual(db.get(B2BSalesOrder, order_id).status, "invoiced")
            self.assertEqual(
                db.query(InventoryTransaction)
                .filter_by(transaction_type="b2b_invoice_cancellation", source_id=order_id)
                .count(),
                0,
            )
            self.assertEqual(
                db.query(AuditLog)
                .filter_by(action="b2b_sales_order_cancelled", entity_id=str(order_id))
                .count(),
                0,
            )

    def test_draft_order_cannot_be_cancelled(self) -> None:
        with self.Session.begin() as db:
            order = B2BSalesOrder(
                order_number="B2B-DRAFT",
                customer_id=self.customer_id,
                customer_name_snapshot="Rganic",
                delivery_date=date(2026, 10, 10),
                status="draft",
                total_amount=Decimal("10"),
            )
            db.add(order)
            db.flush()
            order_id = order.id
        with self.Session() as db, self.assertRaisesRegex(B2BValidationError, "Only invoiced"):
            cancel_b2b_invoice(db, order_id, self.user_id, "Not allowed")

    def test_invoice_with_payment_cannot_be_cancelled(self) -> None:
        order_id = self._create_order(number="B2B-PAID")
        with self.Session.begin() as db:
            db.add(
                B2BARPayment(
                    sales_order_id=order_id,
                    payment_date=date(2026, 7, 20),
                    amount=Decimal("100"),
                    comment="Partial payment",
                    created_by_user_id=self.user_id,
                )
            )
        with self.Session() as db, self.assertRaisesRegex(B2BValidationError, "registered payments"):
            cancel_b2b_invoice(db, order_id, self.user_id, "Should be blocked")
        with self.Session() as db:
            self.assertEqual(db.get(B2BSalesOrder, order_id).status, "invoiced")
            self.assertEqual(
                db.query(InventoryTransaction)
                .filter_by(transaction_type="b2b_invoice_cancellation", source_id=order_id)
                .count(),
                0,
            )

    def test_invoice_cannot_be_cancelled_twice(self) -> None:
        order_id = self._create_order(number="B2B-TWICE")
        with self.Session() as db:
            cancel_b2b_invoice(db, order_id, self.user_id, "First cancellation")
        with self.Session() as db, self.assertRaisesRegex(B2BValidationError, "already been cancelled"):
            cancel_b2b_invoice(db, order_id, self.user_id, "Second cancellation")
        with self.Session() as db:
            self.assertEqual(
                db.query(InventoryTransaction)
                .filter_by(transaction_type="b2b_invoice_cancellation", source_id=order_id)
                .count(),
                1,
            )


if __name__ == "__main__":
    unittest.main()
