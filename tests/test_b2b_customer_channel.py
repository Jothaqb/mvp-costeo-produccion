import unittest
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from unittest import mock

from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app import database as database_module
from app.models import Base, B2BCustomer, B2BCustomerProduct, B2BSalesOrder, Channel, Product
from app.services.b2b_customer_import_service import import_b2b_customers_csv
from app.services.b2b_sales_service import (
    B2BValidationError,
    create_customer,
    create_sales_order,
    current_b2b_operational_date,
    update_customer,
)


class B2BCustomerChannelTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)
        with self.Session.begin() as db:
            natural = Channel(name="Tiendas Naturales", active=True, applies_to_b2b=True)
            supermarkets = Channel(name="Supermercados", active=True, applies_to_b2b=True)
            inactive = Channel(name="Canal inactivo", active=False, applies_to_b2b=True)
            b2c_only = Channel(name="Solo B2C", active=True, applies_to_b2b=False, applies_to_b2c=True)
            product = Product(sku="SKU-CHANNEL", name="Producto canal", active=True)
            db.add_all((natural, supermarkets, inactive, b2c_only, product))
            db.flush()
            self.natural_id = natural.id
            self.supermarkets_id = supermarkets.id
            self.inactive_id = inactive.id
            self.b2c_only_id = b2c_only.id

    def tearDown(self) -> None:
        Base.metadata.drop_all(self.engine)
        self.engine.dispose()

    @staticmethod
    def _customer_args(name: str = "Angel Azul") -> tuple:
        return (name, "", "", "", "", "", "", "", "", "30", True)

    def _create_customer_with_catalog(self, db, channel_id: int | None) -> B2BCustomer:
        customer = create_customer(
            db,
            *self._customer_args(),
            channel_id=str(channel_id or ""),
        )
        db.add(
            B2BCustomerProduct(
                customer_id=customer.id,
                sku="SKU-CHANNEL",
                description="Producto canal",
                distributor_price=Decimal("2500"),
                active=True,
            )
        )
        db.commit()
        return customer

    def test_create_and_edit_customer_channel(self) -> None:
        with self.Session() as db:
            customer = create_customer(
                db,
                *self._customer_args(),
                channel_id=str(self.natural_id),
            )
            self.assertEqual(customer.channel_id, self.natural_id)

            updated = update_customer(
                db,
                customer.id,
                *self._customer_args(),
                channel_id=str(self.supermarkets_id),
            )
            self.assertEqual(updated.channel_id, self.supermarkets_id)

    def test_customer_may_remain_without_channel(self) -> None:
        with self.Session() as db:
            customer = create_customer(db, *self._customer_args(), channel_id="")
            self.assertIsNone(customer.channel_id)

    def test_invalid_inactive_or_non_b2b_channel_is_rejected(self) -> None:
        with self.Session() as db:
            for channel_id, message in (
                ("999999", "does not exist"),
                (str(self.inactive_id), "not active"),
                (str(self.b2c_only_id), "not valid for B2B"),
            ):
                with self.subTest(channel_id=channel_id):
                    with self.assertRaisesRegex(B2BValidationError, message):
                        create_customer(db, *self._customer_args(), channel_id=channel_id)
                    db.rollback()

    def test_new_order_snapshots_customer_channel(self) -> None:
        with self.Session() as db:
            customer = self._create_customer_with_catalog(db, self.natural_id)
            order = create_sales_order(
                db,
                customer.id,
                current_b2b_operational_date() + timedelta(days=1),
                [{"sku": "SKU-CHANNEL", "quantity": "2"}],
                "",
            )
            self.assertEqual(order.channel_id, self.natural_id)
            self.assertEqual(order.b2b_channel_name_snapshot, "Tiendas Naturales")

    def test_order_snapshot_does_not_change_when_customer_channel_changes(self) -> None:
        with self.Session() as db:
            customer = self._create_customer_with_catalog(db, self.natural_id)
            order = create_sales_order(
                db,
                customer.id,
                current_b2b_operational_date() + timedelta(days=1),
                [{"sku": "SKU-CHANNEL", "quantity": "1"}],
                "",
            )
            order.status = "invoiced"
            db.commit()
            order_id = order.id
            update_customer(
                db,
                customer.id,
                *self._customer_args(),
                channel_id=str(self.supermarkets_id),
            )

            unchanged = db.query(B2BSalesOrder).filter(B2BSalesOrder.id == order_id).one()
            self.assertEqual(unchanged.channel_id, self.natural_id)
            self.assertEqual(unchanged.b2b_channel_name_snapshot, "Tiendas Naturales")

    def test_new_order_without_customer_channel_is_blocked(self) -> None:
        with self.Session() as db:
            customer = self._create_customer_with_catalog(db, None)
            with self.assertRaisesRegex(
                B2BValidationError,
                "Debe asignar un canal B2B al cliente antes de crear órdenes",
            ):
                create_sales_order(
                    db,
                    customer.id,
                    current_b2b_operational_date() + timedelta(days=1),
                    [{"sku": "SKU-CHANNEL", "quantity": "1"}],
                    "",
                )

    def test_customer_import_without_channel_still_works(self) -> None:
        csv_bytes = (
            "customer_name,sku,product_name,distributor_price\n"
            "Cliente importado,SKU-CHANNEL,Producto canal,2200\n"
        ).encode("utf-8")
        with self.Session() as db:
            result = import_b2b_customers_csv(db, file_name="customers.csv", file_bytes=csv_bytes)
            imported = db.query(B2BCustomer).filter(B2BCustomer.customer_name == "Cliente importado").one()
            self.assertEqual(result.customers_created, 1)
            self.assertIsNone(imported.channel_id)

    def test_forms_move_channel_selection_from_order_to_customer(self) -> None:
        customer_form = Path("app/templates/b2b_customer_form.html").read_text(encoding="utf-8")
        order_form = Path("app/templates/b2b_order_form.html").read_text(encoding="utf-8")
        order_edit = Path("app/templates/b2b_order_edit.html").read_text(encoding="utf-8")
        self.assertIn('name="channel_id"', customer_form)
        self.assertIn("B2B Channel", customer_form)
        self.assertNotIn('name="b2b_channel_id"', order_form)
        self.assertNotIn('name="b2b_channel_id"', order_edit)
        self.assertIn("Debe asignar un canal B2B al cliente antes de crear órdenes.", order_form)

    def test_existing_sqlite_schema_migration_is_idempotent_and_adds_fk(self) -> None:
        migration_engine = create_engine("sqlite:///:memory:", poolclass=StaticPool)
        with migration_engine.begin() as connection:
            connection.exec_driver_sql("PRAGMA foreign_keys=ON")
            connection.exec_driver_sql(
                """
                CREATE TABLE channels (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(255) NOT NULL UNIQUE,
                    active BOOLEAN NOT NULL DEFAULT 1,
                    applies_to_b2b BOOLEAN NOT NULL DEFAULT 0,
                    applies_to_b2c BOOLEAN NOT NULL DEFAULT 0,
                    observations TEXT,
                    created_at DATETIME NOT NULL,
                    updated_at DATETIME NOT NULL
                )
                """
            )
            connection.exec_driver_sql("CREATE TABLE b2b_customers (id INTEGER PRIMARY KEY)")
            connection.exec_driver_sql("CREATE TABLE b2b_sales_orders (id INTEGER PRIMARY KEY)")
            connection.exec_driver_sql("CREATE TABLE b2c_sales_orders (id INTEGER PRIMARY KEY)")

        with mock.patch.object(database_module, "engine", migration_engine):
            database_module.ensure_channel_master_tables()
            database_module.ensure_b2b_customer_channel_column()
            database_module.ensure_b2b_customer_channel_column()

        inspector = inspect(migration_engine)
        self.assertIn("channel_id", {column["name"] for column in inspector.get_columns("b2b_customers")})
        self.assertTrue(
            any(
                foreign_key["referred_table"] == "channels"
                and foreign_key["constrained_columns"] == ["channel_id"]
                for foreign_key in inspector.get_foreign_keys("b2b_customers")
            )
        )
        migration_engine.dispose()


if __name__ == "__main__":
    unittest.main()
