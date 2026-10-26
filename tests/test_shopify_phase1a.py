import os
import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest import mock

from sqlalchemy import create_engine, inspect, select, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import joinedload, sessionmaker
from sqlalchemy.pool import StaticPool

from app.database import Base
from app import database as database_module
from app.models import (
    AppSequence,
    AuditLog,
    B2CCustomer,
    B2CSalesOrder,
    B2CSalesOrderLine,
    Channel,
    InventoryBalance,
    InventoryTransaction,
    Product,
    ShopifyCustomerMapping,
    ShopifyOrderIntegration,
    ShopifyOrderLineIntegration,
    ShopifyWebhookEvent,
    User,
)
from app.services.audit_service import add_audit_event_in_session
from app.services.accounting_income_statement_service import get_accounting_sales_summary
from app.services.b2c_customer_service import create_b2c_customer_in_session
from app.services.b2c_sales_service import (
    B2CValidationError,
    build_b2c_shipping_cost_csrf_token,
    calculate_b2c_order_margins,
    create_b2c_sales_order,
    create_b2c_sales_order_in_session,
    invoice_b2c_order_in_erp,
    _generate_b2c_order_number,
    line_type_for_new_b2c_line,
    record_b2c_shipping_cost_in_session,
    recalculate_b2c_order_totals_from_line_snapshots,
    set_b2c_line_pricing_snapshots,
    validate_b2c_shipping_cost_csrf_token,
)
from app.services.shopify_phase1a_service import (
    ShopifyPhase1AError,
    add_shopify_customer_mapping_in_session,
    has_sufficient_shopify_customer_identity,
    normalize_shopify_phone,
    register_shopify_webhook_event_in_session,
    resolve_online_web_channel,
    resolve_shopify_customer_match,
    resolve_shopify_product,
    shopify_integration_enabled,
)
from app.services.total_sales_service import (
    get_sales_by_order,
    get_sales_categories_pareto,
    get_sales_items_pareto,
    get_sales_summary,
)


ZERO = Decimal("0")


class ShopifyPhase1ATests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)
        with self.Session.begin() as db:
            channel = Channel(name="Online Web", active=True, applies_to_b2c=True)
            product = Product(
                sku="P-100",
                name="Producto físico",
                active=True,
                available_for_sale_gc=True,
                b2c_price=Decimal("1000"),
            )
            gam = Product(
                sku="10219",
                name="Envío GAM",
                active=True,
                available_for_sale_gc=True,
                b2c_price=Decimal("2500"),
            )
            outside = Product(
                sku="10258",
                name="Envío fuera GAM",
                active=True,
                available_for_sale_gc=True,
                b2c_price=Decimal("3500"),
            )
            db.add_all((channel, product, gam, outside))
            db.flush()
            self.channel_id = channel.id
            self.product_id = product.id
            self.gam_product_id = gam.id
            self.outside_product_id = outside.id
            db.add(
                InventoryBalance(
                    product_id=product.id,
                    on_hand_qty=Decimal("10"),
                    average_unit_cost=Decimal("400"),
                    inventory_value=Decimal("4000"),
                )
            )

    def tearDown(self) -> None:
        Base.metadata.drop_all(self.engine)
        self.engine.dispose()

    def _create_manual_order(
        self,
        *,
        product_quantity: str = "2",
        include_shipping: bool = True,
        shipping_sku: str = "10219",
        shipping_quantity: str = "1",
    ) -> B2CSalesOrder:
        lines = [{"sku": "P-100", "quantity": product_quantity, "unit_price": "1000"}]
        if include_shipping:
            lines.append(
                {
                    "sku": shipping_sku,
                    "quantity": shipping_quantity,
                    "unit_price": "2500",
                }
            )
        with self.Session() as db:
            return create_b2c_sales_order(
                db,
                order_date=date(2026, 10, 25),
                b2c_customer_id="",
                customer_name="Cliente web",
                customer_phone="8888-7777",
                customer_email="",
                customer_address_snapshot="San José",
                province_snapshot="San José",
                canton_snapshot="San José",
                district_snapshot="Carmen",
                customer_observations_snapshot="",
                channel_id=str(self.channel_id),
                discount_rule_id="",
                observations="",
                line_inputs=lines,
            )

    def test_phase1a_schema_and_defaults_preserve_historical_lines(self) -> None:
        columns = {column["name"] for column in inspect(self.engine).get_columns("b2c_sales_orders")}
        self.assertTrue(
            {
                "shipping_cost_amount",
                "shipping_cost_source",
                "shipping_cost_recorded_at",
                "shipping_cost_recorded_by_user_id",
            }.issubset(columns)
        )
        integration_tables = set(inspect(self.engine).get_table_names())
        self.assertTrue(
            {
                "shopify_webhook_events",
                "shopify_order_integrations",
                "shopify_order_line_integrations",
                "shopify_customer_mappings",
            }.issubset(integration_tables)
        )
        with self.Session.begin() as db:
            order = B2CSalesOrder(
                order_number="B2C-HIST",
                order_date=date(2025, 1, 1),
                channel="Online Web",
                status="invoiced",
                subtotal_amount=Decimal("100"),
                discount_amount=ZERO,
                total_amount=Decimal("100"),
            )
            db.add(order)
            db.flush()
            line = B2CSalesOrderLine(
                sales_order_id=order.id,
                line_number=1,
                sku_snapshot="10219",
                description_snapshot="Histórico no reclasificado",
                quantity=Decimal("1"),
                unit_price_snapshot=Decimal("100"),
                line_total=Decimal("100"),
            )
            db.add(line)
            db.flush()
            self.assertEqual(line.line_type, "product")

    def test_incremental_sqlite_schema_upgrade_is_idempotent_and_preserves_rows(self) -> None:
        legacy_engine = create_engine("sqlite://", poolclass=StaticPool)
        with legacy_engine.begin() as connection:
            connection.execute(
                text(
                    "CREATE TABLE b2c_sales_orders ("
                    "id INTEGER PRIMARY KEY, order_number VARCHAR(100), order_date DATE, "
                    "channel VARCHAR(255), status VARCHAR(50), subtotal_amount NUMERIC, "
                    "discount_amount NUMERIC, total_amount NUMERIC, created_at DATETIME, updated_at DATETIME)"
                )
            )
            connection.execute(
                text(
                    "CREATE TABLE b2c_sales_order_lines ("
                    "id INTEGER PRIMARY KEY, sales_order_id INTEGER, line_number INTEGER, "
                    "sku_snapshot VARCHAR(100), description_snapshot VARCHAR(255), quantity NUMERIC, "
                    "unit_price_snapshot NUMERIC, line_total NUMERIC, created_at DATETIME, updated_at DATETIME)"
                )
            )
            connection.execute(
                text(
                    "INSERT INTO b2c_sales_orders "
                    "(id, order_number, order_date, channel, status, subtotal_amount, discount_amount, total_amount) "
                    "VALUES (1, 'B2C-LEGACY', '2025-01-01', 'Online Web', 'invoiced', 100, 0, 100)"
                )
            )
            connection.execute(
                text(
                    "INSERT INTO b2c_sales_order_lines "
                    "(id, sales_order_id, line_number, sku_snapshot, description_snapshot, quantity, unit_price_snapshot, line_total) "
                    "VALUES (1, 1, 1, '10219', 'Historical line', 1, 100, 100)"
                )
            )
        with mock.patch.object(database_module, "engine", legacy_engine):
            database_module.ensure_shopify_phase1a_schema()
            database_module.ensure_shopify_phase1a_schema()
        with legacy_engine.connect() as connection:
            row = connection.execute(text("SELECT line_type FROM b2c_sales_order_lines WHERE id = 1")).one()
            self.assertEqual(row.line_type, "product")
            order_columns = {column["name"] for column in inspect(legacy_engine).get_columns("b2c_sales_orders")}
            self.assertIn("shipping_cost_amount", order_columns)
            with self.assertRaises(IntegrityError):
                connection.execute(
                    text(
                        "INSERT INTO b2c_sales_order_lines "
                        "(id, sales_order_id, line_number, line_type, sku_snapshot, "
                        "description_snapshot, quantity, unit_price_snapshot, line_total) "
                        "VALUES (2, 1, 2, 'invalid', 'P-100', 'Invalid type', 1, 100, 100)"
                    )
                )
        legacy_engine.dispose()

    def test_postgresql_incremental_sql_is_idempotent_and_column_scoped(self) -> None:
        fake_engine = mock.MagicMock()
        fake_engine.dialect.name = "postgresql"
        connection = mock.MagicMock()
        result = mock.MagicMock()
        result.fetchone.return_value = None
        connection.exec_driver_sql.return_value = result
        fake_engine.begin.return_value.__enter__.return_value = connection
        with mock.patch.object(database_module, "engine", fake_engine):
            database_module.ensure_shopify_phase1a_schema()
        statements = "\n".join(call.args[0] for call in connection.exec_driver_sql.call_args_list)
        self.assertIn("ADD COLUMN IF NOT EXISTS line_type", statements)
        self.assertIn("ADD COLUMN IF NOT EXISTS shipping_cost_amount", statements)
        self.assertIn("a.attnum = ANY(c.conkey)", statements)
        self.assertNotIn("ILIKE", statements)

    def test_postgresql_shipping_cost_lock_targets_only_the_order_table(self) -> None:
        statement = (
            select(B2CSalesOrder)
            .options(joinedload(B2CSalesOrder.lines))
            .where(B2CSalesOrder.id == 1)
            .with_for_update(of=B2CSalesOrder)
        )
        compiled = str(statement.compile(dialect=postgresql.dialect()))
        self.assertIn("FOR UPDATE OF b2c_sales_orders", compiled)
        service_source = Path("app/services/b2c_sales_service.py").read_text(encoding="utf-8")
        main_source = Path("app/main.py").read_text(encoding="utf-8")
        self.assertIn(".with_for_update(of=B2CSalesOrder)", service_source)
        self.assertIn(".with_for_update(of=B2CSalesOrder)", main_source)

    def test_manual_orders_classify_only_confirmed_shipping_skus(self) -> None:
        order = self._create_manual_order()
        with self.Session() as db:
            stored = db.get(B2CSalesOrder, order.id)
            self.assertEqual([line.line_type for line in stored.lines], ["product", "shipping"])
            self.assertEqual(stored.total_amount, Decimal("4500.0000"))
        self.assertEqual(line_type_for_new_b2c_line("10219"), "shipping")
        self.assertEqual(line_type_for_new_b2c_line("10258"), "shipping")
        self.assertEqual(line_type_for_new_b2c_line("10297"), "product")

    def test_shipping_quantity_must_be_exactly_one(self) -> None:
        with self.assertRaisesRegex(B2CValidationError, "quantity exactly 1"):
            self._create_manual_order(shipping_quantity="2")

    def test_order_cannot_contain_duplicate_shipping_income_lines(self) -> None:
        with self.Session() as db, self.assertRaisesRegex(B2CValidationError, "at most one shipping line"):
            create_b2c_sales_order(
                db,
                order_date=date(2026, 10, 25),
                b2c_customer_id="",
                customer_name="Cliente web",
                customer_phone="8888-7777",
                customer_email="",
                customer_address_snapshot="",
                province_snapshot="",
                canton_snapshot="",
                district_snapshot="",
                customer_observations_snapshot="",
                channel_id=str(self.channel_id),
                discount_rule_id="",
                observations="",
                line_inputs=[
                    {"sku": "10219", "quantity": "1", "unit_price": "2500"},
                    {"sku": "10258", "quantity": "1", "unit_price": "3500"},
                ],
            )

    def test_physical_product_cannot_be_arbitrarily_marked_shipping(self) -> None:
        with self.Session.begin() as db:
            order = self._direct_order(db, "B2C-BAD-SHIPPING")
            order.lines.append(
                self._line(
                    sku="P-100",
                    line_type="shipping",
                    gross="1000",
                    quantity="1",
                )
            )
            order.subtotal_amount = Decimal("1000")
            order.total_amount = Decimal("1000")
            order_id = order.id
        with self.Session() as db, self.assertRaisesRegex(B2CValidationError, "authorized shipping SKUs"):
            invoice_b2c_order_in_erp(db, order_id)

    def test_shipping_invoice_does_not_move_inventory_or_kardex(self) -> None:
        order = self._create_manual_order()
        with self.Session() as db:
            invoice_b2c_order_in_erp(db, order.id)
        with self.Session() as db:
            transactions = db.query(InventoryTransaction).filter_by(source_id=order.id).all()
            self.assertEqual(len(transactions), 1)
            self.assertEqual(transactions[0].product_id, self.product_id)
            self.assertEqual(transactions[0].quantity_out, Decimal("2.0000"))
            self.assertIsNone(
                db.query(InventoryBalance).filter_by(product_id=self.gam_product_id).one_or_none()
            )
            stored = db.get(B2CSalesOrder, order.id)
            shipping_line = next(line for line in stored.lines if line.line_type == "shipping")
            self.assertEqual(shipping_line.cost_unit_snapshot, Decimal("0.0000"))
            self.assertEqual(shipping_line.cost_total_snapshot, Decimal("0.0000"))
            self.assertEqual(stored.total_amount, Decimal("4500.0000"))

    def test_historical_shipping_sku_keeps_product_behavior_when_invoiced(self) -> None:
        with self.Session.begin() as db:
            order = self._direct_order(db, "B2C-HISTORICAL-SHIPPING-SKU")
            line = self._line(sku="10219", line_type="product", gross="2500", quantity="1")
            order.lines.append(line)
            recalculate_b2c_order_totals_from_line_snapshots(order)
            order_id = order.id
        with self.Session() as db:
            invoice_b2c_order_in_erp(db, order_id)
        with self.Session() as db:
            stored = db.get(B2CSalesOrder, order_id)
            self.assertEqual(stored.lines[0].line_type, "product")
            transaction = db.query(InventoryTransaction).filter_by(source_id=order_id).one()
            self.assertEqual(transaction.product_id, self.gam_product_id)

    def test_shipping_invoice_preserves_accounting_cogs_coverage(self) -> None:
        order = self._create_manual_order()
        with self.Session() as db:
            invoice_b2c_order_in_erp(db, order.id)
        with self.Session() as db:
            accounting = get_accounting_sales_summary(db, date(2026, 10, 1))
        self.assertTrue(accounting.has_complete_cogs)
        self.assertEqual(accounting.revenue, Decimal("4500.0000"))
        self.assertEqual(accounting.cogs, Decimal("800.0000"))
        self.assertEqual(accounting.gross_profit, Decimal("3700.0000"))

    def test_product_invoice_keeps_inventory_and_margin_behavior(self) -> None:
        order = self._create_manual_order(include_shipping=False)
        with self.Session() as db:
            invoiced = invoice_b2c_order_in_erp(db, order.id)
            self.assertEqual(invoiced.cost_total_snapshot, Decimal("800.0000"))
            self.assertEqual(invoiced.gross_margin_amount, Decimal("1200.0000"))
        with self.Session() as db:
            balance = db.query(InventoryBalance).filter_by(product_id=self.product_id).one()
            self.assertEqual(balance.on_hand_qty, Decimal("8.0000"))

    def test_shipping_discount_and_free_shipping_are_not_duplicated(self) -> None:
        with self.Session.begin() as db:
            order = self._direct_order(db, "B2C-FREE-SHIPPING")
            product_line = self._line(sku="P-100", line_type="product", gross="20000", quantity="1")
            shipping_line = self._line(sku="10219", line_type="shipping", gross="2500", quantity="1")
            set_b2c_line_pricing_snapshots(
                product_line,
                gross_amount=Decimal("20000"),
                discount_amount=Decimal("2000"),
            )
            set_b2c_line_pricing_snapshots(
                shipping_line,
                gross_amount=Decimal("2500"),
                discount_amount=Decimal("2500"),
            )
            order.lines.extend((product_line, shipping_line))
            recalculate_b2c_order_totals_from_line_snapshots(order)
            self.assertEqual(order.discount_amount, Decimal("4500.0000"))
            self.assertEqual(order.total_amount, Decimal("18000.0000"))
            self.assertEqual(shipping_line.net_line_total_snapshot, ZERO.quantize(Decimal("0.0001")))

    def test_margin_separates_products_shipping_and_unknown_cost(self) -> None:
        order = self._create_manual_order()
        with self.Session() as db:
            invoice_b2c_order_in_erp(db, order.id)
        with self.Session() as db:
            stored = db.get(B2CSalesOrder, order.id)
            margin = calculate_b2c_order_margins(stored)
            self.assertEqual(margin.product_net_sales, Decimal("2000.0000"))
            self.assertEqual(margin.shipping_net_sales, Decimal("2500.0000"))
            self.assertEqual(margin.product_margin_amount, Decimal("1200.0000"))
            self.assertTrue(margin.total_margin_pending)

    def test_zero_shipping_cost_is_known_and_distinct_from_null(self) -> None:
        order = self._create_manual_order()
        with self.Session() as db:
            invoice_b2c_order_in_erp(db, order.id)
        with self.Session.begin() as db:
            user = User(
                username="shipping-cost-user",
                full_name="Shipping Cost User",
                password_hash="not-used",
            )
            db.add(user)
            db.flush()
            record_b2c_shipping_cost_in_session(
                db,
                order_id=order.id,
                amount=ZERO,
                source="carrier_invoice",
                user_id=user.id,
            )
        with self.Session() as db:
            margin = calculate_b2c_order_margins(db.get(B2CSalesOrder, order.id))
            self.assertEqual(margin.shipping_cost, Decimal("0.0000"))
            self.assertFalse(margin.total_margin_pending)
            self.assertEqual(margin.total_margin_amount, Decimal("3700.0000"))

    def test_actual_shipping_cost_is_subtracted_once_from_total_margin(self) -> None:
        order = self._create_manual_order()
        with self.Session() as db:
            invoice_b2c_order_in_erp(db, order.id)
        with self.Session.begin() as db:
            user = User(
                username="shipping-cost-positive",
                full_name="Shipping Cost Positive",
                password_hash="not-used",
            )
            db.add(user)
            db.flush()
            record_b2c_shipping_cost_in_session(
                db,
                order_id=order.id,
                amount=Decimal("1800"),
                source="carrier_invoice",
                user_id=user.id,
            )
        with self.Session() as db:
            stored = db.get(B2CSalesOrder, order.id)
            margin = calculate_b2c_order_margins(stored)
            summary = get_sales_summary(db, sales_type="b2c").b2c
            by_order = get_sales_by_order(db, sales_type="b2c").rows[0]
        self.assertEqual(margin.product_margin_amount, Decimal("1200.0000"))
        self.assertEqual(margin.shipping_cost, Decimal("1800.0000"))
        self.assertEqual(margin.total_margin_amount, Decimal("1900.0000"))
        self.assertEqual(summary.shipping_cost, Decimal("1800.0000"))
        self.assertEqual(summary.total_margin, Decimal("1900.0000"))
        self.assertEqual(by_order.total_margin, Decimal("1900.0000"))

    def test_shipping_cost_rejects_non_finite_values(self) -> None:
        order = self._create_manual_order()
        with self.Session.begin() as db:
            user = User(
                username="shipping-cost-finite",
                full_name="Shipping Cost Finite",
                password_hash="not-used",
            )
            db.add(user)
            db.flush()
            user_id = user.id
        with self.Session() as db, self.assertRaisesRegex(B2CValidationError, "finite"):
            record_b2c_shipping_cost_in_session(
                db,
                order_id=order.id,
                amount=Decimal("Infinity"),
                source="carrier_invoice",
                user_id=user_id,
            )

    def test_total_sales_counts_shipping_once_but_excludes_it_from_units_and_pareto(self) -> None:
        order = self._create_manual_order()
        with self.Session() as db:
            invoice_b2c_order_in_erp(db, order.id)
        with self.Session() as db:
            summary = get_sales_summary(db, sales_type="b2c").b2c
            items = get_sales_items_pareto(db, sales_type="b2c")
            categories = get_sales_categories_pareto(db, sales_type="b2c")
            by_order = get_sales_by_order(db, sales_type="b2c")
        self.assertEqual(summary.total_net_sales, Decimal("4500.0000"))
        self.assertEqual(summary.product_net_sales, Decimal("2000.0000"))
        self.assertEqual(summary.shipping_net_sales, Decimal("2500.0000"))
        self.assertEqual(summary.total_quantity, Decimal("2.0000"))
        self.assertIsNone(summary.total_margin)
        self.assertEqual([row.sku for row in items.rows], ["P-100"])
        self.assertEqual(categories.summary.total_categories, 1)
        self.assertEqual(by_order.rows[0].items_count, 1)
        self.assertEqual(by_order.rows[0].shipping_net_sales, Decimal("2500.0000"))

    def test_phone_normalization_and_phone_only_identity(self) -> None:
        self.assertEqual(normalize_shopify_phone("+506 8888-7777"), "+50688887777")
        self.assertEqual(normalize_shopify_phone("8888 7777"), "+50688887777")
        self.assertTrue(
            has_sufficient_shopify_customer_identity(
                name="Cliente teléfono",
                phone="8888-7777",
                email=None,
            )
        )
        self.assertFalse(has_sufficient_shopify_customer_identity(name="Cliente", phone=None, email=None))

    def test_customer_matching_uses_phone_then_optional_email(self) -> None:
        with self.Session.begin() as db:
            phone_customer = B2CCustomer(name="Teléfono", phone="8888-7777", active=True)
            email_customer = B2CCustomer(name="Correo", email="mail@example.com", active=True)
            db.add_all((phone_customer, email_customer))
            db.flush()
            phone_id = phone_customer.id
            email_id = email_customer.id
        with self.Session() as db:
            phone_match = resolve_shopify_customer_match(
                db,
                shop_domain="greencornercr.com",
                shopify_customer_gid=None,
                phone="+506 8888 7777",
                email=None,
            )
            email_match = resolve_shopify_customer_match(
                db,
                shop_domain="greencornercr.com",
                shopify_customer_gid=None,
                phone=None,
                email="MAIL@example.com",
            )
        self.assertEqual(phone_match.customer.id, phone_id)
        self.assertEqual(phone_match.outcome, "matched_phone")
        self.assertEqual(email_match.customer.id, email_id)
        self.assertEqual(email_match.outcome, "matched_email")

    def test_duplicate_phone_and_conflicting_mapping_never_auto_link(self) -> None:
        with self.Session.begin() as db:
            first = B2CCustomer(name="Primero", phone="8888-7777", active=True)
            second = B2CCustomer(name="Segundo", phone="+50688887777", active=True)
            db.add_all((first, second))
            db.flush()
            add_shopify_customer_mapping_in_session(
                db,
                shop_domain="greencornercr.com",
                shopify_customer_gid="gid://shopify/Customer/1",
                customer=first,
                phone=first.phone,
                email=first.email,
            )
        with self.Session() as db:
            ambiguous = resolve_shopify_customer_match(
                db,
                shop_domain="greencornercr.com",
                shopify_customer_gid=None,
                phone="88887777",
                email=None,
            )
            conflict = resolve_shopify_customer_match(
                db,
                shop_domain="greencornercr.com",
                shopify_customer_gid="gid://shopify/Customer/1",
                phone="88887777",
                email=None,
            )
        self.assertEqual(ambiguous.outcome, "ambiguous")
        self.assertIsNone(ambiguous.customer)
        self.assertEqual(conflict.outcome, "conflict")
        self.assertIsNone(conflict.customer)

    def test_mapping_has_priority_when_contact_data_is_not_contradictory(self) -> None:
        with self.Session.begin() as db:
            customer = B2CCustomer(name="Mapeado", phone="8888-7777", active=True)
            db.add(customer)
            db.flush()
            customer_id = customer.id
            add_shopify_customer_mapping_in_session(
                db,
                shop_domain="greencornercr.com",
                shopify_customer_gid="gid://shopify/Customer/2",
                customer=customer,
                phone=customer.phone,
                email=None,
            )
        with self.Session() as db:
            match = resolve_shopify_customer_match(
                db,
                shop_domain="greencornercr.com",
                shopify_customer_gid="gid://shopify/Customer/2",
                phone="+50688887777",
                email=None,
            )
        self.assertEqual(match.outcome, "matched_mapping")
        self.assertEqual(match.customer.id, customer_id)

    def test_product_and_channel_resolution_are_strict(self) -> None:
        with self.Session() as db:
            self.assertEqual(resolve_online_web_channel(db).id, self.channel_id)
            self.assertEqual(resolve_shopify_product(db, "P-100").id, self.product_id)
        with self.Session.begin() as db:
            db.query(Channel).filter_by(id=self.channel_id).update({"active": False})
            db.query(Product).filter_by(id=self.product_id).update({"available_for_sale_gc": False})
        with self.Session() as db:
            with self.assertRaises(ShopifyPhase1AError):
                resolve_online_web_channel(db)
            with self.assertRaises(ShopifyPhase1AError):
                resolve_shopify_product(db, "P-100")

    def test_webhook_delivery_registration_is_idempotent_and_detects_conflict(self) -> None:
        with self.Session.begin() as db:
            first, created = register_shopify_webhook_event_in_session(
                db,
                shop_domain="greencornercr.com",
                webhook_id="webhook-1",
                topic="orders/create",
                raw_body=b'{"id": 1}',
            )
            repeated, repeated_created = register_shopify_webhook_event_in_session(
                db,
                shop_domain="greencornercr.com",
                webhook_id="webhook-1",
                topic="orders/create",
                raw_body=b'{"id": 1}',
            )
            self.assertEqual(first.id, repeated.id)
            self.assertTrue(created)
            self.assertFalse(repeated_created)
            with self.assertRaises(ShopifyPhase1AError):
                register_shopify_webhook_event_in_session(
                    db,
                    shop_domain="greencornercr.com",
                    webhook_id="webhook-1",
                    topic="orders/create",
                    raw_body=b'{"id": 2}',
                )

    def test_integration_unique_constraints_block_duplicate_shopify_references(self) -> None:
        with self.Session.begin() as db:
            db.add(
                ShopifyOrderIntegration(
                    shop_domain="greencornercr.com",
                    shopify_order_gid="gid://shopify/Order/1",
                )
            )
        with self.Session() as db:
            db.add(
                ShopifyOrderIntegration(
                    shop_domain="greencornercr.com",
                    shopify_order_gid="gid://shopify/Order/1",
                )
            )
            with self.assertRaises(IntegrityError):
                db.flush()
            db.rollback()

        with self.Session.begin() as db:
            customer = B2CCustomer(name="Unique mapping", active=True)
            integration = ShopifyOrderIntegration(
                shop_domain="greencornercr.com",
                shopify_order_gid="gid://shopify/Order/2",
            )
            db.add_all((customer, integration))
            db.flush()
            customer_id = customer.id
            integration_id = integration.id
            db.add(
                ShopifyCustomerMapping(
                    shop_domain="greencornercr.com",
                    shopify_customer_gid="gid://shopify/Customer/unique",
                    b2c_customer_id=customer_id,
                )
            )
            db.add(
                ShopifyOrderLineIntegration(
                    shopify_order_integration_id=integration_id,
                    shopify_line_gid="gid://shopify/LineItem/1",
                    line_type="product",
                    sku_snapshot="P-100",
                    quantity=Decimal("1"),
                    gross_amount=Decimal("1000"),
                    discount_amount=ZERO,
                    net_amount=Decimal("1000"),
                )
            )
        with self.Session() as db:
            db.add(
                ShopifyCustomerMapping(
                    shop_domain="greencornercr.com",
                    shopify_customer_gid="gid://shopify/Customer/unique",
                    b2c_customer_id=customer_id,
                )
            )
            with self.assertRaises(IntegrityError):
                db.flush()
            db.rollback()
        with self.Session() as db:
            db.add(
                ShopifyOrderLineIntegration(
                    shopify_order_integration_id=integration_id,
                    shopify_line_gid="gid://shopify/LineItem/1",
                    line_type="product",
                    sku_snapshot="P-100",
                    quantity=Decimal("1"),
                    gross_amount=Decimal("1000"),
                    discount_amount=ZERO,
                    net_amount=Decimal("1000"),
                )
            )
            with self.assertRaises(IntegrityError):
                db.flush()
            db.rollback()

    def test_postgresql_order_sequence_uses_transaction_lock_and_locked_row(self) -> None:
        db = mock.MagicMock()
        db.get_bind.return_value.dialect.name = "postgresql"
        sequence_query = mock.MagicMock()
        sequence_query.filter.return_value = sequence_query
        sequence_query.with_for_update.return_value = sequence_query
        sequence = AppSequence(name="b2c_sales_order", next_value=42)
        sequence_query.one_or_none.return_value = sequence
        order_numbers_query = mock.MagicMock()
        order_numbers_query.all.return_value = []
        existing_order_query = mock.MagicMock()
        existing_order_query.filter.return_value = existing_order_query
        existing_order_query.one_or_none.return_value = None
        db.query.side_effect = (sequence_query, order_numbers_query, existing_order_query)

        number = _generate_b2c_order_number(db)

        self.assertEqual(number, "B2C0042")
        self.assertEqual(sequence.next_value, 43)
        self.assertTrue(sequence_query.with_for_update.called)
        sql_text = str(db.execute.call_args.args[0])
        self.assertIn("pg_advisory_xact_lock", sql_text)

    def test_transaction_helpers_and_audit_roll_back_together(self) -> None:
        try:
            with self.Session.begin() as db:
                customer = create_b2c_customer_in_session(
                    db,
                    name="Cliente rollback",
                    phone="88887777",
                    email="",
                    address="",
                    province="",
                    canton="",
                    district="",
                    observations="",
                    active=True,
                )
                order = create_b2c_sales_order_in_session(
                    db,
                    order_date=date(2026, 10, 25),
                    b2c_customer_id=str(customer.id),
                    customer_name=customer.name,
                    customer_phone=customer.phone or "",
                    customer_email="",
                    customer_address_snapshot="",
                    province_snapshot="",
                    canton_snapshot="",
                    district_snapshot="",
                    customer_observations_snapshot="",
                    channel_id=str(self.channel_id),
                    discount_rule_id="",
                    observations="",
                    line_inputs=[{"sku": "P-100", "quantity": "1", "unit_price": "1000"}],
                )
                add_shopify_customer_mapping_in_session(
                    db,
                    shop_domain="greencornercr.com",
                    shopify_customer_gid="gid://shopify/Customer/rollback",
                    customer=customer,
                    phone=customer.phone,
                    email=None,
                )
                add_audit_event_in_session(
                    db,
                    module="shopify",
                    action="phase1a_rollback_test",
                    entity_type="b2c_sales_order",
                    entity_id=order.id,
                    username="integration-test",
                )
                raise RuntimeError("force rollback")
        except RuntimeError:
            pass
        with self.Session() as db:
            self.assertEqual(db.query(B2CCustomer).filter_by(name="Cliente rollback").count(), 0)
            self.assertEqual(db.query(ShopifyCustomerMapping).count(), 0)
            self.assertEqual(db.query(AuditLog).filter_by(action="phase1a_rollback_test").count(), 0)
            self.assertEqual(db.query(B2CSalesOrder).count(), 0)
            self.assertEqual(db.query(AppSequence).count(), 0)

    def test_shipping_cost_csrf_is_bound_to_session(self) -> None:
        token = build_b2c_shipping_cost_csrf_token("session-a")
        validate_b2c_shipping_cost_csrf_token("session-a", token)
        with self.assertRaises(B2CValidationError):
            validate_b2c_shipping_cost_csrf_token("session-b", token)

    def test_shipping_cost_and_audit_share_the_caller_transaction(self) -> None:
        order = self._create_manual_order()
        with self.Session.begin() as db:
            user = User(
                username="atomic-shipping-cost",
                full_name="Atomic Shipping Cost",
                password_hash="not-used",
            )
            db.add(user)
            db.flush()
            user_id = user.id
        try:
            with self.Session.begin() as db:
                record_b2c_shipping_cost_in_session(
                    db,
                    order_id=order.id,
                    amount=Decimal("1800"),
                    source="carrier_invoice",
                    user_id=user_id,
                )
                add_audit_event_in_session(
                    db,
                    module="sales",
                    action="atomic_shipping_cost_test",
                    entity_type="b2c_sales_order",
                    entity_id=order.id,
                    username="atomic-shipping-cost",
                )
                raise RuntimeError("force rollback")
        except RuntimeError:
            pass
        with self.Session() as db:
            self.assertIsNone(db.get(B2CSalesOrder, order.id).shipping_cost_amount)
            self.assertEqual(db.query(AuditLog).filter_by(action="atomic_shipping_cost_test").count(), 0)

    def test_shopify_is_disabled_without_configuration(self) -> None:
        previous = os.environ.pop("SHOPIFY_INTEGRATION_ENABLED", None)
        try:
            self.assertFalse(shopify_integration_enabled())
        finally:
            if previous is not None:
                os.environ["SHOPIFY_INTEGRATION_ENABLED"] = previous

    def _direct_order(self, db, number: str) -> B2CSalesOrder:
        order = B2CSalesOrder(
            order_number=number,
            order_date=date(2026, 10, 25),
            channel="Online Web",
            channel_id=self.channel_id,
            status="draft",
            subtotal_amount=ZERO,
            discount_amount=ZERO,
            total_amount=ZERO,
        )
        db.add(order)
        db.flush()
        return order

    @staticmethod
    def _line(*, sku: str, line_type: str, gross: str, quantity: str) -> B2CSalesOrderLine:
        amount = Decimal(gross)
        return B2CSalesOrderLine(
            line_number=1,
            line_type=line_type,
            sku_snapshot=sku,
            description_snapshot=sku,
            quantity=Decimal(quantity),
            unit_price_snapshot=amount,
            line_total=amount,
            discount_amount_snapshot=ZERO,
            net_line_total_snapshot=amount,
        )


if __name__ == "__main__":
    unittest.main()
