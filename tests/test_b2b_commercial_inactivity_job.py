import json
import tempfile
import unittest
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.database import Base
from app.models import (
    AuditLog,
    B2BCommercialAgreement,
    B2BCustomer,
    B2BCustomerProduct,
    B2BSalesOrder,
    B2BSalesOrderLine,
    Channel,
    Product,
    Role,
    User,
    UserRole,
)
from app.services.b2b_commercial_inactivity_service import (
    B2BCommercialInactivityError,
    AUTHORIZED_B2B_CHANNEL_NAMES,
    CHANNEL_SCOPE_REVISION,
    CLASSIFICATION_ELIGIBLE,
    CLASSIFICATION_EXCLUDED,
    CLASSIFICATION_REVIEW_PENDING,
    EXECUTION_CONFIRMATION,
    InactivityConfig,
    build_b2b_commercial_inactivity_plan,
    execute_b2b_commercial_inactivity_plan,
    plan_summary,
    subtract_calendar_months,
)
from app.services.accounts_receivable_service import build_accounts_receivable_dashboard
from tools.b2b_commercial_inactivity_job import (
    _load_approved_fingerprint,
    _prepare_export_dir,
    _write_evidence,
)


AS_OF_DATE = date(2026, 10, 16)
OLD_TIMESTAMP = datetime(2024, 1, 10, 12, 0, 0)


class B2BCommercialInactivityJobTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, autocommit=False, autoflush=False)
        self.db = self.Session()
        self.config = InactivityConfig(
            as_of_date=AS_OF_DATE,
            history_complete_since=date(2025, 10, 1),
            reactivation_grace_days=30,
        )
        self.admin = self._admin_user()
        self.admin_username = self.admin.username
        self.channels = {
            name: self._channel(name)
            for name in AUTHORIZED_B2B_CHANNEL_NAMES
        }
        self.db.commit()

    def tearDown(self):
        self.db.close()
        Base.metadata.drop_all(self.engine)
        self.engine.dispose()

    def _admin_user(self) -> User:
        user = User(
            username="admin.audit",
            full_name="Admin Audit",
            email="admin.audit@example.test",
            password_hash="not-used",
            is_active=True,
        )
        role = Role(code="admin", name="Admin", is_system=True, active=True)
        self.db.add_all([user, role])
        self.db.flush()
        self.db.add(UserRole(user_id=user.id, role_id=role.id))
        self.db.commit()
        return user

    def _channel(
        self,
        name: str,
        *,
        active: bool = True,
        applies_to_b2b: bool = True,
    ) -> Channel:
        channel = Channel(
            name=name,
            active=active,
            applies_to_b2b=applies_to_b2b,
            applies_to_b2c=False,
        )
        self.db.add(channel)
        self.db.flush()
        return channel

    def _customer(
        self,
        name: str = "Cliente",
        *,
        active: bool = True,
        channel_name: str | None = AUTHORIZED_B2B_CHANNEL_NAMES[0],
        created_at: datetime = OLD_TIMESTAMP,
        updated_at: datetime = OLD_TIMESTAMP,
    ) -> B2BCustomer:
        channel = (
            self.db.query(Channel).filter(Channel.name == channel_name).one()
            if channel_name is not None
            else None
        )
        customer = B2BCustomer(
            customer_name=name,
            credit_days=0,
            channel_id=channel.id if channel is not None else None,
            active=active,
            created_at=created_at,
            updated_at=updated_at,
        )
        self.db.add(customer)
        self.db.flush()
        return customer

    def _catalog_product(
        self,
        customer: B2BCustomer,
        sku: str,
        *,
        active: bool = True,
        created_at: datetime = OLD_TIMESTAMP,
        updated_at: datetime = OLD_TIMESTAMP,
        create_global_product: bool = True,
    ) -> B2BCustomerProduct:
        if create_global_product:
            existing = self.db.query(Product).filter(Product.sku == sku).one_or_none()
            if existing is None:
                self.db.add(
                    Product(
                        sku=sku,
                        name=f"Producto {sku}",
                        active=True,
                        available_for_sale_gc=True,
                        created_at=created_at,
                        updated_at=updated_at,
                    )
                )
        catalog_product = B2BCustomerProduct(
            customer_id=customer.id,
            sku=sku,
            description=f"Producto {sku}",
            distributor_price=Decimal("1000"),
            active=active,
            created_at=created_at,
            updated_at=updated_at,
        )
        self.db.add(catalog_product)
        self.db.flush()
        return catalog_product

    def _order(
        self,
        customer: B2BCustomer,
        order_number: str,
        *,
        status: str,
        sku: str = "100",
        invoice_date: date | None = None,
        invoiced_at: datetime | None = None,
        delivery_date: date = date(2024, 1, 1),
        observations: str | None = None,
        total_amount: Decimal = Decimal("1000"),
        order_channel_name: str | None = None,
    ) -> B2BSalesOrder:
        order_channel = (
            self.db.query(Channel).filter(Channel.name == order_channel_name).one()
            if order_channel_name is not None
            else None
        )
        order = B2BSalesOrder(
            order_number=order_number,
            customer_id=customer.id,
            customer_name_snapshot=customer.customer_name,
            channel_id=order_channel.id if order_channel is not None else None,
            delivery_date=delivery_date,
            status=status,
            total_amount=total_amount,
            invoice_date=invoice_date,
            invoiced_at=invoiced_at,
            observations=observations,
            created_at=OLD_TIMESTAMP,
            updated_at=OLD_TIMESTAMP,
        )
        order.lines = [
            B2BSalesOrderLine(
                line_number=1,
                sku_snapshot=sku,
                description_snapshot=f"Producto {sku}",
                unit_price_snapshot=Decimal("1000"),
                quantity=Decimal("1"),
                line_total=total_amount,
                created_at=OLD_TIMESTAMP,
                updated_at=OLD_TIMESTAMP,
            )
        ]
        self.db.add(order)
        self.db.flush()
        return order

    def _plan(self, config: InactivityConfig | None = None):
        self.db.commit()
        return build_b2b_commercial_inactivity_plan(self.db, config=config or self.config)

    def test_subtracts_twelve_calendar_months_including_leap_day(self):
        self.assertEqual(subtract_calendar_months(date(2026, 10, 16), 12), date(2025, 10, 16))
        self.assertEqual(subtract_calendar_months(date(2024, 2, 29), 12), date(2023, 2, 28))

    def test_no_sale_catalog_product_and_customer_are_eligible(self):
        customer = self._customer()
        catalog_product = self._catalog_product(customer, "100")

        plan = self._plan()

        product_row = next(row for row in plan.catalog_products if row.catalog_product_id == catalog_product.id)
        customer_row = next(row for row in plan.customers if row.customer_id == customer.id)
        self.assertEqual(product_row.classification, CLASSIFICATION_ELIGIBLE)
        self.assertEqual(customer_row.classification, CLASSIFICATION_ELIGIBLE)

    def test_scope_resolves_only_the_four_exact_authorized_channel_names(self):
        for index, channel_name in enumerate(AUTHORIZED_B2B_CHANNEL_NAMES, start=1):
            customer = self._customer(
                f"Cliente {index}",
                channel_name=channel_name,
            )
            self._catalog_product(customer, f"SKU-{index}")
        outside_channel = self._channel("Canal no autorizado")
        outside_customer = self._customer(
            "Fuera de alcance",
            channel_name=outside_channel.name,
        )
        outside_product = self._catalog_product(outside_customer, "OUTSIDE")
        no_channel_customer = self._customer("Sin canal", channel_name=None)
        no_channel_product = self._catalog_product(no_channel_customer, "NO-CHANNEL")

        plan = self._plan()
        summary = plan_summary(plan)

        self.assertTrue(plan.channel_scope_is_valid)
        self.assertEqual(
            tuple(row.expected_name for row in plan.channel_scope_resolutions),
            AUTHORIZED_B2B_CHANNEL_NAMES,
        )
        self.assertEqual({row.status for row in plan.channel_scope_resolutions}, {"resolved"})
        self.assertEqual(len(plan.customers), 4)
        self.assertEqual(len(plan.catalog_products), 4)
        self.assertNotIn(outside_product.id, {row.catalog_product_id for row in plan.catalog_products})
        self.assertNotIn(no_channel_product.id, {row.catalog_product_id for row in plan.catalog_products})
        self.assertEqual(summary["customers_registered_total"], 6)
        self.assertEqual(summary["customers_in_authorized_channels"], 4)
        self.assertEqual(summary["customers_excluded_other_channels"], 1)
        self.assertEqual(summary["customers_without_channel"], 1)
        self.assertEqual(summary["catalog_products_evaluated_in_scope"], 4)
        self.assertEqual(summary["catalog_products_outside_scope"], 2)
        self.assertEqual(
            {row.channel_name: row.customers_evaluated for row in plan.channel_breakdown},
            {name: 1 for name in AUTHORIZED_B2B_CHANNEL_NAMES},
        )
        self.assertEqual(
            {row.channel_name: row.catalog_products_potentially_inactive for row in plan.channel_breakdown},
            {name: 1 for name in AUTHORIZED_B2B_CHANNEL_NAMES},
        )

    def test_in_scope_customer_activity_counts_even_when_order_had_another_channel(self):
        outside_channel = self._channel("Canal histórico distinto")
        customer = self._customer()
        catalog_product = self._catalog_product(customer, "100")
        self._order(
            customer,
            "B2B-DIFFERENT-HISTORICAL-CHANNEL",
            status="invoiced",
            sku="100",
            invoice_date=date(2026, 9, 1),
            order_channel_name=outside_channel.name,
        )

        plan = self._plan()
        product_row = next(
            row for row in plan.catalog_products
            if row.catalog_product_id == catalog_product.id
        )
        customer_row = next(
            row for row in plan.customers if row.customer_id == customer.id
        )

        self.assertEqual(product_row.classification, CLASSIFICATION_EXCLUDED)
        self.assertEqual(customer_row.rolling_invoice_count, 1)
        self.assertEqual(customer_row.classification, CLASSIFICATION_EXCLUDED)

    def test_channel_change_invalidates_approved_fingerprint_and_preserves_flags(self):
        customer = self._customer()
        catalog_product = self._catalog_product(customer, "100")
        self.db.commit()
        approved_plan = build_b2b_commercial_inactivity_plan(self.db, config=self.config)
        customer.channel_id = self.channels[AUTHORIZED_B2B_CHANNEL_NAMES[1]].id
        self.db.commit()
        customer_id = customer.id
        catalog_product_id = catalog_product.id
        self.db.close()

        with self.assertRaisesRegex(B2BCommercialInactivityError, "differs"):
            execute_b2b_commercial_inactivity_plan(
                self.Session,
                config=self.config,
                approved_plan_fingerprint=approved_plan.plan_fingerprint,
                actor_username=self.admin_username,
                confirmation=EXECUTION_CONFIRMATION,
            )

        self.db = self.Session()
        self.assertTrue(self.db.get(B2BCustomer, customer_id).active)
        self.assertTrue(self.db.get(B2BCustomerProduct, catalog_product_id).active)
        self.assertEqual(self.db.query(AuditLog).count(), 0)

    def test_execute_blocks_unresolved_channel_scope(self):
        customer = self._customer()
        catalog_product = self._catalog_product(customer, "100")
        missing_channel = self.channels[AUTHORIZED_B2B_CHANNEL_NAMES[-1]]
        self.db.delete(missing_channel)
        self.db.commit()
        plan = build_b2b_commercial_inactivity_plan(self.db, config=self.config)
        self.assertFalse(plan.channel_scope_is_valid)
        customer_id = customer.id
        catalog_product_id = catalog_product.id
        self.db.close()

        with self.assertRaisesRegex(B2BCommercialInactivityError, "could not be resolved"):
            execute_b2b_commercial_inactivity_plan(
                self.Session,
                config=self.config,
                approved_plan_fingerprint=plan.plan_fingerprint,
                actor_username=self.admin_username,
                confirmation=EXECUTION_CONFIRMATION,
            )

        self.db = self.Session()
        self.assertTrue(self.db.get(B2BCustomer, customer_id).active)
        self.assertTrue(self.db.get(B2BCustomerProduct, catalog_product_id).active)

    def test_execute_never_deactivates_customers_or_products_outside_scope(self):
        included_customer = self._customer("Incluido")
        included_product = self._catalog_product(included_customer, "IN")
        outside_channel = self._channel("Canal fuera de alcance")
        outside_customer = self._customer(
            "Excluido",
            channel_name=outside_channel.name,
        )
        outside_product = self._catalog_product(outside_customer, "OUT")
        no_channel_customer = self._customer("Sin canal", channel_name=None)
        no_channel_product = self._catalog_product(no_channel_customer, "NONE")
        self.db.commit()
        plan = build_b2b_commercial_inactivity_plan(self.db, config=self.config)
        ids = {
            "included_customer": included_customer.id,
            "included_product": included_product.id,
            "outside_customer": outside_customer.id,
            "outside_product": outside_product.id,
            "no_channel_customer": no_channel_customer.id,
            "no_channel_product": no_channel_product.id,
        }
        self.db.close()

        result = execute_b2b_commercial_inactivity_plan(
            self.Session,
            config=self.config,
            approved_plan_fingerprint=plan.plan_fingerprint,
            actor_username=self.admin_username,
            confirmation=EXECUTION_CONFIRMATION,
        )

        self.db = self.Session()
        self.assertFalse(self.db.get(B2BCustomer, ids["included_customer"]).active)
        self.assertFalse(self.db.get(B2BCustomerProduct, ids["included_product"]).active)
        self.assertTrue(self.db.get(B2BCustomer, ids["outside_customer"]).active)
        self.assertTrue(self.db.get(B2BCustomerProduct, ids["outside_product"]).active)
        self.assertTrue(self.db.get(B2BCustomer, ids["no_channel_customer"]).active)
        self.assertTrue(self.db.get(B2BCustomerProduct, ids["no_channel_product"]).active)
        self.assertEqual(
            {(change.entity_type, change.entity_id) for change in result.changes},
            {
                ("b2b_customer", ids["included_customer"]),
                ("b2b_customer_product", ids["included_product"]),
            },
        )

    def test_pre_channel_scope_summary_is_rejected_as_obsolete(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            summary_path = Path(temp_dir) / "summary.json"
            summary_path.write_text(
                json.dumps({"plan_fingerprint": "legacy-fingerprint"}),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(B2BCommercialInactivityError, "obsolete"):
                _load_approved_fingerprint(summary_path)

            summary_path.write_text(
                json.dumps(
                    {
                        "channel_scope_revision": CHANNEL_SCOPE_REVISION,
                        "plan_fingerprint": "current-fingerprint",
                    }
                ),
                encoding="utf-8",
            )
            self.assertEqual(
                _load_approved_fingerprint(summary_path),
                "current-fingerprint",
            )

    def test_current_invoice_uses_canonical_date_and_excludes_activity(self):
        customer = self._customer()
        catalog_product = self._catalog_product(customer, "100")
        self._order(
            customer,
            "B2B-CURRENT",
            status="invoiced",
            sku="100",
            invoice_date=date(2026, 9, 1),
            invoiced_at=datetime(2024, 1, 1),
            delivery_date=date(2024, 1, 1),
        )

        plan = self._plan()

        product_row = next(row for row in plan.catalog_products if row.catalog_product_id == catalog_product.id)
        customer_row = next(row for row in plan.customers if row.customer_id == customer.id)
        self.assertEqual(product_row.last_purchase_date, date(2026, 9, 1))
        self.assertEqual(product_row.classification, CLASSIFICATION_EXCLUDED)
        self.assertEqual(customer_row.rolling_invoice_count, 1)
        self.assertEqual(customer_row.classification, CLASSIFICATION_EXCLUDED)
        september = next(row for row in plan.history_coverage if row.period_month == "2026-09")
        self.assertEqual(september.invoiced_orders, 1)
        self.assertEqual(september.regular_orders, 1)
        self.assertEqual(september.historical_import_orders, 0)

    def test_historical_delivery_date_is_used_only_for_historical_marker(self):
        historical_customer = self._customer("Histórico")
        historical_product = self._catalog_product(historical_customer, "HIST")
        self._order(
            historical_customer,
            "B2B-HIST",
            status="invoiced",
            sku="HIST",
            delivery_date=date(2026, 8, 1),
            observations="Historical CSV import 123",
        )
        legacy_customer = self._customer("Legacy")
        legacy_product = self._catalog_product(legacy_customer, "LEG")
        self._order(
            legacy_customer,
            "B2B-LEGACY",
            status="invoiced",
            sku="LEG",
            delivery_date=date(2026, 8, 1),
            observations="Normal legacy record",
        )

        plan = self._plan()
        rows = {row.catalog_product_id: row for row in plan.catalog_products}

        self.assertEqual(rows[historical_product.id].last_purchase_date, date(2026, 8, 1))
        self.assertEqual(rows[historical_product.id].classification, CLASSIFICATION_EXCLUDED)
        self.assertEqual(rows[legacy_product.id].last_purchase_date, OLD_TIMESTAMP.date())
        self.assertEqual(rows[legacy_product.id].classification, CLASSIFICATION_ELIGIBLE)

    def test_draft_and_cancelled_are_not_sales_but_open_order_requires_review(self):
        customer = self._customer()
        draft_product = self._catalog_product(customer, "DRAFT")
        cancelled_product = self._catalog_product(customer, "CANCEL")
        self._order(customer, "B2B-DRAFT", status="draft", sku="DRAFT")
        self._order(customer, "B2B-CANCEL", status="cancelled", sku="CANCEL")

        plan = self._plan()
        rows = {row.catalog_product_id: row for row in plan.catalog_products}
        customer_row = next(row for row in plan.customers if row.customer_id == customer.id)

        self.assertEqual(rows[draft_product.id].classification, CLASSIFICATION_REVIEW_PENDING)
        self.assertEqual(rows[cancelled_product.id].classification, CLASSIFICATION_ELIGIBLE)
        self.assertEqual(customer_row.classification, CLASSIFICATION_REVIEW_PENDING)

    def test_invoice_for_sku_outside_current_catalog_prevents_customer_deactivation(self):
        customer = self._customer()
        catalog_product = self._catalog_product(customer, "CURRENT-CATALOG")
        self._order(
            customer,
            "B2B-OLD-SKU",
            status="invoiced",
            sku="NO-LONGER-CATALOGED",
            invoice_date=date(2026, 7, 15),
        )

        plan = self._plan()
        product_row = next(row for row in plan.catalog_products if row.catalog_product_id == catalog_product.id)
        customer_row = next(row for row in plan.customers if row.customer_id == customer.id)

        self.assertEqual(product_row.classification, CLASSIFICATION_ELIGIBLE)
        self.assertEqual(customer_row.classification, CLASSIFICATION_EXCLUDED)
        self.assertEqual(customer_row.rolling_invoice_count, 1)

    def test_missing_coverage_new_records_and_unknown_sku_require_review(self):
        new_timestamp = datetime(2026, 9, 1, 12, 0, 0)
        customer = self._customer(created_at=new_timestamp, updated_at=new_timestamp)
        catalog_product = self._catalog_product(
            customer,
            "UNKNOWN",
            created_at=new_timestamp,
            updated_at=new_timestamp,
            create_global_product=False,
        )
        config = InactivityConfig(as_of_date=AS_OF_DATE, history_complete_since=None)

        plan = self._plan(config)
        product_row = next(row for row in plan.catalog_products if row.catalog_product_id == catalog_product.id)
        customer_row = next(row for row in plan.customers if row.customer_id == customer.id)

        self.assertEqual(product_row.classification, CLASSIFICATION_REVIEW_PENDING)
        self.assertTrue(any("cobertura histórica" in reason for reason in product_row.reasons))
        self.assertTrue(any("Product actual" in reason for reason in product_row.reasons))
        self.assertEqual(customer_row.classification, CLASSIFICATION_REVIEW_PENDING)

    def test_recent_manual_reactivation_receives_grace_without_schema_change(self):
        customer = self._customer(updated_at=datetime(2026, 10, 10, 12, 0, 0))
        catalog_product = self._catalog_product(
            customer,
            "100",
            updated_at=datetime(2026, 10, 10, 12, 0, 0),
        )

        plan = self._plan()
        product_row = next(row for row in plan.catalog_products if row.catalog_product_id == catalog_product.id)
        customer_row = next(row for row in plan.customers if row.customer_id == customer.id)

        self.assertEqual(product_row.classification, CLASSIFICATION_REVIEW_PENDING)
        self.assertTrue(any("período de gracia" in reason for reason in product_row.reasons))
        self.assertEqual(customer_row.classification, CLASSIFICATION_REVIEW_PENDING)

    def test_pending_receivables_and_agreements_are_reported_but_preserved(self):
        customer = self._customer()
        self._catalog_product(customer, "100")
        self._order(
            customer,
            "B2B-OLD-PENDING",
            status="invoiced",
            sku="100",
            invoiced_at=datetime(2025, 1, 1, 12, 0, 0),
            total_amount=Decimal("14100"),
        )
        agreement = B2BCommercialAgreement(
            customer_id=customer.id,
            version_number=0,
            status="signed",
            credit_days_snapshot=0,
            customer_active_snapshot=True,
            template_revision="test",
            signed_at=datetime(2025, 2, 1, 12, 0, 0),
        )
        self.db.add(agreement)

        plan = self._plan()
        customer_row = next(row for row in plan.customers if row.customer_id == customer.id)

        self.assertEqual(customer_row.classification, CLASSIFICATION_ELIGIBLE)
        self.assertEqual(customer_row.pending_receivables_amount, Decimal("14100"))
        self.assertEqual(customer_row.current_agreement_statuses, ("signed",))
        self.assertTrue(any("saldo pendiente" in reason for reason in customer_row.reasons))
        self.assertTrue(any("acuerdos comerciales" in reason for reason in customer_row.reasons))

    def test_execute_changes_only_customer_catalog_flags_and_writes_atomic_audit(self):
        customer = self._customer()
        catalog_product = self._catalog_product(customer, "100")
        order = self._order(
            customer,
            "B2B-OLD-PRESERVED",
            status="invoiced",
            sku="100",
            invoice_date=date(2025, 1, 1),
            invoiced_at=datetime(2025, 1, 1, 12, 0, 0),
            total_amount=Decimal("1000"),
        )
        agreement = B2BCommercialAgreement(
            customer_id=customer.id,
            version_number=0,
            status="signed",
            credit_days_snapshot=0,
            customer_active_snapshot=True,
            template_revision="test",
            signed_at=datetime(2025, 2, 1, 12, 0, 0),
        )
        self.db.add(agreement)
        self.db.commit()
        global_product = self.db.query(Product).filter(Product.sku == "100").one()
        plan = build_b2b_commercial_inactivity_plan(self.db, config=self.config)
        self.db.close()

        result = execute_b2b_commercial_inactivity_plan(
            self.Session,
            config=self.config,
            approved_plan_fingerprint=plan.plan_fingerprint,
            actor_username=self.admin_username,
            confirmation=EXECUTION_CONFIRMATION,
        )

        self.db = self.Session()
        refreshed_customer = self.db.query(B2BCustomer).filter(B2BCustomer.id == customer.id).one()
        refreshed_catalog = (
            self.db.query(B2BCustomerProduct)
            .filter(B2BCustomerProduct.id == catalog_product.id)
            .one()
        )
        refreshed_global = self.db.query(Product).filter(Product.id == global_product.id).one()
        refreshed_order = self.db.get(B2BSalesOrder, order.id)
        refreshed_agreement = self.db.get(B2BCommercialAgreement, agreement.id)
        audits = self.db.query(AuditLog).order_by(AuditLog.id).all()
        receivables = build_accounts_receivable_dashboard(self.db, today=AS_OF_DATE)
        receivable = next(row for row in receivables.rows if row.order_id == order.id)
        self.assertFalse(refreshed_customer.active)
        self.assertFalse(refreshed_catalog.active)
        self.assertTrue(refreshed_global.active)
        self.assertTrue(refreshed_global.available_for_sale_gc)
        self.assertEqual(refreshed_order.status, "invoiced")
        self.assertEqual(refreshed_order.invoice_date, date(2025, 1, 1))
        self.assertEqual(receivable.pending_amount, Decimal("1000"))
        self.assertEqual(refreshed_agreement.status, "signed")
        self.assertEqual(len(result.changes), 2)
        self.assertEqual(len(audits), 2)
        self.assertEqual({audit.username for audit in audits}, {self.admin_username})

        second_plan = build_b2b_commercial_inactivity_plan(self.db, config=self.config)
        self.assertEqual(second_plan.eligible_catalog_products, ())
        self.assertEqual(second_plan.eligible_customers, ())

    def test_execute_rolls_back_flags_and_audit_when_validation_fails(self):
        customer = self._customer()
        catalog_product = self._catalog_product(customer, "100")
        self.db.commit()
        plan = build_b2b_commercial_inactivity_plan(self.db, config=self.config)
        self.db.close()

        def fail_before_commit(_db):
            raise RuntimeError("forced failure")

        with self.assertRaisesRegex(RuntimeError, "forced failure"):
            execute_b2b_commercial_inactivity_plan(
                self.Session,
                config=self.config,
                approved_plan_fingerprint=plan.plan_fingerprint,
                actor_username=self.admin_username,
                confirmation=EXECUTION_CONFIRMATION,
                before_commit=fail_before_commit,
            )

        self.db = self.Session()
        self.assertTrue(self.db.get(B2BCustomer, customer.id).active)
        self.assertTrue(self.db.get(B2BCustomerProduct, catalog_product.id).active)
        self.assertEqual(self.db.query(AuditLog).count(), 0)

    def test_execute_requires_sufficient_coverage_admin_and_exact_approved_plan(self):
        customer = self._customer()
        self._catalog_product(customer, "100")
        self.db.commit()
        plan = build_b2b_commercial_inactivity_plan(self.db, config=self.config)
        self.db.close()

        with self.assertRaises(B2BCommercialInactivityError):
            execute_b2b_commercial_inactivity_plan(
                self.Session,
                config=self.config,
                approved_plan_fingerprint="wrong",
                actor_username=self.admin_username,
                confirmation=EXECUTION_CONFIRMATION,
            )
        with self.assertRaises(B2BCommercialInactivityError):
            execute_b2b_commercial_inactivity_plan(
                self.Session,
                config=InactivityConfig(as_of_date=AS_OF_DATE),
                approved_plan_fingerprint=plan.plan_fingerprint,
                actor_username=self.admin_username,
                confirmation=EXECUTION_CONFIRMATION,
            )
        with self.assertRaises(B2BCommercialInactivityError):
            execute_b2b_commercial_inactivity_plan(
                self.Session,
                config=self.config,
                approved_plan_fingerprint=plan.plan_fingerprint,
                actor_username=self.admin_username,
                confirmation="WRONG",
            )

    def test_evidence_is_complete_and_nonempty_directory_is_not_overwritten(self):
        customer = self._customer()
        self._catalog_product(customer, "100")
        plan = self._plan()

        with tempfile.TemporaryDirectory() as temp_dir:
            export_dir = Path(temp_dir) / "evidence"
            _prepare_export_dir(export_dir)
            _write_evidence(export_dir, plan=plan, mode="dry-run")

            expected_files = {
                "summary.json",
                "catalog_products.csv",
                "customers.csv",
                "history_coverage.csv",
                "review_pending.csv",
                "excluded.csv",
            }
            self.assertEqual({path.name for path in export_dir.iterdir()}, expected_files)
            summary = json.loads((export_dir / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["plan_fingerprint"], plan.plan_fingerprint)
            self.assertEqual(summary["catalog_products_eligible"], 1)
            self.assertEqual(summary["customers_eligible"], 1)
            with self.assertRaises(B2BCommercialInactivityError):
                _prepare_export_dir(export_dir)


if __name__ == "__main__":
    unittest.main()
