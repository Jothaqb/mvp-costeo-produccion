import unittest
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.database import Base
from app.models import (
    AccountingRubric,
    AccountingSubrubric,
    B2BCustomer,
    B2BSalesOrder,
    B2BSalesOrderLine,
    B2CSalesOrder,
    B2CSalesOrderLine,
    InventoryBalance,
    InventoryTransaction,
)
from app.services.accounting_income_statement_service import (
    AccountingValidationError,
    build_income_statement,
    create_accounting_rubric,
    create_accounting_subrubric,
    get_accounting_sales_summary,
    save_monthly_actuals,
    save_monthly_subrubric_actuals,
    update_accounting_rubric,
    update_accounting_subrubric,
)


class AccountingIncomeStatementTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)
        with self.Session.begin() as db:
            customer = B2BCustomer(customer_name="Cliente contable", credit_days=30)
            db.add(customer)
            db.flush()
            self.customer_id = customer.id
            self.rubrics = {}
            for display_order, (code, name, section, budget) in enumerate(
                (
                    ("op_test", "Operativo", "operating", Decimal("100")),
                    ("admin_test", "Administrativo", "administrative", Decimal("50")),
                    ("financial_test", "Financiero", "financial", Decimal("0")),
                    ("tax_test", "Impuestos pagados", "tax", Decimal("25")),
                ),
                start=1,
            ):
                rubric = AccountingRubric(
                    code=code,
                    name=name,
                    section=section,
                    monthly_budget_amount=budget,
                    display_order=display_order,
                )
                db.add(rubric)
                db.flush()
                self.rubrics[code] = rubric.id

    def tearDown(self) -> None:
        Base.metadata.drop_all(self.engine)
        self.engine.dispose()

    def _add_b2b(
        self,
        db,
        *,
        number: str,
        delivery_date: date,
        invoice_date: date | None,
        status: str = "invoiced",
        line_total: Decimal = Decimal("100"),
        cogs: Decimal | None = Decimal("40"),
        invoiced_at: datetime | None = None,
        observations: str = "Normal order",
    ) -> None:
        order = B2BSalesOrder(
            order_number=number,
            customer_id=self.customer_id,
            customer_name_snapshot="Cliente contable",
            delivery_date=delivery_date,
            invoice_date=invoice_date,
            invoiced_at=invoiced_at,
            status=status,
            total_amount=line_total,
            observations=observations,
        )
        db.add(order)
        db.flush()
        db.add(
            B2BSalesOrderLine(
                sales_order_id=order.id,
                line_number=1,
                sku_snapshot=f"SKU-{number}",
                description_snapshot=number,
                unit_price_snapshot=line_total,
                quantity=Decimal("1"),
                line_total=line_total,
                cost_total_snapshot=cogs,
            )
        )

    def _add_b2c(
        self,
        db,
        *,
        number: str,
        order_date: date,
        status: str = "invoiced",
        gross: Decimal = Decimal("200"),
        net: Decimal = Decimal("180"),
        cogs: Decimal | None = Decimal("90"),
    ) -> None:
        order = B2CSalesOrder(
            order_number=number,
            order_date=order_date,
            channel="Tienda",
            status=status,
            subtotal_amount=gross,
            discount_amount=gross - net,
            total_amount=net,
        )
        db.add(order)
        db.flush()
        db.add(
            B2CSalesOrderLine(
                sales_order_id=order.id,
                line_number=1,
                sku_snapshot=f"SKU-{number}",
                description_snapshot=number,
                quantity=Decimal("1"),
                unit_price_snapshot=gross,
                line_total=gross,
                discount_amount_snapshot=gross - net,
                net_line_total_snapshot=net,
                cost_total_snapshot=cogs,
            )
        )

    def test_sales_use_accounting_dates_invoiced_only_and_line_snapshots(self) -> None:
        with self.Session.begin() as db:
            self._add_b2b(
                db,
                number="B2B-INVOICE-DATE",
                delivery_date=date(2026, 6, 30),
                invoice_date=date(2026, 7, 1),
            )
            self._add_b2b(
                db,
                number="B2B-DRAFT",
                delivery_date=date(2026, 7, 1),
                invoice_date=date(2026, 7, 1),
                status="draft",
                line_total=Decimal("999"),
                cogs=Decimal("999"),
            )
            self._add_b2c(db, number="B2C-JULY", order_date=date(2026, 7, 20))
            self._add_b2c(db, number="B2C-JUNE", order_date=date(2026, 6, 30))

        with self.Session() as db:
            summary = get_accounting_sales_summary(db, date(2026, 7, 1))

        self.assertEqual(summary.revenue, Decimal("280.0000"))
        self.assertEqual(summary.cogs, Decimal("130.0000"))
        self.assertEqual(summary.gross_profit, Decimal("150.0000"))
        self.assertEqual(summary.b2b_orders, 1)
        self.assertEqual(summary.b2c_orders, 1)
        self.assertTrue(summary.has_complete_cogs)

    def test_b2b_invoice_date_precedes_invoiced_and_delivery_dates(self) -> None:
        with self.Session.begin() as db:
            self._add_b2b(
                db,
                number="B2B-PRECEDENCE",
                delivery_date=date(2026, 5, 1),
                invoice_date=date(2026, 7, 10),
                invoiced_at=datetime(2026, 6, 1, 12, 0),
            )

        with self.Session() as db:
            may = get_accounting_sales_summary(db, date(2026, 5, 1))
            june = get_accounting_sales_summary(db, date(2026, 6, 1))
            july = get_accounting_sales_summary(db, date(2026, 7, 1))

        self.assertEqual(may.revenue, Decimal("0.0000"))
        self.assertEqual(june.revenue, Decimal("0.0000"))
        self.assertEqual(july.revenue, Decimal("100.0000"))

    def test_missing_cost_snapshot_warns_and_hides_profit(self) -> None:
        with self.Session.begin() as db:
            self._add_b2b(
                db,
                number="B2B-MISSING-COST",
                delivery_date=date(2026, 8, 1),
                invoice_date=date(2026, 8, 1),
                cogs=None,
            )

        with self.Session() as db:
            statement = build_income_statement(db, date(2026, 8, 1))

        self.assertFalse(statement.sales.has_complete_cogs)
        self.assertEqual(statement.sales.cogs_coverage_label, "0/1 líneas")
        self.assertIsNone(statement.sales.cogs)
        self.assertIsNone(statement.sales.gross_profit)
        self.assertIsNone(statement.profit_before_tax)
        self.assertIsNone(statement.period_result)

    def test_manual_expenses_budget_and_comparisons(self) -> None:
        with self.Session.begin() as db:
            save_monthly_actuals(
                db,
                period_month=date(2026, 7, 15),
                values_by_rubric_id={
                    self.rubrics["op_test"]: ("120", "Operación"),
                    self.rubrics["admin_test"]: ("30", "Administración"),
                    self.rubrics["financial_test"]: ("10", "Banco"),
                    self.rubrics["tax_test"]: ("5", "Impuesto"),
                },
                user_id=None,
            )

        with self.Session() as db:
            july = build_income_statement(db, date(2026, 7, 1))
            august = build_income_statement(db, date(2026, 8, 1))

        self.assertEqual(july.operating_expenses, Decimal("120.0000"))
        self.assertEqual(july.administrative_expenses, Decimal("30.0000"))
        self.assertEqual(july.financial_expenses, Decimal("10.0000"))
        self.assertEqual(july.taxes_paid, Decimal("5.0000"))
        operating = next(section for section in july.sections if section.code == "operating")
        self.assertEqual(operating.budget_amount, Decimal("100.0000"))
        self.assertEqual(operating.difference_amount, Decimal("20.0000"))
        self.assertEqual(operating.difference_percent, Decimal("20.00"))
        self.assertEqual(operating.compliance_percent, Decimal("120.00"))
        august_operating = next(section for section in august.sections if section.code == "operating")
        self.assertEqual(august_operating.budget_amount, Decimal("100.0000"))
        self.assertEqual(august_operating.actual_amount, Decimal("0.0000"))
        financial = next(section for section in july.sections if section.code == "financial")
        self.assertEqual(financial.difference_amount, Decimal("10.0000"))
        self.assertIsNone(financial.difference_percent)
        self.assertIsNone(financial.compliance_percent)

    def test_building_statement_does_not_touch_inventory_tables(self) -> None:
        with self.Session() as db:
            balances_before = db.query(InventoryBalance).count()
            transactions_before = db.query(InventoryTransaction).count()
            build_income_statement(db, date(2026, 7, 1))
            balances_after = db.query(InventoryBalance).count()
            transactions_after = db.query(InventoryTransaction).count()

        self.assertEqual(balances_after, balances_before)
        self.assertEqual(transactions_after, transactions_before)

    def test_create_new_rubric_and_use_it_in_monthly_actuals(self) -> None:
        with self.Session.begin() as db:
            rubric = create_accounting_rubric(
                db,
                code="op_new_service",
                name="Nuevo servicio",
                section="operating",
                monthly_budget_amount="250.50",
                display_order="45",
                active=True,
            )
            rubric_id = rubric.id
            save_monthly_actuals(
                db,
                period_month=date(2026, 9, 1),
                values_by_rubric_id={rubric_id: ("125.25", "Rubro nuevo activo")},
                user_id=None,
            )

        with self.Session() as db:
            statement = build_income_statement(db, date(2026, 9, 1))
            operating = next(section for section in statement.sections if section.code == "operating")
            line = next(item for item in operating.lines if item.rubric.id == rubric_id)

        self.assertEqual(line.rubric.name, "Nuevo servicio")
        self.assertEqual(line.actual_amount, Decimal("125.2500"))
        self.assertEqual(line.budget_amount, Decimal("250.5000"))

    def test_edit_rubric_name_section_budget_order_and_active_state(self) -> None:
        rubric_id = self.rubrics["op_test"]
        with self.Session.begin() as db:
            rubric = db.get(AccountingRubric, rubric_id)
            update_accounting_rubric(
                db,
                rubric,
                name="Operativo editado",
                section="administrative",
                monthly_budget_amount="321.75",
                display_order="99",
                active=False,
            )

        with self.Session() as db:
            rubric = db.get(AccountingRubric, rubric_id)
            self.assertEqual(rubric.name, "Operativo editado")
            self.assertEqual(rubric.section, "administrative")
            self.assertEqual(rubric.monthly_budget_amount, Decimal("321.7500"))
            self.assertEqual(rubric.display_order, 99)
            self.assertFalse(rubric.active)

        with self.Session.begin() as db:
            rubric = db.get(AccountingRubric, rubric_id)
            update_accounting_rubric(
                db,
                rubric,
                name=rubric.name,
                section=rubric.section,
                monthly_budget_amount=rubric.monthly_budget_amount,
                display_order=rubric.display_order,
                active=True,
            )

        with self.Session() as db:
            self.assertTrue(db.get(AccountingRubric, rubric_id).active)

    def test_duplicate_rubric_code_is_rejected(self) -> None:
        with self.Session.begin() as db:
            with self.assertRaises(AccountingValidationError):
                create_accounting_rubric(
                    db,
                    code="op_test",
                    name="Duplicado",
                    section="operating",
                    monthly_budget_amount="0",
                    display_order="50",
                    active=True,
                )

    def test_accounting_forms_use_full_width_and_expose_rubric_controls(self) -> None:
        project_root = Path(__file__).resolve().parents[1]
        rubrics_template = (project_root / "app/templates/accounting_rubrics.html").read_text(encoding="utf-8")
        actuals_template = (project_root / "app/templates/accounting_monthly_actuals.html").read_text(encoding="utf-8")

        self.assertIn('class="wide-form" style="width: 100%;"', rubrics_template)
        self.assertIn('name="name_{{ rubric.id }}"', rubrics_template)
        self.assertIn('name="section_{{ rubric.id }}"', rubrics_template)
        self.assertIn('name="active_{{ rubric.id }}"', rubrics_template)
        self.assertIn('action="/accounting/rubrics/new"', rubrics_template)
        self.assertIn('class="wide-form" style="width: 100%;"', actuals_template)
        self.assertIn('id="rubric_filter"', actuals_template)
        self.assertIn('data-rubric-search="{{ line.rubric.name }} {{ line.rubric.code }} {{ section.code }} {{ section.label }}', actuals_template)
        self.assertIn('filterInput.addEventListener("input", applyFilter)', actuals_template)
        self.assertIn('row.hidden = !matches', actuals_template)

    def test_create_edit_deactivate_and_reactivate_subrubric(self) -> None:
        with self.Session.begin() as db:
            rubric = db.get(AccountingRubric, self.rubrics["op_test"])
            subrubric = create_accounting_subrubric(
                db,
                rubric=rubric,
                code="op_test_person_1",
                name="Persona 1",
                monthly_budget_amount="200",
                display_order="1",
                active=True,
            )
            subrubric_id = subrubric.id
            update_accounting_subrubric(
                db,
                subrubric,
                name="Persona operaria 1",
                monthly_budget_amount="225.50",
                display_order="2",
                active=False,
            )

        with self.Session() as db:
            subrubric = db.get(AccountingSubrubric, subrubric_id)
            self.assertEqual(subrubric.name, "Persona operaria 1")
            self.assertEqual(subrubric.monthly_budget_amount, Decimal("225.5000"))
            self.assertEqual(subrubric.display_order, 2)
            self.assertFalse(subrubric.active)

        with self.Session.begin() as db:
            subrubric = db.get(AccountingSubrubric, subrubric_id)
            update_accounting_subrubric(
                db,
                subrubric,
                name=subrubric.name,
                monthly_budget_amount=subrubric.monthly_budget_amount,
                display_order=subrubric.display_order,
                active=True,
            )
        with self.Session() as db:
            self.assertTrue(db.get(AccountingSubrubric, subrubric_id).active)

    def test_subrubric_actuals_and_budgets_roll_up_to_parent(self) -> None:
        with self.Session.begin() as db:
            rubric = db.get(AccountingRubric, self.rubrics["op_test"])
            first = create_accounting_subrubric(
                db,
                rubric=rubric,
                code="op_test_first",
                name="Primero",
                monthly_budget_amount="60",
                display_order="1",
            )
            second = create_accounting_subrubric(
                db,
                rubric=rubric,
                code="op_test_second",
                name="Segundo",
                monthly_budget_amount="40",
                display_order="2",
            )
            first_id, second_id = first.id, second.id
            save_monthly_subrubric_actuals(
                db,
                period_month=date(2026, 10, 1),
                values_by_subrubric_id={
                    first_id: ("35", "Primer gasto"),
                    second_id: ("45", "Segundo gasto"),
                },
                user_id=None,
            )

        with self.Session() as db:
            statement = build_income_statement(db, date(2026, 10, 1))
            operating = next(section for section in statement.sections if section.code == "operating")
            parent = next(line for line in operating.lines if line.rubric.id == self.rubrics["op_test"])

        self.assertTrue(parent.uses_subrubrics)
        self.assertEqual(parent.direct_actual_amount, Decimal("0.0000"))
        self.assertEqual(parent.actual_amount, Decimal("80.0000"))
        self.assertEqual(parent.budget_amount, Decimal("100.0000"))
        self.assertEqual(len(parent.subrubrics), 2)
        self.assertEqual(sum((line.actual_amount for line in parent.subrubrics), Decimal("0")), Decimal("80.0000"))

    def test_existing_direct_actual_is_preserved_after_subrubrics_are_added(self) -> None:
        rubric_id = self.rubrics["admin_test"]
        with self.Session.begin() as db:
            save_monthly_actuals(
                db,
                period_month=date(2026, 6, 1),
                values_by_rubric_id={rubric_id: ("75", "Monto directo existente")},
                user_id=None,
            )
            rubric = db.get(AccountingRubric, rubric_id)
            create_accounting_subrubric(
                db,
                rubric=rubric,
                code="admin_test_detail",
                name="Detalle futuro",
                monthly_budget_amount="50",
                display_order="1",
            )

        with self.Session() as db:
            statement = build_income_statement(db, date(2026, 6, 1))
            administrative = next(section for section in statement.sections if section.code == "administrative")
            parent = next(line for line in administrative.lines if line.rubric.id == rubric_id)

        self.assertEqual(parent.direct_actual_amount, Decimal("75.0000"))
        self.assertEqual(parent.actual_amount, Decimal("75.0000"))
        self.assertEqual(parent.notes, "Monto directo existente")

    def test_templates_expose_subrubric_loading_filter_and_statement_detail(self) -> None:
        project_root = Path(__file__).resolve().parents[1]
        rubrics_template = (project_root / "app/templates/accounting_rubrics.html").read_text(encoding="utf-8")
        actuals_template = (project_root / "app/templates/accounting_monthly_actuals.html").read_text(encoding="utf-8")
        statement_template = (project_root / "app/templates/accounting_income_statement.html").read_text(encoding="utf-8")

        self.assertIn('action="/accounting/subrubrics/new"', rubrics_template)
        self.assertIn('name="sub_name_{{ subrubric.id }}"', rubrics_template)
        self.assertIn('name="subactual_{{ subline.subrubric.id }}"', actuals_template)
        self.assertIn('{{ subline.subrubric.name }} {{ subline.subrubric.code }}', actuals_template)
        self.assertIn('{% for subline in line.subrubrics %}', statement_template)


if __name__ == "__main__":
    unittest.main()
