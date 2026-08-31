import csv
import io
import unittest
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.database import Base
from app.models import (
    AccountingMonthlyActual,
    AccountingRubric,
    AccountingSubrubric,
    AccountingSubrubricMonthlyActual,
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
    MONTHLY_ACTUALS_TEMPLATE_HEADERS,
    build_income_statement,
    build_monthly_actuals_template_rows,
    build_monthly_expense_export_rows,
    create_accounting_rubric,
    create_accounting_subrubric,
    get_accounting_sales_summary,
    import_monthly_actuals_csv,
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

    def _monthly_csv(
        self,
        rows: list[dict[str, object]],
        headers=None,
        *,
        delimiter: str = ",",
        encoding: str = "utf-8-sig",
    ) -> bytes:
        buffer = io.StringIO(newline="")
        writer = csv.DictWriter(
            buffer,
            fieldnames=headers or MONTHLY_ACTUALS_TEMPLATE_HEADERS,
            delimiter=delimiter,
        )
        writer.writeheader()
        writer.writerows(rows)
        return buffer.getvalue().encode(encoding)

    def _monthly_row(self, **overrides: object) -> dict[str, object]:
        row: dict[str, object] = {
            "Periodo": "2026-11",
            "Grupo": "Gastos Operativos",
            "Cuenta": "Operativo",
            "Subcuenta": "",
            "Codigo Cuenta": "op_test",
            "Codigo Subcuenta": "",
            "Presupuesto mensual": "100.0000",
            "Monto real": "25",
            "Observacion": "Carga CSV",
        }
        row.update(overrides)
        return row

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

    def test_expense_group_totals_exclude_taxes_and_export_all_row_types(self) -> None:
        with self.Session.begin() as db:
            operating = db.get(AccountingRubric, self.rubrics["op_test"])
            detail = create_accounting_subrubric(
                db,
                rubric=operating,
                code="op_test_detail",
                name="Detalle operativo",
                monthly_budget_amount="100",
                display_order="1",
            )
            detail_id = detail.id
            save_monthly_actuals(
                db,
                period_month=date(2026, 11, 1),
                values_by_rubric_id={
                    self.rubrics["admin_test"]: ("30", "Administración"),
                    self.rubrics["financial_test"]: ("10", "Banco"),
                    self.rubrics["tax_test"]: ("5", "Impuesto"),
                },
                user_id=None,
            )
            save_monthly_subrubric_actuals(
                db,
                period_month=date(2026, 11, 1),
                values_by_subrubric_id={detail_id: ("80", "Detalle mensual")},
                user_id=None,
            )

        with self.Session() as db:
            statement = build_income_statement(db, date(2026, 11, 1))
            rows = build_monthly_expense_export_rows(statement)

        sections = {section.code: section for section in statement.sections}
        self.assertEqual(sections["operating"].budget_amount, Decimal("100.0000"))
        self.assertEqual(sections["operating"].actual_amount, Decimal("80.0000"))
        self.assertEqual(sections["administrative"].actual_amount, Decimal("30.0000"))
        self.assertEqual(sections["financial"].actual_amount, Decimal("10.0000"))
        self.assertEqual(sections["tax"].actual_amount, Decimal("5.0000"))
        self.assertEqual(statement.expense_totals.budget_amount, Decimal("150.0000"))
        self.assertEqual(statement.expense_totals.actual_amount, Decimal("120.0000"))
        self.assertEqual(statement.expense_totals.difference_amount, Decimal("-30.0000"))
        self.assertEqual(statement.expense_totals.compliance_percent, Decimal("80.00"))

        row_types = [row[9] for row in rows]
        self.assertIn("cuenta", row_types)
        self.assertIn("subcuenta", row_types)
        self.assertEqual(row_types.count("total_grupo"), 4)
        self.assertEqual(row_types[-1], "gastos_totales")
        self.assertNotIn("op_test", " ".join(str(value) for row in rows for value in row))
        detail_row = next(row for row in rows if row[9] == "subcuenta")
        self.assertEqual(detail_row[2:4], ("Operativo", "Detalle operativo"))
        financial_total = next(row for row in rows if row[1] == "Gastos Financieros" and row[9] == "total_grupo")
        self.assertEqual(financial_total[7], "N/A")
        total_row = rows[-1]
        self.assertEqual(total_row[4:8], ("150.0000", "120.0000", "-30.0000", "80.00"))
        self.assertEqual(total_row[8], "No incluye impuestos.")

    def test_monthly_actuals_template_contains_required_columns_and_active_targets(self) -> None:
        with self.Session.begin() as db:
            rubric = db.get(AccountingRubric, self.rubrics["op_test"])
            active = create_accounting_subrubric(
                db,
                rubric=rubric,
                code="op_active_detail",
                name="Detalle activo",
                monthly_budget_amount="75",
                display_order="1",
            )
            create_accounting_subrubric(
                db,
                rubric=rubric,
                code="op_inactive_detail",
                name="Detalle inactivo",
                monthly_budget_amount="25",
                display_order="2",
                active=False,
            )
            active_id = active.id
            save_monthly_subrubric_actuals(
                db,
                period_month=date(2026, 11, 1),
                values_by_subrubric_id={active_id: ("12", "Precargado")},
                user_id=None,
            )

        with self.Session() as db:
            statement = build_income_statement(db, date(2026, 11, 1))
            rows = build_monthly_actuals_template_rows(statement)

        self.assertEqual(
            MONTHLY_ACTUALS_TEMPLATE_HEADERS,
            (
                "Periodo",
                "Grupo",
                "Cuenta",
                "Subcuenta",
                "Codigo Cuenta",
                "Codigo Subcuenta",
                "Presupuesto mensual",
                "Monto real",
                "Observacion",
            ),
        )
        active_row = next(row for row in rows if row[5] == "op_active_detail")
        self.assertEqual(active_row, (
            "2026-11",
            "Gastos Operativos",
            "Operativo",
            "Detalle activo",
            "op_test",
            "op_active_detail",
            "75.0000",
            "12.0000",
            "Precargado",
        ))
        self.assertFalse(any(row[5] == "op_inactive_detail" for row in rows))
        self.assertFalse(any(row[4] == "op_test" and not row[5] for row in rows))
        self.assertTrue(any(row[4] == "admin_test" and not row[5] for row in rows))

    def test_csv_import_accepts_comma_semicolon_encodings_and_accented_headers(self) -> None:
        accented_headers = tuple(
            {
                "Codigo Cuenta": "Código Cuenta",
                "Codigo Subcuenta": "Código Subcuenta",
                "Observacion": "Observación",
            }.get(header, header)
            for header in MONTHLY_ACTUALS_TEMPLATE_HEADERS
        )
        accented_row = {
            {
                "Codigo Cuenta": "Código Cuenta",
                "Codigo Subcuenta": "Código Subcuenta",
                "Observacion": "Observación",
            }.get(header, header): value
            for header, value in self._monthly_row(**{"Monto real": "44", "Observacion": "Edición"}).items()
        }
        cases = (
            (",", "utf-8-sig", MONTHLY_ACTUALS_TEMPLATE_HEADERS, self._monthly_row(**{"Monto real": "41"})),
            (";", "utf-8", MONTHLY_ACTUALS_TEMPLATE_HEADERS, self._monthly_row(**{"Monto real": "42"})),
            (";", "cp1252", accented_headers, accented_row),
            (";", "latin-1", accented_headers, accented_row),
        )
        for delimiter, encoding, headers, row in cases:
            with self.subTest(delimiter=delimiter, encoding=encoding), self.Session.begin() as db:
                result = import_monthly_actuals_csv(
                    db,
                    period_month=date(2026, 11, 1),
                    filename="gastos.csv",
                    content=self._monthly_csv(
                        [row],
                        headers=headers,
                        delimiter=delimiter,
                        encoding=encoding,
                    ),
                    user_id=None,
                )
                self.assertEqual(result.updated_rows, 1)

        with self.Session() as db:
            actual = db.query(AccountingMonthlyActual).filter_by(
                period_month=date(2026, 11, 1),
                rubric_id=self.rubrics["op_test"],
            ).one()
            self.assertEqual(actual.actual_amount, Decimal("44.0000"))
            self.assertEqual(actual.notes, "Edición")

    def test_csv_import_accepts_excel_currency_and_thousands_amount_formats(self) -> None:
        cases = (
            ("", Decimal("0.0000")),
            ("-", Decimal("0.0000")),
            ("500000", Decimal("500000.0000")),
            ("500,000", Decimal("500000.0000")),
            ("500.000", Decimal("500000.0000")),
            ("₡500,000", Decimal("500000.0000")),
            ("₡500.000", Decimal("500000.0000")),
        )
        for raw_amount, expected in cases:
            with self.subTest(raw_amount=raw_amount), self.Session.begin() as db:
                import_monthly_actuals_csv(
                    db,
                    period_month=date(2026, 11, 1),
                    filename="gastos.csv",
                    content=self._monthly_csv([
                        self._monthly_row(**{"Monto real": raw_amount, "Observacion": "Formato Excel"})
                    ]),
                    user_id=None,
                )
            with self.Session() as db:
                actual = db.query(AccountingMonthlyActual).filter_by(
                    period_month=date(2026, 11, 1),
                    rubric_id=self.rubrics["op_test"],
                ).one()
                self.assertEqual(actual.actual_amount, expected)

    def test_csv_import_replaces_direct_amount_note_and_blank_amount_becomes_zero(self) -> None:
        with self.Session.begin() as db:
            save_monthly_actuals(
                db,
                period_month=date(2026, 11, 1),
                values_by_rubric_id={self.rubrics["op_test"]: ("125", "Anterior")},
                user_id=None,
            )
            save_monthly_actuals(
                db,
                period_month=date(2026, 12, 1),
                values_by_rubric_id={self.rubrics["op_test"]: ("77", "Otro mes")},
                user_id=None,
            )
        with self.Session.begin() as db:
            result = import_monthly_actuals_csv(
                db,
                period_month=date(2026, 11, 1),
                filename="gastos.csv",
                content=self._monthly_csv([self._monthly_row(**{"Monto real": "150", "Observacion": "Reemplazado"})]),
                user_id=None,
            )
            self.assertEqual(result.updated_rows, 1)
        with self.Session() as db:
            actual = db.query(AccountingMonthlyActual).filter_by(
                period_month=date(2026, 11, 1),
                rubric_id=self.rubrics["op_test"],
            ).one()
            self.assertEqual(actual.actual_amount, Decimal("150.0000"))
            self.assertEqual(actual.notes, "Reemplazado")

        with self.Session.begin() as db:
            import_monthly_actuals_csv(
                db,
                period_month=date(2026, 11, 1),
                filename="gastos.csv",
                content=self._monthly_csv([self._monthly_row(**{"Monto real": "", "Observacion": ""})]),
                user_id=None,
            )
        with self.Session() as db:
            actual = db.query(AccountingMonthlyActual).filter_by(
                period_month=date(2026, 11, 1),
                rubric_id=self.rubrics["op_test"],
            ).one()
            other_month = db.query(AccountingMonthlyActual).filter_by(
                period_month=date(2026, 12, 1),
                rubric_id=self.rubrics["op_test"],
            ).one()
            self.assertEqual(actual.actual_amount, Decimal("0.0000"))
            self.assertIsNone(actual.notes)
            self.assertEqual(other_month.actual_amount, Decimal("77.0000"))
            self.assertEqual(other_month.notes, "Otro mes")

    def test_csv_import_replaces_subrubric_amount_and_note(self) -> None:
        with self.Session.begin() as db:
            rubric = db.get(AccountingRubric, self.rubrics["op_test"])
            subrubric = create_accounting_subrubric(
                db,
                rubric=rubric,
                code="op_pauta_meta",
                name="Pauta Meta",
                monthly_budget_amount="200",
                display_order="1",
            )
            subrubric_id = subrubric.id
            save_monthly_subrubric_actuals(
                db,
                period_month=date(2026, 11, 1),
                values_by_subrubric_id={subrubric_id: ("125", "Anterior")},
                user_id=None,
            )
        row = self._monthly_row(**{
            "Subcuenta": "Pauta Meta",
            "Codigo Subcuenta": "op_pauta_meta",
            "Monto real": "150",
            "Observacion": "Actualizado",
        })
        with self.Session.begin() as db:
            import_monthly_actuals_csv(
                db,
                period_month=date(2026, 11, 1),
                filename="gastos.csv",
                content=self._monthly_csv([row]),
                user_id=None,
            )
        with self.Session() as db:
            actual = db.query(AccountingSubrubricMonthlyActual).filter_by(
                period_month=date(2026, 11, 1),
                subrubric_id=subrubric_id,
            ).one()
            self.assertEqual(actual.actual_amount, Decimal("150.0000"))
            self.assertEqual(actual.notes, "Actualizado")

    def test_csv_import_rejects_period_missing_targets_duplicates_and_invalid_amounts(self) -> None:
        cases = (
            ([self._monthly_row(Periodo="2026-13")], "no es válido"),
            ([self._monthly_row(Periodo="2026-12")], "no coincide"),
            ([self._monthly_row(**{"Codigo Cuenta": "missing"})], "no existe"),
            ([self._monthly_row(**{"Codigo Subcuenta": "missing_sub"})], "subcuenta 'missing_sub' no existe"),
            ([self._monthly_row(), self._monthly_row()], "duplicada"),
            ([self._monthly_row(**{"Monto real": "abc"})], "número válido"),
            ([self._monthly_row(**{"Monto real": "-1"})], "negativo"),
        )
        for rows, expected in cases:
            with self.subTest(expected=expected), self.Session() as db:
                with self.assertRaisesRegex(AccountingValidationError, expected):
                    import_monthly_actuals_csv(
                        db,
                        period_month=date(2026, 11, 1),
                        filename="gastos.csv",
                        content=self._monthly_csv(rows),
                        user_id=None,
                    )

    def test_csv_import_rejects_wrong_parent_inactive_targets_file_and_headers(self) -> None:
        with self.Session.begin() as db:
            operating = db.get(AccountingRubric, self.rubrics["op_test"])
            administrative = db.get(AccountingRubric, self.rubrics["admin_test"])
            foreign_subrubric = create_accounting_subrubric(
                db,
                rubric=administrative,
                code="admin_foreign",
                name="Otra cuenta",
                monthly_budget_amount="1",
                display_order="1",
            )
            inactive_subrubric = create_accounting_subrubric(
                db,
                rubric=operating,
                code="op_inactive",
                name="Inactiva",
                monthly_budget_amount="1",
                display_order="1",
                active=False,
            )
            foreign_code = foreign_subrubric.code
            inactive_code = inactive_subrubric.code
            db.get(AccountingRubric, self.rubrics["financial_test"]).active = False

        cases = (
            ("gastos.txt", self._monthly_csv([self._monthly_row()]), "extensión .csv"),
            ("", self._monthly_csv([self._monthly_row()]), "extensión .csv"),
            (
                "gastos.csv",
                self._monthly_csv([{"Periodo": "2026-11"}], headers=("Periodo",)),
                "Faltan columnas",
            ),
            (
                "gastos.csv",
                self._monthly_csv([self._monthly_row(**{"Codigo Subcuenta": foreign_code})]),
                "no pertenece",
            ),
            (
                "gastos.csv",
                self._monthly_csv([self._monthly_row(**{"Codigo Subcuenta": inactive_code})]),
                "está inactiva",
            ),
            (
                "gastos.csv",
                self._monthly_csv([self._monthly_row(**{"Codigo Cuenta": "financial_test"})]),
                "está inactiva",
            ),
        )
        for filename, content, expected in cases:
            with self.subTest(expected=expected), self.Session() as db:
                with self.assertRaisesRegex(AccountingValidationError, expected):
                    import_monthly_actuals_csv(
                        db,
                        period_month=date(2026, 11, 1),
                        filename=filename,
                        content=content,
                        user_id=None,
                    )

        with self.Session() as db, self.assertRaises(AccountingValidationError) as caught:
            import_monthly_actuals_csv(
                db,
                period_month=date(2026, 11, 1),
                filename="gastos.csv",
                content=self._monthly_csv([{"Periodo": "2026-11"}], headers=("Periodo",)),
                user_id=None,
            )
        self.assertIn("Faltan columnas requeridas:", str(caught.exception))
        self.assertIn("Columnas detectadas: Periodo.", str(caught.exception))

    def test_csv_import_is_atomic_and_does_not_change_sales_or_cogs(self) -> None:
        with self.Session.begin() as db:
            save_monthly_actuals(
                db,
                period_month=date(2026, 11, 1),
                values_by_rubric_id={self.rubrics["admin_test"]: ("30", "Original")},
                user_id=None,
            )
            self._add_b2b(
                db,
                number="B2B-CSV-SAFETY",
                delivery_date=date(2026, 11, 10),
                invoice_date=date(2026, 11, 10),
                line_total=Decimal("200"),
                cogs=Decimal("75"),
            )
        with self.Session() as db:
            sales_before = get_accounting_sales_summary(db, date(2026, 11, 1))
            valid = self._monthly_row(**{
                "Grupo": "Gastos Administrativos",
                "Cuenta": "Administrativo",
                "Codigo Cuenta": "admin_test",
                "Monto real": "99",
            })
            invalid = self._monthly_row(**{"Codigo Cuenta": "missing"})
            with self.assertRaises(AccountingValidationError):
                import_monthly_actuals_csv(
                    db,
                    period_month=date(2026, 11, 1),
                    filename="gastos.csv",
                    content=self._monthly_csv([valid, invalid]),
                    user_id=None,
                )
            db.rollback()
        with self.Session() as db:
            actual = db.query(AccountingMonthlyActual).filter_by(
                period_month=date(2026, 11, 1),
                rubric_id=self.rubrics["admin_test"],
            ).one()
            sales_after = get_accounting_sales_summary(db, date(2026, 11, 1))
            self.assertEqual(actual.actual_amount, Decimal("30.0000"))
            self.assertEqual(actual.notes, "Original")
            self.assertEqual(sales_after, sales_before)

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
        self.assertNotIn('<span class="muted-inline">{{ line.rubric.code }}</span>', actuals_template)
        self.assertNotIn('<span class="muted-inline">{{ subline.subrubric.code }}</span>', actuals_template)
        self.assertIn('padding-left: 1.5rem;', actuals_template)
        self.assertIn('Resumen mensual por grupo', actuals_template)
        self.assertIn('statement.expense_totals', actuals_template)
        self.assertIn('/accounting/monthly-actuals/export.csv?period={{ period_value }}', actuals_template)
        self.assertIn('/accounting/monthly-actuals/template.csv?period={{ period_value }}', actuals_template)
        self.assertIn('action="/accounting/monthly-actuals/import"', actuals_template)
        self.assertIn('enctype="multipart/form-data"', actuals_template)
        self.assertIn('name="file" type="file"', actuals_template)

        main_source = (project_root / "app/main.py").read_text(encoding="utf-8")
        self.assertIn('@app.get("/accounting/monthly-actuals/export.csv")', main_source)
        self.assertIn('@app.get("/accounting/monthly-actuals/template.csv")', main_source)
        self.assertIn('@app.post("/accounting/monthly-actuals/import")', main_source)
        self.assertIn('db.rollback()', main_source)
        self.assertIn('"Tipo de fila"', main_source)
        self.assertIn('build_monthly_expense_export_rows(statement)', main_source)
        self.assertIn('content=buffer.getvalue().encode("utf-8-sig")', main_source)

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

        self.assertIn("Cuentas contables y presupuesto", rubrics_template)
        self.assertIn("Cuenta principal", rubrics_template)
        self.assertIn("Agregar subcuenta", rubrics_template)
        self.assertIn("Presupuesto efectivo", rubrics_template)
        self.assertIn("Buscar cuenta o subcuenta", actuals_template)
        self.assertIn("<th>Grupo</th><th>Cuenta</th><th>Subcuenta</th>", actuals_template)
        self.assertIn("Grupo: {{ section.label }}", statement_template)
        self.assertIn("Cuenta / Subcuenta", statement_template)


if __name__ == "__main__":
    unittest.main()
