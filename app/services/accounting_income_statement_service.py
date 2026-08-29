from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
import io
import re

from sqlalchemy.orm import Session

from app.models import (
    AccountingMonthlyActual,
    AccountingRubric,
    AccountingSubrubric,
    AccountingSubrubricMonthlyActual,
    B2BSalesOrder,
    B2BSalesOrderLine,
    B2CSalesOrder,
    B2CSalesOrderLine,
)
from app.services.accounts_receivable_service import resolve_b2b_invoice_date


ZERO = Decimal("0")
MONEY_QUANT = Decimal("0.0001")
PERCENT_QUANT = Decimal("0.01")
ACCOUNTING_SECTIONS = ("operating", "administrative", "financial", "tax")
ACCOUNTING_RUBRIC_CODE_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{1,99}$")
MONTHLY_ACTUALS_TEMPLATE_HEADERS = (
    "Periodo",
    "Grupo",
    "Cuenta",
    "Subcuenta",
    "Codigo Cuenta",
    "Codigo Subcuenta",
    "Presupuesto mensual",
    "Monto real",
    "Observacion",
)
SECTION_LABELS = {
    "operating": "Gastos Operativos",
    "administrative": "Gastos Administrativos",
    "financial": "Gastos Financieros",
    "tax": "Impuestos",
}


class AccountingValidationError(ValueError):
    pass


@dataclass(frozen=True)
class AccountingSalesSummary:
    revenue: Decimal
    cogs: Decimal | None
    gross_profit: Decimal | None
    has_complete_cogs: bool
    cogs_lines_with_value: int
    cogs_total_lines: int
    b2b_orders: int
    b2c_orders: int

    @property
    def cogs_coverage_label(self) -> str:
        return f"{self.cogs_lines_with_value}/{self.cogs_total_lines} líneas"


@dataclass(frozen=True)
class AccountingSubrubricResult:
    subrubric: AccountingSubrubric
    actual_amount: Decimal
    budget_amount: Decimal
    difference_amount: Decimal
    difference_percent: Decimal | None
    compliance_percent: Decimal | None
    notes: str | None


@dataclass(frozen=True)
class AccountingRubricResult:
    rubric: AccountingRubric
    actual_amount: Decimal
    direct_actual_amount: Decimal
    budget_amount: Decimal
    difference_amount: Decimal
    difference_percent: Decimal | None
    compliance_percent: Decimal | None
    notes: str | None
    subrubrics: tuple[AccountingSubrubricResult, ...]
    uses_subrubrics: bool


@dataclass(frozen=True)
class AccountingSectionResult:
    code: str
    label: str
    lines: tuple[AccountingRubricResult, ...]
    actual_amount: Decimal
    budget_amount: Decimal
    difference_amount: Decimal
    difference_percent: Decimal | None
    compliance_percent: Decimal | None


@dataclass(frozen=True)
class AccountingExpenseTotals:
    actual_amount: Decimal
    budget_amount: Decimal
    difference_amount: Decimal
    compliance_percent: Decimal | None


@dataclass(frozen=True)
class AccountingMonthlyActualsImportResult:
    updated_rows: int


@dataclass(frozen=True)
class AccountingIncomeStatement:
    period_month: date
    sales: AccountingSalesSummary
    sections: tuple[AccountingSectionResult, ...]
    operating_expenses: Decimal
    administrative_expenses: Decimal
    financial_expenses: Decimal
    taxes_paid: Decimal
    expense_totals: AccountingExpenseTotals
    profit_before_tax: Decimal | None
    period_result: Decimal | None


def normalize_period_month(value: date) -> date:
    return value.replace(day=1)


def parse_period_month(value: str) -> date:
    normalized = (value or "").strip()
    try:
        parsed = date.fromisoformat(f"{normalized}-01")
    except ValueError as exc:
        raise AccountingValidationError("Month must use YYYY-MM format.") from exc
    return normalize_period_month(parsed)


def next_month(period_month: date) -> date:
    return date(
        period_month.year + (1 if period_month.month == 12 else 0),
        1 if period_month.month == 12 else period_month.month + 1,
        1,
    )


def parse_nonnegative_amount(value: object, field_name: str) -> Decimal:
    normalized = str(value or "").strip().replace(",", "")
    if not normalized:
        return ZERO.quantize(MONEY_QUANT)
    try:
        amount = Decimal(normalized).quantize(MONEY_QUANT)
    except (InvalidOperation, ValueError) as exc:
        raise AccountingValidationError(f"{field_name} must be a valid number.") from exc
    if not amount.is_finite() or amount < ZERO:
        raise AccountingValidationError(f"{field_name} must be zero or greater.")
    return amount


def _normalize_rubric_code(value: object) -> str:
    code = str(value or "").strip().lower()
    if not ACCOUNTING_RUBRIC_CODE_PATTERN.fullmatch(code):
        raise AccountingValidationError(
            "El código contable debe contener de 2 a 100 letras minúsculas, números, guiones bajos o guiones."
        )
    return code


def _normalize_rubric_name(value: object) -> str:
    name = str(value or "").strip()
    if not name:
        raise AccountingValidationError("El nombre de la cuenta o subcuenta es obligatorio.")
    if len(name) > 255:
        raise AccountingValidationError("El nombre de la cuenta o subcuenta no puede exceder 255 caracteres.")
    return name


def _normalize_rubric_section(value: object) -> str:
    section = str(value or "").strip().lower()
    if section not in ACCOUNTING_SECTIONS:
        raise AccountingValidationError("El grupo contable no es válido.")
    return section


def _parse_display_order(value: object) -> int:
    normalized = str(value or "").strip()
    try:
        display_order = int(normalized)
    except ValueError as exc:
        raise AccountingValidationError("Display order must be a whole number.") from exc
    if display_order < 0:
        raise AccountingValidationError("Display order must be zero or greater.")
    return display_order


def get_accounting_sales_summary(db: Session, period_month: date) -> AccountingSalesSummary:
    period_start = normalize_period_month(period_month)
    period_end = next_month(period_start)
    revenue = ZERO
    cogs_values: list[Decimal | None] = []
    b2b_order_ids: set[int] = set()
    b2c_order_ids: set[int] = set()

    b2b_rows = (
        db.query(B2BSalesOrder, B2BSalesOrderLine)
        .join(B2BSalesOrderLine, B2BSalesOrderLine.sales_order_id == B2BSalesOrder.id)
        .filter(B2BSalesOrder.status == "invoiced")
        .all()
    )
    for order, line in b2b_rows:
        invoice_date, _ = resolve_b2b_invoice_date(order)
        if not period_start <= invoice_date < period_end:
            continue
        revenue += line.line_total
        cogs_values.append(line.cost_total_snapshot)
        b2b_order_ids.add(order.id)

    b2c_rows = (
        db.query(B2CSalesOrder, B2CSalesOrderLine)
        .join(B2CSalesOrderLine, B2CSalesOrderLine.sales_order_id == B2CSalesOrder.id)
        .filter(
            B2CSalesOrder.status == "invoiced",
            B2CSalesOrder.order_date >= period_start,
            B2CSalesOrder.order_date < period_end,
        )
        .all()
    )
    for order, line in b2c_rows:
        net_sales = line.net_line_total_snapshot if line.net_line_total_snapshot is not None else line.line_total
        revenue += net_sales
        cogs_values.append(line.cost_total_snapshot)
        b2c_order_ids.add(order.id)

    revenue = revenue.quantize(MONEY_QUANT)
    cogs_lines_with_value = sum(1 for value in cogs_values if value is not None)
    has_complete_cogs = cogs_lines_with_value == len(cogs_values)
    cogs = None
    gross_profit = None
    if has_complete_cogs:
        cogs = sum((value for value in cogs_values if value is not None), ZERO).quantize(MONEY_QUANT)
        gross_profit = (revenue - cogs).quantize(MONEY_QUANT)

    return AccountingSalesSummary(
        revenue=revenue,
        cogs=cogs,
        gross_profit=gross_profit,
        has_complete_cogs=has_complete_cogs,
        cogs_lines_with_value=cogs_lines_with_value,
        cogs_total_lines=len(cogs_values),
        b2b_orders=len(b2b_order_ids),
        b2c_orders=len(b2c_order_ids),
    )


def _comparison(real: Decimal, budget: Decimal) -> tuple[Decimal, Decimal | None, Decimal | None]:
    difference = (real - budget).quantize(MONEY_QUANT)
    if budget == ZERO:
        return difference, None, None
    difference_percent = ((difference / budget) * Decimal("100")).quantize(PERCENT_QUANT)
    compliance_percent = ((real / budget) * Decimal("100")).quantize(PERCENT_QUANT)
    return difference, difference_percent, compliance_percent


def build_income_statement(db: Session, period_month: date) -> AccountingIncomeStatement:
    period_start = normalize_period_month(period_month)
    sales = get_accounting_sales_summary(db, period_start)
    actuals = {
        actual.rubric_id: actual
        for actual in db.query(AccountingMonthlyActual)
        .filter(AccountingMonthlyActual.period_month == period_start)
        .all()
    }
    subrubric_actuals = {
        actual.subrubric_id: actual
        for actual in db.query(AccountingSubrubricMonthlyActual)
        .filter(AccountingSubrubricMonthlyActual.period_month == period_start)
        .all()
    }
    subrubrics_by_rubric: dict[int, list[AccountingSubrubric]] = {}
    for subrubric in (
        db.query(AccountingSubrubric)
        .order_by(AccountingSubrubric.display_order, AccountingSubrubric.id)
        .all()
    ):
        subrubrics_by_rubric.setdefault(subrubric.rubric_id, []).append(subrubric)
    rubrics = (
        db.query(AccountingRubric)
        .order_by(AccountingRubric.display_order, AccountingRubric.id)
        .all()
    )

    section_results: list[AccountingSectionResult] = []
    section_actuals: dict[str, Decimal] = {}
    for section in ACCOUNTING_SECTIONS:
        lines: list[AccountingRubricResult] = []
        for rubric in rubrics:
            actual_record = actuals.get(rubric.id)
            rubric_subrubrics = subrubrics_by_rubric.get(rubric.id, [])
            relevant_subrubrics = [
                subrubric
                for subrubric in rubric_subrubrics
                if subrubric.active or subrubric.id in subrubric_actuals
            ]
            if rubric.section != section or (
                not rubric.active
                and actual_record is None
                and not any(subrubric.id in subrubric_actuals for subrubric in rubric_subrubrics)
            ):
                continue
            direct_real = (actual_record.actual_amount if actual_record is not None else ZERO).quantize(MONEY_QUANT)
            subrubric_lines: list[AccountingSubrubricResult] = []
            for subrubric in relevant_subrubrics:
                sub_actual_record = subrubric_actuals.get(subrubric.id)
                sub_real = (
                    sub_actual_record.actual_amount if sub_actual_record is not None else ZERO
                ).quantize(MONEY_QUANT)
                sub_budget = (
                    subrubric.monthly_budget_amount if subrubric.active else ZERO
                ).quantize(MONEY_QUANT)
                sub_difference, sub_difference_percent, sub_compliance_percent = _comparison(
                    sub_real,
                    sub_budget,
                )
                subrubric_lines.append(
                    AccountingSubrubricResult(
                        subrubric=subrubric,
                        actual_amount=sub_real,
                        budget_amount=sub_budget,
                        difference_amount=sub_difference,
                        difference_percent=sub_difference_percent,
                        compliance_percent=sub_compliance_percent,
                        notes=sub_actual_record.notes if sub_actual_record is not None else None,
                    )
                )
            active_subrubrics = [subrubric for subrubric in rubric_subrubrics if subrubric.active]
            subrubric_real = sum(
                (line.actual_amount for line in subrubric_lines),
                ZERO,
            ).quantize(MONEY_QUANT)
            real = (direct_real + subrubric_real).quantize(MONEY_QUANT)
            budget = (
                sum((subrubric.monthly_budget_amount for subrubric in active_subrubrics), ZERO)
                if active_subrubrics
                else rubric.monthly_budget_amount
            ).quantize(MONEY_QUANT)
            difference, difference_percent, compliance_percent = _comparison(real, budget)
            lines.append(
                AccountingRubricResult(
                    rubric=rubric,
                    actual_amount=real,
                    direct_actual_amount=direct_real,
                    budget_amount=budget,
                    difference_amount=difference,
                    difference_percent=difference_percent,
                    compliance_percent=compliance_percent,
                    notes=actual_record.notes if actual_record is not None else None,
                    subrubrics=tuple(subrubric_lines),
                    uses_subrubrics=bool(active_subrubrics),
                )
            )
        actual_total = sum((line.actual_amount for line in lines), ZERO).quantize(MONEY_QUANT)
        budget_total = sum((line.budget_amount for line in lines), ZERO).quantize(MONEY_QUANT)
        difference, difference_percent, compliance_percent = _comparison(actual_total, budget_total)
        section_actuals[section] = actual_total
        section_results.append(
            AccountingSectionResult(
                code=section,
                label=SECTION_LABELS[section],
                lines=tuple(lines),
                actual_amount=actual_total,
                budget_amount=budget_total,
                difference_amount=difference,
                difference_percent=difference_percent,
                compliance_percent=compliance_percent,
            )
        )

    profit_before_tax = None
    period_result = None
    if sales.gross_profit is not None:
        profit_before_tax = (
            sales.gross_profit
            - section_actuals["operating"]
            - section_actuals["administrative"]
            - section_actuals["financial"]
        ).quantize(MONEY_QUANT)
        period_result = (profit_before_tax - section_actuals["tax"]).quantize(MONEY_QUANT)

    expense_sections = [section for section in section_results if section.code != "tax"]
    expense_actual = sum((section.actual_amount for section in expense_sections), ZERO).quantize(MONEY_QUANT)
    expense_budget = sum((section.budget_amount for section in expense_sections), ZERO).quantize(MONEY_QUANT)
    expense_difference, _, expense_compliance = _comparison(expense_actual, expense_budget)

    return AccountingIncomeStatement(
        period_month=period_start,
        sales=sales,
        sections=tuple(section_results),
        operating_expenses=section_actuals["operating"],
        administrative_expenses=section_actuals["administrative"],
        financial_expenses=section_actuals["financial"],
        taxes_paid=section_actuals["tax"],
        expense_totals=AccountingExpenseTotals(
            actual_amount=expense_actual,
            budget_amount=expense_budget,
            difference_amount=expense_difference,
            compliance_percent=expense_compliance,
        ),
        profit_before_tax=profit_before_tax,
        period_result=period_result,
    )


def build_monthly_expense_export_rows(
    statement: AccountingIncomeStatement,
) -> list[tuple[object, ...]]:
    period_value = statement.period_month.strftime("%Y-%m")
    rows: list[tuple[object, ...]] = []

    def number(value: Decimal) -> str:
        return format(value, "f")

    def percent(value: Decimal | None) -> str:
        return "N/A" if value is None else format(value, "f")

    for section in statement.sections:
        for line in section.lines:
            rows.append(
                (
                    period_value,
                    section.label,
                    line.rubric.name,
                    "",
                    number(line.budget_amount),
                    number(line.actual_amount),
                    number(line.difference_amount),
                    percent(line.compliance_percent),
                    line.notes or "",
                    "cuenta",
                )
            )
            for subline in line.subrubrics:
                rows.append(
                    (
                        period_value,
                        section.label,
                        line.rubric.name,
                        subline.subrubric.name,
                        number(subline.budget_amount),
                        number(subline.actual_amount),
                        number(subline.difference_amount),
                        percent(subline.compliance_percent),
                        subline.notes or "",
                        "subcuenta",
                    )
                )
        rows.append(
            (
                period_value,
                section.label,
                "",
                "",
                number(section.budget_amount),
                number(section.actual_amount),
                number(section.difference_amount),
                percent(section.compliance_percent),
                "",
                "total_grupo",
            )
        )

    totals = statement.expense_totals
    rows.append(
        (
            period_value,
            "Gastos Totales",
            "",
            "",
            number(totals.budget_amount),
            number(totals.actual_amount),
            number(totals.difference_amount),
            percent(totals.compliance_percent),
            "No incluye impuestos.",
            "gastos_totales",
        )
    )
    return rows


def build_monthly_actuals_template_rows(
    statement: AccountingIncomeStatement,
) -> list[tuple[object, ...]]:
    period_value = statement.period_month.strftime("%Y-%m")
    rows: list[tuple[object, ...]] = []
    for section in statement.sections:
        for line in section.lines:
            if not line.rubric.active:
                continue
            active_subrubrics = [subline for subline in line.subrubrics if subline.subrubric.active]
            if active_subrubrics:
                for subline in active_subrubrics:
                    rows.append(
                        (
                            period_value,
                            section.label,
                            line.rubric.name,
                            subline.subrubric.name,
                            line.rubric.code,
                            subline.subrubric.code,
                            format(subline.budget_amount, "f"),
                            format(subline.actual_amount, "f"),
                            subline.notes or "",
                        )
                    )
            else:
                rows.append(
                    (
                        period_value,
                        section.label,
                        line.rubric.name,
                        "",
                        line.rubric.code,
                        "",
                        format(line.budget_amount, "f"),
                        format(line.direct_actual_amount, "f"),
                        line.notes or "",
                    )
                )
    return rows


def import_monthly_actuals_csv(
    db: Session,
    *,
    period_month: date,
    filename: str,
    content: bytes,
    user_id: int | None,
) -> AccountingMonthlyActualsImportResult:
    if not filename or not filename.lower().endswith(".csv"):
        raise AccountingValidationError("Debe seleccionar un archivo con extensión .csv.")
    try:
        decoded = content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise AccountingValidationError("El archivo debe estar codificado en UTF-8.") from exc

    reader = csv.DictReader(io.StringIO(decoded, newline=""))
    fieldnames = tuple(reader.fieldnames or ())
    missing_headers = [header for header in MONTHLY_ACTUALS_TEMPLATE_HEADERS if header not in fieldnames]
    if missing_headers:
        raise AccountingValidationError(
            "Faltan columnas requeridas: " + ", ".join(missing_headers) + "."
        )

    period_start = normalize_period_month(period_month)
    rubrics = db.query(AccountingRubric).all()
    subrubrics = db.query(AccountingSubrubric).all()
    rubrics_by_code = {rubric.code.lower(): rubric for rubric in rubrics}
    subrubrics_by_code = {subrubric.code.lower(): subrubric for subrubric in subrubrics}
    active_subrubrics_by_rubric: dict[int, list[AccountingSubrubric]] = {}
    for subrubric in subrubrics:
        if subrubric.active:
            active_subrubrics_by_rubric.setdefault(subrubric.rubric_id, []).append(subrubric)

    direct_values: dict[int, tuple[Decimal, str]] = {}
    subrubric_values: dict[int, tuple[Decimal, str]] = {}
    seen_targets: set[tuple[int, int | None]] = set()
    errors: list[str] = []
    valid_rows = 0

    for row_number, row in enumerate(reader, start=2):
        if not any(str(value or "").strip() for key, value in row.items() if key is not None):
            continue
        row_errors: list[str] = []
        raw_period = str(row.get("Periodo") or "").strip()
        try:
            row_period = parse_period_month(raw_period)
            if row_period != period_start:
                row_errors.append(
                    f"el periodo {raw_period or '(vacío)'} no coincide con {period_start:%Y-%m}"
                )
        except AccountingValidationError:
            row_errors.append(f"el periodo '{raw_period}' no es válido")

        rubric_code = str(row.get("Codigo Cuenta") or "").strip().lower()
        subrubric_code = str(row.get("Codigo Subcuenta") or "").strip().lower()
        rubric = rubrics_by_code.get(rubric_code)
        subrubric = subrubrics_by_code.get(subrubric_code) if subrubric_code else None
        if rubric is None:
            row_errors.append(f"la cuenta '{rubric_code or '(vacía)'}' no existe")
        elif not rubric.active:
            row_errors.append(f"la cuenta '{rubric.code}' está inactiva")

        if subrubric_code:
            if subrubric is None:
                row_errors.append(f"la subcuenta '{subrubric_code}' no existe")
            elif rubric is not None and subrubric.rubric_id != rubric.id:
                row_errors.append(
                    f"la subcuenta '{subrubric.code}' no pertenece a la cuenta '{rubric.code}'"
                )
            elif not subrubric.active:
                row_errors.append(f"la subcuenta '{subrubric.code}' está inactiva")
        elif rubric is not None and active_subrubrics_by_rubric.get(rubric.id):
            row_errors.append(
                f"la cuenta '{rubric.code}' tiene subcuentas activas; debe indicar Codigo Subcuenta"
            )

        amount: Decimal | None = None
        try:
            amount = parse_nonnegative_amount(row.get("Monto real"), "Monto real")
        except AccountingValidationError as exc:
            row_errors.append(str(exc))

        target: tuple[int, int | None] | None = None
        if rubric is not None and (not subrubric_code or subrubric is not None):
            target = (rubric.id, subrubric.id if subrubric is not None else None)
            if target in seen_targets:
                row_errors.append("la cuenta/subcuenta está duplicada dentro del CSV")
            else:
                seen_targets.add(target)

        if row_errors:
            errors.extend(f"Fila {row_number}: {message}." for message in row_errors)
            continue

        notes = str(row.get("Observacion") or "").strip()
        if subrubric is not None:
            subrubric_values[subrubric.id] = (amount or ZERO, notes)
        else:
            direct_values[rubric.id] = (amount or ZERO, notes)
        valid_rows += 1

    if errors:
        raise AccountingValidationError("Importación rechazada:\n" + "\n".join(errors))

    save_monthly_actuals(
        db,
        period_month=period_start,
        values_by_rubric_id=direct_values,
        user_id=user_id,
    )
    save_monthly_subrubric_actuals(
        db,
        period_month=period_start,
        values_by_subrubric_id=subrubric_values,
        user_id=user_id,
    )
    return AccountingMonthlyActualsImportResult(updated_rows=valid_rows)


def save_monthly_actuals(
    db: Session,
    *,
    period_month: date,
    values_by_rubric_id: dict[int, tuple[object, str]],
    user_id: int | None,
) -> None:
    period_start = normalize_period_month(period_month)
    rubric_ids = set(values_by_rubric_id)
    rubrics = db.query(AccountingRubric).filter(AccountingRubric.id.in_(rubric_ids)).all() if rubric_ids else []
    if {rubric.id for rubric in rubrics} != rubric_ids:
        raise AccountingValidationError("Una o más cuentas contables no existen.")
    existing = {
        item.rubric_id: item
        for item in db.query(AccountingMonthlyActual)
        .filter(
            AccountingMonthlyActual.period_month == period_start,
            AccountingMonthlyActual.rubric_id.in_(rubric_ids),
        )
        .all()
    } if rubric_ids else {}
    for rubric in rubrics:
        raw_amount, raw_notes = values_by_rubric_id[rubric.id]
        amount = parse_nonnegative_amount(raw_amount, rubric.name)
        notes = (raw_notes or "").strip() or None
        actual = existing.get(rubric.id)
        if actual is None:
            actual = AccountingMonthlyActual(
                period_month=period_start,
                rubric_id=rubric.id,
                actual_amount=amount,
                notes=notes,
                created_by_user_id=user_id,
                updated_by_user_id=user_id,
            )
            db.add(actual)
        else:
            actual.actual_amount = amount
            actual.notes = notes
            actual.updated_by_user_id = user_id
    db.flush()


def save_monthly_subrubric_actuals(
    db: Session,
    *,
    period_month: date,
    values_by_subrubric_id: dict[int, tuple[object, str]],
    user_id: int | None,
) -> None:
    period_start = normalize_period_month(period_month)
    subrubric_ids = set(values_by_subrubric_id)
    subrubrics = (
        db.query(AccountingSubrubric).filter(AccountingSubrubric.id.in_(subrubric_ids)).all()
        if subrubric_ids
        else []
    )
    if {subrubric.id for subrubric in subrubrics} != subrubric_ids:
        raise AccountingValidationError("Una o más subcuentas contables no existen.")
    existing = {
        item.subrubric_id: item
        for item in db.query(AccountingSubrubricMonthlyActual)
        .filter(
            AccountingSubrubricMonthlyActual.period_month == period_start,
            AccountingSubrubricMonthlyActual.subrubric_id.in_(subrubric_ids),
        )
        .all()
    } if subrubric_ids else {}
    for subrubric in subrubrics:
        raw_amount, raw_notes = values_by_subrubric_id[subrubric.id]
        amount = parse_nonnegative_amount(raw_amount, subrubric.name)
        notes = (raw_notes or "").strip() or None
        actual = existing.get(subrubric.id)
        if actual is None:
            actual = AccountingSubrubricMonthlyActual(
                period_month=period_start,
                subrubric_id=subrubric.id,
                actual_amount=amount,
                notes=notes,
                created_by_user_id=user_id,
                updated_by_user_id=user_id,
            )
            db.add(actual)
        else:
            actual.actual_amount = amount
            actual.notes = notes
            actual.updated_by_user_id = user_id
    db.flush()


def create_accounting_rubric(
    db: Session,
    *,
    code: object,
    name: object,
    section: object,
    monthly_budget_amount: object,
    display_order: object,
    active: bool = True,
) -> AccountingRubric:
    normalized_code = _normalize_rubric_code(code)
    if (
        db.query(AccountingRubric).filter(AccountingRubric.code == normalized_code).first() is not None
        or db.query(AccountingSubrubric).filter(AccountingSubrubric.code == normalized_code).first() is not None
    ):
        raise AccountingValidationError(f"El código contable '{normalized_code}' ya existe.")
    rubric = AccountingRubric(
        code=normalized_code,
        name=_normalize_rubric_name(name),
        section=_normalize_rubric_section(section),
        monthly_budget_amount=parse_nonnegative_amount(monthly_budget_amount, "Presupuesto mensual"),
        display_order=_parse_display_order(display_order),
        active=bool(active),
    )
    db.add(rubric)
    db.flush()
    return rubric


def create_accounting_subrubric(
    db: Session,
    *,
    rubric: AccountingRubric,
    code: object,
    name: object,
    monthly_budget_amount: object,
    display_order: object,
    active: bool = True,
) -> AccountingSubrubric:
    normalized_code = _normalize_rubric_code(code)
    if (
        db.query(AccountingRubric).filter(AccountingRubric.code == normalized_code).first() is not None
        or db.query(AccountingSubrubric).filter(AccountingSubrubric.code == normalized_code).first() is not None
    ):
        raise AccountingValidationError(f"El código contable '{normalized_code}' ya existe.")
    subrubric = AccountingSubrubric(
        rubric_id=rubric.id,
        code=normalized_code,
        name=_normalize_rubric_name(name),
        monthly_budget_amount=parse_nonnegative_amount(monthly_budget_amount, "Presupuesto mensual"),
        display_order=_parse_display_order(display_order),
        active=bool(active),
    )
    db.add(subrubric)
    db.flush()
    return subrubric


def update_accounting_subrubric(
    db: Session,
    subrubric: AccountingSubrubric,
    *,
    name: object,
    monthly_budget_amount: object,
    display_order: object,
    active: bool,
) -> AccountingSubrubric:
    subrubric.name = _normalize_rubric_name(name)
    subrubric.monthly_budget_amount = parse_nonnegative_amount(
        monthly_budget_amount,
        f"Presupuesto de {subrubric.name}",
    )
    subrubric.display_order = _parse_display_order(display_order)
    subrubric.active = bool(active)
    db.flush()
    return subrubric


def update_accounting_rubric(
    db: Session,
    rubric: AccountingRubric,
    *,
    name: object,
    section: object,
    monthly_budget_amount: object,
    display_order: object,
    active: bool,
) -> AccountingRubric:
    rubric.name = _normalize_rubric_name(name)
    rubric.section = _normalize_rubric_section(section)
    rubric.monthly_budget_amount = parse_nonnegative_amount(
        monthly_budget_amount,
        f"Presupuesto de {rubric.name}",
    )
    rubric.display_order = _parse_display_order(display_order)
    rubric.active = bool(active)
    db.flush()
    return rubric
