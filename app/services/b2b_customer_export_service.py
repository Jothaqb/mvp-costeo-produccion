from __future__ import annotations

from datetime import date, datetime
from io import BytesIO
from typing import Any

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter
from sqlalchemy.orm import Session, joinedload, selectinload

from app.models import B2BCustomer


B2B_CUSTOMER_EXPORT_HEADERS = (
    "Nombre del cliente",
    "Dirección",
    "Provincia",
    "Cantón",
    "Distrito",
    "Razón social",
    "Cédula jurídica",
    "Teléfono",
    "Días de crédito",
    "Forma de pago",
    "Canal B2B",
    "Estado del cliente",
    "Estado del último acuerdo",
    "¿Tenemos acuerdo firmado?",
    "Versión del último acuerdo",
    "Fecha de generación del último acuerdo",
    "Fecha de firma registrada",
)

NO_REGISTERED_SIGNATURE = "Sin firma registrada"
_TEXT_COLUMN_INDEXES = (7, 8)
_DATE_COLUMN_INDEXES = (16, 17)
_COLUMN_WIDTHS = (
    30,
    46,
    18,
    20,
    20,
    34,
    22,
    20,
    16,
    38,
    30,
    20,
    28,
    28,
    25,
    34,
    26,
)


def build_b2b_customers_export_xlsx(db: Session) -> bytes:
    customers = (
        db.query(B2BCustomer)
        .options(
            joinedload(B2BCustomer.channel_record),
            selectinload(B2BCustomer.commercial_agreements),
        )
        .order_by(B2BCustomer.customer_name, B2BCustomer.id)
        .all()
    )

    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "Clientes B2B"
    worksheet.append(B2B_CUSTOMER_EXPORT_HEADERS)

    for customer in customers:
        worksheet.append(_customer_export_row(customer))

    _format_worksheet(worksheet)
    buffer = BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()


def _customer_export_row(customer: B2BCustomer) -> tuple[Any, ...]:
    agreements = list(customer.commercial_agreements)
    latest_agreement = max(
        agreements,
        key=lambda agreement: (agreement.version_number, agreement.id or 0),
        default=None,
    )
    registered_signed_agreements = [
        agreement
        for agreement in agreements
        if agreement.status in {"signed", "archived"} and agreement.signed_at is not None
    ]
    latest_registered_signature = max(
        registered_signed_agreements,
        key=lambda agreement: (
            agreement.signed_at,
            agreement.version_number,
            agreement.id or 0,
        ),
        default=None,
    )

    return (
        customer.customer_name,
        customer.address,
        customer.province,
        customer.canton,
        customer.district,
        customer.legal_name,
        _text_or_none(customer.legal_id),
        _text_or_none(customer.phone),
        customer.credit_days,
        customer.default_payment_terms,
        customer.channel_record.name if customer.channel_record is not None else None,
        "Activo" if customer.active else "Inactivo",
        latest_agreement.status if latest_agreement is not None else None,
        "Sí" if latest_registered_signature is not None else NO_REGISTERED_SIGNATURE,
        latest_agreement.version_number if latest_agreement is not None else None,
        _date_value(latest_agreement.generated_at) if latest_agreement is not None else None,
        _date_value(latest_registered_signature.signed_at)
        if latest_registered_signature is not None
        else None,
    )


def _text_or_none(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _date_value(value: date | datetime | None) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    return value


def _format_worksheet(worksheet) -> None:
    header_fill = PatternFill(fill_type="solid", fgColor="1F4E3D")
    header_font = Font(color="FFFFFF", bold=True)

    worksheet.freeze_panes = "A2"
    worksheet.auto_filter.ref = f"A1:{get_column_letter(len(B2B_CUSTOMER_EXPORT_HEADERS))}{worksheet.max_row}"
    worksheet.row_dimensions[1].height = 34
    worksheet.sheet_view.showGridLines = False

    for cell in worksheet[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    for column_index, width in enumerate(_COLUMN_WIDTHS, start=1):
        worksheet.column_dimensions[get_column_letter(column_index)].width = width

    for row_index in range(2, worksheet.max_row + 1):
        worksheet.row_dimensions[row_index].height = 28
        for column_index in range(1, len(B2B_CUSTOMER_EXPORT_HEADERS) + 1):
            cell = worksheet.cell(row=row_index, column=column_index)
            cell.alignment = Alignment(
                vertical="top",
                wrap_text=True,
            )
            if isinstance(cell.value, str):
                # Prevent user-controlled values such as "=1+1" from becoming
                # executable formulas when the workbook is opened in Excel.
                cell.data_type = "s"
        for column_index in _TEXT_COLUMN_INDEXES:
            worksheet.cell(row=row_index, column=column_index).number_format = "@"
        for column_index in _DATE_COLUMN_INDEXES:
            worksheet.cell(row=row_index, column=column_index).number_format = "yyyy-mm-dd"
