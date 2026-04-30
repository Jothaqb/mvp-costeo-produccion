import csv
import io
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation

from sqlalchemy.orm import Session

from app.models import PurchaseOrder, PurchaseOrderLine


class PurchaseOrderHistoricalImportValidationError(Exception):
    pass


@dataclass(frozen=True)
class PurchaseOrderHistoricalImportMessage:
    row_number: int
    message: str


@dataclass(frozen=True)
class PurchaseOrderHistoricalImportResult:
    file_name: str
    total_rows: int
    created_purchase_orders: int
    skipped_existing_purchase_orders: int
    created_lines: int
    invalid_rows: int
    warnings: list[PurchaseOrderHistoricalImportMessage] = field(default_factory=list)
    errors: list[PurchaseOrderHistoricalImportMessage] = field(default_factory=list)


EXPECTED_HEADERS = (
    "po_number",
    "po_date",
    "supplier_name",
    "sku",
    "description",
    "quantity",
    "received_quantity",
    "unit_cost",
    "line_total",
    "notes",
)

_QUANT = Decimal("0.0001")
_ZERO = Decimal("0.0000")
_TOLERANCE = Decimal("0.01")


def import_historical_purchase_orders_csv(
    db: Session,
    *,
    file_name: str,
    file_bytes: bytes,
) -> PurchaseOrderHistoricalImportResult:
    if not file_bytes:
        raise PurchaseOrderHistoricalImportValidationError("Please choose a CSV file to import.")

    decoded_text = _decode_csv_bytes(file_bytes)
    rows = _read_csv_rows(decoded_text)
    existing_numbers = {value for (value,) in db.query(PurchaseOrder.po_number).all()}

    warnings: list[PurchaseOrderHistoricalImportMessage] = []
    errors: list[PurchaseOrderHistoricalImportMessage] = []
    grouped_rows: dict[str, list[tuple[int, dict[str, str]]]] = defaultdict(list)
    total_rows = 0
    invalid_rows = 0

    for row_number, row in rows:
        if _row_is_blank(row):
            continue
        total_rows += 1
        po_number = _field_value(row, "po_number")
        if not po_number:
            errors.append(PurchaseOrderHistoricalImportMessage(row_number=row_number, message="po_number is required."))
            invalid_rows += 1
            continue
        grouped_rows[po_number].append((row_number, row))

    created_purchase_orders = 0
    skipped_existing_purchase_orders = 0
    created_lines = 0

    for po_number, po_rows in grouped_rows.items():
        if po_number in existing_numbers:
            skipped_existing_purchase_orders += 1
            warnings.append(
                PurchaseOrderHistoricalImportMessage(
                    row_number=po_rows[0][0],
                    message=f"Purchase Order '{po_number}' already exists. PO skipped.",
                )
            )
            continue

        order_payload = _build_purchase_order_payload(
            po_number=po_number,
            po_rows=po_rows,
            warnings=warnings,
            errors=errors,
        )
        if order_payload is None:
            invalid_rows += len(po_rows)
            continue

        order = PurchaseOrder(
            po_number=order_payload["po_number"],
            supplier_name_snapshot=order_payload["supplier_name_snapshot"],
            po_date=order_payload["po_date"],
            status="closed",
            notes=order_payload["notes"],
            estimated_total=order_payload["estimated_total"],
        )
        db.add(order)
        db.flush()

        for line_number, line_data in enumerate(order_payload["lines"], start=1):
            db.add(
                PurchaseOrderLine(
                    purchase_order_id=order.id,
                    line_number=line_number,
                    sku_snapshot=line_data["sku_snapshot"],
                    description_snapshot=line_data["description_snapshot"],
                    supplier_name_snapshot=line_data["supplier_name_snapshot"],
                    quantity=line_data["quantity"],
                    received_quantity=line_data["received_quantity"],
                    unit_cost_snapshot=line_data["unit_cost_snapshot"],
                    line_total=line_data["line_total"],
                )
            )

        existing_numbers.add(po_number)
        created_purchase_orders += 1
        created_lines += len(order_payload["lines"])

    db.commit()
    return PurchaseOrderHistoricalImportResult(
        file_name=file_name,
        total_rows=total_rows,
        created_purchase_orders=created_purchase_orders,
        skipped_existing_purchase_orders=skipped_existing_purchase_orders,
        created_lines=created_lines,
        invalid_rows=invalid_rows,
        warnings=warnings,
        errors=errors,
    )


def _build_purchase_order_payload(
    *,
    po_number: str,
    po_rows: list[tuple[int, dict[str, str]]],
    warnings: list[PurchaseOrderHistoricalImportMessage],
    errors: list[PurchaseOrderHistoricalImportMessage],
) -> dict[str, object] | None:
    first_row_number, first_row = po_rows[0]
    po_date_text = _field_value(first_row, "po_date")
    supplier_name = _field_value(first_row, "supplier_name")
    notes = _field_value(first_row, "notes")

    if not supplier_name:
        errors.append(PurchaseOrderHistoricalImportMessage(first_row_number, f"PO '{po_number}' is missing supplier_name."))
        return None
    if not _rows_share_same_text(po_rows, "po_date"):
        errors.append(PurchaseOrderHistoricalImportMessage(first_row_number, f"PO '{po_number}' has inconsistent po_date values."))
        return None
    if not _rows_share_same_text(po_rows, "supplier_name"):
        errors.append(PurchaseOrderHistoricalImportMessage(first_row_number, f"PO '{po_number}' has inconsistent supplier_name values."))
        return None
    if not _rows_share_same_text(po_rows, "notes"):
        errors.append(PurchaseOrderHistoricalImportMessage(first_row_number, f"PO '{po_number}' has inconsistent notes values."))
        return None

    po_date = _parse_date(po_date_text, field_name="po_date", row_number=first_row_number, errors=errors)
    if po_date is None:
        return None

    lines: list[dict[str, Decimal | str]] = []
    estimated_total = _ZERO

    for row_number, row in po_rows:
        sku = _field_value(row, "sku")
        description = _field_value(row, "description")
        if not sku or not description:
            errors.append(
                PurchaseOrderHistoricalImportMessage(
                    row_number,
                    f"PO '{po_number}' requires sku and description in every row.",
                )
            )
            return None

        quantity = _parse_decimal(row.get("quantity"), row_number=row_number, field_name="quantity", errors=errors, allow_blank=False)
        received_quantity = _parse_decimal(
            row.get("received_quantity"),
            row_number=row_number,
            field_name="received_quantity",
            errors=errors,
            allow_blank=True,
        )
        unit_cost = _parse_decimal(row.get("unit_cost"), row_number=row_number, field_name="unit_cost", errors=errors, allow_blank=False)
        line_total = _parse_decimal(row.get("line_total"), row_number=row_number, field_name="line_total", errors=errors, allow_blank=True)
        if None in {quantity, unit_cost}:
            return None

        if quantity < _ZERO or unit_cost < _ZERO:
            errors.append(PurchaseOrderHistoricalImportMessage(row_number, f"PO '{po_number}' contains negative quantity or unit_cost values."))
            return None

        if received_quantity is None:
            received_quantity = quantity
        elif received_quantity < _ZERO:
            errors.append(PurchaseOrderHistoricalImportMessage(row_number, f"PO '{po_number}' contains negative received_quantity values."))
            return None
        elif received_quantity > quantity:
            errors.append(PurchaseOrderHistoricalImportMessage(row_number, f"PO '{po_number}' has received_quantity greater than quantity."))
            return None

        calculated_line_total = (quantity * unit_cost).quantize(_QUANT)
        if line_total is None:
            line_total = calculated_line_total
        elif line_total < _ZERO:
            errors.append(PurchaseOrderHistoricalImportMessage(row_number, f"PO '{po_number}' contains negative line_total values."))
            return None
        elif _material_difference(line_total, calculated_line_total):
            errors.append(
                PurchaseOrderHistoricalImportMessage(
                    row_number,
                    f"PO '{po_number}' line_total does not match quantity * unit_cost.",
                )
            )
            return None

        estimated_total = (estimated_total + line_total).quantize(_QUANT)
        lines.append(
            {
                "sku_snapshot": sku,
                "description_snapshot": description,
                "supplier_name_snapshot": supplier_name,
                "quantity": quantity,
                "received_quantity": received_quantity,
                "unit_cost_snapshot": unit_cost,
                "line_total": line_total,
            }
        )

    return {
        "po_number": po_number,
        "supplier_name_snapshot": supplier_name,
        "po_date": po_date,
        "notes": _build_historical_notes(notes),
        "estimated_total": estimated_total,
        "lines": lines,
    }


def _read_csv_rows(decoded_text: str) -> list[tuple[int, dict[str, str]]]:
    sample = decoded_text[:4096]
    delimiter = _detect_delimiter(sample)
    reader = csv.DictReader(io.StringIO(decoded_text), delimiter=delimiter)
    if reader.fieldnames is None:
        raise PurchaseOrderHistoricalImportValidationError("The CSV file does not contain a header row.")

    normalized_fieldnames = [_normalize_header(fieldname) for fieldname in reader.fieldnames]
    missing = [header for header in EXPECTED_HEADERS if header not in normalized_fieldnames]
    if missing:
        raise PurchaseOrderHistoricalImportValidationError(
            "The CSV file is missing required columns: " + ", ".join(missing)
        )

    rows: list[tuple[int, dict[str, str]]] = []
    for index, row in enumerate(reader, start=2):
        normalized_row = {_normalize_header(key): _clean_value(value) for key, value in row.items() if key is not None}
        rows.append((index, normalized_row))
    return rows


def _decode_csv_bytes(file_bytes: bytes) -> str:
    for encoding in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            return file_bytes.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise PurchaseOrderHistoricalImportValidationError(
        "CSV files exported from Excel are supported. Recommended format: CSV UTF-8. The importer also accepts common Windows/Latin encodings."
    )


def _detect_delimiter(sample: str) -> str:
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;")
        if dialect.delimiter in {",", ";"}:
            return dialect.delimiter
    except csv.Error:
        pass
    return ";" if sample.count(";") > sample.count(",") else ","


def _parse_date(value: str, *, field_name: str, row_number: int, errors: list[PurchaseOrderHistoricalImportMessage]):
    text = (value or "").strip()
    if not text:
        errors.append(PurchaseOrderHistoricalImportMessage(row_number, f"{field_name} is required."))
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        errors.append(PurchaseOrderHistoricalImportMessage(row_number, f"{field_name} must use YYYY-MM-DD format."))
        return None


def _parse_decimal(
    raw_value: object,
    *,
    row_number: int,
    field_name: str,
    errors: list[PurchaseOrderHistoricalImportMessage],
    allow_blank: bool,
) -> Decimal | None:
    text = str(raw_value or "").strip()
    if not text:
        return None if allow_blank else _append_decimal_error(row_number, field_name, errors)
    normalized = text.replace(" ", "").replace("₡", "").replace("$", "")
    if "," in normalized and "." in normalized:
        normalized = normalized.replace(",", "")
    elif "," in normalized:
        normalized = normalized.replace(",", ".")
    try:
        return Decimal(normalized).quantize(_QUANT)
    except InvalidOperation:
        return _append_decimal_error(row_number, field_name, errors)


def _append_decimal_error(row_number: int, field_name: str, errors: list[PurchaseOrderHistoricalImportMessage]) -> None:
    errors.append(PurchaseOrderHistoricalImportMessage(row_number, f"{field_name} must be a valid decimal value."))
    return None


def _material_difference(expected: Decimal, actual: Decimal) -> bool:
    return abs(expected - actual) > _TOLERANCE


def _build_historical_notes(csv_notes: str) -> str:
    base = "Historical CSV import"
    if csv_notes.strip():
        return f"{base}\n{csv_notes.strip()}"
    return base


def _rows_share_same_text(po_rows: list[tuple[int, dict[str, str]]], field_name: str) -> bool:
    values = {_field_value(row, field_name).strip() for _, row in po_rows}
    return len(values) <= 1


def _field_value(row: dict[str, str], key: str) -> str:
    return str(row.get(key, "") or "").strip()


def _row_is_blank(row: dict[str, str]) -> bool:
    return not any((value or "").strip() for value in row.values())


def _clean_value(value: object) -> str:
    return str(value or "").strip()


def _normalize_header(value: str | None) -> str:
    text = str(value or "").strip().lstrip("\ufeff").lower()
    text = unicodedata.normalize("NFKD", text)
    return "".join(character for character in text if not unicodedata.combining(character))
