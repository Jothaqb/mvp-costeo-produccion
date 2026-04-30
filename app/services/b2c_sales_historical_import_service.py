import csv
import io
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation

from sqlalchemy.orm import Session

from app.models import B2CSalesOrder, B2CSalesOrderLine, Channel


class B2CHistoricalSalesImportValidationError(Exception):
    pass


@dataclass(frozen=True)
class B2CHistoricalSalesImportMessage:
    row_number: int
    message: str


@dataclass(frozen=True)
class B2CHistoricalSalesImportResult:
    file_name: str
    total_rows: int
    created_orders: int
    skipped_existing_orders: int
    created_lines: int
    invalid_rows: int
    warnings: list[B2CHistoricalSalesImportMessage] = field(default_factory=list)
    errors: list[B2CHistoricalSalesImportMessage] = field(default_factory=list)


EXPECTED_HEADERS = (
    "order_number",
    "order_date",
    "customer_name",
    "customer_phone",
    "customer_email",
    "channel",
    "sku",
    "description",
    "quantity",
    "unit_price",
    "gross_line_total",
    "discount_amount",
    "net_line_total",
    "order_subtotal",
    "order_discount",
    "order_total",
    "cost_unit",
    "cost_total",
    "gross_profit",
    "gross_profit_percent",
    "observations",
)

_QUANT = Decimal("0.0001")
_ZERO = Decimal("0.0000")
_TOLERANCE = Decimal("0.01")


def import_b2c_historical_sales_csv(db: Session, *, file_name: str, file_bytes: bytes) -> B2CHistoricalSalesImportResult:
    if not file_bytes:
        raise B2CHistoricalSalesImportValidationError("Please choose a CSV file to import.")

    decoded_text = _decode_csv_bytes(file_bytes)
    rows = _read_csv_rows(decoded_text)

    channel_lookup = _build_channel_lookup(
        db.query(Channel).filter(Channel.applies_to_b2c.is_(True)).order_by(Channel.id).all()
    )
    existing_numbers = {
        value
        for (value,) in db.query(B2CSalesOrder.order_number).all()
    }

    warnings: list[B2CHistoricalSalesImportMessage] = []
    errors: list[B2CHistoricalSalesImportMessage] = []
    grouped_rows: dict[str, list[tuple[int, dict[str, str]]]] = defaultdict(list)
    total_rows = 0
    invalid_rows = 0

    for row_number, row in rows:
        if _row_is_blank(row):
            continue
        total_rows += 1
        order_number = _field_value(row, "order_number")
        if not order_number:
            errors.append(B2CHistoricalSalesImportMessage(row_number=row_number, message="order_number is required."))
            invalid_rows += 1
            continue
        grouped_rows[order_number].append((row_number, row))

    created_orders = 0
    skipped_existing_orders = 0
    created_lines = 0

    for order_number, order_rows in grouped_rows.items():
        if order_number in existing_numbers:
            skipped_existing_orders += 1
            warnings.append(
                B2CHistoricalSalesImportMessage(
                    row_number=order_rows[0][0],
                    message=f"Order '{order_number}' already exists. Order skipped.",
                )
            )
            continue

        order_payload = _build_order_payload(
            order_number=order_number,
            order_rows=order_rows,
            channel_lookup=channel_lookup,
            warnings=warnings,
            errors=errors,
        )
        if order_payload is None:
            invalid_rows += len(order_rows)
            continue

        order = B2CSalesOrder(
            order_number=order_payload["order_number"],
            order_date=order_payload["order_date"],
            b2c_customer_id=None,
            customer_name=order_payload["customer_name"],
            customer_phone=order_payload["customer_phone"],
            customer_email=order_payload["customer_email"],
            channel=order_payload["channel"],
            channel_id=order_payload["channel_id"],
            status="invoiced",
            subtotal_amount=order_payload["subtotal_amount"],
            discount_amount=order_payload["discount_amount"],
            total_amount=order_payload["total_amount"],
            cost_total_snapshot=order_payload["cost_total_snapshot"],
            gross_margin_amount=order_payload["gross_margin_amount"],
            gross_margin_percent=order_payload["gross_margin_percent"],
            observations=order_payload["observations"],
        )
        db.add(order)
        db.flush()

        for line_number, line_data in enumerate(order_payload["lines"], start=1):
            db.add(
                B2CSalesOrderLine(
                    sales_order_id=order.id,
                    line_number=line_number,
                    sku_snapshot=line_data["sku_snapshot"],
                    description_snapshot=line_data["description_snapshot"],
                    quantity=line_data["quantity"],
                    unit_price_snapshot=line_data["unit_price_snapshot"],
                    line_total=line_data["line_total"],
                    discount_amount_snapshot=line_data["discount_amount_snapshot"],
                    net_line_total_snapshot=line_data["net_line_total_snapshot"],
                    cost_unit_snapshot=line_data["cost_unit_snapshot"],
                    cost_total_snapshot=line_data["cost_total_snapshot"],
                    gross_margin_amount=line_data["gross_margin_amount"],
                    gross_margin_percent=line_data["gross_margin_percent"],
                )
            )
        existing_numbers.add(order_number)
        created_orders += 1
        created_lines += len(order_payload["lines"])

    db.commit()
    return B2CHistoricalSalesImportResult(
        file_name=file_name,
        total_rows=total_rows,
        created_orders=created_orders,
        skipped_existing_orders=skipped_existing_orders,
        created_lines=created_lines,
        invalid_rows=invalid_rows,
        warnings=warnings,
        errors=errors,
    )


def _build_order_payload(
    *,
    order_number: str,
    order_rows: list[tuple[int, dict[str, str]]],
    channel_lookup: dict[str, Channel | list[Channel]],
    warnings: list[B2CHistoricalSalesImportMessage],
    errors: list[B2CHistoricalSalesImportMessage],
) -> dict[str, object] | None:
    first_row_number, first_row = order_rows[0]
    order_date_text = _field_value(first_row, "order_date")
    customer_name = _field_value(first_row, "customer_name")
    customer_phone = _field_value(first_row, "customer_phone")
    customer_email = _field_value(first_row, "customer_email")
    channel_name = _field_value(first_row, "channel")
    observations = _field_value(first_row, "observations")

    if not channel_name:
        errors.append(B2CHistoricalSalesImportMessage(first_row_number, f"Order '{order_number}' is missing channel."))
        return None

    if not _rows_share_same_text(order_rows, "order_date"):
        errors.append(B2CHistoricalSalesImportMessage(first_row_number, f"Order '{order_number}' has inconsistent order_date values."))
        return None
    if not _rows_share_same_text(order_rows, "customer_name"):
        errors.append(B2CHistoricalSalesImportMessage(first_row_number, f"Order '{order_number}' has inconsistent customer_name values."))
        return None
    if not _rows_share_same_text(order_rows, "customer_phone"):
        errors.append(B2CHistoricalSalesImportMessage(first_row_number, f"Order '{order_number}' has inconsistent customer_phone values."))
        return None
    if not _rows_share_same_text(order_rows, "customer_email"):
        errors.append(B2CHistoricalSalesImportMessage(first_row_number, f"Order '{order_number}' has inconsistent customer_email values."))
        return None
    if not _rows_share_same_text(order_rows, "channel"):
        errors.append(B2CHistoricalSalesImportMessage(first_row_number, f"Order '{order_number}' has inconsistent channel values."))
        return None
    if not _rows_share_same_text(order_rows, "observations"):
        errors.append(B2CHistoricalSalesImportMessage(first_row_number, f"Order '{order_number}' has inconsistent observations values."))
        return None

    order_date = _parse_date(order_date_text, field_name="order_date", row_number=first_row_number, errors=errors)
    if order_date is None:
        return None

    lines: list[dict[str, Decimal | str | None]] = []
    subtotal_from_lines = _ZERO
    discount_from_lines = _ZERO
    total_from_lines = _ZERO
    order_has_complete_cogs = True
    order_cost_total = _ZERO

    for row_number, row in order_rows:
        sku = _field_value(row, "sku")
        description = _field_value(row, "description")
        if not sku or not description:
            errors.append(
                B2CHistoricalSalesImportMessage(
                    row_number,
                    f"Order '{order_number}' requires sku and description in every row.",
                )
            )
            return None

        quantity = _parse_decimal(row.get("quantity"), row_number=row_number, field_name="quantity", errors=errors, allow_blank=False)
        unit_price = _parse_decimal(row.get("unit_price"), row_number=row_number, field_name="unit_price", errors=errors, allow_blank=False)
        gross_line_total = _parse_decimal(
            row.get("gross_line_total"),
            row_number=row_number,
            field_name="gross_line_total",
            errors=errors,
            allow_blank=False,
        )
        discount_amount = _parse_decimal(
            row.get("discount_amount"),
            row_number=row_number,
            field_name="discount_amount",
            errors=errors,
            allow_blank=True,
        )
        net_line_total = _parse_decimal(
            row.get("net_line_total"),
            row_number=row_number,
            field_name="net_line_total",
            errors=errors,
            allow_blank=False,
        )
        cost_unit = _parse_decimal(row.get("cost_unit"), row_number=row_number, field_name="cost_unit", errors=errors, allow_blank=True)
        cost_total = _parse_decimal(row.get("cost_total"), row_number=row_number, field_name="cost_total", errors=errors, allow_blank=True)
        gross_profit = _parse_decimal(
            row.get("gross_profit"),
            row_number=row_number,
            field_name="gross_profit",
            errors=errors,
            allow_blank=True,
        )
        gross_profit_percent = _parse_decimal(
            row.get("gross_profit_percent"),
            row_number=row_number,
            field_name="gross_profit_percent",
            errors=errors,
            allow_blank=True,
        )
        if None in {quantity, unit_price, gross_line_total, net_line_total}:
            return None
        if quantity < _ZERO or unit_price < _ZERO or gross_line_total < _ZERO or net_line_total < _ZERO:
            errors.append(B2CHistoricalSalesImportMessage(row_number, f"Order '{order_number}' contains negative quantity or price values."))
            return None
        if discount_amount is not None and discount_amount < _ZERO:
            errors.append(B2CHistoricalSalesImportMessage(row_number, f"Order '{order_number}' contains negative discount_amount values."))
            return None
        if cost_unit is not None and cost_unit < _ZERO:
            errors.append(B2CHistoricalSalesImportMessage(row_number, f"Order '{order_number}' contains negative cost_unit values."))
            return None
        if cost_total is not None and cost_total < _ZERO:
            errors.append(B2CHistoricalSalesImportMessage(row_number, f"Order '{order_number}' contains negative cost_total values."))
            return None

        discount_amount = discount_amount or _ZERO
        if gross_profit is None and cost_total is not None:
            gross_profit = (net_line_total - cost_total).quantize(_QUANT)
        if gross_profit_percent is None and gross_profit is not None:
            gross_profit_percent = (gross_profit / net_line_total).quantize(_QUANT) if net_line_total > _ZERO else None

        if cost_total is None:
            order_has_complete_cogs = False
        else:
            order_cost_total = (order_cost_total + cost_total).quantize(_QUANT)

        subtotal_from_lines = (subtotal_from_lines + gross_line_total).quantize(_QUANT)
        discount_from_lines = (discount_from_lines + discount_amount).quantize(_QUANT)
        total_from_lines = (total_from_lines + net_line_total).quantize(_QUANT)
        lines.append(
            {
                "sku_snapshot": sku,
                "description_snapshot": description,
                "quantity": quantity,
                "unit_price_snapshot": unit_price,
                "line_total": gross_line_total,
                "discount_amount_snapshot": discount_amount,
                "net_line_total_snapshot": net_line_total,
                "cost_unit_snapshot": cost_unit,
                "cost_total_snapshot": cost_total,
                "gross_margin_amount": gross_profit,
                "gross_margin_percent": gross_profit_percent,
            }
        )

    order_subtotal = _parse_header_total(order_rows, "order_subtotal", subtotal_from_lines, errors, order_number)
    order_discount = _parse_header_total(order_rows, "order_discount", discount_from_lines, errors, order_number)
    order_total = _parse_header_total(order_rows, "order_total", total_from_lines, errors, order_number)
    if None in {order_subtotal, order_discount, order_total}:
        return None

    if _material_difference(order_subtotal, subtotal_from_lines):
        errors.append(
            B2CHistoricalSalesImportMessage(
                first_row_number,
                f"Order '{order_number}' subtotal does not match the sum of line gross totals.",
            )
        )
        return None
    if _material_difference(order_discount, discount_from_lines):
        errors.append(
            B2CHistoricalSalesImportMessage(
                first_row_number,
                f"Order '{order_number}' discount does not match the sum of line discounts.",
            )
        )
        return None
    if _material_difference(order_total, total_from_lines):
        errors.append(
            B2CHistoricalSalesImportMessage(
                first_row_number,
                f"Order '{order_number}' total does not match the sum of line net totals.",
            )
        )
        return None

    order_cost_snapshot = order_cost_total if order_has_complete_cogs else None
    order_gross_profit = (order_total - order_cost_total).quantize(_QUANT) if order_has_complete_cogs else None
    if order_has_complete_cogs:
        order_gross_profit_percent = (order_gross_profit / order_total).quantize(_QUANT) if order_total > _ZERO else None
    else:
        order_gross_profit_percent = None

    channel_id = None
    channel_match = channel_lookup.get(_normalize_key(channel_name))
    if isinstance(channel_match, Channel):
        channel_id = channel_match.id
    elif isinstance(channel_match, list):
        warnings.append(
            B2CHistoricalSalesImportMessage(
                first_row_number,
                f"Order '{order_number}' channel '{channel_name}' matched multiple channels. Snapshot text preserved without channel_id.",
            )
        )

    return {
        "order_number": order_number,
        "order_date": order_date,
        "customer_name": customer_name or None,
        "customer_phone": customer_phone or None,
        "customer_email": customer_email or None,
        "channel": channel_name,
        "channel_id": channel_id,
        "subtotal_amount": order_subtotal,
        "discount_amount": order_discount,
        "total_amount": order_total,
        "cost_total_snapshot": order_cost_snapshot,
        "gross_margin_amount": order_gross_profit,
        "gross_margin_percent": order_gross_profit_percent,
        "observations": _build_historical_observations(observations),
        "lines": lines,
    }


def _read_csv_rows(decoded_text: str) -> list[tuple[int, dict[str, str]]]:
    sample = decoded_text[:4096]
    delimiter = _detect_delimiter(sample)
    reader = csv.DictReader(io.StringIO(decoded_text), delimiter=delimiter)
    if reader.fieldnames is None:
        raise B2CHistoricalSalesImportValidationError("The CSV file does not contain a header row.")

    normalized_fieldnames = [_normalize_header(fieldname) for fieldname in reader.fieldnames]
    missing = [header for header in EXPECTED_HEADERS if header not in normalized_fieldnames]
    if missing:
        raise B2CHistoricalSalesImportValidationError(
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
    raise B2CHistoricalSalesImportValidationError(
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


def _build_channel_lookup(channels: list[Channel]) -> dict[str, Channel | list[Channel]]:
    lookup: dict[str, Channel | list[Channel]] = {}
    for channel in channels:
        key = _normalize_key(channel.name)
        existing = lookup.get(key)
        if existing is None:
            lookup[key] = channel
        elif isinstance(existing, list):
            existing.append(channel)
        else:
            lookup[key] = [existing, channel]
    return lookup


def _parse_date(value: str, *, field_name: str, row_number: int, errors: list[B2CHistoricalSalesImportMessage]):
    text = (value or "").strip()
    if not text:
        errors.append(B2CHistoricalSalesImportMessage(row_number, f"{field_name} is required."))
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        errors.append(B2CHistoricalSalesImportMessage(row_number, f"{field_name} must use YYYY-MM-DD format."))
        return None


def _parse_decimal(
    raw_value: object,
    *,
    row_number: int,
    field_name: str,
    errors: list[B2CHistoricalSalesImportMessage],
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


def _append_decimal_error(row_number: int, field_name: str, errors: list[B2CHistoricalSalesImportMessage]) -> None:
    errors.append(B2CHistoricalSalesImportMessage(row_number, f"{field_name} must be a valid decimal value."))
    return None


def _parse_header_total(
    order_rows: list[tuple[int, dict[str, str]]],
    field_name: str,
    fallback_value: Decimal,
    errors: list[B2CHistoricalSalesImportMessage],
    order_number: str,
) -> Decimal | None:
    provided_values = {_field_value(row, field_name) for _, row in order_rows if _field_value(row, field_name)}
    if len(provided_values) > 1:
        errors.append(
            B2CHistoricalSalesImportMessage(
                order_rows[0][0],
                f"Order '{order_number}' has inconsistent {field_name} values.",
            )
        )
        return None
    if not provided_values:
        return fallback_value
    value_text = provided_values.pop()
    return _parse_decimal(value_text, row_number=order_rows[0][0], field_name=field_name, errors=errors, allow_blank=False)


def _material_difference(expected: Decimal, actual: Decimal) -> bool:
    return abs(expected - actual) > _TOLERANCE


def _build_historical_observations(csv_observations: str) -> str:
    base = "Historical CSV import"
    if csv_observations.strip():
        return f"{base}\n{csv_observations.strip()}"
    return base


def _rows_share_same_text(order_rows: list[tuple[int, dict[str, str]]], field_name: str) -> bool:
    values = {_field_value(row, field_name).strip() for _, row in order_rows}
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


def _normalize_key(value: str) -> str:
    return " ".join((value or "").strip().split()).casefold()
