import csv
import io
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation

from sqlalchemy.orm import Session

from app.models import B2BCustomer, B2BSalesOrder, B2BSalesOrderLine, Channel


class B2BHistoricalSalesImportValidationError(Exception):
    pass


@dataclass(frozen=True)
class B2BHistoricalSalesImportMessage:
    row_number: int
    message: str


@dataclass(frozen=True)
class B2BHistoricalSalesImportResult:
    file_name: str
    total_rows: int
    created_orders: int
    skipped_existing_orders: int
    created_lines: int
    invalid_rows: int
    warnings: list[B2BHistoricalSalesImportMessage] = field(default_factory=list)
    errors: list[B2BHistoricalSalesImportMessage] = field(default_factory=list)


EXPECTED_HEADERS = (
    "order_number",
    "delivery_date",
    "customer_name",
    "channel",
    "sku",
    "description",
    "quantity",
    "unit_price",
    "line_total",
    "cost_unit",
    "cost_total",
    "gross_profit",
    "gross_profit_percent",
    "observations",
)

_QUANT = Decimal("0.0001")
_ZERO = Decimal("0.0000")


def import_b2b_historical_sales_csv(db: Session, *, file_name: str, file_bytes: bytes) -> B2BHistoricalSalesImportResult:
    if not file_bytes:
        raise B2BHistoricalSalesImportValidationError("Please choose a CSV file to import.")

    decoded_text = _decode_csv_bytes(file_bytes)
    rows = _read_csv_rows(decoded_text)

    customer_lookup = _build_b2b_customer_lookup(db.query(B2BCustomer).order_by(B2BCustomer.id).all())
    channel_lookup = _build_channel_lookup(
        db.query(Channel).filter(Channel.applies_to_b2b.is_(True)).order_by(Channel.id).all()
    )
    existing_numbers = {
        value
        for (value,) in db.query(B2BSalesOrder.order_number).all()
    }

    warnings: list[B2BHistoricalSalesImportMessage] = []
    errors: list[B2BHistoricalSalesImportMessage] = []
    grouped_rows: dict[str, list[tuple[int, dict[str, str]]]] = defaultdict(list)
    total_rows = 0
    invalid_rows = 0

    for row_number, row in rows:
        if _row_is_blank(row):
            continue
        total_rows += 1
        order_number = _field_value(row, "order_number")
        if not order_number:
            errors.append(B2BHistoricalSalesImportMessage(row_number=row_number, message="order_number is required."))
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
                B2BHistoricalSalesImportMessage(
                    row_number=order_rows[0][0],
                    message=f"Order '{order_number}' already exists. Order skipped.",
                )
            )
            continue

        order_payload = _build_order_payload(
            order_number=order_number,
            order_rows=order_rows,
            customer_lookup=customer_lookup,
            channel_lookup=channel_lookup,
            warnings=warnings,
            errors=errors,
        )
        if order_payload is None:
            invalid_rows += len(order_rows)
            continue

        order = B2BSalesOrder(
            order_number=order_payload["order_number"],
            customer_id=order_payload["customer_id"],
            customer_name_snapshot=order_payload["customer_name_snapshot"],
            channel_id=order_payload["channel_id"],
            b2b_channel_name_snapshot=order_payload["channel_name_snapshot"],
            delivery_date=order_payload["delivery_date"],
            status="invoiced",
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
                B2BSalesOrderLine(
                    sales_order_id=order.id,
                    line_number=line_number,
                    sku_snapshot=line_data["sku_snapshot"],
                    description_snapshot=line_data["description_snapshot"],
                    unit_price_snapshot=line_data["unit_price_snapshot"],
                    quantity=line_data["quantity"],
                    line_total=line_data["line_total"],
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
    return B2BHistoricalSalesImportResult(
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
    customer_lookup: dict[str, B2BCustomer | list[B2BCustomer]],
    channel_lookup: dict[str, Channel | list[Channel]],
    warnings: list[B2BHistoricalSalesImportMessage],
    errors: list[B2BHistoricalSalesImportMessage],
) -> dict[str, object] | None:
    first_row_number, first_row = order_rows[0]
    customer_name = _field_value(first_row, "customer_name")
    delivery_date_text = _field_value(first_row, "delivery_date")
    channel_name = _field_value(first_row, "channel")
    observations = _field_value(first_row, "observations")

    if not customer_name:
        errors.append(B2BHistoricalSalesImportMessage(first_row_number, f"Order '{order_number}' is missing customer_name."))
        return None
    customer_match = customer_lookup.get(_normalize_key(customer_name))
    if customer_match is None:
        errors.append(
            B2BHistoricalSalesImportMessage(
                first_row_number,
                f"Order '{order_number}' references B2B customer '{customer_name}' which does not exist. Order skipped.",
            )
        )
        return None
    if isinstance(customer_match, list):
        errors.append(
            B2BHistoricalSalesImportMessage(
                first_row_number,
                f"Order '{order_number}' matches multiple B2B customers for '{customer_name}'. Order skipped.",
            )
        )
        return None

    delivery_date = _parse_date(delivery_date_text, field_name="delivery_date", row_number=first_row_number, errors=errors)
    if delivery_date is None:
        return None

    if not _rows_share_same_text(order_rows, "customer_name"):
        errors.append(B2BHistoricalSalesImportMessage(first_row_number, f"Order '{order_number}' has inconsistent customer_name values."))
        return None
    if not _rows_share_same_text(order_rows, "delivery_date"):
        errors.append(B2BHistoricalSalesImportMessage(first_row_number, f"Order '{order_number}' has inconsistent delivery_date values."))
        return None
    if not _rows_share_same_text(order_rows, "channel"):
        errors.append(B2BHistoricalSalesImportMessage(first_row_number, f"Order '{order_number}' has inconsistent channel values."))
        return None
    if not _rows_share_same_text(order_rows, "observations"):
        errors.append(B2BHistoricalSalesImportMessage(first_row_number, f"Order '{order_number}' has inconsistent observations values."))
        return None

    lines: list[dict[str, Decimal | str | None]] = []
    total_amount = _ZERO
    order_has_complete_cogs = True
    order_cost_total = _ZERO

    for row_number, row in order_rows:
        sku = _field_value(row, "sku")
        description = _field_value(row, "description")
        if not sku or not description:
            errors.append(
                B2BHistoricalSalesImportMessage(
                    row_number,
                    f"Order '{order_number}' requires sku and description in every row.",
                )
            )
            return None

        quantity = _parse_decimal(row.get("quantity"), row_number=row_number, field_name="quantity", errors=errors, allow_blank=False)
        unit_price = _parse_decimal(row.get("unit_price"), row_number=row_number, field_name="unit_price", errors=errors, allow_blank=False)
        line_total = _parse_decimal(row.get("line_total"), row_number=row_number, field_name="line_total", errors=errors, allow_blank=False)
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
        if None in {quantity, unit_price, line_total}:
            return None
        if quantity < _ZERO or unit_price < _ZERO or line_total < _ZERO:
            errors.append(B2BHistoricalSalesImportMessage(row_number, f"Order '{order_number}' contains negative quantity or price values."))
            return None
        if cost_unit is not None and cost_unit < _ZERO:
            errors.append(B2BHistoricalSalesImportMessage(row_number, f"Order '{order_number}' contains negative cost_unit values."))
            return None
        if cost_total is not None and cost_total < _ZERO:
            errors.append(B2BHistoricalSalesImportMessage(row_number, f"Order '{order_number}' contains negative cost_total values."))
            return None

        if gross_profit is None and cost_total is not None:
            gross_profit = (line_total - cost_total).quantize(_QUANT)
        if gross_profit_percent is None and gross_profit is not None:
            gross_profit_percent = (gross_profit / line_total).quantize(_QUANT) if line_total > _ZERO else _ZERO

        if cost_total is None:
            order_has_complete_cogs = False
        else:
            order_cost_total = (order_cost_total + cost_total).quantize(_QUANT)

        total_amount = (total_amount + line_total).quantize(_QUANT)
        lines.append(
            {
                "sku_snapshot": sku,
                "description_snapshot": description,
                "unit_price_snapshot": unit_price,
                "quantity": quantity,
                "line_total": line_total,
                "cost_unit_snapshot": cost_unit,
                "cost_total_snapshot": cost_total,
                "gross_margin_amount": gross_profit,
                "gross_margin_percent": gross_profit_percent,
            }
        )

    order_cost_snapshot = order_cost_total if order_has_complete_cogs else None
    order_gross_profit = (total_amount - order_cost_total).quantize(_QUANT) if order_has_complete_cogs else None
    if order_has_complete_cogs:
        order_gross_profit_percent = (order_gross_profit / total_amount).quantize(_QUANT) if total_amount > _ZERO else _ZERO
    else:
        order_gross_profit_percent = None

    channel_id = None
    if channel_name:
        channel_match = channel_lookup.get(_normalize_key(channel_name))
        if isinstance(channel_match, Channel):
            channel_id = channel_match.id
        elif isinstance(channel_match, list):
            warnings.append(
                B2BHistoricalSalesImportMessage(
                    first_row_number,
                    f"Order '{order_number}' channel '{channel_name}' matched multiple channels. Snapshot text preserved without channel_id.",
                )
            )

    return {
        "order_number": order_number,
        "customer_id": customer_match.id,
        "customer_name_snapshot": customer_name,
        "channel_id": channel_id,
        "channel_name_snapshot": channel_name or None,
        "delivery_date": delivery_date,
        "total_amount": total_amount,
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
        raise B2BHistoricalSalesImportValidationError("The CSV file does not contain a header row.")

    normalized_fieldnames = [_normalize_header(fieldname) for fieldname in reader.fieldnames]
    missing = [header for header in EXPECTED_HEADERS if header not in normalized_fieldnames]
    if missing:
        raise B2BHistoricalSalesImportValidationError(
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
    raise B2BHistoricalSalesImportValidationError(
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


def _build_b2b_customer_lookup(customers: list[B2BCustomer]) -> dict[str, B2BCustomer | list[B2BCustomer]]:
    lookup: dict[str, B2BCustomer | list[B2BCustomer]] = {}
    for customer in customers:
        key = _normalize_key(customer.customer_name)
        existing = lookup.get(key)
        if existing is None:
            lookup[key] = customer
        elif isinstance(existing, list):
            existing.append(customer)
        else:
            lookup[key] = [existing, customer]
    return lookup


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


def _parse_date(value: str, *, field_name: str, row_number: int, errors: list[B2BHistoricalSalesImportMessage]):
    text = (value or "").strip()
    if not text:
        errors.append(B2BHistoricalSalesImportMessage(row_number, f"{field_name} is required."))
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        errors.append(B2BHistoricalSalesImportMessage(row_number, f"{field_name} must use YYYY-MM-DD format."))
        return None


def _parse_decimal(
    raw_value: object,
    *,
    row_number: int,
    field_name: str,
    errors: list[B2BHistoricalSalesImportMessage],
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


def _append_decimal_error(row_number: int, field_name: str, errors: list[B2BHistoricalSalesImportMessage]) -> None:
    errors.append(B2BHistoricalSalesImportMessage(row_number, f"{field_name} must be a valid decimal value."))
    return None


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
