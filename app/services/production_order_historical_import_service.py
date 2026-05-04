import csv
import io
import unicodedata
from dataclasses import dataclass, field
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation

from sqlalchemy.orm import Session

from app.models import Product, ProductionOrder, Route


class ProductionOrderHistoricalImportValidationError(Exception):
    pass


@dataclass(frozen=True)
class ProductionOrderHistoricalImportMessage:
    row_number: int
    message: str


@dataclass(frozen=True)
class ProductionOrderHistoricalImportResult:
    file_name: str
    total_rows: int
    created_production_orders: int
    skipped_existing_production_orders: int
    invalid_rows: int
    warnings: list[ProductionOrderHistoricalImportMessage] = field(default_factory=list)
    errors: list[ProductionOrderHistoricalImportMessage] = field(default_factory=list)


EXPECTED_HEADERS = (
    "internal_order_number",
    "production_date",
    "product_sku",
    "route_name",
    "planned_qty",
    "input_qty",
    "output_qty",
    "unit",
    "yield_percent",
    "material_snapshot_cost_total",
    "real_labor_cost_total",
    "real_machine_cost_total",
    "real_overhead_cost_total",
    "real_total_cost",
    "notes",
    "closed_at",
)

_QUANT = Decimal("0.0001")
_ZERO = Decimal("0.0000")


def import_historical_production_orders_csv(
    db: Session,
    *,
    file_name: str,
    file_bytes: bytes,
) -> ProductionOrderHistoricalImportResult:
    if not file_bytes:
        raise ProductionOrderHistoricalImportValidationError("Please choose a CSV file to import.")

    decoded_text = _decode_csv_bytes(file_bytes)
    rows = _read_csv_rows(decoded_text)
    existing_numbers = {value for (value,) in db.query(ProductionOrder.internal_order_number).all()}
    product_lookup = {
        _normalize_key(product.sku): product
        for product in db.query(Product).order_by(Product.id).all()
    }
    route_lookup = _build_active_route_lookup(db.query(Route).filter(Route.active.is_(True)).order_by(Route.id).all())

    warnings: list[ProductionOrderHistoricalImportMessage] = []
    errors: list[ProductionOrderHistoricalImportMessage] = []
    total_rows = 0
    invalid_rows = 0
    created_production_orders = 0
    skipped_existing_production_orders = 0

    for row_number, row in rows:
        if _row_is_blank(row):
            continue
        total_rows += 1

        internal_order_number = _field_value(row, "internal_order_number")
        if not internal_order_number:
            errors.append(
                ProductionOrderHistoricalImportMessage(
                    row_number=row_number,
                    message="internal_order_number is required.",
                )
            )
            invalid_rows += 1
            continue

        if internal_order_number in existing_numbers:
            skipped_existing_production_orders += 1
            warnings.append(
                ProductionOrderHistoricalImportMessage(
                    row_number=row_number,
                    message=f"Production Order '{internal_order_number}' already exists. Order skipped.",
                )
            )
            continue

        payload = _build_production_order_payload(
            row_number=row_number,
            row=row,
            product_lookup=product_lookup,
            route_lookup=route_lookup,
            errors=errors,
        )
        if payload is None:
            invalid_rows += 1
            continue

        order = ProductionOrder(
            internal_order_number=payload["internal_order_number"],
            production_date=payload["production_date"],
            product_id=payload["product_id"],
            product_sku_snapshot=payload["product_sku_snapshot"],
            product_name_snapshot=payload["product_name_snapshot"],
            route_id=payload["route_id"],
            route_name_snapshot=payload["route_name_snapshot"],
            route_version_snapshot=payload["route_version_snapshot"],
            process_type=payload["process_type"],
            planned_qty=payload["planned_qty"],
            input_qty=payload["input_qty"],
            output_qty=payload["output_qty"],
            unit=payload["unit"],
            yield_percent=payload["yield_percent"],
            material_snapshot_cost_total=payload["material_snapshot_cost_total"],
            real_labor_cost_total=payload["real_labor_cost_total"],
            real_machine_cost_total=payload["real_machine_cost_total"],
            real_overhead_cost_total=payload["real_overhead_cost_total"],
            real_total_cost=payload["real_total_cost"],
            real_unit_cost=payload["real_unit_cost"],
            status="closed",
            notes=payload["notes"],
            closed_at=payload["closed_at"],
        )
        db.add(order)

        existing_numbers.add(internal_order_number)
        created_production_orders += 1

    db.commit()
    return ProductionOrderHistoricalImportResult(
        file_name=file_name,
        total_rows=total_rows,
        created_production_orders=created_production_orders,
        skipped_existing_production_orders=skipped_existing_production_orders,
        invalid_rows=invalid_rows,
        warnings=warnings,
        errors=errors,
    )


def _build_production_order_payload(
    *,
    row_number: int,
    row: dict[str, str],
    product_lookup: dict[str, Product],
    route_lookup: dict[str, Route | list[Route]],
    errors: list[ProductionOrderHistoricalImportMessage],
) -> dict[str, object] | None:
    internal_order_number = _field_value(row, "internal_order_number")
    product_sku = _field_value(row, "product_sku")
    route_name = _field_value(row, "route_name")
    notes = _field_value(row, "notes")

    production_date = _parse_required_date(row.get("production_date"), field_name="production_date", row_number=row_number, errors=errors)
    planned_qty = _parse_decimal(row.get("planned_qty"), row_number=row_number, field_name="planned_qty", errors=errors, allow_blank=False)
    input_qty = _parse_decimal(row.get("input_qty"), row_number=row_number, field_name="input_qty", errors=errors, allow_blank=True)
    output_qty = _parse_decimal(row.get("output_qty"), row_number=row_number, field_name="output_qty", errors=errors, allow_blank=True)
    yield_percent = _parse_decimal(row.get("yield_percent"), row_number=row_number, field_name="yield_percent", errors=errors, allow_blank=True)
    material_snapshot_cost_total = _parse_decimal(
        row.get("material_snapshot_cost_total"),
        row_number=row_number,
        field_name="material_snapshot_cost_total",
        errors=errors,
        allow_blank=True,
    )
    real_labor_cost_total = _parse_decimal(
        row.get("real_labor_cost_total"),
        row_number=row_number,
        field_name="real_labor_cost_total",
        errors=errors,
        allow_blank=True,
    )
    real_machine_cost_total = _parse_decimal(
        row.get("real_machine_cost_total"),
        row_number=row_number,
        field_name="real_machine_cost_total",
        errors=errors,
        allow_blank=True,
    )
    real_overhead_cost_total = _parse_decimal(
        row.get("real_overhead_cost_total"),
        row_number=row_number,
        field_name="real_overhead_cost_total",
        errors=errors,
        allow_blank=True,
    )
    real_total_cost = _parse_decimal(
        row.get("real_total_cost"),
        row_number=row_number,
        field_name="real_total_cost",
        errors=errors,
        allow_blank=True,
    )
    closed_at = _parse_optional_closed_at(row.get("closed_at"), production_date=production_date, row_number=row_number, errors=errors)

    if production_date is None or planned_qty is None or closed_at is None:
        return None
    if not product_sku:
        errors.append(ProductionOrderHistoricalImportMessage(row_number, "product_sku is required."))
        return None
    if not route_name:
        errors.append(ProductionOrderHistoricalImportMessage(row_number, "route_name is required."))
        return None

    numeric_values = [
        ("planned_qty", planned_qty),
        ("input_qty", input_qty),
        ("output_qty", output_qty),
        ("yield_percent", yield_percent),
        ("material_snapshot_cost_total", material_snapshot_cost_total),
        ("real_labor_cost_total", real_labor_cost_total),
        ("real_machine_cost_total", real_machine_cost_total),
        ("real_overhead_cost_total", real_overhead_cost_total),
        ("real_total_cost", real_total_cost),
    ]
    for field_name, value in numeric_values:
        if value is not None and value < _ZERO:
            errors.append(ProductionOrderHistoricalImportMessage(row_number, f"{field_name} cannot be negative."))
            return None

    product = product_lookup.get(_normalize_key(product_sku))
    if product is None:
        errors.append(
            ProductionOrderHistoricalImportMessage(
                row_number,
                f"Production Order '{internal_order_number}' references product_sku '{product_sku}' which does not exist. Order skipped.",
            )
        )
        return None
    if hasattr(product, "is_manufactured") and not product.is_manufactured:
        errors.append(
            ProductionOrderHistoricalImportMessage(
                row_number,
                f"Production Order '{internal_order_number}' references product_sku '{product_sku}' which is not a manufactured product. Order skipped.",
            )
        )
        return None

    route_match = route_lookup.get(_normalize_key(route_name))
    if route_match is None:
        errors.append(
            ProductionOrderHistoricalImportMessage(
                row_number,
                f"Production Order '{internal_order_number}' references route_name '{route_name}' which does not exist. Order skipped.",
            )
        )
        return None
    if isinstance(route_match, list):
        errors.append(
            ProductionOrderHistoricalImportMessage(
                row_number,
                f"Production Order '{internal_order_number}' matches multiple active routes for '{route_name}'. Order skipped.",
            )
        )
        return None

    if yield_percent is None and input_qty is not None and output_qty is not None and input_qty > _ZERO:
        yield_percent = ((output_qty / input_qty) * Decimal("100")).quantize(_QUANT)

    if real_total_cost is None:
        cost_components = (
            material_snapshot_cost_total,
            real_labor_cost_total,
            real_machine_cost_total,
            real_overhead_cost_total,
        )
        if all(component is not None for component in cost_components):
            real_total_cost = sum(cost_components, _ZERO).quantize(_QUANT)

    real_unit_cost = None
    if real_total_cost is not None and output_qty is not None and output_qty > _ZERO:
        real_unit_cost = (real_total_cost / output_qty).quantize(_QUANT)

    unit = _field_value(row, "unit") or (product.unit or None)

    return {
        "internal_order_number": internal_order_number,
        "production_date": production_date,
        "product_id": product.id,
        "product_sku_snapshot": product.sku,
        "product_name_snapshot": product.name,
        "route_id": route_match.id,
        "route_name_snapshot": route_match.name,
        "route_version_snapshot": route_match.version,
        "process_type": route_match.process_type,
        "planned_qty": planned_qty,
        "input_qty": input_qty,
        "output_qty": output_qty,
        "unit": unit,
        "yield_percent": yield_percent,
        "material_snapshot_cost_total": material_snapshot_cost_total,
        "real_labor_cost_total": real_labor_cost_total,
        "real_machine_cost_total": real_machine_cost_total,
        "real_overhead_cost_total": real_overhead_cost_total,
        "real_total_cost": real_total_cost,
        "real_unit_cost": real_unit_cost,
        "notes": _build_historical_notes(notes),
        "closed_at": closed_at,
    }


def _read_csv_rows(decoded_text: str) -> list[tuple[int, dict[str, str]]]:
    sample = decoded_text[:4096]
    delimiter = _detect_delimiter(sample)
    reader = csv.DictReader(io.StringIO(decoded_text), delimiter=delimiter)
    if reader.fieldnames is None:
        raise ProductionOrderHistoricalImportValidationError("The CSV file does not contain a header row.")

    normalized_fieldnames = [_normalize_header(fieldname) for fieldname in reader.fieldnames]
    missing = [header for header in EXPECTED_HEADERS if header not in normalized_fieldnames]
    if missing:
        raise ProductionOrderHistoricalImportValidationError(
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
    raise ProductionOrderHistoricalImportValidationError(
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


def _build_active_route_lookup(routes: list[Route]) -> dict[str, Route | list[Route]]:
    lookup: dict[str, Route | list[Route]] = {}
    for route in routes:
        key = _normalize_key(route.name)
        existing = lookup.get(key)
        if existing is None:
            lookup[key] = route
        elif isinstance(existing, list):
            existing.append(route)
        else:
            lookup[key] = [existing, route]
    return lookup


def _parse_required_date(value: str | None, *, field_name: str, row_number: int, errors: list[ProductionOrderHistoricalImportMessage]) -> date | None:
    text = (value or "").strip()
    if not text:
        errors.append(ProductionOrderHistoricalImportMessage(row_number, f"{field_name} is required."))
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        errors.append(ProductionOrderHistoricalImportMessage(row_number, f"{field_name} must use YYYY-MM-DD format."))
        return None


def _parse_optional_closed_at(
    value: str | None,
    *,
    production_date: date | None,
    row_number: int,
    errors: list[ProductionOrderHistoricalImportMessage],
) -> datetime | None:
    text = (value or "").strip()
    if not text:
        if production_date is None:
            return None
        return datetime.combine(production_date, time.min)
    for date_format in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            parsed = datetime.strptime(text, date_format)
            if date_format == "%Y-%m-%d":
                return datetime.combine(parsed.date(), time.min)
            return parsed
        except ValueError:
            continue
    errors.append(ProductionOrderHistoricalImportMessage(row_number, "closed_at must use YYYY-MM-DD or ISO datetime format."))
    return None


def _parse_decimal(
    raw_value: object,
    *,
    row_number: int,
    field_name: str,
    errors: list[ProductionOrderHistoricalImportMessage],
    allow_blank: bool,
) -> Decimal | None:
    text = str(raw_value or "").strip()
    if not text:
        return None if allow_blank else _append_decimal_error(row_number, field_name, errors)
    normalized = text.replace(" ", "").replace("â‚¡", "").replace("$", "")
    if "," in normalized and "." in normalized:
        normalized = normalized.replace(",", "")
    elif "," in normalized:
        normalized = normalized.replace(",", ".")
    try:
        return Decimal(normalized).quantize(_QUANT)
    except InvalidOperation:
        return _append_decimal_error(row_number, field_name, errors)


def _append_decimal_error(row_number: int, field_name: str, errors: list[ProductionOrderHistoricalImportMessage]) -> None:
    errors.append(ProductionOrderHistoricalImportMessage(row_number, f"{field_name} must be a valid decimal value."))
    return None


def _build_historical_notes(csv_notes: str) -> str:
    base = "Historical CSV import"
    if csv_notes.strip():
        return f"{base}\n{csv_notes.strip()}"
    return base


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
