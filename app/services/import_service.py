import csv
import io
import re
import unicodedata
from collections import Counter
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models import ImportBatch, ImportedBomHeader, ImportedBomLine, Product, ProductCategory
from app.schemas import ComponentType


PARENT_SKU_KEYS = ("sku", "product sku", "item sku", "parent sku", "codigo", "codigo sku")
PARENT_NAME_KEYS = (
    "name",
    "product name",
    "item name",
    "parent name",
    "nombre",
    "nombre del producto",
)
PARENT_COST_KEYS = ("standard cost", "cost", "costo", "coste", "avg cost", "average cost")
PARENT_UNIT_KEYS = ("unit", "unidad", "sold by")
PARENT_HANDLE_KEYS = ("handle", "loyverse handle")
USE_PRODUCTION_KEYS = ("use production", "usar produccion", "produccion", "is composite")

COMPONENT_SKU_KEYS = (
    "component sku",
    "bom sku",
    "ingredient sku",
    "included item sku",
    "sku componente",
    "sku del componente",
)
COMPONENT_NAME_KEYS = (
    "component name",
    "bom item",
    "bom component",
    "ingredient",
    "ingredient name",
    "included item",
    "included item name",
    "nombre componente",
    "nombre del componente",
)
COMPONENT_QUANTITY_KEYS = (
    "component quantity",
    "quantity",
    "qty",
    "cantidad",
    "bom quantity",
    "included quantity",
)
COMPONENT_COST_KEYS = (
    "component cost",
    "component unit cost",
    "unit cost",
    "costo componente",
    "costo del componente",
)

LABOR_PATTERNS = ("mano de obra", "labor", "fictitious labor")
OVERHEAD_PATTERNS = ("indirecto", "indirectos", "overhead", "carga fabril", "gasto indirecto")
OTHER_FICTITIOUS_PATTERNS = ("maquila", "outsourcing", "servicio", "subcontrat")
PACKAGING_PATTERNS = (
    "empaque",
    "envase",
    "bolsa",
    "frasco",
    "tapa",
    "etiqueta",
    "caja",
    "packaging",
    "bandeja",
    "sello",
)

LOYVERSE_PARENT_SKU_INDEX = 1
LOYVERSE_PARENT_NAME_INDEX = 2
LOYVERSE_CATEGORY_INDEX = 3
LOYVERSE_AVERAGE_COST_INDEX = 12
LOYVERSE_SUPPLIER_INDEX = 18
LOYVERSE_AVAILABLE_FOR_SALE_GC_INDEX = 20
LOYVERSE_B2C_PRICE_INDEX = 21
LOYVERSE_INVENTORY_INDEX = 22
LOYVERSE_LOW_STOCK_INDEX = 23
LOYVERSE_OPTIMAL_STOCK_INDEX = 24
LOYVERSE_BOM_INCLUDED_SKU_INDEX = 14
LOYVERSE_BOM_QUANTITY_INDEX = 15
LOYVERSE_USE_PRODUCTION_INDEX = 17
LOYVERSE_HEADER_CANDIDATES = {
    "parent_sku": ("sku", "item sku", "product sku", "parent sku", "codigo", "codigo sku"),
    "parent_name": ("name", "item name", "product name", "parent name", "nombre", "nombre del producto"),
    "category": ("category", "category name", "categoria", "nombre categoria"),
    "average_cost": ("cost", "average cost", "avg cost", "standard cost", "purchase cost", "costo", "coste"),
    "supplier": ("supplier", "proveedor"),
    "use_production": ("use production", "usar produccion", "produccion", "is composite"),
    "available_for_sale_gc": (
        "available for sale [green corner]",
        "available for sale gc",
        "available for sale",
    ),
    "b2c_price": ("price [green corner]", "b2c price", "price", "precio [green corner]", "precio"),
    "inventory": ("in stock [green corner]", "in stock", "inventory", "inventario"),
    "low_stock": ("low stock", "low stock [green corner]", "stock minimo"),
    "optimal_stock": ("optimal stock", "optimal stock [green corner]", "stock optimo"),
    "bom_included_sku": ("included item sku", "component sku", "bom sku", "ingredient sku"),
    "bom_quantity": ("included quantity", "component quantity", "bom quantity", "quantity", "qty", "cantidad"),
}
EXCLUDED_BOM_INCLUDED_SKUS = {
    "10371",
    "10669",
    "10630",
    "10330",
    "10370",
    "10403",
    "10317",
    "10642",
    "10180",
    "10542",
    "10629",
    "10628",
}
MAX_LEDGER_INVENTORY_VALUE = Decimal("99999999.9999")
BOM_QUANTITY_SUSPICIOUS_THRESHOLD = Decimal("1000")


@dataclass(frozen=True)
class ImportSummary:
    batch_id: int
    product_master_upsert_count: int
    bom_parent_product_count: int
    bom_line_count: int
    component_type_counts: dict[str, int]
    unknown_count: int


@dataclass(frozen=True)
class LoyverseColumnMap:
    parent_sku: int = LOYVERSE_PARENT_SKU_INDEX
    parent_name: int = LOYVERSE_PARENT_NAME_INDEX
    category: int = LOYVERSE_CATEGORY_INDEX
    average_cost: int = LOYVERSE_AVERAGE_COST_INDEX
    supplier: int = LOYVERSE_SUPPLIER_INDEX
    use_production: int = LOYVERSE_USE_PRODUCTION_INDEX
    available_for_sale_gc: int = LOYVERSE_AVAILABLE_FOR_SALE_GC_INDEX
    b2c_price: int = LOYVERSE_B2C_PRICE_INDEX
    inventory: int = LOYVERSE_INVENTORY_INDEX
    low_stock: int = LOYVERSE_LOW_STOCK_INDEX
    optimal_stock: int = LOYVERSE_OPTIMAL_STOCK_INDEX
    bom_included_sku: int = LOYVERSE_BOM_INCLUDED_SKU_INDEX
    bom_quantity: int = LOYVERSE_BOM_QUANTITY_INDEX


@dataclass(frozen=True)
class ImportPrecheckIssue:
    row: int
    sku: str
    field: str
    original_value: str
    parsed_value: str | None
    risk_type: str
    blocking: bool


class GreenCornerDecimalParseError(ValueError):
    pass


class GreenCornerImportPrecheckError(ValueError):
    def __init__(self, issues: list[ImportPrecheckIssue]):
        self.issues = issues
        blocking_count = sum(1 for issue in issues if issue.blocking)
        super().__init__(
            f"Green Corner import precheck failed with {blocking_count} blocking issue(s)."
        )


def normalize_key(value: str | None) -> str:
    if not value:
        return ""
    text = remove_accents(value).strip().lower()
    return re.sub(r"\s+", " ", text)


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    text = remove_accents(value).strip().lower()
    return re.sub(r"\s+", " ", text)


def remove_accents(value: str) -> str:
    return "".join(
        char
        for char in unicodedata.normalize("NFKD", value)
        if not unicodedata.combining(char)
    )


def parse_decimal(value: str | int | float | Decimal | None) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    text = str(value).strip()
    if not text:
        return None

    text = text.replace("\xa0", "").replace(" ", "")
    text = re.sub(r"[^\d,.\-]", "", text)
    if not text or text in {"-", ".", ","}:
        return None

    if text.count("-") > 1 or ("-" in text and not text.startswith("-")):
        return None

    sign = "-" if text.startswith("-") else ""
    if sign:
        text = text[1:]

    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        if re.fullmatch(r"\d{1,3}(,\d{3})+", text):
            text = text.replace(",", "")
        else:
            text = text.replace(",", ".")
    elif "." in text:
        if re.fullmatch(r"\d{1,3}(\.\d{3})+", text):
            text = text.replace(".", "")

    text = f"{sign}{text}"

    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def detect_ambiguous_decimal_format(value: str | int | float | Decimal | None) -> bool:
    if value is None or isinstance(value, Decimal):
        return False
    text = str(value).strip()
    if not text:
        return False

    text = text.replace("\xa0", "").replace(" ", "")
    text = re.sub(r"[^\d,.\-]", "", text)
    if not text or text in {"-", ".", ","}:
        return False
    if text.count("-") > 1 or ("-" in text and not text.startswith("-")):
        return True

    unsigned = text[1:] if text.startswith("-") else text
    if "." in unsigned and "," in unsigned:
        return True
    if unsigned.count(".") > 1 or unsigned.count(",") > 1:
        return True
    if ".." in unsigned or ",," in unsigned:
        return True
    return False


def parse_decimal_strict_green_corner(value: str | int | float | Decimal | None) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    text = str(value).strip()
    if not text:
        return None
    if detect_ambiguous_decimal_format(text):
        raise GreenCornerDecimalParseError(f"Ambiguous decimal format: {text}")

    text = text.replace("\xa0", "").replace(" ", "")
    text = re.sub(r"[^\d,.\-]", "", text)
    if not text or text in {"-", ".", ","}:
        return None
    if text.count("-") > 1 or ("-" in text and not text.startswith("-")):
        raise GreenCornerDecimalParseError(f"Invalid decimal format: {text}")

    sign = "-" if text.startswith("-") else ""
    if sign:
        text = text[1:]
    text = text.replace(",", ".")
    if text.count(".") > 1:
        raise GreenCornerDecimalParseError(f"Ambiguous decimal format: {sign}{text}")

    try:
        return Decimal(f"{sign}{text}")
    except InvalidOperation as exc:
        raise GreenCornerDecimalParseError(f"Invalid decimal format: {value}") from exc


def parse_bool(value: str | None) -> bool:
    text = normalize_text(value)
    return text in {"1", "true", "yes", "y", "si", "sí", "x"}


def parse_available_for_sale(value: str | None) -> bool:
    text = normalize_text(value)
    return text in {"1", "true", "yes", "y", "si", "sí", "available", "active"}


def classify_component(component_sku: str | None, component_name: str | None) -> ComponentType:
    text = normalize_text(f"{component_sku or ''} {component_name or ''}")
    if not text:
        return ComponentType.UNKNOWN

    if any(pattern in text for pattern in LABOR_PATTERNS):
        return ComponentType.FICTITIOUS_LABOR
    if any(pattern in text for pattern in OVERHEAD_PATTERNS):
        return ComponentType.FICTITIOUS_OVERHEAD
    if any(pattern in text for pattern in OTHER_FICTITIOUS_PATTERNS):
        return ComponentType.FICTITIOUS_OTHER
    if any(pattern in text for pattern in PACKAGING_PATTERNS):
        return ComponentType.PACKAGING

    return ComponentType.MATERIAL


def should_include_in_real_cost(component_type: ComponentType) -> bool:
    return component_type in {ComponentType.MATERIAL, ComponentType.PACKAGING}


def _parse_green_corner_decimal(
    raw_value: str,
    *,
    row_number: int,
    sku: str,
    field_name: str,
    issues: list[ImportPrecheckIssue] | None = None,
) -> Decimal | None:
    try:
        return parse_decimal_strict_green_corner(raw_value)
    except GreenCornerDecimalParseError:
        if issues is None:
            raise
        issues.append(
            ImportPrecheckIssue(
                row=row_number,
                sku=sku,
                field=field_name,
                original_value=raw_value,
                parsed_value=None,
                risk_type="ambiguous_decimal_format",
                blocking=True,
            )
        )
        return None


def _is_exact_thousand_jump(previous_value: Decimal | None, new_value: Decimal | None) -> bool:
    if previous_value in {None, Decimal("0")} or new_value in {None, Decimal("0")}:
        return False
    return previous_value * Decimal("1000") == new_value or new_value * Decimal("1000") == previous_value


def _append_thousand_jump_issue(
    issues: list[ImportPrecheckIssue],
    *,
    row_number: int,
    sku: str,
    field_name: str,
    original_value: str,
    parsed_value: Decimal | None,
    previous_value: Decimal | None,
) -> None:
    if not _is_exact_thousand_jump(previous_value, parsed_value):
        return
    is_inventory_correction_warning = (
        field_name == "current_inventory_qty"
        and parsed_value is not None
        and previous_value is not None
        and parsed_value >= Decimal("0")
        and previous_value > parsed_value
        and parsed_value * Decimal("1000") == previous_value
    )
    issues.append(
        ImportPrecheckIssue(
            row=row_number,
            sku=sku,
            field=field_name,
            original_value=original_value,
            parsed_value=None if parsed_value is None else str(parsed_value),
            risk_type="exact_x1000_jump_vs_existing",
            blocking=not is_inventory_correction_warning,
        )
    )


def run_green_corner_import_precheck(
    db: Session,
    csv_rows: list[list[str]],
    columns: LoyverseColumnMap | None = None,
) -> None:
    issues: list[ImportPrecheckIssue] = []
    product_cache: dict[str, Product | None] = {}
    current_parent_sku: str | None = None
    resolved_columns, has_header_row = _resolve_loyverse_column_map(csv_rows)
    columns = columns or resolved_columns

    if has_header_row:
        for field_name in _missing_required_loyverse_headers(csv_rows[0]):
            issues.append(
                ImportPrecheckIssue(
                    row=1,
                    sku="(header)",
                    field=field_name,
                    original_value="",
                    parsed_value=None,
                    risk_type="missing_required_loyverse_header",
                    blocking=True,
                )
            )

    for row_number, row in enumerate(csv_rows, start=1):
        if row_number == 1 and _looks_like_header_row(row):
            continue

        sku = _cell(row, columns.parent_sku)
        if _is_valid_loyverse_product_row(row, columns):
            current_parent_sku = sku
            if sku not in product_cache:
                product_cache[sku] = db.query(Product).filter(Product.sku == sku).one_or_none()
            existing_product = product_cache[sku]

            standard_cost_raw = _cell(row, columns.average_cost)
            inventory_raw = _cell(row, columns.inventory)
            low_stock_raw = _cell(row, columns.low_stock)
            optimal_stock_raw = _cell(row, columns.optimal_stock)
            b2c_price_raw = _cell(row, columns.b2c_price)

            standard_cost = _parse_green_corner_decimal(
                standard_cost_raw, row_number=row_number, sku=sku, field_name="standard_cost", issues=issues
            )
            current_inventory_qty = _parse_green_corner_decimal(
                inventory_raw, row_number=row_number, sku=sku, field_name="current_inventory_qty", issues=issues
            )
            low_stock_qty = _parse_green_corner_decimal(
                low_stock_raw, row_number=row_number, sku=sku, field_name="low_stock_qty", issues=issues
            )
            optimal_stock_qty = _parse_green_corner_decimal(
                optimal_stock_raw, row_number=row_number, sku=sku, field_name="optimal_stock_qty", issues=issues
            )
            _parse_green_corner_decimal(
                b2c_price_raw, row_number=row_number, sku=sku, field_name="b2c_price", issues=issues
            )

            if existing_product is not None:
                _append_thousand_jump_issue(
                    issues,
                    row_number=row_number,
                    sku=sku,
                    field_name="current_inventory_qty",
                    original_value=inventory_raw,
                    parsed_value=current_inventory_qty,
                    previous_value=existing_product.current_inventory_qty,
                )
                _append_thousand_jump_issue(
                    issues,
                    row_number=row_number,
                    sku=sku,
                    field_name="standard_cost",
                    original_value=standard_cost_raw,
                    parsed_value=standard_cost,
                    previous_value=existing_product.standard_cost,
                )
                _append_thousand_jump_issue(
                    issues,
                    row_number=row_number,
                    sku=sku,
                    field_name="low_stock_qty",
                    original_value=low_stock_raw,
                    parsed_value=low_stock_qty,
                    previous_value=existing_product.low_stock_qty,
                )
                _append_thousand_jump_issue(
                    issues,
                    row_number=row_number,
                    sku=sku,
                    field_name="optimal_stock_qty",
                    original_value=optimal_stock_raw,
                    parsed_value=optimal_stock_qty,
                    previous_value=existing_product.optimal_stock_qty,
                )

            if current_inventory_qty is not None and standard_cost is not None:
                inventory_value = current_inventory_qty * standard_cost
                if abs(inventory_value) > MAX_LEDGER_INVENTORY_VALUE:
                    issues.append(
                        ImportPrecheckIssue(
                            row=row_number,
                            sku=sku,
                            field="inventory_value",
                            original_value=f"{inventory_raw} * {standard_cost_raw}",
                            parsed_value=str(inventory_value),
                            risk_type="inventory_value_exceeds_numeric_12_4",
                            blocking=True,
                        )
                    )

        component_sku = _cell(row, columns.bom_included_sku)
        component_quantity_raw = _cell(row, columns.bom_quantity)
        if component_sku or component_quantity_raw:
            component_quantity = _parse_green_corner_decimal(
                component_quantity_raw,
                row_number=row_number,
                sku=current_parent_sku or sku or "(no-parent-sku)",
                field_name="bom_quantity",
                issues=issues,
            )
            if component_quantity is not None and abs(component_quantity) >= BOM_QUANTITY_SUSPICIOUS_THRESHOLD:
                issues.append(
                    ImportPrecheckIssue(
                        row=row_number,
                        sku=current_parent_sku or sku or "(no-parent-sku)",
                        field="bom_quantity",
                        original_value=component_quantity_raw,
                        parsed_value=str(component_quantity),
                        risk_type="suspicious_bom_quantity",
                        blocking=True,
                    )
                )

    if any(issue.blocking for issue in issues):
        raise GreenCornerImportPrecheckError(issues)


def import_loyverse_csv(db: Session, file_name: str, content: bytes) -> ImportSummary:
    text = _decode_csv_content(content)
    csv_rows, detected_delimiter = _parse_csv_rows(text)
    columns, _has_header = _resolve_loyverse_column_map(csv_rows)
    run_green_corner_import_precheck(db, csv_rows, columns)

    batch = ImportBatch(file_name=file_name)
    db.add(batch)
    db.flush()

    current_header: ImportedBomHeader | None = None
    upserted_product_skus: set[str] = set()
    imported_products: set[str] = set()
    component_type_counts: Counter[str] = Counter()
    bom_line_count = 0

    for row_number, row in enumerate(csv_rows, start=1):
        if row_number == 1 and _looks_like_header_row(row):
            continue

        product = _upsert_product_master_from_loyverse_row(db, row, columns)
        if product is not None:
            upserted_product_skus.add(product.sku)
            if product.is_manufactured:
                current_header = _create_parent_records_from_loyverse_row(db, batch, row, product, columns)
                imported_products.add(current_header.product_sku)
            else:
                current_header = None

        component = _extract_component_from_loyverse_row(row, columns)
        if current_header is None or not _has_component_signal(component):
            continue
        if _is_excluded_bom_sku(component["sku"]):
            continue

        component_type = classify_component(component["sku"], component["name"])
        db.add(
            ImportedBomLine(
                bom_header=current_header,
                source_row_number=row_number,
                component_sku=component["sku"],
                component_name=component["name"],
                quantity=component["quantity"],
                component_cost=component["cost"],
                component_type=component_type.value,
                include_in_real_cost=should_include_in_real_cost(component_type),
            )
        )
        component_type_counts[component_type.value] += 1
        bom_line_count += 1

    batch.product_master_upsert_count = len(upserted_product_skus)

    if batch.product_master_upsert_count == 0 and bom_line_count == 0 and _has_nonempty_rows(csv_rows):
        batch.notes = (
            "Warning: No products or BOM lines were imported. "
            f"The CSV did not match the expected Loyverse structure after delimiter detection "
            f"('{_delimiter_name(detected_delimiter)}')."
        )

    db.commit()
    return ImportSummary(
        batch_id=batch.id,
        product_master_upsert_count=batch.product_master_upsert_count,
        bom_parent_product_count=len(imported_products),
        bom_line_count=bom_line_count,
        component_type_counts=dict(component_type_counts),
        unknown_count=component_type_counts[ComponentType.UNKNOWN.value],
    )


def _normalize_row(row: dict[str, str]) -> dict[str, str]:
    return {normalize_key(key): (value or "").strip() for key, value in row.items()}


def _parse_csv_rows(text: str) -> tuple[list[list[str]], str]:
    delimiter = _detect_csv_delimiter(text)
    rows = list(csv.reader(io.StringIO(text), delimiter=delimiter, quotechar='"'))
    return rows, delimiter


def _normalized_header_map(row: list[str]) -> dict[str, int]:
    return {
        normalize_key(cell): index
        for index, cell in enumerate(row)
        if normalize_key(cell)
    }


def _resolve_loyverse_column_map(csv_rows: list[list[str]]) -> tuple[LoyverseColumnMap, bool]:
    if not csv_rows:
        return LoyverseColumnMap(), False
    first_row = csv_rows[0]
    if not _looks_like_header_row(first_row):
        return LoyverseColumnMap(), False

    header_map = _normalized_header_map(first_row)

    def _resolve(field_name: str, fallback: int) -> int:
        for candidate in LOYVERSE_HEADER_CANDIDATES[field_name]:
            match = header_map.get(normalize_key(candidate))
            if match is not None:
                return match
        return fallback

    return (
        LoyverseColumnMap(
            parent_sku=_resolve("parent_sku", LOYVERSE_PARENT_SKU_INDEX),
            parent_name=_resolve("parent_name", LOYVERSE_PARENT_NAME_INDEX),
            category=_resolve("category", LOYVERSE_CATEGORY_INDEX),
            average_cost=_resolve("average_cost", LOYVERSE_AVERAGE_COST_INDEX),
            supplier=_resolve("supplier", LOYVERSE_SUPPLIER_INDEX),
            use_production=_resolve("use_production", LOYVERSE_USE_PRODUCTION_INDEX),
            available_for_sale_gc=_resolve("available_for_sale_gc", LOYVERSE_AVAILABLE_FOR_SALE_GC_INDEX),
            b2c_price=_resolve("b2c_price", LOYVERSE_B2C_PRICE_INDEX),
            inventory=_resolve("inventory", LOYVERSE_INVENTORY_INDEX),
            low_stock=_resolve("low_stock", LOYVERSE_LOW_STOCK_INDEX),
            optimal_stock=_resolve("optimal_stock", LOYVERSE_OPTIMAL_STOCK_INDEX),
            bom_included_sku=_resolve("bom_included_sku", LOYVERSE_BOM_INCLUDED_SKU_INDEX),
            bom_quantity=_resolve("bom_quantity", LOYVERSE_BOM_QUANTITY_INDEX),
        ),
        True,
    )


def _detect_csv_delimiter(text: str) -> str:
    sample_lines = [line for line in text.splitlines() if line.strip()][:20]
    sample = "\n".join(sample_lines)
    if sample:
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;")
            if dialect.delimiter in {",", ";"}:
                return dialect.delimiter
        except csv.Error:
            pass

    semicolon_count = sum(line.count(";") for line in sample_lines)
    comma_count = sum(line.count(",") for line in sample_lines)
    if semicolon_count > comma_count:
        return ";"
    return ","


def _cell(row: list[str], index: int) -> str:
    if index >= len(row):
        return ""
    return (row[index] or "").strip()


def _looks_like_header_row(row: list[str]) -> bool:
    normalized_cells = {
        normalize_key(cell)
        for cell in row
        if normalize_key(cell)
    }
    if not normalized_cells:
        return False

    required_header_groups = (
        "parent_sku",
        "parent_name",
        "inventory",
        "average_cost",
    )
    matched_groups = sum(
        1
        for field_name in required_header_groups
        if any(
            normalize_key(candidate) in normalized_cells
            for candidate in LOYVERSE_HEADER_CANDIDATES[field_name]
        )
    )
    return matched_groups >= 2


def _missing_required_loyverse_headers(row: list[str]) -> list[str]:
    header_map = _normalized_header_map(row)
    required_fields = (
        "parent_sku",
        "parent_name",
        "average_cost",
        "inventory",
        "low_stock",
        "optimal_stock",
        "b2c_price",
    )
    missing: list[str] = []
    for field_name in required_fields:
        if not any(normalize_key(candidate) in header_map for candidate in LOYVERSE_HEADER_CANDIDATES[field_name]):
            missing.append(field_name)
    return missing


def _has_nonempty_rows(rows: list[list[str]]) -> bool:
    return any(any((cell or "").strip() for cell in row) for row in rows)


def _delimiter_name(delimiter: str) -> str:
    return "semicolon" if delimiter == ";" else "comma"


def _is_loyverse_parent_row(row: list[str], columns: LoyverseColumnMap) -> bool:
    return bool(_cell(row, columns.parent_sku))


def _is_valid_loyverse_product_row(row: list[str], columns: LoyverseColumnMap) -> bool:
    return bool(_cell(row, columns.parent_sku) and _cell(row, columns.parent_name))


def _is_manufactured_parent_row(row: list[str], columns: LoyverseColumnMap) -> bool:
    return normalize_text(_cell(row, columns.use_production)) == "y"


def _is_excluded_bom_sku(component_sku: str | Decimal | None) -> bool:
    if component_sku is None:
        return False
    return str(component_sku).strip() in EXCLUDED_BOM_INCLUDED_SKUS


def _create_parent_records_from_loyverse_row(
    db: Session,
    batch: ImportBatch,
    row: list[str],
    product: Product,
    columns: LoyverseColumnMap,
) -> ImportedBomHeader:
    sku = product.sku
    name = product.name
    imported_category_name = _clean_optional_text(_cell(row, columns.category))
    imported_b2c_price = parse_decimal_strict_green_corner(_cell(row, columns.b2c_price))

    header = ImportedBomHeader(
        import_batch=batch,
        product_sku=sku,
        product_name=name,
        category_name_snapshot=imported_category_name,
        b2b_price_snapshot=_valid_import_b2c_price(imported_b2c_price),
        standard_cost=product.standard_cost,
        use_production=True,
    )
    db.add(header)
    db.flush()
    return header


def _upsert_product_master_from_loyverse_row(
    db: Session,
    row: list[str],
    columns: LoyverseColumnMap,
) -> Product | None:
    if not _is_valid_loyverse_product_row(row, columns):
        return None

    sku = _cell(row, columns.parent_sku)
    name = _cell(row, columns.parent_name) or sku
    standard_cost = parse_decimal_strict_green_corner(_cell(row, columns.average_cost))
    imported_category_name = _clean_optional_text(_cell(row, columns.category))
    imported_b2c_price = parse_decimal_strict_green_corner(_cell(row, columns.b2c_price))

    product = db.query(Product).filter(Product.sku == sku).one_or_none()
    if product is None:
        product = Product(sku=sku, name=name)
        db.add(product)

    product.name = name
    if standard_cost is not None:
        product.standard_cost = standard_cost
    product.is_manufactured = _is_manufactured_parent_row(row, columns)
    _apply_category_enrichment(db, product, imported_category_name)
    _apply_b2c_price_enrichment(product, imported_b2c_price)
    _apply_planning_snapshot_fields(product, row, columns)
    return product


def _apply_planning_snapshot_fields(
    product: Product,
    row: list[str],
    columns: LoyverseColumnMap,
) -> None:
    available_for_sale = parse_available_for_sale(_cell(row, columns.available_for_sale_gc))
    product.available_for_sale_gc = available_for_sale
    product.active = available_for_sale
    product.supplier = _cell(row, columns.supplier) or None
    product.current_inventory_qty = parse_decimal_strict_green_corner(_cell(row, columns.inventory))
    if not getattr(product, "planning_zones_manual_override", False):
        product.low_stock_qty = parse_decimal_strict_green_corner(_cell(row, columns.low_stock))
        product.optimal_stock_qty = parse_decimal_strict_green_corner(_cell(row, columns.optimal_stock))


def _apply_category_enrichment(db: Session, product: Product, category_name: str | None) -> None:
    if not category_name:
        return

    category = (
        db.query(ProductCategory)
        .filter(func.lower(ProductCategory.name) == category_name.lower())
        .one_or_none()
    )
    if category is None:
        category = ProductCategory(
            name=category_name,
            active=True,
        )
        db.add(category)
        db.flush()
    product.category_id = category.id


def _apply_b2c_price_enrichment(product: Product, imported_b2c_price: Decimal | None) -> None:
    valid_b2c_price = _valid_import_b2c_price(imported_b2c_price)
    if valid_b2c_price is None:
        return
    product.b2c_price = valid_b2c_price


def _valid_import_b2c_price(imported_b2c_price: Decimal | None) -> Decimal | None:
    if imported_b2c_price is None or imported_b2c_price < 0:
        return None
    return imported_b2c_price


def _clean_optional_text(value: str | None) -> str | None:
    cleaned = (value or "").strip()
    return cleaned or None

def _extract_component_from_loyverse_row(
    row: list[str],
    columns: LoyverseColumnMap,
) -> dict[str, str | Decimal | None]:
    component_sku = _cell(row, columns.bom_included_sku)
    quantity = parse_decimal_strict_green_corner(_cell(row, columns.bom_quantity))
    return {
        "sku": component_sku or None,
        "name": None,
        "quantity": quantity,
        "cost": None,
    }


def _decode_csv_content(content: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    return content.decode("utf-8", errors="replace")


def _value(row: dict[str, str], keys: tuple[str, ...]) -> str:
    for key in keys:
        value = row.get(normalize_key(key), "").strip()
        if value:
            return value
    return ""


def _is_confident_parent_row(row: dict[str, str]) -> bool:
    parent_sku = _value(row, PARENT_SKU_KEYS)
    parent_name = _value(row, PARENT_NAME_KEYS)
    component_sku = _value(row, COMPONENT_SKU_KEYS)
    component_name = _value(row, COMPONENT_NAME_KEYS)

    return bool(parent_sku and parent_name and not component_sku and not component_name)


def _create_parent_records(
    db: Session,
    batch: ImportBatch,
    row: dict[str, str],
) -> ImportedBomHeader:
    sku = _value(row, PARENT_SKU_KEYS)
    name = _value(row, PARENT_NAME_KEYS)
    standard_cost = parse_decimal_strict_green_corner(_value(row, PARENT_COST_KEYS))

    product = db.query(Product).filter(Product.sku == sku).one_or_none()
    if product is None:
        product = Product(sku=sku, name=name)
        db.add(product)

    product.name = name
    product.unit = _value(row, PARENT_UNIT_KEYS) or product.unit
    product.standard_cost = standard_cost
    product.loyverse_handle = _value(row, PARENT_HANDLE_KEYS) or product.loyverse_handle
    product.active = True

    header = ImportedBomHeader(
        import_batch=batch,
        product_sku=sku,
        product_name=name,
        standard_cost=standard_cost,
        use_production=parse_bool(_value(row, USE_PRODUCTION_KEYS)),
    )
    db.add(header)
    db.flush()
    return header


def _extract_component(row: dict[str, str]) -> dict[str, str | Decimal | None]:
    return {
        "sku": _value(row, COMPONENT_SKU_KEYS) or None,
        "name": _value(row, COMPONENT_NAME_KEYS) or None,
        "quantity": parse_decimal_strict_green_corner(_value(row, COMPONENT_QUANTITY_KEYS)),
        "cost": parse_decimal_strict_green_corner(_value(row, COMPONENT_COST_KEYS)),
    }


def _has_component_signal(component: dict[str, str | Decimal | None]) -> bool:
    return any(value is not None and value != "" for value in component.values())
