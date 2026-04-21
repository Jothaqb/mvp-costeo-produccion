import csv
import io
import re
import unicodedata
from collections import Counter
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from sqlalchemy.orm import Session

from app.models import ImportBatch, ImportedBomHeader, ImportedBomLine, Product
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
LOYVERSE_AVERAGE_COST_INDEX = 12
LOYVERSE_SUPPLIER_INDEX = 18
LOYVERSE_AVAILABLE_FOR_SALE_GC_INDEX = 20
LOYVERSE_INVENTORY_INDEX = 22
LOYVERSE_LOW_STOCK_INDEX = 23
LOYVERSE_OPTIMAL_STOCK_INDEX = 24
LOYVERSE_BOM_INCLUDED_SKU_INDEX = 14
LOYVERSE_BOM_QUANTITY_INDEX = 15
LOYVERSE_USE_PRODUCTION_INDEX = 17
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


@dataclass(frozen=True)
class ImportSummary:
    batch_id: int
    product_count: int
    bom_line_count: int
    component_type_counts: dict[str, int]
    unknown_count: int


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

    text = text.replace(" ", "")
    text = re.sub(r"[^\d,.\-]", "", text)
    if not text or text in {"-", ".", ","}:
        return None

    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        text = text.replace(",", ".")

    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def parse_bool(value: str | None) -> bool:
    text = normalize_text(value)
    return text in {"1", "true", "yes", "y", "si", "sí", "x"}


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


def import_loyverse_csv(db: Session, file_name: str, content: bytes) -> ImportSummary:
    text = _decode_csv_content(content)
    csv_rows = list(csv.reader(io.StringIO(text)))

    batch = ImportBatch(file_name=file_name)
    db.add(batch)
    db.flush()

    current_header: ImportedBomHeader | None = None
    imported_products: set[str] = set()
    component_type_counts: Counter[str] = Counter()
    bom_line_count = 0

    for row_number, row in enumerate(csv_rows, start=1):
        if row_number == 1 and _looks_like_header_row(row):
            continue

        if _is_loyverse_parent_row(row):
            if _is_manufactured_parent_row(row):
                current_header = _create_parent_records_from_loyverse_row(db, batch, row)
                imported_products.add(current_header.product_sku)
            else:
                _upsert_non_manufactured_product_from_loyverse_row(db, row)
                current_header = None

        component = _extract_component_from_loyverse_row(row)
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

    db.commit()
    return ImportSummary(
        batch_id=batch.id,
        product_count=len(imported_products),
        bom_line_count=bom_line_count,
        component_type_counts=dict(component_type_counts),
        unknown_count=component_type_counts[ComponentType.UNKNOWN.value],
    )


def _normalize_row(row: dict[str, str]) -> dict[str, str]:
    return {normalize_key(key): (value or "").strip() for key, value in row.items()}


def _cell(row: list[str], index: int) -> str:
    if index >= len(row):
        return ""
    return (row[index] or "").strip()


def _looks_like_header_row(row: list[str]) -> bool:
    parent_sku = normalize_text(_cell(row, LOYVERSE_PARENT_SKU_INDEX))
    parent_name = normalize_text(_cell(row, LOYVERSE_PARENT_NAME_INDEX))
    component_sku = normalize_text(_cell(row, LOYVERSE_BOM_INCLUDED_SKU_INDEX))
    return "sku" in parent_sku or "name" in parent_name or "included" in component_sku


def _is_loyverse_parent_row(row: list[str]) -> bool:
    return bool(_cell(row, LOYVERSE_PARENT_SKU_INDEX))


def _is_manufactured_parent_row(row: list[str]) -> bool:
    return normalize_text(_cell(row, LOYVERSE_USE_PRODUCTION_INDEX)) == "y"


def _is_excluded_bom_sku(component_sku: str | Decimal | None) -> bool:
    if component_sku is None:
        return False
    return str(component_sku).strip() in EXCLUDED_BOM_INCLUDED_SKUS


def _create_parent_records_from_loyverse_row(
    db: Session,
    batch: ImportBatch,
    row: list[str],
) -> ImportedBomHeader:
    sku = _cell(row, LOYVERSE_PARENT_SKU_INDEX)
    name = _cell(row, LOYVERSE_PARENT_NAME_INDEX) or sku
    standard_cost = parse_decimal(_cell(row, LOYVERSE_AVERAGE_COST_INDEX))

    product = db.query(Product).filter(Product.sku == sku).one_or_none()
    if product is None:
        product = Product(sku=sku, name=name)
        db.add(product)

    product.name = name
    product.standard_cost = standard_cost
    product.is_manufactured = True
    product.active = True
    _apply_planning_snapshot_fields(product, row)

    header = ImportedBomHeader(
        import_batch=batch,
        product_sku=sku,
        product_name=name,
        standard_cost=standard_cost,
        use_production=True,
    )
    db.add(header)
    db.flush()
    return header


def _upsert_non_manufactured_product_from_loyverse_row(db: Session, row: list[str]) -> None:
    sku = _cell(row, LOYVERSE_PARENT_SKU_INDEX)
    if not sku:
        return

    use_production = normalize_text(_cell(row, LOYVERSE_USE_PRODUCTION_INDEX))
    available_for_sale = parse_bool(_cell(row, LOYVERSE_AVAILABLE_FOR_SALE_GC_INDEX))
    is_planning_purchased = available_for_sale and use_production in {"n", ""}
    product = db.query(Product).filter(Product.sku == sku).one_or_none()
    if not is_planning_purchased:
        if product is not None and not available_for_sale:
            product.available_for_sale_gc = False
        return
    if product is None:
        product = Product(sku=sku, name=_cell(row, LOYVERSE_PARENT_NAME_INDEX) or sku)
        db.add(product)

    product.name = _cell(row, LOYVERSE_PARENT_NAME_INDEX) or sku
    product.standard_cost = parse_decimal(_cell(row, LOYVERSE_AVERAGE_COST_INDEX))
    product.is_manufactured = False
    product.active = True
    _apply_planning_snapshot_fields(product, row)


def _apply_planning_snapshot_fields(product: Product, row: list[str]) -> None:
    product.available_for_sale_gc = parse_bool(_cell(row, LOYVERSE_AVAILABLE_FOR_SALE_GC_INDEX))
    product.supplier = _cell(row, LOYVERSE_SUPPLIER_INDEX) or None
    product.current_inventory_qty = parse_decimal(_cell(row, LOYVERSE_INVENTORY_INDEX))
    product.low_stock_qty = parse_decimal(_cell(row, LOYVERSE_LOW_STOCK_INDEX))
    product.optimal_stock_qty = parse_decimal(_cell(row, LOYVERSE_OPTIMAL_STOCK_INDEX))

def _extract_component_from_loyverse_row(row: list[str]) -> dict[str, str | Decimal | None]:
    component_sku = _cell(row, LOYVERSE_BOM_INCLUDED_SKU_INDEX)
    quantity = parse_decimal(_cell(row, LOYVERSE_BOM_QUANTITY_INDEX))
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
    standard_cost = parse_decimal(_value(row, PARENT_COST_KEYS))

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
        "quantity": parse_decimal(_value(row, COMPONENT_QUANTITY_KEYS)),
        "cost": parse_decimal(_value(row, COMPONENT_COST_KEYS)),
    }


def _has_component_signal(component: dict[str, str | Decimal | None]) -> bool:
    return any(value is not None and value != "" for value in component.values())
