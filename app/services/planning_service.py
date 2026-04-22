from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation

from sqlalchemy.orm import Session, joinedload

from app.models import (
    B2BSalesOrder,
    B2BSalesOrderLine,
    ImportBatch,
    ImportedBomHeader,
    Product,
    Route,
)


PRODUCT_TYPE_MANUFACTURED = "manufactured"
PRODUCT_TYPE_PURCHASED = "purchased"
VALID_PRODUCT_TYPES = {PRODUCT_TYPE_MANUFACTURED, PRODUCT_TYPE_PURCHASED}
PLANNING_STATUSES = {"Red", "Yellow", "Green", "Incomplete"}
OPEN_B2B_STATUSES = {"draft", "in_process"}
ZERO = Decimal("0")


class PlanningValidationError(Exception):
    pass


@dataclass(frozen=True)
class PlanningProductRow:
    product: Product
    green_zone: Decimal | None
    status: str
    suggested_quantity: Decimal | None
    customer_order_requirement: Decimal
    inventory_available: Decimal | None


@dataclass(frozen=True)
class CustomerRequirementRow:
    sku: str
    description: str
    product_type: str
    inventory: Decimal | None
    direct_requirement: Decimal
    exploded_requirement: Decimal
    total_requirement: Decimal


@dataclass
class CustomerRequirementResult:
    rows: list[CustomerRequirementRow]
    totals_by_sku: dict[str, Decimal]
    warnings: list[str] = field(default_factory=list)
    omitted_invalid_sku_count: int = 0
    omitted_invalid_quantity_count: int = 0

    @property
    def has_warnings(self) -> bool:
        return bool(self.warnings or self.omitted_invalid_sku_count or self.omitted_invalid_quantity_count)


def normalize_product_type(product_type: str | None) -> str:
    value = (product_type or "manufactured").strip().lower()
    if value not in VALID_PRODUCT_TYPES:
        return PRODUCT_TYPE_MANUFACTURED
    return value


def list_inventory_parameter_products(
    db: Session,
    product_type: str,
    sku: str = "",
    route_id: str = "",
    supplier: str = "",
) -> list[Product]:
    normalized_type = normalize_product_type(product_type)
    query = _base_planning_query(db, normalized_type)
    search = (sku or "").strip()
    if search:
        query = query.filter(Product.sku.ilike(f"%{search}%"))

    if normalized_type == PRODUCT_TYPE_MANUFACTURED:
        route_filter = (route_id or "").strip()
        if route_filter and route_filter.isdigit():
            query = query.filter(Product.default_route_id == int(route_filter))
    else:
        supplier_filter = (supplier or "").strip()
        if supplier_filter:
            query = query.filter(Product.supplier == supplier_filter)

    return query.order_by(Product.sku).all()


def build_planning_rows(
    db: Session,
    product_type: str,
    sku: str = "",
    route_id: str = "",
    supplier: str = "",
    needs_action: bool = False,
    status: str = "",
    requirement_result: CustomerRequirementResult | None = None,
) -> list[PlanningProductRow]:
    if requirement_result is None:
        requirement_result = build_customer_order_requirements(db)

    query = _base_planning_query(db, product_type)
    search = (sku or "").strip()
    if search:
        query = query.filter(Product.sku.ilike(f"%{search}%"))

    if product_type == PRODUCT_TYPE_MANUFACTURED:
        route_filter = (route_id or "").strip()
        if route_filter and route_filter.isdigit():
            query = query.filter(Product.default_route_id == int(route_filter))
    else:
        supplier_filter = (supplier or "").strip()
        if supplier_filter:
            query = query.filter(Product.supplier == supplier_filter)

    rows = [
        _build_row(product, requirement_result.totals_by_sku.get(product.sku, ZERO))
        for product in query.order_by(Product.sku).all()
    ]
    status_filter = (status or "").strip()
    if status_filter in PLANNING_STATUSES:
        rows = [row for row in rows if row.status == status_filter]
    if needs_action:
        rows = [row for row in rows if row.status in {"Red", "Yellow"}]
    return rows


def build_customer_order_requirements(db: Session) -> CustomerRequirementResult:
    direct: dict[str, Decimal] = {}
    exploded: dict[str, Decimal] = {}
    descriptions: dict[str, str] = {}
    warnings: list[str] = []
    omitted_invalid_sku_count = 0
    omitted_invalid_quantity_count = 0

    lines = (
        db.query(B2BSalesOrderLine)
        .join(B2BSalesOrder)
        .filter(B2BSalesOrder.status.in_(OPEN_B2B_STATUSES))
        .order_by(B2BSalesOrder.order_number, B2BSalesOrderLine.line_number)
        .all()
    )
    for line in lines:
        sku = _clean_sku(line.sku_snapshot)
        if not sku:
            omitted_invalid_sku_count += 1
            continue
        quantity = line.quantity
        if quantity is None or quantity <= ZERO:
            omitted_invalid_quantity_count += 1
            continue
        direct[sku] = direct.get(sku, ZERO) + quantity
        descriptions.setdefault(sku, line.description_snapshot or sku)

    products_by_sku = {product.sku: product for product in db.query(Product).all()}
    bom_lookup = _latest_bom_lookup(db)
    for sku, quantity in direct.items():
        _explode_sku(
            sku,
            quantity,
            level=1,
            chain=[sku],
            exploded=exploded,
            descriptions=descriptions,
            bom_lookup=bom_lookup,
            products_by_sku=products_by_sku,
            warnings=warnings,
        )

    all_skus = sorted(set(direct) | set(exploded))
    rows = []
    totals_by_sku = {}
    for sku in all_skus:
        direct_qty = direct.get(sku, ZERO)
        exploded_qty = exploded.get(sku, ZERO)
        total = direct_qty + exploded_qty
        totals_by_sku[sku] = total
        product = products_by_sku.get(sku)
        rows.append(
            CustomerRequirementRow(
                sku=sku,
                description=product.name if product else descriptions.get(sku, sku),
                product_type=_product_type_label(product),
                inventory=product.current_inventory_qty if product else None,
                direct_requirement=direct_qty,
                exploded_requirement=exploded_qty,
                total_requirement=total,
            )
        )

    if omitted_invalid_sku_count:
        warnings.insert(0, f"{omitted_invalid_sku_count} open B2B order line(s) omitted because SKU was missing or invalid.")
    if omitted_invalid_quantity_count:
        warnings.insert(0, f"{omitted_invalid_quantity_count} open B2B order line(s) omitted because quantity was missing, invalid, or not positive.")

    return CustomerRequirementResult(
        rows=rows,
        totals_by_sku=totals_by_sku,
        warnings=_unique_warnings(warnings),
        omitted_invalid_sku_count=omitted_invalid_sku_count,
        omitted_invalid_quantity_count=omitted_invalid_quantity_count,
    )


def update_product_moqs(db: Session, moq_inputs: dict[int, str]) -> None:
    for product_id, raw_value in moq_inputs.items():
        product = db.query(Product).filter(Product.id == product_id).one_or_none()
        if product is None:
            continue
        product.planning_moq = parse_moq(raw_value)
    db.commit()


def parse_moq(value: str | None) -> Decimal | None:
    text = (value or "").strip()
    if not text:
        return None
    text = text.replace(" ", "")
    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        text = text.replace(",", ".")
    try:
        value_decimal = Decimal(text)
    except InvalidOperation as exc:
        raise PlanningValidationError(f"Invalid MOQ value: {value}.") from exc
    if value_decimal < ZERO:
        raise PlanningValidationError("MOQ must be greater than or equal to 0.")
    return value_decimal


def list_routes_for_filter(db: Session) -> list[Route]:
    return db.query(Route).filter(Route.active.is_(True)).order_by(Route.code).all()


def list_suppliers_for_filter(db: Session) -> list[str]:
    rows = (
        db.query(Product.supplier)
        .filter(
            Product.is_manufactured.is_(False),
            Product.available_for_sale_gc.is_(True),
            Product.supplier.is_not(None),
            Product.supplier != "",
        )
        .distinct()
        .order_by(Product.supplier)
        .all()
    )
    return [row[0] for row in rows if row[0]]


def _base_planning_query(db: Session, product_type: str):
    normalized_type = normalize_product_type(product_type)
    is_manufactured = normalized_type == PRODUCT_TYPE_MANUFACTURED
    return (
        db.query(Product)
        .options(joinedload(Product.default_route))
        .filter(
            Product.is_manufactured.is_(is_manufactured),
            Product.available_for_sale_gc.is_(True),
        )
    )


def _build_row(product: Product, customer_order_requirement: Decimal) -> PlanningProductRow:
    green_zone = _green_zone(product)
    inventory_available = _inventory_available(product, customer_order_requirement)
    status = _status(product, green_zone, inventory_available)
    suggested_quantity = product.planning_moq if status in {"Red", "Yellow"} else ZERO
    if status == "Incomplete":
        suggested_quantity = None
    return PlanningProductRow(
        product=product,
        green_zone=green_zone,
        status=status,
        suggested_quantity=suggested_quantity,
        customer_order_requirement=customer_order_requirement,
        inventory_available=inventory_available,
    )


def _green_zone(product: Product) -> Decimal | None:
    if product.low_stock_qty is None or product.planning_moq is None:
        return None
    return product.low_stock_qty + product.planning_moq


def _inventory_available(product: Product, customer_order_requirement: Decimal) -> Decimal | None:
    if product.current_inventory_qty is None:
        return None
    return product.current_inventory_qty - customer_order_requirement


def _status(product: Product, green_zone: Decimal | None, inventory_available: Decimal | None) -> str:
    if (
        inventory_available is None
        or product.low_stock_qty is None
        or product.optimal_stock_qty is None
        or product.planning_moq is None
        or green_zone is None
    ):
        return "Incomplete"
    if inventory_available <= product.low_stock_qty:
        return "Red"
    if inventory_available <= product.optimal_stock_qty:
        return "Yellow"
    return "Green"


def _latest_bom_lookup(db: Session) -> dict[str, list[ImportedBomHeader]]:
    latest_batch = db.query(ImportBatch).order_by(ImportBatch.id.desc()).first()
    if latest_batch is None:
        return {}
    headers = (
        db.query(ImportedBomHeader)
        .options(joinedload(ImportedBomHeader.bom_lines))
        .filter(ImportedBomHeader.import_batch_id == latest_batch.id)
        .all()
    )
    lookup: dict[str, list[ImportedBomHeader]] = {}
    for header in headers:
        sku = _clean_sku(header.product_sku)
        if not sku:
            continue
        lookup.setdefault(sku, []).append(header)
    return lookup


def _explode_sku(
    sku: str,
    quantity: Decimal,
    level: int,
    chain: list[str],
    exploded: dict[str, Decimal],
    descriptions: dict[str, str],
    bom_lookup: dict[str, list[ImportedBomHeader]],
    products_by_sku: dict[str, Product],
    warnings: list[str],
) -> None:
    if level > 2:
        return
    product = products_by_sku.get(sku)
    if product is None:
        return
    if not product.is_manufactured:
        return

    headers = bom_lookup.get(sku, [])
    if not headers:
        if product is not None and product.is_manufactured:
            warnings.append(f"BOM missing for SKU {sku} in latest import.")
        return
    if len(headers) > 1:
        warnings.append(f"Ambiguous BOM for SKU {sku} in latest import.")
        return

    header = headers[0]
    for line in header.bom_lines:
        component_sku = _clean_sku(line.component_sku)
        if not component_sku:
            warnings.append(f"BOM line skipped for parent SKU {sku} because component SKU is missing.")
            continue
        if component_sku in chain:
            warnings.append(f"Cycle or self-reference skipped: {sku} -> {component_sku}.")
            continue
        if line.quantity is None:
            warnings.append(f"BOM line skipped for parent SKU {sku} and component SKU {component_sku} because quantity is missing.")
            continue

        component_requirement = quantity * line.quantity
        exploded[component_sku] = exploded.get(component_sku, ZERO) + component_requirement
        descriptions.setdefault(component_sku, line.component_name or component_sku)
        _explode_sku(
            component_sku,
            component_requirement,
            level=level + 1,
            chain=[*chain, component_sku],
            exploded=exploded,
            descriptions=descriptions,
            bom_lookup=bom_lookup,
            products_by_sku=products_by_sku,
            warnings=warnings,
        )


def _clean_sku(value: str | None) -> str:
    return (value or "").strip()


def _product_type_label(product: Product | None) -> str:
    if product is None:
        return "Unknown"
    return "Manufactured" if product.is_manufactured else "Purchased"


def _unique_warnings(warnings: list[str]) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for warning in warnings:
        if warning in seen:
            continue
        seen.add(warning)
        unique.append(warning)
    return unique
