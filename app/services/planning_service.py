from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation

from sqlalchemy import func
from sqlalchemy.orm import Session, joinedload

from app.models import (
    B2BSalesOrder,
    B2BSalesOrderLine,
    ImportBatch,
    ImportedBomHeader,
    InventoryBalance,
    Product,
    Route,
)


PRODUCT_TYPE_MANUFACTURED = "manufactured"
PRODUCT_TYPE_PURCHASED = "purchased"
VALID_PRODUCT_TYPES = {PRODUCT_TYPE_MANUFACTURED, PRODUCT_TYPE_PURCHASED}
PLANNING_STATUSES = {"Red", "Yellow", "Green", "Incomplete"}
OPEN_B2B_STATUSES = {"draft", "in_process"}
ZERO = Decimal("0")
TWO = Decimal("2")


class PlanningValidationError(Exception):
    pass


@dataclass(frozen=True)
class PlanningProductRow:
    product: Product
    inventory_on_hand: Decimal
    green_zone: Decimal | None
    status: str
    suggested_quantity: Decimal | None
    customer_order_requirement: Decimal
    mrp_requirement: Decimal
    inventory_available: Decimal | None


@dataclass(frozen=True)
class CustomerRequirementRow:
    sku: str
    description: str
    product_type: str
    inventory: Decimal | None
    direct_requirement: Decimal


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


@dataclass(frozen=True)
class MpsProductRow:
    product: Product


@dataclass(frozen=True)
class MpsRouteGroup:
    route_label: str
    route_sort_key: str
    rows: list[MpsProductRow]
    has_route_warning: bool = False


@dataclass(frozen=True)
class MrpRequirementRow:
    sku: str
    description: str
    product_type: str
    level_1_requirement: Decimal
    level_2_requirement: Decimal
    total_requirement: Decimal


@dataclass
class MrpResult:
    rows: list[MrpRequirementRow]
    totals_by_sku: dict[str, Decimal]
    warnings: list[str] = field(default_factory=list)

    @property
    def has_warnings(self) -> bool:
        return bool(self.warnings)


@dataclass(frozen=True)
class _BomLineRequirement:
    component_sku: str
    component_name: str
    quantity: Decimal
    chain: list[str]


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
    mrp_result: MrpResult | None = None,
) -> list[PlanningProductRow]:
    normalized_type = normalize_product_type(product_type)
    if requirement_result is None:
        requirement_result = build_customer_order_requirements(db)
    if mrp_result is None:
        mrp_result = build_mrp_result(db)

    query = _base_planning_query(db, normalized_type)
    search = (sku or "").strip()
    if search:
        query = query.filter(Product.sku.ilike(f"%{search}%"))

    if normalized_type == PRODUCT_TYPE_MANUFACTURED:
        route_filter = (route_id or "").strip()
        if route_filter and route_filter.isdigit():
            query = query.filter(Product.default_route_id == int(route_filter))
    else:
        query = query.filter(Product.supplier.is_not(None), func.trim(Product.supplier) != "")
        supplier_filter = (supplier or "").strip()
        if supplier_filter:
            query = query.filter(Product.supplier == supplier_filter)

    products = query.order_by(Product.sku).all()
    inventory_by_product_id = _inventory_qty_by_product_id(db, [product.id for product in products])

    rows = [
        _build_row(
            product,
            inventory_by_product_id.get(product.id, ZERO),
            requirement_result.totals_by_sku.get(product.sku, ZERO),
            mrp_result.totals_by_sku.get(product.sku, ZERO) if normalized_type == PRODUCT_TYPE_PURCHASED else ZERO,
        )
        for product in products
    ]
    status_filter = (status or "").strip()
    if status_filter in PLANNING_STATUSES:
        rows = [row for row in rows if row.status == status_filter]
    if needs_action:
        rows = [row for row in rows if row.status in {"Red", "Yellow"}]
    return rows


def build_customer_order_requirements(db: Session) -> CustomerRequirementResult:
    direct: dict[str, Decimal] = {}
    descriptions: dict[str, str] = {}
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
    inventory_by_product_id = _inventory_qty_by_product_id(
        db,
        [product.id for sku, product in products_by_sku.items() if sku in direct],
    )
    rows = []
    for sku in sorted(direct):
        product = products_by_sku.get(sku)
        rows.append(
            CustomerRequirementRow(
                sku=sku,
                description=product.name if product else descriptions.get(sku, sku),
                product_type=_product_type_label(product),
                inventory=inventory_by_product_id.get(product.id, ZERO) if product else ZERO,
                direct_requirement=direct[sku],
            )
        )

    warnings: list[str] = []
    if omitted_invalid_sku_count:
        warnings.append(f"{omitted_invalid_sku_count} open B2B order line(s) omitted because SKU was missing or invalid.")
    if omitted_invalid_quantity_count:
        warnings.append(f"{omitted_invalid_quantity_count} open B2B order line(s) omitted because quantity was missing, invalid, or not positive.")

    return CustomerRequirementResult(
        rows=rows,
        totals_by_sku=direct,
        warnings=warnings,
        omitted_invalid_sku_count=omitted_invalid_sku_count,
        omitted_invalid_quantity_count=omitted_invalid_quantity_count,
    )


def build_mps_groups(db: Session, sku: str = "", route_id: str = "") -> list[MpsRouteGroup]:
    query = _base_planning_query(db, PRODUCT_TYPE_MANUFACTURED).filter(
        Product.planning_quantity.is_not(None),
        Product.planning_quantity > ZERO,
    )
    search = (sku or "").strip()
    if search:
        query = query.filter(Product.sku.ilike(f"%{search}%"))
    route_filter = (route_id or "").strip()
    if route_filter and route_filter.isdigit():
        query = query.filter(Product.default_route_id == int(route_filter))

    grouped: dict[str, list[MpsProductRow]] = {}
    sort_keys: dict[str, str] = {}
    warnings: dict[str, bool] = {}
    for product in query.order_by(Product.sku).all():
        if product.default_route:
            label = f"{product.default_route.code} - {product.default_route.name}"
            sort_key = product.default_route.code or label
            has_warning = False
        else:
            label = "No route assigned"
            sort_key = "ZZZ_NO_ROUTE"
            has_warning = True
        grouped.setdefault(label, []).append(MpsProductRow(product=product))
        sort_keys[label] = sort_key
        warnings[label] = warnings.get(label, False) or has_warning

    return [
        MpsRouteGroup(
            route_label=label,
            route_sort_key=sort_keys[label],
            rows=grouped[label],
            has_route_warning=warnings[label],
        )
        for label in sorted(grouped, key=lambda item: sort_keys[item])
    ]


def build_mrp_result(db: Session) -> MrpResult:
    warnings: list[str] = []
    descriptions: dict[str, str] = {}
    products_by_sku = {product.sku: product for product in db.query(Product).all()}
    bom_lookup = _latest_bom_lookup(db)

    planned_products = (
        _base_planning_query(db, PRODUCT_TYPE_MANUFACTURED)
        .filter(Product.planning_quantity.is_not(None), Product.planning_quantity > ZERO)
        .order_by(Product.sku)
        .all()
    )
    component_skus = _component_skus_in_bom(bom_lookup)

    level_1: dict[str, Decimal] = {}
    level_2: dict[str, Decimal] = {}
    level_1_chains: dict[str, list[list[str]]] = {}

    for product in planned_products:
        planned_quantity = product.planning_quantity or ZERO
        if product.sku in component_skus:
            level_1[product.sku] = level_1.get(product.sku, ZERO) + planned_quantity
            level_1_chains.setdefault(product.sku, []).append([product.sku])
            descriptions.setdefault(product.sku, product.name)
            continue
        for requirement in _explode_one_level(
            product.sku,
            planned_quantity,
            [product.sku],
            bom_lookup,
            warnings,
        ):
            level_1[requirement.component_sku] = level_1.get(requirement.component_sku, ZERO) + requirement.quantity
            level_1_chains.setdefault(requirement.component_sku, []).append(requirement.chain)
            descriptions.setdefault(requirement.component_sku, requirement.component_name)

    for sku in sorted(level_1):
        product = products_by_sku.get(sku)
        if product is None or not product.is_manufactured:
            continue
        chains = level_1_chains.get(sku, [[sku]])
        for requirement in _explode_one_level(sku, level_1[sku], [sku], bom_lookup, warnings):
            if any(requirement.component_sku in chain for chain in chains):
                warnings.append(f"Cycle or self-reference skipped: {sku} -> {requirement.component_sku}.")
                continue
            level_2[requirement.component_sku] = level_2.get(requirement.component_sku, ZERO) + requirement.quantity
            descriptions.setdefault(requirement.component_sku, requirement.component_name)

    all_skus = sorted(set(level_1) | set(level_2))
    rows: list[MrpRequirementRow] = []
    totals_by_sku: dict[str, Decimal] = {}
    for sku in all_skus:
        level_1_qty = level_1.get(sku, ZERO)
        level_2_qty = level_2.get(sku, ZERO)
        total = level_1_qty + level_2_qty
        totals_by_sku[sku] = total
        product = products_by_sku.get(sku)
        rows.append(
            MrpRequirementRow(
                sku=sku,
                description=product.name if product else descriptions.get(sku, sku),
                product_type=_product_type_label(product),
                level_1_requirement=level_1_qty,
                level_2_requirement=level_2_qty,
                total_requirement=total,
            )
        )

    return MrpResult(rows=rows, totals_by_sku=totals_by_sku, warnings=_unique_warnings(warnings))


def update_product_moqs(db: Session, moq_inputs: dict[int, str]) -> None:
    for product_id, raw_value in moq_inputs.items():
        product = db.query(Product).filter(Product.id == product_id).one_or_none()
        if product is None:
            continue
        product.planning_moq = parse_moq(raw_value)
    db.commit()


def update_product_planner_quantities(db: Session, quantity_inputs: dict[int, str]) -> None:
    for product_id, raw_value in quantity_inputs.items():
        product = db.query(Product).filter(Product.id == product_id).one_or_none()
        if product is None:
            continue
        product.planning_quantity = parse_planner_quantity(raw_value)
    db.commit()


def clear_planner_quantities(db: Session) -> None:
    db.query(Product).filter(Product.planning_quantity.is_not(None)).update(
        {Product.planning_quantity: None}, synchronize_session=False
    )
    db.commit()


def parse_moq(value: str | None) -> Decimal | None:
    text = (value or "").strip()
    if not text:
        return None
    value_decimal = _parse_decimal_text(text, "MOQ")
    if value_decimal < ZERO:
        raise PlanningValidationError("MOQ must be greater than or equal to 0.")
    return value_decimal


def parse_planner_quantity(value: str | None) -> Decimal | None:
    text = (value or "").strip()
    if not text:
        return None
    value_decimal = _parse_decimal_text(text, "Planner quantity")
    if value_decimal < ZERO:
        raise PlanningValidationError("Planner quantity must be greater than or equal to 0.")
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
            func.trim(Product.supplier) != "",
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


def _build_row(
    product: Product,
    inventory_on_hand: Decimal,
    customer_order_requirement: Decimal,
    mrp_requirement: Decimal,
) -> PlanningProductRow:
    green_zone = _green_zone(product)
    inventory_available = _inventory_available(inventory_on_hand, customer_order_requirement, mrp_requirement)
    status = _status(product, green_zone, inventory_available)
    suggested_quantity = _suggested_quantity(
        product,
        inventory_on_hand,
        green_zone,
        status,
        customer_order_requirement,
        mrp_requirement,
    )
    return PlanningProductRow(
        product=product,
        inventory_on_hand=inventory_on_hand,
        green_zone=green_zone,
        status=status,
        suggested_quantity=suggested_quantity,
        customer_order_requirement=customer_order_requirement,
        mrp_requirement=mrp_requirement,
        inventory_available=inventory_available,
    )


def _green_zone(product: Product) -> Decimal | None:
    if product.low_stock_qty is None or product.planning_moq is None:
        return None
    return product.low_stock_qty + product.planning_moq


def _inventory_available(
    inventory_on_hand: Decimal,
    customer_order_requirement: Decimal,
    mrp_requirement: Decimal,
) -> Decimal:
    return inventory_on_hand - customer_order_requirement - mrp_requirement


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


def _suggested_quantity(
    product: Product,
    inventory_on_hand: Decimal,
    green_zone: Decimal | None,
    status: str,
    customer_order_requirement: Decimal,
    mrp_requirement: Decimal,
) -> Decimal | None:
    if status == "Incomplete":
        return None
    total_requirement = customer_order_requirement + mrp_requirement
    if total_requirement > ZERO:
        if product.optimal_stock_qty is None or green_zone is None:
            return None
        effective_inventory = inventory_on_hand
        if customer_order_requirement > ZERO and effective_inventory < ZERO:
            effective_inventory = ZERO
        suggested = ((green_zone - product.optimal_stock_qty) / TWO) + product.optimal_stock_qty + total_requirement - effective_inventory
        return suggested if suggested > ZERO else ZERO
    return product.planning_moq if status in {"Red", "Yellow"} else ZERO


def _inventory_qty_by_product_id(db: Session, product_ids: list[int]) -> dict[int, Decimal]:
    unique_ids = sorted({product_id for product_id in product_ids if product_id is not None})
    if not unique_ids:
        return {}

    return {
        balance.product_id: balance.on_hand_qty if balance.on_hand_qty is not None else ZERO
        for balance in db.query(InventoryBalance).filter(InventoryBalance.product_id.in_(unique_ids)).all()
    }


def _component_skus_in_bom(bom_lookup: dict[str, list[ImportedBomHeader]]) -> set[str]:
    component_skus: set[str] = set()
    for headers in bom_lookup.values():
        for header in headers:
            for line in header.bom_lines:
                component_sku = _clean_sku(line.component_sku)
                if component_sku:
                    component_skus.add(component_sku)
    return component_skus

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


def _explode_one_level(
    sku: str,
    quantity: Decimal,
    chain: list[str],
    bom_lookup: dict[str, list[ImportedBomHeader]],
    warnings: list[str],
) -> list[_BomLineRequirement]:
    headers = bom_lookup.get(sku, [])
    if not headers:
        warnings.append(f"BOM missing for SKU {sku} in latest import.")
        return []
    if len(headers) > 1:
        warnings.append(f"Ambiguous BOM for SKU {sku} in latest import.")
        return []

    requirements: list[_BomLineRequirement] = []
    for line in headers[0].bom_lines:
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
        requirements.append(
            _BomLineRequirement(
                component_sku=component_sku,
                component_name=line.component_name or component_sku,
                quantity=quantity * line.quantity,
                chain=[*chain, component_sku],
            )
        )
    return requirements


def _parse_decimal_text(text: str, field_name: str) -> Decimal:
    normalized = text.replace(" ", "")
    if "," in normalized and "." in normalized:
        if normalized.rfind(",") > normalized.rfind("."):
            normalized = normalized.replace(".", "").replace(",", ".")
        else:
            normalized = normalized.replace(",", "")
    elif "," in normalized:
        normalized = normalized.replace(",", ".")
    try:
        return Decimal(normalized)
    except InvalidOperation as exc:
        raise PlanningValidationError(f"Invalid {field_name} value: {text}.") from exc


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
