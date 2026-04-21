from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from sqlalchemy.orm import Session, joinedload

from app.models import Product, Route


PRODUCT_TYPE_MANUFACTURED = "manufactured"
PRODUCT_TYPE_PURCHASED = "purchased"
VALID_PRODUCT_TYPES = {PRODUCT_TYPE_MANUFACTURED, PRODUCT_TYPE_PURCHASED}
PLANNING_STATUSES = {"Red", "Yellow", "Green", "Incomplete"}
ZERO = Decimal("0")


class PlanningValidationError(Exception):
    pass


@dataclass(frozen=True)
class PlanningProductRow:
    product: Product
    green_zone: Decimal | None
    status: str
    suggested_quantity: Decimal | None


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
) -> list[PlanningProductRow]:
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

    rows = [_build_row(product) for product in query.order_by(Product.sku).all()]
    status_filter = (status or "").strip()
    if status_filter in PLANNING_STATUSES:
        rows = [row for row in rows if row.status == status_filter]
    if needs_action:
        rows = [row for row in rows if row.status in {"Red", "Yellow"}]
    return rows


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


def _build_row(product: Product) -> PlanningProductRow:
    green_zone = _green_zone(product)
    status = _status(product, green_zone)
    suggested_quantity = product.planning_moq if status in {"Red", "Yellow"} else ZERO
    if status == "Incomplete":
        suggested_quantity = None
    return PlanningProductRow(
        product=product,
        green_zone=green_zone,
        status=status,
        suggested_quantity=suggested_quantity,
    )


def _green_zone(product: Product) -> Decimal | None:
    if product.low_stock_qty is None or product.planning_moq is None:
        return None
    return product.low_stock_qty + product.planning_moq


def _status(product: Product, green_zone: Decimal | None) -> str:
    if (
        product.current_inventory_qty is None
        or product.low_stock_qty is None
        or product.optimal_stock_qty is None
        or product.planning_moq is None
        or green_zone is None
    ):
        return "Incomplete"
    if product.current_inventory_qty <= product.low_stock_qty:
        return "Red"
    if product.current_inventory_qty <= product.optimal_stock_qty:
        return "Yellow"
    return "Green"