from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from sqlalchemy.orm import Session, joinedload

from app.models import (
    Activity,
    AppSequence,
    ImportedBomHeader,
    ImportedBomLine,
    ImportBatch,
    Product,
    ProductionOrder,
    ProductionOrderActivity,
    ProductionOrderMaterial,
    Route,
    RouteActivity,
)
from app.schemas import ProductionOrderStatus
from app.services.costing_service import CostingValidationError, calculate_order_cost


class ProductionOrderValidationError(Exception):
    pass


PRODUCTION_ORDER_SEQUENCE_NAME = "production_order"
PRODUCTION_ORDER_PREFIX = "OP"


def parse_optional_decimal(value: str | Decimal | None, field_name: str) -> Decimal | None:
    if value is None or value == "":
        return None
    return parse_required_decimal(value, field_name)


def parse_required_decimal(value: str | Decimal | None, field_name: str) -> Decimal:
    if isinstance(value, Decimal):
        parsed = value
    else:
        text = (value or "").strip().replace(" ", "").replace(",", ".")
        try:
            parsed = Decimal(text)
        except InvalidOperation as exc:
            raise ProductionOrderValidationError(f"{field_name} must be a valid number.") from exc
    return parsed


def create_production_order(
    db: Session,
    production_date: date,
    product_id: int,
    planned_qty: Decimal | None,
    notes: str | None,
) -> ProductionOrder:
    product = db.query(Product).filter(Product.id == product_id).one_or_none()
    if product is None:
        raise ProductionOrderValidationError("Product is required.")
    if not product.is_manufactured:
        raise ProductionOrderValidationError("Product must be a manufactured product.")
    if product.default_route_id is None:
        raise ProductionOrderValidationError("Product must have a default route before creating an order.")

    route = db.query(Route).filter(Route.id == product.default_route_id).one_or_none()
    if route is None or not route.active:
        raise ProductionOrderValidationError("Product default route must be active.")

    route_activities = (
        db.query(RouteActivity)
        .options(joinedload(RouteActivity.activity))
        .filter(RouteActivity.route_id == route.id)
        .order_by(RouteActivity.sequence)
        .all()
    )
    if not route_activities:
        raise ProductionOrderValidationError("Product default route must have at least one activity.")

    bom_header = _get_latest_bom_header(db, product.sku)
    if bom_header is None:
        raise ProductionOrderValidationError("Product must have an imported BOM before creating an order.")

    order_number = _generate_internal_order_number(db)

    order = ProductionOrder(
        internal_order_number=order_number,
        loyverse_order_ref=None,
        production_date=production_date,
        product_id=product.id,
        product_sku_snapshot=product.sku,
        product_name_snapshot=product.name,
        route_id=route.id,
        route_name_snapshot=route.name,
        route_version_snapshot=route.version,
        process_type=route.process_type,
        planned_qty=planned_qty,
        unit=product.unit,
        status=ProductionOrderStatus.DRAFT.value,
        notes=(notes or "").strip() or None,
    )
    db.add(order)
    db.flush()

    _copy_route_activities(db, order, route_activities)
    _copy_bom(db, order, bom_header)
    db.commit()
    db.refresh(order)
    return order


def update_activity_capture(
    db: Session,
    order_id: int,
    activity_updates: list[dict[str, str]],
) -> ProductionOrder:
    order = get_order(db, order_id)
    _ensure_not_closed(order)

    activities_by_id = {activity.id: activity for activity in order.activities}
    activity_codes = {activity.activity_code_snapshot for activity in order.activities}
    activity_catalog_by_code = {}
    if activity_codes:
        activity_catalog_by_code = {
            activity.code: activity
            for activity in db.query(Activity).filter(Activity.code.in_(activity_codes)).all()
        }

    for update in activity_updates:
        activity_id = int(update["id"])
        activity = activities_by_id.get(activity_id)
        if activity is None:
            continue

        labor_minutes = parse_optional_decimal(update.get("labor_minutes"), "Labor minutes") or Decimal("0")
        machine_minutes = parse_optional_decimal(update.get("machine_minutes"), "Machine minutes") or Decimal("0")
        activity_catalog = activity_catalog_by_code.get(activity.activity_code_snapshot)
        if activity_catalog is not None and not activity_catalog.applies_labor:
            labor_minutes = Decimal("0")
        if activity_catalog is not None and not activity_catalog.applies_machine:
            machine_minutes = Decimal("0")

        if labor_minutes < 0 or machine_minutes < 0:
            raise ProductionOrderValidationError("Activity minutes cannot be negative.")

        activity.labor_minutes = labor_minutes
        activity.machine_minutes = machine_minutes
        activity.notes = (update.get("notes") or "").strip() or None

    db.commit()
    db.refresh(order)
    return order


def update_yield_capture(
    db: Session,
    order_id: int,
    input_qty: Decimal | None,
    output_qty: Decimal | None,
) -> ProductionOrder:
    order = get_order(db, order_id)
    _ensure_not_closed(order)

    if input_qty is not None and input_qty <= 0:
        raise ProductionOrderValidationError("Input quantity must be greater than 0.")
    if output_qty is not None and output_qty < 0:
        raise ProductionOrderValidationError("Output quantity cannot be negative.")

    order.input_qty = input_qty
    order.output_qty = output_qty
    if input_qty and output_qty is not None:
        order.yield_percent = output_qty / input_qty
    else:
        order.yield_percent = None

    db.commit()
    db.refresh(order)
    return order


def start_order(db: Session, order_id: int) -> ProductionOrder:
    order = get_order(db, order_id)
    if order.status != ProductionOrderStatus.DRAFT.value:
        raise ProductionOrderValidationError("Only draft orders can be started.")

    order.status = ProductionOrderStatus.IN_PROGRESS.value
    db.commit()
    db.refresh(order)
    return order


def close_order(db: Session, order_id: int) -> ProductionOrder:
    order = get_order(db, order_id)
    if order.status != ProductionOrderStatus.IN_PROGRESS.value:
        raise ProductionOrderValidationError("Only in-progress orders can be closed.")
    if order.input_qty is None or order.input_qty <= 0:
        raise ProductionOrderValidationError("Input quantity must be greater than 0 before closing.")
    if order.output_qty is None or order.output_qty <= 0:
        raise ProductionOrderValidationError("Output quantity must be greater than 0 before closing.")

    order.yield_percent = order.output_qty / order.input_qty
    try:
        calculate_order_cost(db, order)
    except CostingValidationError as exc:
        db.rollback()
        raise ProductionOrderValidationError(str(exc)) from exc

    order.status = ProductionOrderStatus.CLOSED.value
    order.closed_at = datetime.utcnow()
    db.commit()
    db.refresh(order)
    return order


def get_order(db: Session, order_id: int) -> ProductionOrder:
    return db.query(ProductionOrder).filter(ProductionOrder.id == order_id).one()


def _generate_internal_order_number(db: Session) -> str:
    sequence = (
        db.query(AppSequence)
        .filter(AppSequence.name == PRODUCTION_ORDER_SEQUENCE_NAME)
        .one_or_none()
    )
    if sequence is None:
        sequence = AppSequence(
            name=PRODUCTION_ORDER_SEQUENCE_NAME,
            next_value=_bootstrap_next_production_order_sequence(db),
        )
        db.add(sequence)
        db.flush()

    order_number = f"{PRODUCTION_ORDER_PREFIX}{sequence.next_value}"
    existing_order = (
        db.query(ProductionOrder)
        .filter(ProductionOrder.internal_order_number == order_number)
        .one_or_none()
    )
    if existing_order is not None:
        raise ProductionOrderValidationError(f"Generated internal order number {order_number} already exists.")

    sequence.next_value += 1
    return order_number


def _bootstrap_next_production_order_sequence(db: Session) -> int:
    highest = 0
    prefix_length = len(PRODUCTION_ORDER_PREFIX)
    order_numbers = db.query(ProductionOrder.internal_order_number).all()
    for (order_number,) in order_numbers:
        if not order_number or not order_number.startswith(PRODUCTION_ORDER_PREFIX):
            continue
        suffix = order_number[prefix_length:]
        if suffix.isdigit():
            highest = max(highest, int(suffix))
    return highest + 1


def _copy_route_activities(
    db: Session,
    order: ProductionOrder,
    route_activities: list[RouteActivity],
) -> None:
    for route_activity in route_activities:
        default_machine = route_activity.activity.default_machine
        db.add(
            ProductionOrderActivity(
                production_order_id=order.id,
                sequence=route_activity.sequence,
                activity_code_snapshot=route_activity.activity.code,
                activity_name_snapshot=route_activity.activity.name,
                machine_id_snapshot=default_machine.id if default_machine else None,
                machine_code_snapshot=default_machine.code if default_machine else None,
                machine_name_snapshot=default_machine.name if default_machine else None,
                labor_minutes=Decimal("0"),
                machine_minutes=Decimal("0"),
            )
        )


def _get_latest_bom_header(db: Session, product_sku: str) -> ImportedBomHeader | None:
    return (
        db.query(ImportedBomHeader)
        .join(ImportBatch)
        .filter(ImportedBomHeader.product_sku == product_sku)
        .order_by(ImportBatch.imported_at.desc(), ImportedBomHeader.imported_at.desc(), ImportedBomHeader.id.desc())
        .first()
    )


def _copy_bom(db: Session, order: ProductionOrder, bom_header: ImportedBomHeader) -> None:
    bom_lines = (
        db.query(ImportedBomLine)
        .filter(ImportedBomLine.bom_header_id == bom_header.id)
        .order_by(ImportedBomLine.source_row_number, ImportedBomLine.id)
        .all()
    )
    for line in bom_lines:
        component_product = None
        if line.component_sku:
            component_product = db.query(Product).filter(Product.sku == line.component_sku).one_or_none()

        quantity_standard = line.quantity
        unit_cost_snapshot = component_product.standard_cost if component_product else line.component_cost
        line_cost = None
        if quantity_standard is not None and unit_cost_snapshot is not None:
            line_cost = quantity_standard * unit_cost_snapshot

        db.add(
            ProductionOrderMaterial(
                production_order_id=order.id,
                component_sku=line.component_sku,
                component_name=component_product.name if component_product else line.component_name,
                quantity_standard=quantity_standard,
                unit_cost_snapshot=unit_cost_snapshot,
                line_cost=line_cost,
                component_type=line.component_type,
                include_in_real_cost=line.include_in_real_cost,
            )
        )


def _ensure_not_closed(order: ProductionOrder) -> None:
    if order.status == ProductionOrderStatus.CLOSED.value:
        raise ProductionOrderValidationError("Closed orders are read-only.")
