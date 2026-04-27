from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from sqlalchemy.orm import Session, joinedload

from app.models import (
    Activity,
    AppSequence,
    ImportedBomHeader,
    ImportedBomLine,
    InventoryBalance,
    InventoryTransaction,
    ImportBatch,
    LotSequence,
    Product,
    ProductBomHeader,
    ProductBomLine,
    ProductionOrder,
    ProductionOrderActivity,
    ProductionOrderMaterial,
    Route,
    RouteActivity,
)
from app.schemas import ProductionOrderStatus
from app.services.costing_service import (
    INPUT_SCALED_PROCESS_TYPES,
    CostingValidationError,
    calculate_order_cost,
)
from app.services.inventory_ledger_service import (
    InventoryLedgerValidationError,
    get_or_create_inventory_balance,
    post_incoming_movement,
    post_outgoing_movement,
)


class ProductionOrderValidationError(Exception):
    pass


PRODUCTION_ORDER_SEQUENCE_NAME = "production_order"
PRODUCTION_ORDER_PREFIX = "OP"
LOT_SEQUENCE_START = 1
ZERO = Decimal("0")
ONE = Decimal("1")
LEDGER_PRODUCTION_SOURCE_TYPE = "production_order"
LEDGER_COMPONENT_CONSUMPTION_TYPE = "production_component_consumption"
LEDGER_PRODUCTION_RECEIPT_TYPE = "production_receipt"
LEDGER_PRODUCTION_RECEIPT_NOTE = (
    "Ledger production receipt cost calculated from ledger component consumption + "
    "labor/overhead/machine. ProductionOrder.real_unit_cost remains unchanged."
)


@dataclass(frozen=True)
class ProductionOrderClosePostingResult:
    order: ProductionOrder
    warnings: list[str]


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

    product_bom_header = _get_product_bom_header(db, product.id)
    imported_bom_header = None
    if product_bom_header is None:
        imported_bom_header = _get_latest_bom_header(db, product.sku)
    if product_bom_header is None and imported_bom_header is None:
        raise ProductionOrderValidationError(
            "Product must have a Product Master BOM or imported BOM before creating a Production Order."
        )

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
    order.lot_number = _generate_lot_number(db, production_date, product.sku)
    db.add(order)
    db.flush()

    _copy_route_activities(db, order, route_activities)
    if product_bom_header is not None:
        _copy_product_bom(db, order, product_bom_header)
    elif imported_bom_header is not None:
        _copy_bom(db, order, imported_bom_header)
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


def update_order_bom(
    db: Session,
    order_id: int,
    material_updates: list[dict[str, str]],
    deleted_material_ids: list[int],
    new_material: dict[str, str],
) -> ProductionOrder:
    order = get_order(db, order_id)
    _ensure_not_closed(order)

    materials_by_id = {material.id: material for material in order.materials}
    deleted_ids = set(deleted_material_ids)
    for material_id in deleted_ids:
        material = materials_by_id.get(material_id)
        if material is not None:
            db.delete(material)

    for update in material_updates:
        material_id = int(update["id"])
        if material_id in deleted_ids:
            continue
        material = materials_by_id.get(material_id)
        if material is None:
            continue

        quantity_standard = parse_optional_decimal(update.get("quantity_standard"), "Quantity standard")
        if quantity_standard is not None and quantity_standard < 0:
            raise ProductionOrderValidationError("Quantity standard cannot be negative.")

        component_product = _resolve_component_product(db, update.get("component_sku"))
        previous_sku = material.component_sku
        material.component_sku = component_product.sku
        material.component_name = component_product.name
        if previous_sku != component_product.sku:
            if component_product.standard_cost is None:
                raise ProductionOrderValidationError(
                    f"Product {component_product.sku} has no standard cost and cannot be used in the BOM."
                )
            material.unit_cost_snapshot = component_product.standard_cost
        material.quantity_standard = quantity_standard
        material.required_quantity = _calculate_required_quantity(order, quantity_standard)

    new_component_sku = (new_material.get("component_sku") or "").strip()
    new_quantity_text = (new_material.get("quantity_standard") or "").strip()
    if new_component_sku or new_quantity_text:
        component_product = _resolve_component_product(db, new_component_sku)
        if component_product.standard_cost is None:
            raise ProductionOrderValidationError(
                f"Product {component_product.sku} has no standard cost and cannot be added to the BOM."
            )
        quantity_standard = parse_optional_decimal(new_quantity_text, "New line quantity standard")
        if quantity_standard is None:
            raise ProductionOrderValidationError("New line quantity standard is required.")
        if quantity_standard < 0:
            raise ProductionOrderValidationError("New line quantity standard cannot be negative.")
        db.add(
            ProductionOrderMaterial(
                production_order_id=order.id,
                component_sku=component_product.sku,
                component_name=component_product.name,
                quantity_standard=quantity_standard,
                required_quantity=_calculate_required_quantity(order, quantity_standard),
                unit_cost_snapshot=component_product.standard_cost,
                line_cost=None,
                component_type="material",
                include_in_real_cost=True,
            )
        )

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
    try:
        _prepare_order_close(db, order)
        db.commit()
    except ProductionOrderValidationError:
        db.rollback()
        raise
    db.refresh(order)
    return order


def close_order_with_inventory_posting(db: Session, order_id: int) -> ProductionOrderClosePostingResult:
    order = get_order(db, order_id)
    try:
        _ensure_no_existing_production_ledger_posting(db, order)
        _prepare_order_close(db, order)
        warnings = _post_production_close_inventory(db, order)
        db.commit()
    except ProductionOrderValidationError:
        db.rollback()
        raise
    except InventoryLedgerValidationError as exc:
        db.rollback()
        raise ProductionOrderValidationError(str(exc)) from exc
    except Exception:
        db.rollback()
        raise
    db.refresh(order)
    return ProductionOrderClosePostingResult(order=order, warnings=warnings)


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


def _generate_lot_number(db: Session, production_date: date, product_sku: str) -> str:
    iso_year, iso_week, _ = production_date.isocalendar()
    sequence = (
        db.query(LotSequence)
        .filter(
            LotSequence.iso_year == iso_year,
            LotSequence.iso_week == iso_week,
            LotSequence.product_sku == product_sku,
        )
        .one_or_none()
    )
    if sequence is None:
        sequence = LotSequence(
            iso_year=iso_year,
            iso_week=iso_week,
            product_sku=product_sku,
            next_value=LOT_SEQUENCE_START,
        )
        db.add(sequence)
        db.flush()

    lot_number = f"{iso_year}{iso_week:02d}{_sku_lot_fragment(product_sku)}{sequence.next_value:02d}"
    existing_order = db.query(ProductionOrder).filter(ProductionOrder.lot_number == lot_number).one_or_none()
    if existing_order is not None:
        raise ProductionOrderValidationError(f"Generated lot number {lot_number} already exists.")

    sequence.next_value += 1
    return lot_number


def _sku_lot_fragment(product_sku: str) -> str:
    return (product_sku or "")[-4:].zfill(4)


def _calculate_required_quantity(order: ProductionOrder, quantity_standard: Decimal | None) -> Decimal | None:
    if order.planned_qty is None or quantity_standard is None:
        return None
    return order.planned_qty * quantity_standard


def _prepare_order_close(db: Session, order: ProductionOrder) -> None:
    if order.status != ProductionOrderStatus.IN_PROGRESS.value:
        raise ProductionOrderValidationError("Only in-progress orders can be closed.")
    if order.input_qty is None or order.input_qty <= ZERO:
        raise ProductionOrderValidationError("Input quantity must be greater than 0 before closing.")
    if order.output_qty is None or order.output_qty <= ZERO:
        raise ProductionOrderValidationError("Output quantity must be greater than 0 before closing.")

    order.yield_percent = order.output_qty / order.input_qty
    try:
        calculate_order_cost(db, order)
    except CostingValidationError as exc:
        raise ProductionOrderValidationError(str(exc)) from exc

    order.status = ProductionOrderStatus.CLOSED.value
    order.closed_at = datetime.utcnow()


def _ensure_no_existing_production_ledger_posting(db: Session, order: ProductionOrder) -> None:
    existing_transaction = (
        db.query(InventoryTransaction.id)
        .filter(
            InventoryTransaction.source_type == LEDGER_PRODUCTION_SOURCE_TYPE,
            InventoryTransaction.source_id == order.id,
            InventoryTransaction.transaction_type.in_(
                [LEDGER_COMPONENT_CONSUMPTION_TYPE, LEDGER_PRODUCTION_RECEIPT_TYPE]
            ),
        )
        .first()
    )
    if existing_transaction is not None:
        raise ProductionOrderValidationError(
            f"Production order {order.internal_order_number} already has inventory ledger postings."
        )


def _post_production_close_inventory(db: Session, order: ProductionOrder) -> list[str]:
    warnings: list[str] = []
    component_consumption_total = ZERO

    for material in sorted(order.materials, key=lambda item: item.id):
        if not (material.component_sku or "").strip():
            raise ProductionOrderValidationError(
                f"Production order material line {material.id} has no component SKU and cannot be posted to inventory."
            )
        component_product = _resolve_component_product(db, material.component_sku)
        consumed_qty = _calculate_ledger_material_consumption_quantity(order, material)
        if consumed_qty <= ZERO:
            continue

        component_warnings = _component_consumption_warnings(db, component_product, consumed_qty)
        result = post_outgoing_movement(
            db,
            product_id=component_product.id,
            transaction_type=LEDGER_COMPONENT_CONSUMPTION_TYPE,
            outgoing_qty=consumed_qty,
            transaction_date=order.closed_at,
            source_type=LEDGER_PRODUCTION_SOURCE_TYPE,
            source_id=order.id,
            source_line_id=material.id,
            notes=_build_component_consumption_note(order, component_product.sku, component_warnings),
        )
        component_consumption_total += result.transaction.total_cost or ZERO
        warnings = _merge_warnings(warnings, component_warnings)

    receipt_unit_cost = _calculate_ledger_receipt_unit_cost(order, component_consumption_total)
    post_incoming_movement(
        db,
        product_id=order.product_id,
        transaction_type=LEDGER_PRODUCTION_RECEIPT_TYPE,
        incoming_qty=order.output_qty,
        incoming_unit_cost=receipt_unit_cost,
        transaction_date=order.closed_at,
        source_type=LEDGER_PRODUCTION_SOURCE_TYPE,
        source_id=order.id,
        notes=LEDGER_PRODUCTION_RECEIPT_NOTE,
    )
    return warnings


def _calculate_ledger_material_consumption_quantity(
    order: ProductionOrder,
    material: ProductionOrderMaterial,
) -> Decimal:
    component_label = material.component_sku or material.component_name or f"material line {material.id}"
    # Production ledger consumption uses the persisted required_quantity snapshot when
    # available, because it represents the component quantity required for this
    # Production Order. The older quantity_standard logic is only a fallback for
    # legacy rows without required_quantity.
    if material.required_quantity is not None and material.required_quantity > ZERO:
        return material.required_quantity
    if material.quantity_standard is None:
        raise ProductionOrderValidationError(
            f"Production component {component_label} has no standard quantity."
        )
    # Must stay aligned with costing_service material quantity logic.
    scaling_factor = order.input_qty if order.process_type in INPUT_SCALED_PROCESS_TYPES else ONE
    return material.quantity_standard * scaling_factor


def _component_consumption_warnings(
    db: Session,
    component_product: Product,
    consumed_qty: Decimal,
) -> list[str]:
    balance = get_or_create_inventory_balance(db, component_product.id)
    current_qty = balance.on_hand_qty if balance.on_hand_qty is not None else ZERO
    current_average_cost = balance.average_unit_cost if balance.average_unit_cost is not None else ZERO
    warnings: list[str] = []
    if current_average_cost == ZERO:
        warnings.append(f"Component {component_product.sku} was consumed with zero average cost.")
    projected_qty = current_qty - consumed_qty
    if projected_qty < ZERO:
        warnings.append(
            f"Production close leaves component {component_product.sku} with negative on-hand quantity {projected_qty}."
        )
    return warnings


def _build_component_consumption_note(
    order: ProductionOrder,
    component_sku: str,
    warning_messages: list[str],
) -> str:
    note = f"Production order {order.internal_order_number} component consumption for {component_sku}."
    if warning_messages:
        note = f"{note} Warnings: {' '.join(warning_messages)}"
    return note


def _calculate_ledger_receipt_unit_cost(
    order: ProductionOrder,
    component_consumption_total: Decimal,
) -> Decimal:
    output_qty = order.output_qty if order.output_qty is not None else ZERO
    if output_qty <= ZERO:
        raise ProductionOrderValidationError("Output quantity must be greater than 0 before posting ledger receipt.")

    labor_total = order.real_labor_cost_total if order.real_labor_cost_total is not None else ZERO
    overhead_total = order.real_overhead_cost_total if order.real_overhead_cost_total is not None else ZERO
    machine_total = order.real_machine_cost_total if order.real_machine_cost_total is not None else ZERO
    receipt_total_cost = component_consumption_total + labor_total + overhead_total + machine_total
    receipt_unit_cost = receipt_total_cost / output_qty
    if receipt_unit_cost < ZERO:
        raise ProductionOrderValidationError("Ledger production receipt unit cost cannot be negative.")
    return receipt_unit_cost


def _merge_warnings(existing: list[str], new_messages: list[str]) -> list[str]:
    merged = list(existing)
    for message in new_messages:
        if message not in merged:
            merged.append(message)
    return merged


def _resolve_component_product(db: Session, component_sku: str | None) -> Product:
    sku = (component_sku or "").strip()
    if not sku:
        raise ProductionOrderValidationError("Component SKU is required.")
    product = db.query(Product).filter(Product.sku == sku).one_or_none()
    if product is None:
        raise ProductionOrderValidationError(f"Component SKU {sku} does not exist.")
    return product


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


def _get_product_bom_header(db: Session, product_id: int) -> ProductBomHeader | None:
    header = (
        db.query(ProductBomHeader)
        .options(
            joinedload(ProductBomHeader.lines).joinedload(ProductBomLine.component_product),
            joinedload(ProductBomHeader.lines).joinedload(ProductBomLine.source_imported_bom_line),
        )
        .filter(ProductBomHeader.product_id == product_id)
        .one_or_none()
    )
    if header is None or not header.lines:
        return None
    return header


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
                required_quantity=_calculate_required_quantity(order, quantity_standard),
                unit_cost_snapshot=unit_cost_snapshot,
                line_cost=line_cost,
                component_type=line.component_type,
                include_in_real_cost=line.include_in_real_cost,
            )
        )


def _copy_product_bom(db: Session, order: ProductionOrder, bom_header: ProductBomHeader) -> None:
    bom_lines = sorted(bom_header.lines, key=lambda line: (line.line_number, line.id))
    for line in bom_lines:
        quantity_standard = line.quantity_standard
        unit_cost_snapshot = _resolve_product_bom_unit_cost(db, line)
        line_cost = None
        if quantity_standard is not None and unit_cost_snapshot is not None:
            line_cost = quantity_standard * unit_cost_snapshot

        db.add(
            ProductionOrderMaterial(
                production_order_id=order.id,
                component_sku=line.component_sku_snapshot,
                component_name=line.component_name_snapshot,
                quantity_standard=quantity_standard,
                required_quantity=_calculate_required_quantity(order, quantity_standard),
                unit_cost_snapshot=unit_cost_snapshot,
                line_cost=line_cost,
                component_type=(line.component_type or "").strip() or "material",
                include_in_real_cost=(
                    line.include_in_real_cost if line.include_in_real_cost is not None else True
                ),
            )
        )


def _resolve_product_bom_unit_cost(db: Session, line: ProductBomLine) -> Decimal | None:
    component_product = line.component_product
    if component_product is not None:
        balance = (
            db.query(InventoryBalance)
            .filter(InventoryBalance.product_id == component_product.id)
            .one_or_none()
        )
        if balance is not None and balance.average_unit_cost is not None and balance.average_unit_cost > ZERO:
            return balance.average_unit_cost
        if component_product.standard_cost is not None and component_product.standard_cost > ZERO:
            return component_product.standard_cost

    imported_line = line.source_imported_bom_line
    if imported_line is not None and imported_line.component_cost is not None:
        return imported_line.component_cost
    return None


def _ensure_not_closed(order: ProductionOrder) -> None:
    if order.status == ProductionOrderStatus.CLOSED.value:
        raise ProductionOrderValidationError("Closed orders are read-only.")
