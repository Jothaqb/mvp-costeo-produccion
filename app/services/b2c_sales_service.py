from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from sqlalchemy.orm import Session, joinedload

from app.models import (
    AppSequence,
    B2CSalesOrder,
    B2CSalesOrderLine,
    InventoryTransaction,
    Product,
)
from app.services.inventory_ledger_service import (
    InventoryLedgerValidationError,
    get_or_create_inventory_balance,
    post_outgoing_movement,
)


B2C_SEQUENCE_NAME = "b2c_sales_order"
B2C_ORDER_PREFIX = "B2C"
B2C_CHANNELS = {"whatsapp", "website", "other"}
EDITABLE_STATUSES = {"draft"}
ORDER_STATUSES = {"draft", "invoiced", "cancelled"}
ALLOWED_STATUS_TRANSITIONS = {
    "draft": {"invoiced", "cancelled"},
    "invoiced": set(),
    "cancelled": set(),
}
ZERO = Decimal("0")
SNAPSHOT_QUANT = Decimal("0.0001")


class B2CValidationError(Exception):
    pass


@dataclass(frozen=True)
class PreparedB2CInvoiceLine:
    line: B2CSalesOrderLine
    product: Product
    quantity: Decimal


def parse_required_decimal(value: str | Decimal | None, field_name: str) -> Decimal:
    if isinstance(value, Decimal):
        return value
    text = (value or "").strip().replace(" ", "").replace(",", ".")
    try:
        return Decimal(text)
    except InvalidOperation as exc:
        raise B2CValidationError(f"{field_name} must be a valid number.") from exc


def create_b2c_sales_order(
    db: Session,
    *,
    order_date: date,
    customer_name: str,
    customer_phone: str,
    customer_email: str,
    channel: str,
    observations: str,
    line_inputs: list[dict[str, str]],
) -> B2CSalesOrder:
    normalized_channel = _normalize_channel(channel)
    line_data = _build_b2c_line_data(db, line_inputs, require_at_least_one=True)
    order = B2CSalesOrder(
        order_number=_generate_b2c_order_number(db),
        order_date=order_date,
        customer_name=_clean_optional_text(customer_name),
        customer_phone=_clean_optional_text(customer_phone),
        customer_email=_clean_optional_text(customer_email),
        channel=normalized_channel,
        status="draft",
        subtotal_amount=ZERO,
        total_amount=ZERO,
        observations=_clean_optional_text(observations),
    )
    db.add(order)
    db.flush()
    _replace_lines(order, line_data)
    _recalculate_b2c_order_total(order)
    db.commit()
    db.refresh(order)
    return order


def update_b2c_sales_order(
    db: Session,
    *,
    order_id: int,
    order_date: date,
    customer_name: str,
    customer_phone: str,
    customer_email: str,
    channel: str,
    observations: str,
    line_updates: list[dict[str, str]],
    deleted_line_ids: list[int],
    new_line_inputs: list[dict[str, str]],
) -> B2CSalesOrder:
    order = (
        db.query(B2CSalesOrder)
        .options(joinedload(B2CSalesOrder.lines))
        .filter(B2CSalesOrder.id == order_id)
        .one()
    )
    _ensure_b2c_order_editable(order)
    order.order_date = order_date
    order.customer_name = _clean_optional_text(customer_name)
    order.customer_phone = _clean_optional_text(customer_phone)
    order.customer_email = _clean_optional_text(customer_email)
    order.channel = _normalize_channel(channel)
    order.observations = _clean_optional_text(observations)

    catalog_by_sku = _sellable_product_catalog_by_sku(db)
    deleted_ids = set(deleted_line_ids)
    lines_by_id = {line.id: line for line in order.lines}
    for line_id in deleted_ids:
        line = lines_by_id.get(line_id)
        if line is not None:
            db.delete(line)

    for update in line_updates:
        line_id = int(update["id"])
        if line_id in deleted_ids:
            continue
        line = lines_by_id.get(line_id)
        if line is None:
            continue
        quantity = parse_required_decimal(update.get("quantity"), "Quantity")
        if quantity <= ZERO:
            raise B2CValidationError("Quantity must be greater than 0.")
        unit_price = parse_required_decimal(update.get("unit_price"), "Unit price")
        if unit_price < ZERO:
            raise B2CValidationError("Unit price cannot be negative.")
        sku = (update.get("sku") or "").strip()
        if sku != line.sku_snapshot:
            product = _catalog_product_for_sku(catalog_by_sku, sku)
            _assign_line_from_product(line, product, quantity, unit_price)
        else:
            line.quantity = quantity
            line.unit_price_snapshot = unit_price
            line.line_total = (quantity * unit_price).quantize(SNAPSHOT_QUANT)

    next_line_number = len([line for line in order.lines if line.id not in deleted_ids]) + 1
    for line_input in _submitted_line_inputs(new_line_inputs):
        product = _catalog_product_for_sku(catalog_by_sku, line_input["sku"])
        quantity = parse_required_decimal(line_input["quantity"], "Quantity")
        if quantity <= ZERO:
            raise B2CValidationError("Quantity must be greater than 0.")
        unit_price = parse_required_decimal(line_input["unit_price"], "Unit price")
        if unit_price < ZERO:
            raise B2CValidationError("Unit price cannot be negative.")
        line = B2CSalesOrderLine(line_number=next_line_number)
        _assign_line_from_product(line, product, quantity, unit_price)
        order.lines.append(line)
        next_line_number += 1

    db.flush()
    remaining_lines = (
        db.query(B2CSalesOrderLine)
        .filter(B2CSalesOrderLine.sales_order_id == order.id)
        .order_by(B2CSalesOrderLine.line_number, B2CSalesOrderLine.id)
        .all()
    )
    if not remaining_lines:
        raise B2CValidationError("Order must have at least one line.")
    _renumber_lines(remaining_lines)
    _recalculate_b2c_order_total(order)
    db.commit()
    db.refresh(order)
    return order


def change_b2c_sales_order_status(db: Session, order_id: int, new_status: str) -> B2CSalesOrder:
    order = db.query(B2CSalesOrder).filter(B2CSalesOrder.id == order_id).one()
    normalized_status = (new_status or "").strip().lower()
    if normalized_status not in ORDER_STATUSES:
        raise B2CValidationError("Invalid status.")
    if normalized_status not in ALLOWED_STATUS_TRANSITIONS[order.status]:
        raise B2CValidationError(f"Cannot change status from {order.status} to {normalized_status}.")
    order.status = normalized_status
    db.commit()
    db.refresh(order)
    return order


def invoice_b2c_order_in_erp(db: Session, order_id: int) -> B2CSalesOrder:
    order = (
        db.query(B2CSalesOrder)
        .options(joinedload(B2CSalesOrder.lines))
        .filter(B2CSalesOrder.id == order_id)
        .one()
    )
    _validate_b2c_invoice_eligibility(order)
    _ensure_no_existing_b2c_invoice_posting(db, order.id)

    prepared_lines = _prepare_b2c_invoice_lines(db, order)
    if not prepared_lines:
        raise B2CValidationError("Order must include at least one positive-quantity line to invoice.")

    transaction_date = datetime.utcnow()
    try:
        for prepared_line in prepared_lines:
            _post_b2c_invoice_line(db, order, prepared_line, transaction_date)

        order.status = "invoiced"
        db.commit()
        db.refresh(order)
        return order
    except InventoryLedgerValidationError as exc:
        db.rollback()
        raise B2CValidationError(str(exc)) from exc
    except B2CValidationError:
        db.rollback()
        raise
    except Exception:
        db.rollback()
        raise


def _validate_b2c_invoice_eligibility(order: B2CSalesOrder) -> None:
    if order.status == "invoiced":
        raise B2CValidationError("Order is already invoiced.")
    if "invoiced" not in ALLOWED_STATUS_TRANSITIONS[order.status]:
        raise B2CValidationError(f"Cannot change status from {order.status} to invoiced.")


def _ensure_no_existing_b2c_invoice_posting(db: Session, order_id: int) -> None:
    existing_transaction = (
        db.query(InventoryTransaction.id)
        .filter(
            InventoryTransaction.transaction_type == "b2c_sale",
            InventoryTransaction.source_type == "b2c_order",
            InventoryTransaction.source_id == order_id,
        )
        .first()
    )
    if existing_transaction is not None:
        raise B2CValidationError("This B2C order already has Kardex invoice transactions.")


def _prepare_b2c_invoice_lines(db: Session, order: B2CSalesOrder) -> list[PreparedB2CInvoiceLine]:
    prepared_lines: list[PreparedB2CInvoiceLine] = []
    for line in sorted(order.lines, key=lambda current_line: (current_line.line_number, current_line.id)):
        quantity = parse_required_decimal(line.quantity, "Quantity")
        if quantity <= ZERO:
            continue
        product = _resolve_b2c_invoice_product(db, line)
        prepared_lines.append(PreparedB2CInvoiceLine(line=line, product=product, quantity=quantity))
    return prepared_lines


def _resolve_b2c_invoice_product(db: Session, line: B2CSalesOrderLine) -> Product:
    sku = (line.sku_snapshot or "").strip()
    if not sku:
        raise B2CValidationError("B2C line is missing SKU snapshot and cannot be invoiced.")
    product = db.query(Product).filter(Product.sku == sku).one_or_none()
    if product is None:
        raise B2CValidationError(f"B2C line SKU {sku} could not be resolved to a product.")
    return product


def _post_b2c_invoice_line(
    db: Session,
    order: B2CSalesOrder,
    prepared_line: PreparedB2CInvoiceLine,
    transaction_date: datetime,
) -> None:
    balance = get_or_create_inventory_balance(db, prepared_line.product.id)
    current_average_cost = parse_required_decimal(balance.average_unit_cost, "Current average cost")
    warning_messages: list[str] = []
    if balance.last_transaction_id is None:
        warning_messages.append("No prior inventory balance existed; average cost defaulted to 0.")
    if current_average_cost == ZERO:
        warning_messages.append("Average cost was 0 at invoice time; cost snapshot and COGS were recorded as 0.")

    base_note = (
        f"B2C order {order.order_number} invoice for SKU {prepared_line.line.sku_snapshot}. "
        "ERP-local invoice posting."
    )
    posting = post_outgoing_movement(
        db,
        product_id=prepared_line.product.id,
        transaction_type="b2c_sale",
        outgoing_qty=prepared_line.quantity,
        transaction_date=transaction_date,
        source_type="b2c_order",
        source_id=order.id,
        source_line_id=prepared_line.line.id,
        notes=base_note,
    )

    warning_messages.extend(posting.warnings)
    if warning_messages:
        posting.transaction.notes = _append_warning_notes(base_note, warning_messages)

    cost_unit_snapshot = parse_required_decimal(posting.transaction.unit_cost, "COGS unit cost").quantize(SNAPSHOT_QUANT)
    cost_total_snapshot = parse_required_decimal(posting.transaction.total_cost, "COGS total cost").quantize(SNAPSHOT_QUANT)
    line_total = parse_required_decimal(prepared_line.line.line_total, "Line total").quantize(SNAPSHOT_QUANT)
    gross_margin_amount = (line_total - cost_total_snapshot).quantize(SNAPSHOT_QUANT)
    gross_margin_percent = (
        (gross_margin_amount / line_total).quantize(SNAPSHOT_QUANT)
        if line_total > ZERO
        else None
    )

    prepared_line.line.cost_unit_snapshot = cost_unit_snapshot
    prepared_line.line.cost_total_snapshot = cost_total_snapshot
    prepared_line.line.gross_margin_amount = gross_margin_amount
    prepared_line.line.gross_margin_percent = gross_margin_percent


def _append_warning_notes(base_note: str, warnings: list[str]) -> str:
    unique_warnings = [warning.strip() for warning in warnings if warning and warning.strip()]
    if not unique_warnings:
        return base_note
    return f"{base_note} " + " ".join(f"Warning: {warning}" for warning in unique_warnings)


def _build_b2c_line_data(
    db: Session,
    line_inputs: list[dict[str, str]],
    require_at_least_one: bool,
) -> list[tuple[Product, Decimal, Decimal]]:
    catalog_by_sku = _sellable_product_catalog_by_sku(db)
    line_data: list[tuple[Product, Decimal, Decimal]] = []
    for line_input in _submitted_line_inputs(line_inputs):
        product = _catalog_product_for_sku(catalog_by_sku, line_input["sku"])
        quantity = parse_required_decimal(line_input["quantity"], "Quantity")
        if quantity <= ZERO:
            raise B2CValidationError("Quantity must be greater than 0.")
        unit_price = parse_required_decimal(line_input["unit_price"], "Unit price")
        if unit_price < ZERO:
            raise B2CValidationError("Unit price cannot be negative.")
        line_data.append((product, quantity, unit_price))
    if require_at_least_one and not line_data:
        raise B2CValidationError("At least one valid order line is required.")
    return line_data


def _submitted_line_inputs(line_inputs: list[dict[str, str]]) -> list[dict[str, str]]:
    return [
        {
            "sku": (line.get("sku") or "").strip(),
            "quantity": (line.get("quantity") or "").strip(),
            "unit_price": (line.get("unit_price") or "").strip(),
        }
        for line in line_inputs
        if (line.get("sku") or "").strip()
        or (line.get("quantity") or "").strip()
        or (line.get("unit_price") or "").strip()
    ]


def _sellable_product_catalog_by_sku(db: Session) -> dict[str, Product]:
    products = (
        db.query(Product)
        .filter(Product.available_for_sale_gc.is_(True))
        .order_by(Product.name, Product.sku)
        .all()
    )
    return {product.sku: product for product in products}


def _catalog_product_for_sku(
    catalog_by_sku: dict[str, Product],
    sku: str,
) -> Product:
    product = catalog_by_sku.get((sku or "").strip())
    if product is None:
        raise B2CValidationError("Selected product is not available for B2C sales.")
    return product


def _replace_lines(order: B2CSalesOrder, line_data: list[tuple[Product, Decimal, Decimal]]) -> None:
    order.lines.clear()
    for index, (product, quantity, unit_price) in enumerate(line_data, start=1):
        line = B2CSalesOrderLine(line_number=index)
        _assign_line_from_product(line, product, quantity, unit_price)
        order.lines.append(line)


def _assign_line_from_product(
    line: B2CSalesOrderLine,
    product: Product,
    quantity: Decimal,
    unit_price: Decimal,
) -> None:
    line.sku_snapshot = product.sku
    line.description_snapshot = product.name
    line.quantity = quantity
    line.unit_price_snapshot = unit_price
    line.line_total = (quantity * unit_price).quantize(SNAPSHOT_QUANT)


def _renumber_lines(lines: list[B2CSalesOrderLine]) -> None:
    for index, line in enumerate(lines, start=1):
        line.line_number = index


def _recalculate_b2c_order_total(order: B2CSalesOrder) -> None:
    subtotal = sum((line.line_total or ZERO for line in order.lines), ZERO).quantize(SNAPSHOT_QUANT)
    order.subtotal_amount = subtotal
    order.total_amount = subtotal


def _ensure_b2c_order_editable(order: B2CSalesOrder) -> None:
    if order.status not in EDITABLE_STATUSES:
        raise B2CValidationError("Only draft B2C orders are editable.")


def _generate_b2c_order_number(db: Session) -> str:
    sequence = db.query(AppSequence).filter(AppSequence.name == B2C_SEQUENCE_NAME).one_or_none()
    if sequence is None:
        sequence = AppSequence(
            name=B2C_SEQUENCE_NAME,
            next_value=_bootstrap_next_b2c_order_sequence(db),
        )
        db.add(sequence)
        db.flush()

    order_number = f"{B2C_ORDER_PREFIX}{sequence.next_value:04d}"
    existing_order = db.query(B2CSalesOrder).filter(B2CSalesOrder.order_number == order_number).one_or_none()
    if existing_order is not None:
        raise B2CValidationError(f"Generated B2C order number {order_number} already exists.")
    sequence.next_value += 1
    return order_number


def _bootstrap_next_b2c_order_sequence(db: Session) -> int:
    highest = 0
    order_numbers = db.query(B2CSalesOrder.order_number).all()
    for (order_number,) in order_numbers:
        if not order_number or not order_number.startswith(B2C_ORDER_PREFIX):
            continue
        suffix = order_number[len(B2C_ORDER_PREFIX):]
        if suffix.isdigit():
            highest = max(highest, int(suffix))
    return highest + 1


def _normalize_channel(channel: str) -> str:
    normalized = (channel or "").strip().lower()
    if normalized not in B2C_CHANNELS:
        raise B2CValidationError("Channel is required.")
    return normalized


def _clean_optional_text(value: str) -> str | None:
    cleaned = (value or "").strip()
    return cleaned or None
