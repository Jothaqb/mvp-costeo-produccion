import secrets
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from sqlalchemy.orm import Session

from app.models import (
    AppSequence,
    Product,
    PurchaseOrder,
    PurchaseOrderLine,
    PurchaseOrderReceiveToken,
)
from app.services.inventory_ledger_service import InventoryLedgerValidationError, post_incoming_movement


PURCHASE_ORDER_SEQUENCE_NAME = "purchase_order"
PURCHASE_ORDER_PREFIX = "PO"
PURCHASE_ORDER_STATUSES = {"draft", "issued", "incomplete", "closed"}
EDITABLE_STATUSES = {"draft"}
RECEIVABLE_STATUSES = {"draft", "incomplete"}
READ_ONLY_STATUSES = {"issued", "closed"}
ZERO = Decimal("0")
LEDGER_PO_RECEIPT_TYPE = "po_receipt"
LEDGER_PO_SOURCE_TYPE = "purchase_order"


class PurchaseOrderValidationError(Exception):
    pass


@dataclass(frozen=True)
class PreparedPurchaseOrderReceiptLine:
    line: PurchaseOrderLine
    receive_now: Decimal
    new_received_quantity: Decimal
    product: Product
    unit_cost: Decimal


def parse_required_decimal(value: str | Decimal | None, field_name: str) -> Decimal:
    if isinstance(value, Decimal):
        return value
    text = (value or "").strip().replace(" ", "").replace(",", ".")
    try:
        return Decimal(text)
    except InvalidOperation as exc:
        raise PurchaseOrderValidationError(f"{field_name} must be a valid number.") from exc


def create_purchase_order(
    db: Session,
    supplier: str,
    po_date: date,
    status: str,
    notes: str,
    line_inputs: list[dict[str, str]],
) -> PurchaseOrder:
    normalized_status = _normalize_save_status(status)
    supplier_name = _normalize_supplier(supplier)
    line_data = _build_line_data(supplier_name, line_inputs, require_at_least_one=True)
    order = PurchaseOrder(
        po_number=_generate_purchase_order_number(db),
        supplier_name_snapshot=supplier_name,
        po_date=po_date,
        status=normalized_status,
        notes=notes.strip() or None,
        estimated_total=ZERO,
    )
    db.add(order)
    db.flush()
    _replace_lines(order, line_data)
    _recalculate_order_total(order)
    db.commit()
    db.refresh(order)
    return order


def update_purchase_order(
    db: Session,
    order_id: int,
    supplier: str,
    po_date: date,
    status: str,
    notes: str,
    line_inputs: list[dict[str, str]],
) -> PurchaseOrder:
    order = db.query(PurchaseOrder).filter(PurchaseOrder.id == order_id).one()
    _ensure_order_editable(order)
    normalized_status = _normalize_save_status(status)
    supplier_name = _normalize_supplier(supplier)
    line_data = _build_line_data(supplier_name, line_inputs, require_at_least_one=True)
    order.supplier_name_snapshot = supplier_name
    order.po_date = po_date
    order.status = normalized_status
    order.notes = notes.strip() or None
    _replace_lines(order, line_data)
    _recalculate_order_total(order)
    db.commit()
    db.refresh(order)
    return order


def receive_purchase_order(
    db: Session,
    order_id: int,
    receive_now_inputs: dict[int, str],
) -> PurchaseOrder:
    order = db.query(PurchaseOrder).filter(PurchaseOrder.id == order_id).one()
    prepared_lines = _prepare_purchase_order_receipt(db, order, receive_now_inputs)
    for prepared_line in prepared_lines:
        prepared_line.line.received_quantity = prepared_line.new_received_quantity
    order.status = "closed" if _all_prepared_lines_fully_received(order, prepared_lines) else "incomplete"
    db.commit()
    db.refresh(order)
    return order


def create_purchase_order_receive_token(db: Session, order_id: int) -> PurchaseOrderReceiveToken:
    order = db.query(PurchaseOrder.id).filter(PurchaseOrder.id == order_id).one_or_none()
    if order is None:
        raise PurchaseOrderValidationError("Purchase Order not found.")

    receive_token = PurchaseOrderReceiveToken(
        purchase_order_id=order_id,
        token=secrets.token_urlsafe(32),
        used_at=None,
    )
    db.add(receive_token)
    db.commit()
    db.refresh(receive_token)
    return receive_token


def receive_purchase_order_with_inventory_posting(
    db: Session,
    order_id: int,
    receive_now_inputs: dict[int, str],
    receive_token: str,
) -> PurchaseOrder:
    order = db.query(PurchaseOrder).filter(PurchaseOrder.id == order_id).one()
    try:
        prepared_lines = _prepare_purchase_order_receipt(db, order, receive_now_inputs)
        _consume_purchase_order_receive_token(db, order.id, receive_token)
        for prepared_line in prepared_lines:
            prepared_line.line.received_quantity = prepared_line.new_received_quantity
            post_incoming_movement(
                db,
                product_id=prepared_line.product.id,
                transaction_type=LEDGER_PO_RECEIPT_TYPE,
                incoming_qty=prepared_line.receive_now,
                incoming_unit_cost=prepared_line.unit_cost,
                transaction_date=datetime.utcnow(),
                source_type=LEDGER_PO_SOURCE_TYPE,
                source_id=order.id,
                source_line_id=prepared_line.line.id,
                notes=(
                    f"Purchase order {order.po_number} receipt for SKU {prepared_line.line.sku_snapshot}. "
                    "Incremental receipt quantity posted from Receive Now."
                ),
            )
        order.status = "closed" if _all_prepared_lines_fully_received(order, prepared_lines) else "incomplete"
        db.commit()
    except PurchaseOrderValidationError:
        db.rollback()
        raise
    except InventoryLedgerValidationError as exc:
        db.rollback()
        raise PurchaseOrderValidationError(str(exc)) from exc
    except Exception:
        db.rollback()
        raise
    db.refresh(order)
    return order


def build_purchase_order_prefill(db: Session, product_id: int, quantity_text: str) -> dict[str, object]:
    quantity = parse_required_decimal(quantity_text, "Planner quantity")
    if quantity <= ZERO:
        raise PurchaseOrderValidationError("Planner quantity must be greater than 0.")

    product = (
        db.query(Product)
        .filter(
            Product.id == product_id,
            Product.is_manufactured.is_(False),
            Product.active.is_(True),
            Product.available_for_sale_gc.is_(True),
        )
        .one_or_none()
    )
    if product is None:
        raise PurchaseOrderValidationError("Selected product is not available for purchased planning.")

    return {
        "supplier": product.supplier or "",
        "lines": [
            {
                "sku": product.sku,
                "description": product.name,
                "quantity": quantity,
                "unit_cost": product.standard_cost if product.standard_cost is not None else ZERO,
            }
        ],
    }


def list_purchase_order_suppliers(db: Session) -> list[str]:
    values = db.query(PurchaseOrder.supplier_name_snapshot).order_by(PurchaseOrder.supplier_name_snapshot).all()
    return sorted({value for (value,) in values if value})


def list_all_product_suppliers(db: Session) -> list[str]:
    values = db.query(Product.supplier).order_by(Product.supplier).all()
    return sorted({(value or "").strip() for (value,) in values if (value or "").strip()})


def pending_quantity_for_line(line: PurchaseOrderLine) -> Decimal:
    return max(Decimal(line.quantity or ZERO) - Decimal(line.received_quantity or ZERO), ZERO)


def can_receive_purchase_order(order: PurchaseOrder) -> bool:
    return order.status in RECEIVABLE_STATUSES


def is_purchase_order_editable(order: PurchaseOrder) -> bool:
    return order.status in EDITABLE_STATUSES


def _normalize_supplier(supplier: str) -> str:
    supplier_name = (supplier or "").strip()
    if not supplier_name:
        raise PurchaseOrderValidationError("Supplier is required.")
    return supplier_name


def _normalize_status(status: str) -> str:
    normalized = (status or "draft").strip().lower()
    if normalized not in PURCHASE_ORDER_STATUSES:
        raise PurchaseOrderValidationError("Invalid status.")
    return normalized


def _normalize_save_status(status: str) -> str:
    normalized = _normalize_status(status)
    if normalized != "draft":
        raise PurchaseOrderValidationError("Purchase Orders can only be saved in Draft. Use Receive to complete the workflow.")
    return normalized


def _prepare_purchase_order_receipt(
    db: Session,
    order: PurchaseOrder,
    receive_now_inputs: dict[int, str],
) -> list[PreparedPurchaseOrderReceiptLine]:
    if order.status not in RECEIVABLE_STATUSES:
        raise PurchaseOrderValidationError(f"Purchase orders in status {order.status} cannot receive.")
    if not order.lines:
        raise PurchaseOrderValidationError("Purchase Order has no lines to receive.")

    line_map = {line.id: line for line in order.lines}
    unknown_line_ids = set(receive_now_inputs) - set(line_map)
    if unknown_line_ids:
        raise PurchaseOrderValidationError("Receive payload contains invalid line references.")

    prepared_lines: list[PreparedPurchaseOrderReceiptLine] = []
    any_received = False
    for line in order.lines:
        receive_now_text = receive_now_inputs.get(line.id, "0")
        receive_now = parse_required_decimal(receive_now_text, f"Receive now for {line.sku_snapshot}")
        if receive_now < ZERO:
            raise PurchaseOrderValidationError("Receive now cannot be negative.")

        ordered_quantity = Decimal(line.quantity or ZERO)
        current_received_quantity = Decimal(line.received_quantity or ZERO)
        new_received_quantity = current_received_quantity + receive_now
        if new_received_quantity > ordered_quantity:
            raise PurchaseOrderValidationError(
                f"Received quantity for {line.sku_snapshot} cannot exceed ordered quantity."
            )
        if receive_now <= ZERO:
            continue

        any_received = True
        product = _resolve_purchase_order_line_product(db, line)
        unit_cost = _validate_purchase_order_line_unit_cost(line)
        prepared_lines.append(
            PreparedPurchaseOrderReceiptLine(
                line=line,
                receive_now=receive_now,
                new_received_quantity=new_received_quantity,
                product=product,
                unit_cost=unit_cost,
            )
        )

    if not any_received:
        raise PurchaseOrderValidationError("Enter a received quantity greater than 0 for at least one line.")
    return prepared_lines


def _resolve_purchase_order_line_product(db: Session, line: PurchaseOrderLine) -> Product:
    sku = (line.sku_snapshot or "").strip()
    if not sku:
        raise PurchaseOrderValidationError("Purchase Order line SKU is missing and cannot be received.")
    product = db.query(Product).filter(Product.sku == sku).one_or_none()
    if product is None:
        raise PurchaseOrderValidationError(f"Purchase Order line SKU {sku} does not exist in Products.")
    return product


def _validate_purchase_order_line_unit_cost(line: PurchaseOrderLine) -> Decimal:
    if line.unit_cost_snapshot is None:
        raise PurchaseOrderValidationError(f"Unit cost for {line.sku_snapshot} is required before receiving.")
    unit_cost = Decimal(line.unit_cost_snapshot)
    if unit_cost < ZERO:
        raise PurchaseOrderValidationError(f"Unit cost for {line.sku_snapshot} cannot be negative.")
    if unit_cost == ZERO:
        raise PurchaseOrderValidationError(f"Unit cost for {line.sku_snapshot} must be greater than 0 before receiving.")
    return unit_cost


def _consume_purchase_order_receive_token(db: Session, order_id: int, receive_token: str) -> None:
    token_value = (receive_token or "").strip()
    if not token_value:
        raise PurchaseOrderValidationError("Receive token is required.")

    used_at = datetime.utcnow()
    updated_rows = (
        db.query(PurchaseOrderReceiveToken)
        .filter(
            PurchaseOrderReceiveToken.purchase_order_id == order_id,
            PurchaseOrderReceiveToken.token == token_value,
            PurchaseOrderReceiveToken.used_at.is_(None),
        )
        .update({PurchaseOrderReceiveToken.used_at: used_at}, synchronize_session=False)
    )
    if updated_rows == 1:
        db.flush()
        return

    token_row = db.query(PurchaseOrderReceiveToken).filter(PurchaseOrderReceiveToken.token == token_value).one_or_none()
    if token_row is None or token_row.purchase_order_id != order_id:
        raise PurchaseOrderValidationError("Receive token is invalid for this Purchase Order.")
    if token_row.used_at is not None:
        raise PurchaseOrderValidationError(
            "This receipt form has already been processed. Reopen Receive to continue."
        )
    raise PurchaseOrderValidationError("Receive token could not be validated.")


def _all_prepared_lines_fully_received(
    order: PurchaseOrder,
    prepared_lines: list[PreparedPurchaseOrderReceiptLine],
) -> bool:
    received_by_line_id = {item.line.id: item.new_received_quantity for item in prepared_lines}
    return all(
        max(
            Decimal(line.quantity or ZERO)
            - Decimal(received_by_line_id.get(line.id, Decimal(line.received_quantity or ZERO))),
            ZERO,
        )
        == ZERO
        for line in order.lines
    )


def _build_line_data(
    supplier_name: str,
    line_inputs: list[dict[str, str]],
    require_at_least_one: bool,
) -> list[dict[str, object]]:
    line_data: list[dict[str, object]] = []
    for line_input in _submitted_line_inputs(line_inputs):
        sku = (line_input.get("sku") or "").strip()
        description = (line_input.get("description") or "").strip()
        if not sku:
            raise PurchaseOrderValidationError("SKU is required.")
        if not description:
            raise PurchaseOrderValidationError("Description is required.")
        quantity = parse_required_decimal(line_input.get("quantity"), "Quantity")
        if quantity <= ZERO:
            raise PurchaseOrderValidationError("Quantity must be greater than 0.")
        unit_cost = parse_required_decimal(line_input.get("unit_cost"), "Unit cost")
        if unit_cost < ZERO:
            raise PurchaseOrderValidationError("Unit cost cannot be negative.")
        line_data.append(
            {
                "sku": sku,
                "description": description,
                "supplier": supplier_name,
                "quantity": quantity,
                "unit_cost": unit_cost,
                "line_total": quantity * unit_cost,
            }
        )
    if require_at_least_one and not line_data:
        raise PurchaseOrderValidationError("Purchase Order must have at least one valid line.")
    return line_data


def _submitted_line_inputs(line_inputs: list[dict[str, str]]) -> list[dict[str, str]]:
    return [
        {
            "sku": (line.get("sku") or "").strip(),
            "description": (line.get("description") or "").strip(),
            "quantity": (line.get("quantity") or "").strip(),
            "unit_cost": (line.get("unit_cost") or "").strip(),
        }
        for line in line_inputs
        if (line.get("sku") or "").strip()
        or (line.get("description") or "").strip()
        or (line.get("quantity") or "").strip()
        or (line.get("unit_cost") or "").strip()
    ]


def _replace_lines(order: PurchaseOrder, line_data: list[dict[str, object]]) -> None:
    order.lines.clear()
    for index, item in enumerate(line_data, start=1):
        order.lines.append(
            PurchaseOrderLine(
                line_number=index,
                sku_snapshot=item["sku"],
                description_snapshot=item["description"],
                supplier_name_snapshot=item["supplier"],
                quantity=item["quantity"],
                received_quantity=ZERO,
                unit_cost_snapshot=item["unit_cost"],
                line_total=item["line_total"],
            )
        )


def _recalculate_order_total(order: PurchaseOrder) -> None:
    order.estimated_total = sum((line.line_total or ZERO for line in order.lines), ZERO)


def _all_lines_fully_received(order: PurchaseOrder) -> bool:
    return all(pending_quantity_for_line(line) == ZERO for line in order.lines)


def _ensure_order_editable(order: PurchaseOrder) -> None:
    if order.status not in EDITABLE_STATUSES:
        raise PurchaseOrderValidationError(f"Purchase orders in status {order.status} are read-only.")


def _generate_purchase_order_number(db: Session) -> str:
    sequence = db.query(AppSequence).filter(AppSequence.name == PURCHASE_ORDER_SEQUENCE_NAME).one_or_none()
    if sequence is None:
        sequence = AppSequence(
            name=PURCHASE_ORDER_SEQUENCE_NAME,
            next_value=_bootstrap_next_purchase_order_sequence(db),
        )
        db.add(sequence)
        db.flush()

    po_number = f"{PURCHASE_ORDER_PREFIX}{sequence.next_value:04d}"
    existing = db.query(PurchaseOrder).filter(PurchaseOrder.po_number == po_number).one_or_none()
    if existing is not None:
        raise PurchaseOrderValidationError(f"Generated purchase order number {po_number} already exists.")
    sequence.next_value += 1
    return po_number


def _bootstrap_next_purchase_order_sequence(db: Session) -> int:
    highest = 0
    numbers = db.query(PurchaseOrder.po_number).all()
    for (po_number,) in numbers:
        if not po_number or not po_number.startswith(PURCHASE_ORDER_PREFIX):
            continue
        suffix = po_number[len(PURCHASE_ORDER_PREFIX):]
        if suffix.isdigit():
            highest = max(highest, int(suffix))
    return highest + 1
