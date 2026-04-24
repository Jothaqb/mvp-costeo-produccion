from datetime import date
from decimal import Decimal, InvalidOperation

from sqlalchemy.orm import Session

from app.models import AppSequence, Product, PurchaseOrder, PurchaseOrderLine


PURCHASE_ORDER_SEQUENCE_NAME = "purchase_order"
PURCHASE_ORDER_PREFIX = "PO"
PURCHASE_ORDER_STATUSES = {"draft", "issued"}
EDITABLE_STATUSES = {"draft"}
ZERO = Decimal("0")


class PurchaseOrderValidationError(Exception):
    pass


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
    normalized_status = _normalize_status(status)
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
    normalized_status = _normalize_status(status)
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
                unit_cost_snapshot=item["unit_cost"],
                line_total=item["line_total"],
            )
        )


def _recalculate_order_total(order: PurchaseOrder) -> None:
    order.estimated_total = sum((line.line_total or ZERO for line in order.lines), ZERO)


def _ensure_order_editable(order: PurchaseOrder) -> None:
    if order.status not in EDITABLE_STATUSES:
        raise PurchaseOrderValidationError("Issued purchase orders are read-only.")


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
