from datetime import date
from decimal import Decimal, InvalidOperation

from sqlalchemy.orm import Session

from app.models import (
    AppSequence,
    B2BCustomer,
    B2BCustomerProduct,
    B2BSalesOrder,
    B2BSalesOrderLine,
)


B2B_SEQUENCE_NAME = "b2b_sales_order"
B2B_ORDER_PREFIX = "B2B"
EDITABLE_STATUSES = {"draft", "in_process"}
ORDER_STATUSES = {"draft", "in_process", "invoiced"}
ALLOWED_STATUS_TRANSITIONS = {
    "draft": {"in_process", "invoiced"},
    "in_process": {"invoiced"},
    "invoiced": set(),
}
ZERO = Decimal("0")


class B2BValidationError(Exception):
    pass


def parse_optional_decimal(value: str | Decimal | None, field_name: str) -> Decimal | None:
    if value is None or value == "":
        return None
    return parse_required_decimal(value, field_name)


def parse_required_decimal(value: str | Decimal | None, field_name: str) -> Decimal:
    if isinstance(value, Decimal):
        return value
    text = (value or "").strip().replace(" ", "").replace(",", ".")
    try:
        return Decimal(text)
    except InvalidOperation as exc:
        raise B2BValidationError(f"{field_name} must be a valid number.") from exc


def validate_future_delivery_date(delivery_date: date) -> None:
    if delivery_date <= date.today():
        raise B2BValidationError("Delivery date must be a future date.")


def create_customer(
    db: Session,
    customer_name: str,
    address: str,
    province: str,
    canton: str,
    district: str,
    legal_name: str,
    legal_id: str,
    phone: str,
    loyverse_customer_id: str,
    active: bool,
) -> B2BCustomer:
    customer = B2BCustomer()
    _assign_customer_fields(
        customer,
        customer_name,
        address,
        province,
        canton,
        district,
        legal_name,
        legal_id,
        phone,
        loyverse_customer_id,
        active,
    )
    db.add(customer)
    db.commit()
    db.refresh(customer)
    return customer


def update_customer(
    db: Session,
    customer_id: int,
    customer_name: str,
    address: str,
    province: str,
    canton: str,
    district: str,
    legal_name: str,
    legal_id: str,
    phone: str,
    loyverse_customer_id: str,
    active: bool,
) -> B2BCustomer:
    customer = db.query(B2BCustomer).filter(B2BCustomer.id == customer_id).one()
    _assign_customer_fields(
        customer,
        customer_name,
        address,
        province,
        canton,
        district,
        legal_name,
        legal_id,
        phone,
        loyverse_customer_id,
        active,
    )
    db.commit()
    db.refresh(customer)
    return customer


def _assign_customer_fields(
    customer: B2BCustomer,
    customer_name: str,
    address: str,
    province: str,
    canton: str,
    district: str,
    legal_name: str,
    legal_id: str,
    phone: str,
    loyverse_customer_id: str,
    active: bool,
) -> None:
    name = customer_name.strip()
    if not name:
        raise B2BValidationError("Customer name is required.")
    customer.customer_name = name
    customer.address = address.strip() or None
    customer.province = province.strip() or None
    customer.canton = canton.strip() or None
    customer.district = district.strip() or None
    customer.legal_name = legal_name.strip() or None
    customer.legal_id = legal_id.strip() or None
    customer.phone = phone.strip() or None
    customer.loyverse_customer_id = loyverse_customer_id.strip() or None
    customer.active = active


def add_customer_product(
    db: Session,
    customer_id: int,
    sku: str,
    description: str,
    distributor_price: str,
    active: bool,
) -> B2BCustomerProduct:
    customer = db.query(B2BCustomer).filter(B2BCustomer.id == customer_id).one()
    product_sku = sku.strip()
    if not product_sku:
        raise B2BValidationError("SKU is required.")
    existing = (
        db.query(B2BCustomerProduct)
        .filter(B2BCustomerProduct.customer_id == customer.id, B2BCustomerProduct.sku == product_sku)
        .one_or_none()
    )
    if existing is not None:
        raise B2BValidationError("This customer already has that SKU in the catalog.")

    product = B2BCustomerProduct(customer_id=customer.id)
    _assign_customer_product_fields(product, product_sku, description, distributor_price, active)
    db.add(product)
    db.commit()
    db.refresh(product)
    return product


def update_customer_product(
    db: Session,
    customer_id: int,
    product_line_id: int,
    description: str,
    distributor_price: str,
    active: bool,
) -> B2BCustomerProduct:
    product = (
        db.query(B2BCustomerProduct)
        .filter(B2BCustomerProduct.customer_id == customer_id, B2BCustomerProduct.id == product_line_id)
        .one()
    )
    _assign_customer_product_fields(product, product.sku, description, distributor_price, active)
    db.commit()
    db.refresh(product)
    return product


def _assign_customer_product_fields(
    product: B2BCustomerProduct,
    sku: str,
    description: str,
    distributor_price: str,
    active: bool,
) -> None:
    desc = description.strip()
    if not desc:
        raise B2BValidationError("Description is required.")
    price = parse_required_decimal(distributor_price, "Distributor price")
    if price < ZERO:
        raise B2BValidationError("Distributor price cannot be negative.")
    product.sku = sku.strip()
    product.description = desc
    product.distributor_price = price
    product.active = active


def create_sales_order(
    db: Session,
    customer_id: int,
    delivery_date: date,
    line_inputs: list[dict[str, str]],
    observations: str,
) -> B2BSalesOrder:
    validate_future_delivery_date(delivery_date)
    customer = db.query(B2BCustomer).filter(B2BCustomer.id == customer_id).one_or_none()
    if customer is None or not customer.active:
        raise B2BValidationError("Active customer is required.")

    line_data = _build_line_data(db, customer.id, line_inputs, require_at_least_one=True)
    order = B2BSalesOrder(
        order_number=_generate_b2b_order_number(db),
        customer_id=customer.id,
        customer_name_snapshot=customer.customer_name,
        address_snapshot=customer.address,
        province_snapshot=customer.province,
        canton_snapshot=customer.canton,
        district_snapshot=customer.district,
        legal_name_snapshot=customer.legal_name,
        legal_id_snapshot=customer.legal_id,
        phone_snapshot=customer.phone,
        loyverse_customer_id_snapshot=customer.loyverse_customer_id,
        delivery_date=delivery_date,
        status="draft",
        total_amount=ZERO,
        observations=observations.strip() or None,
    )
    db.add(order)
    db.flush()
    _replace_lines(order, line_data)
    _recalculate_order_total(order)
    db.commit()
    db.refresh(order)
    return order


def update_sales_order_lines(
    db: Session,
    order_id: int,
    line_updates: list[dict[str, str]],
    deleted_line_ids: list[int],
    new_line_inputs: list[dict[str, str]],
    observations: str,
) -> B2BSalesOrder:
    order = db.query(B2BSalesOrder).filter(B2BSalesOrder.id == order_id).one()
    _ensure_order_editable(order)
    order.observations = observations.strip() or None
    catalog_by_sku = _active_customer_catalog_by_sku(db, order.customer_id)

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
            raise B2BValidationError("Quantity must be greater than 0.")
        sku = (update.get("sku") or "").strip()
        if sku != line.sku_snapshot:
            catalog_product = _catalog_product_for_sku(catalog_by_sku, sku)
            _assign_line_from_catalog(line, catalog_product, quantity)
        else:
            line.quantity = quantity
            line.line_total = line.unit_price_snapshot * quantity

    next_line_number = len([line for line in order.lines if line.id not in deleted_ids]) + 1
    for line_input in _submitted_line_inputs(new_line_inputs):
        catalog_product = _catalog_product_for_sku(catalog_by_sku, line_input["sku"])
        quantity = parse_required_decimal(line_input["quantity"], "Quantity")
        if quantity <= ZERO:
            raise B2BValidationError("Quantity must be greater than 0.")
        line = B2BSalesOrderLine(line_number=next_line_number)
        _assign_line_from_catalog(line, catalog_product, quantity)
        order.lines.append(line)
        next_line_number += 1

    db.flush()
    remaining_lines = (
        db.query(B2BSalesOrderLine)
        .filter(B2BSalesOrderLine.sales_order_id == order.id)
        .order_by(B2BSalesOrderLine.line_number, B2BSalesOrderLine.id)
        .all()
    )
    if not remaining_lines:
        raise B2BValidationError("Order must have at least one line.")
    _renumber_lines(remaining_lines)
    order.total_amount = sum((line.line_total or ZERO for line in remaining_lines), ZERO)
    db.commit()
    db.refresh(order)
    return order


def change_sales_order_status(db: Session, order_id: int, new_status: str) -> B2BSalesOrder:
    order = db.query(B2BSalesOrder).filter(B2BSalesOrder.id == order_id).one()
    if new_status not in ORDER_STATUSES:
        raise B2BValidationError("Invalid status.")
    if new_status not in ALLOWED_STATUS_TRANSITIONS[order.status]:
        raise B2BValidationError(f"Cannot change status from {order.status} to {new_status}.")
    order.status = new_status
    db.commit()
    db.refresh(order)
    return order


def _build_line_data(
    db: Session,
    customer_id: int,
    line_inputs: list[dict[str, str]],
    require_at_least_one: bool,
) -> list[tuple[B2BCustomerProduct, Decimal]]:
    catalog_by_sku = _active_customer_catalog_by_sku(db, customer_id)
    line_data = []
    for line_input in _submitted_line_inputs(line_inputs):
        catalog_product = _catalog_product_for_sku(catalog_by_sku, line_input["sku"])
        quantity = parse_required_decimal(line_input["quantity"], "Quantity")
        if quantity <= ZERO:
            raise B2BValidationError("Quantity must be greater than 0.")
        line_data.append((catalog_product, quantity))
    if require_at_least_one and not line_data:
        raise B2BValidationError("At least one valid order line is required.")
    return line_data


def _submitted_line_inputs(line_inputs: list[dict[str, str]]) -> list[dict[str, str]]:
    return [
        {"sku": (line.get("sku") or "").strip(), "quantity": (line.get("quantity") or "").strip()}
        for line in line_inputs
        if (line.get("sku") or "").strip() or (line.get("quantity") or "").strip()
    ]


def _active_customer_catalog_by_sku(db: Session, customer_id: int) -> dict[str, B2BCustomerProduct]:
    catalog = (
        db.query(B2BCustomerProduct)
        .filter(B2BCustomerProduct.customer_id == customer_id, B2BCustomerProduct.active.is_(True))
        .all()
    )
    return {item.sku: item for item in catalog}


def _catalog_product_for_sku(
    catalog_by_sku: dict[str, B2BCustomerProduct],
    sku: str,
) -> B2BCustomerProduct:
    product = catalog_by_sku.get((sku or "").strip())
    if product is None:
        raise B2BValidationError("Selected product is not active for this customer.")
    return product


def _replace_lines(order: B2BSalesOrder, line_data: list[tuple[B2BCustomerProduct, Decimal]]) -> None:
    order.lines.clear()
    for index, (catalog_product, quantity) in enumerate(line_data, start=1):
        line = B2BSalesOrderLine(line_number=index)
        _assign_line_from_catalog(line, catalog_product, quantity)
        order.lines.append(line)


def _assign_line_from_catalog(
    line: B2BSalesOrderLine,
    catalog_product: B2BCustomerProduct,
    quantity: Decimal,
) -> None:
    line.sku_snapshot = catalog_product.sku
    line.description_snapshot = catalog_product.description
    line.unit_price_snapshot = catalog_product.distributor_price
    line.quantity = quantity
    line.line_total = catalog_product.distributor_price * quantity


def _renumber_lines(lines: list[B2BSalesOrderLine]) -> None:
    for index, line in enumerate(lines, start=1):
        line.line_number = index


def _recalculate_order_total(order: B2BSalesOrder) -> None:
    order.total_amount = sum((line.line_total or ZERO for line in order.lines), ZERO)


def _ensure_order_editable(order: B2BSalesOrder) -> None:
    if order.status not in EDITABLE_STATUSES:
        raise B2BValidationError("Invoiced orders are read-only.")


def _generate_b2b_order_number(db: Session) -> str:
    sequence = db.query(AppSequence).filter(AppSequence.name == B2B_SEQUENCE_NAME).one_or_none()
    if sequence is None:
        sequence = AppSequence(
            name=B2B_SEQUENCE_NAME,
            next_value=_bootstrap_next_b2b_order_sequence(db),
        )
        db.add(sequence)
        db.flush()

    order_number = f"{B2B_ORDER_PREFIX}{sequence.next_value:04d}"
    existing_order = db.query(B2BSalesOrder).filter(B2BSalesOrder.order_number == order_number).one_or_none()
    if existing_order is not None:
        raise B2BValidationError(f"Generated B2B order number {order_number} already exists.")
    sequence.next_value += 1
    return order_number


def _bootstrap_next_b2b_order_sequence(db: Session) -> int:
    highest = 0
    order_numbers = db.query(B2BSalesOrder.order_number).all()
    for (order_number,) in order_numbers:
        if not order_number or not order_number.startswith(B2B_ORDER_PREFIX):
            continue
        suffix = order_number[len(B2B_ORDER_PREFIX):]
        if suffix.isdigit():
            highest = max(highest, int(suffix))
    return highest + 1
