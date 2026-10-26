from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import hashlib
import hmac
import re

from sqlalchemy import text
from sqlalchemy.orm import Session, joinedload

from app.models import (
    AppSequence,
    B2CCustomer,
    B2CSalesOrder,
    B2CSalesOrderLine,
    Channel,
    DiscountRule,
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
B2C_ORDER_NUMBER_PATTERN = re.compile(r"^B2C(\d+)$")
EDITABLE_STATUSES = {"draft"}
ORDER_STATUSES = {"draft", "invoiced", "cancelled"}
ALLOWED_STATUS_TRANSITIONS = {
    "draft": {"invoiced", "cancelled"},
    "invoiced": set(),
    "cancelled": set(),
}
ZERO = Decimal("0")
SNAPSHOT_QUANT = Decimal("0.0001")
PRODUCT_LINE_TYPE = "product"
SHIPPING_LINE_TYPE = "shipping"
AUTHORIZED_SHIPPING_SKUS = frozenset({"10219", "10258"})
_B2C_SEQUENCE_ADVISORY_LOCK_KEY = 71822001
_B2C_SHIPPING_COST_CSRF_PURPOSE = b"green-corner:b2c:shipping-cost:post:v1"


class B2CValidationError(Exception):
    pass


def build_b2c_shipping_cost_csrf_token(session_token: str | None) -> str:
    normalized_session_token = (session_token or "").strip()
    if not normalized_session_token:
        raise B2CValidationError("No valid session is available for CSRF protection.")
    return hmac.new(
        normalized_session_token.encode("utf-8"),
        _B2C_SHIPPING_COST_CSRF_PURPOSE,
        hashlib.sha256,
    ).hexdigest()


def validate_b2c_shipping_cost_csrf_token(
    session_token: str | None,
    submitted_token: str | None,
) -> None:
    expected = build_b2c_shipping_cost_csrf_token(session_token)
    submitted = (submitted_token or "").strip()
    if not submitted or not hmac.compare_digest(submitted, expected):
        raise B2CValidationError("Invalid CSRF token for shipping cost update.")


@dataclass(frozen=True)
class PreparedB2CInvoiceLine:
    line: B2CSalesOrderLine
    product: Product
    quantity: Decimal


@dataclass(frozen=True)
class B2COrderMarginSummary:
    product_net_sales: Decimal
    shipping_net_sales: Decimal
    total_net_sales: Decimal
    product_cost: Decimal | None
    product_margin_amount: Decimal | None
    product_margin_percent: Decimal | None
    shipping_cost: Decimal | None
    total_margin_amount: Decimal | None
    total_margin_percent: Decimal | None
    total_margin_pending: bool


def parse_required_decimal(value: str | Decimal | None, field_name: str) -> Decimal:
    if isinstance(value, Decimal):
        return value
    text = (value or "").strip().replace(" ", "").replace(",", ".")
    try:
        return Decimal(text)
    except InvalidOperation as exc:
        raise B2CValidationError(f"{field_name} must be a valid number.") from exc


def create_b2c_sales_order_in_session(
    db: Session,
    *,
    order_date: date,
    b2c_customer_id: str,
    customer_name: str,
    customer_phone: str,
    customer_email: str,
    customer_address_snapshot: str,
    province_snapshot: str,
    canton_snapshot: str,
    district_snapshot: str,
    customer_observations_snapshot: str,
    channel_id: str,
    discount_rule_id: str,
    observations: str,
    line_inputs: list[dict[str, str]],
) -> B2CSalesOrder:
    selected_channel = _resolve_b2c_channel(db, channel_id)
    line_data = _build_b2c_line_data(db, line_inputs, require_at_least_one=True)
    discount_rule = _resolve_b2c_discount_rule(db, discount_rule_id)
    customer = _resolve_b2c_customer(db, b2c_customer_id)
    order = B2CSalesOrder(
        order_number=_generate_b2c_order_number(db),
        order_date=order_date,
        b2c_customer_id=customer.id if customer is not None else None,
        customer_name=_clean_optional_text(customer_name),
        customer_phone=_clean_optional_text(customer_phone),
        customer_email=_clean_optional_text(customer_email),
        customer_address_snapshot=_clean_optional_text(customer_address_snapshot),
        province_snapshot=_clean_optional_text(province_snapshot),
        canton_snapshot=_clean_optional_text(canton_snapshot),
        district_snapshot=_clean_optional_text(district_snapshot),
        customer_observations_snapshot=_clean_optional_text(customer_observations_snapshot),
        channel=selected_channel.name,
        channel_id=selected_channel.id,
        status="draft",
        discount_amount=ZERO,
        subtotal_amount=ZERO,
        total_amount=ZERO,
        observations=_clean_optional_text(observations),
    )
    db.add(order)
    db.flush()
    _replace_lines(order, line_data)
    _apply_discount_rule_snapshot(order, discount_rule)
    _recalculate_b2c_order_total(order)
    db.flush()
    return order


def create_b2c_sales_order(
    db: Session,
    *,
    order_date: date,
    b2c_customer_id: str,
    customer_name: str,
    customer_phone: str,
    customer_email: str,
    customer_address_snapshot: str,
    province_snapshot: str,
    canton_snapshot: str,
    district_snapshot: str,
    customer_observations_snapshot: str,
    channel_id: str,
    discount_rule_id: str,
    observations: str,
    line_inputs: list[dict[str, str]],
) -> B2CSalesOrder:
    order = create_b2c_sales_order_in_session(
        db,
        order_date=order_date,
        b2c_customer_id=b2c_customer_id,
        customer_name=customer_name,
        customer_phone=customer_phone,
        customer_email=customer_email,
        customer_address_snapshot=customer_address_snapshot,
        province_snapshot=province_snapshot,
        canton_snapshot=canton_snapshot,
        district_snapshot=district_snapshot,
        customer_observations_snapshot=customer_observations_snapshot,
        channel_id=channel_id,
        discount_rule_id=discount_rule_id,
        observations=observations,
        line_inputs=line_inputs,
    )
    db.commit()
    db.refresh(order)
    return order


def update_b2c_sales_order(
    db: Session,
    *,
    order_id: int,
    order_date: date,
    b2c_customer_id: str,
    customer_name: str,
    customer_phone: str,
    customer_email: str,
    customer_address_snapshot: str,
    province_snapshot: str,
    canton_snapshot: str,
    district_snapshot: str,
    customer_observations_snapshot: str,
    channel_id: str,
    discount_rule_id: str,
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
    customer = _resolve_b2c_customer(db, b2c_customer_id, allow_inactive_customer_id=order.b2c_customer_id)
    order.order_date = order_date
    order.b2c_customer_id = customer.id if customer is not None else None
    order.customer_name = _clean_optional_text(customer_name)
    order.customer_phone = _clean_optional_text(customer_phone)
    order.customer_email = _clean_optional_text(customer_email)
    order.customer_address_snapshot = _clean_optional_text(customer_address_snapshot)
    order.province_snapshot = _clean_optional_text(province_snapshot)
    order.canton_snapshot = _clean_optional_text(canton_snapshot)
    order.district_snapshot = _clean_optional_text(district_snapshot)
    order.customer_observations_snapshot = _clean_optional_text(customer_observations_snapshot)
    selected_channel = _resolve_b2c_channel(
        db,
        channel_id,
        allow_inactive_channel_id=order.channel_id,
    )
    order.channel_id = selected_channel.id
    order.channel = selected_channel.name
    order.observations = _clean_optional_text(observations)
    discount_rule = _resolve_b2c_discount_rule(
        db,
        discount_rule_id,
        allow_inactive_rule_id=order.discount_rule_id,
    )

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
    _apply_discount_rule_snapshot(order, discount_rule)
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

        _snapshot_b2c_order_invoice_margin(order)
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
    validate_b2c_shipping_lines(order)
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
    line_type = normalized_b2c_line_type(prepared_line.line)
    if line_type == SHIPPING_LINE_TYPE:
        validate_b2c_shipping_line(prepared_line.line)
        # A shipping line has zero *product* COGS. Its actual carrier cost is stored
        # separately on the order and must never be inferred from Product inventory.
        # Persisting zero here keeps legacy accounting COGS coverage complete without
        # treating this value as the logistical cost.
        prepared_line.line.cost_unit_snapshot = ZERO.quantize(SNAPSHOT_QUANT)
        prepared_line.line.cost_total_snapshot = ZERO.quantize(SNAPSHOT_QUANT)
        prepared_line.line.gross_margin_amount = None
        prepared_line.line.gross_margin_percent = None
        return

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
    net_line_total = _net_line_total_for_margin(prepared_line.line)
    gross_margin_amount = (net_line_total - cost_total_snapshot).quantize(SNAPSHOT_QUANT)
    gross_margin_percent = (
        (gross_margin_amount / net_line_total).quantize(SNAPSHOT_QUANT)
        if net_line_total > ZERO
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
    line.line_type = line_type_for_new_b2c_line(product.sku)
    line.sku_snapshot = product.sku
    line.description_snapshot = product.name
    line.quantity = quantity
    line.unit_price_snapshot = unit_price
    line.line_total = (quantity * unit_price).quantize(SNAPSHOT_QUANT)
    line.discount_amount_snapshot = ZERO
    line.net_line_total_snapshot = line.line_total


def line_type_for_new_b2c_line(sku: str | None) -> str:
    return SHIPPING_LINE_TYPE if (sku or "").strip() in AUTHORIZED_SHIPPING_SKUS else PRODUCT_LINE_TYPE


def normalized_b2c_line_type(line: B2CSalesOrderLine) -> str:
    normalized = (line.line_type or PRODUCT_LINE_TYPE).strip().lower()
    if normalized not in {PRODUCT_LINE_TYPE, SHIPPING_LINE_TYPE}:
        raise B2CValidationError("B2C line type is invalid.")
    return normalized


def validate_b2c_shipping_line(line: B2CSalesOrderLine) -> None:
    if normalized_b2c_line_type(line) != SHIPPING_LINE_TYPE:
        return
    sku = (line.sku_snapshot or "").strip()
    if sku not in AUTHORIZED_SHIPPING_SKUS:
        raise B2CValidationError(
            "Only authorized shipping SKUs 10219 and 10258 may be classified as shipping."
        )
    quantity = parse_required_decimal(line.quantity, "Shipping quantity")
    if quantity != Decimal("1"):
        raise B2CValidationError("Shipping lines must have quantity exactly 1.")


def validate_b2c_shipping_lines(order: B2CSalesOrder) -> None:
    shipping_lines = [
        line for line in order.lines if normalized_b2c_line_type(line) == SHIPPING_LINE_TYPE
    ]
    if len(shipping_lines) > 1:
        raise B2CValidationError("A B2C order may contain at most one shipping line.")
    for line in shipping_lines:
        validate_b2c_shipping_line(line)


def _renumber_lines(lines: list[B2CSalesOrderLine]) -> None:
    for index, line in enumerate(lines, start=1):
        line.line_number = index


def _recalculate_b2c_order_total(order: B2CSalesOrder) -> None:
    validate_b2c_shipping_lines(order)
    subtotal = sum((line.line_total or ZERO for line in order.lines), ZERO).quantize(SNAPSHOT_QUANT)
    order.subtotal_amount = subtotal
    product_subtotal = sum(
        (
            (line.line_total or ZERO)
            for line in order.lines
            if normalized_b2c_line_type(line) == PRODUCT_LINE_TYPE
        ),
        ZERO,
    ).quantize(SNAPSHOT_QUANT)
    discount_value = order.discount_value_snapshot if order.discount_value_snapshot is not None else ZERO
    product_discount_amount = ZERO
    if product_subtotal > ZERO and discount_value > ZERO:
        product_discount_amount = (product_subtotal * discount_value).quantize(SNAPSHOT_QUANT)
        if product_discount_amount > product_subtotal:
            product_discount_amount = product_subtotal
    _allocate_discount_across_lines(order, product_subtotal, product_discount_amount)
    recalculate_b2c_order_totals_from_line_snapshots(order)


def _ensure_b2c_order_editable(order: B2CSalesOrder) -> None:
    if order.status not in EDITABLE_STATUSES:
        raise B2CValidationError("Only draft B2C orders are editable.")


def _resolve_b2c_discount_rule(
    db: Session,
    raw_discount_rule_id: str | int | None,
    *,
    allow_inactive_rule_id: int | None = None,
) -> DiscountRule | None:
    text = str(raw_discount_rule_id or "").strip()
    if not text:
        return None
    if not text.isdigit():
        raise B2CValidationError("Selected discount is invalid.")
    discount_rule_id = int(text)
    discount_rule = db.query(DiscountRule).filter(DiscountRule.id == discount_rule_id).one_or_none()
    if discount_rule is None:
        raise B2CValidationError("Selected discount does not exist.")
    if (
        not discount_rule.active
        and (allow_inactive_rule_id is None or discount_rule.id != allow_inactive_rule_id)
    ):
        raise B2CValidationError("Selected discount is not active.")
    if discount_rule.channel != "b2c" or discount_rule.applies_to != "order_total":
        raise B2CValidationError("Selected discount is not valid for B2C order totals.")
    if discount_rule.discount_type != "percentage":
        raise B2CValidationError("Selected discount type is not supported.")
    return discount_rule


def _resolve_b2c_customer(
    db: Session,
    raw_b2c_customer_id: str | int | None,
    *,
    allow_inactive_customer_id: int | None = None,
) -> B2CCustomer | None:
    text = str(raw_b2c_customer_id or "").strip()
    if not text:
        return None
    if not text.isdigit():
        raise B2CValidationError("Selected B2C customer is invalid.")
    customer_id = int(text)
    customer = db.query(B2CCustomer).filter(B2CCustomer.id == customer_id).one_or_none()
    if customer is None:
        raise B2CValidationError("Selected B2C customer does not exist.")
    if not customer.active and customer.id != allow_inactive_customer_id:
        raise B2CValidationError("Selected B2C customer is not active.")
    return customer


def _apply_discount_rule_snapshot(order: B2CSalesOrder, discount_rule: DiscountRule | None) -> None:
    if discount_rule is None:
        order.discount_rule_id = None
        order.discount_name_snapshot = None
        order.discount_type_snapshot = None
        order.discount_value_snapshot = None
        return

    order.discount_rule_id = discount_rule.id
    order.discount_name_snapshot = discount_rule.name
    order.discount_type_snapshot = discount_rule.discount_type
    order.discount_value_snapshot = discount_rule.value.quantize(SNAPSHOT_QUANT)


def _allocate_discount_across_lines(
    order: B2CSalesOrder,
    subtotal: Decimal,
    discount_amount: Decimal,
) -> None:
    positive_lines = [
        line
        for line in order.lines
        if normalized_b2c_line_type(line) == PRODUCT_LINE_TYPE and (line.line_total or ZERO) > ZERO
    ]
    if subtotal <= ZERO or discount_amount <= ZERO or not positive_lines:
        for line in order.lines:
            if normalized_b2c_line_type(line) == SHIPPING_LINE_TYPE:
                _validate_existing_line_discount(line)
                continue
            gross_line_total = (line.line_total or ZERO).quantize(SNAPSHOT_QUANT)
            line.discount_amount_snapshot = ZERO
            line.net_line_total_snapshot = gross_line_total
        return

    allocated = ZERO
    last_positive_line = positive_lines[-1]
    for line in order.lines:
        gross_line_total = (line.line_total or ZERO).quantize(SNAPSHOT_QUANT)
        if normalized_b2c_line_type(line) == SHIPPING_LINE_TYPE:
            _validate_existing_line_discount(line)
            continue
        if gross_line_total <= ZERO:
            line.discount_amount_snapshot = ZERO
            line.net_line_total_snapshot = gross_line_total
            continue

        if line is last_positive_line:
            line_discount_amount = (discount_amount - allocated).quantize(SNAPSHOT_QUANT)
        else:
            line_discount_amount = ((gross_line_total / subtotal) * discount_amount).quantize(SNAPSHOT_QUANT)
            remaining_discount = (discount_amount - allocated).quantize(SNAPSHOT_QUANT)
            if line_discount_amount > remaining_discount:
                line_discount_amount = remaining_discount
            allocated += line_discount_amount

        line.discount_amount_snapshot = line_discount_amount
        line.net_line_total_snapshot = (gross_line_total - line_discount_amount).quantize(SNAPSHOT_QUANT)


def _validate_existing_line_discount(line: B2CSalesOrderLine) -> None:
    gross_line_total = parse_required_decimal(line.line_total, "Line total").quantize(SNAPSHOT_QUANT)
    discount_amount = parse_required_decimal(
        line.discount_amount_snapshot or ZERO,
        "Line discount",
    ).quantize(SNAPSHOT_QUANT)
    if discount_amount < ZERO or discount_amount > gross_line_total:
        raise B2CValidationError("Line discount must be between zero and the gross line total.")
    line.discount_amount_snapshot = discount_amount
    line.net_line_total_snapshot = (gross_line_total - discount_amount).quantize(SNAPSHOT_QUANT)


def set_b2c_line_pricing_snapshots(
    line: B2CSalesOrderLine,
    *,
    gross_amount: Decimal,
    discount_amount: Decimal,
) -> None:
    gross = parse_required_decimal(gross_amount, "Gross line amount").quantize(SNAPSHOT_QUANT)
    discount = parse_required_decimal(discount_amount, "Line discount").quantize(SNAPSHOT_QUANT)
    if gross < ZERO:
        raise B2CValidationError("Gross line amount cannot be negative.")
    if discount < ZERO or discount > gross:
        raise B2CValidationError("Line discount must be between zero and the gross line total.")
    line.line_total = gross
    line.discount_amount_snapshot = discount
    line.net_line_total_snapshot = (gross - discount).quantize(SNAPSHOT_QUANT)


def recalculate_b2c_order_totals_from_line_snapshots(order: B2CSalesOrder) -> None:
    validate_b2c_shipping_lines(order)
    subtotal = ZERO
    discount_total = ZERO
    net_total = ZERO
    for line in order.lines:
        gross = parse_required_decimal(line.line_total, "Line total").quantize(SNAPSHOT_QUANT)
        discount = parse_required_decimal(
            line.discount_amount_snapshot or ZERO,
            "Line discount",
        ).quantize(SNAPSHOT_QUANT)
        if discount < ZERO or discount > gross:
            raise B2CValidationError("Line discount must be between zero and the gross line total.")
        expected_net = (gross - discount).quantize(SNAPSHOT_QUANT)
        line.net_line_total_snapshot = expected_net
        subtotal += gross
        discount_total += discount
        net_total += expected_net
    order.subtotal_amount = subtotal.quantize(SNAPSHOT_QUANT)
    order.discount_amount = discount_total.quantize(SNAPSHOT_QUANT)
    order.total_amount = net_total.quantize(SNAPSHOT_QUANT)


def _net_line_total_for_margin(line: B2CSalesOrderLine) -> Decimal:
    if line.net_line_total_snapshot is not None:
        return parse_required_decimal(line.net_line_total_snapshot, "Net line total").quantize(SNAPSHOT_QUANT)
    return parse_required_decimal(line.line_total, "Line total").quantize(SNAPSHOT_QUANT)


def _snapshot_b2c_order_invoice_margin(order: B2CSalesOrder) -> None:
    product_lines = [
        line for line in order.lines if normalized_b2c_line_type(line) == PRODUCT_LINE_TYPE
    ]
    cost_total_snapshot = sum(
        (line.cost_total_snapshot or ZERO for line in product_lines),
        ZERO,
    ).quantize(SNAPSHOT_QUANT)
    product_net_sales = sum(
        (_net_line_total_for_margin(line) for line in product_lines),
        ZERO,
    ).quantize(SNAPSHOT_QUANT)
    gross_margin_amount = (product_net_sales - cost_total_snapshot).quantize(SNAPSHOT_QUANT)
    gross_margin_percent = (
        (gross_margin_amount / product_net_sales).quantize(SNAPSHOT_QUANT)
        if product_net_sales > ZERO
        else None
    )
    order.cost_total_snapshot = cost_total_snapshot
    order.gross_margin_amount = gross_margin_amount
    order.gross_margin_percent = gross_margin_percent


def calculate_b2c_order_margins(order: B2CSalesOrder) -> B2COrderMarginSummary:
    product_lines = [
        line for line in order.lines if normalized_b2c_line_type(line) == PRODUCT_LINE_TYPE
    ]
    shipping_lines = [
        line for line in order.lines if normalized_b2c_line_type(line) == SHIPPING_LINE_TYPE
    ]
    product_net_sales = sum(
        (_net_line_total_for_margin(line) for line in product_lines),
        ZERO,
    ).quantize(SNAPSHOT_QUANT)
    shipping_net_sales = sum(
        (_net_line_total_for_margin(line) for line in shipping_lines),
        ZERO,
    ).quantize(SNAPSHOT_QUANT)
    total_net_sales = (product_net_sales + shipping_net_sales).quantize(SNAPSHOT_QUANT)
    has_complete_product_cost = all(line.cost_total_snapshot is not None for line in product_lines)
    product_cost = None
    product_margin_amount = None
    product_margin_percent = None
    if has_complete_product_cost:
        product_cost = sum(
            (parse_required_decimal(line.cost_total_snapshot, "Product cost") for line in product_lines),
            ZERO,
        ).quantize(SNAPSHOT_QUANT)
        product_margin_amount = (product_net_sales - product_cost).quantize(SNAPSHOT_QUANT)
        if product_net_sales > ZERO:
            product_margin_percent = (product_margin_amount / product_net_sales).quantize(SNAPSHOT_QUANT)

    shipping_cost = (
        parse_required_decimal(order.shipping_cost_amount, "Shipping cost").quantize(SNAPSHOT_QUANT)
        if order.shipping_cost_amount is not None
        else None
    )
    total_margin_amount = None
    total_margin_percent = None
    if product_cost is not None and shipping_cost is not None:
        total_margin_amount = (total_net_sales - product_cost - shipping_cost).quantize(SNAPSHOT_QUANT)
        if total_net_sales > ZERO:
            total_margin_percent = (total_margin_amount / total_net_sales).quantize(SNAPSHOT_QUANT)

    return B2COrderMarginSummary(
        product_net_sales=product_net_sales,
        shipping_net_sales=shipping_net_sales,
        total_net_sales=total_net_sales,
        product_cost=product_cost,
        product_margin_amount=product_margin_amount,
        product_margin_percent=product_margin_percent,
        shipping_cost=shipping_cost,
        total_margin_amount=total_margin_amount,
        total_margin_percent=total_margin_percent,
        total_margin_pending=total_margin_amount is None,
    )


def record_b2c_shipping_cost_in_session(
    db: Session,
    *,
    order_id: int,
    amount: Decimal,
    source: str,
    user_id: int,
) -> B2CSalesOrder:
    order = (
        db.query(B2CSalesOrder)
        .options(joinedload(B2CSalesOrder.lines))
        .filter(B2CSalesOrder.id == order_id)
        .with_for_update(of=B2CSalesOrder)
        .populate_existing()
        .one()
    )
    if not any(normalized_b2c_line_type(line) == SHIPPING_LINE_TYPE for line in order.lines):
        raise B2CValidationError("This B2C order has no shipping line.")
    raw_amount = parse_required_decimal(amount, "Shipping cost")
    if not raw_amount.is_finite():
        raise B2CValidationError("Shipping cost must be a finite number.")
    parsed_amount = raw_amount.quantize(SNAPSHOT_QUANT)
    if parsed_amount < ZERO:
        raise B2CValidationError("Shipping cost cannot be negative.")
    normalized_source = (source or "").strip()
    if not normalized_source:
        raise B2CValidationError("Shipping cost source is required.")
    order.shipping_cost_amount = parsed_amount
    order.shipping_cost_source = normalized_source[:100]
    order.shipping_cost_recorded_at = datetime.utcnow()
    order.shipping_cost_recorded_by_user_id = user_id
    db.flush()
    return order


def _generate_b2c_order_number(db: Session) -> str:
    bind = db.get_bind()
    if bind.dialect.name == "postgresql":
        db.execute(
            text("SELECT pg_advisory_xact_lock(:lock_key)"),
            {"lock_key": _B2C_SEQUENCE_ADVISORY_LOCK_KEY},
        )
    sequence_query = db.query(AppSequence).filter(AppSequence.name == B2C_SEQUENCE_NAME)
    if bind.dialect.name == "postgresql":
        sequence_query = sequence_query.with_for_update()
    sequence = sequence_query.one_or_none()
    if sequence is None:
        sequence = AppSequence(
            name=B2C_SEQUENCE_NAME,
            next_value=_bootstrap_next_b2c_order_sequence(db),
        )
        db.add(sequence)
        db.flush()

    candidate_value = max(sequence.next_value, _bootstrap_next_b2c_order_sequence(db))
    while True:
        order_number = f"{B2C_ORDER_PREFIX}{candidate_value:04d}"
        existing_order = db.query(B2CSalesOrder).filter(B2CSalesOrder.order_number == order_number).one_or_none()
        if existing_order is None:
            break
        candidate_value += 1
    sequence.next_value = candidate_value + 1
    return order_number


def _bootstrap_next_b2c_order_sequence(db: Session) -> int:
    highest = 0
    order_numbers = db.query(B2CSalesOrder.order_number).all()
    for (order_number,) in order_numbers:
        if not order_number:
            continue
        match = B2C_ORDER_NUMBER_PATTERN.fullmatch(order_number)
        if match is None:
            continue
        highest = max(highest, int(match.group(1)))
    return highest + 1


def _resolve_b2c_channel(
    db: Session,
    raw_channel_id: str | int | None,
    *,
    allow_inactive_channel_id: int | None = None,
) -> Channel:
    text = str(raw_channel_id or "").strip()
    if not text:
        raise B2CValidationError("Channel is required.")
    if not text.isdigit():
        raise B2CValidationError("Selected channel is invalid.")

    channel_id = int(text)
    channel = db.query(Channel).filter(Channel.id == channel_id).one_or_none()
    if channel is None:
        raise B2CValidationError("Selected channel does not exist.")
    if not channel.applies_to_b2c and channel.id != allow_inactive_channel_id:
        raise B2CValidationError("Selected channel is not valid for B2C orders.")
    if not channel.active and channel.id != allow_inactive_channel_id:
        raise B2CValidationError("Selected channel is not active.")
    return channel


def _clean_optional_text(value: str) -> str | None:
    cleaned = (value or "").strip()
    return cleaned or None
