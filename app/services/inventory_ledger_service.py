from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation

from sqlalchemy.orm import Session

from app.models import InventoryBalance, InventoryTransaction, Product


ZERO = Decimal("0")
DECIMAL_QUANT = Decimal("0.0001")
OPENING_BALANCE = "opening_balance"
ALLOWED_TRANSACTION_TYPES = {
    OPENING_BALANCE,
    "po_receipt",
    "production_receipt",
    "production_component_consumption",
    "b2b_invoice_sale",
    "b2c_sale",
    "manual_adjustment",
    "stock_count_adjustment",
    "waste_loss",
}


class InventoryLedgerValidationError(Exception):
    pass


@dataclass(frozen=True)
class InventoryPostingResult:
    transaction: InventoryTransaction
    balance: InventoryBalance
    warnings: list[str]


@dataclass(frozen=True)
class InventoryInitializationResult:
    initialized_count: int
    zero_quantity_count: int
    zero_cost_count: int
    negative_inventory_zeroed_count: int
    skipped_count: int
    warning_messages: list[str]


def get_or_create_inventory_balance(db: Session, product_id: int) -> InventoryBalance:
    balance = db.query(InventoryBalance).filter(InventoryBalance.product_id == product_id).one_or_none()
    if balance is not None:
        return balance

    balance = InventoryBalance(
        product_id=product_id,
        on_hand_qty=ZERO,
        average_unit_cost=ZERO,
        inventory_value=ZERO,
        last_transaction_id=None,
        last_transaction_at=None,
    )
    db.add(balance)
    db.flush()
    return balance


def post_opening_balance(
    db: Session,
    *,
    product: Product,
    quantity: Decimal,
    unit_cost: Decimal,
    transaction_date: datetime | None = None,
    notes: str | None = None,
) -> InventoryPostingResult:
    transaction_date = transaction_date or datetime.utcnow()
    quantity = _normalize_decimal(quantity, "Opening quantity")
    unit_cost = _normalize_decimal(unit_cost, "Opening unit cost")
    if quantity < ZERO:
        raise InventoryLedgerValidationError("Opening quantity cannot be negative.")
    if unit_cost < ZERO:
        raise InventoryLedgerValidationError("Opening unit cost cannot be negative.")

    balance = get_or_create_inventory_balance(db, product.id)
    if balance.last_transaction_id is not None:
        raise InventoryLedgerValidationError(f"Product {product.sku} already has inventory history.")

    total_cost = (quantity * unit_cost).quantize(DECIMAL_QUANT)
    balance.on_hand_qty = quantity.quantize(DECIMAL_QUANT)
    balance.average_unit_cost = unit_cost.quantize(DECIMAL_QUANT)
    balance.inventory_value = total_cost

    transaction = InventoryTransaction(
        product_id=product.id,
        transaction_date=transaction_date,
        transaction_type=OPENING_BALANCE,
        source_type=None,
        source_id=None,
        source_line_id=None,
        quantity_in=balance.on_hand_qty,
        quantity_out=ZERO,
        unit_cost=balance.average_unit_cost,
        total_cost=total_cost,
        running_quantity=balance.on_hand_qty,
        running_average_cost=balance.average_unit_cost,
        running_inventory_value=balance.inventory_value,
        notes=notes,
    )
    db.add(transaction)
    db.flush()
    _sync_balance_from_transaction(balance, transaction)
    db.flush()
    return InventoryPostingResult(transaction=transaction, balance=balance, warnings=[])


def post_incoming_movement(
    db: Session,
    *,
    product_id: int,
    transaction_type: str,
    incoming_qty: Decimal | str,
    incoming_unit_cost: Decimal | str | None,
    transaction_date: datetime | None = None,
    source_type: str | None = None,
    source_id: int | None = None,
    source_line_id: int | None = None,
    notes: str | None = None,
) -> InventoryPostingResult:
    transaction_type = _normalize_transaction_type(transaction_type)
    incoming_qty = _normalize_decimal(incoming_qty, "Incoming quantity")
    if incoming_qty <= ZERO:
        raise InventoryLedgerValidationError("Incoming quantity must be greater than 0.")
    if incoming_unit_cost is None:
        raise InventoryLedgerValidationError("Incoming unit cost is required.")
    incoming_unit_cost = _normalize_decimal(incoming_unit_cost, "Incoming unit cost")
    if incoming_unit_cost < ZERO:
        raise InventoryLedgerValidationError("Incoming unit cost cannot be negative.")

    product = db.query(Product).filter(Product.id == product_id).one()
    balance = get_or_create_inventory_balance(db, product_id)
    transaction_date = transaction_date or datetime.utcnow()

    current_qty = _normalize_decimal(balance.on_hand_qty, "Current quantity")
    current_average_cost = _normalize_decimal(balance.average_unit_cost, "Current average cost")
    effective_current_qty = max(current_qty, ZERO)
    new_quantity = (current_qty + incoming_qty).quantize(DECIMAL_QUANT)
    denominator = effective_current_qty + incoming_qty
    if denominator <= ZERO:
        raise InventoryLedgerValidationError("Incoming movement denominator must be greater than 0.")

    if effective_current_qty <= ZERO:
        new_average_cost = incoming_unit_cost.quantize(DECIMAL_QUANT)
    else:
        weighted_total = (effective_current_qty * current_average_cost) + (incoming_qty * incoming_unit_cost)
        new_average_cost = (weighted_total / denominator).quantize(DECIMAL_QUANT)

    new_inventory_value = (new_quantity * new_average_cost).quantize(DECIMAL_QUANT)
    transaction = InventoryTransaction(
        product_id=product.id,
        transaction_date=transaction_date,
        transaction_type=transaction_type,
        source_type=(source_type or "").strip() or None,
        source_id=source_id,
        source_line_id=source_line_id,
        quantity_in=incoming_qty.quantize(DECIMAL_QUANT),
        quantity_out=ZERO,
        unit_cost=incoming_unit_cost.quantize(DECIMAL_QUANT),
        total_cost=(incoming_qty * incoming_unit_cost).quantize(DECIMAL_QUANT),
        running_quantity=new_quantity,
        running_average_cost=new_average_cost,
        running_inventory_value=new_inventory_value,
        notes=(notes or "").strip() or None,
    )
    db.add(transaction)
    db.flush()
    _sync_balance_from_transaction(balance, transaction)
    db.flush()
    return InventoryPostingResult(transaction=transaction, balance=balance, warnings=[])


def post_outgoing_movement(
    db: Session,
    *,
    product_id: int,
    transaction_type: str,
    outgoing_qty: Decimal | str,
    transaction_date: datetime | None = None,
    source_type: str | None = None,
    source_id: int | None = None,
    source_line_id: int | None = None,
    notes: str | None = None,
) -> InventoryPostingResult:
    transaction_type = _normalize_transaction_type(transaction_type)
    outgoing_qty = _normalize_decimal(outgoing_qty, "Outgoing quantity")
    if outgoing_qty <= ZERO:
        raise InventoryLedgerValidationError("Outgoing quantity must be greater than 0.")

    product = db.query(Product).filter(Product.id == product_id).one()
    balance = get_or_create_inventory_balance(db, product_id)
    transaction_date = transaction_date or datetime.utcnow()

    current_qty = _normalize_decimal(balance.on_hand_qty, "Current quantity")
    current_average_cost = _normalize_decimal(balance.average_unit_cost, "Current average cost")
    new_quantity = (current_qty - outgoing_qty).quantize(DECIMAL_QUANT)
    cogs = (outgoing_qty * current_average_cost).quantize(DECIMAL_QUANT)
    new_inventory_value = (current_qty * current_average_cost - cogs).quantize(DECIMAL_QUANT)
    warnings: list[str] = []
    if new_quantity < ZERO:
        warnings.append(
            f"Movement leaves product {product.sku} with negative on-hand quantity {new_quantity}."
        )

    transaction = InventoryTransaction(
        product_id=product.id,
        transaction_date=transaction_date,
        transaction_type=transaction_type,
        source_type=(source_type or "").strip() or None,
        source_id=source_id,
        source_line_id=source_line_id,
        quantity_in=ZERO,
        quantity_out=outgoing_qty.quantize(DECIMAL_QUANT),
        unit_cost=current_average_cost.quantize(DECIMAL_QUANT),
        total_cost=cogs,
        running_quantity=new_quantity,
        running_average_cost=current_average_cost.quantize(DECIMAL_QUANT),
        running_inventory_value=new_inventory_value,
        notes=(notes or "").strip() or None,
    )
    db.add(transaction)
    db.flush()
    _sync_balance_from_transaction(balance, transaction)
    db.flush()
    return InventoryPostingResult(transaction=transaction, balance=balance, warnings=warnings)


def post_inventory_transaction(
    db: Session,
    *,
    product_id: int,
    transaction_type: str,
    quantity_in: Decimal | str = ZERO,
    quantity_out: Decimal | str = ZERO,
    unit_cost: Decimal | str | None = None,
    transaction_date: datetime | None = None,
    source_type: str | None = None,
    source_id: int | None = None,
    source_line_id: int | None = None,
    notes: str | None = None,
) -> InventoryPostingResult:
    normalized_in = _normalize_decimal(quantity_in, "Quantity in")
    normalized_out = _normalize_decimal(quantity_out, "Quantity out")
    if normalized_in > ZERO and normalized_out > ZERO:
        raise InventoryLedgerValidationError("A transaction cannot have both quantity in and quantity out.")
    if normalized_in > ZERO:
        return post_incoming_movement(
            db,
            product_id=product_id,
            transaction_type=transaction_type,
            incoming_qty=normalized_in,
            incoming_unit_cost=unit_cost,
            transaction_date=transaction_date,
            source_type=source_type,
            source_id=source_id,
            source_line_id=source_line_id,
            notes=notes,
        )
    if normalized_out > ZERO:
        return post_outgoing_movement(
            db,
            product_id=product_id,
            transaction_type=transaction_type,
            outgoing_qty=normalized_out,
            transaction_date=transaction_date,
            source_type=source_type,
            source_id=source_id,
            source_line_id=source_line_id,
            notes=notes,
        )
    raise InventoryLedgerValidationError("Transaction must include either quantity in or quantity out.")


def initialize_inventory_opening_balances(
    db: Session,
    as_of: datetime | None = None,
) -> InventoryInitializationResult:
    existing_openings = (
        db.query(InventoryTransaction.id)
        .filter(InventoryTransaction.transaction_type == OPENING_BALANCE)
        .first()
    )
    if existing_openings is not None:
        raise InventoryLedgerValidationError("Opening balances have already been initialized.")

    products = db.query(Product).order_by(Product.sku).all()
    initialized_count = 0
    zero_quantity_count = 0
    zero_cost_count = 0
    negative_inventory_zeroed_count = 0
    skipped_count = 0
    warnings: list[str] = []
    timestamp = as_of or datetime.utcnow()

    for product in products:
        if not product.active:
            skipped_count += 1
            continue

        raw_qty = product.current_inventory_qty if product.current_inventory_qty is not None else ZERO
        raw_cost = product.standard_cost if product.standard_cost is not None else ZERO
        qty = _normalize_decimal(raw_qty, "Opening quantity")
        cost = _normalize_decimal(raw_cost, "Opening cost")

        if qty < ZERO:
            warnings.append(
                f"{product.sku}: current inventory {qty} was negative and was initialized as 0."
            )
            negative_inventory_zeroed_count += 1
            qty = ZERO
        if qty == ZERO:
            zero_quantity_count += 1
            warnings.append(f"{product.sku}: initialized with zero quantity.")
        if cost == ZERO:
            zero_cost_count += 1
            warnings.append(f"{product.sku}: initialized with zero cost.")

        post_opening_balance(
            db,
            product=product,
            quantity=qty,
            unit_cost=cost,
            transaction_date=timestamp,
            notes="Initialized from Product.current_inventory_qty and Product.standard_cost.",
        )
        initialized_count += 1

    db.commit()
    return InventoryInitializationResult(
        initialized_count=initialized_count,
        zero_quantity_count=zero_quantity_count,
        zero_cost_count=zero_cost_count,
        negative_inventory_zeroed_count=negative_inventory_zeroed_count,
        skipped_count=skipped_count,
        warning_messages=warnings,
    )


def _sync_balance_from_transaction(balance: InventoryBalance, transaction: InventoryTransaction) -> None:
    balance.on_hand_qty = transaction.running_quantity
    balance.average_unit_cost = transaction.running_average_cost
    balance.inventory_value = transaction.running_inventory_value
    balance.last_transaction_id = transaction.id
    balance.last_transaction_at = transaction.transaction_date


def _normalize_transaction_type(transaction_type: str) -> str:
    normalized = (transaction_type or "").strip().lower()
    if normalized not in ALLOWED_TRANSACTION_TYPES:
        raise InventoryLedgerValidationError("Invalid inventory transaction type.")
    return normalized


def _normalize_decimal(value: Decimal | str | int | float | None, field_name: str) -> Decimal:
    if isinstance(value, Decimal):
        return value.quantize(DECIMAL_QUANT)
    text = str(value if value is not None else "0").strip().replace(" ", "").replace(",", ".")
    try:
        return Decimal(text).quantize(DECIMAL_QUANT)
    except (InvalidOperation, ValueError) as exc:
        raise InventoryLedgerValidationError(f"{field_name} must be a valid number.") from exc
