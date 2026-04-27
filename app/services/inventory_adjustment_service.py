import secrets
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from sqlalchemy.orm import Session

from app.models import (
    AppSequence,
    InventoryAdjustment,
    InventoryAdjustmentPostToken,
    Product,
)
from app.services.inventory_ledger_service import (
    InventoryLedgerValidationError,
    get_or_create_inventory_balance,
    post_incoming_movement,
    post_outgoing_movement,
)


ADJUSTMENT_SEQUENCE_NAME = "inventory_adjustment"
ADJUSTMENT_PREFIX = "ADJ"
SOURCE_TYPE = "inventory_adjustment"
MODE_QUANTITY_ADJUSTMENT = "quantity_adjustment"
MODE_STOCK_COUNT = "stock_count"
TYPE_INCREASE = "increase"
TYPE_DECREASE = "decrease"
STATUS_POSTED = "posted"
REASON_PHYSICAL_COUNT = "physical_count"
REASON_DAMAGE = "damage"
REASON_WASTE = "waste"
REASON_CORRECTION = "correction"
REASON_OTHER = "other"
VALID_MODES = {MODE_QUANTITY_ADJUSTMENT, MODE_STOCK_COUNT}
VALID_TYPES = {TYPE_INCREASE, TYPE_DECREASE}
VALID_REASONS = {
    REASON_PHYSICAL_COUNT,
    REASON_DAMAGE,
    REASON_WASTE,
    REASON_CORRECTION,
    REASON_OTHER,
}
LEDGER_MANUAL_ADJUSTMENT = "manual_adjustment"
LEDGER_STOCK_COUNT_ADJUSTMENT = "stock_count_adjustment"
LEDGER_WASTE_LOSS = "waste_loss"
ZERO = Decimal("0")
DECIMAL_QUANT = Decimal("0.0001")


class InventoryAdjustmentValidationError(Exception):
    pass


def create_inventory_adjustment_post_token(db: Session) -> InventoryAdjustmentPostToken:
    post_token = InventoryAdjustmentPostToken(
        token=secrets.token_urlsafe(32),
        used_at=None,
    )
    db.add(post_token)
    db.commit()
    db.refresh(post_token)
    return post_token


def create_inventory_adjustment_with_posting(
    db: Session,
    *,
    post_token: str,
    adjustment_date: date,
    product_id: int,
    adjustment_mode: str,
    adjustment_type: str,
    quantity: str | Decimal | None,
    counted_qty: str | Decimal | None,
    unit_cost: str | Decimal | None,
    reason: str,
    notes: str,
) -> InventoryAdjustment:
    try:
        _consume_inventory_adjustment_post_token(db, post_token)
        normalized_mode = _normalize_mode(adjustment_mode)
        normalized_reason = _normalize_reason(reason)
        product = db.query(Product).filter(Product.id == product_id).one_or_none()
        if product is None:
            raise InventoryAdjustmentValidationError("Selected product does not exist.")

        balance = get_or_create_inventory_balance(db, product.id)
        current_qty_snapshot = _parse_decimal(balance.on_hand_qty, "Current quantity")
        cleaned_notes = _clean_optional_text(notes)

        adjustment_quantity = ZERO
        counted_qty_value: Decimal | None = None
        normalized_type = TYPE_INCREASE
        transaction_type = LEDGER_MANUAL_ADJUSTMENT

        if normalized_mode == MODE_QUANTITY_ADJUSTMENT:
            normalized_type = _normalize_type(adjustment_type)
            entered_quantity = _parse_decimal(quantity, "Quantity")
            if entered_quantity <= ZERO:
                raise InventoryAdjustmentValidationError("Quantity must be greater than 0.")
            adjustment_quantity = entered_quantity if normalized_type == TYPE_INCREASE else -entered_quantity
            transaction_type = _transaction_type_for_quantity_adjustment(normalized_type, normalized_reason)
        else:
            counted_qty_value = _parse_decimal(counted_qty, "Counted quantity")
            if counted_qty_value < ZERO:
                raise InventoryAdjustmentValidationError("Counted quantity cannot be negative.")
            adjustment_quantity = (counted_qty_value - current_qty_snapshot).quantize(DECIMAL_QUANT)
            if adjustment_quantity == ZERO:
                raise InventoryAdjustmentValidationError(
                    "No adjustment needed because counted quantity matches current inventory."
                )
            normalized_type = TYPE_INCREASE if adjustment_quantity > ZERO else TYPE_DECREASE
            transaction_type = LEDGER_STOCK_COUNT_ADJUSTMENT

        posted_quantity = abs(adjustment_quantity).quantize(DECIMAL_QUANT)
        warning_messages: list[str] = []
        posting_unit_cost: Decimal | None = None
        if normalized_type == TYPE_INCREASE:
            posting_unit_cost = _resolve_positive_adjustment_unit_cost(
                balance_average_unit_cost=balance.average_unit_cost,
                product_standard_cost=product.standard_cost,
                entered_unit_cost=unit_cost,
                warnings=warning_messages,
            )

        adjustment = InventoryAdjustment(
            adjustment_number=_generate_adjustment_number(db),
            adjustment_date=adjustment_date,
            product_id=product.id,
            sku_snapshot=product.sku,
            product_name_snapshot=product.name,
            adjustment_mode=normalized_mode,
            adjustment_type=normalized_type,
            transaction_type=transaction_type,
            reason=normalized_reason,
            current_qty_snapshot=current_qty_snapshot,
            counted_qty=counted_qty_value,
            quantity_adjustment=adjustment_quantity,
            unit_cost=None,
            total_cost=None,
            notes=cleaned_notes,
            warning_notes=None,
            status=STATUS_POSTED,
            inventory_transaction_id=None,
        )
        db.add(adjustment)
        db.flush()

        transaction_date = datetime.combine(adjustment_date, datetime.utcnow().time())
        base_note = _build_adjustment_note(adjustment, cleaned_notes)
        if normalized_type == TYPE_INCREASE:
            posting = post_incoming_movement(
                db,
                product_id=product.id,
                transaction_type=transaction_type,
                incoming_qty=posted_quantity,
                incoming_unit_cost=posting_unit_cost,
                transaction_date=transaction_date,
                source_type=SOURCE_TYPE,
                source_id=adjustment.id,
                source_line_id=None,
                notes=base_note,
            )
        else:
            posting = post_outgoing_movement(
                db,
                product_id=product.id,
                transaction_type=transaction_type,
                outgoing_qty=posted_quantity,
                transaction_date=transaction_date,
                source_type=SOURCE_TYPE,
                source_id=adjustment.id,
                source_line_id=None,
                notes=base_note,
            )

        warning_messages.extend(posting.warnings)
        if warning_messages:
            posting.transaction.notes = _append_warning_notes(base_note, warning_messages)
            adjustment.warning_notes = " ".join(
                f"Warning: {warning}" for warning in warning_messages if warning and warning.strip()
            )

        adjustment.inventory_transaction_id = posting.transaction.id
        adjustment.unit_cost = posting.transaction.unit_cost
        adjustment.total_cost = posting.transaction.total_cost

        db.commit()
        db.refresh(adjustment)
        return adjustment
    except InventoryAdjustmentValidationError:
        db.rollback()
        raise
    except InventoryLedgerValidationError as exc:
        db.rollback()
        raise InventoryAdjustmentValidationError(str(exc)) from exc
    except Exception:
        db.rollback()
        raise


def _consume_inventory_adjustment_post_token(db: Session, post_token: str) -> None:
    token_value = (post_token or "").strip()
    if not token_value:
        raise InventoryAdjustmentValidationError("Adjustment submit token is required.")

    used_at = datetime.utcnow()
    updated_rows = (
        db.query(InventoryAdjustmentPostToken)
        .filter(
            InventoryAdjustmentPostToken.token == token_value,
            InventoryAdjustmentPostToken.used_at.is_(None),
        )
        .update({InventoryAdjustmentPostToken.used_at: used_at}, synchronize_session=False)
    )
    if updated_rows == 1:
        db.flush()
        return

    token_row = (
        db.query(InventoryAdjustmentPostToken)
        .filter(InventoryAdjustmentPostToken.token == token_value)
        .one_or_none()
    )
    if token_row is None:
        raise InventoryAdjustmentValidationError("Adjustment submit token is invalid.")
    if token_row.used_at is not None:
        raise InventoryAdjustmentValidationError(
            "This adjustment form has already been processed. Reopen New Adjustment to continue."
        )
    raise InventoryAdjustmentValidationError("Adjustment submit token could not be validated.")


def _resolve_positive_adjustment_unit_cost(
    *,
    balance_average_unit_cost,
    product_standard_cost,
    entered_unit_cost: str | Decimal | None,
    warnings: list[str],
) -> Decimal:
    if entered_unit_cost not in {None, ""}:
        parsed_entered_cost = _parse_decimal(entered_unit_cost, "Unit cost")
        if parsed_entered_cost < ZERO:
            raise InventoryAdjustmentValidationError("Unit cost cannot be negative.")
        if parsed_entered_cost > ZERO:
            return parsed_entered_cost

    average_unit_cost = _parse_decimal(balance_average_unit_cost, "Average unit cost")
    if average_unit_cost > ZERO:
        return average_unit_cost

    standard_cost = _parse_decimal(product_standard_cost, "Standard cost")
    if standard_cost > ZERO:
        return standard_cost

    warnings.append(
        "Adjustment posted with unit cost 0 because no entered cost, average cost, or standard cost was available."
    )
    return ZERO


def _transaction_type_for_quantity_adjustment(adjustment_type: str, reason: str) -> str:
    if adjustment_type == TYPE_DECREASE and reason == REASON_WASTE:
        return LEDGER_WASTE_LOSS
    return LEDGER_MANUAL_ADJUSTMENT


def _build_adjustment_note(adjustment: InventoryAdjustment, notes: str | None) -> str:
    note = (
        f"Inventory adjustment {adjustment.adjustment_number} posted for SKU {adjustment.sku_snapshot}. "
        f"Mode: {adjustment.adjustment_mode}. Reason: {adjustment.reason}."
    )
    if notes:
        note = f"{note} Notes: {notes}"
    return note


def _append_warning_notes(base_note: str, warnings: list[str]) -> str:
    unique_warnings = [warning.strip() for warning in warnings if warning and warning.strip()]
    if not unique_warnings:
        return base_note
    return f"{base_note} " + " ".join(f"Warning: {warning}" for warning in unique_warnings)


def _generate_adjustment_number(db: Session) -> str:
    sequence = db.query(AppSequence).filter(AppSequence.name == ADJUSTMENT_SEQUENCE_NAME).one_or_none()
    if sequence is None:
        sequence = AppSequence(
            name=ADJUSTMENT_SEQUENCE_NAME,
            next_value=_bootstrap_next_adjustment_sequence(db),
        )
        db.add(sequence)
        db.flush()

    adjustment_number = f"{ADJUSTMENT_PREFIX}{sequence.next_value:04d}"
    existing_adjustment = (
        db.query(InventoryAdjustment)
        .filter(InventoryAdjustment.adjustment_number == adjustment_number)
        .one_or_none()
    )
    if existing_adjustment is not None:
        raise InventoryAdjustmentValidationError(
            f"Generated adjustment number {adjustment_number} already exists."
        )
    sequence.next_value += 1
    return adjustment_number


def _bootstrap_next_adjustment_sequence(db: Session) -> int:
    highest = 0
    adjustment_numbers = db.query(InventoryAdjustment.adjustment_number).all()
    for (adjustment_number,) in adjustment_numbers:
        if not adjustment_number or not adjustment_number.startswith(ADJUSTMENT_PREFIX):
            continue
        suffix = adjustment_number[len(ADJUSTMENT_PREFIX):]
        if suffix.isdigit():
            highest = max(highest, int(suffix))
    return highest + 1


def _normalize_mode(value: str) -> str:
    normalized = (value or "").strip().lower()
    if normalized not in VALID_MODES:
        raise InventoryAdjustmentValidationError("Adjustment mode is invalid.")
    return normalized


def _normalize_type(value: str) -> str:
    normalized = (value or "").strip().lower()
    if normalized not in VALID_TYPES:
        raise InventoryAdjustmentValidationError("Adjustment type is invalid.")
    return normalized


def _normalize_reason(value: str) -> str:
    normalized = (value or "").strip().lower()
    if normalized not in VALID_REASONS:
        raise InventoryAdjustmentValidationError("Reason is invalid.")
    return normalized


def _parse_decimal(value: Decimal | str | int | float | None, field_name: str) -> Decimal:
    if isinstance(value, Decimal):
        return value.quantize(DECIMAL_QUANT)
    text = str(value if value is not None else "0").strip().replace(" ", "").replace(",", ".")
    try:
        return Decimal(text).quantize(DECIMAL_QUANT)
    except (InvalidOperation, ValueError) as exc:
        raise InventoryAdjustmentValidationError(f"{field_name} must be a valid number.") from exc


def _clean_optional_text(value: str | None) -> str | None:
    cleaned = (value or "").strip()
    return cleaned or None
