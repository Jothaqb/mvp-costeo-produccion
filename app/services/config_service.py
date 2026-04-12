from datetime import date
from decimal import Decimal, InvalidOperation

from sqlalchemy.orm import Session

from app.models import Activity, LaborRate, Machine, MachineRate, OverheadRate, Route, RouteActivity
from app.schemas import ProcessType


class ValidationError(Exception):
    pass


def parse_decimal(value: str | Decimal | None, field_name: str) -> Decimal:
    if isinstance(value, Decimal):
        parsed = value
    else:
        text = (value or "").strip().replace(" ", "").replace(",", ".")
        try:
            parsed = Decimal(text)
        except InvalidOperation as exc:
            raise ValidationError(f"{field_name} must be a valid number.") from exc

    if parsed <= 0:
        raise ValidationError(f"{field_name} must be greater than 0.")
    return parsed


def validate_process_type(process_type: str) -> str:
    allowed = {item.value for item in ProcessType}
    if process_type not in allowed:
        raise ValidationError("Process type must be dehydration, grinding, mixing, or packaging.")
    return process_type


def validate_unique_code(
    db: Session,
    model: type[Activity] | type[Machine] | type[Route],
    code: str,
    current_id: int | None = None,
) -> None:
    query = db.query(model).filter(model.code == code)
    if current_id is not None:
        query = query.filter(model.id != current_id)
    if query.first() is not None:
        raise ValidationError(f"Code '{code}' is already in use.")


def validate_route_activity_sequence(
    db: Session,
    route_id: int,
    sequence: int,
    current_id: int | None = None,
) -> None:
    if sequence <= 0:
        raise ValidationError("Sequence must be greater than 0.")

    query = db.query(RouteActivity).filter(
        RouteActivity.route_id == route_id,
        RouteActivity.sequence == sequence,
    )
    if current_id is not None:
        query = query.filter(RouteActivity.id != current_id)
    if query.first() is not None:
        raise ValidationError("Sequence already exists for this route.")


def validate_rate_dates(effective_from: date, effective_to: date | None) -> None:
    if effective_to is not None and effective_to < effective_from:
        raise ValidationError("Effective to cannot be earlier than effective from.")


def validate_labor_rate(
    db: Session,
    effective_from: date,
    effective_to: date | None,
    current_id: int | None = None,
) -> None:
    validate_rate_dates(effective_from, effective_to)
    _validate_no_overlap(db, LaborRate, effective_from, effective_to, current_id=current_id)


def validate_overhead_rate(
    db: Session,
    effective_from: date,
    effective_to: date | None,
    current_id: int | None = None,
) -> None:
    validate_rate_dates(effective_from, effective_to)
    _validate_no_overlap(db, OverheadRate, effective_from, effective_to, current_id=current_id)


def validate_machine_rate(
    db: Session,
    machine_id: int,
    effective_from: date,
    effective_to: date | None,
    current_id: int | None = None,
) -> None:
    validate_rate_dates(effective_from, effective_to)
    _validate_no_overlap(
        db,
        MachineRate,
        effective_from,
        effective_to,
        current_id=current_id,
        machine_id=machine_id,
    )


def _validate_no_overlap(
    db: Session,
    model: type[LaborRate] | type[OverheadRate] | type[MachineRate],
    effective_from: date,
    effective_to: date | None,
    current_id: int | None = None,
    machine_id: int | None = None,
) -> None:
    query = db.query(model)
    if current_id is not None:
        query = query.filter(model.id != current_id)
    if machine_id is not None:
        query = query.filter(model.machine_id == machine_id)

    new_end = effective_to or date.max
    for rate in query.all():
        existing_end = rate.effective_to or date.max
        if effective_from <= existing_end and rate.effective_from <= new_end:
            raise ValidationError("Rate date range overlaps with an existing rate.")
