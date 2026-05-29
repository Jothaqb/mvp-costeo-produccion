from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from sqlalchemy.orm import Session, joinedload

from app.models import (
    Activity,
    LaborRate,
    Machine,
    MachineRate,
    OverheadRate,
    PackagingBatch,
    PackagingBatchActivity,
    RouteActivity,
)


ZERO = Decimal("0")
HOURS_DIVISOR = Decimal("60")


class PackagingBatchActivityValidationError(Exception):
    pass


def get_packaging_batch_with_activities(db: Session, batch_id: int) -> PackagingBatch:
    batch = (
        db.query(PackagingBatch)
        .options(joinedload(PackagingBatch.activities))
        .filter(PackagingBatch.id == batch_id)
        .one_or_none()
    )
    if batch is None:
        raise PackagingBatchActivityValidationError("Packaging batch not found.")
    return batch


def ensure_packaging_batch_activities(
    db: Session,
    batch: PackagingBatch,
    *,
    replace_existing: bool = False,
) -> list[PackagingBatchActivity]:
    if replace_existing:
        for activity in list(batch.activities):
            db.delete(activity)
        db.flush()
        db.expire(batch, ["activities"])

    if batch.activities:
        return list(batch.activities)

    route_activities = (
        db.query(RouteActivity)
        .options(joinedload(RouteActivity.activity).joinedload(Activity.default_machine))
        .filter(RouteActivity.route_id == batch.route_id)
        .order_by(RouteActivity.sequence)
        .all()
    )

    for route_activity in route_activities:
        activity = route_activity.activity
        default_machine = activity.default_machine
        db.add(
            PackagingBatchActivity(
                packaging_batch_id=batch.id,
                sequence=route_activity.sequence,
                activity_id_snapshot=activity.id,
                activity_code_snapshot=activity.code,
                activity_name_snapshot=activity.name,
                applies_labor_snapshot=activity.applies_labor,
                applies_machine_snapshot=activity.applies_machine,
                required=route_activity.required,
                visible=route_activity.visible_default,
                machine_id_snapshot=default_machine.id if default_machine else None,
                machine_code_snapshot=default_machine.code if default_machine else None,
                machine_name_snapshot=default_machine.name if default_machine else None,
                labor_minutes=ZERO,
                machine_minutes=ZERO,
                labor_cost=ZERO,
                overhead_cost=ZERO,
                machine_cost=ZERO,
                total_activity_cost=ZERO,
            )
        )

    _set_batch_activity_totals_pending(batch)
    db.flush()
    db.expire(batch, ["activities"])
    return list(batch.activities)


def has_packaging_batch_captured_activity_times(batch: PackagingBatch) -> bool:
    return any(
        (activity.labor_minutes or ZERO) > ZERO or (activity.machine_minutes or ZERO) > ZERO
        for activity in batch.activities
    )


def update_packaging_batch_activity_times(
    db: Session,
    batch_id: int,
    activity_inputs: list[dict[str, str]],
) -> PackagingBatch:
    batch = get_packaging_batch_with_activities(db, batch_id)
    _ensure_draft(batch)
    ensure_packaging_batch_activities(db, batch)

    activities_by_id = {activity.id: activity for activity in batch.activities}
    for activity_input in activity_inputs:
        activity_id = int(activity_input["id"])
        activity = activities_by_id.get(activity_id)
        if activity is None:
            continue

        labor_minutes = _parse_optional_decimal(activity_input.get("labor_minutes"), "Labor minutes") or ZERO
        machine_minutes = _parse_optional_decimal(activity_input.get("machine_minutes"), "Machine minutes") or ZERO

        if labor_minutes < ZERO or machine_minutes < ZERO:
            raise PackagingBatchActivityValidationError("Activity minutes cannot be negative.")

        activity.labor_minutes = labor_minutes if activity.applies_labor_snapshot else ZERO
        activity.machine_minutes = machine_minutes if activity.applies_machine_snapshot else ZERO
        activity.notes = (activity_input.get("notes") or "").strip() or None

    try:
        recalculate_packaging_batch_activity_costs(db, batch)
        db.commit()
    except PackagingBatchActivityValidationError:
        db.commit()
        raise

    db.refresh(batch)
    return batch


def recalculate_packaging_batch_activity_costs(db: Session, batch: PackagingBatch) -> PackagingBatch:
    _ensure_draft(batch)
    ensure_packaging_batch_activities(db, batch)

    if not batch.activities:
        batch.activity_cost_status = "pending"
        batch.activity_costs_recalculated_at = datetime.utcnow()
        batch.real_labor_cost_total = ZERO
        batch.real_overhead_cost_total = ZERO
        batch.real_machine_cost_total = ZERO
        batch.real_activity_cost_total = ZERO
        db.flush()
        return batch

    try:
        labor_rate = _resolve_one_rate(
            _applicable_labor_rates(db, batch.production_date),
            "labor",
            batch.production_date,
        )
        overhead_rate = _resolve_one_rate(
            _applicable_overhead_rates(db, batch.production_date),
            "overhead",
            batch.production_date,
        )

        computed_rows: list[dict[str, object]] = []
        labor_total = ZERO
        overhead_total = ZERO
        machine_total = ZERO

        for activity in batch.activities:
            labor_minutes = activity.labor_minutes or ZERO
            machine_minutes = activity.machine_minutes or ZERO
            if labor_minutes < ZERO or machine_minutes < ZERO:
                raise PackagingBatchActivityValidationError(
                    f"Activity {activity.activity_code_snapshot} has invalid minutes."
                )

            labor_cost = ZERO
            overhead_cost = ZERO
            if labor_minutes > ZERO:
                labor_cost = (labor_minutes / HOURS_DIVISOR) * labor_rate.hourly_rate
                overhead_cost = (labor_minutes / HOURS_DIVISOR) * overhead_rate.hourly_rate

            machine_rate_snapshot = None
            machine_cost = ZERO
            machine_snapshot = {
                "machine_id_snapshot": activity.machine_id_snapshot,
                "machine_code_snapshot": activity.machine_code_snapshot,
                "machine_name_snapshot": activity.machine_name_snapshot,
            }
            if machine_minutes > ZERO:
                machine = _resolve_activity_machine(db, activity)
                machine_rate = _resolve_one_rate(
                    _applicable_machine_rates(db, machine.id, batch.production_date),
                    f"machine rate for {machine.code} - {machine.name}",
                    batch.production_date,
                )
                machine_snapshot = {
                    "machine_id_snapshot": machine.id,
                    "machine_code_snapshot": machine.code,
                    "machine_name_snapshot": machine.name,
                }
                machine_rate_snapshot = machine_rate.hourly_rate
                machine_cost = (machine_minutes / HOURS_DIVISOR) * machine_rate.hourly_rate

            total_activity_cost = labor_cost + overhead_cost + machine_cost
            computed_rows.append(
                {
                    "activity": activity,
                    "labor_rate_snapshot": labor_rate.hourly_rate,
                    "overhead_rate_snapshot": overhead_rate.hourly_rate,
                    "machine_rate_snapshot": machine_rate_snapshot,
                    "labor_cost": labor_cost,
                    "overhead_cost": overhead_cost,
                    "machine_cost": machine_cost,
                    "total_activity_cost": total_activity_cost,
                    **machine_snapshot,
                }
            )
            labor_total += labor_cost
            overhead_total += overhead_cost
            machine_total += machine_cost

        for row in computed_rows:
            activity = row["activity"]
            activity.labor_rate_snapshot = row["labor_rate_snapshot"]
            activity.overhead_rate_snapshot = row["overhead_rate_snapshot"]
            activity.machine_rate_snapshot = row["machine_rate_snapshot"]
            activity.labor_cost = row["labor_cost"]
            activity.overhead_cost = row["overhead_cost"]
            activity.machine_cost = row["machine_cost"]
            activity.total_activity_cost = row["total_activity_cost"]
            activity.machine_id_snapshot = row["machine_id_snapshot"]
            activity.machine_code_snapshot = row["machine_code_snapshot"]
            activity.machine_name_snapshot = row["machine_name_snapshot"]

        batch.real_labor_cost_total = labor_total
        batch.real_overhead_cost_total = overhead_total
        batch.real_machine_cost_total = machine_total
        batch.real_activity_cost_total = labor_total + overhead_total + machine_total
        batch.activity_costs_recalculated_at = datetime.utcnow()
        readiness = check_packaging_batch_activity_readiness(batch)
        batch.activity_cost_status = readiness["status"]
        db.flush()
        return batch
    except PackagingBatchActivityValidationError:
        batch.activity_cost_status = "error"
        batch.activity_costs_recalculated_at = None
        db.flush()
        raise


def get_packaging_batch_activity_summary(batch: PackagingBatch) -> dict[str, object]:
    total_activities = len(batch.activities)
    required_activities = sum(1 for activity in batch.activities if activity.required)
    labor_minutes_total = sum((activity.labor_minutes or ZERO) for activity in batch.activities)
    machine_minutes_total = sum((activity.machine_minutes or ZERO) for activity in batch.activities)
    return {
        "total_activities": total_activities,
        "required_activities": required_activities,
        "labor_minutes_total": labor_minutes_total,
        "machine_minutes_total": machine_minutes_total,
        "total_labor_cost": batch.real_labor_cost_total if batch.real_labor_cost_total is not None else ZERO,
        "total_overhead_cost": batch.real_overhead_cost_total if batch.real_overhead_cost_total is not None else ZERO,
        "total_machine_cost": batch.real_machine_cost_total if batch.real_machine_cost_total is not None else ZERO,
        "total_activity_cost": batch.real_activity_cost_total if batch.real_activity_cost_total is not None else ZERO,
        "activity_cost_status": (batch.activity_cost_status or "pending").strip().lower(),
        "recalculated_at": batch.activity_costs_recalculated_at,
        "has_activities": total_activities > 0,
    }


def check_packaging_batch_activity_readiness(batch: PackagingBatch) -> dict[str, object]:
    reasons: list[str] = []
    status = "ready"

    if not batch.activities:
        reasons.append("Batch has no activities")
        status = "pending"

    for activity in batch.activities:
        labor_minutes = activity.labor_minutes or ZERO
        machine_minutes = activity.machine_minutes or ZERO
        if labor_minutes < ZERO or machine_minutes < ZERO:
            reasons.append(f"Activity {activity.sequence} has invalid minutes")
            status = "error"
            continue
        if activity.required and activity.applies_labor_snapshot and labor_minutes <= ZERO:
            reasons.append(f"Activity {activity.sequence} is missing labor minutes")
            if status != "error":
                status = "pending"
        if activity.required and activity.applies_machine_snapshot and machine_minutes <= ZERO:
            reasons.append(f"Activity {activity.sequence} is missing machine minutes")
            if status != "error":
                status = "pending"

    if (batch.activity_cost_status or "pending").strip().lower() == "error":
        reasons.append("Activity cost calculation pending/error")
        status = "error"

    return {
        "is_ready": status == "ready",
        "status": status,
        "reasons": reasons,
    }


def _set_batch_activity_totals_pending(batch: PackagingBatch) -> None:
    batch.real_labor_cost_total = ZERO
    batch.real_overhead_cost_total = ZERO
    batch.real_machine_cost_total = ZERO
    batch.real_activity_cost_total = ZERO
    batch.activity_cost_status = "pending"
    batch.activity_costs_recalculated_at = None


def _parse_optional_decimal(value: str | Decimal | None, field_name: str) -> Decimal | None:
    if value is None or value == "":
        return None
    if isinstance(value, Decimal):
        return value
    text = (value or "").strip().replace(" ", "").replace(",", ".")
    try:
        return Decimal(text)
    except InvalidOperation as exc:
        raise PackagingBatchActivityValidationError(f"{field_name} must be a valid number.") from exc


def _applicable_labor_rates(db: Session, production_date: date) -> list[LaborRate]:
    return (
        db.query(LaborRate)
        .filter(
            LaborRate.effective_from <= production_date,
            (LaborRate.effective_to.is_(None)) | (LaborRate.effective_to >= production_date),
        )
        .all()
    )


def _applicable_overhead_rates(db: Session, production_date: date) -> list[OverheadRate]:
    return (
        db.query(OverheadRate)
        .filter(
            OverheadRate.effective_from <= production_date,
            (OverheadRate.effective_to.is_(None)) | (OverheadRate.effective_to >= production_date),
        )
        .all()
    )


def _applicable_machine_rates(db: Session, machine_id: int, production_date: date) -> list[MachineRate]:
    return (
        db.query(MachineRate)
        .filter(
            MachineRate.machine_id == machine_id,
            MachineRate.effective_from <= production_date,
            (MachineRate.effective_to.is_(None)) | (MachineRate.effective_to >= production_date),
        )
        .all()
    )


def _resolve_one_rate(rates: list, rate_label: str, production_date: date):
    if not rates:
        raise PackagingBatchActivityValidationError(
            f"No applicable {rate_label} rate found for {production_date}."
        )
    if len(rates) > 1:
        raise PackagingBatchActivityValidationError(
            f"Multiple applicable {rate_label} rates found for {production_date}."
        )
    return rates[0]


def _resolve_activity_machine(db: Session, activity: PackagingBatchActivity) -> Machine:
    if activity.machine_id_snapshot is not None:
        machine = db.query(Machine).filter(Machine.id == activity.machine_id_snapshot).one_or_none()
        if machine is not None:
            return machine

    if activity.activity_id_snapshot is not None:
        catalog = db.query(Activity).filter(Activity.id == activity.activity_id_snapshot).one_or_none()
        if catalog is not None and catalog.default_machine is not None:
            return catalog.default_machine

    matches = (
        db.query(Activity)
        .filter(Activity.code == activity.activity_code_snapshot, Activity.default_machine_id.is_not(None))
        .all()
    )
    if len(matches) != 1 or matches[0].default_machine is None:
        raise PackagingBatchActivityValidationError(
            f"Activity {activity.activity_code_snapshot} has machine minutes but no clear machine snapshot."
        )
    return matches[0].default_machine


def _ensure_draft(batch: PackagingBatch) -> None:
    if batch.status != "draft":
        raise PackagingBatchActivityValidationError("Activity Time Capture can only be edited while the batch is draft.")
