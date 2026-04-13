from datetime import date
from decimal import Decimal

from sqlalchemy.orm import Session

from app.models import (
    Activity,
    LaborRate,
    Machine,
    MachineRate,
    OverheadRate,
    ProductionOrder,
    ProductionOrderActivity,
)


ZERO = Decimal("0")
ONE = Decimal("1")
HOURS_DIVISOR = Decimal("60")
INPUT_SCALED_PROCESS_TYPES = {"dehydration", "grinding", "mixing"}


class CostingValidationError(Exception):
    pass


def calculate_order_cost(db: Session, order: ProductionOrder) -> None:
    if order.output_qty is None or order.output_qty <= 0:
        raise CostingValidationError("Output quantity must be greater than 0 before calculating cost.")

    labor_rate = _resolve_one_rate(
        _applicable_labor_rates(db, order.production_date),
        "labor",
        order.production_date,
    )
    overhead_rate = _resolve_one_rate(
        _applicable_overhead_rates(db, order.production_date),
        "overhead",
        order.production_date,
    )

    material_total = _calculate_material_total(order)
    labor_total = ZERO
    overhead_total = ZERO
    machine_total = ZERO

    for activity in order.activities:
        labor_minutes = activity.labor_minutes or ZERO
        machine_minutes = activity.machine_minutes or ZERO

        if labor_minutes < 0 or machine_minutes < 0:
            raise CostingValidationError("Activity minutes cannot be negative.")

        activity.labor_rate_snapshot = labor_rate.hourly_rate
        activity.overhead_rate_snapshot = overhead_rate.hourly_rate

        if labor_minutes == ZERO:
            activity.labor_cost = ZERO
            activity.overhead_cost = ZERO
        else:
            activity.labor_cost = (labor_minutes / HOURS_DIVISOR) * labor_rate.hourly_rate
            activity.overhead_cost = (labor_minutes / HOURS_DIVISOR) * overhead_rate.hourly_rate

        if machine_minutes == ZERO:
            activity.machine_cost = ZERO
            activity.machine_rate_snapshot = None
        else:
            machine = _resolve_activity_machine(db, activity)
            machine_rate = _resolve_one_rate(
                _applicable_machine_rates(db, machine.id, order.production_date),
                f"machine rate for {machine.code} - {machine.name}",
                order.production_date,
            )
            activity.machine_id_snapshot = machine.id
            activity.machine_code_snapshot = machine.code
            activity.machine_name_snapshot = machine.name
            activity.machine_rate_snapshot = machine_rate.hourly_rate
            activity.machine_cost = (machine_minutes / HOURS_DIVISOR) * machine_rate.hourly_rate

        activity.total_activity_cost = activity.labor_cost + activity.overhead_cost + activity.machine_cost
        labor_total += activity.labor_cost
        overhead_total += activity.overhead_cost
        machine_total += activity.machine_cost

    order.material_snapshot_cost_total = material_total
    order.real_labor_cost_total = labor_total
    order.real_overhead_cost_total = overhead_total
    order.real_machine_cost_total = machine_total
    order.real_total_cost = material_total + labor_total + overhead_total + machine_total
    order.real_unit_cost = order.real_total_cost / order.output_qty


def _calculate_material_total(order: ProductionOrder) -> Decimal:
    total = ZERO
    scaling_factor = _material_scaling_factor(order)
    for material in order.materials:
        if not material.include_in_real_cost:
            continue
        component = material.component_sku or material.component_name or f"material line {material.id}"
        if material.quantity_standard is None:
            raise CostingValidationError(f"Included material {component} has no standard quantity.")
        if material.unit_cost_snapshot is None:
            raise CostingValidationError(f"Included material {component} has no unit cost snapshot.")
        material.line_cost = material.quantity_standard * scaling_factor * material.unit_cost_snapshot
        total += material.line_cost
    return total


def _material_scaling_factor(order: ProductionOrder) -> Decimal:
    if order.process_type in INPUT_SCALED_PROCESS_TYPES:
        if order.input_qty is None or order.input_qty <= ZERO:
            raise CostingValidationError("Input quantity must be greater than 0 before calculating material cost.")
        return order.input_qty
    return ONE


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
        raise CostingValidationError(f"No applicable {rate_label} rate found for {production_date}.")
    if len(rates) > 1:
        raise CostingValidationError(f"Multiple applicable {rate_label} rates found for {production_date}.")
    return rates[0]


def _resolve_activity_machine(db: Session, activity: ProductionOrderActivity) -> Machine:
    if activity.machine_id_snapshot is not None:
        machine = db.query(Machine).filter(Machine.id == activity.machine_id_snapshot).one_or_none()
        if machine is not None:
            return machine

    matches = (
        db.query(Activity)
        .filter(Activity.code == activity.activity_code_snapshot, Activity.default_machine_id.is_not(None))
        .all()
    )
    if len(matches) != 1 or matches[0].default_machine is None:
        raise CostingValidationError(
            f"Activity {activity.activity_code_snapshot} has machine minutes but no clear machine snapshot."
        )

    machine = matches[0].default_machine
    activity.machine_id_snapshot = machine.id
    activity.machine_code_snapshot = machine.code
    activity.machine_name_snapshot = machine.name
    return machine
