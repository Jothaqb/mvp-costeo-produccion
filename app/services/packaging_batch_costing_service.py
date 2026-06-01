from __future__ import annotations

from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP

from sqlalchemy.orm import Session, joinedload

from app.models import PackagingBatch, PackagingBatchLine


ZERO = Decimal("0")
MONEY_QUANT = Decimal("0.0001")


class PackagingBatchCostingValidationError(Exception):
    pass


def distribute_packaging_batch_activity_costs(db: Session, batch_id: int) -> PackagingBatch:
    batch = get_packaging_batch_with_distribution(db, batch_id)
    _validate_distribution_prerequisites(batch)

    lines = list(batch.lines)
    total_batch_qty = sum((line.planned_qty or ZERO) for line in lines)
    labor_allocations = _allocate_proportional_amount(batch.real_labor_cost_total or ZERO, lines, total_batch_qty)
    overhead_allocations = _allocate_proportional_amount(
        batch.real_overhead_cost_total or ZERO,
        lines,
        total_batch_qty,
    )
    machine_allocations = _allocate_proportional_amount(batch.real_machine_cost_total or ZERO, lines, total_batch_qty)
    distributed_at = datetime.utcnow()

    for index, line in enumerate(lines):
        labor_cost = labor_allocations[index]
        overhead_cost = overhead_allocations[index]
        machine_cost = machine_allocations[index]

        line.real_labor_cost = labor_cost
        line.real_overhead_cost = overhead_cost
        line.real_machine_cost = machine_cost
        line.cost_distributed_at = distributed_at

        if (line.material_snapshot_status or "pending").strip().lower() == "ready" and line.material_snapshot_cost_total is not None:
            total_cost = _quantize_money(line.material_snapshot_cost_total + labor_cost + overhead_cost + machine_cost)
            line.real_total_cost = total_cost
            line.real_unit_cost = _quantize_money(total_cost / line.planned_qty)
            line.cost_distribution_status = "ready"
        else:
            line.real_total_cost = None
            line.real_unit_cost = None
            line.cost_distribution_status = "partial"

    db.commit()
    db.refresh(batch)
    return batch


def get_packaging_batch_with_distribution(db: Session, batch_id: int) -> PackagingBatch:
    batch = (
        db.query(PackagingBatch)
        .options(
            joinedload(PackagingBatch.lines).joinedload(PackagingBatchLine.product),
            joinedload(PackagingBatch.lines).joinedload(PackagingBatchLine.materials),
            joinedload(PackagingBatch.activities),
        )
        .filter(PackagingBatch.id == batch_id)
        .one_or_none()
    )
    if batch is None:
        raise PackagingBatchCostingValidationError("Packaging batch not found.")
    return batch


def get_packaging_batch_cost_distribution_summary(batch: PackagingBatch) -> dict[str, object]:
    total_batch_qty = sum((line.planned_qty or ZERO) for line in batch.lines)
    ready_lines = 0
    partial_lines = 0
    pending_lines = 0
    error_lines = 0
    distributed_activity_cost_total = ZERO
    distributed_at = None

    for line in batch.lines:
        status = (line.cost_distribution_status or "pending").strip().lower()
        if status == "ready":
            ready_lines += 1
        elif status == "partial":
            partial_lines += 1
        elif status == "error":
            error_lines += 1
        else:
            pending_lines += 1

        distributed_activity_cost_total += (line.real_labor_cost or ZERO)
        distributed_activity_cost_total += (line.real_overhead_cost or ZERO)
        distributed_activity_cost_total += (line.real_machine_cost or ZERO)

        if line.cost_distributed_at is not None and (distributed_at is None or line.cost_distributed_at > distributed_at):
            distributed_at = line.cost_distributed_at

    labor_cost_per_unit = None
    overhead_cost_per_unit = None
    machine_cost_per_unit = None
    if total_batch_qty > ZERO and (batch.activity_cost_status or "pending").strip().lower() == "ready":
        labor_cost_per_unit = _quantize_money((batch.real_labor_cost_total or ZERO) / total_batch_qty)
        overhead_cost_per_unit = _quantize_money((batch.real_overhead_cost_total or ZERO) / total_batch_qty)
        machine_cost_per_unit = _quantize_money((batch.real_machine_cost_total or ZERO) / total_batch_qty)

    if not batch.lines:
        distribution_status = "pending"
    elif error_lines > 0:
        distribution_status = "error"
    elif pending_lines > 0:
        distribution_status = "pending"
    elif partial_lines > 0:
        distribution_status = "partial"
    else:
        distribution_status = "ready"

    return {
        "total_batch_qty": total_batch_qty,
        "labor_cost_per_unit": labor_cost_per_unit,
        "overhead_cost_per_unit": overhead_cost_per_unit,
        "machine_cost_per_unit": machine_cost_per_unit,
        "distribution_status": distribution_status,
        "distributed_at": distributed_at,
        "ready_lines": ready_lines,
        "partial_lines": partial_lines,
        "pending_lines": pending_lines,
        "error_lines": error_lines,
        "distributed_activity_cost_total": distributed_activity_cost_total,
        "has_incomplete_total_costs": partial_lines > 0 or pending_lines > 0 or error_lines > 0,
    }


def check_packaging_batch_cost_distribution_readiness(batch: PackagingBatch) -> dict[str, object]:
    reasons: list[str] = []
    status = "ready"

    if batch.status != "draft":
        reasons.append("Batch is not draft")
        status = "pending"
    if not batch.lines:
        reasons.append("Batch has no lines")
        status = "pending"

    total_batch_qty = sum((line.planned_qty or ZERO) for line in batch.lines)
    if total_batch_qty <= ZERO:
        reasons.append("Total batch quantity must be greater than zero")
        status = "pending"

    if (batch.activity_cost_status or "pending").strip().lower() != "ready":
        reasons.append("Activity costs are not ready")
        status = "pending"

    for line in batch.lines:
        if line.planned_qty is None or line.planned_qty <= ZERO:
            reasons.append(f"Line {line.line_number} has invalid planned quantity")
            status = "pending"

    line_statuses = {(line.cost_distribution_status or "pending").strip().lower() for line in batch.lines}
    if "error" in line_statuses:
        status = "error"
        reasons.append("Cost distribution has line errors")
    elif "partial" in line_statuses and status != "error":
        status = "partial"
        reasons.append("Activity costs distributed. Total cost incomplete until Material Snapshot is ready.")
    elif line_statuses == {"pending"} and batch.lines and (batch.activity_cost_status or "pending").strip().lower() == "ready":
        reasons.append("Activity costs have not been distributed yet")
        status = "pending"

    return {
        "is_ready": status == "ready",
        "status": status,
        "reasons": reasons,
    }


def invalidate_packaging_batch_line_cost_distribution(db: Session, batch: PackagingBatch) -> None:
    for line in batch.lines:
        _clear_line_cost_distribution(line)
    db.flush()


def invalidate_packaging_batch_line_cost_distribution_for_line(db: Session, line: PackagingBatchLine) -> None:
    _clear_line_cost_distribution(line)
    db.flush()


def _clear_line_cost_distribution(line: PackagingBatchLine) -> None:
    line.real_labor_cost = None
    line.real_overhead_cost = None
    line.real_machine_cost = None
    line.real_total_cost = None
    line.real_unit_cost = None
    line.cost_distribution_status = "pending"
    line.cost_distributed_at = None


def _validate_distribution_prerequisites(batch: PackagingBatch) -> None:
    if batch.status != "draft":
        raise PackagingBatchCostingValidationError("Activity costs can only be distributed while the batch is draft.")
    if not batch.lines:
        raise PackagingBatchCostingValidationError("Packaging batch must have at least one line before distributing costs.")
    if (batch.activity_cost_status or "pending").strip().lower() != "ready":
        raise PackagingBatchCostingValidationError("Activity costs must be ready before distributing costs.")

    total_batch_qty = ZERO
    for line in batch.lines:
        if line.planned_qty is None or line.planned_qty <= ZERO:
            raise PackagingBatchCostingValidationError(
                f"Line {line.line_number} must have a planned quantity greater than zero."
            )
        total_batch_qty += line.planned_qty
    if total_batch_qty <= ZERO:
        raise PackagingBatchCostingValidationError("Total batch quantity must be greater than zero.")


def _allocate_proportional_amount(
    total_amount: Decimal,
    lines: list[PackagingBatchLine],
    total_batch_qty: Decimal,
) -> list[Decimal]:
    if not lines:
        return []
    if total_batch_qty <= ZERO:
        raise PackagingBatchCostingValidationError("Total batch quantity must be greater than zero.")

    allocations: list[Decimal] = []
    running_total = ZERO
    cost_per_unit = total_amount / total_batch_qty
    for line in lines[:-1]:
        amount = _quantize_money(line.planned_qty * cost_per_unit)
        allocations.append(amount)
        running_total += amount

    residual = _quantize_money(total_amount - running_total)
    allocations.append(residual)
    return allocations


def _quantize_money(value: Decimal) -> Decimal:
    return value.quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)
