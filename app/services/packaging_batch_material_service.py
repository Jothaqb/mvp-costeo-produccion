from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from sqlalchemy.orm import Session, joinedload

from app.models import (
    InventoryBalance,
    PackagingBatch,
    PackagingBatchLine,
    PackagingBatchLineMaterial,
    Product,
    ProductBomHeader,
    ProductBomLine,
)
from app.services.packaging_batch_costing_service import invalidate_packaging_batch_line_cost_distribution


ZERO = Decimal("0")


class PackagingBatchMaterialValidationError(Exception):
    pass


@dataclass(frozen=True)
class PackagingBatchMaterialRefreshOutcome:
    error: str | None = None

    @property
    def is_ready(self) -> bool:
        return self.error is None


def get_packaging_batch_material_summary(batch: PackagingBatch) -> dict[str, object]:
    total_lines = len(batch.lines)
    ready_lines = 0
    pending_lines = 0
    error_lines = 0
    ready_material_snapshot_total = ZERO

    for line in batch.lines:
        status = (line.material_snapshot_status or "pending").strip().lower()
        if status == "ready":
            ready_lines += 1
            if line.material_snapshot_cost_total is not None:
                ready_material_snapshot_total += line.material_snapshot_cost_total
        elif status == "error":
            error_lines += 1
        else:
            pending_lines += 1

    if total_lines == 0:
        readiness_label = "Pending material snapshots"
    elif error_lines > 0:
        readiness_label = "Snapshot errors"
    elif pending_lines > 0:
        readiness_label = "Pending material snapshots"
    else:
        readiness_label = "Ready for next step"

    return {
        "total_lines": total_lines,
        "ready_lines": ready_lines,
        "pending_lines": pending_lines,
        "error_lines": error_lines,
        "ready_material_snapshot_total": ready_material_snapshot_total,
        "has_incomplete_snapshots": pending_lines > 0 or error_lines > 0 or total_lines == 0,
        "readiness_label": readiness_label,
    }


def check_packaging_batch_material_readiness(batch: PackagingBatch) -> dict[str, object]:
    reasons: list[str] = []

    if not batch.lines:
        reasons.append("Batch has no lines")

    for line in batch.lines:
        if line.product_id is None:
            reasons.append(f"Line {line.line_number} has no product")
            continue

        status = (line.material_snapshot_status or "pending").strip().lower()
        if status == "error":
            reasons.append(f"Line {line.line_number} has snapshot error")
        elif status != "ready":
            reasons.append(f"Line {line.line_number} has pending Material Snapshot")

    return {
        "is_ready": len(reasons) == 0,
        "reasons": reasons,
    }


def get_packaging_batch_line_with_materials(
    db: Session,
    *,
    batch_id: int,
    line_id: int,
) -> tuple[PackagingBatch, PackagingBatchLine]:
    line = (
        db.query(PackagingBatchLine)
        .options(
            joinedload(PackagingBatchLine.product),
            joinedload(PackagingBatchLine.materials),
            joinedload(PackagingBatchLine.packaging_batch).joinedload(PackagingBatch.route),
        )
        .filter(
            PackagingBatchLine.id == line_id,
            PackagingBatchLine.packaging_batch_id == batch_id,
        )
        .one_or_none()
    )
    if line is None:
        raise PackagingBatchMaterialValidationError("Packaging batch line not found.")
    return line.packaging_batch, line


def refresh_packaging_batch_line_material_snapshot(
    db: Session,
    *,
    batch_id: int,
    line_id: int,
) -> PackagingBatchLine:
    batch, line = get_packaging_batch_line_with_materials(db, batch_id=batch_id, line_id=line_id)
    outcome = sync_packaging_batch_line_material_snapshot(db, line=line)
    db.commit()
    if outcome.error:
        raise PackagingBatchMaterialValidationError(outcome.error)

    db.refresh(line)
    return line


def sync_packaging_batch_line_material_snapshot(
    db: Session,
    *,
    line: PackagingBatchLine,
) -> PackagingBatchMaterialRefreshOutcome:
    if line.id is None:
        db.flush()

    batch = line.packaging_batch
    if batch is None and line.packaging_batch_id is not None:
        batch = db.query(PackagingBatch).filter(PackagingBatch.id == line.packaging_batch_id).one_or_none()
        line.packaging_batch = batch

    try:
        if batch is None:
            raise PackagingBatchMaterialValidationError("Packaging batch line is not attached to a batch.")
        if batch.status != "draft":
            raise PackagingBatchMaterialValidationError(
                "Material Snapshot can only be refreshed while the batch is draft."
            )

        product = line.product
        if product is None and line.product_id is not None:
            product = db.query(Product).filter(Product.id == line.product_id).one_or_none()
            line.product = product
        if line.product_id is None or product is None:
            raise PackagingBatchMaterialValidationError("Packaging batch line must have a real product.")
        if not product.active:
            raise PackagingBatchMaterialValidationError("Product must be active to refresh Material Snapshot.")
        if not product.is_manufactured:
            raise PackagingBatchMaterialValidationError(
                "Product must be manufactured to refresh Material Snapshot."
            )

        bom_header = _get_active_product_bom_header(db, line.product_id)
        if bom_header is None:
            raise PackagingBatchMaterialValidationError(
                f"Product {product.sku} does not have an active BOM."
            )
        if not bom_header.lines:
            raise PackagingBatchMaterialValidationError(
                f"Active BOM for product {product.sku} has no lines."
            )

        _replace_line_material_snapshot(db, line, bom_header)
        invalidate_packaging_batch_line_cost_distribution(db, batch)
        return PackagingBatchMaterialRefreshOutcome()
    except PackagingBatchMaterialValidationError as exc:
        invalidate_packaging_batch_line_material_snapshot(db, line, status="error")
        if batch is not None:
            invalidate_packaging_batch_line_cost_distribution(db, batch)
        return PackagingBatchMaterialRefreshOutcome(error=str(exc))


def invalidate_packaging_batch_line_material_snapshot(
    db: Session,
    line: PackagingBatchLine,
    *,
    status: str = "pending",
) -> None:
    for material in list(line.materials):
        db.delete(material)
    line.material_snapshot_cost_total = None
    line.material_snapshot_status = status
    line.material_snapshot_refreshed_at = None
    invalidate_packaging_batch_line_cost_distribution(db, line.packaging_batch)
    db.flush()


def _replace_line_material_snapshot(
    db: Session,
    line: PackagingBatchLine,
    bom_header: ProductBomHeader,
) -> None:
    invalidate_packaging_batch_line_material_snapshot(db, line, status="pending")

    total = ZERO
    bom_lines = sorted(bom_header.lines, key=lambda item: (item.line_number, item.id))
    for bom_line in bom_lines:
        quantity_standard = bom_line.quantity_standard
        required_quantity = _calculate_required_quantity(line, quantity_standard)
        if required_quantity is None or required_quantity <= ZERO:
            raise PackagingBatchMaterialValidationError(
                f"Material Snapshot requires a positive required quantity for component {bom_line.component_sku_snapshot or bom_line.component_name_snapshot or bom_line.id}."
            )

        unit_cost_snapshot = _resolve_product_bom_unit_cost(db, bom_line)
        if unit_cost_snapshot is None:
            raise PackagingBatchMaterialValidationError(
                f"Could not resolve a unit cost snapshot for component {bom_line.component_sku_snapshot or bom_line.component_name_snapshot or bom_line.id}."
            )

        line_cost = required_quantity * unit_cost_snapshot
        include_in_real_cost = bom_line.include_in_real_cost if bom_line.include_in_real_cost is not None else True
        db.add(
            PackagingBatchLineMaterial(
                packaging_batch_line_id=line.id,
                component_sku=bom_line.component_sku_snapshot,
                component_name=bom_line.component_name_snapshot,
                quantity_standard=quantity_standard,
                required_quantity=required_quantity,
                unit_cost_snapshot=unit_cost_snapshot,
                line_cost=line_cost,
                component_type=(bom_line.component_type or "").strip() or "material",
                include_in_real_cost=include_in_real_cost,
            )
        )
        if include_in_real_cost:
            total += line_cost

    line.material_snapshot_cost_total = total
    line.material_snapshot_status = "ready"
    line.material_snapshot_refreshed_at = datetime.utcnow()
    db.flush()


def _calculate_required_quantity(
    line: PackagingBatchLine,
    quantity_standard: Decimal | None,
) -> Decimal | None:
    if line.planned_qty is None or quantity_standard is None:
        return None
    return line.planned_qty * quantity_standard


def _get_active_product_bom_header(db: Session, product_id: int) -> ProductBomHeader | None:
    return (
        db.query(ProductBomHeader)
        .options(
            joinedload(ProductBomHeader.lines).joinedload(ProductBomLine.component_product),
            joinedload(ProductBomHeader.lines).joinedload(ProductBomLine.source_imported_bom_line),
        )
        .filter(
            ProductBomHeader.product_id == product_id,
            ProductBomHeader.active.is_(True),
        )
        .one_or_none()
    )


def _resolve_product_bom_unit_cost(db: Session, line: ProductBomLine) -> Decimal | None:
    component_product = line.component_product
    if component_product is not None:
        balance = (
            db.query(InventoryBalance)
            .filter(InventoryBalance.product_id == component_product.id)
            .one_or_none()
        )
        if balance is not None and balance.average_unit_cost is not None and balance.average_unit_cost > ZERO:
            return balance.average_unit_cost
        if component_product.standard_cost is not None and component_product.standard_cost > ZERO:
            return component_product.standard_cost

    imported_line = line.source_imported_bom_line
    if imported_line is not None and imported_line.component_cost is not None:
        return imported_line.component_cost
    return None
