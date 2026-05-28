from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy.orm import Session, joinedload

from app.models import (
    InventoryBalance,
    PackagingBatch,
    PackagingBatchLine,
    PackagingBatchLineMaterial,
    ProductBomHeader,
    ProductBomLine,
)


ZERO = Decimal("0")


class PackagingBatchMaterialValidationError(Exception):
    pass


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
    if batch.status != "draft":
        raise PackagingBatchMaterialValidationError("Material Snapshot can only be refreshed while the batch is draft.")
    if line.product_id is None or line.product is None:
        raise PackagingBatchMaterialValidationError("Packaging batch line must have a real product.")
    if not line.product.active:
        raise PackagingBatchMaterialValidationError("Product must be active to refresh Material Snapshot.")
    if not line.product.is_manufactured:
        raise PackagingBatchMaterialValidationError("Product must be manufactured to refresh Material Snapshot.")

    try:
        bom_header = _get_active_product_bom_header(db, line.product_id)
        if bom_header is None:
            raise PackagingBatchMaterialValidationError(
                f"Product {line.product.sku} does not have an active BOM."
            )
        if not bom_header.lines:
            raise PackagingBatchMaterialValidationError(
                f"Active BOM for product {line.product.sku} has no lines."
            )
        _replace_line_material_snapshot(db, line, bom_header)
        db.commit()
    except PackagingBatchMaterialValidationError:
        invalidate_packaging_batch_line_material_snapshot(db, line, status="error")
        db.commit()
        raise

    db.refresh(line)
    return line


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
