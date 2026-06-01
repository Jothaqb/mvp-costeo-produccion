from __future__ import annotations

from decimal import Decimal

from sqlalchemy.orm import Session, joinedload

from app.models import (
    InventoryBalance,
    InventoryTransaction,
    PackagingBatch,
    PackagingBatchLine,
    PackagingBatchLineMaterial,
    Product,
)


ZERO = Decimal("0")
PACKAGING_BATCH_SOURCE_TYPE = "packaging_batch"
PACKAGING_BATCH_COMPONENT_CONSUMPTION = "packaging_batch_component_consumption"
PACKAGING_BATCH_RECEIPT = "packaging_batch_receipt"


class PackagingBatchCloseValidationError(Exception):
    pass


def get_packaging_batch_for_close(db: Session, batch_id: int) -> PackagingBatch:
    batch = (
        db.query(PackagingBatch)
        .options(
            joinedload(PackagingBatch.route),
            joinedload(PackagingBatch.created_by_user),
            joinedload(PackagingBatch.updated_by_user),
            joinedload(PackagingBatch.closed_by_user),
            joinedload(PackagingBatch.activities),
            joinedload(PackagingBatch.lines).joinedload(PackagingBatchLine.product),
            joinedload(PackagingBatch.lines).joinedload(PackagingBatchLine.materials),
        )
        .filter(PackagingBatch.id == batch_id)
        .one_or_none()
    )
    if batch is None:
        raise PackagingBatchCloseValidationError("Packaging batch not found.")
    return batch


def check_packaging_batch_existing_ledger_postings(db: Session, batch_id: int) -> dict[str, object]:
    transactions = (
        db.query(InventoryTransaction)
        .filter(
            InventoryTransaction.source_type == PACKAGING_BATCH_SOURCE_TYPE,
            InventoryTransaction.source_id == batch_id,
            InventoryTransaction.transaction_type.in_(
                (
                    PACKAGING_BATCH_COMPONENT_CONSUMPTION,
                    PACKAGING_BATCH_RECEIPT,
                )
            ),
        )
        .order_by(InventoryTransaction.id)
        .all()
    )
    return {
        "has_existing_postings": bool(transactions),
        "transaction_count": len(transactions),
        "transactions": transactions,
    }


def get_packaging_batch_stock_warnings(db: Session, batch_id: int) -> list[dict[str, object]]:
    batch = get_packaging_batch_for_close(db, batch_id)
    return _build_stock_warnings(db, batch)


def check_packaging_batch_close_readiness(db: Session, batch_id: int) -> dict[str, object]:
    batch = get_packaging_batch_for_close(db, batch_id)
    ledger_postings = check_packaging_batch_existing_ledger_postings(db, batch_id)
    stock_warnings = _build_stock_warnings(db, batch)

    reasons: list[str] = []
    if (batch.status or "").strip().lower() == "closed":
        reasons.append("Batch is already closed")
    if (batch.status or "").strip().lower() != "draft":
        reasons.append("Batch must be draft before closing")
    if not batch.lines:
        reasons.append("Batch has no lines")
    if batch.route is None:
        reasons.append("Batch route is missing")

    for line in batch.lines:
        if line.planned_qty is None or line.planned_qty <= ZERO:
            reasons.append(f"Line {line.line_number} must have planned quantity greater than zero")
        if (line.material_snapshot_status or "pending").strip().lower() != "ready":
            reasons.append(f"Line {line.line_number} Material Snapshot is not ready")
        if (line.cost_distribution_status or "pending").strip().lower() != "ready":
            reasons.append(f"Line {line.line_number} cost distribution is not ready")
        if line.real_total_cost is None:
            reasons.append(f"Line {line.line_number} is missing real total cost")
        if line.real_unit_cost is None:
            reasons.append(f"Line {line.line_number} is missing real unit cost")

    if (batch.activity_cost_status or "pending").strip().lower() != "ready":
        reasons.append("Activity costs are not ready")

    if ledger_postings["has_existing_postings"]:
        reasons.append("Batch already has inventory ledger postings")

    return {
        "is_ready": len(reasons) == 0,
        "status": "ready" if len(reasons) == 0 else "not_ready",
        "reasons": reasons,
        "warnings": [warning["message"] for warning in stock_warnings],
        "warning_count": len(stock_warnings),
        "has_existing_ledger_postings": ledger_postings["has_existing_postings"],
        "existing_ledger_transaction_count": ledger_postings["transaction_count"],
    }


def get_packaging_batch_close_preview(db: Session, batch_id: int) -> dict[str, object]:
    batch = get_packaging_batch_for_close(db, batch_id)
    stock_warnings = _build_stock_warnings(db, batch)

    materials_to_consume: list[dict[str, object]] = []
    finished_goods_to_receive: list[dict[str, object]] = []
    total_material_cost = ZERO
    total_final_cost = ZERO

    for line in batch.lines:
        total_material_cost += line.material_snapshot_cost_total or ZERO
        if line.real_total_cost is not None:
            total_final_cost += line.real_total_cost

        finished_goods_to_receive.append(
            {
                "line_id": line.id,
                "line_number": line.line_number,
                "product_id": line.product_id,
                "product_sku": line.product_sku_snapshot,
                "product_name": line.product_name_snapshot,
                "planned_qty": line.planned_qty,
                "real_unit_cost": line.real_unit_cost,
                "real_total_cost": line.real_total_cost,
            }
        )

        for material in line.materials:
            materials_to_consume.append(
                {
                    "material_id": material.id,
                    "line_id": line.id,
                    "line_number": line.line_number,
                    "finished_good_sku": line.product_sku_snapshot,
                    "finished_good_name": line.product_name_snapshot,
                    "component_sku": material.component_sku,
                    "component_name": material.component_name,
                    "required_quantity": material.required_quantity,
                    "unit_cost_snapshot": material.unit_cost_snapshot,
                    "line_cost": material.line_cost,
                    "include_in_real_cost": material.include_in_real_cost,
                }
            )

    return {
        "batch": batch,
        "summary": {
            "internal_batch_number": batch.internal_batch_number,
            "production_date": batch.production_date,
            "packaging_type": batch.packaging_type,
            "route": f"{batch.route_name_snapshot} v{batch.route_version_snapshot}",
            "total_lines": len(batch.lines),
            "total_material_cost": total_material_cost,
            "total_labor_cost": batch.real_labor_cost_total or ZERO,
            "total_overhead_cost": batch.real_overhead_cost_total or ZERO,
            "total_machine_cost": batch.real_machine_cost_total or ZERO,
            "total_final_cost": total_final_cost,
        },
        "materials_to_consume": materials_to_consume,
        "finished_goods_to_receive": finished_goods_to_receive,
        "stock_warnings": stock_warnings,
    }


def _build_stock_warnings(db: Session, batch: PackagingBatch) -> list[dict[str, object]]:
    materials = [material for line in batch.lines for material in line.materials]
    component_skus = sorted(
        {
            (material.component_sku or "").strip()
            for material in materials
            if (material.component_sku or "").strip()
        }
    )
    products_by_sku = {
        product.sku: product
        for product in db.query(Product).filter(Product.sku.in_(component_skus)).all()
    } if component_skus else {}
    product_ids = [product.id for product in products_by_sku.values()]
    balances_by_product_id = {
        balance.product_id: balance
        for balance in db.query(InventoryBalance).filter(InventoryBalance.product_id.in_(product_ids)).all()
    } if product_ids else {}

    warnings: list[dict[str, object]] = []
    for line in batch.lines:
        for material in line.materials:
            required_quantity = material.required_quantity or ZERO
            if required_quantity <= ZERO:
                continue

            component_sku = (material.component_sku or "").strip() or None
            component_product = products_by_sku.get(component_sku or "")
            if component_product is None:
                warnings.append(
                    {
                        "line_number": line.line_number,
                        "finished_good_sku": line.product_sku_snapshot,
                        "finished_good_name": line.product_name_snapshot,
                        "component_sku": component_sku,
                        "component_name": material.component_name,
                        "current_stock": None,
                        "required_quantity": required_quantity,
                        "deficit": None,
                        "message": (
                            f"Component {component_sku or material.component_name or material.id} is not mapped to a Product "
                            "record, so current stock could not be verified."
                        ),
                    }
                )
                continue

            balance = balances_by_product_id.get(component_product.id)
            current_stock = balance.on_hand_qty if balance is not None and balance.on_hand_qty is not None else ZERO
            projected_stock = current_stock - required_quantity
            if projected_stock < ZERO:
                warnings.append(
                    {
                        "line_number": line.line_number,
                        "finished_good_sku": line.product_sku_snapshot,
                        "finished_good_name": line.product_name_snapshot,
                        "component_sku": component_product.sku,
                        "component_name": material.component_name or component_product.name,
                        "current_stock": current_stock,
                        "required_quantity": required_quantity,
                        "deficit": abs(projected_stock),
                        "message": (
                            f"Component {component_product.sku} has current stock {current_stock} and would end at "
                            f"{projected_stock} after consuming {required_quantity}."
                        ),
                    }
                )

    return warnings
