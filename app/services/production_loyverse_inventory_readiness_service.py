import os
from decimal import Decimal

from sqlalchemy.orm import Session, joinedload

from app.models import LoyverseVariantMapping, ProductionOrder


ZERO = Decimal("0")


def build_production_inventory_readiness(db: Session, order_id: int) -> dict:
    order = (
        db.query(ProductionOrder)
        .options(joinedload(ProductionOrder.product), joinedload(ProductionOrder.materials))
        .filter(ProductionOrder.id == order_id)
        .one()
    )
    store_id = os.getenv("LOYVERSE_STORE_ID", "").strip()
    token_present = bool(os.getenv("LOYVERSE_API_TOKEN", "").strip())
    store_id_present = bool(store_id)
    finished_good = _finished_good_check(db, order)
    output_qty = _positive_quantity_check(order.output_qty, "Output quantity is present.", "Output quantity is missing or zero.")
    real_unit_cost = _real_unit_cost_check(order)
    materials = _material_checks(db, order)
    required_quantities = _required_quantity_check(order)
    blockers = []

    if order.status != "closed":
        blockers.append("Production Order must be closed before inventory sync readiness can pass.")
    if not token_present:
        blockers.append("LOYVERSE_API_TOKEN is missing.")
    if not store_id_present:
        blockers.append("LOYVERSE_STORE_ID is missing.")
    if not finished_good["ready"]:
        blockers.append(finished_good["message"])
    if not output_qty["ready"]:
        blockers.append(output_qty["message"])
    if not real_unit_cost["ready"]:
        blockers.append(real_unit_cost["message"])
    if not materials:
        blockers.append("Production Order has no material lines.")
    for material in materials:
        if not material["ready"]:
            blockers.append(material["message"])
    if not required_quantities["ready"]:
        blockers.append(required_quantities["message"])
    if order.loyverse_inventory_sync_status == "success":
        blockers.append("Loyverse inventory sync already succeeded for this Production Order.")
    if order.loyverse_inventory_sync_status == "unknown":
        blockers.append("Previous Loyverse inventory sync status is unknown and requires manual verification.")

    return {
        "ready": not blockers,
        "status": "ready" if not blockers else "not_ready",
        "blockers": blockers,
        "token_present": token_present,
        "store_id_present": store_id_present,
        "store_id_preview": store_id if store_id_present else "",
        "order_closed": order.status == "closed",
        "finished_good": finished_good,
        "output_qty": output_qty,
        "real_unit_cost": real_unit_cost,
        "materials": materials,
        "required_quantities": required_quantities,
        "cost_limitation": (
            "Loyverse /inventory writes stock_after quantities only; it does not appear to accept "
            "ProductionOrder.real_unit_cost as part of the inventory update payload."
        ),
        "sync_model_note": (
            "Future sync would read GET /inventory, calculate absolute stock_after values, and write POST /inventory. "
            "No external inventory writes are performed in this readiness phase."
        ),
        "concurrency_warning": (
            "Because POST /inventory writes absolute stock_after values, stock changes in Loyverse between read and write "
            "could be overwritten by a future sync."
        ),
    }


def _finished_good_check(db: Session, order: ProductionOrder) -> dict:
    sku = (order.product_sku_snapshot or "").strip()
    if not sku:
        return {"ready": False, "message": "Finished product SKU snapshot is missing.", "variant_id": None}
    mapping = _resolve_variant_mapping(db, sku)
    if mapping["ready"]:
        mapping["message"] = f"Finished product {sku} has Loyverse variant mapping."
    else:
        mapping["message"] = f"Finished product {sku}: {mapping['message']}"
    return mapping


def _positive_quantity_check(value: Decimal | None, ready_message: str, missing_message: str) -> dict:
    if value is None or value <= ZERO:
        return {"ready": False, "message": missing_message, "value": value}
    return {"ready": True, "message": ready_message, "value": value}


def _real_unit_cost_check(order: ProductionOrder) -> dict:
    if order.real_unit_cost is None:
        return {"ready": False, "message": "Real unit cost is missing.", "value": None}
    return {"ready": True, "message": "Real unit cost is present, but /inventory does not appear to accept cost.", "value": order.real_unit_cost}


def _material_checks(db: Session, order: ProductionOrder) -> list[dict]:
    results = []
    materials = sorted(order.materials, key=lambda material: material.id)
    for material in materials:
        sku = (material.component_sku or "").strip()
        if not sku:
            results.append(
                {
                    "ready": False,
                    "component_sku": "",
                    "component_name": material.component_name or "",
                    "variant_id": None,
                    "message": "A material line is missing component SKU.",
                }
            )
            continue
        mapping = _resolve_variant_mapping(db, sku)
        if mapping["ready"]:
            message = f"Material component {sku} has Loyverse variant mapping."
        else:
            message = f"Material component {sku}: {mapping['message']}"
        results.append(
            {
                "ready": mapping["ready"],
                "component_sku": sku,
                "component_name": material.component_name or "",
                "variant_id": mapping["variant_id"],
                "message": message,
            }
        )
    return results


def _resolve_variant_mapping(db: Session, sku: str) -> dict:
    mappings = (
        db.query(LoyverseVariantMapping)
        .filter(LoyverseVariantMapping.active.is_(True), LoyverseVariantMapping.sku == sku)
        .order_by(LoyverseVariantMapping.item_name, LoyverseVariantMapping.variant_name)
        .all()
    )
    if len(mappings) == 1:
        return {
            "ready": True,
            "variant_id": mappings[0].loyverse_variant_id,
            "message": "Variant resolved by exact SKU match in local Loyverse mapping cache.",
        }
    if not mappings:
        return {
            "ready": False,
            "variant_id": None,
            "message": "No active local Loyverse variant mapping found for this SKU.",
        }
    return {
        "ready": False,
        "variant_id": None,
        "message": "Multiple active local Loyverse variant mappings found for this SKU.",
    }


def _required_quantity_check(order: ProductionOrder) -> dict:
    missing_skus = [
        material.component_sku or "line without SKU"
        for material in order.materials
        if material.required_quantity is None
    ]
    if missing_skus:
        return {
            "ready": False,
            "message": (
                "Some ProductionOrderMaterial lines are missing final required component quantities: "
                f"{', '.join(missing_skus)}. Inventory sync cannot safely calculate component stock_after values."
            ),
        }
    return {"ready": True, "message": "Final required component quantities are persisted for all material lines."}