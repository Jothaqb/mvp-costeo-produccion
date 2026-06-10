from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from fastapi import Request

from app.database import SessionLocal
from app.models import (
    AuditLog,
    B2BCustomerProduct,
    B2BSalesOrder,
    B2CSalesOrder,
    InventoryAdjustment,
    InventoryTransaction,
    PackagingBatch,
    PackagingBatchActivity,
    PackagingBatchLine,
    PackagingBatchLineMaterial,
    Product,
    ProductBomHeader,
    ProductionOrder,
    PurchaseOrder,
    User,
)


def _safe_json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if is_dataclass(value):
        return asdict(value)
    return str(value)


def serialize_audit_payload(payload: Any) -> str | None:
    if payload is None:
        return None
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, default=_safe_json_default)


def parse_audit_payload(payload: Any) -> Any | None:
    if payload is None:
        return None
    if isinstance(payload, (dict, list, int, float, bool)):
        return payload
    if isinstance(payload, str):
        text = payload.strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return payload
    return payload


def format_audit_payload_for_display(payload: Any) -> str:
    parsed = parse_audit_payload(payload)
    if parsed is None:
        return "No data"
    if isinstance(parsed, str):
        return parsed
    return json.dumps(parsed, ensure_ascii=False, indent=2, sort_keys=True, default=_safe_json_default)


def snapshot_product_for_audit(product: Product) -> dict[str, Any]:
    category = getattr(product, "category", None)
    supplier_record = getattr(product, "supplier_record", None)
    default_route = getattr(product, "default_route", None)
    return {
        "product_id": product.id,
        "sku": product.sku,
        "name": product.name,
        "description": product.description,
        "unit": product.unit,
        "category_id": product.category_id,
        "category_name": category.name if category is not None else None,
        "supplier_id": product.supplier_id,
        "supplier_name": supplier_record.name if supplier_record is not None else product.supplier,
        "default_route_id": product.default_route_id,
        "default_route_name": default_route.name if default_route is not None else None,
        "active": product.active,
        "available_for_sale_gc": product.available_for_sale_gc,
        "is_manufactured": product.is_manufactured,
        "is_purchased_product": product.is_purchased_product,
        "b2c_price": product.b2c_price,
        "b2b_price": product.b2b_price,
        "standard_cost": product.standard_cost,
    }


def snapshot_product_bom_for_audit(bom: ProductBomHeader | None) -> list[dict[str, Any]]:
    if bom is None:
        return []

    lines: list[dict[str, Any]] = []
    for line in bom.lines:
        component_product = getattr(line, "component_product", None)
        lines.append(
            {
                "component_product_id": line.component_product_id,
                "component_sku": line.component_sku_snapshot or (
                    component_product.sku if component_product is not None else None
                ),
                "component_name": line.component_name_snapshot or (
                    component_product.name if component_product is not None else None
                ),
                "unit": line.unit_snapshot or (component_product.unit if component_product is not None else None),
                "quantity_standard": line.quantity_standard,
                "component_type": line.component_type,
                "include_in_real_cost": line.include_in_real_cost,
                "notes": line.notes,
            }
        )

    return sorted(
        lines,
        key=lambda item: (
            str(item.get("component_sku") or ""),
            str(item.get("component_product_id") or ""),
            str(item.get("quantity_standard") or ""),
            str(item.get("component_name") or ""),
            str(item.get("notes") or ""),
        ),
    )


def snapshot_b2b_customer_product_for_audit(
    product_line: B2BCustomerProduct,
    *,
    customer_name: str | None = None,
    product_ref: Product | None = None,
) -> dict[str, Any]:
    resolved_customer_name = customer_name
    if resolved_customer_name is None:
        customer = getattr(product_line, "customer", None)
        resolved_customer_name = customer.customer_name if customer is not None else None

    payload: dict[str, Any] = {
        "customer_id": product_line.customer_id,
        "customer_name": resolved_customer_name,
        "product_line_id": product_line.id,
        "sku": product_line.sku,
        "description": product_line.description,
        "distributor_price": product_line.distributor_price,
        "active": product_line.active,
    }
    if product_ref is not None:
        payload["product_id"] = product_ref.id
        payload["b2c_price_reference"] = product_ref.b2c_price
        payload["product_cost_reference"] = product_ref.standard_cost
    return payload


def diff_audit_snapshots(
    old_values: dict[str, Any] | None,
    new_values: dict[str, Any] | None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    old_payload = old_values or {}
    new_payload = new_values or {}
    changed_old: dict[str, Any] = {}
    changed_new: dict[str, Any] = {}

    for key in sorted(set(old_payload) | set(new_payload)):
        old_value = old_payload.get(key)
        new_value = new_payload.get(key)
        if old_value == new_value:
            continue
        changed_old[key] = old_value
        changed_new[key] = new_value

    if not changed_old and not changed_new:
        return None, None
    return changed_old, changed_new


def diff_audit_snapshot_rows(
    old_rows: list[dict[str, Any]] | None,
    new_rows: list[dict[str, Any]] | None,
    *,
    key_field: str,
    identity_fields: list[str] | None = None,
) -> tuple[list[dict[str, Any]] | None, list[dict[str, Any]] | None]:
    old_payload = old_rows or []
    new_payload = new_rows or []
    old_by_key = {str(row.get(key_field)): row for row in old_payload}
    new_by_key = {str(row.get(key_field)): row for row in new_payload}
    changed_old_rows: list[dict[str, Any]] = []
    changed_new_rows: list[dict[str, Any]] = []
    identity_keys = identity_fields or [key_field]

    for row_key in sorted(set(old_by_key) | set(new_by_key)):
        old_row = old_by_key.get(row_key)
        new_row = new_by_key.get(row_key)
        changed_old, changed_new = diff_audit_snapshots(old_row, new_row)
        if changed_old is None and changed_new is None:
            continue

        old_entry = changed_old or {}
        new_entry = changed_new or {}
        source_row = new_row or old_row or {}
        for identity_key in identity_keys:
            identity_value = source_row.get(identity_key)
            old_entry.setdefault(identity_key, identity_value)
            new_entry.setdefault(identity_key, identity_value)
        changed_old_rows.append(old_entry)
        changed_new_rows.append(new_entry)

    if not changed_old_rows and not changed_new_rows:
        return None, None
    return changed_old_rows, changed_new_rows


def summarize_historical_import_result(result: Any) -> dict[str, Any]:
    warnings = list(getattr(result, "warnings", []) or [])
    errors = list(getattr(result, "errors", []) or [])
    summary: dict[str, Any] = {
        "file_name": getattr(result, "file_name", None),
        "total_rows": getattr(result, "total_rows", None),
        "invalid_rows": getattr(result, "invalid_rows", None),
        "warnings_count": len(warnings),
        "errors_count": len(errors),
    }

    created_count = (
        getattr(result, "created_orders", None)
        if hasattr(result, "created_orders")
        else getattr(result, "created_purchase_orders", None)
        if hasattr(result, "created_purchase_orders")
        else getattr(result, "created_production_orders", None)
        if hasattr(result, "created_production_orders")
        else None
    )
    skipped_count = (
        getattr(result, "skipped_existing_orders", None)
        if hasattr(result, "skipped_existing_orders")
        else getattr(result, "skipped_existing_purchase_orders", None)
        if hasattr(result, "skipped_existing_purchase_orders")
        else getattr(result, "skipped_existing_production_orders", None)
        if hasattr(result, "skipped_existing_production_orders")
        else None
    )
    if created_count is not None:
        summary["created_count"] = created_count
    if skipped_count is not None:
        summary["skipped_count"] = skipped_count
    if hasattr(result, "created_lines"):
        summary["created_lines"] = getattr(result, "created_lines")

    warning_examples = [
        f"row {getattr(item, 'row_number', '?')}: {getattr(item, 'message', str(item))}"
        for item in warnings[:5]
    ]
    error_examples = [
        f"row {getattr(item, 'row_number', '?')}: {getattr(item, 'message', str(item))}"
        for item in errors[:5]
    ]
    if warning_examples:
        summary["warning_examples"] = warning_examples
    if error_examples:
        summary["error_examples"] = error_examples
    return summary


def snapshot_b2b_sales_order_for_audit(order: B2BSalesOrder) -> dict[str, Any]:
    return {
        "order_id": order.id,
        "order_number": order.order_number,
        "customer_id": order.customer_id,
        "customer_name": order.customer_name_snapshot,
        "status": order.status,
        "delivery_date": order.delivery_date,
        "total_amount": order.total_amount,
        "cost_total_snapshot": order.cost_total_snapshot,
        "gross_margin_amount": order.gross_margin_amount,
        "gross_margin_percent": order.gross_margin_percent,
        "loyverse_receipt_id": order.loyverse_receipt_id,
        "loyverse_receipt_number": order.loyverse_receipt_number,
        "loyverse_invoice_sync_status": order.loyverse_invoice_sync_status,
        "loyverse_invoice_synced_at": order.loyverse_invoice_synced_at,
    }


def snapshot_b2c_sales_order_for_audit(order: B2CSalesOrder) -> dict[str, Any]:
    return {
        "order_id": order.id,
        "order_number": order.order_number,
        "b2c_customer_id": order.b2c_customer_id,
        "customer_name": order.customer_name,
        "status": order.status,
        "order_date": order.order_date,
        "subtotal_amount": order.subtotal_amount,
        "discount_amount": order.discount_amount,
        "total_amount": order.total_amount,
        "cost_total_snapshot": order.cost_total_snapshot,
        "gross_margin_amount": order.gross_margin_amount,
        "gross_margin_percent": order.gross_margin_percent,
    }


def snapshot_purchase_order_for_audit(order: PurchaseOrder) -> dict[str, Any]:
    lines = sorted(order.lines, key=lambda line: (line.line_number, line.id))
    return {
        "purchase_order_id": order.id,
        "po_number": order.po_number,
        "supplier_name": order.supplier_name_snapshot,
        "po_date": order.po_date,
        "status": order.status,
        "estimated_total": order.estimated_total,
        "lines": [
            {
                "line_id": line.id,
                "sku": line.sku_snapshot,
                "quantity": line.quantity,
                "received_quantity": line.received_quantity,
                "unit_cost": line.unit_cost_snapshot,
            }
            for line in lines
        ],
    }


def snapshot_production_order_for_audit(order: ProductionOrder) -> dict[str, Any]:
    return {
        "production_order_id": order.id,
        "internal_order_number": order.internal_order_number,
        "status": order.status,
        "production_date": order.production_date,
        "product_id": order.product_id,
        "product_sku": order.product_sku_snapshot,
        "product_name": order.product_name_snapshot,
        "route_id": order.route_id,
        "route_name": order.route_name_snapshot,
        "planned_qty": order.planned_qty,
        "input_qty": order.input_qty,
        "output_qty": order.output_qty,
        "yield_percent": order.yield_percent,
        "material_snapshot_cost_total": order.material_snapshot_cost_total,
        "real_labor_cost_total": order.real_labor_cost_total,
        "real_machine_cost_total": order.real_machine_cost_total,
        "real_overhead_cost_total": order.real_overhead_cost_total,
        "real_total_cost": order.real_total_cost,
        "real_unit_cost": order.real_unit_cost,
        "variance_amount": order.variance_amount,
        "variance_percent": order.variance_percent,
        "closed_at": order.closed_at,
    }


def snapshot_production_order_activities_for_audit(order: ProductionOrder) -> list[dict[str, Any]]:
    return [
        {
            "activity_id": activity.id,
            "sequence": activity.sequence,
            "activity_code": activity.activity_code_snapshot,
            "activity_name": activity.activity_name_snapshot,
            "labor_minutes": activity.labor_minutes,
            "machine_minutes": activity.machine_minutes,
            "labor_cost": activity.labor_cost,
            "machine_cost": activity.machine_cost,
            "overhead_cost": activity.overhead_cost,
            "total_activity_cost": activity.total_activity_cost,
            "notes": activity.notes,
        }
        for activity in sorted(order.activities, key=lambda item: (item.sequence, item.id))
    ]


def snapshot_production_order_bom_for_audit(order: ProductionOrder) -> list[dict[str, Any]]:
    return [
        {
            "material_id": material.id,
            "component_sku": material.component_sku,
            "component_name": material.component_name,
            "quantity_standard": material.quantity_standard,
            "required_quantity": material.required_quantity,
            "unit_cost_snapshot": material.unit_cost_snapshot,
            "line_cost": material.line_cost,
            "component_type": material.component_type,
            "include_in_real_cost": material.include_in_real_cost,
        }
        for material in sorted(order.materials, key=lambda item: (str(item.component_sku or ""), item.id))
    ]


def snapshot_packaging_batch_for_audit(batch: PackagingBatch) -> dict[str, Any]:
    return {
        "packaging_batch_id": batch.id,
        "internal_batch_number": batch.internal_batch_number,
        "status": batch.status,
        "production_date": batch.production_date,
        "packaging_type": batch.packaging_type,
        "route_id": batch.route_id,
        "route_name": batch.route_name_snapshot,
        "route_version": batch.route_version_snapshot,
        "process_type": batch.process_type,
        "real_labor_cost_total": batch.real_labor_cost_total,
        "real_overhead_cost_total": batch.real_overhead_cost_total,
        "real_machine_cost_total": batch.real_machine_cost_total,
        "real_activity_cost_total": batch.real_activity_cost_total,
        "activity_cost_status": batch.activity_cost_status,
        "closed_at": batch.closed_at,
        "closed_by_user_id": batch.closed_by_user_id,
        "close_notes": batch.close_notes,
    }


def snapshot_packaging_batch_lines_for_audit(batch: PackagingBatch) -> list[dict[str, Any]]:
    return [
        {
            "line_id": line.id,
            "line_number": line.line_number,
            "product_id": line.product_id,
            "product_sku": line.product_sku_snapshot,
            "product_name": line.product_name_snapshot,
            "planned_qty": line.planned_qty,
            "material_snapshot_cost_total": line.material_snapshot_cost_total,
            "material_snapshot_status": line.material_snapshot_status,
            "real_labor_cost": line.real_labor_cost,
            "real_overhead_cost": line.real_overhead_cost,
            "real_machine_cost": line.real_machine_cost,
            "real_total_cost": line.real_total_cost,
            "real_unit_cost": line.real_unit_cost,
            "cost_distribution_status": line.cost_distribution_status,
            "cost_distributed_at": line.cost_distributed_at,
            "notes": line.notes,
        }
        for line in sorted(batch.lines, key=lambda item: (item.line_number, item.id))
    ]


def snapshot_packaging_batch_materials_for_audit(batch: PackagingBatch) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in sorted(batch.lines, key=lambda item: (item.line_number, item.id)):
        for material in sorted(line.materials, key=lambda item: (str(item.component_sku or ""), item.id)):
            rows.append(
                {
                    "line_id": line.id,
                    "line_number": line.line_number,
                    "material_id": material.id,
                    "finished_good_sku": line.product_sku_snapshot,
                    "component_sku": material.component_sku,
                    "component_name": material.component_name,
                    "quantity_standard": material.quantity_standard,
                    "required_quantity": material.required_quantity,
                    "unit_cost_snapshot": material.unit_cost_snapshot,
                    "line_cost": material.line_cost,
                    "component_type": material.component_type,
                    "include_in_real_cost": material.include_in_real_cost,
                }
            )
    return rows


def snapshot_packaging_batch_activities_for_audit(batch: PackagingBatch) -> list[dict[str, Any]]:
    return [
        {
            "activity_id": activity.id,
            "sequence": activity.sequence,
            "activity_code": activity.activity_code_snapshot,
            "activity_name": activity.activity_name_snapshot,
            "labor_minutes": activity.labor_minutes,
            "machine_minutes": activity.machine_minutes,
            "labor_cost": activity.labor_cost,
            "overhead_cost": activity.overhead_cost,
            "machine_cost": activity.machine_cost,
            "total_activity_cost": activity.total_activity_cost,
            "notes": activity.notes,
        }
        for activity in sorted(batch.activities, key=lambda item: (item.sequence, item.id))
    ]


def snapshot_inventory_transactions_for_audit(
    transactions: list[InventoryTransaction],
) -> list[dict[str, Any]]:
    return [
        {
            "transaction_id": transaction.id,
            "product_id": transaction.product_id,
            "transaction_date": transaction.transaction_date,
            "transaction_type": transaction.transaction_type,
            "source_type": transaction.source_type,
            "source_id": transaction.source_id,
            "source_line_id": transaction.source_line_id,
            "quantity_in": transaction.quantity_in,
            "quantity_out": transaction.quantity_out,
            "unit_cost": transaction.unit_cost,
            "total_cost": transaction.total_cost,
            "running_quantity": transaction.running_quantity,
            "running_average_cost": transaction.running_average_cost,
            "running_inventory_value": transaction.running_inventory_value,
            "notes": transaction.notes,
        }
        for transaction in transactions
    ]


def snapshot_inventory_adjustment_for_audit(adjustment: InventoryAdjustment) -> dict[str, Any]:
    return {
        "adjustment_id": adjustment.id,
        "adjustment_number": adjustment.adjustment_number,
        "adjustment_date": adjustment.adjustment_date,
        "product_id": adjustment.product_id,
        "sku_snapshot": adjustment.sku_snapshot,
        "product_name_snapshot": adjustment.product_name_snapshot,
        "adjustment_mode": adjustment.adjustment_mode,
        "adjustment_type": adjustment.adjustment_type,
        "transaction_type": adjustment.transaction_type,
        "reason": adjustment.reason,
        "current_qty_snapshot": adjustment.current_qty_snapshot,
        "counted_qty": adjustment.counted_qty,
        "quantity_adjustment": adjustment.quantity_adjustment,
        "unit_cost": adjustment.unit_cost,
        "total_cost": adjustment.total_cost,
        "status": adjustment.status,
        "warning_notes": adjustment.warning_notes,
    }


def snapshot_planning_parameter_product_for_audit(product: Product) -> dict[str, Any]:
    return {
        "product_id": product.id,
        "sku": product.sku,
        "name": product.name,
        "planning_moq": product.planning_moq,
        "low_stock_qty": product.low_stock_qty,
        "optimal_stock_qty": product.optimal_stock_qty,
        "planning_zones_manual_override": product.planning_zones_manual_override,
    }


def snapshot_planner_quantity_for_audit(product: Product) -> dict[str, Any]:
    return {
        "product_id": product.id,
        "sku": product.sku,
        "name": product.name,
        "planning_quantity": product.planning_quantity,
    }


def _request_ip_address(request: Request | None) -> str | None:
    if request is None:
        return None
    forwarded_for = (request.headers.get("x-forwarded-for") or "").strip()
    if forwarded_for:
        first_ip = forwarded_for.split(",")[0].strip()
        if first_ip:
            return first_ip[:255]
    if request.client is not None and request.client.host:
        return request.client.host[:255]
    return None


def _resolve_user_context(
    request: Request | None,
    user: User | None,
    username: str | None,
) -> tuple[int | None, str]:
    resolved_user = user
    if resolved_user is None and request is not None:
        resolved_user = getattr(request.state, "current_user", None)
    if resolved_user is not None:
        return resolved_user.id, resolved_user.username
    if username is not None and username.strip():
        return None, username.strip()[:100]
    return None, "anonymous"


def safe_log_audit_event(
    *,
    module: str,
    action: str,
    entity_type: str | None = None,
    entity_id: str | int | None = None,
    entity_label: str | None = None,
    old_values: Any = None,
    new_values: Any = None,
    notes: str | None = None,
    request: Request | None = None,
    user: User | None = None,
    username: str | None = None,
) -> None:
    db = None
    try:
        db = SessionLocal()
        resolved_user_id, resolved_username = _resolve_user_context(request, user, username)
        entry = AuditLog(
            user_id=resolved_user_id,
            username=resolved_username,
            module=(module or "").strip()[:100] or "unknown",
            action=(action or "").strip()[:100] or "unknown",
            entity_type=(entity_type or "").strip()[:100] or None,
            entity_id=None if entity_id is None else str(entity_id)[:100],
            entity_label=(entity_label or "").strip()[:255] or None,
            old_values=serialize_audit_payload(old_values),
            new_values=serialize_audit_payload(new_values),
            request_path=(request.url.path[:500] if request is not None else None),
            method=((request.method or "").strip()[:20] if request is not None else None) or None,
            ip_address=_request_ip_address(request),
            notes=(notes or "").strip() or None,
        )
        db.add(entry)
        db.commit()
    except Exception as exc:
        if db is not None:
            db.rollback()
        print(f"[audit] failed to log event {module}.{action}: {exc}")
    finally:
        if db is not None:
            db.close()
