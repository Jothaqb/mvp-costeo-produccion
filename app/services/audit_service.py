from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from fastapi import Request

from app.database import SessionLocal
from app.models import AuditLog, B2BCustomerProduct, Product, ProductBomHeader, User


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


def snapshot_product_for_audit(product: Product) -> dict[str, Any]:
    category = getattr(product, "category", None)
    supplier_record = getattr(product, "supplier_record", None)
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
