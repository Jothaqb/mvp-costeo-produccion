import json
import os
from datetime import datetime
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from sqlalchemy.orm import Session, joinedload

from app.models import (
    B2BSalesOrder,
    LoyverseCustomerMapping,
    LoyversePaymentTypeMapping,
    LoyverseVariantMapping,
)


LOYVERSE_API_BASE_URL = "https://api.loyverse.com/v1.0"
LOYVERSE_REQUEST_TIMEOUT_SECONDS = 8


class LoyverseMappingSyncError(Exception):
    pass


def refresh_loyverse_customer_mappings(db: Session) -> dict[str, int]:
    token = _require_token()
    refreshed_at = datetime.utcnow()
    customers = _get_paginated_collection(token, "/customers", "customers")

    db.query(LoyverseCustomerMapping).update({LoyverseCustomerMapping.active: False})
    created = 0
    updated = 0
    skipped = 0

    for customer in customers:
        loyverse_customer_id = _string_value(customer, "id", "customer_id")
        if not loyverse_customer_id:
            skipped += 1
            continue

        mapping = (
            db.query(LoyverseCustomerMapping)
            .filter(LoyverseCustomerMapping.loyverse_customer_id == loyverse_customer_id)
            .one_or_none()
        )
        if mapping is None:
            mapping = LoyverseCustomerMapping(loyverse_customer_id=loyverse_customer_id)
            db.add(mapping)
            created += 1
        else:
            updated += 1

        mapping.customer_name = _string_value(customer, "name", "customer_name")
        mapping.phone = _string_value(customer, "phone_number", "phone")
        mapping.email = _string_value(customer, "email")
        mapping.active = True
        mapping.last_refreshed_at = refreshed_at

    db.commit()
    return {"created": created, "updated": updated, "skipped": skipped, "total": len(customers)}


def refresh_loyverse_variant_mappings(db: Session) -> dict[str, int]:
    token = _require_token()
    refreshed_at = datetime.utcnow()
    items = _get_paginated_collection(token, "/items", "items")

    db.query(LoyverseVariantMapping).update({LoyverseVariantMapping.active: False})
    created = 0
    updated = 0
    skipped = 0

    for item in items:
        item_id = _string_value(item, "id", "item_id")
        item_name = _string_value(item, "item_name", "name")
        variants = item.get("variants") or []
        if not variants and _string_value(item, "variant_id", "sku"):
            variants = [item]

        for variant in variants:
            variant_id = _string_value(variant, "variant_id", "id")
            if not variant_id:
                skipped += 1
                continue

            mapping = (
                db.query(LoyverseVariantMapping)
                .filter(LoyverseVariantMapping.loyverse_variant_id == variant_id)
                .one_or_none()
            )
            if mapping is None:
                mapping = LoyverseVariantMapping(loyverse_variant_id=variant_id)
                db.add(mapping)
                created += 1
            else:
                updated += 1

            mapping.sku = _string_value(variant, "sku") or _string_value(item, "sku")
            mapping.loyverse_item_id = _string_value(variant, "item_id") or item_id
            mapping.item_name = item_name
            mapping.variant_name = _string_value(variant, "variant_name", "name")
            mapping.active = True
            mapping.last_refreshed_at = refreshed_at

    db.commit()
    return {"created": created, "updated": updated, "skipped": skipped, "total": created + updated + skipped}


def refresh_loyverse_payment_type_mappings(db: Session) -> dict[str, int]:
    token = _require_token()
    refreshed_at = datetime.utcnow()
    payment_types = _get_paginated_collection(token, "/payment_types", "payment_types")

    db.query(LoyversePaymentTypeMapping).update({LoyversePaymentTypeMapping.active: False})
    created = 0
    updated = 0
    skipped = 0

    for payment_type in payment_types:
        loyverse_payment_type_id = _string_value(payment_type, "id", "payment_type_id")
        name = _string_value(payment_type, "name")
        if not loyverse_payment_type_id or not name:
            skipped += 1
            continue

        mapping = (
            db.query(LoyversePaymentTypeMapping)
            .filter(LoyversePaymentTypeMapping.loyverse_payment_type_id == loyverse_payment_type_id)
            .one_or_none()
        )
        if mapping is None:
            mapping = LoyversePaymentTypeMapping(loyverse_payment_type_id=loyverse_payment_type_id, name=name)
            db.add(mapping)
            created += 1
        else:
            updated += 1

        mapping.name = name
        mapping.payment_type = _string_value(payment_type, "type", "payment_type") or None
        mapping.active = True
        mapping.last_refreshed_at = refreshed_at

    db.commit()
    return {"created": created, "updated": updated, "skipped": skipped, "total": len(payment_types)}


def build_b2b_invoice_readiness(db: Session, order_id: int) -> dict:
    order = (
        db.query(B2BSalesOrder)
        .options(joinedload(B2BSalesOrder.customer), joinedload(B2BSalesOrder.lines))
        .filter(B2BSalesOrder.id == order_id)
        .one()
    )
    token_present = bool(os.getenv("LOYVERSE_API_TOKEN", "").strip())
    store_id_present = bool(os.getenv("LOYVERSE_STORE_ID", "").strip())
    config_ready = token_present and store_id_present
    config_messages = []
    if token_present:
        config_messages.append("LOYVERSE_API_TOKEN is configured.")
    else:
        config_messages.append("LOYVERSE_API_TOKEN is missing.")
    if store_id_present:
        config_messages.append("LOYVERSE_STORE_ID is configured.")
    else:
        config_messages.append("LOYVERSE_STORE_ID is missing.")

    customer = _resolve_customer_mapping(db, order)
    channel = _resolve_channel_mapping(db, order)
    line_results = [_resolve_variant_mapping(db, line.sku_snapshot) for line in sorted(order.lines, key=lambda line: line.line_number)]
    ready = config_ready and customer["ready"] and channel["ready"] and all(line["ready"] for line in line_results)
    return {
        "ready": ready,
        "config_ready": config_ready,
        "token_present": token_present,
        "store_id_present": store_id_present,
        "token_message": " ".join(config_messages),
        "customer": customer,
        "channel": channel,
        "lines": line_results,
        "note": "Readiness only validates mappings and token/store config. It does not create a Loyverse receipt.",
    }


def _resolve_customer_mapping(db: Session, order: B2BSalesOrder) -> dict:
    snapshot_id = (order.loyverse_customer_id_snapshot or "").strip()
    if snapshot_id:
        return {"ready": True, "source": "order snapshot", "loyverse_customer_id": snapshot_id, "message": "Customer id is stored on the order snapshot."}

    master_id = ""
    if order.customer is not None:
        master_id = (order.customer.loyverse_customer_id or "").strip()
    if master_id:
        return {"ready": True, "source": "customer master", "loyverse_customer_id": master_id, "message": "Customer id is stored on the B2B customer master."}

    phone = (order.phone_snapshot or "").strip()
    if not phone:
        return {"ready": False, "source": None, "loyverse_customer_id": None, "message": "No Loyverse customer id or customer phone is available."}

    normalized_phone = _normalize_phone(phone)
    candidates = [
        mapping
        for mapping in db.query(LoyverseCustomerMapping).filter(LoyverseCustomerMapping.active.is_(True)).all()
        if _normalize_phone(mapping.phone or "") == normalized_phone
    ]
    if len(candidates) == 1:
        return {
            "ready": True,
            "source": "local phone mapping",
            "loyverse_customer_id": candidates[0].loyverse_customer_id,
            "message": "Customer resolved by unique phone match in local Loyverse mapping cache.",
        }
    if not candidates:
        return {"ready": False, "source": "local phone mapping", "loyverse_customer_id": None, "message": f"No local Loyverse customer mapping found for phone {phone}."}
    return {"ready": False, "source": "local phone mapping", "loyverse_customer_id": None, "message": f"Multiple local Loyverse customer mappings found for phone {phone}."}


def _resolve_channel_mapping(db: Session, order: B2BSalesOrder) -> dict:
    payment_type_id = (order.loyverse_payment_type_id_snapshot or "").strip()
    channel_name = (order.b2b_channel_name_snapshot or "").strip()
    if not payment_type_id:
        return {"ready": False, "loyverse_payment_type_id": None, "message": "B2B Channel is not selected for this order."}

    mapping = (
        db.query(LoyversePaymentTypeMapping)
        .filter(
            LoyversePaymentTypeMapping.active.is_(True),
            LoyversePaymentTypeMapping.loyverse_payment_type_id == payment_type_id,
        )
        .one_or_none()
    )
    if mapping is None:
        return {
            "ready": False,
            "loyverse_payment_type_id": payment_type_id,
            "message": f"Selected B2B Channel is not active in the local Loyverse payment type cache: {channel_name or payment_type_id}.",
        }
    return {
        "ready": True,
        "loyverse_payment_type_id": payment_type_id,
        "message": f"B2B Channel is mapped to Loyverse payment type: {channel_name or mapping.name}.",
    }


def _resolve_variant_mapping(db: Session, sku: str) -> dict:
    mappings = (
        db.query(LoyverseVariantMapping)
        .filter(LoyverseVariantMapping.active.is_(True), LoyverseVariantMapping.sku == sku)
        .order_by(LoyverseVariantMapping.item_name, LoyverseVariantMapping.variant_name)
        .all()
    )
    if len(mappings) == 1:
        mapping = mappings[0]
        return {
            "ready": True,
            "sku": sku,
            "loyverse_variant_id": mapping.loyverse_variant_id,
            "loyverse_item_id": mapping.loyverse_item_id,
            "message": "Variant resolved by exact SKU match in local Loyverse mapping cache.",
        }
    if not mappings:
        return {"ready": False, "sku": sku, "loyverse_variant_id": None, "loyverse_item_id": None, "message": "No active local Loyverse variant mapping found for this SKU."}
    return {"ready": False, "sku": sku, "loyverse_variant_id": None, "loyverse_item_id": None, "message": "Multiple active local Loyverse variant mappings found for this SKU."}


def _require_token() -> str:
    token = os.getenv("LOYVERSE_API_TOKEN", "").strip()
    if not token:
        raise LoyverseMappingSyncError("LOYVERSE_API_TOKEN is not configured.")
    return token


def _get_paginated_collection(token: str, path: str, collection_key: str) -> list[dict]:
    rows = []
    cursor = None
    while True:
        query = f"?{urlencode({'cursor': cursor})}" if cursor else ""
        payload = _loyverse_request(token, f"{path}{query}")
        collection = payload.get(collection_key) or []
        if not isinstance(collection, list):
            raise LoyverseMappingSyncError(f"Loyverse response for {path} did not include a valid {collection_key} list.")
        rows.extend(collection)
        cursor = _string_value(payload, "cursor", "next_cursor")
        if not cursor:
            return rows


def _loyverse_request(token: str, path: str) -> dict:
    request = Request(
        f"{LOYVERSE_API_BASE_URL}{path}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        method="GET",
    )
    try:
        with urlopen(request, timeout=LOYVERSE_REQUEST_TIMEOUT_SECONDS) as response:
            response_text = response.read().decode("utf-8")
    except HTTPError as exc:
        response_text = exc.read().decode("utf-8", errors="replace")
        raise LoyverseMappingSyncError(f"Loyverse API returned HTTP {exc.code}: {_summarize_response(response_text)}") from exc
    except URLError as exc:
        raise LoyverseMappingSyncError(f"Loyverse API request failed: {exc.reason}") from exc
    except TimeoutError as exc:
        raise LoyverseMappingSyncError("Loyverse API request timed out.") from exc

    if not response_text:
        return {}
    try:
        return json.loads(response_text)
    except json.JSONDecodeError as exc:
        raise LoyverseMappingSyncError("Loyverse API returned invalid JSON.") from exc


def _string_value(payload: dict, *keys: str) -> str:
    for key in keys:
        value = payload.get(key)
        if value is not None:
            return str(value).strip()
    return ""


def _normalize_phone(value: str) -> str:
    return "".join(character for character in value if character.isdigit())


def _summarize_response(response_text: str) -> str:
    text = " ".join(response_text.split())
    if len(text) > 500:
        return f"{text[:500]}..."
    return text