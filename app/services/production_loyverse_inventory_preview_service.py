import base64
import hashlib
import hmac
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from sqlalchemy.orm import Session, joinedload

from app.models import LoyverseVariantMapping, ProductionOrder
from app.services.production_loyverse_inventory_readiness_service import build_production_inventory_readiness


LOYVERSE_API_BASE_URL = "https://api.loyverse.com/v1.0"
LOYVERSE_REQUEST_TIMEOUT_SECONDS = 8
ZERO = Decimal("0")
INVENTORY_LIMIT = 250
PREVIEW_TOKEN_TTL_SECONDS = 300
DECIMAL_QUANT = Decimal("0.0001")
PARTIAL_SUCCESS_STATUS = "partial_success"


class ProductionInventoryPreviewError(Exception):
    pass


@dataclass(frozen=True)
class PreviewParticipant:
    row_key: str
    row_type: str
    sku: str
    name: str
    variant_id: str
    movement_quantity: Decimal
    sort_key: str


def build_production_inventory_preview(db: Session, order_id: int, include_token: bool = True) -> dict:
    readiness = build_production_inventory_readiness(db, order_id)
    if not readiness["ready"]:
        raise ProductionInventoryPreviewError("Production Order is not ready for Loyverse inventory preview.")

    order = (
        db.query(ProductionOrder)
        .options(joinedload(ProductionOrder.materials))
        .filter(ProductionOrder.id == order_id)
        .one()
    )
    token = _require_token()
    store_id = _require_store_id()
    participants = _build_participants(db, order)
    variant_ids = [participant.variant_id for participant in participants]
    inventory_levels = _get_inventory_levels(token, store_id, variant_ids)
    inventory_lookup = _build_inventory_lookup(inventory_levels, store_id)
    prior_state = _load_prior_sync_state(order)

    rows = []
    for participant in participants:
        inventory_row = inventory_lookup.get(participant.variant_id)
        if inventory_row is None:
            current_stock = ZERO
            stock_source = "No row returned; preview assumes 0"
            stock_warning = "No current inventory row was returned for this variant/store after the completed GET /inventory scope."
            inventory_row_returned = False
        else:
            current_stock = _decimal_value(inventory_row, "in_stock", "stock_after", "stock", "available")
            stock_source = "Returned by Loyverse"
            stock_warning = "Current Loyverse stock is already negative." if current_stock < ZERO else ""
            inventory_row_returned = True

        effective_current_stock = max(current_stock, ZERO)
        zero_floor_applied = inventory_row_returned and current_stock < ZERO
        if participant.row_type == "finished_good":
            proposed_stock_after = effective_current_stock + participant.movement_quantity
        else:
            proposed_stock_after = max(effective_current_stock - participant.movement_quantity, ZERO)

        classification = _classify_row(participant, inventory_row_returned, prior_state)
        rows.append(
            {
                "row_key": participant.row_key,
                "row_type": participant.row_type,
                "label": "Finished good" if participant.row_type == "finished_good" else "Component",
                "sku": participant.sku,
                "name": participant.name,
                "variant_id": participant.variant_id,
                "store_id": store_id,
                "current_stock": _quantize(current_stock),
                "effective_current_stock": _quantize(effective_current_stock),
                "stock_source": stock_source,
                "stock_warning": stock_warning,
                "inventory_row_returned": inventory_row_returned,
                "movement_quantity": _quantize(participant.movement_quantity),
                "proposed_stock_after": _quantize(proposed_stock_after),
                "zero_floor_applied": zero_floor_applied,
                "sync_category": classification["sync_category"],
                "sync_eligible": classification["sync_eligible"],
                "sync_note": classification["sync_note"],
                "physical_review_required": classification["physical_review_required"],
                "sort_key": participant.sort_key,
            }
        )

    fingerprint_payload = build_preview_fingerprint_payload(order.id, store_id, rows)
    fingerprint = fingerprint_preview_payload(fingerprint_payload)
    payload_summary = summarize_preview_payload(fingerprint_payload)
    write_blockers = build_write_blockers(rows, prior_state)
    generated_at = datetime.utcnow()
    preview_token = create_preview_token(order.id, store_id, fingerprint, generated_at) if include_token else ""

    included_rows = [row for row in rows if row["sync_category"] == "included"]
    excluded_rows = [row for row in rows if row["sync_category"] == "excluded"]
    already_synced_rows = [row for row in rows if row["sync_category"] == "already_synced"]
    zero_floor_rows = [row for row in rows if row["zero_floor_applied"]]

    return {
        "order": order,
        "store_id": store_id,
        "previewed_at": generated_at,
        "expires_at": generated_at + timedelta(seconds=PREVIEW_TOKEN_TTL_SECONDS),
        "rows": rows,
        "included_rows": included_rows,
        "excluded_rows": excluded_rows,
        "already_synced_rows": already_synced_rows,
        "zero_floor_rows": zero_floor_rows,
        "fingerprint_payload": fingerprint_payload,
        "fingerprint": fingerprint,
        "payload_summary": payload_summary,
        "preview_token": preview_token,
        "write_blockers": write_blockers,
        "write_allowed": not write_blockers,
        "prior_sync_state": prior_state,
        "get_scope": {
            "store_id": store_id,
            "variant_count": len(variant_ids),
            "variant_ids": variant_ids,
            "completed": True,
            "strategy": "GET /inventory filtered by LOYVERSE_STORE_ID and required variant_ids, following cursor pagination.",
        },
        "concurrency_warning": (
            "Loyverse inventory sync writes absolute stock_after values. If stock changes in Loyverse after this preview "
            "is calculated, a future write using these values may overwrite newer inventory levels."
        ),
        "cost_limitation": (
            "This preview shows ProductionOrder.real_unit_cost for context only. The documented /inventory payload appears "
            "to update stock levels only and does not appear to accept real_unit_cost."
        ),
    }


def build_preview_fingerprint_payload(order_id: int, store_id: str, rows: list[dict]) -> dict:
    normalized_rows = []
    for index, row in enumerate(rows):
        normalized_rows.append(
            {
                "index": index,
                "row_key": row["row_key"],
                "row_type": row["row_type"],
                "sku": row["sku"],
                "variant_id": row["variant_id"],
                "store_id": store_id,
                "current_stock": decimal_to_string(row["current_stock"]),
                "effective_current_stock": decimal_to_string(row["effective_current_stock"]),
                "movement_quantity": decimal_to_string(row["movement_quantity"]),
                "proposed_stock_after": decimal_to_string(row["proposed_stock_after"]),
                "inventory_row_returned": bool(row["inventory_row_returned"]),
                "zero_floor_applied": bool(row["zero_floor_applied"]),
                "sync_category": row["sync_category"],
                "sync_eligible": bool(row["sync_eligible"]),
            }
        )
    return {
        "order_id": order_id,
        "store_id": store_id,
        "rows": normalized_rows,
    }


def fingerprint_preview_payload(payload: dict) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def summarize_preview_payload(payload: dict) -> str:
    rows = []
    for row in payload["rows"]:
        rows.append(
            f"{row['row_key']} {row['row_type']} sku={row['sku']} variant={row['variant_id']} "
            f"current={row['current_stock']} effective={row['effective_current_stock']} "
            f"movement={row['movement_quantity']} stock_after={row['proposed_stock_after']} "
            f"returned={row['inventory_row_returned']} category={row['sync_category']}"
        )
    return _truncate_summary(f"store={payload['store_id']}; " + "; ".join(rows), 2000)


def build_inventory_post_payload(preview: dict) -> dict:
    inventory_levels = []
    for row in preview["rows"]:
        if not row["sync_eligible"]:
            continue
        inventory_levels.append(
            {
                "variant_id": row["variant_id"],
                "store_id": preview["store_id"],
                "stock_after": decimal_to_number(row["proposed_stock_after"]),
            }
        )
    return {"inventory_levels": inventory_levels}


def build_write_blockers(rows: list[dict], prior_state: dict | None = None) -> list[str]:
    blockers = []
    seen = set()
    prior_state = prior_state or {}
    if prior_state.get("error"):
        blockers.append(prior_state["error"])
    for row in rows:
        row_key = (row["variant_id"], row["store_id"])
        if row_key in seen:
            blockers.append(f"Duplicate preview row for variant/store {row['variant_id']} / {row['store_id']}.")
        seen.add(row_key)
    if not any(row["sync_eligible"] for row in rows):
        blockers.append("No eligible inventory rows are available to sync in this preview.")
    return blockers


def create_preview_token(order_id: int, store_id: str, fingerprint: str, issued_at: datetime | None = None) -> str:
    issued_at = issued_at or datetime.utcnow()
    payload = {
        "order_id": order_id,
        "store_id": store_id,
        "fingerprint": fingerprint,
        "issued_at": issued_at.isoformat(timespec="seconds"),
        "expires_at": (issued_at + timedelta(seconds=PREVIEW_TOKEN_TTL_SECONDS)).isoformat(timespec="seconds"),
    }
    payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    payload_part = _urlsafe_b64encode(payload_json.encode("utf-8"))
    signature = hmac.new(_preview_secret(), payload_part.encode("ascii"), hashlib.sha256).digest()
    return f"{payload_part}.{_urlsafe_b64encode(signature)}"


def decode_preview_token(token: str) -> dict:
    if not token or "." not in token:
        raise ProductionInventoryPreviewError("Preview token is missing or invalid.")
    payload_part, signature_part = token.split(".", 1)
    expected_signature = hmac.new(_preview_secret(), payload_part.encode("ascii"), hashlib.sha256).digest()
    actual_signature = _urlsafe_b64decode(signature_part)
    if not hmac.compare_digest(expected_signature, actual_signature):
        raise ProductionInventoryPreviewError("Preview token signature is invalid.")
    try:
        payload = json.loads(_urlsafe_b64decode(payload_part).decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
        raise ProductionInventoryPreviewError("Preview token payload is invalid.") from exc
    expires_at_text = str(payload.get("expires_at") or "")
    try:
        expires_at = datetime.fromisoformat(expires_at_text)
    except ValueError as exc:
        raise ProductionInventoryPreviewError("Preview token expiration is invalid.") from exc
    if datetime.utcnow() > expires_at:
        raise ProductionInventoryPreviewError("Preview token has expired. Generate a fresh preview before syncing.")
    return payload


def decimal_to_string(value: Decimal) -> str:
    return format(_quantize(value), "f")


def decimal_to_number(value: Decimal) -> int | float:
    text = decimal_to_string(value)
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    if "." not in text:
        return int(text)
    return float(text)


def _build_participants(db: Session, order: ProductionOrder) -> list[PreviewParticipant]:
    finished_sku = (order.product_sku_snapshot or "").strip()
    output_qty = order.output_qty or ZERO
    participants = [
        PreviewParticipant(
            row_key=f"finished_good:{order.id}",
            row_type="finished_good",
            sku=finished_sku,
            name=order.product_name_snapshot or "",
            variant_id=_resolve_variant_id(db, finished_sku),
            movement_quantity=output_qty,
            sort_key="000000_finished_good",
        )
    ]

    component_participants = []
    for material in sorted(order.materials, key=lambda item: ((item.component_sku or ""), item.id)):
        sku = (material.component_sku or "").strip()
        required_quantity = material.required_quantity or ZERO
        if not sku or required_quantity <= ZERO:
            continue
        component_participants.append(
            PreviewParticipant(
                row_key=f"component:{material.id}",
                row_type="component",
                sku=sku,
                name=material.component_name or "",
                variant_id=_resolve_variant_id(db, sku),
                movement_quantity=required_quantity,
                sort_key=f"{sku}_{material.id:010d}",
            )
        )
    participants.extend(component_participants)
    return participants


def _classify_row(participant: PreviewParticipant, inventory_row_returned: bool, prior_state: dict) -> dict:
    if participant.row_key in prior_state.get("successful_row_keys", set()):
        return {
            "sync_category": "already_synced",
            "sync_eligible": False,
            "sync_note": "Already synced in a previous partial inventory sync for this Production Order.",
            "physical_review_required": False,
        }
    if not inventory_row_returned:
        return {
            "sync_category": "excluded",
            "sync_eligible": False,
            "sync_note": "Excluded: no Loyverse inventory row was returned; physical inventory review is required.",
            "physical_review_required": True,
        }
    if prior_state.get("is_partial_success") and participant.row_key not in prior_state.get("pending_row_keys", set()):
        return {
            "sync_category": "excluded",
            "sync_eligible": False,
            "sync_note": "Excluded: this row was not pending from the previous partial sync and will not be submitted on retry.",
            "physical_review_required": True,
        }
    return {
        "sync_category": "included",
        "sync_eligible": True,
        "sync_note": "Included in the sync batch.",
        "physical_review_required": False,
    }


def _load_prior_sync_state(order: ProductionOrder) -> dict:
    empty_state = {
        "is_partial_success": False,
        "successful_row_keys": set(),
        "pending_row_keys": set(),
        "error": "",
    }
    payload_text = (order.loyverse_inventory_payload_summary or "").strip()
    if order.loyverse_inventory_sync_status != PARTIAL_SUCCESS_STATUS and not payload_text:
        return empty_state
    if not payload_text:
        return _corrupt_partial_state("Previous partial sync row memory is missing; retry is blocked for safety.")
    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError:
        if order.loyverse_inventory_sync_status == PARTIAL_SUCCESS_STATUS:
            return _corrupt_partial_state("Previous partial sync row memory is not valid JSON; retry is blocked for safety.")
        return empty_state
    if isinstance(payload, dict) and payload.get("row_memory_corrupt"):
        return _corrupt_partial_state(str(payload.get("error") or "Previous partial sync row memory is corrupt; retry is blocked for safety."))
    if not isinstance(payload, dict) or payload.get("mode") != "production_inventory_sync":
        if order.loyverse_inventory_sync_status == PARTIAL_SUCCESS_STATUS:
            return _corrupt_partial_state("Previous partial sync row memory has an invalid structure; retry is blocked for safety.")
        return empty_state
    successful = payload.get("successful_row_keys")
    pending = payload.get("pending_row_keys")
    if not isinstance(successful, list) or not isinstance(pending, list):
        if order.loyverse_inventory_sync_status == PARTIAL_SUCCESS_STATUS:
            return _corrupt_partial_state("Previous partial sync row memory is missing row key lists; retry is blocked for safety.")
        return empty_state
    successful_keys = {str(row_key) for row_key in successful if row_key}
    pending_keys = {str(row_key) for row_key in pending if row_key}
    if order.loyverse_inventory_sync_status != PARTIAL_SUCCESS_STATUS and not successful_keys and not pending_keys:
        return empty_state
    return {
        "is_partial_success": True,
        "successful_row_keys": successful_keys,
        "pending_row_keys": pending_keys,
        "error": "",
    }

def _corrupt_partial_state(message: str) -> dict:
    return {
        "is_partial_success": True,
        "successful_row_keys": set(),
        "pending_row_keys": set(),
        "error": message,
    }


def _resolve_variant_id(db: Session, sku: str) -> str:
    mappings = (
        db.query(LoyverseVariantMapping)
        .filter(LoyverseVariantMapping.active.is_(True), LoyverseVariantMapping.sku == sku)
        .order_by(LoyverseVariantMapping.item_name, LoyverseVariantMapping.variant_name)
        .all()
    )
    if len(mappings) == 1:
        return mappings[0].loyverse_variant_id
    if not mappings:
        raise ProductionInventoryPreviewError(f"No active local Loyverse variant mapping found for SKU {sku}.")
    raise ProductionInventoryPreviewError(f"Multiple active local Loyverse variant mappings found for SKU {sku}.")


def _get_inventory_levels(token: str, store_id: str, variant_ids: list[str]) -> list[dict]:
    rows = []
    cursor = None
    variant_id_filter = ",".join(variant_ids)
    while True:
        params = {
            "store_ids": store_id,
            "variant_ids": variant_id_filter,
            "limit": str(INVENTORY_LIMIT),
        }
        if cursor:
            params["cursor"] = cursor
        payload = _loyverse_get(token, f"/inventory?{urlencode(params)}")
        collection = payload.get("inventory_levels") or []
        if not isinstance(collection, list):
            raise ProductionInventoryPreviewError("Loyverse inventory response did not include a valid inventory_levels list.")
        rows.extend(row for row in collection if isinstance(row, dict))
        cursor = _string_value(payload, "cursor", "next_cursor")
        if not cursor:
            return rows


def _build_inventory_lookup(inventory_levels: list[dict], store_id: str) -> dict[str, dict]:
    lookup = {}
    duplicates = []
    for row in inventory_levels:
        row_store_id = _string_value(row, "store_id", "storeId")
        variant_id = _string_value(row, "variant_id", "variantId")
        if row_store_id != store_id or not variant_id:
            continue
        if variant_id in lookup:
            duplicates.append(variant_id)
            continue
        lookup[variant_id] = row
    if duplicates:
        unique_duplicates = ", ".join(sorted(set(duplicates)))
        raise ProductionInventoryPreviewError(f"Loyverse returned multiple inventory rows for variant/store: {unique_duplicates}.")
    return lookup


def _require_token() -> str:
    token = os.getenv("LOYVERSE_API_TOKEN", "").strip()
    if not token:
        raise ProductionInventoryPreviewError("LOYVERSE_API_TOKEN is not configured.")
    return token


def _require_store_id() -> str:
    store_id = os.getenv("LOYVERSE_STORE_ID", "").strip()
    if not store_id:
        raise ProductionInventoryPreviewError("LOYVERSE_STORE_ID is not configured.")
    return store_id


def _loyverse_get(token: str, path: str) -> dict:
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
        raise ProductionInventoryPreviewError(f"Loyverse API returned HTTP {exc.code}: {_summarize_response(response_text)}") from exc
    except TimeoutError as exc:
        raise ProductionInventoryPreviewError("Loyverse inventory request timed out.") from exc
    except URLError as exc:
        raise ProductionInventoryPreviewError(f"Loyverse inventory request failed: {exc.reason}") from exc

    if not response_text:
        return {}
    try:
        return json.loads(response_text)
    except json.JSONDecodeError as exc:
        raise ProductionInventoryPreviewError("Loyverse inventory response was not valid JSON.") from exc


def _decimal_value(payload: dict, *keys: str) -> Decimal:
    for key in keys:
        value = payload.get(key)
        if value is None or value == "":
            continue
        try:
            return _quantize(Decimal(str(value)))
        except (InvalidOperation, ValueError) as exc:
            raise ProductionInventoryPreviewError(f"Loyverse inventory value for {key} is not numeric: {value}.") from exc
    return ZERO


def _quantize(value: Decimal) -> Decimal:
    return Decimal(str(value)).quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP)


def _preview_secret() -> bytes:
    secret = os.getenv("LOYVERSE_INVENTORY_PREVIEW_SECRET", "").strip()
    if not secret:
        secret = os.getenv("LOYVERSE_API_TOKEN", "").strip()
    if not secret:
        raise ProductionInventoryPreviewError("Preview signing secret is unavailable because LOYVERSE_API_TOKEN is not configured.")
    return secret.encode("utf-8")


def _urlsafe_b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _urlsafe_b64decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def _string_value(payload: dict, *keys: str) -> str:
    for key in keys:
        value = payload.get(key)
        if value is not None:
            return str(value).strip()
    return ""


def _summarize_response(response_text: str) -> str:
    return _truncate_summary(" ".join(response_text.split()), 500)


def _truncate_summary(text: str, limit: int) -> str:
    if len(text) > limit:
        return f"{text[:limit]}..."
    return text



