import json
from datetime import datetime
from decimal import Decimal, InvalidOperation
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from sqlalchemy.orm import Session

from app.models import ProductionOrder
from app.services.production_loyverse_inventory_preview_service import (
    LOYVERSE_API_BASE_URL,
    LOYVERSE_REQUEST_TIMEOUT_SECONDS,
    ProductionInventoryPreviewError,
    build_inventory_post_payload,
    build_production_inventory_preview,
    decode_preview_token,
    decimal_to_string,
)


SYNC_STATUS_SUCCESS = "success"
SYNC_STATUS_PARTIAL_SUCCESS = "partial_success"
SYNC_STATUS_FAILED = "failed"
SYNC_STATUS_UNKNOWN = "unknown"
SYNC_STATUS_SKIPPED = "skipped"


class ProductionInventorySyncError(Exception):
    pass


class ProductionInventorySyncSkipped(ProductionInventorySyncError):
    pass


class ProductionInventorySyncFailed(ProductionInventorySyncError):
    pass


class ProductionInventorySyncUnknown(ProductionInventorySyncError):
    pass


def sync_production_inventory_to_loyverse(
    db: Session,
    order_id: int,
    preview_token: str,
    preview_fingerprint: str,
) -> ProductionOrder:
    order = db.query(ProductionOrder).filter(ProductionOrder.id == order_id).one()
    _ensure_not_terminal(order)

    attempted_at = datetime.utcnow()
    fingerprint_for_metadata = (preview_fingerprint or "").strip() or None
    payload_summary = None
    store_id = None

    try:
        token_payload = decode_preview_token(preview_token)
        _validate_token_context(token_payload, order_id)
        store_id = str(token_payload.get("store_id") or "").strip()
        token_fingerprint = str(token_payload.get("fingerprint") or "").strip()
        submitted_fingerprint = (preview_fingerprint or "").strip()
        if not submitted_fingerprint:
            raise ProductionInventorySyncSkipped("Preview fingerprint is missing.")
        if submitted_fingerprint != token_fingerprint:
            raise ProductionInventorySyncSkipped("Submitted preview fingerprint does not match the signed preview token.")

        preview = build_production_inventory_preview(db, order_id, include_token=False)
        fingerprint_for_metadata = preview["fingerprint"]
        if preview["store_id"] != store_id:
            raise ProductionInventorySyncSkipped("Preview store id no longer matches the signed preview token.")
        if preview["fingerprint"] != token_fingerprint:
            raise ProductionInventorySyncSkipped("Preview data changed. Generate a fresh preview before syncing.")
        if preview["write_blockers"]:
            if preview.get("prior_sync_state", {}).get("error"):
                payload_summary = _build_corrupt_row_memory_summary(preview, attempted_at)
            else:
                payload_summary = _build_payload_summary(
                    preview=preview,
                    status=SYNC_STATUS_SKIPPED,
                    attempted_at=attempted_at,
                    request_fingerprint=preview["fingerprint"],
                    submitted_row_keys=[],
                    successful_row_keys=_prior_successful_row_keys(preview),
                    response_status="not_sent",
                )
            raise ProductionInventorySyncSkipped("; ".join(preview["write_blockers"]))

        post_payload = build_inventory_post_payload(preview)
        submitted_levels = post_payload.get("inventory_levels") or []
        submitted_row_keys = [row["row_key"] for row in preview["included_rows"]]
        if not submitted_levels:
            payload_summary = _build_payload_summary(
                preview=preview,
                status=SYNC_STATUS_SKIPPED,
                attempted_at=attempted_at,
                request_fingerprint=preview["fingerprint"],
                submitted_row_keys=[],
                successful_row_keys=_prior_successful_row_keys(preview),
                response_status="not_sent",
            )
            raise ProductionInventorySyncSkipped("No eligible inventory rows are available to submit to Loyverse.")

        payload_summary = _build_payload_summary(
            preview=preview,
            status="submitted",
            attempted_at=attempted_at,
            request_fingerprint=preview["fingerprint"],
            submitted_row_keys=submitted_row_keys,
            successful_row_keys=_prior_successful_row_keys(preview),
            response_status="pending",
        )
        response_payload, response_summary = _post_inventory(post_payload)
        try:
            _validate_success_response(response_payload, post_payload)
        except ProductionInventorySyncUnknown as exc:
            exc.response_summary = response_summary
            raise

        successful_row_keys = sorted(set(_prior_successful_row_keys(preview)) | set(submitted_row_keys))
        pending_row_keys = [row["row_key"] for row in preview["excluded_rows"]]
        final_status = SYNC_STATUS_PARTIAL_SUCCESS if pending_row_keys else SYNC_STATUS_SUCCESS
        synced_at = datetime.utcnow() if final_status == SYNC_STATUS_SUCCESS else None
        payload_summary = _build_payload_summary(
            preview=preview,
            status=final_status,
            attempted_at=attempted_at,
            request_fingerprint=preview["fingerprint"],
            submitted_row_keys=submitted_row_keys,
            successful_row_keys=successful_row_keys,
            response_status="success",
        )
        _mark_order(
            order,
            status=final_status,
            attempted_at=attempted_at,
            store_id=preview["store_id"],
            request_fingerprint=preview["fingerprint"],
            payload_summary=payload_summary,
            response_summary=response_summary,
            error=None,
            synced_at=synced_at,
        )
        db.commit()
        db.refresh(order)
        return order
    except ProductionInventorySyncSkipped as exc:
        _mark_order(
            order,
            status=SYNC_STATUS_SKIPPED,
            attempted_at=attempted_at,
            store_id=store_id,
            request_fingerprint=fingerprint_for_metadata,
            payload_summary=payload_summary,
            response_summary=None,
            error=str(exc),
            synced_at=None,
        )
        db.commit()
        raise
    except ProductionInventorySyncFailed as exc:
        _mark_order(
            order,
            status=SYNC_STATUS_FAILED,
            attempted_at=attempted_at,
            store_id=store_id,
            request_fingerprint=fingerprint_for_metadata,
            payload_summary=payload_summary,
            response_summary=getattr(exc, "response_summary", None),
            error=str(exc),
            synced_at=None,
        )
        db.commit()
        raise
    except ProductionInventorySyncUnknown as exc:
        _mark_order(
            order,
            status=SYNC_STATUS_UNKNOWN,
            attempted_at=attempted_at,
            store_id=store_id,
            request_fingerprint=fingerprint_for_metadata,
            payload_summary=payload_summary,
            response_summary=getattr(exc, "response_summary", None),
            error=str(exc),
            synced_at=None,
        )
        db.commit()
        raise
    except ProductionInventoryPreviewError as exc:
        _mark_order(
            order,
            status=SYNC_STATUS_SKIPPED,
            attempted_at=attempted_at,
            store_id=store_id,
            request_fingerprint=fingerprint_for_metadata,
            payload_summary=payload_summary,
            response_summary=None,
            error=str(exc),
            synced_at=None,
        )
        db.commit()
        raise ProductionInventorySyncSkipped(str(exc)) from exc


def _ensure_not_terminal(order: ProductionOrder) -> None:
    if order.loyverse_inventory_sync_status == SYNC_STATUS_SUCCESS:
        raise ProductionInventorySyncSkipped("Loyverse inventory sync already succeeded for this Production Order.")
    if order.loyverse_inventory_sync_status == SYNC_STATUS_UNKNOWN:
        raise ProductionInventorySyncSkipped("Previous Loyverse inventory sync status is unknown and blocks retry in the MVP.")


def _validate_token_context(token_payload: dict, order_id: int) -> None:
    token_order_id = token_payload.get("order_id")
    if token_order_id != order_id:
        raise ProductionInventorySyncSkipped("Preview token does not match this Production Order.")


def _post_inventory(payload: dict) -> tuple[dict, str]:
    token = _require_token()
    data = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    request = Request(
        f"{LOYVERSE_API_BASE_URL}/inventory",
        data=data,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=LOYVERSE_REQUEST_TIMEOUT_SECONDS) as response:
            response_text = response.read().decode("utf-8")
    except HTTPError as exc:
        response_text = exc.read().decode("utf-8", errors="replace")
        error = ProductionInventorySyncFailed(f"Loyverse API returned HTTP {exc.code}: {_summarize_response(response_text)}")
        error.response_summary = _summarize_response(response_text)
        raise error from exc
    except TimeoutError as exc:
        error = ProductionInventorySyncUnknown("Loyverse inventory request timed out. Manually verify Loyverse before retrying.")
        error.response_summary = None
        raise error from exc
    except URLError as exc:
        error = ProductionInventorySyncUnknown(f"Loyverse inventory request result is unknown: {exc.reason}")
        error.response_summary = None
        raise error from exc

    response_summary = _summarize_response(response_text)
    if not response_text:
        error = ProductionInventorySyncUnknown("Loyverse inventory response was empty after HTTP success.")
        error.response_summary = response_summary
        raise error
    try:
        return json.loads(response_text), response_summary
    except json.JSONDecodeError as exc:
        error = ProductionInventorySyncUnknown("Loyverse inventory response was not valid JSON after HTTP success.")
        error.response_summary = response_summary
        raise error from exc


def _validate_success_response(response_payload: dict, submitted_payload: dict) -> None:
    returned_levels = response_payload.get("inventory_levels") if isinstance(response_payload, dict) else None
    submitted_levels = submitted_payload.get("inventory_levels") or []
    if not isinstance(returned_levels, list):
        raise ProductionInventorySyncUnknown("Loyverse inventory response did not include inventory_levels after HTTP success.")
    if len(returned_levels) != len(submitted_levels):
        raise ProductionInventorySyncUnknown("Loyverse inventory response row count did not match submitted batch row count.")

    returned_by_key = {}
    for row in returned_levels:
        if not isinstance(row, dict):
            raise ProductionInventorySyncUnknown("Loyverse inventory response included a non-object inventory row.")
        variant_id = _string_value(row, "variant_id", "variantId")
        store_id = _string_value(row, "store_id", "storeId")
        key = (variant_id, store_id)
        if not variant_id or not store_id:
            raise ProductionInventorySyncUnknown("Loyverse inventory response row was missing variant_id or store_id.")
        if key in returned_by_key:
            raise ProductionInventorySyncUnknown("Loyverse inventory response included duplicate variant_id/store_id rows.")
        if "in_stock" not in row:
            raise ProductionInventorySyncUnknown("Loyverse inventory response row was missing in_stock.")
        returned_by_key[key] = row

    for submitted in submitted_levels:
        key = (str(submitted.get("variant_id") or "").strip(), str(submitted.get("store_id") or "").strip())
        returned = returned_by_key.get(key)
        if returned is None:
            raise ProductionInventorySyncUnknown("Loyverse inventory response did not include every submitted variant_id/store_id row.")
        returned_stock = _decimal_value(returned.get("in_stock"))
        submitted_stock = _decimal_value(submitted.get("stock_after"))
        if returned_stock != submitted_stock:
            raise ProductionInventorySyncUnknown(
                f"Loyverse inventory response in_stock {decimal_to_string(returned_stock)} did not match submitted stock_after {decimal_to_string(submitted_stock)} for variant {key[0]}."
            )


def _build_payload_summary(
    preview: dict,
    status: str,
    attempted_at: datetime,
    request_fingerprint: str,
    submitted_row_keys: list[str],
    successful_row_keys: list[str],
    response_status: str,
) -> str:
    excluded_rows = [_row_summary(row, "pending") for row in preview["excluded_rows"]]
    included_rows = [_row_summary(row, "success" if row["row_key"] in submitted_row_keys else "included") for row in preview["included_rows"]]
    already_synced_rows = [_row_summary(row, "success") for row in preview["already_synced_rows"]]
    pending_row_keys = [row["row_key"] for row in preview["excluded_rows"]]
    zero_floor_row_keys = [row["row_key"] for row in preview["zero_floor_rows"]]
    payload = {
        "version": 1,
        "mode": "production_inventory_sync",
        "status": status,
        "store_id": preview["store_id"],
        "attempted_at": attempted_at.isoformat(timespec="seconds"),
        "request_fingerprint": request_fingerprint,
        "response_status": response_status,
        "submitted_row_keys": sorted(submitted_row_keys),
        "successful_row_keys": sorted(set(successful_row_keys)),
        "pending_row_keys": sorted(pending_row_keys),
        "zero_floor_row_keys": sorted(zero_floor_row_keys),
        "included_rows": included_rows,
        "excluded_rows": excluded_rows,
        "already_synced_rows": already_synced_rows,
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _row_summary(row: dict, sync_status: str) -> dict:
    return {
        "row_key": row["row_key"],
        "row_type": row["row_type"],
        "sku": row["sku"],
        "variant_id": row["variant_id"],
        "store_id": row["store_id"],
        "raw_current_stock": decimal_to_string(row["current_stock"]),
        "effective_current_stock": decimal_to_string(row["effective_current_stock"]),
        "movement_quantity": decimal_to_string(row["movement_quantity"]),
        "submitted_stock_after": decimal_to_string(row["proposed_stock_after"]),
        "zero_floor_applied": bool(row["zero_floor_applied"]),
        "inventory_row_returned": bool(row["inventory_row_returned"]),
        "reason": row["sync_note"],
        "physical_review_required": bool(row["physical_review_required"]),
        "sync_status": sync_status,
    }


def _prior_successful_row_keys(preview: dict) -> list[str]:
    prior_state = preview.get("prior_sync_state") or {}
    return sorted(prior_state.get("successful_row_keys", set()))


def _build_corrupt_row_memory_summary(preview: dict, attempted_at: datetime) -> str:
    prior_state = preview.get("prior_sync_state") or {}
    payload = {
        "version": 1,
        "mode": "production_inventory_sync",
        "status": SYNC_STATUS_SKIPPED,
        "row_memory_corrupt": True,
        "store_id": preview.get("store_id"),
        "attempted_at": attempted_at.isoformat(timespec="seconds"),
        "request_fingerprint": preview.get("fingerprint"),
        "error": prior_state.get("error") or "Previous partial sync row memory is corrupt; retry is blocked for safety.",
        "submitted_row_keys": [],
        "successful_row_keys": [],
        "pending_row_keys": [],
        "zero_floor_row_keys": [],
        "included_rows": [],
        "excluded_rows": [],
        "already_synced_rows": [],
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _mark_order(
    order: ProductionOrder,
    status: str,
    attempted_at: datetime,
    store_id: str | None,
    request_fingerprint: str | None,
    payload_summary: str | None,
    response_summary: str | None,
    error: str | None,
    synced_at: datetime | None,
) -> None:
    order.loyverse_inventory_sync_status = status
    order.loyverse_inventory_sync_error = _summarize_response(error or "") if error else None
    order.loyverse_inventory_sync_attempted_at = attempted_at
    order.loyverse_inventory_sync_attempt_count = (order.loyverse_inventory_sync_attempt_count or 0) + 1
    order.loyverse_inventory_store_id_snapshot = store_id
    order.loyverse_inventory_request_fingerprint = request_fingerprint
    if payload_summary is not None:
        order.loyverse_inventory_payload_summary = payload_summary
    order.loyverse_inventory_response_summary = response_summary
    order.loyverse_inventory_synced_at = synced_at


def _require_token() -> str:
    import os

    token = os.getenv("LOYVERSE_API_TOKEN", "").strip()
    if not token:
        raise ProductionInventorySyncSkipped("LOYVERSE_API_TOKEN is not configured.")
    return token


def _decimal_value(value) -> Decimal:
    try:
        return Decimal(str(value)).quantize(Decimal("0.0001"))
    except (InvalidOperation, ValueError) as exc:
        raise ProductionInventorySyncUnknown(f"Loyverse inventory value is not numeric: {value}.") from exc


def _string_value(payload: dict, *keys: str) -> str:
    for key in keys:
        value = payload.get(key)
        if value is not None:
            return str(value).strip()
    return ""


def _summarize_response(response_text: str) -> str:
    text = " ".join(str(response_text).split())
    if len(text) > 1000:
        return f"{text[:1000]}..."
    return text



