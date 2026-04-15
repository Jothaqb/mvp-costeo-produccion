import json
import os
from datetime import datetime
from decimal import Decimal
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from sqlalchemy.orm import Session, joinedload

from app.models import ProductionOrder


LOYVERSE_API_BASE_URL = "https://api.loyverse.com/v1.0"
LOYVERSE_REQUEST_TIMEOUT_SECONDS = 3
SYNC_STATUS_SUCCESS = "success"
SYNC_STATUS_FAILED = "failed"
SYNC_STATUS_SKIPPED = "skipped"


def sync_closed_order_cost_to_loyverse(db: Session, order_id: int) -> None:
    order = (
        db.query(ProductionOrder)
        .options(joinedload(ProductionOrder.product))
        .filter(ProductionOrder.id == order_id)
        .one()
    )
    if order.status != "closed":
        return

    attempted_at = datetime.utcnow()
    token = os.getenv("LOYVERSE_API_TOKEN", "").strip()
    cost = order.real_unit_cost

    if not token:
        _mark_skipped(order, attempted_at, "LOYVERSE_API_TOKEN is not configured.")
        db.commit()
        return
    if cost is None:
        _mark_skipped(order, attempted_at, "Production order has no real unit cost.")
        db.commit()
        return
    if order.product is None:
        _mark_skipped(order, attempted_at, "Production order product master record could not be loaded.")
        db.commit()
        return

    item_id = (order.product.loyverse_item_id or "").strip()
    variant_id = (order.product.loyverse_variant_id or "").strip()
    if not variant_id:
        _mark_skipped(order, attempted_at, "Product is missing Loyverse variant mapping.")
        db.commit()
        return

    try:
        if not item_id:
            item_id = _resolve_item_id_for_variant(token, variant_id)
        _update_variant_cost(token, item_id, variant_id, cost)
    except LoyverseSyncError as exc:
        order.loyverse_cost_sync_status = SYNC_STATUS_FAILED
        order.loyverse_cost_sync_attempted_at = attempted_at
        order.loyverse_cost_sync_error = str(exc)
        order.loyverse_cost_sync_pushed_cost = None
        if exc.variant_id:
            order.loyverse_cost_sync_variant_id = exc.variant_id
        db.commit()
        return
    except Exception as exc:
        order.loyverse_cost_sync_status = SYNC_STATUS_FAILED
        order.loyverse_cost_sync_attempted_at = attempted_at
        order.loyverse_cost_sync_error = f"Loyverse sync failed unexpectedly: {exc}"
        order.loyverse_cost_sync_pushed_cost = None
        db.commit()
        return

    order.loyverse_cost_sync_status = SYNC_STATUS_SUCCESS
    order.loyverse_cost_sync_attempted_at = attempted_at
    order.loyverse_cost_sync_error = None
    order.loyverse_cost_sync_variant_id = variant_id
    order.loyverse_cost_sync_pushed_cost = cost
    db.commit()


class LoyverseSyncError(Exception):
    def __init__(self, message: str, variant_id: str | None = None) -> None:
        super().__init__(message)
        self.variant_id = variant_id


def _mark_skipped(order: ProductionOrder, attempted_at: datetime, reason: str) -> None:
    order.loyverse_cost_sync_status = SYNC_STATUS_SKIPPED
    order.loyverse_cost_sync_attempted_at = attempted_at
    order.loyverse_cost_sync_error = reason
    order.loyverse_cost_sync_variant_id = None
    order.loyverse_cost_sync_pushed_cost = None


def _resolve_item_id_for_variant(token: str, variant_id: str) -> str:
    payload = _loyverse_request(token, "GET", f"/variants/{variant_id}", variant_id=variant_id)
    item_id = (payload.get("item_id") or payload.get("itemId") or "").strip()
    if not item_id:
        raise LoyverseSyncError(
            f"Loyverse variant {variant_id} response did not include item_id.",
            variant_id=variant_id,
        )
    return item_id


def _update_variant_cost(token: str, item_id: str, variant_id: str, cost: Decimal) -> None:
    body = {
        "item_id": item_id,
        "variant_id": variant_id,
        "cost": float(cost),
    }
    _loyverse_request(token, "POST", "/variants", body, variant_id=variant_id)


def _loyverse_request(
    token: str,
    method: str,
    path: str,
    body: dict | None = None,
    variant_id: str | None = None,
) -> dict:
    url = f"{LOYVERSE_API_BASE_URL}{path}"
    data = None
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = Request(url, data=data, headers=headers, method=method)
    try:
        with urlopen(request, timeout=LOYVERSE_REQUEST_TIMEOUT_SECONDS) as response:
            response_text = response.read().decode("utf-8")
    except HTTPError as exc:
        response_text = exc.read().decode("utf-8", errors="replace")
        raise LoyverseSyncError(
            f"Loyverse API returned HTTP {exc.code}: {_summarize_response(response_text)}",
            variant_id=variant_id,
        ) from exc
    except URLError as exc:
        raise LoyverseSyncError(f"Loyverse API request failed: {exc.reason}", variant_id=variant_id) from exc
    except TimeoutError as exc:
        raise LoyverseSyncError("Loyverse API request timed out.", variant_id=variant_id) from exc

    if not response_text:
        return {}
    try:
        return json.loads(response_text)
    except json.JSONDecodeError as exc:
        raise LoyverseSyncError("Loyverse API returned invalid JSON.", variant_id=variant_id) from exc


def _summarize_response(response_text: str) -> str:
    text = " ".join(response_text.split())
    if len(text) > 500:
        return f"{text[:500]}..."
    return text
