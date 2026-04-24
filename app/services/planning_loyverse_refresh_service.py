import json
import os
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from sqlalchemy.orm import Session

from app.models import Product
from app.services.planning_service import build_planning_rows


LOYVERSE_API_BASE_URL = "https://api.loyverse.com/v1.0"
LOYVERSE_REQUEST_TIMEOUT_SECONDS = 8
INVENTORY_LIMIT = 250
DECIMAL_QUANT = Decimal("0.0001")


class PlanningLoyverseRefreshError(Exception):
    pass


@dataclass(frozen=True)
class PlanningRefreshResult:
    inventory_refreshed_count: int
    cost_refreshed_count: int
    warning_count: int
    matched_product_count: int
    warning_messages: list[str]


def refresh_planning_inventory_and_cost(
    db: Session,
    product_type: str,
    sku: str = "",
    route_id: str = "",
    supplier: str = "",
    status: str = "",
    needs_action: bool = False,
) -> PlanningRefreshResult:
    token = _require_token()
    store_id = _require_store_id()
    rows = build_planning_rows(
        db,
        product_type,
        sku=sku,
        route_id=route_id,
        supplier=supplier,
        status=status,
        needs_action=needs_action,
    )
    products = [row.product for row in rows]
    if not products:
        return PlanningRefreshResult(0, 0, 0, 0, [])

    eligible_products = []
    warnings: list[str] = []
    for product in products:
        variant_id = (product.loyverse_variant_id or "").strip()
        if not variant_id:
            warnings.append(f"{product.sku}: skipped because Loyverse variant mapping is missing.")
            continue
        eligible_products.append(product)

    if not eligible_products:
        return PlanningRefreshResult(0, 0, len(warnings), 0, warnings)

    inventory_rows, inventory_warnings = _get_inventory_levels(
        token,
        store_id,
        [(product.id, product.loyverse_variant_id.strip()) for product in eligible_products],
    )
    warnings.extend(inventory_warnings)
    refreshed_at = datetime.utcnow()
    inventory_refreshed_count = 0
    cost_refreshed_count = 0

    for product in eligible_products:
        variant_id = product.loyverse_variant_id.strip()
        inventory_row = inventory_rows.get(variant_id)
        if inventory_row is None:
            warnings.append(f"{product.sku}: inventory row was not returned for the configured store.")
        else:
            try:
                product.current_inventory_qty = _extract_inventory_quantity(inventory_row)
                product.loyverse_inventory_refreshed_at = refreshed_at
                inventory_refreshed_count += 1
            except PlanningLoyverseRefreshError as exc:
                warnings.append(f"{product.sku}: inventory refresh skipped because {exc}")

        try:
            product.standard_cost = _read_cost_for_product(token, product)
            product.loyverse_cost_refreshed_at = refreshed_at
            cost_refreshed_count += 1
        except PlanningLoyverseRefreshError as exc:
            warnings.append(f"{product.sku}: cost refresh skipped because {exc}")

    db.commit()
    return PlanningRefreshResult(
        inventory_refreshed_count=inventory_refreshed_count,
        cost_refreshed_count=cost_refreshed_count,
        warning_count=len(warnings),
        matched_product_count=len(eligible_products),
        warning_messages=warnings,
    )


def _require_token() -> str:
    token = os.getenv("LOYVERSE_API_TOKEN", "").strip()
    if not token:
        raise PlanningLoyverseRefreshError("LOYVERSE_API_TOKEN is not configured.")
    return token


def _require_store_id() -> str:
    store_id = os.getenv("LOYVERSE_STORE_ID", "").strip()
    if not store_id:
        raise PlanningLoyverseRefreshError("LOYVERSE_STORE_ID is not configured.")
    return store_id


def _get_inventory_levels(token: str, store_id: str, products: list[tuple[int, str]]) -> tuple[dict[str, dict], list[str]]:
    variant_ids = [variant_id for _, variant_id in products if variant_id]
    if not variant_ids:
        return {}, []

    rows: list[dict] = []
    cursor = None
    while True:
        params = {
            "store_ids": store_id,
            "variant_ids": ",".join(variant_ids),
            "limit": str(INVENTORY_LIMIT),
        }
        if cursor:
            params["cursor"] = cursor
        payload = _loyverse_get(token, f"/inventory?{urlencode(params)}")
        collection = payload.get("inventory_levels") or []
        if not isinstance(collection, list):
            raise PlanningLoyverseRefreshError("Loyverse inventory response did not include a valid inventory_levels list.")
        rows.extend(row for row in collection if isinstance(row, dict))
        cursor = _string_value(payload, "cursor", "next_cursor")
        if not cursor:
            break

    lookup: dict[str, dict] = {}
    duplicates: set[str] = set()
    for row in rows:
        row_store_id = _string_value(row, "store_id", "storeId")
        variant_id = _string_value(row, "variant_id", "variantId")
        if row_store_id != store_id or not variant_id:
            continue
        if variant_id in lookup:
            duplicates.add(variant_id)
            continue
        lookup[variant_id] = row
    warnings = []
    if duplicates:
        duplicate_list = ", ".join(sorted(duplicates))
        warnings.append(
            f"Inventory refresh skipped for duplicate Loyverse inventory rows on variant/store: {duplicate_list}."
        )
        for duplicate_variant_id in duplicates:
            lookup.pop(duplicate_variant_id, None)
    return lookup, warnings


def _extract_inventory_quantity(row: dict) -> Decimal:
    return _decimal_value(row, "in_stock", "stock_after", "stock", "available")


def _read_cost_for_product(token: str, product: Product) -> Decimal:
    variant_id = (product.loyverse_variant_id or "").strip()
    if not variant_id:
        raise PlanningLoyverseRefreshError("Loyverse variant mapping is missing.")

    variant_payload = _loyverse_get(token, f"/variants/{variant_id}")
    variant_cost = _extract_cost_from_payload(variant_payload, variant_id=variant_id)
    if variant_cost is not None:
        return variant_cost

    item_id = _string_value(variant_payload, "item_id", "itemId") or (product.loyverse_item_id or "").strip()
    if item_id:
        item_payload = _loyverse_get(token, f"/items/{item_id}")
        item_cost = _extract_cost_from_payload(item_payload, variant_id=variant_id)
        if item_cost is not None:
            return item_cost

    raise PlanningLoyverseRefreshError("a numeric cost value was not returned by Loyverse.")


def _extract_cost_from_payload(payload: dict, variant_id: str = "") -> Decimal | None:
    direct = _decimal_value_or_none(payload, "cost", "default_cost", "defaultCost", "standard_cost", "standardCost")
    if direct is not None:
        return direct

    variant_section = payload.get("variant")
    if isinstance(variant_section, dict):
        direct = _decimal_value_or_none(
            variant_section,
            "cost",
            "default_cost",
            "defaultCost",
            "standard_cost",
            "standardCost",
        )
        if direct is not None:
            return direct

    item_section = payload.get("item")
    if isinstance(item_section, dict):
        direct = _decimal_value_or_none(
            item_section,
            "cost",
            "default_cost",
            "defaultCost",
            "standard_cost",
            "standardCost",
        )
        if direct is not None:
            return direct
        nested_variants = item_section.get("variants") or []
        matched = _extract_variant_cost_from_collection(nested_variants, variant_id)
        if matched is not None:
            return matched

    variants = payload.get("variants") or []
    matched = _extract_variant_cost_from_collection(variants, variant_id)
    if matched is not None:
        return matched

    return None


def _extract_variant_cost_from_collection(variants, variant_id: str) -> Decimal | None:
    if not isinstance(variants, list):
        return None
    for variant in variants:
        if not isinstance(variant, dict):
            continue
        candidate_variant_id = _string_value(variant, "variant_id", "id")
        if variant_id and candidate_variant_id and candidate_variant_id != variant_id:
            continue
        cost = _decimal_value_or_none(
            variant,
            "cost",
            "default_cost",
            "defaultCost",
            "standard_cost",
            "standardCost",
        )
        if cost is not None:
            return cost
    return None


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
        raise PlanningLoyverseRefreshError(
            f"Loyverse API returned HTTP {exc.code}: {_summarize_response(response_text)}"
        ) from exc
    except URLError as exc:
        raise PlanningLoyverseRefreshError(f"Loyverse API request failed: {exc.reason}") from exc
    except TimeoutError as exc:
        raise PlanningLoyverseRefreshError("Loyverse API request timed out.") from exc

    if not response_text:
        return {}
    try:
        return json.loads(response_text)
    except json.JSONDecodeError as exc:
        raise PlanningLoyverseRefreshError("Loyverse API returned invalid JSON.") from exc


def _decimal_value(payload: dict, *keys: str) -> Decimal:
    value = _decimal_value_or_none(payload, *keys)
    if value is None:
        raise PlanningLoyverseRefreshError(f"a numeric value for {', '.join(keys)} was not returned")
    return value


def _decimal_value_or_none(payload: dict, *keys: str) -> Decimal | None:
    for key in keys:
        value = payload.get(key)
        if value is None or value == "":
            continue
        try:
            return Decimal(str(value)).quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP)
        except (InvalidOperation, ValueError) as exc:
            raise PlanningLoyverseRefreshError(f"{key} was not numeric: {value}") from exc
    return None


def _string_value(payload: dict, *keys: str) -> str:
    for key in keys:
        value = payload.get(key)
        if value is not None:
            return str(value).strip()
    return ""


def _summarize_response(response_text: str) -> str:
    text = " ".join(response_text.split())
    if len(text) > 500:
        return f"{text[:500]}..."
    return text
