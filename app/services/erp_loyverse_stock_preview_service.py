from __future__ import annotations

import json
import os
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from sqlalchemy import or_
from sqlalchemy.orm import Session

from app.models import InventoryBalance, LoyverseVariantMapping, Product


LOYVERSE_API_BASE_URL = "https://api.loyverse.com/v1.0"
LOYVERSE_REQUEST_TIMEOUT_SECONDS = 8
INVENTORY_LIMIT = 250
VARIANT_BATCH_SIZE = 25
DECIMAL_QUANT = Decimal("0.0001")
ZERO = Decimal("0")


@dataclass(frozen=True)
class ERPLoyverseStockPreviewRow:
    product_id: int
    sku: str
    product_name: str
    active: bool
    available_for_sale_gc: bool
    loyverse_variant_id: str
    erp_stock_official: Decimal
    loyverse_stock: Decimal | None
    difference: Decimal | None
    absolute_difference: Decimal | None
    status: str
    notes: list[str]


@dataclass(frozen=True)
class ERPLoyverseStockPreviewSummary:
    products_compared: int
    matched: int
    difference: int
    missing_in_loyverse: int
    missing_inventory_balance: int
    inactive_products: int
    mapping_missing: int
    api_errors: int
    total_absolute_difference: Decimal


@dataclass(frozen=True)
class ERPLoyverseStockPreviewResult:
    store_id: str
    active_only: bool
    available_only: bool
    only_differences: bool
    sku_query: str
    rows: list[ERPLoyverseStockPreviewRow]
    summary: ERPLoyverseStockPreviewSummary
    warnings: list[str]


def build_erp_loyverse_stock_preview(
    db: Session,
    *,
    active_only: bool = True,
    available_only: bool = False,
    only_differences: bool = False,
    sku_query: str = "",
) -> ERPLoyverseStockPreviewResult:
    products = _load_candidate_products(
        db,
        active_only=active_only,
        available_only=available_only,
        sku_query=sku_query,
    )
    mapping_resolutions = _resolve_product_variant_mapping(db, products)
    balances = _load_inventory_balances(db, products)

    warnings: list[str] = []
    api_error_message = ""
    variant_errors: dict[str, str] = {}
    token = _require_token(optional=True)
    store_id = ""
    mapped_variant_ids = sorted(
        {
            resolution.loyverse_variant_id
            for resolution in mapping_resolutions.values()
            if resolution.loyverse_variant_id
        }
    )
    loyverse_inventory_by_variant: dict[str, Decimal] = {}

    if mapped_variant_ids:
        if not token:
            api_error_message = "LOYVERSE_API_TOKEN is not configured."
            warnings.append(api_error_message)
        else:
            store_id, store_warnings, store_error = _resolve_loyverse_store_id(token)
            warnings.extend(store_warnings)
            if store_error:
                api_error_message = store_error
                warnings.append(api_error_message)
            else:
                try:
                    loyverse_inventory_by_variant, variant_errors, inventory_warnings = _load_loyverse_inventory_levels(
                        token,
                        store_id,
                        mapped_variant_ids,
                    )
                    warnings.extend(inventory_warnings)
                except RuntimeError as exc:
                    api_error_message = str(exc)
                    warnings.append(api_error_message)

    rows = _build_comparison_rows(
        products,
        mapping_resolutions,
        balances,
        loyverse_inventory_by_variant,
        variant_errors,
        api_error_message,
    )
    if only_differences:
        rows = [row for row in rows if row.status != "matched"]

    return ERPLoyverseStockPreviewResult(
        store_id=store_id,
        active_only=active_only,
        available_only=available_only,
        only_differences=only_differences,
        sku_query=sku_query,
        rows=rows,
        summary=_build_summary(rows),
        warnings=warnings,
    )


@dataclass(frozen=True)
class _MappingResolution:
    loyverse_variant_id: str
    notes: list[str]


def _load_candidate_products(
    db: Session,
    *,
    active_only: bool,
    available_only: bool,
    sku_query: str,
) -> list[Product]:
    query = db.query(Product)
    if active_only:
        query = query.filter(Product.active.is_(True))
    if available_only:
        query = query.filter(Product.available_for_sale_gc.is_(True))
    normalized_query = (sku_query or "").strip()
    if normalized_query:
        pattern = f"%{normalized_query}%"
        query = query.filter(or_(Product.sku.ilike(pattern), Product.name.ilike(pattern)))
    return query.order_by(Product.sku, Product.id).all()


def _resolve_product_variant_mapping(
    db: Session,
    products: list[Product],
) -> dict[int, _MappingResolution]:
    skus = sorted({(product.sku or "").strip() for product in products if (product.sku or "").strip()})
    mappings_by_sku: dict[str, list[LoyverseVariantMapping]] = {}
    if skus:
        mappings = (
            db.query(LoyverseVariantMapping)
            .filter(
                LoyverseVariantMapping.active.is_(True),
                LoyverseVariantMapping.sku.in_(skus),
            )
            .order_by(LoyverseVariantMapping.sku, LoyverseVariantMapping.loyverse_variant_id)
            .all()
        )
        for mapping in mappings:
            sku = (mapping.sku or "").strip()
            if sku:
                mappings_by_sku.setdefault(sku, []).append(mapping)

    resolutions: dict[int, _MappingResolution] = {}
    for product in products:
        variant_id = (product.loyverse_variant_id or "").strip()
        if variant_id:
            resolutions[product.id] = _MappingResolution(loyverse_variant_id=variant_id, notes=[])
            continue

        sku = (product.sku or "").strip()
        fallback_mappings = mappings_by_sku.get(sku, [])
        if len(fallback_mappings) == 1:
            resolutions[product.id] = _MappingResolution(
                loyverse_variant_id=fallback_mappings[0].loyverse_variant_id,
                notes=["Using active Loyverse variant mapping fallback by exact SKU."],
            )
        elif len(fallback_mappings) > 1:
            resolutions[product.id] = _MappingResolution(
                loyverse_variant_id="",
                notes=["Multiple active Loyverse variant mappings were found for this SKU."],
            )
        else:
            resolutions[product.id] = _MappingResolution(
                loyverse_variant_id="",
                notes=["No Loyverse variant mapping was found for this SKU."],
            )
    return resolutions


def _load_inventory_balances(
    db: Session,
    products: list[Product],
) -> dict[int, InventoryBalance]:
    if not products:
        return {}
    balances = (
        db.query(InventoryBalance)
        .filter(InventoryBalance.product_id.in_([product.id for product in products]))
        .all()
    )
    return {balance.product_id: balance for balance in balances}


def _load_loyverse_inventory_levels(
    token: str,
    store_id: str,
    variant_ids: list[str],
) -> tuple[dict[str, Decimal], dict[str, str], list[str]]:
    rows: list[dict] = []
    warnings: list[str] = []
    variant_errors: dict[str, str] = {}
    for chunk in _chunk_variant_ids(variant_ids):
        chunk_rows, chunk_errors, chunk_warnings = _fetch_inventory_rows_for_variant_chunk(token, store_id, chunk)
        rows.extend(chunk_rows)
        variant_errors.update(chunk_errors)
        warnings.extend(chunk_warnings)

    lookup: dict[str, Decimal] = {}
    seen_variants: set[str] = set()
    duplicate_variants: set[str] = set()

    for row in rows:
        row_store_id = _string_value(row, "store_id", "storeId")
        variant_id = _string_value(row, "variant_id", "variantId")
        if row_store_id != store_id or not variant_id:
            continue
        if variant_id in seen_variants:
            duplicate_variants.add(variant_id)
            continue
        seen_variants.add(variant_id)
        try:
            lookup[variant_id] = _decimal_value(row, "in_stock", "stock_after", "stock", "available")
        except RuntimeError as exc:
            variant_errors[variant_id] = str(exc)

    for duplicate_variant_id in duplicate_variants:
        lookup.pop(duplicate_variant_id, None)
        variant_errors[duplicate_variant_id] = (
            "Loyverse returned multiple inventory rows for this variant/store combination."
        )
    if duplicate_variants:
        warnings.append(
            "Loyverse returned duplicate inventory rows for some variants. Those rows were marked as api_error in the preview."
        )
    return lookup, variant_errors, warnings


def _fetch_inventory_rows_for_variant_chunk(
    token: str,
    store_id: str,
    variant_ids: list[str],
) -> tuple[list[dict], dict[str, str], list[str]]:
    try:
        return _request_inventory_rows(token, store_id, variant_ids), {}, []
    except RuntimeError as exc:
        error_message = str(exc)
        if _is_http_403(error_message) and len(variant_ids) > 1:
            midpoint = max(1, len(variant_ids) // 2)
            left_ids = variant_ids[:midpoint]
            right_ids = variant_ids[midpoint:]
            left_rows, left_errors, left_warnings = _fetch_inventory_rows_for_variant_chunk(token, store_id, left_ids)
            right_rows, right_errors, right_warnings = _fetch_inventory_rows_for_variant_chunk(token, store_id, right_ids)
            return (
                [*left_rows, *right_rows],
                {**left_errors, **right_errors},
                [
                    f"Loyverse rejected a larger inventory batch for {len(variant_ids)} variants; the preview retried with smaller batches.",
                    *left_warnings,
                    *right_warnings,
                ],
            )
        return [], {variant_id: error_message for variant_id in variant_ids}, []


def _request_inventory_rows(
    token: str,
    store_id: str,
    variant_ids: list[str],
) -> list[dict]:
    rows: list[dict] = []
    cursor = ""
    while True:
        params = {
            "store_ids": store_id,
            "variant_ids": ",".join(variant_ids),
            "limit": str(INVENTORY_LIMIT),
        }
        if cursor:
            params["cursor"] = cursor
        payload = _loyverse_get(token, f"/inventory?{urlencode(params)}")
        inventory_rows = payload.get("inventory_levels") or []
        if not isinstance(inventory_rows, list):
            raise RuntimeError("Loyverse inventory response did not include a valid inventory_levels list.")
        rows.extend(row for row in inventory_rows if isinstance(row, dict))
        cursor = _string_value(payload, "cursor", "next_cursor")
        if not cursor:
            break
    return rows


def _resolve_loyverse_store_id(token: str) -> tuple[str, list[str], str]:
    configured_store_id = _require_store_id(optional=True)
    if configured_store_id:
        return configured_store_id, [], ""

    stores = _fetch_loyverse_stores(token)
    if len(stores) == 1:
        store_id = _string_value(stores[0], "id")
        if store_id:
            return (
                store_id,
                [
                    "LOYVERSE_STORE_ID was not configured; automatically using the only Loyverse store returned by the API."
                ],
                "",
            )
    if len(stores) == 0:
        return (
            "",
            [],
            "LOYVERSE_STORE_ID is not configured and no Loyverse stores were returned by the API.",
        )
    return (
        "",
        [],
        "LOYVERSE_STORE_ID is not configured and multiple Loyverse stores were returned. Configure LOYVERSE_STORE_ID explicitly.",
    )


def _fetch_loyverse_stores(token: str) -> list[dict]:
    stores: list[dict] = []
    cursor = ""
    while True:
        path = "/stores"
        if cursor:
            path = f"/stores?{urlencode({'cursor': cursor})}"
        payload = _loyverse_get(token, path)
        store_rows = payload.get("stores") or []
        if not isinstance(store_rows, list):
            raise RuntimeError("Loyverse stores response did not include a valid stores list.")
        stores.extend(row for row in store_rows if isinstance(row, dict))
        cursor = _string_value(payload, "cursor", "next_cursor")
        if not cursor:
            break
    return stores


def _build_comparison_rows(
    products: list[Product],
    mapping_resolutions: dict[int, _MappingResolution],
    balances: dict[int, InventoryBalance],
    loyverse_inventory_by_variant: dict[str, Decimal],
    variant_errors: dict[str, str],
    api_error_message: str,
) -> list[ERPLoyverseStockPreviewRow]:
    rows: list[ERPLoyverseStockPreviewRow] = []
    for product in products:
        resolution = mapping_resolutions[product.id]
        balance = balances.get(product.id)
        erp_stock = _money(balance.on_hand_qty) if balance is not None else ZERO
        loyverse_stock = None
        notes = list(resolution.notes)
        if balance is None:
            notes.append("Official ERP stock uses InventoryBalance. No balance row was found, so ERP stock is shown as 0.")

        if resolution.loyverse_variant_id:
            if api_error_message:
                notes.append(api_error_message)
            elif resolution.loyverse_variant_id in variant_errors:
                notes.append(variant_errors[resolution.loyverse_variant_id])
            elif resolution.loyverse_variant_id in loyverse_inventory_by_variant:
                loyverse_stock = loyverse_inventory_by_variant[resolution.loyverse_variant_id]
            else:
                notes.append("No Loyverse inventory row was returned for this mapped variant/store.")

        difference = (erp_stock - loyverse_stock).quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP) if loyverse_stock is not None else None
        absolute_difference = abs(difference).quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP) if difference is not None else None
        status = _classify_row_status(
            product=product,
            has_inventory_balance=balance is not None,
            loyverse_variant_id=resolution.loyverse_variant_id,
            loyverse_stock=loyverse_stock,
            difference=difference,
            api_error_message=api_error_message,
            variant_error_message=variant_errors.get(resolution.loyverse_variant_id or ""),
        )
        rows.append(
            ERPLoyverseStockPreviewRow(
                product_id=product.id,
                sku=(product.sku or "").strip(),
                product_name=product.name,
                active=bool(product.active),
                available_for_sale_gc=bool(product.available_for_sale_gc),
                loyverse_variant_id=resolution.loyverse_variant_id,
                erp_stock_official=erp_stock,
                loyverse_stock=loyverse_stock,
                difference=difference,
                absolute_difference=absolute_difference,
                status=status,
                notes=notes,
            )
        )
    return rows


def _build_summary(rows: list[ERPLoyverseStockPreviewRow]) -> ERPLoyverseStockPreviewSummary:
    return ERPLoyverseStockPreviewSummary(
        products_compared=len(rows),
        matched=sum(1 for row in rows if row.status == "matched"),
        difference=sum(1 for row in rows if row.status == "difference"),
        missing_in_loyverse=sum(1 for row in rows if row.status == "missing_in_loyverse"),
        missing_inventory_balance=sum(1 for row in rows if row.status == "missing_inventory_balance"),
        inactive_products=sum(1 for row in rows if row.status == "inactive_product"),
        mapping_missing=sum(1 for row in rows if row.status == "mapping_missing"),
        api_errors=sum(1 for row in rows if row.status == "api_error"),
        total_absolute_difference=sum(
            (row.absolute_difference or ZERO for row in rows),
            ZERO,
        ).quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP),
    )


def _classify_row_status(
    *,
    product: Product,
    has_inventory_balance: bool,
    loyverse_variant_id: str,
    loyverse_stock: Decimal | None,
    difference: Decimal | None,
    api_error_message: str,
    variant_error_message: str | None,
) -> str:
    if not product.active:
        return "inactive_product"
    if not product.available_for_sale_gc:
        return "not_available_for_sale_gc"
    if not loyverse_variant_id:
        return "mapping_missing"
    if not has_inventory_balance:
        return "missing_inventory_balance"
    if api_error_message or variant_error_message:
        return "api_error"
    if loyverse_stock is None:
        return "missing_in_loyverse"
    if difference is not None and difference != ZERO:
        return "difference"
    return "matched"


def _chunk_variant_ids(variant_ids: list[str]) -> list[list[str]]:
    return [
        variant_ids[index:index + VARIANT_BATCH_SIZE]
        for index in range(0, len(variant_ids), VARIANT_BATCH_SIZE)
    ]


def _is_http_403(message: str) -> bool:
    return "HTTP 403" in (message or "")


def _require_token(*, optional: bool = False) -> str:
    token = os.getenv("LOYVERSE_API_TOKEN", "").strip()
    if token or optional:
        return token
    raise RuntimeError("LOYVERSE_API_TOKEN is not configured.")


def _require_store_id(*, optional: bool = False) -> str:
    store_id = os.getenv("LOYVERSE_STORE_ID", "").strip()
    if store_id or optional:
        return store_id
    raise RuntimeError("LOYVERSE_STORE_ID is not configured.")


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
        raise RuntimeError(f"Loyverse API returned HTTP {exc.code}: {_summarize_response(response_text)}") from exc
    except TimeoutError as exc:
        raise RuntimeError("Loyverse inventory preview request timed out.") from exc
    except URLError as exc:
        raise RuntimeError(f"Loyverse inventory preview request failed: {exc.reason}") from exc

    if not response_text:
        return {}
    try:
        return json.loads(response_text)
    except ValueError as exc:
        raise RuntimeError("Loyverse inventory preview response was not valid JSON.") from exc


def _decimal_value(payload: dict, *keys: str) -> Decimal:
    for key in keys:
        value = payload.get(key)
        if value in (None, ""):
            continue
        try:
            return Decimal(str(value)).quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP)
        except (InvalidOperation, ValueError) as exc:
            raise RuntimeError(f"Loyverse inventory value for {key} was not numeric: {value}.") from exc
    return ZERO


def _money(value: Decimal | int | str | None) -> Decimal:
    if value is None:
        return ZERO
    return Decimal(str(value)).quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP)


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
