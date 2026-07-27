from __future__ import annotations

import json
import os
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from sqlalchemy import or_
from sqlalchemy.orm import Session

from app.models import InventoryBalance, LoyverseVariantMapping, Product


LOYVERSE_API_BASE_URL = "https://api.loyverse.com/v1.0"
LOYVERSE_REQUEST_TIMEOUT_SECONDS = 8
DECIMAL_QUANT = Decimal("0.0001")
COST_TOLERANCE = Decimal("0.01")
MAX_COST_PREVIEW_PRODUCTS = 75
ZERO = Decimal("0")


@dataclass(frozen=True)
class ERPLoyverseCostPreviewRow:
    product_id: int
    sku: str
    product_name: str
    erp_standard_cost: Decimal | None
    loyverse_cost: Decimal | None
    difference: Decimal | None
    absolute_difference: Decimal | None
    average_unit_cost_reference: Decimal | None
    status: str
    notes: list[str]
    loyverse_variant_id: str
    loyverse_item_id: str
    cost_source: str


@dataclass(frozen=True)
class ERPLoyverseCostPreviewSummary:
    products_eligible_before_limit: int
    products_compared: int
    products_skipped_by_limit: int
    matched: int
    difference: int
    missing_erp_cost: int
    missing_loyverse_cost: int
    mapping_missing: int
    inactive_products: int
    not_available_for_sale: int
    api_errors: int
    total_absolute_cost_difference: Decimal


@dataclass(frozen=True)
class ERPLoyverseCostPreviewResult:
    active_only: bool
    available_only: bool
    only_differences: bool
    sku_query: str
    rows: list[ERPLoyverseCostPreviewRow]
    summary: ERPLoyverseCostPreviewSummary
    warnings: list[str]


@dataclass(frozen=True)
class _MappingResolution:
    loyverse_variant_id: str
    loyverse_item_id: str
    notes: list[str]


@dataclass(frozen=True)
class _LoyverseCostLookup:
    cost: Decimal | None
    item_id: str
    cost_source: str
    error: str


def build_erp_loyverse_cost_preview(
    db: Session,
    *,
    active_only: bool = True,
    available_only: bool = False,
    only_differences: bool = False,
    sku_query: str = "",
) -> ERPLoyverseCostPreviewResult:
    candidate_products = _load_candidate_products(
        db,
        active_only=active_only,
        available_only=available_only,
        sku_query=sku_query,
    )
    products = candidate_products[:MAX_COST_PREVIEW_PRODUCTS]
    mapping_resolutions = _resolve_product_mapping(db, products)
    balances_by_product_id = _load_inventory_balances(db, products)

    token = _require_token(optional=True)
    warnings: list[str] = [
        (
            f"Cost preview is limited to {MAX_COST_PREVIEW_PRODUCTS} products per run to avoid excessive "
            "Loyverse API calls. Use SKU search or filters to narrow the comparison."
        )
    ]
    api_error_message = ""
    variant_cache: dict[str, dict] = {}
    item_cache: dict[str, dict] = {}

    if not token:
        api_error_message = "LOYVERSE_API_TOKEN is not configured."
        warnings.append(api_error_message)

    rows = _build_comparison_rows(
        products=products,
        mapping_resolutions=mapping_resolutions,
        balances_by_product_id=balances_by_product_id,
        token=token,
        api_error_message=api_error_message,
        variant_cache=variant_cache,
        item_cache=item_cache,
    )
    if only_differences:
        rows = [row for row in rows if row.status != "matched"]

    return ERPLoyverseCostPreviewResult(
        active_only=active_only,
        available_only=available_only,
        only_differences=only_differences,
        sku_query=sku_query,
        rows=rows,
        summary=_build_summary(rows, len(candidate_products)),
        warnings=warnings,
    )


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


def _resolve_product_mapping(
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
        item_id = (product.loyverse_item_id or "").strip()
        if variant_id:
            resolutions[product.id] = _MappingResolution(
                loyverse_variant_id=variant_id,
                loyverse_item_id=item_id,
                notes=[],
            )
            continue

        sku = (product.sku or "").strip()
        fallback_mappings = mappings_by_sku.get(sku, [])
        if len(fallback_mappings) == 1:
            mapping = fallback_mappings[0]
            resolutions[product.id] = _MappingResolution(
                loyverse_variant_id=(mapping.loyverse_variant_id or "").strip(),
                loyverse_item_id=(mapping.loyverse_item_id or "").strip(),
                notes=["Using active Loyverse variant mapping fallback by exact SKU."],
            )
        elif len(fallback_mappings) > 1:
            resolutions[product.id] = _MappingResolution(
                loyverse_variant_id="",
                loyverse_item_id="",
                notes=["Multiple active Loyverse variant mappings were found for this SKU."],
            )
        else:
            resolutions[product.id] = _MappingResolution(
                loyverse_variant_id="",
                loyverse_item_id="",
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


def _fetch_loyverse_variant(
    token: str,
    variant_id: str,
    cache: dict[str, dict],
) -> dict:
    cached = cache.get(variant_id)
    if cached is not None:
        return cached
    payload = _loyverse_get(token, f"/variants/{variant_id}")
    cache[variant_id] = payload
    return payload


def _fetch_loyverse_item(
    token: str,
    item_id: str,
    cache: dict[str, dict],
) -> dict:
    cached = cache.get(item_id)
    if cached is not None:
        return cached
    payload = _loyverse_get(token, f"/items/{item_id}")
    cache[item_id] = payload
    return payload


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


def _read_loyverse_cost_for_product(
    *,
    token: str,
    mapping: _MappingResolution,
    variant_cache: dict[str, dict],
    item_cache: dict[str, dict],
) -> _LoyverseCostLookup:
    variant_id = (mapping.loyverse_variant_id or "").strip()
    fallback_item_id = (mapping.loyverse_item_id or "").strip()
    if not variant_id:
        return _LoyverseCostLookup(cost=None, item_id=fallback_item_id, cost_source="missing", error="")

    try:
        variant_payload = _fetch_loyverse_variant(token, variant_id, variant_cache)
        variant_cost = _extract_cost_from_payload(variant_payload, variant_id=variant_id)
        resolved_item_id = _string_value(variant_payload, "item_id", "itemId") or fallback_item_id
        if variant_cost is not None:
            return _LoyverseCostLookup(
                cost=variant_cost,
                item_id=resolved_item_id,
                cost_source="variant",
                error="",
            )

        if resolved_item_id:
            item_payload = _fetch_loyverse_item(token, resolved_item_id, item_cache)
            item_cost = _extract_cost_from_payload(item_payload, variant_id=variant_id)
            if item_cost is not None:
                return _LoyverseCostLookup(
                    cost=item_cost,
                    item_id=resolved_item_id,
                    cost_source="item",
                    error="",
                )

        return _LoyverseCostLookup(cost=None, item_id=resolved_item_id, cost_source="missing", error="")
    except RuntimeError as exc:
        return _LoyverseCostLookup(
            cost=None,
            item_id=fallback_item_id,
            cost_source="api_error",
            error=str(exc),
        )


def _build_comparison_rows(
    *,
    products: list[Product],
    mapping_resolutions: dict[int, _MappingResolution],
    balances_by_product_id: dict[int, InventoryBalance],
    token: str,
    api_error_message: str,
    variant_cache: dict[str, dict],
    item_cache: dict[str, dict],
) -> list[ERPLoyverseCostPreviewRow]:
    rows: list[ERPLoyverseCostPreviewRow] = []
    for product in products:
        mapping = mapping_resolutions[product.id]
        erp_standard_cost = _optional_money(product.standard_cost)
        average_unit_cost_reference = None
        balance = balances_by_product_id.get(product.id)
        if balance is not None:
            average_unit_cost_reference = _optional_money(balance.average_unit_cost)

        notes = list(mapping.notes)
        loyverse_cost = None
        loyverse_item_id = (mapping.loyverse_item_id or "").strip()
        cost_source = "missing"
        row_api_error = ""
        if api_error_message:
            row_api_error = api_error_message
        elif mapping.loyverse_variant_id:
            lookup = _read_loyverse_cost_for_product(
                token=token,
                mapping=mapping,
                variant_cache=variant_cache,
                item_cache=item_cache,
            )
            loyverse_cost = lookup.cost
            loyverse_item_id = lookup.item_id
            cost_source = lookup.cost_source
            row_api_error = lookup.error
            if lookup.error:
                notes.append(lookup.error)

        difference = None
        absolute_difference = None
        if erp_standard_cost is not None and loyverse_cost is not None:
            difference = (erp_standard_cost - loyverse_cost).quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP)
            absolute_difference = abs(difference).quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP)

        status = _classify_row_status(
            product=product,
            api_error_message=row_api_error,
            loyverse_variant_id=mapping.loyverse_variant_id,
            erp_standard_cost=erp_standard_cost,
            loyverse_cost=loyverse_cost,
            absolute_difference=absolute_difference,
        )
        if loyverse_cost is None and not row_api_error and mapping.loyverse_variant_id:
            notes.append("No numeric cost value was returned by Loyverse for this variant/item mapping.")

        rows.append(
            ERPLoyverseCostPreviewRow(
                product_id=product.id,
                sku=(product.sku or "").strip(),
                product_name=product.name,
                erp_standard_cost=erp_standard_cost,
                loyverse_cost=loyverse_cost,
                difference=difference,
                absolute_difference=absolute_difference,
                average_unit_cost_reference=average_unit_cost_reference,
                status=status,
                notes=notes,
                loyverse_variant_id=mapping.loyverse_variant_id,
                loyverse_item_id=loyverse_item_id,
                cost_source=cost_source,
            )
        )
    return rows


def _build_summary(
    rows: list[ERPLoyverseCostPreviewRow],
    products_eligible_before_limit: int,
) -> ERPLoyverseCostPreviewSummary:
    return ERPLoyverseCostPreviewSummary(
        products_eligible_before_limit=products_eligible_before_limit,
        products_compared=len(rows),
        products_skipped_by_limit=max(products_eligible_before_limit - len(rows), 0),
        matched=sum(1 for row in rows if row.status == "matched"),
        difference=sum(1 for row in rows if row.status == "difference"),
        missing_erp_cost=sum(1 for row in rows if row.status == "missing_erp_cost"),
        missing_loyverse_cost=sum(1 for row in rows if row.status == "missing_loyverse_cost"),
        mapping_missing=sum(1 for row in rows if row.status == "mapping_missing"),
        inactive_products=sum(1 for row in rows if row.status == "inactive_product"),
        not_available_for_sale=sum(1 for row in rows if row.status == "not_available_for_sale_gc"),
        api_errors=sum(1 for row in rows if row.status == "api_error"),
        total_absolute_cost_difference=sum(
            (row.absolute_difference or ZERO for row in rows),
            ZERO,
        ).quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP),
    )


def _classify_row_status(
    *,
    product: Product,
    api_error_message: str,
    loyverse_variant_id: str,
    erp_standard_cost: Decimal | None,
    loyverse_cost: Decimal | None,
    absolute_difference: Decimal | None,
) -> str:
    if api_error_message:
        return "api_error"
    if not loyverse_variant_id:
        return "mapping_missing"
    if not product.active:
        return "inactive_product"
    if not product.available_for_sale_gc:
        return "not_available_for_sale_gc"
    if erp_standard_cost is None:
        return "missing_erp_cost"
    if loyverse_cost is None:
        return "missing_loyverse_cost"
    if absolute_difference is not None and absolute_difference <= COST_TOLERANCE:
        return "matched"
    return "difference"


def _require_token(*, optional: bool = False) -> str:
    token = os.getenv("LOYVERSE_API_TOKEN", "").strip()
    if token or optional:
        return token
    raise RuntimeError("LOYVERSE_API_TOKEN is not configured.")


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
    except URLError as exc:
        raise RuntimeError(f"Loyverse API request failed: {exc.reason}") from exc
    except TimeoutError as exc:
        raise RuntimeError("Loyverse API request timed out.") from exc

    if not response_text:
        return {}
    try:
        return json.loads(response_text)
    except ValueError as exc:
        raise RuntimeError("Loyverse API returned invalid JSON.") from exc


def _decimal_value_or_none(payload: dict, *keys: str) -> Decimal | None:
    for key in keys:
        value = payload.get(key)
        if value is None or value == "":
            continue
        try:
            return Decimal(str(value)).quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP)
        except (InvalidOperation, ValueError) as exc:
            raise RuntimeError(f"{key} was not numeric: {value}") from exc
    return None


def _optional_money(value) -> Decimal | None:
    if value is None:
        return None
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
