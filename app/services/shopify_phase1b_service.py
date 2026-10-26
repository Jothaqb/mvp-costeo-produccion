from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import threading
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Mapping

from sqlalchemy import or_, text
from sqlalchemy.orm import Session

from app.models import (
    B2CCustomer,
    B2CSalesOrder,
    ShopifyOrderIntegration,
    ShopifyOrderLineIntegration,
    ShopifyWebhookEvent,
)
from app.services.audit_service import add_audit_event_in_session, snapshot_b2c_sales_order_for_audit
from app.services.b2c_customer_service import create_b2c_customer_in_session
from app.services.b2c_sales_service import (
    create_b2c_sales_order_in_session,
    recalculate_b2c_order_totals_from_line_snapshots,
    set_b2c_line_pricing_snapshots,
)
from app.services.shopify_phase1a_service import (
    add_shopify_customer_mapping_in_session,
    has_sufficient_shopify_customer_identity,
    resolve_online_web_channel,
    resolve_shopify_customer_match,
    resolve_shopify_product,
    shopify_integration_enabled,
)


SHOPIFY_WEBHOOK_PATH = "/integrations/shopify/webhooks/orders-create"
SHOPIFY_ORDERS_CREATE_TOPIC = "orders/create"
SHOPIFY_API_VERSION_DEFAULT = "2026-10"
SHOPIFY_MAX_WEBHOOK_BYTES_DEFAULT = 2 * 1024 * 1024
SHOPIFY_MAX_PROCESSING_ATTEMPTS = 8
SHOPIFY_RETRY_BASE_SECONDS = 30
SHOPIFY_PROCESSING_STALE_MINUTES = 10
SHOPIFY_WORKER_INTERVAL_SECONDS_DEFAULT = 15
SHOPIFY_RETRY_CSRF_PURPOSE = b"green-corner:shopify:event-retry:post:v1"
SHOPIFY_SHIPPING_SKUS = frozenset({"10219", "10258"})
SHOPIFY_PICKUP_SENTINEL = "pickup"
SHOPIFY_COSTA_RICA_TZ = timezone(timedelta(hours=-6))
MONEY_QUANT = Decimal("0.0001")
ZERO = Decimal("0")


class ShopifyPhase1BError(Exception):
    pass


class ShopifyConfigurationError(ShopifyPhase1BError):
    pass


class ShopifyWebhookSecurityError(ShopifyPhase1BError):
    pass


class ShopifyReviewRequired(ShopifyPhase1BError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class ShopifyTransientError(ShopifyPhase1BError):
    pass


@dataclass(frozen=True)
class ShopifyRuntimeConfig:
    shop_domain: str
    webhook_secret: str
    api_version: str
    shipping_method_map: dict[str, str]
    max_webhook_bytes: int


@dataclass(frozen=True)
class PreparedShopifyLine:
    shopify_line_gid: str
    shopify_variant_gid: str | None
    sku: str
    description: str
    quantity: Decimal
    unit_price: Decimal
    gross_amount: Decimal
    discount_amount: Decimal
    line_type: str

    @property
    def net_amount(self) -> Decimal:
        return (self.gross_amount - self.discount_amount).quantize(MONEY_QUANT)


@dataclass(frozen=True)
class PreparedShopifyOrder:
    order_gid: str
    order_name: str | None
    customer_gid: str | None
    customer_name: str
    phone: str | None
    email: str | None
    address: str | None
    province: str | None
    canton: str | None
    district: str | None
    order_date: date
    financial_status: str | None
    fulfillment_status: str | None
    currency_code: str
    total_received_amount: Decimal | None
    outstanding_amount: Decimal | None
    refunded_amount: Decimal | None
    financial_updated_at: datetime | None
    product_net_amount: Decimal
    shipping_gross_amount: Decimal
    shipping_discount_amount: Decimal
    shipping_net_amount: Decimal
    tax_amount: Decimal
    total_amount: Decimal
    delivery_method_type: str
    shipping_source: str | None
    shipping_code: str | None
    shipping_title: str | None
    created_at: datetime | None
    updated_at: datetime | None
    lines: tuple[PreparedShopifyLine, ...]
    source_fingerprint: str


@dataclass(frozen=True)
class ShopifyProcessingResult:
    event_id: int
    status: str
    order_integration_id: int | None
    b2c_sales_order_id: int | None
    message: str | None = None


@dataclass(frozen=True)
class ShopifyFinancialSnapshot:
    order_gid: str
    financial_status: str | None
    currency_code: str
    total_amount: Decimal
    tax_amount: Decimal
    total_received_amount: Decimal
    outstanding_amount: Decimal
    refunded_amount: Decimal
    updated_at: datetime


@dataclass(frozen=True)
class ShopifyReconciliationResult:
    since: datetime
    fetched_order_gids: tuple[str, ...]
    existing_order_gids: tuple[str, ...]
    missing_order_gids: tuple[str, ...]


def normalize_shop_domain(value: str | None) -> str:
    normalized = (value or "").strip().lower().rstrip(".")
    if not normalized or not normalized.endswith(".myshopify.com"):
        raise ShopifyConfigurationError("SHOPIFY_SHOP_DOMAIN must be an exact myshopify.com domain.")
    if "/" in normalized or ":" in normalized:
        raise ShopifyConfigurationError("SHOPIFY_SHOP_DOMAIN must not include a scheme, port, or path.")
    return normalized


def normalize_shipping_method_key(source: str | None, code: str | None) -> str:
    normalized_source = (source or "").strip().casefold()
    normalized_code = (code or "").strip().casefold()
    if not normalized_source or not normalized_code:
        return ""
    return f"{normalized_source}|{normalized_code}"


def parse_shipping_method_map(raw_value: str | None) -> dict[str, str]:
    raw = (raw_value or "").strip()
    if not raw:
        return {}
    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ShopifyConfigurationError("SHOPIFY_SHIPPING_METHOD_MAP must contain valid JSON.") from exc
    if not isinstance(decoded, dict):
        raise ShopifyConfigurationError("SHOPIFY_SHIPPING_METHOD_MAP must be a JSON object.")
    result: dict[str, str] = {}
    for raw_key, raw_destination in decoded.items():
        if not isinstance(raw_key, str) or "|" not in raw_key:
            raise ShopifyConfigurationError(
                "Each Shopify shipping mapping key must use the exact source|code format."
            )
        source, code = raw_key.split("|", 1)
        key = normalize_shipping_method_key(source, code)
        destination = str(raw_destination or "").strip().lower()
        if not key or destination not in SHOPIFY_SHIPPING_SKUS | {SHOPIFY_PICKUP_SENTINEL}:
            raise ShopifyConfigurationError(
                "Shopify shipping mappings may target only 10219, 10258, or pickup."
            )
        if key in result:
            raise ShopifyConfigurationError("Shopify shipping method mappings must be unique.")
        result[key] = destination
    return result


def load_shopify_runtime_config(*, require_secret: bool = True) -> ShopifyRuntimeConfig:
    shop_domain = normalize_shop_domain(os.getenv("SHOPIFY_SHOP_DOMAIN"))
    webhook_secret = os.getenv("SHOPIFY_WEBHOOK_SECRET", "").strip()
    client_secret = os.getenv("SHOPIFY_CLIENT_SECRET", "").strip()
    if webhook_secret and client_secret and not hmac.compare_digest(webhook_secret, client_secret):
        raise ShopifyConfigurationError(
            "SHOPIFY_WEBHOOK_SECRET must match SHOPIFY_CLIENT_SECRET for the configured Shopify app."
        )
    if not webhook_secret and client_secret:
        webhook_secret = client_secret
    if require_secret and not webhook_secret:
        raise ShopifyConfigurationError("SHOPIFY_WEBHOOK_SECRET is required.")
    api_version = os.getenv("SHOPIFY_API_VERSION", SHOPIFY_API_VERSION_DEFAULT).strip()
    if not api_version:
        raise ShopifyConfigurationError("SHOPIFY_API_VERSION is required.")
    raw_max_bytes = os.getenv("SHOPIFY_MAX_WEBHOOK_BYTES", "").strip()
    try:
        max_bytes = int(raw_max_bytes) if raw_max_bytes else SHOPIFY_MAX_WEBHOOK_BYTES_DEFAULT
    except ValueError as exc:
        raise ShopifyConfigurationError("SHOPIFY_MAX_WEBHOOK_BYTES must be an integer.") from exc
    if max_bytes < 1024 or max_bytes > 10 * 1024 * 1024:
        raise ShopifyConfigurationError("SHOPIFY_MAX_WEBHOOK_BYTES must be between 1 KB and 10 MB.")
    return ShopifyRuntimeConfig(
        shop_domain=shop_domain,
        webhook_secret=webhook_secret,
        api_version=api_version,
        shipping_method_map=parse_shipping_method_map(os.getenv("SHOPIFY_SHIPPING_METHOD_MAP")),
        max_webhook_bytes=max_bytes,
    )


def shopify_worker_enabled() -> bool:
    return shopify_integration_enabled() and os.getenv(
        "SHOPIFY_BACKGROUND_PROCESSOR_ENABLED", ""
    ).strip().lower() in {"1", "true", "yes", "on"}


def shopify_worker_interval_seconds() -> int:
    raw = os.getenv("SHOPIFY_WORKER_INTERVAL_SECONDS", "").strip()
    try:
        value = int(raw) if raw else SHOPIFY_WORKER_INTERVAL_SECONDS_DEFAULT
    except ValueError as exc:
        raise ShopifyConfigurationError("SHOPIFY_WORKER_INTERVAL_SECONDS must be an integer.") from exc
    if value < 5 or value > 300:
        raise ShopifyConfigurationError("SHOPIFY_WORKER_INTERVAL_SECONDS must be between 5 and 300.")
    return value


def verify_shopify_webhook_hmac(raw_body: bytes, provided_hmac: str | None, secret: str) -> bool:
    provided = (provided_hmac or "").strip()
    if not provided or not secret:
        return False
    expected = base64.b64encode(
        hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).digest()
    ).decode("ascii")
    return hmac.compare_digest(provided, expected)


def validate_shopify_webhook_request(
    *,
    raw_body: bytes,
    headers: Mapping[str, str],
    config: ShopifyRuntimeConfig,
) -> tuple[str, str, str, datetime | None]:
    if not shopify_integration_enabled():
        raise ShopifyWebhookSecurityError("Shopify integration is disabled.")
    if not raw_body:
        raise ShopifyWebhookSecurityError("Shopify webhook body is empty.")
    if len(raw_body) > config.max_webhook_bytes:
        raise ShopifyWebhookSecurityError("Shopify webhook body exceeds the configured limit.")
    shop_domain = (headers.get("x-shopify-shop-domain") or "").strip().lower()
    if not hmac.compare_digest(shop_domain, config.shop_domain):
        raise ShopifyWebhookSecurityError("Shopify webhook shop is not authorized.")
    topic = (headers.get("x-shopify-topic") or "").strip().lower()
    if not hmac.compare_digest(topic, SHOPIFY_ORDERS_CREATE_TOPIC):
        raise ShopifyWebhookSecurityError("Shopify webhook topic is not authorized.")
    webhook_id = (headers.get("x-shopify-webhook-id") or "").strip()
    if not webhook_id:
        raise ShopifyWebhookSecurityError("Shopify webhook ID is required.")
    if not verify_shopify_webhook_hmac(
        raw_body,
        headers.get("x-shopify-hmac-sha256"),
        config.webhook_secret,
    ):
        raise ShopifyWebhookSecurityError("Shopify webhook signature is invalid.")
    try:
        payload = json.loads(raw_body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ShopifyWebhookSecurityError("Shopify webhook body is not valid JSON.") from exc
    if not isinstance(payload, dict):
        raise ShopifyWebhookSecurityError("Shopify webhook payload must be a JSON object.")
    api_version = (headers.get("x-shopify-api-version") or "").strip()
    triggered_at = _parse_datetime(headers.get("x-shopify-triggered-at"), required=False)
    return shop_domain, webhook_id, api_version, triggered_at


def build_shopify_retry_csrf_token(session_token: str | None, event_id: int) -> str:
    normalized_session = (session_token or "").strip()
    if not normalized_session:
        raise ShopifyWebhookSecurityError("No valid session is available for CSRF protection.")
    payload = SHOPIFY_RETRY_CSRF_PURPOSE + b":" + str(event_id).encode("ascii")
    return hmac.new(normalized_session.encode("utf-8"), payload, hashlib.sha256).hexdigest()


def validate_shopify_retry_csrf_token(
    session_token: str | None,
    event_id: int,
    submitted_token: str | None,
) -> None:
    expected = build_shopify_retry_csrf_token(session_token, event_id)
    submitted = (submitted_token or "").strip()
    if not submitted or not hmac.compare_digest(submitted, expected):
        raise ShopifyWebhookSecurityError("Invalid CSRF token for Shopify event retry.")


def lock_shopify_order_identity(db: Session, *, shop_domain: str, order_gid: str) -> None:
    if db.get_bind().dialect.name != "postgresql":
        return
    identity = f"shopify-order|{shop_domain.strip().lower()}|{order_gid.strip()}"
    digest = hashlib.sha256(identity.encode("utf-8")).digest()
    lock_key = int.from_bytes(digest[:8], "big", signed=False) & 0x7FFF_FFFF_FFFF_FFFF
    db.execute(text("SELECT pg_advisory_xact_lock(:lock_key)"), {"lock_key": lock_key})


def _money(value: Any, field_name: str, *, required: bool = True) -> Decimal | None:
    if value is None or value == "":
        if required:
            raise ShopifyReviewRequired("missing_money", f"Shopify {field_name} is missing.")
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ShopifyReviewRequired("invalid_money", f"Shopify {field_name} is invalid.") from exc
    if not parsed.is_finite() or parsed < ZERO:
        raise ShopifyReviewRequired("invalid_money", f"Shopify {field_name} must be non-negative.")
    return parsed.quantize(MONEY_QUANT)


def _parse_datetime(value: Any, *, required: bool) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        if required:
            raise ShopifyReviewRequired("missing_datetime", "Shopify order timestamp is missing.")
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        if required:
            raise ShopifyReviewRequired("invalid_datetime", "Shopify order timestamp is invalid.") from exc
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _costa_rica_order_date(value: Any) -> date:
    raw = str(value or "").strip()
    if not raw:
        raise ShopifyReviewRequired("missing_datetime", "Shopify order timestamp is missing.")
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ShopifyReviewRequired("invalid_datetime", "Shopify order timestamp is invalid.") from exc
    if parsed.tzinfo is None:
        raise ShopifyReviewRequired(
            "invalid_datetime",
            "Shopify order timestamp must include its UTC offset.",
        )
    return parsed.astimezone(SHOPIFY_COSTA_RICA_TZ).date()


def _discount_allocations_total(container: Mapping[str, Any], field_name: str) -> Decimal:
    total = ZERO
    allocations = container.get("discount_allocations") or []
    if not isinstance(allocations, list):
        raise ShopifyReviewRequired("invalid_discount", f"Shopify {field_name} discounts are invalid.")
    for allocation in allocations:
        if not isinstance(allocation, dict):
            raise ShopifyReviewRequired("invalid_discount", f"Shopify {field_name} discounts are invalid.")
        amount = allocation.get("amount")
        if amount is None:
            amount = ((allocation.get("amount_set") or {}).get("shop_money") or {}).get("amount")
        total += _money(amount, f"{field_name} discount") or ZERO
    return total.quantize(MONEY_QUANT)


def _order_gid(payload: Mapping[str, Any]) -> str:
    value = str(payload.get("admin_graphql_api_id") or "").strip()
    if value.startswith("gid://shopify/Order/"):
        return value
    numeric_id = str(payload.get("id") or "").strip()
    if numeric_id.isdigit():
        return f"gid://shopify/Order/{numeric_id}"
    raise ShopifyReviewRequired("missing_order_id", "Shopify Order GID is missing.")


def _line_gid(line: Mapping[str, Any], index: int) -> str:
    value = str(line.get("admin_graphql_api_id") or "").strip()
    if value:
        return value
    numeric_id = str(line.get("id") or "").strip()
    if numeric_id:
        return f"gid://shopify/LineItem/{numeric_id}"
    return f"shopify-line-index:{index}"


def _customer_identity(payload: Mapping[str, Any]) -> tuple[str | None, str, str | None, str | None]:
    customer = payload.get("customer") if isinstance(payload.get("customer"), dict) else {}
    shipping = payload.get("shipping_address") if isinstance(payload.get("shipping_address"), dict) else {}
    billing = payload.get("billing_address") if isinstance(payload.get("billing_address"), dict) else {}
    customer_gid = str(customer.get("admin_graphql_api_id") or "").strip() or None
    if customer_gid is None and str(customer.get("id") or "").isdigit():
        customer_gid = f"gid://shopify/Customer/{customer['id']}"
    candidate_names = (
        shipping.get("name"),
        " ".join(str(customer.get(key) or "").strip() for key in ("first_name", "last_name")).strip(),
        billing.get("name"),
    )
    name = next((str(value).strip() for value in candidate_names if str(value or "").strip()), "")
    phone = next(
        (str(value).strip() for value in (shipping.get("phone"), payload.get("phone"), customer.get("phone"), billing.get("phone")) if str(value or "").strip()),
        None,
    )
    email = next(
        (str(value).strip() for value in (payload.get("email"), payload.get("contact_email"), customer.get("email")) if str(value or "").strip()),
        None,
    )
    return customer_gid, name, phone, email


def _address_snapshots(payload: Mapping[str, Any]) -> tuple[str | None, str | None, str | None, str | None]:
    address = payload.get("shipping_address") if isinstance(payload.get("shipping_address"), dict) else {}
    if not address:
        address = payload.get("billing_address") if isinstance(payload.get("billing_address"), dict) else {}
    address_parts = [
        str(address.get(key) or "").strip()
        for key in ("address1", "address2", "city", "province", "zip", "country")
        if str(address.get(key) or "").strip()
    ]
    return (
        ", ".join(address_parts) or None,
        str(address.get("province") or "").strip() or None,
        str(address.get("city") or "").strip() or None,
        str(address.get("address2") or "").strip() or None,
    )


def prepare_shopify_order(
    db: Session,
    *,
    raw_payload: str,
    shipping_method_map: Mapping[str, str],
) -> PreparedShopifyOrder:
    try:
        payload = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        raise ShopifyReviewRequired("invalid_json", "Stored Shopify payload is not valid JSON.") from exc
    if not isinstance(payload, dict):
        raise ShopifyReviewRequired("invalid_json", "Stored Shopify payload must be a JSON object.")

    order_gid = _order_gid(payload)
    currency = str(payload.get("currency") or payload.get("presentment_currency") or "").strip().upper()
    if currency != "CRC":
        raise ShopifyReviewRequired("unsupported_currency", f"Shopify currency {currency or 'missing'} is not CRC.")
    tax_amount = _money(payload.get("current_total_tax", payload.get("total_tax", "0")), "tax") or ZERO
    if tax_amount != ZERO:
        raise ShopifyReviewRequired("nonzero_tax", "Shopify order has non-zero tax.")

    product_lines: list[PreparedShopifyLine] = []
    line_items = payload.get("line_items") or []
    if not isinstance(line_items, list) or not line_items:
        raise ShopifyReviewRequired("missing_products", "Shopify order has no product lines.")
    all_products_require_shipping = False
    for index, source_line in enumerate(line_items, start=1):
        if not isinstance(source_line, dict):
            raise ShopifyReviewRequired("invalid_product_line", "Shopify product line is invalid.")
        sku = str(source_line.get("sku") or "").strip()
        try:
            product = resolve_shopify_product(db, sku)
        except Exception as exc:
            raise ShopifyReviewRequired("product_sku", str(exc)) from exc
        try:
            quantity = Decimal(str(source_line.get("quantity")))
        except (InvalidOperation, ValueError) as exc:
            raise ShopifyReviewRequired("invalid_quantity", f"Shopify SKU {sku} has invalid quantity.") from exc
        if not quantity.is_finite() or quantity <= ZERO:
            raise ShopifyReviewRequired("invalid_quantity", f"Shopify SKU {sku} has invalid quantity.")
        unit_price = _money(source_line.get("price"), f"SKU {sku} unit price") or ZERO
        gross = (unit_price * quantity).quantize(MONEY_QUANT)
        discount = _discount_allocations_total(source_line, f"SKU {sku}")
        if discount > gross:
            raise ShopifyReviewRequired("invalid_discount", f"Shopify SKU {sku} discount exceeds its gross amount.")
        all_products_require_shipping = all_products_require_shipping or bool(source_line.get("requires_shipping", True))
        variant_gid = str(source_line.get("variant_id") or "").strip() or None
        if variant_gid and not variant_gid.startswith("gid://"):
            variant_gid = f"gid://shopify/ProductVariant/{variant_gid}"
        product_lines.append(
            PreparedShopifyLine(
                shopify_line_gid=_line_gid(source_line, index),
                shopify_variant_gid=variant_gid,
                sku=product.sku,
                description=str(source_line.get("name") or product.name).strip() or product.name,
                quantity=quantity,
                unit_price=unit_price,
                gross_amount=gross,
                discount_amount=discount,
                line_type="product",
            )
        )

    shipping_lines = payload.get("shipping_lines") or []
    if not isinstance(shipping_lines, list) or len(shipping_lines) > 1:
        raise ShopifyReviewRequired("shipping_lines", "Shopify order must contain at most one shipping method.")
    shipping_line: PreparedShopifyLine | None = None
    shipping_source = None
    shipping_code = None
    shipping_title = None
    delivery_method_type = "none"
    shipping_gross = ZERO
    shipping_discount = ZERO
    if shipping_lines:
        source_shipping = shipping_lines[0]
        if not isinstance(source_shipping, dict):
            raise ShopifyReviewRequired("shipping_method", "Shopify shipping method is invalid.")
        shipping_source = str(source_shipping.get("source") or "").strip() or None
        shipping_code = str(source_shipping.get("code") or "").strip() or None
        shipping_title = str(source_shipping.get("title") or "").strip() or None
        method_key = normalize_shipping_method_key(shipping_source, shipping_code)
        destination = shipping_method_map.get(method_key) if method_key else None
        if destination is None:
            raise ShopifyReviewRequired(
                "shipping_mapping",
                "Shopify shipping source/code is not configured for an ERP shipping SKU.",
            )
        shipping_gross = _money(source_shipping.get("price", "0"), "shipping charge") or ZERO
        shipping_discount = _discount_allocations_total(source_shipping, "shipping")
        if shipping_discount > shipping_gross:
            raise ShopifyReviewRequired("invalid_shipping_discount", "Shopify shipping discount exceeds shipping charge.")
        if destination == SHOPIFY_PICKUP_SENTINEL:
            if shipping_gross != ZERO or shipping_discount != ZERO:
                raise ShopifyReviewRequired("pickup_amount", "Shopify pickup must not have a shipping charge.")
            delivery_method_type = "pickup"
        else:
            try:
                shipping_product = resolve_shopify_product(db, destination)
            except Exception as exc:
                raise ShopifyReviewRequired("shipping_sku", str(exc)) from exc
            delivery_method_type = "shipping"
            shipping_line = PreparedShopifyLine(
                shopify_line_gid=(
                    str(source_shipping.get("admin_graphql_api_id") or "").strip()
                    or f"gid://shopify/ShippingLine/{source_shipping.get('id', 'primary')}"
                ),
                shopify_variant_gid=None,
                sku=shipping_product.sku,
                description=shipping_title or shipping_product.name,
                quantity=Decimal("1"),
                unit_price=shipping_gross,
                gross_amount=shipping_gross,
                discount_amount=shipping_discount,
                line_type="shipping",
            )
    elif all_products_require_shipping:
        raise ShopifyReviewRequired(
            "shipping_method_missing",
            "Shopify order contains shippable products but no identifiable shipping method.",
        )

    lines = tuple(product_lines + ([shipping_line] if shipping_line is not None else []))
    product_net = sum((line.net_amount for line in product_lines), ZERO).quantize(MONEY_QUANT)
    shipping_net = (shipping_gross - shipping_discount).quantize(MONEY_QUANT)
    calculated_total = (product_net + shipping_net).quantize(MONEY_QUANT)
    shopify_total = _money(
        payload.get("current_total_price", payload.get("total_price")),
        "order total",
    ) or ZERO
    if calculated_total != shopify_total:
        raise ShopifyReviewRequired(
            "total_mismatch",
            f"ERP calculated total {calculated_total} does not match Shopify total {shopify_total}.",
        )
    reported_discount = _money(
        payload.get("current_total_discounts", payload.get("total_discounts")),
        "total discount",
        required=False,
    )
    allocated_discount = sum((line.discount_amount for line in lines), ZERO).quantize(MONEY_QUANT)
    if reported_discount is not None and reported_discount != allocated_discount:
        raise ShopifyReviewRequired(
            "discount_mismatch",
            "Allocated product and shipping discounts do not match Shopify total discounts.",
        )

    customer_gid, customer_name, phone, email = _customer_identity(payload)
    if not has_sufficient_shopify_customer_identity(name=customer_name, phone=phone, email=email):
        raise ShopifyReviewRequired(
            "customer_identity",
            "Shopify buyer needs a name and at least one valid phone or email.",
        )
    address, province, canton, district = _address_snapshots(payload)
    created_at = _parse_datetime(payload.get("created_at"), required=True)
    updated_at = _parse_datetime(payload.get("updated_at"), required=False)
    total_received = _money(payload.get("total_received"), "total received", required=False)
    outstanding = _money(payload.get("total_outstanding"), "outstanding total", required=False)
    refunded = _money(payload.get("total_refunded"), "refunded total", required=False)
    raw_canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return PreparedShopifyOrder(
        order_gid=order_gid,
        order_name=str(payload.get("name") or payload.get("order_number") or "").strip() or None,
        customer_gid=customer_gid,
        customer_name=customer_name,
        phone=phone,
        email=email,
        address=address,
        province=province,
        canton=canton,
        district=district,
        order_date=_costa_rica_order_date(payload.get("created_at")),
        financial_status=str(payload.get("financial_status") or "").strip().upper() or None,
        fulfillment_status=str(payload.get("fulfillment_status") or "").strip().upper() or None,
        currency_code=currency,
        total_received_amount=total_received,
        outstanding_amount=outstanding,
        refunded_amount=refunded,
        financial_updated_at=updated_at or created_at,
        product_net_amount=product_net,
        shipping_gross_amount=shipping_gross,
        shipping_discount_amount=shipping_discount,
        shipping_net_amount=shipping_net,
        tax_amount=tax_amount,
        total_amount=shopify_total,
        delivery_method_type=delivery_method_type,
        shipping_source=shipping_source,
        shipping_code=shipping_code,
        shipping_title=shipping_title,
        created_at=created_at,
        updated_at=updated_at,
        lines=lines,
        source_fingerprint=hashlib.sha256(raw_canonical.encode("utf-8")).hexdigest(),
    )


def _get_or_create_order_integration(
    db: Session,
    *,
    event: ShopifyWebhookEvent,
    order_gid: str,
    order_name: str | None,
) -> ShopifyOrderIntegration:
    lock_shopify_order_identity(db, shop_domain=event.shop_domain, order_gid=order_gid)
    integration = (
        db.query(ShopifyOrderIntegration)
        .filter(
            ShopifyOrderIntegration.shop_domain == event.shop_domain,
            ShopifyOrderIntegration.shopify_order_gid == order_gid,
        )
        .one_or_none()
    )
    if integration is None:
        integration = ShopifyOrderIntegration(
            shop_domain=event.shop_domain,
            shopify_order_gid=order_gid,
            shopify_order_name=order_name,
            source_webhook_event_id=event.id,
            import_status="received",
        )
        db.add(integration)
        db.flush()
    return integration


def _snapshot_integration(integration: ShopifyOrderIntegration) -> dict[str, Any]:
    return {
        "integration_id": integration.id,
        "shop_domain": integration.shop_domain,
        "shopify_order_gid": integration.shopify_order_gid,
        "shopify_order_name": integration.shopify_order_name,
        "import_status": integration.import_status,
        "review_code": integration.review_code,
        "review_message": integration.review_message,
        "b2c_sales_order_id": integration.b2c_sales_order_id,
        "financial_status": integration.financial_status,
        "shopify_total_amount": integration.shopify_total_amount,
    }


def _apply_prepared_integration_snapshot(
    integration: ShopifyOrderIntegration,
    prepared: PreparedShopifyOrder,
) -> None:
    integration.shopify_order_name = prepared.order_name
    integration.shopify_customer_gid = prepared.customer_gid
    integration.financial_status = prepared.financial_status
    integration.fulfillment_status = prepared.fulfillment_status
    integration.currency_code = prepared.currency_code
    integration.total_received_amount = prepared.total_received_amount
    integration.outstanding_amount = prepared.outstanding_amount
    integration.refunded_amount = prepared.refunded_amount
    integration.financial_updated_at = prepared.financial_updated_at
    integration.product_net_amount = prepared.product_net_amount
    integration.shipping_gross_amount = prepared.shipping_gross_amount
    integration.shipping_discount_amount = prepared.shipping_discount_amount
    integration.shipping_net_amount = prepared.shipping_net_amount
    integration.tax_amount = prepared.tax_amount
    integration.shopify_total_amount = prepared.total_amount
    integration.delivery_method_type = prepared.delivery_method_type
    integration.shipping_source = prepared.shipping_source
    integration.shipping_code = prepared.shipping_code
    integration.shipping_title = prepared.shipping_title
    integration.source_fingerprint = prepared.source_fingerprint
    integration.shopify_created_at = prepared.created_at
    integration.shopify_updated_at = prepared.updated_at


def _resolve_or_create_customer(
    db: Session,
    *,
    shop_domain: str,
    prepared: PreparedShopifyOrder,
) -> B2CCustomer:
    match = resolve_shopify_customer_match(
        db,
        shop_domain=shop_domain,
        shopify_customer_gid=prepared.customer_gid,
        phone=prepared.phone,
        email=prepared.email,
    )
    if match.outcome in {"ambiguous", "conflict", "review_pending"}:
        raise ShopifyReviewRequired("customer_match", match.reason or "Shopify customer requires review.")
    customer = match.customer
    if customer is None:
        customer = create_b2c_customer_in_session(
            db,
            name=prepared.customer_name,
            phone=prepared.phone or "",
            email=prepared.email or "",
            address=prepared.address or "",
            province=prepared.province or "",
            canton=prepared.canton or "",
            district=prepared.district or "",
            observations="Created from Shopify order. Master data is not updated automatically.",
            active=True,
        )
    if prepared.customer_gid and match.outcome != "matched_mapping":
        add_shopify_customer_mapping_in_session(
            db,
            shop_domain=shop_domain,
            shopify_customer_gid=prepared.customer_gid,
            customer=customer,
            phone=prepared.phone,
            email=prepared.email,
        )
    return customer


def _create_b2c_order_from_prepared(
    db: Session,
    *,
    event: ShopifyWebhookEvent,
    integration: ShopifyOrderIntegration,
    prepared: PreparedShopifyOrder,
) -> B2CSalesOrder:
    channel = resolve_online_web_channel(db)
    customer = _resolve_or_create_customer(db, shop_domain=event.shop_domain, prepared=prepared)
    order = create_b2c_sales_order_in_session(
        db,
        order_date=prepared.order_date,
        b2c_customer_id=str(customer.id),
        customer_name=prepared.customer_name,
        customer_phone=prepared.phone or "",
        customer_email=prepared.email or "",
        customer_address_snapshot=prepared.address or "",
        province_snapshot=prepared.province or "",
        canton_snapshot=prepared.canton or "",
        district_snapshot=prepared.district or "",
        customer_observations_snapshot="",
        channel_id=str(channel.id),
        discount_rule_id="",
        observations=(
            f"Shopify order {prepared.order_name or prepared.order_gid}. "
            f"Financial status: {prepared.financial_status or 'unknown'}."
        ),
        line_inputs=[
            {
                "sku": line.sku,
                "quantity": str(line.quantity),
                "unit_price": str(line.unit_price),
            }
            for line in prepared.lines
        ],
    )
    if len(order.lines) != len(prepared.lines):
        raise ShopifyTransientError("ERP line creation did not preserve the Shopify line count.")
    for b2c_line, source_line in zip(order.lines, prepared.lines, strict=True):
        b2c_line.line_type = source_line.line_type
        b2c_line.description_snapshot = source_line.description
        set_b2c_line_pricing_snapshots(
            b2c_line,
            gross_amount=source_line.gross_amount,
            discount_amount=source_line.discount_amount,
        )
    recalculate_b2c_order_totals_from_line_snapshots(order)
    if order.total_amount != prepared.total_amount:
        raise ShopifyTransientError("ERP draft total changed while importing the Shopify order.")
    db.flush()
    integration.b2c_sales_order_id = order.id
    integration.import_status = "imported"
    integration.review_code = None
    integration.review_message = None
    integration.error_code = None
    integration.error_message = None
    integration.imported_at = datetime.utcnow()
    for b2c_line, source_line in zip(order.lines, prepared.lines, strict=True):
        db.add(
            ShopifyOrderLineIntegration(
                shopify_order_integration_id=integration.id,
                shopify_line_gid=source_line.shopify_line_gid,
                shopify_variant_gid=source_line.shopify_variant_gid,
                b2c_sales_order_line_id=b2c_line.id,
                line_type=source_line.line_type,
                sku_snapshot=source_line.sku,
                quantity=source_line.quantity,
                gross_amount=source_line.gross_amount,
                discount_amount=source_line.discount_amount,
                net_amount=source_line.net_amount,
            )
        )
    db.flush()
    add_audit_event_in_session(
        db,
        module="shopify",
        action="shopify_b2c_draft_created",
        entity_type="b2c_sales_order",
        entity_id=order.id,
        entity_label=order.order_number,
        new_values={
            "shopify": _snapshot_integration(integration),
            "order": snapshot_b2c_sales_order_for_audit(order),
        },
        notes="Created atomically from a verified Shopify orders/create webhook.",
        username="shopify-webhook",
    )
    return order


def process_claimed_shopify_event_in_session(
    db: Session,
    *,
    event_id: int,
    config: ShopifyRuntimeConfig,
    graph_ql_client: ShopifyGraphQLClient | None = None,
) -> ShopifyProcessingResult:
    query = db.query(ShopifyWebhookEvent).filter(ShopifyWebhookEvent.id == event_id)
    if db.get_bind().dialect.name == "postgresql":
        query = query.with_for_update(of=ShopifyWebhookEvent)
    event = query.one_or_none()
    if event is None:
        raise ShopifyPhase1BError("Shopify event was not found.")
    if event.processing_status == "processed":
        integration = (
            db.query(ShopifyOrderIntegration)
            .filter(ShopifyOrderIntegration.source_webhook_event_id == event.id)
            .one_or_none()
        )
        return ShopifyProcessingResult(
            event.id,
            "processed",
            integration.id if integration else None,
            integration.b2c_sales_order_id if integration else None,
            "Event was already processed.",
        )
    if event.processing_status != "processing":
        raise ShopifyTransientError("Shopify event was not claimed for processing.")
    try:
        if event.shop_domain != config.shop_domain or event.topic != SHOPIFY_ORDERS_CREATE_TOPIC:
            raise ShopifyReviewRequired("event_scope", "Stored Shopify event is outside the configured scope.")
        if event.api_version != config.api_version:
            raise ShopifyReviewRequired(
                "api_version_mismatch",
                "Shopify webhook API version does not match the configured integration version.",
            )
        if not event.payload_body:
            raise ShopifyReviewRequired("payload_missing", "Stored Shopify event has no recoverable payload.")
        prepared = prepare_shopify_order(
            db,
            raw_payload=event.payload_body,
            shipping_method_map=config.shipping_method_map,
        )
        financial_snapshot = (
            graph_ql_client.get_order_financial_snapshot(prepared.order_gid)
            if graph_ql_client is not None
            else None
        )
        if financial_snapshot is not None:
            if financial_snapshot.order_gid != prepared.order_gid:
                raise ShopifyTransientError("Shopify GraphQL returned an unexpected order identity.")
            if financial_snapshot.currency_code != prepared.currency_code:
                raise ShopifyReviewRequired(
                    "financial_currency_mismatch",
                    "Shopify GraphQL financial currency does not match the orders/create payload.",
                )
            if financial_snapshot.total_amount != prepared.total_amount:
                raise ShopifyReviewRequired(
                    "financial_total_changed",
                    "Shopify order total changed after the orders/create payload; generate a new review.",
                )
            if financial_snapshot.tax_amount != prepared.tax_amount:
                raise ShopifyReviewRequired(
                    "financial_tax_changed",
                    "Shopify order tax changed after the orders/create payload; generate a new review.",
                )
        integration = _get_or_create_order_integration(
            db,
            event=event,
            order_gid=prepared.order_gid,
            order_name=prepared.order_name,
        )
        _apply_prepared_integration_snapshot(integration, prepared)
        if financial_snapshot is not None:
            integration.financial_status = financial_snapshot.financial_status
            integration.currency_code = financial_snapshot.currency_code
            integration.total_received_amount = financial_snapshot.total_received_amount
            integration.outstanding_amount = financial_snapshot.outstanding_amount
            integration.refunded_amount = financial_snapshot.refunded_amount
            integration.financial_updated_at = financial_snapshot.updated_at
        if integration.b2c_sales_order_id is not None or integration.import_status == "imported":
            event.processing_status = "processed"
            event.processed_at = datetime.utcnow()
            event.processing_started_at = None
            event.error_code = None
            event.error_message = None
            return ShopifyProcessingResult(
                event.id,
                "processed",
                integration.id,
                integration.b2c_sales_order_id,
                "Shopify order was already imported.",
            )
        order = _create_b2c_order_from_prepared(
            db,
            event=event,
            integration=integration,
            prepared=prepared,
        )
        event.processing_status = "processed"
        event.processed_at = datetime.utcnow()
        event.processing_started_at = None
        event.next_attempt_at = None
        event.error_code = None
        event.error_message = None
        db.flush()
        return ShopifyProcessingResult(event.id, "processed", integration.id, order.id)
    except ShopifyReviewRequired as exc:
        order_gid = None
        order_name = None
        try:
            payload = json.loads(event.payload_body)
            if isinstance(payload, dict):
                order_gid = _order_gid(payload)
                order_name = str(payload.get("name") or "").strip() or None
        except Exception:
            pass
        integration = None
        if order_gid:
            integration = _get_or_create_order_integration(
                db,
                event=event,
                order_gid=order_gid,
                order_name=order_name,
            )
            if integration.b2c_sales_order_id is not None:
                event.processing_status = "processed"
                event.processed_at = datetime.utcnow()
                event.processing_started_at = None
                return ShopifyProcessingResult(
                    event.id,
                    "processed",
                    integration.id,
                    integration.b2c_sales_order_id,
                    "Shopify order was already imported.",
                )
            integration.import_status = "review_pending"
            integration.review_code = exc.code[:100]
            integration.review_message = exc.message
        event.processing_status = "review_pending"
        event.processing_started_at = None
        event.error_code = exc.code[:100]
        event.error_message = exc.message
        event.next_attempt_at = None
        add_audit_event_in_session(
            db,
            module="shopify",
            action="shopify_order_review_pending",
            entity_type="shopify_order_integration" if integration else "shopify_webhook_event",
            entity_id=integration.id if integration else event.id,
            entity_label=order_name or event.webhook_id,
            new_values={"review_code": exc.code, "review_message": exc.message},
            notes="No partial B2C order was created.",
            username="shopify-webhook",
        )
        db.flush()
        return ShopifyProcessingResult(
            event.id,
            "review_pending",
            integration.id if integration else None,
            None,
            exc.message,
        )


def claim_next_shopify_event(db: Session, *, now: datetime | None = None) -> int | None:
    current = now or datetime.utcnow()
    stale_before = current - timedelta(minutes=SHOPIFY_PROCESSING_STALE_MINUTES)
    db.query(ShopifyWebhookEvent).filter(
        ShopifyWebhookEvent.processing_status == "processing",
        ShopifyWebhookEvent.processing_started_at < stale_before,
    ).update(
        {
            ShopifyWebhookEvent.processing_status: "retry",
            ShopifyWebhookEvent.next_attempt_at: current,
            ShopifyWebhookEvent.error_code: "stale_claim",
            ShopifyWebhookEvent.error_message: "Recovered an interrupted Shopify event claim.",
        },
        synchronize_session=False,
    )
    query = (
        db.query(ShopifyWebhookEvent)
        .filter(
            ShopifyWebhookEvent.processing_status.in_(("received", "retry")),
            or_(
                ShopifyWebhookEvent.next_attempt_at.is_(None),
                ShopifyWebhookEvent.next_attempt_at <= current,
            ),
        )
        .order_by(ShopifyWebhookEvent.received_at, ShopifyWebhookEvent.id)
    )
    if db.get_bind().dialect.name == "postgresql":
        query = query.with_for_update(of=ShopifyWebhookEvent, skip_locked=True)
    event = query.first()
    if event is None:
        return None
    event.processing_status = "processing"
    event.processing_started_at = current
    event.attempt_count = int(event.attempt_count or 0) + 1
    event.next_attempt_at = None
    db.flush()
    return event.id


def mark_shopify_event_retry(
    db: Session,
    *,
    event_id: int,
    error: Exception,
    now: datetime | None = None,
) -> None:
    current = now or datetime.utcnow()
    query = db.query(ShopifyWebhookEvent).filter(ShopifyWebhookEvent.id == event_id)
    if db.get_bind().dialect.name == "postgresql":
        query = query.with_for_update(of=ShopifyWebhookEvent)
    event = query.one()
    if event.processing_status == "processed":
        return
    event.error_code = type(error).__name__[:100]
    event.error_message = str(error)[:2000]
    if event.attempt_count >= SHOPIFY_MAX_PROCESSING_ATTEMPTS:
        event.processing_status = "failed"
        event.next_attempt_at = None
    else:
        delay = SHOPIFY_RETRY_BASE_SECONDS * (2 ** max(event.attempt_count - 1, 0))
        event.processing_status = "retry"
        event.next_attempt_at = current + timedelta(seconds=delay)
    event.processing_started_at = None
    db.flush()


def run_shopify_worker_cycle(
    session_factory: Callable[[], Session],
    *,
    max_events: int = 10,
    config: ShopifyRuntimeConfig | None = None,
    graph_ql_client: ShopifyGraphQLClient | None = None,
) -> list[ShopifyProcessingResult]:
    if not shopify_worker_enabled():
        return []
    runtime = config or load_shopify_runtime_config()
    api_client = graph_ql_client or ShopifyGraphQLClient.from_environment()
    results: list[ShopifyProcessingResult] = []
    for _ in range(max_events):
        with session_factory() as claim_db:
            try:
                event_id = claim_next_shopify_event(claim_db)
                claim_db.commit()
            except Exception:
                claim_db.rollback()
                raise
        if event_id is None:
            break
        try:
            with session_factory() as process_db:
                result = process_claimed_shopify_event_in_session(
                    process_db,
                    event_id=event_id,
                    config=runtime,
                    graph_ql_client=api_client,
                )
                process_db.commit()
                results.append(result)
        except Exception as exc:
            with session_factory() as retry_db:
                mark_shopify_event_retry(retry_db, event_id=event_id, error=exc)
                retry_db.commit()
    return results


def queue_shopify_event_retry_in_session(db: Session, *, event_id: int) -> ShopifyWebhookEvent:
    query = db.query(ShopifyWebhookEvent).filter(ShopifyWebhookEvent.id == event_id)
    if db.get_bind().dialect.name == "postgresql":
        query = query.with_for_update(of=ShopifyWebhookEvent)
    event = query.one_or_none()
    if event is None:
        raise ShopifyPhase1BError("Shopify event was not found.")
    if event.processing_status == "processed":
        raise ShopifyPhase1BError("Processed Shopify events cannot be queued again.")
    if event.processing_status == "processing":
        raise ShopifyPhase1BError("Shopify event is currently being processed.")
    event.processing_status = "received"
    event.next_attempt_at = None
    event.processing_started_at = None
    event.error_code = None
    event.error_message = None
    integration = (
        db.query(ShopifyOrderIntegration)
        .filter(ShopifyOrderIntegration.source_webhook_event_id == event.id)
        .one_or_none()
    )
    if integration is not None and integration.b2c_sales_order_id is None:
        integration.import_status = "received"
        integration.review_code = None
        integration.review_message = None
    db.flush()
    return event


class ShopifyAccessTokenProvider:
    """Provide a static custom-app token or renewable client-credentials token."""

    def __init__(
        self,
        *,
        shop_domain: str,
        static_access_token: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        http_post: Callable[[str, bytes, Mapping[str, str]], bytes] | None = None,
    ) -> None:
        self.shop_domain = normalize_shop_domain(shop_domain)
        self.static_access_token = (static_access_token or "").strip() or None
        self.client_id = (client_id or "").strip() or None
        self.client_secret = (client_secret or "").strip() or None
        self.http_post = http_post or _http_post
        self._cached_token: str | None = None
        self._expires_at: datetime | None = None
        self._lock = threading.Lock()

    @classmethod
    def from_environment(cls) -> "ShopifyAccessTokenProvider":
        return cls(
            shop_domain=os.getenv("SHOPIFY_SHOP_DOMAIN", ""),
            static_access_token=os.getenv("SHOPIFY_ADMIN_ACCESS_TOKEN"),
            client_id=os.getenv("SHOPIFY_CLIENT_ID"),
            client_secret=os.getenv("SHOPIFY_CLIENT_SECRET"),
        )

    def get_token(self, *, now: datetime | None = None) -> str:
        if self.static_access_token:
            return self.static_access_token
        if not self.client_id or not self.client_secret:
            raise ShopifyConfigurationError(
                "Configure SHOPIFY_ADMIN_ACCESS_TOKEN or SHOPIFY_CLIENT_ID/SHOPIFY_CLIENT_SECRET."
            )
        current = now or datetime.now(timezone.utc)
        with self._lock:
            if self._cached_token and self._expires_at and current < self._expires_at - timedelta(minutes=5):
                return self._cached_token
            body = urllib.parse.urlencode(
                {
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "grant_type": "client_credentials",
                }
            ).encode("utf-8")
            response = self.http_post(
                f"https://{self.shop_domain}/admin/oauth/access_token",
                body,
                {"Content-Type": "application/x-www-form-urlencoded"},
            )
            try:
                payload = json.loads(response)
                token = str(payload["access_token"]).strip()
                expires_in = int(payload.get("expires_in", 0))
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ShopifyTransientError("Shopify token response is invalid.") from exc
            if not token or expires_in <= 0:
                raise ShopifyTransientError("Shopify token response is incomplete.")
            self._cached_token = token
            self._expires_at = current + timedelta(seconds=expires_in)
            return token

    def validate_configuration(self) -> None:
        has_static = self.static_access_token is not None
        has_client_id = self.client_id is not None
        has_client_secret = self.client_secret is not None
        if has_static and (has_client_id or has_client_secret):
            raise ShopifyConfigurationError(
                "Configure either SHOPIFY_ADMIN_ACCESS_TOKEN or client credentials, not both."
            )
        if has_client_id != has_client_secret:
            raise ShopifyConfigurationError(
                "SHOPIFY_CLIENT_ID and SHOPIFY_CLIENT_SECRET must be configured together."
            )
        if not has_static and not (has_client_id and has_client_secret):
            raise ShopifyConfigurationError(
                "Configure SHOPIFY_ADMIN_ACCESS_TOKEN or SHOPIFY_CLIENT_ID/SHOPIFY_CLIENT_SECRET."
            )


class ShopifyGraphQLClient:
    def __init__(
        self,
        *,
        shop_domain: str,
        api_version: str,
        token_provider: ShopifyAccessTokenProvider,
        http_post: Callable[[str, bytes, Mapping[str, str]], bytes] | None = None,
    ) -> None:
        self.shop_domain = normalize_shop_domain(shop_domain)
        self.api_version = api_version.strip()
        self.token_provider = token_provider
        self.http_post = http_post or _http_post

    @classmethod
    def from_environment(cls) -> "ShopifyGraphQLClient":
        config = load_shopify_runtime_config(require_secret=False)
        token_provider = ShopifyAccessTokenProvider.from_environment()
        token_provider.validate_configuration()
        return cls(
            shop_domain=config.shop_domain,
            api_version=config.api_version,
            token_provider=token_provider,
        )

    def execute(self, query: str, variables: Mapping[str, Any] | None = None) -> dict[str, Any]:
        request_body = json.dumps({"query": query, "variables": dict(variables or {})}).encode("utf-8")
        response = self.http_post(
            f"https://{self.shop_domain}/admin/api/{self.api_version}/graphql.json",
            request_body,
            {
                "Content-Type": "application/json",
                "X-Shopify-Access-Token": self.token_provider.get_token(),
            },
        )
        try:
            payload = json.loads(response)
        except json.JSONDecodeError as exc:
            raise ShopifyTransientError("Shopify GraphQL response is not valid JSON.") from exc
        if payload.get("errors"):
            raise ShopifyTransientError("Shopify GraphQL returned errors.")
        data = payload.get("data")
        if not isinstance(data, dict):
            raise ShopifyTransientError("Shopify GraphQL response has no data object.")
        return data

    @staticmethod
    def _shop_money(order: Mapping[str, Any], field_name: str) -> tuple[Decimal, str]:
        money_bag = order.get(field_name)
        shop_money = money_bag.get("shopMoney") if isinstance(money_bag, dict) else None
        if not isinstance(shop_money, dict):
            raise ShopifyTransientError(f"Shopify GraphQL omitted {field_name} shop money.")
        currency = str(shop_money.get("currencyCode") or "").strip().upper()
        if not currency:
            raise ShopifyTransientError(f"Shopify GraphQL omitted {field_name} currency.")
        try:
            amount = _money(shop_money.get("amount"), field_name)
        except ShopifyReviewRequired as exc:
            raise ShopifyTransientError(f"Shopify GraphQL returned invalid {field_name} money.") from exc
        assert amount is not None
        return amount, currency

    def get_order_financial_snapshot(self, order_gid: str) -> ShopifyFinancialSnapshot:
        query = """
        query OrderFinancialSnapshot($id: ID!) {
          order(id: $id) {
            id
            displayFinancialStatus
            updatedAt
            currentTotalPriceSet { shopMoney { amount currencyCode } }
            currentTotalTaxSet { shopMoney { amount currencyCode } }
            totalReceivedSet { shopMoney { amount currencyCode } }
            totalOutstandingSet { shopMoney { amount currencyCode } }
            totalRefundedSet { shopMoney { amount currencyCode } }
          }
        }
        """
        data = self.execute(query, {"id": order_gid})
        order = data.get("order")
        if not isinstance(order, dict):
            raise ShopifyTransientError("Shopify GraphQL order is not yet available.")
        returned_gid = str(order.get("id") or "").strip()
        if returned_gid != order_gid:
            raise ShopifyTransientError("Shopify GraphQL returned an unexpected order identity.")
        totals = {
            name: self._shop_money(order, field)
            for name, field in (
                ("total", "currentTotalPriceSet"),
                ("tax", "currentTotalTaxSet"),
                ("received", "totalReceivedSet"),
                ("outstanding", "totalOutstandingSet"),
                ("refunded", "totalRefundedSet"),
            )
        }
        currencies = {currency for _, currency in totals.values()}
        if len(currencies) != 1:
            raise ShopifyTransientError("Shopify GraphQL financial currencies are inconsistent.")
        updated_at = _parse_datetime(order.get("updatedAt"), required=True)
        assert updated_at is not None
        return ShopifyFinancialSnapshot(
            order_gid=returned_gid,
            financial_status=str(order.get("displayFinancialStatus") or "").strip().upper() or None,
            currency_code=currencies.pop(),
            total_amount=totals["total"][0],
            tax_amount=totals["tax"][0],
            total_received_amount=totals["received"][0],
            outstanding_amount=totals["outstanding"][0],
            refunded_amount=totals["refunded"][0],
            updated_at=updated_at,
        )

    def list_order_gids_created_since(self, since: datetime, *, max_pages: int = 100) -> list[str]:
        query = """
        query OrdersSince($query: String!, $cursor: String) {
          orders(first: 100, after: $cursor, query: $query, sortKey: CREATED_AT) {
            nodes { id }
            pageInfo { hasNextPage endCursor }
          }
        }
        """
        since_utc = since.astimezone(timezone.utc) if since.tzinfo else since.replace(tzinfo=timezone.utc)
        search = f"created_at:>={since_utc.isoformat().replace('+00:00', 'Z')}"
        cursor = None
        gids: list[str] = []
        for _ in range(max_pages):
            data = self.execute(query, {"query": search, "cursor": cursor})
            connection = data.get("orders") or {}
            nodes = connection.get("nodes") or []
            gids.extend(str(node.get("id") or "").strip() for node in nodes if node.get("id"))
            page_info = connection.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                return gids
            cursor = page_info.get("endCursor")
            if not cursor:
                raise ShopifyTransientError("Shopify pagination omitted the next cursor.")
        raise ShopifyTransientError("Shopify reconciliation exceeded the configured page limit.")


def detect_missing_shopify_orders(
    db: Session,
    *,
    client: ShopifyGraphQLClient,
    since: datetime,
) -> ShopifyReconciliationResult:
    fetched = tuple(dict.fromkeys(client.list_order_gids_created_since(since)))
    if not fetched:
        return ShopifyReconciliationResult(since, (), (), ())
    existing = {
        gid
        for (gid,) in db.query(ShopifyOrderIntegration.shopify_order_gid)
        .filter(
            ShopifyOrderIntegration.shop_domain == client.shop_domain,
            ShopifyOrderIntegration.shopify_order_gid.in_(fetched),
        )
        .all()
    }
    return ShopifyReconciliationResult(
        since=since,
        fetched_order_gids=fetched,
        existing_order_gids=tuple(gid for gid in fetched if gid in existing),
        missing_order_gids=tuple(gid for gid in fetched if gid not in existing),
    )


def _http_post(url: str, body: bytes, headers: Mapping[str, str]) -> bytes:
    request = urllib.request.Request(url, data=body, headers=dict(headers), method="POST")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.read()
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
        raise ShopifyTransientError("Shopify API request failed.") from exc
