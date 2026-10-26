from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.models import (
    B2CCustomer,
    Channel,
    Product,
    ShopifyCustomerMapping,
    ShopifyWebhookEvent,
)


ONLINE_WEB_CHANNEL_NAME = "Online Web"
SHOPIFY_INTEGRATION_ENABLED_ENV = "SHOPIFY_INTEGRATION_ENABLED"
SHOPIFY_PHASE1A_ALLOWED_SHIPPING_SKUS = frozenset({"10219", "10258"})


class ShopifyPhase1AError(ValueError):
    pass


@dataclass(frozen=True)
class ShopifyCustomerMatch:
    outcome: str
    customer: B2CCustomer | None
    normalized_phone: str | None
    normalized_email: str | None
    reason: str | None = None


def shopify_integration_enabled() -> bool:
    """Return the feature flag only; Phase 1A exposes no webhook endpoint."""
    return os.getenv(SHOPIFY_INTEGRATION_ENABLED_ENV, "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def normalize_shopify_phone(value: str | None) -> str | None:
    raw = (value or "").strip()
    if not raw:
        return None
    digits = re.sub(r"\D", "", raw)
    if digits.startswith("00"):
        digits = digits[2:]
    if len(digits) == 8:
        return f"+506{digits}"
    if len(digits) == 11 and digits.startswith("506"):
        return f"+{digits}"
    if raw.startswith("+") and 8 <= len(digits) <= 15:
        return f"+{digits}"
    if 8 <= len(digits) <= 15:
        return digits
    return None


def normalize_shopify_email(value: str | None) -> str | None:
    normalized = (value or "").strip().lower()
    if not normalized or normalized.count("@") != 1:
        return None
    local, domain = normalized.split("@", 1)
    if not local or "." not in domain or domain.startswith(".") or domain.endswith("."):
        return None
    return normalized


def has_sufficient_shopify_customer_identity(
    *,
    name: str | None,
    phone: str | None,
    email: str | None,
) -> bool:
    return bool((name or "").strip()) and bool(
        normalize_shopify_phone(phone) or normalize_shopify_email(email)
    )


def resolve_shopify_customer_match(
    db: Session,
    *,
    shop_domain: str,
    shopify_customer_gid: str | None,
    phone: str | None,
    email: str | None,
) -> ShopifyCustomerMatch:
    normalized_domain = (shop_domain or "").strip().lower()
    normalized_gid = (shopify_customer_gid or "").strip() or None
    normalized_phone = normalize_shopify_phone(phone)
    normalized_email = normalize_shopify_email(email)
    lock_shopify_customer_identity(
        db,
        shop_domain=normalized_domain,
        shopify_customer_gid=normalized_gid,
        phone=normalized_phone,
        email=normalized_email,
    )

    customers = db.query(B2CCustomer).order_by(B2CCustomer.id).all()
    phone_matches = tuple(
        customer
        for customer in customers
        if normalized_phone is not None and normalize_shopify_phone(customer.phone) == normalized_phone
    )
    email_matches = tuple(
        customer
        for customer in customers
        if normalized_email is not None and normalize_shopify_email(customer.email) == normalized_email
    )

    mapping = None
    if normalized_domain and normalized_gid:
        mapping = (
            db.query(ShopifyCustomerMapping)
            .filter(
                ShopifyCustomerMapping.shop_domain == normalized_domain,
                ShopifyCustomerMapping.shopify_customer_gid == normalized_gid,
            )
            .one_or_none()
        )
    if mapping is not None:
        customer = db.get(B2CCustomer, mapping.b2c_customer_id)
        if customer is None:
            return ShopifyCustomerMatch(
                "conflict",
                None,
                normalized_phone,
                normalized_email,
                "The Shopify customer mapping points to a missing B2C customer.",
            )
        contradictory_ids = {
            candidate.id
            for candidate in (*phone_matches, *email_matches)
            if candidate.id != customer.id
        }
        if contradictory_ids:
            return ShopifyCustomerMatch(
                "conflict",
                None,
                normalized_phone,
                normalized_email,
                "Shopify mapping conflicts with current phone or email matches.",
            )
        if not customer.active:
            return ShopifyCustomerMatch(
                "review_pending",
                None,
                normalized_phone,
                normalized_email,
                "The mapped B2C customer is inactive.",
            )
        return ShopifyCustomerMatch("matched_mapping", customer, normalized_phone, normalized_email)

    if normalized_phone is not None:
        if len(phone_matches) > 1:
            return ShopifyCustomerMatch(
                "ambiguous",
                None,
                normalized_phone,
                normalized_email,
                "Multiple B2C customers share the normalized phone.",
            )
        if len(phone_matches) == 1:
            customer = phone_matches[0]
            if email_matches and any(candidate.id != customer.id for candidate in email_matches):
                return ShopifyCustomerMatch(
                    "conflict",
                    None,
                    normalized_phone,
                    normalized_email,
                    "Phone and email identify different B2C customers.",
                )
            if len(email_matches) > 1:
                return ShopifyCustomerMatch(
                    "ambiguous",
                    None,
                    normalized_phone,
                    normalized_email,
                    "The optional email is shared by multiple B2C customers.",
                )
            if not customer.active:
                return ShopifyCustomerMatch(
                    "review_pending",
                    None,
                    normalized_phone,
                    normalized_email,
                    "The matching B2C customer is inactive.",
                )
            return ShopifyCustomerMatch("matched_phone", customer, normalized_phone, normalized_email)

    if normalized_email is not None:
        if len(email_matches) > 1:
            return ShopifyCustomerMatch(
                "ambiguous",
                None,
                normalized_phone,
                normalized_email,
                "Multiple B2C customers share the normalized email.",
            )
        if len(email_matches) == 1:
            customer = email_matches[0]
            if not customer.active:
                return ShopifyCustomerMatch(
                    "review_pending",
                    None,
                    normalized_phone,
                    normalized_email,
                    "The matching B2C customer is inactive.",
                )
            return ShopifyCustomerMatch("matched_email", customer, normalized_phone, normalized_email)

    return ShopifyCustomerMatch("no_match", None, normalized_phone, normalized_email)


def lock_shopify_customer_identity(
    db: Session,
    *,
    shop_domain: str,
    shopify_customer_gid: str | None,
    phone: str | None,
    email: str | None,
) -> None:
    """Serialize a future PostgreSQL match/create transaction for one buyer identity.

    The lock is transaction-scoped and does not impose unsafe global uniqueness on phone
    or email. SQLite remains a no-op because Phase 1A uses it only for local tests.
    """
    bind = db.get_bind()
    if bind.dialect.name != "postgresql":
        return
    identity = "|".join(
        (
            (shop_domain or "").strip().lower(),
            (shopify_customer_gid or "").strip(),
            normalize_shopify_phone(phone) or "",
            normalize_shopify_email(email) or "",
        )
    )
    digest = hashlib.sha256(identity.encode("utf-8")).digest()
    lock_key = int.from_bytes(digest[:8], byteorder="big", signed=False) & 0x7FFF_FFFF_FFFF_FFFF
    db.execute(text("SELECT pg_advisory_xact_lock(:lock_key)"), {"lock_key": lock_key})


def add_shopify_customer_mapping_in_session(
    db: Session,
    *,
    shop_domain: str,
    shopify_customer_gid: str,
    customer: B2CCustomer,
    phone: str | None,
    email: str | None,
) -> ShopifyCustomerMapping:
    normalized_domain = (shop_domain or "").strip().lower()
    normalized_gid = (shopify_customer_gid or "").strip()
    if not normalized_domain or not normalized_gid:
        raise ShopifyPhase1AError("Shop domain and Shopify Customer GID are required.")
    mapping = ShopifyCustomerMapping(
        shop_domain=normalized_domain,
        shopify_customer_gid=normalized_gid,
        b2c_customer_id=customer.id,
        phone_snapshot=normalize_shopify_phone(phone),
        email_snapshot=normalize_shopify_email(email),
    )
    db.add(mapping)
    db.flush()
    return mapping


def register_shopify_webhook_event_in_session(
    db: Session,
    *,
    shop_domain: str,
    webhook_id: str,
    topic: str,
    raw_body: bytes,
) -> tuple[ShopifyWebhookEvent, bool]:
    normalized_domain = (shop_domain or "").strip().lower()
    normalized_webhook_id = (webhook_id or "").strip()
    normalized_topic = (topic or "").strip().lower()
    if not normalized_domain or not normalized_webhook_id or not normalized_topic:
        raise ShopifyPhase1AError("Shop domain, webhook ID, and topic are required.")
    payload_sha256 = hashlib.sha256(raw_body).hexdigest()
    existing = (
        db.query(ShopifyWebhookEvent)
        .filter(
            ShopifyWebhookEvent.shop_domain == normalized_domain,
            ShopifyWebhookEvent.webhook_id == normalized_webhook_id,
        )
        .one_or_none()
    )
    if existing is not None:
        if existing.payload_sha256 != payload_sha256 or existing.topic != normalized_topic:
            raise ShopifyPhase1AError("A repeated webhook ID has different content.")
        return existing, False
    event = ShopifyWebhookEvent(
        shop_domain=normalized_domain,
        webhook_id=normalized_webhook_id,
        topic=normalized_topic,
        payload_sha256=payload_sha256,
        processing_status="received",
        received_at=datetime.utcnow(),
    )
    db.add(event)
    db.flush()
    return event, True


def resolve_online_web_channel(db: Session) -> Channel:
    channels = db.query(Channel).filter(Channel.name == ONLINE_WEB_CHANNEL_NAME).all()
    if len(channels) != 1:
        raise ShopifyPhase1AError("Online Web channel is missing or ambiguous.")
    channel = channels[0]
    if not channel.active or not channel.applies_to_b2c:
        raise ShopifyPhase1AError("Online Web channel must be active and enabled for B2C.")
    return channel


def resolve_shopify_product(db: Session, sku: str) -> Product:
    normalized_sku = (sku or "").strip()
    if not normalized_sku:
        raise ShopifyPhase1AError("Shopify product line is missing SKU.")
    product = db.query(Product).filter(Product.sku == normalized_sku).one_or_none()
    if product is None:
        raise ShopifyPhase1AError(f"Shopify SKU {normalized_sku} does not exist in Product Master.")
    if not product.active or not product.available_for_sale_gc:
        raise ShopifyPhase1AError(f"Shopify SKU {normalized_sku} is not active for Green Corner sales.")
    return product
