from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Callable

from sqlalchemy.orm import Session


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.database import SessionLocal  # noqa: E402
from app.models import (  # noqa: E402
    B2CSalesOrder,
    InventoryTransaction,
    ShopifyOrderIntegration,
    ShopifyWebhookEvent,
)
from app.services.shopify_phase1a_service import (  # noqa: E402
    resolve_online_web_channel,
    resolve_shopify_customer_match,
    shopify_integration_enabled,
)
from app.services.shopify_phase1b_service import (  # noqa: E402
    SHOPIFY_ORDERS_CREATE_TOPIC,
    ShopifyGraphQLClient,
    ShopifyPhase1BError,
    ShopifyRuntimeConfig,
    load_shopify_runtime_config,
    mark_shopify_event_retry,
    normalize_shipping_method_key,
    prepare_shopify_order,
    process_claimed_shopify_event_in_session,
)


APPROVED_ORDER_GID = "gid://shopify/Order/8198807126309"
APPROVED_ORDER_NAME = "#1679"
APPROVED_API_VERSION = "2026-10"
APPROVED_TECHNICAL_ORIGIN = "shopify_draft_order"
APPROVED_PRODUCT_SKU = "10265"
APPROVED_PRODUCT_QUANTITY = Decimal("1.0000")
APPROVED_PRODUCT_AMOUNT = Decimal("3300.0000")
APPROVED_SHIPPING_SKU = "10219"
APPROVED_SHIPPING_AMOUNT = Decimal("2500.0000")
APPROVED_SHIPPING_SOURCE = "shopify"
APPROVED_SHIPPING_CODE = "GAM menos de 20000"
APPROVED_TOTAL = Decimal("5800.0000")
APPROVED_ZERO = Decimal("0.0000")
APPROVED_FINANCIAL_STATUS = "PENDING"
CONFIRMATION_PREFIX = "PROCESS_SHOPIFY_EVENT"
READY = "READY"
BLOCKED = "BLOCKED"
ALREADY_PROCESSED = "ALREADY_PROCESSED"


class SingleShopifyEventError(Exception):
    pass


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Dry-run or process one explicitly approved Shopify webhook event without "
            "enabling the background processor."
        )
    )
    parser.add_argument("--event-id", type=int, required=True)
    parser.add_argument("--expected-order-gid", required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm", default="")
    args = parser.parse_args(argv)
    if args.event_id <= 0:
        parser.error("--event-id must be a positive integer.")
    args.expected_order_gid = args.expected_order_gid.strip()
    args.confirm = args.confirm.strip()
    if not args.expected_order_gid.startswith("gid://shopify/Order/"):
        parser.error("--expected-order-gid must be a Shopify Order GID.")
    if args.dry_run and args.confirm:
        parser.error("--confirm is not valid with --dry-run.")
    if args.execute and not args.confirm:
        parser.error("--execute requires --confirm from the immediately preceding dry-run.")
    return args


def _background_processor_is_disabled() -> bool:
    return os.getenv("SHOPIFY_BACKGROUND_PROCESSOR_ENABLED", "").strip().lower() in {
        "",
        "0",
        "false",
        "no",
        "off",
    }


def _event_query(db: Session, event_id: int, *, for_update: bool):
    query = db.query(ShopifyWebhookEvent).filter(ShopifyWebhookEvent.id == event_id)
    if for_update and db.get_bind().dialect.name == "postgresql":
        query = query.with_for_update(of=ShopifyWebhookEvent)
    return query


def _blocked_plan(event_id: int, expected_order_gid: str, reasons: list[str]) -> dict:
    return {
        "status": BLOCKED,
        "event_id": event_id,
        "expected_order_gid": expected_order_gid,
        "reasons": reasons,
        "plan_fingerprint": "",
        "confirmation": "",
    }


def _safe_decimal(value: Decimal | None) -> str | None:
    return format(value, "f") if value is not None else None


def build_single_event_plan(
    db: Session,
    *,
    event_id: int,
    expected_order_gid: str,
    config: ShopifyRuntimeConfig,
    graph_ql_client: ShopifyGraphQLClient,
    for_update: bool = False,
) -> dict:
    reasons: list[str] = []
    event = _event_query(db, event_id, for_update=for_update).one_or_none()
    if event is None:
        return _blocked_plan(event_id, expected_order_gid, ["Shopify event was not found."])
    if event.processing_status == "processed":
        integration = (
            db.query(ShopifyOrderIntegration)
            .filter(ShopifyOrderIntegration.source_webhook_event_id == event.id)
            .one_or_none()
        )
        if integration is None:
            return _blocked_plan(
                event_id,
                expected_order_gid,
                ["Processed event has no Shopify order integration."],
            )
        if expected_order_gid != APPROVED_ORDER_GID:
            return _blocked_plan(
                event_id,
                expected_order_gid,
                ["The expected Order GID is not approved for this controlled execution."],
            )
        if integration.shopify_order_gid != expected_order_gid:
            return _blocked_plan(
                event_id,
                expected_order_gid,
                ["Processed event belongs to a different Shopify Order GID."],
            )
        processed_scope_is_valid = (
            event.shop_domain == config.shop_domain
            and event.topic == SHOPIFY_ORDERS_CREATE_TOPIC
            and event.api_version == config.api_version == APPROVED_API_VERSION
            and integration.shop_domain == config.shop_domain
            and integration.import_status == "imported"
            and integration.b2c_sales_order_id is not None
        )
        if not processed_scope_is_valid:
            return _blocked_plan(
                event_id,
                expected_order_gid,
                ["Processed event or integration is inconsistent with the approved scope."],
            )
        return {
            "status": ALREADY_PROCESSED,
            "event_id": event.id,
            "expected_order_gid": expected_order_gid,
            "integration_id": integration.id,
            "b2c_sales_order_id": integration.b2c_sales_order_id,
            "reasons": ["Event was already processed; no action is required."],
            "plan_fingerprint": "",
            "confirmation": "",
        }
    if event.processing_status != "received":
        reasons.append(
            f"Event status must be received, not {event.processing_status}."
        )
    if not shopify_integration_enabled():
        reasons.append("SHOPIFY_INTEGRATION_ENABLED must be true.")
    if not _background_processor_is_disabled():
        reasons.append("SHOPIFY_BACKGROUND_PROCESSOR_ENABLED must remain false.")
    if expected_order_gid != APPROVED_ORDER_GID:
        reasons.append("The expected Order GID is not approved for this controlled execution.")
    if event.shop_domain != config.shop_domain:
        reasons.append("Stored event shop does not match SHOPIFY_SHOP_DOMAIN.")
    if event.topic != SHOPIFY_ORDERS_CREATE_TOPIC:
        reasons.append("Stored event topic is not orders/create.")
    if event.api_version != config.api_version:
        reasons.append("Stored event API version does not match SHOPIFY_API_VERSION.")
    if config.api_version != APPROVED_API_VERSION:
        reasons.append("SHOPIFY_API_VERSION must be 2026-10 for this controlled execution.")
    if not event.payload_body:
        reasons.append("Stored event has no recoverable payload.")
        return _blocked_plan(event_id, expected_order_gid, reasons)
    payload_sha256 = hashlib.sha256(event.payload_body.encode("utf-8")).hexdigest()
    if payload_sha256 != event.payload_sha256:
        reasons.append("Stored event payload hash does not match the received payload hash.")

    try:
        raw_payload = json.loads(event.payload_body)
    except json.JSONDecodeError:
        reasons.append("Stored event payload is not valid JSON.")
        return _blocked_plan(event_id, expected_order_gid, reasons)
    if not isinstance(raw_payload, dict):
        reasons.append("Stored event payload is not a JSON object.")
        return _blocked_plan(event_id, expected_order_gid, reasons)
    technical_origin = str(raw_payload.get("source_name") or "").strip()
    if technical_origin != APPROVED_TECHNICAL_ORIGIN:
        reasons.append("Stored source_name does not match the approved test order origin.")

    try:
        prepared = prepare_shopify_order(
            db,
            raw_payload=event.payload_body,
            shipping_method_map=config.shipping_method_map,
        )
    except Exception as exc:
        reasons.append(f"ERP order validation failed: {type(exc).__name__}: {exc}")
        return _blocked_plan(event_id, expected_order_gid, reasons)
    if prepared.order_gid != expected_order_gid:
        reasons.append("Stored Shopify Order GID does not match --expected-order-gid.")
    if prepared.order_name != APPROVED_ORDER_NAME:
        reasons.append("Stored Shopify order name is not the approved order #1679.")

    existing_integration = (
        db.query(ShopifyOrderIntegration)
        .filter(
            ShopifyOrderIntegration.shop_domain == event.shop_domain,
            ShopifyOrderIntegration.shopify_order_gid == prepared.order_gid,
        )
        .one_or_none()
    )
    if existing_integration is not None:
        reasons.append("A Shopify integration already exists for this Order GID.")

    try:
        channel = resolve_online_web_channel(db)
    except Exception as exc:
        reasons.append(f"Online Web channel validation failed: {exc}")
        channel = None

    customer_match = resolve_shopify_customer_match(
        db,
        shop_domain=event.shop_domain,
        shopify_customer_gid=prepared.customer_gid,
        phone=prepared.phone,
        email=prepared.email,
    )
    if customer_match.outcome in {"ambiguous", "conflict", "review_pending"}:
        reasons.append(customer_match.reason or "Shopify customer requires review.")
    customer_action = (
        "create"
        if customer_match.outcome == "no_match"
        else "reuse"
        if customer_match.customer is not None
        else "blocked"
    )

    product_lines = [line for line in prepared.lines if line.line_type == "product"]
    shipping_lines = [line for line in prepared.lines if line.line_type == "shipping"]
    if len(product_lines) != 1:
        reasons.append("Approved order must contain exactly one product line.")
    else:
        product_line = product_lines[0]
        if product_line.sku != APPROVED_PRODUCT_SKU:
            reasons.append("Approved product SKU 10265 was not found as the only product line.")
        if product_line.quantity != APPROVED_PRODUCT_QUANTITY:
            reasons.append("Approved product quantity must be 1.")
        if product_line.net_amount != APPROVED_PRODUCT_AMOUNT:
            reasons.append("Approved product net amount must be CRC 3300.")
    if len(shipping_lines) != 1:
        reasons.append("Approved order must contain exactly one shipping line.")
    else:
        shipping_line = shipping_lines[0]
        if shipping_line.sku != APPROVED_SHIPPING_SKU:
            reasons.append("Approved shipping SKU must be 10219.")
        if shipping_line.quantity != Decimal("1.0000"):
            reasons.append("Approved shipping quantity must be 1.")
        if shipping_line.net_amount != APPROVED_SHIPPING_AMOUNT:
            reasons.append("Approved shipping net amount must be CRC 2500.")
    if prepared.shipping_source != APPROVED_SHIPPING_SOURCE:
        reasons.append("Shipping source must be exactly shopify.")
    if prepared.shipping_code != APPROVED_SHIPPING_CODE:
        reasons.append("Shipping code must be exactly GAM menos de 20000.")
    approved_shipping_key = normalize_shipping_method_key(
        APPROVED_SHIPPING_SOURCE,
        APPROVED_SHIPPING_CODE,
    )
    if config.shipping_method_map.get(approved_shipping_key) != APPROVED_SHIPPING_SKU:
        reasons.append("SHOPIFY_SHIPPING_METHOD_MAP does not map the approved GAM method to 10219.")
    total_discount = sum((line.discount_amount for line in prepared.lines), APPROVED_ZERO)
    if total_discount != APPROVED_ZERO:
        reasons.append("Approved order discount must be zero.")
    if prepared.currency_code != "CRC":
        reasons.append("Approved order currency must be CRC.")
    if prepared.tax_amount != APPROVED_ZERO:
        reasons.append("Approved order tax must be zero.")
    if prepared.total_amount != APPROVED_TOTAL:
        reasons.append("Approved order total must be CRC 5800.")
    if prepared.financial_status != APPROVED_FINANCIAL_STATUS:
        reasons.append("Approved webhook financial status must be PENDING.")

    try:
        financial = graph_ql_client.get_order_financial_snapshot(prepared.order_gid)
    except Exception as exc:
        reasons.append(f"Shopify GraphQL validation failed: {type(exc).__name__}: {exc}")
        financial = None
    if financial is not None:
        if financial.order_gid != prepared.order_gid:
            reasons.append("Shopify GraphQL returned a different Order GID.")
        if financial.currency_code != "CRC":
            reasons.append("Shopify GraphQL currency must be CRC.")
        if financial.total_amount != APPROVED_TOTAL:
            reasons.append("Shopify GraphQL total must be CRC 5800.")
        if financial.tax_amount != APPROVED_ZERO:
            reasons.append("Shopify GraphQL tax must be zero.")
        if financial.financial_status != APPROVED_FINANCIAL_STATUS:
            reasons.append("Shopify GraphQL financial status must be PENDING.")
        if financial.total_received_amount != APPROVED_ZERO:
            reasons.append("Shopify GraphQL received amount must be zero.")
        if financial.outstanding_amount != APPROVED_TOTAL:
            reasons.append("Shopify GraphQL outstanding amount must be CRC 5800.")
        if financial.refunded_amount != APPROVED_ZERO:
            reasons.append("Shopify GraphQL refunded amount must be zero.")

    if reasons:
        return _blocked_plan(event_id, expected_order_gid, reasons)

    canonical = {
        "schema": "green-corner-shopify-single-event-v1",
        "event_id": event.id,
        "webhook_id": event.webhook_id,
        "payload_sha256": event.payload_sha256,
        "shop_domain": event.shop_domain,
        "topic": event.topic,
        "api_version": event.api_version,
        "order_gid": prepared.order_gid,
        "order_name": prepared.order_name,
        "technical_origin": technical_origin,
        "channel_id": channel.id if channel else None,
        "customer_action": customer_action,
        "existing_customer_id": customer_match.customer.id if customer_match.customer else None,
        "lines": [
            {
                "sku": line.sku,
                "line_type": line.line_type,
                "quantity": _safe_decimal(line.quantity),
                "net_amount": _safe_decimal(line.net_amount),
            }
            for line in prepared.lines
        ],
        "discount": _safe_decimal(total_discount),
        "total": _safe_decimal(prepared.total_amount),
        "financial_status": financial.financial_status,
        "financial_total": _safe_decimal(financial.total_amount),
        "financial_outstanding": _safe_decimal(financial.outstanding_amount),
    }
    serialized = json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    fingerprint = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
    confirmation = f"{CONFIRMATION_PREFIX}_{event.id}_{fingerprint}"
    return {
        "status": READY,
        "event_id": event.id,
        "expected_order_gid": expected_order_gid,
        "order_name": prepared.order_name,
        "technical_origin": technical_origin,
        "channel": channel.name if channel else None,
        "customer_action": customer_action,
        "existing_customer_id": customer_match.customer.id if customer_match.customer else None,
        "line_count": len(prepared.lines),
        "product_sku": product_lines[0].sku,
        "shipping_sku": shipping_lines[0].sku,
        "discount": _safe_decimal(total_discount),
        "total": _safe_decimal(prepared.total_amount),
        "financial_status": financial.financial_status,
        "plan_fingerprint": fingerprint,
        "confirmation": confirmation,
        "reasons": [],
    }


def _validate_created_order(
    db: Session,
    *,
    event_id: int,
    integration_id: int | None,
    order_id: int | None,
) -> B2CSalesOrder:
    if order_id is None:
        raise SingleShopifyEventError("Processing did not create or return a B2C order.")
    order = db.get(B2CSalesOrder, order_id)
    if order is None:
        raise SingleShopifyEventError("Processed B2C order was not found.")
    if order.status != "draft" or order.channel != "Online Web":
        raise SingleShopifyEventError("Processed order is not an Online Web B2C draft.")
    if order.loyverse_receipt_id is not None or order.loyverse_receipt_status is not None:
        raise SingleShopifyEventError("Processed draft unexpectedly contains invoice evidence.")
    if order.total_amount != APPROVED_TOTAL or order.discount_amount != APPROVED_ZERO:
        raise SingleShopifyEventError("Processed order totals do not match the approved plan.")
    if len(order.lines) != 2:
        raise SingleShopifyEventError("Processed order does not contain exactly two lines.")
    lines = {line.sku_snapshot: line for line in order.lines}
    if set(lines) != {APPROVED_PRODUCT_SKU, APPROVED_SHIPPING_SKU}:
        raise SingleShopifyEventError("Processed order lines do not match the approved SKUs.")
    if lines[APPROVED_PRODUCT_SKU].line_type != "product":
        raise SingleShopifyEventError("SKU 10265 is not stored as a product line.")
    if lines[APPROVED_SHIPPING_SKU].line_type != "shipping":
        raise SingleShopifyEventError("SKU 10219 is not stored as a shipping line.")
    integration = db.get(ShopifyOrderIntegration, integration_id) if integration_id else None
    if integration is None:
        raise SingleShopifyEventError("Processed Shopify integration was not found.")
    if (
        integration.source_webhook_event_id != event_id
        or integration.shopify_order_gid != APPROVED_ORDER_GID
        or integration.b2c_sales_order_id != order.id
    ):
        raise SingleShopifyEventError("Processed Shopify integration identity changed.")
    expected_financial_values = {
        "financial_status": APPROVED_FINANCIAL_STATUS,
        "currency_code": "CRC",
        "total_received_amount": APPROVED_ZERO,
        "outstanding_amount": APPROVED_TOTAL,
        "refunded_amount": APPROVED_ZERO,
        "product_net_amount": APPROVED_PRODUCT_AMOUNT,
        "shipping_gross_amount": APPROVED_SHIPPING_AMOUNT,
        "shipping_discount_amount": APPROVED_ZERO,
        "shipping_net_amount": APPROVED_SHIPPING_AMOUNT,
        "tax_amount": APPROVED_ZERO,
        "shopify_total_amount": APPROVED_TOTAL,
        "delivery_method_type": "shipping",
        "shipping_source": APPROVED_SHIPPING_SOURCE,
        "shipping_code": APPROVED_SHIPPING_CODE,
    }
    for field_name, expected_value in expected_financial_values.items():
        if getattr(integration, field_name) != expected_value:
            raise SingleShopifyEventError(
                f"Processed Shopify integration changed approved field {field_name}."
            )
    inventory_count = (
        db.query(InventoryTransaction)
        .filter(
            InventoryTransaction.source_type == "b2c_order",
            InventoryTransaction.source_id == order.id,
        )
        .count()
    )
    if inventory_count:
        raise SingleShopifyEventError("Processing unexpectedly created inventory transactions.")
    return order


def dry_run_single_event(
    session_factory: Callable[[], Session],
    *,
    event_id: int,
    expected_order_gid: str,
    config: ShopifyRuntimeConfig,
    graph_ql_client: ShopifyGraphQLClient,
) -> dict:
    with session_factory() as db:
        try:
            return build_single_event_plan(
                db,
                event_id=event_id,
                expected_order_gid=expected_order_gid,
                config=config,
                graph_ql_client=graph_ql_client,
            )
        finally:
            db.rollback()


def execute_single_event(
    session_factory: Callable[[], Session],
    *,
    event_id: int,
    expected_order_gid: str,
    confirmation: str,
    config: ShopifyRuntimeConfig,
    graph_ql_client: ShopifyGraphQLClient,
) -> dict:
    processing_started = False
    attempted_count = 0
    try:
        with session_factory() as db:
            with db.begin():
                plan = build_single_event_plan(
                    db,
                    event_id=event_id,
                    expected_order_gid=expected_order_gid,
                    config=config,
                    graph_ql_client=graph_ql_client,
                    for_update=True,
                )
                if plan["status"] == ALREADY_PROCESSED:
                    return plan
                if plan["status"] != READY:
                    raise SingleShopifyEventError("Dry-run revalidation is BLOCKED: " + " | ".join(plan["reasons"]))
                if confirmation != plan["confirmation"]:
                    raise SingleShopifyEventError(
                        "Confirmation does not match the current dry-run fingerprint."
                    )
                event = db.get(ShopifyWebhookEvent, event_id)
                if event is None or event.processing_status != "received":
                    raise SingleShopifyEventError("Event is no longer available for processing.")
                event.processing_status = "processing"
                event.processing_started_at = datetime.utcnow()
                event.attempt_count = int(event.attempt_count or 0) + 1
                attempted_count = event.attempt_count
                event.next_attempt_at = None
                db.flush()
                processing_started = True
                result = process_claimed_shopify_event_in_session(
                    db,
                    event_id=event_id,
                    config=config,
                    graph_ql_client=graph_ql_client,
                )
                if result.status != "processed":
                    raise SingleShopifyEventError(
                        f"Processing finished as {result.status}: {result.message or 'review required'}."
                    )
                order = _validate_created_order(
                    db,
                    event_id=event_id,
                    integration_id=result.order_integration_id,
                    order_id=result.b2c_sales_order_id,
                )
                return {
                    "status": "PROCESSED",
                    "event_id": event_id,
                    "order_gid": expected_order_gid,
                    "integration_id": result.order_integration_id,
                    "b2c_sales_order_id": order.id,
                    "b2c_order_number": order.order_number,
                    "channel": order.channel,
                    "line_count": len(order.lines),
                    "total": _safe_decimal(order.total_amount),
                }
    except SingleShopifyEventError:
        raise
    except Exception as exc:
        if processing_started:
            try:
                with session_factory() as retry_db:
                    event = _event_query(retry_db, event_id, for_update=True).one_or_none()
                    if event is not None and event.processing_status != "processed":
                        event.attempt_count = max(int(event.attempt_count or 0), attempted_count)
                        mark_shopify_event_retry(retry_db, event_id=event_id, error=exc)
                        retry_db.commit()
            except Exception:
                pass
        raise


def _print_result(payload: dict) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True))


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        config = load_shopify_runtime_config()
        graph_ql_client = ShopifyGraphQLClient.from_environment()
        if args.dry_run:
            result = dry_run_single_event(
                SessionLocal,
                event_id=args.event_id,
                expected_order_gid=args.expected_order_gid,
                config=config,
                graph_ql_client=graph_ql_client,
            )
        else:
            result = execute_single_event(
                SessionLocal,
                event_id=args.event_id,
                expected_order_gid=args.expected_order_gid,
                confirmation=args.confirm,
                config=config,
                graph_ql_client=graph_ql_client,
            )
        _print_result(result)
        return 0 if result["status"] in {READY, "PROCESSED", ALREADY_PROCESSED} else 2
    except (SingleShopifyEventError, ShopifyPhase1BError) as exc:
        _print_result({"status": BLOCKED, "event_id": args.event_id, "reasons": [str(exc)]})
        return 2
    except Exception as exc:
        _print_result(
            {
                "status": "ERROR",
                "event_id": args.event_id,
                "error_type": type(exc).__name__,
                "message": "Unexpected processing failure; inspect protected application logs.",
            }
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
