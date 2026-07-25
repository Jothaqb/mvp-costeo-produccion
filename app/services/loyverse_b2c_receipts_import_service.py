from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from sqlalchemy.orm import Session

from app.models import B2CSalesOrder, LoyverseCustomerMapping, LoyversePaymentTypeMapping, Product


LOYVERSE_API_BASE_URL = "https://api.loyverse.com/v1.0"
LOYVERSE_REQUEST_TIMEOUT_SECONDS = 8
LOYVERSE_RECEIPTS_PAGE_LIMIT = 250
DECIMAL_QUANT = Decimal("0.0001")
ROUNDING_TOLERANCE = Decimal("0.01")
ZERO = Decimal("0")
LOYVERSE_LOCAL_TIMEZONE = ZoneInfo("America/Costa_Rica")


class LoyverseB2CReceiptPreviewError(Exception):
    pass


@dataclass(frozen=True)
class PreviewMessage:
    severity: str
    message: str


@dataclass(frozen=True)
class LoyverseReceiptLinePreview:
    line_number: int
    loyverse_line_id: str
    loyverse_variant_id: str
    loyverse_item_id: str
    sku: str
    description: str
    quantity: Decimal | None
    unit_price: Decimal | None
    gross_line_total: Decimal | None
    discount_amount: Decimal | None
    net_line_total: Decimal | None
    product_match_status: str
    product_match_label: str
    blockers: list[str]
    warnings: list[str]


@dataclass(frozen=True)
class LoyverseReceiptPreview:
    receipt_id: str
    receipt_number: str
    receipt_date: datetime | None
    created_at: datetime | None
    customer_id: str
    customer_name: str
    customer_phone: str
    payment_type_id: str
    payment_type_name: str
    receipt_kind: str
    receipt_status_label: str
    already_imported: bool
    duplicate_detection_note: str
    gross_total: Decimal
    discount_total: Decimal
    net_total: Decimal
    importable: bool
    blockers: list[str]
    warnings: list[str]
    lines: list[LoyverseReceiptLinePreview]
    raw_payload_summary: str


@dataclass(frozen=True)
class LoyverseReceiptPreviewSummary:
    receipts_read: int
    importable_count: int
    already_imported_count: int
    excluded_refund_count: int
    excluded_cancelled_count: int
    receipts_with_blockers_count: int
    receipts_with_warnings_count: int
    total_gross: Decimal
    total_net: Decimal


@dataclass(frozen=True)
class LoyverseB2CReceiptsPreviewResult:
    start_datetime: datetime
    end_datetime: datetime
    queried_with_store_id: str
    receipts: list[LoyverseReceiptPreview]
    summary: LoyverseReceiptPreviewSummary
    warnings: list[str]
    limitations: list[str]


def fetch_loyverse_receipts(start_datetime: datetime, end_datetime: datetime) -> list[dict]:
    token = _require_token()
    store_id = os.getenv("LOYVERSE_STORE_ID", "").strip()
    rows: list[dict] = []
    cursor = None

    while True:
        params = {
            "created_at_min": _format_loyverse_datetime(start_datetime),
            "created_at_max": _format_loyverse_datetime(end_datetime),
            "limit": str(LOYVERSE_RECEIPTS_PAGE_LIMIT),
        }
        if store_id:
            params["store_id"] = store_id
        if cursor:
            params["cursor"] = cursor

        payload = _loyverse_get(token, f"/receipts?{urlencode(params)}")
        collection = payload.get("receipts") or []
        if not isinstance(collection, list):
            raise LoyverseB2CReceiptPreviewError("Loyverse receipts response did not include a valid receipts list.")
        rows.extend(item for item in collection if isinstance(item, dict))
        cursor = _string_value(payload, "cursor", "next_cursor")
        if not cursor:
            return rows


def build_loyverse_b2c_receipts_preview(
    db: Session,
    start_datetime: datetime,
    end_datetime: datetime,
) -> LoyverseB2CReceiptsPreviewResult:
    receipts_payloads = fetch_loyverse_receipts(start_datetime, end_datetime)
    customer_mappings = {
        mapping.loyverse_customer_id: mapping
        for mapping in db.query(LoyverseCustomerMapping).filter(LoyverseCustomerMapping.active.is_(True)).all()
    }
    payment_type_mappings = {
        mapping.loyverse_payment_type_id: mapping
        for mapping in db.query(LoyversePaymentTypeMapping).filter(LoyversePaymentTypeMapping.active.is_(True)).all()
    }
    product_rows = db.query(Product).all()
    products_by_variant_id: dict[str, list[Product]] = {}
    products_by_item_id: dict[str, list[Product]] = {}
    products_by_sku: dict[str, list[Product]] = {}
    for product in product_rows:
        variant_id = (product.loyverse_variant_id or "").strip()
        item_id = (product.loyverse_item_id or "").strip()
        sku = (product.sku or "").strip()
        if variant_id:
            products_by_variant_id.setdefault(variant_id, []).append(product)
        if item_id:
            products_by_item_id.setdefault(item_id, []).append(product)
        if sku:
            products_by_sku.setdefault(sku, []).append(product)

    receipts: list[LoyverseReceiptPreview] = []
    limitations = [
        "Duplicate detection now checks Loyverse receipt fields when available and falls back to legacy B2C order numbers. Receipts imported before L1B.1 may only be detected through legacy fallbacks.",
        "The preview uses Loyverse created_at range filters because the current API list endpoint does not expose receipt_date range filters.",
    ]
    result_warnings: list[str] = []
    receipts_with_blockers = 0
    receipts_with_warnings = 0
    excluded_refund = 0
    excluded_cancelled = 0
    importable_count = 0
    already_imported_count = 0
    total_gross = ZERO
    total_net = ZERO

    for payload in receipts_payloads:
        normalized = _normalize_loyverse_receipt(payload)
        customer_mapping = customer_mappings.get(normalized["customer_id"])
        line_previews: list[LoyverseReceiptLinePreview] = []
        receipt_blockers: list[str] = []
        receipt_warnings: list[str] = []

        if not normalized["receipt_number"]:
            receipt_blockers.append("Receipt is missing a usable receipt_number.")

        receipt_kind = _classify_receipt_kind(normalized)
        if receipt_kind == "excluded_refund":
            receipt_blockers.append("Receipt is a refund and is excluded in L1A.")
        elif receipt_kind == "excluded_cancelled":
            receipt_blockers.append("Receipt is cancelled/voided and is excluded in L1A.")

        existing_import = _detect_existing_import(db, normalized)
        if existing_import["already_imported"]:
            already_imported_count += 1

        payment_mapping = _resolve_payment_type_mapping(
            db,
            normalized,
            payment_type_mappings=payment_type_mappings,
        )
        if payment_mapping["warning"]:
            receipt_warnings.append(payment_mapping["warning"])

        if customer_mapping is None and normalized["customer_id"]:
            receipt_warnings.append(
                f"Customer {normalized['customer_id']} is not present in local Loyverse customer mappings."
            )

        for index, line in enumerate(normalized["line_items"], start=1):
            product_match = _resolve_receipt_line_product(
                db,
                line,
                products_by_variant_id=products_by_variant_id,
                products_by_item_id=products_by_item_id,
                products_by_sku=products_by_sku,
            )
            line_blockers = list(product_match["blockers"])
            line_warnings = list(product_match["warnings"])
            if line["quantity"] is None or line["quantity"] <= ZERO:
                line_blockers.append("Line quantity is missing or invalid.")
            if line["unit_price"] is None or line["unit_price"] < ZERO:
                line_blockers.append("Line unit price is missing or invalid.")
            if not line["description"] and not line["sku"] and not line["loyverse_variant_id"]:
                line_blockers.append("Line is missing identifying fields (description / SKU / variant).")

            line_preview = LoyverseReceiptLinePreview(
                line_number=index,
                loyverse_line_id=line["line_id"],
                loyverse_variant_id=line["loyverse_variant_id"],
                loyverse_item_id=line["loyverse_item_id"],
                sku=line["sku"],
                description=line["description"],
                quantity=line["quantity"],
                unit_price=line["unit_price"],
                gross_line_total=line["gross_line_total"],
                discount_amount=line["discount_amount"],
                net_line_total=line["net_line_total"],
                product_match_status=product_match["status"],
                product_match_label=product_match["label"],
                blockers=line_blockers,
                warnings=line_warnings,
            )
            line_previews.append(line_preview)
            receipt_blockers.extend(
                f"Line {index}: {message}"
                for message in line_blockers
            )
            receipt_warnings.extend(
                f"Line {index}: {message}"
                for message in line_warnings
            )

        total_issues = _validate_receipt_totals(normalized)
        for issue in total_issues:
            if issue.severity == "blocker":
                receipt_blockers.append(issue.message)
            else:
                receipt_warnings.append(issue.message)

        if normalized["has_global_discount"]:
            receipt_warnings.append("Receipt includes a global discount; Loyverse values are used as source of truth.")

        receipt_blockers = _dedupe_messages(receipt_blockers)
        receipt_warnings = _dedupe_messages(receipt_warnings)
        if receipt_blockers:
            receipts_with_blockers += 1
        if receipt_warnings:
            receipts_with_warnings += 1
        if receipt_kind == "excluded_refund":
            excluded_refund += 1
        if receipt_kind == "excluded_cancelled":
            excluded_cancelled += 1

        importable = receipt_kind == "sale" and not receipt_blockers
        if importable:
            importable_count += 1

        receipt_preview = LoyverseReceiptPreview(
            receipt_id=normalized["receipt_id"],
            receipt_number=normalized["receipt_number"],
            receipt_date=normalized["receipt_date"],
            created_at=normalized["created_at"],
            customer_id=normalized["customer_id"],
            customer_name=(customer_mapping.customer_name if customer_mapping is not None else normalized["customer_name"]),
            customer_phone=(customer_mapping.phone if customer_mapping is not None else normalized["customer_phone"]),
            payment_type_id=normalized["payment_type_id"],
            payment_type_name=payment_mapping["label"],
            receipt_kind=receipt_kind,
            receipt_status_label=_receipt_status_label(receipt_kind, importable),
            already_imported=existing_import["already_imported"],
            duplicate_detection_note=existing_import["note"],
            gross_total=normalized["gross_total"],
            discount_total=normalized["discount_total"],
            net_total=normalized["total_money"],
            importable=importable,
            blockers=receipt_blockers,
            warnings=receipt_warnings,
            lines=line_previews,
            raw_payload_summary=_summarize_raw_payload(payload),
        )
        receipts.append(receipt_preview)

        if receipt_kind == "sale":
            total_gross = (total_gross + normalized["gross_total"]).quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP)
            total_net = (total_net + normalized["total_money"]).quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP)

    summary = LoyverseReceiptPreviewSummary(
        receipts_read=len(receipts),
        importable_count=importable_count,
        already_imported_count=already_imported_count,
        excluded_refund_count=excluded_refund,
        excluded_cancelled_count=excluded_cancelled,
        receipts_with_blockers_count=receipts_with_blockers,
        receipts_with_warnings_count=receipts_with_warnings,
        total_gross=total_gross,
        total_net=total_net,
    )
    return LoyverseB2CReceiptsPreviewResult(
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        queried_with_store_id=os.getenv("LOYVERSE_STORE_ID", "").strip(),
        receipts=receipts,
        summary=summary,
        warnings=result_warnings,
        limitations=limitations,
    )


def _normalize_loyverse_receipt(payload: dict) -> dict:
    total_money = _decimal_value(payload.get("total_money"))
    total_discount = _decimal_value(payload.get("total_discount"))
    total_tax = _decimal_value(payload.get("total_tax"))
    tip = _decimal_value(payload.get("tip"))
    surcharge = _decimal_value(payload.get("surcharge"))
    normalized_lines = []
    gross_total = ZERO

    for line_payload in payload.get("line_items") or []:
        if not isinstance(line_payload, dict):
            continue
        quantity = _decimal_value(line_payload.get("quantity"))
        unit_price = _decimal_value(line_payload.get("price"))
        gross_line_total = _decimal_value(line_payload.get("gross_total_money"))
        if gross_line_total is None and quantity is not None and unit_price is not None:
            gross_line_total = (quantity * unit_price).quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP)

        discount_amount = _decimal_value(line_payload.get("total_discount"))
        if discount_amount is None:
            discount_amount = ZERO
            for discount in line_payload.get("line_discounts") or []:
                if isinstance(discount, dict):
                    discount_amount += _decimal_value(discount.get("money_amount")) or ZERO
            discount_amount = discount_amount.quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP)

        net_line_total = _decimal_value(line_payload.get("total_money"))
        if net_line_total is None and gross_line_total is not None:
            net_line_total = (gross_line_total - (discount_amount or ZERO)).quantize(
                DECIMAL_QUANT,
                rounding=ROUND_HALF_UP,
            )

        gross_total = (gross_total + (gross_line_total or ZERO)).quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP)
        normalized_lines.append(
            {
                "line_id": _string_value(line_payload, "id"),
                "loyverse_variant_id": _string_value(line_payload, "variant_id", "variantId"),
                "loyverse_item_id": _string_value(line_payload, "item_id", "itemId"),
                "sku": _string_value(line_payload, "sku"),
                "description": _string_value(line_payload, "item_name", "name", "description"),
                "quantity": quantity,
                "unit_price": unit_price,
                "gross_line_total": gross_line_total,
                "discount_amount": discount_amount,
                "net_line_total": net_line_total,
            }
        )

    payments = payload.get("payments") or []
    payment = payments[0] if payments and isinstance(payments[0], dict) else {}
    total_discount = (total_discount or ZERO).quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP)

    return {
        "receipt_id": _string_value(payload, "id", "receipt_id"),
        "receipt_number": _string_value(payload, "receipt_number", "receiptNumber"),
        "receipt_type": _string_value(payload, "receipt_type", "receiptType").upper(),
        "refund_for": _string_value(payload, "refund_for", "refundFor"),
        "cancelled_at": _parse_datetime(payload.get("cancelled_at")),
        "receipt_date": _parse_datetime(payload.get("receipt_date")),
        "created_at": _parse_datetime(payload.get("created_at")),
        "customer_id": _string_value(payload, "customer_id", "customerId"),
        "customer_name": _string_value(payload, "customer_name", "customerName"),
        "customer_phone": _string_value(payload, "customer_phone", "customerPhone"),
        "payment_type_id": _string_value(payment, "payment_type_id", "paymentTypeId"),
        "payment_paid_at": _parse_datetime(payment.get("paid_at") if isinstance(payment, dict) else None),
        "gross_total": gross_total,
        "discount_total": total_discount,
        "total_tax": (total_tax or ZERO).quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP),
        "tip": (tip or ZERO).quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP),
        "surcharge": (surcharge or ZERO).quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP),
        "total_money": (total_money or ZERO).quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP),
        "line_items": normalized_lines,
        "has_global_discount": any(
            isinstance(discount, dict) and _string_value(discount, "scope").upper() == "RECEIPT"
            for discount in (payload.get("total_discounts") or [])
        ),
    }


def _classify_receipt_kind(receipt: dict) -> str:
    if receipt["cancelled_at"] is not None:
        return "excluded_cancelled"
    if receipt["receipt_type"] == "REFUND" or receipt["refund_for"]:
        return "excluded_refund"
    return "sale"


def _receipt_status_label(receipt_kind: str, importable: bool) -> str:
    if receipt_kind == "excluded_refund":
        return "excluded_refund"
    if receipt_kind == "excluded_cancelled":
        return "excluded_cancelled"
    if importable:
        return "importable"
    return "blocked"


def _resolve_receipt_line_product(
    db: Session,
    line: dict,
    *,
    products_by_variant_id: dict[str, list[Product]] | None = None,
    products_by_item_id: dict[str, list[Product]] | None = None,
    products_by_sku: dict[str, list[Product]] | None = None,
) -> dict:
    products_by_variant_id = products_by_variant_id or {}
    products_by_item_id = products_by_item_id or {}
    products_by_sku = products_by_sku or {}

    variant_id = (line.get("loyverse_variant_id") or "").strip()
    if variant_id:
        matches = products_by_variant_id.get(variant_id, [])
        if len(matches) == 1:
            product = matches[0]
            return {
                "status": "matched_variant_id",
                "label": f"{product.sku} - {product.name}",
                "blockers": [],
                "warnings": [],
            }
        if len(matches) > 1:
            return {
                "status": "ambiguous_variant_id",
                "label": "Multiple ERP products share this Loyverse variant mapping.",
                "blockers": ["Multiple ERP products share the same Loyverse variant mapping."],
                "warnings": [],
            }

    item_id = (line.get("loyverse_item_id") or "").strip()
    if item_id:
        matches = products_by_item_id.get(item_id, [])
        if len(matches) == 1:
            product = matches[0]
            return {
                "status": "matched_item_id",
                "label": f"{product.sku} - {product.name}",
                "blockers": [],
                "warnings": ["Matched by Loyverse item_id because variant mapping was not available."],
            }
        if len(matches) > 1:
            return {
                "status": "ambiguous_item_id",
                "label": "Multiple ERP products share this Loyverse item mapping.",
                "blockers": ["Multiple ERP products share the same Loyverse item mapping."],
                "warnings": [],
            }

    sku = (line.get("sku") or "").strip()
    if sku:
        matches = products_by_sku.get(sku, [])
        if len(matches) == 1:
            product = matches[0]
            return {
                "status": "matched_sku",
                "label": f"{product.sku} - {product.name}",
                "blockers": [],
                "warnings": ["Matched by SKU because Loyverse variant mapping was not available."],
            }
        if len(matches) > 1:
            return {
                "status": "ambiguous_sku",
                "label": "Multiple ERP products share this SKU.",
                "blockers": ["Multiple ERP products share the same SKU."],
                "warnings": [],
            }

    return {
        "status": "missing_mapping",
        "label": "No ERP product mapping found.",
        "blockers": ["No ERP product mapping found for this line."],
        "warnings": [],
    }


def _resolve_payment_type_mapping(
    db: Session,
    receipt: dict,
    *,
    payment_type_mappings: dict[str, LoyversePaymentTypeMapping] | None = None,
) -> dict:
    payment_type_mappings = payment_type_mappings or {}
    payment_type_id = (receipt.get("payment_type_id") or "").strip()
    if not payment_type_id:
        return {"label": "", "warning": "Receipt does not include a Loyverse payment type."}

    mapping = payment_type_mappings.get(payment_type_id)
    if mapping is None:
        return {
            "label": payment_type_id,
            "warning": f"Payment type {payment_type_id} is not present in local Loyverse payment type mappings.",
        }
    return {"label": mapping.name, "warning": ""}


def _detect_existing_import(db: Session, receipt: dict) -> dict:
    receipt_number = (receipt.get("receipt_number") or "").strip()
    receipt_id = _normalized_optional_loyverse_id(receipt.get("receipt_id"))
    if not receipt_number and not receipt_id:
        return {
            "already_imported": False,
            "note": "Duplicate detection is unavailable because the receipt has no usable receipt_number or receipt_id.",
        }

    checks = []
    if receipt_number:
        checks.append(("loyverse_receipt_number", B2CSalesOrder.loyverse_receipt_number == receipt_number))
    if receipt_id:
        checks.append(("loyverse_receipt_id", B2CSalesOrder.loyverse_receipt_id == receipt_id))
    if receipt_number:
        checks.extend(
            [
                ("legacy_order_number", B2CSalesOrder.order_number == receipt_number),
                ("loy_prefixed_order_number", B2CSalesOrder.order_number == f"LOY-{receipt_number}"),
            ]
        )

    for label, criterion in checks:
        existing = db.query(B2CSalesOrder.id).filter(criterion).first()
        if existing is not None:
            return {
                "already_imported": True,
                "note": f"Duplicate detected using {label}. Formal Loyverse receipt fields are checked before legacy order_number fallbacks.",
            }

    return {
        "already_imported": False,
        "note": "No existing B2C order matched Loyverse receipt fields or legacy order_number fallbacks.",
    }


def _normalized_optional_loyverse_id(value: object) -> str:
    text = str(value or "").strip()
    if not text or text.upper() == "N/A":
        return ""
    return text


def _validate_receipt_totals(receipt: dict) -> list[PreviewMessage]:
    issues: list[PreviewMessage] = []
    lines = receipt["line_items"]
    if not lines:
        issues.append(PreviewMessage("blocker", "Receipt has no line items."))
        return issues

    net_candidates = [line["net_line_total"] for line in lines if line["net_line_total"] is not None]
    if len(net_candidates) != len(lines):
        issues.append(PreviewMessage("blocker", "One or more line totals could not be calculated."))
        return issues

    lines_net_total = sum(net_candidates, ZERO).quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP)
    receipt_total = receipt["total_money"]
    tax = receipt["total_tax"]
    tip = receipt["tip"]
    surcharge = receipt["surcharge"]
    candidates = [
        ("lines_net_total", lines_net_total),
        ("lines_net_total_plus_tax", lines_net_total + tax),
        ("lines_net_total_plus_tip_surcharge", lines_net_total + tip + surcharge),
        ("lines_net_total_plus_tax_tip_surcharge", lines_net_total + tax + tip + surcharge),
    ]
    closest_label = ""
    closest_total = ZERO
    closest_diff: Decimal | None = None
    for label, candidate in candidates:
        quantized_candidate = candidate.quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP)
        diff = abs(quantized_candidate - receipt_total).quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP)
        if closest_diff is None or diff < closest_diff:
            closest_diff = diff
            closest_total = quantized_candidate
            closest_label = label

    if closest_diff is None:
        return issues
    if closest_diff == ZERO:
        return issues
    if closest_diff <= ROUNDING_TOLERANCE:
        issues.append(
            PreviewMessage(
                "warning",
                f"Receipt total differs from computed line total by {closest_diff} using {closest_label}.",
            )
        )
        return issues

    issues.append(
        PreviewMessage(
            "blocker",
            f"Receipt total {receipt_total} does not reconcile with computed line totals (closest {closest_label}={closest_total}, diff {closest_diff}).",
        )
    )
    return issues


def _summarize_raw_payload(payload: dict) -> str:
    text = json.dumps(payload, ensure_ascii=True, default=str, separators=(",", ":"))
    if len(text) > 1200:
        return f"{text[:1200]}..."
    return text


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
        raise LoyverseB2CReceiptPreviewError(
            f"Loyverse API returned HTTP {exc.code}: {_summarize_response(response_text)}"
        ) from exc
    except URLError as exc:
        raise LoyverseB2CReceiptPreviewError(f"Loyverse API request failed: {exc.reason}") from exc
    except TimeoutError as exc:
        raise LoyverseB2CReceiptPreviewError("Loyverse API request timed out.") from exc

    if not response_text:
        return {}
    try:
        return json.loads(response_text)
    except json.JSONDecodeError as exc:
        raise LoyverseB2CReceiptPreviewError("Loyverse API returned invalid JSON.") from exc


def _require_token() -> str:
    token = os.getenv("LOYVERSE_API_TOKEN", "").strip()
    if not token:
        raise LoyverseB2CReceiptPreviewError("LOYVERSE_API_TOKEN is not configured.")
    return token


def _format_loyverse_datetime(value: datetime) -> str:
    localized = value.replace(tzinfo=LOYVERSE_LOCAL_TIMEZONE) if value.tzinfo is None else value
    utc_value = localized.astimezone(timezone.utc)
    milliseconds = utc_value.microsecond // 1000
    return utc_value.strftime("%Y-%m-%dT%H:%M:%S.") + f"{milliseconds:03d}Z"


def _parse_datetime(value: object) -> datetime | None:
    if value in {None, ""}:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _decimal_value(value: object) -> Decimal | None:
    if value in {None, ""}:
        return None
    try:
        return Decimal(str(value)).quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError):
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


def _dedupe_messages(messages: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for message in messages:
        if message not in seen:
            deduped.append(message)
            seen.add(message)
    return deduped
