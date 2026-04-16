import json
import os
from datetime import datetime
from decimal import Decimal
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from sqlalchemy.orm import Session, joinedload

from app.models import (
    B2BSalesOrder,
    B2BSalesOrderLine,
    LoyverseCustomerMapping,
    LoyversePaymentTypeMapping,
    LoyverseVariantMapping,
)
from app.services.b2b_sales_service import ALLOWED_STATUS_TRANSITIONS, B2BValidationError, ZERO


LOYVERSE_API_BASE_URL = "https://api.loyverse.com/v1.0"
LOYVERSE_REQUEST_TIMEOUT_SECONDS = 8
SYNC_STATUS_SUCCESS = "success"
SYNC_STATUS_FAILED = "failed"
SYNC_STATUS_UNKNOWN = "unknown"


class B2BLoyverseInvoiceError(B2BValidationError):
    pass


class LoyverseReceiptUnknownError(B2BLoyverseInvoiceError):
    pass


def invoice_b2b_order_in_loyverse(db: Session, order_id: int) -> B2BSalesOrder:
    order = (
        db.query(B2BSalesOrder)
        .options(joinedload(B2BSalesOrder.customer), joinedload(B2BSalesOrder.lines))
        .filter(B2BSalesOrder.id == order_id)
        .one()
    )
    if order.status == "invoiced":
        raise B2BLoyverseInvoiceError("Order is already invoiced.")
    if "invoiced" not in ALLOWED_STATUS_TRANSITIONS[order.status]:
        raise B2BLoyverseInvoiceError(f"Cannot change status from {order.status} to invoiced.")
    if (order.loyverse_receipt_id or "").strip() or (order.loyverse_receipt_number or "").strip():
        raise B2BLoyverseInvoiceError("Order already has a Loyverse receipt reference. No duplicate receipt was created.")
    if order.loyverse_invoice_sync_status == SYNC_STATUS_UNKNOWN:
        raise B2BLoyverseInvoiceError("Previous Loyverse receipt attempt is unknown. Manually verify Loyverse before retrying.")

    attempted_at = datetime.utcnow()
    order.loyverse_invoice_sync_attempted_at = attempted_at
    order.loyverse_invoice_sync_attempt_count = (order.loyverse_invoice_sync_attempt_count or 0) + 1

    try:
        token = _require_token()
        store_id = _require_store_id()
        customer_id = _resolve_customer_id(db, order)
        payment_type_id = _resolve_payment_type_id(db, order)
        line_payloads, variant_snapshots = _build_line_payloads(db, order)
        payment_amount = _recalculate_order_total(order)
        if payment_amount <= ZERO:
            raise B2BLoyverseInvoiceError("Order total must be greater than 0 to create a Loyverse receipt.")

        payload = _build_receipt_payload(order, store_id, customer_id, line_payloads, payment_type_id, payment_amount)
        response = _create_loyverse_receipt(token, payload)
        receipt = _extract_receipt(response)
        receipt_id = _string_value(receipt, "receipt_id", "receiptId", "id")
        receipt_number = _string_value(receipt, "receipt_number", "receiptNumber", "number")
        if not receipt_id and not receipt_number:
            raise LoyverseReceiptUnknownError(
                "Loyverse receipt request returned success, but no receipt id or receipt number could be extracted. "
                f"Manually verify Loyverse before retrying. Response summary: {_summarize_payload(response)}"
            )

        order.loyverse_receipt_id = receipt_id or None
        order.loyverse_receipt_number = receipt_number or None
        order.loyverse_invoice_sync_status = SYNC_STATUS_SUCCESS
        order.loyverse_invoice_sync_error = None
        order.loyverse_invoice_synced_at = datetime.utcnow()
        order.total_amount = payment_amount
        order.status = "invoiced"
        for line_id, variant_id in variant_snapshots.items():
            line = next((item for item in order.lines if item.id == line_id), None)
            if line is not None:
                line.loyverse_variant_id_snapshot = variant_id
        db.commit()
        db.refresh(order)
        return order
    except LoyverseReceiptUnknownError as exc:
        order.loyverse_invoice_sync_status = SYNC_STATUS_UNKNOWN
        order.loyverse_invoice_sync_error = str(exc)
        db.commit()
        raise
    except B2BLoyverseInvoiceError as exc:
        order.loyverse_invoice_sync_status = SYNC_STATUS_FAILED
        order.loyverse_invoice_sync_error = str(exc)
        db.commit()
        raise
    except Exception as exc:
        order.loyverse_invoice_sync_status = SYNC_STATUS_FAILED
        order.loyverse_invoice_sync_error = f"Unexpected Loyverse receipt failure: {exc}"
        db.commit()
        raise B2BLoyverseInvoiceError(order.loyverse_invoice_sync_error) from exc


def _require_token() -> str:
    token = os.getenv("LOYVERSE_API_TOKEN", "").strip()
    if not token:
        raise B2BLoyverseInvoiceError("LOYVERSE_API_TOKEN is not configured.")
    return token


def _require_store_id() -> str:
    store_id = os.getenv("LOYVERSE_STORE_ID", "").strip()
    if not store_id:
        raise B2BLoyverseInvoiceError("LOYVERSE_STORE_ID is not configured.")
    return store_id


def _resolve_customer_id(db: Session, order: B2BSalesOrder) -> str:
    snapshot_id = (order.loyverse_customer_id_snapshot or "").strip()
    if snapshot_id:
        return snapshot_id
    if order.customer is not None:
        master_id = (order.customer.loyverse_customer_id or "").strip()
        if master_id:
            return master_id

    phone = (order.phone_snapshot or "").strip()
    if not phone:
        raise B2BLoyverseInvoiceError("No Loyverse customer id or customer phone is available.")
    normalized_phone = _normalize_phone(phone)
    candidates = [
        mapping
        for mapping in db.query(LoyverseCustomerMapping).filter(LoyverseCustomerMapping.active.is_(True)).all()
        if _normalize_phone(mapping.phone or "") == normalized_phone
    ]
    if len(candidates) == 1:
        return candidates[0].loyverse_customer_id
    if not candidates:
        raise B2BLoyverseInvoiceError(f"No local Loyverse customer mapping found for phone {phone}.")
    raise B2BLoyverseInvoiceError(f"Multiple local Loyverse customer mappings found for phone {phone}.")


def _resolve_payment_type_id(db: Session, order: B2BSalesOrder) -> str:
    payment_type_id = (order.loyverse_payment_type_id_snapshot or "").strip()
    if not payment_type_id:
        raise B2BLoyverseInvoiceError("B2B Channel is not selected for this order.")
    mapping = (
        db.query(LoyversePaymentTypeMapping)
        .filter(
            LoyversePaymentTypeMapping.active.is_(True),
            LoyversePaymentTypeMapping.loyverse_payment_type_id == payment_type_id,
        )
        .one_or_none()
    )
    if mapping is None:
        raise B2BLoyverseInvoiceError("Selected B2B Channel is not active in the local Loyverse payment type cache.")
    return payment_type_id


def _build_line_payloads(db: Session, order: B2BSalesOrder) -> tuple[list[dict], dict[int, str]]:
    lines = sorted(order.lines, key=lambda line: line.line_number)
    if not lines:
        raise B2BLoyverseInvoiceError("Order must have at least one line to create a Loyverse receipt.")

    payloads = []
    variant_snapshots = {}
    for line in lines:
        variant_id = _resolve_variant_id(db, line)
        quantity = line.quantity or ZERO
        price = line.unit_price_snapshot or ZERO
        if quantity <= ZERO:
            raise B2BLoyverseInvoiceError(f"Line {line.line_number} quantity must be greater than 0.")
        if price < ZERO:
            raise B2BLoyverseInvoiceError(f"Line {line.line_number} price cannot be negative.")
        payloads.append(
            {
                "variant_id": variant_id,
                "quantity": float(quantity),
                "price": float(price),
            }
        )
        variant_snapshots[line.id] = variant_id
    return payloads, variant_snapshots


def _resolve_variant_id(db: Session, line: B2BSalesOrderLine) -> str:
    existing_variant_id = (line.loyverse_variant_id_snapshot or "").strip()
    if existing_variant_id:
        mapping = (
            db.query(LoyverseVariantMapping)
            .filter(
                LoyverseVariantMapping.active.is_(True),
                LoyverseVariantMapping.loyverse_variant_id == existing_variant_id,
            )
            .one_or_none()
        )
        if mapping is not None:
            return existing_variant_id

    mappings = (
        db.query(LoyverseVariantMapping)
        .filter(LoyverseVariantMapping.active.is_(True), LoyverseVariantMapping.sku == line.sku_snapshot)
        .all()
    )
    if len(mappings) == 1:
        return mappings[0].loyverse_variant_id
    if not mappings:
        raise B2BLoyverseInvoiceError(f"No active local Loyverse variant mapping found for SKU {line.sku_snapshot}.")
    raise B2BLoyverseInvoiceError(f"Multiple active local Loyverse variant mappings found for SKU {line.sku_snapshot}.")


def _recalculate_order_total(order: B2BSalesOrder) -> Decimal:
    return sum((line.line_total or ZERO for line in order.lines), ZERO)


def _build_receipt_payload(
    order: B2BSalesOrder,
    store_id: str,
    customer_id: str,
    line_payloads: list[dict],
    payment_type_id: str,
    payment_amount: Decimal,
) -> dict:
    return {
        "store_id": store_id,
        "customer_id": customer_id,
        "line_items": line_payloads,
        "payments": [
            {
                "payment_type_id": payment_type_id,
                "money_amount": float(payment_amount),
            }
        ],
    }


def _create_loyverse_receipt(token: str, payload: dict) -> dict:
    return _loyverse_request(token, "POST", "/receipts", payload)


def _extract_receipt(response: dict) -> dict:
    if not isinstance(response, dict):
        return {}

    direct_receipt_reference = _string_value(
        response,
        "receipt_id",
        "receiptId",
        "id",
        "receipt_number",
        "receiptNumber",
        "number",
    )
    if direct_receipt_reference:
        return response

    nested_receipt = response.get("receipt")
    if isinstance(nested_receipt, dict):
        return nested_receipt

    receipts = response.get("receipts")
    if isinstance(receipts, list) and len(receipts) == 1 and isinstance(receipts[0], dict):
        return receipts[0]

    return {}


def _summarize_payload(payload: dict) -> str:
    try:
        return _summarize_response(json.dumps(payload, ensure_ascii=True, default=str))
    except TypeError:
        return _summarize_response(str(payload))


def _loyverse_request(token: str, method: str, path: str, body: dict) -> dict:
    data = json.dumps(body).encode("utf-8")
    request = Request(
        f"{LOYVERSE_API_BASE_URL}{path}",
        data=data,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        method=method,
    )
    try:
        with urlopen(request, timeout=LOYVERSE_REQUEST_TIMEOUT_SECONDS) as response:
            response_text = response.read().decode("utf-8")
    except HTTPError as exc:
        response_text = exc.read().decode("utf-8", errors="replace")
        raise B2BLoyverseInvoiceError(f"Loyverse API returned HTTP {exc.code}: {_summarize_response(response_text)}") from exc
    except TimeoutError as exc:
        raise LoyverseReceiptUnknownError("Loyverse receipt request timed out. Manually verify Loyverse before retrying.") from exc
    except URLError as exc:
        raise LoyverseReceiptUnknownError(f"Loyverse receipt request result is unknown: {exc.reason}") from exc

    if not response_text:
        return {}
    try:
        return json.loads(response_text)
    except json.JSONDecodeError as exc:
        raise LoyverseReceiptUnknownError("Loyverse receipt response was not valid JSON. Manually verify Loyverse before retrying.") from exc


def _string_value(payload: dict, *keys: str) -> str:
    for key in keys:
        value = payload.get(key)
        if value is not None:
            return str(value).strip()
    return ""


def _normalize_phone(value: str) -> str:
    return "".join(character for character in value if character.isdigit())


def _summarize_response(response_text: str) -> str:
    text = " ".join(response_text.split())
    if len(text) > 500:
        return f"{text[:500]}..."
    return text