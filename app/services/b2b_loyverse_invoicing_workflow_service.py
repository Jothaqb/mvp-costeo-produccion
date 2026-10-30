from __future__ import annotations

import hashlib
import hmac
import json
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Callable

from fastapi import Request
from sqlalchemy import and_, or_
from sqlalchemy.orm import Session, joinedload

from app.models import B2BLoyverseInvoiceDispatch, B2BSalesOrder, User
from app.services.audit_service import add_audit_event_in_session, safe_log_audit_event
from app.services.b2b_loyverse_invoice_service import (
    B2BLoyverseInvoiceError,
    SYNC_STATUS_FAILED,
    SYNC_STATUS_HISTORICAL_CONFIRMED,
    SYNC_STATUS_SUCCESS,
    SYNC_STATUS_UNKNOWN,
    _require_store_id,
    _require_token,
    _recalculate_order_total,
    resolve_reliable_b2b_invoice_date,
)
from tools.b2b_loyverse_monthly_invoice_job import (
    Evaluation,
    evaluate_order,
    execute_one,
)


WORKER_ENABLED_ENV = "B2B_LOYVERSE_INVOICE_WORKER_ENABLED"
WORKER_INTERVAL_ENV = "B2B_LOYVERSE_INVOICE_WORKER_INTERVAL_SECONDS"
CSRF_PURPOSE = b"green-corner:b2b:loyverse-invoicing:confirm:v1"
CONFIRMATION_PURPOSE = b"green-corner:b2b:loyverse-invoicing:plan:v1"
MAX_DATE_RANGE_DAYS = 62
STALE_PROCESSING_MINUTES = 10


class B2BLoyverseInvoicingWorkflowError(Exception):
    pass


class B2BLoyverseInvoicingSecurityError(B2BLoyverseInvoicingWorkflowError):
    pass


class B2BLoyverseInvoicingStalePlanError(B2BLoyverseInvoicingWorkflowError):
    pass


def _utc_now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


@dataclass(frozen=True)
class B2BLoyversePreviewRow:
    order_id: int
    order_number: str
    invoice_date: date
    invoice_date_source: str
    delivery_date: date
    erp_status: str
    classification: str
    eligible: bool
    total_amount: Decimal
    payload_fingerprint: str
    loyverse_receipt_id: str
    loyverse_receipt_number: str
    loyverse_sync_status: str
    reason: str
    missing_customer_mapping: bool = False
    missing_variant_mapping: bool = False
    missing_payment_type_mapping: bool = False


@dataclass(frozen=True)
class B2BLoyversePreview:
    start_date: date
    end_date: date
    rows: tuple[B2BLoyversePreviewRow, ...]
    plan_fingerprint: str
    total_found: int
    eligible_count: int
    already_sent_count: int
    excluded_count: int
    blocked_count: int
    eligible_amount: Decimal
    missing_customer_mapping_count: int
    missing_variant_mapping_count: int
    missing_payment_type_mapping_count: int


def validate_invoicing_date_range(start_date: date, end_date: date) -> None:
    if end_date < start_date:
        raise B2BLoyverseInvoicingWorkflowError(
            "La fecha final no puede ser anterior a la fecha inicial."
        )
    if (end_date - start_date).days + 1 > MAX_DATE_RANGE_DAYS:
        raise B2BLoyverseInvoicingWorkflowError(
            f"El período no puede exceder {MAX_DATE_RANGE_DAYS} días."
        )


def _order_total(order: B2BSalesOrder) -> Decimal:
    return _recalculate_order_total(order)


def _row_for_order(
    order: B2BSalesOrder,
    *,
    invoice_date: date,
    invoice_date_source: str,
    classification: str,
    reason: str,
) -> B2BLoyversePreviewRow:
    return B2BLoyversePreviewRow(
        order_id=order.id,
        order_number=order.order_number,
        invoice_date=invoice_date,
        invoice_date_source=invoice_date_source,
        delivery_date=order.delivery_date,
        erp_status=order.status,
        classification=classification,
        eligible=False,
        total_amount=_order_total(order),
        payload_fingerprint="",
        loyverse_receipt_id=(order.loyverse_receipt_id or "").strip(),
        loyverse_receipt_number=(order.loyverse_receipt_number or "").strip(),
        loyverse_sync_status=(order.loyverse_invoice_sync_status or "").strip(),
        reason=reason,
    )


def _row_from_evaluation(
    evaluation: Evaluation,
    *,
    invoice_date: date,
    invoice_date_source: str,
) -> B2BLoyversePreviewRow:
    classification = evaluation.classification
    if classification not in {"eligible", "already_sent", "blocked"}:
        classification = "excluded"
    return B2BLoyversePreviewRow(
        order_id=evaluation.order_id,
        order_number=evaluation.order_number,
        invoice_date=invoice_date,
        invoice_date_source=invoice_date_source,
        delivery_date=evaluation.delivery_date,
        erp_status=evaluation.erp_status,
        classification=classification,
        eligible=evaluation.eligible and classification == "eligible",
        total_amount=evaluation.total_amount,
        payload_fingerprint=evaluation.payload_fingerprint,
        loyverse_receipt_id=evaluation.loyverse_receipt_id,
        loyverse_receipt_number=evaluation.loyverse_receipt_number,
        loyverse_sync_status=evaluation.loyverse_sync_status,
        reason=evaluation.reason,
        missing_customer_mapping=evaluation.missing_customer_mapping,
        missing_variant_mapping=evaluation.missing_variant_mapping,
        missing_payment_type_mapping=evaluation.missing_payment_type_mapping,
    )


def _duplicate_receipt_order_ids(db: Session) -> set[int]:
    references: dict[tuple[str, str], list[int]] = {}
    rows = (
        db.query(
            B2BSalesOrder.id,
            B2BSalesOrder.loyverse_receipt_id,
            B2BSalesOrder.loyverse_receipt_number,
        )
        .filter(
            or_(
                B2BSalesOrder.loyverse_receipt_id.is_not(None),
                B2BSalesOrder.loyverse_receipt_number.is_not(None),
            )
        )
        .all()
    )
    for order_id, receipt_id, receipt_number in rows:
        normalized_id = (receipt_id or "").strip()
        normalized_number = (receipt_number or "").strip()
        if normalized_id:
            references.setdefault(("id", normalized_id), []).append(order_id)
        if normalized_number:
            references.setdefault(("number", normalized_number), []).append(order_id)
    return {
        order_id
        for order_ids in references.values()
        if len(set(order_ids)) > 1
        for order_id in order_ids
    }


def _active_dispatches_by_order(
    db: Session,
    order_ids: list[int],
) -> dict[int, B2BLoyverseInvoiceDispatch]:
    if not order_ids:
        return {}
    dispatches = (
        db.query(B2BLoyverseInvoiceDispatch)
        .filter(B2BLoyverseInvoiceDispatch.sales_order_id.in_(order_ids))
        .all()
    )
    return {dispatch.sales_order_id: dispatch for dispatch in dispatches}


def _plan_fingerprint(
    start_date: date,
    end_date: date,
    rows: list[B2BLoyversePreviewRow],
) -> str:
    canonical = {
        "schema": "b2b-loyverse-invoicing-preview-v2-invoice-date",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "orders": [
            {
                "order_id": row.order_id,
                "order_number": row.order_number,
                "invoice_date": row.invoice_date.isoformat(),
                "invoice_date_source": row.invoice_date_source,
                "delivery_date": row.delivery_date.isoformat(),
                "erp_status": row.erp_status,
                "classification": row.classification,
                "eligible": row.eligible,
                "total_amount": format(row.total_amount, "f"),
                "payload_fingerprint": row.payload_fingerprint,
                "loyverse_receipt_id": row.loyverse_receipt_id,
                "loyverse_receipt_number": row.loyverse_receipt_number,
                "loyverse_sync_status": row.loyverse_sync_status,
                "reason": row.reason,
            }
            for row in sorted(rows, key=lambda item: item.order_id)
        ],
    }
    serialized = json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _load_invoiced_orders_by_reliable_invoice_date(
    db: Session,
    start_date: date,
    end_date_exclusive: date,
) -> list[B2BSalesOrder]:
    start_datetime = datetime.combine(start_date, datetime.min.time())
    end_datetime = datetime.combine(end_date_exclusive, datetime.min.time())
    return (
        db.query(B2BSalesOrder)
        .options(joinedload(B2BSalesOrder.customer), joinedload(B2BSalesOrder.lines))
        .filter(
            B2BSalesOrder.status == "invoiced",
            or_(
                and_(
                    B2BSalesOrder.invoice_date >= start_date,
                    B2BSalesOrder.invoice_date < end_date_exclusive,
                ),
                and_(
                    B2BSalesOrder.invoice_date.is_(None),
                    B2BSalesOrder.invoiced_at >= start_datetime,
                    B2BSalesOrder.invoiced_at < end_datetime,
                ),
            ),
        )
        .order_by(
            B2BSalesOrder.invoice_date,
            B2BSalesOrder.invoiced_at,
            B2BSalesOrder.id,
        )
        .all()
    )


def build_b2b_loyverse_invoicing_preview(
    db: Session,
    *,
    start_date: date,
    end_date: date,
) -> B2BLoyversePreview:
    """Build a read-only preview using the existing monthly payload/mapping rules."""
    validate_invoicing_date_range(start_date, end_date)
    end_date_exclusive = end_date + timedelta(days=1)
    orders = _load_invoiced_orders_by_reliable_invoice_date(
        db,
        start_date,
        end_date_exclusive,
    )
    dispatch_by_order = _active_dispatches_by_order(db, [order.id for order in orders])
    duplicate_receipt_order_ids = _duplicate_receipt_order_ids(db)
    store_id = os.getenv("LOYVERSE_STORE_ID", "").strip()
    rows: list[B2BLoyversePreviewRow] = []

    for order in orders:
        invoice_date, invoice_date_source = resolve_reliable_b2b_invoice_date(order)
        if invoice_date is None:
            continue
        if (
            order.invoice_date is not None
            and order.invoiced_at is not None
            and order.invoice_date != order.invoiced_at.date()
        ):
            rows.append(
                _row_for_order(
                    order,
                    invoice_date=invoice_date,
                    invoice_date_source=invoice_date_source,
                    classification="blocked",
                    reason=(
                        "invoice_date e invoiced_at no coinciden; se requiere revisar la fecha real "
                        "antes de enviar."
                    ),
                )
            )
            continue
        if order.id in duplicate_receipt_order_ids:
            rows.append(
                _row_for_order(
                    order,
                    invoice_date=invoice_date,
                    invoice_date_source=invoice_date_source,
                    classification="blocked",
                    reason=(
                        "La referencia de comprobante Loyverse está repetida en más de un pedido ERP; "
                        "se requiere conciliación manual."
                    ),
                )
            )
            continue
        if (order.loyverse_receipt_id or "").strip() or (
            order.loyverse_receipt_number or ""
        ).strip():
            rows.append(
                _row_for_order(
                    order,
                    invoice_date=invoice_date,
                    invoice_date_source=invoice_date_source,
                    classification="already_sent",
                    reason="El pedido ya tiene una referencia de comprobante Loyverse almacenada.",
                )
            )
            continue
        sync_status = (order.loyverse_invoice_sync_status or "").strip()
        if sync_status == SYNC_STATUS_HISTORICAL_CONFIRMED:
            rows.append(
                _row_for_order(
                    order,
                    invoice_date=invoice_date,
                    invoice_date_source=invoice_date_source,
                    classification="already_sent",
                    reason=(
                        "El negocio confirmó que esta factura histórica ya fue facturada en Loyverse."
                    ),
                )
            )
            continue
        dispatch = dispatch_by_order.get(order.id)
        if dispatch is not None and dispatch.status != "failed":
            rows.append(
                _row_for_order(
                    order,
                    invoice_date=invoice_date,
                    invoice_date_source=invoice_date_source,
                    classification="blocked",
                    reason=(
                        "El pedido ya tiene un registro persistente de envío "
                        f"con estado {dispatch.status}; no se permite crear otra ejecución."
                    ),
                )
            )
            continue
        if sync_status == SYNC_STATUS_UNKNOWN:
            rows.append(
                _row_for_order(
                    order,
                    invoice_date=invoice_date,
                    invoice_date_source=invoice_date_source,
                    classification="blocked",
                    reason=(
                        "El resultado anterior es unknown; se requiere verificar Loyverse antes de reintentar."
                    ),
                )
            )
            continue
        if sync_status == SYNC_STATUS_SUCCESS and not (
            (order.loyverse_receipt_id or "").strip()
            or (order.loyverse_receipt_number or "").strip()
        ):
            rows.append(
                _row_for_order(
                    order,
                    invoice_date=invoice_date,
                    invoice_date_source=invoice_date_source,
                    classification="blocked",
                    reason=(
                        "El estado local indica success pero no existe referencia del comprobante; "
                        "se requiere conciliación manual."
                    ),
                )
            )
            continue
        if not store_id:
            rows.append(
                _row_for_order(
                    order,
                    invoice_date=invoice_date,
                    invoice_date_source=invoice_date_source,
                    classification="blocked",
                    reason="LOYVERSE_STORE_ID no está configurado; no se puede construir el envío.",
                )
            )
            continue
        evaluation = evaluate_order(db, order, store_id)
        rows.append(
            _row_from_evaluation(
                evaluation,
                invoice_date=invoice_date,
                invoice_date_source=invoice_date_source,
            )
        )

    fingerprint = _plan_fingerprint(start_date, end_date, rows)
    return B2BLoyversePreview(
        start_date=start_date,
        end_date=end_date,
        rows=tuple(rows),
        plan_fingerprint=fingerprint,
        total_found=len(rows),
        eligible_count=sum(row.classification == "eligible" for row in rows),
        already_sent_count=sum(row.classification == "already_sent" for row in rows),
        excluded_count=sum(row.classification == "excluded" for row in rows),
        blocked_count=sum(row.classification == "blocked" for row in rows),
        eligible_amount=sum(
            (row.total_amount for row in rows if row.classification == "eligible"),
            Decimal("0"),
        ),
        missing_customer_mapping_count=sum(row.missing_customer_mapping for row in rows),
        missing_variant_mapping_count=sum(row.missing_variant_mapping for row in rows),
        missing_payment_type_mapping_count=sum(row.missing_payment_type_mapping for row in rows),
    )


def _session_token(session_token: str | None) -> str:
    normalized = (session_token or "").strip()
    if not normalized:
        raise B2BLoyverseInvoicingSecurityError("No existe una sesión válida para confirmar.")
    return normalized


def build_b2b_loyverse_invoicing_csrf_token(session_token: str | None) -> str:
    return hmac.new(_session_token(session_token).encode(), CSRF_PURPOSE, hashlib.sha256).hexdigest()


def validate_b2b_loyverse_invoicing_csrf_token(
    session_token: str | None,
    submitted_token: str | None,
) -> None:
    expected = build_b2b_loyverse_invoicing_csrf_token(session_token)
    submitted = (submitted_token or "").strip()
    if not submitted or not hmac.compare_digest(expected, submitted):
        raise B2BLoyverseInvoicingSecurityError("El token CSRF no es válido para esta sesión.")


def _confirmation_message(start_date: date, end_date: date, plan_fingerprint: str) -> bytes:
    return (
        f"{CONFIRMATION_PURPOSE.decode()}|{start_date.isoformat()}|"
        f"{end_date.isoformat()}|{plan_fingerprint}"
    ).encode()


def build_b2b_loyverse_invoicing_confirmation_token(
    session_token: str | None,
    *,
    start_date: date,
    end_date: date,
    plan_fingerprint: str,
) -> str:
    return hmac.new(
        _session_token(session_token).encode(),
        _confirmation_message(start_date, end_date, plan_fingerprint),
        hashlib.sha256,
    ).hexdigest()


def validate_b2b_loyverse_invoicing_confirmation_token(
    session_token: str | None,
    submitted_token: str | None,
    *,
    start_date: date,
    end_date: date,
    plan_fingerprint: str,
) -> None:
    expected = build_b2b_loyverse_invoicing_confirmation_token(
        session_token,
        start_date=start_date,
        end_date=end_date,
        plan_fingerprint=plan_fingerprint,
    )
    submitted = (submitted_token or "").strip()
    if not submitted or not hmac.compare_digest(expected, submitted):
        raise B2BLoyverseInvoicingStalePlanError(
            "La vista previa no es válida para esta sesión o ya cambió; genere una nueva."
        )


def b2b_loyverse_invoice_worker_enabled() -> bool:
    return os.getenv(WORKER_ENABLED_ENV, "").strip().lower() in {"1", "true", "yes", "on"}


def b2b_loyverse_invoice_worker_interval_seconds() -> int:
    raw = os.getenv(WORKER_INTERVAL_ENV, "15").strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise B2BLoyverseInvoicingWorkflowError(
            f"{WORKER_INTERVAL_ENV} debe ser un número entero."
        ) from exc
    if value < 5 or value > 3600:
        raise B2BLoyverseInvoicingWorkflowError(
            f"{WORKER_INTERVAL_ENV} debe estar entre 5 y 3600 segundos."
        )
    return value


def validate_b2b_loyverse_execution_configuration() -> None:
    if not b2b_loyverse_invoice_worker_enabled():
        raise B2BLoyverseInvoicingWorkflowError(
            f"La ejecución está deshabilitada. Configure {WORKER_ENABLED_ENV}=true después de aprobar el despliegue."
        )
    try:
        _require_token()
        _require_store_id()
    except B2BLoyverseInvoiceError as exc:
        raise B2BLoyverseInvoicingWorkflowError(str(exc)) from exc


def lock_b2b_loyverse_invoicing_range(
    db: Session,
    *,
    start_date: date,
    end_date: date,
) -> None:
    end_date_exclusive = end_date + timedelta(days=1)
    start_datetime = datetime.combine(start_date, datetime.min.time())
    end_datetime = datetime.combine(end_date_exclusive, datetime.min.time())
    query = db.query(B2BSalesOrder.id).filter(
        B2BSalesOrder.status == "invoiced",
        or_(
            and_(
                B2BSalesOrder.invoice_date >= start_date,
                B2BSalesOrder.invoice_date < end_date_exclusive,
            ),
            and_(
                B2BSalesOrder.invoice_date.is_(None),
                B2BSalesOrder.invoiced_at >= start_datetime,
                B2BSalesOrder.invoiced_at < end_datetime,
            ),
        ),
    )
    if db.bind is not None and db.bind.dialect.name == "postgresql":
        query = query.with_for_update(of=B2BSalesOrder)
    query.all()


def queue_b2b_loyverse_invoicing_preview(
    db: Session,
    *,
    preview: B2BLoyversePreview,
    user: User,
    request: Request,
) -> int:
    eligible_rows = [row for row in preview.rows if row.eligible]
    for row in eligible_rows:
        order = db.query(B2BSalesOrder).filter(B2BSalesOrder.id == row.order_id).one()
        order.loyverse_invoice_sync_status = "queued"
        order.loyverse_invoice_sync_error = None
        dispatch = (
            db.query(B2BLoyverseInvoiceDispatch)
            .filter(B2BLoyverseInvoiceDispatch.sales_order_id == order.id)
            .one_or_none()
        )
        if dispatch is None:
            dispatch = B2BLoyverseInvoiceDispatch(sales_order_id=order.id)
            db.add(dispatch)
        elif dispatch.status != "failed":
            raise B2BLoyverseInvoicingStalePlanError(
                f"El pedido {order.order_number} ya tiene una ejecución no reintentable."
            )
        dispatch.status = "queued"
        dispatch.start_date = preview.start_date
        dispatch.end_date = preview.end_date
        dispatch.plan_fingerprint = preview.plan_fingerprint
        dispatch.payload_fingerprint = row.payload_fingerprint
        dispatch.queued_at = _utc_now()
        dispatch.queued_by_user_id = user.id
        dispatch.processing_started_at = None
        dispatch.completed_at = None
        dispatch.last_error = None
        add_audit_event_in_session(
            db,
            module="sales",
            action="b2b_loyverse_invoice_queued",
            entity_type="b2b_sales_order",
            entity_id=order.id,
            entity_label=order.order_number,
            old_values={"loyverse_invoice_sync_status": row.loyverse_sync_status or None},
            new_values={
                "loyverse_invoice_sync_status": "queued",
                "period_start": preview.start_date,
                "period_end": preview.end_date,
                "plan_fingerprint": preview.plan_fingerprint,
                "payload_fingerprint": row.payload_fingerprint,
            },
            request=request,
            user=user,
        )
    add_audit_event_in_session(
        db,
        module="sales",
        action="b2b_loyverse_invoice_batch_confirmed",
        entity_type="b2b_loyverse_invoice_batch",
        entity_id=preview.plan_fingerprint,
        entity_label=f"{preview.start_date.isoformat()} - {preview.end_date.isoformat()}",
        new_values={
            "eligible_orders_queued": len(eligible_rows),
            "eligible_amount": preview.eligible_amount,
            "plan_fingerprint": preview.plan_fingerprint,
        },
        request=request,
        user=user,
    )
    return len(eligible_rows)


def loyverse_integration_status(order: B2BSalesOrder) -> dict[str, str]:
    receipt = (order.loyverse_receipt_number or order.loyverse_receipt_id or "").strip()
    sync_status = (order.loyverse_invoice_sync_status or "").strip()
    if sync_status == SYNC_STATUS_HISTORICAL_CONFIRMED:
        return {
            "code": "sent",
            "label": "Facturado en Loyverse — Histórico",
            "detail": receipt or "Confirmación histórica del negocio",
        }
    if receipt:
        return {
            "code": "sent",
            "label": "Facturado en Loyverse",
            "detail": receipt,
        }
    if sync_status in {SYNC_STATUS_UNKNOWN, SYNC_STATUS_FAILED, SYNC_STATUS_SUCCESS}:
        return {
            "code": "error",
            "label": "Error / por verificar",
            "detail": (order.loyverse_invoice_sync_error or "").strip(),
        }
    if order.status != "invoiced":
        return {
            "code": "blocked",
            "label": "Bloqueado",
            "detail": f"Estado ERP: {order.status}",
        }
    return {
        "code": "pending",
        "label": "Pendiente de enviar",
        "detail": "En cola" if sync_status == "queued" else "",
    }


def _recover_stale_dispatches(session_factory: Callable[[], Session]) -> int:
    db = session_factory()
    recovered = 0
    try:
        stale_before = _utc_now() - timedelta(
            minutes=STALE_PROCESSING_MINUTES
        )
        dispatches = (
            db.query(B2BLoyverseInvoiceDispatch)
            .filter(
                B2BLoyverseInvoiceDispatch.status == "processing",
                B2BLoyverseInvoiceDispatch.processing_started_at < stale_before,
            )
            .all()
        )
        for dispatch in dispatches:
            order = dispatch.sales_order
            if order.loyverse_receipt_id or order.loyverse_receipt_number:
                dispatch.status = "success"
                dispatch.completed_at = _utc_now()
            elif order.loyverse_invoice_sync_status == SYNC_STATUS_UNKNOWN:
                dispatch.status = "unknown"
                dispatch.last_error = order.loyverse_invoice_sync_error
                dispatch.completed_at = _utc_now()
            else:
                dispatch.status = "queued"
                dispatch.processing_started_at = None
            recovered += 1
        db.commit()
        return recovered
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def _claim_dispatch(session_factory: Callable[[], Session]) -> int | None:
    db = session_factory()
    try:
        query = (
            db.query(B2BLoyverseInvoiceDispatch)
            .filter(B2BLoyverseInvoiceDispatch.status == "queued")
            .order_by(
                B2BLoyverseInvoiceDispatch.queued_at,
                B2BLoyverseInvoiceDispatch.id,
            )
        )
        if db.bind is not None and db.bind.dialect.name == "postgresql":
            query = query.with_for_update(skip_locked=True, of=B2BLoyverseInvoiceDispatch)
        dispatch = query.first()
        if dispatch is None:
            db.rollback()
            return None
        dispatch.status = "processing"
        dispatch.processing_started_at = _utc_now()
        dispatch.last_error = None
        dispatch_id = dispatch.id
        db.commit()
        return dispatch_id
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def _mark_dispatch_failed(
    session_factory: Callable[[], Session],
    dispatch_id: int,
    message: str,
) -> None:
    db = session_factory()
    try:
        dispatch = (
            db.query(B2BLoyverseInvoiceDispatch)
            .filter(B2BLoyverseInvoiceDispatch.id == dispatch_id)
            .one()
        )
        order = dispatch.sales_order
        if not (order.loyverse_receipt_id or order.loyverse_receipt_number):
            order.loyverse_invoice_sync_status = SYNC_STATUS_FAILED
            order.loyverse_invoice_sync_error = message[:2000]
        dispatch.status = "failed"
        dispatch.last_error = message[:2000]
        dispatch.completed_at = _utc_now()
        user = db.query(User).filter(User.id == dispatch.queued_by_user_id).one_or_none()
        add_audit_event_in_session(
            db,
            module="sales",
            action="b2b_loyverse_invoice_result",
            entity_type="b2b_sales_order",
            entity_id=order.id,
            entity_label=order.order_number,
            new_values={"result": "failed", "message": message},
            user=user,
        )
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def _process_dispatch(
    session_factory: Callable[[], Session],
    dispatch_id: int,
) -> str:
    db = session_factory()
    try:
        dispatch = (
            db.query(B2BLoyverseInvoiceDispatch)
            .filter(B2BLoyverseInvoiceDispatch.id == dispatch_id)
            .one()
        )
        order = dispatch.sales_order
        if dispatch.status != "processing":
            return dispatch.status
        if order.status != "invoiced":
            message = "El pedido dejó de estar invoiced antes del envío; no se llamó a Loyverse."
            db.close()
            _mark_dispatch_failed(session_factory, dispatch_id, message)
            return "failed"
        if order.loyverse_receipt_id or order.loyverse_receipt_number:
            message = "El pedido recibió una referencia Loyverse antes del envío; se bloqueó la ejecución."
            db.close()
            _mark_dispatch_failed(session_factory, dispatch_id, message)
            return "failed"
        store_id = _require_store_id()
        evaluation = evaluate_order(db, order, store_id)
        if not evaluation.eligible:
            message = f"La revalidación bloqueó el envío: {evaluation.reason}"
            db.close()
            _mark_dispatch_failed(session_factory, dispatch_id, message)
            return "failed"
        if evaluation.payload_fingerprint != dispatch.payload_fingerprint:
            message = "Los datos o mapeos cambiaron después de confirmar; genere una nueva vista previa."
            db.close()
            _mark_dispatch_failed(session_factory, dispatch_id, message)
            return "failed"
        start_date = dispatch.start_date
        end_date = dispatch.end_date
        queued_by_user_id = dispatch.queued_by_user_id
    finally:
        if db.is_active:
            db.close()

    result, _error = execute_one(
        session_factory,
        evaluation,
        start_date=start_date,
        end_date_exclusive=end_date + timedelta(days=1),
        token=_require_token(),
        store_id=store_id,
        timezone=timezone(timedelta(hours=-6), name="America/Costa_Rica"),
        fallback_payment_type_id="",
        fallback_payment_type_name="",
        customer_override_erp_id=None,
        customer_override_loyverse_id="",
        customer_override_loyverse_name="",
        excluded_order_numbers=set(),
        required_order_status="invoiced",
        approved_date_field="invoice_date",
    )
    outcome = str(result.get("result") or "failed")
    message = str(result.get("message") or "")
    db = session_factory()
    try:
        dispatch = (
            db.query(B2BLoyverseInvoiceDispatch)
            .filter(B2BLoyverseInvoiceDispatch.id == dispatch_id)
            .one()
        )
        order = dispatch.sales_order
        if outcome == "success" and (order.loyverse_receipt_id or order.loyverse_receipt_number):
            dispatch.status = "success"
            dispatch.last_error = None
        elif outcome == "unknown" or order.loyverse_invoice_sync_status == SYNC_STATUS_UNKNOWN:
            dispatch.status = "unknown"
            dispatch.last_error = message[:2000]
        else:
            dispatch.status = "failed"
            dispatch.last_error = message[:2000]
            if not (order.loyverse_receipt_id or order.loyverse_receipt_number):
                order.loyverse_invoice_sync_status = SYNC_STATUS_FAILED
                order.loyverse_invoice_sync_error = message[:2000]
        dispatch.completed_at = _utc_now()
        db.commit()
        user = db.query(User).filter(User.id == queued_by_user_id).one_or_none()
        order_number = order.order_number
        order_id = order.id
        receipt_id = order.loyverse_receipt_id
        receipt_number = order.loyverse_receipt_number
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    safe_log_audit_event(
        module="sales",
        action="b2b_loyverse_invoice_result",
        entity_type="b2b_sales_order",
        entity_id=order_id,
        entity_label=order_number,
        new_values={
            "result": outcome,
            "message": message,
            "loyverse_receipt_id": receipt_id,
            "loyverse_receipt_number": receipt_number,
        },
        user=user,
    )
    return outcome


def run_b2b_loyverse_invoice_worker_cycle(
    session_factory: Callable[[], Session],
    *,
    max_orders: int = 5,
) -> dict[str, int]:
    """Process queued orders. This function is never called unless the feature flag is enabled."""
    _require_token()
    _require_store_id()
    recovered = _recover_stale_dispatches(session_factory)
    processed = 0
    success = 0
    failed = 0
    unknown = 0
    for _ in range(max_orders):
        dispatch_id = _claim_dispatch(session_factory)
        if dispatch_id is None:
            break
        outcome = _process_dispatch(session_factory, dispatch_id)
        processed += 1
        if outcome == "success":
            success += 1
        elif outcome == "unknown":
            unknown += 1
        else:
            failed += 1
    return {
        "recovered": recovered,
        "processed": processed,
        "success": success,
        "failed": failed,
        "unknown": unknown,
    }
