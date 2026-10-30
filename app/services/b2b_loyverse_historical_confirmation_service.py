from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import date

from sqlalchemy import or_
from sqlalchemy.orm import Session, sessionmaker

from app.models import B2BLoyverseInvoiceDispatch, B2BSalesOrder, User
from app.services.audit_service import add_audit_event_in_session
from app.services.auth_service import user_has_permission
from app.services.b2b_loyverse_invoice_service import (
    SYNC_STATUS_HISTORICAL_CONFIRMED,
    SYNC_STATUS_UNKNOWN,
    resolve_reliable_b2b_invoice_date,
)


HISTORICAL_CONFIRMATION_CUTOFF = date(2026, 10, 1)
HISTORICAL_CONFIRMATION_TEXT = (
    "El negocio confirma que todos los pedidos B2B facturados en ERP antes del "
    "01/10/2026 ya fueron facturados en Loyverse."
)
EXECUTION_CONFIRMATION = "MARK_PRE_OCTOBER_2026_B2B_LOYVERSE_HISTORY"


class B2BLoyverseHistoricalConfirmationError(RuntimeError):
    pass


@dataclass(frozen=True)
class HistoricalConfirmationEvidence:
    order_id: int
    order_number: str
    invoice_date: str | None
    invoice_date_source: str
    delivery_date: str
    current_sync_status: str
    receipt_id: str
    receipt_number: str
    classification: str
    reason: str


@dataclass(frozen=True)
class HistoricalConfirmationPlan:
    cutoff_date: date
    rows: tuple[HistoricalConfirmationEvidence, ...]
    fingerprint: str

    @property
    def candidate_count(self) -> int:
        return sum(row.classification == "candidate" for row in self.rows)

    @property
    def already_historical_count(self) -> int:
        return sum(row.classification == "already_historical" for row in self.rows)

    @property
    def outside_cutoff_count(self) -> int:
        return sum(row.classification == "outside_cutoff" for row in self.rows)

    @property
    def review_count(self) -> int:
        return sum(row.classification.startswith("review_") for row in self.rows)


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


def _fingerprint(cutoff_date: date, rows: list[HistoricalConfirmationEvidence]) -> str:
    payload = {
        "schema": "b2b-loyverse-historical-confirmation-v1",
        "cutoff_date": cutoff_date.isoformat(),
        "rows": [row.__dict__ for row in sorted(rows, key=lambda item: item.order_id)],
    }
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def build_historical_confirmation_plan(
    db: Session,
    *,
    cutoff_date: date = HISTORICAL_CONFIRMATION_CUTOFF,
    lock_rows: bool = False,
) -> HistoricalConfirmationPlan:
    query = (
        db.query(B2BSalesOrder)
        .filter(B2BSalesOrder.status == "invoiced")
        .order_by(B2BSalesOrder.id)
    )
    if lock_rows and db.bind is not None and db.bind.dialect.name == "postgresql":
        query = query.with_for_update(of=B2BSalesOrder)
    orders = query.all()
    dispatches = (
        db.query(B2BLoyverseInvoiceDispatch)
        .filter(B2BLoyverseInvoiceDispatch.sales_order_id.in_([order.id for order in orders]))
        .all()
        if orders
        else []
    )
    dispatch_by_order_id = {dispatch.sales_order_id: dispatch for dispatch in dispatches}
    duplicate_ids = _duplicate_receipt_order_ids(db)
    rows: list[HistoricalConfirmationEvidence] = []

    for order in orders:
        invoice_date, date_source = resolve_reliable_b2b_invoice_date(order)
        sync_status = (order.loyverse_invoice_sync_status or "").strip()
        receipt_id = (order.loyverse_receipt_id or "").strip()
        receipt_number = (order.loyverse_receipt_number or "").strip()
        dispatch = dispatch_by_order_id.get(order.id)
        has_date_conflict = (
            order.invoice_date is not None
            and order.invoiced_at is not None
            and order.invoice_date != order.invoiced_at.date()
        )

        if has_date_conflict:
            classification = "review_date_conflict"
            reason = "invoice_date e invoiced_at no coinciden; se requiere validación manual."
        elif invoice_date is None:
            classification = "review_unreliable_date"
            reason = "No existe invoice_date ni invoiced_at; delivery_date no se acepta como evidencia."
        elif invoice_date >= cutoff_date:
            classification = "outside_cutoff"
            reason = "La fecha real de facturación no es anterior al corte histórico."
        elif order.id in duplicate_ids:
            classification = "review_duplicate_receipt"
            reason = "La referencia Loyverse está repetida en más de un pedido ERP."
        elif sync_status == SYNC_STATUS_UNKNOWN:
            classification = "review_unknown"
            reason = "El resultado Loyverse es unknown y debe conciliarse antes de cambiarlo."
        elif dispatch is not None and dispatch.status in {"queued", "processing", "unknown"}:
            classification = "review_active_dispatch"
            reason = f"Existe una ejecución persistente con estado {dispatch.status}."
        elif sync_status == SYNC_STATUS_HISTORICAL_CONFIRMED:
            classification = "already_historical"
            reason = "La confirmación histórica ya está registrada."
        else:
            classification = "candidate"
            reason = (
                "Se conservará la referencia Loyverse existente y se registrará la confirmación del negocio."
                if receipt_id or receipt_number
                else "Se registrará la confirmación histórica del negocio sin inventar un comprobante."
            )

        rows.append(
            HistoricalConfirmationEvidence(
                order_id=order.id,
                order_number=order.order_number,
                invoice_date=invoice_date.isoformat() if invoice_date is not None else None,
                invoice_date_source=date_source,
                delivery_date=order.delivery_date.isoformat(),
                current_sync_status=sync_status,
                receipt_id=receipt_id,
                receipt_number=receipt_number,
                classification=classification,
                reason=reason,
            )
        )

    return HistoricalConfirmationPlan(
        cutoff_date=cutoff_date,
        rows=tuple(rows),
        fingerprint=_fingerprint(cutoff_date, rows),
    )


def execute_historical_confirmation(
    session_factory: sessionmaker,
    *,
    confirmed_by_user_id: int,
    expected_fingerprint: str,
    business_confirmation: str,
) -> HistoricalConfirmationPlan:
    if business_confirmation != HISTORICAL_CONFIRMATION_TEXT:
        raise B2BLoyverseHistoricalConfirmationError(
            "La confirmación textual del negocio no coincide con la declaración aprobada."
        )
    with session_factory() as db:
        with db.begin():
            user = (
                db.query(User)
                .filter(User.id == confirmed_by_user_id, User.is_active.is_(True))
                .one_or_none()
            )
            if user is None or not user_has_permission(user, "sales.invoice"):
                raise B2BLoyverseHistoricalConfirmationError(
                    "Se requiere un usuario activo con permiso sales.invoice."
                )
            plan = build_historical_confirmation_plan(db, lock_rows=True)
            if not hmac.compare_digest(plan.fingerprint, expected_fingerprint):
                raise B2BLoyverseHistoricalConfirmationError(
                    "El dry-run cambió; genere evidencia nueva antes de ejecutar."
                )
            candidates = [row for row in plan.rows if row.classification == "candidate"]
            candidates_by_id = {row.order_id: row for row in candidates}
            if not candidates_by_id:
                return plan
            orders = (
                db.query(B2BSalesOrder)
                .filter(B2BSalesOrder.id.in_(candidates_by_id))
                .order_by(B2BSalesOrder.id)
                .all()
            )
            for order in orders:
                evidence = candidates_by_id[order.id]
                old_status = order.loyverse_invoice_sync_status
                order.loyverse_invoice_sync_status = SYNC_STATUS_HISTORICAL_CONFIRMED
                add_audit_event_in_session(
                    db,
                    module="sales",
                    action="b2b_loyverse_historical_invoice_confirmed",
                    entity_type="b2b_sales_order",
                    entity_id=order.id,
                    entity_label=order.order_number,
                    old_values={
                        "loyverse_invoice_sync_status": old_status,
                        "loyverse_receipt_id": order.loyverse_receipt_id,
                        "loyverse_receipt_number": order.loyverse_receipt_number,
                    },
                    new_values={
                        "loyverse_invoice_sync_status": SYNC_STATUS_HISTORICAL_CONFIRMED,
                        "invoice_date": evidence.invoice_date,
                        "invoice_date_source": evidence.invoice_date_source,
                        "cutoff_date": plan.cutoff_date,
                        "dry_run_fingerprint": plan.fingerprint,
                    },
                    notes=business_confirmation,
                    user=user,
                )
            add_audit_event_in_session(
                db,
                module="sales",
                action="b2b_loyverse_historical_batch_confirmed",
                entity_type="b2b_loyverse_historical_confirmation",
                entity_id=plan.fingerprint,
                entity_label=f"Facturas anteriores a {plan.cutoff_date.isoformat()}",
                new_values={
                    "candidate_count": len(candidates),
                    "cutoff_date": plan.cutoff_date,
                    "dry_run_fingerprint": plan.fingerprint,
                },
                notes=business_confirmation,
                user=user,
            )
            return plan
