from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import Iterable
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
HISTORICAL_IMPORT_PREFIX = "Historical CSV import"
HISTORICAL_CONFIRMATION_TEXT = (
    "El negocio confirma que todos los pedidos B2B facturados en ERP antes del "
    "01/10/2026 y los pedidos identificados por la marca interna "
    "Historical CSV import corresponden a la migración histórica y ya fueron "
    "facturados en Loyverse."
)
EXECUTION_CONFIRMATION = "MARK_PRE_OCTOBER_2026_B2B_LOYVERSE_HISTORY"
EXPLICIT_ID_CONFIRMATION_TEXT = (
    "El negocio confirma que únicamente los pedidos B2B identificados por ID en la "
    "lista aprobada, sin fecha de facturación ni marca técnica de migración, "
    "corresponden a operaciones históricas ya facturadas en Loyverse antes del "
    "01/10/2026. No se inventarán fechas ni comprobantes."
)
EXPLICIT_ID_EXECUTION_CONFIRMATION = "MARK_APPROVED_B2B_LOYVERSE_HISTORY_BY_ID"


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
    explicitly_approved: bool
    classification: str
    reason: str


@dataclass(frozen=True)
class HistoricalConfirmationPlan:
    cutoff_date: date
    rows: tuple[HistoricalConfirmationEvidence, ...]
    explicit_order_ids: tuple[int, ...]
    missing_explicit_order_ids: tuple[int, ...]
    fingerprint: str

    @property
    def candidate_count(self) -> int:
        if self.explicit_order_ids:
            return self.explicit_id_candidate_count
        return self.dated_historical_candidate_count + self.migrated_import_candidate_count

    @property
    def explicit_id_candidate_count(self) -> int:
        return sum(row.classification == "candidate_explicit_id" for row in self.rows)

    @property
    def dated_historical_candidate_count(self) -> int:
        return sum(row.classification == "candidate_dated_historical" for row in self.rows)

    @property
    def migrated_import_candidate_count(self) -> int:
        return sum(row.classification == "candidate_migrated_import" for row in self.rows)

    @property
    def already_historical_count(self) -> int:
        return sum(row.classification == "already_historical" for row in self.rows)

    @property
    def already_sent_count(self) -> int:
        return sum(row.classification == "already_sent" for row in self.rows)

    @property
    def outside_cutoff_count(self) -> int:
        return sum(row.classification == "outside_cutoff" for row in self.rows)

    @property
    def review_count(self) -> int:
        return sum(row.classification.startswith("review_") for row in self.rows)

    @property
    def explicit_selection_is_valid(self) -> bool:
        if not self.explicit_order_ids or self.missing_explicit_order_ids:
            return False
        selected_rows = [row for row in self.rows if row.explicitly_approved]
        return len(selected_rows) == len(self.explicit_order_ids) and all(
            row.classification in {"candidate_explicit_id", "already_historical"}
            for row in selected_rows
        )

    @property
    def explicit_rejected_count(self) -> int:
        return sum(
            row.explicitly_approved
            and row.classification not in {"candidate_explicit_id", "already_historical"}
            for row in self.rows
        ) + len(self.missing_explicit_order_ids)


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


def _fingerprint(
    cutoff_date: date,
    rows: list[HistoricalConfirmationEvidence],
    explicit_order_ids: tuple[int, ...],
    missing_explicit_order_ids: tuple[int, ...],
) -> str:
    payload = {
        "schema": "b2b-loyverse-historical-confirmation-v3",
        "cutoff_date": cutoff_date.isoformat(),
        "execution_scope": "explicit_ids" if explicit_order_ids else "standard",
        "explicit_order_ids": explicit_order_ids,
        "missing_explicit_order_ids": missing_explicit_order_ids,
        "rows": [row.__dict__ for row in sorted(rows, key=lambda item: item.order_id)],
    }
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def build_historical_confirmation_plan(
    db: Session,
    *,
    cutoff_date: date = HISTORICAL_CONFIRMATION_CUTOFF,
    lock_rows: bool = False,
    explicit_order_ids: Iterable[int] | None = None,
) -> HistoricalConfirmationPlan:
    normalized_explicit_ids = tuple(sorted({int(order_id) for order_id in explicit_order_ids or ()}))
    if any(order_id <= 0 for order_id in normalized_explicit_ids):
        raise B2BLoyverseHistoricalConfirmationError(
            "La lista explícita contiene IDs de pedido inválidos."
        )
    explicit_id_set = set(normalized_explicit_ids)
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
    invoiced_order_ids = {order.id for order in orders}
    missing_explicit_order_ids = tuple(sorted(explicit_id_set - invoiced_order_ids))
    rows: list[HistoricalConfirmationEvidence] = []

    for order in orders:
        invoice_date, date_source = resolve_reliable_b2b_invoice_date(order)
        is_migrated_import = (order.observations or "").startswith(HISTORICAL_IMPORT_PREFIX)
        sync_status = (order.loyverse_invoice_sync_status or "").strip()
        receipt_id = (order.loyverse_receipt_id or "").strip()
        receipt_number = (order.loyverse_receipt_number or "").strip()
        explicitly_approved = order.id in explicit_id_set
        dispatch = dispatch_by_order_id.get(order.id)
        has_date_conflict = (
            order.invoice_date is not None
            and order.invoiced_at is not None
            and order.invoice_date != order.invoiced_at.date()
        )

        if has_date_conflict:
            classification = "review_date_conflict"
            reason = "invoice_date e invoiced_at no coinciden; se requiere validación manual."
        elif invoice_date is not None and invoice_date >= cutoff_date:
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
        elif dispatch is not None and dispatch.status == "success":
            classification = "review_success_without_receipt"
            reason = (
                "Existe una ejecución persistente success sin referencia Loyverse en el pedido; "
                "se requiere conciliación manual."
            )
        elif dispatch is not None and dispatch.status == "failed":
            classification = "review_failed_dispatch"
            reason = (
                "Existe una ejecución persistente fallida; se requiere revisar ese intento antes "
                "de aplicar una confirmación histórica."
            )
        elif sync_status == SYNC_STATUS_HISTORICAL_CONFIRMED:
            classification = "already_historical"
            reason = "La confirmación histórica ya está registrada."
        elif receipt_id or receipt_number:
            classification = "already_sent"
            reason = "El pedido ya conserva una referencia Loyverse y no será modificado."
        elif sync_status == "success":
            classification = "review_success_without_receipt"
            reason = (
                "El estado Loyverse es success pero no existe una referencia local; "
                "se requiere conciliación manual."
            )
        elif explicitly_approved and invoice_date is not None:
            classification = "review_explicit_id_has_reliable_date"
            reason = (
                "El ID aprobado tiene fecha de facturación confiable y no corresponde a la "
                "excepción controlada para fechas nulas."
            )
        elif explicitly_approved and is_migrated_import:
            classification = "review_explicit_id_has_migration_marker"
            reason = (
                "El ID aprobado ya tiene la marca técnica de migración y debe procesarse por "
                "la regla estándar, no por la excepción manual."
            )
        elif explicitly_approved:
            classification = "candidate_explicit_id"
            reason = (
                "El ID figura en la lista exacta aprobada por el negocio; no tiene fecha "
                "confiable, referencia Loyverse, envío activo, resultado unknown ni marca de "
                "migración. No se inventará fecha ni comprobante."
            )
        elif is_migrated_import:
            classification = "candidate_migrated_import"
            reason = (
                "La marca interna Historical CSV import identifica el pedido como parte de la "
                "migración histórica confirmada por el negocio; no se inventará una fecha ni un "
                "comprobante Loyverse."
            )
        elif invoice_date is None:
            classification = "review_unreliable_date"
            reason = (
                "No existe invoice_date ni invoiced_at y tampoco la marca interna de migración; "
                "el pedido requiere validación explícita por ID."
            )
        else:
            classification = "candidate_dated_historical"
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
                explicitly_approved=explicitly_approved,
                classification=classification,
                reason=reason,
            )
        )

    return HistoricalConfirmationPlan(
        cutoff_date=cutoff_date,
        rows=tuple(rows),
        explicit_order_ids=normalized_explicit_ids,
        missing_explicit_order_ids=missing_explicit_order_ids,
        fingerprint=_fingerprint(
            cutoff_date,
            rows,
            normalized_explicit_ids,
            missing_explicit_order_ids,
        ),
    )


def execute_historical_confirmation(
    session_factory: sessionmaker,
    *,
    confirmed_by_user_id: int,
    expected_fingerprint: str,
    business_confirmation: str,
    explicit_order_ids: Iterable[int] | None = None,
) -> HistoricalConfirmationPlan:
    normalized_explicit_ids = tuple(sorted({int(order_id) for order_id in explicit_order_ids or ()}))
    required_business_confirmation = (
        EXPLICIT_ID_CONFIRMATION_TEXT if normalized_explicit_ids else HISTORICAL_CONFIRMATION_TEXT
    )
    if business_confirmation != required_business_confirmation:
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
            plan = build_historical_confirmation_plan(
                db,
                lock_rows=True,
                explicit_order_ids=normalized_explicit_ids,
            )
            if not hmac.compare_digest(plan.fingerprint, expected_fingerprint):
                raise B2BLoyverseHistoricalConfirmationError(
                    "El dry-run cambió; genere evidencia nueva antes de ejecutar."
                )
            if normalized_explicit_ids:
                if not plan.explicit_selection_is_valid:
                    raise B2BLoyverseHistoricalConfirmationError(
                        "La lista explícita contiene pedidos ausentes, no facturados o con "
                        "contradicciones. No se aplicó ningún cambio."
                    )
                candidate_classes = {"candidate_explicit_id"}
            else:
                candidate_classes = {
                    "candidate_dated_historical",
                    "candidate_migrated_import",
                }
            candidates = [row for row in plan.rows if row.classification in candidate_classes]
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
                        "confirmation_scope": (
                            "explicit_ids" if normalized_explicit_ids else "standard"
                        ),
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
                    "confirmation_scope": (
                        "explicit_ids" if normalized_explicit_ids else "standard"
                    ),
                    "explicit_order_ids": list(normalized_explicit_ids),
                },
                notes=business_confirmation,
                user=user,
            )
            return plan
