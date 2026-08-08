from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session, joinedload, sessionmaker


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import app.models  # noqa: F401
from app.database import SessionLocal
from app.models import B2BAROpeningBalance, B2BARPayment, B2BSalesOrder


HISTORICAL_IMPORT_PREFIX = "Historical CSV import"
HISTORICAL_DELIVERY_DATE_LIMIT = date(2026, 6, 1)
CONFIRMATION_TEXT = "BACKFILL_B2B_HISTORICAL_INVOICE_DATES_BEFORE_JUNE_2026"
B2B1055_ORDER_NUMBER = "B2B1055"
B2B1055_EXPECTED_INVOICE_DATE = date(2026, 4, 30)


class BackfillValidationError(RuntimeError):
    pass


@dataclass(frozen=True)
class BackfillOrderEvidence:
    order_id: int
    order_number: str
    delivery_date: str
    created_at: str
    invoiced_at: str | None
    current_invoice_date: str | None
    proposed_invoice_date: str
    invoice_date_override: str | None
    classification: str
    reason: str


@dataclass(frozen=True)
class BackfillPlan:
    candidates: tuple[BackfillOrderEvidence, ...]
    updatable: tuple[BackfillOrderEvidence, ...]
    anomalies: tuple[BackfillOrderEvidence, ...]

    @property
    def candidate_count(self) -> int:
        return len(self.candidates)

    @property
    def updatable_count(self) -> int:
        return len(self.updatable)

    @property
    def anomaly_count(self) -> int:
        return len(self.anomalies)


def _historical_universe_query(db: Session):
    return (
        db.query(B2BSalesOrder)
        .options(joinedload(B2BSalesOrder.ar_opening_balance))
        .filter(
            B2BSalesOrder.status == "invoiced",
            B2BSalesOrder.invoice_date.is_(None),
            B2BSalesOrder.invoiced_at.is_(None),
            B2BSalesOrder.delivery_date.is_not(None),
            B2BSalesOrder.delivery_date < HISTORICAL_DELIVERY_DATE_LIMIT,
            B2BSalesOrder.observations.like(f"{HISTORICAL_IMPORT_PREFIX}%"),
        )
        .order_by(B2BSalesOrder.id.asc())
    )


def build_backfill_plan(db: Session) -> BackfillPlan:
    candidates: list[BackfillOrderEvidence] = []
    updatable: list[BackfillOrderEvidence] = []
    anomalies: list[BackfillOrderEvidence] = []

    for order in _historical_universe_query(db).all():
        opening_balance = order.ar_opening_balance
        override = opening_balance.invoice_date_override if opening_balance is not None else None
        is_conflict = override is not None and override != order.delivery_date
        evidence = BackfillOrderEvidence(
            order_id=order.id,
            order_number=order.order_number,
            delivery_date=order.delivery_date.isoformat(),
            created_at=order.created_at.isoformat(sep=" "),
            invoiced_at=order.invoiced_at.isoformat(sep=" ") if order.invoiced_at is not None else None,
            current_invoice_date=order.invoice_date.isoformat() if order.invoice_date is not None else None,
            proposed_invoice_date=order.delivery_date.isoformat(),
            invoice_date_override=override.isoformat() if override is not None else None,
            classification="anomaly" if is_conflict else "updatable",
            reason=(
                "invoice_date_override conflicts with delivery_date; excluded pending manual review."
                if is_conflict
                else (
                    "invoice_date_override matches delivery_date; safe to continue."
                    if override is not None
                    else "Historical CSV import eligible for invoice_date backfill."
                )
            ),
        )
        candidates.append(evidence)
        if is_conflict:
            anomalies.append(evidence)
        else:
            updatable.append(evidence)

    return BackfillPlan(
        candidates=tuple(candidates),
        updatable=tuple(updatable),
        anomalies=tuple(anomalies),
    )


def _table_snapshot(db: Session, table) -> dict[int, dict[str, object]]:
    rows = db.execute(table.select().order_by(table.c.id)).mappings().all()
    return {int(row["id"]): dict(row) for row in rows}


def _capture_protected_state(db: Session) -> dict[str, dict[int, dict[str, object]]]:
    return {
        "orders": _table_snapshot(db, B2BSalesOrder.__table__),
        "payments": _table_snapshot(db, B2BARPayment.__table__),
        "opening_balances": _table_snapshot(db, B2BAROpeningBalance.__table__),
    }


def _validate_execution(
    db: Session,
    *,
    plan: BackfillPlan,
    before: dict[str, dict[int, dict[str, object]]],
    updated_count: int,
) -> None:
    target_ids = {item.order_id for item in plan.updatable}
    if updated_count != len(target_ids):
        raise BackfillValidationError(
            f"Updated row count mismatch: expected {len(target_ids)}, got {updated_count}."
        )

    after = _capture_protected_state(db)
    if before["payments"] != after["payments"]:
        raise BackfillValidationError("Accounts receivable payments changed unexpectedly.")
    if before["opening_balances"] != after["opening_balances"]:
        raise BackfillValidationError("Accounts receivable opening balances changed unexpectedly.")
    if set(before["orders"]) != set(after["orders"]):
        raise BackfillValidationError("The set of B2B sales orders changed unexpectedly.")

    for order_id, before_row in before["orders"].items():
        after_row = after["orders"][order_id]
        if order_id not in target_ids:
            if before_row != after_row:
                raise BackfillValidationError(
                    f"Order id {order_id} outside the approved universe changed unexpectedly."
                )
            continue

        expected_invoice_date = before_row["delivery_date"]
        if after_row["invoice_date"] != expected_invoice_date:
            raise BackfillValidationError(
                f"Order id {order_id} did not receive delivery_date as invoice_date."
            )
        before_without_invoice_date = dict(before_row)
        after_without_invoice_date = dict(after_row)
        before_without_invoice_date.pop("invoice_date", None)
        after_without_invoice_date.pop("invoice_date", None)
        if before_without_invoice_date != after_without_invoice_date:
            raise BackfillValidationError(
                f"Order id {order_id} changed fields other than invoice_date."
            )

    remaining_plan = build_backfill_plan(db)
    remaining_ids = {item.order_id for item in remaining_plan.candidates}
    if target_ids & remaining_ids:
        raise BackfillValidationError("Updated orders remain in the eligible universe.")

    b2b1055 = (
        db.query(B2BSalesOrder)
        .filter(B2BSalesOrder.order_number == B2B1055_ORDER_NUMBER)
        .one_or_none()
    )
    if b2b1055 is not None:
        if b2b1055.invoice_date != B2B1055_EXPECTED_INVOICE_DATE:
            raise BackfillValidationError(
                "B2B1055 invoice_date is not 2026-04-30 after the backfill."
            )
        if b2b1055.created_at != before["orders"][b2b1055.id]["created_at"]:
            raise BackfillValidationError("B2B1055 created_at changed unexpectedly.")


def execute_backfill(
    session_factory: sessionmaker = SessionLocal,
) -> BackfillPlan:
    with session_factory() as db:
        with db.begin():
            plan = build_backfill_plan(db)
            before = _capture_protected_state(db)
            target_ids = [item.order_id for item in plan.updatable]
            updated_count = 0
            if target_ids:
                statement = text(
                    "UPDATE b2b_sales_orders "
                    "SET invoice_date = delivery_date "
                    "WHERE id IN :target_ids"
                ).bindparams(bindparam("target_ids", expanding=True))
                result = db.execute(statement, {"target_ids": target_ids})
                updated_count = int(result.rowcount or 0)
                db.expire_all()
            _validate_execution(
                db,
                plan=plan,
                before=before,
                updated_count=updated_count,
            )
            return plan


def dry_run_backfill(session_factory: sessionmaker = SessionLocal) -> BackfillPlan:
    with session_factory() as db:
        plan = build_backfill_plan(db)
        db.rollback()
        return plan


def _write_exports(export_dir: Path, plan: BackfillPlan) -> None:
    export_dir.mkdir(parents=True, exist_ok=True)
    summary = {
        "candidate_count": plan.candidate_count,
        "updatable_count": plan.updatable_count,
        "anomaly_count": plan.anomaly_count,
        "b2b1055": next(
            (asdict(item) for item in plan.candidates if item.order_number == B2B1055_ORDER_NUMBER),
            None,
        ),
    }
    (export_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    fieldnames = list(BackfillOrderEvidence.__dataclass_fields__)
    for file_name, rows in (
        ("orders_candidates.csv", plan.candidates),
        ("orders_updatable.csv", plan.updatable),
        ("orders_anomalies.csv", plan.anomalies),
    ):
        with (export_dir / file_name).open("w", newline="", encoding="utf-8-sig") as output:
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(asdict(row) for row in rows)


def _print_plan(plan: BackfillPlan, *, mode: str) -> None:
    print(f"mode: {mode}")
    print(f"orders_candidates: {plan.candidate_count}")
    print(f"orders_updatable: {plan.updatable_count}")
    print(f"orders_anomalies: {plan.anomaly_count}")
    for anomaly in plan.anomalies:
        print(
            "ANOMALY "
            f"order={anomaly.order_number} delivery_date={anomaly.delivery_date} "
            f"invoice_date_override={anomaly.invoice_date_override} reason={anomaly.reason}"
        )
    b2b1055 = next(
        (item for item in plan.candidates if item.order_number == B2B1055_ORDER_NUMBER),
        None,
    )
    if b2b1055 is None:
        print("B2B1055: not present in the eligible universe")
    else:
        print(
            "B2B1055: eligible "
            f"current_invoice_date={b2b1055.current_invoice_date} "
            f"proposed_invoice_date={b2b1055.proposed_invoice_date} "
            f"created_at={b2b1055.created_at} classification={b2b1055.classification}"
        )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Preview or backfill invoice_date for pre-June-2026 historical B2B CSV imports."
    )
    parser.add_argument("--execute", action="store_true", help="Execute the transactional backfill.")
    parser.add_argument("--confirm", default="", help="Exact confirmation required with --execute.")
    parser.add_argument("--export-dir", type=Path, help="Optional directory for CSV and JSON evidence.")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    if args.execute and args.confirm != CONFIRMATION_TEXT:
        print(
            "Execution blocked. Use --execute with "
            f"--confirm {CONFIRMATION_TEXT}"
        )
        return 2

    try:
        plan = execute_backfill() if args.execute else dry_run_backfill()
    except Exception as exc:
        print(f"Backfill failed and was rolled back: {exc}")
        return 1

    _print_plan(plan, mode="execute" if args.execute else "dry-run")
    if args.export_dir is not None:
        _write_exports(args.export_dir, plan)
        print(f"evidence_exported_to: {args.export_dir}")
    if args.execute:
        print("transaction_status: committed")
    else:
        print("transaction_status: rolled_back_read_only_dry_run")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
