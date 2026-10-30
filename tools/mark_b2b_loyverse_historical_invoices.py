from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import asdict
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import app.models  # noqa: E402,F401
from app.database import SessionLocal  # noqa: E402
from app.services.b2b_loyverse_historical_confirmation_service import (  # noqa: E402
    EXECUTION_CONFIRMATION,
    HISTORICAL_CONFIRMATION_CUTOFF,
    HISTORICAL_CONFIRMATION_TEXT,
    B2BLoyverseHistoricalConfirmationError,
    build_historical_confirmation_plan,
    execute_historical_confirmation,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Dry-run or explicitly confirm that reliable pre-2026-10-01 B2B ERP invoices "
            "and technically identified Historical CSV imports were already invoiced in "
            "Loyverse. Dry-run is the default."
        )
    )
    parser.add_argument("--export-dir", type=Path)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm", default="")
    parser.add_argument("--confirmed-by-user-id", type=int)
    parser.add_argument("--plan-fingerprint", default="")
    parser.add_argument("--business-confirmation", default="")
    return parser


def _write_evidence(export_dir: Path, plan) -> None:
    if export_dir.exists() and any(export_dir.iterdir()):
        raise B2BLoyverseHistoricalConfirmationError(
            f"El directorio de evidencia no está vacío: {export_dir}"
        )
    export_dir.mkdir(parents=True, exist_ok=True)
    summary = {
        "mode": "dry-run",
        "cutoff_date_exclusive": plan.cutoff_date.isoformat(),
        "invoice_date_policy": ["invoice_date", "invoiced_at"],
        "delivery_date_accepted": False,
        "migrated_import_policy": "observations starts with exact internal prefix Historical CSV import",
        "null_date_alone_accepted": False,
        "total_invoiced_orders_reviewed": len(plan.rows),
        "candidate_count": plan.candidate_count,
        "migrated_import_candidate_count": plan.migrated_import_candidate_count,
        "dated_historical_candidate_count": plan.dated_historical_candidate_count,
        "already_historical_count": plan.already_historical_count,
        "outside_cutoff_count": plan.outside_cutoff_count,
        "review_count": plan.review_count,
        "fingerprint": plan.fingerprint,
        "business_confirmation_required_for_execute": HISTORICAL_CONFIRMATION_TEXT,
        "writes_performed": False,
    }
    (export_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    fieldnames = list(plan.rows[0].__dataclass_fields__) if plan.rows else [
        "order_id",
        "order_number",
        "invoice_date",
        "invoice_date_source",
        "delivery_date",
        "current_sync_status",
        "receipt_id",
        "receipt_number",
        "classification",
        "reason",
    ]
    with (export_dir / "orders.csv").open("w", newline="", encoding="utf-8-sig") as output:
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(asdict(row) for row in plan.rows)
    separated_exports = (
        (
            "orders_migrated_identified.csv",
            (row for row in plan.rows if row.classification == "candidate_migrated_import"),
        ),
        (
            "orders_historical_normal.csv",
            (row for row in plan.rows if row.classification == "candidate_dated_historical"),
        ),
        (
            "orders_review.csv",
            (row for row in plan.rows if row.classification.startswith("review_")),
        ),
        (
            "orders_from_october.csv",
            (row for row in plan.rows if row.classification == "outside_cutoff"),
        ),
    )
    for file_name, rows in separated_exports:
        with (export_dir / file_name).open("w", newline="", encoding="utf-8-sig") as output:
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(asdict(row) for row in rows)


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.execute:
            if args.confirm != EXECUTION_CONFIRMATION:
                raise B2BLoyverseHistoricalConfirmationError(
                    f"Execute requiere --confirm {EXECUTION_CONFIRMATION}."
                )
            if args.confirmed_by_user_id is None:
                raise B2BLoyverseHistoricalConfirmationError(
                    "Execute requiere --confirmed-by-user-id."
                )
            plan = execute_historical_confirmation(
                SessionLocal,
                confirmed_by_user_id=args.confirmed_by_user_id,
                expected_fingerprint=args.plan_fingerprint,
                business_confirmation=args.business_confirmation,
            )
            print(f"mode: execute\nupdated: {plan.candidate_count}\ntransaction_status: committed")
            return 0

        if args.export_dir is None:
            raise B2BLoyverseHistoricalConfirmationError(
                "Dry-run requiere --export-dir con un directorio nuevo."
            )
        with SessionLocal() as db:
            plan = build_historical_confirmation_plan(db)
            db.rollback()
        _write_evidence(args.export_dir, plan)
        print("mode: dry-run")
        print(f"cutoff_date_exclusive: {HISTORICAL_CONFIRMATION_CUTOFF.isoformat()}")
        print(f"candidate_count: {plan.candidate_count}")
        print(f"migrated_import_candidate_count: {plan.migrated_import_candidate_count}")
        print(f"dated_historical_candidate_count: {plan.dated_historical_candidate_count}")
        print(f"already_historical_count: {plan.already_historical_count}")
        print(f"review_count: {plan.review_count}")
        print(f"fingerprint: {plan.fingerprint}")
        print("transaction_status: rolled_back_read_only_dry_run")
        print(f"evidence_exported_to: {args.export_dir}")
        return 0
    except Exception as exc:
        print(f"Historical confirmation failed and no changes were committed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
