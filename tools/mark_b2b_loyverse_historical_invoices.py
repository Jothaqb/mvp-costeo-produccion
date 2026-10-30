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
    EXPLICIT_ID_CONFIRMATION_TEXT,
    EXPLICIT_ID_EXECUTION_CONFIRMATION,
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
    parser.add_argument(
        "--approved-order-ids-file",
        type=Path,
        help="CSV con encabezado order_id para la excepción histórica por lista exacta.",
    )
    return parser


def _load_explicit_order_ids(path: Path | None) -> tuple[int, ...]:
    if path is None:
        return ()
    if not path.is_file():
        raise B2BLoyverseHistoricalConfirmationError(
            f"No existe el archivo de IDs aprobados: {path}"
        )
    with path.open(encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        if reader.fieldnames is None or "order_id" not in reader.fieldnames:
            raise B2BLoyverseHistoricalConfirmationError(
                "El archivo aprobado debe ser CSV y contener el encabezado order_id."
            )
        raw_ids = [(row.get("order_id") or "").strip() for row in reader]
    if not raw_ids or any(not value.isdigit() or int(value) <= 0 for value in raw_ids):
        raise B2BLoyverseHistoricalConfirmationError(
            "El archivo aprobado debe contener únicamente IDs enteros positivos."
        )
    parsed_ids = [int(value) for value in raw_ids]
    if len(parsed_ids) != len(set(parsed_ids)):
        raise B2BLoyverseHistoricalConfirmationError(
            "El archivo aprobado contiene IDs duplicados."
        )
    return tuple(sorted(parsed_ids))


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
        "execution_scope": "explicit_ids" if plan.explicit_order_ids else "standard",
        "explicit_order_ids": list(plan.explicit_order_ids),
        "missing_or_not_invoiced_explicit_order_ids": list(plan.missing_explicit_order_ids),
        "explicit_selection_is_valid": plan.explicit_selection_is_valid,
        "explicit_id_candidate_count": plan.explicit_id_candidate_count,
        "explicit_rejected_count": plan.explicit_rejected_count,
        "total_invoiced_orders_reviewed": len(plan.rows),
        "candidate_count": plan.candidate_count,
        "migrated_import_candidate_count": plan.migrated_import_candidate_count,
        "dated_historical_candidate_count": plan.dated_historical_candidate_count,
        "already_historical_count": plan.already_historical_count,
        "already_sent_count": plan.already_sent_count,
        "outside_cutoff_count": plan.outside_cutoff_count,
        "review_count": plan.review_count,
        "fingerprint": plan.fingerprint,
        "business_confirmation_required_for_execute": (
            EXPLICIT_ID_CONFIRMATION_TEXT
            if plan.explicit_order_ids
            else HISTORICAL_CONFIRMATION_TEXT
        ),
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
        "explicitly_approved",
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
        (
            "orders_already_sent.csv",
            (row for row in plan.rows if row.classification == "already_sent"),
        ),
        (
            "orders_explicit_id_candidates.csv",
            (row for row in plan.rows if row.classification == "candidate_explicit_id"),
        ),
        (
            "orders_explicit_id_rejected.csv",
            (
                row
                for row in plan.rows
                if row.explicitly_approved
                and row.classification
                not in {"candidate_explicit_id", "already_historical"}
            ),
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
        explicit_order_ids = _load_explicit_order_ids(args.approved_order_ids_file)
        if args.execute:
            required_execution_confirmation = (
                EXPLICIT_ID_EXECUTION_CONFIRMATION
                if explicit_order_ids
                else EXECUTION_CONFIRMATION
            )
            if args.confirm != required_execution_confirmation:
                raise B2BLoyverseHistoricalConfirmationError(
                    f"Execute requiere --confirm {required_execution_confirmation}."
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
                explicit_order_ids=explicit_order_ids,
            )
            print(f"mode: execute\nupdated: {plan.candidate_count}\ntransaction_status: committed")
            return 0

        if args.export_dir is None:
            raise B2BLoyverseHistoricalConfirmationError(
                "Dry-run requiere --export-dir con un directorio nuevo."
            )
        with SessionLocal() as db:
            plan = build_historical_confirmation_plan(
                db,
                explicit_order_ids=explicit_order_ids,
            )
            db.rollback()
        _write_evidence(args.export_dir, plan)
        print("mode: dry-run")
        print(f"cutoff_date_exclusive: {HISTORICAL_CONFIRMATION_CUTOFF.isoformat()}")
        print(f"candidate_count: {plan.candidate_count}")
        print(f"execution_scope: {'explicit_ids' if plan.explicit_order_ids else 'standard'}")
        print(f"explicit_order_ids: {','.join(map(str, plan.explicit_order_ids))}")
        print(f"explicit_selection_is_valid: {str(plan.explicit_selection_is_valid).lower()}")
        print(f"explicit_rejected_count: {plan.explicit_rejected_count}")
        print(f"already_sent_count: {plan.already_sent_count}")
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
