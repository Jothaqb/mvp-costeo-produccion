from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import fields
from datetime import date, datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import app.models  # noqa: F401
from app.database import SessionLocal
from app.services.b2b_commercial_inactivity_service import (
    B2BCommercialInactivityError,
    B2BCommercialInactivityPlan,
    CatalogProductInactivityEvidence,
    CHANNEL_SCOPE_REVISION,
    CustomerInactivityEvidence,
    EXECUTION_CONFIRMATION,
    HistoryCoverageEvidence,
    InactivityConfig,
    InactivityExecutionResult,
    build_b2b_commercial_inactivity_plan,
    current_costa_rica_date,
    evidence_dict,
    execute_b2b_commercial_inactivity_plan,
    plan_summary,
)


def _parse_date(value: str, *, field_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"{field_name} must use YYYY-MM-DD format."
        ) from exc


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Preview or execute inactivity deactivation for B2B customer catalogs and customers. "
            "Dry-run is the default and never writes ERP data."
        )
    )
    parser.add_argument(
        "--as-of-date",
        type=lambda value: _parse_date(value, field_name="--as-of-date"),
        help="Costa Rica report date. Defaults to today's Costa Rica date.",
    )
    parser.add_argument(
        "--history-complete-since",
        type=lambda value: _parse_date(value, field_name="--history-complete-since"),
        help=(
            "Operator assertion that B2B invoiced history is complete since this date. "
            "Execute requires a date on or before the 12-month cutoff."
        ),
    )
    parser.add_argument(
        "--reactivation-grace-days",
        type=int,
        default=30,
        help="Conservative grace after a catalog/customer update or reactivation (default: 30).",
    )
    parser.add_argument(
        "--export-dir",
        type=Path,
        required=True,
        help="New or empty directory for JSON/CSV evidence.",
    )
    parser.add_argument("--execute", action="store_true", help="Apply the approved deactivations.")
    parser.add_argument("--confirm", default="", help="Exact literal required with --execute.")
    parser.add_argument(
        "--approved-summary",
        type=Path,
        help="summary.json from the exact dry-run approved for execution.",
    )
    parser.add_argument(
        "--actor-username",
        default="",
        help="Active ERP admin recorded in audit logs; required with --execute.",
    )
    return parser


def _validate_args(args: argparse.Namespace) -> None:
    if args.reactivation_grace_days < 0:
        raise B2BCommercialInactivityError("--reactivation-grace-days cannot be negative.")
    if args.execute:
        if args.confirm != EXECUTION_CONFIRMATION:
            raise B2BCommercialInactivityError(
                f"Execute requires --confirm {EXECUTION_CONFIRMATION}."
            )
        if args.approved_summary is None:
            raise B2BCommercialInactivityError("Execute requires --approved-summary.")
        if not args.actor_username.strip():
            raise B2BCommercialInactivityError("Execute requires --actor-username.")
    elif args.confirm or args.approved_summary is not None or args.actor_username.strip():
        raise B2BCommercialInactivityError(
            "--confirm, --approved-summary and --actor-username are valid only with --execute."
        )


def _prepare_export_dir(export_dir: Path) -> None:
    if export_dir.exists() and any(export_dir.iterdir()):
        raise B2BCommercialInactivityError(
            f"Export directory is not empty; no files were overwritten: {export_dir}"
        )
    export_dir.mkdir(parents=True, exist_ok=True)


def _load_approved_fingerprint(summary_path: Path) -> str:
    try:
        payload = json.loads(summary_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise B2BCommercialInactivityError(
            f"Could not read approved dry-run summary: {summary_path}"
        ) from exc
    if payload.get("channel_scope_revision") != CHANNEL_SCOPE_REVISION:
        raise B2BCommercialInactivityError(
            "Approved dry-run summary uses an obsolete or missing channel scope revision. "
            "Generate and approve a new dry-run."
        )
    fingerprint = str(payload.get("plan_fingerprint") or "").strip()
    if not fingerprint:
        raise B2BCommercialInactivityError(
            "Approved dry-run summary does not contain plan_fingerprint."
        )
    return fingerprint


def _dry_run(config: InactivityConfig) -> B2BCommercialInactivityPlan:
    with SessionLocal() as db:
        try:
            plan = build_b2b_commercial_inactivity_plan(db, config=config)
        finally:
            db.rollback()
        return plan


def _write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8-sig") as output:
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            normalized = dict(row)
            for key, value in normalized.items():
                if isinstance(value, list):
                    normalized[key] = " | ".join(str(item) for item in value)
            writer.writerow(normalized)


def _write_evidence(
    export_dir: Path,
    *,
    plan: B2BCommercialInactivityPlan,
    mode: str,
    execution_result: InactivityExecutionResult | None = None,
) -> None:
    summary = plan_summary(plan)
    summary.update(
        {
            "mode": mode,
            "evidence_generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "changes_applied": len(execution_result.changes) if execution_result else 0,
        }
    )
    (export_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    catalog_rows = [evidence_dict(row) for row in plan.catalog_products]
    customer_rows = [evidence_dict(row) for row in plan.customers]
    _write_csv(
        export_dir / "catalog_products.csv",
        catalog_rows,
        [field.name for field in fields(CatalogProductInactivityEvidence)],
    )
    _write_csv(
        export_dir / "customers.csv",
        customer_rows,
        [field.name for field in fields(CustomerInactivityEvidence)],
    )
    _write_csv(
        export_dir / "history_coverage.csv",
        [evidence_dict(row) for row in plan.history_coverage],
        [field.name for field in fields(HistoryCoverageEvidence)],
    )

    review_rows = [
        {"entity_type": "b2b_customer_product", **row}
        for row in catalog_rows
        if row["classification"] == "review_pending"
    ] + [
        {"entity_type": "b2b_customer", **row}
        for row in customer_rows
        if row["classification"] == "review_pending"
    ]
    excluded_rows = [
        {"entity_type": "b2b_customer_product", **row}
        for row in catalog_rows
        if row["classification"] == "excluded"
    ] + [
        {"entity_type": "b2b_customer", **row}
        for row in customer_rows
        if row["classification"] == "excluded"
    ]
    combined_fields = sorted(
        {key for row in (*review_rows, *excluded_rows) for key in row},
        key=lambda item: (item != "entity_type", item),
    )
    _write_csv(export_dir / "review_pending.csv", review_rows, combined_fields)
    _write_csv(export_dir / "excluded.csv", excluded_rows, combined_fields)

    if execution_result is not None:
        change_rows = [evidence_dict(change) for change in execution_result.changes]
        change_fields = list(change_rows[0]) if change_rows else [
            "entity_type",
            "entity_id",
            "customer_id",
            "customer_name",
            "sku",
            "active_before",
            "active_after",
            "reason",
        ]
        _write_csv(export_dir / "execution_results.csv", change_rows, change_fields)


def _print_summary(plan: B2BCommercialInactivityPlan, *, mode: str) -> None:
    print(f"mode: {mode}")
    print(f"as_of_date: {plan.config.as_of_date.isoformat()}")
    print(f"cutoff_date: {plan.config.cutoff_date.isoformat()}")
    print(f"history_is_sufficient: {str(plan.config.history_is_sufficient).lower()}")
    print(f"channel_scope_is_valid: {str(plan.channel_scope_is_valid).lower()}")
    print(f"customers_registered_total: {plan.registered_customers_total}")
    print(f"customers_in_authorized_channels: {len(plan.customers)}")
    print(f"customers_excluded_other_channels: {plan.customers_excluded_other_channels}")
    print(f"customers_without_channel: {plan.customers_without_channel}")
    print(f"catalog_products_evaluated_in_scope: {len(plan.catalog_products)}")
    print(f"catalog_products_outside_scope: {plan.catalog_products_outside_scope}")
    print(f"catalog_products_eligible: {len(plan.eligible_catalog_products)}")
    print(f"customers_eligible: {len(plan.eligible_customers)}")
    print(f"review_pending: {plan.review_pending_count}")
    print(f"excluded: {plan.excluded_count}")
    print(f"plan_fingerprint: {plan.plan_fingerprint}")


def main() -> int:
    args = _build_parser().parse_args()
    try:
        _validate_args(args)
        _prepare_export_dir(args.export_dir)
        config = InactivityConfig(
            as_of_date=args.as_of_date or current_costa_rica_date(),
            history_complete_since=args.history_complete_since,
            reactivation_grace_days=args.reactivation_grace_days,
        )
        if args.execute:
            approved_fingerprint = _load_approved_fingerprint(args.approved_summary)
            result = execute_b2b_commercial_inactivity_plan(
                SessionLocal,
                config=config,
                approved_plan_fingerprint=approved_fingerprint,
                actor_username=args.actor_username.strip(),
                confirmation=args.confirm,
            )
            _write_evidence(
                args.export_dir,
                plan=result.plan,
                mode="execute",
                execution_result=result,
            )
            _print_summary(result.plan, mode="execute")
            print(f"changes_applied: {len(result.changes)}")
            print("transaction_status: committed")
        else:
            plan = _dry_run(config)
            _write_evidence(args.export_dir, plan=plan, mode="dry-run")
            _print_summary(plan, mode="dry-run")
            print("transaction_status: read_only_rollback")
        print(f"evidence_exported_to: {args.export_dir}")
        return 0
    except Exception as exc:
        print(f"B2B commercial inactivity job failed: {exc}")
        print("transaction_status: no_commit_or_rolled_back")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
