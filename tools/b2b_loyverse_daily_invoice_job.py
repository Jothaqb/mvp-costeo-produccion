from __future__ import annotations

import argparse
import math
import os
import re
import sys
from datetime import date, datetime, time, timedelta, timezone, tzinfo
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlparse
from uuid import uuid4
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.models import B2BSalesOrder  # noqa: E402
from app.services.b2b_loyverse_invoice_service import (  # noqa: E402
    SYNC_STATUS_FAILED,
    SYNC_STATUS_SUCCESS,
    SYNC_STATUS_UNKNOWN,
    _recalculate_order_total,
)
from app.services.erp_loyverse_stock_preview_service import (  # noqa: E402
    _resolve_loyverse_store_id,
)
from tools.b2b_loyverse_monthly_invoice_job import (  # noqa: E402
    CSV_FIELDS,
    ERROR_FIELDS,
    Evaluation,
    decimal_text,
    evaluate_order as evaluate_monthly_order,
    load_orders,
    write_csv,
    write_json,
)


COSTA_RICA_TIMEZONE_NAME = "America/Costa_Rica"
DEFAULT_EXPORT_ROOT = "artifacts/b2b_loyverse_daily"
SUPPORTED_SYNC_STATUSES = {"", SYNC_STATUS_SUCCESS, SYNC_STATUS_FAILED, SYNC_STATUS_UNKNOWN}
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")


class BusinessDateError(ValueError):
    pass


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only daily readiness check for B2B ERP-to-Loyverse receipts. "
            "This Phase 1 job cannot execute receipt creation."
        )
    )
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument("--business-date", help="Explicit Costa Rica business date in YYYY-MM-DD format.")
    date_group.add_argument(
        "--auto-business-date-costa-rica",
        action="store_true",
        help=(
            "Resolve today during 22:00-23:59 Costa Rica or yesterday during "
            "00:00-03:00; abort outside that safety window."
        ),
    )
    parser.add_argument(
        "--use-env",
        action="store_true",
        help="Read DATABASE_URL and LOYVERSE_API_TOKEN from environment variables.",
    )
    parser.add_argument("--export-root", default=DEFAULT_EXPORT_ROOT)
    parser.add_argument("--run-id", default="", help=argparse.SUPPRESS)
    parser.add_argument(
        "--exclude-order-number",
        action="append",
        default=[],
        help="Repeat to classify a specific ERP order number as manually excluded.",
    )
    return parser.parse_args(argv)


def costa_rica_timezone() -> tzinfo:
    try:
        return ZoneInfo(COSTA_RICA_TIMEZONE_NAME)
    except ZoneInfoNotFoundError:
        return timezone(timedelta(hours=-6), name=COSTA_RICA_TIMEZONE_NAME)


def parse_business_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise BusinessDateError("--business-date must use YYYY-MM-DD format.") from exc


def resolve_automatic_business_date(now_cr: datetime) -> tuple[date, str]:
    local_time = now_cr.timetz().replace(tzinfo=None)
    if time(22, 0) <= local_time <= time(23, 59, 59, 999999):
        return now_cr.date(), "automatic_today_22_00_to_23_59_costa_rica"
    if time(0, 0) <= local_time <= time(3, 0):
        return now_cr.date() - timedelta(days=1), "automatic_yesterday_00_00_to_03_00_costa_rica"
    raise BusinessDateError(
        "Automatic business-date resolution is allowed only from 22:00 through 23:59 or "
        "from 00:00 through 03:00 America/Costa_Rica. Use --business-date YYYY-MM-DD."
    )


def resolve_business_date(args: argparse.Namespace, *, now_cr: datetime | None = None) -> tuple[date, str, datetime]:
    timezone_cr = costa_rica_timezone()
    effective_now = now_cr or datetime.now(timezone_cr)
    if effective_now.tzinfo is None:
        effective_now = effective_now.replace(tzinfo=timezone_cr)
    else:
        effective_now = effective_now.astimezone(timezone_cr)

    if args.business_date:
        return parse_business_date(args.business_date), "explicit_cli", effective_now
    business_date, source = resolve_automatic_business_date(effective_now)
    return business_date, source, effective_now


def validate_args(args: argparse.Namespace) -> None:
    if not args.use_env:
        raise SystemExit("--use-env is required; credentials are accepted only from environment variables.")
    args.exclude_order_number = sorted(
        {value.strip() for value in args.exclude_order_number if value and value.strip()}
    )
    args.run_id = args.run_id.strip()
    if args.run_id and not RUN_ID_PATTERN.fullmatch(args.run_id):
        raise SystemExit("--run-id may contain only letters, numbers, dot, underscore, and hyphen.")


def mask_database_component(value: str, *, fallback: str = "(not-applicable)") -> str:
    cleaned = (value or "").strip()
    if not cleaned:
        return fallback
    if len(cleaned) <= 4:
        return cleaned[:1] + "***"
    return cleaned[:2] + "***" + cleaned[-2:]


def require_environment() -> tuple[str, str, str]:
    missing = [
        name
        for name in ("DATABASE_URL", "LOYVERSE_API_TOKEN")
        if not os.getenv(name, "").strip()
    ]
    if missing:
        raise SystemExit("Missing required environment variables: " + ", ".join(missing))

    database_url = os.environ["DATABASE_URL"].strip()
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)
    parsed = urlparse(database_url)
    scheme = (parsed.scheme or "").lower()
    if not scheme:
        raise SystemExit("DATABASE_URL must be a valid SQLAlchemy database URL with a scheme.")
    database_name = (parsed.path or "").lstrip("/")
    database_info_masked = (
        f"scheme={scheme}; "
        f"host={mask_database_component(parsed.hostname or '')}; "
        f"database={mask_database_component(database_name)}"
    )
    return database_url, os.environ["LOYVERSE_API_TOKEN"].strip(), database_info_masked


def resolve_store_read_only(token: str) -> tuple[str, list[str], str]:
    try:
        store_id, warnings, error = _resolve_loyverse_store_id(token)
    except RuntimeError as exc:
        return "", [], f"Unable to resolve the Loyverse store using GET /stores: {exc}"
    if error:
        return "", warnings, error
    if not store_id:
        return "", warnings, "Loyverse store resolution returned no usable store id."
    return store_id, warnings, ""


def generate_run_id(now_utc: datetime | None = None) -> str:
    effective_now = now_utc or datetime.now(timezone.utc)
    return f"{effective_now.strftime('%Y%m%dT%H%M%S%fZ')}-{uuid4().hex[:8]}"


def prepare_run_dir(export_root: str, business_date: date, run_id: str) -> Path:
    effective_run_id = run_id or generate_run_id()
    if not RUN_ID_PATTERN.fullmatch(effective_run_id):
        raise SystemExit("Generated run_id contains unsupported characters.")
    run_dir = Path(export_root) / business_date.isoformat() / effective_run_id
    if run_dir.exists():
        raise SystemExit(f"Run evidence directory already exists; refusing to overwrite: {run_dir}")
    run_dir.mkdir(parents=True, exist_ok=False)
    return run_dir


def evaluation_base(order: B2BSalesOrder) -> dict[str, object]:
    return {
        "order_id": order.id,
        "order_number": order.order_number,
        "delivery_date": order.delivery_date,
        "erp_status": order.status,
        "total_amount": _recalculate_order_total(order),
        "loyverse_receipt_id": (order.loyverse_receipt_id or "").strip(),
        "loyverse_receipt_number": (order.loyverse_receipt_number or "").strip(),
        "loyverse_sync_status": (order.loyverse_invoice_sync_status or "").strip().lower(),
        "payload_fingerprint": "",
        "payload": None,
        "variant_snapshots": {},
    }


def terminal_evaluation(order: B2BSalesOrder, classification: str, reason: str) -> Evaluation:
    return Evaluation(
        **evaluation_base(order),
        classification=classification,
        eligible=False,
        reason=reason,
    )


def payload_validation_error(payload: object, expected_store_id: str) -> str:
    if not isinstance(payload, dict):
        return "Payload is not a JSON object."
    if payload.get("store_id") != expected_store_id:
        return "Payload store_id is missing or inconsistent with the resolved store."
    if not str(payload.get("customer_id") or "").strip():
        return "Payload customer_id is missing."
    line_items = payload.get("line_items")
    if not isinstance(line_items, list) or not line_items:
        return "Payload must contain at least one line item."
    for index, line in enumerate(line_items, start=1):
        if not isinstance(line, dict) or not str(line.get("variant_id") or "").strip():
            return f"Payload line {index} has no variant_id."
        try:
            quantity = float(line.get("quantity"))
            price = float(line.get("price"))
        except (TypeError, ValueError):
            return f"Payload line {index} has invalid quantity or price."
        if not math.isfinite(quantity) or not math.isfinite(price) or quantity <= 0 or price < 0:
            return f"Payload line {index} has invalid quantity or price."
    payments = payload.get("payments")
    if not isinstance(payments, list) or len(payments) != 1 or not isinstance(payments[0], dict):
        return "Payload must contain exactly one payment."
    payment = payments[0]
    if not str(payment.get("payment_type_id") or "").strip():
        return "Payload payment_type_id is missing."
    try:
        amount = float(payment.get("money_amount"))
    except (TypeError, ValueError):
        return "Payload payment amount is invalid."
    if not math.isfinite(amount) or amount <= 0:
        return "Payload payment amount must be greater than zero."
    return ""


def evaluate_daily_order(
    db: Session,
    order: B2BSalesOrder,
    store_id: str,
    *,
    excluded_order_numbers: set[str],
    store_error: str = "",
) -> Evaluation:
    if order.order_number in excluded_order_numbers:
        return terminal_evaluation(
            order,
            "excluded_manual",
            "Manually excluded by --exclude-order-number from this daily readiness check.",
        )

    base = evaluation_base(order)
    receipt_reference_present = bool(base["loyverse_receipt_id"] or base["loyverse_receipt_number"])
    sync_status = str(base["loyverse_sync_status"])

    if sync_status == SYNC_STATUS_UNKNOWN:
        return terminal_evaluation(
            order,
            "blocked",
            "Previous Loyverse result is unknown; manual reconciliation is required and automatic retry is forbidden.",
        )
    if sync_status == SYNC_STATUS_FAILED:
        return terminal_evaluation(
            order,
            "blocked",
            "Previous Loyverse attempt failed; manual review is required and this daily job will not retry it.",
        )
    if sync_status == SYNC_STATUS_SUCCESS and not receipt_reference_present:
        return terminal_evaluation(
            order,
            "blocked",
            "Loyverse sync status is success but no local receipt reference is stored; manual reconciliation is required.",
        )
    if sync_status not in SUPPORTED_SYNC_STATUSES:
        return terminal_evaluation(
            order,
            "blocked",
            f"Unsupported or inconsistent Loyverse sync status: {sync_status}.",
        )
    if receipt_reference_present:
        return terminal_evaluation(
            order,
            "already_sent",
            "A Loyverse receipt reference is already stored locally.",
        )
    if store_error:
        return terminal_evaluation(order, "blocked", store_error)

    evaluated = evaluate_monthly_order(db, order, store_id, excluded_order_numbers=set())
    if evaluated.eligible:
        payload_error = payload_validation_error(evaluated.payload, store_id)
        if payload_error:
            return Evaluation(
                **evaluation_base(order),
                classification="blocked",
                eligible=False,
                reason=payload_error,
                missing_customer_mapping=evaluated.missing_customer_mapping,
                missing_variant_mapping=evaluated.missing_variant_mapping,
                missing_payment_type_mapping=evaluated.missing_payment_type_mapping,
            )
    return evaluated


def evaluate_daily_order_safely(
    db: Session,
    order: B2BSalesOrder,
    store_id: str,
    *,
    excluded_order_numbers: set[str],
    store_error: str = "",
) -> Evaluation:
    try:
        return evaluate_daily_order(
            db,
            order,
            store_id,
            excluded_order_numbers=excluded_order_numbers,
            store_error=store_error,
        )
    except Exception as exc:
        return terminal_evaluation(
            order,
            "error",
            f"Unexpected read-only evaluation error ({type(exc).__name__}); inspect Render logs.",
        )


def build_summary(
    *,
    business_date: date,
    business_date_source: str,
    now_cr: datetime,
    run_id: str,
    database_info_masked: str,
    store_warnings: list[str],
    global_blockers: list[str],
    evaluations: list[Evaluation],
) -> dict[str, object]:
    eligible = [item for item in evaluations if item.classification == "eligible"]
    blocked = [item for item in evaluations if item.classification == "blocked"]
    errors = [item for item in evaluations if item.classification == "error"]
    no_op = not eligible and not blocked and not errors and not global_blockers
    return {
        "run_id": run_id,
        "mode": "dry-run",
        "phase": "daily_phase_1_read_only",
        "business_date": business_date.isoformat(),
        "business_date_source": business_date_source,
        "range_start": business_date.isoformat(),
        "range_end_exclusive": (business_date + timedelta(days=1)).isoformat(),
        "date_field": "B2BSalesOrder.delivery_date",
        "interval": "delivery_date >= business_date and delivery_date < business_date + 1 day",
        "timezone": COSTA_RICA_TIMEZONE_NAME,
        "resolved_at_costa_rica": now_cr.isoformat(),
        "orders_found": len(evaluations),
        "orders_eligible": len(eligible),
        "orders_blocked": len(blocked),
        "orders_errors": len(errors),
        "orders_already_sent": sum(item.classification == "already_sent" for item in evaluations),
        "orders_excluded_manual": sum(item.classification == "excluded_manual" for item in evaluations),
        "excluded_order_numbers": [
            item.order_number for item in evaluations if item.classification == "excluded_manual"
        ],
        "orders_unknown": sum(item.loyverse_sync_status == SYNC_STATUS_UNKNOWN for item in evaluations),
        "orders_failed": sum(item.loyverse_sync_status == SYNC_STATUS_FAILED for item in evaluations),
        "total_amount_eligible": decimal_text(sum((item.total_amount for item in eligible), Decimal("0"))),
        "missing_customer_mappings": sum(item.missing_customer_mapping for item in evaluations),
        "missing_variant_mappings": sum(item.missing_variant_mapping for item in evaluations),
        "missing_payment_type_mappings": sum(item.missing_payment_type_mapping for item in evaluations),
        "global_blockers": global_blockers,
        "warnings": store_warnings,
        "no_op_success": no_op,
        "database_info_masked": database_info_masked,
        "notes": [
            "This Phase 1 job is read-only and has no execute mode.",
            "No Loyverse receipts are created.",
            "Database work is rolled back and the session is closed after evaluation.",
            "A receipt number without a receipt id is accepted as a valid local reference.",
        ],
    }


def write_reports(run_dir: Path, evaluations: list[Evaluation], summary: dict[str, object]) -> None:
    rows = [item.csv_row() for item in evaluations]
    write_csv(run_dir / "orders_preview.csv", CSV_FIELDS, rows)
    write_csv(
        run_dir / "orders_eligible.csv",
        CSV_FIELDS,
        [item.csv_row() for item in evaluations if item.classification == "eligible"],
    )
    write_csv(
        run_dir / "orders_already_sent.csv",
        CSV_FIELDS,
        [item.csv_row() for item in evaluations if item.classification == "already_sent"],
    )
    write_csv(
        run_dir / "orders_blocked.csv",
        CSV_FIELDS,
        [item.csv_row() for item in evaluations if item.classification == "blocked"],
    )
    write_csv(
        run_dir / "orders_excluded.csv",
        CSV_FIELDS,
        [item.csv_row() for item in evaluations if item.classification == "excluded_manual"],
    )
    write_csv(
        run_dir / "errors.csv",
        ERROR_FIELDS,
        [
            {
                "order_id": item.order_id,
                "order_number": item.order_number,
                "stage": "daily_dry_run_validation",
                "classification": item.classification,
                "error": item.reason,
            }
            for item in evaluations
            if item.classification in {"blocked", "error"}
        ],
    )
    write_json(
        run_dir / "payloads_preview.json",
        [
            {
                "order_id": item.order_id,
                "order_number": item.order_number,
                "payload_fingerprint": item.payload_fingerprint,
                "payload": item.payload,
            }
            for item in evaluations
            if item.eligible
        ],
    )
    write_json(run_dir / "summary.json", summary)


def run(args: argparse.Namespace, *, now_cr: datetime | None = None) -> tuple[dict[str, object], Path, int]:
    validate_args(args)
    try:
        business_date, business_date_source, effective_now_cr = resolve_business_date(args, now_cr=now_cr)
    except BusinessDateError as exc:
        raise SystemExit(str(exc)) from exc
    run_dir = prepare_run_dir(args.export_root, business_date, args.run_id)
    run_id = run_dir.name
    database_url, token, database_info_masked = require_environment()
    store_id, store_warnings, store_error = resolve_store_read_only(token)
    global_blockers = [store_error] if store_error else []

    engine = create_engine(database_url)
    session_factory = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db: Session = session_factory()
    evaluations: list[Evaluation] = []
    try:
        end_date_exclusive = business_date + timedelta(days=1)
        orders = load_orders(db, business_date, end_date_exclusive)
        evaluations = [
            evaluate_daily_order_safely(
                db,
                order,
                store_id,
                excluded_order_numbers=set(args.exclude_order_number),
                store_error=store_error,
            )
            for order in orders
        ]
    finally:
        db.rollback()
        db.close()

    summary = build_summary(
        business_date=business_date,
        business_date_source=business_date_source,
        now_cr=effective_now_cr,
        run_id=run_id,
        database_info_masked=database_info_masked,
        store_warnings=store_warnings,
        global_blockers=global_blockers,
        evaluations=evaluations,
    )
    write_reports(run_dir, evaluations, summary)

    has_blockers = bool(summary["orders_blocked"] or summary["orders_errors"] or global_blockers)
    exit_code = 2 if has_blockers else 0
    print(
        f"Mode=dry-run business_date={business_date.isoformat()} found={summary['orders_found']} "
        f"eligible={summary['orders_eligible']} blocked={summary['orders_blocked']} "
        f"errors={summary['orders_errors']} already_sent={summary['orders_already_sent']}"
    )
    if summary["no_op_success"]:
        print("No-op success: no eligible orders and no blockers were found.")
    print(f"Evidence exported to: {run_dir}")
    for warning in store_warnings:
        print(f"WARNING: {warning}")
    for blocker in global_blockers:
        print(f"BLOCKED: {blocker}")
    print("Dry-run complete. No receipt creation was attempted and no database changes were committed.")
    return summary, run_dir, exit_code


def main() -> None:
    _, _, exit_code = run(parse_args())
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
