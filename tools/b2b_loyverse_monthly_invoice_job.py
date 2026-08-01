from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta, timezone as fixed_timezone, tzinfo
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, joinedload, sessionmaker


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.models import B2BSalesOrder  # noqa: E402
from app.services.b2b_loyverse_invoice_service import (  # noqa: E402
    B2BLoyverseInvoiceError,
    LoyverseReceiptUnknownError,
    SYNC_STATUS_FAILED,
    SYNC_STATUS_SUCCESS,
    SYNC_STATUS_UNKNOWN,
    _build_line_payloads,
    _build_receipt_payload,
    _create_loyverse_receipt,
    _extract_receipt,
    _recalculate_order_total,
    _resolve_customer_id,
    _resolve_payment_type_id,
    _resolve_variant_id,
    _string_value,
)
from app.services.erp_loyverse_stock_preview_service import (  # noqa: E402
    _resolve_loyverse_store_id,
)


EXECUTE_CONFIRMATION = "SEND_B2B_JULY_2026_TO_LOYVERSE"
EXECUTE_START_DATE = date(2026, 7, 1)
EXECUTE_END_DATE_EXCLUSIVE = date(2026, 8, 1)
EXECUTE_TIMEZONE = "America/Costa_Rica"
SUPPORTED_ORDER_STATUSES = {"draft", "in_process", "invoiced"}
CSV_FIELDS = [
    "order_id",
    "order_number",
    "delivery_date",
    "erp_status",
    "classification",
    "eligible",
    "total_amount",
    "payload_fingerprint",
    "loyverse_receipt_id",
    "loyverse_receipt_number",
    "loyverse_sync_status",
    "used_payment_type_fallback",
    "reason",
]
ERROR_FIELDS = ["order_id", "order_number", "stage", "classification", "error"]
EXECUTION_FIELDS = [
    "order_id",
    "order_number",
    "result",
    "total_amount",
    "loyverse_receipt_id",
    "loyverse_receipt_number",
    "payload_fingerprint",
    "message",
    "processed_at",
]


@dataclass
class Evaluation:
    order_id: int
    order_number: str
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
    payload: dict[str, object] | None
    variant_snapshots: dict[int, str]
    used_payment_type_fallback: bool = False
    missing_customer_mapping: bool = False
    missing_variant_mapping: bool = False
    missing_payment_type_mapping: bool = False

    def csv_row(self) -> dict[str, object]:
        return {
            "order_id": self.order_id,
            "order_number": self.order_number,
            "delivery_date": self.delivery_date.isoformat(),
            "erp_status": self.erp_status,
            "classification": self.classification,
            "eligible": self.eligible,
            "total_amount": decimal_text(self.total_amount),
            "payload_fingerprint": self.payload_fingerprint,
            "loyverse_receipt_id": self.loyverse_receipt_id,
            "loyverse_receipt_number": self.loyverse_receipt_number,
            "loyverse_sync_status": self.loyverse_sync_status,
            "used_payment_type_fallback": self.used_payment_type_fallback,
            "reason": self.reason,
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Emergency B2B ERP-to-Loyverse monthly receipt job. "
            "Dry-run is the default; POST requests require --execute and exact confirmation."
        )
    )
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date-exclusive", required=True)
    parser.add_argument("--timezone", required=True)
    parser.add_argument("--export-dir", required=True)
    parser.add_argument(
        "--use-env",
        action="store_true",
        help=(
            "Use DATABASE_URL and LOYVERSE_API_TOKEN from the environment. "
            "LOYVERSE_STORE_ID is optional when the API returns exactly one store."
        ),
    )
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm", default="")
    parser.add_argument("--loyverse-payment-type-id", default="")
    parser.add_argument("--loyverse-payment-type-name", default="")
    return parser.parse_args()


def parse_date(value: str, flag: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise SystemExit(f"{flag} must use YYYY-MM-DD format.") from exc


def resolve_timezone(value: str) -> tzinfo:
    try:
        return ZoneInfo(value)
    except ZoneInfoNotFoundError:
        # Windows Python installations do not always include the IANA tzdata
        # package. Costa Rica is explicitly fixed at UTC-06:00 for this job.
        if value == "America/Costa_Rica":
            return fixed_timezone(timedelta(hours=-6), name=value)
        raise SystemExit(f"Unknown timezone: {value}")


def validate_args(args: argparse.Namespace) -> tuple[date, date, tzinfo]:
    start_date = parse_date(args.start_date, "--start-date")
    end_date_exclusive = parse_date(args.end_date_exclusive, "--end-date-exclusive")
    if end_date_exclusive <= start_date:
        raise SystemExit("--end-date-exclusive must be greater than --start-date.")
    timezone = resolve_timezone(args.timezone)
    args.loyverse_payment_type_id = args.loyverse_payment_type_id.strip()
    args.loyverse_payment_type_name = args.loyverse_payment_type_name.strip()
    if bool(args.loyverse_payment_type_id) != bool(args.loyverse_payment_type_name):
        raise SystemExit(
            "--loyverse-payment-type-id and --loyverse-payment-type-name must be provided together."
        )
    if not args.use_env:
        raise SystemExit("--use-env is required; credentials are accepted only from environment variables.")
    if args.execute and args.confirm != EXECUTE_CONFIRMATION:
        raise SystemExit(
            "Execution confirmation mismatch. No receipts were sent. "
            f"Use --confirm {EXECUTE_CONFIRMATION} only after approving the dry-run."
        )
    if args.execute and (
        start_date != EXECUTE_START_DATE
        or end_date_exclusive != EXECUTE_END_DATE_EXCLUSIVE
        or args.timezone != EXECUTE_TIMEZONE
    ):
        raise SystemExit(
            "Execute mode is locked to 2026-07-01 <= delivery_date < 2026-08-01 "
            "with timezone America/Costa_Rica. No receipts were sent."
        )
    if not args.execute and args.confirm:
        raise SystemExit("--confirm is only valid together with --execute.")
    return start_date, end_date_exclusive, timezone


def mask_database_component(value: str, *, fallback: str = "(not-applicable)") -> str:
    cleaned = (value or "").strip()
    if not cleaned:
        return fallback
    if len(cleaned) <= 4:
        return cleaned[:1] + "***"
    return cleaned[:2] + "***" + cleaned[-2:]


def require_environment(*, execute: bool) -> tuple[str, str, str]:
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
    if execute and scheme.startswith("sqlite"):
        raise SystemExit("SQLite is allowed for dry-run only and is blocked in execute mode.")
    database_name = (parsed.path or "").lstrip("/")
    database_info_masked = (
        f"scheme={scheme}; "
        f"host={mask_database_component(parsed.hostname or '')}; "
        f"database={mask_database_component(database_name)}"
    )
    return database_url, os.environ["LOYVERSE_API_TOKEN"].strip(), database_info_masked


def resolve_store(token: str) -> tuple[str, list[str]]:
    try:
        store_id, warnings, error = _resolve_loyverse_store_id(token)
    except RuntimeError as exc:
        raise SystemExit(f"Unable to resolve the Loyverse store using GET /stores: {exc}") from exc
    if error:
        raise SystemExit(error)
    if not store_id:
        raise SystemExit("Loyverse store resolution returned no usable store id.")
    return store_id, warnings


def prepare_export_dir(value: str) -> Path:
    export_dir = Path(value)
    if export_dir.exists() and any(export_dir.iterdir()):
        raise SystemExit(f"Export directory must be empty or absent: {export_dir}")
    export_dir.mkdir(parents=True, exist_ok=True)
    return export_dir


def decimal_text(value: Decimal) -> str:
    return format(value.quantize(Decimal("0.0001")), "f")


def payload_fingerprint(payload: dict[str, object]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def load_orders(
    db: Session,
    start_date: date,
    end_date_exclusive: date,
) -> list[B2BSalesOrder]:
    return (
        db.query(B2BSalesOrder)
        .options(joinedload(B2BSalesOrder.customer), joinedload(B2BSalesOrder.lines))
        .filter(
            B2BSalesOrder.delivery_date >= start_date,
            B2BSalesOrder.delivery_date < end_date_exclusive,
        )
        .order_by(B2BSalesOrder.delivery_date, B2BSalesOrder.id)
        .all()
    )


def evaluate_order(
    db: Session,
    order: B2BSalesOrder,
    store_id: str,
    *,
    fallback_payment_type_id: str = "",
    fallback_payment_type_name: str = "",
) -> Evaluation:
    base = {
        "order_id": order.id,
        "order_number": order.order_number,
        "delivery_date": order.delivery_date,
        "erp_status": order.status,
        "total_amount": _recalculate_order_total(order),
        "loyverse_receipt_id": (order.loyverse_receipt_id or "").strip(),
        "loyverse_receipt_number": (order.loyverse_receipt_number or "").strip(),
        "loyverse_sync_status": (order.loyverse_invoice_sync_status or "").strip(),
        "payload_fingerprint": "",
        "payload": None,
        "variant_snapshots": {},
    }
    if base["loyverse_receipt_id"] or base["loyverse_receipt_number"]:
        return Evaluation(
            **base,
            classification="already_sent",
            eligible=False,
            reason="A Loyverse receipt reference is already stored locally.",
        )
    if base["loyverse_sync_status"] == SYNC_STATUS_UNKNOWN:
        return Evaluation(
            **base,
            classification="blocked",
            eligible=False,
            reason="Previous Loyverse result is unknown; manual reconciliation is required.",
        )
    if order.status not in SUPPORTED_ORDER_STATUSES:
        return Evaluation(
            **base,
            classification="blocked",
            eligible=False,
            reason=f"Unsupported ERP order status: {order.status}.",
        )

    errors: list[str] = []
    missing_customer = False
    missing_payment = False
    missing_variant = False
    used_payment_type_fallback = False
    customer_id = ""
    payment_type_id = ""
    line_payloads: list[dict] = []
    variant_snapshots: dict[int, str] = {}

    try:
        customer_id = _resolve_customer_id(db, order)
    except B2BLoyverseInvoiceError as exc:
        missing_customer = True
        errors.append(str(exc))
    try:
        payment_type_id = _resolve_payment_type_id(db, order)
    except B2BLoyverseInvoiceError as exc:
        if fallback_payment_type_id and fallback_payment_type_name:
            payment_type_id = fallback_payment_type_id
            used_payment_type_fallback = True
        else:
            missing_payment = True
            errors.append(str(exc))
    try:
        line_payloads, variant_snapshots = _build_line_payloads(db, order)
    except B2BLoyverseInvoiceError as exc:
        missing_variant = _has_variant_mapping_error(db, order)
        errors.append(str(exc))

    total = base["total_amount"]
    if total <= Decimal("0"):
        errors.append("Order total must be greater than 0 to create a Loyverse receipt.")

    if errors:
        return Evaluation(
            **base,
            classification="blocked",
            eligible=False,
            reason=" | ".join(errors),
            missing_customer_mapping=missing_customer,
            missing_variant_mapping=missing_variant,
            missing_payment_type_mapping=missing_payment,
            used_payment_type_fallback=used_payment_type_fallback,
        )

    payload = _build_receipt_payload(
        order,
        store_id,
        customer_id,
        line_payloads,
        payment_type_id,
        total,
    )
    fingerprint = payload_fingerprint(payload)
    base["payload"] = payload
    base["payload_fingerprint"] = fingerprint
    base["variant_snapshots"] = variant_snapshots
    return Evaluation(
        **base,
        classification="eligible",
        eligible=True,
        reason=(
            f"Ready for Loyverse receipt creation. Emergency payment type fallback used: "
            f"{fallback_payment_type_name}."
            if used_payment_type_fallback
            else "Ready for Loyverse receipt creation."
        ),
        used_payment_type_fallback=used_payment_type_fallback,
    )


def _has_variant_mapping_error(db: Session, order: B2BSalesOrder) -> bool:
    for line in order.lines:
        try:
            _resolve_variant_id(db, line)
        except B2BLoyverseInvoiceError:
            return True
    return False


def write_csv(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: object) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=True, default=str)


def initial_errors(evaluations: list[Evaluation]) -> list[dict[str, object]]:
    return [
        {
            "order_id": item.order_id,
            "order_number": item.order_number,
            "stage": "dry_run_validation",
            "classification": item.classification,
            "error": item.reason,
        }
        for item in evaluations
        if item.classification == "blocked"
    ]


def build_summary(
    *,
    args: argparse.Namespace,
    timezone: tzinfo,
    database_info_masked: str,
    warnings: list[str],
    evaluations: list[Evaluation],
    execution_results: list[dict[str, object]],
) -> dict[str, object]:
    eligible = [item for item in evaluations if item.classification == "eligible"]
    payment_fallback_count = sum(item.used_payment_type_fallback for item in evaluations)
    summary_warnings = list(warnings)
    if payment_fallback_count:
        summary_warnings.append(
            "Emergency payment type fallback supplied by CLI was used for "
            f"{payment_fallback_count} order(s): {args.loyverse_payment_type_name} "
            f"({args.loyverse_payment_type_id}). No payment type mappings were modified."
        )
    result_counts = {name: 0 for name in ("success", "unknown", "failed")}
    for result in execution_results:
        outcome = str(result.get("result", ""))
        if outcome in result_counts:
            result_counts[outcome] += 1
    total_sent = sum(
        (Decimal(str(row["total_amount"])) for row in execution_results if row.get("result") == "success"),
        Decimal("0"),
    )
    return {
        "range_start": args.start_date,
        "range_end_exclusive": args.end_date_exclusive,
        "date_field": "B2BSalesOrder.delivery_date",
        "interval": "delivery_date >= start and delivery_date < end_exclusive",
        "timezone": args.timezone,
        "orders_found": len(evaluations),
        "orders_eligible": len(eligible),
        "orders_blocked": sum(item.classification == "blocked" for item in evaluations),
        "orders_already_sent": sum(item.classification == "already_sent" for item in evaluations),
        "orders_sent_success": result_counts["success"],
        "orders_unknown": (
            sum(item.loyverse_sync_status == SYNC_STATUS_UNKNOWN for item in evaluations)
            + result_counts["unknown"]
        ),
        "orders_failed": result_counts["failed"],
        "total_amount_eligible": decimal_text(sum((item.total_amount for item in eligible), Decimal("0"))),
        "total_amount_sent": decimal_text(total_sent),
        "missing_customer_mappings": sum(item.missing_customer_mapping for item in evaluations),
        "missing_variant_mappings": sum(item.missing_variant_mapping for item in evaluations),
        "missing_payment_type_mappings": sum(item.missing_payment_type_mapping for item in evaluations),
        "generated_at": datetime.now(timezone).isoformat(),
        "mode": "execute" if args.execute else "dry-run",
        "database_info_masked": database_info_masked,
        "warnings": summary_warnings,
        "orders_using_payment_type_fallback": payment_fallback_count,
        "included_erp_statuses": sorted(SUPPORTED_ORDER_STATUSES),
        "notes": [
            "delivery_date is the business-date criterion for this emergency monthly close.",
            "ERP status invoiced does not exclude an order when no Loyverse receipt reference exists.",
            "The existing Loyverse payload has no documented note/reference field, so no unsupported field was added.",
            "Timezone records the operating context and generated_at; delivery_date itself is a DATE column.",
        ],
    }


def write_reports(
    export_dir: Path,
    args: argparse.Namespace,
    timezone: tzinfo,
    database_info_masked: str,
    warnings: list[str],
    evaluations: list[Evaluation],
    execution_results: list[dict[str, object]],
    errors: list[dict[str, object]],
) -> None:
    all_rows = [item.csv_row() for item in evaluations]
    write_csv(export_dir / "orders_preview.csv", CSV_FIELDS, all_rows)
    write_csv(
        export_dir / "orders_eligible.csv",
        CSV_FIELDS,
        [item.csv_row() for item in evaluations if item.classification == "eligible"],
    )
    write_csv(
        export_dir / "orders_blocked.csv",
        CSV_FIELDS,
        [item.csv_row() for item in evaluations if item.classification == "blocked"],
    )
    write_csv(
        export_dir / "orders_already_sent.csv",
        CSV_FIELDS,
        [item.csv_row() for item in evaluations if item.classification == "already_sent"],
    )
    write_json(
        export_dir / "payloads_preview.json",
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
    write_csv(export_dir / "errors.csv", ERROR_FIELDS, errors)
    if args.execute:
        write_csv(export_dir / "execution_results.csv", EXECUTION_FIELDS, execution_results)
    write_json(
        export_dir / "summary.json",
        build_summary(
            args=args,
            timezone=timezone,
            database_info_masked=database_info_masked,
            warnings=warnings,
            evaluations=evaluations,
            execution_results=execution_results,
        ),
    )


def execute_one(
    session_factory,
    evaluation: Evaluation,
    *,
    start_date: date,
    end_date_exclusive: date,
    token: str,
    store_id: str,
    timezone: tzinfo,
    fallback_payment_type_id: str,
    fallback_payment_type_name: str,
) -> tuple[dict[str, object], dict[str, object] | None]:
    db: Session = session_factory()
    api_call_started = False
    try:
        order = (
            db.query(B2BSalesOrder)
            .options(joinedload(B2BSalesOrder.customer), joinedload(B2BSalesOrder.lines))
            .filter(B2BSalesOrder.id == evaluation.order_id)
            .one()
        )
        if not (start_date <= order.delivery_date < end_date_exclusive):
            return execution_row(evaluation, "failed", "Order left the approved delivery-date range.", timezone), {
                "order_id": order.id,
                "order_number": order.order_number,
                "stage": "execute_revalidation",
                "classification": "failed",
                "error": "Order left the approved delivery-date range.",
            }
        refreshed = evaluate_order(
            db,
            order,
            store_id,
            fallback_payment_type_id=fallback_payment_type_id,
            fallback_payment_type_name=fallback_payment_type_name,
        )
        if not refreshed.eligible:
            return execution_row(evaluation, "failed", refreshed.reason, timezone), {
                "order_id": order.id,
                "order_number": order.order_number,
                "stage": "execute_revalidation",
                "classification": refreshed.classification,
                "error": refreshed.reason,
            }
        if refreshed.payload_fingerprint != evaluation.payload_fingerprint:
            message = "Payload changed after dry-run evaluation; receipt was not sent."
            return execution_row(evaluation, "failed", message, timezone), {
                "order_id": order.id,
                "order_number": order.order_number,
                "stage": "execute_revalidation",
                "classification": "failed",
                "error": message,
            }

        order.loyverse_invoice_sync_attempted_at = datetime.now(UTC).replace(tzinfo=None)
        order.loyverse_invoice_sync_attempt_count = (order.loyverse_invoice_sync_attempt_count or 0) + 1
        order.loyverse_invoice_sync_status = SYNC_STATUS_UNKNOWN
        order.loyverse_invoice_sync_error = (
            "Execution started. If this status remains unknown, reconcile manually in Loyverse before retrying."
        )
        db.commit()

        api_call_started = True
        response = _create_loyverse_receipt(token, refreshed.payload or {})
        receipt = _extract_receipt(response)
        receipt_id = _string_value(receipt, "receipt_id", "receiptId", "id")
        receipt_number = _string_value(receipt, "receipt_number", "receiptNumber", "number")
        if not receipt_id and not receipt_number:
            raise LoyverseReceiptUnknownError(
                "Loyverse returned HTTP success without a usable receipt id or receipt number."
            )

        order.loyverse_receipt_id = receipt_id or None
        order.loyverse_receipt_number = receipt_number or None
        order.loyverse_invoice_sync_status = SYNC_STATUS_SUCCESS
        order.loyverse_invoice_sync_error = None
        order.loyverse_invoice_synced_at = datetime.now(UTC).replace(tzinfo=None)
        order.total_amount = refreshed.total_amount
        for line in order.lines:
            if line.id in refreshed.variant_snapshots:
                line.loyverse_variant_id_snapshot = refreshed.variant_snapshots[line.id]
        db.commit()
        result = execution_row(
            refreshed,
            "success",
            "Receipt created and local reference committed.",
            timezone,
            receipt_id=receipt_id,
            receipt_number=receipt_number,
        )
        return result, None
    except LoyverseReceiptUnknownError as exc:
        db.rollback()
        _persist_result_status(db, evaluation.order_id, SYNC_STATUS_UNKNOWN, str(exc))
        result = execution_row(evaluation, "unknown", str(exc), timezone)
        return result, execution_error(evaluation, "loyverse_response", "unknown", str(exc))
    except B2BLoyverseInvoiceError as exc:
        db.rollback()
        status = SYNC_STATUS_UNKNOWN if api_call_started and "HTTP" not in str(exc) else SYNC_STATUS_FAILED
        _persist_result_status(db, evaluation.order_id, status, str(exc))
        outcome = "unknown" if status == SYNC_STATUS_UNKNOWN else "failed"
        result = execution_row(evaluation, outcome, str(exc), timezone)
        return result, execution_error(evaluation, "loyverse_request", outcome, str(exc))
    except Exception as exc:
        db.rollback()
        message = f"Unexpected execution error: {type(exc).__name__}: {exc}"
        status = SYNC_STATUS_UNKNOWN if api_call_started else SYNC_STATUS_FAILED
        try:
            _persist_result_status(db, evaluation.order_id, status, message)
        except Exception:
            db.rollback()
        outcome = "unknown" if status == SYNC_STATUS_UNKNOWN else "failed"
        result = execution_row(evaluation, outcome, message, timezone)
        return result, execution_error(evaluation, "unexpected", outcome, message)
    finally:
        db.close()


def _persist_result_status(db: Session, order_id: int, status: str, error: str) -> None:
    order = db.query(B2BSalesOrder).filter(B2BSalesOrder.id == order_id).one()
    if not (order.loyverse_receipt_id or order.loyverse_receipt_number):
        order.loyverse_invoice_sync_status = status
        order.loyverse_invoice_sync_error = error[:2000]
        db.commit()


def execution_row(
    evaluation: Evaluation,
    result: str,
    message: str,
    timezone: tzinfo,
    *,
    receipt_id: str = "",
    receipt_number: str = "",
) -> dict[str, object]:
    return {
        "order_id": evaluation.order_id,
        "order_number": evaluation.order_number,
        "result": result,
        "total_amount": decimal_text(evaluation.total_amount),
        "loyverse_receipt_id": receipt_id,
        "loyverse_receipt_number": receipt_number,
        "payload_fingerprint": evaluation.payload_fingerprint,
        "message": message,
        "processed_at": datetime.now(timezone).isoformat(),
    }


def execution_error(
    evaluation: Evaluation,
    stage: str,
    classification: str,
    error: str,
) -> dict[str, object]:
    return {
        "order_id": evaluation.order_id,
        "order_number": evaluation.order_number,
        "stage": stage,
        "classification": classification,
        "error": error,
    }


def main() -> None:
    args = parse_args()
    start_date, end_date_exclusive, timezone = validate_args(args)
    database_url, token, database_info_masked = require_environment(execute=args.execute)
    store_id, warnings = resolve_store(token)
    export_dir = prepare_export_dir(args.export_dir)

    engine = create_engine(database_url)
    session_factory = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db: Session = session_factory()
    try:
        orders = load_orders(db, start_date, end_date_exclusive)
        evaluations = [
            evaluate_order(
                db,
                order,
                store_id,
                fallback_payment_type_id=args.loyverse_payment_type_id,
                fallback_payment_type_name=args.loyverse_payment_type_name,
            )
            for order in orders
        ]
    finally:
        db.rollback()
        db.close()

    execution_results: list[dict[str, object]] = []
    errors = initial_errors(evaluations)
    write_reports(
        export_dir,
        args,
        timezone,
        database_info_masked,
        warnings,
        evaluations,
        execution_results,
        errors,
    )

    if args.execute:
        for evaluation in evaluations:
            if not evaluation.eligible:
                continue
            result, error = execute_one(
                session_factory,
                evaluation,
                start_date=start_date,
                end_date_exclusive=end_date_exclusive,
                token=token,
                store_id=store_id,
                timezone=timezone,
                fallback_payment_type_id=args.loyverse_payment_type_id,
                fallback_payment_type_name=args.loyverse_payment_type_name,
            )
            execution_results.append(result)
            if error is not None:
                errors.append(error)
            write_reports(
                export_dir,
                args,
                timezone,
                database_info_masked,
                warnings,
                evaluations,
                execution_results,
                errors,
            )

    summary = build_summary(
        args=args,
        timezone=timezone,
        database_info_masked=database_info_masked,
        warnings=warnings,
        evaluations=evaluations,
        execution_results=execution_results,
    )
    print(
        f"Mode={summary['mode']} found={summary['orders_found']} "
        f"eligible={summary['orders_eligible']} blocked={summary['orders_blocked']} "
        f"already_sent={summary['orders_already_sent']}"
    )
    print(f"Evidence exported to: {export_dir}")
    for warning in warnings:
        print(f"WARNING: {warning}")
    if not args.execute:
        print("Dry-run complete. No POST requests were made and no database changes were committed.")


if __name__ == "__main__":
    main()
