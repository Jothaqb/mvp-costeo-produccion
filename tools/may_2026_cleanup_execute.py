from __future__ import annotations

import argparse
import csv
import json
import os
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.parse import urlparse

from sqlalchemy import create_engine, func, inspect, or_, text
from sqlalchemy.orm import Session, sessionmaker

from app.models import (
    AppSequence,
    B2BSalesOrder,
    B2BSalesOrderLine,
    B2CSalesOrder,
    InventoryAdjustment,
    InventoryBalance,
    InventoryTransaction,
    LotSequence,
    PackagingBatch,
    PackagingBatchActivity,
    PackagingBatchLine,
    PackagingBatchLineMaterial,
    ProductionOrder,
    ProductionOrderActivity,
    ProductionOrderMaterial,
    PurchaseOrder,
    PurchaseOrderLine,
    PurchaseOrderReceiveToken,
)


EXECUTE_CONFIRM_VALUE = "DELETE_MAY_JUNE_TRANSITION_DATA_2026"
OPENING_BALANCE_TYPE = "opening_balance"

ENV_ARG_MAP = {
    "execute_confirm": "MAY_CLEANUP_EXECUTE_CONFIRM",
    "start_datetime": "MAY_CLEANUP_START",
    "end_datetime": "MAY_CLEANUP_END",
    "export_dir": "MAY_CLEANUP_EXPORT_DIR",
    "expected_db_host_fragment": "MAY_CLEANUP_EXPECTED_DB_HOST_FRAGMENT",
    "expected_db_name": "MAY_CLEANUP_EXPECTED_DB_NAME",
    "approved_dry_run_dir": "MAY_CLEANUP_APPROVED_DRY_RUN_DIR",
    "backup_confirmed": "MAY_CLEANUP_BACKUP_CONFIRMED",
    "backup_reference": "MAY_CLEANUP_BACKUP_REFERENCE",
    "freeze_confirmed": "MAY_CLEANUP_FREEZE_CONFIRMED",
    "freeze_reference": "MAY_CLEANUP_FREEZE_REFERENCE",
    "reset_b2b_sequence": "MAY_CLEANUP_RESET_B2B_SEQUENCE",
    "reset_b2c_sequence": "MAY_CLEANUP_RESET_B2C_SEQUENCE",
}

REQUIRED_DRY_RUN_FILES = {
    "dry_run_summary.json",
    "tables_to_clean.csv",
    "documents_classification.csv",
    "inventory_transactions_scope.csv",
    "balances_impacted.csv",
    "sequences_reset_plan.csv",
    "suspicious_records.csv",
    "abort_conditions.csv",
    "dependency_analysis.csv",
    "child_records_scope.csv",
}

REQUIRED_DRY_RUN_JSON_KEYS = {
    "window_start",
    "window_end",
    "counts",
    "source_type_counts",
    "transaction_type_counts",
    "classification_counts",
}

REQUIRED_CSV_HEADERS = {
    "tables_to_clean.csv": {
        "table_name",
        "record_id",
        "document_number",
        "classification",
        "reason",
    },
    "documents_classification.csv": {
        "table_name",
        "source_type",
        "document_id",
        "document_number",
        "business_date",
        "classification",
        "blocking",
    },
    "inventory_transactions_scope.csv": {
        "txn_id",
        "transaction_date",
        "source_type",
        "source_id",
        "transaction_type",
        "product_id",
    },
    "balances_impacted.csv": {
        "balance_id",
        "product_id",
        "last_transaction_id",
        "issue",
    },
    "sequences_reset_plan.csv": {
        "sequence_name",
        "current_next_value",
        "safe_to_reset",
        "possible_collision",
    },
    "suspicious_records.csv": {
        "category",
        "table_name",
        "record_id",
        "reason",
        "blocking",
    },
    "abort_conditions.csv": {
        "condition_code",
        "severity",
        "detected",
        "detail",
        "blocking",
    },
    "dependency_analysis.csv": {
        "category",
        "severity",
        "table_name",
        "record_id",
        "source_type",
        "source_id",
        "blocking",
    },
    "child_records_scope.csv": {
        "table_name",
        "child_id",
        "parent_table",
        "parent_id",
        "issue",
        "blocking",
    },
}

APPROVED_C_DOCUMENTS = {
    "packaging_batches": {"PB1"},
    "production_orders": {
        "OP19424265",
        "OP19424270",
        "OP19424274",
        "OP19424275",
        "OP19424278",
        "OP19424280",
        "OP19424281",
        "OP19424282",
    },
}

DOCUMENT_MODEL_MAP = {
    "production_orders": {
        "model": ProductionOrder,
        "id_field": "id",
        "number_field": "internal_order_number",
    },
    "b2b_sales_orders": {
        "model": B2BSalesOrder,
        "id_field": "id",
        "number_field": "order_number",
    },
    "packaging_batches": {
        "model": PackagingBatch,
        "id_field": "id",
        "number_field": "internal_batch_number",
    },
    "purchase_orders": {
        "model": PurchaseOrder,
        "id_field": "id",
        "number_field": "po_number",
    },
    "inventory_adjustments": {
        "model": InventoryAdjustment,
        "id_field": "id",
        "number_field": "adjustment_number",
    },
}

DOCUMENT_SEQUENCE_PLAN = {
    "b2b_sales_order": {
        "table_name": "b2b_sales_orders",
        "next_value": 1,
    },
    "b2c_sales_order": {
        "table_name": "b2c_sales_orders",
        "next_value": 1,
    },
}

EXPECTED_BLOCKING_ABORT_CODES = {
    "opening_balance_exists",
    "balances_impacted_by_may_transactions",
}
EXPECTED_BLOCKING_SUSPICIOUS_CATEGORIES: set[str] = set()
EXPECTED_BLOCKING_DEPENDENCY_CATEGORIES: set[str] = set()
EXPECTED_BLOCKING_CHILD_ISSUES: set[str] = set()
ALLOWED_KNOWN_CHILD_SCOPE_ISSUE = "MAY_CHILD_OF_OUT_OF_WINDOW_HEADER"
FORBIDDEN_SEQUENCE_NAMES = {
    "production_order",
    "purchase_order",
    "inventory_adjustment",
    "packaging_batch",
}


@dataclass(frozen=True)
class AbortCondition:
    code: str
    detail: str
    blocking: bool = True


@dataclass
class ApprovedDryRunBundle:
    directory: Path
    summary: dict[str, object]
    tables_to_clean: list[dict[str, str]]
    documents_classification: list[dict[str, str]]
    inventory_transactions_scope: list[dict[str, str]]
    balances_impacted: list[dict[str, str]]
    sequences_reset_plan: list[dict[str, str]]
    suspicious_records: list[dict[str, str]]
    abort_conditions: list[dict[str, str]]
    dependency_analysis: list[dict[str, str]]
    child_records_scope: list[dict[str, str]]
    approved_opening_balance_count: int | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Controlled cleanup executor for May/June 2026 transition data."
    )
    parser.add_argument("--use-env", action="store_true")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--execute-confirm")
    parser.add_argument("--start-datetime")
    parser.add_argument("--end-datetime")
    parser.add_argument("--export-dir")
    parser.add_argument("--expected-db-host-fragment")
    parser.add_argument("--expected-db-name")
    parser.add_argument("--approved-dry-run-dir")
    parser.add_argument("--backup-confirmed", action="store_true")
    parser.add_argument("--backup-reference")
    parser.add_argument("--freeze-confirmed", action="store_true")
    parser.add_argument("--freeze-reference")
    parser.add_argument("--reset-b2b-sequence", action="store_true")
    parser.add_argument("--reset-b2c-sequence", action="store_true")
    return parser.parse_args()


def parse_bool_text(value: str | None) -> bool:
    normalized = (value or "").strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off", ""}:
        return False
    raise SystemExit(
        f"Invalid boolean value '{value}'. Use one of: true, false, 1, 0, yes, no."
    )


def resolve_args(args: argparse.Namespace) -> argparse.Namespace:
    if args.use_env:
        conflicting_cli = [
            flag
            for flag, value in (
                ("--execute-confirm", args.execute_confirm),
                ("--start-datetime", args.start_datetime),
                ("--end-datetime", args.end_datetime),
                ("--export-dir", args.export_dir),
                ("--expected-db-host-fragment", args.expected_db_host_fragment),
                ("--expected-db-name", args.expected_db_name),
                ("--approved-dry-run-dir", args.approved_dry_run_dir),
                ("--backup-confirmed", args.backup_confirmed),
                ("--backup-reference", args.backup_reference),
                ("--freeze-confirmed", args.freeze_confirmed),
                ("--freeze-reference", args.freeze_reference),
                ("--reset-b2b-sequence", args.reset_b2b_sequence),
                ("--reset-b2c-sequence", args.reset_b2c_sequence),
            )
            if value
        ]
        if conflicting_cli:
            raise SystemExit(
                "Do not mix --use-env with explicit CLI parameter flags. "
                "Use exactly one mode: full CLI arguments or --use-env. "
                "Conflicting flags: " + ", ".join(conflicting_cli)
            )

        required_env = [
            "start_datetime",
            "end_datetime",
            "export_dir",
            "expected_db_host_fragment",
            "expected_db_name",
            "approved_dry_run_dir",
        ]
        if args.execute:
            required_env.extend(
                [
                    "execute_confirm",
                    "backup_confirmed",
                    "backup_reference",
                    "freeze_confirmed",
                    "freeze_reference",
                ]
            )
        missing_env = [
            ENV_ARG_MAP[key]
            for key in required_env
            if not os.getenv(ENV_ARG_MAP[key])
        ]
        if missing_env:
            raise SystemExit(
                "Missing required environment variables for --use-env: "
                + ", ".join(missing_env)
            )

        return argparse.Namespace(
            use_env=True,
            execute=args.execute,
            execute_confirm=os.getenv(ENV_ARG_MAP["execute_confirm"]),
            start_datetime=os.getenv(ENV_ARG_MAP["start_datetime"]),
            end_datetime=os.getenv(ENV_ARG_MAP["end_datetime"]),
            export_dir=os.getenv(ENV_ARG_MAP["export_dir"]),
            expected_db_host_fragment=os.getenv(ENV_ARG_MAP["expected_db_host_fragment"]),
            expected_db_name=os.getenv(ENV_ARG_MAP["expected_db_name"]),
            approved_dry_run_dir=os.getenv(ENV_ARG_MAP["approved_dry_run_dir"]),
            backup_confirmed=parse_bool_text(os.getenv(ENV_ARG_MAP["backup_confirmed"])),
            backup_reference=os.getenv(ENV_ARG_MAP["backup_reference"]),
            freeze_confirmed=parse_bool_text(os.getenv(ENV_ARG_MAP["freeze_confirmed"])),
            freeze_reference=os.getenv(ENV_ARG_MAP["freeze_reference"]),
            reset_b2b_sequence=parse_bool_text(os.getenv(ENV_ARG_MAP["reset_b2b_sequence"])),
            reset_b2c_sequence=parse_bool_text(os.getenv(ENV_ARG_MAP["reset_b2c_sequence"])),
        )

    required_cli = [
        flag
        for flag, value in (
            ("--start-datetime", args.start_datetime),
            ("--end-datetime", args.end_datetime),
            ("--export-dir", args.export_dir),
            ("--expected-db-host-fragment", args.expected_db_host_fragment),
            ("--expected-db-name", args.expected_db_name),
            ("--approved-dry-run-dir", args.approved_dry_run_dir),
        )
        if not value
    ]
    if args.execute:
        required_cli.extend(
            [
                flag
                for flag, value in (
                    ("--execute-confirm", args.execute_confirm),
                    ("--backup-confirmed", args.backup_confirmed),
                    ("--backup-reference", args.backup_reference),
                    ("--freeze-confirmed", args.freeze_confirmed),
                    ("--freeze-reference", args.freeze_reference),
                )
                if not value
            ]
        )
    if required_cli:
        raise SystemExit("Missing required CLI arguments: " + ", ".join(required_cli))
    return args


def normalize_database_url(raw_url: str) -> str:
    database_url = (raw_url or "").strip()
    if database_url.startswith("postgres://"):
        return database_url.replace("postgres://", "postgresql://", 1)
    return database_url


def mask_host(host: str | None) -> str:
    if not host:
        return "(missing)"
    if len(host) <= 8:
        return host[:2] + "***"
    return host[:4] + "***" + host[-4:]


def parse_window(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise SystemExit(
            f"Invalid datetime '{value}'. Use ISO format like 2026-05-01T00:00:00."
        ) from exc
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(UTC).replace(tzinfo=None)
    return parsed


def ensure_runtime_prerequisites(args: argparse.Namespace) -> tuple[str, dict[str, str]]:
    raw_database_url = os.getenv("DATABASE_URL")
    if not raw_database_url:
        raise SystemExit("DATABASE_URL is required.")

    database_url = normalize_database_url(raw_database_url)
    parsed = urlparse(database_url)
    scheme = (parsed.scheme or "").lower()
    database_name = (parsed.path or "").lstrip("/")
    host = parsed.hostname or ""

    if not scheme.startswith("postgresql"):
        raise SystemExit("DATABASE_URL must point to PostgreSQL.")
    if scheme.startswith("sqlite") or database_url.startswith("sqlite"):
        raise SystemExit("SQLite environments are not allowed for this script.")
    if args.expected_db_host_fragment not in host:
        raise SystemExit("DATABASE_URL host does not match --expected-db-host-fragment.")
    if database_name != args.expected_db_name:
        raise SystemExit("DATABASE_URL database name does not match --expected-db-name.")
    if args.execute and args.execute_confirm != EXECUTE_CONFIRM_VALUE:
        raise SystemExit(
            "Execute confirmation mismatch. Use "
            f'--execute-confirm "{EXECUTE_CONFIRM_VALUE}" to continue.'
        )
    if args.execute and not args.backup_confirmed:
        raise SystemExit(
            "Execute mode requires backup_confirmed=true (CLI or environment)."
        )
    if args.execute and not args.freeze_confirmed:
        raise SystemExit(
            "Execute mode requires freeze_confirmed=true (CLI or environment)."
        )

    return database_url, {
        "scheme": scheme,
        "host_masked": mask_host(host),
        "database_name": database_name,
    }


def make_export_dir(base_dir: str) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    export_dir = Path(base_dir) / timestamp
    export_dir.mkdir(parents=True, exist_ok=False)
    return export_dir


def serialize_value(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    return value


def write_csv(path: Path, headers: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({header: serialize_value(row.get(header)) for header in headers})


def write_json(path: Path, payload: dict[str, object]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=True, default=str)


def add_abort(
    abort_conditions: list[AbortCondition],
    code: str,
    detail: str,
    *,
    blocking: bool = True,
) -> None:
    abort_conditions.append(AbortCondition(code=code, detail=detail, blocking=blocking))


def add_log(log_rows: list[dict[str, object]], level: str, message: str) -> None:
    log_rows.append(
        {
            "timestamp": datetime.utcnow().isoformat(sep=" "),
            "level": level,
            "message": message,
        }
    )


def read_csv_required(path: Path, required_headers: set[str]) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = set(reader.fieldnames or [])
        missing = sorted(required_headers - fieldnames)
        if missing:
            raise SystemExit(
                f"File '{path.name}' is missing required columns: {', '.join(missing)}"
            )
        return list(reader)


def parse_summary(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    missing = sorted(REQUIRED_DRY_RUN_JSON_KEYS - set(payload.keys()))
    if missing:
        raise SystemExit(
            f"Approved dry-run summary is missing keys: {', '.join(missing)}"
        )
    return payload


def parse_int(value: str | int | None, field_name: str) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise SystemExit(f"Expected integer for '{field_name}', got: {value!r}") from exc


def parse_bool_cell(value: str | bool | None) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def normalize_datetime_text(value: object, field_name: str) -> datetime:
    if value is None:
        raise SystemExit(f"Approved dry-run is missing required datetime field '{field_name}'.")
    return parse_window(str(value))


def load_approved_dry_run(directory: str) -> ApprovedDryRunBundle:
    approved_dir = Path(directory)
    if not approved_dir.exists() or not approved_dir.is_dir():
        raise SystemExit(f"Approved dry-run directory does not exist: {approved_dir}")

    files_present = {path.name for path in approved_dir.iterdir() if path.is_file()}
    missing_files = sorted(REQUIRED_DRY_RUN_FILES - files_present)
    if missing_files:
        raise SystemExit(
            "Approved dry-run directory is missing required files: "
            + ", ".join(missing_files)
        )

    summary = parse_summary(approved_dir / "dry_run_summary.json")
    tables_to_clean = read_csv_required(
        approved_dir / "tables_to_clean.csv",
        REQUIRED_CSV_HEADERS["tables_to_clean.csv"],
    )
    documents_classification = read_csv_required(
        approved_dir / "documents_classification.csv",
        REQUIRED_CSV_HEADERS["documents_classification.csv"],
    )
    inventory_scope = read_csv_required(
        approved_dir / "inventory_transactions_scope.csv",
        REQUIRED_CSV_HEADERS["inventory_transactions_scope.csv"],
    )
    balances_impacted = read_csv_required(
        approved_dir / "balances_impacted.csv",
        REQUIRED_CSV_HEADERS["balances_impacted.csv"],
    )
    sequences_reset_plan = read_csv_required(
        approved_dir / "sequences_reset_plan.csv",
        REQUIRED_CSV_HEADERS["sequences_reset_plan.csv"],
    )
    suspicious_records = read_csv_required(
        approved_dir / "suspicious_records.csv",
        REQUIRED_CSV_HEADERS["suspicious_records.csv"],
    )
    abort_conditions = read_csv_required(
        approved_dir / "abort_conditions.csv",
        REQUIRED_CSV_HEADERS["abort_conditions.csv"],
    )
    dependency_analysis = read_csv_required(
        approved_dir / "dependency_analysis.csv",
        REQUIRED_CSV_HEADERS["dependency_analysis.csv"],
    )
    child_records_scope = read_csv_required(
        approved_dir / "child_records_scope.csv",
        REQUIRED_CSV_HEADERS["child_records_scope.csv"],
    )

    approved_opening_balance_count = None
    opening_re = re.compile(r"Found\s+(\d+)\s+opening balance transactions", re.IGNORECASE)
    for row in abort_conditions:
        if row.get("condition_code") == "opening_balance_exists":
            match = opening_re.search(str(row.get("detail", "")))
            if match:
                approved_opening_balance_count = int(match.group(1))
                break

    return ApprovedDryRunBundle(
        directory=approved_dir,
        summary=summary,
        tables_to_clean=tables_to_clean,
        documents_classification=documents_classification,
        inventory_transactions_scope=inventory_scope,
        balances_impacted=balances_impacted,
        sequences_reset_plan=sequences_reset_plan,
        suspicious_records=suspicious_records,
        abort_conditions=abort_conditions,
        dependency_analysis=dependency_analysis,
        child_records_scope=child_records_scope,
        approved_opening_balance_count=approved_opening_balance_count,
    )


def validate_approved_scope(approved: ApprovedDryRunBundle, start_dt: datetime, end_dt: datetime) -> None:
    approved_start = normalize_datetime_text(approved.summary["window_start"], "window_start")
    approved_end = normalize_datetime_text(approved.summary["window_end"], "window_end")
    if approved_start != start_dt:
        raise SystemExit(
            f"Approved dry-run window_start mismatch. Expected {start_dt.isoformat(sep=' ')}, got {approved_start.isoformat(sep=' ')}."
        )
    if approved_end != end_dt:
        raise SystemExit(
            f"Approved dry-run window_end mismatch. Expected {end_dt.isoformat(sep=' ')}, got {approved_end.isoformat(sep=' ')}."
        )


def approved_gate_aborts(
    approved: ApprovedDryRunBundle,
    approved_targets: dict[str, object],
    current_state: dict[str, object],
    delete_plan: dict[str, int],
) -> tuple[list[AbortCondition], list[dict[str, object]]]:
    aborts: list[AbortCondition] = []
    allowed_known_child_scope_issues: list[dict[str, object]] = []

    for row in approved.abort_conditions:
        if not parse_bool_cell(row.get("blocking")):
            continue
        condition_code = str(row.get("condition_code") or "").strip()
        if condition_code not in EXPECTED_BLOCKING_ABORT_CODES:
            add_abort(
                aborts,
                "unexpected_blocking_abort_condition",
                f"Approved dry-run contains unexpected blocking abort condition: {condition_code}.",
            )

    for row in approved.suspicious_records:
        if not parse_bool_cell(row.get("blocking")):
            continue
        category = str(row.get("category") or "").strip()
        if category not in EXPECTED_BLOCKING_SUSPICIOUS_CATEGORIES:
            add_abort(
                aborts,
                "unexpected_blocking_suspicious_record",
                f"Approved dry-run contains blocking suspicious record category: {category}.",
            )

    for row in approved.dependency_analysis:
        if not parse_bool_cell(row.get("blocking")):
            continue
        category = str(row.get("category") or "").strip()
        if category not in EXPECTED_BLOCKING_DEPENDENCY_CATEGORIES:
            add_abort(
                aborts,
                "unexpected_blocking_dependency",
                f"Approved dry-run contains blocking dependency category: {category}.",
            )

    for row in approved.child_records_scope:
        if not parse_bool_cell(row.get("blocking")):
            continue
        issue = str(row.get("issue") or "").strip()
        if issue == ALLOWED_KNOWN_CHILD_SCOPE_ISSUE:
            table_name = str(row.get("table_name") or "").strip()
            parent_table = str(row.get("parent_table") or "").strip()
            child_id = parse_int(row.get("child_id"), "child_id")
            parent_id = parse_int(row.get("parent_id"), "parent_id")
            if table_name == "purchase_order_lines" and parent_table == "purchase_orders":
                if delete_plan.get("purchase_orders", 0) != 0:
                    add_abort(
                        aborts,
                        "unexpected_blocking_child_record",
                        "Approved child scope issue cannot be allowed because purchase_orders delete_plan is not zero.",
                    )
                    continue
                if delete_plan.get("purchase_order_lines", 0) != 0:
                    add_abort(
                        aborts,
                        "unexpected_blocking_child_record",
                        "Approved child scope issue cannot be allowed because purchase_order_lines delete_plan is not zero.",
                    )
                    continue
                if int(current_state.get("purchase_orders_in_scope", 0)) != 0:
                    add_abort(
                        aborts,
                        "unexpected_blocking_child_record",
                        "Approved child scope issue cannot be allowed because purchase_orders_in_scope is not zero.",
                    )
                    continue
                if int(current_state.get("out_of_window_same_source_count", 0)) != 0:
                    add_abort(
                        aborts,
                        "unexpected_blocking_child_record",
                        "Approved child scope issue cannot be allowed because out_of_window_same_source_count is not zero.",
                    )
                    continue
                purchase_order_orphans = int(
                    current_state.get("orphan_counts", {}).get("purchase_order_lines", 0)
                )
                if purchase_order_orphans != 0:
                    add_abort(
                        aborts,
                        "unexpected_blocking_child_record",
                        "Approved child scope issue cannot be allowed because purchase_order_lines has real orphans.",
                    )
                    continue
                purchase_order_scope_txns = int(
                    current_state.get("source_type_counts", {}).get("purchase_order", 0)
                )
                if purchase_order_scope_txns != 0:
                    add_abort(
                        aborts,
                        "unexpected_blocking_child_record",
                        "Approved child scope issue cannot be allowed because purchase_order inventory transactions exist in scope.",
                    )
                    continue
                if any(count > 0 for count in current_state.get("orphan_counts", {}).values()):
                    add_abort(
                        aborts,
                        "unexpected_blocking_child_record",
                        (
                            "Approved child scope issue MAY_CHILD_OF_OUT_OF_WINDOW_HEADER "
                            "cannot be allowed while any real orphan rows are present."
                        ),
                    )
                    continue
                allowed_known_child_scope_issues.append(
                    {
                        "issue": issue,
                        "detail": "purchase_order_lines_to_purchase_orders_historical_import",
                        "table_name": table_name,
                        "child_id": child_id,
                        "parent_table": parent_table,
                        "parent_id": parent_id,
                    }
                )
                continue
            add_abort(
                aborts,
                "unexpected_blocking_child_record",
                (
                    "Approved dry-run child scope issue MAY_CHILD_OF_OUT_OF_WINDOW_HEADER "
                    f"is only allowed for purchase_order_lines -> purchase_orders, not {table_name} -> {parent_table}."
                ),
            )
            continue
        if issue not in EXPECTED_BLOCKING_CHILD_ISSUES:
            add_abort(
                aborts,
                "unexpected_blocking_child_record",
                f"Approved dry-run contains blocking child scope issue: {issue}.",
            )

    return aborts, allowed_known_child_scope_issues


def build_approved_targets(approved: ApprovedDryRunBundle) -> dict[str, object]:
    docs_by_table: dict[str, list[dict[str, str]]] = defaultdict(list)
    docs_by_number: dict[tuple[str, str], dict[str, str]] = {}
    for row in approved.documents_classification:
        docs_by_table[row["table_name"]].append(row)
        docs_by_number[(row["table_name"], row["document_number"])] = row

    target_a_ids: dict[str, set[int]] = defaultdict(set)
    for row in approved.tables_to_clean:
        table_name = row["table_name"]
        record_id = parse_int(row["record_id"], "record_id")
        target_a_ids[table_name].add(record_id)
        classification_row = next(
            (
                item
                for item in docs_by_table.get(table_name, [])
                if parse_int(item["document_id"], "document_id") == record_id
            ),
            None,
        )
        if classification_row is None or classification_row["classification"] != "A":
            raise SystemExit(
                f"Approved dry-run inconsistency: {table_name} id {record_id} is not classified as A."
            )

    approved_c_rows: dict[str, dict[str, str]] = {}
    for table_name, document_numbers in APPROVED_C_DOCUMENTS.items():
        for document_number in document_numbers:
            row = docs_by_number.get((table_name, document_number))
            if row is None:
                raise SystemExit(
                    f"Approved C document '{document_number}' was not found in documents_classification.csv."
                )
            if row["classification"] != "C":
                raise SystemExit(
                    f"Approved C document '{document_number}' is not classified as C."
                )
            approved_c_rows[document_number] = row

    txn_ids = {
        parse_int(row["txn_id"], "txn_id")
        for row in approved.inventory_transactions_scope
    }
    tx_source_pairs = {
        (row.get("source_type") or None, parse_int(row["source_id"], "source_id"))
        for row in approved.inventory_transactions_scope
        if row.get("source_type") and row.get("source_id")
    }
    balances_impacted_product_ids = {
        parse_int(row["product_id"], "product_id")
        for row in approved.balances_impacted
    }

    sequence_plan = {
        row["sequence_name"]: row
        for row in approved.sequences_reset_plan
    }

    return {
        "a_ids_by_table": target_a_ids,
        "approved_c_rows": approved_c_rows,
        "approved_c_ids_by_table": {
            table_name: {
                parse_int(row["document_id"], "document_id")
                for document_number, row in approved_c_rows.items()
                if row["table_name"] == table_name
            }
            for table_name in APPROVED_C_DOCUMENTS.keys()
        },
        "inventory_txn_ids": txn_ids,
        "inventory_source_pairs": tx_source_pairs,
        "balances_impacted_product_ids": balances_impacted_product_ids,
        "sequence_plan": sequence_plan,
        "approved_source_type_counts": {
            str(key): int(value)
            for key, value in dict(approved.summary["source_type_counts"]).items()
        },
        "approved_transaction_type_counts": {
            str(key): int(value)
            for key, value in dict(approved.summary["transaction_type_counts"]).items()
        },
        "approved_counts": dict(approved.summary["counts"]),
    }


def collect_scope_transaction_summary(
    session: Session,
    start_dt: datetime,
    end_dt: datetime,
) -> tuple[list[dict[str, object]], Counter[str], Counter[str]]:
    rows = (
        session.query(
            InventoryTransaction.id,
            InventoryTransaction.transaction_date,
            InventoryTransaction.transaction_type,
            InventoryTransaction.source_type,
            InventoryTransaction.source_id,
            InventoryTransaction.source_line_id,
            InventoryTransaction.product_id,
        )
        .filter(
            InventoryTransaction.transaction_date >= start_dt,
            InventoryTransaction.transaction_date < end_dt,
        )
        .order_by(InventoryTransaction.transaction_date, InventoryTransaction.id)
        .all()
    )
    txn_rows: list[dict[str, object]] = []
    transaction_type_counts: Counter[str] = Counter()
    source_type_counts: Counter[str] = Counter()
    for row in rows:
        transaction_type_counts[str(row[2])] += 1
        source_type_counts[str(row[3])] += 1
        txn_rows.append(
            {
                "txn_id": int(row[0]),
                "transaction_date": row[1],
                "transaction_type": row[2],
                "source_type": row[3],
                "source_id": row[4],
                "source_line_id": row[5],
                "product_id": row[6],
            }
        )
    return txn_rows, transaction_type_counts, source_type_counts


def collect_current_state(
    session: Session,
    approved_targets: dict[str, object],
    start_dt: datetime,
    end_dt: datetime,
) -> dict[str, object]:
    scope_tx_rows, transaction_type_counts, source_type_counts = collect_scope_transaction_summary(
        session,
        start_dt,
        end_dt,
    )
    opening_balance_count = (
        session.query(func.count())
        .select_from(InventoryTransaction)
        .filter(InventoryTransaction.transaction_type == OPENING_BALANCE_TYPE)
        .scalar()
        or 0
    )
    production_orders_in_scope = (
        session.query(func.count())
        .select_from(ProductionOrder)
        .filter(
            ProductionOrder.production_date >= start_dt.date(),
            ProductionOrder.production_date < end_dt.date(),
        )
        .scalar()
        or 0
    )
    b2b_orders_in_scope = (
        session.query(func.count())
        .select_from(B2BSalesOrder)
        .filter(
            B2BSalesOrder.delivery_date >= start_dt.date(),
            B2BSalesOrder.delivery_date < end_dt.date(),
        )
        .scalar()
        or 0
    )
    b2c_orders_in_scope = (
        session.query(func.count())
        .select_from(B2CSalesOrder)
        .filter(
            B2CSalesOrder.order_date >= start_dt.date(),
            B2CSalesOrder.order_date < end_dt.date(),
        )
        .scalar()
        or 0
    )
    purchase_orders_in_scope = (
        session.query(func.count())
        .select_from(PurchaseOrder)
        .filter(
            PurchaseOrder.po_date >= start_dt.date(),
            PurchaseOrder.po_date < end_dt.date(),
        )
        .scalar()
        or 0
    )
    inventory_adjustments_in_scope = (
        session.query(func.count())
        .select_from(InventoryAdjustment)
        .filter(
            InventoryAdjustment.adjustment_date >= start_dt.date(),
            InventoryAdjustment.adjustment_date < end_dt.date(),
        )
        .scalar()
        or 0
    )
    packaging_batches_in_scope = (
        session.query(func.count())
        .select_from(PackagingBatch)
        .filter(
            PackagingBatch.production_date >= start_dt.date(),
            PackagingBatch.production_date < end_dt.date(),
        )
        .scalar()
        or 0
    )

    balances_impacted_product_ids = sorted(approved_targets["balances_impacted_product_ids"])
    balances_impacted_count = (
        session.query(func.count())
        .select_from(InventoryBalance)
        .filter(
            InventoryBalance.product_id.in_(balances_impacted_product_ids)
            if balances_impacted_product_ids
            else text("1=0")
        )
        .scalar()
        or 0
    )

    document_presence: dict[str, dict[str, object]] = {}
    for table_name, target_ids in approved_targets["a_ids_by_table"].items():
        if table_name not in DOCUMENT_MODEL_MAP:
            continue
        model = DOCUMENT_MODEL_MAP[table_name]["model"]
        number_field = DOCUMENT_MODEL_MAP[table_name]["number_field"]
        rows = (
            session.query(model.id, getattr(model, number_field))
            .filter(model.id.in_(sorted(target_ids)) if target_ids else text("1=0"))
            .all()
        )
        document_presence[table_name] = {
            "expected_ids": sorted(target_ids),
            "found_ids": sorted(int(row[0]) for row in rows),
            "found_numbers": [str(row[1]) for row in rows],
        }

    approved_c_presence: dict[str, dict[str, object]] = {}
    for document_number, row in approved_targets["approved_c_rows"].items():
        table_name = row["table_name"]
        model = DOCUMENT_MODEL_MAP[table_name]["model"]
        number_field = DOCUMENT_MODEL_MAP[table_name]["number_field"]
        db_rows = (
            session.query(model.id, getattr(model, number_field))
            .filter(getattr(model, number_field) == document_number)
            .all()
        )
        approved_c_presence[document_number] = {
            "table_name": table_name,
            "found_ids": [int(item[0]) for item in db_rows],
            "found_count": len(db_rows),
        }

    source_pairs = approved_targets["inventory_source_pairs"]
    out_of_window_same_source_count = 0
    for source_type, source_id in source_pairs:
        count = (
            session.query(func.count())
            .select_from(InventoryTransaction)
            .filter(
                InventoryTransaction.source_type == source_type,
                InventoryTransaction.source_id == source_id,
                or_(
                    InventoryTransaction.transaction_date < start_dt,
                    InventoryTransaction.transaction_date >= end_dt,
                ),
            )
            .scalar()
            or 0
        )
        out_of_window_same_source_count += int(count)

    sequence_current_values = {
        row[0]: int(row[1])
        for row in session.query(AppSequence.name, AppSequence.next_value).all()
    }
    forbidden_sequence_values = {
        row[0]: int(row[1])
        for row in session.query(AppSequence.name, AppSequence.next_value)
        .filter(AppSequence.name.in_(sorted(FORBIDDEN_SEQUENCE_NAMES)))
        .all()
    }
    lot_sequence_snapshot = [
        (int(row[0]), int(row[1]), int(row[2]), str(row[3]), int(row[4]))
        for row in session.query(
            LotSequence.id,
            LotSequence.iso_year,
            LotSequence.iso_week,
            LotSequence.product_sku,
            LotSequence.next_value,
        )
        .order_by(LotSequence.id)
        .all()
    ]

    return {
        "scope_transaction_rows": scope_tx_rows,
        "scope_transaction_count": len(scope_tx_rows),
        "transaction_type_counts": dict(transaction_type_counts),
        "source_type_counts": dict(source_type_counts),
        "opening_balance_count": int(opening_balance_count),
        "production_orders_in_scope": int(production_orders_in_scope),
        "b2b_orders_in_scope": int(b2b_orders_in_scope),
        "b2c_orders_in_scope": int(b2c_orders_in_scope),
        "purchase_orders_in_scope": int(purchase_orders_in_scope),
        "inventory_adjustments_in_scope": int(inventory_adjustments_in_scope),
        "packaging_batches_in_scope": int(packaging_batches_in_scope),
        "balances_impacted_count": int(balances_impacted_count),
        "document_presence": document_presence,
        "approved_c_presence": approved_c_presence,
        "out_of_window_same_source_count": int(out_of_window_same_source_count),
        "sequence_current_values": sequence_current_values,
        "forbidden_sequence_values": forbidden_sequence_values,
        "lot_sequence_snapshot": lot_sequence_snapshot,
        "orphan_counts": collect_orphan_counts(session),
    }


def collect_orphan_counts(session: Session) -> dict[str, int]:
    return {
        "production_order_materials": int(
            session.query(func.count())
            .select_from(ProductionOrderMaterial)
            .outerjoin(ProductionOrder, ProductionOrder.id == ProductionOrderMaterial.production_order_id)
            .filter(ProductionOrder.id.is_(None))
            .scalar()
            or 0
        ),
        "production_order_activities": int(
            session.query(func.count())
            .select_from(ProductionOrderActivity)
            .outerjoin(ProductionOrder, ProductionOrder.id == ProductionOrderActivity.production_order_id)
            .filter(ProductionOrder.id.is_(None))
            .scalar()
            or 0
        ),
        "b2b_sales_order_lines": int(
            session.query(func.count())
            .select_from(B2BSalesOrderLine)
            .outerjoin(B2BSalesOrder, B2BSalesOrder.id == B2BSalesOrderLine.sales_order_id)
            .filter(B2BSalesOrder.id.is_(None))
            .scalar()
            or 0
        ),
        "packaging_batch_lines": int(
            session.query(func.count())
            .select_from(PackagingBatchLine)
            .outerjoin(PackagingBatch, PackagingBatch.id == PackagingBatchLine.packaging_batch_id)
            .filter(PackagingBatch.id.is_(None))
            .scalar()
            or 0
        ),
        "packaging_batch_activities": int(
            session.query(func.count())
            .select_from(PackagingBatchActivity)
            .outerjoin(PackagingBatch, PackagingBatch.id == PackagingBatchActivity.packaging_batch_id)
            .filter(PackagingBatch.id.is_(None))
            .scalar()
            or 0
        ),
        "packaging_batch_line_materials": int(
            session.query(func.count())
            .select_from(PackagingBatchLineMaterial)
            .outerjoin(
                PackagingBatchLine,
                PackagingBatchLine.id == PackagingBatchLineMaterial.packaging_batch_line_id,
            )
            .filter(PackagingBatchLine.id.is_(None))
            .scalar()
            or 0
        ),
        "purchase_order_lines": int(
            session.query(func.count())
            .select_from(PurchaseOrderLine)
            .outerjoin(PurchaseOrder, PurchaseOrder.id == PurchaseOrderLine.purchase_order_id)
            .filter(PurchaseOrder.id.is_(None))
            .scalar()
            or 0
        ),
        "purchase_order_receive_tokens": int(
            session.query(func.count())
            .select_from(PurchaseOrderReceiveToken)
            .outerjoin(PurchaseOrder, PurchaseOrder.id == PurchaseOrderReceiveToken.purchase_order_id)
            .filter(PurchaseOrder.id.is_(None))
            .scalar()
            or 0
        ),
    }


def perform_drift_check(
    approved: ApprovedDryRunBundle,
    approved_targets: dict[str, object],
    current_state: dict[str, object],
    delete_plan: dict[str, int],
) -> tuple[list[AbortCondition], list[dict[str, object]]]:
    aborts, allowed_known_child_scope_issues = approved_gate_aborts(
        approved, approved_targets, current_state, delete_plan
    )

    approved_opening_balance_count = approved.approved_opening_balance_count
    if approved_opening_balance_count is not None:
        if current_state["opening_balance_count"] != approved_opening_balance_count:
            add_abort(
                aborts,
                "opening_balance_drift",
                f"Opening balance count drifted from {approved_opening_balance_count} to {current_state['opening_balance_count']}.",
            )

    approved_txn_count = len(approved.inventory_transactions_scope)
    if current_state["scope_transaction_count"] != approved_txn_count:
        add_abort(
            aborts,
            "inventory_transactions_scope_drift",
            f"Inventory transactions in scope drifted from {approved_txn_count} to {current_state['scope_transaction_count']}.",
        )

    if current_state["transaction_type_counts"] != approved_targets["approved_transaction_type_counts"]:
        add_abort(
            aborts,
            "transaction_type_drift",
            "Transaction type counts drifted from the approved dry-run.",
        )

    if current_state["source_type_counts"] != approved_targets["approved_source_type_counts"]:
        add_abort(
            aborts,
            "source_type_drift",
            "Source type counts drifted from the approved dry-run.",
        )

    approved_counts = approved_targets["approved_counts"]
    counts_mapping = {
        "production_orders_in_may": "production_orders_in_scope",
        "b2b_orders_in_may": "b2b_orders_in_scope",
        "b2c_orders_in_may": "b2c_orders_in_scope",
        "purchase_orders_in_may": "purchase_orders_in_scope",
        "inventory_adjustments_in_may": "inventory_adjustments_in_scope",
        "packaging_batches_in_may": "packaging_batches_in_scope",
    }
    for approved_key, current_key in counts_mapping.items():
        if int(approved_counts.get(approved_key, 0)) != int(current_state[current_key]):
            add_abort(
                aborts,
                f"{approved_key}_drift",
                f"{approved_key} drifted from {approved_counts.get(approved_key, 0)} to {current_state[current_key]}.",
            )

    approved_balance_count = len(approved.balances_impacted)
    if current_state["balances_impacted_count"] != approved_balance_count:
        add_abort(
            aborts,
            "balances_impacted_drift",
            f"Balances impacted drifted from {approved_balance_count} to {current_state['balances_impacted_count']}.",
        )

    for table_name, presence in current_state["document_presence"].items():
        expected_ids = presence["expected_ids"]
        found_ids = presence["found_ids"]
        if expected_ids != found_ids:
            add_abort(
                aborts,
                f"{table_name}_approved_targets_drift",
                f"Approved target IDs for {table_name} no longer match the live database.",
            )

    for document_number, presence in current_state["approved_c_presence"].items():
        if presence["found_count"] != 1:
            add_abort(
                aborts,
                "approved_c_document_drift",
                f"Approved C document {document_number} is not uniquely present in the live database.",
            )

    if current_state["out_of_window_same_source_count"] > 0:
        add_abort(
            aborts,
            "out_of_window_same_source_transactions_present",
            "Found out-of-window inventory transactions tied to the approved scope source documents.",
        )

    for table_name, count in current_state.get("orphan_counts", {}).items():
        if count > 0:
            add_abort(
                aborts,
                "orphan_rows_present",
                f"Found {count} orphan rows in {table_name}.",
            )

    approved_sequence_plan = approved_targets["sequence_plan"]
    for sequence_name, row in approved_sequence_plan.items():
        current_value = current_state["sequence_current_values"].get(sequence_name)
        approved_value = parse_int(row["current_next_value"], "current_next_value")
        if current_value is None or int(current_value) != approved_value:
            add_abort(
                aborts,
                "sequence_drift",
                f"Sequence '{sequence_name}' drifted from {approved_value} to {current_value}.",
            )

    return aborts, allowed_known_child_scope_issues


def fetch_document_ids_by_numbers(session: Session, table_name: str, numbers: set[str]) -> dict[str, int]:
    if table_name not in DOCUMENT_MODEL_MAP:
        raise SystemExit(f"Unsupported document table for approved C handling: {table_name}")
    model = DOCUMENT_MODEL_MAP[table_name]["model"]
    number_field = DOCUMENT_MODEL_MAP[table_name]["number_field"]
    rows = (
        session.query(model.id, getattr(model, number_field))
        .filter(getattr(model, number_field).in_(sorted(numbers)) if numbers else text("1=0"))
        .all()
    )
    return {str(row[1]): int(row[0]) for row in rows}


def build_target_ids(
    session: Session,
    approved_targets: dict[str, object],
) -> dict[str, set[int]]:
    target_ids: dict[str, set[int]] = defaultdict(set)
    for table_name, ids in approved_targets["a_ids_by_table"].items():
        target_ids[table_name].update(ids)

    for table_name, numbers in APPROVED_C_DOCUMENTS.items():
        target_ids[table_name].update(fetch_document_ids_by_numbers(session, table_name, numbers).values())

    target_ids["inventory_transactions_scope"] = set(approved_targets["inventory_txn_ids"])
    target_ids["inventory_transactions_opening_balance"] = {
        int(row[0])
        for row in (
            session.query(InventoryTransaction.id)
            .filter(InventoryTransaction.transaction_type == OPENING_BALANCE_TYPE)
            .all()
        )
    }
    target_ids["inventory_balances"] = {
        int(row[0]) for row in session.query(InventoryBalance.id).all()
    }

    production_ids = sorted(target_ids["production_orders"])
    b2b_ids = sorted(target_ids["b2b_sales_orders"])
    packaging_ids = sorted(target_ids["packaging_batches"])
    purchase_ids = sorted(target_ids["purchase_orders"])
    adjustment_ids = sorted(target_ids["inventory_adjustments"])

    target_ids["production_order_materials"] = {
        int(row[0])
        for row in (
            session.query(ProductionOrderMaterial.id)
            .filter(
                ProductionOrderMaterial.production_order_id.in_(production_ids)
                if production_ids
                else text("1=0")
            )
            .all()
        )
    }
    target_ids["production_order_activities"] = {
        int(row[0])
        for row in (
            session.query(ProductionOrderActivity.id)
            .filter(
                ProductionOrderActivity.production_order_id.in_(production_ids)
                if production_ids
                else text("1=0")
            )
            .all()
        )
    }
    target_ids["b2b_sales_order_lines"] = {
        int(row[0])
        for row in (
            session.query(B2BSalesOrderLine.id)
            .filter(
                B2BSalesOrderLine.sales_order_id.in_(b2b_ids)
                if b2b_ids
                else text("1=0")
            )
            .all()
        )
    }
    target_ids["purchase_order_lines"] = {
        int(row[0])
        for row in (
            session.query(PurchaseOrderLine.id)
            .filter(
                PurchaseOrderLine.purchase_order_id.in_(purchase_ids)
                if purchase_ids
                else text("1=0")
            )
            .all()
        )
    }
    target_ids["purchase_order_receive_tokens"] = {
        int(row[0])
        for row in (
            session.query(PurchaseOrderReceiveToken.id)
            .filter(
                PurchaseOrderReceiveToken.purchase_order_id.in_(purchase_ids)
                if purchase_ids
                else text("1=0")
            )
            .all()
        )
    }
    packaging_line_ids = {
        int(row[0])
        for row in (
            session.query(PackagingBatchLine.id)
            .filter(
                PackagingBatchLine.packaging_batch_id.in_(packaging_ids)
                if packaging_ids
                else text("1=0")
            )
            .all()
        )
    }
    target_ids["packaging_batch_lines"] = packaging_line_ids
    target_ids["packaging_batch_activities"] = {
        int(row[0])
        for row in (
            session.query(PackagingBatchActivity.id)
            .filter(
                PackagingBatchActivity.packaging_batch_id.in_(packaging_ids)
                if packaging_ids
                else text("1=0")
            )
            .all()
        )
    }
    target_ids["packaging_batch_line_materials"] = {
        int(row[0])
        for row in (
            session.query(PackagingBatchLineMaterial.id)
            .filter(
                PackagingBatchLineMaterial.packaging_batch_line_id.in_(sorted(packaging_line_ids))
                if packaging_line_ids
                else text("1=0")
            )
            .all()
        )
    }
    if adjustment_ids:
        target_ids["inventory_adjustments"].update(adjustment_ids)
    return target_ids


def fetch_rows_for_export(
    session: Session,
    *,
    table_name: str,
    target_ids: set[int],
) -> list[dict[str, object]]:
    if table_name == "inventory_transactions_scope":
        rows = (
            session.query(
                InventoryTransaction.id,
                InventoryTransaction.transaction_date,
                InventoryTransaction.transaction_type,
                InventoryTransaction.source_type,
                InventoryTransaction.source_id,
                InventoryTransaction.source_line_id,
                InventoryTransaction.product_id,
                InventoryTransaction.quantity_in,
                InventoryTransaction.quantity_out,
                InventoryTransaction.unit_cost,
                InventoryTransaction.total_cost,
            )
            .filter(InventoryTransaction.id.in_(sorted(target_ids)) if target_ids else text("1=0"))
            .all()
        )
        return [
            {
                "id": row[0],
                "transaction_date": row[1],
                "transaction_type": row[2],
                "source_type": row[3],
                "source_id": row[4],
                "source_line_id": row[5],
                "product_id": row[6],
                "quantity_in": row[7],
                "quantity_out": row[8],
                "unit_cost": row[9],
                "total_cost": row[10],
            }
            for row in rows
        ]
    if table_name == "inventory_transactions_opening_balance":
        rows = (
            session.query(
                InventoryTransaction.id,
                InventoryTransaction.transaction_date,
                InventoryTransaction.transaction_type,
                InventoryTransaction.source_type,
                InventoryTransaction.source_id,
                InventoryTransaction.product_id,
            )
            .filter(InventoryTransaction.id.in_(sorted(target_ids)) if target_ids else text("1=0"))
            .all()
        )
        return [
            {
                "id": row[0],
                "transaction_date": row[1],
                "transaction_type": row[2],
                "source_type": row[3],
                "source_id": row[4],
                "product_id": row[5],
            }
            for row in rows
        ]
    if table_name == "inventory_balances":
        rows = (
            session.query(
                InventoryBalance.id,
                InventoryBalance.product_id,
                InventoryBalance.on_hand_qty,
                InventoryBalance.average_unit_cost,
                InventoryBalance.inventory_value,
                InventoryBalance.last_transaction_id,
                InventoryBalance.last_transaction_at,
            )
            .filter(InventoryBalance.id.in_(sorted(target_ids)) if target_ids else text("1=0"))
            .all()
        )
        return [
            {
                "id": row[0],
                "product_id": row[1],
                "on_hand_qty": row[2],
                "average_unit_cost": row[3],
                "inventory_value": row[4],
                "last_transaction_id": row[5],
                "last_transaction_at": row[6],
            }
            for row in rows
        ]

    table_config = DOCUMENT_MODEL_MAP.get(table_name)
    if table_config:
        model = table_config["model"]
        number_field = table_config["number_field"]
        rows = (
            session.query(model.id, getattr(model, number_field))
            .filter(model.id.in_(sorted(target_ids)) if target_ids else text("1=0"))
            .all()
        )
        return [{"id": row[0], "document_number": row[1]} for row in rows]

    model_map = {
        "production_order_materials": ProductionOrderMaterial,
        "production_order_activities": ProductionOrderActivity,
        "b2b_sales_order_lines": B2BSalesOrderLine,
        "packaging_batch_lines": PackagingBatchLine,
        "packaging_batch_activities": PackagingBatchActivity,
        "packaging_batch_line_materials": PackagingBatchLineMaterial,
        "purchase_order_lines": PurchaseOrderLine,
        "purchase_order_receive_tokens": PurchaseOrderReceiveToken,
        "inventory_adjustments": InventoryAdjustment,
    }
    if table_name not in model_map:
        return []
    model = model_map[table_name]
    rows = (
        session.query(model.id)
        .filter(model.id.in_(sorted(target_ids)) if target_ids else text("1=0"))
        .all()
    )
    return [{"id": row[0]} for row in rows]


def export_pre_cleanup(
    session: Session,
    export_dir: Path,
    target_ids: dict[str, set[int]],
) -> list[str]:
    export_plan = [
        ("pre_cleanup_inventory_transactions.csv", "inventory_transactions_scope"),
        ("pre_cleanup_inventory_balances.csv", "inventory_balances"),
        ("pre_cleanup_production_orders.csv", "production_orders"),
        ("pre_cleanup_production_order_materials.csv", "production_order_materials"),
        ("pre_cleanup_production_order_activities.csv", "production_order_activities"),
        ("pre_cleanup_b2b_sales_orders.csv", "b2b_sales_orders"),
        ("pre_cleanup_b2b_sales_order_lines.csv", "b2b_sales_order_lines"),
        ("pre_cleanup_packaging_batches.csv", "packaging_batches"),
        ("pre_cleanup_packaging_batch_lines.csv", "packaging_batch_lines"),
        ("pre_cleanup_packaging_batch_activities.csv", "packaging_batch_activities"),
        ("pre_cleanup_packaging_batch_line_materials.csv", "packaging_batch_line_materials"),
        ("pre_cleanup_opening_balances.csv", "inventory_transactions_opening_balance"),
        ("pre_cleanup_app_sequences.csv", "app_sequences"),
        ("pre_cleanup_lot_sequences.csv", "lot_sequences"),
    ]
    generated: list[str] = []
    for filename, logical_table in export_plan:
        if logical_table == "app_sequences":
            rows = [
                {"name": row[0], "next_value": row[1]}
                for row in session.query(AppSequence.name, AppSequence.next_value).all()
            ]
        elif logical_table == "lot_sequences":
            rows = [
                {
                    "id": row[0],
                    "iso_year": row[1],
                    "iso_week": row[2],
                    "product_sku": row[3],
                    "next_value": row[4],
                }
                for row in session.query(
                    LotSequence.id,
                    LotSequence.iso_year,
                    LotSequence.iso_week,
                    LotSequence.product_sku,
                    LotSequence.next_value,
                ).all()
            ]
        else:
            rows = fetch_rows_for_export(
                session,
                table_name=logical_table,
                target_ids=target_ids.get(logical_table, set()),
            )
        headers = sorted({key for row in rows for key in row.keys()}) if rows else ["note"]
        if not rows:
            rows = [{"note": "no_rows"}]
        write_csv(export_dir / filename, headers, rows)
        generated.append(filename)
    return generated


def build_delete_plan(target_ids: dict[str, set[int]]) -> dict[str, int]:
    return {table_name: len(ids) for table_name, ids in target_ids.items()}


def pre_commit_validations(
    session: Session,
    target_ids: dict[str, set[int]],
    forbidden_sequence_baseline: dict[str, int],
    lot_sequence_baseline: list[tuple[int, int, int, str, int]],
) -> list[AbortCondition]:
    aborts: list[AbortCondition] = []
    if session.query(func.count()).select_from(InventoryBalance).scalar() != 0:
        add_abort(aborts, "inventory_balances_not_zero", "inventory_balances was not fully cleared.")
    if (
        session.query(func.count())
        .select_from(InventoryTransaction)
        .filter(InventoryTransaction.transaction_type == OPENING_BALANCE_TYPE)
        .scalar()
        != 0
    ):
        add_abort(aborts, "opening_balance_not_zero", "Opening balance transactions remain after cleanup.")
    if target_ids.get("inventory_transactions_scope"):
        remaining_scope = (
            session.query(func.count())
            .select_from(InventoryTransaction)
            .filter(InventoryTransaction.id.in_(sorted(target_ids["inventory_transactions_scope"])))
            .scalar()
            or 0
        )
        if remaining_scope != 0:
            add_abort(aborts, "scope_transactions_remaining", "Approved in-scope inventory transactions remain after delete.")
    for table_name in ("production_orders", "b2b_sales_orders", "packaging_batches", "purchase_orders", "inventory_adjustments"):
        ids = target_ids.get(table_name, set())
        if not ids or table_name not in DOCUMENT_MODEL_MAP:
            continue
        model = DOCUMENT_MODEL_MAP[table_name]["model"]
        remaining = (
            session.query(func.count()).select_from(model).filter(model.id.in_(sorted(ids))).scalar() or 0
        )
        if remaining != 0:
            add_abort(aborts, f"{table_name}_remaining", f"Rows remain in {table_name} after delete.")
    child_checks = [
        ("production_order_materials", ProductionOrderMaterial),
        ("production_order_activities", ProductionOrderActivity),
        ("b2b_sales_order_lines", B2BSalesOrderLine),
        ("packaging_batch_lines", PackagingBatchLine),
        ("packaging_batch_activities", PackagingBatchActivity),
        ("packaging_batch_line_materials", PackagingBatchLineMaterial),
        ("purchase_order_lines", PurchaseOrderLine),
        ("purchase_order_receive_tokens", PurchaseOrderReceiveToken),
    ]
    for table_name, model in child_checks:
        ids = target_ids.get(table_name, set())
        if not ids:
            continue
        remaining = (
            session.query(func.count()).select_from(model).filter(model.id.in_(sorted(ids))).scalar() or 0
        )
        if remaining != 0:
            add_abort(aborts, f"{table_name}_remaining", f"Rows remain in {table_name} after delete.")
    for table_name, count in collect_orphan_counts(session).items():
        if count != 0:
            add_abort(aborts, f"{table_name}_orphans_remaining", f"Orphan rows remain in {table_name} after delete.")
    current_forbidden_sequences = {
        row[0]: int(row[1])
        for row in session.query(AppSequence.name, AppSequence.next_value)
        .filter(AppSequence.name.in_(sorted(FORBIDDEN_SEQUENCE_NAMES)))
        .all()
    }
    if current_forbidden_sequences != forbidden_sequence_baseline:
        add_abort(
            aborts,
            "forbidden_sequences_changed",
            "One or more forbidden sequences changed during cleanup execution.",
        )
    current_lot_snapshot = [
        (int(row[0]), int(row[1]), int(row[2]), str(row[3]), int(row[4]))
        for row in session.query(
            LotSequence.id,
            LotSequence.iso_year,
            LotSequence.iso_week,
            LotSequence.product_sku,
            LotSequence.next_value,
        )
        .order_by(LotSequence.id)
        .all()
    ]
    if current_lot_snapshot != lot_sequence_baseline:
        add_abort(
            aborts,
            "lot_sequences_changed",
            "lot_sequences changed during cleanup execution, which is not allowed.",
        )
    return aborts


def validate_post_commit(
    session: Session,
    target_ids: dict[str, set[int]],
    sequence_actions: list[dict[str, object]],
    forbidden_sequence_baseline: dict[str, int],
    lot_sequence_baseline: list[tuple[int, int, int, str, int]],
) -> dict[str, object]:
    result = {
        "opening_balance_remaining": (
            session.query(func.count())
            .select_from(InventoryTransaction)
            .filter(InventoryTransaction.transaction_type == OPENING_BALANCE_TYPE)
            .scalar()
            or 0
        ),
        "inventory_balances_remaining": session.query(func.count()).select_from(InventoryBalance).scalar() or 0,
        "remaining_scope_transactions": (
            session.query(func.count())
            .select_from(InventoryTransaction)
            .filter(InventoryTransaction.id.in_(sorted(target_ids.get("inventory_transactions_scope", set()))))
            .scalar()
            or 0
        ),
        "sequence_values": {
            row[0]: int(row[1])
            for row in session.query(AppSequence.name, AppSequence.next_value).all()
        },
        "forbidden_sequence_values": {
            row[0]: int(row[1])
            for row in session.query(AppSequence.name, AppSequence.next_value)
            .filter(AppSequence.name.in_(sorted(FORBIDDEN_SEQUENCE_NAMES)))
            .all()
        },
        "lot_sequence_snapshot": [
            (int(row[0]), int(row[1]), int(row[2]), str(row[3]), int(row[4]))
            for row in session.query(
                LotSequence.id,
                LotSequence.iso_year,
                LotSequence.iso_week,
                LotSequence.product_sku,
                LotSequence.next_value,
            )
            .order_by(LotSequence.id)
            .all()
        ],
        "orphan_counts": collect_orphan_counts(session),
        "sequence_actions": sequence_actions,
    }
    result["forbidden_sequence_unchanged"] = (
        result["forbidden_sequence_values"] == forbidden_sequence_baseline
    )
    result["lot_sequences_unchanged"] = (
        result["lot_sequence_snapshot"] == lot_sequence_baseline
    )
    return result


def execute_deletes(
    session: Session,
    target_ids: dict[str, set[int]],
    args: argparse.Namespace,
    approved_targets: dict[str, object],
) -> tuple[dict[str, int], list[dict[str, object]]]:
    deleted_counts: dict[str, int] = {}
    sequence_actions: list[dict[str, object]] = []

    deleted_counts["inventory_balances"] = session.query(InventoryBalance).delete(synchronize_session=False)

    if target_ids.get("inventory_adjustments"):
        deleted_counts["inventory_adjustments"] = (
            session.query(InventoryAdjustment)
            .filter(InventoryAdjustment.id.in_(sorted(target_ids["inventory_adjustments"])))
            .delete(synchronize_session=False)
        )
    else:
        deleted_counts["inventory_adjustments"] = 0

    child_delete_specs = [
        ("production_order_materials", ProductionOrderMaterial),
        ("production_order_activities", ProductionOrderActivity),
        ("b2b_sales_order_lines", B2BSalesOrderLine),
        ("packaging_batch_line_materials", PackagingBatchLineMaterial),
        ("packaging_batch_activities", PackagingBatchActivity),
        ("packaging_batch_lines", PackagingBatchLine),
        ("purchase_order_receive_tokens", PurchaseOrderReceiveToken),
        ("purchase_order_lines", PurchaseOrderLine),
    ]
    for table_name, model in child_delete_specs:
        ids = target_ids.get(table_name, set())
        if ids:
            deleted_counts[table_name] = (
                session.query(model)
                .filter(model.id.in_(sorted(ids)))
                .delete(synchronize_session=False)
            )
        else:
            deleted_counts[table_name] = 0

    header_delete_specs = [
        ("production_orders", ProductionOrder),
        ("b2b_sales_orders", B2BSalesOrder),
        ("packaging_batches", PackagingBatch),
        ("purchase_orders", PurchaseOrder),
    ]
    for table_name, model in header_delete_specs:
        ids = target_ids.get(table_name, set())
        if ids:
            deleted_counts[table_name] = (
                session.query(model)
                .filter(model.id.in_(sorted(ids)))
                .delete(synchronize_session=False)
            )
        else:
            deleted_counts[table_name] = 0

    scope_tx_ids = target_ids.get("inventory_transactions_scope", set())
    deleted_counts["inventory_transactions_scope"] = (
        session.query(InventoryTransaction)
        .filter(InventoryTransaction.id.in_(sorted(scope_tx_ids)) if scope_tx_ids else text("1=0"))
        .delete(synchronize_session=False)
    )
    deleted_counts["inventory_transactions_opening_balance"] = (
        session.query(InventoryTransaction)
        .filter(InventoryTransaction.transaction_type == OPENING_BALANCE_TYPE)
        .delete(synchronize_session=False)
    )

    reset_specs = [
        ("b2b_sales_order", args.reset_b2b_sequence),
        ("b2c_sales_order", args.reset_b2c_sequence),
    ]
    approved_sequence_plan = approved_targets["sequence_plan"]
    for sequence_name, enabled in reset_specs:
        if not enabled:
            sequence_actions.append(
                {
                    "sequence_name": sequence_name,
                    "action": "skipped",
                    "reason": "flag_not_enabled",
                }
            )
            continue
        approved_row = approved_sequence_plan.get(sequence_name)
        if approved_row is None:
            raise RuntimeError(f"Sequence '{sequence_name}' missing from approved dry-run sequence plan.")
        if not parse_bool_cell(approved_row["safe_to_reset"]):
            raise RuntimeError(f"Sequence '{sequence_name}' is not approved as safe_to_reset.")
        sequence = session.query(AppSequence).filter(AppSequence.name == sequence_name).one_or_none()
        if sequence is None:
            raise RuntimeError(f"Sequence '{sequence_name}' does not exist in app_sequences.")
        old_value = int(sequence.next_value)
        sequence.next_value = int(DOCUMENT_SEQUENCE_PLAN[sequence_name]["next_value"])
        sequence_actions.append(
            {
                "sequence_name": sequence_name,
                "action": "reset",
                "old_value": old_value,
                "new_value": int(sequence.next_value),
            }
        )

    return deleted_counts, sequence_actions


def write_cleanup_manifest(
    path: Path,
    *,
    args: argparse.Namespace,
    db_context: dict[str, str],
    approved_dir: Path,
    mode: str,
) -> None:
    write_json(
        path,
        {
            "script": "tools/may_2026_cleanup_execute.py",
            "parameter_source": "environment" if args.use_env else "cli",
            "mode": mode,
            "window_start": args.start_datetime,
            "window_end": args.end_datetime,
            "database": db_context,
            "approved_dry_run_dir": str(approved_dir),
            "backup_reference": args.backup_reference,
            "freeze_reference": args.freeze_reference,
            "reset_b2b_sequence": bool(args.reset_b2b_sequence),
            "reset_b2c_sequence": bool(args.reset_b2c_sequence),
            "generated_at": datetime.utcnow().isoformat(sep=" "),
        },
    )


def main() -> None:
    args = resolve_args(parse_args())
    start_dt = parse_window(args.start_datetime)
    end_dt = parse_window(args.end_datetime)
    if not start_dt < end_dt:
        raise SystemExit("--start-datetime must be earlier than --end-datetime.")

    database_url, db_context = ensure_runtime_prerequisites(args)
    approved = load_approved_dry_run(args.approved_dry_run_dir)
    validate_approved_scope(approved, start_dt, end_dt)
    approved_targets = build_approved_targets(approved)
    export_dir = make_export_dir(args.export_dir)

    log_rows: list[dict[str, object]] = []
    add_log(log_rows, "info", "Cleanup executor initialized.")
    add_log(log_rows, "info", f"Mode: {'execute' if args.execute else 'dry-run'}")

    print("CLEANUP EXECUTOR")
    print(f"Mode: {'EXECUTE' if args.execute else 'DRY-RUN'}")
    print(f"Database host: {db_context['host_masked']}")
    print(f"Database name: {db_context['database_name']}")

    engine = create_engine(database_url)
    if engine.dialect.name != "postgresql":
        raise SystemExit("Detected dialect is not postgresql.")

    write_cleanup_manifest(
        export_dir / "cleanup_manifest.json",
        args=args,
        db_context=db_context,
        approved_dir=approved.directory,
        mode="execute" if args.execute else "dry-run",
    )

    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())
    if "lot_sequences" not in existing_tables:
        raise SystemExit("lot_sequences table is required for validation and must remain untouched.")

    session_factory = sessionmaker(bind=engine, autocommit=False, autoflush=False)
    pre_session = session_factory()
    try:
        if not args.execute:
            pre_session.execute(text("SET TRANSACTION READ ONLY"))
        target_ids = build_target_ids(pre_session, approved_targets)
        delete_plan = build_delete_plan(target_ids)
        current_state = collect_current_state(pre_session, approved_targets, start_dt, end_dt)
        drift_aborts, allowed_known_child_scope_issues = perform_drift_check(
            approved, approved_targets, current_state, delete_plan
        )

        execute_plan_summary = {
            "parameter_source": "environment" if args.use_env else "cli",
            "mode": "execute" if args.execute else "dry-run",
            "window_start": start_dt.isoformat(sep=" "),
            "window_end": end_dt.isoformat(sep=" "),
            "database": db_context,
            "drift_aborts": [
                {"code": item.code, "detail": item.detail, "blocking": item.blocking}
                for item in drift_aborts
            ],
            "allowed_known_child_scope_issue": allowed_known_child_scope_issues,
            "ready_to_execute": not drift_aborts,
            "delete_plan": delete_plan,
            "current_state": {
                key: value
                for key, value in current_state.items()
                if key
                not in {
                    "scope_transaction_rows",
                    "document_presence",
                    "approved_c_presence",
                    "sequence_current_values",
                }
            },
        }
        write_json(export_dir / "execute_plan_summary.json", execute_plan_summary)

        sequence_actions_preview = []
        for sequence_name in ("b2b_sales_order", "b2c_sales_order"):
            enabled = (
                args.reset_b2b_sequence if sequence_name == "b2b_sales_order" else args.reset_b2c_sequence
            )
            approved_row = approved_targets["sequence_plan"].get(sequence_name)
            sequence_actions_preview.append(
                {
                    "sequence_name": sequence_name,
                    "requested": bool(enabled),
                    "safe_to_reset": parse_bool_cell(approved_row["safe_to_reset"]) if approved_row else False,
                }
            )
        write_json(
            export_dir / "sequence_actions.json",
            {
                "mode": "preview" if not args.execute else "pending_execute",
                "actions": sequence_actions_preview,
            },
        )

        if drift_aborts:
            add_log(log_rows, "error", "Drift detected against approved dry-run; execution is blocked.")
        else:
            add_log(log_rows, "info", "No drift detected against approved dry-run.")

    finally:
        pre_session.rollback()
        pre_session.close()

    if not args.execute:
        write_json(
            export_dir / "deleted_counts.json",
            {
                "mode": "dry-run",
                "counts": delete_plan,
                "note": "No deletes executed in dry-run mode.",
            },
        )
        write_json(
            export_dir / "post_cleanup_validation.json",
            {
                "mode": "dry-run",
                "status": "not_executed",
                "reason": "Execute flag was not provided.",
            },
        )
        write_json(export_dir / "cleanup_execution_log.json", {"events": log_rows})
        engine.dispose()
        return

    if drift_aborts:
        write_json(export_dir / "cleanup_execution_log.json", {"events": log_rows})
        engine.dispose()
        raise SystemExit("Execution blocked due to drift against the approved dry-run.")

    transaction_session = session_factory()
    deleted_counts: dict[str, int] = {}
    sequence_actions: list[dict[str, object]] = []
    pre_commit_aborts: list[AbortCondition] = []
    post_validation: dict[str, object] = {}
    try:
        transaction = transaction_session.begin()
        add_log(log_rows, "info", "Transaction started.")
        export_files = export_pre_cleanup(transaction_session, export_dir, target_ids)
        add_log(log_rows, "info", f"Pre-cleanup exports generated: {', '.join(export_files)}")

        deleted_counts, sequence_actions = execute_deletes(
            transaction_session,
            target_ids,
            args,
            approved_targets,
        )
        add_log(log_rows, "info", "Delete plan executed inside transaction.")

        pre_commit_aborts = pre_commit_validations(
            transaction_session,
            target_ids,
            current_state["forbidden_sequence_values"],
            current_state["lot_sequence_snapshot"],
        )
        if pre_commit_aborts:
            for item in pre_commit_aborts:
                add_log(log_rows, "error", item.detail)
            raise RuntimeError("Pre-commit validations failed.")

        transaction.commit()
        add_log(log_rows, "info", "Transaction committed successfully.")
    except Exception as exc:
        transaction_session.rollback()
        add_log(log_rows, "error", f"Rollback executed: {exc}")
        write_json(
            export_dir / "deleted_counts.json",
            {
                "mode": "execute",
                "counts": deleted_counts,
                "rolled_back": True,
            },
        )
        write_json(
            export_dir / "sequence_actions.json",
            {
                "mode": "execute",
                "actions": sequence_actions,
                "rolled_back": True,
            },
        )
        write_json(
            export_dir / "post_cleanup_validation.json",
            {
                "mode": "execute",
                "status": "rolled_back",
                "pre_commit_aborts": [
                    {"code": item.code, "detail": item.detail, "blocking": item.blocking}
                    for item in pre_commit_aborts
                ],
            },
        )
        write_json(export_dir / "cleanup_execution_log.json", {"events": log_rows})
        transaction_session.close()
        engine.dispose()
        raise
    finally:
        if transaction_session.is_active:
            transaction_session.rollback()
        transaction_session.close()

    post_session = session_factory()
    try:
        post_validation = validate_post_commit(
            post_session,
            target_ids,
            sequence_actions,
            current_state["forbidden_sequence_values"],
            current_state["lot_sequence_snapshot"],
        )
        add_log(log_rows, "info", "Post-commit validation completed.")
    finally:
        post_session.rollback()
        post_session.close()
        engine.dispose()

    write_json(
        export_dir / "deleted_counts.json",
        {
            "mode": "execute",
            "counts": deleted_counts,
            "rolled_back": False,
        },
    )
    write_json(
        export_dir / "sequence_actions.json",
        {
            "mode": "execute",
            "actions": sequence_actions,
            "rolled_back": False,
        },
    )
    write_json(export_dir / "post_cleanup_validation.json", post_validation)
    write_json(export_dir / "cleanup_execution_log.json", {"events": log_rows})


if __name__ == "__main__":
    main()
