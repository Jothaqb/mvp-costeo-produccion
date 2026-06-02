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
    B2CSalesOrderLine,
    ImportBatch,
    ImportedBomHeader,
    ImportedBomLine,
    InventoryAdjustment,
    InventoryAdjustmentPostToken,
    InventoryBalance,
    InventoryTransaction,
    PackagingBatch,
    PackagingBatchActivity,
    PackagingBatchLine,
    PackagingBatchLineMaterial,
    Product,
    ProductionOrder,
    ProductionOrderActivity,
    ProductionOrderMaterial,
    PurchaseOrder,
    PurchaseOrderLine,
    PurchaseOrderReceiveToken,
)


DRY_RUN_CONFIRM_VALUE = "READ_ONLY_MAY_2026"
OPENING_BALANCE_TYPE = "opening_balance"

EXPECTED_SOURCE_TYPES = {
    None,
    "production_order",
    "production_order_reversal",
    "b2b_order",
    "b2c_order",
    "purchase_order",
    "inventory_adjustment",
    "packaging_batch",
}

DEPENDENCY_POST_MAY_SAME_PRODUCT = "POST_MAY_SAME_PRODUCT_MOVEMENT"
DEPENDENCY_POST_MAY_SAME_SOURCE = "POST_MAY_SAME_SOURCE_DOCUMENT"
DEPENDENCY_OUT_OF_WINDOW_DOCUMENT_LINKED = "OUT_OF_WINDOW_DOCUMENT_LINKED_TO_MAY_LEDGER"
DEPENDENCY_MAY_DOCUMENT_OUT_OF_WINDOW_LEDGER = "MAY_DOCUMENT_WITH_OUT_OF_WINDOW_LEDGER"
DEPENDENCY_OUT_OF_WINDOW_CHILD_OF_MAY = "OUT_OF_WINDOW_CHILD_OF_MAY_HEADER"
DEPENDENCY_MAY_CHILD_OF_OUT_OF_WINDOW = "MAY_CHILD_OF_OUT_OF_WINDOW_HEADER"
DEPENDENCY_UNKNOWN = "UNKNOWN_DEPENDENCY"

DOCUMENT_SEQUENCE_PLAN = {
    "production_order": {
        "label": "Production Orders",
        "table_name": "production_orders",
        "proposed_next_value": 1,
        "number_field": "internal_order_number",
        "pattern": re.compile(r"^OP(\d+)$"),
        "behavior": "strict_sequence",
    },
    "b2b_sales_order": {
        "label": "B2B Sales Orders",
        "table_name": "b2b_sales_orders",
        "proposed_next_value": 1,
        "number_field": "order_number",
        "pattern": re.compile(r"^B2B(\d+)$"),
        "behavior": "bootstrap_safe",
    },
    "b2c_sales_order": {
        "label": "B2C Sales Orders",
        "table_name": "b2c_sales_orders",
        "proposed_next_value": 1,
        "number_field": "order_number",
        "pattern": re.compile(r"^B2C(\d+)$"),
        "behavior": "bootstrap_safe",
    },
    "purchase_order": {
        "label": "Purchase Orders",
        "table_name": "purchase_orders",
        "proposed_next_value": 1,
        "number_field": "po_number",
        "pattern": re.compile(r"^PO(\d+)$"),
        "behavior": "strict_sequence",
    },
    "inventory_adjustment": {
        "label": "Inventory Adjustments",
        "table_name": "inventory_adjustments",
        "proposed_next_value": 1,
        "number_field": "adjustment_number",
        "pattern": re.compile(r"^ADJ(\d+)$"),
        "behavior": "strict_sequence",
    },
    "packaging_batch": {
        "label": "Packaging Batches",
        "table_name": "packaging_batches",
        "proposed_next_value": 1,
        "number_field": "internal_batch_number",
        "pattern": re.compile(r"^PB(\d+)$"),
        "behavior": "strict_sequence",
    },
}

DOCUMENT_SPECS = {
    "production_order": {
        "table_name": "production_orders",
        "model": ProductionOrder,
        "number_field": "internal_order_number",
        "date_field": "production_date",
        "date_kind": "date",
        "status_field": "status",
    },
    "b2b_order": {
        "table_name": "b2b_sales_orders",
        "model": B2BSalesOrder,
        "number_field": "order_number",
        "date_field": "delivery_date",
        "date_kind": "date",
        "status_field": "status",
    },
    "b2c_order": {
        "table_name": "b2c_sales_orders",
        "model": B2CSalesOrder,
        "number_field": "order_number",
        "date_field": "order_date",
        "date_kind": "date",
        "status_field": "status",
    },
    "purchase_order": {
        "table_name": "purchase_orders",
        "model": PurchaseOrder,
        "number_field": "po_number",
        "date_field": "po_date",
        "date_kind": "date",
        "status_field": "status",
    },
    "inventory_adjustment": {
        "table_name": "inventory_adjustments",
        "model": InventoryAdjustment,
        "number_field": "adjustment_number",
        "date_field": "adjustment_date",
        "date_kind": "date",
        "status_field": "status",
    },
    "packaging_batch": {
        "table_name": "packaging_batches",
        "model": PackagingBatch,
        "number_field": "internal_batch_number",
        "date_field": "production_date",
        "date_kind": "date",
        "status_field": "status",
    },
}

TABLE_DATE_SPECS = [
    ("inventory_transactions", InventoryTransaction, "transaction_date", "datetime"),
    ("inventory_transactions", InventoryTransaction, "created_at", "datetime"),
    ("production_orders", ProductionOrder, "production_date", "date"),
    ("production_orders", ProductionOrder, "created_at", "datetime"),
    ("production_orders", ProductionOrder, "closed_at", "datetime"),
    ("b2b_sales_orders", B2BSalesOrder, "delivery_date", "date"),
    ("b2b_sales_orders", B2BSalesOrder, "created_at", "datetime"),
    ("b2c_sales_orders", B2CSalesOrder, "order_date", "date"),
    ("b2c_sales_orders", B2CSalesOrder, "created_at", "datetime"),
    ("purchase_orders", PurchaseOrder, "po_date", "date"),
    ("purchase_orders", PurchaseOrder, "created_at", "datetime"),
    ("inventory_adjustments", InventoryAdjustment, "adjustment_date", "date"),
    ("inventory_adjustments", InventoryAdjustment, "created_at", "datetime"),
    ("packaging_batches", PackagingBatch, "production_date", "date"),
    ("packaging_batches", PackagingBatch, "created_at", "datetime"),
    ("packaging_batches", PackagingBatch, "closed_at", "datetime"),
    ("production_order_materials", ProductionOrderMaterial, "created_at", "datetime"),
    ("production_order_activities", ProductionOrderActivity, "created_at", "datetime"),
    ("b2b_sales_order_lines", B2BSalesOrderLine, "created_at", "datetime"),
    ("b2c_sales_order_lines", B2CSalesOrderLine, "created_at", "datetime"),
    ("purchase_order_lines", PurchaseOrderLine, "created_at", "datetime"),
    ("purchase_order_receive_tokens", PurchaseOrderReceiveToken, "created_at", "datetime"),
    ("purchase_order_receive_tokens", PurchaseOrderReceiveToken, "used_at", "datetime"),
    ("inventory_adjustment_post_tokens", InventoryAdjustmentPostToken, "created_at", "datetime"),
    ("inventory_adjustment_post_tokens", InventoryAdjustmentPostToken, "used_at", "datetime"),
    ("packaging_batch_lines", PackagingBatchLine, "created_at", "datetime"),
    ("packaging_batch_lines", PackagingBatchLine, "updated_at", "datetime"),
    ("packaging_batch_line_materials", PackagingBatchLineMaterial, "created_at", "datetime"),
    ("packaging_batch_line_materials", PackagingBatchLineMaterial, "updated_at", "datetime"),
    ("packaging_batch_activities", PackagingBatchActivity, "created_at", "datetime"),
    ("packaging_batch_activities", PackagingBatchActivity, "updated_at", "datetime"),
    ("import_batches", ImportBatch, "imported_at", "datetime"),
    ("imported_bom_headers", ImportedBomHeader, "imported_at", "datetime"),
    ("imported_bom_lines", ImportedBomLine, "created_at", "datetime"),
]

CHILD_ANALYSIS_SPECS = [
    {
        "child_table": "production_order_materials",
        "child_model": ProductionOrderMaterial,
        "child_id_field": "id",
        "parent_model": ProductionOrder,
        "parent_source_type": "production_order",
        "parent_table": "production_orders",
        "parent_fk_field": "production_order_id",
        "parent_number_field": "internal_order_number",
        "parent_business_date_field": "production_date",
        "child_date_field": None,
    },
    {
        "child_table": "production_order_activities",
        "child_model": ProductionOrderActivity,
        "child_id_field": "id",
        "parent_model": ProductionOrder,
        "parent_source_type": "production_order",
        "parent_table": "production_orders",
        "parent_fk_field": "production_order_id",
        "parent_number_field": "internal_order_number",
        "parent_business_date_field": "production_date",
        "child_date_field": None,
    },
    {
        "child_table": "b2b_sales_order_lines",
        "child_model": B2BSalesOrderLine,
        "child_id_field": "id",
        "parent_model": B2BSalesOrder,
        "parent_source_type": "b2b_order",
        "parent_table": "b2b_sales_orders",
        "parent_fk_field": "sales_order_id",
        "parent_number_field": "order_number",
        "parent_business_date_field": "delivery_date",
        "child_date_field": "created_at",
    },
    {
        "child_table": "b2c_sales_order_lines",
        "child_model": B2CSalesOrderLine,
        "child_id_field": "id",
        "parent_model": B2CSalesOrder,
        "parent_source_type": "b2c_order",
        "parent_table": "b2c_sales_orders",
        "parent_fk_field": "sales_order_id",
        "parent_number_field": "order_number",
        "parent_business_date_field": "order_date",
        "child_date_field": "created_at",
    },
    {
        "child_table": "purchase_order_lines",
        "child_model": PurchaseOrderLine,
        "child_id_field": "id",
        "parent_model": PurchaseOrder,
        "parent_source_type": "purchase_order",
        "parent_table": "purchase_orders",
        "parent_fk_field": "purchase_order_id",
        "parent_number_field": "po_number",
        "parent_business_date_field": "po_date",
        "child_date_field": "created_at",
    },
    {
        "child_table": "purchase_order_receive_tokens",
        "child_model": PurchaseOrderReceiveToken,
        "child_id_field": "id",
        "parent_model": PurchaseOrder,
        "parent_source_type": "purchase_order",
        "parent_table": "purchase_orders",
        "parent_fk_field": "purchase_order_id",
        "parent_number_field": "po_number",
        "parent_business_date_field": "po_date",
        "child_date_field": "created_at",
    },
    {
        "child_table": "packaging_batch_lines",
        "child_model": PackagingBatchLine,
        "child_id_field": "id",
        "parent_model": PackagingBatch,
        "parent_source_type": "packaging_batch",
        "parent_table": "packaging_batches",
        "parent_fk_field": "packaging_batch_id",
        "parent_number_field": "internal_batch_number",
        "parent_business_date_field": "production_date",
        "child_date_field": "created_at",
    },
    {
        "child_table": "packaging_batch_activities",
        "child_model": PackagingBatchActivity,
        "child_id_field": "id",
        "parent_model": PackagingBatch,
        "parent_source_type": "packaging_batch",
        "parent_table": "packaging_batches",
        "parent_fk_field": "packaging_batch_id",
        "parent_number_field": "internal_batch_number",
        "parent_business_date_field": "production_date",
        "child_date_field": "created_at",
    },
]


@dataclass(frozen=True)
class AbortCondition:
    code: str
    detail: str
    blocking: bool = True


ENV_ARG_MAP = {
    "dry_run_confirm": "MAY_CLEANUP_DRY_RUN_CONFIRM",
    "start_datetime": "MAY_CLEANUP_START",
    "end_datetime": "MAY_CLEANUP_END",
    "export_dir": "MAY_CLEANUP_EXPORT_DIR",
    "expected_db_host_fragment": "MAY_CLEANUP_EXPECTED_DB_HOST_FRAGMENT",
    "expected_db_name": "MAY_CLEANUP_EXPECTED_DB_NAME",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read-only dry-run for May 2026 cleanup pre-go-live."
    )
    parser.add_argument("--use-env", action="store_true")
    parser.add_argument("--dry-run-confirm")
    parser.add_argument("--start-datetime")
    parser.add_argument("--end-datetime")
    parser.add_argument("--export-dir")
    parser.add_argument("--expected-db-host-fragment")
    parser.add_argument("--expected-db-name")
    return parser.parse_args()


def resolve_args(args: argparse.Namespace) -> argparse.Namespace:
    if args.use_env:
        conflicting_cli = [
            flag
            for flag, value in (
                ("--dry-run-confirm", args.dry_run_confirm),
                ("--start-datetime", args.start_datetime),
                ("--end-datetime", args.end_datetime),
                ("--export-dir", args.export_dir),
                ("--expected-db-host-fragment", args.expected_db_host_fragment),
                ("--expected-db-name", args.expected_db_name),
            )
            if value
        ]
        if conflicting_cli:
            raise SystemExit(
                "Do not mix --use-env with explicit CLI parameter flags. "
                "Use exactly one mode: full CLI arguments or --use-env. "
                "Conflicting flags: " + ", ".join(conflicting_cli)
            )
        missing_env = [
            env_name
            for env_name in ENV_ARG_MAP.values()
            if not os.getenv(env_name)
        ]
        if missing_env:
            raise SystemExit(
                "Missing required environment variables for --use-env: "
                + ", ".join(missing_env)
            )
        return argparse.Namespace(
            use_env=True,
            dry_run_confirm=os.getenv(ENV_ARG_MAP["dry_run_confirm"]),
            start_datetime=os.getenv(ENV_ARG_MAP["start_datetime"]),
            end_datetime=os.getenv(ENV_ARG_MAP["end_datetime"]),
            export_dir=os.getenv(ENV_ARG_MAP["export_dir"]),
            expected_db_host_fragment=os.getenv(ENV_ARG_MAP["expected_db_host_fragment"]),
            expected_db_name=os.getenv(ENV_ARG_MAP["expected_db_name"]),
        )

    missing_cli = [
        flag
        for flag, value in (
            ("--dry-run-confirm", args.dry_run_confirm),
            ("--start-datetime", args.start_datetime),
            ("--end-datetime", args.end_datetime),
            ("--export-dir", args.export_dir),
            ("--expected-db-host-fragment", args.expected_db_host_fragment),
            ("--expected-db-name", args.expected_db_name),
        )
        if not value
    ]
    if missing_cli:
        raise SystemExit(
            "Missing required CLI arguments: " + ", ".join(missing_cli)
        )
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


def ensure_read_only_prerequisites(args: argparse.Namespace) -> tuple[str, dict[str, str]]:
    if args.dry_run_confirm != DRY_RUN_CONFIRM_VALUE:
        raise SystemExit(
            "Dry-run confirmation mismatch. Use "
            f'--dry-run-confirm "{DRY_RUN_CONFIRM_VALUE}" to continue.'
        )

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
        raise SystemExit("SQLite environments are not allowed for this dry-run.")
    if args.expected_db_host_fragment not in host:
        raise SystemExit(
            "DATABASE_URL host does not match --expected-db-host-fragment."
        )
    if database_name != args.expected_db_name:
        raise SystemExit(
            "DATABASE_URL database name does not match --expected-db-name."
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
            writer.writerow(
                {header: serialize_value(row.get(header)) for header in headers}
            )


def write_json(path: Path, payload: dict[str, object]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=True, default=str)


def lower_bound_for_kind(kind: str, start_dt: datetime) -> date | datetime:
    return start_dt if kind == "datetime" else start_dt.date()


def upper_bound_for_kind(kind: str, end_dt: datetime) -> date | datetime:
    return end_dt if kind == "datetime" else end_dt.date()


def collect_count_by_status(
    session: Session,
    model,
    date_field_name: str,
    date_kind: str,
    start_dt: datetime,
    end_dt: datetime,
) -> dict[str, int]:
    if not hasattr(model, "status") or not hasattr(model, date_field_name):
        return {}
    date_field = getattr(model, date_field_name)
    lower = lower_bound_for_kind(date_kind, start_dt)
    upper = upper_bound_for_kind(date_kind, end_dt)
    rows = (
        session.query(getattr(model, "status"), func.count())
        .filter(date_field >= lower, date_field < upper)
        .group_by(getattr(model, "status"))
        .all()
    )
    return {str(status): int(count) for status, count in rows}


def add_abort(
    abort_conditions: list[AbortCondition],
    code: str,
    detail: str,
    *,
    blocking: bool = True,
) -> None:
    abort_conditions.append(AbortCondition(code=code, detail=detail, blocking=blocking))


def add_suspicious(
    suspicious_records: list[dict[str, object]],
    *,
    category: str,
    table_name: str,
    record_id: object,
    reason: str,
    linked_source_type: str | None = None,
    linked_source_id: int | None = None,
    date_value: object = None,
    action_required: str = "",
    blocking: bool = False,
) -> None:
    suspicious_records.append(
        {
            "category": category,
            "table_name": table_name,
            "record_id": record_id,
            "reason": reason,
            "linked_source_type": linked_source_type,
            "linked_source_id": linked_source_id,
            "date": date_value,
            "action_required": action_required,
            "blocking": blocking,
        }
    )


def record_dependency(
    dependency_rows: list[dict[str, object]],
    suspicious_records: list[dict[str, object]],
    *,
    category: str,
    severity: str,
    table_name: str,
    record_id: object,
    detail: str,
    blocking: bool,
    source_type: str | None = None,
    source_id: int | None = None,
    product_id: int | None = None,
    sku: str | None = None,
    business_date: object = None,
    transaction_date: object = None,
    document_number: str | None = None,
) -> None:
    dependency_rows.append(
        {
            "category": category,
            "severity": severity,
            "table_name": table_name,
            "record_id": record_id,
            "document_number": document_number,
            "source_type": source_type,
            "source_id": source_id,
            "product_id": product_id,
            "sku": sku,
            "business_date": business_date,
            "transaction_date": transaction_date,
            "detail": detail,
            "blocking": blocking,
        }
    )
    add_suspicious(
        suspicious_records,
        category=category,
        table_name=table_name,
        record_id=record_id,
        reason=detail,
        linked_source_type=source_type,
        linked_source_id=source_id,
        date_value=transaction_date or business_date,
        action_required="Review dependency before any cleanup execution.",
        blocking=blocking,
    )


def new_document_row(
    *,
    table_name: str,
    source_type: str,
    document_id: int,
    document_number: str,
    business_date: object,
    status: str | None,
    has_ledger_in_may: bool,
    may_ledger_txn_count: int,
) -> dict[str, object]:
    return {
        "table_name": table_name,
        "source_type": source_type,
        "document_id": document_id,
        "document_number": document_number,
        "business_date": business_date,
        "status": status,
        "has_ledger_in_may": has_ledger_in_may,
        "may_ledger_txn_count": may_ledger_txn_count,
        "out_of_window_ledger_txn_count": 0,
        "classification": "",
        "reason": "",
        "requires_manual_review": False,
        "blocking": False,
        "child_count_summary": "",
    }


def ensure_document_row(
    rows_by_key: dict[tuple[str, int], dict[str, object]],
    *,
    table_name: str,
    source_type: str,
    document_id: int,
    document_number: str,
    business_date: object,
    status: str | None,
    has_ledger_in_may: bool,
    may_ledger_txn_count: int,
) -> dict[str, object]:
    key = (table_name, document_id)
    if key not in rows_by_key:
        rows_by_key[key] = new_document_row(
            table_name=table_name,
            source_type=source_type,
            document_id=document_id,
            document_number=document_number,
            business_date=business_date,
            status=status,
            has_ledger_in_may=has_ledger_in_may,
            may_ledger_txn_count=may_ledger_txn_count,
        )
    return rows_by_key[key]


def set_document_classification(
    row: dict[str, object],
    classification: str,
    reason: str,
    *,
    manual_review: bool = False,
    blocking: bool = False,
) -> None:
    current = str(row.get("classification") or "")
    priority = {"": 0, "B": 1, "A": 2, "C": 3}
    if priority[classification] >= priority.get(current, 0):
        row["classification"] = classification
        row["reason"] = reason
    if manual_review:
        row["requires_manual_review"] = True
    if blocking:
        row["blocking"] = True


def is_within_window(value: object, start_dt: datetime, end_dt: datetime) -> bool:
    if isinstance(value, datetime):
        comparison = value.replace(tzinfo=None) if value.tzinfo else value
        return start_dt <= comparison < end_dt
    if isinstance(value, date):
        return start_dt.date() <= value < end_dt.date()
    return False


def build_table_date_range(
    session: Session,
    *,
    table_name: str,
    model,
    field_name: str,
    kind: str,
    start_dt: datetime,
    end_dt: datetime,
    suspicious_records: list[dict[str, object]],
) -> dict[str, object]:
    total_count = session.query(func.count()).select_from(model).scalar() or 0
    if not hasattr(model, field_name):
        add_suspicious(
            suspicious_records,
            category="UNAVAILABLE_DATE_FIELD",
            table_name=table_name,
            record_id=field_name,
            reason=f"Field '{field_name}' is unavailable on model {model.__name__}.",
            action_required="Review whether this table needs an alternate date field.",
            blocking=False,
        )
        return {
            "table_name": table_name,
            "date_field": field_name,
            "field_status": "unavailable",
            "min_date": None,
            "max_date": None,
            "count_total": int(total_count),
            "count_in_may_window": None,
            "count_before_window": None,
            "count_after_window": None,
        }

    column = getattr(model, field_name)
    lower = lower_bound_for_kind(kind, start_dt)
    upper = upper_bound_for_kind(kind, end_dt)
    min_date, max_date = session.query(func.min(column), func.max(column)).one()
    count_in_window = (
        session.query(func.count())
        .select_from(model)
        .filter(column >= lower, column < upper)
        .scalar()
        or 0
    )
    count_before_window = (
        session.query(func.count()).select_from(model).filter(column < lower).scalar() or 0
    )
    count_after_window = (
        session.query(func.count()).select_from(model).filter(column >= upper).scalar() or 0
    )
    return {
        "table_name": table_name,
        "date_field": field_name,
        "field_status": "available",
        "min_date": min_date,
        "max_date": max_date,
        "count_total": int(total_count),
        "count_in_may_window": int(count_in_window),
        "count_before_window": int(count_before_window),
        "count_after_window": int(count_after_window),
    }


def build_child_count_summary(
    source_type: str,
    document_id: int,
    child_count_maps: dict[str, dict[str, Counter[int]]],
) -> str:
    summary_parts: list[str] = []
    for label, counter in child_count_maps.get(source_type, {}).items():
        summary_parts.append(f"{label}:{counter.get(document_id, 0)}")
    return ", ".join(summary_parts)


def child_counter_summary(summary_text: str) -> dict[str, int]:
    result: dict[str, int] = {}
    for raw_part in (summary_text or "").split(","):
        part = raw_part.strip()
        if not part or ":" not in part:
            continue
        label, raw_value = part.split(":", 1)
        try:
            result[label.strip()] = int(raw_value.strip())
        except ValueError:
            continue
    return result


def parse_numeric_suffix(value: str | None, pattern: re.Pattern[str]) -> int | None:
    if not value:
        return None
    match = pattern.fullmatch(value)
    if match is None:
        return None
    try:
        return int(match.group(1))
    except (ValueError, IndexError):
        return None


def fetch_document_projection(
    session: Session,
    *,
    model,
    number_field: str,
    business_date_field: str,
    status_field: str,
) -> list[dict[str, object]]:
    rows = (
        session.query(
            model.id,
            getattr(model, number_field),
            getattr(model, business_date_field),
            getattr(model, status_field),
        )
        .all()
    )
    return [
        {
            "id": row[0],
            "document_number": row[1],
            "business_date": row[2],
            "status": row[3],
        }
        for row in rows
    ]


def main() -> None:
    args = resolve_args(parse_args())
    start_dt = parse_window(args.start_datetime)
    end_dt = parse_window(args.end_datetime)
    if not start_dt < end_dt:
        raise SystemExit("--start-datetime must be earlier than --end-datetime.")

    database_url, db_context = ensure_read_only_prerequisites(args)
    export_dir = make_export_dir(args.export_dir)

    print("DRY RUN ONLY - NO DATABASE WRITES")
    print(f"Database host: {db_context['host_masked']}")
    print(f"Database name: {db_context['database_name']}")

    engine = create_engine(database_url)
    if engine.dialect.name != "postgresql":
        raise SystemExit("Detected dialect is not postgresql.")

    inspector = inspect(engine)
    session_factory = sessionmaker(bind=engine, autocommit=False, autoflush=False)
    session = session_factory()

    abort_conditions: list[AbortCondition] = []
    suspicious_records: list[dict[str, object]] = []
    dependency_rows: list[dict[str, object]] = []
    child_scope_rows: list[dict[str, object]] = []

    try:
        session.execute(text("SET TRANSACTION READ ONLY"))

        required_tables = {
            "inventory_transactions",
            "inventory_balances",
            "production_orders",
            "production_order_materials",
            "production_order_activities",
            "b2b_sales_orders",
            "b2b_sales_order_lines",
            "b2c_sales_orders",
            "b2c_sales_order_lines",
            "purchase_orders",
            "purchase_order_lines",
            "purchase_order_receive_tokens",
            "inventory_adjustments",
            "packaging_batches",
            "packaging_batch_lines",
            "packaging_batch_line_materials",
            "packaging_batch_activities",
            "app_sequences",
            "lot_sequences",
        }
        optional_tables = {
            "inventory_adjustment_post_tokens",
            "import_batches",
            "imported_bom_headers",
            "imported_bom_lines",
        }
        existing_tables = set(inspector.get_table_names())
        missing_required = sorted(required_tables - existing_tables)
        missing_optional = sorted(optional_tables - existing_tables)
        if missing_required:
            add_abort(
                abort_conditions,
                "missing_required_tables",
                "Missing required tables: " + ", ".join(missing_required),
            )
        for table_name in missing_optional:
            add_suspicious(
                suspicious_records,
                category="MISSING_OPTIONAL_TABLE",
                table_name=table_name,
                record_id="(table)",
                reason=f"Optional diagnostic table '{table_name}' is missing.",
                action_required="Confirm whether this environment intentionally lacks the table.",
                blocking=False,
            )

        table_date_ranges = [
            build_table_date_range(
                session,
                table_name=table_name,
                model=model,
                field_name=field_name,
                kind=kind,
                start_dt=start_dt,
                end_dt=end_dt,
                suspicious_records=suspicious_records,
            )
            for table_name, model, field_name, kind in TABLE_DATE_SPECS
        ]

        opening_balance_count = (
            session.query(func.count())
            .select_from(InventoryTransaction)
            .filter(InventoryTransaction.transaction_type == OPENING_BALANCE_TYPE)
            .scalar()
            or 0
        )
        if opening_balance_count > 0:
            add_abort(
                abort_conditions,
                "opening_balance_exists",
                f"Found {opening_balance_count} opening balance transactions.",
            )

        may_transactions = (
            session.query(InventoryTransaction, Product)
            .join(Product, Product.id == InventoryTransaction.product_id)
            .filter(
                InventoryTransaction.transaction_date >= start_dt,
                InventoryTransaction.transaction_date < end_dt,
            )
            .order_by(InventoryTransaction.transaction_date, InventoryTransaction.id)
            .all()
        )

        may_tx_by_source: dict[tuple[str | None, int | None], list[InventoryTransaction]] = defaultdict(list)
        tx_type_counts: Counter[str] = Counter()
        source_type_counts: Counter[str] = Counter()
        involved_product_ids: set[int] = set()
        involved_tx_ids: set[int] = set()
        inventory_scope_rows: list[dict[str, object]] = []
        unexpected_source_types: set[str] = set()

        for txn, product in may_transactions:
            may_tx_by_source[(txn.source_type, txn.source_id)].append(txn)
            tx_type_counts[txn.transaction_type] += 1
            source_type_counts[str(txn.source_type)] += 1
            involved_product_ids.add(txn.product_id)
            involved_tx_ids.add(txn.id)
            if txn.source_type not in EXPECTED_SOURCE_TYPES:
                unexpected_source_types.add(str(txn.source_type))
            inventory_scope_rows.append(
                {
                    "txn_id": txn.id,
                    "transaction_date": txn.transaction_date,
                    "product_id": txn.product_id,
                    "sku": product.sku,
                    "transaction_type": txn.transaction_type,
                    "source_type": txn.source_type,
                    "source_id": txn.source_id,
                    "source_line_id": txn.source_line_id,
                    "quantity_in": txn.quantity_in,
                    "quantity_out": txn.quantity_out,
                    "unit_cost": txn.unit_cost,
                    "total_cost": txn.total_cost,
                    "running_quantity": txn.running_quantity,
                    "running_average_cost": txn.running_average_cost,
                    "running_inventory_value": txn.running_inventory_value,
                }
            )

        if unexpected_source_types:
            add_abort(
                abort_conditions,
                "unexpected_source_types",
                "Unexpected source_type values found: " + ", ".join(sorted(unexpected_source_types)),
            )

        may_source_pairs = {
            (txn.source_type, txn.source_id)
            for txn, _ in may_transactions
            if txn.source_type is not None and txn.source_id is not None
        }

        post_may_same_product_rows: list[tuple[InventoryTransaction, Product]] = []
        if involved_product_ids:
            post_may_same_product_rows = (
                session.query(InventoryTransaction, Product)
                .join(Product, Product.id == InventoryTransaction.product_id)
                .filter(
                    InventoryTransaction.product_id.in_(sorted(involved_product_ids)),
                    InventoryTransaction.transaction_date >= end_dt,
                )
                .order_by(InventoryTransaction.transaction_date, InventoryTransaction.id)
                .all()
            )
        for txn, product in post_may_same_product_rows:
            record_dependency(
                dependency_rows,
                suspicious_records,
                category=DEPENDENCY_POST_MAY_SAME_PRODUCT,
                severity="warning",
                table_name="inventory_transactions",
                record_id=txn.id,
                detail="Post-May movement found for a product involved in the May scope.",
                blocking=False,
                source_type=txn.source_type,
                source_id=txn.source_id,
                product_id=txn.product_id,
                sku=product.sku,
                transaction_date=txn.transaction_date,
            )

        outside_tx_by_source: dict[tuple[str | None, int | None], list[InventoryTransaction]] = defaultdict(list)
        if may_source_pairs:
            may_source_ids_by_type: dict[str, set[int]] = defaultdict(set)
            for source_type, source_id in may_source_pairs:
                may_source_ids_by_type[source_type].add(source_id)

            for source_type, source_ids in may_source_ids_by_type.items():
                rows = (
                    session.query(InventoryTransaction)
                    .filter(
                        InventoryTransaction.source_type == source_type,
                        InventoryTransaction.source_id.in_(sorted(source_ids)),
                        or_(
                            InventoryTransaction.transaction_date < start_dt,
                            InventoryTransaction.transaction_date >= end_dt,
                        ),
                    )
                    .order_by(InventoryTransaction.transaction_date, InventoryTransaction.id)
                    .all()
                )
                for txn in rows:
                    outside_tx_by_source[(txn.source_type, txn.source_id)].append(txn)
                    category = (
                        DEPENDENCY_POST_MAY_SAME_SOURCE
                        if txn.transaction_date >= end_dt
                        else DEPENDENCY_UNKNOWN
                    )
                    detail = (
                        "Post-May inventory transaction found for the same source document as a May ledger posting."
                        if txn.transaction_date >= end_dt
                        else "Pre-May inventory transaction found for the same source document as a May ledger posting."
                    )
                    record_dependency(
                        dependency_rows,
                        suspicious_records,
                        category=category,
                        severity="error",
                        table_name="inventory_transactions",
                        record_id=txn.id,
                        detail=detail,
                        blocking=True,
                        source_type=txn.source_type,
                        source_id=txn.source_id,
                        product_id=txn.product_id,
                        transaction_date=txn.transaction_date,
                    )

        if any(row["category"] in {DEPENDENCY_POST_MAY_SAME_SOURCE, DEPENDENCY_UNKNOWN} for row in dependency_rows):
            add_abort(
                abort_conditions,
                "same_source_document_outside_window",
                "Found out-of-window inventory transactions for the same source document as May ledger postings.",
            )

        may_documents_by_source: dict[str, dict[int, object]] = {}
        all_documents_by_source: dict[str, list[dict[str, object]]] = {}
        document_rows_by_key: dict[tuple[str, int], dict[str, object]] = {}

        may_documents: dict[str, list[object]] = {}
        for source_type, spec in DOCUMENT_SPECS.items():
            model = spec["model"]
            date_field = getattr(model, spec["date_field"])
            may_documents[source_type] = (
                session.query(model)
                .filter(
                    date_field >= start_dt.date(),
                    date_field < end_dt.date(),
                )
                .all()
            )
            may_documents_by_source[source_type] = {doc.id: doc for doc in may_documents[source_type]}
            all_documents_by_source[source_type] = fetch_document_projection(
                session,
                model=model,
                number_field=str(spec["number_field"]),
                business_date_field=str(spec["date_field"]),
                status_field=str(spec["status_field"]),
            )

        child_count_maps: dict[str, dict[str, Counter[int]]] = defaultdict(dict)

        def append_child_scope(
            *,
            table_name: str,
            child_id: int,
            parent_table: str,
            parent_id: int | None,
            parent_document_number: str | None,
            parent_source_type: str | None,
            parent_business_date: object,
            parent_in_may_window: bool,
            parent_classification: str,
            child_date_field: str,
            child_date: object,
            child_in_may_window: bool | None,
            issue: str,
            blocking: bool,
        ) -> None:
            child_scope_rows.append(
                {
                    "table_name": table_name,
                    "child_id": child_id,
                    "parent_table": parent_table,
                    "parent_id": parent_id,
                    "parent_document_number": parent_document_number,
                    "parent_source_type": parent_source_type,
                    "parent_business_date": parent_business_date,
                    "parent_in_may_window": parent_in_may_window,
                    "parent_classification": parent_classification,
                    "child_date_field": child_date_field,
                    "child_date": child_date,
                    "child_in_may_window": child_in_may_window,
                    "issue": issue,
                    "blocking": blocking,
                }
            )

        def analyze_child_rows(spec: dict[str, object]) -> Counter[int]:
            child_model = spec["child_model"]
            child_table = str(spec["child_table"])
            child_id_field = str(spec["child_id_field"])
            parent_model = spec["parent_model"]
            parent_source_type = str(spec["parent_source_type"])
            parent_table = str(spec["parent_table"])
            parent_fk_field = str(spec["parent_fk_field"])
            parent_number_field = str(spec["parent_number_field"])
            parent_business_date_field = str(spec["parent_business_date_field"])
            child_date_field = spec["child_date_field"]
            parent_ids_may = set(may_documents_by_source[parent_source_type].keys())
            filters = []
            parent_fk_column = getattr(child_model, parent_fk_field)
            if parent_ids_may:
                filters.append(parent_fk_column.in_(sorted(parent_ids_may)))
            if child_date_field and hasattr(child_model, child_date_field):
                child_date_column = getattr(child_model, child_date_field)
                filters.append(
                    (child_date_column >= start_dt) & (child_date_column < end_dt)
                )

            if not filters:
                if child_date_field is None:
                    add_suspicious(
                        suspicious_records,
                        category="CHILD_TABLE_WINDOW_UNAVAILABLE",
                        table_name=child_table,
                        record_id="(table)",
                        reason=f"Child table '{child_table}' has no direct date field for out-of-window diagnostics.",
                        action_required="Treat dependency analysis for this child table as partial.",
                        blocking=False,
                    )
                return Counter()

            rows = (
                session.query(child_model, parent_model)
                .outerjoin(parent_model, parent_fk_column == parent_model.id)
                .filter(or_(*filters))
                .all()
            )
            if child_date_field is None:
                add_suspicious(
                    suspicious_records,
                    category="CHILD_TABLE_WINDOW_UNAVAILABLE",
                    table_name=child_table,
                    record_id="(table)",
                    reason=f"Child table '{child_table}' has no direct date field; May/out-of-window child diagnostics are conservative.",
                    action_required="Review this child table manually if cleanup progresses.",
                    blocking=False,
                )

            count_by_may_parent: Counter[int] = Counter()
            for child, parent in rows:
                child_id = getattr(child, child_id_field)
                parent_id = getattr(child, parent_fk_field)
                child_date = (
                    getattr(child, child_date_field)
                    if child_date_field and hasattr(child, child_date_field)
                    else None
                )
                child_in_may = (
                    is_within_window(child_date, start_dt, end_dt)
                    if child_date is not None
                    else None
                )
                if parent is None:
                    append_child_scope(
                        table_name=child_table,
                        child_id=child_id,
                        parent_table=parent_table,
                        parent_id=parent_id,
                        parent_document_number=None,
                        parent_source_type=parent_source_type,
                        parent_business_date=None,
                        parent_in_may_window=False,
                        parent_classification="unavailable",
                        child_date_field=str(child_date_field or "unavailable"),
                        child_date=child_date,
                        child_in_may_window=child_in_may,
                        issue="ORPHAN_CHILD",
                        blocking=True,
                    )
                    add_suspicious(
                        suspicious_records,
                        category="ORPHAN_CHILD",
                        table_name=child_table,
                        record_id=child_id,
                        reason=f"Child row in '{child_table}' references a missing parent header.",
                        linked_source_type=parent_source_type,
                        linked_source_id=parent_id,
                        date_value=child_date,
                        action_required="Investigate referential integrity before cleanup.",
                        blocking=True,
                    )
                    add_abort(
                        abort_conditions,
                        f"orphan_{child_table}",
                        f"Found orphan child rows in {child_table}.",
                    )
                    continue

                parent_business_date = getattr(parent, parent_business_date_field)
                parent_in_may = is_within_window(parent_business_date, start_dt, end_dt)
                if parent_in_may:
                    count_by_may_parent[parent.id] += 1
                parent_classification = document_rows_by_key.get(
                    (parent_table, parent.id), {}
                ).get("classification", "")
                issue = "LINKED_TO_MAY_HEADER" if parent_in_may else "PARENT_OUTSIDE_MAY"
                blocking = False
                if parent_in_may and child_date is not None and child_in_may is False:
                    issue = DEPENDENCY_OUT_OF_WINDOW_CHILD_OF_MAY
                    record_dependency(
                        dependency_rows,
                        suspicious_records,
                        category=DEPENDENCY_OUT_OF_WINDOW_CHILD_OF_MAY,
                        severity="warning",
                        table_name=child_table,
                        record_id=child_id,
                        detail="Child row linked to a May header has a timestamp outside May.",
                        blocking=False,
                        source_type=parent_source_type,
                        source_id=parent.id,
                        business_date=parent_business_date,
                        transaction_date=child_date,
                        document_number=str(getattr(parent, parent_number_field)),
                    )
                elif (not parent_in_may) and child_date is not None and child_in_may is True:
                    issue = DEPENDENCY_MAY_CHILD_OF_OUT_OF_WINDOW
                    blocking = True
                    record_dependency(
                        dependency_rows,
                        suspicious_records,
                        category=DEPENDENCY_MAY_CHILD_OF_OUT_OF_WINDOW,
                        severity="error",
                        table_name=child_table,
                        record_id=child_id,
                        detail="Child row timestamp is in May but its parent header falls outside May.",
                        blocking=True,
                        source_type=parent_source_type,
                        source_id=parent.id,
                        business_date=parent_business_date,
                        transaction_date=child_date,
                        document_number=str(getattr(parent, parent_number_field)),
                    )
                append_child_scope(
                    table_name=child_table,
                    child_id=child_id,
                    parent_table=parent_table,
                    parent_id=parent.id,
                    parent_document_number=str(getattr(parent, parent_number_field)),
                    parent_source_type=parent_source_type,
                    parent_business_date=parent_business_date,
                    parent_in_may_window=parent_in_may,
                    parent_classification=parent_classification,
                    child_date_field=str(child_date_field or "unavailable"),
                    child_date=child_date,
                    child_in_may_window=child_in_may,
                    issue=issue,
                    blocking=blocking,
                )
            return count_by_may_parent

        for child_spec in CHILD_ANALYSIS_SPECS:
            counter = analyze_child_rows(child_spec)
            parent_source_type = str(child_spec["parent_source_type"])
            child_table = str(child_spec["child_table"])
            if child_table == "production_order_materials":
                child_count_maps[parent_source_type]["materials"] = counter
            elif child_table == "production_order_activities":
                child_count_maps[parent_source_type]["activities"] = counter
            elif child_table == "b2b_sales_order_lines":
                child_count_maps[parent_source_type]["lines"] = counter
            elif child_table == "b2c_sales_order_lines":
                child_count_maps[parent_source_type]["lines"] = counter
            elif child_table == "purchase_order_lines":
                child_count_maps[parent_source_type]["lines"] = counter
            elif child_table == "purchase_order_receive_tokens":
                child_count_maps[parent_source_type]["receive_tokens"] = counter
            elif child_table == "packaging_batch_lines":
                child_count_maps[parent_source_type]["lines"] = counter
            elif child_table == "packaging_batch_activities":
                child_count_maps[parent_source_type]["activities"] = counter

        packaging_material_counter: Counter[int] = Counter()
        packaging_batch_ids_may = set(may_documents_by_source["packaging_batch"].keys())
        packaging_material_filters = []
        if packaging_batch_ids_may:
            packaging_material_filters.append(PackagingBatch.id.in_(sorted(packaging_batch_ids_may)))
        packaging_material_filters.append(
            (PackagingBatchLineMaterial.created_at >= start_dt)
            & (PackagingBatchLineMaterial.created_at < end_dt)
        )
        packaging_material_rows = (
            session.query(PackagingBatchLineMaterial, PackagingBatchLine, PackagingBatch)
            .outerjoin(
                PackagingBatchLine,
                PackagingBatchLineMaterial.packaging_batch_line_id == PackagingBatchLine.id,
            )
            .outerjoin(
                PackagingBatch,
                PackagingBatchLine.packaging_batch_id == PackagingBatch.id,
            )
            .filter(or_(*packaging_material_filters))
            .all()
        )
        for material, line, batch in packaging_material_rows:
            child_in_may = is_within_window(material.created_at, start_dt, end_dt)
            if line is None or batch is None:
                append_child_scope(
                    table_name="packaging_batch_line_materials",
                    child_id=material.id,
                    parent_table="packaging_batches",
                    parent_id=None,
                    parent_document_number=None,
                    parent_source_type="packaging_batch",
                    parent_business_date=None,
                    parent_in_may_window=False,
                    parent_classification="unavailable",
                    child_date_field="created_at",
                    child_date=material.created_at,
                    child_in_may_window=child_in_may,
                    issue="ORPHAN_CHILD",
                    blocking=True,
                )
                add_suspicious(
                    suspicious_records,
                    category="ORPHAN_CHILD",
                    table_name="packaging_batch_line_materials",
                    record_id=material.id,
                    reason="Packaging batch line material is missing its parent line or batch.",
                    linked_source_type="packaging_batch",
                    date_value=material.created_at,
                    action_required="Investigate referential integrity before cleanup.",
                    blocking=True,
                )
                add_abort(
                    abort_conditions,
                    "orphan_packaging_batch_line_materials",
                    "Found orphan packaging_batch_line_materials rows.",
                )
                continue
            parent_in_may = is_within_window(batch.production_date, start_dt, end_dt)
            if parent_in_may:
                packaging_material_counter[batch.id] += 1
            parent_classification = document_rows_by_key.get(
                ("packaging_batches", batch.id), {}
            ).get("classification", "")
            issue = "LINKED_TO_MAY_HEADER" if parent_in_may else "PARENT_OUTSIDE_MAY"
            blocking = False
            if parent_in_may and not child_in_may:
                issue = DEPENDENCY_OUT_OF_WINDOW_CHILD_OF_MAY
                record_dependency(
                    dependency_rows,
                    suspicious_records,
                    category=DEPENDENCY_OUT_OF_WINDOW_CHILD_OF_MAY,
                    severity="warning",
                    table_name="packaging_batch_line_materials",
                    record_id=material.id,
                    detail="Packaging batch line material belongs to a May batch but was created outside May.",
                    blocking=False,
                    source_type="packaging_batch",
                    source_id=batch.id,
                    business_date=batch.production_date,
                    transaction_date=material.created_at,
                    document_number=batch.internal_batch_number,
                )
            elif (not parent_in_may) and child_in_may:
                issue = DEPENDENCY_MAY_CHILD_OF_OUT_OF_WINDOW
                blocking = True
                record_dependency(
                    dependency_rows,
                    suspicious_records,
                    category=DEPENDENCY_MAY_CHILD_OF_OUT_OF_WINDOW,
                    severity="error",
                    table_name="packaging_batch_line_materials",
                    record_id=material.id,
                    detail="Packaging batch line material was created in May but its batch falls outside May.",
                    blocking=True,
                    source_type="packaging_batch",
                    source_id=batch.id,
                    business_date=batch.production_date,
                    transaction_date=material.created_at,
                    document_number=batch.internal_batch_number,
                )
            append_child_scope(
                table_name="packaging_batch_line_materials",
                child_id=material.id,
                parent_table="packaging_batches",
                parent_id=batch.id,
                parent_document_number=batch.internal_batch_number,
                parent_source_type="packaging_batch",
                parent_business_date=batch.production_date,
                parent_in_may_window=parent_in_may,
                parent_classification=parent_classification,
                child_date_field="created_at",
                child_date=material.created_at,
                child_in_may_window=child_in_may,
                issue=issue,
                blocking=blocking,
            )
        child_count_maps["packaging_batch"]["line_materials"] = packaging_material_counter

        import_batches_total = (
            session.query(func.count()).select_from(ImportBatch).scalar() or 0
            if "import_batches" in existing_tables
            else 0
        )
        imported_bom_headers_total = (
            session.query(func.count()).select_from(ImportedBomHeader).scalar() or 0
            if "imported_bom_headers" in existing_tables
            else 0
        )
        imported_bom_lines_total = (
            session.query(func.count()).select_from(ImportedBomLine).scalar() or 0
            if "imported_bom_lines" in existing_tables
            else 0
        )
        inventory_adjustment_tokens_total = (
            session.query(func.count()).select_from(InventoryAdjustmentPostToken).scalar() or 0
            if "inventory_adjustment_post_tokens" in existing_tables
            else 0
        )

        for source_type, docs in may_documents.items():
            spec = DOCUMENT_SPECS[source_type]
            for doc in docs:
                row = ensure_document_row(
                    document_rows_by_key,
                    table_name=spec["table_name"],
                    source_type=source_type,
                    document_id=doc.id,
                    document_number=str(getattr(doc, spec["number_field"])),
                    business_date=getattr(doc, spec["date_field"]),
                    status=getattr(doc, spec["status_field"], None),
                    has_ledger_in_may=bool(may_tx_by_source.get((source_type, doc.id), [])),
                    may_ledger_txn_count=len(may_tx_by_source.get((source_type, doc.id), [])),
                )
                row["child_count_summary"] = build_child_count_summary(
                    source_type,
                    doc.id,
                    child_count_maps,
                )
                status = str(row["status"]) if row["status"] is not None else ""
                has_ledger = bool(row["has_ledger_in_may"])
                child_counts = child_counter_summary(str(row["child_count_summary"]))

                if has_ledger:
                    set_document_classification(
                        row,
                        "A",
                        "Operational contaminating document with ledger associated inside May.",
                    )
                    continue

                if source_type == "production_order":
                    if status == "closed" and child_counts.get("materials", 0) == 0 and child_counts.get("activities", 0) == 0:
                        set_document_classification(
                            row,
                            "B",
                            "Likely historical production import: closed without ledger and without operational child rows.",
                            manual_review=True,
                        )
                    else:
                        set_document_classification(
                            row,
                            "C",
                            "Production order has contradictory operational signals without May ledger.",
                            manual_review=True,
                            blocking=True,
                        )
                elif source_type == "b2b_order":
                    if status == "invoiced" and child_counts.get("lines", 0) > 0:
                        set_document_classification(
                            row,
                            "B",
                            "Likely historical B2B import: invoiced without ledger.",
                            manual_review=True,
                        )
                    else:
                        set_document_classification(
                            row,
                            "C",
                            "B2B order cannot be classified safely: operational status or missing lines without May ledger.",
                            manual_review=True,
                            blocking=True,
                        )
                elif source_type == "b2c_order":
                    if status == "invoiced" and child_counts.get("lines", 0) > 0:
                        set_document_classification(
                            row,
                            "B",
                            "Likely historical B2C import: invoiced without ledger.",
                            manual_review=True,
                        )
                    else:
                        set_document_classification(
                            row,
                            "C",
                            "B2C order cannot be classified safely: operational status or missing lines without May ledger.",
                            manual_review=True,
                            blocking=True,
                        )
                elif source_type == "purchase_order":
                    if status == "closed" and child_counts.get("lines", 0) > 0:
                        set_document_classification(
                            row,
                            "B",
                            "Likely historical purchase import: closed without May receipt ledger.",
                            manual_review=True,
                        )
                    else:
                        set_document_classification(
                            row,
                            "C",
                            "Purchase order cannot be classified safely: operational status or missing lines without May ledger.",
                            manual_review=True,
                            blocking=True,
                        )
                elif source_type == "inventory_adjustment":
                    set_document_classification(
                        row,
                        "C",
                        "Inventory adjustment without ledger is contradictory and requires manual review.",
                        manual_review=True,
                        blocking=True,
                    )
                elif source_type == "packaging_batch":
                    set_document_classification(
                        row,
                        "C",
                        "Packaging batch without ledger is contradictory because no historical import flow exists.",
                        manual_review=True,
                        blocking=True,
                    )

        for source_type, spec in DOCUMENT_SPECS.items():
            source_ids = sorted(
                {
                    source_id
                    for tx_source_type, source_id in may_source_pairs
                    if tx_source_type == source_type and source_id is not None
                }
            )
            if not source_ids:
                continue
            model = spec["model"]
            date_field = getattr(model, spec["date_field"])
            outside_docs = (
                session.query(model)
                .filter(
                    model.id.in_(source_ids),
                    or_(date_field < start_dt.date(), date_field >= end_dt.date()),
                )
                .all()
            )
            for doc in outside_docs:
                row = ensure_document_row(
                    document_rows_by_key,
                    table_name=spec["table_name"],
                    source_type=source_type,
                    document_id=doc.id,
                    document_number=str(getattr(doc, spec["number_field"])),
                    business_date=getattr(doc, spec["date_field"]),
                    status=getattr(doc, spec["status_field"], None),
                    has_ledger_in_may=True,
                    may_ledger_txn_count=len(may_tx_by_source.get((source_type, doc.id), [])),
                )
                set_document_classification(
                    row,
                    "C",
                    "Document business date falls outside May but it is linked to May ledger postings.",
                    manual_review=True,
                    blocking=True,
                )
                record_dependency(
                    dependency_rows,
                    suspicious_records,
                    category=DEPENDENCY_OUT_OF_WINDOW_DOCUMENT_LINKED,
                    severity="error",
                    table_name=spec["table_name"],
                    record_id=doc.id,
                    detail="Document is outside May but linked to May ledger through source_id/source_type.",
                    blocking=True,
                    source_type=source_type,
                    source_id=doc.id,
                    business_date=getattr(doc, spec["date_field"]),
                    document_number=str(getattr(doc, spec["number_field"])),
                )

        if any(row["category"] == DEPENDENCY_OUT_OF_WINDOW_DOCUMENT_LINKED for row in dependency_rows):
            add_abort(
                abort_conditions,
                "out_of_window_document_linked_to_may_ledger",
                "Found documents outside May directly linked to May ledger postings.",
            )

        for source_type, docs_by_id in may_documents_by_source.items():
            spec = DOCUMENT_SPECS[source_type]
            for doc_id, doc in docs_by_id.items():
                outside_rows = outside_tx_by_source.get((source_type, doc_id), [])
                if not outside_rows:
                    continue
                row = document_rows_by_key[(spec["table_name"], doc_id)]
                row["out_of_window_ledger_txn_count"] = len(outside_rows)
                set_document_classification(
                    row,
                    "C",
                    "May document has ledger postings outside the approved May window.",
                    manual_review=True,
                    blocking=True,
                )
                record_dependency(
                    dependency_rows,
                    suspicious_records,
                    category=DEPENDENCY_MAY_DOCUMENT_OUT_OF_WINDOW_LEDGER,
                    severity="error",
                    table_name=spec["table_name"],
                    record_id=doc.id,
                    detail="May document has out-of-window ledger postings.",
                    blocking=True,
                    source_type=source_type,
                    source_id=doc.id,
                    business_date=getattr(doc, spec["date_field"]),
                    transaction_date=outside_rows[0].transaction_date,
                    document_number=str(getattr(doc, spec["number_field"])),
                )

        if any(row["category"] == DEPENDENCY_MAY_DOCUMENT_OUT_OF_WINDOW_LEDGER for row in dependency_rows):
            add_abort(
                abort_conditions,
                "may_document_with_out_of_window_ledger",
                "Found May documents whose ledger postings extend outside the May window.",
            )

        impacted_adjustments = (
            session.query(InventoryAdjustment)
            .filter(
                InventoryAdjustment.inventory_transaction_id.in_(sorted(involved_tx_ids))
                if involved_tx_ids
                else text("1=0")
            )
            .all()
        )
        if impacted_adjustments:
            add_suspicious(
                suspicious_records,
                category="ADJUSTMENTS_LINKED_TO_MAY_LEDGER",
                table_name="inventory_adjustments",
                record_id="(multiple)",
                reason="Inventory adjustments reference May inventory transactions.",
                linked_source_type="inventory_adjustment",
                action_required="Review whether linked adjustments belong to the May cleanup scope.",
                blocking=True,
            )

        balances_impacted_rows: list[dict[str, object]] = []
        balances = (
            session.query(InventoryBalance, Product)
            .join(Product, Product.id == InventoryBalance.product_id)
            .filter(
                InventoryBalance.product_id.in_(sorted(involved_product_ids))
                if involved_product_ids
                else text("1=0")
            )
            .all()
        )
        for balance, product in balances:
            issue = ""
            if balance.last_transaction_id in involved_tx_ids:
                issue = "last_transaction_points_to_may_scope"
            elif balance.last_transaction_id is None:
                issue = "no_last_transaction"
            balances_impacted_rows.append(
                {
                    "product_id": product.id,
                    "sku": product.sku,
                    "balance_id": balance.id,
                    "on_hand_qty": balance.on_hand_qty,
                    "average_unit_cost": balance.average_unit_cost,
                    "inventory_value": balance.inventory_value,
                    "last_transaction_id": balance.last_transaction_id,
                    "last_transaction_at": balance.last_transaction_at,
                    "last_txn_in_scope": balance.last_transaction_id in involved_tx_ids,
                    "issue": issue,
                }
            )
        if any(row["issue"] == "last_transaction_points_to_may_scope" for row in balances_impacted_rows):
            add_suspicious(
                suspicious_records,
                category="BALANCE_POINTS_TO_MAY_TXN",
                table_name="inventory_balances",
                record_id="(multiple)",
                reason="One or more InventoryBalance rows point to May transactions.",
                action_required="Balances would need clean reconstruction after cleanup.",
                blocking=True,
            )
            add_abort(
                abort_conditions,
                "balances_impacted_by_may_transactions",
                "InventoryBalance rows point to May transactions that are candidates for cleanup.",
            )

        for row in document_rows_by_key.values():
            counts = child_counter_summary(str(row["child_count_summary"]))
            if row["classification"] == "B" and row["out_of_window_ledger_txn_count"]:
                set_document_classification(
                    row,
                    "C",
                    "Document looked historical but has ledger outside May, which is contradictory.",
                    manual_review=True,
                    blocking=True,
                )
            if row["classification"] == "A" and row["table_name"] == "production_orders" and counts.get("materials", 0) == 0:
                add_suspicious(
                    suspicious_records,
                    category="HEADER_WITHOUT_EXPECTED_CHILDREN",
                    table_name="production_orders",
                    record_id=row["document_id"],
                    reason="Production order has May ledger but no material child rows.",
                    linked_source_type="production_order",
                    linked_source_id=row["document_id"],
                    date_value=row["business_date"],
                    action_required="Review whether cleanup should treat this document as partially populated.",
                    blocking=False,
                )
            if row["classification"] == "A" and row["table_name"] in {"b2b_sales_orders", "b2c_sales_orders", "purchase_orders"} and counts.get("lines", 0) == 0:
                set_document_classification(
                    row,
                    "C",
                    "Document has May ledger but no expected line rows, so it cannot be classified safely.",
                    manual_review=True,
                    blocking=True,
                )
            if row["classification"] == "A" and row["table_name"] == "packaging_batches" and counts.get("lines", 0) == 0:
                set_document_classification(
                    row,
                    "C",
                    "Packaging batch has May ledger but no lines, so it cannot be classified safely.",
                    manual_review=True,
                    blocking=True,
                )

        for row in document_rows_by_key.values():
            if row["classification"] == "C":
                add_suspicious(
                    suspicious_records,
                    category="DOCUMENT_CLASSIFICATION_C",
                    table_name=str(row["table_name"]),
                    record_id=row["document_id"],
                    reason=str(row["reason"]),
                    linked_source_type=str(row["source_type"]),
                    linked_source_id=row["document_id"],
                    date_value=row["business_date"],
                    action_required="Manual review required before any cleanup execution.",
                    blocking=bool(row["blocking"]),
                )

        if any(row["classification"] == "C" for row in document_rows_by_key.values()):
            add_abort(
                abort_conditions,
                "documents_not_classifiable_with_confidence",
                "Found May or related documents classified as C (manual review required).",
            )

        if any(row["classification"] == "B" for row in document_rows_by_key.values()):
            add_suspicious(
                suspicious_records,
                category="REPORTING_ONLY_DOCUMENTS_PRESENT",
                table_name="documents_classification",
                record_id="(multiple)",
                reason="May contains likely reporting-only documents without ledger.",
                action_required="Manual keep/delete decision required for classification B records.",
                blocking=False,
            )

        sequences = session.query(AppSequence).order_by(AppSequence.name).all()
        classification_by_table: dict[str, dict[int, dict[str, object]]] = defaultdict(dict)
        for row in document_rows_by_key.values():
            classification_by_table[str(row["table_name"])][int(row["document_id"])] = row

        sequence_rows: list[dict[str, object]] = []
        for sequence in sequences:
            plan = DOCUMENT_SEQUENCE_PLAN.get(sequence.name)
            dependent_table = plan["label"] if plan else ""
            proposed_next_value = plan["proposed_next_value"] if plan else ""
            surviving_numbers: list[str] = []
            possible_collision = False
            safe_to_reset = False
            reason = "Sequence not in approved reset plan."
            if plan:
                source_type = sequence.name
                all_docs = all_documents_by_source.get(source_type, [])
                classified_docs = classification_by_table.get(plan["table_name"], {})
                parsed_suffixes: list[int] = []
                has_unparseable = False
                for doc in all_docs:
                    classified = classified_docs.get(int(doc["id"]))
                    if classified and classified["classification"] == "A":
                        continue
                    number = str(doc["document_number"])
                    surviving_numbers.append(number)
                    parsed_suffix = parse_numeric_suffix(number, plan["pattern"])
                    if parsed_suffix is None:
                        has_unparseable = True
                    else:
                        parsed_suffixes.append(parsed_suffix)

                if plan["behavior"] == "bootstrap_safe":
                    possible_collision = False
                    safe_to_reset = True
                    reason = (
                        "Generator bootstraps against existing documents and re-checks collisions before assigning numbers."
                    )
                elif has_unparseable:
                    possible_collision = False
                    safe_to_reset = False
                    reason = (
                        "Surviving document numbers include non-standard formats; collision safety cannot be validated."
                    )
                else:
                    possible_collision = any(
                        suffix >= int(proposed_next_value) for suffix in parsed_suffixes
                    )
                    safe_to_reset = not possible_collision
                    reason = (
                        "Strict sequence would collide with surviving document numbers after reset."
                        if possible_collision
                        else "No surviving standard-format document numbers would collide with the reset start value."
                    )

            sequence_rows.append(
                {
                    "sequence_name": sequence.name,
                    "current_next_value": sequence.next_value,
                    "proposed_next_value": proposed_next_value,
                    "dependent_table": dependent_table,
                    "existing_document_numbers_after_cleanup_estimate": ", ".join(surviving_numbers[:25]),
                    "possible_collision": possible_collision,
                    "safe_to_reset": safe_to_reset,
                    "reason": reason,
                }
            )
            if plan and not safe_to_reset:
                add_suspicious(
                    suspicious_records,
                    category="SEQUENCE_COLLISION_RISK",
                    table_name="app_sequences",
                    record_id=sequence.name,
                    reason=f"Sequence '{sequence.name}' cannot be validated as safe to reset.",
                    action_required="Do not reset this sequence automatically; resolve surviving documents or numbering rules first.",
                    blocking=True,
                )
                add_abort(
                    abort_conditions,
                    "sequence_collision_risk",
                    f"Sequence '{sequence.name}' cannot be validated as safe to reset.",
                )

        if "lot_sequences" in existing_tables:
            add_suspicious(
                suspicious_records,
                category="LOT_SEQUENCE_MANUAL_POLICY",
                table_name="lot_sequences",
                record_id="(table)",
                reason="lot_sequences should not be reset globally.",
                action_required="Review lot numbering separately; keep global reset disabled.",
                blocking=False,
            )

        tables_to_clean_rows = [
            {
                "table_name": row["table_name"],
                "record_id": row["document_id"],
                "document_number": row["document_number"],
                "classification": row["classification"],
                "may_ledger_txn_count": row["may_ledger_txn_count"],
                "reason": row["reason"],
            }
            for row in document_rows_by_key.values()
            if row["classification"] == "A"
        ]

        documents_classification_rows = [
            row
            for row in sorted(
                document_rows_by_key.values(),
                key=lambda item: (
                    str(item["table_name"]),
                    str(item["document_number"]),
                    int(item["document_id"]),
                ),
            )
        ]

        dependency_analysis_rows = [
            {
                "category": row["category"],
                "severity": row["severity"],
                "table_name": row["table_name"],
                "record_id": row["record_id"],
                "document_number": row["document_number"],
                "source_type": row["source_type"],
                "source_id": row["source_id"],
                "product_id": row["product_id"],
                "sku": row["sku"],
                "business_date": row["business_date"],
                "transaction_date": row["transaction_date"],
                "detail": row["detail"],
                "blocking": row["blocking"],
            }
            for row in dependency_rows
        ]

        technical_read_only_ok = True
        abort_conditions_found = bool(abort_conditions)
        manual_review_required = bool(
            suspicious_records
            or any(row["classification"] in {"B", "C"} for row in documents_classification_rows)
        )
        functional_scope_ok = (
            not abort_conditions_found
            and not unexpected_source_types
            and not any(row["classification"] == "C" for row in documents_classification_rows)
        )
        safe_to_execute_cleanup_script_later = (
            technical_read_only_ok
            and functional_scope_ok
            and not any(bool(row["blocking"]) for row in suspicious_records)
            and all(bool(row["safe_to_reset"]) for row in sequence_rows if row["proposed_next_value"] != "")
        )

        tables_to_clean_headers = [
            "table_name",
            "record_id",
            "document_number",
            "classification",
            "may_ledger_txn_count",
            "reason",
        ]
        classification_headers = [
            "table_name",
            "source_type",
            "document_id",
            "document_number",
            "business_date",
            "status",
            "has_ledger_in_may",
            "may_ledger_txn_count",
            "out_of_window_ledger_txn_count",
            "classification",
            "requires_manual_review",
            "blocking",
            "child_count_summary",
            "reason",
        ]
        inventory_headers = [
            "txn_id",
            "transaction_date",
            "product_id",
            "sku",
            "transaction_type",
            "source_type",
            "source_id",
            "source_line_id",
            "quantity_in",
            "quantity_out",
            "unit_cost",
            "total_cost",
            "running_quantity",
            "running_average_cost",
            "running_inventory_value",
        ]
        balances_headers = [
            "product_id",
            "sku",
            "balance_id",
            "on_hand_qty",
            "average_unit_cost",
            "inventory_value",
            "last_transaction_id",
            "last_transaction_at",
            "last_txn_in_scope",
            "issue",
        ]
        sequence_headers = [
            "sequence_name",
            "current_next_value",
            "proposed_next_value",
            "dependent_table",
            "existing_document_numbers_after_cleanup_estimate",
            "possible_collision",
            "safe_to_reset",
            "reason",
        ]
        suspicious_headers = [
            "category",
            "table_name",
            "record_id",
            "reason",
            "linked_source_type",
            "linked_source_id",
            "date",
            "action_required",
            "blocking",
        ]
        abort_headers = ["condition_code", "severity", "detected", "detail", "blocking"]
        table_range_headers = [
            "table_name",
            "date_field",
            "field_status",
            "min_date",
            "max_date",
            "count_total",
            "count_in_may_window",
            "count_before_window",
            "count_after_window",
        ]
        child_scope_headers = [
            "table_name",
            "child_id",
            "parent_table",
            "parent_id",
            "parent_document_number",
            "parent_source_type",
            "parent_business_date",
            "parent_in_may_window",
            "parent_classification",
            "child_date_field",
            "child_date",
            "child_in_may_window",
            "issue",
            "blocking",
        ]
        dependency_headers = [
            "category",
            "severity",
            "table_name",
            "record_id",
            "document_number",
            "source_type",
            "source_id",
            "product_id",
            "sku",
            "business_date",
            "transaction_date",
            "detail",
            "blocking",
        ]

        abort_rows = [
            {
                "condition_code": item.code,
                "severity": "error" if item.blocking else "warning",
                "detected": True,
                "detail": item.detail,
                "blocking": item.blocking,
            }
            for item in abort_conditions
        ]

        write_csv(export_dir / "tables_to_clean.csv", tables_to_clean_headers, tables_to_clean_rows)
        write_csv(export_dir / "documents_classification.csv", classification_headers, documents_classification_rows)
        write_csv(export_dir / "inventory_transactions_scope.csv", inventory_headers, inventory_scope_rows)
        write_csv(export_dir / "balances_impacted.csv", balances_headers, balances_impacted_rows)
        write_csv(export_dir / "sequences_reset_plan.csv", sequence_headers, sequence_rows)
        write_csv(export_dir / "suspicious_records.csv", suspicious_headers, suspicious_records)
        write_csv(export_dir / "abort_conditions.csv", abort_headers, abort_rows)
        write_csv(export_dir / "table_date_ranges.csv", table_range_headers, table_date_ranges)
        write_csv(export_dir / "child_records_scope.csv", child_scope_headers, child_scope_rows)
        write_csv(export_dir / "dependency_analysis.csv", dependency_headers, dependency_analysis_rows)

        summary = {
            "window_start": start_dt.isoformat(sep=" "),
            "window_end": end_dt.isoformat(sep=" "),
            "database": db_context,
            "parameter_source": "environment" if args.use_env else "cli",
            "dialect": engine.dialect.name,
            "technical_read_only_ok": technical_read_only_ok,
            "functional_scope_ok": functional_scope_ok,
            "abort_conditions_found": abort_conditions_found,
            "manual_review_required": manual_review_required,
            "safe_to_execute_cleanup_script_later": safe_to_execute_cleanup_script_later,
            "go_no_go": "GO" if safe_to_execute_cleanup_script_later else "NO_GO",
            "opening_balance_exists": opening_balance_count > 0,
            "counts": {
                "inventory_transactions_in_scope": len(inventory_scope_rows),
                "production_orders_in_may": len(may_documents["production_order"]),
                "b2b_orders_in_may": len(may_documents["b2b_order"]),
                "b2c_orders_in_may": len(may_documents["b2c_order"]),
                "purchase_orders_in_may": len(may_documents["purchase_order"]),
                "inventory_adjustments_in_may": len(may_documents["inventory_adjustment"]),
                "packaging_batches_in_may": len(may_documents["packaging_batch"]),
                "balances_impacted": len(balances_impacted_rows),
                "impacted_adjustments": len(impacted_adjustments),
                "inventory_adjustment_post_tokens_total": int(inventory_adjustment_tokens_total),
                "import_batches_total": int(import_batches_total),
                "imported_bom_headers_total": int(imported_bom_headers_total),
                "imported_bom_lines_total": int(imported_bom_lines_total),
                "child_records_scope_total": len(child_scope_rows),
                "dependency_rows_total": len(dependency_analysis_rows),
            },
            "status_counts": {
                "production_orders": collect_count_by_status(
                    session, ProductionOrder, "production_date", "date", start_dt, end_dt
                ),
                "b2b_sales_orders": collect_count_by_status(
                    session, B2BSalesOrder, "delivery_date", "date", start_dt, end_dt
                ),
                "b2c_sales_orders": collect_count_by_status(
                    session, B2CSalesOrder, "order_date", "date", start_dt, end_dt
                ),
                "purchase_orders": collect_count_by_status(
                    session, PurchaseOrder, "po_date", "date", start_dt, end_dt
                ),
                "packaging_batches": collect_count_by_status(
                    session, PackagingBatch, "production_date", "date", start_dt, end_dt
                ),
                "inventory_adjustments": collect_count_by_status(
                    session, InventoryAdjustment, "adjustment_date", "date", start_dt, end_dt
                ),
            },
            "transaction_type_counts": dict(tx_type_counts),
            "source_type_counts": dict(source_type_counts),
            "classification_counts": dict(Counter(row["classification"] for row in documents_classification_rows)),
            "dependency_counts": dict(Counter(row["category"] for row in dependency_analysis_rows)),
            "unexpected_source_types": sorted(unexpected_source_types),
            "table_date_ranges": table_date_ranges,
            "recommendation_lot_sequences": "Do not reset lot_sequences globally.",
            "reasons": [item.detail for item in abort_conditions]
            or ["No blocking conditions detected in read-only scope."],
            "export_dir": str(export_dir),
        }
        write_json(export_dir / "dry_run_summary.json", summary)

    finally:
        session.rollback()
        session.close()
        engine.dispose()


if __name__ == "__main__":
    main()
