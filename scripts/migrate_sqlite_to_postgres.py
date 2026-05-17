from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from sqlalchemy import create_engine, inspect, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import app.models  # noqa: F401
from app.models import Base


EXCLUDED_TABLES = [
    "user_sessions",
    "inventory_adjustment_post_tokens",
    "purchase_order_receive_tokens",
]

MIGRATION_TABLES = [
    "app_sequences",
    "lot_sequences",
    "roles",
    "permissions",
    "product_categories",
    "suppliers",
    "routes",
    "channels",
    "discount_rules",
    "import_batches",
    "loyverse_customer_mappings",
    "loyverse_payment_type_mappings",
    "loyverse_variant_mappings",
    "labor_rates",
    "machines",
    "overhead_rates",
    "b2b_customers",
    "users",
    "activities",
    "machine_rates",
    "b2c_customers",
    "role_permissions",
    "user_roles",
    "audit_logs",
    "route_activities",
    "products",
    "b2b_customer_products",
    "imported_bom_headers",
    "imported_bom_lines",
    "product_bom_headers",
    "product_bom_lines",
    "b2b_sales_orders",
    "b2b_sales_order_lines",
    "b2c_sales_orders",
    "b2c_sales_order_lines",
    "purchase_orders",
    "purchase_order_lines",
    "production_orders",
    "production_order_activities",
    "production_order_materials",
    "inventory_transactions",
    "inventory_balances",
    "inventory_adjustments",
]

AGGREGATE_SPECS = [
    ("b2b_sales_orders", "total_amount"),
    ("b2c_sales_orders", "total_amount"),
    ("b2c_sales_orders", "subtotal_amount"),
    ("purchase_orders", "estimated_total"),
    ("production_orders", "real_total_cost"),
    ("inventory_transactions", "quantity_in"),
    ("inventory_transactions", "quantity_out"),
    ("inventory_transactions", "total_cost"),
    ("inventory_balances", "on_hand_qty"),
    ("inventory_balances", "inventory_value"),
]


@dataclass
class TableProfile:
    name: str
    columns: list[str]
    count: int
    min_id: int | None = None
    max_id: int | None = None


@dataclass
class RunSummary:
    dry_run: bool
    source_sqlite_path: str
    source_table_count: int = 0
    source_profiles: dict[str, TableProfile] = field(default_factory=dict)
    source_aggregates: dict[str, str] = field(default_factory=dict)
    target_available: bool = False
    target_masked_url: str = "unavailable"
    target_table_count: int | None = None
    target_profiles: dict[str, TableProfile] = field(default_factory=dict)
    target_aggregates: dict[str, str] = field(default_factory=dict)
    target_schema_diffs: dict[str, dict[str, list[str]]] = field(default_factory=dict)
    migration_order_valid: bool = False
    migration_order_errors: list[str] = field(default_factory=list)
    tables_to_migrate: list[str] = field(default_factory=lambda: list(MIGRATION_TABLES))
    tables_excluded: list[str] = field(default_factory=lambda: list(EXCLUDED_TABLES))
    skipped: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    validations_ok: list[str] = field(default_factory=list)
    validations_failed: list[str] = field(default_factory=list)
    migrated_counts: dict[str, int] = field(default_factory=dict)
    duration_seconds: float = 0.0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inspect and migrate ERP data from SQLite into PostgreSQL."
    )
    parser.add_argument(
        "--sqlite-path",
        default="costeo.db",
        help="Path to the source SQLite database. Defaults to costeo.db.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Inspect source and target metadata without writing to PostgreSQL.",
    )
    parser.add_argument(
        "--reset-target",
        action="store_true",
        help="Reserved for a future approved sprint. Not available in 34E.",
    )
    return parser


def _normalize_database_url(raw_url: str | None) -> str | None:
    url = (raw_url or "").strip()
    if not url:
        return None
    if url.startswith("postgres://"):
        return "postgresql://" + url[len("postgres://") :]
    return url


def _mask_database_url(url: str | None) -> str:
    if not url:
        return "unavailable"
    parsed = urlsplit(url)
    if not parsed.scheme:
        return "<redacted>"
    db_name = parsed.path.rsplit("/", 1)[-1] if parsed.path else ""
    if db_name:
        return f"{parsed.scheme}://<redacted>/{db_name}"
    return f"{parsed.scheme}://<redacted>"


def _format_value(value: Any) -> str:
    if value is None:
        return "0"
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, float):
        return format(value, ".4f")
    return str(value)


def _load_source_table_names(sqlite_path: Path) -> list[str]:
    import sqlite3

    conn = sqlite3.connect(sqlite_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


def _profile_model_tables(engine: Engine, table_names: list[str]) -> dict[str, TableProfile]:
    profiles: dict[str, TableProfile] = {}
    with engine.connect() as conn:
        for table_name in table_names:
            table = Base.metadata.tables[table_name]
            rows = conn.execute(select(text("count(*)")).select_from(table)).scalar_one()
            min_id = max_id = None
            if "id" in table.c:
                min_id = conn.execute(select(text("min(id)")).select_from(table)).scalar_one()
                max_id = conn.execute(select(text("max(id)")).select_from(table)).scalar_one()
            profiles[table_name] = TableProfile(
                name=table_name,
                columns=[column.name for column in table.columns],
                count=int(rows or 0),
                min_id=min_id,
                max_id=max_id,
            )
    return profiles


def _aggregate_model_metrics(engine: Engine, available_tables: set[str] | None = None) -> dict[str, str]:
    aggregates: dict[str, str] = {}
    with engine.connect() as conn:
        for table_name, column_name in AGGREGATE_SPECS:
            if available_tables is not None and table_name not in available_tables:
                continue
            table = Base.metadata.tables[table_name]
            query = select(text(f"COALESCE(SUM({column_name}), 0)")).select_from(table)
            value = conn.execute(query).scalar_one()
            aggregates[f"{table_name}.{column_name}"] = _format_value(value)
    return aggregates


def _profile_target_engine(target_engine: Engine) -> tuple[int, dict[str, TableProfile], dict[str, str]]:
    inspector = inspect(target_engine)
    table_names = sorted(inspector.get_table_names())
    profiles = _profile_model_tables(target_engine, table_names) if table_names else {}
    aggregates = _aggregate_model_metrics(target_engine, set(table_names)) if table_names else {}
    return len(table_names), profiles, aggregates


def _compare_schema(
    source_profiles: dict[str, TableProfile],
    target_profiles: dict[str, TableProfile],
) -> dict[str, dict[str, list[str]]]:
    diffs: dict[str, dict[str, list[str]]] = {}
    all_tables = sorted(set(source_profiles) | set(target_profiles))
    for table_name in all_tables:
        source_columns = set(source_profiles.get(table_name, TableProfile(table_name, [], 0)).columns)
        target_columns = set(target_profiles.get(table_name, TableProfile(table_name, [], 0)).columns)
        if source_columns != target_columns:
            diffs[table_name] = {
                "source_only": sorted(source_columns - target_columns),
                "target_only": sorted(target_columns - source_columns),
            }
    return diffs


def _validate_migration_order() -> tuple[bool, list[str]]:
    positions = {table_name: index for index, table_name in enumerate(MIGRATION_TABLES)}
    errors: list[str] = []
    for table_name in MIGRATION_TABLES:
        table = Base.metadata.tables[table_name]
        for foreign_key in table.foreign_keys:
            dependency = foreign_key.column.table.name
            if dependency in EXCLUDED_TABLES:
                errors.append(f"{table_name} depends on excluded table {dependency}.")
            elif dependency in positions and positions[dependency] > positions[table_name]:
                errors.append(f"{table_name} depends on later table {dependency}.")
    return not errors, errors


def _load_rows(engine: Engine, table_name: str) -> list[dict[str, Any]]:
    table = Base.metadata.tables[table_name]
    query = select(table)
    if "id" in table.c:
        query = query.order_by(table.c.id)
    with engine.connect() as conn:
        return [dict(row._mapping) for row in conn.execute(query)]


def _assert_target_is_empty(target_engine: Engine) -> None:
    profiles = _profile_model_tables(target_engine, MIGRATION_TABLES)
    non_empty = [f"{name}={profile.count}" for name, profile in profiles.items() if profile.count > 0]
    if non_empty:
        raise RuntimeError(
            "Target PostgreSQL is not empty for migratable tables: " + ", ".join(non_empty)
        )


def _reset_sequences(target_engine: Engine) -> None:
    with target_engine.begin() as conn:
        for table_name in MIGRATION_TABLES:
            table = Base.metadata.tables[table_name]
            if "id" not in table.c:
                continue
            conn.execute(
                text(
                    f"SELECT setval(pg_get_serial_sequence('{table_name}', 'id'), "
                    f"COALESCE((SELECT MAX(id) FROM {table_name}), 1), true)"
                )
            )


def _migrate_rows(source_engine: Engine, target_engine: Engine, summary: RunSummary) -> None:
    target_metadata = Base.metadata
    with target_engine.begin() as conn:
        for table_name in MIGRATION_TABLES:
            rows = _load_rows(source_engine, table_name)
            summary.migrated_counts[table_name] = len(rows)
            if not rows:
                summary.skipped.append(f"{table_name}: source empty")
                continue
            conn.execute(target_metadata.tables[table_name].insert(), rows)


def _confirm_migration() -> None:
    confirmation = input("Type MIGRATE to continue: ").strip()
    if confirmation != "MIGRATE":
        raise RuntimeError("Migration cancelled: confirmation text did not match.")


def _print_profiles(title: str, profiles: dict[str, TableProfile], table_names: list[str]) -> None:
    print(title)
    for table_name in table_names:
        profile = profiles[table_name]
        id_fragment = ""
        if profile.min_id is not None or profile.max_id is not None:
            id_fragment = f" min_id={profile.min_id} max_id={profile.max_id}"
        print(
            f"  - {table_name}: count={profile.count} columns={len(profile.columns)}{id_fragment}"
        )


def _print_aggregates(title: str, aggregates: dict[str, str]) -> None:
    print(title)
    for key, value in aggregates.items():
        print(f"  - {key}={value}")


def _print_summary(summary: RunSummary) -> None:
    print("")
    print("Migration summary")
    print(f"  dry_run={summary.dry_run}")
    print(f"  source_sqlite={summary.source_sqlite_path}")
    print(f"  source_tables={summary.source_table_count}")
    print(f"  target_available={summary.target_available}")
    print(f"  target={summary.target_masked_url}")
    if summary.target_table_count is not None:
        print(f"  target_tables={summary.target_table_count}")
    print(f"  migration_order_valid={summary.migration_order_valid}")
    print(f"  tables_to_migrate={len(summary.tables_to_migrate)}")
    print(f"  tables_excluded={len(summary.tables_excluded)}")
    print(f"  skipped={len(summary.skipped)}")
    print(f"  errors={len(summary.errors)}")
    print(f"  duration_seconds={summary.duration_seconds:.2f}")
    if summary.validations_ok:
        print("  validations_ok:")
        for item in summary.validations_ok:
            print(f"    - {item}")
    if summary.validations_failed:
        print("  validations_failed:")
        for item in summary.validations_failed:
            print(f"    - {item}")
    if summary.errors:
        print("  error_details:")
        for item in summary.errors:
            print(f"    - {item}")


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.reset_target:
        print("--reset-target is reserved for a later approved sprint and is disabled in 34E.")
        return 1

    sqlite_path = Path(args.sqlite_path).expanduser().resolve()
    if not sqlite_path.exists():
        print(f"SQLite source not found: {sqlite_path}")
        return 1

    start = time.perf_counter()
    summary = RunSummary(dry_run=args.dry_run, source_sqlite_path=str(sqlite_path))
    source_engine = None
    target_engine = None

    try:
        source_url = f"sqlite:///{sqlite_path.as_posix()}"
        source_engine = create_engine(source_url)

        raw_target_url = os.getenv("DATABASE_URL")
        target_url = _normalize_database_url(raw_target_url)
        summary.target_available = bool(target_url)
        summary.target_masked_url = _mask_database_url(target_url)

        source_table_names = _load_source_table_names(sqlite_path)
        summary.source_table_count = len(source_table_names)
        summary.source_profiles = _profile_model_tables(source_engine, source_table_names)
        summary.source_aggregates = _aggregate_model_metrics(source_engine)

        print(f"Source SQLite database: {sqlite_path}")
        print(f"Target PostgreSQL: {summary.target_masked_url}")
        print(f"Source tables detected: {summary.source_table_count}")
        print(f"Tables to migrate ({len(MIGRATION_TABLES)}): {', '.join(MIGRATION_TABLES)}")
        print(f"Tables excluded ({len(EXCLUDED_TABLES)}): {', '.join(EXCLUDED_TABLES)}")

        summary.migration_order_valid, summary.migration_order_errors = _validate_migration_order()
        if summary.migration_order_valid:
            summary.validations_ok.append("Migration order is FK-safe against current models.")
        else:
            summary.validations_failed.extend(summary.migration_order_errors)

        missing_source_tables = sorted(
            (set(MIGRATION_TABLES) | set(EXCLUDED_TABLES)) - set(source_table_names)
        )
        if missing_source_tables:
            summary.validations_failed.append(
                "Source SQLite is missing expected tables: " + ", ".join(missing_source_tables)
            )
        else:
            summary.validations_ok.append("Source SQLite contains every expected table.")

        source_subset = {name: summary.source_profiles[name] for name in source_table_names}
        _print_profiles("Source table profile", source_subset, source_table_names)
        _print_aggregates("Source aggregates", summary.source_aggregates)

        if target_url:
            target_engine = create_engine(target_url)
            summary.target_table_count, summary.target_profiles, summary.target_aggregates = _profile_target_engine(
                target_engine
            )
            summary.target_schema_diffs = _compare_schema(
                {name: profile for name, profile in summary.source_profiles.items()},
                summary.target_profiles,
            )
            if summary.target_schema_diffs:
                summary.validations_failed.append("Target schema differs from source/models.")
            else:
                summary.validations_ok.append("Target schema matches source column sets.")
            print(f"Target tables detected: {summary.target_table_count}")
            if summary.target_profiles:
                target_subset = {
                    name: summary.target_profiles[name]
                    for name in sorted(summary.target_profiles.keys())
                }
                _print_profiles("Target table profile", target_subset, sorted(target_subset.keys()))
                _print_aggregates("Target aggregates", summary.target_aggregates)
            if summary.target_schema_diffs:
                print("Target schema differences detected:")
                for table_name, diff in summary.target_schema_diffs.items():
                    print(
                        f"  - {table_name}: source_only={diff['source_only']} target_only={diff['target_only']}"
                    )
        else:
            summary.validations_ok.append("Target PostgreSQL inspection skipped because DATABASE_URL is absent.")
            print("Target inspection skipped: DATABASE_URL is not set.")

        if args.dry_run:
            summary.validations_ok.append("Dry-run completed without writing to PostgreSQL.")
            return_code = 0
        else:
            if not target_url:
                raise RuntimeError("DATABASE_URL is required for a real migration.")
            if not summary.migration_order_valid:
                raise RuntimeError("Migration order validation failed.")
            if summary.target_schema_diffs:
                raise RuntimeError("Target schema differs from source/models. Resolve before migrating.")
            _assert_target_is_empty(target_engine)
            _confirm_migration()
            _migrate_rows(source_engine, target_engine, summary)
            if target_engine.dialect.name == "postgresql":
                _reset_sequences(target_engine)
            summary.validations_ok.append("Migration completed and sequences reset.")
            return_code = 0

    except (SQLAlchemyError, RuntimeError) as exc:
        summary.errors.append(str(exc))
        summary.validations_failed.append(str(exc))
        return_code = 1
    finally:
        summary.duration_seconds = time.perf_counter() - start
        _print_summary(summary)
        if source_engine is not None:
            source_engine.dispose()
        if target_engine is not None:
            target_engine.dispose()

    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
