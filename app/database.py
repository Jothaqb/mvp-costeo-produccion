from collections.abc import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker


SQLALCHEMY_DATABASE_URL = "sqlite:///./costeo.db"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False},
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def ensure_product_default_route_column() -> None:
    if engine.dialect.name != "sqlite":
        return

    with engine.begin() as connection:
        columns = connection.exec_driver_sql("PRAGMA table_info(products)").fetchall()
        if not columns:
            return

        column_names = {column[1] for column in columns}
        if "default_route_id" not in column_names:
            connection.exec_driver_sql("ALTER TABLE products ADD COLUMN default_route_id INTEGER")


def ensure_product_is_manufactured_column() -> None:
    if engine.dialect.name != "sqlite":
        return

    with engine.begin() as connection:
        columns = connection.exec_driver_sql("PRAGMA table_info(products)").fetchall()
        if not columns:
            return

        column_names = {column[1] for column in columns}
        if "is_manufactured" not in column_names:
            connection.exec_driver_sql(
                "ALTER TABLE products ADD COLUMN is_manufactured BOOLEAN NOT NULL DEFAULT 0"
            )


def ensure_product_loyverse_mapping_columns() -> None:
    if engine.dialect.name != "sqlite":
        return

    with engine.begin() as connection:
        _ensure_columns(
            connection,
            "products",
            {
                "loyverse_item_id": "VARCHAR(100)",
                "loyverse_variant_id": "VARCHAR(100)",
            },
        )


def ensure_app_sequences_table() -> None:
    if engine.dialect.name != "sqlite":
        return

    with engine.begin() as connection:
        connection.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS app_sequences (
                id INTEGER NOT NULL PRIMARY KEY,
                name VARCHAR(100) NOT NULL UNIQUE,
                next_value INTEGER NOT NULL
            )
            """
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_app_sequences_name ON app_sequences (name)"
        )


def ensure_sprint7c_lot_columns_and_tables() -> None:
    if engine.dialect.name != "sqlite":
        return

    with engine.begin() as connection:
        _ensure_columns(
            connection,
            "production_orders",
            {
                "lot_number": "VARCHAR(50)",
            },
        )
        connection.exec_driver_sql(
            "CREATE UNIQUE INDEX IF NOT EXISTS ix_production_orders_lot_number "
            "ON production_orders (lot_number)"
        )
        connection.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS lot_sequences (
                id INTEGER NOT NULL PRIMARY KEY,
                iso_year INTEGER NOT NULL,
                iso_week INTEGER NOT NULL,
                product_sku VARCHAR(100) NOT NULL,
                next_value INTEGER NOT NULL,
                CONSTRAINT uq_lot_sequence_scope UNIQUE (iso_year, iso_week, product_sku)
            )
            """
        )


def ensure_sprint4_costing_columns() -> None:
    if engine.dialect.name != "sqlite":
        return

    with engine.begin() as connection:
        _ensure_columns(
            connection,
            "production_orders",
            {
                "material_snapshot_cost_total": "NUMERIC(12, 4)",
                "real_labor_cost_total": "NUMERIC(12, 4)",
                "real_overhead_cost_total": "NUMERIC(12, 4)",
                "real_machine_cost_total": "NUMERIC(12, 4)",
                "real_total_cost": "NUMERIC(12, 4)",
                "real_unit_cost": "NUMERIC(12, 4)",
            },
        )
        _ensure_columns(
            connection,
            "production_order_activities",
            {
                "machine_id_snapshot": "INTEGER",
                "machine_code_snapshot": "VARCHAR(50)",
                "machine_name_snapshot": "VARCHAR(255)",
                "labor_cost": "NUMERIC(12, 4) NOT NULL DEFAULT 0",
                "overhead_cost": "NUMERIC(12, 4) NOT NULL DEFAULT 0",
                "machine_cost": "NUMERIC(12, 4) NOT NULL DEFAULT 0",
                "total_activity_cost": "NUMERIC(12, 4) NOT NULL DEFAULT 0",
            },
        )


def ensure_sprint5_comparison_columns() -> None:
    if engine.dialect.name != "sqlite":
        return

    with engine.begin() as connection:
        _ensure_columns(
            connection,
            "production_orders",
            {
                "variance_amount": "NUMERIC(12, 4)",
                "variance_percent": "NUMERIC(12, 4)",
            },
        )


def ensure_sprint6_loyverse_cost_sync_columns() -> None:
    if engine.dialect.name != "sqlite":
        return

    with engine.begin() as connection:
        _ensure_columns(
            connection,
            "production_orders",
            {
                "loyverse_cost_sync_status": "VARCHAR(50)",
                "loyverse_cost_sync_attempted_at": "DATETIME",
                "loyverse_cost_sync_error": "TEXT",
                "loyverse_cost_sync_variant_id": "VARCHAR(100)",
                "loyverse_cost_sync_pushed_cost": "NUMERIC(12, 4)",
            },
        )


def ensure_production_loyverse_inventory_sync_columns() -> None:
    if engine.dialect.name != "sqlite":
        return

    with engine.begin() as connection:
        _ensure_columns(
            connection,
            "production_orders",
            {
                "loyverse_inventory_sync_status": "VARCHAR(50)",
                "loyverse_inventory_sync_error": "TEXT",
                "loyverse_inventory_sync_attempted_at": "DATETIME",
                "loyverse_inventory_synced_at": "DATETIME",
                "loyverse_inventory_sync_attempt_count": "INTEGER NOT NULL DEFAULT 0",
                "loyverse_inventory_store_id_snapshot": "VARCHAR(100)",
                "loyverse_inventory_response_summary": "TEXT",
                "loyverse_inventory_request_fingerprint": "VARCHAR(128)",
                "loyverse_inventory_payload_summary": "TEXT",
            },
        )
        _ensure_columns(
            connection,
            "production_order_materials",
            {
                "required_quantity": "NUMERIC(12, 4)",
            },
        )


def ensure_b2b_sales_followup_columns() -> None:
    if engine.dialect.name != "sqlite":
        return

    with engine.begin() as connection:
        _ensure_columns(
            connection,
            "b2b_sales_orders",
            {
                "observations": "TEXT",
            },
        )


def ensure_b2b_loyverse_mapping_tables() -> None:
    if engine.dialect.name != "sqlite":
        return

    with engine.begin() as connection:
        _ensure_columns(
            connection,
            "b2b_sales_orders",
            {
                "b2b_channel_name_snapshot": "VARCHAR(255)",
                "loyverse_payment_type_id_snapshot": "VARCHAR(100)",
                "loyverse_receipt_id": "VARCHAR(100)",
                "loyverse_receipt_number": "VARCHAR(100)",
                "loyverse_invoice_sync_status": "VARCHAR(50)",
                "loyverse_invoice_sync_error": "TEXT",
                "loyverse_invoice_sync_attempted_at": "DATETIME",
                "loyverse_invoice_synced_at": "DATETIME",
                "loyverse_invoice_sync_attempt_count": "INTEGER NOT NULL DEFAULT 0",
            },
        )
        _ensure_columns(
            connection,
            "b2b_sales_order_lines",
            {
                "loyverse_variant_id_snapshot": "VARCHAR(100)",
            },
        )
        connection.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS loyverse_customer_mappings (
                id INTEGER NOT NULL PRIMARY KEY,
                loyverse_customer_id VARCHAR(100) NOT NULL UNIQUE,
                customer_name VARCHAR(255),
                phone VARCHAR(100),
                email VARCHAR(255),
                active BOOLEAN NOT NULL DEFAULT 1,
                last_refreshed_at DATETIME NOT NULL
            )
            """
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_loyverse_customer_mappings_loyverse_customer_id "
            "ON loyverse_customer_mappings (loyverse_customer_id)"
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_loyverse_customer_mappings_phone "
            "ON loyverse_customer_mappings (phone)"
        )
        connection.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS loyverse_variant_mappings (
                id INTEGER NOT NULL PRIMARY KEY,
                sku VARCHAR(100),
                loyverse_variant_id VARCHAR(100) NOT NULL UNIQUE,
                loyverse_item_id VARCHAR(100),
                item_name VARCHAR(255),
                variant_name VARCHAR(255),
                active BOOLEAN NOT NULL DEFAULT 1,
                last_refreshed_at DATETIME NOT NULL
            )
            """
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_loyverse_variant_mappings_sku "
            "ON loyverse_variant_mappings (sku)"
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_loyverse_variant_mappings_loyverse_variant_id "
            "ON loyverse_variant_mappings (loyverse_variant_id)"
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_loyverse_variant_mappings_loyverse_item_id "
            "ON loyverse_variant_mappings (loyverse_item_id)"
        )
        connection.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS loyverse_payment_type_mappings (
                id INTEGER NOT NULL PRIMARY KEY,
                loyverse_payment_type_id VARCHAR(100) NOT NULL UNIQUE,
                name VARCHAR(255) NOT NULL,
                payment_type VARCHAR(100),
                active BOOLEAN NOT NULL DEFAULT 1,
                last_refreshed_at DATETIME NOT NULL
            )
            """
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_loyverse_payment_type_mappings_loyverse_payment_type_id "
            "ON loyverse_payment_type_mappings (loyverse_payment_type_id)"
        )


def _ensure_columns(connection, table_name: str, column_definitions: dict[str, str]) -> None:
    columns = connection.exec_driver_sql(f"PRAGMA table_info({table_name})").fetchall()
    if not columns:
        return

    column_names = {column[1] for column in columns}
    for column_name, column_type in column_definitions.items():
        if column_name not in column_names:
            connection.exec_driver_sql(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
