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


def ensure_product_planning_columns() -> None:
    if engine.dialect.name != "sqlite":
        return

    with engine.begin() as connection:
        _ensure_columns(
            connection,
            "products",
            {
                "available_for_sale_gc": "BOOLEAN NOT NULL DEFAULT 0",
                "supplier": "VARCHAR(255)",
                "current_inventory_qty": "NUMERIC(12, 4)",
                "loyverse_inventory_refreshed_at": "DATETIME",
                "loyverse_cost_refreshed_at": "DATETIME",
                "low_stock_qty": "NUMERIC(12, 4)",
                "optimal_stock_qty": "NUMERIC(12, 4)",
                "planning_moq": "NUMERIC(12, 4)",
                "planning_quantity": "NUMERIC(12, 4)",
            },
        )


def ensure_master_data_tables() -> None:
    if engine.dialect.name != "sqlite":
        return

    with engine.begin() as connection:
        connection.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS product_categories (
                id INTEGER NOT NULL PRIMARY KEY,
                name VARCHAR(255) NOT NULL UNIQUE,
                description TEXT,
                active BOOLEAN NOT NULL DEFAULT 1,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL
            )
            """
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_product_categories_name ON product_categories (name)"
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_product_categories_active ON product_categories (active)"
        )

        connection.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS suppliers (
                id INTEGER NOT NULL PRIMARY KEY,
                name VARCHAR(255) NOT NULL UNIQUE,
                contact_name VARCHAR(255),
                phone VARCHAR(100),
                email VARCHAR(255),
                notes TEXT,
                active BOOLEAN NOT NULL DEFAULT 1,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL
            )
            """
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_suppliers_name ON suppliers (name)"
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_suppliers_active ON suppliers (active)"
        )

        _ensure_columns(
            connection,
            "products",
            {
                "category_id": "INTEGER",
                "supplier_id": "INTEGER",
                "description": "TEXT",
                "observations": "TEXT",
                "b2c_price": "NUMERIC(12, 4)",
                "b2b_price": "NUMERIC(12, 4)",
                "is_purchased_product": "BOOLEAN NOT NULL DEFAULT 0",
            },
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_products_category_id ON products (category_id)"
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_products_supplier_id ON products (supplier_id)"
        )
        _ensure_columns(
            connection,
            "imported_bom_headers",
            {
                "category_name_snapshot": "VARCHAR(255)",
                "b2b_price_snapshot": "NUMERIC(12, 4)",
            },
        )
        _ensure_columns(
            connection,
            "import_batches",
            {
                "product_master_upsert_count": "INTEGER NOT NULL DEFAULT 0",
            },
        )


def ensure_discount_master_tables() -> None:
    if engine.dialect.name != "sqlite":
        return

    with engine.begin() as connection:
        connection.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS discount_rules (
                id INTEGER NOT NULL PRIMARY KEY,
                name VARCHAR(255) NOT NULL UNIQUE,
                discount_type VARCHAR(50) NOT NULL,
                value NUMERIC(12, 4) NOT NULL,
                applies_to VARCHAR(50) NOT NULL,
                channel VARCHAR(50) NOT NULL,
                active BOOLEAN NOT NULL DEFAULT 1,
                description TEXT,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                CONSTRAINT ck_discount_rules_discount_type CHECK (discount_type IN ('percentage')),
                CONSTRAINT ck_discount_rules_applies_to CHECK (applies_to IN ('order_total')),
                CONSTRAINT ck_discount_rules_channel CHECK (channel IN ('b2c'))
            )
            """
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_discount_rules_name ON discount_rules (name)"
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_discount_rules_active ON discount_rules (active)"
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_discount_rules_channel_applies_to "
            "ON discount_rules (channel, applies_to, active)"
        )

        _ensure_columns(
            connection,
            "b2c_sales_orders",
            {
                "discount_rule_id": "INTEGER",
                "discount_name_snapshot": "VARCHAR(255)",
                "discount_type_snapshot": "VARCHAR(50)",
                "discount_value_snapshot": "NUMERIC(12, 4)",
                "discount_amount": "NUMERIC(12, 4) NOT NULL DEFAULT 0",
                "cost_total_snapshot": "NUMERIC(12, 4)",
                "gross_margin_amount": "NUMERIC(12, 4)",
                "gross_margin_percent": "NUMERIC(12, 4)",
            },
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_b2c_sales_orders_discount_rule_id ON b2c_sales_orders (discount_rule_id)"
        )

        _ensure_columns(
            connection,
            "b2c_sales_order_lines",
            {
                "discount_amount_snapshot": "NUMERIC(12, 4)",
                "net_line_total_snapshot": "NUMERIC(12, 4)",
            },
        )


def ensure_product_bom_tables() -> None:
    if engine.dialect.name != "sqlite":
        return

    with engine.begin() as connection:
        connection.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS product_bom_headers (
                id INTEGER NOT NULL PRIMARY KEY,
                product_id INTEGER NOT NULL UNIQUE,
                name VARCHAR(255),
                active BOOLEAN NOT NULL DEFAULT 1,
                source_type VARCHAR(50),
                source_imported_bom_header_id INTEGER,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                FOREIGN KEY(product_id) REFERENCES products (id),
                FOREIGN KEY(source_imported_bom_header_id) REFERENCES imported_bom_headers (id)
            )
            """
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_product_bom_headers_product_id ON product_bom_headers (product_id)"
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_product_bom_headers_source_imported_bom_header_id "
            "ON product_bom_headers (source_imported_bom_header_id)"
        )

        connection.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS product_bom_lines (
                id INTEGER NOT NULL PRIMARY KEY,
                bom_header_id INTEGER NOT NULL,
                component_product_id INTEGER,
                component_sku_snapshot VARCHAR(100),
                component_name_snapshot VARCHAR(255),
                unit_snapshot VARCHAR(50),
                quantity_standard NUMERIC(12, 4),
                line_number INTEGER NOT NULL,
                notes TEXT,
                source_imported_bom_line_id INTEGER,
                component_type VARCHAR(50) NOT NULL DEFAULT 'material',
                include_in_real_cost BOOLEAN NOT NULL DEFAULT 1,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                FOREIGN KEY(bom_header_id) REFERENCES product_bom_headers (id),
                FOREIGN KEY(component_product_id) REFERENCES products (id),
                FOREIGN KEY(source_imported_bom_line_id) REFERENCES imported_bom_lines (id),
                CONSTRAINT uq_product_bom_lines_line_number UNIQUE (bom_header_id, line_number)
            )
            """
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_product_bom_lines_bom_header_id ON product_bom_lines (bom_header_id)"
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_product_bom_lines_component_product_id "
            "ON product_bom_lines (component_product_id)"
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_product_bom_lines_component_sku_snapshot "
            "ON product_bom_lines (component_sku_snapshot)"
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_product_bom_lines_source_imported_bom_line_id "
            "ON product_bom_lines (source_imported_bom_line_id)"
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



def ensure_b2b_invoice_snapshot_columns() -> None:
    if engine.dialect.name != "sqlite":
        return

    with engine.begin() as connection:
        _ensure_columns(
            connection,
            "b2b_sales_order_lines",
            {
                "cost_unit_snapshot": "NUMERIC(12, 4)",
                "cost_total_snapshot": "NUMERIC(12, 4)",
                "gross_margin_amount": "NUMERIC(12, 4)",
                "gross_margin_percent": "NUMERIC(12, 4)",
            },
        )


def ensure_b2c_sales_tables() -> None:
    if engine.dialect.name != "sqlite":
        return

    with engine.begin() as connection:
        connection.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS b2c_sales_orders (
                id INTEGER NOT NULL PRIMARY KEY,
                order_number VARCHAR(100) NOT NULL UNIQUE,
                order_date DATE NOT NULL,
                customer_name VARCHAR(255),
                customer_phone VARCHAR(100),
                customer_email VARCHAR(255),
                channel VARCHAR(50) NOT NULL,
                status VARCHAR(50) NOT NULL,
                subtotal_amount NUMERIC(12, 4) NOT NULL DEFAULT 0,
                total_amount NUMERIC(12, 4) NOT NULL DEFAULT 0,
                observations TEXT,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL
            )
            """
        )
        connection.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS b2c_sales_order_lines (
                id INTEGER NOT NULL PRIMARY KEY,
                sales_order_id INTEGER NOT NULL,
                line_number INTEGER NOT NULL,
                sku_snapshot VARCHAR(100) NOT NULL,
                description_snapshot VARCHAR(255) NOT NULL,
                quantity NUMERIC(12, 4) NOT NULL,
                unit_price_snapshot NUMERIC(12, 4) NOT NULL,
                line_total NUMERIC(12, 4) NOT NULL,
                cost_unit_snapshot NUMERIC(12, 4),
                cost_total_snapshot NUMERIC(12, 4),
                gross_margin_amount NUMERIC(12, 4),
                gross_margin_percent NUMERIC(12, 4),
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                FOREIGN KEY(sales_order_id) REFERENCES b2c_sales_orders (id)
            )
            """
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_b2c_sales_orders_order_number ON b2c_sales_orders (order_number)"
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_b2c_sales_orders_order_date ON b2c_sales_orders (order_date)"
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_b2c_sales_orders_status ON b2c_sales_orders (status)"
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_b2c_sales_order_lines_sales_order_id ON b2c_sales_order_lines (sales_order_id)"
        )


def ensure_b2c_customer_tables() -> None:
    if engine.dialect.name != "sqlite":
        return

    with engine.begin() as connection:
        connection.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS b2c_customers (
                id INTEGER NOT NULL PRIMARY KEY,
                active BOOLEAN NOT NULL DEFAULT 1,
                name VARCHAR(255) NOT NULL,
                phone VARCHAR(100),
                email VARCHAR(255),
                address VARCHAR(500),
                province VARCHAR(100),
                canton VARCHAR(100),
                district VARCHAR(100),
                observations TEXT,
                source_customer_mapping_id INTEGER UNIQUE,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                FOREIGN KEY(source_customer_mapping_id) REFERENCES loyverse_customer_mappings (id)
            )
            """
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_b2c_customers_active_name ON b2c_customers (active, name)"
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_b2c_customers_source_customer_mapping_id "
            "ON b2c_customers (source_customer_mapping_id)"
        )
        _ensure_columns(
            connection,
            "b2c_sales_orders",
            {
                "b2c_customer_id": "INTEGER",
                "customer_address_snapshot": "VARCHAR(500)",
                "province_snapshot": "VARCHAR(100)",
                "canton_snapshot": "VARCHAR(100)",
                "district_snapshot": "VARCHAR(100)",
                "customer_observations_snapshot": "TEXT",
            },
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_b2c_sales_orders_b2c_customer_id ON b2c_sales_orders (b2c_customer_id)"
        )


def ensure_purchase_order_tables() -> None:
    if engine.dialect.name != "sqlite":
        return

    with engine.begin() as connection:
        connection.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS purchase_orders (
                id INTEGER NOT NULL PRIMARY KEY,
                po_number VARCHAR(100) NOT NULL UNIQUE,
                supplier_name_snapshot VARCHAR(255) NOT NULL,
                po_date DATE NOT NULL,
                status VARCHAR(50) NOT NULL,
                notes TEXT,
                estimated_total NUMERIC(12, 4) NOT NULL DEFAULT 0,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL
            )
            """
        )
        connection.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS purchase_order_lines (
                id INTEGER NOT NULL PRIMARY KEY,
                purchase_order_id INTEGER NOT NULL,
                line_number INTEGER NOT NULL,
                sku_snapshot VARCHAR(100) NOT NULL,
                description_snapshot VARCHAR(255) NOT NULL,
                supplier_name_snapshot VARCHAR(255) NOT NULL,
                quantity NUMERIC(12, 4) NOT NULL,
                received_quantity NUMERIC(12, 4) NOT NULL DEFAULT 0,
                unit_cost_snapshot NUMERIC(12, 4) NOT NULL,
                line_total NUMERIC(12, 4) NOT NULL,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                FOREIGN KEY(purchase_order_id) REFERENCES purchase_orders (id)
            )
            """
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_purchase_orders_po_number ON purchase_orders (po_number)"
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_purchase_orders_po_date ON purchase_orders (po_date)"
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_purchase_orders_supplier_name_snapshot ON purchase_orders (supplier_name_snapshot)"
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_purchase_order_lines_purchase_order_id ON purchase_order_lines (purchase_order_id)"
        )
        _ensure_purchase_order_status_supports_receive_workflow(connection)
        _ensure_purchase_order_lines_received_quantity(connection)
        _ensure_purchase_order_receive_tokens_table(connection)


def ensure_inventory_ledger_tables() -> None:
    if engine.dialect.name != "sqlite":
        return

    with engine.begin() as connection:
        connection.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS inventory_transactions (
                id INTEGER NOT NULL PRIMARY KEY,
                product_id INTEGER NOT NULL,
                transaction_date DATETIME NOT NULL,
                transaction_type VARCHAR(50) NOT NULL,
                source_type VARCHAR(50),
                source_id INTEGER,
                source_line_id INTEGER,
                quantity_in NUMERIC(12, 4) NOT NULL DEFAULT 0,
                quantity_out NUMERIC(12, 4) NOT NULL DEFAULT 0,
                unit_cost NUMERIC(12, 4),
                total_cost NUMERIC(12, 4),
                running_quantity NUMERIC(12, 4) NOT NULL,
                running_average_cost NUMERIC(12, 4) NOT NULL,
                running_inventory_value NUMERIC(12, 4) NOT NULL,
                notes TEXT,
                created_at DATETIME NOT NULL,
                FOREIGN KEY(product_id) REFERENCES products (id)
            )
            """
        )
        connection.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS inventory_balances (
                id INTEGER NOT NULL PRIMARY KEY,
                product_id INTEGER NOT NULL UNIQUE,
                on_hand_qty NUMERIC(12, 4) NOT NULL DEFAULT 0,
                average_unit_cost NUMERIC(12, 4) NOT NULL DEFAULT 0,
                inventory_value NUMERIC(12, 4) NOT NULL DEFAULT 0,
                last_transaction_id INTEGER,
                last_transaction_at DATETIME,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                FOREIGN KEY(product_id) REFERENCES products (id),
                FOREIGN KEY(last_transaction_id) REFERENCES inventory_transactions (id)
            )
            """
        )
        connection.exec_driver_sql(
            "CREATE UNIQUE INDEX IF NOT EXISTS ix_inventory_balances_product_id ON inventory_balances (product_id)"
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_inventory_transactions_product_date_id ON inventory_transactions (product_id, transaction_date, id)"
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_inventory_transactions_source ON inventory_transactions (source_type, source_id)"
        )


def ensure_inventory_adjustment_tables() -> None:
    if engine.dialect.name != "sqlite":
        return

    with engine.begin() as connection:
        connection.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS inventory_adjustment_post_tokens (
                id INTEGER NOT NULL PRIMARY KEY,
                token VARCHAR(255) NOT NULL UNIQUE,
                used_at DATETIME,
                created_at DATETIME NOT NULL
            )
            """
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_inventory_adjustment_post_tokens_token "
            "ON inventory_adjustment_post_tokens (token)"
        )
        connection.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS inventory_adjustments (
                id INTEGER NOT NULL PRIMARY KEY,
                adjustment_number VARCHAR(100) NOT NULL UNIQUE,
                adjustment_date DATE NOT NULL,
                product_id INTEGER NOT NULL,
                sku_snapshot VARCHAR(100) NOT NULL,
                product_name_snapshot VARCHAR(255) NOT NULL,
                adjustment_mode VARCHAR(50) NOT NULL,
                adjustment_type VARCHAR(50) NOT NULL,
                transaction_type VARCHAR(50) NOT NULL,
                reason VARCHAR(50) NOT NULL,
                current_qty_snapshot NUMERIC(12, 4) NOT NULL,
                counted_qty NUMERIC(12, 4),
                quantity_adjustment NUMERIC(12, 4) NOT NULL,
                unit_cost NUMERIC(12, 4),
                total_cost NUMERIC(12, 4),
                notes TEXT,
                warning_notes TEXT,
                status VARCHAR(50) NOT NULL,
                inventory_transaction_id INTEGER,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                CONSTRAINT ck_inventory_adjustments_mode CHECK (adjustment_mode IN ('quantity_adjustment', 'stock_count')),
                CONSTRAINT ck_inventory_adjustments_type CHECK (adjustment_type IN ('increase', 'decrease')),
                CONSTRAINT ck_inventory_adjustments_status CHECK (status IN ('posted')),
                CONSTRAINT ck_inventory_adjustments_reason CHECK (reason IN ('physical_count', 'damage', 'waste', 'correction', 'other')),
                FOREIGN KEY(product_id) REFERENCES products (id),
                FOREIGN KEY(inventory_transaction_id) REFERENCES inventory_transactions (id)
            )
            """
        )
        connection.exec_driver_sql(
            "CREATE UNIQUE INDEX IF NOT EXISTS ix_inventory_adjustments_adjustment_number "
            "ON inventory_adjustments (adjustment_number)"
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_inventory_adjustments_product_id "
            "ON inventory_adjustments (product_id)"
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_inventory_adjustments_inventory_transaction_id "
            "ON inventory_adjustments (inventory_transaction_id)"
        )
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_inventory_adjustments_adjustment_date_id "
            "ON inventory_adjustments (adjustment_date, id)"
        )


def _ensure_purchase_order_status_supports_receive_workflow(connection) -> None:
    create_sql_row = connection.exec_driver_sql(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='purchase_orders'"
    ).fetchone()
    create_sql = (create_sql_row[0] or "") if create_sql_row else ""
    if "'incomplete'" in create_sql and "'closed'" in create_sql:
        return

    connection.exec_driver_sql("ALTER TABLE purchase_orders RENAME TO purchase_orders_old")
    connection.exec_driver_sql(
        """
        CREATE TABLE purchase_orders (
            id INTEGER NOT NULL PRIMARY KEY,
            po_number VARCHAR(100) NOT NULL UNIQUE,
            supplier_name_snapshot VARCHAR(255) NOT NULL,
            po_date DATE NOT NULL,
            status VARCHAR(50) NOT NULL,
            notes TEXT,
            estimated_total NUMERIC(12, 4) NOT NULL DEFAULT 0,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL
        )
        """
    )
    connection.exec_driver_sql(
        """
        INSERT INTO purchase_orders (
            id,
            po_number,
            supplier_name_snapshot,
            po_date,
            status,
            notes,
            estimated_total,
            created_at,
            updated_at
        )
        SELECT
            id,
            po_number,
            supplier_name_snapshot,
            po_date,
            status,
            notes,
            estimated_total,
            created_at,
            updated_at
        FROM purchase_orders_old
        """
    )
    connection.exec_driver_sql("DROP TABLE purchase_orders_old")
    connection.exec_driver_sql(
        "CREATE INDEX IF NOT EXISTS ix_purchase_orders_po_number ON purchase_orders (po_number)"
    )
    connection.exec_driver_sql(
        "CREATE INDEX IF NOT EXISTS ix_purchase_orders_po_date ON purchase_orders (po_date)"
    )
    connection.exec_driver_sql(
        "CREATE INDEX IF NOT EXISTS ix_purchase_orders_supplier_name_snapshot ON purchase_orders (supplier_name_snapshot)"
    )


def _ensure_purchase_order_lines_received_quantity(connection) -> None:
    columns = {
        row[1]
        for row in connection.exec_driver_sql("PRAGMA table_info('purchase_order_lines')").fetchall()
    }
    if "received_quantity" not in columns:
        connection.exec_driver_sql(
            "ALTER TABLE purchase_order_lines ADD COLUMN received_quantity NUMERIC(12, 4) NOT NULL DEFAULT 0"
        )


def _ensure_purchase_order_receive_tokens_table(connection) -> None:
    connection.exec_driver_sql(
        """
        CREATE TABLE IF NOT EXISTS purchase_order_receive_tokens (
            id INTEGER NOT NULL PRIMARY KEY,
            purchase_order_id INTEGER NOT NULL,
            token VARCHAR(255) NOT NULL UNIQUE,
            used_at DATETIME,
            created_at DATETIME NOT NULL,
            FOREIGN KEY(purchase_order_id) REFERENCES purchase_orders (id)
        )
        """
    )
    connection.exec_driver_sql(
        "CREATE INDEX IF NOT EXISTS ix_purchase_order_receive_tokens_purchase_order_id ON purchase_order_receive_tokens (purchase_order_id)"
    )
    connection.exec_driver_sql(
        "CREATE INDEX IF NOT EXISTS ix_purchase_order_receive_tokens_token ON purchase_order_receive_tokens (token)"
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
