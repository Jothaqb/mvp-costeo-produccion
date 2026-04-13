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


def _ensure_columns(connection, table_name: str, column_definitions: dict[str, str]) -> None:
    columns = connection.exec_driver_sql(f"PRAGMA table_info({table_name})").fetchall()
    if not columns:
        return

    column_names = {column[1] for column in columns}
    for column_name, column_type in column_definitions.items():
        if column_name not in column_names:
            connection.exec_driver_sql(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
