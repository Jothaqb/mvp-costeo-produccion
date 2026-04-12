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
