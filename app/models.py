from datetime import datetime
from decimal import Decimal

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class Product(Base):
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    sku: Mapped[str] = mapped_column(String(100), unique=True, index=True, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    unit: Mapped[str | None] = mapped_column(String(50), nullable=True)
    standard_cost: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    loyverse_handle: Mapped[str | None] = mapped_column(String(255), nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )


class ImportBatch(Base):
    __tablename__ = "import_batches"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    file_name: Mapped[str] = mapped_column(String(255), nullable=False)
    imported_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    notes: Mapped[str | None] = mapped_column(String(500), nullable=True)

    bom_headers: Mapped[list["ImportedBomHeader"]] = relationship(
        back_populates="import_batch",
        cascade="all, delete-orphan",
    )


class ImportedBomHeader(Base):
    __tablename__ = "imported_bom_headers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    import_batch_id: Mapped[int] = mapped_column(ForeignKey("import_batches.id"), nullable=False)
    product_sku: Mapped[str] = mapped_column(String(100), index=True, nullable=False)
    product_name: Mapped[str] = mapped_column(String(255), nullable=False)
    standard_cost: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    use_production: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    imported_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    import_batch: Mapped[ImportBatch] = relationship(back_populates="bom_headers")
    bom_lines: Mapped[list["ImportedBomLine"]] = relationship(
        back_populates="bom_header",
        cascade="all, delete-orphan",
    )


class ImportedBomLine(Base):
    __tablename__ = "imported_bom_lines"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    bom_header_id: Mapped[int] = mapped_column(ForeignKey("imported_bom_headers.id"), nullable=False)
    source_row_number: Mapped[int] = mapped_column(Integer, nullable=False)
    component_sku: Mapped[str | None] = mapped_column(String(100), index=True, nullable=True)
    component_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    quantity: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    component_cost: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    component_type: Mapped[str] = mapped_column(String(50), default="unknown", nullable=False)
    include_in_real_cost: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    bom_header: Mapped[ImportedBomHeader] = relationship(back_populates="bom_lines")
