from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import Boolean, CheckConstraint, Date, DateTime, ForeignKey, Integer, Numeric, String, UniqueConstraint
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
    default_route_id: Mapped[int | None] = mapped_column(ForeignKey("routes.id"), nullable=True)
    is_manufactured: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    default_route: Mapped["Route | None"] = relationship("Route", back_populates="products")


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


class Machine(Base):
    __tablename__ = "machines"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    code: Mapped[str] = mapped_column(String(50), unique=True, index=True, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    activities: Mapped[list["Activity"]] = relationship(back_populates="default_machine")
    rates: Mapped[list["MachineRate"]] = relationship(back_populates="machine")


class Activity(Base):
    __tablename__ = "activities"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    code: Mapped[str] = mapped_column(String(50), unique=True, index=True, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(String(500), nullable=True)
    applies_labor: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    applies_machine: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    default_machine_id: Mapped[int | None] = mapped_column(ForeignKey("machines.id"), nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    default_machine: Mapped[Machine | None] = relationship(back_populates="activities")
    route_activities: Mapped[list["RouteActivity"]] = relationship(back_populates="activity")


class Route(Base):
    __tablename__ = "routes"
    __table_args__ = (
        CheckConstraint(
            "process_type IN ('dehydration', 'grinding', 'mixing', 'packaging')",
            name="ck_routes_process_type",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    code: Mapped[str] = mapped_column(String(50), unique=True, index=True, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    process_type: Mapped[str] = mapped_column(String(50), nullable=False)
    version: Mapped[str] = mapped_column(String(50), default="1", nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    route_activities: Mapped[list["RouteActivity"]] = relationship(
        back_populates="route",
        cascade="all, delete-orphan",
    )
    products: Mapped[list[Product]] = relationship(back_populates="default_route")


class RouteActivity(Base):
    __tablename__ = "route_activities"
    __table_args__ = (UniqueConstraint("route_id", "sequence", name="uq_route_activity_sequence"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    route_id: Mapped[int] = mapped_column(ForeignKey("routes.id"), nullable=False)
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    activity_id: Mapped[int] = mapped_column(ForeignKey("activities.id"), nullable=False)
    required: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    visible_default: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    route: Mapped[Route] = relationship(back_populates="route_activities")
    activity: Mapped[Activity] = relationship(back_populates="route_activities")


class LaborRate(Base):
    __tablename__ = "labor_rates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    effective_from: Mapped[date] = mapped_column(Date, nullable=False)
    effective_to: Mapped[date | None] = mapped_column(Date, nullable=True)
    hourly_rate: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    notes: Mapped[str | None] = mapped_column(String(500), nullable=True)


class OverheadRate(Base):
    __tablename__ = "overhead_rates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    effective_from: Mapped[date] = mapped_column(Date, nullable=False)
    effective_to: Mapped[date | None] = mapped_column(Date, nullable=True)
    hourly_rate: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    notes: Mapped[str | None] = mapped_column(String(500), nullable=True)


class MachineRate(Base):
    __tablename__ = "machine_rates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    machine_id: Mapped[int] = mapped_column(ForeignKey("machines.id"), nullable=False)
    effective_from: Mapped[date] = mapped_column(Date, nullable=False)
    effective_to: Mapped[date | None] = mapped_column(Date, nullable=True)
    hourly_rate: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    notes: Mapped[str | None] = mapped_column(String(500), nullable=True)

    machine: Mapped[Machine] = relationship(back_populates="rates")
