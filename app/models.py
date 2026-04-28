from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import Boolean, CheckConstraint, Date, DateTime, ForeignKey, Integer, Numeric, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class ProductCategory(Base):
    __tablename__ = "product_categories"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, index=True, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    products: Mapped[list["Product"]] = relationship("Product", back_populates="category")


class Supplier(Base):
    __tablename__ = "suppliers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, index=True, nullable=False)
    contact_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    phone: Mapped[str | None] = mapped_column(String(100), nullable=True)
    email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    products: Mapped[list["Product"]] = relationship("Product", back_populates="supplier_record")


class DiscountRule(Base):
    __tablename__ = "discount_rules"
    __table_args__ = (
        CheckConstraint("discount_type IN ('percentage')", name="ck_discount_rules_discount_type"),
        CheckConstraint("applies_to IN ('order_total')", name="ck_discount_rules_applies_to"),
        CheckConstraint("channel IN ('b2c')", name="ck_discount_rules_channel"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, index=True, nullable=False)
    discount_type: Mapped[str] = mapped_column(String(50), nullable=False)
    value: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    applies_to: Mapped[str] = mapped_column(String(50), nullable=False)
    channel: Mapped[str] = mapped_column(String(50), nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    b2c_orders: Mapped[list["B2CSalesOrder"]] = relationship("B2CSalesOrder", back_populates="discount_rule")


class Product(Base):
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    sku: Mapped[str] = mapped_column(String(100), unique=True, index=True, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    unit: Mapped[str | None] = mapped_column(String(50), nullable=True)
    category_id: Mapped[int | None] = mapped_column(ForeignKey("product_categories.id"), nullable=True)
    supplier_id: Mapped[int | None] = mapped_column(ForeignKey("suppliers.id"), nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    observations: Mapped[str | None] = mapped_column(Text, nullable=True)
    b2c_price: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    b2b_price: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    standard_cost: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    loyverse_handle: Mapped[str | None] = mapped_column(String(255), nullable=True)
    loyverse_item_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    loyverse_variant_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    default_route_id: Mapped[int | None] = mapped_column(ForeignKey("routes.id"), nullable=True)
    is_manufactured: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_purchased_product: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    available_for_sale_gc: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    supplier: Mapped[str | None] = mapped_column(String(255), nullable=True)
    current_inventory_qty: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    loyverse_inventory_refreshed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    loyverse_cost_refreshed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    low_stock_qty: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    optimal_stock_qty: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    planning_moq: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    planning_quantity: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    planning_zones_manual_override: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    default_route: Mapped["Route | None"] = relationship("Route", back_populates="products")
    category: Mapped["ProductCategory | None"] = relationship("ProductCategory", back_populates="products")
    supplier_record: Mapped["Supplier | None"] = relationship("Supplier", back_populates="products")
    bom_header: Mapped["ProductBomHeader | None"] = relationship(
        "ProductBomHeader",
        back_populates="product",
        uselist=False,
    )


class AppSequence(Base):
    __tablename__ = "app_sequences"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(100), unique=True, index=True, nullable=False)
    next_value: Mapped[int] = mapped_column(Integer, nullable=False)


class LotSequence(Base):
    __tablename__ = "lot_sequences"
    __table_args__ = (
        UniqueConstraint("iso_year", "iso_week", "product_sku", name="uq_lot_sequence_scope"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    iso_year: Mapped[int] = mapped_column(Integer, nullable=False)
    iso_week: Mapped[int] = mapped_column(Integer, nullable=False)
    product_sku: Mapped[str] = mapped_column(String(100), nullable=False)
    next_value: Mapped[int] = mapped_column(Integer, nullable=False)


class LoyverseCustomerMapping(Base):
    __tablename__ = "loyverse_customer_mappings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    loyverse_customer_id: Mapped[str] = mapped_column(String(100), unique=True, index=True, nullable=False)
    customer_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    phone: Mapped[str | None] = mapped_column(String(100), index=True, nullable=True)
    email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    last_refreshed_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class LoyverseVariantMapping(Base):
    __tablename__ = "loyverse_variant_mappings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    sku: Mapped[str | None] = mapped_column(String(100), index=True, nullable=True)
    loyverse_variant_id: Mapped[str] = mapped_column(String(100), unique=True, index=True, nullable=False)
    loyverse_item_id: Mapped[str | None] = mapped_column(String(100), index=True, nullable=True)
    item_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    variant_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    last_refreshed_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class LoyversePaymentTypeMapping(Base):
    __tablename__ = "loyverse_payment_type_mappings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    loyverse_payment_type_id: Mapped[str] = mapped_column(String(100), unique=True, index=True, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    payment_type: Mapped[str | None] = mapped_column(String(100), nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    last_refreshed_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class B2BCustomer(Base):
    __tablename__ = "b2b_customers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    customer_name: Mapped[str] = mapped_column(String(255), nullable=False)
    address: Mapped[str | None] = mapped_column(String(500), nullable=True)
    province: Mapped[str | None] = mapped_column(String(100), nullable=True)
    canton: Mapped[str | None] = mapped_column(String(100), nullable=True)
    district: Mapped[str | None] = mapped_column(String(100), nullable=True)
    legal_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    legal_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    phone: Mapped[str | None] = mapped_column(String(100), nullable=True)
    loyverse_customer_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    products: Mapped[list["B2BCustomerProduct"]] = relationship(
        back_populates="customer",
        cascade="all, delete-orphan",
    )
    orders: Mapped[list["B2BSalesOrder"]] = relationship(back_populates="customer")


class B2CCustomer(Base):
    __tablename__ = "b2c_customers"
    __table_args__ = (
        UniqueConstraint("source_customer_mapping_id", name="uq_b2c_customers_source_customer_mapping_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    phone: Mapped[str | None] = mapped_column(String(100), nullable=True)
    email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    address: Mapped[str | None] = mapped_column(String(500), nullable=True)
    province: Mapped[str | None] = mapped_column(String(100), nullable=True)
    canton: Mapped[str | None] = mapped_column(String(100), nullable=True)
    district: Mapped[str | None] = mapped_column(String(100), nullable=True)
    observations: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_customer_mapping_id: Mapped[int | None] = mapped_column(
        ForeignKey("loyverse_customer_mappings.id"),
        nullable=True,
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    source_customer_mapping: Mapped["LoyverseCustomerMapping | None"] = relationship(
        foreign_keys=[source_customer_mapping_id]
    )
    orders: Mapped[list["B2CSalesOrder"]] = relationship(back_populates="customer")


class B2BCustomerProduct(Base):
    __tablename__ = "b2b_customer_products"
    __table_args__ = (UniqueConstraint("customer_id", "sku", name="uq_b2b_customer_product_sku"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    customer_id: Mapped[int] = mapped_column(ForeignKey("b2b_customers.id"), nullable=False)
    sku: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[str] = mapped_column(String(255), nullable=False)
    distributor_price: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    customer: Mapped[B2BCustomer] = relationship(back_populates="products")


class B2BSalesOrder(Base):
    __tablename__ = "b2b_sales_orders"
    __table_args__ = (
        CheckConstraint(
            "status IN ('draft', 'in_process', 'invoiced')",
            name="ck_b2b_sales_orders_status",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    order_number: Mapped[str] = mapped_column(String(100), unique=True, index=True, nullable=False)
    customer_id: Mapped[int] = mapped_column(ForeignKey("b2b_customers.id"), nullable=False)
    customer_name_snapshot: Mapped[str] = mapped_column(String(255), nullable=False)
    address_snapshot: Mapped[str | None] = mapped_column(String(500), nullable=True)
    province_snapshot: Mapped[str | None] = mapped_column(String(100), nullable=True)
    canton_snapshot: Mapped[str | None] = mapped_column(String(100), nullable=True)
    district_snapshot: Mapped[str | None] = mapped_column(String(100), nullable=True)
    legal_name_snapshot: Mapped[str | None] = mapped_column(String(255), nullable=True)
    legal_id_snapshot: Mapped[str | None] = mapped_column(String(100), nullable=True)
    phone_snapshot: Mapped[str | None] = mapped_column(String(100), nullable=True)
    loyverse_customer_id_snapshot: Mapped[str | None] = mapped_column(String(100), nullable=True)
    delivery_date: Mapped[date] = mapped_column(Date, nullable=False)
    status: Mapped[str] = mapped_column(String(50), default="draft", nullable=False)
    total_amount: Mapped[Decimal] = mapped_column(Numeric(12, 4), default=Decimal("0"), nullable=False)
    observations: Mapped[str | None] = mapped_column(Text, nullable=True)
    b2b_channel_name_snapshot: Mapped[str | None] = mapped_column(String(255), nullable=True)
    loyverse_payment_type_id_snapshot: Mapped[str | None] = mapped_column(String(100), nullable=True)
    loyverse_receipt_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    loyverse_receipt_number: Mapped[str | None] = mapped_column(String(100), nullable=True)
    loyverse_invoice_sync_status: Mapped[str | None] = mapped_column(String(50), nullable=True)
    loyverse_invoice_sync_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    loyverse_invoice_sync_attempted_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    loyverse_invoice_synced_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    loyverse_invoice_sync_attempt_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    customer: Mapped[B2BCustomer] = relationship(back_populates="orders")
    lines: Mapped[list["B2BSalesOrderLine"]] = relationship(
        back_populates="sales_order",
        cascade="all, delete-orphan",
    )


class B2BSalesOrderLine(Base):
    __tablename__ = "b2b_sales_order_lines"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    sales_order_id: Mapped[int] = mapped_column(ForeignKey("b2b_sales_orders.id"), nullable=False)
    line_number: Mapped[int] = mapped_column(Integer, nullable=False)
    sku_snapshot: Mapped[str] = mapped_column(String(100), nullable=False)
    description_snapshot: Mapped[str] = mapped_column(String(255), nullable=False)
    unit_price_snapshot: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    quantity: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    line_total: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    cost_unit_snapshot: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    cost_total_snapshot: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    gross_margin_amount: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    gross_margin_percent: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    loyverse_variant_id_snapshot: Mapped[str | None] = mapped_column(String(100), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    sales_order: Mapped[B2BSalesOrder] = relationship(back_populates="lines")


class B2CSalesOrder(Base):
    __tablename__ = "b2c_sales_orders"
    __table_args__ = (
        CheckConstraint(
            "channel IN ('whatsapp', 'website', 'other')",
            name="ck_b2c_sales_orders_channel",
        ),
        CheckConstraint(
            "status IN ('draft', 'invoiced', 'cancelled')",
            name="ck_b2c_sales_orders_status",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    order_number: Mapped[str] = mapped_column(String(100), unique=True, index=True, nullable=False)
    order_date: Mapped[date] = mapped_column(Date, nullable=False)
    b2c_customer_id: Mapped[int | None] = mapped_column(ForeignKey("b2c_customers.id"), nullable=True)
    customer_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    customer_phone: Mapped[str | None] = mapped_column(String(100), nullable=True)
    customer_email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    customer_address_snapshot: Mapped[str | None] = mapped_column(String(500), nullable=True)
    province_snapshot: Mapped[str | None] = mapped_column(String(100), nullable=True)
    canton_snapshot: Mapped[str | None] = mapped_column(String(100), nullable=True)
    district_snapshot: Mapped[str | None] = mapped_column(String(100), nullable=True)
    customer_observations_snapshot: Mapped[str | None] = mapped_column(Text, nullable=True)
    channel: Mapped[str] = mapped_column(String(50), nullable=False)
    status: Mapped[str] = mapped_column(String(50), default="draft", nullable=False)
    discount_rule_id: Mapped[int | None] = mapped_column(ForeignKey("discount_rules.id"), nullable=True)
    discount_name_snapshot: Mapped[str | None] = mapped_column(String(255), nullable=True)
    discount_type_snapshot: Mapped[str | None] = mapped_column(String(50), nullable=True)
    discount_value_snapshot: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    subtotal_amount: Mapped[Decimal] = mapped_column(Numeric(12, 4), default=Decimal("0"), nullable=False)
    discount_amount: Mapped[Decimal] = mapped_column(Numeric(12, 4), default=Decimal("0"), nullable=False)
    total_amount: Mapped[Decimal] = mapped_column(Numeric(12, 4), default=Decimal("0"), nullable=False)
    cost_total_snapshot: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    gross_margin_amount: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    gross_margin_percent: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    observations: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    customer: Mapped["B2CCustomer | None"] = relationship(back_populates="orders")
    discount_rule: Mapped["DiscountRule | None"] = relationship("DiscountRule", back_populates="b2c_orders")
    lines: Mapped[list["B2CSalesOrderLine"]] = relationship(
        back_populates="sales_order",
        cascade="all, delete-orphan",
    )


class B2CSalesOrderLine(Base):
    __tablename__ = "b2c_sales_order_lines"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    sales_order_id: Mapped[int] = mapped_column(ForeignKey("b2c_sales_orders.id"), nullable=False)
    line_number: Mapped[int] = mapped_column(Integer, nullable=False)
    sku_snapshot: Mapped[str] = mapped_column(String(100), nullable=False)
    description_snapshot: Mapped[str] = mapped_column(String(255), nullable=False)
    quantity: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    unit_price_snapshot: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    line_total: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    discount_amount_snapshot: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    net_line_total_snapshot: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    cost_unit_snapshot: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    cost_total_snapshot: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    gross_margin_amount: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    gross_margin_percent: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    sales_order: Mapped[B2CSalesOrder] = relationship(back_populates="lines")


class PurchaseOrder(Base):
    __tablename__ = "purchase_orders"
    __table_args__ = (
        CheckConstraint(
            "status IN ('draft', 'issued', 'incomplete', 'closed')",
            name="ck_purchase_orders_status",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    po_number: Mapped[str] = mapped_column(String(100), unique=True, index=True, nullable=False)
    supplier_name_snapshot: Mapped[str] = mapped_column(String(255), nullable=False)
    po_date: Mapped[date] = mapped_column(Date, nullable=False)
    status: Mapped[str] = mapped_column(String(50), default="draft", nullable=False)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    estimated_total: Mapped[Decimal] = mapped_column(Numeric(12, 4), default=Decimal("0"), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    lines: Mapped[list["PurchaseOrderLine"]] = relationship(
        back_populates="purchase_order",
        cascade="all, delete-orphan",
    )


class PurchaseOrderLine(Base):
    __tablename__ = "purchase_order_lines"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    purchase_order_id: Mapped[int] = mapped_column(ForeignKey("purchase_orders.id"), nullable=False)
    line_number: Mapped[int] = mapped_column(Integer, nullable=False)
    sku_snapshot: Mapped[str] = mapped_column(String(100), nullable=False)
    description_snapshot: Mapped[str] = mapped_column(String(255), nullable=False)
    supplier_name_snapshot: Mapped[str] = mapped_column(String(255), nullable=False)
    quantity: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    received_quantity: Mapped[Decimal] = mapped_column(Numeric(12, 4), default=Decimal("0"), nullable=False)
    unit_cost_snapshot: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    line_total: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    purchase_order: Mapped[PurchaseOrder] = relationship(back_populates="lines")


class PurchaseOrderReceiveToken(Base):
    __tablename__ = "purchase_order_receive_tokens"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    purchase_order_id: Mapped[int] = mapped_column(ForeignKey("purchase_orders.id"), nullable=False, index=True)
    token: Mapped[str] = mapped_column(String(255), unique=True, index=True, nullable=False)
    used_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    purchase_order: Mapped[PurchaseOrder] = relationship()


class InventoryAdjustmentPostToken(Base):
    __tablename__ = "inventory_adjustment_post_tokens"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    token: Mapped[str] = mapped_column(String(255), unique=True, index=True, nullable=False)
    used_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class InventoryTransaction(Base):
    __tablename__ = "inventory_transactions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"), nullable=False, index=True)
    transaction_date: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    transaction_type: Mapped[str] = mapped_column(String(50), nullable=False)
    source_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    source_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    source_line_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    quantity_in: Mapped[Decimal] = mapped_column(Numeric(12, 4), default=Decimal("0"), nullable=False)
    quantity_out: Mapped[Decimal] = mapped_column(Numeric(12, 4), default=Decimal("0"), nullable=False)
    unit_cost: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    total_cost: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    running_quantity: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    running_average_cost: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    running_inventory_value: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    product: Mapped[Product] = relationship()


class InventoryBalance(Base):
    __tablename__ = "inventory_balances"
    __table_args__ = (
        UniqueConstraint("product_id", name="uq_inventory_balances_product_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"), nullable=False, index=True)
    on_hand_qty: Mapped[Decimal] = mapped_column(Numeric(12, 4), default=Decimal("0"), nullable=False)
    average_unit_cost: Mapped[Decimal] = mapped_column(Numeric(12, 4), default=Decimal("0"), nullable=False)
    inventory_value: Mapped[Decimal] = mapped_column(Numeric(12, 4), default=Decimal("0"), nullable=False)
    last_transaction_id: Mapped[int | None] = mapped_column(ForeignKey("inventory_transactions.id"), nullable=True)
    last_transaction_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    product: Mapped[Product] = relationship(foreign_keys=[product_id])
    last_transaction: Mapped[InventoryTransaction | None] = relationship(foreign_keys=[last_transaction_id])


class InventoryAdjustment(Base):
    __tablename__ = "inventory_adjustments"
    __table_args__ = (
        CheckConstraint(
            "adjustment_mode IN ('quantity_adjustment', 'stock_count')",
            name="ck_inventory_adjustments_mode",
        ),
        CheckConstraint(
            "adjustment_type IN ('increase', 'decrease')",
            name="ck_inventory_adjustments_type",
        ),
        CheckConstraint(
            "status IN ('posted')",
            name="ck_inventory_adjustments_status",
        ),
        CheckConstraint(
            "reason IN ('physical_count', 'damage', 'waste', 'correction', 'other')",
            name="ck_inventory_adjustments_reason",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    adjustment_number: Mapped[str] = mapped_column(String(100), unique=True, index=True, nullable=False)
    adjustment_date: Mapped[date] = mapped_column(Date, nullable=False)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"), nullable=False, index=True)
    sku_snapshot: Mapped[str] = mapped_column(String(100), nullable=False)
    product_name_snapshot: Mapped[str] = mapped_column(String(255), nullable=False)
    adjustment_mode: Mapped[str] = mapped_column(String(50), nullable=False)
    adjustment_type: Mapped[str] = mapped_column(String(50), nullable=False)
    transaction_type: Mapped[str] = mapped_column(String(50), nullable=False)
    reason: Mapped[str] = mapped_column(String(50), nullable=False)
    current_qty_snapshot: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    counted_qty: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    quantity_adjustment: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    unit_cost: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    total_cost: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    warning_notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(50), default="posted", nullable=False)
    inventory_transaction_id: Mapped[int | None] = mapped_column(
        ForeignKey("inventory_transactions.id"),
        nullable=True,
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    product: Mapped[Product] = relationship(foreign_keys=[product_id])
    inventory_transaction: Mapped[InventoryTransaction | None] = relationship(
        foreign_keys=[inventory_transaction_id]
    )


class ProductBomHeader(Base):
    __tablename__ = "product_bom_headers"
    __table_args__ = (UniqueConstraint("product_id", name="uq_product_bom_headers_product_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"), nullable=False, index=True)
    name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    source_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    source_imported_bom_header_id: Mapped[int | None] = mapped_column(
        ForeignKey("imported_bom_headers.id"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    product: Mapped[Product] = relationship("Product", back_populates="bom_header")
    source_imported_bom_header: Mapped["ImportedBomHeader | None"] = relationship(
        "ImportedBomHeader",
        foreign_keys=[source_imported_bom_header_id],
    )
    lines: Mapped[list["ProductBomLine"]] = relationship(
        back_populates="bom_header",
        cascade="all, delete-orphan",
    )


class ProductBomLine(Base):
    __tablename__ = "product_bom_lines"
    __table_args__ = (UniqueConstraint("bom_header_id", "line_number", name="uq_product_bom_lines_line_number"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    bom_header_id: Mapped[int] = mapped_column(ForeignKey("product_bom_headers.id"), nullable=False, index=True)
    component_product_id: Mapped[int | None] = mapped_column(ForeignKey("products.id"), nullable=True, index=True)
    component_sku_snapshot: Mapped[str | None] = mapped_column(String(100), nullable=True)
    component_name_snapshot: Mapped[str | None] = mapped_column(String(255), nullable=True)
    unit_snapshot: Mapped[str | None] = mapped_column(String(50), nullable=True)
    quantity_standard: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    line_number: Mapped[int] = mapped_column(Integer, nullable=False)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_imported_bom_line_id: Mapped[int | None] = mapped_column(
        ForeignKey("imported_bom_lines.id"),
        nullable=True,
    )
    component_type: Mapped[str] = mapped_column(String(50), default="material", nullable=False)
    include_in_real_cost: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    bom_header: Mapped[ProductBomHeader] = relationship("ProductBomHeader", back_populates="lines")
    component_product: Mapped[Product | None] = relationship("Product", foreign_keys=[component_product_id])
    source_imported_bom_line: Mapped["ImportedBomLine | None"] = relationship(
        "ImportedBomLine",
        foreign_keys=[source_imported_bom_line_id],
    )


class ImportBatch(Base):
    __tablename__ = "import_batches"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    file_name: Mapped[str] = mapped_column(String(255), nullable=False)
    imported_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    product_master_upsert_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
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
    category_name_snapshot: Mapped[str | None] = mapped_column(String(255), nullable=True)
    b2b_price_snapshot: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
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


class ProductionOrder(Base):
    __tablename__ = "production_orders"
    __table_args__ = (
        CheckConstraint(
            "status IN ('draft', 'in_progress', 'closed')",
            name="ck_production_orders_status",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    internal_order_number: Mapped[str] = mapped_column(String(100), unique=True, index=True, nullable=False)
    loyverse_order_ref: Mapped[str | None] = mapped_column(String(100), nullable=True)
    production_date: Mapped[date] = mapped_column(Date, nullable=False)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"), nullable=False)
    product_sku_snapshot: Mapped[str] = mapped_column(String(100), nullable=False)
    product_name_snapshot: Mapped[str] = mapped_column(String(255), nullable=False)
    route_id: Mapped[int] = mapped_column(ForeignKey("routes.id"), nullable=False)
    route_name_snapshot: Mapped[str] = mapped_column(String(255), nullable=False)
    route_version_snapshot: Mapped[str] = mapped_column(String(50), nullable=False)
    process_type: Mapped[str] = mapped_column(String(50), nullable=False)
    planned_qty: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    input_qty: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    output_qty: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    unit: Mapped[str | None] = mapped_column(String(50), nullable=True)
    yield_percent: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    material_snapshot_cost_total: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    real_labor_cost_total: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    real_overhead_cost_total: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    real_machine_cost_total: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    real_total_cost: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    real_unit_cost: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    variance_amount: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    variance_percent: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    lot_number: Mapped[str | None] = mapped_column(String(50), unique=True, index=True, nullable=True)
    loyverse_cost_sync_status: Mapped[str | None] = mapped_column(String(50), nullable=True)
    loyverse_cost_sync_attempted_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    loyverse_cost_sync_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    loyverse_cost_sync_variant_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    loyverse_cost_sync_pushed_cost: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    loyverse_inventory_sync_status: Mapped[str | None] = mapped_column(String(50), nullable=True)
    loyverse_inventory_sync_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    loyverse_inventory_sync_attempted_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    loyverse_inventory_synced_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    loyverse_inventory_sync_attempt_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    loyverse_inventory_store_id_snapshot: Mapped[str | None] = mapped_column(String(100), nullable=True)
    loyverse_inventory_response_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    loyverse_inventory_request_fingerprint: Mapped[str | None] = mapped_column(String(128), nullable=True)
    loyverse_inventory_payload_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(50), default="draft", nullable=False)
    notes: Mapped[str | None] = mapped_column(String(500), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    product: Mapped[Product] = relationship()
    route: Mapped[Route] = relationship()
    materials: Mapped[list["ProductionOrderMaterial"]] = relationship(
        back_populates="production_order",
        cascade="all, delete-orphan",
    )
    activities: Mapped[list["ProductionOrderActivity"]] = relationship(
        back_populates="production_order",
        cascade="all, delete-orphan",
    )


class ProductionOrderMaterial(Base):
    __tablename__ = "production_order_materials"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    production_order_id: Mapped[int] = mapped_column(ForeignKey("production_orders.id"), nullable=False)
    component_sku: Mapped[str | None] = mapped_column(String(100), nullable=True)
    component_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    quantity_standard: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    required_quantity: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    unit_cost_snapshot: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    line_cost: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    component_type: Mapped[str] = mapped_column(String(50), nullable=False)
    include_in_real_cost: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    production_order: Mapped[ProductionOrder] = relationship(back_populates="materials")


class ProductionOrderActivity(Base):
    __tablename__ = "production_order_activities"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    production_order_id: Mapped[int] = mapped_column(ForeignKey("production_orders.id"), nullable=False)
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    activity_code_snapshot: Mapped[str] = mapped_column(String(50), nullable=False)
    activity_name_snapshot: Mapped[str] = mapped_column(String(255), nullable=False)
    machine_id_snapshot: Mapped[int | None] = mapped_column(Integer, nullable=True)
    machine_code_snapshot: Mapped[str | None] = mapped_column(String(50), nullable=True)
    machine_name_snapshot: Mapped[str | None] = mapped_column(String(255), nullable=True)
    labor_minutes: Mapped[Decimal] = mapped_column(Numeric(12, 4), default=Decimal("0"), nullable=False)
    machine_minutes: Mapped[Decimal] = mapped_column(Numeric(12, 4), default=Decimal("0"), nullable=False)
    labor_rate_snapshot: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    overhead_rate_snapshot: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    machine_rate_snapshot: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    labor_cost: Mapped[Decimal] = mapped_column(Numeric(12, 4), default=Decimal("0"), nullable=False)
    overhead_cost: Mapped[Decimal] = mapped_column(Numeric(12, 4), default=Decimal("0"), nullable=False)
    machine_cost: Mapped[Decimal] = mapped_column(Numeric(12, 4), default=Decimal("0"), nullable=False)
    total_activity_cost: Mapped[Decimal] = mapped_column(Numeric(12, 4), default=Decimal("0"), nullable=False)
    notes: Mapped[str | None] = mapped_column(String(500), nullable=True)

    production_order: Mapped[ProductionOrder] = relationship(back_populates="activities")
