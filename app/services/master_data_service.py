from datetime import datetime
from decimal import Decimal, InvalidOperation

from sqlalchemy import func
from sqlalchemy.orm import Session, joinedload

from app.models import DiscountRule, InventoryBalance, Product, ProductCategory, Supplier


ZERO = Decimal("0")
ONE = Decimal("1")
DISCOUNT_TYPE_PERCENTAGE = "percentage"
DISCOUNT_APPLIES_TO_ORDER_TOTAL = "order_total"
DISCOUNT_CHANNEL_B2C = "b2c"


class MasterDataValidationError(Exception):
    pass


def create_product_category(
    db: Session,
    *,
    name: str,
    description: str,
    active: bool,
) -> ProductCategory:
    category = ProductCategory()
    _assign_category_fields(db, category, name=name, description=description, active=active)
    db.add(category)
    db.commit()
    db.refresh(category)
    return category


def update_product_category(
    db: Session,
    *,
    category_id: int,
    name: str,
    description: str,
    active: bool,
) -> ProductCategory:
    category = db.query(ProductCategory).filter(ProductCategory.id == category_id).one()
    _assign_category_fields(db, category, name=name, description=description, active=active)
    db.commit()
    db.refresh(category)
    return category


def create_supplier(
    db: Session,
    *,
    name: str,
    contact_name: str,
    phone: str,
    email: str,
    notes: str,
    active: bool,
) -> Supplier:
    supplier = Supplier()
    _assign_supplier_fields(
        db,
        supplier,
        name=name,
        contact_name=contact_name,
        phone=phone,
        email=email,
        notes=notes,
        active=active,
    )
    db.add(supplier)
    db.commit()
    db.refresh(supplier)
    return supplier


def update_supplier(
    db: Session,
    *,
    supplier_id: int,
    name: str,
    contact_name: str,
    phone: str,
    email: str,
    notes: str,
    active: bool,
) -> Supplier:
    supplier = db.query(Supplier).filter(Supplier.id == supplier_id).one()
    _assign_supplier_fields(
        db,
        supplier,
        name=name,
        contact_name=contact_name,
        phone=phone,
        email=email,
        notes=notes,
        active=active,
    )
    db.commit()
    db.refresh(supplier)
    return supplier


def create_discount_rule(
    db: Session,
    *,
    name: str,
    discount_type: str,
    value: str,
    applies_to: str,
    channel: str,
    active: bool,
    description: str,
) -> DiscountRule:
    discount_rule = DiscountRule()
    _assign_discount_rule_fields(
        db,
        discount_rule,
        name=name,
        discount_type=discount_type,
        value=value,
        applies_to=applies_to,
        channel=channel,
        active=active,
        description=description,
    )
    db.add(discount_rule)
    db.commit()
    db.refresh(discount_rule)
    return discount_rule


def update_discount_rule(
    db: Session,
    *,
    discount_rule_id: int,
    name: str,
    discount_type: str,
    value: str,
    applies_to: str,
    channel: str,
    active: bool,
    description: str,
) -> DiscountRule:
    discount_rule = db.query(DiscountRule).filter(DiscountRule.id == discount_rule_id).one()
    _assign_discount_rule_fields(
        db,
        discount_rule,
        name=name,
        discount_type=discount_type,
        value=value,
        applies_to=applies_to,
        channel=channel,
        active=active,
        description=description,
    )
    db.commit()
    db.refresh(discount_rule)
    return discount_rule


def create_product_master(
    db: Session,
    *,
    sku: str,
    name: str,
    unit: str,
    category_id: str,
    supplier_id: str,
    description: str,
    observations: str,
    b2c_price: str,
    b2b_price: str,
    standard_cost: str,
    active: bool,
    available_for_sale_gc: bool,
    is_manufactured: bool,
    is_purchased_product: bool,
) -> Product:
    product = Product(created_at=datetime.utcnow(), updated_at=datetime.utcnow())
    _assign_product_fields(
        db,
        product,
        sku=sku,
        name=name,
        unit=unit,
        category_id=category_id,
        supplier_id=supplier_id,
        description=description,
        observations=observations,
        b2c_price=b2c_price,
        b2b_price=b2b_price,
        standard_cost=standard_cost,
        active=active,
        available_for_sale_gc=available_for_sale_gc,
        is_manufactured=is_manufactured,
        is_purchased_product=is_purchased_product,
    )
    db.add(product)
    db.commit()
    db.refresh(product)
    return product


def update_product_master(
    db: Session,
    *,
    product_id: int,
    sku: str,
    name: str,
    unit: str,
    category_id: str,
    supplier_id: str,
    description: str,
    observations: str,
    b2c_price: str,
    b2b_price: str,
    standard_cost: str,
    active: bool,
    available_for_sale_gc: bool,
    is_manufactured: bool,
    is_purchased_product: bool,
) -> Product:
    product = db.query(Product).filter(Product.id == product_id).one()
    _assign_product_fields(
        db,
        product,
        sku=sku,
        name=name,
        unit=unit,
        category_id=category_id,
        supplier_id=supplier_id,
        description=description,
        observations=observations,
        b2c_price=b2c_price,
        b2b_price=b2b_price,
        standard_cost=standard_cost,
        active=active,
        available_for_sale_gc=available_for_sale_gc,
        is_manufactured=is_manufactured,
        is_purchased_product=is_purchased_product,
    )
    db.commit()
    db.refresh(product)
    return product


def list_category_options(db: Session, current_category_id: int | None = None) -> list[ProductCategory]:
    query = db.query(ProductCategory)
    if current_category_id is None:
        query = query.filter(ProductCategory.active.is_(True))
    else:
        query = query.filter(
            (ProductCategory.active.is_(True)) | (ProductCategory.id == current_category_id)
        )
    return query.order_by(ProductCategory.name, ProductCategory.id).all()


def list_supplier_options(db: Session, current_supplier_id: int | None = None) -> list[Supplier]:
    query = db.query(Supplier)
    if current_supplier_id is None:
        query = query.filter(Supplier.active.is_(True))
    else:
        query = query.filter((Supplier.active.is_(True)) | (Supplier.id == current_supplier_id))
    return query.order_by(Supplier.name, Supplier.id).all()


def list_discount_rule_options(
    db: Session,
    current_discount_rule_id: int | None = None,
    *,
    channel: str = DISCOUNT_CHANNEL_B2C,
    applies_to: str = DISCOUNT_APPLIES_TO_ORDER_TOTAL,
) -> list[DiscountRule]:
    query = db.query(DiscountRule).filter(
        DiscountRule.channel == channel,
        DiscountRule.applies_to == applies_to,
    )
    if current_discount_rule_id is None:
        query = query.filter(DiscountRule.active.is_(True))
    else:
        query = query.filter(
            (DiscountRule.active.is_(True)) | (DiscountRule.id == current_discount_rule_id)
        )
    return query.order_by(DiscountRule.name, DiscountRule.id).all()


def get_product_for_detail(db: Session, product_id: int) -> Product:
    return (
        db.query(Product)
        .options(joinedload(Product.category), joinedload(Product.supplier_record))
        .filter(Product.id == product_id)
        .one()
    )


def get_product_balance(db: Session, product_id: int) -> InventoryBalance | None:
    return db.query(InventoryBalance).filter(InventoryBalance.product_id == product_id).one_or_none()


def _assign_category_fields(
    db: Session,
    category: ProductCategory,
    *,
    name: str,
    description: str,
    active: bool,
) -> None:
    normalized_name = _required_text(name, "Category name")
    existing = (
        db.query(ProductCategory.id)
        .filter(func.lower(ProductCategory.name) == normalized_name.lower())
        .first()
    )
    if existing is not None and existing[0] != category.id:
        raise MasterDataValidationError(f"Category '{normalized_name}' already exists.")
    category.name = normalized_name
    category.description = _optional_text(description)
    category.active = active


def _assign_supplier_fields(
    db: Session,
    supplier: Supplier,
    *,
    name: str,
    contact_name: str,
    phone: str,
    email: str,
    notes: str,
    active: bool,
) -> None:
    normalized_name = _required_text(name, "Supplier name")
    existing = db.query(Supplier.id).filter(func.lower(Supplier.name) == normalized_name.lower()).first()
    if existing is not None and existing[0] != supplier.id:
        raise MasterDataValidationError(f"Supplier '{normalized_name}' already exists.")
    supplier.name = normalized_name
    supplier.contact_name = _optional_text(contact_name)
    supplier.phone = _optional_text(phone)
    supplier.email = _optional_text(email)
    supplier.notes = _optional_text(notes)
    supplier.active = active


def _assign_discount_rule_fields(
    db: Session,
    discount_rule: DiscountRule,
    *,
    name: str,
    discount_type: str,
    value: str,
    applies_to: str,
    channel: str,
    active: bool,
    description: str,
) -> None:
    normalized_name = _required_text(name, "Discount name")
    existing = db.query(DiscountRule.id).filter(func.lower(DiscountRule.name) == normalized_name.lower()).first()
    if existing is not None and existing[0] != discount_rule.id:
        raise MasterDataValidationError(f"Discount '{normalized_name}' already exists.")

    normalized_type = _normalize_discount_type(discount_type)
    normalized_applies_to = _normalize_discount_applies_to(applies_to)
    normalized_channel = _normalize_discount_channel(channel)
    parsed_value = _parse_nonnegative_decimal(value, "Discount value")
    if parsed_value is None:
        raise MasterDataValidationError("Discount value is required.")
    if normalized_type == DISCOUNT_TYPE_PERCENTAGE and parsed_value > ONE:
        raise MasterDataValidationError("Percentage discount value cannot exceed 1.")

    discount_rule.name = normalized_name
    discount_rule.discount_type = normalized_type
    discount_rule.value = parsed_value
    discount_rule.applies_to = normalized_applies_to
    discount_rule.channel = normalized_channel
    discount_rule.active = active
    discount_rule.description = _optional_text(description)


def _assign_product_fields(
    db: Session,
    product: Product,
    *,
    sku: str,
    name: str,
    unit: str,
    category_id: str,
    supplier_id: str,
    description: str,
    observations: str,
    b2c_price: str,
    b2b_price: str,
    standard_cost: str,
    active: bool,
    available_for_sale_gc: bool,
    is_manufactured: bool,
    is_purchased_product: bool,
) -> None:
    normalized_sku = _required_text(sku, "SKU")
    normalized_name = _required_text(name, "Name")
    existing = db.query(Product.id).filter(func.lower(Product.sku) == normalized_sku.lower()).first()
    if existing is not None and existing[0] != product.id:
        raise MasterDataValidationError(f"SKU '{normalized_sku}' already exists.")

    selected_category = _resolve_category(db, category_id)
    selected_supplier = _resolve_supplier(db, supplier_id)

    product.sku = normalized_sku
    product.name = normalized_name
    product.unit = _optional_text(unit)
    product.category_id = selected_category.id if selected_category is not None else None
    product.supplier_id = selected_supplier.id if selected_supplier is not None else None
    product.description = _optional_text(description)
    product.observations = _optional_text(observations)
    product.b2c_price = _parse_nonnegative_decimal(b2c_price, "B2C price")
    product.b2b_price = _parse_nonnegative_decimal(b2b_price, "B2B price")
    product.standard_cost = _parse_nonnegative_decimal(standard_cost, "Standard cost")
    product.active = active
    product.available_for_sale_gc = available_for_sale_gc
    product.is_manufactured = is_manufactured
    product.is_purchased_product = is_purchased_product
    product.supplier = selected_supplier.name if selected_supplier is not None else None


def _resolve_category(db: Session, raw_category_id: str) -> ProductCategory | None:
    category_id = _parse_optional_int(raw_category_id, "Category")
    if category_id is None:
        return None
    category = db.query(ProductCategory).filter(ProductCategory.id == category_id).one_or_none()
    if category is None:
        raise MasterDataValidationError("Selected category does not exist.")
    return category


def _resolve_supplier(db: Session, raw_supplier_id: str) -> Supplier | None:
    supplier_id = _parse_optional_int(raw_supplier_id, "Supplier")
    if supplier_id is None:
        return None
    supplier = db.query(Supplier).filter(Supplier.id == supplier_id).one_or_none()
    if supplier is None:
        raise MasterDataValidationError("Selected supplier does not exist.")
    return supplier


def _normalize_discount_type(value: str) -> str:
    normalized = (value or "").strip().lower()
    if normalized != DISCOUNT_TYPE_PERCENTAGE:
        raise MasterDataValidationError("Discount type is invalid.")
    return normalized


def _normalize_discount_applies_to(value: str) -> str:
    normalized = (value or "").strip().lower()
    if normalized != DISCOUNT_APPLIES_TO_ORDER_TOTAL:
        raise MasterDataValidationError("Discount applies_to is invalid.")
    return normalized


def _normalize_discount_channel(value: str) -> str:
    normalized = (value or "").strip().lower()
    if normalized != DISCOUNT_CHANNEL_B2C:
        raise MasterDataValidationError("Discount channel is invalid.")
    return normalized


def _parse_optional_int(value: str | int | None, field_name: str) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    if not text.isdigit():
        raise MasterDataValidationError(f"{field_name} is invalid.")
    return int(text)


def _parse_nonnegative_decimal(value: str | Decimal | None, field_name: str) -> Decimal | None:
    if value is None or value == "":
        return None
    if isinstance(value, Decimal):
        parsed = value
    else:
        text = str(value).strip().replace(" ", "").replace(",", ".")
        try:
            parsed = Decimal(text)
        except InvalidOperation as exc:
            raise MasterDataValidationError(f"{field_name} must be a valid number.") from exc
    if parsed < ZERO:
        raise MasterDataValidationError(f"{field_name} cannot be negative.")
    return parsed


def _required_text(value: str, field_name: str) -> str:
    cleaned = (value or "").strip()
    if not cleaned:
        raise MasterDataValidationError(f"{field_name} is required.")
    return cleaned


def _optional_text(value: str | None) -> str | None:
    cleaned = (value or "").strip()
    return cleaned or None
