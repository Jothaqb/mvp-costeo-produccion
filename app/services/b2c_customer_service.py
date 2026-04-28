from dataclasses import dataclass

from sqlalchemy.orm import Session

from app.models import B2CCustomer, LoyverseCustomerMapping


class B2CCustomerValidationError(Exception):
    pass


@dataclass(frozen=True)
class B2CCustomerInitializationResult:
    created: int
    skipped: int


def create_b2c_customer(
    db: Session,
    *,
    name: str,
    phone: str,
    email: str,
    address: str,
    province: str,
    canton: str,
    district: str,
    observations: str,
    active: bool,
) -> B2CCustomer:
    customer = B2CCustomer()
    _assign_b2c_customer_fields(
        customer,
        name=name,
        phone=phone,
        email=email,
        address=address,
        province=province,
        canton=canton,
        district=district,
        observations=observations,
        active=active,
    )
    db.add(customer)
    db.commit()
    db.refresh(customer)
    return customer


def update_b2c_customer(
    db: Session,
    *,
    customer_id: int,
    name: str,
    phone: str,
    email: str,
    address: str,
    province: str,
    canton: str,
    district: str,
    observations: str,
    active: bool,
) -> B2CCustomer:
    customer = db.query(B2CCustomer).filter(B2CCustomer.id == customer_id).one()
    _assign_b2c_customer_fields(
        customer,
        name=name,
        phone=phone,
        email=email,
        address=address,
        province=province,
        canton=canton,
        district=district,
        observations=observations,
        active=active,
    )
    db.commit()
    db.refresh(customer)
    return customer


def initialize_b2c_customers_from_mappings(db: Session) -> B2CCustomerInitializationResult:
    created = 0
    skipped = 0
    mappings = (
        db.query(LoyverseCustomerMapping)
        .order_by(LoyverseCustomerMapping.active.desc(), LoyverseCustomerMapping.customer_name, LoyverseCustomerMapping.id)
        .all()
    )
    for mapping in mappings:
        existing = (
            db.query(B2CCustomer.id)
            .filter(B2CCustomer.source_customer_mapping_id == mapping.id)
            .first()
        )
        if existing is not None:
            skipped += 1
            continue

        name = _clean_optional_text(mapping.customer_name)
        if not name:
            skipped += 1
            continue

        db.add(
            B2CCustomer(
                active=mapping.active,
                name=name,
                phone=_clean_optional_text(mapping.phone),
                email=_clean_optional_text(mapping.email),
                source_customer_mapping_id=mapping.id,
            )
        )
        created += 1

    db.commit()
    return B2CCustomerInitializationResult(created=created, skipped=skipped)


def list_b2c_customer_options(db: Session, current_customer_id: int | None = None) -> list[B2CCustomer]:
    query = db.query(B2CCustomer)
    if current_customer_id is None:
        query = query.filter(B2CCustomer.active.is_(True))
    else:
        query = query.filter((B2CCustomer.active.is_(True)) | (B2CCustomer.id == current_customer_id))
    return query.order_by(B2CCustomer.name, B2CCustomer.id).all()


def _assign_b2c_customer_fields(
    customer: B2CCustomer,
    *,
    name: str,
    phone: str,
    email: str,
    address: str,
    province: str,
    canton: str,
    district: str,
    observations: str,
    active: bool,
) -> None:
    normalized_name = (name or "").strip()
    if not normalized_name:
        raise B2CCustomerValidationError("Customer name is required.")

    customer.name = normalized_name
    customer.phone = _clean_optional_text(phone)
    customer.email = _clean_optional_text(email)
    customer.address = _clean_optional_text(address)
    customer.province = _clean_optional_text(province)
    customer.canton = _clean_optional_text(canton)
    customer.district = _clean_optional_text(district)
    customer.observations = _clean_optional_text(observations)
    customer.active = active


def _clean_optional_text(value: str | None) -> str | None:
    cleaned = (value or "").strip()
    return cleaned or None
