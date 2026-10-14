from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy.orm import Session, joinedload, selectinload

from app.models import (
    B2BCommercialAgreement,
    B2BCommercialAgreementPerson,
    B2BCommercialAgreementSection,
    B2BCustomer,
)


AGREEMENT_STATUSES = {"draft", "generated", "signed", "archived"}
AUTHORIZATION_TYPES = {"orders", "receiving"}
DEFAULT_CATALOG_REFERENCE = (
    "Catálogo o lista de precios aplicable: Ver catálogo comercial vigente adjunto."
)
DEFAULT_TEMPLATE_REVISION = "b2b-direct-purchase-v0"
DEFAULT_AGREEMENT_SECTIONS = (
    ("prices", "Precios"),
    ("b2b_discount_and_volume", "Descuento comercial B2B y volumen de compra"),
    ("advance_payment_discount", "Descuento adicional por pago anticipado"),
    ("minimum_order_and_delivery_cost", "Pedido mínimo y costo de entrega"),
    ("order_process", "Realización, recepción y confirmación de pedidos"),
    ("preparation_and_delivery", "Alisto y condiciones de entrega"),
    ("commercial_credit", "Crédito comercial"),
    ("overdue_invoices", "Facturas vencidas y suspensión de despachos"),
    ("late_payment_interest", "Intereses por mora"),
    ("credit_benefit_changes", "Pérdida o modificación del beneficio de crédito"),
    ("goods_receipt", "Recepción y revisión de mercadería"),
    ("returns_and_claims", "Devoluciones y reclamos"),
    ("post_delivery_liability", "Responsabilidad después de la entrega"),
    ("customer_information_changes", "Cambios en la información o persona jurídica del cliente"),
    ("commercial_relationship_termination", "Terminación de la relación comercial"),
    ("communications", "Comunicaciones"),
    ("term", "Vigencia"),
    ("applicable_law", "Legislación aplicable"),
    ("acceptance", "Aceptación"),
)


class B2BCommercialAgreementValidationError(Exception):
    pass


def _base_section_text(title: str) -> str:
    return (
        f"Texto base editable para {title.lower()}. "
        "Debe revisarse y sustituirse por la redacción comercial y legal aprobada."
    )


def list_customer_commercial_agreements(
    db: Session,
    customer_id: int,
) -> tuple[B2BCustomer, list[B2BCommercialAgreement]]:
    customer = db.query(B2BCustomer).filter(B2BCustomer.id == customer_id).one()
    agreements = (
        db.query(B2BCommercialAgreement)
        .filter(B2BCommercialAgreement.customer_id == customer_id)
        .order_by(B2BCommercialAgreement.version_number.desc(), B2BCommercialAgreement.id.desc())
        .all()
    )
    return customer, agreements


def get_commercial_agreement(db: Session, agreement_id: int) -> B2BCommercialAgreement:
    return (
        db.query(B2BCommercialAgreement)
        .options(
            joinedload(B2BCommercialAgreement.customer),
            selectinload(B2BCommercialAgreement.authorized_people),
            selectinload(B2BCommercialAgreement.sections),
        )
        .filter(B2BCommercialAgreement.id == agreement_id)
        .one()
    )


def create_commercial_agreement_draft(
    db: Session,
    *,
    customer_id: int,
    user_id: int | None,
) -> B2BCommercialAgreement:
    customer = (
        db.query(B2BCustomer)
        .options(joinedload(B2BCustomer.channel_record))
        .filter(B2BCustomer.id == customer_id)
        .one()
    )
    existing_agreement = (
        db.query(B2BCommercialAgreement.status)
        .filter(B2BCommercialAgreement.customer_id == customer_id)
        .order_by(B2BCommercialAgreement.version_number.desc())
        .first()
    )
    if existing_agreement is not None and existing_agreement[0] == "draft":
        raise B2BCommercialAgreementValidationError(
            "El cliente ya tiene un acuerdo comercial en borrador."
        )
    if existing_agreement is not None:
        raise B2BCommercialAgreementValidationError(
            "La creación de nuevas versiones se implementará en la fase 2."
        )
    agreement = B2BCommercialAgreement(
        customer_id=customer.id,
        version_number=0,
        status="draft",
        customer_name_snapshot=customer.customer_name,
        legal_name_snapshot=customer.legal_name,
        legal_id_snapshot=customer.legal_id,
        phone_snapshot=customer.phone,
        address_snapshot=customer.address,
        province_snapshot=customer.province,
        canton_snapshot=customer.canton,
        district_snapshot=customer.district,
        credit_days_snapshot=customer.credit_days,
        b2b_channel_name_snapshot=(
            customer.channel_record.name if customer.channel_record is not None else None
        ),
        customer_active_snapshot=customer.active,
        billing_address=customer.address,
        whatsapp=customer.phone,
        credit_approved=customer.credit_days > 0,
        contract_credit_days=customer.credit_days if customer.credit_days > 0 else None,
        catalog_reference=DEFAULT_CATALOG_REFERENCE,
        template_revision=DEFAULT_TEMPLATE_REVISION,
        created_by_user_id=user_id,
        updated_by_user_id=user_id,
    )
    agreement.sections = [
        B2BCommercialAgreementSection(
            section_key=section_key,
            section_number=section_number,
            title=title,
            body_text=_base_section_text(title),
            display_order=section_number,
        )
        for section_number, (section_key, title) in enumerate(DEFAULT_AGREEMENT_SECTIONS, start=1)
    ]
    db.add(agreement)
    db.commit()
    return get_commercial_agreement(db, agreement.id)


def update_commercial_agreement_draft(
    db: Session,
    *,
    agreement_id: int,
    fields: dict[str, Any],
    people: list[dict[str, Any]],
    section_updates: dict[str, dict[str, Any]],
    user_id: int | None,
) -> B2BCommercialAgreement:
    agreement = get_commercial_agreement(db, agreement_id)
    _require_draft(agreement)

    for field_name in (
        "legal_representative_name",
        "legal_representative_id",
        "legal_representative_position",
        "billing_address",
        "billing_email",
        "administrative_email",
        "whatsapp",
        "payment_terms",
        "order_reception_days",
        "usual_prep_time",
        "usual_delivery_type",
        "signature_place",
        "client_signer_name",
        "client_signer_id",
        "client_signer_position",
        "catalog_reference",
    ):
        setattr(agreement, field_name, _optional_text(fields.get(field_name)))

    agreement.b2b_discount_percent = _optional_decimal(
        fields.get("b2b_discount_percent"),
        "Descuento comercial B2B",
        minimum=Decimal("0"),
        maximum=Decimal("100"),
    )
    agreement.advance_payment_enabled = bool(fields.get("advance_payment_enabled"))
    agreement.credit_approved = bool(fields.get("credit_approved"))
    agreement.contract_credit_days = _optional_integer(
        fields.get("contract_credit_days"),
        "Días de crédito contractual",
        minimum=0,
    )
    agreement.credit_limit = _optional_decimal(
        fields.get("credit_limit"),
        "Límite de crédito",
        minimum=Decimal("0"),
    )
    agreement.signature_date = _optional_date(fields.get("signature_date"), "Fecha de firma")
    agreement.updated_by_user_id = user_id

    agreement.authorized_people.clear()
    db.flush()
    for person in _normalize_people(people):
        agreement.authorized_people.append(B2BCommercialAgreementPerson(**person))

    sections_by_key = {section.section_key: section for section in agreement.sections}
    unknown_keys = sorted(set(section_updates) - set(sections_by_key))
    if unknown_keys:
        raise B2BCommercialAgreementValidationError(
            f"Apartados contractuales desconocidos: {', '.join(unknown_keys)}."
        )
    for section_key, values in section_updates.items():
        section = sections_by_key[section_key]
        section.title = str(values.get("title") or "").strip()
        section.body_text = str(values.get("body_text") or "").strip()

    db.commit()
    return get_commercial_agreement(db, agreement.id)


def validate_commercial_agreement(agreement: B2BCommercialAgreement) -> list[str]:
    missing: list[str] = []
    if not agreement.customer_active_snapshot:
        missing.append("El cliente debe estar activo en el snapshot del acuerdo.")
    _append_missing(missing, agreement.customer_name_snapshot, "Nombre comercial")
    _append_missing(missing, agreement.legal_name_snapshot, "Razón social")
    _append_missing(missing, agreement.legal_id_snapshot, "Cédula jurídica")
    _append_missing(missing, agreement.address_snapshot, "Dirección")
    _append_missing(missing, agreement.province_snapshot, "Provincia")
    _append_missing(missing, agreement.canton_snapshot, "Cantón")
    _append_missing(missing, agreement.district_snapshot, "Distrito")
    _append_missing(missing, agreement.legal_representative_name, "Representante legal")
    _append_missing(missing, agreement.legal_representative_id, "Identificación del representante")
    _append_missing(missing, agreement.legal_representative_position, "Cargo del representante")
    _append_missing(missing, agreement.billing_email, "Correo de facturación")
    _append_missing(missing, agreement.payment_terms, "Forma de pago")
    if agreement.b2b_discount_percent is None:
        missing.append("Descuento comercial B2B")
    elif agreement.b2b_discount_percent < 0 or agreement.b2b_discount_percent > 100:
        missing.append("El descuento comercial B2B debe estar entre 0 y 100.")

    people = list(agreement.authorized_people or [])
    if not any(person.authorization_type == "orders" and person.name.strip() for person in people):
        missing.append("Al menos una persona autorizada para pedidos")
    if not any(person.authorization_type == "receiving" and person.name.strip() for person in people):
        missing.append("Al menos una persona autorizada para recibir mercadería")

    _append_missing(missing, agreement.order_reception_days, "Días de recepción de pedidos")
    _append_missing(missing, agreement.usual_prep_time, "Tiempo habitual de alisto")
    _append_missing(missing, agreement.usual_delivery_type, "Tipo de entrega habitual")
    _append_missing(missing, agreement.signature_place, "Lugar de firma")
    if agreement.signature_date is None:
        missing.append("Fecha de firma")
    _append_missing(missing, agreement.catalog_reference, "Referencia al catálogo")

    if agreement.credit_approved:
        if agreement.contract_credit_days is None or agreement.contract_credit_days <= 0:
            missing.append("Los días de crédito contractual deben ser mayores a 0.")
        if agreement.credit_limit is None or agreement.credit_limit <= 0:
            missing.append("El límite de crédito debe ser mayor a 0.")

    expected_keys = {section_key for section_key, _title in DEFAULT_AGREEMENT_SECTIONS}
    sections_by_key = {section.section_key: section for section in agreement.sections or []}
    for section_number, (section_key, default_title) in enumerate(DEFAULT_AGREEMENT_SECTIONS, start=1):
        section = sections_by_key.get(section_key)
        if section is None:
            missing.append(f"Apartado {section_number}: {default_title}")
            continue
        if not (section.title or "").strip():
            missing.append(f"Título del apartado {section_number}")
        if not (section.body_text or "").strip():
            missing.append(f"Texto del apartado {section_number}: {section.title or default_title}")
    extra_keys = sorted(set(sections_by_key) - expected_keys)
    if extra_keys:
        missing.append(f"Apartados no reconocidos: {', '.join(extra_keys)}.")
    return missing


def _require_draft(agreement: B2BCommercialAgreement) -> None:
    if agreement.status != "draft":
        raise B2BCommercialAgreementValidationError(
            "Solo los acuerdos comerciales en estado draft se pueden editar."
        )


def _normalize_people(people: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    counters = {authorization_type: 0 for authorization_type in AUTHORIZATION_TYPES}
    for raw_person in people:
        authorization_type = str(raw_person.get("authorization_type") or "").strip().lower()
        if authorization_type not in AUTHORIZATION_TYPES:
            raise B2BCommercialAgreementValidationError("Tipo de autorización de persona inválido.")
        values = {
            "name": _optional_text(raw_person.get("name")),
            "identification": _optional_text(raw_person.get("identification")),
            "position": _optional_text(raw_person.get("position")),
            "phone": _optional_text(raw_person.get("phone")),
            "email": _optional_text(raw_person.get("email")),
        }
        if not any(values.values()):
            continue
        if values["name"] is None:
            raise B2BCommercialAgreementValidationError(
                "El nombre es obligatorio en toda persona autorizada."
            )
        counters[authorization_type] += 1
        normalized.append(
            {
                "authorization_type": authorization_type,
                **values,
                "display_order": counters[authorization_type],
            }
        )
    return normalized


def _append_missing(missing: list[str], value: Any, label: str) -> None:
    if value is None or not str(value).strip():
        missing.append(label)


def _optional_text(value: Any) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


def _optional_decimal(
    value: Any,
    field_name: str,
    *,
    minimum: Decimal | None = None,
    maximum: Decimal | None = None,
) -> Decimal | None:
    if value is None or str(value).strip() == "":
        return None
    normalized = str(value).strip().replace(" ", "").replace(",", ".")
    try:
        parsed = Decimal(normalized)
    except InvalidOperation as exc:
        raise B2BCommercialAgreementValidationError(
            f"{field_name} debe ser un número válido."
        ) from exc
    if minimum is not None and parsed < minimum:
        raise B2BCommercialAgreementValidationError(
            f"{field_name} no puede ser menor que {minimum}."
        )
    if maximum is not None and parsed > maximum:
        raise B2BCommercialAgreementValidationError(
            f"{field_name} no puede ser mayor que {maximum}."
        )
    return parsed


def _optional_integer(value: Any, field_name: str, *, minimum: int | None = None) -> int | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        parsed = int(str(value).strip())
    except ValueError as exc:
        raise B2BCommercialAgreementValidationError(
            f"{field_name} debe ser un número entero."
        ) from exc
    if minimum is not None and parsed < minimum:
        raise B2BCommercialAgreementValidationError(
            f"{field_name} no puede ser menor que {minimum}."
        )
    return parsed


def _optional_date(value: Any, field_name: str) -> date | None:
    if value is None or str(value).strip() == "":
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value).strip())
    except ValueError as exc:
        raise B2BCommercialAgreementValidationError(
            f"{field_name} debe usar el formato YYYY-MM-DD."
        ) from exc
