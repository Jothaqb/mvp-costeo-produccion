from __future__ import annotations

import re
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
DEFAULT_TEMPLATE_REVISION = "b2b-direct-purchase-v1.1"
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
DEFAULT_AGREEMENT_SECTION_TEXTS = {
    "prices": (
        "GREEN CORNER suministrará a EL CLIENTE, identificado como {{customer_name}} "
        "({{legal_name}}, cédula jurídica {{legal_id}}), los precios comerciales vigentes al "
        "momento de confirmar cada pedido. Los precios podrán ser actualizados por GREEN CORNER "
        "cuando cambien las condiciones comerciales, los costos o el mercado, previa comunicación "
        "a EL CLIENTE.\n\n"
        f"{DEFAULT_CATALOG_REFERENCE}"
    ),
    "b2b_discount_and_volume": (
        "GREEN CORNER reconoce a EL CLIENTE un descuento comercial B2B de "
        "{{b2b_discount_percent}}% sobre los precios B2C que correspondan, sujeto a la vigencia de "
        "la relación comercial, al volumen de compra y al cumplimiento de las condiciones de este "
        "acuerdo. Cualquier descuento distinto deberá constar por escrito y aplicará únicamente a "
        "los pedidos expresamente confirmados bajo esa condición."
    ),
    "advance_payment_discount": (
        "Cuando GREEN CORNER habilite un beneficio adicional por pago anticipado, este deberá ser "
        "informado y confirmado por escrito antes de la facturación del pedido. El beneficio no es "
        "automático, no se acumula con otras promociones salvo autorización expresa y podrá ser "
        "retirado para pedidos futuros mediante comunicación a EL CLIENTE."
    ),
    "minimum_order_and_delivery_cost": (
        "El pedido mínimo y cualquier costo de entrega serán informados por GREEN CORNER antes de "
        "confirmar el pedido. EL CLIENTE podrá aceptar el despacho bajo esas condiciones, retirar "
        "la mercadería por sus propios medios o ajustar el pedido. Ningún pedido se considerará "
        "aceptado hasta que GREEN CORNER confirme disponibilidad, monto y condiciones de entrega."
    ),
    "order_process": (
        "EL CLIENTE remitirá sus pedidos por los medios autorizados y dentro de los siguientes días "
        "o ventanas habituales de recepción: {{order_reception_days}}. GREEN CORNER verificará la "
        "identidad de la persona solicitante, disponibilidad y condiciones comerciales, y comunicará "
        "la aceptación o los ajustes necesarios. Solo la confirmación de GREEN CORNER convierte la "
        "solicitud en un pedido aceptado."
    ),
    "preparation_and_delivery": (
        "El tiempo habitual de alisto es {{usual_prep_time}} y la modalidad habitual de entrega es "
        "{{usual_delivery_type}}. Estos datos son estimaciones operativas y podrán variar por "
        "disponibilidad, volumen, ubicación, fuerza mayor o coordinación con EL CLIENTE. GREEN CORNER "
        "informará oportunamente cualquier cambio material en el despacho confirmado."
    ),
    "commercial_credit": (
        "Cuando GREEN CORNER apruebe crédito comercial, las condiciones particulares serán un plazo "
        "de {{contract_credit_days}} días naturales y un límite máximo de crédito de ₡{{credit_limit}}. "
        "El crédito no constituye una obligación permanente de financiamiento y cada despacho queda "
        "sujeto a disponibilidad de cupo, comportamiento de pago y ausencia de facturas vencidas. "
        "Si no se ha aprobado crédito, EL CLIENTE deberá pagar según la forma indicada para el pedido."
    ),
    "overdue_invoices": (
        "La existencia de facturas vencidas faculta a GREEN CORNER para suspender la preparación o "
        "el despacho de pedidos, retener pedidos no entregados y solicitar el pago de los saldos "
        "pendientes antes de continuar la relación a crédito. La suspensión no extingue las "
        "obligaciones de pago ya adquiridas por EL CLIENTE."
    ),
    "late_payment_interest": (
        "Los saldos vencidos podrán generar intereses moratorios desde su fecha de vencimiento, "
        "hasta el máximo permitido por la legislación costarricense y conforme a la comunicación "
        "comercial aplicable. El pago de intereses no limita el derecho de GREEN CORNER a exigir el "
        "principal, gastos de cobro razonables u otros remedios legalmente disponibles."
    ),
    "credit_benefit_changes": (
        "GREEN CORNER podrá reducir, suspender o retirar el beneficio de crédito cuando exista mora, "
        "deterioro en el comportamiento de pago, información desactualizada, cambio relevante en la "
        "situación de EL CLIENTE o aumento del riesgo comercial. La modificación aplicará a pedidos "
        "futuros y no altera las obligaciones ya facturadas."
    ),
    "goods_receipt": (
        "La mercadería será recibida únicamente por las personas autorizadas por EL CLIENTE. Al "
        "momento de la entrega deberán verificarse cantidades, condición aparente, identificación de "
        "los productos y concordancia con el documento de entrega. La firma o confirmación de recibido "
        "acredita la entrega, sin perjuicio de reclamos oportunos por defectos no visibles."
    ),
    "returns_and_claims": (
        "Todo reclamo o solicitud de devolución deberá comunicarse a GREEN CORNER tan pronto sea "
        "detectado, con la identificación del pedido, factura, producto, cantidad, motivo y evidencia "
        "disponible. GREEN CORNER evaluará el caso y autorizará por escrito, cuando corresponda, la "
        "devolución, reposición o nota de crédito. No se aceptarán devoluciones no autorizadas."
    ),
    "post_delivery_liability": (
        "Después de la entrega, EL CLIENTE será responsable por la manipulación, almacenamiento, "
        "rotación, conservación y uso adecuado de la mercadería. GREEN CORNER no responderá por "
        "daños derivados de condiciones posteriores a la entrega, salvo que se demuestre un defecto "
        "atribuible al producto entregado y el reclamo se presente conforme a este acuerdo."
    ),
    "customer_information_changes": (
        "EL CLIENTE deberá informar por escrito cualquier cambio en su razón social, cédula jurídica, "
        "domicilio, representación legal, contactos, personas autorizadas o información de facturación. "
        "Mientras el cambio no sea recibido y confirmado, GREEN CORNER podrá utilizar la información "
        "registrada en este acuerdo y considerar válidas las comunicaciones y entregas realizadas con ella."
    ),
    "commercial_relationship_termination": (
        "Cualquiera de las partes podrá dar por terminada la relación comercial mediante comunicación "
        "escrita. GREEN CORNER podrá terminarla de inmediato ante incumplimiento grave, mora reiterada, "
        "uso indebido de condiciones comerciales o información falsa. La terminación no afecta pedidos "
        "ya entregados, facturas emitidas ni obligaciones pendientes de pago."
    ),
    "communications": (
        "Las comunicaciones operativas y comerciales se realizarán por los medios registrados por EL "
        "CLIENTE, incluyendo correo de facturación {{billing_email}}, correo administrativo "
        "{{administrative_email}} y WhatsApp {{whatsapp}}. Cada parte deberá mantener actualizados sus "
        "datos de contacto. Las comunicaciones enviadas a esos medios se presumirán recibidas en el "
        "curso normal de operación, salvo prueba en contrario."
    ),
    "term": (
        "Este acuerdo entra en vigencia a partir del {{signature_date}} y permanecerá vigente mientras "
        "subsista la relación comercial, salvo terminación por cualquiera de las partes. Las condiciones "
        "podrán actualizarse por acuerdo escrito o mediante una nueva versión aceptada, sin perjuicio "
        "de las obligaciones nacidas durante la vigencia de la versión anterior."
    ),
    "applicable_law": (
        "Este acuerdo se regirá e interpretará conforme a las leyes de la República de Costa Rica. "
        "Las partes procurarán resolver de buena fe cualquier diferencia; si no fuera posible, se "
        "someterán a la jurisdicción competente de Costa Rica, sin perjuicio de otros mecanismos de "
        "solución de controversias que acuerden por escrito."
    ),
    "acceptance": (
        "EL CLIENTE, representado por {{legal_representative_name}}, declara haber leído, entendido y "
        "aceptado las condiciones de este acuerdo. Para constancia, las partes lo suscriben en "
        "{{signature_place}}, el {{signature_date}}. Por EL CLIENTE firma {{client_signer_name}}, "
        "identificación {{client_signer_id}}, en calidad de {{client_signer_position}}."
    ),
}

_TEMPLATE_VARIABLE_PATTERN = re.compile(r"{{\s*([A-Za-z_][A-Za-z0-9_]*)\s*}}")
_TEMPLATE_VARIABLE_FIELDS = {
    "customer_name": "customer_name_snapshot",
    "legal_name": "legal_name_snapshot",
    "legal_id": "legal_id_snapshot",
    "phone": "phone_snapshot",
    "address": "address_snapshot",
    "province": "province_snapshot",
    "canton": "canton_snapshot",
    "district": "district_snapshot",
    "credit_days": "credit_days_snapshot",
    "b2b_channel_name": "b2b_channel_name_snapshot",
    "legal_representative_name": "legal_representative_name",
    "legal_representative_id": "legal_representative_id",
    "legal_representative_position": "legal_representative_position",
    "billing_address": "billing_address",
    "billing_email": "billing_email",
    "administrative_email": "administrative_email",
    "whatsapp": "whatsapp",
    "b2b_discount_percent": "b2b_discount_percent",
    "payment_terms": "payment_terms",
    "contract_credit_days": "contract_credit_days",
    "credit_limit": "credit_limit",
    "order_reception_days": "order_reception_days",
    "usual_prep_time": "usual_prep_time",
    "usual_delivery_type": "usual_delivery_type",
    "signature_place": "signature_place",
    "signature_date": "signature_date",
    "client_signer_name": "client_signer_name",
    "client_signer_id": "client_signer_id",
    "client_signer_position": "client_signer_position",
}


class B2BCommercialAgreementValidationError(Exception):
    pass


def _base_section_text(section_key: str) -> str:
    return DEFAULT_AGREEMENT_SECTION_TEXTS[section_key]


def render_agreement_template_text(
    text: str | None,
    agreement: B2BCommercialAgreement,
    *,
    missing_value: str = "______",
) -> str:
    """Render known agreement variables without evaluating arbitrary template syntax."""

    source = str(text or "")

    def replace(match: re.Match[str]) -> str:
        variable_name = match.group(1)
        field_name = _TEMPLATE_VARIABLE_FIELDS.get(variable_name)
        if field_name is None:
            return match.group(0)
        value = getattr(agreement, field_name, None)
        if value is None or (isinstance(value, str) and not value.strip()):
            return missing_value
        return _format_template_value(value)

    return _TEMPLATE_VARIABLE_PATTERN.sub(replace, source)


def find_missing_template_variables(
    text: str | None,
    agreement: B2BCommercialAgreement,
) -> list[str]:
    """Return known variables referenced by text that do not currently have a value."""

    missing: list[str] = []
    for match in _TEMPLATE_VARIABLE_PATTERN.finditer(str(text or "")):
        variable_name = match.group(1)
        field_name = _TEMPLATE_VARIABLE_FIELDS.get(variable_name)
        if field_name is None or variable_name in missing:
            continue
        if variable_name in {"contract_credit_days", "credit_limit"} and not agreement.credit_approved:
            continue
        value = getattr(agreement, field_name, None)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing.append(variable_name)
    return missing


def reset_commercial_agreement_sections_to_template(
    db: Session,
    *,
    agreement_id: int,
) -> B2BCommercialAgreement:
    agreement = get_commercial_agreement(db, agreement_id)
    _require_draft(agreement)
    sections_by_key = {section.section_key: section for section in agreement.sections}

    for section_number, (section_key, title) in enumerate(DEFAULT_AGREEMENT_SECTIONS, start=1):
        section = sections_by_key.get(section_key)
        if section is None:
            agreement.sections.append(
                B2BCommercialAgreementSection(
                    section_key=section_key,
                    section_number=section_number,
                    title=title,
                    body_text=_base_section_text(section_key),
                    display_order=section_number,
                )
            )
            continue
        section.title = title
        section.body_text = _base_section_text(section_key)

    db.commit()
    return get_commercial_agreement(db, agreement.id)


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
            body_text=_base_section_text(section_key),
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
            continue
        for variable_name in find_missing_template_variables(section.body_text, agreement):
            missing.append(
                f"Variable pendiente en apartado {section_number}: {{{{{variable_name}}}}}"
            )
    extra_keys = sorted(set(sections_by_key) - expected_keys)
    if extra_keys:
        missing.append(f"Apartados no reconocidos: {', '.join(extra_keys)}.")
    return missing


def _require_draft(agreement: B2BCommercialAgreement) -> None:
    if agreement.status != "draft":
        raise B2BCommercialAgreementValidationError(
            "Solo los acuerdos comerciales en estado draft se pueden editar."
        )


def _format_template_value(value: Any) -> str:
    if isinstance(value, Decimal):
        normalized = value.normalize()
        if normalized == normalized.to_integral():
            return f"{int(normalized):,}"
        return format(normalized, "f").rstrip("0").rstrip(".")
    if isinstance(value, date):
        return value.isoformat()
    return str(value).strip()


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
