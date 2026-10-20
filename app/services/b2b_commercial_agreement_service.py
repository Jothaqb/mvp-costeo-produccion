from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy.orm import Session, joinedload, selectinload

from app.models import (
    AuditLog,
    B2BCommercialAgreement,
    B2BCommercialAgreementPerson,
    B2BCommercialAgreementSection,
    B2BCustomer,
)
from app.services.audit_service import serialize_audit_payload


AGREEMENT_STATUSES = {"draft", "generated", "signed", "archived"}
AUTHORIZATION_TYPES = {"orders", "receiving"}
PAYMENT_TERM_OPTIONS = (
    "Pago anticipado antes del despacho",
    "Contado contra entrega",
    "Crédito comercial aprobado",
    "Consignación y pago contra entrega",
)
USUAL_DELIVERY_TYPE_OPTIONS = (
    "Ruta de entrega GREEN CORNER",
    "Encomienda",
    "Retiro",
)
CUSTOMER_SYNC_PRESERVED_FIELDS = (
    "Representante legal, identificación y cargo",
    "Correos de facturación y administrativo",
    "Descuento comercial, límite y plazo contractual negociado",
    "Forma de pago e indicadores contractuales",
    "Personas autorizadas, condiciones y textos contractuales",
    "Datos de firma y PDF histórico",
)
_CUSTOMER_SYNC_CSRF_PURPOSE = b"b2b-commercial-agreement-customer-sync-csrf-v1"
_CUSTOMER_SYNC_CONFIRM_PURPOSE = b"b2b-commercial-agreement-customer-sync-confirm-v1"
_CUSTOMER_SNAPSHOT_FIELDS = (
    ("customer_name_snapshot", "Nombre comercial", "customer_name"),
    ("legal_name_snapshot", "Razón social", "legal_name"),
    ("legal_id_snapshot", "Cédula jurídica", "legal_id"),
    ("phone_snapshot", "Teléfono", "phone"),
    ("address_snapshot", "Dirección", "address"),
    ("province_snapshot", "Provincia", "province"),
    ("canton_snapshot", "Cantón", "canton"),
    ("district_snapshot", "Distrito", "district"),
    ("credit_days_snapshot", "Días de crédito del cliente", "credit_days"),
    ("customer_active_snapshot", "Estado activo del cliente", "active"),
)
_PAYMENT_TERM_ALIASES = {
    "contado": "Contado contra entrega",
    "crédito": "Crédito comercial aprobado",
    "credito": "Crédito comercial aprobado",
    "pago anticipado": "Pago anticipado antes del despacho",
    "consignacion y pago contra entrega": "Consignación y pago contra entrega",
}
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
        "La forma de pago acordada es {{payment_terms}}. Cuando GREEN CORNER habilite un beneficio "
        "adicional por pago anticipado, este deberá ser "
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
_TEMPLATE_VARIABLE_LABELS = {
    "customer_name": "Nombre comercial",
    "legal_name": "Razón social",
    "legal_id": "Cédula jurídica",
    "phone": "Teléfono del cliente",
    "address": "Dirección",
    "province": "Provincia",
    "canton": "Cantón",
    "district": "Distrito",
    "credit_days": "Días de crédito del cliente",
    "b2b_channel_name": "Canal B2B",
    "legal_representative_name": "Representante legal",
    "legal_representative_id": "Identificación del representante",
    "legal_representative_position": "Cargo del representante",
    "billing_address": "Dirección de facturación",
    "billing_email": "Correo de facturación",
    "administrative_email": "Correo administrativo",
    "whatsapp": "WhatsApp",
    "b2b_discount_percent": "Descuento comercial B2B",
    "payment_terms": "Forma de pago",
    "contract_credit_days": "Días de crédito contractual",
    "credit_limit": "Límite de crédito",
    "order_reception_days": "Días de recepción de pedidos",
    "usual_prep_time": "Tiempo habitual de alisto",
    "usual_delivery_type": "Entrega habitual",
    "signature_place": "Lugar de firma",
    "signature_date": "Fecha de firma",
    "client_signer_name": "Nombre del firmante del cliente",
    "client_signer_id": "Identificación del firmante del cliente",
    "client_signer_position": "Cargo del firmante del cliente",
}


class B2BCommercialAgreementValidationError(Exception):
    pass


class B2BCommercialAgreementCustomerSyncSecurityError(Exception):
    pass


class B2BCommercialAgreementCustomerSyncStaleError(
    B2BCommercialAgreementValidationError
):
    pass


@dataclass(frozen=True)
class AgreementCustomerSyncChange:
    field_name: str
    label: str
    old_value: Any
    new_value: Any


@dataclass(frozen=True)
class AgreementCustomerSyncPreview:
    agreement_id: int
    customer_id: int
    customer_name: str
    version_number: int
    status: str
    changes: tuple[AgreementCustomerSyncChange, ...]
    exceptions: tuple[str, ...]
    fingerprint: str

    @property
    def has_changes(self) -> bool:
        return bool(self.changes)


@dataclass(frozen=True)
class BulkAgreementCustomerSyncPreview:
    reviewed_count: int
    changed_count: int
    unchanged_count: int
    agreements: tuple[AgreementCustomerSyncPreview, ...]
    fingerprint: str


@dataclass(frozen=True)
class AgreementCustomerSyncResult:
    reviewed_count: int
    updated_count: int
    updated_agreement_ids: tuple[int, ...]


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
        if (
            variable_name in {"contract_credit_days", "credit_limit"}
            and normalize_payment_terms(agreement.payment_terms) != "Crédito comercial aprobado"
        ):
            continue
        value = getattr(agreement, field_name, None)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing.append(variable_name)
    return missing


def normalize_payment_terms(value: Any) -> str:
    normalized = str(value or "").strip()
    return _PAYMENT_TERM_ALIASES.get(normalized.casefold(), normalized)


def _set_agreement_payment_terms(
    agreement: B2BCommercialAgreement,
    payment_terms: str,
) -> None:
    agreement.payment_terms = payment_terms
    agreement.advance_payment_enabled = payment_terms == "Pago anticipado antes del despacho"
    agreement.credit_approved = payment_terms == "Crédito comercial aprobado"


def format_person_contact_info(phone: Any, email: Any) -> str:
    phone_value = str(phone or "").strip()
    email_value = str(email or "").strip()
    if phone_value and email_value:
        return f"{phone_value} / {email_value}"
    return phone_value or email_value


def parse_person_contact_info(value: Any) -> tuple[str | None, str | None]:
    contact_info = str(value or "").strip()
    if not contact_info:
        return None, None
    if " / " in contact_info:
        phone_value, email_value = (part.strip() for part in contact_info.split(" / ", 1))
        return phone_value or None, email_value or None
    if "@" in contact_info:
        return None, contact_info
    return contact_info, None


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
            joinedload(B2BCommercialAgreement.customer).joinedload(
                B2BCustomer.channel_record
            ),
            selectinload(B2BCommercialAgreement.authorized_people),
            selectinload(B2BCommercialAgreement.sections),
        )
        .populate_existing()
        .filter(B2BCommercialAgreement.id == agreement_id)
        .one()
    )


def build_commercial_agreement_customer_sync_csrf_token(
    session_token: str | None,
) -> str:
    normalized_session_token = (session_token or "").strip()
    if not normalized_session_token:
        raise B2BCommercialAgreementCustomerSyncSecurityError(
            "No hay una sesión válida para generar la protección CSRF."
        )
    return hmac.new(
        normalized_session_token.encode("utf-8"),
        _CUSTOMER_SYNC_CSRF_PURPOSE,
        hashlib.sha256,
    ).hexdigest()


def validate_commercial_agreement_customer_sync_csrf_token(
    session_token: str | None,
    submitted_token: str | None,
) -> None:
    normalized_submitted_token = (submitted_token or "").strip()
    if not normalized_submitted_token:
        raise B2BCommercialAgreementCustomerSyncSecurityError(
            "Falta el token CSRF del formulario."
        )
    expected_token = build_commercial_agreement_customer_sync_csrf_token(session_token)
    if not hmac.compare_digest(normalized_submitted_token, expected_token):
        raise B2BCommercialAgreementCustomerSyncSecurityError(
            "El token CSRF no es válido para esta sesión."
        )


def build_commercial_agreement_customer_sync_confirmation_token(
    session_token: str | None,
    *,
    scope: str,
    fingerprint: str,
) -> str:
    normalized_session_token = (session_token or "").strip()
    if not normalized_session_token:
        raise B2BCommercialAgreementCustomerSyncSecurityError(
            "No hay una sesión válida para confirmar la actualización."
        )
    payload = (
        _CUSTOMER_SYNC_CONFIRM_PURPOSE
        + b"|"
        + scope.encode("utf-8")
        + b"|"
        + fingerprint.encode("ascii")
    )
    return hmac.new(
        normalized_session_token.encode("utf-8"),
        payload,
        hashlib.sha256,
    ).hexdigest()


def validate_commercial_agreement_customer_sync_confirmation_token(
    session_token: str | None,
    submitted_token: str | None,
    *,
    scope: str,
    fingerprint: str,
) -> None:
    normalized_submitted_token = (submitted_token or "").strip()
    if not normalized_submitted_token:
        raise B2BCommercialAgreementCustomerSyncSecurityError(
            "Falta la confirmación firmada de la vista previa."
        )
    expected_token = build_commercial_agreement_customer_sync_confirmation_token(
        session_token,
        scope=scope,
        fingerprint=fingerprint,
    )
    if not hmac.compare_digest(normalized_submitted_token, expected_token):
        raise B2BCommercialAgreementCustomerSyncSecurityError(
            "La confirmación no corresponde a esta sesión o vista previa."
        )


def preview_commercial_agreement_customer_sync(
    db: Session,
    agreement_id: int,
) -> AgreementCustomerSyncPreview:
    agreement = get_commercial_agreement(db, agreement_id)
    _require_draft(agreement)
    return _build_customer_sync_preview(agreement)


def preview_all_draft_commercial_agreement_customer_sync(
    db: Session,
) -> BulkAgreementCustomerSyncPreview:
    agreements = _query_draft_agreements_for_customer_sync(db).all()
    return _build_bulk_customer_sync_preview(agreements)


def execute_commercial_agreement_customer_sync(
    db: Session,
    *,
    agreement_id: int,
    expected_fingerprint: str,
    user_id: int | None,
    username: str,
    request_path: str | None = None,
) -> AgreementCustomerSyncResult:
    try:
        agreement = (
            _query_agreements_for_customer_sync(db)
            .filter(B2BCommercialAgreement.id == agreement_id)
            .with_for_update()
            .one()
        )
        _require_draft(agreement)
        preview = _build_customer_sync_preview(agreement)
        if not hmac.compare_digest(preview.fingerprint, expected_fingerprint or ""):
            raise B2BCommercialAgreementCustomerSyncStaleError(
                "Los datos cambiaron después de la vista previa. Revise nuevamente antes de confirmar."
            )
        updated_ids: tuple[int, ...] = ()
        if preview.changes:
            _apply_customer_sync_changes(
                db,
                agreement=agreement,
                preview=preview,
                user_id=user_id,
                username=username,
                request_path=request_path,
                scope="individual",
            )
            updated_ids = (agreement.id,)
        db.commit()
        return AgreementCustomerSyncResult(
            reviewed_count=1,
            updated_count=len(updated_ids),
            updated_agreement_ids=updated_ids,
        )
    except Exception:
        db.rollback()
        raise


def execute_all_draft_commercial_agreement_customer_sync(
    db: Session,
    *,
    expected_fingerprint: str,
    user_id: int | None,
    username: str,
    request_path: str | None = None,
) -> AgreementCustomerSyncResult:
    try:
        agreements = (
            _query_draft_agreements_for_customer_sync(db)
            .with_for_update()
            .all()
        )
        preview = _build_bulk_customer_sync_preview(agreements)
        if not hmac.compare_digest(preview.fingerprint, expected_fingerprint or ""):
            raise B2BCommercialAgreementCustomerSyncStaleError(
                "Los borradores o datos de clientes cambiaron después de la vista previa. "
                "Genere una nueva revisión antes de confirmar."
            )
        agreements_by_id = {agreement.id: agreement for agreement in agreements}
        updated_ids: list[int] = []
        for agreement_preview in preview.agreements:
            if not agreement_preview.changes:
                continue
            agreement = agreements_by_id[agreement_preview.agreement_id]
            _apply_customer_sync_changes(
                db,
                agreement=agreement,
                preview=agreement_preview,
                user_id=user_id,
                username=username,
                request_path=request_path,
                scope="all_drafts",
            )
            updated_ids.append(agreement.id)
        db.commit()
        return AgreementCustomerSyncResult(
            reviewed_count=preview.reviewed_count,
            updated_count=len(updated_ids),
            updated_agreement_ids=tuple(updated_ids),
        )
    except Exception:
        db.rollback()
        raise


def _query_agreements_for_customer_sync(db: Session):
    return (
        db.query(B2BCommercialAgreement)
        .options(
            joinedload(B2BCommercialAgreement.customer).joinedload(
                B2BCustomer.channel_record
            )
        )
        .populate_existing()
    )


def _query_draft_agreements_for_customer_sync(db: Session):
    return (
        _query_agreements_for_customer_sync(db)
        .filter(B2BCommercialAgreement.status == "draft")
        .order_by(B2BCommercialAgreement.id)
    )


def _build_customer_sync_preview(
    agreement: B2BCommercialAgreement,
) -> AgreementCustomerSyncPreview:
    customer = agreement.customer
    changes: list[AgreementCustomerSyncChange] = []
    exceptions: list[str] = []
    old_address_snapshot = agreement.address_snapshot
    old_phone_snapshot = agreement.phone_snapshot

    for agreement_field, label, customer_field in _CUSTOMER_SNAPSHOT_FIELDS:
        _append_customer_sync_change(
            changes,
            agreement_field,
            label,
            getattr(agreement, agreement_field),
            getattr(customer, customer_field),
        )
    channel_name = (
        customer.channel_record.name if customer.channel_record is not None else None
    )
    _append_customer_sync_change(
        changes,
        "b2b_channel_name_snapshot",
        "Canal B2B",
        agreement.b2b_channel_name_snapshot,
        channel_name,
    )

    address_changed = not _customer_sync_values_equal(
        old_address_snapshot, customer.address
    )
    if address_changed:
        if _customer_sync_values_equal(agreement.billing_address, old_address_snapshot):
            _append_customer_sync_change(
                changes,
                "billing_address",
                "Dirección de facturación (sin personalizar)",
                agreement.billing_address,
                customer.address,
            )
        else:
            exceptions.append(
                "Dirección de facturación personalizada: se conserva y no se reemplaza por la dirección del cliente."
            )

    phone_changed = not _customer_sync_values_equal(old_phone_snapshot, customer.phone)
    if phone_changed:
        if _customer_sync_values_equal(agreement.whatsapp, old_phone_snapshot):
            _append_customer_sync_change(
                changes,
                "whatsapp",
                "WhatsApp (sin personalizar)",
                agreement.whatsapp,
                customer.phone,
            )
        else:
            exceptions.append(
                "WhatsApp personalizado: se conserva y no se reemplaza por el teléfono del cliente."
            )

    customer_payment_terms = normalize_payment_terms(customer.default_payment_terms)
    agreement_payment_terms = normalize_payment_terms(agreement.payment_terms)
    if not _customer_sync_values_equal(customer_payment_terms, agreement_payment_terms):
        exceptions.append(
            "Forma de pago diferente: se conserva la condición contractual del borrador; "
            "debe decidirse por separado con la acción de forma de pago."
        )
    if (
        agreement.credit_days_snapshot != customer.credit_days
        and agreement.contract_credit_days is not None
    ):
        exceptions.append(
            "Días de crédito contractual negociados: se conservan aunque se actualicen los días de crédito del cliente."
        )

    state_payload = _customer_sync_state_payload(agreement)
    return AgreementCustomerSyncPreview(
        agreement_id=agreement.id,
        customer_id=customer.id,
        customer_name=customer.customer_name,
        version_number=agreement.version_number,
        status=agreement.status,
        changes=tuple(changes),
        exceptions=tuple(exceptions),
        fingerprint=_customer_sync_fingerprint(
            {"scope": f"agreement:{agreement.id}", "agreement": state_payload}
        ),
    )


def _build_bulk_customer_sync_preview(
    agreements: list[B2BCommercialAgreement],
) -> BulkAgreementCustomerSyncPreview:
    agreement_previews = tuple(
        _build_customer_sync_preview(agreement) for agreement in agreements
    )
    changed_count = sum(preview.has_changes for preview in agreement_previews)
    return BulkAgreementCustomerSyncPreview(
        reviewed_count=len(agreement_previews),
        changed_count=changed_count,
        unchanged_count=len(agreement_previews) - changed_count,
        agreements=agreement_previews,
        fingerprint=_customer_sync_fingerprint(
            {
                "scope": "all_drafts",
                "agreements": [
                    _customer_sync_state_payload(agreement) for agreement in agreements
                ],
            }
        ),
    )


def _customer_sync_state_payload(agreement: B2BCommercialAgreement) -> dict[str, Any]:
    customer = agreement.customer
    customer_values = {
        customer_field: getattr(customer, customer_field)
        for _agreement_field, _label, customer_field in _CUSTOMER_SNAPSHOT_FIELDS
    }
    return {
        "agreement_id": agreement.id,
        "customer_id": agreement.customer_id,
        "status": agreement.status,
        "agreement_updated_at": agreement.updated_at,
        "customer_updated_at": customer.updated_at,
        "snapshots": {
            agreement_field: getattr(agreement, agreement_field)
            for agreement_field, _label, _customer_field in _CUSTOMER_SNAPSHOT_FIELDS
        },
        "b2b_channel_name_snapshot": agreement.b2b_channel_name_snapshot,
        "billing_address": agreement.billing_address,
        "whatsapp": agreement.whatsapp,
        "payment_terms": agreement.payment_terms,
        "advance_payment_enabled": agreement.advance_payment_enabled,
        "credit_approved": agreement.credit_approved,
        "contract_credit_days": agreement.contract_credit_days,
        "customer": {
            **customer_values,
            "channel_id": customer.channel_id,
            "channel_name": (
                customer.channel_record.name
                if customer.channel_record is not None
                else None
            ),
            "default_payment_terms": customer.default_payment_terms,
        },
    }


def _customer_sync_fingerprint(payload: dict[str, Any]) -> str:
    serialized = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _append_customer_sync_change(
    changes: list[AgreementCustomerSyncChange],
    field_name: str,
    label: str,
    old_value: Any,
    new_value: Any,
) -> None:
    if _customer_sync_values_equal(old_value, new_value):
        return
    changes.append(
        AgreementCustomerSyncChange(
            field_name=field_name,
            label=label,
            old_value=old_value,
            new_value=new_value,
        )
    )


def _customer_sync_values_equal(left: Any, right: Any) -> bool:
    if isinstance(left, str) or isinstance(right, str):
        return str(left or "").strip() == str(right or "").strip()
    return left == right


def _apply_customer_sync_changes(
    db: Session,
    *,
    agreement: B2BCommercialAgreement,
    preview: AgreementCustomerSyncPreview,
    user_id: int | None,
    username: str,
    request_path: str | None,
    scope: str,
) -> None:
    old_values = {
        change.field_name: change.old_value for change in preview.changes
    }
    new_values = {
        change.field_name: change.new_value for change in preview.changes
    }
    for change in preview.changes:
        setattr(agreement, change.field_name, change.new_value)
    agreement.updated_by_user_id = user_id
    db.add(
        AuditLog(
            user_id=user_id,
            username=(username or "system")[:100],
            module="commercial",
            action="commercial_agreement_customer_data_synced",
            entity_type="b2b_commercial_agreement",
            entity_id=str(agreement.id),
            entity_label=f"{agreement.customer_name_snapshot} v{agreement.version_number}",
            old_values=serialize_audit_payload(old_values),
            new_values=serialize_audit_payload(new_values),
            request_path=request_path,
            method="POST",
            notes=serialize_audit_payload(
                {
                    "scope": scope,
                    "preview_fingerprint": preview.fingerprint,
                    "preserved_exceptions": list(preview.exceptions),
                }
            ),
        )
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
    payment_terms = normalize_payment_terms(customer.default_payment_terms)
    if payment_terms and payment_terms not in PAYMENT_TERM_OPTIONS:
        raise B2BCommercialAgreementValidationError(
            "La forma de pago predeterminada del cliente no es válida."
        )
    credit_approved = payment_terms == "Crédito comercial aprobado"
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
        payment_terms=payment_terms or None,
        advance_payment_enabled=payment_terms == "Pago anticipado antes del despacho",
        credit_approved=credit_approved,
        contract_credit_days=(
            customer.credit_days if credit_approved and customer.credit_days > 0 else None
        ),
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


def sync_commercial_agreement_payment_terms_from_customer(
    db: Session,
    *,
    agreement_id: int,
    user_id: int | None,
) -> B2BCommercialAgreement:
    agreement = get_commercial_agreement(db, agreement_id)
    _require_draft(agreement)
    payment_terms = normalize_payment_terms(agreement.customer.default_payment_terms)
    if payment_terms not in PAYMENT_TERM_OPTIONS:
        raise B2BCommercialAgreementValidationError(
            "El cliente no tiene una forma de pago predeterminada válida."
        )
    _set_agreement_payment_terms(agreement, payment_terms)
    agreement.updated_by_user_id = user_id
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
        "order_reception_days",
        "usual_prep_time",
        "signature_place",
        "client_signer_name",
        "client_signer_id",
        "client_signer_position",
        "catalog_reference",
    ):
        setattr(agreement, field_name, _optional_text(fields.get(field_name)))

    payment_terms = normalize_payment_terms(fields.get("payment_terms"))
    if payment_terms not in PAYMENT_TERM_OPTIONS:
        raise B2BCommercialAgreementValidationError(
            "Forma de pago debe ser una de las opciones permitidas."
        )
    usual_delivery_type = str(fields.get("usual_delivery_type") or "").strip()
    if usual_delivery_type not in USUAL_DELIVERY_TYPE_OPTIONS:
        raise B2BCommercialAgreementValidationError(
            "Entrega habitual debe ser una de las opciones permitidas."
        )
    _set_agreement_payment_terms(agreement, payment_terms)
    agreement.usual_delivery_type = usual_delivery_type

    agreement.b2b_discount_percent = _optional_decimal(
        fields.get("b2b_discount_percent"),
        "Descuento comercial B2B",
        minimum=Decimal("0"),
        maximum=Decimal("100"),
    )
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
    reported_field_names: set[str] = set()
    if not agreement.customer_active_snapshot:
        missing.append("El cliente debe estar activo en el snapshot del acuerdo.")
    for field_name, label in (
        ("customer_name_snapshot", "Nombre comercial"),
        ("legal_name_snapshot", "Razón social"),
        ("legal_id_snapshot", "Cédula jurídica"),
        ("address_snapshot", "Dirección"),
        ("province_snapshot", "Provincia"),
        ("canton_snapshot", "Cantón"),
        ("district_snapshot", "Distrito"),
        ("legal_representative_name", "Representante legal"),
        ("legal_representative_id", "Identificación del representante"),
        ("legal_representative_position", "Cargo del representante"),
        ("billing_email", "Correo de facturación"),
    ):
        _append_missing_field(
            missing,
            reported_field_names,
            field_name,
            getattr(agreement, field_name),
            label,
        )
    payment_terms = normalize_payment_terms(agreement.payment_terms)
    if payment_terms not in PAYMENT_TERM_OPTIONS:
        missing.append("Forma de pago debe ser una de las opciones permitidas.")
        reported_field_names.add("payment_terms")
    if agreement.b2b_discount_percent is None:
        missing.append("Descuento comercial B2B")
        reported_field_names.add("b2b_discount_percent")
    elif agreement.b2b_discount_percent < 0 or agreement.b2b_discount_percent > 100:
        missing.append("El descuento comercial B2B debe estar entre 0 y 100.")
        reported_field_names.add("b2b_discount_percent")

    people = list(agreement.authorized_people or [])
    if not any(
        person.authorization_type == "orders" and _is_complete_authorized_person(person)
        for person in people
    ):
        missing.append(
            "Al menos una persona autorizada para pedidos "
            "(nombre, cargo y teléfono o correo)."
        )
    if not any(
        person.authorization_type == "receiving" and _is_complete_authorized_person(person)
        for person in people
    ):
        missing.append(
            "Al menos una persona autorizada para recibir mercadería "
            "(nombre, cargo y teléfono o correo)."
        )

    for field_name, label in (
        ("order_reception_days", "Días de recepción de pedidos"),
        ("usual_prep_time", "Tiempo habitual de alisto"),
    ):
        _append_missing_field(
            missing,
            reported_field_names,
            field_name,
            getattr(agreement, field_name),
            label,
        )
    if agreement.usual_delivery_type not in USUAL_DELIVERY_TYPE_OPTIONS:
        missing.append("Entrega habitual debe ser una de las opciones permitidas.")
        reported_field_names.add("usual_delivery_type")
    _append_missing_field(
        missing,
        reported_field_names,
        "signature_place",
        agreement.signature_place,
        "Lugar de firma",
    )
    if agreement.signature_date is None:
        missing.append("Fecha de firma")
        reported_field_names.add("signature_date")
    _append_missing_field(
        missing,
        reported_field_names,
        "catalog_reference",
        agreement.catalog_reference,
        "Referencia al catálogo",
    )

    if payment_terms == "Crédito comercial aprobado":
        if agreement.contract_credit_days is None or agreement.contract_credit_days <= 0:
            missing.append("Los días de crédito contractual deben ser mayores a 0.")
            reported_field_names.add("contract_credit_days")
        if agreement.credit_limit is None or agreement.credit_limit <= 0:
            missing.append("El límite de crédito debe ser mayor a 0.")
            reported_field_names.add("credit_limit")

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
            field_name = _TEMPLATE_VARIABLE_FIELDS[variable_name]
            if field_name in reported_field_names:
                continue
            missing.append(_TEMPLATE_VARIABLE_LABELS[variable_name])
            reported_field_names.add(field_name)
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
        if not any(
            values[field_name]
            for field_name in ("name", "position", "phone", "email")
        ):
            continue
        if values["name"] is None:
            raise B2BCommercialAgreementValidationError(
                "El nombre es obligatorio en toda persona autorizada."
            )
        if values["phone"] is None and values["email"] is None:
            raise B2BCommercialAgreementValidationError(
                "Teléfono / correo es obligatorio en toda persona autorizada."
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


def _is_complete_authorized_person(person: B2BCommercialAgreementPerson) -> bool:
    return bool(
        (person.name or "").strip()
        and (person.position or "").strip()
        and ((person.phone or "").strip() or (person.email or "").strip())
    )


def _append_missing_field(
    missing: list[str],
    reported_field_names: set[str],
    field_name: str,
    value: Any,
    label: str,
) -> None:
    if value is None or not str(value).strip():
        missing.append(label)
        reported_field_names.add(field_name)


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
