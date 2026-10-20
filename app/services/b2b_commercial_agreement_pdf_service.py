from __future__ import annotations

import hashlib
import re
import unicodedata
from datetime import date, datetime
from decimal import Decimal
from html import escape
from io import BytesIO
from pathlib import Path
from typing import Callable

import reportlab
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    CondPageBreak,
    Image,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)
from sqlalchemy.orm import Session

from app.models import B2BCommercialAgreement
from app.services.b2b_commercial_agreement_service import (
    B2BCommercialAgreementValidationError,
    DEFAULT_CATALOG_REFERENCE,
    format_person_contact_info,
    get_commercial_agreement,
    render_agreement_template_text,
    validate_commercial_agreement,
)


_GREEN = colors.HexColor("#2F6B42")
_DARK_GREEN = colors.HexColor("#244D32")
_SOFT_GREEN = colors.HexColor("#EAF2EC")
_GRID = colors.HexColor("#C7D2C9")
_TEXT = colors.HexColor("#26332A")
_MUTED = colors.HexColor("#657169")


class B2BCommercialAgreementIncompleteError(Exception):
    def __init__(self, missing_fields: list[str]) -> None:
        self.missing_fields = list(missing_fields)
        super().__init__("El acuerdo tiene campos pendientes y no puede generar el PDF.")


class B2BCommercialAgreementPDFNotFoundError(Exception):
    pass


def safe_b2b_commercial_agreement_pdf_filename(
    customer_name: str | None,
    version_number: int,
) -> str:
    normalized_name = unicodedata.normalize("NFKD", customer_name or "")
    ascii_name = normalized_name.encode("ascii", "ignore").decode("ascii")
    safe_name = re.sub(r"[^A-Za-z0-9]+", "_", ascii_name).strip("_")
    return f"Acuerdo_Comercial_B2B_{safe_name or 'Cliente'}_v{version_number}.pdf"


def build_b2b_commercial_agreement_pdf(
    agreement: B2BCommercialAgreement,
    *,
    logo_bytes: bytes | None = None,
) -> bytes:
    regular_font, bold_font = _register_agreement_fonts()
    output = BytesIO()
    document = SimpleDocTemplate(
        output,
        pagesize=letter,
        leftMargin=0.62 * inch,
        rightMargin=0.62 * inch,
        topMargin=0.52 * inch,
        bottomMargin=0.58 * inch,
        title=(
            f"Acuerdo Comercial B2B - {agreement.customer_name_snapshot or 'Cliente'} "
            f"v{agreement.version_number}"
        ),
        author="Green Corner",
        subject="Acuerdo comercial B2B de compra directa",
    )

    styles = _agreement_styles(regular_font, bold_font)
    story: list[object] = []
    story.extend(_document_header(document, agreement, logo_bytes, styles))
    story.extend(
        _text_block(
            "Introducción",
            (
                "El presente Acuerdo Comercial B2B - Compra Directa establece las condiciones "
                "generales que regirán la relación comercial entre GREEN CORNER y EL CLIENTE. "
                "Las partes manifiestan su intención de actuar de buena fe y cumplir las condiciones "
                "comerciales, operativas y de pago aquí consignadas."
            ),
            styles,
        )
    )

    story.append(_section_heading("Información del cliente", styles))
    story.append(
        _details_table(
            [
                ("Nombre comercial", agreement.customer_name_snapshot),
                ("Razón social", agreement.legal_name_snapshot),
                ("Cédula jurídica", agreement.legal_id_snapshot),
                ("Representante legal", agreement.legal_representative_name),
                ("Número de identificación", agreement.legal_representative_id),
                ("Cargo", agreement.legal_representative_position),
                ("Dirección del establecimiento", agreement.address_snapshot),
                ("Dirección para facturación", agreement.billing_address),
                ("Correo para facturación electrónica", agreement.billing_email),
                ("Correo administrativo", agreement.administrative_email),
                ("Teléfono", agreement.phone_snapshot),
                ("WhatsApp", agreement.whatsapp),
            ],
            document.width,
            styles,
        )
    )
    story.append(Spacer(1, 0.16 * inch))

    story.append(_section_heading("Condiciones comerciales", styles))
    story.append(
        _details_table(
            [
                (
                    "Catálogo o lista de precios aplicable",
                    agreement.catalog_reference or DEFAULT_CATALOG_REFERENCE,
                ),
                ("Descuento comercial B2B asignado", _format_percent(agreement.b2b_discount_percent)),
                ("Forma de pago autorizada", agreement.payment_terms),
                (
                    "Plazo de crédito",
                    f"{agreement.contract_credit_days} días"
                    if agreement.credit_approved and agreement.contract_credit_days is not None
                    else "No aplica",
                ),
                (
                    "Límite máximo de crédito",
                    _format_currency(agreement.credit_limit)
                    if agreement.credit_approved and agreement.credit_limit is not None
                    else "No aplica",
                ),
                ("Días establecidos para recepción de pedidos", agreement.order_reception_days),
                ("Tiempo habitual de alisto", agreement.usual_prep_time),
                ("Entrega habitual", agreement.usual_delivery_type),
            ],
            document.width,
            styles,
        )
    )
    story.append(Spacer(1, 0.16 * inch))

    order_people = [
        person for person in agreement.authorized_people if person.authorization_type == "orders"
    ]
    receiving_people = [
        person for person in agreement.authorized_people if person.authorization_type == "receiving"
    ]
    story.append(_section_heading("Personas autorizadas para realizar pedidos", styles))
    story.append(_people_table(order_people, document.width, styles))
    story.append(Spacer(1, 0.15 * inch))
    story.append(_section_heading("Personas autorizadas para recibir mercadería", styles))
    story.append(_people_table(receiving_people, document.width, styles))
    story.append(Spacer(1, 0.22 * inch))

    story.append(_section_heading("Apartados contractuales", styles))
    for section in sorted(
        agreement.sections,
        key=lambda item: (item.display_order, item.section_number, item.id),
    ):
        story.append(CondPageBreak(0.85 * inch))
        story.append(
            Paragraph(
                f"{section.section_number}. {escape(section.title or '')}",
                styles["contract_heading"],
            )
        )
        rendered_text = render_agreement_template_text(section.body_text, agreement)
        story.extend(_body_paragraphs(rendered_text, styles["body"]))
        story.append(Spacer(1, 0.1 * inch))

    story.append(CondPageBreak(2.55 * inch))
    story.append(_section_heading("Aceptación y firmas", styles))
    story.append(
        Paragraph(
            (
                f"Firmado en {escape(agreement.signature_place or '')}, Costa Rica, "
                f"el {escape(_format_date(agreement.signature_date))}."
            ),
            styles["body"],
        )
    )
    story.append(Spacer(1, 0.16 * inch))
    story.append(_signature_table(agreement, document.width, styles))

    footer = _page_footer(regular_font, agreement.version_number)
    document.build(story, onFirstPage=footer, onLaterPages=footer)
    return output.getvalue()


def generate_and_store_b2b_commercial_agreement_pdf(
    db: Session,
    *,
    agreement_id: int,
    user_id: int | None,
    logo_bytes: bytes | None = None,
    pdf_builder: Callable[..., bytes] = build_b2b_commercial_agreement_pdf,
) -> B2BCommercialAgreement:
    agreement = get_commercial_agreement(db, agreement_id)
    if agreement.status != "draft":
        raise B2BCommercialAgreementValidationError(
            "Solo los acuerdos comerciales en estado draft pueden generar el PDF."
        )
    if any(
        value is not None
        for value in (
            agreement.generated_pdf_bytes,
            agreement.generated_pdf_filename,
            agreement.generated_pdf_sha256,
            agreement.generated_at,
            agreement.generated_by_user_id,
        )
    ):
        raise B2BCommercialAgreementValidationError(
            "El borrador conserva referencias de un PDF activo y no puede sobrescribirse."
        )

    missing_fields = validate_commercial_agreement(agreement)
    if missing_fields:
        raise B2BCommercialAgreementIncompleteError(missing_fields)

    try:
        pdf_bytes = pdf_builder(agreement, logo_bytes=logo_bytes)
        if not isinstance(pdf_bytes, bytes) or not pdf_bytes.startswith(b"%PDF"):
            raise ValueError("El generador no produjo un PDF válido.")
        generated_at = datetime.utcnow()
        agreement.generated_pdf_bytes = pdf_bytes
        agreement.generated_pdf_filename = safe_b2b_commercial_agreement_pdf_filename(
            agreement.customer_name_snapshot,
            agreement.version_number,
        )
        agreement.generated_pdf_sha256 = hashlib.sha256(pdf_bytes).hexdigest()
        agreement.generated_at = generated_at
        agreement.generated_by_user_id = user_id
        agreement.updated_at = generated_at
        agreement.updated_by_user_id = user_id
        agreement.status = "generated"
        db.commit()
    except Exception:
        db.rollback()
        raise

    return get_commercial_agreement(db, agreement_id)


def get_stored_b2b_commercial_agreement_pdf(
    agreement: B2BCommercialAgreement,
) -> tuple[bytes, str]:
    if not agreement.generated_pdf_bytes:
        raise B2BCommercialAgreementPDFNotFoundError(
            "Este acuerdo todavía no tiene un PDF generado."
        )
    filename = agreement.generated_pdf_filename or safe_b2b_commercial_agreement_pdf_filename(
        agreement.customer_name_snapshot,
        agreement.version_number,
    )
    if any(character in filename for character in '\r\n/\\:"<>|'):
        filename = safe_b2b_commercial_agreement_pdf_filename(
            agreement.customer_name_snapshot,
            agreement.version_number,
        )
    return bytes(agreement.generated_pdf_bytes), filename


def _document_header(document, agreement, logo_bytes, styles) -> list[object]:
    logo = _build_logo_flowable(logo_bytes)
    title = Paragraph(
        "ACUERDO COMERCIAL B2B - COMPRA DIRECTA",
        styles["title"],
    )
    subtitle = Paragraph("GREEN CORNER | ATENCIÓN A NEGOCIOS", styles["subtitle"])
    client = Paragraph(
        (
            f"<b>Cliente:</b> {escape(agreement.customer_name_snapshot or '')}<br/>"
            f"<b>Versión:</b> {agreement.version_number} &nbsp; "
            f"<b>Fecha:</b> {escape(_format_date(agreement.signature_date))}"
        ),
        styles["meta"],
    )
    header = Table(
        [[[title, subtitle, Spacer(1, 0.06 * inch), client], logo or ""]],
        colWidths=[document.width - 1.32 * inch, 1.32 * inch],
    )
    header.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("ALIGN", (1, 0), (1, 0), "RIGHT"),
                ("LEFTPADDING", (0, 0), (-1, -1), 0),
                ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                ("TOPPADDING", (0, 0), (-1, -1), 0),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
                ("LINEBELOW", (0, 0), (-1, -1), 1.1, _GREEN),
            ]
        )
    )
    return [header, Spacer(1, 0.16 * inch)]


def _agreement_styles(regular_font: str, bold_font: str) -> dict[str, ParagraphStyle]:
    sample = getSampleStyleSheet()
    body = ParagraphStyle(
        "AgreementBody",
        parent=sample["BodyText"],
        fontName=regular_font,
        fontSize=8.2,
        leading=11.1,
        textColor=_TEXT,
        spaceAfter=5,
    )
    return {
        "title": ParagraphStyle(
            "AgreementTitle",
            parent=sample["Title"],
            fontName=bold_font,
            fontSize=15.5,
            leading=18,
            textColor=_GREEN,
            alignment=0,
            spaceAfter=2,
        ),
        "subtitle": ParagraphStyle(
            "AgreementSubtitle",
            parent=body,
            fontName=bold_font,
            fontSize=9.3,
            textColor=_DARK_GREEN,
            spaceAfter=1,
        ),
        "meta": ParagraphStyle(
            "AgreementMeta",
            parent=body,
            fontSize=8.3,
            leading=10.5,
        ),
        "section": ParagraphStyle(
            "AgreementSection",
            parent=sample["Heading2"],
            fontName=bold_font,
            fontSize=10.3,
            leading=13,
            textColor=_GREEN,
            spaceBefore=2,
            spaceAfter=5,
        ),
        "contract_heading": ParagraphStyle(
            "AgreementContractHeading",
            parent=sample["Heading3"],
            fontName=bold_font,
            fontSize=9.1,
            leading=11.5,
            textColor=_DARK_GREEN,
            spaceBefore=3,
            spaceAfter=4,
        ),
        "body": body,
        "label": ParagraphStyle(
            "AgreementLabel",
            parent=body,
            fontName=bold_font,
            fontSize=7.7,
            leading=9.5,
            textColor=_DARK_GREEN,
        ),
        "table": ParagraphStyle(
            "AgreementTable",
            parent=body,
            fontSize=7.7,
            leading=9.5,
        ),
        "table_header": ParagraphStyle(
            "AgreementTableHeader",
            parent=body,
            fontName=bold_font,
            fontSize=7.6,
            leading=9.5,
            textColor=colors.white,
            alignment=TA_CENTER,
        ),
        "signature": ParagraphStyle(
            "AgreementSignature",
            parent=body,
            fontSize=7.7,
            leading=10,
        ),
    }


def _text_block(title: str, text: str, styles) -> list[object]:
    return [_section_heading(title, styles), *_body_paragraphs(text, styles["body"])]


def _section_heading(title: str, styles) -> Paragraph:
    return Paragraph(escape(title), styles["section"])


def _body_paragraphs(text: str | None, style: ParagraphStyle) -> list[Paragraph]:
    source = str(text or "").strip()
    if not source:
        return [Paragraph("", style)]
    paragraphs = [part.strip() for part in re.split(r"\n\s*\n", source) if part.strip()]
    return [Paragraph(escape(part).replace("\n", "<br/>"), style) for part in paragraphs]


def _details_table(items, width: float, styles) -> Table:
    rows = []
    for index in range(0, len(items), 2):
        pair = list(items[index : index + 2])
        while len(pair) < 2:
            pair.append(("", ""))
        row = []
        for label, value in pair:
            row.extend(
                [
                    Paragraph(escape(str(label)), styles["label"]) if label else "",
                    Paragraph(escape(_display(value)), styles["table"]) if label else "",
                ]
            )
        rows.append(row)
    table = Table(rows, colWidths=[width * 0.17, width * 0.33, width * 0.17, width * 0.33])
    table.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("GRID", (0, 0), (-1, -1), 0.3, _GRID),
                ("BACKGROUND", (0, 0), (-1, -1), colors.white),
                ("BACKGROUND", (0, 0), (0, -1), _SOFT_GREEN),
                ("BACKGROUND", (2, 0), (2, -1), _SOFT_GREEN),
                ("LEFTPADDING", (0, 0), (-1, -1), 4),
                ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    return table


def _people_table(people, width: float, styles) -> Table:
    rows = [
        [
            Paragraph("Nombre", styles["table_header"]),
            Paragraph("Cargo", styles["table_header"]),
            Paragraph("Teléfono / correo", styles["table_header"]),
        ]
    ]
    for person in people:
        rows.append(
            [
                Paragraph(escape(person.name or ""), styles["table"]),
                Paragraph(escape(person.position or ""), styles["table"]),
                Paragraph(
                    escape(format_person_contact_info(person.phone, person.email)),
                    styles["table"],
                ),
            ]
        )
    if not people:
        rows.append([Paragraph("No se registraron personas autorizadas.", styles["table"]), "", ""])
    table = Table(rows, colWidths=[width * 0.34, width * 0.25, width * 0.41], repeatRows=1)
    table_style = [
        ("BACKGROUND", (0, 0), (-1, 0), _GREEN),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("GRID", (0, 0), (-1, -1), 0.3, _GRID),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, _SOFT_GREEN]),
    ]
    if not people:
        table_style.append(("SPAN", (0, 1), (-1, 1)))
    table.setStyle(TableStyle(table_style))
    return table


def _signature_table(agreement, width: float, styles) -> Table:
    green_corner = (
        "<b>GREEN CORNER</b><br/>"
        "Razón social: Esquina Verde Productos Naturales SRL<br/>"
        "Cédula jurídica: 3102848795<br/>"
        "Representante legal: Jonathan Quirós<br/>"
        "Identificación representante legal: 1 1359 0900<br/>"
        "Cargo: Gerente Comercial<br/><br/><br/>"
        "Firma: _______________________________"
    )
    client = (
        "<b>EL CLIENTE</b><br/>"
        f"Razón social: {escape(agreement.legal_name_snapshot or '')}<br/>"
        f"Nombre comercial: {escape(agreement.customer_name_snapshot or '')}<br/>"
        f"Representante legal: {escape(agreement.legal_representative_name or '')}<br/>"
        f"Identificación: {escape(agreement.legal_representative_id or '')}<br/>"
        f"Cargo: {escape(agreement.legal_representative_position or '')}<br/><br/><br/>"
        "Firma: _______________________________"
    )
    table = Table(
        [[Paragraph(green_corner, styles["signature"]), Paragraph(client, styles["signature"])]],
        colWidths=[width / 2, width / 2],
    )
    table.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("BOX", (0, 0), (-1, -1), 0.4, _GRID),
                ("INNERGRID", (0, 0), (-1, -1), 0.4, _GRID),
                ("LEFTPADDING", (0, 0), (-1, -1), 7),
                ("RIGHTPADDING", (0, 0), (-1, -1), 7),
                ("TOPPADDING", (0, 0), (-1, -1), 7),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
            ]
        )
    )
    return table


def _register_agreement_fonts() -> tuple[str, str]:
    try:
        import font_roboto

        if "B2BAgreement" not in pdfmetrics.getRegisteredFontNames():
            pdfmetrics.registerFont(TTFont("B2BAgreement", font_roboto.Roboto))
        if "B2BAgreement-Bold" not in pdfmetrics.getRegisteredFontNames():
            pdfmetrics.registerFont(TTFont("B2BAgreement-Bold", font_roboto.RobotoBold))
        return "B2BAgreement", "B2BAgreement-Bold"
    except Exception:
        pass

    font_dir = Path(reportlab.__file__).resolve().parent / "fonts"
    try:
        if "B2BAgreement" not in pdfmetrics.getRegisteredFontNames():
            pdfmetrics.registerFont(TTFont("B2BAgreement", str(font_dir / "Vera.ttf")))
        if "B2BAgreement-Bold" not in pdfmetrics.getRegisteredFontNames():
            pdfmetrics.registerFont(TTFont("B2BAgreement-Bold", str(font_dir / "VeraBd.ttf")))
        return "B2BAgreement", "B2BAgreement-Bold"
    except Exception:
        return "Helvetica", "Helvetica-Bold"


def _build_logo_flowable(logo_bytes: bytes | None) -> Image | None:
    if not logo_bytes:
        return None
    try:
        image = Image(BytesIO(logo_bytes))
        image._restrictSize(1.12 * inch, 0.68 * inch)
        return image
    except Exception:
        return None


def _page_footer(font_name: str, version_number: int):
    def draw_footer(canvas, document) -> None:
        canvas.saveState()
        canvas.setStrokeColor(_GRID)
        canvas.line(document.leftMargin, 0.38 * inch, letter[0] - document.rightMargin, 0.38 * inch)
        canvas.setFont(font_name, 7)
        canvas.setFillColor(_MUTED)
        canvas.drawCentredString(
            letter[0] / 2,
            0.22 * inch,
            f"Green Corner | Versión {version_number} | Página {canvas.getPageNumber()}",
        )
        canvas.restoreState()

    return draw_footer


def _display(value: object) -> str:
    if value is None:
        return ""
    return str(value)


def _format_percent(value: Decimal | None) -> str:
    if value is None:
        return ""
    normalized = value.normalize()
    rendered = (
        str(int(normalized))
        if normalized == normalized.to_integral()
        else format(normalized, "f").rstrip("0").rstrip(".")
    )
    return f"{rendered}%"


def _format_currency(value: Decimal | None) -> str:
    if value is None:
        return ""
    return f"₡{value:,.2f}".rstrip("0").rstrip(".")


def _format_date(value: date | None) -> str:
    if value is None:
        return ""
    month_names = (
        "enero",
        "febrero",
        "marzo",
        "abril",
        "mayo",
        "junio",
        "julio",
        "agosto",
        "septiembre",
        "octubre",
        "noviembre",
        "diciembre",
    )
    return f"{value.day} de {month_names[value.month - 1]} de {value.year}"
