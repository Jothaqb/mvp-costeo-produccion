from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from html import escape
from io import BytesIO
from pathlib import Path
from typing import Iterable

import reportlab
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_RIGHT
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import Image, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from app.models import B2BCustomer, B2BCustomerProduct


_GREEN = colors.HexColor("#2F6B42")
_SOFT_GREEN = colors.HexColor("#EAF2EC")
_GRID = colors.HexColor("#CBD5CC")
_TEXT = colors.HexColor("#26332A")


def format_crc_catalog_price(value: Decimal | int | float | str | None) -> str:
    try:
        amount = Decimal(str(value or 0)).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError):
        amount = Decimal("0")
    return f"₡{amount:,.0f}"


def build_b2b_customer_catalog_pdf(
    customer: B2BCustomer,
    products: Iterable[B2BCustomerProduct],
    *,
    logo_bytes: bytes | None = None,
) -> bytes:
    regular_font, bold_font = _register_catalog_fonts()
    output = BytesIO()
    document = SimpleDocTemplate(
        output,
        pagesize=letter,
        rightMargin=0.55 * inch,
        leftMargin=0.55 * inch,
        topMargin=0.5 * inch,
        bottomMargin=0.5 * inch,
        title=f"Catálogo: {customer.customer_name}",
        author="Green Corner",
    )

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "B2BCatalogTitle",
        parent=styles["Title"],
        fontName=bold_font,
        fontSize=19,
        leading=23,
        textColor=_GREEN,
        alignment=0,
        spaceAfter=0,
    )
    field_style = ParagraphStyle(
        "B2BCatalogField",
        parent=styles["BodyText"],
        fontName=regular_font,
        fontSize=8.5,
        leading=11,
        textColor=_TEXT,
    )
    header_style = ParagraphStyle(
        "B2BCatalogHeader",
        parent=field_style,
        fontName=bold_font,
        textColor=colors.white,
        alignment=TA_CENTER,
    )
    price_style = ParagraphStyle(
        "B2BCatalogPrice",
        parent=field_style,
        alignment=TA_RIGHT,
    )
    status_style = ParagraphStyle(
        "B2BCatalogStatus",
        parent=field_style,
        alignment=TA_CENTER,
    )

    logo = _build_logo_flowable(logo_bytes)
    header = Table(
        [[Paragraph(f"Catálogo: {escape(customer.customer_name)}", title_style), logo or ""]],
        colWidths=[document.width - 1.25 * inch, 1.25 * inch],
        rowHeights=[0.6 * inch],
    )
    header.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("ALIGN", (1, 0), (1, 0), "RIGHT"),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ("LINEBELOW", (0, 0), (-1, -1), 1, _GREEN),
            ]
        )
    )

    channel_name = customer.channel_record.name if customer.channel_record is not None else ""
    customer_rows = [
        (_field("Customer name", customer.customer_name, field_style, bold_font), _field("Legal name", customer.legal_name, field_style, bold_font)),
        (_field("Legal ID", customer.legal_id, field_style, bold_font), _field("Phone", customer.phone, field_style, bold_font)),
        (_field("Address", customer.address, field_style, bold_font), _field("B2B Channel", channel_name, field_style, bold_font)),
        (_field("Province", customer.province, field_style, bold_font), _field("Credit days", customer.credit_days, field_style, bold_font)),
        (_field("Canton", customer.canton, field_style, bold_font), _field("Active", "yes" if customer.active else "no", field_style, bold_font)),
        (_field("District", customer.district, field_style, bold_font), ""),
    ]
    customer_table = Table(customer_rows, colWidths=[document.width / 2, document.width / 2])
    customer_table.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#F7FAF7")),
                ("BOX", (0, 0), (-1, -1), 0.5, _GRID),
                ("INNERGRID", (0, 0), (-1, -1), 0.25, _GRID),
            ]
        )
    )

    product_rows = [
        [
            Paragraph("Código", header_style),
            Paragraph("Descripción", header_style),
            Paragraph("Precio", header_style),
            Paragraph("Estado", header_style),
        ]
    ]
    product_list = list(products)
    for product in product_list:
        product_rows.append(
            [
                Paragraph(escape(product.sku or ""), field_style),
                Paragraph(escape(product.description or ""), field_style),
                Paragraph(format_crc_catalog_price(product.distributor_price), price_style),
                Paragraph("Activo" if product.active else "Inactivo", status_style),
            ]
        )
    if not product_list:
        product_rows.append([Paragraph("No hay productos en el catálogo.", field_style), "", "", ""])

    product_table = Table(
        product_rows,
        colWidths=[0.85 * inch, document.width - 2.75 * inch, 1.05 * inch, 0.85 * inch],
        repeatRows=1,
    )
    product_style = [
        ("BACKGROUND", (0, 0), (-1, 0), _GREEN),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("GRID", (0, 0), (-1, -1), 0.35, _GRID),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, _SOFT_GREEN]),
    ]
    if not product_list:
        product_style.extend(
            [
                ("SPAN", (0, 1), (-1, 1)),
                ("ALIGN", (0, 1), (-1, 1), "CENTER"),
            ]
        )
    product_table.setStyle(TableStyle(product_style))

    story = [header, Spacer(1, 0.18 * inch), customer_table, Spacer(1, 0.28 * inch), product_table]
    footer = _page_footer(regular_font)
    document.build(story, onFirstPage=footer, onLaterPages=footer)
    return output.getvalue()


def _field(label: str, value: object, style: ParagraphStyle, bold_font: str) -> Paragraph:
    text = "" if value is None else str(value)
    return Paragraph(f'<font name="{bold_font}">{escape(label)}:</font> {escape(text)}', style)


def _register_catalog_fonts() -> tuple[str, str]:
    try:
        import font_roboto

        if "B2BCatalog" not in pdfmetrics.getRegisteredFontNames():
            pdfmetrics.registerFont(TTFont("B2BCatalog", font_roboto.Roboto))
        if "B2BCatalog-Bold" not in pdfmetrics.getRegisteredFontNames():
            pdfmetrics.registerFont(TTFont("B2BCatalog-Bold", font_roboto.RobotoBold))
        return "B2BCatalog", "B2BCatalog-Bold"
    except Exception:
        pass

    font_dir = Path(reportlab.__file__).resolve().parent / "fonts"
    try:
        if "B2BCatalog" not in pdfmetrics.getRegisteredFontNames():
            pdfmetrics.registerFont(TTFont("B2BCatalog", str(font_dir / "Vera.ttf")))
        if "B2BCatalog-Bold" not in pdfmetrics.getRegisteredFontNames():
            pdfmetrics.registerFont(TTFont("B2BCatalog-Bold", str(font_dir / "VeraBd.ttf")))
        return "B2BCatalog", "B2BCatalog-Bold"
    except Exception:
        return "Helvetica", "Helvetica-Bold"


def _build_logo_flowable(logo_bytes: bytes | None) -> Image | None:
    if not logo_bytes:
        return None
    try:
        image = Image(BytesIO(logo_bytes))
        image._restrictSize(1.1 * inch, 0.48 * inch)
        return image
    except Exception:
        return None


def _page_footer(font_name: str):
    def draw_footer(canvas, document) -> None:
        canvas.saveState()
        canvas.setStrokeColor(_GRID)
        canvas.line(document.leftMargin, 0.34 * inch, letter[0] - document.rightMargin, 0.34 * inch)
        canvas.setFont(font_name, 7)
        canvas.setFillColor(colors.HexColor("#66736A"))
        canvas.drawString(document.leftMargin, 0.2 * inch, "Green Corner")
        canvas.drawRightString(letter[0] - document.rightMargin, 0.2 * inch, f"Página {document.page}")
        canvas.restoreState()

    return draw_footer
