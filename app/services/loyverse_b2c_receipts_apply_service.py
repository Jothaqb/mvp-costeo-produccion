from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP

from sqlalchemy.orm import Session

from app.models import B2CCustomer, B2CSalesOrder, B2CSalesOrderLine, LoyverseCustomerMapping, Product, User
from app.services.loyverse_b2c_receipts_import_service import (
    LoyverseB2CReceiptsPreviewResult,
    LoyverseReceiptLinePreview,
    LoyverseReceiptPreview,
    build_loyverse_b2c_receipts_preview,
)


DECIMAL_QUANT = Decimal("0.0001")
ZERO = Decimal("0")
LOYVERSE_SOURCE = "loyverse_b2c_l1b2"


class LoyverseB2CReceiptApplyError(Exception):
    pass


@dataclass(frozen=True)
class LoyverseB2CImportedReceipt:
    order_id: int
    order_number: str
    receipt_number: str
    total_amount: Decimal


@dataclass(frozen=True)
class LoyverseB2CSkippedReceipt:
    receipt_number: str
    reason: str
    blockers: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class LoyverseB2CApplyResult:
    start_datetime: datetime
    end_datetime: datetime
    receipts_read: int
    imported_receipts: list[LoyverseB2CImportedReceipt]
    skipped_duplicates: list[LoyverseB2CSkippedReceipt]
    skipped_blocked: list[LoyverseB2CSkippedReceipt]
    excluded_refunds_count: int
    excluded_cancelled_count: int
    warning_count: int
    total_imported: Decimal
    preview: LoyverseB2CReceiptsPreviewResult

    @property
    def imported_count(self) -> int:
        return len(self.imported_receipts)

    @property
    def skipped_duplicate_count(self) -> int:
        return len(self.skipped_duplicates)

    @property
    def skipped_blocked_count(self) -> int:
        return len(self.skipped_blocked)

    @property
    def imported_receipt_numbers(self) -> list[str]:
        return [receipt.receipt_number for receipt in self.imported_receipts]


def apply_loyverse_b2c_receipts_reporting_only(
    db: Session,
    start_datetime: datetime,
    end_datetime: datetime,
    acting_user: User | None,
) -> LoyverseB2CApplyResult:
    preview = _rebuild_preview_for_apply(db, start_datetime, end_datetime)
    receipts_for_apply = _select_receipts_for_apply(preview)
    imported: list[LoyverseB2CImportedReceipt] = []
    skipped_duplicates: list[LoyverseB2CSkippedReceipt] = []
    skipped_blocked: list[LoyverseB2CSkippedReceipt] = []

    try:
        for receipt in receipts_for_apply:
            revalidation = _revalidate_receipt_before_apply(db, receipt)
            if revalidation["duplicate"]:
                skipped_duplicates.append(
                    LoyverseB2CSkippedReceipt(
                        receipt_number=receipt.receipt_number or "N/A",
                        reason=str(revalidation["reason"]),
                        blockers=[],
                        warnings=list(receipt.warnings),
                    )
                )
                continue
            if revalidation["blocked"]:
                skipped_blocked.append(
                    LoyverseB2CSkippedReceipt(
                        receipt_number=receipt.receipt_number or "N/A",
                        reason=str(revalidation["reason"]),
                        blockers=list(receipt.blockers),
                        warnings=list(receipt.warnings),
                    )
                )
                continue

            resolved_lines = _resolve_lines_for_apply(db, receipt)
            order = _create_b2c_sales_order_from_receipt(db, receipt, resolved_lines, acting_user)
            _create_b2c_sales_order_lines_from_receipt(db, order, receipt, resolved_lines)
            _snapshot_reporting_only_margin(order)
            db.flush()
            imported.append(
                LoyverseB2CImportedReceipt(
                    order_id=order.id,
                    order_number=order.order_number,
                    receipt_number=receipt.receipt_number,
                    total_amount=_money(receipt.net_total),
                )
            )

        db.commit()
    except Exception:
        db.rollback()
        raise

    return _summarize_apply_result(
        preview=preview,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        imported=imported,
        skipped_duplicates=skipped_duplicates,
        skipped_blocked=skipped_blocked,
    )


def _rebuild_preview_for_apply(
    db: Session,
    start_datetime: datetime,
    end_datetime: datetime,
) -> LoyverseB2CReceiptsPreviewResult:
    return build_loyverse_b2c_receipts_preview(db, start_datetime, end_datetime)


def _select_receipts_for_apply(preview: LoyverseB2CReceiptsPreviewResult) -> list[LoyverseReceiptPreview]:
    return [receipt for receipt in preview.receipts if receipt.receipt_kind == "sale" and receipt.importable]


def _revalidate_receipt_before_apply(db: Session, receipt: LoyverseReceiptPreview) -> dict[str, object]:
    if receipt.receipt_kind != "sale":
        return {"duplicate": False, "blocked": True, "reason": f"Receipt kind {receipt.receipt_kind} is excluded."}
    if receipt.blockers:
        return {"duplicate": False, "blocked": True, "reason": "Receipt has blockers."}
    if not receipt.receipt_number:
        return {"duplicate": False, "blocked": True, "reason": "Receipt is missing receipt_number."}
    if _existing_b2c_order_for_receipt(db, receipt) is not None:
        return {"duplicate": True, "blocked": False, "reason": "Receipt was already imported."}
    if any(line.product_id is None for line in receipt.lines):
        return {"duplicate": False, "blocked": True, "reason": "One or more lines do not have a resolved ERP product."}
    return {"duplicate": False, "blocked": False, "reason": ""}


def _create_b2c_sales_order_from_receipt(
    db: Session,
    receipt: LoyverseReceiptPreview,
    resolved_lines: list[tuple[LoyverseReceiptLinePreview, Product]],
    acting_user: User | None,
) -> B2CSalesOrder:
    del resolved_lines, acting_user
    customer_snapshot = _build_b2c_customer_snapshot(db, receipt)
    payment_snapshot = _build_payment_type_snapshot(receipt)
    order = B2CSalesOrder(
        order_number=_build_order_number_from_receipt(receipt.receipt_number),
        order_date=_receipt_order_date(receipt),
        b2c_customer_id=customer_snapshot["b2c_customer_id"],
        customer_name=customer_snapshot["customer_name"],
        customer_phone=customer_snapshot["customer_phone"],
        customer_email=None,
        customer_address_snapshot=None,
        province_snapshot=None,
        canton_snapshot=None,
        district_snapshot=None,
        customer_observations_snapshot=None,
        channel="Loyverse B2C",
        channel_id=None,
        status="invoiced",
        subtotal_amount=_money(receipt.gross_total),
        discount_amount=_money(receipt.discount_total),
        total_amount=_money(receipt.net_total),
        observations=(
            f"Loyverse API reporting-only import. Receipt {receipt.receipt_number}. "
            f"Payment type: {payment_snapshot['payment_type_name'] or 'N/A'}."
        ),
        loyverse_receipt_id=_normalized_optional_loyverse_id(receipt.receipt_id) or None,
        loyverse_receipt_number=receipt.receipt_number or None,
        loyverse_receipt_date=receipt.receipt_date,
        loyverse_receipt_status=receipt.receipt_status_label,
        loyverse_payment_type_id_snapshot=payment_snapshot["payment_type_id"],
        loyverse_payment_type_name_snapshot=payment_snapshot["payment_type_name"],
        loyverse_source=LOYVERSE_SOURCE,
        loyverse_imported_at=datetime.utcnow(),
        loyverse_raw_payload_summary=receipt.raw_payload_summary,
    )
    db.add(order)
    db.flush()
    return order


def _create_b2c_sales_order_lines_from_receipt(
    db: Session,
    order: B2CSalesOrder,
    receipt: LoyverseReceiptPreview,
    resolved_lines: list[tuple[LoyverseReceiptLinePreview, Product]],
) -> None:
    del db, receipt
    for index, (line_preview, product) in enumerate(resolved_lines, start=1):
        quantity = _money(line_preview.quantity)
        gross_line_total = _money(line_preview.gross_line_total)
        if gross_line_total == ZERO and line_preview.quantity is not None and line_preview.unit_price is not None:
            gross_line_total = _money(line_preview.quantity * line_preview.unit_price)

        standard_cost = _optional_money(product.standard_cost)
        cost_total = _money(standard_cost * quantity) if standard_cost is not None else None
        net_line_total = _money(line_preview.net_line_total)
        gross_margin_amount = _money(net_line_total - cost_total) if cost_total is not None else None
        gross_margin_percent = (
            (gross_margin_amount / net_line_total).quantize(DECIMAL_QUANT)
            if gross_margin_amount is not None and net_line_total > ZERO
            else None
        )

        order.lines.append(
            B2CSalesOrderLine(
                line_number=index,
                sku_snapshot=product.sku,
                description_snapshot=product.name,
                quantity=quantity,
                unit_price_snapshot=_money(line_preview.unit_price),
                line_total=gross_line_total,
                discount_amount_snapshot=_money(line_preview.discount_amount),
                net_line_total_snapshot=net_line_total,
                cost_unit_snapshot=standard_cost,
                cost_total_snapshot=cost_total,
                gross_margin_amount=gross_margin_amount,
                gross_margin_percent=gross_margin_percent,
            )
        )


def _build_b2c_customer_snapshot(db: Session, receipt: LoyverseReceiptPreview) -> dict[str, object]:
    customer_name = (receipt.customer_name or "").strip() or "Loyverse Customer N/A"
    customer_phone = (receipt.customer_phone or "").strip() or None
    b2c_customer_id = None
    loyverse_customer_id = (receipt.customer_id or "").strip()
    if loyverse_customer_id:
        mapping = (
            db.query(LoyverseCustomerMapping)
            .filter(
                LoyverseCustomerMapping.loyverse_customer_id == loyverse_customer_id,
                LoyverseCustomerMapping.active.is_(True),
            )
            .one_or_none()
        )
        if mapping is not None:
            customer = (
                db.query(B2CCustomer)
                .filter(B2CCustomer.source_customer_mapping_id == mapping.id, B2CCustomer.active.is_(True))
                .one_or_none()
            )
            if customer is not None:
                b2c_customer_id = customer.id
                customer_name = customer.name
                customer_phone = customer.phone or customer_phone
    return {"b2c_customer_id": b2c_customer_id, "customer_name": customer_name, "customer_phone": customer_phone}


def _build_payment_type_snapshot(receipt: LoyverseReceiptPreview) -> dict[str, str | None]:
    payment_type_id = (receipt.payment_type_id or "").strip() or None
    payment_type_name = (receipt.payment_type_name or "").strip() or payment_type_id
    return {"payment_type_id": payment_type_id, "payment_type_name": payment_type_name}


def _build_order_number_from_receipt(receipt_number: str) -> str:
    return f"LOY-{receipt_number.strip()}"


def _summarize_apply_result(
    *,
    preview: LoyverseB2CReceiptsPreviewResult,
    start_datetime: datetime,
    end_datetime: datetime,
    imported: list[LoyverseB2CImportedReceipt],
    skipped_duplicates: list[LoyverseB2CSkippedReceipt],
    skipped_blocked: list[LoyverseB2CSkippedReceipt],
) -> LoyverseB2CApplyResult:
    total_imported = sum((receipt.total_amount for receipt in imported), ZERO).quantize(DECIMAL_QUANT)
    return LoyverseB2CApplyResult(
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        receipts_read=preview.summary.receipts_read,
        imported_receipts=imported,
        skipped_duplicates=skipped_duplicates,
        skipped_blocked=skipped_blocked,
        excluded_refunds_count=preview.summary.excluded_refund_count,
        excluded_cancelled_count=preview.summary.excluded_cancelled_count,
        warning_count=preview.summary.receipts_with_warnings_count,
        total_imported=total_imported,
        preview=preview,
    )


def _resolve_lines_for_apply(
    db: Session,
    receipt: LoyverseReceiptPreview,
) -> list[tuple[LoyverseReceiptLinePreview, Product]]:
    resolved_lines: list[tuple[LoyverseReceiptLinePreview, Product]] = []
    for line in receipt.lines:
        if line.product_id is None:
            raise LoyverseB2CReceiptApplyError(
                f"Receipt {receipt.receipt_number} line {line.line_number} does not have a resolved product."
            )
        product = db.query(Product).filter(Product.id == line.product_id).one_or_none()
        if product is None:
            raise LoyverseB2CReceiptApplyError(
                f"Receipt {receipt.receipt_number} line {line.line_number} product {line.product_id} no longer exists."
            )
        resolved_lines.append((line, product))
    if not resolved_lines:
        raise LoyverseB2CReceiptApplyError(f"Receipt {receipt.receipt_number} has no lines to import.")
    return resolved_lines


def _existing_b2c_order_for_receipt(db: Session, receipt: LoyverseReceiptPreview) -> int | None:
    receipt_number = (receipt.receipt_number or "").strip()
    receipt_id = _normalized_optional_loyverse_id(receipt.receipt_id)
    checks = []
    if receipt_number:
        checks.append(B2CSalesOrder.loyverse_receipt_number == receipt_number)
    if receipt_id:
        checks.append(B2CSalesOrder.loyverse_receipt_id == receipt_id)
    if receipt_number:
        checks.append(B2CSalesOrder.order_number == receipt_number)
        checks.append(B2CSalesOrder.order_number == _build_order_number_from_receipt(receipt_number))
    for criterion in checks:
        existing = db.query(B2CSalesOrder.id).filter(criterion).first()
        if existing is not None:
            return existing[0]
    return None


def _receipt_order_date(receipt: LoyverseReceiptPreview):
    source_datetime = receipt.receipt_date or receipt.created_at or datetime.utcnow()
    return source_datetime.date()


def _snapshot_reporting_only_margin(order: B2CSalesOrder) -> None:
    if any(line.cost_total_snapshot is None for line in order.lines):
        order.cost_total_snapshot = None
        order.gross_margin_amount = None
        order.gross_margin_percent = None
        return
    cost_total = sum((line.cost_total_snapshot or ZERO for line in order.lines), ZERO).quantize(DECIMAL_QUANT)
    total_amount = _money(order.total_amount)
    gross_margin_amount = (total_amount - cost_total).quantize(DECIMAL_QUANT)
    gross_margin_percent = (gross_margin_amount / total_amount).quantize(DECIMAL_QUANT) if total_amount > ZERO else None
    order.cost_total_snapshot = cost_total
    order.gross_margin_amount = gross_margin_amount
    order.gross_margin_percent = gross_margin_percent


def _normalized_optional_loyverse_id(value: object) -> str:
    text = str(value or "").strip()
    if not text or text.upper() == "N/A":
        return ""
    return text


def _money(value: Decimal | int | str | None) -> Decimal:
    if value is None:
        return ZERO
    return Decimal(str(value)).quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP)


def _optional_money(value: Decimal | int | str | None) -> Decimal | None:
    if value is None:
        return None
    return _money(value)
