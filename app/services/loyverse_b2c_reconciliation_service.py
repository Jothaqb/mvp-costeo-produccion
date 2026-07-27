from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import or_
from sqlalchemy.orm import Session

from app.models import B2CSalesOrder
from app.services.loyverse_b2c_receipts_import_service import (
    DECIMAL_QUANT,
    LOYVERSE_TOTAL_TOLERANCE,
    ZERO,
    LoyverseB2CReceiptsPreviewResult,
    LoyverseReceiptPreview,
    build_loyverse_b2c_receipts_preview,
)


@dataclass(frozen=True)
class LoyverseB2CReconciliationRow:
    receipt_number: str
    receipt_id: str
    receipt_date: datetime | None
    created_at: datetime | None
    customer_name: str
    payment_type_name: str
    loyverse_net_total: Decimal
    erp_order_ids: list[int]
    erp_order_numbers: list[str]
    erp_statuses: list[str]
    erp_total: Decimal
    difference: Decimal
    reconciliation_status: str
    notes: list[str]
    blockers: list[str]
    warnings: list[str]


@dataclass(frozen=True)
class LoyverseB2CReconciliationSummary:
    receipts_read: int
    erp_matched_orders: int
    missing_in_erp: int
    duplicate_erp_orders: int
    excluded_refunds: int
    excluded_cancelled: int
    total_loyverse_net: Decimal
    total_erp_matched_net: Decimal
    total_difference: Decimal
    receipts_with_blockers: int
    receipts_with_warnings: int


@dataclass(frozen=True)
class LoyverseB2CReconciliationDaySummary:
    day: date | None
    loyverse_receipts: int
    erp_matched: int
    missing_erp: int
    refunds: int
    cancelled: int
    loyverse_net_total: Decimal
    erp_matched_total: Decimal
    difference: Decimal


@dataclass(frozen=True)
class LoyverseB2CReconciliationPaymentTypeSummary:
    payment_type: str
    loyverse_receipts: int
    erp_matched: int
    missing_erp: int
    loyverse_net_total: Decimal
    erp_matched_total: Decimal
    difference: Decimal


@dataclass(frozen=True)
class LoyverseB2CReconciliationResult:
    start_datetime: datetime
    end_datetime: datetime
    queried_with_store_id: str
    rows: list[LoyverseB2CReconciliationRow]
    summary: LoyverseB2CReconciliationSummary
    by_day: list[LoyverseB2CReconciliationDaySummary]
    by_payment_type: list[LoyverseB2CReconciliationPaymentTypeSummary]
    warnings: list[str]
    limitations: list[str]


def build_loyverse_b2c_reconciliation(
    db: Session,
    start_datetime: datetime,
    end_datetime: datetime,
) -> LoyverseB2CReconciliationResult:
    preview = build_loyverse_b2c_receipts_preview(db, start_datetime, end_datetime)
    rows = [
        _build_reconciliation_row(receipt, _match_loyverse_receipt_to_erp_orders(db, receipt))
        for receipt in preview.receipts
    ]
    return LoyverseB2CReconciliationResult(
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        queried_with_store_id=preview.queried_with_store_id,
        rows=rows,
        summary=_build_reconciliation_summary(preview, rows),
        by_day=_group_by_day(rows),
        by_payment_type=_group_by_payment_type(rows),
        warnings=preview.warnings,
        limitations=[
            "This reconciliation is read-only. It does not import receipts or change inventory.",
            "Loyverse API range filters use created_at. Day summaries use receipt_date when available, then created_at.",
            *preview.limitations,
        ],
    )


def _load_erp_b2c_orders_by_receipt_number(db: Session, receipt_number: str) -> list[B2CSalesOrder]:
    receipt_number = (receipt_number or "").strip()
    if not receipt_number:
        return []
    return (
        db.query(B2CSalesOrder)
        .filter(B2CSalesOrder.loyverse_receipt_number == receipt_number)
        .order_by(B2CSalesOrder.id)
        .all()
    )


def _load_erp_b2c_orders_by_receipt_id(db: Session, receipt_id: str) -> list[B2CSalesOrder]:
    receipt_id = _normalized_optional_loyverse_id(receipt_id)
    if not receipt_id:
        return []
    return (
        db.query(B2CSalesOrder)
        .filter(B2CSalesOrder.loyverse_receipt_id == receipt_id)
        .order_by(B2CSalesOrder.id)
        .all()
    )


def _match_loyverse_receipt_to_erp_orders(db: Session, receipt: LoyverseReceiptPreview) -> list[B2CSalesOrder]:
    receipt_number = (receipt.receipt_number or "").strip()
    receipt_id = _normalized_optional_loyverse_id(receipt.receipt_id)
    criteria = []
    if receipt_number:
        criteria.append(B2CSalesOrder.loyverse_receipt_number == receipt_number)
    if receipt_id:
        criteria.append(B2CSalesOrder.loyverse_receipt_id == receipt_id)
    if receipt_number:
        criteria.append(B2CSalesOrder.order_number == f"LOY-{receipt_number}")
        criteria.append(B2CSalesOrder.order_number == receipt_number)
    if not criteria:
        return []
    return db.query(B2CSalesOrder).filter(or_(*criteria)).order_by(B2CSalesOrder.id).all()


def _build_reconciliation_row(
    receipt: LoyverseReceiptPreview,
    erp_orders: list[B2CSalesOrder],
) -> LoyverseB2CReconciliationRow:
    erp_total = _sum_money(order.total_amount for order in erp_orders)
    difference = (receipt.net_total - erp_total).quantize(DECIMAL_QUANT)
    status = _reconciliation_status(receipt, erp_orders, difference)
    notes = _row_notes(receipt, erp_orders, difference, status)
    return LoyverseB2CReconciliationRow(
        receipt_number=receipt.receipt_number,
        receipt_id=receipt.receipt_id,
        receipt_date=receipt.receipt_date,
        created_at=receipt.created_at,
        customer_name=receipt.customer_name,
        payment_type_name=receipt.payment_type_name or receipt.payment_type_id or "N/A",
        loyverse_net_total=receipt.net_total,
        erp_order_ids=[order.id for order in erp_orders],
        erp_order_numbers=[order.order_number for order in erp_orders],
        erp_statuses=[order.status for order in erp_orders],
        erp_total=erp_total,
        difference=difference,
        reconciliation_status=status,
        notes=notes,
        blockers=list(receipt.blockers),
        warnings=list(receipt.warnings),
    )


def _build_reconciliation_summary(
    preview: LoyverseB2CReceiptsPreviewResult,
    rows: list[LoyverseB2CReconciliationRow],
) -> LoyverseB2CReconciliationSummary:
    erp_matched_rows = [row for row in rows if row.erp_order_numbers]
    sale_rows = [row for row in rows if row.reconciliation_status not in {"excluded_refund", "excluded_cancelled"}]
    total_loyverse_net = _sum_money(row.loyverse_net_total for row in sale_rows)
    total_erp_matched_net = _sum_money(row.erp_total for row in erp_matched_rows)
    return LoyverseB2CReconciliationSummary(
        receipts_read=preview.summary.receipts_read,
        erp_matched_orders=len(erp_matched_rows),
        missing_in_erp=sum(1 for row in rows if row.reconciliation_status == "missing_in_erp"),
        duplicate_erp_orders=sum(1 for row in rows if row.reconciliation_status == "duplicate_in_erp"),
        excluded_refunds=preview.summary.excluded_refund_count,
        excluded_cancelled=preview.summary.excluded_cancelled_count,
        total_loyverse_net=total_loyverse_net,
        total_erp_matched_net=total_erp_matched_net,
        total_difference=(total_loyverse_net - total_erp_matched_net).quantize(DECIMAL_QUANT),
        receipts_with_blockers=preview.summary.receipts_with_blockers_count,
        receipts_with_warnings=preview.summary.receipts_with_warnings_count,
    )


def _group_by_day(rows: list[LoyverseB2CReconciliationRow]) -> list[LoyverseB2CReconciliationDaySummary]:
    buckets: dict[date | None, list[LoyverseB2CReconciliationRow]] = {}
    for row in rows:
        buckets.setdefault(_commercial_day(row), []).append(row)
    summaries = []
    for day, day_rows in sorted(buckets.items(), key=lambda item: (item[0] is None, item[0] or date.min)):
        sale_rows = [row for row in day_rows if row.reconciliation_status not in {"excluded_refund", "excluded_cancelled"}]
        erp_rows = [row for row in day_rows if row.erp_order_numbers]
        loyverse_total = _sum_money(row.loyverse_net_total for row in sale_rows)
        erp_total = _sum_money(row.erp_total for row in erp_rows)
        summaries.append(
            LoyverseB2CReconciliationDaySummary(
                day=day,
                loyverse_receipts=len(day_rows),
                erp_matched=len(erp_rows),
                missing_erp=sum(1 for row in day_rows if row.reconciliation_status == "missing_in_erp"),
                refunds=sum(1 for row in day_rows if row.reconciliation_status == "excluded_refund"),
                cancelled=sum(1 for row in day_rows if row.reconciliation_status == "excluded_cancelled"),
                loyverse_net_total=loyverse_total,
                erp_matched_total=erp_total,
                difference=(loyverse_total - erp_total).quantize(DECIMAL_QUANT),
            )
        )
    return summaries


def _group_by_payment_type(rows: list[LoyverseB2CReconciliationRow]) -> list[LoyverseB2CReconciliationPaymentTypeSummary]:
    buckets: dict[str, list[LoyverseB2CReconciliationRow]] = {}
    for row in rows:
        buckets.setdefault(row.payment_type_name or "N/A", []).append(row)
    summaries = []
    for payment_type, payment_rows in sorted(buckets.items(), key=lambda item: item[0]):
        sale_rows = [
            row for row in payment_rows if row.reconciliation_status not in {"excluded_refund", "excluded_cancelled"}
        ]
        erp_rows = [row for row in payment_rows if row.erp_order_numbers]
        loyverse_total = _sum_money(row.loyverse_net_total for row in sale_rows)
        erp_total = _sum_money(row.erp_total for row in erp_rows)
        summaries.append(
            LoyverseB2CReconciliationPaymentTypeSummary(
                payment_type=payment_type,
                loyverse_receipts=len(payment_rows),
                erp_matched=len(erp_rows),
                missing_erp=sum(1 for row in payment_rows if row.reconciliation_status == "missing_in_erp"),
                loyverse_net_total=loyverse_total,
                erp_matched_total=erp_total,
                difference=(loyverse_total - erp_total).quantize(DECIMAL_QUANT),
            )
        )
    return summaries


def _detect_erp_duplicates(row: LoyverseB2CReconciliationRow) -> bool:
    return len(row.erp_order_numbers) > 1


def _detect_total_differences(row: LoyverseB2CReconciliationRow) -> bool:
    return abs(row.difference) > LOYVERSE_TOTAL_TOLERANCE


def _reconciliation_status(
    receipt: LoyverseReceiptPreview,
    erp_orders: list[B2CSalesOrder],
    difference: Decimal,
) -> str:
    if receipt.receipt_kind == "excluded_refund":
        return "excluded_refund"
    if receipt.receipt_kind == "excluded_cancelled":
        return "excluded_cancelled"
    if len(erp_orders) > 1:
        return "duplicate_in_erp"
    if len(erp_orders) == 1:
        if abs(difference) > LOYVERSE_TOTAL_TOLERANCE:
            return "total_mismatch"
        if receipt.warnings or receipt.blockers:
            return "warning_only"
        return "matched"
    if receipt.blockers:
        return "blocked_mapping"
    return "missing_in_erp"


def _row_notes(
    receipt: LoyverseReceiptPreview,
    erp_orders: list[B2CSalesOrder],
    difference: Decimal,
    status: str,
) -> list[str]:
    notes = []
    if status == "excluded_refund":
        notes.append("Refund receipt is excluded and should not appear as missing in ERP.")
    elif status == "excluded_cancelled":
        notes.append("Cancelled/voided receipt is excluded and should not appear as missing in ERP.")
    elif status == "duplicate_in_erp":
        notes.append("More than one ERP B2C order matched this Loyverse receipt.")
    elif status == "missing_in_erp":
        notes.append("No ERP B2C order matched this importable Loyverse receipt.")
    elif status == "blocked_mapping":
        notes.append("No ERP match found and the receipt has blockers in the preview classification.")
    elif status == "total_mismatch":
        notes.append(f"Total difference {difference} exceeds tolerance {LOYVERSE_TOTAL_TOLERANCE}.")
    elif status == "warning_only":
        notes.append("ERP match found within tolerance, but preview warnings/blockers exist.")
    else:
        notes.append("ERP match found within tolerance.")
    if receipt.duplicate_detection_note:
        notes.append(receipt.duplicate_detection_note)
    if erp_orders:
        notes.append(f"Matched ERP order(s): {', '.join(order.order_number for order in erp_orders)}.")
    return notes


def _commercial_day(row: LoyverseB2CReconciliationRow) -> date | None:
    source = row.receipt_date or row.created_at
    return source.date() if source is not None else None


def _sum_money(values) -> Decimal:
    return sum((_money(value) for value in values), ZERO).quantize(DECIMAL_QUANT)


def _money(value) -> Decimal:
    if value is None:
        return ZERO
    return Decimal(str(value)).quantize(DECIMAL_QUANT)


def _normalized_optional_loyverse_id(value: object) -> str:
    text = str(value or "").strip()
    if not text or text.upper() == "N/A":
        return ""
    return text
