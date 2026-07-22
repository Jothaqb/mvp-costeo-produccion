from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy.orm import Session, joinedload

from app.models import B2BCustomer, B2BSalesOrder


ZERO = Decimal("0")
STATUS_ALL = "all"
STATUS_IN_CREDIT = "in_credit"
STATUS_OVERDUE = "overdue"
STATUS_PAID = "paid"
STATUS_OPTIONS = (
    (STATUS_ALL, "Todos"),
    (STATUS_IN_CREDIT, "En credito"),
    (STATUS_OVERDUE, "Vencida"),
    (STATUS_PAID, "Pagada"),
)


@dataclass(frozen=True)
class AccountsReceivableRow:
    order_id: int
    order_number: str
    customer_id: int
    customer_name: str
    invoice_date: date
    invoice_date_is_fallback: bool
    created_at: datetime
    net_sales: Decimal
    paid_amount: Decimal
    pending_amount: Decimal
    last_payment_date: date | None
    credit_days: int
    days_remaining: int | None
    status_code: str
    status_label: str
    status_class: str
    days_class: str


@dataclass(frozen=True)
class AccountsReceivableSummary:
    total_in_credit: Decimal
    total_overdue: Decimal
    total_paid: Decimal
    total_pending: Decimal


@dataclass(frozen=True)
class AccountsReceivableDashboard:
    rows: list[AccountsReceivableRow]
    customers: list[B2BCustomer]
    summary: AccountsReceivableSummary


def build_accounts_receivable_dashboard(
    db: Session,
    *,
    customer_id: int | None = None,
    status: str = STATUS_ALL,
    today: date | None = None,
) -> AccountsReceivableDashboard:
    effective_today = today or date.today()
    normalized_status = status if status in {item[0] for item in STATUS_OPTIONS} else STATUS_ALL

    orders_query = (
        db.query(B2BSalesOrder)
        .filter(B2BSalesOrder.status == "invoiced")
        .options(
            joinedload(B2BSalesOrder.customer),
            joinedload(B2BSalesOrder.ar_opening_balance),
        )
        .order_by(B2BSalesOrder.created_at.desc(), B2BSalesOrder.id.desc())
    )
    if customer_id is not None:
        orders_query = orders_query.filter(B2BSalesOrder.customer_id == customer_id)
    orders = orders_query.all()

    customers = (
        db.query(B2BCustomer)
        .join(B2BSalesOrder, B2BSalesOrder.customer_id == B2BCustomer.id)
        .filter(B2BSalesOrder.status == "invoiced")
        .order_by(B2BCustomer.customer_name.asc(), B2BCustomer.id.asc())
        .distinct()
        .all()
    )

    rows: list[AccountsReceivableRow] = []
    total_in_credit = ZERO
    total_overdue = ZERO
    total_paid = ZERO
    total_pending = ZERO

    for order in orders:
        row = _build_row(order, effective_today)
        total_paid += row.paid_amount
        total_pending += row.pending_amount
        if row.status_code == STATUS_IN_CREDIT:
            total_in_credit += row.pending_amount
        elif row.status_code == STATUS_OVERDUE:
            total_overdue += row.pending_amount

        if normalized_status != STATUS_ALL and row.status_code != normalized_status:
            continue
        rows.append(row)

    return AccountsReceivableDashboard(
        rows=rows,
        customers=customers,
        summary=AccountsReceivableSummary(
            total_in_credit=total_in_credit,
            total_overdue=total_overdue,
            total_paid=total_paid,
            total_pending=total_pending,
        ),
    )


def _build_row(order: B2BSalesOrder, today: date) -> AccountsReceivableRow:
    invoice_date_is_fallback = order.invoiced_at is None
    invoice_date = (
        order.invoiced_at.date()
        if order.invoiced_at is not None
        else order.created_at.date()
    )
    if order.ar_opening_balance is not None:
        pending_amount = order.ar_opening_balance.outstanding_amount or ZERO
        if pending_amount < ZERO:
            pending_amount = ZERO
        paid_amount = order.total_amount - pending_amount
        if paid_amount < ZERO:
            paid_amount = ZERO
    else:
        pending_amount = ZERO
        paid_amount = order.total_amount
    last_payment_date = None
    credit_days = order.customer.credit_days if order.customer is not None else 0
    due_date = invoice_date if credit_days <= 0 else invoice_date.fromordinal(invoice_date.toordinal() + credit_days)
    days_remaining = (due_date - today).days if pending_amount > ZERO else None
    status_code, status_label, status_class, days_class = _build_status(pending_amount, days_remaining)
    return AccountsReceivableRow(
        order_id=order.id,
        order_number=order.order_number,
        customer_id=order.customer_id,
        customer_name=order.customer_name_snapshot,
        invoice_date=invoice_date,
        invoice_date_is_fallback=invoice_date_is_fallback,
        created_at=order.created_at,
        net_sales=order.total_amount,
        paid_amount=paid_amount,
        pending_amount=pending_amount,
        last_payment_date=last_payment_date,
        credit_days=credit_days,
        days_remaining=days_remaining,
        status_code=status_code,
        status_label=status_label,
        status_class=status_class,
        days_class=days_class,
    )

def _build_status(pending_amount: Decimal, days_remaining: int | None) -> tuple[str, str, str, str]:
    if pending_amount <= ZERO:
        return STATUS_PAID, "Pagada", "status-green", "status-green"
    if days_remaining is not None and days_remaining < 0:
        return STATUS_OVERDUE, "Vencida", "status-red", "status-red"
    if days_remaining is not None and days_remaining <= 4:
        return STATUS_IN_CREDIT, "En credito", "status-yellow", "status-yellow"
    return STATUS_IN_CREDIT, "En credito", "status-green", "status-green"
