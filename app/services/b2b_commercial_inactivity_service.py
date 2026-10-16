from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Callable

from sqlalchemy.orm import Session, selectinload, sessionmaker

from app.models import (
    AuditLog,
    B2BCustomer,
    B2BCustomerProduct,
    B2BSalesOrder,
    Channel,
    Product,
    User,
)
from app.services.accounts_receivable_service import (
    build_accounts_receivable_dashboard,
    resolve_b2b_invoice_date,
)
from app.services.audit_service import serialize_audit_payload


ZERO = Decimal("0")
HISTORICAL_IMPORT_PREFIX = "Historical CSV import"
OPEN_ORDER_STATUSES = {"draft", "in_process"}
CLASSIFICATION_ELIGIBLE = "eligible"
CLASSIFICATION_REVIEW_PENDING = "review_pending"
CLASSIFICATION_EXCLUDED = "excluded"
EXECUTION_CONFIRMATION = "DEACTIVATE_CONFIRMED_B2B_INACTIVITY_CANDIDATES"
COSTA_RICA_TIMEZONE = timezone(timedelta(hours=-6), name="America/Costa_Rica")
CHANNEL_SCOPE_REVISION = "b2b-commercial-inactivity-authorized-channels-v1"
ELIGIBILITY_POLICY_REVISION = "b2b-commercial-inactivity-sales-evidence-v2"
AUTHORIZED_B2B_CHANNEL_NAMES = (
    "Tiendas Naturales (Minisuper)",
    "Tiendas Naturas (Macobioticas)",
    "Tiendas Naturales (Supermercados)",
    "Tienda Online (Terceros)",
)


class B2BCommercialInactivityError(RuntimeError):
    pass


@dataclass(frozen=True)
class InactivityConfig:
    as_of_date: date
    history_complete_since: date | None = None

    @property
    def cutoff_date(self) -> date:
        return subtract_calendar_months(self.as_of_date, 12)

    @property
    def history_is_sufficient(self) -> bool:
        return (
            self.history_complete_since is not None
            and self.history_complete_since <= self.cutoff_date
        )


@dataclass(frozen=True)
class CatalogProductInactivityEvidence:
    catalog_product_id: int
    customer_id: int
    customer_name: str
    channel_id: int
    channel_name: str
    sku: str
    description: str
    active_before: bool
    proposed_active: bool
    last_purchase_date: date | None
    created_at: datetime
    updated_at: datetime
    has_open_order: bool
    classification: str
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class CustomerInactivityEvidence:
    customer_id: int
    customer_name: str
    channel_id: int
    channel_name: str
    active_before: bool
    proposed_active: bool
    last_invoice_date: date | None
    rolling_invoice_count: int
    rolling_sales_amount: Decimal
    pending_receivables_amount: Decimal
    has_open_orders: bool
    current_agreement_statuses: tuple[str, ...]
    created_at: datetime
    updated_at: datetime
    classification: str
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class HistoryCoverageEvidence:
    period_month: str
    invoiced_orders: int
    historical_import_orders: int
    regular_orders: int


@dataclass(frozen=True)
class ChannelScopeResolutionEvidence:
    expected_name: str
    status: str
    channel_id: int | None
    stored_name: str | None
    active: bool | None
    applies_to_b2b: bool | None


@dataclass(frozen=True)
class ChannelBreakdownEvidence:
    channel_name: str
    channel_id: int | None
    customers_evaluated: int
    catalog_products_evaluated: int
    customers_potentially_inactive: int
    catalog_products_potentially_inactive: int


@dataclass(frozen=True)
class B2BCommercialInactivityPlan:
    config: InactivityConfig
    channel_scope_revision: str
    eligibility_policy_revision: str
    channel_scope_resolutions: tuple[ChannelScopeResolutionEvidence, ...]
    available_b2b_channel_names: tuple[str, ...]
    channel_breakdown: tuple[ChannelBreakdownEvidence, ...]
    registered_customers_total: int
    customers_excluded_other_channels: int
    customers_without_channel: int
    registered_catalog_products_total: int
    catalog_products_outside_scope: int
    catalog_products: tuple[CatalogProductInactivityEvidence, ...]
    customers: tuple[CustomerInactivityEvidence, ...]
    history_coverage: tuple[HistoryCoverageEvidence, ...]
    earliest_invoice_date: date | None
    latest_invoice_date: date | None
    plan_fingerprint: str

    @property
    def channel_scope_is_valid(self) -> bool:
        return all(
            row.status == "resolved"
            for row in self.channel_scope_resolutions
        )

    @property
    def eligible_catalog_products(self) -> tuple[CatalogProductInactivityEvidence, ...]:
        return tuple(
            row for row in self.catalog_products if row.classification == CLASSIFICATION_ELIGIBLE
        )

    @property
    def eligible_customers(self) -> tuple[CustomerInactivityEvidence, ...]:
        return tuple(row for row in self.customers if row.classification == CLASSIFICATION_ELIGIBLE)

    @property
    def review_pending_count(self) -> int:
        return sum(
            row.classification == CLASSIFICATION_REVIEW_PENDING
            for row in (*self.catalog_products, *self.customers)
        )

    @property
    def excluded_count(self) -> int:
        return sum(
            row.classification == CLASSIFICATION_EXCLUDED
            for row in (*self.catalog_products, *self.customers)
        )


@dataclass(frozen=True)
class InactivityExecutionChange:
    entity_type: str
    entity_id: int
    customer_id: int
    customer_name: str
    sku: str | None
    active_before: bool
    active_after: bool
    reason: str


@dataclass(frozen=True)
class InactivityExecutionResult:
    plan: B2BCommercialInactivityPlan
    changes: tuple[InactivityExecutionChange, ...]


def current_costa_rica_date() -> date:
    return datetime.now(COSTA_RICA_TIMEZONE).date()


def subtract_calendar_months(value: date, months: int) -> date:
    if months < 0:
        raise ValueError("months must be zero or positive")
    target_month_index = value.year * 12 + (value.month - 1) - months
    target_year, target_month_zero_based = divmod(target_month_index, 12)
    target_month = target_month_zero_based + 1
    next_month = date(target_year + (target_month == 12), 1 if target_month == 12 else target_month + 1, 1)
    last_day = (next_month - timedelta(days=1)).day
    return date(target_year, target_month, min(value.day, last_day))


def build_b2b_commercial_inactivity_plan(
    db: Session,
    *,
    config: InactivityConfig,
) -> B2BCommercialInactivityPlan:
    if config.history_complete_since is not None and config.history_complete_since > config.as_of_date:
        raise B2BCommercialInactivityError("History coverage cannot start after the report date.")

    channels = db.query(Channel).order_by(Channel.id).all()
    channel_scope_resolutions = _resolve_authorized_channel_scope(channels)
    authorized_channel_ids = {
        row.channel_id
        for row in channel_scope_resolutions
        if row.status == "resolved" and row.channel_id is not None
    }
    all_customers = (
        db.query(B2BCustomer)
        .options(
            selectinload(B2BCustomer.products),
            selectinload(B2BCustomer.commercial_agreements),
            selectinload(B2BCustomer.channel_record),
        )
        .order_by(B2BCustomer.id)
        .all()
    )
    customers = [
        customer
        for customer in all_customers
        if customer.channel_id in authorized_channel_ids
    ]
    included_customer_ids = {customer.id for customer in customers}
    customers_without_channel = sum(
        customer.channel_id is None for customer in all_customers
    )
    customers_excluded_other_channels = sum(
        customer.channel_id is not None
        and customer.channel_id not in authorized_channel_ids
        for customer in all_customers
    )
    registered_catalog_products_total = sum(
        len(customer.products) for customer in all_customers
    )
    catalog_products_outside_scope = sum(
        len(customer.products)
        for customer in all_customers
        if customer.id not in included_customer_ids
    )
    products_by_sku = {
        product.sku: product
        for product in db.query(Product).order_by(Product.id).all()
        if (product.sku or "").strip()
    }
    if included_customer_ids:
        invoiced_orders = (
            db.query(B2BSalesOrder)
            .filter(
                B2BSalesOrder.status == "invoiced",
                B2BSalesOrder.customer_id.in_(sorted(included_customer_ids)),
            )
            .options(selectinload(B2BSalesOrder.lines))
            .order_by(B2BSalesOrder.id)
            .all()
        )
        open_orders = (
            db.query(B2BSalesOrder)
            .filter(
                B2BSalesOrder.status.in_(sorted(OPEN_ORDER_STATUSES)),
                B2BSalesOrder.customer_id.in_(sorted(included_customer_ids)),
            )
            .options(selectinload(B2BSalesOrder.lines))
            .order_by(B2BSalesOrder.id)
            .all()
        )
    else:
        invoiced_orders = []
        open_orders = []

    sales = _build_sales_facts(invoiced_orders, config)
    open_customer_ids = {order.customer_id for order in open_orders}
    open_customer_skus = {
        (order.customer_id, (line.sku_snapshot or "").strip())
        for order in open_orders
        for line in order.lines
        if (line.sku_snapshot or "").strip()
    }
    pending_receivables = _pending_receivables_by_customer(
        db,
        as_of_date=config.as_of_date,
    )

    catalog_rows: list[CatalogProductInactivityEvidence] = []
    for customer in customers:
        for catalog_product in sorted(customer.products, key=lambda row: row.id):
            catalog_rows.append(
                _classify_catalog_product(
                    customer=customer,
                    catalog_product=catalog_product,
                    product_exists=catalog_product.sku in products_by_sku,
                    latest_purchase=sales.latest_purchase_by_customer_sku.get(
                        (customer.id, catalog_product.sku)
                    ),
                    has_future_purchase=(customer.id, catalog_product.sku)
                    in sales.future_customer_skus,
                    has_open_order=(customer.id, catalog_product.sku) in open_customer_skus,
                    config=config,
                )
            )

    customer_rows = [
        _classify_customer(
            customer=customer,
            last_invoice_date=sales.latest_invoice_by_customer.get(customer.id),
            has_future_invoice=customer.id in sales.future_customer_ids,
            rolling_invoice_count=sales.rolling_invoice_count_by_customer.get(customer.id, 0),
            rolling_sales_amount=sales.rolling_sales_by_customer.get(customer.id, ZERO),
            pending_receivables_amount=pending_receivables.get(customer.id, ZERO),
            has_open_orders=customer.id in open_customer_ids,
            config=config,
        )
        for customer in customers
    ]
    channel_breakdown = _build_channel_breakdown(
        channel_scope_resolutions,
        catalog_rows,
        customer_rows,
    )

    fingerprint = _plan_fingerprint(
        config,
        channel_scope_resolutions,
        [(customer.id, customer.channel_id) for customer in all_customers],
        registered_catalog_products_total,
        catalog_rows,
        customer_rows,
        sales.history_coverage,
        sales.earliest_invoice_date,
        sales.latest_invoice_date,
    )
    return B2BCommercialInactivityPlan(
        config=config,
        channel_scope_revision=CHANNEL_SCOPE_REVISION,
        eligibility_policy_revision=ELIGIBILITY_POLICY_REVISION,
        channel_scope_resolutions=channel_scope_resolutions,
        available_b2b_channel_names=tuple(
            channel.name for channel in channels if channel.applies_to_b2b
        ),
        channel_breakdown=channel_breakdown,
        registered_customers_total=len(all_customers),
        customers_excluded_other_channels=customers_excluded_other_channels,
        customers_without_channel=customers_without_channel,
        registered_catalog_products_total=registered_catalog_products_total,
        catalog_products_outside_scope=catalog_products_outside_scope,
        catalog_products=tuple(catalog_rows),
        customers=tuple(customer_rows),
        history_coverage=sales.history_coverage,
        earliest_invoice_date=sales.earliest_invoice_date,
        latest_invoice_date=sales.latest_invoice_date,
        plan_fingerprint=fingerprint,
    )


def _resolve_authorized_channel_scope(
    channels: list[Channel],
) -> tuple[ChannelScopeResolutionEvidence, ...]:
    channels_by_name = {channel.name: channel for channel in channels}
    resolutions: list[ChannelScopeResolutionEvidence] = []
    for expected_name in AUTHORIZED_B2B_CHANNEL_NAMES:
        channel = channels_by_name.get(expected_name)
        if channel is None:
            resolutions.append(
                ChannelScopeResolutionEvidence(
                    expected_name=expected_name,
                    status="missing",
                    channel_id=None,
                    stored_name=None,
                    active=None,
                    applies_to_b2b=None,
                )
            )
            continue
        status = "resolved" if channel.applies_to_b2b else "not_b2b"
        resolutions.append(
            ChannelScopeResolutionEvidence(
                expected_name=expected_name,
                status=status,
                channel_id=channel.id,
                stored_name=channel.name,
                active=bool(channel.active),
                applies_to_b2b=bool(channel.applies_to_b2b),
            )
        )
    return tuple(resolutions)


def _build_channel_breakdown(
    resolutions: tuple[ChannelScopeResolutionEvidence, ...],
    catalog_rows: list[CatalogProductInactivityEvidence],
    customer_rows: list[CustomerInactivityEvidence],
) -> tuple[ChannelBreakdownEvidence, ...]:
    potentially_inactive = {
        CLASSIFICATION_ELIGIBLE,
        CLASSIFICATION_REVIEW_PENDING,
    }
    return tuple(
        ChannelBreakdownEvidence(
            channel_name=resolution.expected_name,
            channel_id=resolution.channel_id,
            customers_evaluated=sum(
                row.channel_id == resolution.channel_id for row in customer_rows
            ) if resolution.status == "resolved" else 0,
            catalog_products_evaluated=sum(
                row.channel_id == resolution.channel_id for row in catalog_rows
            ) if resolution.status == "resolved" else 0,
            customers_potentially_inactive=sum(
                row.channel_id == resolution.channel_id
                and row.classification in potentially_inactive
                for row in customer_rows
            ) if resolution.status == "resolved" else 0,
            catalog_products_potentially_inactive=sum(
                row.channel_id == resolution.channel_id
                and row.classification in potentially_inactive
                for row in catalog_rows
            ) if resolution.status == "resolved" else 0,
        )
        for resolution in resolutions
    )


@dataclass(frozen=True)
class _SalesFacts:
    latest_purchase_by_customer_sku: dict[tuple[int, str], date]
    latest_invoice_by_customer: dict[int, date]
    rolling_invoice_count_by_customer: dict[int, int]
    rolling_sales_by_customer: dict[int, Decimal]
    future_customer_skus: set[tuple[int, str]]
    future_customer_ids: set[int]
    history_coverage: tuple[HistoryCoverageEvidence, ...]
    earliest_invoice_date: date | None
    latest_invoice_date: date | None


def _build_sales_facts(
    orders: list[B2BSalesOrder],
    config: InactivityConfig,
) -> _SalesFacts:
    latest_purchase: dict[tuple[int, str], date] = {}
    latest_invoice: dict[int, date] = {}
    rolling_counts: dict[int, int] = {}
    rolling_sales: dict[int, Decimal] = {}
    future_customer_skus: set[tuple[int, str]] = set()
    future_customer_ids: set[int] = set()
    monthly_counts: dict[str, list[int]] = {
        period: [0, 0, 0]
        for period in _month_keys(config.cutoff_date, config.as_of_date)
    }
    invoice_dates: list[date] = []

    for order in orders:
        invoice_date, _ = resolve_b2b_invoice_date(order)
        if invoice_date > config.as_of_date:
            future_customer_ids.add(order.customer_id)
            future_customer_skus.update(
                (order.customer_id, (line.sku_snapshot or "").strip())
                for line in order.lines
                if (line.sku_snapshot or "").strip()
            )
            continue

        invoice_dates.append(invoice_date)
        if invoice_date >= config.cutoff_date:
            period = invoice_date.strftime("%Y-%m")
            counts = monthly_counts.setdefault(period, [0, 0, 0])
            counts[0] += 1
            if (order.observations or "").startswith(HISTORICAL_IMPORT_PREFIX):
                counts[1] += 1
            else:
                counts[2] += 1

        latest_invoice[order.customer_id] = max(
            latest_invoice.get(order.customer_id, date.min),
            invoice_date,
        )
        if invoice_date >= config.cutoff_date:
            rolling_counts[order.customer_id] = rolling_counts.get(order.customer_id, 0) + 1
            rolling_sales[order.customer_id] = (
                rolling_sales.get(order.customer_id, ZERO) + (order.total_amount or ZERO)
            )

        for line in order.lines:
            sku = (line.sku_snapshot or "").strip()
            if not sku or (line.quantity or ZERO) <= ZERO:
                continue
            key = (order.customer_id, sku)
            latest_purchase[key] = max(latest_purchase.get(key, date.min), invoice_date)

    return _SalesFacts(
        latest_purchase_by_customer_sku=latest_purchase,
        latest_invoice_by_customer=latest_invoice,
        rolling_invoice_count_by_customer=rolling_counts,
        rolling_sales_by_customer=rolling_sales,
        future_customer_skus=future_customer_skus,
        future_customer_ids=future_customer_ids,
        history_coverage=tuple(
            HistoryCoverageEvidence(
                period_month=period,
                invoiced_orders=counts[0],
                historical_import_orders=counts[1],
                regular_orders=counts[2],
            )
            for period, counts in sorted(monthly_counts.items())
        ),
        earliest_invoice_date=min(invoice_dates, default=None),
        latest_invoice_date=max(invoice_dates, default=None),
    )


def _month_keys(start: date, end: date) -> list[str]:
    current_year = start.year
    current_month = start.month
    values: list[str] = []
    while (current_year, current_month) <= (end.year, end.month):
        values.append(f"{current_year:04d}-{current_month:02d}")
        if current_month == 12:
            current_year += 1
            current_month = 1
        else:
            current_month += 1
    return values


def _pending_receivables_by_customer(
    db: Session,
    *,
    as_of_date: date,
) -> dict[int, Decimal]:
    dashboard = build_accounts_receivable_dashboard(db, today=as_of_date)
    totals: dict[int, Decimal] = {}
    for row in dashboard.rows:
        if row.pending_amount <= ZERO:
            continue
        totals[row.customer_id] = totals.get(row.customer_id, ZERO) + row.pending_amount
    return totals


def _classify_catalog_product(
    *,
    customer: B2BCustomer,
    catalog_product: B2BCustomerProduct,
    product_exists: bool,
    latest_purchase: date | None,
    has_future_purchase: bool,
    has_open_order: bool,
    config: InactivityConfig,
) -> CatalogProductInactivityEvidence:
    reasons: list[str] = []
    if not catalog_product.active:
        classification = CLASSIFICATION_EXCLUDED
        reasons.append("El producto ya está inactivo en el catálogo del cliente.")
    elif latest_purchase is not None and latest_purchase >= config.cutoff_date:
        classification = CLASSIFICATION_EXCLUDED
        reasons.append("El cliente compró el SKU dentro de la ventana móvil de 12 meses.")
    else:
        if not config.history_is_sufficient:
            reasons.append("No existe una confirmación de cobertura histórica completa desde el inicio de la ventana.")
        if not product_exists:
            reasons.append("El SKU del catálogo no coincide exactamente con un Product actual.")
        if has_open_order:
            reasons.append("El SKU está incluido en un pedido B2B abierto del cliente.")
        if has_future_purchase:
            reasons.append("Existe una factura con fecha futura para este cliente y SKU.")
        if not customer.active:
            reasons.append("El cliente ya está inactivo; el catálogo requiere revisión manual.")
        classification = (
            CLASSIFICATION_REVIEW_PENDING if reasons else CLASSIFICATION_ELIGIBLE
        )
        if classification == CLASSIFICATION_ELIGIBLE:
            reasons.append("Sin compras facturadas del SKU durante los últimos 12 meses.")

    return CatalogProductInactivityEvidence(
        catalog_product_id=catalog_product.id,
        customer_id=customer.id,
        customer_name=customer.customer_name,
        channel_id=customer.channel_id,
        channel_name=customer.channel_record.name,
        sku=catalog_product.sku,
        description=catalog_product.description,
        active_before=bool(catalog_product.active),
        proposed_active=False if classification == CLASSIFICATION_ELIGIBLE else bool(catalog_product.active),
        last_purchase_date=latest_purchase,
        created_at=catalog_product.created_at,
        updated_at=catalog_product.updated_at,
        has_open_order=has_open_order,
        classification=classification,
        reasons=tuple(reasons),
    )


def _classify_customer(
    *,
    customer: B2BCustomer,
    last_invoice_date: date | None,
    has_future_invoice: bool,
    rolling_invoice_count: int,
    rolling_sales_amount: Decimal,
    pending_receivables_amount: Decimal,
    has_open_orders: bool,
    config: InactivityConfig,
) -> CustomerInactivityEvidence:
    reasons: list[str] = []
    if not customer.active:
        classification = CLASSIFICATION_EXCLUDED
        reasons.append("El cliente ya está inactivo.")
    elif rolling_invoice_count > 0:
        classification = CLASSIFICATION_EXCLUDED
        reasons.append("El cliente tiene ventas B2B facturadas dentro de la ventana móvil de 12 meses.")
    else:
        if not config.history_is_sufficient:
            reasons.append("No existe una confirmación de cobertura histórica completa desde el inicio de la ventana.")
        if has_open_orders:
            reasons.append("El cliente tiene pedidos B2B abiertos.")
        if has_future_invoice:
            reasons.append("El cliente tiene una factura con fecha futura.")

        classification = (
            CLASSIFICATION_REVIEW_PENDING if reasons else CLASSIFICATION_ELIGIBLE
        )
        if classification == CLASSIFICATION_ELIGIBLE:
            reasons.append("Sin facturas B2B durante los últimos 12 meses y sin pedidos abiertos.")

    if pending_receivables_amount > ZERO:
        reasons.append(
            "Advertencia: conserva saldo pendiente en CxC; la depuración no modifica ese saldo."
        )

    agreement_statuses = tuple(
        sorted({agreement.status for agreement in customer.commercial_agreements})
    )
    if agreement_statuses:
        reasons.append(
            "Información: existen acuerdos comerciales que se conservarán sin cambios "
            f"({', '.join(agreement_statuses)})."
        )

    return CustomerInactivityEvidence(
        customer_id=customer.id,
        customer_name=customer.customer_name,
        channel_id=customer.channel_id,
        channel_name=customer.channel_record.name,
        active_before=bool(customer.active),
        proposed_active=False if classification == CLASSIFICATION_ELIGIBLE else bool(customer.active),
        last_invoice_date=last_invoice_date,
        rolling_invoice_count=rolling_invoice_count,
        rolling_sales_amount=rolling_sales_amount,
        pending_receivables_amount=pending_receivables_amount,
        has_open_orders=has_open_orders,
        current_agreement_statuses=agreement_statuses,
        created_at=customer.created_at,
        updated_at=customer.updated_at,
        classification=classification,
        reasons=tuple(reasons),
    )


def _plan_fingerprint(
    config: InactivityConfig,
    channel_scope_resolutions: tuple[ChannelScopeResolutionEvidence, ...],
    customer_channel_assignments: list[tuple[int, int | None]],
    registered_catalog_products_total: int,
    catalog_rows: list[CatalogProductInactivityEvidence],
    customer_rows: list[CustomerInactivityEvidence],
    history_coverage: tuple[HistoryCoverageEvidence, ...],
    earliest_invoice_date: date | None,
    latest_invoice_date: date | None,
) -> str:
    payload = {
        "channel_scope_revision": CHANNEL_SCOPE_REVISION,
        "eligibility_policy_revision": ELIGIBILITY_POLICY_REVISION,
        "channel_scope_resolutions": [
            evidence_dict(row) for row in channel_scope_resolutions
        ],
        "customer_channel_assignments": sorted(customer_channel_assignments),
        "registered_catalog_products_total": registered_catalog_products_total,
        "as_of_date": config.as_of_date.isoformat(),
        "cutoff_date": config.cutoff_date.isoformat(),
        "history_complete_since": (
            config.history_complete_since.isoformat()
            if config.history_complete_since is not None
            else None
        ),
        "eligible_catalog_product_ids": sorted(
            row.catalog_product_id
            for row in catalog_rows
            if row.classification == CLASSIFICATION_ELIGIBLE
        ),
        "eligible_customer_ids": sorted(
            row.customer_id
            for row in customer_rows
            if row.classification == CLASSIFICATION_ELIGIBLE
        ),
        "catalog_products": [evidence_dict(row) for row in catalog_rows],
        "customers": [evidence_dict(row) for row in customer_rows],
        "history_coverage": [evidence_dict(row) for row in history_coverage],
        "earliest_invoice_date": (
            earliest_invoice_date.isoformat() if earliest_invoice_date is not None else None
        ),
        "latest_invoice_date": (
            latest_invoice_date.isoformat() if latest_invoice_date is not None else None
        ),
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def execute_b2b_commercial_inactivity_plan(
    session_factory: sessionmaker,
    *,
    config: InactivityConfig,
    approved_plan_fingerprint: str,
    actor_username: str,
    confirmation: str,
    before_commit: Callable[[Session], None] | None = None,
) -> InactivityExecutionResult:
    if confirmation != EXECUTION_CONFIRMATION:
        raise B2BCommercialInactivityError("The execute confirmation text is invalid.")
    if not config.history_is_sufficient:
        raise B2BCommercialInactivityError(
            "Execute requires confirmed history coverage beginning on or before the 12-month cutoff."
        )
    if not approved_plan_fingerprint:
        raise B2BCommercialInactivityError("An approved dry-run plan fingerprint is required.")

    with session_factory() as db:
        try:
            with db.begin():
                actor = (
                    db.query(User)
                    .options(selectinload(User.roles))
                    .filter(User.username == actor_username, User.is_active.is_(True))
                    .one_or_none()
                )
                if actor is None or not any(role.active and role.code == "admin" for role in actor.roles):
                    raise B2BCommercialInactivityError(
                        "Execute requires an active ERP administrator as the audit actor."
                    )

                initial_plan = build_b2b_commercial_inactivity_plan(db, config=config)
                if not initial_plan.channel_scope_is_valid:
                    raise B2BCommercialInactivityError(
                        "Execute is blocked because one or more authorized B2B channels "
                        "could not be resolved exactly."
                    )
                if initial_plan.plan_fingerprint != approved_plan_fingerprint:
                    raise B2BCommercialInactivityError(
                        "The recalculated plan differs from the approved dry-run; execute was blocked."
                    )

                catalog_ids = [row.catalog_product_id for row in initial_plan.eligible_catalog_products]
                customer_ids = [row.customer_id for row in initial_plan.eligible_customers]
                lock_customer_ids = sorted(
                    set(customer_ids)
                    | {row.customer_id for row in initial_plan.eligible_catalog_products}
                )
                if lock_customer_ids:
                    (
                        db.query(B2BCustomer)
                        .filter(B2BCustomer.id.in_(lock_customer_ids))
                        .with_for_update()
                        .all()
                    )
                if catalog_ids:
                    (
                        db.query(B2BCustomerProduct)
                        .filter(B2BCustomerProduct.id.in_(catalog_ids))
                        .with_for_update()
                        .all()
                    )

                db.expire_all()
                plan = build_b2b_commercial_inactivity_plan(db, config=config)
                if plan.plan_fingerprint != approved_plan_fingerprint:
                    raise B2BCommercialInactivityError(
                        "The plan changed while target rows were being locked; execute was blocked."
                    )

                changes: list[InactivityExecutionChange] = []
                catalog_evidence = {
                    row.catalog_product_id: row for row in plan.eligible_catalog_products
                }
                if catalog_evidence:
                    target_catalog_products = (
                        db.query(B2BCustomerProduct)
                        .filter(B2BCustomerProduct.id.in_(sorted(catalog_evidence)))
                        .order_by(B2BCustomerProduct.id)
                        .all()
                    )
                    if len(target_catalog_products) != len(catalog_evidence):
                        raise B2BCommercialInactivityError("An approved catalog product no longer exists.")
                    for catalog_product in target_catalog_products:
                        evidence = catalog_evidence[catalog_product.id]
                        if not catalog_product.active:
                            raise B2BCommercialInactivityError(
                                f"Catalog product {catalog_product.id} is no longer active."
                            )
                        catalog_product.active = False
                        reason = " | ".join(evidence.reasons)
                        db.add(
                            _audit_log(
                                actor=actor,
                                action="b2b_customer_product_inactivity_deactivated",
                                entity_type="b2b_customer_product",
                                entity_id=catalog_product.id,
                                entity_label=(
                                    f"{evidence.customer_name} - {catalog_product.sku}"
                                ),
                                old_values={"active": True},
                                new_values={"active": False},
                                notes=reason,
                            )
                        )
                        changes.append(
                            InactivityExecutionChange(
                                entity_type="b2b_customer_product",
                                entity_id=catalog_product.id,
                                customer_id=evidence.customer_id,
                                customer_name=evidence.customer_name,
                                sku=catalog_product.sku,
                                active_before=True,
                                active_after=False,
                                reason=reason,
                            )
                        )

                customer_evidence = {row.customer_id: row for row in plan.eligible_customers}
                if customer_evidence:
                    target_customers = (
                        db.query(B2BCustomer)
                        .filter(B2BCustomer.id.in_(sorted(customer_evidence)))
                        .order_by(B2BCustomer.id)
                        .all()
                    )
                    if len(target_customers) != len(customer_evidence):
                        raise B2BCommercialInactivityError("An approved customer no longer exists.")
                    for customer in target_customers:
                        evidence = customer_evidence[customer.id]
                        if not customer.active:
                            raise B2BCommercialInactivityError(
                                f"Customer {customer.id} is no longer active."
                            )
                        customer.active = False
                        reason = " | ".join(evidence.reasons)
                        db.add(
                            _audit_log(
                                actor=actor,
                                action="b2b_customer_inactivity_deactivated",
                                entity_type="b2b_customer",
                                entity_id=customer.id,
                                entity_label=customer.customer_name,
                                old_values={"active": True},
                                new_values={"active": False},
                                notes=reason,
                            )
                        )
                        changes.append(
                            InactivityExecutionChange(
                                entity_type="b2b_customer",
                                entity_id=customer.id,
                                customer_id=customer.id,
                                customer_name=customer.customer_name,
                                sku=None,
                                active_before=True,
                                active_after=False,
                                reason=reason,
                            )
                        )

                db.flush()
                _validate_applied_changes(db, plan)
                if before_commit is not None:
                    before_commit(db)
            return InactivityExecutionResult(plan=plan, changes=tuple(changes))
        except Exception:
            db.rollback()
            raise


def _audit_log(
    *,
    actor: User,
    action: str,
    entity_type: str,
    entity_id: int,
    entity_label: str,
    old_values: dict[str, object],
    new_values: dict[str, object],
    notes: str,
) -> AuditLog:
    return AuditLog(
        user_id=actor.id,
        username=actor.username,
        module="sales",
        action=action,
        entity_type=entity_type,
        entity_id=str(entity_id),
        entity_label=entity_label,
        old_values=serialize_audit_payload(old_values),
        new_values=serialize_audit_payload(new_values),
        request_path="tools/b2b_commercial_inactivity_job.py",
        method="JOB",
        notes=notes,
    )


def _validate_applied_changes(
    db: Session,
    plan: B2BCommercialInactivityPlan,
) -> None:
    catalog_ids = [row.catalog_product_id for row in plan.eligible_catalog_products]
    customer_ids = [row.customer_id for row in plan.eligible_customers]
    if catalog_ids:
        remaining_active = (
            db.query(B2BCustomerProduct.id)
            .filter(
                B2BCustomerProduct.id.in_(catalog_ids),
                B2BCustomerProduct.active.is_(True),
            )
            .count()
        )
        if remaining_active:
            raise B2BCommercialInactivityError(
                "One or more approved catalog products remained active after execution."
            )
    if customer_ids:
        remaining_active = (
            db.query(B2BCustomer.id)
            .filter(B2BCustomer.id.in_(customer_ids), B2BCustomer.active.is_(True))
            .count()
        )
        if remaining_active:
            raise B2BCommercialInactivityError(
                "One or more approved customers remained active after execution."
            )


def plan_summary(plan: B2BCommercialInactivityPlan) -> dict[str, object]:
    return {
        "mode": "dry-run",
        "channel_scope_revision": plan.channel_scope_revision,
        "eligibility_policy_revision": plan.eligibility_policy_revision,
        "channel_scope_is_valid": plan.channel_scope_is_valid,
        "authorized_channel_names": list(AUTHORIZED_B2B_CHANNEL_NAMES),
        "channel_scope_resolutions": [
            evidence_dict(row) for row in plan.channel_scope_resolutions
        ],
        "available_b2b_channel_names": list(plan.available_b2b_channel_names),
        "channel_breakdown": [
            evidence_dict(row) for row in plan.channel_breakdown
        ],
        "as_of_date": plan.config.as_of_date.isoformat(),
        "cutoff_date": plan.config.cutoff_date.isoformat(),
        "history_complete_since": (
            plan.config.history_complete_since.isoformat()
            if plan.config.history_complete_since is not None
            else None
        ),
        "history_is_sufficient": plan.config.history_is_sufficient,
        "earliest_invoice_date": (
            plan.earliest_invoice_date.isoformat()
            if plan.earliest_invoice_date is not None
            else None
        ),
        "latest_invoice_date": (
            plan.latest_invoice_date.isoformat()
            if plan.latest_invoice_date is not None
            else None
        ),
        "customers_registered_total": plan.registered_customers_total,
        "customers_in_authorized_channels": len(plan.customers),
        "customers_excluded_other_channels": plan.customers_excluded_other_channels,
        "customers_without_channel": plan.customers_without_channel,
        "catalog_products_registered_total": plan.registered_catalog_products_total,
        "catalog_products_evaluated_in_scope": len(plan.catalog_products),
        "catalog_products_outside_scope": plan.catalog_products_outside_scope,
        "catalog_products_total": len(plan.catalog_products),
        "catalog_products_eligible": len(plan.eligible_catalog_products),
        "catalog_products_review_pending": sum(
            row.classification == CLASSIFICATION_REVIEW_PENDING
            for row in plan.catalog_products
        ),
        "catalog_products_excluded_other_conditions": sum(
            row.classification == CLASSIFICATION_EXCLUDED
            for row in plan.catalog_products
        ),
        "customers_total": len(plan.customers),
        "customers_eligible": len(plan.eligible_customers),
        "customers_review_pending": sum(
            row.classification == CLASSIFICATION_REVIEW_PENDING
            for row in plan.customers
        ),
        "customers_excluded_other_conditions": sum(
            row.classification == CLASSIFICATION_EXCLUDED
            for row in plan.customers
        ),
        "review_pending": plan.review_pending_count,
        "excluded": plan.excluded_count,
        "eligible_catalog_product_ids": [
            row.catalog_product_id for row in plan.eligible_catalog_products
        ],
        "eligible_customer_ids": [row.customer_id for row in plan.eligible_customers],
        "plan_fingerprint": plan.plan_fingerprint,
    }


def evidence_dict(value: object) -> dict[str, object]:
    payload = asdict(value)
    return {key: _json_value(item) for key, item in payload.items()}


def _json_value(value: object) -> object:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, tuple):
        return [_json_value(item) for item in value]
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    return value
