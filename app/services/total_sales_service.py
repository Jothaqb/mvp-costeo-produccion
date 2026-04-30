from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal

from sqlalchemy.orm import Session

from app.models import B2BSalesOrder, B2BSalesOrderLine, B2CSalesOrder, B2CSalesOrderLine, Product, ProductCategory


ZERO = Decimal("0")
HUNDRED = Decimal("100")
PERCENT_QUANT = Decimal("0.1")


@dataclass(frozen=True)
class TotalSalesRow:
    sales_source: str
    order_id: int
    order_number: str
    order_date: date
    customer_name: str | None
    channel_name: str | None
    sku: str
    description: str
    category_name: str | None
    quantity: Decimal
    unit_price: Decimal
    discount_amount: Decimal | None
    line_total: Decimal
    order_status: str
    order_created_at: datetime
    line_number: int


@dataclass(frozen=True)
class SalesSummaryBucket:
    label: str
    total_net_sales: Decimal
    total_orders: int
    total_lines: int
    total_quantity: Decimal
    total_discount: Decimal
    average_order_value: Decimal
    average_line_value: Decimal
    sales_percent: Decimal


@dataclass(frozen=True)
class SalesSummary:
    total: SalesSummaryBucket
    b2b: SalesSummaryBucket
    b2c: SalesSummaryBucket


@dataclass(frozen=True)
class SalesItemParetoRow:
    rank: int
    sku: str
    description: str
    net_sales: Decimal
    quantity: Decimal
    lines: int
    orders: int
    discount: Decimal
    percent_of_total_sales: Decimal
    cumulative_percent: Decimal
    pareto_class: str


@dataclass(frozen=True)
class SalesItemsParetoSummary:
    total_net_sales: Decimal
    total_items: int
    total_quantity: Decimal
    a_items_count: int
    b_items_count: int
    c_items_count: int


@dataclass(frozen=True)
class SalesItemsPareto:
    summary: SalesItemsParetoSummary
    rows: list[SalesItemParetoRow]


@dataclass(frozen=True)
class SalesCategoryParetoRow:
    rank: int
    category_name: str
    net_sales: Decimal
    quantity: Decimal
    lines: int
    orders: int
    items_count: int
    discount: Decimal
    percent_of_total_sales: Decimal
    cumulative_percent: Decimal
    pareto_class: str


@dataclass(frozen=True)
class SalesCategoriesParetoSummary:
    total_net_sales: Decimal
    total_categories: int
    total_quantity: Decimal
    a_categories_count: int
    b_categories_count: int
    c_categories_count: int


@dataclass(frozen=True)
class SalesCategoriesPareto:
    summary: SalesCategoriesParetoSummary
    rows: list[SalesCategoryParetoRow]


@dataclass(frozen=True)
class SalesByOrderRow:
    sales_source: str
    order_id: int
    order_number: str
    order_date: date | None
    customer_name: str | None
    channel_name: str | None
    order_status: str
    net_sales: Decimal
    total_quantity: Decimal
    total_discount: Decimal
    lines_count: int
    items_count: int
    categories_count: int
    average_line_value: Decimal
    detail_url: str


@dataclass(frozen=True)
class SalesByOrderSummary:
    total_net_sales: Decimal
    total_orders: int
    total_quantity: Decimal
    average_order_value: Decimal
    b2b_orders: int
    b2c_orders: int


@dataclass(frozen=True)
class SalesByOrderResult:
    summary: SalesByOrderSummary
    rows: list[SalesByOrderRow]


def get_total_sales_rows(
    db: Session,
    *,
    date_from: date | None = None,
    date_to: date | None = None,
    sales_type: str = "all",
) -> list[TotalSalesRow]:
    normalized_sales_type = (sales_type or "all").strip().lower()
    if normalized_sales_type not in {"all", "b2b", "b2c"}:
        normalized_sales_type = "all"

    b2b_rows: list[TotalSalesRow] = []
    b2c_rows: list[TotalSalesRow] = []

    if normalized_sales_type in {"all", "b2b"}:
        b2b_rows = _get_b2b_sales_rows(db, date_from=date_from, date_to=date_to)
    if normalized_sales_type in {"all", "b2c"}:
        b2c_rows = _get_b2c_sales_rows(db, date_from=date_from, date_to=date_to)

    rows = b2b_rows + b2c_rows
    category_by_sku = _current_category_name_by_sku(db, {row.sku for row in rows})
    enriched_rows = [
        TotalSalesRow(
            sales_source=row.sales_source,
            order_id=row.order_id,
            order_number=row.order_number,
            order_date=row.order_date,
            customer_name=row.customer_name,
            channel_name=row.channel_name,
            sku=row.sku,
            description=row.description,
            # Category is resolved from current Product Master by SKU. This is not a historical snapshot.
            category_name=category_by_sku.get(row.sku),
            quantity=row.quantity,
            unit_price=row.unit_price,
            discount_amount=row.discount_amount,
            line_total=row.line_total,
            order_status=row.order_status,
            order_created_at=row.order_created_at,
            line_number=row.line_number,
        )
        for row in rows
    ]
    return sorted(
        enriched_rows,
        key=lambda row: (
            row.order_date,
            row.order_created_at,
            row.order_number,
        ),
        reverse=True,
    )


def get_sales_summary(
    db: Session,
    *,
    date_from: date | None = None,
    date_to: date | None = None,
    sales_type: str = "all",
) -> SalesSummary:
    rows = get_total_sales_rows(
        db,
        date_from=date_from,
        date_to=date_to,
        sales_type=sales_type,
    )
    total_bucket = _build_sales_summary_bucket("Total", rows, total_net_sales_base=None)
    b2b_rows = [row for row in rows if row.sales_source == "B2B"]
    b2c_rows = [row for row in rows if row.sales_source == "B2C"]
    b2b_bucket = _build_sales_summary_bucket("B2B", b2b_rows, total_net_sales_base=total_bucket.total_net_sales)
    b2c_bucket = _build_sales_summary_bucket("B2C", b2c_rows, total_net_sales_base=total_bucket.total_net_sales)
    total_percent = HUNDRED if total_bucket.total_net_sales > ZERO else ZERO
    total_bucket = SalesSummaryBucket(
        label=total_bucket.label,
        total_net_sales=total_bucket.total_net_sales,
        total_orders=total_bucket.total_orders,
        total_lines=total_bucket.total_lines,
        total_quantity=total_bucket.total_quantity,
        total_discount=total_bucket.total_discount,
        average_order_value=total_bucket.average_order_value,
        average_line_value=total_bucket.average_line_value,
        sales_percent=total_percent,
    )
    return SalesSummary(total=total_bucket, b2b=b2b_bucket, b2c=b2c_bucket)


def get_sales_items_pareto(
    db: Session,
    *,
    date_from: date | None = None,
    date_to: date | None = None,
    sales_type: str = "all",
) -> SalesItemsPareto:
    rows = get_total_sales_rows(
        db,
        date_from=date_from,
        date_to=date_to,
        sales_type=sales_type,
    )
    return _build_sales_items_pareto(rows)


def get_sales_categories_pareto(
    db: Session,
    *,
    date_from: date | None = None,
    date_to: date | None = None,
    sales_type: str = "all",
) -> SalesCategoriesPareto:
    rows = get_total_sales_rows(
        db,
        date_from=date_from,
        date_to=date_to,
        sales_type=sales_type,
    )
    return _build_sales_categories_pareto(rows)


def get_sales_by_order(
    db: Session,
    *,
    date_from: date | None = None,
    date_to: date | None = None,
    sales_type: str = "all",
) -> SalesByOrderResult:
    rows = get_total_sales_rows(
        db,
        date_from=date_from,
        date_to=date_to,
        sales_type=sales_type,
    )
    return _build_sales_by_order(rows)


def _get_b2b_sales_rows(
    db: Session,
    *,
    date_from: date | None = None,
    date_to: date | None = None,
) -> list[TotalSalesRow]:
    query = (
        db.query(B2BSalesOrder, B2BSalesOrderLine)
        .join(B2BSalesOrderLine, B2BSalesOrderLine.sales_order_id == B2BSalesOrder.id)
        .filter(B2BSalesOrder.status == "invoiced")
    )
    if date_from is not None:
        query = query.filter(B2BSalesOrder.created_at >= datetime.combine(date_from, time.min))
    if date_to is not None:
        next_day = date_to + timedelta(days=1)
        # Keep the user-facing date_to inclusive by filtering before the next day at 00:00.
        query = query.filter(B2BSalesOrder.created_at < datetime.combine(next_day, time.min))

    rows: list[TotalSalesRow] = []
    for order, line in query.order_by(
        B2BSalesOrder.created_at.desc(),
        B2BSalesOrder.id.desc(),
        B2BSalesOrderLine.line_number.asc(),
    ).all():
        # For B2B, report date uses created_at because B2B orders do not have explicit order_date.
        rows.append(
            TotalSalesRow(
                sales_source="B2B",
                order_id=order.id,
                order_number=order.order_number,
                order_date=order.created_at.date(),
                customer_name=order.customer_name_snapshot,
                channel_name=order.b2b_channel_name_snapshot,
                sku=line.sku_snapshot,
                description=line.description_snapshot,
                category_name=None,
                quantity=line.quantity,
                unit_price=line.unit_price_snapshot,
                discount_amount=None,
                line_total=line.line_total,
                order_status=order.status,
                order_created_at=order.created_at,
                line_number=line.line_number,
            )
        )
    return rows


def _get_b2c_sales_rows(
    db: Session,
    *,
    date_from: date | None = None,
    date_to: date | None = None,
) -> list[TotalSalesRow]:
    query = (
        db.query(B2CSalesOrder, B2CSalesOrderLine)
        .join(B2CSalesOrderLine, B2CSalesOrderLine.sales_order_id == B2CSalesOrder.id)
        .filter(B2CSalesOrder.status == "invoiced")
    )
    if date_from is not None:
        query = query.filter(B2CSalesOrder.order_date >= date_from)
    if date_to is not None:
        query = query.filter(B2CSalesOrder.order_date <= date_to)

    rows: list[TotalSalesRow] = []
    for order, line in query.order_by(
        B2CSalesOrder.order_date.desc(),
        B2CSalesOrder.id.desc(),
        B2CSalesOrderLine.line_number.asc(),
    ).all():
        rows.append(
            TotalSalesRow(
                sales_source="B2C",
                order_id=order.id,
                order_number=order.order_number,
                order_date=order.order_date,
                customer_name=order.customer_name,
                channel_name=order.channel,
                sku=line.sku_snapshot,
                description=line.description_snapshot,
                category_name=None,
                quantity=line.quantity,
                unit_price=line.unit_price_snapshot,
                discount_amount=line.discount_amount_snapshot,
                line_total=line.net_line_total_snapshot if line.net_line_total_snapshot is not None else line.line_total,
                order_status=order.status,
                order_created_at=order.created_at,
                line_number=line.line_number,
            )
        )
    return rows


def _current_category_name_by_sku(db: Session, skus: set[str]) -> dict[str, str | None]:
    if not skus:
        return {}
    rows = (
        db.query(Product.sku, ProductCategory.name)
        .outerjoin(ProductCategory, Product.category_id == ProductCategory.id)
        .filter(Product.sku.in_(skus))
        .all()
    )
    return {sku: category_name for sku, category_name in rows}


def _build_sales_summary_bucket(
    label: str,
    rows: list[TotalSalesRow],
    *,
    total_net_sales_base: Decimal | None,
) -> SalesSummaryBucket:
    total_net_sales = sum((row.line_total for row in rows), ZERO)
    total_lines = len(rows)
    total_quantity = sum((row.quantity for row in rows), ZERO)
    total_discount = sum((((row.discount_amount or ZERO) for row in rows)), ZERO)
    total_orders = len({(row.sales_source, row.order_id) for row in rows})
    average_order_value = _safe_divide(total_net_sales, Decimal(total_orders))
    average_line_value = _safe_divide(total_net_sales, Decimal(total_lines))
    if total_net_sales_base is None:
        sales_percent = ZERO
    else:
        sales_percent = _safe_percent(total_net_sales, total_net_sales_base)
    return SalesSummaryBucket(
        label=label,
        total_net_sales=total_net_sales,
        total_orders=total_orders,
        total_lines=total_lines,
        total_quantity=total_quantity,
        total_discount=total_discount,
        average_order_value=average_order_value,
        average_line_value=average_line_value,
        sales_percent=sales_percent,
    )


def _build_sales_items_pareto(rows: list[TotalSalesRow]) -> SalesItemsPareto:
    grouped_rows: dict[str, list[TotalSalesRow]] = {}
    for row in rows:
        sku = (row.sku or "").strip() or "(sin SKU)"
        grouped_rows.setdefault(sku, []).append(row)

    total_net_sales = sum((row.line_total for row in rows), ZERO)
    total_quantity = sum((row.quantity for row in rows), ZERO)

    ranked_base: list[tuple[str, str, Decimal, Decimal, int, int, Decimal]] = []
    for sku, sku_rows in grouped_rows.items():
        net_sales = sum((row.line_total for row in sku_rows), ZERO)
        quantity = sum((row.quantity for row in sku_rows), ZERO)
        lines = len(sku_rows)
        orders = len({(row.sales_source, row.order_id) for row in sku_rows})
        discount = sum(((row.discount_amount or ZERO) for row in sku_rows), ZERO)
        ranked_base.append(
            (
                sku,
                _resolve_pareto_description(sku_rows),
                net_sales,
                quantity,
                lines,
                orders,
                discount,
            )
        )

    ranked_base.sort(key=lambda item: (item[2], item[0]), reverse=True)

    pareto_rows: list[SalesItemParetoRow] = []
    running_sales = ZERO
    a_items_count = 0
    b_items_count = 0
    c_items_count = 0
    for index, item in enumerate(ranked_base, start=1):
        sku, description, net_sales, quantity, lines, orders, discount = item
        percent_of_total_sales = _safe_percent(net_sales, total_net_sales)
        running_sales += net_sales
        cumulative_percent = _safe_percent(running_sales, total_net_sales)
        pareto_class = _classify_pareto(cumulative_percent)
        if pareto_class == "A":
            a_items_count += 1
        elif pareto_class == "B":
            b_items_count += 1
        else:
            c_items_count += 1
        pareto_rows.append(
            SalesItemParetoRow(
                rank=index,
                sku=sku,
                description=description,
                net_sales=net_sales,
                quantity=quantity,
                lines=lines,
                orders=orders,
                discount=discount,
                percent_of_total_sales=percent_of_total_sales,
                cumulative_percent=cumulative_percent,
                pareto_class=pareto_class,
            )
        )

    summary = SalesItemsParetoSummary(
        total_net_sales=total_net_sales,
        total_items=len(pareto_rows),
        total_quantity=total_quantity,
        a_items_count=a_items_count,
        b_items_count=b_items_count,
        c_items_count=c_items_count,
    )
    return SalesItemsPareto(summary=summary, rows=pareto_rows)


def _build_sales_categories_pareto(rows: list[TotalSalesRow]) -> SalesCategoriesPareto:
    grouped_rows: dict[str, list[TotalSalesRow]] = {}
    for row in rows:
        category_name = (row.category_name or "").strip() or "(sin categoría)"
        grouped_rows.setdefault(category_name, []).append(row)

    total_net_sales = sum((row.line_total for row in rows), ZERO)
    total_quantity = sum((row.quantity for row in rows), ZERO)

    ranked_base: list[tuple[str, Decimal, Decimal, int, int, int, Decimal]] = []
    for category_name, category_rows in grouped_rows.items():
        net_sales = sum((row.line_total for row in category_rows), ZERO)
        quantity = sum((row.quantity for row in category_rows), ZERO)
        lines = len(category_rows)
        orders = len({(row.sales_source, row.order_id) for row in category_rows})
        items_count = len({((row.sku or "").strip() or "(sin SKU)") for row in category_rows})
        discount = sum(((row.discount_amount or ZERO) for row in category_rows), ZERO)
        ranked_base.append(
            (
                category_name,
                net_sales,
                quantity,
                lines,
                orders,
                items_count,
                discount,
            )
        )

    ranked_base.sort(key=lambda item: (item[1], item[0]), reverse=True)

    pareto_rows: list[SalesCategoryParetoRow] = []
    running_sales = ZERO
    a_categories_count = 0
    b_categories_count = 0
    c_categories_count = 0
    for index, item in enumerate(ranked_base, start=1):
        category_name, net_sales, quantity, lines, orders, items_count, discount = item
        percent_of_total_sales = _safe_percent(net_sales, total_net_sales)
        running_sales += net_sales
        cumulative_percent = _safe_percent(running_sales, total_net_sales)
        pareto_class = _classify_pareto(cumulative_percent)
        if pareto_class == "A":
            a_categories_count += 1
        elif pareto_class == "B":
            b_categories_count += 1
        else:
            c_categories_count += 1
        pareto_rows.append(
            SalesCategoryParetoRow(
                rank=index,
                category_name=category_name,
                net_sales=net_sales,
                quantity=quantity,
                lines=lines,
                orders=orders,
                items_count=items_count,
                discount=discount,
                percent_of_total_sales=percent_of_total_sales,
                cumulative_percent=cumulative_percent,
                pareto_class=pareto_class,
            )
        )

    summary = SalesCategoriesParetoSummary(
        total_net_sales=total_net_sales,
        total_categories=len(pareto_rows),
        total_quantity=total_quantity,
        a_categories_count=a_categories_count,
        b_categories_count=b_categories_count,
        c_categories_count=c_categories_count,
    )
    return SalesCategoriesPareto(summary=summary, rows=pareto_rows)


def _build_sales_by_order(rows: list[TotalSalesRow]) -> SalesByOrderResult:
    grouped_rows: dict[tuple[str, int], list[TotalSalesRow]] = {}
    for row in rows:
        grouped_rows.setdefault((row.sales_source, row.order_id), []).append(row)

    order_rows: list[SalesByOrderRow] = []
    for (sales_source, order_id), order_group_rows in grouped_rows.items():
        first_row = order_group_rows[0]
        net_sales = sum((row.line_total for row in order_group_rows), ZERO)
        total_quantity = sum((row.quantity for row in order_group_rows), ZERO)
        total_discount = sum(((row.discount_amount or ZERO) for row in order_group_rows), ZERO)
        lines_count = len(order_group_rows)
        items_count = len({((row.sku or "").strip() or "(sin SKU)") for row in order_group_rows})
        categories_count = len({((row.category_name or "").strip() or "(sin categoría)") for row in order_group_rows})
        average_line_value = _safe_divide(net_sales, Decimal(lines_count))
        detail_url = f"/b2b/orders/{order_id}" if sales_source == "B2B" else f"/b2c/orders/{order_id}"
        order_rows.append(
            SalesByOrderRow(
                sales_source=sales_source,
                order_id=order_id,
                order_number=first_row.order_number,
                order_date=first_row.order_date,
                customer_name=first_row.customer_name,
                channel_name=first_row.channel_name,
                order_status=first_row.order_status,
                net_sales=net_sales,
                total_quantity=total_quantity,
                total_discount=total_discount,
                lines_count=lines_count,
                items_count=items_count,
                categories_count=categories_count,
                average_line_value=average_line_value,
                detail_url=detail_url,
            )
        )

    order_rows.sort(
        key=lambda row: (
            row.order_date is not None,
            row.order_date or date.min,
            row.order_number,
        ),
        reverse=True,
    )

    total_net_sales = sum((row.net_sales for row in order_rows), ZERO)
    total_orders = len(order_rows)
    total_quantity = sum((row.total_quantity for row in order_rows), ZERO)
    average_order_value = _safe_divide(total_net_sales, Decimal(total_orders))
    b2b_orders = sum((1 for row in order_rows if row.sales_source == "B2B"))
    b2c_orders = sum((1 for row in order_rows if row.sales_source == "B2C"))
    summary = SalesByOrderSummary(
        total_net_sales=total_net_sales,
        total_orders=total_orders,
        total_quantity=total_quantity,
        average_order_value=average_order_value,
        b2b_orders=b2b_orders,
        b2c_orders=b2c_orders,
    )
    return SalesByOrderResult(summary=summary, rows=order_rows)


def _resolve_pareto_description(rows: list[TotalSalesRow]) -> str:
    # Pareto groups by SKU; description is informational and resolved from the most frequent
    # non-empty historical snapshot for that SKU.
    non_empty_descriptions = [(row.description or "").strip() for row in rows if (row.description or "").strip()]
    if not non_empty_descriptions:
        return ""
    description_counts = Counter(non_empty_descriptions)
    max_count = max(description_counts.values())
    for description in non_empty_descriptions:
        if description_counts[description] == max_count:
            return description
    return non_empty_descriptions[0]


def _classify_pareto(cumulative_percent: Decimal) -> str:
    # Pareto class uses the cumulative percentage after adding the item:
    # A if cumulative_percent <= 80, B if cumulative_percent <= 95, C otherwise.
    if cumulative_percent <= Decimal("80"):
        return "A"
    if cumulative_percent <= Decimal("95"):
        return "B"
    return "C"


def _safe_divide(numerator: Decimal, denominator: Decimal) -> Decimal:
    if denominator == ZERO:
        return ZERO
    return numerator / denominator


def _safe_percent(part: Decimal, whole: Decimal) -> Decimal:
    if whole == ZERO:
        return ZERO
    return ((part * HUNDRED) / whole).quantize(PERCENT_QUANT)
