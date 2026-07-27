from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, ROUND_HALF_UP

from sqlalchemy.orm import Session, joinedload

from app.models import B2CSalesOrder, B2CSalesOrderLine, InventoryBalance, InventoryTransaction, Product


DECIMAL_QUANT = Decimal("0.0001")
ZERO = Decimal("0")
LOYVERSE_B2C_REPORTING_SOURCE = "loyverse_b2c_l1b2"


@dataclass(frozen=True)
class B2CInventoryImpactLineRow:
    order_id: int
    order_number: str
    order_date: date
    receipt_number: str
    receipt_date: object
    line_id: int
    line_number: int
    sku: str
    product_id: int | None
    product_name: str
    quantity: Decimal
    stock_current: Decimal
    stock_after: Decimal
    cost_unit: Decimal | None
    cost_total: Decimal | None
    status: str
    notes: list[str]


@dataclass(frozen=True)
class B2CInventoryImpactProductRow:
    product_id: int | None
    sku: str
    product_name: str
    stock_current: Decimal
    simulated_qty: Decimal
    stock_after: Decimal
    cost_unit: Decimal | None
    cost_total: Decimal | None
    alerts: list[str]


@dataclass(frozen=True)
class B2CInventoryImpactSummary:
    orders_included: int
    lines_included: int
    affected_products: int
    total_simulated_qty: Decimal
    total_estimated_cost: Decimal
    products_with_negative_stock: int
    lines_without_product: int
    lines_without_cost: int
    lines_possibly_already_impacted: int


@dataclass(frozen=True)
class B2CInventoryImpactPreviewResult:
    start_date: date
    end_date: date
    summary: B2CInventoryImpactSummary
    product_rows: list[B2CInventoryImpactProductRow]
    line_rows: list[B2CInventoryImpactLineRow]
    warnings: list[str]
    limitations: list[str]


def build_b2c_inventory_impact_preview(
    db: Session,
    start_date: date,
    end_date: date,
) -> B2CInventoryImpactPreviewResult:
    orders = _load_b2c_loyverse_orders(db, start_date, end_date)
    lines = _load_order_lines(orders)
    products_by_sku = _load_products_by_sku(db, lines)
    balances_by_product_id = _load_inventory_balances_for_products(db, products_by_sku.values())
    existing_impacts = _load_existing_b2c_inventory_transactions(db, orders)

    line_rows = [
        _build_line_impact_row(
            order=order,
            line=line,
            product=_resolve_line_product(line, products_by_sku),
            balances_by_product_id=balances_by_product_id,
            existing_impacts=existing_impacts,
        )
        for order in orders
        for line in sorted(order.lines, key=lambda current: (current.line_number, current.id))
    ]
    product_rows = _build_product_summary(line_rows)

    summary = B2CInventoryImpactSummary(
        orders_included=len(orders),
        lines_included=len(line_rows),
        affected_products=sum(1 for row in product_rows if row.product_id is not None),
        total_simulated_qty=_sum_money(row.quantity for row in line_rows),
        total_estimated_cost=_sum_optional_money(row.cost_total for row in line_rows),
        products_with_negative_stock=sum(1 for row in product_rows if _detect_negative_stock(row.stock_after)),
        lines_without_product=sum(1 for row in line_rows if _detect_missing_product(row)),
        lines_without_cost=sum(1 for row in line_rows if _detect_missing_cost(row)),
        lines_possibly_already_impacted=sum(1 for row in line_rows if _detect_possible_already_impacted(row)),
    )
    return B2CInventoryImpactPreviewResult(
        start_date=start_date,
        end_date=end_date,
        summary=summary,
        product_rows=product_rows,
        line_rows=line_rows,
        warnings=[],
        limitations=[
            "This screen is read-only. It does not create inventory transactions or change stock.",
            "Stock current is based on InventoryBalance.on_hand_qty. Product.current_inventory_qty is not used as the official stock source.",
            "Line-level stock after uses current stock minus that line quantity. Product summary aggregates repeated lines by product.",
        ],
    )


def _load_b2c_loyverse_orders(db: Session, start_date: date, end_date: date) -> list[B2CSalesOrder]:
    return (
        db.query(B2CSalesOrder)
        .options(joinedload(B2CSalesOrder.lines))
        .filter(
            B2CSalesOrder.status == "invoiced",
            B2CSalesOrder.loyverse_source == LOYVERSE_B2C_REPORTING_SOURCE,
            B2CSalesOrder.order_date >= start_date,
            B2CSalesOrder.order_date <= end_date,
        )
        .order_by(B2CSalesOrder.order_date, B2CSalesOrder.id)
        .all()
    )


def _load_order_lines(orders: list[B2CSalesOrder]) -> list[B2CSalesOrderLine]:
    return [
        line
        for order in orders
        for line in order.lines
    ]


def _load_products_by_sku(db: Session, lines: list[B2CSalesOrderLine]) -> dict[str, Product]:
    skus = sorted({(line.sku_snapshot or "").strip() for line in lines if (line.sku_snapshot or "").strip()})
    if not skus:
        return {}
    products = db.query(Product).filter(Product.sku.in_(skus)).all()
    return {product.sku.strip(): product for product in products if (product.sku or "").strip()}


def _resolve_line_product(line: B2CSalesOrderLine, products_by_sku: dict[str, Product]) -> Product | None:
    sku = (line.sku_snapshot or "").strip()
    if not sku:
        return None
    return products_by_sku.get(sku)


def _load_inventory_balances_for_products(
    db: Session,
    products: object,
) -> dict[int, InventoryBalance]:
    product_list = [product for product in products if product is not None]
    if not product_list:
        return {}
    product_ids = [product.id for product in product_list]
    balances = db.query(InventoryBalance).filter(InventoryBalance.product_id.in_(product_ids)).all()
    return {balance.product_id: balance for balance in balances}


def _load_existing_b2c_inventory_transactions(
    db: Session,
    orders: list[B2CSalesOrder],
) -> dict[tuple[int, int | None], bool]:
    if not orders:
        return {}
    order_ids = [order.id for order in orders]
    transactions = (
        db.query(InventoryTransaction)
        .filter(
            InventoryTransaction.transaction_type == "b2c_sale",
            InventoryTransaction.source_type == "b2c_order",
            InventoryTransaction.source_id.in_(order_ids),
        )
        .all()
    )
    existing: dict[tuple[int, int | None], bool] = {}
    for transaction in transactions:
        existing[(transaction.source_id or 0, transaction.source_line_id)] = True
        existing[(transaction.source_id or 0, None)] = True
    return existing


def _build_line_impact_row(
    *,
    order: B2CSalesOrder,
    line: B2CSalesOrderLine,
    product: Product | None,
    balances_by_product_id: dict[int, InventoryBalance],
    existing_impacts: dict[tuple[int, int | None], bool],
) -> B2CInventoryImpactLineRow:
    quantity = _money(line.quantity)
    stock_current = ZERO
    notes: list[str] = []

    if product is None:
        notes.append("missing_product")
    else:
        balance = balances_by_product_id.get(product.id)
        if balance is None:
            notes.append("missing_inventory_balance")
        else:
            stock_current = _money(balance.on_hand_qty)
        if not product.active:
            notes.append("inactive_product")

    stock_after = (stock_current - quantity).quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP)
    if _detect_negative_stock(stock_after):
        notes.append("negative_stock")

    cost_unit = _optional_money(line.cost_unit_snapshot)
    if cost_unit is None and product is not None:
        cost_unit = _optional_money(product.standard_cost)
    if cost_unit is None:
        notes.append("missing_cost")
        cost_total = None
    else:
        cost_total = _money(cost_unit * quantity)

    if existing_impacts.get((order.id, line.id)) or existing_impacts.get((order.id, None)):
        notes.append("possible_already_impacted")

    return B2CInventoryImpactLineRow(
        order_id=order.id,
        order_number=order.order_number,
        order_date=order.order_date,
        receipt_number=order.loyverse_receipt_number or "",
        receipt_date=order.loyverse_receipt_date,
        line_id=line.id,
        line_number=line.line_number,
        sku=(line.sku_snapshot or "").strip(),
        product_id=product.id if product is not None else None,
        product_name=product.name if product is not None else (line.description_snapshot or "N/A"),
        quantity=quantity,
        stock_current=stock_current,
        stock_after=stock_after,
        cost_unit=cost_unit,
        cost_total=cost_total,
        status=_line_status(notes),
        notes=notes,
    )


def _build_product_summary(line_rows: list[B2CInventoryImpactLineRow]) -> list[B2CInventoryImpactProductRow]:
    grouped: dict[tuple[int | None, str], list[B2CInventoryImpactLineRow]] = {}
    for row in line_rows:
        grouped.setdefault((row.product_id, row.sku), []).append(row)

    product_rows: list[B2CInventoryImpactProductRow] = []
    for _, rows in sorted(grouped.items(), key=lambda item: ((item[0][1] or ""), (item[0][0] or 0))):
        first = rows[0]
        simulated_qty = _sum_money(row.quantity for row in rows)
        stock_current = first.stock_current
        stock_after = (stock_current - simulated_qty).quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP)
        cost_unit = first.cost_unit if all(row.cost_unit == first.cost_unit for row in rows) else None
        cost_total = _sum_optional_money(row.cost_total for row in rows)
        alerts = _dedupe_notes([note for row in rows for note in row.notes])
        if _detect_negative_stock(stock_after) and "negative_stock" not in alerts:
            alerts.append("negative_stock")
        product_rows.append(
            B2CInventoryImpactProductRow(
                product_id=first.product_id,
                sku=first.sku or "N/A",
                product_name=first.product_name,
                stock_current=stock_current,
                simulated_qty=simulated_qty,
                stock_after=stock_after,
                cost_unit=cost_unit,
                cost_total=cost_total if cost_total != ZERO or any(row.cost_total is not None for row in rows) else None,
                alerts=alerts,
            )
        )
    return product_rows


def _detect_missing_product(row: B2CInventoryImpactLineRow) -> bool:
    return row.product_id is None


def _detect_missing_cost(row: B2CInventoryImpactLineRow) -> bool:
    return row.cost_unit is None


def _detect_negative_stock(stock_after: Decimal) -> bool:
    return stock_after < ZERO


def _detect_possible_already_impacted(row: B2CInventoryImpactLineRow) -> bool:
    return "possible_already_impacted" in row.notes


def _line_status(notes: list[str]) -> str:
    if "possible_already_impacted" in notes:
        return "possible_already_impacted"
    if "missing_product" in notes:
        return "missing_product"
    if "missing_cost" in notes:
        return "missing_cost"
    if "negative_stock" in notes:
        return "negative_stock"
    if "inactive_product" in notes:
        return "inactive_product"
    return "ok"


def _dedupe_notes(notes: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for note in notes:
        if note not in seen:
            ordered.append(note)
            seen.add(note)
    return ordered


def _money(value: Decimal | int | str | None) -> Decimal:
    if value is None:
        return ZERO
    return Decimal(str(value)).quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP)


def _optional_money(value: Decimal | int | str | None) -> Decimal | None:
    if value is None:
        return None
    return _money(value)


def _sum_money(values) -> Decimal:
    return sum((_money(value) for value in values), ZERO).quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP)


def _sum_optional_money(values) -> Decimal:
    total = ZERO
    for value in values:
        if value is not None:
            total += _money(value)
    return total.quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP)
