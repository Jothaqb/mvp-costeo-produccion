from collections import Counter
from datetime import date, datetime, timedelta
from urllib.parse import quote

from fastapi import Depends, FastAPI, File, Form, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session, joinedload

from app.database import (
    Base,
    engine,
    ensure_b2b_invoice_snapshot_columns,
    ensure_b2b_sales_followup_columns,
    ensure_b2b_loyverse_mapping_tables,
    ensure_app_sequences_table,
    ensure_inventory_ledger_tables,
    ensure_product_default_route_column,
    ensure_product_is_manufactured_column,
    ensure_product_loyverse_mapping_columns,
    ensure_product_planning_columns,
    ensure_purchase_order_tables,
    ensure_production_loyverse_inventory_sync_columns,
    ensure_sprint4_costing_columns,
    ensure_sprint5_comparison_columns,
    ensure_sprint6_loyverse_cost_sync_columns,
    ensure_sprint7c_lot_columns_and_tables,
    get_db,
)
from app.models import (
    Activity,
    B2BCustomer,
    B2BCustomerProduct,
    B2BSalesOrder,
    B2BSalesOrderLine,
    ImportBatch,
    ImportedBomHeader,
    ImportedBomLine,
    InventoryBalance,
    InventoryTransaction,
    LaborRate,
    LoyverseCustomerMapping,
    LoyversePaymentTypeMapping,
    LoyverseVariantMapping,
    Machine,
    MachineRate,
    OverheadRate,
    Product,
    ProductionOrder,
    ProductionOrderActivity,
    ProductionOrderMaterial,
    PurchaseOrder,
    PurchaseOrderLine,
    Route,
    RouteActivity,
)
from app.schemas import ComponentType, ProcessType, ProductionOrderStatus
from app.services.b2b_sales_service import (
    B2BValidationError,
    add_customer_product,
    change_sales_order_status,
    create_customer,
    create_sales_order,
    invoice_b2b_order_in_erp,
    update_customer,
    update_customer_product,
    update_sales_order_lines,
)
from app.services.b2b_loyverse_mapping_service import (
    LoyverseMappingSyncError,
    refresh_loyverse_customer_mappings,
    refresh_loyverse_payment_type_mappings,
    refresh_loyverse_variant_mappings,
)
from app.services.config_service import (
    ValidationError,
    parse_decimal,
    validate_labor_rate,
    validate_machine_rate,
    validate_overhead_rate,
    validate_process_type,
    validate_route_activity_sequence,
    validate_unique_code,
)
from app.services.import_service import import_loyverse_csv
from app.services.inventory_ledger_service import (
    InventoryInitializationResult,
    InventoryLedgerValidationError,
    initialize_inventory_opening_balances,
)
from app.services.planning_loyverse_refresh_service import (
    PlanningLoyverseRefreshError,
    refresh_planning_inventory_and_cost,
)
from app.services.purchase_order_service import (
    PurchaseOrderValidationError,
    build_purchase_order_prefill,
    can_receive_purchase_order,
    create_purchase_order_receive_token,
    create_purchase_order,
    is_purchase_order_editable,
    list_all_product_suppliers,
    list_purchase_order_suppliers,
    pending_quantity_for_line,
    receive_purchase_order_with_inventory_posting,
    update_purchase_order,
)
from app.services.planning_service import (
    PRODUCT_TYPE_MANUFACTURED,
    PRODUCT_TYPE_PURCHASED,
    PlanningValidationError,
    build_customer_order_requirements,
    build_mps_groups,
    build_mrp_result,
    build_planning_rows,
    clear_planner_quantities,
    list_inventory_parameter_products,
    list_routes_for_filter,
    list_suppliers_for_filter,
    normalize_product_type,
    parse_moq,
    parse_planner_quantity,
    update_product_moqs,
    update_product_planner_quantities,
)
from app.services.production_loyverse_inventory_preview_service import (
    ProductionInventoryPreviewError,
    build_production_inventory_preview,
)
from app.services.production_loyverse_inventory_readiness_service import build_production_inventory_readiness
from app.services.production_loyverse_inventory_sync_service import (
    ProductionInventorySyncError,
    sync_production_inventory_to_loyverse,
)
from app.services.production_order_service import (
    ProductionOrderValidationError,
    close_order_with_inventory_posting,
    create_production_order,
    parse_optional_decimal,
    start_order,
    update_activity_capture,
    update_order_bom,
    update_yield_capture,
)


Base.metadata.create_all(bind=engine)
ensure_product_default_route_column()
ensure_product_is_manufactured_column()
ensure_product_loyverse_mapping_columns()
ensure_product_planning_columns()
ensure_app_sequences_table()
ensure_inventory_ledger_tables()
ensure_purchase_order_tables()
ensure_sprint4_costing_columns()
ensure_sprint5_comparison_columns()
ensure_sprint6_loyverse_cost_sync_columns()
ensure_production_loyverse_inventory_sync_columns()
ensure_sprint7c_lot_columns_and_tables()
ensure_b2b_sales_followup_columns()
ensure_b2b_invoice_snapshot_columns()
ensure_b2b_loyverse_mapping_tables()

app = FastAPI(title="Real Production Costing MVP")
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")


def _redirect(url: str) -> RedirectResponse:
    return RedirectResponse(url=url, status_code=303)


def _parse_optional_date(value: str) -> date | None:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def _process_types() -> list[str]:
    return [item.value for item in ProcessType]


def _production_order_statuses() -> list[str]:
    return [item.value for item in ProductionOrderStatus]


def _b2b_statuses() -> list[str]:
    return ["draft", "in_process", "invoiced"]


def _line_inputs_from_form(form, prefix: str, count: int | None = None) -> list[dict[str, str]]:
    indexes = [str(index).strip() for index in form.getlist(f"{prefix}_index") if str(index).strip()]
    if not indexes and count is not None:
        indexes = [str(index) for index in range(1, count + 1)]
    return [
        {
            "sku": str(form.get(f"{prefix}_sku_{index}", "")),
            "quantity": str(form.get(f"{prefix}_quantity_{index}", "")),
        }
        for index in indexes
    ]



def _purchase_order_line_inputs_from_form(form) -> list[dict[str, str]]:
    indexes = [str(index).strip() for index in form.getlist("line_index") if str(index).strip()]
    return [
        {
            "sku": str(form.get(f"line_sku_{index}", "")),
            "description": str(form.get(f"line_description_{index}", "")),
            "quantity": str(form.get(f"line_quantity_{index}", "")),
            "unit_cost": str(form.get(f"line_unit_cost_{index}", "")),
        }
        for index in indexes
    ]


def _purchase_order_receive_inputs_from_form(form) -> dict[int, str]:
    line_ids = [str(line_id).strip() for line_id in form.getlist("line_id") if str(line_id).strip()]
    return {
        int(line_id): str(form.get(f"receive_now_{line_id}", "0"))
        for line_id in line_ids
    }


def _purchase_order_history_query(supplier: str, start_date: str, end_date: str) -> str:
    query = []
    if supplier:
        query.append(f"supplier={quote(supplier)}")
    if start_date:
        query.append(f"start_date={quote(start_date)}")
    if end_date:
        query.append(f"end_date={quote(end_date)}")
    return "&".join(query)


def _purchase_order_receive_rows(
    db: Session,
    order_id: int,
    receive_now_inputs: dict[int, str] | None = None,
) -> list[dict[str, object]]:
    receive_now_inputs = receive_now_inputs or {}
    lines = (
        db.query(PurchaseOrderLine)
        .filter(PurchaseOrderLine.purchase_order_id == order_id)
        .order_by(PurchaseOrderLine.line_number)
        .all()
    )
    return [
        {
            "line": line,
            "ordered_quantity": line.quantity,
            "received_quantity": line.received_quantity,
            "pending_quantity": pending_quantity_for_line(line),
            "receive_now": receive_now_inputs.get(line.id, ""),
        }
        for line in lines
    ]


def _purchase_order_form_context(
    title: str,
    form_action: str,
    order_data: dict[str, object],
    supplier_options: list[str],
    product_options: list[dict[str, object]],
    error: str | None = None,
) -> dict[str, object]:
    lines = order_data.get("lines") or [{"sku": "", "description": "", "quantity": None, "unit_cost": None}]
    selected_supplier = str(order_data.get("supplier") or "").strip()
    normalized_suppliers = list(supplier_options)
    if selected_supplier and selected_supplier not in normalized_suppliers:
        normalized_suppliers.append(selected_supplier)
        normalized_suppliers.sort()
    return {
        "title": title,
        "form_action": form_action,
        "order_data": {
            "supplier": selected_supplier,
            "po_date": order_data.get("po_date") or date.today().isoformat(),
            "status": order_data.get("status") or "draft",
            "notes": order_data.get("notes") or "",
            "lines": lines,
        },
        "supplier_options": normalized_suppliers,
        "product_options": product_options,
        "error": error,
    }


def _inventory_opening_balance_exists(db: Session) -> bool:
    return (
        db.query(InventoryTransaction.id)
        .filter(InventoryTransaction.transaction_type == "opening_balance")
        .first()
        is not None
    )


def _inventory_init_context(
    result: InventoryInitializationResult | None = None,
    error: str | None = None,
    confirmation_required: bool = True,
    already_initialized: bool = False,
) -> dict[str, object]:
    return {
        "title": "Initialize Opening Balances",
        "result": result,
        "error": error,
        "confirmation_required": confirmation_required,
        "already_initialized": already_initialized,
    }


def _po_product_options(db: Session) -> list[dict[str, object]]:
    products = (
        db.query(Product)
        .filter(Product.is_manufactured.is_(False))
        .order_by(Product.sku)
        .all()
    )
    return [
        {
            "id": product.id,
            "sku": product.sku,
            "name": product.name,
            "supplier": (product.supplier or "").strip(),
            "unit_cost": product.standard_cost,
            "lookup_label": f"{product.sku} - {product.name}",
        }
        for product in products
    ]


def _manufactured_product_options(db: Session) -> list[dict[str, object]]:
    products = (
        db.query(Product)
        .filter(Product.is_manufactured.is_(True), Product.active.is_(True))
        .order_by(Product.sku)
        .all()
    )
    return [
        {
            "id": product.id,
            "sku": product.sku,
            "name": product.name,
            "lookup_label": f"{product.sku} - {product.name}",
        }
        for product in products
    ]
def _production_order_detail_response(
    order_id: int,
    request: Request,
    db: Session,
    error: str | None = None,
) -> HTMLResponse:
    order = db.query(ProductionOrder).filter(ProductionOrder.id == order_id).one()
    materials = (
        db.query(ProductionOrderMaterial)
        .filter(ProductionOrderMaterial.production_order_id == order_id)
        .order_by(ProductionOrderMaterial.id)
        .all()
    )
    activities = (
        db.query(ProductionOrderActivity)
        .filter(ProductionOrderActivity.production_order_id == order_id)
        .order_by(ProductionOrderActivity.sequence)
        .all()
    )
    activity_codes = {activity.activity_code_snapshot for activity in activities}
    activity_catalog_by_code = {}
    if activity_codes:
        activity_catalog_by_code = {
            activity.code: activity
            for activity in db.query(Activity).filter(Activity.code.in_(activity_codes)).all()
        }
    activity_permissions = {
        activity.id: {
            "applies_labor": activity_catalog_by_code[activity.activity_code_snapshot].applies_labor,
            "applies_machine": activity_catalog_by_code[activity.activity_code_snapshot].applies_machine,
        }
        for activity in activities
        if activity.activity_code_snapshot in activity_catalog_by_code
    }
    return templates.TemplateResponse(
        request=request,
        name="production_order_detail.html",
        context={
            "title": "Production Order",
            "order": order,
            "materials": materials,
            "activities": activities,
            "activity_permissions": activity_permissions,
            "inventory_readiness": build_production_inventory_readiness(db, order_id),
            "error": error,
            "is_closed": order.status == "closed",
        },
    )


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={"title": "Production Costing MVP"},
    )


@app.get("/production", response_class=HTMLResponse)
def production_home(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="production_home.html",
        context={"title": "Production"},
    )


@app.get("/production/routes", response_class=HTMLResponse)
def production_routes_home(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="production_routes_home.html",
        context={"title": "Production Routes"},
    )


@app.get("/sales", response_class=HTMLResponse)
def sales_home(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="sales_home.html",
        context={"title": "Sales"},
    )


@app.get("/planning", response_class=HTMLResponse)
def planning_home(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="planning_home.html",
        context={"title": "Planning"},
    )


@app.get("/inventory/balances", response_class=HTMLResponse)
def inventory_balances_report(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    balances = (
        db.query(InventoryBalance)
        .options(joinedload(InventoryBalance.product))
        .order_by(InventoryBalance.last_transaction_at.desc(), InventoryBalance.id.desc())
        .all()
    )
    return templates.TemplateResponse(
        request=request,
        name="inventory_balances.html",
        context={
            "title": "Inventory Balances",
            "balances": balances,
            "opening_balances_initialized": _inventory_opening_balance_exists(db),
        },
    )


@app.get("/inventory/transactions", response_class=HTMLResponse)
def inventory_transactions_report(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    transactions = (
        db.query(InventoryTransaction)
        .options(joinedload(InventoryTransaction.product))
        .order_by(InventoryTransaction.transaction_date.desc(), InventoryTransaction.id.desc())
        .all()
    )
    return templates.TemplateResponse(
        request=request,
        name="inventory_transactions.html",
        context={
            "title": "Inventory Transactions",
            "transactions": transactions,
            "opening_balances_initialized": _inventory_opening_balance_exists(db),
        },
    )


@app.get("/inventory/initialize-opening-balances", response_class=HTMLResponse)
def inventory_initialize_opening_balances_page(
    request: Request,
    db: Session = Depends(get_db),
) -> HTMLResponse:
    already_initialized = _inventory_opening_balance_exists(db)
    return templates.TemplateResponse(
        request=request,
        name="inventory_initialize.html",
        context=_inventory_init_context(
            already_initialized=already_initialized,
            confirmation_required=not already_initialized,
        ),
    )


@app.post("/inventory/initialize-opening-balances")
async def inventory_initialize_opening_balances_route(
    request: Request,
    db: Session = Depends(get_db),
) -> HTMLResponse:
    form = await request.form()
    already_initialized = _inventory_opening_balance_exists(db)
    if already_initialized:
        return templates.TemplateResponse(
            request=request,
            name="inventory_initialize.html",
            context=_inventory_init_context(
                error="Opening balances have already been initialized.",
                already_initialized=True,
                confirmation_required=False,
            ),
        )

    confirmation = str(form.get("confirm_initialize", "")).strip().lower()
    if confirmation != "yes":
        return templates.TemplateResponse(
            request=request,
            name="inventory_initialize.html",
            context=_inventory_init_context(
                error="Explicit confirmation is required to initialize opening balances.",
                already_initialized=False,
                confirmation_required=True,
            ),
        )

    try:
        result = initialize_inventory_opening_balances(db)
        return templates.TemplateResponse(
            request=request,
            name="inventory_initialize.html",
            context=_inventory_init_context(
                result=result,
                already_initialized=True,
                confirmation_required=False,
            ),
        )
    except InventoryLedgerValidationError as exc:
        db.rollback()
        return templates.TemplateResponse(
            request=request,
            name="inventory_initialize.html",
            context=_inventory_init_context(
                error=str(exc),
                already_initialized=_inventory_opening_balance_exists(db),
                confirmation_required=not _inventory_opening_balance_exists(db),
            ),
        )


@app.get("/planning/customer-order-requirements", response_class=HTMLResponse)
def customer_order_requirements(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    requirement_result = build_customer_order_requirements(db)
    return templates.TemplateResponse(
        request=request,
        name="customer_order_requirements.html",
        context={
            "title": "Customer Order Requirements",
            "result": requirement_result,
        },
    )


@app.get("/planning/inventory-parameters", response_class=HTMLResponse)
def inventory_parameters(
    request: Request,
    product_type: str = Query(PRODUCT_TYPE_MANUFACTURED),
    sku: str = Query(""),
    route_id: str = Query(""),
    supplier: str = Query(""),
    error: str | None = Query(default=None),
    message: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    normalized_type = normalize_product_type(product_type)
    products = list_inventory_parameter_products(
        db,
        normalized_type,
        sku=sku,
        route_id=route_id,
        supplier=supplier,
    )
    routes = list_routes_for_filter(db)
    suppliers = list_suppliers_for_filter(db)
    return templates.TemplateResponse(
        request=request,
        name="inventory_parameters.html",
        context={
            "title": "Inventory Parameters",
            "product_type": normalized_type,
            "products": products,
            "routes": routes,
            "suppliers": suppliers,
            "filters": {
                "sku": sku.strip(),
                "route_id": route_id.strip(),
                "supplier": supplier.strip(),
            },
            "sku": sku.strip(),
            "error": error,
            "message": message,
        },
    )


@app.post("/planning/inventory-parameters")
async def update_inventory_parameters(request: Request, db: Session = Depends(get_db)) -> Response:
    form = await request.form()
    product_type = normalize_product_type(str(form.get("product_type", PRODUCT_TYPE_MANUFACTURED)))
    sku = str(form.get("sku", "")).strip()
    route_id = str(form.get("route_id", "")).strip()
    supplier = str(form.get("supplier", "")).strip()
    moq_inputs: dict[int, str] = {}
    for key, value in form.items():
        key_text = str(key)
        if not key_text.startswith("moq_"):
            continue
        product_id = int(key_text.replace("moq_", "", 1))
        moq_inputs[product_id] = str(value)
    try:
        update_product_moqs(db, moq_inputs)
        query = f"product_type={quote(product_type)}&message={quote('MOQ values saved.')}"
        if sku:
            query += f"&sku={quote(sku)}"
        if route_id:
            query += f"&route_id={quote(route_id)}"
        if supplier:
            query += f"&supplier={quote(supplier)}"
        return _redirect(f"/planning/inventory-parameters?{query}")
    except PlanningValidationError as exc:
        db.rollback()
        query = f"product_type={quote(product_type)}&error={quote(str(exc))}"
        if sku:
            query += f"&sku={quote(sku)}"
        if route_id:
            query += f"&route_id={quote(route_id)}"
        if supplier:
            query += f"&supplier={quote(supplier)}"
        return _redirect(f"/planning/inventory-parameters?{query}")


@app.get("/planning/suggestions", response_class=HTMLResponse)
def planning_suggestions(
    request: Request,
    product_type: str = Query(PRODUCT_TYPE_MANUFACTURED),
    sku: str = Query(""),
    route_id: str = Query(""),
    supplier: str = Query(""),
    status: str = Query(""),
    needs_action: bool = Query(False),
    error: str | None = Query(default=None),
    message: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    normalized_type = normalize_product_type(product_type)
    requirement_result = build_customer_order_requirements(db)
    mrp_result = build_mrp_result(db)
    rows = build_planning_rows(
        db,
        normalized_type,
        sku=sku,
        route_id=route_id,
        supplier=supplier,
        needs_action=needs_action,
        status=status,
        requirement_result=requirement_result,
        mrp_result=mrp_result,
    )
    routes = list_routes_for_filter(db)
    suppliers = list_suppliers_for_filter(db)
    return templates.TemplateResponse(
        request=request,
        name="planning_suggestions.html",
        context={
            "title": "Open DDMRP",
            "product_type": normalized_type,
            "rows": rows,
            "routes": routes,
            "suppliers": suppliers,
            "statuses": ["Red", "Yellow", "Green", "Incomplete"],
            "requirement_has_warnings": requirement_result.has_warnings,
            "mrp_has_warnings": mrp_result.has_warnings,
            "view_sync_summary": _planning_view_sync_summary(rows),
            "error": error,
            "message": message,
            "filters": {
                "sku": sku.strip(),
                "route_id": route_id.strip(),
                "supplier": supplier.strip(),
                "status": status.strip(),
                "needs_action": needs_action,
            },
        },
    )


@app.post("/planning/suggestions/refresh-inventory-cost")
def refresh_planning_inventory_and_cost_route(
    product_type: str = Form(PRODUCT_TYPE_MANUFACTURED),
    sku: str = Form(""),
    route_id: str = Form(""),
    supplier: str = Form(""),
    status: str = Form(""),
    needs_action: bool = Form(False),
    db: Session = Depends(get_db),
) -> Response:
    query = _planning_suggestions_query(product_type, sku, route_id, supplier, status, needs_action)
    try:
        result = refresh_planning_inventory_and_cost(
            db,
            normalize_product_type(product_type),
            sku=sku,
            route_id=route_id,
            supplier=supplier,
            status=status,
            needs_action=needs_action,
        )
        message = (
            f"Inventory refreshed: {result.inventory_refreshed_count}. "
            f"Cost refreshed: {result.cost_refreshed_count}. "
            f"Warnings: {result.warning_count}."
        )
        return _redirect(f"/planning/suggestions?{query}&message={quote(message)}")
    except PlanningLoyverseRefreshError as exc:
        db.rollback()
        return _redirect(f"/planning/suggestions?{query}&error={quote(str(exc))}")


@app.post("/planning/suggestions/planner-quantities")
async def update_planner_quantities(request: Request, db: Session = Depends(get_db)) -> Response:
    form = await request.form()
    product_type = normalize_product_type(str(form.get("product_type", PRODUCT_TYPE_MANUFACTURED)))
    sku = str(form.get("sku", "")).strip()
    route_id = str(form.get("route_id", "")).strip()
    supplier = str(form.get("supplier", "")).strip()
    status = str(form.get("status", "")).strip()
    needs_action = str(form.get("needs_action", "")).lower() in {"1", "true", "yes", "on"}
    quantity_inputs: dict[int, str] = {}
    for key, value in form.items():
        key_text = str(key)
        if not key_text.startswith("planner_qty_"):
            continue
        product_id = int(key_text.replace("planner_qty_", "", 1))
        quantity_inputs[product_id] = str(value)
    try:
        update_product_planner_quantities(db, quantity_inputs)
        query = _planning_suggestions_query(product_type, sku, route_id, supplier, status, needs_action)
        query += f"&message={quote('Planner quantities saved.')}"
        return _redirect(f"/planning/suggestions?{query}")
    except PlanningValidationError as exc:
        db.rollback()
        query = _planning_suggestions_query(product_type, sku, route_id, supplier, status, needs_action)
        query += f"&error={quote(str(exc))}"
        return _redirect(f"/planning/suggestions?{query}")


@app.post("/planning/suggestions/clear-planner-quantities")
def clear_planning_quantities(
    product_type: str = Form(PRODUCT_TYPE_MANUFACTURED),
    sku: str = Form(""),
    route_id: str = Form(""),
    supplier: str = Form(""),
    status: str = Form(""),
    needs_action: bool = Form(False),
    db: Session = Depends(get_db),
) -> Response:
    clear_planner_quantities(db)
    query = _planning_suggestions_query(product_type, sku, route_id, supplier, status, needs_action)
    query += f"&message={quote('Planner quantities cleared.')}"
    return _redirect(f"/planning/suggestions?{query}")


@app.get("/planning/suggestions/create-production-order")
def create_production_order_from_planning(
    product_id: int = Query(...),
    planner_qty: str = Query(""),
    db: Session = Depends(get_db),
) -> Response:
    quantity_error = "Planner quantity must be numeric and greater than 0."
    try:
        quantity = parse_planner_quantity(planner_qty)
    except PlanningValidationError:
        return _redirect(f"/planning/suggestions?product_type=manufactured&error={quote(quantity_error)}")

    if quantity is None or quantity <= 0:
        return _redirect(f"/planning/suggestions?product_type=manufactured&error={quote(quantity_error)}")

    product = (
        db.query(Product)
        .filter(
            Product.id == product_id,
            Product.is_manufactured.is_(True),
            Product.active.is_(True),
            Product.available_for_sale_gc.is_(True),
        )
        .one_or_none()
    )
    if product is None:
        return _redirect(
            "/planning/suggestions?product_type=manufactured&error="
            f"{quote('Selected product is not available for manufactured planning.')}"
        )

    return _redirect(f"/production-orders/new?product_id={product.id}&planned_qty={quote(str(quantity))}")



@app.get("/planning/purchase-orders", response_class=HTMLResponse)
def purchase_orders(
    request: Request,
    supplier: str = Query(""),
    start_date: str = Query(""),
    end_date: str = Query(""),
    error: str | None = Query(default=None),
    message: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    query = db.query(PurchaseOrder).options(joinedload(PurchaseOrder.lines)).order_by(PurchaseOrder.po_date.desc(), PurchaseOrder.id.desc())
    if supplier.strip():
        query = query.filter(PurchaseOrder.supplier_name_snapshot == supplier.strip())
    if start_date.strip():
        query = query.filter(PurchaseOrder.po_date >= _parse_optional_date(start_date.strip()))
    if end_date.strip():
        query = query.filter(PurchaseOrder.po_date <= _parse_optional_date(end_date.strip()))
    orders = query.all()
    suppliers = list_purchase_order_suppliers(db)
    return templates.TemplateResponse(
        request=request,
        name="purchase_orders.html",
        context={
            "title": "Purchase Orders",
            "orders": orders,
            "suppliers": suppliers,
            "filters": {
                "supplier": supplier.strip(),
                "start_date": start_date.strip(),
                "end_date": end_date.strip(),
            },
            "error": error,
            "message": message,
        },
    )


@app.get("/planning/purchase-orders/new", response_class=HTMLResponse)
def new_purchase_order(
    request: Request,
    product_id: int | None = Query(default=None),
    quantity: str = Query(""),
    db: Session = Depends(get_db),
) -> Response:
    supplier_options = list_all_product_suppliers(db)
    product_options = _po_product_options(db)
    order_data = {
        "supplier": "",
        "po_date": date.today().isoformat(),
        "status": "draft",
        "notes": "",
        "lines": [{"sku": "", "description": "", "quantity": None, "unit_cost": None}],
    }
    if product_id is not None or quantity.strip():
        quantity_error = "Planner quantity must be numeric and greater than 0."
        try:
            order_data = {
                **order_data,
                **build_purchase_order_prefill(db, product_id or 0, quantity),
            }
        except PurchaseOrderValidationError as exc:
            error_message = str(exc)
            if "Planner quantity" in error_message:
                error_message = quantity_error
            return _redirect(
                "/planning/suggestions?product_type=purchased&error="
                f"{quote(error_message)}"
            )

    return templates.TemplateResponse(
        request=request,
        name="purchase_order_form.html",
        context=_purchase_order_form_context(
            title="New Purchase Order",
            form_action="/planning/purchase-orders",
            order_data=order_data,
            supplier_options=supplier_options,
            product_options=product_options,
        ),
    )


@app.post("/planning/purchase-orders")
async def create_purchase_order_route(request: Request, db: Session = Depends(get_db)) -> Response:
    form = await request.form()
    supplier_options = list_all_product_suppliers(db)
    product_options = _po_product_options(db)
    supplier = str(form.get("supplier", ""))
    po_date_text = str(form.get("po_date", "")).strip()
    status = str(form.get("status", "draft"))
    notes = str(form.get("notes", ""))
    line_inputs = _purchase_order_line_inputs_from_form(form)
    order_data = {
        "supplier": supplier,
        "po_date": po_date_text,
        "status": status,
        "notes": notes,
        "lines": line_inputs,
    }
    try:
        po_date = _parse_optional_date(po_date_text)
        if po_date is None:
            raise PurchaseOrderValidationError("Date is required.")
        order = create_purchase_order(
            db=db,
            supplier=supplier,
            po_date=po_date,
            status=status,
            notes=notes,
            line_inputs=line_inputs,
        )
        return _redirect(f"/planning/purchase-orders/{order.id}")
    except PurchaseOrderValidationError as exc:
        db.rollback()
        return templates.TemplateResponse(
            request=request,
            name="purchase_order_form.html",
            context=_purchase_order_form_context(
                title="New Purchase Order",
                form_action="/planning/purchase-orders",
                order_data=order_data,
                supplier_options=supplier_options,
                product_options=product_options,
                error=str(exc),
            ),
        )


@app.get("/planning/purchase-orders/{po_id}", response_class=HTMLResponse)
def purchase_order_detail(request: Request, po_id: int, db: Session = Depends(get_db)) -> HTMLResponse:
    order = (
        db.query(PurchaseOrder)
        .options(joinedload(PurchaseOrder.lines))
        .filter(PurchaseOrder.id == po_id)
        .one()
    )
    return templates.TemplateResponse(
        request=request,
        name="purchase_order_detail.html",
        context={
            "title": f"Purchase Order {order.po_number}",
            "order": order,
            "can_edit": is_purchase_order_editable(order),
            "can_receive": can_receive_purchase_order(order),
            "pending_quantities": {line.id: pending_quantity_for_line(line) for line in order.lines},
            "error": request.query_params.get("error"),
            "message": request.query_params.get("message"),
        },
    )


@app.get("/planning/purchase-orders/{po_id}/edit", response_class=HTMLResponse)
def edit_purchase_order(request: Request, po_id: int, db: Session = Depends(get_db)) -> Response:
    supplier_options = list_all_product_suppliers(db)
    product_options = _po_product_options(db)
    order = (
        db.query(PurchaseOrder)
        .options(joinedload(PurchaseOrder.lines))
        .filter(PurchaseOrder.id == po_id)
        .one()
    )
    if not is_purchase_order_editable(order):
        return _redirect(
            f"/planning/purchase-orders/{order.id}?error={quote(f'Purchase orders in status {order.status} are read-only.')}"
        )
    order_data = {
        "supplier": order.supplier_name_snapshot,
        "po_date": order.po_date.isoformat(),
        "status": order.status,
        "notes": order.notes or "",
        "lines": [
            {
                "sku": line.sku_snapshot,
                "description": line.description_snapshot,
                "quantity": line.quantity,
                "unit_cost": line.unit_cost_snapshot,
            }
            for line in sorted(order.lines, key=lambda item: item.line_number)
        ],
    }
    return templates.TemplateResponse(
        request=request,
        name="purchase_order_form.html",
        context=_purchase_order_form_context(
            title=f"Edit Purchase Order {order.po_number}",
            form_action=f"/planning/purchase-orders/{order.id}",
            order_data=order_data,
            supplier_options=supplier_options,
            product_options=product_options,
        ),
    )


@app.post("/planning/purchase-orders/{po_id}")
async def update_purchase_order_route(request: Request, po_id: int, db: Session = Depends(get_db)) -> Response:
    form = await request.form()
    supplier_options = list_all_product_suppliers(db)
    product_options = _po_product_options(db)
    supplier = str(form.get("supplier", ""))
    po_date_text = str(form.get("po_date", "")).strip()
    status = str(form.get("status", "draft"))
    notes = str(form.get("notes", ""))
    line_inputs = _purchase_order_line_inputs_from_form(form)
    order = (
        db.query(PurchaseOrder)
        .options(joinedload(PurchaseOrder.lines))
        .filter(PurchaseOrder.id == po_id)
        .one()
    )
    order_data = {
        "supplier": supplier,
        "po_date": po_date_text,
        "status": status,
        "notes": notes,
        "lines": line_inputs,
    }
    try:
        po_date = _parse_optional_date(po_date_text)
        if po_date is None:
            raise PurchaseOrderValidationError("Date is required.")
        updated_order = update_purchase_order(
            db=db,
            order_id=po_id,
            supplier=supplier,
            po_date=po_date,
            status=status,
            notes=notes,
            line_inputs=line_inputs,
        )
        return _redirect(f"/planning/purchase-orders/{updated_order.id}")
    except PurchaseOrderValidationError as exc:
        db.rollback()
        return templates.TemplateResponse(
            request=request,
            name="purchase_order_form.html",
            context=_purchase_order_form_context(
                title=f"Edit Purchase Order {order.po_number}",
                form_action=f"/planning/purchase-orders/{order.id}",
                order_data=order_data,
                supplier_options=supplier_options,
                product_options=product_options,
                error=str(exc),
            ),
        )


@app.get("/planning/purchase-orders/{po_id}/print", response_class=HTMLResponse)
def print_purchase_order(request: Request, po_id: int, db: Session = Depends(get_db)) -> HTMLResponse:
    order = (
        db.query(PurchaseOrder)
        .options(joinedload(PurchaseOrder.lines))
        .filter(PurchaseOrder.id == po_id)
        .one()
    )
    return templates.TemplateResponse(
        request=request,
        name="purchase_order_print.html",
        context={
            "title": f"Print Purchase Order {order.po_number}",
            "order": order,
            "supplier_number": None,
            "supplier_address": None,
            "company_email": None,
            "company_address": None,
        },
    )


@app.get("/planning/purchase-orders/{po_id}/receive", response_class=HTMLResponse)
def receive_purchase_order_form(request: Request, po_id: int, db: Session = Depends(get_db)) -> Response:
    order = (
        db.query(PurchaseOrder)
        .options(joinedload(PurchaseOrder.lines))
        .filter(PurchaseOrder.id == po_id)
        .one()
    )
    if not can_receive_purchase_order(order):
        return _redirect(
            f"/planning/purchase-orders/{order.id}?error={quote(f'Purchase orders in status {order.status} cannot receive.')}"
        )
    receive_token = create_purchase_order_receive_token(db, order.id)
    return templates.TemplateResponse(
        request=request,
        name="purchase_order_receive.html",
        context={
            "title": f"Receive Purchase Order {order.po_number}",
            "order": order,
            "line_rows": _purchase_order_receive_rows(db, order.id),
            "receive_token": receive_token.token,
            "error": request.query_params.get("error"),
        },
    )


@app.post("/planning/purchase-orders/{po_id}/receive")
async def receive_purchase_order_route(request: Request, po_id: int, db: Session = Depends(get_db)) -> Response:
    form = await request.form()
    receive_inputs = _purchase_order_receive_inputs_from_form(form)
    receive_token = str(form.get("receive_token", ""))
    order = (
        db.query(PurchaseOrder)
        .options(joinedload(PurchaseOrder.lines))
        .filter(PurchaseOrder.id == po_id)
        .one()
    )
    try:
        order = receive_purchase_order_with_inventory_posting(
            db=db,
            order_id=po_id,
            receive_now_inputs=receive_inputs,
            receive_token=receive_token,
        )
        return _redirect(f"/planning/purchase-orders/{order.id}?message={quote('Receipt saved successfully.')}")
    except PurchaseOrderValidationError as exc:
        db.rollback()
        retry_token = create_purchase_order_receive_token(db, order.id)
        return templates.TemplateResponse(
            request=request,
            name="purchase_order_receive.html",
            context={
                "title": f"Receive Purchase Order {order.po_number}",
                "order": order,
                "line_rows": _purchase_order_receive_rows(db, order.id, receive_inputs),
                "receive_token": retry_token.token,
                "error": str(exc),
            },
        )


@app.get("/planning/mps", response_class=HTMLResponse)
def mps_report(
    request: Request,
    sku: str = Query(""),
    route_id: str = Query(""),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    groups = build_mps_groups(db, sku=sku, route_id=route_id)
    routes = list_routes_for_filter(db)
    return templates.TemplateResponse(
        request=request,
        name="mps_report.html",
        context={
            "title": "MPS",
            "groups": groups,
            "routes": routes,
            "filters": {"sku": sku.strip(), "route_id": route_id.strip()},
        },
    )


@app.get("/planning/mrp", response_class=HTMLResponse)
def mrp_report(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    result = build_mrp_result(db)
    return templates.TemplateResponse(
        request=request,
        name="mrp_report.html",
        context={"title": "MRP", "result": result},
    )


def _planning_suggestions_query(
    product_type: str,
    sku: str,
    route_id: str,
    supplier: str,
    status: str,
    needs_action: bool,
) -> str:
    query = f"product_type={quote(normalize_product_type(product_type))}"
    if sku:
        query += f"&sku={quote(sku)}"
    if route_id:
        query += f"&route_id={quote(route_id)}"
    if supplier:
        query += f"&supplier={quote(supplier)}"
    if status:
        query += f"&status={quote(status)}"
    if needs_action:
        query += "&needs_action=true"
    return query


def _format_sync_timestamp(value: datetime | None) -> str:
    if value is None:
        return "Not synced yet"
    return value.strftime("%Y-%m-%d %H:%M:%S UTC")


def _planning_view_sync_summary(rows: list) -> dict[str, str]:
    inventory_times = [
        row.product.loyverse_inventory_refreshed_at
        for row in rows
        if getattr(row.product, "loyverse_inventory_refreshed_at", None) is not None
    ]
    cost_times = [
        row.product.loyverse_cost_refreshed_at
        for row in rows
        if getattr(row.product, "loyverse_cost_refreshed_at", None) is not None
    ]
    return {
        "inventory_last_sync": _format_sync_timestamp(max(inventory_times) if inventory_times else None),
        "cost_last_sync": _format_sync_timestamp(max(cost_times) if cost_times else None),
    }

@app.get("/b2b/customers", response_class=HTMLResponse)
def b2b_customers(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    customers = db.query(B2BCustomer).order_by(B2BCustomer.customer_name).all()
    return templates.TemplateResponse(
        request=request,
        name="b2b_customers_list.html",
        context={"title": "B2B Customers", "customers": customers},
    )


@app.get("/b2b/customers/new", response_class=HTMLResponse)
def new_b2b_customer(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="b2b_customer_form.html",
        context={"title": "New B2B Customer", "customer": None, "error": None},
    )


@app.post("/b2b/customers")
def create_b2b_customer(
    request: Request,
    customer_name: str = Form(...),
    address: str = Form(""),
    province: str = Form(""),
    canton: str = Form(""),
    district: str = Form(""),
    legal_name: str = Form(""),
    legal_id: str = Form(""),
    phone: str = Form(""),
    loyverse_customer_id: str = Form(""),
    active: bool = Form(False),
    db: Session = Depends(get_db),
) -> Response:
    try:
        customer = create_customer(
            db,
            customer_name,
            address,
            province,
            canton,
            district,
            legal_name,
            legal_id,
            phone,
            loyverse_customer_id,
            active,
        )
        return _redirect(f"/b2b/customers/{customer.id}/products")
    except B2BValidationError as exc:
        db.rollback()
        return templates.TemplateResponse(
            request=request,
            name="b2b_customer_form.html",
            context={"title": "New B2B Customer", "customer": None, "error": str(exc)},
        )


@app.get("/b2b/customers/{customer_id}/edit", response_class=HTMLResponse)
def edit_b2b_customer(customer_id: int, request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    customer = db.query(B2BCustomer).filter(B2BCustomer.id == customer_id).one()
    return templates.TemplateResponse(
        request=request,
        name="b2b_customer_form.html",
        context={"title": "Edit B2B Customer", "customer": customer, "error": None},
    )


@app.post("/b2b/customers/{customer_id}/edit")
def update_b2b_customer(
    customer_id: int,
    request: Request,
    customer_name: str = Form(...),
    address: str = Form(""),
    province: str = Form(""),
    canton: str = Form(""),
    district: str = Form(""),
    legal_name: str = Form(""),
    legal_id: str = Form(""),
    phone: str = Form(""),
    loyverse_customer_id: str = Form(""),
    active: bool = Form(False),
    db: Session = Depends(get_db),
) -> Response:
    customer = db.query(B2BCustomer).filter(B2BCustomer.id == customer_id).one()
    try:
        update_customer(
            db,
            customer_id,
            customer_name,
            address,
            province,
            canton,
            district,
            legal_name,
            legal_id,
            phone,
            loyverse_customer_id,
            active,
        )
        return _redirect("/b2b/customers")
    except B2BValidationError as exc:
        db.rollback()
        return templates.TemplateResponse(
            request=request,
            name="b2b_customer_form.html",
            context={"title": "Edit B2B Customer", "customer": customer, "error": str(exc)},
        )


@app.get("/b2b/customers/{customer_id}/products", response_class=HTMLResponse)
def b2b_customer_products(customer_id: int, request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    customer = db.query(B2BCustomer).filter(B2BCustomer.id == customer_id).one()
    products = (
        db.query(B2BCustomerProduct)
        .filter(B2BCustomerProduct.customer_id == customer_id)
        .order_by(B2BCustomerProduct.sku)
        .all()
    )
    return templates.TemplateResponse(
        request=request,
        name="b2b_customer_products.html",
        context={"title": "B2B Customer Products", "customer": customer, "products": products, "error": None},
    )


@app.post("/b2b/customers/{customer_id}/products")
def add_b2b_customer_product(
    customer_id: int,
    request: Request,
    sku: str = Form(...),
    description: str = Form(...),
    distributor_price: str = Form(...),
    active: bool = Form(False),
    db: Session = Depends(get_db),
) -> Response:
    try:
        add_customer_product(db, customer_id, sku, description, distributor_price, active)
        return _redirect(f"/b2b/customers/{customer_id}/products")
    except B2BValidationError as exc:
        db.rollback()
        customer = db.query(B2BCustomer).filter(B2BCustomer.id == customer_id).one()
        products = (
            db.query(B2BCustomerProduct)
            .filter(B2BCustomerProduct.customer_id == customer_id)
            .order_by(B2BCustomerProduct.sku)
            .all()
        )
        return templates.TemplateResponse(
            request=request,
            name="b2b_customer_products.html",
            context={"title": "B2B Customer Products", "customer": customer, "products": products, "error": str(exc)},
        )


@app.post("/b2b/customers/{customer_id}/products/{product_line_id}/edit")
def update_b2b_customer_product(
    customer_id: int,
    product_line_id: int,
    request: Request,
    description: str = Form(...),
    distributor_price: str = Form(...),
    active: bool = Form(False),
    db: Session = Depends(get_db),
) -> Response:
    try:
        update_customer_product(db, customer_id, product_line_id, description, distributor_price, active)
        return _redirect(f"/b2b/customers/{customer_id}/products")
    except B2BValidationError as exc:
        db.rollback()
        customer = db.query(B2BCustomer).filter(B2BCustomer.id == customer_id).one()
        products = (
            db.query(B2BCustomerProduct)
            .filter(B2BCustomerProduct.customer_id == customer_id)
            .order_by(B2BCustomerProduct.sku)
            .all()
        )
        return templates.TemplateResponse(
            request=request,
            name="b2b_customer_products.html",
            context={"title": "B2B Customer Products", "customer": customer, "products": products, "error": str(exc)},
        )


@app.get("/b2b/loyverse-mappings", response_class=HTMLResponse)
def b2b_loyverse_mappings(
    request: Request,
    message: str | None = Query(default=None),
    error: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    customers = (
        db.query(LoyverseCustomerMapping)
        .order_by(LoyverseCustomerMapping.active.desc(), LoyverseCustomerMapping.customer_name, LoyverseCustomerMapping.phone)
        .limit(100)
        .all()
    )
    variants = (
        db.query(LoyverseVariantMapping)
        .order_by(LoyverseVariantMapping.active.desc(), LoyverseVariantMapping.sku, LoyverseVariantMapping.item_name)
        .limit(150)
        .all()
    )
    payment_types = (
        db.query(LoyversePaymentTypeMapping)
        .order_by(LoyversePaymentTypeMapping.active.desc(), LoyversePaymentTypeMapping.name)
        .limit(100)
        .all()
    )
    customer_count = db.query(LoyverseCustomerMapping).count()
    active_customer_count = db.query(LoyverseCustomerMapping).filter(LoyverseCustomerMapping.active.is_(True)).count()
    variant_count = db.query(LoyverseVariantMapping).count()
    active_variant_count = db.query(LoyverseVariantMapping).filter(LoyverseVariantMapping.active.is_(True)).count()
    payment_type_count = db.query(LoyversePaymentTypeMapping).count()
    active_payment_type_count = db.query(LoyversePaymentTypeMapping).filter(LoyversePaymentTypeMapping.active.is_(True)).count()
    return templates.TemplateResponse(
        request=request,
        name="b2b_loyverse_mappings.html",
        context={
            "title": "B2B Loyverse Mappings",
            "customers": customers,
            "variants": variants,
            "payment_types": payment_types,
            "customer_count": customer_count,
            "active_customer_count": active_customer_count,
            "variant_count": variant_count,
            "active_variant_count": active_variant_count,
            "payment_type_count": payment_type_count,
            "active_payment_type_count": active_payment_type_count,
            "message": message,
            "error": error,
        },
    )


@app.post("/b2b/loyverse-mappings/customers/refresh")
def refresh_b2b_loyverse_customers(db: Session = Depends(get_db)) -> Response:
    try:
        result = refresh_loyverse_customer_mappings(db)
        message = f"Customer mappings refreshed. Created: {result['created']}, updated: {result['updated']}, skipped: {result['skipped']}."
        return _redirect(f"/b2b/loyverse-mappings?message={quote(message)}")
    except LoyverseMappingSyncError as exc:
        db.rollback()
        return _redirect(f"/b2b/loyverse-mappings?error={quote(str(exc))}")


@app.post("/b2b/loyverse-mappings/variants/refresh")
def refresh_b2b_loyverse_variants(db: Session = Depends(get_db)) -> Response:
    try:
        result = refresh_loyverse_variant_mappings(db)
        message = f"Variant mappings refreshed. Created: {result['created']}, updated: {result['updated']}, skipped: {result['skipped']}."
        return _redirect(f"/b2b/loyverse-mappings?message={quote(message)}")
    except LoyverseMappingSyncError as exc:
        db.rollback()
        return _redirect(f"/b2b/loyverse-mappings?error={quote(str(exc))}")


@app.post("/b2b/loyverse-mappings/payment-types/refresh")
def refresh_b2b_loyverse_payment_types(db: Session = Depends(get_db)) -> Response:
    try:
        result = refresh_loyverse_payment_type_mappings(db)
        message = f"Payment type mappings refreshed. Created: {result['created']}, updated: {result['updated']}, skipped: {result['skipped']}."
        return _redirect(f"/b2b/loyverse-mappings?message={quote(message)}")
    except LoyverseMappingSyncError as exc:
        db.rollback()
        return _redirect(f"/b2b/loyverse-mappings?error={quote(str(exc))}")


@app.get("/b2b/orders", response_class=HTMLResponse)
def b2b_orders(
    request: Request,
    customer_id: str = Query(""),
    status: str = Query(""),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    filters = {"customer_id": customer_id.strip(), "status": status.strip()}
    order_query = db.query(B2BSalesOrder)
    if filters["customer_id"]:
        order_query = order_query.filter(B2BSalesOrder.customer_id == int(filters["customer_id"]))
    if filters["status"] in _b2b_statuses():
        order_query = order_query.filter(B2BSalesOrder.status == filters["status"])
    orders = order_query.order_by(B2BSalesOrder.created_at.desc(), B2BSalesOrder.id.desc()).all()
    customers = db.query(B2BCustomer).order_by(B2BCustomer.customer_name).all()
    return templates.TemplateResponse(
        request=request,
        name="b2b_orders_list.html",
        context={
            "title": "B2B Sales Orders",
            "orders": orders,
            "customers": customers,
            "statuses": _b2b_statuses(),
            "filters": filters,
        },
    )


@app.get("/b2b/orders/new", response_class=HTMLResponse)
def new_b2b_order(
    request: Request,
    customer_id: str = Query(""),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    customers = db.query(B2BCustomer).filter(B2BCustomer.active.is_(True)).order_by(B2BCustomer.customer_name).all()
    selected_customer = None
    catalog = []
    payment_types = (
        db.query(LoyversePaymentTypeMapping)
        .filter(LoyversePaymentTypeMapping.active.is_(True))
        .order_by(LoyversePaymentTypeMapping.name)
        .all()
    )
    if customer_id.strip():
        selected_customer = db.query(B2BCustomer).filter(B2BCustomer.id == int(customer_id)).one_or_none()
        if selected_customer is not None:
            catalog = (
                db.query(B2BCustomerProduct)
                .filter(
                    B2BCustomerProduct.customer_id == selected_customer.id,
                    B2BCustomerProduct.active.is_(True),
                )
                .order_by(B2BCustomerProduct.description)
                .all()
            )
    return templates.TemplateResponse(
        request=request,
        name="b2b_order_form.html",
        context={
            "title": "New B2B Order",
            "customers": customers,
            "selected_customer": selected_customer,
            "catalog": catalog,
            "payment_types": payment_types,
            "error": None,
            "min_delivery_date": date.today() + timedelta(days=1),
        },
    )


@app.post("/b2b/orders")
async def create_b2b_order(request: Request, db: Session = Depends(get_db)) -> Response:
    form = await request.form()
    customer_id = int(str(form.get("customer_id", "0") or "0"))
    delivery_date = datetime.strptime(str(form.get("delivery_date")), "%Y-%m-%d").date()
    line_inputs = _line_inputs_from_form(form, "line")
    observations = str(form.get("observations", ""))
    b2b_channel_id = str(form.get("b2b_channel_id", ""))
    try:
        order = create_sales_order(db, customer_id, delivery_date, line_inputs, observations, b2b_channel_id)
        return _redirect(f"/b2b/orders/{order.id}")
    except (B2BValidationError, ValueError) as exc:
        db.rollback()
        customers = db.query(B2BCustomer).filter(B2BCustomer.active.is_(True)).order_by(B2BCustomer.customer_name).all()
        selected_customer = db.query(B2BCustomer).filter(B2BCustomer.id == customer_id).one_or_none()
        catalog = []
        payment_types = (
            db.query(LoyversePaymentTypeMapping)
            .filter(LoyversePaymentTypeMapping.active.is_(True))
            .order_by(LoyversePaymentTypeMapping.name)
            .all()
        )
        if selected_customer is not None:
            catalog = (
                db.query(B2BCustomerProduct)
                .filter(B2BCustomerProduct.customer_id == selected_customer.id, B2BCustomerProduct.active.is_(True))
                .order_by(B2BCustomerProduct.description)
                .all()
            )
        return templates.TemplateResponse(
            request=request,
            name="b2b_order_form.html",
            context={
                "title": "New B2B Order",
                "customers": customers,
                "selected_customer": selected_customer,
                "catalog": catalog,
                "payment_types": payment_types,
                "error": str(exc),
                "min_delivery_date": date.today() + timedelta(days=1),
                "submitted_observations": observations,
                "submitted_b2b_channel_id": b2b_channel_id,
                "submitted_lines": line_inputs,
            },
        )


@app.get("/b2b/orders/{order_id}", response_class=HTMLResponse)
def b2b_order_detail(order_id: int, request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    order = db.query(B2BSalesOrder).filter(B2BSalesOrder.id == order_id).one()
    lines = (
        db.query(B2BSalesOrderLine)
        .filter(B2BSalesOrderLine.sales_order_id == order_id)
        .order_by(B2BSalesOrderLine.line_number)
        .all()
    )
    return templates.TemplateResponse(
        request=request,
        name="b2b_order_detail.html",
        context={"title": "B2B Order Detail", "order": order, "lines": lines, "error": None},
    )


@app.get("/b2b/orders/{order_id}/document", response_class=HTMLResponse)
def b2b_order_document(order_id: int, request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    order = db.query(B2BSalesOrder).filter(B2BSalesOrder.id == order_id).one()
    lines = (
        db.query(B2BSalesOrderLine)
        .filter(B2BSalesOrderLine.sales_order_id == order_id)
        .order_by(B2BSalesOrderLine.line_number)
        .all()
    )
    document_title = "Factura" if order.status == "invoiced" else "Proforma"
    return templates.TemplateResponse(
        request=request,
        name="b2b_order_document.html",
        context={"title": document_title, "document_title": document_title, "order": order, "lines": lines},
    )


@app.get("/b2b/orders/{order_id}/edit", response_class=HTMLResponse)
def edit_b2b_order(order_id: int, request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    order = db.query(B2BSalesOrder).filter(B2BSalesOrder.id == order_id).one()
    if order.status == "invoiced":
        return b2b_order_detail(order_id, request, db)
    lines = (
        db.query(B2BSalesOrderLine)
        .filter(B2BSalesOrderLine.sales_order_id == order_id)
        .order_by(B2BSalesOrderLine.line_number)
        .all()
    )
    catalog = (
        db.query(B2BCustomerProduct)
        .filter(B2BCustomerProduct.customer_id == order.customer_id, B2BCustomerProduct.active.is_(True))
        .order_by(B2BCustomerProduct.description)
        .all()
    )
    catalog_by_sku = {item.sku: item for item in catalog}
    payment_types = (
        db.query(LoyversePaymentTypeMapping)
        .filter(LoyversePaymentTypeMapping.active.is_(True))
        .order_by(LoyversePaymentTypeMapping.name)
        .all()
    )
    return templates.TemplateResponse(
        request=request,
        name="b2b_order_edit.html",
        context={
            "title": "Edit B2B Order",
            "order": order,
            "lines": lines,
            "catalog": catalog,
            "catalog_by_sku": catalog_by_sku,
            "payment_types": payment_types,
            "error": None,
        },
    )


@app.post("/b2b/orders/{order_id}/edit")
async def update_b2b_order(order_id: int, request: Request, db: Session = Depends(get_db)) -> Response:
    form = await request.form()
    line_ids = form.getlist("line_id")
    line_updates = [
        {
            "id": line_id,
            "sku": str(form.get(f"line_sku_{line_id}", "")),
            "quantity": str(form.get(f"line_quantity_{line_id}", "")),
        }
        for line_id in line_ids
    ]
    deleted_line_ids = [int(line_id) for line_id in form.getlist("delete_line_id")]
    new_line_inputs = _line_inputs_from_form(form, "new_line", 3)
    observations = str(form.get("observations", ""))
    b2b_channel_id = str(form.get("b2b_channel_id", ""))
    try:
        update_sales_order_lines(db, order_id, line_updates, deleted_line_ids, new_line_inputs, observations, b2b_channel_id)
        return _redirect(f"/b2b/orders/{order_id}")
    except B2BValidationError as exc:
        db.rollback()
        order = db.query(B2BSalesOrder).filter(B2BSalesOrder.id == order_id).one()
        lines = (
            db.query(B2BSalesOrderLine)
            .filter(B2BSalesOrderLine.sales_order_id == order_id)
            .order_by(B2BSalesOrderLine.line_number)
            .all()
        )
        catalog = (
            db.query(B2BCustomerProduct)
            .filter(B2BCustomerProduct.customer_id == order.customer_id, B2BCustomerProduct.active.is_(True))
            .order_by(B2BCustomerProduct.description)
            .all()
        )
        catalog_by_sku = {item.sku: item for item in catalog}
        payment_types = (
            db.query(LoyversePaymentTypeMapping)
            .filter(LoyversePaymentTypeMapping.active.is_(True))
            .order_by(LoyversePaymentTypeMapping.name)
            .all()
        )
        return templates.TemplateResponse(
            request=request,
            name="b2b_order_edit.html",
            context={
                "title": "Edit B2B Order",
                "order": order,
                "lines": lines,
                "catalog": catalog,
                "catalog_by_sku": catalog_by_sku,
                "payment_types": payment_types,
                "error": str(exc),
                "submitted_observations": observations,
                "submitted_b2b_channel_id": b2b_channel_id,
            },
        )


@app.post("/b2b/orders/{order_id}/status")
def update_b2b_order_status(
    request: Request,
    order_id: int,
    status: str = Form(...),
    db: Session = Depends(get_db),
) -> Response:
    try:
        if status == "invoiced":
            invoice_b2b_order_in_erp(db, order_id)
        else:
            change_sales_order_status(db, order_id, status)
        return _redirect(f"/b2b/orders/{order_id}")
    except B2BValidationError as exc:
        db.rollback()
        order = db.query(B2BSalesOrder).filter(B2BSalesOrder.id == order_id).one()
        lines = (
            db.query(B2BSalesOrderLine)
            .filter(B2BSalesOrderLine.sales_order_id == order_id)
            .order_by(B2BSalesOrderLine.line_number)
            .all()
        )
        return templates.TemplateResponse(
            request=request,
            name="b2b_order_detail.html",
            context={"title": "B2B Order Detail", "order": order, "lines": lines, "error": str(exc)},
        )

@app.get("/imports", response_class=HTMLResponse)
def list_imports(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    batches = db.query(ImportBatch).order_by(ImportBatch.imported_at.desc()).all()
    rows = []
    for batch in batches:
        headers = (
            db.query(ImportedBomHeader)
            .filter(ImportedBomHeader.import_batch_id == batch.id)
            .all()
        )
        line_count = (
            db.query(ImportedBomLine)
            .join(ImportedBomHeader)
            .filter(ImportedBomHeader.import_batch_id == batch.id)
            .count()
        )
        rows.append(
            {
                "id": batch.id,
                "file_name": batch.file_name,
                "imported_at": batch.imported_at,
                "product_count": len(headers),
                "line_count": line_count,
            }
        )

    return templates.TemplateResponse(
        request=request,
        name="import_list.html",
        context={"title": "Imported Data", "batches": rows},
    )


@app.get("/imports/new", response_class=HTMLResponse)
def new_import(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="import_upload.html",
        context={"title": "Import CSV"},
    )


@app.post("/imports")
async def create_import(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    content = await file.read()
    summary = import_loyverse_csv(db, file.filename or "loyverse.csv", content)
    return RedirectResponse(url=f"/imports/{summary.batch_id}", status_code=303)


@app.get("/imports/{batch_id}", response_class=HTMLResponse)
def review_import(
    batch_id: int,
    request: Request,
    q: str = Query(""),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    batch = db.query(ImportBatch).filter(ImportBatch.id == batch_id).one()
    search = q.strip()
    header_query = db.query(ImportedBomHeader).filter(ImportedBomHeader.import_batch_id == batch_id)
    if search:
        like_search = f"%{search}%"
        header_query = header_query.filter(
            ImportedBomHeader.product_sku.ilike(like_search)
            | ImportedBomHeader.product_name.ilike(like_search)
        )
    headers = header_query.order_by(ImportedBomHeader.product_sku).all()
    header_ids = [header.id for header in headers]

    line_query = (
        db.query(ImportedBomLine)
        .join(ImportedBomHeader)
        .options(joinedload(ImportedBomLine.bom_header))
        .filter(ImportedBomHeader.import_batch_id == batch_id)
    )
    if search:
        line_query = line_query.filter(ImportedBomLine.bom_header_id.in_(header_ids))
    lines = line_query.order_by(ImportedBomLine.source_row_number).all()
    component_type_counts = Counter(line.component_type for line in lines)
    for component_type in ComponentType:
        component_type_counts.setdefault(component_type.value, 0)

    return templates.TemplateResponse(
        request=request,
        name="import_review.html",
        context={
            "title": "Import Review",
            "batch": batch,
            "headers": headers,
            "lines": lines,
            "product_count": len(headers),
            "bom_line_count": len(lines),
            "component_type_counts": dict(component_type_counts),
            "unknown_count": component_type_counts[ComponentType.UNKNOWN.value],
            "search": search,
        },
    )


@app.get("/activities", response_class=HTMLResponse)
def list_activities(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    activities = db.query(Activity).order_by(Activity.code).all()
    return templates.TemplateResponse(
        request=request,
        name="activities_list.html",
        context={"title": "Activities", "activities": activities},
    )


@app.get("/activities/new", response_class=HTMLResponse)
def new_activity(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    machines = db.query(Machine).order_by(Machine.code).all()
    return templates.TemplateResponse(
        request=request,
        name="activity_form.html",
        context={"title": "New Activity", "activity": None, "machines": machines, "error": None},
    )


@app.post("/activities")
def create_activity(
    request: Request,
    code: str = Form(...),
    name: str = Form(...),
    description: str = Form(""),
    applies_labor: bool = Form(False),
    applies_machine: bool = Form(False),
    default_machine_id: str = Form(""),
    active: bool = Form(False),
    db: Session = Depends(get_db),
) -> Response:
    machines = db.query(Machine).order_by(Machine.code).all()
    try:
        validate_unique_code(db, Activity, code.strip())
        activity = Activity(
            code=code.strip(),
            name=name.strip(),
            description=description.strip() or None,
            applies_labor=applies_labor,
            applies_machine=applies_machine,
            default_machine_id=int(default_machine_id) if default_machine_id else None,
            active=active,
        )
        db.add(activity)
        db.commit()
        return _redirect("/activities")
    except ValidationError as exc:
        return templates.TemplateResponse(
            request=request,
            name="activity_form.html",
            context={"title": "New Activity", "activity": None, "machines": machines, "error": str(exc)},
        )


@app.get("/activities/{activity_id}/edit", response_class=HTMLResponse)
def edit_activity(activity_id: int, request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    activity = db.query(Activity).filter(Activity.id == activity_id).one()
    machines = db.query(Machine).order_by(Machine.code).all()
    return templates.TemplateResponse(
        request=request,
        name="activity_form.html",
        context={"title": "Edit Activity", "activity": activity, "machines": machines, "error": None},
    )


@app.post("/activities/{activity_id}/edit")
def update_activity(
    activity_id: int,
    request: Request,
    code: str = Form(...),
    name: str = Form(...),
    description: str = Form(""),
    applies_labor: bool = Form(False),
    applies_machine: bool = Form(False),
    default_machine_id: str = Form(""),
    active: bool = Form(False),
    db: Session = Depends(get_db),
) -> Response:
    activity = db.query(Activity).filter(Activity.id == activity_id).one()
    machines = db.query(Machine).order_by(Machine.code).all()
    try:
        validate_unique_code(db, Activity, code.strip(), current_id=activity_id)
        activity.code = code.strip()
        activity.name = name.strip()
        activity.description = description.strip() or None
        activity.applies_labor = applies_labor
        activity.applies_machine = applies_machine
        activity.default_machine_id = int(default_machine_id) if default_machine_id else None
        activity.active = active
        db.commit()
        return _redirect("/activities")
    except ValidationError as exc:
        return templates.TemplateResponse(
            request=request,
            name="activity_form.html",
            context={"title": "Edit Activity", "activity": activity, "machines": machines, "error": str(exc)},
        )


@app.get("/machines", response_class=HTMLResponse)
def list_machines(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    machines = db.query(Machine).order_by(Machine.code).all()
    return templates.TemplateResponse(
        request=request,
        name="machines_list.html",
        context={"title": "Machines", "machines": machines},
    )


@app.get("/machines/new", response_class=HTMLResponse)
def new_machine(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="machine_form.html",
        context={"title": "New Machine", "machine": None, "error": None},
    )


@app.post("/machines")
def create_machine(
    request: Request,
    code: str = Form(...),
    name: str = Form(...),
    active: bool = Form(False),
    db: Session = Depends(get_db),
) -> Response:
    try:
        validate_unique_code(db, Machine, code.strip())
        db.add(Machine(code=code.strip(), name=name.strip(), active=active))
        db.commit()
        return _redirect("/machines")
    except ValidationError as exc:
        return templates.TemplateResponse(
            request=request,
            name="machine_form.html",
            context={"title": "New Machine", "machine": None, "error": str(exc)},
        )


@app.get("/machines/{machine_id}/edit", response_class=HTMLResponse)
def edit_machine(machine_id: int, request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    machine = db.query(Machine).filter(Machine.id == machine_id).one()
    return templates.TemplateResponse(
        request=request,
        name="machine_form.html",
        context={"title": "Edit Machine", "machine": machine, "error": None},
    )


@app.post("/machines/{machine_id}/edit")
def update_machine(
    machine_id: int,
    request: Request,
    code: str = Form(...),
    name: str = Form(...),
    active: bool = Form(False),
    db: Session = Depends(get_db),
) -> Response:
    machine = db.query(Machine).filter(Machine.id == machine_id).one()
    try:
        validate_unique_code(db, Machine, code.strip(), current_id=machine_id)
        machine.code = code.strip()
        machine.name = name.strip()
        machine.active = active
        db.commit()
        return _redirect("/machines")
    except ValidationError as exc:
        return templates.TemplateResponse(
            request=request,
            name="machine_form.html",
            context={"title": "Edit Machine", "machine": machine, "error": str(exc)},
        )


@app.get("/routes", response_class=HTMLResponse)
def list_routes(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    routes = db.query(Route).order_by(Route.code).all()
    return templates.TemplateResponse(
        request=request,
        name="routes_list.html",
        context={"title": "Routes", "routes": routes},
    )


@app.get("/routes/new", response_class=HTMLResponse)
def new_route(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="route_form.html",
        context={"title": "New Route", "route": None, "process_types": _process_types(), "error": None},
    )


@app.post("/routes")
def create_route(
    request: Request,
    code: str = Form(...),
    name: str = Form(...),
    process_type: str = Form(...),
    version: str = Form("1"),
    active: bool = Form(False),
    db: Session = Depends(get_db),
) -> Response:
    try:
        validate_unique_code(db, Route, code.strip())
        validate_process_type(process_type)
        route = Route(
            code=code.strip(),
            name=name.strip(),
            process_type=process_type,
            version=version.strip() or "1",
            active=active,
        )
        db.add(route)
        db.commit()
        return _redirect("/routes")
    except ValidationError as exc:
        return templates.TemplateResponse(
            request=request,
            name="route_form.html",
            context={"title": "New Route", "route": None, "process_types": _process_types(), "error": str(exc)},
        )


@app.get("/routes/{route_id}/edit", response_class=HTMLResponse)
def edit_route(route_id: int, request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    route = db.query(Route).filter(Route.id == route_id).one()
    return templates.TemplateResponse(
        request=request,
        name="route_form.html",
        context={"title": "Edit Route", "route": route, "process_types": _process_types(), "error": None},
    )


@app.post("/routes/{route_id}/edit")
def update_route(
    route_id: int,
    request: Request,
    code: str = Form(...),
    name: str = Form(...),
    process_type: str = Form(...),
    version: str = Form("1"),
    active: bool = Form(False),
    db: Session = Depends(get_db),
) -> Response:
    route = db.query(Route).filter(Route.id == route_id).one()
    try:
        validate_unique_code(db, Route, code.strip(), current_id=route_id)
        validate_process_type(process_type)
        route.code = code.strip()
        route.name = name.strip()
        route.process_type = process_type
        route.version = version.strip() or "1"
        route.active = active
        db.commit()
        return _redirect("/routes")
    except ValidationError as exc:
        return templates.TemplateResponse(
            request=request,
            name="route_form.html",
            context={"title": "Edit Route", "route": route, "process_types": _process_types(), "error": str(exc)},
        )


@app.get("/routes/{route_id}", response_class=HTMLResponse)
def route_detail(route_id: int, request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    route = db.query(Route).filter(Route.id == route_id).one()
    route_activities = (
        db.query(RouteActivity)
        .options(joinedload(RouteActivity.activity))
        .filter(RouteActivity.route_id == route_id)
        .order_by(RouteActivity.sequence)
        .all()
    )
    activities = db.query(Activity).filter(Activity.active.is_(True)).order_by(Activity.code).all()
    return templates.TemplateResponse(
        request=request,
        name="route_detail.html",
        context={
            "title": "Route Detail",
            "route": route,
            "route_activities": route_activities,
            "activities": activities,
            "error": None,
        },
    )


@app.post("/routes/{route_id}/activities")
def add_route_activity(
    route_id: int,
    request: Request,
    sequence: int = Form(...),
    activity_id: int = Form(...),
    required: bool = Form(False),
    visible_default: bool = Form(False),
    db: Session = Depends(get_db),
) -> Response:
    route = db.query(Route).filter(Route.id == route_id).one()
    route_activities = (
        db.query(RouteActivity)
        .options(joinedload(RouteActivity.activity))
        .filter(RouteActivity.route_id == route_id)
        .order_by(RouteActivity.sequence)
        .all()
    )
    activities = db.query(Activity).filter(Activity.active.is_(True)).order_by(Activity.code).all()
    try:
        validate_route_activity_sequence(db, route_id, sequence)
        db.add(
            RouteActivity(
                route_id=route_id,
                sequence=sequence,
                activity_id=activity_id,
                required=required,
                visible_default=visible_default,
            )
        )
        db.commit()
        return _redirect(f"/routes/{route_id}")
    except ValidationError as exc:
        return templates.TemplateResponse(
            request=request,
            name="route_detail.html",
            context={
                "title": "Route Detail",
                "route": route,
                "route_activities": route_activities,
                "activities": activities,
                "error": str(exc),
            },
        )


@app.post("/routes/{route_id}/activities/{route_activity_id}/delete")
def delete_route_activity(
    route_id: int,
    route_activity_id: int,
    db: Session = Depends(get_db),
) -> RedirectResponse:
    route_activity = (
        db.query(RouteActivity)
        .filter(RouteActivity.route_id == route_id, RouteActivity.id == route_activity_id)
        .one()
    )
    db.delete(route_activity)
    db.commit()
    return _redirect(f"/routes/{route_id}")


def _product_routes_redirect_url(
    product_sku: str = "",
    route_id: str = "",
    route_status: str = "",
    message: str = "",
    error: str = "",
) -> str:
    params: list[str] = []
    if product_sku.strip():
        params.append(f"product_sku={quote(product_sku.strip())}")
    if route_status.strip() == "no_route":
        params.append("route_status=no_route")
    elif route_id.strip():
        params.append(f"route_id={quote(route_id.strip())}")
    if message:
        params.append(f"message={quote(message)}")
    if error:
        params.append(f"error={quote(error)}")
    return "/product-routes" + (f"?{'&'.join(params)}" if params else "")


@app.get("/product-routes", response_class=HTMLResponse)
def product_routes(
    request: Request,
    product_sku: str = Query(""),
    route_id: str = Query(""),
    route_status: str = Query(""),
    message: str | None = Query(default=None),
    error: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    search_sku = product_sku.strip()
    selected_route_id = route_id.strip()
    selected_route_status = route_status.strip()
    product_query = db.query(Product).filter(Product.is_manufactured.is_(True))
    if search_sku:
        product_query = product_query.filter(Product.sku.ilike(f"%{search_sku}%"))
    if selected_route_status == "no_route":
        product_query = product_query.filter(Product.default_route_id.is_(None))
        selected_route_id = ""
    elif selected_route_id.isdigit():
        product_query = product_query.filter(Product.default_route_id == int(selected_route_id))
    products = product_query.order_by(Product.sku).all()
    routes = db.query(Route).filter(Route.active.is_(True)).order_by(Route.code).all()
    return templates.TemplateResponse(
        request=request,
        name="product_routes.html",
        context={
            "title": "Product Routes",
            "products": products,
            "routes": routes,
            "filters": {
                "product_sku": search_sku,
                "route_id": selected_route_id,
                "route_status": selected_route_status,
            },
            "product_sku": search_sku,
            "message": message,
            "error": error,
        },
    )


@app.post("/product-routes/bulk-assign")
def bulk_assign_product_routes(
    product_ids: list[int] = Form(default=[]),
    bulk_route_id: str = Form(""),
    product_sku: str = Form(""),
    route_id: str = Form(""),
    route_status: str = Form(""),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    if not product_ids:
        return _redirect(
            _product_routes_redirect_url(product_sku, route_id, route_status, error="Select at least one product.")
        )
    if not bulk_route_id.strip():
        return _redirect(
            _product_routes_redirect_url(product_sku, route_id, route_status, error="Choose a route to assign.")
        )
    if not bulk_route_id.strip().isdigit():
        return _redirect(
            _product_routes_redirect_url(product_sku, route_id, route_status, error="Selected route is not available.")
        )

    route = db.query(Route).filter(Route.id == int(bulk_route_id), Route.active.is_(True)).one_or_none()
    if route is None:
        return _redirect(
            _product_routes_redirect_url(product_sku, route_id, route_status, error="Selected route is not available.")
        )

    products = (
        db.query(Product)
        .filter(Product.id.in_(product_ids), Product.is_manufactured.is_(True))
        .all()
    )
    for product in products:
        product.default_route_id = route.id
    db.commit()
    return _redirect(
        _product_routes_redirect_url(
            product_sku,
            route_id,
            route_status,
            message=f"Route assigned to {len(products)} selected products.",
        )
    )


@app.post("/product-routes/{product_id}")
def update_product_route(
    product_id: int,
    default_route_id: str = Form(""),
    product_sku: str = Form(""),
    route_id: str = Form(""),
    route_status: str = Form(""),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    product = db.query(Product).filter(Product.id == product_id).one()
    product.default_route_id = int(default_route_id) if default_route_id else None
    db.commit()
    return _redirect(_product_routes_redirect_url(product_sku, route_id, route_status))


@app.get("/production-orders", response_class=HTMLResponse)
def list_production_orders(
    request: Request,
    order_number: str = Query(""),
    product_sku: str = Query(""),
    product_name: str = Query(""),
    process_type: str = Query(""),
    status: str = Query(""),
    date_from: str = Query(""),
    date_to: str = Query(""),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    date_from_value = _parse_optional_date(date_from.strip())
    date_to_value = _parse_optional_date(date_to.strip())
    filters = {
        "order_number": order_number.strip(),
        "product_sku": product_sku.strip(),
        "product_name": product_name.strip(),
        "process_type": process_type.strip(),
        "status": status.strip(),
        "date_from": date_from_value,
        "date_to": date_to_value,
    }
    order_query = db.query(ProductionOrder)
    if filters["order_number"]:
        order_query = order_query.filter(
            ProductionOrder.internal_order_number.ilike(f"%{filters['order_number']}%")
        )
    if filters["product_sku"]:
        order_query = order_query.filter(
            ProductionOrder.product_sku_snapshot.ilike(f"%{filters['product_sku']}%")
        )
    if filters["product_name"]:
        order_query = order_query.filter(
            ProductionOrder.product_name_snapshot.ilike(f"%{filters['product_name']}%")
        )
    if filters["process_type"] in _process_types():
        order_query = order_query.filter(ProductionOrder.process_type == filters["process_type"])
    if filters["status"] in _production_order_statuses():
        order_query = order_query.filter(ProductionOrder.status == filters["status"])
    if filters["date_from"]:
        order_query = order_query.filter(ProductionOrder.production_date >= filters["date_from"])
    if filters["date_to"]:
        order_query = order_query.filter(ProductionOrder.production_date <= filters["date_to"])

    orders = order_query.order_by(ProductionOrder.production_date.desc(), ProductionOrder.id.desc()).all()
    return templates.TemplateResponse(
        request=request,
        name="production_orders_list.html",
        context={
            "title": "Production Orders",
            "orders": orders,
            "filters": filters,
            "process_types": _process_types(),
            "statuses": _production_order_statuses(),
        },
    )


@app.get("/production-orders/new", response_class=HTMLResponse)
def new_production_order(
    request: Request,
    q: str = Query(""),
    product_id: int | None = Query(default=None),
    planned_qty: str = Query(""),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    selected_product_id = product_id
    selected_product = None
    if selected_product_id is not None:
        selected_product = (
            db.query(Product)
            .filter(Product.id == selected_product_id, Product.is_manufactured.is_(True), Product.active.is_(True))
            .one_or_none()
        )
    product_options = _manufactured_product_options(db)
    return templates.TemplateResponse(
        request=request,
        name="production_order_form.html",
        context={
            "title": "New Production Order",
            "product_options": product_options,
            "selected_product": selected_product,
            "error": None,
            "selected_product_id": selected_product_id,
            "prefill_planned_qty": planned_qty.strip(),
        },
    )


@app.post("/production-orders")
def create_production_order_route(
    request: Request,
    production_date: date = Form(...),
    product_id: int = Form(...),
    planned_qty: str = Form(""),
    notes: str = Form(""),
    db: Session = Depends(get_db),
) -> Response:
    product_options = _manufactured_product_options(db)
    selected_product = (
        db.query(Product)
        .filter(Product.id == product_id, Product.is_manufactured.is_(True), Product.active.is_(True))
        .one_or_none()
    )
    try:
        planned_qty_value = parse_optional_decimal(planned_qty, "Planned quantity")
        order = create_production_order(
            db=db,
            production_date=production_date,
            product_id=product_id,
            planned_qty=planned_qty_value,
            notes=notes,
        )
        return _redirect(f"/production-orders/{order.id}")
    except ProductionOrderValidationError as exc:
        return templates.TemplateResponse(
            request=request,
            name="production_order_form.html",
            context={
                "title": "New Production Order",
                "product_options": product_options,
                "selected_product": selected_product,
                "error": str(exc),
                "selected_product_id": product_id,
                "prefill_planned_qty": planned_qty,
            },
        )


@app.get("/production-orders/{order_id}", response_class=HTMLResponse)
def production_order_detail(order_id: int, request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    return _production_order_detail_response(order_id, request, db)


@app.get("/production-orders/{order_id}/loyverse-inventory-preview", response_class=HTMLResponse)
def production_order_inventory_preview(
    order_id: int,
    request: Request,
    db: Session = Depends(get_db),
) -> HTMLResponse:
    order = db.query(ProductionOrder).filter(ProductionOrder.id == order_id).one()
    readiness = build_production_inventory_readiness(db, order_id)
    preview = None
    error = None
    if readiness["ready"]:
        try:
            preview = build_production_inventory_preview(db, order_id)
        except ProductionInventoryPreviewError as exc:
            error = str(exc)
    else:
        error = "Production Order is not ready for Loyverse inventory preview."

    return templates.TemplateResponse(
        request=request,
        name="production_loyverse_inventory_preview.html",
        context={
            "title": "Loyverse Inventory Preview",
            "order": order,
            "readiness": readiness,
            "preview": preview,
            "error": error,
        },
    )

@app.post("/production-orders/{order_id}/loyverse-inventory-sync")
def sync_production_order_inventory(
    order_id: int,
    request: Request,
    preview_token: str = Form(""),
    preview_fingerprint: str = Form(""),
    db: Session = Depends(get_db),
) -> Response:
    try:
        sync_production_inventory_to_loyverse(db, order_id, preview_token, preview_fingerprint)
        return _redirect(f"/production-orders/{order_id}")
    except ProductionInventorySyncError as exc:
        return _production_order_detail_response(order_id, request, db, str(exc))

@app.get("/production-orders/{order_id}/print", response_class=HTMLResponse)
def production_order_print(order_id: int, request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    order = db.query(ProductionOrder).filter(ProductionOrder.id == order_id).one()
    if order.status not in {"draft", "in_progress"}:
        return _production_order_detail_response(order_id, request, db, "Only draft or in-progress orders can be printed.")

    materials = (
        db.query(ProductionOrderMaterial)
        .filter(ProductionOrderMaterial.production_order_id == order_id)
        .order_by(ProductionOrderMaterial.id)
        .all()
    )
    activities = (
        db.query(ProductionOrderActivity)
        .filter(ProductionOrderActivity.production_order_id == order_id)
        .order_by(ProductionOrderActivity.sequence)
        .all()
    )
    issue_date = date.today()
    return templates.TemplateResponse(
        request=request,
        name="production_order_print.html",
        context={
            "title": "Production Order Print",
            "order": order,
            "materials": materials,
            "activities": activities,
            "issue_date": issue_date,
            "week_number": order.production_date.isocalendar().week,
        },
    )


@app.post("/production-orders/{order_id}/activities")
async def update_production_order_activities(
    order_id: int,
    request: Request,
    db: Session = Depends(get_db),
) -> Response:
    form = await request.form()
    activity_ids = form.getlist("activity_id")
    updates = [
        {
            "id": activity_id,
            "labor_minutes": str(form.get(f"labor_minutes_{activity_id}", "")),
            "machine_minutes": str(form.get(f"machine_minutes_{activity_id}", "")),
            "notes": str(form.get(f"notes_{activity_id}", "")),
        }
        for activity_id in activity_ids
    ]
    try:
        update_activity_capture(db, order_id, updates)
        return _redirect(f"/production-orders/{order_id}")
    except ProductionOrderValidationError as exc:
        return _production_order_detail_response(order_id, request, db, str(exc))


@app.post("/production-orders/{order_id}/yield")
def update_production_order_yield(
    order_id: int,
    request: Request,
    input_qty: str = Form(""),
    output_qty: str = Form(""),
    db: Session = Depends(get_db),
) -> Response:
    try:
        input_qty_value = parse_optional_decimal(input_qty, "Input quantity")
        output_qty_value = parse_optional_decimal(output_qty, "Output quantity")
        update_yield_capture(db, order_id, input_qty_value, output_qty_value)
        return _redirect(f"/production-orders/{order_id}")
    except ProductionOrderValidationError as exc:
        return _production_order_detail_response(order_id, request, db, str(exc))


@app.get("/production-orders/{order_id}/bom/edit", response_class=HTMLResponse)
def edit_production_order_bom(
    order_id: int,
    request: Request,
    db: Session = Depends(get_db),
) -> HTMLResponse:
    order = db.query(ProductionOrder).filter(ProductionOrder.id == order_id).one()
    if order.status == "closed":
        return _production_order_detail_response(order_id, request, db, "Closed orders are read-only.")

    materials = (
        db.query(ProductionOrderMaterial)
        .filter(ProductionOrderMaterial.production_order_id == order_id)
        .order_by(ProductionOrderMaterial.id)
        .all()
    )
    products = db.query(Product).order_by(Product.sku).all()
    products_by_sku = {product.sku: product for product in products}
    return templates.TemplateResponse(
        request=request,
        name="production_order_bom_edit.html",
        context={
            "title": "Edit BOM",
            "order": order,
            "materials": materials,
            "products": products,
            "products_by_sku": products_by_sku,
            "error": None,
        },
    )


@app.post("/production-orders/{order_id}/bom/edit")
async def update_production_order_bom(
    order_id: int,
    request: Request,
    db: Session = Depends(get_db),
) -> Response:
    form = await request.form()
    material_ids = form.getlist("material_id")
    updates = [
        {
            "id": material_id,
            "component_sku": str(form.get(f"component_sku_{material_id}", "")),
            "quantity_standard": str(form.get(f"quantity_standard_{material_id}", "")),
        }
        for material_id in material_ids
    ]
    deleted_material_ids = [int(material_id) for material_id in form.getlist("delete_material_id")]
    new_material = {
        "component_sku": str(form.get("new_component_sku", "")),
        "quantity_standard": str(form.get("new_quantity_standard", "")),
    }
    try:
        update_order_bom(db, order_id, updates, deleted_material_ids, new_material)
        return _redirect(f"/production-orders/{order_id}")
    except ProductionOrderValidationError as exc:
        order = db.query(ProductionOrder).filter(ProductionOrder.id == order_id).one()
        materials = (
            db.query(ProductionOrderMaterial)
            .filter(ProductionOrderMaterial.production_order_id == order_id)
            .order_by(ProductionOrderMaterial.id)
            .all()
        )
        products = db.query(Product).order_by(Product.sku).all()
        products_by_sku = {product.sku: product for product in products}
        return templates.TemplateResponse(
            request=request,
            name="production_order_bom_edit.html",
            context={
                "title": "Edit BOM",
                "order": order,
                "materials": materials,
                "products": products,
                "products_by_sku": products_by_sku,
                "error": str(exc),
            },
        )


@app.post("/production-orders/{order_id}/start")
def start_production_order(order_id: int, request: Request, db: Session = Depends(get_db)) -> Response:
    try:
        start_order(db, order_id)
        return _redirect(f"/production-orders/{order_id}")
    except ProductionOrderValidationError as exc:
        return _production_order_detail_response(order_id, request, db, str(exc))


@app.post("/production-orders/{order_id}/close")
def close_production_order(order_id: int, request: Request, db: Session = Depends(get_db)) -> Response:
    try:
        close_order_with_inventory_posting(db, order_id)
        # Loyverse cost sync is intentionally disabled here.
        # The ERP is now the source of truth for production cost,
        # so Production Close should not push cost updates to Loyverse.
        return _redirect(f"/production-orders/{order_id}")
    except ProductionOrderValidationError as exc:
        return _production_order_detail_response(order_id, request, db, str(exc))


@app.get("/rates", response_class=HTMLResponse)
def list_rates(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    labor_rates = db.query(LaborRate).order_by(LaborRate.effective_from.desc()).all()
    overhead_rates = db.query(OverheadRate).order_by(OverheadRate.effective_from.desc()).all()
    machine_rates = (
        db.query(MachineRate)
        .options(joinedload(MachineRate.machine))
        .order_by(MachineRate.effective_from.desc())
        .all()
    )
    return templates.TemplateResponse(
        request=request,
        name="rates_list.html",
        context={
            "title": "Rates",
            "labor_rates": labor_rates,
            "overhead_rates": overhead_rates,
            "machine_rates": machine_rates,
        },
    )


@app.get("/rates/labor/new", response_class=HTMLResponse)
def new_labor_rate(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="labor_rate_form.html",
        context={"title": "New Labor Rate", "rate": None, "error": None},
    )


@app.post("/rates/labor")
def create_labor_rate(
    request: Request,
    effective_from: date = Form(...),
    effective_to: str = Form(""),
    hourly_rate: str = Form(...),
    notes: str = Form(""),
    db: Session = Depends(get_db),
) -> Response:
    effective_to_date = _parse_optional_date(effective_to)
    try:
        rate_value = parse_decimal(hourly_rate, "Hourly rate")
        validate_labor_rate(db, effective_from, effective_to_date)
        db.add(
            LaborRate(
                effective_from=effective_from,
                effective_to=effective_to_date,
                hourly_rate=rate_value,
                notes=notes.strip() or None,
            )
        )
        db.commit()
        return _redirect("/rates")
    except ValidationError as exc:
        return templates.TemplateResponse(
            request=request,
            name="labor_rate_form.html",
            context={"title": "New Labor Rate", "rate": None, "error": str(exc)},
        )


@app.get("/rates/labor/{rate_id}/edit", response_class=HTMLResponse)
def edit_labor_rate(rate_id: int, request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    rate = db.query(LaborRate).filter(LaborRate.id == rate_id).one()
    return templates.TemplateResponse(
        request=request,
        name="labor_rate_form.html",
        context={"title": "Edit Labor Rate", "rate": rate, "error": None},
    )


@app.post("/rates/labor/{rate_id}/edit")
def update_labor_rate(
    rate_id: int,
    request: Request,
    effective_from: date = Form(...),
    effective_to: str = Form(""),
    hourly_rate: str = Form(...),
    notes: str = Form(""),
    db: Session = Depends(get_db),
) -> Response:
    rate = db.query(LaborRate).filter(LaborRate.id == rate_id).one()
    effective_to_date = _parse_optional_date(effective_to)
    try:
        rate.hourly_rate = parse_decimal(hourly_rate, "Hourly rate")
        validate_labor_rate(db, effective_from, effective_to_date, current_id=rate_id)
        rate.effective_from = effective_from
        rate.effective_to = effective_to_date
        rate.notes = notes.strip() or None
        db.commit()
        return _redirect("/rates")
    except ValidationError as exc:
        return templates.TemplateResponse(
            request=request,
            name="labor_rate_form.html",
            context={"title": "Edit Labor Rate", "rate": rate, "error": str(exc)},
        )


@app.get("/rates/overhead/new", response_class=HTMLResponse)
def new_overhead_rate(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="overhead_rate_form.html",
        context={"title": "New Overhead Rate", "rate": None, "error": None},
    )


@app.post("/rates/overhead")
def create_overhead_rate(
    request: Request,
    effective_from: date = Form(...),
    effective_to: str = Form(""),
    hourly_rate: str = Form(...),
    notes: str = Form(""),
    db: Session = Depends(get_db),
) -> Response:
    effective_to_date = _parse_optional_date(effective_to)
    try:
        rate_value = parse_decimal(hourly_rate, "Hourly rate")
        validate_overhead_rate(db, effective_from, effective_to_date)
        db.add(
            OverheadRate(
                effective_from=effective_from,
                effective_to=effective_to_date,
                hourly_rate=rate_value,
                notes=notes.strip() or None,
            )
        )
        db.commit()
        return _redirect("/rates")
    except ValidationError as exc:
        return templates.TemplateResponse(
            request=request,
            name="overhead_rate_form.html",
            context={"title": "New Overhead Rate", "rate": None, "error": str(exc)},
        )


@app.get("/rates/overhead/{rate_id}/edit", response_class=HTMLResponse)
def edit_overhead_rate(rate_id: int, request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    rate = db.query(OverheadRate).filter(OverheadRate.id == rate_id).one()
    return templates.TemplateResponse(
        request=request,
        name="overhead_rate_form.html",
        context={"title": "Edit Overhead Rate", "rate": rate, "error": None},
    )


@app.post("/rates/overhead/{rate_id}/edit")
def update_overhead_rate(
    rate_id: int,
    request: Request,
    effective_from: date = Form(...),
    effective_to: str = Form(""),
    hourly_rate: str = Form(...),
    notes: str = Form(""),
    db: Session = Depends(get_db),
) -> Response:
    rate = db.query(OverheadRate).filter(OverheadRate.id == rate_id).one()
    effective_to_date = _parse_optional_date(effective_to)
    try:
        rate.hourly_rate = parse_decimal(hourly_rate, "Hourly rate")
        validate_overhead_rate(db, effective_from, effective_to_date, current_id=rate_id)
        rate.effective_from = effective_from
        rate.effective_to = effective_to_date
        rate.notes = notes.strip() or None
        db.commit()
        return _redirect("/rates")
    except ValidationError as exc:
        return templates.TemplateResponse(
            request=request,
            name="overhead_rate_form.html",
            context={"title": "Edit Overhead Rate", "rate": rate, "error": str(exc)},
        )


@app.get("/rates/machine/new", response_class=HTMLResponse)
def new_machine_rate(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    machines = db.query(Machine).order_by(Machine.code).all()
    return templates.TemplateResponse(
        request=request,
        name="machine_rate_form.html",
        context={"title": "New Machine Rate", "rate": None, "machines": machines, "error": None},
    )


@app.post("/rates/machine")
def create_machine_rate(
    request: Request,
    machine_id: int = Form(...),
    effective_from: date = Form(...),
    effective_to: str = Form(""),
    hourly_rate: str = Form(...),
    notes: str = Form(""),
    db: Session = Depends(get_db),
) -> Response:
    machines = db.query(Machine).order_by(Machine.code).all()
    effective_to_date = _parse_optional_date(effective_to)
    try:
        rate_value = parse_decimal(hourly_rate, "Hourly rate")
        validate_machine_rate(db, machine_id, effective_from, effective_to_date)
        db.add(
            MachineRate(
                machine_id=machine_id,
                effective_from=effective_from,
                effective_to=effective_to_date,
                hourly_rate=rate_value,
                notes=notes.strip() or None,
            )
        )
        db.commit()
        return _redirect("/rates")
    except ValidationError as exc:
        return templates.TemplateResponse(
            request=request,
            name="machine_rate_form.html",
            context={"title": "New Machine Rate", "rate": None, "machines": machines, "error": str(exc)},
        )


@app.get("/rates/machine/{rate_id}/edit", response_class=HTMLResponse)
def edit_machine_rate(rate_id: int, request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    rate = db.query(MachineRate).filter(MachineRate.id == rate_id).one()
    machines = db.query(Machine).order_by(Machine.code).all()
    return templates.TemplateResponse(
        request=request,
        name="machine_rate_form.html",
        context={"title": "Edit Machine Rate", "rate": rate, "machines": machines, "error": None},
    )


@app.post("/rates/machine/{rate_id}/edit")
def update_machine_rate(
    rate_id: int,
    request: Request,
    machine_id: int = Form(...),
    effective_from: date = Form(...),
    effective_to: str = Form(""),
    hourly_rate: str = Form(...),
    notes: str = Form(""),
    db: Session = Depends(get_db),
) -> Response:
    rate = db.query(MachineRate).filter(MachineRate.id == rate_id).one()
    machines = db.query(Machine).order_by(Machine.code).all()
    effective_to_date = _parse_optional_date(effective_to)
    try:
        rate.hourly_rate = parse_decimal(hourly_rate, "Hourly rate")
        validate_machine_rate(db, machine_id, effective_from, effective_to_date, current_id=rate_id)
        rate.machine_id = machine_id
        rate.effective_from = effective_from
        rate.effective_to = effective_to_date
        rate.notes = notes.strip() or None
        db.commit()
        return _redirect("/rates")
    except ValidationError as exc:
        return templates.TemplateResponse(
            request=request,
            name="machine_rate_form.html",
            context={"title": "Edit Machine Rate", "rate": rate, "machines": machines, "error": str(exc)},
        )
