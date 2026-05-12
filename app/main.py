import csv
import io
from collections import Counter
from datetime import date, datetime, timedelta
from decimal import Decimal
from urllib.parse import quote, urlencode

from fastapi import Depends, FastAPI, File, Form, Query, Request, UploadFile
from fastapi import HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, Response, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import or_
from sqlalchemy.orm import Session, joinedload

from app.database import (
    Base,
    engine,
    SessionLocal,
    ensure_b2b_invoice_snapshot_columns,
    ensure_b2b_sales_followup_columns,
    ensure_auth_tables,
    ensure_channel_master_tables,
    ensure_b2c_sales_tables,
    ensure_b2c_customer_tables,
    ensure_b2b_loyverse_mapping_tables,
    ensure_app_sequences_table,
    ensure_discount_master_tables,
    ensure_inventory_adjustment_tables,
    ensure_inventory_ledger_tables,
    ensure_master_data_tables,
    ensure_product_bom_tables,
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
    B2CCustomer,
    B2BSalesOrder,
    B2BSalesOrderLine,
    B2CSalesOrder,
    B2CSalesOrderLine,
    Channel,
    DiscountRule,
    ImportBatch,
    ImportedBomHeader,
    ImportedBomLine,
    InventoryAdjustment,
    InventoryBalance,
    InventoryTransaction,
    LaborRate,
    LoyverseCustomerMapping,
    LoyversePaymentTypeMapping,
    LoyverseVariantMapping,
    Machine,
    MachineRate,
    OverheadRate,
    Permission,
    Product,
    ProductCategory,
    ProductionOrder,
    ProductionOrderActivity,
    ProductionOrderMaterial,
    PurchaseOrder,
    PurchaseOrderLine,
    Role,
    RolePermission,
    Route,
    RouteActivity,
    Supplier,
    User,
    UserRole,
    UserSession,
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
from app.services.b2c_sales_service import (
    B2CValidationError,
    change_b2c_sales_order_status,
    create_b2c_sales_order,
    invoice_b2c_order_in_erp,
    update_b2c_sales_order,
)
from app.services.b2c_customer_service import (
    B2CCustomerValidationError,
    create_b2c_customer,
    initialize_b2c_customers_from_mappings,
    list_b2c_customer_options,
    update_b2c_customer,
)
from app.services.b2b_loyverse_mapping_service import (
    LoyverseMappingSyncError,
    refresh_loyverse_customer_mappings,
    refresh_loyverse_payment_type_mappings,
    refresh_loyverse_variant_mappings,
)
from app.services.b2b_customer_import_service import (
    B2BCustomerImportResult,
    B2BCustomerImportValidationError,
    import_b2b_customers_csv,
)
from app.services.b2b_sales_historical_import_service import (
    B2BHistoricalSalesImportResult,
    B2BHistoricalSalesImportValidationError,
    EXPECTED_HEADERS as B2B_HISTORICAL_IMPORT_HEADERS,
    import_b2b_historical_sales_csv,
)
from app.services.b2c_sales_historical_import_service import (
    B2CHistoricalSalesImportResult,
    B2CHistoricalSalesImportValidationError,
    EXPECTED_HEADERS as B2C_HISTORICAL_IMPORT_HEADERS,
    import_b2c_historical_sales_csv,
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
from app.services.inventory_adjustment_service import (
    InventoryAdjustmentValidationError,
    create_inventory_adjustment_post_token,
    create_inventory_adjustment_with_posting,
)
from app.services.master_data_service import (
    MasterDataValidationError,
    create_channel,
    create_discount_rule,
    create_product_category,
    create_product_master,
    create_supplier,
    get_product_balance,
    get_product_for_detail,
    list_category_options,
    list_channel_options,
    list_discount_rule_options,
    list_supplier_options,
    update_channel,
    update_discount_rule,
    update_product_category,
    update_product_master,
    update_supplier,
)
from app.services.planning_loyverse_refresh_service import (
    PlanningLoyverseRefreshError,
    refresh_planning_inventory_and_cost,
)
from app.services.product_bom_service import (
    ProductBomValidationError,
    get_or_seed_product_bom,
    list_bom_component_options,
    save_product_bom,
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
    update_product_inventory_parameters,
    update_product_planner_quantities,
)
from app.services.production_order_historical_import_service import (
    EXPECTED_HEADERS as PRODUCTION_ORDER_HISTORICAL_IMPORT_HEADERS,
    ProductionOrderHistoricalImportResult,
    ProductionOrderHistoricalImportValidationError,
    import_historical_production_orders_csv,
)
from app.services.purchase_order_historical_import_service import (
    EXPECTED_HEADERS as PURCHASE_ORDER_HISTORICAL_IMPORT_HEADERS,
    PurchaseOrderHistoricalImportResult,
    PurchaseOrderHistoricalImportValidationError,
    import_historical_purchase_orders_csv,
)
from app.services.supplier_import_service import (
    SupplierImportResult,
    SupplierImportValidationError,
    import_suppliers_csv,
)
from app.services.auth_service import (
    SESSION_COOKIE_NAME,
    SESSION_DURATION,
    any_active_users,
    bootstrap_admin_user,
    can,
    create_session,
    get_current_user_from_request,
    get_login_redirect_target,
    hash_password,
    is_local_request,
    is_public_path,
    require_authenticated_user,
    require_permission,
    revoke_session,
    verify_password,
)
from app.services.total_sales_service import (
    get_sales_by_order,
    get_sales_categories_pareto,
    get_sales_items_pareto,
    get_sales_summary,
    get_total_sales_monthly_summary,
    get_total_sales_rows,
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
ensure_master_data_tables()
ensure_product_bom_tables()
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
ensure_b2c_sales_tables()
ensure_discount_master_tables()
ensure_b2b_loyverse_mapping_tables()
ensure_b2c_customer_tables()
ensure_channel_master_tables()
ensure_inventory_adjustment_tables()
ensure_auth_tables()

app = FastAPI(title="Real Production Costing MVP")
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")
ZERO = Decimal("0")
templates.env.globals["can"] = can


def _redirect(url: str) -> RedirectResponse:
    return RedirectResponse(url=url, status_code=303)


def _auth_redirect(target: str, request: Request) -> RedirectResponse:
    current_path = request.url.path
    if request.url.query:
        current_path += f"?{request.url.query}"
    return _redirect(f"{target}?next={quote(current_path)}")


def _set_auth_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=token,
        httponly=True,
        samesite="lax",
        secure=False,  # Set to True behind HTTPS in production.
        max_age=int(SESSION_DURATION.total_seconds()),
    )


def _clear_auth_cookie(response: Response) -> None:
    response.delete_cookie(key=SESSION_COOKIE_NAME, httponly=True, samesite="lax")


def _csv_attachment_response(*, filename: str, headers: tuple[str, ...], example_row: tuple[str, ...]) -> Response:
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(headers)
    writer.writerow(example_row)
    return Response(
        content=buffer.getvalue().encode("utf-8-sig"),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _csv_report_response(*, filename: str, headers: tuple[str, ...], rows: list[tuple[object, ...]]) -> Response:
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(headers)
    writer.writerows(rows)
    return Response(
        content=buffer.getvalue().encode("utf-8-sig"),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _parse_optional_date(value: str) -> date | None:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def _normalize_sales_reporting_filters(
    *,
    date_from: str,
    date_to: str,
    sales_type: str,
    apply_default_date_window: bool,
) -> tuple[dict[str, str], date | None, date | None, bool]:
    filters = {
        "date_from": date_from.strip(),
        "date_to": date_to.strip(),
        "sales_type": (sales_type.strip().lower() or "all"),
    }
    using_default_date_window = False

    if apply_default_date_window and filters["date_from"] == "" and filters["date_to"] == "":
        default_date_to = date.today()
        default_date_from = default_date_to - timedelta(days=30)
        parsed_date_from = default_date_from
        parsed_date_to = default_date_to
        filters["date_from"] = default_date_from.isoformat()
        filters["date_to"] = default_date_to.isoformat()
        using_default_date_window = True
    else:
        parsed_date_from = _parse_optional_date(filters["date_from"])
        parsed_date_to = _parse_optional_date(filters["date_to"])

    if parsed_date_from is not None and parsed_date_to is not None and parsed_date_from > parsed_date_to:
        raise ValueError("Date from cannot be later than date to.")
    if filters["sales_type"] not in {"all", "b2b", "b2c"}:
        filters["sales_type"] = "all"
    return filters, parsed_date_from, parsed_date_to, using_default_date_window


def _build_sales_export_url(path: str, filters: dict[str, str]) -> str:
    query_params: dict[str, str] = {"sales_type": filters.get("sales_type", "all") or "all"}
    if filters.get("date_from"):
        query_params["date_from"] = filters["date_from"]
    if filters.get("date_to"):
        query_params["date_to"] = filters["date_to"]
    return f"{path}?{urlencode(query_params)}"


def _csv_blank_if_none(value: object) -> object:
    return "" if value is None else value


def _sales_row_detail_url(sales_source: str, order_id: int) -> str:
    return f"/b2b/orders/{order_id}" if sales_source == "B2B" else f"/b2c/orders/{order_id}"


def _serialize_decimal_for_chart(value: Decimal | None) -> float | None:
    if value is None:
        return None
    return float(value)


@app.middleware("http")
async def authentication_middleware(request: Request, call_next):
    path = request.url.path
    request.state.current_user = None
    request.state.current_permissions = set()
    if is_public_path(path):
        return await call_next(request)

    db = SessionLocal()
    try:
        current_auth = get_current_user_from_request(db, request)
        if current_auth is None:
            if not any_active_users(db):
                return _auth_redirect("/auth/bootstrap-admin", request)
            return _auth_redirect("/login", request)
        request.state.current_user = current_auth.user
        request.state.current_permissions = current_auth.permissions
        response = await call_next(request)
        db.commit()
        return response
    finally:
        db.close()


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    if exc.status_code == 403:
        return templates.TemplateResponse(
            request=request,
            name="forbidden.html",
            context={"title": "Forbidden", "detail": exc.detail},
            status_code=403,
        )
    if exc.status_code == 401:
        return _auth_redirect("/login", request)
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request, db: Session = Depends(get_db)) -> Response:
    current_auth = get_current_user_from_request(db, request)
    if current_auth is not None:
        return _redirect("/")
    if not any_active_users(db):
        return _redirect("/auth/bootstrap-admin")
    return templates.TemplateResponse(
        request=request,
        name="login.html",
        context={
            "title": "Login",
            "error": None,
            "username": "",
            "next": get_login_redirect_target(request),
        },
    )


@app.post("/login")
async def login_submit(request: Request, db: Session = Depends(get_db)) -> Response:
    if not any_active_users(db):
        return _redirect("/auth/bootstrap-admin")

    form = await request.form()
    username = str(form.get("username", "")).strip()
    password = str(form.get("password", ""))
    next_target = str(form.get("next", "/")).strip()
    if not next_target.startswith("/") or next_target.startswith("//"):
        next_target = "/"

    user = (
        db.query(User)
        .options(
            joinedload(User.user_role_links)
            .joinedload(UserRole.role)
            .joinedload(Role.permission_links)
            .joinedload(RolePermission.permission)
        )
        .filter(User.username == username)
        .one_or_none()
    )
    if user is None or not user.is_active or not verify_password(password, user.password_hash):
        return templates.TemplateResponse(
            request=request,
            name="login.html",
            context={
                "title": "Login",
                "error": "Invalid username or password.",
                "username": username,
                "next": next_target,
            },
            status_code=400,
        )

    _, raw_token = create_session(db, user, request)
    db.commit()
    response = _redirect(next_target or "/")
    _set_auth_cookie(response, raw_token)
    return response


@app.post("/logout")
async def logout_submit(request: Request, db: Session = Depends(get_db)) -> Response:
    revoke_session(db, request.cookies.get(SESSION_COOKIE_NAME))
    db.commit()
    response = _redirect("/login")
    _clear_auth_cookie(response)
    return response


@app.get("/auth/change-password", response_class=HTMLResponse)
def change_password_page(request: Request) -> HTMLResponse:
    require_authenticated_user(request)
    return templates.TemplateResponse(
        request=request,
        name="change_password.html",
        context={
            "title": "Change Password",
            "error": None,
            "success": None,
        },
    )


@app.post("/auth/change-password")
async def change_password_submit(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    current_user = require_authenticated_user(request)
    db_user = db.query(User).filter(User.id == current_user.id).one()
    form = await request.form()
    current_password = str(form.get("current_password", ""))
    new_password = str(form.get("new_password", ""))
    confirm_new_password = str(form.get("confirm_new_password", ""))

    error: str | None = None
    if not current_password:
        error = "Current password is required."
    elif not new_password:
        error = "New password is required."
    elif not confirm_new_password:
        error = "Password confirmation is required."
    elif not verify_password(current_password, db_user.password_hash):
        error = "Current password is incorrect."
    elif new_password != confirm_new_password:
        error = "New password and confirmation do not match."
    elif not new_password.strip():
        error = "New password cannot be empty or only spaces."
    elif len(new_password) < 10:
        error = "New password must be at least 10 characters long."
    elif verify_password(new_password, db_user.password_hash):
        error = "New password must be different from the current password."

    if error:
        return templates.TemplateResponse(
            request=request,
            name="change_password.html",
            context={
                "title": "Change Password",
                "error": error,
                "success": None,
            },
            status_code=400,
        )

    db_user.password_hash = hash_password(new_password)
    db_user.updated_at = datetime.utcnow()
    db_user.must_change_password = False
    db.commit()
    db.refresh(db_user)
    request.state.current_user = db_user
    return templates.TemplateResponse(
        request=request,
        name="change_password.html",
        context={
            "title": "Change Password",
            "error": None,
            "success": "Password updated successfully.",
        },
    )


@app.get("/auth/bootstrap-admin", response_class=HTMLResponse)
def bootstrap_admin_page(request: Request, db: Session = Depends(get_db)) -> Response:
    if any_active_users(db) or not is_local_request(request):
        raise HTTPException(status_code=404, detail="Not found.")
    return templates.TemplateResponse(
        request=request,
        name="bootstrap_admin.html",
        context={
            "title": "Bootstrap Admin",
            "error": None,
            "form_data": {
                "username": "admin",
                "full_name": "ERP Admin",
                "email": "",
            },
        },
    )


@app.post("/auth/bootstrap-admin")
async def bootstrap_admin_submit(request: Request, db: Session = Depends(get_db)) -> Response:
    if any_active_users(db) or not is_local_request(request):
        raise HTTPException(status_code=404, detail="Not found.")

    form = await request.form()
    form_data = {
        "username": str(form.get("username", "")).strip(),
        "full_name": str(form.get("full_name", "")).strip(),
        "email": str(form.get("email", "")).strip(),
    }
    password = str(form.get("password", ""))
    password_confirm = str(form.get("password_confirm", ""))

    if not form_data["username"] or not form_data["full_name"] or not password:
        return templates.TemplateResponse(
            request=request,
            name="bootstrap_admin.html",
            context={
                "title": "Bootstrap Admin",
                "error": "Username, full name, and password are required.",
                "form_data": form_data,
            },
            status_code=400,
        )
    if password != password_confirm:
        return templates.TemplateResponse(
            request=request,
            name="bootstrap_admin.html",
            context={
                "title": "Bootstrap Admin",
                "error": "Password confirmation does not match.",
                "form_data": form_data,
            },
            status_code=400,
        )

    try:
        user = bootstrap_admin_user(
            db,
            username=form_data["username"],
            full_name=form_data["full_name"],
            email=form_data["email"] or None,
            password=password,
        )
        _, raw_token = create_session(db, user, request)
        db.commit()
        response = _redirect("/")
        _set_auth_cookie(response, raw_token)
        return response
    except ValueError as exc:
        db.rollback()
        return templates.TemplateResponse(
            request=request,
            name="bootstrap_admin.html",
            context={
                "title": "Bootstrap Admin",
                "error": str(exc),
                "form_data": form_data,
            },
            status_code=400,
        )


def _process_types() -> list[str]:
    return [item.value for item in ProcessType]


def _production_order_statuses() -> list[str]:
    return [item.value for item in ProductionOrderStatus]


def _b2b_statuses() -> list[str]:
    return ["draft", "in_process", "invoiced"]


def _b2c_statuses() -> list[str]:
    return ["draft", "invoiced", "cancelled"]


def _default_channel_selection(
    channels: list[Channel],
    *,
    current_channel_id: int | None = None,
    snapshot_name: str | None = None,
) -> str:
    if current_channel_id is not None:
        for channel in channels:
            if channel.id == current_channel_id:
                return str(channel.id)

    normalized_snapshot = (snapshot_name or "").strip().casefold()
    if not normalized_snapshot:
        return ""

    for channel in channels:
        if channel.name.strip().casefold() == normalized_snapshot:
            return str(channel.id)
    return ""


def _discount_types() -> list[str]:
    return ["percentage"]


def _discount_applies_to_options() -> list[str]:
    return ["order_total"]


def _discount_channels() -> list[str]:
    return ["b2c"]


def _inventory_adjustment_modes() -> list[str]:
    return ["quantity_adjustment", "stock_count"]


def _inventory_adjustment_types() -> list[str]:
    return ["increase", "decrease"]


def _inventory_adjustment_reasons() -> list[str]:
    return ["physical_count", "damage", "waste", "correction", "other"]


def _form_bool(form, field_name: str) -> bool:
    return str(form.get(field_name, "")).strip().lower() in {"1", "true", "yes", "on"}


def _list_b2c_sellable_products(db: Session) -> list[Product]:
    return (
        db.query(Product)
        .filter(Product.available_for_sale_gc.is_(True))
        .order_by(Product.name, Product.sku)
        .all()
    )


def _product_form_context(
    *,
    title: str,
    form_action: str,
    product: Product | None,
    categories: list[ProductCategory],
    suppliers: list[Supplier],
    error: str | None = None,
    product_data: dict[str, object] | None = None,
) -> dict[str, object]:
    data = product_data or {
        "sku": product.sku if product else "",
        "name": product.name if product else "",
        "unit": product.unit if product and product.unit else "",
        "category_id": str(product.category_id) if product and product.category_id else "",
        "supplier_id": str(product.supplier_id) if product and product.supplier_id else "",
        "description": product.description if product and product.description else "",
        "observations": product.observations if product and product.observations else "",
        "b2c_price": product.b2c_price if product else None,
        "b2b_price": product.b2b_price if product else None,
        "standard_cost": product.standard_cost if product else None,
        "active": product.active if product else True,
        "available_for_sale_gc": product.available_for_sale_gc if product else False,
        "is_manufactured": product.is_manufactured if product else False,
        "is_purchased_product": product.is_purchased_product if product else False,
    }
    return {
        "title": title,
        "form_action": form_action,
        "product": product,
        "product_data": data,
        "categories": categories,
        "suppliers": suppliers,
        "error": error,
    }


def _b2c_customer_form_context(
    *,
    title: str,
    customer: B2CCustomer | None,
    error: str | None = None,
) -> dict[str, object]:
    return {
        "title": title,
        "customer": customer,
        "error": error,
    }


def _product_bom_existing_rows(lines) -> list[dict[str, object]]:
    return [
        {
            "id": line.id,
            "component_sku_snapshot": line.component_sku_snapshot or "",
            "component_name_snapshot": line.component_name_snapshot or "",
            "unit_snapshot": line.unit_snapshot or "",
            "quantity_standard": str(line.quantity_standard) if line.quantity_standard is not None else "",
            "notes": line.notes or "",
            "component_product_id": line.component_product_id,
            "delete": False,
        }
        for line in lines
    ]


def _blank_product_bom_new_rows(count: int = 5) -> list[dict[str, object]]:
    return [
        {
            "index": index,
            "component_sku": "",
            "quantity_standard": "",
            "notes": "",
        }
        for index in range(1, count + 1)
    ]


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


def _b2c_line_inputs_from_form(form, prefix: str, count: int | None = None) -> list[dict[str, str]]:
    indexes = [str(index).strip() for index in form.getlist(f"{prefix}_index") if str(index).strip()]
    if not indexes and count is not None:
        indexes = [str(index) for index in range(1, count + 1)]
    return [
        {
            "sku": str(form.get(f"{prefix}_sku_{index}", "")),
            "quantity": str(form.get(f"{prefix}_quantity_{index}", "")),
            "unit_price": str(form.get(f"{prefix}_unit_price_{index}", "")),
        }
        for index in indexes
    ]


def _parse_optional_date_query(value: str | None) -> date | None:
    text = (value or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


def _product_lookup_payload(product: Product) -> dict[str, object]:
    return {
        "id": product.id,
        "sku": product.sku,
        "name": product.name,
        "description": product.description,
        "b2c_price": str(product.b2c_price) if product.b2c_price is not None else None,
        "standard_cost": str(product.standard_cost) if product.standard_cost is not None else None,
    }


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


def _inventory_adjustment_product_options(db: Session) -> list[dict[str, object]]:
    products = db.query(Product).order_by(Product.sku, Product.name).all()
    balances_by_product_id = {
        balance.product_id: balance
        for balance in db.query(InventoryBalance).order_by(InventoryBalance.product_id).all()
    }
    return [
        {
            "id": product.id,
            "sku": product.sku,
            "name": product.name,
            "active": product.active,
            "current_qty": balances_by_product_id.get(product.id).on_hand_qty if balances_by_product_id.get(product.id) else None,
            "average_unit_cost": balances_by_product_id.get(product.id).average_unit_cost if balances_by_product_id.get(product.id) else None,
            "lookup_label": f"{product.sku} - {product.name}",
        }
        for product in products
    ]


def _inventory_adjustment_form_context(
    *,
    title: str,
    form_action: str,
    product_options: list[dict[str, object]],
    post_token: str,
    error: str | None = None,
    form_data: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "title": title,
        "form_action": form_action,
        "product_options": product_options,
        "post_token": post_token,
        "adjustment_modes": _inventory_adjustment_modes(),
        "adjustment_types": _inventory_adjustment_types(),
        "adjustment_reasons": _inventory_adjustment_reasons(),
        "error": error,
        "form_data": form_data
        or {
            "adjustment_date": date.today().isoformat(),
            "product_id": "",
            "adjustment_mode": "quantity_adjustment",
            "adjustment_type": "increase",
            "quantity": "",
            "counted_qty": "",
            "unit_cost": "",
            "reason": "correction",
            "notes": "",
        },
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


@app.get("/inventory", response_class=HTMLResponse)
def inventory_home(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="inventory_home.html",
        context={"title": "Inventory"},
    )


@app.get("/master-data", response_class=HTMLResponse)
def master_data_home(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="master_data_home.html",
        context={"title": "Master Data"},
    )


@app.get("/master-data/categories", response_class=HTMLResponse)
def product_categories(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    categories = db.query(ProductCategory).order_by(ProductCategory.name, ProductCategory.id).all()
    return templates.TemplateResponse(
        request=request,
        name="product_categories_list.html",
        context={"title": "Product Categories", "categories": categories},
    )


@app.get("/master-data/categories/new", response_class=HTMLResponse)
def new_product_category(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="product_category_form.html",
        context={
            "title": "New Product Category",
            "form_action": "/master-data/categories",
            "category": None,
            "form_data": {"name": "", "description": "", "active": True},
            "error": None,
        },
    )


@app.post("/master-data/categories")
async def create_product_category_route(request: Request, db: Session = Depends(get_db)) -> Response:
    form = await request.form()
    form_data = {
        "name": str(form.get("name", "")),
        "description": str(form.get("description", "")),
        "active": _form_bool(form, "active"),
    }
    try:
        category = create_product_category(
            db,
            name=str(form_data["name"]),
            description=str(form_data["description"]),
            active=bool(form_data["active"]),
        )
        return _redirect(f"/master-data/categories/{category.id}/edit")
    except MasterDataValidationError as exc:
        db.rollback()
        return templates.TemplateResponse(
            request=request,
            name="product_category_form.html",
            context={
                "title": "New Product Category",
                "form_action": "/master-data/categories",
                "category": None,
                "form_data": form_data,
                "error": str(exc),
            },
        )


@app.get("/master-data/categories/{category_id}/edit", response_class=HTMLResponse)
def edit_product_category(category_id: int, request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    category = db.query(ProductCategory).filter(ProductCategory.id == category_id).one()
    return templates.TemplateResponse(
        request=request,
        name="product_category_form.html",
        context={
            "title": "Edit Product Category",
            "form_action": f"/master-data/categories/{category.id}/edit",
            "category": category,
            "form_data": {
                "name": category.name,
                "description": category.description or "",
                "active": category.active,
            },
            "error": None,
        },
    )


@app.post("/master-data/categories/{category_id}/edit")
async def update_product_category_route(
    category_id: int,
    request: Request,
    db: Session = Depends(get_db),
) -> Response:
    form = await request.form()
    form_data = {
        "name": str(form.get("name", "")),
        "description": str(form.get("description", "")),
        "active": _form_bool(form, "active"),
    }
    try:
        update_product_category(
            db,
            category_id=category_id,
            name=str(form_data["name"]),
            description=str(form_data["description"]),
            active=bool(form_data["active"]),
        )
        return _redirect("/master-data/categories")
    except MasterDataValidationError as exc:
        db.rollback()
        category = db.query(ProductCategory).filter(ProductCategory.id == category_id).one()
        return templates.TemplateResponse(
            request=request,
            name="product_category_form.html",
            context={
                "title": "Edit Product Category",
                "form_action": f"/master-data/categories/{category.id}/edit",
                "category": category,
                "form_data": form_data,
                "error": str(exc),
            },
        )


@app.get("/master-data/suppliers", response_class=HTMLResponse)
def suppliers(
    request: Request,
    q: str = Query(""),
    active: str = Query("all"),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    filters = {"q": q.strip(), "active": (active.strip().lower() or "all")}
    supplier_query = db.query(Supplier)
    if filters["q"]:
        term = f"%{filters['q']}%"
        supplier_query = supplier_query.filter(
            or_(
                Supplier.name.ilike(term),
                Supplier.contact_name.ilike(term),
                Supplier.phone.ilike(term),
                Supplier.email.ilike(term),
            )
        )
    if filters["active"] == "active":
        supplier_query = supplier_query.filter(Supplier.active.is_(True))
    elif filters["active"] == "inactive":
        supplier_query = supplier_query.filter(Supplier.active.is_(False))
    supplier_rows = supplier_query.order_by(Supplier.name, Supplier.id).all()
    return templates.TemplateResponse(
        request=request,
        name="suppliers_list.html",
        context={
            "title": "Suppliers",
            "suppliers": supplier_rows,
            "filters": filters,
            "result_count": len(supplier_rows),
        },
    )


@app.get("/master-data/suppliers/import", response_class=HTMLResponse)
def import_suppliers_form(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="supplier_import_form.html",
        context={
            "title": "Import Suppliers CSV",
            "result": None,
            "error": None,
        },
    )


@app.post("/master-data/suppliers/import")
async def import_suppliers_route(request: Request, file: UploadFile = File(...), db: Session = Depends(get_db)) -> HTMLResponse:
    result: SupplierImportResult | None = None
    error: str | None = None
    try:
        file_bytes = await file.read()
        result = import_suppliers_csv(
            db,
            file_name=file.filename or "suppliers.csv",
            file_bytes=file_bytes,
        )
    except (SupplierImportValidationError, UnicodeDecodeError) as exc:
        db.rollback()
        error = str(exc)
    return templates.TemplateResponse(
        request=request,
        name="supplier_import_form.html",
        context={
            "title": "Import Suppliers CSV",
            "result": result,
            "error": error,
        },
    )


@app.get("/master-data/suppliers/new", response_class=HTMLResponse)
def new_supplier(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="supplier_form.html",
        context={
            "title": "New Supplier",
            "form_action": "/master-data/suppliers",
            "supplier": None,
            "form_data": {
                "name": "",
                "contact_name": "",
                "phone": "",
                "email": "",
                "notes": "",
                "active": True,
            },
            "error": None,
        },
    )


@app.post("/master-data/suppliers")
async def create_supplier_route(request: Request, db: Session = Depends(get_db)) -> Response:
    form = await request.form()
    form_data = {
        "name": str(form.get("name", "")),
        "contact_name": str(form.get("contact_name", "")),
        "phone": str(form.get("phone", "")),
        "email": str(form.get("email", "")),
        "notes": str(form.get("notes", "")),
        "active": _form_bool(form, "active"),
    }
    try:
        supplier = create_supplier(
            db,
            name=str(form_data["name"]),
            contact_name=str(form_data["contact_name"]),
            phone=str(form_data["phone"]),
            email=str(form_data["email"]),
            notes=str(form_data["notes"]),
            active=bool(form_data["active"]),
        )
        return _redirect(f"/master-data/suppliers/{supplier.id}/edit")
    except MasterDataValidationError as exc:
        db.rollback()
        return templates.TemplateResponse(
            request=request,
            name="supplier_form.html",
            context={
                "title": "New Supplier",
                "form_action": "/master-data/suppliers",
                "supplier": None,
                "form_data": form_data,
                "error": str(exc),
            },
        )


@app.get("/master-data/suppliers/{supplier_id}/edit", response_class=HTMLResponse)
def edit_supplier(supplier_id: int, request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    supplier = db.query(Supplier).filter(Supplier.id == supplier_id).one()
    return templates.TemplateResponse(
        request=request,
        name="supplier_form.html",
        context={
            "title": "Edit Supplier",
            "form_action": f"/master-data/suppliers/{supplier.id}/edit",
            "supplier": supplier,
            "form_data": {
                "name": supplier.name,
                "contact_name": supplier.contact_name or "",
                "phone": supplier.phone or "",
                "email": supplier.email or "",
                "notes": supplier.notes or "",
                "active": supplier.active,
            },
            "error": None,
        },
    )


@app.post("/master-data/suppliers/{supplier_id}/edit")
async def update_supplier_route(supplier_id: int, request: Request, db: Session = Depends(get_db)) -> Response:
    form = await request.form()
    form_data = {
        "name": str(form.get("name", "")),
        "contact_name": str(form.get("contact_name", "")),
        "phone": str(form.get("phone", "")),
        "email": str(form.get("email", "")),
        "notes": str(form.get("notes", "")),
        "active": _form_bool(form, "active"),
    }
    try:
        update_supplier(
            db,
            supplier_id=supplier_id,
            name=str(form_data["name"]),
            contact_name=str(form_data["contact_name"]),
            phone=str(form_data["phone"]),
            email=str(form_data["email"]),
            notes=str(form_data["notes"]),
            active=bool(form_data["active"]),
        )
        return _redirect("/master-data/suppliers")
    except MasterDataValidationError as exc:
        db.rollback()
        supplier = db.query(Supplier).filter(Supplier.id == supplier_id).one()
        return templates.TemplateResponse(
            request=request,
            name="supplier_form.html",
            context={
                "title": "Edit Supplier",
                "form_action": f"/master-data/suppliers/{supplier.id}/edit",
                "supplier": supplier,
                "form_data": form_data,
                "error": str(exc),
            },
        )


@app.get("/master-data/channels", response_class=HTMLResponse)
def channels_master(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    channels = db.query(Channel).order_by(Channel.name, Channel.id).all()
    return templates.TemplateResponse(
        request=request,
        name="channels_list.html",
        context={"title": "Channels", "channels": channels},
    )


@app.get("/master-data/channels/new", response_class=HTMLResponse)
def new_channel(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="channel_form.html",
        context={
            "title": "New Channel",
            "form_action": "/master-data/channels",
            "channel": None,
            "form_data": {
                "name": "",
                "active": True,
                "applies_to_b2b": False,
                "applies_to_b2c": False,
                "observations": "",
            },
            "error": None,
        },
    )


@app.post("/master-data/channels")
async def create_channel_route(request: Request, db: Session = Depends(get_db)) -> Response:
    form = await request.form()
    form_data = {
        "name": str(form.get("name", "")),
        "active": _form_bool(form, "active"),
        "applies_to_b2b": _form_bool(form, "applies_to_b2b"),
        "applies_to_b2c": _form_bool(form, "applies_to_b2c"),
        "observations": str(form.get("observations", "")),
    }
    try:
        channel = create_channel(db, **form_data)
        return _redirect(f"/master-data/channels/{channel.id}/edit")
    except MasterDataValidationError as exc:
        db.rollback()
        return templates.TemplateResponse(
            request=request,
            name="channel_form.html",
            context={
                "title": "New Channel",
                "form_action": "/master-data/channels",
                "channel": None,
                "form_data": form_data,
                "error": str(exc),
            },
        )


@app.get("/master-data/channels/{channel_id}/edit", response_class=HTMLResponse)
def edit_channel(channel_id: int, request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    channel = db.query(Channel).filter(Channel.id == channel_id).one()
    return templates.TemplateResponse(
        request=request,
        name="channel_form.html",
        context={
            "title": "Edit Channel",
            "form_action": f"/master-data/channels/{channel.id}/edit",
            "channel": channel,
            "form_data": {
                "name": channel.name,
                "active": channel.active,
                "applies_to_b2b": channel.applies_to_b2b,
                "applies_to_b2c": channel.applies_to_b2c,
                "observations": channel.observations or "",
            },
            "error": None,
        },
    )


@app.post("/master-data/channels/{channel_id}/edit")
async def update_channel_route(channel_id: int, request: Request, db: Session = Depends(get_db)) -> Response:
    form = await request.form()
    form_data = {
        "name": str(form.get("name", "")),
        "active": _form_bool(form, "active"),
        "applies_to_b2b": _form_bool(form, "applies_to_b2b"),
        "applies_to_b2c": _form_bool(form, "applies_to_b2c"),
        "observations": str(form.get("observations", "")),
    }
    try:
        update_channel(db, channel_id=channel_id, **form_data)
        return _redirect("/master-data/channels")
    except MasterDataValidationError as exc:
        db.rollback()
        channel = db.query(Channel).filter(Channel.id == channel_id).one()
        return templates.TemplateResponse(
            request=request,
            name="channel_form.html",
            context={
                "title": "Edit Channel",
                "form_action": f"/master-data/channels/{channel.id}/edit",
                "channel": channel,
                "form_data": form_data,
                "error": str(exc),
            },
        )


@app.get("/master-data/discounts", response_class=HTMLResponse)
def discounts_master(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    discount_rules = db.query(DiscountRule).order_by(DiscountRule.name, DiscountRule.id).all()
    return templates.TemplateResponse(
        request=request,
        name="discounts_list.html",
        context={"title": "Discounts", "discount_rules": discount_rules},
    )


@app.get("/master-data/discounts/new", response_class=HTMLResponse)
def new_discount_rule(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="discount_form.html",
        context={
            "title": "New Discount",
            "form_action": "/master-data/discounts",
            "discount_rule": None,
            "form_data": {
                "name": "",
                "discount_type": "percentage",
                "value": "",
                "applies_to": "order_total",
                "channel": "b2c",
                "active": True,
                "description": "",
            },
            "discount_types": _discount_types(),
            "discount_applies_to_options": _discount_applies_to_options(),
            "discount_channels": _discount_channels(),
            "error": None,
        },
    )


@app.post("/master-data/discounts")
async def create_discount_rule_route(request: Request, db: Session = Depends(get_db)) -> Response:
    form = await request.form()
    form_data = {
        "name": str(form.get("name", "")),
        "discount_type": str(form.get("discount_type", "")),
        "value": str(form.get("value", "")),
        "applies_to": str(form.get("applies_to", "")),
        "channel": str(form.get("channel", "")),
        "active": _form_bool(form, "active"),
        "description": str(form.get("description", "")),
    }
    try:
        discount_rule = create_discount_rule(db, **form_data)
        return _redirect(f"/master-data/discounts/{discount_rule.id}/edit")
    except MasterDataValidationError as exc:
        db.rollback()
        return templates.TemplateResponse(
            request=request,
            name="discount_form.html",
            context={
                "title": "New Discount",
                "form_action": "/master-data/discounts",
                "discount_rule": None,
                "form_data": form_data,
                "discount_types": _discount_types(),
                "discount_applies_to_options": _discount_applies_to_options(),
                "discount_channels": _discount_channels(),
                "error": str(exc),
            },
        )


@app.get("/master-data/discounts/{discount_id}/edit", response_class=HTMLResponse)
def edit_discount_rule(discount_id: int, request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    discount_rule = db.query(DiscountRule).filter(DiscountRule.id == discount_id).one()
    return templates.TemplateResponse(
        request=request,
        name="discount_form.html",
        context={
            "title": "Edit Discount",
            "form_action": f"/master-data/discounts/{discount_rule.id}/edit",
            "discount_rule": discount_rule,
            "form_data": {
                "name": discount_rule.name,
                "discount_type": discount_rule.discount_type,
                "value": discount_rule.value,
                "applies_to": discount_rule.applies_to,
                "channel": discount_rule.channel,
                "active": discount_rule.active,
                "description": discount_rule.description or "",
            },
            "discount_types": _discount_types(),
            "discount_applies_to_options": _discount_applies_to_options(),
            "discount_channels": _discount_channels(),
            "error": None,
        },
    )


@app.post("/master-data/discounts/{discount_id}/edit")
async def update_discount_rule_route(discount_id: int, request: Request, db: Session = Depends(get_db)) -> Response:
    form = await request.form()
    form_data = {
        "name": str(form.get("name", "")),
        "discount_type": str(form.get("discount_type", "")),
        "value": str(form.get("value", "")),
        "applies_to": str(form.get("applies_to", "")),
        "channel": str(form.get("channel", "")),
        "active": _form_bool(form, "active"),
        "description": str(form.get("description", "")),
    }
    try:
        update_discount_rule(db, discount_rule_id=discount_id, **form_data)
        return _redirect("/master-data/discounts")
    except MasterDataValidationError as exc:
        db.rollback()
        discount_rule = db.query(DiscountRule).filter(DiscountRule.id == discount_id).one()
        return templates.TemplateResponse(
            request=request,
            name="discount_form.html",
            context={
                "title": "Edit Discount",
                "form_action": f"/master-data/discounts/{discount_rule.id}/edit",
                "discount_rule": discount_rule,
                "form_data": form_data,
                "discount_types": _discount_types(),
                "discount_applies_to_options": _discount_applies_to_options(),
                "discount_channels": _discount_channels(),
                "error": str(exc),
            },
        )


@app.get("/master-data/products", response_class=HTMLResponse)
def products_master(
    request: Request,
    q: str = Query(""),
    active: str = Query("all"),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    filters = {"q": q.strip(), "active": (active.strip().lower() or "all")}
    product_query = (
        db.query(Product)
        .options(
            joinedload(Product.category),
            joinedload(Product.supplier_record),
            joinedload(Product.default_route),
        )
    )
    if filters["q"]:
        term = f"%{filters['q']}%"
        product_query = product_query.filter(
            or_(
                Product.sku.ilike(term),
                Product.name.ilike(term),
            )
        )
    if filters["active"] == "active":
        product_query = product_query.filter(Product.active.is_(True))
    elif filters["active"] == "inactive":
        product_query = product_query.filter(Product.active.is_(False))
    products = product_query.order_by(Product.sku, Product.id).all()
    return templates.TemplateResponse(
        request=request,
        name="products_list.html",
        context={
            "title": "Products / SKUs",
            "products": products,
            "filters": filters,
            "result_count": len(products),
        },
    )


@app.get("/master-data/products/export")
def export_products_master_csv(db: Session = Depends(get_db)) -> Response:
    products = (
        db.query(Product)
        .options(
            joinedload(Product.category),
            joinedload(Product.supplier_record),
            joinedload(Product.default_route),
        )
        .order_by(Product.sku, Product.id)
        .all()
    )

    def _yes_no(value: bool) -> str:
        return "yes" if value else "no"

    def _decimal_text(value: object) -> str:
        return "" if value is None else str(value)

    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(
        (
            "SKU",
            "Name",
            "Category",
            "Supplier",
            "Active",
            "Available for sale",
            "Manufactured",
            "Purchased",
            "B2C price",
            "B2B price",
            "Standard cost (reference)",
            "Has route assigned",
            "Route code",
            "Route name",
            "Route version",
            "Route process type",
            "Route active",
            "Detail URL",
        )
    )
    for product in products:
        route = product.default_route
        writer.writerow(
            (
                product.sku,
                product.name,
                product.category.name if product.category else "",
                product.supplier_record.name if product.supplier_record else (product.supplier or ""),
                _yes_no(product.active),
                _yes_no(product.available_for_sale_gc),
                _yes_no(product.is_manufactured),
                _yes_no(product.is_purchased_product),
                _decimal_text(product.b2c_price),
                _decimal_text(product.b2b_price),
                _decimal_text(product.standard_cost),
                _yes_no(route is not None),
                route.code if route else "",
                route.name if route else "",
                route.version if route else "",
                route.process_type if route else "",
                _yes_no(route.active) if route else "",
                f"/master-data/products/{product.id}",
            )
        )

    return Response(
        content=buffer.getvalue().encode("utf-8-sig"),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="product_master_with_routes.csv"'},
    )


@app.get("/master-data/products/search")
def search_master_data_products(
    q: str = Query(default=""),
    db: Session = Depends(get_db),
) -> JSONResponse:
    search_text = q.strip()
    if len(search_text) < 2:
        return JSONResponse([])

    pattern = f"%{search_text}%"
    products = (
        db.query(Product)
        .filter(
            Product.active.is_(True),
            or_(
                Product.sku.ilike(pattern),
                Product.name.ilike(pattern),
                Product.description.ilike(pattern),
            ),
        )
        .order_by(Product.sku, Product.name)
        .limit(20)
        .all()
    )
    return JSONResponse([_product_lookup_payload(product) for product in products])


@app.get("/master-data/products/new", response_class=HTMLResponse)
def new_product_master(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="product_form.html",
        context=_product_form_context(
            title="New Product / SKU",
            form_action="/master-data/products",
            product=None,
            categories=list_category_options(db),
            suppliers=list_supplier_options(db),
        ),
    )


@app.post("/master-data/products")
async def create_product_master_route(request: Request, db: Session = Depends(get_db)) -> Response:
    form = await request.form()
    product_data = {
        "sku": str(form.get("sku", "")),
        "name": str(form.get("name", "")),
        "unit": str(form.get("unit", "")),
        "category_id": str(form.get("category_id", "")),
        "supplier_id": str(form.get("supplier_id", "")),
        "description": str(form.get("description", "")),
        "observations": str(form.get("observations", "")),
        "b2c_price": str(form.get("b2c_price", "")),
        "b2b_price": str(form.get("b2b_price", "")),
        "standard_cost": str(form.get("standard_cost", "")),
        "active": _form_bool(form, "active"),
        "available_for_sale_gc": _form_bool(form, "available_for_sale_gc"),
        "is_manufactured": _form_bool(form, "is_manufactured"),
        "is_purchased_product": _form_bool(form, "is_purchased_product"),
    }
    try:
        product = create_product_master(db, **product_data)
        return _redirect(f"/master-data/products/{product.id}")
    except MasterDataValidationError as exc:
        db.rollback()
        category_id = int(product_data["category_id"]) if str(product_data["category_id"]).isdigit() else None
        supplier_id = int(product_data["supplier_id"]) if str(product_data["supplier_id"]).isdigit() else None
        return templates.TemplateResponse(
            request=request,
            name="product_form.html",
            context=_product_form_context(
                title="New Product / SKU",
                form_action="/master-data/products",
                product=None,
                categories=list_category_options(db, category_id),
                suppliers=list_supplier_options(db, supplier_id),
                error=str(exc),
                product_data=product_data,
            ),
        )


@app.get("/master-data/products/{product_id}", response_class=HTMLResponse)
def product_detail(product_id: int, request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    product = get_product_for_detail(db, product_id)
    balance = get_product_balance(db, product_id)
    bom = get_or_seed_product_bom(db, product) if product.is_manufactured else None
    bom_lines = sorted(
        bom.lines,
        key=lambda line: (line.line_number, line.id),
    ) if bom is not None else []
    return templates.TemplateResponse(
        request=request,
        name="product_detail.html",
        context={
            "title": "Product Detail",
            "product": product,
            "balance": balance,
            "bom": bom,
            "bom_lines": bom_lines,
        },
    )


@app.get("/master-data/products/{product_id}/edit", response_class=HTMLResponse)
def edit_product_master(product_id: int, request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    product = get_product_for_detail(db, product_id)
    return templates.TemplateResponse(
        request=request,
        name="product_form.html",
        context=_product_form_context(
            title="Edit Product / SKU",
            form_action=f"/master-data/products/{product.id}/edit",
            product=product,
            categories=list_category_options(db, product.category_id),
            suppliers=list_supplier_options(db, product.supplier_id),
        ),
    )


@app.post("/master-data/products/{product_id}/edit")
async def update_product_master_route(
    product_id: int,
    request: Request,
    db: Session = Depends(get_db),
) -> Response:
    form = await request.form()
    product_data = {
        "sku": str(form.get("sku", "")),
        "name": str(form.get("name", "")),
        "unit": str(form.get("unit", "")),
        "category_id": str(form.get("category_id", "")),
        "supplier_id": str(form.get("supplier_id", "")),
        "description": str(form.get("description", "")),
        "observations": str(form.get("observations", "")),
        "b2c_price": str(form.get("b2c_price", "")),
        "b2b_price": str(form.get("b2b_price", "")),
        "standard_cost": str(form.get("standard_cost", "")),
        "active": _form_bool(form, "active"),
        "available_for_sale_gc": _form_bool(form, "available_for_sale_gc"),
        "is_manufactured": _form_bool(form, "is_manufactured"),
        "is_purchased_product": _form_bool(form, "is_purchased_product"),
    }
    try:
        update_product_master(db, product_id=product_id, **product_data)
        return _redirect(f"/master-data/products/{product_id}")
    except MasterDataValidationError as exc:
        db.rollback()
        product = get_product_for_detail(db, product_id)
        category_id = int(product_data["category_id"]) if str(product_data["category_id"]).isdigit() else product.category_id
        supplier_id = int(product_data["supplier_id"]) if str(product_data["supplier_id"]).isdigit() else product.supplier_id
        return templates.TemplateResponse(
            request=request,
            name="product_form.html",
            context=_product_form_context(
                title="Edit Product / SKU",
                form_action=f"/master-data/products/{product.id}/edit",
                product=product,
                categories=list_category_options(db, category_id),
                suppliers=list_supplier_options(db, supplier_id),
                error=str(exc),
                product_data=product_data,
            ),
        )


@app.get("/master-data/products/{product_id}/bom/edit", response_class=HTMLResponse)
def edit_product_bom(product_id: int, request: Request, db: Session = Depends(get_db)) -> Response:
    product = get_product_for_detail(db, product_id)
    if not product.is_manufactured:
        return _redirect(f"/master-data/products/{product_id}")

    bom = get_or_seed_product_bom(db, product)
    bom_lines = sorted(
        bom.lines,
        key=lambda line: (line.line_number, line.id),
    ) if bom is not None else []
    current_component_ids = [line.component_product_id for line in bom_lines if line.component_product_id is not None]
    existing_component_options = list_bom_component_options(
        db,
        current_component_ids,
        active_only=True,
        exclude_product_id=product.id,
    )
    new_component_options = list_bom_component_options(db, active_only=True, exclude_product_id=product.id)
    return templates.TemplateResponse(
        request=request,
        name="product_bom_edit.html",
        context={
            "title": "Edit BOM",
            "product": product,
            "bom": bom,
            "existing_lines": _product_bom_existing_rows(bom_lines),
            "new_rows": _blank_product_bom_new_rows(),
            "existing_component_options": existing_component_options,
            "existing_component_option_skus": [option.sku for option in existing_component_options],
            "new_component_options": new_component_options,
            "error": None,
        },
    )


@app.post("/master-data/products/{product_id}/bom/edit")
async def update_product_bom_route(
    product_id: int,
    request: Request,
    db: Session = Depends(get_db),
) -> Response:
    form = await request.form()
    line_ids = [str(line_id).strip() for line_id in form.getlist("line_id") if str(line_id).strip()]
    deleted_line_ids = {str(line_id).strip() for line_id in form.getlist("delete_line_id") if str(line_id).strip()}
    line_updates = [
        {
            "id": line_id,
            "component_sku": str(form.get(f"component_sku_{line_id}", "")),
            "quantity_standard": str(form.get(f"quantity_standard_{line_id}", "")),
            "notes": str(form.get(f"notes_{line_id}", "")),
            "delete": line_id in deleted_line_ids,
        }
        for line_id in line_ids
    ]
    new_indexes = [str(index).strip() for index in form.getlist("new_line_index") if str(index).strip()]
    new_lines = [
        {
            "component_sku": str(form.get(f"new_component_sku_{index}", "")),
            "quantity_standard": str(form.get(f"new_quantity_standard_{index}", "")),
            "notes": str(form.get(f"new_notes_{index}", "")),
        }
        for index in new_indexes
    ]

    try:
        save_product_bom(
            db,
            product_id=product_id,
            line_updates=line_updates,
            new_lines=new_lines,
        )
        return _redirect(f"/master-data/products/{product_id}")
    except ProductBomValidationError as exc:
        db.rollback()
        product = get_product_for_detail(db, product_id)
        bom = get_or_seed_product_bom(db, product) if product.is_manufactured else None
        bom_lines = sorted(
            bom.lines,
            key=lambda line: (line.line_number, line.id),
        ) if bom is not None else []
        current_component_ids = [line.component_product_id for line in bom_lines if line.component_product_id is not None]
        existing_component_options = list_bom_component_options(
            db,
            current_component_ids,
            active_only=True,
            exclude_product_id=product.id,
        )
        new_component_options = list_bom_component_options(db, active_only=True, exclude_product_id=product.id)
        component_lookup = {option.sku: option for option in [*existing_component_options, *new_component_options]}
        existing_lines_by_id = {str(line.id): line for line in bom_lines}
        existing_rows: list[dict[str, object]] = []
        for update in line_updates:
            current_line = existing_lines_by_id.get(str(update["id"]))
            selected_sku = str(update["component_sku"] or "").strip()
            selected_product = component_lookup.get(selected_sku)
            existing_rows.append(
                {
                    "id": current_line.id if current_line is not None else update["id"],
                    "component_sku_snapshot": selected_sku,
                    "component_name_snapshot": (
                        selected_product.name
                        if selected_product is not None
                        else (current_line.component_name_snapshot if current_line is not None else "")
                    ) or "",
                    "unit_snapshot": (
                        selected_product.unit
                        if selected_product is not None
                        else (current_line.unit_snapshot if current_line is not None else "")
                    ) or "",
                    "quantity_standard": str(update["quantity_standard"] or ""),
                    "notes": str(update["notes"] or ""),
                    "component_product_id": (
                        selected_product.id
                        if selected_product is not None
                        else (current_line.component_product_id if current_line is not None else None)
                    ),
                    "delete": bool(update["delete"]),
                }
            )

        new_rows = [
            {
                "index": index,
                "component_sku": str(line["component_sku"] or ""),
                "quantity_standard": str(line["quantity_standard"] or ""),
                "notes": str(line["notes"] or ""),
            }
            for index, line in enumerate(new_lines, start=1)
        ]
        while len(new_rows) < 5:
            new_rows.append(
                {
                    "index": len(new_rows) + 1,
                    "component_sku": "",
                    "quantity_standard": "",
                    "notes": "",
                }
            )

        return templates.TemplateResponse(
            request=request,
            name="product_bom_edit.html",
            context={
                "title": "Edit BOM",
                "product": product,
                "bom": bom,
                "existing_lines": existing_rows,
                "new_rows": new_rows,
                "existing_component_options": existing_component_options,
                "existing_component_option_skus": [option.sku for option in existing_component_options],
                "new_component_options": new_component_options,
                "error": str(exc),
            },
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


@app.get("/sales/orders-menu", response_class=HTMLResponse)
def sales_orders_menu(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="sales_orders_menu.html",
        context={"title": "Sales Orders"},
    )


@app.get("/sales/customers-menu", response_class=HTMLResponse)
def sales_customers_menu(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="sales_customers_menu.html",
        context={"title": "Customers"},
    )


@app.get("/sales/reporting", response_class=HTMLResponse)
def sales_reporting_menu(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="sales_reporting_menu.html",
        context={"title": "Reporting"},
    )


@app.get("/sales/total", response_class=HTMLResponse)
def total_sales(
    request: Request,
    date_from: str = Query(""),
    date_to: str = Query(""),
    sales_type: str = Query("all"),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    error = None
    rows = []
    monthly_chart_data: list[dict[str, object]] = []
    monthly_cogs_hidden = False
    export_url = _build_sales_export_url("/sales/total/export", {"sales_type": "all"})
    filters = {"date_from": "", "date_to": "", "sales_type": "all"}
    using_default_date_window = False

    try:
        filters, parsed_date_from, parsed_date_to, using_default_date_window = _normalize_sales_reporting_filters(
            date_from=date_from,
            date_to=date_to,
            sales_type=sales_type,
            apply_default_date_window=True,
        )
        rows = get_total_sales_rows(
            db,
            date_from=parsed_date_from,
            date_to=parsed_date_to,
            sales_type=filters["sales_type"],
        )
        monthly_chart_points = get_total_sales_monthly_summary(
            db,
            date_from=parsed_date_from,
            date_to=parsed_date_to,
            sales_type=filters["sales_type"],
        )
        monthly_chart_data = [
            {
                "label": point.month_label,
                "net_sales": _serialize_decimal_for_chart(point.net_sales),
                "cogs": _serialize_decimal_for_chart(point.cogs),
                "gross_profit": _serialize_decimal_for_chart(point.gross_profit),
            }
            for point in monthly_chart_points
        ]
        monthly_cogs_hidden = any(not point.has_complete_cogs for point in monthly_chart_points)
        export_url = _build_sales_export_url("/sales/total/export", filters)
    except ValueError:
        error = "Dates must use YYYY-MM-DD."
        try:
            raw_filters = {
                "date_from": date_from.strip(),
                "date_to": date_to.strip(),
            }
            if raw_filters["date_from"] and raw_filters["date_to"]:
                parsed_date_from = _parse_optional_date(raw_filters["date_from"])
                parsed_date_to = _parse_optional_date(raw_filters["date_to"])
                if parsed_date_from is not None and parsed_date_to is not None and parsed_date_from > parsed_date_to:
                    error = "Date from cannot be later than date to."
        except ValueError:
            pass

    return templates.TemplateResponse(
        request=request,
        name="total_sales.html",
        context={
            "title": "Total Sales",
            "rows": rows,
            "filters": filters,
            "error": error,
            "result_count": len(rows),
            "using_default_date_window": using_default_date_window,
            "export_url": export_url,
            "monthly_chart_data": monthly_chart_data,
            "monthly_cogs_hidden": monthly_cogs_hidden,
        },
    )


@app.get("/sales/total/export")
def export_total_sales(
    date_from: str = Query(""),
    date_to: str = Query(""),
    sales_type: str = Query("all"),
    db: Session = Depends(get_db),
) -> Response:
    try:
        filters, parsed_date_from, parsed_date_to, _ = _normalize_sales_reporting_filters(
            date_from=date_from,
            date_to=date_to,
            sales_type=sales_type,
            apply_default_date_window=True,
        )
    except ValueError as exc:
        return Response(str(exc), status_code=400, media_type="text/plain; charset=utf-8")

    rows = get_total_sales_rows(
        db,
        date_from=parsed_date_from,
        date_to=parsed_date_to,
        sales_type=filters["sales_type"],
    )
    csv_rows = [
        (
            row.sales_source,
            row.order_number,
            row.order_date,
            row.customer_name or "",
            row.channel_name or "",
            row.sku,
            row.description,
            row.category_name or "",
            row.quantity,
            row.unit_price,
            row.gross_sales,
            row.discount_amount or ZERO,
            row.line_total,
            _csv_blank_if_none(row.cogs),
            _csv_blank_if_none(row.gross_profit),
            row.order_status,
            _sales_row_detail_url(row.sales_source, row.order_id),
        )
        for row in rows
    ]
    return _csv_report_response(
        filename="total_sales_report.csv",
        headers=(
            "Sales Source",
            "Order Number",
            "Report Date",
            "Customer",
            "Channel",
            "SKU",
            "Description",
            "Category",
            "Quantity",
            "Unit Price",
            "Gross Sales",
            "Discount",
            "Net Sales",
            "COGS",
            "Gross Profit",
            "Order Status",
            "Detail URL",
        ),
        rows=csv_rows,
    )


@app.get("/sales/summary", response_class=HTMLResponse)
def sales_summary(
    request: Request,
    date_from: str = Query(""),
    date_to: str = Query(""),
    sales_type: str = Query("all"),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    filters = {"date_from": "", "date_to": "", "sales_type": "all"}
    error = None
    summary = None
    export_url = _build_sales_export_url("/sales/summary/export", {"sales_type": "all"})
    net_sales_mix_chart = None
    gross_profit_mix_chart = None
    gross_profit_chart_message = None

    try:
        filters, parsed_date_from, parsed_date_to, _ = _normalize_sales_reporting_filters(
            date_from=date_from,
            date_to=date_to,
            sales_type=sales_type,
            apply_default_date_window=False,
        )
        summary = get_sales_summary(
            db,
            date_from=parsed_date_from,
            date_to=parsed_date_to,
            sales_type=filters["sales_type"],
        )
        export_url = _build_sales_export_url("/sales/summary/export", filters)
        total_net_sales_mix = summary.b2b.total_net_sales + summary.b2c.total_net_sales
        if total_net_sales_mix > ZERO:
            net_sales_mix_chart = {
                "title": "Net Sales mix",
                "series": [
                    {"label": "B2B", "value": _serialize_decimal_for_chart(summary.b2b.total_net_sales)},
                    {"label": "B2C", "value": _serialize_decimal_for_chart(summary.b2c.total_net_sales)},
                ],
            }
        if summary.b2b.gross_profit is not None and summary.b2c.gross_profit is not None:
            total_gross_profit_mix = summary.b2b.gross_profit + summary.b2c.gross_profit
            if (
                total_gross_profit_mix > ZERO
                and summary.b2b.gross_profit >= ZERO
                and summary.b2c.gross_profit >= ZERO
            ):
                gross_profit_mix_chart = {
                    "title": "Gross Profit mix",
                    "series": [
                        {"label": "B2B", "value": _serialize_decimal_for_chart(summary.b2b.gross_profit)},
                        {"label": "B2C", "value": _serialize_decimal_for_chart(summary.b2c.gross_profit)},
                    ],
                }
            else:
                gross_profit_chart_message = "Gross Profit chart unavailable for the current mix."
        else:
            gross_profit_chart_message = "Gross Profit chart unavailable because COGS coverage is incomplete."
    except ValueError:
        error = "Dates must use YYYY-MM-DD."
        try:
            raw_filters = {
                "date_from": date_from.strip(),
                "date_to": date_to.strip(),
            }
            if raw_filters["date_from"] and raw_filters["date_to"]:
                parsed_date_from = _parse_optional_date(raw_filters["date_from"])
                parsed_date_to = _parse_optional_date(raw_filters["date_to"])
                if parsed_date_from is not None and parsed_date_to is not None and parsed_date_from > parsed_date_to:
                    error = "Date from cannot be later than date to."
        except ValueError:
            pass

    return templates.TemplateResponse(
        request=request,
        name="sales_summary.html",
        context={
            "title": "Sales Summary",
            "summary": summary,
            "filters": filters,
            "error": error,
            "export_url": export_url,
            "net_sales_mix_chart": net_sales_mix_chart,
            "gross_profit_mix_chart": gross_profit_mix_chart,
            "gross_profit_chart_message": gross_profit_chart_message,
        },
    )


@app.get("/sales/summary/export")
def export_sales_summary(
    date_from: str = Query(""),
    date_to: str = Query(""),
    sales_type: str = Query("all"),
    db: Session = Depends(get_db),
) -> Response:
    try:
        filters, parsed_date_from, parsed_date_to, _ = _normalize_sales_reporting_filters(
            date_from=date_from,
            date_to=date_to,
            sales_type=sales_type,
            apply_default_date_window=False,
        )
    except ValueError as exc:
        return Response(str(exc), status_code=400, media_type="text/plain; charset=utf-8")

    summary = get_sales_summary(
        db,
        date_from=parsed_date_from,
        date_to=parsed_date_to,
        sales_type=filters["sales_type"],
    )
    csv_rows = [
        (
            bucket.label,
            bucket.total_gross_sales,
            bucket.total_discount,
            bucket.total_net_sales,
            _csv_blank_if_none(bucket.cogs),
            _csv_blank_if_none(bucket.gross_profit),
            bucket.total_orders,
            bucket.total_lines,
            bucket.total_quantity,
            bucket.average_order_value,
            bucket.average_line_value,
            bucket.cogs_coverage_label,
            bucket.cogs_lines_with_value,
            bucket.cogs_total_lines,
        )
        for bucket in [summary.b2b, summary.b2c, summary.total]
    ]
    return _csv_report_response(
        filename="sales_by_business_activity_report.csv",
        headers=(
            "Source",
            "Gross Sales",
            "Discount",
            "Net Sales",
            "COGS",
            "Gross Profit",
            "Orders",
            "Lines",
            "Quantity",
            "Average Ticket",
            "Average Line Value",
            "COGS Coverage",
            "COGS Lines With Value",
            "COGS Total Lines",
        ),
        rows=csv_rows,
    )


@app.get("/sales/items-pareto", response_class=HTMLResponse)
def sales_items_pareto(
    request: Request,
    date_from: str = Query(""),
    date_to: str = Query(""),
    sales_type: str = Query("all"),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    filters = {"date_from": "", "date_to": "", "sales_type": "all"}
    error = None
    pareto = None
    export_url = _build_sales_export_url("/sales/items-pareto/export", {"sales_type": "all"})

    try:
        filters, parsed_date_from, parsed_date_to, _ = _normalize_sales_reporting_filters(
            date_from=date_from,
            date_to=date_to,
            sales_type=sales_type,
            apply_default_date_window=False,
        )
        pareto = get_sales_items_pareto(
            db,
            date_from=parsed_date_from,
            date_to=parsed_date_to,
            sales_type=filters["sales_type"],
        )
        export_url = _build_sales_export_url("/sales/items-pareto/export", filters)
    except ValueError:
        error = "Dates must use YYYY-MM-DD."
        try:
            raw_filters = {
                "date_from": date_from.strip(),
                "date_to": date_to.strip(),
            }
            if raw_filters["date_from"] and raw_filters["date_to"]:
                parsed_date_from = _parse_optional_date(raw_filters["date_from"])
                parsed_date_to = _parse_optional_date(raw_filters["date_to"])
                if parsed_date_from is not None and parsed_date_to is not None and parsed_date_from > parsed_date_to:
                    error = "Date from cannot be later than date to."
        except ValueError:
            pass

    return templates.TemplateResponse(
        request=request,
        name="sales_items_pareto.html",
        context={
            "title": "Sales by Items Pareto",
            "pareto": pareto,
            "filters": filters,
            "error": error,
            "export_url": export_url,
        },
    )


@app.get("/sales/items-pareto/export")
def export_sales_items_pareto(
    date_from: str = Query(""),
    date_to: str = Query(""),
    sales_type: str = Query("all"),
    db: Session = Depends(get_db),
) -> Response:
    try:
        filters, parsed_date_from, parsed_date_to, _ = _normalize_sales_reporting_filters(
            date_from=date_from,
            date_to=date_to,
            sales_type=sales_type,
            apply_default_date_window=False,
        )
    except ValueError as exc:
        return Response(str(exc), status_code=400, media_type="text/plain; charset=utf-8")

    pareto = get_sales_items_pareto(
        db,
        date_from=parsed_date_from,
        date_to=parsed_date_to,
        sales_type=filters["sales_type"],
    )
    csv_rows = [
        (
            row.sku,
            row.description,
            row.net_sales,
            row.quantity,
            row.lines,
            row.orders,
            row.discount,
            row.percent_of_total_sales,
            row.cumulative_percent,
            row.pareto_class,
        )
        for row in pareto.rows
    ]
    return _csv_report_response(
        filename="sales_by_items_pareto_report.csv",
        headers=(
            "SKU",
            "Description",
            "Net Sales",
            "Quantity",
            "Lines",
            "Orders",
            "Discount",
            "% Sales",
            "Cumulative %",
            "Class",
        ),
        rows=csv_rows,
    )


@app.get("/sales/categories-pareto", response_class=HTMLResponse)
def sales_categories_pareto(
    request: Request,
    date_from: str = Query(""),
    date_to: str = Query(""),
    sales_type: str = Query("all"),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    filters = {"date_from": "", "date_to": "", "sales_type": "all"}
    error = None
    pareto = None
    export_url = _build_sales_export_url("/sales/categories-pareto/export", {"sales_type": "all"})

    try:
        filters, parsed_date_from, parsed_date_to, _ = _normalize_sales_reporting_filters(
            date_from=date_from,
            date_to=date_to,
            sales_type=sales_type,
            apply_default_date_window=False,
        )
        pareto = get_sales_categories_pareto(
            db,
            date_from=parsed_date_from,
            date_to=parsed_date_to,
            sales_type=filters["sales_type"],
        )
        export_url = _build_sales_export_url("/sales/categories-pareto/export", filters)
    except ValueError:
        error = "Dates must use YYYY-MM-DD."
        try:
            raw_filters = {
                "date_from": date_from.strip(),
                "date_to": date_to.strip(),
            }
            if raw_filters["date_from"] and raw_filters["date_to"]:
                parsed_date_from = _parse_optional_date(raw_filters["date_from"])
                parsed_date_to = _parse_optional_date(raw_filters["date_to"])
                if parsed_date_from is not None and parsed_date_to is not None and parsed_date_from > parsed_date_to:
                    error = "Date from cannot be later than date to."
        except ValueError:
            pass

    return templates.TemplateResponse(
        request=request,
        name="sales_categories_pareto.html",
        context={
            "title": "Sales by Category Pareto",
            "pareto": pareto,
            "filters": filters,
            "error": error,
            "export_url": export_url,
        },
    )


@app.get("/sales/categories-pareto/export")
def export_sales_categories_pareto(
    date_from: str = Query(""),
    date_to: str = Query(""),
    sales_type: str = Query("all"),
    db: Session = Depends(get_db),
) -> Response:
    try:
        filters, parsed_date_from, parsed_date_to, _ = _normalize_sales_reporting_filters(
            date_from=date_from,
            date_to=date_to,
            sales_type=sales_type,
            apply_default_date_window=False,
        )
    except ValueError as exc:
        return Response(str(exc), status_code=400, media_type="text/plain; charset=utf-8")

    pareto = get_sales_categories_pareto(
        db,
        date_from=parsed_date_from,
        date_to=parsed_date_to,
        sales_type=filters["sales_type"],
    )
    csv_rows = [
        (
            row.category_name,
            row.net_sales,
            row.quantity,
            row.lines,
            row.orders,
            row.items_count,
            row.discount,
            row.percent_of_total_sales,
            row.cumulative_percent,
            row.pareto_class,
        )
        for row in pareto.rows
    ]
    return _csv_report_response(
        filename="sales_by_category_pareto_report.csv",
        headers=(
            "Category",
            "Net Sales",
            "Quantity",
            "Lines",
            "Orders",
            "Items",
            "Discount",
            "% Sales",
            "Cumulative %",
            "Class",
        ),
        rows=csv_rows,
    )


@app.get("/sales/orders", response_class=HTMLResponse)
def sales_by_order(
    request: Request,
    date_from: str = Query(""),
    date_to: str = Query(""),
    sales_type: str = Query("all"),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    filters = {"date_from": "", "date_to": "", "sales_type": "all"}
    error = None
    result = None
    export_url = _build_sales_export_url("/sales/orders/export", {"sales_type": "all"})
    using_default_date_window = False

    try:
        filters, parsed_date_from, parsed_date_to, using_default_date_window = _normalize_sales_reporting_filters(
            date_from=date_from,
            date_to=date_to,
            sales_type=sales_type,
            apply_default_date_window=True,
        )
        result = get_sales_by_order(
            db,
            date_from=parsed_date_from,
            date_to=parsed_date_to,
            sales_type=filters["sales_type"],
        )
        export_url = _build_sales_export_url("/sales/orders/export", filters)
    except ValueError:
        error = "Dates must use YYYY-MM-DD."
        try:
            raw_filters = {
                "date_from": date_from.strip(),
                "date_to": date_to.strip(),
            }
            if raw_filters["date_from"] and raw_filters["date_to"]:
                parsed_date_from = _parse_optional_date(raw_filters["date_from"])
                parsed_date_to = _parse_optional_date(raw_filters["date_to"])
                if parsed_date_from is not None and parsed_date_to is not None and parsed_date_from > parsed_date_to:
                    error = "Date from cannot be later than date to."
        except ValueError:
            pass

    return templates.TemplateResponse(
        request=request,
        name="sales_by_order.html",
        context={
            "title": "Sales by Order",
            "result": result,
            "filters": filters,
            "error": error,
            "using_default_date_window": using_default_date_window,
            "export_url": export_url,
        },
    )


@app.get("/sales/orders/export")
def export_sales_by_order(
    date_from: str = Query(""),
    date_to: str = Query(""),
    sales_type: str = Query("all"),
    db: Session = Depends(get_db),
) -> Response:
    try:
        filters, parsed_date_from, parsed_date_to, _ = _normalize_sales_reporting_filters(
            date_from=date_from,
            date_to=date_to,
            sales_type=sales_type,
            apply_default_date_window=True,
        )
    except ValueError as exc:
        return Response(str(exc), status_code=400, media_type="text/plain; charset=utf-8")

    result = get_sales_by_order(
        db,
        date_from=parsed_date_from,
        date_to=parsed_date_to,
        sales_type=filters["sales_type"],
    )
    csv_rows = [
        (
            row.sales_source,
            row.order_number,
            row.order_date,
            row.customer_name or "",
            row.channel_name or "",
            row.gross_sales,
            row.total_discount,
            row.net_sales,
            _csv_blank_if_none(row.cogs),
            _csv_blank_if_none(row.gross_profit),
            row.lines_count,
            row.average_line_value,
            row.detail_url,
            row.order_status,
            row.total_quantity,
            row.items_count,
            row.categories_count,
        )
        for row in result.rows
    ]
    return _csv_report_response(
        filename="sales_by_order_report.csv",
        headers=(
            "Sales Source",
            "Order Number",
            "Order Date / Reporting Date",
            "Customer",
            "Channel",
            "Gross Sales",
            "Discount",
            "Net Sales",
            "COGS",
            "Gross Profit",
            "Lines",
            "Average Line Value",
            "Detail URL",
            "Order Status",
            "Quantity",
            "Items",
            "Categories",
        ),
        rows=csv_rows,
    )


@app.get("/sales/b2c-customers", response_class=HTMLResponse)
def b2c_customers(
    request: Request,
    q: str = Query(""),
    active: str = Query("all"),
    message: str | None = Query(default=None),
    error: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    filters = {"q": q.strip(), "active": (active.strip().lower() or "all")}
    customer_query = db.query(B2CCustomer)
    if filters["q"]:
        term = f"%{filters['q']}%"
        customer_query = customer_query.filter(
            or_(
                B2CCustomer.name.ilike(term),
                B2CCustomer.phone.ilike(term),
                B2CCustomer.email.ilike(term),
            )
        )
    if filters["active"] == "active":
        customer_query = customer_query.filter(B2CCustomer.active.is_(True))
    elif filters["active"] == "inactive":
        customer_query = customer_query.filter(B2CCustomer.active.is_(False))
    customers = customer_query.order_by(B2CCustomer.active.desc(), B2CCustomer.name, B2CCustomer.id).all()
    return templates.TemplateResponse(
        request=request,
        name="b2c_customers_list.html",
        context={
            "title": "B2C Customers",
            "customers": customers,
            "filters": filters,
            "result_count": len(customers),
            "message": message,
            "error": error,
        },
    )


@app.get("/sales/b2c-customers/new", response_class=HTMLResponse)
def new_b2c_customer(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="b2c_customer_form.html",
        context=_b2c_customer_form_context(title="New B2C Customer", customer=None),
    )


@app.post("/sales/b2c-customers")
def create_b2c_customer_route(
    request: Request,
    name: str = Form(...),
    phone: str = Form(""),
    email: str = Form(""),
    address: str = Form(""),
    province: str = Form(""),
    canton: str = Form(""),
    district: str = Form(""),
    observations: str = Form(""),
    active: bool = Form(False),
    db: Session = Depends(get_db),
) -> Response:
    try:
        customer = create_b2c_customer(
            db,
            name=name,
            phone=phone,
            email=email,
            address=address,
            province=province,
            canton=canton,
            district=district,
            observations=observations,
            active=active,
        )
        return _redirect(f"/sales/b2c-customers/{customer.id}")
    except B2CCustomerValidationError as exc:
        db.rollback()
        customer = B2CCustomer(
            active=active,
            name=name,
            phone=phone or None,
            email=email or None,
            address=address or None,
            province=province or None,
            canton=canton or None,
            district=district or None,
            observations=observations or None,
        )
        return templates.TemplateResponse(
            request=request,
            name="b2c_customer_form.html",
            context=_b2c_customer_form_context(title="New B2C Customer", customer=customer, error=str(exc)),
        )


@app.get("/sales/b2c-customers/{customer_id}", response_class=HTMLResponse)
def b2c_customer_detail(customer_id: int, request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    customer = db.query(B2CCustomer).filter(B2CCustomer.id == customer_id).one()
    return templates.TemplateResponse(
        request=request,
        name="b2c_customer_detail.html",
        context={"title": "B2C Customer Detail", "customer": customer},
    )


@app.get("/sales/b2c-customers/{customer_id}/edit", response_class=HTMLResponse)
def edit_b2c_customer(customer_id: int, request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    customer = db.query(B2CCustomer).filter(B2CCustomer.id == customer_id).one()
    return templates.TemplateResponse(
        request=request,
        name="b2c_customer_form.html",
        context=_b2c_customer_form_context(title="Edit B2C Customer", customer=customer),
    )


@app.post("/sales/b2c-customers/{customer_id}/edit")
def update_b2c_customer_route(
    customer_id: int,
    request: Request,
    name: str = Form(...),
    phone: str = Form(""),
    email: str = Form(""),
    address: str = Form(""),
    province: str = Form(""),
    canton: str = Form(""),
    district: str = Form(""),
    observations: str = Form(""),
    active: bool = Form(False),
    db: Session = Depends(get_db),
) -> Response:
    try:
        update_b2c_customer(
            db,
            customer_id=customer_id,
            name=name,
            phone=phone,
            email=email,
            address=address,
            province=province,
            canton=canton,
            district=district,
            observations=observations,
            active=active,
        )
        return _redirect(f"/sales/b2c-customers/{customer_id}")
    except B2CCustomerValidationError as exc:
        db.rollback()
        customer = db.query(B2CCustomer).filter(B2CCustomer.id == customer_id).one()
        customer.active = active
        customer.name = name
        customer.phone = phone or None
        customer.email = email or None
        customer.address = address or None
        customer.province = province or None
        customer.canton = canton or None
        customer.district = district or None
        customer.observations = observations or None
        return templates.TemplateResponse(
            request=request,
            name="b2c_customer_form.html",
            context=_b2c_customer_form_context(title="Edit B2C Customer", customer=customer, error=str(exc)),
        )


@app.post("/sales/b2c-customers/initialize-from-mappings")
def initialize_b2c_customers_route(db: Session = Depends(get_db)) -> Response:
    try:
        result = initialize_b2c_customers_from_mappings(db)
        message = (
            f"B2C customers initialized from mappings. Created: {result.created}, skipped: {result.skipped}."
        )
        return _redirect(f"/sales/b2c-customers?message={quote(message)}")
    except B2CCustomerValidationError as exc:
        db.rollback()
        return _redirect(f"/sales/b2c-customers?error={quote(str(exc))}")


@app.get("/planning", response_class=HTMLResponse)
def planning_home(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="planning_home.html",
        context={"title": "Planning"},
    )


@app.get("/inventory/balances", response_class=HTMLResponse)
def inventory_balances_report(
    request: Request,
    q: str = Query(default=""),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    search_text = q.strip()
    balances_query = db.query(InventoryBalance).options(joinedload(InventoryBalance.product))
    if search_text:
        pattern = f"%{search_text}%"
        balances_query = balances_query.join(InventoryBalance.product).filter(
            or_(
                Product.sku.ilike(pattern),
                Product.name.ilike(pattern),
                Product.description.ilike(pattern),
            )
        )
    balances = (
        balances_query
        .order_by(InventoryBalance.last_transaction_at.desc(), InventoryBalance.id.desc())
        .all()
    )
    return templates.TemplateResponse(
        request=request,
        name="inventory_balances.html",
        context={
            "title": "Inventory Balances",
            "balances": balances,
            "q": search_text,
            "opening_balances_initialized": _inventory_opening_balance_exists(db),
        },
    )


@app.get("/inventory/transactions", response_class=HTMLResponse)
def inventory_transactions_report(
    request: Request,
    date_from: str = Query(default=""),
    date_to: str = Query(default=""),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    parsed_date_from = _parse_optional_date_query(date_from)
    parsed_date_to = _parse_optional_date_query(date_to)

    transactions_query = db.query(InventoryTransaction).options(joinedload(InventoryTransaction.product))
    if parsed_date_from is not None:
        transactions_query = transactions_query.filter(
            InventoryTransaction.transaction_date >= datetime.combine(parsed_date_from, datetime.min.time())
        )
    if parsed_date_to is not None:
        transactions_query = transactions_query.filter(
            InventoryTransaction.transaction_date < datetime.combine(parsed_date_to + timedelta(days=1), datetime.min.time())
        )

    transactions = (
        transactions_query
        .order_by(InventoryTransaction.transaction_date.desc(), InventoryTransaction.id.desc())
        .all()
    )
    return templates.TemplateResponse(
        request=request,
        name="inventory_transactions.html",
        context={
            "title": "Inventory Transactions",
            "transactions": transactions,
            "date_from": date_from.strip(),
            "date_to": date_to.strip(),
            "opening_balances_initialized": _inventory_opening_balance_exists(db),
        },
    )


@app.get("/inventory/adjustments", response_class=HTMLResponse)
def inventory_adjustments_report(
    request: Request,
    date_from: str = Query(default=""),
    date_to: str = Query(default=""),
    adjustment_type: str = Query(default=""),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    parsed_date_from = _parse_optional_date_query(date_from)
    parsed_date_to = _parse_optional_date_query(date_to)
    selected_adjustment_type = adjustment_type.strip()
    adjustment_type_options = [
        value
        for (value,) in db.query(InventoryAdjustment.adjustment_type)
        .distinct()
        .order_by(InventoryAdjustment.adjustment_type)
        .all()
        if value
    ]
    if selected_adjustment_type and selected_adjustment_type not in adjustment_type_options:
        adjustment_type_options.append(selected_adjustment_type)
        adjustment_type_options.sort()

    adjustments_query = db.query(InventoryAdjustment).options(
        joinedload(InventoryAdjustment.product),
        joinedload(InventoryAdjustment.inventory_transaction),
    )
    if parsed_date_from is not None:
        adjustments_query = adjustments_query.filter(InventoryAdjustment.adjustment_date >= parsed_date_from)
    if parsed_date_to is not None:
        adjustments_query = adjustments_query.filter(InventoryAdjustment.adjustment_date <= parsed_date_to)
    if selected_adjustment_type:
        adjustments_query = adjustments_query.filter(InventoryAdjustment.adjustment_type == selected_adjustment_type)

    adjustments = (
        adjustments_query
        .order_by(InventoryAdjustment.adjustment_date.desc(), InventoryAdjustment.id.desc())
        .all()
    )
    return templates.TemplateResponse(
        request=request,
        name="inventory_adjustments_list.html",
        context={
            "title": "Inventory Adjustments",
            "adjustments": adjustments,
            "date_from": date_from.strip(),
            "date_to": date_to.strip(),
            "adjustment_type": selected_adjustment_type,
            "adjustment_type_options": adjustment_type_options,
            "opening_balances_initialized": _inventory_opening_balance_exists(db),
        },
    )


@app.get("/inventory/adjustments/new", response_class=HTMLResponse)
def new_inventory_adjustment(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    product_options = _inventory_adjustment_product_options(db)
    post_token = create_inventory_adjustment_post_token(db)
    return templates.TemplateResponse(
        request=request,
        name="inventory_adjustment_form.html",
        context=_inventory_adjustment_form_context(
            title="New Inventory Adjustment",
            form_action="/inventory/adjustments",
            product_options=product_options,
            post_token=post_token.token,
        ),
    )


@app.post("/inventory/adjustments")
async def create_inventory_adjustment_route(
    request: Request,
    db: Session = Depends(get_db),
) -> Response:
    form = await request.form()
    form_data = {
        "adjustment_date": str(form.get("adjustment_date", "")),
        "product_id": str(form.get("product_id", "")),
        "adjustment_mode": str(form.get("adjustment_mode", "")),
        "adjustment_type": str(form.get("adjustment_type", "")),
        "quantity": str(form.get("quantity", "")),
        "counted_qty": str(form.get("counted_qty", "")),
        "unit_cost": str(form.get("unit_cost", "")),
        "reason": str(form.get("reason", "")),
        "notes": str(form.get("notes", "")),
    }
    post_token = str(form.get("post_token", ""))
    try:
        adjustment_date = datetime.strptime(form_data["adjustment_date"], "%Y-%m-%d").date()
        product_id_text = form_data["product_id"].strip()
        if not product_id_text.isdigit():
            raise InventoryAdjustmentValidationError("Product is required.")
        adjustment = create_inventory_adjustment_with_posting(
            db,
            post_token=post_token,
            adjustment_date=adjustment_date,
            product_id=int(product_id_text),
            adjustment_mode=form_data["adjustment_mode"],
            adjustment_type=form_data["adjustment_type"],
            quantity=form_data["quantity"],
            counted_qty=form_data["counted_qty"],
            unit_cost=form_data["unit_cost"],
            reason=form_data["reason"],
            notes=form_data["notes"],
        )
        return _redirect(f"/inventory/adjustments/{adjustment.id}")
    except (InventoryAdjustmentValidationError, ValueError) as exc:
        db.rollback()
        retry_token = create_inventory_adjustment_post_token(db)
        product_options = _inventory_adjustment_product_options(db)
        return templates.TemplateResponse(
            request=request,
            name="inventory_adjustment_form.html",
            context=_inventory_adjustment_form_context(
                title="New Inventory Adjustment",
                form_action="/inventory/adjustments",
                product_options=product_options,
                post_token=retry_token.token,
                error=str(exc),
                form_data=form_data,
            ),
        )


@app.get("/inventory/adjustments/{adjustment_id}", response_class=HTMLResponse)
def inventory_adjustment_detail(
    adjustment_id: int,
    request: Request,
    db: Session = Depends(get_db),
) -> HTMLResponse:
    adjustment = (
        db.query(InventoryAdjustment)
        .options(
            joinedload(InventoryAdjustment.product),
            joinedload(InventoryAdjustment.inventory_transaction),
        )
        .filter(InventoryAdjustment.id == adjustment_id)
        .one()
    )
    return templates.TemplateResponse(
        request=request,
        name="inventory_adjustment_detail.html",
        context={
            "title": "Inventory Adjustment",
            "adjustment": adjustment,
            "transaction": adjustment.inventory_transaction,
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
    red_zone_inputs: dict[int, str] = {}
    yellow_zone_inputs: dict[int, str] = {}
    for key, value in form.items():
        key_text = str(key)
        if key_text.startswith("moq_"):
            product_id = int(key_text.replace("moq_", "", 1))
            moq_inputs[product_id] = str(value)
        elif key_text.startswith("red_zone_"):
            product_id = int(key_text.replace("red_zone_", "", 1))
            red_zone_inputs[product_id] = str(value)
        elif key_text.startswith("yellow_zone_"):
            product_id = int(key_text.replace("yellow_zone_", "", 1))
            yellow_zone_inputs[product_id] = str(value)
    try:
        update_product_inventory_parameters(db, moq_inputs, red_zone_inputs, yellow_zone_inputs)
        query = f"product_type={quote(product_type)}&message={quote('Planning parameters saved.')}"
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


@app.get("/planning/purchase-orders/import", response_class=HTMLResponse)
def purchase_orders_import_form(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="purchase_order_import_form.html",
        context={"title": "Import Historical Purchase Orders", "error": None, "result": None},
    )


@app.get("/planning/purchase-orders/import/template")
def download_purchase_orders_import_template() -> Response:
    return _csv_attachment_response(
        filename="purchase_orders_historical_import_template.csv",
        headers=PURCHASE_ORDER_HISTORICAL_IMPORT_HEADERS,
        example_row=(
            "PO-HIST-001",
            "2025-01-15",
            "Proveedor Ejemplo",
            "SKU001",
            "Producto ejemplo",
            "10",
            "10",
            "1500",
            "15000",
            "Historical purchase order example",
        ),
    )


@app.post("/planning/purchase-orders/import", response_class=HTMLResponse)
async def import_purchase_orders(request: Request, file: UploadFile = File(...), db: Session = Depends(get_db)) -> HTMLResponse:
    result: PurchaseOrderHistoricalImportResult | None = None
    error = None
    try:
        result = import_historical_purchase_orders_csv(
            db,
            file_name=file.filename or "historical_purchase_orders.csv",
            file_bytes=await file.read(),
        )
    except PurchaseOrderHistoricalImportValidationError as exc:
        db.rollback()
        error = str(exc)
    return templates.TemplateResponse(
        request=request,
        name="purchase_order_import_form.html",
        context={"title": "Import Historical Purchase Orders", "error": error, "result": result},
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
def b2b_customers(
    request: Request,
    q: str = Query(""),
    active: str = Query("all"),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    filters = {"q": q.strip(), "active": (active.strip().lower() or "all")}
    customer_query = db.query(B2BCustomer)
    if filters["q"]:
        term = f"%{filters['q']}%"
        customer_query = customer_query.filter(
            or_(
                B2BCustomer.customer_name.ilike(term),
                B2BCustomer.legal_name.ilike(term),
                B2BCustomer.legal_id.ilike(term),
                B2BCustomer.phone.ilike(term),
            )
        )
    if filters["active"] == "active":
        customer_query = customer_query.filter(B2BCustomer.active.is_(True))
    elif filters["active"] == "inactive":
        customer_query = customer_query.filter(B2BCustomer.active.is_(False))
    customers = customer_query.order_by(B2BCustomer.customer_name, B2BCustomer.id).all()
    return templates.TemplateResponse(
        request=request,
        name="b2b_customers_list.html",
        context={
            "title": "B2B Customers",
            "customers": customers,
            "filters": filters,
            "result_count": len(customers),
        },
    )


@app.get("/b2b/customers/import", response_class=HTMLResponse)
def import_b2b_customers_form(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="b2b_customer_import_form.html",
        context={
            "title": "Import B2B Customers CSV",
            "result": None,
            "error": None,
        },
    )


@app.post("/b2b/customers/import")
async def import_b2b_customers_route(
    request: Request,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    result: B2BCustomerImportResult | None = None
    error: str | None = None
    try:
        file_bytes = await file.read()
        result = import_b2b_customers_csv(
            db,
            file_name=file.filename or "b2b_customers.csv",
            file_bytes=file_bytes,
        )
    except (B2BCustomerImportValidationError, UnicodeDecodeError) as exc:
        db.rollback()
        error = str(exc)
    return templates.TemplateResponse(
        request=request,
        name="b2b_customer_import_form.html",
        context={
            "title": "Import B2B Customers CSV",
            "result": result,
            "error": error,
        },
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


@app.get("/b2b/orders/import", response_class=HTMLResponse)
def b2b_orders_import_form(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="b2b_order_import_form.html",
        context={"title": "Import Historical B2B Sales Orders", "error": None, "result": None},
    )


@app.get("/b2b/orders/import/template")
def download_b2b_orders_import_template() -> Response:
    return _csv_attachment_response(
        filename="b2b_historical_sales_import_template.csv",
        headers=B2B_HISTORICAL_IMPORT_HEADERS,
        example_row=(
            "B2B-HIST-001",
            "2025-01-15",
            "Cliente Ejemplo",
            "Wholesale",
            "SKU001",
            "Producto ejemplo",
            "10",
            "2500",
            "25000",
            "1200",
            "12000",
            "13000",
            "0.52",
            "Historical order example",
        ),
    )


@app.post("/b2b/orders/import", response_class=HTMLResponse)
async def import_b2b_orders(request: Request, file: UploadFile = File(...), db: Session = Depends(get_db)) -> HTMLResponse:
    result: B2BHistoricalSalesImportResult | None = None
    error = None
    try:
        result = import_b2b_historical_sales_csv(
            db,
            file_name=file.filename or "historical_b2b_sales.csv",
            file_bytes=await file.read(),
        )
    except B2BHistoricalSalesImportValidationError as exc:
        db.rollback()
        error = str(exc)
    return templates.TemplateResponse(
        request=request,
        name="b2b_order_import_form.html",
        context={"title": "Import Historical B2B Sales Orders", "error": error, "result": result},
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
    channels = list_channel_options(db, applies_to="b2b")
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
            "channels": channels,
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
        channels = list_channel_options(db, applies_to="b2b")
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
                "channels": channels,
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
    channels = list_channel_options(db, order.channel_id, applies_to="b2b")
    return templates.TemplateResponse(
        request=request,
        name="b2b_order_edit.html",
        context={
            "title": "Edit B2B Order",
            "order": order,
            "lines": lines,
            "catalog": catalog,
            "catalog_by_sku": catalog_by_sku,
            "channels": channels,
            "selected_channel_id": _default_channel_selection(
                channels,
                current_channel_id=order.channel_id,
                snapshot_name=order.b2b_channel_name_snapshot,
            ),
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
        channels = list_channel_options(db, order.channel_id, applies_to="b2b")
        return templates.TemplateResponse(
            request=request,
            name="b2b_order_edit.html",
            context={
                "title": "Edit B2B Order",
                "order": order,
                "lines": lines,
                "catalog": catalog,
                "catalog_by_sku": catalog_by_sku,
                "channels": channels,
                "selected_channel_id": _default_channel_selection(
                    channels,
                    current_channel_id=order.channel_id,
                    snapshot_name=order.b2b_channel_name_snapshot,
                ),
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


@app.get("/b2c/orders", response_class=HTMLResponse)
def b2c_orders(
    request: Request,
    q: str = Query(""),
    status: str = Query(""),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    filters = {"q": q.strip(), "status": status.strip()}
    order_query = db.query(B2CSalesOrder)
    if filters["q"]:
        term = f"%{filters['q']}%"
        order_query = order_query.filter(
            or_(
                B2CSalesOrder.customer_name.ilike(term),
                B2CSalesOrder.order_number.ilike(term),
            )
        )
    if filters["status"] in _b2c_statuses():
        order_query = order_query.filter(B2CSalesOrder.status == filters["status"])
    orders = order_query.order_by(B2CSalesOrder.created_at.desc(), B2CSalesOrder.id.desc()).all()
    return templates.TemplateResponse(
        request=request,
        name="b2c_orders_list.html",
        context={
            "title": "B2C Sales Orders",
            "orders": orders,
            "statuses": _b2c_statuses(),
            "filters": filters,
            "result_count": len(orders),
        },
    )


@app.get("/b2c/orders/import", response_class=HTMLResponse)
def b2c_orders_import_form(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="b2c_order_import_form.html",
        context={"title": "Import Historical B2C Sales Orders", "error": None, "result": None},
    )


@app.get("/b2c/orders/import/template")
def download_b2c_orders_import_template() -> Response:
    return _csv_attachment_response(
        filename="b2c_historical_sales_import_template.csv",
        headers=B2C_HISTORICAL_IMPORT_HEADERS,
        example_row=(
            "B2C-HIST-001",
            "2025-01-15",
            "Cliente Ejemplo",
            "88888888",
            "cliente@example.com",
            "Website",
            "SKU001",
            "Producto ejemplo",
            "2",
            "5000",
            "10000",
            "1000",
            "9000",
            "10000",
            "1000",
            "9000",
            "2500",
            "5000",
            "4000",
            "0.4444",
            "Historical order example",
        ),
    )


@app.post("/b2c/orders/import", response_class=HTMLResponse)
async def import_b2c_orders(request: Request, file: UploadFile = File(...), db: Session = Depends(get_db)) -> HTMLResponse:
    result: B2CHistoricalSalesImportResult | None = None
    error = None
    try:
        result = import_b2c_historical_sales_csv(
            db,
            file_name=file.filename or "historical_b2c_sales.csv",
            file_bytes=await file.read(),
        )
    except B2CHistoricalSalesImportValidationError as exc:
        db.rollback()
        error = str(exc)
    return templates.TemplateResponse(
        request=request,
        name="b2c_order_import_form.html",
        context={"title": "Import Historical B2C Sales Orders", "error": error, "result": result},
    )


@app.get("/b2c/orders/new", response_class=HTMLResponse)
def new_b2c_order(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    products = _list_b2c_sellable_products(db)
    discount_rules = list_discount_rule_options(db)
    b2c_customers = list_b2c_customer_options(db)
    channels = list_channel_options(db, applies_to="b2c")
    return templates.TemplateResponse(
        request=request,
        name="b2c_order_form.html",
        context={
            "title": "New B2C Order",
            "products": products,
            "product_skus": {product.sku for product in products},
            "discount_rules": discount_rules,
            "discount_rule_ids": {rule.id for rule in discount_rules},
            "b2c_customers": b2c_customers,
            "channels": channels,
            "error": None,
            "default_order_date": date.today().isoformat(),
        },
    )


@app.post("/b2c/orders")
async def create_b2c_order(request: Request, db: Session = Depends(get_db)) -> Response:
    form = await request.form()
    order_date_text = str(form.get("order_date", "")).strip()
    line_inputs = _b2c_line_inputs_from_form(form, "line")
    b2c_customer_id = str(form.get("b2c_customer_id", ""))
    customer_name = str(form.get("customer_name", ""))
    customer_phone = str(form.get("customer_phone", ""))
    customer_email = str(form.get("customer_email", ""))
    customer_address_snapshot = str(form.get("customer_address_snapshot", ""))
    province_snapshot = str(form.get("province_snapshot", ""))
    canton_snapshot = str(form.get("canton_snapshot", ""))
    district_snapshot = str(form.get("district_snapshot", ""))
    customer_observations_snapshot = str(form.get("customer_observations_snapshot", ""))
    channel_id = str(form.get("channel_id", ""))
    discount_rule_id = str(form.get("discount_rule_id", ""))
    observations = str(form.get("observations", ""))
    try:
        order_date = datetime.strptime(order_date_text, "%Y-%m-%d").date()
        order = create_b2c_sales_order(
            db,
            order_date=order_date,
            b2c_customer_id=b2c_customer_id,
            customer_name=customer_name,
            customer_phone=customer_phone,
            customer_email=customer_email,
            customer_address_snapshot=customer_address_snapshot,
            province_snapshot=province_snapshot,
            canton_snapshot=canton_snapshot,
            district_snapshot=district_snapshot,
            customer_observations_snapshot=customer_observations_snapshot,
            channel_id=channel_id,
            discount_rule_id=discount_rule_id,
            observations=observations,
            line_inputs=line_inputs,
        )
        return _redirect(f"/b2c/orders/{order.id}")
    except (B2CValidationError, ValueError) as exc:
        db.rollback()
        products = _list_b2c_sellable_products(db)
        current_discount_rule_id = int(discount_rule_id) if discount_rule_id.isdigit() else None
        discount_rules = list_discount_rule_options(db, current_discount_rule_id)
        current_b2c_customer_id = int(b2c_customer_id) if b2c_customer_id.isdigit() else None
        b2c_customers = list_b2c_customer_options(db, current_b2c_customer_id)
        channels = list_channel_options(db, applies_to="b2c")
        return templates.TemplateResponse(
            request=request,
            name="b2c_order_form.html",
            context={
                "title": "New B2C Order",
                "products": products,
                "product_skus": {product.sku for product in products},
                "discount_rules": discount_rules,
                "discount_rule_ids": {rule.id for rule in discount_rules},
                "b2c_customers": b2c_customers,
                "channels": channels,
                "error": str(exc),
                "default_order_date": order_date_text or date.today().isoformat(),
                "submitted_b2c_customer_id": b2c_customer_id,
                "submitted_customer_name": customer_name,
                "submitted_customer_phone": customer_phone,
                "submitted_customer_email": customer_email,
                "submitted_customer_address_snapshot": customer_address_snapshot,
                "submitted_province_snapshot": province_snapshot,
                "submitted_canton_snapshot": canton_snapshot,
                "submitted_district_snapshot": district_snapshot,
                "submitted_customer_observations_snapshot": customer_observations_snapshot,
                "submitted_channel_id": channel_id,
                "submitted_discount_rule_id": discount_rule_id,
                "submitted_observations": observations,
                "submitted_lines": line_inputs,
            },
        )


@app.get("/b2c/orders/{order_id}", response_class=HTMLResponse)
def b2c_order_detail(order_id: int, request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    order = db.query(B2CSalesOrder).options(joinedload(B2CSalesOrder.customer)).filter(B2CSalesOrder.id == order_id).one()
    lines = (
        db.query(B2CSalesOrderLine)
        .filter(B2CSalesOrderLine.sales_order_id == order_id)
        .order_by(B2CSalesOrderLine.line_number)
        .all()
    )
    return templates.TemplateResponse(
        request=request,
        name="b2c_order_detail.html",
        context={"title": "B2C Order Detail", "order": order, "lines": lines, "error": None},
    )


@app.get("/b2c/orders/{order_id}/edit", response_class=HTMLResponse)
def edit_b2c_order(order_id: int, request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    order = db.query(B2CSalesOrder).options(joinedload(B2CSalesOrder.customer)).filter(B2CSalesOrder.id == order_id).one()
    if order.status != "draft":
        return b2c_order_detail(order_id, request, db)
    lines = (
        db.query(B2CSalesOrderLine)
        .filter(B2CSalesOrderLine.sales_order_id == order_id)
        .order_by(B2CSalesOrderLine.line_number)
        .all()
    )
    products = _list_b2c_sellable_products(db)
    discount_rules = list_discount_rule_options(db, order.discount_rule_id)
    b2c_customers = list_b2c_customer_options(db, order.b2c_customer_id)
    channels = list_channel_options(db, order.channel_id, applies_to="b2c")
    return templates.TemplateResponse(
        request=request,
        name="b2c_order_edit.html",
        context={
            "title": "Edit B2C Order",
            "order": order,
            "lines": lines,
            "products": products,
            "product_skus": {product.sku for product in products},
            "discount_rules": discount_rules,
            "discount_rule_ids": {rule.id for rule in discount_rules},
            "b2c_customers": b2c_customers,
            "channels": channels,
            "selected_channel_id": _default_channel_selection(
                channels,
                current_channel_id=order.channel_id,
                snapshot_name=order.channel,
            ),
            "error": None,
        },
    )


@app.post("/b2c/orders/{order_id}/edit")
async def update_b2c_order(order_id: int, request: Request, db: Session = Depends(get_db)) -> Response:
    form = await request.form()
    order_date_text = str(form.get("order_date", "")).strip()
    b2c_customer_id = str(form.get("b2c_customer_id", ""))
    customer_name = str(form.get("customer_name", ""))
    customer_phone = str(form.get("customer_phone", ""))
    customer_email = str(form.get("customer_email", ""))
    customer_address_snapshot = str(form.get("customer_address_snapshot", ""))
    province_snapshot = str(form.get("province_snapshot", ""))
    canton_snapshot = str(form.get("canton_snapshot", ""))
    district_snapshot = str(form.get("district_snapshot", ""))
    customer_observations_snapshot = str(form.get("customer_observations_snapshot", ""))
    channel_id = str(form.get("channel_id", ""))
    discount_rule_id = str(form.get("discount_rule_id", ""))
    observations = str(form.get("observations", ""))
    line_ids = form.getlist("line_id")
    line_updates = [
        {
            "id": line_id,
            "sku": str(form.get(f"line_sku_{line_id}", "")),
            "quantity": str(form.get(f"line_quantity_{line_id}", "")),
            "unit_price": str(form.get(f"line_unit_price_{line_id}", "")),
        }
        for line_id in line_ids
    ]
    deleted_line_ids = [int(line_id) for line_id in form.getlist("delete_line_id")]
    new_line_inputs = _b2c_line_inputs_from_form(form, "new_line", 3)
    try:
        order_date = datetime.strptime(order_date_text, "%Y-%m-%d").date()
        update_b2c_sales_order(
            db,
            order_id=order_id,
            order_date=order_date,
            b2c_customer_id=b2c_customer_id,
            customer_name=customer_name,
            customer_phone=customer_phone,
            customer_email=customer_email,
            customer_address_snapshot=customer_address_snapshot,
            province_snapshot=province_snapshot,
            canton_snapshot=canton_snapshot,
            district_snapshot=district_snapshot,
            customer_observations_snapshot=customer_observations_snapshot,
            channel_id=channel_id,
            discount_rule_id=discount_rule_id,
            observations=observations,
            line_updates=line_updates,
            deleted_line_ids=deleted_line_ids,
            new_line_inputs=new_line_inputs,
        )
        return _redirect(f"/b2c/orders/{order_id}")
    except (B2CValidationError, ValueError) as exc:
        db.rollback()
        order = db.query(B2CSalesOrder).options(joinedload(B2CSalesOrder.customer)).filter(B2CSalesOrder.id == order_id).one()
        lines = (
            db.query(B2CSalesOrderLine)
            .filter(B2CSalesOrderLine.sales_order_id == order_id)
            .order_by(B2CSalesOrderLine.line_number)
            .all()
        )
        submitted_line_updates = {int(update["id"]): update for update in line_updates if str(update["id"]).strip()}
        products = _list_b2c_sellable_products(db)
        current_discount_rule_id = int(discount_rule_id) if discount_rule_id.isdigit() else order.discount_rule_id
        discount_rules = list_discount_rule_options(db, current_discount_rule_id)
        current_b2c_customer_id = int(b2c_customer_id) if b2c_customer_id.isdigit() else order.b2c_customer_id
        b2c_customers = list_b2c_customer_options(db, current_b2c_customer_id)
        channels = list_channel_options(db, order.channel_id, applies_to="b2c")
        return templates.TemplateResponse(
            request=request,
            name="b2c_order_edit.html",
            context={
                "title": "Edit B2C Order",
                "order": order,
                "lines": lines,
                "products": products,
                "product_skus": {product.sku for product in products},
                "discount_rules": discount_rules,
                "discount_rule_ids": {rule.id for rule in discount_rules},
                "b2c_customers": b2c_customers,
                "channels": channels,
                "selected_channel_id": _default_channel_selection(
                    channels,
                    current_channel_id=order.channel_id,
                    snapshot_name=order.channel,
                ),
                "error": str(exc),
                "submitted_order_date": order_date_text,
                "submitted_b2c_customer_id": b2c_customer_id,
                "submitted_customer_name": customer_name,
                "submitted_customer_phone": customer_phone,
                "submitted_customer_email": customer_email,
                "submitted_customer_address_snapshot": customer_address_snapshot,
                "submitted_province_snapshot": province_snapshot,
                "submitted_canton_snapshot": canton_snapshot,
                "submitted_district_snapshot": district_snapshot,
                "submitted_customer_observations_snapshot": customer_observations_snapshot,
                "submitted_channel_id": channel_id,
                "submitted_discount_rule_id": discount_rule_id,
                "submitted_observations": observations,
                "submitted_line_updates": submitted_line_updates,
                "submitted_new_lines": new_line_inputs,
            },
        )


@app.post("/b2c/orders/{order_id}/status")
def update_b2c_order_status(
    request: Request,
    order_id: int,
    status: str = Form(...),
    db: Session = Depends(get_db),
) -> Response:
    try:
        if status == "invoiced":
            invoice_b2c_order_in_erp(db, order_id)
        else:
            change_b2c_sales_order_status(db, order_id, status)
        return _redirect(f"/b2c/orders/{order_id}")
    except B2CValidationError as exc:
        db.rollback()
        order = db.query(B2CSalesOrder).filter(B2CSalesOrder.id == order_id).one()
        lines = (
            db.query(B2CSalesOrderLine)
            .filter(B2CSalesOrderLine.sales_order_id == order_id)
            .order_by(B2CSalesOrderLine.line_number)
            .all()
        )
        return templates.TemplateResponse(
            request=request,
            name="b2c_order_detail.html",
            context={"title": "B2C Order Detail", "order": order, "lines": lines, "error": str(exc)},
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
                "product_master_upsert_count": batch.product_master_upsert_count,
                "bom_parent_count": len(headers),
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
            "product_master_upsert_count": batch.product_master_upsert_count,
            "bom_parent_count": len(headers),
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


@app.get("/production-orders/import", response_class=HTMLResponse)
def production_orders_import_form(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="production_order_import_form.html",
        context={"title": "Import Historical Production Orders", "error": None, "result": None},
    )


@app.get("/production-orders/import/template")
def download_production_orders_import_template() -> Response:
    return _csv_attachment_response(
        filename="production_orders_historical_import_template.csv",
        headers=PRODUCTION_ORDER_HISTORICAL_IMPORT_HEADERS,
        example_row=(
            "PROD-HIST-001",
            "2025-01-15",
            "SKU001",
            "Mezclado",
            "100",
            "105",
            "98",
            "unit",
            "93.33",
            "25000",
            "5000",
            "2000",
            "3000",
            "35000",
            "Historical production order example",
            "2025-01-15",
        ),
    )


@app.post("/production-orders/import", response_class=HTMLResponse)
async def import_production_orders(request: Request, file: UploadFile = File(...), db: Session = Depends(get_db)) -> HTMLResponse:
    result: ProductionOrderHistoricalImportResult | None = None
    error = None
    try:
        result = import_historical_production_orders_csv(
            db,
            file_name=file.filename or "historical_production_orders.csv",
            file_bytes=await file.read(),
        )
    except ProductionOrderHistoricalImportValidationError as exc:
        db.rollback()
        error = str(exc)

    return templates.TemplateResponse(
        request=request,
        name="production_order_import_form.html",
        context={
            "title": "Import Historical Production Orders",
            "error": error,
            "result": result,
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
