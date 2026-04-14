from collections import Counter
from datetime import date, datetime
from urllib.parse import quote

from fastapi import Depends, FastAPI, File, Form, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session, joinedload

from app.database import (
    Base,
    engine,
    ensure_app_sequences_table,
    ensure_product_default_route_column,
    ensure_product_is_manufactured_column,
    ensure_product_loyverse_mapping_columns,
    ensure_sprint4_costing_columns,
    ensure_sprint5_comparison_columns,
    ensure_sprint6_loyverse_cost_sync_columns,
    get_db,
)
from app.models import (
    Activity,
    ImportBatch,
    ImportedBomHeader,
    ImportedBomLine,
    LaborRate,
    Machine,
    MachineRate,
    OverheadRate,
    Product,
    ProductionOrder,
    ProductionOrderActivity,
    ProductionOrderMaterial,
    Route,
    RouteActivity,
)
from app.schemas import ComponentType, ProcessType, ProductionOrderStatus
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
from app.services.loyverse_service import sync_closed_order_cost_to_loyverse
from app.services.production_order_service import (
    ProductionOrderValidationError,
    close_order,
    create_production_order,
    parse_optional_decimal,
    start_order,
    update_activity_capture,
    update_yield_capture,
)


Base.metadata.create_all(bind=engine)
ensure_product_default_route_column()
ensure_product_is_manufactured_column()
ensure_product_loyverse_mapping_columns()
ensure_app_sequences_table()
ensure_sprint4_costing_columns()
ensure_sprint5_comparison_columns()
ensure_sprint6_loyverse_cost_sync_columns()

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


@app.get("/product-routes", response_class=HTMLResponse)
def product_routes(
    request: Request,
    product_sku: str = Query(""),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    search_sku = product_sku.strip()
    product_query = db.query(Product).filter(Product.is_manufactured.is_(True))
    if search_sku:
        product_query = product_query.filter(Product.sku.ilike(f"%{search_sku}%"))
    products = product_query.order_by(Product.sku).all()
    routes = db.query(Route).filter(Route.active.is_(True)).order_by(Route.code).all()
    return templates.TemplateResponse(
        request=request,
        name="product_routes.html",
        context={"title": "Product Routes", "products": products, "routes": routes, "product_sku": search_sku},
    )


@app.post("/product-routes/{product_id}")
def update_product_route(
    product_id: int,
    default_route_id: str = Form(""),
    product_sku: str = Form(""),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    product = db.query(Product).filter(Product.id == product_id).one()
    product.default_route_id = int(default_route_id) if default_route_id else None
    db.commit()
    search_sku = product_sku.strip()
    if search_sku:
        return _redirect(f"/product-routes?product_sku={quote(search_sku)}")
    return _redirect("/product-routes")


@app.get("/production-orders", response_class=HTMLResponse)
def list_production_orders(
    request: Request,
    order_number: str = Query(""),
    product_sku: str = Query(""),
    product_name: str = Query(""),
    process_type: str = Query(""),
    status: str = Query(""),
    date_from: date | None = Query(None),
    date_to: date | None = Query(None),
    db: Session = Depends(get_db),
) -> HTMLResponse:
    filters = {
        "order_number": order_number.strip(),
        "product_sku": product_sku.strip(),
        "product_name": product_name.strip(),
        "process_type": process_type.strip(),
        "status": status.strip(),
        "date_from": date_from,
        "date_to": date_to,
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
    db: Session = Depends(get_db),
) -> HTMLResponse:
    search = q.strip()
    product_query = db.query(Product).filter(Product.is_manufactured.is_(True), Product.active.is_(True))
    if search:
        like_search = f"%{search}%"
        product_query = product_query.filter(Product.sku.ilike(like_search) | Product.name.ilike(like_search))
    products = product_query.order_by(Product.sku).limit(50).all()
    return templates.TemplateResponse(
        request=request,
        name="production_order_form.html",
        context={"title": "New Production Order", "products": products, "error": None, "search": search},
    )


@app.post("/production-orders")
def create_production_order_route(
    request: Request,
    production_date: date = Form(...),
    product_id: int = Form(...),
    planned_qty: str = Form(""),
    notes: str = Form(""),
    search: str = Form(""),
    db: Session = Depends(get_db),
) -> Response:
    product_query = db.query(Product).filter(Product.is_manufactured.is_(True), Product.active.is_(True))
    if search.strip():
        like_search = f"%{search.strip()}%"
        product_query = product_query.filter(Product.sku.ilike(like_search) | Product.name.ilike(like_search))
    products = product_query.order_by(Product.sku).limit(50).all()
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
                "products": products,
                "error": str(exc),
                "search": search.strip(),
            },
        )


@app.get("/production-orders/{order_id}", response_class=HTMLResponse)
def production_order_detail(order_id: int, request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    return _production_order_detail_response(order_id, request, db)


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
        order_before_close = db.query(ProductionOrder).filter(ProductionOrder.id == order_id).one()
        status_before_close = order_before_close.status
        close_order(db, order_id)
        order_after_close = db.query(ProductionOrder).filter(ProductionOrder.id == order_id).one()
        if status_before_close != "closed" and order_after_close.status == "closed":
            sync_closed_order_cost_to_loyverse(db, order_after_close.id)
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
