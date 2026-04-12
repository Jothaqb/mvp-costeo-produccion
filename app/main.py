from collections import Counter
from datetime import date, datetime

from fastapi import Depends, FastAPI, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session, joinedload

from app.database import (
    Base,
    engine,
    ensure_product_default_route_column,
    ensure_product_is_manufactured_column,
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
    Route,
    RouteActivity,
)
from app.schemas import ComponentType, ProcessType
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


Base.metadata.create_all(bind=engine)
ensure_product_default_route_column()
ensure_product_is_manufactured_column()

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
    db: Session = Depends(get_db),
) -> HTMLResponse:
    batch = db.query(ImportBatch).filter(ImportBatch.id == batch_id).one()
    headers = (
        db.query(ImportedBomHeader)
        .filter(ImportedBomHeader.import_batch_id == batch_id)
        .order_by(ImportedBomHeader.product_sku)
        .all()
    )
    lines = (
        db.query(ImportedBomLine)
        .join(ImportedBomHeader)
        .options(joinedload(ImportedBomLine.bom_header))
        .filter(ImportedBomHeader.import_batch_id == batch_id)
        .order_by(ImportedBomLine.source_row_number)
        .all()
    )
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
def product_routes(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    products = db.query(Product).filter(Product.is_manufactured.is_(True)).order_by(Product.sku).all()
    routes = db.query(Route).filter(Route.active.is_(True)).order_by(Route.code).all()
    return templates.TemplateResponse(
        request=request,
        name="product_routes.html",
        context={"title": "Product Routes", "products": products, "routes": routes},
    )


@app.post("/product-routes/{product_id}")
def update_product_route(
    product_id: int,
    default_route_id: str = Form(""),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    product = db.query(Product).filter(Product.id == product_id).one()
    product.default_route_id = int(default_route_id) if default_route_id else None
    db.commit()
    return _redirect("/product-routes")


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
