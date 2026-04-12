from collections import Counter

from fastapi import Depends, FastAPI, File, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session, joinedload

from app.database import Base, engine, get_db
from app.models import ImportBatch, ImportedBomHeader, ImportedBomLine
from app.schemas import ComponentType
from app.services.import_service import import_loyverse_csv


Base.metadata.create_all(bind=engine)

app = FastAPI(title="Real Production Costing MVP")
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")


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
