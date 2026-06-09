from __future__ import annotations

from datetime import date
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from sqlalchemy.orm import Session, joinedload

from app.models import AppSequence, PackagingBatch, PackagingBatchLine, Product, Route, User
from app.schemas import ProcessType
from app.services.packaging_batch_activity_service import (
    ensure_packaging_batch_activities,
    has_packaging_batch_captured_activity_times,
)
from app.services.packaging_batch_costing_service import invalidate_packaging_batch_line_cost_distribution
from app.services.packaging_batch_material_service import sync_packaging_batch_line_material_snapshot


class PackagingBatchValidationError(Exception):
    pass


@dataclass(frozen=True)
class PackagingBatchLineSaveResult:
    line: PackagingBatchLine
    auto_refresh_attempted: bool = False
    auto_refresh_error: str | None = None


PACKAGING_BATCH_SEQUENCE_NAME = "packaging_batch"
PACKAGING_BATCH_PREFIX = "PB"
JAR_ROUTE_CODES = {"R_ENV_FRASCOS"}
DOYPACK_ROUTE_CODES = {"R_ENV_DOYPACK"}
PACKAGING_TYPE_OPTIONS = (
    ("jar", "Frascos"),
    ("doypack", "Doypacks"),
)
EXPECTED_PACKAGING_ROUTE_CODES = {
    "jar": "R_ENV_FRASCOS",
    "doypack": "R_ENV_DOYPACK",
}


def list_packaging_routes(db: Session) -> list[Route]:
    allowed_codes = JAR_ROUTE_CODES | DOYPACK_ROUTE_CODES
    return (
        db.query(Route)
        .filter(
            Route.active.is_(True),
            Route.process_type == ProcessType.PACKAGING.value,
            Route.code.in_(allowed_codes),
        )
        .order_by(Route.code, Route.name, Route.version)
        .all()
    )


def list_packaging_batches(
    db: Session,
    *,
    status: str | None = None,
    packaging_type: str | None = None,
) -> list[PackagingBatch]:
    query = (
        db.query(PackagingBatch)
        .options(
            joinedload(PackagingBatch.created_by_user),
            joinedload(PackagingBatch.closed_by_user),
            joinedload(PackagingBatch.lines),
        )
        .order_by(PackagingBatch.production_date.desc(), PackagingBatch.id.desc())
    )
    normalized_status = (status or "").strip().lower()
    if normalized_status:
        query = query.filter(PackagingBatch.status == normalized_status)
    normalized_packaging_type = (packaging_type or "").strip().lower()
    if normalized_packaging_type:
        query = query.filter(PackagingBatch.packaging_type == normalized_packaging_type)
    return query.all()


def get_packaging_route_preview_map(db: Session) -> dict[str, dict[str, str | None]]:
    preview_map: dict[str, dict[str, str | None]] = {}
    for packaging_type, route_code in EXPECTED_PACKAGING_ROUTE_CODES.items():
        route = db.query(Route).filter(Route.code == route_code).one_or_none()
        if route is None:
            preview_map[packaging_type] = {
                "code": route_code,
                "label": None,
                "error": f"Expected route {route_code} does not exist.",
            }
            continue
        if not route.active:
            preview_map[packaging_type] = {
                "code": route_code,
                "label": f"{route.code} - {route.name} v{route.version}",
                "error": f"Expected route {route_code} is inactive.",
            }
            continue
        if route.process_type != ProcessType.PACKAGING.value:
            preview_map[packaging_type] = {
                "code": route_code,
                "label": f"{route.code} - {route.name} v{route.version}",
                "error": f"Expected route {route_code} must belong to the packaging process.",
            }
            continue
        preview_map[packaging_type] = {
            "code": route_code,
            "label": f"{route.code} - {route.name} v{route.version}",
            "error": None,
        }
    return preview_map


def get_packaging_batch(db: Session, batch_id: int) -> PackagingBatch:
    batch = (
        db.query(PackagingBatch)
        .options(
            joinedload(PackagingBatch.route),
            joinedload(PackagingBatch.created_by_user),
            joinedload(PackagingBatch.updated_by_user),
            joinedload(PackagingBatch.activities),
            joinedload(PackagingBatch.lines).joinedload(PackagingBatchLine.product),
            joinedload(PackagingBatch.lines).joinedload(PackagingBatchLine.materials),
        )
        .filter(PackagingBatch.id == batch_id)
        .one_or_none()
    )
    if batch is None:
        raise PackagingBatchValidationError("Packaging batch not found.")
    return batch


def create_packaging_batch(
    db: Session,
    *,
    production_date: date,
    packaging_type: str,
    notes: str | None,
    current_user: User | None,
) -> PackagingBatch:
    route = _resolve_packaging_route(db, packaging_type=packaging_type)
    batch = PackagingBatch(
        internal_batch_number=_generate_internal_batch_number(db),
        production_date=production_date,
        packaging_type=packaging_type,
        route_id=route.id,
        route_name_snapshot=route.name,
        route_version_snapshot=route.version,
        process_type=route.process_type,
        status="draft",
        notes=_normalize_optional_text(notes),
        created_by_user_id=current_user.id if current_user else None,
        updated_by_user_id=current_user.id if current_user else None,
    )
    db.add(batch)
    db.flush()
    ensure_packaging_batch_activities(db, batch)
    db.commit()
    db.refresh(batch)
    return get_packaging_batch(db, batch.id)


def update_packaging_batch_header(
    db: Session,
    *,
    batch_id: int,
    production_date: date,
    packaging_type: str,
    notes: str | None,
    current_user: User | None,
) -> PackagingBatch:
    batch = get_packaging_batch(db, batch_id)
    _ensure_draft(batch)
    route = _resolve_packaging_route(db, packaging_type=packaging_type)
    route_changed = batch.route_id != route.id
    if route_changed and has_packaging_batch_captured_activity_times(batch):
        raise PackagingBatchValidationError(
            "Route cannot be changed after Activity Time Capture has recorded minutes."
        )
    batch.production_date = production_date
    batch.packaging_type = packaging_type
    batch.route_id = route.id
    batch.route_name_snapshot = route.name
    batch.route_version_snapshot = route.version
    batch.process_type = route.process_type
    batch.notes = _normalize_optional_text(notes)
    batch.updated_by_user_id = current_user.id if current_user else batch.updated_by_user_id
    ensure_packaging_batch_activities(db, batch, replace_existing=route_changed)
    if route_changed:
        invalidate_packaging_batch_line_cost_distribution(db, batch)
    db.commit()
    db.refresh(batch)
    return get_packaging_batch(db, batch.id)


def add_packaging_batch_real_line(
    db: Session,
    *,
    batch_id: int,
    product_id: int,
    planned_qty: str | Decimal,
    notes: str | None,
) -> PackagingBatchLineSaveResult:
    batch = get_packaging_batch(db, batch_id)
    _ensure_draft(batch)
    product = (
        db.query(Product)
        .filter(Product.id == product_id, Product.active.is_(True))
        .one_or_none()
    )
    if product is None:
        raise PackagingBatchValidationError("Product is required.")
    if not product.is_manufactured:
        raise PackagingBatchValidationError("Product must be manufactured.")
    if any(line.product_id == product.id for line in batch.lines if line.product_id is not None):
        raise PackagingBatchValidationError("Product is already included in this batch.")

    line = PackagingBatchLine(
        packaging_batch_id=batch.id,
        line_number=_next_line_number(batch),
        product_id=product.id,
        is_manual_line=False,
        product_sku_snapshot=product.sku,
        product_name_snapshot=product.name,
        unit_snapshot=(product.unit or "").strip() or None,
        planned_qty=_parse_positive_decimal(planned_qty, "Planned quantity"),
        material_snapshot_cost_total=None,
        material_snapshot_status="pending",
        material_snapshot_refreshed_at=None,
        notes=_normalize_optional_text(notes),
    )
    db.add(line)
    invalidate_packaging_batch_line_cost_distribution(db, batch)
    auto_refresh_error = sync_packaging_batch_line_material_snapshot(db, line=line).error
    db.commit()
    db.refresh(line)
    return PackagingBatchLineSaveResult(
        line=line,
        auto_refresh_attempted=True,
        auto_refresh_error=auto_refresh_error,
    )


def update_packaging_batch_line(
    db: Session,
    *,
    batch_id: int,
    line_id: int,
    planned_qty: str | Decimal,
    notes: str | None,
) -> PackagingBatchLineSaveResult:
    batch = get_packaging_batch(db, batch_id)
    _ensure_draft(batch)
    line = _get_batch_line(batch, line_id)
    original_planned_qty = line.planned_qty
    line.planned_qty = _parse_positive_decimal(planned_qty, "Planned quantity")
    line.notes = _normalize_optional_text(notes)
    auto_refresh_error = None
    if line.planned_qty != original_planned_qty:
        auto_refresh_error = sync_packaging_batch_line_material_snapshot(db, line=line).error
    db.commit()
    db.refresh(line)
    return PackagingBatchLineSaveResult(
        line=line,
        auto_refresh_attempted=line.planned_qty != original_planned_qty,
        auto_refresh_error=auto_refresh_error,
    )


def delete_packaging_batch_line(db: Session, *, batch_id: int, line_id: int) -> None:
    batch = get_packaging_batch(db, batch_id)
    _ensure_draft(batch)
    line = _get_batch_line(batch, line_id)
    invalidate_packaging_batch_line_cost_distribution(db, batch)
    db.delete(line)
    db.commit()


def packaging_type_label(value: str) -> str:
    for code, label in PACKAGING_TYPE_OPTIONS:
        if code == value:
            return label
    return value


def _normalize_packaging_type(packaging_type: str) -> str:
    normalized_packaging_type = (packaging_type or "").strip().lower()
    if normalized_packaging_type not in {value for value, _ in PACKAGING_TYPE_OPTIONS}:
        raise PackagingBatchValidationError("Packaging type is required.")
    return normalized_packaging_type


def _resolve_packaging_route(db: Session, *, packaging_type: str) -> Route:
    normalized_packaging_type = _normalize_packaging_type(packaging_type)
    route_code = EXPECTED_PACKAGING_ROUTE_CODES[normalized_packaging_type]
    route = db.query(Route).filter(Route.code == route_code).one_or_none()
    if route is None:
        raise PackagingBatchValidationError(f"Expected route {route_code} does not exist.")
    if not route.active:
        raise PackagingBatchValidationError(f"Expected route {route_code} is inactive.")
    if route.process_type != ProcessType.PACKAGING.value:
        raise PackagingBatchValidationError(f"Expected route {route_code} must belong to the packaging process.")
    return route


def _generate_internal_batch_number(db: Session) -> str:
    sequence = db.query(AppSequence).filter(AppSequence.name == PACKAGING_BATCH_SEQUENCE_NAME).one_or_none()
    if sequence is None:
        sequence = AppSequence(
            name=PACKAGING_BATCH_SEQUENCE_NAME,
            next_value=_bootstrap_next_packaging_batch_sequence(db),
        )
        db.add(sequence)
        db.flush()

    batch_number = f"{PACKAGING_BATCH_PREFIX}{sequence.next_value}"
    exists = (
        db.query(PackagingBatch)
        .filter(PackagingBatch.internal_batch_number == batch_number)
        .one_or_none()
    )
    if exists is not None:
        raise PackagingBatchValidationError(f"Generated batch number {batch_number} already exists.")

    sequence.next_value += 1
    return batch_number


def _bootstrap_next_packaging_batch_sequence(db: Session) -> int:
    highest = 0
    prefix_length = len(PACKAGING_BATCH_PREFIX)
    rows = db.query(PackagingBatch.internal_batch_number).all()
    for (batch_number,) in rows:
        if not batch_number or not batch_number.startswith(PACKAGING_BATCH_PREFIX):
            continue
        suffix = batch_number[prefix_length:]
        if suffix.isdigit():
            highest = max(highest, int(suffix))
    return highest + 1 if highest else 1


def _next_line_number(batch: PackagingBatch) -> int:
    return max((line.line_number for line in batch.lines), default=0) + 1


def _get_batch_line(batch: PackagingBatch, line_id: int) -> PackagingBatchLine:
    for line in batch.lines:
        if line.id == line_id:
            return line
    raise PackagingBatchValidationError("Packaging batch line not found.")


def _ensure_draft(batch: PackagingBatch) -> None:
    if batch.status != "draft":
        raise PackagingBatchValidationError("Only draft packaging batches can be edited in this sprint.")


def _normalize_optional_text(value: str | None) -> str | None:
    normalized = (value or "").strip()
    return normalized or None


def _parse_positive_decimal(value: str | Decimal, field_name: str) -> Decimal:
    if isinstance(value, Decimal):
        parsed = value
    else:
        text = (value or "").strip().replace(" ", "").replace(",", ".")
        if not text:
            raise PackagingBatchValidationError(f"{field_name} is required.")
        try:
            parsed = Decimal(text)
        except InvalidOperation as exc:
            raise PackagingBatchValidationError(f"{field_name} must be a valid number.") from exc
    if parsed <= 0:
        raise PackagingBatchValidationError(f"{field_name} must be greater than 0.")
    return parsed
