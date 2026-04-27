from datetime import datetime
from decimal import Decimal, InvalidOperation

from sqlalchemy.orm import Session, joinedload

from app.models import ImportBatch, ImportedBomHeader, ImportedBomLine, Product, ProductBomHeader, ProductBomLine


ZERO = Decimal("0")


class ProductBomValidationError(Exception):
    pass


def get_product_bom(db: Session, product_id: int) -> ProductBomHeader | None:
    return (
        db.query(ProductBomHeader)
        .options(
            joinedload(ProductBomHeader.product),
            joinedload(ProductBomHeader.lines).joinedload(ProductBomLine.component_product),
        )
        .filter(ProductBomHeader.product_id == product_id)
        .one_or_none()
    )


def get_or_seed_product_bom(db: Session, product: Product) -> ProductBomHeader | None:
    existing = get_product_bom(db, product.id)
    if existing is not None:
        return existing

    imported_header = get_latest_imported_bom_header(db, product.sku)
    if imported_header is None:
        return None

    header = ProductBomHeader(
        product_id=product.id,
        name=f"{product.sku} BOM",
        active=True,
        source_type="imported_bom",
        source_imported_bom_header_id=imported_header.id,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(header)
    db.flush()

    imported_lines = (
        db.query(ImportedBomLine)
        .filter(ImportedBomLine.bom_header_id == imported_header.id)
        .order_by(ImportedBomLine.source_row_number, ImportedBomLine.id)
        .all()
    )
    for line_number, imported_line in enumerate(imported_lines, start=1):
        component_product = None
        if imported_line.component_sku:
            component_product = db.query(Product).filter(Product.sku == imported_line.component_sku).one_or_none()
        db.add(
            ProductBomLine(
                bom_header_id=header.id,
                component_product_id=component_product.id if component_product is not None else None,
                component_sku_snapshot=imported_line.component_sku,
                component_name_snapshot=component_product.name if component_product is not None else imported_line.component_name,
                unit_snapshot=component_product.unit if component_product is not None else None,
                quantity_standard=imported_line.quantity,
                line_number=line_number,
                notes=None,
                source_imported_bom_line_id=imported_line.id,
                component_type=imported_line.component_type,
                include_in_real_cost=imported_line.include_in_real_cost,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
        )

    db.commit()
    return get_product_bom(db, product.id)


def list_bom_component_options(
    db: Session,
    current_component_ids: list[int] | None = None,
    *,
    active_only: bool = True,
    exclude_product_id: int | None = None,
) -> list[Product]:
    current_ids = [component_id for component_id in (current_component_ids or []) if component_id is not None]
    query = db.query(Product)
    if active_only:
        if current_ids:
            query = query.filter((Product.active.is_(True)) | (Product.id.in_(current_ids)))
        else:
            query = query.filter(Product.active.is_(True))
    if exclude_product_id is not None:
        query = query.filter(Product.id != exclude_product_id)
    return query.order_by(Product.sku, Product.id).all()


def save_product_bom(
    db: Session,
    *,
    product_id: int,
    line_updates: list[dict[str, object]],
    new_lines: list[dict[str, object]],
) -> ProductBomHeader:
    product = db.query(Product).filter(Product.id == product_id).one_or_none()
    if product is None:
        raise ProductBomValidationError("Finished product does not exist.")
    if not product.is_manufactured:
        raise ProductBomValidationError("BOM management is available only for manufactured products.")

    header = get_product_bom(db, product.id)
    if header is None:
        header = get_or_seed_product_bom(db, product)
    if header is None:
        header = ProductBomHeader(
            product_id=product.id,
            name=f"{product.sku} BOM",
            active=True,
            source_type="manual",
            source_imported_bom_header_id=None,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )
        db.add(header)
        db.flush()
    else:
        db.refresh(header)

    existing_lines_by_id = {line.id: line for line in header.lines}
    prepared_lines: list[dict[str, object]] = []
    seen_components: set[tuple[str, object]] = set()

    for update in line_updates:
        line_id = _parse_line_id(update.get("id"))
        existing_line = existing_lines_by_id.get(line_id)
        if existing_line is None:
            raise ProductBomValidationError("BOM line does not exist.")
        if bool(update.get("delete")):
            continue
        prepared = _prepare_product_bom_line(
            db,
            product=product,
            component_sku=update.get("component_sku"),
            quantity_standard=update.get("quantity_standard"),
            notes=update.get("notes"),
            existing_line=existing_line,
        )
        if prepared is None:
            continue
        duplicate_key = _duplicate_component_key(prepared)
        if duplicate_key in seen_components:
            raise ProductBomValidationError("Duplicate component in the same BOM is not allowed.")
        seen_components.add(duplicate_key)
        prepared_lines.append(prepared)

    for update in new_lines:
        prepared = _prepare_product_bom_line(
            db,
            product=product,
            component_sku=update.get("component_sku"),
            quantity_standard=update.get("quantity_standard"),
            notes=update.get("notes"),
            existing_line=None,
        )
        if prepared is None:
            continue
        duplicate_key = _duplicate_component_key(prepared)
        if duplicate_key in seen_components:
            raise ProductBomValidationError("Duplicate component in the same BOM is not allowed.")
        seen_components.add(duplicate_key)
        prepared_lines.append(prepared)

    if not prepared_lines:
        raise ProductBomValidationError("BOM must contain at least one valid component line.")

    for existing_line in list(header.lines):
        db.delete(existing_line)
    db.flush()

    for line_number, prepared in enumerate(prepared_lines, start=1):
        db.add(
            ProductBomLine(
                bom_header_id=header.id,
                component_product_id=prepared["component_product_id"],
                component_sku_snapshot=prepared["component_sku_snapshot"],
                component_name_snapshot=prepared["component_name_snapshot"],
                unit_snapshot=prepared["unit_snapshot"],
                quantity_standard=prepared["quantity_standard"],
                line_number=line_number,
                notes=prepared["notes"],
                source_imported_bom_line_id=prepared["source_imported_bom_line_id"],
                component_type=prepared["component_type"],
                include_in_real_cost=prepared["include_in_real_cost"],
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
        )

    header.active = True
    header.updated_at = datetime.utcnow()
    db.commit()
    return get_product_bom(db, product.id)  # type: ignore[return-value]


def get_latest_imported_bom_header(db: Session, product_sku: str) -> ImportedBomHeader | None:
    return (
        db.query(ImportedBomHeader)
        .join(ImportBatch)
        .filter(ImportedBomHeader.product_sku == product_sku)
        .order_by(ImportBatch.imported_at.desc(), ImportedBomHeader.imported_at.desc(), ImportedBomHeader.id.desc())
        .first()
    )


def _prepare_product_bom_line(
    db: Session,
    *,
    product: Product,
    component_sku: object,
    quantity_standard: object,
    notes: object,
    existing_line: ProductBomLine | None,
) -> dict[str, object] | None:
    selected_sku = str(component_sku or "").strip()
    quantity_text = str(quantity_standard or "").strip()
    notes_text = str(notes or "").strip() or None

    if existing_line is None and not selected_sku and not quantity_text and not notes_text:
        return None

    if not selected_sku:
        raise ProductBomValidationError("Component product is required.")

    if selected_sku == product.sku:
        raise ProductBomValidationError("Finished product cannot be its own component.")

    parsed_quantity = _parse_positive_decimal(quantity_text, "Quantity standard")
    component_product = db.query(Product).filter(Product.sku == selected_sku).one_or_none()

    if (
        existing_line is not None
        and existing_line.component_product_id is None
        and selected_sku == (existing_line.component_sku_snapshot or "").strip()
        and component_product is None
    ):
        return {
            "component_product_id": None,
            "component_sku_snapshot": existing_line.component_sku_snapshot,
            "component_name_snapshot": existing_line.component_name_snapshot,
            "unit_snapshot": existing_line.unit_snapshot,
            "quantity_standard": parsed_quantity,
            "notes": notes_text,
            "source_imported_bom_line_id": existing_line.source_imported_bom_line_id,
            "component_type": existing_line.component_type,
            "include_in_real_cost": existing_line.include_in_real_cost,
        }

    if component_product is None:
        raise ProductBomValidationError(f"Component SKU {selected_sku} does not exist.")

    return {
        "component_product_id": component_product.id,
        "component_sku_snapshot": component_product.sku,
        "component_name_snapshot": component_product.name,
        "unit_snapshot": component_product.unit,
        "quantity_standard": parsed_quantity,
        "notes": notes_text,
        "source_imported_bom_line_id": (
            existing_line.source_imported_bom_line_id
            if existing_line is not None and selected_sku == (existing_line.component_sku_snapshot or "").strip()
            else None
        ),
        "component_type": existing_line.component_type if existing_line is not None else "material",
        "include_in_real_cost": existing_line.include_in_real_cost if existing_line is not None else True,
    }


def _duplicate_component_key(prepared_line: dict[str, object]) -> tuple[str, object]:
    component_product_id = prepared_line.get("component_product_id")
    if component_product_id is not None:
        return ("product", component_product_id)
    return ("sku", str(prepared_line.get("component_sku_snapshot") or "").strip().upper())


def _parse_positive_decimal(value: str, field_name: str) -> Decimal:
    text = (value or "").strip().replace(" ", "").replace(",", ".")
    if not text:
        raise ProductBomValidationError(f"{field_name} is required.")
    try:
        parsed = Decimal(text)
    except InvalidOperation as exc:
        raise ProductBomValidationError(f"{field_name} must be a valid number.") from exc
    if parsed <= ZERO:
        raise ProductBomValidationError(f"{field_name} must be greater than 0.")
    return parsed


def _parse_line_id(value: object) -> int:
    text = str(value or "").strip()
    if not text.isdigit():
        raise ProductBomValidationError("BOM line identifier is invalid.")
    return int(text)
