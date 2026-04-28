import csv
import io
import unicodedata
from dataclasses import dataclass, field

from sqlalchemy.orm import Session

from app.models import Supplier


class SupplierImportValidationError(Exception):
    pass


@dataclass(frozen=True)
class SupplierImportWarning:
    row_number: int
    message: str


@dataclass(frozen=True)
class SupplierImportResult:
    file_name: str
    total_rows: int
    created: int
    updated: int
    skipped: int
    warnings: list[SupplierImportWarning] = field(default_factory=list)


_SUPPLIER_NAME_HEADERS = ("supplier_name", "name", "supplier", "proveedor")
_ACTIVE_HEADERS = ("active", "activo", "esta_activo")
_CONTACT_HEADERS = ("contact", "contact_name", "contacto")
_PHONE_HEADERS = ("phone", "telefono")
_EMAIL_HEADERS = ("email", "correo")
_ADDRESS_HEADERS = ("address", "direccion")
_OBSERVATION_HEADERS = ("observations", "observaciones", "notes")

_ACTIVE_TRUE = {"yes", "y", "true", "1", "si", "sí", "active", "activo"}
_ACTIVE_FALSE = {"no", "n", "false", "0", "inactive", "inactivo"}


def import_suppliers_csv(db: Session, *, file_name: str, file_bytes: bytes) -> SupplierImportResult:
    if not file_bytes:
        raise SupplierImportValidationError("Please choose a CSV file to import.")

    decoded_text = _decode_csv_bytes(file_bytes)
    rows = _read_csv_rows(decoded_text)
    existing_suppliers = db.query(Supplier).order_by(Supplier.id).all()
    supplier_lookup = _existing_supplier_lookup(existing_suppliers)
    seen_names: dict[str, int] = {}
    warnings: list[SupplierImportWarning] = []
    created = 0
    updated = 0
    skipped = 0
    total_rows = 0

    for row_number, row in rows:
        if _row_is_blank(row):
            continue
        total_rows += 1
        supplier_name = _field_value(row, _SUPPLIER_NAME_HEADERS)
        normalized_name = _normalize_key(supplier_name)
        if not normalized_name:
            warnings.append(SupplierImportWarning(row_number=row_number, message="Supplier name is required. Row skipped."))
            skipped += 1
            continue

        if normalized_name in seen_names:
            warnings.append(
                SupplierImportWarning(
                    row_number=row_number,
                    message=f"Supplier '{supplier_name.strip()}' appears more than once in the CSV. Applying latest row values.",
                )
            )
        seen_names[normalized_name] = row_number

        existing_match = supplier_lookup.get(normalized_name)
        if isinstance(existing_match, list):
            warnings.append(
                SupplierImportWarning(
                    row_number=row_number,
                    message=f"Supplier '{supplier_name.strip()}' matches multiple existing suppliers. Row skipped.",
                )
            )
            skipped += 1
            continue

        if existing_match is None:
            supplier = Supplier()
            supplier.name = _normalize_name(supplier_name)
            supplier.active = _resolve_active(_field_value(row, _ACTIVE_HEADERS), current_value=None, is_new=True, row_number=row_number, warnings=warnings)
            _apply_optional_text_fields(supplier, row)
            db.add(supplier)
            supplier_lookup[normalized_name] = supplier
            created += 1
            continue

        supplier = existing_match
        supplier.name = _normalize_name(supplier_name)
        supplier.active = _resolve_active(_field_value(row, _ACTIVE_HEADERS), current_value=supplier.active, is_new=False, row_number=row_number, warnings=warnings)
        _apply_optional_text_fields(supplier, row)
        updated += 1

    db.commit()
    return SupplierImportResult(
        file_name=file_name,
        total_rows=total_rows,
        created=created,
        updated=updated,
        skipped=skipped,
        warnings=warnings,
    )


def _read_csv_rows(decoded_text: str) -> list[tuple[int, dict[str, str]]]:
    sample = decoded_text[:4096]
    delimiter = _detect_delimiter(sample)
    reader = csv.DictReader(io.StringIO(decoded_text), delimiter=delimiter)
    if reader.fieldnames is None:
        raise SupplierImportValidationError("The CSV file does not contain a header row.")

    normalized_fieldnames = [_normalize_header(fieldname) for fieldname in reader.fieldnames]
    if not any(fieldname in _SUPPLIER_NAME_HEADERS for fieldname in normalized_fieldnames):
        raise SupplierImportValidationError("The CSV file must include a supplier_name column or accepted alias.")

    rows: list[tuple[int, dict[str, str]]] = []
    for index, row in enumerate(reader, start=2):
        normalized_row = {_normalize_header(key): _clean_value(value) for key, value in row.items() if key is not None}
        rows.append((index, normalized_row))
    return rows


def _detect_delimiter(sample: str) -> str:
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;")
        if dialect.delimiter in {",", ";"}:
            return dialect.delimiter
    except csv.Error:
        pass
    comma_count = sample.count(",")
    semicolon_count = sample.count(";")
    return ";" if semicolon_count > comma_count else ","


def _decode_csv_bytes(file_bytes: bytes) -> str:
    for encoding in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            return file_bytes.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise SupplierImportValidationError(
        "CSV files exported from Excel are supported. Recommended format: CSV UTF-8. The importer also accepts common Windows/Latin encodings."
    )


def _existing_supplier_lookup(suppliers: list[Supplier]) -> dict[str, Supplier | list[Supplier]]:
    lookup: dict[str, Supplier | list[Supplier]] = {}
    for supplier in suppliers:
        normalized_name = _normalize_key(supplier.name)
        existing = lookup.get(normalized_name)
        if existing is None:
            lookup[normalized_name] = supplier
        elif isinstance(existing, list):
            existing.append(supplier)
        else:
            lookup[normalized_name] = [existing, supplier]
    return lookup


def _apply_optional_text_fields(supplier: Supplier, row: dict[str, str]) -> None:
    contact_name = _field_value(row, _CONTACT_HEADERS)
    phone = _field_value(row, _PHONE_HEADERS)
    email = _field_value(row, _EMAIL_HEADERS)
    notes_value = _build_notes_value(
        address=_field_value(row, _ADDRESS_HEADERS),
        observations=_field_value(row, _OBSERVATION_HEADERS),
    )
    if contact_name:
        supplier.contact_name = contact_name
    if phone:
        supplier.phone = phone
    if email:
        supplier.email = email
    if notes_value:
        supplier.notes = notes_value


def _build_notes_value(*, address: str, observations: str) -> str | None:
    parts: list[str] = []
    if address:
        parts.append(f"Address: {address}")
    if observations:
        if address:
            parts.append(f"Notes: {observations}")
        else:
            parts.append(observations)
    return "\n".join(parts) if parts else None


def _resolve_active(
    raw_value: str,
    *,
    current_value: bool | None,
    is_new: bool,
    row_number: int,
    warnings: list[SupplierImportWarning],
) -> bool:
    normalized = _normalize_header(raw_value)
    if not normalized:
        return True if is_new else bool(current_value)
    if normalized in _ACTIVE_TRUE:
        return True
    if normalized in _ACTIVE_FALSE:
        return False
    warnings.append(
        SupplierImportWarning(
            row_number=row_number,
            message=f"Active value '{raw_value}' is invalid. Using default preserved behavior.",
        )
    )
    return True if is_new else bool(current_value)


def _field_value(row: dict[str, str], aliases: tuple[str, ...]) -> str:
    fallback = ""
    for alias in aliases:
        value = row.get(alias)
        if value is not None:
            cleaned = value.strip()
            if cleaned:
                return cleaned
            fallback = cleaned
    return fallback


def _clean_value(value: object) -> str:
    return str(value or "").strip()


def _row_is_blank(row: dict[str, str]) -> bool:
    return not any((value or "").strip() for value in row.values())


def _normalize_name(value: str) -> str:
    return " ".join((value or "").strip().split())


def _normalize_key(value: str) -> str:
    return _normalize_name(value).casefold()


def _normalize_header(value: str | None) -> str:
    text = str(value or "").strip().lstrip("\ufeff").lower()
    text = unicodedata.normalize("NFKD", text)
    return "".join(character for character in text if not unicodedata.combining(character))
