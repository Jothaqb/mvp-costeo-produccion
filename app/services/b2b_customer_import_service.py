import csv
import io
import unicodedata
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation

from sqlalchemy.orm import Session

from app.models import B2BCustomer, B2BCustomerProduct, Product


class B2BCustomerImportValidationError(Exception):
    pass


@dataclass(frozen=True)
class B2BCustomerImportWarning:
    row_number: int
    message: str


@dataclass(frozen=True)
class B2BCustomerImportResult:
    file_name: str
    total_rows: int
    customers_created: int
    customers_updated: int
    relations_created: int
    relations_updated: int
    skipped_rows: int
    warnings: list[B2BCustomerImportWarning] = field(default_factory=list)


_CUSTOMER_NAME_HEADERS = ("customer_name", "customer", "cliente", "nombre_cliente")
_CUSTOMER_ACTIVE_HEADERS = ("active", "activo", "esta_activo")
_LEGAL_NAME_HEADERS = ("legal_name", "razon_social")
_LEGAL_ID_HEADERS = ("legal_id", "cedula", "identificacion")
_PHONE_HEADERS = ("phone", "telefono")
_ADDRESS_HEADERS = ("address", "direccion")
_PROVINCE_HEADERS = ("province", "provincia")
_CANTON_HEADERS = ("canton", "canton")
_DISTRICT_HEADERS = ("district", "distrito")
_OBSERVATIONS_HEADERS = ("observations", "observaciones", "notes")

_SKU_HEADERS = ("sku", "product_sku", "codigo")
_PRODUCT_NAME_HEADERS = ("product_name", "producto", "description", "descripcion")
_PRICE_HEADERS = ("distributor_price", "b2b_price", "price", "precio", "precio_b2b")
_ACTIVE_PRODUCT_HEADERS = ("active_product", "producto_activo", "active_item")

_TRUE_VALUES = {"yes", "y", "true", "1", "si", "active", "activo"}
_FALSE_VALUES = {"no", "n", "false", "0", "inactive", "inactivo"}
_CURRENCY_SYMBOLS = {"₡", "$", "¢"}


def import_b2b_customers_csv(db: Session, *, file_name: str, file_bytes: bytes) -> B2BCustomerImportResult:
    if not file_bytes:
        raise B2BCustomerImportValidationError("Please choose a CSV file to import.")

    decoded_text = _decode_csv_bytes(file_bytes)
    rows = _read_csv_rows(decoded_text)

    customer_lookup = _build_customer_lookup(db.query(B2BCustomer).order_by(B2BCustomer.id).all())
    product_lookup = _build_product_lookup(db.query(Product).order_by(Product.id).all())
    relation_lookup = _build_relation_lookup(db.query(B2BCustomerProduct).order_by(B2BCustomerProduct.id).all())

    warnings: list[B2BCustomerImportWarning] = []
    seen_customer_keys: dict[str, int] = {}
    seen_relation_keys: dict[tuple[int, str], int] = {}
    customer_created_ids: set[int] = set()
    customer_updated_ids: set[int] = set()
    relation_created_keys: set[tuple[int, str]] = set()
    relation_updated_keys: set[tuple[int, str]] = set()
    total_rows = 0
    skipped_rows = 0

    for row_number, row in rows:
        if _row_is_blank(row):
            continue

        total_rows += 1
        customer_name_raw = _field_value(row, _CUSTOMER_NAME_HEADERS)
        customer_name = _normalize_name(customer_name_raw)
        legal_id_raw = _field_value(row, _LEGAL_ID_HEADERS)
        legal_id = legal_id_raw.strip()
        name_key = _normalize_name_key(customer_name)
        legal_key = _normalize_legal_id_key(legal_id)

        if not legal_key and not name_key:
            warnings.append(
                B2BCustomerImportWarning(
                    row_number=row_number,
                    message="Customer identification is required. Provide legal_id or customer_name. Row skipped.",
                )
            )
            skipped_rows += 1
            continue

        customer_match = _resolve_customer_match(customer_lookup, legal_key, name_key)
        if customer_match == "ambiguous":
            identifier = legal_id or customer_name or "unknown customer"
            warnings.append(
                B2BCustomerImportWarning(
                    row_number=row_number,
                    message=f"Customer '{identifier}' matches multiple existing B2B customers. Row skipped.",
                )
            )
            skipped_rows += 1
            continue

        customer = customer_match
        customer_identity_key = f"legal:{legal_key}" if legal_key else f"name:{name_key}"
        if customer_identity_key in seen_customer_keys:
            warnings.append(
                B2BCustomerImportWarning(
                    row_number=row_number,
                    message="Customer appears more than once in the CSV. Applying latest row values.",
                )
            )
        seen_customer_keys[customer_identity_key] = row_number

        if customer is None:
            customer = B2BCustomer(active=True)
            if customer_name:
                customer.customer_name = customer_name
            elif legal_id:
                customer.customer_name = legal_id
            customer.active, _ = _resolve_active_value(
                _field_value(row, _CUSTOMER_ACTIVE_HEADERS),
                current_value=None,
                is_new=True,
                row_number=row_number,
                warnings=warnings,
                label="Customer active",
            )
            _apply_customer_fields(customer, row, allow_blank_overwrite=False)
            db.add(customer)
            db.flush()
            _register_customer_lookup(customer_lookup, customer)
            customer_created_ids.add(customer.id)
        else:
            customer.active, active_changed = _resolve_active_value(
                _field_value(row, _CUSTOMER_ACTIVE_HEADERS),
                current_value=customer.active,
                is_new=False,
                row_number=row_number,
                warnings=warnings,
                label="Customer active",
            )
            customer_changed = _apply_customer_fields(customer, row, allow_blank_overwrite=False)
            if (active_changed or customer_changed) and customer.id not in customer_created_ids:
                customer_updated_ids.add(customer.id)
            _register_customer_lookup(customer_lookup, customer)

        observations = _field_value(row, _OBSERVATIONS_HEADERS)
        if observations:
            warnings.append(
                B2BCustomerImportWarning(
                    row_number=row_number,
                    message="observations column ignored because B2BCustomer has no observations field.",
                )
            )

        sku_value = _field_value(row, _SKU_HEADERS)
        sku_key = _normalize_sku_key(sku_value)
        if not sku_key:
            warnings.append(
                B2BCustomerImportWarning(
                    row_number=row_number,
                    message="SKU is required for customer-product assignment. Relation skipped.",
                )
            )
            skipped_rows += 1
            continue

        product_match = product_lookup.get(sku_key)
        if isinstance(product_match, list):
            warnings.append(
                B2BCustomerImportWarning(
                    row_number=row_number,
                    message=f"SKU '{sku_value.strip()}' matches multiple products. Relation skipped.",
                )
            )
            skipped_rows += 1
            continue
        if product_match is None:
            warnings.append(
                B2BCustomerImportWarning(
                    row_number=row_number,
                    message=f"SKU '{sku_value.strip()}' was not found in Product Master. Relation skipped.",
                )
            )
            skipped_rows += 1
            continue

        relation_key = (customer.id, sku_key)
        if relation_key in seen_relation_keys:
            warnings.append(
                B2BCustomerImportWarning(
                    row_number=row_number,
                    message=f"Customer/SKU pair '{customer.customer_name} / {product_match.sku}' appears more than once in the CSV. Applying latest row values.",
                )
            )
        seen_relation_keys[relation_key] = row_number

        existing_relation = relation_lookup.get(relation_key)
        if isinstance(existing_relation, list):
            warnings.append(
                B2BCustomerImportWarning(
                    row_number=row_number,
                    message=f"Customer/SKU pair '{customer.customer_name} / {product_match.sku}' matches multiple existing catalog rows. Relation skipped.",
                )
            )
            skipped_rows += 1
            continue

        description = _field_value(row, _PRODUCT_NAME_HEADERS) or (product_match.name or "").strip()
        if not description:
            warnings.append(
                B2BCustomerImportWarning(
                    row_number=row_number,
                    message=f"SKU '{product_match.sku}' has no description in CSV or Product Master. Relation skipped.",
                )
            )
            skipped_rows += 1
            continue

        price_value = _field_value(row, _PRICE_HEADERS)
        parsed_price, price_error = _parse_distributor_price(price_value)
        if existing_relation is None:
            if price_value == "":
                warnings.append(
                    B2BCustomerImportWarning(
                        row_number=row_number,
                        message=f"Distributor price is required to create customer SKU '{product_match.sku}'. Relation skipped.",
                    )
                )
                skipped_rows += 1
                continue
            if price_error:
                warnings.append(B2BCustomerImportWarning(row_number=row_number, message=price_error))
                skipped_rows += 1
                continue

            relation = B2BCustomerProduct(
                customer_id=customer.id,
                sku=product_match.sku,
                description=description,
                distributor_price=parsed_price,
                active=_resolve_active_value(
                    _field_value(row, _ACTIVE_PRODUCT_HEADERS),
                    current_value=None,
                    is_new=True,
                    row_number=row_number,
                    warnings=warnings,
                    label="active_product",
                )[0],
            )
            db.add(relation)
            db.flush()
            relation_lookup[relation_key] = relation
            relation_created_keys.add(relation_key)
            continue

        if _field_value(row, _PRODUCT_NAME_HEADERS):
            existing_relation.description = description

        if price_value != "":
            if price_error:
                warnings.append(B2BCustomerImportWarning(row_number=row_number, message=price_error))
                skipped_rows += 1
            else:
                existing_relation.distributor_price = parsed_price
                if relation_key not in relation_created_keys:
                    relation_updated_keys.add(relation_key)

        active_value = _field_value(row, _ACTIVE_PRODUCT_HEADERS)
        if active_value != "":
            existing_relation.active, active_changed = _resolve_active_value(
                active_value,
                current_value=existing_relation.active,
                is_new=False,
                row_number=row_number,
                warnings=warnings,
                label="active_product",
            )
            if active_changed and relation_key not in relation_created_keys:
                relation_updated_keys.add(relation_key)

        if _field_value(row, _PRODUCT_NAME_HEADERS) and relation_key not in relation_created_keys:
            relation_updated_keys.add(relation_key)

    db.commit()
    return B2BCustomerImportResult(
        file_name=file_name,
        total_rows=total_rows,
        customers_created=len(customer_created_ids),
        customers_updated=len(customer_updated_ids),
        relations_created=len(relation_created_keys),
        relations_updated=len(relation_updated_keys),
        skipped_rows=skipped_rows,
        warnings=warnings,
    )


def _read_csv_rows(decoded_text: str) -> list[tuple[int, dict[str, str]]]:
    sample = decoded_text[:4096]
    delimiter = _detect_delimiter(sample)
    reader = csv.DictReader(io.StringIO(decoded_text), delimiter=delimiter)
    if reader.fieldnames is None:
        raise B2BCustomerImportValidationError("The CSV file does not contain a header row.")

    normalized_fieldnames = [_normalize_header(fieldname) for fieldname in reader.fieldnames]
    has_customer_identifier = any(fieldname in _CUSTOMER_NAME_HEADERS + _LEGAL_ID_HEADERS for fieldname in normalized_fieldnames)
    has_sku = any(fieldname in _SKU_HEADERS for fieldname in normalized_fieldnames)
    has_price = any(fieldname in _PRICE_HEADERS for fieldname in normalized_fieldnames)
    if not has_customer_identifier:
        raise B2BCustomerImportValidationError("The CSV file must include customer_name or legal_id headers (or accepted aliases).")
    if not has_sku:
        raise B2BCustomerImportValidationError("The CSV file must include an sku header (or accepted alias).")
    if not has_price:
        raise B2BCustomerImportValidationError("The CSV file must include a distributor_price header (or accepted alias).")

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
    return ";" if sample.count(";") > sample.count(",") else ","


def _decode_csv_bytes(file_bytes: bytes) -> str:
    for encoding in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            return file_bytes.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise B2BCustomerImportValidationError(
        "CSV files exported from Excel are supported. Recommended format: CSV UTF-8. The importer also accepts common Windows/Latin encodings."
    )


def _build_customer_lookup(customers: list[B2BCustomer]) -> dict[str, B2BCustomer | list[B2BCustomer]]:
    lookup: dict[str, B2BCustomer | list[B2BCustomer]] = {}
    for customer in customers:
        name_key = _normalize_name_key(customer.customer_name)
        legal_key = _normalize_legal_id_key(customer.legal_id or "")
        if name_key:
            _append_lookup_value(lookup, f"name:{name_key}", customer)
        if legal_key:
            _append_lookup_value(lookup, f"legal:{legal_key}", customer)
    return lookup


def _build_product_lookup(products: list[Product]) -> dict[str, Product | list[Product]]:
    lookup: dict[str, Product | list[Product]] = {}
    for product in products:
        sku_key = _normalize_sku_key(product.sku)
        if sku_key:
            _append_lookup_value(lookup, sku_key, product)
    return lookup


def _build_relation_lookup(
    relations: list[B2BCustomerProduct],
) -> dict[tuple[int, str], B2BCustomerProduct | list[B2BCustomerProduct]]:
    lookup: dict[tuple[int, str], B2BCustomerProduct | list[B2BCustomerProduct]] = {}
    for relation in relations:
        key = (relation.customer_id, _normalize_sku_key(relation.sku))
        existing = lookup.get(key)
        if existing is None:
            lookup[key] = relation
        elif isinstance(existing, list):
            existing.append(relation)
        else:
            lookup[key] = [existing, relation]
    return lookup


def _resolve_customer_match(
    lookup: dict[str, B2BCustomer | list[B2BCustomer]],
    legal_key: str,
    name_key: str,
) -> B2BCustomer | str | None:
    if legal_key:
        match = lookup.get(f"legal:{legal_key}")
    else:
        match = lookup.get(f"name:{name_key}")
    if isinstance(match, list):
        return "ambiguous"
    return match


def _register_customer_lookup(
    lookup: dict[str, B2BCustomer | list[B2BCustomer]],
    customer: B2BCustomer,
) -> None:
    name_key = _normalize_name_key(customer.customer_name)
    legal_key = _normalize_legal_id_key(customer.legal_id or "")
    if name_key:
        lookup[f"name:{name_key}"] = customer
    if legal_key:
        lookup[f"legal:{legal_key}"] = customer


def _append_lookup_value(lookup: dict, key: object, value: object) -> None:
    existing = lookup.get(key)
    if existing is None:
        lookup[key] = value
    elif isinstance(existing, list):
        existing.append(value)
    else:
        lookup[key] = [existing, value]


def _apply_customer_fields(customer: B2BCustomer, row: dict[str, str], *, allow_blank_overwrite: bool) -> bool:
    fields = {
        "customer_name": _normalize_name(_field_value(row, _CUSTOMER_NAME_HEADERS)),
        "legal_name": _field_value(row, _LEGAL_NAME_HEADERS),
        "legal_id": _field_value(row, _LEGAL_ID_HEADERS),
        "phone": _field_value(row, _PHONE_HEADERS),
        "address": _field_value(row, _ADDRESS_HEADERS),
        "province": _field_value(row, _PROVINCE_HEADERS),
        "canton": _field_value(row, _CANTON_HEADERS),
        "district": _field_value(row, _DISTRICT_HEADERS),
    }
    changed = False
    for field_name, value in fields.items():
        if value or allow_blank_overwrite:
            new_value = value or None
            if getattr(customer, field_name) != new_value:
                setattr(customer, field_name, new_value)
                changed = True
    return changed


def _resolve_active_value(
    raw_value: str,
    *,
    current_value: bool | None,
    is_new: bool,
    row_number: int,
    warnings: list[B2BCustomerImportWarning],
    label: str,
) -> tuple[bool, bool]:
    normalized = _normalize_token(raw_value)
    if not normalized:
        fallback_value = True if is_new else bool(current_value)
        return fallback_value, False
    if normalized in _TRUE_VALUES:
        return True, bool(current_value) is not True
    if normalized in _FALSE_VALUES:
        return False, bool(current_value) is not False
    warnings.append(
        B2BCustomerImportWarning(
            row_number=row_number,
            message=f"{label} value '{raw_value}' is invalid. Using default preserved behavior.",
        )
    )
    fallback_value = True if is_new else bool(current_value)
    return fallback_value, False


def _parse_distributor_price(raw_value: str) -> tuple[Decimal | None, str | None]:
    if raw_value == "":
        return None, None
    cleaned = raw_value.strip()
    if not cleaned:
        return None, None
    for symbol in _CURRENCY_SYMBOLS:
        cleaned = cleaned.replace(symbol, "")
    cleaned = cleaned.replace(" ", "")
    if "," in cleaned and "." in cleaned:
        if cleaned.rfind(",") > cleaned.rfind("."):
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    elif "," in cleaned:
        parts = cleaned.split(",")
        if all(part.isdigit() for part in parts) and all(len(part) == 3 for part in parts[1:]):
            cleaned = "".join(parts)
        else:
            cleaned = cleaned.replace(",", ".")
    try:
        value = Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return None, f"Distributor price '{raw_value}' is invalid. Relation skipped."
    if value < Decimal("0"):
        return None, f"Distributor price '{raw_value}' cannot be negative. Relation skipped."
    return value, None


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


def _normalize_name_key(value: str) -> str:
    return _normalize_name(value).casefold()


def _normalize_legal_id_key(value: str) -> str:
    cleaned = "".join((value or "").strip().split()).replace("-", "")
    return cleaned.casefold()


def _normalize_sku_key(value: str) -> str:
    return _normalize_name(value).casefold()


def _normalize_token(value: str) -> str:
    return _normalize_header(value).replace("_", "").replace(" ", "")


def _normalize_header(value: str | None) -> str:
    text = str(value or "").strip().lstrip("\ufeff").lower()
    text = unicodedata.normalize("NFKD", text)
    return "".join(character for character in text if not unicodedata.combining(character))
