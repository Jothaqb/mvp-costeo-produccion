from __future__ import annotations

import hashlib
import hmac
import os
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urlsplit

from sqlalchemy.orm import Session

from app.models import AuditLog, Permission, Role, RolePermission, User, UserRole
from app.services.audit_service import serialize_audit_payload


MODULE_ORDER = (
    "Administración",
    "Ventas",
    "Clientes y catálogo B2B",
    "Acuerdos comerciales",
    "Cuentas por cobrar",
    "Productos",
    "BOM",
    "Inventario y Kardex",
    "Compras",
    "Producción",
    "Planeación",
    "Contabilidad",
    "Reportes",
    "Integraciones",
)
ROLE_PERMISSIONS_CSRF_PURPOSE = b"green-corner:admin:roles-permissions:post:v1"
LOCAL_CSRF_HOSTS = {"localhost", "127.0.0.1", "::1"}

PERMISSION_TEXT_ES = {
    "admin.users.manage": ("Administrar usuarios", "Crear, editar, activar y desactivar usuarios del ERP."),
    "admin.roles.manage": ("Administrar roles y permisos", "Consultar y modificar los permisos asignados a roles."),
    "audit.view": ("Ver auditoría", "Consultar el historial de auditoría del ERP."),
    "reporting.view": ("Ver reportes", "Consultar reportes comerciales y operativos."),
    "reporting.export": ("Exportar reportes", "Descargar datos de los reportes disponibles."),
    "accounting.view": ("Ver contabilidad", "Consultar el Estado de Resultados gerencial mensual."),
    "accounting.edit": ("Cargar gastos reales", "Registrar y actualizar montos reales mensuales de contabilidad."),
    "accounting.manage_budget": ("Administrar cuentas y presupuesto", "Crear y editar cuentas, subcuentas y presupuestos."),
    "product.view": ("Ver productos", "Consultar productos y utilizar búsquedas del catálogo maestro."),
    "product.create": ("Crear productos", "Crear productos en el catálogo maestro."),
    "product.edit": ("Editar productos", "Editar los datos maestros de productos."),
    "product.edit_prices": ("Editar precios", "Modificar precios de venta de productos."),
    "product.edit_cost": ("Editar costo estándar", "Modificar el costo estándar de productos."),
    "product.export": ("Exportar productos", "Descargar datos del catálogo maestro de productos."),
    "bom.view": ("Ver BOM", "Consultar listas de materiales de productos."),
    "bom.create": ("Crear BOM", "Permiso definido para crear listas de materiales."),
    "bom.edit": ("Editar BOM", "Crear o modificar componentes mediante el editor actual de BOM."),
    "bom.delete": ("Eliminar componentes BOM", "Permiso definido para eliminar componentes de listas de materiales."),
    "b2b_customer_products.edit_prices": ("Editar catálogo B2B", "Agregar, editar, activar o desactivar productos y precios por cliente B2B."),
    "b2b_sales.cancel_invoice": ("Anular factura B2B", "Anular facturas B2B y registrar el reverso formal de inventario."),
    "b2b_commercial_agreement.view": ("Ver acuerdos comerciales", "Consultar acuerdos comerciales B2B y descargar sus PDF generados."),
    "b2b_commercial_agreement.edit": ("Editar acuerdos comerciales", "Crear y editar borradores de acuerdos comerciales B2B."),
    "b2b_commercial_agreement.generate": ("Generar acuerdo PDF", "Validar y generar el PDF inmutable de un acuerdo comercial."),
    "b2b_commercial_agreement.sign_archive": ("Firmar o archivar acuerdos", "Permiso reservado para firma y archivo de acuerdos comerciales."),
    "planning.view": ("Ver planeación", "Consultar información y resultados de planeación."),
    "planning.edit_parameters": ("Editar parámetros de planeación", "Modificar parámetros generales de planeación."),
    "planning.edit_moq": ("Editar MOQ", "Modificar cantidades mínimas de compra en planeación."),
    "planning.edit_zones": ("Editar zonas de planeación", "Modificar zonas y cantidades del planificador."),
    "sales.view": ("Ver ventas y clientes", "Consultar órdenes B2B/B2C, clientes y exportaciones de catálogo protegidas por este permiso compartido."),
    "sales.create": ("Crear ventas", "Crear órdenes de venta B2B y B2C."),
    "sales.edit": ("Editar ventas", "Editar órdenes de venta B2B y B2C."),
    "sales.invoice": ("Facturar ventas", "Cambiar órdenes B2B y B2C al estado facturado."),
    "sales.import": ("Importar ventas históricas", "Ejecutar importaciones históricas de ventas."),
    "sales.export": ("Exportar ventas", "Permiso definido para exportación general de ventas."),
    "sales.import_loyverse_b2c": ("Previsualizar recibos B2C de Loyverse", "Consultar recibos B2C de Loyverse antes de importarlos."),
    "sales.apply_loyverse_b2c": ("Aplicar recibos B2C de Loyverse", "Aplicar recibos B2C de Loyverse como ventas de reporte en el ERP."),
    "sales.reconcile_loyverse_b2c": ("Conciliar ventas B2C de Loyverse", "Conciliar recibos B2C de Loyverse con ventas del ERP."),
    "sales.preview_b2c_inventory_impact": ("Previsualizar impacto B2C", "Previsualizar impacto de inventario de ventas B2C importadas."),
    "ar.view": ("Ver cuentas por cobrar", "Consultar facturas, saldos y estados de cuentas por cobrar."),
    "ar.record_payment": ("Registrar pagos", "Registrar pagos manuales en cuentas por cobrar."),
    "ar.export": ("Exportar cuentas por cobrar", "Descargar información de cuentas por cobrar."),
    "ar.manage_opening_balance": ("Administrar saldos iniciales", "Crear o actualizar saldos iniciales de cuentas por cobrar."),
    "purchase_order.view": ("Ver compras", "Consultar órdenes de compra."),
    "purchase_order.create": ("Crear compras", "Crear órdenes de compra."),
    "purchase_order.edit": ("Editar compras", "Editar órdenes de compra."),
    "purchase_order.receive": ("Recibir o cerrar compras", "Recibir productos y cerrar órdenes de compra."),
    "purchase_order.import": ("Importar compras históricas", "Importar órdenes de compra históricas."),
    "production_order.view": ("Ver producción", "Consultar órdenes de producción."),
    "production_order.create": ("Crear producción", "Crear órdenes de producción."),
    "production_order.edit": ("Editar producción", "Editar órdenes y registros del proceso productivo."),
    "production_order.close": ("Cerrar producción", "Cerrar o revertir cierres de órdenes de producción según la operación protegida."),
    "production_order.import": ("Importar producción histórica", "Importar órdenes de producción históricas."),
    "inventory.view": ("Ver inventario y Kardex", "Consultar balances, movimientos y Kardex de inventario."),
    "inventory.adjust": ("Ajustar inventario", "Crear y contabilizar ajustes manuales de inventario."),
    "inventory.preview_erp_loyverse_stock": ("Comparar existencias ERP/Loyverse", "Previsualizar diferencias de existencias sin actualizar sistemas."),
    "inventory.preview_erp_loyverse_cost": ("Comparar costos ERP/Loyverse", "Previsualizar diferencias de costos sin actualizar sistemas."),
}

NOT_BACKEND_ENFORCED = {
    "bom.create": "El editor actual utiliza bom.edit; este código no protege una ruta backend independiente.",
    "bom.delete": "La eliminación de componentes se procesa bajo bom.edit; este código no se valida por separado.",
    "b2b_commercial_agreement.sign_archive": "La funcionalidad de firma y archivo todavía no tiene endpoints operativos.",
    "sales.export": "Las exportaciones actuales usan permisos específicos o sales.view; este código no protege una ruta backend.",
}

SHARED_PERMISSION_NOTES = {
    "sales.view": "Permiso compartido por ventas B2B/B2C, clientes B2B y algunas descargas de catálogo.",
    "sales.create": "Permiso compartido por creación de órdenes B2B y B2C.",
    "sales.edit": "Permiso compartido por edición de órdenes B2B y B2C.",
    "sales.invoice": "Permiso compartido por facturación de órdenes B2B y B2C.",
    "sales.import": "Permiso compartido por varias importaciones históricas de ventas.",
    "product.view": "También habilita la búsqueda de productos al gestionar el catálogo B2B.",
    "reporting.view": "Protege varias pantallas de reportes con un único permiso.",
    "reporting.export": "Protege varias exportaciones de reportes con un único permiso.",
}


class RolePermissionAdminError(Exception):
    pass


class RolePermissionNotFoundError(RolePermissionAdminError):
    pass


class RolePermissionValidationError(RolePermissionAdminError):
    pass


class RolePermissionProtectedRoleError(RolePermissionAdminError):
    pass


class RolePermissionConcurrentUpdateError(RolePermissionAdminError):
    pass


class RolePermissionCSRFError(RolePermissionAdminError):
    pass


@dataclass(frozen=True)
class RolePermissionOption:
    id: int
    code: str
    name: str
    active: bool
    protected: bool
    user_count: int
    active_user_count: int


@dataclass(frozen=True)
class PermissionRow:
    code: str
    label: str
    description: str
    technical_description: str | None
    action_group: str
    action_group_label: str
    coverage_status: str
    coverage_label: str
    coverage_note: str
    assigned: bool


@dataclass(frozen=True)
class PermissionGroup:
    name: str
    permissions: tuple[PermissionRow, ...]


@dataclass(frozen=True)
class RolePermissionPanelState:
    roles: tuple[RolePermissionOption, ...]
    selected_role: RolePermissionOption
    permission_groups: tuple[PermissionGroup, ...]
    permission_version: str
    assigned_permission_codes: tuple[str, ...]


@dataclass(frozen=True)
class RolePermissionUpdateResult:
    role_id: int
    role_code: str
    previous_permission_codes: tuple[str, ...]
    new_permission_codes: tuple[str, ...]
    added_permission_codes: tuple[str, ...]
    removed_permission_codes: tuple[str, ...]
    affected_user_count: int
    changed: bool


def build_role_permissions_csrf_token(session_token: str | None) -> str:
    normalized_session_token = (session_token or "").strip()
    if not normalized_session_token:
        raise RolePermissionCSRFError("No hay una sesión válida para generar la protección CSRF.")
    return hmac.new(
        normalized_session_token.encode("utf-8"),
        ROLE_PERMISSIONS_CSRF_PURPOSE,
        hashlib.sha256,
    ).hexdigest()


def validate_role_permissions_csrf_token(
    session_token: str | None,
    submitted_token: str | None,
) -> None:
    normalized_submitted_token = (submitted_token or "").strip()
    if not normalized_submitted_token:
        raise RolePermissionCSRFError("Falta el token CSRF del formulario.")
    expected_token = build_role_permissions_csrf_token(session_token)
    if not hmac.compare_digest(normalized_submitted_token, expected_token):
        raise RolePermissionCSRFError("El token CSRF no es válido para esta sesión.")


def configured_public_origin() -> str | None:
    configured_url = (
        os.getenv("APP_BASE_URL", "").strip()
        or os.getenv("RENDER_EXTERNAL_URL", "").strip()
    )
    if not configured_url:
        return None
    normalized = _normalize_origin(configured_url)
    if normalized is None:
        raise RolePermissionCSRFError(
            "APP_BASE_URL o RENDER_EXTERNAL_URL debe contener un origen HTTP(S) válido."
        )
    return normalized


def validate_role_permissions_request_origin(
    *,
    origin_header: str | None,
    referer_header: str | None,
    trusted_origin: str | None = None,
) -> None:
    supplied_headers = [
        (header_name, header_value.strip())
        for header_name, header_value in (
            ("Origin", origin_header or ""),
            ("Referer", referer_header or ""),
        )
        if header_value.strip()
    ]
    if not supplied_headers:
        raise RolePermissionCSRFError("La solicitud no incluye Origin ni Referer para validar su origen.")

    if trusted_origin is not None:
        normalized_trusted_origin = _normalize_origin(trusted_origin)
        if normalized_trusted_origin is None:
            raise RolePermissionCSRFError("El origen público confiable configurado no es válido.")
    else:
        normalized_trusted_origin = configured_public_origin()
    normalized_headers: list[tuple[str, str]] = []
    for header_name, header_value in supplied_headers:
        normalized_value = _normalize_origin(header_value)
        if normalized_value is None:
            raise RolePermissionCSRFError(f"El encabezado {header_name} no contiene un origen válido.")
        normalized_headers.append((header_name, normalized_value))

    if normalized_trusted_origin is None:
        candidate_origins = {origin for _name, origin in normalized_headers}
        if len(candidate_origins) != 1:
            raise RolePermissionCSRFError("Origin y Referer no coinciden.")
        candidate_origin = next(iter(candidate_origins))
        candidate_host = urlsplit(candidate_origin).hostname
        if candidate_host not in LOCAL_CSRF_HOSTS:
            raise RolePermissionCSRFError(
                "Configure APP_BASE_URL o RENDER_EXTERNAL_URL para validar solicitudes en este entorno."
            )
        return

    for header_name, normalized_value in normalized_headers:
        if not hmac.compare_digest(normalized_value, normalized_trusted_origin):
            raise RolePermissionCSRFError(
                f"El encabezado {header_name} no coincide con el origen público autorizado."
            )


def list_role_permission_panel_state(
    db: Session,
    *,
    selected_role_id: int | None = None,
) -> RolePermissionPanelState:
    roles = db.query(Role).filter(Role.active.is_(True)).all()
    roles.sort(key=lambda role: (role.code == "admin", role.name.lower(), role.code))
    if not roles:
        raise RolePermissionNotFoundError("No hay roles activos disponibles.")

    selected_role = None
    if selected_role_id is not None:
        selected_role = next((role for role in roles if role.id == selected_role_id), None)
        if selected_role is None:
            raise RolePermissionNotFoundError("El rol seleccionado no existe o está inactivo.")
    else:
        selected_role = roles[0]

    user_counts = Counter(
        role_id
        for role_id, _user_id in db.query(UserRole.role_id, UserRole.user_id).all()
    )
    active_user_counts = Counter(
        role_id
        for role_id, _user_id in (
            db.query(UserRole.role_id, UserRole.user_id)
            .join(User, User.id == UserRole.user_id)
            .filter(User.is_active.is_(True))
            .all()
        )
    )
    role_options = tuple(
        RolePermissionOption(
            id=role.id,
            code=role.code,
            name=role.name,
            active=role.active,
            protected=role.code == "admin",
            user_count=user_counts[role.id],
            active_user_count=active_user_counts[role.id],
        )
        for role in roles
    )
    selected_option = next(option for option in role_options if option.id == selected_role.id)

    active_permissions = (
        db.query(Permission)
        .filter(Permission.active.is_(True))
        .order_by(Permission.module, Permission.action, Permission.code)
        .all()
    )
    assigned_codes = _active_permission_codes_for_role(db, selected_role.id)
    grouped_rows: dict[str, list[PermissionRow]] = {module_name: [] for module_name in MODULE_ORDER}
    for permission in active_permissions:
        module_name = _module_name(permission)
        grouped_rows.setdefault(module_name, []).append(
            _permission_row(permission, assigned=permission.code in assigned_codes)
        )
    permission_groups = tuple(
        PermissionGroup(
            name=module_name,
            permissions=tuple(
                sorted(
                    grouped_rows[module_name],
                    key=lambda row: (_action_group_order(row.action_group), row.label.lower(), row.code),
                )
            ),
        )
        for module_name in (*MODULE_ORDER, *sorted(set(grouped_rows) - set(MODULE_ORDER)))
        if grouped_rows.get(module_name)
    )
    return RolePermissionPanelState(
        roles=role_options,
        selected_role=selected_option,
        permission_groups=permission_groups,
        permission_version=_permission_version(selected_role, assigned_codes),
        assigned_permission_codes=tuple(sorted(assigned_codes)),
    )


def update_role_permissions(
    db: Session,
    *,
    role_id: int,
    permission_codes: set[str],
    expected_version: str,
    acting_user: User,
    request_path: str,
    request_method: str,
    ip_address: str | None,
) -> RolePermissionUpdateResult:
    role = (
        db.query(Role)
        .filter(Role.id == role_id)
        .with_for_update()
        .one_or_none()
    )
    if role is None or not role.active:
        raise RolePermissionNotFoundError("El rol seleccionado no existe o está inactivo.")
    if role.code == "admin":
        raise RolePermissionProtectedRoleError("El rol admin está protegido y no puede modificarse desde este panel.")

    normalized_codes = {str(code).strip() for code in permission_codes if str(code).strip()}
    active_permissions = (
        db.query(Permission)
        .filter(Permission.code.in_(sorted(normalized_codes)), Permission.active.is_(True))
        .all()
        if normalized_codes
        else []
    )
    permissions_by_code = {permission.code: permission for permission in active_permissions}
    invalid_codes = sorted(normalized_codes - set(permissions_by_code))
    if invalid_codes:
        raise RolePermissionValidationError(
            "Los siguientes permisos no existen o están inactivos: " + ", ".join(invalid_codes)
        )

    previous_codes = _active_permission_codes_for_role(db, role.id)
    current_version = _permission_version(role, previous_codes)
    if not expected_version or not _secure_version_match(expected_version, current_version):
        raise RolePermissionConcurrentUpdateError(
            "Los permisos del rol cambiaron desde que abrió la pantalla. Recargue y revise antes de guardar nuevamente."
        )

    added_codes = normalized_codes - previous_codes
    removed_codes = previous_codes - normalized_codes
    affected_user_count = db.query(UserRole).filter(UserRole.role_id == role.id).count()
    if not added_codes and not removed_codes:
        return RolePermissionUpdateResult(
            role_id=role.id,
            role_code=role.code,
            previous_permission_codes=tuple(sorted(previous_codes)),
            new_permission_codes=tuple(sorted(normalized_codes)),
            added_permission_codes=(),
            removed_permission_codes=(),
            affected_user_count=affected_user_count,
            changed=False,
        )

    permission_ids_by_code = {
        code: permission.id for code, permission in permissions_by_code.items()
    }
    if removed_codes:
        removed_permission_ids = [
            permission_id
            for permission_id, in (
                db.query(Permission.id).filter(Permission.code.in_(sorted(removed_codes))).all()
            )
        ]
        db.query(RolePermission).filter(
            RolePermission.role_id == role.id,
            RolePermission.permission_id.in_(removed_permission_ids),
        ).delete(synchronize_session=False)
    for code in sorted(added_codes):
        db.add(RolePermission(role_id=role.id, permission_id=permission_ids_by_code[code]))

    role.updated_at = datetime.utcnow()
    audit_entry = AuditLog(
        user_id=acting_user.id,
        username=acting_user.username,
        module="admin",
        action="role_permissions_updated",
        entity_type="role",
        entity_id=str(role.id),
        entity_label=f"{role.name} ({role.code})",
        old_values=serialize_audit_payload(
            {
                "role_id": role.id,
                "role_code": role.code,
                "permissions": sorted(previous_codes),
            }
        ),
        new_values=serialize_audit_payload(
            {
                "role_id": role.id,
                "role_code": role.code,
                "permissions": sorted(normalized_codes),
                "added_permissions": sorted(added_codes),
                "removed_permissions": sorted(removed_codes),
            }
        ),
        request_path=(request_path or "")[:500] or None,
        method=(request_method or "")[:20] or None,
        ip_address=(ip_address or "")[:255] or None,
        notes=f"affected_users={affected_user_count}",
    )
    db.add(audit_entry)
    db.flush()

    return RolePermissionUpdateResult(
        role_id=role.id,
        role_code=role.code,
        previous_permission_codes=tuple(sorted(previous_codes)),
        new_permission_codes=tuple(sorted(normalized_codes)),
        added_permission_codes=tuple(sorted(added_codes)),
        removed_permission_codes=tuple(sorted(removed_codes)),
        affected_user_count=affected_user_count,
        changed=True,
    )


def _active_permission_codes_for_role(db: Session, role_id: int) -> set[str]:
    return {
        code
        for code, in (
            db.query(Permission.code)
            .join(RolePermission, RolePermission.permission_id == Permission.id)
            .filter(RolePermission.role_id == role_id, Permission.active.is_(True))
            .all()
        )
    }


def _permission_version(role: Role, permission_codes: set[str]) -> str:
    updated_at = role.updated_at.isoformat(timespec="microseconds") if role.updated_at else ""
    payload = f"{role.id}|{updated_at}|{'|'.join(sorted(permission_codes))}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _secure_version_match(submitted: str, current: str) -> bool:
    return hmac.compare_digest(submitted, current)


def _normalize_origin(value: str | None) -> str | None:
    raw_value = (value or "").strip()
    if not raw_value or raw_value.lower() == "null":
        return None
    try:
        parsed = urlsplit(raw_value)
        scheme = parsed.scheme.lower()
        hostname = (parsed.hostname or "").lower()
        port = parsed.port
    except ValueError:
        return None
    if scheme not in {"http", "https"} or not hostname or parsed.username or parsed.password:
        return None
    default_port = 443 if scheme == "https" else 80
    port_suffix = f":{port}" if port is not None and port != default_port else ""
    display_hostname = f"[{hostname}]" if ":" in hostname else hostname
    return f"{scheme}://{display_hostname}{port_suffix}"


def _module_name(permission: Permission) -> str:
    if permission.code.startswith("admin.") or permission.code == "audit.view":
        return "Administración"
    if permission.code.startswith("b2b_customer_products."):
        return "Clientes y catálogo B2B"
    if permission.code.startswith("b2b_commercial_agreement."):
        return "Acuerdos comerciales"
    if permission.code.startswith("ar."):
        return "Cuentas por cobrar"
    if permission.code.startswith("product."):
        return "Productos"
    if permission.code.startswith("bom."):
        return "BOM"
    if permission.code.startswith("purchase_order."):
        return "Compras"
    if permission.code.startswith("production_order."):
        return "Producción"
    if permission.code.startswith("planning."):
        return "Planeación"
    if permission.code.startswith("accounting."):
        return "Contabilidad"
    if permission.code.startswith("reporting."):
        return "Reportes"
    if "loyverse" in permission.code or permission.code == "sales.preview_b2c_inventory_impact":
        return "Integraciones"
    if permission.code.startswith("inventory."):
        return "Inventario y Kardex"
    if permission.code.startswith("sales.") or permission.code.startswith("b2b_sales."):
        return "Ventas"
    return permission.module.replace("_", " ").title()


def _permission_row(permission: Permission, *, assigned: bool) -> PermissionRow:
    label, description = PERMISSION_TEXT_ES.get(
        permission.code,
        (
            permission.code.replace(".", " / ").replace("_", " "),
            "Permiso existente sin descripción funcional en español; revisar antes de asignar.",
        ),
    )
    action_group = _action_group(permission.action)
    if permission.code in NOT_BACKEND_ENFORCED:
        coverage_status = "not_enforced"
        coverage_label = "Sin control backend independiente"
        coverage_note = NOT_BACKEND_ENFORCED[permission.code]
    elif permission.code in SHARED_PERMISSION_NOTES:
        coverage_status = "shared"
        coverage_label = "Permiso compartido"
        coverage_note = SHARED_PERMISSION_NOTES[permission.code]
    else:
        coverage_status = "enforced"
        coverage_label = "Control backend activo"
        coverage_note = "Este permiso se valida en el backend de la operación correspondiente."
    return PermissionRow(
        code=permission.code,
        label=label,
        description=description,
        technical_description=permission.description,
        action_group=action_group,
        action_group_label={
            "view": "Ver",
            "create": "Crear",
            "edit": "Editar",
            "special": "Acción especial",
        }[action_group],
        coverage_status=coverage_status,
        coverage_label=coverage_label,
        coverage_note=coverage_note,
        assigned=assigned,
    )


def _action_group(action: str) -> str:
    normalized = (action or "").strip().lower()
    if normalized == "view":
        return "view"
    if normalized == "create":
        return "create"
    if normalized.startswith("edit") or normalized in {
        "users.manage",
        "roles.manage",
        "manage_budget",
        "manage_opening_balance",
    }:
        return "edit"
    return "special"


def _action_group_order(action_group: str) -> int:
    return {"view": 0, "create": 1, "edit": 2, "special": 3}.get(action_group, 4)
