from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from fastapi import HTTPException, Request, status
from sqlalchemy.orm import Session, joinedload

from app.models import Permission, Role, RolePermission, User, UserRole, UserSession


SCRYPT_N = 2**14
SCRYPT_R = 8
SCRYPT_P = 1
SCRYPT_DKLEN = 32
SESSION_COOKIE_NAME = "erp_session"
SESSION_DURATION = timedelta(hours=12)
SESSION_REFRESH_INTERVAL = timedelta(minutes=5)
PUBLIC_PERMISSIONLESS_ROUTES = {
    "/login",
    "/logout",
    "/auth/bootstrap-admin",
}
BASE_PERMISSION_DEFINITIONS = (
    ("admin.users.manage", "admin", "users.manage", "Manage ERP users."),
    ("admin.roles.manage", "admin", "roles.manage", "Manage ERP roles and role assignments."),
    ("reporting.view", "reporting", "view", "View sales and operational reports."),
    ("reporting.export", "reporting", "export", "Export reporting data."),
    ("product.view", "product", "view", "View product master data."),
    ("product.create", "product", "create", "Create products."),
    ("product.edit", "product", "edit", "Edit product master data."),
    ("product.edit_prices", "product", "edit_prices", "Edit product prices."),
    ("product.edit_cost", "product", "edit_cost", "Edit product standard cost."),
    ("product.export", "product", "export", "Export product master data."),
    ("bom.view", "bom", "view", "View product BOMs."),
    ("bom.create", "bom", "create", "Create product BOMs."),
    ("bom.edit", "bom", "edit", "Edit product BOMs."),
    ("bom.delete", "bom", "delete", "Delete product BOM components."),
    ("b2b_customer_products.edit_prices", "b2b_customer_products", "edit_prices", "Edit B2B customer-specific pricing."),
    ("planning.view", "planning", "view", "View planning data."),
    ("planning.edit_parameters", "planning", "edit_parameters", "Edit planning parameters."),
    ("sales.view", "sales", "view", "View sales orders and customers."),
    ("sales.create", "sales", "create", "Create sales orders."),
    ("sales.edit", "sales", "edit", "Edit sales orders."),
    ("sales.invoice", "sales", "invoice", "Invoice sales orders."),
    ("sales.import", "sales", "import", "Import historical sales data."),
    ("sales.export", "sales", "export", "Export sales data."),
    ("purchase_order.view", "purchase_order", "view", "View purchase orders."),
    ("purchase_order.create", "purchase_order", "create", "Create purchase orders."),
    ("purchase_order.edit", "purchase_order", "edit", "Edit purchase orders."),
    ("purchase_order.receive", "purchase_order", "receive", "Receive or close purchase orders."),
    ("purchase_order.import", "purchase_order", "import", "Import historical purchase orders."),
    ("production_order.view", "production_order", "view", "View production orders."),
    ("production_order.create", "production_order", "create", "Create production orders."),
    ("production_order.edit", "production_order", "edit", "Edit production orders."),
    ("production_order.close", "production_order", "close", "Close production orders."),
    ("production_order.import", "production_order", "import", "Import historical production orders."),
    ("inventory.view", "inventory", "view", "View inventory balances and transactions."),
    ("inventory.adjust", "inventory", "adjust", "Create manual inventory adjustments."),
    ("audit.view", "audit", "view", "View audit trails."),
)


@dataclass(frozen=True)
class AuthenticatedSession:
    user: User
    session: UserSession
    permissions: set[str]


def hash_password(password: str) -> str:
    password_value = password or ""
    if not password_value.strip():
        raise ValueError("Password is required.")
    salt = secrets.token_bytes(16)
    derived_key = hashlib.scrypt(
        password_value.encode("utf-8"),
        salt=salt,
        n=SCRYPT_N,
        r=SCRYPT_R,
        p=SCRYPT_P,
        dklen=SCRYPT_DKLEN,
    )
    salt_encoded = base64.b64encode(salt).decode("ascii")
    hash_encoded = base64.b64encode(derived_key).decode("ascii")
    return f"scrypt${SCRYPT_N}${SCRYPT_R}${SCRYPT_P}${salt_encoded}${hash_encoded}"


def verify_password(password: str, password_hash: str) -> bool:
    try:
        algorithm, n_value, r_value, p_value, salt_encoded, hash_encoded = password_hash.split("$", 5)
        if algorithm != "scrypt":
            return False
        salt = base64.b64decode(salt_encoded.encode("ascii"))
        expected_hash = base64.b64decode(hash_encoded.encode("ascii"))
        candidate_hash = hashlib.scrypt(
            (password or "").encode("utf-8"),
            salt=salt,
            n=int(n_value),
            r=int(r_value),
            p=int(p_value),
            dklen=len(expected_hash),
        )
        return hmac.compare_digest(candidate_hash, expected_hash)
    except Exception:
        return False


def any_active_users(db: Session) -> bool:
    return db.query(User).filter(User.is_active.is_(True)).first() is not None


def is_local_request(request: Request) -> bool:
    client_host = request.client.host if request.client else ""
    return client_host in {"127.0.0.1", "::1", "localhost"}


def ensure_base_permissions(db: Session) -> list[Permission]:
    existing_permissions = {permission.code: permission for permission in db.query(Permission).all()}
    created = False
    for code, module, action, description in BASE_PERMISSION_DEFINITIONS:
        permission = existing_permissions.get(code)
        if permission is None:
            permission = Permission(
                code=code,
                module=module,
                action=action,
                description=description,
                active=True,
            )
            db.add(permission)
            existing_permissions[code] = permission
            created = True
        else:
            permission.module = module
            permission.action = action
            permission.description = description
            permission.active = True
    if created:
        db.flush()
    return list(existing_permissions.values())


def ensure_admin_role(db: Session) -> Role:
    role = db.query(Role).filter(Role.code == "admin").one_or_none()
    if role is None:
        role = Role(
            code="admin",
            name="Admin",
            description="Full ERP administrator role.",
            is_system=True,
            active=True,
        )
        db.add(role)
        db.flush()
    else:
        role.name = "Admin"
        role.description = "Full ERP administrator role."
        role.is_system = True
        role.active = True
    return role


def sync_admin_role_permissions(db: Session, role: Role) -> None:
    permissions = ensure_base_permissions(db)
    existing_permission_ids = {
        link.permission_id
        for link in db.query(RolePermission).filter(RolePermission.role_id == role.id).all()
    }
    for permission in permissions:
        if permission.id not in existing_permission_ids:
            db.add(RolePermission(role_id=role.id, permission_id=permission.id))
    db.flush()


def ensure_auth_seed_state(db: Session) -> None:
    ensure_base_permissions(db)
    admin_role = ensure_admin_role(db)
    sync_admin_role_permissions(db, admin_role)
    db.flush()


def assign_role_to_user(db: Session, user: User, role: Role) -> None:
    link = db.query(UserRole).filter(UserRole.user_id == user.id, UserRole.role_id == role.id).one_or_none()
    if link is None:
        db.add(UserRole(user_id=user.id, role_id=role.id))
        db.flush()


def bootstrap_admin_user(
    db: Session,
    *,
    username: str,
    full_name: str,
    email: str | None,
    password: str,
) -> User:
    if any_active_users(db):
        raise ValueError("Bootstrap admin is disabled because active users already exist.")

    ensure_auth_seed_state(db)
    admin_role = db.query(Role).filter(Role.code == "admin").one()

    normalized_username = username.strip()
    normalized_email = email.strip() if email else None
    if not normalized_username:
        raise ValueError("Username is required.")
    if not full_name.strip():
        raise ValueError("Full name is required.")
    existing_user = db.query(User).filter(User.username == normalized_username).one_or_none()
    if existing_user is not None:
        raise ValueError("Username already exists.")
    if normalized_email:
        existing_email = db.query(User).filter(User.email == normalized_email).one_or_none()
        if existing_email is not None:
            raise ValueError("Email already exists.")
    user = User(
        username=normalized_username,
        full_name=full_name.strip(),
        email=normalized_email or None,
        password_hash=hash_password(password),
        is_active=True,
        must_change_password=False,
    )
    db.add(user)
    db.flush()
    assign_role_to_user(db, user, admin_role)
    return user


def _hash_session_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def create_session(db: Session, user: User, request: Request) -> tuple[UserSession, str]:
    raw_token = secrets.token_urlsafe(32)
    expires_at = _utcnow() + SESSION_DURATION
    session = UserSession(
        user_id=user.id,
        session_token_hash=_hash_session_token(raw_token),
        created_at=datetime.utcnow(),
        last_seen_at=datetime.utcnow(),
        expires_at=expires_at.replace(tzinfo=None),
        revoked_at=None,
        ip_address=_client_ip_address(request),
        user_agent=(request.headers.get("user-agent") or "")[:500] or None,
    )
    user.last_login_at = datetime.utcnow()
    db.add(session)
    db.flush()
    return session, raw_token


def revoke_session(db: Session, token: str | None) -> None:
    if not token:
        return
    session = (
        db.query(UserSession)
        .filter(UserSession.session_token_hash == _hash_session_token(token), UserSession.revoked_at.is_(None))
        .one_or_none()
    )
    if session is not None:
        session.revoked_at = datetime.utcnow()
        db.flush()


def get_current_user_from_request(db: Session, request: Request) -> AuthenticatedSession | None:
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if not token:
        return None
    session = (
        db.query(UserSession)
        .options(
            joinedload(UserSession.user)
            .joinedload(User.user_role_links)
            .joinedload(UserRole.role)
            .joinedload(Role.permission_links)
            .joinedload(RolePermission.permission)
        )
        .filter(UserSession.session_token_hash == _hash_session_token(token))
        .one_or_none()
    )
    if session is None or session.revoked_at is not None:
        return None
    if session.expires_at <= datetime.utcnow():
        session.revoked_at = datetime.utcnow()
        db.flush()
        return None
    user = session.user
    if user is None or not user.is_active:
        return None

    if session.last_seen_at <= datetime.utcnow() - SESSION_REFRESH_INTERVAL:
        session.last_seen_at = datetime.utcnow()
        db.flush()

    permissions: set[str] = set()
    for user_role in user.user_role_links:
        role = user_role.role
        if role is None or not role.active:
            continue
        for permission_link in role.permission_links:
            permission = permission_link.permission
            if permission is not None and permission.active:
                permissions.add(permission.code)
    return AuthenticatedSession(user=user, session=session, permissions=permissions)


def user_has_permission(user: User | None, permission_code: str) -> bool:
    if user is None or not permission_code:
        return False
    for user_role in user.user_role_links:
        role = user_role.role
        if role is None or not role.active:
            continue
        for permission_link in role.permission_links:
            permission = permission_link.permission
            if permission is not None and permission.active and permission.code == permission_code:
                return True
    return False


def require_authenticated_user(request: Request) -> User:
    current_user = getattr(request.state, "current_user", None)
    if current_user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required.")
    return current_user


def require_permission(request: Request, permission_code: str) -> User:
    current_user = require_authenticated_user(request)
    if not can(request, permission_code):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="You do not have access to this action.")
    return current_user


def can(request: Request, permission_code: str) -> bool:
    permissions = getattr(request.state, "current_permissions", set()) or set()
    return permission_code in permissions


def get_login_redirect_target(request: Request) -> str:
    next_value = (request.query_params.get("next") or "").strip()
    if next_value.startswith("/") and not next_value.startswith("//"):
        return next_value
    return "/"


def is_public_path(path: str) -> bool:
    if path == "/static" or path.startswith("/static/"):
        return True
    return path in PUBLIC_PERMISSIONLESS_ROUTES


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _client_ip_address(request: Request) -> str | None:
    if request.client is None:
        return None
    return request.client.host
