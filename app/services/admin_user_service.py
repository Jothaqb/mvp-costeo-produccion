from __future__ import annotations

import secrets
from dataclasses import dataclass

from sqlalchemy import func
from sqlalchemy.orm import Session, joinedload

from app.models import Role, User, UserRole
from app.services.auth_service import hash_password
from app.services.password_reset_service import (
    PasswordResetIssueResult,
    issue_password_setup_for_user,
    revoke_all_active_sessions_for_user,
)


FUNCTIONAL_ROLE_CODES = ("admin", "general_operator", "general_approver")


class AdminUserValidationError(Exception):
    pass


class AdminUserConfigurationError(Exception):
    pass


class AdminUserNotFoundError(Exception):
    pass


@dataclass(frozen=True)
class AdminUserUpdateResult:
    revoked_session_count: int
    previous_role_code: str | None
    current_role_code: str


def list_admin_users(db: Session) -> list[User]:
    return (
        db.query(User)
        .options(joinedload(User.user_role_links).joinedload(UserRole.role))
        .order_by(func.lower(User.username), User.id)
        .all()
    )


def get_admin_user_for_edit(db: Session, user_id: int) -> User:
    user = (
        db.query(User)
        .options(joinedload(User.user_role_links).joinedload(UserRole.role))
        .filter(User.id == user_id)
        .one_or_none()
    )
    if user is None:
        raise AdminUserNotFoundError("User not found.")
    return user


def list_assignable_roles(db: Session) -> list[Role]:
    roles_by_code = validate_required_functional_roles_exist(db)
    return [roles_by_code[code] for code in FUNCTIONAL_ROLE_CODES]


def validate_required_functional_roles_exist(db: Session) -> dict[str, Role]:
    roles = (
        db.query(Role)
        .filter(Role.code.in_(FUNCTIONAL_ROLE_CODES), Role.active.is_(True))
        .order_by(Role.id)
        .all()
    )
    roles_by_code = {role.code: role for role in roles}
    missing_codes = [code for code in FUNCTIONAL_ROLE_CODES if code not in roles_by_code]
    if missing_codes:
        raise AdminUserConfigurationError(
            "Missing required functional roles in this environment: " + ", ".join(missing_codes)
        )
    return roles_by_code


def create_admin_user(
    db: Session,
    *,
    username: str,
    full_name: str,
    email: str,
    is_active: bool,
    role_code: str,
    requested_ip: str | None,
    requested_user_agent: str | None,
    app_base_url: str,
) -> tuple[User, PasswordResetIssueResult]:
    normalized_username = (username or "").strip()
    normalized_full_name = (full_name or "").strip()
    normalized_email = normalize_admin_email(email)
    validate_required_functional_roles_exist(db)
    _validate_functional_role_code(role_code)

    if not normalized_username:
        raise AdminUserValidationError("Username is required.")
    if not normalized_full_name:
        raise AdminUserValidationError("Full name is required.")
    if not normalized_email:
        raise AdminUserValidationError("Email is required.")

    validate_email_basic_format(normalized_email)
    ensure_unique_username(db, normalized_username)
    ensure_unique_email(db, normalized_email)

    user = User(
        username=normalized_username,
        full_name=normalized_full_name,
        email=normalized_email,
        password_hash=hash_password(secrets.token_urlsafe(24)),
        is_active=bool(is_active),
        must_change_password=True,
    )
    db.add(user)
    db.flush()

    replace_user_functional_role(db, user, role_code=role_code)
    setup_result = send_password_setup_link_for_user(
        db,
        user,
        requested_ip=requested_ip,
        requested_user_agent=requested_user_agent,
        app_base_url=app_base_url,
    )
    db.flush()
    return user, setup_result


def update_admin_user(
    db: Session,
    *,
    user: User,
    acting_user: User,
    full_name: str,
    email: str,
    is_active: bool,
    role_code: str,
) -> AdminUserUpdateResult:
    normalized_full_name = (full_name or "").strip()
    normalized_email = normalize_admin_email(email)
    validate_required_functional_roles_exist(db)
    _validate_functional_role_code(role_code)

    if not normalized_full_name:
        raise AdminUserValidationError("Full name is required.")
    if not normalized_email:
        raise AdminUserValidationError("Email is required.")

    validate_email_basic_format(normalized_email)
    ensure_unique_email(db, normalized_email, exclude_user_id=user.id)

    previous_role_code = get_user_functional_role_code(user)

    if not is_active:
        ensure_not_self_deactivate(target_user=user, acting_user=acting_user)

    ensure_not_last_active_admin(
        db,
        target_user=user,
        new_role_code=role_code,
        new_is_active=bool(is_active),
    )

    revoked_session_count = 0
    user.full_name = normalized_full_name
    user.email = normalized_email
    user.is_active = bool(is_active)

    replace_user_functional_role(db, user, role_code=role_code)

    if not user.is_active:
        revoked_session_count = revoke_all_active_sessions_for_user(db, user.id)

    db.flush()
    return AdminUserUpdateResult(
        revoked_session_count=revoked_session_count,
        previous_role_code=previous_role_code,
        current_role_code=role_code,
    )


def replace_user_functional_role(db: Session, user: User, role_code: str) -> None:
    roles_by_code = validate_required_functional_roles_exist(db)
    target_role = roles_by_code.get(role_code)
    if target_role is None:
        raise AdminUserValidationError("Selected role is not available.")

    existing_links = db.query(UserRole).filter(UserRole.user_id == user.id).all()
    has_target_link = False
    for link in existing_links:
        if link.role_id == target_role.id:
            has_target_link = True
            continue
        db.delete(link)

    if not has_target_link:
        db.add(UserRole(user_id=user.id, role_id=target_role.id))
    db.flush()


def deactivate_admin_user(db: Session, target_user: User, acting_user: User) -> int:
    ensure_not_self_deactivate(target_user=target_user, acting_user=acting_user)
    ensure_not_last_active_admin(
        db,
        target_user=target_user,
        new_role_code=get_user_functional_role_code(target_user),
        new_is_active=False,
    )
    target_user.is_active = False
    revoked_session_count = revoke_all_active_sessions_for_user(db, target_user.id)
    db.flush()
    return revoked_session_count


def activate_admin_user(db: Session, target_user: User) -> User:
    if get_user_functional_role_code(target_user) is None:
        raise AdminUserValidationError("You must assign a functional role before activating this user.")
    target_user.is_active = True
    db.flush()
    return target_user


def send_password_setup_link_for_user(
    db: Session,
    user: User,
    *,
    requested_ip: str | None,
    requested_user_agent: str | None,
    app_base_url: str,
) -> PasswordResetIssueResult:
    normalized_email = normalize_admin_email(user.email or "")
    if not normalized_email:
        raise AdminUserValidationError("A valid email is required before sending a password setup link.")
    validate_email_basic_format(normalized_email)
    ensure_unique_email(db, normalized_email, exclude_user_id=user.id)
    user.email = normalized_email
    return issue_password_setup_for_user(
        db,
        user=user,
        requested_ip=requested_ip,
        requested_user_agent=requested_user_agent,
        app_base_url=app_base_url,
    )


def count_active_admin_users(db: Session) -> int:
    return (
        db.query(User.id)
        .join(UserRole, UserRole.user_id == User.id)
        .join(Role, Role.id == UserRole.role_id)
        .filter(User.is_active.is_(True), Role.active.is_(True), Role.code == "admin")
        .distinct()
        .count()
    )


def ensure_not_last_active_admin(
    db: Session,
    *,
    target_user: User,
    new_role_code: str | None,
    new_is_active: bool,
) -> None:
    current_role_code = get_user_functional_role_code(target_user)
    is_current_active_admin = target_user.is_active and current_role_code == "admin"
    will_be_active_admin = bool(new_is_active) and new_role_code == "admin"

    if is_current_active_admin and not will_be_active_admin and count_active_admin_users(db) <= 1:
        if not new_is_active:
            raise AdminUserValidationError("Cannot deactivate the last active admin user.")
        raise AdminUserValidationError("Cannot remove admin role from the last active admin user.")


def ensure_not_self_deactivate(*, target_user: User, acting_user: User) -> None:
    if target_user.id == acting_user.id:
        raise AdminUserValidationError("You cannot deactivate your own user account.")


def ensure_unique_username(db: Session, username: str, exclude_user_id: int | None = None) -> None:
    normalized_username = (username or "").strip().lower()
    if not normalized_username:
        raise AdminUserValidationError("Username is required.")

    query = db.query(User).filter(func.lower(User.username) == normalized_username)
    if exclude_user_id is not None:
        query = query.filter(User.id != exclude_user_id)
    if query.one_or_none() is not None:
        raise AdminUserValidationError("Username already exists.")


def ensure_unique_email(db: Session, email: str, exclude_user_id: int | None = None) -> None:
    normalized_email = normalize_admin_email(email)
    if not normalized_email:
        raise AdminUserValidationError("Email is required.")

    query = db.query(User).filter(User.email.is_not(None), func.lower(User.email) == normalized_email)
    if exclude_user_id is not None:
        query = query.filter(User.id != exclude_user_id)
    if query.one_or_none() is not None:
        raise AdminUserValidationError("Email already exists.")


def validate_email_basic_format(email: str) -> None:
    value = normalize_admin_email(email)
    if not value or "@" not in value:
        raise AdminUserValidationError("Email format is invalid.")
    local_part, _, domain_part = value.rpartition("@")
    if not local_part or not domain_part or domain_part.startswith(".") or domain_part.endswith("."):
        raise AdminUserValidationError("Email format is invalid.")
    if "." not in domain_part:
        raise AdminUserValidationError("Email format is invalid.")


def get_user_functional_role_code(user: User) -> str | None:
    user_role_codes = {
        user_role.role.code
        for user_role in user.user_role_links
        if user_role.role is not None and user_role.role.code in FUNCTIONAL_ROLE_CODES
    }
    for role_code in FUNCTIONAL_ROLE_CODES:
        if role_code in user_role_codes:
            return role_code
    return None


def normalize_admin_email(email: str) -> str:
    return (email or "").strip().lower()


def _validate_functional_role_code(role_code: str) -> None:
    if role_code not in FUNCTIONAL_ROLE_CODES:
        raise AdminUserValidationError("You must assign exactly one functional role.")
