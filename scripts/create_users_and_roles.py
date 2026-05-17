from __future__ import annotations

import argparse
import getpass
import secrets
import string
import sys
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import app.models  # noqa: F401
from app.database import SessionLocal
from app.models import Permission, Role, RolePermission, User
from app.services.auth_service import (
    assign_role_to_user,
    ensure_auth_seed_state,
    hash_password,
    sync_admin_role_permissions,
)


ROLE_DEFINITIONS = {
    "general_operator": {
        "name": "General Operator",
        "description": "General ERP operator without BOM, pricing, cost, audit, admin, or import privileges.",
        "is_system": False,
        "permissions": {
            "product.view",
            "product.create",
            "product.edit",
            "product.export",
            "bom.view",
            "sales.view",
            "sales.create",
            "sales.edit",
            "sales.invoice",
            "sales.export",
            "reporting.view",
            "reporting.export",
            "purchase_order.view",
            "purchase_order.create",
            "purchase_order.edit",
            "purchase_order.receive",
            "production_order.view",
            "production_order.create",
            "production_order.edit",
            "production_order.close",
            "inventory.view",
            "inventory.adjust",
            "planning.view",
            "planning.edit_parameters",
            "planning.edit_moq",
            "planning.edit_zones",
        },
    },
    "general_approver": {
        "name": "General Approver",
        "description": "General ERP operator with BOM and pricing approval privileges, but without cost, import, audit, or admin privileges.",
        "is_system": False,
        "permissions": {
            "product.view",
            "product.create",
            "product.edit",
            "product.export",
            "product.edit_prices",
            "bom.view",
            "bom.create",
            "bom.edit",
            "bom.delete",
            "b2b_customer_products.edit_prices",
            "sales.view",
            "sales.create",
            "sales.edit",
            "sales.invoice",
            "sales.export",
            "reporting.view",
            "reporting.export",
            "purchase_order.view",
            "purchase_order.create",
            "purchase_order.edit",
            "purchase_order.receive",
            "production_order.view",
            "production_order.create",
            "production_order.edit",
            "production_order.close",
            "inventory.view",
            "inventory.adjust",
            "planning.view",
            "planning.edit_parameters",
            "planning.edit_moq",
            "planning.edit_zones",
        },
    },
}

APPROVED_USERS = (
    {
        "full_name": "Olivia",
        "username": "olivia.rincon",
        "email": "morioly@gmail.com",
        "role": "general_operator",
    },
    {
        "full_name": "Andreina",
        "username": "andreina.rincon",
        "email": "greencornercr2@gmail.com",
        "role": "general_approver",
    },
    {
        "full_name": "Jonathan",
        "username": "jonathan.quirosb",
        "email": "greencornercr3@gmail.com",
        "role": "general_approver",
    },
    {
        "full_name": "Jonathan",
        "username": "jonathan.quiros",
        "email": "greencornercr1@gmail.com",
        "role": "admin",
    },
)

PASSWORD_ALPHABET = string.ascii_letters + string.digits + "!@#$%^&*()-_=+"


@dataclass
class RoleChangeSummary:
    code: str
    created: bool
    added_permissions: list[str]
    removed_permissions: list[str]
    final_permissions: list[str]


@dataclass
class UserPlan:
    username: str
    full_name: str
    email: str
    role_code: str
    must_change_password: bool
    is_active: bool


@dataclass
class UserCreateSummary:
    username: str
    full_name: str
    email: str
    role_code: str
    must_change_password: bool
    is_active: bool


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Safely preview or sync ERP roles and create approved nominal users."
    )
    parser.add_argument(
        "--sync-roles",
        action="store_true",
        help="Create or synchronize the approved operational roles.",
    )
    parser.add_argument(
        "--create-approved-users",
        action="store_true",
        help="Create the approved nominal users after synchronizing roles.",
    )
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Preview role and approved-user changes without writing them to the database.",
    )
    parser.add_argument(
        "--password-mode",
        choices=("generate", "getpass"),
        default="generate",
        help="Password handling mode for future user creation. Defaults to secure random generation.",
    )
    return parser


def _get_or_create_role(db, code: str, *, name: str, description: str | None, is_system: bool) -> tuple[Role, bool]:
    role = db.query(Role).filter(Role.code == code).one_or_none()
    created = False
    if role is None:
        role = Role(
            code=code,
            name=name,
            description=description,
            is_system=is_system,
            active=True,
        )
        db.add(role)
        db.flush()
        created = True
    else:
        role.name = name
        role.description = description
        role.is_system = is_system
        role.active = True
        db.flush()
    return role, created


def _sync_exact_role_permissions(db, role: Role, permission_codes: set[str]) -> tuple[list[str], list[str], list[str]]:
    permissions_by_code = {
        permission.code: permission for permission in db.query(Permission).filter(Permission.active.is_(True)).all()
    }
    missing = sorted(permission_codes - set(permissions_by_code.keys()))
    if missing:
        raise ValueError(
            f"Role '{role.code}' references undefined permissions: {', '.join(missing)}"
        )

    desired_ids = {permissions_by_code[code].id for code in permission_codes}
    existing_links = db.query(RolePermission).filter(RolePermission.role_id == role.id).all()
    existing_ids = {link.permission_id for link in existing_links}

    added_ids = desired_ids - existing_ids
    removed_ids = existing_ids - desired_ids

    for permission_id in sorted(added_ids):
        db.add(RolePermission(role_id=role.id, permission_id=permission_id))

    for link in existing_links:
        if link.permission_id in removed_ids:
            db.delete(link)

    db.flush()

    id_to_code = {permission.id: permission.code for permission in permissions_by_code.values()}
    final_permissions = sorted(permission_codes)
    added_permissions = sorted(id_to_code[permission_id] for permission_id in added_ids)
    removed_permissions = sorted(id_to_code[permission_id] for permission_id in removed_ids)
    return added_permissions, removed_permissions, final_permissions


def _sync_admin_role(db) -> RoleChangeSummary:
    admin_role = db.query(Role).filter(Role.code == "admin").one()
    before = {link.permission.code for link in admin_role.permission_links if link.permission is not None}
    sync_admin_role_permissions(db, admin_role)
    db.refresh(admin_role)
    after = {link.permission.code for link in admin_role.permission_links if link.permission is not None}
    return RoleChangeSummary(
        code="admin",
        created=False,
        added_permissions=sorted(after - before),
        removed_permissions=sorted(before - after),
        final_permissions=sorted(after),
    )


def _sync_operational_role(db, role_code: str) -> RoleChangeSummary:
    definition = ROLE_DEFINITIONS[role_code]
    role, created = _get_or_create_role(
        db,
        role_code,
        name=definition["name"],
        description=definition["description"],
        is_system=definition["is_system"],
    )
    added_permissions, removed_permissions, final_permissions = _sync_exact_role_permissions(
        db, role, set(definition["permissions"])
    )
    return RoleChangeSummary(
        code=role_code,
        created=created,
        added_permissions=added_permissions,
        removed_permissions=removed_permissions,
        final_permissions=final_permissions,
    )


def _build_approved_user_plans() -> list[UserPlan]:
    return [
        UserPlan(
            username=user["username"],
            full_name=user["full_name"],
            email=user["email"],
            role_code=user["role"],
            must_change_password=True,
            is_active=True,
        )
        for user in APPROVED_USERS
    ]


def _validate_user_plans(db, user_plans: list[UserPlan]) -> None:
    available_roles = {role.code for role in db.query(Role).filter(Role.active.is_(True)).all()}
    required_roles = {"admin", *ROLE_DEFINITIONS.keys()}
    missing_roles = sorted(required_roles - available_roles)
    if missing_roles:
        raise ValueError(f"Missing roles required for user creation: {', '.join(missing_roles)}")

    seen_usernames: set[str] = set()
    seen_emails: set[str] = set()
    for user_plan in user_plans:
        if user_plan.role_code not in available_roles:
            raise ValueError(f"Role '{user_plan.role_code}' is not available for user '{user_plan.username}'.")
        if user_plan.username in seen_usernames:
            raise ValueError(f"Duplicate username in approved list: {user_plan.username}")
        if user_plan.email in seen_emails:
            raise ValueError(f"Duplicate email in approved list: {user_plan.email}")
        seen_usernames.add(user_plan.username)
        seen_emails.add(user_plan.email)

        existing_user = db.query(User).filter(User.username == user_plan.username).one_or_none()
        if existing_user is not None:
            raise ValueError(f"Username already exists: {user_plan.username}")
        existing_email = db.query(User).filter(User.email == user_plan.email).one_or_none()
        if existing_email is not None:
            raise ValueError(f"Email already exists: {user_plan.email}")


def _generate_password(length: int = 20) -> str:
    while True:
        password = "".join(secrets.choice(PASSWORD_ALPHABET) for _ in range(length))
        if (
            any(char.islower() for char in password)
            and any(char.isupper() for char in password)
            and any(char.isdigit() for char in password)
            and any(char in "!@#$%^&*()-_=+" for char in password)
        ):
            return password


def _prompt_password_for_user(user_plan: UserPlan) -> str:
    while True:
        password = getpass.getpass(f"Temporary password for {user_plan.username}: ")
        confirm = getpass.getpass(f"Confirm temporary password for {user_plan.username}: ")
        if not password:
            print("Temporary password is required.")
            continue
        if password != confirm:
            print("Temporary password and confirmation do not match.")
            continue
        if len(password) < 10:
            print("Temporary password must be at least 10 characters long.")
            continue
        return password


def _create_user(db, user_plan: UserPlan, password: str) -> UserCreateSummary:
    role = db.query(Role).filter(Role.code == user_plan.role_code).one()
    user = User(
        username=user_plan.username,
        full_name=user_plan.full_name,
        email=user_plan.email,
        password_hash=hash_password(password),
        is_active=user_plan.is_active,
        must_change_password=user_plan.must_change_password,
    )
    db.add(user)
    db.flush()
    assign_role_to_user(db, user, role)
    return UserCreateSummary(
        username=user.username,
        full_name=user.full_name,
        email=user.email or "",
        role_code=role.code,
        must_change_password=user.must_change_password,
        is_active=user.is_active,
    )


def _print_role_summary(mode: str, summaries: list[RoleChangeSummary]) -> None:
    print(f"Mode: {mode}")
    for summary in summaries:
        print(f"Role: {summary.code}")
        print(f"  created={summary.created}")
        print(f"  added_permissions={summary.added_permissions or []}")
        print(f"  removed_permissions={summary.removed_permissions or []}")
        print(f"  final_permissions={summary.final_permissions}")


def _print_user_plan_summary(user_plans: list[UserPlan], *, password_mode: str) -> None:
    print("Approved users plan:")
    for user_plan in user_plans:
        print(
            f"  username={user_plan.username}, full_name={user_plan.full_name}, "
            f"email={user_plan.email}, role={user_plan.role_code}, "
            f"must_change_password={user_plan.must_change_password}, is_active={user_plan.is_active}"
        )
    print(f"  password_mode={password_mode}")


def _print_created_users(created_users: list[UserCreateSummary]) -> None:
    print("Created users:")
    for user in created_users:
        print(
            f"  username={user.username}, full_name={user.full_name}, email={user.email}, "
            f"role={user.role_code}, must_change_password={user.must_change_password}, is_active={user.is_active}"
        )


def _collect_generated_passwords(user_plans: list[UserPlan], password_mode: str) -> dict[str, str]:
    passwords: dict[str, str] = {}
    for user_plan in user_plans:
        if password_mode == "generate":
            passwords[user_plan.username] = _generate_password()
        else:
            passwords[user_plan.username] = _prompt_password_for_user(user_plan)
    return passwords


def _confirm_create_users(user_plans: list[UserPlan], *, password_mode: str) -> None:
    print("Create approved users summary:")
    _print_user_plan_summary(user_plans, password_mode=password_mode)
    confirmation = input("Type CREATE_USERS to continue: ").strip()
    if confirmation != "CREATE_USERS":
        raise ValueError("User creation cancelled: confirmation text did not match.")


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if not args.sync_roles and not args.create_approved_users:
        parser.print_help()
        return 1

    db = SessionLocal()
    try:
        ensure_auth_seed_state(db)
        role_summaries = [_sync_admin_role(db)]
        role_codes_to_sync = sorted(ROLE_DEFINITIONS.keys())
        if args.sync_roles or args.create_approved_users:
            for role_code in role_codes_to_sync:
                role_summaries.append(_sync_operational_role(db, role_code))

        user_plans: list[UserPlan] = []
        if args.create_approved_users:
            user_plans = _build_approved_user_plans()
            _validate_user_plans(db, user_plans)

        if args.preview:
            _print_role_summary("preview", role_summaries)
            if user_plans:
                _print_user_plan_summary(user_plans, password_mode=args.password_mode)
            db.rollback()
            print("Preview completed without writing role or user changes.")
            return 0

        created_users: list[UserCreateSummary] = []
        generated_passwords: dict[str, str] = {}

        if args.create_approved_users:
            _confirm_create_users(user_plans, password_mode=args.password_mode)
            generated_passwords = _collect_generated_passwords(user_plans, args.password_mode)
            for user_plan in user_plans:
                created_users.append(_create_user(db, user_plan, generated_passwords[user_plan.username]))

        db.commit()
        _print_role_summary("sync", role_summaries)
        if created_users:
            _print_created_users(created_users)
            print("Temporary passwords (shown once, do not persist them):")
            for username, password in generated_passwords.items():
                print(f"  {username}: {password}")
            print("Approved user creation completed successfully.")
        else:
            print("Role synchronization completed successfully.")
        return 0
    except Exception as exc:
        db.rollback()
        print(f"Role/user synchronization failed: {exc}")
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
