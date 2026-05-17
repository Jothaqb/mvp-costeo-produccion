from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import app.models  # noqa: F401
from app.database import SessionLocal
from app.models import Permission, Role, RolePermission
from app.services.auth_service import ensure_auth_seed_state, sync_admin_role_permissions


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


@dataclass
class RoleChangeSummary:
    code: str
    created: bool
    added_permissions: list[str]
    removed_permissions: list[str]
    final_permissions: list[str]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Safely preview or sync ERP roles without creating nominal users."
    )
    parser.add_argument(
        "--sync-roles",
        action="store_true",
        help="Create or synchronize the approved operational roles.",
    )
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Preview role changes without writing them to the database.",
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


def _print_summary(mode: str, summaries: list[RoleChangeSummary]) -> None:
    print(f"Mode: {mode}")
    for summary in summaries:
        print(f"Role: {summary.code}")
        print(f"  created={summary.created}")
        print(f"  added_permissions={summary.added_permissions or []}")
        print(f"  removed_permissions={summary.removed_permissions or []}")
        print(f"  final_permissions={summary.final_permissions}")


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if not args.sync_roles:
        parser.print_help()
        return 1

    db = SessionLocal()
    try:
        ensure_auth_seed_state(db)
        summaries = [_sync_admin_role(db)]
        for role_code in sorted(ROLE_DEFINITIONS.keys()):
            summaries.append(_sync_operational_role(db, role_code))

        if args.preview:
            _print_summary("preview", summaries)
            db.rollback()
            print("Preview completed without writing role changes.")
            return 0

        db.commit()
        _print_summary("sync", summaries)
        print("Role synchronization completed successfully.")
        return 0
    except Exception as exc:
        db.rollback()
        print(f"Role synchronization failed: {exc}")
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
