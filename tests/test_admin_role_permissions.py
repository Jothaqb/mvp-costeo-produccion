from __future__ import annotations

import hashlib
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
from fastapi import HTTPException
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.models import AuditLog, Base, Permission, Role, RolePermission, User, UserRole, UserSession
from app.services.admin_role_permission_service import (
    NOT_BACKEND_ENFORCED,
    PERMISSION_TEXT_ES,
    RolePermissionCSRFError,
    RolePermissionConcurrentUpdateError,
    RolePermissionProtectedRoleError,
    RolePermissionValidationError,
    build_role_permissions_csrf_token,
    configured_public_origin,
    list_role_permission_panel_state,
    update_role_permissions,
    validate_role_permissions_csrf_token,
    validate_role_permissions_request_origin,
)
from app.services.audit_service import parse_audit_payload
from app.services.auth_service import (
    BASE_PERMISSION_DEFINITIONS,
    ensure_auth_seed_state,
    get_current_user_from_request,
    require_permission,
)


class AdminRolePermissionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)
        with self.Session.begin() as db:
            ensure_auth_seed_state(db)
            admin_role = db.query(Role).filter(Role.code == "admin").one()
            operator_role = db.query(Role).filter(Role.code == "general_operator").one()
            self.admin = User(
                username="role.admin",
                full_name="Role Admin",
                email="role.admin@example.com",
                password_hash="test-only",
                is_active=True,
            )
            self.operator = User(
                username="active.operator",
                full_name="Active Operator",
                email="active.operator@example.com",
                password_hash="test-only",
                is_active=True,
            )
            db.add_all((self.admin, self.operator))
            db.flush()
            db.add_all(
                (
                    UserRole(user_id=self.admin.id, role_id=admin_role.id),
                    UserRole(user_id=self.operator.id, role_id=operator_role.id),
                )
            )

    def tearDown(self) -> None:
        self.engine.dispose()

    def _role(self, db, code: str) -> Role:
        return db.query(Role).filter(Role.code == code).one()

    def _codes(self, db, role_code: str) -> set[str]:
        return {
            code
            for code, in (
                db.query(Permission.code)
                .join(RolePermission, RolePermission.permission_id == Permission.id)
                .join(Role, Role.id == RolePermission.role_id)
                .filter(Role.code == role_code)
                .all()
            )
        }

    def _update(self, db, *, role_code: str, new_codes: set[str]):
        role = self._role(db, role_code)
        state = list_role_permission_panel_state(db, selected_role_id=role.id)
        actor = db.query(User).filter(User.id == self.admin.id).one()
        return update_role_permissions(
            db,
            role_id=role.id,
            permission_codes=new_codes,
            expected_version=state.permission_version,
            acting_user=actor,
            request_path="/admin/roles-permissions",
            request_method="POST",
            ip_address="127.0.0.1",
        )

    def _request(self, permissions: set[str]):
        return SimpleNamespace(
            state=SimpleNamespace(current_user=self.admin, current_permissions=permissions),
            url=SimpleNamespace(path="/admin/roles-permissions"),
            method="GET",
            client=SimpleNamespace(host="127.0.0.1"),
        )

    def test_get_and_post_require_admin_roles_manage(self) -> None:
        denied_request = self._request(set())
        with self.assertRaises(HTTPException) as denied:
            require_permission(denied_request, "admin.roles.manage")
        self.assertEqual(denied.exception.status_code, 403)

        allowed_request = self._request({"admin.roles.manage"})
        self.assertEqual(
            require_permission(allowed_request, "admin.roles.manage").id,
            self.admin.id,
        )

        main_source = Path("app/main.py").read_text(encoding="utf-8")
        get_route = main_source.index('@app.get("/admin/roles-permissions"')
        post_route = main_source.index('@app.post("/admin/roles-permissions"')
        branding_route = main_source.index('@app.get("/admin/branding"')
        self.assertIn(
            'require_permission(request, "admin.roles.manage")',
            main_source[get_route:post_route],
        )
        self.assertIn(
            'require_permission(request, "admin.roles.manage")',
            main_source[post_route:branding_route],
        )
        post_source = main_source[post_route:branding_route]
        self.assertLess(
            post_source.index('require_permission(request, "admin.roles.manage")'),
            post_source.index("validate_role_permissions_csrf_token("),
        )
        self.assertLess(
            post_source.index("validate_role_permissions_request_origin("),
            post_source.index("update_role_permissions("),
        )

    def test_valid_csrf_token_and_trusted_render_origin_allow_update(self) -> None:
        session_token = "secure-random-session-token-a"
        csrf_token = build_role_permissions_csrf_token(session_token)
        validate_role_permissions_csrf_token(session_token, csrf_token)
        with mock.patch.dict(
            "os.environ",
            {"APP_BASE_URL": "", "RENDER_EXTERNAL_URL": "https://green-corner.onrender.com"},
            clear=False,
        ):
            self.assertEqual(configured_public_origin(), "https://green-corner.onrender.com")
            validate_role_permissions_request_origin(
                origin_header="https://green-corner.onrender.com",
                referer_header="https://green-corner.onrender.com/admin/roles-permissions",
            )

        with self.Session() as db:
            configured = self._codes(db, "general_operator") | {"reporting.view"}
            result = self._update(db, role_code="general_operator", new_codes=configured)
            db.commit()
        self.assertTrue(result.changed)

    def test_missing_invalid_and_other_session_csrf_tokens_are_rejected(self) -> None:
        session_token = "secure-random-session-token-a"
        with self.assertRaises(RolePermissionCSRFError):
            validate_role_permissions_csrf_token(session_token, None)
        with self.assertRaises(RolePermissionCSRFError):
            validate_role_permissions_csrf_token(session_token, "incorrect-token")
        other_session_token = "secure-random-session-token-b"
        token_from_other_session = build_role_permissions_csrf_token(other_session_token)
        with self.assertRaises(RolePermissionCSRFError):
            validate_role_permissions_csrf_token(session_token, token_from_other_session)

    def test_external_or_missing_origin_is_rejected(self) -> None:
        with mock.patch.dict(
            "os.environ",
            {"APP_BASE_URL": "https://erp.example.com", "RENDER_EXTERNAL_URL": ""},
            clear=False,
        ):
            with self.assertRaises(RolePermissionCSRFError):
                validate_role_permissions_request_origin(
                    origin_header="https://evil.example",
                    referer_header=None,
                )
            with self.assertRaises(RolePermissionCSRFError):
                validate_role_permissions_request_origin(
                    origin_header=None,
                    referer_header=None,
                )

    def test_render_origin_ignores_untrusted_proxy_and_host_values(self) -> None:
        with mock.patch.dict(
            "os.environ",
            {"APP_BASE_URL": "https://erp.green-corner.example", "RENDER_EXTERNAL_URL": ""},
            clear=False,
        ):
            validate_role_permissions_request_origin(
                origin_header="https://erp.green-corner.example",
                referer_header=None,
            )
            with self.assertRaises(RolePermissionCSRFError):
                validate_role_permissions_request_origin(
                    origin_header="https://attacker.example",
                    referer_header="https://attacker.example/admin/roles-permissions",
                )

    def test_csrf_failure_changes_neither_permissions_nor_audit(self) -> None:
        with self.Session() as db:
            before_codes = self._codes(db, "general_operator")
            before_audit_count = db.query(AuditLog).count()

        with self.assertRaises(RolePermissionCSRFError):
            validate_role_permissions_csrf_token(
                "secure-random-session-token-a",
                "invalid-token",
            )

        with self.Session() as db:
            self.assertEqual(self._codes(db, "general_operator"), before_codes)
            self.assertEqual(db.query(AuditLog).count(), before_audit_count)

    def test_panel_loads_current_roles_permissions_counts_and_coverage(self) -> None:
        with self.Session() as db:
            sales_manager = self._role(db, "sales_manager")
            state = list_role_permission_panel_state(db, selected_role_id=sales_manager.id)
            selected_codes = self._codes(db, "sales_manager")

        role_codes = {role.code for role in state.roles}
        self.assertTrue({"admin", "sales_manager", "general_approver", "general_operator"}.issubset(role_codes))
        admin_option = next(role for role in state.roles if role.code == "admin")
        operator_option = next(role for role in state.roles if role.code == "general_operator")
        self.assertTrue(admin_option.protected)
        self.assertEqual(operator_option.user_count, 1)
        self.assertEqual(operator_option.active_user_count, 1)
        rows = [row for group in state.permission_groups for row in group.permissions]
        rows_by_code = {row.code: row for row in rows}
        self.assertEqual(rows_by_code["sales.view"].coverage_status, "shared")
        for permission_code in NOT_BACKEND_ENFORCED:
            self.assertEqual(rows_by_code[permission_code].coverage_status, "not_enforced")
        self.assertEqual(
            set(PERMISSION_TEXT_ES),
            {code for code, _module, _action, _description in BASE_PERMISSION_DEFINITIONS},
        )
        self.assertEqual(
            set(state.assigned_permission_codes),
            selected_codes,
        )

    def test_can_add_and_revoke_permissions_with_atomic_audit(self) -> None:
        with self.Session() as db:
            original = self._codes(db, "general_operator")
            new_codes = (original - {"ar.export"}) | {"reporting.view"}
            result = self._update(db, role_code="general_operator", new_codes=new_codes)
            db.commit()

        self.assertEqual(result.added_permission_codes, ("reporting.view",))
        self.assertEqual(result.removed_permission_codes, ("ar.export",))
        self.assertEqual(result.affected_user_count, 1)
        with self.Session() as db:
            self.assertEqual(self._codes(db, "general_operator"), new_codes)
            audit = db.query(AuditLog).filter(AuditLog.action == "role_permissions_updated").one()
            self.assertEqual(audit.user_id, self.admin.id)
            self.assertEqual(audit.username, "role.admin")
            old_values = parse_audit_payload(audit.old_values)
            new_values = parse_audit_payload(audit.new_values)
            self.assertEqual(set(old_values["permissions"]), original)
            self.assertEqual(set(new_values["permissions"]), new_codes)
            self.assertEqual(new_values["added_permissions"], ["reporting.view"])
            self.assertEqual(new_values["removed_permissions"], ["ar.export"])

    def test_non_admin_role_can_be_saved_with_no_permissions(self) -> None:
        with self.Session() as db:
            result = self._update(db, role_code="general_operator", new_codes=set())
            db.commit()
        self.assertTrue(result.changed)
        with self.Session() as db:
            self.assertEqual(self._codes(db, "general_operator"), set())

    def test_invalid_or_inactive_permission_codes_are_rejected(self) -> None:
        with self.Session() as db:
            role = self._role(db, "general_operator")
            state = list_role_permission_panel_state(db, selected_role_id=role.id)
            actor = db.query(User).filter(User.id == self.admin.id).one()
            with self.assertRaises(RolePermissionValidationError):
                update_role_permissions(
                    db,
                    role_id=role.id,
                    permission_codes={"missing.permission"},
                    expected_version=state.permission_version,
                    acting_user=actor,
                    request_path="/admin/roles-permissions",
                    request_method="POST",
                    ip_address=None,
                )
            db.rollback()
        with self.Session() as db:
            self.assertEqual(self._codes(db, "general_operator"), {"ar.view", "ar.record_payment", "ar.export"})

        with self.Session() as db:
            inactive = db.query(Permission).filter(Permission.code == "reporting.view").one()
            inactive.active = False
            db.commit()
        with self.Session() as db:
            role = self._role(db, "general_operator")
            state = list_role_permission_panel_state(db, selected_role_id=role.id)
            actor = db.query(User).filter(User.id == self.admin.id).one()
            with self.assertRaises(RolePermissionValidationError):
                update_role_permissions(
                    db,
                    role_id=role.id,
                    permission_codes={"reporting.view"},
                    expected_version=state.permission_version,
                    acting_user=actor,
                    request_path="/admin/roles-permissions",
                    request_method="POST",
                    ip_address=None,
                )

    def test_admin_role_is_visible_but_cannot_be_modified(self) -> None:
        with self.Session() as db:
            admin_role = self._role(db, "admin")
            state = list_role_permission_panel_state(db, selected_role_id=admin_role.id)
            actor = db.query(User).filter(User.id == self.admin.id).one()
            self.assertTrue(state.selected_role.protected)
            with self.assertRaises(RolePermissionProtectedRoleError):
                update_role_permissions(
                    db,
                    role_id=admin_role.id,
                    permission_codes=set(),
                    expected_version=state.permission_version,
                    acting_user=actor,
                    request_path="/admin/roles-permissions",
                    request_method="POST",
                    ip_address=None,
                )

    def test_stale_version_cannot_overwrite_another_administrator_change(self) -> None:
        with self.Session() as db:
            role = self._role(db, "general_operator")
            stale_version = list_role_permission_panel_state(db, selected_role_id=role.id).permission_version
            actor = db.query(User).filter(User.id == self.admin.id).one()
            current_codes = self._codes(db, "general_operator")
            update_role_permissions(
                db,
                role_id=role.id,
                permission_codes=current_codes | {"reporting.view"},
                expected_version=stale_version,
                acting_user=actor,
                request_path="/admin/roles-permissions",
                request_method="POST",
                ip_address=None,
            )
            db.commit()

        with self.Session() as db:
            role = self._role(db, "general_operator")
            actor = db.query(User).filter(User.id == self.admin.id).one()
            with self.assertRaises(RolePermissionConcurrentUpdateError):
                update_role_permissions(
                    db,
                    role_id=role.id,
                    permission_codes={"ar.view"},
                    expected_version=stale_version,
                    acting_user=actor,
                    request_path="/admin/roles-permissions",
                    request_method="POST",
                    ip_address=None,
                )

    def test_audit_failure_rolls_back_permission_changes(self) -> None:
        with self.Session() as db:
            original = self._codes(db, "general_operator")

        def fail_audit_insert(_mapper, _connection, _target):
            raise RuntimeError("simulated audit failure")

        event.listen(AuditLog, "before_insert", fail_audit_insert)
        try:
            with self.Session() as db:
                with self.assertRaises(RuntimeError):
                    self._update(
                        db,
                        role_code="general_operator",
                        new_codes=original | {"reporting.view"},
                    )
                    db.commit()
                db.rollback()
        finally:
            event.remove(AuditLog, "before_insert", fail_audit_insert)

        with self.Session() as db:
            self.assertEqual(self._codes(db, "general_operator"), original)
            self.assertEqual(db.query(AuditLog).count(), 0)

    def test_manual_configuration_persists_after_seed_runs_again(self) -> None:
        with self.Session() as db:
            original = self._codes(db, "sales_manager")
            configured = (original - {"sales.view"}) | {"reporting.view"}
            self._update(db, role_code="sales_manager", new_codes=configured)
            db.commit()
        with self.Session.begin() as db:
            ensure_auth_seed_state(db)
        with self.Session() as db:
            self.assertEqual(self._codes(db, "sales_manager"), configured)

    def test_active_session_reads_new_effective_permissions_on_next_request(self) -> None:
        token = "active-session-token"
        with self.Session.begin() as db:
            db.add(
                UserSession(
                    user_id=self.operator.id,
                    session_token_hash=hashlib.sha256(token.encode("utf-8")).hexdigest(),
                    created_at=datetime.utcnow(),
                    last_seen_at=datetime.utcnow(),
                    expires_at=datetime.utcnow() + timedelta(hours=1),
                    revoked_at=None,
                )
            )
        request = SimpleNamespace(cookies={"erp_session": token})
        with self.Session() as db:
            before = get_current_user_from_request(db, request)
            self.assertNotIn("reporting.view", before.permissions)

        with self.Session() as db:
            configured = self._codes(db, "general_operator") | {"reporting.view"}
            self._update(db, role_code="general_operator", new_codes=configured)
            db.commit()

        with self.Session() as db:
            after = get_current_user_from_request(db, request)
            self.assertIn("reporting.view", after.permissions)

    def test_updating_one_role_does_not_change_other_roles(self) -> None:
        with self.Session() as db:
            before = {
                code: self._codes(db, code)
                for code in ("admin", "sales_manager", "general_approver", "general_operator")
            }
            self._update(db, role_code="general_operator", new_codes={"ar.view"})
            db.commit()
        with self.Session() as db:
            self.assertEqual(self._codes(db, "admin"), before["admin"])
            self.assertEqual(self._codes(db, "sales_manager"), before["sales_manager"])
            self.assertEqual(self._codes(db, "general_approver"), before["general_approver"])
            self.assertEqual(self._codes(db, "general_operator"), {"ar.view"})

    def test_template_warns_before_save_and_marks_coverage_limitations(self) -> None:
        template = Path("app/templates/admin_roles_permissions.html").read_text(encoding="utf-8")
        self.assertIn(
            "Estos cambios modificarán los accesos de todos los usuarios asociados al perfil seleccionado.",
            template,
        )
        self.assertIn("window.confirm", template)
        self.assertIn('name="csrf_token"', template)
        self.assertIn("coverage_status", template)
        base_template = Path("app/templates/base.html").read_text(encoding="utf-8")
        self.assertIn('can(request, "admin.roles.manage")', base_template)
        self.assertIn('href="/admin/roles-permissions"', base_template)


if __name__ == "__main__":
    unittest.main()
