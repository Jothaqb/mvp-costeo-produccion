import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.models import Base, Permission, Role, RolePermission, User, UserRole
from app.services.admin_user_service import (
    FUNCTIONAL_ROLE_CODES,
    FUNCTIONAL_ROLE_LABELS,
    get_user_functional_role_code,
    list_assignable_roles,
    replace_user_functional_role,
)
from app.services.auth_service import BASE_PERMISSION_DEFINITIONS, ensure_auth_seed_state


SALES_MANAGER_PERMISSION_CODES = {
    "sales.view",
    "product.view",
    "b2b_customer_products.edit_prices",
    "b2b_sales.cancel_invoice",
    "b2b_commercial_agreement.view",
    "b2b_commercial_agreement.edit",
    "b2b_commercial_agreement.generate",
}


class SalesManagerRoleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(bind=self.engine)
        with self.SessionLocal.begin() as db:
            db.add_all(
                (
                    Role(
                        code="general_operator",
                        name="General Operator",
                        description="General operator role.",
                        is_system=True,
                        active=True,
                    ),
                    Role(
                        code="general_approver",
                        name="General Approver",
                        description="General approver role.",
                        is_system=True,
                        active=True,
                    ),
                )
            )
            ensure_auth_seed_state(db)

    def tearDown(self) -> None:
        self.engine.dispose()

    def _permission_codes(self, db, role_code: str) -> set[str]:
        return {
            code
            for (code,) in (
                db.query(Permission.code)
                .join(RolePermission, RolePermission.permission_id == Permission.id)
                .join(Role, Role.id == RolePermission.role_id)
                .filter(Role.code == role_code)
                .all()
            )
        }

    def test_sales_manager_is_an_assignable_functional_role_with_label(self) -> None:
        self.assertIn("sales_manager", FUNCTIONAL_ROLE_CODES)
        self.assertEqual(FUNCTIONAL_ROLE_LABELS["sales_manager"], "Sales Manager")
        with self.SessionLocal() as db:
            roles = list_assignable_roles(db)
            self.assertEqual([role.code for role in roles], list(FUNCTIONAL_ROLE_CODES))
            sales_manager = next(role for role in roles if role.code == "sales_manager")
            self.assertEqual(sales_manager.name, "Sales Manager")
            self.assertTrue(sales_manager.active)
            self.assertTrue(sales_manager.is_system)

    def test_sales_manager_receives_only_catalog_permissions(self) -> None:
        with self.SessionLocal() as db:
            self.assertEqual(self._permission_codes(db, "sales_manager"), SALES_MANAGER_PERMISSION_CODES)

    def test_general_operator_does_not_receive_catalog_permissions(self) -> None:
        with self.SessionLocal() as db:
            operator_permissions = self._permission_codes(db, "general_operator")
            self.assertTrue(operator_permissions.isdisjoint(SALES_MANAGER_PERMISSION_CODES))

    def test_admin_keeps_all_base_permissions(self) -> None:
        expected_permissions = {definition[0] for definition in BASE_PERMISSION_DEFINITIONS}
        with self.SessionLocal() as db:
            self.assertEqual(self._permission_codes(db, "admin"), expected_permissions)

    def test_sales_manager_can_replace_general_operator_as_the_functional_role(self) -> None:
        with self.SessionLocal.begin() as db:
            user = User(
                username="santiago.ciscar",
                full_name="Santiago Ciscar",
                email="santiagojoseciscar@gmail.com",
                password_hash="test-only",
                is_active=True,
            )
            db.add(user)
            db.flush()
            operator = db.query(Role).filter(Role.code == "general_operator").one()
            db.add(UserRole(user_id=user.id, role_id=operator.id))
            db.flush()

            replace_user_functional_role(db, user, role_code="sales_manager")
            db.expire_all()

            self.assertEqual(get_user_functional_role_code(user), "sales_manager")
            self.assertEqual([link.role.code for link in user.user_role_links], ["sales_manager"])

    def test_sales_manager_seed_is_idempotent(self) -> None:
        with self.SessionLocal.begin() as db:
            ensure_auth_seed_state(db)
            ensure_auth_seed_state(db)
            self.assertEqual(db.query(Role).filter(Role.code == "sales_manager").count(), 1)
            role = db.query(Role).filter(Role.code == "sales_manager").one()
            self.assertEqual(
                db.query(RolePermission).filter(RolePermission.role_id == role.id).count(),
                len(SALES_MANAGER_PERMISSION_CODES),
            )


if __name__ == "__main__":
    unittest.main()
