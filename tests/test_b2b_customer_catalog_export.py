import csv
import io
import os
import tempfile
import unittest
from datetime import timezone
from decimal import Decimal
from pathlib import Path
from unittest import mock

from fastapi import HTTPException


_PREVIOUS_DATABASE_URL = os.environ.get("DATABASE_URL")
_TEMP_DB = tempfile.NamedTemporaryFile(prefix="b2b_catalog_export_", suffix=".db", delete=False)
_TEMP_DB.close()
os.environ["DATABASE_URL"] = f"sqlite:///{Path(_TEMP_DB.name).as_posix()}"

with mock.patch("zoneinfo.ZoneInfo", return_value=timezone.utc):
    from app import main  # noqa: E402
from app.models import AppSettings, B2BCustomer, B2BCustomerProduct, Channel, Permission, Role, RolePermission  # noqa: E402
from app.services.b2b_catalog_export_service import format_crc_catalog_price  # noqa: E402


class B2BCustomerCatalogExportTests(unittest.TestCase):
    @classmethod
    def tearDownClass(cls) -> None:
        main.engine.dispose()
        Path(_TEMP_DB.name).unlink(missing_ok=True)
        if _PREVIOUS_DATABASE_URL is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = _PREVIOUS_DATABASE_URL

    def setUp(self) -> None:
        with main.SessionLocal.begin() as db:
            db.query(B2BCustomerProduct).delete()
            db.query(B2BCustomer).delete()
            db.query(Channel).delete()
            db.query(AppSettings).delete()
            channel = Channel(name="Tiendas Naturales (Minisuper)", active=True, applies_to_b2b=True)
            db.add(channel)
            db.flush()
            customer_a = B2BCustomer(
                customer_name="Café Cliente A",
                legal_name="Grupo Café Cliente A S.A.",
                legal_id="3-101-123456",
                phone="24797018",
                address="Costado Norte del Parque, La Fortuna\nAlajuela",
                province="Alajuela",
                canton="San Carlos",
                district="La Fortuna",
                credit_days=10,
                channel_id=channel.id,
                active=True,
            )
            customer_b = B2BCustomer(customer_name="Cliente B")
            db.add_all((customer_a, customer_b))
            db.flush()
            self.customer_a_id = customer_a.id
            self.customer_b_id = customer_b.id
            db.add_all(
                (
                    B2BCustomerProduct(
                        customer_id=customer_a.id,
                        sku="A-001",
                        description="Té limón",
                        distributor_price=Decimal("1250.5000"),
                        active=True,
                    ),
                    B2BCustomerProduct(
                        customer_id=customer_a.id,
                        sku="A-002",
                        description="Producto inactivo",
                        distributor_price=Decimal("800.0000"),
                        active=False,
                    ),
                    B2BCustomerProduct(
                        customer_id=customer_b.id,
                        sku="B-SECRET",
                        description="Otro cliente",
                        distributor_price=Decimal("9999.0000"),
                        active=True,
                    ),
                )
            )

    def _export_customer(self, customer_id: int):
        with main.SessionLocal() as db, mock.patch.object(main, "require_permission") as permission:
            response = main.export_b2b_customer_products_csv(
                customer_id=customer_id,
                request=mock.Mock(),
                db=db,
            )
            permission.assert_called_once_with(mock.ANY, "sales.view")
            self.assertFalse(db.dirty)
            self.assertFalse(db.new)
            self.assertFalse(db.deleted)
            return response

    def _role_permissions(self, role_code: str) -> set[str]:
        with main.SessionLocal() as db:
            return {
                code
                for (code,) in (
                    db.query(Permission.code)
                    .join(RolePermission, RolePermission.permission_id == Permission.id)
                    .join(Role, Role.id == RolePermission.role_id)
                    .filter(Role.code == role_code, Role.active.is_(True), Permission.active.is_(True))
                    .all()
                )
            }

    def _export_customer_pdf(self, customer_id: int, permission_codes: set[str]):
        request = self._request_with_permissions(permission_codes)
        with main.SessionLocal() as db:
            response = main.export_b2b_customer_products_pdf(
                customer_id=customer_id,
                request=request,
                db=db,
            )
            self.assertFalse(db.dirty)
            self.assertFalse(db.new)
            self.assertFalse(db.deleted)
            return response

    @staticmethod
    def _request_with_permissions(permission_codes: set[str]):
        request = mock.Mock()
        request.state.current_user = mock.Mock(id=123, username="catalog-test-user")
        request.state.current_permissions = permission_codes
        return request

    def test_export_is_utf8_bom_and_strictly_scoped_to_customer(self) -> None:
        response = self._export_customer(self.customer_a_id)
        self.assertTrue(response.body.startswith(b"\xef\xbb\xbf"))
        self.assertEqual(
            response.headers["content-disposition"],
            f'attachment; filename="b2b_customer_{self.customer_a_id}_catalog.csv"',
        )
        rows = list(csv.reader(io.StringIO(response.body.decode("utf-8-sig"))))
        self.assertEqual(rows[0], ["Catalogo:", "Café Cliente A"])
        self.assertEqual(rows[1], [])
        self.assertNotIn(["Campo", "Valor", "Campo", "Valor"], rows)
        self.assertEqual(rows[2], ["Customer name", "Café Cliente A", "Legal name", "Grupo Café Cliente A S.A."])
        self.assertEqual(rows[3], ["Legal ID", "3-101-123456", "Phone", "24797018"])
        self.assertEqual(
            rows[4],
            [
                "Address",
                "Costado Norte del Parque, La Fortuna\nAlajuela",
                "B2B Channel",
                "Tiendas Naturales (Minisuper)",
            ],
        )
        self.assertEqual(rows[5], ["Province", "Alajuela", "Credit days", "10"])
        self.assertEqual(rows[6], ["Canton", "San Carlos", "Active", "yes"])
        self.assertEqual(rows[7], ["District", "La Fortuna", "", ""])
        self.assertEqual(rows[8], [])
        self.assertEqual(rows[9], ["Codigo", "Descripcion", "Precio", "Estado"])
        self.assertEqual(
            rows[10:],
            [
                ["A-001", "Té limón", "1250.5000", "Activo"],
                ["A-002", "Producto inactivo", "800.0000", "Inactivo"],
            ],
        )
        self.assertIn('"Costado Norte del Parque, La Fortuna', response.body.decode("utf-8-sig"))
        self.assertNotIn("B-SECRET", response.body.decode("utf-8-sig"))

    def test_empty_customer_catalog_exports_only_headers(self) -> None:
        with main.SessionLocal.begin() as db:
            empty_customer = B2BCustomer(customer_name="Sin catálogo")
            db.add(empty_customer)
            db.flush()
            empty_customer_id = empty_customer.id

        response = self._export_customer(empty_customer_id)
        rows = list(csv.reader(io.StringIO(response.body.decode("utf-8-sig"))))
        self.assertEqual(rows[0], ["Catalogo:", "Sin catálogo"])
        self.assertEqual(rows[4], ["Address", "", "B2B Channel", ""])
        self.assertEqual(rows[8], [])
        self.assertEqual(rows[9], ["Codigo", "Descripcion", "Precio", "Estado"])
        self.assertEqual(len(rows), 10)

    def test_templates_show_catalog_and_download_labels(self) -> None:
        customer_list = Path("app/templates/b2b_customers_list.html").read_text(encoding="utf-8")
        catalog = Path("app/templates/b2b_customer_products.html").read_text(encoding="utf-8")
        self.assertIn(">Catálogo</a>", customer_list)
        self.assertNotIn(">Products</a>", customer_list)
        self.assertIn("Descargar CSV", catalog)
        self.assertIn("Descargar PDF", catalog)
        self.assertIn("products/export.csv", catalog)
        self.assertIn("products/export.pdf", catalog)
        self.assertEqual(catalog.count("products/export.pdf"), 1)

    def test_pdf_endpoint_exists_and_admin_and_sales_manager_can_download(self) -> None:
        self.assertEqual(
            sum(
                getattr(route, "path", "") == "/b2b/customers/{customer_id}/products/export.pdf"
                for route in main.app.routes
            ),
            1,
        )
        for role_code in ("admin", "sales_manager"):
            with self.subTest(role_code=role_code):
                response = self._export_customer_pdf(self.customer_a_id, self._role_permissions(role_code))
                self.assertEqual(response.media_type, "application/pdf")
                self.assertTrue(response.body.startswith(b"%PDF"))
                self.assertGreater(len(response.body), 1500)
                self.assertEqual(
                    response.headers["content-disposition"],
                    f'attachment; filename="b2b_customer_{self.customer_a_id}_catalog.pdf"',
                )

    def test_pdf_endpoint_rejects_user_without_sales_view(self) -> None:
        with self.assertRaises(HTTPException) as raised:
            self._export_customer_pdf(self.customer_a_id, {"product.view"})
        self.assertEqual(raised.exception.status_code, 403)

    def test_pdf_handles_customer_without_channel_and_empty_catalog(self) -> None:
        with main.SessionLocal.begin() as db:
            empty_customer = B2BCustomer(
                customer_name="Cliente sin canal ni catálogo",
                address="Dirección extensa, con comas, tildes y suficiente texto para probar ajuste de línea",
            )
            db.add(empty_customer)
            db.flush()
            empty_customer_id = empty_customer.id

        response = self._export_customer_pdf(empty_customer_id, self._role_permissions("admin"))
        self.assertTrue(response.body.startswith(b"%PDF"))
        self.assertGreater(len(response.body), 1500)

    def test_pdf_currency_helper_uses_colones_and_rounds_to_whole_colones(self) -> None:
        self.assertEqual(format_crc_catalog_price(Decimal("3200.0000")), "₡3,200")
        self.assertEqual(format_crc_catalog_price(Decimal("2350.4900")), "₡2,350")
        self.assertEqual(format_crc_catalog_price(Decimal("20900.5000")), "₡20,901")

    def test_pdf_attempts_database_logo_and_survives_unreadable_logo(self) -> None:
        with main.SessionLocal.begin() as db:
            db.add(
                AppSettings(
                    id=main.BRANDING_SETTINGS_ID,
                    company_name="Green Corner",
                    logo_bytes=b"not-a-decodable-image",
                    logo_mime_type="image/png",
                )
            )

        with mock.patch("app.services.b2b_catalog_export_service._build_logo_flowable", return_value=None) as logo:
            response = self._export_customer_pdf(self.customer_a_id, self._role_permissions("admin"))
        logo.assert_called_once_with(b"not-a-decodable-image")
        self.assertTrue(response.body.startswith(b"%PDF"))

    def test_add_form_supports_manual_and_b2c_discount_pricing(self) -> None:
        catalog = Path("app/templates/b2b_customer_products.html").read_text(encoding="utf-8")
        self.assertIn("Método de precio", catalog)
        self.assertIn('value="manual"', catalog)
        self.assertIn("Digitar precio manual", catalog)
        self.assertIn('value="b2c_discount"', catalog)
        self.assertIn("Calcular por descuento sobre B2C", catalog)
        self.assertIn('id="b2c_discount_percent"', catalog)
        self.assertIn("Descuento sobre precio B2C (%)", catalog)
        self.assertIn("Precio sugerido al por mayor", catalog)
        self.assertIn("suggestedPrice = b2cPrice * (1 - (discountPercent / 100))", catalog)
        self.assertIn("distributorPriceInput.value = suggestedPrice.toFixed(2)", catalog)
        self.assertIn("El precio final de este campo es el único valor que se guarda.", catalog)

    def test_discount_mode_saves_final_distributor_price_not_discount(self) -> None:
        request = self._request_with_permissions(self._role_permissions("sales_manager"))
        with main.SessionLocal() as db, mock.patch.object(main, "safe_log_audit_event"):
            response = main.add_b2b_customer_product(
                customer_id=self.customer_a_id,
                request=request,
                sku="A-DISCOUNT",
                description="Precio final redondeado",
                distributor_price="2350",
                pricing_method="b2c_discount",
                discount_percent="26.8",
                active=True,
                db=db,
            )
            saved = (
                db.query(B2BCustomerProduct)
                .filter(
                    B2BCustomerProduct.customer_id == self.customer_a_id,
                    B2BCustomerProduct.sku == "A-DISCOUNT",
                )
                .one()
            )
        self.assertEqual(response.status_code, 303)
        self.assertEqual(saved.distributor_price, Decimal("2350.0000"))

    def test_admin_can_still_add_a_manual_price(self) -> None:
        request = self._request_with_permissions(self._role_permissions("admin"))
        with main.SessionLocal() as db, mock.patch.object(main, "safe_log_audit_event"):
            response = main.add_b2b_customer_product(
                customer_id=self.customer_a_id,
                request=request,
                sku="A-MANUAL",
                description="Precio manual",
                distributor_price="1800.50",
                pricing_method="manual",
                discount_percent="",
                active=True,
                db=db,
            )
            saved = db.query(B2BCustomerProduct).filter(B2BCustomerProduct.sku == "A-MANUAL").one()
        self.assertEqual(response.status_code, 303)
        self.assertEqual(saved.distributor_price, Decimal("1800.5000"))

    def test_user_without_catalog_edit_permission_cannot_add(self) -> None:
        request = self._request_with_permissions({"sales.view", "product.view"})
        with main.SessionLocal() as db:
            with self.assertRaises(HTTPException) as raised:
                main.add_b2b_customer_product(
                    customer_id=self.customer_a_id,
                    request=request,
                    sku="A-FORBIDDEN",
                    description="No debe guardarse",
                    distributor_price="1000",
                    pricing_method="manual",
                    discount_percent="",
                    active=True,
                    db=db,
                )
            self.assertEqual(raised.exception.status_code, 403)
            self.assertIsNone(
                db.query(B2BCustomerProduct).filter(B2BCustomerProduct.sku == "A-FORBIDDEN").one_or_none()
            )

    def test_add_pricing_validation_rejects_invalid_discount_and_nonpositive_final_price(self) -> None:
        with self.assertRaisesRegex(main.B2BValidationError, "between 0 and 100"):
            main._validate_b2b_catalog_add_pricing("b2c_discount", "100.01", "1000")
        with self.assertRaisesRegex(main.B2BValidationError, "greater than 0"):
            main._validate_b2b_catalog_add_pricing("manual", "", "0")


if __name__ == "__main__":
    unittest.main()
