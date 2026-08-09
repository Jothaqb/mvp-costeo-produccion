import csv
import io
import os
import tempfile
import unittest
from datetime import timezone
from decimal import Decimal
from pathlib import Path
from unittest import mock


_PREVIOUS_DATABASE_URL = os.environ.get("DATABASE_URL")
_TEMP_DB = tempfile.NamedTemporaryFile(prefix="b2b_catalog_export_", suffix=".db", delete=False)
_TEMP_DB.close()
os.environ["DATABASE_URL"] = f"sqlite:///{Path(_TEMP_DB.name).as_posix()}"

with mock.patch("zoneinfo.ZoneInfo", return_value=timezone.utc):
    from app import main  # noqa: E402
from app.models import B2BCustomer, B2BCustomerProduct  # noqa: E402


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
            customer_a = B2BCustomer(customer_name="Café Cliente A")
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

    def test_export_is_utf8_bom_and_strictly_scoped_to_customer(self) -> None:
        response = self._export_customer(self.customer_a_id)
        self.assertTrue(response.body.startswith(b"\xef\xbb\xbf"))
        self.assertEqual(
            response.headers["content-disposition"],
            f'attachment; filename="b2b_customer_{self.customer_a_id}_catalog.csv"',
        )
        rows = list(csv.reader(io.StringIO(response.body.decode("utf-8-sig"))))
        self.assertEqual(rows[0], ["Codigo", "Descripcion", "Precio", "Estado"])
        self.assertEqual(
            rows[1:],
            [
                ["A-001", "Té limón", "1250.5000", "Activo"],
                ["A-002", "Producto inactivo", "800.0000", "Inactivo"],
            ],
        )
        self.assertNotIn("B-SECRET", response.body.decode("utf-8-sig"))

    def test_empty_customer_catalog_exports_only_headers(self) -> None:
        with main.SessionLocal.begin() as db:
            empty_customer = B2BCustomer(customer_name="Sin catálogo")
            db.add(empty_customer)
            db.flush()
            empty_customer_id = empty_customer.id

        response = self._export_customer(empty_customer_id)
        rows = list(csv.reader(io.StringIO(response.body.decode("utf-8-sig"))))
        self.assertEqual(rows, [["Codigo", "Descripcion", "Precio", "Estado"]])

    def test_templates_show_catalog_and_download_labels(self) -> None:
        customer_list = Path("app/templates/b2b_customers_list.html").read_text(encoding="utf-8")
        catalog = Path("app/templates/b2b_customer_products.html").read_text(encoding="utf-8")
        self.assertIn(">Catálogo</a>", customer_list)
        self.assertNotIn(">Products</a>", customer_list)
        self.assertIn("Descargar CSV", catalog)
        self.assertIn("products/export.csv", catalog)


if __name__ == "__main__":
    unittest.main()
