import unittest
from decimal import Decimal
from unittest import mock

from app.services.import_service import (
    GreenCornerDecimalParseError,
    GreenCornerImportPrecheckError,
    ImportPrecheckIssue,
    LoyverseColumnMap,
    _resolve_loyverse_column_map,
    _upsert_product_master_from_loyverse_row,
    import_loyverse_csv,
    parse_decimal_strict_green_corner,
    run_green_corner_import_precheck,
)


def _build_loyverse_row(
    *,
    sku: str = "SKU-1",
    name: str = "Product 1",
    standard_cost: str = "",
    inventory: str = "",
    low_stock: str = "",
    optimal_stock: str = "",
    bom_component_sku: str = "",
    bom_quantity: str = "",
) -> list[str]:
    row = [""] * 25
    row[1] = sku
    row[2] = name
    row[12] = standard_cost
    row[15] = bom_quantity
    row[14] = bom_component_sku
    row[22] = inventory
    row[23] = low_stock
    row[24] = optimal_stock
    return row


def _build_loyverse_header_row() -> list[str]:
    row = [""] * 25
    row[0] = "Handle"
    row[4] = "Price [Green Corner]"
    row[5] = "In stock [Green Corner]"
    row[6] = "Low stock"
    row[7] = "Optimal stock"
    row[8] = "Average cost"
    row[9] = "Supplier"
    row[10] = "Available for sale [Green Corner]"
    row[11] = "Use production"
    row[12] = "Included item SKU"
    row[13] = "Included quantity"
    row[14] = "SKU"
    row[15] = "Item name"
    row[16] = "Category"
    return row


def _build_loyverse_header_mapped_row(
    *,
    sku: str,
    name: str,
    average_cost: str,
    price: str,
    inventory: str,
    low_stock: str = "",
    optimal_stock: str = "",
) -> list[str]:
    row = [""] * 25
    row[4] = price
    row[5] = inventory
    row[6] = low_stock
    row[7] = optimal_stock
    row[8] = average_cost
    row[10] = "yes"
    row[14] = sku
    row[15] = name
    return row


class ParseDecimalStrictGreenCornerTests(unittest.TestCase):
    def test_dot_with_three_decimals_stays_decimal(self) -> None:
        self.assertEqual(parse_decimal_strict_green_corner("1.250"), Decimal("1.250"))
        self.assertEqual(parse_decimal_strict_green_corner("12.500"), Decimal("12.500"))
        self.assertEqual(parse_decimal_strict_green_corner("123.456"), Decimal("123.456"))

    def test_comma_decimal_values_are_supported(self) -> None:
        self.assertEqual(parse_decimal_strict_green_corner("1,250"), Decimal("1.250"))
        self.assertEqual(parse_decimal_strict_green_corner("1,2500"), Decimal("1.2500"))
        self.assertEqual(parse_decimal_strict_green_corner("0,250"), Decimal("0.250"))

    def test_plain_and_negative_values_are_supported(self) -> None:
        self.assertEqual(parse_decimal_strict_green_corner("1250"), Decimal("1250"))
        self.assertEqual(parse_decimal_strict_green_corner("0.250"), Decimal("0.250"))
        self.assertEqual(parse_decimal_strict_green_corner("-1.250"), Decimal("-1.250"))

    def test_ambiguous_formats_raise(self) -> None:
        for raw_value in ("1,250.50", "1.250,50", "1..250", "1,,250"):
            with self.subTest(raw_value=raw_value):
                with self.assertRaises(GreenCornerDecimalParseError):
                    parse_decimal_strict_green_corner(raw_value)


class GreenCornerImportPrecheckTests(unittest.TestCase):
    def _build_db_with_existing_product(
        self,
        *,
        current_inventory_qty: Decimal | None = None,
        standard_cost: Decimal | None = None,
        low_stock_qty: Decimal | None = None,
        optimal_stock_qty: Decimal | None = None,
    ) -> mock.Mock:
        existing_product = mock.Mock(
            current_inventory_qty=current_inventory_qty,
            standard_cost=standard_cost,
            low_stock_qty=low_stock_qty,
            optimal_stock_qty=optimal_stock_qty,
        )
        query = mock.Mock()
        query.filter.return_value = query
        query.one_or_none.return_value = existing_product
        db = mock.Mock()
        db.query.return_value = query
        return db

    def test_precheck_flags_ambiguous_decimal_as_blocking(self) -> None:
        row = _build_loyverse_row(inventory="1.250,50")
        query = mock.Mock()
        query.filter.return_value = query
        query.one_or_none.return_value = None
        db = mock.Mock()
        db.query.return_value = query

        with self.assertRaises(GreenCornerImportPrecheckError) as ctx:
            run_green_corner_import_precheck(db, [row])

        self.assertTrue(any(issue.blocking for issue in ctx.exception.issues))
        self.assertTrue(
            any(issue.risk_type == "ambiguous_decimal_format" for issue in ctx.exception.issues)
        )

    def test_header_mapping_uses_in_stock_not_price_for_inventory(self) -> None:
        header = _build_loyverse_header_row()
        row = _build_loyverse_header_mapped_row(
            sku="10488",
            name="Producto 10488",
            average_cost="15208",
            price="55000",
            inventory="1.773",
        )
        query = mock.Mock()
        query.filter.return_value = query
        query.one_or_none.return_value = None
        db = mock.Mock()
        db.query.return_value = query

        columns, has_header = _resolve_loyverse_column_map([header, row])
        self.assertTrue(has_header)
        self.assertEqual(columns.inventory, 5)
        self.assertEqual(columns.b2c_price, 4)
        self.assertEqual(columns.average_cost, 8)

        run_green_corner_import_precheck(db, [header, row], columns)

        product = _upsert_product_master_from_loyverse_row(db, row, columns)
        self.assertIsNotNone(product)
        self.assertEqual(product.current_inventory_qty, Decimal("1.773"))
        self.assertEqual(product.standard_cost, Decimal("15208"))
        self.assertEqual(product.b2c_price, Decimal("55000"))

    def test_inventory_x1000_correction_is_warning_not_blocking(self) -> None:
        row = _build_loyverse_row(standard_cost="15208", inventory="1.773")
        db = self._build_db_with_existing_product(
            current_inventory_qty=Decimal("1773"),
            standard_cost=Decimal("15208"),
        )

        try:
            run_green_corner_import_precheck(db, [row])
        except GreenCornerImportPrecheckError as exc:  # pragma: no cover - should not happen
            self.fail(f"Precheck should not block correction warning, got: {exc.issues}")

    def test_inventory_x1000_growth_stays_blocking(self) -> None:
        row = _build_loyverse_row(standard_cost="15208", inventory="1773")
        db = self._build_db_with_existing_product(
            current_inventory_qty=Decimal("1.773"),
            standard_cost=Decimal("15208"),
        )

        with self.assertRaises(GreenCornerImportPrecheckError) as ctx:
            run_green_corner_import_precheck(db, [row])

        self.assertTrue(
            any(
                issue.risk_type == "exact_x1000_jump_vs_existing" and issue.blocking
                for issue in ctx.exception.issues
            )
        )

    def test_inventory_value_overflow_still_blocks_during_x1000_correction(self) -> None:
        row = _build_loyverse_row(standard_cost="100000", inventory="1000")
        db = self._build_db_with_existing_product(
            current_inventory_qty=Decimal("1000000"),
            standard_cost=Decimal("100000"),
        )

        with self.assertRaises(GreenCornerImportPrecheckError) as ctx:
            run_green_corner_import_precheck(db, [row])

        self.assertTrue(
            any(
                issue.risk_type == "inventory_value_exceeds_numeric_12_4" and issue.blocking
                for issue in ctx.exception.issues
            )
        )

    def test_import_aborts_before_writes_when_precheck_blocks(self) -> None:
        issue = ImportPrecheckIssue(
            row=2,
            sku="SKU-1",
            field="current_inventory_qty",
            original_value="1.250,50",
            parsed_value=None,
            risk_type="ambiguous_decimal_format",
            blocking=True,
        )
        db = mock.Mock()
        with mock.patch(
            "app.services.import_service.run_green_corner_import_precheck",
            side_effect=GreenCornerImportPrecheckError([issue]),
        ):
            with self.assertRaises(GreenCornerImportPrecheckError):
                import_loyverse_csv(db, "loyverse.csv", b"sku,name\nSKU-1,Product 1\n")

        db.add.assert_not_called()
        db.flush.assert_not_called()
        db.commit.assert_not_called()


if __name__ == "__main__":
    unittest.main()
