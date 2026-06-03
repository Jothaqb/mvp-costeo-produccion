import unittest
from decimal import Decimal
from unittest import mock

from app.services.import_service import (
    GreenCornerDecimalParseError,
    GreenCornerImportPrecheckError,
    ImportPrecheckIssue,
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
