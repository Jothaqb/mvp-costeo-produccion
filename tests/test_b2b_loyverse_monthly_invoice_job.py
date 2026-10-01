import argparse
import unittest
from datetime import date

from tools import b2b_loyverse_monthly_invoice_job as monthly


class B2BLoyverseMonthlyInvoiceJobArgsTests(unittest.TestCase):
    def _args(
        self,
        *,
        start_date: str = "2026-09-01",
        end_date_exclusive: str = "2026-10-01",
        execute: bool = False,
        confirm: str = "",
    ) -> argparse.Namespace:
        return argparse.Namespace(
            start_date=start_date,
            end_date_exclusive=end_date_exclusive,
            timezone="America/Costa_Rica",
            export_dir="unused-test-artifacts",
            use_env=True,
            execute=execute,
            confirm=confirm,
            loyverse_payment_type_id="1d4486cd-cac0-4933-85df-1587be6c0973",
            loyverse_payment_type_name="Tiendas Naturales",
            customer_override_erp_id=6,
            customer_override_loyverse_id="25bf9d0b-47ee-4286-aba4-c8b77bedd507",
            customer_override_loyverse_name="Super cocles",
            exclude_order_number=["B2B1208"],
        )

    def test_september_execute_accepts_september_confirmation(self) -> None:
        args = self._args(
            execute=True,
            confirm=monthly.SEPTEMBER_2026_EXECUTE_CONFIRMATION,
        )

        start_date, end_date_exclusive, _ = monthly.validate_args(args)

        self.assertEqual(start_date, date(2026, 9, 1))
        self.assertEqual(end_date_exclusive, date(2026, 10, 1))
        self.assertEqual(args.exclude_order_number, ["B2B1208"])

    def test_september_execute_rejects_july_confirmation(self) -> None:
        args = self._args(execute=True, confirm=monthly.EXECUTE_CONFIRMATION)

        with self.assertRaisesRegex(SystemExit, monthly.SEPTEMBER_2026_EXECUTE_CONFIRMATION):
            monthly.validate_args(args)

    def test_september_execute_requires_confirmation(self) -> None:
        args = self._args(execute=True)

        with self.assertRaisesRegex(SystemExit, monthly.SEPTEMBER_2026_EXECUTE_CONFIRMATION):
            monthly.validate_args(args)

    def test_unapproved_month_remains_blocked(self) -> None:
        args = self._args(
            start_date="2026-10-01",
            end_date_exclusive="2026-11-01",
            execute=True,
            confirm=monthly.SEPTEMBER_2026_EXECUTE_CONFIRMATION,
        )

        with self.assertRaisesRegex(SystemExit, "explicitly approved"):
            monthly.validate_args(args)

    def test_dry_run_does_not_require_confirmation(self) -> None:
        args = self._args()

        start_date, end_date_exclusive, _ = monthly.validate_args(args)

        self.assertEqual((start_date, end_date_exclusive), (date(2026, 9, 1), date(2026, 10, 1)))

    def test_payload_inputs_and_order_status_policy_are_unchanged(self) -> None:
        args = self._args()

        monthly.validate_args(args)

        self.assertEqual(monthly.SUPPORTED_ORDER_STATUSES, {"draft", "in_process", "invoiced"})
        self.assertEqual(args.loyverse_payment_type_name, "Tiendas Naturales")
        self.assertEqual(args.customer_override_erp_id, 6)
        self.assertEqual(args.exclude_order_number, ["B2B1208"])


if __name__ == "__main__":
    unittest.main()
