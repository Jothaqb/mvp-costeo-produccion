import argparse
import tempfile
import unittest
from datetime import date, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock, patch

from tools import b2b_loyverse_daily_invoice_job as daily
from tools.b2b_loyverse_monthly_invoice_job import Evaluation


class DailyBusinessDateTests(unittest.TestCase):
    def _resolve(self, hour: int, minute: int = 0, second: int = 0) -> date:
        now_cr = datetime(2026, 8, 10, hour, minute, second, tzinfo=daily.costa_rica_timezone())
        resolved, _ = daily.resolve_automatic_business_date(now_cr)
        return resolved

    def test_evening_window_uses_today(self) -> None:
        self.assertEqual(self._resolve(22, 0), date(2026, 8, 10))
        self.assertEqual(self._resolve(23, 30), date(2026, 8, 10))
        self.assertEqual(self._resolve(23, 59), date(2026, 8, 10))

    def test_delayed_window_uses_yesterday(self) -> None:
        self.assertEqual(self._resolve(0, 0), date(2026, 8, 9))
        self.assertEqual(self._resolve(3, 0), date(2026, 8, 9))

    def test_outside_window_aborts(self) -> None:
        with self.assertRaises(daily.BusinessDateError):
            self._resolve(3, 0, 1)
        with self.assertRaises(daily.BusinessDateError):
            self._resolve(21, 59)

    def test_explicit_business_date_works_outside_window(self) -> None:
        args = argparse.Namespace(business_date="2026-08-10", auto_business_date_costa_rica=False)
        resolved, source, _ = daily.resolve_business_date(
            args,
            now_cr=datetime(2026, 8, 10, 12, 0, tzinfo=daily.costa_rica_timezone()),
        )
        self.assertEqual(resolved, date(2026, 8, 10))
        self.assertEqual(source, "explicit_cli")

    def test_execute_flag_does_not_exist(self) -> None:
        with self.assertRaises(SystemExit):
            daily.parse_args(["--business-date", "2026-08-10", "--use-env", "--execute"])


class DailyClassificationTests(unittest.TestCase):
    def _order(self, *, sync_status: str = "", receipt_id: str = "", receipt_number: str = ""):
        line = SimpleNamespace(line_total=10)
        return SimpleNamespace(
            id=1,
            order_number="B2B1",
            delivery_date=date(2026, 8, 10),
            status="invoiced",
            lines=[line],
            loyverse_receipt_id=receipt_id,
            loyverse_receipt_number=receipt_number,
            loyverse_invoice_sync_status=sync_status,
        )

    def test_unknown_and_failed_are_blocked(self) -> None:
        for status in ("unknown", "failed"):
            with self.subTest(status=status):
                result = daily.evaluate_daily_order(
                    Mock(), self._order(sync_status=status), "store-1", excluded_order_numbers=set()
                )
                self.assertEqual(result.classification, "blocked")
                self.assertFalse(result.eligible)

    def test_success_without_reference_is_blocked(self) -> None:
        result = daily.evaluate_daily_order(
            Mock(), self._order(sync_status="success"), "store-1", excluded_order_numbers=set()
        )
        self.assertEqual(result.classification, "blocked")

    def test_receipt_number_is_already_sent(self) -> None:
        result = daily.evaluate_daily_order(
            Mock(), self._order(sync_status="success", receipt_number="1-100"), "store-1", excluded_order_numbers=set()
        )
        self.assertEqual(result.classification, "already_sent")

    def test_store_resolution_error_blocks_order(self) -> None:
        result = daily.evaluate_daily_order(
            Mock(),
            self._order(),
            "",
            excluded_order_numbers=set(),
            store_error="No Loyverse stores were returned.",
        )
        self.assertEqual(result.classification, "blocked")
        self.assertIn("stores", result.reason)


class DailyReadOnlyRunTests(unittest.TestCase):
    def _args(self, root: str, run_id: str) -> argparse.Namespace:
        return argparse.Namespace(
            business_date="2026-08-10",
            auto_business_date_costa_rica=False,
            use_env=True,
            export_root=root,
            run_id=run_id,
            exclude_order_number=[],
        )

    def test_dry_run_does_not_create_receipts_or_commit(self) -> None:
        session = Mock()
        session_factory = Mock(return_value=session)
        eligible = Evaluation(
            order_id=1,
            order_number="B2B1",
            delivery_date=date(2026, 8, 10),
            erp_status="invoiced",
            classification="eligible",
            eligible=True,
            total_amount=Decimal("10"),
            payload_fingerprint="fingerprint",
            loyverse_receipt_id="",
            loyverse_receipt_number="",
            loyverse_sync_status="",
            reason="Ready.",
            payload={"store_id": "store-1"},
            variant_snapshots={},
        )
        with tempfile.TemporaryDirectory() as temporary_dir, patch.dict(
            "os.environ",
            {"DATABASE_URL": "sqlite:///:memory:", "LOYVERSE_API_TOKEN": "test-token"},
            clear=False,
        ), patch.object(daily, "create_engine", return_value=Mock()), patch.object(
            daily, "sessionmaker", return_value=session_factory
        ), patch.object(daily, "resolve_store_read_only", return_value=("store-1", [], "")), patch.object(
            daily, "load_orders", return_value=[Mock()]
        ), patch.object(daily, "evaluate_daily_order_safely", return_value=eligible), patch(
            "tools.b2b_loyverse_monthly_invoice_job._create_loyverse_receipt"
        ) as create_receipt:
            summary, run_dir, exit_code = daily.run(self._args(temporary_dir, "run-1"))
            self.assertEqual(exit_code, 0)
            self.assertEqual(summary["orders_eligible"], 1)
            self.assertTrue((run_dir / "summary.json").exists())
            self.assertFalse((run_dir / "execution_results.csv").exists())
            session.commit.assert_not_called()
            session.rollback.assert_called_once()
            create_receipt.assert_not_called()

    def test_run_id_never_overwrites_existing_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_dir:
            run_dir = daily.prepare_run_dir(temporary_dir, date(2026, 8, 10), "same-run")
            marker = run_dir / "summary.json"
            marker.write_text("preserve", encoding="utf-8")
            with self.assertRaises(SystemExit):
                daily.prepare_run_dir(temporary_dir, date(2026, 8, 10), "same-run")
            self.assertEqual(marker.read_text(encoding="utf-8"), "preserve")


if __name__ == "__main__":
    unittest.main()
