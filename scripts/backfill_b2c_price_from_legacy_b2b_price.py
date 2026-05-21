from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import app.models  # noqa: F401
from app.database import SessionLocal
from app.models import Product


CONFIRM_TOKEN = "BACKFILL_B2C_PRICE"
DEFAULT_SAMPLE_SIZE = 20
CASE_A = "A"
CASE_B = "B"
CASE_C = "C"
CASE_D = "D"
CASE_E = "E"


@dataclass(frozen=True)
class ProductSnapshot:
    sku: str
    name: str
    b2c_price: Decimal | None
    b2b_price: Decimal | None


@dataclass
class Analysis:
    case_a: list[Product]
    case_b: list[Product]
    case_c: list[Product]
    case_d: list[Product]
    case_e: list[Product]

    @property
    def total_reviewed(self) -> int:
        return sum(
            len(items)
            for items in (self.case_a, self.case_b, self.case_c, self.case_d, self.case_e)
        )

    @property
    def untouched_count(self) -> int:
        return len(self.case_d) + len(self.case_e)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Inspect or backfill Product.b2c_price from legacy Product.b2b_price values "
            "left by the historical Loyverse import."
        ),
        epilog=(
            "Examples:\n"
            "  python scripts\\backfill_b2c_price_from_legacy_b2b_price.py\n"
            "  python scripts\\backfill_b2c_price_from_legacy_b2b_price.py --sample-size 10\n"
            "  python scripts\\backfill_b2c_price_from_legacy_b2b_price.py --sku-prefix GC-\n"
            "  python scripts\\backfill_b2c_price_from_legacy_b2b_price.py "
            "--apply --confirm BACKFILL_B2C_PRICE"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply the backfill for safe cases only (requires --confirm BACKFILL_B2C_PRICE).",
    )
    parser.add_argument(
        "--confirm",
        help="Confirmation token required together with --apply.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=DEFAULT_SAMPLE_SIZE,
        help=f"Maximum number of sample products to print per case (default: {DEFAULT_SAMPLE_SIZE}).",
    )
    parser.add_argument(
        "--sku-prefix",
        help="Optional SKU prefix filter for a narrower inspection or apply run.",
    )
    return parser


def _validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if args.sample_size < 0:
        parser.error("--sample-size must be 0 or greater.")
    if args.confirm and not args.apply:
        parser.error("--confirm can only be used together with --apply.")
    if args.apply and args.confirm != CONFIRM_TOKEN:
        parser.error(f"--apply requires --confirm {CONFIRM_TOKEN}")


def _classify_product(product: Product) -> str:
    has_b2c = product.b2c_price is not None
    has_b2b = product.b2b_price is not None

    if not has_b2c and has_b2b:
        return CASE_A
    if has_b2c and has_b2b and product.b2c_price == product.b2b_price:
        return CASE_B
    if has_b2c and has_b2b and product.b2c_price != product.b2b_price:
        return CASE_C
    if not has_b2c and not has_b2b:
        return CASE_D
    return CASE_E


def _analyze_products(products: list[Product]) -> Analysis:
    case_a: list[Product] = []
    case_b: list[Product] = []
    case_c: list[Product] = []
    case_d: list[Product] = []
    case_e: list[Product] = []

    for product in products:
        case = _classify_product(product)
        if case == CASE_A:
            case_a.append(product)
        elif case == CASE_B:
            case_b.append(product)
        elif case == CASE_C:
            case_c.append(product)
        elif case == CASE_D:
            case_d.append(product)
        else:
            case_e.append(product)

    return Analysis(
        case_a=case_a,
        case_b=case_b,
        case_c=case_c,
        case_d=case_d,
        case_e=case_e,
    )


def _format_price(value: Decimal | None) -> str:
    if value is None:
        return "-"
    return format(value, "f")


def _snapshot(product: Product) -> ProductSnapshot:
    return ProductSnapshot(
        sku=product.sku,
        name=product.name,
        b2c_price=product.b2c_price,
        b2b_price=product.b2b_price,
    )


def _print_case_samples(title: str, products: list[Product], sample_size: int) -> None:
    print(f"\n{title}: {len(products)}")
    if sample_size == 0 or not products:
        return

    print("  Samples:")
    for product in products[:sample_size]:
        sample = _snapshot(product)
        print(
            "   - SKU={sku} | Name={name} | b2c_price={b2c} | b2b_price={b2b}".format(
                sku=sample.sku,
                name=sample.name,
                b2c=_format_price(sample.b2c_price),
                b2b=_format_price(sample.b2b_price),
            )
        )


def _print_summary(analysis: Analysis, sample_size: int, apply_mode: bool, sku_prefix: str | None) -> None:
    print("B2C backfill analysis from legacy Product.b2b_price")
    print(f"Mode: {'APPLY' if apply_mode else 'DRY RUN'}")
    if sku_prefix:
        print(f"SKU prefix filter: {sku_prefix}")
    print(f"Total products reviewed: {analysis.total_reviewed}")
    print(f"Untouched products: {analysis.untouched_count}")

    _print_case_samples(
        "Case A (copy b2b_price -> b2c_price, then clear b2b_price)",
        analysis.case_a,
        sample_size,
    )
    _print_case_samples(
        "Case B (same b2c_price and b2b_price, clear b2b_price)",
        analysis.case_b,
        sample_size,
    )
    _print_case_samples(
        "Case C conflicts (different b2c_price and b2b_price, no automatic change)",
        analysis.case_c,
        sample_size,
    )
    _print_case_samples(
        "Case D (both prices empty, untouched)",
        analysis.case_d,
        sample_size,
    )
    _print_case_samples(
        "Case E (b2c_price set and b2b_price empty, untouched)",
        analysis.case_e,
        sample_size,
    )


def _apply_backfill(analysis: Analysis) -> int:
    updated_count = 0

    for product in analysis.case_a:
        product.b2c_price = product.b2b_price
        product.b2b_price = None
        updated_count += 1

    for product in analysis.case_b:
        product.b2b_price = None
        updated_count += 1

    return updated_count


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    _validate_args(parser, args)

    db = SessionLocal()
    try:
        query = db.query(Product).order_by(Product.sku.asc(), Product.id.asc())
        sku_prefix = (args.sku_prefix or "").strip()
        if sku_prefix:
            query = query.filter(Product.sku.ilike(f"{sku_prefix}%"))

        products = query.all()
        analysis = _analyze_products(products)
        _print_summary(analysis, args.sample_size, args.apply, sku_prefix or None)

        if not args.apply:
            print("\nDry-run only. No changes were applied.")
            return 0

        updated_count = _apply_backfill(analysis)
        db.commit()
        print(
            "\nApply complete. Updated products: {updated}. "
            "Case C conflicts were not modified.".format(updated=updated_count)
        )
        return 0
    except KeyboardInterrupt:
        db.rollback()
        print("\nOperation cancelled. Transaction rolled back.")
        return 1
    except Exception as exc:
        db.rollback()
        print(
            "\nBackfill failed. Transaction rolled back. "
            f"Error type: {type(exc).__name__}"
        )
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
