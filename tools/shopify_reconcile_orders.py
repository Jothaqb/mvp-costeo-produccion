from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.database import SessionLocal  # noqa: E402
from app.services.shopify_phase1a_service import shopify_integration_enabled  # noqa: E402
from app.services.shopify_phase1b_service import (  # noqa: E402
    ShopifyGraphQLClient,
    detect_missing_shopify_orders,
)


CONFIRMATION = "DETECT_ONLY"


def _parse_datetime(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("--since must be an ISO-8601 timestamp.") from exc


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Read-only Shopify reconciliation from an explicitly approved activation timestamp."
    )
    parser.add_argument("--since", required=True, type=_parse_datetime)
    parser.add_argument("--confirm", required=True)
    args = parser.parse_args()
    if args.confirm != CONFIRMATION:
        parser.error(f"--confirm must equal {CONFIRMATION}")
    if not shopify_integration_enabled():
        parser.error("SHOPIFY_INTEGRATION_ENABLED is not enabled.")

    client = ShopifyGraphQLClient.from_environment()
    with SessionLocal() as db:
        result = detect_missing_shopify_orders(db, client=client, since=args.since)
        db.rollback()
    print(
        json.dumps(
            {
                "mode": "detect_only",
                "since": result.since.isoformat(),
                "fetched_count": len(result.fetched_order_gids),
                "existing_count": len(result.existing_order_gids),
                "missing_count": len(result.missing_order_gids),
                "missing_order_gids": list(result.missing_order_gids),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
