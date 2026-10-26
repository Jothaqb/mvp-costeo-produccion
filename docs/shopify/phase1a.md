# Shopify integration foundation — Phase 1A

## Status and scope

Phase 1A prepares ERP persistence and B2C transactional behavior only. It does not
connect to Shopify, expose a webhook endpoint, import an order, create an app, or
require credentials. `SHOPIFY_INTEGRATION_ENABLED` defaults to disabled, and no
startup task reads or processes Shopify data.

Approved storefront: `greencornercr.com`. The future importer must resolve the
existing active B2C channel named exactly `Online Web`; it must not create a
Shopify channel or use Loyverse mapping fields.

## Schema

The following tables are created by `Base.metadata.create_all()`:

- `shopify_webhook_events`: delivery identity, topic, body SHA-256, status,
  attempts, errors, and processing timestamps. `(shop_domain, webhook_id)` is
  unique.
- `shopify_order_integrations`: unique Shopify Order GID and unique optional B2C
  order link, customer GID, financial/fulfillment status, reconciliation amounts,
  delivery evidence, review/error details, fingerprints, and timestamps.
- `shopify_order_line_integrations`: unique Shopify line per integration, unique
  optional B2C line link, SKU, quantities, gross/discount/net values, variant ID,
  and `product|shipping` type.
- `shopify_customer_mappings`: unique Shopify Customer GID per store linked to a
  B2C customer, without reusing Loyverse fields.

Incremental, idempotent initialization adds to existing B2C tables:

- `b2c_sales_order_lines.line_type`, defaulting existing rows to `product`.
- `b2c_sales_orders.shipping_cost_amount`.
- `b2c_sales_orders.shipping_cost_source`.
- `b2c_sales_orders.shipping_cost_recorded_at`.
- `b2c_sales_orders.shipping_cost_recorded_by_user_id`.

No historical row is reclassified or recalculated. PostgreSQL receives an
idempotent line-type check and shipping-cost user foreign key. SQLite receives
compatible incremental columns for local/test use.

## Customer matching foundation

The future Phase 1B importer must perform one transaction-scoped PostgreSQL
advisory lock over the normalized Shopify identity before match/create. This
serializes concurrent imports without imposing unsafe global uniqueness on ERP
phone or email fields.

Matching priority is:

1. Existing `(shop_domain, Shopify Customer GID)` mapping.
2. Unique normalized phone match; Costa Rican eight-digit numbers normalize to
   `+506` form.
3. Unique normalized email match when phone is absent.
4. No match, ambiguity, inactive customer, or contradictory identifiers are
   explicit outcomes; ambiguity/conflict never auto-links.

A new customer is eligible only with a name and at least one valid phone or
email. Phase 1A exposes the no-commit customer helper and matching rules but does
not create customers from Shopify.

## Product, shipping, and totals

Physical Shopify lines must resolve an exact Product Master SKU that is active
and available for Green Corner sales. Invalid lines block the future whole-order
transaction; partial import is not supported.

Only SKU `10219` (GAM) and `10258` (outside GAM) may be new `shipping` lines.
New manual B2C orders use the same classification rule. Historical lines remain
`product`. Shipping quantity must equal one and a new order may contain at most
one shipping line, preventing duplicate shipping revenue.

Shipping lines:

- remain part of the B2C invoiced total and retain their SKU;
- never create InventoryBalance, inventory transaction, or Kardex movement;
- never read average inventory cost;
- persist zero product COGS only for compatibility with existing accounting
  coverage; this is not the carrier cost, which remains on the order;
- store their own gross, discount, and net values (including fully discounted
  shipping with net zero);
- are excluded from physical unit, product Pareto, category Pareto, product COGS,
  and product margin calculations.

The existing order-level discount rule applies to product lines only. A future
Shopify importer uses the explicit line-pricing snapshot helper so product and
shipping discounts are preserved exactly once.

## Margins and actual carrier cost

Product margin is:

`product net sales - product COGS`

Total order margin is:

`product net sales + shipping net sales - product COGS - actual shipping cost`

The total margin is pending while either product COGS or actual shipping cost is
unknown. `shipping_cost_amount = 0` is a known zero and is distinct from NULL.
The actual carrier cost can be recorded later from the B2C order detail using the
existing `sales.invoice` permission, session-bound CSRF, same-origin validation,
and an audit row in the same transaction.

Total Sales keeps total invoiced revenue including shipping, shows product and
shipping revenue separately, excludes shipping from units and Pareto, and only
exposes total margin when required costs are known. The shipping integration
amount is reconciliation evidence, never a second revenue entry.

## Transaction boundaries

No-commit helpers are available for B2C customer creation, B2C order creation,
Shopify mapping/event persistence, and audit insertion. Existing manual wrappers
retain their prior commit behavior. PostgreSQL B2C numbering uses a
transaction-scoped advisory lock plus a locked sequence row. A future Phase 1B
importer must create customer/order/lines/mappings/integration records/audit in
one session and issue exactly one final commit.

## Phase 1B pending work

- Shopify app and credential configuration.
- Raw-body HMAC verification and authorized-domain enforcement.
- Durable webhook receiver/queue/worker; not in-memory-only BackgroundTasks.
- Shopify GraphQL/API access and recoverable order processing.
- Exact verified mapping of Shopify GAM/outside-GAM delivery method codes.
- CRC/zero-tax checks and review handling for unsupported money components.
- Atomic import orchestration, reconciliation UI, retries, and real test orders.
- Privacy/retention controls for Shopify personal data.

Until those items are implemented and explicitly enabled, Shopify integration
remains inactive.
