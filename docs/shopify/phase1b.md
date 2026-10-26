# Shopify Phase 1B: Shopify order to ERP B2C draft

## Scope and safety boundary

Phase 1B receives Shopify `orders/create` webhooks and creates one existing ERP
`B2CSalesOrder` in `draft`. It does not create an invoice, ERP payment, account
receivable, inventory transaction, or Kardex entry. It does not synchronize ERP
changes back to Shopify and it does not import historical orders automatically.

The integration and its processor are disabled unless both feature flags are
enabled. Deploying the code with the defaults below does not contact Shopify or
process a persisted event.

## Reception and recovery architecture

1. `POST /integrations/shopify/webhooks/orders-create` reads a size-limited raw
   body, verifies the HMAC over those exact bytes, verifies the exact shop and
   topic, and persists the event in `shopify_webhook_events`.
2. The endpoint returns HTTP 202 after the database commit. The request does not
   create the ERP order.
3. When explicitly enabled, a lightweight in-process loop claims persisted rows
   using PostgreSQL `FOR UPDATE SKIP LOCKED`. The claim is committed before work,
   so an interrupted process can recover stale claims after ten minutes.
4. Processing reads the current financial snapshot through Shopify GraphQL,
   validates products, customer identity, shipping mapping, discounts, tax,
   currency, and the exact total, then creates customer/mapping/order/lines/audit
   in one transaction.
5. Transient failures use bounded exponential retries. Business validation issues
   become `review_pending` without a partial B2C order and can be queued again by
   an authorized user from the existing B2C Sales Orders page.

The persisted `X-Shopify-API-Version` must exactly match the configured API
version. Missing or stale-version deliveries remain in review and cannot create
a partial ERP order.

This design uses the existing Render web service and PostgreSQL database. It does
not require another paid worker service. The operational tradeoff is that event
processing pauses while the web service is stopped; persisted events remain
recoverable when it starts again.

## Required Render variables

Keep these values disabled until the activation checklist is complete:

```text
SHOPIFY_INTEGRATION_ENABLED=false
SHOPIFY_BACKGROUND_PROCESSOR_ENABLED=false
```

Configure these before a controlled activation:

```text
SHOPIFY_SHOP_DOMAIN=<exact-store>.myshopify.com
SHOPIFY_WEBHOOK_SECRET=<Shopify app client secret used to sign webhooks>
SHOPIFY_API_VERSION=2026-10
SHOPIFY_SHIPPING_METHOD_MAP={"<exact-source>|<exact-code>":"10219","<exact-source>|<exact-code>":"10258","<exact-source>|<exact-code>":"pickup"}
```

Configure exactly one Admin API authentication mode:

```text
# Stable access token supplied by an eligible Shopify custom app installation
SHOPIFY_ADMIN_ACCESS_TOKEN=<secret>

# OR client credentials; the ERP obtains and renews expiring access tokens
SHOPIFY_CLIENT_ID=<secret>
SHOPIFY_CLIENT_SECRET=<secret>
```

`SHOPIFY_WEBHOOK_SECRET` is not a second Shopify credential: it must be the same
Client secret for the configured app. When client credentials are used, the ERP
accepts `SHOPIFY_CLIENT_SECRET` as the webhook secret if the duplicate variable
is omitted, and rejects startup configuration when both values are present but
different. The background processor also rejects missing, partial, or ambiguous
Admin API authentication before it can claim queued events.

Optional controls:

```text
SHOPIFY_MAX_WEBHOOK_BYTES=2097152
SHOPIFY_WORKER_INTERVAL_SECONDS=15
APP_BASE_URL=https://erp.greencornercr.com
```

Secrets must remain in Render environment configuration. They must not be added
to Git, templates, query strings, application logs, or audit payloads.

## Shopify Dev Dashboard configuration

1. Select the Green Corner app and authorize the Admin API `read_orders` scope.
   Shopify approval for protected customer data may also be required because the
   order contains name, phone, email, and delivery address.
2. Select Admin API version `2026-10`.
3. Create one app-specific JSON webhook subscription:
   - topic: `ORDERS_CREATE` / `orders/create`
   - callback URL:
     `https://erp.greencornercr.com/integrations/shopify/webhooks/orders-create`
4. Copy the app's webhook signing secret to `SHOPIFY_WEBHOOK_SECRET`.
5. Obtain the exact `source` and `code` values for every live Shopify delivery
   option and configure the explicit mapping. Do not infer GAM from province or
   shipping price.

The real shipping method values are still required. At minimum Green Corner must
identify the exact source/code pairs for GAM, outside GAM, store pickup, and any
free-shipping variants. Free shipping may still map to SKU `10219` or `10258`; its
stored net line amount is zero after the Shopify shipping discount. Pickup maps
to `pickup` and creates no shipping SKU line.

## Review rules

The first release accepts only:

- CRC orders;
- zero tax;
- one known, active, sellable ERP product for every exact Shopify SKU;
- at most one shipping method with an exact configured source/code mapping;
- complete discount allocations whose sum matches the Shopify order discount;
- a unique, non-conflicting customer match, or enough name/contact data to create
  a new B2C customer;
- exact equality between ERP net lines and the Shopify total;
- a GraphQL financial total and tax that still match the webhook payload.

`PAID`, `PARTIALLY_PAID`, or other Shopify financial states remain informational.
The resulting ERP order remains `draft` and no Shopify payment is recorded again.

## Controlled reconciliation

Reconciliation is detect-only. It lists Shopify order GIDs created since an
explicit activation timestamp and compares them with persisted integration rows.
It never imports a missing order:

```powershell
.venv\Scripts\python.exe tools\shopify_reconcile_orders.py `
  --since 2026-10-25T19:00:00-06:00 `
  --confirm DETECT_ONLY
```

Use the actual activation timestamp. Do not use this command to backfill orders
that predate activation.

## Controlled activation checklist

1. Keep both feature flags false while configuring Shopify and Render.
2. Confirm PostgreSQL backup/PITR and deploy the schema changes.
3. Confirm the exact authorized `.myshopify.com` domain.
4. Confirm Admin API credentials and `read_orders` access without printing them.
5. Obtain and test every real shipping source/code pair, including free shipping
   and pickup; update `SHOPIFY_SHIPPING_METHOD_MAP`.
6. Register the single `orders/create` webhook and send Shopify's test delivery.
7. Verify the test event is persisted once, HMAC failures create no row, and no
   B2C order is created while the processor flag is false.
8. Enable `SHOPIFY_INTEGRATION_ENABLED`, then enable
   `SHOPIFY_BACKGROUND_PROCESSOR_ENABLED` for one controlled real order.
9. Confirm one B2C `draft`, exact line/discount/total reconciliation, `Online Web`
   channel, financial snapshot, and zero inventory/Kardex/payment effects.
10. Run detect-only reconciliation from the activation timestamp and review any
    missing GIDs. Do not enable historical import.

## Deferred work

- order updates and cancellations;
- ERP-to-Shopify synchronization;
- automatic historical import;
- fulfillment synchronization;
- automated remediation of `review_pending` rows.
