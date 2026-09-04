export const largeHtmlBlockWithTable = `# Stripe

<HideInUI>

This page contains the setup guide and reference information for the [Stripe](https://stripe.com/) source connector.

</HideInUI>

## Prerequisites

- Access to the Stripe account containing the data you wish to replicate
- Stripe Account ID

## Setup guide

:::note
To authenticate the Stripe connector, you need to use a Stripe API key. Although you may use an existing key, we recommend that you create a new restricted key specifically for Airbyte and grant it **Read** privileges only. We also recommend granting **Read** privileges to all available permissions, and configuring the specific data you would like to replicate within Airbyte.
:::

### Step 1: Set up Stripe

1. Log in to your [Stripe account](https://dashboard.stripe.com/login).
2. In the top navigation bar, click **Developers**.
3. In the top-left corner, click **API keys**.
4. Click **+ Create restricted key**.
5. Choose a **Key name**, and select **Read** for all available permissions.
6. Click **Create key**. You may be prompted to enter a confirmation code sent to your email address.

For more information on Stripe API Keys, see the [Stripe documentation](https://stripe.com/docs/keys).

### Step 2: Set up the Stripe connector in Airbyte

<!-- env:cloud -->
### For Airbyte Cloud:

1. [Log into your Airbyte Cloud](https://cloud.airbyte.com/workspaces) account.
2. Click Sources and then click + New source.
3. On the Set up the source page, select Stripe from the Source type dropdown.
4. Enter a name for the Stripe connector.
<!-- /env:cloud -->
<!-- env:oss -->
### For Airbyte Open Source:

1. Navigate to the Airbyte Open Source dashboard.
2. Click Sources and then click + New source.
3. On the Set up the source page, select Stripe from the Source type dropdown.
4. Enter a name for the Stripe connector.
<!-- /env:oss -->
<!-- markdownlint-disable MD029 -->
5. For **Account ID**, enter your Stripe Account ID. This ID begins with \`acct_\`, and can be found in the top-right corner of your Stripe [account settings page](https://dashboard.stripe.com/settings/account).
6. For **Secret Key**, enter the restricted key you created for the connection.
7. For **Replication Start Date**, use the provided datepicker or enter a UTC date and time programmatically in the format \`YYYY-MM-DDTHH:mm:ssZ\`. The data added on and after this date will be replicated.
8. (Optional) For **Lookback Window**, you may specify a number of days from the present day to reread data. This allows the connector to retrieve data that might have been updated after its initial creation, and is useful for handling any post-transaction adjustments. This applies only to streams that do not support event-based incremental syncs, please see [the list below](#troubleshooting).

   - Leaving the **Lookback Window** at its default value of 0 means Airbyte will not re-export data after it has been synced.
   - Setting the **Lookback Window** to 1 means Airbyte will re-export data from the past day, capturing any changes made in the last 24 hours.
   - Setting the **Lookback Window** to 7 means Airbyte will re-export and capture any data changes within the last week.

9. (Optional) For **Data Request Window**, you may specify the time window in days used by the connector when requesting data from the Stripe API. This window defines the span of time covered in each request, with larger values encompassing more days in a single request. Generally speaking, the lack of overhead from making fewer requests means a larger window is faster to sync. However, this also means the state of the sync will persist less frequently. If an issue occurs or the sync is interrupted, a larger window means more data will need to be resynced, potentially causing a delay in the overall process.

   For example, if you are replicating three years worth of data:

   - A **Data Request Window** of 365 days means Airbyte makes 3 requests, each for a year. This is generally faster but risks needing to resync up to a year's data if the sync is interrupted.
   - A **Data Request Window** of 30 days means 36 requests, each for a month. This may be slower but minimizes the amount of data that needs to be resynced if an issue occurs.

   If you are unsure of which value to use, we recommend leaving this setting at its default value of 365 days.

10. (Optional) For **Streams with API Data Retention Validation**, select the streams whose cursor age the connector checks against Stripe's 30-day [Events API retention limit](https://stripe.com/docs/api/events). If a selected stream's cursor is older than 30 days, the connector runs a full refresh for that stream instead of an incremental sync that would miss older changes. By default, no streams are selected, so the connector never overrides a stream's configured sync mode based on cursor age. For advice on which streams benefit from this check, see [Cursor age validation and automatic full refresh](#cursor-age-validation-and-automatic-full-refresh).

11. (Optional) For **Number of Concurrent Threads**, enter the number of worker threads to use for the sync. The default is 10. You can set this to any value between 2 and 100. Higher values increase throughput but also increase API usage. The effective upper bound depends on your Stripe account's rate limits.

12. (Optional) For **Max Number of API Calls per Second**, enter the maximum number of API requests per second the connector is allowed to make. If not specified, the connector defaults to 25 calls per second for test and sandbox API keys and 100 calls per second for live API keys. This value cannot exceed Stripe's actual [rate limits](https://stripe.com/docs/rate-limits).

13. Click **Set up source** and wait for the tests to complete.

<!-- markdownlint-enable MD029 -->
<HideInUI>

## Supported sync modes

The Stripe source connector supports the following [sync modes](https://docs.airbyte.com/cloud/core-concepts/#connection-sync-modes):

- Full Refresh
- Incremental

## Supported Streams

The Stripe source connector supports the following streams:

- [Accounts](https://stripe.com/docs/api/accounts/list) \\(Incremental\\)
- [Application Fees](https://stripe.com/docs/api/application_fees) \\(Incremental\\)
- [Application Fee Refunds](https://stripe.com/docs/api/fee_refunds/list) \\(Incremental\\)
- [Authorizations](https://stripe.com/docs/api/issuing/authorizations/list) \\(Incremental\\)
- [Balance Transactions](https://stripe.com/docs/api/balance_transactions/list) \\(Incremental\\)
- [Bank accounts](https://stripe.com/docs/api/customer_bank_accounts/list) \\(Incremental\\)
- [Cardholders](https://stripe.com/docs/api/issuing/cardholders/list) \\(Incremental\\)
- [Cards](https://stripe.com/docs/api/issuing/cards/list) \\(Incremental\\)
- [Charges](https://stripe.com/docs/api/charges/list) \\(Incremental\\)
  :::note
  The \`amount\` column defaults to the smallest currency unit. Check [the Stripe docs](https://stripe.com/docs/api/charges/object) for more details.
  :::
- [Checkout Sessions](https://stripe.com/docs/api/checkout/sessions/list) \\(Incremental\\)
- [Checkout Sessions Line Items](https://stripe.com/docs/api/checkout/sessions/line_items) \\(Incremental\\)
- [Coupons](https://stripe.com/docs/api/coupons/list) \\(Incremental\\)
- [Credit Notes](https://stripe.com/docs/api/credit_notes/list) \\(Incremental\\)
- [Customer Balance Transactions](https://stripe.com/docs/api/customer_balance_transactions/list) \\(Incremental\\)
- [Customers](https://stripe.com/docs/api/customers/list) \\(Incremental\\)
- [Disputes](https://stripe.com/docs/api/disputes/list) \\(Incremental\\)
- [Early Fraud Warnings](https://stripe.com/docs/api/radar/early_fraud_warnings/list) \\(Incremental\\)
- [Events](https://stripe.com/docs/api/events/list) \\(Incremental\\)
- [External Account Bank Accounts](https://stripe.com/docs/api/external_account_bank_accounts/list) \\(Incremental\\)
- [External Account Cards](https://stripe.com/docs/api/external_account_cards/list) \\(Incremental\\)
- [File Links](https://stripe.com/docs/api/file_links/list) \\(Incremental\\)
- [Files](https://stripe.com/docs/api/files/list) \\(Incremental\\)
- [Invoice Items](https://stripe.com/docs/api/invoiceitems/list) \\(Incremental\\)
- [Invoice Line Items](https://stripe.com/docs/api/invoices/invoice_lines) \\(Incremental\\)
- [Invoices](https://stripe.com/docs/api/invoices/list) \\(Incremental\\)
- [Payment Intents](https://stripe.com/docs/api/payment_intents/list) \\(Incremental\\)
- [Payment Methods](https://docs.stripe.com/api/payment_methods/customer_list?lang=curl) \\(Incremental\\)
- [Payouts](https://stripe.com/docs/api/payouts/list) \\(Incremental\\)
- [Payout Balance Transactions](https://docs.stripe.com/api/balance_transactions/list) \\(Incremental\\)
  :::note
  This stream is built with a call using payout_id from the payout stream (parent) as a parameter to the balance transaction API to get balance transactions that comprised the actual amount of the payout. Check [the Stripe docs](https://docs.stripe.com/api/balance_transactions/list) for more details.
  :::
- [Promotion Codes](https://stripe.com/docs/api/promotion_codes/list) \\(Incremental\\)
- [Persons](https://stripe.com/docs/api/persons/list) \\(Incremental\\)
- [Plans](https://stripe.com/docs/api/plans/list) \\(Incremental\\)
- [Prices](https://stripe.com/docs/api/prices/list) \\(Incremental\\)
- [Products](https://stripe.com/docs/api/products/list) \\(Incremental\\)
- [Refunds](https://stripe.com/docs/api/refunds/list) \\(Incremental\\)
- [Reviews](https://stripe.com/docs/api/radar/reviews/list) \\(Incremental\\)
- [Setup Attempts](https://stripe.com/docs/api/setup_attempts/list) \\(Incremental\\)
- [Setup Intents](https://stripe.com/docs/api/setup_intents/list) \\(Incremental\\)
- [Shipping Rates](https://stripe.com/docs/api/shipping_rates/list) \\(Incremental\\)
- [Subscription Items](https://stripe.com/docs/api/subscription_items/list) \\(Incremental\\)
- [Subscription Schedule](https://stripe.com/docs/api/subscription_schedules) \\(Incremental\\)
- [Subscriptions](https://stripe.com/docs/api/subscriptions/list) \\(Incremental\\)
- [Top Ups](https://stripe.com/docs/api/topups/list) \\(Incremental\\)
- [Transactions](https://stripe.com/docs/api/issuing/transactions/list) \\(Incremental\\)
- [Transfers](https://stripe.com/docs/api/transfers/list) \\(Incremental\\)
- [Transfer Reversals](https://stripe.com/docs/api/transfer_reversals/list) \\(Incremental\\)
- [Usage Records](https://stripe.com/docs/api/usage_records)

### Entity-Relationship Diagram (ERD)
<EntityRelationshipDiagram></EntityRelationshipDiagram>

### Data type map

The [Stripe API](https://stripe.com/docs/api) uses the same [JSON Schema](https://json-schema.org/understanding-json-schema) types that Airbyte uses internally \\(\`string\`, \`date-time\`, \`object\`, \`array\`, \`boolean\`, \`integer\`, and \`number\`\\), so no type conversions are performed for the Stripe connector.

### Stripe API version

This connector uses Stripe API version \`2022-11-15\`. Stripe returns data shaped according to this version regardless of the version configured in your Stripe dashboard. For details on Stripe API versioning, see [Stripe API upgrades](https://docs.stripe.com/upgrades).

## Limitations & Troubleshooting

<details>
<summary>
Expand to see details about Stripe connector limitations and troubleshooting.
</summary>

### Connector limitations

#### Rate limiting

The Stripe connector should not run into Stripe API limitations under normal usage. See Stripe [Rate limits](https://stripe.com/docs/rate-limits) documentation. [Create an issue](https://github.com/airbytehq/airbyte/issues) if you see any rate limit issues that are not automatically retried successfully.

:::warning
**Stripe API Restriction on Events Data**: Access to the events endpoint is [guaranteed only for the last 30 days](https://stripe.com/docs/api/events) by Stripe. If you use the Full Refresh Overwrite sync, be aware that any events data older than 30 days will be **deleted** from your target destination and replaced with the data from the last 30 days only. Use an Append sync mode to ensure historical data is retained.
For the streams that rely on the Events API — the ones listed below as using the \`updated\` cursor — this also means incremental sync can't replicate a change that is more than 30 days old. To keep those streams current, sync at least once every 30 days.
:::

#### Cursor age validation and automatic full refresh

To prevent data loss caused by the 30-day Events API retention limit, the connector can validate the age of each stream's cursor before choosing between incremental and full refresh sync. If a stream's cursor is older than 30 days, the connector automatically falls back to a full refresh for that stream instead of using the Events API, which would only return the last 30 days of data.

**This behavior is configurable via the "Streams with API Data Retention Validation" setting** (see [setup guide](#step-2-set-up-the-stripe-connector-in-airbyte) step 10). Only streams listed in this setting will have their cursor age validated. By default, no streams are selected — all streams will use incremental sync without cursor age validation.

For high-usage streams like \`Charges\`, \`Invoice Items\`, \`Invoice Line Items\`, \`Invoices\`, \`Payment Intents\`, and \`Payouts\`, enabling cursor age validation is recommended since a stale cursor likely indicates missed data rather than normal inactivity. Streams like \`Customers\`, \`Subscriptions\`, \`Products\`, and \`Plans\` may not need validation because some accounts legitimately have no new records in 30+ days, making a full refresh unnecessary.

You can customize which streams have cursor age validation by modifying the **Streams with API Data Retention Validation** list in your connection settings. The full list of streams eligible for cursor age validation is:

- \`Accounts\`
- \`Application Fees\`
- \`Application Fee Refunds\`
- \`Authorizations\`
- \`Bank Accounts\`
- \`Cardholders\`
- \`Charges\`
- \`Checkout Sessions\`
- \`Coupons\`
- \`Credit Notes\`
- \`Customers\`
- \`Disputes\`
- \`Early Fraud Warnings\`
- \`External Account Bank Accounts\`
- \`External Account Cards\`
- \`Invoice Items\`
- \`Invoice Line Items\`
- \`Invoices\`
- \`Payment Intents\`
- \`Payment Methods\`
- \`Payouts\`
- \`Persons\`
- \`Plans\`
- \`Prices\`
- \`Products\`
- \`Promotion Codes\`
- \`Refunds\`
- \`Reviews\`
- \`Setup Intents\`
- \`Subscription Items\`
- \`Subscription Schedule\`
- \`Subscriptions\`
- \`Top Ups\`
- \`Transactions\`
- \`Transfers\`

:::warning
**Important**: If a stream is removed from the validation list and its cursor becomes stale (older than 30 days), the connector will continue using the Events API for incremental sync, which only returns the last 30 days of data. This may result in missed updates for records older than 30 days. Only remove streams from the validation list if you are confident that a stale cursor is acceptable for your use case.
:::

### Troubleshooting

Since the Stripe API does not allow querying objects which were updated since the last sync, the Stripe connector uses the Events API under the hood to implement incremental syncs and export data based on its update date.
However, not all the entities are supported by the Events API, so the Stripe connector uses the \`created\` field or its analogue to query for new data in your Stripe account. These are the entities synced based on the date of creation:

- \`Balance Transactions\`
- \`Customer Balance Transactions\`
- \`Events\`
- \`File Links\`
- \`Files\`
- \`Setup Attempts\`
- \`Shipping Rates\`
- \`Transfer Reversals\`

On the other hand, the following streams use the \`updated\` field value as a cursor:

:::note

\`updated\` is an artificial cursor field Airbyte adds for incremental sync. Stripe doesn't return it.

For records the connector rebuilds from the Events API, \`updated\` comes from the event timestamp, which Stripe records with one-second resolution. Records derived from a \`*.created\` event get a cursor one second earlier than the event itself. That offset breaks ties: when a create and an update for the same object happen in the same second, the update sorts higher, so deduplicating destinations keep the update's payload rather than the older creation payload.

:::

- \`Accounts\`
- \`Application Fees\`
- \`Application Fee Refunds\`
- \`Authorizations\`

</details>

## Changelog

<details>
  <summary>Expand to review</summary>

| Version     | Date       | Pull Request                                                 | Subject                                                                                                                                                                                                                       |
|:------------|:-----------|:-------------------------------------------------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 6.0.15 | 2026-08-18 | [84768](https://github.com/airbytehq/airbyte/pull/84768) | Update dependencies |
| 6.0.14 | 2026-08-17 | [84355](https://github.com/airbytehq/airbyte/pull/84355) | Update events now win same-second cursor ties with creation events so the newer payload is kept at the destination. |
| 6.0.13 | 2026-08-11 | [84134](https://github.com/airbytehq/airbyte/pull/84134) | Update dependencies |
| 6.0.12 | 2026-08-04 | [83634](https://github.com/airbytehq/airbyte/pull/83634) | Update dependencies |
| 6.0.11 | 2026-07-28 | [83119](https://github.com/airbytehq/airbyte/pull/83119) | Update dependencies |
| 6.0.10 | 2026-07-21 | [82611](https://github.com/airbytehq/airbyte/pull/82611) | Update dependencies |
| 6.0.9 | 2026-07-14 | [82034](https://github.com/airbytehq/airbyte/pull/82034) | Update dependencies |
| 6.0.8 | 2026-06-30 | [81285](https://github.com/airbytehq/airbyte/pull/81285) | Update dependencies |
| 6.0.7 | 2026-06-23 | [80686](https://github.com/airbytehq/airbyte/pull/80686) | Update dependencies |
| 6.0.6 | 2026-06-16 | [80070](https://github.com/airbytehq/airbyte/pull/80070) | Update dependencies |
| 6.0.5 | 2026-06-09 | [79523](https://github.com/airbytehq/airbyte/pull/79523) | Update dependencies |
| 6.0.4 | 2026-06-03 | [79042](https://github.com/airbytehq/airbyte/pull/79042) | Update dependencies |
| 6.0.3 | 2026-04-28 | [77424](https://github.com/airbytehq/airbyte/pull/77424) | Update dependencies |
| 6.0.2 | 2026-04-21 | [76777](https://github.com/airbytehq/airbyte/pull/76777) | Update dependencies |
| 6.0.1 | 2026-04-13 | [76276](https://github.com/airbytehq/airbyte/pull/76276) | Rename "concurrent workers" to "concurrent threads" in connector spec |
| 6.0.0 | 2026-04-13 | [76095](https://github.com/airbytehq/airbyte/pull/76095) | Fix missing records in \`invoice_line_items\` and \`subscription_items\` incremental streams by replacing \`DpathFlattenFields\` with \`RecordExpander\` to correctly expand nested line items from Stripe events. This is a breaking change — previously synced data for these streams may be incomplete. See the [migration guide](https://docs.airbyte.com/integrations/sources/stripe-migrations#upgrading-to-600) for details. |
| 5.15.23 | 2026-03-31 | [75864](https://github.com/airbytehq/airbyte/pull/75864) | Update dependencies |
| 5.15.22 | 2026-03-12 | [74770](https://github.com/airbytehq/airbyte/pull/74770) | Upgrade CDK to 7.13.0 |
| 5.15.21 | 2026-03-06 | [74342](https://github.com/airbytehq/airbyte/pull/74342) | Promoting release candidate 5.15.21-rc.5 to a main version. |
| 5.15.21-rc.5 | 2026-03-06 | [74337](https://github.com/airbytehq/airbyte/pull/74337) | Make API data retention validation optional per stream via new \`api_retention_streams\` config field, upgrade CDK to 7.8.1.post54 |
| 5.15.21-rc.4 | 2026-03-04 | [74290](https://github.com/airbytehq/airbyte/pull/74290) | Lower default concurrency from 25 to 10 and increase default data request time increment from 30 to 365 days to reduce rate limiting |
| 5.15.21-rc.3 | 2026-03-03 | [74259](https://github.com/airbytehq/airbyte/pull/74259) | Fix cursor age validation to clear state before constructing full refresh stream, ensuring true full refresh from start_date |
| 5.15.21-rc.2 | 2026-02-25 | [74051](https://github.com/airbytehq/airbyte/pull/74051) | Fix sync failure when unselected parent streams have stale cursor state during cursor age validation |
| 5.15.21-rc.1 | 2026-02-25 | [73646](https://github.com/airbytehq/airbyte/pull/73646) | Add cursor age validation for StateDelegatingStream streams to prevent data loss when initial sync fails mid-way |
| 5.15.20 | 2026-02-24 | [73944](https://github.com/airbytehq/airbyte/pull/73944) | Update dependencies |
| 5.15.19 | 2026-02-17 | [73466](https://github.com/airbytehq/airbyte/pull/73466) | Update dependencies |
| 5.15.18 | 2026-02-12 | [73318](https://github.com/airbytehq/airbyte/pull/73318) | Promoting release candidate 5.15.18-rc.1 to a main version. |
| 5.15.18-rc.1 | 2026-02-04 | [72432](https://github.com/airbytehq/airbyte/pull/72432) | fix(source-stripe):  Fix Missing ConcurrencyLevel & Reduce Default Step Size |
| 5.15.17 | 2026-01-27 | [72363](https://github.com/airbytehq/airbyte/pull/72363) | fix(source-stripe): Add date filtering for checkout_sessions full refresh |
| 5.15.16 | 2026-01-20 | [72106](https://github.com/airbytehq/airbyte/pull/72106) | Update dependencies |
| 5.15.15 | 2026-01-14 | [71614](https://github.com/airbytehq/airbyte/pull/71614) | Update dependencies |
| 5.15.14 | 2025-12-18 | [70638](https://github.com/airbytehq/airbyte/pull/70638) | Update dependencies |
| 5.15.13 | 2025-11-25 | [70048](https://github.com/airbytehq/airbyte/pull/70048) | Update dependencies |
| 5.15.12 | 2025-11-18 | [69580](https://github.com/airbytehq/airbyte/pull/69580) | Update dependencies |
| 5.15.11 | 2025-10-29 | [68994](https://github.com/airbytehq/airbyte/pull/68994) | Update dependencies |
| 5.15.10 | 2025-10-21 | [68527](https://github.com/airbytehq/airbyte/pull/68527) | Update dependencies |
| 5.15.9 | 2025-10-14 | [67472](https://github.com/airbytehq/airbyte/pull/67472) | Update dependencies |
| 5.15.8 | 2025-09-30 | [66891](https://github.com/airbytehq/airbyte/pull/66891) | Update dependencies |
| 5.15.7 | 2025-09-24 | [66367](https://github.com/airbytehq/airbyte/pull/66367) | Update dependencies |
| 5.15.6 | 2025-09-09 | [66115](https://github.com/airbytehq/airbyte/pull/66115) | Update dependencies |
| 5.15.5 | 2025-08-24 | [65489](https://github.com/airbytehq/airbyte/pull/65489) | Update dependencies |
| 5.15.4 | 2025-08-10 | [64840](https://github.com/airbytehq/airbyte/pull/64840) | Update dependencies |
| 5.15.3 | 2025-08-04 | [64484](https://github.com/airbytehq/airbyte/pull/64484) | Fix memory issue by moving schema loaders out of $parameters |
| 5.15.2 | 2025-08-03 | [64439](https://github.com/airbytehq/airbyte/pull/64439) | Update dependencies |
| 5.15.1 | 2025-07-26 | [60561](https://github.com/airbytehq/airbyte/pull/60561) | Update dependencies |
| 5.15.0 | 2025-07-23 | [63743](https://github.com/airbytehq/airbyte/pull/63743) | Promoting release candidate 5.15.0-rc.1 to a main version. |
| 5.15.0-rc.1 | 2025-07-21 | [63370](https://github.com/airbytehq/airbyte/pull/63370) | Migrate to manifest-only format. |
| 5.14.1 | 2025-07-15 | [62893](https://github.com/airbytehq/airbyte/pull/62893) | Increase the timeout for syncs that fail without any records to one day. |
| 5.14.0 | 2025-07-15 | [63303](https://github.com/airbytehq/airbyte/pull/63303) | Promoting release candidate 5.14.0-rc.1 to a main version. |
| 5.14.0-rc.1 | 2025-06-12 | [60846](https://github.com/airbytehq/airbyte/pull/60846) | Rollback Low Code per partition streams; update slicer for invoice_line_items and normalization for events based streams |
| 5.13.0 | 2025-05-22 | [60846](https://github.com/airbytehq/airbyte/pull/60846) | Update subscription_items and usage_records stream to python implementation |
| 5.12.0 | 2025-05-12 | [59743](https://github.com/airbytehq/airbyte/pull/59743) | Update invoice_line_items stream to python implementation |
| 5.11.3 | 2025-05-10 | [60053](https://github.com/airbytehq/airbyte/pull/60053) | Update dependencies |
| 5.11.2 | 2025-05-04 | [59645](https://github.com/airbytehq/airbyte/pull/59645) | Update dependencies |
| 5.11.1 | 2025-04-27 | [58979](https://github.com/airbytehq/airbyte/pull/58979) | Update dependencies |
| 5.11.0 | 2025-04-24 | [58637](https://github.com/airbytehq/airbyte/pull/58637) | Promoting release candidate 5.11.0-rc.2 to a main version. |
| 5.11.0-rc.2 | 2025-04-18 | [58136](https://github.com/airbytehq/airbyte/pull/58136) | Enable progressive rollout |
| 5.11.0-rc.1 | 2025-04-18 | [54162](https://github.com/airbytehq/airbyte/pull/54162) | Migrate to low-code |
| 5.10.1 | 2025-04-17 | [58124](https://github.com/airbytehq/airbyte/pull/58124) | Extend safe state to support nested states |
| 5.10.0 | 2025-04-17 | [58117](https://github.com/airbytehq/airbyte/pull/58117) | Promoting release candidate 5.10.0-rc.1 to a main version. |

</details>

</HideInUI>
`;
