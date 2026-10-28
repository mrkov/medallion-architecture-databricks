# Guide alignment and runnable examples

Business data follows **Bronze → Silver → Gold → Platinum**. Contracts reject skipped-layer and same-layer
reads. Pipeline boundaries follow source ownership, Gold business domains, and Platinum departments.

| Guide rule | POC implementation |
|---|---|
| Independent source timing | `ingest_erp`, `ingest_crm`, `ingest_ads`; each runs its source's Bronze/Silver pipeline |
| Gold by business domain | Eight commerce outputs in `gold_pipeline`; root ODCS orchestration supports additional domain pipelines |
| Platinum by department | Sales ordinary views; Marketing dimension view plus materialized campaign aggregate |
| Eventual refresh | `refresh_gold` watches Silver; `refresh_platinum_marketing` watches only its Gold inputs |
| Daily completion | Source load receipts and `refresh_gold_daily_commerce`; optional daily policy/schedule per domain |
| One shared key definition | Contract `key_definitions`, compiled by `medallion_poc.keys`; normalization in Silver |
| Missing dimension values | UNKNOWN row per dimension; present-but-late IDs retain their deterministic key |
| SCD2 | Gold facts store entity keys; Platinum resolves a version with half-open timestamp intervals |
| Fact grain | Natural primary keys, informational MV constraints and failing runtime uniqueness checks |
| Collision safeguards | Each hashed dimension key is checked against its business identity in the input-derived query |

The one commerce pipeline is appropriate for this small domain. Splitting those eight related outputs into
per-table pipelines would add orchestration. Sales has a publication job rather than a compute pipeline
because all its outputs are ordinary views. Marketing has both a publication job and an MV refresh pipeline.

## Upgrade an existing v2 deployment

Use the same catalog, bundle identity, target and deployment user as before. Source pipeline resource keys
are preserved. Do not rename or transfer source tables to a new pipeline.

This is a breaking interface change: SHA256 keys become BINARY, Gold facts remove `customer_version_key`,
and the Marketing campaign aggregate becomes an MV. Allow a maintenance window; reports may be unavailable
until the rebuild and view publication finish. Stop source schedules, pause automatic refresh, and let all
active source, Gold and Platinum updates finish before starting the upgrade.

```bash
git pull
uv sync --locked
.venv/bin/python scripts/generate_bundle_resources.py --bootstrap

databricks bundle deploy --profile medallion-poc -t dev \
  --var catalog=main --var gold_trigger_status=PAUSED --var platinum_trigger_status=PAUSED

databricks bundle run upgrade_poc --profile medallion-poc -t dev --var catalog=main

.venv/bin/python scripts/generate_bundle_resources.py
databricks bundle deploy --profile medallion-poc -t dev \
  --var catalog=main --var gold_trigger_status=UNPAUSED --var platinum_trigger_status=UNPAUSED
```

`--bootstrap` removes both sets of table watchers so the new watcher can be installed after its inputs exist.
It preserves the existing job/pipeline resource IDs. It does not remove optional schedules you have configured;
pause those separately in Jobs for maintenance.

`upgrade_poc` prepares storage, runs source pipelines **without full refresh**, then fully refreshes derived
Gold and Platinum MVs and publishes ordinary views. Bronze records, landing files and source checkpoints
remain intact. The preparation step drops only an existing ordinary view at the name of a contracted
Platinum MV; it never drops source data or an existing MV. Reapply any grants you had attached to that old
Marketing view. If an upgrade task fails, repair the job before enabling watchers again.

For a fresh catalog, follow [README setup](../README.md#deploy-and-set-up) and run `setup_poc` instead.
After setup, ordinary source jobs can run independently. No `schema_prefix` variable is used.

## Dimension and fact keys

The POC defaults to SHA256 stored as 32-byte BINARY, rather than 64-character hexadecimal text. Namespaces
separate ERP customers, ERP products, Ads campaigns and customer versions. Facts and dimensions compile
the same `poc_key(...)` definition. This is deterministic key derivation, not an allocated-ID mapping table.
CRM already provides `erp_customer_id`, so this example does not need entity matching or a crosswalk.
Unrelated identifiers across real systems would need an upstream master-ID mapping before hashing.

Silver contracts apply `nullif(upper(trim(...)), '')` to the relevant business identifiers. This is an explicit
business rule for these sample systems; case-sensitive real identifiers need a different contract rule.
Bronze retains the original strings.

| Fact's dimension identifier | Fact key and later join |
|---|---|
| Null/blank after contracted normalization | Family's UNKNOWN key; matches the dimension's UNKNOWN member |
| Present, dimension not available yet | Entity's deterministic key; LEFT JOIN keeps the fact with UNMATCHED status |
| Same present identifier, dimension arrives later | Same fact key; join becomes MATCHED after automatic refresh |

The compiler hashes one tagged JSON value, so delimiter characters and component boundaries cannot silently
change an identity. UNKNOWN is a separate payload kind; a real identifier literally named `UNKNOWN` is
not the missing-value member. Each dimension has a collision check, and every declared grain is checked.
The history dimension checks both its entity hash and version hash. Current-state checks only cover
identities retained in the output; lifetime collision auditing would require a retained key registry.

`xxhash64` is supported for a deliberate BIGINT key choice. Change the shared definition **and all producer
and consumer property types** (BIGINT / ODCS integer), then rebuild affected outputs together. Do not change
only a dimension. A 64-bit hash has collision risk; measure cardinality and keep collision checks. Integer
joins can be cheaper, but engine plans and data sizes matter more than a blanket speed claim.

Facts use natural grains such as `order_line_id`, without a generated fact-row ID. The contract declares
that grain; runtime checks fail duplicates. Informational MV primary keys help consumers understand it,
but do not enforce uniqueness. No identity columns or row-number-generated entity keys are used.

## Run the missing/late dimension demo

After setup/upgrade, enable automatic Gold refresh and run:

```bash
databricks bundle run key_handling_demo --profile medallion-poc -t dev --var catalog=main
```

It writes contract-defined ERP samples and runs only `ingest_erp`. Observation tasks wait for Gold to update
through its independent trigger. Both facts remain visible while the late dimension is absent. The second
stage adds customer/product rows and demonstrates canonicalization of the original padded, lowercase IDs.

```sql
SELECT order_line_id, customer_status, product_status, product_name
FROM main.platinum_sales.sales_detail
WHERE order_line_id IN ('OL_KEY_NULL', 'OL_KEY_LATE');

SELECT order_line_id, customer_id, hex(customer_key) AS customer_key_hex
FROM main.gold_common.fact_sales
WHERE order_line_id IN ('OL_KEY_NULL', 'OL_KEY_LATE');
```

The final statuses are UNKNOWN for `OL_KEY_NULL` and MATCHED for `OL_KEY_LATE`. Their amounts and fact keys
do not change when dimensions arrive. Run this demo after the original enrichment demo if showing both;
the enrichment demo requires initially empty source data. Each fixture demo guards against reuse of its own
sample IDs and preserves raw history. Repair a failed run or use another catalog for a complete repeat.

The separate [SCD2 demo](scd2.md) shows historical enrichment. Gold facts retain stable entity keys while
Platinum resolves `[valid_from, valid_to)`, including a late historical correction. A known customer with
missing history remains MISSING_HISTORY; only a missing customer ID maps to the timeless UNKNOWN member.

## Daily completion

Each source job writes STARTED, runs its pipeline through Silver, then writes SUCCEEDED or FAILED to
`<catalog>.bronze_operations.load_events`. These are append-only operational receipts. Success can mean
zero new rows: a valid empty delivery still completes a batch. Running a source pipeline directly bypasses
receipts; use its `ingest_*` job when demonstrating the gate. Setup initializes datasets but does not certify
a business day's deliveries.

Receipts are scoped by business date and load group. The latest started attempt for every required source
must have succeeded; an older success cannot hide a newer failure or in-progress load. The date defaults to
the configured `Europe/Belgrade` business timezone and is fixed at the STARTED task, including overnight runs.
For backfills and late deliveries, supply the intended business date explicitly:

```bash
databricks bundle run ingest_erp --profile medallion-poc -t dev --var catalog=main \
  --params business_date=2026-10-01,load_group=standard
```

In production, the source job must start only when the producer has confirmed delivery completeness.
Auto Loader finding no new files cannot distinguish an empty delivery from one that has not arrived.
That producer acknowledgement is a source-specific integration, beyond these sample-file pipelines.

To demonstrate completion gating separately from eventual refresh, pause the automatic Gold trigger,
let any active Gold run finish, and run:

```bash
databricks bundle deploy --profile medallion-poc -t dev \
  --var catalog=main --var gold_trigger_status=PAUSED --var platinum_trigger_status=UNPAUSED
databricks bundle run daily_gate_demo --profile medallion-poc -t dev --var catalog=main
```

The demo uses a unique load group and the contract's sample business date. It verifies the gate is blocked,
runs ERP and CRM, verifies Ads is still missing, then runs Ads and passes the gate. It creates no new sample
files, demonstrating completion even when all files were processed previously. The final wrapper invokes
the same serialized `refresh_gold` job used by automatic refresh. Marketing then refreshes on Gold changes;
ordinary views need no refresh. Restore normal mode afterwards:

```bash
databricks bundle deploy --profile medallion-poc -t dev \
  --var catalog=main --var gold_trigger_status=UNPAUSED --var platinum_trigger_status=UNPAUSED
```

For an actual daily domain, change root `orchestration.gold_domains.commerce` in the Gold ODCS contract:

```yaml
commerce:
  pipeline_key: gold_pipeline
  job_key: refresh_gold
  refresh: daily
  required_sources: [erp, crm, ads]
  schedule:
    quartz_cron_expression: '0 0 2 * * ?'
    timezone_id: Europe/Belgrade
    pause_status: PAUSED
  # Optional: failure_emails: [your-operations-address]
```

Regenerate resources and deploy. This replaces that domain's table trigger with a gated scheduled job;
enable its schedule after selecting a real deadline. The example 02:00 schedule uses the current business
date unless you set the job's `business_date` parameter; it does not automatically mean the previous day.
A scheduled run fails if a source is incomplete, allowing configured failure notifications. With no
schedule, a daily-policy manual run skips Gold when the gate is incomplete. The explicit
`refresh_gold_daily_commerce` wrapper always fails when incomplete. Re-run after the late source succeeds.
Source schedules can be declared separately under `orchestration.source_schedules.<source>` using the same
schedule shape. Real deadlines and notification destinations remain deployment choices.

This gate is a **completion barrier**, not a frozen, transactional daily snapshot. It reads the latest Silver
state and does not pin Delta versions; a concurrent source update may change inputs after the check. A
regulated snapshot also needs versioned batch inputs or an enforced freeze window and snapshot publication.

## Validation and runtime limits

Local tests execute contract SQL with Spark, including missing/late dimensions, temporal SCD2 joins,
duplicate-grain failures, simulated hash collisions, canonicalization and business-date gate isolation.
Contract validation and bundle-generation drift checks run in CI. Notebook tests cover the resolved receipt
date so job-parameter pushdown cannot replace it with an empty date.

Live Databricks deployment requires workspace credentials and remains a separate check. Table triggers,
MV refresh plans and informational PK schema support depend on the runtime. If informational declarations
are unsupported, deploy with `--var informational_keys=false`; runtime grain/collision checks remain active.

Serverless MV refresh is not guaranteed to be incremental or inexpensive for these queries. History and
quality windows can require full recomputation, and receipt notebooks add job startup overhead. Inspect
pipeline event logs and billing. For daily sources, batching refreshes behind the daily gate can reduce
repeated work compared with reacting to every Silver change.
