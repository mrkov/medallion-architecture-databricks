# Four-layer Databricks POC driven by ODCS

An Ifolor-style ERP, CRM and advertising example using Unity Catalog, Lakeflow Declarative Pipelines,
Lakeflow Jobs and ODCS v3.2.0. Source jobs run independently and finish at Silver. A **table-update trigger**
observes Silver changes and starts Gold. Platinum contains ordinary views over Gold.

```mermaid
flowchart LR
  ERP[ERP job] --> BE[bronze_erp] --> SE[silver_erp]
  CRM[CRM job] --> BC[bronze_crm] --> SC[silver_crm]
  Ads[Ads job] --> BA[bronze_ads] --> SA[silver_ads]
  SE --> Trigger[Silver table-update trigger]
  SC --> Trigger
  SA --> Trigger
  Trigger --> Gold[Gold pipeline: gold_common]
  Gold --> Sales[platinum_sales views]
  Gold --> Marketing[platinum_marketing views]
```

## Schemas and ownership

| Schema | Contents | Owner |
|---|---|---|
| `bronze_erp`, `bronze_crm`, `bronze_ads` | Append-only streaming tables, all payload columns strings; a `landing` volume in each schema | Respective source pipeline |
| `silver_erp`, `silver_crm`, `silver_ads` | Typed, deduplicated materialized views and `contract_quarantine` | Respective source pipeline |
| `gold_common` | Persisted facts/dimensions and `data_readiness`, implemented as Lakeflow materialized views | Gold pipeline |
| `platinum_sales` | Ordinary views: `fact_sales`, `sales_detail`, `dim_customer`, `dim_product` | Published from ODCS |
| `platinum_marketing` | Ordinary views: `fact_campaign_performance`, `dim_campaign` | Published from ODCS |

There is **no schema prefix**. Set `catalog` to isolate a deployment; schema names are exactly as shown.
Gold materialized views store their results in Delta, as agreed for this POC. They are not ordinary virtual
views. Every Gold definition reads Silver directly; every Platinum view reads Gold directly. Platinum
views evaluate their Gold inputs when queried and require no data refresh job.

Each source has its own `src/pipelines/<source>/bronze.py` and `silver.py`. These small entrypoints register
that source's contracted datasets through shared code. All four pipelines use serverless compute.
Source jobs have no schedule in this POC; each can acquire an independent schedule or arrival trigger.

## What makes Gold refresh automatically

The `refresh_gold` job has a native `table_update` trigger:

- It monitors the seven Silver interfaces referenced by Gold contracts, with `ANY_UPDATED` semantics.
- It waits for 60 seconds without another watched update and allows at least 120 seconds between triggers.
- Job concurrency is one, with queueing enabled.
- Its sole task starts a normal Gold pipeline update. Source jobs never call this job.

The **Jobs trigger detects changes and starts execution**. Lakeflow then uses its dependency graph and
refresh planner to maintain the Gold materialized views. The optimizer may reuse unchanged results,
incrementally refresh, or recompute an output; incremental execution is not guaranteed for every query.
Inspect the Gold pipeline event log for the actual plan and refresh costs.

This is eventual, progressive enrichment: a debounce window is not an atomic multi-source batch barrier.
A source can update several Silver tables at different moments. Gold can first show incomplete data and
converge on a later update. `fact_attributed_sales` exposes `ERP_ONLY`, `CRM_ENRICHED`, then `COMPLETE`.
`data_readiness` reports which sources have records and their latest timestamps. It does not certify that
all data for a business period has arrived. A strict all-sources-complete process needs explicit source
batch/completion markers and a readiness gate.

For a larger deployment, use refresh groups based on shared dependencies and freshness requirements, and
increase the batching window where appropriate. Do not assume one trigger can watch an unlimited number
of tables: check the workspace's current table-trigger limits. The seven-table example stays small.

## Contracts are the executable source of truth

The runtime reads the YAML with the official
[`open-data-contract-standard`](https://github.com/datacontract/open-data-contract-standard-python) library.
The library supplies the ODCS models; this project's generic interpreter executes the documented SQL subset.
ODCS itself is not a transformation engine.

| Contract field | Runtime use |
|---|---|
| `schema[].physicalName` | Output table/view address and schema creation |
| `properties[].name`, `logicalType`, `physicalType` | Ordered output columns and Spark SQL types |
| `properties[].transformLogic` | Executed Spark SQL expression for that output column |
| `properties[].transformSourceObjects` | Column lineage documentation, checked against declared object inputs |
| `required`, `enum` | Lakeflow expectations and Silver rejection rules |
| `primaryKey`, `primaryKeyPosition` | Silver deduplication keys and declared output grain |
| `customProperties.poc.inputs` / `from_sql` | Input references, joins, subqueries and aggregations |
| `customProperties.poc.order_by` | Silver's deterministic latest-record ordering |
| `customProperties.poc.row_checks` | Named, row-level SQL predicates; false/null causes rejection |
| `customProperties.poc.demo_rows` on source objects | Sample records for the staged demo |

The `poc` values are an **ODCS custom-property extension defined by this repository**, not a portable ODCS
execution standard. See [the execution profile](docs/contract-execution.md) for supported syntax and limits.
Contract files are trusted, reviewed code: changing `transformLogic` changes executed SQL. The loader checks
that object reads use declared input placeholders and the correct layer. It does not sandbox SQL functions.

Silver applies tolerant casts, records invalid rows in its source's quarantine, and keeps the latest valid
row per primary key. Gold uses strict casts and failing expectations. Primary-key metadata does not create
a database-enforced uniqueness constraint on a materialized view; the transformations and integration
tests enforce/check the declared grains. Platinum projections specify columns rather than `SELECT *`.

The product split is executable YAML in [`contracts/silver/erp.odcs.yaml`](contracts/silver/erp.odcs.yaml):

```yaml
name: product_family
physicalType: STRING
transformLogic: trim(try_element_at(split(src.product_hierarchy, '[|]', -1), 1))
```

The four-part input `PHOTOBOOK|PREMIUM|A4|LANDSCAPE` produces family, tier, format and orientation.
Missing/extra/empty segments are quarantined. `try_element_at` and `TRY_CAST` prevent invalid inputs from
crashing under ANSI SQL. The raw hierarchy and original rejected payload remain available for diagnosis.

There are no business column lists, joins, SQL view definitions or embedded CSV strings in notebooks.
`demo_control.py` was removed. Generic notebooks handle storage setup, view publication and example-file
arrival. `demo/scenario.yaml` contains observations/expected results, not production transformations.

## Deploy and set up

Requirements: Databricks CLI, Python 3.11+ and uv to render deployment resources, a Unity Catalog catalog,
permission to create schemas/volumes and pipelines, serverless compute, and workspace support for table-update
triggers on the Silver materialized views.
The job run identity needs access to those Silver objects. Use your existing `medallion-poc` CLI profile.

This revision uses the new bundle identity **`medallion-architecture-poc-v2`**. The old deployment and its
`medallion_poc_*` schemas remain untouched. Pause old scheduled jobs before moving to the new POC.
Do not delete or transfer ownership of the old tables. Data is not migrated automatically: upload source
files to the new volumes or use the sample-arrival demo. The new source pipelines have fresh checkpoints.

For the first deployment, generate resources without a table watcher. This avoids asking the Jobs API to
watch Silver tables before they exist. Enable the trigger after setup:

```bash
git pull
UV_CACHE_DIR=/tmp/medallion-uv-cache uv sync --locked
.venv/bin/python scripts/generate_bundle_resources.py --bootstrap

databricks bundle deploy --profile medallion-poc -t dev \
  --var catalog=main --var gold_trigger_status=PAUSED

databricks bundle run setup_poc --profile medallion-poc -t dev \
  --var catalog=main

.venv/bin/python scripts/generate_bundle_resources.py
databricks bundle deploy --profile medallion-poc -t dev \
  --var catalog=main --var gold_trigger_status=UNPAUSED
```

`setup_poc` creates missing schemas/volumes/folders, initializes all three source pipelines, runs Gold once
to bootstrap its interfaces, and publishes the Platinum views. It uses no full refresh and deletes no data.
Existing files are processed incrementally. The bootstrap generation is temporary; normal generation restores
the checked-in resources with the trigger, preserving the same job/pipeline IDs. The Gold trigger must be absent
or paused while running setup to avoid concurrent bootstrap and triggered updates. The setup notebook alone only provisions storage;
use the setup **job** for the complete initialization.

The explicit resource `pause_status` is controlled by `gold_trigger_status`; the development target also
allows unpaused triggers. Check **Gold on Silver changes → Schedules & Triggers** after deployment.
Normal deployments default to UNPAUSED. Use `--var gold_trigger_status=PAUSED` when pausing for setup/maintenance.
The setup does not require an API credential inside a notebook or notebook-driven changes to job schedules.

## Demonstrate separate arrivals

After setup and enabling the trigger, run:

```bash
databricks bundle run medallion_poc_demo --profile medallion-poc -t dev \
  --var catalog=main
```

The demo stages ERP examples, runs `ingest_erp`, then **only observes** the Gold/Platinum results until
`ERP_ONLY` appears. It repeats for CRM (`CRM_ENRICHED`) and Ads (`COMPLETE`). It never calls the Gold pipeline
or `refresh_gold`. If the automatic trigger is paused or broken, the observation task fails after 15 minutes
with a diagnostic rather than silently refreshing Gold itself. Startup and trigger delays can take minutes.

The same `OL100` order ultimately has CHF 59.80 attributed revenue, CHF 20.00 ad spend and ROAS 2.99.
This example uses a single currency; multi-currency reporting needs currency-aware grain/conversion rules. `P_BAD`
is retained in Bronze and quarantined in Silver. Ads also supplies an undeclared `placement_type`, retained
in Bronze `_rescued_data` while the Silver interface stays stable.

Run the staged sequence against an initially empty POC catalog. The demo checks that Gold readiness reports no existing
Silver records before beginning; it does not clear landing files, reset checkpoints or delete raw records.
For another full demonstration use another catalog. Identical example files are content-addressed and
never overwritten; ingestion retries do not append the same file again.

Outside the demo, upload immutable new CSV files and run only the corresponding source job:

```text
/Volumes/main/bronze_erp/landing/erp_orders/<new-file>.csv
/Volumes/main/bronze_crm/landing/crm_contacts/<new-file>.csv
/Volumes/main/bronze_ads/landing/ads_campaigns/<new-file>.csv
```

```bash
databricks bundle run ingest_crm --profile medallion-poc -t dev --var catalog=main
```

That job stops at Silver. Watch the independently triggered `refresh_gold` run, then query:

```sql
SELECT order_line_id, completeness_status FROM main.gold_common.fact_attributed_sales;
SELECT * FROM main.platinum_sales.sales_detail;
SELECT * FROM main.platinum_marketing.fact_campaign_performance;
```

After changing a Platinum contract, run `publish_platinum_views` once to replace its view definitions.
Data changes in Gold require no view republication. After changing a Gold expression without a Silver data
change, explicitly run `refresh_gold` once: table-update triggers monitor data commits, not YAML edits.

## Edit and validate

Python 3.11+ and Java 17+ are required for local Spark tests:

```bash
UV_CACHE_DIR=/tmp/medallion-uv-cache uv sync --locked --extra dev
.venv/bin/python scripts/fetch_odcs_schema.py
.venv/bin/python scripts/validate_contracts.py
.venv/bin/python scripts/generate_bundle_resources.py
.venv/bin/pytest
.venv/bin/ruff check .
```

Runtime transformations read YAML directly. `generate_bundle_resources.py` only produces deployment
resources: source pipeline/job definitions and the Gold trigger's watched tables. Commit its generated
`resources/medallion_poc.yml` after changing contract names or dependencies. CI checks for drift. `--bootstrap` temporarily omits the watcher for first-time setup; restore normal
generation before committing or enabling automatic refresh.
When adding a source, also add its two small pipeline entrypoints following the existing source folders.

Local tests execute the actual Silver/Gold/Platinum SQL under Spark ANSI mode, including the three arrival
stages, output types/grains, invalid values and a changed contract expression/output column. Registration
tests simulate Lakeflow without `__file__`; setup and example-file tests check repeatability. These tests
cannot prove Auto Loader behavior, native trigger delivery, permissions or Lakeflow incremental refresh
plans: the staged Databricks job is the workspace acceptance test.
