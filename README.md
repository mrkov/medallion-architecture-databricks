# Four-layer Databricks medallion POC

This repository demonstrates an Ifolor-style commerce platform implemented with Lakeflow Declarative Pipelines (formerly Delta Live Tables), Unity Catalog, Databricks Asset Bundles, and executable Open Data Contract Standard (ODCS) v3.2.0 contracts.

The POC answers two questions:

1. How can Bronze, Silver, Gold, and Platinum have explicit responsibilities and dependency rules?
2. How does a Gold record converge when ERP, CRM, and advertising sources arrive at different times?

## Architecture

```text
ERP arrival ─> ERP pipeline: Bronze ─> Silver ──┐
CRM arrival ─> CRM pipeline: Bronze ─> Silver ──┼─> Gold pipeline ─> Platinum pipeline
Ads arrival ─> Ads pipeline: Bronze ─> Silver ──┘
                     source jobs trigger ────────────────^
```

The bundle deploys five Lakeflow pipelines. ERP, CRM, and Ads each own an independent Bronze-to-Silver
pipeline. Gold and Platinum each have their own pipeline. The three source ingestion jobs invoke the same
queued `curated_refresh` job after their source pipeline succeeds, so source schedules can differ without
duplicating curated logic.

| Layer | Responsibility | Enforcement |
|---|---|---|
| Bronze | Append-only source copies; every payload field is a string | ODCS expectations observe violations without discarding raw data |
| Silver | Typed, deduplicated, source-oriented records | Invalid records go to a quarantine table owned by that source |
| Gold | Facts and dimensions with stable grain | Strict ODCS-derived expectations; every table reads Silver directly |
| Platinum | Consumer-facing datamarts | Strict expectations; every object reads Gold directly |

Unity Catalog schemas are named `<schema_prefix>_bronze`, `_silver`, and `_gold`. Platinum is organized as
two data marts: `<schema_prefix>_platinum_sales` and `<schema_prefix>_platinum_marketing`. Landing files use
a managed volume in `<schema_prefix>_landing`.

## Platinum data marts

A data mart is a collection of related objects for a business domain. Each Platinum mart has its own schema
and ODCS contract defining the complete collection, including its physical tables/views and grains.

| Mart schema (default prefix) | Materialized views containing facts/reports | Virtual dimension views |
|---|---|---|
| `medallion_poc_platinum_sales` | `fact_sales`, `sales_detail` | `dim_customer`, `dim_product` |
| `medallion_poc_platinum_marketing` | `fact_campaign_performance` | `dim_campaign` |

Sales consumers can join `fact_sales` to the dimension views by `customer_key` and `product_key` to form a star
model. The existing flattened report is retained as `sales_detail` for consumers that prefer descriptive
attributes in one object. Marketing consumers join their performance fact to `dim_campaign` by `campaign_code`.

Facts and the reporting table are persisted as Lakeflow materialized views. Dimensions are ordinary Unity
Catalog SQL views over Gold, so they always reflect the current Gold dimensions without duplicating storage.
Every object reads Gold directly: `sales_detail` joins Gold facts and dimensions rather than other Platinum
objects. One Platinum pipeline publishes both mart schemas; separate schemas do not require separate pipelines.

For example, query the Sales mart without accessing Gold:

```sql
SELECT product.product_family, SUM(fact.line_amount) AS revenue
FROM main.medallion_poc_platinum_sales.fact_sales AS fact
JOIN main.medallion_poc_platinum_sales.dim_product AS product USING (product_key)
GROUP BY product.product_family;
```

## How Gold knows when to update

Lakeflow lineage tells the Gold pipeline which Silver inputs changed and which Gold materialized views need
recalculation. Lineage does not start a separate pipeline. A trigger still has to start the Gold pipeline.

This POC makes that trigger explicit with Databricks Workflows:

1. A source-specific job runs its Bronze-to-Silver pipeline.
2. On success, it calls the shared `curated_refresh` job.
3. `curated_refresh` runs Gold, creates the mart schemas and dimension views, then refreshes Platinum facts/reports.
4. If source jobs finish close together, job queueing serializes the curated refresh requests.

The source jobs are intentionally unscheduled. They can be started on demand for the POC. In production,
each can have its own schedule or file-arrival/table-update trigger. A stricter model can place a readiness
gate before Gold when the business requires all sources for a reporting period; this POC demonstrates
progressive enrichment instead.

## Asynchronous arrival demonstration

The bundle workflow initializes the three source interfaces, then runs three source-specific available-now
updates and refreshes Gold and Platinum after each arrival:

1. ERP publishes order `O100`. `fact_attributed_sales.completeness_status` is `ERP_ONLY`.
2. CRM publishes the matching contact. The existing order is reevaluated and becomes `CRM_ENRICHED`.
3. Advertising publishes campaign metadata and spend. The same order becomes `COMPLETE`; the Platinum marketing datamart reports CHF 59.80 attributed revenue, CHF 20.00 spend, and ROAS 2.99.

The empty CRM and Ads interfaces are initialized before the first Gold update. This lets Gold publish useful
ERP-only records before those sources have produced data, while preserving stable contracted Silver table
interfaces.

## Executable ODCS contracts

Contracts live under [`contracts`](contracts) and use the official ODCS v3.2.0 schema. They define interface fields, logical types, required fields, enumerations, primary keys, grain, transformation lineage, ownership, and freshness objectives.

The Platinum contracts [`sales_mart.odcs.yaml`](contracts/platinum/sales_mart.odcs.yaml) and
[`marketing_mart.odcs.yaml`](contracts/platinum/marketing_mart.odcs.yaml) describe each complete data mart.
Their versions are `2.0.0` because moving and renaming the consumer interfaces is a breaking change. Required
fields on materialized outputs use compiled Platinum expectations; virtual dimensions inherit the enforced
Gold constraints and the demo checks their grain and joins.

The ODCS schema is fetched from the immutable `v3.2.0` Git tag and verified against a pinned SHA-256 checksum:

```bash
python scripts/fetch_odcs_schema.py
```

`scripts/compile_contracts.py` validates every contract and compiles required-field and enum constraints into Lakeflow expectation dictionaries:

```bash
python scripts/compile_contracts.py
python scripts/compile_contracts.py --check
```

Contract compatibility tests allow nullable additions and flag removals, type changes, newly required fields, and removed objects as breaking changes.

The advertising demo also sends an undeclared `placement_type` field. Auto Loader retains it in Bronze `_rescued_data`, while the source contract and Silver interface remain stable.

## Silver derived-column example

ERP sends a hierarchy such as:

```text
PHOTOBOOK|PREMIUM|A4|LANDSCAPE
```

Silver retains it as `product_hierarchy_raw` and derives `product_family`, `product_tier`, `product_format`, and `product_orientation`. The ODCS contract records each field's source and transformation. Nulls, empty segments, and any segment count other than four are sent to `silver.erp_contract_quarantine` with a stable violation code. The demo intentionally includes `P_BAD` to prove this behavior.

## Local validation

Python 3.11 or newer and `uv` are recommended:

```bash
UV_CACHE_DIR=/tmp/medallion-uv-cache uv sync --extra dev
.venv/bin/python scripts/fetch_odcs_schema.py
.venv/bin/python scripts/compile_contracts.py --check
.venv/bin/pytest
.venv/bin/ruff check .
```

These checks validate ODCS files, generated Lakeflow rules, compatibility behavior, layer dependencies, and delimiter edge cases. Spark execution requires Databricks because Lakeflow decorators and Auto Loader are runtime features.

## Deploy and run

Authenticate the Databricks CLI to a Unity Catalog-enabled workspace, then choose a catalog where you can create schemas and a managed volume:

```bash
databricks bundle validate -t dev --var="catalog=main,schema_prefix=medallion_poc"
databricks bundle deploy -t dev --var="catalog=main,schema_prefix=medallion_poc"
databricks bundle run medallion_poc_demo -t dev --var="catalog=main,schema_prefix=medallion_poc"
```

The workflow creates the schemas and landing volume, stages each source independently, runs the relevant
source pipeline, refreshes Gold and Platinum, publishes virtual Platinum dimension views, and fails
immediately if any expected transition is missing.

The initial source tasks use a full refresh so the demo is repeatable. Run this workflow only against a
dedicated POC schema prefix: resetting the landing volume is part of the demonstration.

After the demo has initialized every contracted source interface, place new files in the appropriate landing
directory and run only that source job:

```bash
databricks bundle run ingest_erp -t dev --var="catalog=main,schema_prefix=medallion_poc"
databricks bundle run ingest_crm -t dev --var="catalog=main,schema_prefix=medallion_poc"
databricks bundle run ingest_ads -t dev --var="catalog=main,schema_prefix=medallion_poc"
```

Each command finishes its own Bronze/Silver load and then runs Gold followed by Platinum. To use the
individual jobs without running the staged demo first, run `initialize_source_interfaces` once after deploy.
You can also run `curated_refresh` directly when Silver data was changed by another process.

Earlier versions of this POC used one pipeline with the target schema `medallion_poc_pipeline`. That schema
was empty because every table name was fully qualified into its actual layer schema. The new bundle no longer
uses that target. An already-created empty schema may remain after redeployment; the data tables are under
`medallion_poc_bronze`, `_silver`, `_gold`, `_platinum_sales`, and `_platinum_marketing`.

### Upgrading from the single Platinum schema

Deploy the updated bundle with your existing catalog and prefix, then run `curated_refresh` to create the
two marts using your current Gold data:

```bash
databricks bundle deploy -t dev --profile medallion-poc \
  --var="catalog=main,schema_prefix=medallion_poc"
databricks bundle run curated_refresh -t dev --profile medallion-poc \
  --var="catalog=main,schema_prefix=medallion_poc"
```

This update keeps the existing pipeline resource identities. It requires no deletion of source pipelines and
no full refresh of Bronze or Silver. Update consumer queries from `_platinum.sales_datamart` to
`_platinum_sales.sales_detail`, and from `_platinum.marketing_performance` to
`_platinum_marketing.fact_campaign_performance`. The old `_platinum` schema is retained as the pipeline's
default namespace for existing deployments; all new consumer objects have fully qualified mart names.
Legacy objects in that schema may remain and are no longer updated by the new definitions.

### Upgrading from the earlier single-pipeline POC

Unity Catalog tables managed by Lakeflow retain the ID of the pipeline that created them. Before the five
new pipelines can use the same schema prefix, delete the legacy pipeline and its POC datasets once. Copy the
legacy pipeline ID from the `TABLE_ALREADY_MANAGED_BY_OTHER_PIPELINE` error, then run:

```bash
databricks pipelines delete <legacy-pipeline-id> --cascade --profile medallion-poc
databricks bundle deploy -t dev --profile medallion-poc \
  --var="catalog=main,schema_prefix=medallion_poc"
databricks bundle run medallion_poc_demo -t dev --profile medallion-poc \
  --var="catalog=main,schema_prefix=medallion_poc"
```

The cascading delete removes every table owned by the legacy monolithic pipeline. The staged demo recreates
the schemas, source data, and tables under their new pipeline owners. To retain the legacy POC tables, deploy
with a new dedicated prefix such as `schema_prefix=medallion_poc_v2` instead.

## Key outputs

| Object | Grain | Purpose |
|---|---|---|
| `silver.erp_products` | Product | Typed product record with the split hierarchy |
| `silver.erp_contract_quarantine` | Rejected ERP source record | Payload, source metadata, and violation codes |
| `gold.fact_sales` | Order line | Stable sales fact |
| `gold.fact_attributed_sales` | Order line | Late-arriving CRM and campaign enrichment status |
| `gold.data_readiness` | Source object | Source timestamps, ingestion timestamps, and availability |
| `platinum_sales.fact_sales` | Order line | Materialized sales fact with keys for the Sales mart dimensions |
| `platinum_sales.sales_detail` | Order line | Flattened sales report within the Sales mart |
| `platinum_sales.dim_customer`, `dim_product` | Dimension business key | Virtual Sales mart views over Gold dimensions |
| `platinum_marketing.fact_campaign_performance` | Date and campaign | Spend, attributed revenue, incomplete revenue, and ROAS |
| `platinum_marketing.dim_campaign` | Campaign | Virtual Marketing mart view over the Gold campaign dimension |

Pipeline expectation metrics are available in the Lakeflow event log. Unity Catalog lineage shows Bronze-to-Silver, Silver-to-Gold, and Gold-to-Platinum dependencies.
