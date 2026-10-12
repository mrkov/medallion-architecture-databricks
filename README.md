# Four-layer Databricks medallion POC

This repository demonstrates an Ifolor-style commerce platform implemented with Lakeflow Declarative Pipelines (formerly Delta Live Tables), Unity Catalog, Databricks Asset Bundles, and executable Open Data Contract Standard (ODCS) v3.2.0 contracts.

The POC answers two questions:

1. How can Bronze, Silver, Gold, and Platinum have explicit responsibilities and dependency rules?
2. How does a Gold record converge when ERP, CRM, and advertising sources arrive at different times?

## Architecture

```text
ERP CSV ──> Bronze ERP ──> Silver ERP ───────┐
CRM CSV ──> Bronze CRM ──> Silver CRM ───────┼─> Gold facts/dimensions ─> Platinum datamarts
Ads CSV ──> Bronze Ads ──> Silver Ads ───────┘
```

| Layer | Responsibility | Enforcement |
|---|---|---|
| Bronze | Append-only source copies; every payload field is a string | ODCS expectations observe violations without discarding raw data |
| Silver | Typed, deduplicated, source-oriented records | Invalid records go to `contract_quarantine` |
| Gold | Facts and dimensions with stable grain | Strict ODCS-derived expectations; every table reads Silver directly |
| Platinum | Consumer-facing datamarts | Strict expectations; every object reads Gold directly |

Unity Catalog schemas are named `<schema_prefix>_bronze`, `_silver`, `_gold`, and `_platinum`. Landing files use a managed volume in `<schema_prefix>_landing`.

## Asynchronous arrival demonstration

The bundle workflow runs three available-now pipeline updates:

1. ERP publishes order `O100`. `fact_attributed_sales.completeness_status` is `ERP_ONLY`.
2. CRM publishes the matching contact. The existing order is reevaluated and becomes `CRM_ENRICHED`.
3. Advertising publishes campaign metadata and spend. The same order becomes `COMPLETE`; the Platinum marketing datamart reports CHF 59.80 attributed revenue, CHF 20.00 spend, and ROAS 2.99.

Gold uses materialized views for this enrichment. A triggered pipeline does not start itself when a file arrives, so the Asset Bundle job explicitly invokes an available-now update after each staged arrival. In production the equivalent trigger would normally be a schedule or an external ingestion event.

## Executable ODCS contracts

Contracts live under [`contracts`](contracts) and use the official ODCS v3.2.0 schema. They define interface fields, logical types, required fields, enumerations, primary keys, grain, transformation lineage, ownership, and freshness objectives.

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

Silver retains it as `product_hierarchy_raw` and derives `product_family`, `product_tier`, `product_format`, and `product_orientation`. The ODCS contract records each field's source and transformation. Nulls, empty segments, and any segment count other than four are sent to `silver.contract_quarantine` with a stable violation code. The demo intentionally includes `P_BAD` to prove this behavior.

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

The workflow creates the schemas and landing volume, stages each source independently, runs the pipeline three times, publishes virtual Platinum dimension views, and fails immediately if any expected transition is missing.

The first pipeline task uses a full refresh so the demo is repeatable. Run this workflow only against a dedicated POC schema prefix: resetting the landing volume is part of the demonstration.

## Key outputs

| Object | Grain | Purpose |
|---|---|---|
| `silver.erp_products` | Product | Typed product record with the split hierarchy |
| `silver.contract_quarantine` | Rejected source record | Payload, source metadata, and violation codes |
| `gold.fact_sales` | Order line | Stable sales fact |
| `gold.fact_attributed_sales` | Order line | Late-arriving CRM and campaign enrichment status |
| `gold.data_readiness` | Source object | Source timestamps, ingestion timestamps, and availability |
| `platinum.sales_datamart` | Order line | Materialized semantic-model input |
| `platinum.marketing_performance` | Date and campaign | Spend, attributed revenue, incomplete revenue, and ROAS |
| `platinum.dim_*_vw` | Dimension business key | Virtual views over Gold dimensions |

Pipeline expectation metrics are available in the Lakeflow event log. Unity Catalog lineage shows Bronze-to-Silver, Silver-to-Gold, and Gold-to-Platinum dependencies.
