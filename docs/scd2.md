# Customer SCD Type 2 demonstration

This example retains historical ERP customer attributes and associates each sale with the customer version
valid at its **order timestamp**. It preserves the existing current-state reports and adds historical ones.

| Object | Purpose |
|---|---|
| `bronze_erp.erp_customers` | Append-only source records, including unchanged repeats and corrections |
| `silver_erp.erp_customers` | Latest valid customer record |
| `silver_erp.erp_customers_history` | Typed change events, deduplicated by customer ID and effective timestamp; unchanged repeats collapsed |
| `gold_common.dim_customer` | Current ERP customer attributes and current CRM enrichment |
| `gold_common.dim_customer_history` | SCD2 versions of ERP customer number and country, with validity intervals |
| `gold_common.fact_sales`, `fact_attributed_sales` | Existing business-level `customer_key` plus temporal `customer_version_key` |
| `platinum_sales.dim_customer_history` | Ordinary view of the history dimension |
| `platinum_sales.sales_history` | Sales with customer number and country as of the order timestamp |

Gold facts derive their version keys directly from Silver history; they never read the Gold dimension.
Platinum joins the Gold fact and Gold history dimension by `customer_version_key`. Current `sales_detail`
and `dim_customer` retain their existing meaning. CRM and product attributes are not historized in this example.

## Install the updated definitions

For a new deployment, follow the README's bootstrap/setup instructions. For an existing deployment, the
same sequence adds the history objects and republishes the Platinum views without clearing data:

```bash
git pull
uv sync --locked
.venv/bin/python scripts/generate_bundle_resources.py --bootstrap
databricks bundle deploy --profile medallion-poc -t dev \
  --var catalog=main --var gold_trigger_status=PAUSED
databricks bundle run setup_poc --profile medallion-poc -t dev --var catalog=main
.venv/bin/python scripts/generate_bundle_resources.py
databricks bundle deploy --profile medallion-poc -t dev \
  --var catalog=main --var gold_trigger_status=UNPAUSED
```

Let any already-running source/Gold updates finish before running setup. The temporary bootstrap deployment
removes the table watcher while setup creates `silver_erp.erp_customers_history`. Normal generation then adds
that table to the watcher alongside the seven existing Silver inputs. No full refresh or table deletion is used.
The new history materialized view reads the existing retained Bronze records on initialization.

## Run the demonstration

```bash
databricks bundle run scd2_demo --profile medallion-poc -t dev --var catalog=main
```

The job appears as **[dev] Observe customer SCD Type 2 history and late arrivals**. Watch **Gold on Silver
changes** alongside it. The demo seeds contract-defined CSV batches, runs `ingest_erp`, and observes results.
It never invokes Gold itself. A disabled trigger causes observations to time out rather than bypassing it.

It uses dedicated `C_SCD2`, `P_SCD2`, and `O_SCD2_*`/`OL_SCD2_*` identifiers, so it can run after the existing
enrichment demo in the same catalog. Run the enrichment demo first if demonstrating both: that original
scenario requires empty data. The SCD2 demo checks that its customer has not already been ingested. Repair
a failed job from the failed task, or use another catalog for a complete repeat; it does not delete history.

| Stage | Arriving customer record | Expected result |
|---|---|---|
| Initial | CH, effective October 1 at 09:00 UTC | One version; known subsequent sales provisionally use CH |
| Move | DE, effective October 3 at 00:00 UTC | CH closes; DE opens; earlier sales retain CH |
| Repeat | DE again, effective October 4 | Bronze retains it, but no additional history version is created |
| Late | AT, effective October 2, arriving last | AT is inserted between CH and DE; only sales in that interval change version |

All dates are in 2026. The final history is:

| customer_id | country_code | valid_from (UTC) | valid_to (UTC) | is_current |
|---|---|---|---|---|
| C_SCD2 | CH | October 1, 09:00 | October 2, 00:00 | false |
| C_SCD2 | AT | October 2, 00:00 | October 3, 00:00 | false |
| C_SCD2 | DE | October 3, 00:00 | NULL | true |

Query the views (timestamps display in your SQL session's timezone):

```sql
SELECT customer_id, customer_version_key, country_code, valid_from, valid_to, is_current
FROM main.platinum_sales.dim_customer_history
WHERE customer_id = 'C_SCD2'
ORDER BY valid_from;

SELECT order_line_id, order_timestamp, customer_version_key,
       country_code_at_order, customer_history_status
FROM main.platinum_sales.sales_history
WHERE order_line_id LIKE 'OL_SCD2_%'
ORDER BY order_timestamp;
```

Expected sales after the final stage:

| Order line | Order timestamp (UTC) | Country at order | History status |
|---|---|---|---|
| OL_SCD2_EARLY | September 30, 10:00 | NULL | MISSING_HISTORY |
| OL_SCD2_BEFORE | October 1, 10:00 | CH | MATCHED |
| OL_SCD2_MIDDLE | October 2, 10:00 | AT | MATCHED |
| OL_SCD2_AFTER | October 3, 00:00 | DE | MATCHED |

The middle sale initially joins CH, then switches to AT when the late record arrives. Its amount stays the
same. The before/after sale keys stay stable. The sale exactly at the move timestamp joins the new DE
version: validity intervals include `valid_from` and exclude `valid_to`. A sale before the first known
version remains in the fact with a null version key; no historical state is invented.

## Contracts control history

In `contracts/silver/erp.odcs.yaml`, the history object's `poc` extension declares:

```yaml
history:
  business_keys: [customer_id]
  sequence_by: updated_at
  tracked_columns: [customer_number, country_code]
```

Its primary key is `(customer_id, updated_at)`. The generic executor applies the normal contract casts and
quality checks, deduplicates each effective timestamp using the declared `order_by`, then compares each
valid row's tracked attributes with the preceding row. Null-safe comparisons suppress unchanged repeats;
returning to a previous country after another country creates a new version. Metadata and unrelated fields
do not open versions. The separate current-state Silver table still retains the latest valid full record.

Gold contract expressions define the validity columns with `lead(updated_at)`, `is_current`, and the key.
A version key is a SHA256 string derived from customer ID and the effective timestamp in epoch microseconds.
It does not change merely because a file is reingested, a version closes, or an unchanged repeat arrives.
The existing `customer_key` remains a business-level hash. No integer identity registry is introduced.

The fixture batches live in the source ODCS contract under `poc.demo_batches`; `demo/scd2.yaml` contains
observations and expected results. No customer fields or business transformations are hardcoded in notebooks.

## Semantics and limits

- `updated_at` is treated as business-effective time in this POC. Real sources may need a separate effective
  timestamp. Daily snapshots capture only observed states; they cannot reconstruct changes never received.
- For conflicting versions at the same effective timestamp, the latest valid `_ingested_at` wins; the declared
  file ordering and row hash break remaining ties. Invalid corrections are quarantined. A production source
  should supply a reliable event sequence if ingestion order does not define correction precedence.
- This is effective-time history. Late events/corrections may revise historical intervals and fact keys.
  It does not retain a second timeline of what the warehouse believed at each earlier ingestion time.
- Every input is a full after-image. Deletes, partial CDC updates and inferred deletion from missing snapshot
  rows are not implemented. `is_current` marks the last known effective version, not a wall-clock filter;
  future-dated changes would require a separately defined policy.
- History comes from retained Bronze data. The materialized-view queries recompute the correct result from
  those records, including out-of-order arrivals. This example does not use Lakeflow AUTO CDC or claim
  streaming SCD2 maintenance. Window operations may require full recomputation; inspect refresh plans and
  costs before scaling. Daily workloads can batch Gold refreshes to their reporting deadline.
- Cross-table refresh is eventually consistent. A new fact key and its dimension row may become visible
  at different moments during an update; the observation tasks wait for the joined result to converge.
