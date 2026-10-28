# POC ODCS execution profile

The official ODCS Python package loads every YAML at pipeline/notebook initialization. The repository's
interpreter adds the following explicit conventions. Contracts are trusted application code and are
validated against the pinned official v3.2.0 JSON schema in CI.

## Interfaces

`source`, `silver`, `gold` and `platinum` folders define objects. Input references use `bronze.<object>`,
`silver.<object>` and `gold.<object>`; Bronze resolves to the matching `source` definition.
Names must be unique within each layer. `physicalName` is a three-part name containing the optional
`<catalog>` placeholder. Schema names are fixed: `bronze_<source>`, `silver_<source>`, `gold_common`,
`platinum_<department>`. A catalog isolates deployments.

Source fields must be strings; the source contract's `source_system` custom property groups its objects.
Bronze adds `_ingested_at`, `_source_file`, `_source_system`, `_batch_id` and `_rescued_data` as fixed
platform metadata. Silver contracts explicitly include the metadata they publish. Source files land in
that source's Bronze schema's `landing` volume, under a folder named after the source object.

Every output property specifies its `name`, ODCS `logicalType` and Spark `physicalType`. Supported physical
types are STRING, BINARY, BOOLEAN, INT, BIGINT, DATE, TIMESTAMP and DECIMAL(precision,scale). BINARY uses ODCS
`logicalType: string` with its explicit physical type. Silver and later layers
also require `transformLogic`. Runtime projection order follows the property order. Silver uses TRY_CAST;
Gold and Platinum use CAST. Input strings must match Spark's accepted timestamp/date formats.

## Transformations

An object has one `customProperties` entry named `poc`:

```yaml
customProperties:
  - property: poc
    value:
      inputs:
        raw: bronze.erp_products
      from_sql: '{{ raw }} AS src'
      order_by:
        - updated_at DESC NULLS LAST
        - _ingested_at DESC
        - _source_file DESC
      row_checks:
        HIERARCHY_SEGMENT_COUNT: "size(split(product_hierarchy_raw, '[|]', -1)) = 4"
```

`inputs` maps aliases to contracted objects; `from_sql` defines the FROM/JOIN relation used by the generated
SELECT. Each input must appear as a `{{ alias }}` placeholder. Subqueries and CTEs are allowed. All table
reads must resolve through placeholders; fully qualified or undeclared table reads are rejected. Column
`transformSourceObjects`, when present, must refer to declared inputs. SQL functions operate in the
Databricks/Spark dialect. This is not a SQL security sandbox or a general-purpose ODCS engine.

Silver is source-oriented: exactly one Bronze input, named `raw`, exposed as `src`. Its `primaryKey` fields
define deduplication. `order_by` supports a contracted column, ASC/DESC, and optional NULLS FIRST/LAST. A
stable hash of the projected row resolves remaining ties. Filtering invalid rows precedes deduplication;
an invalid newest source record does not replace an older valid Silver record.

One Bronze interface may feed multiple Silver objects in its source pipeline. For example, ERP customers
have both a current-state object and `erp_customers_history`, each reading Bronze directly. An optional
`poc.history` mapping enables change history in Silver:

```yaml
history:
  business_keys: [customer_id]
  sequence_by: updated_at
  tracked_columns: [customer_number, country_code]
```

The declared primary key must contain exactly the business keys and sequence column, all required; the
sequence must be a TIMESTAMP. After normal quality filtering and per-version deduplication, the executor
keeps the first row and subsequent changes to the tracked columns using null-safe comparisons. Later
metadata-only/unchanged repeats are collapsed. Rows retain the sequence timestamp that opens the version.
Gold contract SQL derives half-open validity intervals with LEAD and constructs version keys. Facts retain
only the entity key and event timestamp. Platinum resolves the version with an entity-key/range join. See
[SCD2 semantics and the runnable demo](scd2.md) for late events, corrections and missing history.

`required` and `enum` compile to row expectations; `row_checks` adds named predicates over projected
columns. A false or null predicate rejects a Silver row to `silver_<source>.contract_quarantine`, retaining
its original Bronze payload, keys, violation codes and file metadata. These are generic operational
quarantine interfaces. Gold uses failing expectations. Gold and Platinum queries also validate primary-key
grain and `unique` properties using window counts, plus `poc.key_checks` using business identities per hash.
Violations call Spark `raise_error`; they cannot silently pass a `count(*)` query that prunes unused columns.
These checks operate on the same preceding-layer inputs as the output, so they introduce no same-layer reads.
Ordinary-view checks run at query time; materialized-view checks run during refresh. Nulls form one group
for uniqueness checks. Gold joins deliberately choose one CRM contact per ERP customer.

Gold and Platinum MVs emit informational primary-key schema declarations by default. They are metadata,
not enforced database constraints; runtime grain checks remain independent. The bundle variable
`informational_keys=false` suppresses only that declaration if a runtime does not support it.

ODCS `quality` query documents and SLA declarations are not a scheduler or automatically executed generic
quality suite. This profile uses required/enum/row_checks explicitly; unsupported execution features are
rejected rather than interpreted as transformations. Ownership, descriptions and other descriptive ODCS
metadata remain documentation. Add runtime support and tests before relying on another ODCS feature.

## Shared keys and unknown members

The Gold contract's root `customProperties.key_definitions` is loaded once into the catalog. Each entry has
`namespace`, `algorithm` (`sha256` or `xxhash64`) and positive integer `arity`. A property can use
`poc_key('customer', ord.customer_id)` in `transformLogic`; the generic Python compiler expands it to Spark
SQL in both fact and dimension definitions. Definitions are shared across contracts. No SQL UDF installation
or ID allocation table is needed. Silver's explicit transforms standardize business identifiers first.

Keys hash a single tagged JSON payload containing the namespace and ordered string arguments. This preserves
component boundaries, avoiding delimiter ambiguity and Spark's multi-argument null-skipping behavior.
Any missing argument produces the family's separate UNKNOWN payload. A literal business ID `UNKNOWN`
remains an ordinary entity. `sha256` requires a BINARY property; `xxhash64` requires BIGINT. Changing algorithms,
namespaces or normalization rules changes keys and requires a coordinated rebuild of related outputs.

Each Gold dimension declares `poc.unknown_member`, a mapping from **every output column** to a literal SQL
expression, including `poc_key(..., NULL)`. The executor unions this row into the normal input-derived result.
The history UNKNOWN row has null validity bounds and a special Platinum match for missing customer IDs.
Present but unmatched fact IDs keep their deterministic keys; left joins preserve them until dimensions arrive.

`poc.key_checks` lists `{key: customer_key, identity: [customer_id]}` entries. A hash mapping to different
identity tuples fails the update. History declares checks for both entity and version hashes. These checks
detect collisions among retained output identities, not discarded lifetime IDs in current-state dimensions.

## Publication and deployment

`physicalType: materialized view` is used for persisted Silver and Gold datasets. Platinum permits `view`
for projections/joins and `materialized view` for aggregates. Both use explicit typed projections and read
Gold only. Republishing ordinary views is separate from refreshing a department's materializations.

Contract `domain` and root `customProperties.orchestration` determine Gold ownership, department ownership,
`eventual`/`daily` refresh policies and optional schedules/failure email destinations. Source input lineage
determines Gold watchers; Gold input lineage determines Platinum MV watchers. Ordinary views need only
publication jobs. The configuration also declares the operational receipt schema, columns, business timezone
and daily demo date. See [the orchestration example](guide-alignment.md#daily-completion).

The generated YAML is checked into Git for ordinary `databricks bundle deploy` usage; CI's `--check` prevents
drift. Eventual jobs use native ANY_UPDATED triggers with a 60-second quiet period and a 120-second minimum
interval. These are batching controls. Daily gates use successful source receipts, including zero-row loads.
Job queueing serializes runs of the same refresh job; setup/upgrade must run while automatic triggers are
paused. Lakeflow owns refresh planning. Window-based quality/history queries can require full recomputation.

## Example data

Source object `poc.demo_rows` contains illustrative record dictionaries. The demo writer takes CSV headers
from the source properties, appending any intentional extra example fields (such as `placement_type`).
File names hash the content. Existing files are never overwritten. Demo stage expectations are defined
separately in `demo/scenario.yaml`, and never supply production table definitions.
Named `poc.demo_batches` maps batch names to example rows for `demo/scd2.yaml` and `demo/keys.yaml`. The writer skips source
objects without that named batch, rejects an unknown/empty batch, and applies the same immutable-file
behavior. The enrichment, SCD2 and key demos observe independent triggers. `daily_gate_demo` separately
exercises the explicit completion gate; it adds receipts without writing new sample files.
