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
types are STRING, BOOLEAN, INT, BIGINT, DATE, TIMESTAMP and DECIMAL(precision,scale). Silver and later layers
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

`required` and `enum` compile to row expectations; `row_checks` adds named predicates over projected
columns. A false or null predicate rejects a Silver row to `silver_<source>.contract_quarantine`, retaining
its original Bronze payload, keys, violation codes and file metadata. These are generic operational
quarantine interfaces. Gold uses failing expectations. Primary keys are declarative grain, not enforced
Unity Catalog primary-key constraints. Gold joins deliberately choose one CRM contact per ERP customer.

ODCS `quality` query documents and SLA declarations are not a scheduler or automatically executed generic
quality suite. This profile uses required/enum/row_checks explicitly; unsupported execution features are
rejected rather than interpreted as transformations. Ownership, descriptions and other descriptive ODCS
metadata remain documentation. Add runtime support and tests before relying on another ODCS feature.

## Publication and deployment

`physicalType: materialized view` is used for persisted Silver and Gold datasets; Platinum requires
`physicalType: view`. Platinum SELECTs have explicit typed projections and read Gold only. Republishing
view definitions is separate from querying the latest Gold data.

Bundle generation derives each source's target schema and the Gold job's watched Silver tables from
contracts. The generated YAML is checked into Git for ordinary `databricks bundle deploy` usage; CI's
`--check` prevents it from silently drifting from ODCS. Gold uses one native ANY_UPDATED trigger with a
60-second quiet period and a 120-second minimum interval. These are batching controls, not completion
markers. Job queueing serializes runs; Lakeflow owns refresh planning.

## Example data

Source object `poc.demo_rows` contains illustrative record dictionaries. The demo writer takes CSV headers
from the source properties, appending any intentional extra example fields (such as `placement_type`).
File names hash the content. Existing files are never overwritten. Demo stage expectations are defined
separately in `demo/scenario.yaml`, and never supply production table definitions.
