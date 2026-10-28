"""Execute the documented SQL subset of ODCS; no product-specific transformations live here."""

from __future__ import annotations

import re
from collections.abc import Callable

import sqlglot
from sqlglot import exp

from medallion_poc.keys import expand_keys

TOKEN = re.compile(r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")
IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
TYPE = re.compile(r"(?:STRING|BINARY|BOOLEAN|INT|BIGINT|DATE|TIMESTAMP|DECIMAL\(\d+,\d+\))\Z", re.I)
ORDER = re.compile(r"([A-Za-z_][A-Za-z0-9_]*) (ASC|DESC)(?: NULLS (FIRST|LAST))?\Z")


def settings(obj: dict) -> dict:
    values = [item["value"] for item in obj.get("customProperties", []) if item["property"] == "poc"]
    if len(values) != 1 or not isinstance(values[0], dict):
        raise ValueError(f"{obj['name']} needs one poc execution mapping")
    return values[0]


def keys(obj: dict) -> list[str]:
    return [
        p["name"]
        for p in sorted(obj["properties"], key=lambda p: p.get("primaryKeyPosition", 999))
        if p.get("primaryKey")
    ]


def quote(name: str) -> str:
    if any(not IDENTIFIER.fullmatch(part) for part in name.split(".")):
        raise ValueError(f"Invalid identifier: {name!r}")
    return ".".join(f"`{part}`" for part in name.split("."))


def physical_name(obj: dict, catalog: str) -> str:
    if not IDENTIFIER.fullmatch(catalog):
        raise ValueError(f"Invalid catalog identifier: {catalog!r}")
    name = obj["physicalName"].replace("<catalog>", catalog)
    quote(name)
    if len(name.split(".")) != 3:
        raise ValueError(f"Expected catalog.schema.table: {name}")
    return name


def resolve(catalog, reference: str) -> dict:
    return catalog.objects[reference.replace("bronze.", "source.", 1)]


def projection(obj: dict, tolerant: bool = False) -> str:
    cast = "TRY_CAST" if tolerant else "CAST"
    return ",\n".join(
        f"{cast}(({expand_keys(p['transformLogic'], obj.get('key_definitions', {}))}) "
        f"AS {p['physicalType']}) AS {quote(p['name'])}"
        for p in obj["properties"]
    )


def render_query(obj: dict, resolve_name: Callable[[str], str], tolerant: bool = False, raw: bool = False) -> str:
    config = settings(obj)
    payload = ", to_json(struct(src.*), map('ignoreNullFields', 'false')) AS _raw_payload" if raw else ""
    sql = f"SELECT {projection(obj, tolerant)}{payload} FROM {config['from_sql']}"
    sql = TOKEN.sub(lambda m: resolve_name(config["inputs"][m[1]]), sql)
    if member := config.get("unknown_member"):
        expressions = [
            f"CAST(({expand_keys(member[p['name']], obj.get('key_definitions', {}))}) AS {p['physicalType']})"
            for p in obj["properties"]
        ]
        sql += " UNION ALL SELECT " + ", ".join(expressions)
    return sql


def checked_query(obj, resolve_name):
    """Fail within the same input-derived query; no quality table reads a peer output."""
    query = render_query(obj, resolve_name)
    checks = []
    groups = [keys(obj), *[[p["name"]] for p in obj["properties"] if p.get("unique")]]
    for i, group in enumerate(groups):
        columns = ", ".join(quote(c) for c in group)
        checks.append(f"count(*) OVER (PARTITION BY {columns}) AS _poc_unique_{i}")
    conditions = [f"_poc_unique_{i} = 1" for i in range(len(groups))]
    for i, check in enumerate(settings(obj).get("key_checks", [])):
        identity = ", ".join(f"'{c}', {quote(c)}" for c in check["identity"])
        identity = f"to_json(named_struct({identity}), map('ignoreNullFields', 'false'))"
        window = f"OVER (PARTITION BY {quote(check['key'])})"
        checks.append(f"(min({identity}) {window} = max({identity}) {window}) AS _poc_collision_{i}")
        conditions.append(f"_poc_collision_{i}")
    conditions.extend(f"coalesce(({value}), false)" for value in rules(obj).values())
    columns = ", ".join(quote(p["name"]) for p in obj["properties"])
    condition = " AND ".join(conditions)
    return (
        f"SELECT {columns} FROM (SELECT _data.*, {', '.join(checks)} FROM ({query}) AS _data) AS _checked "
        f"WHERE CASE WHEN {condition} THEN true ELSE CAST(raise_error('Contract quality failure: "
        f"{obj['name']} grain, key collision or row constraint') AS BOOLEAN) END"
    )


def schema_ddl(obj, primary_keys=True):
    columns = [
        f"{quote(p['name'])} {p['physicalType']}" + (" NOT NULL" if p.get("required") else "")
        for p in obj["properties"]
    ]
    if primary_keys:
        columns.append(f"CONSTRAINT {quote(obj['name'] + '_pk')} PRIMARY KEY ({', '.join(map(quote, keys(obj)))})")
    return ", ".join(columns)


def rules(obj: dict) -> dict[str, str]:
    from medallion_poc.contracts import compile_object

    compiled = compile_object({"id": obj["contract_id"], "schema": [obj]}, obj["name"])
    return {**compiled.expectations, **settings(obj).get("row_checks", {})}


def validate_catalog(catalog) -> None:
    """Fail early on undeclared lineage, wrong-layer reads and incomplete executable interfaces."""
    physical_names = set()
    for reference, obj in catalog.objects.items():
        layer = reference.split(".")[0]
        name = physical_name(obj, "main")
        if name.casefold() in physical_names:
            raise ValueError(f"Duplicate physical output: {name}")
        physical_names.add(name.casefold())
        columns = [p["name"] for p in obj["properties"]]
        if len(set(c.casefold() for c in columns)) != len(columns):
            raise ValueError(f"Duplicate columns in {reference}")
        for prop in obj["properties"]:
            if not IDENTIFIER.fullmatch(prop["name"]):
                raise ValueError(f"Invalid column in {reference}: {prop['name']}")
            if prop["name"].lower().startswith("_poc_"):
                raise ValueError(f"Column uses reserved quality-check prefix: {reference}.{prop['name']}")
            physical_type = prop.get("physicalType", "").upper()
            if not TYPE.fullmatch(physical_type):
                raise ValueError(f"Unsupported or missing physicalType: {reference}.{prop['name']}")
            expected_logical_type = {
                "STRING": "string",
                "BINARY": "string",
                "INT": "integer",
                "BIGINT": "integer",
                "BOOLEAN": "boolean",
                "DATE": "date",
                "TIMESTAMP": "timestamp",
                "DECIMAL": "number",
            }[physical_type.split("(")[0]]
            if prop["logicalType"] != expected_logical_type:
                raise ValueError(f"Logical/physical type mismatch in {reference}.{prop['name']}")
            if prop.get("quality"):
                raise ValueError(f"{reference}: use poc.row_checks for executable quality predicates")
        if obj.get("quality"):
            raise ValueError(f"{reference}: use poc.row_checks for executable quality predicates")
        if layer == "source":
            continue
        if layer in {"silver", "gold"} and obj["physicalType"] != "materialized view":
            raise ValueError(f"{reference} must declare physicalType: materialized view")
        config = settings(obj)
        unknown = set(config) - {
            "inputs",
            "from_sql",
            "order_by",
            "row_checks",
            "history",
            "unknown_member",
            "key_checks",
        }
        if unknown:
            raise ValueError(f"Unsupported execution settings in {reference}: {sorted(unknown)}")
        allowed = {"silver": "bronze", "gold": "silver", "platinum": "gold"}[layer]
        for dependency in config["inputs"].values():
            if dependency.split(".")[0] != allowed:
                raise ValueError(f"{reference} must read only {allowed}: {dependency}")
            resolve(catalog, dependency)
        if not keys(obj):
            raise ValueError(f"{reference} needs a primary key defining its grain")
        if member := config.get("unknown_member"):
            if layer != "gold" or set(member) != set(columns):
                raise ValueError(f"{reference}: unknown_member must define every Gold output column")
        for check in config.get("key_checks", []):
            if (
                set(check) != {"key", "identity"}
                or check["key"] not in columns
                or not check["identity"]
                or not set(check["identity"]) <= set(columns)
            ):
                raise ValueError(f"{reference}: invalid key collision check")
        for prop in obj["properties"]:
            if not prop.get("transformLogic"):
                raise ValueError(f"Missing transformLogic: {reference}.{prop['name']}")
            expression = sqlglot.parse_one(prop["transformLogic"], read="databricks")
            for node in expression.find_all(exp.Anonymous):
                if node.name.lower() == "poc_key":
                    definition = obj["key_definitions"][node.expressions[0].this]
                    key_type = "BINARY" if definition["algorithm"] == "sha256" else "BIGINT"
                    if prop["physicalType"].upper() != key_type:
                        raise ValueError(f"{reference}.{prop['name']}: shared key requires {key_type}")
            if not set(prop.get("transformSourceObjects", [])) <= set(config["inputs"].values()):
                raise ValueError(f"{reference}: property lineage must refer to declared inputs")
        tokens = set(TOKEN.findall(f"SELECT {projection(obj)} FROM {config['from_sql']}"))
        if tokens != set(config["inputs"]):
            raise ValueError(f"{reference}: input aliases and SQL placeholders disagree")
        query = render_query(obj, lambda ref: "input_" + ref.replace(".", "_"))
        statements = sqlglot.parse(query, read="databricks")
        if len(statements) != 1 or not isinstance(statements[0], exp.Query):
            raise ValueError(f"{reference} must be a single SELECT query")
        ctes = {cte.alias for cte in statements[0].find_all(exp.CTE)}
        expected = {"input_" + ref.replace(".", "_") for ref in config["inputs"].values()}
        actual = {t.name for t in statements[0].find_all(exp.Table) if t.name not in ctes}
        if actual != expected or any(t.db or t.catalog for t in statements[0].find_all(exp.Table)):
            raise ValueError(f"{reference}: all table reads must use declared input placeholders")
        if layer == "silver" and not config.get("order_by"):
            raise ValueError(f"{reference} needs deterministic deduplication order_by")
        if layer == "silver":
            if set(config["inputs"]) != {"raw"} or config["from_sql"].strip() != "{{ raw }} AS src":
                raise ValueError(f"{reference}: Silver must read one raw Bronze input as src")
            for value in config["order_by"]:
                match = ORDER.fullmatch(value)
                if not match or match[1] not in columns:
                    raise ValueError(f"{reference}: order_by must name a contracted column and ASC/DESC")
        if "history" in config:
            history = config["history"]
            if (
                layer != "silver"
                or not isinstance(history, dict)
                or set(history) != {"business_keys", "sequence_by", "tracked_columns"}
            ):
                raise ValueError(f"{reference}: history needs Silver business_keys, sequence_by and tracked_columns")
            business_keys, tracked = history["business_keys"], history["tracked_columns"]
            sequence = history["sequence_by"]
            if (
                not isinstance(business_keys, list)
                or not business_keys
                or not isinstance(tracked, list)
                or not tracked
                or not all(isinstance(c, str) and c in columns for c in [*business_keys, *tracked])
                or not isinstance(sequence, str)
                or sequence not in columns
                or len(set(business_keys)) != len(business_keys)
                or len(set(tracked)) != len(tracked)
                or set(business_keys) & set(tracked)
                or sequence in [*business_keys, *tracked]
                or set(keys(obj)) != {*business_keys, sequence}
            ):
                raise ValueError(f"{reference}: history must use distinct declared columns and version primary keys")
            properties = {p["name"]: p for p in obj["properties"]}
            if properties[sequence]["physicalType"].upper() != "TIMESTAMP" or any(
                not properties[c].get("required") for c in [*business_keys, sequence]
            ):
                raise ValueError(f"{reference}: history keys must be required and sequence_by must be a TIMESTAMP")
        if layer == "platinum" and obj["physicalType"] not in {"view", "materialized view"}:
            raise ValueError(f"{reference} must be a view or materialized view")


def evaluate_silver(spark, obj: dict, resolve_name):
    from pyspark.sql import functions as F

    frame = spark.sql(render_query(obj, resolve_name, tolerant=True, raw=True))
    checks = [
        F.when(~F.coalesce(F.expr(expression), F.lit(False)), F.lit(name)) for name, expression in rules(obj).items()
    ]
    return frame.withColumn("_contract_violations", F.filter(F.array(*checks), lambda item: item.isNotNull()))


def accepted_silver(frame, obj: dict):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    order = []
    for value in settings(obj)["order_by"]:
        match = ORDER.fullmatch(value)
        column, direction, nulls = match.groups()
        method = direction.lower() + (f"_nulls_{nulls.lower()}" if nulls else "")
        order.append(getattr(F.col(column), method)())
    # A stable payload hash breaks equal source timestamps/file names without arbitrary winners.
    order.append(F.sha2(F.to_json(F.struct(*[F.col(p["name"]) for p in obj["properties"]])), 256).desc())
    window = Window.partitionBy(*keys(obj)).orderBy(*order)
    result = (
        frame.filter(F.size("_contract_violations") == 0)
        .withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank", "_contract_violations", "_raw_payload")
    )
    history = settings(obj).get("history")
    if history:
        # Deduplicate each effective timestamp first, then retain only attribute changes.
        # Re-evaluating Bronze history allows late events to split existing intervals.
        timeline = Window.partitionBy(*history["business_keys"]).orderBy(history["sequence_by"])
        state = F.struct(*[F.col(c) for c in history["tracked_columns"]])
        result = (
            result.withColumn("_history_previous", F.lag(state).over(timeline))
            .filter(~state.eqNullSafe(F.col("_history_previous")))
            .drop("_history_previous")
        )
    return result
