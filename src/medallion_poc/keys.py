"""Compile shared ODCS key definitions into deterministic, null-safe Spark SQL."""

import sqlglot
from sqlglot import exp


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def expand_keys(expression, definitions):
    tree = sqlglot.parse_one(expression, read="databricks")

    def expand(node):
        if not isinstance(node, exp.Anonymous) or node.name.lower() != "poc_key":
            return node
        args = node.expressions
        if not args or not isinstance(args[0], exp.Literal) or not args[0].is_string:
            raise ValueError("poc_key needs a literal definition name")
        definition = definitions[args[0].this]
        values = [arg.sql(dialect="databricks") for arg in args[1:]]
        if len(values) != definition["arity"]:
            raise ValueError(f"Wrong number of arguments for key {args[0].this}")
        namespace = literal(definition["namespace"])
        # A tagged JSON array preserves boundaries and null positions. UNKNOWN is a
        # separate kind, so a real business ID literally named UNKNOWN cannot collide.
        payload = (
            f"to_json(named_struct('namespace', {namespace}, 'kind', 'entity', 'values', "
            f"array({', '.join(f'CAST(({v}) AS STRING)' for v in values)})), "
            "map('ignoreNullFields', 'false'))"
        )
        unknown = f"to_json(named_struct('namespace', {namespace}, 'kind', 'unknown'))"
        missing = " OR ".join(f"({v}) IS NULL" for v in values)
        payload = f"CASE WHEN {missing} THEN {unknown} ELSE {payload} END"
        sql = f"unhex(sha2({payload}, 256))" if definition["algorithm"] == "sha256" else f"xxhash64({payload})"
        return sqlglot.parse_one(sql, read="databricks")

    return tree.transform(expand).sql(dialect="databricks")


def validate_definitions(definitions):
    namespaces = set()
    for name, definition in definitions.items():
        if set(definition) != {"namespace", "algorithm", "arity"}:
            raise ValueError(f"Invalid key definition: {name}")
        if definition["algorithm"] not in {"sha256", "xxhash64"} or not isinstance(definition["arity"], int):
            raise ValueError(f"Invalid key algorithm or arity: {name}")
        if definition["arity"] < 1 or not isinstance(definition["namespace"], str) or not definition["namespace"]:
            raise ValueError(f"Invalid key namespace or arity: {name}")
        if definition["namespace"] in namespaces:
            raise ValueError(f"Duplicate key namespace: {definition['namespace']}")
        namespaces.add(definition["namespace"])
