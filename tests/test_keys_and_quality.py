"""Actual Spark SQL verifies shared keys, missing values, late members and failing checks."""

from copy import deepcopy
from datetime import datetime

import pytest

from medallion_poc.execution import accepted_silver, checked_query, evaluate_silver, settings
from medallion_poc.keys import expand_keys


def local_name(ref):
    return ref.replace(".", "_")


def materialize(spark, frame, name):
    result = spark.createDataFrame(frame.collect(), frame.schema)
    result.createOrReplaceTempView(name)
    return result


def test_unknown_keys_are_distinct_from_real_ids_and_namespaced(spark, contracts):
    expressions = [
        "poc_key('customer', NULL)",
        "poc_key('customer', 'UNKNOWN')",
        "poc_key('customer', '1001')",
        "poc_key('product', '1001')",
    ]
    row = spark.sql(
        "SELECT "
        + ", ".join(expand_keys(e, contracts.key_definitions) + f" AS k{i}" for i, e in enumerate(expressions))
    ).first()
    assert len({bytes(v) for v in row}) == 4
    assert all(len(v) == 32 for v in row)
    defs = {"composite": dict(namespace="TEST", algorithm="sha256", arity=2)}
    row = spark.sql(
        "SELECT "
        + ", ".join(
            expand_keys(e, defs) + f" AS k{i}"
            for i, e in enumerate(["poc_key('composite', 'A|B', 'C')", "poc_key('composite', 'A', 'B|C')"])
        )
    ).first()
    assert row[0] != row[1]
    defs["composite"]["algorithm"] = "xxhash64"
    assert isinstance(spark.sql("SELECT " + expand_keys("poc_key('composite', 'A', 'B')", defs)).first()[0], int)


def test_missing_values_and_late_dimensions_preserve_facts_and_keys(spark, contracts):
    records = {name: [] for name in contracts.sources}
    before = None
    for batch in ["keys_initial", "keys_late"]:
        for name, source in contracts.sources.items():
            records[name].extend(settings(contracts.objects["source." + name]).get("demo_batches", {}).get(batch, []))
            metadata = (datetime(2026, 10, 18), batch + ".csv", source.source_system, batch)
            rows = [tuple(r.get(c) for c in source.columns) + metadata for r in records[name]]
            spark.createDataFrame(
                rows,
                source.spark_schema
                + ", _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING, _batch_id STRING",
            ).createOrReplaceTempView("bronze_" + name)
        for ref, obj in contracts.objects.items():
            if ref.startswith("silver."):
                materialize(spark, accepted_silver(evaluate_silver(spark, obj, local_name), obj), local_name(ref))
        for ref in [
            "gold.dim_customer",
            "gold.dim_customer_history",
            "gold.dim_product",
            "gold.fact_sales",
            "gold.fact_attributed_sales",
        ]:
            materialize(spark, spark.sql(checked_query(contracts.objects[ref], local_name)), local_name(ref))
        facts = {r.order_line_id: r for r in spark.table("gold_fact_sales").collect()}
        reports = {
            r.order_line_id: r
            for r in spark.sql(checked_query(contracts.objects["platinum.sales_detail"], local_name)).collect()
        }
        history = {
            r.order_line_id: r
            for r in spark.sql(checked_query(contracts.objects["platinum.sales_history"], local_name)).collect()
        }
        assert len(facts) == len(reports) == 2
        assert reports["OL_KEY_NULL"].customer_status == reports["OL_KEY_NULL"].product_status == "UNKNOWN"
        assert history["OL_KEY_NULL"].customer_history_status == "UNKNOWN"
        status = "UNMATCHED" if batch == "keys_initial" else "MATCHED"
        assert reports["OL_KEY_LATE"].customer_status == reports["OL_KEY_LATE"].product_status == status
        assert facts["OL_KEY_LATE"].customer_id == "C_KEY_LATE"
        assert facts["OL_KEY_LATE"].product_id == "P_KEY_LATE"
        assert facts["OL_KEY_NULL"].customer_key != facts["OL_KEY_LATE"].customer_key
        if before:
            assert {k: bytes(v.customer_key) for k, v in facts.items()} == before
        before = {k: bytes(v.customer_key) for k, v in facts.items()}
        # Raw identifiers retain their original whitespace/case.
        assert spark.table("bronze_erp_orders").filter("order_id='O_KEY_LATE'").first().customer_id == " c_key_late "


def test_contract_check_fails_on_duplicate_fact_grain(spark, contracts):
    obj = deepcopy(contracts.objects["gold.fact_ad_spend_daily"])
    # Duplicate input grain is deliberately injected after Silver deduplication.
    spark.sql(
        "SELECT DATE '2026-10-01' spend_date, 'A' campaign_code, 10 spend_amount, 1 clicks, 2 impressions "
        "UNION ALL SELECT DATE '2026-10-01', 'A', 20, 1, 2"
    ).createOrReplaceTempView("quality_input")
    with pytest.raises(Exception, match="Contract quality failure"):
        spark.sql(checked_query(obj, lambda _: "quality_input")).count()


def test_collision_check_fails_even_when_version_grain_is_unique(spark, contracts):
    obj = deepcopy(contracts.objects["gold.dim_customer_history"])
    # Simulate an entity-hash collision; version keys remain distinct.
    next(p for p in obj["properties"] if p["name"] == "customer_key")["transformLogic"] = "unhex('00')"
    settings(obj).pop("unknown_member")
    spark.sql(
        "SELECT 'C1' customer_id, 'N1' customer_number, 'CH' country_code, TIMESTAMP '2026-10-01' updated_at "
        "UNION ALL SELECT 'C2', 'N2', 'DE', TIMESTAMP '2026-10-01'"
    ).createOrReplaceTempView("collision_input")
    with pytest.raises(Exception, match="Contract quality failure"):
        spark.sql(checked_query(obj, lambda _: "collision_input")).collect()
