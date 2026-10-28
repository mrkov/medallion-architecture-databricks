"""Run the real contract SQL through three source arrivals using local Spark."""

from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace

from medallion_poc.execution import (
    accepted_silver,
    checked_query,
    evaluate_silver,
    keys,
    physical_name,
    rules,
    settings,
)
from medallion_poc.workspace import publish_views


def local_name(reference):
    return reference.replace(".", "_")


def assert_interface(frame, obj):
    assert frame.columns == [p["name"] for p in obj["properties"]]
    rows = frame.collect()
    assert len(
        {tuple(bytes(row[key]) if isinstance(row[key], bytearray) else row[key] for key in keys(obj)) for row in rows}
    ) == len(rows)
    condition = " AND ".join(f"coalesce(({expression}), false)" for expression in rules(obj).values())
    assert frame.filter(f"NOT ({condition})").count() == 0
    expected_types = {
        "string": "string",
        "boolean": "boolean",
        "date": "date",
        "timestamp": "timestamp",
        "int": "int",
        "bigint": "bigint",
    }
    for field, prop in zip(frame.schema, obj["properties"], strict=True):
        declared = prop["physicalType"].lower()
        assert field.dataType.simpleString() == expected_types.get(declared, declared)


def test_late_arrivals_converge_and_every_mart_is_a_gold_view(spark, contracts):
    loaded = set()
    for stage, expected in [("erp", "ERP_ONLY"), ("crm", "CRM_ENRICHED"), ("ads", "COMPLETE")]:
        loaded.add(stage)
        for source in contracts.sources.values():
            obj = contracts.objects["source." + source.object_name]
            examples = settings(obj)["demo_rows"] if source.source_system in loaded else []
            metadata = (datetime(2026, 10, 1), "fixture.csv", source.source_system, "fixture")
            data = [tuple(row.get(c) for c in source.columns) + metadata for row in examples]
            schema = (
                source.spark_schema
                + ", _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING, _batch_id STRING"
            )
            spark.createDataFrame(data, schema).createOrReplaceTempView("bronze_" + source.object_name)
        for reference, silver in contracts.objects.items():
            if not reference.startswith("silver."):
                continue
            result = accepted_silver(evaluate_silver(spark, silver, local_name), silver)
            # Materialize the local substitute for a persisted Silver output.
            result = spark.createDataFrame(result.collect(), result.schema)
            result.createOrReplaceTempView(local_name(reference))
        for reference, obj in contracts.objects.items():
            if not reference.startswith("gold."):
                continue
            result = spark.sql(checked_query(obj, local_name))
            result = spark.createDataFrame(result.collect(), result.schema)
            result.createOrReplaceTempView(local_name(reference))
            if stage == "ads":
                assert_interface(result, obj)
        row = spark.table("gold_fact_attributed_sales").first()
        assert row.order_line_id == "OL100"
        assert row.completeness_status == expected
        for reference, obj in contracts.objects.items():
            if not reference.startswith("platinum."):
                continue
            assert obj["physicalType"] in {"view", "materialized view"}
            spark.sql(f"CREATE OR REPLACE TEMP VIEW {local_name(reference)} AS {checked_query(obj, local_name)}")
            if stage == "ads":
                assert_interface(spark.table(local_name(reference)), obj)
        sales = spark.table("platinum_sales_detail").first()
        assert (sales.product_family, sales.product_tier, sales.line_amount) == (
            "PHOTOBOOK",
            "PREMIUM",
            Decimal("59.80"),
        )
    performance = spark.table("platinum_fact_campaign_performance").first()
    assert performance.attributed_revenue == Decimal("59.80")
    assert performance.incomplete_revenue == Decimal("0")
    assert performance.spend_amount == Decimal("20.00")
    assert performance.return_on_ad_spend == Decimal("2.9900")
    assert spark.table("gold_dim_product").filter("product_id IS NOT NULL").count() == 1
    assert spark.table("bronze_erp_products").count() == 2


def test_publisher_creates_every_mart_as_an_ordinary_view(contracts):
    statements = []
    publish_views(SimpleNamespace(sql=statements.append), contracts, "main")
    expected = [
        obj for ref, obj in contracts.objects.items() if ref.startswith("platinum.") and obj["physicalType"] == "view"
    ]
    assert len(statements) == len(expected) == 7
    for query, obj in zip(statements, expected, strict=True):
        assert query.startswith("CREATE OR REPLACE VIEW ")
        assert physical_name(obj, "main").replace(".", "`.`") in query
        assert "`main`.`gold_common`." in query
        assert "SELECT *" not in query
