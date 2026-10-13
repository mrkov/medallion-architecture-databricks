"""Execute the actual YAML-defined Silver transformations under ANSI Spark SQL."""

import shutil
from datetime import datetime

import pytest
import yaml

from medallion_poc.contracts import load_contract_catalog, load_yaml
from medallion_poc.execution import accepted_silver, evaluate_silver


def product_frame(spark, contracts, rows):
    interface = contracts.sources["erp_products"]
    schema = (
        interface.spark_schema
        + ", _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING, _batch_id STRING"
    )
    data = [(*row, datetime(2026, 10, 1), "test.csv", "erp", "test") for row in rows]
    spark.createDataFrame(data, schema).createOrReplaceTempView("raw_products")
    return evaluate_silver(spark, contracts.objects["silver.erp_products"], lambda ref: "raw_products")


@pytest.mark.parametrize(
    "hierarchy,code",
    [
        (None, "product_hierarchy_raw_not_null"),
        ("PHOTOBOOK|PREMIUM|A4", "HIERARCHY_SEGMENT_COUNT"),
        ("PHOTOBOOK|PREMIUM|A4|LANDSCAPE|EXTRA", "HIERARCHY_SEGMENT_COUNT"),
        ("PHOTOBOOK||A4|LANDSCAPE", "HIERARCHY_EMPTY_SEGMENT"),
    ],
)
def test_bad_hierarchy_is_quarantined_without_ansi_exceptions(spark, contracts, hierarchy, code):
    result = product_frame(spark, contracts, [("P1", "Book", hierarchy, "true", "2026-10-01")]).first()
    assert code in result._contract_violations
    assert "product_hierarchy" in result._raw_payload


def test_cast_failures_are_quarantined_and_dedup_is_deterministic(spark, contracts):
    frame = product_frame(
        spark,
        contracts,
        [
            ("P1", "Old", " PHOTOBOOK | PREMIUM | A4 | LANDSCAPE ", "true", "2026-09-01"),
            ("P1", "New", " PHOTOBOOK | PREMIUM | A4 | LANDSCAPE ", "true", "2026-10-01"),
            ("P2", "Invalid", "PHOTOBOOK|PREMIUM|A4|LANDSCAPE", "perhaps", "not-a-timestamp"),
        ],
    )
    invalid = frame.filter("product_id = 'P2'").first()
    assert {"active_not_null", "updated_at_not_null"} <= set(invalid._contract_violations)
    result = accepted_silver(frame, contracts.objects["silver.erp_products"]).collect()
    assert len(result) == 1
    assert (result[0].product_name, result[0].product_family, result[0].product_tier) == ("New", "PHOTOBOOK", "PREMIUM")


def test_editing_contract_expression_and_output_column_changes_executed_result(
    spark, contracts, repository_root, tmp_path
):
    product_frame(spark, contracts, [("P1", "Book", "PHOTOBOOK|PREMIUM|A4|LANDSCAPE", "true", "2026-10-01")])
    shutil.copytree(repository_root / "contracts", tmp_path / "contracts")
    path = tmp_path / "contracts/silver/erp.odcs.yaml"
    contract = load_yaml(path)
    definition = next(obj for obj in contract["schema"] if obj["name"] == "erp_products")
    definition["properties"].append(
        {
            "name": "product_label",
            "logicalType": "string",
            "physicalType": "STRING",
            "required": True,
            "transformLogic": "concat(src.product_name, ' / ', lower(src.product_id))",
        }
    )
    tier = next(p for p in definition["properties"] if p["name"] == "product_tier")
    tier["transformLogic"] = "lower(trim(try_element_at(split(src.product_hierarchy, '[|]', -1), 2)))"
    path.write_text(yaml.safe_dump(contract, sort_keys=False))
    definition = load_contract_catalog(tmp_path / "contracts").objects["silver.erp_products"]
    result = accepted_silver(evaluate_silver(spark, definition, lambda ref: "raw_products"), definition).first()
    assert result.product_label == "Book / p1"
    assert result.product_tier == "premium"
