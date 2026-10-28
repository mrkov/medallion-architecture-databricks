"""Exercise contract-defined SCD2 history, temporal fact keys and the deployed demo scenario."""

from copy import deepcopy
from datetime import datetime, timedelta

import pytest
import yaml

from medallion_poc.contracts import load_contract_catalog, load_yaml
from medallion_poc.demo import scenario_query
from medallion_poc.execution import accepted_silver, checked_query, evaluate_silver, settings, validate_catalog


def local_name(reference):
    return reference.replace(".", "_")


def bronze(spark, contracts, name, rows):
    source = contracts.sources[name]
    schema = (
        source.spark_schema + ", _ingested_at TIMESTAMP, _source_file STRING, _source_system STRING, _batch_id STRING"
    )
    data = [
        tuple(row.get(c) for c in source.columns)
        + (datetime(2026, 11, 1) + timedelta(seconds=i), f"{i:04}.csv", source.source_system, str(i))
        for i, row in enumerate(rows)
    ]
    spark.createDataFrame(data, schema).createOrReplaceTempView("bronze_" + name)


def materialize(spark, frame, name):
    result = spark.createDataFrame(frame.collect(), frame.schema)
    result.createOrReplaceTempView(name)
    return result


def history(spark, contracts, rows):
    bronze(spark, contracts, "erp_customers", rows)
    definition = contracts.objects["silver.erp_customers_history"]
    return accepted_silver(evaluate_silver(spark, definition, local_name), definition)


def test_scd2_demo_resolves_versions_in_platinum_without_rekeying_facts(spark, contracts, repository_root):
    scenario = load_yaml(repository_root / "demo/scd2.yaml")
    records = {name: [] for name in contracts.sources}
    initial_key = after_key = None
    previous_middle_key = None
    for stage, step in scenario["stages"].items():
        for name in records:
            records[name].extend(
                settings(contracts.objects["source." + name]).get("demo_batches", {}).get(step["example_batch"], [])
            )
            bronze(spark, contracts, name, records[name])
        for reference, obj in contracts.objects.items():
            if reference.startswith("silver."):
                materialize(spark, accepted_silver(evaluate_silver(spark, obj, local_name), obj), local_name(reference))
        for reference in ["gold.dim_customer_history", "gold.fact_sales", "gold.fact_attributed_sales"]:
            materialize(
                spark, spark.sql(checked_query(contracts.objects[reference], local_name)), local_name(reference)
            )
        for reference in ["platinum.dim_customer_history", "platinum.sales_history"]:
            materialize(
                spark, spark.sql(checked_query(contracts.objects[reference], local_name)), local_name(reference)
            )
        # Run the very same assertions that the Databricks demo observes.
        for check in step["assertions"]:
            query = scenario_query(check["query"], contracts, "main")
            for reference, obj in contracts.objects.items():
                physical = obj["physicalName"].replace("<catalog>", "main")
                query = query.replace(
                    ".".join(f"`{part}`" for part in physical.split(".")),
                    local_name(reference.replace("source.", "bronze.")),
                )
            assert spark.sql(query).first()[0] == check["equals"], (stage, check["name"])
        versions = (
            spark.table("gold_dim_customer_history").filter("customer_id = 'C_SCD2'").orderBy("valid_from").collect()
        )
        assert len({bytes(v.customer_version_key) for v in versions}) == len(versions)
        assert sum(v.is_current for v in versions) == 1
        assert versions[-1].valid_to is None
        for before, after in zip(versions, versions[1:], strict=False):
            assert before.valid_to == after.valid_from
            assert before.valid_from < before.valid_to
        sales = {r.order_line_id: r for r in spark.table("gold_fact_sales").collect()}
        attributed = {r.order_line_id: r for r in spark.table("gold_fact_attributed_sales").collect()}
        assert len(sales) == len(attributed) == 4
        assert "customer_version_key" not in sales["OL_SCD2_EARLY"].asDict()
        report = {r.order_line_id: r for r in spark.table("platinum_sales_history").collect()}
        assert report["OL_SCD2_EARLY"].customer_version_key is None
        for name in sales:
            assert sales[name].customer_key == attributed[name].customer_key
        key = sales["OL_SCD2_BEFORE"].customer_key
        initial_key = initial_key or key
        assert key == initial_key
        if stage == "move":
            after_key = report["OL_SCD2_AFTER"].customer_version_key
            previous_middle_key = report["OL_SCD2_MIDDLE"].customer_version_key
            assert report["OL_SCD2_AFTER"].customer_key == initial_key
        if stage in {"repeat", "late"}:
            assert report["OL_SCD2_AFTER"].customer_version_key == after_key
        if stage == "late":
            assert all(row.customer_key == initial_key for row in sales.values())
            assert report["OL_SCD2_MIDDLE"].customer_version_key != previous_middle_key
            assert [v.country_code for v in versions] == ["CH", "AT", "DE"]
    # Same retained Bronze records give the same keys on another materialization.
    rerun = (
        spark.sql(checked_query(contracts.objects["gold.dim_customer_history"], local_name))
        .filter("customer_id = 'C_SCD2'")
        .collect()
    )
    assert {bytes(v.customer_version_key) for v in rerun} == {bytes(v.customer_version_key) for v in versions}


def test_same_time_corrections_invalid_rows_and_return_to_previous_state(spark, contracts):
    base = deepcopy(settings(contracts.objects["source.erp_customers"])["demo_rows"][0])
    rows = [
        base,
        {**base, "country_code": "FR", "updated_at": "2026-10-03T00:00:00Z"},
        {**base, "country_code": "DE", "updated_at": "2026-10-03T00:00:00Z"},
        {**base, "country_code": None, "updated_at": "2026-10-03T00:00:00Z"},
        {**base, "country_code": "DE", "updated_at": "bad-timestamp"},
        {**base, "country_code": "CH", "updated_at": "2026-10-05T00:00:00Z"},
        # Arrives last, but is effective before the German version.
        {**base, "country_code": "AT", "updated_at": "2026-10-02T00:00:00Z"},
    ]
    result = history(spark, contracts, rows).orderBy("updated_at").collect()
    assert [r.country_code for r in result] == ["CH", "AT", "DE", "CH"]
    assert result[2]._source_file == "0002.csv"
    invalid = evaluate_silver(spark, contracts.objects["silver.erp_customers_history"], local_name)
    assert invalid.filter("size(_contract_violations) > 0").count() == 2


def test_history_configuration_is_loaded_from_yaml(spark, repository_root, tmp_path):
    import shutil

    shutil.copytree(repository_root / "contracts", tmp_path / "contracts")
    path = tmp_path / "contracts/silver/erp.odcs.yaml"
    contract = load_yaml(path)
    obj = next(o for o in contract["schema"] if o["name"] == "erp_customers_history")
    settings(obj)["history"]["tracked_columns"] = ["customer_number"]
    path.write_text(yaml.safe_dump(contract, sort_keys=False))
    changed = load_contract_catalog(tmp_path / "contracts")
    base = deepcopy(settings(changed.objects["source.erp_customers"])["demo_rows"][0])
    result = history(spark, changed, [base, {**base, "country_code": "DE", "updated_at": "2026-10-03"}]).collect()
    assert len(result) == 1  # Country changes no longer open a version after this contract-only edit.


@pytest.mark.parametrize("invalid", ["missing_column", "wrong_grain", "nullable_sequence", "wrong_layer"])
def test_invalid_history_contract_is_rejected(contracts, invalid):
    obj = contracts.objects["silver.erp_customers_history"]
    if invalid == "missing_column":
        settings(obj)["history"]["tracked_columns"] = ["typo"]
    elif invalid == "wrong_grain":
        next(p for p in obj["properties"] if p["name"] == "updated_at").pop("primaryKey")
    elif invalid == "nullable_sequence":
        next(p for p in obj["properties"] if p["name"] == "updated_at")["required"] = False
    else:
        settings(contracts.objects["gold.dim_customer_history"])["history"] = settings(obj)["history"]
    with pytest.raises(ValueError, match="history"):
        validate_catalog(contracts)
