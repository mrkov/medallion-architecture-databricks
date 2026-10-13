import ast
import re
import runpy
from types import SimpleNamespace

from medallion_poc.contracts import compile_object, load_yaml


def _mart_contracts(repository_root):
    return [load_yaml(path) for path in sorted(repository_root.glob("contracts/platinum/*.odcs.yaml"))]


def test_materialized_outputs_match_mart_contract_interfaces(repository_root):
    source = ast.parse((repository_root / "src/pipelines/platinum.py").read_text())
    outputs = {
        f"<catalog>.<schema_prefix>_{call.args[0].value}.{call.args[1].value}"
        for call in ast.walk(source)
        if isinstance(call, ast.Call)
        and isinstance(call.func, ast.Name)
        and call.func.id == "table_name"
        and call.args[0].value.startswith("platinum_")
    }
    contracted_outputs = {
        element["physicalName"]
        for contract in _mart_contracts(repository_root)
        for element in contract["schema"]
        if element["physicalType"] == "materialized view"
    }
    assert outputs == contracted_outputs
    assert len(outputs) == 3


def test_virtual_dimensions_and_sales_fact_preserve_gold_interfaces(repository_root):
    gold = load_yaml(repository_root / "contracts/gold/commerce_model.odcs.yaml")
    for contract in _mart_contracts(repository_root):
        for element in contract["schema"]:
            if element["physicalType"] != "view" and element["name"] != "fact_sales":
                continue
            mart_interface = compile_object(contract, element["name"])
            gold_interface = compile_object(gold, element["name"])
            assert mart_interface.columns == gold_interface.columns
            assert mart_interface.primary_key == gold_interface.primary_key
            assert mart_interface.expectations == gold_interface.expectations
            assert [prop["logicalType"] for prop in element["properties"]] == [
                prop["logicalType"]
                for prop in next(obj for obj in gold["schema"] if obj["name"] == element["name"])["properties"]
            ]


def test_view_publication_matches_contracts_and_reads_gold(repository_root):
    queries = []
    parameters = {
        "action": "publish_views",
        "catalog": "main",
        "schema_prefix": "medallion_poc",
        "volume_name": "landing",
    }
    dbutils = SimpleNamespace(widgets=SimpleNamespace(text=lambda *args: None, get=parameters.__getitem__))
    runpy.run_path(
        str(repository_root / "src/notebooks/demo_control.py"),
        init_globals={"dbutils": dbutils, "spark": SimpleNamespace(sql=queries.append)},
    )
    created_schemas = {
        match.group(1)
        for query in queries
        if (match := re.fullmatch(r"CREATE SCHEMA IF NOT EXISTS `main`\.`([^`]+)`", query))
    }
    assert created_schemas == {"medallion_poc_platinum_sales", "medallion_poc_platinum_marketing"}

    views = {
        (match.group(1), match.group(2), match.group(3))
        for query in queries
        if (
            match := re.fullmatch(
                r"CREATE OR REPLACE VIEW `main`\.`([^`]+)`\.`([^`]+)` AS "
                r"SELECT \* FROM `main`\.`medallion_poc_gold`\.`([^`]+)`",
                query,
            )
        )
    }
    expected = {
        (
            element["physicalName"].split(".")[1].replace("<schema_prefix>", "medallion_poc"),
            element["name"],
            element["name"],
        )
        for contract in _mart_contracts(repository_root)
        for element in contract["schema"]
        if element["physicalType"] == "view"
    }
    assert views == expected
    assert len(views) == 3
