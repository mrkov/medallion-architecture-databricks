"""Exercise configured source imports and dataset registration without a Spark runtime."""

import glob
import runpy
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from medallion_poc.contracts import load_yaml


@pytest.fixture
def registered_datasets(monkeypatch):
    registered = {}

    def dataset_decorator(kind, **settings):
        def register(function):
            name = settings["name"]
            assert name not in registered, f"Dataset registered more than once: {name}"
            registered[name] = {"kind": kind, "expectations": getattr(function, "expectations", {})}
            return function

        return register

    def expectation_decorator(policy, rules):
        def decorate(function):
            function.expectations = {"policy": policy, "rules": rules}
            return function

        return decorate

    dp = ModuleType("pyspark.pipelines")
    dp.table = lambda **settings: dataset_decorator("streaming_table", **settings)
    dp.materialized_view = lambda **settings: dataset_decorator("materialized_view", **settings)
    dp.expect_all = lambda rules: expectation_decorator("observe", rules)
    dp.expect_all_or_fail = lambda rules: expectation_decorator("fail", rules)

    configuration = {"poc.catalog": "main", "poc.schema_prefix": "poc", "poc.volume_name": "landing"}
    spark = SimpleNamespace(conf=SimpleNamespace(get=configuration.__getitem__))
    sql = ModuleType("pyspark.sql")
    sql.SparkSession = SimpleNamespace(getActiveSession=lambda: spark)
    sql.DataFrame = type("DataFrame", (), {})
    sql.Window = type("Window", (), {})
    sql.functions = ModuleType("pyspark.sql.functions")
    pyspark = ModuleType("pyspark")
    pyspark.pipelines = dp
    pyspark.sql = sql

    for name, module in {
        "pyspark": pyspark,
        "pyspark.pipelines": dp,
        "pyspark.sql": sql,
        "pyspark.sql.functions": sql.functions,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)
    # Clear shared-module caches for each pipeline and restore the original entries after the test.
    for name in ("pipeline_common", "silver_common", "contract_rules"):
        monkeypatch.setitem(sys.modules, name, None)
        monkeypatch.delitem(sys.modules, name)
    return registered


def _configured_files(repository_root, pipeline):
    config_directory = repository_root / "resources"
    paths = []
    for library in pipeline["libraries"]:
        if "glob" in library:
            pattern = str(config_directory / library["glob"]["include"])
            paths.extend(Path(path).resolve() for path in glob.glob(pattern, recursive=True) if path.endswith(".py"))
        else:
            paths.append((config_directory / library["file"]["path"]).resolve())
    return sorted(paths)


@pytest.mark.parametrize("owner", ["erp", "crm", "ads", "gold", "platinum"])
def test_configured_pipeline_registers_only_its_contracted_tables(
    repository_root, monkeypatch, registered_datasets, owner
):
    pipelines = load_yaml(repository_root / "resources/medallion_poc.yml")["resources"]["pipelines"]
    key = f"{owner}_source_pipeline" if owner in {"erp", "crm", "ads"} else f"{owner}_pipeline"
    pipeline_root = (repository_root / "src/pipelines").resolve()
    if owner in {"erp", "crm", "ads"}:
        # Nested source files must locate shared modules even when only their own directory is on sys.path.
        monkeypatch.setattr(
            sys,
            "path",
            [str(pipeline_root / owner), *[entry for entry in sys.path if Path(entry).resolve() != pipeline_root]],
        )
    else:
        monkeypatch.syspath_prepend(str(pipeline_root))
    files = _configured_files(repository_root, pipelines[key])
    assert files
    for path in files:
        runpy.run_path(str(path))

    if owner in {"erp", "crm", "ads"}:
        contract = load_yaml(repository_root / f"contracts/source/{owner}.odcs.yaml")
        objects = {element["name"] for element in contract["schema"]}
        expected = {f"main.poc_{layer}.{name}" for layer in ("bronze", "silver") for name in objects} | {
            f"main.poc_silver.{owner}_contract_quarantine"
        }
        assert set(registered_datasets) == expected
        for name in objects:
            bronze = registered_datasets[f"main.poc_bronze.{name}"]
            assert bronze["kind"] == "streaming_table"
            assert bronze["expectations"]["policy"] == "observe"
            assert registered_datasets[f"main.poc_silver.{name}"]["kind"] == "materialized_view"
        if owner == "erp":
            assert registered_datasets["main.poc_silver.erp_products"]["expectations"]["policy"] == "fail"
    else:
        contracts = [load_yaml(path) for path in sorted(repository_root.glob(f"contracts/{owner}/*.odcs.yaml"))]
        if owner == "gold":
            expected = {f"main.poc_gold.{element['name']}" for contract in contracts for element in contract["schema"]}
        else:
            expected = {
                element["physicalName"].replace("<catalog>", "main").replace("<schema_prefix>", "poc")
                for contract in contracts
                for element in contract["schema"]
                if element["physicalType"] == "materialized view"
            }
        assert set(registered_datasets) == expected
        assert all(value["expectations"]["policy"] == "fail" for value in registered_datasets.values())


def test_read_landing_consumes_contract_definitions(repository_root, monkeypatch, registered_datasets):
    class Expression:
        def __init__(self, name):
            self.name = name

        def cast(self, _type):
            return self

        def alias(self, name):
            return Expression(name)

    class Reader:
        def format(self, format_name):
            self.format_name = format_name
            return self

        def option(self, _name, _value):
            return self

        def schema(self, ddl):
            self.ddl = ddl
            return self

        def load(self, path):
            self.path = path
            return self

        def select(self, *expressions):
            self.selected = [expression.name for expression in expressions]
            return self

        def withColumn(self, _name, _expression):
            return self

    # A contract can add a raw field and change its source route without changing pipeline_common.py.
    definitions = ModuleType("contract_rules")
    definitions.SOURCE_COLUMNS = {"source_customers": ("business_id", "new_contract_field")}
    definitions.SOURCE_SYSTEM = {"source_customers": "declared_source"}
    definitions.SOURCE_SCHEMAS = {"source_customers": "business_id STRING, new_contract_field STRING"}
    monkeypatch.setitem(sys.modules, "contract_rules", definitions)
    reader = Reader()
    spark = sys.modules["pyspark.sql"].SparkSession.getActiveSession()
    spark.readStream = reader
    functions = sys.modules["pyspark.sql.functions"]
    functions.col = Expression
    functions.lit = Expression
    functions.current_timestamp = lambda: Expression("timestamp")
    functions.regexp_extract = lambda *args: Expression("batch_id")
    module = runpy.run_path(str(repository_root / "src/pipelines/pipeline_common.py"))
    assert module["read_landing"]("source_customers") is reader
    assert reader.format_name == "cloudFiles"
    assert reader.ddl == definitions.SOURCE_SCHEMAS["source_customers"]
    assert reader.path == "/Volumes/main/poc_landing/landing/declared_source/source_customers"
    assert reader.selected == ["business_id", "new_contract_field", "_rescued_data", "_source_file"]
