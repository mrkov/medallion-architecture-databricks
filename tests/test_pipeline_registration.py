"""Exercise configured source imports and dataset registration without a Spark runtime."""

import glob
import runpy
import shutil
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest
import yaml

from medallion_poc.contracts import load_yaml


@pytest.fixture
def registered_datasets(monkeypatch, repository_root):
    registered = {}

    def dataset_decorator(kind, **settings):
        def register(function):
            name = settings["name"]
            assert name not in registered, f"Dataset registered more than once: {name}"
            registered[name] = {
                "kind": kind,
                "schema": settings.get("schema"),
                "expectations": getattr(function, "expectations", {}),
            }
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

    configuration = {
        "poc.catalog": "main",
        "poc.volume_name": "landing",
        "poc.bundle_root": str(repository_root),
    }
    spark = SimpleNamespace(conf=SimpleNamespace(get=configuration.get, set=configuration.__setitem__))
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
    for name in ("pipeline_common", "silver_common", "contract_loader"):
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
def test_pipeline_evaluation_without_file_registers_only_its_contracted_tables(
    repository_root, tmp_path, monkeypatch, registered_datasets, owner
):
    pipelines = load_yaml(repository_root / "resources/medallion_poc.yml")["resources"]["pipelines"]
    key = (
        f"{owner}_source_pipeline"
        if owner in {"erp", "crm", "ads"}
        else ("platinum_marketing_pipeline" if owner == "platinum" else "gold_pipeline")
    )
    if owner == "platinum":
        sys.modules["pyspark.sql"].SparkSession.getActiveSession().conf.set("poc.domain", "marketing")
    pipeline_root = (repository_root / "src/pipelines").resolve()
    # Simulate Lakeflow: no __file__, no injected spark, an unrelated cwd, and no shared source paths.
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "path",
        [entry for entry in sys.path if Path(entry).resolve() not in {pipeline_root, pipeline_root.parent}],
    )
    files = _configured_files(repository_root, pipelines[key])
    assert files
    for path in files:
        namespace = {"__name__": "__pipeline__"}
        exec(compile(path.read_text(), str(path), "exec"), namespace)
        assert "__file__" not in namespace

    if owner in {"erp", "crm", "ads"}:
        contract = load_yaml(repository_root / f"contracts/source/{owner}.odcs.yaml")
        objects = {element["name"] for element in contract["schema"]}
        silver = load_yaml(repository_root / f"contracts/silver/{owner}.odcs.yaml")
        expected = (
            {f"main.bronze_{owner}.{name}" for name in objects}
            | {element["physicalName"].replace("<catalog>", "main") for element in silver["schema"]}
            | {f"main.silver_{owner}.contract_quarantine"}
        )
        assert set(registered_datasets) == expected
        for name in objects:
            bronze = registered_datasets[f"main.bronze_{owner}.{name}"]
            assert bronze["kind"] == "streaming_table"
            assert bronze["expectations"]["policy"] == "observe"
            assert registered_datasets[f"main.silver_{owner}.{name}"]["kind"] == "materialized_view"
        if owner == "erp":
            assert registered_datasets["main.silver_erp.erp_products"]["expectations"]["policy"] == "fail"
    else:
        contracts = [load_yaml(path) for path in sorted(repository_root.glob(f"contracts/{owner}/*.odcs.yaml"))]
        expected = {
            element["physicalName"].replace("<catalog>", "main")
            for contract in contracts
            for element in contract["schema"]
            if element["physicalType"] == "materialized view"
        }
        assert set(registered_datasets) == expected
        assert all(value["expectations"]["policy"] == "fail" for value in registered_datasets.values())
        assert all("PRIMARY KEY" in value["schema"] for value in registered_datasets.values())


def test_contract_loader_without_file_uses_configured_root(repository_root, tmp_path, monkeypatch, registered_datasets):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "path", sys.path.copy())
    path = repository_root / "src/pipelines/contract_loader.py"
    namespace = {"__name__": "__pipeline__"}
    exec(compile(path.read_text(), str(path), "exec"), namespace)
    assert "__file__" not in namespace
    assert namespace["CONTRACT_ROOT"] == repository_root / "contracts"
    assert namespace["source_definition"]("crm_contacts").source_system == "crm"


@pytest.mark.parametrize("bundle_root", ["", "relative/path"])
def test_missing_or_relative_bundle_root_requests_redeployment(repository_root, registered_datasets, bundle_root):
    spark = sys.modules["pyspark.sql"].SparkSession.getActiveSession()
    spark.conf.set("poc.bundle_root", bundle_root)
    path = repository_root / "src/pipelines/crm/bronze.py"
    with pytest.raises(RuntimeError, match="Redeploy the bundle"):
        exec(compile(path.read_text(), str(path), "exec"), {"__name__": "__pipeline__"})


def test_read_landing_consumes_deployed_yaml_directly(repository_root, tmp_path, monkeypatch, registered_datasets):
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

    # Change only deployed YAML contracts; use the actual loader and ingestion helper.
    shutil.copytree(repository_root / "src", tmp_path / "src", ignore=shutil.ignore_patterns("__pycache__"))
    shutil.copytree(repository_root / "contracts", tmp_path / "contracts")
    contract_path = tmp_path / "contracts/source/crm.odcs.yaml"
    contract = load_yaml(contract_path)
    contract["schema"][0]["properties"].append(
        {"name": "new_contract_field", "logicalType": "string", "physicalType": "STRING", "required": False}
    )
    contract["customProperties"][0]["value"] = "declared_source"
    contract_path.write_text(yaml.safe_dump(contract, sort_keys=False))
    policy_path = tmp_path / "contracts/gold/commerce_model.odcs.yaml"
    policy = load_yaml(policy_path)
    config = next(p["value"] for p in policy["customProperties"] if p["property"] == "orchestration")
    config["gold_domains"]["commerce"]["required_sources"] = ["erp", "declared_source", "ads"]
    policy_path.write_text(yaml.safe_dump(policy, sort_keys=False))
    monkeypatch.syspath_prepend(str(tmp_path / "src/pipelines"))
    reader = Reader()
    spark = sys.modules["pyspark.sql"].SparkSession.getActiveSession()
    spark.conf.set("poc.bundle_root", str(tmp_path))
    spark.readStream = reader
    functions = sys.modules["pyspark.sql.functions"]
    functions.col = Expression
    functions.lit = Expression
    functions.current_timestamp = lambda: Expression("timestamp")
    functions.regexp_extract = lambda *args: Expression("batch_id")
    module = runpy.run_path(str(tmp_path / "src/pipelines/pipeline_common.py"))
    assert module["read_landing"]("crm_contacts") is reader
    assert reader.format_name == "cloudFiles"
    assert reader.ddl.endswith("new_contract_field STRING")
    assert reader.path == "/Volumes/main/bronze_crm/landing/crm_contacts"
    assert reader.selected == [
        *[prop["name"] for prop in contract["schema"][0]["properties"]],
        "_rescued_data",
        "_source_file",
    ]
