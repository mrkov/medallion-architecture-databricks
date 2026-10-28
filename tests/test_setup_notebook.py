import copy
import re
import sys
from types import SimpleNamespace

from medallion_poc.demo import seed_source, wait_for_assertions
from medallion_poc.workspace import prepare_storage


def test_setup_is_repeatable_without_deleting_data(contracts):
    schemas = set()
    volumes = set()
    directories = set()
    files = {"/Volumes/main/bronze_erp/landing/existing.csv": "original"}
    original = copy.deepcopy(files)

    def sql(query):
        if m := re.fullmatch(r"CREATE SCHEMA IF NOT EXISTS `([^`]+)`\.`([^`]+)`", query):
            schemas.add(m.groups())
        elif m := re.fullmatch(r"CREATE VOLUME IF NOT EXISTS `([^`]+)`\.`([^`]+)`\.`([^`]+)`", query):
            assert m.groups()[:2] in schemas
            volumes.add(m.groups())
        else:
            raise AssertionError(query)

    def mkdirs(path):
        assert tuple(path.split("/")[2:5]) in volumes
        directories.add(path)

    for _ in range(2):
        prepare_storage(
            SimpleNamespace(sql=sql), SimpleNamespace(fs=SimpleNamespace(mkdirs=mkdirs)), contracts, "main", "landing"
        )
    assert schemas == {
        ("main", s)
        for s in (
            "bronze_erp",
            "bronze_crm",
            "bronze_ads",
            "silver_erp",
            "silver_crm",
            "silver_ads",
            "gold_common",
            "platinum_sales",
            "platinum_marketing",
        )
    }
    assert len(volumes) == 3
    assert len(directories) == 7
    assert files == original


def test_setup_notebook_can_execute_without_file_global(repository_root, monkeypatch):
    monkeypatch.setattr(sys, "path", sys.path.copy())
    params = {"catalog": "main", "volume_name": "landing", "bundle_root": str(repository_root)}
    namespace = {
        "spark": SimpleNamespace(sql=lambda query: SimpleNamespace(collect=lambda: [])),
        "dbutils": SimpleNamespace(
            widgets=SimpleNamespace(text=lambda *args: None, get=params.__getitem__),
            fs=SimpleNamespace(mkdirs=lambda path: None),
        ),
    }
    path = repository_root / "src/notebooks/setup_poc.py"
    exec(compile(path.read_text(), str(path), "exec"), namespace)
    assert "__file__" not in namespace


def test_demo_seed_is_append_only_and_uses_contract_columns(contracts):
    files = {}

    def put(path, text, overwrite):
        assert not overwrite and path not in files
        files[path] = text

    fs = SimpleNamespace(
        ls=lambda folder: [SimpleNamespace(path=path) for path in files if path.startswith(folder + "/")],
        put=put,
        head=lambda path, size: files[path],
    )
    dbutils = SimpleNamespace(fs=fs)
    seed_source(dbutils, contracts, "erp", "main", "landing")
    before = files.copy()
    seed_source(dbutils, contracts, "erp", "main", "landing")
    assert files == before
    for name, source in contracts.sources.items():
        if source.source_system == "erp":
            text = next(text for path, text in files.items() if "/" + name + "/" in path)
            assert text.splitlines()[0] == ",".join(source.columns)


def test_demo_wait_reports_a_disabled_or_broken_trigger(contracts, monkeypatch):
    import pytest

    spark = SimpleNamespace(sql=lambda query: SimpleNamespace(collect=lambda: []))
    with pytest.raises(TimeoutError, match="table-update trigger"):
        wait_for_assertions(
            spark, [{"name": "late arrival", "query": "SELECT 1", "equals": 1}], contracts, "main", timeout_seconds=0
        )


def test_named_demo_batch_writes_only_changed_source_objects_and_is_retryable(contracts):
    import csv
    import io

    import pytest

    files = {}

    def put(path, text, overwrite):
        assert not overwrite and path not in files
        files[path] = text

    fs = SimpleNamespace(
        ls=lambda folder: [SimpleNamespace(path=p) for p in files if p.startswith(folder + "/")],
        put=put,
        head=lambda path, size: files[path],
    )
    dbutils = SimpleNamespace(fs=fs)
    for _ in range(2):
        seed_source(dbutils, contracts, "erp", "main", "landing", "scd2_move")
    assert len(files) == 1
    path, text = next(iter(files.items()))
    assert "/erp_customers/" in path
    assert list(csv.DictReader(io.StringIO(text)))[0]["country_code"] == "DE"
    with pytest.raises(ValueError, match="No example rows"):
        seed_source(dbutils, contracts, "erp", "main", "landing", "misspelled_batch")
