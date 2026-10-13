import copy
import re
import sys
from types import SimpleNamespace

import pytest

from medallion_poc.contracts import load_contract_catalog


def test_setup_notebook_creates_storage_and_preserves_existing_data(repository_root, monkeypatch):
    monkeypatch.setattr(sys, "path", sys.path.copy())
    parameters = {
        "catalog": "main",
        "schema_prefix": "customer_poc",
        "volume_name": "arrivals",
        "bundle_root": str(repository_root),
    }
    schemas = {("main", "customer_poc_bronze")}
    volumes = set()
    directories = set()
    files = {"/Volumes/main/customer_poc_landing/arrivals/crm/crm_contacts/existing.csv": "original CRM payload"}
    tables = {"main.customer_poc_bronze.crm_contacts": [{"contact_id": "CRM_EXISTING"}]}
    original_files = copy.deepcopy(files)
    original_tables = copy.deepcopy(tables)

    def sql(query):
        if match := re.fullmatch(r"CREATE SCHEMA IF NOT EXISTS `([^`]+)`\.`([^`]+)`", query):
            schemas.add(match.groups())
        elif match := re.fullmatch(r"CREATE VOLUME IF NOT EXISTS `([^`]+)`\.`([^`]+)`\.`([^`]+)`", query):
            assert match.groups()[:2] in schemas
            volumes.add(match.groups())
        else:
            raise AssertionError(f"Unexpected setup SQL: {query}")

    def mkdirs(directory):
        assert tuple(directory.split("/")[2:5]) in volumes, "The landing volume must exist before source folders"
        directories.add(directory)

    def forbidden(*args):
        raise AssertionError("Setup must not delete or overwrite source files")

    dbutils = SimpleNamespace(
        widgets=SimpleNamespace(text=lambda *args: None, get=parameters.__getitem__),
        fs=SimpleNamespace(mkdirs=mkdirs, rm=forbidden, put=forbidden),
    )
    spark = SimpleNamespace(sql=sql)
    path = repository_root / "src/notebooks/setup_poc.py"
    for _ in range(2):
        namespace = {"__name__": "__notebook__", "dbutils": dbutils, "spark": spark}
        exec(compile(path.read_text(), str(path), "exec"), namespace)
        assert "__file__" not in namespace
    assert schemas == {
        ("main", f"customer_poc_{suffix}")
        for suffix in ("landing", "bronze", "silver", "gold", "platinum", "platinum_sales", "platinum_marketing")
    }
    assert volumes == {("main", "customer_poc_landing", "arrivals")}
    sources = load_contract_catalog(repository_root / "contracts").sources
    assert directories == {
        f"/Volumes/main/customer_poc_landing/arrivals/{source.source_system}/{source.object_name}"
        for source in sources.values()
    }
    assert files == original_files
    assert tables == original_tables
    resolve = namespace["bundle_root_from_notebook_path"]
    assert str(resolve("/Users/test/.bundle/poc/dev/files/src/notebooks/setup_poc")) == (
        "/Workspace/Users/test/.bundle/poc/dev/files"
    )
    assert str(resolve("/Workspace/Users/test/.bundle/poc/dev/files/src/notebooks/setup_poc")) == (
        "/Workspace/Users/test/.bundle/poc/dev/files"
    )
    with pytest.raises(ValueError, match="bundle_root widget"):
        resolve("/Users/test/another_folder/setup_poc")
