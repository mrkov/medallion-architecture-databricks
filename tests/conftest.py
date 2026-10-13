from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def repository_root() -> Path:
    return Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def odcs_schema(repository_root: Path) -> Path:
    path = repository_root / "contracts/schema/odcs-json-schema-v3.2.0.json"
    if not path.exists():
        pytest.fail("Run `python scripts/fetch_odcs_schema.py` before tests")
    return path


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from pyspark.sql import SparkSession

    warehouse = tmp_path_factory.mktemp("spark")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("odcs-contract-execution")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def contracts(repository_root):
    from medallion_poc.contracts import load_contract_catalog

    return load_contract_catalog(repository_root / "contracts")
