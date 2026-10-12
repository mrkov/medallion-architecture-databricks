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

