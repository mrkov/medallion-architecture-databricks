"""Validate ODCS YAML against the pinned schema and the pipeline's runtime loader."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from medallion_poc.contracts import load_contract_catalog, validate_contract  # noqa: E402


def main() -> int:
    schema = ROOT / "contracts/schema/odcs-json-schema-v3.2.0.json"
    paths = sorted((ROOT / "contracts").glob("*/*.odcs.yaml"))
    for path in paths:
        validate_contract(path, schema)
    catalog = load_contract_catalog(ROOT / "contracts")
    print(f"Validated {len(paths)} ODCS contracts and {len(catalog.sources)} raw source interfaces")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
