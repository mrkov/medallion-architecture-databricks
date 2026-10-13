"""Compile ODCS source interfaces and constraints into pipeline definitions."""

from __future__ import annotations

import argparse
import pprint
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from medallion_poc.contracts import compile_object, compile_source_objects, validate_contract  # noqa: E402

SCHEMA = ROOT / "contracts" / "schema" / "odcs-json-schema-v3.2.0.json"
DESTINATION = ROOT / "src" / "pipelines" / "contract_rules.py"


def compiled_definitions() -> dict[str, Any]:
    groups: dict[str, dict[str, dict[str, str]]] = {
        "SOURCE_EXPECTATIONS": {},
        "SILVER_EXPECTATIONS": {},
        "GOLD_EXPECTATIONS": {},
        "PLATINUM_EXPECTATIONS": {},
    }
    contract_roots = {
        "source": "SOURCE_EXPECTATIONS",
        "silver": "SILVER_EXPECTATIONS",
        "gold": "GOLD_EXPECTATIONS",
        "platinum": "PLATINUM_EXPECTATIONS",
    }
    source_columns: dict[str, tuple[str, ...]] = {}
    source_systems: dict[str, str] = {}
    source_schemas: dict[str, str] = {}
    for directory, constant in contract_roots.items():
        for path in sorted((ROOT / "contracts" / directory).glob("*.odcs.yaml")):
            contract = validate_contract(path, SCHEMA)
            if directory == "source":
                for source in compile_source_objects(contract):
                    if source.object_name in source_columns:
                        raise ValueError(f"Source object {source.object_name!r} is declared by multiple contracts")
                    source_columns[source.object_name] = source.columns
                    source_systems[source.object_name] = source.source_system
                    source_schemas[source.object_name] = source.spark_schema
            for element in contract.get("schema", []):
                if element["name"] in groups[constant]:
                    raise ValueError(f"Duplicate object {element['name']!r} in {directory} contracts: {path}")
                groups[constant][element["name"]] = compile_object(contract, element["name"]).expectations
    return {
        "SOURCE_COLUMNS": source_columns,
        "SOURCE_SYSTEM": source_systems,
        "SOURCE_SCHEMAS": source_schemas,
        **groups,
    }


def render() -> str:
    sections = [
        '"""Generated from ODCS contracts by scripts/compile_contracts.py."""',
        "",
    ]
    for name, definitions in compiled_definitions().items():
        sections.extend([f"{name} = {pprint.pformat(definitions, sort_dicts=True, width=100)}", ""])
    sections.append("SILVER_PRODUCT_EXPECTATIONS = SILVER_EXPECTATIONS['erp_products']")
    sections.append("")
    return "\n".join(sections)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    expected = render()
    if args.check:
        actual = DESTINATION.read_text(encoding="utf-8") if DESTINATION.exists() else ""
        if actual != expected:
            print(f"Generated contract definitions are stale: {DESTINATION}", file=sys.stderr)
            return 1
        print("Generated contract definitions are current")
        return 0
    DESTINATION.write_text(expected, encoding="utf-8")
    print(f"Wrote {DESTINATION}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
