"""Compile ODCS required/enum constraints into Lakeflow expectation dictionaries."""

from __future__ import annotations

import argparse
import pprint
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from medallion_poc.contracts import compile_object, validate_contract  # noqa: E402

SCHEMA = ROOT / "contracts" / "schema" / "odcs-json-schema-v3.2.0.json"
DESTINATION = ROOT / "src" / "pipelines" / "contract_rules.py"


def compiled_rules() -> dict[str, dict[str, dict[str, str]]]:
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
    for directory, constant in contract_roots.items():
        for path in sorted((ROOT / "contracts" / directory).glob("*.odcs.yaml")):
            contract = validate_contract(path, SCHEMA)
            for element in contract.get("schema", []):
                groups[constant][element["name"]] = compile_object(
                    contract, element["name"]
                ).expectations
    return groups


def render() -> str:
    sections = [
        '"""Generated from ODCS contracts by scripts/compile_contracts.py."""',
        "",
    ]
    for name, rules in compiled_rules().items():
        sections.extend([f"{name} = {pprint.pformat(rules, sort_dicts=True, width=100)}", ""])
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
            print(f"Generated contract rules are stale: {DESTINATION}", file=sys.stderr)
            return 1
        print("Generated contract rules are current")
        return 0
    DESTINATION.write_text(expected, encoding="utf-8")
    print(f"Wrote {DESTINATION}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

