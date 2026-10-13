from __future__ import annotations

import copy
import subprocess
import sys
from pathlib import Path

from medallion_poc.contracts import breaking_changes, compile_object, load_yaml, validate_contract


def contract_paths(root: Path) -> list[Path]:
    return sorted(root.glob("contracts/*/*.odcs.yaml"))


def test_all_contracts_conform_to_official_odcs_schema(repository_root, odcs_schema):
    paths = contract_paths(repository_root)
    assert len(paths) == 7
    for path in paths:
        contract = validate_contract(path, odcs_schema)
        assert contract["apiVersion"] == "v3.2.0"


def test_bronze_source_contracts_define_all_payload_fields_as_strings(repository_root):
    for path in sorted(repository_root.glob("contracts/source/*.odcs.yaml")):
        contract = load_yaml(path)
        for element in contract["schema"]:
            assert element["dataGranularityDescription"]
            assert all(prop["logicalType"] == "string" for prop in element["properties"])


def test_silver_product_contract_documents_derived_columns(repository_root):
    contract = load_yaml(repository_root / "contracts/silver/erp_products.odcs.yaml")
    element = contract["schema"][0]
    properties = {prop["name"]: prop for prop in element["properties"]}
    assert properties["product_hierarchy_raw"]["transformSourceObjects"] == [
        "bronze.erp_products"
    ]
    for name in ("product_family", "product_tier", "product_format", "product_orientation"):
        assert "split" in properties[name]["transformLogic"]
        assert properties[name]["required"] is True


def test_contract_compiler_generates_required_and_enum_expectations(repository_root):
    contract = load_yaml(repository_root / "contracts/gold/commerce_model.odcs.yaml")
    compiled = compile_object(contract, "fact_attributed_sales")
    assert compiled.primary_key == ("order_line_id",)
    assert compiled.expectations["order_line_id_not_null"] == "order_line_id IS NOT NULL"
    assert compiled.expectations["completeness_status_accepted_values"] == (
        "completeness_status IN ('ERP_ONLY', 'CRM_ENRICHED', 'COMPLETE')"
    )


def test_generated_lakeflow_rules_are_current(repository_root):
    completed = subprocess.run(
        [sys.executable, "scripts/compile_contracts.py", "--check"],
        cwd=repository_root,
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr


def test_compatibility_allows_nullable_additions(repository_root):
    previous = load_yaml(repository_root / "contracts/platinum/sales_mart.odcs.yaml")
    current = copy.deepcopy(previous)
    current["version"] = "2.1.0"
    current["schema"][0]["properties"].append(
        {"name": "optional_label", "logicalType": "string", "required": False}
    )
    assert breaking_changes(previous, current) == []


def test_compatibility_detects_removed_or_retyped_fields(repository_root):
    previous = load_yaml(repository_root / "contracts/platinum/sales_mart.odcs.yaml")
    current = copy.deepcopy(previous)
    properties = current["schema"][0]["properties"]
    properties[:] = [prop for prop in properties if prop["name"] != "currency_code"]
    next(prop for prop in properties if prop["name"] == "line_amount")["logicalType"] = "string"
    assert breaking_changes(previous, current) == [
        "changed type of sales_detail.line_amount",
        "removed property sales_detail.currency_code",
    ]

