from __future__ import annotations

import copy
import runpy
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from medallion_poc.contracts import (
    breaking_changes,
    compile_object,
    compile_source_objects,
    load_yaml,
    validate_contract,
)


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
    assert properties["product_hierarchy_raw"]["transformSourceObjects"] == ["bronze.erp_products"]
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


def test_source_contract_controls_column_order_raw_types_and_source_route(repository_root):
    contract = load_yaml(repository_root / "contracts/source/erp.odcs.yaml")
    contract["customProperties"][0]["value"] = "replacement_erp"
    contract["schema"][0]["properties"].insert(
        1, {"name": "optional_customer_label", "logicalType": "string", "required": False}
    )
    customers = compile_source_objects(contract)[0]
    assert customers.source_system == "replacement_erp"
    assert customers.columns[:3] == ("customer_id", "optional_customer_label", "customer_number")
    assert customers.spark_schema.startswith(
        "customer_id STRING, optional_customer_label STRING, customer_number STRING"
    )
    assert "optional_customer_label_not_null" not in compile_object(contract, "erp_customers").expectations


@pytest.mark.parametrize(
    "invalid", ["missing_source", "unsafe_source", "typed_bronze", "duplicate_column", "metadata_column"]
)
def test_invalid_ingestion_contracts_fail_compilation(repository_root, invalid):
    contract = load_yaml(repository_root / "contracts/source/crm.odcs.yaml")
    properties = contract["schema"][0]["properties"]
    if invalid == "missing_source":
        contract.pop("customProperties")
    elif invalid == "unsafe_source":
        contract["customProperties"][0]["value"] = "../other_source"
    elif invalid == "typed_bronze":
        properties[0]["logicalType"] = "integer"
    elif invalid == "duplicate_column":
        properties.append(copy.deepcopy(properties[0]))
    else:
        properties.append({"name": "_ingested_at", "logicalType": "string"})
    with pytest.raises(ValueError):
        compile_source_objects(contract)


def test_contract_edit_requires_regeneration_and_updates_ingestion_definitions(repository_root, tmp_path):
    for directory in ("contracts", "src"):
        shutil.copytree(repository_root / directory, tmp_path / directory, ignore=shutil.ignore_patterns("__pycache__"))
    (tmp_path / "scripts").mkdir()
    shutil.copy2(repository_root / "scripts/compile_contracts.py", tmp_path / "scripts/compile_contracts.py")
    contract_path = tmp_path / "contracts/source/ads.odcs.yaml"
    contract = load_yaml(contract_path)
    contract["schema"][0]["properties"].append({"name": "placement_type", "logicalType": "string", "required": False})
    contract["customProperties"][0]["value"] = "paid_media"
    contract_path.write_text(yaml.safe_dump(contract, sort_keys=False))

    def run_compiler(*arguments):
        return subprocess.run(
            [sys.executable, "scripts/compile_contracts.py", *arguments],
            cwd=tmp_path,
            text=True,
            capture_output=True,
            check=False,
        )

    stale = run_compiler("--check")
    assert stale.returncode == 1
    assert "stale" in stale.stderr
    regenerated = run_compiler()
    assert regenerated.returncode == 0, regenerated.stderr
    definitions = runpy.run_path(str(tmp_path / "src/pipelines/contract_rules.py"))
    assert definitions["SOURCE_COLUMNS"]["ads_campaigns"][-1] == "placement_type"
    assert definitions["SOURCE_SCHEMAS"]["ads_campaigns"].endswith("placement_type STRING")
    assert definitions["SOURCE_SYSTEM"]["ads_campaigns"] == "paid_media"
    assert "placement_type_not_null" not in definitions["SOURCE_EXPECTATIONS"]["ads_campaigns"]
    assert run_compiler("--check").returncode == 0


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
    current["schema"][0]["properties"].append({"name": "optional_label", "logicalType": "string", "required": False})
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
