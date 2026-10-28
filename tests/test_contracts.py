from __future__ import annotations

import copy
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml
from open_data_contract_standard.model import OpenDataContractStandard

from medallion_poc.contracts import (
    breaking_changes,
    compile_object,
    load_contract_catalog,
    load_yaml,
    source_interfaces,
    validate_contract,
)


def contract_paths(root: Path) -> list[Path]:
    return sorted(root.glob("contracts/*/*.odcs.yaml"))


def test_all_contracts_conform_to_official_odcs_schema(repository_root, odcs_schema):
    paths = contract_paths(repository_root)
    assert len(paths) == 9
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
    contract = load_yaml(repository_root / "contracts/silver/erp.odcs.yaml")
    element = next(obj for obj in contract["schema"] if obj["name"] == "erp_products")
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
    customers = source_interfaces(contract)[0]
    assert customers.source_system == "replacement_erp"
    assert customers.columns[:3] == ("customer_id", "optional_customer_label", "customer_number")
    assert customers.spark_schema.startswith(
        "customer_id STRING, optional_customer_label STRING, customer_number STRING"
    )
    assert "optional_customer_label_not_null" not in compile_object(contract, "erp_customers").expectations


@pytest.mark.parametrize(
    "invalid", ["missing_source", "unsafe_source", "typed_bronze", "duplicate_column", "metadata_column"]
)
def test_invalid_ingestion_contracts_fail_validation(repository_root, invalid):
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
        source_interfaces(contract)


def test_yaml_edit_changes_ingestion_and_expectations_without_regeneration(repository_root, tmp_path):
    shutil.copytree(repository_root / "contracts", tmp_path / "contracts")
    before = load_contract_catalog(tmp_path / "contracts")
    contract_path = tmp_path / "contracts/source/ads.odcs.yaml"
    contract = load_yaml(contract_path)
    contract["schema"][0]["properties"].append(
        {"name": "placement_type", "logicalType": "string", "physicalType": "STRING", "required": False}
    )
    contract["customProperties"][0]["value"] = "paid_media"
    contract_path.write_text(yaml.safe_dump(contract, sort_keys=False))
    # A renamed source must also be named consistently in the contract's completion policy.
    policy_path = tmp_path / "contracts/gold/commerce_model.odcs.yaml"
    policy = load_yaml(policy_path)
    config = next(p["value"] for p in policy["customProperties"] if p["property"] == "orchestration")
    config["gold_domains"]["commerce"]["required_sources"] = ["erp", "crm", "paid_media"]
    policy_path.write_text(yaml.safe_dump(policy, sort_keys=False))

    after = load_contract_catalog(tmp_path / "contracts")
    assert "placement_type" not in before.sources["ads_campaigns"].columns
    assert after.sources["ads_campaigns"].columns[-1] == "placement_type"
    assert after.sources["ads_campaigns"].spark_schema.endswith("placement_type STRING")
    assert after.sources["ads_campaigns"].source_system == "paid_media"
    assert "placement_type_not_null" not in after.expectations["source"]["ads_campaigns"]
    # Required fields and enumerations also respond to YAML changes during initialization.
    contract["schema"][0]["properties"][-1]["required"] = True
    contract["schema"][0]["properties"][-1]["enum"] = [{"value": "Feed"}, {"value": "Stories"}]
    contract_path.write_text(yaml.safe_dump(contract, sort_keys=False))
    rules = load_contract_catalog(tmp_path / "contracts").expectations["source"]["ads_campaigns"]
    assert rules["placement_type_not_null"] == "placement_type IS NOT NULL"
    assert rules["placement_type_accepted_values"] == "placement_type IN ('Feed', 'Stories')"


def test_contract_validation_command(repository_root):
    completed = subprocess.run(
        [sys.executable, "scripts/validate_contracts.py"],
        cwd=repository_root,
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr


def test_runtime_catalog_uses_odcs_library_and_matches_all_contracts(repository_root, monkeypatch):
    paths = []
    original = OpenDataContractStandard.from_file

    def load(path):
        paths.append(Path(path))
        return original(path)

    monkeypatch.setattr(OpenDataContractStandard, "from_file", load)
    catalog = load_contract_catalog(repository_root / "contracts")
    assert set(paths) == set(contract_paths(repository_root))
    for path in paths:
        contract = load_yaml(path)
        layer = path.parent.name
        for element in contract["schema"]:
            assert (
                catalog.expectations[layer][element["name"]] == compile_object(contract, element["name"]).expectations
            )
            if layer == "source":
                assert catalog.sources[element["name"]].columns == tuple(prop["name"] for prop in element["properties"])


def test_missing_deployed_contracts_fail_with_a_useful_error(tmp_path):
    with pytest.raises(ValueError, match="No ODCS contracts found"):
        load_contract_catalog(tmp_path)


def test_inconsistent_logical_and_physical_type_is_rejected(contracts):
    from medallion_poc.execution import validate_catalog

    contracts.objects["gold.dim_product"]["properties"][0]["logicalType"] = "integer"
    with pytest.raises(ValueError, match="Logical/physical type mismatch"):
        validate_catalog(contracts)


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


def test_key_storage_type_change_is_breaking_even_with_same_logical_type(repository_root):
    previous = load_yaml(repository_root / "contracts/gold/commerce_model.odcs.yaml")
    current = copy.deepcopy(previous)
    previous["schema"][0]["properties"][0]["physicalType"] = "STRING"
    assert breaking_changes(previous, current) == ["changed physical type of dim_customer.customer_key"]
