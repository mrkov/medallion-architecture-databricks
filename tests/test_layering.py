import copy

import pytest

from medallion_poc.execution import settings, validate_catalog


@pytest.mark.parametrize("layer,allowed", [("silver", "bronze"), ("gold", "silver"), ("platinum", "gold")])
def test_only_previous_layer_inputs_are_accepted(contracts, layer, allowed):
    objects = [obj for reference, obj in contracts.objects.items() if reference.startswith(layer + ".")]
    assert objects
    assert all(ref.startswith(allowed + ".") for obj in objects for ref in settings(obj)["inputs"].values())
    damaged = copy.deepcopy(contracts)
    obj = next(obj for ref, obj in damaged.objects.items() if ref.startswith(layer + "."))
    config = settings(obj)
    config["inputs"][next(iter(config["inputs"]))] = "gold.dim_customer"
    if allowed == "gold":
        config["inputs"][next(iter(config["inputs"]))] = "platinum.dim_customer"
    with pytest.raises(ValueError, match="must read only"):
        validate_catalog(damaged)


def test_undeclared_sql_table_read_is_rejected(contracts):
    obj = contracts.objects["gold.dim_product"]
    settings(obj)["from_sql"] += " JOIN secret.other_table ON true"
    with pytest.raises(ValueError, match="all table reads"):
        validate_catalog(contracts)


def test_bronze_preserves_raw_data_and_supported_metadata(repository_root):
    source = (repository_root / "src/pipelines/pipeline_common.py").read_text()
    assert "readStream" in source
    assert '"delta.appendOnly": "true"' in source
    assert "_metadata.file_path" in source
    assert "input_file_name" not in source
