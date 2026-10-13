"""Load ODCS YAML contracts once when a pipeline process initializes."""
# ruff: noqa: E402

import sys
from pathlib import Path

# The bundle uploads both src/ and contracts/; resolve paths relative to this module.
project_src = str(Path(__file__).resolve().parents[1])
if project_src not in sys.path:
    sys.path.insert(0, project_src)

from medallion_poc.contracts import SourceInterface, load_contract_catalog

CONTRACT_ROOT = Path(__file__).resolve().parents[2] / "contracts"
catalog = load_contract_catalog(CONTRACT_ROOT)

SOURCE_EXPECTATIONS = catalog.expectations["source"]
SILVER_EXPECTATIONS = catalog.expectations["silver"]
GOLD_EXPECTATIONS = catalog.expectations["gold"]
PLATINUM_EXPECTATIONS = catalog.expectations["platinum"]
SILVER_PRODUCT_EXPECTATIONS = SILVER_EXPECTATIONS["erp_products"]


def source_definition(object_name: str) -> SourceInterface:
    return catalog.sources[object_name]
