"""Load ODCS YAML contracts once when a pipeline process initializes."""
# ruff: noqa: E402

import sys
from pathlib import Path

from pyspark.sql import SparkSession

# Source evaluation may omit __file__; the bundle supplies the actual workspace file root.
active_spark = SparkSession.getActiveSession()
if active_spark is None:
    raise RuntimeError("An active Spark session is required")
bundle_root = active_spark.conf.get("poc.bundle_root", "")
if not bundle_root or not Path(bundle_root).is_absolute():
    raise RuntimeError("Redeploy the bundle to configure poc.bundle_root as an absolute workspace path")
project_src = str(Path(bundle_root) / "src")
if project_src not in sys.path:
    sys.path.insert(0, project_src)

from medallion_poc.contracts import SourceInterface, load_contract_catalog

CONTRACT_ROOT = Path(bundle_root) / "contracts"
catalog = load_contract_catalog(CONTRACT_ROOT)

SOURCE_EXPECTATIONS = catalog.expectations["source"]
SILVER_EXPECTATIONS = catalog.expectations["silver"]
GOLD_EXPECTATIONS = catalog.expectations["gold"]
PLATINUM_EXPECTATIONS = catalog.expectations["platinum"]
SILVER_PRODUCT_EXPECTATIONS = SILVER_EXPECTATIONS["erp_products"]


def source_definition(object_name: str) -> SourceInterface:
    return catalog.sources[object_name]
