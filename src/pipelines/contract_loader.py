"""Read the deployed ODCS files once per pipeline initialization."""

import sys
from pathlib import Path

from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()
if spark is None:
    raise RuntimeError("An active Spark session is required")
bundle_root = spark.conf.get("poc.bundle_root", "")
if not bundle_root or not Path(bundle_root).is_absolute():
    raise RuntimeError("Redeploy the bundle to configure poc.bundle_root as an absolute workspace path")
sys.path.insert(0, str(Path(bundle_root) / "src"))
from medallion_poc.contracts import load_contract_catalog  # noqa: E402

CONTRACT_ROOT = Path(bundle_root) / "contracts"
CONTRACTS = load_contract_catalog(CONTRACT_ROOT)


def source_definition(object_name):
    return CONTRACTS.sources[object_name]
