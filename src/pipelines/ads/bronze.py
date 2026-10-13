"""Append-only ADS Bronze streaming tables."""
# ruff: noqa: E402

# Lakeflow can evaluate source without __file__. The bundle supplies the deployed root.
import sys
from pathlib import Path

from pyspark.sql import SparkSession

active_spark = SparkSession.getActiveSession()
if active_spark is None:
    raise RuntimeError("An active Spark session is required")
bundle_root = active_spark.conf.get("poc.bundle_root", "")
if not bundle_root or not Path(bundle_root).is_absolute():
    raise RuntimeError("Redeploy the bundle to configure poc.bundle_root as an absolute workspace path")
for directory in ("src", "src/pipelines"):
    import_path = str(Path(bundle_root) / directory)
    if import_path not in sys.path:
        sys.path.insert(0, import_path)

from contract_loader import SOURCE_EXPECTATIONS
from pipeline_common import read_landing, table_name
from pyspark import pipelines as dp


@dp.table(
    name=table_name("bronze", "ads_campaigns"),
    comment="Exact advertising campaign payload as strings plus ingestion metadata.",
    table_properties={"layer": "bronze", "source_system": "ads"},
)
@dp.expect_all(SOURCE_EXPECTATIONS["ads_campaigns"])
def bronze_ads_campaigns():
    return read_landing("ads_campaigns")


@dp.table(
    name=table_name("bronze", "ads_spend"),
    comment="Exact advertising spend payload as strings plus ingestion metadata.",
    table_properties={"layer": "bronze", "source_system": "ads"},
)
@dp.expect_all(SOURCE_EXPECTATIONS["ads_spend"])
def bronze_ads_spend():
    return read_landing("ads_spend")
