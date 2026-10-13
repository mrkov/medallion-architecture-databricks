"""Append-only CRM Bronze streaming tables."""
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
    name=table_name("bronze", "crm_contacts"),
    comment="Exact CRM contact payload as strings plus ingestion metadata.",
    table_properties={"layer": "bronze", "source_system": "crm"},
)
@dp.expect_all(SOURCE_EXPECTATIONS["crm_contacts"])
def bronze_crm_contacts():
    return read_landing("crm_contacts")
