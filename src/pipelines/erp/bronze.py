"""Append-only ERP Bronze streaming tables."""
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
    name=table_name("bronze", "erp_customers"),
    comment="Exact ERP customer payload as strings plus ingestion metadata.",
    table_properties={"layer": "bronze", "source_system": "erp"},
)
@dp.expect_all(SOURCE_EXPECTATIONS["erp_customers"])
def bronze_erp_customers():
    return read_landing("erp_customers")


@dp.table(
    name=table_name("bronze", "erp_products"),
    comment="Exact ERP product payload as strings plus ingestion metadata.",
    table_properties={"layer": "bronze", "source_system": "erp"},
)
@dp.expect_all(SOURCE_EXPECTATIONS["erp_products"])
def bronze_erp_products():
    return read_landing("erp_products")


@dp.table(
    name=table_name("bronze", "erp_orders"),
    comment="Exact ERP order payload as strings plus ingestion metadata.",
    table_properties={"layer": "bronze", "source_system": "erp"},
)
@dp.expect_all(SOURCE_EXPECTATIONS["erp_orders"])
def bronze_erp_orders():
    return read_landing("erp_orders")


@dp.table(
    name=table_name("bronze", "erp_order_lines"),
    comment="Exact ERP order-line payload as strings plus ingestion metadata.",
    table_properties={"layer": "bronze", "source_system": "erp"},
)
@dp.expect_all(SOURCE_EXPECTATIONS["erp_order_lines"])
def bronze_erp_order_lines():
    return read_landing("erp_order_lines")
