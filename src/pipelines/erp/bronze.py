"""Append-only ERP Bronze streaming tables."""
# ruff: noqa: E402

# Databricks executes these source files from their own directory.
# Make the shared pipeline modules available without depending on the working directory.
import sys
from pathlib import Path

pipeline_root = str(Path(__file__).resolve().parents[1])
if pipeline_root not in sys.path:
    sys.path.insert(0, pipeline_root)

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
