"""Append-only ADS Bronze streaming tables."""
# ruff: noqa: E402

# Databricks executes these source files from their own directory.
# Make the shared pipeline modules available without depending on the working directory.
import sys
from pathlib import Path

pipeline_root = str(Path(__file__).resolve().parents[1])
if pipeline_root not in sys.path:
    sys.path.insert(0, pipeline_root)

from contract_rules import SOURCE_EXPECTATIONS
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
