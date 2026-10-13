"""Append-only CRM Bronze streaming tables."""
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
    name=table_name("bronze", "crm_contacts"),
    comment="Exact CRM contact payload as strings plus ingestion metadata.",
    table_properties={"layer": "bronze", "source_system": "crm"},
)
@dp.expect_all(SOURCE_EXPECTATIONS["crm_contacts"])
def bronze_crm_contacts():
    return read_landing("crm_contacts")
