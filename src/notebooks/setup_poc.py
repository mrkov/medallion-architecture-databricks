# Databricks notebook source
# ruff: noqa: E402, F821
# MAGIC %md
# MAGIC Run with the same catalog and volume as the deployed bundle.
# MAGIC Leave bundle_root empty when opening this notebook from the bundle's src/notebooks folder.

# COMMAND ----------
# MAGIC %pip install open-data-contract-standard==3.2.0 "sqlglot>=27,<28"

# COMMAND ----------
import sys
from pathlib import Path

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("volume_name", "landing")
dbutils.widgets.text("bundle_root", "")
catalog = dbutils.widgets.get("catalog")
volume_name = dbutils.widgets.get("volume_name")
configured_root = dbutils.widgets.get("bundle_root").strip()
if configured_root:
    bundle_root = Path(configured_root)
else:
    path = Path(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
    if path.parent.name != "notebooks" or path.parent.parent.name != "src":
        raise ValueError("Open this notebook under src/notebooks, or set the bundle_root widget")
    if not path.is_absolute():
        raise ValueError("Notebook path must be absolute")
    if path.parts[1] != "Workspace":
        path = Path("/Workspace") / path.relative_to("/")
    bundle_root = path.parents[2]
if not bundle_root.is_absolute():
    raise ValueError("bundle_root must be absolute")
sys.path.insert(0, str(bundle_root / "src"))
from medallion_poc.contracts import load_contract_catalog
from medallion_poc.execution import quote

for identifier in (catalog, volume_name):
    quote(identifier)
contracts = load_contract_catalog(bundle_root / "contracts")

# COMMAND ----------
from medallion_poc.workspace import prepare_storage

for path in prepare_storage(spark, dbutils, contracts, catalog, volume_name):
    print(path)
print("Storage ready. Existing files and tables were preserved.")

# COMMAND ----------
# MAGIC %md
# MAGIC The setup_poc job additionally initializes source interfaces, builds empty Gold outputs,
# MAGIC and publishes Platinum views. Then enable the Gold table-update trigger as described in README.
