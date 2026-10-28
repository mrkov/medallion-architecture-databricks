# Databricks notebook source
# MAGIC %md
# MAGIC Run with the same catalog and volume as the deployed bundle.
# MAGIC Leave bundle_root empty when opening this notebook from the bundle's src/notebooks folder.

# COMMAND ----------
# MAGIC %pip install open-data-contract-standard==3.2.0 "sqlglot==27.29.0"

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
from medallion_poc.contracts import load_yaml
from medallion_poc.demo import scenario_query, seed_source, wait_for_assertions

dbutils.widgets.dropdown("action", "seed", ["seed", "wait"])
dbutils.widgets.text("stage", "erp")
dbutils.widgets.dropdown("scenario", "scenario", ["scenario", "scd2", "keys"])
stage = dbutils.widgets.get("stage")
scenario_name = dbutils.widgets.get("scenario")
if scenario_name not in {"scenario", "scd2", "keys"}:
    raise ValueError("Unknown demo scenario")
scenario = load_yaml(bundle_root / f"demo/{scenario_name}.yaml")
step = scenario["stages"][stage]
if dbutils.widgets.get("action") == "seed":
    if "precondition" in step:
        check = step["precondition"]
        actual = spark.sql(scenario_query(check["query"], contracts, catalog)).first()[0]
        if actual != check["equals"]:
            raise ValueError(check["message"])
    seed_source(dbutils, contracts, step["source"], catalog, volume_name, step.get("example_batch"))
else:
    wait_for_assertions(spark, step["assertions"], contracts, catalog)
print(f"Completed {dbutils.widgets.get('action')} for {stage}")
