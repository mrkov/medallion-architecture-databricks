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
from datetime import datetime
from zoneinfo import ZoneInfo

from medallion_poc.operations import check_gate, record_load

for name, default in {
    "action": "gate",
    "source": "",
    "receipt_business_date": "",
    "load_group": "standard",
    "run_id": "",
    "domain": "commerce",
    "expected_ready": "",
}.items():
    dbutils.widgets.text(name, default)
params = {
    name: dbutils.widgets.get(name)
    for name in ("action", "source", "receipt_business_date", "load_group", "run_id", "domain", "expected_ready")
}
day = (
    params["receipt_business_date"]
    or datetime.now(ZoneInfo(contracts.orchestration["business_timezone"])).date().isoformat()
)
if params["action"] not in {"STARTED", "SUCCEEDED", "FAILED", "gate", "require_ready"}:
    raise ValueError(f"Unsupported load-control action: {params['action']}")
if params["expected_ready"] not in {"", "true", "false"}:
    raise ValueError("expected_ready must be empty, true or false")
if params["action"] in {"STARTED", "SUCCEEDED", "FAILED"}:
    record_load(
        spark, contracts, catalog, params["source"], day, params["load_group"], params["run_id"], params["action"]
    )
else:
    ready, missing = check_gate(spark, contracts, catalog, params["domain"], day, params["load_group"])
    dbutils.jobs.taskValues.set(key="ready", value="true" if ready else "false")
    print(f"Completion for {day}: ready={ready}; missing or incomplete={missing}")
    if params["expected_ready"] and ready != (params["expected_ready"] == "true"):
        raise RuntimeError(f"Unexpected daily gate result: {missing}")
    if params["action"] == "require_ready" and not ready:
        raise RuntimeError(f"Late/incomplete sources for {day}: {missing}")

dbutils.jobs.taskValues.set(key="business_date", value=day)
