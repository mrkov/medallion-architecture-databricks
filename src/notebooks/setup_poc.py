# Databricks notebook source
# ruff: noqa: E402, F821
# MAGIC %md
# MAGIC # Set up the four-layer POC
# MAGIC Choose the same catalog, schema prefix, and volume name as your bundle, then **Run all**.
# MAGIC This creates missing schemas, the landing volume, and the source folders declared in ODCS.
# MAGIC Existing files and tables are preserved. No sample data is written.
# MAGIC When opened from the deployed bundle, leave `bundle_root` empty to discover it from the notebook path.

# COMMAND ----------
# MAGIC %pip install open-data-contract-standard==3.2.0

# COMMAND ----------
import re
import sys
from pathlib import Path

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema_prefix", "medallion_poc")
dbutils.widgets.text("volume_name", "landing")
dbutils.widgets.text("bundle_root", "")

catalog = dbutils.widgets.get("catalog")
schema_prefix = dbutils.widgets.get("schema_prefix")
volume_name = dbutils.widgets.get("volume_name")
configured_root = dbutils.widgets.get("bundle_root").strip()

for identifier in (catalog, schema_prefix, volume_name):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        raise ValueError(f"Unsafe Unity Catalog identifier: {identifier!r}")


def bundle_root_from_notebook_path(notebook_path: str) -> Path:
    path = Path(notebook_path)
    if path.parent.name != "notebooks" or path.parent.parent.name != "src":
        raise ValueError("Open this notebook under the bundle's src/notebooks folder, or set the bundle_root widget")
    if not path.is_absolute():
        raise ValueError("The notebook context must supply an absolute workspace path")
    if path.parts[1] != "Workspace":
        path = Path("/Workspace") / path.relative_to("/")
    return path.parents[2]


if configured_root:
    bundle_root = Path(configured_root)
else:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    bundle_root = bundle_root_from_notebook_path(notebook_path)
if not bundle_root.is_absolute():
    raise ValueError("bundle_root must be an absolute workspace path")

project_src = str(bundle_root / "src")
if project_src not in sys.path:
    sys.path.insert(0, project_src)

from medallion_poc.contracts import load_contract_catalog

source_interfaces = load_contract_catalog(bundle_root / "contracts").sources

# COMMAND ----------
schemas = [
    f"{schema_prefix}_{suffix}"
    for suffix in ("landing", "bronze", "silver", "gold", "platinum", "platinum_sales", "platinum_marketing")
]
for schema in schemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

landing_schema = f"{schema_prefix}_landing"
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{landing_schema}`.`{volume_name}`")
landing_root = f"/Volumes/{catalog}/{landing_schema}/{volume_name}"
landing_directories = [
    f"{landing_root}/{source.source_system}/{source.object_name}" for source in source_interfaces.values()
]
for directory in landing_directories:
    dbutils.fs.mkdirs(directory)

# COMMAND ----------
print("POC storage setup complete. Existing files and tables were preserved.")
print(f"Schemas: {', '.join(f'{catalog}.{schema}' for schema in schemas)}")
print(f"Landing volume: {catalog}.{landing_schema}.{volume_name}")
print("Upload source CSV files to:")
for directory in landing_directories:
    print(f"  {directory}")

# COMMAND ----------
# MAGIC %md
# MAGIC Storage is ready. For a fresh POC, run the bundle's `initialize_source_interfaces` job once to
# MAGIC create the Bronze/Silver table interfaces before refreshing Gold. The `setup_poc` bundle job
# MAGIC runs this notebook and that initialization together. Individual ingestion jobs run this notebook
# MAGIC before their source pipeline. The staged demo additionally writes sample data and resets its landing folders.
