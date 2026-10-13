"""Typed, deduplicated CRM Silver tables and source-specific quarantine."""
# ruff: noqa: E402

from __future__ import annotations

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

from pipeline_common import table_name
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from silver_common import quarantine_rows, read_bronze, valid_records, violations


def _contacts() -> DataFrame:
    base = read_bronze("crm_contacts").withColumn("_marketing_consent_raw", F.lower(F.trim(F.col("marketing_consent"))))
    return (
        base.withColumn("_updated_at_raw", F.col("updated_at"))
        .withColumn(
            "marketing_consent",
            F.when(F.col("_marketing_consent_raw").isin("1", "true", "y", "yes"), True)
            .when(F.col("_marketing_consent_raw").isin("0", "false", "n", "no"), False)
            .otherwise(F.lit(None).cast("boolean")),
        )
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn(
            "_contract_violations",
            violations(
                ("CONTACT_ID_REQUIRED", F.col("contact_id").isNull()),
                ("ERP_CUSTOMER_ID_REQUIRED", F.col("erp_customer_id").isNull()),
                ("EMAIL_REQUIRED", F.col("email").isNull()),
                ("MARKETING_CONSENT_INVALID", F.col("marketing_consent").isNull()),
                ("ACQUISITION_CHANNEL_REQUIRED", F.col("acquisition_channel").isNull()),
                ("UPDATED_AT_INVALID", F.col("updated_at").isNull()),
            ),
        )
    )


@dp.materialized_view(name=table_name("silver", "crm_contacts"), table_properties={"layer": "silver"})
def silver_crm_contacts():
    return valid_records(_contacts(), ["contact_id"]).select(
        "contact_id",
        "erp_customer_id",
        "email",
        "marketing_consent",
        "acquisition_channel",
        "updated_at",
        "_ingested_at",
        "_source_file",
        "_source_system",
        "_batch_id",
    )


@dp.materialized_view(
    name=table_name("silver", "crm_contract_quarantine"),
    comment="CRM records rejected from typed Silver outputs with ODCS-derived violation codes.",
    table_properties={"layer": "silver", "quality": "quarantine"},
)
def silver_crm_contract_quarantine():
    return quarantine_rows(_contacts(), "crm_contacts", "contact_id")
