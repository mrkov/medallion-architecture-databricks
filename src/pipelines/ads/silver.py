"""Typed, deduplicated ADS Silver tables and source-specific quarantine."""
# ruff: noqa: E402

from __future__ import annotations

# Databricks executes these source files from their own directory.
# Make the shared pipeline modules available without depending on the working directory.
import sys
from pathlib import Path

pipeline_root = str(Path(__file__).resolve().parents[1])
if pipeline_root not in sys.path:
    sys.path.insert(0, pipeline_root)


from functools import reduce

from pipeline_common import table_name
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from silver_common import quarantine_rows, read_bronze, valid_records, violations


def _campaigns() -> DataFrame:
    return (
        read_bronze("ads_campaigns")
        .withColumn("_updated_at_raw", F.col("updated_at"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn(
            "_contract_violations",
            violations(
                ("CAMPAIGN_CODE_REQUIRED", F.col("campaign_code").isNull()),
                ("CAMPAIGN_NAME_REQUIRED", F.col("campaign_name").isNull()),
                ("PLATFORM_REQUIRED", F.col("platform").isNull()),
                ("UPDATED_AT_INVALID", F.col("updated_at").isNull()),
            ),
        )
    )


def _spend() -> DataFrame:
    return (
        read_bronze("ads_spend")
        .withColumn("_spend_date_raw", F.col("spend_date"))
        .withColumn("_spend_amount_raw", F.col("spend_amount"))
        .withColumn("_clicks_raw", F.col("clicks"))
        .withColumn("_impressions_raw", F.col("impressions"))
        .withColumn("_updated_at_raw", F.col("updated_at"))
        .withColumn("spend_date", F.to_date("spend_date"))
        .withColumn("spend_amount", F.col("spend_amount").cast("decimal(18,2)"))
        .withColumn("clicks", F.col("clicks").cast("long"))
        .withColumn("impressions", F.col("impressions").cast("long"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn(
            "_contract_violations",
            violations(
                ("SPEND_DATE_INVALID", F.col("spend_date").isNull()),
                ("CAMPAIGN_CODE_REQUIRED", F.col("campaign_code").isNull()),
                (
                    "SPEND_AMOUNT_INVALID",
                    F.col("spend_amount").isNull() | (F.col("spend_amount") < 0),
                ),
                ("CLICKS_INVALID", F.col("clicks").isNull() | (F.col("clicks") < 0)),
                (
                    "IMPRESSIONS_INVALID",
                    F.col("impressions").isNull() | (F.col("impressions") < 0),
                ),
                ("UPDATED_AT_INVALID", F.col("updated_at").isNull()),
            ),
        )
    )


@dp.materialized_view(name=table_name("silver", "ads_campaigns"), table_properties={"layer": "silver"})
def silver_ads_campaigns():
    return valid_records(_campaigns(), ["campaign_code"]).select(
        "campaign_code",
        "campaign_name",
        "platform",
        "updated_at",
        "_ingested_at",
        "_source_file",
        "_source_system",
        "_batch_id",
    )


@dp.materialized_view(name=table_name("silver", "ads_spend"), table_properties={"layer": "silver"})
def silver_ads_spend():
    return valid_records(_spend(), ["spend_date", "campaign_code"]).select(
        "spend_date",
        "campaign_code",
        "spend_amount",
        "clicks",
        "impressions",
        "updated_at",
        "_ingested_at",
        "_source_file",
        "_source_system",
        "_batch_id",
    )


@dp.materialized_view(
    name=table_name("silver", "ads_contract_quarantine"),
    comment="Advertising records rejected from typed Silver outputs with ODCS-derived violation codes.",
    table_properties={"layer": "silver", "quality": "quarantine"},
)
def silver_ads_contract_quarantine():
    frames = [
        quarantine_rows(_campaigns(), "ads_campaigns", "campaign_code"),
        quarantine_rows(_spend(), "ads_spend", "campaign_code"),
    ]
    return reduce(lambda left, right: left.unionByName(right), frames)
