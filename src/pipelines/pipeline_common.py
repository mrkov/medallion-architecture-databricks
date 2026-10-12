"""Shared names, source schemas, and helpers for the Lakeflow pipeline."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.getActiveSession()
if spark is None:
    raise RuntimeError("An active Spark session is required")

CATALOG = spark.conf.get("poc.catalog")
SCHEMA_PREFIX = spark.conf.get("poc.schema_prefix")
VOLUME_NAME = spark.conf.get("poc.volume_name")
PIPELINE_SCOPE = spark.conf.get("poc.pipeline_scope")
LANDING_ROOT = f"/Volumes/{CATALOG}/{SCHEMA_PREFIX}_landing/{VOLUME_NAME}"

VALID_PIPELINE_SCOPES = {"erp", "crm", "ads", "gold", "platinum"}
if PIPELINE_SCOPE not in VALID_PIPELINE_SCOPES:
    raise ValueError(
        f"Unsupported poc.pipeline_scope {PIPELINE_SCOPE!r}; expected one of {sorted(VALID_PIPELINE_SCOPES)}"
    )

SOURCE_COLUMNS: dict[str, tuple[str, ...]] = {
    "erp_customers": (
        "customer_id",
        "customer_number",
        "country_code",
        "created_at",
        "updated_at",
    ),
    "erp_products": (
        "product_id",
        "product_name",
        "product_hierarchy",
        "active_flag",
        "updated_at",
    ),
    "erp_orders": (
        "order_id",
        "customer_id",
        "order_timestamp",
        "order_status",
        "currency_code",
        "campaign_code",
        "updated_at",
    ),
    "erp_order_lines": (
        "order_line_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "updated_at",
    ),
    "crm_contacts": (
        "contact_id",
        "erp_customer_id",
        "email",
        "marketing_consent",
        "acquisition_channel",
        "updated_at",
    ),
    "ads_campaigns": ("campaign_code", "campaign_name", "platform", "updated_at"),
    "ads_spend": (
        "spend_date",
        "campaign_code",
        "spend_amount",
        "clicks",
        "impressions",
        "updated_at",
    ),
}

SOURCE_SYSTEM: dict[str, str] = {
    "erp_customers": "erp",
    "erp_products": "erp",
    "erp_orders": "erp",
    "erp_order_lines": "erp",
    "crm_contacts": "crm",
    "ads_campaigns": "ads",
    "ads_spend": "ads",
}


def table_name(layer: str, name: str) -> str:
    return f"{CATALOG}.{SCHEMA_PREFIX}_{layer}.{name}"


def scoped(scope: str, decorator):
    """Register a dataset only in the pipeline that owns the requested scope."""
    if PIPELINE_SCOPE == scope:
        return decorator
    return lambda function: function


def read_landing(object_name: str) -> DataFrame:
    columns = SOURCE_COLUMNS[object_name]
    source_system = SOURCE_SYSTEM[object_name]
    schema = ", ".join(f"{column} STRING" for column in columns)
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .load(f"{LANDING_ROOT}/{source_system}/{object_name}")
        .select(
            *(F.col(column).cast("string").alias(column) for column in columns),
            F.col("_rescued_data").cast("string").alias("_rescued_data"),
            F.col("_metadata.file_path").cast("string").alias("_source_file"),
        )
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_system", F.lit(source_system))
        .withColumn(
            "_batch_id",
            F.regexp_extract(F.col("_source_file"), r"([^/]+)\.csv$", 1),
        )
    )


def latest_record(df: DataFrame, keys: list[str], sequence_column: str = "updated_at") -> DataFrame:
    window = Window.partitionBy(*keys).orderBy(
        F.col(sequence_column).desc_nulls_last(),
        F.col("_ingested_at").desc(),
        F.col("_source_file").desc(),
    )
    return (
        df.withColumn("_row_number", F.row_number().over(window)).filter(F.col("_row_number") == 1).drop("_row_number")
    )


def deterministic_key(namespace: str, column: str):
    return F.sha2(F.concat_ws("|", F.lit(namespace), F.col(column)), 256)
