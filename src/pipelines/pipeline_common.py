"""Shared names, source schemas, and helpers for the Lakeflow pipeline."""

from __future__ import annotations

from contract_rules import SOURCE_COLUMNS, SOURCE_SCHEMAS, SOURCE_SYSTEM
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.getActiveSession()
if spark is None:
    raise RuntimeError("An active Spark session is required")

CATALOG = spark.conf.get("poc.catalog")
SCHEMA_PREFIX = spark.conf.get("poc.schema_prefix")
VOLUME_NAME = spark.conf.get("poc.volume_name")
LANDING_ROOT = f"/Volumes/{CATALOG}/{SCHEMA_PREFIX}_landing/{VOLUME_NAME}"


def table_name(layer: str, name: str) -> str:
    return f"{CATALOG}.{SCHEMA_PREFIX}_{layer}.{name}"


def read_landing(object_name: str) -> DataFrame:
    columns = SOURCE_COLUMNS[object_name]
    source_system = SOURCE_SYSTEM[object_name]
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(SOURCE_SCHEMAS[object_name])
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
