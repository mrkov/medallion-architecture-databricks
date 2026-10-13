"""Shared Bronze reads, validation, deduplication, and quarantine helpers for Silver."""

from __future__ import annotations

from pipeline_common import SOURCE_COLUMNS, latest_record, spark, table_name
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def read_bronze(object_name: str) -> DataFrame:
    columns = SOURCE_COLUMNS[object_name]
    return spark.read.table(table_name("bronze", object_name)).withColumn(
        "_raw_payload", F.to_json(F.struct(*(F.col(column) for column in columns)))
    )


def violations(*rules):
    return F.filter(F.array(*[F.when(condition, F.lit(code)) for code, condition in rules]), lambda x: x.isNotNull())


def valid_records(df: DataFrame, keys: list[str]) -> DataFrame:
    return latest_record(df.filter(F.size("_contract_violations") == 0), keys).drop(
        "_contract_violations", "_raw_payload"
    )


def quarantine_rows(df: DataFrame, source_object: str, key_column: str) -> DataFrame:
    return df.filter(F.size("_contract_violations") > 0).select(
        F.lit(source_object).alias("source_object"),
        F.col(key_column).cast("string").alias("business_key"),
        F.col("_raw_payload").alias("raw_payload"),
        F.col("_contract_violations").alias("violation_codes"),
        "_ingested_at",
        "_source_file",
        "_batch_id",
    )
