"""Generic Lakeflow registration and append-only ingestion of contracted source interfaces."""

from functools import reduce

from contract_loader import CONTRACTS, source_definition
from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from medallion_poc.execution import (
    accepted_silver,
    evaluate_silver,
    keys,
    physical_name,
    quote,
    render_query,
    resolve,
    rules,
)

spark = SparkSession.getActiveSession()
CATALOG = spark.conf.get("poc.catalog")
VOLUME_NAME = spark.conf.get("poc.volume_name")


def name_of(reference):
    return physical_name(resolve(CONTRACTS, reference), CATALOG)


def sql_name(reference):
    return quote(name_of(reference))


def read_landing(object_name):
    source = source_definition(object_name)
    schema = name_of(f"bronze.{object_name}").split(".")[1]
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(source.spark_schema)
        .load(f"/Volumes/{CATALOG}/{schema}/{VOLUME_NAME}/{object_name}")
        .select(
            *(F.col(column).cast("string").alias(column) for column in source.columns),
            F.col("_rescued_data").cast("string").alias("_rescued_data"),
            F.col("_metadata.file_path").cast("string").alias("_source_file"),
        )
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_system", F.lit(source.source_system))
        .withColumn("_batch_id", F.regexp_extract(F.col("_source_file"), r"([^/]+)\.csv$", 1))
    )


def register_bronze(system):
    for source in CONTRACTS.sources.values():
        if source.source_system != system:
            continue

        def dataset(object_name=source.object_name):
            return read_landing(object_name)

        dataset = dp.expect_all(CONTRACTS.expectations["source"][source.object_name])(dataset)
        dp.table(
            name=name_of(f"bronze.{source.object_name}"),
            table_properties={"layer": "bronze", "delta.appendOnly": "true"},
        )(dataset)


def register_silver(system):
    objects = [
        CONTRACTS.objects[f"silver.{s.object_name}"] for s in CONTRACTS.sources.values() if s.source_system == system
    ]
    for obj in objects:

        def dataset(definition=obj):
            return accepted_silver(evaluate_silver(spark, definition, sql_name), definition)

        dataset = dp.expect_all_or_fail(rules(obj))(dataset)
        dp.materialized_view(name=physical_name(obj, CATALOG), table_properties={"layer": "silver"})(dataset)

    def quarantine():
        frames = []
        for obj in objects:
            invalid = evaluate_silver(spark, obj, sql_name).filter(F.size("_contract_violations") > 0)
            frames.append(
                invalid.select(
                    F.lit(obj["name"]).alias("source_object"),
                    F.to_json(F.struct(*[F.col(key) for key in keys(obj)])).alias("business_key"),
                    F.col("_raw_payload").alias("raw_payload"),
                    F.col("_contract_violations").alias("violation_codes"),
                    "_ingested_at",
                    "_source_file",
                    "_batch_id",
                )
            )
        return reduce(lambda left, right: left.unionByName(right), frames)

    schema = physical_name(objects[0], CATALOG).rsplit(".", 1)[0]
    dp.materialized_view(
        name=f"{schema}.contract_quarantine", table_properties={"layer": "silver", "quality": "quarantine"}
    )(quarantine)


def register_gold():
    for reference, obj in CONTRACTS.objects.items():
        if not reference.startswith("gold."):
            continue

        def dataset(definition=obj):
            return spark.sql(render_query(definition, sql_name))

        dataset = dp.expect_all_or_fail(rules(obj))(dataset)
        dp.materialized_view(
            name=physical_name(obj, CATALOG),
            table_properties={"layer": "gold", "contract_version": obj["contract_version"]},
        )(dataset)
