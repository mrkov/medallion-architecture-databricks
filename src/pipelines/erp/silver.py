"""Typed, deduplicated ERP Silver tables and source-specific quarantine."""
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


from functools import reduce

from contract_loader import SILVER_PRODUCT_EXPECTATIONS
from pipeline_common import table_name
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from silver_common import quarantine_rows, read_bronze, valid_records, violations


def _customers() -> DataFrame:
    return (
        read_bronze("erp_customers")
        .withColumn("_created_at_raw", F.col("created_at"))
        .withColumn("_updated_at_raw", F.col("updated_at"))
        .withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn(
            "_contract_violations",
            violations(
                ("CUSTOMER_ID_REQUIRED", F.col("customer_id").isNull()),
                ("CUSTOMER_NUMBER_REQUIRED", F.col("customer_number").isNull()),
                ("COUNTRY_CODE_REQUIRED", F.col("country_code").isNull()),
                ("CREATED_AT_INVALID", F.col("created_at").isNull()),
                ("UPDATED_AT_INVALID", F.col("updated_at").isNull()),
            ),
        )
    )


def _products() -> DataFrame:
    base = read_bronze("erp_products").withColumnRenamed("product_hierarchy", "product_hierarchy_raw")
    parts = F.split(F.trim(F.col("product_hierarchy_raw")), r"\|", -1)
    return (
        base.withColumn("_active_flag_raw", F.lower(F.trim(F.col("active_flag"))))
        .withColumn("_updated_at_raw", F.col("updated_at"))
        .withColumn("hierarchy_segment_count", F.size(parts))
        .withColumn("product_family", F.trim(F.element_at(parts, 1)))
        .withColumn("product_tier", F.trim(F.element_at(parts, 2)))
        .withColumn("product_format", F.trim(F.element_at(parts, 3)))
        .withColumn("product_orientation", F.trim(F.element_at(parts, 4)))
        .withColumn(
            "active",
            F.when(F.col("_active_flag_raw").isin("1", "true", "y", "yes"), True)
            .when(F.col("_active_flag_raw").isin("0", "false", "n", "no"), False)
            .otherwise(F.lit(None).cast("boolean")),
        )
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn(
            "_contract_violations",
            violations(
                ("PRODUCT_ID_REQUIRED", F.col("product_id").isNull()),
                ("PRODUCT_NAME_REQUIRED", F.col("product_name").isNull()),
                (
                    "HIERARCHY_SEGMENT_COUNT",
                    F.col("product_hierarchy_raw").isNull() | (F.size(parts) != 4),
                ),
                (
                    "HIERARCHY_EMPTY_SEGMENT",
                    (F.size(parts) == 4) & F.exists(parts, lambda segment: F.length(F.trim(segment)) == 0),
                ),
                ("ACTIVE_FLAG_INVALID", F.col("active").isNull()),
                ("UPDATED_AT_INVALID", F.col("updated_at").isNull()),
            ),
        )
    )


def _orders() -> DataFrame:
    return (
        read_bronze("erp_orders")
        .withColumn("_order_timestamp_raw", F.col("order_timestamp"))
        .withColumn("_updated_at_raw", F.col("updated_at"))
        .withColumn("order_timestamp", F.to_timestamp("order_timestamp"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn(
            "_contract_violations",
            violations(
                ("ORDER_ID_REQUIRED", F.col("order_id").isNull()),
                ("CUSTOMER_ID_REQUIRED", F.col("customer_id").isNull()),
                ("ORDER_TIMESTAMP_INVALID", F.col("order_timestamp").isNull()),
                ("ORDER_STATUS_REQUIRED", F.col("order_status").isNull()),
                ("CURRENCY_CODE_REQUIRED", F.col("currency_code").isNull()),
                ("UPDATED_AT_INVALID", F.col("updated_at").isNull()),
            ),
        )
    )


def _order_lines() -> DataFrame:
    return (
        read_bronze("erp_order_lines")
        .withColumn("_quantity_raw", F.col("quantity"))
        .withColumn("_unit_price_raw", F.col("unit_price"))
        .withColumn("_updated_at_raw", F.col("updated_at"))
        .withColumn("quantity", F.col("quantity").cast("integer"))
        .withColumn("unit_price", F.col("unit_price").cast("decimal(18,2)"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn(
            "_contract_violations",
            violations(
                ("ORDER_LINE_ID_REQUIRED", F.col("order_line_id").isNull()),
                ("ORDER_ID_REQUIRED", F.col("order_id").isNull()),
                ("PRODUCT_ID_REQUIRED", F.col("product_id").isNull()),
                ("QUANTITY_INVALID", F.col("quantity").isNull() | (F.col("quantity") <= 0)),
                ("UNIT_PRICE_INVALID", F.col("unit_price").isNull() | (F.col("unit_price") < 0)),
                ("UPDATED_AT_INVALID", F.col("updated_at").isNull()),
            ),
        )
    )


@dp.materialized_view(name=table_name("silver", "erp_customers"), table_properties={"layer": "silver"})
def silver_erp_customers():
    return valid_records(_customers(), ["customer_id"]).select(
        "customer_id",
        "customer_number",
        "country_code",
        "created_at",
        "updated_at",
        "_ingested_at",
        "_source_file",
        "_source_system",
        "_batch_id",
    )


@dp.materialized_view(name=table_name("silver", "erp_products"), table_properties={"layer": "silver"})
@dp.expect_all_or_fail(SILVER_PRODUCT_EXPECTATIONS)
def silver_erp_products():
    return valid_records(_products(), ["product_id"]).select(
        "product_id",
        "product_name",
        "product_hierarchy_raw",
        "product_family",
        "product_tier",
        "product_format",
        "product_orientation",
        "active",
        "updated_at",
        "_ingested_at",
        "_source_file",
        "_source_system",
        "_batch_id",
    )


@dp.materialized_view(name=table_name("silver", "erp_orders"), table_properties={"layer": "silver"})
def silver_erp_orders():
    return valid_records(_orders(), ["order_id"]).select(
        "order_id",
        "customer_id",
        "order_timestamp",
        "order_status",
        "currency_code",
        "campaign_code",
        "updated_at",
        "_ingested_at",
        "_source_file",
        "_source_system",
        "_batch_id",
    )


@dp.materialized_view(name=table_name("silver", "erp_order_lines"), table_properties={"layer": "silver"})
def silver_erp_order_lines():
    return valid_records(_order_lines(), ["order_line_id"]).select(
        "order_line_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "updated_at",
        "_ingested_at",
        "_source_file",
        "_source_system",
        "_batch_id",
    )


@dp.materialized_view(
    name=table_name("silver", "erp_contract_quarantine"),
    comment="ERP records rejected from typed Silver outputs with ODCS-derived violation codes.",
    table_properties={"layer": "silver", "quality": "quarantine"},
)
def silver_erp_contract_quarantine():
    frames = [
        quarantine_rows(_customers(), "erp_customers", "customer_id"),
        quarantine_rows(_products(), "erp_products", "product_id"),
        quarantine_rows(_orders(), "erp_orders", "order_id"),
        quarantine_rows(_order_lines(), "erp_order_lines", "order_line_id"),
    ]
    return reduce(lambda left, right: left.unionByName(right), frames)
