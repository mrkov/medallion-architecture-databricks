"""Typed, deduplicated, source-oriented Silver materialized views."""

from __future__ import annotations

from functools import reduce

from contract_rules import SILVER_PRODUCT_EXPECTATIONS
from pipeline_common import SOURCE_COLUMNS, latest_record, spark, table_name
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _violations(*rules):
    return F.filter(F.array(*[F.when(condition, F.lit(code)) for code, condition in rules]), lambda x: x.isNotNull())


def _bronze(object_name: str) -> DataFrame:
    columns = SOURCE_COLUMNS[object_name]
    return spark.read.table(table_name("bronze", object_name)).withColumn(
        "_raw_payload", F.to_json(F.struct(*(F.col(column) for column in columns)))
    )


def _customers() -> DataFrame:
    return (
        _bronze("erp_customers")
        .withColumn("_created_at_raw", F.col("created_at"))
        .withColumn("_updated_at_raw", F.col("updated_at"))
        .withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn(
            "_contract_violations",
            _violations(
                ("CUSTOMER_ID_REQUIRED", F.col("customer_id").isNull()),
                ("CUSTOMER_NUMBER_REQUIRED", F.col("customer_number").isNull()),
                ("COUNTRY_CODE_REQUIRED", F.col("country_code").isNull()),
                ("CREATED_AT_INVALID", F.col("created_at").isNull()),
                ("UPDATED_AT_INVALID", F.col("updated_at").isNull()),
            ),
        )
    )


def _products() -> DataFrame:
    base = _bronze("erp_products").withColumnRenamed(
        "product_hierarchy", "product_hierarchy_raw"
    )
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
            _violations(
                ("PRODUCT_ID_REQUIRED", F.col("product_id").isNull()),
                ("PRODUCT_NAME_REQUIRED", F.col("product_name").isNull()),
                (
                    "HIERARCHY_SEGMENT_COUNT",
                    F.col("product_hierarchy_raw").isNull() | (F.size(parts) != 4),
                ),
                (
                    "HIERARCHY_EMPTY_SEGMENT",
                    (F.size(parts) == 4)
                    & F.exists(parts, lambda segment: F.length(F.trim(segment)) == 0),
                ),
                ("ACTIVE_FLAG_INVALID", F.col("active").isNull()),
                ("UPDATED_AT_INVALID", F.col("updated_at").isNull()),
            ),
        )
    )


def _orders() -> DataFrame:
    return (
        _bronze("erp_orders")
        .withColumn("_order_timestamp_raw", F.col("order_timestamp"))
        .withColumn("_updated_at_raw", F.col("updated_at"))
        .withColumn("order_timestamp", F.to_timestamp("order_timestamp"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn(
            "_contract_violations",
            _violations(
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
        _bronze("erp_order_lines")
        .withColumn("_quantity_raw", F.col("quantity"))
        .withColumn("_unit_price_raw", F.col("unit_price"))
        .withColumn("_updated_at_raw", F.col("updated_at"))
        .withColumn("quantity", F.col("quantity").cast("integer"))
        .withColumn("unit_price", F.col("unit_price").cast("decimal(18,2)"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn(
            "_contract_violations",
            _violations(
                ("ORDER_LINE_ID_REQUIRED", F.col("order_line_id").isNull()),
                ("ORDER_ID_REQUIRED", F.col("order_id").isNull()),
                ("PRODUCT_ID_REQUIRED", F.col("product_id").isNull()),
                ("QUANTITY_INVALID", F.col("quantity").isNull() | (F.col("quantity") <= 0)),
                ("UNIT_PRICE_INVALID", F.col("unit_price").isNull() | (F.col("unit_price") < 0)),
                ("UPDATED_AT_INVALID", F.col("updated_at").isNull()),
            ),
        )
    )


def _contacts() -> DataFrame:
    base = _bronze("crm_contacts").withColumn(
        "_marketing_consent_raw", F.lower(F.trim(F.col("marketing_consent")))
    )
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
            _violations(
                ("CONTACT_ID_REQUIRED", F.col("contact_id").isNull()),
                ("ERP_CUSTOMER_ID_REQUIRED", F.col("erp_customer_id").isNull()),
                ("EMAIL_REQUIRED", F.col("email").isNull()),
                ("MARKETING_CONSENT_INVALID", F.col("marketing_consent").isNull()),
                ("ACQUISITION_CHANNEL_REQUIRED", F.col("acquisition_channel").isNull()),
                ("UPDATED_AT_INVALID", F.col("updated_at").isNull()),
            ),
        )
    )


def _campaigns() -> DataFrame:
    return (
        _bronze("ads_campaigns")
        .withColumn("_updated_at_raw", F.col("updated_at"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn(
            "_contract_violations",
            _violations(
                ("CAMPAIGN_CODE_REQUIRED", F.col("campaign_code").isNull()),
                ("CAMPAIGN_NAME_REQUIRED", F.col("campaign_name").isNull()),
                ("PLATFORM_REQUIRED", F.col("platform").isNull()),
                ("UPDATED_AT_INVALID", F.col("updated_at").isNull()),
            ),
        )
    )


def _spend() -> DataFrame:
    return (
        _bronze("ads_spend")
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
            _violations(
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


def _valid(df: DataFrame, keys: list[str]) -> DataFrame:
    return latest_record(df.filter(F.size("_contract_violations") == 0), keys).drop(
        "_contract_violations", "_raw_payload"
    )


@dp.materialized_view(name=table_name("silver", "erp_customers"), table_properties={"layer": "silver"})
def silver_erp_customers():
    return _valid(_customers(), ["customer_id"]).select(
        "customer_id", "customer_number", "country_code", "created_at", "updated_at",
        "_ingested_at", "_source_file", "_source_system", "_batch_id"
    )


@dp.materialized_view(name=table_name("silver", "erp_products"), table_properties={"layer": "silver"})
@dp.expect_all_or_fail(SILVER_PRODUCT_EXPECTATIONS)
def silver_erp_products():
    return _valid(_products(), ["product_id"]).select(
        "product_id", "product_name", "product_hierarchy_raw", "product_family", "product_tier",
        "product_format", "product_orientation", "active", "updated_at", "_ingested_at",
        "_source_file", "_source_system", "_batch_id"
    )


@dp.materialized_view(name=table_name("silver", "erp_orders"), table_properties={"layer": "silver"})
def silver_erp_orders():
    return _valid(_orders(), ["order_id"]).select(
        "order_id", "customer_id", "order_timestamp", "order_status", "currency_code",
        "campaign_code", "updated_at", "_ingested_at", "_source_file", "_source_system", "_batch_id"
    )


@dp.materialized_view(name=table_name("silver", "erp_order_lines"), table_properties={"layer": "silver"})
def silver_erp_order_lines():
    return _valid(_order_lines(), ["order_line_id"]).select(
        "order_line_id", "order_id", "product_id", "quantity", "unit_price", "updated_at",
        "_ingested_at", "_source_file", "_source_system", "_batch_id"
    )


@dp.materialized_view(name=table_name("silver", "crm_contacts"), table_properties={"layer": "silver"})
def silver_crm_contacts():
    return _valid(_contacts(), ["contact_id"]).select(
        "contact_id", "erp_customer_id", "email", "marketing_consent", "acquisition_channel",
        "updated_at", "_ingested_at", "_source_file", "_source_system", "_batch_id"
    )


@dp.materialized_view(name=table_name("silver", "ads_campaigns"), table_properties={"layer": "silver"})
def silver_ads_campaigns():
    return _valid(_campaigns(), ["campaign_code"]).select(
        "campaign_code", "campaign_name", "platform", "updated_at", "_ingested_at",
        "_source_file", "_source_system", "_batch_id"
    )


@dp.materialized_view(name=table_name("silver", "ads_spend"), table_properties={"layer": "silver"})
def silver_ads_spend():
    return _valid(_spend(), ["spend_date", "campaign_code"]).select(
        "spend_date", "campaign_code", "spend_amount", "clicks", "impressions", "updated_at",
        "_ingested_at", "_source_file", "_source_system", "_batch_id"
    )


def _quarantine_rows(df: DataFrame, source_object: str, key_column: str) -> DataFrame:
    return df.filter(F.size("_contract_violations") > 0).select(
        F.lit(source_object).alias("source_object"),
        F.col(key_column).cast("string").alias("business_key"),
        F.col("_raw_payload").alias("raw_payload"),
        F.col("_contract_violations").alias("violation_codes"),
        "_ingested_at",
        "_source_file",
        "_batch_id",
    )


@dp.materialized_view(
    name=table_name("silver", "contract_quarantine"),
    comment="Records rejected from typed Silver outputs with ODCS-derived violation codes.",
    table_properties={"layer": "silver", "quality": "quarantine"},
)
def silver_contract_quarantine():
    frames = [
        _quarantine_rows(_customers(), "erp_customers", "customer_id"),
        _quarantine_rows(_products(), "erp_products", "product_id"),
        _quarantine_rows(_orders(), "erp_orders", "order_id"),
        _quarantine_rows(_order_lines(), "erp_order_lines", "order_line_id"),
        _quarantine_rows(_contacts(), "crm_contacts", "contact_id"),
        _quarantine_rows(_campaigns(), "ads_campaigns", "campaign_code"),
        _quarantine_rows(
            _spend(), "ads_spend", "campaign_code"
        ),
    ]
    return reduce(lambda left, right: left.unionByName(right), frames)

