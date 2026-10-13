"""Conformed Gold facts and dimensions, each sourced directly from Silver."""

from __future__ import annotations

from functools import reduce

from contract_loader import GOLD_EXPECTATIONS
from pipeline_common import deterministic_key, latest_record, spark, table_name
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name=table_name("gold", "dim_customer"), table_properties={"layer": "gold"})
@dp.expect_all_or_fail(GOLD_EXPECTATIONS["dim_customer"])
def gold_dim_customer():
    customers = spark.read.table(table_name("silver", "erp_customers")).alias("erp")
    contacts = latest_record(spark.read.table(table_name("silver", "crm_contacts")), ["erp_customer_id"]).alias("crm")
    return customers.join(
        contacts,
        F.col("erp.customer_id") == F.col("crm.erp_customer_id"),
        "left",
    ).select(
        deterministic_key("customer", "erp.customer_id").alias("customer_key"),
        F.col("erp.customer_id").alias("customer_id"),
        F.col("erp.customer_number").alias("customer_number"),
        F.col("erp.country_code").alias("country_code"),
        F.col("crm.contact_id").alias("crm_contact_id"),
        F.col("crm.email").alias("email"),
        F.col("crm.marketing_consent").alias("marketing_consent"),
        F.col("crm.acquisition_channel").alias("acquisition_channel"),
        F.when(F.col("crm.contact_id").isNotNull(), "CRM_ENRICHED").otherwise("ERP_ONLY").alias("enrichment_status"),
    )


@dp.materialized_view(name=table_name("gold", "dim_product"), table_properties={"layer": "gold"})
@dp.expect_all_or_fail(GOLD_EXPECTATIONS["dim_product"])
def gold_dim_product():
    products = spark.read.table(table_name("silver", "erp_products"))
    return products.select(
        deterministic_key("product", "product_id").alias("product_key"),
        "product_id",
        "product_name",
        "product_family",
        "product_tier",
        "product_format",
        "product_orientation",
        "active",
    )


@dp.materialized_view(name=table_name("gold", "dim_campaign"), table_properties={"layer": "gold"})
@dp.expect_all_or_fail(GOLD_EXPECTATIONS["dim_campaign"])
def gold_dim_campaign():
    campaigns = spark.read.table(table_name("silver", "ads_campaigns"))
    return campaigns.select(
        deterministic_key("campaign", "campaign_code").alias("campaign_key"),
        "campaign_code",
        "campaign_name",
        "platform",
    )


@dp.materialized_view(name=table_name("gold", "fact_sales"), table_properties={"layer": "gold"})
@dp.expect_all_or_fail(GOLD_EXPECTATIONS["fact_sales"])
def gold_fact_sales():
    lines = spark.read.table(table_name("silver", "erp_order_lines")).alias("line")
    orders = spark.read.table(table_name("silver", "erp_orders")).alias("ord")
    return lines.join(orders, F.col("line.order_id") == F.col("ord.order_id"), "inner").select(
        F.col("line.order_line_id").alias("order_line_id"),
        F.col("ord.order_id").alias("order_id"),
        F.col("ord.order_timestamp").alias("order_timestamp"),
        F.to_date(F.col("ord.order_timestamp")).alias("order_date"),
        deterministic_key("customer", "ord.customer_id").alias("customer_key"),
        deterministic_key("product", "line.product_id").alias("product_key"),
        F.col("ord.campaign_code").alias("campaign_code"),
        F.col("line.quantity").alias("quantity"),
        F.col("line.unit_price").alias("unit_price"),
        (F.col("line.quantity") * F.col("line.unit_price")).cast("decimal(18,2)").alias("line_amount"),
        F.col("ord.currency_code").alias("currency_code"),
        F.col("ord.order_status").alias("order_status"),
    )


@dp.materialized_view(name=table_name("gold", "fact_attributed_sales"), table_properties={"layer": "gold"})
@dp.expect_all_or_fail(GOLD_EXPECTATIONS["fact_attributed_sales"])
def gold_fact_attributed_sales():
    lines = spark.read.table(table_name("silver", "erp_order_lines")).alias("line")
    orders = spark.read.table(table_name("silver", "erp_orders")).alias("ord")
    customers = spark.read.table(table_name("silver", "erp_customers")).alias("customer")
    contacts = latest_record(spark.read.table(table_name("silver", "crm_contacts")), ["erp_customer_id"]).alias("crm")
    campaigns = spark.read.table(table_name("silver", "ads_campaigns")).alias("campaign")

    joined = (
        lines.join(orders, F.col("line.order_id") == F.col("ord.order_id"), "inner")
        .join(
            customers,
            F.col("ord.customer_id") == F.col("customer.customer_id"),
            "inner",
        )
        .join(
            contacts,
            F.col("customer.customer_id") == F.col("crm.erp_customer_id"),
            "left",
        )
        .join(
            campaigns,
            F.col("ord.campaign_code") == F.col("campaign.campaign_code"),
            "left",
        )
    )
    crm_matched = F.col("crm.contact_id").isNotNull()
    campaign_matched = F.col("campaign.campaign_code").isNotNull()
    return joined.select(
        F.col("line.order_line_id").alias("order_line_id"),
        F.col("ord.order_id").alias("order_id"),
        F.to_date(F.col("ord.order_timestamp")).alias("order_date"),
        deterministic_key("customer", "customer.customer_id").alias("customer_key"),
        F.when(campaign_matched, deterministic_key("campaign", "campaign.campaign_code"))
        .otherwise(F.lit(None).cast("string"))
        .alias("campaign_key"),
        F.col("ord.campaign_code").alias("campaign_code"),
        F.col("crm.acquisition_channel").alias("acquisition_channel"),
        (F.col("line.quantity") * F.col("line.unit_price")).cast("decimal(18,2)").alias("line_amount"),
        F.when(crm_matched & campaign_matched, "COMPLETE")
        .when(crm_matched, "CRM_ENRICHED")
        .otherwise("ERP_ONLY")
        .alias("completeness_status"),
        F.when(crm_matched, "MATCHED").otherwise("MISSING").alias("crm_match_status"),
        F.when(F.col("ord.campaign_code").isNull(), "NOT_PROVIDED")
        .when(campaign_matched, "MATCHED")
        .otherwise("MISSING")
        .alias("campaign_match_status"),
        F.greatest(
            F.col("line.updated_at"),
            F.col("ord.updated_at"),
            F.col("customer.updated_at"),
            F.col("crm.updated_at"),
            F.col("campaign.updated_at"),
        ).alias("latest_source_timestamp"),
    )


@dp.materialized_view(name=table_name("gold", "fact_ad_spend_daily"), table_properties={"layer": "gold"})
@dp.expect_all_or_fail(GOLD_EXPECTATIONS["fact_ad_spend_daily"])
def gold_fact_ad_spend_daily():
    spend = spark.read.table(table_name("silver", "ads_spend"))
    return spend.select(
        "spend_date",
        "campaign_code",
        deterministic_key("campaign", "campaign_code").alias("campaign_key"),
        "spend_amount",
        "clicks",
        "impressions",
    )


def _readiness(source_object: str, source_timestamp: str):
    source = spark.read.table(table_name("silver", source_object))
    return source.agg(
        F.lit(source_object).alias("source_object"),
        F.max(F.col(source_timestamp)).alias("latest_source_timestamp"),
        F.max(F.col("_ingested_at")).alias("latest_ingestion_timestamp"),
        F.count(F.lit(1)).cast("long").alias("record_count"),
    ).withColumn(
        "readiness_status",
        F.when(F.col("record_count") > 0, "AVAILABLE").otherwise("AWAITING_DATA"),
    )


@dp.materialized_view(name=table_name("gold", "data_readiness"), table_properties={"layer": "gold"})
@dp.expect_all_or_fail(GOLD_EXPECTATIONS["data_readiness"])
def gold_data_readiness():
    frames = [
        _readiness("erp_customers", "updated_at"),
        _readiness("erp_products", "updated_at"),
        _readiness("erp_orders", "updated_at"),
        _readiness("erp_order_lines", "updated_at"),
        _readiness("crm_contacts", "updated_at"),
        _readiness("ads_campaigns", "updated_at"),
        _readiness("ads_spend", "updated_at"),
    ]
    return reduce(lambda left, right: left.unionByName(right), frames)
