"""Materialized facts and reports in domain-specific Platinum data mart schemas."""

from contract_loader import PLATINUM_EXPECTATIONS
from pipeline_common import spark, table_name
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name=table_name("platinum_sales", "fact_sales"),
    comment="Sales mart fact with customer and product keys; one row per order line.",
    table_properties={"layer": "platinum", "data_mart": "sales", "contract_version": "2.0.0"},
)
@dp.expect_all_or_fail(PLATINUM_EXPECTATIONS["fact_sales"])
def platinum_sales_fact():
    return spark.read.table(table_name("gold", "fact_sales")).select(
        "order_line_id",
        "order_id",
        "order_timestamp",
        "order_date",
        "customer_key",
        "product_key",
        "campaign_code",
        "quantity",
        "unit_price",
        "line_amount",
        "currency_code",
        "order_status",
    )


@dp.materialized_view(
    name=table_name("platinum_sales", "sales_detail"),
    comment="Flattened sales report in the Sales mart; one row per order line.",
    table_properties={"layer": "platinum", "data_mart": "sales", "contract_version": "2.0.0"},
)
@dp.expect_all_or_fail(PLATINUM_EXPECTATIONS["sales_detail"])
def platinum_sales_detail():
    sales = spark.read.table(table_name("gold", "fact_sales")).alias("sales")
    customers = spark.read.table(table_name("gold", "dim_customer")).alias("customer")
    products = spark.read.table(table_name("gold", "dim_product")).alias("product")
    return (
        sales.join(customers, F.col("sales.customer_key") == F.col("customer.customer_key"), "inner")
        .join(products, F.col("sales.product_key") == F.col("product.product_key"), "inner")
        .select(
            F.col("sales.order_line_id").alias("order_line_id"),
            F.col("sales.order_id").alias("order_id"),
            F.col("sales.order_date").alias("order_date"),
            F.col("customer.customer_number").alias("customer_number"),
            F.col("customer.country_code").alias("country_code"),
            F.col("product.product_name").alias("product_name"),
            F.col("product.product_family").alias("product_family"),
            F.col("product.product_tier").alias("product_tier"),
            F.col("sales.quantity").alias("quantity"),
            F.col("sales.line_amount").alias("line_amount"),
            F.col("sales.currency_code").alias("currency_code"),
        )
    )


@dp.materialized_view(
    name=table_name("platinum_marketing", "fact_campaign_performance"),
    comment="Materialized campaign performance fact at activity date and campaign grain.",
    table_properties={"layer": "platinum", "data_mart": "marketing", "contract_version": "2.0.0"},
)
@dp.expect_all_or_fail(PLATINUM_EXPECTATIONS["fact_campaign_performance"])
def platinum_campaign_performance_fact():
    attributed = (
        spark.read.table(table_name("gold", "fact_attributed_sales"))
        .filter(F.col("campaign_code").isNotNull())
        .groupBy(F.col("order_date").alias("activity_date"), "campaign_code")
        .agg(
            F.sum(F.when(F.col("completeness_status") == "COMPLETE", F.col("line_amount")).otherwise(0)).alias(
                "attributed_revenue"
            ),
            F.sum(F.when(F.col("completeness_status") != "COMPLETE", F.col("line_amount")).otherwise(0)).alias(
                "incomplete_revenue"
            ),
        )
        .alias("revenue")
    )
    spend = (
        spark.read.table(table_name("gold", "fact_ad_spend_daily"))
        .select(F.col("spend_date").alias("activity_date"), "campaign_code", "spend_amount", "clicks", "impressions")
        .alias("spend")
    )
    joined = attributed.join(spend, ["activity_date", "campaign_code"], "full")
    attributed_revenue = F.coalesce(F.col("attributed_revenue"), F.lit(0)).cast("decimal(18,2)")
    incomplete_revenue = F.coalesce(F.col("incomplete_revenue"), F.lit(0)).cast("decimal(18,2)")
    spend_amount = F.coalesce(F.col("spend_amount"), F.lit(0)).cast("decimal(18,2)")
    return joined.select(
        "activity_date",
        "campaign_code",
        attributed_revenue.alias("attributed_revenue"),
        incomplete_revenue.alias("incomplete_revenue"),
        spend_amount.alias("spend_amount"),
        F.coalesce(F.col("clicks"), F.lit(0)).cast("long").alias("clicks"),
        F.coalesce(F.col("impressions"), F.lit(0)).cast("long").alias("impressions"),
        F.when(spend_amount > 0, (attributed_revenue / spend_amount).cast("decimal(18,4)"))
        .otherwise(F.lit(None).cast("decimal(18,4)"))
        .alias("return_on_ad_spend"),
    )
