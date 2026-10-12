"""Consumer-facing Platinum datamarts sourced directly from Gold tables."""

from contract_rules import PLATINUM_EXPECTATIONS
from pipeline_common import spark, table_name
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name=table_name("platinum", "sales_datamart"),
    comment="Materialized sales fact for semantic models; one row per order line.",
    table_properties={"layer": "platinum", "contract_version": "1.0.0"},
)
@dp.expect_all_or_fail(PLATINUM_EXPECTATIONS["sales_datamart"])
def platinum_sales_datamart():
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
    name=table_name("platinum", "marketing_performance"),
    comment="Materialized campaign performance fact at activity date and campaign grain.",
    table_properties={"layer": "platinum", "contract_version": "1.0.0"},
)
@dp.expect_all_or_fail(PLATINUM_EXPECTATIONS["marketing_performance"])
def platinum_marketing_performance():
    attributed = (
        spark.read.table(table_name("gold", "fact_attributed_sales"))
        .filter(F.col("campaign_code").isNotNull())
        .groupBy(F.col("order_date").alias("activity_date"), "campaign_code")
        .agg(
            F.sum(
                F.when(F.col("completeness_status") == "COMPLETE", F.col("line_amount")).otherwise(0)
            ).alias("attributed_revenue"),
            F.sum(
                F.when(F.col("completeness_status") != "COMPLETE", F.col("line_amount")).otherwise(0)
            ).alias("incomplete_revenue"),
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

