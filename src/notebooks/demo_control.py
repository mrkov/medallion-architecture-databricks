# Databricks notebook source
# ruff: noqa: F821
"""Seed staged source arrivals, publish virtual dimensions, and assert demo outcomes."""

import re

dbutils.widgets.text("action", "seed_erp")
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema_prefix", "medallion_poc")
dbutils.widgets.text("volume_name", "landing")

action = dbutils.widgets.get("action")
catalog = dbutils.widgets.get("catalog")
schema_prefix = dbutils.widgets.get("schema_prefix")
volume_name = dbutils.widgets.get("volume_name")

for identifier in (catalog, schema_prefix, volume_name):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        raise ValueError(f"Unsafe Unity Catalog identifier: {identifier!r}")

landing_schema = f"{schema_prefix}_landing"
landing_root = f"/Volumes/{catalog}/{landing_schema}/{volume_name}"
layer_schema = {layer: f"{schema_prefix}_{layer}" for layer in ("bronze", "silver", "gold", "platinum")}

source_directories = (
    "erp/erp_customers",
    "erp/erp_products",
    "erp/erp_orders",
    "erp/erp_order_lines",
    "crm/crm_contacts",
    "ads/ads_campaigns",
    "ads/ads_spend",
)


def setup_storage(reset: bool) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{landing_schema}`")
    for schema in layer_schema.values():
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{landing_schema}`.`{volume_name}`")
    if reset:
        for directory in source_directories:
            dbutils.fs.rm(f"{landing_root}/{directory}", True)
    for directory in source_directories:
        dbutils.fs.mkdirs(f"{landing_root}/{directory}")


def put_csv(relative_path: str, contents: str) -> None:
    dbutils.fs.put(f"{landing_root}/{relative_path}", contents.strip() + "\n", True)


def scalar(query: str):
    return spark.sql(query).first()[0]


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def require_unique(qualified_table: str, columns: list[str]) -> None:
    keys = ", ".join(f"`{column}`" for column in columns)
    duplicates = scalar(
        f"SELECT count(*) FROM (SELECT {keys}, count(*) AS row_count "
        f"FROM {qualified_table} GROUP BY {keys} HAVING count(*) > 1)"
    )
    require(duplicates == 0, f"Contract grain violation in {qualified_table}: {columns}")


if action == "seed_erp":
    setup_storage(reset=True)
    put_csv(
        "erp/erp_customers/erp_001.csv",
        """
customer_id,customer_number,country_code,created_at,updated_at
C100,CUST-100,CH,2026-10-01T09:00:00Z,2026-10-01T09:00:00Z
""",
    )
    put_csv(
        "erp/erp_products/erp_001.csv",
        """
product_id,product_name,product_hierarchy,active_flag,updated_at
P100,Premium Photo Book,PHOTOBOOK|PREMIUM|A4|LANDSCAPE,true,2026-10-01T09:00:00Z
P_BAD,Broken Hierarchy,PHOTOBOOK|PREMIUM|A4,true,2026-10-01T09:00:00Z
""",
    )
    put_csv(
        "erp/erp_orders/erp_001.csv",
        """
order_id,customer_id,order_timestamp,order_status,currency_code,campaign_code,updated_at
O100,C100,2026-10-01T10:00:00Z,CONFIRMED,CHF,META_FALL_2026,2026-10-01T10:00:00Z
""",
    )
    put_csv(
        "erp/erp_order_lines/erp_001.csv",
        """
order_line_id,order_id,product_id,quantity,unit_price,updated_at
OL100,O100,P100,2,29.90,2026-10-01T10:00:00Z
""",
    )
elif action == "seed_crm":
    setup_storage(reset=False)
    put_csv(
        "crm/crm_contacts/crm_001.csv",
        """
contact_id,erp_customer_id,email,marketing_consent,acquisition_channel,updated_at
CRM100,C100,customer100@example.test,true,Paid Social,2026-10-01T11:00:00Z
""",
    )
elif action == "seed_ads":
    setup_storage(reset=False)
    put_csv(
        "ads/ads_campaigns/ads_001.csv",
        """
campaign_code,campaign_name,platform,updated_at,placement_type
META_FALL_2026,Fall Photo Books 2026,Meta,2026-10-01T12:00:00Z,Feed
""",
    )
    put_csv(
        "ads/ads_spend/ads_001.csv",
        """
spend_date,campaign_code,spend_amount,clicks,impressions,updated_at
2026-10-01,META_FALL_2026,20.00,80,4000,2026-10-01T12:00:00Z
""",
    )
elif action == "publish_views":
    platinum = layer_schema["platinum"]
    gold = layer_schema["gold"]
    spark.sql(
        f"CREATE OR REPLACE VIEW `{catalog}`.`{platinum}`.`dim_customer_vw` AS "
        f"SELECT * FROM `{catalog}`.`{gold}`.`dim_customer`"
    )
    spark.sql(
        f"CREATE OR REPLACE VIEW `{catalog}`.`{platinum}`.`dim_product_vw` AS "
        f"SELECT * FROM `{catalog}`.`{gold}`.`dim_product`"
    )
    spark.sql(
        f"CREATE OR REPLACE VIEW `{catalog}`.`{platinum}`.`dim_campaign_vw` AS "
        f"SELECT * FROM `{catalog}`.`{gold}`.`dim_campaign`"
    )
elif action in {"assert_erp_only", "assert_crm_enriched", "assert_complete"}:
    gold = layer_schema["gold"]
    silver = layer_schema["silver"]
    platinum = layer_schema["platinum"]
    expected_status = {
        "assert_erp_only": "ERP_ONLY",
        "assert_crm_enriched": "CRM_ENRICHED",
        "assert_complete": "COMPLETE",
    }[action]
    actual_status = scalar(
        f"SELECT completeness_status FROM `{catalog}`.`{gold}`.`fact_attributed_sales` WHERE order_line_id = 'OL100'"
    )
    require(actual_status == expected_status, f"Expected {expected_status}, got {actual_status}")
    require_unique(f"`{catalog}`.`{gold}`.`fact_sales`", ["order_line_id"])
    require_unique(f"`{catalog}`.`{gold}`.`fact_attributed_sales`", ["order_line_id"])
    require_unique(f"`{catalog}`.`{gold}`.`dim_customer`", ["customer_key"])
    require_unique(f"`{catalog}`.`{gold}`.`dim_product`", ["product_key"])
    require(
        scalar(
            f"SELECT count(*) FROM `{catalog}`.`{silver}`.`erp_contract_quarantine` "
            "WHERE business_key = 'P_BAD' AND array_contains(violation_codes, 'HIERARCHY_SEGMENT_COUNT')"
        )
        == 1,
        "Malformed product hierarchy was not quarantined exactly once",
    )
    require(
        scalar(
            f"SELECT count(*) FROM `{catalog}`.`{platinum}`.`sales_datamart` "
            "WHERE order_line_id = 'OL100' AND product_family = 'PHOTOBOOK' "
            "AND product_tier = 'PREMIUM'"
        )
        == 1,
        "Silver hierarchy-derived columns did not reach the sales datamart",
    )
    if action == "assert_complete":
        performance = spark.sql(
            f"SELECT attributed_revenue, incomplete_revenue, spend_amount, return_on_ad_spend "
            f"FROM `{catalog}`.`{platinum}`.`marketing_performance` "
            "WHERE activity_date = DATE '2026-10-01' AND campaign_code = 'META_FALL_2026'"
        ).first()
        require(performance is not None, "Marketing performance row was not published")
        require(float(performance.attributed_revenue) == 59.80, "Attributed revenue did not converge")
        require(float(performance.incomplete_revenue) == 0.0, "Revenue remained incomplete")
        require(float(performance.spend_amount) == 20.0, "Advertising spend did not arrive")
        require(float(performance.return_on_ad_spend) == 2.99, "ROAS was not calculated")
        require(
            scalar(
                f"SELECT count(*) FROM `{catalog}`.`{layer_schema['bronze']}`.`ads_campaigns` "
                "WHERE _rescued_data LIKE '%placement_type%'"
            )
            == 1,
            "Unexpected advertising field was not retained in the Bronze rescue column",
        )
else:
    raise ValueError(f"Unsupported action: {action}")

print(f"Completed demo action: {action}")
