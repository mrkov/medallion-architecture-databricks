"""Append-only, source-oriented Bronze streaming tables."""

from contract_rules import SOURCE_EXPECTATIONS
from pipeline_common import read_landing, table_name
from pyspark import pipelines as dp


@dp.table(
    name=table_name("bronze", "erp_customers"),
    comment="Exact ERP customer payload as strings plus ingestion metadata.",
    table_properties={"layer": "bronze", "source_system": "erp"},
)
@dp.expect_all(SOURCE_EXPECTATIONS["erp_customers"])
def bronze_erp_customers():
    return read_landing("erp_customers")


@dp.table(
    name=table_name("bronze", "erp_products"),
    comment="Exact ERP product payload as strings plus ingestion metadata.",
    table_properties={"layer": "bronze", "source_system": "erp"},
)
@dp.expect_all(SOURCE_EXPECTATIONS["erp_products"])
def bronze_erp_products():
    return read_landing("erp_products")


@dp.table(
    name=table_name("bronze", "erp_orders"),
    comment="Exact ERP order payload as strings plus ingestion metadata.",
    table_properties={"layer": "bronze", "source_system": "erp"},
)
@dp.expect_all(SOURCE_EXPECTATIONS["erp_orders"])
def bronze_erp_orders():
    return read_landing("erp_orders")


@dp.table(
    name=table_name("bronze", "erp_order_lines"),
    comment="Exact ERP order-line payload as strings plus ingestion metadata.",
    table_properties={"layer": "bronze", "source_system": "erp"},
)
@dp.expect_all(SOURCE_EXPECTATIONS["erp_order_lines"])
def bronze_erp_order_lines():
    return read_landing("erp_order_lines")


@dp.table(
    name=table_name("bronze", "crm_contacts"),
    comment="Exact CRM contact payload as strings plus ingestion metadata.",
    table_properties={"layer": "bronze", "source_system": "crm"},
)
@dp.expect_all(SOURCE_EXPECTATIONS["crm_contacts"])
def bronze_crm_contacts():
    return read_landing("crm_contacts")


@dp.table(
    name=table_name("bronze", "ads_campaigns"),
    comment="Exact advertising campaign payload as strings plus ingestion metadata.",
    table_properties={"layer": "bronze", "source_system": "ads"},
)
@dp.expect_all(SOURCE_EXPECTATIONS["ads_campaigns"])
def bronze_ads_campaigns():
    return read_landing("ads_campaigns")


@dp.table(
    name=table_name("bronze", "ads_spend"),
    comment="Exact advertising spend payload as strings plus ingestion metadata.",
    table_properties={"layer": "bronze", "source_system": "ads"},
)
@dp.expect_all(SOURCE_EXPECTATIONS["ads_spend"])
def bronze_ads_spend():
    return read_landing("ads_spend")

