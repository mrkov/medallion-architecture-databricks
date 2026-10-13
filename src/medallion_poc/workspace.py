"""Shared storage and view publication; all business interfaces come from ODCS."""

from medallion_poc.execution import IDENTIFIER, physical_name, quote, render_query, resolve


def prepare_storage(spark, dbutils, contracts, catalog, volume):
    for value in (catalog, volume):
        if not IDENTIFIER.fullmatch(value):
            raise ValueError(f"Invalid catalog or volume identifier: {value!r}")
    schemas = {physical_name(obj, catalog).rsplit(".", 1)[0] for obj in contracts.objects.values()}
    for schema in sorted(schemas):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quote(schema)}")
    source_schemas = {
        physical_name(contracts.objects[f"source.{source.object_name}"], catalog).split(".")[1]
        for source in contracts.sources.values()
    }
    for schema in sorted(source_schemas):
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {quote(f'{catalog}.{schema}.{volume}')}")
    directories = [
        f"/Volumes/{catalog}/{physical_name(contracts.objects[f'source.{s.object_name}'], catalog).split('.')[1]}"
        f"/{volume}/{s.object_name}"
        for s in contracts.sources.values()
    ]
    for path in directories:
        dbutils.fs.mkdirs(path)
    return directories


def publish_views(spark, contracts, catalog):
    def sql_name(reference):
        return quote(physical_name(resolve(contracts, reference), catalog))

    for reference, obj in contracts.objects.items():
        if not reference.startswith("platinum."):
            continue
        target = quote(physical_name(obj, catalog))
        spark.sql(f"CREATE OR REPLACE VIEW {target} AS {render_query(obj, sql_name)}")
