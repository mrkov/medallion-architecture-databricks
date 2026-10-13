"""Immutable contract examples and bounded observation of the independently triggered refresh."""

import csv
import hashlib
import io
import re
import time
from decimal import Decimal

from medallion_poc.execution import physical_name, quote, resolve, settings


def seed_source(dbutils, contracts, source_system, catalog, volume):
    selected = [s for s in contracts.sources.values() if s.source_system == source_system]
    if not selected:
        raise ValueError(f"Unknown source: {source_system}")
    for source in selected:
        obj = contracts.objects[f"source.{source.object_name}"]
        rows = settings(obj)["demo_rows"]
        extras = sorted({key for row in rows for key in row} - set(source.columns))
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=[*source.columns, *extras], lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
        contents = buffer.getvalue()
        filename = hashlib.sha256(contents.encode()).hexdigest()[:16] + ".csv"
        schema = physical_name(obj, catalog).split(".")[1]
        folder = f"/Volumes/{catalog}/{schema}/{volume}/{source.object_name}"
        path = f"{folder}/{filename}"
        if any(entry.path.rstrip("/").endswith("/" + filename) for entry in dbutils.fs.ls(folder)):
            if dbutils.fs.head(path, len(contents.encode()) + 1) != contents:
                raise ValueError(f"Existing demo file differs: {path}")
            continue
        dbutils.fs.put(path, contents, False)


def scenario_query(query, contracts, catalog):
    return re.sub(
        r"\{\{\s*([a-z_]+\.[a-z_]+)\s*\}\}", lambda m: quote(physical_name(resolve(contracts, m[1]), catalog)), query
    )


def wait_for_assertions(spark, checks, contracts, catalog, timeout_seconds=900):
    deadline = time.monotonic() + timeout_seconds
    while True:
        failures = []
        for check in checks:
            # Missing tables, invalid SQL and permission errors fail immediately.
            rows = spark.sql(scenario_query(check["query"], contracts, catalog)).collect()
            actual = rows[0][0] if len(rows) == 1 else None
            expected = check["equals"]
            if isinstance(actual, Decimal):
                expected = Decimal(str(expected))
            if actual != expected:
                failures.append(f"{check['name']}: expected {check['equals']!r}, got {actual!r}")
        if not failures:
            return
        if time.monotonic() >= deadline:
            raise TimeoutError(
                "Gold did not converge. Check the table-update trigger and refresh_gold runs. " + "; ".join(failures)
            )
        print("Waiting for the Silver-triggered Gold refresh: " + "; ".join(failures), flush=True)
        time.sleep(15)
