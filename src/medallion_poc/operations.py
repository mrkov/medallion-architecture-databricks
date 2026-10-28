"""Operational load receipts and a business-date completion gate, independent of data-layer lineage."""

from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

from medallion_poc.execution import IDENTIFIER, quote, settings


def validate_orchestration(contracts):
    config = contracts.orchestration
    allowed = {
        "control_schema",
        "business_timezone",
        "gold_domains",
        "departments",
        "daily_demo",
        "load_event_columns",
        "source_schedules",
    }
    if not isinstance(config, dict) or set(config) - allowed or allowed - {"source_schedules"} - set(config):
        raise ValueError("Orchestration needs control storage, timezone, domains, departments and a daily demo")
    if not IDENTIFIER.fullmatch(config["control_schema"]) or not config["control_schema"].startswith("bronze_"):
        raise ValueError("Operational receipt schema must be a bronze_ identifier")
    ZoneInfo(config["business_timezone"])
    columns = config["load_event_columns"]
    expected = {
        "source_system": "STRING",
        "business_date": "DATE",
        "load_group": "STRING",
        "run_id": "STRING",
        "status": "STRING",
        "event_time": "TIMESTAMP",
    }
    if len(columns) != len(expected) or {p["name"]: p["physicalType"] for p in columns} != expected:
        raise ValueError("load_event_columns must declare the operational receipt interface")
    systems = {s.source_system for s in contracts.sources.values()}
    if not set(config.get("source_schedules", {})) <= systems:
        raise ValueError("Schedules must refer to declared sources")
    for layer, field in (("gold", "gold_domains"), ("platinum", "departments")):
        declared = {obj["domain"] for ref, obj in contracts.objects.items() if ref.startswith(layer + ".")}
        if set(config[field]) != declared or any(not IDENTIFIER.fullmatch(name) for name in declared):
            raise ValueError(f"{field} must match the {layer} contract domains")
    pipeline_names = [s + "_source_pipeline" for s in systems]
    pipeline_names += ["platinum_" + d + "_pipeline" for d in config["departments"]]
    job_names = ["ingest_" + s for s in systems]
    job_names += [
        "setup_poc",
        "upgrade_poc",
        "publish_platinum_views",
        "medallion_poc_demo",
        "scd2_demo",
        "key_handling_demo",
        "daily_gate_demo",
    ]
    for department in config["departments"]:
        if config["departments"][department] != {}:
            raise ValueError("Department settings are currently empty mappings")
        job_names += ["publish_platinum_" + department, "refresh_platinum_" + department]
    for domain, policy in config["gold_domains"].items():
        if set(policy) - {"pipeline_key", "job_key", "refresh", "required_sources", "schedule", "failure_emails"}:
            raise ValueError(f"Unknown Gold policy setting: {domain}")
        if policy["refresh"] not in {"eventual", "daily"}:
            raise ValueError(f"Invalid Gold refresh policy: {domain}")
        if policy["refresh"] == "eventual" and policy.get("schedule"):
            raise ValueError(f"Use daily policy for a scheduled Gold refresh: {domain}")
        required = policy["required_sources"]
        if not required or len(required) != len(set(required)) or not set(required) <= systems:
            raise ValueError(f"Invalid required_sources: {domain}")
        used = {
            contracts.sources[raw.removeprefix("bronze.")].source_system
            for ref, obj in contracts.objects.items()
            if ref.startswith("gold.") and obj["domain"] == domain
            for silver in settings(obj)["inputs"].values()
            for raw in settings(contracts.objects[silver])["inputs"].values()
        }
        if not used <= set(required):
            raise ValueError(f"required_sources omits a Gold input source: {domain}")
        pipeline_names.append(policy["pipeline_key"])
        job_names += [policy["job_key"], "refresh_gold_daily_" + domain]
    for names in (pipeline_names, job_names):
        if len(names) != len(set(names)) or any(not IDENTIFIER.fullmatch(n) for n in names):
            raise ValueError("Orchestration resource keys must be valid and unique")
    demo = config["daily_demo"]
    if set(demo) != {"domain", "business_date"} or demo["domain"] not in config["gold_domains"]:
        raise ValueError("daily_demo must identify a declared Gold domain and business date")
    if len(config["gold_domains"][demo["domain"]]["required_sources"]) < 2:
        raise ValueError("The staged daily demo needs at least two sources")
    date.fromisoformat(demo["business_date"])


def event_schema(contracts):
    return ", ".join(f"{quote(p['name'])} {p['physicalType']}" for p in contracts.orchestration["load_event_columns"])


def control_table(contracts, catalog):
    return quote(f"{catalog}.{contracts.orchestration['control_schema']}.load_events")


def prepare_control(spark, contracts, catalog):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quote(catalog + '.' + contracts.orchestration['control_schema'])}")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {control_table(contracts, catalog)} ({event_schema(contracts)}) USING DELTA "
        "TBLPROPERTIES ('delta.appendOnly' = 'true')"
    )


def record_load(spark, contracts, catalog, source, business_date, load_group, run_id, status):
    if source not in {s.source_system for s in contracts.sources.values()}:
        raise ValueError(f"Unknown source: {source}")
    if status not in {"STARTED", "SUCCEEDED", "FAILED"} or not run_id or not load_group:
        raise ValueError("Load receipt needs a status, run ID and load group")
    day = date.fromisoformat(business_date)
    record = dict(
        source_system=source,
        business_date=day,
        load_group=load_group,
        run_id=run_id,
        status=status,
        event_time=datetime.now(UTC),
    )
    values = [tuple(record[p["name"]] for p in contracts.orchestration["load_event_columns"])]
    spark.createDataFrame(values, event_schema(contracts)).write.mode("append").saveAsTable(
        control_table(contracts, catalog)
    )


def readiness(events, required_sources):
    """The latest *started* attempt must succeed; a late old success cannot mask a new failure."""
    attempts = {}
    for row in events:
        key = (row["source_system"], row["run_id"])
        attempts.setdefault(key, []).append(row)
    latest = {}
    for (source, run_id), rows in attempts.items():
        starts = [r["event_time"] for r in rows if r["status"] == "STARTED"]
        if not starts:
            continue
        started = max(starts)
        last = max(rows, key=lambda r: (r["event_time"], {"STARTED": 0, "SUCCEEDED": 1, "FAILED": 2}[r["status"]]))
        candidate = (started, run_id, last["status"])
        if source not in latest or candidate[:2] > latest[source][:2]:
            latest[source] = candidate
    missing = {
        source: latest[source][2] if source in latest else "NOT_STARTED"
        for source in required_sources
        if source not in latest or latest[source][2] != "SUCCEEDED"
    }
    return not missing, missing


def check_gate(spark, contracts, catalog, domain, business_date, load_group):
    from pyspark.sql import functions as F

    day = date.fromisoformat(business_date)
    rows = (
        spark.table(control_table(contracts, catalog))
        .filter((F.col("business_date") == F.lit(day)) & (F.col("load_group") == F.lit(load_group)))
        .collect()
    )
    required = contracts.orchestration["gold_domains"][domain]["required_sources"]
    return readiness([r.asDict() for r in rows], required)
