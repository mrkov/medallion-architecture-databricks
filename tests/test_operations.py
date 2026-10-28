"""Daily gates use completion receipts rather than whether a source table changed."""

from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from medallion_poc.operations import check_gate, readiness, record_load, validate_orchestration
from medallion_poc.workspace import prepare_materializations


def event(source, run, status, minute):
    return dict(
        source_system=source, run_id=run, status=status, event_time=datetime(2026, 10, 1) + timedelta(minutes=minute)
    )


def test_gate_accepts_successful_zero_row_loads_and_rejects_incomplete_attempts():
    rows = [event("erp", "1", "STARTED", 0), event("erp", "1", "SUCCEEDED", 1)]
    assert readiness(rows, ["erp", "crm"]) == (False, {"crm": "NOT_STARTED"})
    rows += [event("crm", "2", "STARTED", 2), event("crm", "2", "SUCCEEDED", 3)]
    # No row-count or table-change condition exists: zero-row successful loads count.
    assert readiness(rows, ["erp", "crm"]) == (True, {})
    rows += [event("erp", "3", "STARTED", 4)]
    assert readiness(rows, ["erp", "crm"]) == (False, {"erp": "STARTED"})
    rows += [event("erp", "3", "FAILED", 5), event("erp", "1", "SUCCEEDED", 6)]
    assert readiness(rows, ["erp", "crm"]) == (False, {"erp": "FAILED"})
    rows += [event("erp", "4", "STARTED", 7), event("erp", "4", "SUCCEEDED", 8)]
    assert readiness(rows, ["erp", "crm"]) == (True, {})


def test_receipts_without_a_start_do_not_satisfy_gate():
    assert readiness([event("erp", "orphan", "SUCCEEDED", 0)], ["erp"])[0] is False


def test_gate_is_scoped_to_business_date_and_load_group(spark, contracts, monkeypatch):
    from medallion_poc import operations

    data = [
        ("erp", "2026-10-01", "standard", "1", "STARTED", datetime(2026, 10, 1)),
        ("erp", "2026-10-01", "standard", "1", "SUCCEEDED", datetime(2026, 10, 1, 1)),
        ("crm", "2026-09-30", "standard", "2", "STARTED", datetime(2026, 9, 30)),
        ("crm", "2026-09-30", "standard", "2", "SUCCEEDED", datetime(2026, 9, 30, 1)),
        ("ads", "2026-10-01", "different", "3", "STARTED", datetime(2026, 10, 1)),
        ("ads", "2026-10-01", "different", "3", "SUCCEEDED", datetime(2026, 10, 1, 1)),
    ]
    spark.createDataFrame(
        data,
        "source_system STRING, business_date STRING, load_group STRING, run_id STRING, "
        "status STRING, event_time TIMESTAMP",
    ).createOrReplaceTempView("gate_receipts")
    monkeypatch.setattr(operations, "control_table", lambda *_: "gate_receipts")
    assert check_gate(spark, contracts, "main", "commerce", "2026-10-01", "standard") == (
        False,
        {"crm": "NOT_STARTED", "ads": "NOT_STARTED"},
    )


def test_record_load_rejects_invalid_markers_before_writing(contracts):
    with pytest.raises(ValueError, match="Unknown source"):
        record_load(None, contracts, "main", "typo", "2026-10-01", "standard", "1", "SUCCEEDED")
    with pytest.raises(ValueError):
        record_load(None, contracts, "main", "erp", "2026-10-01", "", "1", "SUCCEEDED")


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        ("refresh", "daliy", "refresh policy"),
        ("required_sources", ["erp", "crm"], "omits a Gold input"),
        ("required_sources", ["erp", "crm", "typo"], "required_sources"),
        ("job_key", "ingest_erp", "valid and unique"),
    ],
)
def test_invalid_orchestration_fails_before_deployment(contracts, field, value, error):
    contracts.orchestration["gold_domains"]["commerce"][field] = value
    with pytest.raises(ValueError, match=error):
        validate_orchestration(contracts)


def test_load_control_notebook_retains_resolved_day_and_requires_completion(repository_root, monkeypatch):
    import sys

    from medallion_poc import operations

    monkeypatch.setattr(sys, "path", sys.path.copy())
    params = {
        "catalog": "main",
        "volume_name": "landing",
        "bundle_root": str(repository_root),
        "action": "SUCCEEDED",
        "source": "erp",
        "receipt_business_date": "2026-10-01",
        # Job-level business_date can be blank on overnight runs; receipt date comes from STARTED.
        "business_date": "",
        "load_group": "standard",
        "run_id": "123",
        "domain": "commerce",
        "expected_ready": "",
    }
    values, receipts = {}, []
    monkeypatch.setattr(operations, "record_load", lambda *args: receipts.append(args))
    monkeypatch.setattr(operations, "check_gate", lambda *args: (False, {"ads": "NOT_STARTED"}))
    namespace = {
        "spark": object(),
        "dbutils": SimpleNamespace(
            widgets=SimpleNamespace(
                text=lambda name, default: params.setdefault(name, default), get=params.__getitem__
            ),
            jobs=SimpleNamespace(taskValues=SimpleNamespace(set=lambda key, value: values.update({key: value}))),
        ),
    }
    path = repository_root / "src/notebooks/load_control.py"
    exec(compile(path.read_text(), str(path), "exec"), namespace)
    assert receipts[0][3:] == ("erp", "2026-10-01", "standard", "123", "SUCCEEDED")
    assert values == {"business_date": "2026-10-01"}
    params["action"] = "require_ready"
    with pytest.raises(RuntimeError, match="Late/incomplete sources for 2026-10-01"):
        exec(compile(path.read_text(), str(path), "exec"), namespace)
    assert values["ready"] == "false"


def test_upgrade_replaces_only_the_declared_ordinary_view(contracts):
    statements = []

    def sql(query):
        statements.append(query)
        return SimpleNamespace(collect=lambda: [("VIEW",)])

    prepare_materializations(SimpleNamespace(sql=sql), contracts, "main")
    drops = [q for q in statements if q.startswith("DROP")]
    assert drops == ["DROP VIEW `main`.`platinum_marketing`.`fact_campaign_performance`"]
    statements.clear()
    prepare_materializations(
        SimpleNamespace(
            sql=lambda q: statements.append(q) or SimpleNamespace(collect=lambda: [("MATERIALIZED_VIEW",)])
        ),
        contracts,
        "main",
    )
    assert not any(q.startswith("DROP") for q in statements)
