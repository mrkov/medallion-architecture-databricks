import runpy
import subprocess
import sys

import pytest

from medallion_poc.contracts import load_yaml
from medallion_poc.execution import physical_name, settings


def test_source_jobs_end_at_silver_and_gold_has_table_change_trigger(repository_root, contracts):
    resource = load_yaml(repository_root / "resources/medallion_poc.yml")["resources"]
    assert set(resource["pipelines"]) == {
        "erp_source_pipeline",
        "crm_source_pipeline",
        "ads_source_pipeline",
        "gold_pipeline",
        "platinum_marketing_pipeline",
    }
    for source in ("erp", "crm", "ads"):
        job = resource["jobs"][f"ingest_{source}"]
        pipeline_tasks = [task for task in job["tasks"] if "pipeline_task" in task]
        assert len(pipeline_tasks) == 1
        assert (
            pipeline_tasks[0]["pipeline_task"]["pipeline_id"]
            == "${resources.pipelines." + source + "_source_pipeline.id}"
        )
        assert resource["pipelines"][source + "_source_pipeline"]["schema"] == "bronze_" + source
    gold = resource["jobs"]["refresh_gold"]
    trigger = gold["trigger"]["table_update"]
    expected = {
        physical_name(contracts.objects[ref], "main")
        for name, obj in contracts.objects.items()
        if name.startswith("gold.")
        for ref in settings(obj)["inputs"].values()
    }
    assert {name.replace("${var.catalog}", "main") for name in trigger["table_names"]} == expected
    assert trigger["condition"] == "ANY_UPDATED"
    assert trigger["wait_after_last_change_seconds"] >= 60
    assert trigger["min_time_between_triggers_seconds"] >= 60
    assert gold["max_concurrent_runs"] == 1
    assert gold["queue"]["enabled"]


@pytest.mark.parametrize("job", ["medallion_poc_demo", "scd2_demo", "key_handling_demo"])
def test_demo_only_ingests_sources_and_observes_results(repository_root, job):
    resource = load_yaml(repository_root / "resources/medallion_poc.yml")["resources"]
    demo = resource["jobs"][job]
    for task in demo["tasks"]:
        assert "pipeline_task" not in task
        if "run_job_task" in task:
            assert "resources.jobs.ingest_" in task["run_job_task"]["job_id"]
    for job_name, definition in resource["jobs"].items():
        for task in definition["tasks"]:
            pipeline = task.get("pipeline_task", {})
            if pipeline.get("full_refresh"):
                assert job_name == "upgrade_poc"
                assert "_source_pipeline" not in pipeline["pipeline_id"]
    setup = {task["task_key"]: task for task in resource["jobs"]["setup_poc"]["tasks"]}
    assert {dep["task_key"] for dep in setup["initialize_gold"]["depends_on"]} == {
        "initialize_erp",
        "initialize_crm",
        "initialize_ads",
    }
    assert setup["initialize_platinum_marketing"]["depends_on"] == [{"task_key": "initialize_gold"}]
    for department in ("sales", "marketing"):
        assert {d["task_key"] for d in setup["publish_" + department]["depends_on"]} == {
            "initialize_gold",
            "initialize_platinum_marketing",
        }


def test_generated_bundle_has_no_contract_drift(repository_root):
    subprocess.run([sys.executable, "scripts/generate_bundle_resources.py", "--check"], cwd=repository_root, check=True)


def test_bootstrap_omits_watcher_without_changing_resource_identities(repository_root, contracts):
    generate = runpy.run_path(str(repository_root / "scripts/generate_bundle_resources.py"))["resources"]
    normal = generate(contracts)["resources"]
    bootstrap = generate(contracts, bootstrap=True)["resources"]
    assert "trigger" not in bootstrap["jobs"]["refresh_gold"]
    for job in normal["jobs"].values():
        job.pop("trigger", None)
    assert bootstrap == normal


def test_receipts_follow_success_and_platinum_watches_only_gold(repository_root):
    resources = load_yaml(repository_root / "resources/medallion_poc.yml")["resources"]
    for source in ("erp", "crm", "ads"):
        tasks = {t["task_key"]: t for t in resources["jobs"]["ingest_" + source]["tasks"]}
        assert tasks["ingest_" + source]["depends_on"] == [{"task_key": "record_started"}]
        assert tasks["record_success"]["depends_on"] == [{"task_key": "ingest_" + source}]
        assert tasks["record_failure"]["run_if"] == "AT_LEAST_ONE_FAILED"
        params = tasks["record_success"]["notebook_task"]["base_parameters"]
        assert params["receipt_business_date"] == "{{tasks.record_started.values.business_date}}"
        assert "business_date" not in params  # Prevent job-parameter pushdown from overriding the resolved day.
    marketing = resources["jobs"]["refresh_platinum_marketing"]
    assert set(marketing["trigger"]["table_update"]["table_names"]) == {
        "${var.catalog}.gold_common.fact_ad_spend_daily",
        "${var.catalog}.gold_common.fact_attributed_sales",
    }
    assert "platinum_sales_pipeline" not in resources["pipelines"]  # Ordinary views have no refresh computation.


def test_daily_policy_gates_the_domain_and_preserves_resource_identity(repository_root, contracts):
    generate = runpy.run_path(str(repository_root / "scripts/generate_bundle_resources.py"))["resources"]
    contracts.orchestration["gold_domains"]["commerce"]["refresh"] = "daily"
    resources = generate(contracts)["resources"]
    job = resources["jobs"]["refresh_gold"]
    assert "trigger" not in job
    tasks = {t["task_key"]: t for t in job["tasks"]}
    assert tasks["refresh_gold"]["depends_on"] == [{"task_key": "all_sources_ready", "outcome": "true"}]
    assert tasks["check_completion"]["notebook_task"]["base_parameters"]["domain"] == "commerce"
    assert "gold_pipeline" in resources["pipelines"]
    demo = resources["jobs"]["daily_gate_demo"]["tasks"]
    params = demo[0]["notebook_task"]["base_parameters"]
    assert params["receipt_business_date"] == contracts.orchestration["daily_demo"]["business_date"]
    assert params["expected_ready"] == "false"
    assert demo[-1]["run_job_task"]["job_id"] == "${resources.jobs.refresh_gold_daily_commerce.id}"


def test_new_gold_domain_has_its_own_pipeline_and_inputs(repository_root, contracts):
    generate = runpy.run_path(str(repository_root / "scripts/generate_bundle_resources.py"))["resources"]
    contracts.objects["gold.fact_ad_spend_daily"]["domain"] = "advertising"
    contracts.orchestration["gold_domains"]["advertising"] = {
        "pipeline_key": "advertising_gold_pipeline",
        "job_key": "refresh_advertising",
        "refresh": "eventual",
        "required_sources": ["ads"],
    }
    resources = generate(contracts)["resources"]
    assert resources["pipelines"]["advertising_gold_pipeline"]["configuration"]["poc.domain"] == "advertising"
    assert resources["jobs"]["refresh_advertising"]["trigger"]["table_update"]["table_names"] == [
        "${var.catalog}.silver_ads.ads_spend"
    ]
