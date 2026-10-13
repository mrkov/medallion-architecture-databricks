import runpy
import subprocess
import sys

from medallion_poc.contracts import load_yaml
from medallion_poc.execution import physical_name, settings


def test_source_jobs_end_at_silver_and_gold_has_table_change_trigger(repository_root, contracts):
    resource = load_yaml(repository_root / "resources/medallion_poc.yml")["resources"]
    assert set(resource["pipelines"]) == {
        "erp_source_pipeline",
        "crm_source_pipeline",
        "ads_source_pipeline",
        "gold_pipeline",
    }
    for source in ("erp", "crm", "ads"):
        job = resource["jobs"][f"ingest_{source}"]
        assert len(job["tasks"]) == 1
        assert (
            job["tasks"][0]["pipeline_task"]["pipeline_id"]
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


def test_demo_only_ingests_sources_and_observes_results(repository_root):
    resource = load_yaml(repository_root / "resources/medallion_poc.yml")["resources"]
    demo = resource["jobs"]["medallion_poc_demo"]
    for task in demo["tasks"]:
        assert "pipeline_task" not in task
        if "run_job_task" in task:
            assert "resources.jobs.ingest_" in task["run_job_task"]["job_id"]
    assert "full_refresh" not in (repository_root / "resources/medallion_poc.yml").read_text()
    setup = {task["task_key"]: task for task in resource["jobs"]["setup_poc"]["tasks"]}
    assert {dep["task_key"] for dep in setup["initialize_gold"]["depends_on"]} == {
        "initialize_erp",
        "initialize_crm",
        "initialize_ads",
    }
    assert setup["publish_views"]["depends_on"] == [{"task_key": "initialize_gold"}]


def test_generated_bundle_has_no_contract_drift(repository_root):
    subprocess.run([sys.executable, "scripts/generate_bundle_resources.py", "--check"], cwd=repository_root, check=True)


def test_bootstrap_omits_watcher_without_changing_resource_identities(repository_root, contracts):
    generate = runpy.run_path(str(repository_root / "scripts/generate_bundle_resources.py"))["resources"]
    normal = generate(contracts)["resources"]
    bootstrap = generate(contracts, bootstrap=True)["resources"]
    assert "trigger" not in bootstrap["jobs"]["refresh_gold"]
    normal["jobs"]["refresh_gold"].pop("trigger")
    assert bootstrap == normal
