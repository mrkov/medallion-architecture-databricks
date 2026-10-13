from pathlib import Path

import yaml


def _resources(repository_root: Path) -> dict:
    path = repository_root / "resources/medallion_poc.yml"
    return yaml.safe_load(path.read_text())["resources"]


def test_pipeline_per_source_and_curated_layer(repository_root):
    pipelines = _resources(repository_root)["pipelines"]
    assert set(pipelines) == {
        "erp_source_pipeline",
        "crm_source_pipeline",
        "ads_source_pipeline",
        "gold_pipeline",
        "platinum_pipeline",
    }
    assert {key: value["configuration"]["poc.pipeline_scope"] for key, value in pipelines.items()} == {
        "erp_source_pipeline": "erp",
        "crm_source_pipeline": "crm",
        "ads_source_pipeline": "ads",
        "gold_pipeline": "gold",
        "platinum_pipeline": "platinum",
    }


def test_each_source_job_refreshes_curated_layers(repository_root):
    jobs = _resources(repository_root)["jobs"]
    for source in ("erp", "crm", "ads"):
        tasks = jobs[f"ingest_{source}"]["tasks"]
        assert tasks[0]["pipeline_task"]["pipeline_id"] == (f"${{resources.pipelines.{source}_source_pipeline.id}}")
        assert tasks[1]["run_job_task"]["job_id"] == "${resources.jobs.curated_refresh.id}"

    curated_tasks = {task["task_key"]: task for task in jobs["curated_refresh"]["tasks"]}
    assert curated_tasks["refresh_gold"]["pipeline_task"]["pipeline_id"] == ("${resources.pipelines.gold_pipeline.id}")
    assert curated_tasks["publish_dimension_views"]["depends_on"] == [{"task_key": "refresh_gold"}]
    assert curated_tasks["refresh_platinum"]["depends_on"] == [{"task_key": "publish_dimension_views"}]
    assert curated_tasks["refresh_platinum"]["pipeline_task"]["pipeline_id"] == (
        "${resources.pipelines.platinum_pipeline.id}"
    )


def test_demo_refreshes_gold_and_platinum_after_each_arrival(repository_root):
    tasks = _resources(repository_root)["jobs"]["medallion_poc_demo"]["tasks"]
    by_key = {task["task_key"]: task for task in tasks}
    for source in ("erp", "crm", "ads"):
        gold = by_key[f"gold_after_{source}"]
        platinum = by_key[f"platinum_after_{source}"]
        if source != "erp":
            assert gold["depends_on"] == [{"task_key": f"process_{source}"}]
        if source == "erp":
            assert by_key["publish_views"]["depends_on"] == [{"task_key": "gold_after_erp"}]
            assert platinum["depends_on"] == [{"task_key": "publish_views"}]
            assert by_key["assert_erp_only"]["depends_on"] == [{"task_key": "platinum_after_erp"}]
        else:
            assert platinum["depends_on"] == [{"task_key": f"gold_after_{source}"}]
