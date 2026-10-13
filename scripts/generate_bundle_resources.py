"""Generate deployable source jobs and Gold trigger dependencies from ODCS, with CI drift checks."""

import argparse
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from medallion_poc.contracts import load_contract_catalog  # noqa: E402
from medallion_poc.execution import settings  # noqa: E402


def bundle_name(obj):
    return obj["physicalName"].replace("<catalog>", "${var.catalog}")


def resources(contracts, bootstrap=False):
    systems = sorted({source.source_system for source in contracts.sources.values()})
    parameters = {
        "bundle_root": "${workspace.file_path}",
        "catalog": "${var.catalog}",
        "volume_name": "${var.volume_name}",
    }

    def notebook(task, file, **params):
        return {
            "task_key": task,
            "notebook_task": {
                "notebook_path": f"../src/notebooks/{file}.py",
                "base_parameters": {**parameters, **params},
            },
        }

    def pipeline_task(task, key):
        return {"task_key": task, "pipeline_task": {"pipeline_id": "${resources.pipelines." + key + ".id}"}}

    def after(task, *dependencies):
        return {**task, "depends_on": [{"task_key": dep} for dep in dependencies]}

    pipelines = {}
    for owner in [*systems, "gold"]:
        key = "gold_pipeline" if owner == "gold" else f"{owner}_source_pipeline"
        target = (
            next(obj for ref, obj in contracts.objects.items() if ref.startswith("gold."))
            if owner == "gold"
            else (
                contracts.objects[
                    "source." + next(s.object_name for s in contracts.sources.values() if s.source_system == owner)
                ]
            )
        )
        pipelines[key] = {
            "name": f"[${{bundle.target}}] {owner.upper()} contract-driven pipeline",
            "catalog": "${var.catalog}",
            "schema": target["physicalName"].split(".")[1],
            "channel": "${var.pipeline_channel}",
            "serverless": True,
            "continuous": False,
            "photon": True,
            "environment": {"dependencies": ["open-data-contract-standard==3.2.0", "sqlglot==27.29.0"]},
            "libraries": (
                [{"file": {"path": "../src/pipelines/gold.py"}}]
                if owner == "gold"
                else [{"glob": {"include": f"../src/pipelines/{owner}/**"}}]
            ),
            "configuration": {f"poc.{key}": value for key, value in parameters.items()},
        }

    inputs = sorted(
        {
            reference
            for name, obj in contracts.objects.items()
            if name.startswith("gold.")
            for reference in settings(obj)["inputs"].values()
        }
    )
    jobs = {
        "refresh_gold": {
            "name": "[${bundle.target}] Gold on Silver changes",
            "max_concurrent_runs": 1,
            "queue": {"enabled": True},
            "trigger": {
                "pause_status": "${var.gold_trigger_status}",
                "table_update": {
                    "condition": "ANY_UPDATED",
                    "table_names": [bundle_name(contracts.objects[reference]) for reference in inputs],
                    "wait_after_last_change_seconds": 60,
                    "min_time_between_triggers_seconds": 120,
                },
            },
            "tasks": [pipeline_task("refresh_gold", "gold_pipeline")],
        },
        "setup_poc": {
            "name": "[${bundle.target}] Set up contracts, source interfaces and Platinum views",
            "max_concurrent_runs": 1,
            "tasks": [
                notebook("prepare_storage", "setup_poc"),
                *[
                    after(pipeline_task(f"initialize_{system}", f"{system}_source_pipeline"), "prepare_storage")
                    for system in systems
                ],
                after(pipeline_task("initialize_gold", "gold_pipeline"), *[f"initialize_{s}" for s in systems]),
                after(notebook("publish_views", "publish_views"), "initialize_gold"),
            ],
        },
        "publish_platinum_views": {
            "name": "[${bundle.target}] Publish contract-defined Platinum views",
            "tasks": [notebook("publish_views", "publish_views")],
        },
    }
    for system in systems:
        jobs[f"ingest_{system}"] = {
            "name": f"[${{bundle.target}}] Ingest {system.upper()} through Silver",
            "max_concurrent_runs": 1,
            "queue": {"enabled": True},
            "tasks": [pipeline_task(f"ingest_{system}", f"{system}_source_pipeline")],
        }
    # The demo observes Gold; it never starts the Gold pipeline or refresh job.
    scenario = yaml.safe_load((ROOT / "demo/scenario.yaml").read_text())
    tasks = []
    previous = None
    for stage, definition in scenario["stages"].items():
        seed = notebook(f"seed_{stage}", "demo_arrivals", action="seed", stage=stage)
        tasks.append(after(seed, previous) if previous else seed)
        tasks.append(
            after(
                {
                    "task_key": f"ingest_{stage}",
                    "run_job_task": {"job_id": "${resources.jobs.ingest_" + definition["source"] + ".id}"},
                },
                f"seed_{stage}",
            )
        )
        tasks.append(
            after(notebook(f"observe_{stage}", "demo_arrivals", action="wait", stage=stage), f"ingest_{stage}")
        )
        previous = f"observe_{stage}"
    jobs["medallion_poc_demo"] = {
        "name": "[${bundle.target}] Observe automatic Gold enrichment after staged arrivals",
        "max_concurrent_runs": 1,
        "tasks": tasks,
    }
    if bootstrap:
        # The Jobs API need not accept watchers on tables that do not exist yet, even when paused.
        # Keep the same job ID; a subsequent normal deployment adds its trigger after setup.
        jobs["refresh_gold"].pop("trigger")
    return {"resources": {"pipelines": pipelines, "jobs": jobs}}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--bootstrap", action="store_true", help="Omit table watchers for the first deployment")
    args = parser.parse_args()
    data = resources(load_contract_catalog(ROOT / "contracts"), bootstrap=args.bootstrap)
    text = "# Generated from ODCS by scripts/generate_bundle_resources.py; do not hand-edit.\n"
    text += yaml.safe_dump(data, sort_keys=False, width=110)
    path = ROOT / "resources/medallion_poc.yml"
    if args.check:
        if path.read_text() != text:
            raise SystemExit("Bundle resources differ from ODCS. Run scripts/generate_bundle_resources.py")
    else:
        path.write_text(text)


if __name__ == "__main__":
    main()
