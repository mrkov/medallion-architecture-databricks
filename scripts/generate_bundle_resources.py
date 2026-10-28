"""Generate deployable source jobs and Gold trigger dependencies from ODCS, with CI drift checks."""

import argparse
import copy
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
    domains = contracts.orchestration["gold_domains"]
    departments = contracts.orchestration["departments"]
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

    def run_job(task, key, **params):
        value = {"job_id": "${resources.jobs." + key + ".id}"}
        if params:
            value["job_parameters"] = params
        return {"task_key": task, "run_job_task": value}

    def selected(layer, domain):
        return [
            obj for ref, obj in contracts.objects.items() if ref.startswith(layer + ".") and obj["domain"] == domain
        ]

    def trigger(objects, status):
        refs = sorted({ref for obj in objects for ref in settings(obj)["inputs"].values()})
        return {
            "pause_status": status,
            "table_update": {
                "condition": "ANY_UPDATED",
                "table_names": [bundle_name(contracts.objects[ref]) for ref in refs],
                "wait_after_last_change_seconds": 60,
                "min_time_between_triggers_seconds": 120,
            },
        }

    def add_pipeline(key, title, obj, libraries, domain=None):
        configuration = {f"poc.{key}": value for key, value in parameters.items()}
        configuration["poc.informational_keys"] = "${var.informational_keys}"
        if domain:
            configuration["poc.domain"] = domain
        pipelines[key] = {
            "name": "[${bundle.target}] " + title,
            "catalog": "${var.catalog}",
            "schema": obj["physicalName"].split(".")[1],
            "channel": "${var.pipeline_channel}",
            "serverless": True,
            "continuous": False,
            "photon": True,
            "environment": {"dependencies": ["open-data-contract-standard==3.2.0", "sqlglot==27.29.0"]},
            "libraries": libraries,
            "configuration": configuration,
        }

    pipelines, jobs = {}, {}
    serialized = {"max_concurrent_runs": 1, "queue": {"enabled": True}}
    load_params = [{"name": "business_date", "default": ""}, {"name": "load_group", "default": "standard"}]
    for system in systems:
        source = next(s for s in contracts.sources.values() if s.source_system == system)
        add_pipeline(
            f"{system}_source_pipeline",
            system.upper() + " contract-driven pipeline",
            contracts.objects["source." + source.object_name],
            [{"glob": {"include": f"../src/pipelines/{system}/**"}}],
        )
        receipt = {
            "source": system,
            "receipt_business_date": "{{job.parameters.business_date}}",
            "load_group": "{{job.parameters.load_group}}",
            "run_id": "{{job.run_id}}",
        }
        started = notebook("record_started", "load_control", action="STARTED", **receipt)
        ingestion = after(pipeline_task(f"ingest_{system}", f"{system}_source_pipeline"), "record_started")
        receipt["receipt_business_date"] = "{{tasks.record_started.values.business_date}}"
        success = after(notebook("record_success", "load_control", action="SUCCEEDED", **receipt), f"ingest_{system}")
        failure = after(notebook("record_failure", "load_control", action="FAILED", **receipt), f"ingest_{system}")
        failure["run_if"] = "AT_LEAST_ONE_FAILED"
        jobs[f"ingest_{system}"] = {
            "name": "[${bundle.target}] Ingest " + system.upper() + " through Silver",
            **serialized,
            "parameters": load_params,
            "tasks": [started, ingestion, success, failure],
        }
        if schedule := contracts.orchestration.get("source_schedules", {}).get(system):
            jobs[f"ingest_{system}"]["schedule"] = schedule

    gold_init = []
    for domain, config in domains.items():
        objects = selected("gold", domain)
        key, job = config["pipeline_key"], config["job_key"]
        add_pipeline(
            key, domain + " Gold pipeline", objects[0], [{"file": {"path": "../src/pipelines/gold.py"}}], domain
        )
        tasks = [pipeline_task("refresh_gold", key)]
        resource = {
            "name": "[${bundle.target}] " + domain + " Gold refresh",
            **serialized,
            "parameters": load_params,
            "tasks": tasks,
        }
        if config["refresh"] == "eventual":
            resource["trigger"] = trigger(objects, "${var.gold_trigger_status}")
        else:
            gate = notebook(
                "check_completion",
                "load_control",
                action="require_ready" if config.get("schedule") else "gate",
                domain=domain,
                receipt_business_date="{{job.parameters.business_date}}",
                load_group="{{job.parameters.load_group}}",
            )
            condition = after(
                {
                    "task_key": "all_sources_ready",
                    "condition_task": {
                        "op": "EQUAL_TO",
                        "left": "{{tasks.check_completion.values.ready}}",
                        "right": "true",
                    },
                },
                "check_completion",
            )
            tasks[0]["depends_on"] = [{"task_key": "all_sources_ready", "outcome": "true"}]
            resource["tasks"] = [gate, condition, *tasks]
            if config.get("schedule"):
                resource["schedule"] = config["schedule"]
        if config.get("failure_emails"):
            resource["email_notifications"] = {"on_failure": config["failure_emails"]}
        jobs[job] = resource
        daily_gate = notebook(
            "require_completion",
            "load_control",
            action="require_ready",
            domain=domain,
            receipt_business_date="{{job.parameters.business_date}}",
            load_group="{{job.parameters.load_group}}",
        )
        jobs["refresh_gold_daily_" + domain] = {
            "name": "[${bundle.target}] " + domain + " daily completion gate",
            **serialized,
            "parameters": load_params,
            "tasks": [
                daily_gate,
                after(
                    run_job(
                        "refresh",
                        job,
                        business_date="{{tasks.require_completion.values.business_date}}",
                        load_group="{{job.parameters.load_group}}",
                    ),
                    "require_completion",
                ),
            ],
        }
        if config.get("failure_emails"):
            jobs["refresh_gold_daily_" + domain]["email_notifications"] = {"on_failure": config["failure_emails"]}
        gold_init.append(
            after(
                pipeline_task("initialize_gold" if len(domains) == 1 else "initialize_gold_" + domain, key),
                *["initialize_" + s for s in systems],
            )
        )

    platinum_init = []
    for department in departments:
        objects = selected("platinum", department)
        materialized = [o for o in objects if o["physicalType"] == "materialized view"]
        jobs["publish_platinum_" + department] = {
            "name": "[${bundle.target}] Publish " + department + " views",
            "tasks": [notebook("publish_views", "publish_views", department=department)],
        }
        if materialized:
            key = "platinum_" + department + "_pipeline"
            add_pipeline(
                key,
                department + " Platinum aggregates",
                materialized[0],
                [{"file": {"path": "../src/pipelines/platinum.py"}}],
                department,
            )
            jobs["refresh_platinum_" + department] = {
                "name": "[${bundle.target}] " + department + " Platinum on Gold changes",
                **serialized,
                "trigger": trigger(materialized, "${var.platinum_trigger_status}"),
                "tasks": [pipeline_task("refresh_platinum", key)],
            }
            platinum_init.append(
                after(pipeline_task("initialize_platinum_" + department, key), *[t["task_key"] for t in gold_init])
            )
    publish = [notebook("publish_" + d, "publish_views", department=d) for d in departments]
    jobs["publish_platinum_views"] = {"name": "[${bundle.target}] Publish all Platinum views", "tasks": publish}
    jobs["setup_poc"] = {
        "name": "[${bundle.target}] Initialize contracts, layers and operational receipts",
        **serialized,
        "tasks": [
            notebook("prepare_storage", "setup_poc"),
            *[after(pipeline_task("initialize_" + s, s + "_source_pipeline"), "prepare_storage") for s in systems],
            *gold_init,
            *platinum_init,
            *[after(t, *[x["task_key"] for x in [*gold_init, *platinum_init]]) for t in publish],
        ],
    }
    # The key representation changes from STRING to BINARY. Only derived pipelines
    # are rebuilt during an explicit upgrade; source/Bronze checkpoints are retained.
    upgrade = copy.deepcopy(jobs["setup_poc"])
    upgrade["name"] = "[${bundle.target}] Upgrade derived schemas while retaining Bronze history"
    derived = {config["pipeline_key"] for config in domains.values()} | {
        key for key in pipelines if key.startswith("platinum_")
    }
    for task in upgrade["tasks"]:
        if "pipeline_task" in task and any(
            task["pipeline_task"]["pipeline_id"] == "${resources.pipelines." + key + ".id}" for key in derived
        ):
            task["pipeline_task"]["full_refresh"] = True
    jobs["upgrade_poc"] = upgrade

    for scenario_name, job_name, title in [
        ("scenario", "medallion_poc_demo", "Observe automatic enrichment"),
        ("scd2", "scd2_demo", "Observe SCD2 history"),
        ("keys", "key_handling_demo", "Observe unknown and late dimensions"),
    ]:
        scenario = yaml.safe_load((ROOT / f"demo/{scenario_name}.yaml").read_text())
        tasks, previous = [], None
        for stage, definition in scenario["stages"].items():
            params = {"stage": stage, "scenario": scenario_name}
            seed = notebook("seed_" + stage, "demo_arrivals", action="seed", **params)
            tasks.append(after(seed, previous) if previous else seed)
            tasks.append(after(run_job("ingest_" + stage, "ingest_" + definition["source"]), "seed_" + stage))
            tasks.append(
                after(notebook("observe_" + stage, "demo_arrivals", action="wait", **params), "ingest_" + stage)
            )
            previous = "observe_" + stage
        jobs[job_name] = {"name": "[${bundle.target}] " + title, **serialized, "tasks": tasks}

    demo = contracts.orchestration["daily_demo"]
    group = "daily_demo_{{job.run_id}}"
    demo_params = {"domain": demo["domain"], "receipt_business_date": demo["business_date"], "load_group": group}
    required = domains[demo["domain"]]["required_sources"]
    tasks = [notebook("observe_blocked", "load_control", action="gate", expected_ready="false", **demo_params)]
    for source in required[:-1]:
        tasks.append(
            after(
                run_job("load_" + source, "ingest_" + source, business_date=demo["business_date"], load_group=group),
                "observe_blocked",
            )
        )
    tasks.append(
        after(
            notebook("observe_late_source", "load_control", action="gate", expected_ready="false", **demo_params),
            *["load_" + s for s in required[:-1]],
        )
    )
    tasks.append(
        after(
            run_job(
                "load_" + required[-1], "ingest_" + required[-1], business_date=demo["business_date"], load_group=group
            ),
            "observe_late_source",
        )
    )
    tasks.append(
        after(
            run_job(
                "gated_gold",
                "refresh_gold_daily_" + demo["domain"],
                business_date=demo["business_date"],
                load_group=group,
            ),
            "load_" + required[-1],
        )
    )
    jobs["daily_gate_demo"] = {
        "name": "[${bundle.target}] Daily completion including zero-new-data loads",
        **serialized,
        "tasks": tasks,
    }
    if bootstrap:
        for job in jobs.values():
            job.pop("trigger", None)
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
