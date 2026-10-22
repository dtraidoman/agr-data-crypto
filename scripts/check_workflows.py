from __future__ import annotations

import pathlib
import re
import sys

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]

EXPECTED = {
    "ci.yml": {"push", "pull_request", "workflow_dispatch"},
    "collect-a.yml": {"schedule", "workflow_dispatch"},
    "collect-b.yml": {"schedule", "workflow_dispatch"},
    "watchdog.yml": {"schedule", "workflow_dispatch"},
    "backfill.yml": {"workflow_dispatch"},
    "keepalive.yml": {"schedule", "workflow_dispatch"},
}

CRON_RE = re.compile(r"^[\d*/,\- ]+$")

problems: list[str] = []
for name, expected_triggers in EXPECTED.items():
    path = ROOT / ".github" / "workflows" / name
    if not path.exists():
        problems.append(f"{name}: missing")
        continue
    # PyYAML follows YAML 1.1 and turns the bare key `on` into True.
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    triggers = data.get("on", data.get(True))
    if not isinstance(triggers, dict):
        problems.append(f"{name}: triggers are not a mapping: {triggers!r}")
        continue
    missing = expected_triggers - set(triggers)
    if missing:
        problems.append(f"{name}: missing triggers {sorted(missing)}")

    jobs = data.get("jobs")
    if not isinstance(jobs, dict) or not jobs:
        problems.append(f"{name}: no jobs")
        continue
    for job_name, job in jobs.items():
        if "runs-on" not in job:
            problems.append(f"{name}/{job_name}: no runs-on")
        if "steps" not in job or not job["steps"]:
            problems.append(f"{name}/{job_name}: no steps")
        for step in job.get("steps", []):
            if "uses" not in step and "run" not in step:
                problems.append(f"{name}/{job_name}: step without uses/run: {step}")

    for entry in triggers.get("schedule", []):
        cron = entry.get("cron", "")
        if not CRON_RE.match(cron):
            problems.append(f"{name}: suspicious cron {cron!r}")

    if name.startswith("collect-"):
        env = jobs["collect"].get("env", {})
        for key in ("COLLECTOR_ROLE", "COLLECTOR_RUN_ID", "COLLECTOR_REDIS_URL", "COLLECTOR_SUPABASE_KEY"):
            if key not in env:
                problems.append(f"{name}: env missing {key}")
        if jobs["collect"].get("timeout-minutes") != 265:
            problems.append(f"{name}: timeout-minutes must be 265")

if problems:
    print("PROBLEMS:")
    for problem in problems:
        print(" -", problem)
    sys.exit(1)

print(f"OK: {len(EXPECTED)} workflow files parsed and structurally valid")
