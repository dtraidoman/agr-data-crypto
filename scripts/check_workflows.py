from __future__ import annotations

import sys
from pathlib import Path

import yaml

ROOT = Path(".github/workflows")
REQUIRED = [
    "collect-a.yml",
    "collect-b.yml",
    "backfill.yml",
    "reconcile.yml",
    "ci.yml",
    "watchdog.yml",
]


def main() -> int:
    problems: list[str] = []
    for name in REQUIRED:
        path = ROOT / name
        if not path.exists():
            problems.append(f"missing {path}")
            continue
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if "jobs" not in data:
            problems.append(f"{name}: no jobs")
    for name, role in (("collect-a.yml", "alpha"), ("collect-b.yml", "bravo")):
        path = ROOT / name
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        if f"COLLECTOR_ROLE: {role}" not in text:
            problems.append(f"{name}: missing COLLECTOR_ROLE={role}")
        if 'COLLECTOR_SOFT_DEADLINE_SEC: "16200"' not in text:
            problems.append(f"{name}: soft deadline must overlap the next 4-hour schedule")
        if "concurrency:" in text:
            problems.append(f"{name}: a concurrency group would create a handoff gap")
        if "python -m collector.main" not in text:
            problems.append(f"{name}: missing collector entrypoint")
    backfill = ROOT / "backfill.yml"
    if backfill.exists() and "scripts/backfill_derivatives.py" not in backfill.read_text(encoding="utf-8"):
        problems.append("backfill.yml: wrong script")
    for path in ROOT.glob("*.yml"):
        text = path.read_text(encoding="utf-8")
        for stale in ("bootstrap_history.py", "COLLECTOR_INTERVAL", "ohlcv"):
            if stale in text:
                problems.append(f"{path.name}: stale reference {stale}")
    if problems:
        for problem in problems:
            print(f"ERROR: {problem}")
        return 1
    print("workflows ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
