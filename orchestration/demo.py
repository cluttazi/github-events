"""End-to-end demo: events -> bronze -> raw vault -> business vault -> gold -> DQ.

Why a Python orchestrator instead of Makefile chaining: the demo must share
one run_id across steps, time each step, capture logs, survive a mid-run
failure gracefully, and print a summary humans can screenshot. That's a
program, not a Makefile recipe. Each step runs as a **subprocess**, so at
most one Spark JVM is alive at a time and a crash in one step cannot take
down the driver.

The step order encodes the layer dependencies (Bronze -> Raw Vault ->
Business Vault -> Gold -> DQ) — the same DAG the Databricks Asset Bundle
job declares with ``depends_on``. The ``raw_vault_idempotency`` step is the
DV2.0 proof: it re-runs the entire vault load and fails unless every object
gains zero rows.

The per-step `make` targets invoke the same modules; this file only
sequences them.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

from pipelines.common.config import load_config

REPO_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Step:
    name: str
    argv: list[str]
    timeout_s: int = 900
    essential: bool = True  # non-essential steps run even after earlier failures


@dataclass
class StepOutcome:
    step: Step
    status: str  # success | failed | skipped
    duration_s: float
    log_path: Path
    tail: str = ""


def build_steps(events: int, seed: int, corrupt_pct: str) -> list[Step]:
    python = [sys.executable, "-m"]
    return [
        Step(
            "generate_events",
            [
                *python,
                "ingestion.github_archive",
                "--events",
                str(events),
                "--seed",
                str(seed),
                "--corrupt-pct",
                corrupt_pct,
            ],
        ),
        Step("bronze_copy_into", [*python, "pipelines.bronze.copy_into"]),
        Step(
            "raw_vault_load_and_verify",
            [*python, "pipelines.raw_vault.job", "--verify-idempotent"],
            timeout_s=1200,
        ),
        Step("business_vault_build", [*python, "pipelines.business_vault.job"]),
        Step("gold_marts", [*python, "pipelines.gold.job"]),
        Step("data_quality", [*python, "quality.expectations.runner"]),
        Step("observability_report", [*python, "observability.metrics.report"], essential=False),
    ]


def run_step(step: Step, log_dir: Path, env: dict[str, str]) -> StepOutcome:
    log_path = log_dir / f"{step.name}.log"
    started = time.monotonic()
    try:
        with log_path.open("w", encoding="utf-8") as log:
            completed = subprocess.run(
                step.argv,
                cwd=REPO_ROOT,
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=step.timeout_s,
                check=False,
            )
        status = "success" if completed.returncode == 0 else "failed"
    except subprocess.TimeoutExpired:
        status = "failed"
    duration = time.monotonic() - started
    tail = ""
    if log_path.exists():
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
        tail = "\n".join(lines[-12:])
    return StepOutcome(step=step, status=status, duration_s=duration, log_path=log_path, tail=tail)


def print_summary(run_id: str, outcomes: list[StepOutcome], log_dir: Path) -> None:
    config = load_config()
    width = 74
    print("\n" + "=" * width)
    print(f"  github-events-lakehouse demo summary   run_id={run_id}")
    print("=" * width)
    for outcome in outcomes:
        marker = {"success": "ok", "failed": "FAILED", "skipped": "skipped"}[outcome.status]
        print(f"  [{marker:>7s}] {outcome.step.name:26s} {outcome.duration_s:7.1f}s")
    failed = [o for o in outcomes if o.status == "failed"]
    print("-" * width)
    if failed:
        print(f"  {len(failed)} step(s) failed — logs under {log_dir}")
        for outcome in failed:
            print(f"\n  --- tail of {outcome.step.name} ---")
            print("  " + "\n  ".join(outcome.tail.splitlines()[-8:]))
    else:
        print("  all steps green (incl. raw-vault idempotency: re-run added 0 rows)")
    print("-" * width)
    print("  artifacts:")
    print(f"    lakehouse tables   {config.storage.lakehouse_root}")
    print(f"    run logs           {log_dir}")
    print(f"    dq results         {config.storage.run_dir / 'dq'}")
    print(f"    html run report    {config.storage.reports_dir / 'observability.html'}")
    print("=" * width)


def main() -> int:
    events = int(os.environ.get("EVENTS", "2000"))
    seed = int(os.environ.get("SEED", "42"))
    corrupt_pct = os.environ.get("CORRUPT_PCT", "2")
    run_id = f"demo-{uuid.uuid4().hex[:8]}"

    config = load_config()
    log_dir = config.storage.run_dir / run_id / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    env = {**os.environ, "LAKEHOUSE_RUN_ID": run_id}

    print(f"github-events-lakehouse demo  run_id={run_id}  events={events} seed={seed}")
    outcomes: list[StepOutcome] = []
    pipeline_broken = False
    for step in build_steps(events, seed, corrupt_pct):
        if pipeline_broken and step.essential:
            outcomes.append(StepOutcome(step, "skipped", 0.0, log_dir / f"{step.name}.log"))
            print(f"  -> {step.name:26s} skipped (earlier failure)")
            continue
        print(f"  -> {step.name:26s} running...", flush=True)
        outcome = run_step(step, log_dir, env)
        outcomes.append(outcome)
        print(f"     {outcome.status} in {outcome.duration_s:.1f}s")
        if outcome.status == "failed" and step.essential:
            pipeline_broken = True

    print_summary(run_id, outcomes, log_dir)
    return 1 if any(o.status != "success" for o in outcomes) else 0


if __name__ == "__main__":
    sys.exit(main())
