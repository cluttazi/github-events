"""Run every DQ suite against the lakehouse and publish the results.

Results go three places, on purpose:
* stdout — the human-readable report `make dq` prints,
* ``<run_dir>/dq/<table>.json`` — machine-readable, read by the incident agent,
* the metrics Delta table — one row per suite, feeding the dashboard.

Exit code is non-zero only for error-severity failures; warns are signals,
not build-breakers (mirroring the dbt severity model).
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from observability.metrics.writer import RunMetric, current_run_id, write_metric
from pipelines.common.config import LakehouseConfig, load_config
from pipelines.common.session import get_spark
from quality.expectations.checks import run_check
from quality.expectations.suite import Suite, SuiteResult, load_suites


def _load_table(spark: SparkSession, config: LakehouseConfig, table: str) -> DataFrame:
    return spark.read.format("delta").load(str(config.storage.lakehouse_root / table))


def run_suite(spark: SparkSession, config: LakehouseConfig, suite: Suite) -> SuiteResult:
    df = _load_table(spark, config, suite.table)
    if suite.filter:
        df = df.filter(suite.filter)
    df = df.cache()
    try:
        resolver = {
            check.ref_table: _load_table(spark, config, check.ref_table)
            for check in suite.checks
            if hasattr(check, "ref_table")
        }
        results = [run_check(spark, df, check, resolver) for check in suite.checks]
        return SuiteResult(
            table=suite.table,
            layer=suite.layer,
            row_count=df.count(),
            checks=results,
            executed_at=datetime.now(tz=UTC),
        )
    finally:
        df.unpersist()


def _write_json(config: LakehouseConfig, result: SuiteResult) -> Path:
    out_dir = config.storage.run_dir / "dq"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{result.table.replace('/', '_')}.json"
    payload = {
        "table": result.table,
        "layer": result.layer,
        "row_count": result.row_count,
        "executed_at": result.executed_at.isoformat(),
        "pass_rate": result.pass_rate,
        "checks": [
            {
                "description": c.description,
                "severity": c.severity,
                "status": c.status,
                "observed": c.observed,
            }
            for c in result.checks
        ],
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def _record_metric(
    spark: SparkSession, config: LakehouseConfig, run_id: str, result: SuiteResult
) -> None:
    metric = RunMetric(
        run_id=run_id,
        pipeline="quality",
        step=f"dq[{result.table}]",
        layer=result.layer,
        rows_read=result.row_count,
        dq_checks_passed=result.n_passed,
        dq_checks_failed=result.n_failed + result.n_warned,
        dq_pass_rate=result.pass_rate,
        extra={"warns": str(result.n_warned), "errors": str(result.n_failed)},
    )
    metric.finish("success" if result.n_failed == 0 else "failed")
    write_metric(spark, config, metric)


def run_suites(
    config: LakehouseConfig | None = None, spark: SparkSession | None = None
) -> list[SuiteResult]:
    """Run all suites; write JSON + metrics; return results."""
    config = config or load_config()
    spark = spark or get_spark("dq-runner", config)
    run_id = current_run_id()
    results = []
    for suite in load_suites():
        result = run_suite(spark, config, suite)
        _write_json(config, result)
        _record_metric(spark, config, run_id, result)
        results.append(result)
    return results


STATUS_ICON = {"pass": "ok", "warn": "WARN", "fail": "FAIL"}


def main() -> int:
    results = run_suites()
    total_failed = 0
    print("data quality report")
    print("=" * 78)
    for result in results:
        print(
            f"\n{result.table}  [{result.layer}]  rows={result.row_count}  "
            f"pass_rate={result.pass_rate:.0%}"
        )
        for check in result.checks:
            print(f"  [{STATUS_ICON[check.status]:>4s}] {check.description:52s} {check.observed}")
        total_failed += result.n_failed
    print("\n" + "=" * 78)
    n_warns = sum(r.n_warned for r in results)
    print(f"suites={len(results)} errors={total_failed} warns={n_warns}")
    return 1 if total_failed else 0


if __name__ == "__main__":
    sys.exit(main())
