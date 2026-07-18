"""Gold build job: rebuild the three information marts.

Reads raw-vault and business-vault tables only; writes deterministic
overwrites under ``gold/``. Marts are recomputable projections — the
insert-only history lives in the raw vault, never here.
"""

from __future__ import annotations

import sys

from pyspark.sql import DataFrame, SparkSession

from observability.metrics.writer import current_run_id, track_step
from pipelines.common.config import LakehouseConfig, load_config
from pipelines.common.session import get_spark
from pipelines.gold.collaboration import build_collaboration
from pipelines.gold.developer_360 import build_developer_360
from pipelines.gold.repo_activity import build_repo_activity
from pipelines.raw_vault.loaders import table_path


def _read(spark: SparkSession, config: LakehouseConfig, zone: str, name: str) -> DataFrame:
    return spark.read.format("delta").load(table_path(config, zone, name))


def _write(config: LakehouseConfig, df: DataFrame, name: str) -> int:
    count = df.count()
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(table_path(config, "gold", name))
    )
    return count


def run_build(
    config: LakehouseConfig | None = None, spark: SparkSession | None = None
) -> dict[str, int]:
    """Build all marts; returns row counts per mart."""
    config = config or load_config()
    spark = spark or get_spark("gold-marts", config)
    run_id = current_run_id()
    counts: dict[str, int] = {}

    def raw(name: str) -> DataFrame:
        return _read(spark, config, "raw_vault", name)

    def bv(name: str) -> DataFrame:
        return _read(spark, config, "business_vault", name)

    with track_step(
        spark, config, run_id=run_id, pipeline="gold", step="repo_activity_mart", layer="gold"
    ) as metric:
        mart = build_repo_activity(
            mas=raw("sat_actor_repo_event"),
            link_actor_repo=raw("link_actor_repo"),
            hub_repo=raw("hub_repo"),
            pit_repo_day=bv("pit_repo_day"),
            sat_repo_profile=raw("sat_repo_profile"),
            sat_repo_stats=raw("sat_repo_stats"),
        )
        counts["repo_activity_mart"] = _write(config, mart, "repo_activity_mart")
        metric.rows_written = counts["repo_activity_mart"]

    with track_step(
        spark, config, run_id=run_id, pipeline="gold", step="developer_360_mart", layer="gold"
    ) as metric:
        mart = build_developer_360(
            mas=raw("sat_actor_repo_event"),
            link_actor_repo=raw("link_actor_repo"),
            hub_actor=raw("hub_actor"),
            pit_actor_day=bv("pit_actor_day"),
            sat_actor_profile=raw("sat_actor_profile"),
            bridge=bv("bridge_repo_collaboration"),
            bsat_pr_lifecycle=bv("bsat_pr_lifecycle"),
        )
        counts["developer_360_mart"] = _write(config, mart, "developer_360_mart")
        metric.rows_written = counts["developer_360_mart"]

    with track_step(
        spark, config, run_id=run_id, pipeline="gold", step="collaboration_mart", layer="gold"
    ) as metric:
        mart = build_collaboration(
            bridge=bv("bridge_repo_collaboration"),
            bsat_pr_lifecycle=bv("bsat_pr_lifecycle"),
            bsat_issue_lifecycle=bv("bsat_issue_lifecycle"),
            pit_pull_request_day=bv("pit_pull_request_day"),
            sat_pull_request_details=raw("sat_pull_request_details"),
        )
        counts["collaboration_mart"] = _write(config, mart, "collaboration_mart")
        metric.rows_written = counts["collaboration_mart"]

    return counts


def main() -> int:
    counts = run_build()
    print("gold: rows per mart")
    for name, count in sorted(counts.items()):
        print(f"  {name:24s} {count}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
