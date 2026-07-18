"""Business vault build job: derived satellites, PIT tables, bridge.

Reads only raw-vault tables; writes deterministic overwrites under
``business_vault/`` — this layer is recomputable by design, unlike the
insert-only raw vault it derives from.
"""

from __future__ import annotations

import sys

from pyspark.sql import DataFrame, SparkSession

from observability.metrics.writer import current_run_id, track_step
from pipelines.business_vault.bridge import bridge_repo_collaboration
from pipelines.business_vault.derived import (
    issue_lifecycle_from,
    pr_lifecycle_from_details,
    resolve_effectivity,
)
from pipelines.business_vault.pit import date_spine, pit_from_satellites
from pipelines.common.config import LakehouseConfig, load_config
from pipelines.common.session import get_spark
from pipelines.raw_vault.loaders import table_path

PIT_DEFINITIONS: dict[str, tuple[str, list[str]]] = {
    # pit name -> (hub, satellites whose states it points at)
    "pit_repo_day": ("hub_repo", ["sat_repo_profile", "sat_repo_stats"]),
    "pit_actor_day": ("hub_actor", ["sat_actor_profile"]),
    "pit_pull_request_day": ("hub_pull_request", ["sat_pull_request_details"]),
}


def _read(spark: SparkSession, config: LakehouseConfig, name: str) -> DataFrame:
    return spark.read.format("delta").load(table_path(config, "raw_vault", name))


def _write(config: LakehouseConfig, df: DataFrame, name: str) -> int:
    count = df.count()
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(table_path(config, "business_vault", name))
    )
    return count


def run_build(
    config: LakehouseConfig | None = None, spark: SparkSession | None = None
) -> dict[str, int]:
    """Build every business-vault object; returns row counts per table."""
    config = config or load_config()
    spark = spark or get_spark("business-vault", config)
    run_id = current_run_id()
    counts: dict[str, int] = {}

    with track_step(
        spark, config, run_id=run_id, pipeline="business_vault", step="derived", layer="silver"
    ) as metric:
        resolved = resolve_effectivity(_read(spark, config, "eff_sat_issue_assignee"))
        counts["bsat_pr_lifecycle"] = _write(
            config,
            pr_lifecycle_from_details(_read(spark, config, "sat_pull_request_details")),
            "bsat_pr_lifecycle",
        )
        counts["bsat_issue_lifecycle"] = _write(
            config,
            issue_lifecycle_from(_read(spark, config, "sat_issue_details"), resolved),
            "bsat_issue_lifecycle",
        )
        metric.rows_written = counts["bsat_pr_lifecycle"] + counts["bsat_issue_lifecycle"]

    with track_step(
        spark, config, run_id=run_id, pipeline="business_vault", step="pit", layer="silver"
    ) as metric:
        spine = date_spine(_read(spark, config, "sat_actor_repo_event"))
        for pit_name, (hub_name, sat_names) in PIT_DEFINITIONS.items():
            pit = pit_from_satellites(
                _read(spark, config, hub_name),
                config.vault.hub(hub_name).hash_key_column,
                {name: _read(spark, config, name) for name in sat_names},
                spine,
            )
            counts[pit_name] = _write(config, pit, pit_name)
        metric.rows_written = sum(counts[name] for name in PIT_DEFINITIONS)

    with track_step(
        spark, config, run_id=run_id, pipeline="business_vault", step="bridge", layer="silver"
    ) as metric:
        bridge = bridge_repo_collaboration(
            hub_actor=_read(spark, config, "hub_actor"),
            hub_repo=_read(spark, config, "hub_repo"),
            hub_pull_request=_read(spark, config, "hub_pull_request"),
            hub_issue=_read(spark, config, "hub_issue"),
            link_actor_pull_request=_read(spark, config, "link_actor_pull_request"),
            link_actor_issue=_read(spark, config, "link_actor_issue"),
            resolved_assignments=resolve_effectivity(
                _read(spark, config, "eff_sat_issue_assignee")
            ),
        )
        counts["bridge_repo_collaboration"] = _write(config, bridge, "bridge_repo_collaboration")
        metric.rows_written = counts["bridge_repo_collaboration"]

    return counts


def main() -> int:
    counts = run_build()
    print("business_vault: rows per object")
    for name, count in sorted(counts.items()):
        print(f"  {name:28s} {count}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
