"""Raw vault load job: stage → enforce → load hubs, links, satellites.

Load order respects referential integrity: hubs first, then links, then
satellites — a link row never references a hub key that is not already
present, which the DQ suites assert as an error-severity check.

``--verify-idempotent`` re-runs the entire load and exits non-zero if any
table gained rows: the executable proof that raw-vault loads are pure
functions of bronze (ADR 003). The orchestrator runs it on every demo.
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from observability.metrics.writer import current_run_id, track_step
from pipelines.common.config import LakehouseConfig, load_config
from pipelines.common.session import get_spark
from pipelines.raw_vault.enforcement import enforce_contract
from pipelines.raw_vault.loaders import (
    insert_only_merge,
    load_effectivity_satellite,
    load_hubs,
    load_links,
    load_multi_active_satellite,
    load_standard_satellite,
    table_path,
)
from pipelines.raw_vault.staging import CONTRACT_BY_EVENT_TYPE, stage_events
from quality.contracts.loader import load_contracts


def _enforce_all(
    spark: SparkSession, config: LakehouseConfig, run_id: str
) -> tuple[dict[str, DataFrame], int]:
    """Stage bronze, apply contracts; returns (valid frames, quarantined count)."""
    contracts = load_contracts()
    batch = stage_events(spark, config, config.vault.record_source_prefix)
    valid: dict[str, DataFrame] = {}
    bad_frames: list[DataFrame] = []
    total_quarantined = 0
    for event_type, staged in batch.by_type.items():
        contract = contracts[CONTRACT_BY_EVENT_TYPE[event_type]]
        result = enforce_contract(staged, contract)
        valid[event_type] = result.valid
        n_bad = result.quarantined.count()
        if n_bad:
            bad_frames.append(
                result.quarantined.withColumn("violations", F.to_json("violations"))
                .withColumn("quarantined_run_id", F.lit(run_id))
                .selectExpr(
                    "event_id", "record_source", "violations", "quarantined_run_id", "occurred_at"
                )
            )
        total_quarantined += n_bad
    if bad_frames:
        quarantine = bad_frames[0]
        for frame in bad_frames[1:]:
            quarantine = quarantine.unionByName(frame)
        # insert-only on event_id: the verify re-run must not duplicate rows
        insert_only_merge(
            spark, table_path(config, "raw_vault", "quarantine"), quarantine, ["event_id"]
        )
    return valid, total_quarantined


def run_load(
    config: LakehouseConfig | None = None, spark: SparkSession | None = None
) -> dict[str, int]:
    """Run the full raw-vault load; returns rows inserted per vault object."""
    config = config or load_config()
    spark = spark or get_spark("raw-vault", config)
    run_id = current_run_id()
    inserted: dict[str, int] = {}

    with track_step(
        spark, config, run_id=run_id, pipeline="raw_vault", step="stage_enforce", layer="silver"
    ) as metric:
        staged, quarantined = _enforce_all(spark, config, run_id)
        metric.rows_quarantined = quarantined

    with track_step(
        spark, config, run_id=run_id, pipeline="raw_vault", step="hubs", layer="silver"
    ) as metric:
        hub_counts = load_hubs(spark, config, staged)
        inserted.update(hub_counts)
        metric.rows_written = sum(hub_counts.values())

    with track_step(
        spark, config, run_id=run_id, pipeline="raw_vault", step="links", layer="silver"
    ) as metric:
        link_counts = load_links(spark, config, staged)
        inserted.update(link_counts)
        metric.rows_written = sum(link_counts.values())

    with track_step(
        spark, config, run_id=run_id, pipeline="raw_vault", step="satellites", layer="silver"
    ) as metric:
        sat_counts: dict[str, int] = {}
        for sat in config.vault.satellites:
            if sat.kind == "standard":
                sat_counts[sat.name] = load_standard_satellite(spark, config, sat.name, staged)
            elif sat.kind == "multi_active":
                sat_counts[sat.name] = load_multi_active_satellite(spark, config, sat.name, staged)
            else:
                sat_counts[sat.name] = load_effectivity_satellite(spark, config, sat.name, staged)
        inserted.update(sat_counts)
        metric.rows_written = sum(sat_counts.values())

    return inserted


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="raw_vault", description="Load the Data Vault 2.0 raw vault from bronze."
    )
    parser.add_argument(
        "--verify-idempotent",
        action="store_true",
        help="re-run the load and fail unless every table gains zero rows",
    )
    args = parser.parse_args(argv)

    config = load_config()
    spark = get_spark("raw-vault", config)

    inserted = run_load(config, spark)
    print("raw_vault: rows inserted per object")
    for name, count in sorted(inserted.items()):
        print(f"  {name:28s} +{count}")

    if args.verify_idempotent:
        second = run_load(config, spark)
        dirty = {name: count for name, count in second.items() if count != 0}
        if dirty:
            print(f"raw_vault: IDEMPOTENCY VIOLATION — re-run inserted rows: {dirty}")
            return 2
        print("raw_vault: idempotency verified — re-run inserted 0 rows in every object")
    return 0


if __name__ == "__main__":
    sys.exit(main())
