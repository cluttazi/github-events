"""Bronze COPY INTO integration: exactly-once ledger, quarantine, envelope."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from ingestion.github_archive.generator import GeneratorConfig, run_generator
from pipelines.bronze.copy_into import (
    events_table_path,
    quarantine_table_path,
    run_copy_into,
)
from pipelines.common.config import LakehouseConfig

pytestmark = [pytest.mark.spark, pytest.mark.integration]


@pytest.fixture()
def seeded_landing(lakehouse_config: LakehouseConfig) -> tuple[LakehouseConfig, int]:
    summary = run_generator(
        GeneratorConfig(
            events=300, seed=42, corrupt_pct=5.0, landing_dir=lakehouse_config.source.landing_dir
        )
    )
    return lakehouse_config, summary.corrupt_events


def test_load_quarantine_and_exactly_once(
    spark: SparkSession, seeded_landing: tuple[LakehouseConfig, int]
) -> None:
    config, corrupt_count = seeded_landing

    first = run_copy_into(config, spark)
    assert first["rows_loaded"] + first["rows_quarantined"] == 300
    # 'truncate' corruption can still leave the envelope fields intact for
    # long lines, so quarantined <= injected; every fully-broken line lands.
    assert 0 < first["rows_quarantined"] <= corrupt_count

    events = spark.read.format("delta").load(events_table_path(config))
    assert events.count() == first["rows_loaded"]
    expected_cols = {
        "event_key",
        "raw_value",
        "event_type",
        "ts_ms",
        "source_ref",
        "run_id",
        "ingest_ts",
        "ingest_date",
    }
    assert expected_cols <= set(events.columns)

    quarantine = spark.read.format("delta").load(quarantine_table_path(config))
    reasons = {r["error_reason"] for r in quarantine.select("error_reason").distinct().collect()}
    assert reasons <= {
        "unparseable_json",
        "missing_id",
        "missing_or_bad_created_at",
        "unknown_event_type",
    }
    assert quarantine.count() == first["rows_quarantined"]

    # Re-run: the ledger makes it a no-op.
    second = run_copy_into(config, spark)
    assert second == {"rows_loaded": 0, "rows_quarantined": 0, "files_loaded": 0}
    events_after = spark.read.format("delta").load(events_table_path(config))
    assert events_after.count() == first["rows_loaded"]


def test_resent_file_under_new_name_loads(
    spark: SparkSession, seeded_landing: tuple[LakehouseConfig, int]
) -> None:
    config, _ = seeded_landing
    run_copy_into(config, spark)
    before = spark.read.format("delta").load(events_table_path(config)).count()

    landing = config.source.landing_dir
    src = sorted(landing.glob("*.ndjson"))[0]
    resent = landing / f"resent-{src.name}"
    resent.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")

    result = run_copy_into(config, spark)
    assert result["files_loaded"] == 1
    after = spark.read.format("delta").load(events_table_path(config)).count()
    assert after == before + result["rows_loaded"]
