"""COPY INTO-semantics batch loader for the GH-Archive landing zone.

Databricks' ``COPY INTO`` guarantees each file loads exactly once by tracking
loaded files in table metadata. Locally we make that ledger explicit: a Delta
table (``bronze/ops/file_ledger``) records every ingested file path, and each
run loads only the set difference. Re-running is a no-op; a *resent* file
under a new name is loaded (new path = new file) and left for the raw vault
to deduplicate — the same behavior COPY INTO exhibits.

Bronze stays full-fidelity and append-only: the entire original line rides in
``raw_value``; only the minimal envelope (id, type, created_at) is extracted
for routing. Lines that fail the envelope (unparseable JSON, missing id or
timestamp, unknown event type) go to ``bronze/quarantine`` with a reason —
quarantine over fail-fast. See docs/adr/001-batch-copy-into-only.md for why
there is no streaming path.
"""

from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from observability.metrics.writer import current_run_id, track_step
from pipelines.common.config import LakehouseConfig, load_config
from pipelines.common.session import get_spark

DATASET = "github_events"

LEDGER_SCHEMA = StructType(
    [
        StructField("file_path", StringType(), nullable=False),
        StructField("dataset", StringType(), nullable=False),
        StructField("rows_loaded", LongType(), nullable=False),
        StructField("run_id", StringType(), nullable=False),
        StructField("loaded_at", TimestampType(), nullable=False),
    ]
)

# Minimal envelope for routing; the full payload stays as raw_value text.
ENVELOPE_SCHEMA = "id string, type string, created_at string"


def events_table_path(config: LakehouseConfig) -> str:
    return str(config.storage.lakehouse_root / "bronze" / DATASET)


def quarantine_table_path(config: LakehouseConfig) -> str:
    return str(config.storage.lakehouse_root / "bronze" / "quarantine")


def ledger_path(config: LakehouseConfig) -> str:
    return str(config.storage.lakehouse_root / "bronze" / "ops" / "file_ledger")


def _already_loaded(spark: SparkSession, config: LakehouseConfig) -> set[str]:
    try:
        ledger = spark.read.format("delta").load(ledger_path(config))
    except Exception:  # first run: ledger doesn't exist yet
        return set()
    rows = ledger.filter(F.col("dataset") == DATASET).select("file_path").collect()
    return {r["file_path"] for r in rows}


def _read_lines(spark: SparkSession, paths: list[str]) -> DataFrame:
    # _metadata.file_path is a file:// URI; the ledger stores plain filesystem
    # paths so the set-difference against os-listed files actually matches —
    # a scheme mismatch here would silently reload every file on every run.
    return spark.read.text(paths).withColumn(
        "source_ref", F.regexp_replace(F.col("_metadata.file_path"), "^file:/{0,2}(?=/)", "")
    )


def classify_lines(lines: DataFrame, event_types: list[str]) -> tuple[DataFrame, DataFrame]:
    """Split raw text lines into (valid envelope rows, quarantined rows)."""
    parsed = lines.select(
        F.col("value").alias("raw_value"),
        F.col("source_ref"),
        F.from_json(F.col("value"), ENVELOPE_SCHEMA).alias("envelope"),
    ).select(
        "raw_value",
        "source_ref",
        F.col("envelope.id").alias("event_key"),
        F.col("envelope.type").alias("event_type"),
        F.try_to_timestamp(F.col("envelope.created_at")).alias("created_ts"),
        F.col("envelope").isNull().alias("_unparseable"),
    )
    reason = (
        F.when(F.col("_unparseable"), F.lit("unparseable_json"))
        .when(F.col("event_key").isNull(), F.lit("missing_id"))
        .when(F.col("created_ts").isNull(), F.lit("missing_or_bad_created_at"))
        .when(~F.col("event_type").isin(event_types), F.lit("unknown_event_type"))
    )
    classified = parsed.withColumn("error_reason", reason)
    valid = classified.filter(F.col("error_reason").isNull()).drop("error_reason", "_unparseable")
    quarantined = classified.filter(F.col("error_reason").isNotNull()).select(
        F.col("raw_value").alias("raw_line"), "error_reason", "source_ref"
    )
    return valid, quarantined


def _with_audit(df: DataFrame, run_id: str) -> DataFrame:
    return (
        df.withColumn("run_id", F.lit(run_id))
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("ingest_date", F.to_date(F.current_timestamp()))
    )


def run_copy_into(
    config: LakehouseConfig | None = None, spark: SparkSession | None = None
) -> dict[str, int]:
    """Load new landing files exactly once; returns row counts by outcome."""
    config = config or load_config()
    spark = spark or get_spark("bronze-copy-into", config)
    run_id = current_run_id()
    counts = {"rows_loaded": 0, "rows_quarantined": 0, "files_loaded": 0}

    with track_step(
        spark, config, run_id=run_id, pipeline="bronze", step="copy_into", layer="bronze"
    ) as metric:
        landing = Path(config.source.landing_dir)
        all_files = (
            sorted(str(p) for p in landing.iterdir() if p.is_file()) if landing.exists() else []
        )
        already = _already_loaded(spark, config)
        new_files = [p for p in all_files if p not in already]
        if new_files:
            lines = _read_lines(spark, new_files)
            valid, quarantined = classify_lines(lines, config.event_types)

            events = _with_audit(
                valid.select(
                    "event_key",
                    "raw_value",
                    "event_type",
                    (F.col("created_ts").cast("long") * 1000).alias("ts_ms"),
                    "source_ref",
                ),
                run_id,
            )
            (
                events.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .partitionBy("ingest_date")
                .save(events_table_path(config))
            )

            quarantine = _with_audit(quarantined, run_id)
            quarantined_count = quarantine.count()
            if quarantined_count:
                (
                    quarantine.write.format("delta")
                    .mode("append")
                    .option("mergeSchema", "true")
                    .save(quarantine_table_path(config))
                )

            per_file = (
                lines.groupBy("source_ref")
                .count()
                .select(
                    F.col("source_ref").alias("file_path"),
                    F.lit(DATASET).alias("dataset"),
                    F.col("count").cast("long").alias("rows_loaded"),
                    F.lit(run_id).alias("run_id"),
                    F.current_timestamp().alias("loaded_at"),
                )
            )
            per_file.write.format("delta").mode("append").save(ledger_path(config))

            counts["files_loaded"] = len(new_files)
            counts["rows_quarantined"] = quarantined_count
            counts["rows_loaded"] = sum(r["rows_loaded"] for r in per_file.collect()) - (
                quarantined_count
            )

        metric.rows_written = counts["rows_loaded"]
        metric.rows_quarantined = counts["rows_quarantined"]
        metric.extra = {k: str(v) for k, v in sorted(counts.items())}
    return counts


def main() -> int:
    counts = run_copy_into()
    print(
        f"copy_into: loaded {counts['rows_loaded']} rows from {counts['files_loaded']} new files "
        f"({counts['rows_quarantined']} quarantined)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
