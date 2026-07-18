"""Writer for the ``pipeline_run_metrics`` Delta table.

Usage — either record a fully-built :class:`RunMetric`, or wrap a step in
:func:`track_step` and let it time/record success and failure uniformly::

    with track_step(spark, config, run_id="...", pipeline="raw_vault", step="hub_actor") as m:
        m.rows_written = 1234
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pipelines.common.config import LakehouseConfig

METRICS_TABLE_RELPATH = Path("observability/pipeline_run_metrics")

METRICS_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), nullable=False),
        StructField("run_date", DateType(), nullable=False),
        StructField("pipeline", StringType(), nullable=False),
        StructField("step", StringType(), nullable=False),
        StructField("layer", StringType(), nullable=True),
        StructField("status", StringType(), nullable=False),  # success | failed | skipped
        StructField("started_at", TimestampType(), nullable=False),
        StructField("finished_at", TimestampType(), nullable=False),
        StructField("duration_s", DoubleType(), nullable=False),
        StructField("rows_read", LongType(), nullable=True),
        StructField("rows_written", LongType(), nullable=True),
        StructField("rows_quarantined", LongType(), nullable=True),
        StructField("dq_checks_passed", IntegerType(), nullable=True),
        StructField("dq_checks_failed", IntegerType(), nullable=True),
        StructField("dq_pass_rate", DoubleType(), nullable=True),
        StructField("error", StringType(), nullable=True),
        StructField("extra", MapType(StringType(), StringType()), nullable=True),
    ]
)


def current_run_id() -> str:
    """Run correlation id: the orchestrator sets LAKEHOUSE_RUN_ID so every
    step of one demo run shares it; standalone invocations get a fresh one."""
    return os.environ.get("LAKEHOUSE_RUN_ID") or f"adhoc-{uuid.uuid4().hex[:10]}"


@dataclass
class RunMetric:
    run_id: str
    pipeline: str
    step: str
    layer: str | None = None
    status: str = "success"
    started_at: datetime = field(default_factory=lambda: datetime.now(tz=UTC))
    finished_at: datetime | None = None
    rows_read: int | None = None
    rows_written: int | None = None
    rows_quarantined: int | None = None
    dq_checks_passed: int | None = None
    dq_checks_failed: int | None = None
    dq_pass_rate: float | None = None
    error: str | None = None
    extra: dict[str, str] = field(default_factory=dict)

    def finish(self, status: str, error: str | None = None) -> None:
        self.finished_at = datetime.now(tz=UTC)
        self.status = status
        self.error = error


def metrics_table_path(config: LakehouseConfig) -> Path:
    return config.storage.lakehouse_root / METRICS_TABLE_RELPATH


def write_metric(spark: SparkSession, config: LakehouseConfig, metric: RunMetric) -> None:
    """Append one metric row; creates the Delta table on first write."""
    finished = metric.finished_at or datetime.now(tz=UTC)
    duration = (finished - metric.started_at).total_seconds()
    row = (
        metric.run_id,
        metric.started_at.date(),
        metric.pipeline,
        metric.step,
        metric.layer,
        metric.status,
        metric.started_at,
        finished,
        duration,
        metric.rows_read,
        metric.rows_written,
        metric.rows_quarantined,
        metric.dq_checks_passed,
        metric.dq_checks_failed,
        metric.dq_pass_rate,
        metric.error,
        metric.extra or None,
    )
    df = spark.createDataFrame([row], schema=METRICS_SCHEMA)
    (
        df.write.format("delta")
        .mode("append")
        .partitionBy("run_date")
        .save(str(metrics_table_path(config)))
    )


@contextmanager
def track_step(
    spark: SparkSession,
    config: LakehouseConfig,
    *,
    run_id: str,
    pipeline: str,
    step: str,
    layer: str | None = None,
) -> Iterator[RunMetric]:
    """Time a pipeline step and always record it — success or failure.

    Failures are recorded *and re-raised*: observability must never swallow
    the error that CI or the orchestrator needs to see.
    """
    metric = RunMetric(run_id=run_id, pipeline=pipeline, step=step, layer=layer)
    try:
        yield metric
    except Exception as exc:
        metric.finish("failed", error=f"{type(exc).__name__}: {exc}")
        write_metric(spark, config, metric)
        raise
    else:
        metric.finish("success")
        write_metric(spark, config, metric)
