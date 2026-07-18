"""Spark session factory for all local pipelines.

Centralizes the settings that would otherwise drift between jobs:

* Delta Lake wiring via ``configure_spark_with_delta_pip`` — the Delta jars
  ship inside the ``delta-spark`` wheel, so no network resolution happens
  for local runs.
* UTC session timezone: GitHub event ``created_at`` is UTC; satellite
  ``load_dts`` ordering must not depend on the host timezone.
* ANSI mode stays ON (the Spark 4 default). Dirty event payloads are handled
  with ``try_cast``/permissive JSON parsing at the edges, not by globally
  disabling correctness.
* Delta schema auto-merge is enabled so additive, nullable contract changes
  (the only kind ``quality.contracts.compat`` lets through) flow into MERGE
  targets without manual DDL.
"""

from __future__ import annotations

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from pipelines.common.config import LakehouseConfig


def get_spark(app_name: str, config: LakehouseConfig) -> SparkSession:
    """Build (or reuse) the local SparkSession with Delta enabled."""
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.driver.memory", config.spark.driver_memory)
        .config("spark.sql.shuffle.partitions", str(config.spark.shuffle_partitions))
        .config("spark.sql.session.timeZone", config.spark.session_timezone)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.ui.enabled", "false")
        .config("spark.sql.sources.parallelPartitionDiscovery.parallelism", "4")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
