"""Derived (computed) business-vault objects — the soft-rule layer.

Everything here is business logic deliberately kept OUT of the raw vault:

* effectivity resolution — collapsing insert-only assignment intervals to
  their current truth (closed versions supersede open ones)
* ``bsat_pr_lifecycle`` — opened/closed/merged timestamps and cycle time
  derived from the PR details satellite history
* ``bsat_issue_lifecycle`` — time-to-close and assignee-change counts

All functions are pure DataFrame transforms; ``job.py`` owns IO. The
business vault is rebuilt deterministically each run (derived layer), unlike
the insert-only raw vault it reads from.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from pipelines.raw_vault.loaders import HIGH_DTS

SECONDS_PER_HOUR = 3600.0


def resolve_effectivity(eff: DataFrame) -> DataFrame:
    """Collapse insert-only effectivity rows to current intervals.

    The raw vault records an open interval (``end_dts = 9999-12-31``) and,
    when it later closes, inserts the closed version as a new row. Per
    ``(link key, start_dts)`` the earliest ``end_dts`` is the truth; an
    interval is active when even that is the high date.
    """
    return (
        eff.groupBy("lhk_issue_assignee", "hk_issue", "hk_actor", "start_dts")
        .agg(F.min("end_dts").alias("end_dts"))
        .withColumn("is_active", F.col("end_dts") == F.lit(HIGH_DTS).cast("timestamp"))
    )


def pr_lifecycle_from_details(details: DataFrame) -> DataFrame:
    """One row per pull request: lifecycle milestones and cycle time.

    Soft rules applied (documented per ADR 003):
    * ``opened_at``  — earliest ``opened`` state in the details history
    * ``closed_at``  — earliest ``closed`` state (a PR may re-open later;
      first close is the milestone this mart reports)
    * ``is_merged``  — any state observed with ``pr_merged = true``
    * ``cycle_time_hours`` — closed_at - opened_at, null while open
    """
    opened = F.min(F.when(F.col("action") == "opened", F.col("occurred_at")))
    closed = F.min(F.when(F.col("action") == "closed", F.col("occurred_at")))
    merged = F.max(F.when(F.col("pr_merged") & (F.col("action") == "closed"), F.col("occurred_at")))
    return (
        details.groupBy("hk_pull_request")
        .agg(
            opened.alias("opened_at"),
            closed.alias("closed_at"),
            merged.alias("merged_at"),
            F.max(F.coalesce(F.col("pr_merged"), F.lit(False))).alias("is_merged"),
            F.max("occurred_at").alias("last_activity_at"),
        )
        .withColumn(
            "cycle_time_hours",
            (F.col("closed_at").cast("long") - F.col("opened_at").cast("long"))
            / F.lit(SECONDS_PER_HOUR),
        )
    )


def issue_lifecycle_from(details: DataFrame, resolved_eff: DataFrame) -> DataFrame:
    """One row per issue: lifecycle milestones plus assignment churn."""
    opened = F.min(F.when(F.col("action") == "opened", F.col("occurred_at")))
    closed = F.min(F.when(F.col("action") == "closed", F.col("occurred_at")))
    lifecycle = (
        details.groupBy("hk_issue")
        .agg(
            opened.alias("opened_at"),
            closed.alias("closed_at"),
            F.max("occurred_at").alias("last_activity_at"),
        )
        .withColumn(
            "time_to_close_hours",
            (F.col("closed_at").cast("long") - F.col("opened_at").cast("long"))
            / F.lit(SECONDS_PER_HOUR),
        )
    )
    churn = resolved_eff.groupBy("hk_issue").agg(
        F.count("*").alias("assignee_change_count"),
        F.max(F.when(F.col("is_active"), F.col("hk_actor"))).alias("active_assignee_hk"),
    )
    return lifecycle.join(churn, "hk_issue", "left").withColumn(
        "assignee_change_count", F.coalesce(F.col("assignee_change_count"), F.lit(0))
    )


def latest_state(details: DataFrame, key_col: str) -> DataFrame:
    """Most recent satellite state per parent key (dimension-style lookup)."""
    window = Window.partitionBy(key_col).orderBy(
        F.col("occurred_at").desc(), F.col("hash_diff").desc()
    )
    return (
        details.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")
    )
