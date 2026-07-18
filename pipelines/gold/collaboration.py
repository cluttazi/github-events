"""collaboration_mart — one row per collaboration item (PR or issue).

Grain: ``(item_type, repo_name, item_number)``.

The bridge supplies the multi-link traversal (who touched the item, and
how); the lifecycle business satellites supply milestones and cycle times;
the current PR state resolves through the latest ``pit_pull_request_day``
pointer into the details satellite; the active assignee comes from the
resolved effectivity intervals already flattened into the bridge.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_collaboration(
    bridge: DataFrame,
    bsat_pr_lifecycle: DataFrame,
    bsat_issue_lifecycle: DataFrame,
    pit_pull_request_day: DataFrame,
    sat_pull_request_details: DataFrame,
) -> DataFrame:
    is_participant = F.col("relationship_type").isin("pr_actor", "issue_actor")
    is_assignee = F.col("relationship_type") == "issue_assignee"
    items = bridge.groupBy("item_type", "repo_name", "item_number", "hk_item").agg(
        F.countDistinct(F.when(is_participant, F.col("hk_actor"))).alias("participants"),
        F.max(F.when(is_assignee, F.col("actor_login"))).alias("active_assignee"),
    )

    pr_lifecycle = bsat_pr_lifecycle.select(
        F.col("hk_pull_request").alias("_pr_hk"),
        F.col("opened_at").alias("pr_opened_at"),
        F.col("closed_at").alias("pr_closed_at"),
        F.col("merged_at"),
        "is_merged",
        "cycle_time_hours",
    )
    issue_lifecycle = bsat_issue_lifecycle.select(
        F.col("hk_issue").alias("_issue_hk"),
        F.col("opened_at").alias("issue_opened_at"),
        F.col("closed_at").alias("issue_closed_at"),
        "time_to_close_hours",
        "assignee_change_count",
    )

    # Current PR state: the latest PIT day's pointer into the details satellite.
    latest_pit = (
        pit_pull_request_day.groupBy("hk_pull_request")
        .agg(F.max("sat_pull_request_details_pit_ts").alias("_state_ts"))
        .withColumnRenamed("hk_pull_request", "_pit_hk")
    )
    pr_state = sat_pull_request_details.select(
        F.col("hk_pull_request").alias("_state_hk"),
        F.col("occurred_at").alias("_state_ts2"),
        F.col("pr_title"),
        F.col("pr_state"),
    )

    return (
        items.join(pr_lifecycle, F.col("hk_item") == F.col("_pr_hk"), "left")
        .join(issue_lifecycle, F.col("hk_item") == F.col("_issue_hk"), "left")
        .join(latest_pit, F.col("hk_item") == F.col("_pit_hk"), "left")
        .join(
            pr_state,
            (F.col("hk_item") == F.col("_state_hk")) & (F.col("_state_ts") == F.col("_state_ts2")),
            "left",
        )
        .select(
            "item_type",
            "repo_name",
            "item_number",
            "participants",
            "active_assignee",
            "pr_title",
            "pr_state",
            F.coalesce("pr_opened_at", F.col("issue_opened_at")).alias("opened_at"),
            F.coalesce("pr_closed_at", F.col("issue_closed_at")).alias("closed_at"),
            "merged_at",
            "is_merged",
            F.coalesce("cycle_time_hours", F.col("time_to_close_hours")).alias("resolution_hours"),
            F.coalesce("assignee_change_count", F.lit(0)).alias("assignee_change_count"),
            "hk_item",
        )
    )
