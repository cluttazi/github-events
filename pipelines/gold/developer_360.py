"""developer_360_mart — one row per actor per active day.

Grain: ``(actor_login, activity_date)``.

Daily measures come from the multi-active satellite through
``link_actor_repo``; profile attributes resolve as-of-day through
``pit_actor_day``. Lifetime collaboration columns (PRs/issues acted on,
PRs merged) come from the bridge and ``bsat_pr_lifecycle`` — they are
dimension-like and constant across a given actor's rows by design.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_developer_360(
    mas: DataFrame,
    link_actor_repo: DataFrame,
    hub_actor: DataFrame,
    pit_actor_day: DataFrame,
    sat_actor_profile: DataFrame,
    bridge: DataFrame,
    bsat_pr_lifecycle: DataFrame,
) -> DataFrame:
    activity = mas.select(
        "lhk_actor_repo", "event_id", "event_type", "push_size", "occurred_at"
    ).join(link_actor_repo.select("lhk_actor_repo", "hk_repo", "hk_actor"), "lhk_actor_repo")

    daily = (
        activity.withColumn("activity_date", F.to_date("occurred_at"))
        .groupBy("hk_actor", "activity_date")
        .agg(
            F.count("*").alias("events_count"),
            F.countDistinct("hk_repo").alias("repos_touched"),
            F.count(F.when(F.col("event_type") == "PushEvent", 1)).alias("pushes"),
            F.sum(F.when(F.col("event_type") == "PushEvent", F.col("push_size"))).alias(
                "commits_pushed"
            ),
            F.count(F.when(F.col("event_type") == "WatchEvent", 1)).alias("stars_given"),
            F.count(F.when(F.col("event_type") == "PullRequestEvent", 1)).alias("pr_events"),
            F.count(F.when(F.col("event_type") == "IssuesEvent", 1)).alias("issue_events"),
        )
    )

    pit = pit_actor_day.select(
        F.col("hk_actor").alias("_pit_hk"),
        "as_of_date",
        "sat_actor_profile_pit_ts",
    )
    profile = sat_actor_profile.select(
        F.col("hk_actor").alias("_prof_hk"),
        F.col("occurred_at").alias("_prof_ts"),
        "display_login",
        "avatar_url",
    )

    collab = (
        bridge.groupBy("hk_actor")
        .agg(
            F.countDistinct(F.when(F.col("item_type") == "pull_request", F.col("hk_item"))).alias(
                "prs_acted"
            ),
            F.countDistinct(F.when(F.col("item_type") == "issue", F.col("hk_item"))).alias(
                "issues_acted"
            ),
        )
        .withColumnRenamed("hk_actor", "_collab_hk")
    )

    merged_prs = (
        bridge.filter(
            (F.col("item_type") == "pull_request") & (F.col("relationship_type") == "pr_actor")
        )
        .select("hk_actor", F.col("hk_item").alias("hk_pull_request"))
        .join(
            bsat_pr_lifecycle.filter(F.col("is_merged")).select("hk_pull_request"),
            "hk_pull_request",
        )
        .groupBy("hk_actor")
        .agg(F.countDistinct("hk_pull_request").alias("prs_merged"))
        .withColumnRenamed("hk_actor", "_merged_hk")
    )

    return (
        daily.join(hub_actor.select("hk_actor", "actor_login"), "hk_actor")
        .join(
            pit,
            (F.col("hk_actor") == F.col("_pit_hk"))
            & (F.col("activity_date") == F.col("as_of_date")),
            "left",
        )
        .join(
            profile,
            (F.col("hk_actor") == F.col("_prof_hk"))
            & (F.col("sat_actor_profile_pit_ts") == F.col("_prof_ts")),
            "left",
        )
        .join(collab, F.col("hk_actor") == F.col("_collab_hk"), "left")
        .join(merged_prs, F.col("hk_actor") == F.col("_merged_hk"), "left")
        .select(
            "actor_login",
            "activity_date",
            "events_count",
            "repos_touched",
            "pushes",
            F.coalesce("commits_pushed", F.lit(0)).alias("commits_pushed"),
            "stars_given",
            "pr_events",
            "issue_events",
            "display_login",
            "avatar_url",
            F.coalesce("prs_acted", F.lit(0)).alias("prs_acted"),
            F.coalesce("issues_acted", F.lit(0)).alias("issues_acted"),
            F.coalesce("prs_merged", F.lit(0)).alias("prs_merged"),
            "hk_actor",
        )
    )
