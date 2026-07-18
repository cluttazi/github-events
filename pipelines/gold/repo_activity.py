"""repo_activity_mart — repository activity fact at repo x day x event type.

Grain: one row per ``(repo_name, activity_date, event_type)``.

Fact measures come from the multi-active satellite (the event stream) via
``link_actor_repo``; dimension attributes are the repo state *as of that
day*, resolved through ``pit_repo_day`` pointers into the profile and stats
satellites (PIT-driven SCD2-style as-of joins — no window logic here).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_repo_activity(
    mas: DataFrame,
    link_actor_repo: DataFrame,
    hub_repo: DataFrame,
    pit_repo_day: DataFrame,
    sat_repo_profile: DataFrame,
    sat_repo_stats: DataFrame,
) -> DataFrame:
    activity = mas.select(
        "lhk_actor_repo", "event_id", "event_type", "push_size", "occurred_at"
    ).join(link_actor_repo.select("lhk_actor_repo", "hk_repo", "hk_actor"), "lhk_actor_repo")

    facts = (
        activity.withColumn("activity_date", F.to_date("occurred_at"))
        .groupBy("hk_repo", "activity_date", "event_type")
        .agg(
            F.count("*").alias("events_count"),
            F.countDistinct("hk_actor").alias("distinct_actors"),
            F.sum("push_size").alias("push_commits"),
        )
    )

    pit = pit_repo_day.select(
        F.col("hk_repo").alias("_pit_hk"),
        F.col("as_of_date"),
        F.col("sat_repo_profile_pit_ts"),
        F.col("sat_repo_stats_pit_ts"),
    )
    profile = sat_repo_profile.select(
        F.col("hk_repo").alias("_prof_hk"),
        F.col("occurred_at").alias("_prof_ts"),
        F.col("owner_login").alias("repo_owner"),
        F.col("language").alias("repo_language"),
        F.col("license").alias("repo_license"),
        F.col("default_branch").alias("repo_default_branch"),
    )
    stats = sat_repo_stats.select(
        F.col("hk_repo").alias("_stat_hk"),
        F.col("occurred_at").alias("_stat_ts"),
        "stargazers_count",
        "forks_count",
        "open_issues_count",
        "watchers_count",
    )

    return (
        facts.join(hub_repo.select("hk_repo", "repo_name"), "hk_repo")
        .join(
            pit,
            (F.col("hk_repo") == F.col("_pit_hk"))
            & (F.col("activity_date") == F.col("as_of_date")),
            "left",
        )
        .join(
            profile,
            (F.col("hk_repo") == F.col("_prof_hk"))
            & (F.col("sat_repo_profile_pit_ts") == F.col("_prof_ts")),
            "left",
        )
        .join(
            stats,
            (F.col("hk_repo") == F.col("_stat_hk"))
            & (F.col("sat_repo_stats_pit_ts") == F.col("_stat_ts")),
            "left",
        )
        .select(
            "repo_name",
            "activity_date",
            "event_type",
            "events_count",
            "distinct_actors",
            F.coalesce("push_commits", F.lit(0)).alias("push_commits"),
            "repo_owner",
            "repo_language",
            "repo_license",
            "repo_default_branch",
            "stargazers_count",
            "forks_count",
            "open_issues_count",
            "watchers_count",
            F.col("hk_repo"),
        )
    )
