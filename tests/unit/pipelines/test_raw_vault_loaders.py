"""Generic raw-vault loader semantics over handcrafted staged frames."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipelines.common.config import LakehouseConfig
from pipelines.raw_vault.loaders import (
    HIGH_DTS,
    load_effectivity_satellite,
    load_hubs,
    load_links,
    load_multi_active_satellite,
    load_standard_satellite,
    table_path,
)

pytestmark = pytest.mark.spark


def _ts(minute: int) -> datetime:
    return datetime(2026, 1, 1, 12, minute, 0, tzinfo=UTC)


_WATCH_SCHEMA = (
    "event_id string, actor_login string, actor_display_login string, "
    "actor_avatar_url string, repo_name string, created_at timestamp, action string, "
    "record_source string, occurred_at timestamp"
)

_PR_SCHEMA = (
    "event_id string, actor_login string, actor_display_login string, "
    "actor_avatar_url string, repo_name string, created_at timestamp, action string, "
    "pr_number int, pr_title string, pr_state string, pr_merged boolean, base_ref string, "
    "head_ref string, base_repo_owner string, base_repo_language string, "
    "base_repo_default_branch string, base_repo_license string, "
    "base_repo_created_at timestamp, base_repo_stargazers bigint, base_repo_forks bigint, "
    "base_repo_open_issues bigint, base_repo_watchers bigint, "
    "record_source string, occurred_at timestamp"
)

_ISSUES_SCHEMA = (
    "event_id string, actor_login string, actor_display_login string, "
    "actor_avatar_url string, repo_name string, created_at timestamp, action string, "
    "issue_number int, issue_title string, issue_state string, issue_comments int, "
    "assignee_login string, record_source string, occurred_at timestamp"
)


def _watch(spark: SparkSession, rows: list[tuple]) -> DataFrame:  # type: ignore[type-arg]
    return spark.createDataFrame(rows, _WATCH_SCHEMA)


def _watch_row(event_id: str, actor: str, repo: str, minute: int) -> tuple:  # type: ignore[type-arg]
    ts = _ts(minute)
    return (
        event_id,
        actor,
        actor,
        f"https://a/{actor}",
        repo,
        ts,
        "started",
        "gharchive.WatchEvent",
        ts,
    )


def _pr_row(event_id: str, actor: str, repo: str, minute: int, pr_number: int, stars: int) -> tuple:  # type: ignore[type-arg]
    ts = _ts(minute)
    return (
        event_id,
        actor,
        actor,
        f"https://a/{actor}",
        repo,
        ts,
        "opened",
        pr_number,
        f"PR {pr_number}",
        "open",
        False,
        "main",
        f"feat-{pr_number}",
        "octocat",
        "Python",
        "main",
        "mit",
        _ts(0),
        stars,
        3,
        1,
        9,
        "gharchive.PullRequestEvent",
        ts,
    )


def _issue_row(
    event_id: str,
    actor: str,
    repo: str,
    minute: int,
    number: int,
    action: str,
    assignee: str | None,
) -> tuple:  # type: ignore[type-arg]
    ts = _ts(minute)
    return (
        event_id,
        actor,
        actor,
        f"https://a/{actor}",
        repo,
        ts,
        action,
        number,
        f"Issue {number}",
        "open",
        0,
        assignee,
        "gharchive.IssuesEvent",
        ts,
    )


def _read(spark: SparkSession, config: LakehouseConfig, name: str) -> DataFrame:
    return spark.read.format("delta").load(table_path(config, "raw_vault", name))


def test_hub_and_link_double_load_is_noop(
    spark: SparkSession, lakehouse_config: LakehouseConfig
) -> None:
    staged = {
        "WatchEvent": _watch(
            spark,
            [
                _watch_row("1", "alice", "octo/alpha", 1),
                _watch_row("2", "bob", "octo/beta", 2),
                _watch_row("3", "alice", "octo/beta", 3),
            ],
        )
    }
    first = load_hubs(spark, lakehouse_config, staged)
    assert first["hub_actor"] == 2
    assert first["hub_repo"] == 2
    assert first["hub_pull_request"] == 0  # no PR events staged

    first_links = load_links(spark, lakehouse_config, staged)
    assert first_links["link_actor_repo"] == 3  # alice-alpha, bob-beta, alice-beta

    assert load_hubs(spark, lakehouse_config, staged) == dict.fromkeys(first, 0)
    assert load_links(spark, lakehouse_config, staged) == dict.fromkeys(first_links, 0)

    hub_actor = _read(spark, lakehouse_config, "hub_actor")
    assert {r["actor_login"] for r in hub_actor.collect()} == {"alice", "bob"}
    assert {len(r["hk_actor"]) for r in hub_actor.collect()} == {64}


def test_standard_satellite_hash_diff_gate(
    spark: SparkSession, lakehouse_config: LakehouseConfig
) -> None:
    rows = [
        _pr_row("1", "alice", "octo/alpha", 1, 1, stars=10),
        _pr_row("2", "bob", "octo/alpha", 2, 2, stars=20),  # stats changed
        _pr_row("3", "carol", "octo/alpha", 3, 3, stars=20),  # unchanged -> no new state
    ]
    staged = {"PullRequestEvent": spark.createDataFrame(rows, _PR_SCHEMA)}
    assert load_standard_satellite(spark, lakehouse_config, "sat_repo_stats", staged) == 2
    # replay: no-op
    assert load_standard_satellite(spark, lakehouse_config, "sat_repo_stats", staged) == 0

    # a genuinely new state inserts exactly one row
    rows.append(_pr_row("4", "dave", "octo/alpha", 4, 4, stars=30))
    staged = {"PullRequestEvent": spark.createDataFrame(rows, _PR_SCHEMA)}
    assert load_standard_satellite(spark, lakehouse_config, "sat_repo_stats", staged) == 1

    sat = _read(spark, lakehouse_config, "sat_repo_stats")
    assert sat.count() == 3
    assert {r["stargazers_count"] for r in sat.collect()} == {10, 20, 30}
    assert sat.select("hash_diff").distinct().count() == 3
    mandatory = {"hk_repo", "load_dts", "record_source", "hash_diff"}
    assert mandatory <= set(sat.columns)


def test_multi_active_satellite_one_row_per_event(
    spark: SparkSession, lakehouse_config: LakehouseConfig
) -> None:
    staged = {
        "WatchEvent": _watch(
            spark,
            [
                _watch_row("10", "alice", "octo/alpha", 1),
                _watch_row("11", "alice", "octo/alpha", 2),  # same link, new event
                _watch_row("11", "alice", "octo/alpha", 2),  # exact duplicate (resent file)
            ],
        )
    }
    assert load_multi_active_satellite(spark, lakehouse_config, "sat_actor_repo_event", staged) == 2
    assert load_multi_active_satellite(spark, lakehouse_config, "sat_actor_repo_event", staged) == 0
    mas = _read(spark, lakehouse_config, "sat_actor_repo_event")
    assert mas.count() == 2
    # multiple concurrently-valid rows share one link hash key
    assert mas.select("lhk_actor_repo").distinct().count() == 1


def test_effectivity_satellite_closes_previous_assignment(
    spark: SparkSession, lakehouse_config: LakehouseConfig
) -> None:
    issues = [_issue_row("20", "maint", "octo/alpha", 1, 7, "assigned", "alice")]
    staged = {"IssuesEvent": spark.createDataFrame(issues, _ISSUES_SCHEMA)}
    assert (
        load_effectivity_satellite(spark, lakehouse_config, "eff_sat_issue_assignee", staged) == 1
    )
    eff = _read(spark, lakehouse_config, "eff_sat_issue_assignee")
    row = eff.first()
    assert row is not None
    assert str(row["end_dts"]).startswith("9999-12-31")

    # reassignment: alice's interval closes at bob's start; replay is a no-op
    issues.append(_issue_row("21", "maint", "octo/alpha", 5, 7, "assigned", "bob"))
    staged = {"IssuesEvent": spark.createDataFrame(issues, _ISSUES_SCHEMA)}
    assert (
        load_effectivity_satellite(spark, lakehouse_config, "eff_sat_issue_assignee", staged) == 2
    )
    assert (
        load_effectivity_satellite(spark, lakehouse_config, "eff_sat_issue_assignee", staged) == 0
    )

    eff = _read(spark, lakehouse_config, "eff_sat_issue_assignee")
    assert eff.count() == 3  # alice-open (superseded), alice-closed, bob-open

    # resolution: per (lhk, start_dts) the earliest end wins; then only bob is open
    resolved = (
        eff.groupBy("lhk_issue_assignee", "hk_issue", "hk_actor", "start_dts")
        .agg(F.min("end_dts").alias("end_dts"))
        .filter(F.col("end_dts") == F.lit(HIGH_DTS).cast("timestamp"))
    )
    assert resolved.count() == 1

    # unassigned closes without opening a new interval
    issues.append(_issue_row("22", "maint", "octo/alpha", 9, 7, "unassigned", "bob"))
    staged = {"IssuesEvent": spark.createDataFrame(issues, _ISSUES_SCHEMA)}
    assert (
        load_effectivity_satellite(spark, lakehouse_config, "eff_sat_issue_assignee", staged) == 1
    )
    eff = _read(spark, lakehouse_config, "eff_sat_issue_assignee")
    resolved = (
        eff.groupBy("lhk_issue_assignee", "start_dts")
        .agg(F.min("end_dts").alias("end_dts"))
        .filter(F.col("end_dts") == F.lit(HIGH_DTS).cast("timestamp"))
    )
    assert resolved.count() == 0
