"""Gold mart builders over a handcrafted mini-vault."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipelines.common.hashing import hash_hex
from pipelines.gold.collaboration import build_collaboration
from pipelines.gold.developer_360 import build_developer_360
from pipelines.gold.repo_activity import build_repo_activity

pytestmark = pytest.mark.spark


def _ts(day: int, hour: int = 12) -> datetime:
    return datetime(2026, 1, day, hour, 0, 0, tzinfo=UTC)


HK_REPO = hash_hex(["octo/alpha"])
HK_ALICE = hash_hex(["alice"])
HK_BOB = hash_hex(["bob"])
HK_PR1 = hash_hex(["octo/alpha", "1"])
HK_ISSUE7 = hash_hex(["octo/alpha", "7"])
LHK_ALICE = hash_hex(["alice", "octo/alpha"])
LHK_BOB = hash_hex(["bob", "octo/alpha"])

_MAS_SCHEMA = (
    "lhk_actor_repo string, event_id string, event_type string, push_size int, "
    "occurred_at timestamp"
)
_LINK_SCHEMA = "lhk_actor_repo string, hk_actor string, hk_repo string"
_BRIDGE_SCHEMA = (
    "hk_repo string, repo_name string, hk_actor string, actor_login string, "
    "item_type string, hk_item string, item_number int, relationship_type string"
)


@pytest.fixture()
def mini_vault(spark: SparkSession) -> dict[str, DataFrame]:
    mas = spark.createDataFrame(
        [
            (LHK_ALICE, "1", "PushEvent", 3, _ts(1, 9)),
            (LHK_BOB, "2", "WatchEvent", None, _ts(1, 10)),
            (LHK_ALICE, "3", "PullRequestEvent", None, _ts(2, 9)),
        ],
        _MAS_SCHEMA,
    )
    link = spark.createDataFrame(
        [(LHK_ALICE, HK_ALICE, HK_REPO), (LHK_BOB, HK_BOB, HK_REPO)], _LINK_SCHEMA
    )
    hub_repo = spark.createDataFrame([(HK_REPO, "octo/alpha")], "hk_repo string, repo_name string")
    hub_actor = spark.createDataFrame(
        [(HK_ALICE, "alice"), (HK_BOB, "bob")], "hk_actor string, actor_login string"
    )
    pit_repo = spark.createDataFrame(
        [
            (HK_REPO, _ts(1).date(), _ts(1, 8), _ts(1, 8)),
            (HK_REPO, _ts(2).date(), _ts(1, 8), _ts(2, 8)),
        ],
        "hk_repo string, as_of_date date, sat_repo_profile_pit_ts timestamp, "
        "sat_repo_stats_pit_ts timestamp",
    )
    sat_profile = spark.createDataFrame(
        [(HK_REPO, _ts(1, 8), "octocat", "Python", "mit", "main")],
        "hk_repo string, occurred_at timestamp, owner_login string, language string, "
        "license string, default_branch string",
    )
    sat_stats = spark.createDataFrame(
        [(HK_REPO, _ts(1, 8), 10, 2, 1, 5), (HK_REPO, _ts(2, 8), 12, 2, 1, 6)],
        "hk_repo string, occurred_at timestamp, stargazers_count long, forks_count long, "
        "open_issues_count long, watchers_count long",
    )
    pit_actor = spark.createDataFrame(
        [
            (HK_ALICE, _ts(1).date(), _ts(1, 8)),
            (HK_ALICE, _ts(2).date(), _ts(1, 8)),
            (HK_BOB, _ts(1).date(), _ts(1, 8)),
        ],
        "hk_actor string, as_of_date date, sat_actor_profile_pit_ts timestamp",
    )
    sat_actor_profile = spark.createDataFrame(
        [
            (HK_ALICE, _ts(1, 8), "alice", "https://a/alice"),
            (HK_BOB, _ts(1, 8), "bob", "https://a/bob"),
        ],
        "hk_actor string, occurred_at timestamp, display_login string, avatar_url string",
    )
    bridge = spark.createDataFrame(
        [
            (HK_REPO, "octo/alpha", HK_ALICE, "alice", "pull_request", HK_PR1, 1, "pr_actor"),
            (HK_REPO, "octo/alpha", HK_BOB, "bob", "issue", HK_ISSUE7, 7, "issue_actor"),
            (HK_REPO, "octo/alpha", HK_ALICE, "alice", "issue", HK_ISSUE7, 7, "issue_assignee"),
        ],
        _BRIDGE_SCHEMA,
    )
    pr_lifecycle = spark.createDataFrame(
        [(HK_PR1, _ts(2, 9), _ts(3, 9), _ts(3, 9), True, 24.0, _ts(3, 9))],
        "hk_pull_request string, opened_at timestamp, closed_at timestamp, "
        "merged_at timestamp, is_merged boolean, cycle_time_hours double, "
        "last_activity_at timestamp",
    )
    issue_lifecycle = spark.createDataFrame(
        [(HK_ISSUE7, _ts(1, 9), None, None, 1, HK_ALICE, _ts(2, 9))],
        "hk_issue string, opened_at timestamp, closed_at timestamp, "
        "time_to_close_hours double, assignee_change_count int, active_assignee_hk string, "
        "last_activity_at timestamp",
    )
    pit_pr = spark.createDataFrame(
        [(HK_PR1, _ts(2).date(), _ts(2, 9)), (HK_PR1, _ts(3).date(), _ts(3, 9))],
        "hk_pull_request string, as_of_date date, sat_pull_request_details_pit_ts timestamp",
    )
    sat_pr_details = spark.createDataFrame(
        [
            (HK_PR1, _ts(2, 9), "Add feature", "open"),
            (HK_PR1, _ts(3, 9), "Add feature", "closed"),
        ],
        "hk_pull_request string, occurred_at timestamp, pr_title string, pr_state string",
    )
    return {
        "mas": mas,
        "link": link,
        "hub_repo": hub_repo,
        "hub_actor": hub_actor,
        "pit_repo": pit_repo,
        "sat_profile": sat_profile,
        "sat_stats": sat_stats,
        "pit_actor": pit_actor,
        "sat_actor_profile": sat_actor_profile,
        "bridge": bridge,
        "pr_lifecycle": pr_lifecycle,
        "issue_lifecycle": issue_lifecycle,
        "pit_pr": pit_pr,
        "sat_pr_details": sat_pr_details,
    }


def test_repo_activity_mart(mini_vault: dict[str, DataFrame]) -> None:
    mart = build_repo_activity(
        mas=mini_vault["mas"],
        link_actor_repo=mini_vault["link"],
        hub_repo=mini_vault["hub_repo"],
        pit_repo_day=mini_vault["pit_repo"],
        sat_repo_profile=mini_vault["sat_profile"],
        sat_repo_stats=mini_vault["sat_stats"],
    )
    rows = {(str(r["activity_date"]), r["event_type"]): r for r in mart.collect()}
    assert mart.count() == 3  # grain: repo x day x event type
    assert mart.groupBy("repo_name", "activity_date", "event_type").count().count() == 3

    push = rows[("2026-01-01", "PushEvent")]
    assert push["events_count"] == 1
    assert push["push_commits"] == 3
    assert push["stargazers_count"] == 10  # day-1 stats state
    pr = rows[("2026-01-02", "PullRequestEvent")]
    assert pr["stargazers_count"] == 12  # day-2 stats state via PIT
    assert pr["repo_language"] == "Python"


def test_developer_360_mart(mini_vault: dict[str, DataFrame]) -> None:
    mart = build_developer_360(
        mas=mini_vault["mas"],
        link_actor_repo=mini_vault["link"],
        hub_actor=mini_vault["hub_actor"],
        pit_actor_day=mini_vault["pit_actor"],
        sat_actor_profile=mini_vault["sat_actor_profile"],
        bridge=mini_vault["bridge"],
        bsat_pr_lifecycle=mini_vault["pr_lifecycle"],
    )
    assert mart.count() == 3  # alice day1, alice day2, bob day1
    assert mart.groupBy("actor_login", "activity_date").count().count() == 3

    by_key = {(r["actor_login"], str(r["activity_date"])): r for r in mart.collect()}
    alice_d1 = by_key[("alice", "2026-01-01")]
    assert alice_d1["pushes"] == 1
    assert alice_d1["commits_pushed"] == 3
    assert alice_d1["prs_acted"] == 1
    assert alice_d1["prs_merged"] == 1
    assert alice_d1["display_login"] == "alice"
    bob_d1 = by_key[("bob", "2026-01-01")]
    assert bob_d1["stars_given"] == 1
    assert bob_d1["prs_merged"] == 0


def test_collaboration_mart(mini_vault: dict[str, DataFrame]) -> None:
    mart = build_collaboration(
        bridge=mini_vault["bridge"],
        bsat_pr_lifecycle=mini_vault["pr_lifecycle"],
        bsat_issue_lifecycle=mini_vault["issue_lifecycle"],
        pit_pull_request_day=mini_vault["pit_pr"],
        sat_pull_request_details=mini_vault["sat_pr_details"],
    )
    assert mart.count() == 2  # one PR item + one issue item
    assert mart.groupBy("item_type", "repo_name", "item_number").count().count() == 2

    by_item = {r["item_type"]: r for r in mart.collect()}
    pr = by_item["pull_request"]
    assert pr["participants"] == 1
    assert pr["is_merged"] is True
    assert pr["resolution_hours"] == pytest.approx(24.0)
    assert pr["pr_state"] == "closed"  # latest PIT pointer resolves current state

    issue = by_item["issue"]
    assert issue["participants"] == 1  # bob acted; assignee rows don't count as actors
    assert issue["active_assignee"] == "alice"
    assert issue["assignee_change_count"] == 1
    assert issue["closed_at"] is None


def test_marts_expose_non_null_grain_keys(mini_vault: dict[str, DataFrame]) -> None:
    mart = build_repo_activity(
        mas=mini_vault["mas"],
        link_actor_repo=mini_vault["link"],
        hub_repo=mini_vault["hub_repo"],
        pit_repo_day=mini_vault["pit_repo"],
        sat_repo_profile=mini_vault["sat_profile"],
        sat_repo_stats=mini_vault["sat_stats"],
    )
    grain_nulls = mart.filter(
        F.col("repo_name").isNull() | F.col("activity_date").isNull() | F.col("event_type").isNull()
    ).count()
    assert grain_nulls == 0
