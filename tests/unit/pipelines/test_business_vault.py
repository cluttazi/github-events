"""Business-vault pure-function tests over handcrafted vault frames."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipelines.business_vault.bridge import bridge_repo_collaboration
from pipelines.business_vault.derived import (
    issue_lifecycle_from,
    pr_lifecycle_from_details,
    resolve_effectivity,
)
from pipelines.business_vault.pit import GHOST_DTS, date_spine, pit_from_satellites
from pipelines.common.hashing import hash_hex

pytestmark = pytest.mark.spark


def _ts(day: int, hour: int = 12) -> datetime:
    return datetime(2026, 1, day, hour, 0, 0, tzinfo=UTC)


HIGH = datetime(9999, 12, 31, 0, 0, 0, tzinfo=UTC)

HK_REPO = hash_hex(["octo/alpha"])
HK_ACTOR_A = hash_hex(["alice"])
HK_ACTOR_B = hash_hex(["bob"])
HK_ISSUE = hash_hex(["octo/alpha", "7"])
HK_PR = hash_hex(["octo/alpha", "1"])


def test_resolve_effectivity_closed_supersedes_open(spark: SparkSession) -> None:
    lhk_a = hash_hex(["octo/alpha", "7", "alice"])
    lhk_b = hash_hex(["octo/alpha", "7", "bob"])
    rows = [
        (lhk_a, HK_ISSUE, HK_ACTOR_A, _ts(1), HIGH),  # open (superseded later)
        (lhk_a, HK_ISSUE, HK_ACTOR_A, _ts(1), _ts(3)),  # closed version
        (lhk_b, HK_ISSUE, HK_ACTOR_B, _ts(3), HIGH),  # currently active
    ]
    eff = spark.createDataFrame(
        rows,
        "lhk_issue_assignee string, hk_issue string, hk_actor string, "
        "start_dts timestamp, end_dts timestamp",
    )
    resolved = resolve_effectivity(eff)
    assert resolved.count() == 2  # one row per (lhk, start)
    active = resolved.filter(F.col("is_active")).collect()
    assert len(active) == 1
    assert active[0]["hk_actor"] == HK_ACTOR_B


def test_pr_lifecycle_milestones_and_cycle_time(spark: SparkSession) -> None:
    rows = [
        (HK_PR, "opened", False, _ts(1, 10)),
        (HK_PR, "closed", True, _ts(2, 10)),  # merged close, 24h later
    ]
    details = spark.createDataFrame(
        rows, "hk_pull_request string, action string, pr_merged boolean, occurred_at timestamp"
    )
    lifecycle = pr_lifecycle_from_details(details).collect()[0]
    assert lifecycle["opened_at"] == _ts(1, 10).replace(tzinfo=None)
    assert lifecycle["is_merged"] is True
    assert lifecycle["cycle_time_hours"] == pytest.approx(24.0)


def test_issue_lifecycle_includes_assignment_churn(spark: SparkSession) -> None:
    details = spark.createDataFrame(
        [(HK_ISSUE, "opened", _ts(1)), (HK_ISSUE, "closed", _ts(4))],
        "hk_issue string, action string, occurred_at timestamp",
    )
    resolved = spark.createDataFrame(
        [
            (hash_hex(["octo/alpha", "7", "alice"]), HK_ISSUE, HK_ACTOR_A, _ts(2), _ts(3), False),
            (hash_hex(["octo/alpha", "7", "bob"]), HK_ISSUE, HK_ACTOR_B, _ts(3), HIGH, True),
        ],
        "lhk_issue_assignee string, hk_issue string, hk_actor string, "
        "start_dts timestamp, end_dts timestamp, is_active boolean",
    )
    lifecycle = issue_lifecycle_from(details, resolved).collect()[0]
    assert lifecycle["time_to_close_hours"] == pytest.approx(72.0)
    assert lifecycle["assignee_change_count"] == 2
    assert lifecycle["active_assignee_hk"] == HK_ACTOR_B


def test_pit_pointers_and_ghost_records(spark: SparkSession) -> None:
    hub = spark.createDataFrame([(HK_REPO, "octo/alpha")], "hk_repo string, repo_name string")
    stats = spark.createDataFrame(
        [(HK_REPO, "s1", _ts(2, 9)), (HK_REPO, "s2", _ts(3, 9))],
        "hk_repo string, hash_diff string, occurred_at timestamp",
    )
    activity = spark.createDataFrame([(_ts(1),), (_ts(2),), (_ts(3),)], "occurred_at timestamp")
    spine = date_spine(activity)
    assert spine.count() == 3

    pit = pit_from_satellites(hub, "hk_repo", {"sat_repo_stats": stats}, spine)
    assert pit.count() == 3  # 1 repo x 3 days
    by_day = {str(r["as_of_date"]): r["sat_repo_stats_pit_ts"] for r in pit.collect()}
    # day 1: no state yet -> ghost; day 2: s1; day 3: s2
    assert str(by_day["2026-01-01"]).startswith("1900-01-01")
    assert by_day["2026-01-02"] == _ts(2, 9).replace(tzinfo=None)
    assert by_day["2026-01-03"] == _ts(3, 9).replace(tzinfo=None)
    assert GHOST_DTS.startswith("1900-01-01")


def _df(spark: SparkSession, rows: list[tuple], schema: str) -> DataFrame:  # type: ignore[type-arg]
    return spark.createDataFrame(rows, schema)


def test_bridge_traversals(spark: SparkSession) -> None:
    hub_actor = _df(
        spark,
        [(HK_ACTOR_A, "alice"), (HK_ACTOR_B, "bob")],
        "hk_actor string, actor_login string",
    )
    hub_repo = _df(spark, [(HK_REPO, "octo/alpha")], "hk_repo string, repo_name string")
    hub_pr = _df(
        spark,
        [(HK_PR, "octo/alpha", 1)],
        "hk_pull_request string, repo_name string, pr_number int",
    )
    hub_issue = _df(
        spark,
        [(HK_ISSUE, "octo/alpha", 7)],
        "hk_issue string, repo_name string, issue_number int",
    )
    link_pr = _df(
        spark,
        [(hash_hex(["alice", "octo/alpha", "1", "octo/alpha"]), HK_ACTOR_A, HK_PR, HK_REPO)],
        "lhk_actor_pull_request string, hk_actor string, hk_pull_request string, hk_repo string",
    )
    link_issue = _df(
        spark,
        [(hash_hex(["bob", "octo/alpha", "7", "octo/alpha"]), HK_ACTOR_B, HK_ISSUE, HK_REPO)],
        "lhk_actor_issue string, hk_actor string, hk_issue string, hk_repo string",
    )
    resolved = _df(
        spark,
        [(hash_hex(["octo/alpha", "7", "alice"]), HK_ISSUE, HK_ACTOR_A, _ts(2), HIGH, True)],
        "lhk_issue_assignee string, hk_issue string, hk_actor string, "
        "start_dts timestamp, end_dts timestamp, is_active boolean",
    )

    bridge = bridge_repo_collaboration(
        hub_actor, hub_repo, hub_pr, hub_issue, link_pr, link_issue, resolved
    )
    rows = {(r["relationship_type"], r["actor_login"], r["item_type"]) for r in bridge.collect()}
    assert rows == {
        ("pr_actor", "alice", "pull_request"),
        ("issue_actor", "bob", "issue"),
        ("issue_assignee", "alice", "issue"),
    }
    # every row resolved to the same repo, and item numbers survive traversal
    assert {r["repo_name"] for r in bridge.collect()} == {"octo/alpha"}
    assert {r["item_number"] for r in bridge.collect()} == {1, 7}
