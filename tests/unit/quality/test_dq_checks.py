"""DQ check implementations and suite loading."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from quality.expectations.checks import run_check
from quality.expectations.suite import (
    NotNullCheck,
    ReferentialIntegrityCheck,
    RowCountMatchCheck,
    UniqueCheck,
    load_suites,
)


def test_all_suites_load_and_cover_every_vault_object() -> None:
    suites = load_suites()
    tables = {s.table for s in suites}
    assert "bronze/github_events" in tables
    for hub in ("hub_actor", "hub_repo", "hub_pull_request", "hub_issue"):
        assert f"raw_vault/{hub}" in tables
    for link in (
        "link_actor_repo",
        "link_actor_pull_request",
        "link_actor_issue",
        "link_issue_assignee",
    ):
        assert f"raw_vault/{link}" in tables
    for mart in ("repo_activity_mart", "developer_360_mart", "collaboration_mart"):
        assert f"gold/{mart}" in tables


def test_reconciliation_suite_targets_bronze() -> None:
    suites = {s.table: s for s in load_suites()}
    mas_suite = suites["raw_vault/sat_actor_repo_event"]
    match = [c for c in mas_suite.checks if isinstance(c, RowCountMatchCheck)]
    assert len(match) == 1
    assert match[0].ref_table == "bronze/github_events"
    assert match[0].tolerance == 0


@pytest.mark.spark
def test_shape_checks(spark: SparkSession) -> None:
    df = spark.createDataFrame([("a", 1), ("b", 2), ("b", 3), (None, 4)], "key string, value int")
    not_null = run_check(spark, df, NotNullCheck(type="not_null", column="key"), {})
    assert not not_null.passed
    assert "1 null" in not_null.observed

    unique = run_check(spark, df, UniqueCheck(type="unique", columns=["key"]), {})
    assert not unique.passed  # (b twice) + (None... distinct counts null once)


@pytest.mark.spark
def test_referential_integrity_check(spark: SparkSession) -> None:
    child = spark.createDataFrame([("k1",), ("k2",), ("orphan",)], "fk string")
    parent = spark.createDataFrame([("k1",), ("k2",)], "pk string")
    check = ReferentialIntegrityCheck(
        type="referential_integrity", column="fk", ref_table="parent", ref_column="pk"
    )
    result = run_check(spark, child, check, {"parent": parent})
    assert not result.passed
    assert "1 orphaned" in result.observed


@pytest.mark.spark
def test_row_count_match_check(spark: SparkSession) -> None:
    left = spark.createDataFrame([(i,) for i in range(5)], "v int")
    ref = spark.createDataFrame([(i,) for i in range(5)], "v int")
    check = RowCountMatchCheck(type="row_count_match", ref_table="ref")
    assert run_check(spark, left, check, {"ref": ref}).passed

    ref_short = spark.createDataFrame([(i,) for i in range(3)], "v int")
    result = run_check(spark, left, check, {"ref": ref_short})
    assert not result.passed
    assert "drift 2" in result.observed

    tolerant = RowCountMatchCheck(type="row_count_match", ref_table="ref", tolerance=2)
    assert run_check(spark, left, tolerant, {"ref": ref_short}).passed
