"""Spark ↔ pure-Python hashing parity — the guarantee that lets unit tests
and non-Spark tools reason about vault keys without a JVM."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from pipelines.common.hashing import hash_diff, hash_hex, hash_key

pytestmark = pytest.mark.spark


def test_hash_key_matches_pure_python(spark: SparkSession) -> None:
    rows = [
        ("octocat", "octo/repo"),
        ("  Padded  ", "MixedCase/Name"),
        (None, "repo-only"),
        ("", "empty-login"),
    ]
    df = spark.createDataFrame(rows, ["actor_login", "repo_name"])
    got = [r["hk"] for r in df.select(hash_key(["actor_login", "repo_name"]).alias("hk")).collect()]
    expected = [hash_hex([login, repo]) for login, repo in rows]
    assert got == expected


def test_hash_diff_matches_pure_python(spark: SparkSession) -> None:
    rows = [("Python", "main", None, "42")]
    df = spark.createDataFrame(rows, "language string, branch string, license string, stars string")
    got = df.select(hash_diff(["language", "branch", "license", "stars"]).alias("hd")).first()
    assert got is not None
    assert got["hd"] == hash_hex(["Python", "main", None, "42"])


def test_numeric_components_cast_like_python_strings(spark: SparkSession) -> None:
    df = spark.createDataFrame([("octo/repo", 42)], ["repo_name", "pr_number"])
    got = df.select(hash_key(["repo_name", "pr_number"]).alias("hk")).first()
    assert got is not None
    assert got["hk"] == hash_hex(["octo/repo", "42"])
