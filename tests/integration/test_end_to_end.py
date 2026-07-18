"""End-to-end flow: seeded events → bronze → raw vault (idempotency proof).

Extended by later layers: business vault and gold marts build on the vault
this test populates.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ingestion.github_archive.generator import GeneratorConfig, run_generator
from pipelines.bronze.copy_into import events_table_path, run_copy_into
from pipelines.business_vault.job import run_build as build_business_vault
from pipelines.common.config import LakehouseConfig
from pipelines.gold.job import run_build as build_gold
from pipelines.raw_vault.job import run_load
from pipelines.raw_vault.loaders import table_path
from tests.conftest import make_config

pytestmark = [pytest.mark.spark, pytest.mark.integration]

EVENTS = 400
SEED = 42


@pytest.fixture(scope="module")
def vault(
    tmp_path_factory: pytest.TempPathFactory, spark: SparkSession
) -> tuple[LakehouseConfig, dict[str, int]]:
    config = make_config(tmp_path_factory.mktemp("e2e"))
    run_generator(
        GeneratorConfig(
            events=EVENTS, seed=SEED, corrupt_pct=3.0, landing_dir=config.source.landing_dir
        )
    )
    run_copy_into(config, spark)
    inserted = run_load(config, spark)
    return config, inserted


def _read(spark: SparkSession, config: LakehouseConfig, name: str) -> DataFrame:
    return spark.read.format("delta").load(table_path(config, "raw_vault", name))


def _landing_events(config: LakehouseConfig) -> list[dict[str, Any]]:
    events = []
    for path in sorted(config.source.landing_dir.glob("*.ndjson")):
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if "id" in event and "created_at" in event:
                events.append(event)
    return events


def test_hub_counts_match_distinct_business_keys(
    spark: SparkSession, vault: tuple[LakehouseConfig, dict[str, int]]
) -> None:
    config, _ = vault
    events = _landing_events(config)

    actors = {e["actor"]["login"] for e in events}
    for e in events:
        if e["type"] == "IssuesEvent":
            assignee = e["payload"].get("assignee")
            if assignee:
                actors.add(assignee["login"])
    repos = {e["repo"]["name"] for e in events}
    for e in events:
        if e["type"] == "ForkEvent":
            repos.add(e["payload"]["forkee"]["full_name"])

    assert _read(spark, config, "hub_actor").count() == len(actors)
    assert _read(spark, config, "hub_repo").count() == len(repos)

    prs = {
        (e["repo"]["name"], e["payload"]["number"])
        for e in events
        if e["type"] == "PullRequestEvent"
    }
    assert _read(spark, config, "hub_pull_request").count() == len(prs)


def test_mas_reconciles_with_bronze(
    spark: SparkSession, vault: tuple[LakehouseConfig, dict[str, int]]
) -> None:
    """Every valid bronze event becomes exactly one multi-active satellite row."""
    config, _ = vault
    bronze_count = spark.read.format("delta").load(events_table_path(config)).count()
    mas_count = _read(spark, config, "sat_actor_repo_event").count()
    assert mas_count == bronze_count


def test_rerun_inserts_zero_rows_everywhere(
    spark: SparkSession, vault: tuple[LakehouseConfig, dict[str, int]]
) -> None:
    config, first = vault
    assert any(count > 0 for count in first.values())
    second = run_load(config, spark)
    assert second == dict.fromkeys(first, 0)


def test_link_referential_integrity(
    spark: SparkSession, vault: tuple[LakehouseConfig, dict[str, int]]
) -> None:
    config, _ = vault
    hub_actor = _read(spark, config, "hub_actor").select("hk_actor")
    hub_repo = _read(spark, config, "hub_repo").select("hk_repo")
    link = _read(spark, config, "link_actor_repo")
    orphans_actor = link.join(hub_actor, "hk_actor", "left_anti").count()
    orphans_repo = link.join(hub_repo, "hk_repo", "left_anti").count()
    assert orphans_actor == 0
    assert orphans_repo == 0

    assignee_link = _read(spark, config, "link_issue_assignee")
    assert assignee_link.join(hub_actor, "hk_actor", "left_anti").count() == 0


def test_satellites_carry_mandatory_metadata(
    spark: SparkSession, vault: tuple[LakehouseConfig, dict[str, int]]
) -> None:
    config, _ = vault
    for sat in ("sat_actor_profile", "sat_repo_stats", "sat_pull_request_details"):
        df = _read(spark, config, sat)
        assert {"load_dts", "record_source", "hash_diff"} <= set(df.columns)
        assert df.filter(F.col("hash_diff").isNull()).count() == 0

    eff = _read(spark, config, "eff_sat_issue_assignee")
    assert {"lhk_issue_assignee", "hk_issue", "hk_actor", "start_dts", "end_dts"} <= set(
        eff.columns
    )
    assert eff.count() > 0


def test_business_vault_and_gold_build(
    spark: SparkSession, vault: tuple[LakehouseConfig, dict[str, int]]
) -> None:
    """The derived layers build on the vault and hold their documented grains."""
    config, _ = vault
    bv_counts = build_business_vault(config, spark)
    assert all(count > 0 for count in bv_counts.values()), bv_counts

    gold_counts = build_gold(config, spark)
    assert all(count > 0 for count in gold_counts.values()), gold_counts

    for mart in config.marts:
        df = spark.read.format("delta").load(table_path(config, "gold", mart.name))
        assert df.count() == df.select(*mart.grain).distinct().count(), (
            f"{mart.name} grain {mart.grain} is not unique"
        )
        nulls = sum(df.filter(F.col(c).isNull()).count() for c in mart.grain)
        assert nulls == 0, f"{mart.name} has null grain keys"
