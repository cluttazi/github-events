"""Shared fixtures: one local SparkSession per test session, tmp lakehouse configs."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from pipelines.common.config import (
    HubConfig,
    LakehouseConfig,
    LinkConfig,
    MartConfig,
    SatelliteConfig,
    SourceConfig,
    SparkConfig,
    StorageConfig,
    VaultConfig,
)
from pipelines.common.session import get_spark

DEFAULT_EVENT_TYPES = [
    "PushEvent",
    "PullRequestEvent",
    "IssuesEvent",
    "WatchEvent",
    "ForkEvent",
    "ReleaseEvent",
]

DEFAULT_VAULT = VaultConfig(
    record_source_prefix="gharchive",
    hubs=[
        HubConfig(name="hub_actor", business_keys=["actor_login"]),
        HubConfig(name="hub_repo", business_keys=["repo_name"]),
        HubConfig(name="hub_pull_request", business_keys=["repo_name", "pr_number"]),
        HubConfig(name="hub_issue", business_keys=["repo_name", "issue_number"]),
    ],
    links=[
        LinkConfig(name="link_actor_repo", hubs=["hub_actor", "hub_repo"]),
        LinkConfig(
            name="link_actor_pull_request", hubs=["hub_actor", "hub_pull_request", "hub_repo"]
        ),
        LinkConfig(name="link_actor_issue", hubs=["hub_actor", "hub_issue", "hub_repo"]),
        LinkConfig(
            name="link_issue_assignee", hubs=["hub_issue", "hub_actor"], driving_key="hub_issue"
        ),
    ],
    satellites=[
        SatelliteConfig(name="sat_actor_profile", parent="hub_actor"),
        SatelliteConfig(name="sat_repo_profile", parent="hub_repo"),
        SatelliteConfig(name="sat_repo_stats", parent="hub_repo"),
        SatelliteConfig(name="sat_pull_request_details", parent="hub_pull_request"),
        SatelliteConfig(name="sat_issue_details", parent="hub_issue"),
        SatelliteConfig(
            name="sat_actor_repo_event",
            parent="link_actor_repo",
            kind="multi_active",
            subsequence_key="event_id",
        ),
        SatelliteConfig(
            name="eff_sat_issue_assignee", parent="link_issue_assignee", kind="effectivity"
        ),
    ],
)

DEFAULT_MARTS = [
    MartConfig(
        name="repo_activity_mart",
        grain=["repo_name", "activity_date", "event_type"],
        pit="pit_repo_day",
    ),
    MartConfig(
        name="developer_360_mart", grain=["actor_login", "activity_date"], pit="pit_actor_day"
    ),
    MartConfig(
        name="collaboration_mart",
        grain=["item_type", "repo_name", "item_number"],
        pit="pit_pull_request_day",
    ),
]


def make_config(root: Path) -> LakehouseConfig:
    """Lakehouse config rooted at a temp directory; small Spark knobs for tests."""
    return LakehouseConfig(
        source=SourceConfig(landing_dir=root / "landing" / "github"),
        storage=StorageConfig(
            lakehouse_root=root / "lakehouse",
            run_dir=root / "run",
            reports_dir=root / "reports",
        ),
        event_types=DEFAULT_EVENT_TYPES,
        vault=DEFAULT_VAULT.model_copy(deep=True),
        marts=[m.model_copy(deep=True) for m in DEFAULT_MARTS],
        spark=SparkConfig(driver_memory="2g", shuffle_partitions=2),
    )


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SparkSession]:
    """One Delta-enabled local SparkSession shared by all spark-marked tests."""
    config = make_config(tmp_path_factory.mktemp("spark-session"))
    session = get_spark("lakehouse-tests", config)
    yield session
    session.stop()


@pytest.fixture()
def lakehouse_config(tmp_path: Path) -> LakehouseConfig:
    return make_config(tmp_path)
