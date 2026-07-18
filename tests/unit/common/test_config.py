"""Config loader and vault-topology validator tests."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from pipelines.common.config import (
    HubConfig,
    LinkConfig,
    SatelliteConfig,
    VaultConfig,
    load_config,
)
from tests.conftest import make_config


def _minimal_hubs() -> list[HubConfig]:
    return [
        HubConfig(name="hub_actor", business_keys=["actor_login"]),
        HubConfig(name="hub_repo", business_keys=["repo_name"]),
    ]


def test_link_referencing_unknown_hub_fails() -> None:
    with pytest.raises(ValidationError, match="unknown hubs"):
        VaultConfig(
            record_source_prefix="x",
            hubs=_minimal_hubs(),
            links=[LinkConfig(name="link_a_b", hubs=["hub_actor", "hub_ghost"])],
        )


def test_driving_key_must_be_member_hub() -> None:
    with pytest.raises(ValidationError, match="driving_key"):
        LinkConfig(name="link_a_r", hubs=["hub_actor", "hub_repo"], driving_key="hub_issue")


def test_multi_active_requires_subsequence_key() -> None:
    with pytest.raises(ValidationError, match="subsequence_key"):
        SatelliteConfig(name="sat_x", parent="hub_actor", kind="multi_active")


def test_subsequence_key_only_on_multi_active() -> None:
    with pytest.raises(ValidationError, match="subsequence_key"):
        SatelliteConfig(name="sat_x", parent="hub_actor", subsequence_key="event_id")


def test_satellite_with_unknown_parent_fails() -> None:
    with pytest.raises(ValidationError, match="unknown parent"):
        VaultConfig(
            record_source_prefix="x",
            hubs=_minimal_hubs(),
            satellites=[SatelliteConfig(name="sat_x", parent="hub_ghost")],
        )


def test_hash_key_column_naming() -> None:
    assert HubConfig(name="hub_pull_request", business_keys=["a", "b"]).hash_key_column == (
        "hk_pull_request"
    )
    assert LinkConfig(name="link_actor_repo", hubs=["hub_actor", "hub_repo"]).hash_key_column == (
        "lhk_actor_repo"
    )


def test_repo_config_loads_and_lookups_work() -> None:
    config = load_config()
    assert config.vault.hub("hub_actor").business_keys == ["actor_login"]
    assert config.vault.link("link_issue_assignee").driving_key == "hub_issue"
    assert config.vault.satellite("sat_actor_repo_event").subsequence_key == "event_id"
    assert config.mart("developer_360_mart").pit == "pit_actor_day"
    with pytest.raises(KeyError):
        config.vault.hub("hub_ghost")


def test_lakehouse_root_env_override(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    load_config.cache_clear()
    monkeypatch.setenv("LAKEHOUSE_ROOT", str(tmp_path))
    try:
        config = load_config()
        assert config.storage.lakehouse_root == tmp_path / "data" / "lakehouse"
        assert config.source.landing_dir == tmp_path / "data" / "landing" / "github"
    finally:
        load_config.cache_clear()


def test_make_config_paths_rooted_at_tmp(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    assert str(config.storage.lakehouse_root).startswith(str(tmp_path))
