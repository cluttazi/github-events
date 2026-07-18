"""Typed loader for ``config/lakehouse.yaml``.

Pydantic gives us validation with useful error messages at process start —
a config typo fails fast instead of surfacing as a cryptic Spark error three
stages later. The vault topology (hubs, links, satellites, marts) is declared
here so the generic raw-vault loaders, the DQ suites, and the docs all read
one source of truth; the per-satellite column extraction logic stays in code
(``pipelines/raw_vault/staging.py``) where it can be typed and tested.

Environment variables override the one knob that varies between local/demo/CI
runs (the storage root).
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, model_validator

SatelliteKind = Literal["standard", "multi_active", "effectivity"]

_REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = _REPO_ROOT / "config" / "lakehouse.yaml"


class SourceConfig(BaseModel):
    landing_dir: Path


class StorageConfig(BaseModel):
    lakehouse_root: Path
    run_dir: Path
    reports_dir: Path


class HubConfig(BaseModel):
    name: str
    business_keys: list[str] = Field(min_length=1)

    @property
    def hash_key_column(self) -> str:
        """``hub_actor`` -> ``hk_actor``."""
        return "hk_" + self.name.removeprefix("hub_")


class LinkConfig(BaseModel):
    name: str
    hubs: list[str] = Field(min_length=2)
    driving_key: str | None = None

    @model_validator(mode="after")
    def _driving_key_is_member(self) -> LinkConfig:
        if self.driving_key is not None and self.driving_key not in self.hubs:
            raise ValueError(
                f"link {self.name!r}: driving_key {self.driving_key!r} "
                f"is not one of its hubs {self.hubs}"
            )
        return self

    @property
    def hash_key_column(self) -> str:
        """``link_actor_repo`` -> ``lhk_actor_repo``."""
        return "lhk_" + self.name.removeprefix("link_")


class SatelliteConfig(BaseModel):
    name: str
    parent: str
    kind: SatelliteKind = "standard"
    subsequence_key: str | None = None

    @model_validator(mode="after")
    def _multi_active_needs_subsequence(self) -> SatelliteConfig:
        if self.kind == "multi_active" and not self.subsequence_key:
            raise ValueError(f"multi-active satellite {self.name!r} requires a subsequence_key")
        if self.kind != "multi_active" and self.subsequence_key:
            raise ValueError(f"satellite {self.name!r}: subsequence_key only valid on multi_active")
        return self


class VaultConfig(BaseModel):
    record_source_prefix: str
    hubs: list[HubConfig] = Field(min_length=1)
    links: list[LinkConfig] = Field(default_factory=list)
    satellites: list[SatelliteConfig] = Field(default_factory=list)

    @model_validator(mode="after")
    def _references_resolve(self) -> VaultConfig:
        hub_names = {hub.name for hub in self.hubs}
        link_names = {link.name for link in self.links}
        for link in self.links:
            unknown = [h for h in link.hubs if h not in hub_names]
            if unknown:
                raise ValueError(f"link {link.name!r} references unknown hubs {unknown}")
        for sat in self.satellites:
            if sat.parent not in hub_names | link_names:
                raise ValueError(f"satellite {sat.name!r} references unknown parent {sat.parent!r}")
        return self

    def hub(self, name: str) -> HubConfig:
        for hub in self.hubs:
            if hub.name == name:
                return hub
        raise KeyError(f"unknown hub {name!r}; configured: {[h.name for h in self.hubs]}")

    def link(self, name: str) -> LinkConfig:
        for link in self.links:
            if link.name == name:
                return link
        raise KeyError(f"unknown link {name!r}; configured: {[link.name for link in self.links]}")

    def satellite(self, name: str) -> SatelliteConfig:
        for sat in self.satellites:
            if sat.name == name:
                return sat
        raise KeyError(
            f"unknown satellite {name!r}; configured: {[s.name for s in self.satellites]}"
        )


class MartConfig(BaseModel):
    name: str
    grain: list[str] = Field(min_length=1)
    pit: str


class SparkConfig(BaseModel):
    driver_memory: str = "3g"
    shuffle_partitions: int = 8
    session_timezone: str = "UTC"


class LakehouseConfig(BaseModel):
    source: SourceConfig
    storage: StorageConfig
    event_types: list[str] = Field(min_length=1)
    vault: VaultConfig
    marts: list[MartConfig] = Field(min_length=1)
    spark: SparkConfig = SparkConfig()

    def mart(self, name: str) -> MartConfig:
        for mart in self.marts:
            if mart.name == name:
                return mart
        raise KeyError(f"unknown mart {name!r}; configured: {[m.name for m in self.marts]}")


def _resolve(base: Path, path: Path) -> Path:
    return path if path.is_absolute() else base / path


@lru_cache(maxsize=4)
def load_config(config_path: Path = DEFAULT_CONFIG_PATH) -> LakehouseConfig:
    """Load, validate, and env-override the lakehouse configuration.

    All relative paths are anchored at the repo root (or ``LAKEHOUSE_ROOT``)
    so every entry point (make targets, tests, wheel entry points) sees the
    same absolute layout regardless of its working directory.
    """
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config = LakehouseConfig.model_validate(raw)

    root = Path(os.environ.get("LAKEHOUSE_ROOT", str(_REPO_ROOT)))
    config.source.landing_dir = _resolve(root, config.source.landing_dir)
    storage = config.storage
    storage.lakehouse_root = _resolve(root, storage.lakehouse_root)
    storage.run_dir = _resolve(root, storage.run_dir)
    storage.reports_dir = _resolve(root, storage.reports_dir)
    return config
