"""Determinism and shape tests for the GH-Archive-style event generator."""

from __future__ import annotations

import json
from pathlib import Path

from ingestion.github_archive.generator import GeneratorConfig, run_generator


def _generate(
    root: Path, events: int = 400, seed: int = 42, corrupt_pct: float = 0.0
) -> tuple[Path, list[str]]:
    landing = root / "landing"
    config = GeneratorConfig(events=events, seed=seed, corrupt_pct=corrupt_pct, landing_dir=landing)
    run_generator(config)
    lines = []
    for path in sorted(landing.glob("*.ndjson")):
        lines.extend(path.read_text(encoding="utf-8").splitlines())
    return landing, lines


def test_same_seed_is_byte_identical(tmp_path: Path) -> None:
    _, first = _generate(tmp_path / "a", seed=7)
    _, second = _generate(tmp_path / "b", seed=7)
    assert first == second


def test_different_seed_differs(tmp_path: Path) -> None:
    _, first = _generate(tmp_path / "a", seed=7)
    _, second = _generate(tmp_path / "b", seed=8)
    assert first != second


def test_all_event_types_present(tmp_path: Path) -> None:
    _, lines = _generate(tmp_path, events=600)
    types = {json.loads(line)["type"] for line in lines}
    assert types == {
        "PushEvent",
        "PullRequestEvent",
        "IssuesEvent",
        "WatchEvent",
        "ForkEvent",
        "ReleaseEvent",
    }


def test_event_ids_unique_and_created_at_monotone(tmp_path: Path) -> None:
    _, lines = _generate(tmp_path, events=500)
    events = [json.loads(line) for line in lines]
    ids = [e["id"] for e in events]
    assert len(ids) == len(set(ids)) == 500
    created = [e["created_at"] for e in events]
    assert created == sorted(created)


def test_corrupt_pct_produces_exact_quarantine_fodder(tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    summary = run_generator(
        GeneratorConfig(events=500, seed=42, corrupt_pct=10.0, landing_dir=landing)
    )
    bad = 0
    for path in landing.glob("*.ndjson"):
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                bad += 1
                continue
            if "id" not in event:
                bad += 1
    assert bad == summary.corrupt_events
    assert 0 < summary.corrupt_events < 500


def test_hourly_gharchive_file_naming(tmp_path: Path) -> None:
    landing, _ = _generate(tmp_path, events=800)
    names = [p.name for p in sorted(landing.glob("*.ndjson"))]
    assert len(names) > 1  # logical clock crosses hour boundaries
    for name in names:
        date_part, hour_part = name.removesuffix(".ndjson").rsplit("-", 1)
        assert len(date_part.split("-")) == 3
        assert 0 <= int(hour_part) <= 23


def test_issue_lifecycle_actions_present(tmp_path: Path) -> None:
    """The effectivity satellite needs assigned/unassigned events to exist."""
    _, lines = _generate(tmp_path, events=2000)
    actions = {
        json.loads(line)["payload"].get("action")
        for line in lines
        if json.loads(line)["type"] == "IssuesEvent"
    }
    assert {"opened", "closed", "assigned"} <= actions
