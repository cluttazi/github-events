"""Event-loop that emits a seeded GH-Archive-style NDJSON feed.

The mix of event types is weighted to look like the public GitHub firehose
(pushes dominate; releases are rare) while guaranteeing the actions the raw
vault needs to exercise every construct: pull-request and issue lifecycles
(details satellites), issue ``assigned``/``unassigned`` (effectivity
satellite), watches/forks (repo stats churn).

Time is a logical clock: it starts at a fixed instant and advances a random
number of seconds per event, so the same seed produces byte-identical files.
Files rotate hourly under GH-Archive naming (``YYYY-MM-DD-H.ndjson``). A
configurable fraction of lines is deliberately corrupted before writing —
those must survive to the bronze quarantine, which the integration tests
assert by exact count.
"""

from __future__ import annotations

import json
import random
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

from ingestion.github_archive import events as ev
from ingestion.github_archive.domain import Actor, GithubDomain, Repo

CLOCK_START = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
MIN_STEP_S = 5
MAX_STEP_S = 90

EVENT_WEIGHTS: list[tuple[str, int]] = [
    ("PushEvent", 40),
    ("PullRequestEvent", 18),
    ("IssuesEvent", 18),
    ("WatchEvent", 12),
    ("ForkEvent", 7),
    ("ReleaseEvent", 5),
]

PR_ACTION_WEIGHTS: list[tuple[str, int]] = [("opened", 5), ("closed", 4), ("reopened", 1)]
ISSUE_ACTION_WEIGHTS: list[tuple[str, int]] = [
    ("opened", 4),
    ("closed", 3),
    ("assigned", 3),
    ("unassigned", 1),
    ("reopened", 1),
]


@dataclass
class GeneratorConfig:
    events: int
    seed: int = 42
    corrupt_pct: float = 0.0
    landing_dir: Path = Path("data/landing/github")


@dataclass
class GeneratorSummary:
    events_emitted: int = 0
    corrupt_events: int = 0
    files_written: int = 0
    by_type: Counter[str] = field(default_factory=Counter)

    def record(self, event_type: str) -> None:
        self.events_emitted += 1
        self.by_type[event_type] += 1


def _corrupt_line(line: str, rng: random.Random) -> str:
    """Damage a serialized event in one of three realistic ways."""
    variant = rng.choice(["truncate", "no_id", "not_json"])
    if variant == "truncate":
        return line[: max(10, len(line) // 2)]
    if variant == "no_id":
        return line.replace('"id":', '"id_":', 1)
    return "GARBAGE " + line[:40]


def _weighted(rng: random.Random, weights: list[tuple[str, int]]) -> str:
    choices, wts = zip(*weights, strict=True)
    return rng.choices(choices, weights=wts, k=1)[0]


def _hour_file(landing_dir: Path, ts: datetime) -> Path:
    return landing_dir / f"{ts:%Y-%m-%d}-{ts.hour}.ndjson"


class _EventFactory:
    """Builds one event of a requested type, mutating the domain as it goes."""

    def __init__(self, domain: GithubDomain) -> None:
        self.domain = domain

    def build(self, event_type: str, event_id: int, created_at: str) -> dict[str, object]:
        domain = self.domain
        actor = domain.any_actor()
        repo = domain.any_repo()
        if event_type == "PushEvent":
            size = domain.rng.randint(1, 12)
            distinct = domain.rng.randint(1, size)
            return ev.push_event(event_id, actor, repo, created_at, size, distinct)
        if event_type == "PullRequestEvent":
            return self._pull_request(event_id, actor, repo, created_at)
        if event_type == "IssuesEvent":
            return self._issue(event_id, actor, repo, created_at)
        if event_type == "WatchEvent":
            repo.stargazers += 1
            repo.watchers += 1
            return ev.watch_event(event_id, actor, repo, created_at)
        if event_type == "ForkEvent":
            repo.forks += 1
            forkee = Repo(
                repo_id=repo.repo_id + 900000,
                name=f"{actor.login}/{repo.name.split('/', 1)[1]}",
                owner_login=actor.login,
                language=repo.language,
                default_branch=repo.default_branch,
                license_key=repo.license_key,
                created_at=created_at,
            )
            return ev.fork_event(event_id, actor, repo, forkee, created_at)
        if event_type == "ReleaseEvent":
            tag = (
                f"v{domain.rng.randint(0, 4)}.{domain.rng.randint(0, 9)}.{domain.rng.randint(0, 9)}"
            )
            return ev.release_event(event_id, actor, repo, tag, f"Release {tag}", created_at)
        raise ValueError(f"unknown event type {event_type!r}")

    def _pull_request(
        self, event_id: int, actor: Actor, repo: Repo, created_at: str
    ) -> dict[str, object]:
        domain = self.domain
        action = _weighted(domain.rng, PR_ACTION_WEIGHTS)
        open_prs = domain.open_prs(repo)
        if action == "opened" or not open_prs:
            action = "opened"
            pr = domain.open_pull_request(repo)
        elif action == "closed":
            pr = domain.rng.choice(open_prs)
            pr.state = "closed"
            pr.merged = domain.rng.random() < 0.7
        else:  # reopened
            pr = domain.rng.choice(open_prs)
        return ev.pull_request_event(event_id, actor, repo, pr, action, created_at)

    def _issue(self, event_id: int, actor: Actor, repo: Repo, created_at: str) -> dict[str, object]:
        domain = self.domain
        action = _weighted(domain.rng, ISSUE_ACTION_WEIGHTS)
        open_issues = domain.open_issues_for(repo)
        assignee_login: str | None = None
        if action == "opened" or not open_issues:
            action = "opened"
            issue = domain.open_issue(repo)
        elif action == "closed":
            issue = domain.rng.choice(open_issues)
            issue.state = "closed"
            repo.open_issues = max(0, repo.open_issues - 1)
        elif action == "assigned":
            issue = domain.rng.choice(open_issues)
            issue.assignee = domain.any_actor().login
            assignee_login = issue.assignee
        elif action == "unassigned":
            issue = domain.rng.choice(open_issues)
            assignee_login = issue.assignee or domain.any_actor().login
            issue.assignee = None
        else:  # reopened
            issue = domain.rng.choice(open_issues)
        issue.comments += domain.rng.randint(0, 2)
        return ev.issues_event(event_id, actor, repo, issue, action, created_at, assignee_login)


def run_generator(config: GeneratorConfig) -> GeneratorSummary:
    """Emit the configured number of events; returns an emission summary."""
    domain = GithubDomain(seed=config.seed)
    factory = _EventFactory(domain)
    corrupt_rng = random.Random(config.seed + 1)
    summary = GeneratorSummary()

    config.landing_dir.mkdir(parents=True, exist_ok=True)
    clock = CLOCK_START
    open_path: Path | None = None
    lines: list[str] = []

    def flush() -> None:
        nonlocal lines, open_path
        if open_path is not None and lines:
            open_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            summary.files_written += 1
        lines = []

    for event_id in range(1, config.events + 1):
        clock += timedelta(seconds=domain.rng.randint(MIN_STEP_S, MAX_STEP_S))
        target = _hour_file(config.landing_dir, clock)
        if target != open_path:
            flush()
            open_path = target
        event_type = _weighted(domain.rng, EVENT_WEIGHTS)
        created_at = clock.strftime("%Y-%m-%dT%H:%M:%SZ")
        event = factory.build(event_type, event_id, created_at)
        line = json.dumps(event, separators=(",", ":"))
        if corrupt_rng.random() < config.corrupt_pct / 100.0:
            line = _corrupt_line(line, corrupt_rng)
            summary.corrupt_events += 1
        lines.append(line)
        summary.record(event_type)
    flush()
    return summary
