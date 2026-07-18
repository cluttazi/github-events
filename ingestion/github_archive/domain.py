"""Mutable synthetic GitHub world that the event generator draws from.

All state transitions are driven by a single seeded ``random.Random`` and a
seeded Faker instance, so the same seed always produces the same world and
the same event stream — no wall-clock time ever enters a payload. Repo
statistics (stars, forks, open issues) evolve as events touch them, which is
what makes ``sat_repo_stats`` a genuinely fast-changing satellite downstream.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field

from faker import Faker

LANGUAGES = ["Python", "Scala", "TypeScript", "Go", "Rust", "Java"]
LICENSES = ["mit", "apache-2.0", "bsd-3-clause", None]

BOOTSTRAP_ACTORS = 20
BOOTSTRAP_REPOS = 12


@dataclass
class Actor:
    actor_id: int
    login: str
    display_login: str
    avatar_url: str


@dataclass
class Repo:
    repo_id: int
    name: str  # owner/name
    owner_login: str
    language: str
    default_branch: str
    license_key: str | None
    created_at: str
    stargazers: int = 0
    forks: int = 0
    open_issues: int = 0
    watchers: int = 0


@dataclass
class PullRequest:
    number: int
    title: str
    state: str = "open"  # open | closed
    merged: bool = False
    base_ref: str = "main"
    head_ref: str = "feature"


@dataclass
class Issue:
    number: int
    title: str
    state: str = "open"  # open | closed
    comments: int = 0
    assignee: str | None = None


@dataclass
class GithubDomain:
    """Seeded world of actors, repos, pull requests, and issues."""

    seed: int
    rng: random.Random = field(init=False)
    faker: Faker = field(init=False)
    actors: list[Actor] = field(default_factory=list)
    repos: list[Repo] = field(default_factory=list)
    pull_requests: dict[tuple[str, int], PullRequest] = field(default_factory=dict)
    issues: dict[tuple[str, int], Issue] = field(default_factory=dict)
    _next_actor_id: int = 1000
    _next_repo_id: int = 5000
    _next_pr_number: dict[str, int] = field(default_factory=dict)
    _next_issue_number: dict[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.rng = random.Random(self.seed)
        self.faker = Faker()
        self.faker.seed_instance(self.seed)
        for _ in range(BOOTSTRAP_ACTORS):
            self.new_actor()
        for _ in range(BOOTSTRAP_REPOS):
            self.new_repo(bootstrap_ts="2025-06-01T00:00:00Z")

    def new_actor(self) -> Actor:
        self._next_actor_id += 1
        login = f"{self.faker.user_name()}{self._next_actor_id % 97}"
        actor = Actor(
            actor_id=self._next_actor_id,
            login=login,
            display_login=login,
            avatar_url=f"https://avatars.example.test/u/{self._next_actor_id}",
        )
        self.actors.append(actor)
        return actor

    def new_repo(self, bootstrap_ts: str) -> Repo:
        self._next_repo_id += 1
        owner = self.rng.choice(self.actors)
        slug = self.faker.slug()
        repo = Repo(
            repo_id=self._next_repo_id,
            name=f"{owner.login}/{slug}",
            owner_login=owner.login,
            language=self.rng.choice(LANGUAGES),
            default_branch=self.rng.choice(["main", "main", "main", "trunk"]),
            license_key=self.rng.choice(LICENSES),
            created_at=bootstrap_ts,
            stargazers=self.rng.randint(0, 50),
            forks=self.rng.randint(0, 10),
            watchers=self.rng.randint(0, 30),
        )
        self.repos.append(repo)
        return repo

    def any_actor(self) -> Actor:
        return self.rng.choice(self.actors)

    def any_repo(self) -> Repo:
        return self.rng.choice(self.repos)

    def open_pull_request(self, repo: Repo) -> PullRequest:
        number = self._next_pr_number.get(repo.name, 0) + 1
        self._next_pr_number[repo.name] = number
        pr = PullRequest(
            number=number,
            title=self.faker.sentence(nb_words=5).rstrip("."),
            head_ref=f"feature/{self.faker.word()}-{number}",
            base_ref=repo.default_branch,
        )
        self.pull_requests[(repo.name, number)] = pr
        return pr

    def open_prs(self, repo: Repo) -> list[PullRequest]:
        return [
            pr
            for (name, _), pr in self.pull_requests.items()
            if name == repo.name and pr.state == "open"
        ]

    def open_issue(self, repo: Repo) -> Issue:
        number = self._next_issue_number.get(repo.name, 0) + 1
        self._next_issue_number[repo.name] = number
        issue = Issue(number=number, title=self.faker.sentence(nb_words=6).rstrip("."))
        self.issues[(repo.name, number)] = issue
        repo.open_issues += 1
        return issue

    def open_issues_for(self, repo: Repo) -> list[Issue]:
        return [
            issue
            for (name, _), issue in self.issues.items()
            if name == repo.name and issue.state == "open"
        ]
