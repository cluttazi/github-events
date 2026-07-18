"""GH-Archive-shaped event payload builders.

Each builder returns the JSON object for one event line, matching the public
GitHub events feed closely enough that downstream staging reads the same
paths a real feed would provide:

* every event: ``id``, ``type``, ``actor{id,login,display_login,avatar_url}``,
  ``repo{id,name,url}``, ``payload{...}``, ``created_at``
* the ``repo`` envelope object is deliberately minimal (as in the real feed);
  full repo detail (language, stats, license) only travels inside
  ``payload.pull_request.base.repo`` and ``payload.forkee`` — which is why
  ``sat_repo_profile``/``sat_repo_stats`` load from those payloads only.
"""

from __future__ import annotations

from ingestion.github_archive.domain import Actor, Issue, PullRequest, Repo

JsonObj = dict[str, object]


def _actor_obj(actor: Actor) -> JsonObj:
    return {
        "id": actor.actor_id,
        "login": actor.login,
        "display_login": actor.display_login,
        "avatar_url": actor.avatar_url,
    }


def _repo_envelope(repo: Repo) -> JsonObj:
    return {
        "id": repo.repo_id,
        "name": repo.name,
        "url": f"https://api.github.example.test/repos/{repo.name}",
    }


def _full_repo_obj(repo: Repo) -> JsonObj:
    return {
        "id": repo.repo_id,
        "name": repo.name.split("/", 1)[1],
        "full_name": repo.name,
        "owner": {"login": repo.owner_login},
        "language": repo.language,
        "default_branch": repo.default_branch,
        "license": {"key": repo.license_key} if repo.license_key else None,
        "created_at": repo.created_at,
        "stargazers_count": repo.stargazers,
        "forks_count": repo.forks,
        "open_issues_count": repo.open_issues,
        "watchers_count": repo.watchers,
    }


def _envelope(
    event_id: int, event_type: str, actor: Actor, repo: Repo, payload: JsonObj, created_at: str
) -> JsonObj:
    return {
        "id": str(event_id),
        "type": event_type,
        "actor": _actor_obj(actor),
        "repo": _repo_envelope(repo),
        "payload": payload,
        "public": True,
        "created_at": created_at,
    }


def push_event(
    event_id: int, actor: Actor, repo: Repo, created_at: str, size: int, distinct_size: int
) -> JsonObj:
    payload: JsonObj = {
        "push_id": event_id * 10,
        "size": size,
        "distinct_size": distinct_size,
        "ref": f"refs/heads/{repo.default_branch}",
        "head": f"{event_id:040x}"[-40:],
        "before": f"{event_id - 1:040x}"[-40:],
    }
    return _envelope(event_id, "PushEvent", actor, repo, payload, created_at)


def pull_request_event(
    event_id: int, actor: Actor, repo: Repo, pr: PullRequest, action: str, created_at: str
) -> JsonObj:
    payload: JsonObj = {
        "action": action,
        "number": pr.number,
        "pull_request": {
            "number": pr.number,
            "title": pr.title,
            "state": pr.state,
            "merged": pr.merged,
            "base": {"ref": pr.base_ref, "repo": _full_repo_obj(repo)},
            "head": {"ref": pr.head_ref},
        },
    }
    return _envelope(event_id, "PullRequestEvent", actor, repo, payload, created_at)


def issues_event(
    event_id: int,
    actor: Actor,
    repo: Repo,
    issue: Issue,
    action: str,
    created_at: str,
    assignee_login: str | None = None,
) -> JsonObj:
    payload: JsonObj = {
        "action": action,
        "issue": {
            "number": issue.number,
            "title": issue.title,
            "state": issue.state,
            "comments": issue.comments,
        },
    }
    if assignee_login is not None:
        payload["assignee"] = {"login": assignee_login}
    return _envelope(event_id, "IssuesEvent", actor, repo, payload, created_at)


def watch_event(event_id: int, actor: Actor, repo: Repo, created_at: str) -> JsonObj:
    return _envelope(event_id, "WatchEvent", actor, repo, {"action": "started"}, created_at)


def fork_event(event_id: int, actor: Actor, repo: Repo, forkee: Repo, created_at: str) -> JsonObj:
    return _envelope(
        event_id, "ForkEvent", actor, repo, {"forkee": _full_repo_obj(forkee)}, created_at
    )


def release_event(
    event_id: int, actor: Actor, repo: Repo, tag: str, release_name: str, created_at: str
) -> JsonObj:
    payload: JsonObj = {
        "action": "published",
        "release": {"tag_name": tag, "name": release_name, "draft": False, "prerelease": False},
    }
    return _envelope(event_id, "ReleaseEvent", actor, repo, payload, created_at)
