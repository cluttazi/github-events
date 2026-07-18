"""Flatten bronze ``raw_value`` JSON into contract-shaped staged frames.

This module owns the *nesting knowledge*: which JSON paths inside each event
type's payload produce which flat contract column. The contracts
(``quality/contracts/definitions``) describe the flat output; this code is
the executable mapping to it. Hard rules only — extraction and typing, no
derivations, no business logic (that belongs to the business vault / gold).

Every staged frame carries, beyond its contract columns:

* ``record_source`` — ``gharchive.<EventType>`` (the DV2.0 provenance tag)
* ``occurred_at``   — the event time; satellites order history by it
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from pipelines.bronze.copy_into import events_table_path
from pipelines.common.config import LakehouseConfig

# One nested parse schema per event type: everything the payload can carry,
# typed loosely (strings/longs); contract casting happens on the flat side.
_ACTOR = "actor struct<id bigint, login string, display_login string, avatar_url string>"
_REPO = "repo struct<id bigint, name string, url string>"
_FULL_REPO = (
    "struct<id bigint, name string, full_name string, owner struct<login string>, "
    "language string, default_branch string, license struct<key string>, created_at string, "
    "stargazers_count bigint, forks_count bigint, open_issues_count bigint, "
    "watchers_count bigint>"
)

_PAYLOADS: dict[str, str] = {
    "PushEvent": "struct<push_id bigint, size int, distinct_size int, ref string>",
    "PullRequestEvent": (
        "struct<action string, number int, pull_request struct<number int, title string, "
        f"state string, merged boolean, base struct<ref string, repo {_FULL_REPO}>, "
        "head struct<ref string>>>"
    ),
    "IssuesEvent": (
        "struct<action string, issue struct<number int, title string, state string, "
        "comments int>, assignee struct<login string>>"
    ),
    "WatchEvent": "struct<action string>",
    "ForkEvent": f"struct<forkee {_FULL_REPO}>",
    "ReleaseEvent": (
        "struct<action string, release struct<tag_name string, name string, draft boolean, "
        "prerelease boolean>>"
    ),
}

# Contract name per event type ("PushEvent" -> "push_event").
CONTRACT_BY_EVENT_TYPE: dict[str, str] = {
    "PushEvent": "push_event",
    "PullRequestEvent": "pull_request_event",
    "IssuesEvent": "issues_event",
    "WatchEvent": "watch_event",
    "ForkEvent": "fork_event",
    "ReleaseEvent": "release_event",
}


def _event_schema(event_type: str) -> str:
    return (
        f"id string, type string, created_at string, {_ACTOR}, {_REPO}, "
        f"payload {_PAYLOADS[event_type]}"
    )


def _common_columns() -> dict[str, Column]:
    return {
        "event_id": F.col("e.id"),
        "actor_login": F.col("e.actor.login"),
        "actor_display_login": F.col("e.actor.display_login"),
        "actor_avatar_url": F.col("e.actor.avatar_url"),
        "repo_name": F.col("e.repo.name"),
        "created_at": F.try_to_timestamp(F.col("e.created_at")),
    }


def _type_columns(event_type: str) -> dict[str, Column]:
    p = "e.payload"
    if event_type == "PushEvent":
        return {
            "push_id": F.col(f"{p}.push_id"),
            "push_size": F.col(f"{p}.size"),
            "push_distinct_size": F.col(f"{p}.distinct_size"),
            "ref_name": F.col(f"{p}.ref"),
        }
    if event_type == "PullRequestEvent":
        base_repo = f"{p}.pull_request.base.repo"
        return {
            "action": F.col(f"{p}.action"),
            "pr_number": F.col(f"{p}.pull_request.number"),
            "pr_title": F.col(f"{p}.pull_request.title"),
            "pr_state": F.col(f"{p}.pull_request.state"),
            "pr_merged": F.col(f"{p}.pull_request.merged"),
            "base_ref": F.col(f"{p}.pull_request.base.ref"),
            "head_ref": F.col(f"{p}.pull_request.head.ref"),
            "base_repo_owner": F.col(f"{base_repo}.owner.login"),
            "base_repo_language": F.col(f"{base_repo}.language"),
            "base_repo_default_branch": F.col(f"{base_repo}.default_branch"),
            "base_repo_license": F.col(f"{base_repo}.license.key"),
            "base_repo_created_at": F.try_to_timestamp(F.col(f"{base_repo}.created_at")),
            "base_repo_stargazers": F.col(f"{base_repo}.stargazers_count"),
            "base_repo_forks": F.col(f"{base_repo}.forks_count"),
            "base_repo_open_issues": F.col(f"{base_repo}.open_issues_count"),
            "base_repo_watchers": F.col(f"{base_repo}.watchers_count"),
        }
    if event_type == "IssuesEvent":
        return {
            "action": F.col(f"{p}.action"),
            "issue_number": F.col(f"{p}.issue.number"),
            "issue_title": F.col(f"{p}.issue.title"),
            "issue_state": F.col(f"{p}.issue.state"),
            "issue_comments": F.col(f"{p}.issue.comments"),
            "assignee_login": F.col(f"{p}.assignee.login"),
        }
    if event_type == "WatchEvent":
        return {"action": F.col(f"{p}.action")}
    if event_type == "ForkEvent":
        forkee = f"{p}.forkee"
        return {
            "forkee_full_name": F.col(f"{forkee}.full_name"),
            "forkee_owner": F.col(f"{forkee}.owner.login"),
            "forkee_language": F.col(f"{forkee}.language"),
            "forkee_default_branch": F.col(f"{forkee}.default_branch"),
            "forkee_license": F.col(f"{forkee}.license.key"),
            "forkee_created_at": F.try_to_timestamp(F.col(f"{forkee}.created_at")),
        }
    if event_type == "ReleaseEvent":
        return {
            "action": F.col(f"{p}.action"),
            "release_tag": F.col(f"{p}.release.tag_name"),
            "release_name": F.col(f"{p}.release.name"),
            "release_prerelease": F.col(f"{p}.release.prerelease"),
        }
    raise ValueError(f"no staging spec for event type {event_type!r}")


@dataclass(frozen=True)
class StagedBatch:
    """Contract-shaped frames per event type, pre-enforcement."""

    by_type: dict[str, DataFrame]  # event type -> staged frame

    def frame(self, event_type: str) -> DataFrame:
        return self.by_type[event_type]


def stage_events(
    spark: SparkSession, config: LakehouseConfig, record_source_prefix: str
) -> StagedBatch:
    """Read the full bronze table and flatten it per event type.

    Raw-vault loads recompute candidates from *all* bronze history each run;
    the insert-only merges downstream turn unchanged candidates into no-ops.
    That trade (O(history) per run, honest at demo scale) is what makes the
    loads pure functions of bronze — deterministic and idempotent.
    """
    bronze = spark.read.format("delta").load(events_table_path(config))
    by_type: dict[str, DataFrame] = {}
    for event_type in _PAYLOADS:
        subset = bronze.filter(F.col("event_type") == event_type)
        parsed = subset.select(
            F.from_json(F.col("raw_value"), _event_schema(event_type)).alias("e")
        )
        columns = _common_columns() | _type_columns(event_type)
        staged = parsed.select(*[expr.alias(name) for name, expr in columns.items()])
        by_type[event_type] = staged.withColumn(
            "record_source", F.lit(f"{record_source_prefix}.{event_type}")
        ).withColumn("occurred_at", F.col("created_at"))
    return StagedBatch(by_type=by_type)
