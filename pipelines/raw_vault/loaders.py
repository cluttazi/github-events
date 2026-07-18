"""Generic, insert-only Data Vault 2.0 loaders (ADR 003).

Every loader is a pure function of the staged frames: candidates are
recomputed from full bronze history, then MERGEd insert-only into the target.
Re-running against unchanged bronze therefore adds zero rows — the property
``pipelines.raw_vault.job --verify-idempotent`` proves on every demo run.

Merge keys per object kind:

* hub / link — the hash key (insert-if-absent; first arrival wins, so
  ``load_dts``/``record_source`` of existing rows are never touched)
* standard satellite — ``(hash_key, hash_diff)``: a new row only when the
  descriptive attributes actually changed (change detection is ordered by
  ``occurred_at``, the event time; ``load_dts`` stays the wall-clock arrival)
* multi-active satellite — ``(link_hash_key, subsequence key)``: one row per
  source event, multiple concurrently-valid rows per link key
* effectivity satellite — ``(link_hash_key, start_dts, end_dts)``: intervals
  computed over the driving key's full event history; when an open interval
  (``end_dts = 9999-12-31``) later closes, the *closed* version is inserted
  as a new row and supersedes the open one at query time (insert-only, no
  updates — the classic DV2.0 effectivity pattern)

All loaders return the number of rows actually added to the target.
"""

from __future__ import annotations

from dataclasses import dataclass

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from pipelines.common.config import LakehouseConfig, LinkConfig, VaultConfig
from pipelines.common.hashing import hash_diff, hash_key

HIGH_DTS = "9999-12-31 00:00:00"

EVENT_TYPES_ALL = [
    "PushEvent",
    "PullRequestEvent",
    "IssuesEvent",
    "WatchEvent",
    "ForkEvent",
    "ReleaseEvent",
]

# hub name -> [(event type, {business key col -> staged col})]
HUB_SOURCES: dict[str, list[tuple[str, dict[str, str]]]] = {
    "hub_actor": [(t, {"actor_login": "actor_login"}) for t in EVENT_TYPES_ALL]
    + [("IssuesEvent", {"actor_login": "assignee_login"})],
    "hub_repo": [(t, {"repo_name": "repo_name"}) for t in EVENT_TYPES_ALL]
    + [("ForkEvent", {"repo_name": "forkee_full_name"})],
    "hub_pull_request": [
        ("PullRequestEvent", {"repo_name": "repo_name", "pr_number": "pr_number"})
    ],
    "hub_issue": [("IssuesEvent", {"repo_name": "repo_name", "issue_number": "issue_number"})],
}

# link name -> [(event type, filter col expr or None, {hub name -> {bk col -> staged col}})]
LINK_SOURCES: dict[str, list[tuple[str, dict[str, dict[str, str]]]]] = {
    "link_actor_repo": [
        (t, {"hub_actor": {"actor_login": "actor_login"}, "hub_repo": {"repo_name": "repo_name"}})
        for t in EVENT_TYPES_ALL
    ],
    "link_actor_pull_request": [
        (
            "PullRequestEvent",
            {
                "hub_actor": {"actor_login": "actor_login"},
                "hub_pull_request": {"repo_name": "repo_name", "pr_number": "pr_number"},
                "hub_repo": {"repo_name": "repo_name"},
            },
        )
    ],
    "link_actor_issue": [
        (
            "IssuesEvent",
            {
                "hub_actor": {"actor_login": "actor_login"},
                "hub_issue": {"repo_name": "repo_name", "issue_number": "issue_number"},
                "hub_repo": {"repo_name": "repo_name"},
            },
        )
    ],
    "link_issue_assignee": [
        (
            "IssuesEvent",
            {
                "hub_issue": {"repo_name": "repo_name", "issue_number": "issue_number"},
                "hub_actor": {"actor_login": "assignee_login"},
            },
        )
    ],
}

ASSIGNMENT_ACTIONS = ["assigned", "unassigned"]


@dataclass(frozen=True)
class SatelliteSpec:
    """Column-level load spec for one satellite (the config declares topology,
    this declares extraction)."""

    name: str
    parent_keys: dict[str, dict[str, str]]  # event type -> {parent bk col -> staged col}
    attributes: dict[str, dict[str, str]]  # event type -> {attr col -> staged col}


SATELLITE_SPECS: dict[str, SatelliteSpec] = {
    "sat_actor_profile": SatelliteSpec(
        name="sat_actor_profile",
        parent_keys={t: {"actor_login": "actor_login"} for t in EVENT_TYPES_ALL},
        attributes={
            t: {
                "display_login": "actor_display_login",
                "avatar_url": "actor_avatar_url",
            }
            for t in EVENT_TYPES_ALL
        },
    ),
    "sat_repo_profile": SatelliteSpec(
        name="sat_repo_profile",
        parent_keys={
            "PullRequestEvent": {"repo_name": "repo_name"},
            "ForkEvent": {"repo_name": "forkee_full_name"},
        },
        attributes={
            "PullRequestEvent": {
                "owner_login": "base_repo_owner",
                "language": "base_repo_language",
                "default_branch": "base_repo_default_branch",
                "license": "base_repo_license",
                "repo_created_at": "base_repo_created_at",
            },
            "ForkEvent": {
                "owner_login": "forkee_owner",
                "language": "forkee_language",
                "default_branch": "forkee_default_branch",
                "license": "forkee_license",
                "repo_created_at": "forkee_created_at",
            },
        },
    ),
    "sat_repo_stats": SatelliteSpec(
        name="sat_repo_stats",
        parent_keys={"PullRequestEvent": {"repo_name": "repo_name"}},
        attributes={
            "PullRequestEvent": {
                "stargazers_count": "base_repo_stargazers",
                "forks_count": "base_repo_forks",
                "open_issues_count": "base_repo_open_issues",
                "watchers_count": "base_repo_watchers",
            }
        },
    ),
    "sat_pull_request_details": SatelliteSpec(
        name="sat_pull_request_details",
        parent_keys={"PullRequestEvent": {"repo_name": "repo_name", "pr_number": "pr_number"}},
        attributes={
            "PullRequestEvent": {
                "action": "action",
                "pr_title": "pr_title",
                "pr_state": "pr_state",
                "pr_merged": "pr_merged",
                "base_ref": "base_ref",
                "head_ref": "head_ref",
            }
        },
    ),
    "sat_issue_details": SatelliteSpec(
        name="sat_issue_details",
        parent_keys={"IssuesEvent": {"repo_name": "repo_name", "issue_number": "issue_number"}},
        attributes={
            "IssuesEvent": {
                "action": "action",
                "issue_title": "issue_title",
                "issue_state": "issue_state",
                "issue_comments": "issue_comments",
            }
        },
    ),
}

# The multi-active satellite rides link_actor_repo: one row per event.
MAS_ATTRIBUTES: dict[str, dict[str, str]] = {
    "PushEvent": {
        "push_size": "push_size",
        "push_distinct_size": "push_distinct_size",
        "ref_name": "ref_name",
    },
    "PullRequestEvent": {"action": "action"},
    "IssuesEvent": {"action": "action"},
    "WatchEvent": {"action": "action"},
    "ForkEvent": {},
    "ReleaseEvent": {"action": "action"},
}
MAS_ATTRIBUTE_TYPES: dict[str, str] = {
    "event_type": "string",
    "action": "string",
    "push_size": "int",
    "push_distinct_size": "int",
    "ref_name": "string",
}
MAS_ATTRIBUTE_COLUMNS = list(MAS_ATTRIBUTE_TYPES)


def table_path(config: LakehouseConfig, zone: str, name: str) -> str:
    return str(config.storage.lakehouse_root / zone / name)


def _target_count(spark: SparkSession, path: str) -> int:
    if not DeltaTable.isDeltaTable(spark, path):
        return 0
    return spark.read.format("delta").load(path).count()


def insert_only_merge(
    spark: SparkSession, target_path: str, candidates: DataFrame, key_cols: list[str]
) -> int:
    """Insert-if-absent MERGE on ``key_cols``; returns rows actually added.

    ``whenNotMatchedInsertAll`` with no matched clause: existing rows are
    never updated or deleted — the raw vault is insert-only by construction.
    """
    if not DeltaTable.isDeltaTable(spark, target_path):
        candidates.write.format("delta").mode("overwrite").save(target_path)
        return _target_count(spark, target_path)

    before = _target_count(spark, target_path)
    target = DeltaTable.forPath(spark, target_path)
    condition = " AND ".join(f"t.{c} <=> s.{c}" for c in key_cols)
    (target.alias("t").merge(candidates.alias("s"), condition).whenNotMatchedInsertAll().execute())
    return _target_count(spark, target_path) - before


def _first_per_key(df: DataFrame, key_cols: list[str], order_cols: list[str]) -> DataFrame:
    """Deterministically keep the earliest row per key (stable across runs)."""
    window = Window.partitionBy(*key_cols).orderBy(*[F.col(c).asc_nulls_last() for c in order_cols])
    return df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")


def _with_load_dts(df: DataFrame) -> DataFrame:
    return df.withColumn("load_dts", F.current_timestamp())


def _link_component_columns(
    link: LinkConfig, vault: VaultConfig, hub_mappings: dict[str, dict[str, str]]
) -> tuple[list[Column], dict[str, Column]]:
    """(ordered lhk hash components, {hk column name -> hash expr}) for a link."""
    components: list[Column] = []
    hk_columns: dict[str, Column] = {}
    for hub_name in link.hubs:
        hub = vault.hub(hub_name)
        mapping = hub_mappings[hub_name]
        staged_cols = [F.col(mapping[bk]) for bk in hub.business_keys]
        components.extend(staged_cols)
        hk_columns[hub.hash_key_column] = hash_key(staged_cols)
    return components, hk_columns


def load_hubs(
    spark: SparkSession,
    config: LakehouseConfig,
    staged: dict[str, DataFrame],
) -> dict[str, int]:
    """Load every configured hub; returns rows inserted per hub."""
    results: dict[str, int] = {}
    for hub in config.vault.hubs:
        frames = []
        for event_type, mapping in HUB_SOURCES[hub.name]:
            if event_type not in staged:
                continue
            frame = staged[event_type].select(
                *[F.col(src).alias(bk) for bk, src in mapping.items()],
                "record_source",
                "occurred_at",
            )
            frames.append(frame)
        if not frames:
            results[hub.name] = 0
            continue
        candidates = frames[0]
        for frame in frames[1:]:
            candidates = candidates.unionByName(frame)
        all_keys_present = F.lit(True)
        for bk in hub.business_keys:
            all_keys_present = all_keys_present & F.col(bk).isNotNull()
        candidates = candidates.filter(all_keys_present)
        candidates = candidates.withColumn(
            hub.hash_key_column, hash_key([F.col(bk) for bk in hub.business_keys])
        )
        candidates = _first_per_key(
            candidates, [hub.hash_key_column], ["occurred_at", "record_source"]
        )
        candidates = _with_load_dts(
            candidates.select(hub.hash_key_column, *hub.business_keys, "record_source")
        )
        results[hub.name] = insert_only_merge(
            spark, table_path(config, "raw_vault", hub.name), candidates, [hub.hash_key_column]
        )
    return results


def _link_candidates(
    link: LinkConfig, vault: VaultConfig, staged: dict[str, DataFrame]
) -> DataFrame | None:
    frames = []
    for event_type, hub_mappings in LINK_SOURCES[link.name]:
        if event_type not in staged:
            continue
        source = staged[event_type]
        if link.name == "link_issue_assignee":
            source = source.filter(
                F.col("action").isin(ASSIGNMENT_ACTIONS) & F.col("assignee_login").isNotNull()
            )
        components, hk_columns = _link_component_columns(link, vault, hub_mappings)
        # Skip rows where any participating business key is null: a link row
        # must reference every parent hub.
        not_null = [c.isNotNull() for c in components]
        condition = not_null[0]
        for check in not_null[1:]:
            condition = condition & check
        frame = source.filter(condition).select(
            hash_key(components).alias(link.hash_key_column),
            *[expr.alias(name) for name, expr in hk_columns.items()],
            "record_source",
            "occurred_at",
        )
        frames.append(frame)
    if not frames:
        return None
    candidates = frames[0]
    for frame in frames[1:]:
        candidates = candidates.unionByName(frame)
    return candidates


def load_links(
    spark: SparkSession,
    config: LakehouseConfig,
    staged: dict[str, DataFrame],
) -> dict[str, int]:
    """Load every configured link; returns rows inserted per link."""
    results: dict[str, int] = {}
    for link in config.vault.links:
        candidates = _link_candidates(link, config.vault, staged)
        if candidates is None:
            results[link.name] = 0
            continue
        candidates = _first_per_key(
            candidates, [link.hash_key_column], ["occurred_at", "record_source"]
        )
        hk_cols = [config.vault.hub(h).hash_key_column for h in link.hubs]
        candidates = _with_load_dts(
            candidates.select(link.hash_key_column, *dict.fromkeys(hk_cols), "record_source")
        )
        results[link.name] = insert_only_merge(
            spark, table_path(config, "raw_vault", link.name), candidates, [link.hash_key_column]
        )
    return results


def _standard_sat_candidates(
    spec: SatelliteSpec, parent_hash_col: str, staged: dict[str, DataFrame]
) -> DataFrame | None:
    attr_names = sorted({attr for attrs in spec.attributes.values() for attr in attrs})
    frames = []
    for event_type, key_mapping in spec.parent_keys.items():
        if event_type not in staged:
            continue
        source = staged[event_type]
        key_cols = [F.col(src) for src in key_mapping.values()]
        not_null = key_cols[0].isNotNull()
        for col in key_cols[1:]:
            not_null = not_null & col.isNotNull()
        attrs = spec.attributes[event_type]
        selected = source.filter(not_null).select(
            hash_key(key_cols).alias(parent_hash_col),
            *[(F.col(attrs[a]) if a in attrs else F.lit(None)).alias(a) for a in attr_names],
            "record_source",
            "occurred_at",
        )
        frames.append(selected)
    if not frames:
        return None
    candidates = frames[0]
    for frame in frames[1:]:
        candidates = candidates.unionByName(frame)
    return candidates.withColumn("hash_diff", hash_diff([F.col(a) for a in attr_names]))


def load_standard_satellite(
    spark: SparkSession,
    config: LakehouseConfig,
    sat_name: str,
    staged: dict[str, DataFrame],
) -> int:
    """Load one standard satellite: a row per *distinct attribute state*.

    Change detection orders the full history by event time and keeps rows
    whose ``hash_diff`` differs from the previous state; the merge key
    ``(hash_key, hash_diff)`` then makes replays no-ops. A state that flips
    A→B→A collapses to its first occurrence — recorded in DECISIONS.md.
    """
    sat = config.vault.satellite(sat_name)
    spec = SATELLITE_SPECS[sat_name]
    parent_hash_col = config.vault.hub(sat.parent).hash_key_column
    candidates = _standard_sat_candidates(spec, parent_hash_col, staged)
    if candidates is None:
        return 0

    window = Window.partitionBy(parent_hash_col).orderBy("occurred_at", "record_source")
    changed = (
        candidates.withColumn("_prev_diff", F.lag("hash_diff").over(window))
        .filter(F.col("_prev_diff").isNull() | (F.col("_prev_diff") != F.col("hash_diff")))
        .drop("_prev_diff")
    )
    deduped = _first_per_key(changed, [parent_hash_col, "hash_diff"], ["occurred_at"])
    final = _with_load_dts(deduped)
    return insert_only_merge(
        spark,
        table_path(config, "raw_vault", sat_name),
        final,
        [parent_hash_col, "hash_diff"],
    )


def load_multi_active_satellite(
    spark: SparkSession,
    config: LakehouseConfig,
    sat_name: str,
    staged: dict[str, DataFrame],
) -> int:
    """Load the multi-active satellite: one row per source event.

    The subsequence key (``event_id``) joins the link hash key in the grain,
    so many rows per link key are concurrently valid — the transactional
    event stream lives here.
    """
    sat = config.vault.satellite(sat_name)
    link = config.vault.link(sat.parent)
    assert sat.subsequence_key is not None  # validated by config
    frames = []
    for event_type, hub_mappings in LINK_SOURCES[link.name]:
        if event_type not in staged:
            continue
        source = staged[event_type]
        components, _ = _link_component_columns(link, config.vault, hub_mappings)
        attrs = MAS_ATTRIBUTES[event_type]
        attr_exprs = []
        for name, dtype in MAS_ATTRIBUTE_TYPES.items():
            if name == "event_type":
                attr_exprs.append(F.lit(event_type).alias(name))
            elif name in attrs:
                attr_exprs.append(F.col(attrs[name]).cast(dtype).alias(name))
            else:
                attr_exprs.append(F.lit(None).cast(dtype).alias(name))
        selected = source.select(
            hash_key(components).alias(link.hash_key_column),
            F.col("event_id").alias(sat.subsequence_key),
            *attr_exprs,
            "record_source",
            "occurred_at",
        )
        frames.append(selected)
    if not frames:
        return 0
    candidates = frames[0]
    for frame in frames[1:]:
        candidates = candidates.unionByName(frame, allowMissingColumns=False)
    candidates = candidates.withColumn(
        "hash_diff", hash_diff([F.col(a) for a in MAS_ATTRIBUTE_COLUMNS])
    )
    deduped = _first_per_key(
        candidates, [link.hash_key_column, sat.subsequence_key], ["occurred_at"]
    )
    final = _with_load_dts(deduped)
    return insert_only_merge(
        spark,
        table_path(config, "raw_vault", sat_name),
        final,
        [link.hash_key_column, sat.subsequence_key],
    )


def load_effectivity_satellite(
    spark: SparkSession,
    config: LakehouseConfig,
    sat_name: str,
    staged: dict[str, DataFrame],
) -> int:
    """Load the effectivity satellite for the driving-key link (insert-only).

    Assignment intervals are recomputed from the driving key's full event
    history: each ``assigned`` event opens an interval; the next assignment
    event on the same issue (either action) closes it. Open intervals carry
    ``end_dts = 9999-12-31``; when a later batch closes one, the closed
    version is inserted as a *new* row and supersedes the open row at query
    time (resolution lives in the business vault).
    """
    sat = config.vault.satellite(sat_name)
    link = config.vault.link(sat.parent)
    if "IssuesEvent" not in staged:
        return 0
    events = staged["IssuesEvent"].filter(
        F.col("action").isin(ASSIGNMENT_ACTIONS) & F.col("assignee_login").isNotNull()
    )

    issue_window = Window.partitionBy("repo_name", "issue_number").orderBy(
        "occurred_at", "event_id"
    )
    with_next = events.withColumn("_next_change", F.lead("occurred_at").over(issue_window))
    intervals = with_next.filter(F.col("action") == "assigned").select(
        hash_key([F.col("repo_name"), F.col("issue_number"), F.col("assignee_login")]).alias(
            link.hash_key_column
        ),
        hash_key([F.col("repo_name"), F.col("issue_number")]).alias("hk_issue"),
        hash_key([F.col("assignee_login")]).alias("hk_actor"),
        F.col("occurred_at").alias("start_dts"),
        F.coalesce(F.col("_next_change"), F.lit(HIGH_DTS).cast("timestamp")).alias("end_dts"),
        "record_source",
    )
    final = _with_load_dts(intervals)
    return insert_only_merge(
        spark,
        table_path(config, "raw_vault", sat_name),
        final,
        [link.hash_key_column, "start_dts", "end_dts"],
    )
