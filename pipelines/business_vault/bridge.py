"""Bridge table: pre-joined traversal across the collaboration links.

``bridge_repo_collaboration`` flattens three link traversals into one
mart-ready table — who touched which PR/issue in which repo, and how:

* ``pr_actor``       — via ``link_actor_pull_request``
* ``issue_actor``    — via ``link_actor_issue``
* ``issue_assignee`` — via ``link_issue_assignee`` + the *resolved*
  effectivity satellite (active assignments only)

Business keys are resolved from the hubs at build time so gold marts never
join raw-vault objects directly. Rebuilt each run (derived layer).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipelines.common.hashing import hash_key


def bridge_repo_collaboration(
    hub_actor: DataFrame,
    hub_repo: DataFrame,
    hub_pull_request: DataFrame,
    hub_issue: DataFrame,
    link_actor_pull_request: DataFrame,
    link_actor_issue: DataFrame,
    resolved_assignments: DataFrame,
) -> DataFrame:
    """Union of the three traversals with hub-resolved business keys."""
    actors = hub_actor.select("hk_actor", "actor_login")
    repos = hub_repo.select("hk_repo", "repo_name")

    prs = hub_pull_request.select(
        "hk_pull_request",
        F.col("pr_number").alias("item_number"),
        F.col("repo_name").alias("_item_repo"),
    )
    pr_rows = (
        link_actor_pull_request.join(actors, "hk_actor")
        .join(repos, "hk_repo")
        .join(prs, "hk_pull_request")
        .select(
            "hk_repo",
            "repo_name",
            "hk_actor",
            "actor_login",
            F.lit("pull_request").alias("item_type"),
            F.col("hk_pull_request").alias("hk_item"),
            "item_number",
            F.lit("pr_actor").alias("relationship_type"),
        )
    )

    issues = hub_issue.select(
        "hk_issue",
        F.col("issue_number").alias("item_number"),
        F.col("repo_name").alias("_item_repo"),
    )
    issue_rows = (
        link_actor_issue.join(actors, "hk_actor")
        .join(repos, "hk_repo")
        .join(issues, "hk_issue")
        .select(
            "hk_repo",
            "repo_name",
            "hk_actor",
            "actor_login",
            F.lit("issue").alias("item_type"),
            F.col("hk_issue").alias("hk_item"),
            "item_number",
            F.lit("issue_actor").alias("relationship_type"),
        )
    )

    # link_issue_assignee carries no hk_repo; the traversal recovers it from
    # the issue hub's business key — the multi-link hop bridges exist for.
    issue_repo = hub_issue.select(
        "hk_issue",
        F.col("issue_number").alias("item_number"),
        hash_key([F.col("repo_name")]).alias("hk_repo"),
    )
    assignee_rows = (
        resolved_assignments.filter(F.col("is_active"))
        .join(actors, "hk_actor")
        .join(issue_repo, "hk_issue")
        .join(repos, "hk_repo")
        .select(
            "hk_repo",
            "repo_name",
            "hk_actor",
            "actor_login",
            F.lit("issue").alias("item_type"),
            F.col("hk_issue").alias("hk_item"),
            "item_number",
            F.lit("issue_assignee").alias("relationship_type"),
        )
    )

    return pr_rows.unionByName(issue_rows).unionByName(assignee_rows).distinct()
