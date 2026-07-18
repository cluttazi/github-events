# pipelines/gold

Information marts — the consumer-facing star-schema layer.

| Mart | Grain | Facts | Dimensions (SCD behavior) |
|---|---|---|---|
| `repo_activity_mart` | repo x day x event type | events, distinct actors, push commits | repo profile + stats **as of that day** via `pit_repo_day` (PIT-resolved type-2 semantics) |
| `developer_360_mart` | actor x day | events, repos touched, pushes/commits, stars, PR/issue events | actor profile as-of-day via `pit_actor_day`; lifetime collaboration columns (prs/issues acted, prs merged) from bridge + `bsat_pr_lifecycle` (type-1 semantics, documented) |
| `collaboration_mart` | one row per PR/issue item | participants, milestones, resolution hours, assignee churn | current PR state via latest `pit_pull_request_day` pointer; active assignee from resolved effectivity |

## Surrogate handling

Marts expose business keys (`repo_name`, `actor_login`, item numbers) for
consumers and carry the vault hash key (`hk_*`) as the lineage-stable
surrogate back-reference. No new surrogates are minted in gold.

## Why marts are physical tables rebuilt by overwrite

The template favors materialized gold at demo scale (simple to inspect,
cheap to rebuild, no view/engine coupling); history is never lost because
the raw vault is the insert-only system of record. Virtualized marts over
PIT+satellites are a straightforward swap if consumption patterns change.

## Why PySpark (not dbt/Scala)

One engine end-to-end keeps the single hashing definition and lets the
Databricks Asset Bundle chain wheel tasks Bronze → Raw Vault → Business
Vault → Gold. See docs/adr/002-pyspark-gold-over-dbt-and-scala.md.
