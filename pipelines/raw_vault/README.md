# pipelines/raw_vault

Data Vault 2.0 raw vault: hubs, links, satellites — hard rules only.

## Objects

| Object | Kind | Key | Notes |
|---|---|---|---|
| `hub_actor` | hub | `hk_actor` ← `actor_login` | assignees load here too (RI for `link_issue_assignee`) |
| `hub_repo` | hub | `hk_repo` ← `repo_name` | forkees load here too |
| `hub_pull_request` | hub | `hk_pull_request` ← `(repo_name, pr_number)` | composite business key |
| `hub_issue` | hub | `hk_issue` ← `(repo_name, issue_number)` | composite business key |
| `link_actor_repo` | link | `lhk_actor_repo` | carrier of the event stream |
| `link_actor_pull_request` | link | 3-way (actor, PR, repo) | |
| `link_actor_issue` | link | 3-way (actor, issue, repo) | |
| `link_issue_assignee` | link | driving key `hub_issue` | → effectivity satellite |
| `sat_actor_profile` | satellite | `(hk_actor, hash_diff)` | |
| `sat_repo_profile` | satellite (slow) | `(hk_repo, hash_diff)` | owner/language/branch/license |
| `sat_repo_stats` | satellite (fast) | `(hk_repo, hash_diff)` | star/fork/issue/watcher counts — split by rate of change |
| `sat_pull_request_details` | satellite | `(hk_pull_request, hash_diff)` | |
| `sat_issue_details` | satellite | `(hk_issue, hash_diff)` | |
| `sat_actor_repo_event` | multi-active satellite | `(lhk_actor_repo, event_id)` | one row per event |
| `eff_sat_issue_assignee` | effectivity satellite | `(lhk, start_dts, end_dts)` | insert-only intervals |
| `quarantine` | ops | `event_id` | contract violations with reasons |

## Why loads recompute from full bronze history

Candidates are pure functions of the (append-only) bronze table; insert-only
MERGEs turn unchanged candidates into no-ops. That makes every load
deterministic and idempotent — `python -m pipelines.raw_vault.job
--verify-idempotent` re-runs the whole load and fails unless every object
gains zero rows. O(history) per run is the honest trade at demo scale; the
production variant narrows the recompute window to keys touched since the
last run without changing the MERGE.

## Where the rules live

- **Topology** (which hubs/links/sats exist): `config/lakehouse.yaml`
- **Extraction** (JSON paths → contract columns): `staging.py`
- **Hard rules** (nullability, enums): contracts, applied in `enforcement.py`
- **Hashing**: `pipelines/common/hashing.py` only (ADR 004)
- **No business logic here** — soft rules live in the business vault (ADR 003)
