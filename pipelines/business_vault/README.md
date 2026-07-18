# pipelines/business_vault

Business vault — the soft-rule layer between raw vault and gold.

| Table | Kind | Grain | Soft rules applied |
|---|---|---|---|
| `pit_repo_day` | PIT | `hk_repo` × day | end-of-day state pointers to both repo satellites; ghost `1900-01-01` for missing history |
| `pit_actor_day` | PIT | `hk_actor` × day | pointer to `sat_actor_profile` |
| `pit_pull_request_day` | PIT | `hk_pull_request` × day | pointer to `sat_pull_request_details` |
| `bridge_repo_collaboration` | bridge | repo × actor × item × relationship | flattens 3 link traversals; assignee rows use *resolved* (active) effectivity intervals; recovers `hk_repo` for the assignee link via the issue hub |
| `bsat_pr_lifecycle` | computed satellite | one row per PR | first `opened`/`closed` as milestones, merged flag, `cycle_time_hours` |
| `bsat_issue_lifecycle` | computed satellite | one row per issue | `time_to_close_hours`, assignee churn from effectivity history |

## Why rebuilt-by-overwrite

The raw vault is the insert-only system of record; everything here is a
deterministic function of it. Rebuilding is cheaper to reason about than
incrementally maintaining derived state, and idempotency is trivial: same
vault in, same tables out.

## Why PIT points at event time

`load_dts` is wall-clock arrival — after one batch load every satellite row
shares one instant, which cannot express "state as of last Tuesday". The
reporting timeline is `occurred_at` (event time); see DECISIONS.md.
