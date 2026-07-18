# repo_activity_mart

**Grain**: one row per `(repo_name, activity_date, event_type)`
**Consumers**: engineering-analytics dashboards (repo health, activity trends)
**SCD behavior**: repo dimension attributes are *as of the activity day*,
resolved through `pit_repo_day` pointers into the profile and stats
satellites (type-2 semantics served through PIT equi-joins; no window logic
in the mart).

## Entity diagram

```mermaid
erDiagram
    hub_repo ||--o{ sat_repo_profile : describes
    hub_repo ||--o{ sat_repo_stats : measures
    hub_repo ||--o{ pit_repo_day : "as-of pointers"
    hub_actor ||--o{ link_actor_repo : participates
    hub_repo ||--o{ link_actor_repo : participates
    link_actor_repo ||--o{ sat_actor_repo_event : "event stream (MAS)"
    pit_repo_day ||--o{ repo_activity_mart : "resolves dimensions"
    sat_actor_repo_event ||--o{ repo_activity_mart : "supplies facts"

    repo_activity_mart {
        string repo_name PK
        date activity_date PK
        string event_type PK
        long events_count
        long distinct_actors
        long push_commits
        string repo_owner
        string repo_language
        long stargazers_count
    }
```

## Lineage

```
landing NDJSON
  -> bronze/github_events                     (copy_into, ledger, quarantine)
  -> raw_vault: hub_repo, hub_actor, link_actor_repo,
                sat_actor_repo_event (facts), sat_repo_profile + sat_repo_stats (dims)
  -> business_vault: pit_repo_day             (end-of-day state pointers)
  -> gold/repo_activity_mart                  (pipelines/gold/repo_activity.py)
```

Facts aggregate the multi-active satellite (one row per event) grouped by
repo, day, and type; `push_commits` sums PushEvent sizes. Dimension columns
join through the PIT pointer for the row's own day — ghost-pointer days
surface as nulls, meaning "no repo detail observed yet".
