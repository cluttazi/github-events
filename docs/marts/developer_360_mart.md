# developer_360_mart

**Grain**: one row per `(actor_login, activity_date)` (active days only)
**Consumers**: developer-experience / community teams (contributor 360 view)
**SCD behavior**: actor profile is as-of-day via `pit_actor_day` (type-2
served through PIT). Lifetime collaboration columns (`prs_acted`,
`issues_acted`, `prs_merged`) are current-state type-1 by design — they
answer "who is this contributor overall", not "what were they that day"
(documented deliberately; see pipelines/gold/README.md).

## Entity diagram

```mermaid
erDiagram
    hub_actor ||--o{ sat_actor_profile : describes
    hub_actor ||--o{ pit_actor_day : "as-of pointers"
    hub_actor ||--o{ link_actor_repo : participates
    link_actor_repo ||--o{ sat_actor_repo_event : "event stream (MAS)"
    hub_actor ||--o{ bridge_repo_collaboration : "collaboration rows"
    bridge_repo_collaboration }o--|| bsat_pr_lifecycle : "merged PRs"
    pit_actor_day ||--o{ developer_360_mart : "resolves profile"
    sat_actor_repo_event ||--o{ developer_360_mart : "daily measures"
    bridge_repo_collaboration ||--o{ developer_360_mart : "lifetime measures"

    developer_360_mart {
        string actor_login PK
        date activity_date PK
        long events_count
        long repos_touched
        long pushes
        long commits_pushed
        long stars_given
        long prs_acted
        long prs_merged
    }
```

## Lineage

```
bronze/github_events
  -> raw_vault: hub_actor, link_actor_repo, sat_actor_repo_event, sat_actor_profile
  -> business_vault: pit_actor_day, bridge_repo_collaboration, bsat_pr_lifecycle
  -> gold/developer_360_mart                  (pipelines/gold/developer_360.py)
```
