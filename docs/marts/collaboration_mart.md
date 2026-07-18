# collaboration_mart

**Grain**: one row per collaboration item — `(item_type, repo_name,
item_number)` where `item_type ∈ {pull_request, issue}`
**Consumers**: engineering managers (PR/issue flow, cycle time, assignment
health)
**SCD behavior**: current-state per item (type 1). The PR's current state
resolves through the *latest* `pit_pull_request_day` pointer; the active
assignee comes from the resolved effectivity satellite (closed intervals
supersede open ones). Full history remains queryable in the raw vault.

## Entity diagram

```mermaid
erDiagram
    hub_pull_request ||--o{ sat_pull_request_details : describes
    hub_pull_request ||--o{ pit_pull_request_day : "as-of pointers"
    hub_issue ||--o{ link_issue_assignee : "driving key"
    link_issue_assignee ||--o{ eff_sat_issue_assignee : "assignment intervals"
    hub_actor ||--o{ link_actor_pull_request : participates
    hub_actor ||--o{ link_actor_issue : participates
    link_actor_pull_request ||--o{ bridge_repo_collaboration : flattened
    link_actor_issue ||--o{ bridge_repo_collaboration : flattened
    eff_sat_issue_assignee ||--o{ bridge_repo_collaboration : "active assignees"
    bridge_repo_collaboration ||--o{ collaboration_mart : "participants + traversal"
    bsat_pr_lifecycle ||--o{ collaboration_mart : "PR milestones"
    bsat_issue_lifecycle ||--o{ collaboration_mart : "issue milestones"

    collaboration_mart {
        string item_type PK
        string repo_name PK
        int item_number PK
        long participants
        string active_assignee
        string pr_state
        timestamp opened_at
        timestamp closed_at
        boolean is_merged
        double resolution_hours
        int assignee_change_count
    }
```

## Lineage

```
bronze/github_events
  -> raw_vault: hub_pull_request, hub_issue, hub_actor,
                link_actor_pull_request, link_actor_issue,
                link_issue_assignee (driving key: issue) + eff_sat_issue_assignee,
                sat_pull_request_details, sat_issue_details
  -> business_vault: bridge_repo_collaboration (3-link traversal),
                     bsat_pr_lifecycle, bsat_issue_lifecycle, pit_pull_request_day
  -> gold/collaboration_mart                  (pipelines/gold/collaboration.py)
```

This mart is the full DV2.0 pattern exercise: multi-link traversal via the
bridge, driving-key effectivity resolution, computed business satellites,
and PIT state resolution in one star.
