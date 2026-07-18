# Data Vault object catalog

Every generated object: its medallion layer, Data Vault 2.0 type, the code
that produces it, and where it lives (local Delta path under
`data/lakehouse/`; Unity Catalog name per `platform/terraform`).

## Tables

| Object | Layer | DV 2.0 type | Producing code | Local Delta path | Unity Catalog |
|---|---|---|---|---|---|
| `github_events` | Bronze | persistent staging (append-only) | `pipelines/bronze/copy_into.py` | `bronze/github_events` | `bronze.github.github_events` |
| `quarantine` (bronze) | Bronze | envelope quarantine | `pipelines/bronze/copy_into.py` | `bronze/quarantine` | `bronze.quarantine.github_events` |
| `file_ledger` | Bronze | ops (exactly-once ledger) | `pipelines/bronze/copy_into.py` | `bronze/ops/file_ledger` | `bronze.ops.file_ledger` |
| `hub_actor` | Silver | hub (bk `actor_login`) | `pipelines/raw_vault/loaders.py` | `raw_vault/hub_actor` | `silver.raw_vault.hub_actor` |
| `hub_repo` | Silver | hub (bk `repo_name`) | `pipelines/raw_vault/loaders.py` | `raw_vault/hub_repo` | `silver.raw_vault.hub_repo` |
| `hub_pull_request` | Silver | hub (composite bk `repo_name, pr_number`) | `pipelines/raw_vault/loaders.py` | `raw_vault/hub_pull_request` | `silver.raw_vault.hub_pull_request` |
| `hub_issue` | Silver | hub (composite bk `repo_name, issue_number`) | `pipelines/raw_vault/loaders.py` | `raw_vault/hub_issue` | `silver.raw_vault.hub_issue` |
| `link_actor_repo` | Silver | link (2-way) | `pipelines/raw_vault/loaders.py` | `raw_vault/link_actor_repo` | `silver.raw_vault.link_actor_repo` |
| `link_actor_pull_request` | Silver | link (3-way) | `pipelines/raw_vault/loaders.py` | `raw_vault/link_actor_pull_request` | `silver.raw_vault.link_actor_pull_request` |
| `link_actor_issue` | Silver | link (3-way) | `pipelines/raw_vault/loaders.py` | `raw_vault/link_actor_issue` | `silver.raw_vault.link_actor_issue` |
| `link_issue_assignee` | Silver | link (driving key `hub_issue`) | `pipelines/raw_vault/loaders.py` | `raw_vault/link_issue_assignee` | `silver.raw_vault.link_issue_assignee` |
| `sat_actor_profile` | Silver | satellite (standard) | `pipelines/raw_vault/loaders.py` | `raw_vault/sat_actor_profile` | `silver.raw_vault.sat_actor_profile` |
| `sat_repo_profile` | Silver | satellite (standard, slow-changing) | `pipelines/raw_vault/loaders.py` | `raw_vault/sat_repo_profile` | `silver.raw_vault.sat_repo_profile` |
| `sat_repo_stats` | Silver | satellite (standard, fast-changing — rate-of-change split) | `pipelines/raw_vault/loaders.py` | `raw_vault/sat_repo_stats` | `silver.raw_vault.sat_repo_stats` |
| `sat_pull_request_details` | Silver | satellite (standard) | `pipelines/raw_vault/loaders.py` | `raw_vault/sat_pull_request_details` | `silver.raw_vault.sat_pull_request_details` |
| `sat_issue_details` | Silver | satellite (standard) | `pipelines/raw_vault/loaders.py` | `raw_vault/sat_issue_details` | `silver.raw_vault.sat_issue_details` |
| `sat_actor_repo_event` | Silver | **multi-active satellite** (subsequence key `event_id`) | `pipelines/raw_vault/loaders.py` | `raw_vault/sat_actor_repo_event` | `silver.raw_vault.sat_actor_repo_event` |
| `eff_sat_issue_assignee` | Silver | **effectivity satellite** (driving key `hub_issue`) | `pipelines/raw_vault/loaders.py` | `raw_vault/eff_sat_issue_assignee` | `silver.raw_vault.eff_sat_issue_assignee` |
| `quarantine` (raw vault) | Silver | contract-violation quarantine | `pipelines/raw_vault/job.py` | `raw_vault/quarantine` | `silver.raw_vault.quarantine` |
| `pit_repo_day` | Silver | PIT table (hub_repo × day) | `pipelines/business_vault/pit.py` | `business_vault/pit_repo_day` | `silver.business_vault.pit_repo_day` |
| `pit_actor_day` | Silver | PIT table (hub_actor × day) | `pipelines/business_vault/pit.py` | `business_vault/pit_actor_day` | `silver.business_vault.pit_actor_day` |
| `pit_pull_request_day` | Silver | PIT table (hub_pull_request × day) | `pipelines/business_vault/pit.py` | `business_vault/pit_pull_request_day` | `silver.business_vault.pit_pull_request_day` |
| `bridge_repo_collaboration` | Silver | bridge table (3-link traversal) | `pipelines/business_vault/bridge.py` | `business_vault/bridge_repo_collaboration` | `silver.business_vault.bridge_repo_collaboration` |
| `bsat_pr_lifecycle` | Silver | computed (business) satellite | `pipelines/business_vault/derived.py` | `business_vault/bsat_pr_lifecycle` | `silver.business_vault.bsat_pr_lifecycle` |
| `bsat_issue_lifecycle` | Silver | computed (business) satellite | `pipelines/business_vault/derived.py` | `business_vault/bsat_issue_lifecycle` | `silver.business_vault.bsat_issue_lifecycle` |
| `repo_activity_mart` | Gold | information mart (fact + PIT-resolved dims) | `pipelines/gold/repo_activity.py` | `gold/repo_activity_mart` | `gold.marts.repo_activity_mart` |
| `developer_360_mart` | Gold | information mart | `pipelines/gold/developer_360.py` | `gold/developer_360_mart` | `gold.marts.developer_360_mart` |
| `collaboration_mart` | Gold | information mart | `pipelines/gold/collaboration.py` | `gold/collaboration_mart` | `gold.marts.collaboration_mart` |
| `pipeline_run_metrics` | — | observability | `observability/metrics/writer.py` | `observability/pipeline_run_metrics` | `gold.observability.pipeline_run_metrics` |

## Supporting artifacts

| Artifact | Purpose | Path |
|---|---|---|
| Event contracts (×6) | staged-schema source of truth, PII flags | `quality/contracts/definitions/*.v1.yaml` |
| DQ suites (×19) | declarative invariants incl. Bronze→Vault reconciliation | `quality/expectations/suites/*.yaml` |
| Hash rules | the single DV2.0 hashing definition | `pipelines/common/hashing.py` |
| Vault topology | hubs/links/satellites/marts declaration | `config/lakehouse.yaml` |
| Governance | grants matrix + generated PII tags / access matrix | `governance/unity_catalog/` |
| UC DDL | catalogs/schemas/grants as code | `platform/terraform/` |
| Asset bundle | deployed job DAG (wheel entry points) | `databricks.yml`, `platform/databricks/resources/` |
| Orchestrator | local end-to-end demo with idempotency proof | `orchestration/demo.py` |
