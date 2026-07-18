# platform

Deployment surfaces. Nothing here runs in the local demo — the demo is
path-based Delta on a laptop; these define what production looks like.

## terraform/

Unity Catalog layout as code: the three medallion catalogs
(`github_events_<env>_{bronze,silver,gold}`), their schemas
(`bronze: github/quarantine/ops`, `silver: raw_vault/business_vault`,
`gold: marts/observability`), and the grants mirroring
`governance/unity_catalog/grants.yaml`. Assumes an existing workspace and
metastore (recorded in docs/DECISIONS.md); authentication comes from the
standard `DATABRICKS_*` environment variables. CI runs
`terraform fmt -check`, `init -backend=false`, `validate` only — applies are
always explicit and human-driven.

## databricks/ (+ /databricks.yml at the repo root)

The Databricks Asset Bundle: packages the repo as a wheel and deploys one
job whose task DAG encodes the layer dependencies —
`bronze_copy_into → raw_vault_load (--verify-idempotent) →
business_vault_build → gold_marts → data_quality`. Tasks invoke the same
`[project.scripts]` entry points the Makefile uses locally; the only
difference is `LAKEHOUSE_ROOT` pointing at a UC volume instead of `data/`.
CI validates the bundle offline; `databricks bundle deploy -t dev|prod` is
a deliberate human action.
