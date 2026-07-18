# ADR 005 — Databricks Asset Bundle (validate-only CI, env-var path targeting)

**Status**: accepted (user-approved extension; the template has no bundle)

## Decision

`databricks.yml` + `platform/databricks/resources/lakehouse_job.yml` define
one job whose task DAG is the layer dependency chain: `bronze_copy_into ->
raw_vault_load (--verify-idempotent) -> business_vault_build -> gold_marts
-> data_quality`. Tasks are python_wheel_tasks hitting the same
`[project.scripts]` entry points the Makefile runs locally; the deployed job
differs from `make demo` only by `LAKEHOUSE_ROOT` pointing at a UC volume.

Responsibilities split three ways:
* **terraform** owns UC DDL (catalogs, schemas, grants),
* **the bundle** owns compute + job topology,
* **the code** owns everything else and never knows which environment it's in.

CI runs `databricks bundle validate -t dev` offline; deploys are explicit
(`databricks bundle deploy -t <target>`), never automatic.
