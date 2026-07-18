# github-events

GitHub events lakehouse: **medallion architecture + Data Vault 2.0** on
Delta Lake, built to the conventions of the
[`data-engineering`](https://github.com/cluttazi/data-engineering) template.
Seeded synthetic GH-Archive-style events flow Bronze → Raw Vault (hubs,
links, satellites — insert-only, provably idempotent) → Business Vault
(PIT, bridge, computed satellites) → three Gold information marts.
Everything runs locally without Docker or cloud credentials.

## Quickstart

```bash
uv sync --group dev     # Python 3.11 + Java 21 required
make demo               # events -> bronze -> raw vault (+idempotency proof)
                        #   -> business vault -> gold marts -> data quality
make help               # all individual targets
make test               # pytest (spark-marked tests need the JVM)
make lint               # ruff + mypy strict
```

The demo summary must end `all steps green (incl. raw-vault idempotency:
re-run added 0 rows)` — that line is the Data Vault 2.0 contract being
enforced, not decoration.

## What's inside

| Layer | Contents |
|---|---|
| Bronze | `copy_into` batch load with exactly-once file ledger, full-fidelity `raw_value`, envelope quarantine |
| Raw Vault | 4 hubs, 4 links (one with a driving key), 7 satellites — including a rate-of-change split, a **multi-active** satellite (the event stream) and an **effectivity** satellite (issue assignments) — all keyed by the single SHA-256 hash definition in `pipelines/common/hashing.py` |
| Business Vault | 3 PIT tables, `bridge_repo_collaboration`, computed lifecycle satellites |
| Gold | `repo_activity_mart`, `developer_360_mart`, `collaboration_mart` |
| Quality | versioned data contracts + compatibility gate; 19 declarative DQ suites incl. Bronze→Vault row reconciliation |
| Governance | grants-as-code, PII tags generated from contract flags (CI drift gate) |
| Platform | Terraform Unity Catalog layout; Databricks Asset Bundle running the same wheel entry points as `make demo` |

## Repository layout

```
config/            lakehouse.yaml — runtime config + vault topology declaration
ingestion/         seeded GH-Archive-style NDJSON event generator
pipelines/
  common/          config loader, Spark session factory, THE hash definition
  bronze/          COPY INTO-semantics batch loader (ledger + quarantine)
  raw_vault/       staging, contract enforcement, generic insert-only loaders, job
  business_vault/  PIT, bridge, derived satellites (soft rules live here)
  gold/            the three information marts
quality/           contracts (+compat gate) and the declarative DQ framework
governance/        UC grants matrix + generated PII tags / access matrix
observability/     pipeline_run_metrics writer + run report
orchestration/     end-to-end demo sequencer
platform/          terraform (UC DDL) + databricks asset bundle resources
docs/              architecture, ADRs, per-mart diagrams, DECISIONS, object catalog
tests/             unit + spark-marked + integration suites
```

## Documentation

- [docs/architecture.md](docs/architecture.md) — medallion ↔ DV2.0 mapping, data flow
- [docs/data_vault_catalog.md](docs/data_vault_catalog.md) — every object: layer, DV type, path
- [docs/marts/](docs/marts) — per-mart entity diagrams (Mermaid) and lineage
- [docs/adr/](docs/adr) — the five recorded trade-offs
- [docs/DECISIONS.md](docs/DECISIONS.md) — assumption ledger vs. the template
