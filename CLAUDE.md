# CLAUDE.md — agent instructions for github-events-lakehouse

## What this repo is

A local-first lakehouse over a synthetic GitHub events feed, implementing
Data Vault 2.0 inside a medallion architecture: seeded GH-Archive-style
NDJSON ingestion (bronze) → hubs/links/satellites (raw vault) → PIT/bridge/
derived satellites (business vault) → three information marts (gold), with
versioned data contracts, a custom DQ framework, and governance-as-code.
Everything runs without Docker or cloud credentials.

## Commands

```bash
uv sync --group dev          # environment (Python 3.11, Java 21 required)
make demo                    # full end-to-end run with summary
make events bronze ...       # individual steps (see make help)
make lint                    # ruff check + format check + mypy (all must pass)
make test                    # pytest; spark-marked tests need the JVM
```

## Hard rules

- **Version lockstep**: `pyspark==4.1.1` + `delta-spark==4.3.1` in
  `pyproject.toml` are a verified pair. Java 21 requires Spark 4.x — never
  downgrade to the 3.5 line.
- **ANSI mode stays on.** Use `try_cast` on untrusted data; never set
  `spark.sql.ansi.enabled=false`.
- **The hash rules are defined once** (`pipelines/common/hashing.py`):
  SHA-256, uppercase-trim, `||` delimiter, `^^` null token, lowercase hex.
  Every hub hash key, link hash key, and satellite hash diff goes through
  that module — never inline a hash expression (ADR 004).
- **The raw vault is insert-only.** No updates or deletes; history lives in
  satellites via `load_dts`. Re-running a load must add zero rows
  (`make vault-verify` proves it). Business logic (derivations, soft rules)
  belongs in the business vault or gold, never bronze or raw vault (ADR 003).
- **Contracts are the source of truth** (`quality/contracts/definitions`).
  Schema changes go through a new contract version that passes
  `uv run python -m quality.contracts.compat`. PII flags there drive the
  generated governance artifacts — after touching contracts or
  `governance/unity_catalog/grants.yaml`, run
  `uv run python -m governance.unity_catalog.render` and commit the diff
  (CI fails on drift).
- **No real data, ever.** All events are Faker-generated under seeds; keep
  generators deterministic (seeded RNG + logical clock, no wall-time in
  payloads).
- `data/` is disposable (`make clean`); never commit it.

## Conventions

- Conventional commits (`feat(raw-vault): ...`, `docs: ...`).
- Type hints everywhere; mypy strict is a gate. New untyped third-party
  deps get a targeted override in `pyproject.toml`, never a blanket ignore.
- Every module README explains *why*, not just how; significant design
  trade-offs get an ADR in `docs/adr/`.
- Tests: unit tests colocated by module under `tests/unit/…`; anything
  starting a SparkSession gets `@pytest.mark.spark` (or module-level
  `pytestmark`); cross-layer flows live in `tests/integration/`.

## Architecture pointers

Read `docs/architecture.md` first, then the ADR index in `docs/adr/` and the
object catalog in `docs/data_vault_catalog.md`. The one intentional
cross-module dependency is `pipelines/common` (config + session factory +
hashing) — everything else communicates through data (Delta tables, JSON
results), not imports.
