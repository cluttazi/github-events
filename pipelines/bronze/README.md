# pipelines/bronze

Batch loader with COPY INTO semantics for the GH-Archive landing zone.

## Why batch-only (no streaming)

GH-Archive is an hourly-file feed — the archetype of the case where an
explicit, auditable file ledger beats notification/checkpoint infrastructure
(docs/adr/001-batch-copy-into-only.md). One load mechanism means one
idempotency story: the ledger (`bronze/ops/file_ledger`) records every
ingested path, each run loads only the set difference, and re-running is a
no-op by construction.

## Why one table for all event types

The feed is a single stream whose payload schema varies by `type`. Bronze
keeps full source fidelity — the whole original line in `raw_value` — and
only extracts the minimal routing envelope (`event_key`, `event_type`,
`ts_ms`). Splitting by type here would push schema knowledge into bronze;
that knowledge belongs to the contracts, which the raw-vault staging applies
per event type.

## Quarantine over fail-fast

Lines that fail the envelope (unparseable JSON, missing id/timestamp,
unknown event type) land in `bronze/quarantine` with an `error_reason`.
The generator's `--corrupt-pct` count must equal the quarantine count —
integration tests assert this exactly.
