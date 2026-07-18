# ADR 001 — Bronze is batch COPY INTO only (no streaming path)

**Status**: accepted

## Context

The template repo ships two bronze mechanisms: a Structured Streaming CDC
path (checkpoints, Kafka option) and a batch `copy_into` path with an
explicit file ledger. GH-Archive delivers hourly NDJSON files — there is no
CDC stream and no notification infrastructure to model.

## Decision

One bronze mechanism: `copy_into` semantics with a Delta file ledger
(`bronze/ops/file_ledger`). Each run loads the set difference of landing
files; envelope validation and quarantine happen at load (the template only
quarantined on its streaming path).

## Consequences

* One idempotency story — re-running bronze is a ledger no-op by construction.
* No checkpoints directory, no Kafka package, no docker-compose stack.
* A resent file under a new name loads again (new path = new file), exactly
  as Databricks COPY INTO behaves; the raw vault deduplicates downstream.
