# ADR 003 — Insert-only raw vault, recompute-then-merge, idempotency as a gate

**Status**: accepted

## Decision

Raw-vault loads are pure functions of the (append-only) bronze table:
candidates are recomputed from full history each run and MERGEd insert-only
— `whenNotMatchedInsertAll` with no matched clause, so nothing is ever
updated or deleted. Merge keys: hubs/links on the hash key; standard
satellites on `(hash_key, hash_diff)`; the multi-active satellite on
`(link hash key, event_id)`; the effectivity satellite on
`(link hash key, start_dts, end_dts)` with closed intervals inserted as new
rows that supersede their open versions at query time.

Idempotency is not a convention but a gate: `pipelines.raw_vault.job
--verify-idempotent` re-runs the entire load and fails unless every object
gains zero rows. The demo and the deployed job both run it every time.

Business logic (lifecycle milestones, effectivity resolution, cycle times)
lives only in the business vault and gold, which are rebuilt by
deterministic overwrite — they are derived, recomputable projections of the
insert-only system of record.

## Consequences

* O(history) recompute per run — honest at demo scale; the production
  variant narrows the candidate window to keys touched since the last run
  without changing merge semantics.
* Satellite history is by *distinct state*: A->B->A retains the first A only
  (see DECISIONS #10).
