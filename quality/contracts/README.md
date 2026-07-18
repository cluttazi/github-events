# quality/contracts

Versioned, executable data contracts — the source of truth for staged
schemas, nullability, enums, and PII flags.

## What a contract describes here

Each `definitions/<event_type>.v<N>.yaml` describes the **flattened staged
schema** raw-vault staging produces from bronze `raw_value` — not the nested
source JSON. The contract language is flat (a deliberate template
convention); nesting knowledge lives in `pipelines/raw_vault/staging.py`,
which is code and therefore testable.

## Lifecycle

Schema changes never edit an existing version — they add
`<event_type>.v<N+1>.yaml`. `compat.py` (a CI gate) allows only
non-breaking evolution: additive nullable fields, loosened nullability,
grown enums. `loader.py` serves the highest version for enforcement.

PII flags (`pii: true` + `pii_category`) drive the generated governance
artifacts (`governance/unity_catalog/generated/pii_tags.sql`) — after
touching contracts, run `make render` and commit the diff.
