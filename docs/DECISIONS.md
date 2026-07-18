# DECISIONS

Assumptions made where the template repo (`cluttazi/data-engineering`) is
silent, plus deliberate adaptations. Larger trade-offs have full ADRs in
`docs/adr/`; this is the complete ledger.

| # | Decision | Rationale |
|---|---|---|
| 1 | **Bronze is batch-only** (`copy_into` + file ledger); no streaming, Kafka, or docker-compose | GH-Archive is an hourly-file feed — the template's own ADR-001 case for an auditable ledger over notification infra. One load mechanism = one idempotency story. (ADR 001) |
| 2 | **Gold marts in PySpark**; the template's dbt-duckdb and Scala layers are dropped | One engine keeps the single hashing definition and a uniform DAG for the asset bundle; avoids the cross-language lockstep hazard the template itself flags. (ADR 002) |
| 3 | **Hash normalization**: SHA-256, uppercase-trim, `\|\|` delimiter, `^^` null token, lowercase hex | Case-insensitive GitHub logins; null ≠ empty string; hex over binary for debuggability. Defined once in `pipelines/common/hashing.py`. (ADR 004) |
| 4 | **`actor_login` is the actor business key** (not the numeric GitHub id) | Readability of the demo vault; rename risk accepted for synthetic data. A production feed would key on the durable numeric id with login in the profile satellite. |
| 5 | **No `hub_org`** — the repo owner is an attribute in `sat_repo_profile` | Owner is a slowly-changing property of the repo in this feed; a fifth hub adds objects without exercising any new DV construct. |
| 6 | **Watch/Fork/Release events model only as multi-active satellite rows** (no release hub etc.) | They carry no business key beyond actor/repo; the event stream satellite is their natural home. |
| 7 | **Bronze envelope substitutes `event_type` for the template's CDC `op`** | GH events are facts, not change records; `event_type` is the routing discriminator with the same envelope position. |
| 8 | **One bronze table for all event types** | Single-feed fidelity; per-type schema knowledge belongs to contracts + staging, not bronze. |
| 9 | **Contracts describe the flattened staged schema**, not the nested source JSON | The template contract language is flat; nesting knowledge lives (typed, tested) in `pipelines/raw_vault/staging.py`. |
| 10 | **Standard-satellite grain is `(hash_key, hash_diff)`** — one row per distinct attribute state, ordered by `occurred_at`; `load_dts` stays wall-clock arrival time | Deterministic + idempotent under full-history recompute. Consequence: a state that flips A→B→A keeps only the first A row. Acceptable for this domain; noted per satellite. |
| 11 | **Raw-vault loads recompute candidates from full bronze history each run** | Pure function of bronze ⇒ re-run inserts zero rows (proof: `make vault-verify`). O(history) is honest at demo scale; production narrows the window without changing the MERGE. (ADR 003) |
| 12 | **Effectivity satellite is insert-only with supersession**: intervals keyed `(lhk, start_dts, end_dts)`; closing an interval inserts the closed version; resolution (min `end_dts` per `(lhk, start_dts)`) lives in the business vault | Keeps the raw vault strictly insert-only; "at most one active per driving key" is enforced by resolution and covered by tests. (ADR 003) |
| 13 | **PIT pointers use event time (`occurred_at`), not `load_dts`** | With batch loads all rows share one wall-clock instant; only event time can express "state as of day D". Pointer columns are named `<sat>_pit_ts`. |
| 14 | **Business vault and gold are rebuilt by deterministic overwrite** | Derived layers; the insert-only system of record is the raw vault. Same input ⇒ same output makes idempotency trivial. |
| 15 | **MAS grain is `(lhk_actor_repo, event_id)`** | The GH event id is the natural subsequence key; one row per event. |
| 16 | **`actor_login` (and display/avatar/assignee fields) flagged PII `quasi_identifier`** | Public data, but flagged to exercise the contract→governance rendering path end-to-end. |
| 17 | **PII physical placement map lives in `governance/unity_catalog/render.py`** and rendering fails if a contract PII field is unmapped | Staging renames columns on the way into the vault, so contract-name-based tagging (template style) can't locate physical columns; the validated map keeps "derived, never hand-maintained" honest. |
| 18 | **Terraform assumes an existing workspace + metastore** (unlike the template's full AWS provisioning) | The interesting surface for this project is the UC catalog/schema/grant layout; workspace bootstrap is orthogonal. |
| 19 | **Databricks Asset Bundle added** (template has none; user-approved extension); validate-only in CI, `LAKEHOUSE_ROOT` env targeting at runtime, UC DDL owned by terraform | Same code paths locally and deployed; deploys stay explicit human actions. (ADR 005) |
| 20 | **Per-environment config via `LAKEHOUSE_ROOT`/env vars + terraform `environment` variable**, not per-env YAML files | Mirrors the template exactly: one runtime YAML, env overrides at the edges. |
| 21 | **Hash parity is ASCII-scoped**: Python `str.upper()`/`strip()` and Spark `upper`/`trim` are proven byte-identical on the synthetic data's ASCII keys | Unicode edge cases (locale casing) are out of scope for the demo; a production feed would pin a normalization form first. |
