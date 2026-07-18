# Modernization audit — github-events-lakehouse

Date: 2026-07-18 · Branch: `claude/repo-modernization-audit-6se2sn`

## 1. Gate results (baseline, before any change)

| Gate | Command | Result |
|---|---|---|
| Env sync | `uv sync --group dev` | PASS |
| Lint / format / types | `make lint` (ruff check, ruff format --check, mypy strict) | PASS (70 files clean) |
| Tests | `make test` | PASS — 71/71 in ~7 min (Spark suites included) |
| End-to-end demo | `make demo` (2000 events, seed 42) | PASS — all 7 steps green, incl. idempotency proof |
| Vault idempotency | `make vault-verify` | PASS — re-run inserted 0 rows in every object |
| Contract compatibility | `uv run python -m quality.contracts.compat` | PASS (no multi-version lineages yet) |
| Governance drift | render + `git diff governance/unity_catalog/generated/` | PASS — no drift |

The repo is in excellent shape; this is a light-touch pass, not a rescue.

## 2. Dependency inventory

Locked (`uv.lock`) vs latest stable on PyPI (checked 2026-07-18):

| Package | Constraint | Locked | Latest | Verdict |
|---|---|---|---|---|
| pyspark | `==4.1.1` | 4.1.1 | 4.2.0 | **Hold.** delta-spark 4.3.1 requires pyspark `>=4.0.1,<=4.1.1`; CLAUDE.md lockstep rule — never move one side alone. |
| delta-spark | `==4.3.1` | 4.3.1 | 4.3.1 | Current. |
| faker | `~=37.0` | 37.12.0 | 40.31.0 | **Defer.** Runtime dep feeding the deterministic generators; 3 major versions could change seeded output. Not a "clearly safe" bump. |
| pydantic | `~=2.13` | 2.13.4 | 2.13.4 | Current. |
| pyyaml | `~=6.0` | 6.0.3 | 6.0.3 | Current. |
| pytest | `~=8.3` | 8.4.2 | 9.1.1 | **Bump candidate** (dev tool) — gates must stay green. |
| ruff | `~=0.14.0` | 0.14.14 | 0.15.22 | **Bump candidate** (dev tool) — gates must stay green. |
| mypy | `~=1.15` | 1.20.2 | 2.3.0 | **Bump candidate** (dev tool, major) — only if strict mode stays clean. |
| types-pyyaml | `~=6.0` | 6.0.12.20260518 | same | Current. |

## 3. CI workflows

`ci.yml` mirrors the local gates faithfully: lint + mypy + contract compat +
governance drift, pytest split (non-spark then spark), terraform fmt/validate,
offline bundle validate. `demo.yml` proves `make demo` weekly and on main.
No gate gap found.

Action versions in use vs current majors (GitHub API is unreachable through
this environment's proxy, so "latest" is from training knowledge, early 2026):

| Action | Used | Current major | Verdict |
|---|---|---|---|
| actions/checkout | v4 | v5 | bump |
| actions/setup-java | v4 | v5 | bump |
| astral-sh/setup-uv | v5 | v7 | bump (workflow only uses `enable-cache`, supported across majors) |
| actions/upload-artifact | v4 | v5 | bump |
| hashicorp/setup-terraform | v3 | v3 | current |
| databricks/setup-cli | @main | — | intentionally floating; leave |

## 4. Planned changes (Phase 2/3)

1. `chore(dev-deps): bump ruff/mypy/pytest` — raise constraints, re-lock,
   re-run `make lint` + `make test`; fix only trivial fallout, otherwise
   revert the offending tool.
2. `ci:` bump action majors listed above; no workflow-logic changes.
3. No contract, hashing, vault-loader, or generator changes: all hard rules
   (ANSI on, hashing via `pipelines/common/hashing.py`, insert-only vault,
   contracts as source of truth, deterministic seeds) are respected and no
   violations were found in the audit.

## 5. Explicitly out of scope / deferred

- pyspark 4.2.0 — blocked until delta-spark certifies a matching release
  (verified-pair rule in CLAUDE.md).
- faker 38–40 — runtime data-shape risk for a cosmetic gain; revisit with a
  dedicated run that re-baselines the demo summary.
- databricks/setup-cli pin — floating `@main` is a deliberate upstream
  recommendation; leave as-is.

## 6. Done vs deferred

(filled in at the end of the pass — see final section below)
