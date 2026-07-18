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

### Done

- `docs:` this audit plan (baseline gate results + inventory).
- `chore(dev-deps):` ruff `~=0.14.0` → `~=0.15.22`, mypy `~=1.15` → `~=2.3`,
  pytest `~=8.3` → `~=9.1`; `uv.lock` re-resolved. Gates re-run after the
  bump: `make lint` clean (ruff check + format, mypy strict 70 files),
  `make test` 71/71 passing including all Spark suites.
- `ci:` action majors bumped — `actions/checkout` v4→v5,
  `actions/setup-java` v4→v5, `astral-sh/setup-uv` v5→v7,
  `actions/upload-artifact` v4→v5. Workflow logic untouched; both files
  YAML-validated. CI already mirrors every local gate (lint, mypy, contract
  compat, governance drift, split pytest, terraform, offline bundle
  validate, weekly `make demo`), so no gate additions were needed.

### Deferred (with reasons)

- **pyspark 4.2.0**: delta-spark 4.3.1 pins pyspark `<=4.1.1`; the CLAUDE.md
  verified-pair rule forbids moving one side alone. Revisit when delta-spark
  ships a release certified against Spark 4.2.
- **faker 40.x** (from 37.12): runtime dependency behind the deterministic
  event generators; a 3-major jump can change seeded output shapes.
  Deserves its own pass with a demo re-baseline, not a light-touch bump.
- **databricks/setup-cli@main**: floating pin is the upstream-documented
  usage; left as-is.
- **No code/bug fixes**: the audit found zero failing gates and no
  hard-rule violations (hashing centralized, ANSI on, insert-only vault
  verified by re-run, contracts/governance drift-free), so there was
  nothing to fix without inventing work.

## 7. Summary (PR-description style)

Light-touch modernization pass. Baseline audit first: every repo gate —
`uv sync`, `make lint`, `make test` (71/71), `make demo` (all 7 steps,
idempotency proof included), `make vault-verify` (+0 rows everywhere),
contract compat, governance render drift — was green before any change.
Changes are strictly tooling: dev-tool constraint bumps (ruff 0.15 /
mypy 2.3 / pytest 9.1, all gates re-verified green afterwards) and GitHub
Actions major-version bumps in `ci.yml`/`demo.yml`. Runtime dependencies,
pipelines, contracts, and governance artifacts are untouched; the
pyspark 4.1.1 + delta-spark 4.3.1 verified pair is preserved per the
repo's hard rules. Deferred items and reasons are recorded above.
