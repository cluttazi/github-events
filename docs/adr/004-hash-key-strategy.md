# ADR 004 — Hash key strategy

**Status**: accepted

## Decision

All DV2.0 hashing goes through `pipelines/common/hashing.py`:

* SHA-256, output as 64-char lowercase hex
* components cast to string, trimmed, uppercased; NULL -> `^^` token
* components joined with `||` before hashing
* hub keys hash the business key columns in declared order; link keys hash
  the ordered concatenation of parent-hub business keys; `hash_diff` hashes
  all descriptive attributes in declared (sorted) order
* a pure-Python twin (`hash_hex`) is proven byte-identical to the Spark
  expressions by test — non-Spark tools can compute vault keys

## Rationale

Uppercase-trim absorbs case/whitespace noise in GitHub logins and repo
names; the null token distinguishes NULL from empty string; the delimiter
prevents component smearing ("ab","c" vs "a","bc"); hex-over-binary keeps
tables human-debuggable and joins engine-agnostic. Parity is ASCII-scoped
for the synthetic domain (DECISIONS #21).
