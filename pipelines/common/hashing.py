"""The single Data Vault 2.0 hashing definition (ADR 004).

Every hub hash key, link hash key, and satellite hash diff in the lakehouse
is produced by this module — the hash rules exist exactly once:

* algorithm: SHA-256, stored as 64-char lowercase hex
* component normalization: cast to string, trim, uppercase
* delimiter between components: ``||``
* null token: ``^^`` (distinguishes NULL from empty string — ``""`` trims to
  ``""``, stays ``""``; NULL becomes ``^^``)

Hub hash keys hash the normalized business key components in their declared
order. Link hash keys hash the ordered concatenation of the parent hubs'
business keys (declaration order in ``config/lakehouse.yaml``), flattened
into one component sequence. Satellite hash diffs hash *all* descriptive
attributes in declared order — any attribute change flips the diff.

``hash_hex`` is the pure-Python twin used by unit tests and non-Spark code;
it MUST stay byte-identical to the Spark expressions
(``tests/unit/pipelines/test_hashing_spark.py`` enforces the parity).
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence

from pyspark.sql import Column
from pyspark.sql import functions as F

DELIMITER = "||"
NULL_TOKEN = "^^"


def _as_column(component: Column | str) -> Column:
    return F.col(component) if isinstance(component, str) else component


def normalize_component(component: Column | str) -> Column:
    """Uppercase-trimmed string form of one hash component; NULL -> ``^^``."""
    col = _as_column(component)
    return F.coalesce(F.upper(F.trim(col.cast("string"))), F.lit(NULL_TOKEN))


def hash_key(components: Sequence[Column | str]) -> Column:
    """SHA-256 hash key over business key components (hubs and links alike)."""
    if not components:
        raise ValueError("hash_key requires at least one component")
    normalized = [normalize_component(c) for c in components]
    return F.sha2(F.concat_ws(DELIMITER, *normalized), 256)


def hash_diff(columns: Sequence[Column | str]) -> Column:
    """SHA-256 diff over all descriptive attributes of a satellite row."""
    if not columns:
        raise ValueError("hash_diff requires at least one column")
    return hash_key(columns)


def normalize_value(value: str | None) -> str:
    """Pure-Python twin of :func:`normalize_component`."""
    if value is None:
        return NULL_TOKEN
    return value.strip().upper()


def hash_hex(values: Sequence[str | None]) -> str:
    """Pure-Python twin of :func:`hash_key` — must byte-match Spark output."""
    if not values:
        raise ValueError("hash_hex requires at least one value")
    joined = DELIMITER.join(normalize_value(v) for v in values)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()
