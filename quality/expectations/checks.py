"""Check implementations: one small function per expectation type.

Each returns a :class:`CheckResult` with the *observed* value spelled out —
"3 orphaned values" beats "check failed" when the incident agent (or a human
at 3am) reads the output.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from quality.expectations.suite import (
    AcceptedValuesCheck,
    Check,
    CheckResult,
    FreshnessCheck,
    NotNullCheck,
    ReferentialIntegrityCheck,
    RowCountBetweenCheck,
    RowCountMatchCheck,
    UniqueCheck,
)

REFERENCE_TIME_ENV = "LAKEHOUSE_DQ_REFERENCE_TIME"


def reference_time() -> datetime:
    """Freshness anchor: wall clock, or a pinned ISO timestamp for
    deterministic runs over logical-clock demo data (backfill-validation
    pattern — the orchestrator pins this to the simulated business day)."""
    if pinned := os.environ.get(REFERENCE_TIME_ENV):
        return datetime.fromisoformat(pinned)
    return datetime.now(tz=UTC)


def _result(check: Check, passed: bool, observed: str) -> CheckResult:
    return CheckResult(
        description=check.describe(), severity=check.severity, passed=passed, observed=observed
    )


def _run_shape_check(df: DataFrame, check: Check) -> CheckResult | None:
    """Column/row-shape checks that need only the table itself."""
    if isinstance(check, NotNullCheck):
        nulls = df.filter(F.col(check.column).isNull()).count()
        return _result(check, nulls == 0, f"{nulls} null values")

    if isinstance(check, UniqueCheck):
        total = df.count()
        distinct = df.select(*check.columns).distinct().count()
        dupes = total - distinct
        return _result(check, dupes == 0, f"{dupes} duplicate keys")

    if isinstance(check, AcceptedValuesCheck):
        bad = df.filter(
            F.col(check.column).isNotNull() & ~F.col(check.column).isin(*check.values)
        ).count()
        return _result(check, bad == 0, f"{bad} out-of-domain values")

    if isinstance(check, RowCountBetweenCheck):
        count = df.count()
        ok = count >= check.min_rows and (check.max_rows is None or count <= check.max_rows)
        return _result(check, ok, f"{count} rows")

    return None


def run_check(
    spark: SparkSession, df: DataFrame, check: Check, table_resolver: dict[str, DataFrame]
) -> CheckResult:
    """Dispatch one check against the (already filtered) table DataFrame."""
    if (result := _run_shape_check(df, check)) is not None:
        return result

    if isinstance(check, FreshnessCheck):
        max_ts = df.agg(F.max(check.column)).first()
        newest = max_ts[0] if max_ts else None
        if newest is None:
            return _result(check, False, "no rows / all-null event time")
        if newest.tzinfo is None:  # Spark returns naive UTC timestamps
            newest = newest.replace(tzinfo=UTC)
        age_hours = (reference_time() - newest).total_seconds() / 3600
        return _result(
            check, age_hours <= check.max_age_hours, f"newest event {age_hours:.1f}h old"
        )

    if isinstance(check, ReferentialIntegrityCheck):
        ref = table_resolver[check.ref_table]
        if check.ref_filter:
            ref = ref.filter(check.ref_filter)
        orphans = (
            df.filter(F.col(check.column).isNotNull())
            .join(
                ref.select(F.col(check.ref_column).alias("__ref_key")).distinct(),
                F.col(check.column) == F.col("__ref_key"),
                "left_anti",
            )
            .count()
        )
        return _result(check, orphans == 0, f"{orphans} orphaned values")

    if isinstance(check, RowCountMatchCheck):
        ref = table_resolver[check.ref_table]
        if check.ref_filter:
            ref = ref.filter(check.ref_filter)
        count, ref_count = df.count(), ref.count()
        drift = abs(count - ref_count)
        return _result(
            check,
            drift <= check.tolerance,
            f"{count} rows vs {ref_count} reference rows (drift {drift})",
        )

    raise TypeError(f"unhandled check type: {type(check).__name__}")
