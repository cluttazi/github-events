"""Point-in-time (PIT) tables: one row per hub key per reporting day.

A PIT table pre-resolves "which satellite state was current as of day D" so
gold marts join hub x date x satellite in one equi-join instead of repeating
as-of window logic per query. Pointer columns hold the satellite's
``occurred_at`` of the state effective at end of day; missing history gets
the ghost timestamp ``1900-01-01`` (a deliberate non-null sentinel so mart
joins stay equi-joins).

The reporting timeline is **event time** (``occurred_at``): with batch loads
the wall-clock ``load_dts`` collapses to the run moment and cannot express
"as of last Tuesday" — recorded in DECISIONS.md.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

GHOST_DTS = "1900-01-01 00:00:00"


def date_spine(activity: DataFrame, date_col: str = "occurred_at") -> DataFrame:
    """Distinct reporting days observed in the event stream."""
    return activity.select(F.to_date(date_col).alias("as_of_date")).distinct()


def pit_from_satellites(
    hub: DataFrame,
    hash_key_col: str,
    satellites: dict[str, DataFrame],
    spine: DataFrame,
) -> DataFrame:
    """Build a PIT table: hub keys x spine days x one pointer per satellite.

    Each pointer is the max ``occurred_at`` of the satellite's states at or
    before the end of the as-of day, or the ghost timestamp when the
    satellite has no history yet for that key/day.
    """
    pit = hub.select(hash_key_col).distinct().crossJoin(spine)
    for sat_name, sat in satellites.items():
        pointer = f"{sat_name}_pit_ts"
        states = sat.select(
            F.col(hash_key_col),
            F.col("occurred_at").alias("_state_ts"),
            F.to_date("occurred_at").alias("_state_date"),
        )
        effective = (
            pit.join(states, hash_key_col, "left")
            .filter(F.col("_state_date").isNull() | (F.col("_state_date") <= F.col("as_of_date")))
            .groupBy(hash_key_col, "as_of_date")
            .agg(F.max("_state_ts").alias(pointer))
        )
        pit = pit.join(effective, [hash_key_col, "as_of_date"], "left").withColumn(
            pointer, F.coalesce(F.col(pointer), F.lit(GHOST_DTS).cast("timestamp"))
        )
    return pit
