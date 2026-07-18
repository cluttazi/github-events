"""Compile a contract into a Spark ``StructType``.

Raw-vault staging flattens the bronze ``raw_value`` JSON per event type and
``try_cast``s each staged column to its contract type: a value that cannot
be coerced becomes null, and the contract's nullability rules then decide
whether the row is a violation.
This is the ANSI-mode-friendly pattern — parsing is lenient at the edge,
enforcement is explicit, and nothing downstream needs global lenient casts.
"""

from __future__ import annotations

import re

from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from quality.contracts.models import Contract

_DECIMAL = re.compile(r"^decimal\((\d{1,2}),\s*(\d{1,2})\)$")

_SIMPLE_TYPES: dict[str, DataType] = {
    "string": StringType(),
    "integer": IntegerType(),
    "bigint": LongType(),
    "double": DoubleType(),
    "boolean": BooleanType(),
    "date": DateType(),
    "timestamp": TimestampType(),
}


def to_spark_type(contract_type: str) -> DataType:
    if match := _DECIMAL.match(contract_type):
        return DecimalType(int(match.group(1)), int(match.group(2)))
    try:
        return _SIMPLE_TYPES[contract_type]
    except KeyError as exc:  # models.py validates first; this guards drift
        raise ValueError(f"unsupported contract type {contract_type!r}") from exc


def to_struct_type(contract: Contract) -> StructType:
    """Payload schema for parsing; all fields nullable at parse time.

    Nullability is *enforcement* (violation → quarantine), not *parsing*:
    a non-nullable StructField would make Spark silently drop the whole row
    instead of letting us route it with a reason.
    """
    return StructType(
        [StructField(f.name, to_spark_type(f.type), nullable=True) for f in contract.fields]
    )
