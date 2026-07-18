"""Suite definitions: YAML in, validated pydantic models out.

Each check type is a discriminated-union member, so an unknown type or a
missing parameter fails at load time with a precise error — the same
fail-at-the-edge philosophy as the data contracts.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Annotated, Literal

import yaml
from pydantic import BaseModel, Field, TypeAdapter

Severity = Literal["error", "warn"]

SUITES_DIR = Path(__file__).resolve().parent / "suites"


class BaseCheck(BaseModel):
    severity: Severity = "error"

    def describe(self) -> str:  # overridden per check
        return self.__class__.__name__


class NotNullCheck(BaseCheck):
    type: Literal["not_null"]
    column: str

    def describe(self) -> str:
        return f"not_null({self.column})"


class UniqueCheck(BaseCheck):
    type: Literal["unique"]
    columns: list[str] = Field(min_length=1)

    def describe(self) -> str:
        return f"unique({', '.join(self.columns)})"


class AcceptedValuesCheck(BaseCheck):
    type: Literal["accepted_values"]
    column: str
    values: list[str] = Field(min_length=1)

    def describe(self) -> str:
        return f"accepted_values({self.column})"


class RowCountBetweenCheck(BaseCheck):
    type: Literal["row_count_between"]
    min_rows: int = 0
    max_rows: int | None = None

    def describe(self) -> str:
        upper = self.max_rows if self.max_rows is not None else "inf"
        return f"row_count_between({self.min_rows}, {upper})"


class FreshnessCheck(BaseCheck):
    type: Literal["freshness"]
    column: str
    max_age_hours: float

    def describe(self) -> str:
        return f"freshness({self.column} <= {self.max_age_hours}h)"


class ReferentialIntegrityCheck(BaseCheck):
    type: Literal["referential_integrity"]
    column: str
    ref_table: str
    ref_column: str
    ref_filter: str | None = None

    def describe(self) -> str:
        return f"referential_integrity({self.column} -> {self.ref_table}.{self.ref_column})"


class RowCountMatchCheck(BaseCheck):
    """Row-count reconciliation against a reference table.

    The vault use case: every valid bronze event must become exactly one
    multi-active satellite row — ``sat_actor_repo_event`` count must equal
    ``bronze/github_events`` count (tolerance 0).
    """

    type: Literal["row_count_match"]
    ref_table: str
    ref_filter: str | None = None
    tolerance: int = 0

    def describe(self) -> str:
        return f"row_count_match(vs {self.ref_table}, tolerance={self.tolerance})"


Check = Annotated[
    NotNullCheck
    | UniqueCheck
    | AcceptedValuesCheck
    | RowCountBetweenCheck
    | FreshnessCheck
    | ReferentialIntegrityCheck
    | RowCountMatchCheck,
    Field(discriminator="type"),
]


class Suite(BaseModel):
    """DQ suite for one lakehouse table (path relative to the lakehouse root)."""

    table: str
    layer: Literal["bronze", "silver", "gold"]
    filter: str | None = None
    checks: list[Check] = Field(min_length=1)


@dataclass(frozen=True)
class CheckResult:
    description: str
    severity: Severity
    passed: bool
    observed: str

    @property
    def status(self) -> str:
        if self.passed:
            return "pass"
        return "fail" if self.severity == "error" else "warn"


@dataclass(frozen=True)
class SuiteResult:
    table: str
    layer: str
    row_count: int
    checks: list[CheckResult]
    executed_at: datetime

    @property
    def n_passed(self) -> int:
        return sum(1 for c in self.checks if c.passed)

    @property
    def n_failed(self) -> int:
        return sum(1 for c in self.checks if not c.passed and c.severity == "error")

    @property
    def n_warned(self) -> int:
        return sum(1 for c in self.checks if not c.passed and c.severity == "warn")

    @property
    def pass_rate(self) -> float:
        return self.n_passed / len(self.checks) if self.checks else 1.0


def load_suites(suites_dir: Path = SUITES_DIR) -> list[Suite]:
    """Load and validate every suite YAML, sorted for deterministic runs."""
    adapter = TypeAdapter(Suite)
    suites = [
        adapter.validate_python(yaml.safe_load(path.read_text(encoding="utf-8")))
        for path in sorted(suites_dir.glob("*.yaml"))
    ]
    if not suites:
        raise FileNotFoundError(f"no DQ suites found under {suites_dir}")
    return suites
