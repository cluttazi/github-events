"""Contract enforcement over staged frames: split valid vs quarantined.

Hard rules only — nullability and enum membership from the contract. A row
that violates any rule is quarantined with the list of reasons; it never
reaches the vault. Business rules (soft rules) live in the business vault.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from quality.contracts.models import Contract


@dataclass(frozen=True)
class EnforcementResult:
    valid: DataFrame
    quarantined: DataFrame
    checked_rules: int


def _violation_checks(contract: Contract) -> list[Column]:
    checks: list[Column] = []
    for spec in contract.fields:
        if not spec.nullable:
            checks.append(F.when(F.col(spec.name).isNull(), F.lit(f"null_violation:{spec.name}")))
        if spec.allowed_values:
            checks.append(
                F.when(
                    F.col(spec.name).isNotNull()
                    & ~F.col(spec.name).isin(list(spec.allowed_values)),
                    F.lit(f"enum_violation:{spec.name}"),
                )
            )
    return checks


def enforce_contract(staged: DataFrame, contract: Contract) -> EnforcementResult:
    """Apply the contract's hard rules; returns valid and quarantined frames."""
    checks = _violation_checks(contract)
    if not checks:
        return EnforcementResult(valid=staged, quarantined=staged.limit(0), checked_rules=0)

    flagged = staged.withColumn("violations", F.array_compact(F.array(*checks)))
    valid = flagged.filter(F.size("violations") == 0).drop("violations")
    quarantined = flagged.filter(F.size("violations") > 0)
    return EnforcementResult(valid=valid, quarantined=quarantined, checked_rules=len(checks))
