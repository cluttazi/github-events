"""Contract version compatibility rules.

A new contract version must not break existing consumers. Between adjacent
versions of the same contract we enforce:

* primary key and event-time field are immutable,
* no field may be removed or change type,
* nullability may only loosen (required -> optional), never tighten,
* new fields must be nullable (v-1 producers stay conformant),
* an enum may gain values but not lose them (existing data stays valid).

Run as a module (CI does) to verify every contract lineage on disk::

    uv run python -m quality.contracts.compat
"""

from __future__ import annotations

import sys
from collections import defaultdict
from itertools import pairwise
from pathlib import Path

from quality.contracts.loader import DEFINITIONS_DIR, load_contract
from quality.contracts.models import Contract


def check_compatibility(old: Contract, new: Contract) -> list[str]:
    """Return the list of breaking changes between two contract versions."""
    breaks: list[str] = []
    if new.primary_key != old.primary_key:
        breaks.append(f"primary_key changed: {old.primary_key} -> {new.primary_key}")
    if new.event_time_field != old.event_time_field:
        breaks.append(f"event_time_field changed: {old.event_time_field} -> {new.event_time_field}")

    new_fields = {f.name: f for f in new.fields}
    for old_field in old.fields:
        new_field = new_fields.get(old_field.name)
        if new_field is None:
            breaks.append(f"field removed: {old_field.name}")
            continue
        if new_field.type != old_field.type:
            breaks.append(
                f"field {old_field.name} type changed: {old_field.type} -> {new_field.type}"
            )
        if old_field.nullable and not new_field.nullable:
            breaks.append(f"field {old_field.name} tightened from nullable to required")
        if old_field.allowed_values:
            removed = set(old_field.allowed_values) - set(new_field.allowed_values or [])
            if removed:
                breaks.append(f"field {old_field.name} lost enum values: {sorted(removed)}")

    old_names = {f.name for f in old.fields}
    for name, field in new_fields.items():
        if name not in old_names and not field.nullable:
            breaks.append(f"new field {name} must be nullable for compatibility")
    return breaks


def check_all(definitions_dir: Path = DEFINITIONS_DIR) -> dict[str, list[str]]:
    """Check every adjacent version pair; returns {'entity v1->v2': [breaks]}."""
    lineages: dict[str, list[Contract]] = defaultdict(list)
    for path in sorted(definitions_dir.glob("*.yaml")):
        contract = load_contract(path)
        lineages[contract.contract].append(contract)

    findings: dict[str, list[str]] = {}
    for name, versions in sorted(lineages.items()):
        ordered = sorted(versions, key=lambda c: c.version)
        for old, new in pairwise(ordered):
            key = f"{name} v{old.version}->v{new.version}"
            findings[key] = check_compatibility(old, new)
    return findings


def main() -> int:
    findings = check_all()
    failed = False
    for pair, breaks in findings.items():
        status = "BREAKING" if breaks else "compatible"
        print(f"{pair}: {status}")
        for issue in breaks:
            print(f"  - {issue}")
        failed = failed or bool(breaks)
    if not findings:
        print("no multi-version contract lineages found")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
