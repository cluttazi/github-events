"""Contract compatibility gate tests."""

from __future__ import annotations

from quality.contracts.compat import check_all, check_compatibility
from quality.contracts.models import Contract, FieldSpec, SlaSpec


def _contract(version: int, fields: list[FieldSpec]) -> Contract:
    return Contract(
        contract="sample",
        version=version,
        owner="x@example",
        description="d",
        primary_key=["event_id"],
        event_time_field="created_at",
        sla=SlaSpec(freshness_hours=24),
        fields=fields,
    )


def _v1_fields() -> list[FieldSpec]:
    return [
        FieldSpec(name="event_id", type="string", nullable=False),
        FieldSpec(name="created_at", type="timestamp", nullable=False),
        FieldSpec(name="action", type="string", allowed_values=["opened", "closed"]),
    ]


def test_additive_nullable_field_is_compatible() -> None:
    v2 = _contract(2, [*_v1_fields(), FieldSpec(name="labels", type="string", nullable=True)])
    assert check_compatibility(_contract(1, _v1_fields()), v2) == []


def test_type_change_is_breaking() -> None:
    changed = [
        FieldSpec(name="event_id", type="bigint", nullable=False),
        FieldSpec(name="created_at", type="timestamp", nullable=False),
        FieldSpec(name="action", type="string", allowed_values=["opened", "closed"]),
    ]
    breaks = check_compatibility(_contract(1, _v1_fields()), _contract(2, changed))
    assert any("type changed" in b for b in breaks)


def test_removed_field_and_lost_enum_value_are_breaking() -> None:
    shrunk = [
        FieldSpec(name="event_id", type="string", nullable=False),
        FieldSpec(name="created_at", type="timestamp", nullable=False),
        FieldSpec(name="action", type="string", allowed_values=["opened"]),
    ]
    breaks = check_compatibility(_contract(1, _v1_fields()), _contract(2, shrunk))
    assert any("lost enum values" in b for b in breaks)

    removed = [
        FieldSpec(name="event_id", type="string", nullable=False),
        FieldSpec(name="created_at", type="timestamp", nullable=False),
    ]
    breaks = check_compatibility(_contract(1, _v1_fields()), _contract(2, removed))
    assert any("field removed: action" in b for b in breaks)


def test_new_required_field_is_breaking() -> None:
    v2 = _contract(2, [*_v1_fields(), FieldSpec(name="mandatory", type="string", nullable=False)])
    breaks = check_compatibility(_contract(1, _v1_fields()), v2)
    assert any("must be nullable" in b for b in breaks)


def test_repo_lineages_are_compatible() -> None:
    for pair, breaks in check_all().items():
        assert breaks == [], f"{pair} has breaking changes: {breaks}"
