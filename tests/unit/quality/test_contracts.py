"""Contract model, loader, and Spark-schema compiler tests."""

from __future__ import annotations

import pytest
from pydantic import ValidationError
from pyspark.sql.types import StringType, TimestampType

from quality.contracts.loader import load_contracts
from quality.contracts.models import Contract, FieldSpec, SlaSpec
from quality.contracts.spark_schema import to_spark_type, to_struct_type

EXPECTED_CONTRACTS = {
    "push_event",
    "pull_request_event",
    "issues_event",
    "watch_event",
    "fork_event",
    "release_event",
}


def test_all_event_contracts_load() -> None:
    contracts = load_contracts()
    assert set(contracts) == EXPECTED_CONTRACTS


def test_every_contract_keys_on_event_id_and_flags_actor_pii() -> None:
    for contract in load_contracts().values():
        assert contract.primary_key == ["event_id"]
        assert contract.event_time_field == "created_at"
        actor = contract.field_spec("actor_login")
        assert actor.pii and actor.pii_category == "quasi_identifier"
        assert not contract.field_spec("repo_name").nullable


def test_struct_type_compiles_for_all_contracts() -> None:
    for contract in load_contracts().values():
        struct = to_struct_type(contract)
        assert [f.name for f in struct.fields] == [f.name for f in contract.fields]
        assert all(f.nullable for f in struct.fields)  # nullability is enforcement, not parsing


def test_spark_type_mapping() -> None:
    assert to_spark_type("string") == StringType()
    assert to_spark_type("timestamp") == TimestampType()
    with pytest.raises(ValueError, match="unsupported"):
        to_spark_type("blob")


def _contract(fields: list[FieldSpec]) -> Contract:
    return Contract(
        contract="sample",
        version=1,
        owner="x@example",
        description="d",
        primary_key=["event_id"],
        event_time_field="created_at",
        sla=SlaSpec(freshness_hours=24),
        fields=fields,
    )


def _base_fields() -> list[FieldSpec]:
    return [
        FieldSpec(name="event_id", type="string", nullable=False),
        FieldSpec(name="created_at", type="timestamp", nullable=False),
    ]


def test_pii_requires_category() -> None:
    with pytest.raises(ValidationError, match="pii_category"):
        FieldSpec(name="actor_login", type="string", pii=True)


def test_nullable_primary_key_rejected() -> None:
    fields = [
        FieldSpec(name="event_id", type="string", nullable=True),
        FieldSpec(name="created_at", type="timestamp", nullable=False),
    ]
    with pytest.raises(ValidationError, match="must be nullable: false"):
        _contract(fields)


def test_unknown_event_time_field_rejected() -> None:
    fields = [FieldSpec(name="event_id", type="string", nullable=False)]
    with pytest.raises(ValidationError, match="event_time_field"):
        _contract(fields)


def test_duplicate_field_names_rejected() -> None:
    fields = [*_base_fields(), FieldSpec(name="event_id", type="string", nullable=False)]
    with pytest.raises(ValidationError, match="duplicate"):
        _contract(fields)
