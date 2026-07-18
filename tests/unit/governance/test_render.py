"""Governance rendering: derived artifacts and the placement drift gate."""

from __future__ import annotations

import pytest

from governance.unity_catalog.render import (
    PII_PLACEMENTS,
    render_access_matrix,
    render_pii_tag_sql,
)
from quality.contracts.loader import load_contracts


def test_every_contract_pii_field_has_a_placement() -> None:
    for contract in load_contracts().values():
        for field in contract.pii_fields:
            assert field.name in PII_PLACEMENTS, (
                f"PII field {field.name!r} has no physical placement"
            )


def test_pii_tag_sql_covers_hub_actor_and_marts() -> None:
    sql = render_pii_tag_sql()
    assert "silver.raw_vault.hub_actor ALTER COLUMN actor_login" in sql
    assert "gold.marts.developer_360_mart ALTER COLUMN actor_login" in sql
    assert "gold.marts.collaboration_mart ALTER COLUMN active_assignee" in sql
    assert sql.count("SET TAGS") >= 5


def test_missing_placement_fails_rendering(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delitem(PII_PLACEMENTS, "actor_login")
    with pytest.raises(SystemExit, match="actor_login"):
        render_pii_tag_sql()


def test_access_matrix_reflects_grants() -> None:
    matrix = render_access_matrix()
    assert "| analysts | group |" in matrix
    assert "svc-vault" in matrix
    # analysts see gold only — exactly one SELECT cell in their row
    analyst_row = next(line for line in matrix.splitlines() if line.startswith("| analysts"))
    assert analyst_row.count("SELECT") == 1
