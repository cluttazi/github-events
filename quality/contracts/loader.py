"""Load and index contract YAML files from ``quality/contracts/definitions``."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import yaml

from quality.contracts.models import Contract

DEFINITIONS_DIR = Path(__file__).resolve().parent / "definitions"


def load_contract(path: Path) -> Contract:
    """Parse + validate a single contract file."""
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    return Contract.model_validate(raw)


@lru_cache(maxsize=1)
def load_contracts(definitions_dir: Path = DEFINITIONS_DIR) -> dict[str, Contract]:
    """Load the *latest version* of every contract, keyed by entity name.

    Files are named ``<entity>.v<version>.yaml``; multiple versions may
    coexist on disk (that's the point of versioned contracts) and the highest
    version wins for enforcement. Compatibility between versions is checked
    by ``quality.contracts.compat`` in CI, not here.
    """
    latest: dict[str, Contract] = {}
    for path in sorted(definitions_dir.glob("*.yaml")):
        contract = load_contract(path)
        current = latest.get(contract.contract)
        if current is None or contract.version > current.version:
            latest[contract.contract] = contract
    if not latest:
        raise FileNotFoundError(f"no contract definitions found under {definitions_dir}")
    return latest
