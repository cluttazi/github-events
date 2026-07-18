"""Unit tests for the single DV2.0 hashing definition (pure-Python twin)."""

from __future__ import annotations

import hashlib

import pytest

from pipelines.common.hashing import DELIMITER, NULL_TOKEN, hash_hex, normalize_value


def test_known_vector_single_component() -> None:
    assert hash_hex(["octocat"]) == hashlib.sha256(b"OCTOCAT").hexdigest()


def test_known_vector_with_null_component() -> None:
    assert hash_hex(["octocat", None]) == hashlib.sha256(b"OCTOCAT||^^").hexdigest()


def test_normalization_uppercase_and_trim() -> None:
    assert hash_hex(["  Octocat  "]) == hash_hex(["OCTOCAT"])
    assert hash_hex(["octo/repo", "42"]) == hash_hex([" octo/repo ", " 42 "])


def test_null_differs_from_empty_string() -> None:
    assert normalize_value(None) == NULL_TOKEN
    assert normalize_value("") == ""
    assert hash_hex([None]) != hash_hex([""])


def test_component_order_matters() -> None:
    assert hash_hex(["a", "b"]) != hash_hex(["b", "a"])


def test_delimiter_prevents_component_smearing() -> None:
    # ("ab", "c") must not collide with ("a", "bc")
    assert hash_hex(["ab", "c"]) != hash_hex(["a", "bc"])
    assert DELIMITER == "||"


def test_output_is_64_char_lowercase_hex() -> None:
    digest = hash_hex(["anything"])
    assert len(digest) == 64
    assert digest == digest.lower()
    assert all(c in "0123456789abcdef" for c in digest)


def test_empty_components_rejected() -> None:
    with pytest.raises(ValueError, match="at least one"):
        hash_hex([])
