"""Pydantic models for data contracts.

The YAML on disk is the reviewable artifact; these models are the executable
guarantee that a contract is well-formed before any pipeline trusts it.
Validation failures raise at load time with precise field paths — a broken
contract should never make it to a Spark job.
"""

from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator

# Logical types supported by the contract language. decimal takes (precision, scale).
_TYPE_PATTERN = re.compile(
    r"^(string|integer|bigint|double|boolean|date|timestamp|decimal\(\d{1,2},\s*\d{1,2}\))$"
)

PiiCategory = Literal["direct_identifier", "quasi_identifier", "sensitive", "financial"]


class FieldSpec(BaseModel):
    name: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    type: str
    nullable: bool = True
    description: str = ""
    allowed_values: list[str] | None = None
    pii: bool = False
    pii_category: PiiCategory | None = None

    @field_validator("type")
    @classmethod
    def _valid_type(cls, value: str) -> str:
        if not _TYPE_PATTERN.match(value):
            raise ValueError(f"unsupported contract type {value!r}")
        return value

    @model_validator(mode="after")
    def _pii_category_requires_pii(self) -> FieldSpec:
        if self.pii_category is not None and not self.pii:
            raise ValueError(f"field {self.name!r} has pii_category but pii=false")
        if self.pii and self.pii_category is None:
            raise ValueError(f"field {self.name!r} is pii but lacks pii_category")
        return self


class SlaSpec(BaseModel):
    freshness_hours: int = Field(gt=0)


class Contract(BaseModel):
    contract: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    version: int = Field(ge=1)
    owner: str
    description: str
    primary_key: list[str] = Field(min_length=1)
    event_time_field: str
    sla: SlaSpec
    fields: list[FieldSpec] = Field(min_length=1)

    @model_validator(mode="after")
    def _referenced_fields_exist(self) -> Contract:
        names = {f.name for f in self.fields}
        if len(names) != len(self.fields):
            raise ValueError(f"contract {self.contract!r} has duplicate field names")
        missing_pk = [k for k in self.primary_key if k not in names]
        if missing_pk:
            raise ValueError(f"primary_key fields not defined: {missing_pk}")
        if self.event_time_field not in names:
            raise ValueError(f"event_time_field {self.event_time_field!r} not defined")
        for key in self.primary_key:
            spec = next(f for f in self.fields if f.name == key)
            if spec.nullable:
                raise ValueError(f"primary_key field {key!r} must be nullable: false")
        return self

    @property
    def pii_fields(self) -> list[FieldSpec]:
        return [f for f in self.fields if f.pii]

    def field_spec(self, name: str) -> FieldSpec:
        for spec in self.fields:
            if spec.name == name:
                return spec
        raise KeyError(f"contract {self.contract!r} has no field {name!r}")
