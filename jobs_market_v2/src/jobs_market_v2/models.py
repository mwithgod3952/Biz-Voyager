"""Pydantic models used across the project."""

from __future__ import annotations

from pydantic import BaseModel, Field, field_validator

from .constants import COMPANY_TIERS, SUPPORTED_SOURCE_TYPES
from .utils import parse_aliases, strip_protocol


class CompanyInput(BaseModel):
    company_name: str
    company_tier: str
    official_domain: str | None = None
    company_name_en: str | None = None
    region: str | None = None
    aliases: list[str] = Field(default_factory=list)
    discovery_method: str = "manual"
    candidate_seed_type: str | None = None
    candidate_seed_url: str | None = None
    candidate_seed_title: str | None = None
    candidate_seed_reason: str | None = None

    @field_validator("company_name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("company_name은 비어 있을 수 없습니다.")
        return value

    @field_validator("company_tier")
    @classmethod
    def validate_tier(cls, value: str) -> str:
        if value not in COMPANY_TIERS:
            raise ValueError(f"허용되지 않은 company_tier입니다: {value}")
        return value

    @field_validator("official_domain")
    @classmethod
    def normalize_domain(cls, value: str | None) -> str | None:
        return strip_protocol(value) or None

    @field_validator("candidate_seed_url")
    @classmethod
    def normalize_candidate_seed_url(cls, value: str | None) -> str | None:
        return value.strip() if isinstance(value, str) and value.strip() else None

    @field_validator("aliases", mode="before")
    @classmethod
    def normalize_aliases(cls, value: object) -> list[str]:
        return parse_aliases(value)


class SourceInput(BaseModel):
    company_name: str
    company_tier: str
    source_name: str
    source_url: str
    source_type: str
    official_domain: str | None = None
    is_official_hint: bool = False
    structure_hint: str | None = None
    discovery_method: str = "manual"

    @field_validator("source_type")
    @classmethod
    def validate_source_type(cls, value: str) -> str:
        if value not in SUPPORTED_SOURCE_TYPES:
            raise ValueError(f"허용되지 않은 source_type입니다: {value}")
        return value

    @field_validator("official_domain")
    @classmethod
    def normalize_domain(cls, value: str | None) -> str | None:
        return strip_protocol(value) or None


class GateResult(BaseModel):
    passed: bool
    reasons: list[str]
    metrics: dict[str, object]


class DoctorSummary(BaseModel):
    passed: bool
    checks: list[dict[str, object]]
