"""Source eligibility screening logic."""

from __future__ import annotations

import re
from urllib.parse import urlparse

import pandas as pd

from .constants import APPROVED_SOURCE_TYPES, ATS_SOURCE_TYPES, BLOCKED_SOURCE_DOMAINS, SOURCE_REGISTRY_COLUMNS, SUPPORTED_SOURCE_TYPES
from .storage import coerce_bool
from .utils import canonicalize_runtime_source_url, extract_domain, normalize_whitespace, strip_protocol

_HTML_HIRING_PATH_HINTS = (
    "recruit",
    "rcrt",
    "career",
    "jobs",
    "job",
    "employment",
    "hire",
    "joinus",
    "join-us",
    "talent",
)
_HTML_HIRING_TEXT_HINTS = (
    "채용",
    "모집",
    "career",
    "jobs",
    "job board",
    "recruit",
)


def _dedupe_screened_sources(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "source_url" not in frame.columns:
        return frame
    ranked = frame.copy()
    if "last_active_job_count" in ranked.columns:
        ranked["last_active_job_count"] = pd.to_numeric(ranked["last_active_job_count"], errors="coerce").fillna(0)
    if "source_quality_score" in ranked.columns:
        ranked["source_quality_score"] = pd.to_numeric(ranked["source_quality_score"], errors="coerce").fillna(0)
    sort_columns = [column for column in ("last_active_job_count", "source_quality_score", "last_success_at") if column in ranked.columns]
    if sort_columns:
        ranked = ranked.sort_values(sort_columns, ascending=[False] * len(sort_columns), kind="stable")
    return ranked.drop_duplicates(subset=["source_url"], keep="first").reset_index(drop=True)


def infer_structure_hint(source_url: str, source_type: str) -> str:
    if source_type in {"json_api", "jsonld"} or source_url.endswith(".json"):
        return "json"
    if source_type == "rss" or source_url.endswith(".xml"):
        return "rss"
    if source_type == "sitemap":
        return "sitemap"
    if source_type in ATS_SOURCE_TYPES:
        return "ats"
    return "html"


def score_source_quality(row: dict) -> float:
    score = 0.0
    source_type = row.get("source_type") or ""
    source_domain = strip_protocol(row.get("source_domain"))
    official_domain = strip_protocol(row.get("official_domain"))
    structure_hint = row.get("structure_hint") or infer_structure_hint(str(row.get("source_url") or ""), source_type)
    source_url = normalize_whitespace(row.get("source_url"))
    is_official_hint = coerce_bool(row.get("is_official_hint"))

    if source_type in APPROVED_SOURCE_TYPES:
        score += 0.45
    elif source_type == "html_page":
        if source_url:
            parsed = urlparse(source_url)
            path = normalize_whitespace(parsed.path).lower()
            if any(token in path for token in _HTML_HIRING_PATH_HINTS):
                score += 0.22
            else:
                score -= 0.25
        else:
            score -= 0.25

    if official_domain and source_domain and source_domain.endswith(official_domain):
        score += 0.25
    if is_official_hint:
        score += 0.1
    if structure_hint in {"json", "rss", "sitemap", "ats"}:
        score += 0.1
    if source_domain in BLOCKED_SOURCE_DOMAINS:
        score -= 1.0
    if source_type == "html_page":
        source_name = normalize_whitespace(str(row.get("source_name") or "")).lower()
        candidate_text = f"{source_name} {source_url.lower()}"
        if any(token in candidate_text for token in _HTML_HIRING_TEXT_HINTS):
            score += 0.08
        if "about" in candidate_text and not any(token in candidate_text for token in _HTML_HIRING_PATH_HINTS):
            score -= 0.18
    return max(min(round(score, 3), 1.0), 0.0)


def _apply_screening_row(row: pd.Series) -> pd.Series:
    source_url = canonicalize_runtime_source_url(row.get("source_url"))
    source_type = normalize_whitespace(str(row.get("source_type") or ""))
    source_domain = extract_domain(source_url)
    official_domain = strip_protocol(row.get("official_domain"))
    structure_hint = normalize_whitespace(str(row.get("structure_hint") or infer_structure_hint(source_url, source_type)))
    is_official_hint = coerce_bool(row.get("is_official_hint"))
    quality_score = score_source_quality(
        {
            **row.to_dict(),
            "source_url": source_url,
            "source_domain": source_domain,
            "official_domain": official_domain,
            "structure_hint": structure_hint,
            "is_official_hint": is_official_hint,
        }
    )
    bucket = "approved" if quality_score >= 0.5 else "candidate"
    if source_domain in BLOCKED_SOURCE_DOMAINS:
        bucket = "rejected"
    return pd.Series(
        {
            **row.to_dict(),
            "source_url": source_url,
            "source_domain": source_domain,
            "official_domain": official_domain,
            "structure_hint": structure_hint,
            "is_official_hint": is_official_hint,
            "source_quality_score": quality_score,
            "source_bucket": bucket,
        }
    )


def screen_source_registry(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=list(SOURCE_REGISTRY_COLUMNS))
    screened = frame.apply(_apply_screening_row, axis=1)
    for column in SOURCE_REGISTRY_COLUMNS:
        if column not in screened.columns:
            screened[column] = None
    screened = screened[list(SOURCE_REGISTRY_COLUMNS)]
    return _dedupe_screened_sources(screened)
