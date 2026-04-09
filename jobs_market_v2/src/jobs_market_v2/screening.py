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


def _matches_official_domain(source_domain: str, official_domain: str) -> bool:
    source = strip_protocol(source_domain)
    official = strip_protocol(official_domain)
    if not source or not official:
        return False
    return source == official or source.endswith(f".{official}")


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
    structure_hint = row.get("structure_hint") or infer_structure_hint(row.get("source_url", ""), source_type)
    domain_confidence = float(row.get("official_domain_confidence") or 0.0)
    is_structured_source = source_type in ATS_SOURCE_TYPES or structure_hint in {"json", "rss", "sitemap", "ats"}

    if _matches_official_domain(source_domain, official_domain):
        score += 0.5
    elif source_type in ATS_SOURCE_TYPES and official_domain:
        score += 0.35
    elif coerce_bool(row.get("is_official_hint")):
        score += 0.2

    if source_type in APPROVED_SOURCE_TYPES:
        score += 0.25
    elif source_type in SUPPORTED_SOURCE_TYPES:
        score += 0.1

    if is_structured_source:
        score += 0.15
    elif structure_hint == "html":
        score += 0.05

    score += min(domain_confidence, 1.0) * 0.1
    return round(min(score, 1.0), 2)


def _has_html_hiring_signal(row: dict) -> bool:
    parsed = urlparse(normalize_whitespace(row.get("source_url")))
    path_and_query = f"{parsed.path} {parsed.query}".lower()
    title = normalize_whitespace(row.get("source_title")).lower()
    return any(hint in path_and_query for hint in _HTML_HIRING_PATH_HINTS) or any(
        hint in title for hint in _HTML_HIRING_TEXT_HINTS
    )


def _reject_reason(row: dict) -> str | None:
    source_url = normalize_whitespace(str(row.get("source_url") or ""))
    source_type = normalize_whitespace(str(row.get("source_type") or ""))
    domain = extract_domain(source_url)
    path = urlparse(source_url).path.lower()
    if not source_url.startswith(("http://", "https://")):
        return "비공개 또는 비표준 URL"
    if domain in BLOCKED_SOURCE_DOMAINS:
        return "채용 포털 직접 수집 금지"
    if re.search(r"\.(png|jpg|jpeg|svg|webp)$", path):
        return "파싱 불가 에셋 URL"
    if source_type not in SUPPORTED_SOURCE_TYPES:
        return "지원하지 않는 source_type"
    return None


def screen_sources(source_candidates: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    bucket_columns = list(SOURCE_REGISTRY_COLUMNS) + ["reject_reason"]
    approved_rows: list[dict] = []
    candidate_rows: list[dict] = []
    rejected_rows: list[dict] = []

    for row in source_candidates.fillna("").to_dict(orient="records"):
        row["source_url"] = canonicalize_runtime_source_url(row.get("source_url") or "")
        row["source_domain"] = extract_domain(row.get("source_url") or "")
        row["official_domain"] = strip_protocol(row.get("official_domain"))
        row["structure_hint"] = row.get("structure_hint") or infer_structure_hint(row.get("source_url", ""), row.get("source_type", ""))
        row["official_domain_confidence"] = float(row.get("official_domain_confidence") or (0.99 if row.get("official_domain") else 0.0))
        row["source_quality_score"] = score_source_quality(row)
        row["failure_count"] = int(row.get("failure_count") or 0)
        row["last_success_at"] = row.get("last_success_at") or ""
        row["last_active_job_count"] = int(row.get("last_active_job_count") or 0)
        row["quarantine_reason"] = row.get("quarantine_reason") or ""
        row["is_quarantined"] = coerce_bool(row.get("is_quarantined"))
        row["verification_status"] = row.get("verification_status") or "미검증"

        reject_reason = _reject_reason(row)
        if reject_reason:
            rejected_rows.append({**row, "source_bucket": "rejected", "reject_reason": reject_reason})
            continue

        is_official_domain_match = _matches_official_domain(row["source_domain"], row["official_domain"])
        is_official_ats = row["source_type"] in ATS_SOURCE_TYPES and row["official_domain"] != ""
        html_approved_eligible = (
            row["source_type"] != "html_page"
            or row["last_active_job_count"] > 0
            or _has_html_hiring_signal(row)
        )

        if (
            (is_official_domain_match or is_official_ats or row["is_official_hint"])
            and row["source_quality_score"] >= 0.75
            and html_approved_eligible
        ):
            approved_rows.append({**row, "source_bucket": "approved"})
        elif row["source_quality_score"] >= 0.35:
            candidate_rows.append({**row, "source_bucket": "candidate"})
        else:
            rejected_rows.append({**row, "source_bucket": "rejected", "reject_reason": "공식성/구조 신뢰도 부족"})

    approved = pd.DataFrame(approved_rows, columns=bucket_columns)
    candidate = pd.DataFrame(candidate_rows, columns=bucket_columns)
    rejected = pd.DataFrame(rejected_rows, columns=bucket_columns)
    approved = _dedupe_screened_sources(approved)
    candidate = _dedupe_screened_sources(candidate)
    rejected = _dedupe_screened_sources(rejected)
    registry = pd.concat([approved, candidate, rejected], ignore_index=True) if any(
        not frame.empty for frame in (approved, candidate, rejected)
    ) else pd.DataFrame(columns=list(SOURCE_REGISTRY_COLUMNS))

    for column in SOURCE_REGISTRY_COLUMNS:
        if column not in registry.columns:
            registry[column] = None
    return approved, candidate, rejected, registry[list(SOURCE_REGISTRY_COLUMNS)]
