"""Source-backed company seed record collection."""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from io import BytesIO, StringIO
from pathlib import Path
from time import monotonic
from typing import Any
from urllib.parse import parse_qs, quote_plus, unquote, urljoin, urlparse

import httpx
import pandas as pd
from bs4 import BeautifulSoup

from .constants import COMPANY_SEED_SOURCE_COLUMNS, IMPORT_COMPANY_COLUMNS
from .network import build_timeout
from .storage import atomic_write_text, read_csv_or_empty, write_csv
from .utils import load_yaml, normalize_whitespace, parse_aliases, strip_protocol

CATALOG_SOURCE_TYPES = {"html_link_catalog", "html_link_catalog_file", "html_link_catalog_url"}
TRUSTED_EXTERNAL_CATALOG_SUFFIXES = (".go.kr", ".or.kr", ".re.kr")
CAREER_DOMAIN_KEYWORDS = ("career", "careers", "recruit", "jobs", "job", "talent", "apply")
PUBLIC_SERVICE_HOST_BLOCKLIST = {
    "open.go.kr",
    "epeople.go.kr",
    "epost.go.kr",
    "weather.go.kr",
    "history.go.kr",
    "129.go.kr",
    "egov.go.kr",
}
INVALID_COMPANY_SEED_SOURCE_COLUMNS = [*COMPANY_SEED_SOURCE_COLUMNS, "invalid_reason", "invalidated_at"]
CATALOG_HOST_KEYWORDS = (
    "진흥원",
    "테크노파크",
    "혁신센터",
    "경제자유구역청",
    "연구개발특구",
    "산업진흥",
    "창업진흥",
    "기술정보진흥",
    "무역투자진흥",
    "정보문화산업진흥",
    "콘텐츠진흥",
    "재단",
    "공사",
    "협회",
    "평가원",
    "관리원",
    "공단",
    "지원단",
)
CATALOG_HOST_EVIDENCE_TYPES = {"후보시드근거", "공식도메인", "공식채용소스"}
SEARCH_CATALOG_SITE_FILTERS = ("site:go.kr", "site:or.kr", "site:re.kr", "site:kr")
SEARCH_CATALOG_CORE_TERMS = (
    "참여기업 공급기업 수행기관",
    "참여기업 선정기업 기업목록",
    "공급기업 pool company list",
    "입주기업 회원사 기관목록",
)
SEARCH_CATALOG_ORG_TERMS = (
    "진흥원",
    "재단",
    "테크노파크",
    "혁신센터",
    "경제자유구역청",
    "지원단",
    "연구개발특구",
)


def _search_progress_path(paths) -> Path:
    return paths.runtime_dir / "company_seed_search_progress.json"


def _catalog_skip_cache_path(paths) -> Path:
    return paths.runtime_dir / "company_seed_catalog_skip_cache.json"


def _catalog_refresh_cache_path(paths) -> Path:
    return paths.runtime_dir / "company_seed_catalog_refresh_cache.json"


def _catalog_progress_path(paths) -> Path:
    return paths.runtime_dir / "company_seed_catalog_progress.json"


def _load_configured_company_seed_sources(paths) -> pd.DataFrame:
    raw = load_yaml(paths.company_seed_sources_path)
    sources = raw.get("sources") if isinstance(raw, dict) else []
    frame = pd.DataFrame(sources or [])
    for column in COMPANY_SEED_SOURCE_COLUMNS:
        if column not in frame.columns:
            frame[column] = ""
    return frame[list(COMPANY_SEED_SOURCE_COLUMNS)]


def load_company_seed_sources(paths) -> pd.DataFrame:
    configured = _load_configured_company_seed_sources(paths)
    discovered = read_csv_or_empty(paths.discovered_company_seed_sources_path, COMPANY_SEED_SOURCE_COLUMNS)
    frames = [frame for frame in (configured, discovered) if not frame.empty]
    frame = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    for column in COMPANY_SEED_SOURCE_COLUMNS:
        if column not in frame.columns:
            frame[column] = ""
    frame = frame[list(COMPANY_SEED_SOURCE_COLUMNS)]
    if frame.empty:
        return frame
    dedupe_key = frame.apply(_seed_source_dedupe_key, axis=1)
    return frame.loc[~dedupe_key.duplicated(keep="last")].reset_index(drop=True)


def load_shadow_company_seed_sources(paths) -> pd.DataFrame:
    frame = read_csv_or_empty(paths.shadow_company_seed_sources_path, COMPANY_SEED_SOURCE_COLUMNS)
    for column in COMPANY_SEED_SOURCE_COLUMNS:
        if column not in frame.columns:
            frame[column] = ""
    return frame[list(COMPANY_SEED_SOURCE_COLUMNS)]


def load_invalid_company_seed_sources(paths) -> pd.DataFrame:
    frame = read_csv_or_empty(paths.invalid_company_seed_sources_path, INVALID_COMPANY_SEED_SOURCE_COLUMNS)
    for column in INVALID_COMPANY_SEED_SOURCE_COLUMNS:
        if column not in frame.columns:
            frame[column] = ""
    return frame[list(INVALID_COMPANY_SEED_SOURCE_COLUMNS)]


def _resolve_local_path(paths, value: str) -> Path:
    candidate = Path(value).expanduser()
    if candidate.is_absolute():
        return candidate
    rooted = paths.root / value
    if rooted.exists():
        return rooted
    return paths.config_dir / value


def _relative_local_path(paths, value: Path) -> str:
    try:
        return str(value.relative_to(paths.root))
    except ValueError:
        return str(value)


def _seed_source_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_seed_record_value(value: Any) -> str:
    if isinstance(value, list):
        return "; ".join(normalize_whitespace(str(item)) for item in value if normalize_whitespace(str(item)))
    return normalize_whitespace("" if value is None else str(value))


def _company_seed_record_priority(row: dict[str, Any]) -> tuple[int, int, int, int, int]:
    official_domain = _normalize_host(row.get("official_domain"))
    host = official_domain.lower()
    domain_score = 0
    if official_domain:
        domain_score += 2
        if any(keyword in host for keyword in CAREER_DOMAIN_KEYWORDS):
            domain_score += 6
        if host.startswith(("career.", "careers.", "recruit.", "jobs.", "job.", "talent.", "apply.")):
            domain_score += 3
        if host.endswith(".greetinghr.com"):
            domain_score += 4
        if host.endswith("lever.co") or "lever.co" in host:
            domain_score += 3
        if host.endswith("greenhouse.io") or "greenhouse.io" in host:
            domain_score += 3
    provenance_score = int(bool(_normalize_seed_record_value(row.get("candidate_seed_url"))))
    provenance_score += int(bool(_normalize_seed_record_value(row.get("candidate_seed_reason"))))
    metadata_score = sum(
        int(bool(_normalize_seed_record_value(row.get(column))))
        for column in ("company_name_en", "region", "aliases")
    )
    discovery_score = int(bool(_normalize_seed_record_value(row.get("discovery_method"))))
    return domain_score, provenance_score, metadata_score, discovery_score, len(host)


def _collapse_company_seed_records(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy() if frame is not None else pd.DataFrame()
    for column in IMPORT_COMPANY_COLUMNS:
        if column not in normalized.columns:
            normalized[column] = ""
    if normalized.empty:
        return normalized[list(IMPORT_COMPANY_COLUMNS)]

    collapsed_rows: list[dict[str, Any]] = []
    for _, group in normalized.fillna("").groupby("company_name", sort=False):
        records = group.to_dict(orient="records")
        ordered = sorted(records, key=_company_seed_record_priority, reverse=True)
        merged = {column: "" for column in IMPORT_COMPANY_COLUMNS}
        best = ordered[0]
        merged.update(best)
        alias_values: list[str] = []
        for record in ordered:
            for column in IMPORT_COMPANY_COLUMNS:
                if _normalize_seed_record_value(merged.get(column)):
                    continue
                value = _normalize_seed_record_value(record.get(column))
                if value:
                    merged[column] = value
            alias_values.extend(parse_aliases(record.get("aliases")))
        if alias_values:
            merged["aliases"] = "; ".join(dict.fromkeys(alias_values))
        collapsed_rows.append({column: merged.get(column, "") for column in IMPORT_COMPANY_COLUMNS})
    return pd.DataFrame(collapsed_rows, columns=list(IMPORT_COMPANY_COLUMNS))


def _parse_iso_datetime(value: Any) -> datetime | None:
    normalized = normalize_whitespace("" if value is None else str(value))
    if not normalized:
        return None
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _ensure_seed_source_columns(
    frame: pd.DataFrame,
    columns: list[str] | tuple[str, ...] = COMPANY_SEED_SOURCE_COLUMNS,
) -> pd.DataFrame:
    normalized = frame.copy() if frame is not None else pd.DataFrame()
    for column in columns:
        if column not in normalized.columns:
            normalized[column] = ""
    return normalized[list(columns)]


def _stamp_seed_source_frame(frame: pd.DataFrame, *, now_iso: str | None = None) -> pd.DataFrame:
    normalized = _ensure_seed_source_columns(frame)
    if normalized.empty:
        return normalized
    current = now_iso or _seed_source_now_iso()
    first_seen = normalized["first_seen_at"].fillna("").astype(str).map(normalize_whitespace)
    last_seen = normalized["last_seen_at"].fillna("").astype(str).map(normalize_whitespace)
    normalized["first_seen_at"] = first_seen.mask(first_seen.eq(""), last_seen).mask(first_seen.eq("") & last_seen.eq(""), current)
    normalized["last_seen_at"] = last_seen.mask(last_seen.eq(""), normalized["first_seen_at"]).mask(last_seen.eq("") & normalized["first_seen_at"].eq(""), current)
    return normalized


def _prepare_discovered_seed_source_frame(
    discovered: pd.DataFrame,
    existing: pd.DataFrame,
    *,
    now_iso: str | None = None,
) -> pd.DataFrame:
    normalized = _ensure_seed_source_columns(discovered)
    if normalized.empty:
        return normalized
    current = now_iso or _seed_source_now_iso()
    existing_normalized = _stamp_seed_source_frame(existing, now_iso=current)
    existing_first_seen = {}
    if not existing_normalized.empty:
        keys = existing_normalized.apply(_seed_source_dedupe_key, axis=1)
        existing_first_seen = dict(zip(keys, existing_normalized["first_seen_at"].tolist(), strict=False))
    keys = normalized.apply(_seed_source_dedupe_key, axis=1)
    normalized["first_seen_at"] = [existing_first_seen.get(key, current) for key in keys]
    normalized["last_seen_at"] = current
    return normalized


def _prune_seed_source_frame(
    frame: pd.DataFrame,
    *,
    timestamp_column: str,
    retention_hours: int,
    max_rows: int = 0,
    now: datetime | None = None,
) -> tuple[pd.DataFrame, int]:
    normalized = frame.copy() if frame is not None else pd.DataFrame()
    if normalized.empty or retention_hours <= 0 or timestamp_column not in normalized.columns:
        if normalized.empty or max_rows <= 0 or len(normalized) <= max_rows:
            return normalized, 0
        sorted_frame = normalized.assign(
            __sort_key=normalized[timestamp_column].map(lambda value: _parse_iso_datetime(value) or datetime.min.replace(tzinfo=timezone.utc))
        ).sort_values("__sort_key", ascending=False, kind="stable")
        trimmed = sorted_frame.head(max_rows).drop(columns="__sort_key").reset_index(drop=True)
        return trimmed, int(len(normalized) - len(trimmed))
    current = now or datetime.now(timezone.utc)
    cutoff = current - timedelta(hours=retention_hours)
    keep_mask = []
    pruned = 0
    for value in normalized[timestamp_column].tolist():
        parsed = _parse_iso_datetime(value)
        is_stale = parsed is not None and parsed < cutoff
        keep_mask.append(not is_stale)
        if is_stale:
            pruned += 1
    pruned_frame = normalized.loc[keep_mask].reset_index(drop=True)
    if max_rows > 0 and len(pruned_frame) > max_rows:
        pruned_frame = pruned_frame.assign(
            __sort_key=pruned_frame[timestamp_column].map(
                lambda value: _parse_iso_datetime(value) or datetime.min.replace(tzinfo=timezone.utc)
            )
        ).sort_values("__sort_key", ascending=False, kind="stable")
        trimmed = pruned_frame.head(max_rows).drop(columns="__sort_key").reset_index(drop=True)
        pruned += int(len(pruned_frame) - len(trimmed))
        pruned_frame = trimmed
    return pruned_frame, pruned


def _prune_shadow_seed_sources(frame: pd.DataFrame, settings) -> tuple[pd.DataFrame, int]:
    retention_hours = int(getattr(settings, "company_seed_shadow_retention_hours", 168) or 168) if settings is not None else 168
    max_rows = int(getattr(settings, "company_seed_shadow_max_rows", 5000) or 5000) if settings is not None else 5000
    return _prune_seed_source_frame(
        _stamp_seed_source_frame(frame),
        timestamp_column="last_seen_at",
        retention_hours=retention_hours,
        max_rows=max_rows,
    )


def _prune_invalid_seed_sources(frame: pd.DataFrame, settings) -> tuple[pd.DataFrame, int]:
    normalized = frame.copy() if frame is not None else pd.DataFrame()
    for column in INVALID_COMPANY_SEED_SOURCE_COLUMNS:
        if column not in normalized.columns:
            normalized[column] = ""
    retention_hours = int(getattr(settings, "company_seed_invalid_retention_hours", 336) or 336) if settings is not None else 336
    max_rows = int(getattr(settings, "company_seed_invalid_max_rows", 5000) or 5000) if settings is not None else 5000
    return _prune_seed_source_frame(
        normalized[list(INVALID_COMPANY_SEED_SOURCE_COLUMNS)],
        timestamp_column="invalidated_at",
        retention_hours=retention_hours,
        max_rows=max_rows,
    )


def _allowed_domains(source: dict[str, Any], fallback_url: str) -> set[str]:
    configured = {
        strip_protocol(domain)
        for domain in parse_aliases(source.get("allowed_domains"))
        if strip_protocol(domain)
    }
    parsed = urlparse(fallback_url)
    if parsed.netloc:
        configured.add(strip_protocol(parsed.netloc))
    return {domain for domain in configured if domain}


def _normalize_https_url(domain: str) -> str:
    normalized = _normalize_host(domain)
    if not normalized:
        return ""
    return f"https://{normalized}"


def _is_trusted_external_catalog_domain(domain: str) -> bool:
    normalized = _normalize_host(domain).lower()
    if normalized in PUBLIC_SERVICE_HOST_BLOCKLIST:
        return False
    return any(normalized.endswith(suffix) for suffix in TRUSTED_EXTERNAL_CATALOG_SUFFIXES)


def _is_public_bare_kr_catalog_domain(domain: str, haystack: str) -> bool:
    normalized = _normalize_host(domain).lower()
    if not normalized.endswith(".kr"):
        return False
    if any(
        normalized.endswith(suffix)
        for suffix in (
            ".co.kr",
            ".go.kr",
            ".or.kr",
            ".re.kr",
            ".ac.kr",
            ".ne.kr",
            ".mil.kr",
        )
    ):
        return False
    public_host_keywords = (
        *CATALOG_HOST_KEYWORDS,
        "한국",
        "국가",
        "공공",
        "기관",
        "센터",
        "위원회",
        "재단법인",
        "진흥원",
        "연구원",
    )
    blocked_host_keywords = (
        "쇼핑",
        "blog",
        "블로그",
        "cafe",
        "카페",
        "mall",
        "shop",
        "store",
        "portal",
    )
    if any(keyword.lower() in haystack for keyword in blocked_host_keywords):
        return False
    return any(keyword.lower() in haystack for keyword in public_host_keywords)


def _is_search_trusted_catalog_domain(domain: str, haystack: str) -> bool:
    normalized = _normalize_host(domain)
    return _is_trusted_external_catalog_domain(normalized) or _is_public_bare_kr_catalog_domain(normalized, haystack)


def _normalize_host(domain: str) -> str:
    return strip_protocol(domain).lower().removeprefix("www.")


def _link_haystack(text: str, link_url: str) -> str:
    return normalize_whitespace(f"{text} {link_url}").lower()


def _matches_keywords(haystack: str, keywords: list[str]) -> bool:
    if not keywords:
        return True
    return any(keyword.lower() in haystack for keyword in keywords)


def _blocked_by_keywords(haystack: str, keywords: list[str]) -> bool:
    if not keywords:
        return False
    return any(keyword.lower() in haystack for keyword in keywords)


def _infer_discovered_source_type(*, source_url: str = "", local_path: str = "", label: str = "") -> str:
    value = normalize_whitespace(f"{local_path or source_url} {label}").lower()
    if ".csv" in value:
        return "csv_file" if local_path else "csv_url"
    if ".xls" in value or ".xlsx" in value:
        return "xlsx_file" if local_path else "xlsx_url"
    return "html_table_file" if local_path else "html_table_url"


def _infer_catalog_page_source_type(*, local_path: str = "") -> str:
    return "html_link_catalog_file" if local_path else "html_link_catalog_url"


def _normalize_seed_source_url_for_dedupe(value: str) -> str:
    if pd.isna(value):
        return ""
    normalized = normalize_whitespace(value)
    if not normalized:
        return ""
    parsed = urlparse(normalized)
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}{parsed.path}"
    return normalized


def _seed_source_dedupe_key(row: pd.Series | dict[str, Any]) -> str:
    def _value(key: str) -> Any:
        value = row.get(key) if isinstance(row, dict) else row.get(key, "")
        return "" if pd.isna(value) else value

    source_type = normalize_whitespace(_value("source_type"))
    local_path = normalize_whitespace(_value("local_path"))
    source_url = _normalize_seed_source_url_for_dedupe(_value("source_url"))
    request_url = _normalize_seed_source_url_for_dedupe(_value("request_url"))
    primary_location = local_path or source_url or request_url
    if primary_location:
        return "|".join((source_type, primary_location))
    source_name = normalize_whitespace(_value("source_name"))
    source_title = normalize_whitespace(_value("source_title"))
    return "|".join((source_name, source_title, source_type, local_path, source_url))


def _resolve_catalog_link(
    source: dict[str, Any],
    href: str,
    *,
    base_url: str,
    paths,
) -> tuple[str, str, bool]:
    source_type = normalize_whitespace(source.get("source_type"))
    if source_type == "html_link_catalog_file":
        resolved_path = (Path(urlparse(base_url).path).parent / href).resolve()
        return resolved_path.as_uri(), _relative_local_path(paths, resolved_path), True
    resolved_url = urljoin(base_url, href)
    return resolved_url, "", False


def _build_discovered_source_row(
    source: dict[str, Any],
    *,
    label: str,
    source_url: str,
    local_path: str = "",
) -> dict[str, Any]:
    base_source_name = _normalize_discovered_source_base_name(normalize_whitespace(source.get("source_name")))
    normalized_label = _normalize_discovered_source_label(label, source_url=source_url, local_path=local_path)
    discovered_type = normalize_whitespace(source.get("discovered_source_type")) or _infer_discovered_source_type(
        source_url=source_url,
        local_path=local_path,
        label=normalized_label,
    )
    request_url = source_url or local_path
    return {
        "source_name": f"{base_source_name} - {normalized_label[:60]}",
        "source_type": discovered_type,
        "discovered_source_type": normalize_whitespace(source.get("discovered_source_type")),
        "source_url": source_url,
        "request_url": request_url,
        "local_path": local_path,
        "source_title": normalized_label[:120],
        "company_tier": normalize_whitespace(source.get("company_tier")),
        "candidate_seed_type": normalize_whitespace(source.get("candidate_seed_type")),
        "candidate_seed_reason": normalize_whitespace(source.get("candidate_seed_reason")),
        "table_index": normalize_whitespace(source.get("table_index")),
        "header_row": normalize_whitespace(str(source.get("header_row") or "")),
        "company_name_column": normalize_whitespace(source.get("company_name_column")),
        "official_domain_column": normalize_whitespace(source.get("official_domain_column")),
        "company_name_en_column": normalize_whitespace(source.get("company_name_en_column")),
        "region_column": normalize_whitespace(source.get("region_column")),
        "aliases_column": normalize_whitespace(source.get("aliases_column")),
        "filter_text_columns": normalize_whitespace(source.get("filter_text_columns")),
        "include_keywords": normalize_whitespace(source.get("include_keywords")),
        "exclude_keywords": normalize_whitespace(source.get("exclude_keywords")),
        "allowed_domains": normalize_whitespace(source.get("allowed_domains")),
        "discovery_include_keywords": "",
        "discovery_exclude_keywords": "",
        "max_discovered_sources": "",
    }


def _normalize_discovered_source_base_name(name: str) -> str:
    normalized = normalize_whitespace(name)
    if not normalized:
        return "catalog-source"
    lower = normalized.lower()
    if "http://" in lower or "https://" in lower or normalized.count(" - ") >= 2 or len(normalized) > 120:
        return normalize_whitespace(normalized.split(" - ", 1)[0])
    return normalized


def _normalize_discovered_source_label(label: str, *, source_url: str = "", local_path: str = "") -> str:
    normalized = normalize_whitespace(label)
    if not normalized:
        return _fallback_discovered_source_label(source_url=source_url, local_path=local_path)
    lower = normalized.lower()
    segment_count = max(lower.count(" - "), lower.count(" | "), lower.count(" / "))
    if (
        "http://" in lower
        or "https://" in lower
        or segment_count >= 3
        or len(normalized) > 120
    ):
        return _fallback_discovered_source_label(source_url=source_url, local_path=local_path)
    return normalized


def _fallback_discovered_source_label(*, source_url: str = "", local_path: str = "") -> str:
    if local_path:
        name = Path(local_path).name
        if name:
            return normalize_whitespace(name)
    parsed = urlparse(normalize_whitespace(source_url))
    path_name = Path(parsed.path).name
    if path_name:
        return normalize_whitespace(path_name)
    host = strip_protocol(parsed.netloc)
    if host:
        return host
    return "catalog-source"


def _build_external_catalog_source_row(
    source: dict[str, Any],
    *,
    label: str,
    source_url: str,
    allowed_domain: str,
) -> dict[str, Any]:
    row = _build_discovered_source_row(source, label=label, source_url=source_url)
    row["source_type"] = "html_link_catalog_url"
    row["discovered_source_type"] = ""
    row["allowed_domains"] = strip_protocol(allowed_domain)
    row["candidate_seed_type"] = "공식카탈로그도메인자동발견"
    row["candidate_seed_reason"] = "기존 공식 host가 가리킨 외부 공식 도메인 카탈로그를 자동 발견"
    return row


def _build_external_catalog_host_row(
    source: dict[str, Any],
    *,
    label: str,
    allowed_domain: str,
) -> dict[str, Any]:
    host = _normalize_host(allowed_domain)
    row = _build_discovered_source_row(source, label=label, source_url=_normalize_https_url(host))
    row["source_type"] = "html_link_catalog_url"
    row["discovered_source_type"] = ""
    row["allowed_domains"] = host
    row["candidate_seed_type"] = "외부공식카탈로그호스트자동발견"
    row["candidate_seed_reason"] = "기존 공식 host가 가리킨 외부 trusted 공식 도메인의 루트 카탈로그 host를 자동 발견"
    return row


def _build_search_catalog_source_row(*, label: str, source_url: str, allowed_domain: str) -> dict[str, Any]:
    host = _normalize_host(allowed_domain)
    return {
        "source_name": f"{host} 웹검색 자동 카탈로그 탐색",
        "source_type": "html_link_catalog_url",
        "discovered_source_type": "",
        "source_url": normalize_whitespace(source_url),
        "request_url": "",
        "local_path": "",
        "source_title": normalize_whitespace(label) or host,
        "company_tier": "",
        "candidate_seed_type": "웹검색공식카탈로그자동발견",
        "candidate_seed_reason": "웹검색 결과에서 trusted 공식 도메인 카탈로그 URL을 자동 발견",
        "table_index": "",
        "header_row": "",
        "company_name_column": "",
        "official_domain_column": "",
        "company_name_en_column": "",
        "region_column": "",
        "aliases_column": "",
        "filter_text_columns": "",
        "include_keywords": "",
        "exclude_keywords": "",
        "allowed_domains": host,
        "discovery_include_keywords": "참여기업;공급기업;수행기관;지원기업;선정기업;기업 목록;기관목록;pool;participants;company-list",
        "discovery_exclude_keywords": "채용;입찰;공지;보도;교육;세미나;설명회;계획서;안내문;pdf;zip;login;logout;member;join;main.do;board.do;accessmsg",
        "max_discovered_sources": "5",
    }


def _build_search_catalog_host_row(*, label: str, allowed_domain: str, source_url: str = "") -> dict[str, Any]:
    host = _normalize_host(allowed_domain)
    return {
        "source_name": f"{host} 웹검색 자동 호스트 탐색",
        "source_type": "html_link_catalog_url",
        "discovered_source_type": "",
        "source_url": _normalize_https_url(host),
        "request_url": normalize_whitespace(source_url),
        "local_path": "",
        "source_title": normalize_whitespace(label) or host,
        "company_tier": "",
        "candidate_seed_type": "웹검색공식카탈로그호스트자동발견",
        "candidate_seed_reason": "웹검색 결과에서 trusted 공식 도메인 루트 host를 자동 발견",
        "table_index": "",
        "header_row": "",
        "company_name_column": "",
        "official_domain_column": "",
        "company_name_en_column": "",
        "region_column": "",
        "aliases_column": "",
        "filter_text_columns": "",
        "include_keywords": "",
        "exclude_keywords": "",
        "allowed_domains": host,
        "discovery_include_keywords": "참여기업;공급기업;수행기관;지원기업;선정기업;기업 목록;기관목록;pool;participants;company-list",
        "discovery_exclude_keywords": "채용;입찰;공지;보도;교육;세미나;설명회;계획서;안내문;pdf;zip;login;logout;member;join;main.do;board.do;accessmsg",
        "max_discovered_sources": "5",
    }


def _looks_like_catalog_candidate(haystack: str, *, source_url: str = "", local_path: str = "") -> bool:
    path_value = normalize_whitespace(local_path or urlparse(source_url).path).lower()
    if any(path_value.endswith(suffix) for suffix in (".csv", ".xls", ".xlsx", ".zip", ".pdf", ".hwp", ".doc", ".docx")):
        return False
    blocked_path_tokens = (
        "filedownload.do",
        "download.do",
        "login",
        "logout",
        "member",
        "join",
        "signup",
        "signin",
        "main.do",
        "view.do",
        "board.do",
        "accessmsg",
        "privacy",
        "policy",
        "faq",
        "notice",
    )
    if any(token in path_value for token in blocked_path_tokens):
        return False
    catalog_keywords = (
        "참여기업",
        "공급기업",
        "수행기관",
        "지원기업",
        "선정기업",
        "기업 목록",
        "기업목록",
        "기관목록",
        "풀",
        "pool",
        "participants",
        "partners",
        "company list",
        "supplier",
    )
    return any(keyword.lower() in haystack for keyword in catalog_keywords)


def _looks_like_external_catalog_host_candidate(haystack: str, *, source_url: str = "") -> bool:
    host = _normalize_host(urlparse(source_url).netloc)
    if not _is_trusted_external_catalog_domain(host):
        return False
    path_value = normalize_whitespace(urlparse(source_url).path).lower()
    if any(path_value.endswith(suffix) for suffix in (".csv", ".xls", ".xlsx", ".zip", ".pdf", ".hwp", ".doc", ".docx")):
        return False
    blocked_path_tokens = (
        "login",
        "logout",
        "member",
        "join",
        "signup",
        "signin",
        "privacy",
        "policy",
        "faq",
    )
    if any(token in path_value for token in blocked_path_tokens):
        return False
    host_keywords = (*CATALOG_HOST_KEYWORDS, "기관", "센터", "재단법인")
    return any(keyword.lower() in haystack for keyword in host_keywords)


def _looks_like_public_bare_kr_catalog_host_candidate(haystack: str, *, source_url: str = "") -> bool:
    host = _normalize_host(urlparse(source_url).netloc)
    if not _is_public_bare_kr_catalog_domain(host, haystack):
        return False
    return True


def _search_catalog_queries() -> list[str]:
    queries: list[str] = []
    for site_filter in SEARCH_CATALOG_SITE_FILTERS:
        for core_term in SEARCH_CATALOG_CORE_TERMS:
            queries.append(f"{site_filter} {core_term}")
            for org_term in SEARCH_CATALOG_ORG_TERMS:
                queries.append(f"{site_filter} {core_term} {org_term}")
    for core_term in SEARCH_CATALOG_CORE_TERMS:
        for org_term in SEARCH_CATALOG_ORG_TERMS:
            queries.append(f"{core_term} {org_term}")
    deduped: list[str] = []
    seen: set[str] = set()
    for query in queries:
        normalized = normalize_whitespace(query)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _search_catalog_query_urls() -> list[str]:
    return [f"https://html.duckduckgo.com/html/?q={quote_plus(query)}" for query in _search_catalog_queries()]


def _search_catalog_query_signature() -> str:
    return "|".join(_search_catalog_queries())


def _extract_search_result_url(href: str) -> str:
    normalized = normalize_whitespace(href)
    if not normalized:
        return ""
    parsed = urlparse(normalized)
    if parsed.netloc in {"duckduckgo.com", "html.duckduckgo.com"}:
        params = parse_qs(parsed.query)
        for key in ("uddg", "rut"):
            values = params.get(key)
            if values:
                return normalize_whitespace(unquote(values[0]))
        return ""
    if normalized.startswith("//"):
        return f"https:{normalized}"
    return normalized


def _should_run_catalog_search(paths, settings) -> bool:
    if settings is None:
        return False
    cooldown_hours = int(getattr(settings, "company_seed_search_cooldown_hours", 24) or 24)
    progress_path = _search_progress_path(paths)
    if not progress_path.exists():
        return True
    try:
        payload = json.loads(progress_path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return True
    last_search_at = normalize_whitespace(payload.get("last_search_at"))
    if normalize_whitespace(payload.get("query_signature")) != _search_catalog_query_signature():
        return True
    if int(payload.get("next_query_offset", 0) or 0) > 0:
        return True
    if not last_search_at:
        return True
    try:
        last_run = datetime.fromisoformat(last_search_at)
    except ValueError:
        return True
    if last_run.tzinfo is None:
        last_run = last_run.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - last_run >= timedelta(hours=cooldown_hours)


def _write_catalog_search_progress(paths, *, discovered_count: int, next_query_offset: int = 0, total_query_count: int = 0) -> None:
    progress_path = _search_progress_path(paths)
    atomic_write_text(
        progress_path,
        json.dumps(
            {
                "last_search_at": datetime.now(timezone.utc).isoformat(),
                "discovered_count": int(discovered_count),
                "next_query_offset": int(next_query_offset),
                "total_query_count": int(total_query_count),
                "query_signature": _search_catalog_query_signature(),
            },
            ensure_ascii=False,
        ),
    )


def _load_catalog_skip_cache(paths) -> dict[str, dict[str, Any]]:
    cache_path = _catalog_skip_cache_path(paths)
    if not cache_path.exists():
        return {}
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_catalog_skip_cache(paths, cache: dict[str, dict[str, Any]]) -> None:
    cache_path = _catalog_skip_cache_path(paths)
    atomic_write_text(cache_path, json.dumps(cache, ensure_ascii=False))


def _load_catalog_refresh_cache(paths) -> dict[str, dict[str, Any]]:
    cache_path = _catalog_refresh_cache_path(paths)
    if not cache_path.exists():
        return {}
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_catalog_refresh_cache(paths, cache: dict[str, dict[str, Any]]) -> None:
    cache_path = _catalog_refresh_cache_path(paths)
    atomic_write_text(cache_path, json.dumps(cache, ensure_ascii=False))


def _load_catalog_progress(paths) -> dict[str, Any]:
    progress_path = _catalog_progress_path(paths)
    if not progress_path.exists():
        return {"next_offset": 0}
    try:
        payload = json.loads(progress_path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return {"next_offset": 0}
    return payload if isinstance(payload, dict) else {"next_offset": 0}


def _write_catalog_progress(paths, *, next_offset: int, total_catalog_source_count: int) -> None:
    progress_path = _catalog_progress_path(paths)
    atomic_write_text(
        progress_path,
        json.dumps(
            {
                "next_offset": int(next_offset),
                "total_catalog_source_count": int(total_catalog_source_count),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
            ensure_ascii=False,
        ),
    )


def _timeout_like_reason(reason: str) -> bool:
    return any(token in reason for token in ("ConnectTimeout", "ReadTimeout", "RemoteProtocolError", "timed out"))


def _is_catalog_skip_cache_active(entry: dict[str, Any], settings) -> bool:
    cooldown_hours = 24
    if settings is not None:
        cooldown_hours = int(getattr(settings, "company_seed_catalog_skip_cooldown_hours", 24) or 24)
    skipped_at = normalize_whitespace((entry or {}).get("skipped_at"))
    if not skipped_at:
        return False
    try:
        last_run = datetime.fromisoformat(skipped_at)
    except ValueError:
        return False
    if last_run.tzinfo is None:
        last_run = last_run.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - last_run < timedelta(hours=cooldown_hours)


def _is_catalog_refresh_cache_active(entry: dict[str, Any], settings) -> bool:
    if settings is None:
        return False
    refresh_hours = int(getattr(settings, "company_seed_catalog_refresh_hours", 24) or 24)
    refreshed_at = normalize_whitespace((entry or {}).get("refreshed_at"))
    if not refreshed_at:
        return False
    try:
        last_run = datetime.fromisoformat(refreshed_at)
    except ValueError:
        return False
    if last_run.tzinfo is None:
        last_run = last_run.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - last_run < timedelta(hours=refresh_hours)


def _discover_catalog_seed_sources_from_search(
    paths,
    existing_sources: list[dict[str, Any]],
    settings,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not _should_run_catalog_search(paths, settings):
        return [], {
            "search_query_count": 0,
            "search_query_start_offset": 0,
            "search_query_next_offset": 0,
            "search_query_batch_count": 0,
            "search_query_batch_size": 0,
            "search_discovered_seed_source_count": 0,
        }
    queries = _search_catalog_queries()
    total_query_count = len(queries)
    progress_path = _search_progress_path(paths)
    next_query_offset = 0
    if progress_path.exists():
        try:
            payload = json.loads(progress_path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            payload = {}
        next_query_offset = int(payload.get("next_query_offset", 0) or 0)
        if normalize_whitespace(payload.get("query_signature")) != _search_catalog_query_signature():
            next_query_offset = 0
    if total_query_count and next_query_offset >= total_query_count:
        next_query_offset = 0
    batch_size = int(getattr(settings, "company_seed_search_query_batch_size", total_query_count) or total_query_count)
    if batch_size > 0 and total_query_count > batch_size:
        end_offset = min(next_query_offset + batch_size, total_query_count)
        selected_queries = queries[next_query_offset:end_offset]
        following_offset = 0 if end_offset >= total_query_count else end_offset
    else:
        selected_queries = queries
        next_query_offset = 0
        following_offset = 0
    existing_domains = {
        _normalize_host(urlparse(normalize_whitespace(source.get("source_url"))).netloc)
        for source in existing_sources
        if normalize_whitespace(source.get("source_type")) in CATALOG_SOURCE_TYPES
    }
    discovered: list[dict[str, Any]] = []
    seen: set[str] = set()
    for query in selected_queries:
        query_url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
        try:
            html = _fetch_text(query_url, settings)
        except Exception:  # noqa: BLE001
            continue
        soup = BeautifulSoup(html, "lxml")
        for anchor in soup.find_all("a", href=True):
            label = normalize_whitespace(anchor.get_text(" ", strip=True))
            result_url = _extract_search_result_url(anchor.get("href"))
            if not result_url:
                continue
            host = _normalize_host(urlparse(result_url).netloc)
            haystack = _link_haystack(label, result_url)
            if not _is_search_trusted_catalog_domain(host, haystack):
                continue
            if host in existing_domains:
                continue
            row: dict[str, Any] | None = None
            if _matches_keywords(haystack, ["참여기업", "공급기업", "수행기관", "participants", "company list"]) and _looks_like_catalog_candidate(
                haystack,
                source_url=result_url,
            ):
                dedupe_key = _normalize_seed_source_url_for_dedupe(result_url)
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                row = _build_search_catalog_source_row(
                    label=label,
                    source_url=result_url,
                    allowed_domain=host,
                )
            elif _looks_like_external_catalog_host_candidate(
                haystack,
                source_url=result_url,
            ) or _looks_like_public_bare_kr_catalog_host_candidate(
                haystack,
                source_url=result_url,
            ):
                dedupe_key = _normalize_seed_source_url_for_dedupe(_normalize_https_url(host))
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                row = _build_search_catalog_host_row(
                    label=label,
                    allowed_domain=host,
                    source_url=result_url,
                )
            if row is None:
                continue
            discovered.append(row)
            existing_domains.add(host)
    _write_catalog_search_progress(
        paths,
        discovered_count=len(discovered),
        next_query_offset=following_offset,
        total_query_count=total_query_count,
    )
    return discovered, {
        "search_query_count": int(total_query_count),
        "search_query_start_offset": int(next_query_offset),
        "search_query_next_offset": int(following_offset),
        "search_query_batch_count": int(len(selected_queries)),
        "search_query_batch_size": int(batch_size),
        "search_discovered_seed_source_count": int(len(discovered)),
    }


def _fetch_catalog_html(source: dict[str, Any], paths, settings, *, source_url: str, local_path: str) -> str:
    if local_path:
        return _resolve_local_path(paths, local_path).read_text(encoding="utf-8")
    return _fetch_text(source_url, settings)


def _html_page_contains_table(*, source_url: str, local_path: str, paths, settings) -> bool:
    try:
        html = _fetch_catalog_html({}, paths, settings, source_url=source_url, local_path=local_path)
        tables = pd.read_html(StringIO(html))
    except Exception:  # noqa: BLE001
        return False
    return any(not table.empty for table in tables)


def _discover_attachment_sources_from_detail_page(
    source: dict[str, Any],
    *,
    source_url: str,
    local_path: str,
    label: str,
    paths,
    settings,
) -> list[dict[str, Any]]:
    html = _fetch_catalog_html(source, paths, settings, source_url=source_url, local_path=local_path)
    base_url = Path(_resolve_local_path(paths, local_path)).as_uri() if local_path else source_url
    allowed_domains = _allowed_domains(source, source_url)
    detail_sources: list[dict[str, Any]] = []
    seen: set[str] = set()
    soup = BeautifulSoup(html, "lxml")
    for anchor in soup.find_all("a", href=True):
        href = normalize_whitespace(anchor.get("href"))
        if not href:
            continue
        resolved_url, resolved_local_path, is_local = _resolve_catalog_link(source, href, base_url=base_url, paths=paths)
        inferred_type = _infer_discovered_source_type(
            source_url="" if resolved_local_path else resolved_url,
            local_path=resolved_local_path,
            label=normalize_whitespace(anchor.get_text(" ", strip=True)),
        )
        if inferred_type not in {"csv_file", "csv_url", "xlsx_file", "xlsx_url"}:
            continue
        if not is_local:
            resolved_domain = strip_protocol(urlparse(resolved_url).netloc)
            if allowed_domains and not any(
                resolved_domain == domain or resolved_domain.endswith(f".{domain}") for domain in allowed_domains
            ):
                continue
        dedupe_key = resolved_local_path or resolved_url
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        child_label = normalize_whitespace(anchor.get_text(" ", strip=True)) or Path(urlparse(resolved_url).path).name or label
        detail_sources.append(
            _build_discovered_source_row(
                source,
                label=f"{label} / {child_label}",
                source_url="" if resolved_local_path else resolved_url,
                local_path=resolved_local_path,
            )
        )
    return detail_sources


def _header_row_value(source: dict[str, Any]) -> int | None:
    raw = str(source.get("header_row") or "").strip()
    if raw == "":
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _looks_like_embedded_header(values: list[str]) -> bool:
    normalized = [normalize_whitespace(value) for value in values if normalize_whitespace(value)]
    if len(normalized) < 2:
        return False
    header_tokens = {
        "회사명",
        "기업명",
        "기관명",
        "업체명",
        "법인명",
        "홈페이지",
        "기업정보",
        "기관정보",
        "지역",
        "소재지",
        "전문분야",
        "AI솔루션",
    }
    return sum(token in normalized for token in header_tokens) >= 2


def _promote_embedded_header_row(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame

    column_names = [normalize_whitespace(str(column)) for column in frame.columns]
    unnamed_count = sum(
        1
        for column in column_names
        if not column or column.lower().startswith("unnamed:")
    )
    if unnamed_count == 0 and not any("현황" in column for column in column_names):
        return frame

    probe_rows = min(len(frame), 5)
    for idx in range(probe_rows):
        row_values = [normalize_whitespace(str(value)) for value in frame.iloc[idx].tolist()]
        if not _looks_like_embedded_header(row_values):
            continue
        promoted = frame.iloc[idx + 1 :].copy().reset_index(drop=True)
        promoted.columns = [
            value or f"column_{position + 1}" for position, value in enumerate(row_values)
        ]
        return promoted
    return frame


def _request_content(
    url: str,
    settings,
    *,
    method: str = "GET",
    data: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[bytes, str | None]:
    request_headers = {"User-Agent": getattr(settings, "user_agent", "jobs-market-v2/0.1")}
    if headers:
        request_headers.update(headers)
    total_timeout = getattr(settings, "company_seed_timeout_seconds", None)
    connect_timeout = getattr(settings, "company_seed_connect_timeout_seconds", None)
    if total_timeout is None:
        total_timeout = getattr(settings, "timeout_seconds", 20.0)
    if connect_timeout is None:
        connect_timeout = getattr(settings, "connect_timeout_seconds", 5.0)
    deadline = monotonic() + float(total_timeout)
    with httpx.stream(
        method.upper(),
        url,
        headers=request_headers,
        timeout=build_timeout(
            total_timeout,
            connect_timeout,
        ),
        follow_redirects=True,
        data=data,
        json=json_body,
    ) as response:
        response.raise_for_status()
        chunks: list[bytes] = []
        for chunk in response.iter_bytes():
            chunks.append(chunk)
            if monotonic() >= deadline:
                raise httpx.ReadTimeout(
                    f"company seed request exceeded {float(total_timeout):.1f}s deadline",
                    request=response.request,
                )
        return b"".join(chunks), response.encoding


def _fetch_text(
    url: str,
    settings,
    *,
    method: str = "GET",
    data: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> str:
    content, encoding = _request_content(
        url,
        settings,
        method=method,
        data=data,
        json_body=json_body,
        headers=headers,
    )
    return content.decode(encoding or "utf-8", errors="ignore")


def _fetch_bytes(url: str, settings) -> bytes:
    content, _ = _request_content(url, settings)
    return content


def _fetch_json(
    url: str,
    settings,
    *,
    method: str = "GET",
    data: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    content, encoding = _request_content(
        url,
        settings,
        method=method,
        data=data,
        json_body=json_body,
        headers=headers,
    )
    return json.loads(content.decode(encoding or "utf-8", errors="ignore"))


def _discover_sources_from_html_catalog(source: dict[str, Any], paths, settings) -> list[dict[str, Any]]:
    source_type = normalize_whitespace(source.get("source_type"))
    if source_type == "html_link_catalog_file":
        catalog_path = _resolve_local_path(paths, normalize_whitespace(source.get("local_path")))
        base_url = catalog_path.as_uri()
        html = catalog_path.read_text(encoding="utf-8")
    else:
        base_url = normalize_whitespace(source.get("source_url"))
        html = _fetch_text(base_url, settings)

    soup = BeautifulSoup(html, "lxml")
    include_keywords = _keyword_list(source.get("discovery_include_keywords"))
    exclude_keywords = _keyword_list(source.get("discovery_exclude_keywords"))
    if not include_keywords:
        include_keywords = [
            "기업목록",
            "기업 리스트",
            "참여기업",
            "선정기업",
            "수행기관",
            "공급기업",
            "기관목록",
            "기업현황",
            "입주기업",
            "회원사",
            "company list",
            "participants",
            "partners",
        ]
    if not exclude_keywords:
        exclude_keywords = ["공지", "안내", "FAQ", "제안요청", "서식", "매뉴얼", "notice", "faq", "guide", "manual"]

    allowed_domains = _allowed_domains(source, base_url)
    discovered: list[dict[str, Any]] = []
    seen: set[str] = set()
    max_discovered = int(str(source.get("max_discovered_sources") or "").strip() or 20)

    for anchor in soup.find_all("a", href=True):
        href = normalize_whitespace(anchor.get("href"))
        text = normalize_whitespace(anchor.get_text(" ", strip=True)) or href
        if not href:
            continue

        resolved_url, local_path, is_local = _resolve_catalog_link(source, href, base_url=base_url, paths=paths)
        if is_local:
            domain_ok = True
        else:
            resolved_domain = strip_protocol(urlparse(resolved_url).netloc)
            domain_ok = not allowed_domains or any(
                resolved_domain == domain or resolved_domain.endswith(f".{domain}") for domain in allowed_domains
            )

        haystack = _link_haystack(text, resolved_url)
        if not domain_ok:
            if (
                not is_local
                and _matches_keywords(haystack, include_keywords)
                and not _blocked_by_keywords(haystack, exclude_keywords)
                and _looks_like_external_catalog_host_candidate(haystack, source_url=resolved_url)
            ):
                host_key = _normalize_seed_source_url_for_dedupe(_normalize_https_url(resolved_domain))
                if host_key not in seen:
                    seen.add(host_key)
                    discovered.append(
                        _build_external_catalog_host_row(
                            source,
                            label=text,
                            allowed_domain=resolved_domain,
                        )
                    )
                    if len(discovered) >= max_discovered:
                        break
            if (
                not is_local
                and _is_trusted_external_catalog_domain(resolved_domain)
                and _matches_keywords(haystack, include_keywords)
                and not _blocked_by_keywords(haystack, exclude_keywords)
                and _looks_like_catalog_candidate(haystack, source_url=resolved_url)
            ):
                dedupe_key = _normalize_seed_source_url_for_dedupe(resolved_url)
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                discovered.append(
                    _build_external_catalog_source_row(
                        source,
                        label=text,
                        source_url=resolved_url,
                        allowed_domain=resolved_domain,
                    )
                )
                if len(discovered) >= max_discovered:
                    break
            continue
        if not _matches_keywords(haystack, include_keywords):
            continue
        if _blocked_by_keywords(haystack, exclude_keywords):
            continue

        dedupe_key = local_path or resolved_url
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        inferred_type = _infer_discovered_source_type(
            source_url="" if local_path else resolved_url,
            local_path=local_path,
            label=text,
        )
        effective_type = normalize_whitespace(source.get("discovered_source_type")) or inferred_type
        if effective_type in {"html_table_file", "html_table_url"}:
            detail_sources = _discover_attachment_sources_from_detail_page(
                source,
                source_url="" if local_path else resolved_url,
                local_path=local_path,
                label=text,
                paths=paths,
                settings=settings,
            )
            if detail_sources:
                for detail_source in detail_sources:
                    detail_key = normalize_whitespace(detail_source.get("local_path")) or normalize_whitespace(detail_source.get("source_url"))
                    if detail_key in seen:
                        continue
                    seen.add(detail_key)
                    discovered.append(detail_source)
                    if len(discovered) >= max_discovered:
                        break
                if len(discovered) >= max_discovered:
                    break
                continue
            if _html_page_contains_table(
                source_url="" if local_path else resolved_url,
                local_path=local_path,
                paths=paths,
                settings=settings,
            ):
                discovered.append(
                    _build_discovered_source_row(
                        source,
                        label=text,
                        source_url="" if local_path else resolved_url,
                        local_path=local_path,
                    )
                )
                if len(discovered) >= max_discovered:
                    break
                continue
            if _looks_like_catalog_candidate(
                haystack,
                source_url="" if local_path else resolved_url,
                local_path=local_path,
            ):
                discovered.append(
                    _build_discovered_source_row(
                        source,
                        label=text,
                        source_url="" if local_path else resolved_url,
                        local_path=local_path,
                    )
                    | {"source_type": _infer_catalog_page_source_type(local_path=local_path)}
                )
                if len(discovered) >= max_discovered:
                    break
                continue
            # Generic detail/download HTML pages should not fall back to discovered table sources.
            continue

        discovered.append(
            _build_discovered_source_row(
                source,
                label=text,
                source_url="" if local_path else resolved_url,
                local_path=local_path,
            )
        )
        if len(discovered) >= max_discovered:
            break
    return discovered


def _discover_top_level_catalog_sources_from_sitemap(source: dict[str, Any], settings) -> list[dict[str, Any]]:
    source_type = normalize_whitespace(source.get("source_type"))
    source_url = normalize_whitespace(source.get("source_url"))
    if source_type != "html_link_catalog_url" or not source_url:
        return []

    include_keywords = _keyword_list(source.get("discovery_include_keywords"))
    exclude_keywords = _keyword_list(source.get("discovery_exclude_keywords"))
    if not include_keywords:
        include_keywords = [
            "참여기업",
            "공급기업",
            "수행기관",
            "선정기업",
            "기관목록",
            "기업목록",
            "pool",
            "company-list",
            "participants",
        ]
    allowed_domains = _allowed_domains(source, source_url)
    max_discovered = min(int(str(source.get("max_discovered_sources") or "").strip() or 20), 10)
    sitemap_candidates = []
    for domain in sorted(allowed_domains):
        sitemap_candidates.extend(
            [
                f"https://{domain}/sitemap.xml",
                f"https://{domain}/sitemap_index.xml",
            ]
        )

    discovered: list[dict[str, Any]] = []
    seen: set[str] = set()
    for sitemap_url in sitemap_candidates:
        try:
            xml = _fetch_text(sitemap_url, settings)
        except Exception:  # noqa: BLE001
            continue
        soup = BeautifulSoup(xml, "xml")
        for loc in soup.find_all("loc"):
            loc_url = normalize_whitespace(loc.get_text(" ", strip=True))
            if not loc_url or loc_url == source_url:
                continue
            parsed = urlparse(loc_url)
            domain = strip_protocol(parsed.netloc)
            if allowed_domains and not any(
                domain == allowed_domain or domain.endswith(f".{allowed_domain}") for allowed_domain in allowed_domains
            ):
                continue
            haystack = _link_haystack(parsed.path, loc_url)
            if not _matches_keywords(haystack, include_keywords):
                continue
            if _blocked_by_keywords(haystack, exclude_keywords):
                continue
            if not _looks_like_catalog_candidate(haystack, source_url=loc_url):
                continue
            dedupe_key = _normalize_seed_source_url_for_dedupe(loc_url)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            label = Path(parsed.path).name or parsed.path or loc_url
            discovered.append(
                _build_discovered_source_row(source, label=label, source_url=loc_url)
                | {"source_type": "html_link_catalog_url"}
            )
            if len(discovered) >= max_discovered:
                return discovered
    return discovered


def _looks_like_catalog_host_company(company_name: str, company_tier: str, official_domain: str) -> bool:
    if company_tier != "공공·연구기관" or not official_domain:
        return False
    haystack = normalize_whitespace(f"{company_name} {official_domain}").lower()
    return any(keyword.lower() in haystack for keyword in CATALOG_HOST_KEYWORDS)


def _load_catalog_host_company_rows(paths) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    if paths.companies_registry_path.exists():
        frame = pd.read_csv(paths.companies_registry_path)
        for column in ("company_name", "company_tier", "official_domain"):
            if column not in frame.columns:
                frame[column] = ""
        frames.append(frame[["company_name", "company_tier", "official_domain"]])
    for path in (paths.company_seed_records_path, paths.collected_company_seed_records_path):
        frame = read_csv_or_empty(path, IMPORT_COMPANY_COLUMNS)
        if frame.empty:
            continue
        frames.append(frame[["company_name", "company_tier", "official_domain"]])
    if not frames:
        return pd.DataFrame(columns=["company_name", "company_tier", "official_domain"])
    combined = pd.concat(frames, ignore_index=True).fillna("")
    combined = combined.drop_duplicates(subset=["company_name", "official_domain"], keep="last").reset_index(drop=True)
    return combined


def _load_candidate_catalog_host_rows(paths) -> pd.DataFrame:
    frame = read_csv_or_empty(paths.company_candidates_path)
    if frame.empty:
        return pd.DataFrame(columns=["company_name", "company_tier", "official_domain", "evidence_type", "evidence_url"])
    for column in ("company_name", "company_tier", "official_domain", "primary_evidence_type", "primary_evidence_url"):
        if column not in frame.columns:
            frame[column] = ""
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in frame.fillna("").to_dict(orient="records"):
        company_name = normalize_whitespace(row.get("company_name"))
        company_tier = normalize_whitespace(row.get("company_tier"))
        host = _normalize_host(row.get("official_domain"))
        if not host:
            host = _normalize_host(urlparse(normalize_whitespace(row.get("primary_evidence_url"))).netloc)
        if not host:
            continue
        key = (company_name, host)
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "company_name": company_name,
                "company_tier": company_tier,
                "official_domain": host,
                "evidence_type": normalize_whitespace(row.get("primary_evidence_type")),
                "evidence_url": normalize_whitespace(row.get("primary_evidence_url")),
            }
        )
    return pd.DataFrame(rows)


def _build_catalog_host_seed_source(row: dict[str, Any]) -> dict[str, Any]:
    official_domain = _normalize_host(row.get("official_domain"))
    return {
        "source_name": f"{normalize_whitespace(row.get('company_name'))} 공식 홈페이지 자동 카탈로그 탐색",
        "source_type": "html_link_catalog_url",
        "discovered_source_type": "",
        "source_url": _normalize_https_url(official_domain),
        "request_url": "",
        "local_path": "",
        "source_title": f"{normalize_whitespace(row.get('company_name'))} 공식 홈페이지",
        "company_tier": normalize_whitespace(row.get("company_tier")),
        "candidate_seed_type": "공식카탈로그도메인자동발견",
        "candidate_seed_reason": "공공·지원기관 공식 도메인에서 참여기업·공급기업·수행기관 목록 카탈로그를 자동 탐색",
        "table_index": "",
        "header_row": "",
        "company_name_column": "",
        "official_domain_column": "",
        "company_name_en_column": "",
        "region_column": "",
        "aliases_column": "",
        "filter_text_columns": "",
        "include_keywords": "",
        "exclude_keywords": "",
        "allowed_domains": official_domain,
        "discovery_include_keywords": "참여기업;공급기업;수행기관;지원기업;선정기업;기업 목록;기관목록;pool;participants;company-list",
        "discovery_exclude_keywords": "채용;입찰;공지;보도;교육;세미나;설명회;계획서;안내문;pdf;zip;login;logout;member;join;main.do;board.do;accessmsg",
        "max_discovered_sources": "5",
    }


def _load_evidence_catalog_host_rows(paths) -> pd.DataFrame:
    evidence = read_csv_or_empty(paths.company_evidence_path)
    if evidence.empty or "evidence_url" not in evidence.columns:
        return pd.DataFrame(columns=["company_name", "company_tier", "official_domain", "evidence_type", "evidence_url"])

    companies = read_csv_or_empty(paths.company_candidates_path)
    company_tier_map: dict[str, str] = {}
    if not companies.empty and "company_name" in companies.columns:
        tier_series = companies.set_index("company_name").get("company_tier")
        if tier_series is not None:
            company_tier_map = tier_series.fillna("").astype(str).to_dict()

    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for row in evidence.fillna("").to_dict(orient="records"):
        evidence_type = normalize_whitespace(row.get("evidence_type"))
        if evidence_type not in CATALOG_HOST_EVIDENCE_TYPES:
            continue
        evidence_url = normalize_whitespace(row.get("evidence_url"))
        host = _normalize_host(urlparse(evidence_url).netloc)
        if not _is_trusted_external_catalog_domain(host):
            continue
        company_name = normalize_whitespace(row.get("company_name"))
        company_tier = company_tier_map.get(company_name, "")
        key = (company_name, company_tier, host)
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "company_name": company_name,
                "company_tier": company_tier,
                "official_domain": host,
                "evidence_type": evidence_type,
                "evidence_url": evidence_url,
            }
        )
    return pd.DataFrame(rows)


def _catalog_host_score(row: dict[str, Any]) -> tuple[int, int]:
    company_name = normalize_whitespace(row.get("company_name"))
    company_tier = normalize_whitespace(row.get("company_tier"))
    official_domain = _normalize_host(row.get("official_domain"))
    evidence_type = normalize_whitespace(row.get("evidence_type"))
    evidence_url = normalize_whitespace(row.get("evidence_url"))
    haystack = normalize_whitespace(f"{company_name} {official_domain} {evidence_type} {evidence_url}").lower()
    score = 0
    if company_tier == "공공·연구기관":
        score += 4
    if any(keyword.lower() in haystack for keyword in CATALOG_HOST_KEYWORDS):
        score += 3
    if evidence_type in {"후보시드근거", "공식도메인"}:
        score += 2
    if any(token in haystack for token in ("참여기업", "공급기업", "수행기관", "지원기업", "선정기업", "participants", "company-list", "pool")):
        score += 2
    if _is_trusted_external_catalog_domain(official_domain):
        score += 1
    return score, len(company_name)


def _discover_catalog_host_seed_sources(paths, settings, existing_sources: list[dict[str, Any]]) -> list[dict[str, Any]]:
    existing_domains = {
        _normalize_host(urlparse(normalize_whitespace(source.get("source_url"))).netloc)
        for source in existing_sources
        if normalize_whitespace(source.get("source_type")) in CATALOG_SOURCE_TYPES
    }
    host_rows = _load_catalog_host_company_rows(paths)
    candidate_host_rows = _load_candidate_catalog_host_rows(paths)
    evidence_host_rows = _load_evidence_catalog_host_rows(paths)
    if host_rows.empty and candidate_host_rows.empty and evidence_host_rows.empty:
        return []
    max_hosts = int(getattr(settings, "company_seed_catalog_host_limit", 25) or 25)
    discovered_hosts: list[dict[str, Any]] = []
    ranked_rows: list[dict[str, Any]] = []
    if not host_rows.empty:
        ranked_rows.extend(host_rows.fillna("").to_dict(orient="records"))
    if not candidate_host_rows.empty:
        ranked_rows.extend(candidate_host_rows.fillna("").to_dict(orient="records"))
    if not evidence_host_rows.empty:
        ranked_rows.extend(evidence_host_rows.fillna("").to_dict(orient="records"))
    ranked_rows = sorted(
        ranked_rows,
        key=lambda row: _catalog_host_score(row),
        reverse=True,
    )

    seen_candidate_domains: set[str] = set()
    for row in ranked_rows:
        company_name = normalize_whitespace(row.get("company_name"))
        company_tier = normalize_whitespace(row.get("company_tier"))
        official_domain = _normalize_host(row.get("official_domain"))
        if not official_domain:
            continue
        if official_domain in existing_domains or official_domain in seen_candidate_domains:
            continue
        if not (
            _looks_like_catalog_host_company(company_name, company_tier, official_domain)
            or _catalog_host_score(row)[0] >= 5
        ):
            continue
        discovered_hosts.append(_build_catalog_host_seed_source(row))
        existing_domains.add(official_domain)
        seen_candidate_domains.add(official_domain)
        if len(discovered_hosts) >= max_hosts:
            break
    return discovered_hosts


def _source_request_url(source: dict[str, Any]) -> str:
    return normalize_whitespace(source.get("request_url")) or normalize_whitespace(source.get("source_url"))


def _kind_company_tier(title_cell, fallback_tier: str) -> str:
    badge_alts = {normalize_whitespace(img.get("alt")) for img in title_cell.find_all("img")}
    if {"KOSPI200", "KRX300"} & badge_alts:
        return "대기업"
    return fallback_tier or "중견/중소"


def _load_kind_corp_list(source: dict[str, Any], settings) -> pd.DataFrame:
    payload = {
        "method": "searchCorpList",
        "pageIndex": "1",
        "currentPageSize": "3000",
        "comAbbrv": "",
        "beginIndex": "",
        "orderMode": "",
        "orderStat": "",
        "isurCd": "",
        "repIsuSrtCd": "",
        "searchCodeType": "",
        "marketType": "",
        "industry": "",
        "fiscalYearEnd": "all",
        "location": "all",
    }
    html = _fetch_text(_source_request_url(source), settings, method="POST", data=payload)
    soup = BeautifulSoup(html, "lxml")
    rows: list[dict[str, Any]] = []
    fallback_tier = normalize_whitespace(source.get("company_tier"))
    for tr in soup.select("tbody tr"):
        cells = tr.find_all("td")
        if len(cells) != 8:
            continue
        title_cell = cells[0]
        company_anchor = title_cell.find("a")
        homepage_anchor = cells[6].find("a", href=True)
        company_name = normalize_whitespace(company_anchor.get_text(" ", strip=True) if company_anchor else title_cell.get_text(" ", strip=True))
        if not company_name:
            continue
        rows.append(
            {
                "company_name": company_name,
                "company_tier": _kind_company_tier(title_cell, fallback_tier),
                "official_domain": strip_protocol(homepage_anchor.get("href") if homepage_anchor else ""),
                "company_name_en": "",
                "region": normalize_whitespace(cells[7].get_text(" ", strip=True)),
                "aliases": [],
                "industry": normalize_whitespace(cells[1].get("title") or cells[1].get_text(" ", strip=True)),
                "products": normalize_whitespace(cells[2].get("title") or cells[2].get_text(" ", strip=True)),
            }
        )
    return pd.DataFrame(rows)


def _load_alio_public_agency_list(source: dict[str, Any], settings) -> pd.DataFrame:
    request_url = _source_request_url(source) or "https://www.alio.go.kr/organ/findOrganApbaList.json"
    referer_url = normalize_whitespace(source.get("source_url")) or "https://www.alio.go.kr/guide/publicAgencyList.do"
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": referer_url,
    }
    first_page = _fetch_json(request_url, settings, method="POST", json_body={"apbaNa": "", "pageNo": 1}, headers=headers)
    organ_list = (((first_page or {}).get("data") or {}).get("organList") or {})
    total_pages = int((((organ_list.get("page") or {}).get("totalPage")) or 1))
    rows = list(organ_list.get("result") or [])
    for page_no in range(2, total_pages + 1):
        page_payload = _fetch_json(
            request_url,
            settings,
            method="POST",
            json_body={"apbaNa": "", "pageNo": page_no},
            headers=headers,
        )
        rows.extend((((page_payload or {}).get("data") or {}).get("organList") or {}).get("result") or [])

    frame_rows: list[dict[str, Any]] = []
    for row in rows:
        company_name = normalize_whitespace(row.get("apbaNa"))
        if not company_name:
            continue
        frame_rows.append(
            {
                "company_name": company_name,
                "company_tier": normalize_whitespace(source.get("company_tier")) or "공공·연구기관",
                "official_domain": strip_protocol(row.get("homepage")),
                "company_name_en": "",
                "region": normalize_whitespace(row.get("addrCd")),
                "aliases": [],
                "agency_type": normalize_whitespace(row.get("typeNa")),
                "authority": normalize_whitespace(row.get("jidtNa")),
                "contents": normalize_whitespace(str(row.get("contents") or "").replace("&cr;", " ")),
            }
        )
    return pd.DataFrame(frame_rows)


def _load_nst_research_institutes(source: dict[str, Any], settings) -> pd.DataFrame:
    html = _fetch_text(_source_request_url(source), settings)
    soup = BeautifulSoup(html, "lxml")
    rows: list[dict[str, Any]] = []
    fallback_tier = normalize_whitespace(source.get("company_tier")) or "공공·연구기관"
    for card in soup.select(".agency_list .link_item"):
        inner = card.select_one(".link_inner")
        if inner is None:
            continue
        logo = inner.select_one(".link_logo img")
        name = normalize_whitespace(logo.get("alt") if logo else "")
        link = inner.select_one(".link_text a[href]")
        description = normalize_whitespace(inner.select_one(".link_text p").get_text(" ", strip=True) if inner.select_one(".link_text p") else "")
        if not name:
            continue
        rows.append(
            {
                "company_name": name,
                "company_tier": fallback_tier,
                "official_domain": strip_protocol(link.get("href") if link else ""),
                "company_name_en": "",
                "region": "",
                "aliases": [],
                "description": description,
            }
        )
    return pd.DataFrame(rows)


def _load_source_frame(source: dict[str, Any], paths, settings) -> pd.DataFrame:
    source_type = normalize_whitespace(source.get("source_type"))
    source_url = normalize_whitespace(source.get("source_url"))
    local_path = normalize_whitespace(source.get("local_path"))
    header_row = _header_row_value(source)
    read_kwargs = {"header": header_row} if header_row is not None else {}

    if source_type == "csv_file":
        return _promote_embedded_header_row(pd.read_csv(_resolve_local_path(paths, local_path), **read_kwargs))
    if source_type == "xlsx_file":
        return _promote_embedded_header_row(pd.read_excel(_resolve_local_path(paths, local_path), **read_kwargs))
    if source_type == "html_table_file":
        html = _resolve_local_path(paths, local_path).read_text(encoding="utf-8")
        tables = pd.read_html(StringIO(html))
        return _promote_embedded_header_row(tables[int(source.get("table_index") or 0)])
    if source_type == "csv_url":
        return _promote_embedded_header_row(
            pd.read_csv(StringIO(_fetch_text(_source_request_url(source) or source_url, settings)), **read_kwargs)
        )
    if source_type == "xlsx_url":
        return _promote_embedded_header_row(
            pd.read_excel(BytesIO(_fetch_bytes(_source_request_url(source) or source_url, settings)), **read_kwargs)
        )
    if source_type == "html_table_url":
        tables = pd.read_html(StringIO(_fetch_text(_source_request_url(source) or source_url, settings)))
        return _promote_embedded_header_row(tables[int(source.get("table_index") or 0)])
    if source_type == "kind_corp_list":
        return _load_kind_corp_list(source, settings)
    if source_type == "alio_public_agency_list":
        return _load_alio_public_agency_list(source, settings)
    if source_type == "nst_research_institutes":
        return _load_nst_research_institutes(source, settings)
    raise ValueError(f"지원하지 않는 company seed source type입니다: {source_type}")


def _evaluate_shadow_seed_source(source: dict[str, Any], paths, settings) -> tuple[bool, str]:
    source_type = normalize_whitespace(source.get("source_type"))
    if source_type in CATALOG_SOURCE_TYPES:
        discovered_children = _discover_sources_from_html_catalog(source, paths, settings)
        if discovered_children:
            return True, f"child seed source {len(discovered_children)}건 발견"
        return False, "child seed source 0건"
    raw_frame = _load_source_frame(source, paths, settings)
    filtered_frame, filter_summary = _apply_source_filters(raw_frame, source)
    records = _frame_to_company_records(filtered_frame, source)
    if records:
        return True, f"회사 레코드 {len(records)}건 추출"
    return False, f"회사 레코드 0건 (raw={int(filter_summary['raw_count'])}, filtered={int(filter_summary['filtered_count'])})"


def _mapped_value(row: dict[str, Any], explicit_column: str) -> str:
    column_name = normalize_whitespace(explicit_column)
    if not column_name:
        return ""
    value = row.get(column_name)
    if isinstance(value, list):
        return ""
    return normalize_whitespace("" if value is None else str(value))


def _detect_column(frame: pd.DataFrame, explicit_column: str, candidates: tuple[str, ...]) -> str:
    column_name = normalize_whitespace(explicit_column)
    if column_name and column_name in frame.columns:
        return column_name
    normalized_columns = {normalize_whitespace(column).lower(): column for column in frame.columns}
    for candidate in candidates:
        matched = normalized_columns.get(candidate.lower())
        if matched:
            return matched
    for column in frame.columns:
        normalized = normalize_whitespace(column).lower()
        if any(candidate.lower() in normalized for candidate in candidates):
            return column
    return ""


def _extract_domain_from_text(text: str) -> str:
    normalized = normalize_whitespace(text)
    if not normalized:
        return ""
    patterns = [
        r"https?://[^\s)>\]]+",
        r"www\.[^\s)>\]]+",
        r"[A-Za-z0-9.-]+\.[A-Za-z]{2,}(?:/[^\s)>\]]*)?",
    ]
    for pattern in patterns:
        matched = re.search(pattern, normalized)
        if matched:
            return strip_protocol(matched.group(0))
    return ""


def _extract_region_from_text(text: str) -> str:
    normalized = normalize_whitespace(text)
    if not normalized:
        return ""
    labeled = re.search(
        r"(?:지역|소재지|주소)\s*:\s*(.+?)(?=\s*(?:-|/)?\s*(?:홈페이지|웹사이트|website)\b|https?://|www\.|$)",
        normalized,
    )
    if labeled:
        return normalize_whitespace(labeled.group(1))
    region_match = re.search(
        r"(서울특별시|부산광역시|대구광역시|인천광역시|광주광역시|대전광역시|울산광역시|세종특별자치시|경기도|강원특별자치도|충청북도|충청남도|전북특별자치도|전라남도|경상북도|경상남도|제주특별자치도)",
        normalized,
    )
    if region_match:
        return region_match.group(1)
    return ""


def _keyword_list(value: Any) -> list[str]:
    return [keyword for keyword in parse_aliases(value) if keyword]


def _filter_text_columns(frame: pd.DataFrame, source: dict[str, Any]) -> list[str]:
    configured = _keyword_list(source.get("filter_text_columns"))
    if not configured:
        return list(frame.columns)
    columns = [column for column in configured if column in frame.columns]
    return columns or list(frame.columns)


def _apply_source_filters(frame: pd.DataFrame, source: dict[str, Any]) -> tuple[pd.DataFrame, dict[str, Any]]:
    include_keywords = _keyword_list(source.get("include_keywords"))
    exclude_keywords = _keyword_list(source.get("exclude_keywords"))
    if frame.empty or (not include_keywords and not exclude_keywords):
        return frame, {
            "raw_count": int(len(frame)),
            "filtered_count": int(len(frame)),
            "filter_applied": False,
        }

    text_columns = _filter_text_columns(frame, source)
    text = (
        frame[text_columns]
        .fillna("")
        .astype(str)
        .apply(lambda column: column.map(normalize_whitespace))
        .agg(" ".join, axis=1)
    )
    mask = pd.Series(True, index=frame.index)
    if include_keywords:
        include_pattern = "|".join(re.escape(keyword) for keyword in include_keywords)
        mask &= text.str.contains(include_pattern, case=False, regex=True, na=False)
    if exclude_keywords:
        exclude_pattern = "|".join(re.escape(keyword) for keyword in exclude_keywords)
        mask &= ~text.str.contains(exclude_pattern, case=False, regex=True, na=False)
    filtered = frame.loc[mask].reset_index(drop=True)
    return filtered, {
        "raw_count": int(len(frame)),
        "filtered_count": int(len(filtered)),
        "filter_applied": True,
        "filter_text_columns": text_columns,
    }


def _frame_to_company_records(frame: pd.DataFrame, source: dict[str, Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    source_url = normalize_whitespace(source.get("source_url"))
    source_title = normalize_whitespace(source.get("source_title")) or normalize_whitespace(source.get("source_name"))
    company_name_column = _detect_column(
        frame,
        source.get("company_name_column", ""),
        ("company_name", "회사명", "기업명", "기관명", "업체명", "법인명", "참여기업", "선정기업", "수행기관", "기관"),
    )
    official_domain_column = _detect_column(
        frame,
        source.get("official_domain_column", ""),
        ("official_domain", "홈페이지", "홈페이지주소", "웹사이트", "website", "url", "기관홈페이지"),
    )
    company_name_en_column = _detect_column(
        frame,
        source.get("company_name_en_column", ""),
        ("company_name_en", "영문명", "영문회사명", "영문기관명"),
    )
    region_column = _detect_column(
        frame,
        source.get("region_column", ""),
        ("region", "지역", "소재지", "주소", "본사", "소속지역"),
    )
    aliases_column = _detect_column(
        frame,
        source.get("aliases_column", ""),
        ("aliases", "별칭", "약칭"),
    )

    for row in frame.fillna("").to_dict(orient="records"):
        company_name = _mapped_value(row, company_name_column)
        if not company_name:
            company_name = normalize_whitespace(row.get("company_name"))
        if not company_name:
            continue
        row_text = " ".join(normalize_whitespace(str(value)) for value in row.values() if normalize_whitespace(str(value)))
        official_domain_text = _mapped_value(row, official_domain_column) or normalize_whitespace(row.get("official_domain"))
        official_domain = strip_protocol(official_domain_text)
        if official_domain and (" " in official_domain or "지역" in official_domain or "홈페이지" in official_domain):
            official_domain = ""
        if not official_domain:
            official_domain = _extract_domain_from_text(row_text)
        region_text = _mapped_value(row, region_column) or normalize_whitespace(row.get("region"))
        region = _extract_region_from_text(region_text) if region_text else ""
        if not region:
            region = normalize_whitespace(region_text)
        if region and any(token in region.lower() for token in ("홈페이지", "website", "http://", "https://", "www.")):
            region = ""
        if not region:
            region = _extract_region_from_text(row_text)
        records.append(
            {
                "company_name": company_name,
                "company_tier": (
                    _mapped_value(row, source.get("company_tier_column", ""))
                    or normalize_whitespace(row.get("company_tier"))
                    or normalize_whitespace(source.get("company_tier"))
                ),
                "official_domain": official_domain,
                "company_name_en": _mapped_value(row, company_name_en_column) or normalize_whitespace(row.get("company_name_en")),
                "region": region,
                "aliases": parse_aliases(_mapped_value(row, aliases_column) or row.get("aliases")),
                "discovery_method": "company_seed_source",
                "candidate_seed_type": normalize_whitespace(source.get("candidate_seed_type")),
                "candidate_seed_url": source_url,
                "candidate_seed_title": source_title,
                "candidate_seed_reason": normalize_whitespace(source.get("candidate_seed_reason")),
            }
        )
    return records


def discover_company_seed_sources(paths, settings, *, target: str = "shadow") -> tuple[pd.DataFrame, dict]:
    if target not in {"shadow", "approved"}:
        raise ValueError("target must be 'shadow' or 'approved'.")
    target_path = paths.shadow_company_seed_sources_path if target == "shadow" else paths.discovered_company_seed_sources_path
    current_iso = _seed_source_now_iso()
    existing = _stamp_seed_source_frame(read_csv_or_empty(target_path, COMPANY_SEED_SOURCE_COLUMNS), now_iso=current_iso)
    pruned_shadow_count = 0
    if target == "shadow":
        existing, pruned_shadow_count = _prune_shadow_seed_sources(existing, settings)
    invalid = load_invalid_company_seed_sources(paths)
    invalid, pruned_invalid_count = _prune_invalid_seed_sources(invalid, settings)
    write_csv(invalid, paths.invalid_company_seed_sources_path)
    skip_cache = _load_catalog_skip_cache(paths)
    refresh_cache = _load_catalog_refresh_cache(paths)
    invalid_keys = (
        set(invalid.apply(lambda row: _seed_source_dedupe_key(row.to_dict()), axis=1).tolist())
        if not invalid.empty
        else set()
    )
    base_sources = load_yaml(paths.company_seed_sources_path)
    sources = (base_sources or {}).get("sources") if isinstance(base_sources, dict) else []
    approved_discovered = read_csv_or_empty(paths.discovered_company_seed_sources_path, COMPANY_SEED_SOURCE_COLUMNS)
    approved_catalog_sources = approved_discovered[
        approved_discovered["source_type"].fillna("").isin(CATALOG_SOURCE_TYPES)
    ].fillna("").to_dict(orient="records")
    target_existing_sources = existing.fillna("").to_dict(orient="records")
    seed_universe = [*(sources or []), *approved_catalog_sources]
    search_discovered_sources, search_summary = _discover_catalog_seed_sources_from_search(
        paths,
        [*seed_universe, *target_existing_sources],
        settings,
    )
    auto_host_sources = _discover_catalog_host_seed_sources(
        paths,
        settings,
        [*seed_universe, *target_existing_sources, *search_discovered_sources],
    )
    persisted_catalog_sources = [
        row
        for row in search_discovered_sources
        if _seed_source_dedupe_key(row) not in invalid_keys
    ]
    catalog_sources = [
        source
        for source in [*seed_universe, *persisted_catalog_sources, *auto_host_sources]
        if normalize_whitespace(source.get("source_type")) in CATALOG_SOURCE_TYPES
    ]
    deduped_catalog_sources: list[dict[str, Any]] = []
    seen_catalog_keys: set[str] = set()
    cached_skip_count = 0
    cached_refresh_count = 0
    for source in catalog_sources:
        key = _seed_source_dedupe_key(source)
        if key in seen_catalog_keys:
            continue
        seen_catalog_keys.add(key)
        if _is_catalog_skip_cache_active(skip_cache.get(key, {}), settings):
            cached_skip_count += 1
            continue
        if _is_catalog_refresh_cache_active(refresh_cache.get(key, {}), settings):
            cached_refresh_count += 1
            continue
        deduped_catalog_sources.append(source)
    catalog_sources = deduped_catalog_sources
    total_catalog_source_count = int(len(catalog_sources))
    progress = _load_catalog_progress(paths)
    start_offset = int(progress.get("next_offset", 0) or 0)
    batch_size = total_catalog_source_count if settings is None else int(
        getattr(settings, "company_seed_catalog_batch_size", total_catalog_source_count) or total_catalog_source_count
    )
    if total_catalog_source_count and start_offset >= total_catalog_source_count:
        start_offset = 0
    if batch_size > 0 and total_catalog_source_count > batch_size:
        end_offset = min(start_offset + batch_size, total_catalog_source_count)
        selected_catalog_sources = catalog_sources[start_offset:end_offset]
        next_offset = 0 if end_offset >= total_catalog_source_count else end_offset
    else:
        selected_catalog_sources = catalog_sources
        start_offset = 0
        next_offset = 0
    discovered_rows: list[dict[str, Any]] = []
    per_catalog_counts: dict[str, int] = {}
    skipped_catalog_sources: list[dict[str, str]] = []
    max_runtime_seconds = 0.0 if settings is None else float(
        getattr(settings, "company_seed_catalog_max_runtime_seconds", 0.0) or 0.0
    )
    deadline = monotonic() + max_runtime_seconds if max_runtime_seconds > 0 else None
    processed_catalog_source_count = 0
    runtime_limited = False

    for source in selected_catalog_sources:
        name = normalize_whitespace(source.get("source_name")) or normalize_whitespace(source.get("catalog_name"))
        rows: list[dict[str, Any]] = []
        source_key = _seed_source_dedupe_key(source)
        source_had_timeout = False
        try:
            rows.extend(_discover_sources_from_html_catalog(source, paths, settings))
        except Exception as exc:  # noqa: BLE001
            reason = f"{exc.__class__.__name__}: {exc}"
            skipped_catalog_sources.append(
                {
                    "source_name": name,
                    "stage": "html_catalog",
                    "error": reason,
                }
            )
            source_had_timeout = source_had_timeout or _timeout_like_reason(reason)
        try:
            rows.extend(_discover_top_level_catalog_sources_from_sitemap(source, settings))
        except Exception as exc:  # noqa: BLE001
            reason = f"{exc.__class__.__name__}: {exc}"
            skipped_catalog_sources.append(
                {
                    "source_name": name,
                    "stage": "sitemap",
                    "error": reason,
                }
            )
            source_had_timeout = source_had_timeout or _timeout_like_reason(reason)
        rows = [row for row in rows if _seed_source_dedupe_key(row) not in invalid_keys]
        discovered_rows.extend(rows)
        per_catalog_counts[name] = int(len(rows))
        if source_had_timeout:
            skip_cache[source_key] = {
                "source_name": name,
                "skipped_at": datetime.now(timezone.utc).isoformat(),
            }
            refresh_cache.pop(source_key, None)
        else:
            skip_cache.pop(source_key, None)
            refresh_cache[source_key] = {
                "source_name": name,
                "refreshed_at": datetime.now(timezone.utc).isoformat(),
            }
        processed_catalog_source_count += 1
        if deadline is not None and monotonic() >= deadline:
            runtime_limited = True
            absolute_next_offset = start_offset + processed_catalog_source_count
            next_offset = 0 if absolute_next_offset >= total_catalog_source_count else absolute_next_offset
            break

    discovered = _prepare_discovered_seed_source_frame(
        pd.DataFrame([*persisted_catalog_sources, *discovered_rows]),
        existing,
        now_iso=current_iso,
    )
    frames = [frame for frame in (existing, discovered) if not frame.empty]
    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=list(COMPANY_SEED_SOURCE_COLUMNS))
    combined = _ensure_seed_source_columns(combined)
    if not combined.empty:
        dedupe_key = combined.apply(_seed_source_dedupe_key, axis=1)
        combined = combined.loc[~dedupe_key.duplicated(keep="last")].reset_index(drop=True)
    write_csv(combined, target_path)
    _write_catalog_skip_cache(paths, skip_cache)
    _write_catalog_refresh_cache(paths, refresh_cache)
    _write_catalog_progress(
        paths,
        next_offset=next_offset,
        total_catalog_source_count=total_catalog_source_count,
    )
    summary = {
        "target": target,
        **search_summary,
        "catalog_source_count": int(processed_catalog_source_count),
        "total_catalog_source_count": total_catalog_source_count,
        "catalog_source_start_offset": int(start_offset),
        "catalog_source_next_offset": int(next_offset),
        "catalog_source_runtime_limited": runtime_limited,
        "catalog_source_runtime_budget_seconds": float(max_runtime_seconds),
        "catalog_host_seed_source_count": int(len(auto_host_sources)),
        "cached_skip_catalog_source_count": int(cached_skip_count),
        "cached_refresh_catalog_source_count": int(cached_refresh_count),
        "discovered_seed_source_count": int(len(combined)),
        "newly_discovered_seed_source_count": int(len(discovered)),
        "catalog_discovery_counts": per_catalog_counts,
        "previous_discovered_seed_source_count": int(len(existing)),
        "skipped_catalog_source_count": int(len(skipped_catalog_sources)),
        "skipped_catalog_sources": skipped_catalog_sources,
        "pruned_shadow_seed_source_count": int(pruned_shadow_count),
        "pruned_invalid_seed_source_count": int(pruned_invalid_count),
    }
    return combined, summary


def promote_shadow_company_seed_sources(paths) -> tuple[pd.DataFrame, dict]:
    approved = _stamp_seed_source_frame(read_csv_or_empty(paths.discovered_company_seed_sources_path, COMPANY_SEED_SOURCE_COLUMNS))
    shadow = _stamp_seed_source_frame(read_csv_or_empty(paths.shadow_company_seed_sources_path, COMPANY_SEED_SOURCE_COLUMNS))
    before_count = int(len(approved))
    shadow_count = int(len(shadow))
    frames = [frame for frame in (approved, shadow) if not frame.empty]
    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=list(COMPANY_SEED_SOURCE_COLUMNS))
    combined = _ensure_seed_source_columns(combined)
    if not combined.empty:
        dedupe_key = combined.apply(_seed_source_dedupe_key, axis=1)
        combined = combined.loc[~dedupe_key.duplicated(keep="last")].reset_index(drop=True)
    write_csv(combined, paths.discovered_company_seed_sources_path)
    write_csv(pd.DataFrame(columns=list(COMPANY_SEED_SOURCE_COLUMNS)), paths.shadow_company_seed_sources_path)
    summary = {
        "approved_seed_source_count_before": before_count,
        "shadow_seed_source_count": shadow_count,
        "approved_seed_source_count_after": int(len(combined)),
        "promoted_shadow_seed_source_count": max(int(len(combined)) - before_count, 0),
    }
    return combined, summary


def auto_promote_shadow_company_seed_sources(paths, settings) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    current_iso = _seed_source_now_iso()
    approved = _stamp_seed_source_frame(
        read_csv_or_empty(paths.discovered_company_seed_sources_path, COMPANY_SEED_SOURCE_COLUMNS),
        now_iso=current_iso,
    )
    shadow = _stamp_seed_source_frame(
        read_csv_or_empty(paths.shadow_company_seed_sources_path, COMPANY_SEED_SOURCE_COLUMNS),
        now_iso=current_iso,
    )
    shadow, pruned_shadow_count = _prune_shadow_seed_sources(shadow, settings)
    effective_batch_size = int(getattr(settings, "company_seed_shadow_batch_size", 200) or 200) if settings is not None else 200
    effective_max_batches = int(getattr(settings, "company_seed_shadow_max_batches_per_run", 2) or 2) if settings is not None else 2
    process_limit = max(effective_batch_size, 0) * max(effective_max_batches, 0)
    shadow_to_process = shadow if process_limit <= 0 else shadow.head(process_limit).reset_index(drop=True)
    shadow_unprocessed = shadow.iloc[len(shadow_to_process) :].reset_index(drop=True) if process_limit > 0 else pd.DataFrame(columns=list(COMPANY_SEED_SOURCE_COLUMNS))
    shadow_records = shadow_to_process.fillna("").to_dict(orient="records")
    invalid_cache = load_invalid_company_seed_sources(paths)
    invalid_cache, pruned_invalid_count = _prune_invalid_seed_sources(invalid_cache, settings)
    approved_count_before = int(len(approved))
    shadow_count_before = int(len(shadow))
    approved_keys = (
        set(approved.apply(_seed_source_dedupe_key, axis=1).tolist())
        if not approved.empty
        else set()
    )
    invalid_keys = (
        set(invalid_cache.apply(lambda row: _seed_source_dedupe_key(row.to_dict()), axis=1).tolist())
        if not invalid_cache.empty
        else set()
    )
    promoted_rows: list[dict[str, Any]] = []
    remaining_rows: list[dict[str, Any]] = []
    deferred_rows: list[dict[str, Any]] = []
    runtime_unprocessed_rows: list[dict[str, Any]] = []
    duplicate_count = 0
    invalid_sources: list[dict[str, str]] = []
    invalid_rows: list[dict[str, Any]] = []
    processed_count = 0
    max_runtime_seconds = 0.0 if settings is None else float(
        getattr(settings, "company_seed_shadow_max_runtime_seconds", 0.0) or 0.0
    )
    deadline = monotonic() + max_runtime_seconds if max_runtime_seconds > 0 else None
    runtime_limited = False

    for index, source in enumerate(shadow_records):
        if deadline is not None and monotonic() >= deadline:
            runtime_limited = True
            runtime_unprocessed_rows.extend(shadow_records[index:])
            break
        dedupe_key = _seed_source_dedupe_key(source)
        if dedupe_key in approved_keys:
            duplicate_count += 1
            processed_count += 1
            continue
        if dedupe_key in invalid_keys:
            duplicate_count += 1
            processed_count += 1
            continue
        name = normalize_whitespace(source.get("source_name")) or normalize_whitespace(source.get("source_title"))
        try:
            is_promotable, reason = _evaluate_shadow_seed_source(source, paths, settings)
        except Exception as exc:  # noqa: BLE001
            reason = f"{exc.__class__.__name__}: {exc}"
            if any(token in reason for token in ("ConnectTimeout", "ReadTimeout", "RemoteProtocolError", "timed out")):
                deferred_rows.append(source)
            invalid_sources.append(
                {
                    "source_name": name,
                    "source_type": normalize_whitespace(source.get("source_type")),
                    "reason": reason,
                }
            )
            if dedupe_key not in invalid_keys and not any(
                token in reason for token in ("ConnectTimeout", "ReadTimeout", "RemoteProtocolError", "timed out")
            ):
                invalid_rows.append({**source, "invalid_reason": reason, "invalidated_at": current_iso})
                invalid_keys.add(dedupe_key)
            processed_count += 1
            continue
        if is_promotable:
            promoted_rows.append(source)
            approved_keys.add(dedupe_key)
            processed_count += 1
            continue
        if any(token in reason for token in ("ConnectTimeout", "ReadTimeout", "RemoteProtocolError", "timed out")):
            deferred_rows.append(source)
        invalid_sources.append(
            {
                "source_name": name,
                "source_type": normalize_whitespace(source.get("source_type")),
                "reason": reason,
            }
        )
        if dedupe_key not in invalid_keys:
            invalid_rows.append({**source, "invalid_reason": reason, "invalidated_at": current_iso})
            invalid_keys.add(dedupe_key)
        processed_count += 1

    frames = [frame for frame in (approved, pd.DataFrame(promoted_rows)) if not frame.empty]
    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=list(COMPANY_SEED_SOURCE_COLUMNS))
    combined = _ensure_seed_source_columns(combined)
    if not combined.empty:
        dedupe_key = combined.apply(_seed_source_dedupe_key, axis=1)
        combined = combined.loc[~dedupe_key.duplicated(keep="last")].reset_index(drop=True)

    remaining_shadow = _stamp_seed_source_frame(
        pd.concat(
            [
                frame
                for frame in (
                    shadow_unprocessed,
                    pd.DataFrame(runtime_unprocessed_rows, columns=list(COMPANY_SEED_SOURCE_COLUMNS)),
                    pd.DataFrame(deferred_rows, columns=list(COMPANY_SEED_SOURCE_COLUMNS)),
                    pd.DataFrame(remaining_rows, columns=list(COMPANY_SEED_SOURCE_COLUMNS)),
                )
                if not frame.empty
            ],
            ignore_index=True,
        )
        if (not shadow_unprocessed.empty or runtime_unprocessed_rows or deferred_rows or remaining_rows)
        else pd.DataFrame(columns=list(COMPANY_SEED_SOURCE_COLUMNS)),
        now_iso=current_iso,
    )
    invalid_frames = [frame for frame in (invalid_cache, pd.DataFrame(invalid_rows)) if not frame.empty]
    invalid_combined = (
        pd.concat(invalid_frames, ignore_index=True)
        if invalid_frames
        else pd.DataFrame(columns=list(INVALID_COMPANY_SEED_SOURCE_COLUMNS))
    )
    for column in INVALID_COMPANY_SEED_SOURCE_COLUMNS:
        if column not in invalid_combined.columns:
            invalid_combined[column] = ""
    invalid_combined = invalid_combined[list(INVALID_COMPANY_SEED_SOURCE_COLUMNS)]
    if not invalid_combined.empty:
        dedupe_key = invalid_combined.apply(lambda row: _seed_source_dedupe_key(row.to_dict()), axis=1)
        invalid_combined = invalid_combined.loc[~dedupe_key.duplicated(keep="last")].reset_index(drop=True)
    write_csv(combined, paths.discovered_company_seed_sources_path)
    write_csv(remaining_shadow, paths.shadow_company_seed_sources_path)
    write_csv(invalid_combined, paths.invalid_company_seed_sources_path)
    summary = {
        "approved_seed_source_count_before": approved_count_before,
        "shadow_seed_source_count_before": shadow_count_before,
        "auto_promoted_shadow_seed_source_count": int(len(promoted_rows)),
        "duplicate_shadow_seed_source_count": duplicate_count,
        "remaining_shadow_seed_source_count": int(len(remaining_shadow)),
        "approved_seed_source_count_after": int(len(combined)),
        "invalid_seed_source_cache_count": int(len(invalid_combined)),
        "invalid_shadow_seed_source_count": int(len(invalid_sources)),
        "invalid_shadow_seed_sources": invalid_sources,
        "pruned_shadow_seed_source_count": int(pruned_shadow_count),
        "pruned_invalid_seed_source_count": int(pruned_invalid_count),
        "shadow_batch_size": int(effective_batch_size),
        "shadow_max_batches_per_run": int(effective_max_batches),
        "processed_shadow_seed_source_count": int(processed_count),
        "deferred_shadow_seed_source_count": int(len(deferred_rows)),
        "shadow_runtime_limited": runtime_limited,
        "shadow_runtime_budget_seconds": float(max_runtime_seconds),
        "runtime_unprocessed_shadow_seed_source_count": int(len(runtime_unprocessed_rows)),
    }
    return combined, remaining_shadow, summary


def compact_company_seed_source_caches(paths, settings) -> dict[str, int]:
    shadow = _stamp_seed_source_frame(read_csv_or_empty(paths.shadow_company_seed_sources_path, COMPANY_SEED_SOURCE_COLUMNS))
    invalid = load_invalid_company_seed_sources(paths)
    pruned_shadow, pruned_shadow_count = _prune_shadow_seed_sources(shadow, settings)
    pruned_invalid, pruned_invalid_count = _prune_invalid_seed_sources(invalid, settings)
    write_csv(pruned_shadow, paths.shadow_company_seed_sources_path)
    write_csv(pruned_invalid, paths.invalid_company_seed_sources_path)
    return {
        "shadow_seed_source_count_before": int(len(shadow)),
        "shadow_seed_source_count_after": int(len(pruned_shadow)),
        "pruned_shadow_seed_source_count": int(pruned_shadow_count),
        "invalid_seed_source_count_before": int(len(invalid)),
        "invalid_seed_source_count_after": int(len(pruned_invalid)),
        "pruned_invalid_seed_source_count": int(pruned_invalid_count),
    }


def refresh_company_seed_sources(paths, settings) -> dict[str, Any]:
    compaction_summary = compact_company_seed_source_caches(paths, settings)
    _, discovery_summary = discover_company_seed_sources(paths, settings, target="shadow")
    approved_sources, remaining_shadow, auto_promotion_summary = auto_promote_shadow_company_seed_sources(paths, settings)
    summary: dict[str, Any] = {
        "cache_compaction": compaction_summary,
        **discovery_summary,
        **auto_promotion_summary,
        "approved_seed_source_count": int(len(approved_sources)),
        "shadow_seed_source_count": int(len(remaining_shadow)),
    }
    return summary


def collect_company_seed_records(paths, settings) -> tuple[pd.DataFrame, dict]:
    compaction_summary = compact_company_seed_source_caches(paths, settings)
    total_discovered_seed_source_count = 0
    catalog_source_count = 0
    newly_discovered_seed_source_count = 0
    auto_promoted_shadow_seed_source_count = 0
    duplicate_shadow_seed_source_count = 0
    invalid_shadow_seed_source_count = 0
    invalid_shadow_seed_sources: list[dict[str, str]] = []
    remaining_shadow = pd.DataFrame(columns=list(COMPANY_SEED_SOURCE_COLUMNS))
    max_passes = int(getattr(settings, "company_seed_catalog_max_passes", 2) or 2)

    for _ in range(max_passes):
        discovered_sources, discovery_summary = discover_company_seed_sources(paths, settings, target="shadow")
        _, remaining_shadow, auto_promotion_summary = auto_promote_shadow_company_seed_sources(paths, settings)
        total_discovered_seed_source_count = int(discovery_summary.get("discovered_seed_source_count", 0))
        catalog_source_count = int(discovery_summary.get("catalog_source_count", 0))
        newly_discovered_seed_source_count += int(discovery_summary.get("newly_discovered_seed_source_count", 0))
        auto_promoted_shadow_seed_source_count += int(auto_promotion_summary.get("auto_promoted_shadow_seed_source_count", 0))
        duplicate_shadow_seed_source_count += int(auto_promotion_summary.get("duplicate_shadow_seed_source_count", 0))
        invalid_shadow_seed_source_count += int(auto_promotion_summary.get("invalid_shadow_seed_source_count", 0))
        invalid_shadow_seed_sources.extend(auto_promotion_summary.get("invalid_shadow_seed_sources", []))
        if (
            int(discovery_summary.get("newly_discovered_seed_source_count", 0)) == 0
            and int(auto_promotion_summary.get("auto_promoted_shadow_seed_source_count", 0)) == 0
        ):
            break
    source_frame = load_company_seed_sources(paths)
    if not source_frame.empty:
        source_frame = source_frame[
            ~source_frame["source_type"].fillna("").isin(CATALOG_SOURCE_TYPES)
        ].reset_index(drop=True)
    collected_frames = []
    per_source_counts: dict[str, int] = {}
    per_source_raw_counts: dict[str, int] = {}
    skipped_sources: list[dict[str, str]] = []

    for source in source_frame.fillna("").to_dict(orient="records"):
        name = normalize_whitespace(source.get("source_name"))
        if not name:
            continue
        try:
            raw_frame = _load_source_frame(source, paths, settings)
        except Exception as exc:  # noqa: BLE001
            skipped_sources.append(
                {
                    "source_name": name,
                    "source_type": normalize_whitespace(source.get("source_type")),
                    "error": f"{exc.__class__.__name__}: {exc}",
                }
            )
            continue
        filtered_frame, filter_summary = _apply_source_filters(raw_frame, source)
        records = _frame_to_company_records(filtered_frame, source)
        frame = pd.DataFrame(records, columns=list(IMPORT_COMPANY_COLUMNS))
        collected_frames.append(frame)
        per_source_counts[name] = int(len(frame))
        per_source_raw_counts[name] = int(filter_summary["raw_count"])

    if not collected_frames:
        empty = pd.DataFrame(columns=list(IMPORT_COMPANY_COLUMNS))
        return empty, {
            "seed_source_count": int(len(source_frame)),
            "collected_seed_record_count": 0,
            "collected_company_count": 0,
            "seed_source_mode": "configured_sources",
            "catalog_source_count": catalog_source_count,
            "discovered_seed_source_count": total_discovered_seed_source_count,
            "newly_discovered_seed_source_count": newly_discovered_seed_source_count,
            "shadow_seed_source_count": int(len(remaining_shadow)),
            "approved_seed_source_count": int(len(source_frame)),
            "auto_promoted_shadow_seed_source_count": auto_promoted_shadow_seed_source_count,
            "duplicate_shadow_seed_source_count": duplicate_shadow_seed_source_count,
            "remaining_shadow_seed_source_count": int(len(remaining_shadow)),
            "invalid_shadow_seed_source_count": invalid_shadow_seed_source_count,
            "invalid_shadow_seed_sources": invalid_shadow_seed_sources,
            "source_record_counts": {},
            "source_raw_record_counts": {},
            "skipped_seed_source_count": int(len(skipped_sources)),
            "skipped_seed_sources": skipped_sources,
            "cache_compaction": compaction_summary,
        }

    combined = pd.concat(collected_frames, ignore_index=True)
    combined = _collapse_company_seed_records(combined)

    summary = {
        "seed_source_count": int(len(source_frame)),
        "collected_seed_record_count": int(sum(per_source_counts.values())),
        "collected_company_count": int(len(combined)),
        "seed_source_mode": "configured_sources",
        "catalog_source_count": catalog_source_count,
        "discovered_seed_source_count": total_discovered_seed_source_count,
        "newly_discovered_seed_source_count": newly_discovered_seed_source_count,
        "shadow_seed_source_count": int(len(remaining_shadow)),
        "approved_seed_source_count": int(len(source_frame)),
        "auto_promoted_shadow_seed_source_count": auto_promoted_shadow_seed_source_count,
        "duplicate_shadow_seed_source_count": duplicate_shadow_seed_source_count,
        "remaining_shadow_seed_source_count": int(len(remaining_shadow)),
        "invalid_shadow_seed_source_count": invalid_shadow_seed_source_count,
        "invalid_shadow_seed_sources": invalid_shadow_seed_sources,
        "source_record_counts": per_source_counts,
        "source_raw_record_counts": per_source_raw_counts,
        "skipped_seed_source_count": int(len(skipped_sources)),
        "skipped_seed_sources": skipped_sources,
        "tier_counts": combined["company_tier"].value_counts().to_dict() if not combined.empty else {},
        "cache_compaction": compaction_summary,
    }
    return combined, summary


def load_all_company_seed_records(paths) -> pd.DataFrame:
    frames = [
        read_csv_or_empty(paths.company_seed_records_path, IMPORT_COMPANY_COLUMNS),
        read_csv_or_empty(paths.collected_company_seed_records_path, IMPORT_COMPANY_COLUMNS),
    ]
    combined = (
        pd.concat([frame for frame in frames if not frame.empty], ignore_index=True)
        if any(not frame.empty for frame in frames)
        else pd.DataFrame(columns=list(IMPORT_COMPANY_COLUMNS))
    )
    if combined.empty:
        return combined
    return _collapse_company_seed_records(combined)
