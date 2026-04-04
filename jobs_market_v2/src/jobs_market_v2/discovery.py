"""Company and source discovery pipeline."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import re
from pathlib import Path
from urllib.parse import urljoin, urlparse

import httpx
import pandas as pd
from bs4 import BeautifulSoup

from .company_seed_sources import load_all_company_seed_records
from .constants import (
    ASSET_TOKENS,
    BLOCKED_LISTING_TOKENS,
    COMMON_KOREAN_SURNAMES,
    COMPANY_TIERS,
    IMPORT_COMPANY_COLUMNS,
    IMPORT_SOURCE_COLUMNS,
)
from .models import CompanyInput, SourceInput
from .network import build_timeout
from .storage import append_deduplicated, load_tabular_input, read_csv_or_empty, write_csv
from .utils import extract_domain, is_person_like, load_yaml, normalize_whitespace, parse_aliases, strip_protocol


HIRING_LINK_HINTS = (
    "career",
    "careers",
    "job",
    "jobs",
    "position",
    "positions",
    "recruit",
    "recruitment",
    "hiring",
    "opening",
    "openings",
    "apply",
    "join",
    "talent",
    "work with us",
    "join us",
    "채용",
    "채용중",
    "인재",
    "인재채용",
    "채용공고",
    "공개채용",
    "공고",
    "커리어",
    "모집",
)

HIRING_LINK_BLOCK_HINTS = (
    "notice",
    "notices",
    "announcement",
    "announcements",
    "news",
    "press",
    "media",
    "blog",
    "board",
    "invest",
    "investor",
    "ir",
    "esg",
    "sustainability",
    "공시",
    "공지",
    "공지사항",
    "뉴스",
    "보도",
    "홍보",
    "미디어",
    "블로그",
    "투자정보",
    "전자공고",
)

HIRING_URL_HINTS = (
    "career",
    "careers",
    "hr",
    "rcrt",
    "recruit",
    "recruitment",
    "hiring",
    "opening",
    "openings",
    "jobs",
    "position",
    "positions",
    "apply",
    "join",
    "employment",
    "employment-announcement",
    "employ",
    "talent",
    "채용",
    "인재채용",
    "채용공고",
    "공개채용",
    "커리어",
    "모집",
)

HIRING_PATH_BLOCK_HINTS = (
    "about",
    "culture",
    "personnel",
    "training",
    "evaluation",
    "reward",
    "benefit",
    "benefits",
    "welfare",
    "faq",
    "guide",
    "procedure",
    "process",
    "ideal",
    "leadership",
    "life",
    "introduction",
    "introduce",
    "consult",
    "partner",
    "social",
    "library",
    "model",
    "philosophy",
    "develop",
    "facility",
    "membership",
    "copy-of",
    "cnts",
    "login",
    "signup",
    "privacy",
    "policy",
    "tos",
    "email",
    "pwd",
    "id",
    "user",
    "mem",
    "myform",
    "insight",
    "story",
    "wellness",
    "workspace",
    "people_detail",
    "people",
    "value",
    "subsid",
    "main.do",
    "index.jsp",
    "job-request",
    "request-center",
    "stop-job",
)

HIRING_PRIORITY_HINTS = (
    "position",
    "positions open",
    "public recruitment",
    "employment announcement",
    "employment-announcement",
    "rcrt",
    "채용공고",
    "공개채용",
    "채용안내",
    "모집공고",
)

KNOWN_ATS_DOMAIN_TYPES = {
    "greenhouse.io": "greenhouse",
    "lever.co": "lever",
    "greetinghr.com": "greetinghr",
    "recruiter.co.kr": "recruiter",
    "jobs.workable.com": "workable",
    "smartrecruiters.com": "smartrecruiters",
    "jobvite.com": "jobvite",
    "ashbyhq.com": "ashby",
    "bamboohr.com": "bamboohr",
    "teamtailor.com": "teamtailor",
}


def _normalize_company_name(value: str) -> str:
    text = normalize_whitespace(value)
    text = re.sub(r"\[[^\]]+\]", "", text)
    text = re.sub(r"\([^\)]*(채용|공고|목록|이미지)[^\)]*\)", "", text)
    return normalize_whitespace(text)


def _coalesce_text(*values: object) -> str:
    for value in values:
        if value is None:
            continue
        if pd.isna(value):
            continue
        text = normalize_whitespace(str(value))
        if text and text.lower() != "nan":
            return text
    return ""


def _is_same_company_domain(source_domain: str, official_domain: str) -> bool:
    if not source_domain or not official_domain:
        return False
    return source_domain == official_domain or source_domain.endswith(f".{official_domain}")


def _source_type_from_url(source_url: str) -> str:
    domain = strip_protocol(extract_domain(source_url))
    for known_domain, source_type in KNOWN_ATS_DOMAIN_TYPES.items():
        if domain == known_domain or domain.endswith(f".{known_domain}"):
            return source_type
    return "html_page"


def _looks_like_hiring_link(text: str, source_url: str) -> bool:
    haystack = normalize_whitespace(f"{text} {source_url}").lower()
    return any(hint in haystack for hint in HIRING_LINK_HINTS)


def _has_explicit_hiring_signal(text: str, source_url: str) -> bool:
    haystack = normalize_whitespace(f"{text} {source_url}").lower()
    return any(hint in haystack for hint in HIRING_PRIORITY_HINTS)


def _looks_like_blocked_non_hiring_link(text: str, source_url: str) -> bool:
    if _has_explicit_hiring_signal(text, source_url):
        return False
    haystack = normalize_whitespace(f"{text} {source_url}").lower()
    return any(token in haystack for token in HIRING_LINK_BLOCK_HINTS)


def _looks_like_direct_hiring_domain(domain: str) -> bool:
    haystack = normalize_whitespace(domain.replace(".", " ")).lower()
    return any(hint in haystack for hint in HIRING_URL_HINTS)


def _canonicalize_source_url(source_url: str, *, drop_query: bool = False) -> str:
    parsed = urlparse(source_url)
    path = parsed.path.rstrip("/")
    if path.endswith("/index"):
        path = path[: -len("/index")]
    query = "" if drop_query else parsed.query
    return parsed._replace(path=path, query=query, fragment="").geturl().rstrip("/")


def _source_candidate_priority(source_row: dict) -> int:
    source_url = normalize_whitespace(source_row.get("source_url"))
    discovery_method = normalize_whitespace(source_row.get("discovery_method"))
    parsed = urlparse(source_url)
    haystack = normalize_whitespace(" ".join(part for part in (parsed.netloc, parsed.path, parsed.query, parsed.fragment) if part)).lower()
    score = 0
    if discovery_method == "official_domain_probe":
        score += 40
    if any(hint in haystack for hint in HIRING_PRIORITY_HINTS):
        score += 60
    if any(hint in haystack for hint in HIRING_URL_HINTS):
        score += 25
    if any(token in haystack for token in ("list", "view", "detail", "posting", "article", "annoid", "articleno")):
        score += 15
    if any(token in haystack for token in HIRING_PATH_BLOCK_HINTS):
        score -= 40
    return score


def _trim_company_source_rows(rows: list[dict], official_domain: str) -> list[dict]:
    if not rows:
        return rows

    limit = 2 if _looks_like_direct_hiring_domain(official_domain) else 3
    kept_rows: list[dict] = []
    same_domain_html_rows: list[dict] = []
    seen_urls: set[str] = set()

    for row in rows:
        source_url = row.get("source_url", "")
        if not source_url or source_url in seen_urls:
            continue
        seen_urls.add(source_url)
        source_domain = strip_protocol(extract_domain(source_url))
        if (
            row.get("source_type") == "html_page"
            and row.get("discovery_method") == "homepage_link_probe"
            and _is_same_company_domain(source_domain, official_domain)
        ):
            same_domain_html_rows.append(row)
            continue
        kept_rows.append(row)

    ranked_same_domain_rows = sorted(
        same_domain_html_rows,
        key=lambda row: (_source_candidate_priority(row), row.get("source_url", "")),
        reverse=True,
    )
    kept_rows.extend(row for row in ranked_same_domain_rows[:limit] if _source_candidate_priority(row) > 0)
    return kept_rows


def _is_viable_same_domain_hiring_link(text: str, source_url: str, official_domain: str) -> bool:
    if _looks_like_blocked_non_hiring_link(text, source_url):
        return False

    parsed = urlparse(source_url)
    source_domain = strip_protocol(parsed.netloc)
    host_haystack = source_domain.replace(".", " ").lower()
    path_haystack = normalize_whitespace(" ".join(part for part in (parsed.path, parsed.query, parsed.fragment) if part)).lower()
    link_haystack = normalize_whitespace(text).lower()
    has_strong_url_hint = any(hint in path_haystack for hint in HIRING_URL_HINTS)
    has_explicit_hiring_signal = _has_explicit_hiring_signal(link_haystack, source_url)
    has_hiring_text_hint = any(hint in link_haystack for hint in HIRING_LINK_HINTS)
    has_path_block_hint = any(token in path_haystack for token in HIRING_PATH_BLOCK_HINTS)

    is_exact_homepage_root = source_domain == official_domain and not parsed.path.strip("/") and not parsed.query and not parsed.fragment
    if is_exact_homepage_root:
        return False
    if has_path_block_hint and not has_explicit_hiring_signal:
        return False

    return has_strong_url_hint or has_explicit_hiring_signal or has_hiring_text_hint


def _fetch_company_homepage(url: str, settings) -> tuple[str, str]:
    response = httpx.get(
        url,
        headers={"User-Agent": getattr(settings, "user_agent", "jobs-market-v2/0.1")},
        timeout=build_timeout(
            min(float(getattr(settings, "timeout_seconds", 20.0)), 8.0),
            min(float(getattr(settings, "connect_timeout_seconds", 5.0)), 4.0),
        ),
        follow_redirects=True,
    )
    response.raise_for_status()
    return response.text, str(response.url)


def _extract_homepage_source_candidates(company_row: dict, html: str, base_url: str) -> list[dict]:
    official_domain = strip_protocol(company_row.get("official_domain"))
    if not official_domain:
        return []
    soup = BeautifulSoup(html, "lxml")
    rows: list[dict] = []
    seen_urls: set[str] = set()

    for anchor in soup.find_all("a", href=True):
        href = normalize_whitespace(anchor.get("href"))
        if not href or href.startswith(("mailto:", "tel:", "javascript:", "#")):
            continue
        raw_absolute_url = urljoin(base_url, href)
        if not raw_absolute_url.startswith(("http://", "https://")):
            continue

        source_domain = strip_protocol(extract_domain(raw_absolute_url))
        source_type = _source_type_from_url(raw_absolute_url)
        is_same_company_link = _is_same_company_domain(source_domain, official_domain)
        is_known_ats_link = source_type != "html_page"
        absolute_url = _canonicalize_source_url(
            raw_absolute_url,
            drop_query=is_same_company_link and not is_known_ats_link,
        )

        if not is_same_company_link and not is_known_ats_link:
            continue

        link_text = normalize_whitespace(
            " ".join(
                part
                for part in (
                    anchor.get_text(" ", strip=True),
                    anchor.get("title"),
                    anchor.get("aria-label"),
                )
                if normalize_whitespace(part)
            )
        )
        if is_known_ats_link:
            if not _looks_like_hiring_link(link_text, absolute_url):
                continue
        elif not _is_viable_same_domain_hiring_link(link_text, absolute_url, official_domain):
            continue

        canonical_url = absolute_url.rstrip("/")
        if canonical_url in seen_urls:
            continue
        seen_urls.add(canonical_url)

        label = "공개 ATS" if is_known_ats_link else "공식 채용"
        rows.append(
            {
                "company_name": company_row["company_name"],
                "company_tier": company_row.get("company_tier", ""),
                "source_name": f'{company_row["company_name"]} {label}',
                "source_url": canonical_url,
                "source_type": source_type,
                "official_domain": official_domain,
                "is_official_hint": True,
                "structure_hint": "ats" if is_known_ats_link else "html",
                "discovery_method": "homepage_link_probe",
            }
        )
    return rows


def _probe_company_source_candidates(company_row: dict, settings) -> list[dict]:
    official_domain = strip_protocol(company_row.get("official_domain"))
    if not official_domain:
        return []
    rows: list[dict] = []
    homepage_url = _canonicalize_source_url(f"https://{official_domain}")
    homepage_source_type = _source_type_from_url(homepage_url)
    if homepage_source_type != "html_page" or _looks_like_direct_hiring_domain(official_domain):
        label = "공개 ATS" if homepage_source_type != "html_page" else "공식 채용"
        rows.append(
            {
                "company_name": company_row["company_name"],
                "company_tier": company_row.get("company_tier", ""),
                "source_name": f'{company_row["company_name"]} {label}',
                "source_url": homepage_url,
                "source_type": homepage_source_type,
                "official_domain": official_domain,
                "is_official_hint": True,
                "structure_hint": "ats" if homepage_source_type != "html_page" else "html",
                "discovery_method": "official_domain_probe",
            }
        )
    try:
        html, final_url = _fetch_company_homepage(f"https://{official_domain}", settings)
    except Exception:  # noqa: BLE001
        return rows

    seen_urls = {row["source_url"] for row in rows}
    for row in _extract_homepage_source_candidates(company_row, html, final_url):
        if row["source_url"] in seen_urls:
            continue
        seen_urls.add(row["source_url"])
        rows.append(row)
    return _trim_company_source_rows(rows, official_domain)


def _probe_source_candidates_from_homepages(companies: pd.DataFrame, companies_with_manual_sources: set[str], settings) -> list[dict]:
    probe_rows: list[dict] = []
    target_rows = [
        row
        for row in companies.fillna("").to_dict(orient="records")
        if strip_protocol(row.get("official_domain"))
        and _normalize_company_name(str(row.get("company_name") or "")).casefold() not in companies_with_manual_sources
    ]
    if not target_rows:
        return probe_rows

    max_workers = min(16, max(4, len(target_rows) // 20 or 4))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_probe_company_source_candidates, row, settings): row for row in target_rows}
        for future in as_completed(futures):
            try:
                probe_rows.extend(future.result())
            except Exception:  # noqa: BLE001
                continue
    return probe_rows


def _non_company_reason(name: str) -> str | None:
    lowered = name.lower()
    if not name:
        return "빈값"
    if any(token in lowered for token in ASSET_TOKENS):
        return "에셋토큰"
    if any(token in name for token in BLOCKED_LISTING_TOKENS):
        return "목록/문서제목"
    if re.search(r"\.(png|jpg|jpeg|svg|webp)$", lowered):
        return "이미지파일명"
    if is_person_like(name, COMMON_KOREAN_SURNAMES):
        return "사람이름추정"
    return None


def load_manual_companies(paths) -> pd.DataFrame:
    frames = []
    for candidate in (
        paths.config_dir / "manual_companies_seed.csv",
        paths.manual_companies_path,
    ):
        frame = read_csv_or_empty(candidate, IMPORT_COMPANY_COLUMNS)
        if not frame.empty:
            frames.append(frame)
    if not frames:
        return pd.DataFrame(columns=list(IMPORT_COMPANY_COLUMNS))
    combined = pd.concat(frames, ignore_index=True)
    for column in IMPORT_COMPANY_COLUMNS:
        if column not in combined.columns:
            combined[column] = None
    return combined[list(IMPORT_COMPANY_COLUMNS)].drop_duplicates(subset=["company_name"], keep="last")


def load_company_seed_records(paths) -> pd.DataFrame:
    frame = load_all_company_seed_records(paths)
    if frame.empty:
        return pd.DataFrame(columns=list(IMPORT_COMPANY_COLUMNS))
    return frame[list(IMPORT_COMPANY_COLUMNS)].drop_duplicates(subset=["company_name"], keep="last")


def load_manual_sources(paths) -> pd.DataFrame:
    frames = []
    for candidate in (
        paths.config_dir / "manual_sources_seed.yaml",
        paths.manual_sources_path,
    ):
        if candidate.exists():
            frame = load_tabular_input(candidate)
            if not frame.empty:
                frames.append(frame)
    if not frames:
        return pd.DataFrame(columns=list(IMPORT_SOURCE_COLUMNS))
    combined = pd.concat(frames, ignore_index=True)
    for column in IMPORT_SOURCE_COLUMNS:
        if column not in combined.columns:
            combined[column] = None
    return combined[list(IMPORT_SOURCE_COLUMNS)].drop_duplicates(subset=["source_url"], keep="last")


def _company_rows_from_manual_sources(paths) -> pd.DataFrame:
    manual_sources = load_manual_sources(paths)
    if manual_sources.empty:
        return pd.DataFrame(columns=list(IMPORT_COMPANY_COLUMNS))
    rows: list[dict] = []
    seen: set[str] = set()
    for row in manual_sources.fillna("").to_dict(orient="records"):
        company_name = normalize_whitespace(str(row.get("company_name") or ""))
        company_key = _normalize_company_name(company_name).casefold()
        if not company_name or company_key in seen:
            continue
        seen.add(company_key)
        rows.append(
            {
                "company_name": company_name,
                "company_tier": row.get("company_tier") or "",
                "official_domain": strip_protocol(row.get("official_domain") or ""),
                "company_name_en": "",
                "region": "",
                "aliases": [],
                "discovery_method": "manual_source_seed",
                "candidate_seed_type": "공식채용소스시드",
                "candidate_seed_url": row.get("source_url") or "",
                "candidate_seed_title": row.get("source_name") or "",
                "candidate_seed_reason": f"{row.get('source_type', '')} 기반 공식 공개 채용 소스 등록",
            }
        )
    return pd.DataFrame(rows, columns=list(IMPORT_COMPANY_COLUMNS))


def generate_company_candidates(paths) -> pd.DataFrame:
    frames = [
        load_company_seed_records(paths),
        _company_rows_from_manual_sources(paths),
    ]

    manual_companies = load_manual_companies(paths)
    if not manual_companies.empty:
        manual_companies = manual_companies.copy()
        seeded_mask = (
            manual_companies["candidate_seed_url"].fillna("").astype(str).str.strip().ne("")
            | manual_companies["candidate_seed_title"].fillna("").astype(str).str.strip().ne("")
            | manual_companies["candidate_seed_reason"].fillna("").astype(str).str.strip().ne("")
            | manual_companies["candidate_seed_type"].fillna("").astype(str).str.strip().ne("")
        )
        manual_seeded = manual_companies[seeded_mask].copy()
        if not manual_seeded.empty:
            manual_seeded["discovery_method"] = manual_seeded["discovery_method"].fillna("").replace("", "manual_seed")
            frames.append(manual_seeded[list(IMPORT_COMPANY_COLUMNS)])

    combined = pd.concat([frame for frame in frames if not frame.empty], ignore_index=True) if any(not frame.empty for frame in frames) else pd.DataFrame(columns=list(IMPORT_COMPANY_COLUMNS))
    if not combined.empty:
        return combined.drop_duplicates(subset=["company_name"], keep="last").reset_index(drop=True)

    seed = load_yaml(paths.config_dir / "seed_company_inputs.yaml")
    rows: list[dict] = []
    for tier, values in (seed.get("layers") or {}).items():
        for value in values or []:
            rows.append(
                {
                    "company_name": normalize_whitespace(str(value)),
                    "company_tier": tier,
                    "official_domain": None,
                    "company_name_en": None,
                    "region": None,
                    "aliases": [],
                    "discovery_method": "config_seed",
                    "candidate_seed_type": "",
                    "candidate_seed_url": "",
                    "candidate_seed_title": "",
                    "candidate_seed_reason": "",
                }
            )
    return pd.DataFrame(rows, columns=list(IMPORT_COMPANY_COLUMNS))


def clean_non_company_entities(candidates: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    cleaned_rows: list[dict] = []
    rejected_rows: list[dict] = []
    seen: set[str] = set()

    for row in candidates.to_dict(orient="records"):
        company_name = _normalize_company_name(str(row.get("company_name") or ""))
        reason = _non_company_reason(company_name)
        normalized_key = company_name.casefold()
        if reason:
            rejected_rows.append({**row, "company_name": company_name, "reject_reason": reason})
            continue
        if normalized_key in seen:
            rejected_rows.append({**row, "company_name": company_name, "reject_reason": "중복"})
            continue
        seen.add(normalized_key)
        cleaned_rows.append({**row, "company_name": company_name})

    return (
        pd.DataFrame(cleaned_rows, columns=list(IMPORT_COMPANY_COLUMNS)),
        pd.DataFrame(rejected_rows),
    )


def resolve_official_domains(companies: pd.DataFrame, paths) -> pd.DataFrame:
    manual = load_manual_companies(paths)
    manual_sources = load_manual_sources(paths)
    alias_map: dict[str, dict] = {}
    for row in manual.to_dict(orient="records"):
        aliases = parse_aliases(row.get("aliases"))
        canonical = _normalize_company_name(str(row.get("company_name") or ""))
        alias_map[canonical.casefold()] = row
        for alias in aliases:
            alias_map[_normalize_company_name(alias).casefold()] = row

    source_seed_map: dict[str, dict] = {}
    for row in manual_sources.to_dict(orient="records"):
        canonical = _normalize_company_name(str(row.get("company_name") or ""))
        if not canonical or canonical.casefold() in source_seed_map:
            continue
        source_seed_map[canonical.casefold()] = {
            "candidate_seed_type": "공식채용소스시드",
            "candidate_seed_url": row.get("source_url") or "",
            "candidate_seed_title": row.get("source_name") or "",
            "candidate_seed_reason": f"{row.get('source_type', '')} 기반 공식 공개 채용 소스 등록",
        }

    resolved_rows: list[dict] = []
    for row in companies.to_dict(orient="records"):
        key = _normalize_company_name(str(row.get("company_name") or "")).casefold()
        matched = alias_map.get(key, {})
        source_seed = source_seed_map.get(key, {})
        canonical_company_name = _coalesce_text(matched.get("company_name"), row.get("company_name"))
        official_domain = strip_protocol(matched.get("official_domain") or row.get("official_domain"))
        resolved_rows.append(
            {
                **row,
                "company_name": canonical_company_name,
                "official_domain": official_domain or "",
                "company_name_en": _coalesce_text(matched.get("company_name_en"), row.get("company_name_en")),
                "region": _coalesce_text(matched.get("region"), row.get("region")),
                "aliases": parse_aliases(matched.get("aliases") or row.get("aliases")),
                "candidate_seed_type": _coalesce_text(
                    row.get("candidate_seed_type"),
                    matched.get("candidate_seed_type"),
                    source_seed.get("candidate_seed_type", ""),
                ),
                "candidate_seed_url": _coalesce_text(
                    row.get("candidate_seed_url"),
                    matched.get("candidate_seed_url"),
                    source_seed.get("candidate_seed_url", ""),
                ),
                "candidate_seed_title": _coalesce_text(
                    row.get("candidate_seed_title"),
                    matched.get("candidate_seed_title"),
                    source_seed.get("candidate_seed_title", ""),
                ),
                "candidate_seed_reason": _coalesce_text(
                    row.get("candidate_seed_reason"),
                    matched.get("candidate_seed_reason"),
                    source_seed.get("candidate_seed_reason", ""),
                ),
                "official_domain_confidence": 0.99 if official_domain else 0.0,
            }
        )
    resolved = pd.DataFrame(resolved_rows)
    return resolved.drop_duplicates(subset=["company_name"], keep="first").reset_index(drop=True)


def discover_companies(paths) -> tuple[pd.DataFrame, dict]:
    candidates = generate_company_candidates(paths)
    cleaned, rejected = clean_non_company_entities(candidates)
    resolved = resolve_official_domains(cleaned, paths)
    write_csv(resolved, paths.companies_registry_path)
    summary = {
        "raw_candidate_count": int(len(candidates)),
        "discovered_company_count": int(len(resolved)),
        "non_company_removed_count": int(len(rejected)),
        "candidate_input_mode": "source_backed_seed_records" if candidates["candidate_seed_url"].fillna("").astype(str).str.strip().ne("").any() else "bootstrap_seed_inputs",
        "seeded_candidate_count": int(candidates["candidate_seed_url"].fillna("").astype(str).str.strip().ne("").sum()),
        "tier_counts": resolved["company_tier"].value_counts().to_dict() if not resolved.empty else {},
    }
    return resolved, summary


def _validate_company_frame(frame: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for record in frame.fillna("").to_dict(orient="records"):
        validated = CompanyInput(**record)
        rows.append(validated.model_dump())
    return pd.DataFrame(rows, columns=list(IMPORT_COMPANY_COLUMNS))


def import_companies(paths, input_path: Path) -> dict:
    frame = load_tabular_input(input_path)
    validated = _validate_company_frame(frame)
    existing = read_csv_or_empty(paths.manual_companies_path, IMPORT_COMPANY_COLUMNS)
    combined = append_deduplicated(existing, validated, ["company_name"])
    write_csv(combined, paths.manual_companies_path)
    return {"imported_company_count": int(len(validated)), "stored_company_count": int(len(combined))}


def _validate_source_frame(frame: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for record in frame.fillna("").to_dict(orient="records"):
        validated = SourceInput(**record)
        rows.append(validated.model_dump())
    frame = pd.DataFrame(rows, columns=list(IMPORT_SOURCE_COLUMNS))
    if frame.empty:
        return frame
    return frame.drop_duplicates(subset=["source_url"], keep="last").reset_index(drop=True)


def import_sources(paths, input_path: Path) -> dict:
    frame = load_tabular_input(input_path)
    validated = _validate_source_frame(frame)
    validated["official_domain"] = validated["official_domain"].fillna("").map(strip_protocol)
    validated["source_domain"] = validated["source_url"].map(extract_domain)
    existing = read_csv_or_empty(paths.manual_sources_path, IMPORT_SOURCE_COLUMNS)
    combined = append_deduplicated(existing, validated[list(IMPORT_SOURCE_COLUMNS)], ["source_url"])
    write_csv(combined, paths.manual_sources_path)
    return {"imported_source_count": int(len(validated)), "stored_source_count": int(len(combined))}


def discover_source_candidates(companies: pd.DataFrame, paths, settings=None) -> pd.DataFrame:
    manual_sources = load_manual_sources(paths)
    if companies.empty:
        companies = read_csv_or_empty(paths.companies_registry_path)

    company_lookup = {
        _normalize_company_name(str(row["company_name"])).casefold(): row
        for row in companies.fillna("").to_dict(orient="records")
    }

    rows: list[dict] = []
    companies_with_manual_sources: set[str] = set()
    for record in manual_sources.fillna("").to_dict(orient="records"):
        company_key = _normalize_company_name(str(record.get("company_name") or "")).casefold()
        if company_lookup and company_key not in company_lookup:
            continue
        company_row = company_lookup.get(company_key, {})
        companies_with_manual_sources.add(company_key)
        rows.append(
            {
                **record,
                "company_tier": record.get("company_tier") or company_row.get("company_tier") or "",
                "official_domain": strip_protocol(record.get("official_domain") or company_row.get("official_domain")),
                "discovery_method": record.get("discovery_method") or "manual_seed",
            }
        )

    if settings:
        rows.extend(_probe_source_candidates_from_homepages(companies, companies_with_manual_sources, settings))

    if settings and settings.enable_fallback_source_guess:
        existing_urls = {row["source_url"] for row in rows}
        for company in companies.fillna("").to_dict(orient="records"):
            company_key = _normalize_company_name(str(company.get("company_name") or "")).casefold()
            if company_key in companies_with_manual_sources:
                continue
            official_domain = strip_protocol(company.get("official_domain"))
            if not official_domain:
                continue
            fallback_url = f"https://{official_domain}/careers"
            if fallback_url in existing_urls:
                continue
            rows.append(
                {
                    "company_name": company["company_name"],
                    "company_tier": company["company_tier"],
                    "source_name": f'{company["company_name"]} 공식 채용',
                    "source_url": fallback_url,
                    "source_type": "official_careers",
                    "official_domain": official_domain,
                    "is_official_hint": True,
                    "structure_hint": "html",
                    "discovery_method": "fallback_generated",
                }
            )
    return pd.DataFrame(rows, columns=list(IMPORT_SOURCE_COLUMNS))
