"""Company and source discovery pipeline."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import json
import re
import time
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urljoin, urlparse

import httpx
import pandas as pd
from bs4 import BeautifulSoup

from .company_seed_sources import load_all_company_seed_records
from .constants import (
    ALLOWED_JOB_ROLES,
    ASSET_TOKENS,
    BLOCKED_LISTING_TOKENS,
    COMMON_KOREAN_SURNAMES,
    COMPANY_TIERS,
    IMPORT_COMPANY_COLUMNS,
    IMPORT_SOURCE_COLUMNS,
    WORK24_POPULATION_JOB_COLUMNS,
    WORK24_POPULATION_SCAN_LOG_COLUMNS,
    WORK24_POPULATION_SHADOW_COMPANY_COLUMNS,
)
from .gemini import GeminiBudget
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

POPULATION_DISCOVERY_METHODS = {
    "manual_seed_public_jobboard",
    "population_discovery_only",
    "work24_population_discovery",
}
WORK24_LIMITED_PUBLIC_BOARD_DISCOVERY_METHOD = "work24_limited_public_board_fallback"
WORK24_PUBLIC_SEARCH_BASE_URL = "https://www.work24.go.kr/wk/a/b/1200/retriveDtlEmpSrchList.do"
WORK24_REGIONAL_LOCATION_RE = re.compile(
    r"부산|대구|대전|광주|울산|세종|강원|충북|충남|충청|전북|전남|전라|경북|경남|경상|제주|청주|천안|아산|전주|포항|창원|김해|구미|춘천|원주|익산|군산|여수|순천"
)
WORK24_PUBLIC_RESEARCH_COMPANY_RE = re.compile(
    r"대학교|과학기술원|고등과학원|산학협력단|연구원|연구소|재단법인|공사|공단|진흥원|테크노파크"
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
    "oapi.saramin.co.kr": "saramin_api",
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


def load_work24_population_candidates(paths) -> pd.DataFrame:
    path = getattr(paths, "work24_population_candidates_path", None)
    if not path:
        return pd.DataFrame(columns=list(IMPORT_COMPANY_COLUMNS))
    frame = read_csv_or_empty(path, IMPORT_COMPANY_COLUMNS)
    if frame.empty:
        return pd.DataFrame(columns=list(IMPORT_COMPANY_COLUMNS))
    return frame[list(IMPORT_COMPANY_COLUMNS)].drop_duplicates(subset=["company_name"], keep="last")


def load_work24_population_jobs(paths) -> pd.DataFrame:
    path = getattr(paths, "work24_population_jobs_path", None)
    if not path:
        return pd.DataFrame(columns=list(WORK24_POPULATION_JOB_COLUMNS))
    frame = read_csv_or_empty(path, WORK24_POPULATION_JOB_COLUMNS)
    if frame.empty:
        return pd.DataFrame(columns=list(WORK24_POPULATION_JOB_COLUMNS))
    frame = _refresh_work24_population_job_frame(frame)
    return frame[list(WORK24_POPULATION_JOB_COLUMNS)].drop_duplicates(
        subset=["worknet_wanted_auth_no"],
        keep="last",
    )


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


def _manual_source_discovery_method(row: dict) -> str:
    return normalize_whitespace(str(row.get("discovery_method") or "")).casefold()


def _is_population_discovery_source(row: dict) -> bool:
    return _manual_source_discovery_method(row) in POPULATION_DISCOVERY_METHODS


def load_population_discovery_sources(paths) -> pd.DataFrame:
    manual_sources = load_manual_sources(paths)
    if manual_sources.empty:
        return pd.DataFrame(columns=list(IMPORT_SOURCE_COLUMNS))
    rows = [
        row
        for row in manual_sources.fillna("").to_dict(orient="records")
        if _is_population_discovery_source(row)
    ]
    return pd.DataFrame(rows, columns=list(IMPORT_SOURCE_COLUMNS))


def _company_rows_from_manual_sources(paths) -> pd.DataFrame:
    manual_sources = load_manual_sources(paths)
    if manual_sources.empty:
        return pd.DataFrame(columns=list(IMPORT_COMPANY_COLUMNS))
    rows: list[dict] = []
    seen: set[str] = set()
    for row in manual_sources.fillna("").to_dict(orient="records"):
        if _is_population_discovery_source(row):
            continue
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


def _work24_population_query_value(source_url: str, key: str) -> str:
    parts = urlparse(normalize_whitespace(source_url))
    values = parse_qs(parts.query, keep_blank_values=True).get(key)
    return normalize_whitespace(values[0] if values else "")


def _work24_population_query_int(source_url: str, key: str, default: int, *, minimum: int, maximum: int) -> int:
    value = _work24_population_query_value(source_url, key)
    try:
        parsed = int(value) if value else default
    except (TypeError, ValueError):
        parsed = default
    return max(min(parsed, maximum), minimum)


def _work24_population_role_hint(source_record: dict, job: dict) -> str:
    target_hint = normalize_whitespace(job.get("work24_llm_target_hint")).lower()
    if target_hint == "false":
        return ""
    suggested_role = normalize_whitespace(job.get("work24_llm_suggested_role"))
    from .collection import classify_job_role

    role = classify_job_role(
        normalize_whitespace(job.get("title")),
        normalize_whitespace(job.get("listing_context")),
        normalize_whitespace(job.get("main_tasks")),
        normalize_whitespace(job.get("requirements")),
    )
    if role in ALLOWED_JOB_ROLES:
        return role
    if suggested_role in ALLOWED_JOB_ROLES:
        return suggested_role
    return ""


def _track_work24_population_listings_with_llm(listings: list[dict], settings, paths=None) -> list[dict]:
    if not listings:
        return []
    from .collection import _track_work24_public_listings_with_llm

    max_calls = max(0, int(getattr(settings, "gemini_html_listing_max_calls_per_run", 0) or 0))
    budget = GeminiBudget(max_calls=max_calls)
    tracked: list[dict] = []
    chunk_size = 25
    for offset in range(0, len(listings), chunk_size):
        chunk = [listing.copy() for listing in listings[offset : offset + chunk_size]]
        if not budget.can_call():
            tracked.extend(chunk)
            continue
        tracked.extend(_track_work24_public_listings_with_llm(chunk, settings, paths=paths, budget=budget))
    return tracked


def _append_work24_population_scan_log(paths, rows: list[dict]) -> None:
    if paths is None or not rows:
        return
    path = getattr(paths, "work24_population_scan_log_path", None)
    if not path:
        return
    frame = pd.DataFrame(rows, columns=list(WORK24_POPULATION_SCAN_LOG_COLUMNS))
    existing = read_csv_or_empty(path, WORK24_POPULATION_SCAN_LOG_COLUMNS)
    combined = pd.concat([existing, frame], ignore_index=True) if not existing.empty else frame
    write_csv(combined[list(WORK24_POPULATION_SCAN_LOG_COLUMNS)], path)


def _fetch_work24_population_source_jobs(source_url: str, settings, paths=None, source_name: str = "") -> list[dict]:
    from .collection import (
        _WORK24_PUBLIC_SEARCH_BASE_URL,
        _WORK24_PUBLIC_SEARCH_POST_URL,
        _build_work24_public_search_form,
        _fetch_work24_public_html,
        _parse_work24_public_list_jobs,
        _work24_public_auth_no,
    )

    requested_scan_depth = _work24_population_query_int(
        source_url,
        "scanDepth",
        int(getattr(settings, "work24_population_max_pages_per_source", 200) or 200),
        minimum=1,
        maximum=1000,
    )
    max_pages_per_source = max(1, int(getattr(settings, "work24_population_max_pages_per_source", 200) or 200))
    max_pages = min(requested_scan_depth, max_pages_per_source)
    empty_page_stop_count = max(1, int(getattr(settings, "work24_population_empty_page_stop_count", 1) or 1))
    stale_page_stop_count = max(1, int(getattr(settings, "work24_population_stale_page_stop_count", 2) or 2))
    delay_seconds = max(0.0, float(getattr(settings, "work24_population_page_delay_seconds", 0.0) or 0.0))
    population_query = _work24_population_query_value(source_url, "srcKeyword") or _work24_population_query_value(source_url, "keyword")
    scanned_at = datetime.now(timezone.utc).isoformat()

    jobs: list[dict] = []
    seen_keys: set[str] = set()
    scan_log_rows: list[dict] = []
    empty_pages = 0
    stale_pages = 0
    for page in range(1, max_pages + 1):
        form_data = _build_work24_public_search_form(source_url, page)
        list_html = _fetch_work24_public_html(_WORK24_PUBLIC_SEARCH_POST_URL, settings, data=form_data)
        listings = _parse_work24_public_list_jobs(list_html, base_url=_WORK24_PUBLIC_SEARCH_BASE_URL)
        page_new_count = 0
        stopped_reason = ""
        if not listings:
            empty_pages += 1
            stale_pages = 0
            if empty_pages >= empty_page_stop_count:
                stopped_reason = "empty_page_stop"
            scan_log_rows.append(
                {
                    "scanned_at": scanned_at,
                    "population_source_name": normalize_whitespace(source_name),
                    "population_source_url": source_url,
                    "population_query": population_query,
                    "page": page,
                    "listing_count": 0,
                    "new_listing_count": 0,
                    "cumulative_unique_job_count": len(jobs),
                    "stopped_reason": stopped_reason,
                }
            )
            if empty_pages >= empty_page_stop_count:
                break
            continue
        empty_pages = 0
        for listing in listings:
            auth_no = _work24_public_auth_no(listing)
            key = auth_no or normalize_whitespace(listing.get("job_url"))
            if not key or key in seen_keys:
                continue
            seen_keys.add(key)
            listing = listing.copy()
            listing["work24_public_page"] = str(page)
            listing["work24_public_tracking_signal"] = "new"
            jobs.append(listing)
            page_new_count += 1
        if page_new_count == 0:
            stale_pages += 1
            if stale_pages >= stale_page_stop_count:
                stopped_reason = "stale_page_stop"
        else:
            stale_pages = 0
        if page >= max_pages and not stopped_reason:
            stopped_reason = "max_page_safety_cap"
        scan_log_rows.append(
            {
                "scanned_at": scanned_at,
                "population_source_name": normalize_whitespace(source_name),
                "population_source_url": source_url,
                "population_query": population_query,
                "page": page,
                "listing_count": len(listings),
                "new_listing_count": page_new_count,
                "cumulative_unique_job_count": len(jobs),
                "stopped_reason": stopped_reason,
            }
        )
        if stopped_reason:
            break
        if delay_seconds and page < max_pages:
            time.sleep(delay_seconds)
    _append_work24_population_scan_log(paths, scan_log_rows)
    return _track_work24_population_listings_with_llm(jobs, settings, paths=paths)


def _work24_population_job_row(source_record: dict, job: dict) -> dict:
    from .collection import _work24_public_auth_no

    source_url = normalize_whitespace(source_record.get("source_url"))
    population_query = _work24_population_query_value(source_url, "srcKeyword") or _work24_population_query_value(source_url, "keyword")
    auth_no = _work24_public_auth_no(job)
    company_name = normalize_whitespace(
        job.get("company_name_hint")
        or job.get("company_name")
        or job.get("company")
        or ""
    )
    return {
        "worknet_wanted_auth_no": auth_no,
        "population_source_name": normalize_whitespace(source_record.get("source_name")),
        "population_source_url": source_url,
        "population_page": normalize_whitespace(str(job.get("work24_public_page") or "")),
        "population_query": population_query,
        "population_role_hint": _work24_population_role_hint(source_record, job),
        "title": normalize_whitespace(job.get("title") or job.get("job_title_raw") or ""),
        "company_name": company_name,
        "location": normalize_whitespace(job.get("location") or job.get("country") or ""),
        "experience_level": normalize_whitespace(job.get("experience_level")),
        "job_url": normalize_whitespace(job.get("job_url")),
        "listing_context": normalize_whitespace(job.get("listing_context")),
        "work24_public_tracking_signal": normalize_whitespace(job.get("work24_public_tracking_signal")),
        "work24_llm_target_hint": normalize_whitespace(job.get("work24_llm_target_hint")),
        "work24_llm_suggested_role": normalize_whitespace(job.get("work24_llm_suggested_role")),
        "work24_llm_reason": normalize_whitespace(job.get("work24_llm_reason")),
    }


def _work24_population_refresh_job_row(record: dict) -> dict:
    source_record = {
        "source_name": normalize_whitespace(record.get("population_source_name")),
        "source_url": normalize_whitespace(record.get("population_source_url")),
    }
    job = {
        "company_name_hint": normalize_whitespace(record.get("company_name")),
        "company_name": normalize_whitespace(record.get("company_name")),
        "title": normalize_whitespace(record.get("title")),
        "job_url": normalize_whitespace(record.get("job_url")),
        "worknet_wanted_auth_no": normalize_whitespace(record.get("worknet_wanted_auth_no")),
        "location": normalize_whitespace(record.get("location")),
        "country": normalize_whitespace(record.get("location")),
        "listing_context": normalize_whitespace(record.get("listing_context")),
        "work24_public_tracking_signal": normalize_whitespace(record.get("work24_public_tracking_signal")),
        "work24_llm_target_hint": normalize_whitespace(record.get("work24_llm_target_hint")),
        "work24_llm_suggested_role": normalize_whitespace(record.get("work24_llm_suggested_role")),
        "work24_llm_reason": normalize_whitespace(record.get("work24_llm_reason")),
        "experience_level": normalize_whitespace(record.get("experience_level")),
        "work24_public_page": normalize_whitespace(record.get("population_page")),
    }
    return _work24_population_job_row(source_record, job)


def _work24_population_candidate_row(source_record: dict, job: dict) -> dict | None:
    company_name = normalize_whitespace(
        job.get("company_name_hint")
        or job.get("company_name")
        or job.get("company")
        or ""
    )
    if not company_name or _normalize_company_name(company_name) == "고용24":
        return None
    title = normalize_whitespace(job.get("title") or job.get("job_title_raw") or "")
    job_url = normalize_whitespace(job.get("job_url")) or normalize_whitespace(source_record.get("source_url"))
    auth_no = normalize_whitespace(job.get("worknet_wanted_auth_no"))
    suggested_role = normalize_whitespace(job.get("work24_llm_suggested_role"))
    target_hint = normalize_whitespace(job.get("work24_llm_target_hint"))
    role_hint = _work24_population_role_hint(source_record, job)
    if not role_hint and target_hint.lower() != "true":
        return None
    tracking_signal = normalize_whitespace(job.get("work24_public_tracking_signal"))
    source_name = normalize_whitespace(source_record.get("source_name")) or "고용24 공개 채용검색"
    reason_parts = [f"{source_name}에서 기업 후보 발견"]
    if title:
        reason_parts.append(f"공고명={title}")
    if auth_no:
        reason_parts.append(f"wantedAuthNo={auth_no}")
    if tracking_signal:
        reason_parts.append(f"추적상태={tracking_signal}")
    if suggested_role:
        reason_parts.append(f"LLM직무힌트={suggested_role}")
    elif target_hint:
        reason_parts.append(f"LLM타깃힌트={target_hint}")
    if role_hint:
        reason_parts.append(f"직군힌트={role_hint}")
    location = normalize_whitespace(job.get("location") or job.get("country") or "")
    return {
        "company_name": company_name,
        "company_tier": _work24_population_candidate_tier(company_name, location),
        "official_domain": "",
        "company_name_en": "",
        "region": location,
        "aliases": [],
        "discovery_method": "work24_population_discovery",
        "candidate_seed_type": "고용24공개채용검색",
        "candidate_seed_url": job_url,
        "candidate_seed_title": title,
        "candidate_seed_reason": "; ".join(reason_parts),
    }


def _work24_population_candidate_tier(company_name: str | None, location: str | None = None) -> str:
    company_text = normalize_whitespace(company_name)
    if company_text and WORK24_PUBLIC_RESEARCH_COMPANY_RE.search(company_text):
        return "공공·연구기관"
    location_text = normalize_whitespace(location)
    if location_text and WORK24_REGIONAL_LOCATION_RE.search(location_text):
        return "지역기업"
    return "중견/중소"


def _work24_limited_public_board_source_url(company_name: str) -> str:
    query = {
        "srcKeyword": company_name,
        "siteClcd": "WORK",
        "resultCnt": "20",
        "pageLimit": "1",
        "hotPageLimit": "1",
        "scanDepth": "3",
        "detailLimit": "5",
        "keywordWantedTitle": "N",
        "keywordJobCont": "N",
        "keywordBusiNm": "Y",
        "keywordStaAreaNm": "N",
    }
    return f"{WORK24_PUBLIC_SEARCH_BASE_URL}?{urlencode(query)}"


def _deduplicate_work24_population_jobs(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=list(WORK24_POPULATION_JOB_COLUMNS))
    frame = frame.copy()
    auth_key = frame["worknet_wanted_auth_no"].fillna("").astype(str).str.strip()
    url_key = frame["job_url"].fillna("").astype(str).str.strip()
    fallback_key = (
        frame["company_name"].fillna("").astype(str).str.strip()
        + "|"
        + frame["title"].fillna("").astype(str).str.strip()
        + "|"
        + frame["population_source_name"].fillna("").astype(str).str.strip()
    )
    frame["_dedupe_key"] = auth_key.where(auth_key.ne(""), url_key.where(url_key.ne(""), fallback_key))
    frame = frame.drop_duplicates(subset=["_dedupe_key"], keep="last").drop(columns=["_dedupe_key"])
    return frame[list(WORK24_POPULATION_JOB_COLUMNS)].reset_index(drop=True)


def _refresh_work24_population_job_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=list(WORK24_POPULATION_JOB_COLUMNS))
    refreshed_rows = []
    for record in frame.fillna("").to_dict(orient="records"):
        refreshed = _work24_population_refresh_job_row(record)
        refreshed_rows.append({column: refreshed.get(column, "") for column in WORK24_POPULATION_JOB_COLUMNS})
    refreshed_frame = pd.DataFrame(refreshed_rows, columns=list(WORK24_POPULATION_JOB_COLUMNS))
    return _deduplicate_work24_population_jobs(refreshed_frame)


def _work24_population_unique_join(values, *, limit: int = 3) -> str:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        text = normalize_whitespace(value)
        if not text or text in seen:
            continue
        seen.add(text)
        output.append(text)
        if len(output) >= limit:
            break
    return " | ".join(output)


def _work24_population_first_nonempty(values) -> str:
    for value in values:
        text = normalize_whitespace(value)
        if text:
            return text
    return ""


def _work24_population_shadow_company_rows(job_frame: pd.DataFrame, company_frame: pd.DataFrame) -> pd.DataFrame:
    if job_frame.empty:
        return pd.DataFrame(columns=list(WORK24_POPULATION_SHADOW_COMPANY_COLUMNS))

    jobs = job_frame.copy()
    for column in WORK24_POPULATION_JOB_COLUMNS:
        if column not in jobs.columns:
            jobs[column] = ""
    jobs["company_name"] = jobs["company_name"].fillna("").astype(str).map(normalize_whitespace)
    jobs["_company_key"] = jobs["company_name"].map(lambda value: _normalize_company_name(value).casefold())
    jobs = jobs[(jobs["company_name"].ne("")) & (jobs["_company_key"].ne("고용24"))]
    if jobs.empty:
        return pd.DataFrame(columns=list(WORK24_POPULATION_SHADOW_COMPANY_COLUMNS))

    promoted_lookup: dict[str, dict] = {}
    if not company_frame.empty:
        candidates = company_frame.copy()
        for column in IMPORT_COMPANY_COLUMNS:
            if column not in candidates.columns:
                candidates[column] = ""
        for row in candidates.fillna("").to_dict(orient="records"):
            key = _normalize_company_name(row.get("company_name")).casefold()
            if key:
                promoted_lookup[key] = row

    rows: list[dict] = []
    for company_key, group in jobs.groupby("_company_key", sort=True):
        company_name = _work24_population_first_nonempty(group["company_name"])
        location = _work24_population_first_nonempty(group["location"].fillna("").astype(str))
        role_hint_count = int(group["population_role_hint"].fillna("").astype(str).str.strip().ne("").sum())
        llm_hints = group["work24_llm_target_hint"].fillna("").astype(str).str.strip().str.casefold()
        true_count = int(llm_hints.isin({"true", "1", "yes", "y"}).sum())
        false_count = int(llm_hints.isin({"false", "0", "no", "n"}).sum())
        promoted_row = promoted_lookup.get(company_key, {})
        promoted = bool(promoted_row)
        raw_job_count = int(len(group))
        if promoted:
            reason = normalize_whitespace(promoted_row.get("candidate_seed_reason"))
            status = "promoted_candidate"
        else:
            reason = (
                "Work24 raw population에서 기업 발견"
                f"; raw_job_count={raw_job_count}"
                f"; role_hint_count={role_hint_count}"
                f"; llm_target_true_count={true_count}"
            )
            status = "pending_detail_validation"
        rows.append(
            {
                "company_name": company_name,
                "company_tier": normalize_whitespace(promoted_row.get("company_tier"))
                or _work24_population_candidate_tier(company_name, location),
                "region": normalize_whitespace(promoted_row.get("region")) or location,
                "raw_job_count": raw_job_count,
                "role_hint_count": role_hint_count,
                "llm_target_true_count": true_count,
                "llm_target_false_count": false_count,
                "sample_titles": _work24_population_unique_join(group["title"].fillna("").astype(str), limit=3),
                "sample_job_urls": _work24_population_unique_join(group["job_url"].fillna("").astype(str), limit=3),
                "sample_role_hints": _work24_population_unique_join(group["population_role_hint"].fillna("").astype(str), limit=4),
                "sample_queries": _work24_population_unique_join(group["population_query"].fillna("").astype(str), limit=4),
                "shadow_status": status,
                "promoted_candidate": "true" if promoted else "false",
                "candidate_seed_reason": reason,
            }
        )

    frame = pd.DataFrame(rows, columns=list(WORK24_POPULATION_SHADOW_COMPANY_COLUMNS))
    if frame.empty:
        return frame
    frame["_promoted_sort"] = frame["promoted_candidate"].eq("true").astype(int)
    frame = frame.sort_values(
        by=["_promoted_sort", "raw_job_count", "company_name"],
        ascending=[False, False, True],
    ).drop(columns=["_promoted_sort"])
    return frame[list(WORK24_POPULATION_SHADOW_COMPANY_COLUMNS)].reset_index(drop=True)


def discover_work24_population(paths, settings=None) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    population_sources = load_population_discovery_sources(paths)
    work24_sources = [
        row
        for row in population_sources.fillna("").to_dict(orient="records")
        if normalize_whitespace(row.get("source_type")).casefold() == "work24_public_html"
    ]
    candidate_rows: list[dict] = []
    job_rows: list[dict] = []
    errors: list[dict] = []
    source_job_count = 0

    if settings is None and work24_sources:
        return (
            pd.DataFrame(columns=list(IMPORT_COMPANY_COLUMNS)),
            pd.DataFrame(columns=list(WORK24_POPULATION_JOB_COLUMNS)),
            pd.DataFrame(columns=list(WORK24_POPULATION_SHADOW_COMPANY_COLUMNS)),
            {
                "population_discovery_source_count": int(len(population_sources)),
                "work24_population_source_count": int(len(work24_sources)),
                "work24_population_job_count": 0,
                "work24_population_candidate_count": 0,
                "work24_population_shadow_company_count": 0,
                "work24_population_shadow_pending_count": 0,
                "work24_population_error_count": int(len(work24_sources)),
                "work24_population_errors": [{"reason": "settings_unavailable"}],
            },
        )

    for source_record in work24_sources:
        source_url = normalize_whitespace(source_record.get("source_url"))
        if not source_url:
            continue
        try:
            jobs = _fetch_work24_population_source_jobs(
                source_url,
                settings,
                paths=paths,
                source_name=normalize_whitespace(source_record.get("source_name")),
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(
                {
                    "source_name": normalize_whitespace(source_record.get("source_name")),
                    "source_url": source_url,
                    "error_type": exc.__class__.__name__,
                    "error_message": str(exc),
                }
            )
            continue
        source_job_count += len(jobs)
        for job in jobs:
            job_rows.append(_work24_population_job_row(source_record, job))
            candidate_row = _work24_population_candidate_row(source_record, job)
            if candidate_row is not None:
                candidate_rows.append(candidate_row)

    company_frame = pd.DataFrame(candidate_rows, columns=list(IMPORT_COMPANY_COLUMNS))
    if not company_frame.empty:
        company_frame = company_frame.drop_duplicates(subset=["company_name"], keep="last").reset_index(drop=True)
    job_frame = _deduplicate_work24_population_jobs(pd.DataFrame(job_rows, columns=list(WORK24_POPULATION_JOB_COLUMNS)))
    shadow_frame = _work24_population_shadow_company_rows(job_frame, company_frame)
    role_counts = job_frame["population_role_hint"].fillna("").astype(str).str.strip()
    role_counts = role_counts[role_counts.ne("")].value_counts().to_dict()
    summary = {
        "population_discovery_source_count": int(len(population_sources)),
        "work24_population_source_count": int(len(work24_sources)),
        "work24_population_raw_job_count": int(source_job_count),
        "work24_population_job_count": int(len(job_frame)),
        "work24_population_candidate_count": int(len(company_frame)),
        "work24_population_shadow_company_count": int(len(shadow_frame)),
        "work24_population_shadow_pending_count": int(
            shadow_frame["promoted_candidate"].fillna("").astype(str).str.casefold().eq("false").sum()
        )
        if not shadow_frame.empty
        else 0,
        "work24_population_role_counts": {str(key): int(value) for key, value in role_counts.items()},
        "work24_population_error_count": int(len(errors)),
        "work24_population_errors": errors[:5],
    }
    return company_frame, job_frame, shadow_frame, summary


def discover_work24_population_candidates(paths, settings=None) -> tuple[pd.DataFrame, dict]:
    company_frame, _job_frame, _shadow_frame, summary = discover_work24_population(paths, settings)
    return company_frame, summary


def _company_keys_with_existing_collectable_sources(paths, current_rows: list[dict]) -> set[str]:
    covered: set[str] = set()
    for row in current_rows:
        source_type = normalize_whitespace(row.get("source_type")).casefold()
        if source_type == "work24_public_html":
            continue
        company_key = _normalize_company_name(str(row.get("company_name") or "")).casefold()
        if company_key:
            covered.add(company_key)

    existing_registry = read_csv_or_empty(getattr(paths, "source_registry_path", Path("")))
    if existing_registry.empty:
        return covered
    for row in existing_registry.fillna("").to_dict(orient="records"):
        source_type = normalize_whitespace(row.get("source_type")).casefold()
        source_bucket = normalize_whitespace(row.get("source_bucket")).casefold()
        if source_type == "work24_public_html" or source_bucket not in {"approved", "candidate"}:
            continue
        company_key = _normalize_company_name(str(row.get("company_name") or "")).casefold()
        if company_key:
            covered.add(company_key)
    return covered


def _companies_with_work24_population_candidates(companies: pd.DataFrame, paths) -> pd.DataFrame:
    population_candidates = load_work24_population_candidates(paths)
    if population_candidates.empty:
        return companies
    frames = [frame for frame in (companies, population_candidates) if not frame.empty]
    if not frames:
        return pd.DataFrame(columns=list(IMPORT_COMPANY_COLUMNS))
    combined = pd.concat(frames, ignore_index=True)
    if "company_name" not in combined.columns:
        return pd.DataFrame(columns=list(IMPORT_COMPANY_COLUMNS))
    return combined.drop_duplicates(subset=["company_name"], keep="first").reset_index(drop=True)


def _work24_limited_public_board_source_candidates(companies: pd.DataFrame, paths, covered_company_keys: set[str]) -> list[dict]:
    population_candidates = load_work24_population_candidates(paths)
    if population_candidates.empty or companies.empty:
        return []

    population_lookup = {
        _normalize_company_name(str(row.get("company_name") or "")).casefold(): row
        for row in population_candidates.fillna("").to_dict(orient="records")
    }
    rows: list[dict] = []
    seen_urls: set[str] = set()
    for company in companies.fillna("").to_dict(orient="records"):
        company_name = _normalize_company_name(str(company.get("company_name") or ""))
        company_key = company_name.casefold()
        if not company_name or company_key in covered_company_keys or company_key not in population_lookup:
            continue
        source_url = _work24_limited_public_board_source_url(company_name)
        if source_url in seen_urls:
            continue
        seen_urls.add(source_url)
        seed = population_lookup[company_key]
        seed_title = normalize_whitespace(seed.get("candidate_seed_title"))
        seed_reason = normalize_whitespace(seed.get("candidate_seed_reason"))
        rows.append(
            {
                "company_name": company_name,
                "company_tier": company.get("company_tier") or seed.get("company_tier") or "",
                "source_name": f"고용24 제한 공개보드 - {company_name}",
                "source_url": source_url,
                "source_type": "work24_public_html",
                "official_domain": "",
                "is_official_hint": False,
                "structure_hint": "limited_public_board",
                "discovery_method": WORK24_LIMITED_PUBLIC_BOARD_DISCOVERY_METHOD,
                "source_title": seed_title,
                "candidate_seed_reason": seed_reason,
            }
        )
    return rows


def generate_company_candidates(paths) -> pd.DataFrame:
    frames = [
        load_company_seed_records(paths),
        load_work24_population_candidates(paths),
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
        if _is_population_discovery_source(row):
            continue
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
        if _is_population_discovery_source(record):
            continue
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
        homepage_rows = _probe_source_candidates_from_homepages(companies, companies_with_manual_sources, settings)
        rows.extend(homepage_rows)

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
    covered_company_keys = _company_keys_with_existing_collectable_sources(paths, rows)
    fallback_companies = _companies_with_work24_population_candidates(companies, paths)
    rows.extend(_work24_limited_public_board_source_candidates(fallback_companies, paths, covered_company_keys))
    return pd.DataFrame(rows, columns=list(IMPORT_SOURCE_COLUMNS))
