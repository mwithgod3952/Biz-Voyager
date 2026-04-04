"""Quality gate rules."""

from __future__ import annotations

import json
import re
from difflib import SequenceMatcher
from urllib.parse import parse_qs, urlparse

import pandas as pd

from .constants import ALLOWED_JOB_ROLES
from .gemini import GeminiBudget, maybe_adjudicate_duplicate_pair
from .models import GateResult
from .presentation import (
    _DEFAULT_EXPERIENCE_DISPLAY,
    _DEFAULT_PREFERRED_DISPLAY,
    _PREFERRED_HEADING_PATTERNS,
    _extract_section_from_raw_detail,
    _preserve_english_task_lines,
    _track_from_text,
    build_display_fields,
    compose_detail_fallback,
    count_english_leaks,
    detail_prefers_structured_sections,
    sanitize_core_skill_text,
    sanitize_name_or_title_text,
    sanitize_section_text,
    section_loss_looks_high,
    section_output_is_substantive,
    section_output_looks_noisy,
)
from .storage import atomic_write_text
from .utils import canonicalize_runtime_source_url, normalize_whitespace


_DETAIL_LEGAL_NOTICE_RE = re.compile(
    r"공평한 기회|열려 있는 회사|개인정보|문서 반환|전형 절차|채용 절차|privacy notice|document return|recruitment process|application review|equal opportunit",
    flags=re.IGNORECASE,
)
_ADMIN_NOTICE_LINE_PATTERNS = (
    re.compile(r"채용 관련 문의"),
    re.compile(r"지원 전[, ]*확인"),
    re.compile(r"허위 사실"),
    re.compile(r"레퍼런스 체크"),
    re.compile(r"시용기간"),
    re.compile(r"채용 디비"),
    re.compile(r"관련 자료"),
    re.compile(r"온보딩"),
    re.compile(r"채용 과정"),
    re.compile(r"제출 자료"),
    re.compile(r"민감 정보"),
    re.compile(r"근무환경"),
    re.compile(r"근무 형태"),
    re.compile(r"모집 절차"),
    re.compile(r"최종 결과 발표"),
    re.compile(r"절차는 상황에 따라"),
    re.compile(r"문의 부탁"),
)
_MAIN_TASK_SIGNAL_PATTERNS = (
    re.compile(r"개발"),
    re.compile(r"설계"),
    re.compile(r"구현"),
    re.compile(r"수행"),
    re.compile(r"연구"),
    re.compile(r"최적화"),
    re.compile(r"구축"),
    re.compile(r"운영"),
    re.compile(r"담당"),
    re.compile(r"협업"),
    re.compile(r"주도"),
    re.compile(r"배포"),
    re.compile(r"\bdesign\b", flags=re.IGNORECASE),
    re.compile(r"\bdevelop\b", flags=re.IGNORECASE),
    re.compile(r"\bbuild\b", flags=re.IGNORECASE),
    re.compile(r"\bresearch\b", flags=re.IGNORECASE),
    re.compile(r"\boptimiz", flags=re.IGNORECASE),
)
_REQUIREMENT_SIGNAL_PATTERNS = (
    re.compile(r"경험"),
    re.compile(r"학위"),
    re.compile(r"지식"),
    re.compile(r"능숙"),
    re.compile(r"보유"),
    re.compile(r"이해"),
    re.compile(r"가능"),
    re.compile(r"필요"),
    re.compile(r"소지"),
    re.compile(r"역량"),
    re.compile(r"능력"),
    re.compile(r"있어야"),
    re.compile(r"필수"),
    re.compile(r"\brequired\b", flags=re.IGNORECASE),
    re.compile(r"\bmust\b", flags=re.IGNORECASE),
    re.compile(r"\bqualification", flags=re.IGNORECASE),
)
_GENERIC_POOL_TITLE_RE = re.compile(r"talent\s*pool|전문연구요원\s*\(r&d\)", flags=re.IGNORECASE)
_TITLE_MAIN_TASK_FALLBACKS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"post[- ]training", flags=re.IGNORECASE), "거대 언어 모델 사후학습과 성능 최적화를 수행합니다."),
    (re.compile(r"\beval\b|evaluation", flags=re.IGNORECASE), "거대 언어 모델 평가 체계를 설계하고 모델 성능을 분석합니다."),
)
_TITLE_FOCUS_FALLBACKS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"research|scientist|리서처|연구|r&d", flags=re.IGNORECASE), "연구"),
    (re.compile(r"runtime|library|kernel|driver|compiler|platform|validation", flags=re.IGNORECASE), "인프라"),
    (re.compile(r"post[- ]training|eval|optimization", flags=re.IGNORECASE), "최적화"),
)
_IMPOSSIBLE_EXPERIENCE_RE = re.compile(r"경력\s*(?:20\d{2}|\d{3,})년\+")
_PRACTICAL_DUPLICATE_TITLE_CODE_RE = re.compile(r"\[[A-Za-z]+-?\d+\]")
_LOCATION_HINT_RE = re.compile(
    r"(서울|울산|창원|강남오피스|강남|판교|부산|대전|광주|대구|인천|수원|성남|분당|제주|원격|remote)",
    flags=re.IGNORECASE,
)
_PRACTICAL_DUPLICATE_TRACK_HINT_RE = re.compile(
    r"전문연구요원|alternative military service|military service|intern|인턴|contract|계약직",
    flags=re.IGNORECASE,
)
_NEAR_DUPLICATE_NOISE_RE = re.compile(
    r"전문연구요원|alternative military service|military service|intern|인턴|contract|계약직|정규직|정규|경력|신입|채용|공고|모집",
    flags=re.IGNORECASE,
)
_NEAR_DUPLICATE_LEVEL_RE = re.compile(
    r"\b(?:senior|staff|sr|jr|lead|principal|director|manager|ii|iii|iv)\b|시니어|주니어|책임|수석|선임",
    flags=re.IGNORECASE,
)
_QUALITY_SCORE_TARGET = 99.0
_MAIN_TASK_BLANK_THRESHOLD = 0.15
_REQUIREMENT_BLANK_THRESHOLD = 0.2
_PREFERRED_BLANK_THRESHOLD = 0.35
_EXPERIENCE_PLACEHOLDER_THRESHOLD = 0.2
_FOCUS_BLANK_THRESHOLD = 0.2
_TRACK_CUE_BLANK_THRESHOLD = 0.2
_POSITION_SUMMARY_BLANK_THRESHOLD = 0.12
_DROPPED_LOW_QUALITY_RATIO_THRESHOLD = 0.03
_DUPLICATE_JOB_URL_RATIO_THRESHOLD = 0.01
_IMPOSSIBLE_EXPERIENCE_RATIO_THRESHOLD = 0.01


def _normalized_cell(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    return normalize_whitespace(str(value))


def _first_nonempty_text(*values: object) -> str:
    for value in values:
        normalized = _normalized_cell(value)
        if normalized:
            return normalized
    return ""


def _extract_display_disambiguator(row: pd.Series) -> str:
    job_id = _normalized_cell(row.get("job_id"))
    if job_id:
        match = re.search(r"([A-Za-z0-9]{5,})$", job_id)
        if match:
            return match.group(1)

    job_url = _normalized_cell(row.get("job_url"))
    if job_url:
        parsed = urlparse(job_url)
        params = parse_qs(parsed.query)
        for param in ("gh_jid", "jobId", "job_id", "posting_id", "id"):
            values = params.get(param)
            if values:
                value = normalize_whitespace(values[0])
                if value:
                    return value
        path = normalize_whitespace(parsed.path.strip("/"))
        if path:
            tail = path.rsplit("/", 1)[-1]
            match = re.search(r"([A-Za-z0-9_-]{5,})$", tail)
            if match:
                return match.group(1)
    job_key = _normalized_cell(row.get("job_key"))
    if job_key:
        compact = re.sub(r"[^A-Za-z0-9]", "", job_key)
        if compact:
            return compact[-8:]
    return ""


def _normalized_display_title_stem(value: object) -> str:
    text = _normalized_cell(value)
    if not text:
        return ""
    text = re.sub(r"\[[^\]]+\]", " ", text)
    text = re.sub(r"\([^)]+\)", " ", text)
    text = re.sub(r"[^0-9A-Za-z가-힣]+", " ", text)
    text = normalize_whitespace(text).lower()
    stopwords = {
        "senior",
        "staff",
        "sr",
        "jr",
        "lead",
        "principal",
        "intern",
        "contract",
        "assistant",
        "associate",
        "전문연구요원",
        "학사",
        "석사",
        "박사",
        "인턴",
        "전환형",
        "경력",
        "신입",
        "채용",
    }
    tokens = [token for token in text.split() if token not in stopwords]
    return normalize_whitespace(" ".join(tokens))


def _normalized_practical_duplicate_title(value: object) -> str:
    text = _normalized_cell(value)
    if not text:
        return ""
    text = _PRACTICAL_DUPLICATE_TITLE_CODE_RE.sub(" ", text)
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text


def _analysis_content_signature(row: pd.Series) -> tuple[str, ...]:
    return (
        _normalized_cell(row.get("주요업무_분석용")),
        _normalized_cell(row.get("자격요건_분석용")),
        _normalized_cell(row.get("우대사항_분석용")),
        _normalized_cell(row.get("상세본문_분석용")),
    )


def _normalized_duplicate_text(value: object, *, strip_variants: bool = False) -> str:
    text = _normalized_cell(value).lower()
    if not text:
        return ""
    text = _PRACTICAL_DUPLICATE_TITLE_CODE_RE.sub(" ", text)
    text = _LOCATION_HINT_RE.sub(" ", text)
    if strip_variants:
        text = _PRACTICAL_DUPLICATE_TRACK_HINT_RE.sub(" ", text)
        text = _NEAR_DUPLICATE_LEVEL_RE.sub(" ", text)
        text = _NEAR_DUPLICATE_NOISE_RE.sub(" ", text)
    text = re.sub(r"[^0-9a-z가-힣]+", " ", text)
    return normalize_whitespace(text)


def _duplicate_tokens(value: object, *, strip_variants: bool = False) -> set[str]:
    tokens = [
        token
        for token in _normalized_duplicate_text(value, strip_variants=strip_variants).split()
        if len(token) >= 2
    ]
    return set(tokens)


def _token_jaccard(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


def _practical_duplicate_body_text(row: pd.Series) -> str:
    parts = [
        _normalized_cell(row.get("주요업무_분석용")),
        _normalized_cell(row.get("자격요건_분석용")),
        _normalized_cell(row.get("우대사항_분석용")),
        _normalized_cell(row.get("상세본문_분석용")),
    ]
    return "\n".join(part for part in parts if part)


def _practical_duplicate_title_anchor(row: pd.Series) -> str:
    title = _first_nonempty_text(row.get("job_title_raw"), row.get("job_title_ko"), row.get("공고제목_표시"))
    return _normalized_duplicate_text(title, strip_variants=True)


def _practical_duplicate_title_stem(row: pd.Series) -> str:
    title = _first_nonempty_text(row.get("job_title_raw"), row.get("job_title_ko"), row.get("공고제목_표시"))
    return _normalized_display_title_stem(title)


def _practical_duplicate_same_title_family(row: pd.Series, other: pd.Series) -> bool:
    stem = _practical_duplicate_title_stem(row)
    other_stem = _practical_duplicate_title_stem(other)
    if stem and other_stem and stem == other_stem:
        return True
    anchor = _practical_duplicate_title_anchor(row)
    other_anchor = _practical_duplicate_title_anchor(other)
    return bool(anchor and other_anchor and anchor == other_anchor)


def _variant_signal_source_text(row: pd.Series) -> str:
    parts = [
        _first_nonempty_text(row.get("job_title_raw"), row.get("job_title_ko"), row.get("공고제목_표시")),
        _normalized_cell(row.get("구분요약_표시")),
        _normalized_cell(row.get("채용트랙_표시")),
        _normalized_cell(row.get("경력수준_표시")),
    ]
    return "\n".join(part for part in parts if part)


def _extract_location_hints(row: pd.Series) -> tuple[str, ...]:
    text = _variant_signal_source_text(row)
    hints = {
        normalize_whitespace(match).lower()
        for match in _LOCATION_HINT_RE.findall(text)
        if normalize_whitespace(match)
    }
    return tuple(sorted(hints))


def _extract_track_signals(row: pd.Series) -> tuple[str, ...]:
    text = _variant_signal_source_text(row)
    lowered = text.lower()
    tokens: set[str] = set()
    if "전문연구요원" in text or "alternative military service" in lowered or "military service" in lowered:
        tokens.add("military")
    if "전환형 인턴" in text:
        tokens.add("convert_intern")
    if "인턴" in text or re.search(r"\bintern\b", lowered):
        tokens.add("intern")
    if "계약직" in text or re.search(r"\bcontract\b", lowered):
        tokens.add("contract")
    return tuple(sorted(tokens))


def _extract_level_signals(row: pd.Series) -> tuple[str, ...]:
    text = _variant_signal_source_text(row)
    lowered = text.lower()
    tokens: set[str] = set()
    if "주니어" in text or re.search(r"\b(?:jr|junior)\b", lowered):
        tokens.add("junior")
    if "시니어" in text or re.search(r"\b(?:sr|senior)\b", lowered):
        tokens.add("senior")
    if "스태프" in text or re.search(r"\bstaff\b", lowered):
        tokens.add("staff")
    if re.search(r"\blead\b", lowered):
        tokens.add("lead")
    if re.search(r"\bprincipal\b", lowered):
        tokens.add("principal")
    if re.search(r"\bdirector\b", lowered):
        tokens.add("director")
    if re.search(r"\bmanager\b", lowered):
        tokens.add("manager")
    if "신입" in text:
        tokens.add("entry")
    if "경력" in text or re.search(r"career|\d+\s*years?", lowered):
        tokens.add("experienced")
    return tuple(sorted(tokens))


def _extract_degree_signals(row: pd.Series) -> tuple[str, ...]:
    text = _variant_signal_source_text(row)
    tokens: set[str] = set()
    for token in ("학사", "석사", "박사"):
        if token in text:
            tokens.add(token)
    return tuple(sorted(tokens))


def _variant_signals(row: pd.Series) -> dict[str, tuple[str, ...]]:
    return {
        "location": _extract_location_hints(row),
        "track": _extract_track_signals(row),
        "level": _extract_level_signals(row),
        "degree": _extract_degree_signals(row),
    }


def _rows_have_explicit_variant_conflict(row: pd.Series, other: pd.Series) -> bool:
    left = _variant_signals(row)
    right = _variant_signals(other)
    if left["location"] and right["location"] and left["location"] != right["location"]:
        return True
    for key in ("track", "level", "degree"):
        if left[key] != right[key] and (left[key] or right[key]):
            return True
    return False


def _practical_duplicate_similarity(row: pd.Series, other: pd.Series) -> tuple[float, float, float]:
    body = _practical_duplicate_body_text(row)
    other_body = _practical_duplicate_body_text(other)
    body_seq = (
        SequenceMatcher(
            None,
            _normalized_duplicate_text(body, strip_variants=True),
            _normalized_duplicate_text(other_body, strip_variants=True),
        ).ratio()
        if body and other_body
        else 0.0
    )
    token_score = _token_jaccard(
        _duplicate_tokens(body, strip_variants=True),
        _duplicate_tokens(other_body, strip_variants=True),
    )
    title_seq = SequenceMatcher(
        None,
        _practical_duplicate_title_anchor(row),
        _practical_duplicate_title_anchor(other),
    ).ratio()
    return body_seq, token_score, title_seq


def _visible_detail_similarity(row: pd.Series, other: pd.Series) -> float:
    detail = _normalized_cell(row.get("상세본문_분석용"))
    other_detail = _normalized_cell(other.get("상세본문_분석용"))
    if not detail or not other_detail:
        return 0.0
    return SequenceMatcher(None, detail, other_detail).ratio()


def _rows_are_near_practical_duplicates(row: pd.Series, other: pd.Series) -> bool:
    company = _first_nonempty_text(row.get("company_name"), row.get("회사명_표시"))
    other_company = _first_nonempty_text(other.get("company_name"), other.get("회사명_표시"))
    role = _first_nonempty_text(row.get("job_role"), row.get("직무명_표시"))
    other_role = _first_nonempty_text(other.get("job_role"), other.get("직무명_표시"))
    if not company or company != other_company or not role or role != other_role:
        return False

    exact_signature = _analysis_content_signature(row) == _analysis_content_signature(other)
    if exact_signature and any(_analysis_content_signature(row)):
        return True

    body_seq, token_score, title_seq = _practical_duplicate_similarity(row, other)
    strong_title_match = _practical_duplicate_same_title_family(row, other) or title_seq >= 0.98
    if not strong_title_match:
        return False
    if _practical_duplicate_same_title_family(row, other):
        return True
    visible_detail_seq = _visible_detail_similarity(row, other)
    if visible_detail_seq >= 0.75:
        return True
    if body_seq >= 0.8 and token_score >= 0.65:
        return True
    if visible_detail_seq >= 0.9 and title_seq >= 0.75:
        return True

    if _rows_have_explicit_variant_conflict(row, other):
        return body_seq >= 0.89 and title_seq >= 0.75

    if body_seq >= 0.992 and title_seq >= 0.9:
        return True
    if body_seq >= 0.97 and token_score >= 0.9 and title_seq >= 0.93:
        return True
    if body_seq >= 0.95 and token_score >= 0.94 and title_seq >= 0.96:
        return True
    return False


def _should_request_duplicate_adjudication(
    row: pd.Series,
    other: pd.Series,
    *,
    body_seq: float,
    token_score: float,
    title_seq: float,
    visible_detail_seq: float,
) -> bool:
    strong_title_match = _practical_duplicate_same_title_family(row, other) or title_seq >= 0.98
    if not strong_title_match:
        return False
    if visible_detail_seq >= 0.88 and title_seq >= 0.75:
        return True
    if body_seq < 0.78:
        return False
    if token_score < 0.65 and title_seq < 0.7 and body_seq < 0.9:
        return False
    if body_seq >= 0.9 and title_seq >= 0.7:
        return True
    if body_seq >= 0.84 and title_seq >= 0.82:
        return True
    same_location = _location_hint_from_title(row) == _location_hint_from_title(other)
    left_track = _normalized_cell(row.get("채용트랙_표시"))
    right_track = _normalized_cell(other.get("채용트랙_표시"))
    if not same_location and body_seq < 0.97:
        return False
    if left_track and right_track and left_track != right_track and body_seq < 0.92:
        return False
    return body_seq >= 0.82 and title_seq >= 0.75


def _location_hint_from_title(row: pd.Series) -> str:
    title = _first_nonempty_text(row.get("job_title_raw"), row.get("job_title_ko"), row.get("공고제목_표시"))
    if not title:
        return ""
    candidates = re.findall(r"\[([^\]]+)\]|\(([^)]+)\)", title)
    flattened = [part for pair in candidates for part in pair if part]
    flattened.append(title)
    for candidate in flattened:
        normalized = normalize_whitespace(candidate)
        if not normalized:
            continue
        match = _LOCATION_HINT_RE.search(normalized)
        if match:
            return normalize_whitespace(match.group(1)).lower()
    return ""


def _practical_duplicate_variant_key(row: pd.Series) -> tuple[str, ...]:
    signals = _variant_signals(row)
    return (
        *(f"loc:{value}" for value in signals["location"]),
        *(f"track:{value}" for value in signals["track"]),
        *(f"level:{value}" for value in signals["level"]),
        *(f"degree:{value}" for value in signals["degree"]),
    )


def _practical_duplicate_keep_rank(row: pd.Series) -> tuple[float, int]:
    title = _first_nonempty_text(row.get("job_title_raw"), row.get("job_title_ko"), row.get("공고제목_표시"))
    summary = _normalized_cell(row.get("구분요약_표시"))
    track_text = _normalized_cell(row.get("채용트랙_표시"))
    has_track_variant = bool(_PRACTICAL_DUPLICATE_TRACK_HINT_RE.search(track_text)) or bool(_PRACTICAL_DUPLICATE_TRACK_HINT_RE.search(title))
    score = 0.0
    if _normalized_cell(row.get("job_url")):
        score += 1.0
    if not has_track_variant:
        score += 2.0
    detail = _normalized_cell(row.get("상세본문_분석용"))
    main_tasks = _normalized_cell(row.get("주요업무_분석용"))
    requirements = _normalized_cell(row.get("자격요건_분석용"))
    preferred = _normalized_cell(row.get("우대사항_분석용"))
    score += min(len(detail), 1500) / 300.0
    score += min(len(main_tasks), 600) / 400.0
    score += min(len(requirements), 600) / 500.0
    score += min(len(preferred), 400) / 800.0
    if not _location_hint_from_title(row):
        score += 0.5
    if _PRACTICAL_DUPLICATE_TITLE_CODE_RE.search(title):
        score -= 0.5
    score += min(len(title), 100) / 500.0
    score += min(len(summary), 80) / 800.0
    return score, int(row.name)


def _group_location_hints(group: pd.DataFrame) -> list[str]:
    hints: list[str] = []
    for index in group.index:
        hint = _location_hint_from_title(group.loc[index])
        if hint and hint not in hints:
            hints.append(hint)
    return hints


def _merge_location_hints_into_title(title: str, hints: list[str]) -> str:
    normalized_title = normalize_whitespace(title)
    if not normalized_title or len(hints) <= 1:
        return normalized_title
    match = re.match(r"^\[([^\]]+)\]\s*(.*)$", normalized_title)
    if match and _LOCATION_HINT_RE.search(match.group(1)):
        normalized_title = normalize_whitespace(match.group(2))
    merged = " / ".join(hints)
    if merged and merged not in normalized_title:
        return f"[{merged}] {normalized_title}"
    return normalized_title


def _extract_title_variant_hint(row: pd.Series) -> str:
    raw_title = _first_nonempty_text(row.get("job_title_raw"), row.get("job_title_ko"))
    if raw_title:
        variant_candidates: list[str] = []
        if "|" in raw_title:
            variant_candidates.extend(part.strip() for part in raw_title.split("|")[1:] if part.strip())
        variant_candidates.extend(match.strip() for match in re.findall(r"\(([^)]+)\)", raw_title))
        variant_candidates.extend(match.strip() for match in re.findall(r"\[([^\]]+)\]", raw_title))

        for candidate in variant_candidates:
            sanitized = sanitize_name_or_title_text(candidate, allow_english=True)
            if sanitized:
                return sanitized

    candidates = [
        _normalized_cell(row.get("구분요약_표시")),
        _normalized_cell(row.get("채용트랙_표시")),
        _normalized_cell(row.get("직무초점_표시")),
    ]
    for candidate in candidates:
        if candidate:
            return candidate
    return ""


def _disambiguate_duplicate_display_titles(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"회사명_표시", "공고제목_표시"}
    if frame.empty or not required.issubset(frame.columns):
        return frame

    normalized = frame.copy()
    normalized["_title_stem"] = normalized["공고제목_표시"].map(_normalized_display_title_stem)
    stem_sizes = normalized.groupby(["회사명_표시", "_title_stem"], dropna=False)["_title_stem"].transform("size")
    practical_duplicate_mask = stem_sizes > 1
    if practical_duplicate_mask.any():
        for (_, _), group in normalized[practical_duplicate_mask].groupby(["회사명_표시", "_title_stem"], dropna=False):
            if len(group) <= 1:
                continue
            raw_variants = {
                _first_nonempty_text(group.loc[index].get("job_title_raw"), group.loc[index].get("job_title_ko"), group.loc[index].get("공고제목_표시"))
                for index in group.index
            }
            raw_variants = {normalize_whitespace(value) for value in raw_variants if normalize_whitespace(value)}
            if len(raw_variants) <= 1:
                continue
            used_hints: set[str] = set()
            for index in group.index:
                title = _normalized_cell(normalized.at[index, "공고제목_표시"])
                hint = _extract_title_variant_hint(normalized.loc[index])
                if not hint or hint in used_hints:
                    continue
                if hint not in title:
                    normalized.at[index, "공고제목_표시"] = f"{title} | {hint}"
                used_hints.add(hint)

    group_sizes = normalized.groupby(["회사명_표시", "공고제목_표시"], dropna=False)["공고제목_표시"].transform("size")
    duplicate_mask = group_sizes > 1
    if duplicate_mask.any():
        duplicate_indices = normalized.index[duplicate_mask]
        suffixes: dict[int, str] = {}
        fallback_counters: dict[tuple[str, str], int] = {}
        seen_suffixes: dict[tuple[str, str], set[str]] = {}

        for index in duplicate_indices:
            row = normalized.loc[index]
            group_key = (_normalized_cell(row.get("회사명_표시")), _normalized_cell(row.get("공고제목_표시")))
            seen_suffixes.setdefault(group_key, set())
            suffix = _extract_display_disambiguator(row)
            if suffix and suffix not in seen_suffixes[group_key]:
                seen_suffixes[group_key].add(suffix)
                suffixes[index] = suffix
                continue

            fallback_counters[group_key] = fallback_counters.get(group_key, 0) + 1
            ordinal = fallback_counters[group_key]
            while f"#{ordinal}" in seen_suffixes[group_key]:
                ordinal += 1
            seen_suffixes[group_key].add(f"#{ordinal}")
            suffixes[index] = f"{ordinal}"

        normalized.loc[duplicate_indices, "공고제목_표시"] = [
            f"{normalized.at[index, '공고제목_표시']} [공고 {suffixes[index]}]"
            for index in duplicate_indices
        ]

    normalized = normalized.drop(columns=["_title_stem"], errors="ignore")
    return normalized


def _row_has_low_quality_analysis(row: pd.Series) -> bool:
    detail = _normalized_cell(row.get("상세본문_분석용"))
    main_tasks = _normalized_cell(row.get("주요업무_분석용"))
    requirements = _normalized_cell(row.get("자격요건_분석용"))
    preferred = _normalized_cell(row.get("우대사항_분석용"))
    core_skills = _normalized_cell(row.get("핵심기술_분석용"))
    title = _first_nonempty_text(row.get("job_title_raw"), row.get("job_title_ko"), row.get("공고제목_표시"))
    if core_skills and section_output_looks_noisy(core_skills):
        return True
    if main_tasks and section_output_looks_noisy(main_tasks):
        return True
    if requirements and section_output_looks_noisy(requirements):
        return True
    if preferred and section_output_looks_noisy(preferred):
        return True
    detail_ok = section_output_is_substantive(detail)
    main_ok = bool(main_tasks) and not section_output_looks_noisy(main_tasks)
    requirements_ok = bool(requirements) and not section_output_looks_noisy(requirements)

    if detail and _DETAIL_LEGAL_NOTICE_RE.search(detail):
        return True
    if _GENERIC_POOL_TITLE_RE.search(title) and not (main_ok and requirements_ok):
        return True
    if not main_ok and _admin_notice_density_high(detail, requirements, preferred):
        return True
    if not detail_ok and not (main_ok and requirements_ok):
        return True
    return False


def _normalize_main_task_analysis(row: pd.Series) -> str:
    for candidate in (row.get("주요업무_분석용"), row.get("main_tasks")):
        raw = _normalized_cell(candidate)
        if not raw:
            continue
        sanitized = sanitize_section_text(raw)
        if sanitized and not section_loss_looks_high(raw, sanitized):
            return sanitized
        preserved = _preserve_english_task_lines(raw)
        if preserved:
            return preserved
        if sanitized:
            return sanitized
    return ""


def _iter_meaningful_lines(text: object) -> list[str]:
    normalized = _normalized_cell(text)
    if not normalized:
        return []
    lines: list[str] = []
    seen: set[str] = set()
    for line in re.split(r"[\r\n]+", normalized):
        compact = normalize_whitespace(line)
        if len(compact) < 4 or compact in seen:
            continue
        seen.add(compact)
        lines.append(compact)
    return lines


def _line_looks_admin_notice(line: str) -> bool:
    normalized = normalize_whitespace(line)
    if not normalized:
        return False
    return any(pattern.search(normalized) for pattern in _ADMIN_NOTICE_LINE_PATTERNS)


def _extract_signal_lines(text: object, patterns: tuple[re.Pattern[str], ...], *, max_lines: int = 6) -> str:
    matched: list[str] = []
    for line in _iter_meaningful_lines(text):
        if _line_looks_admin_notice(line):
            continue
        if any(pattern.search(line) for pattern in patterns):
            matched.append(line)
        if len(matched) >= max_lines:
            break
    return sanitize_section_text("\n".join(matched))


def _derive_main_tasks_from_title(row: pd.Series) -> str:
    title = _first_nonempty_text(row.get("job_title_raw"), row.get("job_title_ko"), row.get("공고제목_표시"))
    if not title:
        return ""
    for pattern, template in _TITLE_MAIN_TASK_FALLBACKS:
        if pattern.search(title):
            return template
    return ""


def _focus_fallback_from_row(row: pd.Series) -> str:
    text = " ".join(
        value
        for value in (
            _first_nonempty_text(row.get("job_title_raw"), row.get("job_title_ko"), row.get("공고제목_표시")),
            _first_nonempty_text(row.get("job_role"), row.get("직무명_표시")),
            _normalized_cell(row.get("주요업무_분석용")),
            _normalized_cell(row.get("자격요건_분석용")),
            _normalized_cell(row.get("우대사항_분석용")),
        )
        if value
    )
    if not text:
        return ""
    for pattern, tag in _TITLE_FOCUS_FALLBACKS:
        if pattern.search(text):
            return tag
    return ""


def _admin_notice_density_high(*texts: object) -> bool:
    lines: list[str] = []
    for text in texts:
        lines.extend(_iter_meaningful_lines(text))
    if not lines:
        return False
    admin_count = sum(1 for line in lines if _line_looks_admin_notice(line))
    return admin_count >= 3 and admin_count >= max(2, len(lines) // 2)


def _recover_missing_analysis_fields(row: pd.Series) -> pd.Series:
    updated = row.copy()
    detail = _normalized_cell(updated.get("상세본문_분석용"))
    main_tasks = _normalized_cell(updated.get("주요업무_분석용"))
    requirements = _normalized_cell(updated.get("자격요건_분석용"))
    preferred = _normalized_cell(updated.get("우대사항_분석용"))
    title_main_tasks = _derive_main_tasks_from_title(updated)

    if not requirements:
        requirements = _extract_signal_lines(detail, _REQUIREMENT_SIGNAL_PATTERNS)
    if not requirements and preferred:
        requirements = _extract_signal_lines(preferred, _REQUIREMENT_SIGNAL_PATTERNS)
    if not main_tasks:
        main_tasks = title_main_tasks or _extract_signal_lines(detail, _MAIN_TASK_SIGNAL_PATTERNS)
    elif title_main_tasks and any(pattern.search(main_tasks) for pattern in _REQUIREMENT_SIGNAL_PATTERNS):
        main_tasks = title_main_tasks

    updated["주요업무_분석용"] = main_tasks
    updated["자격요건_분석용"] = requirements
    updated["우대사항_분석용"] = preferred
    return updated


def _practical_duplicate_group_key(row: pd.Series) -> tuple[str, ...]:
    return (
        _first_nonempty_text(row.get("company_name"), row.get("회사명_표시")),
        _normalized_cell(row.get("job_role")),
        *_analysis_content_signature(row),
    )


def _collapse_practical_duplicates(frame: pd.DataFrame, *, settings=None, paths=None) -> tuple[pd.DataFrame, pd.DataFrame]:
    if frame.empty:
        return frame.copy(), frame.iloc[0:0].copy()

    normalized = frame.copy()
    keep_indices: list[int] = []
    drop_indices: list[int] = []
    duplicate_budget = None
    if settings is not None and paths is not None and getattr(settings, "enable_gemini_duplicate_adjudication", False):
        duplicate_budget = GeminiBudget(max_calls=int(getattr(settings, "gemini_duplicate_max_calls_per_run", 0) or 0))

    normalized["_practical_duplicate_company"] = normalized.apply(
        lambda row: _first_nonempty_text(row.get("company_name"), row.get("회사명_표시")),
        axis=1,
    )
    normalized["_practical_duplicate_role"] = normalized.apply(
        lambda row: _first_nonempty_text(row.get("job_role"), row.get("직무명_표시")),
        axis=1,
    )

    grouped = normalized.groupby(
        ["_practical_duplicate_company", "_practical_duplicate_role"],
        dropna=False,
        sort=False,
    )

    for _, group in grouped:
        indices = list(group.index)
        if len(indices) <= 1:
            keep_indices.extend(indices)
            continue

        substantive_indices = [
            index
            for index in indices
            if any(
                _normalized_cell(group.loc[index].get(column))
                for column in ("주요업무_분석용", "자격요건_분석용", "상세본문_분석용")
            )
        ]
        if len(substantive_indices) <= 1:
            keep_indices.extend(indices)
            continue

        parent = {index: index for index in indices}

        def find(index: int) -> int:
            while parent[index] != index:
                parent[index] = parent[parent[index]]
                index = parent[index]
            return index

        def union(left: int, right: int) -> None:
            root_left = find(left)
            root_right = find(right)
            if root_left != root_right:
                parent[root_right] = root_left

        for offset, left in enumerate(indices):
            for right in indices[offset + 1 :]:
                if _rows_are_near_practical_duplicates(group.loc[left], group.loc[right]):
                    union(left, right)
                    continue
                if duplicate_budget is None:
                    continue
                body_seq, token_score, title_seq = _practical_duplicate_similarity(group.loc[left], group.loc[right])
                visible_detail_seq = _visible_detail_similarity(group.loc[left], group.loc[right])
                if not _should_request_duplicate_adjudication(
                    group.loc[left],
                    group.loc[right],
                    body_seq=body_seq,
                    token_score=token_score,
                    title_seq=title_seq,
                    visible_detail_seq=visible_detail_seq,
                ):
                    continue
                adjudicated = maybe_adjudicate_duplicate_pair(
                    group.loc[left].to_dict(),
                    group.loc[right].to_dict(),
                    {
                        "body_seq": body_seq,
                        "token_score": token_score,
                        "title_seq": title_seq,
                        "visible_detail_seq": visible_detail_seq,
                    },
                    settings,
                    paths,
                    duplicate_budget,
                )
                if adjudicated is True:
                    union(left, right)

        components: dict[int, list[int]] = {}
        for index in indices:
            components.setdefault(find(index), []).append(index)

        for component_indices in components.values():
            if len(component_indices) <= 1:
                keep_indices.extend(component_indices)
                continue
            keep_index = max(component_indices, key=lambda index: _practical_duplicate_keep_rank(group.loc[index]))
            location_hints = _group_location_hints(group.loc[component_indices])
            if len(location_hints) > 1:
                normalized.at[keep_index, "공고제목_표시"] = _merge_location_hints_into_title(
                    _normalized_cell(normalized.at[keep_index, "공고제목_표시"]),
                    location_hints,
                )
            keep_indices.append(keep_index)
            drop_indices.extend(index for index in component_indices if index != keep_index)

    kept = normalized.loc[sorted(keep_indices)].drop(
        columns=["_practical_duplicate_company", "_practical_duplicate_role"],
        errors="ignore",
    ).reset_index(drop=True)
    dropped = normalized.loc[sorted(drop_indices)].drop(
        columns=["_practical_duplicate_company", "_practical_duplicate_role"],
        errors="ignore",
    ).reset_index(drop=True)
    return kept, dropped


def _duplicate_job_url_count(frame: pd.DataFrame) -> int:
    if frame.empty or "job_url" not in frame.columns:
        return 0
    job_urls = frame["job_url"].fillna("").astype(str).str.strip()
    job_urls = job_urls[job_urls.ne("")]
    if job_urls.empty:
        return 0
    return int(job_urls.duplicated(keep=False).sum())


def _impossible_experience_count(frame: pd.DataFrame) -> int:
    if frame.empty or "경력수준_표시" not in frame.columns:
        return 0
    values = frame["경력수준_표시"].fillna("").astype(str).map(normalize_whitespace)
    return int(values.str.contains(_IMPOSSIBLE_EXPERIENCE_RE, regex=True, na=False).sum())


def _blank_ratio(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame.columns:
        return 1.0
    values = frame[column].fillna("").astype(str).map(normalize_whitespace)
    return float(values.eq("").mean())


def _placeholder_ratio(frame: pd.DataFrame, column: str, placeholder_values: set[str] | None = None) -> float:
    if frame.empty or column not in frame.columns:
        return 1.0
    placeholders = {normalize_whitespace(value) for value in (placeholder_values or set()) if normalize_whitespace(value)}
    values = frame[column].fillna("").astype(str).map(normalize_whitespace)
    return float((values.eq("") | values.isin(placeholders)).mean())


def _semantic_blank_ratio(frame: pd.DataFrame, column: str, placeholder_values: set[str] | None = None) -> float:
    if frame.empty or column not in frame.columns:
        return 1.0
    placeholders = {normalize_whitespace(value) for value in (placeholder_values or set()) if normalize_whitespace(value)}
    values = frame[column].fillna("").astype(str).map(normalize_whitespace)
    return float((values.eq("") | values.isin(placeholders)).mean())


def _track_signal_text(row: pd.Series) -> str:
    return normalize_whitespace(
        " ".join(
            filter(
                None,
                (
                    _normalized_cell(row.get("job_title_ko")),
                    _normalized_cell(row.get("job_title_raw")),
                    _normalized_cell(row.get("자격요건_분석용")),
                    _normalized_cell(row.get("우대사항_분석용")),
                    _normalized_cell(row.get("상세본문_분석용")),
                ),
            )
        )
    )


def _hiring_track_blank_with_cue_stats(frame: pd.DataFrame) -> tuple[float, float, int, int]:
    if frame.empty:
        return 1.0, 1.0, 0, 0

    blank_with_cue = 0
    cue_total = 0
    for _, row in frame.iterrows():
        cue = bool(_track_from_text(_track_signal_text(row), include_degrees=True))
        if not cue:
            continue
        cue_total += 1
        if _normalized_cell(row.get("채용트랙_표시")) == "":
            blank_with_cue += 1

    overall_ratio = float(blank_with_cue / len(frame))
    cue_miss_ratio = float(blank_with_cue / cue_total) if cue_total else 0.0
    return overall_ratio, cue_miss_ratio, blank_with_cue, cue_total


def _ratio_penalty(value: float, threshold: float, weight: float) -> float:
    if threshold <= 0 or value <= 0 or weight <= 0:
        return 0.0
    normalized = min(value / threshold, 1.0)
    return float(weight * (normalized**2))


def _shortfall_penalty(actual: float, minimum: float, weight: float) -> float:
    if minimum <= 0 or weight <= 0:
        return 0.0
    shortfall = max(0.0, minimum - actual)
    if shortfall <= 0:
        return 0.0
    normalized = min(shortfall / minimum, 1.0)
    return float(weight * (normalized**2))


def _quality_score_breakdown(
    *,
    english_leaks: int,
    dropped_low_quality_job_count: int,
    total_staging_jobs: int,
    duplicate_job_url_count: int,
    filtered_job_count: int,
    impossible_experience_count: int,
    main_task_blank_ratio: float,
    requirement_blank_ratio: float,
    preferred_blank_ratio: float,
    experience_placeholder_ratio: float,
    focus_blank_ratio: float,
    hiring_track_cue_blank_ratio: float,
    position_summary_blank_ratio: float,
    zero_role_count: int,
    success_rate: float,
    discovered_company_count: int,
) -> tuple[float, dict[str, float]]:
    total_jobs = max(total_staging_jobs, 1)
    filtered_jobs = max(filtered_job_count, 1)
    penalties = {
        "english_leaks": 25.0 if english_leaks > 0 else 0.0,
        "low_quality_drops": _ratio_penalty(
            dropped_low_quality_job_count / total_jobs,
            _DROPPED_LOW_QUALITY_RATIO_THRESHOLD,
            20.0,
        ),
        "duplicate_job_urls": _ratio_penalty(
            duplicate_job_url_count / filtered_jobs,
            _DUPLICATE_JOB_URL_RATIO_THRESHOLD,
            25.0,
        ),
        "impossible_experience": _ratio_penalty(
            impossible_experience_count / filtered_jobs,
            _IMPOSSIBLE_EXPERIENCE_RATIO_THRESHOLD,
            20.0,
        ),
        "main_task_blank": _ratio_penalty(main_task_blank_ratio, _MAIN_TASK_BLANK_THRESHOLD, 2.0),
        "requirement_blank": _ratio_penalty(requirement_blank_ratio, _REQUIREMENT_BLANK_THRESHOLD, 2.0),
        "preferred_blank": _ratio_penalty(preferred_blank_ratio, _PREFERRED_BLANK_THRESHOLD, 1.0),
        "experience_placeholder": _ratio_penalty(
            experience_placeholder_ratio,
            _EXPERIENCE_PLACEHOLDER_THRESHOLD,
            0.5,
        ),
        "focus_blank": _ratio_penalty(focus_blank_ratio, _FOCUS_BLANK_THRESHOLD, 1.25),
        "hiring_track_cue_blank": _ratio_penalty(hiring_track_cue_blank_ratio, _TRACK_CUE_BLANK_THRESHOLD, 2.25),
        "position_summary_blank": _ratio_penalty(
            position_summary_blank_ratio,
            _POSITION_SUMMARY_BLANK_THRESHOLD,
            2.0,
        ),
        "role_coverage": float(3.0 * ((zero_role_count / max(len(ALLOWED_JOB_ROLES), 1)) ** 2)),
        "source_success_rate": _shortfall_penalty(success_rate, 0.6, 6.0),
        "no_active_companies": 10.0 if discovered_company_count == 0 else 0.0,
    }
    score = max(0.0, 100.0 - sum(penalties.values()))
    return round(score, 2), {key: round(value, 4) for key, value in penalties.items()}


def normalize_job_analysis_fields(staging_jobs: pd.DataFrame) -> pd.DataFrame:
    if staging_jobs.empty:
        return staging_jobs.copy()

    normalized = staging_jobs.copy()
    if "source_url" in normalized.columns:
        normalized["source_url"] = normalized["source_url"].fillna("").astype(str).map(canonicalize_runtime_source_url)
    for column in (
        "회사명_표시",
        "소스명_표시",
        "공고제목_표시",
        "경력수준_표시",
        "경력근거_표시",
        "채용트랙_표시",
        "채용트랙근거_표시",
        "직무초점_표시",
        "직무초점근거_표시",
        "구분요약_표시",
        "직무명_표시",
    ):
        if column not in normalized.columns:
            normalized[column] = ""
    if "회사명_표시" in normalized.columns:
        normalized["회사명_표시"] = normalized.apply(
            lambda row: sanitize_name_or_title_text(_first_nonempty_text(row.get("company_name"), row.get("회사명_표시")), unknown_name=True),
            axis=1,
        )
    if "소스명_표시" in normalized.columns:
        normalized["소스명_표시"] = normalized.apply(
            lambda row: sanitize_name_or_title_text(_first_nonempty_text(row.get("source_name"), row.get("소스명_표시")), unknown_name=True),
            axis=1,
        )
    if "공고제목_표시" in normalized.columns:
        normalized["공고제목_표시"] = normalized.apply(
            lambda row: sanitize_name_or_title_text(
                _first_nonempty_text(row.get("job_title_ko"), row.get("job_title_raw"), row.get("공고제목_표시")),
                allow_english=True,
            ),
            axis=1,
        )
    recomputed_display_columns = {
        "경력수준_표시",
        "경력근거_표시",
        "채용트랙_표시",
        "채용트랙근거_표시",
        "직무초점_표시",
        "직무초점근거_표시",
        "구분요약_표시",
    } & set(normalized.columns)
    if recomputed_display_columns:
        display_recomputed = normalized.apply(
            lambda row: pd.Series(
                build_display_fields(
                    row.to_dict(),
                    analysis_fields={
                        "주요업무_분석용": row.get("주요업무_분석용", ""),
                        "자격요건_분석용": row.get("자격요건_분석용", ""),
                        "우대사항_분석용": row.get("우대사항_분석용", ""),
                        "핵심기술_분석용": row.get("핵심기술_분석용", ""),
                        "상세본문_분석용": row.get("상세본문_분석용", ""),
                    },
                )
            ),
            axis=1,
        )
        for column in recomputed_display_columns:
            normalized[column] = display_recomputed[column]
    if "직무명_표시" in normalized.columns:
        normalized["직무명_표시"] = normalized.apply(
            lambda row: sanitize_name_or_title_text(_first_nonempty_text(row.get("job_role"), row.get("직무명_표시"))),
            axis=1,
        )

    if "주요업무_분석용" in normalized.columns:
        normalized["주요업무_분석용"] = normalized.apply(_normalize_main_task_analysis, axis=1)
    for column in ("자격요건_분석용", "우대사항_분석용"):
        if column in normalized.columns:
            normalized[column] = normalized[column].fillna("").astype(str).map(sanitize_section_text)
    if "핵심기술_분석용" in normalized.columns:
        normalized["핵심기술_분석용"] = normalized["핵심기술_분석용"].fillna("").astype(str).map(sanitize_core_skill_text)
    if "상세본문_분석용" in normalized.columns:
        normalized["상세본문_분석용"] = normalized["상세본문_분석용"].fillna("").astype(str).map(sanitize_section_text)
        normalized["상세본문_분석용"] = normalized["상세본문_분석용"].map(
            lambda value: value if section_output_is_substantive(value) else ""
        )
        normalized["상세본문_분석용"] = normalized.apply(
            lambda row: ""
            if detail_prefers_structured_sections(
                row.get("상세본문_분석용"),
                row.get("주요업무_분석용"),
                row.get("자격요건_분석용"),
                row.get("우대사항_분석용"),
            )
            else row["상세본문_분석용"],
            axis=1,
        )
        normalized["상세본문_분석용"] = normalized.apply(
            lambda row: row["상세본문_분석용"]
            or compose_detail_fallback(
                row.get("주요업무_분석용"),
                row.get("자격요건_분석용"),
                row.get("우대사항_분석용"),
            ),
            axis=1,
        )
    if "우대사항_분석용" in normalized.columns and "상세본문_분석용" in normalized.columns:
        normalized["우대사항_분석용"] = normalized.apply(
            lambda row: row.get("우대사항_분석용")
            or _extract_section_from_raw_detail(row.get("상세본문_분석용"), _PREFERRED_HEADING_PATTERNS),
            axis=1,
        )
    normalized = normalized.apply(_recover_missing_analysis_fields, axis=1)
    if recomputed_display_columns:
        display_recomputed = normalized.apply(
            lambda row: pd.Series(
                build_display_fields(
                    row.to_dict(),
                    analysis_fields={
                        "주요업무_분석용": row.get("주요업무_분석용", ""),
                        "자격요건_분석용": row.get("자격요건_분석용", ""),
                        "우대사항_분석용": row.get("우대사항_분석용", ""),
                        "핵심기술_분석용": row.get("핵심기술_분석용", ""),
                        "상세본문_분석용": row.get("상세본문_분석용", ""),
                    },
                )
            ),
            axis=1,
        )
        for column in recomputed_display_columns:
            normalized[column] = display_recomputed[column]
    if "직무초점_표시" in normalized.columns:
        normalized["직무초점_표시"] = normalized.apply(
            lambda row: _normalized_cell(row.get("직무초점_표시")) or _focus_fallback_from_row(row),
            axis=1,
        )
    if "직무초점근거_표시" in normalized.columns:
        normalized["직무초점근거_표시"] = normalized.apply(
            lambda row: _normalized_cell(row.get("직무초점근거_표시")) or ("제목/요건추론" if _normalized_cell(row.get("직무초점_표시")) else ""),
            axis=1,
        )

    for analysis_column, display_column in (
        ("주요업무_분석용", "주요업무_표시"),
        ("자격요건_분석용", "자격요건_표시"),
        ("우대사항_분석용", "우대사항_표시"),
        ("핵심기술_분석용", "핵심기술_표시"),
    ):
        if analysis_column in normalized.columns and display_column in normalized.columns:
            normalized[display_column] = normalized[analysis_column]
    if "우대사항_표시" in normalized.columns:
        normalized["우대사항_표시"] = normalized["우대사항_표시"].fillna("").astype(str).map(normalize_whitespace)
        normalized["우대사항_표시"] = normalized["우대사항_표시"].replace("", _DEFAULT_PREFERRED_DISPLAY)

    return _disambiguate_duplicate_display_titles(normalized)


def filter_low_quality_jobs(staging_jobs: pd.DataFrame, *, settings=None, paths=None) -> tuple[pd.DataFrame, pd.DataFrame]:
    if staging_jobs.empty:
        return staging_jobs.copy(), staging_jobs.iloc[0:0].copy()

    normalized = normalize_job_analysis_fields(staging_jobs)
    keep_mask = ~normalized.apply(_row_has_low_quality_analysis, axis=1)
    filtered = normalized.loc[keep_mask].reset_index(drop=True)
    dropped = normalized.loc[~keep_mask].reset_index(drop=True)
    filtered, duplicate_dropped = _collapse_practical_duplicates(filtered, settings=settings, paths=paths)
    if not duplicate_dropped.empty:
        dropped = pd.concat([dropped, duplicate_dropped], ignore_index=True)
    return filtered, dropped


def evaluate_quality_gate(staging_jobs: pd.DataFrame, source_registry: pd.DataFrame, *, settings=None, paths=None, already_filtered: bool = False) -> GateResult:
    reasons: list[str] = []
    if already_filtered:
        filtered_jobs = staging_jobs.copy()
        dropped_jobs = staging_jobs.iloc[0:0].copy()
    else:
        filtered_jobs, dropped_jobs = filter_low_quality_jobs(staging_jobs, settings=settings, paths=paths)
    active_jobs = filtered_jobs[filtered_jobs["is_active"] == True] if not filtered_jobs.empty else filtered_jobs  # noqa: E712

    english_leaks = count_english_leaks(active_jobs)
    if english_leaks > 0:
        reasons.append("사용자 노출 필드에 영문 누수가 존재합니다.")

    if not dropped_jobs.empty:
        reasons.append("분석용 본문/섹션 품질 미달 공고가 존재합니다.")

    duplicate_job_url_count = _duplicate_job_url_count(filtered_jobs)
    if duplicate_job_url_count > 0:
        reasons.append("동일 job_url 중복 공고가 존재합니다.")

    impossible_experience_count = _impossible_experience_count(filtered_jobs)
    if impossible_experience_count > 0:
        reasons.append("경력수준 추출값에 비정상 연차가 포함되어 있습니다.")

    carry_forward_hold_only_count = 0
    if not active_jobs.empty and "record_status" in active_jobs.columns:
        carry_forward_hold_only_count = int(active_jobs["record_status"].fillna("").astype(str).eq("검증실패보류").sum())
        if carry_forward_hold_only_count == int(len(active_jobs)):
            reasons.append("이번 staging은 검증실패보류 상태만 포함합니다.")

    main_task_blank_ratio = _blank_ratio(filtered_jobs, "주요업무_표시")
    requirement_blank_ratio = _blank_ratio(filtered_jobs, "자격요건_표시")
    preferred_blank_ratio = _semantic_blank_ratio(filtered_jobs, "우대사항_표시", {_DEFAULT_PREFERRED_DISPLAY})
    experience_placeholder_ratio = _placeholder_ratio(filtered_jobs, "경력수준_표시", {_DEFAULT_EXPERIENCE_DISPLAY})
    preferred_placeholder_ratio = _placeholder_ratio(filtered_jobs, "우대사항_표시", {_DEFAULT_PREFERRED_DISPLAY})
    focus_blank_ratio = _blank_ratio(filtered_jobs, "직무초점_표시")
    hiring_track_blank_ratio = _blank_ratio(filtered_jobs, "채용트랙_표시")
    hiring_track_cue_blank_ratio, hiring_track_cue_miss_ratio, hiring_track_cue_blank_count, hiring_track_cue_total = _hiring_track_blank_with_cue_stats(filtered_jobs)
    position_summary_blank_ratio = _blank_ratio(filtered_jobs, "구분요약_표시")
    if main_task_blank_ratio > _MAIN_TASK_BLANK_THRESHOLD:
        reasons.append("주요업무 표시값 공란 비율이 너무 높습니다.")
    if requirement_blank_ratio > _REQUIREMENT_BLANK_THRESHOLD:
        reasons.append("자격요건 표시값 공란 비율이 너무 높습니다.")
    if preferred_blank_ratio > _PREFERRED_BLANK_THRESHOLD:
        reasons.append("우대사항 표시값 공란 비율이 너무 높습니다.")
    if experience_placeholder_ratio > _EXPERIENCE_PLACEHOLDER_THRESHOLD:
        reasons.append("경력수준 표시값 미기재 비율이 너무 높습니다.")
    if focus_blank_ratio > _FOCUS_BLANK_THRESHOLD:
        reasons.append("직무초점 표시값 공란 비율이 너무 높습니다.")
    if hiring_track_cue_blank_ratio > _TRACK_CUE_BLANK_THRESHOLD:
        reasons.append("채용트랙 신호가 있는데 표시값 누락 비율이 너무 높습니다.")
    if position_summary_blank_ratio > _POSITION_SUMMARY_BLANK_THRESHOLD:
        reasons.append("구분요약 표시값 공란 비율이 너무 높습니다.")

    role_counts = active_jobs["job_role"].value_counts().to_dict() if not active_jobs.empty else {}
    zero_role_count = sum(1 for role in ALLOWED_JOB_ROLES if role_counts.get(role, 0) == 0)
    if zero_role_count >= 2:
        reasons.append("허용 직무 4개 중 2개 이상이 0건입니다.")

    tier_counts = active_jobs["company_tier"].value_counts().to_dict() if not active_jobs.empty else {}
    if tier_counts.get("중견/중소", 0) == 0 and tier_counts.get("지역기업", 0) == 0:
        reasons.append("중견/중소와 지역기업 활성 공고가 모두 0건입니다.")

    eligible_sources = source_registry[source_registry["source_bucket"].isin(["approved", "candidate"])] if not source_registry.empty else source_registry
    if not eligible_sources.empty:
        success_rate = float((eligible_sources["verification_status"] == "성공").mean())
        if success_rate < 0.6:
            reasons.append("official domain 기반 소스 검증 성공률이 너무 낮습니다.")
    else:
        success_rate = 0.0
        reasons.append("discovery funnel 실패: 검증 대상 소스가 없습니다.")

    discovered_company_count = int(active_jobs["company_name"].nunique()) if not active_jobs.empty else 0
    if discovered_company_count == 0:
        reasons.append("discovery funnel 실패: 활성 공고가 없습니다.")

    quality_score_100, score_breakdown = _quality_score_breakdown(
        english_leaks=english_leaks,
        dropped_low_quality_job_count=int(len(dropped_jobs)),
        total_staging_jobs=int(len(staging_jobs)),
        duplicate_job_url_count=duplicate_job_url_count,
        filtered_job_count=int(len(filtered_jobs)),
        impossible_experience_count=impossible_experience_count,
        main_task_blank_ratio=main_task_blank_ratio,
        requirement_blank_ratio=requirement_blank_ratio,
        preferred_blank_ratio=preferred_blank_ratio,
        experience_placeholder_ratio=experience_placeholder_ratio,
        focus_blank_ratio=focus_blank_ratio,
        hiring_track_cue_blank_ratio=hiring_track_cue_blank_ratio,
        position_summary_blank_ratio=position_summary_blank_ratio,
        zero_role_count=zero_role_count,
        success_rate=success_rate,
        discovered_company_count=discovered_company_count,
    )
    if quality_score_100 < _QUALITY_SCORE_TARGET:
        reasons.append(f"품질 점수가 {_QUALITY_SCORE_TARGET:.0f}점 미만입니다.")

    metrics = {
        "english_leak_count": english_leaks,
        "role_counts": role_counts,
        "tier_counts": tier_counts,
        "official_source_success_rate": success_rate,
        "active_job_count": int(len(active_jobs)),
        "dropped_low_quality_job_count": int(len(dropped_jobs)),
        "duplicate_job_url_count": duplicate_job_url_count,
        "impossible_experience_count": impossible_experience_count,
        "carry_forward_hold_only_count": carry_forward_hold_only_count,
        "main_task_blank_ratio": main_task_blank_ratio,
        "requirement_blank_ratio": requirement_blank_ratio,
        "preferred_blank_ratio": preferred_blank_ratio,
        "experience_placeholder_ratio": experience_placeholder_ratio,
        "preferred_placeholder_ratio": preferred_placeholder_ratio,
        "focus_blank_ratio": focus_blank_ratio,
        "hiring_track_blank_ratio": hiring_track_blank_ratio,
        "hiring_track_cue_blank_ratio": hiring_track_cue_blank_ratio,
        "hiring_track_cue_miss_ratio": hiring_track_cue_miss_ratio,
        "hiring_track_cue_blank_count": hiring_track_cue_blank_count,
        "hiring_track_cue_total": hiring_track_cue_total,
        "position_summary_blank_ratio": position_summary_blank_ratio,
        "quality_score_target": _QUALITY_SCORE_TARGET,
        "quality_score_100": quality_score_100,
        "quality_score_penalties": score_breakdown,
    }
    return GateResult(passed=not reasons, reasons=reasons, metrics=metrics)


def write_quality_gate(result: GateResult, path) -> None:
    atomic_write_text(path, result.model_dump_json(indent=2))


def read_quality_gate(path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))
