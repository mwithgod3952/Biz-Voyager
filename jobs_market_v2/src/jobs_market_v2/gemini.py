"""Optional low-budget Gemini fallback for lossy section normalization."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

import httpx

from .constants import ALLOWED_JOB_ROLES
from .network import build_timeout
from .presentation import extract_core_skills_from_text, sanitize_section_text, section_loss_looks_high, section_output_looks_noisy
from .storage import atomic_write_text
from .utils import has_hangul, normalize_whitespace, stable_hash


SECTION_FIELDS = ("main_tasks", "requirements", "preferred", "core_skills")
ANALYSIS_FIELD_MAP = {
    "main_tasks": "주요업무_분석용",
    "requirements": "자격요건_분석용",
    "preferred": "우대사항_분석용",
    "core_skills": "핵심기술_분석용",
}

_BOILERPLATE_LINE_PATTERNS = (
    re.compile(r"^(recruitment process|application review|phone interview|onsite|offer)$", flags=re.IGNORECASE),
    re.compile(r"^(details to consider|privacy notice|document return policy|equal opportunities for all)$", flags=re.IGNORECASE),
    re.compile(r"^(전형 절차|전형절차|참고 사항|개인정보 처리방침|서류 반환 정책)$"),
    re.compile(r"^(privacy policy|application privacy notice)$", flags=re.IGNORECASE),
    re.compile(r"^https?://", flags=re.IGNORECASE),
    re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"),
    re.compile(r"^[-_=]{3,}$"),
    re.compile(r"^\[(korean|english)\]$", flags=re.IGNORECASE),
)
_MAX_SECTION_LINES = 10
_MAX_SECTION_CHARS = 1400
_ROLE_SALVAGE_HINT_PATTERNS = (
    re.compile(r"\b(ai|artificial intelligence|machine learning|ml|llm|nlp|computer vision|vision|speech|recommendation|ranking|search)\b", flags=re.IGNORECASE),
    re.compile(r"(인공지능|머신러닝|딥러닝|자연어|컴퓨터 비전|비전 모델|추천|검색|랭킹|데이터 사이언스|데이터사이언스|데이터 분석|데이터분석|데이터사이언티스트|모델 개발)"),
)
_GEMINI_DISPLAY_CACHE_VERSION = "v3"
_ENGLISH_PROSE_RE = re.compile(r"[A-Za-z]{3,}")


@dataclass
class GeminiBudget:
    max_calls: int
    used_calls: int = 0

    def can_call(self) -> bool:
        return self.used_calls < self.max_calls

    def consume(self) -> None:
        self.used_calls += 1


def needs_gemini_refinement(raw_text: str | None, normalized_text: str | None) -> bool:
    normalized = normalize_whitespace(normalized_text)
    english_only_leak = (
        bool(normalized)
        and not has_hangul(normalized)
        and len(_ENGLISH_PROSE_RE.findall(normalized)) >= 8
        and len(normalized) >= 80
    )
    return section_loss_looks_high(raw_text, normalized_text) or section_output_looks_noisy(normalized_text) or english_only_leak


def _load_cache(path) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _save_cache(path, cache: dict[str, dict[str, str]]) -> None:
    atomic_write_text(path, json.dumps(cache, ensure_ascii=False, indent=2))


def _extract_response_text(payload: dict) -> str:
    candidates = payload.get("candidates") or []
    if not candidates:
        return ""
    content = candidates[0].get("content") or {}
    parts = content.get("parts") or []
    if not parts:
        return ""
    return parts[0].get("text", "")


def _extract_chat_completions_text(payload: dict[str, Any]) -> str:
    choices = payload.get("choices") or []
    if not choices:
        return ""
    message = choices[0].get("message") or {}
    content = message.get("content")
    if isinstance(content, list):
        text_parts: list[str] = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                text = item.get("text")
                if isinstance(text, str):
                    text_parts.append(text)
        return "\n".join(text_parts)
    return content if isinstance(content, str) else ""


def _strip_json_fence(text: str) -> str:
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = stripped.strip("`")
        if "\n" in stripped:
            stripped = stripped.split("\n", 1)[1]
    if stripped.endswith("```"):
        stripped = stripped[:-3]
    return stripped.strip()


def _parse_response_json(text: str) -> dict[str, str]:
    stripped = _strip_json_fence(text)
    try:
        return json.loads(stripped or "{}")
    except json.JSONDecodeError:
        start = stripped.find("{")
        end = stripped.rfind("}")
        if start >= 0 and end > start:
            return json.loads(stripped[start : end + 1])
        raise


def _active_llm_provider(settings) -> str:
    provider = normalize_whitespace(getattr(settings, "llm_provider", "")).lower()
    if provider:
        return provider
    if normalize_whitespace(getattr(settings, "llm_base_url", "")) or normalize_whitespace(getattr(settings, "llm_api_key", "")):
        return "openai_compatible"
    return "gemini"


def _active_llm_api_key(settings) -> str:
    return normalize_whitespace(getattr(settings, "llm_api_key", "") or getattr(settings, "gemini_api_key", ""))


def _active_llm_model(settings) -> str:
    return normalize_whitespace(getattr(settings, "llm_model", "") or getattr(settings, "gemini_model", ""))


def _active_llm_base_url(settings) -> str:
    return normalize_whitespace(getattr(settings, "llm_base_url", ""))


def _resolve_chat_completions_url(base_url: str) -> str:
    normalized = normalize_whitespace(base_url)
    if not normalized:
        raise ValueError("Missing OpenAI-compatible LLM base URL")
    if normalized.endswith("/chat/completions"):
        return normalized
    if normalized.endswith("/v1"):
        return normalized.rstrip("/") + "/chat/completions"
    return normalized.rstrip("/") + "/v1/chat/completions"


def _call_json_llm(
    *,
    prompt: str,
    settings,
    temperature: float,
    max_output_tokens: int,
    gemini_fallback_model: str | None = None,
) -> dict[str, Any]:
    provider = _active_llm_provider(settings)
    api_key = _active_llm_api_key(settings)
    model = _active_llm_model(settings)
    if provider in {"", "gemini"}:
        model_candidates = [model, gemini_fallback_model]
        last_error: Exception | None = None
        for candidate_model in dict.fromkeys(model for model in model_candidates if model):
            try:
                response = httpx.post(
                    f"https://generativelanguage.googleapis.com/v1beta/models/{candidate_model}:generateContent",
                    params={"key": api_key},
                    json={
                        "contents": [{"parts": [{"text": prompt}]}],
                        "generationConfig": {
                            "temperature": temperature,
                            "maxOutputTokens": max_output_tokens,
                            "responseMimeType": "application/json",
                            "thinkingConfig": {"thinkingBudget": 0},
                        },
                    },
                    timeout=build_timeout(settings.gemini_timeout_seconds, getattr(settings, "connect_timeout_seconds", 5.0)),
                )
                response.raise_for_status()
                return _parse_response_json(_extract_response_text(response.json()))
            except httpx.HTTPStatusError as exc:
                last_error = exc
                if exc.response.status_code == 404:
                    continue
                raise
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                raise
        if last_error:
            raise last_error
        return {}

    if provider not in {"openai", "openai_compatible", "openai-compatible", "vibemakers"}:
        raise ValueError(f"Unsupported LLM provider: {provider}")

    response = httpx.post(
        _resolve_chat_completions_url(_active_llm_base_url(settings)),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": model,
            "messages": [
                {
                    "role": "system",
                    "content": "Return only a single JSON object that matches the user's requested schema. Do not include markdown fences or commentary.",
                },
                {"role": "user", "content": prompt},
            ],
            "max_tokens": max_output_tokens,
            "temperature": temperature,
        },
        timeout=build_timeout(settings.gemini_timeout_seconds, getattr(settings, "connect_timeout_seconds", 5.0)),
    )
    response.raise_for_status()
    return _parse_response_json(_extract_chat_completions_text(response.json()))


def _looks_like_boilerplate(line: str) -> bool:
    normalized = normalize_whitespace(line)
    if not normalized:
        return True
    return any(pattern.search(normalized) for pattern in _BOILERPLATE_LINE_PATTERNS)


def _prepare_section_for_gemini(value: str | None) -> str:
    text = normalize_whitespace(value)
    if not text:
        return ""

    raw_lines = [normalize_whitespace(line) for line in text.split("\n") if normalize_whitespace(line)]
    informative_lines = [line for line in raw_lines if not _looks_like_boilerplate(line)]
    if not informative_lines:
        return ""

    if sum(1 for line in informative_lines if has_hangul(line)) >= 2:
        informative_lines = [line for line in informative_lines if has_hangul(line)]

    deduped_lines: list[str] = []
    seen: set[str] = set()
    total_chars = 0
    for line in informative_lines:
        key = re.sub(r"\s+", " ", line).strip().lower()
        if key in seen:
            continue
        seen.add(key)
        if len(deduped_lines) >= _MAX_SECTION_LINES:
            break
        if total_chars + len(line) > _MAX_SECTION_CHARS:
            break
        deduped_lines.append(line)
        total_chars += len(line)

    return "\n".join(deduped_lines)


def _prepare_sections_for_gemini(raw_sections: dict[str, str]) -> dict[str, str]:
    prepared = {
        field: prepared
        for field, prepared in (
            (field, _prepare_section_for_gemini(raw_sections.get(field)))
            for field in SECTION_FIELDS
        )
        if prepared
    }
    if not prepared:
        description_context = _prepare_section_for_gemini(raw_sections.get("description_text"))
        if description_context:
            prepared["description_text"] = description_context
    return prepared


def _call_gemini(raw_sections: dict[str, str], settings) -> dict[str, str]:
    skill_hints = extract_core_skills_from_text(
        raw_sections.get("main_tasks"),
        raw_sections.get("requirements"),
        raw_sections.get("preferred"),
        raw_sections.get("core_skills"),
        raw_sections.get("description_text"),
    )
    prompt = f"""
아래 채용 공고 섹션을 한국어 분석용 텍스트로 정리하라.

제약:
- 의미를 최대한 보존하라
- HTML은 제거하라
- 영어 기술 용어는 가능한 자연스러운 한국어 표기로 바꿔라
- 절대 새 정보를 만들지 마라
- 모르면 빈 문자열을 반환하라
- JSON만 반환하라
- 같은 뜻의 중복 문장은 합쳐도 된다
- 채용 절차, 개인정보 처리방침, 법적 고지, 링크, 이메일은 제외하라
- core_skills는 핵심 기술명만 줄바꿈으로 반환하라
- 입력에 description_text만 있으면 본문만 보고 주요업무, 자격요건, 우대사항, core_skills를 복원하라
- 각 섹션은 핵심만 3~6줄로 간결하게 정리하라
- 문장형 섹션은 한 줄당 하나의 의미만 담고 너무 길어지지 않게 써라
- 회사 소개, 투자 현황, 홍보 문구, 복지 설명은 제외하라

반환 키:
- main_tasks
- requirements
- preferred
- core_skills

참고용 기술 힌트:
{skill_hints or ""}

입력:
{json.dumps(raw_sections, ensure_ascii=False)}
""".strip()

    return _call_json_llm(
        prompt=prompt,
        settings=settings,
        temperature=0.1,
        max_output_tokens=1024,
        gemini_fallback_model="gemini-2.5-flash",
    )


def maybe_refine_analysis_fields(raw_sections: dict[str, str], analysis_fields: dict[str, str], settings, paths, budget: GeminiBudget) -> dict[str, str]:
    if not settings.enable_gemini_fallback or not _active_llm_api_key(settings) or not _active_llm_model(settings):
        return analysis_fields
    if not budget.can_call():
        return analysis_fields

    candidate_sections = {
        field: normalize_whitespace(raw_sections.get(field))
        for field in SECTION_FIELDS
        if needs_gemini_refinement(raw_sections.get(field), analysis_fields.get(ANALYSIS_FIELD_MAP[field], ""))
    }
    has_blank_analysis = any(
        not normalize_whitespace(analysis_fields.get(ANALYSIS_FIELD_MAP[field], ""))
        for field in SECTION_FIELDS
    )
    if not candidate_sections and not has_blank_analysis:
        return analysis_fields
    payload_sections = {
        field: normalize_whitespace(raw_sections.get(field))
        for field in SECTION_FIELDS
    }
    description_text = normalize_whitespace(raw_sections.get("description_text"))
    if description_text:
        payload_sections["description_text"] = description_text
    prepared_sections = _prepare_sections_for_gemini(payload_sections or raw_sections)
    if not prepared_sections:
        return analysis_fields

    cache_path = paths.logs_dir / "gemini_display_cache.json"
    cache = _load_cache(cache_path)
    cache_key = stable_hash([_GEMINI_DISPLAY_CACHE_VERSION, *[prepared_sections.get(field, "") for field in (*SECTION_FIELDS, "description_text")]])
    if cache_key not in cache:
        budget.consume()
        try:
            cache[cache_key] = _call_gemini(prepared_sections, settings)
            _save_cache(cache_path, cache)
        except Exception:  # noqa: BLE001
            return analysis_fields

    refined = cache.get(cache_key, {})
    if not refined:
        return analysis_fields

    updated = analysis_fields.copy()
    for section_field, analysis_field in ANALYSIS_FIELD_MAP.items():
        candidate = refined.get(section_field)
        if not candidate:
            continue
        sanitized = sanitize_section_text(candidate)
        if sanitized:
            updated[analysis_field] = sanitized
    return updated


def _should_attempt_role_salvage(payload: dict[str, str]) -> bool:
    title_text = normalize_whitespace(" ".join(filter(None, (payload.get("title_raw"), payload.get("title_ko")))))
    body_text = normalize_whitespace(
        " ".join(
            filter(
                None,
                (
                    payload.get("main_tasks"),
                    payload.get("requirements"),
                    payload.get("preferred"),
                    payload.get("description_text"),
                ),
            )
        )
    )
    combined = normalize_whitespace(" ".join(filter(None, (title_text, body_text))))
    if not combined:
        return False
    return any(pattern.search(combined) for pattern in _ROLE_SALVAGE_HINT_PATTERNS)


def _prepare_role_salvage_payload(raw_sections: dict[str, str]) -> dict[str, str]:
    prepared = {
        "title_raw": normalize_whitespace(raw_sections.get("title_raw")),
        "title_ko": normalize_whitespace(raw_sections.get("title_ko")),
        "main_tasks": _prepare_section_for_gemini(raw_sections.get("main_tasks")),
        "requirements": _prepare_section_for_gemini(raw_sections.get("requirements")),
        "preferred": _prepare_section_for_gemini(raw_sections.get("preferred")),
        "description_text": _prepare_section_for_gemini(raw_sections.get("description_text")),
        "source_type": normalize_whitespace(raw_sections.get("source_type")),
    }
    return {key: value for key, value in prepared.items() if value}


def _call_gemini_role_salvage(payload: dict[str, str], settings) -> dict[str, Any]:
    prompt = f"""
아래 채용 공고를 허용 직무 4개 중 하나로만 분류하라.

규칙:
- 반드시 아래 네 가지 중 하나만 반환하거나, 불명확하면 빈 문자열을 반환하라
- 허용 직무: {", ".join(ALLOWED_JOB_ROLES)}
- 입력에 AI/ML/LLM/데이터 분석/데이터 사이언스/추천/검색/컴퓨터비전/연구 신호가 충분할 때만 분류하라
- title만 보지 말고 주요업무, 자격요건, 우대사항, 본문도 함께 보라
- 반도체 일반 설계, 일반 영업, 일반 기획, 일반 운영, 일반 마케팅은 분류하지 마라
- JSON만 반환하라

반환 키:
- job_role
- confidence
- reason

입력:
{json.dumps(payload, ensure_ascii=False)}
""".strip()

    return _call_json_llm(
        prompt=prompt,
        settings=settings,
        temperature=0.0,
        max_output_tokens=256,
    )


def maybe_salvage_job_role(
    raw_sections: dict[str, str],
    settings,
    paths,
    budget: GeminiBudget,
) -> str:
    if not settings.enable_gemini_fallback:
        return ""
    if not _active_llm_api_key(settings) or not _active_llm_model(settings) or not budget.can_call():
        return ""
    if not _should_attempt_role_salvage(raw_sections):
        return ""

    payload = _prepare_role_salvage_payload(raw_sections)
    if not payload:
        return ""

    cache_path = paths.logs_dir / "gemini_role_salvage_cache.json"
    cache = _load_cache(cache_path)
    cache_key = stable_hash(
        [
            payload.get("title_raw", ""),
            payload.get("title_ko", ""),
            payload.get("main_tasks", ""),
            payload.get("requirements", ""),
            payload.get("preferred", ""),
            payload.get("description_text", ""),
            payload.get("source_type", ""),
        ]
    )

    if cache_key not in cache:
        budget.consume()
        try:
            cache[cache_key] = _call_gemini_role_salvage(payload, settings)
            _save_cache(cache_path, cache)
        except Exception:  # noqa: BLE001
            return ""

    decision = cache.get(cache_key, {}) or {}
    candidate = normalize_whitespace(decision.get("job_role"))
    try:
        confidence = float(decision.get("confidence", 0.0))
    except (TypeError, ValueError):
        confidence = 0.0
    if confidence < 0.7:
        return ""
    if candidate in ALLOWED_JOB_ROLES:
        return candidate
    return ""


_DUPLICATE_COMPARE_FIELDS = (
    "company_name",
    "job_role",
    "source_type",
    "source_name",
    "source_url",
    "job_url",
    "job_key",
    "job_title_raw",
    "job_title_ko",
    "공고제목_표시",
    "구분요약_표시",
    "경력수준_표시",
    "채용트랙_표시",
    "직무초점_표시",
    "주요업무_분석용",
    "자격요건_분석용",
    "우대사항_분석용",
    "핵심기술_분석용",
    "상세본문_분석용",
)

_DUPLICATE_ADJUDICATION_POLICY_VERSION = "service_duplicate_v3"


def _normalized_duplicate_field(value: Any) -> str:
    return normalize_whitespace("" if value is None else str(value))


def _prepare_duplicate_row_for_gemini(row: dict[str, Any]) -> dict[str, str]:
    prepared: dict[str, str] = {}
    for field in _DUPLICATE_COMPARE_FIELDS:
        value = _normalized_duplicate_field(row.get(field))
        if not value:
            continue
        if field in {"주요업무_분석용", "자격요건_분석용", "우대사항_분석용", "핵심기술_분석용", "상세본문_분석용"}:
            value = _prepare_section_for_gemini(value)
        if value:
            prepared[field] = value
    return prepared


def _prepare_duplicate_adjudication_payload(
    left_row: dict[str, Any],
    right_row: dict[str, Any],
    metrics: dict[str, float],
) -> dict[str, Any]:
    return {
        "policy_version": _DUPLICATE_ADJUDICATION_POLICY_VERSION,
        "left": _prepare_duplicate_row_for_gemini(left_row),
        "right": _prepare_duplicate_row_for_gemini(right_row),
        "metrics": {
            "body_seq": round(float(metrics.get("body_seq", 0.0)), 4),
            "token_score": round(float(metrics.get("token_score", 0.0)), 4),
            "title_seq": round(float(metrics.get("title_seq", 0.0)), 4),
            "visible_detail_seq": round(float(metrics.get("visible_detail_seq", 0.0)), 4),
        },
    }


def _call_gemini_duplicate_adjudication(payload: dict[str, Any], settings) -> dict[str, Any]:
    prompt = f"""
아래 두 채용 공고가 서비스 노출 관점에서 사실상 같은 공고군인지 판정하라.

판정 원칙:
- 사용자가 서비스에서 봤을 때 사실상 같은 채용 기회라면 same_posting=true
- 특히 사용자가 실제로 보는 `상세본문_분석용`이 거의 같고 같은 회사/같은 역할군이면 same_posting=true 쪽으로 강하게 본다
- 제목, 공고번호, URL, 요약, 전문연구요원/인턴/학사/석사/박사 표기, Senior/Staff 같은 레벨 표기만 다르고
  주요업무/자격요건/상세본문이 사실상 같으면 same_posting=true 쪽으로 본다
- 반대로 팀/도메인/핵심 문제영역/주요업무/자격요건이 substantively 다르면 same_posting=false
- 같은 역할군이라도 책임 범위나 자격요건이 실질적으로 다르면 same_posting=false
- 확신이 낮으면 same_posting=false
- 절대 추측으로 합치지 말고, 본문과 요구사항 중심으로 판단하라
- JSON만 반환하라

반환 키:
- same_posting: boolean
- confidence: number (0~1)
- reason: string

입력:
{json.dumps(payload, ensure_ascii=False)}
""".strip()

    return _call_json_llm(
        prompt=prompt,
        settings=settings,
        temperature=0.0,
        max_output_tokens=512,
    )


def maybe_adjudicate_duplicate_pair(
    left_row: dict[str, Any],
    right_row: dict[str, Any],
    metrics: dict[str, float],
    settings,
    paths,
    budget: GeminiBudget,
) -> bool | None:
    if not getattr(settings, "enable_gemini_duplicate_adjudication", False):
        return None
    if not _active_llm_api_key(settings) or not _active_llm_model(settings) or not budget.can_call():
        return None

    payload = _prepare_duplicate_adjudication_payload(left_row, right_row, metrics)
    cache_path = paths.logs_dir / "gemini_duplicate_adjudication_cache.json"
    cache = _load_cache(cache_path)
    cache_key = stable_hash(
        [
            payload["policy_version"],
            _active_llm_provider(settings),
            normalize_whitespace(_active_llm_model(settings)),
            json.dumps(payload["left"], ensure_ascii=False, sort_keys=True),
            json.dumps(payload["right"], ensure_ascii=False, sort_keys=True),
            json.dumps(payload["metrics"], ensure_ascii=False, sort_keys=True),
        ]
    )

    if cache_key not in cache:
        budget.consume()
        try:
            cache[cache_key] = _call_gemini_duplicate_adjudication(payload, settings)
            _save_cache(cache_path, cache)
        except Exception:  # noqa: BLE001
            return None

    decision = cache.get(cache_key, {}) or {}
    same_posting = decision.get("same_posting")
    try:
        confidence = float(decision.get("confidence", 0.0))
    except (TypeError, ValueError):
        confidence = 0.0
    if confidence < 0.65:
        return None
    if isinstance(same_posting, bool):
        return same_posting
    if isinstance(same_posting, str):
        normalized = same_posting.strip().lower()
        if normalized in {"true", "yes", "y"}:
            return True
        if normalized in {"false", "no", "n"}:
            return False
    return None
