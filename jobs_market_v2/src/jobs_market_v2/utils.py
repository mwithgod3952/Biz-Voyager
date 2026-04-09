"""Utility helpers."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from zoneinfo import ZoneInfo

import yaml


_MULTISPACE_RE = re.compile(r"[ \t\r\f\v]+")
_ENGLISH_RE = re.compile(r"[A-Za-z]")
_HANGUL_RE = re.compile(r"[가-힣]")
_ROOT_CANONICAL_SOURCE_HOST_HINTS = (
    "career.greetinghr.com",
    "recruiter.co.kr",
)
_GREETINGHR_PATH_PRESERVE_HINTS = ("guide", "jobs", "positions")


def normalize_whitespace(value: object | None) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return ""
    value_text = text.replace("\u00a0", " ").replace("\r\n", "\n").replace("\r", "\n")
    lines = [_MULTISPACE_RE.sub(" ", line).strip() for line in value_text.split("\n")]
    return "\n".join(line for line in lines if line)


def has_hangul(value: str | None) -> bool:
    return bool(value and _HANGUL_RE.search(value))


def contains_english(value: str | None) -> bool:
    return bool(value and _ENGLISH_RE.search(value))


def extract_domain(url: str | None) -> str:
    if not url:
        return ""
    url_text = str(url).strip()
    if not url_text or url_text.lower() == "nan":
        return ""
    match = re.match(r"^[a-zA-Z]+://([^/]+)", url_text)
    if not match:
        return ""
    return match.group(1).lower()


def strip_protocol(url_or_domain: str | None) -> str:
    if not url_or_domain:
        return ""
    text = str(url_or_domain).strip()
    if not text or text.lower() == "nan":
        return ""
    return extract_domain(text) or text.lower().removeprefix("www.")


def canonicalize_runtime_source_url(url: str | None) -> str:
    normalized = normalize_whitespace(url)
    if not normalized:
        return ""
    parts = urlsplit(normalized)
    if not parts.scheme or not parts.netloc:
        return normalized.rstrip("/")
    path = parts.path.rstrip("/")
    if path.endswith("/index"):
        path = path[: -len("/index")]
    host = parts.netloc.lower()
    if host.endswith("career.greetinghr.com"):
        lowered_path = path.lower()
        if not any(f"/{hint}" in lowered_path for hint in _GREETINGHR_PATH_PRESERVE_HINTS):
            path = ""
    elif any(hint in host for hint in _ROOT_CANONICAL_SOURCE_HOST_HINTS):
        path = ""
    return urlunsplit((parts.scheme, parts.netloc, path, parts.query, "")).rstrip("/")


def stable_hash(parts: list[str | int | None]) -> str:
    joined = "||".join("" if part is None else str(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def now_kst_iso() -> str:
    return datetime.now(tz=ZoneInfo("Asia/Seoul")).replace(microsecond=0).isoformat()


def today_kst() -> str:
    return datetime.now(tz=ZoneInfo("Asia/Seoul")).date().isoformat()


def load_yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def dump_json(data: object) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True)


def is_person_like(name: str, common_surnames: set[str]) -> bool:
    value = normalize_whitespace(name)
    if len(value) != 3:
        return False
    return has_hangul(value) and value[0] in common_surnames


def parse_aliases(value: object) -> list[str]:
    if value is None or value == "":
        return []
    if isinstance(value, list):
        return [normalize_whitespace(str(item)) for item in value if normalize_whitespace(str(item))]
    text = str(value).replace("|", ";")
    return [normalize_whitespace(part) for part in text.split(";") if normalize_whitespace(part)]
