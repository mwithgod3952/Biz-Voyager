"""macOS Vision OCR helper for image-heavy recruiter postings."""

from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import httpx

from .network import build_timeout
from .utils import normalize_whitespace

_OCR_CACHE: dict[str, str] = {}
_SUPPORTED_SUFFIXES = (".pdf", ".png", ".jpg", ".jpeg", ".webp")


def _normalize_multiline_text(value: str) -> str:
    lines = [normalize_whitespace(line) for line in value.splitlines() if normalize_whitespace(line)]
    return "\n".join(lines)


def _supports_macos_ocr() -> bool:
    return sys.platform == "darwin" and shutil.which("swift") is not None


def _looks_like_supported_asset(url: str) -> bool:
    lowered = normalize_whitespace(url).lower()
    return any(suffix in lowered for suffix in _SUPPORTED_SUFFIXES)


def _script_path() -> Path:
    return Path(__file__).with_name("macos_ocr.swift")


def extract_text_from_asset_urls(
    asset_urls: list[str],
    *,
    user_agent: str,
    timeout_seconds: float,
    connect_timeout_seconds: float,
    max_assets: int = 3,
    command_timeout_seconds: float = 45.0,
) -> str:
    if not _supports_macos_ocr():
        return ""

    texts: list[str] = []
    for url in asset_urls:
        if len(texts) >= max(max_assets, 0):
            break
        normalized_url = normalize_whitespace(url)
        if not normalized_url or not _looks_like_supported_asset(normalized_url):
            continue
        if normalized_url in _OCR_CACHE:
            cached = _OCR_CACHE[normalized_url]
            if cached:
                texts.append(cached)
            continue
        try:
            response = httpx.get(
                normalized_url,
                follow_redirects=True,
                headers={"User-Agent": user_agent},
                timeout=build_timeout(timeout_seconds, connect_timeout_seconds),
            )
            response.raise_for_status()
        except Exception:  # noqa: BLE001
            _OCR_CACHE[normalized_url] = ""
            continue

        suffix = next((candidate for candidate in _SUPPORTED_SUFFIXES if candidate in normalized_url.lower()), ".bin")
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as handle:
            handle.write(response.content)
            local_path = Path(handle.name)

        try:
            completed = subprocess.run(
                ["swift", str(_script_path()), str(local_path)],
                capture_output=True,
                text=True,
                timeout=command_timeout_seconds,
                check=False,
            )
            recovered = _normalize_multiline_text(completed.stdout)
        except Exception:  # noqa: BLE001
            recovered = ""
        finally:
            local_path.unlink(missing_ok=True)

        _OCR_CACHE[normalized_url] = recovered
        if recovered:
            texts.append(recovered)

    unique_texts: list[str] = []
    seen: set[str] = set()
    for text in texts:
        if not text or text in seen:
            continue
        seen.add(text)
        unique_texts.append(text)
    return "\n".join(unique_texts)
