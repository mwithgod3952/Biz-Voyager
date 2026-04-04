"""Shared network helpers."""

from __future__ import annotations

import httpx


def build_timeout(total_timeout_seconds: float, connect_timeout_seconds: float | None = None) -> httpx.Timeout:
    total = max(float(total_timeout_seconds or 0), 1.0)
    if connect_timeout_seconds is None:
        connect = min(5.0, total)
    else:
        connect = max(min(float(connect_timeout_seconds), total), 0.5)
    return httpx.Timeout(total, connect=connect)
