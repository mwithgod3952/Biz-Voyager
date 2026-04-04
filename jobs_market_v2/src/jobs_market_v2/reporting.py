"""Coverage reporting."""

from __future__ import annotations

import json

import pandas as pd

from .constants import ALLOWED_JOB_ROLES, COMPANY_TIERS
from .storage import atomic_write_text


def _top5_concentration(active_jobs: pd.DataFrame) -> float:
    if active_jobs.empty:
        return 0.0
    counts = active_jobs["company_name"].value_counts()
    total = counts.sum()
    return round(float(counts.head(5).sum() / total), 4) if total else 0.0


def _hhi(active_jobs: pd.DataFrame) -> float:
    if active_jobs.empty:
        return 0.0
    shares = active_jobs["company_name"].value_counts(normalize=True)
    return round(float((shares**2).sum()), 4)


def build_coverage_report(companies: pd.DataFrame, source_registry: pd.DataFrame, jobs: pd.DataFrame, *, non_company_removed_count: int = 0) -> dict:
    active_jobs = jobs[jobs["is_active"] == True] if not jobs.empty else jobs  # noqa: E712
    verified_sources = source_registry[source_registry["verification_status"] == "성공"] if not source_registry.empty else source_registry

    tier_company_counts = companies["company_tier"].value_counts().to_dict() if not companies.empty else {}
    official_success_by_tier = (
        verified_sources.groupby("company_tier")["source_url"].nunique().to_dict() if not verified_sources.empty else {}
    )
    source_type_success_rate = {}
    if not source_registry.empty:
        grouped = source_registry.groupby("source_type")["verification_status"].apply(lambda series: float((series == "성공").mean()))
        source_type_success_rate = {key: round(value, 4) for key, value in grouped.to_dict().items()}

    record_status_counts = active_jobs["record_status"].value_counts().to_dict() if not active_jobs.empty else {}
    report = {
        "발견 기업 수": int(companies["company_name"].nunique()) if not companies.empty else 0,
        "층별 발견 기업 수": {tier: int(tier_company_counts.get(tier, 0)) for tier in COMPANY_TIERS},
        "공식 도메인 검증 성공 수": int(companies[companies["official_domain_confidence"] >= 0.7]["company_name"].nunique()) if not companies.empty else 0,
        "층별 공식 도메인 검증 성공 수": {tier: int(official_success_by_tier.get(tier, 0)) for tier in COMPANY_TIERS},
        "공식 채용 소스 검증 성공 수": int(verified_sources["source_url"].nunique()) if not verified_sources.empty else 0,
        "discovery source 유형별 성공률": source_type_success_rate,
        "비기업 엔티티 제거 수": int(non_company_removed_count),
        "quarantine source 수": int(source_registry["is_quarantined"].sum()) if not source_registry.empty else 0,
        "활성 공고 수": int(len(active_jobs)),
        "직무별 공고 수": {role: int(active_jobs["job_role"].eq(role).sum()) for role in ALLOWED_JOB_ROLES},
        "신규/유지/변경/미발견 수": {
            "신규": int(record_status_counts.get("신규", 0)),
            "유지": int(record_status_counts.get("유지", 0)),
            "변경": int(record_status_counts.get("변경", 0)),
            "미발견": int(jobs["record_status"].eq("미발견").sum()) if not jobs.empty else 0,
        },
        "HHI": _hhi(active_jobs),
        "top5 집중도": _top5_concentration(active_jobs),
    }
    return report


def write_coverage_report(report: dict, path) -> None:
    atomic_write_text(path, json.dumps(report, ensure_ascii=False, indent=2))
