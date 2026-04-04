"""Company candidate generation, evidence collection, and screening."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from .collection import collect_jobs_from_sources
from .constants import (
    COMPANY_CANDIDATE_COLUMNS,
    COMPANY_EVIDENCE_COLUMNS,
    IMPORT_COMPANY_COLUMNS,
    SOURCE_REGISTRY_COLUMNS,
)
from .discovery import discover_companies, discover_source_candidates
from .screening import screen_sources
from .storage import append_deduplicated, atomic_write_text, read_csv_or_empty, write_csv
from .utils import normalize_whitespace

_ROLE_SIGNAL_COLUMNS = {
    "데이터 분석가": "role_analyst_signal",
    "데이터 사이언티스트": "role_ds_signal",
    "인공지능 리서처": "role_researcher_signal",
    "인공지능 엔지니어": "role_ai_engineer_signal",
}

_STRUCTURED_SOURCE_TYPES = {
    "greenhouse",
    "lever",
    "greetinghr",
    "recruiter",
    "json_api",
    "rss",
    "rss_feed",
    "xml",
}


def _now() -> str:
    return datetime.now(tz=ZoneInfo("Asia/Seoul")).replace(microsecond=0).isoformat()


def _base_company_candidates(paths) -> pd.DataFrame:
    companies = read_csv_or_empty(paths.companies_registry_path, IMPORT_COMPANY_COLUMNS)
    if companies.empty:
        companies, _ = discover_companies(paths)
    frame = companies.copy()
    for column in COMPANY_CANDIDATE_COLUMNS:
        if column not in frame.columns:
            frame[column] = ""
    for column in ("officiality_score", "source_seed_count", "verified_source_count", "active_job_count", "role_fit_score", "hiring_signal_score", "evidence_count"):
        frame[column] = 0
    for column in _ROLE_SIGNAL_COLUMNS.values():
        frame[column] = 0
    frame["aliases"] = frame["aliases"].fillna("")
    return frame[list(COMPANY_CANDIDATE_COLUMNS)]


def _source_registry_for_company_screening(companies: pd.DataFrame, paths, settings) -> pd.DataFrame:
    source_candidates = discover_source_candidates(companies, paths, settings=settings)
    _, _, _, registry = screen_sources(source_candidates)
    existing_registry = read_csv_or_empty(paths.source_registry_path, SOURCE_REGISTRY_COLUMNS)
    if not existing_registry.empty and not registry.empty:
        existing_registry = existing_registry.drop_duplicates(subset=["source_url"], keep="last")
        existing_state = existing_registry.set_index("source_url")
        for state_column in ("verification_status", "failure_count", "last_success_at", "last_active_job_count", "quarantine_reason", "is_quarantined"):
            if state_column in existing_state.columns:
                registry[state_column] = registry["source_url"].map(existing_state[state_column]).combine_first(registry[state_column])
    for column in SOURCE_REGISTRY_COLUMNS:
        if column not in registry.columns:
            registry[column] = ""
    return registry[list(SOURCE_REGISTRY_COLUMNS)]


def _source_summary(source_registry: pd.DataFrame) -> dict[str, dict[str, int]]:
    summary: dict[str, dict[str, int]] = defaultdict(
        lambda: {
            "source_seed_count": 0,
            "verified_source_count": 0,
            "verified_structured_source_count": 0,
        }
    )
    for row in source_registry.fillna("").to_dict(orient="records"):
        company_name = normalize_whitespace(row.get("company_name"))
        if not company_name:
            continue
        summary[company_name]["source_seed_count"] += 1
        if row.get("verification_status") == "성공":
            summary[company_name]["verified_source_count"] += 1
            if normalize_whitespace(row.get("source_type")).lower() in _STRUCTURED_SOURCE_TYPES:
                summary[company_name]["verified_structured_source_count"] += 1
    return summary


def _job_summary(jobs_frame: pd.DataFrame) -> dict[str, dict[str, int]]:
    summary: dict[str, dict[str, int]] = defaultdict(
        lambda: {
            "active_job_count": 0,
            "role_analyst_signal": 0,
            "role_ds_signal": 0,
            "role_researcher_signal": 0,
            "role_ai_engineer_signal": 0,
        }
    )
    if jobs_frame.empty:
        return summary
    active_jobs = jobs_frame[jobs_frame["is_active"] == True] if "is_active" in jobs_frame.columns else jobs_frame  # noqa: E712
    for row in active_jobs.fillna("").to_dict(orient="records"):
        company_name = normalize_whitespace(row.get("company_name"))
        if not company_name:
            continue
        summary[company_name]["active_job_count"] += 1
        role = row.get("job_role")
        role_column = _ROLE_SIGNAL_COLUMNS.get(role)
        if role_column:
            summary[company_name][role_column] += 1
    return summary


def _company_bucket(
    officiality_score: int,
    source_seed_count: int,
    verified_source_count: int,
    verified_structured_source_count: int,
    active_job_count: int,
    role_fit_score: int,
    has_candidate_seed_provenance: bool,
) -> tuple[str, str]:
    if has_candidate_seed_provenance and officiality_score >= 3 and verified_source_count >= 1 and active_job_count >= 1 and role_fit_score >= 3:
        return "approved", ""
    if has_candidate_seed_provenance and officiality_score >= 4 and verified_structured_source_count >= 1:
        return "approved", ""
    if has_candidate_seed_provenance and officiality_score >= 4 and verified_source_count >= 2:
        return "approved", ""
    if officiality_score >= 2 and (source_seed_count >= 1 or verified_source_count >= 1 or active_job_count >= 1):
        return "candidate", ""
    if officiality_score >= 2:
        return "candidate", ""
    return "rejected", "공식 도메인 또는 공식 채용 소스 근거가 부족합니다."


def _primary_evidence_rows(evidence_frame: pd.DataFrame) -> dict[str, dict[str, str]]:
    rows: dict[str, dict[str, str]] = {}
    if evidence_frame.empty:
        return rows
    sorted_frame = evidence_frame.sort_values(by=["evidence_strength", "company_name"], ascending=[False, True])
    for row in sorted_frame.fillna("").to_dict(orient="records"):
        company_name = row["company_name"]
        if company_name in rows:
            continue
        rows[company_name] = {
            "primary_evidence_type": row.get("evidence_type", ""),
            "primary_evidence_url": row.get("evidence_url", ""),
            "primary_evidence_text": row.get("evidence_text", ""),
        }
    return rows


def _checkpoint_dir(paths, run_id: str) -> Path:
    checkpoint_dir = paths.runtime_dir / "checkpoints" / run_id
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    return checkpoint_dir


def _company_evidence_working_paths(paths) -> dict[str, Path]:
    return {
        "candidates": paths.runtime_dir / "company_candidates_in_progress.csv",
        "evidence": paths.runtime_dir / "company_evidence_in_progress.csv",
        "registry": paths.runtime_dir / "source_registry_in_progress.csv",
    }


def _load_working_frame(published_path: Path, working_path: Path, columns) -> pd.DataFrame:
    if working_path.exists() and working_path.stat().st_size > 0:
        return read_csv_or_empty(working_path, columns)
    return read_csv_or_empty(published_path, columns)


def _clear_working_company_evidence(paths) -> None:
    for path in _company_evidence_working_paths(paths).values():
        if path.exists():
            path.unlink()


def _load_company_evidence_progress(paths) -> dict[str, int | str | bool]:
    path = paths.company_evidence_progress_path
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _write_company_evidence_progress(paths, payload: dict[str, int | str | bool]) -> None:
    atomic_write_text(
        paths.company_evidence_progress_path,
        json.dumps(payload, ensure_ascii=False, indent=2),
    )


def _resume_company_evidence_offset(progress: dict[str, int | str | bool], companies: pd.DataFrame, batch_size: int) -> int:
    if not progress or companies.empty:
        return 0

    if int(progress.get("batch_size", 0) or 0) != int(batch_size or 0):
        return 0

    candidate_company_count = int(progress.get("candidate_company_count", 0) or 0)
    next_offset = int(progress.get("next_offset", 0) or 0)
    if candidate_company_count == len(companies):
        return 0 if next_offset >= len(companies) else next_offset

    if bool(progress.get("completed_full_scan")):
        return 0

    last_company_name = normalize_whitespace(str(progress.get("last_company_name") or ""))
    if not last_company_name:
        return 0

    matches = companies.index[companies["company_name"].fillna("").astype(str) == last_company_name]
    if len(matches) == 0:
        return 0

    start_offset = int(matches[-1]) + 1
    return 0 if start_offset >= len(companies) else start_offset


def _batch_company_frames(companies: pd.DataFrame, batch_size: int) -> list[pd.DataFrame]:
    if companies.empty:
        return []
    if batch_size <= 0 or len(companies) <= batch_size:
        return [companies.reset_index(drop=True)]
    frames: list[pd.DataFrame] = []
    for start in range(0, len(companies), batch_size):
        frames.append(companies.iloc[start : start + batch_size].reset_index(drop=True))
    return frames


def _collect_company_evidence_batch(
    companies: pd.DataFrame,
    paths,
    settings,
    *,
    run_id: str,
    collected_at: str,
    snapshot_date: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    source_registry = _source_registry_for_company_screening(companies, paths, settings)
    jobs, _, updated_registry, collection_summary = collect_jobs_from_sources(
        source_registry,
        paths,
        settings,
        run_id=run_id,
        snapshot_date=snapshot_date,
        collected_at=collected_at,
        enable_recruiter_ocr_recovery=False,
    )

    source_summary = _source_summary(updated_registry)
    job_summary = _job_summary(jobs)
    evidence_rows: list[dict] = []

    for company in companies.fillna("").to_dict(orient="records"):
        company_name = company["company_name"]
        company_tier = company.get("company_tier", "")
        candidate_seed_url = normalize_whitespace(company.get("candidate_seed_url"))
        candidate_seed_title = normalize_whitespace(company.get("candidate_seed_title"))
        candidate_seed_type = normalize_whitespace(company.get("candidate_seed_type"))
        candidate_seed_reason = normalize_whitespace(company.get("candidate_seed_reason"))
        if candidate_seed_url or candidate_seed_title or candidate_seed_reason:
            evidence_rows.append(
                {
                    "company_name": company_name,
                    "company_tier": company_tier,
                    "evidence_type": "후보시드근거",
                    "evidence_url": candidate_seed_url,
                    "evidence_text": normalize_whitespace(
                        " / ".join(part for part in (candidate_seed_type, candidate_seed_title, candidate_seed_reason) if part)
                    ),
                    "role_analyst_signal": 0,
                    "role_ds_signal": 0,
                    "role_researcher_signal": 0,
                    "role_ai_engineer_signal": 0,
                    "evidence_strength": 2,
                    "captured_at": collected_at,
                }
            )
        official_domain = normalize_whitespace(company.get("official_domain"))
        if official_domain:
            evidence_rows.append(
                {
                    "company_name": company_name,
                    "company_tier": company_tier,
                    "evidence_type": "공식도메인",
                    "evidence_url": f"https://{official_domain}",
                    "evidence_text": f"공식 도메인 확인: {official_domain}",
                    "role_analyst_signal": 0,
                    "role_ds_signal": 0,
                    "role_researcher_signal": 0,
                    "role_ai_engineer_signal": 0,
                    "evidence_strength": 1,
                    "captured_at": collected_at,
                }
            )

    for row in updated_registry.fillna("").to_dict(orient="records"):
        company_name = normalize_whitespace(row.get("company_name"))
        if not company_name:
            continue
        evidence_rows.append(
            {
                "company_name": company_name,
                "company_tier": row.get("company_tier", ""),
                "evidence_type": "공식채용소스",
                "evidence_url": row.get("source_url", ""),
                "evidence_text": f"{row.get('source_name', '')} / {row.get('source_type', '')} / 검증 {row.get('verification_status', '')} / 활성공고 {int(row.get('last_active_job_count') or 0)}건",
                "role_analyst_signal": 0,
                "role_ds_signal": 0,
                "role_researcher_signal": 0,
                "role_ai_engineer_signal": 0,
                "evidence_strength": 3 if row.get("verification_status") == "성공" else 1,
                "captured_at": collected_at,
            }
        )

    for row in jobs.fillna("").to_dict(orient="records"):
        company_name = normalize_whitespace(row.get("company_name"))
        if not company_name:
            continue
        role_column = _ROLE_SIGNAL_COLUMNS.get(row.get("job_role"))
        evidence = {
            "company_name": company_name,
            "company_tier": row.get("company_tier", ""),
            "evidence_type": "타깃직무공고",
            "evidence_url": row.get("job_url") or row.get("source_url") or "",
            "evidence_text": f"{row.get('job_role', '')} / {row.get('job_title_raw', '')}",
            "role_analyst_signal": 0,
            "role_ds_signal": 0,
            "role_researcher_signal": 0,
            "role_ai_engineer_signal": 0,
            "evidence_strength": 5,
            "captured_at": collected_at,
        }
        if role_column:
            evidence[role_column] = 1
        evidence_rows.append(evidence)

    evidence_frame = pd.DataFrame(evidence_rows)
    for column in COMPANY_EVIDENCE_COLUMNS:
        if column not in evidence_frame.columns:
            evidence_frame[column] = "" if column not in {"evidence_strength", *tuple(_ROLE_SIGNAL_COLUMNS.values())} else 0
    evidence_frame = evidence_frame[list(COMPANY_EVIDENCE_COLUMNS)]

    evidence_counts = evidence_frame["company_name"].value_counts().to_dict() if not evidence_frame.empty else {}
    primary_evidence = _primary_evidence_rows(evidence_frame)

    candidate_rows: list[dict] = []
    for row in companies.fillna("").to_dict(orient="records"):
        company_name = row["company_name"]
        source_stats = source_summary.get(company_name, {})
        job_stats = job_summary.get(company_name, {})
        has_candidate_seed_provenance = bool(normalize_whitespace(row.get("candidate_seed_url")))
        officiality_score = int(bool(normalize_whitespace(row.get("official_domain")))) * 2
        officiality_score += 1 if source_stats.get("source_seed_count", 0) > 0 else 0
        officiality_score += 1 if source_stats.get("verified_source_count", 0) > 0 else 0
        role_signal_count = sum(1 for column in _ROLE_SIGNAL_COLUMNS.values() if job_stats.get(column, 0) > 0)
        active_job_count = int(job_stats.get("active_job_count", 0))
        role_fit_score = role_signal_count * 2 + min(active_job_count, 3)
        hiring_signal_score = min(active_job_count, 5)
        bucket, reject_reason = _company_bucket(
            officiality_score=officiality_score,
            source_seed_count=int(source_stats.get("source_seed_count", 0)),
            verified_source_count=int(source_stats.get("verified_source_count", 0)),
            verified_structured_source_count=int(source_stats.get("verified_structured_source_count", 0)),
            active_job_count=active_job_count,
            role_fit_score=role_fit_score,
            has_candidate_seed_provenance=has_candidate_seed_provenance,
        )
        primary = primary_evidence.get(company_name, {})
        candidate_rows.append(
            {
                **row,
                "officiality_score": officiality_score,
                "source_seed_count": int(source_stats.get("source_seed_count", 0)),
                "verified_source_count": int(source_stats.get("verified_source_count", 0)),
                "active_job_count": active_job_count,
                "role_analyst_signal": int(job_stats.get("role_analyst_signal", 0)),
                "role_ds_signal": int(job_stats.get("role_ds_signal", 0)),
                "role_researcher_signal": int(job_stats.get("role_researcher_signal", 0)),
                "role_ai_engineer_signal": int(job_stats.get("role_ai_engineer_signal", 0)),
                "role_fit_score": role_fit_score,
                "hiring_signal_score": hiring_signal_score,
                "evidence_count": int(evidence_counts.get(company_name, 0)),
                "primary_evidence_type": primary.get("primary_evidence_type", ""),
                "primary_evidence_url": primary.get("primary_evidence_url", ""),
                "primary_evidence_text": primary.get("primary_evidence_text", ""),
                "company_bucket": bucket,
                "reject_reason": reject_reason,
                "last_verified_at": collected_at if int(source_stats.get("verified_source_count", 0)) > 0 else "",
            }
        )

    candidate_frame = pd.DataFrame(candidate_rows)
    for column in COMPANY_CANDIDATE_COLUMNS:
        if column not in candidate_frame.columns:
            candidate_frame[column] = ""
    candidate_frame = candidate_frame[list(COMPANY_CANDIDATE_COLUMNS)]
    summary = {
        "candidate_company_count": int(len(candidate_frame)),
        "company_evidence_count": int(len(evidence_frame)),
        "approved_company_count": int((candidate_frame["company_bucket"] == "approved").sum()),
        "candidate_bucket_count": int((candidate_frame["company_bucket"] == "candidate").sum()),
        "rejected_company_count": int((candidate_frame["company_bucket"] == "rejected").sum()),
        "candidate_seeded_company_count": int(candidate_frame["candidate_seed_url"].fillna("").astype(str).str.strip().ne("").sum()),
        "verified_source_success_count": int(collection_summary.get("verified_source_success_count", 0)),
        "active_target_job_count": int(len(jobs)),
    }
    return candidate_frame, evidence_frame, updated_registry, summary


def collect_company_evidence(
    paths,
    settings,
    *,
    batch_size: int | None = None,
    max_batches: int | None = None,
    resume: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    companies = _base_company_candidates(paths)
    collected_at = _now()
    run_id = f"company-evidence-{datetime.now(tz=ZoneInfo('Asia/Seoul')).strftime('%Y%m%d%H%M%S')}"
    snapshot_date = collected_at[:10]
    effective_batch_size = max(int(batch_size or getattr(settings, "company_evidence_batch_size", 0) or 0), 0)
    effective_max_batches = max(int(max_batches or getattr(settings, "company_evidence_max_batches_per_run", 0) or 0), 0)
    companies = companies.sort_values(by=["company_tier", "company_name"], ascending=[True, True]).reset_index(drop=True)
    progress = _load_company_evidence_progress(paths) if resume else {}
    start_offset = _resume_company_evidence_offset(progress, companies, effective_batch_size)
    companies_to_process = companies.iloc[start_offset:].reset_index(drop=True) if start_offset > 0 else companies
    batch_frames = _batch_company_frames(companies_to_process, effective_batch_size)
    if effective_max_batches > 0:
        batch_frames = batch_frames[:effective_max_batches]
    checkpoint_dir = _checkpoint_dir(paths, run_id)
    working_paths = _company_evidence_working_paths(paths)

    if not batch_frames:
        existing_candidates = _load_working_frame(paths.company_candidates_path, working_paths["candidates"], COMPANY_CANDIDATE_COLUMNS)
        existing_evidence = _load_working_frame(paths.company_evidence_path, working_paths["evidence"], COMPANY_EVIDENCE_COLUMNS)
        existing_registry = _load_working_frame(paths.source_registry_path, working_paths["registry"], SOURCE_REGISTRY_COLUMNS)
        summary = {
            "candidate_company_count": 0,
            "company_evidence_count": 0,
            "approved_company_count": 0,
            "candidate_bucket_count": 0,
            "rejected_company_count": 0,
            "candidate_seeded_company_count": 0,
            "verified_source_success_count": 0,
            "active_target_job_count": 0,
            "batch_size": effective_batch_size,
            "batch_count": 0,
            "completed_batch_count": 0,
            "checkpoint_dir": str(checkpoint_dir),
            "batch_summaries": [],
            "start_offset": int(start_offset),
            "next_offset": int(start_offset),
            "completed_full_scan": True,
            "published_company_state": True,
            "company_candidates_in_progress_path": str(working_paths["candidates"]),
            "company_evidence_in_progress_path": str(working_paths["evidence"]),
            "source_registry_in_progress_path": str(working_paths["registry"]),
        }
        return existing_candidates, existing_evidence, existing_registry, summary

    current_candidates = _load_working_frame(paths.company_candidates_path, working_paths["candidates"], COMPANY_CANDIDATE_COLUMNS)
    current_evidence = _load_working_frame(paths.company_evidence_path, working_paths["evidence"], COMPANY_EVIDENCE_COLUMNS)
    current_registry = _load_working_frame(paths.source_registry_path, working_paths["registry"], SOURCE_REGISTRY_COLUMNS)
    batch_summaries: list[dict] = []
    current_offset = start_offset

    for batch_index, company_batch in enumerate(batch_frames, start=1):
        candidate_batch, evidence_batch, registry_batch, batch_summary = _collect_company_evidence_batch(
            company_batch,
            paths,
            settings,
            run_id=f"{run_id}-b{batch_index:04d}",
            collected_at=collected_at,
            snapshot_date=snapshot_date,
        )
        current_candidates = append_deduplicated(current_candidates, candidate_batch, ["company_name"])
        current_registry = append_deduplicated(current_registry, registry_batch, ["source_url"])
        current_evidence = pd.concat([current_evidence, evidence_batch], ignore_index=True)
        if not current_evidence.empty:
            current_evidence = current_evidence.drop_duplicates(
                subset=["company_name", "evidence_type", "evidence_url", "evidence_text"],
                keep="last",
            ).reset_index(drop=True)

        write_csv(candidate_batch, checkpoint_dir / f"company_candidates_batch_{batch_index:04d}.csv")
        write_csv(evidence_batch, checkpoint_dir / f"company_evidence_batch_{batch_index:04d}.csv")
        write_csv(registry_batch, checkpoint_dir / f"source_registry_batch_{batch_index:04d}.csv")
        write_csv(current_candidates, working_paths["candidates"])
        write_csv(current_evidence, working_paths["evidence"])
        write_csv(current_registry, working_paths["registry"])

        batch_summaries.append(
            {
                "batch_index": batch_index,
                "company_offset_start": int(current_offset),
                "company_offset_end": int(current_offset + len(company_batch)),
                "batch_company_count": int(len(company_batch)),
                **batch_summary,
            }
        )
        current_offset += len(company_batch)

        completed_full_scan = current_offset >= len(companies)
        _write_company_evidence_progress(
            paths,
            {
                "run_id": run_id,
                "candidate_company_count": int(len(companies)),
                "batch_size": int(effective_batch_size if effective_batch_size > 0 else len(companies)),
                "max_batches_per_run": int(effective_max_batches),
                "next_offset": 0 if completed_full_scan else int(current_offset),
                "completed_full_scan": completed_full_scan,
                "last_company_name": normalize_whitespace(str(company_batch.iloc[-1].get("company_name", ""))),
                "updated_at": _now(),
            },
        )

    candidate_frame = current_candidates
    evidence_frame = current_evidence
    updated_registry = current_registry

    if not candidate_frame.empty:
        candidate_frame = candidate_frame.drop_duplicates(subset=["company_name"], keep="last").reset_index(drop=True)
    for column in COMPANY_CANDIDATE_COLUMNS:
        if column not in candidate_frame.columns:
            candidate_frame[column] = ""
    candidate_frame = candidate_frame[list(COMPANY_CANDIDATE_COLUMNS)]

    for column in COMPANY_EVIDENCE_COLUMNS:
        if column not in evidence_frame.columns:
            evidence_frame[column] = "" if column not in {"evidence_strength", *tuple(_ROLE_SIGNAL_COLUMNS.values())} else 0
    evidence_frame = evidence_frame[list(COMPANY_EVIDENCE_COLUMNS)]

    if not updated_registry.empty:
        updated_registry = updated_registry.drop_duplicates(subset=["source_url"], keep="last").reset_index(drop=True)
    for column in SOURCE_REGISTRY_COLUMNS:
        if column not in updated_registry.columns:
            updated_registry[column] = ""
    updated_registry = updated_registry[list(SOURCE_REGISTRY_COLUMNS)]

    completed_full_scan = bool(current_offset >= len(companies))
    if completed_full_scan:
        write_csv(candidate_frame, paths.company_candidates_path)
        write_csv(evidence_frame, paths.company_evidence_path)
        write_csv(updated_registry, paths.source_registry_path)
        _clear_working_company_evidence(paths)
    else:
        write_csv(candidate_frame, working_paths["candidates"])
        write_csv(evidence_frame, working_paths["evidence"])
        write_csv(updated_registry, working_paths["registry"])

    summary = {
        "candidate_company_count": int(len(candidate_frame)),
        "company_evidence_count": int(len(evidence_frame)),
        "approved_company_count": int((candidate_frame["company_bucket"] == "approved").sum()) if not candidate_frame.empty else 0,
        "candidate_bucket_count": int((candidate_frame["company_bucket"] == "candidate").sum()) if not candidate_frame.empty else 0,
        "rejected_company_count": int((candidate_frame["company_bucket"] == "rejected").sum()) if not candidate_frame.empty else 0,
        "candidate_seeded_company_count": int(candidate_frame["candidate_seed_url"].fillna("").astype(str).str.strip().ne("").sum()) if not candidate_frame.empty else 0,
        "verified_source_success_count": int(sum(batch["verified_source_success_count"] for batch in batch_summaries)),
        "active_target_job_count": int(sum(batch["active_target_job_count"] for batch in batch_summaries)),
        "batch_size": effective_batch_size if effective_batch_size > 0 else int(len(companies)),
        "batch_count": int(len(batch_frames)),
        "completed_batch_count": int(len(batch_summaries)),
        "checkpoint_dir": str(checkpoint_dir),
        "batch_summaries": batch_summaries,
        "start_offset": int(start_offset),
        "next_offset": int(current_offset if current_offset < len(companies) else 0),
        "completed_full_scan": completed_full_scan,
        "published_company_state": completed_full_scan,
        "company_candidates_in_progress_path": str(working_paths["candidates"]),
        "company_evidence_in_progress_path": str(working_paths["evidence"]),
        "source_registry_in_progress_path": str(working_paths["registry"]),
    }
    return candidate_frame, evidence_frame, updated_registry, summary


def split_company_buckets(candidate_frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if candidate_frame.empty:
        empty = pd.DataFrame(columns=list(COMPANY_CANDIDATE_COLUMNS))
        return empty, empty, empty
    approved = candidate_frame[candidate_frame["company_bucket"] == "approved"].copy()
    candidate = candidate_frame[candidate_frame["company_bucket"] == "candidate"].copy()
    rejected = candidate_frame[candidate_frame["company_bucket"] == "rejected"].copy()
    return approved, candidate, rejected
