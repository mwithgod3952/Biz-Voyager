"""CLI orchestration pipelines."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
import time
from uuid import uuid4
from zoneinfo import ZoneInfo

import pandas as pd

from .collection import canonicalize_job_key, collect_jobs_from_sources, merge_incremental
from .company_screening import collect_company_evidence, split_company_buckets
from .company_seed_sources import (
    CATALOG_SOURCE_TYPES,
    collect_company_seed_records,
    discover_company_seed_sources,
    load_company_seed_sources,
    load_invalid_company_seed_sources,
    load_shadow_company_seed_sources,
    promote_shadow_company_seed_sources,
    refresh_company_seed_sources,
)
from .constants import COMPANY_CANDIDATE_COLUMNS, IMPORT_COMPANY_COLUMNS, JOB_COLUMNS, RUN_COLUMNS, SOURCE_REGISTRY_COLUMNS
from .discovery import (
    WORK24_LIMITED_PUBLIC_BOARD_DISCOVERY_METHOD,
    audit_work24_population,
    discover_companies,
    discover_source_candidates,
    discover_work24_population,
    import_companies,
    import_sources,
)
from .doctor import run_doctor
from .quality import evaluate_quality_gate, filter_low_quality_jobs, write_quality_gate
from .reporting import build_coverage_report, write_coverage_report
from .screening import screen_sources
from .settings import get_paths, get_settings
from .sheets import build_sheet_tabs, export_tabs_locally, sync_tabs_to_google_sheets
from .storage import append_error_record, append_run_record, coerce_bool, read_csv_or_empty, write_csv, write_jsonl, write_parquet
from .utils import normalize_whitespace

_PROMOTION_SHRINK_MIN_PREVIOUS_COUNT = 50
_PROMOTION_SHRINK_MIN_DROP_COUNT = 5
_PROMOTION_SHRINK_MIN_DROP_RATIO = 0.03
_PROMOTION_SHRINK_MIN_MISSING_COUNT = 10


def _run_id(command: str) -> str:
    stamp = datetime.now(tz=ZoneInfo("Asia/Seoul")).strftime("%Y%m%d%H%M%S%f")
    return f"{command}-{stamp}-{uuid4().hex[:8]}"


def _now() -> str:
    return datetime.now(tz=ZoneInfo("Asia/Seoul")).replace(microsecond=0).isoformat()


def _today() -> str:
    return datetime.now(tz=ZoneInfo("Asia/Seoul")).date().isoformat()


def _record_run(paths, command: str, run_id: str, started_at: str, summary: dict, status: str = "성공") -> None:
    append_run_record(
        paths.runs_path,
        {
            "run_id": run_id,
            "command": command,
            "status": status,
            "started_at": started_at,
            "finished_at": _now(),
            "summary_json": json.dumps(summary, ensure_ascii=False),
        },
    )


def _record_error(paths, command: str, run_id: str, exc: Exception) -> None:
    append_error_record(
        paths.errors_path,
        {
            "run_id": run_id,
            "command": command,
            "logged_at": _now(),
            "error_type": exc.__class__.__name__,
            "error_message": str(exc),
        },
    )


def _latest_run_summary(paths, command: str) -> dict:
    runs = read_csv_or_empty(paths.runs_path, RUN_COLUMNS)
    if runs.empty:
        return {}
    matched = runs[runs["command"] == command]
    if matched.empty:
        return {}
    summary_json = matched.iloc[-1]["summary_json"]
    return json.loads(summary_json) if summary_json else {}


def _latest_collection_summary(paths) -> dict:
    runs = read_csv_or_empty(paths.runs_path, RUN_COLUMNS)
    if runs.empty:
        return {}

    collection_commands = {"run-collection-cycle", "run-daily-tracking", "update-incremental", "collect-jobs"}
    matched = runs[runs["command"].isin(collection_commands)]
    if matched.empty:
        return {}

    for _, row in matched.iloc[::-1].iterrows():
        summary_json = row.get("summary_json", "")
        if not summary_json:
            continue
        summary = json.loads(summary_json)
        if row["command"] in {"run-collection-cycle", "run-daily-tracking"}:
            nested = summary.get("collection")
            if isinstance(nested, dict):
                return nested
        if isinstance(summary, dict):
            return summary
    return {}


def _has_nonempty_csv(path: Path) -> bool:
    return path.exists() and path.stat().st_size > 0


def _path_mtime(path: Path) -> float:
    try:
        return path.stat().st_mtime
    except FileNotFoundError:
        return 0.0


def _load_collection_baseline(paths) -> tuple[pd.DataFrame, str]:
    master = read_csv_or_empty(paths.master_jobs_path, JOB_COLUMNS)
    staging = read_csv_or_empty(paths.staging_jobs_path, JOB_COLUMNS)
    has_master = _has_nonempty_csv(paths.master_jobs_path)
    has_staging = _has_nonempty_csv(paths.staging_jobs_path)
    if has_staging and (not has_master or _path_mtime(paths.staging_jobs_path) >= _path_mtime(paths.master_jobs_path)):
        return staging, "staging"
    if has_master:
        return master, "master"
    if has_staging:
        return staging, "staging"
    return pd.DataFrame(columns=list(JOB_COLUMNS)), "empty"


def _rescreen_existing_registry(registry: pd.DataFrame) -> pd.DataFrame:
    if registry.empty:
        return registry
    _, _, _, rescored = screen_sources(registry)
    return rescored


def _merge_existing_source_registry_state(registry: pd.DataFrame, existing_registry: pd.DataFrame) -> pd.DataFrame:
    if registry.empty or existing_registry.empty:
        return registry
    merged = registry.copy()
    existing_state = existing_registry.drop_duplicates(subset=["source_url"], keep="last").set_index("source_url")
    for state_column in ("verification_status", "failure_count", "last_success_at", "last_active_job_count", "quarantine_reason", "is_quarantined"):
        if state_column in existing_state.columns:
            merged[state_column] = merged["source_url"].map(existing_state[state_column]).combine_first(merged[state_column])
    return merged


def _merge_updated_source_registry(existing_registry: pd.DataFrame, updated_registry: pd.DataFrame) -> pd.DataFrame:
    if existing_registry.empty:
        return updated_registry
    if updated_registry.empty:
        return existing_registry
    if "source_url" not in existing_registry.columns or "source_url" not in updated_registry.columns:
        return updated_registry

    snapshot_managed_methods = {"work24_limited_public_board_fallback"}
    if "discovery_method" in existing_registry.columns:
        existing_registry = existing_registry[
            ~existing_registry["discovery_method"].fillna("").astype(str).isin(snapshot_managed_methods)
        ].copy()

    merged = existing_registry.copy().astype(object)
    updated_by_url = updated_registry.drop_duplicates(subset=["source_url"], keep="last").astype(object).set_index("source_url")
    merged = merged.set_index("source_url")
    overlapping_columns = [column for column in updated_by_url.columns if column in merged.columns]
    if overlapping_columns:
        merged.update(updated_by_url[overlapping_columns])
    missing_rows = updated_by_url.loc[~updated_by_url.index.isin(merged.index)].reset_index()
    merged = merged.reset_index()
    if not missing_rows.empty:
        merged = pd.concat([merged, missing_rows], ignore_index=True)
    for column in SOURCE_REGISTRY_COLUMNS:
        if column not in merged.columns:
            merged[column] = None
    return merged.reindex(columns=list(SOURCE_REGISTRY_COLUMNS))


def _load_partial_company_scan_state(paths) -> dict[str, object]:
    in_progress_candidates_path = paths.runtime_dir / "company_candidates_in_progress.csv"
    in_progress_registry_path = paths.runtime_dir / "source_registry_in_progress.csv"
    in_progress_candidates = read_csv_or_empty(in_progress_candidates_path, COMPANY_CANDIDATE_COLUMNS)
    in_progress_registry = _rescreen_existing_registry(read_csv_or_empty(in_progress_registry_path, SOURCE_REGISTRY_COLUMNS))
    published_candidates = read_csv_or_empty(paths.company_candidates_path, COMPANY_CANDIDATE_COLUMNS)
    published_registry = _rescreen_existing_registry(read_csv_or_empty(paths.source_registry_path, SOURCE_REGISTRY_COLUMNS))

    return {
        "candidates": in_progress_candidates if not in_progress_candidates.empty else published_candidates,
        "registry": in_progress_registry if not in_progress_registry.empty else published_registry,
        "company_state_mode": (
            "reuse_in_progress_partial_scan" if not in_progress_candidates.empty else "reuse_published_partial_scan"
        ),
        "source_state_mode": (
            "reuse_in_progress_partial_scan" if not in_progress_registry.empty else "reuse_published_partial_scan"
        ),
        "registry_output_path": in_progress_registry_path if not in_progress_registry.empty else paths.source_registry_path,
        "using_in_progress_registry": bool(not in_progress_registry.empty),
    }


def _prepare_jobs_for_growth(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=list(JOB_COLUMNS))
    prepared = frame.copy()
    prepared["job_key"] = [
        canonicalize_job_key(row)
        for row in prepared.fillna("").to_dict(orient="records")
    ]
    return prepared.drop_duplicates(subset=["job_key"], keep="last").reset_index(drop=True)


def _processed_verified_source_urls(source_registry: pd.DataFrame, collected_at: str) -> set[str]:
    if source_registry.empty:
        return set()
    if "source_url" not in source_registry.columns or "verification_status" not in source_registry.columns:
        return set()
    verified_mask = source_registry["verification_status"].fillna("").astype(str).eq("성공")
    if "last_success_at" in source_registry.columns:
        processed_mask = source_registry["last_success_at"].fillna("").astype(str).eq(str(collected_at))
    else:
        processed_mask = pd.Series(False, index=source_registry.index)
    if "last_active_job_count" in source_registry.columns:
        active_mask = pd.to_numeric(source_registry["last_active_job_count"], errors="coerce").fillna(0).gt(0)
    else:
        active_mask = pd.Series(True, index=source_registry.index)
    urls = (
        source_registry.loc[verified_mask & processed_mask & active_mask, "source_url"]
        .fillna("")
        .astype(str)
        .tolist()
    )
    return {url for url in urls if url}


def _active_source_urls(frame: pd.DataFrame) -> set[str]:
    if frame.empty or "source_url" not in frame.columns:
        return set()
    if "is_active" in frame.columns:
        active_mask = frame["is_active"].map(coerce_bool)
    else:
        active_mask = pd.Series(True, index=frame.index)
    urls = frame.loc[active_mask, "source_url"].fillna("").astype(str).map(normalize_whitespace)
    return {url for url in urls.tolist() if url}


def _work24_priority_seed_source_urls(registry: pd.DataFrame) -> set[str]:
    if registry.empty or "source_url" not in registry.columns:
        return set()

    source_type = registry.get("source_type", pd.Series("", index=registry.index)).fillna("").astype(str).map(normalize_whitespace)
    discovery_method = registry.get("discovery_method", pd.Series("", index=registry.index)).fillna("").astype(str).map(normalize_whitespace)
    verification_status = registry.get("verification_status", pd.Series("", index=registry.index)).fillna("").astype(str).map(normalize_whitespace)
    urls = registry.get("source_url", pd.Series("", index=registry.index)).fillna("").astype(str).map(normalize_whitespace)

    priority_mask = (
        source_type.eq("work24_public_html")
        & discovery_method.eq(WORK24_LIMITED_PUBLIC_BOARD_DISCOVERY_METHOD)
        & verification_status.ne("성공")
    )
    return {url for url in urls.loc[priority_mask].tolist() if url}


def _work24_registry_frame(registry: pd.DataFrame) -> pd.DataFrame:
    if registry.empty or "source_type" not in registry.columns:
        return pd.DataFrame(columns=list(SOURCE_REGISTRY_COLUMNS))
    work24 = registry[
        registry["source_type"].fillna("").astype(str).map(normalize_whitespace).eq("work24_public_html")
    ].copy()
    for column in SOURCE_REGISTRY_COLUMNS:
        if column not in work24.columns:
            work24[column] = ""
    return work24[list(SOURCE_REGISTRY_COLUMNS)]


def _work24_registry_summary(registry: pd.DataFrame) -> dict[str, object]:
    work24 = _work24_registry_frame(registry)
    if work24.empty:
        return {
            "work24_source_count": 0,
            "work24_verified_source_success_count": 0,
            "work24_verified_source_failure_count": 0,
            "work24_unverified_source_count": 0,
            "work24_active_source_count": 0,
            "work24_active_job_sum": 0,
            "work24_source_bucket_counts": {},
        }
    verification = work24["verification_status"].fillna("").astype(str).map(normalize_whitespace)
    active_job_counts = pd.to_numeric(work24["last_active_job_count"], errors="coerce").fillna(0)
    return {
        "work24_source_count": int(len(work24)),
        "work24_verified_source_success_count": int(verification.eq("성공").sum()),
        "work24_verified_source_failure_count": int(verification.eq("실패").sum()),
        "work24_unverified_source_count": int(verification.eq("미검증").sum()),
        "work24_active_source_count": int(active_job_counts.gt(0).sum()),
        "work24_active_job_sum": int(active_job_counts.sum()),
        "work24_source_bucket_counts": {
            str(key): int(value)
            for key, value in work24["source_bucket"].fillna("").astype(str).map(normalize_whitespace).value_counts().to_dict().items()
        },
    }


def _work24_population_source_urls(paths) -> set[str]:
    jobs = read_csv_or_empty(paths.work24_population_jobs_path)
    if jobs.empty or "population_source_url" not in jobs.columns:
        return set()
    urls = jobs["population_source_url"].fillna("").astype(str).map(normalize_whitespace)
    return {url for url in urls.tolist() if url}


def _work24_registry_source_urls(registry: pd.DataFrame) -> set[str]:
    work24 = _work24_registry_frame(registry)
    if work24.empty or "source_url" not in work24.columns:
        return set()
    urls = work24["source_url"].fillna("").astype(str).map(normalize_whitespace)
    return {url for url in urls.tolist() if url}


def _work24_source_discovery_gap(paths, registry: pd.DataFrame) -> dict[str, object]:
    expected_urls = _work24_population_source_urls(paths)
    registry_urls = _work24_registry_source_urls(registry)
    missing_urls = sorted(expected_urls - registry_urls)
    return {
        "work24_expected_source_count": int(len(expected_urls)),
        "work24_registry_source_count": int(len(registry_urls)),
        "work24_missing_source_count": int(len(missing_urls)),
        "work24_missing_source_urls_sample": missing_urls[:5],
    }


def _work24_job_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "source_url" not in frame.columns:
        return pd.DataFrame(columns=list(JOB_COLUMNS))
    work24 = frame[
        frame["source_url"].fillna("").astype(str).str.contains("work24.go.kr", regex=False)
    ].copy()
    for column in JOB_COLUMNS:
        if column not in work24.columns:
            work24[column] = ""
    return work24[list(JOB_COLUMNS)]


def _work24_job_summary(frame: pd.DataFrame) -> dict[str, object]:
    work24 = _work24_job_frame(frame)
    if work24.empty:
        return {
            "work24_job_count": 0,
            "work24_active_job_count": 0,
            "work24_role_counts": {},
            "work24_company_count": 0,
        }
    active_count = (
        int(work24["is_active"].map(coerce_bool).sum())
        if "is_active" in work24.columns
        else int(len(work24))
    )
    return {
        "work24_job_count": int(len(work24)),
        "work24_active_job_count": int(active_count),
        "work24_role_counts": {
            str(key): int(value)
            for key, value in work24["job_role"].fillna("").astype(str).map(normalize_whitespace).replace("", pd.NA).dropna().value_counts().to_dict().items()
        },
        "work24_company_count": int(
            work24["company_name"].fillna("").astype(str).map(normalize_whitespace).replace("", pd.NA).dropna().nunique()
        ),
    }


def _active_job_count(frame: pd.DataFrame) -> int:
    if frame.empty or "is_active" not in frame.columns:
        return int(len(frame))
    return int(frame["is_active"].map(coerce_bool).sum())


def _evaluate_publish_shrink_guard(candidate_master: pd.DataFrame, paths) -> dict[str, object]:
    previous_master = read_csv_or_empty(paths.master_jobs_path, JOB_COLUMNS)
    previous_master_count = int(len(previous_master))
    candidate_master_count = int(len(candidate_master))
    drop_count = max(previous_master_count - candidate_master_count, 0)
    drop_ratio = drop_count / max(previous_master_count, 1)
    previous_active_count = _active_job_count(previous_master)
    candidate_active_count = _active_job_count(candidate_master)
    active_drop_count = max(previous_active_count - candidate_active_count, 0)
    active_drop_ratio = active_drop_count / max(previous_active_count, 1)

    collection_summary = _latest_collection_summary(paths)
    summary_staging_count = int(collection_summary.get("staging_job_count", candidate_master_count) or 0)
    completed_full_source_scan = bool(collection_summary.get("completed_full_source_scan", True))
    new_job_count = int(collection_summary.get("new_job_count", 0) or 0)
    changed_job_count = int(collection_summary.get("changed_job_count", 0) or 0)
    missing_job_count = int(collection_summary.get("missing_job_count", 0) or 0)
    held_job_count = int(collection_summary.get("held_job_count", 0) or 0)

    metrics = {
        "triggered": False,
        "previous_master_count": previous_master_count,
        "candidate_master_count": candidate_master_count,
        "drop_count": drop_count,
        "drop_ratio": drop_ratio,
        "previous_active_count": previous_active_count,
        "candidate_active_count": candidate_active_count,
        "active_drop_count": active_drop_count,
        "active_drop_ratio": active_drop_ratio,
        "summary_staging_count": summary_staging_count,
        "completed_full_source_scan": completed_full_source_scan,
        "new_job_count": new_job_count,
        "changed_job_count": changed_job_count,
        "missing_job_count": missing_job_count,
        "held_job_count": held_job_count,
        "reason": "",
    }

    if previous_master_count < _PROMOTION_SHRINK_MIN_PREVIOUS_COUNT:
        return metrics
    if candidate_master_count >= previous_master_count:
        return metrics
    if summary_staging_count != candidate_master_count:
        return metrics

    suspicious_shrink = (
        not completed_full_source_scan
        and drop_count >= _PROMOTION_SHRINK_MIN_DROP_COUNT
        and drop_ratio >= _PROMOTION_SHRINK_MIN_DROP_RATIO
        and new_job_count == 0
        and missing_job_count >= max(_PROMOTION_SHRINK_MIN_MISSING_COUNT, drop_count)
    )
    if not suspicious_shrink:
        return metrics

    metrics["triggered"] = True
    metrics["reason"] = "비정상 감소가 감지되어 master 승격을 차단합니다."
    return metrics


def _processed_source_outcomes(
    previous_registry: pd.DataFrame,
    updated_registry: pd.DataFrame,
    collected_at: str,
) -> dict[str, str]:
    if updated_registry.empty or "source_url" not in updated_registry.columns:
        return {}

    previous_by_url: dict[str, dict[str, object]] = {}
    if not previous_registry.empty and "source_url" in previous_registry.columns:
        for row in previous_registry.fillna("").to_dict(orient="records"):
            source_url = normalize_whitespace(row.get("source_url"))
            if source_url:
                previous_by_url[source_url] = row

    outcomes: dict[str, str] = {}
    collected_at_text = str(collected_at)
    for row in updated_registry.fillna("").to_dict(orient="records"):
        source_url = normalize_whitespace(row.get("source_url"))
        if not source_url:
            continue
        verification_status = normalize_whitespace(row.get("verification_status"))
        last_success_at = normalize_whitespace(row.get("last_success_at"))
        if verification_status == "성공" and last_success_at == collected_at_text:
            outcomes[source_url] = "success"
            continue

        previous = previous_by_url.get(source_url, {})
        current_failure_count = int(row.get("failure_count") or 0)
        previous_failure_count = int(previous.get("failure_count") or 0)
        if verification_status == "실패" and current_failure_count > previous_failure_count:
            outcomes[source_url] = "failure"
    return outcomes


def _summarize_incremental_growth(baseline: pd.DataFrame, current: pd.DataFrame) -> dict[str, int]:
    prepared_baseline = _prepare_jobs_for_growth(baseline)
    prepared_current = _prepare_jobs_for_growth(current)
    baseline_by_key = {
        row["job_key"]: row
        for row in prepared_baseline.fillna("").to_dict(orient="records")
    }
    status_counts = (
        prepared_current["record_status"].fillna("").value_counts().to_dict()
        if not prepared_current.empty and "record_status" in prepared_current.columns
        else {}
    )
    active_before = int(prepared_baseline["is_active"].fillna(False).map(coerce_bool).sum()) if not prepared_baseline.empty else 0
    active_after = int(prepared_current["is_active"].fillna(False).map(coerce_bool).sum()) if not prepared_current.empty else 0
    reactivated_count = 0
    newly_inactive_count = 0
    for row in prepared_current.fillna("").to_dict(orient="records"):
        previous = baseline_by_key.get(row.get("job_key"))
        if previous is None:
            continue
        previous_active = coerce_bool(previous.get("is_active"))
        current_active = coerce_bool(row.get("is_active"))
        if not previous_active and current_active:
            reactivated_count += 1
        if previous_active and not current_active:
            newly_inactive_count += 1
    return {
        "baseline_job_count": int(len(prepared_baseline)),
        "baseline_active_job_count": int(active_before),
        "merged_job_count": int(len(prepared_current)),
        "merged_active_job_count": int(active_after),
        "merged_inactive_job_count": int(max(len(prepared_current) - active_after, 0)),
        "new_job_count": int(status_counts.get("신규", 0)),
        "changed_job_count": int(status_counts.get("변경", 0)),
        "unchanged_job_count": int(status_counts.get("유지", 0)),
        "missing_job_count": int(status_counts.get("미발견", 0)),
        "held_job_count": int(status_counts.get("검증실패보류", 0)),
        "reactivated_job_count": int(reactivated_count),
        "newly_inactive_job_count": int(newly_inactive_count),
        "carried_forward_job_count": int(status_counts.get("미발견", 0) + status_counts.get("검증실패보류", 0)),
        "net_job_delta": int(len(prepared_current) - len(prepared_baseline)),
        "net_active_job_delta": int(active_after - active_before),
    }


def _skipped_collection_summary(paths, reason: str) -> dict[str, object]:
    staging = read_csv_or_empty(paths.staging_jobs_path, JOB_COLUMNS)
    return {
        "collection_mode": "skipped",
        "collection_run_mode": "guarded_skip",
        "collection_state": reason,
        "collected_job_count": 0,
        "verified_source_success_count": 0,
        "verified_source_failure_count": 0,
        "staging_job_count": int(len(staging)),
        "dropped_low_quality_job_count": 0,
        "quality_gate_passed": False,
        "quality_gate_reasons": [reason],
        "completed_full_source_scan": False,
        "source_scan_mode": "guarded_skip",
        "total_collectable_source_count": 0,
        "selected_collectable_source_count": 0,
        "processed_collectable_source_count": 0,
        "deferred_collectable_source_count": 0,
        "pending_collectable_source_count": 0,
        "source_scan_start_offset": 0,
        "source_scan_next_offset": 0,
        "source_scan_completed_full_pass_count": 0,
        "source_scan_runtime_budget_seconds": 0.0,
        "source_scan_runtime_limited": False,
        "source_scan_registry_signature_changed": False,
    }


def _promotion_hold_summary(
    *,
    reason: str,
    staging_job_count: int,
    quality_gate_passed: bool = False,
    quality_gate_reasons: list[str] | None = None,
) -> dict[str, object]:
    return {
        "quality_gate_passed": bool(quality_gate_passed),
        "quality_gate_reasons": quality_gate_reasons or [reason],
        "dropped_low_quality_job_count": 0,
        "promoted_job_count": 0,
        "promotion_skipped": True,
        "promotion_skipped_reason": reason,
        "staging_job_count": int(staging_job_count),
    }


def _summarize_company_bucket_frame(frame: pd.DataFrame, *, mode: str) -> dict[str, object]:
    if frame.empty:
        return {
            "approved_company_count": 0,
            "candidate_company_count": 0,
            "rejected_company_count": 0,
            "screened_company_count": 0,
            "company_state_mode": mode,
        }
    return {
        "approved_company_count": int((frame["company_bucket"] == "approved").sum()),
        "candidate_company_count": int((frame["company_bucket"] == "candidate").sum()),
        "rejected_company_count": int((frame["company_bucket"] == "rejected").sum()),
        "screened_company_count": int(len(frame)),
        "company_state_mode": mode,
    }


def _summarize_source_bucket_frame(frame: pd.DataFrame, *, mode: str) -> dict[str, object]:
    if frame.empty:
        return {
            "approved_source_count": 0,
            "candidate_source_count": 0,
            "rejected_source_count": 0,
            "screened_source_count": 0,
            "company_input_count": 0,
            "company_input_mode": mode,
        }
    company_input_count = int(frame["company_name"].fillna("").astype(str).replace("", pd.NA).dropna().nunique()) if "company_name" in frame.columns else 0
    return {
        "approved_source_count": int((frame["source_bucket"] == "approved").sum()),
        "candidate_source_count": int((frame["source_bucket"] == "candidate").sum()),
        "rejected_source_count": int((frame["source_bucket"] == "rejected").sum()),
        "screened_source_count": int(len(frame)),
        "company_input_count": company_input_count,
        "company_input_mode": mode,
    }


def discover_companies_pipeline(project_root: Path | None = None) -> dict:
    paths = get_paths(project_root)
    started_at = _now()
    run_id = _run_id("discover-companies")
    try:
        companies, summary = discover_companies(paths)
        _record_run(paths, "discover-companies", run_id, started_at, summary)
        return summary
    except Exception as exc:  # noqa: BLE001
        _record_error(paths, "discover-companies", run_id, exc)
        raise


def collect_company_seed_records_pipeline(project_root: Path | None = None) -> dict:
    paths = get_paths(project_root)
    settings = get_settings(project_root)
    started_at = _now()
    run_id = _run_id("collect-company-seed-records")
    try:
        refresh_hours = int(getattr(settings, "company_seed_record_refresh_hours", 0) or 0)
        if (
            refresh_hours > 0
            and paths.collected_company_seed_records_path.exists()
            and paths.collected_company_seed_records_path.stat().st_size > 0
        ):
            timezone_info = ZoneInfo(getattr(settings, "timezone", "Asia/Seoul"))
            modified_at = datetime.fromtimestamp(
                paths.collected_company_seed_records_path.stat().st_mtime,
                tz=timezone_info,
            )
            if modified_at >= datetime.now(timezone_info) - timedelta(hours=refresh_hours):
                cached_records = read_csv_or_empty(paths.collected_company_seed_records_path)
                refresh_summary = refresh_company_seed_sources(paths, settings)
                if (
                    int(refresh_summary.get("newly_discovered_seed_source_count", 0)) > 0
                    or int(refresh_summary.get("auto_promoted_shadow_seed_source_count", 0)) > 0
                ):
                    records, refreshed_summary = collect_company_seed_records(paths, settings)
                    write_csv(records, paths.collected_company_seed_records_path)
                    _record_run(paths, "collect-company-seed-records", run_id, started_at, refreshed_summary)
                    return refreshed_summary
                approved_seed_sources = load_company_seed_sources(paths)
                collectable_seed_sources = approved_seed_sources[
                    ~approved_seed_sources["source_type"].fillna("").isin(CATALOG_SOURCE_TYPES)
                ].reset_index(drop=True)
                shadow_seed_sources = load_shadow_company_seed_sources(paths)
                invalid_seed_sources = load_invalid_company_seed_sources(paths)
                summary = {
                    "seed_source_count": int(len(collectable_seed_sources)),
                    "collected_seed_record_count": int(len(cached_records)),
                    "collected_company_count": int(len(cached_records)),
                    "seed_source_mode": "cached_records",
                    "catalog_source_count": int(refresh_summary.get("catalog_source_count", 0)),
                    "total_catalog_source_count": int(refresh_summary.get("total_catalog_source_count", 0)),
                    "catalog_source_start_offset": int(refresh_summary.get("catalog_source_start_offset", 0)),
                    "catalog_source_next_offset": int(refresh_summary.get("catalog_source_next_offset", 0)),
                    "catalog_source_runtime_limited": bool(refresh_summary.get("catalog_source_runtime_limited", False)),
                    "catalog_host_seed_source_count": int(refresh_summary.get("catalog_host_seed_source_count", 0)),
                    "search_query_count": int(refresh_summary.get("search_query_count", 0)),
                    "search_query_start_offset": int(refresh_summary.get("search_query_start_offset", 0)),
                    "search_query_next_offset": int(refresh_summary.get("search_query_next_offset", 0)),
                    "search_query_batch_count": int(refresh_summary.get("search_query_batch_count", 0)),
                    "search_query_batch_size": int(refresh_summary.get("search_query_batch_size", 0)),
                    "search_discovered_seed_source_count": int(refresh_summary.get("search_discovered_seed_source_count", 0)),
                    "discovered_seed_source_count": int(refresh_summary.get("discovered_seed_source_count", 0)),
                    "newly_discovered_seed_source_count": int(refresh_summary.get("newly_discovered_seed_source_count", 0)),
                    "shadow_seed_source_count": int(len(shadow_seed_sources)),
                    "approved_seed_source_count": int(len(collectable_seed_sources)),
                    "auto_promoted_shadow_seed_source_count": int(refresh_summary.get("auto_promoted_shadow_seed_source_count", 0)),
                    "duplicate_shadow_seed_source_count": int(refresh_summary.get("duplicate_shadow_seed_source_count", 0)),
                    "remaining_shadow_seed_source_count": int(len(shadow_seed_sources)),
                    "invalid_shadow_seed_source_count": int(refresh_summary.get("invalid_shadow_seed_source_count", len(invalid_seed_sources))),
                    "invalid_shadow_seed_sources": refresh_summary.get("invalid_shadow_seed_sources", []),
                    "source_record_counts": {},
                    "source_raw_record_counts": {},
                    "skipped_seed_source_count": 0,
                    "skipped_seed_sources": [],
                    "tier_counts": (
                        cached_records["company_tier"].value_counts().to_dict()
                        if not cached_records.empty and "company_tier" in cached_records.columns
                        else {}
                    ),
                    "used_cached_collected_seed_records": True,
                    "company_seed_record_refresh_hours": refresh_hours,
                    "cached_collected_seed_record_modified_at": modified_at.isoformat(),
                    "seed_source_refresh": refresh_summary,
                }
                _record_run(paths, "collect-company-seed-records", run_id, started_at, summary)
                return summary
        records, summary = collect_company_seed_records(paths, settings)
        write_csv(records, paths.collected_company_seed_records_path)
        _record_run(paths, "collect-company-seed-records", run_id, started_at, summary)
        return summary
    except Exception as exc:  # noqa: BLE001
        _record_error(paths, "collect-company-seed-records", run_id, exc)
        raise


def discover_company_seed_sources_pipeline(project_root: Path | None = None) -> dict:
    paths = get_paths(project_root)
    settings = get_settings(project_root)
    started_at = _now()
    run_id = _run_id("discover-company-seed-sources")
    try:
        _, summary = discover_company_seed_sources(paths, settings, target="shadow")
        _record_run(paths, "discover-company-seed-sources", run_id, started_at, summary)
        return summary
    except Exception as exc:  # noqa: BLE001
        _record_error(paths, "discover-company-seed-sources", run_id, exc)
        raise


def promote_shadow_seed_sources_pipeline(project_root: Path | None = None) -> dict:
    paths = get_paths(project_root)
    started_at = _now()
    run_id = _run_id("promote-shadow-seed-sources")
    try:
        _, summary = promote_shadow_company_seed_sources(paths)
        _record_run(paths, "promote-shadow-seed-sources", run_id, started_at, summary)
        return summary
    except Exception as exc:  # noqa: BLE001
        _record_error(paths, "promote-shadow-seed-sources", run_id, exc)
        raise


def discover_work24_population_candidates_pipeline(project_root: Path | None = None) -> dict:
    paths = get_paths(project_root)
    settings = get_settings(project_root)
    started_at = _now()
    run_id = _run_id("discover-work24-population")
    try:
        discovered, discovered_jobs, shadow_companies, summary = discover_work24_population(paths, settings)
        combined = discovered.reset_index(drop=True) if not discovered.empty else pd.DataFrame(columns=list(IMPORT_COMPANY_COLUMNS))
        write_csv(combined, paths.work24_population_candidates_path)
        combined_jobs = discovered_jobs.reset_index(drop=True)
        if not combined_jobs.empty and "worknet_wanted_auth_no" in combined_jobs.columns:
            auth_key = combined_jobs["worknet_wanted_auth_no"].fillna("").astype(str).str.strip()
            url_key = combined_jobs.get("job_url", pd.Series([""] * len(combined_jobs))).fillna("").astype(str).str.strip()
            combined_jobs = combined_jobs.assign(_dedupe_key=auth_key.where(auth_key.ne(""), url_key))
            combined_jobs = combined_jobs.drop_duplicates(subset=["_dedupe_key"], keep="last").drop(columns=["_dedupe_key"])
        write_csv(combined_jobs, paths.work24_population_jobs_path)
        combined_shadow_companies = shadow_companies.reset_index(drop=True)
        write_csv(combined_shadow_companies, paths.work24_population_shadow_companies_path)
        summary = {
            **summary,
            "stored_work24_population_candidate_count": int(len(combined)),
            "stored_work24_population_job_count": int(len(combined_jobs)),
            "stored_work24_population_shadow_company_count": int(len(combined_shadow_companies)),
            "new_work24_population_candidate_count": int(len(discovered)),
            "new_work24_population_job_count": int(len(discovered_jobs)),
            "new_work24_population_shadow_company_count": int(len(shadow_companies)),
            "work24_population_artifact": str(paths.work24_population_candidates_path),
            "work24_population_jobs_artifact": str(paths.work24_population_jobs_path),
            "work24_population_shadow_companies_artifact": str(paths.work24_population_shadow_companies_path),
            "work24_population_scan_log_artifact": str(paths.work24_population_scan_log_path),
        }
        _record_run(paths, "discover-work24-population", run_id, started_at, summary)
        return summary
    except Exception as exc:  # noqa: BLE001
        _record_error(paths, "discover-work24-population", run_id, exc)
        raise


def audit_work24_population_pipeline(project_root: Path | None = None) -> dict:
    paths = get_paths(project_root)
    started_at = _now()
    run_id = _run_id("audit-work24-population")
    try:
        summary = audit_work24_population(paths)
        _record_run(paths, "audit-work24-population", run_id, started_at, summary)
        return summary
    except Exception as exc:  # noqa: BLE001
        _record_error(paths, "audit-work24-population", run_id, exc)
        raise


def run_work24_convergence_pipeline(
    project_root: Path | None = None,
    *,
    metric_name: str = "work24_suspicious_positive_count",
    max_iterations: int = 4,
    stable_passes: int = 1,
    sleep_seconds: float = 0.0,
) -> dict:
    paths = get_paths(project_root)
    started_at = _now()
    run_id = _run_id("run-work24-convergence")
    max_iterations = max(1, int(max_iterations))
    stable_passes = max(1, int(stable_passes))
    sleep_seconds = max(0.0, float(sleep_seconds))
    metric_name = normalize_whitespace(metric_name) or "work24_suspicious_positive_count"

    iterations: list[dict[str, object]] = []
    consecutive_zero_passes = 0
    final_metric_value = 0

    try:
        for iteration in range(1, max_iterations + 1):
            discover_summary = discover_work24_population_candidates_pipeline(project_root)
            audit_summary = audit_work24_population_pipeline(project_root)
            final_metric_value = int(audit_summary.get(metric_name, 0))
            if final_metric_value == 0:
                consecutive_zero_passes += 1
            else:
                consecutive_zero_passes = 0
            iterations.append(
                {
                    "iteration": iteration,
                    "candidate_count": int(discover_summary.get("stored_work24_population_candidate_count", 0)),
                    "job_count": int(discover_summary.get("stored_work24_population_job_count", 0)),
                    "metric_value": final_metric_value,
                    "metric_name": metric_name,
                    "reason_counts": dict(audit_summary.get("work24_suspicious_positive_reason_counts", {})),
                    "blank_role_counts": dict(audit_summary.get("work24_strong_target_blank_role_counts", {})),
                }
            )
            if consecutive_zero_passes >= stable_passes:
                break
            if sleep_seconds and iteration < max_iterations:
                time.sleep(sleep_seconds)

        converged = final_metric_value == 0 and consecutive_zero_passes >= stable_passes
        latest_audit = audit_work24_population(paths)
        summary = {
            "converged": converged,
            "work24_convergence_metric_name": metric_name,
            "work24_convergence_metric_value": int(latest_audit.get(metric_name, final_metric_value)),
            "work24_convergence_required_stable_passes": stable_passes,
            "work24_convergence_zero_passes": consecutive_zero_passes,
            "work24_convergence_iteration_count": int(len(iterations)),
            "work24_convergence_iterations": iterations,
            "work24_population_candidate_count": int(latest_audit.get("work24_population_candidate_count", 0)),
            "work24_population_job_count": int(latest_audit.get("work24_population_job_count", 0)),
            "work24_population_role_hint_job_count": int(latest_audit.get("work24_population_role_hint_job_count", 0)),
            "work24_suspicious_positive_count": int(latest_audit.get("work24_suspicious_positive_count", 0)),
            "work24_candidate_noise_count": int(latest_audit.get("work24_candidate_noise_count", 0)),
            "work24_strong_target_blank_count": int(latest_audit.get("work24_strong_target_blank_count", 0)),
            "work24_strong_target_blank_role_counts": dict(
                latest_audit.get("work24_strong_target_blank_role_counts", {})
            ),
            "work24_suspicious_positive_reason_counts": dict(
                latest_audit.get("work24_suspicious_positive_reason_counts", {})
            ),
            "work24_population_audit_artifact": latest_audit.get("work24_population_audit_artifact", ""),
        }
        _record_run(
            paths,
            "run-work24-convergence",
            run_id,
            started_at,
            summary,
            status="성공" if converged else "보류",
        )
        return summary
    except Exception as exc:  # noqa: BLE001
        _record_error(paths, "run-work24-convergence", run_id, exc)
        raise


def run_work24_improvement_pipeline(project_root: Path | None = None) -> dict:
    paths = get_paths(project_root)
    started_at = _now()
    run_id = _run_id("run-work24-improvement")
    try:
        reuse_existing_population = (
            _has_nonempty_csv(paths.work24_population_candidates_path)
            and _has_nonempty_csv(paths.work24_population_jobs_path)
        )
        if reuse_existing_population:
            initial_audit_summary = audit_work24_population_pipeline(project_root)
            shadow_companies = read_csv_or_empty(paths.work24_population_shadow_companies_path)
            work24_population_summary = {
                "stored_work24_population_candidate_count": int(initial_audit_summary.get("work24_population_candidate_count", 0)),
                "stored_work24_population_job_count": int(initial_audit_summary.get("work24_population_job_count", 0)),
                "stored_work24_population_shadow_company_count": int(len(shadow_companies)),
                "new_work24_population_candidate_count": int(initial_audit_summary.get("work24_population_candidate_count", 0)),
                "new_work24_population_job_count": int(initial_audit_summary.get("work24_population_job_count", 0)),
                "new_work24_population_shadow_company_count": int(len(shadow_companies)),
                "work24_population_artifact": str(paths.work24_population_candidates_path),
                "work24_population_jobs_artifact": str(paths.work24_population_jobs_path),
                "work24_population_shadow_companies_artifact": str(paths.work24_population_shadow_companies_path),
                "work24_population_audit_artifact": initial_audit_summary.get("work24_population_audit_artifact", str(paths.work24_population_audit_path)),
                "work24_population_reuse_mode": "existing_artifacts",
            }
        else:
            work24_population_summary = discover_work24_population_candidates_pipeline(project_root)
            initial_audit_summary = audit_work24_population_pipeline(project_root)

        convergence_summary = {}
        for label, metric_name in (
            ("suspicious_positive", "work24_suspicious_positive_count"),
            ("strong_target_blank", "work24_strong_target_blank_count"),
        ):
            metric_value = int(initial_audit_summary.get(metric_name, 0))
            if metric_value == 0:
                convergence_summary[label] = {
                    "converged": True,
                    "work24_convergence_metric_name": metric_name,
                    "work24_convergence_metric_value": 0,
                    "work24_convergence_required_stable_passes": 1,
                    "work24_convergence_zero_passes": 1,
                    "work24_convergence_iteration_count": 0,
                    "work24_convergence_iterations": [],
                    "work24_population_candidate_count": int(initial_audit_summary.get("work24_population_candidate_count", 0)),
                    "work24_population_job_count": int(initial_audit_summary.get("work24_population_job_count", 0)),
                    "work24_population_role_hint_job_count": int(initial_audit_summary.get("work24_population_role_hint_job_count", 0)),
                    "work24_suspicious_positive_count": int(initial_audit_summary.get("work24_suspicious_positive_count", 0)),
                    "work24_candidate_noise_count": int(initial_audit_summary.get("work24_candidate_noise_count", 0)),
                    "work24_strong_target_blank_count": int(initial_audit_summary.get("work24_strong_target_blank_count", 0)),
                    "work24_strong_target_blank_role_counts": dict(
                        initial_audit_summary.get("work24_strong_target_blank_role_counts", {})
                    ),
                    "work24_suspicious_positive_reason_counts": dict(
                        initial_audit_summary.get("work24_suspicious_positive_reason_counts", {})
                    ),
                    "work24_population_audit_artifact": initial_audit_summary.get("work24_population_audit_artifact", ""),
                }
            else:
                convergence_summary[label] = run_work24_convergence_pipeline(
                    project_root,
                    metric_name=metric_name,
                    max_iterations=3,
                    stable_passes=1,
                )
        published_registry = read_csv_or_empty(paths.source_registry_path, SOURCE_REGISTRY_COLUMNS)
        published_work24_summary = _work24_registry_summary(published_registry)
        source_discovery_gap = _work24_source_discovery_gap(paths, published_registry)
        target_work24_source_count = int(work24_population_summary.get("stored_work24_population_candidate_count", 0))
        needs_source_discovery = bool(source_discovery_gap.get("work24_missing_source_count", 0))
        if not needs_source_discovery and int(source_discovery_gap.get("work24_expected_source_count", 0)) == 0:
            needs_source_discovery = int(published_work24_summary.get("work24_source_count", 0)) < target_work24_source_count

        if _has_nonempty_csv(paths.companies_registry_path):
            existing_companies = read_csv_or_empty(paths.companies_registry_path)
            company_expansion_summary = {
                "discovered_company_count": int(len(existing_companies)),
                "candidate_input_mode": "existing_companies_registry",
            }
        else:
            company_expansion_summary = discover_companies_pipeline(project_root)

        if needs_source_discovery:
            source_discovery_summary = discover_sources_pipeline(project_root)
            refreshed_registry = read_csv_or_empty(paths.source_registry_path, SOURCE_REGISTRY_COLUMNS)
            source_discovery_gap = _work24_source_discovery_gap(paths, refreshed_registry)
        else:
            source_discovery_summary = {
                **_summarize_source_bucket_frame(published_registry, mode="published_source_registry"),
                **source_discovery_gap,
                "work24_registry_reuse_mode": "published_source_registry",
            }
            refreshed_registry = published_registry

        work24_registry = _work24_registry_frame(refreshed_registry)
        activation_summary = update_incremental_pipeline(
            project_root,
            allow_source_discovery_fallback=False,
            enable_source_scan_progress=False,
            registry_frame=work24_registry,
        )
        promotion_summary = promote_staging_pipeline(project_root)

        refreshed_registry = read_csv_or_empty(paths.source_registry_path, SOURCE_REGISTRY_COLUMNS)
        staging = read_csv_or_empty(paths.staging_jobs_path, JOB_COLUMNS)
        master = read_csv_or_empty(paths.master_jobs_path, JOB_COLUMNS)

        report = {
            "completed": bool(
                convergence_summary["suspicious_positive"].get("converged")
                and convergence_summary["strong_target_blank"].get("converged")
                and bool(activation_summary.get("quality_gate_passed", False))
                and bool(promotion_summary.get("quality_gate_passed", False))
                and int(promotion_summary.get("promoted_job_count", 0)) > 0
            ),
            "run_id": run_id,
            "started_at": started_at,
            "completed_at": _now(),
            "convergence": convergence_summary,
            "work24_population": work24_population_summary,
            "company_expansion": {
                "expanded_candidate_company_count": int(company_expansion_summary.get("discovered_company_count", 0)),
                "candidate_input_mode": company_expansion_summary.get("candidate_input_mode", ""),
            },
            "source_discovery": {
                **source_discovery_summary,
                **source_discovery_gap,
                **_work24_registry_summary(refreshed_registry),
            },
            "activation": activation_summary,
            "promotion": promotion_summary,
            "staging": _work24_job_summary(staging),
            "master": _work24_job_summary(master),
            "artifacts": {
                "work24_population_audit": str(paths.work24_population_audit_path),
                "work24_population_candidates": str(paths.work24_population_candidates_path),
                "work24_population_jobs": str(paths.work24_population_jobs_path),
                "work24_population_shadow_companies": str(paths.work24_population_shadow_companies_path),
                "source_registry": str(paths.source_registry_path),
                "staging_jobs": str(paths.staging_jobs_path),
                "master_jobs": str(paths.master_jobs_path),
                "work24_improvement_report": str(paths.work24_improvement_report_path),
            },
        }
        paths.work24_improvement_report_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        _record_run(
            paths,
            "run-work24-improvement",
            run_id,
            started_at,
            report,
            status="성공" if report["completed"] else "보류",
        )
        return report
    except Exception as exc:  # noqa: BLE001
        _record_error(paths, "run-work24-improvement", run_id, exc)
        raise


def expand_company_candidates_pipeline(project_root: Path | None = None) -> dict:
    paths = get_paths(project_root)
    started_at = _now()
    run_id = _run_id("expand-company-candidates")
    try:
        seed_summary = collect_company_seed_records_pipeline(project_root)
        work24_summary = discover_work24_population_candidates_pipeline(project_root)
        discover_summary = discover_companies_pipeline(project_root)
        summary = {
            **seed_summary,
            "work24_population": work24_summary,
            "work24_population_candidate_count": int(work24_summary.get("stored_work24_population_candidate_count", 0)),
            "work24_population_new_candidate_count": int(work24_summary.get("new_work24_population_candidate_count", 0)),
            "expanded_candidate_company_count": int(discover_summary.get("discovered_company_count", 0)),
            "candidate_input_mode": discover_summary.get("candidate_input_mode", ""),
            "seeded_candidate_count": int(discover_summary.get("seeded_candidate_count", 0)),
        }
        _record_run(paths, "expand-company-candidates", run_id, started_at, summary)
        return summary
    except Exception as exc:  # noqa: BLE001
        _record_error(paths, "expand-company-candidates", run_id, exc)
        raise


def discover_sources_pipeline(project_root: Path | None = None) -> dict:
    paths = get_paths(project_root)
    started_at = _now()
    run_id = _run_id("discover-sources")
    try:
        approved_companies = read_csv_or_empty(paths.approved_companies_path)
        registry_companies = read_csv_or_empty(paths.companies_registry_path)
        companies = approved_companies
        company_input_mode = "approved_companies" if not approved_companies.empty else "companies_registry"
        if companies.empty and registry_companies.empty:
            discover_companies_pipeline(project_root)
            registry_companies = read_csv_or_empty(paths.companies_registry_path)
        if companies.empty:
            companies = registry_companies
        elif not registry_companies.empty:
            companies = (
                pd.concat([approved_companies, registry_companies], ignore_index=True)
                .drop_duplicates(subset=["company_name"], keep="first")
                .reset_index(drop=True)
            )
            company_input_mode = "approved_companies_plus_registry"
        source_candidates = discover_source_candidates(companies, paths, settings=get_settings(project_root))
        approved, candidate, rejected, registry = screen_sources(source_candidates)
        existing_registry = read_csv_or_empty(paths.source_registry_path, SOURCE_REGISTRY_COLUMNS)
        registry = _merge_updated_source_registry(existing_registry, registry)
        registry = _merge_existing_source_registry_state(registry, existing_registry)
        approved = registry[registry["source_bucket"] == "approved"].copy()
        candidate = registry[registry["source_bucket"] == "candidate"].copy()
        rejected = registry[registry["source_bucket"] == "rejected"].copy()
        write_csv(approved, paths.approved_sources_path)
        write_csv(candidate, paths.candidate_sources_path)
        write_csv(rejected, paths.rejected_sources_path)
        write_csv(registry, paths.source_registry_path)
        summary = {
            "approved_source_count": int(len(approved)),
            "candidate_source_count": int(len(candidate)),
            "rejected_source_count": int(len(rejected)),
            "screened_source_count": int(len(registry)),
            "company_input_count": int(len(companies)),
            "company_input_mode": company_input_mode,
        }
        _record_run(paths, "discover-sources", run_id, started_at, summary)
        return summary
    except Exception as exc:  # noqa: BLE001
        _record_error(paths, "discover-sources", run_id, exc)
        raise


def collect_company_evidence_pipeline(
    project_root: Path | None = None,
    *,
    batch_size: int | None = None,
    max_batches: int | None = None,
    resume: bool = True,
) -> dict:
    paths = get_paths(project_root)
    settings = get_settings(project_root)
    started_at = _now()
    run_id = _run_id("collect-company-evidence")
    try:
        candidates, evidence, updated_registry, summary = collect_company_evidence(
            paths,
            settings,
            batch_size=batch_size,
            max_batches=max_batches,
            resume=resume,
        )
        _record_run(paths, "collect-company-evidence", run_id, started_at, summary)
        return summary
    except Exception as exc:  # noqa: BLE001
        _record_error(paths, "collect-company-evidence", run_id, exc)
        raise


def screen_companies_pipeline(project_root: Path | None = None) -> dict:
    paths = get_paths(project_root)
    started_at = _now()
    run_id = _run_id("screen-companies")
    try:
        candidates = read_csv_or_empty(paths.company_candidates_path)
        if candidates.empty:
            collect_company_evidence_pipeline(project_root)
            candidates = read_csv_or_empty(paths.company_candidates_path)
        approved, candidate, rejected = split_company_buckets(candidates)
        write_csv(approved, paths.approved_companies_path)
        write_csv(candidate, paths.candidate_companies_path)
        write_csv(rejected, paths.rejected_companies_path)
        summary = {
            "approved_company_count": int(len(approved)),
            "candidate_company_count": int(len(candidate)),
            "rejected_company_count": int(len(rejected)),
            "screened_company_count": int(len(candidates)),
            "company_state_mode": "published",
        }
        _record_run(paths, "screen-companies", run_id, started_at, summary)
        return summary
    except Exception as exc:  # noqa: BLE001
        _record_error(paths, "screen-companies", run_id, exc)
        raise


def import_companies_pipeline(input_path: str, project_root: Path | None = None) -> dict:
    paths = get_paths(project_root)
    started_at = _now()
    run_id = _run_id("import-companies")
    try:
        summary = import_companies(paths, Path(input_path))
        _record_run(paths, "import-companies", run_id, started_at, summary)
        return summary
    except Exception as exc:  # noqa: BLE001
        _record_error(paths, "import-companies", run_id, exc)
        raise


def import_sources_pipeline(input_path: str, project_root: Path | None = None) -> dict:
    paths = get_paths(project_root)
    started_at = _now()
    run_id = _run_id("import-sources")
    try:
        summary = import_sources(paths, Path(input_path))
        _record_run(paths, "import-sources", run_id, started_at, summary)
        return summary
    except Exception as exc:  # noqa: BLE001
        _record_error(paths, "import-sources", run_id, exc)
        raise


def verify_sources_pipeline(project_root: Path | None = None) -> dict:
    paths = get_paths(project_root)
    settings = get_settings(project_root)
    started_at = _now()
    run_id = _run_id("verify-sources")
    try:
        registry = read_csv_or_empty(paths.source_registry_path, SOURCE_REGISTRY_COLUMNS)
        if registry.empty:
            discover_sources_pipeline(project_root)
            registry = read_csv_or_empty(paths.source_registry_path, SOURCE_REGISTRY_COLUMNS)
        _, raw_records, updated_registry, summary = collect_jobs_from_sources(
            registry,
            paths,
            settings,
            run_id=run_id,
            snapshot_date=_today(),
            collected_at=_now(),
            enable_source_scan_progress=True,
            enable_recruiter_ocr_recovery=True,
        )
        write_csv(updated_registry, paths.source_registry_path)
        verification_report = updated_registry[["company_name", "source_name", "source_url", "verification_status", "failure_count", "last_active_job_count"]]
        write_csv(verification_report, paths.source_verification_report_path)
        _record_run(paths, "verify-sources", run_id, started_at, summary)
        return summary
    except Exception as exc:  # noqa: BLE001
        _record_error(paths, "verify-sources", run_id, exc)
        raise


def collect_jobs_pipeline(
    *,
    dry_run: bool = False,
    project_root: Path | None = None,
    allow_source_discovery_fallback: bool = True,
    enable_source_scan_progress: bool = True,
    registry_frame: pd.DataFrame | None = None,
    registry_output_path: Path | None = None,
) -> dict:
    paths = get_paths(project_root)
    settings = get_settings(project_root)
    started_at = _now()
    run_id = _run_id("collect-jobs-dry-run" if dry_run else "collect-jobs")
    try:
        registry = (
            registry_frame.copy()
            if registry_frame is not None
            else read_csv_or_empty(paths.source_registry_path, SOURCE_REGISTRY_COLUMNS)
        )
        if registry.empty:
            if allow_source_discovery_fallback:
                discover_sources_pipeline(project_root)
            registry = read_csv_or_empty(paths.source_registry_path, SOURCE_REGISTRY_COLUMNS)
        if registry.empty:
            summary = _skipped_collection_summary(paths, "published_source_registry_unavailable")
            _record_run(
                paths,
                "collect-jobs --dry-run" if dry_run else "collect-jobs",
                run_id,
                started_at,
                summary,
                status="보류",
            )
            return summary
        collected_at = _now()
        snapshot_date = _today()
        jobs, raw_records, updated_registry, summary = collect_jobs_from_sources(
            registry,
            paths,
            settings,
            run_id=run_id,
            snapshot_date=snapshot_date,
            collected_at=collected_at,
            enable_source_scan_progress=enable_source_scan_progress and not dry_run,
            enable_recruiter_ocr_recovery=True,
        )
        registry_to_write = updated_registry
        if registry_frame is not None and (registry_output_path is None or registry_output_path == paths.source_registry_path):
            existing_registry = read_csv_or_empty(paths.source_registry_path, SOURCE_REGISTRY_COLUMNS)
            registry_to_write = _merge_updated_source_registry(existing_registry, updated_registry)
        if not dry_run:
            write_csv(registry_to_write, registry_output_path or paths.source_registry_path)
            filtered_jobs, dropped_jobs = filter_low_quality_jobs(jobs, settings=settings, paths=paths)
            write_csv(filtered_jobs, paths.staging_jobs_path)
            write_parquet(filtered_jobs, paths.first_snapshot_path)
            write_jsonl(raw_records, paths.raw_detail_path)
            gate = evaluate_quality_gate(filtered_jobs, registry_to_write, settings=settings, paths=paths, already_filtered=True)
            write_quality_gate(gate, paths.quality_gate_path)
            summary["quality_gate_passed"] = gate.passed
            summary["quality_gate_reasons"] = gate.reasons
            summary["dropped_low_quality_job_count"] = int(len(dropped_jobs))
        summary["collection_run_mode"] = "bootstrap_collect"
        _record_run(paths, "collect-jobs --dry-run" if dry_run else "collect-jobs", run_id, started_at, summary)
        return summary
    except Exception as exc:  # noqa: BLE001
        _record_error(paths, "collect-jobs", run_id, exc)
        raise


def update_incremental_pipeline(
    project_root: Path | None = None,
    *,
    allow_source_discovery_fallback: bool = True,
    enable_source_scan_progress: bool = True,
    registry_frame: pd.DataFrame | None = None,
    registry_output_path: Path | None = None,
) -> dict:
    paths = get_paths(project_root)
    settings = get_settings(project_root)
    started_at = _now()
    run_id = _run_id("update-incremental")
    try:
        registry = (
            registry_frame.copy()
            if registry_frame is not None
            else read_csv_or_empty(paths.source_registry_path, SOURCE_REGISTRY_COLUMNS)
        )
        if registry.empty:
            if allow_source_discovery_fallback:
                discover_sources_pipeline(project_root)
            registry = read_csv_or_empty(paths.source_registry_path, SOURCE_REGISTRY_COLUMNS)
        baseline, baseline_mode = _load_collection_baseline(paths)
        if registry.empty:
            summary = {
                **_skipped_collection_summary(paths, "published_source_registry_unavailable"),
                "incremental_baseline_mode": baseline_mode,
                **_summarize_incremental_growth(baseline, baseline),
            }
            _record_run(paths, "update-incremental", run_id, started_at, summary, status="보류")
            return summary
        collected_at = _now()
        snapshot_date = _today()
        prioritized_registry = registry.copy()
        active_source_urls = _active_source_urls(baseline)
        priority_seed_urls = _work24_priority_seed_source_urls(prioritized_registry)
        if "source_url" in prioritized_registry.columns:
            normalized_source_urls = prioritized_registry["source_url"].fillna("").astype(str).map(normalize_whitespace)
            prioritized_registry["_always_refresh_source"] = normalized_source_urls.isin(active_source_urls)
            prioritized_registry["_priority_seed_source"] = normalized_source_urls.isin(priority_seed_urls)
        new_jobs, raw_records, updated_registry, summary = collect_jobs_from_sources(
            prioritized_registry,
            paths,
            settings,
            run_id=run_id,
            snapshot_date=snapshot_date,
            collected_at=collected_at,
            enable_source_scan_progress=enable_source_scan_progress,
            enable_recruiter_ocr_recovery=True,
        )
        source_outcomes = _processed_source_outcomes(registry, updated_registry, collected_at)
        merged = merge_incremental(baseline, new_jobs, source_outcomes, run_id, snapshot_date, collected_at)
        registry_to_write = updated_registry
        if registry_frame is not None and (registry_output_path is None or registry_output_path == paths.source_registry_path):
            existing_registry = read_csv_or_empty(paths.source_registry_path, SOURCE_REGISTRY_COLUMNS)
            registry_to_write = _merge_updated_source_registry(existing_registry, updated_registry)
        write_csv(registry_to_write, registry_output_path or paths.source_registry_path)
        filtered_jobs, dropped_jobs = filter_low_quality_jobs(merged, settings=settings, paths=paths)
        write_csv(filtered_jobs, paths.staging_jobs_path)
        write_jsonl(raw_records, paths.raw_detail_path)
        snapshot_path = paths.snapshots_dir / f"{run_id}.parquet"
        write_parquet(filtered_jobs, snapshot_path)
        gate = evaluate_quality_gate(filtered_jobs, registry_to_write, settings=settings, paths=paths, already_filtered=True)
        write_quality_gate(gate, paths.quality_gate_path)
        growth_summary = _summarize_incremental_growth(baseline, filtered_jobs)
        summary.update(
            {
                "incremental_baseline_mode": baseline_mode,
                "collection_run_mode": "incremental_merge",
                "staging_job_count": int(len(filtered_jobs)),
                "dropped_low_quality_job_count": int(len(dropped_jobs)),
                "quality_gate_passed": gate.passed,
                "quality_gate_reasons": gate.reasons,
                **growth_summary,
            }
        )
        _record_run(paths, "update-incremental", run_id, started_at, summary)
        return summary
    except Exception as exc:  # noqa: BLE001
        _record_error(paths, "update-incremental", run_id, exc)
        raise


def run_weekly_expansion_pipeline(project_root: Path | None = None) -> dict:
    paths = get_paths(project_root)
    settings = get_settings(project_root)
    started_at = _now()
    run_id = _run_id("run-weekly-expansion")
    try:
        expansion_summary = expand_company_candidates_pipeline(project_root)
        evidence_summary = collect_company_evidence_pipeline(
            project_root,
            batch_size=getattr(settings, "company_evidence_batch_size", None),
            max_batches=getattr(settings, "company_evidence_max_batches_per_run", None),
        )
        published_company_state = bool(evidence_summary.get("published_company_state", False))
        if published_company_state:
            company_summary = screen_companies_pipeline(project_root)
            source_summary = discover_sources_pipeline(project_root)
            verify_summary = verify_sources_pipeline(project_root)
            published_registry = read_csv_or_empty(paths.source_registry_path, SOURCE_REGISTRY_COLUMNS)
        else:
            partial_state = _load_partial_company_scan_state(paths)
            published_candidates = partial_state["candidates"]
            published_registry = partial_state["registry"]
            company_summary = _summarize_company_bucket_frame(
                published_candidates,
                mode=str(partial_state["company_state_mode"]),
            )
            source_summary = _summarize_source_bucket_frame(
                published_registry,
                mode=str(partial_state["source_state_mode"]),
            )
            verify_summary = {
                "collection_mode": "skipped",
                "collected_job_count": 0,
                "verified_source_success_count": int((published_registry["verification_status"] == "성공").sum())
                if not published_registry.empty and "verification_status" in published_registry.columns
                else 0,
                "verified_source_failure_count": int((published_registry["verification_status"] == "실패").sum())
                if not published_registry.empty and "verification_status" in published_registry.columns
                else 0,
                "verification_mode": "deferred_until_company_scan_complete",
            }

        coverage_summary = build_coverage_report_pipeline(project_root)
        summary = {
            "run_mode": "weekly_expansion",
            "automation_ready": bool(not published_registry.empty),
            "published_state": {
                "published_company_state": published_company_state,
                "published_source_registry_ready": bool(not published_registry.empty),
                "collection_ready": False,
                "collection_ready_reason": "weekly_expansion_only",
                "allow_source_discovery_fallback": False,
                "promotion_allowed": False,
                "promotion_block_reason": "weekly_expansion_only",
            },
            "candidate_expansion": expansion_summary,
            "company_evidence": evidence_summary,
            "company_screening": company_summary,
            "source_discovery": source_summary,
            "source_verification": verify_summary,
            "coverage": coverage_summary,
        }
        _record_run(paths, "run-weekly-expansion", run_id, started_at, summary)
        return summary
    except Exception as exc:  # noqa: BLE001
        _record_error(paths, "run-weekly-expansion", run_id, exc)
        raise


def run_collection_cycle_pipeline(*, sync_sheets: bool = True, project_root: Path | None = None) -> dict:
    paths = get_paths(project_root)
    settings = get_settings(project_root)
    started_at = _now()
    run_id = _run_id("run-collection-cycle")
    try:
        expansion_summary = expand_company_candidates_pipeline(project_root)
        evidence_summary = collect_company_evidence_pipeline(
            project_root,
            batch_size=getattr(settings, "company_evidence_batch_size", None),
            max_batches=getattr(settings, "company_evidence_max_batches_per_run", None),
        )
        published_company_state = bool(evidence_summary.get("published_company_state", False))
        if published_company_state:
            company_summary = screen_companies_pipeline(project_root)
            source_summary = discover_sources_pipeline(project_root)
            published_registry = read_csv_or_empty(paths.source_registry_path, SOURCE_REGISTRY_COLUMNS)
            registry_output_path = paths.source_registry_path
            collection_ready = True
            collection_ready_reason = "published_company_state"
            allow_source_discovery_fallback = True
        else:
            partial_state = _load_partial_company_scan_state(paths)
            published_candidates = partial_state["candidates"]
            published_registry = partial_state["registry"]
            registry_output_path = partial_state["registry_output_path"]
            approved = (
                published_candidates[published_candidates["company_bucket"] == "approved"].copy()
                if not published_candidates.empty
                else pd.DataFrame(columns=list(COMPANY_CANDIDATE_COLUMNS))
            )
            candidate = (
                published_candidates[published_candidates["company_bucket"] == "candidate"].copy()
                if not published_candidates.empty
                else pd.DataFrame(columns=list(COMPANY_CANDIDATE_COLUMNS))
            )
            rejected = (
                published_candidates[published_candidates["company_bucket"] == "rejected"].copy()
                if not published_candidates.empty
                else pd.DataFrame(columns=list(COMPANY_CANDIDATE_COLUMNS))
            )
            company_summary = {
                "approved_company_count": int(len(approved)),
                "candidate_company_count": int(len(candidate)),
                "rejected_company_count": int(len(rejected)),
                "screened_company_count": int(len(published_candidates)),
                "company_state_mode": str(partial_state["company_state_mode"]),
            }
            source_summary = {
                "approved_source_count": int((published_registry["source_bucket"] == "approved").sum()) if not published_registry.empty else 0,
                "candidate_source_count": int((published_registry["source_bucket"] == "candidate").sum()) if not published_registry.empty else 0,
                "rejected_source_count": int((published_registry["source_bucket"] == "rejected").sum()) if not published_registry.empty else 0,
                "screened_source_count": int(len(published_registry)),
                "company_input_count": int(len(approved)) if not approved.empty else int(len(published_candidates)),
                "company_input_mode": str(partial_state["source_state_mode"]),
            }
            collection_ready = not published_registry.empty
            collection_ready_reason = (
                "reuse_in_progress_source_registry_during_partial_company_scan"
                if collection_ready and bool(partial_state["using_in_progress_registry"])
                else "reuse_published_source_registry_during_partial_company_scan"
            )
            if not collection_ready:
                collection_ready_reason = "published_source_registry_unavailable_while_company_scan_in_progress"
            allow_source_discovery_fallback = False

        has_master_baseline = _has_nonempty_csv(paths.master_jobs_path)
        has_staging_baseline = _has_nonempty_csv(paths.staging_jobs_path)
        if has_master_baseline:
            collection_mode = "incremental"
        elif has_staging_baseline:
            collection_mode = "bootstrap_resume"
        else:
            collection_mode = "bootstrap"

        if collection_ready:
            if collection_mode in {"incremental", "bootstrap_resume"}:
                collection_summary = update_incremental_pipeline(
                    project_root,
                    allow_source_discovery_fallback=allow_source_discovery_fallback,
                    enable_source_scan_progress=True,
                    registry_frame=published_registry,
                    registry_output_path=registry_output_path,
                )
            else:
                collection_summary = collect_jobs_pipeline(
                    dry_run=False,
                    project_root=project_root,
                    allow_source_discovery_fallback=allow_source_discovery_fallback,
                    enable_source_scan_progress=True,
                    registry_frame=published_registry,
                    registry_output_path=registry_output_path,
                )
            coverage_summary = build_coverage_report_pipeline(project_root)
            promotion_allowed = not (
                collection_mode in {"bootstrap", "bootstrap_resume"}
                and not bool(collection_summary.get("completed_full_source_scan", False))
            )
            promotion_block_reason = ""
            if not promotion_allowed:
                promotion_block_reason = "bootstrap_source_scan_incomplete"
                promote_summary = _promotion_hold_summary(
                    reason=promotion_block_reason,
                    staging_job_count=int(collection_summary.get("staging_job_count", 0)),
                    quality_gate_passed=bool(collection_summary.get("quality_gate_passed", False)),
                    quality_gate_reasons=list(collection_summary.get("quality_gate_reasons", [])),
                )
            else:
                promote_summary = promote_staging_pipeline(project_root)
        else:
            collection_summary = _skipped_collection_summary(paths, collection_ready_reason)
            coverage_summary = {"skipped": True, "reason": collection_ready_reason}
            promotion_allowed = False
            promotion_block_reason = collection_ready_reason
            promote_summary = _promotion_hold_summary(
                reason=collection_ready_reason,
                staging_job_count=int(collection_summary.get("staging_job_count", 0)),
            )

        verify_summary = {
            "collection_mode": collection_summary.get("collection_mode"),
            "collected_job_count": int(collection_summary.get("collected_job_count", 0)),
            "verified_source_success_count": int(collection_summary.get("verified_source_success_count", 0)),
            "verified_source_failure_count": int(collection_summary.get("verified_source_failure_count", 0)),
            "verification_mode": "reuse_collection_fetch",
        }

        sync_summary: dict[str, dict | None] = {"staging": None, "master": None}
        if sync_sheets and promotion_allowed:
            sync_summary["staging"] = sync_sheets_pipeline("staging", project_root)
            sync_summary["master"] = sync_sheets_pipeline("master", project_root)

        checklist = {
            "후보군_재확장": expansion_summary.get("expanded_candidate_company_count", 0) > 0,
            "기업근거_재수집": evidence_summary.get("company_evidence_count", 0) > 0,
            "승인기업_재선별": company_summary.get("approved_company_count", 0) > 0,
            "공식소스_재탐색": source_summary.get("screened_source_count", 0) > 0,
            "공식소스_검증": verify_summary.get("verified_source_success_count", 0) > 0,
            "모집단_수집_또는_증분": (
                collection_summary.get("staging_job_count", 0) > 0
                or collection_summary.get("collected_job_count", 0) > 0
            ),
            "품질게이트_통과": bool(collection_summary.get("quality_gate_passed", False)),
            "master_승격": promote_summary.get("promoted_job_count", 0) > 0,
            "시트동기화": bool(
                sync_sheets
                and sync_summary["staging"]
                and sync_summary["staging"].get("google_sheets_synced")
                and sync_summary["master"]
                and sync_summary["master"].get("google_sheets_synced")
            ),
        }
        published_state = {
            "published_company_state": bool(published_company_state),
            "published_source_registry_ready": bool(not published_registry.empty),
            "collection_ready": bool(collection_ready),
            "collection_ready_reason": collection_ready_reason,
            "allow_source_discovery_fallback": bool(allow_source_discovery_fallback),
            "promotion_allowed": bool(promotion_allowed),
            "promotion_block_reason": promotion_block_reason,
        }
        summary = {
            "run_mode": collection_mode,
            "checklist": checklist,
            "automation_ready": all(checklist.values()),
            "published_state": published_state,
            "candidate_expansion": expansion_summary,
            "company_evidence": evidence_summary,
            "company_screening": company_summary,
            "source_discovery": source_summary,
            "source_verification": verify_summary,
            "collection": collection_summary,
            "coverage": coverage_summary,
            "promotion": promote_summary,
            "sync": sync_summary,
        }
        _record_run(paths, "run-collection-cycle", run_id, started_at, summary)
        return summary
    except Exception as exc:  # noqa: BLE001
        _record_error(paths, "run-collection-cycle", run_id, exc)
        raise


def run_daily_tracking_pipeline(*, sync_sheets: bool = True, project_root: Path | None = None) -> dict:
    paths = get_paths(project_root)
    started_at = _now()
    run_id = _run_id("run-daily-tracking")
    try:
        published_registry = read_csv_or_empty(paths.source_registry_path, SOURCE_REGISTRY_COLUMNS)
        collection_ready = not published_registry.empty
        collection_ready_reason = "published_source_registry" if collection_ready else "published_source_registry_unavailable"

        if collection_ready:
            collection_summary = update_incremental_pipeline(
                project_root,
                allow_source_discovery_fallback=False,
            )
            coverage_summary = build_coverage_report_pipeline(project_root)
        else:
            collection_summary = _skipped_collection_summary(paths, collection_ready_reason)
            coverage_summary = {"skipped": True, "reason": collection_ready_reason}

        quality_gate_passed = bool(collection_summary.get("quality_gate_passed", False))
        promotion_allowed = bool(collection_ready and quality_gate_passed)
        promotion_block_reason = ""
        if not collection_ready:
            promotion_block_reason = collection_ready_reason
            promote_summary = _promotion_hold_summary(
                reason=promotion_block_reason,
                staging_job_count=int(collection_summary.get("staging_job_count", 0)),
            )
        elif not quality_gate_passed:
            promotion_block_reason = "quality_gate_failed"
            promote_summary = _promotion_hold_summary(
                reason=promotion_block_reason,
                staging_job_count=int(collection_summary.get("staging_job_count", 0)),
                quality_gate_passed=False,
                quality_gate_reasons=list(collection_summary.get("quality_gate_reasons", [])),
            )
        else:
            promote_summary = promote_staging_pipeline(project_root)
            promotion_allowed = bool(promote_summary.get("quality_gate_passed", False)) and int(
                promote_summary.get("promoted_job_count", 0)
            ) > 0
            if not promotion_allowed:
                promotion_block_reason = str(
                    (promote_summary.get("quality_gate_reasons") or ["promotion_quality_gate_failed"])[0]
                )

        sync_summary: dict[str, dict | None] = {"staging": None, "master": None}
        if sync_sheets and promotion_allowed:
            sync_summary["staging"] = sync_sheets_pipeline("staging", project_root)
            sync_summary["master"] = sync_sheets_pipeline("master", project_root)

        verify_summary = {
            "collection_mode": collection_summary.get("collection_mode"),
            "collected_job_count": int(collection_summary.get("collected_job_count", 0)),
            "verified_source_success_count": int(collection_summary.get("verified_source_success_count", 0)),
            "verified_source_failure_count": int(collection_summary.get("verified_source_failure_count", 0)),
            "verification_mode": "reuse_collection_fetch",
        }
        checklist = {
            "증분수집": (
                collection_summary.get("staging_job_count", 0) > 0
                or collection_summary.get("collected_job_count", 0) > 0
            ),
            "품질게이트_통과": quality_gate_passed,
            "master_승격": promote_summary.get("promoted_job_count", 0) > 0,
            "시트동기화": bool(
                sync_sheets
                and sync_summary["staging"]
                and sync_summary["staging"].get("google_sheets_synced")
                and sync_summary["master"]
                and sync_summary["master"].get("google_sheets_synced")
            ),
        }
        summary = {
            "run_mode": "incremental",
            "checklist": checklist,
            "automation_ready": all(checklist.values()),
            "published_state": {
                "published_company_state": True,
                "published_source_registry_ready": bool(collection_ready),
                "collection_ready": bool(collection_ready),
                "collection_ready_reason": collection_ready_reason,
                "allow_source_discovery_fallback": False,
                "promotion_allowed": bool(promotion_allowed),
                "promotion_block_reason": promotion_block_reason,
            },
            "source_verification": verify_summary,
            "collection": collection_summary,
            "coverage": coverage_summary,
            "promotion": promote_summary,
            "sync": sync_summary,
        }
        _record_run(paths, "run-daily-tracking", run_id, started_at, summary)
        return summary
    except Exception as exc:  # noqa: BLE001
        _record_error(paths, "run-daily-tracking", run_id, exc)
        raise


def promote_staging_pipeline(project_root: Path | None = None) -> dict:
    paths = get_paths(project_root)
    settings = get_settings(project_root)
    started_at = _now()
    run_id = _run_id("promote-staging")
    try:
        staging = read_csv_or_empty(paths.staging_jobs_path, JOB_COLUMNS)
        filtered_staging, dropped_jobs = filter_low_quality_jobs(staging, settings=settings, paths=paths)
        registry = read_csv_or_empty(paths.source_registry_path, SOURCE_REGISTRY_COLUMNS)
        gate = evaluate_quality_gate(filtered_staging, registry, settings=settings, paths=paths, already_filtered=True)
        write_csv(filtered_staging, paths.staging_jobs_path)
        shrink_guard = _evaluate_publish_shrink_guard(filtered_staging, paths)
        if shrink_guard["triggered"]:
            gate = gate.model_copy(
                update={
                    "passed": False,
                    "reasons": list(dict.fromkeys([*gate.reasons, str(shrink_guard["reason"])])),
                    "metrics": {
                        **gate.metrics,
                        "publish_shrink_guard": shrink_guard,
                    },
                }
            )
        else:
            gate = gate.model_copy(
                update={
                    "metrics": {
                        **gate.metrics,
                        "publish_shrink_guard": shrink_guard,
                    },
                }
            )
        write_quality_gate(gate, paths.quality_gate_path)
        summary = {
            "quality_gate_passed": gate.passed,
            "quality_gate_reasons": gate.reasons,
            "dropped_low_quality_job_count": int(len(dropped_jobs)),
            "publish_shrink_guard_triggered": bool(shrink_guard["triggered"]),
            "publish_shrink_guard_reason": str(shrink_guard["reason"]),
        }
        if gate.passed:
            write_csv(filtered_staging, paths.master_jobs_path)
            summary["promoted_job_count"] = int(len(filtered_staging))
        else:
            summary["promoted_job_count"] = 0
        _record_run(paths, "promote-staging", run_id, started_at, summary, status="성공" if gate.passed else "보류")
        return summary
    except Exception as exc:  # noqa: BLE001
        _record_error(paths, "promote-staging", run_id, exc)
        raise


def quarantine_bad_sources_pipeline(project_root: Path | None = None) -> dict:
    paths = get_paths(project_root)
    settings = get_settings(project_root)
    started_at = _now()
    run_id = _run_id("quarantine-bad-sources")
    try:
        registry = read_csv_or_empty(paths.source_registry_path, SOURCE_REGISTRY_COLUMNS)
        if registry.empty:
            return {"quarantined_source_count": 0}
        registry = registry.copy()
        condition = (registry["failure_count"].fillna(0).astype(int) >= settings.source_failure_threshold) | (
            registry["source_quality_score"].fillna(0).astype(float) < 0.35
        )
        registry.loc[condition, "is_quarantined"] = True
        registry.loc[condition & registry["quarantine_reason"].fillna("").eq(""), "quarantine_reason"] = "반복 실패 또는 저품질 소스"
        write_csv(registry, paths.source_registry_path)
        summary = {"quarantined_source_count": int(condition.sum())}
        _record_run(paths, "quarantine-bad-sources", run_id, started_at, summary)
        return summary
    except Exception as exc:  # noqa: BLE001
        _record_error(paths, "quarantine-bad-sources", run_id, exc)
        raise


def build_coverage_report_pipeline(project_root: Path | None = None) -> dict:
    paths = get_paths(project_root)
    started_at = _now()
    run_id = _run_id("build-coverage-report")
    try:
        companies = read_csv_or_empty(paths.companies_registry_path)
        source_registry = read_csv_or_empty(paths.source_registry_path, SOURCE_REGISTRY_COLUMNS)
        jobs = read_csv_or_empty(paths.staging_jobs_path, JOB_COLUMNS)
        discover_summary = _latest_run_summary(paths, "discover-companies")
        report = build_coverage_report(
            companies,
            source_registry,
            jobs,
            non_company_removed_count=int(discover_summary.get("non_company_removed_count", 0)),
        )
        write_coverage_report(report, paths.coverage_report_path)
        _record_run(paths, "build-coverage-report", run_id, started_at, report)
        return report
    except Exception as exc:  # noqa: BLE001
        _record_error(paths, "build-coverage-report", run_id, exc)
        raise


def sync_sheets_pipeline(target: str, project_root: Path | None = None) -> dict:
    paths = get_paths(project_root)
    settings = get_settings(project_root)
    started_at = _now()
    run_id = _run_id("sync-sheets")
    try:
        tabs = build_sheet_tabs(paths)
        exported_files = export_tabs_locally(paths, tabs, target)
        remote_tab_names = ["staging 탭"] if target == "staging" else [tab for tab in tabs if tab != "staging 탭"]
        synced = sync_tabs_to_google_sheets(tabs, settings, tab_names=remote_tab_names)
        summary = {
            "target": target,
            "local_export_file_count": len(exported_files),
            "google_sheets_synced": synced,
            "remote_tab_names": remote_tab_names,
        }
        _record_run(paths, "sync-sheets", run_id, started_at, summary)
        return summary
    except Exception as exc:  # noqa: BLE001
        _record_error(paths, "sync-sheets", run_id, exc)
        raise


def doctor_pipeline(project_root: Path | None = None) -> dict:
    paths = get_paths(project_root)
    started_at = _now()
    run_id = _run_id("doctor")
    try:
        summary = run_doctor(paths).model_dump()
        _record_run(paths, "doctor", run_id, started_at, summary, status="성공" if summary["passed"] else "실패")
        return summary
    except Exception as exc:  # noqa: BLE001
        _record_error(paths, "doctor", run_id, exc)
        raise
