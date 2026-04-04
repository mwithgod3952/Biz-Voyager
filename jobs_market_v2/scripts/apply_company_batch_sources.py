from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from jobs_market_v2.constants import SOURCE_REGISTRY_COLUMNS
from jobs_market_v2.discovery import discover_source_candidates, import_companies
from jobs_market_v2.pipelines import (
    _merge_updated_source_registry,
    discover_companies_pipeline,
    doctor_pipeline,
    promote_staging_pipeline,
    sync_sheets_pipeline,
    update_incremental_pipeline,
)
from jobs_market_v2.screening import screen_sources
from jobs_market_v2.settings import get_paths, get_settings
from jobs_market_v2.storage import read_csv_or_empty, write_csv


def _normalize_domain(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return value.lower().strip().replace("www.", "")


def _merge_registry_like(existing_path: Path, incoming: pd.DataFrame) -> pd.DataFrame:
    existing = read_csv_or_empty(existing_path, SOURCE_REGISTRY_COLUMNS)
    merged = _merge_updated_source_registry(existing, incoming.reindex(columns=list(SOURCE_REGISTRY_COLUMNS)))
    write_csv(merged, existing_path)
    return merged


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("batch_csv")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    paths = get_paths(project_root)
    settings = get_settings(project_root)

    batch_path = (project_root / args.batch_csv).resolve()
    import_companies(paths, batch_path)
    discover_companies_pipeline(project_root=project_root)
    batch = pd.read_csv(batch_path)
    batch_names = {str(value).strip() for value in batch.get("company_name", []) if isinstance(value, str) and value.strip()}
    batch_domains = {_normalize_domain(value) for value in batch.get("official_domain", []) if isinstance(value, str) and value.strip()}

    companies = pd.read_csv(paths.companies_registry_path)
    companies["official_domain_norm"] = companies.get("official_domain", pd.Series(dtype=str)).map(_normalize_domain)
    name_matched = companies[companies["company_name"].astype(str).str.strip().isin(batch_names)].copy()
    matched_domains = set(name_matched["official_domain_norm"].dropna().astype(str))
    unresolved_domains = {domain for domain in batch_domains if domain and domain not in matched_domains}
    domain_fallback = companies[companies["official_domain_norm"].isin(unresolved_domains)].copy()
    subset = pd.concat([name_matched, domain_fallback], ignore_index=True).drop_duplicates().reset_index(drop=True)
    subset = subset.drop(columns=["official_domain_norm"], errors="ignore").reset_index(drop=True)

    source_candidates = discover_source_candidates(subset, paths, settings=settings)
    approved, candidate, rejected, registry = screen_sources(source_candidates)

    merged_registry = _merge_registry_like(paths.source_registry_path, registry)
    write_csv(merged_registry[merged_registry["source_bucket"] == "approved"].copy(), paths.approved_sources_path)
    write_csv(merged_registry[merged_registry["source_bucket"] == "candidate"].copy(), paths.candidate_sources_path)
    write_csv(merged_registry[merged_registry["source_bucket"] == "rejected"].copy(), paths.rejected_sources_path)

    summary = {
        "batch_csv": str(batch_path),
        "subset_company_count": int(len(subset)),
        "source_candidate_count": int(len(source_candidates)),
        "approved_count": int(len(approved)),
        "candidate_count": int(len(candidate)),
        "rejected_count": int(len(rejected)),
    }

    if not approved.empty:
        collection_summary = update_incremental_pipeline(
            project_root=project_root,
            allow_source_discovery_fallback=False,
            enable_source_scan_progress=False,
            registry_frame=approved,
            registry_output_path=paths.source_registry_path,
        )
        summary.update(
            {
                "collected_job_count": int(collection_summary.get("collected_job_count", 0)),
                "new_job_count": int(collection_summary.get("new_job_count", 0)),
                "staging_job_count": int(collection_summary.get("staging_job_count", 0)),
                "quality_gate_passed": bool(collection_summary.get("quality_gate_passed", False)),
            }
        )
        if summary["quality_gate_passed"]:
            promote_summary = promote_staging_pipeline(project_root=project_root)
            master_sync = sync_sheets_pipeline("master", project_root=project_root)
            staging_sync = sync_sheets_pipeline("staging", project_root=project_root)
            doctor_summary = doctor_pipeline(project_root=project_root)
            summary.update(
                {
                    "promoted_job_count": int(promote_summary.get("promoted_job_count", 0)),
                    "master_sheet_sync": bool(master_sync.get("google_sheets_synced", False)),
                    "staging_sheet_sync": bool(staging_sync.get("google_sheets_synced", False)),
                    "doctor_passed": bool(doctor_summary.get("passed", False)),
                }
            )

    print(summary)


if __name__ == "__main__":
    main()
