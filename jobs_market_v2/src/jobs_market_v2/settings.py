"""Project settings and path helpers."""

from __future__ import annotations

import os
import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv
from pydantic import BaseModel, Field


def default_project_root() -> Path:
    env_root = os.getenv("JOBS_MARKET_V2_HOME")
    if env_root:
        return Path(env_root).expanduser().resolve()
    return Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class ProjectPaths:
    root: Path
    config_dir: Path
    runtime_dir: Path
    output_dir: Path
    notebooks_dir: Path
    scripts_dir: Path
    tests_dir: Path
    imports_dir: Path
    snapshots_dir: Path
    logs_dir: Path
    sheets_export_dir: Path
    approved_sources_path: Path
    candidate_sources_path: Path
    rejected_sources_path: Path
    source_registry_path: Path
    companies_registry_path: Path
    company_candidates_path: Path
    company_evidence_path: Path
    approved_companies_path: Path
    candidate_companies_path: Path
    rejected_companies_path: Path
    collected_company_seed_records_path: Path
    discovered_company_seed_sources_path: Path
    shadow_company_seed_sources_path: Path
    invalid_company_seed_sources_path: Path
    company_evidence_progress_path: Path
    work24_population_candidates_path: Path
    work24_population_jobs_path: Path
    work24_population_shadow_companies_path: Path
    work24_population_scan_log_path: Path
    work24_population_audit_path: Path
    work24_improvement_report_path: Path
    staging_jobs_path: Path
    master_jobs_path: Path
    raw_detail_path: Path
    runs_path: Path
    errors_path: Path
    quality_gate_path: Path
    coverage_report_path: Path
    first_snapshot_path: Path
    manual_companies_path: Path
    manual_sources_path: Path
    company_seed_records_path: Path
    company_seed_sources_path: Path
    mock_source_registry_path: Path
    source_verification_report_path: Path
    source_collection_progress_path: Path

    @classmethod
    def from_root(cls, root: Path) -> "ProjectPaths":
        runtime_dir = root / "runtime"
        imports_dir = runtime_dir / "imports"
        return cls(
            root=root,
            config_dir=root / "config",
            runtime_dir=runtime_dir,
            output_dir=root / "output_samples",
            notebooks_dir=root / "notebooks",
            scripts_dir=root / "scripts",
            tests_dir=root / "tests",
            imports_dir=imports_dir,
            snapshots_dir=runtime_dir / "snapshots",
            logs_dir=runtime_dir / "logs",
            sheets_export_dir=runtime_dir / "sheets_exports",
            approved_sources_path=root / "output_samples" / "approved_sources.csv",
            candidate_sources_path=root / "output_samples" / "candidate_sources.csv",
            rejected_sources_path=root / "output_samples" / "rejected_sources.csv",
            source_registry_path=runtime_dir / "source_registry.csv",
            companies_registry_path=runtime_dir / "companies_registry.csv",
            company_candidates_path=runtime_dir / "company_candidates.csv",
            company_evidence_path=runtime_dir / "company_evidence.csv",
            approved_companies_path=runtime_dir / "approved_companies.csv",
            candidate_companies_path=runtime_dir / "candidate_companies.csv",
            rejected_companies_path=runtime_dir / "rejected_companies.csv",
            collected_company_seed_records_path=runtime_dir / "company_seed_records_collected.csv",
            discovered_company_seed_sources_path=runtime_dir / "company_seed_sources_discovered.csv",
            shadow_company_seed_sources_path=runtime_dir / "company_seed_sources_shadow.csv",
            invalid_company_seed_sources_path=runtime_dir / "company_seed_sources_invalid.csv",
            company_evidence_progress_path=runtime_dir / "company_evidence_progress.json",
            work24_population_candidates_path=runtime_dir / "work24_population_candidates.csv",
            work24_population_jobs_path=runtime_dir / "work24_population_jobs.csv",
            work24_population_shadow_companies_path=runtime_dir / "work24_population_shadow_companies.csv",
            work24_population_scan_log_path=runtime_dir / "work24_population_scan_log.csv",
            work24_population_audit_path=runtime_dir / "work24_population_audit.json",
            work24_improvement_report_path=runtime_dir / "work24_improvement_report.json",
            staging_jobs_path=runtime_dir / "staging_jobs.csv",
            master_jobs_path=runtime_dir / "master_jobs.csv",
            raw_detail_path=runtime_dir / "raw_detail.jsonl",
            runs_path=runtime_dir / "runs.csv",
            errors_path=runtime_dir / "errors.csv",
            quality_gate_path=runtime_dir / "quality_gate.json",
            coverage_report_path=runtime_dir / "coverage_report.json",
            first_snapshot_path=root / "output_samples" / "first_snapshot_jobs.parquet",
            manual_companies_path=imports_dir / "manual_companies.csv",
            manual_sources_path=imports_dir / "manual_sources.csv",
            company_seed_records_path=root / "config" / "company_seed_records.csv",
            company_seed_sources_path=root / "config" / "company_seed_sources.yaml",
            mock_source_registry_path=root / "config" / "mock_source_payloads.yaml",
            source_verification_report_path=runtime_dir / "source_verification_report.csv",
            source_collection_progress_path=runtime_dir / "source_collection_progress.json",
        )

    def ensure_directories(self) -> None:
        for path in (
            self.runtime_dir,
            self.output_dir,
            self.imports_dir,
            self.snapshots_dir,
            self.logs_dir,
            self.sheets_export_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)


class AppSettings(BaseModel):
    timezone: str = "Asia/Seoul"
    timeout_seconds: float = Field(default=20.0)
    connect_timeout_seconds: float = Field(default=5.0)
    html_source_timeout_seconds: float = Field(default=8.0)
    html_source_connect_timeout_seconds: float = Field(default=3.0)
    ats_source_timeout_seconds: float = Field(default=10.0)
    ats_source_connect_timeout_seconds: float = Field(default=3.0)
    company_seed_timeout_seconds: float = Field(default=8.0)
    company_seed_connect_timeout_seconds: float = Field(default=3.0)
    user_agent: str = "jobs-market-v2/0.1"
    use_mock_sources: bool = False
    enable_fallback_source_guess: bool = False
    google_sheets_spreadsheet_id: str | None = None
    google_service_account_json: str | None = None
    saramin_api_access_key: str | None = None
    worknet_api_auth_key: str | None = None
    google_sheets_timeout_seconds: float = Field(default=20.0)
    google_sheets_connect_timeout_seconds: float = Field(default=5.0)
    llm_provider: str | None = None
    llm_base_url: str | None = None
    llm_api_key: str | None = None
    llm_model: str | None = None
    gemini_api_key: str | None = None
    gemini_model: str | None = None
    enable_gemini_fallback: bool = False
    enable_gemini_duplicate_adjudication: bool = False
    gemini_max_calls_per_run: int = 8
    gemini_html_listing_max_calls_per_run: int = 180
    gemini_duplicate_max_calls_per_run: int = 8
    gemini_role_salvage_max_calls_per_run: int = 64
    gemini_timeout_seconds: float = 15.0
    source_failure_threshold: int = 2
    job_collection_source_batch_size: int = 120
    job_collection_source_max_batches_per_run: int = 2
    job_collection_max_runtime_seconds: float = Field(default=300.0)
    company_evidence_batch_size: int = 200
    company_evidence_max_batches_per_run: int = 2
    company_seed_record_refresh_hours: int = 6
    company_seed_catalog_max_passes: int = 2
    company_seed_catalog_batch_size: int = 10
    company_seed_catalog_max_runtime_seconds: float = Field(default=60.0)
    company_seed_catalog_host_limit: int = 25
    company_seed_catalog_skip_cooldown_hours: int = 24
    company_seed_catalog_refresh_hours: int = 24
    company_seed_search_cooldown_hours: int = 24
    company_seed_search_query_batch_size: int = 12
    company_seed_shadow_retention_hours: int = 168
    company_seed_invalid_retention_hours: int = 336
    company_seed_shadow_max_rows: int = 5000
    company_seed_invalid_max_rows: int = 5000
    company_seed_shadow_batch_size: int = 200
    company_seed_shadow_max_batches_per_run: int = 2
    company_seed_shadow_max_runtime_seconds: float = Field(default=30.0)
    work24_population_max_pages_per_source: int = 200
    work24_population_empty_page_stop_count: int = 1
    work24_population_stale_page_stop_count: int = 2
    work24_population_page_delay_seconds: float = 0.2
    work24_population_keyword_fanout_max_terms: int = 16
    work24_population_keyword_fanout_max_pages_per_term: int = 5


def _load_published_llm_backend_config(paths: ProjectPaths) -> dict[str, str]:
    config_path = paths.config_dir / "published_llm_backend.json"
    if not config_path.exists():
        return {}
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    return {str(key): str(value) for key, value in payload.items() if value is not None}


def _parse_env_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "y"}


@lru_cache(maxsize=1)
def get_paths(project_root: Path | None = None) -> ProjectPaths:
    paths = ProjectPaths.from_root(project_root or default_project_root())
    paths.ensure_directories()
    return paths


@lru_cache(maxsize=1)
def get_settings(project_root: Path | None = None) -> AppSettings:
    paths = get_paths(project_root)
    load_dotenv(paths.root / ".env", override=True)
    llm_defaults = _load_published_llm_backend_config(paths)
    llm_api_key = os.getenv("JOBS_MARKET_V2_LLM_API_KEY")
    legacy_llm_api_key = os.getenv("GEMINI_API_KEY")
    enable_llm_fallback_raw = os.getenv("JOBS_MARKET_V2_ENABLE_LLM_FALLBACK")
    legacy_enable_llm_fallback_raw = os.getenv("JOBS_MARKET_V2_ENABLE_GEMINI_FALLBACK")
    if enable_llm_fallback_raw is not None:
        enable_gemini_fallback = _parse_env_bool(enable_llm_fallback_raw)
    elif legacy_enable_llm_fallback_raw is not None:
        enable_gemini_fallback = _parse_env_bool(legacy_enable_llm_fallback_raw)
    else:
        enable_gemini_fallback = bool(llm_api_key or legacy_llm_api_key)

    duplicate_adjudication_raw = os.getenv("JOBS_MARKET_V2_ENABLE_LLM_DUPLICATE_ADJUDICATION")
    if duplicate_adjudication_raw is None:
        duplicate_adjudication_raw = os.getenv("JOBS_MARKET_V2_ENABLE_GEMINI_DUPLICATE_ADJUDICATION")

    llm_max_calls_per_run = int(
        os.getenv("JOBS_MARKET_V2_LLM_MAX_CALLS_PER_RUN") or os.getenv("JOBS_MARKET_V2_GEMINI_MAX_CALLS_PER_RUN", "8")
    )
    llm_timeout_seconds = float(
        os.getenv("JOBS_MARKET_V2_LLM_TIMEOUT_SECONDS") or os.getenv("JOBS_MARKET_V2_GEMINI_TIMEOUT_SECONDS", "15")
    )
    return AppSettings(
        timeout_seconds=float(os.getenv("JOBS_MARKET_V2_TIMEOUT_SECONDS", "20")),
        connect_timeout_seconds=float(os.getenv("JOBS_MARKET_V2_CONNECT_TIMEOUT_SECONDS", "5")),
        html_source_timeout_seconds=float(os.getenv("JOBS_MARKET_V2_HTML_SOURCE_TIMEOUT_SECONDS", "8")),
        html_source_connect_timeout_seconds=float(os.getenv("JOBS_MARKET_V2_HTML_SOURCE_CONNECT_TIMEOUT_SECONDS", "3")),
        ats_source_timeout_seconds=float(os.getenv("JOBS_MARKET_V2_ATS_SOURCE_TIMEOUT_SECONDS", "10")),
        ats_source_connect_timeout_seconds=float(os.getenv("JOBS_MARKET_V2_ATS_SOURCE_CONNECT_TIMEOUT_SECONDS", "3")),
        company_seed_timeout_seconds=float(os.getenv("JOBS_MARKET_V2_COMPANY_SEED_TIMEOUT_SECONDS", "8")),
        company_seed_connect_timeout_seconds=float(os.getenv("JOBS_MARKET_V2_COMPANY_SEED_CONNECT_TIMEOUT_SECONDS", "3")),
        user_agent=os.getenv("JOBS_MARKET_V2_USER_AGENT", "jobs-market-v2/0.1"),
        use_mock_sources=os.getenv("JOBS_MARKET_V2_USE_MOCK_SOURCES", "false").lower() in {"1", "true", "yes", "y"},
        enable_fallback_source_guess=os.getenv("JOBS_MARKET_V2_ENABLE_FALLBACK_SOURCE_GUESS", "false").lower() in {"1", "true", "yes", "y"},
        google_sheets_spreadsheet_id=os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID"),
        google_service_account_json=os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"),
        saramin_api_access_key=os.getenv("JOBS_MARKET_V2_SARAMIN_API_ACCESS_KEY"),
        worknet_api_auth_key=os.getenv("JOBS_MARKET_V2_WORKNET_API_AUTH_KEY"),
        google_sheets_timeout_seconds=float(os.getenv("JOBS_MARKET_V2_GOOGLE_SHEETS_TIMEOUT_SECONDS", "20")),
        google_sheets_connect_timeout_seconds=float(os.getenv("JOBS_MARKET_V2_GOOGLE_SHEETS_CONNECT_TIMEOUT_SECONDS", "5")),
        llm_provider=os.getenv("JOBS_MARKET_V2_LLM_PROVIDER") or llm_defaults.get("provider"),
        llm_base_url=os.getenv("JOBS_MARKET_V2_LLM_BASE_URL") or llm_defaults.get("base_url"),
        llm_api_key=llm_api_key,
        llm_model=os.getenv("JOBS_MARKET_V2_LLM_MODEL") or llm_defaults.get("model"),
        gemini_api_key=legacy_llm_api_key,
        gemini_model=os.getenv("JOBS_MARKET_V2_GEMINI_MODEL", "gemini-2.5-flash"),
        enable_gemini_fallback=enable_gemini_fallback,
        enable_gemini_duplicate_adjudication=(
            _parse_env_bool(duplicate_adjudication_raw)
            if duplicate_adjudication_raw is not None
            else enable_gemini_fallback
        ),
        gemini_max_calls_per_run=llm_max_calls_per_run,
        gemini_html_listing_max_calls_per_run=int(os.getenv("JOBS_MARKET_V2_GEMINI_HTML_LISTING_MAX_CALLS_PER_RUN", "180")),
        gemini_duplicate_max_calls_per_run=int(os.getenv("JOBS_MARKET_V2_GEMINI_DUPLICATE_MAX_CALLS_PER_RUN", "8")),
        gemini_role_salvage_max_calls_per_run=int(os.getenv("JOBS_MARKET_V2_GEMINI_ROLE_SALVAGE_MAX_CALLS_PER_RUN", "64")),
        gemini_timeout_seconds=llm_timeout_seconds,
        job_collection_source_batch_size=int(os.getenv("JOBS_MARKET_V2_JOB_COLLECTION_SOURCE_BATCH_SIZE", "120")),
        job_collection_source_max_batches_per_run=int(os.getenv("JOBS_MARKET_V2_JOB_COLLECTION_SOURCE_MAX_BATCHES_PER_RUN", "2")),
        job_collection_max_runtime_seconds=float(os.getenv("JOBS_MARKET_V2_JOB_COLLECTION_MAX_RUNTIME_SECONDS", "300")),
        company_evidence_batch_size=int(os.getenv("JOBS_MARKET_V2_COMPANY_EVIDENCE_BATCH_SIZE", "200")),
        company_evidence_max_batches_per_run=int(os.getenv("JOBS_MARKET_V2_COMPANY_EVIDENCE_MAX_BATCHES_PER_RUN", "2")),
        company_seed_record_refresh_hours=int(os.getenv("JOBS_MARKET_V2_COMPANY_SEED_RECORD_REFRESH_HOURS", "6")),
        company_seed_catalog_max_passes=int(os.getenv("JOBS_MARKET_V2_COMPANY_SEED_CATALOG_MAX_PASSES", "2")),
        company_seed_catalog_batch_size=int(os.getenv("JOBS_MARKET_V2_COMPANY_SEED_CATALOG_BATCH_SIZE", "10")),
        company_seed_catalog_max_runtime_seconds=float(
            os.getenv("JOBS_MARKET_V2_COMPANY_SEED_CATALOG_MAX_RUNTIME_SECONDS", "60")
        ),
        company_seed_catalog_host_limit=int(os.getenv("JOBS_MARKET_V2_COMPANY_SEED_CATALOG_HOST_LIMIT", "25")),
        company_seed_catalog_skip_cooldown_hours=int(os.getenv("JOBS_MARKET_V2_COMPANY_SEED_CATALOG_SKIP_COOLDOWN_HOURS", "24")),
        company_seed_catalog_refresh_hours=int(os.getenv("JOBS_MARKET_V2_COMPANY_SEED_CATALOG_REFRESH_HOURS", "24")),
        company_seed_search_cooldown_hours=int(os.getenv("JOBS_MARKET_V2_COMPANY_SEED_SEARCH_COOLDOWN_HOURS", "24")),
        company_seed_search_query_batch_size=int(os.getenv("JOBS_MARKET_V2_COMPANY_SEED_SEARCH_QUERY_BATCH_SIZE", "12")),
        company_seed_shadow_retention_hours=int(os.getenv("JOBS_MARKET_V2_COMPANY_SEED_SHADOW_RETENTION_HOURS", "168")),
        company_seed_invalid_retention_hours=int(os.getenv("JOBS_MARKET_V2_COMPANY_SEED_INVALID_RETENTION_HOURS", "336")),
        company_seed_shadow_max_rows=int(os.getenv("JOBS_MARKET_V2_COMPANY_SEED_SHADOW_MAX_ROWS", "5000")),
        company_seed_invalid_max_rows=int(os.getenv("JOBS_MARKET_V2_COMPANY_SEED_INVALID_MAX_ROWS", "5000")),
        company_seed_shadow_batch_size=int(os.getenv("JOBS_MARKET_V2_COMPANY_SEED_SHADOW_BATCH_SIZE", "200")),
        company_seed_shadow_max_batches_per_run=int(os.getenv("JOBS_MARKET_V2_COMPANY_SEED_SHADOW_MAX_BATCHES_PER_RUN", "2")),
        company_seed_shadow_max_runtime_seconds=float(
            os.getenv("JOBS_MARKET_V2_COMPANY_SEED_SHADOW_MAX_RUNTIME_SECONDS", "30")
        ),
        work24_population_max_pages_per_source=int(os.getenv("JOBS_MARKET_V2_WORK24_POPULATION_MAX_PAGES_PER_SOURCE", "200")),
        work24_population_empty_page_stop_count=int(os.getenv("JOBS_MARKET_V2_WORK24_POPULATION_EMPTY_PAGE_STOP_COUNT", "1")),
        work24_population_stale_page_stop_count=int(os.getenv("JOBS_MARKET_V2_WORK24_POPULATION_STALE_PAGE_STOP_COUNT", "2")),
        work24_population_page_delay_seconds=float(os.getenv("JOBS_MARKET_V2_WORK24_POPULATION_PAGE_DELAY_SECONDS", "0.2")),
        work24_population_keyword_fanout_max_terms=int(
            os.getenv("JOBS_MARKET_V2_WORK24_POPULATION_KEYWORD_FANOUT_MAX_TERMS", "16")
        ),
        work24_population_keyword_fanout_max_pages_per_term=int(
            os.getenv("JOBS_MARKET_V2_WORK24_POPULATION_KEYWORD_FANOUT_MAX_PAGES_PER_TERM", "5")
        ),
    )
