"""Command line interface for jobs_market_v2."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime
import json
import os
import sys

import fcntl

from .pipelines import (
    build_coverage_report_pipeline,
    collect_company_seed_records_pipeline,
    collect_jobs_pipeline,
    collect_company_evidence_pipeline,
    discover_company_seed_sources_pipeline,
    discover_companies_pipeline,
    discover_sources_pipeline,
    doctor_pipeline,
    expand_company_candidates_pipeline,
    import_companies_pipeline,
    import_sources_pipeline,
    promote_shadow_seed_sources_pipeline,
    promote_staging_pipeline,
    quarantine_bad_sources_pipeline,
    run_collection_cycle_pipeline,
    screen_companies_pipeline,
    sync_sheets_pipeline,
    update_incremental_pipeline,
    verify_sources_pipeline,
)
from .settings import get_paths


_LOCKED_COMMANDS = {
    "discover-companies",
    "discover-company-seed-sources",
    "promote-shadow-seed-sources",
    "collect-company-seed-records",
    "expand-company-candidates",
    "collect-company-evidence",
    "screen-companies",
    "discover-sources",
    "verify-sources",
    "run-collection-cycle",
    "import-companies",
    "import-sources",
    "collect-jobs",
    "update-incremental",
    "promote-staging",
    "quarantine-bad-sources",
    "build-coverage-report",
    "sync-sheets",
}


def _print(summary: dict) -> None:
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def _summary_exit_code(command: str, summary: dict) -> int:
    if command == "doctor":
        return 0 if summary.get("passed") else 1
    if command == "run-collection-cycle":
        # run-collection-cycle can complete successfully while intentionally
        # holding promotion/sync, so automation policy should be decided by the
        # caller from the returned summary rather than by a generic shell code.
        return 0
    if command in {"update-incremental", "collect-jobs"}:
        return 0 if summary.get("quality_gate_passed", False) else 1
    if command == "promote-staging":
        return 0 if summary.get("quality_gate_passed", False) and int(summary.get("promoted_job_count", 0)) > 0 else 1
    if command == "sync-sheets":
        return 0 if summary.get("google_sheets_synced") else 1
    return 0


@contextmanager
def _runtime_command_lock(command: str):
    if command not in _LOCKED_COMMANDS:
        yield
        return

    paths = get_paths()
    paths.ensure_directories()
    lock_path = paths.runtime_dir / ".runtime.lock"
    with lock_path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            handle.seek(0)
            holder = handle.read().strip()
            raise RuntimeError(
                f"다른 runtime 명령이 이미 실행 중입니다. 순차 실행 후 다시 시도하세요. {holder}".strip()
            ) from exc
        metadata = {
            "command": command,
            "pid": os.getpid(),
            "started_at": datetime.now().isoformat(timespec="seconds"),
        }
        handle.seek(0)
        handle.truncate()
        handle.write(json.dumps(metadata, ensure_ascii=False))
        handle.flush()
        os.fsync(handle.fileno())
        try:
            yield
        finally:
            handle.seek(0)
            handle.truncate()
            handle.flush()
            os.fsync(handle.fileno())
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="국내 채용시장 수집 파이프라인 CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("discover-companies")
    subparsers.add_parser("discover-company-seed-sources")
    subparsers.add_parser("promote-shadow-seed-sources")
    subparsers.add_parser("collect-company-seed-records")
    subparsers.add_parser("expand-company-candidates")
    collect_company_evidence_parser = subparsers.add_parser("collect-company-evidence")
    collect_company_evidence_parser.add_argument("--batch-size", type=int, default=None)
    collect_company_evidence_parser.add_argument("--max-batches", type=int, default=None)
    collect_company_evidence_parser.add_argument("--no-resume", action="store_true")
    subparsers.add_parser("screen-companies")
    subparsers.add_parser("discover-sources")
    subparsers.add_parser("verify-sources")
    run_collection_cycle_parser = subparsers.add_parser("run-collection-cycle")
    run_collection_cycle_parser.add_argument("--skip-sync", action="store_true")
    subparsers.add_parser("update-incremental")
    subparsers.add_parser("promote-staging")
    subparsers.add_parser("quarantine-bad-sources")
    subparsers.add_parser("build-coverage-report")
    subparsers.add_parser("doctor")

    import_companies_parser = subparsers.add_parser("import-companies")
    import_companies_parser.add_argument("input_path")

    import_sources_parser = subparsers.add_parser("import-sources")
    import_sources_parser.add_argument("input_path")

    collect_jobs_parser = subparsers.add_parser("collect-jobs")
    collect_jobs_parser.add_argument("--dry-run", action="store_true")

    sync_sheets_parser = subparsers.add_parser("sync-sheets")
    sync_sheets_parser.add_argument("--target", choices=["staging", "master"], required=True)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        with _runtime_command_lock(args.command):
            if args.command == "discover-companies":
                summary = discover_companies_pipeline()
                _print(summary)
                return _summary_exit_code(args.command, summary)
            elif args.command == "discover-company-seed-sources":
                summary = discover_company_seed_sources_pipeline()
                _print(summary)
                return _summary_exit_code(args.command, summary)
            elif args.command == "promote-shadow-seed-sources":
                summary = promote_shadow_seed_sources_pipeline()
                _print(summary)
                return _summary_exit_code(args.command, summary)
            elif args.command == "collect-company-seed-records":
                summary = collect_company_seed_records_pipeline()
                _print(summary)
                return _summary_exit_code(args.command, summary)
            elif args.command == "expand-company-candidates":
                summary = expand_company_candidates_pipeline()
                _print(summary)
                return _summary_exit_code(args.command, summary)
            elif args.command == "collect-company-evidence":
                summary = collect_company_evidence_pipeline(
                    batch_size=args.batch_size,
                    max_batches=args.max_batches,
                    resume=not args.no_resume,
                )
                _print(summary)
                return _summary_exit_code(args.command, summary)
            elif args.command == "screen-companies":
                summary = screen_companies_pipeline()
                _print(summary)
                return _summary_exit_code(args.command, summary)
            elif args.command == "discover-sources":
                summary = discover_sources_pipeline()
                _print(summary)
                return _summary_exit_code(args.command, summary)
            elif args.command == "verify-sources":
                summary = verify_sources_pipeline()
                _print(summary)
                return _summary_exit_code(args.command, summary)
            elif args.command == "run-collection-cycle":
                summary = run_collection_cycle_pipeline(sync_sheets=not args.skip_sync)
                _print(summary)
                return _summary_exit_code(args.command, summary)
            elif args.command == "import-companies":
                summary = import_companies_pipeline(args.input_path)
                _print(summary)
                return _summary_exit_code(args.command, summary)
            elif args.command == "import-sources":
                summary = import_sources_pipeline(args.input_path)
                _print(summary)
                return _summary_exit_code(args.command, summary)
            elif args.command == "collect-jobs":
                summary = collect_jobs_pipeline(dry_run=args.dry_run)
                _print(summary)
                return _summary_exit_code(args.command, summary)
            elif args.command == "update-incremental":
                summary = update_incremental_pipeline()
                _print(summary)
                return _summary_exit_code(args.command, summary)
            elif args.command == "promote-staging":
                summary = promote_staging_pipeline()
                _print(summary)
                return _summary_exit_code(args.command, summary)
            elif args.command == "quarantine-bad-sources":
                summary = quarantine_bad_sources_pipeline()
                _print(summary)
                return _summary_exit_code(args.command, summary)
            elif args.command == "build-coverage-report":
                summary = build_coverage_report_pipeline()
                _print(summary)
                return _summary_exit_code(args.command, summary)
            elif args.command == "sync-sheets":
                summary = sync_sheets_pipeline(args.target)
                _print(summary)
                return _summary_exit_code(args.command, summary)
            elif args.command == "doctor":
                summary = doctor_pipeline()
                _print(summary)
                return _summary_exit_code(args.command, summary)
            else:
                parser.error("지원하지 않는 명령입니다.")
    except Exception as exc:  # noqa: BLE001
        print(
            json.dumps(
                {"status": "실패", "error": str(exc) or repr(exc)},
                ensure_ascii=False,
                indent=2,
            ),
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
