#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
SRC_ROOT = PROJECT_ROOT / "src"
for candidate in (SRC_ROOT, PROJECT_ROOT):
    candidate_str = str(candidate)
    if candidate.exists() and candidate_str not in sys.path:
        sys.path.insert(0, candidate_str)

from jobs_market_v2.github_actions_runtime import (
    append_github_output,
    capture_cycle_status,
    capture_work24_improvement_status,
    close_incident_issue,
    finalize_cycle,
    open_or_update_incident_issue,
    resolve_cycle_status,
    send_slack_notification,
    write_cycle_status,
    write_actions_env_file,
    write_failure_state,
    write_success_or_hold_state,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    configure_env = subparsers.add_parser("configure-env")
    configure_env.add_argument("--env-path", required=True)

    capture_status = subparsers.add_parser("capture-cycle-status")
    capture_status.add_argument("--project-root", required=True)
    capture_status.add_argument("--summary-path", required=True)
    capture_status.add_argument("--status-path", required=True)
    capture_status.add_argument("--exit-code", required=True, type=int)
    capture_status.add_argument("--github-output")

    capture_work24_status = subparsers.add_parser("capture-work24-improvement-status")
    capture_work24_status.add_argument("--report-path", required=True)
    capture_work24_status.add_argument("--status-path", required=True)
    capture_work24_status.add_argument("--exit-code", required=True, type=int)
    capture_work24_status.add_argument("--github-output")

    write_status = subparsers.add_parser("write-cycle-status")
    write_status.add_argument("--status-path", required=True)
    write_status.add_argument("--exit-code", required=True, type=int)
    write_status.add_argument("--quality-gate-passed", action="store_true")
    write_status.add_argument("--hold-reason", default="")
    write_status.add_argument("--promotion-block-reason", default="")
    write_status.add_argument("--automation-ready", action="store_true")
    write_status.add_argument("--github-output")

    emit_status = subparsers.add_parser("emit-cycle-status")
    emit_status.add_argument("--project-root", required=True)
    emit_status.add_argument("--status-path", required=True)
    emit_status.add_argument("--github-output", required=True)

    write_state = subparsers.add_parser("write-workflow-state")
    write_state.add_argument("--project-root", required=True)
    write_state.add_argument("--state-path", required=True)
    write_state.add_argument("--status-path", required=True)
    write_state.add_argument("--run-url", required=True)
    write_state.add_argument("--runtime-status-path")

    finalize = subparsers.add_parser("finalize-cycle")
    finalize.add_argument("--project-root", required=True)
    finalize.add_argument("--state-path", required=True)
    finalize.add_argument("--status-path", required=True)
    finalize.add_argument("--run-url", required=True)
    finalize.add_argument("--runtime-status-path")
    finalize.add_argument("--api-url", default="")
    finalize.add_argument("--repo", default="")
    finalize.add_argument("--token", default="")
    finalize.add_argument("--slack-webhook-url", default="")
    finalize.add_argument("--result-path", required=True)

    write_failure = subparsers.add_parser("write-failure-state")
    write_failure.add_argument("--project-root", required=True)
    write_failure.add_argument("--state-path", required=True)
    write_failure.add_argument("--run-url", required=True)
    write_failure.add_argument("--runtime-status-path")
    write_failure.add_argument("--github-output")

    close_issue = subparsers.add_parser("close-incident-issue")
    close_issue.add_argument("--api-url", required=True)
    close_issue.add_argument("--repo", required=True)
    close_issue.add_argument("--token", required=True)

    open_issue = subparsers.add_parser("open-or-update-incident-issue")
    open_issue.add_argument("--api-url", required=True)
    open_issue.add_argument("--repo", required=True)
    open_issue.add_argument("--token", required=True)
    open_issue.add_argument("--run-url", required=True)
    open_issue.add_argument("--failure-streak", required=True, type=int)

    notify_slack = subparsers.add_parser("notify-slack")
    notify_slack.add_argument("--webhook-url", required=True)
    notify_slack.add_argument("--run-url", required=True)
    notify_slack.add_argument("--failure-streak", required=True, type=int)

    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    if args.command == "configure-env":
        lines = write_actions_env_file(
            Path(args.env_path),
            spreadsheet_id=os.environ.get("GOOGLE_SHEETS_SPREADSHEET_ID", ""),
            service_account_json=os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", ""),
            gemini_api_key=os.environ.get("GEMINI_API_KEY", ""),
            llm_provider=os.environ.get("JOBS_MARKET_V2_LLM_PROVIDER", ""),
            llm_base_url=os.environ.get("JOBS_MARKET_V2_LLM_BASE_URL", ""),
            llm_api_key=os.environ.get("JOBS_MARKET_V2_LLM_API_KEY", ""),
            llm_model=os.environ.get("JOBS_MARKET_V2_LLM_MODEL", ""),
        )
        print(json.dumps({"line_count": len(lines)}, ensure_ascii=False))
        return 0

    if args.command == "capture-cycle-status":
        status = capture_cycle_status(
            project_root=Path(args.project_root),
            summary_path=Path(args.summary_path),
            status_path=Path(args.status_path),
            exit_code=int(args.exit_code),
        )
        if args.github_output:
            append_github_output(Path(args.github_output), status.github_output_values())
        print(json.dumps(status.to_json(), ensure_ascii=False))
        return 0

    if args.command == "capture-work24-improvement-status":
        status = capture_work24_improvement_status(
            report_path=Path(args.report_path),
            status_path=Path(args.status_path),
            exit_code=int(args.exit_code),
        )
        if args.github_output:
            append_github_output(Path(args.github_output), status.github_output_values())
        print(json.dumps(status.to_json(), ensure_ascii=False))
        return 0

    if args.command == "write-cycle-status":
        status = write_cycle_status(
            Path(args.status_path),
            exit_code=int(args.exit_code),
            quality_gate_passed=bool(args.quality_gate_passed),
            hold_reason=args.hold_reason,
            promotion_block_reason=args.promotion_block_reason,
            automation_ready=bool(args.automation_ready),
        )
        if args.github_output:
            append_github_output(Path(args.github_output), status.github_output_values())
        print(json.dumps(status.to_json(), ensure_ascii=False))
        return 0

    if args.command == "emit-cycle-status":
        status = resolve_cycle_status(Path(args.project_root), Path(args.status_path))
        append_github_output(Path(args.github_output), status.github_output_values())
        print(json.dumps(status.to_json(), ensure_ascii=False))
        return 0

    if args.command == "write-workflow-state":
        runtime_status_path = Path(args.runtime_status_path) if args.runtime_status_path else Path(args.project_root) / "runtime" / "automation_status.json"
        status = resolve_cycle_status(Path(args.project_root), Path(args.status_path))
        payload = write_success_or_hold_state(
            state_path=Path(args.state_path),
            status=status,
            run_url=args.run_url,
            runtime_status_path=runtime_status_path,
        )
        print(json.dumps(payload, ensure_ascii=False))
        return 0

    if args.command == "finalize-cycle":
        runtime_status_path = Path(args.runtime_status_path) if args.runtime_status_path else Path(args.project_root) / "runtime" / "automation_status.json"
        result = finalize_cycle(
            project_root=Path(args.project_root),
            status_path=Path(args.status_path),
            state_path=Path(args.state_path),
            run_url=args.run_url,
            runtime_status_path=runtime_status_path,
            api_url=args.api_url,
            repo=args.repo,
            token=args.token,
            slack_webhook_url=args.slack_webhook_url,
        )
        result_path = Path(args.result_path)
        result_path.parent.mkdir(parents=True, exist_ok=True)
        result_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(result, ensure_ascii=False))
        return 0

    if args.command == "write-failure-state":
        runtime_status_path = Path(args.runtime_status_path) if args.runtime_status_path else Path(args.project_root) / "runtime" / "automation_status.json"
        payload = write_failure_state(
            state_path=Path(args.state_path),
            run_url=args.run_url,
            runtime_status_path=runtime_status_path,
        )
        if args.github_output:
            append_github_output(Path(args.github_output), {"failure_streak": str(int(payload.get("consecutive_failures", 0)))})
        print(json.dumps(payload, ensure_ascii=False))
        return 0

    if args.command == "close-incident-issue":
        closed = close_incident_issue(args.api_url, args.repo, args.token)
        print(json.dumps({"closed": closed}, ensure_ascii=False))
        return 0

    if args.command == "open-or-update-incident-issue":
        issue_number = open_or_update_incident_issue(
            args.api_url,
            args.repo,
            args.token,
            run_url=args.run_url,
            failure_streak=int(args.failure_streak),
        )
        print(json.dumps({"issue_number": issue_number}, ensure_ascii=False))
        return 0

    send_slack_notification(args.webhook_url, args.run_url, int(args.failure_streak))
    print(json.dumps({"notified": bool(args.webhook_url)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
