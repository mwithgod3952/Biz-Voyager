from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any
import urllib.parse
import urllib.request


INCIDENT_TITLE = "[Automation] jobs_market_v2 production failures"
INCIDENT_LABELS = ("automation", "incident")
DEFAULT_ENV_FLAGS = (
    ("JOBS_MARKET_V2_USE_MOCK_SOURCES", "false"),
    ("JOBS_MARKET_V2_ENABLE_FALLBACK_SOURCE_GUESS", "false"),
    ("JOBS_MARKET_V2_ENABLE_GEMINI_FALLBACK", "true"),
    ("JOBS_MARKET_V2_GEMINI_MAX_CALLS_PER_RUN", "24"),
    ("JOBS_MARKET_V2_JOB_COLLECTION_MAX_RUNTIME_SECONDS", "1800"),
)


@dataclass(frozen=True)
class WorkflowCycleStatus:
    exit_code: int
    quality_gate_passed: bool
    hold_reason: str = ""
    promotion_block_reason: str = ""
    automation_ready: bool = False

    @property
    def should_save_state(self) -> bool:
        return self.exit_code == 0 or bool(self.hold_reason)

    @property
    def is_failure(self) -> bool:
        return not self.hold_reason and (self.exit_code != 0 or not self.quality_gate_passed)

    @property
    def is_recovered(self) -> bool:
        return not self.hold_reason and self.exit_code == 0 and self.quality_gate_passed

    @property
    def last_result(self) -> str:
        if self.hold_reason:
            return "hold"
        if self.quality_gate_passed:
            return "success"
        return "failure"

    def github_output_values(self) -> dict[str, str]:
        return {
            "exit_code": str(self.exit_code),
            "quality_gate_passed": str(self.quality_gate_passed).lower(),
            "hold_reason": self.hold_reason,
            "should_save_state": str(self.should_save_state).lower(),
            "is_failure": str(self.is_failure).lower(),
            "is_recovered": str(self.is_recovered).lower(),
        }

    def to_json(self) -> dict[str, Any]:
        return {
            "exit_code": self.exit_code,
            "quality_gate_passed": self.quality_gate_passed,
            "hold_reason": self.hold_reason,
            "promotion_block_reason": self.promotion_block_reason,
            "automation_ready": self.automation_ready,
        }


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _int_or_default(value: Any, default: int) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def _derive_hold_reason(summary: dict[str, Any]) -> str:
    run_mode = str(summary.get("run_mode", "") or "")
    published_state = summary.get("published_state", {}) if isinstance(summary, dict) else {}
    collection_summary = summary.get("collection", {}) if isinstance(summary, dict) else {}
    promotion_block_reason = str(published_state.get("promotion_block_reason", "") or "")
    collection_ready = bool(published_state.get("collection_ready", True))
    collection_ready_reason = str(published_state.get("collection_ready_reason", "") or "")
    completed_full_source_scan = bool(collection_summary.get("completed_full_source_scan", True))

    if run_mode not in {"bootstrap", "bootstrap_resume"}:
        return ""
    if not collection_ready:
        return collection_ready_reason or "bootstrap_collection_not_ready"
    if promotion_block_reason == "bootstrap_source_scan_incomplete":
        return promotion_block_reason
    if not completed_full_source_scan:
        return "bootstrap_source_scan_incomplete"
    return ""


def derive_cycle_status(summary: dict[str, Any], exit_code: int, quality_gate_fallback: bool = False) -> WorkflowCycleStatus:
    published_state = summary.get("published_state", {}) if isinstance(summary, dict) else {}
    collection_summary = summary.get("collection", {}) if isinstance(summary, dict) else {}
    promotion_block_reason = str(published_state.get("promotion_block_reason", "") or "")
    quality_gate_passed = bool(collection_summary.get("quality_gate_passed", quality_gate_fallback))
    return WorkflowCycleStatus(
        exit_code=exit_code,
        quality_gate_passed=quality_gate_passed,
        hold_reason=_derive_hold_reason(summary),
        promotion_block_reason=promotion_block_reason,
        automation_ready=bool(summary.get("automation_ready", False)),
    )


def capture_cycle_status(project_root: Path, summary_path: Path, status_path: Path, exit_code: int) -> WorkflowCycleStatus:
    project_root = project_root.resolve()
    summary = _load_json(summary_path)
    quality_gate_path = project_root / "runtime/quality_gate.json"
    quality_gate_fallback = bool(_load_json(quality_gate_path).get("passed", False))
    status = derive_cycle_status(summary, exit_code=exit_code, quality_gate_fallback=quality_gate_fallback)
    _write_json(status_path, status.to_json())
    return status


def resolve_cycle_status(project_root: Path, status_path: Path) -> WorkflowCycleStatus:
    project_root = project_root.resolve()
    payload = _load_json(status_path)
    if payload:
        return WorkflowCycleStatus(
            exit_code=_int_or_default(payload.get("exit_code"), 1),
            quality_gate_passed=bool(payload.get("quality_gate_passed", False)),
            hold_reason=str(payload.get("hold_reason", "") or ""),
            promotion_block_reason=str(payload.get("promotion_block_reason", "") or ""),
            automation_ready=bool(payload.get("automation_ready", False)),
        )
    summary_path = project_root / "runtime/github_workflow_cycle_summary.json"
    quality_gate_path = project_root / "runtime/quality_gate.json"
    return derive_cycle_status(
        _load_json(summary_path),
        exit_code=1,
        quality_gate_fallback=bool(_load_json(quality_gate_path).get("passed", False)),
    )


def append_github_output(output_path: Path, values: dict[str, str]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("a", encoding="utf-8") as handle:
        for key, raw_value in values.items():
            value = str(raw_value).replace("\r", " ").replace("\n", " ")
            handle.write(f"{key}={value}\n")


def shell_quote_env_value(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"


def _materialize_service_account_json(service_account_json: str, env_dir: Path) -> str:
    candidate = Path(service_account_json).expanduser()
    if candidate.exists():
        return str(candidate)
    payload = json.loads(service_account_json)
    service_account_path = env_dir / ".ci" / "google_service_account.json"
    service_account_path.parent.mkdir(parents=True, exist_ok=True)
    service_account_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return str(service_account_path)


def write_actions_env_file(
    env_path: Path,
    spreadsheet_id: str,
    service_account_json: str,
    gemini_api_key: str = "",
) -> list[str]:
    if not spreadsheet_id or not service_account_json:
        raise ValueError("Missing required secrets: GOOGLE_SHEETS_SPREADSHEET_ID, GOOGLE_SERVICE_ACCOUNT_JSON")
    env_path = env_path.resolve()
    service_account_value = _materialize_service_account_json(service_account_json, env_path.parent)
    lines = [
        f"GOOGLE_SHEETS_SPREADSHEET_ID={shell_quote_env_value(spreadsheet_id)}",
        f"GOOGLE_SERVICE_ACCOUNT_JSON={shell_quote_env_value(service_account_value)}",
    ]
    lines.extend(f"{key}={value}" for key, value in DEFAULT_ENV_FLAGS)
    if gemini_api_key:
        lines.append(f"GEMINI_API_KEY={shell_quote_env_value(gemini_api_key)}")
    env_path.parent.mkdir(parents=True, exist_ok=True)
    env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return lines


def build_workflow_state_payload(
    existing_payload: dict[str, Any],
    status: WorkflowCycleStatus,
    run_url: str,
    runtime_status: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = dict(existing_payload)
    payload.update(
        {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "consecutive_failures": 0 if status.should_save_state and not status.is_failure else 1,
            "last_result": status.last_result,
            "hold_reason": status.hold_reason,
            "run_url": run_url,
        }
    )
    if runtime_status is not None:
        payload["automation_status"] = runtime_status
    return payload


def build_failure_state_payload(
    existing_payload: dict[str, Any],
    run_url: str,
    runtime_status: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = dict(existing_payload)
    payload["updated_at"] = datetime.now(timezone.utc).isoformat()
    payload["consecutive_failures"] = int(payload.get("consecutive_failures", 0)) + 1
    payload["last_result"] = "failure"
    payload["hold_reason"] = ""
    payload["run_url"] = run_url
    if runtime_status is not None:
        payload["automation_status"] = runtime_status
    return payload


def _load_runtime_status(runtime_status_path: Path) -> dict[str, Any] | None:
    payload = _load_json(runtime_status_path)
    return payload or None


def write_success_or_hold_state(
    state_path: Path,
    status: WorkflowCycleStatus,
    run_url: str,
    runtime_status_path: Path,
) -> dict[str, Any]:
    existing_payload = _load_json(state_path)
    runtime_status = _load_runtime_status(runtime_status_path)
    payload = build_workflow_state_payload(existing_payload, status, run_url, runtime_status)
    _write_json(state_path, payload)
    return payload


def write_failure_state(
    state_path: Path,
    run_url: str,
    runtime_status_path: Path,
) -> dict[str, Any]:
    existing_payload = _load_json(state_path)
    runtime_status = _load_runtime_status(runtime_status_path)
    payload = build_failure_state_payload(existing_payload, run_url, runtime_status)
    _write_json(state_path, payload)
    return payload


def finalize_cycle(
    *,
    project_root: Path,
    status_path: Path,
    state_path: Path,
    run_url: str,
    runtime_status_path: Path,
    api_url: str = "",
    repo: str = "",
    token: str = "",
    slack_webhook_url: str = "",
) -> dict[str, Any]:
    status = resolve_cycle_status(project_root, status_path)
    issue_closed = False
    issue_number: int | None = None
    slack_notified = False

    if status.should_save_state:
        payload = write_success_or_hold_state(
            state_path=state_path,
            status=status,
            run_url=run_url,
            runtime_status_path=runtime_status_path,
        )
        failure_streak = int(payload.get("consecutive_failures", 0))
        if status.is_recovered and api_url and repo and token:
            issue_closed = close_incident_issue(api_url, repo, token)
    elif status.is_failure:
        payload = write_failure_state(
            state_path=state_path,
            run_url=run_url,
            runtime_status_path=runtime_status_path,
        )
        failure_streak = int(payload.get("consecutive_failures", 0))
        if api_url and repo and token:
            issue_number = open_or_update_incident_issue(
                api_url,
                repo,
                token,
                run_url=run_url,
                failure_streak=failure_streak,
            )
        if slack_webhook_url and failure_streak >= 3:
            send_slack_notification(slack_webhook_url, run_url, failure_streak)
            slack_notified = True
    else:
        payload = _load_json(state_path)
        failure_streak = int(payload.get("consecutive_failures", 0))

    return {
        "exit_code": status.exit_code,
        "quality_gate_passed": status.quality_gate_passed,
        "hold_reason": status.hold_reason,
        "should_save_state": status.should_save_state,
        "is_failure": status.is_failure,
        "is_recovered": status.is_recovered,
        "last_result": status.last_result,
        "failure_streak": failure_streak,
        "issue_closed": issue_closed,
        "issue_number": issue_number,
        "slack_notified": slack_notified,
    }


def _github_request(method: str, url: str, token: str, payload: dict[str, Any] | None = None) -> Any:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
        },
        method=method,
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        body = response.read()
    if not body:
        return None
    return json.loads(body.decode("utf-8"))


def _list_open_incident_issues(api_url: str, repo: str, token: str) -> list[dict[str, Any]]:
    encoded_labels = urllib.parse.quote(",".join(INCIDENT_LABELS), safe="")
    url = f"{api_url.rstrip('/')}/repos/{repo}/issues?state=open&labels={encoded_labels}&per_page=100"
    response = _github_request("GET", url, token)
    return response if isinstance(response, list) else []


def close_incident_issue(api_url: str, repo: str, token: str) -> bool:
    for issue in _list_open_incident_issues(api_url, repo, token):
        if issue.get("title") != INCIDENT_TITLE:
            continue
        issue_number = issue["number"]
        _github_request(
            "POST",
            f"{api_url.rstrip('/')}/repos/{repo}/issues/{issue_number}/comments",
            token,
            {"body": "A later scheduled run succeeded. Closing this automation incident automatically."},
        )
        _github_request(
            "PATCH",
            f"{api_url.rstrip('/')}/repos/{repo}/issues/{issue_number}",
            token,
            {"state": "closed"},
        )
        return True
    return False


def open_or_update_incident_issue(api_url: str, repo: str, token: str, run_url: str, failure_streak: int) -> int:
    body = (
        "jobs_market_v2 production cycle is repeatedly failing.\n\n"
        f"- Consecutive failures: {failure_streak}\n"
        f"- Latest run: {run_url}\n"
    )
    existing_issue = None
    for issue in _list_open_incident_issues(api_url, repo, token):
        if issue.get("title") == INCIDENT_TITLE:
            existing_issue = issue
            break
    if existing_issue is None:
        issue = _github_request(
            "POST",
            f"{api_url.rstrip('/')}/repos/{repo}/issues",
            token,
            {"title": INCIDENT_TITLE, "body": body, "labels": list(INCIDENT_LABELS)},
        )
        issue_number = int(issue["number"])
    else:
        issue_number = int(existing_issue["number"])
        _github_request(
            "PATCH",
            f"{api_url.rstrip('/')}/repos/{repo}/issues/{issue_number}",
            token,
            {"body": body},
        )
    _github_request(
        "POST",
        f"{api_url.rstrip('/')}/repos/{repo}/issues/{issue_number}/comments",
        token,
        {"body": f"Repeated failure detected again.\n\n- Latest run: {run_url}\n- Consecutive failures: {failure_streak}"},
    )
    return issue_number


def send_slack_notification(webhook_url: str, run_url: str, failure_streak: int) -> None:
    if not webhook_url:
        return
    payload = {
        "text": (
            f"jobs_market_v2 production cycle failed {failure_streak} times in a row.\n"
            f"Run: {run_url}"
        )
    }
    request = urllib.request.Request(
        webhook_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        if response.status >= 300:
            raise RuntimeError(f"Slack webhook failed with status {response.status}")
