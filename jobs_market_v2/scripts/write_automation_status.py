#!/usr/bin/env python3
"""Write visible automation progress snapshots for hourly runs."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from jobs_market_v2.quality import evaluate_quality_gate
from jobs_market_v2.settings import ProjectPaths
from jobs_market_v2.utils import normalize_whitespace


def _now_local() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _last_successful_run_id(paths: ProjectPaths) -> str:
    runs = _read_csv(paths.runs_path)
    if runs.empty:
        return ""
    successful = runs[runs["status"].fillna("") == "성공"].copy()
    if successful.empty:
        return ""
    successful = successful.sort_values("finished_at")
    return str(successful.iloc[-1].get("run_id", "") or "")


def _safe_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _snapshot(paths: ProjectPaths) -> dict[str, object]:
    staging = _read_csv(paths.staging_jobs_path)
    master = _read_csv(paths.master_jobs_path)
    source_registry = _read_csv(paths.source_registry_path)
    approved_companies = _read_csv(paths.approved_companies_path)
    gate = evaluate_quality_gate(staging, source_registry)
    quality_score = _safe_float(gate.metrics.get("quality_score_100"))
    quality_target = _safe_float(gate.metrics.get("quality_score_target"))
    verified_success_count = 0
    if not source_registry.empty and "verification_status" in source_registry.columns:
        verified_success_count = int((source_registry["verification_status"].fillna("") == "성공").sum())
    return {
        "timestamp": _now_local(),
        "last_successful_run_id": _last_successful_run_id(paths),
        "quality_gate_passed": bool(gate.passed),
        "quality_score_100": quality_score,
        "quality_score_target": quality_target,
        "master_rows": int(len(master)),
        "staging_rows": int(len(staging)),
        "approved_companies": int(len(approved_companies)),
        "verified_source_success_count": verified_success_count,
        "quality_reasons": list(gate.reasons),
    }


def _write_json_status(paths: ProjectPaths, payload: dict[str, object]) -> None:
    status_path = paths.runtime_dir / "automation_status.json"
    status_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _append_markdown_status(paths: ProjectPaths, payload: dict[str, object]) -> None:
    status_doc = paths.root / "docs" / "AUTOMATION_STATUS.md"
    lines = [
        f"## {payload['timestamp']}",
        f"- phase: {payload.get('phase', '')}",
        f"- result: {payload.get('result', '')}",
        f"- quality_score_100: {payload.get('quality_score_100', '')}",
        f"- quality_score_target: {payload.get('quality_score_target', '')}",
        f"- master_rows: {payload.get('master_rows', '')}",
        f"- staging_rows: {payload.get('staging_rows', '')}",
        f"- approved_companies: {payload.get('approved_companies', '')}",
        f"- verified_source_success_count: {payload.get('verified_source_success_count', '')}",
        f"- last_successful_run_id: {payload.get('last_successful_run_id', '')}",
        f"- resume_next_step: {payload.get('resume_next_step', '')}",
    ]
    reasons = payload.get("quality_reasons") or []
    if reasons:
        lines.append("- quality_reasons:")
        for reason in reasons:
            lines.append(f"  - {normalize_whitespace(str(reason))}")
    lines.append("")
    with status_doc.open("a", encoding="utf-8") as handle:
        handle.write("\n".join(lines))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("start", "end"))
    parser.add_argument("--phase", default="")
    parser.add_argument("--result", default="")
    parser.add_argument("--resume-next-step", default="")
    parser.add_argument("--project-root", default=str(Path(__file__).resolve().parents[1]))
    args = parser.parse_args()

    paths = ProjectPaths.from_root(Path(args.project_root).resolve())
    paths.ensure_directories()
    snapshot = _snapshot(paths)
    payload = {
        **snapshot,
        "mode": args.mode,
        "phase": args.phase,
        "result": args.result,
        "resume_next_step": args.resume_next_step,
    }
    _write_json_status(paths, payload)
    if args.mode == "end":
        _append_markdown_status(paths, payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
