"""Google Sheets export and optional sync."""

from __future__ import annotations

import json
from pathlib import Path
from time import sleep

import pandas as pd

from .constants import KOREAN_HEADER_MAP, SHEET_TAB_NAMES
from .storage import read_csv_or_empty, read_jsonl, write_csv

GOOGLE_SHEETS_MAX_CELL_CHARS = 50000
GOOGLE_SHEETS_SAFE_CELL_CHARS = 49000
GOOGLE_SHEETS_UPDATE_RETRY_ATTEMPTS = 3
GOOGLE_SHEETS_UPDATE_RETRY_BASE_SECONDS = 1.0


def _rename_headers(frame: pd.DataFrame) -> pd.DataFrame:
    renamed = frame.copy()
    renamed.columns = [KOREAN_HEADER_MAP.get(column, column) for column in renamed.columns]
    return renamed


def _truncate_sheet_cell(value: object) -> str:
    text = "" if value is None else str(value)
    if len(text) <= GOOGLE_SHEETS_MAX_CELL_CHARS:
        return text
    return text[:GOOGLE_SHEETS_SAFE_CELL_CHARS]


def _is_retryable_sheet_error(exc: Exception) -> bool:
    if isinstance(exc, TimeoutError):
        return True
    module = exc.__class__.__module__.lower()
    name = exc.__class__.__name__.lower()
    text = str(exc).lower()
    if module.startswith(("requests", "urllib3", "httpx")):
        return True
    if any(token in name for token in ("timeout", "connection")):
        return True
    if "timed out" in text or "timeout" in text or "connection reset" in text:
        return True
    return False


def _update_worksheet_with_retry(worksheet, values) -> None:
    last_error: Exception | None = None
    for attempt in range(GOOGLE_SHEETS_UPDATE_RETRY_ATTEMPTS):
        try:
            worksheet.update(values if values else [[]])
            return
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            is_last_attempt = attempt == GOOGLE_SHEETS_UPDATE_RETRY_ATTEMPTS - 1
            if is_last_attempt or not _is_retryable_sheet_error(exc):
                raise
            sleep(GOOGLE_SHEETS_UPDATE_RETRY_BASE_SECONDS * (attempt + 1))
    if last_error is not None:
        raise last_error


def _resize_worksheet_if_needed(worksheet, *, rows: int, cols: int) -> None:
    resize = getattr(worksheet, "resize", None)
    if resize is None:
        return
    current_rows = int(getattr(worksheet, "row_count", 0) or 0)
    current_cols = int(getattr(worksheet, "col_count", 0) or 0)
    target_rows = max(int(rows), 1)
    target_cols = max(int(cols), 1)
    if current_rows == target_rows and current_cols == target_cols:
        return
    resize(rows=target_rows, cols=target_cols)


def build_sheet_tabs(paths) -> dict[str, pd.DataFrame]:
    company_registry = _rename_headers(read_csv_or_empty(paths.company_candidates_path))
    company_evidence = _rename_headers(read_csv_or_empty(paths.company_evidence_path))
    staging = _rename_headers(read_csv_or_empty(paths.staging_jobs_path))
    master = _rename_headers(read_csv_or_empty(paths.master_jobs_path))
    source_registry = _rename_headers(read_csv_or_empty(paths.source_registry_path))
    runs = _rename_headers(read_csv_or_empty(paths.runs_path))
    errors = _rename_headers(read_csv_or_empty(paths.errors_path))
    raw_detail = _rename_headers(pd.DataFrame(read_jsonl(paths.raw_detail_path)))
    return {
        "기업선정 탭": company_registry,
        "기업근거 탭": company_evidence,
        "staging 탭": staging,
        "master 탭": master,
        "raw/detail 탭": raw_detail,
        "runs 탭": runs,
        "source_registry 탭": source_registry,
        "errors 탭": errors,
    }


def export_tabs_locally(paths, tabs: dict[str, pd.DataFrame], target: str) -> list[str]:
    export_dir = paths.sheets_export_dir / target
    export_dir.mkdir(parents=True, exist_ok=True)
    exported: list[str] = []
    for tab_name, frame in tabs.items():
        file_name = tab_name.replace("/", "_").replace(" ", "_") + ".csv"
        file_path = export_dir / file_name
        write_csv(frame, file_path)
        exported.append(str(file_path))
    return exported


def _service_account_info(settings) -> dict | None:
    raw = settings.google_service_account_json
    if not raw:
        return None
    candidate = Path(raw).expanduser()
    if candidate.exists():
        return json.loads(candidate.read_text(encoding="utf-8"))
    return json.loads(raw)


def sync_tabs_to_google_sheets(tabs: dict[str, pd.DataFrame], settings, *, tab_names: list[str] | None = None) -> bool:
    if not settings.google_sheets_spreadsheet_id or not settings.google_service_account_json:
        return False

    import gspread
    from google.oauth2.service_account import Credentials

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    info = _service_account_info(settings)
    credentials = Credentials.from_service_account_info(info, scopes=scopes)
    client = gspread.authorize(credentials)
    client.set_timeout(
        (
            float(getattr(settings, "google_sheets_connect_timeout_seconds", 5.0) or 5.0),
            float(getattr(settings, "google_sheets_timeout_seconds", 20.0) or 20.0),
        )
    )
    try:
        spreadsheet = client.open_by_key(settings.google_sheets_spreadsheet_id)
    except Exception as exc:  # noqa: BLE001
        service_account = info.get("client_email", "서비스 계정")
        raise PermissionError(
            f"서비스 계정 {service_account} 에 스프레드시트 접근 권한이 없습니다. "
            "해당 스프레드시트를 이 계정에 공유한 뒤 다시 실행하세요."
        ) from exc

    selected_tab_names = [tab_name for tab_name in SHEET_TAB_NAMES if tab_names is None or tab_name in set(tab_names)]
    for tab_name in selected_tab_names:
        frame = tabs.get(tab_name, pd.DataFrame())
        safe_frame = frame.fillna("").apply(lambda column: column.map(_truncate_sheet_cell)) if not frame.empty else frame
        values = [safe_frame.columns.tolist()] + safe_frame.values.tolist()
        target_rows = max(len(values), 1)
        target_cols = max(len(safe_frame.columns), 1)
        try:
            worksheet = spreadsheet.worksheet(tab_name)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=tab_name, rows=target_rows, cols=target_cols)
        else:
            current_rows = int(getattr(worksheet, "row_count", 0) or 0)
            current_cols = int(getattr(worksheet, "col_count", 0) or 0)
            grow_rows = max(current_rows, target_rows)
            grow_cols = max(current_cols, target_cols)
            if grow_rows != current_rows or grow_cols != current_cols:
                _resize_worksheet_if_needed(worksheet, rows=grow_rows, cols=grow_cols)
        _update_worksheet_with_retry(worksheet, values)
        _resize_worksheet_if_needed(worksheet, rows=target_rows, cols=target_cols)
    return True
