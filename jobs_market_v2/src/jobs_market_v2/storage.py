"""Storage helpers for CSV, parquet, YAML, and JSONL."""

from __future__ import annotations

import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile

import pandas as pd
import yaml
from pandas.errors import ParserError

from .constants import ERROR_COLUMNS, RUN_COLUMNS
from .utils import normalize_whitespace


def read_csv_or_empty(path: Path, columns: tuple[str, ...] | list[str] | None = None) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=list(columns or []))
    try:
        frame = pd.read_csv(path)
    except ParserError:
        # Runtime bookkeeping CSVs can be partially written if an automation is interrupted.
        # Skip malformed lines so operational views and sheet sync can still proceed.
        frame = pd.read_csv(path, engine="python", on_bad_lines="skip")
    if not frame.empty and frame.shape[1] > 1:
        normalized = frame.fillna("").astype(str).apply(lambda column: column.map(normalize_whitespace))
        nonempty_counts = normalized.ne("").sum(axis=1)
        frame = frame.loc[nonempty_counts > 1].reset_index(drop=True)
    for boolean_column in ("is_active", "is_quarantined", "is_official_hint"):
        if boolean_column in frame.columns:
            frame[boolean_column] = frame[boolean_column].map(coerce_bool)
    for numeric_column in ("missing_count", "failure_count", "last_active_job_count", "official_domain_confidence", "source_quality_score"):
        if numeric_column in frame.columns:
            frame[numeric_column] = pd.to_numeric(frame[numeric_column], errors="coerce").fillna(0)
    if columns:
        for column in columns:
            if column not in frame.columns:
                frame[column] = None
        frame = frame[list(columns)]
    return frame


def _atomic_replace(path: Path, writer) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None
    directory_fd: int | None = None
    try:
        with NamedTemporaryFile(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp", delete=False) as handle:
            temp_path = Path(handle.name)
        writer(temp_path)
        with temp_path.open("rb") as handle:
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        os.fsync(directory_fd)
    finally:
        if directory_fd is not None:
            os.close(directory_fd)
        if temp_path is not None and temp_path.exists():
            try:
                temp_path.unlink()
            except FileNotFoundError:
                pass


def atomic_write_text(path: Path, text: str, *, encoding: str = "utf-8") -> None:
    _atomic_replace(path, lambda temp_path: temp_path.write_text(text, encoding=encoding))


def write_csv(frame: pd.DataFrame, path: Path) -> None:
    _atomic_replace(path, lambda temp_path: frame.to_csv(temp_path, index=False))


def write_parquet(frame: pd.DataFrame, path: Path) -> None:
    _atomic_replace(path, lambda temp_path: frame.to_parquet(temp_path, index=False))


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    records: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            records.append(json.loads(line))
    return records


def write_jsonl(records: list[dict], path: Path) -> None:
    atomic_write_text(
        path,
        "\n".join(json.dumps(record, ensure_ascii=False) for record in records),
    )


def load_tabular_input(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".yaml", ".yml"}:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or []
        if isinstance(raw, dict):
            for key in ("rows", "items", "sources", "companies"):
                if key in raw and isinstance(raw[key], list):
                    raw = raw[key]
                    break
            else:
                raw = [raw]
        return pd.DataFrame(raw)
    if suffix == ".xlsx":
        return pd.read_excel(path)
    raise ValueError(f"지원하지 않는 입력 형식입니다: {path}")


def append_deduplicated(existing: pd.DataFrame, incoming: pd.DataFrame, subset: list[str]) -> pd.DataFrame:
    if existing.empty:
        combined = incoming.copy()
    elif incoming.empty:
        combined = existing.copy()
    else:
        combined = pd.concat([existing, incoming], ignore_index=True)
    if subset:
        combined = combined.drop_duplicates(subset=subset, keep="last")
    return combined.reset_index(drop=True)


def coerce_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    text = normalize_whitespace(str(value)).lower()
    return text in {"1", "true", "yes", "y", "예", "참"}


def append_run_record(path: Path, row: dict) -> None:
    frame = read_csv_or_empty(path, RUN_COLUMNS)
    updated = append_deduplicated(frame, pd.DataFrame([row]), ["run_id"])
    write_csv(updated, path)


def append_error_record(path: Path, row: dict) -> None:
    frame = read_csv_or_empty(path, ERROR_COLUMNS)
    updated = pd.concat([frame, pd.DataFrame([row])], ignore_index=True)
    write_csv(updated, path)
