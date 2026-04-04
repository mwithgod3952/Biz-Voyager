from __future__ import annotations

import tarfile
from pathlib import Path


def load_runtime_state_manifest(manifest_path: Path) -> list[Path]:
    entries: list[Path] = []
    for raw_line in manifest_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        entries.append(Path(line))
    return entries


def build_runtime_state_bundle(
    project_root: Path,
    bundle_path: Path,
    manifest_path: Path,
) -> list[str]:
    project_root = project_root.resolve()
    bundle_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_entries = load_runtime_state_manifest(manifest_path)
    archived: list[str] = []
    with tarfile.open(bundle_path, "w:gz") as archive:
        for relative_path in manifest_entries:
            source_path = (project_root / relative_path).resolve()
            if not source_path.exists() or not source_path.is_file():
                continue
            archive.add(source_path, arcname=relative_path.as_posix())
            archived.append(relative_path.as_posix())
    return archived


def restore_runtime_state_bundle(project_root: Path, bundle_path: Path) -> list[str]:
    project_root = project_root.resolve()
    restored: list[str] = []
    with tarfile.open(bundle_path, "r:gz") as archive:
        for member in archive.getmembers():
            member_path = Path(member.name)
            if member_path.is_absolute() or ".." in member_path.parts:
                raise ValueError(f"Unsafe archive member: {member.name}")
        archive.extractall(project_root)
        restored = [member.name for member in archive.getmembers() if member.isfile()]
    return restored
