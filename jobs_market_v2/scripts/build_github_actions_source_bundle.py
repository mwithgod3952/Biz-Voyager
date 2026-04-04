#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import tarfile
from pathlib import Path


def load_manifest(manifest_path: Path) -> list[Path]:
    entries: list[Path] = []
    for raw_line in manifest_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        entries.append(Path(line))
    return entries


def build_bundle(project_root: Path, manifest_path: Path, tar_path: Path, base64_path: Path) -> dict[str, object]:
    archived_files: list[str] = []
    tar_path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(tar_path, "w:gz") as archive:
        for relative_path in load_manifest(manifest_path):
            source_path = (project_root / relative_path).resolve()
            if not source_path.exists() or not source_path.is_file():
                raise FileNotFoundError(f"Missing bundle source file: {relative_path.as_posix()}")
            archive.add(source_path, arcname=relative_path.as_posix())
            archived_files.append(relative_path.as_posix())

    encoded = base64.b64encode(tar_path.read_bytes()).decode("ascii")
    wrapped = "\n".join(encoded[i : i + 76] for i in range(0, len(encoded), 76)) + "\n"
    base64_path.write_text(wrapped, encoding="utf-8")
    return {
        "archived_files": archived_files,
        "tar_path": str(tar_path),
        "base64_path": str(base64_path),
        "encoded_size": len(encoded),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", required=True)
    parser.add_argument("--manifest-path", required=True)
    parser.add_argument("--tar-path", required=True)
    parser.add_argument("--base64-path", required=True)
    args = parser.parse_args()

    summary = build_bundle(
        project_root=Path(args.project_root).resolve(),
        manifest_path=Path(args.manifest_path).resolve(),
        tar_path=Path(args.tar_path).resolve(),
        base64_path=Path(args.base64_path).resolve(),
    )
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
