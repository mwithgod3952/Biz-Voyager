#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from jobs_market_v2.runtime_state import (
    build_runtime_state_bundle,
    restore_runtime_state_bundle,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    create_parser = subparsers.add_parser("create")
    create_parser.add_argument("--project-root", required=True)
    create_parser.add_argument("--bundle-path", required=True)
    create_parser.add_argument("--manifest-path", required=True)

    extract_parser = subparsers.add_parser("extract")
    extract_parser.add_argument("--project-root", required=True)
    extract_parser.add_argument("--bundle-path", required=True)

    args = parser.parse_args()
    project_root = Path(args.project_root).resolve()
    bundle_path = Path(args.bundle_path).resolve()

    if args.command == "create":
        manifest_path = Path(args.manifest_path).resolve()
        archived = build_runtime_state_bundle(project_root, bundle_path, manifest_path)
        print(json.dumps({"archived_files": archived}, ensure_ascii=False))
        return 0

    restored = restore_runtime_state_bundle(project_root, bundle_path)
    print(json.dumps({"restored_files": restored}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
