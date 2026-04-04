"""Environment and project doctor checks."""

from __future__ import annotations

import importlib

from .models import DoctorSummary


def run_doctor(paths) -> DoctorSummary:
    required_files = [
        paths.root / "README.md",
        paths.root / ".env.example",
        paths.notebooks_dir / "00_source_screening.ipynb",
        paths.notebooks_dir / "01_bootstrap_population.ipynb",
        paths.scripts_dir / "setup_env.sh",
        paths.scripts_dir / "setup_env.ps1",
        paths.scripts_dir / "register_kernel.sh",
        paths.scripts_dir / "run_jupyter.sh",
        paths.config_dir / "seed_company_inputs.yaml",
        paths.company_seed_records_path,
        paths.company_seed_sources_path,
        paths.config_dir / "manual_companies_seed.csv",
        paths.config_dir / "manual_sources_seed.yaml",
        paths.mock_source_registry_path,
    ]
    required_modules = [
        "jupyterlab",
        "ipykernel",
        "pandas",
        "pyarrow",
        "yaml",
        "httpx",
        "bs4",
        "lxml",
        "dotenv",
        "pytest",
        "pydantic",
        "tenacity",
        "tqdm",
        "openpyxl",
    ]

    checks: list[dict[str, object]] = []
    for file_path in required_files:
        checks.append({"name": str(file_path.relative_to(paths.root)), "passed": file_path.exists()})

    for module_name in required_modules:
        try:
            importlib.import_module(module_name)
            passed = True
        except Exception:  # noqa: BLE001
            passed = False
        checks.append({"name": f"import:{module_name}", "passed": passed})

    paths.ensure_directories()
    sentinel = paths.logs_dir / ".doctor_write_check"
    sentinel.write_text("ok", encoding="utf-8")
    checks.append({"name": "runtime_write_access", "passed": sentinel.exists()})
    sentinel.unlink(missing_ok=True)

    return DoctorSummary(passed=all(check["passed"] for check in checks), checks=checks)
