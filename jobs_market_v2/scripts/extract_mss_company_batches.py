from __future__ import annotations

from pathlib import Path

from jobs_market_v2.mss_seed_batches import write_mss_company_batches


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    outputs = write_mss_company_batches(project_root)
    for key, value in outputs.items():
        print(f"{key}={value}")


if __name__ == "__main__":
    main()
