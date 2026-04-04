from __future__ import annotations

import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup


LIST_URL = (
    "https://www.seoulaihub.kr/partner/partner.asp"
    "?scrID=0000000195&pageNum=2&subNum=1&ssubNum=1&page=1"
)
DETAIL_URL = "https://www.seoulaihub.kr/partner/partner_detail.asp"
BAD_HOSTS = {
    "seoulaihub.kr",
    "facebook.com",
    "instagram.com",
    "linkedin.com",
    "youtube.com",
    "naver.com",
}


def normalize_host(href: str) -> str:
    href = href.strip()
    if not href:
        return ""
    if href.startswith("//"):
        href = f"https:{href}"
    if not href.startswith("http"):
        href = f"https://{href.lstrip('/')}"
    host = urlparse(href).netloc.lower().replace("www.", "").strip()
    if not host or any(bad in host for bad in BAD_HOSTS):
        return ""
    return host


def scrape_company_items(session: requests.Session) -> list[dict[str, str]]:
    html = session.get(LIST_URL, timeout=30).text
    soup = BeautifulSoup(html, "html.parser")
    items: list[dict[str, str]] = []
    for ul in soup.select("ul.partner_list"):
        for li in ul.find_all("li", recursive=False):
            heading = li.select_one("h3")
            if heading is None:
                continue
            onclick = li.get("onclick", "")
            if "goDetail('" not in onclick:
                continue
            bd_num = onclick.split("goDetail('", 1)[1].split("'", 1)[0]
            tags = [span.get_text(" ", strip=True) for span in li.select("ul.tag li span")]
            items.append(
                {
                    "company_name": heading.get_text(" ", strip=True),
                    "tags": " | ".join(tags),
                    "bd_num": bd_num,
                }
            )
    return items


def scrape_homepage_host(bd_num: str) -> str:
    detail = requests.post(
        DETAIL_URL,
        data={
            "scrID": "0000000195",
            "pageNum": "2",
            "subNum": "1",
            "ssubNum": "1",
            "act": "view",
            "bd_num": bd_num,
        },
        timeout=30,
    ).text
    soup = BeautifulSoup(detail, "html.parser")
    for row in soup.select("div.com03_detail_txt ul li"):
        label = row.select_one("strong")
        if label is None or label.get_text(" ", strip=True) != "홈페이지":
            continue
        link = row.select_one("a[href]")
        if link is None:
            return ""
        return normalize_host(link.get("href", ""))
    return ""


def build_rows(items: list[dict[str, str]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []

    def build_row(item: dict[str, str]) -> dict[str, str] | None:
        try:
            host = scrape_homepage_host(item["bd_num"])
        except requests.RequestException:
            return None
        if not host:
            return None
        name = item["company_name"].strip()
        aliases: list[str] = []
        if name.startswith("[") and "]" in name:
            aliases.append(name)
            inner = name[1:].split("]", 1)[0].strip()
            tail = name.split("]", 1)[1].strip()
            name = tail or inner
        residency = "graduate" if "졸업기업" in item["tags"] else "resident"
        return {
            "company_name": name,
            "company_tier": "스타트업",
            "official_domain": host,
            "company_name_en": "",
            "region": "서울",
            "aliases": " | ".join(aliases),
            "discovery_method": "manual_seed_import",
            "candidate_seed_type": "SEOUL_AI_HUB_COMPANY_LIST",
            "candidate_seed_url": LIST_URL,
            "candidate_seed_title": "서울 AI 허브 입주·졸업기업 공식 리스트",
            "candidate_seed_reason": f"서울 AI 허브 공식 {residency} roster 기반 기업 모집단 확장",
        }

    completed = 0
    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = [executor.submit(build_row, item) for item in items]
        for future in as_completed(futures):
            row = future.result()
            completed += 1
            if row is not None:
                rows.append(row)
            if completed % 50 == 0:
                print(f"detail fetched: {completed}", flush=True)
    deduped: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        key = (row["official_domain"], row["company_name"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def filter_new_rows(
    rows: list[dict[str, str]], companies_registry_path: Path
) -> list[dict[str, str]]:
    registry = pd.read_csv(companies_registry_path)
    existing_domains = {
        str(value).lower().strip().replace("www.", "")
        for value in registry.get("official_domain", [])
        if isinstance(value, str) and value.strip()
    }
    existing_names = {
        str(value).strip()
        for value in registry.get("company_name", [])
        if isinstance(value, str) and value.strip()
    }
    return [
        row
        for row in rows
        if row["official_domain"] not in existing_domains and row["company_name"] not in existing_names
    ]


def write_csv(rows: list[dict[str, str]], output_path: Path) -> None:
    fieldnames = [
        "company_name",
        "company_tier",
        "official_domain",
        "company_name_en",
        "region",
        "aliases",
        "discovery_method",
        "candidate_seed_type",
        "candidate_seed_url",
        "candidate_seed_title",
        "candidate_seed_reason",
    ]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    output_path = root / "config" / "seoul_ai_hub_company_batch_20260403.csv"
    companies_registry_path = root / "runtime" / "companies_registry.csv"
    session = requests.Session()
    items = scrape_company_items(session)
    print(f"parsed items: {len(items)}", flush=True)
    rows = build_rows(items)
    print(f"with domains: {len(rows)}", flush=True)
    new_rows = filter_new_rows(rows, companies_registry_path)
    print(f"new after registry filter: {len(new_rows)}", flush=True)
    write_csv(new_rows, output_path)
    print(f"wrote: {output_path}", flush=True)
    print("sample:", new_rows[:12], flush=True)


if __name__ == "__main__":
    main()
