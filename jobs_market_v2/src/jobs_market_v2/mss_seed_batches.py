from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import re

import pandas as pd
import requests
from pypdf import PdfReader


BABY_UNICORN_2023_PDF_URL = (
    "https://www.mss.go.kr/common/board/Download.do"
    "?bcIdx=1042145&cbIdx=197&streFileNm=a44ba7a3-17ac-4097-a781-6a859204f63c.pdf"
)

DIPS_1000_2023_PDF_URL = (
    "https://mss.go.kr/common/board/Download.do"
    "?bcIdx=1041656&cbIdx=160&streFileNm=e8d11f1d-167a-4f67-b5e7-bc21e5a06273.pdf"
)

BABY_UNICORN_2023_NAMES_RAW = [
    "네오켄바이오",
    "농업회사법인 푸디웜",
    "뉴클릭스바이오",
    "다리소프트",
    "더트라이브",
    "디버",
    "디오리진",
    "디오비스튜디오",
    "리브애니웨어",
    "리서리스테라퓨틱스",
    "리콘랩스",
    "마이크로트",
    "마인즈에이아이",
    "메디사피엔스",
    "미림진",
    "바스젠바이오",
    "벌스워크",
    "브이에스팜텍",
    "비주얼허",
    "셀라퓨틱스바이오",
    "쉐어잇",
    "슈파스",
    "스누아이랩",
    "씨에스아이엠",
    "아이네블루메",
    "아토플렉스",
    "알고리즘랩스",
    "알세미",
    "에디스바이오텍",
    "에스티씨랩",
    "에이조스바이오",
    "엔닷라이트",
    "엠엑스티 바이오텍",
    "옵토전자",
    "와이즈에이아이",
    "유씨아이테라퓨틱스",
    "인게니움테라퓨틱스",
    "인이지",
    "인터엑스",
    "인포마이닝",
    "일레븐코퍼레이션",
    "조인앤조인",
    "주식회사 와따",
    "지이모션",
    "캠프파이어애니웍스",
    "코가로보틱스",
    "콜로세움코퍼레이션",
    "콥틱",
    "트리오어",
    "티큐브잇",
    "플라스탈",
]


@dataclass(frozen=True)
class SectionSpec:
    title: str
    count: int


DIPS_2023_SECTION_SPECS = (
    SectionSpec("시스템반도체(25개사)", 25),
    SectionSpec("바이오·헬스(45개사)", 45),
    SectionSpec("미래 모빌리티(30개사)", 30),
    SectionSpec("친환경·에너지(25개사)", 25),
    SectionSpec("로봇(25개사)", 25),
    SectionSpec("시스템반도체(5개사)", 5),
    SectionSpec("바이오·헬스(13개사)", 13),
    SectionSpec("미래 모빌리티(7개사)", 7),
)


def _normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def normalize_company_name(name: str) -> str:
    text = _normalize_whitespace(name)
    text = re.sub(r"\s*\([^)]*Inc\.\)\s*", " ", text)
    text = re.sub(r"^\(?주식회사\)?\s*", "", text)
    text = re.sub(r"^\(주\)\s*", "", text)
    text = re.sub(r"\s*\(주\)$", "", text)
    text = re.sub(r"\s+주식회사$", "", text)
    text = re.sub(r"^주식회사\s+", "", text)
    text = re.sub(r"^농업회사법인\s+", "", text)
    return _normalize_whitespace(text)


def _alias_payload(raw_name: str, normalized_name: str) -> str:
    aliases = []
    raw_name = _normalize_whitespace(raw_name)
    if raw_name and raw_name != normalized_name:
        aliases.append(raw_name)
    return json.dumps(aliases, ensure_ascii=False)


def _pdf_text(pdf_path: Path) -> str:
    reader = PdfReader(str(pdf_path))
    return "\n".join((page.extract_text() or "") for page in reader.pages)


def _download_pdf(url: str, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    output_path.write_bytes(response.content)
    return output_path


def _parse_numbered_entries(segment_text: str, expected_count: int) -> list[str]:
    segment_text = re.sub(r"-\s*\d+\s*-", " ", segment_text)
    segment_text = segment_text.replace("분야연번기업명", " ")
    segment_text = _normalize_whitespace(segment_text)
    segment_text = re.sub(r"(?<!\d)(\d{1,2})(?=\s*(?:\(|[가-힣A-Za-z]))", r"\n\1 ", segment_text)
    entries = []
    for line in segment_text.splitlines():
        line = _normalize_whitespace(line)
        if not line:
            continue
        line = re.sub(r"^\d+\s*", "", line)
        line = _normalize_whitespace(line)
        if line:
            entries.append(line)
    if len(entries) != expected_count:
        raise ValueError(f"Expected {expected_count} names but found {len(entries)}")
    return entries


def parse_dips_2023_names_from_text(text: str) -> list[str]:
    if " 신규지원 150개사" in text and " 후속지원 25개사" in text:
        def between(start_marker: str, end_marker: str, *, search_start: int = 0) -> tuple[str, int]:
            start_idx = text.find(start_marker, search_start)
            if start_idx < 0:
                raise ValueError(f"Could not find start marker: {start_marker}")
            body_start = start_idx + len(start_marker)
            end_idx = text.find(end_marker, body_start)
            if end_idx < 0:
                raise ValueError(f"Could not find end marker: {end_marker}")
            return text[body_start:end_idx], end_idx

        names: list[str] = []
        system_segment, cursor = between("시스템반도체(25개사)", "바이오·헬스(45개사)")
        names.extend(_parse_numbered_entries(system_segment, 25))

        bio_part1, cursor = between("바이오·헬스(45개사)", "- 10 -", search_start=cursor - len("바이오·헬스(45개사)"))
        names.extend(_parse_numbered_entries(bio_part1, 9))
        bio_part2, cursor = between("바이오·헬스", "미래 모빌리티1", search_start=cursor)
        names.extend(_parse_numbered_entries(bio_part2, 36))

        mobility_part1, cursor = between("미래 모빌리티", "- 11 -", search_start=cursor)
        names.extend(_parse_numbered_entries(mobility_part1, 3))
        mobility_part2, cursor = between("미래 모빌리티(30개사)", "친환경·에너지(25개사)", search_start=cursor)
        names.extend(_parse_numbered_entries(mobility_part2, 27))

        eco_part1, cursor = between("친환경·에너지(25개사)", "- 12 -", search_start=cursor)
        names.extend(_parse_numbered_entries(eco_part1, 12))
        eco_part2, cursor = between("친환경·에너지", "로봇(25개사)", search_start=cursor)
        names.extend(_parse_numbered_entries(eco_part2, 13))

        robot_segment, cursor = between("로봇(25개사)", " 후속지원 25개사", search_start=cursor)
        names.extend(_parse_numbered_entries(robot_segment, 25))

        followup_system, cursor = between("시스템반도체(5개사)", "바이오·헬스(13개사)", search_start=cursor)
        names.extend(_parse_numbered_entries(followup_system, 5))
        followup_bio, cursor = between("바이오·헬스(13개사)", "미래 모빌리티(7개사)", search_start=cursor)
        names.extend(_parse_numbered_entries(followup_bio, 13))
        followup_mobility = text[text.find("미래 모빌리티(7개사)", cursor) + len("미래 모빌리티(7개사)") :]
        names.extend(_parse_numbered_entries(followup_mobility, 7))
        return names

    section_positions: list[tuple[SectionSpec, int]] = []
    for spec in DIPS_2023_SECTION_SPECS:
        idx = text.find(spec.title)
        if idx < 0:
            raise ValueError(f"Could not find section title: {spec.title}")
        section_positions.append((spec, idx))
    section_positions.sort(key=lambda item: item[1])

    names: list[str] = []
    for index, (spec, start_idx) in enumerate(section_positions):
        end_idx = len(text)
        if index + 1 < len(section_positions):
            next_spec, next_start_idx = section_positions[index + 1]
            next_base_title = re.sub(r"\([^)]*\)", "", next_spec.title).strip()
            next_base_idx = text.find(next_base_title, start_idx + len(spec.title))
            candidates = [candidate for candidate in (next_start_idx, next_base_idx) if candidate >= 0]
            if candidates:
                end_idx = min(candidates)
        section_body = text[start_idx + len(spec.title) : end_idx]
        section_body = re.sub(r"-\s*\d+\s*-", " ", section_body)
        section_body = section_body.replace("분야연번기업명", " ")
        current_base_title = re.sub(r"\([^)]*\)", "", spec.title).strip()
        if current_base_title:
            section_body = section_body.replace(current_base_title, " ")
        names.extend(_parse_numbered_entries(section_body, spec.count))
    return names


def build_baby_unicorn_2023_rows() -> pd.DataFrame:
    rows = []
    for raw_name in BABY_UNICORN_2023_NAMES_RAW:
        normalized = normalize_company_name(raw_name)
        rows.append(
            {
                "company_name": normalized,
                "company_tier": "스타트업",
                "official_domain": "",
                "company_name_en": "",
                "region": "대한민국",
                "aliases": _alias_payload(raw_name, normalized),
                "discovery_method": "manual_seed_import",
                "candidate_seed_type": "MSS_BABY_UNICORN_2023",
                "candidate_seed_url": BABY_UNICORN_2023_PDF_URL,
                "candidate_seed_title": "MSS 2023 Baby Unicorn official selected companies",
                "candidate_seed_reason": "중소벤처기업부 2023 아기유니콘 선정기업 공식 보도자료 기반 bounded 확장",
            }
        )
    return pd.DataFrame(rows)


def build_dips_2023_rows(pdf_path: Path) -> pd.DataFrame:
    rows = []
    for raw_name in parse_dips_2023_names_from_text(_pdf_text(pdf_path)):
        normalized = normalize_company_name(raw_name)
        rows.append(
            {
                "company_name": normalized,
                "company_tier": "스타트업",
                "official_domain": "",
                "company_name_en": "",
                "region": "대한민국",
                "aliases": _alias_payload(raw_name, normalized),
                "discovery_method": "manual_seed_import",
                "candidate_seed_type": "MSS_DIPS_1000_2023",
                "candidate_seed_url": DIPS_1000_2023_PDF_URL,
                "candidate_seed_title": "MSS 2023 DIPS 1000+ official selected companies",
                "candidate_seed_reason": "중소벤처기업부 초격차 스타트업 1000+ 프로젝트 선정기업 공식 보도자료 기반 bounded 확장",
            }
        )
    return pd.DataFrame(rows)


def write_mss_company_batches(project_root: Path) -> dict[str, Path]:
    runtime_dir = project_root / "runtime" / "tmp_mss_lists"
    baby_pdf_path = _download_pdf(BABY_UNICORN_2023_PDF_URL, runtime_dir / "baby_unicorn_2023.pdf")
    dips_pdf_path = _download_pdf(DIPS_1000_2023_PDF_URL, runtime_dir / "dips1000_2023_selected_150.pdf")

    baby_frame = build_baby_unicorn_2023_rows()
    dips_frame = build_dips_2023_rows(dips_pdf_path)

    baby_output = project_root / "config" / "mss_baby_unicorn_2023_batch_20260409.csv"
    dips_output = project_root / "config" / "mss_dips_1000_2023_batch_20260409.csv"

    baby_frame.to_csv(baby_output, index=False)
    dips_frame.to_csv(dips_output, index=False)

    return {
        "baby_pdf": baby_pdf_path,
        "dips_pdf": dips_pdf_path,
        "baby_csv": baby_output,
        "dips_csv": dips_output,
    }
