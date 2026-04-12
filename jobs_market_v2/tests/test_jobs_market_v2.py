from __future__ import annotations

import base64
from datetime import datetime, timedelta, timezone
from io import BytesIO
import json
import shutil
import sys
import types
from pathlib import Path

import httpx
import pandas as pd
import pytest

import jobs_market_v2.cli as cli_module
import jobs_market_v2.collection as collection_module
import jobs_market_v2.company_screening as company_screening_module
import jobs_market_v2.company_seed_sources as company_seed_module
import jobs_market_v2.discovery as discovery_module
import jobs_market_v2.gemini as gemini_module
import jobs_market_v2.github_actions_runtime as github_actions_runtime_module
import jobs_market_v2.pipelines as pipelines_module
import jobs_market_v2.quality as quality_module
import jobs_market_v2.runtime_state as runtime_state_module
from jobs_market_v2.collection import (
    _build_greetinghr_jobs_from_html,
    _build_recruiter_jobs_from_payload,
    _source_timeout_values,
    classify_job_role,
    merge_incremental,
    normalize_job_payload,
    normalize_experience_level,
    parse_jobs_from_payload,
    refresh_job_roles,
)
from jobs_market_v2.constants import (
    COMPANY_CANDIDATE_COLUMNS,
    COMPANY_EVIDENCE_COLUMNS,
    COMPANY_SEED_SOURCE_COLUMNS,
    IMPORT_COMPANY_COLUMNS,
    JOB_COLUMNS,
    SOURCE_REGISTRY_COLUMNS,
)
from jobs_market_v2.company_seed_sources import (
    auto_promote_shadow_company_seed_sources,
    compact_company_seed_source_caches,
    collect_company_seed_records,
    discover_company_seed_sources,
    load_company_seed_sources,
    load_invalid_company_seed_sources,
    load_shadow_company_seed_sources,
    promote_shadow_company_seed_sources,
    _seed_source_dedupe_key,
)
from jobs_market_v2.discovery import (
    clean_non_company_entities,
    discover_companies,
    discover_source_candidates,
    generate_company_candidates,
    import_companies,
    import_sources,
    resolve_official_domains,
)
from jobs_market_v2.gemini import needs_gemini_refinement
from jobs_market_v2.html_utils import clean_html_text, extract_sections_from_description
from jobs_market_v2.models import GateResult
from jobs_market_v2.network import build_timeout
from jobs_market_v2.pipelines import (
    collect_company_seed_records_pipeline,
    collect_jobs_pipeline,
    collect_company_evidence_pipeline,
    discover_company_seed_sources_pipeline,
    discover_companies_pipeline,
    discover_sources_pipeline,
    promote_staging_pipeline,
    run_daily_tracking_pipeline,
    run_collection_cycle_pipeline,
    run_weekly_expansion_pipeline,
    screen_companies_pipeline,
    expand_company_candidates_pipeline,
    update_incremental_pipeline,
)
from jobs_market_v2.presentation import (
    build_analysis_fields,
    build_display_fields,
    count_english_leaks,
    sanitize_core_skill_text,
    sanitize_section_text,
    section_output_is_substantive,
    section_output_looks_noisy,
)
from jobs_market_v2.quality import evaluate_quality_gate, filter_low_quality_jobs, normalize_job_analysis_fields
from jobs_market_v2.screening import screen_sources
from jobs_market_v2.settings import AppSettings, ProjectPaths
from jobs_market_v2.sheets import build_sheet_tabs, export_tabs_locally, sync_tabs_to_google_sheets
from jobs_market_v2.storage import read_csv_or_empty
from jobs_market_v2.storage import write_csv
from jobs_market_v2.utils import canonicalize_runtime_source_url, contains_english


@pytest.fixture()
def sandbox_project(tmp_path: Path) -> Path:
    source_root = Path(__file__).resolve().parents[1]
    project_root = tmp_path / "jobs_market_v2"
    (project_root / "config").mkdir(parents=True, exist_ok=True)
    (project_root / "tests" / "fixtures").mkdir(parents=True, exist_ok=True)
    (project_root / "output_samples").mkdir(parents=True, exist_ok=True)
    (project_root / "runtime").mkdir(parents=True, exist_ok=True)
    shutil.copytree(source_root / "config", project_root / "config", dirs_exist_ok=True)
    shutil.copytree(source_root / "tests" / "fixtures", project_root / "tests" / "fixtures", dirs_exist_ok=True)
    (project_root / ".env").write_text(
        "JOBS_MARKET_V2_USE_MOCK_SOURCES=true\n"
        "JOBS_MARKET_V2_ENABLE_FALLBACK_SOURCE_GUESS=false\n"
        "JOBS_MARKET_V2_ENABLE_GEMINI_FALLBACK=false\n",
        encoding="utf-8",
    )
    (project_root / "config" / "manual_sources_seed.yaml").write_text(
        """
sources:
  - company_name: 삼성전자
    company_tier: 대기업
    source_name: 삼성전자 공식 채용
    source_url: https://careers.samsung.com/job-feed/data-ai.json
    source_type: json_api
    official_domain: samsungcareers.com
    is_official_hint: true
    structure_hint: json
    discovery_method: manual_seed
  - company_name: 네이버
    company_tier: 대기업
    source_name: 네이버 커리어
    source_url: https://recruit.navercorp.com/rcrt/list.do
    source_type: html_page
    official_domain: recruit.navercorp.com
    is_official_hint: true
    structure_hint: html
    discovery_method: manual_seed
  - company_name: 당근
    company_tier: 스타트업
    source_name: 당근 채용
    source_url: https://career.daangn.com/jobs/data-ai.html
    source_type: html_page
    official_domain: career.daangn.com
    is_official_hint: true
    structure_hint: html
    discovery_method: manual_seed
  - company_name: 티맥스소프트
    company_tier: 중견/중소
    source_name: 티맥스소프트 채용
    source_url: https://careers.tmaxsoft.com/jobs/data-ai.html
    source_type: html_page
    official_domain: careers.tmaxsoft.com
    is_official_hint: true
    structure_hint: html
    discovery_method: manual_seed
  - company_name: 한국전자통신연구원
    company_tier: 공공·연구기관
    source_name: 한국전자통신연구원 연구채용 RSS
    source_url: https://recruit.etri.re.kr/rss/research-jobs.xml
    source_type: rss
    official_domain: etri.re.kr
    is_official_hint: true
    structure_hint: rss
    discovery_method: manual_seed
  - company_name: 한국마이크로소프트
    company_tier: 외국계 한국법인
    source_name: 한국마이크로소프트 글로벌 채용
    source_url: https://careers.microsoft.com/v2/global/ko-kr/jobs-korea.json
    source_type: json_api
    official_domain: microsoft.com
    is_official_hint: true
    structure_hint: json
    discovery_method: manual_seed
  - company_name: 보쉬코리아
    company_tier: 외국계 한국법인
    source_name: 보쉬코리아 공개 공고 RSS
    source_url: https://jobs.bosch.co.kr/kr/rss/jobs.xml
    source_type: rss
    official_domain: bosch.co.kr
    is_official_hint: true
    structure_hint: rss
    discovery_method: manual_seed
  - company_name: 선보공업
    company_tier: 지역기업
    source_name: 선보공업 채용
    source_url: https://sunbo.careers/jobs/ai-engineer.html
    source_type: html_page
    official_domain: sunbo.careers
    is_official_hint: true
    structure_hint: html
    discovery_method: manual_seed
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (project_root / "config" / "company_seed_sources.yaml").write_text(
        """
sources:
  - source_name: KIND 샘플 목록
    source_type: csv_file
    source_url: https://kind.krx.co.kr/common/JLDDST35000.html
    local_path: tests/fixtures/company_seed_sources/kind_sample.csv
    source_title: KIND 상장법인목록 샘플
    company_tier: 중견/중소
    candidate_seed_type: 상장사목록
    candidate_seed_reason: KRX 상장기업 기준 후보
    company_name_column: 회사명
    company_name_en_column: 영문명
    official_domain_column: 홈페이지
    region_column: 지역
  - source_name: NST 샘플 목록
    source_type: html_table_file
    source_url: https://www.nst.re.kr/www/index.do
    local_path: tests/fixtures/company_seed_sources/nst_sample.html
    source_title: NST 소관 연구기관 샘플
    company_tier: 공공·연구기관
    candidate_seed_type: 출연연기관목록
    candidate_seed_reason: 연구기관 후보
    company_name_column: 기관명
    official_domain_column: 홈페이지
    region_column: 지역
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return project_root


def test_read_csv_or_empty_skips_malformed_lines(tmp_path: Path) -> None:
    broken = tmp_path / "broken.csv"
    broken.write_text(
        "run_id,command,status,started_at,finished_at,summary\n"
        'good-1,doctor,성공,2026-04-02T00:00:00+09:00,2026-04-02T00:00:01+09:00,"{""passed"": true}"\n'
        'malformed fragment line that should be skipped\n',
        encoding="utf-8",
    )

    frame = read_csv_or_empty(broken)

    assert len(frame) == 1
    assert frame.iloc[0]["run_id"] == "good-1"


def _clear_catalog_discovery_side_inputs(paths: ProjectPaths) -> None:
    empty_files = (
        (paths.company_seed_records_path, IMPORT_COMPANY_COLUMNS),
        (paths.company_candidates_path, COMPANY_CANDIDATE_COLUMNS),
        (paths.company_evidence_path, COMPANY_EVIDENCE_COLUMNS),
        (paths.discovered_company_seed_sources_path, COMPANY_SEED_SOURCE_COLUMNS),
        (paths.shadow_company_seed_sources_path, COMPANY_SEED_SOURCE_COLUMNS),
        (paths.invalid_company_seed_sources_path, COMPANY_SEED_SOURCE_COLUMNS),
    )
    for path, columns in empty_files:
        path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(columns=list(columns)).to_csv(path, index=False)


def test_build_timeout_caps_connect_timeout() -> None:
    timeout = build_timeout(20, 5)
    assert timeout.connect == 5
    assert timeout.read == 20
    assert timeout.write == 20
    assert timeout.pool == 20


def test_seed_source_dedupe_key_ignores_query_string() -> None:
    row_a = {
        "source_name": "NIPA 카탈로그 첨부",
        "source_title": "POOL 최종 목록.xlsx",
        "source_type": "xlsx_url",
        "source_url": "https://www.nipa.kr/comm/getFile?fileNo=aaa&token=1",
        "local_path": "",
    }
    row_b = {
        "source_name": "NIPA 카탈로그 첨부",
        "source_title": "POOL 최종 목록.xlsx",
        "source_type": "xlsx_url",
        "source_url": "https://www.nipa.kr/comm/getFile?fileNo=bbb&token=2",
        "local_path": "",
    }
    assert _seed_source_dedupe_key(row_a) == _seed_source_dedupe_key(row_b)


def test_company_candidate_generation(sandbox_project: Path) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    frame = generate_company_candidates(paths)
    assert set(frame["company_tier"]) >= {"대기업", "스타트업", "중견/중소", "공공·연구기관", "외국계 한국법인", "지역기업"}
    assert frame["candidate_seed_url"].fillna("").astype(str).str.strip().ne("").any()


def test_load_all_company_seed_records_prefers_career_domain_over_generic_domain(sandbox_project: Path) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    write_csv(
        pd.DataFrame(
            [
                {
                    "company_name": "삼성전자",
                    "company_tier": "대기업",
                    "official_domain": "samsungcareers.com",
                    "company_name_en": "",
                    "region": "",
                    "aliases": "삼전",
                    "discovery_method": "manual_seed",
                    "candidate_seed_type": "상장사목록",
                    "candidate_seed_url": "https://kind.example.com/list",
                    "candidate_seed_title": "KIND 목록",
                    "candidate_seed_reason": "커리어 도메인 확보",
                }
            ]
        ),
        paths.company_seed_records_path,
    )
    write_csv(
        pd.DataFrame(
            [
                {
                    "company_name": "삼성전자",
                    "company_tier": "대기업",
                    "official_domain": "samsung.com",
                    "company_name_en": "Samsung Electronics",
                    "region": "경기도",
                    "aliases": "Samsung",
                    "discovery_method": "catalog_collect",
                    "candidate_seed_type": "상장법인목록",
                    "candidate_seed_url": "https://kind.krx.co.kr/corpgeneral/corpList.do?method=loadInitPage",
                    "candidate_seed_title": "상장법인목록",
                    "candidate_seed_reason": "일반 홈페이지 수집",
                }
            ]
        ),
        paths.collected_company_seed_records_path,
    )

    combined = company_seed_module.load_all_company_seed_records(paths)

    assert len(combined) == 1
    row = combined.iloc[0]
    assert row["official_domain"] == "samsungcareers.com"
    assert row["company_name_en"] == "Samsung Electronics"
    assert row["region"] == "경기도"
    assert "삼전" in str(row["aliases"])
    assert "Samsung" in str(row["aliases"])


def test_collect_company_seed_records_supports_csv_and_html_sources(sandbox_project: Path) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    records, summary = collect_company_seed_records(paths, settings=None)
    assert summary["seed_source_count"] == 2
    assert summary["collected_company_count"] == 4
    assert {"알파데이터", "베타애널리틱스", "감마AI연구원", "델타로보틱스연구소"} <= set(records["company_name"])
    assert records["candidate_seed_url"].fillna("").astype(str).str.strip().ne("").all()


def test_collect_company_seed_records_supports_kind_alio_and_nst_sources(monkeypatch: pytest.MonkeyPatch, sandbox_project: Path) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    (paths.company_seed_sources_path).write_text(
        """
sources:
  - source_name: KIND 실전 샘플
    source_type: kind_corp_list
    source_url: https://kind.krx.co.kr/corpgeneral/corpList.do?method=loadInitPage
    request_url: https://kind.krx.co.kr/corpgeneral/corpList.do
    source_title: KIND 상장법인목록
    company_tier: 중견/중소
    candidate_seed_type: 상장법인목록
    candidate_seed_reason: 상장법인 중 디지털 후보
    filter_text_columns: industry;products
    include_keywords: 소프트웨어;데이터
  - source_name: ALIO 실전 샘플
    source_type: alio_public_agency_list
    source_url: https://www.alio.go.kr/guide/publicAgencyList.do
    request_url: https://www.alio.go.kr/organ/findOrganApbaList.json
    source_title: ALIO 공공기관 현황
    company_tier: 공공·연구기관
    candidate_seed_type: 공공기관목록
    candidate_seed_reason: 데이터/정보통신 기관 후보
    filter_text_columns: apbaNa
    include_keywords: 데이터;정보통신
  - source_name: NST 실전 샘플
    source_type: nst_research_institutes
    source_url: https://audit.nst.re.kr/reference_agency.jsp
    request_url: https://audit.nst.re.kr/reference_agency.jsp
    source_title: NST 소관 연구기관
    company_tier: 공공·연구기관
    candidate_seed_type: 출연연기관목록
    candidate_seed_reason: NST 소관 연구기관 후보
""".strip()
        + "\n",
        encoding="utf-8",
    )

    kind_html = """
    <section class="scrarea type-00">
      <table><tbody>
        <tr>
          <td class="first" title="알파소프트(주)">
            <img alt="유가증권" />
            <img alt="KOSPI200" />
            <a title="알파소프트">알파소프트</a>
          </td>
          <td title="소프트웨어 개발 및 공급업">소프트웨어 개발 및 공급업</td>
          <td title="데이터 분석 솔루션">데이터 분석 솔루션</td>
          <td>2020-01-01</td>
          <td>12월</td>
          <td>홍길동</td>
          <td><a href="https://alpha.example.com"><span>홈페이지 보기</span></a></td>
          <td>서울특별시</td>
        </tr>
        <tr>
          <td class="first" title="베타푸드(주)">
            <img alt="유가증권" />
            <a title="베타푸드">베타푸드</a>
          </td>
          <td title="식료품 제조업">식료품 제조업</td>
          <td title="식품 제조">식품 제조</td>
          <td>2020-01-01</td>
          <td>12월</td>
          <td>홍길동</td>
          <td><a href="https://beta.example.com"><span>홈페이지 보기</span></a></td>
          <td>서울특별시</td>
        </tr>
      </tbody></table>
    </section>
    """
    nst_html = """
    <div class="agency_list">
      <div class="link_item">
        <div class="link_inner">
          <div class="link_logo"><img alt="감마연구원" /></div>
          <div class="link_text">
            <p>AI 연구개발 수행</p>
            <a href="https://gamma.re.kr">www.gamma.re.kr</a>
          </div>
        </div>
      </div>
    </div>
    """
    alio_payload = {
        "status": "success",
        "data": {
            "organList": {
                "result": [
                    {
                        "apbaNa": "한국데이터산업진흥원",
                        "homepage": "https://www.kdata.or.kr",
                        "addrCd": "서울특별시",
                        "typeNa": "기타공공기관",
                        "jidtNa": "과학기술정보통신부",
                        "contents": "데이터 산업 진흥",
                    },
                    {
                        "apbaNa": "예술경영지원센터",
                        "homepage": "https://www.gokams.or.kr",
                        "addrCd": "서울특별시",
                        "typeNa": "기타공공기관",
                        "jidtNa": "문화체육관광부",
                        "contents": "예술 경영 지원",
                    },
                ],
                "page": {"totalPage": 1},
            }
        },
    }

    def fake_fetch_text(url: str, settings=None, **kwargs) -> str:
        if "kind.krx.co.kr" in url:
            return kind_html
        if "audit.nst.re.kr" in url:
            return nst_html
        raise AssertionError(f"unexpected text url: {url}")

    def fake_fetch_json(url: str, settings=None, **kwargs) -> dict:
        if "alio.go.kr" in url:
            return alio_payload
        raise AssertionError(f"unexpected json url: {url}")

    monkeypatch.setattr(company_seed_module, "_fetch_text", fake_fetch_text)
    monkeypatch.setattr(company_seed_module, "_fetch_json", fake_fetch_json)

    records, summary = collect_company_seed_records(paths, settings=None)
    assert summary["seed_source_count"] == 3
    assert summary["collected_company_count"] == 3
    assert {"알파소프트", "한국데이터산업진흥원", "감마연구원"} <= set(records["company_name"])
    alpha = records.loc[records["company_name"] == "알파소프트"].iloc[0]
    assert alpha["company_tier"] == "대기업"
    assert alpha["official_domain"] == "alpha.example.com"


def test_discover_company_seed_sources_catalog_collects_attachment_source_and_seed_records(
    monkeypatch: pytest.MonkeyPatch, sandbox_project: Path
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    _clear_catalog_discovery_side_inputs(paths)
    (paths.company_seed_sources_path).write_text(
        """
sources:
  - source_name: NIPA 카탈로그 샘플
    source_type: html_link_catalog
    discovered_source_type: xlsx_url
    source_url: https://www.nipa.kr/home/2-2/16546
    source_title: NIPA 사업공고 샘플
    company_tier: 중견/중소
    candidate_seed_type: 정부지원사업공급기업목록
    candidate_seed_reason: 공식 공고 첨부 공급기업 목록
    header_row: 2
    company_name_column: 기업명
    official_domain_column: 기업정보
    region_column: 기업정보
    filter_text_columns: 기업명;전문분야;AI솔루션;기업정보
    allowed_domains: nipa.kr
    discovery_include_keywords: 공급기업;POOL;최종 목록
    discovery_exclude_keywords: 매뉴얼;zip
    max_discovered_sources: 3
""".strip()
        + "\n",
        encoding="utf-8",
    )

    catalog_html = """
    <div class="attachments">
      <a href="/comm/getFile?fileNo=pool-final">2026년 AI바우처 지원사업 공급기업 POOL 최종 목록(1428개사).xlsx</a>
      <a href="/comm/getFile?fileNo=manual">접수 매뉴얼.zip</a>
    </div>
    """

    table = pd.DataFrame(
        [
            {
                "연번": 1,
                "기업명": "알파AI",
                "전문분야": "분석지능,시각지능",
                "AI솔루션": "수요예측 솔루션",
                "기업정보": "- 지역 : 서울특별시 - 홈페이지 : https://alpha.ai",
            },
            {
                "연번": 2,
                "기업명": "베타로보틱스",
                "전문분야": "행동지능",
                "AI솔루션": "로봇 제어 솔루션",
                "기업정보": "- 지역 : 경기도 - 홈페이지 : http://beta-robotics.kr",
            },
        ]
    )
    workbook = BytesIO()
    with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
        table.to_excel(writer, index=False, startrow=2)
    workbook_bytes = workbook.getvalue()

    def fake_fetch_text(url: str, settings=None, **kwargs) -> str:
        if "nipa.kr/home/2-2/16546" in url:
            return catalog_html
        raise AssertionError(f"unexpected text url: {url}")

    def fake_fetch_bytes(url: str, settings=None) -> bytes:
        if "fileNo=pool-final" in url:
            return workbook_bytes
        raise AssertionError(f"unexpected bytes url: {url}")

    monkeypatch.setattr(company_seed_module, "_fetch_text", fake_fetch_text)
    monkeypatch.setattr(company_seed_module, "_fetch_bytes", fake_fetch_bytes)

    discovery_summary = discover_company_seed_sources_pipeline(project_root=sandbox_project)
    discovered_sources = read_csv_or_empty(paths.shadow_company_seed_sources_path)

    assert discovery_summary["catalog_source_count"] >= 1
    assert discovery_summary["discovered_seed_source_count"] == 1
    assert discovery_summary["target"] == "shadow"
    assert discovered_sources.iloc[0]["source_type"] == "xlsx_url"
    assert "pool-final" in discovered_sources.iloc[0]["source_url"]
    assert "pool-final" in discovered_sources.iloc[0]["request_url"]
    assert read_csv_or_empty(paths.discovered_company_seed_sources_path).empty

    collect_summary = collect_company_seed_records_pipeline(project_root=sandbox_project)
    collected_records = read_csv_or_empty(paths.collected_company_seed_records_path)

    assert collect_summary["collected_company_count"] == 2
    assert collect_summary["auto_promoted_shadow_seed_source_count"] == 1
    assert collect_summary["shadow_seed_source_count"] == 0
    assert collect_summary["approved_seed_source_count"] == 1
    assert read_csv_or_empty(paths.discovered_company_seed_sources_path).shape[0] == 1
    assert load_shadow_company_seed_sources(paths).empty
    assert {"알파AI", "베타로보틱스"} <= set(collected_records["company_name"])
    alpha = collected_records.loc[collected_records["company_name"] == "알파AI"].iloc[0]
    beta = collected_records.loc[collected_records["company_name"] == "베타로보틱스"].iloc[0]
    assert alpha["official_domain"] == "alpha.ai"
    assert alpha["region"] == "서울특별시"
    assert beta["official_domain"] == "beta-robotics.kr"
    assert beta["region"] == "경기도"


def test_discover_company_seed_sources_from_local_catalog_and_collect_records(sandbox_project: Path) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    _clear_catalog_discovery_side_inputs(paths)
    catalog_dir = sandbox_project / "tests" / "fixtures" / "company_seed_sources"
    catalog_dir.mkdir(parents=True, exist_ok=True)
    (catalog_dir / "catalog_companies.csv").write_text(
        "기업명,홈페이지,지역\n에이아이랩,https://ailab.example.com,서울\n데이터파트너스,https://datapartners.example.com,경기\n",
        encoding="utf-8",
    )
    (catalog_dir / "catalog_institutes.html").write_text(
        """
        <table>
          <tr><th>기관명</th><th>홈페이지</th><th>지역</th></tr>
          <tr><td>공공데이터연구원</td><td>https://publicdata.example.or.kr</td><td>대전</td></tr>
        </table>
        """.strip(),
        encoding="utf-8",
    )
    (catalog_dir / "detail_suppliers.csv").write_text(
        "회사명,홈페이지,지역\n머신러닝웍스,https://mlworks.example.com,서울\n",
        encoding="utf-8",
    )
    (catalog_dir / "detail_notice.html").write_text(
        """
        <html><body>
          <a href="detail_suppliers.csv">공급기업 pool CSV</a>
        </body></html>
        """.strip(),
        encoding="utf-8",
    )
    (catalog_dir / "catalog.html").write_text(
        """
        <html><body>
          <a href="catalog_companies.csv">참여기업 목록 CSV</a>
          <a href="catalog_institutes.html">기관목록</a>
          <a href="detail_notice.html">공급기업 공고</a>
          <a href="guide.pdf">안내문</a>
          <a href="https://unofficial.example.com/company-list.csv">외부 비공식 목록</a>
          <a href="catalog_companies.csv">참여기업 목록 CSV</a>
        </body></html>
        """.strip(),
        encoding="utf-8",
    )
    paths.company_seed_sources_path.write_text(
        f"""
sources:
  - source_name: 공식 참여기관 카탈로그
    source_type: html_link_catalog_file
    source_url: https://official.example.com/catalog
    local_path: {catalog_dir.relative_to(sandbox_project) / 'catalog.html'}
    source_title: 공식 참여기관 카탈로그
    company_tier: 공공·연구기관
    candidate_seed_type: 참여기관목록
    candidate_seed_reason: 공식 카탈로그 기반 후보
    discovery_include_keywords: 참여기업;기관목록;공급기업
    discovery_exclude_keywords: 안내문;pdf
    max_discovered_sources: 5
""".strip()
        + "\n",
        encoding="utf-8",
    )

    discovered, discovery_summary = discover_company_seed_sources(paths, settings=None)
    assert discovery_summary["target"] == "shadow"
    main_sources = load_company_seed_sources(paths)
    assert len(main_sources) == 1
    assert set(main_sources["source_type"]) == {"html_link_catalog_file"}
    assert not load_shadow_company_seed_sources(paths).empty

    promoted, promote_summary = promote_shadow_company_seed_sources(paths)
    assert promote_summary["shadow_seed_source_count"] == len(discovered)
    assert not promoted.empty

    records, summary = collect_company_seed_records(paths, settings=None)

    assert discovery_summary["catalog_source_count"] >= 1
    assert discovery_summary["discovered_seed_source_count"] == 3
    assert set(discovered["source_type"]) == {"csv_file", "html_table_file"}
    assert "guide.pdf" not in discovered["local_path"].tolist()
    assert all("unofficial.example.com" not in value for value in discovered["source_url"].fillna("").tolist())
    assert summary["catalog_source_count"] >= 1
    assert summary["discovered_seed_source_count"] == 3
    assert summary["seed_source_count"] == 3
    assert {"에이아이랩", "데이터파트너스", "공공데이터연구원", "머신러닝웍스"} <= set(records["company_name"])


def test_collect_company_seed_records_auto_discovers_nested_catalog_sources(sandbox_project: Path) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    _clear_catalog_discovery_side_inputs(paths)
    catalog_dir = sandbox_project / "tests" / "fixtures" / "company_seed_sources"
    catalog_dir.mkdir(parents=True, exist_ok=True)
    (catalog_dir / "nested_companies.csv").write_text(
        "기업명,홈페이지,지역\n오토카탈로그랩,https://autocatalog.example.com,서울\n",
        encoding="utf-8",
    )
    (catalog_dir / "detail_catalog.html").write_text(
        """
        <html><body>
          <a href="nested_companies.csv">참여기업 목록 CSV</a>
        </body></html>
        """.strip(),
        encoding="utf-8",
    )
    (catalog_dir / "root_catalog.html").write_text(
        """
        <html><body>
          <a href="detail_catalog.html">공급기업 목록 상세</a>
        </body></html>
        """.strip(),
        encoding="utf-8",
    )
    paths.company_seed_sources_path.write_text(
        f"""
sources:
  - source_name: 중첩 카탈로그 루트
    source_type: html_link_catalog_file
    source_url: https://official.example.com/root
    local_path: {catalog_dir.relative_to(sandbox_project) / 'root_catalog.html'}
    source_title: 중첩 카탈로그 루트
    company_tier: 중견/중소
    candidate_seed_type: 참여기관목록
    candidate_seed_reason: 공식 카탈로그 기반 후보
    discovery_include_keywords: 공급기업;참여기업
    max_discovered_sources: 5
""".strip()
        + "\n",
        encoding="utf-8",
    )

    records, summary = collect_company_seed_records(paths, settings=None)
    approved_sources = load_company_seed_sources(paths)

    assert summary["newly_discovered_seed_source_count"] >= 2
    assert summary["auto_promoted_shadow_seed_source_count"] >= 1
    assert summary["shadow_seed_source_count"] == 0
    assert "오토카탈로그랩" in set(records["company_name"])
    assert "html_link_catalog_file" in set(approved_sources["source_type"])
    assert "csv_file" in set(approved_sources["source_type"])


def test_shadow_seed_sources_require_promotion_before_main_load(sandbox_project: Path) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    _clear_catalog_discovery_side_inputs(paths)
    catalog_dir = sandbox_project / "tests" / "fixtures" / "company_seed_sources"
    catalog_dir.mkdir(parents=True, exist_ok=True)
    (catalog_dir / "catalog_companies.csv").write_text(
        "기업명,홈페이지,지역\n에이아이랩,https://ailab.example.com,서울\n",
        encoding="utf-8",
    )
    (catalog_dir / "catalog.html").write_text(
        '<html><body><a href="catalog_companies.csv">참여기업 목록 CSV</a></body></html>',
        encoding="utf-8",
    )
    paths.company_seed_sources_path.write_text(
        f"""
sources:
  - source_name: 공식 참여기관 카탈로그
    source_type: html_link_catalog_file
    source_url: https://official.example.com/catalog
    local_path: {catalog_dir.relative_to(sandbox_project) / 'catalog.html'}
    source_title: 공식 참여기관 카탈로그
    company_tier: 중견/중소
    candidate_seed_type: 참여기관목록
    candidate_seed_reason: 공식 카탈로그 기반 후보
    discovery_include_keywords: 참여기업
    max_discovered_sources: 5
""".strip()
        + "\n",
        encoding="utf-8",
    )

    shadow, summary = discover_company_seed_sources(paths, settings=None)
    assert summary["target"] == "shadow"
    assert not shadow.empty
    main_sources = load_company_seed_sources(paths)
    assert len(main_sources) == 1
    assert set(main_sources["source_type"]) == {"html_link_catalog_file"}
    assert not load_shadow_company_seed_sources(paths).empty

    records, collect_summary = collect_company_seed_records(paths, settings=None)
    approved_sources = load_company_seed_sources(paths)
    assert collect_summary["auto_promoted_shadow_seed_source_count"] == 1
    assert collect_summary["shadow_seed_source_count"] == 0
    assert len(approved_sources) == 2
    assert load_shadow_company_seed_sources(paths).empty
    assert {"에이아이랩"} <= set(records["company_name"])


def test_collect_company_seed_records_skips_unparsable_discovered_sources(sandbox_project: Path) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    _clear_catalog_discovery_side_inputs(paths)
    catalog_dir = sandbox_project / "tests" / "fixtures" / "company_seed_sources"
    catalog_dir.mkdir(parents=True, exist_ok=True)
    (catalog_dir / "empty_notice.html").write_text("<html><body><p>표가 없는 공고</p></body></html>", encoding="utf-8")
    (catalog_dir / "catalog.html").write_text(
        """
        <html><body>
          <a href="empty_notice.html">참여기업 안내</a>
        </body></html>
        """.strip(),
        encoding="utf-8",
    )
    paths.company_seed_sources_path.write_text(
        f"""
sources:
  - source_name: 파싱불가 카탈로그
    source_type: html_link_catalog_file
    source_url: https://official.example.com/catalog
    local_path: {catalog_dir.relative_to(sandbox_project) / 'catalog.html'}
    source_title: 파싱불가 카탈로그
    company_tier: 중견/중소
    candidate_seed_type: 참여기관목록
    candidate_seed_reason: 공식 카탈로그 기반 후보
    discovery_include_keywords: 참여기업
    discovery_exclude_keywords: pdf
    max_discovered_sources: 3
""".strip()
        + "\n",
        encoding="utf-8",
    )

    records, summary = collect_company_seed_records(paths, settings=None)

    assert records.empty
    assert summary["discovered_seed_source_count"] == 0
    assert summary["newly_discovered_seed_source_count"] == 0
    assert summary["auto_promoted_shadow_seed_source_count"] == 0
    assert summary["invalid_shadow_seed_source_count"] == 0
    assert summary["shadow_seed_source_count"] == 0
    assert summary["skipped_seed_source_count"] == 0
    assert read_csv_or_empty(paths.discovered_company_seed_sources_path).empty
    assert load_shadow_company_seed_sources(paths).empty
    assert load_invalid_company_seed_sources(paths).empty
    assert records["candidate_seed_url"].fillna("").astype(str).str.strip().eq("https://official.example.com/catalog").all()


def test_invalid_shadow_seed_sources_are_cached_and_not_rediscovered(sandbox_project: Path) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    _clear_catalog_discovery_side_inputs(paths)
    catalog_dir = sandbox_project / "tests" / "fixtures" / "company_seed_sources"
    catalog_dir.mkdir(parents=True, exist_ok=True)
    (catalog_dir / "empty_notice.html").write_text("<html><body><p>표가 없는 공고</p></body></html>", encoding="utf-8")
    (catalog_dir / "catalog.html").write_text(
        """
        <html><body>
          <a href="empty_notice.html">참여기업 안내</a>
        </body></html>
        """.strip(),
        encoding="utf-8",
    )
    paths.company_seed_sources_path.write_text(
        f"""
sources:
  - source_name: 캐시 테스트 카탈로그
    source_type: html_link_catalog_file
    source_url: https://official.example.com/catalog
    local_path: {catalog_dir.relative_to(sandbox_project) / 'catalog.html'}
    source_title: 캐시 테스트 카탈로그
    company_tier: 중견/중소
    candidate_seed_type: 참여기관목록
    candidate_seed_reason: 공식 카탈로그 기반 후보
    discovery_include_keywords: 참여기업
    discovery_exclude_keywords: pdf
    max_discovered_sources: 3
""".strip()
        + "\n",
        encoding="utf-8",
    )

    _, first_summary = collect_company_seed_records(paths, settings=None)
    _, second_summary = collect_company_seed_records(paths, settings=None)

    assert first_summary["invalid_shadow_seed_source_count"] == 0
    assert second_summary["invalid_shadow_seed_source_count"] == 0
    assert second_summary["discovered_seed_source_count"] == 0
    assert load_invalid_company_seed_sources(paths).empty


def test_discover_company_seed_sources_finds_top_level_catalog_from_sitemap(
    monkeypatch: pytest.MonkeyPatch, sandbox_project: Path
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    _clear_catalog_discovery_side_inputs(paths)
    paths.company_seed_sources_path.write_text(
        """
sources:
  - source_name: 상위 공식 카탈로그
    source_type: html_link_catalog_url
    source_url: https://catalog.example.com/root
    source_title: 상위 공식 카탈로그
    company_tier: 중견/중소
    candidate_seed_type: 공식사업참여기업목록
    candidate_seed_reason: 공식 카탈로그 기반 후보
    discovery_include_keywords: 참여기업;participants
    discovery_exclude_keywords: notice
    max_discovered_sources: 5
""".strip()
        + "\n",
        encoding="utf-8",
    )

    def fake_fetch_text(url: str, settings=None, **kwargs) -> str:
        if url == "https://catalog.example.com/root":
            return "<html><body><a href='/notice'>공지</a></body></html>"
        if url == "https://catalog.example.com/sitemap.xml":
            return """
            <urlset>
              <url><loc>https://catalog.example.com/participants</loc></url>
              <url><loc>https://catalog.example.com/participants?download=1</loc></url>
              <url><loc>https://catalog.example.com/login.do?menu=participants</loc></url>
              <url><loc>https://catalog.example.com/notice</loc></url>
              <url><loc>https://external.example.com/participants</loc></url>
            </urlset>
            """.strip()
        if url == "https://catalog.example.com/sitemap_index.xml":
            return "<sitemapindex></sitemapindex>"
        return "<html></html>"

    monkeypatch.setattr(company_seed_module, "_fetch_text", fake_fetch_text)

    discovered, summary = discover_company_seed_sources(paths, settings=AppSettings())

    assert summary["target"] == "shadow"
    assert summary["newly_discovered_seed_source_count"] == 1
    assert len(discovered) == 1
    row = discovered.iloc[0].fillna("").to_dict()
    assert row["source_type"] == "html_link_catalog_url"
    assert row["source_url"] == "https://catalog.example.com/participants"
    assert "external.example.com" not in set(discovered["source_url"])
    assert "https://catalog.example.com/login.do?menu=participants" not in set(discovered["source_url"])


def test_collect_company_seed_records_promotes_sitemap_discovered_catalog_and_collects_companies(
    monkeypatch: pytest.MonkeyPatch, sandbox_project: Path
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    paths.company_seed_sources_path.write_text(
        """
sources:
  - source_name: 상위 공식 카탈로그
    source_type: html_link_catalog_url
    source_url: https://catalog.example.com/root
    source_title: 상위 공식 카탈로그
    company_tier: 중견/중소
    candidate_seed_type: 공식사업참여기업목록
    candidate_seed_reason: 공식 카탈로그 기반 후보
    discovery_include_keywords: 참여기업;participants
    max_discovered_sources: 5
""".strip()
        + "\n",
        encoding="utf-8",
    )

    def fake_fetch_text(url: str, settings=None, **kwargs) -> str:
        if url == "https://catalog.example.com/root":
            return "<html><body><p>공식 카탈로그 루트</p></body></html>"
        if url == "https://catalog.example.com/sitemap.xml":
            return """
            <urlset>
              <url><loc>https://catalog.example.com/participants</loc></url>
            </urlset>
            """.strip()
        if url == "https://catalog.example.com/sitemap_index.xml":
            return "<sitemapindex></sitemapindex>"
        if url == "https://catalog.example.com/participants":
            return '<html><body><a href="companies.csv">참여기업 목록 CSV</a></body></html>'
        if url == "https://catalog.example.com/companies.csv":
            return "기업명,홈페이지,지역\n알파AI,https://alpha.ai,서울\n"
        return "<html></html>"

    monkeypatch.setattr(company_seed_module, "_fetch_text", fake_fetch_text)

    records, summary = collect_company_seed_records(paths, settings=None)
    approved_sources = load_company_seed_sources(paths)

    assert {"알파AI"} <= set(records["company_name"])
    assert summary["auto_promoted_shadow_seed_source_count"] >= 2
    assert summary["shadow_seed_source_count"] == 0
    assert "html_link_catalog_url" in set(approved_sources["source_type"])
    assert "csv_url" in set(approved_sources["source_type"])
    assert "https://catalog.example.com/companies.csv" in set(approved_sources["source_url"])


def test_discover_company_seed_sources_uses_public_catalog_host_domains(
    monkeypatch: pytest.MonkeyPatch, sandbox_project: Path
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    _clear_catalog_discovery_side_inputs(paths)
    paths.company_seed_sources_path.write_text("sources: []\n", encoding="utf-8")
    pd.DataFrame(
        [
            {
                "company_name": "테스트산업진흥원",
                "company_tier": "공공·연구기관",
                "official_domain": "support.example.com",
            },
            {
                "company_name": "기초과학연구원",
                "company_tier": "공공·연구기관",
                "official_domain": "research.example.com",
            },
        ]
    ).to_csv(paths.companies_registry_path, index=False)

    fetched_urls: list[str] = []

    def fake_fetch_text(url: str, settings=None, **kwargs) -> str:
        fetched_urls.append(url)
        if url == "https://support.example.com":
            return "<html><body><p>공식 지원기관</p></body></html>"
        if url == "https://support.example.com/sitemap.xml":
            return """
            <urlset>
              <url><loc>https://support.example.com/participants</loc></url>
              <url><loc>https://external.example.com/participants</loc></url>
            </urlset>
            """.strip()
        if url == "https://support.example.com/sitemap_index.xml":
            return "<sitemapindex></sitemapindex>"
        return "<html></html>"

    monkeypatch.setattr(company_seed_module, "_fetch_text", fake_fetch_text)

    discovered, summary = discover_company_seed_sources(paths, settings=AppSettings())

    assert summary["catalog_host_seed_source_count"] >= 1
    assert summary["newly_discovered_seed_source_count"] == 1
    assert "https://support.example.com/participants" in set(discovered["source_url"])
    assert "https://support.example.com/sitemap.xml" in fetched_urls
    assert "https://research.example.com/sitemap.xml" not in fetched_urls


def test_discover_company_seed_sources_uses_trusted_evidence_domains_as_catalog_hosts(
    monkeypatch: pytest.MonkeyPatch, sandbox_project: Path
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    _clear_catalog_discovery_side_inputs(paths)
    paths.company_seed_sources_path.write_text("sources: []\n", encoding="utf-8")
    pd.DataFrame(
        [
            {
                "company_name": "한국여성과학기술인육성재단",
                "company_tier": "공공·연구기관",
                "company_bucket": "candidate",
            }
        ]
    ).to_csv(paths.company_candidates_path, index=False)
    pd.DataFrame(
        [
            {
                "company_name": "한국여성과학기술인육성재단",
                "evidence_type": "공식채용소스",
                "evidence_url": "https://wiset.or.kr/prog/bbsArticle/BBSMSTR_000000000307/view.do",
                "evidence_text": "한국여성과학기술인육성재단 공식 채용",
            }
        ]
    ).to_csv(paths.company_evidence_path, index=False)

    fetched_urls: list[str] = []

    def fake_fetch_text(url: str, settings=None, **kwargs) -> str:
        fetched_urls.append(url)
        if url == "https://wiset.or.kr":
            return "<html><body><p>재단 홈페이지</p></body></html>"
        if url == "https://wiset.or.kr/sitemap.xml":
            return """
            <urlset>
              <url><loc>https://wiset.or.kr/program/participants/list.do</loc></url>
            </urlset>
            """.strip()
        if url == "https://wiset.or.kr/sitemap_index.xml":
            return "<sitemapindex></sitemapindex>"
        return "<html></html>"

    monkeypatch.setattr(company_seed_module, "_fetch_text", fake_fetch_text)

    discovered, summary = discover_company_seed_sources(paths, settings=AppSettings())

    assert summary["catalog_host_seed_source_count"] >= 1
    assert summary["newly_discovered_seed_source_count"] == 1
    assert "https://wiset.or.kr/program/participants/list.do" in set(discovered["source_url"])
    assert "https://wiset.or.kr/sitemap.xml" in fetched_urls


def test_discover_company_seed_sources_uses_candidate_official_domain_for_bare_kr_host(
    monkeypatch: pytest.MonkeyPatch, sandbox_project: Path
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    paths.company_seed_sources_path.write_text("sources: []\n", encoding="utf-8")
    pd.DataFrame(
        [
            {
                "company_name": "한국방송통신전파진흥원",
                "company_tier": "공공·연구기관",
                "official_domain": "kca.kr",
                "primary_evidence_type": "공식채용소스",
                "primary_evidence_url": "https://www.kca.kr/boardList.do",
            }
        ]
    ).to_csv(paths.company_candidates_path, index=False)

    fetched_urls: list[str] = []

    def fake_fetch_text(url: str, settings=None, **kwargs) -> str:
        fetched_urls.append(url)
        if url == "https://kca.kr":
            return "<html><body><p>공식 기관 홈페이지</p></body></html>"
        if url == "https://kca.kr/sitemap.xml":
            return """
            <urlset>
              <url><loc>https://kca.kr/participants/list.do</loc></url>
            </urlset>
            """.strip()
        if url == "https://kca.kr/sitemap_index.xml":
            return "<sitemapindex></sitemapindex>"
        return "<html></html>"

    monkeypatch.setattr(company_seed_module, "_fetch_text", fake_fetch_text)

    discovered, summary = discover_company_seed_sources(paths, settings=None)

    assert summary["catalog_host_seed_source_count"] >= 1
    assert "https://kca.kr/participants/list.do" in set(discovered["source_url"])
    assert "https://kca.kr/sitemap.xml" in fetched_urls


def test_discover_sources_from_html_catalog_promotes_trusted_external_official_domain(
    monkeypatch: pytest.MonkeyPatch, sandbox_project: Path
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    _clear_catalog_discovery_side_inputs(paths)
    paths.company_seed_sources_path.write_text(
        """
sources:
  - source_name: 공공기관 공식 카탈로그
    source_type: html_link_catalog_url
    source_url: https://host.example.com/catalog
    source_title: 공공기관 공식 카탈로그
    company_tier: 공공·연구기관
    candidate_seed_type: 사업참여기업목록
    candidate_seed_reason: 공식 카탈로그 기반 후보
    allowed_domains: host.example.com
    discovery_include_keywords: 참여기업;participants
    discovery_exclude_keywords: notice
    max_discovered_sources: 5
""".strip()
        + "\n",
        encoding="utf-8",
    )

    def fake_fetch_text(url: str, settings=None, **kwargs) -> str:
        if url == "https://host.example.com/catalog":
            return """
            <html><body>
              <a href="https://support.or.kr/participants/list">참여기업 목록</a>
              <a href="https://private.example.com/participants/list">참여기업 목록</a>
            </body></html>
            """.strip()
        if url == "https://host.example.com/sitemap.xml":
            return "<urlset></urlset>"
        if url == "https://host.example.com/sitemap_index.xml":
            return "<sitemapindex></sitemapindex>"
        return "<html></html>"

    monkeypatch.setattr(company_seed_module, "_fetch_text", fake_fetch_text)

    discovered, summary = discover_company_seed_sources(paths, settings=None)

    assert summary["newly_discovered_seed_source_count"] == 1
    row = discovered.iloc[0].fillna("").to_dict()
    assert row["source_type"] == "html_link_catalog_url"
    assert row["source_url"] == "https://support.or.kr/participants/list"
    assert row["allowed_domains"] == "support.or.kr"
    assert row["candidate_seed_type"] == "공식카탈로그도메인자동발견"


def test_discover_sources_from_html_catalog_discovers_external_trusted_host_root(
    monkeypatch: pytest.MonkeyPatch, sandbox_project: Path
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    _clear_catalog_discovery_side_inputs(paths)
    paths.company_seed_sources_path.write_text(
        """
sources:
  - source_name: 공공기관 공식 카탈로그
    source_type: html_link_catalog_url
    source_url: https://host.example.com/catalog
    source_title: 공공기관 공식 카탈로그
    company_tier: 공공·연구기관
    candidate_seed_type: 사업참여기업목록
    candidate_seed_reason: 공식 카탈로그 기반 후보
    allowed_domains: host.example.com
    discovery_include_keywords: 참여기업;participants
    discovery_exclude_keywords: notice
    max_discovered_sources: 5
""".strip()
        + "\n",
        encoding="utf-8",
    )

    def fake_fetch_text(url: str, settings=None, **kwargs) -> str:
        if url == "https://host.example.com/catalog":
            return """
            <html><body>
              <a href="https://support.or.kr/main.do">한국지원기관 참여기업 목록</a>
            </body></html>
            """.strip()
        if url == "https://host.example.com/sitemap.xml":
            return "<urlset></urlset>"
        if url == "https://host.example.com/sitemap_index.xml":
            return "<sitemapindex></sitemapindex>"
        return "<html></html>"

    monkeypatch.setattr(company_seed_module, "_fetch_text", fake_fetch_text)

    discovered, summary = discover_company_seed_sources(paths, settings=AppSettings())

    assert summary["newly_discovered_seed_source_count"] == 1
    row = discovered.iloc[0].fillna("").to_dict()
    assert row["source_type"] == "html_link_catalog_url"
    assert row["source_url"] == "https://support.or.kr"
    assert row["allowed_domains"] == "support.or.kr"
    assert row["candidate_seed_type"] == "외부공식카탈로그호스트자동발견"


def test_discover_sources_from_html_catalog_skips_generic_external_public_root(
    monkeypatch: pytest.MonkeyPatch, sandbox_project: Path
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    _clear_catalog_discovery_side_inputs(paths)
    paths.company_seed_sources_path.write_text(
        """
sources:
  - source_name: 공공기관 공식 카탈로그
    source_type: html_link_catalog_url
    source_url: https://host.example.com/catalog
    source_title: 공공기관 공식 카탈로그
    company_tier: 공공·연구기관
    candidate_seed_type: 사업참여기업목록
    candidate_seed_reason: 공식 카탈로그 기반 후보
    allowed_domains: host.example.com
    discovery_include_keywords: 참여기업;participants
    discovery_exclude_keywords: notice
    max_discovered_sources: 5
""".strip()
        + "\n",
        encoding="utf-8",
    )

    def fake_fetch_text(url: str, settings=None, **kwargs) -> str:
        if url == "https://host.example.com/catalog":
            return """
            <html><body>
              <a href="https://open.go.kr">정보공개청구</a>
            </body></html>
            """.strip()
        if url == "https://host.example.com/sitemap.xml":
            return "<urlset></urlset>"
        if url == "https://host.example.com/sitemap_index.xml":
            return "<sitemapindex></sitemapindex>"
        return "<html></html>"

    monkeypatch.setattr(company_seed_module, "_fetch_text", fake_fetch_text)

    discovered, summary = discover_company_seed_sources(paths, settings=AppSettings())

    assert summary["newly_discovered_seed_source_count"] == 0
    assert discovered.empty


def test_discover_sources_from_html_catalog_skips_blocklisted_public_service_host(
    monkeypatch: pytest.MonkeyPatch, sandbox_project: Path
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    _clear_catalog_discovery_side_inputs(paths)
    paths.company_seed_sources_path.write_text(
        """
sources:
  - source_name: 공공기관 공식 카탈로그
    source_type: html_link_catalog_url
    source_url: https://host.example.com/catalog
    source_title: 공공기관 공식 카탈로그
    company_tier: 공공·연구기관
    candidate_seed_type: 사업참여기업목록
    candidate_seed_reason: 공식 카탈로그 기반 후보
    allowed_domains: host.example.com
    discovery_include_keywords: 참여기업;participants
    discovery_exclude_keywords: notice
    max_discovered_sources: 5
""".strip()
        + "\n",
        encoding="utf-8",
    )

    def fake_fetch_text(url: str, settings=None, **kwargs) -> str:
        if url == "https://host.example.com/catalog":
            return """
            <html><body>
              <a href="https://epeople.go.kr/nep/pttn/gnrlPttn/pttnSittnList.npaid">참여기업 민원 포털</a>
            </body></html>
            """.strip()
        if url == "https://host.example.com/sitemap.xml":
            return "<urlset></urlset>"
        if url == "https://host.example.com/sitemap_index.xml":
            return "<sitemapindex></sitemapindex>"
        return "<html></html>"

    monkeypatch.setattr(company_seed_module, "_fetch_text", fake_fetch_text)

    discovered, summary = discover_company_seed_sources(paths, settings=AppSettings())

    assert summary["newly_discovered_seed_source_count"] == 0
    assert discovered.empty


def test_discover_sources_from_html_catalog_skips_generic_external_institution_root_without_catalog_keywords(
    monkeypatch: pytest.MonkeyPatch, sandbox_project: Path
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    _clear_catalog_discovery_side_inputs(paths)
    paths.company_seed_sources_path.write_text(
        """
sources:
  - source_name: 공공기관 공식 카탈로그
    source_type: html_link_catalog_url
    source_url: https://host.example.com/catalog
    source_title: 공공기관 공식 카탈로그
    company_tier: 공공·연구기관
    candidate_seed_type: 사업참여기업목록
    candidate_seed_reason: 공식 카탈로그 기반 후보
    allowed_domains: host.example.com
    discovery_include_keywords: 참여기업;participants
    discovery_exclude_keywords: notice
    max_discovered_sources: 5
""".strip()
        + "\n",
        encoding="utf-8",
    )

    def fake_fetch_text(url: str, settings=None, **kwargs) -> str:
        if url == "https://host.example.com/catalog":
            return """
            <html><body>
              <a href="https://weather.go.kr">기상청</a>
            </body></html>
            """.strip()
        if url == "https://host.example.com/sitemap.xml":
            return "<urlset></urlset>"
        if url == "https://host.example.com/sitemap_index.xml":
            return "<sitemapindex></sitemapindex>"
        return "<html></html>"

    monkeypatch.setattr(company_seed_module, "_fetch_text", fake_fetch_text)

    discovered, summary = discover_company_seed_sources(paths, settings=AppSettings())

    assert summary["newly_discovered_seed_source_count"] == 0
    assert discovered.empty


def test_discover_sources_from_html_catalog_skips_detail_and_download_urls(
    monkeypatch: pytest.MonkeyPatch, sandbox_project: Path
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    _clear_catalog_discovery_side_inputs(paths)
    paths.company_seed_sources_path.write_text(
        """
sources:
  - source_name: 공공기관 공식 카탈로그
    source_type: html_link_catalog_url
    source_url: https://host.example.com/catalog
    source_title: 공공기관 공식 카탈로그
    company_tier: 공공·연구기관
    candidate_seed_type: 사업참여기업목록
    candidate_seed_reason: 공식 카탈로그 기반 후보
    allowed_domains: host.example.com
    discovery_include_keywords: 참여기업;participants
    discovery_exclude_keywords: notice
    max_discovered_sources: 5
""".strip()
        + "\n",
        encoding="utf-8",
    )

    def fake_fetch_text(url: str, settings=None, **kwargs) -> str:
        if url == "https://host.example.com/catalog":
            return """
            <html><body>
              <a href="https://host.example.com/fileDownload.do?action=fileDown">참여기업 목록 첨부파일</a>
              <a href="https://host.example.com/pims/view.do?id=100">참여기업 목록 상세보기</a>
            </body></html>
            """.strip()
        if url == "https://host.example.com/sitemap.xml":
            return "<urlset></urlset>"
        if url == "https://host.example.com/sitemap_index.xml":
            return "<sitemapindex></sitemapindex>"
        return "<html></html>"

    monkeypatch.setattr(company_seed_module, "_fetch_text", fake_fetch_text)

    discovered, summary = discover_company_seed_sources(paths, settings=AppSettings())

    assert summary["newly_discovered_seed_source_count"] == 0
    assert discovered.empty


def test_discover_company_seed_sources_uses_web_search_for_new_top_level_catalogs(
    monkeypatch: pytest.MonkeyPatch, sandbox_project: Path
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    _clear_catalog_discovery_side_inputs(paths)
    paths.company_seed_sources_path.write_text("sources: []\n", encoding="utf-8")

    def fake_fetch_text(url: str, settings=None, **kwargs) -> str:
        if "html.duckduckgo.com/html/" in url:
            return """
            <html><body>
              <a href="https://duckduckgo.com/l/?uddg=https%3A%2F%2Fbrandnew.or.kr%2Fparticipants%2Flist.do">참여기업 목록</a>
              <a href="https://duckduckgo.com/l/?uddg=https%3A%2F%2Ffresh.go.kr%2Fmain.do">한국진흥원</a>
            </body></html>
            """.strip()
        return "<html></html>"

    monkeypatch.setattr(company_seed_module, "_fetch_text", fake_fetch_text)

    discovered, summary = discover_company_seed_sources(paths, settings=AppSettings())

    assert summary["newly_discovered_seed_source_count"] >= 2
    discovered_rows = discovered.fillna("").to_dict(orient="records")
    assert any(
        row["source_url"] == "https://brandnew.or.kr/participants/list.do"
        and row["candidate_seed_type"] == "웹검색공식카탈로그자동발견"
        for row in discovered_rows
    )
    assert any(
        row["source_url"] == "https://fresh.go.kr"
        and row["candidate_seed_type"] == "웹검색공식카탈로그호스트자동발견"
        for row in discovered_rows
    )


def test_discover_company_seed_sources_uses_web_search_for_public_bare_kr_catalog_host(
    monkeypatch: pytest.MonkeyPatch, sandbox_project: Path
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    _clear_catalog_discovery_side_inputs(paths)
    paths.company_seed_sources_path.write_text("sources: []\n", encoding="utf-8")

    def fake_fetch_text(url: str, settings=None, **kwargs) -> str:
        if "html.duckduckgo.com/html/" in url:
            return """
            <html><body>
              <a href="https://duckduckgo.com/l/?uddg=https%3A%2F%2Fkca.kr%2Fmain.do">한국방송통신전파진흥원</a>
            </body></html>
            """.strip()
        return "<html></html>"

    monkeypatch.setattr(company_seed_module, "_fetch_text", fake_fetch_text)

    discovered, summary = discover_company_seed_sources(paths, settings=AppSettings())

    assert summary["newly_discovered_seed_source_count"] >= 1
    discovered_rows = discovered.fillna("").to_dict(orient="records")
    assert any(
        row["source_url"] == "https://kca.kr"
        and row["candidate_seed_type"] == "웹검색공식카탈로그호스트자동발견"
        for row in discovered_rows
    )


def test_discover_company_seed_sources_web_search_respects_cooldown(
    monkeypatch: pytest.MonkeyPatch, sandbox_project: Path
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    _clear_catalog_discovery_side_inputs(paths)
    paths.company_seed_sources_path.write_text("sources: []\n", encoding="utf-8")

    fetch_count = {"search": 0}

    def fake_fetch_text(url: str, settings=None, **kwargs) -> str:
        if "html.duckduckgo.com/html/" in url:
            fetch_count["search"] += 1
            return """
            <html><body>
              <a href="https://duckduckgo.com/l/?uddg=https%3A%2F%2Fbrandnew.or.kr%2Fparticipants%2Flist.do">참여기업 목록</a>
            </body></html>
            """.strip()
        return "<html></html>"

    monkeypatch.setattr(company_seed_module, "_fetch_text", fake_fetch_text)

    _, first_summary = discover_company_seed_sources(paths, settings=AppSettings())
    _, second_summary = discover_company_seed_sources(paths, settings=AppSettings())

    assert first_summary["newly_discovered_seed_source_count"] >= 1
    assert second_summary["newly_discovered_seed_source_count"] == 0
    assert fetch_count["search"] >= 1


def test_discover_company_seed_sources_web_search_reruns_when_query_signature_changes(
    monkeypatch: pytest.MonkeyPatch, sandbox_project: Path
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    _clear_catalog_discovery_side_inputs(paths)
    paths.company_seed_sources_path.write_text("sources: []\n", encoding="utf-8")

    fetch_count = {"search": 0}

    def fake_fetch_text(url: str, settings=None, **kwargs) -> str:
        if "html.duckduckgo.com/html/" in url:
            fetch_count["search"] += 1
            return """
            <html><body>
              <a href="https://duckduckgo.com/l/?uddg=https%3A%2F%2Fbrandnew.or.kr%2Fparticipants%2Flist.do">참여기업 목록</a>
            </body></html>
            """.strip()
        return "<html></html>"

    monkeypatch.setattr(company_seed_module, "_fetch_text", fake_fetch_text)

    _, first_summary = discover_company_seed_sources(paths, settings=AppSettings())
    progress_path = paths.runtime_dir / "company_seed_search_progress.json"
    payload = json.loads(progress_path.read_text(encoding="utf-8"))
    payload["query_signature"] = "outdated"
    progress_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    _, second_summary = discover_company_seed_sources(paths, settings=AppSettings())

    assert first_summary["newly_discovered_seed_source_count"] >= 1
    assert second_summary["newly_discovered_seed_source_count"] == 0
    assert fetch_count["search"] >= 2


def test_discover_company_seed_sources_skips_timed_out_catalog_host_sources(
    monkeypatch: pytest.MonkeyPatch, sandbox_project: Path
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    _clear_catalog_discovery_side_inputs(paths)
    paths.company_seed_sources_path.write_text("sources: []\n", encoding="utf-8")
    pd.DataFrame(
        [
            {
                "company_name": "테스트산업진흥원",
                "company_tier": "공공·연구기관",
                "official_domain": "support.example.com",
            }
        ]
    ).to_csv(paths.companies_registry_path, index=False)

    def fake_fetch_text(url: str, settings=None, **kwargs) -> str:
        if url == "https://support.example.com":
            raise httpx.ConnectTimeout("timed out")
        if url == "https://support.example.com/sitemap.xml":
            return "<urlset></urlset>"
        if url == "https://support.example.com/sitemap_index.xml":
            return "<sitemapindex></sitemapindex>"
        return "<html></html>"

    monkeypatch.setattr(company_seed_module, "_fetch_text", fake_fetch_text)

    discovered, summary = discover_company_seed_sources(paths, settings=None)

    assert discovered.empty
    assert summary["catalog_host_seed_source_count"] >= 1
    assert summary["skipped_catalog_source_count"] == 1
    assert summary["skipped_catalog_sources"][0]["stage"] == "html_catalog"


def test_discover_company_seed_sources_caches_timed_out_catalog_host_sources(
    monkeypatch: pytest.MonkeyPatch, sandbox_project: Path
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    _clear_catalog_discovery_side_inputs(paths)
    paths.company_seed_sources_path.write_text("sources: []\n", encoding="utf-8")
    pd.DataFrame(
        [
            {
                "company_name": "테스트산업진흥원",
                "company_tier": "공공·연구기관",
                "official_domain": "support.example.com",
            }
        ]
    ).to_csv(paths.companies_registry_path, index=False)

    fetch_count = {"host": 0}

    def fake_fetch_text(url: str, settings=None, **kwargs) -> str:
        if url == "https://support.example.com":
            fetch_count["host"] += 1
            raise httpx.ConnectTimeout("timed out")
        if url == "https://support.example.com/sitemap.xml":
            return "<urlset></urlset>"
        if url == "https://support.example.com/sitemap_index.xml":
            return "<sitemapindex></sitemapindex>"
        return "<html></html>"

    monkeypatch.setattr(company_seed_module, "_fetch_text", fake_fetch_text)

    first_discovered, first_summary = discover_company_seed_sources(paths, settings=AppSettings())
    second_discovered, second_summary = discover_company_seed_sources(paths, settings=AppSettings())

    assert first_discovered.empty
    assert first_summary["skipped_catalog_source_count"] == 1
    assert second_discovered.empty
    assert second_summary["cached_skip_catalog_source_count"] >= 1
    assert second_summary["skipped_catalog_source_count"] == 0
    assert fetch_count["host"] == 1


def test_discover_company_seed_sources_skips_recently_refreshed_catalog_sources(
    monkeypatch: pytest.MonkeyPatch, sandbox_project: Path
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    _clear_catalog_discovery_side_inputs(paths)
    paths.company_seed_sources_path.write_text(
        """
sources:
  - source_name: 상위 공식 카탈로그
    source_type: html_link_catalog_url
    source_url: https://catalog.example.com/root
    source_title: 상위 공식 카탈로그
    company_tier: 중견/중소
    candidate_seed_type: 공식사업참여기업목록
    candidate_seed_reason: 공식 카탈로그 기반 후보
    discovery_include_keywords: 참여기업;participants
    max_discovered_sources: 5
""".strip()
        + "\n",
        encoding="utf-8",
    )

    fetch_count = {"root": 0}

    def fake_fetch_text(url: str, settings=None, **kwargs) -> str:
        if url == "https://catalog.example.com/root":
            fetch_count["root"] += 1
            return '<html><body><a href="companies.csv">참여기업 목록 CSV</a></body></html>'
        if url == "https://catalog.example.com/companies.csv":
            return "기업명,홈페이지,지역\n알파AI,https://alpha.ai,서울\n"
        if url == "https://catalog.example.com/sitemap.xml":
            return "<urlset></urlset>"
        if url == "https://catalog.example.com/sitemap_index.xml":
            return "<sitemapindex></sitemapindex>"
        return "<html></html>"

    monkeypatch.setattr(company_seed_module, "_fetch_text", fake_fetch_text)

    _, first_summary = discover_company_seed_sources(paths, settings=AppSettings())
    _, second_summary = discover_company_seed_sources(paths, settings=AppSettings())

    assert first_summary["newly_discovered_seed_source_count"] >= 1
    assert second_summary["cached_refresh_catalog_source_count"] >= 1
    assert fetch_count["root"] == 1


def test_discover_company_seed_sources_processes_catalog_sources_in_batches(
    monkeypatch: pytest.MonkeyPatch,
    sandbox_project: Path,
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    _clear_catalog_discovery_side_inputs(paths)
    catalog_dir = sandbox_project / "tests" / "fixtures" / "company_seed_sources"
    catalog_dir.mkdir(parents=True, exist_ok=True)
    for idx in range(3):
        (catalog_dir / f"catalog_{idx}.html").write_text(
            f'<html><body><a href="companies_{idx}.csv">참여기업 목록 {idx}</a></body></html>',
            encoding="utf-8",
        )
        (catalog_dir / f"companies_{idx}.csv").write_text(
            f"기업명,홈페이지,지역\n테스트기업{idx},https://test{idx}.example.com,서울\n",
            encoding="utf-8",
        )
    paths.company_seed_sources_path.write_text(
        f"""
sources:
  - source_name: 카탈로그 0
    source_type: html_link_catalog_file
    source_url: https://catalog0.example.com/root
    local_path: {catalog_dir.relative_to(sandbox_project) / 'catalog_0.html'}
    source_title: 카탈로그 0
    company_tier: 중견/중소
    candidate_seed_type: 참여기관목록
    candidate_seed_reason: 공식 카탈로그 기반 후보
    discovery_include_keywords: 참여기업
  - source_name: 카탈로그 1
    source_type: html_link_catalog_file
    source_url: https://catalog1.example.com/root
    local_path: {catalog_dir.relative_to(sandbox_project) / 'catalog_1.html'}
    source_title: 카탈로그 1
    company_tier: 중견/중소
    candidate_seed_type: 참여기관목록
    candidate_seed_reason: 공식 카탈로그 기반 후보
    discovery_include_keywords: 참여기업
  - source_name: 카탈로그 2
    source_type: html_link_catalog_file
    source_url: https://catalog2.example.com/root
    local_path: {catalog_dir.relative_to(sandbox_project) / 'catalog_2.html'}
    source_title: 카탈로그 2
    company_tier: 중견/중소
    candidate_seed_type: 참여기관목록
    candidate_seed_reason: 공식 카탈로그 기반 후보
    discovery_include_keywords: 참여기업
""".strip()
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        company_seed_module,
        "_discover_catalog_seed_sources_from_search",
        lambda *args, **kwargs: (
            [],
            {
                "search_query_count": 0,
                "search_query_start_offset": 0,
                "search_query_next_offset": 0,
                "search_query_batch_count": 0,
                "search_query_batch_size": 0,
                "search_discovered_seed_source_count": 0,
            },
        ),
    )
    monkeypatch.setattr(company_seed_module, "_discover_catalog_host_seed_sources", lambda *args, **kwargs: [])
    monkeypatch.setattr(company_seed_module, "_discover_top_level_catalog_sources_from_sitemap", lambda *args, **kwargs: [])

    settings = AppSettings(company_seed_catalog_batch_size=1, company_seed_catalog_refresh_hours=0)
    _, first_summary = discover_company_seed_sources(paths, settings=settings)
    _, second_summary = discover_company_seed_sources(paths, settings=settings)

    assert first_summary["catalog_source_count"] == 1
    assert first_summary["catalog_source_start_offset"] == 0
    assert first_summary["catalog_source_next_offset"] == 1
    assert second_summary["catalog_source_count"] == 1
    assert second_summary["catalog_source_start_offset"] == 1
    assert second_summary["catalog_source_next_offset"] in {0, 2}


def test_search_catalog_queries_expand_site_and_org_terms() -> None:
    queries = company_seed_module._search_catalog_queries()

    assert "site:go.kr 참여기업 공급기업 수행기관" in queries
    assert "site:or.kr 참여기업 선정기업 기업목록 혁신센터" in queries
    assert "site:kr 입주기업 회원사 기관목록 테크노파크" in queries
    assert "공급기업 pool company list 지원단" in queries
    assert len(queries) == len(set(queries))


def test_search_catalog_query_urls_match_generated_queries() -> None:
    queries = company_seed_module._search_catalog_queries()
    urls = company_seed_module._search_catalog_query_urls()

    assert len(urls) == len(queries)
    assert urls[0].startswith("https://html.duckduckgo.com/html/?q=")
    assert company_seed_module._search_catalog_query_signature() == "|".join(queries)


def test_discover_catalog_seed_sources_from_search_processes_queries_in_batches(
    monkeypatch: pytest.MonkeyPatch,
    sandbox_project: Path,
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    fetched_urls: list[str] = []

    def fake_fetch_text(url: str, settings=None, **kwargs) -> str:
        fetched_urls.append(url)
        return "<html><body></body></html>"

    monkeypatch.setattr(company_seed_module, "_fetch_text", fake_fetch_text)

    settings = AppSettings(company_seed_search_query_batch_size=2, company_seed_search_cooldown_hours=24)
    first, first_summary = company_seed_module._discover_catalog_seed_sources_from_search(paths, [], settings)
    second, second_summary = company_seed_module._discover_catalog_seed_sources_from_search(paths, [], settings)

    assert first == []
    assert second == []
    assert len(fetched_urls) == 4
    assert first_summary["search_query_start_offset"] == 0
    assert first_summary["search_query_next_offset"] == 2
    assert first_summary["search_query_batch_count"] == 2
    assert second_summary["search_query_start_offset"] == 2
    assert second_summary["search_query_next_offset"] == 4

    progress = json.loads((paths.runtime_dir / "company_seed_search_progress.json").read_text(encoding="utf-8"))
    assert progress["total_query_count"] == len(company_seed_module._search_catalog_queries())
    assert progress["next_query_offset"] == 4


def test_discover_company_seed_sources_respects_runtime_budget(
    monkeypatch: pytest.MonkeyPatch,
    sandbox_project: Path,
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    _clear_catalog_discovery_side_inputs(paths)
    catalog_dir = sandbox_project / "tests" / "fixtures" / "company_seed_sources"
    catalog_dir.mkdir(parents=True, exist_ok=True)
    for idx in range(3):
        (catalog_dir / f"runtime_catalog_{idx}.html").write_text(
            f'<html><body><a href="companies_{idx}.csv">참여기업 목록 {idx}</a></body></html>',
            encoding="utf-8",
        )
        (catalog_dir / f"companies_{idx}.csv").write_text(
            f"기업명,홈페이지,지역\n런타임기업{idx},https://runtime{idx}.example.com,서울\n",
            encoding="utf-8",
        )
    paths.company_seed_sources_path.write_text(
        f"""
sources:
  - source_name: 런타임 카탈로그 0
    source_type: html_link_catalog_file
    source_url: https://runtime0.example.com/root
    local_path: {catalog_dir.relative_to(sandbox_project) / 'runtime_catalog_0.html'}
    source_title: 런타임 카탈로그 0
    company_tier: 중견/중소
    candidate_seed_type: 참여기관목록
    candidate_seed_reason: 공식 카탈로그 기반 후보
    discovery_include_keywords: 참여기업
  - source_name: 런타임 카탈로그 1
    source_type: html_link_catalog_file
    source_url: https://runtime1.example.com/root
    local_path: {catalog_dir.relative_to(sandbox_project) / 'runtime_catalog_1.html'}
    source_title: 런타임 카탈로그 1
    company_tier: 중견/중소
    candidate_seed_type: 참여기관목록
    candidate_seed_reason: 공식 카탈로그 기반 후보
    discovery_include_keywords: 참여기업
  - source_name: 런타임 카탈로그 2
    source_type: html_link_catalog_file
    source_url: https://runtime2.example.com/root
    local_path: {catalog_dir.relative_to(sandbox_project) / 'runtime_catalog_2.html'}
    source_title: 런타임 카탈로그 2
    company_tier: 중견/중소
    candidate_seed_type: 참여기관목록
    candidate_seed_reason: 공식 카탈로그 기반 후보
    discovery_include_keywords: 참여기업
""".strip()
        + "\n",
        encoding="utf-8",
    )

    clock_values = iter([0.0, 0.2, 1.1, 1.2])
    monkeypatch.setattr(company_seed_module, "monotonic", lambda: next(clock_values, 1.3))
    monkeypatch.setattr(
        company_seed_module,
        "_discover_catalog_seed_sources_from_search",
        lambda *args, **kwargs: (
            [],
            {
                "search_query_count": 0,
                "search_query_start_offset": 0,
                "search_query_next_offset": 0,
                "search_query_batch_count": 0,
                "search_discovered_seed_source_count": 0,
            },
        ),
    )
    monkeypatch.setattr(company_seed_module, "_discover_catalog_host_seed_sources", lambda *args, **kwargs: [])
    monkeypatch.setattr(company_seed_module, "_discover_top_level_catalog_sources_from_sitemap", lambda *args, **kwargs: [])

    settings = AppSettings(
        company_seed_catalog_batch_size=3,
        company_seed_catalog_max_runtime_seconds=1.0,
        company_seed_catalog_refresh_hours=0,
    )
    _, summary = discover_company_seed_sources(paths, settings=settings)

    assert summary["catalog_source_runtime_limited"] is True
    assert summary["catalog_source_count"] == 2
    assert summary["catalog_source_start_offset"] == 0
    assert summary["catalog_source_next_offset"] == 2


def test_collect_company_seed_records_pipeline_reuses_recent_cached_records(
    monkeypatch: pytest.MonkeyPatch,
    sandbox_project: Path,
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    cached = pd.DataFrame(
        [
            {
                "company_name": "캐시기업",
                "company_name_en": "",
                "company_tier": "중견/중소",
                "region": "서울",
                "official_domain": "cached.example.com",
                "source_url": "",
                "candidate_seed_type": "공식사업참여기업목록",
                "candidate_seed_url": "https://catalog.example.com/list.xlsx",
                "candidate_seed_title": "캐시 목록",
                "candidate_seed_reason": "최근 캐시",
            }
        ],
        columns=list(IMPORT_COMPANY_COLUMNS),
    )
    cached.to_csv(paths.collected_company_seed_records_path, index=False)
    paths.company_seed_sources_path.write_text(
        """
sources:
  - source_name: 승인 소스
    source_type: csv_url
    source_url: https://example.com/approved.csv
    source_title: 승인 소스
  - source_name: 카탈로그 소스
    source_type: html_link_catalog_url
    source_url: https://example.com/catalog
    source_title: 카탈로그 소스
""".strip()
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        pipelines_module,
        "collect_company_seed_records",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should use cached records")),
    )
    monkeypatch.setattr(
        pipelines_module,
        "refresh_company_seed_sources",
        lambda *args, **kwargs: {
            "catalog_source_count": 3,
            "total_catalog_source_count": 10,
            "catalog_source_start_offset": 2,
            "catalog_source_next_offset": 5,
            "catalog_host_seed_source_count": 7,
            "search_query_count": 99,
            "search_query_start_offset": 12,
            "search_query_next_offset": 24,
            "search_query_batch_count": 12,
            "search_query_batch_size": 12,
            "search_discovered_seed_source_count": 4,
            "discovered_seed_source_count": 8,
            "newly_discovered_seed_source_count": 0,
            "auto_promoted_shadow_seed_source_count": 0,
            "duplicate_shadow_seed_source_count": 2,
            "invalid_shadow_seed_source_count": 0,
        },
    )

    summary = collect_company_seed_records_pipeline(project_root=sandbox_project)

    assert summary["used_cached_collected_seed_records"] is True
    assert summary["collected_company_count"] == 1
    assert summary["seed_source_mode"] == "cached_records"
    assert summary["seed_source_count"] == 1
    assert summary["approved_seed_source_count"] == 1
    assert summary["newly_discovered_seed_source_count"] == 0
    assert summary["auto_promoted_shadow_seed_source_count"] == 0
    assert summary["catalog_source_count"] == 3
    assert summary["search_query_count"] == 99
    assert summary["seed_source_refresh"]["search_query_next_offset"] == 24


def test_collect_company_seed_records_pipeline_refreshes_cache_when_seed_sources_change(
    monkeypatch: pytest.MonkeyPatch,
    sandbox_project: Path,
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    cached = pd.DataFrame(
        [
            {
                "company_name": "캐시기업",
                "company_name_en": "",
                "company_tier": "중견/중소",
                "region": "서울",
                "official_domain": "cached.example.com",
                "source_url": "",
                "candidate_seed_type": "공식사업참여기업목록",
                "candidate_seed_url": "https://catalog.example.com/list.xlsx",
                "candidate_seed_title": "캐시 목록",
                "candidate_seed_reason": "최근 캐시",
            }
        ],
        columns=list(IMPORT_COMPANY_COLUMNS),
    )
    cached.to_csv(paths.collected_company_seed_records_path, index=False)

    refreshed = cached.copy()
    refreshed.loc[0, "company_name"] = "새기업"
    monkeypatch.setattr(
        pipelines_module,
        "collect_company_seed_records",
        lambda *args, **kwargs: (
            refreshed,
            {
                "seed_source_count": 2,
                "collected_seed_record_count": 1,
                "collected_company_count": 1,
                "seed_source_mode": "refreshed_records",
                "newly_discovered_seed_source_count": 1,
                "auto_promoted_shadow_seed_source_count": 0,
            },
        ),
    )
    monkeypatch.setattr(
        pipelines_module,
        "refresh_company_seed_sources",
        lambda *args, **kwargs: {
            "newly_discovered_seed_source_count": 1,
            "auto_promoted_shadow_seed_source_count": 0,
        },
    )

    summary = collect_company_seed_records_pipeline(project_root=sandbox_project)
    stored = read_csv_or_empty(paths.collected_company_seed_records_path)

    assert summary["seed_source_mode"] == "refreshed_records"
    assert stored.iloc[0]["company_name"] == "새기업"


def test_build_discovered_source_row_normalizes_noisy_label() -> None:
    row = company_seed_module._build_discovered_source_row(
        {
            "source_name": "한국에너지정보문화재단 공식 홈페이지 자동 카탈로그 탐색 - http://www.keia.or.kr - 국립춘천박물관"
        },
        label="http://www.keia.or.kr - https://www.webwatch.or.kr/page - 국립춘천박물관 - 2026-03-30 - 한국고전 번역원",
        source_url="https://www.keia.or.kr/MedicalKorea",
    )

    assert row["source_title"] == "MedicalKorea"
    assert row["source_name"] == "한국에너지정보문화재단 공식 홈페이지 자동 카탈로그 탐색 - MedicalKorea"


def test_load_company_seed_sources_dedupes_same_url_with_different_names(sandbox_project: Path) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    discovered = pd.DataFrame(
        [
            {
                "source_name": "기관 A 공식 홈페이지 자동 카탈로그 탐색 - 긴 제목 A",
                "source_type": "html_link_catalog_url",
                "discovered_source_type": "",
                "source_url": "https://example.or.kr/catalog",
                "request_url": "",
                "local_path": "",
                "source_title": "긴 제목 A",
                "company_tier": "공공·연구기관",
                "candidate_seed_type": "공식카탈로그도메인자동발견",
                "candidate_seed_reason": "테스트",
                "table_index": "",
                "header_row": "",
                "company_name_column": "",
                "official_domain_column": "",
                "company_name_en_column": "",
                "region_column": "",
                "aliases_column": "",
                "filter_text_columns": "",
                "include_keywords": "",
                "exclude_keywords": "",
                "allowed_domains": "example.or.kr",
                "discovery_include_keywords": "",
                "discovery_exclude_keywords": "",
                "max_discovered_sources": "",
                "first_seen_at": "2026-03-31T10:00:00+09:00",
                "last_seen_at": "2026-03-31T10:00:00+09:00",
            },
            {
                "source_name": "기관 A 공식 홈페이지 자동 카탈로그 탐색 - 긴 제목 B",
                "source_type": "html_link_catalog_url",
                "discovered_source_type": "",
                "source_url": "https://example.or.kr/catalog?ref=other",
                "request_url": "",
                "local_path": "",
                "source_title": "긴 제목 B",
                "company_tier": "공공·연구기관",
                "candidate_seed_type": "공식카탈로그도메인자동발견",
                "candidate_seed_reason": "테스트",
                "table_index": "",
                "header_row": "",
                "company_name_column": "",
                "official_domain_column": "",
                "company_name_en_column": "",
                "region_column": "",
                "aliases_column": "",
                "filter_text_columns": "",
                "include_keywords": "",
                "exclude_keywords": "",
                "allowed_domains": "example.or.kr",
                "discovery_include_keywords": "",
                "discovery_exclude_keywords": "",
                "max_discovered_sources": "",
                "first_seen_at": "2026-03-31T10:00:00+09:00",
                "last_seen_at": "2026-03-31T11:00:00+09:00",
            },
        ],
        columns=list(company_seed_module.COMPANY_SEED_SOURCE_COLUMNS),
    )
    discovered.to_csv(paths.discovered_company_seed_sources_path, index=False)

    loaded = company_seed_module.load_company_seed_sources(paths)
    loaded = loaded.loc[loaded["source_url"].fillna("").str.contains("example\\.or\\.kr/catalog", regex=True)].reset_index(
        drop=True
    )

    assert len(loaded) == 1
    assert loaded.iloc[0]["source_title"] == "긴 제목 B"


def test_auto_promote_shadow_seed_sources_skips_duplicate_source(sandbox_project: Path) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    approved = pd.DataFrame(
        [
            {
                "source_name": "NIPA 목록 - 공급기업",
                "source_type": "xlsx_url",
                "discovered_source_type": "",
                "source_url": "https://www.nipa.kr/file.xlsx?token=1",
                "request_url": "https://www.nipa.kr/file.xlsx?token=1",
                "local_path": "",
                "source_title": "공급기업 목록",
                "company_tier": "스타트업",
                "candidate_seed_type": "공식사업참여기업목록",
                "candidate_seed_reason": "공식 첨부파일",
                "table_index": "",
                "header_row": "",
                "company_name_column": "",
                "official_domain_column": "",
                "company_name_en_column": "",
                "region_column": "",
                "aliases_column": "",
                "filter_text_columns": "",
                "include_keywords": "",
                "exclude_keywords": "",
                "allowed_domains": "",
                "discovery_include_keywords": "",
                "discovery_exclude_keywords": "",
                "max_discovered_sources": "",
            }
        ]
    )
    shadow = approved.copy()
    shadow.loc[0, "source_url"] = "https://www.nipa.kr/file.xlsx?token=2"
    shadow.loc[0, "request_url"] = "https://www.nipa.kr/file.xlsx?token=2"
    approved.to_csv(paths.discovered_company_seed_sources_path, index=False)
    shadow.to_csv(paths.shadow_company_seed_sources_path, index=False)

    combined, remaining, summary = auto_promote_shadow_company_seed_sources(paths, settings=None)

    assert len(combined) == 1
    assert remaining.empty
    assert summary["auto_promoted_shadow_seed_source_count"] == 0
    assert summary["duplicate_shadow_seed_source_count"] == 1
    assert summary["remaining_shadow_seed_source_count"] == 0


def test_auto_promote_shadow_seed_sources_prunes_stale_shadow_rows(
    monkeypatch: pytest.MonkeyPatch, sandbox_project: Path
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    stale_shadow = pd.DataFrame(
        [
            {
                "source_name": "오래된 shadow source",
                "source_type": "html_table_url",
                "discovered_source_type": "",
                "source_url": "https://stale.example.com/catalog",
                "request_url": "https://stale.example.com/catalog",
                "local_path": "",
                "source_title": "오래된 카탈로그",
                "company_tier": "공공·연구기관",
                "candidate_seed_type": "웹검색공식카탈로그자동발견",
                "candidate_seed_reason": "오래된 탐색 결과",
                "table_index": "",
                "header_row": "",
                "company_name_column": "",
                "official_domain_column": "",
                "company_name_en_column": "",
                "region_column": "",
                "aliases_column": "",
                "filter_text_columns": "",
                "include_keywords": "",
                "exclude_keywords": "",
                "allowed_domains": "stale.example.com",
                "discovery_include_keywords": "",
                "discovery_exclude_keywords": "",
                "max_discovered_sources": "",
                "first_seen_at": "2000-01-01T00:00:00+00:00",
                "last_seen_at": "2000-01-01T00:00:00+00:00",
            }
        ]
    )
    stale_shadow.to_csv(paths.shadow_company_seed_sources_path, index=False)
    monkeypatch.setattr(
        company_seed_module,
        "_evaluate_shadow_seed_source",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("stale shadow should be pruned before evaluation")),
    )

    combined, remaining, summary = auto_promote_shadow_company_seed_sources(
        paths,
        AppSettings(company_seed_shadow_retention_hours=1),
    )

    assert combined.empty
    assert remaining.empty
    assert summary["pruned_shadow_seed_source_count"] == 1
    assert summary["shadow_seed_source_count_before"] == 0
    assert read_csv_or_empty(paths.shadow_company_seed_sources_path).empty


def test_auto_promote_shadow_seed_sources_prunes_stale_invalid_cache(
    monkeypatch: pytest.MonkeyPatch, sandbox_project: Path
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    now_iso = datetime.now(timezone.utc).isoformat()
    shadow = pd.DataFrame(
        [
            {
                "source_name": "승격 대상 source",
                "source_type": "html_table_url",
                "discovered_source_type": "",
                "source_url": "https://fresh.example.com/catalog",
                "request_url": "https://fresh.example.com/catalog",
                "local_path": "",
                "source_title": "신규 카탈로그",
                "company_tier": "공공·연구기관",
                "candidate_seed_type": "웹검색공식카탈로그자동발견",
                "candidate_seed_reason": "신규 탐색 결과",
                "table_index": "",
                "header_row": "",
                "company_name_column": "",
                "official_domain_column": "",
                "company_name_en_column": "",
                "region_column": "",
                "aliases_column": "",
                "filter_text_columns": "",
                "include_keywords": "",
                "exclude_keywords": "",
                "allowed_domains": "fresh.example.com",
                "discovery_include_keywords": "",
                "discovery_exclude_keywords": "",
                "max_discovered_sources": "",
                "first_seen_at": now_iso,
                "last_seen_at": now_iso,
            }
        ]
    )
    invalid = shadow.copy()
    invalid["invalid_reason"] = "old invalid"
    invalid["invalidated_at"] = "2000-01-01T00:00:00+00:00"
    shadow.to_csv(paths.shadow_company_seed_sources_path, index=False)
    invalid.to_csv(paths.invalid_company_seed_sources_path, index=False)
    monkeypatch.setattr(company_seed_module, "_evaluate_shadow_seed_source", lambda *args, **kwargs: (True, "회사 레코드 1건 추출"))

    combined, remaining, summary = auto_promote_shadow_company_seed_sources(
        paths,
        AppSettings(company_seed_invalid_retention_hours=1),
    )

    assert len(combined) == 1
    assert remaining.empty
    assert summary["auto_promoted_shadow_seed_source_count"] == 1
    assert summary["duplicate_shadow_seed_source_count"] == 0
    assert summary["pruned_invalid_seed_source_count"] == 1
    assert load_invalid_company_seed_sources(paths).empty


def test_auto_promote_shadow_seed_sources_caps_shadow_backlog(
    monkeypatch: pytest.MonkeyPatch, sandbox_project: Path
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    now = datetime.now(timezone.utc)
    rows = []
    for index in range(3):
        seen_at = (now - timedelta(minutes=index)).replace(microsecond=0)
        rows.append(
            {
                "source_name": f"shadow-{index}",
                "source_type": "html_table_url",
                "discovered_source_type": "",
                "source_url": f"https://shadow{index}.example.com/catalog",
                "request_url": f"https://shadow{index}.example.com/catalog",
                "local_path": "",
                "source_title": f"shadow {index}",
                "company_tier": "공공·연구기관",
                "candidate_seed_type": "웹검색공식카탈로그자동발견",
                "candidate_seed_reason": "backlog cap test",
                "table_index": "",
                "header_row": "",
                "company_name_column": "",
                "official_domain_column": "",
                "company_name_en_column": "",
                "region_column": "",
                "aliases_column": "",
                "filter_text_columns": "",
                "include_keywords": "",
                "exclude_keywords": "",
                "allowed_domains": f"shadow{index}.example.com",
                "discovery_include_keywords": "",
                "discovery_exclude_keywords": "",
                "max_discovered_sources": "",
                "first_seen_at": seen_at.isoformat(),
                "last_seen_at": seen_at.isoformat(),
            }
        )
    pd.DataFrame(rows).to_csv(paths.shadow_company_seed_sources_path, index=False)
    monkeypatch.setattr(
        company_seed_module,
        "_evaluate_shadow_seed_source",
        lambda *args, **kwargs: (False, "ConnectTimeout: timed out"),
    )

    combined, remaining, summary = auto_promote_shadow_company_seed_sources(
        paths,
        AppSettings(company_seed_shadow_max_rows=2),
    )

    assert combined.empty
    assert len(remaining) == 2
    assert summary["pruned_shadow_seed_source_count"] == 1
    remaining_names = set(remaining["source_name"])
    assert "shadow-2" not in remaining_names


def test_compact_company_seed_source_caches_prunes_shadow_and_invalid(sandbox_project: Path) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    shadow = pd.DataFrame(
        [
            {
                "source_name": "stale-shadow",
                "source_type": "html_table_url",
                "discovered_source_type": "",
                "source_url": "https://stale-shadow.example.com",
                "request_url": "https://stale-shadow.example.com",
                "local_path": "",
                "source_title": "stale",
                "company_tier": "공공·연구기관",
                "candidate_seed_type": "웹검색공식카탈로그자동발견",
                "candidate_seed_reason": "stale",
                "table_index": "",
                "header_row": "",
                "company_name_column": "",
                "official_domain_column": "",
                "company_name_en_column": "",
                "region_column": "",
                "aliases_column": "",
                "filter_text_columns": "",
                "include_keywords": "",
                "exclude_keywords": "",
                "allowed_domains": "stale-shadow.example.com",
                "discovery_include_keywords": "",
                "discovery_exclude_keywords": "",
                "max_discovered_sources": "",
                "first_seen_at": "2000-01-01T00:00:00+00:00",
                "last_seen_at": "2000-01-01T00:00:00+00:00",
            },
            {
                "source_name": "fresh-shadow",
                "source_type": "html_table_url",
                "discovered_source_type": "",
                "source_url": "https://fresh-shadow.example.com",
                "request_url": "https://fresh-shadow.example.com",
                "local_path": "",
                "source_title": "fresh",
                "company_tier": "공공·연구기관",
                "candidate_seed_type": "웹검색공식카탈로그자동발견",
                "candidate_seed_reason": "fresh",
                "table_index": "",
                "header_row": "",
                "company_name_column": "",
                "official_domain_column": "",
                "company_name_en_column": "",
                "region_column": "",
                "aliases_column": "",
                "filter_text_columns": "",
                "include_keywords": "",
                "exclude_keywords": "",
                "allowed_domains": "fresh-shadow.example.com",
                "discovery_include_keywords": "",
                "discovery_exclude_keywords": "",
                "max_discovered_sources": "",
                "first_seen_at": datetime.now(timezone.utc).isoformat(),
                "last_seen_at": datetime.now(timezone.utc).isoformat(),
            },
        ]
    )
    invalid = pd.DataFrame(
        [
            {
                **shadow.iloc[0].to_dict(),
                "invalid_reason": "old invalid",
                "invalidated_at": "2000-01-01T00:00:00+00:00",
            }
        ]
    )
    shadow.to_csv(paths.shadow_company_seed_sources_path, index=False)
    invalid.to_csv(paths.invalid_company_seed_sources_path, index=False)

    summary = compact_company_seed_source_caches(
        paths,
        AppSettings(company_seed_shadow_retention_hours=1, company_seed_invalid_retention_hours=1),
    )

    assert summary["pruned_shadow_seed_source_count"] == 1
    assert summary["pruned_invalid_seed_source_count"] == 1
    remaining_shadow = load_shadow_company_seed_sources(paths)
    remaining_invalid = load_invalid_company_seed_sources(paths)
    assert len(remaining_shadow) == 1
    assert remaining_invalid.empty


def test_auto_promote_shadow_seed_sources_processes_shadow_in_batches(
    monkeypatch: pytest.MonkeyPatch, sandbox_project: Path
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    now_iso = datetime.now(timezone.utc).isoformat()
    shadow = pd.DataFrame(
        [
            {
                "source_name": f"shadow-{index}",
                "source_type": "html_table_url",
                "discovered_source_type": "",
                "source_url": f"https://shadow{index}.example.com/catalog",
                "request_url": f"https://shadow{index}.example.com/catalog",
                "local_path": "",
                "source_title": f"shadow {index}",
                "company_tier": "공공·연구기관",
                "candidate_seed_type": "웹검색공식카탈로그자동발견",
                "candidate_seed_reason": "batch test",
                "table_index": "",
                "header_row": "",
                "company_name_column": "",
                "official_domain_column": "",
                "company_name_en_column": "",
                "region_column": "",
                "aliases_column": "",
                "filter_text_columns": "",
                "include_keywords": "",
                "exclude_keywords": "",
                "allowed_domains": f"shadow{index}.example.com",
                "discovery_include_keywords": "",
                "discovery_exclude_keywords": "",
                "max_discovered_sources": "",
                "first_seen_at": now_iso,
                "last_seen_at": now_iso,
            }
            for index in range(3)
        ]
    )
    shadow.to_csv(paths.shadow_company_seed_sources_path, index=False)
    evaluated: list[str] = []

    def fake_evaluate(source, *_args, **_kwargs):
        evaluated.append(source["source_name"])
        return False, "ConnectTimeout: timed out"

    monkeypatch.setattr(company_seed_module, "_evaluate_shadow_seed_source", fake_evaluate)

    combined, remaining, summary = auto_promote_shadow_company_seed_sources(
        paths,
        AppSettings(company_seed_shadow_batch_size=1, company_seed_shadow_max_batches_per_run=2),
    )

    assert combined.empty
    assert evaluated == ["shadow-0", "shadow-1"]
    assert summary["processed_shadow_seed_source_count"] == 2
    assert summary["deferred_shadow_seed_source_count"] == 2
    assert len(remaining) == 3
    assert list(remaining["source_name"]) == ["shadow-2", "shadow-0", "shadow-1"]


def test_auto_promote_shadow_seed_sources_respects_runtime_budget(
    monkeypatch: pytest.MonkeyPatch, sandbox_project: Path
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    now_iso = datetime.now(timezone.utc).isoformat()
    shadow = pd.DataFrame(
        [
            {
                "source_name": f"runtime-shadow-{index}",
                "source_type": "html_table_url",
                "discovered_source_type": "",
                "source_url": f"https://runtime-shadow{index}.example.com/catalog",
                "request_url": f"https://runtime-shadow{index}.example.com/catalog",
                "local_path": "",
                "source_title": f"runtime shadow {index}",
                "company_tier": "공공·연구기관",
                "candidate_seed_type": "웹검색공식카탈로그자동발견",
                "candidate_seed_reason": "runtime budget test",
                "table_index": "",
                "header_row": "",
                "company_name_column": "",
                "official_domain_column": "",
                "company_name_en_column": "",
                "region_column": "",
                "aliases_column": "",
                "filter_text_columns": "",
                "include_keywords": "",
                "exclude_keywords": "",
                "allowed_domains": f"runtime-shadow{index}.example.com",
                "discovery_include_keywords": "",
                "discovery_exclude_keywords": "",
                "max_discovered_sources": "",
                "first_seen_at": now_iso,
                "last_seen_at": now_iso,
            }
            for index in range(3)
        ]
    )
    shadow.to_csv(paths.shadow_company_seed_sources_path, index=False)

    evaluated: list[str] = []

    def fake_evaluate(source, *_args, **_kwargs):
        evaluated.append(source["source_name"])
        return True, "회사 레코드 1건 추출"

    clock_values = iter([0.0, 0.2, 0.8, 1.1])
    monkeypatch.setattr(company_seed_module, "_evaluate_shadow_seed_source", fake_evaluate)
    monkeypatch.setattr(company_seed_module, "monotonic", lambda: next(clock_values))

    combined, remaining, summary = auto_promote_shadow_company_seed_sources(
        paths,
        AppSettings(
            company_seed_shadow_batch_size=3,
            company_seed_shadow_max_batches_per_run=1,
            company_seed_shadow_max_runtime_seconds=1.0,
        ),
    )

    assert evaluated == ["runtime-shadow-0", "runtime-shadow-1"]
    assert summary["shadow_runtime_limited"] is True
    assert summary["processed_shadow_seed_source_count"] == 2
    assert summary["runtime_unprocessed_shadow_seed_source_count"] == 1
    assert summary["approved_seed_source_count_after"] == 2
    assert list(remaining["source_name"]) == ["runtime-shadow-2"]
    assert list(combined["source_name"]) == ["runtime-shadow-0", "runtime-shadow-1"]


def test_fetch_text_enforces_wall_clock_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeResponse:
        encoding = "utf-8"
        request = httpx.Request("GET", "https://slow.example.com/catalog")

        def raise_for_status(self) -> None:
            return None

        def iter_bytes(self):
            yield b"first"
            yield b"second"

    class FakeStream:
        def __enter__(self):
            return FakeResponse()

        def __exit__(self, exc_type, exc, tb):
            return False

    clock_values = iter([0.0, 0.6, 1.1])
    monkeypatch.setattr(company_seed_module, "monotonic", lambda: next(clock_values))
    monkeypatch.setattr(company_seed_module.httpx, "stream", lambda *args, **kwargs: FakeStream())

    with pytest.raises(httpx.ReadTimeout):
        company_seed_module._fetch_text(
            "https://slow.example.com/catalog",
            AppSettings(
                company_seed_timeout_seconds=1.0,
                company_seed_connect_timeout_seconds=0.5,
            ),
        )


def test_collect_company_seed_records_promotes_embedded_header_row_from_xlsx(sandbox_project: Path) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    fixture_dir = sandbox_project / "tests" / "fixtures" / "company_seed_sources"
    fixture_dir.mkdir(parents=True, exist_ok=True)

    workbook = BytesIO()
    with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
        pd.DataFrame(
            [
                ["2026년 AI 공급기업 현황", "", "", ""],
                ["", "", "", ""],
                ["연번", "기업명", "전문분야", "기업정보"],
                [1, "알파AI", "분석지능", "- 지역 : 서울특별시 - 홈페이지 : https://alpha.ai"],
            ]
        ).to_excel(writer, index=False, header=False)
    xlsx_path = fixture_dir / "embedded_header.xlsx"
    xlsx_path.write_bytes(workbook.getvalue())

    paths.company_seed_sources_path.write_text(
        f"""
sources:
  - source_name: 내장 헤더 XLSX
    source_type: xlsx_file
    local_path: {xlsx_path.relative_to(sandbox_project)}
    source_url: https://official.example.com/embedded-header
    source_title: 내장 헤더 XLSX
    company_tier: 중견/중소
    candidate_seed_type: 공식사업참여기업목록
    candidate_seed_reason: 공식 첨부파일
    header_row: 0
    company_name_column: 기업명
    official_domain_column: 기업정보
    region_column: 기업정보
    filter_text_columns: 기업명;전문분야;기업정보
""".strip()
        + "\n",
        encoding="utf-8",
    )

    records, summary = collect_company_seed_records(paths, settings=None)

    assert summary["collected_company_count"] == 1
    alpha = records.loc[records["company_name"] == "알파AI"].iloc[0]
    assert alpha["official_domain"] == "alpha.ai"
    assert alpha["region"] == "서울특별시"


def test_discover_companies_prefers_source_backed_seed_records(sandbox_project: Path) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    companies, summary = discover_companies(paths)
    assert summary["candidate_input_mode"] == "source_backed_seed_records"
    assert summary["seeded_candidate_count"] >= 8
    assert "토스" not in companies["company_name"].tolist()
    assert "부산테크" not in companies["company_name"].tolist()


def test_expand_company_candidates_pipeline_collects_seed_records_first(sandbox_project: Path) -> None:
    summary = expand_company_candidates_pipeline(project_root=sandbox_project)
    paths = ProjectPaths.from_root(sandbox_project)
    records = read_csv_or_empty(paths.collected_company_seed_records_path)
    companies = read_csv_or_empty(paths.companies_registry_path)
    assert summary["seed_source_count"] == 2
    assert summary["collected_company_count"] == 4
    assert summary["expanded_candidate_company_count"] >= 8
    assert not records.empty
    assert {"알파데이터", "감마AI연구원"} <= set(companies["company_name"])


def test_non_company_cleaning_removes_noise() -> None:
    frame = pd.DataFrame(
        [
            {"company_name": "logo", "company_tier": "대기업"},
            {"company_name": "김민수", "company_tier": "스타트업"},
            {"company_name": "당근", "company_tier": "스타트업"},
            {"company_name": "채용공고 목록", "company_tier": "대기업"},
        ]
    )
    cleaned, rejected = clean_non_company_entities(frame)
    assert cleaned["company_name"].tolist() == ["당근"]
    assert len(rejected) == 3


def test_official_domain_resolution_uses_manual_seed(sandbox_project: Path) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    companies, _ = discover_companies(paths)
    samsung = companies.loc[companies["company_name"] == "삼성전자"].iloc[0]
    assert samsung["official_domain"] == "samsungcareers.com"
    assert samsung["official_domain_confidence"] == pytest.approx(0.99)


def test_company_candidate_provenance_is_derived_from_manual_sources(sandbox_project: Path) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    companies, _ = discover_companies(paths)
    daangn = companies.loc[companies["company_name"] == "당근"].iloc[0]
    assert daangn["candidate_seed_type"] == "공식채용소스시드"
    assert daangn["candidate_seed_url"] == "https://career.daangn.com/jobs/data-ai.html"
    assert "공식 공개 채용 소스 등록" in daangn["candidate_seed_reason"]


def test_discover_companies_collapses_alias_variants_to_canonical_name(sandbox_project: Path) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    companies, _ = discover_companies(paths)
    names = companies["company_name"].tolist()
    assert "NAVER" not in names
    assert names.count("네이버") == 1


def test_source_screening_uses_approved_and_candidate_buckets(sandbox_project: Path) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    companies, _ = discover_companies(paths)
    candidates = discover_source_candidates(companies, paths)
    approved, candidate, rejected, _ = screen_sources(candidates)
    assert "https://recruit.navercorp.com/rcrt/list.do" in approved["source_url"].tolist()
    assert "https://careers.samsung.com/job-feed/data-ai.json" in candidate["source_url"].tolist()
    assert "https://careers.microsoft.com/v2/global/ko-kr/jobs-korea.json" in approved["source_url"].tolist()
    assert rejected.empty


def test_source_screening_treats_live_ats_source_types_as_structured_even_with_custom_hints() -> None:
    candidates = pd.DataFrame(
        [
            {
                "company_name": "카카오모빌리티",
                "company_tier": "대기업",
                "source_name": "카카오모빌리티 GreetingHR 공개 채용",
                "source_url": "https://kakaomobility.career.greetinghr.com",
                "source_type": "greetinghr",
                "source_bucket": "candidate",
                "official_domain": "kakaomobility.com",
                "official_domain_confidence": 0.99,
                "is_official_hint": True,
                "structure_hint": "next_data",
            },
            {
                "company_name": "한국뇌연구원 AI 실증지원사업단",
                "company_tier": "공공·연구기관",
                "source_name": "한국뇌연구원 AI 실증지원사업단 Recruiter 공개 채용",
                "source_url": "https://kbri-ai.recruiter.co.kr",
                "source_type": "recruiter",
                "source_bucket": "candidate",
                "official_domain": "kbri.re.kr",
                "official_domain_confidence": 0.99,
                "is_official_hint": True,
                "structure_hint": "recruiter",
            },
        ]
    )
    for column in SOURCE_REGISTRY_COLUMNS:
        if column not in candidates.columns:
            candidates[column] = ""

    approved, candidate, rejected, registry = screen_sources(candidates[list(SOURCE_REGISTRY_COLUMNS)])

    assert candidate.empty
    assert rejected.empty
    assert set(approved["source_url"]) == {
        "https://kakaomobility.career.greetinghr.com",
        "https://kbri-ai.recruiter.co.kr",
    }
    assert registry["source_quality_score"].min() >= 0.75


def test_source_screening_demotes_official_html_page_without_hiring_signal_to_candidate() -> None:
    candidates = pd.DataFrame(
        [
            {
                "company_name": "대한무역투자진흥공사",
                "company_tier": "공공·연구기관",
                "source_name": "대한무역투자진흥공사 공식 채용",
                "source_url": "https://kotra.or.kr/subList/20000005958",
                "source_type": "html_page",
                "source_bucket": "candidate",
                "official_domain": "kotra.or.kr",
                "official_domain_confidence": 0.99,
                "is_official_hint": True,
                "structure_hint": "html",
                "last_active_job_count": 0,
            }
        ]
    )
    for column in SOURCE_REGISTRY_COLUMNS:
        if column not in candidates.columns:
            candidates[column] = ""

    approved, candidate, rejected, _ = screen_sources(candidates[list(SOURCE_REGISTRY_COLUMNS)])

    assert approved.empty
    assert rejected.empty
    assert candidate["source_url"].tolist() == ["https://kotra.or.kr/subList/20000005958"]


def test_source_screening_keeps_active_official_html_page_approved_without_path_hints() -> None:
    candidates = pd.DataFrame(
        [
            {
                "company_name": "대한무역투자진흥공사",
                "company_tier": "공공·연구기관",
                "source_name": "대한무역투자진흥공사 공식 채용",
                "source_url": "https://kotra.or.kr/subList/20000005958",
                "source_type": "html_page",
                "source_bucket": "candidate",
                "official_domain": "kotra.or.kr",
                "official_domain_confidence": 0.99,
                "is_official_hint": True,
                "structure_hint": "html",
                "last_active_job_count": 3,
            }
        ]
    )
    for column in SOURCE_REGISTRY_COLUMNS:
        if column not in candidates.columns:
            candidates[column] = ""

    approved, candidate, rejected, _ = screen_sources(candidates[list(SOURCE_REGISTRY_COLUMNS)])

    assert candidate.empty
    assert rejected.empty
    assert approved["source_url"].tolist() == ["https://kotra.or.kr/subList/20000005958"]


def test_load_partial_company_scan_state_rescreens_existing_registry(sandbox_project: Path) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    write_csv(
        pd.DataFrame(
            [
                {
                    "company_name": "카카오모빌리티",
                    "company_tier": "대기업",
                    "source_name": "카카오모빌리티 GreetingHR 공개 채용",
                    "source_url": "https://kakaomobility.career.greetinghr.com",
                    "source_type": "greetinghr",
                    "source_bucket": "candidate",
                    "official_domain": "kakaomobility.com",
                    "official_domain_confidence": 0.99,
                    "is_official_hint": True,
                    "structure_hint": "next_data",
                    "source_quality_score": 0.7,
                    "verification_status": "성공",
                    "last_active_job_count": 7,
                }
            ],
            columns=list(SOURCE_REGISTRY_COLUMNS),
        ),
        paths.runtime_dir / "source_registry_in_progress.csv",
    )

    state = pipelines_module._load_partial_company_scan_state(paths)
    registry = state["registry"]

    assert len(registry) == 1
    assert registry.iloc[0]["source_bucket"] == "approved"
    assert registry.iloc[0]["source_quality_score"] >= 0.75


def test_discover_source_candidates_probes_official_homepage_links(monkeypatch: pytest.MonkeyPatch, sandbox_project: Path) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    companies = pd.DataFrame(
        [
            {
                "company_name": "알파소프트",
                "company_tier": "중견/중소",
                "official_domain": "alpha.example.com",
                "company_name_en": "",
                "region": "서울특별시",
                "aliases": [],
                "discovery_method": "company_seed_source",
                "candidate_seed_type": "상장법인목록",
                "candidate_seed_url": "https://kind.krx.co.kr/corpgeneral/corpList.do?method=loadInitPage",
                "candidate_seed_title": "KIND 상장법인목록",
                "candidate_seed_reason": "디지털 후보",
            }
        ]
    )

    class Settings:
        user_agent = "jobs-market-v2-test"
        timeout_seconds = 5.0
        enable_fallback_source_guess = False

    homepage_html = """
    <html><body>
      <a href="/careers">채용</a>
      <a href="https://jobs.lever.co/alphasoft">Careers</a>
      <a href="/about">회사소개</a>
    </body></html>
    """

    def fake_fetch(url: str, settings: object):
        assert url == "https://alpha.example.com"
        return homepage_html, url

    monkeypatch.setattr(discovery_module, "_fetch_company_homepage", fake_fetch)

    source_candidates = discover_source_candidates(companies, paths, settings=Settings())
    urls = set(source_candidates["source_url"].tolist())
    assert "https://alpha.example.com/careers" in urls
    assert "https://jobs.lever.co/alphasoft" in urls
    lever_row = source_candidates.loc[source_candidates["source_url"] == "https://jobs.lever.co/alphasoft"].iloc[0]
    assert lever_row["source_type"] == "lever"
    same_domain_row = source_candidates.loc[source_candidates["source_url"] == "https://alpha.example.com/careers"].iloc[0]
    assert same_domain_row["source_type"] == "html_page"


def test_discover_source_candidates_skips_root_and_notice_like_homepage_links(monkeypatch: pytest.MonkeyPatch, sandbox_project: Path) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    companies = pd.DataFrame(
        [
            {
                "company_name": "알파소프트",
                "company_tier": "중견/중소",
                "official_domain": "alpha.example.com",
                "company_name_en": "",
                "region": "서울특별시",
                "aliases": [],
                "discovery_method": "company_seed_source",
                "candidate_seed_type": "상장법인목록",
                "candidate_seed_url": "https://kind.krx.co.kr/corpgeneral/corpList.do?method=loadInitPage",
                "candidate_seed_title": "KIND 상장법인목록",
                "candidate_seed_reason": "디지털 후보",
            }
        ]
    )

    class Settings:
        user_agent = "jobs-market-v2-test"
        timeout_seconds = 5.0
        enable_fallback_source_guess = False

    homepage_html = """
    <html><body>
      <a href="https://alpha.example.com">채용</a>
      <a href="/notice/recruit">채용 공지</a>
      <a href="https://careers.alpha.example.com">Careers</a>
    </body></html>
    """

    def fake_fetch(url: str, settings: object):
        assert url == "https://alpha.example.com"
        return homepage_html, url

    monkeypatch.setattr(discovery_module, "_fetch_company_homepage", fake_fetch)

    source_candidates = discover_source_candidates(companies, paths, settings=Settings())
    urls = set(source_candidates["source_url"].tolist())
    assert "https://careers.alpha.example.com" in urls
    assert "https://alpha.example.com" not in urls
    assert "https://alpha.example.com/notice/recruit" not in urls


def test_discover_source_candidates_keeps_employment_announcement_page(monkeypatch: pytest.MonkeyPatch, sandbox_project: Path) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    companies = pd.DataFrame(
        [
            {
                "company_name": "알파연구원",
                "company_tier": "공공·연구기관",
                "official_domain": "alpha.re.kr",
                "company_name_en": "",
                "region": "대전",
                "aliases": [],
                "discovery_method": "company_seed_source",
                "candidate_seed_type": "출연연기관목록",
                "candidate_seed_url": "https://www.nst.re.kr/www/index.do",
                "candidate_seed_title": "NST 소관 연구기관",
                "candidate_seed_reason": "연구기관 후보",
            }
        ]
    )

    class Settings:
        user_agent = "jobs-market-v2-test"
        timeout_seconds = 5.0
        enable_fallback_source_guess = False

    homepage_html = """
    <html><body>
      <a href="/ko/notice/employment-announcement.do">공개채용 안내</a>
      <a href="/ko/news/notice.do">일반 공지</a>
    </body></html>
    """

    def fake_fetch(url: str, settings: object):
        assert url == "https://alpha.re.kr"
        return homepage_html, url

    monkeypatch.setattr(discovery_module, "_fetch_company_homepage", fake_fetch)

    source_candidates = discover_source_candidates(companies, paths, settings=Settings())
    urls = set(source_candidates["source_url"].tolist())
    assert "https://alpha.re.kr/ko/notice/employment-announcement.do" in urls
    assert "https://alpha.re.kr/ko/news/notice.do" not in urls


def test_discover_source_candidates_skips_policy_and_login_pages_on_direct_career_domain(monkeypatch: pytest.MonkeyPatch, sandbox_project: Path) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    companies = pd.DataFrame(
        [
            {
                "company_name": "알파소프트",
                "company_tier": "중견/중소",
                "official_domain": "recruit.alpha.example.com",
                "company_name_en": "",
                "region": "서울특별시",
                "aliases": [],
                "discovery_method": "company_seed_source",
                "candidate_seed_type": "상장법인목록",
                "candidate_seed_url": "https://kind.krx.co.kr/corpgeneral/corpList.do?method=loadInitPage",
                "candidate_seed_title": "KIND 상장법인목록",
                "candidate_seed_reason": "디지털 후보",
            }
        ]
    )

    class Settings:
        user_agent = "jobs-market-v2-test"
        timeout_seconds = 5.0
        enable_fallback_source_guess = False

    homepage_html = """
    <html><body>
      <a href="/rcrt/list.do">채용공고</a>
      <a href="/mem/login.do">로그인</a>
      <a href="/policy/privacy">개인정보처리방침</a>
      <a href="/cnts/people">People</a>
    </body></html>
    """

    def fake_fetch(url: str, settings: object):
        assert url == "https://recruit.alpha.example.com"
        return homepage_html, url

    monkeypatch.setattr(discovery_module, "_fetch_company_homepage", fake_fetch)

    source_candidates = discover_source_candidates(companies, paths, settings=Settings())
    urls = set(source_candidates["source_url"].tolist())
    assert "https://recruit.alpha.example.com/rcrt/list.do" in urls
    assert "https://recruit.alpha.example.com/mem/login.do" not in urls
    assert "https://recruit.alpha.example.com/policy/privacy" not in urls
    assert "https://recruit.alpha.example.com/cnts/people" not in urls


def test_discover_source_candidates_uses_direct_career_domain_as_source(monkeypatch: pytest.MonkeyPatch, sandbox_project: Path) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    companies = pd.DataFrame(
        [
            {
                "company_name": "알파소프트",
                "company_tier": "중견/중소",
                "official_domain": "careers.alpha.example.com",
                "company_name_en": "",
                "region": "서울특별시",
                "aliases": [],
                "discovery_method": "company_seed_source",
                "candidate_seed_type": "상장법인목록",
                "candidate_seed_url": "https://kind.krx.co.kr/corpgeneral/corpList.do?method=loadInitPage",
                "candidate_seed_title": "KIND 상장법인목록",
                "candidate_seed_reason": "디지털 후보",
            }
        ]
    )

    class Settings:
        user_agent = "jobs-market-v2-test"
        timeout_seconds = 5.0
        enable_fallback_source_guess = False

    def fake_fetch(url: str, settings: object):
        assert url == "https://careers.alpha.example.com"
        return "<html><body><p>채용 메인</p></body></html>", url

    monkeypatch.setattr(discovery_module, "_fetch_company_homepage", fake_fetch)

    source_candidates = discover_source_candidates(companies, paths, settings=Settings())
    row = source_candidates.loc[source_candidates["source_url"] == "https://careers.alpha.example.com"].iloc[0]
    assert row["source_type"] == "html_page"
    assert row["discovery_method"] == "official_domain_probe"


def test_discover_source_candidates_trims_noisy_same_domain_pages_on_direct_career_domain(
    monkeypatch: pytest.MonkeyPatch, sandbox_project: Path
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    companies = pd.DataFrame(
        [
            {
                "company_name": "알파소프트",
                "company_tier": "중견/중소",
                "official_domain": "recruit.alpha.example.com",
                "company_name_en": "",
                "region": "서울특별시",
                "aliases": [],
                "discovery_method": "company_seed_source",
                "candidate_seed_type": "상장법인목록",
                "candidate_seed_url": "https://kind.krx.co.kr/corpgeneral/corpList.do?method=loadInitPage",
                "candidate_seed_title": "KIND 상장법인목록",
                "candidate_seed_reason": "디지털 후보",
            }
        ]
    )

    class Settings:
        user_agent = "jobs-market-v2-test"
        timeout_seconds = 5.0
        enable_fallback_source_guess = False

    homepage_html = """
    <html><body>
      <a href="/rcrt/list.do">채용공고</a>
      <a href="/main.do">채용 메인</a>
      <a href="/index.jsp">채용 홈</a>
      <a href="/cnts/service">Service</a>
    </body></html>
    """

    def fake_fetch(url: str, settings: object):
        assert url == "https://recruit.alpha.example.com"
        return homepage_html, url

    monkeypatch.setattr(discovery_module, "_fetch_company_homepage", fake_fetch)

    source_candidates = discover_source_candidates(companies, paths, settings=Settings())
    urls = set(source_candidates["source_url"].tolist())
    assert "https://recruit.alpha.example.com" in urls
    assert "https://recruit.alpha.example.com/rcrt/list.do" in urls
    assert "https://recruit.alpha.example.com/main.do" not in urls
    assert "https://recruit.alpha.example.com/index.jsp" not in urls
    assert "https://recruit.alpha.example.com/cnts/service" not in urls


def test_discover_source_candidates_prefers_strong_hiring_pages_over_generic_pages(
    monkeypatch: pytest.MonkeyPatch, sandbox_project: Path
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    companies = pd.DataFrame(
        [
            {
                "company_name": "알파연구원",
                "company_tier": "공공·연구기관",
                "official_domain": "alpha.re.kr",
                "company_name_en": "",
                "region": "대전",
                "aliases": [],
                "discovery_method": "company_seed_source",
                "candidate_seed_type": "출연연기관목록",
                "candidate_seed_url": "https://www.nst.re.kr/www/index.do",
                "candidate_seed_title": "NST 소관 연구기관",
                "candidate_seed_reason": "연구기관 후보",
            }
        ]
    )

    class Settings:
        user_agent = "jobs-market-v2-test"
        timeout_seconds = 5.0
        enable_fallback_source_guess = False

    homepage_html = """
    <html><body>
      <a href="/ko/notice/employment-announcement.do">채용공고</a>
      <a href="/ko/intro/footsteps.do">채용제도 소개</a>
      <a href="/ko/participate/stop-job-request-center.do">채용 요청센터</a>
      <a href="/ko/news/latest-research-results.do">채용 관련 연구소식</a>
    </body></html>
    """

    def fake_fetch(url: str, settings: object):
        assert url == "https://alpha.re.kr"
        return homepage_html, url

    monkeypatch.setattr(discovery_module, "_fetch_company_homepage", fake_fetch)

    source_candidates = discover_source_candidates(companies, paths, settings=Settings())
    urls = set(source_candidates["source_url"].tolist())
    assert "https://alpha.re.kr/ko/notice/employment-announcement.do" in urls
    assert "https://alpha.re.kr/ko/intro/footsteps.do" not in urls
    assert "https://alpha.re.kr/ko/participate/stop-job-request-center.do" not in urls
    assert "https://alpha.re.kr/ko/news/latest-research-results.do" not in urls


def test_manual_import_supports_csv_and_yaml(sandbox_project: Path, tmp_path: Path) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    company_csv = tmp_path / "companies.csv"
    company_csv.write_text(
        "company_name,company_tier,official_domain,company_name_en,region,aliases,discovery_method\n테스트기업,스타트업,test.example,Test,수도권,,manual\n",
        encoding="utf-8",
    )
    source_yaml = tmp_path / "sources.yaml"
    source_yaml.write_text(
        """
sources:
  - company_name: 테스트기업
    company_tier: 스타트업
    source_name: 테스트기업 채용
    source_url: https://test.example/careers
    source_type: html_page
    official_domain: test.example
    is_official_hint: true
    structure_hint: html
    discovery_method: manual
""",
        encoding="utf-8",
    )
    company_summary = import_companies(paths, company_csv)
    source_summary = import_sources(paths, source_yaml)
    assert company_summary["stored_company_count"] >= 1
    assert source_summary["stored_source_count"] >= 1


def test_job_role_classification() -> None:
    assert classify_job_role("Data Analyst") == "데이터 분석가"
    assert classify_job_role("데이터 사이언티스트") == "데이터 사이언티스트"
    assert classify_job_role("데이터분석(데이터사이언스) 인재모집") == "데이터 사이언티스트"
    assert classify_job_role("AI서비스개발 직무 인재모집") == "인공지능 엔지니어"
    assert classify_job_role("[AI시험인증2팀] AI 검증 및 개발 (신입/경력)") == "인공지능 엔지니어"
    assert classify_job_role("AI Researcher") == "인공지능 리서처"
    assert classify_job_role("머신러닝 엔지니어") == "인공지능 엔지니어"
    assert classify_job_role("Software Engineer, Machine Learning") == "인공지능 엔지니어"
    assert classify_job_role("Machine Learning Software Engineer") == "인공지능 엔지니어"
    assert classify_job_role("Senior Machine Learning Platform Engineer") == "인공지능 엔지니어"
    assert classify_job_role("Sr. Software Engineer, Machine Learning Infrastructure") == "인공지능 엔지니어"
    assert classify_job_role(
        "System Software Engineer 신입/경력 채용",
        "",
        "AI 반도체 칩 개발 및 양산 프로세스 전반에 대한 이해",
    ) == "인공지능 엔지니어"
    assert classify_job_role("[AI Research Div.] Postdoctoral Researcher - LLMs (계약직)") == "인공지능 리서처"
    assert classify_job_role("[AI Transformation Dept.] AI FDE(Forward Deployed Engineer) 집중채용") == "인공지능 엔지니어"
    assert classify_job_role("Director - Machine Learning & Computer Vision") == "인공지능 엔지니어"
    assert classify_job_role("ML Ops Engineer") == "인공지능 엔지니어"
    assert classify_job_role("ML/MLOps 엔지니어") == "인공지능 엔지니어"
    assert classify_job_role("음성인식 엔진/모델 개발자") == "인공지능 엔지니어"
    assert classify_job_role("[AI실증지원사업단] [A-21](연구직) 2025년 제4차 사업단 직원(계약직) 채용") == "인공지능 리서처"
    assert classify_job_role("[HDC랩스/본사] AI LAB팀 AI 엔지니어 및 데이터 사이언스 부문 경력직 채용") in {"데이터 사이언티스트", "인공지능 엔지니어"}
    assert classify_job_role("Manager, QA Engineering (Eats Customer)") == ""
    assert classify_job_role("Director of Product Management (Growth Marketing - ML Product)") == ""
    assert classify_job_role("Staff Backend Software Engineer", "machine learning platform") == ""
    assert classify_job_role("Security Engineer | 인프라 (보안, AI Security)") == ""
    assert classify_job_role("Software Engineer, Backend | ML Data Platform") == ""
    assert classify_job_role("AI Agent 프론트엔드 개발자") == ""
    assert classify_job_role("전문연구요원 및 산업기능요원(보충역)", "", "현재 지원 가능한 직무는 아래와 같습니다.") == ""
    assert classify_job_role("Senior Software Engineer- System", "", "CI/CD 운영과 시스템 소프트웨어를 담당합니다.", "GPU") == ""
    assert classify_job_role("SoC Design Verification Engineer", "", "CPU UVM 테스트벤치와 SoC 검증을 담당합니다.") == ""
    assert classify_job_role("생체신호 FE Junior 개발자", "", "의료 소프트웨어 제품의 프론트엔드 기능을 개발합니다.") == ""
    assert classify_job_role("[Upstage] AI 교육 전문 강사 및 멘토풀 모집", "", "AI 기술 지식과 교육 역량을 보유한 강사를 모집합니다.") == ""
    assert classify_job_role("Software Engineer - Launching Platform", "", "클라우드 마켓플레이스향 제품 패키징과 내부 운영 도구를 개발합니다.") == ""
    assert classify_job_role("Process Innovation(PI)(대리~팀장)", "", "전사 프로세스 진단 및 표준 운영 절차를 수립합니다.") == ""
    assert classify_job_role("IT(AX)전략 담당자 대리~본부장급(팀 세팅 중)", "", "고객사 AX 전략 방향성과 시스템 현황 분석을 담당합니다.") == ""
    assert classify_job_role("AX팀 기술 리더 (AX Tech Leader)", "", "시스템 아키텍처와 개발 표준을 수립하고 팀을 리딩합니다.") == ""
    assert classify_job_role("[인터엑스] 2026년 전문연구요원 대규모 채용", "", "제조 특화 언어모델 훈련과 연구 직무를 포함한 대규모 채용입니다.") == ""
    assert classify_job_role("[우주항공국방기술실(판교)] SW검증 (경력)", "", "우주항공 방산 분야 소프트웨어 검증과 신뢰성 시험을 수행합니다.") == ""
    assert classify_job_role("Advanced Research | 플래닛 대전 [신입/경력] Firmware Evaluation Engineer", "", "펌웨어 코드 평가와 제어 루프 검증을 수행합니다.") == ""
    assert (
        classify_job_role(
            "[AI Research Div.] [전문연구요원] Research Scientist/Engineer - Vision-Language-Action (VLA) for Robotics (2년 이상)",
            "",
            "VLA 모델과 강화학습, 로봇 학습 알고리즘 연구개발을 수행하고 학회 논문 수준의 연구를 진행합니다.",
        )
        == "인공지능 리서처"
    )


def test_refresh_job_roles_reapplies_current_taxonomy() -> None:
    def job_row(company: str, title: str, role: str, main_tasks: str, job_key: str) -> dict:
        row = {column: "" for column in JOB_COLUMNS}
        row.update(
            {
                "job_key": job_key,
                "change_hash": f"hash-{job_key}",
                "first_seen_at": "2026-04-12T00:00:00+09:00",
                "last_seen_at": "2026-04-12T00:00:00+09:00",
                "is_active": True,
                "missing_count": 0,
                "snapshot_date": "2026-04-12",
                "run_id": "test-run",
                "source_url": f"https://example.com/{job_key}",
                "source_name": "Example Careers",
                "company_name": company,
                "company_tier": "스타트업",
                "job_title_raw": title,
                "experience_level_raw": "경력",
                "job_role": role,
                "job_url": f"https://example.com/jobs/{job_key}",
                "record_status": "신규",
                "주요업무_분석용": main_tasks,
            }
        )
        return row

    frame = pd.DataFrame(
        [
            job_row("리벨리온", "SoC Design Verification Engineer", "인공지능 엔지니어", "CPU UVM 테스트벤치와 SoC 검증을 담당합니다.", "soc"),
            job_row(
                "크래프톤",
                "[AI Research Div.] [전문연구요원] Research Scientist/Engineer - Vision-Language-Action (VLA) for Robotics (2년 이상)",
                "인공지능 엔지니어",
                "VLA 모델과 강화학습, 로봇 학습 알고리즘 연구개발을 수행하고 학회 논문 수준의 연구를 진행합니다.",
                "vla",
            ),
            job_row("테스트AI", "Machine Learning Engineer", "인공지능 엔지니어", "추천 모델 학습과 서빙 파이프라인을 개발합니다.", "ml"),
        ],
        columns=list(JOB_COLUMNS),
    )

    refreshed = refresh_job_roles(frame)

    assert "SoC Design Verification Engineer" not in set(refreshed["job_title_raw"])
    assert refreshed.loc[refreshed["job_key"] == "vla", "job_role"].item() == "인공지능 리서처"
    assert refreshed.loc[refreshed["job_key"] == "ml", "job_role"].item() == "인공지능 엔지니어"


def test_html_cleaning_and_section_extraction() -> None:
    html = """
    <div>
      <h2>주요업무</h2><ul><li>모델 개발</li></ul>
      <h2>자격요건</h2><ul><li>파이썬 경험</li></ul>
    </div>
    """
    cleaned = clean_html_text("<p>테스트 <b>문장</b></p>")
    sections = extract_sections_from_description(html)
    assert cleaned == "테스트\n문장" or cleaned == "테스트 문장"
    assert "모델 개발" in sections["주요업무"]
    assert "파이썬 경험" in sections["자격요건"]


def test_parse_html_jobs_handles_anchor_listing_cards_with_onclick_detail() -> None:
    html = """
    <html><body>
      <a class="card_link" href="/rcrt/list.do#n" onclick="show('30004704')">
        [네이버랩스] Data Platform Frontend Engineer (인턴십)
        모집 부서 Tech
        모집 분야 Frontend
        모집 경력 신입
        모집 기간 2026.03.16 ~ 2026.03.30
      </a>
      <a class="card_link" href="/rcrt/list.do#n" onclick="show('30004698')">
        [네이버랩스] Computer Vision & 3D Recon. Research Engineer
        모집 부서 Tech
        모집 분야 AI/ML
        모집 경력 경력
        모집 기간 2026.03.12 ~ 2026.03.30
      </a>
    </body></html>
    """
    jobs = parse_jobs_from_payload(
        html,
        "text/html",
        "html_page",
        base_url="https://recruit.navercorp.com/rcrt/list.do",
    )
    assert len(jobs) == 2
    assert jobs[0]["job_url"] == "https://recruit.navercorp.com/rcrt/view.do?annoId=30004704"
    assert "Data Platform Frontend Engineer" in jobs[0]["title"]
    assert jobs[1]["job_url"] == "https://recruit.navercorp.com/rcrt/view.do?annoId=30004698"
    assert "Research Engineer" in jobs[1]["title"]


def test_parse_html_jobs_uses_gemini_fallback_when_heuristics_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    html = """
    <html><body>
      <h1>채용공고</h1>
      <a href="/careers/posting/123">상세보기</a>
    </body></html>
    """

    monkeypatch.setattr(
        collection_module,
        "_extract_html_jobs_with_gemini",
        lambda *args, **kwargs: [
            {
                "title": "Machine Learning Engineer",
                "description_html": "<div>Machine Learning Engineer</div>",
                "job_url": "https://example.com/careers/posting/123",
            }
        ],
    )

    settings = AppSettings(enable_gemini_fallback=True, gemini_api_key="test-key", gemini_model="gemini-2.5-flash")
    jobs = parse_jobs_from_payload(
        html,
        "text/html",
        "html_page",
        base_url="https://example.com/careers",
        settings=settings,
        paths=ProjectPaths.from_root(Path(__file__).resolve().parents[1]),
        gemini_budget=collection_module.GeminiBudget(max_calls=2),
    )

    assert len(jobs) == 1
    assert jobs[0]["job_url"] == "https://example.com/careers/posting/123"
    assert jobs[0]["title"] == "Machine Learning Engineer"


def test_parse_html_jobs_follows_generic_hiring_redirect_page(monkeypatch: pytest.MonkeyPatch) -> None:
    first_html = """
    <html><body>
      <h1>입찰공고</h1>
      <a href="/careers">채용 공고</a>
    </body></html>
    """
    second_html = """
    <html><body>
      <h1>Careers</h1>
      <a href="/careers/posting/123">Machine Learning Engineer</a>
    </body></html>
    """

    def fake_gemini_extract(content: str, base_url: str, **kwargs):
        if "입찰공고" in content:
            return [{"title": "채용 공고", "description_html": "<div>채용 공고</div>", "job_url": "https://example.com/careers"}]
        if "Careers" in content:
            return [
                {
                    "title": "Machine Learning Engineer",
                    "description_html": "<div>Machine Learning Engineer</div>",
                    "job_url": "https://example.com/careers/posting/123",
                }
            ]
        return []

    def fake_fetch_source_content(url: str, paths, settings, source_type: str = "html_page", **kwargs):
        assert source_type == "html_page"
        if url == "https://example.com/careers":
            return second_html, "text/html"
        raise AssertionError(f"unexpected redirect url: {url}")

    monkeypatch.setattr(collection_module, "_extract_html_jobs_with_gemini", fake_gemini_extract)
    monkeypatch.setattr(collection_module, "fetch_source_content", fake_fetch_source_content)

    settings = AppSettings(enable_gemini_fallback=True, gemini_api_key="test-key", gemini_model="gemini-2.5-flash")
    jobs = parse_jobs_from_payload(
        first_html,
        "text/html",
        "html_page",
        base_url="https://example.com/bid",
        settings=settings,
        paths=ProjectPaths.from_root(Path(__file__).resolve().parents[1]),
        gemini_budget=collection_module.GeminiBudget(max_calls=3),
    )

    assert len(jobs) == 1
    assert jobs[0]["job_url"] == "https://example.com/careers/posting/123"
    assert jobs[0]["title"] == "Machine Learning Engineer"


def test_parse_html_jobs_prefers_single_detail_page_over_generic_nav_links() -> None:
    html = """
    <html><body>
      <h1>AI개발자_ML개발자</h1>
      <a href="/ko/company/career/job-openings">채용공고 바로가기</a>
      <a href="mailto:recruit@datamaker.io">recruit@datamaker.io</a>
      <div class="detail_box">
        <h3>이런 일을 해요</h3>
        <div class="detail_text"><ul><li>ML 모델을 개발하고 서비스에 적용합니다.</li></ul></div>
      </div>
      <div class="detail_box">
        <h3>이런 분을 원해요</h3>
        <div class="detail_text"><ul><li>Python과 PyTorch 활용 경험이 필요합니다.</li></ul></div>
      </div>
      <div class="detail_box">
        <h3>이런 분이면 더 좋아요</h3>
        <div class="detail_text"><ul><li>검색 또는 추천 시스템 경험 우대</li></ul></div>
      </div>
    </body></html>
    """

    jobs = parse_jobs_from_payload(
        html,
        "text/html",
        "html_page",
        base_url="https://datamaker.io/ko/company/career/job-openings/53",
    )

    assert len(jobs) == 1
    assert jobs[0]["job_url"] == "https://datamaker.io/ko/company/career/job-openings/53"
    assert jobs[0]["title"] == "AI개발자_ML개발자"
    assert "ML 모델을 개발하고 서비스에 적용합니다." in clean_html_text(jobs[0]["description_html"])


def test_parse_html_jobs_extracts_same_prefix_detail_links_from_list_like_page() -> None:
    html = """
    <html><body>
      <a href="/ko-kr/career/">채용</a>
      <a href="/ko-kr/career/recruit/">채용공고</a>
      <div class="job-list">
        <a href="/ko-kr/career/recruit/back_end_dev/">백엔드 개발자 (3년이상) 연구개발 경력 정규직</a>
        <a href="/ko-kr/career/recruit/front_end_dev/">프론트엔드 개발자 (3년 이상) 연구개발 경력 정규직</a>
        <a href="/ko-kr/career/recruit/page/2/">2</a>
      </div>
    </body></html>
    """

    jobs = parse_jobs_from_payload(
        html,
        "text/html",
        "html_page",
        base_url="https://www.rsupport.com/ko-kr/career/recruit/",
    )

    urls = {job["job_url"] for job in jobs}
    assert "https://www.rsupport.com/ko-kr/career/recruit/back_end_dev/" in urls
    assert "https://www.rsupport.com/ko-kr/career/recruit/front_end_dev/" in urls
    assert "https://www.rsupport.com/ko-kr/career/recruit/" not in urls
    assert "https://www.rsupport.com/ko-kr/career/recruit/page/2/" not in urls


def test_parse_html_jobs_extracts_same_prefix_detail_links_from_careers_cards() -> None:
    html = """
    <html><body>
      <a href="/careers">Careers</a>
      <div class="panel">
        <div class="panel-body">
          채용 중 [병역특례 전문연구요원] AI 기술 및 응용솔루션 R&D 엔지니어
          <a href="/careers/ai-rnd-engineer-special">자세히 보기</a>
        </div>
      </div>
      <div class="panel">
        <div class="panel-body">
          채용 중 웹 대시보드 백엔드 개발자
          <a href="/careers/web-backend-developer">자세히 보기</a>
        </div>
      </div>
    </body></html>
    """

    jobs = parse_jobs_from_payload(
        html,
        "text/html",
        "html_page",
        base_url="https://gazzi.ai/careers",
    )

    urls = {job["job_url"] for job in jobs}
    assert "https://gazzi.ai/careers/ai-rnd-engineer-special" in urls
    assert "https://gazzi.ai/careers/web-backend-developer" in urls
    assert "https://gazzi.ai/careers" not in urls


def test_parse_html_jobs_extracts_panel_heading_title_from_careers_detail_page() -> None:
    html = """
    <html><head><title>(주)가치랩스</title></head><body>
      <h2>Careers</h2>
      <div class="panel-heading">
        <h3>[병역특례 전문연구요원] AI 기술 및 응용솔루션 R&D 엔지니어 (1명)</h3>
      </div>
      <div class="title">담당업무</div>
      <div class="content">멀티모달 데이터 임베딩 및 도메인 특화 RAG 파이프라인 구축</div>
      <div class="title">자격요건</div>
      <div class="content">대학원(석사) 이상, Python 활용 가능</div>
      <div class="title">우대사항</div>
      <div class="content">LLM 연구 경험자 우대</div>
    </body></html>
    """

    jobs = parse_jobs_from_payload(
        html,
        "text/html",
        "html_page",
        base_url="https://gazzi.ai/careers/ai-rnd-engineer-special",
    )

    assert len(jobs) == 1
    assert jobs[0]["title"] == "[병역특례 전문연구요원] AI 기술 및 응용솔루션 R&D 엔지니어 (1명)"
    assert "RAG 파이프라인 구축" in clean_html_text(jobs[0]["description_html"])


def test_parse_html_jobs_prefers_gemini_when_anchor_jobs_are_only_generic_nav_stubs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    html = """
    <html><body>
      <h1>Careers</h1>
      <a href="/careers">Careers</a>
      <a href="/careers/jobs">Jobs</a>
    </body></html>
    """

    monkeypatch.setattr(
        collection_module,
        "_extract_html_jobs_with_gemini",
        lambda *args, **kwargs: [
            {
                "title": "Machine Learning Engineer",
                "description_html": "<div>Machine Learning Engineer</div>",
                "job_url": "https://example.com/careers/jobs/123",
            }
        ],
    )

    settings = AppSettings(enable_gemini_fallback=True, gemini_api_key="test-key", gemini_model="gemini-2.5-flash")
    jobs = parse_jobs_from_payload(
        html,
        "text/html",
        "html_page",
        base_url="https://example.com/careers/jobs",
        settings=settings,
        paths=ProjectPaths.from_root(Path(__file__).resolve().parents[1]),
        gemini_budget=collection_module.GeminiBudget(max_calls=2),
    )

    assert len(jobs) == 1
    assert jobs[0]["job_url"] == "https://example.com/careers/jobs/123"
    assert jobs[0]["title"] == "Machine Learning Engineer"


def test_hydrate_html_job_details_fetches_detail_page_for_weak_listing(monkeypatch: pytest.MonkeyPatch) -> None:
    detail_html = """
    <html><body>
      <div class="card_title">[네이버랩스] Generative AI Research Engineer</div>
      <dl class="card_info">
        <dt>모집 경력</dt><dd>경력</dd>
        <dt>근무지</dt><dd>경기도 성남시</dd>
      </dl>
      <div class="detail_box">
        <h4 class="detail_title">Key Responsibilities</h4>
        <div class="detail_text"><p>Generative AI 모델 연구 및 개발</p></div>
      </div>
      <div class="detail_box">
        <h4 class="detail_title">Preferred Skills</h4>
        <div class="detail_text"><p>석사 또는 박사 학위</p></div>
      </div>
    </body></html>
    """

    def fake_fetch_source_content(url: str, paths, settings, source_type: str = "html_page", **kwargs):
        assert url == "https://recruit.navercorp.com/rcrt/view.do?annoId=30004759"
        assert source_type == "html_page"
        return detail_html, "text/html"

    monkeypatch.setattr(collection_module, "fetch_source_content", fake_fetch_source_content)

    jobs = collection_module._hydrate_html_job_details(
        [
            {
                "title": "[네이버랩스] Generative AI Research Engineer",
                "description_html": (
                    "<div>[네이버랩스] Generative AI Research Engineer "
                    "모집 부서 NAVER LABS 모집 분야 AI/ML 모집 경력 경력 근로 조건 정규 모집 기간 2026.04.01 ~ 2026.04.13</div>"
                ),
                "job_url": "https://recruit.navercorp.com/rcrt/view.do?annoId=30004759",
            }
        ],
        {
            "source_type": "html_page",
            "source_url": "https://recruit.navercorp.com/rcrt/list.do",
        },
        settings=AppSettings(),
        paths=ProjectPaths.from_root(Path(__file__).resolve().parents[1]),
    )

    assert len(jobs) == 1
    assert jobs[0]["experience_level"] == "경력"
    assert jobs[0]["location"] == "경기도 성남시"
    assert "Key Responsibilities" in jobs[0]["description_html"]
    assert "Preferred Skills" in jobs[0]["description_html"]
    assert "석사 또는 박사 학위" in jobs[0]["description_html"]


def test_hydrate_html_job_details_uses_title_content_pairs_for_generic_detail_pages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    detail_html = """
    <html><body>
      <main class="recruit">
        <h2>Recruit</h2>
        <div class="detail">
          <div class="title">채용포지션</div>
          <div class="content">메디컬그룹 DS(Data Science)팀 연구원</div>
          <div class="title">필요 경력</div>
          <div class="content">신입지원가능</div>
          <div class="title">직무내용</div>
          <div class="content">심전도 기반 AI 데이터 분석 및 연구 전반을 주도합니다.</div>
          <div class="title">지원 자격</div>
          <div class="content">석사 이상 학위 보유자</div>
        </div>
      </main>
    </body></html>
    """

    def fake_fetch_source_content(url: str, paths, settings, source_type: str = "html_page", **kwargs):
        assert url == "https://medicalai.com/en/recruit?recruit_id=2"
        assert source_type == "html_page"
        return detail_html, "text/html"

    monkeypatch.setattr(collection_module, "fetch_source_content", fake_fetch_source_content)

    jobs = collection_module._hydrate_html_job_details(
        [
            {
                "title": "상시채용 Data Science MEDICAL GROUP",
                "description_html": "<div>모집중</div>",
                "job_url": "https://medicalai.com/en/recruit?recruit_id=2",
            }
        ],
        {
            "source_type": "html_page",
            "source_url": "https://medicalai.com/en/recruit",
        },
        settings=AppSettings(),
        paths=ProjectPaths.from_root(Path(__file__).resolve().parents[1]),
    )

    assert len(jobs) == 1
    assert jobs[0]["title"] == "메디컬그룹 DS(Data Science)팀 연구원"
    assert jobs[0]["experience_level"] == "신입지원가능"
    assert "직무내용" in jobs[0]["description_html"]
    assert "지원 자격" in jobs[0]["description_html"]
    assert "Recruit" not in jobs[0]["description_html"]


def test_section_extraction_handles_greenhouse_style_paragraph_headings() -> None:
    html = """
    <p><strong>Role</strong><strong> Overview</strong></p>
    <p>데이터 기반 의사결정을 지원합니다.</p>
    <p><strong>Key Responsibilities</strong></p>
    <ul>
      <li>지표 설계</li>
      <li>실험 분석</li>
    </ul>
    <p><strong>Qualifications</strong></p>
    <ul>
      <li>SQL 활용 경험</li>
    </ul>
    <p><strong>Preferred Qualifications</strong></p>
    <ul>
      <li>A/B 테스트 경험</li>
    </ul>
    """
    sections = extract_sections_from_description(html)
    assert "데이터 기반 의사결정을 지원합니다." in sections["주요업무"]
    assert "지표 설계" in sections["주요업무"]
    assert "실험 분석" in sections["주요업무"]
    assert "SQL 활용 경험" in sections["자격요건"]
    assert "A/B 테스트 경험" in sections["우대사항"]


def test_section_extraction_falls_back_to_line_based_split_headings() -> None:
    html = """
    <div>직무 소개</div>
    <div>데이터 기반 운영 의사결정을 지원합니다.</div>
    <div>업무 내용</div>
    <div>모델 구축</div>
    <div>지표 분석</div>
    <div>자격</div>
    <div>요건</div>
    <div>파이썬 경험</div>
    <div>에스큐엘 활용 경험</div>
    <div>핵심 기술 역량</div>
    <div>Python</div>
    <div>SQL</div>
    """
    sections = extract_sections_from_description(html)
    assert "데이터 기반 운영 의사결정을 지원합니다." in sections["주요업무"]
    assert "모델 구축" in sections["주요업무"]
    assert "지표 분석" in sections["주요업무"]
    assert "파이썬 경험" in sections["자격요건"]
    assert "에스큐엘 활용 경험" in sections["자격요건"]
    assert "Python" in sections["핵심기술"]


def test_section_extraction_stops_at_non_target_headings() -> None:
    html = """
    <div>우대사항</div>
    <div>파이썬 경험 우대</div>
    <div>전형 절차</div>
    <div>서류전형 - 면접 - 합격</div>
    """
    sections = extract_sections_from_description(html)
    assert "파이썬 경험 우대" in sections["우대사항"]
    assert "서류전형" not in sections["우대사항"]


def test_section_extraction_stops_at_extended_process_heading_sentence() -> None:
    html = """
    <div>이런 경험들이 있다면 저희가 찾는 그 분입니다! (우대요건)</div>
    <div>탑티어 학회 발표 경험</div>
    <div>크래프톤의 도전에 함께 하기 위해 아래의 전형 과정이 필요합니다.</div>
    <div>서류 전형 > 면접 > 합격</div>
    """
    sections = extract_sections_from_description(html)
    assert "탑티어 학회 발표 경험" in sections["우대사항"]
    assert "서류 전형" not in sections["우대사항"]


def test_section_extraction_matches_descriptive_heading_patterns() -> None:
    html = """
    <div>The Impact You’ll Be Contributing to Moloco:</div>
    <div>실험 설계와 모델 개선을 수행합니다.</div>
    <div>The Opportunity:</div>
    <div>대규모 데이터 분석과 시스템 개선을 담당합니다.</div>
    <div>How Do I Know if the Role is Right For Me?</div>
    <div>파이썬 및 통계 기반 연구 경험</div>
    <div>Technical Proficiencies</div>
    <div>Python</div>
    <div>SQL</div>
    """
    sections = extract_sections_from_description(html)
    assert "실험 설계와 모델 개선을 수행합니다." in sections["주요업무"]
    assert "대규모 데이터 분석과 시스템 개선을 담당합니다." in sections["주요업무"]
    assert "파이썬 및 통계 기반 연구 경험" in sections["자격요건"]
    assert "Python" in sections["핵심기술"]
    assert "SQL" in sections["핵심기술"]


def test_section_extraction_matches_korean_sentence_headings_with_emoji() -> None:
    html = """
    <div>어떤 업무를 담당하나요?🤔</div>
    <div>데이터 기반 실험과 모델 고도화를 담당합니다.</div>
    <div>이런 분과 함께 하고 싶어요🙌</div>
    <div>파이썬과 에스큐엘 활용 경험</div>
    <div>이런 경험이 있으면 더 좋아요✨</div>
    <div>검색증강생성 서비스 경험</div>
    """
    sections = extract_sections_from_description(html)
    assert "데이터 기반 실험과 모델 고도화를 담당합니다." in sections["주요업무"]
    assert "파이썬과 에스큐엘 활용 경험" in sections["자격요건"]
    assert "검색증강생성 서비스 경험" in sections["우대사항"]


def test_section_extraction_matches_daangn_and_sendbird_style_headings() -> None:
    html = """
    <h3>이런 일을 해요</h3>
    <ul><li>추천 모델을 개발하고 개선해요</li></ul>
    <h3>이런 분을 찾고 있어요</h3>
    <ul><li>머신러닝 모델링 경험</li></ul>
    <h3>You need to have:</h3>
    <ul><li>LLM 기반 기능을 운영한 경험</li></ul>
    """
    sections = extract_sections_from_description(html)
    assert "추천 모델을 개발하고 개선해요" in sections["주요업무"]
    assert "머신러닝 모델링 경험" in sections["자격요건"]
    assert "LLM 기반 기능을 운영한 경험" in sections["자격요건"]


def test_section_extraction_matches_krafton_style_mission_and_requirement_headings() -> None:
    html = """
    <div>우리 팀과 함께할 미션을 소개합니다.</div>
    <div>대규모 엘엘엠 학습 및 선행 연구를 진행합니다.</div>
    <div>이런 경험을 가진 분과 함께 성장하고 싶습니다! (필수요건)</div>
    <div>딥러닝 관련 연구 경력</div>
    <div>이런 경험들이 있다면 저희가 찾는 그 분입니다! (우대요건)</div>
    <div>탑티어 학회 발표 경험</div>
    """
    sections = extract_sections_from_description(html)
    assert "대규모 엘엘엠 학습 및 선행 연구를 진행합니다." in sections["주요업무"]
    assert "딥러닝 관련 연구 경력" in sections["자격요건"]
    assert "탑티어 학회 발표 경험" in sections["우대사항"]


def test_plain_sentence_with_technology_is_not_misclassified_as_skill_heading() -> None:
    html = """
    <div>With bold imagination and breakthrough technology, we create unforgettable worlds.</div>
    <div>우리 팀과 함께할 미션을 소개합니다.</div>
    <div>모델 연구를 수행합니다.</div>
    """
    sections = extract_sections_from_description(html)
    assert sections["핵심기술"] == ""
    assert "모델 연구를 수행합니다." in sections["주요업무"]


def test_section_extraction_matches_prefixed_job_intro_headings() -> None:
    html = """
    <div>[ML Research Scientist 직무 소개]</div>
    <div>제품 문제를 연구 문제로 재정의하고 실험합니다.</div>
    <div>You might be this person if:</div>
    <div>멀티모달 LLM 연구 경험</div>
    <div>참고해 주세요</div>
    <div>정규직 채용의 경우 수습기간이 있어요</div>
    """
    sections = extract_sections_from_description(html)
    assert "제품 문제를 연구 문제로 재정의하고 실험합니다." in sections["주요업무"]
    assert "멀티모달 LLM 연구 경험" in sections["자격요건"]
    assert "수습기간" not in sections["자격요건"]


def test_section_extraction_matches_korean_sentence_style_headings() -> None:
    html = """
    <blockquote><strong>합류하면 담당할 업무예요</strong></blockquote>
    <ul><li>추천 모델 실험을 설계합니다.</li></ul>
    <blockquote><strong>이런 동료를 기다립니다</strong></blockquote>
    <ul><li>SQL과 Python 기반 분석 경험이 있습니다.</li></ul>
    <blockquote><strong>이런 분이라면 더욱 좋습니다</strong></blockquote>
    <ul><li>A/B 테스트 설계 경험이 있습니다.</li></ul>
    """
    sections = extract_sections_from_description(html)
    assert "추천 모델 실험을 설계합니다." in sections["주요업무"]
    assert "SQL과 Python 기반 분석 경험이 있습니다." in sections["자격요건"]
    assert "A/B 테스트 설계 경험이 있습니다." in sections["우대사항"]


def test_clean_html_text_repairs_fragmented_character_lines() -> None:
    html = """
    <div><
    l
    i
    >
    다
    양
    한
    입
    력
    <
    /
    l
    i
    ></div>
    """
    cleaned = clean_html_text(html)
    assert "다양한입력" in cleaned
    assert "<" not in cleaned
    assert "l" not in cleaned


def test_greenhouse_and_lever_payload_parsing_support_live_ats() -> None:
    greenhouse_payload = json.dumps(
        {
            "jobs": [
                {
                    "title": "Applied Scientist II",
                    "absolute_url": "https://job-boards.greenhouse.io/moloco/jobs/1",
                    "location": {"name": "Seoul, Korea"},
                    "content": "&lt;h2&gt;Responsibilities&lt;/h2&gt;&lt;ul&gt;&lt;li&gt;Model research&lt;/li&gt;&lt;/ul&gt;",
                    "metadata": [{"name": "Job Level", "value": "Experienced"}],
                }
            ]
        },
        ensure_ascii=False,
    )
    lever_payload = json.dumps(
        [
            {
                "text": "Machine Learning Engineer",
                "hostedUrl": "https://jobs.lever.co/example/1",
                "description": "<h2>Requirements</h2><ul><li>Python</li></ul>",
                "categories": {"location": "Seoul", "commitment": "Full-Time"},
                "country": "KR",
            }
        ],
        ensure_ascii=False,
    )
    greenhouse_jobs = parse_jobs_from_payload(greenhouse_payload, "application/json", "greenhouse")
    lever_jobs = parse_jobs_from_payload(lever_payload, "application/json", "lever")
    assert greenhouse_jobs[0]["job_url"] == "https://job-boards.greenhouse.io/moloco/jobs/1"
    assert "Responsibilities" in greenhouse_jobs[0]["description_html"]
    assert lever_jobs[0]["job_url"] == "https://jobs.lever.co/example/1"
    assert "Requirements" in lever_jobs[0]["description_html"]


def test_source_timeout_values_prefer_source_specific_settings() -> None:
    class Settings:
        timeout_seconds = 20.0
        connect_timeout_seconds = 5.0
        html_source_timeout_seconds = 8.0
        html_source_connect_timeout_seconds = 3.0
        ats_source_timeout_seconds = 10.0
        ats_source_connect_timeout_seconds = 2.0

    assert _source_timeout_values(Settings(), "html_page") == (8.0, 3.0)
    assert _source_timeout_values(Settings(), "greetinghr") == (10.0, 2.0)
    assert _source_timeout_values(Settings(), "recruiter") == (10.0, 2.0)
    assert _source_timeout_values(Settings(), "greenhouse") == (10.0, 2.0)
    assert _source_timeout_values(Settings(), "unknown") == (20.0, 5.0)


def test_fetch_source_content_uses_source_specific_timeout(monkeypatch: pytest.MonkeyPatch, sandbox_project: Path) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    captured: list[tuple[str, float, float | None]] = []

    class Settings:
        use_mock_sources = False
        user_agent = "jobs-market-v2-test"
        timeout_seconds = 20.0
        connect_timeout_seconds = 5.0
        html_source_timeout_seconds = 7.0
        html_source_connect_timeout_seconds = 2.5
        ats_source_timeout_seconds = 9.0
        ats_source_connect_timeout_seconds = 2.0

    def fake_fetch(url: str, timeout_seconds: float, user_agent: str, *, connect_timeout_seconds: float | None = None):
        captured.append((url, timeout_seconds, connect_timeout_seconds))
        return "<html></html>", "text/html"

    monkeypatch.setattr(collection_module, "_fetch_remote", fake_fetch)

    collection_module.fetch_source_content("https://example.com/careers", paths, Settings(), "html_page")
    collection_module.fetch_source_content("https://boards-api.greenhouse.io/v1/boards/example/jobs", paths, Settings(), "greenhouse")

    assert captured == [
        ("https://example.com/careers", 7.0, 2.5),
        ("https://boards-api.greenhouse.io/v1/boards/example/jobs", 9.0, 2.0),
    ]


def test_fetch_source_content_routes_embedded_greetinghr_custom_domain(
    monkeypatch: pytest.MonkeyPatch,
    sandbox_project: Path,
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)

    class Settings:
        use_mock_sources = False
        user_agent = "jobs-market-v2-test"
        timeout_seconds = 20.0
        connect_timeout_seconds = 5.0
        html_source_timeout_seconds = 7.0
        html_source_connect_timeout_seconds = 2.5
        ats_source_timeout_seconds = 9.0
        ats_source_connect_timeout_seconds = 2.0
        gemini_html_listing_max_calls_per_run = 0

    board_payload = {
        "props": {
            "pageProps": {
                "dehydratedState": {
                    "queries": [
                        {
                            "queryKey": ["openings"],
                            "state": {
                                "data": [
                                    {
                                        "openingId": 135719,
                                        "deploy": True,
                                        "title": "[기술본부] Data Engineer (BI/DW)",
                                        "openingJobPosition": {"openingJobPositions": []},
                                    }
                                ]
                            },
                        }
                    ]
                }
            }
        }
    }
    custom_domain_html = (
        '<img src="https://profiles.greetinghr.com/group/example/logo.png"/>'
        '<script id="__NEXT_DATA__" type="application/json">'
        + json.dumps(board_payload, ensure_ascii=False)
        + "</script>"
    )

    def fake_fetch(url: str, timeout_seconds: float, user_agent: str, *, connect_timeout_seconds: float | None = None):
        return custom_domain_html, "text/html"

    def fake_fetch_greetinghr(url: str, paths, settings):
        return json.dumps({"jobs": [{"title": "[기술본부] Data Engineer (BI/DW)"}]}, ensure_ascii=False), "application/json"

    monkeypatch.setattr(collection_module, "_fetch_remote", fake_fetch)
    monkeypatch.setattr(collection_module, "_fetch_greetinghr_source", fake_fetch_greetinghr)

    content, content_type = collection_module.fetch_source_content(
        "https://careers.devsisters.com",
        paths,
        Settings(),
        "html_page",
    )

    assert content_type == "application/json"
    assert "Data Engineer" in content


def test_fetch_source_content_routes_embedded_ninehire_custom_domain(
    monkeypatch: pytest.MonkeyPatch,
    sandbox_project: Path,
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)

    class Settings:
        use_mock_sources = False
        user_agent = "jobs-market-v2-test"
        timeout_seconds = 20.0
        connect_timeout_seconds = 5.0
        html_source_timeout_seconds = 7.0
        html_source_connect_timeout_seconds = 2.5
        ats_source_timeout_seconds = 9.0
        ats_source_connect_timeout_seconds = 2.0
        gemini_html_listing_max_calls_per_run = 0

    next_payload = {
        "page": "/[menu]",
        "props": {
            "pageProps": {
                "homepageProps": {
                    "info": {"companyId": "company-123"},
                    "homepage": {
                        "companyId": "company-123",
                        "pages": [
                            {
                                "sections": [
                                    {
                                        "layouts": [
                                            {
                                                "columns": [
                                                    {
                                                        "blocks": [
                                                            {
                                                                "type": "job_posting",
                                                                "countPerPage": 25,
                                                                "orderBy": "created_at_desc",
                                                                "fixedRecruitmentIds": [],
                                                            }
                                                        ]
                                                    }
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            }
                        ],
                    },
                }
            }
        },
    }
    html = (
        '<div data-provider="ninehire"></div>'
        '<script id="__NEXT_DATA__" type="application/json">'
        + json.dumps(next_payload, ensure_ascii=False)
        + "</script>"
    )

    def fake_fetch(url: str, timeout_seconds: float, user_agent: str, *, connect_timeout_seconds: float | None = None):
        return html, "text/html"

    def fake_fetch_ninehire(url: str, paths, settings):
        return json.dumps({"jobs": [{"title": "Data Analyst"}]}, ensure_ascii=False), "application/json"

    monkeypatch.setattr(collection_module, "_fetch_remote", fake_fetch)
    monkeypatch.setattr(collection_module, "_fetch_ninehire_source", fake_fetch_ninehire)

    content, content_type = collection_module.fetch_source_content(
        "https://careers.example.com/openposition",
        paths,
        Settings(),
        "html_page",
    )

    assert content_type == "application/json"
    assert "Data Analyst" in content


def test_fetch_source_content_routes_bytesize_custom_api_with_ocr(
    monkeypatch: pytest.MonkeyPatch,
    sandbox_project: Path,
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)

    class Settings:
        use_mock_sources = False
        user_agent = "jobs-market-v2-test"
        timeout_seconds = 20.0
        connect_timeout_seconds = 5.0
        html_source_timeout_seconds = 7.0
        html_source_connect_timeout_seconds = 2.5
        ats_source_timeout_seconds = 9.0
        ats_source_connect_timeout_seconds = 2.0
        gemini_html_listing_max_calls_per_run = 0

    shell_html = """
    <!doctype html>
    <html lang="en">
      <head><title>ByteSize Careers</title></head>
      <body><div id="root"></div></body>
    </html>
    """.strip()
    list_payload = {
        "status": True,
        "result": [
            {
                "id": 27,
                "title": "LLM RAG 및 AI Agent 개발 (신입 가능)",
                "job": "개발",
                "career_type": 1,
                "min_career_year": 5,
                "max_career_year": None,
                "work_type": "정규직",
                "is_pool": False,
            },
            {
                "id": 28,
                "title": "웹 벡엔드 개발 및 서버 운영(10년 이상)",
                "job": "개발",
                "career_type": 1,
                "min_career_year": 10,
                "max_career_year": None,
                "work_type": "정규직",
                "is_pool": False,
            },
        ],
    }
    detail_payload = {
        "status": True,
        "result": {
            "id": 27,
            "title": "LLM RAG 및 AI Agent 개발 (신입 가능)",
            "job": "개발",
            "career_type": 1,
            "min_career_year": 5,
            "max_career_year": None,
            "work_type": "정규직",
            "is_pool": False,
            "images": ["https://cdn.example.com/bytesize-27.jpg"],
            "content": None,
        },
    }

    def fake_fetch(url: str, timeout_seconds: float, user_agent: str, *, connect_timeout_seconds: float | None = None):
        if url == "https://career.thebytesize.ai/jobs":
            return shell_html, "text/html"
        if url == "https://career-api.thebytesize.ai/career/api/v1/jobs":
            return json.dumps(list_payload, ensure_ascii=False), "application/json"
        if url == "https://career-api.thebytesize.ai/career/api/v1/jobs/27":
            return json.dumps(detail_payload, ensure_ascii=False), "application/json"
        raise AssertionError(f"unexpected url: {url}")

    monkeypatch.setattr(collection_module, "_fetch_remote", fake_fetch)
    monkeypatch.setattr(
        collection_module,
        "extract_text_from_asset_urls",
        lambda asset_urls, **_: "\n".join(
            [
                "LLM RAG 및 AI Agent 개발",
                "이런 업무를 해요",
                "주요 업무",
                "- 데이터 분석형 AI Agent 모듈 개발",
                "- LLM 기반 RAG 시스템 설계 및 구현",
                "이런분이 적합해요",
                "자격 요건",
                "- Python 프로그래밍 능숙자",
                "- 생성형 AI 및 RAG 방법론 이해",
                "이런 분이면 더욱 좋아요",
                "우대 사항",
                "- LangChain 활용 경험자",
            ]
        ),
    )

    content, content_type = collection_module.fetch_source_content(
        "https://career.thebytesize.ai/jobs",
        paths,
        Settings(),
        "html_page",
    )

    assert content_type == "application/json"
    jobs = parse_jobs_from_payload(
        content,
        content_type,
        "html_page",
        base_url="https://career.thebytesize.ai/jobs",
        settings=AppSettings(),
        paths=paths,
        gemini_budget=collection_module.GeminiBudget(max_calls=0),
    )
    assert len(jobs) == 1
    assert jobs[0]["job_url"] == "https://career.thebytesize.ai/jobs/application?id=27"

    source_row = {
        "source_url": "https://career.thebytesize.ai/jobs",
        "source_bucket": "",
        "source_name": "바이트사이즈 채용",
        "company_name": "바이트사이즈",
        "company_tier": "스타트업",
        "source_type": "html_page",
    }
    normalized, _ = normalize_job_payload(
        jobs[0],
        source_row,
        "test-run",
        "2026-04-02",
        "2026-04-02T23:59:59+09:00",
        settings=AppSettings(),
        paths=paths,
        gemini_budget=collection_module.GeminiBudget(max_calls=0),
        refine_with_gemini=False,
    )
    assert normalized["job_role"] == "인공지능 엔지니어"
    assert "엘엘엠 기반 검색증강생성 시스템 설계 및 구현" in normalized["주요업무_표시"]
    assert "이런분이 적합해요" not in normalized["주요업무_표시"]
    assert "파이썬 프로그래밍 능숙자" in normalized["자격요건_표시"]


def test_classify_job_role_accepts_llm_rag_ai_agent_developer() -> None:
    assert collection_module.classify_job_role("LLM RAG 및 AI Agent 개발 (신입 가능)") == "인공지능 엔지니어"


def test_greetinghr_board_and_detail_parsing_support_live_ats() -> None:
    board_payload = {
        "props": {
            "pageProps": {
                "dehydratedState": {
                    "queries": [
                        {
                            "queryKey": ["openings"],
                            "state": {
                                "data": [
                                    {
                                        "openingId": 197728,
                                        "deploy": True,
                                        "title": "데이터 사이언티스트",
                                        "openingJobPosition": {
                                            "openingJobPositions": [
                                                {
                                                    "workspacePlace": {"place": "경기도 성남시 분당구 판교역로 152"},
                                                    "jobPositionCareer": {"careerType": "EXPERIENCED", "careerFrom": 5},
                                                }
                                            ]
                                        },
                                    }
                                ]
                            },
                        }
                    ]
                }
            }
        }
    }
    board_html = (
        '<script id="__NEXT_DATA__" type="application/json">'
        + json.dumps(board_payload, ensure_ascii=False)
        + "</script>"
    )
    detail_payload = {
        "props": {
            "pageProps": {
                "dehydratedState": {
                    "queries": [
                        {
                            "queryKey": ["career", "getOpeningById", {"openingId": 197728, "workspaceId": 14346}],
                            "state": {
                                "data": {
                                    "data": {
                                        "openingsInfo": {
                                            "title": "데이터 사이언티스트",
                                            "detail": "<blockquote><strong>합류하게 되면 이런 일을 하게 됩니다</strong></blockquote><ul><li>강화학습 모델 개발</li></ul>",
                                        }
                                    }
                                }
                            },
                        }
                    ]
                }
            }
        }
    }
    detail_html = (
        '<script id="__NEXT_DATA__" type="application/json">'
        + json.dumps(detail_payload, ensure_ascii=False)
        + "</script>"
    )
    jobs = _build_greetinghr_jobs_from_html(
        board_html,
        "https://kakaomobility.career.greetinghr.com",
        lambda _: detail_html,
    )
    assert jobs[0]["job_url"] == "https://kakaomobility.career.greetinghr.com/ko/o/197728"
    assert jobs[0]["experience_level"] == "경력 5년 이상"
    assert "강화학습 모델 개발" in jobs[0]["description_html"]


def test_greetinghr_board_parsing_falls_back_to_detail_links_when_openings_empty() -> None:
    board_payload = {
        "props": {
            "pageProps": {
                "dehydratedState": {
                    "queries": [
                        {
                            "queryKey": ["openings"],
                            "state": {"data": []},
                        }
                    ]
                }
            }
        }
    }
    board_html = (
        '<script id="__NEXT_DATA__" type="application/json">'
        + json.dumps(board_payload, ensure_ascii=False)
        + "</script>"
        + '<a href="/ko/o/203940">[AI] Machine Learning Engineer</a>'
    )
    detail_payload = {
        "props": {
            "pageProps": {
                "dehydratedState": {
                    "queries": [
                        {
                            "queryKey": ["career", "getOpeningById", {"openingId": 203940, "workspaceId": 7152}],
                            "state": {
                                "data": {
                                    "data": {
                                        "openingsInfo": {
                                            "title": "[AI] Machine Learning Engineer",
                                            "detail": "<div><h2>자격요건</h2><ul><li>머신러닝 모델 개발</li></ul></div>",
                                        }
                                    }
                                }
                            },
                        }
                    ]
                }
            }
        }
    }
    detail_html = (
        '<script id="__NEXT_DATA__" type="application/json">'
        + json.dumps(detail_payload, ensure_ascii=False)
        + "</script>"
    )
    jobs = _build_greetinghr_jobs_from_html(
        board_html,
        "https://hanamts.career.greetinghr.com/ko/home",
        lambda _: detail_html,
    )
    assert len(jobs) == 1
    assert jobs[0]["job_url"] == "https://hanamts.career.greetinghr.com/ko/o/203940"
    assert "머신러닝 모델 개발" in jobs[0]["description_html"]


def test_fetch_greetinghr_source_uses_gemini_router_when_heuristics_miss(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    root_html = "<html><body><h1>Careers</h1><a href='/ko/guide'>지원 안내</a></body></html>"
    detail_payload = {
        "props": {
            "pageProps": {
                "dehydratedState": {
                    "queries": [
                        {
                            "queryKey": ["career", "getOpeningById", {"openingId": 777}],
                            "state": {
                                "data": {
                                    "data": {
                                        "openingsInfo": {
                                            "title": "AI Platform Engineer",
                                            "detail": "<div><h2>주요업무</h2><ul><li>머신러닝 플랫폼 개발</li></ul></div>",
                                        }
                                    }
                                }
                            },
                        }
                    ]
                }
            }
        }
    }
    detail_html = (
        '<script id="__NEXT_DATA__" type="application/json">'
        + json.dumps(detail_payload, ensure_ascii=False)
        + "</script>"
    )

    class FakeResponse:
        def __init__(self, text: str, url: str):
            self.text = text
            self.url = url

        def raise_for_status(self) -> None:
            return None

    class FakeClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

        def get(self, url: str):
            if url.endswith("/ko/o/777"):
                return FakeResponse(detail_html, url)
            return FakeResponse(root_html, url)

    monkeypatch.setattr(collection_module.httpx, "Client", FakeClient)
    monkeypatch.setattr(
        collection_module,
        "_extract_html_jobs_with_gemini",
        lambda *args, **kwargs: [
            {
                "title": "AI Platform Engineer",
                "job_url": "https://example.career.greetinghr.com/ko/o/777",
                "location": "",
                "experience_level": "",
            }
        ],
    )

    paths = ProjectPaths.from_root(tmp_path)
    paths.ensure_directories()
    raw, content_type = collection_module._fetch_greetinghr_source(
        "https://example.career.greetinghr.com/ko/guide",
        paths,
        AppSettings(enable_gemini_fallback=True, gemini_api_key="test-key", gemini_model="gemini-2.5-flash"),
    )
    payload = json.loads(raw)
    assert content_type == "application/json"
    assert len(payload["jobs"]) == 1
    assert payload["jobs"][0]["job_url"] == "https://example.career.greetinghr.com/ko/o/777"
    assert "머신러닝 플랫폼 개발" in payload["jobs"][0]["description_html"]


def test_recruiter_list_and_detail_parsing_support_live_ats() -> None:
    payload = {
        "list": [
            {
                "jobnoticeName": "[AI실증지원사업단] [A-21](연구직) 2025년 제4차 사업단 직원(계약직) 채용",
                "jobnoticeSn": 229578,
                "systemKindCode": "MRS2",
            }
        ]
    }
    detail_html = """
    <table>
      <tbody>
        <tr><th>공고명</th><td><span class="view-bbs-title">[AI실증지원사업단] [A-21](연구직) 2025년 제4차 사업단 직원(계약직) 채용</span></td></tr>
        <tr><th>근무지</th><td>대구광역시 동구 첨단로 61</td></tr>
      </tbody>
    </table>
    <textarea id="jobnoticeContents">&lt;div&gt;&lt;p&gt;AI 기반 디지털의료기기 실증 연구를 수행합니다.&lt;/p&gt;&lt;/div&gt;</textarea>
    """
    jobs = _build_recruiter_jobs_from_payload(
        payload,
        "https://kbri-ai.recruiter.co.kr",
        lambda _: detail_html,
    )
    assert jobs[0]["job_url"] == "https://kbri-ai.recruiter.co.kr/app/jobnotice/view?systemKindCode=MRS2&jobnoticeSn=229578"
    assert jobs[0]["location"] == "대구광역시 동구 첨단로 61"
    assert "AI 기반 디지털의료기기 실증 연구" in jobs[0]["description_html"]


def test_parse_html_jobs_follows_external_recruiter_redirect(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    shell_html = """
    <html><body>
      <a href="https://hdc-labs.recruiter.co.kr/career/home">채용홈페이지</a>
    </body></html>
    """
    recruiter_payload = {
        "jobs": [
            {
                "title": "[HDC랩스/본사] AI LAB팀 AI 엔지니어 및 데이터 사이언스 부문 경력직 채용",
                "description_html": "<div><h2>주요업무</h2><ul><li>AI 모델 개발</li></ul></div>",
                "job_url": "https://hdc-labs.recruiter.co.kr/app/jobnotice/view?systemKindCode=MRS2&jobnoticeSn=249700",
                "location": "서울",
                "country": "서울",
            }
        ]
    }

    def fake_fetch_source_content(url: str, paths, settings, source_type: str = "html_page", **kwargs):
        if url == "https://example.com/recruit":
            return shell_html, "text/html"
        if url == "https://hdc-labs.recruiter.co.kr/career/home":
            assert source_type == "recruiter"
            return json.dumps(recruiter_payload, ensure_ascii=False), "application/json"
        raise AssertionError(f"unexpected url: {url}")

    monkeypatch.setattr(collection_module, "fetch_source_content", fake_fetch_source_content)

    paths = ProjectPaths.from_root(tmp_path)
    paths.ensure_directories()
    jobs = parse_jobs_from_payload(
        shell_html,
        "text/html",
        "html_page",
        base_url="https://example.com/recruit",
        settings=AppSettings(),
        paths=paths,
        gemini_budget=collection_module.GeminiBudget(max_calls=0),
    )

    assert len(jobs) == 1
    assert jobs[0]["job_url"] == "https://hdc-labs.recruiter.co.kr/app/jobnotice/view?systemKindCode=MRS2&jobnoticeSn=249700"


def test_parse_html_jobs_follows_two_hop_external_ats_redirect(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    official_html = """
    <html><body>
      <section>
        <p>알체라는 채용 중 AI 로 꿈의 시대를 만들어 갈 진취적인 동료를 기다립니다.</p>
        <a href="https://www.saramin.co.kr/zf_user/company-info/view-inner-recruit?csn=test">상시 채용 중</a>
      </section>
    </body></html>
    """
    saramin_listing_html = """
    <html><body>
      <div class="list_item">
        <div class="box_item">
          <div class="job_tit">
            <a href="/zf_user/jobs/relay/view?view_type=list&rec_idx=53396371">[알체라] AI 연구원 모집 (얼굴인식/위조판별)</a>
          </div>
          <div class="job_condition">서울 강남구 3 ~ 8년 · 정규직</div>
        </div>
      </div>
    </body></html>
    """
    relay_payload = {
        "jobs": [
            {
                "title": "[알체라] AI 연구원 모집 (얼굴인식/위조판별)",
                "description_html": "<div><h2>주요업무</h2><ul><li>위조판별 모델 성능 고도화</li></ul><h2>자격요건</h2><ul><li>경력 3년 이상</li></ul><h2>우대사항</h2><ul><li>얼굴인식 경험자</li></ul></div>",
                "requirements": "경력 3년 이상",
                "preferred": "얼굴인식 경험자",
                "job_url": "https://www.saramin.co.kr/zf_user/jobs/relay/view?view_type=list&rec_idx=53396371",
                "location": "서울 강남구",
                "experience_level": "경력 3~8년",
            }
        ]
    }

    def fake_fetch_source_content(url: str, paths, settings, source_type: str = "html_page", **kwargs):
        if url == "https://example.com/career":
            return official_html, "text/html"
        if url == "https://www.saramin.co.kr/zf_user/company-info/view-inner-recruit?csn=test":
            return saramin_listing_html, "text/html"
        if url == "https://www.saramin.co.kr/zf_user/jobs/relay/view?view_type=list&rec_idx=53396371":
            return json.dumps(relay_payload, ensure_ascii=False), "application/json"
        raise AssertionError(f"unexpected url: {url}")

    monkeypatch.setattr(collection_module, "fetch_source_content", fake_fetch_source_content)

    paths = ProjectPaths.from_root(tmp_path)
    paths.ensure_directories()
    jobs = parse_jobs_from_payload(
        official_html,
        "text/html",
        "html_page",
        base_url="https://example.com/career",
        settings=AppSettings(),
        paths=paths,
        gemini_budget=collection_module.GeminiBudget(max_calls=0),
    )

    assert len(jobs) == 1
    assert jobs[0]["job_url"] == "https://www.saramin.co.kr/zf_user/jobs/relay/view?view_type=list&rec_idx=53396371"
    assert jobs[0]["requirements"] == "경력 3년 이상"


def test_fetch_ninehire_source_builds_jobs_from_public_api(
    monkeypatch: pytest.MonkeyPatch,
    sandbox_project: Path,
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)

    class Settings:
        use_mock_sources = False
        user_agent = "jobs-market-v2-test"
        timeout_seconds = 20.0
        connect_timeout_seconds = 5.0
        html_source_timeout_seconds = 7.0
        html_source_connect_timeout_seconds = 2.5
        ats_source_timeout_seconds = 9.0
        ats_source_connect_timeout_seconds = 2.0

    next_payload = {
        "page": "/[menu]",
        "props": {
            "pageProps": {
                "homepageProps": {
                    "info": {"companyId": "company-123"},
                    "homepage": {
                        "companyId": "company-123",
                        "pages": [
                            {
                                "sections": [
                                    {
                                        "layouts": [
                                            {
                                                "columns": [
                                                    {
                                                        "blocks": [
                                                            {
                                                                "type": "job_posting",
                                                                "countPerPage": 25,
                                                                "orderBy": "created_at_desc",
                                                                "fixedRecruitmentIds": [],
                                                            }
                                                        ]
                                                    }
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            }
                        ],
                    },
                }
            }
        },
    }
    html = (
        '<div data-provider="ninehire"></div>'
        '<script id="__NEXT_DATA__" type="application/json">'
        + json.dumps(next_payload, ensure_ascii=False)
        + "</script>"
    )

    def fake_fetch(url: str, timeout_seconds: float, user_agent: str, *, connect_timeout_seconds: float | None = None):
        assert url == "https://careers.example.com/openposition"
        return html, "text/html"

    def fake_fetch_ninehire_json(url: str, settings, *, headers=None) -> dict:
        if "identity-access/homepage/recruitments" in url:
            return {
                "count": 1,
                "results": [
                    {
                        "recruitmentId": "recruit-1",
                        "addressKey": "abc123",
                        "status": "in_progress",
                        "externalTitle": "Machine Learning Engineer",
                        "career": {"type": "experienced", "range": {"over": 3, "below": 7}},
                        "jobLocations": [{"addressName": "서울"}],
                    }
                ],
            }
        if "recruiting/job-posting" in url:
            return {
                "content": "<h3>담당 업무</h3><ul><li>모델 개발</li></ul><h3>자격 요건</h3><ul><li>PyTorch</li></ul>"
            }
        raise AssertionError(f"unexpected url: {url}")

    monkeypatch.setattr(collection_module, "_fetch_remote", fake_fetch)
    monkeypatch.setattr(collection_module, "_fetch_ninehire_public_json", fake_fetch_ninehire_json)

    content, content_type = collection_module._fetch_ninehire_source(
        "https://careers.example.com/openposition",
        paths,
        Settings(),
    )
    payload = json.loads(content)

    assert content_type == "application/json"
    assert len(payload["jobs"]) == 1
    assert payload["jobs"][0]["job_url"] == "https://careers.example.com/job_posting/abc123"
    assert payload["jobs"][0]["experience_level"] == "경력 3~7년"
    assert payload["jobs"][0]["location"] == "서울"
    assert "모델 개발" in payload["jobs"][0]["description_html"]


def test_parse_html_jobs_fetches_flex_public_job_descriptions(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    next_payload = {
        "props": {
            "pageProps": {
                "recruitingSiteResponse": {
                    "customerIdHash": "cust123",
                    "subdomain": "example",
                    "design": {"siteDesignPages": [{"type": "JOB_DESCRIPTION"}]},
                }
            }
        },
        "runtimeConfig": {
            "API_ENDPOINT": "https://flex.team",
            "RECRUITING_SITE_ROOT_DOMAIN": "careers.team",
        },
    }
    html = (
        '<script id="__NEXT_DATA__" type="application/json">'
        + json.dumps(next_payload, ensure_ascii=False)
        + "</script>"
    )
    lexical_payload = {
        "root": {
            "type": "root",
            "children": [
                {
                    "type": "heading",
                    "tag": "h3",
                    "version": 1,
                    "children": [{"type": "text", "text": "담당 업무", "version": 1}],
                },
                {
                    "type": "list",
                    "tag": "ul",
                    "version": 1,
                    "children": [
                        {
                            "type": "listitem",
                            "value": 1,
                            "version": 1,
                            "children": [{"type": "text", "text": "신경망 최적화 개발", "version": 1}],
                        }
                    ],
                },
                {
                    "type": "heading",
                    "tag": "h3",
                    "version": 1,
                    "children": [{"type": "text", "text": "자격 요건", "version": 1}],
                },
                {
                    "type": "list",
                    "tag": "ul",
                    "version": 1,
                    "children": [
                        {
                            "type": "listitem",
                            "value": 1,
                            "version": 1,
                            "children": [{"type": "text", "text": "PyTorch 활용 경험", "version": 1}],
                        }
                    ],
                },
            ],
        }
    }

    def fake_fetch_flex_public_json(url: str, settings) -> dict:
        if url.endswith("/sites/job-descriptions"):
            return {
                "jobDescriptions": [
                    {
                        "jobDescriptionIdHash": "abc123",
                        "title": "[NPU] Neural Network Optimization Engineer",
                        "jobRoleName": "개발",
                    }
                ]
            }
        if url.endswith("/job-descriptions/abc123/details"):
            return {
                "title": "[NPU] Neural Network Optimization Engineer",
                "content": {
                    "schema": "LEXICAL_V1",
                    "data": json.dumps(lexical_payload, ensure_ascii=False),
                },
            }
        raise AssertionError(f"unexpected url: {url}")

    monkeypatch.setattr(collection_module, "_fetch_flex_public_json", fake_fetch_flex_public_json)

    paths = ProjectPaths.from_root(tmp_path)
    paths.ensure_directories()
    jobs = parse_jobs_from_payload(
        html,
        "text/html",
        "html_page",
        base_url="https://www.openedges.com/positions",
        settings=AppSettings(),
        paths=paths,
        gemini_budget=collection_module.GeminiBudget(max_calls=0),
    )

    assert len(jobs) == 1
    assert jobs[0]["job_url"] == "https://example.careers.team/job-descriptions/abc123"
    assert "신경망 최적화 개발" in jobs[0]["description_html"]
    assert "PyTorch 활용 경험" in jobs[0]["description_html"]


def test_parse_html_jobs_extracts_next_rsc_notice_family_roles(tmp_path: Path) -> None:
    rsc_list_chunk = (
        '7:["$","div",null,{"children":["$","$L1b",null,{"list":['
        '{"id":10,"title":"[신입/경력] 전문연구요원 상시 모집","content":"$24","department":"개발","business_place":"플래닛 서울/ 플래닛 대전"}'
        ']}]}]'
    )
    family_html = (
        "<p><strong>포지션(분야)</strong></p>"
        "<table><tbody>"
        "<tr><td>Artificial Intelligence (인공지능)</td><td>"
        "<p>- Reinforcement learning for robot control</p>"
        "<p>- Motion recognition for the diagnosis of diseases</p>"
        "</td></tr>"
        "<tr><td>Data (데이터)</td><td>"
        "<p>- Human-robot integrative big-data management</p>"
        "<p>- Management of data-cloud (AWS)</p>"
        "</td></tr>"
        "</tbody></table>"
        "<h3>지원 자격</h3>"
        "<p>관련 연구 또는 개발 경험이 있는 분</p>"
        "<h3>우대 사항</h3>"
        "<p>로보틱스 프로젝트 경험이 있는 분</p>"
    )
    html = (
        "<html><body>"
        f'<script>self.__next_f.push([1,{json.dumps(rsc_list_chunk, ensure_ascii=False)}])</script>'
        f'<script>self.__next_f.push([1,{json.dumps("24:T10,", ensure_ascii=False)}])</script>'
        f'<script>self.__next_f.push([1,{json.dumps(family_html, ensure_ascii=False)}])</script>'
        "</body></html>"
    )

    paths = ProjectPaths.from_root(tmp_path)
    paths.ensure_directories()
    jobs = parse_jobs_from_payload(
        html,
        "text/html",
        "html_page",
        base_url="https://www.angel-robotics.com/ko/recruit/notice",
        settings=AppSettings(),
        paths=paths,
        gemini_budget=collection_module.GeminiBudget(max_calls=0),
    )

    assert [job["title"] for job in jobs] == [
        "개발 | 플래닛 서울/ 플래닛 대전 [신입/경력] 전문연구요원 상시 모집 - AI Researcher (Artificial Intelligence (인공지능))",
        "개발 | 플래닛 서울/ 플래닛 대전 [신입/경력] 전문연구요원 상시 모집 - Data Scientist (Data (데이터))",
    ]
    assert jobs[0]["job_url"] == "https://www.angel-robotics.com/ko/recruit/notice/10#artificial-intelligence-인공지능"
    assert jobs[1]["job_url"] == "https://www.angel-robotics.com/ko/recruit/notice/10#data-데이터"
    assert "주요 업무" in jobs[0]["description_html"]
    assert "지원 자격" in jobs[0]["description_html"]
    assert "Human-robot integrative big-data management" in jobs[1]["description_html"]

    source_row = {
        "source_url": "https://www.angel-robotics.com/ko/recruit/notice",
        "source_bucket": "",
        "source_name": "엔젤로보틱스 채용",
        "company_name": "엔젤로보틱스",
        "company_tier": "스타트업",
        "source_type": "html_page",
    }
    normalized = [
        normalize_job_payload(
            job,
            source_row,
            "test-run",
            "2026-04-02",
            "2026-04-02T20:00:00+09:00",
            settings=AppSettings(),
            paths=paths,
            gemini_budget=collection_module.GeminiBudget(max_calls=0),
            refine_with_gemini=False,
        )[0]
        for job in jobs
    ]
    assert [row["job_role"] for row in normalized] == ["인공지능 리서처", "데이터 사이언티스트"]
    assert normalized[0]["주요업무_분석용"] == ""
    assert normalized[0]["주요업무_표시"] == ""
    assert contains_english(normalized[1]["주요업무_표시"]) is False
    assert "포지션(분야)" not in normalized[0]["주요업무_표시"]
    assert "관련 연구 또는 개발 경험이 있는 분" in normalized[0]["자격요건_표시"]
    assert "로보틱스 프로젝트 경험이 있는 분" in normalized[1]["우대사항_표시"]
    filtered, dropped = filter_low_quality_jobs(pd.DataFrame(normalized), settings=AppSettings(), paths=paths)
    assert len(filtered) == 1
    assert len(dropped) == 1
    assert filtered.iloc[0]["job_role"] == "인공지능 리서처"
    assert dropped.iloc[0]["job_role"] == "데이터 사이언티스트"
    assert filtered.iloc[0]["자격요건_표시"] == "관련 연구 또는 개발 경험이 있는 분"


def test_classify_job_role_accepts_neural_network_optimization_engineer() -> None:
    assert collection_module.classify_job_role("[NPU] Neural Network Optimization Engineer") == "인공지능 엔지니어"


def test_recruiter_image_only_detail_uses_ocr_recovery(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    payload = {
        "list": [
            {
                "jobnoticeName": "[테스트기업] AI 엔지니어 경력직 채용",
                "jobnoticeSn": 123456,
                "systemKindCode": "MRS2",
            }
        ]
    }
    detail_html = """
    <table>
      <tbody>
        <tr><th>공고명</th><td><span class="view-bbs-title">[테스트기업] AI 엔지니어 경력직 채용</span></td></tr>
        <tr><th>접수기간</th><td>2026.04.01 ~ 2026.04.10</td></tr>
        <tr><th>근무지</th><td>서울</td></tr>
        <tr><th>첨부파일</th><td><a class="fileWrapperView" href="/mrs2/attachFile/downloadFile?fileUid=notice.pdf">(공고문) notice.pdf</a></td></tr>
      </tbody>
    </table>
    <textarea id="jobnoticeContents">&lt;div&gt;&lt;img src=&quot;/upload/notice.png&quot; /&gt;&lt;/div&gt;</textarea>
    """

    def fake_ocr(asset_urls: list[str], **_: object) -> str:
        assert "https://example.recruiter.co.kr/mrs2/attachFile/downloadFile?fileUid=notice.pdf" in asset_urls
        assert "https://example.recruiter.co.kr/upload/notice.png" in asset_urls
        return "\n".join(
            [
                "모집직무/담당업무",
                "- 인공지능 모델 개발 및 운영",
                "자격요건 및 우대경력(경험)",
                "- 파이썬 활용 경험",
                "우대사항",
                "- 석사 이상",
            ]
        )

    monkeypatch.setattr(collection_module, "extract_text_from_asset_urls", fake_ocr)
    paths = ProjectPaths.from_root(tmp_path)
    paths.ensure_directories()
    jobs = _build_recruiter_jobs_from_payload(
        payload,
        "https://example.recruiter.co.kr",
        lambda _: detail_html,
        paths=paths,
        settings=AppSettings(),
        enable_ocr_recovery=True,
    )
    assert "인공지능 모델 개발 및 운영" in jobs[0]["description_html"]

    record, _ = normalize_job_payload(
        jobs[0],
        {
            "company_name": "테스트기업",
            "source_url": "https://example.recruiter.co.kr",
            "source_bucket": "approved",
            "source_name": "테스트 recruiter",
            "company_tier": "중견/중소",
        },
        "run-1",
        "2026-04-01",
        "2026-04-01T00:00:00+09:00",
    )
    assert record["주요업무_분석용"] == "- 인공지능 모델 개발 및 운영"
    assert record["자격요건_분석용"] == "- 파이썬 활용 경험"

    filtered, dropped = filter_low_quality_jobs(pd.DataFrame([record]))
    assert len(filtered) == 1
    assert dropped.empty


def test_recruiter_ocr_recovery_drops_admin_tail_noise(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    payload = {
        "list": [
            {
                "jobnoticeName": "[테스트기업] AI 리서처 채용",
                "jobnoticeSn": 654321,
                "systemKindCode": "MRS2",
            }
        ]
    }
    detail_html = """
    <table>
      <tbody>
        <tr><th>공고명</th><td><span class="view-bbs-title">[테스트기업] AI 리서처 채용</span></td></tr>
        <tr><th>첨부파일</th><td><a class="fileWrapperView" href="/mrs2/attachFile/downloadFile?fileUid=notice.pdf">(공고문) notice.pdf</a></td></tr>
      </tbody>
    </table>
    <textarea id="jobnoticeContents">&lt;div&gt;&lt;img src=&quot;/upload/notice.png&quot; /&gt;&lt;/div&gt;</textarea>
    """

    def fake_ocr(_: list[str], **__: object) -> str:
        return "\n".join(
            [
                "수행 직무",
                "1) 의료 인공지능 알고리즘 개발 및 데이터 기반 모델링",
                "2) 디지털 치료기기 임상 및 비임상 데이터 분석",
                "우대사항",
                "1) 의료 데이터 분석 논문 또는 특허 실적 보유자",
                "• 지원 시 참고사항",
                "- 수탁연구사업 종료일: 2026. 12. 31.",
                "채용인원 2명",
            ]
        )

    monkeypatch.setattr(collection_module, "extract_text_from_asset_urls", fake_ocr)
    paths = ProjectPaths.from_root(tmp_path)
    paths.ensure_directories()
    jobs = _build_recruiter_jobs_from_payload(
        payload,
        "https://example.recruiter.co.kr",
        lambda _: detail_html,
        paths=paths,
        settings=AppSettings(),
        enable_ocr_recovery=True,
    )

    description_html = jobs[0]["description_html"]
    assert "지원 시 참고사항" not in description_html
    assert "수탁연구사업 종료일" not in description_html
    assert "채용인원" not in description_html
    assert "의료 인공지능 알고리즘 개발" in description_html


def test_fetch_recruiter_source_falls_back_without_state_code(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    class FakeResponse:
        def __init__(self, payload: dict):
            self._payload = payload

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return self._payload

    class FakeClient:
        def __init__(self, *args, **kwargs) -> None:
            self.post_calls: list[dict] = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

        def post(self, url: str, data: dict):
            self.post_calls.append({"url": url, "data": dict(data)})
            if data.get("jobnoticeStateCode") == "10":
                return FakeResponse({"list": []})
            return FakeResponse(
                {
                    "list": [
                        {
                            "jobnoticeName": "[AI] ML Engineer",
                            "jobnoticeSn": 111,
                            "systemKindCode": "MRS2",
                            "applyStartDate": {"time": 0},
                            "applyEndDate": {"time": 4102444800000},
                        },
                        {
                            "jobnoticeName": "[Closed] Old Notice",
                            "jobnoticeSn": 222,
                            "systemKindCode": "MRS2",
                            "recruitEndYn": "Y",
                        },
                    ],
                    "totalCount": 2,
                }
            )

        def get(self, url: str):
            raise AssertionError(f"unexpected detail fetch: {url}")

    captured_payloads: list[dict] = []

    def fake_build(payload: dict, source_url: str, detail_fetcher, **kwargs):
        captured_payloads.append(payload)
        return [
            {
                "title": item["jobnoticeName"],
                "description_html": "<div>stub</div>",
                "job_url": f"{source_url}/app/jobnotice/view?systemKindCode={item.get('systemKindCode')}&jobnoticeSn={item.get('jobnoticeSn')}",
            }
            for item in payload.get("list", [])
        ]

    monkeypatch.setattr(collection_module.httpx, "Client", FakeClient)
    monkeypatch.setattr(collection_module, "_build_recruiter_jobs_from_payload", fake_build)

    paths = ProjectPaths.from_root(tmp_path)
    paths.ensure_directories()
    raw, content_type = collection_module._fetch_recruiter_source(
        "https://tenant.recruiter.co.kr/appsite/company",
        paths,
        AppSettings(),
    )

    payload = json.loads(raw)
    assert content_type == "application/json"
    assert len(payload["jobs"]) == 1
    assert payload["jobs"][0]["title"] == "[AI] ML Engineer"
    assert len(captured_payloads) == 1
    assert len(captured_payloads[0]["list"]) == 1


def test_fetch_recruiter_source_filters_closed_notices_even_with_state10(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    class FakeResponse:
        def __init__(self, payload: dict):
            self._payload = payload

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return self._payload

    class FakeClient:
        def __init__(self, *args, **kwargs) -> None:
            self.post_calls: list[dict] = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

        def post(self, url: str, data: dict):
            self.post_calls.append({"url": url, "data": dict(data)})
            return FakeResponse(
                {
                    "list": [
                        {
                            "jobnoticeName": "[Open] Data Scientist",
                            "jobnoticeSn": 111,
                            "systemKindCode": "MRS2",
                            "applyStartDate": {"time": 0},
                            "applyEndDate": {"time": 4102444800000},
                            "receiptState": "접수중",
                            "deadlineCount": 5,
                        },
                        {
                            "jobnoticeName": "[Closed] AI Engineer",
                            "jobnoticeSn": 222,
                            "systemKindCode": "MRS2",
                            "applyStartDate": {"time": 0},
                            "applyEndDate": {"time": 0},
                            "receiptState": "접수마감",
                            "deadlineCount": -6,
                        },
                    ],
                    "totalCount": 2,
                }
            )

        def get(self, url: str):
            raise AssertionError(f"unexpected detail fetch: {url}")

    captured_payloads: list[dict] = []

    def fake_build(payload: dict, source_url: str, detail_fetcher, **kwargs):
        captured_payloads.append(payload)
        return [
            {
                "title": item["jobnoticeName"],
                "description_html": "<div>stub</div>",
                "job_url": f"{source_url}/app/jobnotice/view?systemKindCode={item.get('systemKindCode')}&jobnoticeSn={item.get('jobnoticeSn')}",
            }
            for item in payload.get("list", [])
        ]

    monkeypatch.setattr(collection_module.httpx, "Client", FakeClient)
    monkeypatch.setattr(collection_module, "_build_recruiter_jobs_from_payload", fake_build)

    paths = ProjectPaths.from_root(tmp_path)
    paths.ensure_directories()
    raw, content_type = collection_module._fetch_recruiter_source(
        "https://tenant.recruiter.co.kr/career/home",
        paths,
        AppSettings(),
    )

    payload = json.loads(raw)
    assert content_type == "application/json"
    assert len(payload["jobs"]) == 1
    assert payload["jobs"][0]["title"] == "[Open] Data Scientist"
    assert len(captured_payloads) == 1
    assert len(captured_payloads[0]["list"]) == 1


def test_explicit_non_korea_location_is_filtered_out() -> None:
    record, raw = normalize_job_payload(
        {
            "title": "Machine Learning Engineer",
            "description_html": "<h2>Requirements</h2><ul><li>Python</li></ul>",
            "job_url": "https://jobs.lever.co/example/1",
            "location": "Tokyo, Japan",
            "country": "JP",
        },
        {
            "company_name": "테스트회사",
            "source_url": "https://api.lever.co/v0/postings/example?mode=json",
            "source_bucket": "approved",
            "source_name": "테스트 소스",
            "company_tier": "스타트업",
        },
        "run-1",
        "2026-03-29",
        "2026-03-29T00:00:00+09:00",
    )
    assert record == {}
    assert raw == {}


def test_normalize_job_payload_drops_closed_html_notice_from_title_or_context() -> None:
    record, raw = normalize_job_payload(
        {
            "title": "[전문연구요원 가능] AI 리서치 엔지니어 채용 완료",
            "listing_context": "AI 리서치 엔지니어 채용 완료",
            "description_html": "<h2>주요업무</h2><ul><li>LLM 연구 개발</li></ul>",
            "job_url": "https://recruit.example.com/jobs/closed-ai-role",
        },
        {
            "company_name": "메이사",
            "source_url": "https://recruit.example.com",
            "source_type": "html_page",
            "source_bucket": "approved",
            "source_name": "메이사 공식 채용",
            "company_tier": "스타트업",
        },
        "run-1",
        "2026-04-02",
        "2026-04-02T00:00:00+09:00",
    )

    assert record == {}
    assert raw == {}


def test_normalize_job_payload_keeps_open_notice_when_description_mentions_conditional_early_close() -> None:
    record, raw = normalize_job_payload(
        {
            "title": "[공통] Data Scientist",
            "experience_level": "경력 2~6년",
            "location": "서울특별시 강남구 도산대로 317",
            "country": "서울특별시 강남구 도산대로 317",
            "description_html": (
                "<h2>주요 업무</h2><ul><li>금융 데이터를 기반으로 모델을 설계합니다.</li></ul>"
                "<p>기간 내 채용 완료 시 조기 마감될 수 있으며, 이후 인재풀로 활용될 수 있음을 참고 부탁드립니다.</p>"
            ),
            "job_url": "https://gowid.career.greetinghr.com/ko/o/165121",
        },
        {
            "company_name": "고위드",
            "source_url": "https://gowid.career.greetinghr.com",
            "source_type": "greetinghr",
            "source_bucket": "approved",
            "source_name": "고위드 GreetingHR 공개 채용",
            "company_tier": "스타트업",
        },
        "run-1",
        "2026-04-02",
        "2026-04-02T00:00:00+09:00",
    )

    assert record["job_role"] == "데이터 사이언티스트"
    assert record["job_url"] == "https://gowid.career.greetinghr.com/ko/o/165121"
    assert raw["job_title_raw"] == "[공통] Data Scientist"


def test_build_display_fields_infers_new_grad_from_graduation_candidate_signal() -> None:
    record = {
        "job_title_raw": "[2026 당근 ML] Software Engineer, Machine Learning (석사)",
        "job_title_ko": "[2026 당근 ML] Software Engineer, Machine Learning (석사)",
        "experience_level_raw": "",
        "experience_level_ko": "",
        "requirements": "2026년 8월 이내 석사 졸업 예정자 또는 석사 학위 기졸업자여야 합니다.",
        "preferred": "",
        "description_text": "",
    }

    display = build_display_fields(record, analysis_fields={})

    assert display["경력수준_표시"] == "신입"


def test_build_display_fields_prefers_explicit_years_over_new_grad_phrase() -> None:
    record = {
        "job_title_raw": "Applied Scientist II (응용 과학자 II)",
        "job_title_ko": "Applied Scientist II (응용 과학자 II)",
        "experience_level_raw": "",
        "experience_level_ko": "",
        "requirements": "박사 학위(신규 졸업자 환영) 또는 정량적 분야에서 2년 이상의 산업 또는 대학원 연구 경험이 필요합니다.",
        "preferred": "",
        "description_text": "",
    }

    display = build_display_fields(record, analysis_fields={})

    assert display["경력수준_표시"] == "경력 2년+"


def test_build_display_fields_infers_experience_for_research_track_without_numeric_years() -> None:
    record = {
        "job_title_raw": "(전문연지원가능) AI Research Engineer / Scientist",
        "job_title_ko": "(전문연지원가능) AI Research Engineer / Scientist",
        "experience_level_raw": "",
        "experience_level_ko": "",
        "requirements": "전문연구요원 편입 가능 대상자이며 딥러닝 기반 알고리즘 연구 및 고도화 경험이 필요합니다.",
        "preferred": "국제 학회 논문 게재 경험이 있으면 좋습니다.",
        "description_text": "생체신호 처리 및 모델 최적화 연구를 수행합니다.",
    }

    display = build_display_fields(record, analysis_fields={})

    assert display["경력수준_표시"] == "경력"


def test_build_display_fields_infers_experience_for_degree_based_research_role() -> None:
    record = {
        "job_title_raw": "상시채용 R&D Center AI Group",
        "job_title_ko": "상시채용 R&D Center AI Group",
        "experience_level_raw": "",
        "experience_level_ko": "",
        "requirements": "파이토치 활용 역량과 논리적 사고가 필요합니다.",
        "preferred": "관련 분야 석사 또는 박사 학위를 소지한 분, SCI급 저널 논문 게재 경험이 있는 분",
        "description_text": "최신 생체신호 관련 인공지능 기술 논문 서베이와 모델 개발 및 분석을 담당합니다.",
    }

    display = build_display_fields(record, analysis_fields={})

    assert display["경력수준_표시"] == "경력"


def test_normalize_job_payload_uses_gemini_role_salvage_when_heuristics_fail(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(collection_module, "maybe_salvage_job_role", lambda *args, **kwargs: "데이터 분석가")

    paths = ProjectPaths.from_root(tmp_path)
    paths.ensure_directories()
    settings = AppSettings(
        enable_gemini_fallback=True,
        gemini_api_key="test-key",
        gemini_model="gemini-2.5-flash",
        gemini_role_salvage_max_calls_per_run=2,
    )
    record, _ = normalize_job_payload(
        {
            "title": "Business Operations Specialist",
            "description_html": "<h2>주요업무</h2><ul><li>대시보드 기반 지표 모니터링 및 리포트 자동화</li></ul>",
            "job_url": "https://example.com/jobs/1",
        },
        {
            "company_name": "신세계I&C",
            "source_url": "https://shinsegaeinc.recruiter.co.kr/appsite/company",
            "source_type": "recruiter",
            "source_bucket": "approved",
            "source_name": "신세계I&C 공개 ATS",
            "company_tier": "대기업",
        },
        "run-1",
        "2026-04-02",
        "2026-04-02T00:00:00+09:00",
        settings=settings,
        paths=paths,
        gemini_budget=collection_module.GeminiBudget(max_calls=2),
        refine_with_gemini=False,
    )

    assert record["job_role"] == "데이터 분석가"


def test_normalize_job_payload_uses_description_context_for_ai_engineer_titles(
    tmp_path: Path,
) -> None:
    paths = ProjectPaths.from_root(tmp_path)
    paths.ensure_directories()
    settings = AppSettings(
        enable_gemini_fallback=False,
        gemini_role_salvage_max_calls_per_run=0,
    )
    record, _ = normalize_job_payload(
        {
            "title": "System Software Engineer 신입/경력 채용",
            "description_html": (
                "<h2>주요업무</h2><ul><li>AI 반도체 칩 개발 및 양산 프로세스 전반에 대한 이해</li>"
                "<li>System Software 설계 및 최적화</li></ul>"
            ),
            "job_url": "https://example.com/jobs/system-software-engineer",
        },
        {
            "company_name": "하이퍼엑셀",
            "source_url": "https://hyperaccel.career.greetinghr.com/ko/guide",
            "source_type": "greetinghr",
            "source_bucket": "approved",
            "source_name": "하이퍼엑셀 GreetingHR",
            "company_tier": "스타트업",
        },
        "run-1",
        "2026-04-09",
        "2026-04-09T00:00:00+09:00",
        settings=settings,
        paths=paths,
        gemini_budget=collection_module.GeminiBudget(max_calls=0),
        refine_with_gemini=False,
    )

    assert record["job_role"] == "인공지능 엔지니어"


def test_normalize_job_payload_skips_gemini_role_salvage_for_general_service_delivery_title(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(collection_module, "maybe_salvage_job_role", lambda *args, **kwargs: "인공지능 엔지니어")

    paths = ProjectPaths.from_root(tmp_path)
    paths.ensure_directories()
    settings = AppSettings(
        enable_gemini_fallback=True,
        gemini_api_key="test-key",
        gemini_model="gemini-2.5-flash",
        gemini_role_salvage_max_calls_per_run=2,
    )
    record, _ = normalize_job_payload(
        {
            "title": "서비스 개발자(Service Developer)",
            "description_html": (
                "<h2>담당 업무</h2><ul><li>서비스 플랫폼 설계 및 개발</li><li>UX/UI 개선</li></ul>"
                "<h2>자격 요건</h2><ul><li>3년 이상의 웹/모바일 서비스 개발 경험</li><li>RESTful API 설계 및 구현 경험</li></ul>"
                "<h2>우대 사항</h2><ul><li>React, Node.js, Django 활용 경험</li><li>SQL, NoSQL 기반 서비스 운영 경험</li></ul>"
            ),
            "job_url": "https://example.com/jobs/service",
        },
        {
            "company_name": "테스트 서비스랩",
            "source_url": "https://example.com/recruit",
            "source_type": "html_page",
            "source_bucket": "approved",
            "source_name": "테스트 서비스랩 채용",
            "company_tier": "스타트업",
        },
        "run-1",
        "2026-04-03",
        "2026-04-03T00:00:00+09:00",
        settings=settings,
        paths=paths,
        gemini_budget=collection_module.GeminiBudget(max_calls=2),
        refine_with_gemini=False,
    )

    assert record == {}


def test_korean_only_display_fields() -> None:
    display = build_display_fields(
        {
            "company_name": "Microsoft",
            "source_name": "Global Careers",
            "job_title_raw": "AI Engineer",
            "job_role": "인공지능 엔지니어",
            "experience_level_raw": "Experienced",
            "main_tasks": "모델 개발",
            "requirements": "파이썬 활용",
            "preferred": "서비스 운영 경험",
            "core_skills": "PyTorch",
        }
    )
    assert display["회사명_표시"] == "한글명 미확인"
    assert display["소스명_표시"] == "한글명 미확인"
    assert display["공고제목_표시"] == "AI Engineer"
    assert display["경력수준_표시"] == "경력"
    assert display["경력근거_표시"] == "구조화 메타데이터"
    assert display["채용트랙_표시"] == "일반채용"
    assert display["채용트랙근거_표시"] == "기본추론"
    assert display["직무초점_표시"] == ""
    assert display["직무초점근거_표시"] == ""
    assert display["구분요약_표시"] == "경력"
    assert display["직무명_표시"] == "인공지능 엔지니어"
    assert display["핵심기술_표시"] == "파이토치"


def test_normalize_experience_level_drops_employment_type_labels() -> None:
    assert normalize_experience_level("Full-time") == ""
    assert normalize_experience_level("contract") == ""
    assert normalize_experience_level("경력") == "경력"


def test_display_fields_surface_seniority_and_track_signals() -> None:
    senior_display = build_display_fields(
        {
            "company_name": "하이퍼커넥트",
            "source_name": "Lever",
            "job_title_raw": "Senior Machine Learning Engineer (Match Group AI)",
            "experience_level_raw": "",
            "job_role": "인공지능 엔지니어",
            "main_tasks": "모델 개발",
            "requirements": "파이썬 활용",
            "preferred": "",
            "core_skills": "PyTorch",
        }
    )
    assert senior_display["경력수준_표시"] == "시니어"
    assert senior_display["경력근거_표시"] == "구조화 메타데이터"
    assert senior_display["채용트랙_표시"] == "일반채용"
    assert senior_display["채용트랙근거_표시"] == "기본추론"
    assert senior_display["직무초점_표시"] == ""
    assert senior_display["직무초점근거_표시"] == ""
    assert senior_display["구분요약_표시"] == "시니어"

    track_display = build_display_fields(
        {
            "company_name": "하이퍼커넥트",
            "source_name": "Lever",
            "job_title_raw": "Machine Learning Engineer (Match Group AI | 전문연구요원 편입/전직 가능)",
            "experience_level_raw": "Full-time",
            "job_role": "인공지능 엔지니어",
            "main_tasks": "모델 개발",
            "requirements": "파이썬 활용",
            "preferred": "",
            "core_skills": "PyTorch",
        }
    )
    assert track_display["경력수준_표시"] == "미기재"
    assert track_display["경력근거_표시"] == "표시기본값"
    assert track_display["채용트랙_표시"] == "전문연구요원"
    assert track_display["채용트랙근거_표시"] == "구조화 메타데이터"
    assert track_display["직무초점_표시"] == ""
    assert track_display["직무초점근거_표시"] == ""
    assert track_display["구분요약_표시"] == "전문연구요원"


def test_display_fields_defaults_general_track_without_polluting_summary() -> None:
    display = build_display_fields(
        {
            "company_name": "테스트회사",
            "source_name": "테스트소스",
            "job_title_raw": "Data Scientist",
            "experience_level_raw": "",
            "job_role": "데이터 사이언티스트",
            "requirements": "파이썬과 SQL 활용 경험이 필요합니다.",
            "preferred": "",
            "main_tasks": "데이터 모델을 개발합니다.",
            "core_skills": "Python",
        }
    )
    assert display["채용트랙_표시"] == "일반채용"
    assert display["채용트랙근거_표시"] == "기본추론"
    assert display["구분요약_표시"] == ""


def test_display_fields_can_extract_experience_from_requirements_with_source() -> None:
    display = build_display_fields(
        {
            "company_name": "테스트회사",
            "source_name": "테스트소스",
            "job_title_raw": "Machine Learning Engineer",
            "experience_level_raw": "",
            "job_role": "인공지능 엔지니어",
            "requirements": "관련 분야 경력 5년 이상이 필요합니다.",
            "preferred": "",
            "main_tasks": "모델 개발",
            "core_skills": "PyTorch",
        }
    )
    assert display["경력수준_표시"] == "경력 5년+"
    assert display["경력근거_표시"] == "자격요건"


def test_display_fields_can_extract_broad_korean_year_patterns_from_title_and_requirements() -> None:
    title_display = build_display_fields(
        {
            "company_name": "테스트회사",
            "source_name": "테스트소스",
            "job_title_raw": "Research Engineer - Foundation Models (2년 이상 / 계약직)",
            "experience_level_raw": "",
            "job_role": "인공지능 엔지니어",
            "requirements": "",
            "preferred": "",
            "main_tasks": "모델 개발",
            "core_skills": "PyTorch",
        }
    )
    assert title_display["경력수준_표시"] == "경력 2년+"
    assert title_display["경력근거_표시"] == "구조화 메타데이터"

    requirement_display = build_display_fields(
        {
            "company_name": "테스트회사",
            "source_name": "테스트소스",
            "job_title_raw": "AI Backend Engineer",
            "experience_level_raw": "",
            "job_role": "인공지능 엔지니어",
            "requirements": "2년 이상의 웹 백엔드 개발 경험이 있는 분",
            "preferred": "",
            "main_tasks": "모델 서비스 개발",
            "core_skills": "Python",
        }
    )
    assert requirement_display["경력수준_표시"] == "경력 2년+"
    assert requirement_display["경력근거_표시"] == "자격요건"


def test_display_fields_infer_broad_experience_from_strong_professional_signals() -> None:
    display = build_display_fields(
        {
            "company_name": "당근",
            "source_name": "당근 채용",
            "job_title_raw": "Software Engineer, Machine Learning | 검색 (품질)",
            "experience_level_raw": "",
            "job_role": "인공지능 엔지니어",
            "requirements": "머신러닝 기반 추천/검색/랭킹 시스템을 실제로 설계·운영해본 경험이 있는 분",
            "preferred": "",
            "main_tasks": "검색 품질 개선을 위한 모델을 개발합니다.",
            "core_skills": "Python",
        }
    )
    assert display["경력수준_표시"] == "경력"
    assert display["경력근거_표시"] == "자격요건"


def test_display_fields_drop_english_only_prose_from_visible_sections() -> None:
    display = build_display_fields(
        {
            "company_name": "몰로코",
            "source_name": "몰로코 채용",
            "job_title_raw": "Senior Applied Scientist",
            "experience_level_raw": "",
            "job_role": "데이터 사이언티스트",
            "requirements": "",
            "preferred": "",
            "main_tasks": "",
            "core_skills": "",
        },
        analysis_fields={
            "주요업무_분석용": (
                "Leading research projects with cross-functional collaborators to evaluate "
                "system health and drive infrastructure improvements."
            ),
            "자격요건_분석용": "",
            "우대사항_분석용": "",
            "핵심기술_분석용": "",
            "상세본문_분석용": "",
        },
    )
    assert display["주요업무_표시"] == ""
    assert display["우대사항_표시"] == "별도 우대사항 미기재"


def test_display_fields_default_experience_to_unspecified_without_polluting_summary() -> None:
    display = build_display_fields(
        {
            "company_name": "테스트회사",
            "source_name": "테스트소스",
            "job_title_raw": "AI Engineer",
            "experience_level_raw": "",
            "job_role": "인공지능 엔지니어",
            "requirements": "파이썬과 SQL 활용 능력이 필요합니다.",
            "preferred": "",
            "main_tasks": "데이터 파이프라인을 개발합니다.",
            "core_skills": "Python",
        }
    )
    assert display["경력수준_표시"] == "미기재"
    assert display["경력근거_표시"] == "표시기본값"
    assert "미기재" not in display["구분요약_표시"]


def test_display_fields_surface_focus_signals_for_summary() -> None:
    display = build_display_fields(
        {
            "company_name": "당근",
            "source_name": "당근 채용",
            "job_title_raw": "Software Engineer, Machine Learning | 검색 (품질)",
            "experience_level_raw": "",
            "job_role": "인공지능 엔지니어",
            "requirements": "추천/검색/랭킹 시스템 운영 경험",
            "preferred": "",
            "main_tasks": "자연어처리와 개인화 알고리즘으로 검색 품질 향상",
            "core_skills": "PyTorch",
        }
    )
    assert display["직무초점_표시"] == "검색 / 추천"
    assert display["직무초점근거_표시"] == "공고제목 / 자격요건"
    assert display["구분요약_표시"] == "검색 / 추천"


def test_display_fields_ignore_calendar_year_in_requirements() -> None:
    display = build_display_fields(
        {
            "company_name": "당근",
            "source_name": "당근 채용",
            "job_title_raw": "[2026 당근 ML] Software Engineer, Machine Learning (학사)",
            "experience_level_raw": "",
            "job_role": "인공지능 엔지니어",
            "requirements": "2026년 2월 이후 졸업 예정자 또는 기졸업자 지원 가능",
            "preferred": "",
            "main_tasks": "모델 개발",
            "core_skills": "PyTorch",
        }
    )
    assert display["경력수준_표시"] == "신입"
    assert display["경력근거_표시"] == "자격요건"


def test_build_analysis_fields_backfills_sections_from_raw_detail_headings() -> None:
    analysis = build_analysis_fields(
        {
            "job_title_raw": "Research Engineer",
            "description_text": """
[Mission of the Role]
비전 모델을 연구 개발합니다.
실험 결과를 분석합니다.
[Qualifications]
파이썬 활용 경험이 필요합니다.
머신러닝 프로젝트 경험이 필요합니다.
[Preferred]
검색증강생성 경험 우대
""",
            "main_tasks": "",
            "requirements": "",
            "preferred": "",
            "core_skills": "",
        }
    )
    assert "비전 모델을 연구 개발합니다." in analysis["주요업무_분석용"]
    assert "파이썬 활용 경험이 필요합니다." in analysis["자격요건_분석용"]
    assert "검색증강생성 경험 우대" in analysis["우대사항_분석용"]


def test_sanitize_core_skill_text_drops_generic_only_labels() -> None:
    assert sanitize_core_skill_text("인공지능\n머신러닝") == ""
    assert sanitize_core_skill_text("인공지능\n파이토치") == "파이토치"


def test_build_analysis_fields_backfills_korean_template_headings() -> None:
    analysis = build_analysis_fields(
        {
            "job_title_raw": "Data Analyst",
            "description_text": """
이런 일을 해요
제품 데이터를 분석하고 실험을 설계합니다.
이런 분과 함께하고 싶어요
SQL과 Python 활용 경험이 필요합니다.
이런 분이면 더 좋아요
마케팅 자동화 경험이 있으면 좋습니다.
""",
            "main_tasks": "",
            "requirements": "",
            "preferred": "",
            "core_skills": "",
        }
    )
    assert "제품 데이터를 분석하고 실험을 설계합니다." in analysis["주요업무_분석용"]
    assert "에스큐엘과 파이썬 활용 경험이 필요합니다." in analysis["자격요건_분석용"]
    assert "마케팅 자동화 경험이 있으면 좋습니다." in analysis["우대사항_분석용"]


def test_build_analysis_fields_recovers_alternate_korean_requirement_and_preferred_headings() -> None:
    analysis = build_analysis_fields(
        {
            "job_title_raw": "AI Engineer",
            "description_text": """
이런 일을 해요
AI 모델을 연구하고 개발합니다.
이런 분을 원해요
2년 이상의 웹 백엔드 개발 경험이 있는 분
이런 분이라면 더 좋아요
도커와 쿠버네티스 사용 경험이 있으신 분
이렇게 근무해요
유연근무제를 운영합니다.
""",
            "main_tasks": "",
            "requirements": "",
            "preferred": "",
            "core_skills": "",
        }
    )
    assert "모델을 연구하고 개발합니다." in analysis["주요업무_분석용"]
    assert "2년 이상의 웹 백엔드 개발 경험이 있는 분" in analysis["자격요건_분석용"]
    assert "도커와 쿠버네티스 사용 경험이 있으신 분" in analysis["우대사항_분석용"]
    assert "유연근무제를 운영합니다." not in analysis["우대사항_분석용"]


def test_build_analysis_fields_recovers_inline_and_html_preferred_sections() -> None:
    analysis = build_analysis_fields(
        {
            "job_title_raw": "Data Analyst",
            "description_html": """
<div>
  <p>Responsibilities</p>
  <ul><li>제품 지표를 분석합니다.</li></ul>
  <p>공통자격</p>
  <ul><li>파이썬 활용 경험이 필요합니다.</li></ul>
  <p>Preferred Qualifications</p>
  <ul><li>문제를 구조화해서 설명할 수 있는 분이면 좋습니다.</li></ul>
  <p>선호: 시스템 관점에서 사고하고 협업하는 역량</p>
</div>
""",
            "main_tasks": "",
            "requirements": "",
            "preferred": "",
            "core_skills": "",
        }
    )
    assert "제품 지표를 분석합니다." in analysis["주요업무_분석용"]
    assert "파이썬 활용 경험이 필요합니다." in analysis["자격요건_분석용"]
    assert "문제를 구조화해서 설명할 수 있는 분이면 좋습니다." in analysis["우대사항_분석용"]
    assert "시스템 관점에서 사고하고 협업하는 역량" in analysis["우대사항_분석용"]


def test_build_analysis_fields_recovers_expanded_english_preferred_headings() -> None:
    analysis = build_analysis_fields(
        {
            "job_title_raw": "Applied AI Engineer",
            "description_text": """
Requirements
Production 환경에서 LLM 기능을 설계하고 운영한 경험
Bonus Points
검색증강생성 시스템을 상용화한 경험
Would be a plus
음성 또는 멀티모달 시스템 구축 경험
""",
            "main_tasks": "",
            "requirements": "",
            "preferred": "",
            "core_skills": "",
        }
    )
    assert "환경에서 엘엘엠 기능을 설계하고 운영한 경험" in analysis["자격요건_분석용"]
    assert "검색증강생성 시스템을 상용화한 경험" in analysis["우대사항_분석용"]
    assert "음성 또는 멀티모달 시스템 구축 경험" in analysis["우대사항_분석용"]


def test_build_analysis_fields_recovers_colloquial_korean_preferred_heading() -> None:
    analysis = build_analysis_fields(
        {
            "job_title_raw": "음성인식 엔진/모델 개발자",
            "description_text": """
직무를 수행해요 !
머신러닝 프레임워크 기반 음성인식 모델 개발 및 학습
경력이 필요해요 !
경력 2년 이상 연구 경험이 필요합니다.
경험을 보유하셨다면 좋아요 !
파인 튜닝 경험을 보유하신 분
대용량 음성/텍스트 데이터 관리 경험을 보유하신 분
채용 프로세스를 참고해요 !
서류 전형 > 인터뷰
""",
            "main_tasks": "",
            "requirements": "",
            "preferred": "",
            "core_skills": "",
        }
    )
    assert "파인 튜닝 경험을 보유하신 분" in analysis["우대사항_분석용"]
    assert "대용량 음성/텍스트 데이터 관리 경험을 보유하신 분" in analysis["우대사항_분석용"]


def test_normalize_job_analysis_fields_defaults_preferred_display_when_analysis_is_blank() -> None:
    frame = pd.DataFrame(
        [
            {
                "company_name": "테스트회사",
                "source_name": "테스트소스",
                "job_title_raw": "Applied AI Engineer",
                "job_role": "인공지능 엔지니어",
                "experience_level_raw": "",
                "main_tasks": "모델을 개발합니다.",
                "requirements": "프로덕션 환경에서 기능을 운영한 경험",
                "preferred": "",
                "core_skills": "Python",
                "description_text": "프로덕션 환경에서 기능을 운영한 경험",
                "job_url": "https://example.com/jobs/1",
                "회사명_표시": "테스트회사",
                "소스명_표시": "테스트소스",
                "공고제목_표시": "Applied AI Engineer",
                "경력수준_표시": "",
                "경력근거_표시": "",
                "채용트랙_표시": "",
                "채용트랙근거_표시": "",
                "직무초점_표시": "",
                "직무초점근거_표시": "",
                "구분요약_표시": "",
                "직무명_표시": "인공지능 엔지니어",
                "주요업무_분석용": "모델을 개발합니다.",
                "자격요건_분석용": "프로덕션 환경에서 기능을 운영한 경험",
                "우대사항_분석용": "",
                "핵심기술_분석용": "파이썬",
                "상세본문_분석용": "프로덕션 환경에서 기능을 운영한 경험",
                "주요업무_표시": "",
                "자격요건_표시": "",
                "우대사항_표시": "",
                "핵심기술_표시": "",
            }
        ]
    )

    normalized = normalize_job_analysis_fields(frame)

    assert normalized.loc[0, "경력수준_표시"] == "경력"
    assert normalized.loc[0, "우대사항_분석용"] == ""
    assert normalized.loc[0, "우대사항_표시"] == "별도 우대사항 미기재"


def test_normalize_job_analysis_fields_salvages_preferred_from_detail_when_missing() -> None:
    frame = pd.DataFrame(
        [
            {
                "company_name": "셀바스AI",
                "source_name": "셀바스AI 채용",
                "source_url": "https://www.selvasai.com/careers",
                "job_title_raw": "음성인식 엔진/모델 개발자",
                "job_role": "인공지능 엔지니어",
                "experience_level_raw": "",
                "job_url": "https://example.com/jobs/selvas",
                "회사명_표시": "셀바스AI",
                "소스명_표시": "셀바스AI 채용",
                "공고제목_표시": "음성인식 엔진/모델 개발자",
                "경력수준_표시": "",
                "경력근거_표시": "",
                "채용트랙_표시": "",
                "채용트랙근거_표시": "",
                "직무초점_표시": "",
                "직무초점근거_표시": "",
                "구분요약_표시": "",
                "직무명_표시": "인공지능 엔지니어",
                "주요업무_분석용": "음성인식 모델을 개발합니다.",
                "자격요건_분석용": "경력 2년 이상 연구 경험이 필요합니다.",
                "우대사항_분석용": "",
                "핵심기술_분석용": "파이토치",
                "상세본문_분석용": "경험을 보유하셨다면 좋아요 !\n파인 튜닝 경험을 보유하신 분\n대용량 음성/텍스트 데이터 관리 경험을 보유하신 분\n채용 프로세스를 참고해요 !",
                "주요업무_표시": "",
                "자격요건_표시": "",
                "우대사항_표시": "",
                "핵심기술_표시": "",
            }
        ]
    )

    normalized = normalize_job_analysis_fields(frame)

    assert "파인 튜닝 경험을 보유하신 분" in normalized.loc[0, "우대사항_분석용"]
    assert "대용량 음성/텍스트 데이터 관리 경험을 보유하신 분" in normalized.loc[0, "우대사항_표시"]


def test_normalize_job_analysis_fields_recovers_requirements_and_main_task_from_sparse_ai_posting() -> None:
    frame = pd.DataFrame(
        [
            {
                "company_name": "업스테이지",
                "source_name": "GreetingHR",
                "job_title_raw": "AI Research Engineer - LLM Post-training",
                "job_title_ko": "AI Research Engineer - LLM Post-training",
                "job_role": "인공지능 엔지니어",
                "주요업무_분석용": "",
                "자격요건_분석용": "",
                "우대사항_분석용": "엘엘엠을 수행하여 특정 문제에서 최고 성능을 달성해본 경험\n머신러닝과 엔엘피 토픽으로 국제 학회에서 출판 기록 (1저자 혹은 교신저자)",
                "핵심기술_분석용": "엘엘엠\n최적화",
                "상세본문_분석용": "근무 형태\n정규직\n엘엘엠을 수행하여 특정 문제에서 최고 성능을 달성해본 경험\n머신러닝과 엔엘피 토픽으로 국제 학회에서 출판 기록 (1저자 혹은 교신저자)",
            }
        ]
    )

    normalized = normalize_job_analysis_fields(frame)

    assert "사후학습" in normalized.loc[0, "주요업무_분석용"]
    assert "최고 성능을 달성해본 경험" in normalized.loc[0, "자격요건_분석용"]
    assert normalized.loc[0, "직무초점_표시"] in {"연구", "최적화", "LLM / 최적화"}


def test_filter_low_quality_jobs_drops_admin_only_pool_posting() -> None:
    staging = pd.DataFrame(
        [
            {
                "is_active": True,
                "company_name": "노타",
                "company_tier": "스타트업",
                "job_role": "인공지능 리서처",
                "job_title_raw": "Talent Pool (R&D)",
                "job_title_ko": "Talent Pool (R&D)",
                "job_url": "https://career.nota.ai/en/jobs/pool",
                "주요업무_분석용": "",
                "자격요건_분석용": "채용 관련 문의사항은 메일로 보내주세요.\n지원 전 확인해주세요.\n제출하신 서류는 3년간 채용 디비에 보관됩니다.",
                "우대사항_분석용": "",
                "핵심기술_분석용": "",
                "상세본문_분석용": "채용 관련 문의사항은 메일로 보내주세요.\n지원 전 확인해주세요.\n허위 사실이 있을 경우 채용이 취소될 수 있습니다.\n제출하신 서류는 3년간 채용 디비에 보관됩니다.",
            }
        ]
    )

    filtered, dropped = filter_low_quality_jobs(staging)

    assert filtered.empty
    assert len(dropped) == 1


def test_normalize_job_analysis_fields_adds_focus_fallback_for_research_titles() -> None:
    frame = pd.DataFrame(
        [
            {
                "company_name": "퓨리오사AI",
                "source_name": "Careers",
                "job_title_raw": "Algorithm - AI Research Engineer",
                "job_title_ko": "Algorithm - AI Research Engineer",
                "job_role": "인공지능 리서처",
                "주요업무_분석용": "선도적인 인공지능 연구를 주도적으로 수행합니다.",
                "자격요건_분석용": "관련 분야 석사 학위 또는 이에 준하는 경험이 필요합니다.",
                "우대사항_분석용": "",
                "핵심기술_분석용": "파이토치\n엘엘엠",
                "상세본문_분석용": "선도적인 인공지능 연구를 수행합니다.\n관련 분야 석사 학위가 필요합니다.",
            }
        ]
    )

    normalized = normalize_job_analysis_fields(frame)

    assert normalized.loc[0, "직무초점_표시"] == "연구"
    assert normalized.loc[0, "직무초점근거_표시"] in {"제목/요건추론", "주요업무", "자격요건"}


def test_canonicalize_runtime_source_url_roots_custom_ats_hosts() -> None:
    assert canonicalize_runtime_source_url("https://interxlab.career.greetinghr.com/aboutinterxlab") == "https://interxlab.career.greetinghr.com"
    assert canonicalize_runtime_source_url("https://webzen.recruiter.co.kr/app/jobnotice/list") == "https://webzen.recruiter.co.kr"
    assert canonicalize_runtime_source_url("https://example.com/careers/openings") == "https://example.com/careers/openings"


def test_canonicalize_runtime_source_url_preserves_greetinghr_listing_paths() -> None:
    assert canonicalize_runtime_source_url("https://hyperaccel.career.greetinghr.com/ko/guide") == "https://hyperaccel.career.greetinghr.com/ko/guide"
    assert canonicalize_runtime_source_url("https://autocrypt.career.greetinghr.com/ko/positions") == "https://autocrypt.career.greetinghr.com/ko/positions"
    assert canonicalize_runtime_source_url("https://q-semi.career.greetinghr.com/ko/jobs") == "https://q-semi.career.greetinghr.com/ko/jobs"


def test_screen_sources_canonicalizes_and_dedupes_custom_greetinghr_urls() -> None:
    source_candidates = pd.DataFrame(
        [
            {
                "company_name": "인터엑스",
                "company_tier": "스타트업",
                "source_name": "인터엑스 GreetingHR Root",
                "source_url": "https://interxlab.career.greetinghr.com",
                "source_type": "greetinghr",
                "official_domain": "interxlab.career.greetinghr.com",
                "is_official_hint": True,
                "structure_hint": "html",
                "last_active_job_count": 4,
            },
            {
                "company_name": "인터엑스",
                "company_tier": "스타트업",
                "source_name": "인터엑스 GreetingHR About",
                "source_url": "https://interxlab.career.greetinghr.com/aboutinterxlab",
                "source_type": "greetinghr",
                "official_domain": "interxlab.career.greetinghr.com",
                "is_official_hint": True,
                "structure_hint": "html",
                "last_active_job_count": 1,
            },
        ]
    )

    approved, candidate, rejected, registry = screen_sources(source_candidates)

    assert candidate.empty
    assert rejected.empty
    assert len(approved) == 1
    assert len(registry) == 1
    assert registry.loc[0, "source_url"] == "https://interxlab.career.greetinghr.com"
    assert int(registry.loc[0, "last_active_job_count"]) == 4


def test_screen_sources_keeps_distinct_greetinghr_guide_path() -> None:
    source_candidates = pd.DataFrame(
        [
            {
                "company_name": "하이퍼엑셀",
                "company_tier": "스타트업",
                "source_name": "하이퍼엑셀 GreetingHR Root",
                "source_url": "https://hyperaccel.career.greetinghr.com",
                "source_type": "greetinghr",
                "official_domain": "hyperaccel.ai",
                "is_official_hint": True,
                "structure_hint": "next_data",
                "last_active_job_count": 0,
            },
            {
                "company_name": "하이퍼엑셀",
                "company_tier": "스타트업",
                "source_name": "하이퍼엑셀 GreetingHR Guide",
                "source_url": "https://hyperaccel.career.greetinghr.com/ko/guide",
                "source_type": "greetinghr",
                "official_domain": "hyperaccel.ai",
                "is_official_hint": True,
                "structure_hint": "next_data",
                "last_active_job_count": 13,
            },
        ]
    )

    approved, candidate, rejected, registry = screen_sources(source_candidates)

    assert candidate.empty
    assert rejected.empty
    assert len(approved) == 2
    assert set(registry["source_url"]) == {
        "https://hyperaccel.career.greetinghr.com",
        "https://hyperaccel.career.greetinghr.com/ko/guide",
    }


def test_screen_sources_approves_official_subdomain_html_source() -> None:
    source_candidates = pd.DataFrame(
        [
            {
                "company_name": "슈어소프트테크",
                "company_tier": "중견/중소",
                "source_name": "슈어소프트테크 채용공고",
                "source_url": "https://careers.suresofttech.com/job1",
                "source_type": "html_page",
                "official_domain": "suresofttech.com",
                "is_official_hint": True,
                "structure_hint": "html",
                "last_active_job_count": 0,
            }
        ]
    )

    approved, candidate, rejected, registry = screen_sources(source_candidates)

    assert rejected.empty
    assert candidate.empty
    assert len(approved) == 1
    assert registry.loc[0, "source_bucket"] == "approved"
    assert registry.loc[0, "source_url"] == "https://careers.suresofttech.com/job1"


def test_track_display_extracts_degree_and_special_track_from_detail_text() -> None:
    display = build_display_fields(
        {
            "company_name": "뷰노",
            "source_name": "GreetingHR",
            "job_title_raw": "AI Research Engineer / Scientist",
            "experience_level_raw": "",
            "job_role": "인공지능 리서처",
            "description_text": "전문연지원가능\n석사 이상 또는 이에 준하는 연구 경험이 필요합니다.",
            "requirements": "",
            "preferred": "",
            "main_tasks": "",
            "core_skills": "PyTorch",
        }
    )
    assert display["채용트랙_표시"] == "전문연구요원 / 석사"


def test_display_fields_merge_track_signals_from_detail_sections() -> None:
    display = build_display_fields(
        {
            "company_name": "이지케어텍",
            "source_name": "GreetingHR",
            "job_title_raw": "[경력] 연구소 MLOps Engineer 채용",
            "experience_level_raw": "",
            "job_role": "인공지능 엔지니어",
            "description_text": """
EMPLOYMENT CONDITIONS
계약직 12개월 (정규직 전환 가능)
지원자격
병역특례(전문연구요원) 지원 가능
""",
            "requirements": "",
            "preferred": "",
            "main_tasks": "",
            "core_skills": "PyTorch",
        }
    )
    assert display["채용트랙_표시"] == "전문연구요원 / 계약직"
    assert display["채용트랙근거_표시"] == "자격요건 / 상세본문"


def test_display_fields_do_not_confuse_intermediate_with_intern() -> None:
    display = build_display_fields(
        {
            "company_name": "쿠팡",
            "source_name": "쿠팡 채용",
            "job_title_raw": "Senior Data Analyst",
            "experience_level_raw": "",
            "job_role": "데이터 분석가",
            "requirements": "Intermediate in English communication and SQL 활용 경험이 필요합니다.",
            "preferred": "",
            "main_tasks": "데이터를 분석하고 리포트를 작성합니다.",
            "core_skills": "SQL",
        }
    )
    assert display["경력수준_표시"] == "시니어"
    assert display["채용트랙_표시"] == "일반채용"
    assert display["채용트랙근거_표시"] == "기본추론"


def test_display_fields_ignore_research_word_when_inferring_search_focus() -> None:
    display = build_display_fields(
        {
            "company_name": "뷰노",
            "source_name": "뷰노 채용",
            "job_title_raw": "AI Research Engineer",
            "experience_level_raw": "",
            "job_role": "인공지능 리서처",
            "requirements": "파이썬 활용 경험이 필요합니다.",
            "preferred": "",
            "main_tasks": "연구 실험을 설계하고 결과를 검증합니다.",
            "core_skills": "PyTorch",
        }
    )
    assert display["직무초점_표시"] == ""


def test_display_fields_surface_focus_from_body_sections() -> None:
    display = build_display_fields(
        {
            "company_name": "카카오모빌리티",
            "source_name": "카카오모빌리티 채용",
            "job_title_raw": "데이터 사이언티스트",
            "experience_level_raw": "",
            "job_role": "데이터 사이언티스트",
            "requirements": "모빌리티 서비스 데이터 분석 경험이 필요합니다.",
            "preferred": "경로 계획 최적화 또는 배차 문제 해결 경험 우대",
            "main_tasks": "모빌리티 수요 예측 모델을 개발합니다.",
            "core_skills": "Python",
        }
    )
    assert display["직무초점_표시"] == "모빌리티 / 시계열"
    assert display["직무초점근거_표시"] == "자격요건 / 주요업무"


def test_display_preserves_common_technical_terms_in_koreanized_form() -> None:
    analysis = build_analysis_fields(
        {
            "main_tasks": "SQL 활용 경험과 A/B 테스트 경험이 필요합니다.",
            "requirements": "Python 활용 경험",
            "preferred": "MLOps 경험 우대",
            "core_skills": "PyTorch, Spark",
            "description_text": "Data Pipeline 운영 경험",
        }
    )
    display = build_display_fields(
        {
            "company_name": "테스트회사",
            "source_name": "테스트 소스",
            "job_title_raw": "데이터 분석가",
            "job_role": "데이터 분석가",
            "experience_level_raw": "경력",
            "main_tasks": "SQL 활용 경험과 A/B 테스트 경험이 필요합니다.",
            "requirements": "Python 활용 경험",
            "preferred": "MLOps 경험 우대",
            "core_skills": "PyTorch, Spark",
        },
        analysis_fields=analysis,
    )
    assert "에스큐엘 활용 경험" in analysis["주요업무_분석용"]
    assert "파이썬 활용 경험" == analysis["자격요건_분석용"]
    assert "에스큐엘 활용 경험" in display["주요업무_표시"]
    assert "에이비 테스트 경험" in display["주요업무_표시"]
    assert display["자격요건_표시"] == "파이썬 활용 경험"
    assert "엠엘옵스 경험 우대" in display["우대사항_표시"]
    assert "파이토치" in display["핵심기술_표시"]
    assert "스파크" in display["핵심기술_표시"]


def test_analysis_prefers_korean_lines_when_section_is_bilingual() -> None:
    analysis = build_analysis_fields(
        {
            "main_tasks": (
                "Build data models and dashboards\n"
                "데이터 모델과 대시보드를 구축합니다.\n"
                "Analyze experiments\n"
                "실험 결과를 분석합니다."
            ),
            "description_text": "",
        }
    )
    assert "데이터 모델과 대시보드를 구축합니다." in analysis["주요업무_분석용"]
    assert "실험 결과를 분석합니다." in analysis["주요업무_분석용"]
    assert "Build data models" not in analysis["주요업무_분석용"]


def test_analysis_drops_english_only_main_task_lines_that_cause_display_leaks() -> None:
    analysis = build_analysis_fields(
        {
            "main_tasks": (
                "Leading research projects, possibly with a small team of applied scientists, "
                "as part of a group of cross-functional collaborators to evaluate the health "
                "of both internal and external components."
            ),
            "description_text": "",
        }
    )
    display = build_display_fields(
        {
            "company_name": "테스트회사",
            "source_name": "테스트소스",
            "job_title_raw": "응용 과학자",
            "job_role": "데이터 사이언티스트",
            "experience_level_raw": "경력",
            "main_tasks": (
                "Leading research projects, possibly with a small team of applied scientists, "
                "as part of a group of cross-functional collaborators to evaluate the health "
                "of both internal and external components."
            ),
            "requirements": "",
            "preferred": "",
            "core_skills": "",
        },
        analysis_fields=analysis,
    )
    frame = pd.DataFrame([display])
    assert analysis["주요업무_분석용"] == ""
    assert display["주요업무_표시"] == ""
    assert count_english_leaks(frame) == 0


def test_core_skills_are_inferred_from_requirements_and_description() -> None:
    analysis = build_analysis_fields(
        {
            "requirements": "Python, SQL, Airflow 활용 경험과 Spark 기반 데이터 처리 경험",
            "preferred": "AWS 환경 운영 경험 및 Docker 사용 경험 우대",
            "description_text": "LLM 기반 서비스와 RAG 파이프라인 구축 경험",
        }
    )
    assert "파이썬" in analysis["핵심기술_분석용"]
    assert "에스큐엘" in analysis["핵심기술_분석용"]
    assert "에어플로" in analysis["핵심기술_분석용"]
    assert "스파크" in analysis["핵심기술_분석용"]
    assert "에이더블유에스" in analysis["핵심기술_분석용"]
    assert "도커" in analysis["핵심기술_분석용"]
    assert "엘엘엠" in analysis["핵심기술_분석용"]
    assert "검색증강생성" in analysis["핵심기술_분석용"]


def test_core_skills_are_inferred_when_english_acronyms_touch_korean_particles() -> None:
    analysis = build_analysis_fields(
        {
            "description_text": "LLM에서는 스케일링 법칙을 검토하고 연구용 GPU클러스터를 운영합니다. 멀티모달 LLM 연구와 강화학습, 인과 추론, 게임 이론 기반 실험을 수행합니다.",
        }
    )
    assert "엘엘엠" in analysis["핵심기술_분석용"]
    assert "지피유" in analysis["핵심기술_분석용"]
    assert "멀티모달" in analysis["핵심기술_분석용"]
    assert "강화학습" in analysis["핵심기술_분석용"]
    assert "인과 추론" in analysis["핵심기술_분석용"]
    assert "게임 이론" in analysis["핵심기술_분석용"]


def test_english_prose_is_not_partially_transliterated_into_garbled_korean() -> None:
    text = (
        "We are looking for interns who can contribute to machine learning modeling and infrastructure. "
        "This includes TPU/GPU-based model training, scalable data pipelines, and monitoring."
    )
    assert sanitize_section_text(text) == ""
    assert sanitize_section_text(["Python", "SQL"]) == "파이썬\n에스큐엘"
    assert section_output_looks_noisy("머신러닝 지피유-,,") is True


def test_section_sanitizer_drops_legal_notice_and_heading_artifacts() -> None:
    text = """
쿠팡은 모두에게 공평한 기회를 제공합니다
쿠팡은 어느 누구에게나 열려 있는 회사입니다.
국가보훈대상자는 관계 법령에 따라 우대하오니, 해당되시는 분께서는 지원 시 고지해주시고 채용 시 증빙서류를 제출해주시기 바랍니다.
[BASIC]
[PREFERRED]
LLM Fine-Tuning:
실제 업무 설명은 아래와 같습니다.
모델을 설계하고 배포합니다.
""".strip()
    sanitized = sanitize_section_text(text)
    assert "공평한 기회" not in sanitized
    assert "국가보훈대상자" not in sanitized
    assert "비에이에스아이씨" not in sanitized
    assert "피알이에프이알알이디" not in sanitized
    assert "엘엘엠 파인튜닝:" not in sanitized
    assert "모델을 설계하고 배포합니다." in sanitized


def test_section_sanitizer_drops_cta_and_stops_before_benefits_tail() -> None:
    text = """
👨‍👦‍👦함께하게 될 팀을 소개합니다!
함께하게 될 팀원들이 궁금하다면 ?
자세히보기
담당업무
씨어스는
모델을 설계하고 배포합니다.
파이프라인을 운영합니다.
근무 조건
주 5일 근무
채용 절차
서류전형
""".strip()
    sanitized = sanitize_section_text(text)
    assert "소개합니다" not in sanitized
    assert "궁금하다면" not in sanitized
    assert "자세히보기" not in sanitized
    assert "씨어스는" not in sanitized
    assert "근무 조건" not in sanitized
    assert "서류전형" not in sanitized
    assert sanitized == "모델을 설계하고 배포합니다.\n파이프라인을 운영합니다."


def test_section_sanitizer_drops_hiring_admin_sections_without_spaced_headings() -> None:
    text = """
한국어 비즈니스 의사소통 능력이 있는 분
해외 여행에 결격 사유가 없는 분
채용전형
서류전형 > 직무 테스트 및 면접전형 > 처우 협의 > 최종 합격
근무조건
계약직
""".strip()

    sanitized = sanitize_section_text(text)

    assert "한국어 비즈니스 의사소통" in sanitized
    assert "채용전형" not in sanitized
    assert "서류전형" not in sanitized
    assert "근무조건" not in sanitized


def test_section_sanitizer_drops_application_document_and_reference_check_tails() -> None:
    text = """
국제 학회 및 저널 논문 출판 경험이 있는 분
의료 인공지능 프로젝트 경험이 있는 분
필수 – 이력서, 논문(석사 이상 학위 소지자)
선택 –, 포트폴리오 등
*(에 따라 레퍼런스체크 실시될 수 있음)
""".strip()

    sanitized = sanitize_section_text(text)

    assert "국제 학회 및 저널 논문 출판 경험" in sanitized
    assert "의료 인공지능 프로젝트 경험" in sanitized
    assert "이력서" not in sanitized
    assert "포트폴리오" not in sanitized
    assert "레퍼런스" not in sanitized


def test_section_sanitizer_drops_faq_lines_and_deduplicates_repeated_content() -> None:
    text = """
Senior Data Analyst 역할에서 핵심 의사결정을 지원합니다.
Senior Data Analyst 역할에서 핵심 의사결정을 지원합니다.
) 근무 형태는 어떻게 되나요?
주 5일 대면 근무입니다.
    ) 근무 시작일은 어떻게 되나요?
    개인별 조율이 가능합니다.
""".strip()
    sanitized = sanitize_section_text(text)
    assert "근무 형태" not in sanitized
    assert "근무 시작일" not in sanitized
    assert "개인별 조율" not in sanitized
    assert sanitized.count("핵심 의사결정을 지원합니다.") == 1


def test_core_skill_sanitizer_drops_experience_noise() -> None:
    text = """
Python
SQL
석사 학위 우대
5년 이상 경력
MLOps
협업
문제 해결
영어 의사소통
""".strip()
    assert sanitize_core_skill_text(text) == "파이썬\n에스큐엘\n엠엘옵스"


def test_section_output_is_substantive_rejects_short_heading_only_text() -> None:
    assert section_output_is_substantive("엘엘엠 파인튜닝:") is False
    assert section_output_is_substantive("[비에이에스아이씨 ]\n[피알이에프이알알이디 ]") is False
    assert section_output_is_substantive("(오오에스)\n파이썬\n태블로\n파워비아이\n인공지능") is False
    assert section_output_is_substantive("모델을 설계하고 배포합니다.\n실험 결과를 분석합니다.") is True


def test_build_analysis_fields_falls_back_to_sections_when_detail_is_removed() -> None:
    analysis = build_analysis_fields(
        {
            "main_tasks": "모델을 설계합니다.",
            "requirements": "파이썬 활용 경험이 필요합니다.",
            "preferred": "에이비 테스트 경험 우대",
            "description_text": "쿠팡은 모두에게 공평한 기회를 제공합니다",
        }
    )
    assert analysis["상세본문_분석용"] == "모델을 설계합니다.\n파이썬 활용 경험이 필요합니다.\n에이비 테스트 경험 우대"


def test_build_analysis_fields_extracts_sections_from_korean_job_detail_headings() -> None:
    analysis = build_analysis_fields(
        {
            "description_text": (
                "직무 상세\n"
                "최신 생체신호 관련 AI 기술 논문 서베이를 통한 연구 동향 파악\n"
                "생체신호 데이터를 활용한 질병 진단 인공지능 모델 개발 및 분석\n"
                "지원 자격\n"
                "PyTorch 등 딥러닝 프레임워크에 익숙하신 분\n"
                "석사 이상 학위 보유자\n"
                "우대사항\n"
                "SCI급 저널 논문 게재 경험\n"
            )
        }
    )
    assert "연구 동향 파악" in analysis["주요업무_분석용"]
    assert "파이토치" in analysis["자격요건_분석용"]
    assert "석사" in analysis["자격요건_분석용"]
    assert "에스씨아이급" in analysis["우대사항_분석용"]


def test_build_analysis_fields_prefers_structured_sections_over_intro_heavy_detail() -> None:
    analysis = build_analysis_fields(
        {
            "main_tasks": "모델을 설계합니다.\n파이프라인을 운영합니다.",
            "requirements": "파이썬 활용 경험이 필요합니다.",
            "preferred": "검색증강생성 경험 우대",
            "description_text": "팀 소개\n2024년 연구 성과를 소개합니다.\n자세히보기\n전형 절차",
        }
    )
    assert analysis["상세본문_분석용"] == "모델을 설계합니다.\n파이프라인을 운영합니다.\n파이썬 활용 경험이 필요합니다.\n검색증강생성 경험 우대"


def test_build_analysis_fields_skips_noisy_run_on_section_in_detail_fallback() -> None:
    analysis = build_analysis_fields(
        {
            "main_tasks": "모델을 설계합니다.\n파이프라인을 운영합니다.",
            "requirements": "에이아이엠엘도메인전반에대한이해와적어도한개이상의특정도메인에대한깊이있는지식을갖추고관련문제를해결할수있는분",
            "preferred": "검색증강생성 경험 우대",
            "description_text": "팀 소개\n인터뷰\n전형 절차",
        }
    )
    assert analysis["상세본문_분석용"] == "모델을 설계합니다.\n파이프라인을 운영합니다.\n검색증강생성 경험 우대"


def test_gemini_refinement_gate_only_triggers_for_lossy_sections() -> None:
    assert needs_gemini_refinement("SQL 활용 경험과 A/B 테스트 경험이 필요합니다.", "활용 경험") is True
    assert needs_gemini_refinement("Feature store 구축 경험이 필요합니다.", "구축 경험") is True
    assert needs_gemini_refinement(
        "We are looking for interns who can contribute to machine learning modeling and infrastructure.",
        "머신러닝 머신러닝 머신러닝",
    ) is True
    assert needs_gemini_refinement(
        "We build large-scale AI systems for production.",
        "Leading research projects, possibly with a small team of applied scientists, as part of a group of cross-functional collaborators to evaluate the health of both internal and external components to make sure the system is running safely and efficiently.",
    ) is True
    assert needs_gemini_refinement("서비스 지표를 분석합니다.", "서비스 지표를 분석합니다.") is False


def test_bootstrap_population_collection(sandbox_project: Path) -> None:
    discover_companies_pipeline(project_root=sandbox_project)
    discover_sources_pipeline(project_root=sandbox_project)
    summary = collect_jobs_pipeline(dry_run=False, project_root=sandbox_project)
    paths = ProjectPaths.from_root(sandbox_project)
    staging = read_csv_or_empty(paths.staging_jobs_path)
    assert summary["collected_job_count"] >= 6
    assert summary["collection_mode"] == "mock"
    assert paths.first_snapshot_path.exists()
    assert set(staging["job_role"]) >= {"데이터 분석가", "데이터 사이언티스트", "인공지능 리서처", "인공지능 엔지니어"}
    assert {"주요업무_분석용", "자격요건_분석용", "우대사항_분석용", "핵심기술_분석용", "상세본문_분석용"} <= set(staging.columns)


def test_company_screening_pipeline_creates_evidence_and_bucket_files(sandbox_project: Path) -> None:
    discover_companies_pipeline(project_root=sandbox_project)
    evidence_summary = collect_company_evidence_pipeline(project_root=sandbox_project)
    bucket_summary = screen_companies_pipeline(project_root=sandbox_project)
    paths = ProjectPaths.from_root(sandbox_project)

    candidates = read_csv_or_empty(paths.company_candidates_path)
    evidence = read_csv_or_empty(paths.company_evidence_path)
    approved = read_csv_or_empty(paths.approved_companies_path)
    candidate = read_csv_or_empty(paths.candidate_companies_path)
    rejected = read_csv_or_empty(paths.rejected_companies_path)

    assert evidence_summary["candidate_company_count"] >= 8
    assert evidence_summary["company_evidence_count"] > 0
    assert {"후보시드근거", "공식도메인", "공식채용소스", "타깃직무공고"} <= set(evidence["evidence_type"])
    assert {"approved", "candidate", "rejected"} & set(candidates["company_bucket"])
    assert bucket_summary["screened_company_count"] == len(candidates)
    assert bucket_summary["approved_company_count"] == len(approved)
    assert bucket_summary["candidate_company_count"] == len(candidate)
    assert bucket_summary["rejected_company_count"] == len(rejected)
    assert bucket_summary["approved_company_count"] >= 1
    assert approved["candidate_seed_url"].fillna("").astype(str).str.strip().ne("").all()
    assert evidence_summary["batch_count"] >= 1
    assert evidence_summary["completed_batch_count"] == evidence_summary["batch_count"]


def test_company_screening_source_summary_counts_structured_verified_sources() -> None:
    registry = pd.DataFrame(
        [
            {"company_name": "알파", "source_type": "greetinghr", "verification_status": "성공"},
            {"company_name": "알파", "source_type": "html_page", "verification_status": "성공"},
            {"company_name": "알파", "source_type": "html_page", "verification_status": "실패"},
        ]
    )

    summary = company_screening_module._source_summary(registry)

    assert summary["알파"]["source_seed_count"] == 3
    assert summary["알파"]["verified_source_count"] == 2
    assert summary["알파"]["verified_structured_source_count"] == 1


def test_company_bucket_approves_source_ready_company_without_active_jobs() -> None:
    assert company_screening_module._company_bucket(
        officiality_score=4,
        source_seed_count=1,
        verified_source_count=1,
        verified_structured_source_count=1,
        active_job_count=0,
        role_fit_score=0,
        has_candidate_seed_provenance=True,
    ) == ("approved", "")
    assert company_screening_module._company_bucket(
        officiality_score=4,
        source_seed_count=2,
        verified_source_count=2,
        verified_structured_source_count=0,
        active_job_count=0,
        role_fit_score=0,
        has_candidate_seed_provenance=True,
    ) == ("approved", "")
    assert company_screening_module._company_bucket(
        officiality_score=4,
        source_seed_count=1,
        verified_source_count=1,
        verified_structured_source_count=0,
        active_job_count=0,
        role_fit_score=0,
        has_candidate_seed_provenance=True,
    )[0] == "candidate"


def test_company_screening_pipeline_supports_batched_evidence_collection(sandbox_project: Path) -> None:
    discover_companies_pipeline(project_root=sandbox_project)
    evidence_summary = collect_company_evidence_pipeline(project_root=sandbox_project, batch_size=2, max_batches=1)
    paths = ProjectPaths.from_root(sandbox_project)

    in_progress_candidates = read_csv_or_empty(paths.runtime_dir / "company_candidates_in_progress.csv")
    in_progress_evidence = read_csv_or_empty(paths.runtime_dir / "company_evidence_in_progress.csv")

    assert evidence_summary["batch_size"] == 2
    assert evidence_summary["batch_count"] == 1
    assert evidence_summary["completed_batch_count"] == 1
    assert Path(evidence_summary["checkpoint_dir"]).exists()
    assert evidence_summary["start_offset"] == 0
    assert evidence_summary["next_offset"] > 0
    assert evidence_summary["completed_full_scan"] is False
    assert evidence_summary["published_company_state"] is False
    assert len(in_progress_candidates) == evidence_summary["candidate_company_count"]
    assert len(in_progress_evidence) == evidence_summary["company_evidence_count"]
    assert read_csv_or_empty(paths.company_candidates_path).empty

    resumed_summary = collect_company_evidence_pipeline(project_root=sandbox_project, batch_size=2, max_batches=1)
    assert resumed_summary["start_offset"] == evidence_summary["next_offset"]
    assert resumed_summary["completed_batch_count"] == 1


def test_company_screening_pipeline_resumes_when_candidate_count_changes(sandbox_project: Path) -> None:
    discover_companies_pipeline(project_root=sandbox_project)
    first_summary = collect_company_evidence_pipeline(project_root=sandbox_project, batch_size=2, max_batches=1)
    paths = ProjectPaths.from_root(sandbox_project)

    registry = read_csv_or_empty(paths.companies_registry_path, IMPORT_COMPANY_COLUMNS)
    sorted_registry = registry.sort_values(by=["company_tier", "company_name"], ascending=[True, True]).reset_index(drop=True)
    tail_tier = sorted_registry.iloc[-1]["company_tier"]

    new_row = sorted_registry.iloc[0].copy()
    new_row["company_name"] = "힣테스트증분회사"
    new_row["company_tier"] = tail_tier
    new_row["candidate_seed_url"] = "https://example.com/catalog"
    new_row["candidate_seed_reason"] = "증분 재개 테스트"
    new_row["official_domain"] = "example.com"
    updated_registry = pd.concat([registry, pd.DataFrame([new_row])], ignore_index=True)
    write_csv(updated_registry[list(IMPORT_COMPANY_COLUMNS)], paths.companies_registry_path)

    resumed_summary = collect_company_evidence_pipeline(project_root=sandbox_project, batch_size=2, max_batches=1)
    assert resumed_summary["start_offset"] == first_summary["next_offset"]
    progress = json.loads(paths.company_evidence_progress_path.read_text(encoding="utf-8"))
    assert progress["last_company_name"]


def test_quality_gate_rejects_duplicate_job_urls_and_impossible_experience() -> None:
    staging = pd.DataFrame(
        [
            {
                "company_name": "이지케어텍",
                "source_name": "GreetingHR",
                "job_title_raw": "[경력] AI Engineer",
                "job_title_ko": "[경력] AI Engineer",
                "job_role": "인공지능 엔지니어",
                "job_url": "https://example.com/jobs/1",
                "source_url": "https://example.com/feed",
                "source_bucket": "approved",
                "company_tier": "중견/중소",
                "record_status": "신규",
                "is_active": True,
                "주요업무_분석용": "모델을 설계합니다.\n파이프라인을 운영합니다.",
                "자격요건_분석용": "파이썬 활용 경험이 필요합니다.",
                "우대사항_분석용": "검색증강생성 경험 우대",
                "핵심기술_분석용": "파이썬\n파이토치",
                "상세본문_분석용": "모델을 설계합니다.\n파이프라인을 운영합니다.\n파이썬 활용 경험이 필요합니다.",
                "경력수준_표시": "경력 2026년+",
                "경력근거_표시": "자격요건",
                "채용트랙_표시": "",
                "채용트랙근거_표시": "",
                "구분요약_표시": "경력 2026년+",
                "회사명_표시": "이지케어텍",
                "소스명_표시": "GreetingHR",
                "공고제목_표시": "[경력] AI Engineer",
                "직무명_표시": "인공지능 엔지니어",
                "주요업무_표시": "모델을 설계합니다.\n파이프라인을 운영합니다.",
                "자격요건_표시": "파이썬 활용 경험이 필요합니다.",
                "우대사항_표시": "검색증강생성 경험 우대",
                "핵심기술_표시": "파이썬\n파이토치",
            },
            {
                "company_name": "이지케어텍 (주)",
                "source_name": "GreetingHR",
                "job_title_raw": "[경력] AI Engineer",
                "job_title_ko": "[경력] AI Engineer",
                "job_role": "인공지능 엔지니어",
                "job_url": "https://example.com/jobs/1",
                "source_url": "https://example.com/feed",
                "source_bucket": "approved",
                "company_tier": "중견/중소",
                "record_status": "신규",
                "is_active": True,
                "주요업무_분석용": "모델을 설계합니다.\n파이프라인을 운영합니다.",
                "자격요건_분석용": "파이썬 활용 경험이 필요합니다.",
                "우대사항_분석용": "검색증강생성 경험 우대",
                "핵심기술_분석용": "파이썬\n파이토치",
                "상세본문_분석용": "모델을 설계합니다.\n파이프라인을 운영합니다.\n파이썬 활용 경험이 필요합니다.",
                "경력수준_표시": "경력",
                "경력근거_표시": "구조화 메타데이터",
                "채용트랙_표시": "",
                "채용트랙근거_표시": "",
                "구분요약_표시": "경력",
                "회사명_표시": "이지케어텍 (주)",
                "소스명_표시": "GreetingHR",
                "공고제목_표시": "[경력] AI Engineer",
                "직무명_표시": "인공지능 엔지니어",
                "주요업무_표시": "모델을 설계합니다.\n파이프라인을 운영합니다.",
                "자격요건_표시": "파이썬 활용 경험이 필요합니다.",
                "우대사항_표시": "검색증강생성 경험 우대",
                "핵심기술_표시": "파이썬\n파이토치",
            },
        ]
    )
    registry = pd.DataFrame(
        [
            {
                "source_url": "https://example.com/feed",
                "source_bucket": "approved",
                "verification_status": "성공",
            }
        ]
    )
    gate = evaluate_quality_gate(staging, registry)
    assert gate.passed is False
    assert "동일 job_url 중복 공고가 존재합니다." in gate.reasons


def test_quality_gate_rejects_high_blank_position_summary_ratio() -> None:
    rows = []
    for idx in range(10):
        rows.append(
            {
                "company_name": f"회사{idx}",
                "source_name": "소스",
                "job_title_raw": f"공고 {idx}",
                "job_title_ko": f"공고 {idx}",
                "job_role": "인공지능 엔지니어",
                "job_url": f"https://example.com/jobs/{idx}",
                "source_url": "https://example.com/feed",
                "source_bucket": "approved",
                "company_tier": "중견/중소",
                "record_status": "신규",
                "is_active": True,
                "주요업무_분석용": "모델 개발을 담당합니다.",
                "자격요건_분석용": "파이썬 활용 경험이 필요합니다.",
                "우대사항_분석용": "",
                "핵심기술_분석용": "파이썬\n파이토치",
                "상세본문_분석용": "모델 개발을 담당합니다.\n파이썬 활용 경험이 필요합니다.",
                "경력수준_표시": "",
                "경력근거_표시": "",
                "채용트랙_표시": "",
                "채용트랙근거_표시": "",
                "직무초점_표시": "",
                "직무초점근거_표시": "",
                "구분요약_표시": "" if idx < 2 else "LLM",
                "회사명_표시": f"회사{idx}",
                "소스명_표시": "소스",
                "공고제목_표시": f"공고 {idx}",
                "직무명_표시": "인공지능 엔지니어",
                "주요업무_표시": "모델 개발을 담당합니다.",
                "자격요건_표시": "파이썬 활용 경험이 필요합니다.",
                "우대사항_표시": "",
                "핵심기술_표시": "파이썬\n파이토치",
            }
        )
    staging = pd.DataFrame(rows)
    registry = pd.DataFrame(
        [
            {
                "source_url": "https://example.com/feed",
                "source_bucket": "approved",
                "verification_status": "성공",
            }
        ]
    )
    gate = evaluate_quality_gate(staging, registry)
    assert gate.passed is False
    assert "구분요약 표시값 공란 비율이 너무 높습니다." in gate.reasons


def test_quality_gate_rejects_high_blank_focus_ratio_even_when_preferred_display_defaults() -> None:
    rows = []
    for idx in range(10):
        rows.append(
            {
                "company_name": f"회사{idx}",
                "source_name": "소스",
                "job_title_raw": f"공고 {idx}",
                "job_title_ko": f"공고 {idx}",
                "job_role": "인공지능 엔지니어",
                "job_url": f"https://example.com/jobs/pf-{idx}",
                "source_url": "https://example.com/feed",
                "source_bucket": "approved",
                "company_tier": "중견/중소",
                "record_status": "신규",
                "is_active": True,
                "주요업무_분석용": "모델 개발을 담당합니다.",
                "자격요건_분석용": "파이썬 활용 경험이 필요합니다.",
                "우대사항_분석용": "" if idx < 4 else "추천 시스템 경험 우대",
                "핵심기술_분석용": "파이썬\n파이토치",
                "상세본문_분석용": "모델 개발을 담당합니다.\n파이썬 활용 경험이 필요합니다.",
                "경력수준_표시": "경력",
                "경력근거_표시": "구조화 메타데이터",
                "채용트랙_표시": "",
                "채용트랙근거_표시": "",
                "직무초점_표시": "" if idx < 3 else "추천",
                "직무초점근거_표시": "" if idx < 3 else "자격요건",
                "구분요약_표시": "경력 / 추천",
                "회사명_표시": f"회사{idx}",
                "소스명_표시": "소스",
                "공고제목_표시": f"공고 {idx}",
                "직무명_표시": "인공지능 엔지니어",
                "주요업무_표시": "모델 개발을 담당합니다.",
                "자격요건_표시": "파이썬 활용 경험이 필요합니다.",
                "우대사항_표시": "" if idx < 4 else "추천 시스템 경험 우대",
                "핵심기술_표시": "파이썬\n파이토치",
            }
        )
    staging = pd.DataFrame(rows)
    registry = pd.DataFrame(
        [
            {
                "source_url": "https://example.com/feed",
                "source_bucket": "approved",
                "verification_status": "성공",
            }
        ]
    )
    gate = evaluate_quality_gate(staging, registry)
    assert gate.passed is False
    assert "우대사항 표시값 공란 비율이 너무 높습니다." in gate.reasons
    assert "직무초점 표시값 공란 비율이 너무 높습니다." in gate.reasons


def test_normalize_job_analysis_fields_infers_degree_track_from_requirements() -> None:
    staging = pd.DataFrame(
        [
            {
                "company_name": "테스트랩",
                "source_name": "소스",
                "job_title_raw": "AI Research Engineer",
                "job_title_ko": "AI Research Engineer",
                "job_role": "인공지능 리서처",
                "job_url": "https://example.com/jobs/research",
                "source_url": "https://example.com/feed",
                "source_bucket": "approved",
                "company_tier": "중견/중소",
                "record_status": "신규",
                "is_active": True,
                "주요업무_분석용": "모델 연구를 수행합니다.",
                "자격요건_분석용": "관련 분야 석사 학위 이상 또는 이에 준하는 연구 경험이 필요합니다.",
                "우대사항_분석용": "",
                "핵심기술_분석용": "파이썬\n파이토치",
                "상세본문_분석용": "관련 분야 석사 학위 이상 또는 이에 준하는 연구 경험이 필요합니다.",
            }
        ]
    )

    normalized = normalize_job_analysis_fields(staging)

    assert normalized.iloc[0]["채용트랙_표시"] == "석사"
    assert normalized.iloc[0]["채용트랙근거_표시"] == "자격요건"


def test_quality_gate_reports_quality_score_and_track_cue_metrics() -> None:
    rows = []
    for idx in range(10):
        rows.append(
            {
                "company_name": f"회사{idx}",
                "source_name": "소스",
                "job_title_raw": f"AI Research Engineer {idx}",
                "job_title_ko": f"AI Research Engineer {idx}",
                "job_role": "인공지능 리서처",
                "job_url": f"https://example.com/jobs/track-{idx}",
                "source_url": "https://example.com/feed",
                "source_bucket": "approved",
                "company_tier": "중견/중소",
                "record_status": "신규",
                "is_active": True,
                "주요업무_분석용": "모델 연구를 수행합니다.",
                "자격요건_분석용": "관련 분야 석사 학위 이상 또는 이에 준하는 연구 경험이 필요합니다.",
                "우대사항_분석용": "",
                "핵심기술_분석용": "파이썬\n파이토치",
                "상세본문_분석용": "관련 분야 석사 학위 이상 또는 이에 준하는 연구 경험이 필요합니다.",
                "경력수준_표시": "경력",
                "경력근거_표시": "구조화 메타데이터",
                "채용트랙_표시": "" if idx < 8 else "석사",
                "채용트랙근거_표시": "" if idx < 8 else "자격요건",
                "직무초점_표시": "LLM",
                "직무초점근거_표시": "자격요건",
                "구분요약_표시": "경력 / LLM",
                "회사명_표시": f"회사{idx}",
                "소스명_표시": "소스",
                "공고제목_표시": f"AI Research Engineer {idx}",
                "직무명_표시": "인공지능 리서처",
                "주요업무_표시": "모델 연구를 수행합니다.",
                "자격요건_표시": "관련 분야 석사 학위 이상 또는 이에 준하는 연구 경험이 필요합니다.",
                "우대사항_표시": "",
                "핵심기술_표시": "파이썬\n파이토치",
            }
        )
    staging = pd.DataFrame(rows)
    registry = pd.DataFrame(
        [
            {
                "source_url": "https://example.com/feed",
                "source_bucket": "approved",
                "verification_status": "성공",
            }
        ]
    )

    gate = evaluate_quality_gate(staging, registry)

    assert gate.passed is False
    assert "quality_score_100" in gate.metrics
    assert "quality_score_penalties" in gate.metrics
    assert "hiring_track_cue_blank_count" in gate.metrics
    assert "experience_placeholder_ratio" in gate.metrics
    assert "preferred_placeholder_ratio" in gate.metrics
    assert gate.metrics["hiring_track_cue_total"] == 10
    assert gate.metrics["quality_score_100"] < gate.metrics["quality_score_target"]


def test_quality_gate_reports_semantic_placeholder_ratios() -> None:
    staging = pd.DataFrame(
        [
            {
                "company_name": "테스트랩",
                "source_name": "소스",
                "job_title_raw": "AI Engineer",
                "job_title_ko": "AI Engineer",
                "job_role": "인공지능 엔지니어",
                "job_url": "https://example.com/jobs/semantic-1",
                "source_url": "https://example.com/feed",
                "source_bucket": "approved",
                "company_tier": "중견/중소",
                "record_status": "신규",
                "is_active": True,
                "주요업무_분석용": "모델 개발을 담당합니다.",
                "자격요건_분석용": "파이썬 활용 경험이 필요합니다.",
                "우대사항_분석용": "",
                "핵심기술_분석용": "파이썬\n파이토치",
                "상세본문_분석용": "모델 개발을 담당합니다.\n파이썬 활용 경험이 필요합니다.",
                "경력수준_표시": "",
                "우대사항_표시": "별도 우대사항 미기재",
                "직무초점_표시": "모델링",
                "구분요약_표시": "모델링",
                "주요업무_표시": "모델 개발을 담당합니다.",
                "자격요건_표시": "파이썬 활용 경험이 필요합니다.",
            }
        ]
    )
    registry = pd.DataFrame(
        [
            {
                "source_url": "https://example.com/feed",
                "source_bucket": "approved",
                "verification_status": "성공",
            }
        ]
    )

    gate = evaluate_quality_gate(staging, registry, already_filtered=True)

    assert gate.metrics["experience_placeholder_ratio"] == 1.0
    assert gate.metrics["preferred_placeholder_ratio"] == 1.0
    assert gate.metrics["preferred_blank_ratio"] == 1.0
    assert "경력수준 표시값 미기재 비율이 너무 높습니다." in gate.reasons


def test_normalize_job_analysis_fields_sanitizes_visible_sections_from_analysis_text() -> None:
    staging = pd.DataFrame(
        [
            {
                "company_name": "몰로코",
                "source_name": "몰로코 채용",
                "job_title_raw": "Senior Applied Scientist",
                "job_title_ko": "Senior Applied Scientist",
                "job_role": "데이터 사이언티스트",
                "job_url": "https://example.com/jobs/english-leak",
                "source_url": "https://example.com/feed",
                "source_bucket": "approved",
                "company_tier": "스타트업",
                "record_status": "유지",
                "is_active": True,
                "주요업무_분석용": (
                    "Leading research projects with cross-functional collaborators to evaluate "
                    "system health and drive infrastructure improvements."
                ),
                "자격요건_분석용": "",
                "우대사항_분석용": "",
                "핵심기술_분석용": "",
                "상세본문_분석용": "",
                "회사명_표시": "몰로코",
                "소스명_표시": "몰로코 채용",
                "공고제목_표시": "Senior Applied Scientist",
                "경력수준_표시": "미기재",
                "경력근거_표시": "",
                "채용트랙_표시": "일반채용",
                "채용트랙근거_표시": "",
                "직무초점_표시": "",
                "직무초점근거_표시": "",
                "구분요약_표시": "",
                "직무명_표시": "데이터 사이언티스트",
                "주요업무_표시": (
                    "Leading research projects with cross-functional collaborators to evaluate "
                    "system health and drive infrastructure improvements."
                ),
                "자격요건_표시": "",
                "우대사항_표시": "",
                "핵심기술_표시": "",
            }
        ]
    )
    registry = pd.DataFrame(
        [
            {
                "source_url": "https://example.com/feed",
                "source_bucket": "approved",
                "verification_status": "성공",
            }
        ]
    )

    normalized = normalize_job_analysis_fields(staging)
    gate = evaluate_quality_gate(normalized, registry, already_filtered=True)

    assert normalized.iloc[0]["주요업무_표시"] == ""
    assert gate.metrics["english_leak_count"] == 0


def test_normalize_job_analysis_fields_truncates_cross_column_and_admin_tails() -> None:
    staging = pd.DataFrame(
        [
            {
                "company_name": "테스트회사",
                "source_name": "테스트 채용",
                "job_title_raw": "Data Engineer",
                "job_title_ko": "Data Engineer",
                "job_role": "인공지능 엔지니어",
                "job_url": "https://example.com/jobs/data-engineer",
                "source_url": "https://example.com/feed",
                "source_bucket": "approved",
                "company_tier": "스타트업",
                "record_status": "유지",
                "is_active": True,
                "주요업무_분석용": "데이터레이크 구축 및 운영 업무를 담당합니다.\n자격요건\n파이썬 개발 경험",
                "자격요건_분석용": "파이썬 개발 경험\n우대조건\n대규모 데이터 처리 경험",
                "우대사항_분석용": (
                    "의료 인공지능 프로젝트 경험\n"
                    "필수 – 이력서, 논문(석사 이상 학위 소지자)\n"
                    "선택 –, 포트폴리오 등\n"
                    "*(에 따라 레퍼런스체크 실시될 수 있음)"
                ),
                "핵심기술_분석용": "Python\nSQL",
                "상세본문_분석용": "",
                "회사명_표시": "테스트회사",
                "소스명_표시": "테스트 채용",
                "공고제목_표시": "Data Engineer",
                "경력수준_표시": "",
                "경력근거_표시": "",
                "채용트랙_표시": "",
                "채용트랙근거_표시": "",
                "직무초점_표시": "",
                "직무초점근거_표시": "",
                "구분요약_표시": "",
                "직무명_표시": "인공지능 엔지니어",
                "주요업무_표시": "",
                "자격요건_표시": "",
                "우대사항_표시": "",
                "핵심기술_표시": "",
            }
        ]
    )

    normalized = normalize_job_analysis_fields(staging)
    row = normalized.iloc[0]

    assert row["주요업무_분석용"] == "데이터레이크 구축 및 운영 업무를 담당합니다."
    assert row["자격요건_분석용"] == "파이썬 개발 경험"
    assert row["우대사항_분석용"] == "의료 인공지능 프로젝트 경험"
    assert "자격요건" not in row["주요업무_분석용"]
    assert "우대조건" not in row["자격요건_분석용"]
    assert "이력서" not in row["우대사항_분석용"]
    assert "레퍼런스" not in row["우대사항_분석용"]
    assert row["주요업무_표시"] == "데이터레이크 구축 및 운영 업무를 담당합니다."


def test_discover_sources_prefers_approved_companies_when_bucket_exists(sandbox_project: Path) -> None:
    discover_companies_pipeline(project_root=sandbox_project)
    collect_company_evidence_pipeline(project_root=sandbox_project)
    screen_companies_pipeline(project_root=sandbox_project)
    paths = ProjectPaths.from_root(sandbox_project)
    existing_registry = read_csv_or_empty(paths.source_registry_path)
    existing_companies = set(existing_registry["company_name"].dropna().tolist())

    approved = read_csv_or_empty(paths.approved_companies_path)
    samsung_only = approved[approved["company_name"] == "삼성전자"].copy()
    assert not samsung_only.empty
    samsung_only.to_csv(paths.approved_companies_path, index=False)

    summary = discover_sources_pipeline(project_root=sandbox_project)
    registry = read_csv_or_empty(paths.source_registry_path)

    assert summary["company_input_mode"] == "approved_companies"
    registry_companies = set(registry["company_name"].dropna().tolist())
    assert "삼성전자" in registry_companies
    assert existing_companies.issubset(registry_companies)


def test_sheet_tabs_include_company_registry_and_evidence(sandbox_project: Path) -> None:
    discover_companies_pipeline(project_root=sandbox_project)
    collect_company_evidence_pipeline(project_root=sandbox_project)
    screen_companies_pipeline(project_root=sandbox_project)
    paths = ProjectPaths.from_root(sandbox_project)

    tabs = build_sheet_tabs(paths)
    exported = export_tabs_locally(paths, tabs, "staging")

    assert "기업선정 탭" in tabs
    assert "기업근거 탭" in tabs
    assert "기업버킷" in tabs["기업선정 탭"].columns
    assert "근거유형" in tabs["기업근거 탭"].columns
    assert any(path.endswith("기업선정_탭.csv") for path in exported)
    assert any(path.endswith("기업근거_탭.csv") for path in exported)


def test_sync_tabs_to_google_sheets_sets_client_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    timeout_calls: list[tuple[float, float] | None] = []
    updates: list[str] = []
    calls: list[str] = []

    class FakeWorksheet:
        def __init__(self, title: str) -> None:
            self.title = title
            self.row_count = 100
            self.col_count = 20

        def clear(self) -> None:
            calls.append(f"{self.title}:clear")

        def update(self, values) -> None:
            calls.append(f"{self.title}:update")
            updates.append(self.title)

        def resize(self, rows: int | None = None, cols: int | None = None) -> None:
            if rows is not None:
                self.row_count = rows
            if cols is not None:
                self.col_count = cols

    class WorksheetNotFound(Exception):
        pass

    class FakeSpreadsheet:
        def worksheet(self, tab_name: str):
            return FakeWorksheet(tab_name)

        def add_worksheet(self, title: str, rows: int, cols: int):
            return FakeWorksheet(title)

    class FakeClient:
        def set_timeout(self, timeout=None) -> None:
            timeout_calls.append(timeout)

        def open_by_key(self, key: str):
            return FakeSpreadsheet()

    fake_gspread = types.SimpleNamespace(
        authorize=lambda credentials: FakeClient(),
        WorksheetNotFound=WorksheetNotFound,
    )

    class FakeCredentials:
        @classmethod
        def from_service_account_info(cls, info, scopes=None):
            return object()

    fake_service_account_module = types.SimpleNamespace(Credentials=FakeCredentials)

    monkeypatch.setitem(sys.modules, "gspread", fake_gspread)
    monkeypatch.setitem(sys.modules, "google", types.ModuleType("google"))
    monkeypatch.setitem(sys.modules, "google.oauth2", types.ModuleType("google.oauth2"))
    monkeypatch.setitem(sys.modules, "google.oauth2.service_account", fake_service_account_module)

    settings = AppSettings(
        google_sheets_spreadsheet_id="spreadsheet-id",
        google_service_account_json=json.dumps(
            {
                "type": "service_account",
                "client_email": "svc@example.com",
                "private_key": "-----BEGIN PRIVATE KEY-----\\nabc\\n-----END PRIVATE KEY-----\\n",
                "token_uri": "https://oauth2.googleapis.com/token",
            },
            ensure_ascii=False,
        ),
        google_sheets_connect_timeout_seconds=3,
        google_sheets_timeout_seconds=12,
    )

    synced = sync_tabs_to_google_sheets({"기업선정 탭": pd.DataFrame([{"기업명": "테스트"}])}, settings)

    assert synced is True
    assert timeout_calls == [(3.0, 12.0)]
    assert "기업선정 탭" in updates
    assert calls[:2] == ["기업선정 탭:clear", "기업선정 탭:update"]


def test_sync_tabs_to_google_sheets_truncates_oversized_cells(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_values: dict[str, list[list[str]]] = {}

    class FakeWorksheet:
        def __init__(self, title: str) -> None:
            self.title = title
            self.row_count = 100
            self.col_count = 20

        def clear(self) -> None:
            return None

        def update(self, values) -> None:
            captured_values[self.title] = values

        def resize(self, rows: int | None = None, cols: int | None = None) -> None:
            if rows is not None:
                self.row_count = rows
            if cols is not None:
                self.col_count = cols

    class WorksheetNotFound(Exception):
        pass

    class FakeSpreadsheet:
        def worksheet(self, tab_name: str):
            return FakeWorksheet(tab_name)

        def add_worksheet(self, title: str, rows: int, cols: int):
            return FakeWorksheet(title)

    class FakeClient:
        def set_timeout(self, timeout=None) -> None:
            return None

        def open_by_key(self, key: str):
            return FakeSpreadsheet()

    fake_gspread = types.SimpleNamespace(
        authorize=lambda credentials: FakeClient(),
        WorksheetNotFound=WorksheetNotFound,
    )

    class FakeCredentials:
        @classmethod
        def from_service_account_info(cls, info, scopes=None):
            return object()

    fake_service_account_module = types.SimpleNamespace(Credentials=FakeCredentials)

    monkeypatch.setitem(sys.modules, "gspread", fake_gspread)
    monkeypatch.setitem(sys.modules, "google", types.ModuleType("google"))
    monkeypatch.setitem(sys.modules, "google.oauth2", types.ModuleType("google.oauth2"))
    monkeypatch.setitem(sys.modules, "google.oauth2.service_account", fake_service_account_module)

    settings = AppSettings(
        google_sheets_spreadsheet_id="spreadsheet-id",
        google_service_account_json=json.dumps(
            {
                "type": "service_account",
                "client_email": "svc@example.com",
                "private_key": "-----BEGIN PRIVATE KEY-----\\nabc\\n-----END PRIVATE KEY-----\\n",
                "token_uri": "https://oauth2.googleapis.com/token",
            },
            ensure_ascii=False,
        ),
    )

    very_long = "x" * 60000
    synced = sync_tabs_to_google_sheets({"기업선정 탭": pd.DataFrame([{"기업명": very_long}])}, settings)

    assert synced is True
    uploaded = captured_values["기업선정 탭"][1][0]
    assert len(uploaded) == 49000


def test_sync_tabs_to_google_sheets_retries_retryable_update_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    update_attempts: list[int] = []
    sleep_calls: list[float] = []

    class FakeWorksheet:
        def __init__(self, title: str) -> None:
            self.title = title
            self.row_count = 100
            self.col_count = 20

        def clear(self) -> None:
            return None

        def update(self, values) -> None:
            update_attempts.append(1)
            if len(update_attempts) == 1:
                raise TimeoutError("read timed out")

        def resize(self, rows: int | None = None, cols: int | None = None) -> None:
            if rows is not None:
                self.row_count = rows
            if cols is not None:
                self.col_count = cols

    class WorksheetNotFound(Exception):
        pass

    class FakeSpreadsheet:
        def worksheet(self, tab_name: str):
            return FakeWorksheet(tab_name)

        def add_worksheet(self, title: str, rows: int, cols: int):
            return FakeWorksheet(title)

    class FakeClient:
        def set_timeout(self, timeout=None) -> None:
            return None

        def open_by_key(self, key: str):
            return FakeSpreadsheet()

    fake_gspread = types.SimpleNamespace(
        authorize=lambda credentials: FakeClient(),
        WorksheetNotFound=WorksheetNotFound,
    )

    class FakeCredentials:
        @classmethod
        def from_service_account_info(cls, info, scopes=None):
            return object()

    fake_service_account_module = types.SimpleNamespace(Credentials=FakeCredentials)

    monkeypatch.setitem(sys.modules, "gspread", fake_gspread)
    monkeypatch.setitem(sys.modules, "google", types.ModuleType("google"))
    monkeypatch.setitem(sys.modules, "google.oauth2", types.ModuleType("google.oauth2"))
    monkeypatch.setitem(sys.modules, "google.oauth2.service_account", fake_service_account_module)
    monkeypatch.setattr("jobs_market_v2.sheets.sleep", lambda seconds: sleep_calls.append(seconds))

    settings = AppSettings(
        google_sheets_spreadsheet_id="spreadsheet-id",
        google_service_account_json=json.dumps(
            {
                "type": "service_account",
                "client_email": "svc@example.com",
                "private_key": "-----BEGIN PRIVATE KEY-----\\nabc\\n-----END PRIVATE KEY-----\\n",
                "token_uri": "https://oauth2.googleapis.com/token",
            },
            ensure_ascii=False,
        ),
    )

    synced = sync_tabs_to_google_sheets({"기업선정 탭": pd.DataFrame([{"기업명": "테스트"}])}, settings)

    assert synced is True
    assert len(update_attempts) >= 2
    assert sleep_calls == [1.0]


def test_sync_tabs_to_google_sheets_updates_only_selected_tabs(monkeypatch: pytest.MonkeyPatch) -> None:
    updates: list[str] = []

    class FakeWorksheet:
        def __init__(self, title: str) -> None:
            self.title = title
            self.row_count = 100
            self.col_count = 20

        def clear(self) -> None:
            return None

        def update(self, values) -> None:
            updates.append(self.title)

        def resize(self, rows: int | None = None, cols: int | None = None) -> None:
            if rows is not None:
                self.row_count = rows
            if cols is not None:
                self.col_count = cols

    class WorksheetNotFound(Exception):
        pass

    class FakeSpreadsheet:
        def worksheet(self, tab_name: str):
            return FakeWorksheet(tab_name)

        def add_worksheet(self, title: str, rows: int, cols: int):
            return FakeWorksheet(title)

    class FakeClient:
        def set_timeout(self, timeout=None) -> None:
            return None

        def open_by_key(self, key: str):
            return FakeSpreadsheet()

    fake_gspread = types.SimpleNamespace(
        authorize=lambda credentials: FakeClient(),
        WorksheetNotFound=WorksheetNotFound,
    )

    class FakeCredentials:
        @classmethod
        def from_service_account_info(cls, info, scopes=None):
            return object()

    fake_service_account_module = types.SimpleNamespace(Credentials=FakeCredentials)

    monkeypatch.setitem(sys.modules, "gspread", fake_gspread)
    monkeypatch.setitem(sys.modules, "google", types.ModuleType("google"))
    monkeypatch.setitem(sys.modules, "google.oauth2", types.ModuleType("google.oauth2"))
    monkeypatch.setitem(sys.modules, "google.oauth2.service_account", fake_service_account_module)

    settings = AppSettings(
        google_sheets_spreadsheet_id="spreadsheet-id",
        google_service_account_json=json.dumps(
            {
                "type": "service_account",
                "client_email": "svc@example.com",
                "private_key": "-----BEGIN PRIVATE KEY-----\\nabc\\n-----END PRIVATE KEY-----\\n",
                "token_uri": "https://oauth2.googleapis.com/token",
            },
            ensure_ascii=False,
        ),
    )

    synced = sync_tabs_to_google_sheets(
        {
            "기업선정 탭": pd.DataFrame([{"기업명": "테스트"}]),
            "staging 탭": pd.DataFrame([{"회사명": "테스트"}]),
        },
        settings,
        tab_names=["staging 탭"],
    )

    assert synced is True
    assert updates == ["staging 탭"]


def test_incremental_update_pipeline_marks_changed_and_missing(sandbox_project: Path) -> None:
    discover_companies_pipeline(project_root=sandbox_project)
    discover_sources_pipeline(project_root=sandbox_project)
    collect_jobs_pipeline(dry_run=False, project_root=sandbox_project)
    promote_staging_pipeline(project_root=sandbox_project)

    samsung_fixture = sandbox_project / "tests" / "fixtures" / "sources" / "samsung_jobs.json"
    samsung_fixture.write_text(json.dumps({"jobs": []}, ensure_ascii=False), encoding="utf-8")

    naver_fixture = sandbox_project / "tests" / "fixtures" / "sources" / "naver_jobs.html"
    naver_fixture.write_text(
        naver_fixture.read_text(encoding="utf-8").replace("추천 모델을 실험합니다.", "추천 모델을 대규모 트래픽에 적용합니다."),
        encoding="utf-8",
    )

    summary = update_incremental_pipeline(project_root=sandbox_project)
    paths = ProjectPaths.from_root(sandbox_project)
    staging = read_csv_or_empty(paths.staging_jobs_path)
    assert summary["staging_job_count"] >= 6
    assert "미발견" in staging["record_status"].tolist()
    assert summary["dropped_low_quality_job_count"] >= 0
    assert summary["incremental_baseline_mode"] in {"master", "staging"}
    assert summary["completed_full_source_scan"] is True
    assert summary["processed_collectable_source_count"] == summary["selected_collectable_source_count"]
    assert summary["held_job_count"] == 0
    assert summary["carried_forward_job_count"] >= 1
    assert summary["merged_job_count"] == summary["staging_job_count"]
    assert set(staging["record_status"]) & {"유지", "미발견"}


def test_update_incremental_pipeline_with_subset_registry_preserves_full_source_registry(sandbox_project: Path) -> None:
    discover_companies_pipeline(project_root=sandbox_project)
    discover_sources_pipeline(project_root=sandbox_project)
    collect_jobs_pipeline(dry_run=False, project_root=sandbox_project)
    promote_staging_pipeline(project_root=sandbox_project)

    paths = ProjectPaths.from_root(sandbox_project)
    full_registry = read_csv_or_empty(paths.source_registry_path, SOURCE_REGISTRY_COLUMNS)
    subset_registry = full_registry.head(1).copy()
    before_urls = set(full_registry["source_url"].fillna("").astype(str))

    summary = update_incremental_pipeline(
        project_root=sandbox_project,
        allow_source_discovery_fallback=False,
        enable_source_scan_progress=False,
        registry_frame=subset_registry,
    )

    refreshed_registry = read_csv_or_empty(paths.source_registry_path, SOURCE_REGISTRY_COLUMNS)
    after_urls = set(refreshed_registry["source_url"].fillna("").astype(str))
    assert before_urls == after_urls
    assert len(refreshed_registry) == len(full_registry)
    assert summary["verified_source_success_count"] >= 1


def test_run_daily_tracking_pipeline_uses_published_registry_for_incremental_only(
    monkeypatch: pytest.MonkeyPatch,
    sandbox_project: Path,
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    registry = pd.DataFrame(
        [
            {
                "company_name": "테스트회사",
                "company_tier": "스타트업",
                "source_name": "테스트 공식 채용",
                "source_url": "https://example.com/jobs",
                "source_type": "html_page",
                "source_bucket": "approved",
                "verification_status": "성공",
            }
        ]
    )
    for column in SOURCE_REGISTRY_COLUMNS:
        if column not in registry.columns:
            registry[column] = ""
    write_csv(registry[list(SOURCE_REGISTRY_COLUMNS)], paths.source_registry_path)

    called: dict[str, object] = {}

    def fake_update_incremental_pipeline(project_root=None, **kwargs):
        called["update_kwargs"] = kwargs
        return {
            "collection_mode": "live",
            "collected_job_count": 4,
            "verified_source_success_count": 2,
            "verified_source_failure_count": 0,
            "staging_job_count": 5,
            "quality_gate_passed": True,
            "quality_gate_reasons": [],
        }

    def fake_build_coverage_report_pipeline(project_root=None):
        called["coverage"] = True
        return {"활성 공고 수": 5}

    def fake_promote_staging_pipeline(project_root=None):
        called["promote"] = True
        return {
            "quality_gate_passed": True,
            "quality_gate_reasons": [],
            "promoted_job_count": 5,
        }

    def fake_sync_sheets_pipeline(target, project_root=None):
        called.setdefault("sync_targets", []).append(target)
        return {"target": target, "google_sheets_synced": True}

    monkeypatch.setattr(pipelines_module, "update_incremental_pipeline", fake_update_incremental_pipeline)
    monkeypatch.setattr(pipelines_module, "build_coverage_report_pipeline", fake_build_coverage_report_pipeline)
    monkeypatch.setattr(pipelines_module, "promote_staging_pipeline", fake_promote_staging_pipeline)
    monkeypatch.setattr(pipelines_module, "sync_sheets_pipeline", fake_sync_sheets_pipeline)

    summary = run_daily_tracking_pipeline(project_root=sandbox_project)

    assert called["update_kwargs"]["allow_source_discovery_fallback"] is False
    assert summary["run_mode"] == "incremental"
    assert summary["published_state"]["published_source_registry_ready"] is True
    assert summary["published_state"]["allow_source_discovery_fallback"] is False
    assert summary["promotion"]["promoted_job_count"] == 5
    assert called["sync_targets"] == ["staging", "master"]


def test_run_daily_tracking_pipeline_skips_promotion_and_sync_when_quality_gate_fails(
    monkeypatch: pytest.MonkeyPatch,
    sandbox_project: Path,
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    registry = pd.DataFrame(
        [
            {
                "company_name": "테스트회사",
                "company_tier": "스타트업",
                "source_name": "테스트 공식 채용",
                "source_url": "https://example.com/jobs",
                "source_type": "html_page",
                "source_bucket": "approved",
                "verification_status": "성공",
            }
        ]
    )
    for column in SOURCE_REGISTRY_COLUMNS:
        if column not in registry.columns:
            registry[column] = ""
    write_csv(registry[list(SOURCE_REGISTRY_COLUMNS)], paths.source_registry_path)

    called = {"promote": 0, "sync": 0}

    monkeypatch.setattr(
        pipelines_module,
        "update_incremental_pipeline",
        lambda project_root=None, **kwargs: {
            "collection_mode": "live",
            "collected_job_count": 2,
            "verified_source_success_count": 1,
            "verified_source_failure_count": 0,
            "staging_job_count": 4,
            "quality_gate_passed": False,
            "quality_gate_reasons": ["quality_gate_failed"],
        },
    )
    monkeypatch.setattr(pipelines_module, "build_coverage_report_pipeline", lambda project_root=None: {"활성 공고 수": 4})

    def fake_promote_staging_pipeline(project_root=None):
        called["promote"] += 1
        return {}

    def fake_sync_sheets_pipeline(target, project_root=None):
        called["sync"] += 1
        return {"target": target, "google_sheets_synced": True}

    monkeypatch.setattr(pipelines_module, "promote_staging_pipeline", fake_promote_staging_pipeline)
    monkeypatch.setattr(pipelines_module, "sync_sheets_pipeline", fake_sync_sheets_pipeline)

    summary = run_daily_tracking_pipeline(project_root=sandbox_project)

    assert called["promote"] == 0
    assert called["sync"] == 0
    assert summary["published_state"]["promotion_allowed"] is False
    assert summary["published_state"]["promotion_block_reason"] == "quality_gate_failed"
    assert summary["promotion"]["promotion_skipped"] is True
    assert summary["promotion"]["promotion_skipped_reason"] == "quality_gate_failed"


def test_run_weekly_expansion_pipeline_progresses_state_without_collection_publish(
    monkeypatch: pytest.MonkeyPatch,
    sandbox_project: Path,
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    in_progress_candidates_path = paths.runtime_dir / "company_candidates_in_progress.csv"
    in_progress_registry_path = paths.runtime_dir / "source_registry_in_progress.csv"

    write_csv(
        pd.DataFrame(
            [
                {
                    "company_name": "알파",
                    "company_tier": "중견/중소",
                    "official_domain": "alpha.example",
                    "company_name_en": "",
                    "region": "",
                    "aliases": "",
                    "discovery_method": "seed",
                    "candidate_seed_type": "html_table_url",
                    "candidate_seed_url": "https://alpha.example/seed",
                    "candidate_seed_title": "알파",
                    "candidate_seed_reason": "테스트",
                    "officiality_score": 4,
                    "source_seed_count": 1,
                    "verified_source_count": 0,
                    "active_job_count": 0,
                    "role_analyst_signal": 0,
                    "role_ds_signal": 0,
                    "role_researcher_signal": 0,
                    "role_ai_engineer_signal": 1,
                    "role_fit_score": 1,
                    "hiring_signal_score": 1,
                    "evidence_count": 1,
                    "primary_evidence_type": "company_registry",
                    "primary_evidence_url": "https://alpha.example",
                    "primary_evidence_text": "알파",
                    "company_bucket": "approved",
                    "reject_reason": "",
                    "last_verified_at": "2026-04-01T00:00:00+09:00",
                }
            ]
        ),
        in_progress_candidates_path,
    )
    registry = pd.DataFrame(
        [
            {
                "company_name": "알파",
                "company_tier": "중견/중소",
                "source_name": "알파 공식 채용",
                "source_url": "https://alpha.example/jobs",
                "source_type": "html_page",
                "official_domain": "alpha.example",
                "is_official_hint": True,
                "structure_hint": "html",
                "discovery_method": "test",
                "source_bucket": "approved",
                "screening_reason": "",
                "verification_status": "성공",
                "verification_note": "",
                "last_checked_at": "",
                "last_success_at": "",
                "failure_count": 0,
                "last_active_job_count": 0,
                "source_quality_score": 0,
                "quarantine_reason": "",
                "is_quarantined": False,
            }
        ]
    )
    for column in SOURCE_REGISTRY_COLUMNS:
        if column not in registry.columns:
            registry[column] = ""
    write_csv(registry[list(SOURCE_REGISTRY_COLUMNS)], in_progress_registry_path)

    called = {"screen": 0, "discover": 0, "verify": 0}

    monkeypatch.setattr(pipelines_module, "expand_company_candidates_pipeline", lambda project_root=None: {"expanded_candidate_company_count": 12})
    monkeypatch.setattr(
        pipelines_module,
        "collect_company_evidence_pipeline",
        lambda project_root=None, **kwargs: {
            "company_evidence_count": 5,
            "approved_company_count": 1,
            "candidate_bucket_count": 0,
            "rejected_company_count": 0,
            "published_company_state": False,
        },
    )
    monkeypatch.setattr(pipelines_module, "build_coverage_report_pipeline", lambda project_root=None: {"활성 공고 수": 0})

    def fake_screen_companies_pipeline(project_root=None):
        called["screen"] += 1
        return {}

    def fake_discover_sources_pipeline(project_root=None):
        called["discover"] += 1
        return {}

    def fake_verify_sources_pipeline(project_root=None):
        called["verify"] += 1
        return {}

    monkeypatch.setattr(pipelines_module, "screen_companies_pipeline", fake_screen_companies_pipeline)
    monkeypatch.setattr(pipelines_module, "discover_sources_pipeline", fake_discover_sources_pipeline)
    monkeypatch.setattr(pipelines_module, "verify_sources_pipeline", fake_verify_sources_pipeline)

    summary = run_weekly_expansion_pipeline(project_root=sandbox_project)

    assert called == {"screen": 0, "discover": 0, "verify": 0}
    assert summary["run_mode"] == "weekly_expansion"
    assert summary["published_state"]["collection_ready"] is False
    assert summary["published_state"]["promotion_block_reason"] == "weekly_expansion_only"
    assert summary["source_discovery"]["screened_source_count"] == 1
    assert summary["source_verification"]["verification_mode"] == "deferred_until_company_scan_complete"


def test_verify_sources_pipeline_uses_incremental_source_scan_progress(
    monkeypatch: pytest.MonkeyPatch,
    sandbox_project: Path,
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    registry = pd.DataFrame(
        [
            {
                "company_name": "알파",
                "company_tier": "중견/중소",
                "source_name": "알파 공식 채용",
                "source_url": "https://alpha.example/jobs",
                "source_type": "html_page",
                "official_domain": "alpha.example",
                "is_official_hint": True,
                "structure_hint": "html",
                "discovery_method": "test",
                "source_bucket": "approved",
                "screening_reason": "",
                "verification_status": "",
                "failure_count": 0,
                "last_verified_at": "",
                "last_success_at": "",
                "last_active_job_count": 0,
                "is_quarantined": False,
                "quarantine_reason": "",
            }
        ],
        columns=list(SOURCE_REGISTRY_COLUMNS),
    )
    write_csv(registry, paths.source_registry_path)

    called: dict[str, object] = {}

    def fake_collect_jobs_from_sources(
        source_registry,
        paths_arg,
        settings_arg,
        *,
        run_id,
        snapshot_date,
        collected_at,
        enable_source_scan_progress=False,
        enable_recruiter_ocr_recovery=False,
    ):
        called["enable_source_scan_progress"] = enable_source_scan_progress
        called["enable_recruiter_ocr_recovery"] = enable_recruiter_ocr_recovery
        updated_registry = registry.copy()
        updated_registry.loc[:, "verification_status"] = "성공"
        updated_registry.loc[:, "failure_count"] = 0
        updated_registry.loc[:, "last_active_job_count"] = 1
        summary = {
            "collection_mode": "live",
            "verified_source_success_count": 1,
            "verified_source_failure_count": 0,
            "source_scan_mode": "incremental_cursor",
            "completed_full_source_scan": False,
            "processed_collectable_source_count": 1,
            "selected_collectable_source_count": 1,
        }
        return pd.DataFrame(columns=list(JOB_COLUMNS)), [], updated_registry, summary

    monkeypatch.setattr(pipelines_module, "collect_jobs_from_sources", fake_collect_jobs_from_sources)

    summary = pipelines_module.verify_sources_pipeline(project_root=sandbox_project)

    assert called["enable_source_scan_progress"] is True
    assert called["enable_recruiter_ocr_recovery"] is True
    assert summary["source_scan_mode"] == "incremental_cursor"
    assert summary["completed_full_source_scan"] is False


def test_processed_verified_source_urls_only_include_current_run_successes() -> None:
    registry = pd.DataFrame(
        [
            {
                "source_url": "https://processed.example/jobs",
                "verification_status": "성공",
                "last_success_at": "2026-04-01T06:00:03+09:00",
                "last_active_job_count": 2,
            },
            {
                "source_url": "https://zero-active.example/jobs",
                "verification_status": "성공",
                "last_success_at": "2026-04-01T06:00:03+09:00",
                "last_active_job_count": 0,
            },
            {
                "source_url": "https://stale.example/jobs",
                "verification_status": "성공",
                "last_success_at": "2026-03-31T06:00:03+09:00",
                "last_active_job_count": 5,
            },
            {
                "source_url": "https://failed.example/jobs",
                "verification_status": "실패",
                "last_success_at": "2026-04-01T06:00:03+09:00",
                "last_active_job_count": 3,
            },
            {
                "source_url": "",
                "verification_status": "성공",
                "last_success_at": "2026-04-01T06:00:03+09:00",
                "last_active_job_count": 1,
            },
        ]
    )

    verified_urls = pipelines_module._processed_verified_source_urls(registry, "2026-04-01T06:00:03+09:00")

    assert verified_urls == {"https://processed.example/jobs"}


def test_processed_source_outcomes_distinguish_success_failure_and_unattempted() -> None:
    previous_registry = pd.DataFrame(
        [
            {
                "source_url": "https://success.example/jobs",
                "verification_status": "성공",
                "failure_count": 0,
                "last_success_at": "2026-03-31T06:00:03+09:00",
                "last_active_job_count": 3,
            },
            {
                "source_url": "https://failure.example/jobs",
                "verification_status": "실패",
                "failure_count": 1,
                "last_success_at": "",
                "last_active_job_count": 0,
            },
            {
                "source_url": "https://unattempted.example/jobs",
                "verification_status": "성공",
                "failure_count": 0,
                "last_success_at": "2026-03-31T06:00:03+09:00",
                "last_active_job_count": 2,
            },
        ]
    )
    updated_registry = pd.DataFrame(
        [
            {
                "source_url": "https://success.example/jobs",
                "verification_status": "성공",
                "failure_count": 0,
                "last_success_at": "2026-04-01T06:00:03+09:00",
                "last_active_job_count": 0,
            },
            {
                "source_url": "https://failure.example/jobs",
                "verification_status": "실패",
                "failure_count": 2,
                "last_success_at": "",
                "last_active_job_count": 0,
            },
            {
                "source_url": "https://unattempted.example/jobs",
                "verification_status": "성공",
                "failure_count": 0,
                "last_success_at": "2026-03-31T06:00:03+09:00",
                "last_active_job_count": 2,
            },
        ]
    )

    outcomes = pipelines_module._processed_source_outcomes(
        previous_registry,
        updated_registry,
        "2026-04-01T06:00:03+09:00",
    )

    assert outcomes == {
        "https://success.example/jobs": "success",
        "https://failure.example/jobs": "failure",
    }


def test_processed_source_outcomes_distinguish_success_failure_and_unattempted() -> None:
    previous_registry = pd.DataFrame(
        [
            {
                "source_url": "https://success.example/jobs",
                "verification_status": "성공",
                "failure_count": 0,
                "last_success_at": "2026-03-31T06:00:03+09:00",
                "last_active_job_count": 3,
            },
            {
                "source_url": "https://failure.example/jobs",
                "verification_status": "실패",
                "failure_count": 1,
                "last_success_at": "",
                "last_active_job_count": 0,
            },
            {
                "source_url": "https://unattempted.example/jobs",
                "verification_status": "성공",
                "failure_count": 0,
                "last_success_at": "2026-03-31T06:00:03+09:00",
                "last_active_job_count": 2,
            },
        ]
    )
    updated_registry = pd.DataFrame(
        [
            {
                "source_url": "https://success.example/jobs",
                "verification_status": "성공",
                "failure_count": 0,
                "last_success_at": "2026-04-01T06:00:03+09:00",
                "last_active_job_count": 0,
            },
            {
                "source_url": "https://failure.example/jobs",
                "verification_status": "실패",
                "failure_count": 2,
                "last_success_at": "",
                "last_active_job_count": 0,
            },
            {
                "source_url": "https://unattempted.example/jobs",
                "verification_status": "성공",
                "failure_count": 0,
                "last_success_at": "2026-03-31T06:00:03+09:00",
                "last_active_job_count": 2,
            },
        ]
    )

    outcomes = pipelines_module._processed_source_outcomes(
        previous_registry,
        updated_registry,
        "2026-04-01T06:00:03+09:00",
    )

    assert outcomes == {
        "https://success.example/jobs": "success",
        "https://failure.example/jobs": "failure",
    }


def test_collect_jobs_from_sources_prioritizes_ats_before_html_pages(
    monkeypatch: pytest.MonkeyPatch,
    sandbox_project: Path,
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    registry = pd.DataFrame(
        [
            {
                "company_name": "회사HTML",
                "company_tier": "중견/중소",
                "source_name": "HTML 채용",
                "source_url": "https://html.example/jobs",
                "source_type": "html_page",
                "source_bucket": "approved",
            },
            {
                "company_name": "회사ATS",
                "company_tier": "중견/중소",
                "source_name": "ATS 채용",
                "source_url": "https://ats.example/jobs",
                "source_type": "greetinghr",
                "source_bucket": "approved",
            },
        ]
    )
    for column in SOURCE_REGISTRY_COLUMNS:
        if column not in registry.columns:
            registry[column] = ""
    registry = registry[list(SOURCE_REGISTRY_COLUMNS)]

    called_urls: list[str] = []

    def fake_fetch_source_content(url: str, *_args, **_kwargs) -> tuple[str, str]:
        called_urls.append(url)
        return "{\"jobs\": []}", "application/json"

    monkeypatch.setattr(collection_module, "fetch_source_content", fake_fetch_source_content)
    monkeypatch.setattr(collection_module, "parse_jobs_from_payload", lambda *args, **kwargs: [])

    settings = AppSettings(
        use_mock_sources=True,
        enable_gemini_fallback=False,
        job_collection_source_batch_size=1,
        job_collection_source_max_batches_per_run=1,
        job_collection_max_runtime_seconds=120,
    )

    first_jobs, first_raw, first_updated_registry, first_summary = collection_module.collect_jobs_from_sources(
        registry,
        paths,
        settings,
        run_id="run-1",
        snapshot_date="2026-04-01",
        collected_at="2026-04-01T08:00:00+09:00",
        enable_source_scan_progress=True,
    )
    second_jobs, second_raw, second_updated_registry, second_summary = collection_module.collect_jobs_from_sources(
        registry,
        paths,
        settings,
        run_id="run-2",
        snapshot_date="2026-04-01",
        collected_at="2026-04-01T08:05:00+09:00",
        enable_source_scan_progress=True,
    )

    assert first_jobs.empty
    assert second_jobs.empty
    assert len(first_raw) == 0
    assert len(second_raw) == 0
    assert len(first_updated_registry) == 2
    assert len(second_updated_registry) == 2
    assert called_urls == [
        "https://ats.example/jobs",
        "https://html.example/jobs",
    ]
    assert first_summary["source_scan_start_offset"] == 0
    assert first_summary["source_scan_next_offset"] == 1
    assert first_summary["completed_full_source_scan"] is False
    assert second_summary["source_scan_start_offset"] == 1
    assert second_summary["source_scan_next_offset"] == 0
    assert second_summary["completed_full_source_scan"] is True


def test_collect_jobs_from_sources_uses_gemini_html_listing_fallback(
    monkeypatch: pytest.MonkeyPatch,
    sandbox_project: Path,
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    registry = pd.DataFrame(
        [
            {
                "company_name": "회사HTML",
                "company_tier": "중견/중소",
                "source_name": "HTML 채용",
                "source_url": "https://html.example/jobs",
                "source_type": "html_page",
                "source_bucket": "approved",
            },
        ]
    )
    for column in SOURCE_REGISTRY_COLUMNS:
        if column not in registry.columns:
            registry[column] = ""
    registry = registry[list(SOURCE_REGISTRY_COLUMNS)]

    def fake_fetch_source_content(url: str, *_args, **_kwargs) -> tuple[str, str]:
        return "<html><body><a href='/jobs/1'>상세보기</a></body></html>", "text/html"

    monkeypatch.setattr(collection_module, "fetch_source_content", fake_fetch_source_content)
    monkeypatch.setattr(
        collection_module,
        "_extract_html_jobs_with_gemini",
        lambda *args, **kwargs: [
            {
                "title": "Machine Learning Engineer",
                "description_html": "<div>Machine Learning Engineer</div>",
                "job_url": "https://html.example/jobs/1",
            }
        ],
    )

    settings = AppSettings(
        use_mock_sources=True,
        enable_gemini_fallback=True,
        gemini_api_key="test-key",
        gemini_model="gemini-2.5-flash",
        gemini_html_listing_max_calls_per_run=4,
        job_collection_source_batch_size=10,
        job_collection_source_max_batches_per_run=1,
        job_collection_max_runtime_seconds=120,
    )

    jobs, raw, updated_registry, summary = collection_module.collect_jobs_from_sources(
        registry,
        paths,
        settings,
        run_id="run-1",
        snapshot_date="2026-04-02",
        collected_at="2026-04-02T02:00:00+09:00",
        enable_source_scan_progress=False,
    )

    assert len(raw) == 1
    assert len(jobs) == 1
    assert jobs.iloc[0]["job_title_raw"] == "Machine Learning Engineer"
    assert jobs.iloc[0]["job_url"] == "https://html.example/jobs/1"
    assert summary["collected_job_count"] == 1
    assert summary["verified_source_success_count"] == 1
    assert int(updated_registry.iloc[0]["last_active_job_count"]) == 1


def test_prepare_html_gemini_probe_payload_includes_detail_href_candidates_even_without_hiring_text() -> None:
    html_doc = """
    <html><body>
      <a href="/rcrt/view.do?annoId=123">연구직 6급</a>
      <a href="/notice/view.do?articleNo=9">공지사항</a>
    </body></html>
    """
    payload, allowed_urls = collection_module._prepare_html_gemini_probe_payload(
        html_doc,
        "https://example.com/rcrt/list.do",
    )

    assert {"text": "연구직 6급", "url": "https://example.com/rcrt/view.do?annoId=123"} in payload["anchors"]
    assert "https://example.com/rcrt/view.do?annoId=123" in allowed_urls


def test_extract_followable_html_redirect_urls_treats_saramin_as_external_recruit_host() -> None:
    jobs = [
        {
            "title": "인재채용",
            "job_url": "https://www.saramin.co.kr/zf_user/company-info/view-inner-recruit?csn=test",
            "description_html": "<div>인재채용</div>",
        }
    ]

    urls = collection_module._extract_followable_html_redirect_urls(
        jobs,
        base_url="https://www.alchera.ai/company/career",
    )

    assert urls == ["https://www.saramin.co.kr/zf_user/company-info/view-inner-recruit?csn=test"]


def test_build_html_listing_jobs_from_anchors_keeps_short_external_recruit_cta() -> None:
    html_doc = """
    <html><body>
      <section>
        <p>알체라는 채용 중 AI 로 꿈의 시대를 만들어 갈 진취적인 동료를 기다립니다.</p>
        <a href="https://www.saramin.co.kr/zf_user/company-info/view-inner-recruit?csn=test">상시 채용 중</a>
      </section>
    </body></html>
    """

    jobs = collection_module._build_html_listing_jobs_from_anchors(
        html_doc,
        "https://www.alchera.ai/company/career",
    )

    assert jobs == [
        {
            "title": "상시 채용 중",
            "description_html": "<div>상시 채용 중</div>",
            "job_url": "https://www.saramin.co.kr/zf_user/company-info/view-inner-recruit?csn=test",
            "listing_context": "알체라는 채용 중 AI 로 꿈의 시대를 만들어 갈 진취적인 동료를 기다립니다. 상시 채용 중",
        }
    ]


def test_extract_followable_html_redirect_urls_follows_generic_detail_cta_with_hiring_context() -> None:
    jobs = [
        {
            "title": "자세히\n보기",
            "job_url": "https://gazzi.ai/careers/ai-rnd-engineer-special",
            "listing_context": "채용 중 [병역특례 전문연구요원] AI 기술 및 응용솔루션 R&D 엔지니어 자세히\n보기",
            "description_html": "<div>자세히\n보기</div>",
        }
    ]

    urls = collection_module._extract_followable_html_redirect_urls(
        jobs,
        base_url="https://gazzi.ai/careers",
    )

    assert urls == ["https://gazzi.ai/careers/ai-rnd-engineer-special"]


def test_build_html_listing_jobs_from_anchors_keeps_saramin_relay_posting_links() -> None:
    html_doc = """
    <html><body>
      <div class="list_item">
        <div class="box_item">
          <div class="job_tit">
            <a href="/zf_user/jobs/relay/view?view_type=list&rec_idx=53396371">[알체라] AI 연구원 모집 (얼굴인식/위조판별)</a>
          </div>
          <div class="job_condition">서울 강남구 3 ~ 8년 · 정규직</div>
        </div>
      </div>
    </body></html>
    """

    jobs = collection_module._build_html_listing_jobs_from_anchors(
        html_doc,
        "https://www.saramin.co.kr/zf_user/company-info/view-inner-recruit?csn=test",
    )

    assert jobs == [
        {
            "title": "[알체라] AI 연구원 모집 (얼굴인식/위조판별)",
            "description_html": "<div>[알체라] AI 연구원 모집 (얼굴인식/위조판별)</div>",
            "job_url": "https://www.saramin.co.kr/zf_user/jobs/relay/view?view_type=list&rec_idx=53396371",
            "listing_context": "[알체라] AI 연구원 모집 (얼굴인식/위조판별) 서울 강남구 3 ~ 8년 · 정규직",
        }
    ]


def test_extract_followable_html_redirect_urls_follows_external_ats_detail_url() -> None:
    jobs = [
        {
            "title": "[알체라] AI 연구원 모집 (얼굴인식/위조판별)",
            "job_url": "https://www.saramin.co.kr/zf_user/jobs/relay/view?view_type=list&rec_idx=53396371",
            "description_html": "<div>[알체라] AI 연구원 모집 (얼굴인식/위조판별)</div>",
            "listing_context": "[알체라] AI 연구원 모집 (얼굴인식/위조판별) 서울 강남구 3 ~ 8년 · 정규직",
        }
    ]

    urls = collection_module._extract_followable_html_redirect_urls(
        jobs,
        base_url="https://www.alchera.ai/company/career",
    )

    assert urls == ["https://www.saramin.co.kr/zf_user/jobs/relay/view?view_type=list&rec_idx=53396371"]


def test_extract_saramin_company_info_jobs_prefers_real_relay_postings() -> None:
    html_doc = """
    <html><body>
      <div class="list_item">
        <div class="box_item">
          <div class="job_tit">
            <a class="str_tit" title="[알체라] AI 연구원 모집 (얼굴인식/위조판별)" href="/zf_user/jobs/relay/view?view_type=list&rec_idx=53396371"><span>[알체라] AI 연구원 모집 (얼굴인식/위조판별)</span></a>
          </div>
          <div class="job_meta"><span>모델링</span><span>Python</span></div>
          <div class="recruit_info">
            <p class="work_place">서울 강남구</p>
            <p class="career">3 ~ 8년 · 정규직</p>
          </div>
        </div>
      </div>
      <a href="https://komate.saramin.co.kr">외국인 채용정보는 KoMate</a>
    </body></html>
    """

    jobs = collection_module._extract_saramin_company_info_jobs(
        html_doc,
        "https://www.saramin.co.kr/zf_user/company-info/view-inner-recruit?csn=test",
    )

    assert jobs == [
        {
            "title": "[알체라] AI 연구원 모집 (얼굴인식/위조판별)",
            "description_html": "<div>[알체라] AI 연구원 모집 (얼굴인식/위조판별)</div>",
            "job_url": "https://www.saramin.co.kr/zf_user/jobs/relay/view?view_type=list&rec_idx=53396371",
            "location": "서울 강남구",
            "experience_level": "3 ~ 8년 · 정규직",
            "listing_context": "[알체라] AI 연구원 모집 (얼굴인식/위조판별) 모델링 Python 서울 강남구 3 ~ 8년 · 정규직",
        }
    ]


def test_extract_multi_role_html_jobs_splits_same_page_roles() -> None:
    html_doc = """
    <html><body>
      <section class="jobs">
        <div class="job">
          <h3>AI 개발자(AI Model Developer/Researcher)</h3>
          <div class="job_con">
            <h4>담당 업무</h4>
            <p>외부 AI 모델 개발자들과 협업하여 고성능 AI 모델을 개발합니다.</p>
            <h4>자격 요건</h4>
            <p>AI/ML 관련 분야 3년 이상의 경력</p>
            <h4>우대 사항</h4>
            <p>자연어 처리 경험</p>
          </div>
        </div>
        <div class="job">
          <h3>서비스 개발자(Service Developer)</h3>
          <div class="job_con">
            <h4>담당 업무</h4>
            <p>서비스 플랫폼 설계 및 개발</p>
            <h4>자격 요건</h4>
            <p>3년 이상의 웹 서비스 개발 경험</p>
            <h4>우대 사항</h4>
            <p>AI 플랫폼 서비스 개발 경험</p>
          </div>
        </div>
      </section>
    </body></html>
    """

    jobs = collection_module._extract_multi_role_html_jobs(
        html_doc,
        "https://hugeailab.com/recruit",
    )

    assert len(jobs) == 2
    assert jobs[0]["title"] == "AI 개발자(AI Model Developer/Researcher)"
    assert jobs[0]["job_url"] == "https://hugeailab.com/recruit#role-ai-개발자-ai-model-developer-researcher"
    assert jobs[1]["job_url"] == "https://hugeailab.com/recruit#role-서비스-개발자-service-developer"
    assert len({job["job_url"] for job in jobs}) == 2
    assert "자격 요건" in jobs[0]["description_html"]


def test_fetch_source_content_recovers_saramin_relay_detail_as_json_job(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    detail_html = """
    <main class="job-posting job-posting--list">
      <div class="job-header">
        <p class="job-header__title">[알체라] AI 연구원 모집 (얼굴인식/위조판별)</p>
      </div>
      <div class="job-content">
        <div class="info-block">
          <p class="info-block__title">📋 주요업무</p>
          <div class="info-block__list"><p>모델 성능 고도화</p></div>
          <p class="info-block__title">📋 자격요건</p>
          <div class="info-block__list"><p>경력 3년 이상</p><p>Python</p></div>
          <p><strong>📋 우대 사항</strong></p>
          <p>얼굴인식 경험자</p>
        </div>
      </div>
    </main>
    """
    encoded_detail = base64.b64encode(detail_html.encode("utf-8")).decode("ascii")
    mobile_html = f"""
    <html><head>
      <title>[(주)알체라] [알체라] AI 연구원 모집 (얼굴인식/위조판별) (D-16) - 사람인</title>
      <meta property="og:title" content="[(주)알체라] [알체라] AI 연구원 모집 (얼굴인식/위조판별) (D-16) - 사람인" />
    </head><body>
      <dl class="item_info"><dt class="tit">경력</dt><dd class="desc">경력 3~8년</dd></dl>
      <dl class="item_info"><dt class="tit">지역</dt><dd class="desc">서울 강남구</dd></dl>
      <script>var detailContents_53396371 = {{ contents: '{encoded_detail}' }};</script>
    </body></html>
    """

    class Settings:
        use_mock_sources = False
        user_agent = "jobs-market-v2-test"
        timeout_seconds = 20.0
        connect_timeout_seconds = 5.0
        html_source_timeout_seconds = 8.0
        html_source_connect_timeout_seconds = 3.0

    def fake_fetch_remote(url: str, timeout_seconds: float, user_agent: str, *, connect_timeout_seconds: float | None = None):
        assert url == "https://m.saramin.co.kr/job-search/view?rec_idx=53396371"
        return mobile_html, "text/html; charset=UTF-8"

    monkeypatch.setattr(collection_module, "_fetch_remote", fake_fetch_remote)

    paths = ProjectPaths.from_root(tmp_path)
    paths.ensure_directories()
    content, content_type = collection_module.fetch_source_content(
        "https://www.saramin.co.kr/zf_user/jobs/relay/view?view_type=list&rec_idx=53396371",
        paths,
        Settings(),
        "html_page",
    )
    jobs = parse_jobs_from_payload(
        content,
        content_type,
        "html_page",
        base_url="https://www.saramin.co.kr/zf_user/jobs/relay/view?view_type=list&rec_idx=53396371",
    )

    assert len(jobs) == 1
    assert jobs[0]["title"] == "[알체라] AI 연구원 모집 (얼굴인식/위조판별)"
    assert jobs[0]["job_url"] == "https://www.saramin.co.kr/zf_user/jobs/relay/view?view_type=list&rec_idx=53396371"
    assert jobs[0]["experience_level"] == "경력 3~8년"
    assert jobs[0]["location"] == "서울 강남구"
    assert "모델 성능 고도화" in jobs[0]["description_html"]
    assert "경력 3년 이상" in jobs[0]["requirements"]
    assert "얼굴인식 경험자" in jobs[0]["preferred"]


def test_is_html_scout_candidate_requires_strong_listing_path() -> None:
    strong = {
        "source_type": "html_page",
        "source_url": "https://example.com/recruit/list.do",
        "last_active_job_count": 0,
    }
    generic = {
        "source_type": "html_page",
        "source_url": "https://example.com/recruit",
        "last_active_job_count": 0,
    }

    assert collection_module._is_html_scout_candidate(strong) is True
    assert collection_module._is_html_scout_candidate(generic) is False


def test_collect_jobs_from_sources_prioritizes_unscanned_and_historically_active_ats_sources_before_zero_active_successes(
    monkeypatch: pytest.MonkeyPatch,
    sandbox_project: Path,
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    registry = pd.DataFrame(
        [
            {
                "company_name": "회사HTML",
                "company_tier": "중견/중소",
                "source_name": "HTML 채용",
                "source_url": "https://html.example/jobs",
                "source_type": "html_page",
                "source_bucket": "approved",
                "verification_status": "성공",
                "last_success_at": "2026-04-01T07:00:00+09:00",
                "last_active_job_count": 0,
            },
            {
                "company_name": "회사ATS활성",
                "company_tier": "중견/중소",
                "source_name": "활성 ATS 채용",
                "source_url": "https://active.example/jobs",
                "source_type": "greenhouse",
                "source_bucket": "approved",
                "verification_status": "성공",
                "last_success_at": "2026-04-01T07:00:00+09:00",
                "last_active_job_count": 3,
            },
            {
                "company_name": "회사ATS미스캔",
                "company_tier": "중견/중소",
                "source_name": "미스캔 ATS 채용",
                "source_url": "https://newats.example/jobs",
                "source_type": "recruiter",
                "source_bucket": "approved",
                "verification_status": "미검증",
                "last_success_at": "",
                "last_active_job_count": 0,
            },
            {
                "company_name": "회사ATS영활성0",
                "company_tier": "중견/중소",
                "source_name": "영활성 ATS 채용",
                "source_url": "https://zeroats.example/jobs",
                "source_type": "greetinghr",
                "source_bucket": "approved",
                "verification_status": "성공",
                "last_success_at": "2026-04-01T07:00:00+09:00",
                "last_active_job_count": 0,
            },
        ]
    )
    for column in SOURCE_REGISTRY_COLUMNS:
        if column not in registry.columns:
            registry[column] = ""
    registry = registry[list(SOURCE_REGISTRY_COLUMNS)]

    called_urls: list[str] = []

    def fake_fetch_source_content(url: str, *_args, **_kwargs) -> tuple[str, str]:
        called_urls.append(url)
        return "{\"jobs\": []}", "application/json"

    monkeypatch.setattr(collection_module, "fetch_source_content", fake_fetch_source_content)
    monkeypatch.setattr(collection_module, "parse_jobs_from_payload", lambda *args, **kwargs: [])

    settings = AppSettings(
        use_mock_sources=True,
        enable_gemini_fallback=False,
        job_collection_source_batch_size=2,
        job_collection_source_max_batches_per_run=1,
        job_collection_max_runtime_seconds=120,
    )

    collection_module.collect_jobs_from_sources(
        registry,
        paths,
        settings,
        run_id="run-1",
        snapshot_date="2026-04-01",
        collected_at="2026-04-01T08:00:00+09:00",
        enable_source_scan_progress=True,
    )
    collection_module.collect_jobs_from_sources(
        registry,
        paths,
        settings,
        run_id="run-2",
        snapshot_date="2026-04-01",
        collected_at="2026-04-01T08:05:00+09:00",
        enable_source_scan_progress=True,
    )

    assert called_urls == [
        "https://newats.example/jobs",
        "https://active.example/jobs",
        "https://zeroats.example/jobs",
        "https://html.example/jobs",
    ]


def test_collect_jobs_from_sources_prioritizes_active_and_unverified_before_zero_active_successes(
    monkeypatch: pytest.MonkeyPatch,
    sandbox_project: Path,
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    registry = pd.DataFrame(
        [
            {
                "company_name": "회사성공0",
                "company_tier": "중견/중소",
                "source_name": "성공 0건",
                "source_url": "https://zero.example/jobs",
                "source_type": "greetinghr",
                "source_bucket": "approved",
                "verification_status": "성공",
                "last_active_job_count": 0,
            },
            {
                "company_name": "회사활성",
                "company_tier": "중견/중소",
                "source_name": "활성 성공",
                "source_url": "https://active.example/jobs",
                "source_type": "greetinghr",
                "source_bucket": "approved",
                "verification_status": "성공",
                "last_active_job_count": 4,
            },
            {
                "company_name": "회사미검증",
                "company_tier": "중견/중소",
                "source_name": "미검증",
                "source_url": "https://fresh.example/jobs",
                "source_type": "greetinghr",
                "source_bucket": "approved",
                "verification_status": "미검증",
                "last_active_job_count": 0,
            },
        ]
    )
    for column in SOURCE_REGISTRY_COLUMNS:
        if column not in registry.columns:
            registry[column] = ""
    registry = registry[list(SOURCE_REGISTRY_COLUMNS)]

    called_urls: list[str] = []

    def fake_fetch_source_content(url: str, *_args, **_kwargs) -> tuple[str, str]:
        called_urls.append(url)
        return "{\"jobs\": []}", "application/json"

    monkeypatch.setattr(collection_module, "fetch_source_content", fake_fetch_source_content)
    monkeypatch.setattr(collection_module, "parse_jobs_from_payload", lambda *args, **kwargs: [])

    settings = AppSettings(
        use_mock_sources=True,
        enable_gemini_fallback=False,
        job_collection_source_batch_size=2,
        job_collection_source_max_batches_per_run=1,
        job_collection_max_runtime_seconds=120,
    )

    _, _, _, first_summary = collection_module.collect_jobs_from_sources(
        registry,
        paths,
        settings,
        run_id="run-1",
        snapshot_date="2026-04-01",
        collected_at="2026-04-01T08:00:00+09:00",
        enable_source_scan_progress=True,
    )
    _, _, _, second_summary = collection_module.collect_jobs_from_sources(
        registry,
        paths,
        settings,
        run_id="run-2",
        snapshot_date="2026-04-01",
        collected_at="2026-04-01T08:05:00+09:00",
        enable_source_scan_progress=True,
    )

    assert called_urls == [
        "https://active.example/jobs",
        "https://fresh.example/jobs",
        "https://zero.example/jobs",
    ]
    assert first_summary["source_scan_start_offset"] == 0
    assert first_summary["source_scan_next_offset"] == 2
    assert first_summary["completed_full_source_scan"] is False
    assert second_summary["source_scan_start_offset"] == 2
    assert second_summary["source_scan_next_offset"] == 0
    assert second_summary["completed_full_source_scan"] is True


def test_collect_jobs_from_sources_interleaves_same_signal_ats_types(
    monkeypatch: pytest.MonkeyPatch,
    sandbox_project: Path,
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    registry = pd.DataFrame(
        [
            {
                "company_name": "회사활성고정",
                "company_tier": "중견/중소",
                "source_name": "활성 ATS",
                "source_url": "https://hot.example/jobs",
                "source_type": "greenhouse",
                "source_bucket": "approved",
                "verification_status": "성공",
                "last_success_at": "2026-04-01T07:00:00+09:00",
                "last_active_job_count": 5,
            },
            {
                "company_name": "회사그리팅1",
                "company_tier": "중견/중소",
                "source_name": "그리팅 1",
                "source_url": "https://greeting-a.example/jobs",
                "source_type": "greetinghr",
                "source_bucket": "approved",
                "verification_status": "성공",
                "last_success_at": "2026-04-01T07:00:00+09:00",
                "last_active_job_count": 0,
            },
            {
                "company_name": "회사그리팅2",
                "company_tier": "중견/중소",
                "source_name": "그리팅 2",
                "source_url": "https://greeting-b.example/jobs",
                "source_type": "greetinghr",
                "source_bucket": "approved",
                "verification_status": "성공",
                "last_success_at": "2026-04-01T07:00:00+09:00",
                "last_active_job_count": 0,
            },
            {
                "company_name": "회사리크루터1",
                "company_tier": "중견/중소",
                "source_name": "리크루터 1",
                "source_url": "https://recruiter-a.example/jobs",
                "source_type": "recruiter",
                "source_bucket": "approved",
                "verification_status": "성공",
                "last_success_at": "2026-04-01T07:00:00+09:00",
                "last_active_job_count": 0,
            },
            {
                "company_name": "회사리크루터2",
                "company_tier": "중견/중소",
                "source_name": "리크루터 2",
                "source_url": "https://recruiter-b.example/jobs",
                "source_type": "recruiter",
                "source_bucket": "approved",
                "verification_status": "성공",
                "last_success_at": "2026-04-01T07:00:00+09:00",
                "last_active_job_count": 0,
            },
        ]
    )
    for column in SOURCE_REGISTRY_COLUMNS:
        if column not in registry.columns:
            registry[column] = ""
    registry = registry[list(SOURCE_REGISTRY_COLUMNS)]

    called_urls: list[str] = []

    def fake_fetch_source_content(url: str, *_args, **_kwargs) -> tuple[str, str]:
        called_urls.append(url)
        return "{\"jobs\": []}", "application/json"

    monkeypatch.setattr(collection_module, "fetch_source_content", fake_fetch_source_content)
    monkeypatch.setattr(collection_module, "parse_jobs_from_payload", lambda *args, **kwargs: [])

    settings = AppSettings(
        use_mock_sources=True,
        enable_gemini_fallback=False,
        job_collection_source_batch_size=5,
        job_collection_source_max_batches_per_run=1,
        job_collection_max_runtime_seconds=120,
    )

    collection_module.collect_jobs_from_sources(
        registry,
        paths,
        settings,
        run_id="run-1",
        snapshot_date="2026-04-01",
        collected_at="2026-04-01T08:00:00+09:00",
        enable_source_scan_progress=True,
    )

    assert called_urls == [
        "https://hot.example/jobs",
        "https://greeting-a.example/jobs",
        "https://recruiter-a.example/jobs",
        "https://greeting-b.example/jobs",
        "https://recruiter-b.example/jobs",
    ]


def test_collect_jobs_from_sources_rechecks_historically_active_sources_while_cursor_advances(
    monkeypatch: pytest.MonkeyPatch,
    sandbox_project: Path,
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    registry = pd.DataFrame(
        [
            {
                "company_name": "회사활성",
                "company_tier": "중견/중소",
                "source_name": "활성 ATS",
                "source_url": "https://hot.example/jobs",
                "source_type": "greenhouse",
                "source_bucket": "approved",
                "verification_status": "성공",
                "last_success_at": "2026-04-01T07:00:00+09:00",
                "last_active_job_count": 6,
            },
            *[
                {
                    "company_name": f"회사{i}",
                    "company_tier": "중견/중소",
                    "source_name": f"HTML {i}",
                    "source_url": f"https://html-{i}.example/jobs",
                    "source_type": "html_page",
                    "source_bucket": "approved",
                    "verification_status": "성공",
                    "last_success_at": "2026-04-01T07:00:00+09:00",
                    "last_active_job_count": 0,
                }
                for i in range(1, 10)
            ],
        ]
    )
    for column in SOURCE_REGISTRY_COLUMNS:
        if column not in registry.columns:
            registry[column] = ""
    registry = registry[list(SOURCE_REGISTRY_COLUMNS)]

    called_urls: list[str] = []

    def fake_fetch_source_content(url: str, *_args, **_kwargs) -> tuple[str, str]:
        called_urls.append(url)
        return "{\"jobs\": []}", "application/json"

    monkeypatch.setattr(collection_module, "fetch_source_content", fake_fetch_source_content)
    monkeypatch.setattr(collection_module, "parse_jobs_from_payload", lambda *args, **kwargs: [])

    settings = AppSettings(
        use_mock_sources=True,
        enable_gemini_fallback=False,
        job_collection_source_batch_size=5,
        job_collection_source_max_batches_per_run=1,
        job_collection_max_runtime_seconds=120,
    )

    _, _, _, first_summary = collection_module.collect_jobs_from_sources(
        registry,
        paths,
        settings,
        run_id="run-1",
        snapshot_date="2026-04-01",
        collected_at="2026-04-01T08:00:00+09:00",
        enable_source_scan_progress=True,
    )
    _, _, _, second_summary = collection_module.collect_jobs_from_sources(
        registry,
        paths,
        settings,
        run_id="run-2",
        snapshot_date="2026-04-01",
        collected_at="2026-04-01T08:05:00+09:00",
        enable_source_scan_progress=True,
    )

    assert called_urls == [
        "https://hot.example/jobs",
        "https://html-1.example/jobs",
        "https://html-2.example/jobs",
        "https://html-3.example/jobs",
        "https://html-4.example/jobs",
        "https://hot.example/jobs",
        "https://html-5.example/jobs",
        "https://html-6.example/jobs",
        "https://html-7.example/jobs",
        "https://html-8.example/jobs",
    ]
    assert first_summary["pinned_collectable_source_count"] == 0
    assert second_summary["source_scan_start_offset"] == 5
    assert second_summary["cursor_selected_collectable_source_count"] == 4
    assert second_summary["cursor_processed_collectable_source_count"] == 4
    assert second_summary["pinned_collectable_source_count"] == 1
    assert second_summary["source_scan_next_offset"] == 9


def test_source_collection_registry_signature_ignores_transient_source_activity() -> None:
    base_rows = [
        {
            "source_bucket": "approved",
            "verification_status": "성공",
            "last_active_job_count": 0,
            "source_type": "greetinghr",
            "source_url": "https://example.com/jobs",
        }
    ]
    changed_rows = [
        {
            "source_bucket": "approved",
            "verification_status": "성공",
            "last_active_job_count": 3,
            "source_type": "greetinghr",
            "source_url": "https://example.com/jobs",
        }
    ]

    assert collection_module._source_collection_registry_signature(base_rows) == collection_module._source_collection_registry_signature(changed_rows)


def test_collect_jobs_from_sources_resumes_with_surviving_cursor_when_registry_set_changes(
    monkeypatch: pytest.MonkeyPatch,
    sandbox_project: Path,
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    first_registry = pd.DataFrame(
        [
            {
                "company_name": "회사A",
                "company_tier": "중견/중소",
                "source_name": "A 채용",
                "source_url": "https://a.example/jobs",
                "source_type": "html_page",
                "source_bucket": "approved",
            },
            {
                "company_name": "회사B",
                "company_tier": "중견/중소",
                "source_name": "B 채용",
                "source_url": "https://b.example/jobs",
                "source_type": "html_page",
                "source_bucket": "approved",
            },
        ]
    )
    second_registry = pd.DataFrame(
        [
            {
                "company_name": "회사ATS",
                "company_tier": "중견/중소",
                "source_name": "ATS 채용",
                "source_url": "https://ats.example/jobs",
                "source_type": "greetinghr",
                "source_bucket": "approved",
            },
            *first_registry.to_dict(orient="records"),
        ]
    )
    for frame in (first_registry, second_registry):
        for column in SOURCE_REGISTRY_COLUMNS:
            if column not in frame.columns:
                frame[column] = ""

    called_urls: list[str] = []

    def fake_fetch_source_content(url: str, *_args, **_kwargs) -> tuple[str, str]:
        called_urls.append(url)
        return "{\"jobs\": []}", "application/json"

    monkeypatch.setattr(collection_module, "fetch_source_content", fake_fetch_source_content)
    monkeypatch.setattr(collection_module, "parse_jobs_from_payload", lambda *args, **kwargs: [])

    settings = AppSettings(
        use_mock_sources=True,
        enable_gemini_fallback=False,
        job_collection_source_batch_size=1,
        job_collection_source_max_batches_per_run=1,
        job_collection_max_runtime_seconds=120,
    )

    _, _, _, first_summary = collection_module.collect_jobs_from_sources(
        first_registry[list(SOURCE_REGISTRY_COLUMNS)],
        paths,
        settings,
        run_id="run-1",
        snapshot_date="2026-04-01",
        collected_at="2026-04-01T08:00:00+09:00",
        enable_source_scan_progress=True,
    )
    _, _, _, second_summary = collection_module.collect_jobs_from_sources(
        second_registry[list(SOURCE_REGISTRY_COLUMNS)],
        paths,
        settings,
        run_id="run-2",
        snapshot_date="2026-04-01",
        collected_at="2026-04-01T08:05:00+09:00",
        enable_source_scan_progress=True,
    )
    assert first_summary["source_scan_start_offset"] == 0
    assert first_summary["source_scan_next_offset"] == 1
    assert first_summary["source_scan_resume_strategy"] == "reset"
    assert second_summary["source_scan_registry_signature_changed"] is True
    assert second_summary["source_scan_start_offset"] == 2
    assert second_summary["source_scan_resume_strategy"] == "cursor"
    assert called_urls == [
        "https://a.example/jobs",
        "https://b.example/jobs",
    ]


def test_discover_sources_pipeline_preserves_existing_verification_state(sandbox_project: Path) -> None:
    discover_companies_pipeline(project_root=sandbox_project)
    summary = discover_sources_pipeline(project_root=sandbox_project)
    assert summary["screened_source_count"] > 0

    paths = ProjectPaths.from_root(sandbox_project)
    registry = read_csv_or_empty(paths.source_registry_path, SOURCE_REGISTRY_COLUMNS)
    target_url = "https://careers.samsung.com/job-feed/data-ai.json"
    registry["last_success_at"] = registry["last_success_at"].astype("object")
    registry.loc[registry["source_url"] == target_url, "verification_status"] = "성공"
    registry.loc[registry["source_url"] == target_url, "failure_count"] = 0
    registry.loc[registry["source_url"] == target_url, "last_success_at"] = "2026-04-01T09:00:00+09:00"
    registry.loc[registry["source_url"] == target_url, "last_active_job_count"] = 3
    write_csv(registry, paths.source_registry_path)

    discover_sources_pipeline(project_root=sandbox_project)

    refreshed = read_csv_or_empty(paths.source_registry_path, SOURCE_REGISTRY_COLUMNS)
    preserved = refreshed[refreshed["source_url"] == target_url].iloc[0]
    assert preserved["verification_status"] == "성공"
    assert int(preserved["failure_count"]) == 0
    assert preserved["last_success_at"] == "2026-04-01T09:00:00+09:00"
    assert int(preserved["last_active_job_count"]) == 3


def test_discover_sources_pipeline_preserves_existing_sources_missing_from_current_discovery(
    monkeypatch: pytest.MonkeyPatch,
    sandbox_project: Path,
) -> None:
    discover_companies_pipeline(project_root=sandbox_project)
    paths = ProjectPaths.from_root(sandbox_project)
    existing_registry = pd.DataFrame(
        [
            {
                "company_name": "레거시회사",
                "company_tier": "스타트업",
                "source_name": "레거시 공식 채용",
                "source_url": "https://legacy.example/jobs",
                "source_type": "html_page",
                "source_bucket": "approved",
                "official_domain": "legacy.example",
                "is_official_hint": True,
                "structure_hint": "html",
                "discovery_method": "manual_seed",
                "source_quality_score": 0.9,
                "verification_status": "성공",
                "failure_count": 0,
                "last_success_at": "2026-04-01T09:00:00+09:00",
                "last_active_job_count": 2,
                "quarantine_reason": "",
                "is_quarantined": False,
            }
        ],
        columns=list(SOURCE_REGISTRY_COLUMNS),
    )
    write_csv(existing_registry, paths.source_registry_path)

    monkeypatch.setattr(
        pipelines_module,
        "discover_source_candidates",
        lambda companies, paths, settings=None: pd.DataFrame(
            [
                {
                    "company_name": "삼성전자",
                    "company_tier": "대기업",
                    "source_name": "삼성전자 공식 채용",
                    "source_url": "https://careers.samsung.com/job-feed/data-ai.json",
                    "source_type": "json_api",
                    "official_domain": "samsungcareers.com",
                    "is_official_hint": True,
                    "structure_hint": "json",
                    "discovery_method": "manual_seed",
                }
            ]
        ),
    )

    summary = discover_sources_pipeline(project_root=sandbox_project)

    refreshed = read_csv_or_empty(paths.source_registry_path, SOURCE_REGISTRY_COLUMNS)
    assert summary["screened_source_count"] >= 2
    assert "https://legacy.example/jobs" in refreshed["source_url"].tolist()


def test_quality_gate_fails_when_active_rows_are_only_verification_holdovers() -> None:
    staging = pd.DataFrame(
        [
            {
                "job_key": "hold-1",
                "change_hash": "a",
                "first_seen_at": "2026-04-01T00:00:00+09:00",
                "last_seen_at": "2026-04-01T00:00:00+09:00",
                "is_active": True,
                "missing_count": 0,
                "snapshot_date": "2026-04-01",
                "run_id": "r1",
                "source_url": "https://hold.example/jobs",
                "source_bucket": "approved",
                "source_name": "홀드 테스트",
                "company_name": "회사A",
                "company_tier": "중견/중소",
                "job_title_raw": "AI Engineer",
                "experience_level_raw": "경력",
                "job_role": "인공지능 엔지니어",
                "job_url": "https://hold.example/jobs/1",
                "record_status": "검증실패보류",
                "주요업무_분석용": "모델을 개발합니다.",
                "자격요건_분석용": "파이썬 경험",
                "우대사항_분석용": "서비스 경험",
                "핵심기술_분석용": "파이썬",
                "상세본문_분석용": "모델을 개발합니다.\n파이썬 경험\n서비스 경험",
                "회사명_표시": "회사A",
                "소스명_표시": "홀드 테스트",
                "공고제목_표시": "AI Engineer",
                "경력수준_표시": "경력",
                "경력근거_표시": "구조화 메타데이터",
                "채용트랙_표시": "일반채용",
                "채용트랙근거_표시": "기본추론",
                "직무초점_표시": "서비스개발",
                "직무초점근거_표시": "공고제목",
                "구분요약_표시": "경력",
                "직무명_표시": "인공지능 엔지니어",
                "주요업무_표시": "모델을 개발합니다.",
                "자격요건_표시": "파이썬 경험",
                "우대사항_표시": "서비스 경험",
                "핵심기술_표시": "파이썬",
            }
        ],
        columns=list(JOB_COLUMNS),
    )
    source_registry = pd.DataFrame(
        [
            {
                "company_name": "회사A",
                "company_tier": "중견/중소",
                "source_name": "홀드 테스트",
                "source_url": "https://hold.example/jobs",
                "source_type": "html_page",
                "source_bucket": "approved",
                "official_domain": "hold.example",
                "is_official_hint": True,
                "structure_hint": "html",
                "discovery_method": "manual_seed",
                "source_quality_score": 1.0,
                "verification_status": "실패",
                "failure_count": 1,
                "last_success_at": "",
                "last_active_job_count": 0,
                "quarantine_reason": "",
                "is_quarantined": False,
            }
        ],
        columns=list(SOURCE_REGISTRY_COLUMNS),
    )

    result = evaluate_quality_gate(staging, source_registry, already_filtered=True)

    assert result.passed is False
    assert "이번 staging은 검증실패보류 상태만 포함합니다." in result.reasons


def test_quality_gate_fails_when_carry_forward_hold_ratio_is_too_high() -> None:
    staging = pd.DataFrame(
        [
            {
                "job_key": "hold-1",
                "change_hash": "a",
                "first_seen_at": "2026-04-01T00:00:00+09:00",
                "last_seen_at": "2026-04-01T00:00:00+09:00",
                "is_active": True,
                "missing_count": 0,
                "snapshot_date": "2026-04-01",
                "run_id": "r1",
                "source_url": "https://hold.example/jobs",
                "source_bucket": "approved",
                "source_name": "홀드 테스트",
                "company_name": "회사A",
                "company_tier": "중견/중소",
                "job_title_raw": "AI Engineer",
                "experience_level_raw": "경력",
                "job_role": "인공지능 엔지니어",
                "job_url": "https://hold.example/jobs/1",
                "record_status": "검증실패보류",
                "주요업무_분석용": "모델을 개발합니다.",
                "자격요건_분석용": "파이썬 경험",
                "우대사항_분석용": "서비스 경험",
                "핵심기술_분석용": "파이썬",
                "상세본문_분석용": "모델을 개발합니다.\n파이썬 경험\n서비스 경험",
                "회사명_표시": "회사A",
                "소스명_표시": "홀드 테스트",
                "공고제목_표시": "AI Engineer",
                "경력수준_표시": "경력",
                "경력근거_표시": "구조화 메타데이터",
                "채용트랙_표시": "일반채용",
                "채용트랙근거_표시": "기본추론",
                "직무초점_표시": "서비스개발",
                "직무초점근거_표시": "공고제목",
                "구분요약_표시": "경력",
                "직무명_표시": "인공지능 엔지니어",
                "주요업무_표시": "모델을 개발합니다.",
                "자격요건_표시": "파이썬 경험",
                "우대사항_표시": "서비스 경험",
                "핵심기술_표시": "파이썬",
            },
            {
                "job_key": "keep-1",
                "change_hash": "b",
                "first_seen_at": "2026-04-01T00:00:00+09:00",
                "last_seen_at": "2026-04-01T00:00:00+09:00",
                "is_active": True,
                "missing_count": 0,
                "snapshot_date": "2026-04-01",
                "run_id": "r1",
                "source_url": "https://keep.example/jobs",
                "source_bucket": "approved",
                "source_name": "유지 테스트",
                "company_name": "회사B",
                "company_tier": "중견/중소",
                "job_title_raw": "Data Scientist",
                "experience_level_raw": "경력",
                "job_role": "데이터 사이언티스트",
                "job_url": "https://keep.example/jobs/1",
                "record_status": "유지",
                "주요업무_분석용": "모델을 개발합니다.",
                "자격요건_분석용": "파이썬 경험",
                "우대사항_분석용": "서비스 경험",
                "핵심기술_분석용": "파이썬",
                "상세본문_분석용": "모델을 개발합니다.\n파이썬 경험\n서비스 경험",
                "회사명_표시": "회사B",
                "소스명_표시": "유지 테스트",
                "공고제목_표시": "Data Scientist",
                "경력수준_표시": "경력",
                "경력근거_표시": "구조화 메타데이터",
                "채용트랙_표시": "일반채용",
                "채용트랙근거_표시": "기본추론",
                "직무초점_표시": "서비스개발",
                "직무초점근거_표시": "공고제목",
                "구분요약_표시": "경력",
                "직무명_표시": "데이터 사이언티스트",
                "주요업무_표시": "모델을 개발합니다.",
                "자격요건_표시": "파이썬 경험",
                "우대사항_표시": "서비스 경험",
                "핵심기술_표시": "파이썬",
            },
        ],
        columns=list(JOB_COLUMNS),
    )
    source_registry = pd.DataFrame(
        [
            {
                "source_url": "https://hold.example/jobs",
                "source_bucket": "approved",
                "verification_status": "성공",
            },
            {
                "source_url": "https://keep.example/jobs",
                "source_bucket": "approved",
                "verification_status": "성공",
            },
        ]
    )

    result = evaluate_quality_gate(staging, source_registry, already_filtered=True)

    assert result.passed is False
    assert "검증실패보류 carry-forward 비율이 너무 높습니다." in result.reasons


def test_resume_source_scan_offset_uses_surviving_remaining_cursor_when_registry_set_changes() -> None:
    progress = {
        "next_source_offset": 1,
        "next_source_cursor": "https://missing.example/jobs",
        "collectable_source_urls": [
            "https://a.example/jobs",
            "https://b.example/jobs",
            "https://c.example/jobs",
        ],
    }

    start_offset, strategy = collection_module._resume_source_scan_offset(
        progress,
        [
            "https://ats.example/jobs",
            "https://a.example/jobs",
            "https://c.example/jobs",
        ],
        registry_signature_changed=True,
    )

    assert start_offset == 2
    assert strategy == "surviving_remaining_cursor"


def test_resume_source_scan_offset_uses_saved_offset_when_registry_set_changes_without_survivor() -> None:
    progress = {
        "next_source_offset": 3,
        "next_source_cursor": "https://missing.example/jobs",
    }

    start_offset, strategy = collection_module._resume_source_scan_offset(
        progress,
        [
            "https://ats.example/jobs",
            "https://a.example/jobs",
            "https://b.example/jobs",
            "https://c.example/jobs",
            "https://d.example/jobs",
        ],
        registry_signature_changed=True,
    )

    assert start_offset == 3
    assert strategy == "offset_after_registry_change"


def test_resume_source_scan_offset_ignores_cursor_after_completed_full_scan() -> None:
    progress = {
        "next_source_offset": 0,
        "next_source_cursor": "https://c.example/jobs",
    }

    start_offset, strategy = collection_module._resume_source_scan_offset(
        progress,
        [
            "https://a.example/jobs",
            "https://b.example/jobs",
            "https://c.example/jobs",
        ],
        registry_signature_changed=False,
    )

    assert start_offset == 0
    assert strategy == "reset"


def test_resume_source_scan_offset_resets_when_registry_set_changes_without_progress() -> None:
    start_offset, strategy = collection_module._resume_source_scan_offset(
        {},
        [
            "https://ats.example/jobs",
            "https://a.example/jobs",
        ],
        registry_signature_changed=True,
    )

    assert start_offset == 0
    assert strategy == "reset_after_registry_change"


def test_ordered_collectable_positions_prioritizes_recruitish_html_pages_over_generic_html_pages() -> None:
    rows = [
        {
            "company_name": "Generic Agency",
            "source_bucket": "approved",
            "source_type": "html_page",
            "source_url": "https://agency.example.com/board/list.do",
            "verification_status": "성공",
            "last_active_job_count": 0,
            "last_success_at": "2026-04-02T00:00:00+09:00",
            "is_quarantined": False,
        },
        {
            "company_name": "Recruit Company",
            "source_bucket": "approved",
            "source_type": "html_page",
            "source_url": "https://company.example.com/recruit/list",
            "verification_status": "성공",
            "last_active_job_count": 0,
            "last_success_at": "2026-04-02T00:00:00+09:00",
            "is_quarantined": False,
        },
    ]

    ordered = collection_module._ordered_collectable_positions(rows)

    assert ordered == [1, 0]


def test_collect_jobs_from_sources_resets_progress_when_scan_policy_changes(
    monkeypatch: pytest.MonkeyPatch,
    sandbox_project: Path,
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    registry = pd.DataFrame(
        [
            {
                "company_name": "회사활성",
                "company_tier": "중견/중소",
                "source_name": "활성",
                "source_url": "https://hot.example/jobs",
                "source_type": "greenhouse",
                "source_bucket": "approved",
                "verification_status": "성공",
                "last_success_at": "2026-04-01T07:00:00+09:00",
                "last_active_job_count": 5,
            },
            {
                "company_name": "회사탐색",
                "company_tier": "중견/중소",
                "source_name": "탐색",
                "source_url": "https://explore.example/jobs",
                "source_type": "recruiter",
                "source_bucket": "approved",
                "verification_status": "성공",
                "last_success_at": "2026-04-01T07:00:00+09:00",
                "last_active_job_count": 0,
            },
        ]
    )
    for column in SOURCE_REGISTRY_COLUMNS:
        if column not in registry.columns:
            registry[column] = ""
    registry = registry[list(SOURCE_REGISTRY_COLUMNS)]

    paths.source_collection_progress_path.write_text(
        json.dumps(
            {
                "next_source_offset": 1,
                "next_source_cursor": "https://explore.example/jobs",
                "registry_signature": "legacy-signature",
                "completed_full_scan_count": 2,
                "collectable_source_urls": [
                    "https://hot.example/jobs",
                    "https://explore.example/jobs",
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    called_urls: list[str] = []

    def fake_fetch_source_content(url: str, *_args, **_kwargs) -> tuple[str, str]:
        called_urls.append(url)
        return "{\"jobs\": []}", "application/json"

    monkeypatch.setattr(collection_module, "fetch_source_content", fake_fetch_source_content)
    monkeypatch.setattr(collection_module, "parse_jobs_from_payload", lambda *args, **kwargs: [])

    settings = AppSettings(
        use_mock_sources=True,
        enable_gemini_fallback=False,
        job_collection_source_batch_size=1,
        job_collection_source_max_batches_per_run=1,
        job_collection_max_runtime_seconds=120,
    )

    _, _, _, summary = collection_module.collect_jobs_from_sources(
        registry,
        paths,
        settings,
        run_id="run-1",
        snapshot_date="2026-04-01",
        collected_at="2026-04-01T08:00:00+09:00",
        enable_source_scan_progress=True,
    )

    assert called_urls == ["https://hot.example/jobs"]
    assert summary["source_scan_start_offset"] == 0
    assert summary["source_scan_resume_strategy"] == "policy_reset"


def test_select_incremental_collectable_positions_always_include_forced_refresh_sources() -> None:
    rows = [
        {
            "source_url": "https://first.example/jobs",
            "source_bucket": "approved",
            "source_type": "html_page",
            "_always_refresh_source": False,
        },
        {
            "source_url": "https://forced.example/jobs",
            "source_bucket": "approved",
            "source_type": "greetinghr",
            "_always_refresh_source": True,
        },
        {
            "source_url": "https://third.example/jobs",
            "source_bucket": "approved",
            "source_type": "html_page",
            "_always_refresh_source": False,
        },
    ]

    selected_positions, cursor_positions, pinned_positions = collection_module._select_incremental_collectable_positions(
        [0, 1, 2],
        rows,
        start_offset=2,
        process_limit=1,
    )

    assert selected_positions == [1]
    assert cursor_positions == []
    assert pinned_positions == [1]


def _no_search_refresh_summary() -> dict[str, int | list]:
    return {
        "search_query_count": 0,
        "search_query_start_offset": 0,
        "search_query_next_offset": 0,
        "search_query_batch_count": 0,
        "search_query_batch_size": 0,
        "search_discovered_seed_source_count": 0,
        "discovered_seed_source_count": 0,
        "newly_discovered_seed_source_count": 0,
        "auto_promoted_shadow_seed_source_count": 0,
        "duplicate_shadow_seed_source_count": 0,
        "invalid_shadow_seed_source_count": 0,
        "invalid_shadow_seed_sources": [],
    }


def test_run_collection_cycle_pipeline_bootstrap_and_incremental(
    monkeypatch: pytest.MonkeyPatch,
    sandbox_project: Path,
) -> None:
    monkeypatch.setattr(pipelines_module, "refresh_company_seed_sources", lambda *args, **kwargs: _no_search_refresh_summary())
    bootstrap_summary = run_collection_cycle_pipeline(sync_sheets=False, project_root=sandbox_project)
    paths = ProjectPaths.from_root(sandbox_project)

    assert bootstrap_summary["run_mode"] == "bootstrap"
    assert bootstrap_summary["checklist"]["후보군_재확장"] is True
    assert bootstrap_summary["checklist"]["기업근거_재수집"] is True
    assert bootstrap_summary["checklist"]["승인기업_재선별"] is True
    assert bootstrap_summary["checklist"]["공식소스_재탐색"] is True
    assert bootstrap_summary["checklist"]["공식소스_검증"] is True
    assert bootstrap_summary["checklist"]["모집단_수집_또는_증분"] is True
    assert bootstrap_summary["candidate_expansion"]["expanded_candidate_company_count"] > 0
    assert "auto_promoted_shadow_seed_source_count" in bootstrap_summary["candidate_expansion"]
    assert bootstrap_summary["checklist"]["시트동기화"] is False
    assert bootstrap_summary["automation_ready"] is False

    samsung_fixture = sandbox_project / "tests" / "fixtures" / "sources" / "samsung_jobs.json"
    samsung_fixture.write_text(json.dumps({"jobs": []}, ensure_ascii=False), encoding="utf-8")

    incremental_summary = run_collection_cycle_pipeline(sync_sheets=False, project_root=sandbox_project)
    staging = read_csv_or_empty(paths.staging_jobs_path)

    assert incremental_summary["run_mode"] in {"incremental", "bootstrap_resume"}
    assert incremental_summary["checklist"]["후보군_재확장"] is True
    assert incremental_summary["collection"]["staging_job_count"] >= 1
    assert incremental_summary["checklist"]["시트동기화"] is False
    assert incremental_summary["automation_ready"] is False
    assert "미발견" in staging["record_status"].tolist()
    assert incremental_summary["collection"]["held_job_count"] == 0
    assert incremental_summary["collection"]["dropped_low_quality_job_count"] >= 0


def test_run_collection_cycle_summary_exit_code_ignores_automation_ready() -> None:
    assert cli_module._summary_exit_code("run-collection-cycle", {"automation_ready": False}) == 0
    assert cli_module._summary_exit_code("run-collection-cycle", {"automation_ready": True}) == 0


def test_github_actions_runtime_derives_bootstrap_hold_status() -> None:
    status = github_actions_runtime_module.derive_cycle_status(
        {
            "run_mode": "bootstrap_resume",
            "automation_ready": False,
            "published_state": {
                "promotion_block_reason": "bootstrap_source_scan_incomplete",
                "collection_ready": True,
                "collection_ready_reason": "",
            },
            "collection": {
                "quality_gate_passed": True,
                "completed_full_source_scan": False,
            },
        },
        exit_code=0,
    )

    assert status.hold_reason == "bootstrap_source_scan_incomplete"
    assert status.should_save_state is True
    assert status.is_failure is False
    assert status.is_recovered is False


def test_github_actions_runtime_capture_and_resolve_status_from_runtime_files(tmp_path: Path) -> None:
    project_root = tmp_path / "jobs_market_v2"
    runtime_dir = project_root / "runtime"
    runtime_dir.mkdir(parents=True)
    summary_path = runtime_dir / "github_workflow_cycle_summary.json"
    status_path = runtime_dir / "github_workflow_cycle_status.json"

    summary_path.write_text(
        json.dumps(
            {
                "run_mode": "incremental",
                "automation_ready": True,
                "published_state": {
                    "promotion_block_reason": "",
                    "collection_ready": True,
                    "collection_ready_reason": "",
                },
                "collection": {
                    "quality_gate_passed": True,
                    "completed_full_source_scan": True,
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    captured = github_actions_runtime_module.capture_cycle_status(project_root, summary_path, status_path, exit_code=0)
    resolved = github_actions_runtime_module.resolve_cycle_status(project_root, status_path)

    assert captured.exit_code == 0
    assert captured.quality_gate_passed is True
    assert captured.hold_reason == ""
    assert resolved.github_output_values()["should_save_state"] == "true"
    assert resolved.github_output_values()["is_failure"] == "false"


def test_github_actions_runtime_write_cycle_status_round_trip(tmp_path: Path) -> None:
    status_path = tmp_path / "github_workflow_cycle_status.json"

    written = github_actions_runtime_module.write_cycle_status(
        status_path,
        exit_code=0,
        quality_gate_passed=True,
        automation_ready=True,
    )
    resolved = github_actions_runtime_module.resolve_cycle_status(tmp_path, status_path)

    assert written.exit_code == 0
    assert written.quality_gate_passed is True
    assert resolved.is_failure is False
    assert resolved.github_output_values()["should_save_state"] == "true"


def test_github_actions_runtime_build_workflow_state_payload_preserves_metadata() -> None:
    status = github_actions_runtime_module.WorkflowCycleStatus(exit_code=0, quality_gate_passed=True)
    payload = github_actions_runtime_module.build_workflow_state_payload(
        {"consecutive_failures": 9, "seeded_from": "local_validated_runtime_minimal"},
        status,
        run_url="https://example.com/runs/1",
        runtime_status={"ok": True},
    )

    assert payload["consecutive_failures"] == 0
    assert payload["last_result"] == "success"
    assert payload["run_url"] == "https://example.com/runs/1"
    assert payload["seeded_from"] == "local_validated_runtime_minimal"
    assert payload["automation_status"] == {"ok": True}


def test_github_actions_runtime_write_actions_env_file_materializes_multiline_service_account(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / ".env"
    raw_json = json.dumps(
        {"type": "service_account", "project_id": "demo", "private_key": "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----\n"},
        ensure_ascii=False,
        indent=2,
    )

    lines = github_actions_runtime_module.write_actions_env_file(
        env_path,
        spreadsheet_id="sheet-id",
        service_account_json=raw_json,
        gemini_api_key="gem-key",
    )

    env_text = env_path.read_text(encoding="utf-8")
    service_account_path = tmp_path / ".ci" / "google_service_account.json"

    assert service_account_path.exists()
    assert json.loads(service_account_path.read_text(encoding="utf-8"))["project_id"] == "demo"
    assert f"GOOGLE_SERVICE_ACCOUNT_JSON='{service_account_path}'" in env_text
    assert any(line.startswith("GOOGLE_SERVICE_ACCOUNT_JSON=") for line in lines)


def test_github_actions_runtime_write_actions_env_file_handles_long_raw_service_account_json(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / ".env"
    raw_json = json.dumps(
        {
            "type": "service_account",
            "project_id": "demo",
            "private_key": "-----BEGIN PRIVATE KEY-----\n" + ("A" * 5000) + "\n-----END PRIVATE KEY-----\n",
            "client_email": "robot@example.com",
        },
        ensure_ascii=False,
        indent=2,
    )

    github_actions_runtime_module.write_actions_env_file(
        env_path,
        spreadsheet_id="sheet-id",
        service_account_json=raw_json,
        gemini_api_key="gem-key",
    )

    service_account_path = tmp_path / ".ci" / "google_service_account.json"
    assert service_account_path.exists()
    assert json.loads(service_account_path.read_text(encoding="utf-8"))["client_email"] == "robot@example.com"


def test_github_actions_runtime_write_actions_env_file_includes_generic_llm_config(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / ".env"
    raw_json = json.dumps({"type": "service_account", "project_id": "demo"}, ensure_ascii=False)

    github_actions_runtime_module.write_actions_env_file(
        env_path,
        spreadsheet_id="sheet-id",
        service_account_json=raw_json,
        llm_provider="openai_compatible",
        llm_base_url="https://api.vibemakers.kr",
        llm_api_key="vibe-key",
        llm_model="gemma-4-31b",
    )

    env_text = env_path.read_text(encoding="utf-8")
    assert "JOBS_MARKET_V2_LLM_PROVIDER='openai_compatible'" in env_text
    assert "JOBS_MARKET_V2_LLM_BASE_URL='https://api.vibemakers.kr'" in env_text
    assert "JOBS_MARKET_V2_LLM_API_KEY='vibe-key'" in env_text
    assert "JOBS_MARKET_V2_LLM_MODEL='gemma-4-31b'" in env_text


def test_github_actions_runtime_build_failure_state_payload_increments_failure_streak() -> None:
    payload = github_actions_runtime_module.build_failure_state_payload(
        {"consecutive_failures": 2, "seeded_from": "local_validated_runtime_minimal", "hold_reason": "old"},
        run_url="https://example.com/runs/2",
    )

    assert payload["consecutive_failures"] == 3
    assert payload["last_result"] == "failure"
    assert payload["hold_reason"] == ""
    assert payload["seeded_from"] == "local_validated_runtime_minimal"


def test_github_actions_runtime_finalize_cycle_records_success_without_step_outputs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "jobs_market_v2"
    runtime_dir = project_root / "runtime"
    runtime_dir.mkdir(parents=True)
    status_path = runtime_dir / "github_workflow_cycle_status.json"
    state_path = tmp_path / "automation-state" / "workflow_state.json"

    status_path.write_text(
        json.dumps(
            {
                "exit_code": 0,
                "quality_gate_passed": True,
                "hold_reason": "",
                "promotion_block_reason": "",
                "automation_ready": True,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    closed_calls: list[tuple[str, str, str]] = []

    def _close_issue(api_url: str, repo: str, token: str) -> bool:
        closed_calls.append((api_url, repo, token))
        return True

    monkeypatch.setattr(github_actions_runtime_module, "close_incident_issue", _close_issue)

    result = github_actions_runtime_module.finalize_cycle(
        project_root=project_root,
        status_path=status_path,
        state_path=state_path,
        run_url="https://example.com/runs/3",
        runtime_status_path=runtime_dir / "automation_status.json",
        api_url="https://api.github.com",
        repo="owner/repo",
        token="token",
    )

    assert result["is_failure"] is False
    assert result["should_save_state"] is True
    assert result["last_result"] == "success"
    assert result["issue_closed"] is True
    assert state_path.exists()
    assert closed_calls == [("https://api.github.com", "owner/repo", "token")]


def test_call_gemini_supports_openai_compatible_chat_completions(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {
                "choices": [
                    {
                        "message": {
                            "content": '{"main_tasks":"데이터 파이프라인 구축","requirements":"Python 경험","preferred":"ML 경험","core_skills":"Python\\nSQL"}'
                        }
                    }
                ]
            }

    def fake_post(url: str, **kwargs) -> FakeResponse:
        captured["url"] = url
        captured["kwargs"] = kwargs
        return FakeResponse()

    monkeypatch.setattr(gemini_module.httpx, "post", fake_post)

    settings = AppSettings(
        llm_provider="openai_compatible",
        llm_base_url="https://api.vibemakers.kr",
        llm_api_key="vibe-key",
        llm_model="gemma-4-31b",
        gemini_timeout_seconds=15.0,
        connect_timeout_seconds=5.0,
    )

    result = gemini_module._call_gemini(
        {
            "main_tasks": "Build data pipelines",
            "requirements": "Python",
            "preferred": "ML",
            "core_skills": "Python, SQL",
            "description_text": "",
        },
        settings,
    )

    assert captured["url"] == "https://api.vibemakers.kr/v1/chat/completions"
    request_kwargs = captured["kwargs"]
    assert request_kwargs["headers"]["Authorization"] == "Bearer vibe-key"
    assert request_kwargs["json"]["model"] == "gemma-4-31b"
    assert request_kwargs["json"]["messages"][0]["role"] == "system"
    assert result["main_tasks"] == "데이터 파이프라인 구축"


def test_get_settings_uses_published_llm_backend_defaults(
    monkeypatch: pytest.MonkeyPatch,
    sandbox_project: Path,
) -> None:
    (sandbox_project / "config" / "published_llm_backend.json").write_text(
        json.dumps(
            {
                "provider": "openai_compatible",
                "base_url": "https://api.vibemakers.kr",
                "model": "gemma-4-31b",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    settings_module = sys.modules["jobs_market_v2.settings"]
    settings_module.get_settings.cache_clear()
    settings_module.get_paths.cache_clear()
    monkeypatch.delenv("JOBS_MARKET_V2_LLM_PROVIDER", raising=False)
    monkeypatch.delenv("JOBS_MARKET_V2_LLM_BASE_URL", raising=False)
    monkeypatch.delenv("JOBS_MARKET_V2_LLM_MODEL", raising=False)

    settings = settings_module.get_settings(sandbox_project)

    assert settings.llm_provider == "openai_compatible"
    assert settings.llm_base_url == "https://api.vibemakers.kr"
    assert settings.llm_model == "gemma-4-31b"


def test_get_settings_auto_enables_llm_fallback_when_generic_api_key_present(
    monkeypatch: pytest.MonkeyPatch,
    sandbox_project: Path,
) -> None:
    (sandbox_project / ".env").write_text(
        "JOBS_MARKET_V2_USE_MOCK_SOURCES=true\n"
        "JOBS_MARKET_V2_ENABLE_FALLBACK_SOURCE_GUESS=false\n",
        encoding="utf-8",
    )
    settings_module = sys.modules["jobs_market_v2.settings"]
    settings_module.get_settings.cache_clear()
    settings_module.get_paths.cache_clear()
    monkeypatch.setenv("JOBS_MARKET_V2_LLM_API_KEY", "llm-key")
    monkeypatch.delenv("JOBS_MARKET_V2_ENABLE_LLM_FALLBACK", raising=False)
    monkeypatch.delenv("JOBS_MARKET_V2_ENABLE_GEMINI_FALLBACK", raising=False)

    settings = settings_module.get_settings(sandbox_project)

    assert settings.enable_gemini_fallback is True
    assert settings.llm_api_key == "llm-key"


def test_get_settings_prefers_generic_llm_aliases_for_flags_and_limits(
    monkeypatch: pytest.MonkeyPatch,
    sandbox_project: Path,
) -> None:
    settings_module = sys.modules["jobs_market_v2.settings"]
    settings_module.get_settings.cache_clear()
    settings_module.get_paths.cache_clear()
    monkeypatch.setenv("JOBS_MARKET_V2_LLM_API_KEY", "llm-key")
    monkeypatch.setenv("JOBS_MARKET_V2_ENABLE_LLM_FALLBACK", "true")
    monkeypatch.setenv("JOBS_MARKET_V2_ENABLE_LLM_DUPLICATE_ADJUDICATION", "true")
    monkeypatch.setenv("JOBS_MARKET_V2_LLM_MAX_CALLS_PER_RUN", "11")
    monkeypatch.setenv("JOBS_MARKET_V2_LLM_TIMEOUT_SECONDS", "9")
    monkeypatch.setenv("JOBS_MARKET_V2_ENABLE_GEMINI_FALLBACK", "false")
    monkeypatch.setenv("JOBS_MARKET_V2_GEMINI_MAX_CALLS_PER_RUN", "3")
    monkeypatch.setenv("JOBS_MARKET_V2_GEMINI_TIMEOUT_SECONDS", "4")

    settings = settings_module.get_settings(sandbox_project)

    assert settings.enable_gemini_fallback is True
    assert settings.enable_gemini_duplicate_adjudication is True
    assert settings.gemini_max_calls_per_run == 11
    assert settings.gemini_timeout_seconds == 9.0


def test_github_actions_runtime_finalize_cycle_records_failure_and_notifies(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "jobs_market_v2"
    runtime_dir = project_root / "runtime"
    runtime_dir.mkdir(parents=True)
    status_path = runtime_dir / "github_workflow_cycle_status.json"
    state_path = tmp_path / "automation-state" / "workflow_state.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps({"consecutive_failures": 2, "last_result": "failure"}, ensure_ascii=False),
        encoding="utf-8",
    )
    status_path.write_text(
        json.dumps(
            {
                "exit_code": 1,
                "quality_gate_passed": False,
                "hold_reason": "",
                "promotion_block_reason": "",
                "automation_ready": False,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    opened: list[tuple[str, str, str, str, int]] = []
    slack_calls: list[tuple[str, str, int]] = []

    def _open_issue(api_url: str, repo: str, token: str, run_url: str, failure_streak: int) -> int:
        opened.append((api_url, repo, token, run_url, failure_streak))
        return 7

    def _notify(webhook_url: str, run_url: str, failure_streak: int) -> None:
        slack_calls.append((webhook_url, run_url, failure_streak))

    monkeypatch.setattr(github_actions_runtime_module, "open_or_update_incident_issue", _open_issue)
    monkeypatch.setattr(github_actions_runtime_module, "send_slack_notification", _notify)

    result = github_actions_runtime_module.finalize_cycle(
        project_root=project_root,
        status_path=status_path,
        state_path=state_path,
        run_url="https://example.com/runs/4",
        runtime_status_path=runtime_dir / "automation_status.json",
        api_url="https://api.github.com",
        repo="owner/repo",
        token="token",
        slack_webhook_url="https://hooks.slack.com/services/test",
    )

    assert result["is_failure"] is True
    assert result["should_save_state"] is False
    assert result["failure_streak"] == 3
    assert result["issue_number"] == 7
    assert result["slack_notified"] is True
    assert opened == [("https://api.github.com", "owner/repo", "token", "https://example.com/runs/4", 3)]
    assert slack_calls == [("https://hooks.slack.com/services/test", "https://example.com/runs/4", 3)]


def test_run_collection_cycle_pipeline_bootstrap_resume_until_full_scan(
    monkeypatch: pytest.MonkeyPatch,
    sandbox_project: Path,
) -> None:
    monkeypatch.setattr(pipelines_module, "refresh_company_seed_sources", lambda *args, **kwargs: _no_search_refresh_summary())
    env_path = sandbox_project / ".env"
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "JOBS_MARKET_V2_JOB_COLLECTION_SOURCE_BATCH_SIZE=3\n"
        + "JOBS_MARKET_V2_JOB_COLLECTION_SOURCE_MAX_BATCHES_PER_RUN=1\n"
        + "JOBS_MARKET_V2_JOB_COLLECTION_MAX_RUNTIME_SECONDS=120\n",
        encoding="utf-8",
    )
    pipelines_module.get_settings.cache_clear()

    summaries = [run_collection_cycle_pipeline(sync_sheets=False, project_root=sandbox_project) for _ in range(7)]
    first_summary = summaries[0]
    second_summary = summaries[1]
    paths = ProjectPaths.from_root(sandbox_project)

    assert first_summary["run_mode"] == "bootstrap"
    assert first_summary["promotion"]["promotion_skipped"] is True
    assert first_summary["promotion"]["promotion_skipped_reason"] == "bootstrap_source_scan_incomplete"
    assert first_summary["collection"]["source_scan_next_offset"] == 3
    assert first_summary["collection"]["completed_full_source_scan"] is False

    assert second_summary["run_mode"] == "bootstrap_resume"
    assert second_summary["collection"]["incremental_baseline_mode"] == "staging"
    assert second_summary["collection"]["source_scan_start_offset"] == 3
    assert second_summary["promotion"]["promotion_skipped"] is True

    resume_summaries = summaries[1:]
    completed_index = next(
        (index for index, summary in enumerate(resume_summaries) if summary["collection"]["completed_full_source_scan"]),
        None,
    )
    assert completed_index is not None
    assert completed_index < len(resume_summaries) - 1
    for summary in resume_summaries[:completed_index]:
        assert summary["promotion"]["promotion_skipped"] is True
        assert summary["promotion"]["promotion_skipped_reason"] == "bootstrap_source_scan_incomplete"

    completion_summary = resume_summaries[completed_index]
    post_completion_summary = resume_summaries[completed_index + 1]

    assert completion_summary["run_mode"] == "bootstrap_resume"
    if completion_summary["collection"]["quality_gate_passed"]:
        assert completion_summary["promotion"]["promoted_job_count"] > 0
    else:
        assert completion_summary["promotion"]["promoted_job_count"] == 0

    assert post_completion_summary["run_mode"] in {"bootstrap_resume", "incremental"}
    assert post_completion_summary["collection"]["source_scan_start_offset"] == 0
    assert post_completion_summary["collection"]["completed_full_source_scan"] is False
    assert paths.source_collection_progress_path.exists()


def test_run_collection_cycle_pipeline_skips_collection_when_published_state_is_not_ready(
    monkeypatch: pytest.MonkeyPatch,
    sandbox_project: Path,
) -> None:
    monkeypatch.setattr(pipelines_module, "refresh_company_seed_sources", lambda *args, **kwargs: _no_search_refresh_summary())
    env_path = sandbox_project / ".env"
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "JOBS_MARKET_V2_COMPANY_EVIDENCE_BATCH_SIZE=2\n"
        + "JOBS_MARKET_V2_COMPANY_EVIDENCE_MAX_BATCHES_PER_RUN=1\n",
        encoding="utf-8",
    )
    pipelines_module.get_settings.cache_clear()

    summary = run_collection_cycle_pipeline(sync_sheets=False, project_root=sandbox_project)
    paths = ProjectPaths.from_root(sandbox_project)

    assert summary["company_evidence"]["published_company_state"] is False
    assert summary["published_state"]["collection_ready"] is False
    assert summary["published_state"]["collection_ready_reason"] == "published_source_registry_unavailable_while_company_scan_in_progress"
    assert summary["collection"]["collection_state"] == "published_source_registry_unavailable_while_company_scan_in_progress"
    assert summary["promotion"]["promotion_skipped"] is True
    assert not paths.source_collection_progress_path.exists()
    assert read_csv_or_empty(paths.source_registry_path, SOURCE_REGISTRY_COLUMNS).empty


def test_run_collection_cycle_pipeline_uses_in_progress_registry_during_partial_company_scan(
    monkeypatch: pytest.MonkeyPatch,
    sandbox_project: Path,
) -> None:
    monkeypatch.setattr(pipelines_module, "refresh_company_seed_sources", lambda *args, **kwargs: _no_search_refresh_summary())
    env_path = sandbox_project / ".env"
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "JOBS_MARKET_V2_COMPANY_EVIDENCE_BATCH_SIZE=2\n"
        + "JOBS_MARKET_V2_COMPANY_EVIDENCE_MAX_BATCHES_PER_RUN=1\n",
        encoding="utf-8",
    )
    pipelines_module.get_settings.cache_clear()

    paths = ProjectPaths.from_root(sandbox_project)
    in_progress_candidates_path = paths.runtime_dir / "company_candidates_in_progress.csv"
    in_progress_registry_path = paths.runtime_dir / "source_registry_in_progress.csv"
    write_csv(
        pd.DataFrame(
            [
                {
                    "company_name": "알파",
                    "company_tier": "중견/중소",
                    "official_domain": "alpha.example",
                    "company_name_en": "",
                    "region": "",
                    "aliases": "",
                    "discovery_method": "seed",
                    "candidate_seed_type": "html_table_url",
                    "candidate_seed_url": "https://alpha.example/seed",
                    "candidate_seed_title": "알파",
                    "candidate_seed_reason": "테스트",
                    "officiality_score": 4,
                    "source_seed_count": 1,
                    "verified_source_count": 1,
                    "active_job_count": 0,
                    "role_analyst_signal": 0,
                    "role_ds_signal": 0,
                    "role_researcher_signal": 0,
                    "role_ai_engineer_signal": 1,
                    "role_fit_score": 1,
                    "hiring_signal_score": 1,
                    "evidence_count": 1,
                    "primary_evidence_type": "company_registry",
                    "primary_evidence_url": "https://alpha.example",
                    "primary_evidence_text": "알파",
                    "company_bucket": "approved",
                    "reject_reason": "",
                    "last_verified_at": "2026-04-01T00:00:00+09:00",
                }
            ]
        ),
        in_progress_candidates_path,
    )
    write_csv(
        pd.DataFrame(
            [
                {
                    "company_name": "알파",
                    "company_tier": "중견/중소",
                    "source_name": "알파 채용",
                    "source_url": "https://alpha.example/jobs",
                    "source_type": "greenhouse",
                    "official_domain": "alpha.example",
                    "is_official_hint": True,
                    "structure_hint": "html",
                    "discovery_method": "test",
                    "source_bucket": "approved",
                    "screening_reason": "",
                    "verification_status": "",
                    "verification_note": "",
                    "last_checked_at": "",
                    "last_success_at": "",
                    "failure_count": 0,
                    "last_active_job_count": 0,
                    "source_quality_score": 0,
                    "quarantine_reason": "",
                    "is_quarantined": False,
                }
            ]
        ),
        in_progress_registry_path,
    )

    captured: dict[str, object] = {}

    def fake_collect_jobs_pipeline(
        *,
        dry_run: bool = False,
        project_root: Path | None = None,
        allow_source_discovery_fallback: bool = True,
        enable_source_scan_progress: bool = True,
        registry_frame: pd.DataFrame | None = None,
        registry_output_path: Path | None = None,
    ) -> dict:
        captured["registry_frame"] = registry_frame.copy() if registry_frame is not None else None
        captured["registry_output_path"] = registry_output_path
        return {
            "collection_mode": "live",
            "collected_job_count": 1,
            "verified_source_success_count": 1,
            "verified_source_failure_count": 0,
            "quality_gate_passed": True,
            "quality_gate_reasons": [],
            "staging_job_count": 1,
            "completed_full_source_scan": True,
            "promoted_job_count": 0,
        }

    monkeypatch.setattr(pipelines_module, "collect_jobs_pipeline", fake_collect_jobs_pipeline)
    monkeypatch.setattr(pipelines_module, "build_coverage_report_pipeline", lambda *args, **kwargs: {"report": "ok"})
    monkeypatch.setattr(
        pipelines_module,
        "promote_staging_pipeline",
        lambda *args, **kwargs: {"promoted_job_count": 0, "promotion_skipped": False},
    )

    summary = run_collection_cycle_pipeline(sync_sheets=False, project_root=sandbox_project)

    assert summary["company_evidence"]["published_company_state"] is False
    assert summary["published_state"]["collection_ready"] is True
    assert summary["published_state"]["collection_ready_reason"] == "reuse_in_progress_source_registry_during_partial_company_scan"
    assert summary["company_screening"]["company_state_mode"] == "reuse_in_progress_partial_scan"
    assert summary["source_discovery"]["company_input_mode"] == "reuse_in_progress_partial_scan"
    assert captured["registry_output_path"] == in_progress_registry_path
    registry_frame = captured["registry_frame"]
    assert registry_frame is not None
    assert list(registry_frame["source_url"]) == ["https://alpha.example/jobs"]


def test_missing_count_increments_only_for_verified_sources() -> None:
    baseline = pd.DataFrame(
        [
            {
                "job_key": "a",
                "change_hash": "1",
                "first_seen_at": "2026-03-29T00:00:00+09:00",
                "last_seen_at": "2026-03-29T00:00:00+09:00",
                "is_active": True,
                "missing_count": 0,
                "snapshot_date": "2026-03-29",
                "run_id": "r1",
                "source_url": "https://a.example/jobs",
                "source_bucket": "approved",
                "source_name": "테스트",
                "company_name": "테스트",
                "company_tier": "중견/중소",
                "job_title_raw": "데이터 분석가",
                "experience_level_raw": "경력",
                "job_role": "데이터 분석가",
                "job_url": "",
                "record_status": "신규",
                "회사명_표시": "테스트",
                "소스명_표시": "테스트",
                "공고제목_표시": "데이터 분석가",
                "경력수준_표시": "경력",
                "직무명_표시": "데이터 분석가",
                "주요업무_표시": "분석",
                "자격요건_표시": "SQL",
                "우대사항_표시": "",
                "핵심기술_표시": "SQL",
            }
        ]
    )
    merged = merge_incremental(
        baseline,
        pd.DataFrame(columns=baseline.columns),
        {"https://a.example/jobs"},
        "r2",
        "2026-03-30",
        "2026-03-30T00:00:00+09:00",
    )
    assert int(merged.iloc[0]["missing_count"]) == 1
    assert merged.iloc[0]["record_status"] == "미발견"


def test_merge_incremental_collapses_legacy_company_alias_duplicates_by_job_url() -> None:
    baseline = pd.DataFrame(
        [
            {
                "job_key": "legacy-a",
                "change_hash": "1",
                "first_seen_at": "2026-03-29T00:00:00+09:00",
                "last_seen_at": "2026-03-29T00:00:00+09:00",
                "is_active": True,
                "missing_count": 0,
                "snapshot_date": "2026-03-29",
                "run_id": "r1",
                "source_url": "https://example.com/feed",
                "source_bucket": "approved",
                "source_name": "테스트",
                "company_name": "이지케어텍 (주)",
                "company_tier": "중견/중소",
                "job_title_raw": "[경력] AI Engineer",
                "experience_level_raw": "경력",
                "job_role": "인공지능 엔지니어",
                "job_url": "https://example.com/jobs/1",
                "record_status": "유지",
            },
            {
                "job_key": "legacy-b",
                "change_hash": "1",
                "first_seen_at": "2026-03-29T00:00:00+09:00",
                "last_seen_at": "2026-03-29T00:00:00+09:00",
                "is_active": True,
                "missing_count": 0,
                "snapshot_date": "2026-03-29",
                "run_id": "r1",
                "source_url": "https://example.com/feed",
                "source_bucket": "approved",
                "source_name": "테스트",
                "company_name": "이지케어텍",
                "company_tier": "중견/중소",
                "job_title_raw": "[경력] AI Engineer",
                "experience_level_raw": "경력",
                "job_role": "인공지능 엔지니어",
                "job_url": "https://example.com/jobs/1",
                "record_status": "유지",
            },
        ]
    )
    new = pd.DataFrame(
        [
            {
                "job_key": "",
                "change_hash": "1",
                "first_seen_at": "2026-04-01T00:00:00+09:00",
                "last_seen_at": "2026-04-01T00:00:00+09:00",
                "is_active": True,
                "missing_count": 0,
                "snapshot_date": "2026-04-01",
                "run_id": "r2",
                "source_url": "https://example.com/feed",
                "source_bucket": "approved",
                "source_name": "테스트",
                "company_name": "이지케어텍",
                "company_tier": "중견/중소",
                "job_title_raw": "[경력] AI Engineer",
                "experience_level_raw": "경력",
                "job_role": "인공지능 엔지니어",
                "job_url": "https://example.com/jobs/1",
                "record_status": "신규",
            }
        ]
    )
    merged = merge_incremental(
        baseline,
        new,
        {"https://example.com/feed"},
        "r2",
        "2026-04-01",
        "2026-04-01T00:00:00+09:00",
    )
    assert len(merged) == 1
    assert merged.iloc[0]["job_url"] == "https://example.com/jobs/1"


def test_merge_incremental_drops_legacy_same_page_base_url_when_fragmentized_replacement_exists() -> None:
    baseline = pd.DataFrame(
        [
            {
                "job_key": "legacy-role",
                "change_hash": "1",
                "first_seen_at": "2026-04-02T00:00:00+09:00",
                "last_seen_at": "2026-04-02T00:00:00+09:00",
                "is_active": True,
                "missing_count": 0,
                "snapshot_date": "2026-04-02",
                "run_id": "r1",
                "source_url": "https://example.com/recruit",
                "source_bucket": "approved",
                "source_name": "테스트",
                "company_name": "테스트랩",
                "company_tier": "스타트업",
                "job_title_raw": "서비스 개발자(Service Developer)",
                "experience_level_raw": "",
                "job_role": "인공지능 엔지니어",
                "job_url": "https://example.com/recruit",
                "record_status": "유지",
            }
        ]
    )
    new = pd.DataFrame(
        [
            {
                "job_key": "",
                "change_hash": "2",
                "first_seen_at": "2026-04-03T00:00:00+09:00",
                "last_seen_at": "2026-04-03T00:00:00+09:00",
                "is_active": True,
                "missing_count": 0,
                "snapshot_date": "2026-04-03",
                "run_id": "r2",
                "source_url": "https://example.com/recruit",
                "source_bucket": "approved",
                "source_name": "테스트",
                "company_name": "테스트랩",
                "company_tier": "스타트업",
                "job_title_raw": "AI 개발자(AI Model Developer/Researcher)",
                "experience_level_raw": "",
                "job_role": "인공지능 엔지니어",
                "job_url": "https://example.com/recruit#role-ai-developer",
                "record_status": "신규",
            }
        ]
    )
    merged = merge_incremental(
        baseline,
        new,
        {"https://example.com/recruit"},
        "r2",
        "2026-04-03",
        "2026-04-03T00:00:00+09:00",
    )
    assert len(merged) == 1
    assert merged.iloc[0]["job_title_raw"] == "AI 개발자(AI Model Developer/Researcher)"
    assert merged.iloc[0]["job_url"] == "https://example.com/recruit#role-ai-developer"


def test_two_consecutive_missing_deactivates_job() -> None:
    baseline = pd.DataFrame(
        [
            {
                "job_key": "a",
                "change_hash": "1",
                "first_seen_at": "2026-03-29T00:00:00+09:00",
                "last_seen_at": "2026-03-29T00:00:00+09:00",
                "is_active": True,
                "missing_count": 1,
                "snapshot_date": "2026-03-29",
                "run_id": "r1",
                "source_url": "https://a.example/jobs",
                "source_bucket": "approved",
                "source_name": "테스트",
                "company_name": "테스트",
                "company_tier": "지역기업",
                "job_title_raw": "인공지능 엔지니어",
                "experience_level_raw": "경력",
                "job_role": "인공지능 엔지니어",
                "job_url": "",
                "record_status": "미발견",
                "회사명_표시": "테스트",
                "소스명_표시": "테스트",
                "공고제목_표시": "인공지능 엔지니어",
                "경력수준_표시": "경력",
                "직무명_표시": "인공지능 엔지니어",
                "주요업무_표시": "개발",
                "자격요건_표시": "파이썬",
                "우대사항_표시": "",
                "핵심기술_표시": "파이썬",
            }
        ]
    )
    merged = merge_incremental(
        baseline,
        pd.DataFrame(columns=baseline.columns),
        {"https://a.example/jobs"},
        "r2",
        "2026-03-30",
        "2026-03-30T00:00:00+09:00",
    )
    assert int(merged.iloc[0]["missing_count"]) == 2
    assert bool(merged.iloc[0]["is_active"]) is False


def test_merge_incremental_preserves_unattempted_active_rows_without_hold_downgrade() -> None:
    baseline = pd.DataFrame(
        [
            {
                "job_key": "a",
                "change_hash": "1",
                "first_seen_at": "2026-03-29T00:00:00+09:00",
                "last_seen_at": "2026-03-29T00:00:00+09:00",
                "is_active": True,
                "missing_count": 0,
                "snapshot_date": "2026-03-29",
                "run_id": "r1",
                "source_url": "https://a.example/jobs",
                "source_bucket": "approved",
                "source_name": "테스트",
                "company_name": "테스트",
                "company_tier": "지역기업",
                "job_title_raw": "인공지능 엔지니어",
                "experience_level_raw": "경력",
                "job_role": "인공지능 엔지니어",
                "job_url": "",
                "record_status": "검증실패보류",
                "회사명_표시": "테스트",
                "소스명_표시": "테스트",
                "공고제목_표시": "인공지능 엔지니어",
                "경력수준_표시": "경력",
                "직무명_표시": "인공지능 엔지니어",
                "주요업무_표시": "개발",
                "자격요건_표시": "파이썬",
                "우대사항_표시": "",
                "핵심기술_표시": "파이썬",
            }
        ]
    )

    merged = merge_incremental(
        baseline,
        pd.DataFrame(columns=baseline.columns),
        {},
        "r2",
        "2026-03-30",
        "2026-03-30T00:00:00+09:00",
    )

    assert merged.iloc[0]["record_status"] == "유지"
    assert int(merged.iloc[0]["missing_count"]) == 0
    assert bool(merged.iloc[0]["is_active"]) is True


def test_merge_incremental_preserves_unattempted_active_rows_without_hold_downgrade() -> None:
    baseline = pd.DataFrame(
        [
            {
                "job_key": "a",
                "change_hash": "1",
                "first_seen_at": "2026-03-29T00:00:00+09:00",
                "last_seen_at": "2026-03-29T00:00:00+09:00",
                "is_active": True,
                "missing_count": 0,
                "snapshot_date": "2026-03-29",
                "run_id": "r1",
                "source_url": "https://a.example/jobs",
                "source_bucket": "approved",
                "source_name": "테스트",
                "company_name": "테스트",
                "company_tier": "지역기업",
                "job_title_raw": "인공지능 엔지니어",
                "experience_level_raw": "경력",
                "job_role": "인공지능 엔지니어",
                "job_url": "",
                "record_status": "검증실패보류",
                "회사명_표시": "테스트",
                "소스명_표시": "테스트",
                "공고제목_표시": "인공지능 엔지니어",
                "경력수준_표시": "경력",
                "직무명_표시": "인공지능 엔지니어",
                "주요업무_표시": "개발",
                "자격요건_표시": "파이썬",
                "우대사항_표시": "",
                "핵심기술_표시": "파이썬",
            }
        ]
    )

    merged = merge_incremental(
        baseline,
        pd.DataFrame(columns=baseline.columns),
        {},
        "r2",
        "2026-03-30",
        "2026-03-30T00:00:00+09:00",
    )

    assert merged.iloc[0]["record_status"] == "유지"
    assert int(merged.iloc[0]["missing_count"]) == 0
    assert bool(merged.iloc[0]["is_active"]) is True


def test_quality_gate_for_staging_and_master() -> None:
    staging = pd.DataFrame(
        [
            {
                "job_key": "1",
                "change_hash": "a",
                "first_seen_at": "2026-03-29T00:00:00+09:00",
                "last_seen_at": "2026-03-29T00:00:00+09:00",
                "is_active": True,
                "missing_count": 0,
                "snapshot_date": "2026-03-29",
                "run_id": "r1",
                "source_url": "https://example.com/1",
                "source_bucket": "approved",
                "source_name": "소스1",
                "company_name": "회사1",
                "company_tier": "대기업",
                "job_title_raw": "데이터 분석가",
                "experience_level_raw": "경력",
                "job_role": "데이터 분석가",
                "job_url": "",
                "record_status": "신규",
                "회사명_표시": "회사1",
                "소스명_표시": "소스1",
                "공고제목_표시": "데이터 분석가",
                "경력수준_표시": "경력",
                "직무명_표시": "데이터 분석가",
                "주요업무_표시": "분석",
                "자격요건_표시": "SQL",
                "우대사항_표시": "",
                "핵심기술_표시": "SQL",
            }
        ]
    )
    source_registry = pd.DataFrame(
        [
            {
                "source_url": "https://example.com/1",
                "source_bucket": "approved",
                "verification_status": "성공",
            }
        ]
    )
    result = evaluate_quality_gate(staging, source_registry)
    assert result.passed is False
    assert any("허용 직무 4개 중 2개 이상이 0건입니다." == reason for reason in result.reasons)


def test_promote_staging_blocks_suspicious_partial_scan_shrink(
    sandbox_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)

    def _job_row(index: int, *, status: str = "유지") -> dict[str, object]:
        role = "인공지능 엔지니어" if index % 2 == 0 else "데이터 분석가"
        return {
            "job_key": f"job-{index}",
            "change_hash": f"hash-{index}",
            "first_seen_at": "2026-04-01T00:00:00+09:00",
            "last_seen_at": "2026-04-01T00:00:00+09:00",
            "is_active": True,
            "missing_count": 0,
            "snapshot_date": "2026-04-07",
            "run_id": "r1",
            "source_url": f"https://example.com/source/{index}",
            "source_bucket": "approved",
            "source_name": f"소스{index}",
            "company_name": f"회사{index}",
            "company_tier": "중견/중소",
            "job_title_raw": f"공고 {index}",
            "experience_level_raw": "경력",
            "job_role": role,
            "job_url": f"https://example.com/jobs/{index}",
            "record_status": status,
            "주요업무_분석용": "모델을 개발합니다.",
            "자격요건_분석용": "파이썬 경험",
            "우대사항_분석용": "서비스 경험",
            "핵심기술_분석용": "파이썬",
            "상세본문_분석용": "모델을 개발합니다.\n파이썬 경험\n서비스 경험",
            "회사명_표시": f"회사{index}",
            "소스명_표시": f"소스{index}",
            "공고제목_표시": f"공고 {index}",
            "경력수준_표시": "경력",
            "경력근거_표시": "구조화 메타데이터",
            "채용트랙_표시": "일반채용",
            "채용트랙근거_표시": "기본추론",
            "직무초점_표시": "서비스개발",
            "직무초점근거_표시": "공고제목",
            "구분요약_표시": "경력",
            "직무명_표시": role,
            "주요업무_표시": "모델을 개발합니다.",
            "자격요건_표시": "파이썬 경험",
            "우대사항_표시": "서비스 경험",
            "핵심기술_표시": "파이썬",
        }

    master = pd.DataFrame([_job_row(index) for index in range(10)], columns=list(JOB_COLUMNS))
    staging = pd.DataFrame(
        [_job_row(index, status="미발견" if index < 6 else "변경") for index in range(7)],
        columns=list(JOB_COLUMNS),
    )
    registry = pd.DataFrame(
        [
            {
                "source_url": f"https://example.com/source/{index}",
                "source_bucket": "approved",
                "verification_status": "성공",
            }
            for index in range(10)
        ],
        columns=list(SOURCE_REGISTRY_COLUMNS),
    )
    runs = pd.DataFrame(
        [
            {
                "run_id": "update-incremental-1",
                "command": "update-incremental",
                "status": "성공",
                "started_at": "2026-04-07T10:00:00+09:00",
                "finished_at": "2026-04-07T10:01:00+09:00",
                "summary_json": json.dumps(
                    {
                        "staging_job_count": 7,
                        "completed_full_source_scan": False,
                        "new_job_count": 0,
                        "changed_job_count": 1,
                        "missing_job_count": 6,
                        "held_job_count": 0,
                    },
                    ensure_ascii=False,
                ),
            }
        ]
    )
    write_csv(master, paths.master_jobs_path)
    write_csv(staging, paths.staging_jobs_path)
    write_csv(registry, paths.source_registry_path)
    write_csv(runs, paths.runs_path)

    monkeypatch.setattr(pipelines_module, "_PROMOTION_SHRINK_MIN_PREVIOUS_COUNT", 5)
    monkeypatch.setattr(pipelines_module, "_PROMOTION_SHRINK_MIN_DROP_COUNT", 2)
    monkeypatch.setattr(pipelines_module, "_PROMOTION_SHRINK_MIN_DROP_RATIO", 0.1)
    monkeypatch.setattr(pipelines_module, "_PROMOTION_SHRINK_MIN_MISSING_COUNT", 3)
    monkeypatch.setattr(
        pipelines_module,
        "filter_low_quality_jobs",
        lambda staging_jobs, settings=None, paths=None: (staging_jobs.copy(), staging_jobs.iloc[0:0].copy()),
    )
    monkeypatch.setattr(
        pipelines_module,
        "evaluate_quality_gate",
        lambda staging_jobs, registry_frame, settings=None, paths=None, already_filtered=False: GateResult(
            passed=True,
            reasons=[],
            metrics={},
        ),
    )

    summary = promote_staging_pipeline(project_root=sandbox_project)

    assert summary["quality_gate_passed"] is False
    assert summary["promoted_job_count"] == 0
    assert summary["publish_shrink_guard_triggered"] is True
    assert "비정상 감소가 감지되어 master 승격을 차단합니다." in summary["quality_gate_reasons"]
    refreshed_master = read_csv_or_empty(paths.master_jobs_path, JOB_COLUMNS)
    assert len(refreshed_master) == 10


def test_promote_staging_allows_legitimate_shrink_after_full_scan(
    sandbox_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)

    def _job_row(index: int, *, status: str = "유지") -> dict[str, object]:
        role = "인공지능 엔지니어" if index % 2 == 0 else "데이터 분석가"
        return {
            "job_key": f"job-{index}",
            "change_hash": f"hash-{index}",
            "first_seen_at": "2026-04-01T00:00:00+09:00",
            "last_seen_at": "2026-04-01T00:00:00+09:00",
            "is_active": True,
            "missing_count": 0,
            "snapshot_date": "2026-04-07",
            "run_id": "r1",
            "source_url": f"https://example.com/source/{index}",
            "source_bucket": "approved",
            "source_name": f"소스{index}",
            "company_name": f"회사{index}",
            "company_tier": "중견/중소",
            "job_title_raw": f"공고 {index}",
            "experience_level_raw": "경력",
            "job_role": role,
            "job_url": f"https://example.com/jobs/{index}",
            "record_status": status,
            "주요업무_분석용": "모델을 개발합니다.",
            "자격요건_분석용": "파이썬 경험",
            "우대사항_분석용": "서비스 경험",
            "핵심기술_분석용": "파이썬",
            "상세본문_분석용": "모델을 개발합니다.\n파이썬 경험\n서비스 경험",
            "회사명_표시": f"회사{index}",
            "소스명_표시": f"소스{index}",
            "공고제목_표시": f"공고 {index}",
            "경력수준_표시": "경력",
            "경력근거_표시": "구조화 메타데이터",
            "채용트랙_표시": "일반채용",
            "채용트랙근거_표시": "기본추론",
            "직무초점_표시": "서비스개발",
            "직무초점근거_표시": "공고제목",
            "구분요약_표시": "경력",
            "직무명_표시": role,
            "주요업무_표시": "모델을 개발합니다.",
            "자격요건_표시": "파이썬 경험",
            "우대사항_표시": "서비스 경험",
            "핵심기술_표시": "파이썬",
        }

    master = pd.DataFrame([_job_row(index) for index in range(10)], columns=list(JOB_COLUMNS))
    staging = pd.DataFrame(
        [_job_row(index, status="미발견" if index < 3 else "변경") for index in range(7)],
        columns=list(JOB_COLUMNS),
    )
    registry = pd.DataFrame(
        [
            {
                "source_url": f"https://example.com/source/{index}",
                "source_bucket": "approved",
                "verification_status": "성공",
            }
            for index in range(10)
        ],
        columns=list(SOURCE_REGISTRY_COLUMNS),
    )
    runs = pd.DataFrame(
        [
            {
                "run_id": "update-incremental-1",
                "command": "update-incremental",
                "status": "성공",
                "started_at": "2026-04-07T10:00:00+09:00",
                "finished_at": "2026-04-07T10:01:00+09:00",
                "summary_json": json.dumps(
                    {
                        "staging_job_count": 7,
                        "completed_full_source_scan": True,
                        "new_job_count": 1,
                        "changed_job_count": 3,
                        "missing_job_count": 3,
                        "held_job_count": 0,
                    },
                    ensure_ascii=False,
                ),
            }
        ]
    )
    write_csv(master, paths.master_jobs_path)
    write_csv(staging, paths.staging_jobs_path)
    write_csv(registry, paths.source_registry_path)
    write_csv(runs, paths.runs_path)

    monkeypatch.setattr(pipelines_module, "_PROMOTION_SHRINK_MIN_PREVIOUS_COUNT", 5)
    monkeypatch.setattr(pipelines_module, "_PROMOTION_SHRINK_MIN_DROP_COUNT", 2)
    monkeypatch.setattr(pipelines_module, "_PROMOTION_SHRINK_MIN_DROP_RATIO", 0.1)
    monkeypatch.setattr(pipelines_module, "_PROMOTION_SHRINK_MIN_MISSING_COUNT", 3)
    monkeypatch.setattr(
        pipelines_module,
        "filter_low_quality_jobs",
        lambda staging_jobs, settings=None, paths=None: (staging_jobs.copy(), staging_jobs.iloc[0:0].copy()),
    )
    monkeypatch.setattr(
        pipelines_module,
        "evaluate_quality_gate",
        lambda staging_jobs, registry_frame, settings=None, paths=None, already_filtered=False: GateResult(
            passed=True,
            reasons=[],
            metrics={},
        ),
    )

    summary = promote_staging_pipeline(project_root=sandbox_project)

    assert summary["quality_gate_passed"] is True
    assert summary["promoted_job_count"] == 7
    assert summary["publish_shrink_guard_triggered"] is False
    refreshed_master = read_csv_or_empty(paths.master_jobs_path, JOB_COLUMNS)
    assert len(refreshed_master) == 7


def test_filter_low_quality_jobs_drops_empty_or_noisy_analysis_rows() -> None:
    staging = pd.DataFrame(
        [
            {
                "is_active": True,
                "company_name": "회사1",
                "company_tier": "대기업",
                "job_role": "데이터 분석가",
                "job_title_raw": "공고1",
                "record_status": "유지",
                "주요업무_분석용": "분석합니다.\n모델을 운영합니다.",
                "자격요건_분석용": "SQL 경험이 필요합니다.\n파이썬 활용이 필요합니다.",
                "핵심기술_분석용": "파이썬\n에스큐엘",
                "상세본문_분석용": "",
            },
            {
                "is_active": True,
                "company_name": "회사2",
                "company_tier": "중견/중소",
                "job_role": "인공지능 엔지니어",
                "job_title_raw": "공고2",
                "record_status": "유지",
                "주요업무_분석용": "",
                "자격요건_분석용": "필요 역량",
                "핵심기술_분석용": "파이썬\n에스큐엘",
                "상세본문_분석용": "[비에이에스아이씨 ]\n[피알이에프이알알이디 ]",
            },
            {
                "is_active": True,
                "company_name": "회사3",
                "company_tier": "스타트업",
                "job_role": "인공지능 리서처",
                "job_title_raw": "공고3",
                "record_status": "검증실패보류",
                "주요업무_분석용": "실험을 수행합니다.",
                "자격요건_분석용": "논문 구현 경험",
                "핵심기술_분석용": "파이토치\n엘엘엠",
                "상세본문_분석용": "실험을 수행하고 모델을 개선합니다.\n논문을 구현합니다.",
            },
            {
                "is_active": True,
                "company_name": "회사4",
                "company_tier": "대기업",
                "job_role": "데이터 분석가",
                "job_title_raw": "공고4",
                "record_status": "유지",
                "주요업무_분석용": "에이비테스트설계및검증을통한제품개선제품혹은기능개선을위해명확한가설을수립하고데이터기반에이비테스트를설계합니다",
                "자격요건_분석용": "에스큐엘 경험",
                "핵심기술_분석용": "파이썬\n에스큐엘",
                "상세본문_분석용": "에이비테스트설계및검증을통한제품개선제품혹은기능개선을위해명확한가설을수립하고데이터기반에이비테스트를설계합니다\n에스큐엘 경험",
            },
        ]
    )
    filtered, dropped = filter_low_quality_jobs(staging)
    assert len(filtered) == 2
    assert len(dropped) == 2
    assert set(filtered["company_name"]) == {"회사1", "회사3"}


def test_filter_low_quality_jobs_collapses_practical_duplicates_with_identical_content() -> None:
    staging = pd.DataFrame(
        [
            {
                "is_active": True,
                "company_name": "한국뇌연구원 AI 실증지원사업단",
                "company_tier": "공공·연구기관",
                "job_role": "인공지능 리서처",
                "job_title_raw": "[AI실증지원사업단] [A-20](연구직) 2025년 제4차 사업단 직원(계약직) 채용",
                "job_title_ko": "",
                "job_url": "https://kbri-ai.recruiter.co.kr/app/jobnotice/view?systemKindCode=MRS2&jobnoticeSn=229218",
                "회사명_표시": "한국뇌연구원 AI 실증지원사업단",
                "소스명_표시": "KBRI",
                "공고제목_표시": "[AI실증지원사업단] [A-20](연구직) 2025년 제4차 사업단 직원(계약직) 채용",
                "구분요약_표시": "계약직 / 학사 / 비전 / 시계열",
                "주요업무_분석용": "인공지능 기반 뇌 발달 질환 디지털 치료기기 실증 지원 연구를 수행합니다.",
                "자격요건_분석용": "학사 이상의 학위를 소지해야 합니다.",
                "우대사항_분석용": "",
                "핵심기술_분석용": "비전\n시계열",
                "상세본문_분석용": "인공지능 기반 뇌 발달 질환 디지털 치료기기 실증 지원 연구를 수행합니다.\n학사 이상의 학위를 소지해야 합니다.",
            },
            {
                "is_active": True,
                "company_name": "한국뇌연구원 AI 실증지원사업단",
                "company_tier": "공공·연구기관",
                "job_role": "인공지능 리서처",
                "job_title_raw": "[AI실증지원사업단] [A-21](연구직) 2025년 제4차 사업단 직원(계약직) 채용",
                "job_title_ko": "",
                "job_url": "https://kbri-ai.recruiter.co.kr/app/jobnotice/view?systemKindCode=MRS2&jobnoticeSn=229578",
                "회사명_표시": "한국뇌연구원 AI 실증지원사업단",
                "소스명_표시": "KBRI",
                "공고제목_표시": "[AI실증지원사업단] [A-21](연구직) 2025년 제4차 사업단 직원(계약직) 채용",
                "구분요약_표시": "계약직 / 학사 / 비전 / 시계열",
                "주요업무_분석용": "인공지능 기반 뇌 발달 질환 디지털 치료기기 실증 지원 연구를 수행합니다.",
                "자격요건_분석용": "학사 이상의 학위를 소지해야 합니다.",
                "우대사항_분석용": "",
                "핵심기술_분석용": "비전\n시계열",
                "상세본문_분석용": "인공지능 기반 뇌 발달 질환 디지털 치료기기 실증 지원 연구를 수행합니다.\n학사 이상의 학위를 소지해야 합니다.",
            },
        ]
    )

    filtered, dropped = filter_low_quality_jobs(staging)

    assert len(filtered) == 1
    assert len(dropped) == 1
    assert filtered.iloc[0]["job_url"] == "https://kbri-ai.recruiter.co.kr/app/jobnotice/view?systemKindCode=MRS2&jobnoticeSn=229578"


def test_normalize_job_analysis_fields_replaces_noisy_recruiter_hold_detail_with_structured_fallback() -> None:
    staging = pd.DataFrame(
        [
            {
                "is_active": True,
                "record_status": "검증실패보류",
                "company_name": "한국뇌연구원 AI 실증지원사업단",
                "company_tier": "공공·연구기관",
                "job_role": "인공지능 리서처",
                "job_title_raw": "[AI실증지원사업단] [A-21](연구직) 2025년 제4차 사업단 직원(계약직) 채용",
                "job_title_ko": "",
                "job_url": "https://kbri-ai.recruiter.co.kr/app/jobnotice/view?systemKindCode=MRS2&jobnoticeSn=229578",
                "회사명_표시": "한국뇌연구원 AI 실증지원사업단",
                "소스명_표시": "KBRI",
                "공고제목_표시": "[AI실증지원사업단] [A-21](연구직) 2025년 제4차 사업단 직원(계약직) 채용",
                "주요업무_분석용": "인공지능 기반 뇌 발달 질환 디지털 치료기기 실증 지원 연구를 수행합니다.\n디지털 치료기기 제품 고도화 및 기술 검증을 지원합니다.",
                "자격요건_분석용": "학사 이상의 학위를 소지해야 합니다.",
                "우대사항_분석용": "의료 데이터 분석 관련 논문 또는 특허 실적 보유자를 우대합니다.",
                "핵심기술_분석용": "데이터 분석\n모델링",
                "상세본문_분석용": "\n".join(
                    [
                        "학위: 학사이상 소지자 • 수행 직무",
                        "1) 기반 뇌발달질환 디지털 치료기기 실증 지원 관련 연구 업무",
                        "2) 디지털 치료기기 제품 고도화 및 기술 검증 지원",
                        "1) 의료 데이터 분석 논문 특허 실적 보유자",
                        "• 지원 시 참고사항",
                        "- 수탁연구사업 종료일: 2026. 12. 31.",
                        "채용인원2명",
                    ]
                ),
            }
        ]
    )

    normalized = normalize_job_analysis_fields(staging)

    detail = normalized.iloc[0]["상세본문_분석용"]
    assert "지원 시 참고사항" not in detail
    assert "채용인원" not in detail
    assert detail == "\n".join(
        [
            "인공지능 기반 뇌 발달 질환 디지털 치료기기 실증 지원 연구를 수행합니다.",
            "디지털 치료기기 제품 고도화 및 기술 검증을 지원합니다.",
            "학사 이상의 학위를 소지해야 합니다.",
            "의료 데이터 분석 관련 논문 또는 특허 실적 보유자를 우대합니다.",
        ]
    )


def test_normalize_job_analysis_fields_does_not_recover_main_tasks_from_admin_notice_lines() -> None:
    staging = pd.DataFrame(
        [
            {
                "company_name": "한국뇌연구원 AI 실증지원사업단",
                "source_name": "KBRI",
                "job_title_raw": "[AI실증지원사업단] [A-21](연구직) 2025년 제4차 사업단 직원(계약직) 채용",
                "job_role": "인공지능 리서처",
                "주요업무_분석용": "",
                "자격요건_분석용": "",
                "우대사항_분석용": "",
                "핵심기술_분석용": "비전\n시계열",
                "상세본문_분석용": "\n".join(
                    [
                        "공고명 [AI실증지원사업단] [A-21](연구직) 2025년 제4차 사업단 직원(계약직) 채용",
                        "공고문 및 양식을 반드시 확인해 주세요.",
                        "연구실적목록 및 연구계획서 제출이 필요합니다.",
                        "양식은 첨부파일을 참조해 주세요.",
                    ]
                ),
                "주요업무_표시": "",
                "자격요건_표시": "",
                "우대사항_표시": "",
                "핵심기술_표시": "",
            }
        ]
    )

    normalized = normalize_job_analysis_fields(staging)

    assert normalized.loc[0, "주요업무_분석용"] == ""
    assert normalized.loc[0, "주요업무_표시"] == ""
    assert normalized.loc[0, "자격요건_분석용"] == ""
    assert normalized.loc[0, "자격요건_표시"] == ""
    assert normalized.loc[0, "상세본문_분석용"] == ""


def test_normalize_job_analysis_fields_stops_preferred_salvage_before_faq_tail() -> None:
    staging = pd.DataFrame(
        [
            {
                "company_name": "크래프톤",
                "source_name": "크래프톤 채용",
                "job_title_raw": "AI FDE 집중채용",
                "job_role": "인공지능 엔지니어",
                "주요업무_분석용": "AI 기반 서비스 성능을 개선합니다.",
                "자격요건_분석용": "파이썬 기반 서비스 개발 경험이 필요합니다.",
                "우대사항_분석용": "",
                "핵심기술_분석용": "파이썬\n머신러닝",
                "상세본문_분석용": "\n".join(
                    [
                        "우대사항",
                        "대규모 실험 설계 및 분석 경험이 있는 분",
                        "추천 시스템 또는 검색 품질 개선 경험이 있는 분",
                        ") 근무 형태는 어떻게 되나요?",
                        "주 5일 오피스 출근을 기본으로 합니다.",
                        ") 근무 시작일은 어떻게 되나요?",
                        "입사일은 개별 협의를 통해 조정됩니다.",
                    ]
                ),
                "주요업무_표시": "",
                "자격요건_표시": "",
                "우대사항_표시": "",
                "핵심기술_표시": "",
            }
        ]
    )

    normalized = normalize_job_analysis_fields(staging)

    preferred_analysis = normalized.loc[0, "우대사항_분석용"]
    preferred_display = normalized.loc[0, "우대사항_표시"]
    assert "대규모 실험 설계 및 분석 경험이 있는 분" in preferred_analysis
    assert "추천 시스템 또는 검색 품질 개선 경험이 있는 분" in preferred_display
    assert "근무 형태" not in preferred_analysis
    assert "근무 시작일" not in preferred_display
    assert "입사일은 개별 협의를 통해 조정됩니다." not in preferred_display


def test_filter_low_quality_jobs_collapses_same_content_duplicates_with_same_variant_key() -> None:
    staging = pd.DataFrame(
        [
            {
                "is_active": True,
                "company_name": "테스트회사",
                "company_tier": "스타트업",
                "job_role": "인공지능 엔지니어",
                "job_title_raw": "Machine Learning Engineer",
                "job_title_ko": "",
                "job_url": "https://example.com/jobs/1",
                "회사명_표시": "테스트회사",
                "소스명_표시": "TEST",
                "공고제목_표시": "Machine Learning Engineer",
                "구분요약_표시": "경력 / LLM",
                "주요업무_분석용": "모델을 개발합니다.",
                "자격요건_분석용": "파이썬 경험이 필요합니다.",
                "우대사항_분석용": "",
                "핵심기술_분석용": "파이썬\n머신러닝",
                "상세본문_분석용": "모델을 개발합니다.\n파이썬 경험이 필요합니다.",
            },
            {
                "is_active": True,
                "company_name": "테스트회사",
                "company_tier": "스타트업",
                "job_role": "인공지능 엔지니어",
                "job_title_raw": "Machine Learning Engineer",
                "job_title_ko": "",
                "job_url": "https://example.com/jobs/2",
                "회사명_표시": "테스트회사",
                "소스명_표시": "TEST",
                "공고제목_표시": "Machine Learning Engineer | LLM / 검색",
                "구분요약_표시": "경력 / LLM / 검색",
                "주요업무_분석용": "모델을 개발합니다.",
                "자격요건_분석용": "파이썬 경험이 필요합니다.",
                "우대사항_분석용": "",
                "핵심기술_분석용": "파이썬\n머신러닝",
                "상세본문_분석용": "모델을 개발합니다.\n파이썬 경험이 필요합니다.",
            },
        ]
    )

    filtered, dropped = filter_low_quality_jobs(staging)

    assert len(filtered) == 1
    assert len(dropped) == 1
    assert filtered.iloc[0]["job_url"] == "https://example.com/jobs/2"


def test_filter_low_quality_jobs_collapses_same_content_duplicates_with_different_locations() -> None:
    staging = pd.DataFrame(
        [
            {
                "is_active": True,
                "company_name": "인터엑스",
                "company_tier": "중견/중소",
                "job_role": "인공지능 엔지니어",
                "job_title_raw": "[서울] AI 비전·데이터 분석 엔지니어",
                "job_url": "https://example.com/seoul",
                "회사명_표시": "인터엑스",
                "공고제목_표시": "[서울] AI 비전·데이터 분석 엔지니어",
                "구분요약_표시": "경력 / 비전 / 최적화",
                "주요업무_분석용": "비전 모델을 개발합니다.",
                "자격요건_분석용": "파이썬 경험이 필요합니다.",
                "우대사항_분석용": "",
                "핵심기술_분석용": "파이썬\n비전",
                "상세본문_분석용": "비전 모델을 개발합니다.\n파이썬 경험이 필요합니다.",
            },
            {
                "is_active": True,
                "company_name": "인터엑스",
                "company_tier": "중견/중소",
                "job_role": "인공지능 엔지니어",
                "job_title_raw": "[울산] AI 비전·데이터 분석 엔지니어",
                "job_url": "https://example.com/ulsan",
                "회사명_표시": "인터엑스",
                "공고제목_표시": "[울산] AI 비전·데이터 분석 엔지니어",
                "구분요약_표시": "경력 / 비전 / 최적화",
                "주요업무_분석용": "비전 모델을 개발합니다.",
                "자격요건_분석용": "파이썬 경험이 필요합니다.",
                "우대사항_분석용": "",
                "핵심기술_분석용": "파이썬\n비전",
                "상세본문_분석용": "비전 모델을 개발합니다.\n파이썬 경험이 필요합니다.",
            },
        ]
    )

    filtered, dropped = filter_low_quality_jobs(staging)

    assert len(filtered) == 1
    assert len(dropped) == 1
    assert "서울 / 울산" in filtered.iloc[0]["공고제목_표시"]


def test_filter_low_quality_jobs_collapses_same_company_role_family_even_with_different_levels() -> None:
    staging = pd.DataFrame(
        [
            {
                "is_active": True,
                "company_name": "서울로보틱스",
                "company_tier": "스타트업",
                "job_role": "인공지능 엔지니어",
                "job_title_raw": "Senior Machine Learning Engineer - Computer Vision and Perception",
                "job_url": "https://example.com/senior",
                "회사명_표시": "서울로보틱스",
                "공고제목_표시": "Senior Machine Learning Engineer - Computer Vision and Perception",
                "구분요약_표시": "시니어 / 박사 / 석사 / 학사 / 비전 / 최적화",
                "주요업무_분석용": "비전 모델을 개발합니다.",
                "자격요건_분석용": "박사 또는 석사 이상의 학위가 필요합니다.",
                "우대사항_분석용": "최적화 경험을 우대합니다.",
                "핵심기술_분석용": "비전\n최적화",
                "상세본문_분석용": "비전 모델을 개발합니다.\n박사 또는 석사 이상의 학위가 필요합니다.\n최적화 경험을 우대합니다.",
            },
            {
                "is_active": True,
                "company_name": "서울로보틱스",
                "company_tier": "스타트업",
                "job_role": "인공지능 엔지니어",
                "job_title_raw": "Director - Machine Learning & Computer Vision",
                "job_url": "https://example.com/director",
                "회사명_표시": "서울로보틱스",
                "공고제목_표시": "Director - Machine Learning & Computer Vision",
                "구분요약_표시": "경력 / 박사 / 석사 / 학사 / 비전 / 최적화",
                "주요업무_분석용": "비전 모델을 개발합니다.",
                "자격요건_분석용": "박사 또는 석사 이상의 학위가 필요합니다.",
                "우대사항_분석용": "최적화 경험을 우대합니다.",
                "핵심기술_분석용": "비전\n최적화",
                "상세본문_분석용": "비전 모델을 개발합니다.\n박사 또는 석사 이상의 학위가 필요합니다.\n최적화 경험을 우대합니다.",
            },
        ]
    )

    filtered, dropped = filter_low_quality_jobs(staging)

    assert len(filtered) == 1
    assert len(dropped) == 1


def test_filter_low_quality_jobs_collapses_same_content_duplicates_with_different_tracks() -> None:
    staging = pd.DataFrame(
        [
            {
                "is_active": True,
                "company_name": "마키나락스",
                "company_tier": "스타트업",
                "job_role": "인공지능 리서처",
                "job_title_raw": "AI Research Engineer (Junior)",
                "job_url": "https://example.com/junior",
                "회사명_표시": "마키나락스",
                "공고제목_표시": "AI Research Engineer (Junior)",
                "구분요약_표시": "주니어 / 석사",
                "경력수준_표시": "주니어",
                "채용트랙_표시": "석사",
                "주요업무_분석용": "연구를 수행합니다.",
                "자격요건_분석용": "석사 이상 학위가 필요합니다.",
                "우대사항_분석용": "",
                "핵심기술_분석용": "파이썬\n머신러닝",
                "상세본문_분석용": "연구를 수행합니다.\n석사 이상 학위가 필요합니다.",
            },
            {
                "is_active": True,
                "company_name": "마키나락스",
                "company_tier": "스타트업",
                "job_role": "인공지능 리서처",
                "job_title_raw": "[전문연구요원] AI Research Engineer",
                "job_url": "https://example.com/military",
                "회사명_표시": "마키나락스",
                "공고제목_표시": "[전문연구요원] AI Research Engineer",
                "구분요약_표시": "전문연구요원 / 석사",
                "경력수준_표시": "",
                "채용트랙_표시": "전문연구요원 / 석사",
                "주요업무_분석용": "연구를 수행합니다.",
                "자격요건_분석용": "석사 이상 학위가 필요합니다.",
                "우대사항_분석용": "",
                "핵심기술_분석용": "파이썬\n머신러닝",
                "상세본문_분석용": "연구를 수행합니다.\n석사 이상 학위가 필요합니다.",
            },
        ]
    )

    filtered, dropped = filter_low_quality_jobs(staging)

    assert len(filtered) == 1
    assert len(dropped) == 1
    assert filtered.iloc[0]["job_url"] == "https://example.com/junior"


def test_filter_low_quality_jobs_collapses_near_duplicates_with_high_body_similarity() -> None:
    staging = pd.DataFrame(
        [
            {
                "is_active": True,
                "company_name": "클로버추얼패션",
                "company_tier": "스타트업",
                "job_role": "인공지능 엔지니어",
                "job_title_raw": "Machine Learning Engineer, R&D",
                "job_url": "https://example.com/jobs/rd",
                "회사명_표시": "클로버추얼패션",
                "공고제목_표시": "Machine Learning Engineer, R&D | 경력 2년+ / 박사 / LLM / 비전",
                "구분요약_표시": "경력 2년+ / 박사 / LLM / 비전",
                "주요업무_분석용": "가상 피팅 및 패션 생성 모델을 연구하고 개발합니다.\n멀티모달 및 생성형 인공지능 모델을 고도화합니다.",
                "자격요건_분석용": "파이썬과 딥러닝 프레임워크 활용 경험이 필요합니다.\n컴퓨터 비전 또는 생성 모델 경험이 필요합니다.",
                "우대사항_분석용": "석사 또는 박사 학위를 우대합니다.\n대규모 학습 파이프라인 구축 경험을 우대합니다.",
                "핵심기술_분석용": "파이썬\n딥러닝\n비전\n엘엘엠",
                "상세본문_분석용": "가상 피팅 및 패션 생성 모델을 연구하고 개발합니다.\n멀티모달 및 생성형 인공지능 모델을 고도화합니다.\n파이썬과 딥러닝 프레임워크 활용 경험이 필요합니다.\n컴퓨터 비전 또는 생성 모델 경험이 필요합니다.\n석사 또는 박사 학위를 우대합니다.",
            },
            {
                "is_active": True,
                "company_name": "클로버추얼패션",
                "company_tier": "스타트업",
                "job_role": "인공지능 엔지니어",
                "job_title_raw": "Machine Learning Engineer (전문연구요원, alternative military service), R&D",
                "job_url": "https://example.com/jobs/alt",
                "회사명_표시": "클로버추얼패션",
                "공고제목_표시": "Machine Learning Engineer (전문연구요원, alternative military service), R&D",
                "구분요약_표시": "전문연구요원 / 박사 / LLM / 비전",
                "주요업무_분석용": "가상 피팅과 패션 생성 모델 연구 및 개발을 수행합니다.\n멀티모달 생성형 인공지능 모델을 개선합니다.",
                "자격요건_분석용": "파이썬 및 딥러닝 프레임워크 활용 경험이 필요합니다.\n컴퓨터 비전 혹은 생성 모델 경험이 필요합니다.",
                "우대사항_분석용": "석사/박사 학위를 우대합니다.\n대규모 학습 파이프라인 구축 경험을 우대합니다.",
                "핵심기술_분석용": "파이썬\n딥러닝\n비전\n엘엘엠",
                "상세본문_분석용": "가상 피팅과 패션 생성 모델 연구 및 개발을 수행합니다.\n멀티모달 생성형 인공지능 모델을 개선합니다.\n파이썬 및 딥러닝 프레임워크 활용 경험이 필요합니다.\n컴퓨터 비전 혹은 생성 모델 경험이 필요합니다.\n석사/박사 학위를 우대합니다.",
            },
        ]
    )

    filtered, dropped = filter_low_quality_jobs(staging)

    assert len(filtered) == 1
    assert len(dropped) == 1


def test_filter_low_quality_jobs_collapses_same_title_family_with_high_visible_detail_similarity() -> None:
    staging = pd.DataFrame(
        [
            {
                "is_active": True,
                "company_name": "하이퍼커넥트",
                "company_tier": "유니콘",
                "job_role": "인공지능 엔지니어",
                "job_title_raw": "Senior Machine Learning Engineer (Match Group AI)",
                "job_url": "https://example.com/hyperconnect/senior",
                "회사명_표시": "하이퍼커넥트",
                "공고제목_표시": "Senior Machine Learning Engineer (Match Group AI)",
                "구분요약_표시": "시니어 / 박사 / 비전 / 제품분석",
                "경력수준_표시": "시니어",
                "채용트랙_표시": "박사",
                "주요업무_분석용": "제품 문제를 머신러닝 문제로 재정의합니다.\n모델과 시스템 설계를 주도합니다.",
                "자격요건_분석용": "박사 또는 이에 준하는 연구 경험이 필요합니다.\n기술 의사결정을 이끌 수 있어야 합니다.",
                "우대사항_분석용": "리드 경험을 우대합니다.",
                "핵심기술_분석용": "머신러닝\n비전\n최적화",
                "상세본문_분석용": "인공지능의 머신러닝 엔지니어는 최신 인공지능 및 머신러닝 기술을 연구하고 적용하는 과학자이자 실제 서비스 환경에 맞게 모델 및 시스템을 설계하고 최적화하는 엔지니어입니다. 서비스 중인 제품의 비즈니스 문제를 머신러닝 문제로 재정의하고 최선의 방법론을 찾아 해결합니다. 새로운 제품 및 기능 개발에 참여하고 아이디어 구상부터 프로토타이핑, 실제 사용자 도달까지 인공지능 기술을 활용합니다. 대규모 언어 모델 및 멀티모달 모델을 연구하고 제품의 다양한 영역에 적용합니다. 제품 목표와 정렬된 평가 지표 정의, 데이터 편향 및 노이즈 해소, 대규모 추론 최적화까지 다룹니다.",
            },
            {
                "is_active": True,
                "company_name": "하이퍼커넥트",
                "company_tier": "유니콘",
                "job_role": "인공지능 엔지니어",
                "job_title_raw": "Machine Learning Engineer (Match Group AI | 전문연구요원 편입/전직 가능)",
                "job_url": "https://example.com/hyperconnect/military",
                "회사명_표시": "하이퍼커넥트",
                "공고제목_표시": "Machine Learning Engineer (Match Group AI | 전문연구요원 편입/전직 가능)",
                "구분요약_표시": "전문연구요원 / 비전 / 최적화",
                "경력수준_표시": "",
                "채용트랙_표시": "전문연구요원",
                "주요업무_분석용": "문제를 머신러닝 문제로 재정의합니다.\n제품 기능 개발에 참여합니다.",
                "자격요건_분석용": "최신 기술을 빠르게 학습할 수 있어야 합니다.\n머신러닝 경험이 필요합니다.",
                "우대사항_분석용": "연구 경험을 우대합니다.",
                "핵심기술_분석용": "머신러닝\n비전\n최적화",
                "상세본문_분석용": "인공지능의 머신러닝 엔지니어는 최신 인공지능 및 머신러닝 기술을 연구하고 적용하는 과학자이자 실제 서비스 환경에 맞게 모델 및 시스템을 설계하고 최적화하는 엔지니어입니다. 서비스 중인 제품의 비즈니스 문제를 머신러닝 문제로 재정의하고 최선의 방법론을 찾아 해결합니다. 새로운 제품 및 기능 개발에 참여하고 아이디어 구상부터 프로토타이핑, 실제 사용자 도달까지 인공지능 기술을 활용합니다. 대규모 언어 모델 및 멀티모달 모델을 연구하고 제품의 다양한 영역에 적용합니다. 제품 목표와 정렬된 평가 지표 정의, 데이터 편향 및 노이즈 해소, 대규모 추론 최적화까지 다룹니다.",
            },
        ]
    )

    filtered, dropped = filter_low_quality_jobs(staging)

    assert len(filtered) == 1
    assert len(dropped) == 1


def test_filter_low_quality_jobs_collapses_high_visible_detail_similarity_with_near_identical_titles() -> None:
    staging = pd.DataFrame(
        [
            {
                "is_active": True,
                "company_name": "크래프톤",
                "company_tier": "대기업",
                "job_role": "인공지능 리서처",
                "job_title_raw": "[AI Research Div.] [전문연구요원] Research Scientist - Foundation Model (2년 이상)",
                "job_url": "https://example.com/krafton/military",
                "회사명_표시": "크래프톤",
                "공고제목_표시": "[AI Research Div.] [전문연구요원] Research Scientist - Foundation Model (2년 이상)",
                "구분요약_표시": "경력 / 전문연구요원 / 박사 / 비전",
                "경력수준_표시": "경력",
                "채용트랙_표시": "전문연구요원 / 박사",
                "주요업무_분석용": "대규모 멀티모달 파운데이션 모델 연구를 수행합니다.\n모델 성능 향상을 위한 평가 파이프라인을 고도화합니다.",
                "자격요건_분석용": "석사 또는 박사 수준의 연구 경력이 필요합니다.\n최상위 학회 또는 저널 논문 작성 경험이 필요합니다.",
                "우대사항_분석용": "모델 배포 경험을 우대합니다.\n실험 설계 역량을 우대합니다.",
                "핵심기술_분석용": "엘엘엠\n비전",
                "상세본문_분석용": "대규모 멀티모달 엘엘엠 학습 및 선행 연구를 진행합니다. 모델 성능 향상을 위한 모델링, 데이터, 평가 파이프라인을 고도화합니다. 연구 성과를 최상위 학회에 논문 또는 워크숍 형태로 발표합니다. 딥러닝 관련 분야 석사 또는 박사 학위 소지자 또는 그에 준하는 연구 경력이 필요합니다. 인공지능 및 머신러닝 최상위 학회 또는 저널 논문 작성 경험이 있어야 합니다. 연구를 주도하고 딥러닝 논문을 빠르게 이해하며 실험을 설계할 수 있어야 합니다.",
            },
            {
                "is_active": True,
                "company_name": "크래프톤",
                "company_tier": "대기업",
                "job_role": "인공지능 리서처",
                "job_title_raw": "[AI Research Div.] Research Scientist - Foundation Models (2년 이상 / 계약직)",
                "job_url": "https://example.com/krafton/contract",
                "회사명_표시": "크래프톤",
                "공고제목_표시": "[AI Research Div.] Research Scientist - Foundation Models (2년 이상 / 계약직)",
                "구분요약_표시": "경력 / 계약직 / 박사 / 석사 / 비전",
                "경력수준_표시": "경력",
                "채용트랙_표시": "계약직 / 박사 / 석사",
                "주요업무_분석용": "대규모 멀티모달 파운데이션 모델 연구를 진행합니다.\n모델 성능 향상을 위한 평가 파이프라인을 고도화합니다.",
                "자격요건_분석용": "석사 또는 박사 수준의 연구 경력이 필요합니다.\n최상위 학회 또는 저널 논문 작성 경험이 필요합니다.",
                "우대사항_분석용": "모델 배포 경험을 우대합니다.\n실험 설계 역량을 우대합니다.",
                "핵심기술_분석용": "엘엘엠\n비전",
                "상세본문_분석용": "대규모 멀티모달 엘엘엠 학습 및 선행 연구를 진행합니다. 모델 성능 향상을 위한 모델링, 데이터, 평가 파이프라인을 고도화합니다. 연구 성과를 최상위 학회에 논문 또는 워크숍 형태로 발표합니다. 딥러닝 관련 분야 석사 또는 박사 학위 소지자 또는 그에 준하는 연구 경력이 필요합니다. 인공지능 및 머신러닝 최상위 학회 또는 저널 논문 작성 경험이 있어야 합니다. 연구를 주도하고 딥러닝 논문을 빠르게 이해하며 실험을 설계할 수 있어야 합니다.",
            },
        ]
    )

    filtered, dropped = filter_low_quality_jobs(staging)

    assert len(filtered) == 1
    assert len(dropped) == 1


def test_filter_low_quality_jobs_collapses_same_company_job_family_to_one_representative() -> None:
    staging = pd.DataFrame(
        [
            {
                "is_active": True,
                "company_name": "몰로코",
                "company_tier": "외국계 한국법인",
                "job_role": "데이터 사이언티스트",
                "job_title_raw": "Senior Applied Scientist",
                "job_url": "https://example.com/jobs/senior",
                "회사명_표시": "몰로코",
                "공고제목_표시": "Senior Applied Scientist (시니어 응용 과학자)",
                "구분요약_표시": "시니어 / 광고 / 모델링",
                "주요업무_분석용": "광고 최적화 모델을 연구하고 실험합니다.\n온라인 실험 결과를 해석합니다.",
                "자격요건_분석용": "확률 모델링과 인과 추론 경험이 필요합니다.",
                "우대사항_분석용": "광고 도메인 경험을 우대합니다.",
                "핵심기술_분석용": "파이썬\n인과 추론",
                "상세본문_분석용": "광고 최적화 모델을 연구하고 실험합니다.\n온라인 실험 결과를 해석합니다.\n확률 모델링과 인과 추론 경험이 필요합니다.",
            },
            {
                "is_active": True,
                "company_name": "몰로코",
                "company_tier": "외국계 한국법인",
                "job_role": "데이터 사이언티스트",
                "job_title_raw": "Applied Scientist II",
                "job_url": "https://example.com/jobs/as2",
                "회사명_표시": "몰로코",
                "공고제목_표시": "Applied Scientist II (응용 과학자 II)",
                "구분요약_표시": "추천 / LLM / 모델링",
                "주요업무_분석용": "추천 및 생성형 인공지능 모델을 개발합니다.\n오프라인 평가와 배포를 수행합니다.",
                "자격요건_분석용": "추천 시스템 또는 생성형 인공지능 경험이 필요합니다.",
                "우대사항_분석용": "대규모 배포 경험을 우대합니다.",
                "핵심기술_분석용": "파이썬\n추천 시스템\n엘엘엠",
                "상세본문_분석용": "추천 및 생성형 인공지능 모델을 개발합니다.\n오프라인 평가와 배포를 수행합니다.\n추천 시스템 또는 생성형 인공지능 경험이 필요합니다.",
            },
        ]
    )

    filtered, dropped = filter_low_quality_jobs(staging)

    assert len(filtered) == 1
    assert len(dropped) == 1
    assert filtered.iloc[0]["job_url"] in {
        "https://example.com/jobs/senior",
        "https://example.com/jobs/as2",
    }


def test_filter_low_quality_jobs_uses_gemini_duplicate_adjudication_for_borderline_pairs(
    sandbox_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    staging = pd.DataFrame(
        [
            {
                "is_active": True,
                "company_name": "쿠팡",
                "company_tier": "대기업",
                "job_role": "데이터 분석가",
                "job_title_raw": "Staff Data Analyst (Fraud & Risk)",
                "job_url": "https://www.coupang.jobs/en/jobs/?gh_jid=7064205",
                "회사명_표시": "쿠팡",
                "공고제목_표시": "Staff Data Analyst (Fraud & Risk) [공고 7064205]",
                "구분요약_표시": "스태프 / 리스크 / 분석",
                "주요업무_분석용": "사기 탐지와 리스크 데이터 분석을 수행합니다.\n운영 지표를 개선합니다.",
                "자격요건_분석용": "에스큐엘과 통계 분석 경험이 필요합니다.",
                "우대사항_분석용": "리스크 분석 경험을 우대합니다.",
                "핵심기술_분석용": "에스큐엘\n통계 분석",
                "상세본문_분석용": "사기 탐지와 리스크 데이터 분석을 수행합니다.\n운영 지표를 개선합니다.\n에스큐엘과 통계 분석 경험이 필요합니다.",
            },
            {
                "is_active": True,
                "company_name": "쿠팡",
                "company_tier": "대기업",
                "job_role": "데이터 분석가",
                "job_title_raw": "Staff Data Analyst (Fraud & Risk)",
                "job_url": "https://www.coupang.jobs/en/jobs/?gh_jid=7064215",
                "회사명_표시": "쿠팡",
                "공고제목_표시": "Staff Data Analyst (Fraud & Risk) [공고 7064215]",
                "구분요약_표시": "스태프 / 리스크 / 운영분석",
                "주요업무_분석용": "사기 탐지와 리스크 데이터 분석을 수행합니다.\n운영 지표를 개선합니다.",
                "자격요건_분석용": "에스큐엘과 통계 분석 경험이 필요합니다.",
                "우대사항_분석용": "리스크 분석 경험을 우대합니다.",
                "핵심기술_분석용": "에스큐엘\n통계 분석",
                "상세본문_분석용": "사기 탐지와 리스크 데이터 분석을 수행합니다.\n운영 지표를 개선합니다.\n에스큐엘과 통계 분석 경험이 필요합니다.",
            },
        ]
    )
    settings = AppSettings(
        enable_gemini_fallback=True,
        enable_gemini_duplicate_adjudication=True,
        gemini_api_key="test-key",
        gemini_model="gemini-2.5-flash",
        gemini_duplicate_max_calls_per_run=2,
    )
    paths = ProjectPaths.from_root(sandbox_project)

    monkeypatch.setattr(quality_module, "_rows_are_near_practical_duplicates", lambda left, right: False)
    monkeypatch.setattr(
        quality_module,
        "maybe_adjudicate_duplicate_pair",
        lambda left, right, metrics, settings, paths, budget: True,
    )

    filtered, dropped = filter_low_quality_jobs(staging, settings=settings, paths=paths)

    assert len(filtered) == 1
    assert len(dropped) == 1


def test_filter_low_quality_jobs_keeps_borderline_pairs_when_gemini_says_distinct(
    sandbox_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    staging = pd.DataFrame(
        [
            {
                "is_active": True,
                "company_name": "하이퍼커넥트",
                "company_tier": "유니콘",
                "job_role": "인공지능 엔지니어",
                "job_title_raw": "Machine Learning Engineer (Match Group AI | 전문연구요원 편입/전직 가능)",
                "job_url": "https://example.com/jobs/1",
                "회사명_표시": "하이퍼커넥트",
                "공고제목_표시": "Machine Learning Engineer (Match Group AI | 전문연구요원 편입/전직 가능)",
                "구분요약_표시": "전문연구요원 / 비전 / 최적화",
                "주요업무_분석용": "비전 모델을 개발합니다.\n최적화 실험을 수행합니다.",
                "자격요건_분석용": "머신러닝 경험이 필요합니다.",
                "우대사항_분석용": "비전 경험 우대",
                "핵심기술_분석용": "머신러닝\n비전",
                "상세본문_분석용": "비전 모델을 개발합니다.\n최적화 실험을 수행합니다.\n머신러닝 경험이 필요합니다.",
            },
            {
                "is_active": True,
                "company_name": "하이퍼커넥트",
                "company_tier": "유니콘",
                "job_role": "인공지능 엔지니어",
                "job_title_raw": "Machine Learning Software Engineer (Match Group AI | 전문연구요원 편입/전직 가능)",
                "job_url": "https://example.com/jobs/2",
                "회사명_표시": "하이퍼커넥트",
                "공고제목_표시": "Machine Learning Software Engineer (Match Group AI | 전문연구요원 편입/전직 가능)",
                "구분요약_표시": "전문연구요원 / 인프라 / 최적화",
                "주요업무_분석용": "인프라와 소프트웨어 시스템을 개발합니다.\n최적화 실험을 수행합니다.",
                "자격요건_분석용": "머신러닝 시스템 경험이 필요합니다.",
                "우대사항_분석용": "인프라 경험 우대",
                "핵심기술_분석용": "머신러닝\n인프라",
                "상세본문_분석용": "인프라와 소프트웨어 시스템을 개발합니다.\n최적화 실험을 수행합니다.\n머신러닝 시스템 경험이 필요합니다.",
            },
        ]
    )
    settings = AppSettings(
        enable_gemini_fallback=True,
        enable_gemini_duplicate_adjudication=True,
        gemini_api_key="test-key",
        gemini_model="gemini-2.5-flash",
        gemini_duplicate_max_calls_per_run=2,
    )
    paths = ProjectPaths.from_root(sandbox_project)

    monkeypatch.setattr(quality_module, "_rows_are_near_practical_duplicates", lambda left, right: False)
    monkeypatch.setattr(
        quality_module,
        "maybe_adjudicate_duplicate_pair",
        lambda left, right, metrics, settings, paths, budget: False,
    )

    filtered, dropped = filter_low_quality_jobs(staging, settings=settings, paths=paths)

    assert len(filtered) == 2
    assert len(dropped) == 0


def test_gemini_duplicate_adjudication_cache_key_includes_policy_version(
    sandbox_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = ProjectPaths.from_root(sandbox_project)
    settings = AppSettings(
        enable_gemini_fallback=True,
        enable_gemini_duplicate_adjudication=True,
        gemini_api_key="test-key",
        gemini_model="gemini-2.5-flash",
        gemini_duplicate_max_calls_per_run=3,
    )
    budget = gemini_module.GeminiBudget(max_calls=3)
    calls: list[str] = []

    monkeypatch.setattr(
        gemini_module,
        "_call_gemini_duplicate_adjudication",
        lambda payload, settings: calls.append(payload["policy_version"]) or {"same_posting": True, "confidence": 0.9},
    )

    left = {
        "company_name": "테스트랩",
        "job_role": "인공지능 엔지니어",
        "job_title_raw": "AI Engineer",
        "job_url": "https://example.com/jobs/1",
        "상세본문_분석용": "모델 개발을 담당합니다.",
    }
    right = {
        "company_name": "테스트랩",
        "job_role": "인공지능 엔지니어",
        "job_title_raw": "AI Engineer",
        "job_url": "https://example.com/jobs/2",
        "상세본문_분석용": "모델 개발을 담당합니다.",
    }
    metrics = {"body_seq": 0.9, "token_score": 0.9, "title_seq": 0.9, "visible_detail_seq": 0.9}

    first = gemini_module.maybe_adjudicate_duplicate_pair(left, right, metrics, settings, paths, budget)
    monkeypatch.setattr(gemini_module, "_DUPLICATE_ADJUDICATION_POLICY_VERSION", "service_duplicate_v4")
    second = gemini_module.maybe_adjudicate_duplicate_pair(left, right, metrics, settings, paths, budget)
    settings_v2 = settings.model_copy(update={"gemini_model": "gemini-2.5-pro"})
    third = gemini_module.maybe_adjudicate_duplicate_pair(left, right, metrics, settings_v2, paths, budget)

    assert first is True
    assert second is True
    assert third is True
    assert calls == ["service_duplicate_v3", "service_duplicate_v4", "service_duplicate_v4"]


def test_normalize_job_analysis_fields_disambiguates_duplicate_display_titles() -> None:
    staging = pd.DataFrame(
        [
            {
                "company_name": "쿠팡",
                "회사명_표시": "쿠팡",
                "job_title_raw": "Staff Data Analyst (Fraud & Risk)",
                "공고제목_표시": "Staff Data Analyst (Fraud & Risk)",
                "job_url": "https://www.coupang.jobs/en/jobs/?gh_jid=7064205",
                "job_role": "데이터 분석가",
                "주요업무_분석용": "분석합니다.",
                "자격요건_분석용": "에스큐엘 경험",
                "우대사항_분석용": "",
                "핵심기술_분석용": "파이썬\n에스큐엘",
                "상세본문_분석용": "분석합니다.\n에스큐엘 경험",
            },
            {
                "company_name": "쿠팡",
                "회사명_표시": "쿠팡",
                "job_title_raw": "Staff Data Analyst (Fraud & Risk)",
                "공고제목_표시": "Staff Data Analyst (Fraud & Risk)",
                "job_url": "https://www.coupang.jobs/en/jobs/?gh_jid=7064215",
                "job_role": "데이터 분석가",
                "주요업무_분석용": "모델을 개선합니다.",
                "자격요건_분석용": "통계 역량",
                "우대사항_분석용": "",
                "핵심기술_분석용": "통계 분석",
                "상세본문_분석용": "모델을 개선합니다.\n통계 역량",
            },
        ]
    )

    normalized = normalize_job_analysis_fields(staging)
    assert set(normalized["공고제목_표시"]) == {
        "Staff Data Analyst (Fraud & Risk) [공고 7064205]",
        "Staff Data Analyst (Fraud & Risk) [공고 7064215]",
    }


def test_normalize_job_analysis_fields_disambiguates_practical_duplicate_title_stems() -> None:
    staging = pd.DataFrame(
        [
            {
                "company_name": "당근",
                "회사명_표시": "당근",
                "job_title_ko": "Software Engineer, Machine Learning",
                "job_title_raw": "Software Engineer, Machine Learning | ML 인프라",
                "공고제목_표시": "Software Engineer, Machine Learning",
                "job_url": "https://about.daangn.com?gh_jid=7498021003",
                "job_role": "인공지능 엔지니어",
                "주요업무_분석용": "모델을 운영합니다.",
                "자격요건_분석용": "파이썬 경험",
                "우대사항_분석용": "",
                "핵심기술_분석용": "파이썬\n머신러닝",
                "상세본문_분석용": "모델을 운영합니다.\n파이썬 경험",
            },
            {
                "company_name": "당근",
                "회사명_표시": "당근",
                "job_title_ko": "Software Engineer, Machine Learning",
                "job_title_raw": "Software Engineer, Machine Learning | 검색 (품질)",
                "공고제목_표시": "Software Engineer, Machine Learning",
                "job_url": "https://about.daangn.com?gh_jid=6610455003",
                "job_role": "인공지능 엔지니어",
                "주요업무_분석용": "검색 품질을 개선합니다.",
                "자격요건_분석용": "파이썬 경험",
                "우대사항_분석용": "",
                "핵심기술_분석용": "파이썬\n머신러닝",
                "상세본문_분석용": "검색 품질을 개선합니다.\n파이썬 경험",
            },
        ]
    )

    normalized = normalize_job_analysis_fields(staging)
    titles = set(normalized["공고제목_표시"])
    assert len(titles) == 2
    assert any("ML 인프라" in title for title in titles)
    assert any("검색" in title for title in titles)


def test_runtime_state_bundle_roundtrip_restores_manifest_files(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    runtime_dir = project_root / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = project_root / "runtime_state_manifest.txt"
    manifest_path.write_text(
        "runtime/master_jobs.csv\nruntime/staging_jobs.csv\nruntime/source_registry.csv\n",
        encoding="utf-8",
    )
    (runtime_dir / "master_jobs.csv").write_text("a,b\n1,2\n", encoding="utf-8")
    (runtime_dir / "staging_jobs.csv").write_text("c,d\n3,4\n", encoding="utf-8")
    bundle_path = tmp_path / "runtime_state.tar.gz"

    archived = runtime_state_module.build_runtime_state_bundle(project_root, bundle_path, manifest_path)
    assert archived == ["runtime/master_jobs.csv", "runtime/staging_jobs.csv"]

    shutil.rmtree(runtime_dir)
    runtime_dir.mkdir(parents=True, exist_ok=True)
    restored = runtime_state_module.restore_runtime_state_bundle(project_root, bundle_path)
    assert restored == ["runtime/master_jobs.csv", "runtime/staging_jobs.csv"]
    assert (runtime_dir / "master_jobs.csv").read_text(encoding="utf-8") == "a,b\n1,2\n"
    assert (runtime_dir / "staging_jobs.csv").read_text(encoding="utf-8") == "c,d\n3,4\n"


def test_runtime_state_bundle_rejects_unsafe_members(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir(parents=True, exist_ok=True)
    bundle_path = tmp_path / "unsafe.tar.gz"

    import tarfile

    unsafe_file = tmp_path / "unsafe.txt"
    unsafe_file.write_text("bad", encoding="utf-8")
    with tarfile.open(bundle_path, "w:gz") as archive:
        archive.add(unsafe_file, arcname="../escape.txt")

    with pytest.raises(ValueError, match="Unsafe archive member"):
        runtime_state_module.restore_runtime_state_bundle(project_root, bundle_path)
