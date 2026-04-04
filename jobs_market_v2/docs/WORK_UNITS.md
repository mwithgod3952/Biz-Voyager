# 작업 단위 기록

이 문서는 반복 개발을 `작업 단위`로 적립하기 위한 기록이다. 각 단위는 아래 4가지를 반드시 남긴다.

- 코드 변경 범위
- 실행 명령
- 테스트 결과
- 다음 단위의 입력 산출물

운영 원칙:

- 먼저 `모집단 1차 수집`을 만든다.
- 그 다음부터 `증분 갱신`으로 운영한다.

## 작업 단위 19

이름: `Conservative Closed-Signal Guard + Gowid Activation`

목적:
- 긴 공고 본문 안의 조건부 마감 안내만으로 active 공고를 `closed`로 오인하는 문제를 막는다.
- GreetingHR 계열에서 이미 parsed는 되는데 `active_signal = false`로 죽는 AI/data 공고를 실제 active source로 되살린다.

주요 코드:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/collection.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py`

실행 명령:

```bash
source .venv/bin/activate
pytest -q tests/test_jobs_market_v2.py -k 'closed_html_notice or conditional_early_close or hydrate_html_job_details_uses_title_content_pairs'

python -u - <<'PY'
import json
import shutil
from datetime import datetime
from pathlib import Path
import pandas as pd
from jobs_market_v2.settings import get_paths
from jobs_market_v2.pipelines import update_incremental_pipeline, promote_staging_pipeline, sync_sheets_pipeline, doctor_pipeline

root = Path('/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2')
paths = get_paths(root)
backup_dir = paths.runtime_dir / 'tmp_gowid_backup_' / datetime.now().strftime('%Y%m%d%H%M%S')
backup_dir.mkdir(parents=True, exist_ok=True)
for p in (paths.master_jobs_path, paths.staging_jobs_path, paths.source_registry_path, paths.quality_gate_path):
    if p.exists():
        shutil.copy2(p, backup_dir / p.name)

registry = pd.read_csv(paths.source_registry_path).fillna('')
subset = registry[registry['source_url'].astype(str).eq('https://gowid.career.greetinghr.com')].copy()

summary = update_incremental_pipeline(
    project_root=root,
    allow_source_discovery_fallback=False,
    enable_source_scan_progress=False,
    registry_frame=subset,
)
print(summary)
if summary.get('quality_gate_passed'):
    print(promote_staging_pipeline(project_root=root))
    print(sync_sheets_pipeline('master', project_root=root))
    print(sync_sheets_pipeline('staging', project_root=root))
    print(doctor_pipeline(project_root=root))
PY
```

검증 결과:
- targeted pytest: 통과
- probe:
  - `고위드 parsed 11 / accepted 0 -> accepted 1`
  - `[공통] Data Scientist`가 `경력 / 일반채용 / 시계열 / 제품분석`으로 정상 publish candidate가 됨
- bounded publish:
  - `master 100 -> 100`
  - `staging 100 -> 100`
  - `new_job_count = 0`
  - `unchanged_job_count = 1`
  - `quality_gate_passed = true`
- source registry:
  - `https://gowid.career.greetinghr.com`
  - `last_active_job_count 0 -> 1`
- `sync-sheets --target master`: 성공
- `sync-sheets --target staging`: 성공
- `doctor`: 통과

산출물:
- `runtime/master_jobs.csv = 100`
- `runtime/staging_jobs.csv = 100`
- `runtime/source_registry.csv`에서 `active_gt_0_sources = 41`
- `고위드` published row:
  - `[공통] Data Scientist`

다음 단위 입력:
- growth candidate:
  - `알체라`
  - `가치랩스`
  - remaining zero-active verified source families
- metadata blocker:
  - `경력수준_표시 blank = 12`
  - `우대사항_표시 blank = 15`

## 작업 단위 18

이름: `MedicalAI Detail Hydration + Bounded Publish`

목적:
- generic HTML detail page에서 `Recruit` 같은 shell title이 detail/ listing title을 덮지 못하게 막는다.
- `div.title + div.content` layout을 범용 detail field pair로 읽어 `직무내용/직무 상세/지원 자격`을 실제 분석 섹션으로 회수한다.
- `메디컬에이아이`를 `zero-active verified source`에서 실제 `active source`로 전환한다.

주요 코드:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/collection.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/presentation.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py`

실행 명령:

```bash
source .venv/bin/activate
pytest -q tests/test_jobs_market_v2.py -k 'hydrate_html_job_details or korean_job_detail_headings or display_fields_defaults_general_track_without_polluting_summary'

python -u - <<'PY'
import json
import shutil
from datetime import datetime
from pathlib import Path
import pandas as pd
from jobs_market_v2.settings import get_paths
from jobs_market_v2.pipelines import update_incremental_pipeline, promote_staging_pipeline, sync_sheets_pipeline, doctor_pipeline

root = Path('/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2')
paths = get_paths(root)
backup_dir = paths.runtime_dir / 'tmp_medicalai_backup_' / datetime.now().strftime('%Y%m%d%H%M%S')
backup_dir.mkdir(parents=True, exist_ok=True)
for p in (paths.master_jobs_path, paths.staging_jobs_path, paths.source_registry_path, paths.quality_gate_path):
    if p.exists():
        shutil.copy2(p, backup_dir / p.name)

registry = pd.read_csv(paths.source_registry_path).fillna('')
subset = registry[registry['source_url'].astype(str).eq('https://medicalai.com/en/recruit')].copy()

summary = update_incremental_pipeline(
    project_root=root,
    allow_source_discovery_fallback=False,
    enable_source_scan_progress=False,
    registry_frame=subset,
)
print(summary)
if summary.get('quality_gate_passed'):
    print(promote_staging_pipeline(project_root=root))
    print(sync_sheets_pipeline('master', project_root=root))
    print(sync_sheets_pipeline('staging', project_root=root))
    print(doctor_pipeline(project_root=root))
PY
```

검증 결과:
- targeted pytest: 통과
- bounded publish:
  - `baseline master 98 -> master 100`
  - `staging 98 -> 100`
  - `new_job_count = 2`
  - `net_active_job_delta = 2`
  - `quality_gate_passed = true`
- `promote-staging`: 성공
- `sync-sheets --target master`: 성공
- `sync-sheets --target staging`: 성공
- `doctor`: 통과
- source registry:
  - `https://medicalai.com/en/recruit`
  - `last_active_job_count 0 -> 2`

산출물:
- `runtime/master_jobs.csv = 100`
- `runtime/staging_jobs.csv = 100`
- `runtime/sheets_exports/master/master_탭.csv = 100`
- `runtime/sheets_exports/staging/staging_탭.csv = 100`
- `메디컬에이아이` published rows:
  - `메디컬그룹 DS(Data Science)팀 연구원`
  - `상시채용 R&D Center AI Group`

다음 단위 입력:
- growth candidate:
  - `알체라`
  - `가치랩스`
  - additional zero-active verified source families
- metadata blocker:
  - `경력수준_표시 blank = 12`
  - `우대사항_표시 blank = 15`

## 작업 단위 17

이름: `General Hiring Track Default + Blank-Source Refresh`

목적:
- `채용트랙_표시` 공란을 schema 정책 차원에서 닫는다.
- 특수 트랙이 없는 일반 AI/data 공고도 분석 가능한 기본 분류(`일반채용`)를 갖게 한다.
- blank hotspot source만 다시 수집해 published/master/sheet까지 즉시 반영한다.

주요 코드:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/presentation.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py`

실행 명령:

```bash
source .venv/bin/activate
pytest -q tests/test_jobs_market_v2.py -k 'display_fields or normalize_job_analysis_fields or quality_gate_reports_quality_score_and_track_cue_metrics'

python -u - <<'PY'
import os
from pathlib import Path
from jobs_market_v2.storage import read_csv_or_empty
from jobs_market_v2.constants import SOURCE_REGISTRY_COLUMNS
from jobs_market_v2.pipelines import update_incremental_pipeline, promote_staging_pipeline, sync_sheets_pipeline, doctor_pipeline

os.environ['JOBS_MARKET_V2_GOOGLE_SHEETS_TIMEOUT_SECONDS'] = '60'
os.environ['JOBS_MARKET_V2_GOOGLE_SHEETS_CONNECT_TIMEOUT_SECONDS'] = '10'

project_root = Path('/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2')
runtime = project_root / 'runtime'
target_urls = [
    'https://boards-api.greenhouse.io/v1/boards/daangn/jobs?content=true',
    'https://makinarocks.career.greetinghr.com',
    'https://interxlab.career.greetinghr.com/aboutinterxlab',
    'https://careers.devsisters.com',
    'https://boards-api.greenhouse.io/v1/boards/sendbird/jobs?content=true',
    'https://kakaomobility.career.greetinghr.com',
    'https://gowid.career.greetinghr.com',
]
registry = read_csv_or_empty(runtime / 'source_registry.csv', SOURCE_REGISTRY_COLUMNS)
subset = registry[registry['source_url'].isin(target_urls)].copy()
print(update_incremental_pipeline(project_root, allow_source_discovery_fallback=False, enable_source_scan_progress=False, registry_frame=subset, registry_output_path=runtime / 'source_registry.csv'))
print(promote_staging_pipeline(project_root))
print(sync_sheets_pipeline('staging', project_root))
print(sync_sheets_pipeline('master', project_root))
print(doctor_pipeline(project_root))
PY
```

검증 결과:
- targeted pytest: 통과
- bounded refresh:
  - `baseline master 91 -> master 98`
  - `staging 91 -> 98`
  - `new_job_count = 7`
  - `net_active_job_delta = 7`
  - `quality_gate_passed = true`
- `sync-sheets --target staging`: 성공
- `sync-sheets --target master`: 성공
- `doctor`: 통과
- latest quality:
  - `quality_score_100 = 99.07`
  - `hiring_track_blank_ratio = 0.0`

산출물:
- `runtime/master_jobs.csv = 98`
- `runtime/staging_jobs.csv = 98`
- `runtime/sheets_exports/master/master_탭.csv = 98`
- `runtime/sheets_exports/staging/staging_탭.csv = 98`
- `채용트랙_표시 blank = 0`

다음 단위 입력:
- `경력수준_표시 blank = 11` hotspot
  - `당근`
  - `데이터메이커`
  - `뷰노`
  - `서울로보틱스`
- `우대사항_표시 blank = 15` hotspot
  - `센드버드`
  - `몰로코`
  - `엔젤로보틱스`
- 다음 growth source 후보:
  - `메디컬에이아이`
  - `알체라`
  - `가치랩스`

## 작업 단위 16

이름: `Polaris Growth + Sync Retry + Closed Notice Guard`

목적:
- `verified 성공인데 active 0`인 실제 growth source를 하나 더 `master` 증분으로 번역한다.
- Google Sheets sync timeout 한 번에 전체 run이 죽는 운영 불안정을 줄인다.
- generic HTML source에서 닫힌 공고를 active AI/data 공고로 잘못 세는 문제를 막는다.

주요 코드:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/collection.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/sheets.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py`

실행 명령:

```bash
source .venv/bin/activate
python - <<'PY'
from pathlib import Path
import csv
import pandas as pd
from jobs_market_v2.pipelines import update_incremental_pipeline, promote_staging_pipeline
from jobs_market_v2.settings import ProjectPaths
from jobs_market_v2.constants import SOURCE_REGISTRY_COLUMNS

root = Path('/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2')
paths = ProjectPaths.from_root(root)
with paths.source_registry_path.open(newline='', encoding='utf-8') as f:
    rows = list(csv.DictReader(f))
subset = [r for r in rows if r.get('source_url') == 'https://polarisofficerecruit.career.greetinghr.com']
frame = pd.DataFrame(subset)
for c in SOURCE_REGISTRY_COLUMNS:
    if c not in frame.columns:
        frame[c] = ''
frame = frame[list(SOURCE_REGISTRY_COLUMNS)]
print(update_incremental_pipeline(project_root=root, allow_source_discovery_fallback=False, enable_source_scan_progress=False, registry_frame=frame))
print(promote_staging_pipeline(project_root=root))
PY

export JOBS_MARKET_V2_GOOGLE_SHEETS_TIMEOUT_SECONDS=60
export JOBS_MARKET_V2_GOOGLE_SHEETS_CONNECT_TIMEOUT_SECONDS=10
python -m jobs_market_v2.cli sync-sheets --target master
python -m jobs_market_v2.cli sync-sheets --target staging
python -m jobs_market_v2.cli doctor
pytest -q tests/test_jobs_market_v2.py -k 'saramin_as_external_recruit_host or drops_closed_html_notice or sync_tabs_to_google_sheets or bytesize or next_rsc_notice_family_roles or embedded_greetinghr_custom_domain'
```

검증 결과:
- `Polaris` probe: `parsed 3 / accepted 1`
- bounded publish:
  - `master 85 -> 86`
  - `staging 85 -> 86`
  - `new_job_count = 1`
  - `net_job_delta = 1`
  - `net_active_job_delta = 1`
- registry:
  - `https://polarisofficerecruit.career.greetinghr.com`
  - `last_active_job_count 0 -> 1`
- `sync-sheets --target master`: 성공
- `sync-sheets --target staging`: 성공
- `doctor`: 통과
- targeted pytest: 통과
- `Meissa` probe after closed-notice guard:
  - `parsed 41 / accepted 0`

산출물:
- `runtime/master_jobs.csv = 86`
- `runtime/staging_jobs.csv = 86`
- `runtime/source_registry.csv`에서 `active_gt_0_sources = 41`

다음 단위 입력:
- `알체라` external recruit host follow
- `greenhouse` metadata blank 축소
- `verified_success -> active_gt_0` 전환률 개선

## 작업 단위 15

이름: `Conservative Duplicate Recovery`

목적:
- broad near-duplicate collapse 때문에 distinct variant가 잘못 삭제된 문제를 되돌린다.
- `명시적 차이`가 있는 공고는 보존하고, 정말 같은 공고만 접는 보수 정책으로 전환한다.
- published와 sheet를 다시 일치시킨다.

주요 코드:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/quality.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/runtime/staging_jobs.csv`

실행 명령:

```bash
python -m jobs_market_v2.cli doctor
python -m jobs_market_v2.cli promote-staging
python -m jobs_market_v2.cli sync-sheets --target master
python -m jobs_market_v2.cli sync-sheets --target staging
```

검증 결과:
- duplicate false-positive 방지 회귀 8개 통과
- `doctor`: 통과
- snapshot union 복구 후 `promote-staging`: `promoted_job_count = 104`
- `sync-sheets --target master`: 성공
- `sync-sheets --target staging`: 성공

산출물:
- `runtime/master_jobs.csv = 104`
- `runtime/staging_jobs.csv = 104`
- `runtime/sheets_exports/master/master_탭.csv = 104`
- `runtime/sheets_exports/staging/staging_탭.csv = 104`

다음 단위 입력:
- gray-zone unresolved duplicate pairs
- `approved/source -> master` 번역률 보강 작업

## 작업 단위 0

이름: `야간 로컬 자동화 러너`

목적:
- 내장 app automation이 `ARCHIVED`로 즉시 사라져 가시성이 없는 문제를 우회한다.
- `codex exec` 기반으로 실제 개선 run을 시간반복으로 실행한다.
- 시작/종료 heartbeat와 visible summary를 파일에 남겨 아침에 바로 확인할 수 있게 한다.

주요 파일:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/scripts/hourly_master_growth_prompt.txt`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/scripts/run_hourly_master_growth_once.sh`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/scripts/run_hourly_master_growth_loop.sh`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/scripts/start_hourly_master_growth.sh`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/scripts/stop_hourly_master_growth.sh`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/scripts/write_automation_status.py`

확인 파일:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/runtime/automation_status.json`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/docs/AUTOMATION_STATUS.md`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/runtime/automation/hourly_master_growth_*.log`

현재 상태:
- 1회성 검증 run이 먼저 실행 중이다.
- 그 run이 끝나면 launcher가 시간반복 루프를 자동 시작한다.
- 중복 실행 방지를 위해 lock 디렉터리를 사용한다.

## 작업 단위 13

이름: `One-line Full Cycle Runtime Hardening`

목적:
- 반복 실행 시 `collect-company-seed-records`가 full cycle 시작부에서 오래 붙는 문제를 줄인다.
- 최근 seed record는 재사용하고, catalog discovery는 per-run runtime budget을 넘기면 다음 offset으로 이어받게 만든다.
- fresh `run-collection-cycle` 성공 row를 다시 확보한다.

주요 코드:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/settings.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/company_seed_sources.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/pipelines.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py`

검증 결과:
- `pytest -q tests/test_jobs_market_v2.py -k 'processes_catalog_sources_in_batches or respects_runtime_budget or reuses_recent_cached_records'`: 통과
- `pytest -q`: 전체 통과
- fresh one-line cycle:
  - `run-collection-cycle-20260331170304`
  - `automation_ready true`
  - `started_at 2026-03-31T17:03:04+09:00`
  - `finished_at 2026-03-31T17:07:25+09:00`
  - `staging/master sync` 둘 다 성공

## 작업 단위 14

이름: `Top-level Catalog Search Batching`

목적:
- 웹검색 기반 top-level catalog 자동 발견을 더 넓히되, 한 번의 실행이 검색 쿼리 전량 때문에 무거워지지 않게 한다.
- `site:go.kr / or.kr / re.kr / kr` + 공공·지원기관 키워드 조합으로 검색 범위를 넓힌다.
- query batch + cursor로 다음 run이 이어받게 만든다.

주요 코드:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/company_seed_sources.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/settings.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py`

검증 결과:
- `pytest -q tests/test_jobs_market_v2.py -k 'search_catalog_queries_expand_site_and_org_terms or search_catalog_query_urls_match_generated_queries or processes_queries_in_batches'`: 통과
- `python -m jobs_market_v2.cli discover-company-seed-sources`: 성공

## 작업 단위 1

이름: `기업 후보군 / 근거 / 승인 계층`

목적:
- 소스 수집 이전에 `직무 적합 기업 후보군`을 만들고 근거를 적재한다.
- 기업을 `approved / candidate / rejected`로 나눠 이후 source discovery 입력을 통제한다.
- 각 후보에 `왜 처음 후보가 되었는지`를 설명하는 후보시드 provenance를 남긴다.
- `company_seed_records.csv`와 공식 공개 채용 소스를 우선 사용하고, source-backed 입력이 없을 때만 bootstrap fallback을 사용한다.
- 이후 상세정보 수집체계가 붙더라도 같은 승인 기업 집합을 재사용할 수 있게 한다.

주요 코드:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/company_screening.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/pipelines.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/cli.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py`

산출물:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/runtime/company_candidates.csv`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/runtime/company_evidence.csv`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/runtime/approved_companies.csv`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/runtime/candidate_companies.csv`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/runtime/rejected_companies.csv`

실행 명령:

```bash
python -m jobs_market_v2.cli discover-companies
python -m jobs_market_v2.cli collect-company-evidence
python -m jobs_market_v2.cli screen-companies
python -m jobs_market_v2.cli discover-sources
```

완료 조건:
- company candidate / evidence / bucket 파일 5종 생성
- approved 기업 우선 source discovery 동작
- pytest 전체 통과
- doctor 통과

검증 결과:
- `pytest --collect-only -q`: `37` tests
- `pytest -q -rA`: 전체 통과
- `python -m jobs_market_v2.cli doctor`: 통과
- `python -m jobs_market_v2.cli discover-companies`
  - raw candidate `21`
  - discovered company `21`
  - non-company removed `0`
  - candidate input mode `source_backed_seed_records`
  - seeded candidate `21`
- `python -m jobs_market_v2.cli collect-company-evidence`
  - candidate company `21`
  - company evidence `126`
  - approved company `15`
  - candidate bucket `6`
  - verified source success `15`
  - active target job `68`
- 후보시드URL 보유 기업 `21`
- approved 중 후보시드URL 보유 기업 `15`
- 근거 유형 분포:
  - `타깃직무공고 68`
  - `공식도메인 21`
  - `후보시드근거 21`
  - `공식채용소스 15`
- `python -m jobs_market_v2.cli screen-companies`
  - approved `15`
  - candidate `6`
  - rejected `0`
- `python -m jobs_market_v2.cli discover-sources`
  - company input mode `approved_companies`
  - company input count `15`
  - screened source `15`
  - approved source `9`
  - candidate source `6`

현재 approved 기업층 분포:
- 스타트업 `5`
- 중견/중소 `4`
- 대기업 `3`
- 외국계 한국법인 `2`
- 공공·연구기관 `1`

다음 단위 입력:
- `approved_companies.csv`
- `company_evidence.csv`
- `source_registry.csv`

## 작업 단위 2

이름: `후보군 확장 수집`

목적:
- 회사명을 직접 넣는 대신 `회사 목록 출처`를 읽어 seed record를 수집한다.
- `csv`, `xlsx`, `html table` 기반 공식/공개 목록을 회사 후보 레코드로 바꿀 수 있게 한다.
- 이후 수백 개 후보군 확장 코드를 붙일 때 공통 수집기를 재사용한다.

주요 코드:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/company_seed_sources.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/pipelines.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/cli.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/discovery.py`

주요 설정:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/config/company_seed_sources.yaml`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/config/company_seed_records.csv`

실행 명령:

```bash
python -m jobs_market_v2.cli collect-company-seed-records
python -m jobs_market_v2.cli expand-company-candidates
```

검증 결과:
- `pytest --collect-only -q`: `43` tests
- `pytest -q`: 전체 통과
- `python -m jobs_market_v2.cli doctor`: 통과
- `python -m jobs_market_v2.cli collect-company-seed-records`
  - seed source `3`
  - collected seed record `812`
  - collected company `812`
  - source raw counts
    - KIND `2767 -> 660`
    - ALIO `344 -> 133`
    - NST `19 -> 19`
- `python -m jobs_market_v2.cli expand-company-candidates`
  - expanded candidate company `814`
  - candidate input mode `source_backed_seed_records`
  - seeded candidate `826`
- `python -m jobs_market_v2.cli collect-company-evidence`
  - candidate company `814`
  - company evidence `1687`
  - approved `15`
  - candidate `775`
  - rejected `24`
  - verified source success `15`
  - active target job `68`
- `python -m jobs_market_v2.cli screen-companies`
  - approved `15`
  - candidate `775`
  - rejected `24`
- `python -m jobs_market_v2.cli sync-sheets --target staging`: `google_sheets_synced=true`
- `python -m jobs_market_v2.cli sync-sheets --target master`: `google_sheets_synced=true`

현재 상태:
- 확장 수집은 이제 `실제 공식 출처 3개`에서 동작한다.
- `기업선정 탭`은 `814행`으로 확장되었다.
- approved는 아직 기존 verified live 기업 `15개`이고, 신규 대량 후보는 대부분 `candidate` 버킷이다.
- 다음 병목은 `후보군 확장`이 아니라 `candidate 대량 집합의 공식 source discovery / 공식 domain 보강`이다.

다음 단위 입력:
- `config/company_seed_sources.yaml`
- `runtime/company_seed_records_collected.csv`
- `runtime/company_candidates.csv`

## 작업 단위 3

이름: `candidate -> approved 전환용 source discovery 보강`

목적:
- 수백 개 후보군에서 실제 공식 채용 source를 더 많이 연결한다.
- noisy homepage link를 줄이고, `careers.*`/`recruit.*` 같은 직접 채용 도메인을 source 후보로 인정한다.
- 후보군 확장 이후 병목이던 `approved 증가`를 실제로 개선한다.

주요 코드:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/discovery.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/company_screening.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py`

주요 변경:
- homepage probe에서 root 홈페이지, 공지/뉴스/IR 계열 링크 제외
- same-domain html link는 URL 자체에 채용 신호가 있을 때만 통과
- fragment/query 중복 정리
- `careers.*`, `recruit.*` 같은 직접 채용 도메인은 `official_domain_probe`로 source 후보에 직접 추가

실행 명령:

```bash
python -m jobs_market_v2.cli collect-company-evidence
python -m jobs_market_v2.cli screen-companies
python -m jobs_market_v2.cli sync-sheets --target staging
python -m jobs_market_v2.cli sync-sheets --target master
```

검증 결과:
- 타깃 테스트:
  - `pytest -q tests/test_jobs_market_v2.py -k 'homepage_links or direct_career_domain or skips_root_and_notice'`
  - 통과
- `python -m jobs_market_v2.cli doctor`
  - 통과
- source candidate 재계산:
  - `1562 -> 345 -> 351`
  - latest source type 분포
    - `html_page 296`
    - `greetinghr 25`
    - `recruiter 21`
    - `greenhouse 6`
    - `lever 3`
- 최신 `collect-company-evidence`
  - `candidate company 814`
  - `company evidence 2032`
  - `approved 19`
  - `candidate 771`
  - `rejected 24`
  - `verified source success 308`
  - `active target job 80`
- 최신 `screen-companies`
  - `approved 19`
  - `candidate 771`
  - `rejected 24`
- 최신 시트 반영
  - `sync-sheets --target staging`: `google_sheets_synced=true`
  - `sync-sheets --target master`: `google_sheets_synced=true`

현재 상태:
- approved는 `15 -> 19 -> 21`로 증가했다.
- 신규 approved 주요 예시는 `딥노이드`, `씨어스테크놀로지`, `네이버`다.
- direct hiring domain 타깃 refresh 결과:
  - `네이버`: `verified_source_count 2`, `active_job_count 1`, `approved`
  - `삼성전자`: `verified_source_count 2`, `active_job_count 0`, `candidate`
  - `한국과학기술연구원`: `verified_source_count 1`, `active_job_count 0`, `candidate`
  - `한국전자통신연구원`: `verified_source_count 0`, `active_job_count 0`, `candidate`
- 최신 bucket 상태는 `approved 21 / candidate 769 / rejected 24`다.
- 전체 pytest는 오래 매달리는 현상이 있어, 이번 단위에서는 타깃 테스트 + doctor + 실제 live probe + 시트 재동기화 결과를 기준으로 검증했다.

다음 단위 입력:
- `runtime/source_registry.csv`
- `runtime/company_candidates.csv`
- `runtime/company_evidence.csv`

## 작업 단위 4

이름: `공식 카탈로그 기반 seed source 자동 발견`

목적:
- 등록된 공식 기관/사업 카탈로그 페이지에서 첨부 `csv/xlsx/html table` seed source를 자동 발견한다.
- 발견된 seed source를 다시 `company seed record` 수집기로 태워 후보군을 대량 확장한다.
- bootstrap/manual seed가 아니라 `공식 카탈로그 -> 첨부 목록 -> 회사 레코드` 경로를 실제로 연다.

주요 코드:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/company_seed_sources.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/constants.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/settings.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/config/company_seed_sources.yaml`

주요 변경:
- `html_link_catalog`, `html_link_catalog_url`, `html_link_catalog_file` 경로 보강
- detail page 안의 첨부 `csv/xlsx` follow 지원
- `company_seed_sources_discovered.csv` 런타임 적재
- 발견된 source가 파싱 불가일 때 skip하고 summary에 남기기
- 혼합 문자열(`지역/홈페이지`)에서 `official_domain`, `region` 정제 개선
- 중복 `discover_company_seed_sources` 정의 제거
- `collect-company-evidence` 배치 처리, 체크포인트 파일, 진행 커서 추가
- 반복 실행 시 `next_offset`부터 이어받는 staged reevaluation 지원

실행 명령:

```bash
python -m jobs_market_v2.cli discover-company-seed-sources
python -m jobs_market_v2.cli collect-company-seed-records
python -m jobs_market_v2.cli expand-company-candidates
```

검증 결과:
- `pytest --collect-only`: `58 tests`
- `pytest -q`: 전체 통과
- `python -m jobs_market_v2.cli doctor`: 통과
- `python -m jobs_market_v2.cli discover-company-seed-sources`
  - `catalog_source_count 2`
  - `discovered_seed_source_count 1`
  - NIPA 공식 공고 첨부 xlsx 1건 자동 발견
- `python -m jobs_market_v2.cli collect-company-seed-records`
  - `seed_source_count 4`
  - `collected_seed_record_count 2240`
  - `collected_company_count 2226`
  - NIPA 첨부 xlsx `1428 -> 1428`
- `python -m jobs_market_v2.cli expand-company-candidates`
  - `expanded_candidate_company_count 2224`
  - `candidate_input_mode source_backed_seed_records`
- `python -m jobs_market_v2.cli collect-company-evidence --batch-size 200 --max-batches 1`
  - 1회차 `start_offset 0 -> next_offset 200`
  - 2회차 `start_offset 200 -> next_offset 400`
- `python -m jobs_market_v2.cli run-collection-cycle --skip-sync`
  - 종료 성공
  - `company_evidence.batch_count 2`
  - `company_evidence.start_offset 400`
  - `company_evidence.next_offset 800`
  - `promotion.promoted_job_count 86`

현재 상태:
- 공식 카탈로그를 등록해 두면, 그 안의 첨부 목록에서 seed source를 자동 발견해 후보군까지 확장할 수 있다.
- 즉 `새 seed source를 수동으로 하나씩 적는 단계`는 일부 벗어났다.
- 다만 `새 공식 카탈로그 도메인 자체를 웹에서 자동 발굴`하는 단계는 아직 아니다.
- 또한 `2224 candidate`를 끝까지 재평가해 approved로 승격시키는 full unattended loop는 staged reevaluation까지는 닫혔지만, 단일 명령 full automation은 아직 종료 안정성을 더 봐야 한다.
- 현재는 `공식 카탈로그 기반 후보군 자동 확장`과 `candidate 재평가 staged automation`까지는 완료, `run-collection-cycle` 내부 sync 포함 full automation은 보류가 맞다.

다음 단위 입력:
- `runtime/company_seed_sources_discovered.csv`
- `runtime/company_seed_records_collected.csv`
- `runtime/companies_registry.csv`

## 작업 단위 4

이름: `direct hiring domain 보정과 타깃 approved 전환`

목적:
- stale official domain을 실제 채용 진입점으로 교정한다.
- direct hiring domain 회사를 전체 재평가 전에 소규모로 refresh해 approved 전환 가능성을 확인한다.
- approved 기준 1차 모집단 수집으로 넘어가기 전, 대표 blind spot을 줄인다.

주요 코드:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/discovery.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/collection.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/config/manual_companies_seed.csv`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/config/company_seed_records.csv`

주요 변경:
- stale official domain 교정
  - `네이버 -> recruit.navercorp.com`
  - `삼성전자 -> samsungcareers.com`
  - `한국전자통신연구원 -> etri.re.kr`
  - `한국과학기술연구원 -> kist.re.kr`
- same-domain hiring link 규칙 보강
  - `employment-announcement`, `positions open`, `rcrt`, `hr` 등은 유지
  - policy/login/user/people/story/wellness/subsid 계열은 차단
- html anchor listing fallback 보강
  - `onclick show('...')`를 detail URL로 전환

실행 명령:

```bash
pytest -q tests/test_jobs_market_v2.py -k 'homepage_links or skips_root_and_notice_like_homepage_links or keeps_employment_announcement_page or skips_policy_and_login_pages_on_direct_career_domain or direct_career_domain or official_domain_resolution_uses_manual_seed or anchor_listing_cards_with_onclick_detail'
python -m jobs_market_v2.cli sync-sheets --target staging
python -m jobs_market_v2.cli sync-sheets --target master
```

검증 결과:
- 타깃 pytest `7 passed`
- live probe
  - `네이버 recruit.navercorp.com/rcrt/list.do` -> `인공지능 리서처 1건`
  - `삼성전자 samsungcareers.com` -> source verification 성공, 타깃 공고 0건
  - `한국과학기술연구원 employment-announcement` -> source verification 성공, 타깃 공고 0건
- targeted refresh 후
  - `approved_company_count_after_merge 21`
  - `verified_source_success_count 5`
  - `collected_job_count 1`
- 시트 반영
  - `sync-sheets --target staging`: 성공
  - `sync-sheets --target master`: 성공

현재 상태:
- 후보군 확장은 잠시 멈추고 approved 전환이 병목이다.
- `네이버`는 실제 approved로 전환되었고, direct hiring domain 보정 방식이 유효함이 확인됐다.
- 다음 우선순위는 `삼성전자 / 한국과학기술연구원 / 한국전자통신연구원`처럼 verified source는 있거나 근접했지만 타깃 공고로 이어지지 않는 군을 더 줄이는 것이다.

## 작업 단위 5

이름: `same-domain source trimming과 alias canonicalization`

목적:
- direct hiring domain과 same-domain html_page source에서 noisy 링크를 줄여 approved 전환 신호를 더 선명하게 만든다.
- `NAVER`처럼 alias로 들어온 회사를 정식 회사명으로 canonicalize해 중복 approved를 제거한다.

주요 코드:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/discovery.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py`

주요 변경:
- same-domain html source 후보를 점수화해 회사당 상위 경로만 유지
  - `employment-announcement`, `rcrt`, `positions`, `hr` 등 강한 채용 path 우선
  - `main.do`, `index.jsp`, `cnts/*`, `job-request`류는 제거
- direct hiring domain에서도 root + 실제 listing/detail path만 남도록 trimming
- manual alias와 매칭되는 회사명은 resolve 단계에서 정식 회사명으로 치환
  - `NAVER -> 네이버`
- canonicalize 후 `company_name` 기준 dedupe 수행

실행 명령:

```bash
python -m jobs_market_v2.cli discover-companies
python -m jobs_market_v2.cli collect-company-evidence
python -m jobs_market_v2.cli screen-companies
python -m jobs_market_v2.cli sync-sheets --target staging
python -m jobs_market_v2.cli sync-sheets --target master
```

검증 결과:
- 타깃 pytest `11 passed`
  - discovery homepage probe
  - direct career domain
  - noisy same-domain trimming
  - alias canonicalization
  - html anchor listing fallback
- 최신 `discover-companies`
  - `814 -> 813`
  - `대기업 63 -> 62`
- 최신 `collect-company-evidence`
  - `candidate company 813`
  - `company evidence 2022`
  - `approved 21`
  - `candidate 768`
  - `rejected 24`
  - `verified source success 329`
  - `active target job 84`
- 최신 `screen-companies`
  - `approved 21`
  - `candidate 768`
  - `rejected 24`
- 시트 반영
  - `sync-sheets --target staging`: 성공
  - `sync-sheets --target master`: 성공

현재 상태:
- `네이버`는 `approved`로 남고, `NAVER` 중복은 제거됐다.
- `삼성전자`는 `verified_source_count 2`지만 `active_job_count 0`이라 아직 `candidate`다.
- `한국과학기술연구원`은 `verified_source_count 1`이지만 `active_job_count 0`이라 아직 `candidate`다.
- `한국전자통신연구원`은 여전히 usable source가 없다.
- 지금 병목은 후보군 확대가 아니라 `candidate 768 -> approved` 전환이다.

## 작업 단위 6

이름: `approved 기업 기준 모집단 1차 수집`

목적:
- `approved 21` 기업만 대상으로 공식 source를 다시 발견하고 검증해 첫 모집단 스냅샷을 만든다.
- 품질 게이트를 통과한 결과를 `master`까지 승격해 이후 증분 운영의 기준점을 확정한다.

주요 코드:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/pipelines.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/collection.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/quality.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/reporting.py`

실행 명령:

```bash
python -m jobs_market_v2.cli discover-sources
python -m jobs_market_v2.cli verify-sources
python -m jobs_market_v2.cli collect-jobs
python -m jobs_market_v2.cli build-coverage-report
python -m jobs_market_v2.cli promote-staging
python -m jobs_market_v2.cli sync-sheets --target staging
python -m jobs_market_v2.cli sync-sheets --target master
```

검증 결과:
- `discover-sources`
  - `approved_source_count 19`
  - `candidate_source_count 6`
  - `screened_source_count 25`
  - `company_input_count 21`
- `verify-sources`
  - `verified_source_success_count 25`
  - `verified_source_failure_count 0`
  - `collected_job_count 84`
- `collect-jobs`
  - `collected_job_count 84`
  - `quality_gate_passed true`
  - `quality_gate_reasons []`
- `build-coverage-report`
  - `활성 공고 84`
  - `데이터 분석가 13`
  - `데이터 사이언티스트 11`
  - `인공지능 리서처 24`
  - `인공지능 엔지니어 36`
  - `HHI 0.0751`
  - `top5 집중도 0.5119`
- `promote-staging`
  - `promoted_job_count 84`
- 시트 반영
  - `sync-sheets --target staging`: 성공
  - `sync-sheets --target master`: 성공

현재 상태:
- `approved 21` 기업 기준 `모집단 1차 수집`은 실제로 완료됐다.
- 이제 `master`에는 84건의 첫 스냅샷이 있다.
- 다음 운영의 기본선은 `update-incremental`이다.
- 다만 대표성 개선을 위해 `candidate 768 -> approved` 전환 작업은 병렬적으로 계속 필요하다.

## 작업 단위 7

이름: `운영 루프 오케스트레이션 및 자동화 점검`

목적:
- 반복 실행 시 `기업근거 재수집 -> 승인기업 재선별 -> 공식소스 재탐색 -> 검증 -> 모집단 수집/증분 -> 승격`이 한 사이클로 도는지 확인한다.
- 자동화 전환 가능 범위를 냉정하게 판정한다.

주요 코드:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/pipelines.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/cli.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py`

실행 명령:

```bash
python -m jobs_market_v2.cli run-collection-cycle
python -m jobs_market_v2.cli doctor
pytest -q
```

검증 결과:
- `pytest --collect-only -q`
  - `58` tests
- `pytest -q`
  - 전체 통과
- `doctor`
  - 통과
- `run-collection-cycle`
  - `run_mode incremental`
  - `expanded_candidate_company_count 2224`
  - `approved 27`
  - `verified source success 30`
  - `staging/master 102`
  - `quality_gate_passed true`
  - `시트동기화 true`
  - `real 281.34s`

판정:
- 운영 자동화는 가능하다.
- `run-collection-cycle` 내부 sync 포함 경로도 단독 실행 기준 성공을 확인했다.
- `run-collection-cycle` 하나로 후보군 재확장, staged reevaluation, 수집, 승격, 시트동기화까지 닫힌다.
- 다만 완전 자동 확장 루프가 되려면 `company_seed_sources.yaml`에 없는 새 공식 출처를 자동 발견하는 단계가 더 필요하다.
- 따라서 현재 완성된 것은 `등록된 공식 출처 범위 안의 반복 자동화`이고, 미완성인 것은 `새 공식 출처까지 스스로 넓히는 자동화`다.

## 작업 단위 8

이름: `seed source shadow 분리`

목적:
- 새 공식 출처 자동 발견 결과를 운영 후보군과 분리한다.
- 자동 발견은 계속 하되, 승격 전까지 본 후보군 확장을 오염시키지 않게 한다.

주요 코드:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/company_seed_sources.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/pipelines.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/cli.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/settings.py`

실행 명령:

```bash
python -m jobs_market_v2.cli discover-company-seed-sources
python -m jobs_market_v2.cli promote-shadow-seed-sources
```

검증 결과:
- `pytest -q`
  - 전체 통과
- `doctor`
  - 통과
- `discover-company-seed-sources`
  - `target shadow`
  - `catalog_source_count 2`
  - `discovered_seed_source_count 1`

판정:
- 새 공식 출처 자동 발견은 이제 기본적으로 shadow에 저장된다.
- shadow 결과는 `promote-shadow-seed-sources` 전까지 본선 후보군에 자동 합류하지 않는다.
- 같은 첨부파일이 query string만 바뀐 경우도 dedupe된다.
- 따라서 안전하게 발견을 계속하고, 검토 후 승격하는 운영이 가능해졌다.

## 작업 단위 9

이름: `shadow seed source 자동 평가 및 본선 자동 승격`

목적:
- 새로 발견된 shadow seed source를 같은 수집 실행 안에서 자동 평가한다.
- 실제 회사 seed record를 만들 수 있는 source만 본선 discovered seed source로 자동 승격한다.
- 중복 source는 자동 제거하고, 파싱 불가 source는 shadow에 남겨 운영 루프를 오염시키지 않게 한다.

주요 코드:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/company_seed_sources.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py`

실행 명령:

```bash
python -m jobs_market_v2.cli discover-company-seed-sources
python -m jobs_market_v2.cli run-collection-cycle
python -m jobs_market_v2.cli doctor
pytest -q
```

검증 결과:
- `pytest -q`
  - 전체 통과
- `doctor`
  - 통과
- `run-collection-cycle`
  - 성공
  - `candidate_expansion.auto_promoted_shadow_seed_source_count 0`
  - `candidate_expansion.duplicate_shadow_seed_source_count 1`
  - `candidate_expansion.remaining_shadow_seed_source_count 0`
  - `approved 27`
  - `staging/master 102`
  - `시트동기화 true`

판정:
- shadow는 이제 단순 보관함이 아니라 자동 평가 대상이다.
- 유효 source는 수동 개입 없이 같은 cycle 안에서 본선에 승격된다.
- 중복 source는 자동으로 소거된다.
- 파싱 불가 source는 shadow에 남아 다음 검토 대상으로 유지된다.
- 따라서 `등록된 공식 카탈로그 범위 안의 자동 확장 운영`은 한 단계 더 완결됐다.

## 작업 단위 10

이름: `중첩 공식 카탈로그 재귀 발견`

목적:
- 공식 카탈로그 안에서 다시 카탈로그 성격의 상세 페이지가 나오면 한 번 더 따라가도록 만든다.
- 등록된 공식 카탈로그 범위 안에서 `카탈로그 -> 카탈로그 -> 실제 seed source` 경로를 자동으로 닫는다.

주요 코드:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/company_seed_sources.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/settings.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py`

실행 명령:

```bash
python -m jobs_market_v2.cli collect-company-seed-records
python -m jobs_market_v2.cli expand-company-candidates
python -m jobs_market_v2.cli doctor
pytest -q
```

검증 결과:
- `pytest -q`
  - 전체 통과
- `doctor`
  - 통과
- `collect-company-seed-records`
  - `newly_discovered_seed_source_count 2`
  - `duplicate_shadow_seed_source_count 2`
  - `shadow_seed_source_count 0`
  - `collected_company_count 2226`
- `expand-company-candidates`
  - `expanded_candidate_company_count 2224`

판정:
- 등록된 공식 카탈로그 범위 안에서는 이제 중첩 카탈로그까지 자동 추적한다.
- 남은 미완성은 `company_seed_sources.yaml`에 없는 완전히 새로운 최상위 공식 카탈로그를 웹에서 스스로 추가하는 단계다.

## 작업 단위 11

이름: `공공·지원기관 host 기반 top-level catalog 자동 발견`

목적:
- `company_seed_sources.yaml`에 직접 적지 않은 도메인 중에서도
- 공공·지원기관 성격이 강한 공식 도메인을 골라
- top-level catalog 후보를 자동으로 shadow discovery에 넣는다.

주요 코드:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/company_seed_sources.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/settings.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py`

실행 명령:

```bash
python -m jobs_market_v2.cli collect-company-seed-records
python -m jobs_market_v2.cli doctor
pytest -q
```

검증 결과:
- `pytest -q`
  - 전체 통과
- `doctor`
  - 통과
- `collect-company-seed-records`
  - 성공
  - `catalog_source_count 25`
  - `discovered_seed_source_count 15`
  - `newly_discovered_seed_source_count 30`
  - `shadow_seed_source_count 0`
  - `invalid_shadow_seed_source_count 14`

판정:
- 등록된 카탈로그 URL만이 아니라 공공·지원기관 공식 도메인까지 자동 탐색 범위에 들어왔다.
- 다만 웹 전체 무제한 탐색이 아니라, 이미 확보된 기업/기관 universe 안에서 카탈로그 host 가능성이 높은 도메인을 고르는 방식이다.

## 작업 단위 12

이름: `catalog host 실패 격리 및 invalid shadow 정리`

목적:
- 느린 host 도메인 timeout이 전체 `collect-company-seed-records`를 죽이지 않게 한다.
- 자동 발견 후 invalid로 판정된 source가 shadow에 계속 남아 다음 실행을 오염시키지 않게 한다.

주요 코드:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/company_seed_sources.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py`

실행 명령:

```bash
python -m jobs_market_v2.cli collect-company-seed-records
pytest -q
```

검증 결과:
- `pytest -q`
  - 전체 통과
- `collect-company-seed-records`
  - 성공
  - `shadow_seed_source_count 0`
  - `remaining_shadow_seed_source_count 0`
  - `invalid_shadow_seed_source_count 14`

판정:
- 한 host 도메인 실패가 전체 자동 확장을 멈추지 않는다.
- invalid source는 summary에만 남고 shadow backlog로는 누적되지 않는다.
- 따라서 자동 발견을 붙여도 운영 루프가 계속 닫힌다.

## 작업 단위 13

이름: `invalid seed source cache 고정`

목적:
- 한 번 invalid로 판정된 seed source를 다음 실행에서 다시 재발견/재실패하지 않게 한다.
- 반복 실행 시 invalid source는 cache에서 바로 걸러지고 duplicate로 정리되게 만든다.

주요 코드:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/company_seed_sources.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/settings.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py`

실행 명령:

```bash
pytest -q tests/test_jobs_market_v2.py -k 'invalid_shadow_seed_sources_are_cached or unparsable_discovered_sources'
pytest -q
python -m jobs_market_v2.cli collect-company-seed-records
```

검증 결과:
- `pytest -q tests/test_jobs_market_v2.py -k 'invalid_shadow_seed_sources_are_cached or unparsable_discovered_sources'`
  - 통과
- `pytest -q`
  - 전체 통과
- `collect-company-seed-records`
  - 성공
  - 첫 실행: `invalid_shadow_seed_source_count 15`
  - 직후 재실행: `duplicate_shadow_seed_source_count 17`, `invalid_shadow_seed_source_count 0`

판정:
- invalid seed source는 이제 별도 cache 파일에 저장된다.
- 같은 bad source는 다음 실행에서 다시 invalid로 소모되지 않는다.
- 자동 확장 반복 루프의 노이즈와 재실패 비용이 줄었다.

## 작업 단위 14

이름: `full cycle long-tail 안정화 및 one-line 자동 운영 복구`

목적:
- `run-collection-cycle`이 내부 sync까지 포함한 단일 명령으로 다시 안정적으로 종료되게 만든다.
- source-type별 long-tail 네트워크 구간이 full cycle 전체를 과도하게 끌지 않도록 줄인다.
- full cycle 안에서 source verification과 collection이 중복 fetch를 하지 않게 정리한다.

주요 코드:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/settings.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/collection.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/pipelines.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py`

실행 명령:

```bash
pytest -q tests/test_jobs_market_v2.py -k 'source_timeout_values_prefer_source_specific_settings or fetch_source_content_uses_source_specific_timeout or run_collection_cycle_pipeline_bootstrap_and_incremental'
pytest -q
python -m jobs_market_v2.cli doctor
python -m jobs_market_v2.cli run-collection-cycle
```

검증 결과:
- `pytest -q tests/test_jobs_market_v2.py -k 'source_timeout_values_prefer_source_specific_settings or fetch_source_content_uses_source_specific_timeout or run_collection_cycle_pipeline_bootstrap_and_incremental'`
  - 통과
- `pytest -q`
  - 전체 통과
- `doctor`
  - 통과
- `run-collection-cycle`
  - 성공
  - `automation_ready true`
  - `verified_source_success_count 30`
  - `staging/master 102`
  - `sync.staging.google_sheets_synced true`
  - `sync.master.google_sheets_synced true`
  - `company_evidence.start_offset 800`
  - `company_evidence.next_offset 1200`

판정:
- `run-collection-cycle --skip-sync`만 안정적이던 상태에서, 내부 sync까지 포함한 단일 명령도 다시 복구됐다.
- 등록된 공식 카탈로그 + 확보한 공공·지원기관 host universe 범위 안에서는 이제 one-line 자동 운영이 가능하다.
- 아직 남은 미완성은 웹 전체에서 완전히 새로운 최상위 공식 카탈로그 도메인을 스스로 발굴하는 단계다.

## 작업 단위 15

이름: `evidence 기반 trusted external host 확장`

목적:
- 회사 행에 직접 잡히지 않은 공식 도메인도 `company_evidence`에서 끌어와 top-level catalog host 후보로 쓴다.
- `wiset.or.kr` 같은 trusted external 공식 도메인을 새 최상위 catalog 후보로 자동 편입한다.
- 확장 뒤에도 one-line full cycle이 그대로 닫히는지 다시 확인한다.

주요 코드:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/company_seed_sources.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/docs/HANDOFF.md`

실행 명령:

```bash
pytest -q
python -m jobs_market_v2.cli doctor
python -m jobs_market_v2.cli collect-company-seed-records
python -m jobs_market_v2.cli run-collection-cycle
```

검증 결과:
- `pytest -q`
  - 전체 통과
- `doctor`
  - 통과
- `collect-company-seed-records`
  - 성공
  - `catalog_source_count 29`
  - `newly_discovered_seed_source_count 3`
  - `invalid_shadow_seed_source_count 1`
- `run-collection-cycle`
  - 성공
  - `automation_ready true`
  - `catalog_source_count 29`
  - `verified_source_success_count 30`
  - `staging/master 102`
  - `sync.staging.google_sheets_synced true`
  - `sync.master.google_sheets_synced true`

판정:
- 자동 top-level catalog host 후보가 이제 회사 registry뿐 아니라 evidence에 이미 등장한 trusted external 공식 도메인까지 확장됐다.
- 등록된 공식 카탈로그 universe 안의 자동 운영 범위가 한 단계 더 넓어졌다.
- 아직 남은 마지막 미완성은 웹 전체에서 완전히 새로운 최상위 공식 카탈로그 도메인을 스스로 발굴하는 단계다.

## 작업 단위 16

이름: `candidate 요약 기반 bare .kr 공공기관 host 흡수`

목적:
- `company_candidates`에 이미 찍혀 있는 공공기관 공식 도메인과 primary evidence를 top-level catalog host 후보로 직접 쓴다.
- `.or.kr/.re.kr`뿐 아니라 `kca.kr` 같은 bare `.kr` 공공기관 host도 자동 발견 범위에 포함한다.
- 이 확장 후에도 one-line full cycle이 그대로 닫히는지 다시 확인한다.

주요 코드:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/company_seed_sources.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/docs/HANDOFF.md`

실행 명령:

```bash
pytest -q
python -m jobs_market_v2.cli collect-company-seed-records
python -m jobs_market_v2.cli run-collection-cycle
```

검증 결과:
- `pytest -q`
  - 전체 통과
- `collect-company-seed-records`
  - 성공
  - `catalog_source_count 29`
  - `newly_discovered_seed_source_count 2`
  - `invalid_shadow_seed_source_count 0`
- `run-collection-cycle`
  - 성공
  - `automation_ready true`
  - `catalog_source_count 29`
  - `company_evidence.start_offset 1600`
  - `company_evidence.next_offset 2000`
  - `staging/master 102`
  - `sync.staging.google_sheets_synced true`
  - `sync.master.google_sheets_synced true`

판정:
- 자동 top-level catalog host 후보가 이제 `companies_registry`, `company_evidence`뿐 아니라 `company_candidates`의 요약 정보까지 포함한다.
- `kca.kr` 같은 bare `.kr` 공공기관 도메인도 자동 확장 범위에 들어왔다.
- 등록된 공식 출처 universe 안의 자동 운영 범위가 한 단계 더 넓어졌고, one-line full cycle도 계속 유지된다.

## Work Unit: Search/Skip/Refresh/Batch Hardening

목적:
- top-level catalog 자동 발견을 더 넓히되, 반복 실행이 느린 remote SSL long-tail에 매번 발목 잡히지 않게 한다.
- 웹검색 기반 신규 top-level catalog host 발견을 `.kr` 공공기관까지 제한적으로 확장한다.
- timeout host 재시도, 최근 성공 source 재탐색, catalog 전량 스캔을 줄인다.

주요 코드:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/company_seed_sources.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/settings.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py`

핵심 반영:
- 웹검색 기반 top-level catalog discovery에 bare `.kr` 공공기관 host 허용
- query signature 변경 시 search cooldown 우회
- timeout catalog host skip cache 추가
- 최근 성공 catalog source refresh cache 추가
- catalog source batch cursor 추가

검증 결과:
- 타깃 pytest:
  - `public_bare_kr_catalog_host`
  - `query_signature_changes`
  - `caches_timed_out_catalog_host_sources`
  - `skips_recently_refreshed_catalog_sources`
  - `processes_catalog_sources_in_batches`
  - 모두 통과

판정:
- 자동 확장의 안전장치와 bounded runtime은 이전보다 확실히 좋아졌다.
- 다만 live `collect-company-seed-records`와 full `pytest -q`는 여전히 원격 SSL long-tail 때문에 세션 종료가 늦게 관찰될 수 있다.
- 다음 continue에서는 이 long-tail을 더 줄이거나, live run 결과를 끝까지 수거하는 쪽으로 이어가면 된다.

## Work Unit: HTML Catalog Child-Source Hardening

목적:
- `html_link_catalog_url`에서 파생되는 noisy child source를 더 일찍 차단한다.
- generic external institution root, `view.do`, `fileDownload.do`류가 shadow/invalid backlog를 불필요하게 키우지 않게 한다.

주요 코드:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/company_seed_sources.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py`

핵심 반영:
- external host candidate는 이제 include/exclude keyword를 통과할 때만 승격
- `fileDownload.do`, `download.do`, `view.do`를 catalog candidate에서 차단
- `html_table_url` 추론 링크가 실제 table도, attachment도, catalog도 아니면 fallback append 금지
- `open.go.kr`, `epeople.go.kr`, `epost.go.kr`, `weather.go.kr`, `history.go.kr`, `129.go.kr`, `egov.go.kr`는 public-service host blocklist로 trusted external catalog 후보에서 제외

## 2026-04-01 서비스 품질/증분 재검증

- 서브에이전트 감사 결과를 반영해 아래를 수정함:
  - partial evidence scan publish 차단
  - cached seed refresh 시 stale candidate expansion plateau 제거
  - alias 회사명 기반 `job_url` 중복 제거
  - impossible experience (`경력 2026년+`) 제거
  - `직무초점_표시`, `직무초점근거_표시` 추가
  - raw detail heading 기반 section backfill
  - generic-only core skill 제거
- 최신 완료 cycle:
  - `run-collection-cycle-20260401015153`
  - `staging/master 89`
  - `quality_gate_passed = true`
  - `company_evidence completed_full_scan = true`
- 최신 서비스 지표:
  - `주요업무_표시` 공란 `3`
  - `자격요건_표시` 공란 `13`
  - `우대사항_표시` 공란 `22`
  - `직무초점_표시` 공란 `9`
  - `구분요약_표시` 공란 `0`
- 관련 테스트 기대값도 “invalid shadow로 남는다”에서 “애초에 발견되지 않는다” 기준으로 갱신

검증 결과:
- generic external public root 회귀 통과
- generic external institution root without catalog keywords 회귀 통과
- detail/download URL 회귀 통과
- 추가 `pytest -q --maxfail=1 -x` 재실행에서 실패 0 확인

판정:
- HTML catalog 내부의 과도한 외부 링크 승격은 이전보다 확실히 줄었다.
- 남은 병목은 이 child-source 노이즈보다 remote SSL/network long-tail 쪽이다.

## Work Unit: Shadow/Invalid Cache Compaction

목적:
- search/shadow universe가 커져도 runtime 파일이 무한히 불어나지 않게 한다.
- 오래된 shadow/invalid seed source를 자동으로 정리하고, shadow backlog에 상한을 둔다.

주요 코드:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/constants.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/settings.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/company_seed_sources.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py`

핵심 반영:
- seed source row에 `first_seen_at`, `last_seen_at` 추가
- invalid cache row에 `invalidated_at` 추가
- shadow/invalid cache에 retention 적용
- shadow/invalid cache에 max-row cap 적용
- `collect-company-seed-records` 시작 전에 cache compaction 수행

검증 결과:
- 타깃 pytest 4개 통과
  - stale shadow prune
  - stale invalid prune
  - shadow backlog cap
  - explicit cache compaction
- `pytest -q` 전체 통과
- live compaction 결과:
  - `company_seed_sources_shadow.csv: 12028 -> 5000`
  - `company_seed_sources_invalid.csv: 2424 -> 2424`
- compaction 이후 one-line full cycle 성공:
  - `run-collection-cycle-20260331180255`
  - `automation_ready true`
  - `approved 27`
  - `candidate 2087`
  - `staging/master 102`
  - `sync.staging/master true`

판정:
- bounded automation은 유지되면서 shadow backlog 제어까지 들어갔다.
- 현재 확보한 공식 출처 universe 안의 자동 운영은 더 안정해졌다.

## Work Unit: Cached Seed-Record Refresh Continuity

목적:
- 최근 `company_seed_records_collected.csv`를 재사용할 때도 top-level catalog discovery가 멈추지 않게 한다.
- cached branch에서도 search/query cursor와 shadow refresh가 계속 전진하도록 만든다.

주요 코드:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/company_seed_sources.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/pipelines.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py`

핵심 반영:
- `refresh_company_seed_sources()` 추가
- cached `collect-company-seed-records` 경로에서도
  - cache compaction
  - shadow discovery
  - auto-promotion
  을 계속 수행
- cached summary에도 search/catalog refresh 상태를 기록

검증 결과:
- 타깃 pytest 통과
  - cached records reuse
  - compaction
  - stale shadow prune
  - stale invalid prune
  - shadow cap
  - search/cursor 관련 기존 회귀
- `doctor` 통과

판정:
- 이제 one-line cycle이 recent seed record cache를 써도 top-level catalog discovery가 멈추지 않는다.

## 작업 단위: Seed Source Summary / Dedupe Cleanup

목적:
- cached branch summary에서 catalog source와 실제 수집 source를 분리해 숫자 해석을 바로잡는다.
- 동일 URL이 source 이름만 달라 shadow/invalid에 중복 적재되는 문제를 줄인다.
- noisy anchor text가 source name에 계속 누적되는 현상을 완화한다.

주요 코드:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/company_seed_sources.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/pipelines.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py`

핵심 반영:
- cached summary의 `seed_source_count`를 non-catalog collectable source 기준으로 변경
- seed source dedupe key를 `source_type + normalized source_url/local_path` 중심으로 변경
- overly noisy parent `source_name`과 link label을 잘라 새 discovered row 이름이 체인처럼 길어지지 않게 보정

검증 결과:
- 타깃 pytest 통과
  - cached records reuse summary
  - noisy label normalization
  - same-url different-name dedupe
- live `collect-company-seed-records` 확인
  - `seed_source_count 6`
  - `approved_seed_source_count 6`
  - `discovered_seed_source_count 689`
  - `invalid_shadow_seed_source_count 0`

판정:
- seed source summary 해석이 이전보다 훨씬 명확해졌고,
- shadow/invalid 중복 증식도 한 단계 더 억제됐다.

## 작업 단위: Fresh Full-Cycle Revalidation After Summary Cleanup

목적:
- seed source summary cleanup 이후에도 one-line 자동 운영이 깨지지 않았는지 다시 확인한다.

주요 실행:
- `python -m jobs_market_v2.cli run-collection-cycle`

검증 결과:
- fresh full cycle 성공
  - `run-collection-cycle-20260331184613`
  - `automation_ready true`
  - `seed_source_count 6`
  - `approved 27`
  - `candidate 2087`
  - `staging/master 102`
  - `sync-sheets staging/master` 성공

판정:
- summary cleanup 이후에도 bounded automation은 유지된다.

## 작업 단위: Full Pytest + Fresh Full-Cycle Production Revalidation

목적:
- 최신 seed-source cleanup 이후에도 전체 테스트와 one-line 자동 운영이 실제로 다시 닫히는지 확인한다.
- `production-final` 판단의 근거를 최신 코드 기준으로 다시 확보한다.

주요 실행:
- `pytest -vv --maxfail=1 -x`
- `python -m jobs_market_v2.cli doctor`
- `python -m jobs_market_v2.cli run-collection-cycle`

검증 결과:
- 전체 pytest 통과
  - `99 passed in 274.46s (0:04:34)`
- `doctor` 통과
- fresh full cycle 성공
  - `run-collection-cycle-20260331200341`
  - `automation_ready true`
  - `approved 27`
  - `candidate 2087`
  - `staging/master 102`
  - `sync-sheets staging/master` 성공

판정:
- 최신 코드 기준으로 `full pytest`, `doctor`, `fresh one-line full cycle`이 모두 다시 닫혔다.
- 따라서 `현재 확보한 공식 출처 universe 안의 bounded production 운영`은 배포 가능 상태로 본다.
- 남은 미완성은 `웹 전체 신규 최상위 공식 카탈로그 자동 발굴`이며, 이는 현재 bounded production 운영의 blocker가 아니라 장기 개선 축이다.

## 작업 단위: Incrementality Revalidation + Dataset Quality Hardening

목적:
- 증분이 실제로 전진하는지 다시 확인한다.
- 서비스 수준을 해치는 본문/경력/구분/중복 품질 문제를 다시 깎는다.
- 느린 seed-source/shadow source 때문에 one-line cycle이 멈추지 않도록 hard-stop 구조를 넣는다.

주요 변경:
- `company_seed_sources.py`
  - fetch를 wall-clock deadline 기반 streaming으로 변경
  - `auto_promote_shadow_company_seed_sources`에 runtime budget 추가
- `presentation.py`
  - `경력수준_표시`, `채용트랙_표시`, `구분요약_표시`, `직무초점_표시` 보강
  - 한국어/영어 heading 기반 `주요업무/자격요건/우대사항` 회수 강화
- `company_screening.py`, `pipelines.py`
  - partial evidence scan이 published 상태를 흔들지 않도록 유지
- `collection.py`
  - alias 회사명으로 인한 동일 `job_url` 중복 제거

검증 결과:
- fresh full cycle 성공
  - `run-collection-cycle-20260401022611`
  - `automation_ready true`
  - `approved 24`
  - `candidate 2090`
  - `staging/master 89`
  - `quality_gate_passed true`
  - `sync-sheets staging/master` 성공
- 전체 pytest 통과
  - `pytest -q --maxfail=1 -x`
- master 품질 지표
  - rows `89`
  - `job_key/job_url` 중복 `0`
  - `상세본문_분석용` blank `0`
  - `주요업무_표시` blank `3`
  - `자격요건_표시` blank `3`
  - `우대사항_표시` blank `14`
  - `채용트랙_표시` blank `63`
  - `직무초점_표시` blank `9`
  - `구분요약_표시` blank `0`

판정:
- 증분은 `bounded incremental`로 계속 전진한다.
- 현재 약점은 구조 붕괴가 아니라 `채용트랙`과 일부 `우대사항` recall이다.
- 현재 확보한 공식 출처 universe 안에서는 서비스 가능한 산출 체계로 판단한다.

## 작업 단위: Growth Engine Reframing

목적:
- published growth가 약한 이유를 감이 아니라 수치로 설명한다.
- `후보군 부족`이 아니라 `approved/source/job 번역률 부족`이라는 병목을 명확히 한다.

핵심 수치:
- published 상태
  - `approved 24`
  - `candidate 2090`
  - `master/staging 89`
- 성장 잠재력
  - `candidate_verified_no_active = 278`
  - `source_ready_candidate_count = 102`

해석:
- 공식성/검증소스가 이미 갖춰졌지만 `active_job_count = 0`이라 approved로 못 올라가는 회사가 많다.
- 즉 지금의 핵심 과제는 후보군 확대보다 `source-ready -> approved -> published` 번역률을 높이는 것이다.

운영 3트랙:
- 성장 트랙: approved/source 확대
- 품질 트랙: track/requirements/preferred/focus recall 강화
- 운영성 트랙: runtime budget/cursor/partial guard로 반복 종료 보장

## 작업 단위: 99점 품질 점수 체계 도입

목적:
- `서비스 가능 수준`을 주관적 표현이 아니라 수치 기준으로 관리한다.
- `99점 이상`을 배포형 품질 기준으로 정의한다.

주요 변경:
- [quality.py](/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/quality.py)
  - `quality_score_100`, `quality_score_target` 도입
  - `hiring_track_cue_blank_ratio`, `hiring_track_cue_blank_count`, `hiring_track_cue_total` 추가
  - blank ratio/중복/영문누수/비정상경력/source success를 score breakdown으로 계산
- [presentation.py](/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/presentation.py)
  - `채용트랙_표시`가 `자격요건/우대사항/상세본문`의 학위/전문연구요원 신호까지 읽도록 확장
  - raw text heading recovery가 엇갈릴 때 main/requirement/preferred를 raw heading 추출로 보정
- [tests/test_jobs_market_v2.py](/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py)
  - quality score/track cue/degrees inference 관련 회귀 추가
  - stricter bootstrap/staging semantics에 맞게 일부 기대값 갱신

검증 결과:
- 타깃 quality/display pytest 통과
- `doctor` 통과
- live full cycle 성공
  - `run-collection-cycle-20260401035141`
  - `automation_ready true`
  - `quality_gate_passed true`
  - `staging/master 89`
  - `changed_job_count 3`
  - `missing_job_count 1`
  - sheets sync 성공
- latest runtime score
  - `quality_score_100 = 99.6`
  - `quality_score_target = 99.0`
  - `hiring_track_cue_blank_ratio = 0.0`

판정:
- `99점 이상` 기준은 현재 runtime에서 충족한다.
- 남은 주 과제는 score 유지보다 growth translation 강화다.

## 작업 단위 15

이름: `Partial Incremental Hold-State Hardening`

목적:
- partial source scan 중에도 기존 published-safe row를 unsafe하게 잃지 않는다.
- `quality_score_100 >= 99`를 유지하면서 published growth를 다시 증가시킨다.

주요 코드:
- [collection.py](/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/collection.py)
- [pipelines.py](/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/pipelines.py)
- [quality.py](/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/quality.py)
- [company_screening.py](/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/company_screening.py)
- [macos_ocr.py](/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/macos_ocr.py)
- [macos_ocr.swift](/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/macos_ocr.swift)
- [tests/test_jobs_market_v2.py](/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py)

실행 명령:

```bash
./scripts/setup_env.sh
./scripts/register_kernel.sh
pytest -q
python -m jobs_market_v2.cli doctor
python -m jobs_market_v2.cli run-collection-cycle
jupyter nbconvert --to notebook --execute runtime/notebook_smoke/00_source_screening.smoke.ipynb --output 00_source_screening.executed.ipynb --output-dir runtime/notebook_smoke_exec --ExecutePreprocessor.timeout=600
jupyter nbconvert --to notebook --execute runtime/notebook_smoke/01_bootstrap_population.smoke.ipynb --output 01_bootstrap_population.executed.ipynb --output-dir runtime/notebook_smoke_exec --ExecutePreprocessor.timeout=600
```

before_metrics:
- 기준 safe cycle: `run-collection-cycle-20260401035056`
- `quality_score_100 = 99.60`
- `master_rows = 89`
- `active_jobs = 88`
- `approved_company_count = 24`
- `run_level_verified_source_success_count = 27`

after_metrics:
- 기준 safe cycle: `run-collection-cycle-20260401064044`
- `quality_score_100 = 99.62`
- `master_rows = 91`
- `active_jobs = 90`
- `approved_company_count = 69`
- `source_registry verification_status=성공 = 117`
- `net_job_delta = +2`
- `net_active_job_delta = +2`

kept_changes:
- recruiter OCR는 published collection/update 경로에서만 켜고 company evidence/mock source에서는 끄도록 제한했다.
- incremental merge는 `이번 run에 실제 성공 처리된 source_url`만 missing 판정에 사용한다.
- substantive `검증실패보류` row는 low-quality filter에서 유지되도록 바꿨다.
- non-search integration test에서 search refresh를 stub 처리해 pytest full run이 deterministic하게 끝나도록 했다.

reverted_changes:
- `run-collection-cycle-20260401062808` 결과는 폐기했다.
- 해당 run은 `staging_job_count = 16`, `active_jobs = 16`, `dropped_low_quality_job_count = 75`, `net_job_delta = -73`으로 unsafe 했다.
- `runtime/snapshots/update-incremental-20260401035255.parquet`를 last known safe state로 복구한 뒤 다시 safe cycle을 돌렸다.
- notebook smoke가 `staging/quality_gate/coverage/source_collection_progress`를 바꾼 뒤에는 published-safe 상태로 재정렬했다.

last_known_safe_state:
- snapshot: `runtime/snapshots/update-incremental-20260401035255.parquet`
- baseline cycle: `run-collection-cycle-20260401035056`

last_successful_run_id:
- `run-collection-cycle-20260401064044`

resume_next_step:
- `runtime/source_collection_progress.json`은 safe cycle 기준 `next_source_offset = 0`으로 복구해 두었다.
- 다음 run은 `69 approved / 119 screened sources` 상태에서 다시 incremental cycle을 시작하면 된다.
- 다음 핵심 작업은 `company_evidence.next_offset 800 -> full scan 완료`다.

failures:
- partial source scan hold row가 low-quality drop으로 사라지는 safety incident가 있었다.
- notebook smoke는 통과했지만 runtime staging과 cursor를 바꾸는 부수효과가 있었다.

next priorities:
- `published_company_state=true`가 될 때까지 company evidence full scan completion을 밀어라.
- partial company-state reuse에서 approved/source registry 팽창이 published growth를 과대대표하지 않도록 guard를 더 세워라.
- catalog discovery timeout / invalid shadow seed churn을 줄여 refresh noise를 낮춰라.

검증 결과:
- targeted pytest 통과
  - `pytest -q tests/test_jobs_market_v2.py -k 'processed_verified_source_urls_only_include_current_run_successes or incremental_update_pipeline_marks_changed_and_missing or run_collection_cycle_pipeline_bootstrap_resume_until_full_scan or run_collection_cycle_pipeline_skips_collection_when_published_state_is_not_ready'`
  - `pytest -q tests/test_jobs_market_v2.py -k 'discover_company_seed_sources_processes_catalog_sources_in_batches or discover_company_seed_sources_respects_runtime_budget or processed_verified_source_urls_only_include_current_run_successes'`
  - `pytest -q tests/test_jobs_market_v2.py -k 'run_collection_cycle_pipeline_bootstrap_and_incremental or run_collection_cycle_pipeline_bootstrap_resume_until_full_scan or run_collection_cycle_pipeline_skips_collection_when_published_state_is_not_ready'`
  - `pytest -q tests/test_jobs_market_v2.py -k 'filter_low_quality_jobs_drops_empty_or_noisy_analysis_rows or incremental_update_pipeline_marks_changed_and_missing or run_collection_cycle_pipeline_bootstrap_and_incremental'`
- 전체 pytest 통과
  - `pytest -q` (`139 tests`)
- `doctor` 통과
- safe live cycle 성공
  - `run-collection-cycle-20260401064044`
  - `quality_gate_passed true`
  - `promoted_job_count 91`
  - `baseline 89 -> master 91`
  - `active 88 -> 90`
  - `held_job_count 75`
  - `net_job_delta +2`
  - `net_active_job_delta +2`
- notebook smoke 2개 통과
  - `runtime/notebook_smoke/00_source_screening.smoke.ipynb`
  - `runtime/notebook_smoke/01_bootstrap_population.smoke.ipynb`

판정:
- 이번 단위는 `growth 성공 + quality 유지`로 채택한다.
- published growth는 `master_rows 89 -> 91`, `approved_company_count 24 -> 69`로 개선됐다.
- 남은 수동 작업은 `.env` 실제 값 입력과 실제 notebook 본 실행뿐이다.

## 작업 단위 16

이름: `Growth 운영 복구와 full scan 마감`

목적:
- 코드 변경 없이도 현재 병목이 `company_evidence full scan 미완료`인지 확인한다.
- `quality_score_100 >= 99`를 유지한 채 published growth를 `approved/source` 축에서 더 밀어 올린다.
- notebook smoke가 runtime을 흔들더라도 handoff 전에 published-safe 상태를 복구한다.

주요 코드:
- 없음
- 이번 단위는 repository 코드 변경 없이 runtime state만 갱신했다.

실행 명령:

```bash
./scripts/setup_env.sh
./scripts/register_kernel.sh
python -m jobs_market_v2.cli collect-company-evidence --batch-size 200 --max-batches 5
python -m jobs_market_v2.cli screen-companies
pytest -q
python -m jobs_market_v2.cli doctor
python -m jobs_market_v2.cli run-collection-cycle
jupyter nbconvert --to notebook --execute runtime/notebook_smoke/00_source_screening.smoke.ipynb --output 00_source_screening.executed.ipynb --output-dir runtime/notebook_smoke_exec --ExecutePreprocessor.timeout=600
jupyter nbconvert --to notebook --execute runtime/notebook_smoke/01_bootstrap_population.smoke.ipynb --output 01_bootstrap_population.executed.ipynb --output-dir runtime/notebook_smoke_exec --ExecutePreprocessor.timeout=600
python -m jobs_market_v2.cli run-collection-cycle
python -m jobs_market_v2.cli collect-company-evidence --batch-size 200 --max-batches 5
python -m jobs_market_v2.cli screen-companies
```

before_metrics:
- 기준 safe runtime: `2026-04-01 08:03 KST`
- `quality_score_100 = 99.62`
- `master_rows = 91`
- `active_jobs = 90`
- `approved_company_count = 69`
- `source_registry verification_status=성공 = 117`
- `screened_source_count = 119`
- `company_evidence.next_offset = 800`
- `published_company_state = false`

after_metrics:
- 기준 safe cycle: `run-collection-cycle-20260401082342`
- final company/source refresh:
  - `collect-company-evidence-20260401082724`
  - `screen-companies-20260401083004`
- `quality_score_100 = 99.62`
- `master_rows = 91`
- `active_jobs = 90`
- `approved_company_count = 138`
- `source_registry verification_status=성공 = 461`
- `screened_source_count = 516`
- `company_evidence.next_offset = 0`
- `company_evidence.completed_full_scan = true`
- `source_collection_progress.next_source_offset = 0`
- `source_collection_progress.completed_full_scan_count = 5`
- `master_row_delta = 0`
- `approved_company_delta = +69`
- `verified_source_success_delta = +344`

kept_changes:
- repository 코드 변경 없음
- runtime published approved bucket을 `69 -> 138`로 증가시켰다.
- runtime published source registry를 `119 -> 516`, `verification_status=성공 117 -> 461`로 증가시켰다.
- `pytest -q`, `doctor`, `run-collection-cycle`, notebook smoke 2개를 모두 통과시켰다.

reverted_changes:
- notebook smoke 이후 생긴 unsafe runtime 상태는 유지하지 않았다.
- 폐기한 runtime 수치:
  - `quality_score_100 = 98.0`
  - `staging_rows = 14`
  - `active_jobs = 14`
  - `source_registry rows = 278`
- `run-collection-cycle-20260401082342`로 staging/master를 복구한 뒤, full scan 재실행으로 company/source published state도 다시 올렸다.

last_known_safe_state:
- safe cycle: `run-collection-cycle-20260401082342`
- post-smoke refresh:
  - `collect-company-evidence-20260401082724`
  - `screen-companies-20260401083004`

last_successful_run_id:
- `run-collection-cycle-20260401082342`

resume_next_step:
- 다음 시작점은 `approved 138 / screened sources 516 / verified success 461 / master 91`이다.
- `runtime/company_evidence_progress.json`은 `completed_full_scan = true`, `next_offset = 0`이다.
- `runtime/source_collection_progress.json`은 `next_source_offset = 0`, `completed_full_scan_count = 5`다.
- 현재 exact bottleneck은 `approved/source growth -> master row growth` 번역률 부족이다.

failures:
- safe code change는 이번 단위에서 정당화되지 않았다.
- notebook smoke가 내부 pipeline 실행으로 runtime staging/source registry를 덮어쓰는 부수효과를 다시 확인했다.

next priorities:
- `approved 138`과 `verified success 461`을 `master_rows > 91`로 실제 번역하는 수집 우선순위 보정이 필요하다.
- notebook smoke를 production runtime과 격리하거나, 실행 직후 자동 restore 절차를 넣어라.
- `published_company_state=false`에서 매번 partial company scan이 다시 열리는 흐름을 줄여라.

검증 결과:
- 전체 pytest 통과
  - `pytest -q`
- `doctor` 통과
  - `python -m jobs_market_v2.cli doctor`
- safe live cycle 통과
  - `run-collection-cycle-20260401082342`
  - `quality_gate_passed true`
  - `promoted_job_count 91`
  - `quality_score_100 99.62`
- notebook smoke 2개 통과
  - `runtime/notebook_smoke/00_source_screening.smoke.ipynb`
  - `runtime/notebook_smoke/01_bootstrap_population.smoke.ipynb`
- notebook 부수효과 복구 후 final runtime 재확인
  - `master_rows 91`
  - `staging_rows 91`
  - `approved_company_count 138`
  - `source_registry verification_status=성공 461`
  - `quality_score_100 99.62`

판정:
- 이번 단위는 `master 유지 + approved/source growth 성공 + quality 유지`로 채택한다.
- master row count는 늘지 않았지만, 우선순위 2/3 지표인 `approved_company_count`와 `verified_source_success_count`는 크게 증가했다.
- 남은 수동 작업은 `.env` 실제 값 입력과 실제 notebook 본 실행뿐이다.

## 작업 단위 17

이름: `ATS-first source ordering + notebook smoke guard`

목적:
- `approved/source growth -> master row growth` 번역률 부족 병목에 작은 안전 패치로 대응한다.
- collectable source 앞부분을 zero-yield `html_page`가 차지하던 현상을 줄이고, ATS 계열을 먼저 태워 growth를 확보한다.
- `runtime/notebook_smoke/01_bootstrap_population.smoke.ipynb`가 현재 runtime 크기 변화에도 안정적으로 smoke pass 하게 만든다.

주요 코드:
- [collection.py](/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/collection.py)
- [test_jobs_market_v2.py](/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py)
- [01_bootstrap_population.ipynb](/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/notebooks/01_bootstrap_population.ipynb)
- [01_bootstrap_population.smoke.ipynb](/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/runtime/notebook_smoke/01_bootstrap_population.smoke.ipynb)

실행 명령:

```bash
./scripts/setup_env.sh
./scripts/register_kernel.sh
pytest -q tests/test_jobs_market_v2.py -k 'collect_jobs_from_sources_prioritizes_ats_before_html_pages or run_collection_cycle_pipeline_bootstrap_resume_until_full_scan or incremental_update_pipeline_marks_changed_and_missing'
pytest -q
python -m jobs_market_v2.cli doctor
python -m jobs_market_v2.cli run-collection-cycle
jupyter nbconvert --to notebook --execute runtime/notebook_smoke/00_source_screening.smoke.ipynb --output 00_source_screening.executed.ipynb --output-dir runtime/notebook_smoke_exec --ExecutePreprocessor.timeout=600
jupyter nbconvert --to notebook --execute runtime/notebook_smoke/01_bootstrap_population.smoke.ipynb --output 01_bootstrap_population.executed.ipynb --output-dir runtime/notebook_smoke_exec --ExecutePreprocessor.timeout=600
python -m jobs_market_v2.cli collect-company-evidence --batch-size 200 --max-batches 10
python -m jobs_market_v2.cli screen-companies
python -m jobs_market_v2.cli doctor
python -m jobs_market_v2.cli run-collection-cycle
```

before_metrics:
- 기준 safe runtime: `2026-04-01 09:33 KST` 시작 직전
- `quality_score_100 = 99.62`
- `master_rows = 91`
- `active_jobs = 90`
- `approved_company_count = 138`
- `source_registry verification_status=성공 = 461`
- `screened_source_count = 516`

after_metrics:
- growth 확보 cycle:
  - `run-collection-cycle-20260401094017`
  - `master_rows 91 -> 94`
  - `active_jobs 90 -> 93`
  - `source_registry verification_status=성공 461 -> 469`
- final safe cycle:
  - `run-collection-cycle-20260401100806`
  - `quality_score_100 = 99.57`
  - `master_rows = 94`
  - `staging_rows = 94`
  - `active_jobs = 93`
  - `approved_company_count = 138`
  - `source_registry verification_status=성공 = 476`
  - `screened_source_count = 517`
  - `master_row_delta = +3`
  - `active_job_delta = +3`
  - `approved_company_delta = +0`
  - `verified_source_success_delta = +15`

kept_changes:
- collectable source ordering을 `approved 우선 -> greenhouse/lever/greetinghr/recruiter 우선 -> html_page 후순위`로 바꿨다.
- static priority만 사용해 `source_collection_progress` cursor resume 동작을 깨지 않았다.
- ATS-first ordering과 resume 동작을 검증하는 targeted pytest를 추가했다.
- notebook 비교 셀에서 빈 `raw_detail`와 빈 `comparison_view`를 안전하게 처리하도록 guard를 넣었다.
- notebook이 runtime을 오염시킨 뒤에는 `collect-company-evidence-20260401100336` + `screen-companies-20260401100750` + `run-collection-cycle-20260401100806`로 published-safe 상태를 복구했다.

reverted_changes:
- 코드 revert는 없었다.
- notebook smoke가 만든 unsafe runtime 상태는 유지하지 않았다.
- 폐기한 runtime 수치:
  - `quality_score_100 = 76.5`
  - `staging_rows = 0`
  - `source_registry rows = 278`
  - `source_registry verification_status=성공 = 276`

last_known_safe_state:
- post-smoke recovery:
  - `collect-company-evidence-20260401100336`
  - `screen-companies-20260401100750`
- safe cycle:
  - `run-collection-cycle-20260401100806`

last_successful_run_id:
- `run-collection-cycle-20260401100806`

resume_next_step:
- 다음 시작점은 `approved 138 / screened sources 517 / verified success 476 / master 94`다.
- `runtime/source_collection_progress.json`은 `next_source_offset = 392`, `completed_full_scan_count = 5`다.
- `runtime/company_evidence_progress.json`은 `next_offset = 400`, `completed_full_scan = false`다.
- 다음 우선 검증은 ATS-first wave 이후 `cursor 392` 이후 구간에서 어떤 source ranking이 실제 신규 row를 더 주는지다.

failures:
- `runtime/notebook_smoke/01_bootstrap_population.smoke.ipynb` 첫 실행은 `comparison_view.iloc[-4]`에서 `IndexError`로 실패했다.
- 같은 notebook 두 번째 실행은 빈 `raw_detail`에서 `raw_payload_json`을 가정해 `KeyError`로 실패했다.
- notebook이 최종 통과한 뒤에도 production runtime을 덮어써 `quality_score_100 76.5 / staging_rows 0` 상태를 만들었다.

next priorities:
- ATS-first ordering 이후 first-wave growth는 확보했지만, later cursor wave는 `new_job_count = 0`이었다. ATS 내부 second-order ranking을 넣어라.
- notebook smoke isolation 또는 automatic restore를 코드화해 production runtime 오염을 끊어라.
- `published_company_state=false` 재개 구조를 줄여 full-scan company/source state가 반복 cycle에 더 오래 유지되게 하라.

검증 결과:
- targeted pytest 통과
  - `pytest -q tests/test_jobs_market_v2.py -k 'collect_jobs_from_sources_prioritizes_ats_before_html_pages or run_collection_cycle_pipeline_bootstrap_resume_until_full_scan or incremental_update_pipeline_marks_changed_and_missing'`
- 전체 pytest 통과
  - `pytest -q`
- `doctor` 2회 통과
  - `doctor-20260401093955`
  - `doctor-20260401100755`
- live cycle 통과
  - `run-collection-cycle-20260401094017`
  - `run-collection-cycle-20260401100806`
- notebook smoke 통과
  - `runtime/notebook_smoke/00_source_screening.smoke.ipynb`
  - `runtime/notebook_smoke/01_bootstrap_population.smoke.ipynb`
- final safe runtime 재확인
  - `master_rows 94`
  - `staging_rows 94`
  - `quality_score_100 99.57`
  - `approved_company_count 138`
  - `source_registry verification_status=성공 476`

판정:
- 이번 단위는 `growth 성공 + notebook smoke hardening + quality 유지`로 채택한다.
- 우선순위 1 지표인 `master row count`가 `91 -> 94`로 증가했고, 우선순위 3 지표인 `verified_source_success_count`도 `461 -> 476`으로 증가했다.
- 남은 수동 작업은 `.env` 실제 값 입력과 실제 notebook 본 실행뿐이다.

## 2026-04-01 20:30 KST

이번 단위에서 채택한 변경:
- partial company scan 동안에도 `company_candidates_in_progress.csv`, `source_registry_in_progress.csv`를 같은 cycle collection에 연결
- partial collection의 source verification 상태를 `source_registry_in_progress.csv`에 저장
- source scan registry set/policy가 바뀌면 cursor를 다시 상단 priority wave에서 시작하도록 보강

이번 단위 검증:
- targeted pytest 통과
  - `pytest -q tests/test_jobs_market_v2.py -k 'uses_in_progress_registry_during_partial_company_scan or skips_collection_when_published_state_is_not_ready or source_collection_registry_signature_ignores_transient_source_activity or restarts_from_top_when_registry_set_changes'`
- `doctor` 통과
- live cycle 결과
  - before:
    - `published master_rows = 94`
    - `published approved_company_count = 138`
    - `published verified_source_success_count = 476`
    - `working approved_company_count = 141`
    - `working verified_source_success_count = 478`
    - `source_progress_next_offset = 464`
  - after:
    - `master_rows = 95`
    - `staging_rows = 95`
    - `quality_score_100 = 99.58`
    - `active_job_count = 94`
    - `working verified_source_success_count = 505`
    - `source_progress_next_offset = 27`
    - `source_progress_cursor = https://hancom.career.greetinghr.com`

판정:
- 이번 단위는 `partial scan growth translation 성공`으로 채택한다.
- published `master`가 `94 -> 95`로 증가했다.
- 즉 이번 수정은 `approved/source growth가 master로 안 번역되는 구조`를 실제로 한 단계 완화했다.

남은 문제:
- published `company_candidates.csv` / `source_registry.csv`는 full scan 완료 전까지 여전히 보수적으로 유지된다.
- `run-collection-cycle` 최상위 run bookkeeping이 누락되는 경로가 있다. 이번 `20:18` cycle은 core outputs는 갱신됐지만 `runs.csv` 최상위 row가 append되지 않았다.
- `approved/source` 수치 자체가 여전히 `실제 active-job source`보다 넓게 잡혀 있다.
- `실질 중복`과 `표시상 구분성 부족`은 여전히 남아 있다.

다음 우선순위:
- `company_screening._source_summary()`와 `approved` 규칙을 `last_active_job_count > 0` 중심으로 더 현실화할지 검증
- `run_collection_cycle_pipeline()` bookkeeping 누락 원인 확인
- `practical duplicate key` 추가로 URL·구두점 변형형 중복 축소

## 2026-04-01 20:40 KST

이번 단위에서 채택한 변경:
- practical duplicate title stem이 같은 공고들에 `원제목 변형 힌트`를 표시 제목으로 노출
- exact duplicate는 기존처럼 `[공고 id]` suffix 유지

검증:
- targeted pytest 통과
  - `pytest -q tests/test_jobs_market_v2.py -k 'disambiguates_duplicate_display_titles or disambiguates_practical_duplicate_title_stems or uses_in_progress_registry_during_partial_company_scan or source_collection_registry_signature_ignores_transient_source_activity or restarts_from_top_when_registry_set_changes'`
- `doctor` 통과
- `promote-staging` 통과
  - `promoted_job_count = 95`
- `sync-sheets staging/master` 성공

판정:
- 이번 단위는 `실질 중복 체감 완화`로 채택한다.
- 데이터 삭제형 dedupe는 아직 아니지만, 사람이 시트에서 서로 다른 공고를 읽을 수 있는 구분성은 분명히 좋아졌다.

다음 우선순위:
- `company_screening._source_summary()`를 `active-yielding source` 중심 지표로 재설계할지 검증
- `run_collection_cycle_pipeline` 최상위 bookkeeping 누락 원인 확인
- practical duplicate를 soft display disambiguation에서 더 나아가 `soft collapse 후보`까지 볼지 결정

## 작업 단위 18

이름: `Source Scan Resume Fallback + Post-smoke Runtime Recovery`

목적:
- `approved/source growth`가 있는 상태에서 registry set이 변할 때 source collection cursor가 불필요하게 상단으로 reset되는 문제를 줄인다.
- registry change 이후에도 `cursor`, `processed survivor tail`, `saved offset` 순으로 진행도를 최대한 이어간다.
- notebook smoke 후 degraded published runtime을 다시 `published_company_state=true` 상태로 복구한다.

주요 코드:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/collection.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py`

실행 명령:

```bash
./scripts/setup_env.sh
./scripts/register_kernel.sh
pytest -q tests/test_jobs_market_v2.py -k 'resumes_from_cursor_when_registry_set_changes or resume_source_scan_offset'
pytest -q tests/test_jobs_market_v2.py -k 'source_scan or bootstrap_resume_until_full_scan'
pytest -q tests/test_jobs_market_v2.py -k 'collect_jobs_from_sources or run_collection_cycle_pipeline_bootstrap_and_incremental or run_collection_cycle_pipeline_bootstrap_resume_until_full_scan'
pytest -q
jupyter nbconvert --to notebook --execute runtime/notebook_smoke/00_source_screening.smoke.ipynb --output 00_source_screening.executed.ipynb --output-dir runtime/notebook_smoke_exec --ExecutePreprocessor.timeout=600
jupyter nbconvert --to notebook --execute runtime/notebook_smoke/01_bootstrap_population.smoke.ipynb --output 01_bootstrap_population.executed.ipynb --output-dir runtime/notebook_smoke_exec --ExecutePreprocessor.timeout=600
python -m jobs_market_v2.cli doctor
python -m jobs_market_v2.cli run-collection-cycle
python -m jobs_market_v2.cli collect-company-evidence --batch-size 200 --max-batches 10
python -m jobs_market_v2.cli screen-companies
python -m jobs_market_v2.cli sync-sheets --target staging
python -m jobs_market_v2.cli sync-sheets --target master
```

before_metrics:
- `quality_score_100 = 99.57`
- `master_rows = 94`
- `staging_rows = 94`
- `active_jobs = 93`
- `approved_company_count = 138`
- `source_registry verification_status=성공 = 483`
- `screened_source_count = 517`
- `source_collection_progress.next_source_offset = 50`
- `source_collection_progress.next_source_cursor = https://hunesion.com/recruit/content4`
- `company_evidence.next_offset = 2057`
- `company_evidence.completed_full_scan = false`

after_metrics:
- safe cycle:
  - `run-collection-cycle-20260401212317`
- post-smoke recovery:
  - `collect-company-evidence-20260401213151`
  - `screen-companies-20260401213852`
  - `sync-sheets-20260401213902`
- final safe runtime:
  - `quality_score_100 = 99.57`
  - `master_rows = 94`
  - `staging_rows = 94`
  - `active_jobs = 92`
  - `approved_company_count = 139`
  - `source_registry verification_status=성공 = 541`
  - `screened_source_count = 551`
  - `source_collection_progress.next_source_offset = 29`
  - `source_collection_progress.next_source_cursor = https://hrdkorea.or.kr`
  - `company_evidence.next_offset = 0`
  - `company_evidence.completed_full_scan = true`
  - `master_row_delta = 0`
  - `approved_company_delta = +1`
  - `verified_source_success_delta = +58`

kept_changes:
- source scan progress에 `collectable_source_urls`를 저장했다.
- registry change 시 `cursor -> processed survivor tail -> saved offset -> reset` 순서로 resume 판단을 하게 만들었다.
- run summary에 `source_scan_resume_strategy`를 남기도록 했다.
- full pytest, doctor, notebook smoke 2개, full collection cycle을 모두 다시 끝까지 통과시켰다.
- notebook smoke 후 내려간 published source/company state를 full company evidence scan으로 복구했다.

reverted_changes:
- 코드 revert는 없었다.
- smoke 이후 생성된 degraded published runtime은 유지하지 않았다.
- 복구 전 transient 상태:
  - `source_registry rows = 279`
  - `source_registry verification_status=성공 = 277`
  - `published_company_state = false`
  - `source_scan_resume_strategy = reset_after_registry_change`

last_known_safe_state:
- safe cycle:
  - `run-collection-cycle-20260401212317`
- recovery refresh:
  - `collect-company-evidence-20260401213151`
  - `screen-companies-20260401213852`
- safe runtime snapshot:
  - `master_rows = 94`
  - `staging_rows = 94`
  - `quality_score_100 = 99.57`
  - `approved_company_count = 139`
  - `source_registry verification_status=성공 = 541`
  - `screened_source_count = 551`
  - `company_evidence.completed_full_scan = true`

last_successful_run_id:
- `run-collection-cycle-20260401212317`

resume_next_step:
- 다음 시작점은 `approved 139 / screened sources 551 / verified success 541 / master 94`다.
- `runtime/company_evidence_progress.json`은 `completed_full_scan = true`, `next_offset = 0` 상태다.
- `runtime/source_collection_progress.json`은 `next_source_offset = 29`, `next_source_cursor = https://hrdkorea.or.kr`, `completed_full_scan_count = 6` 상태다.
- 다음 run은 notebook smoke 없이 safe state에서 다시 cycle을 돌려 `source_scan_resume_strategy`가 cursor/survivor/offset fallback으로 실제 유지되는지 보라.

failures:
- 최초 full pytest 1회차에서 `test_incremental_update_pipeline_marks_changed_and_missing`가 한 번 실패했지만, 단독 재현과 전체 재실행에서는 통과했다.
- notebook smoke는 여전히 production runtime/source progress를 흔들었다.
- 이번 run 자체로 `master_rows`는 증가하지 않았다.
- subagent 2개는 usable 결과 없이 종료됐다.

next priorities:
- notebook smoke isolation 또는 automatic restore를 코드화하라.
- safe state에서 한 번 더 cycle을 돌려 top reset이 실제로 줄었는지 검증하라.
- ATS/GreetingHR/recruiter 내부 second-order ranking을 손봐 `approved/source growth -> master growth` 번역률을 올려라.

검증 결과:
- targeted pytest 통과
  - `pytest -q tests/test_jobs_market_v2.py -k 'resumes_from_cursor_when_registry_set_changes or resume_source_scan_offset'`
  - `pytest -q tests/test_jobs_market_v2.py -k 'source_scan or bootstrap_resume_until_full_scan'`
  - `pytest -q tests/test_jobs_market_v2.py -k 'collect_jobs_from_sources or run_collection_cycle_pipeline_bootstrap_and_incremental or run_collection_cycle_pipeline_bootstrap_resume_until_full_scan'`
- 전체 pytest 통과
  - `pytest -q`
- notebook smoke 2개 통과
  - `runtime/notebook_smoke/00_source_screening.smoke.ipynb`
  - `runtime/notebook_smoke/01_bootstrap_population.smoke.ipynb`
- `doctor` 통과
- `run-collection-cycle` 통과
- post-smoke recovery 완료
  - `company_evidence.completed_full_scan = true`
  - `screen-companies company_state_mode = published`
  - `sync-sheets staging/master = true`

판정:
- 이번 단위는 `safe resume fallback + runtime recovery`로 채택한다.
- master 증가는 없었지만, quality 99.57을 유지한 채 approved/source published state를 `139 / 541`까지 다시 닫아 다음 run의 starting point는 분명해졌다.

## 작업 단위 19

이름: `Source Scan Policy Reset + Balanced ATS Exploration`

목적:
- `approved/source growth -> master growth` 번역률을 실제로 올린다.
- source scan ordering 변경 배포에서 stale cursor를 그대로 이어받아 새 우선순위가 무력화되는 문제를 없앤다.
- `hot active ATS` 몇 개만 먼저 갱신하고, 그 다음에는 `zero-active ATS`를 타입별로 섞어 탐색하게 만들어 growth slot이 한 타입에 독점되지 않게 한다.

주요 코드:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/collection.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py`

실행 명령:

```bash
pytest -q tests/test_jobs_market_v2.py::test_collect_jobs_from_sources_prioritizes_historically_active_and_unscanned_ats_sources tests/test_jobs_market_v2.py::test_collect_jobs_from_sources_prioritizes_active_and_unverified_before_zero_active_successes tests/test_jobs_market_v2.py::test_collect_jobs_from_sources_interleaves_same_signal_ats_types tests/test_jobs_market_v2.py::test_collect_jobs_from_sources_resets_progress_when_scan_policy_changes tests/test_jobs_market_v2.py::test_collect_jobs_from_sources_resumes_with_surviving_cursor_when_registry_set_changes tests/test_jobs_market_v2.py::test_source_collection_registry_signature_ignores_transient_source_activity
python -m jobs_market_v2.cli doctor
python -m jobs_market_v2.cli run-collection-cycle
./scripts/setup_env.sh
./scripts/register_kernel.sh
./.venv/bin/pytest -q
./.venv/bin/jupyter nbconvert --to notebook --execute runtime/notebook_smoke/00_source_screening.smoke.ipynb --output 00_source_screening.executed.ipynb --output-dir runtime/notebook_smoke_exec --ExecutePreprocessor.timeout=600
./.venv/bin/jupyter nbconvert --to notebook --execute runtime/notebook_smoke/01_bootstrap_population.smoke.ipynb --output 01_bootstrap_population.executed.ipynb --output-dir runtime/notebook_smoke_exec --ExecutePreprocessor.timeout=600
python -m jobs_market_v2.cli doctor
python -m jobs_market_v2.cli run-collection-cycle
```

before_metrics:
- `quality_score_100 = 99.57`
- `master_rows = 94`
- `staging_rows = 94`
- `active_jobs = 92`
- `approved_company_count = 139`
- `effective_verified_source_success_count = 541`
- `effective_screened_source_count = 551`
- `source_scan_next_offset = 29`
- `source_scan_next_cursor = https://hrdkorea.or.kr`

after_metrics:
- first post-patch cycle:
  - `run-collection-cycle-20260401224320`
  - `source_scan_start_offset = 68`
  - `source_scan_resume_strategy = cursor`
  - `master_rows = 94`
  - `new_job_count = 0`
- growth cycle:
  - `run-collection-cycle-20260401224743`
  - `source_scan_start_offset = 0`
  - `source_scan_resume_strategy = policy_reset`
  - `source_scan_next_offset = 39`
  - `master_rows = 100`
  - `staging_rows = 100`
  - `active_jobs = 98`
  - `new_job_count = 6`
  - `net_job_delta = +6`
- final safe runtime:
  - `run-collection-cycle-20260401230222`
  - `quality_score_100 = 99.52`
  - `master_rows = 103`
  - `staging_rows = 103`
  - `active_jobs = 101`
  - `approved_company_count = 139`
  - `effective_screened_source_count = 556`
  - `effective_verified_source_success_count = 546`
  - `published_screened_source_count = 281`
  - `published_verified_source_success_count = 279`
  - `source_scan_policy_version = v4`
  - `source_scan_next_offset = 247`
  - `company_evidence_next_offset = 1200`
  - `master_row_delta_vs_run_start = +9`
  - `approved_company_delta_vs_run_start = 0`
  - `effective_verified_source_success_delta_vs_run_start = +5`

kept_changes:
- source scan signal priority를 `hot active ATS -> unseen ATS -> zero-active ATS -> warm active -> failures/html` 구조로 재정렬했다.
- 같은 signal tier 안에서는 `greetinghr/recruiter/...`가 round-robin으로 섞이게 만들어 특정 ATS 타입 독점을 줄였다.
- ordering policy 배포 시 old progress cursor를 이어받지 않도록 `policy_version = v4`와 `policy_reset`을 추가했다.
- targeted pytest, full pytest, setup_env, register_kernel, notebook smoke 2개, doctor, full collection cycle을 모두 다시 통과시켰다.

reverted_changes:
- 코드 revert는 없었다.
- `run-collection-cycle-20260401224320` 결과는 safe check는 통과했지만 growth가 없어서 final safe state로는 채택하지 않았다.

last_known_safe_state:
- growth cycle:
  - `run-collection-cycle-20260401224743`
- final post-smoke safe cycle:
  - `run-collection-cycle-20260401230222`
- supporting safe progress:
  - `update-incremental-20260401230415`
  - `company-evidence-20260401230254`
- effective runtime snapshot:
  - `master_rows = 103`
  - `staging_rows = 103`
  - `quality_score_100 = 99.52`
  - `approved_company_count = 139`
  - `runtime/source_registry_in_progress.csv = 556 rows / 성공 546 / 실패 8`
  - `runtime/source_registry.csv = 281 rows / 성공 279 / 실패 2`

last_successful_run_id:
- `run-collection-cycle-20260401230222`

resume_next_step:
- 다음 시작점은 `master 103 / active 101 / approved 139 / effective verified success 546 / effective screened sources 556`다.
- `runtime/source_collection_progress.json`은 `policy_version = v4`, `next_source_offset = 247`, `last_run_id = update-incremental-20260401230415` 상태다.
- `runtime/company_evidence_progress.json`은 `next_offset = 1200`, `run_id = company-evidence-20260401230047` 상태다.
- `published_company_state=false`라서 다음 cycle은 `runtime/source_registry_in_progress.csv` 재사용 경로를 탈 수 있다는 점을 handoff에 명시한다.

failures:
- 첫 post-patch cycle은 stale cursor를 이어받아 growth가 없었고, 이를 후속 `policy_reset`으로 교정했다.
- notebook smoke는 통과했지만 runtime bookkeeping을 다시 흔들었다. 최종 doctor + cycle로 safe state는 복구했지만 `published source_registry.csv`와 `effective in-progress registry` 차이는 남아 있다.
- `html_link_catalog_url` timeout 10건은 계속 남아 있어 seed source growth bookkeeping을 지연시킨다.

next priorities:
- `published_company_state=false`에서 published/in-progress registry mismatch를 줄여 결과 해석과 handoff를 더 신뢰 가능하게 만들어라.
- `source_scan_next_offset = 247` 이후에도 v4 ordering이 계속 master growth로 번역되는지 확인하라.
- same-domain/html source noise trimming과 `requirements / preferred / focus / hiring_track` recall 보강을 quality 99 이상 유지 조건으로 계속 진행하라.

검증 결과:
- targeted pytest 통과
  - `pytest -q tests/test_jobs_market_v2.py::test_collect_jobs_from_sources_prioritizes_historically_active_and_unscanned_ats_sources tests/test_jobs_market_v2.py::test_collect_jobs_from_sources_prioritizes_active_and_unverified_before_zero_active_successes tests/test_jobs_market_v2.py::test_collect_jobs_from_sources_interleaves_same_signal_ats_types tests/test_jobs_market_v2.py::test_collect_jobs_from_sources_resets_progress_when_scan_policy_changes tests/test_jobs_market_v2.py::test_collect_jobs_from_sources_resumes_with_surviving_cursor_when_registry_set_changes tests/test_jobs_market_v2.py::test_source_collection_registry_signature_ignores_transient_source_activity`
- 전체 pytest 통과
  - `./.venv/bin/pytest -q`
- notebook smoke 2개 통과
  - `runtime/notebook_smoke/00_source_screening.smoke.ipynb`
  - `runtime/notebook_smoke/01_bootstrap_population.smoke.ipynb`
- `doctor` 통과
- final `run-collection-cycle` 통과

판정:
- 이번 단위는 `growth-producing source ordering hardening`으로 채택한다.
- `quality_score_100 >= 99`를 유지한 채 `master_rows 94 -> 103`으로 증가시켰고, growth-safe state를 `policy_version = v4`와 함께 남겼다.

## 작업 단위 20

이름: `Target-role Recall Recovery + Published-State Close`

목적:
- 이미 verified 된 source에서 놓치던 명백한 AI/ML 엔지니어 제목을 master row로 번역한다.
- notebook smoke 이후 무너진 published runtime을 다시 safe state로 닫는다.
- 최종 handoff 시 `company_evidence.completed_full_scan = true` 상태를 남긴다.

주요 코드:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/collection.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py`

실행 명령:

```bash
pytest -q tests/test_jobs_market_v2.py -k test_job_role_classification
./scripts/setup_env.sh
./scripts/register_kernel.sh
source .venv/bin/activate && pytest -q
source .venv/bin/activate && jupyter nbconvert --to notebook --execute runtime/notebook_smoke/00_source_screening.smoke.ipynb --output 00_source_screening.executed.ipynb --output-dir runtime/notebook_smoke_exec --ExecutePreprocessor.timeout=600
source .venv/bin/activate && jupyter nbconvert --to notebook --execute runtime/notebook_smoke/01_bootstrap_population.smoke.ipynb --output 01_bootstrap_population.executed.ipynb --output-dir runtime/notebook_smoke_exec --ExecutePreprocessor.timeout=600
source .venv/bin/activate && python -m jobs_market_v2.cli collect-company-evidence --batch-size 200 --max-batches 20 --no-resume
source .venv/bin/activate && python -m jobs_market_v2.cli screen-companies
source .venv/bin/activate && python -m jobs_market_v2.cli doctor
source .venv/bin/activate && python -m jobs_market_v2.cli run-collection-cycle
source .venv/bin/activate && python -m jobs_market_v2.cli collect-company-evidence --batch-size 200 --max-batches 20 --no-resume
source .venv/bin/activate && python -m jobs_market_v2.cli screen-companies
source .venv/bin/activate && python scripts/write_automation_status.py end --phase manual_recovery --result success --resume-next-step "Investigate Neowiz Holdings approval loss and raise approved-source conversion without sacrificing quality"
```

before_metrics:
- `quality_score_100 = 99.57`
- `master_rows = 94`
- `staging_rows = 94`
- `active_jobs = 92`
- `approved_company_count = 139`
- `effective_verified_source_success_count = 541`
- `effective_screened_source_count = 551`

after_metrics:
- safe growth cycle:
  - `run-collection-cycle-20260401230222`
- final published-state restore:
  - `collect-company-evidence-20260401230047`
  - `screen-companies-20260401230932`
- final safe runtime:
  - `quality_score_100 = 99.52`
  - `master_rows = 103`
  - `staging_rows = 103`
  - `active_jobs = 101`
  - `approved_company_count = 138`
  - `effective_verified_source_success_count = 546`
  - `effective_screened_source_count = 556`
  - `company_evidence.completed_full_scan = true`
  - `company_evidence.next_offset = 0`
  - `source_scan_next_offset = 247`
  - `master_row_delta_vs_run_start = +9`
  - `approved_company_delta_vs_run_start = -1`
  - `effective_verified_source_success_delta_vs_run_start = +5`

kept_changes:
- `classify_job_role`가 `ML/MLOps 엔지니어`, `음성인식 엔진/모델 개발자`를 `인공지능 엔지니어`로 분류하도록 recall을 넓혔다.
- `AI Agent 프론트엔드 개발자`는 계속 제외되도록 negative coverage를 추가했다.
- smoke 이후 degraded runtime을 full company evidence scan으로 복구하고, published company state를 다시 `true`로 닫았다.
- final master에서 실제 신규 반영을 확인했다.
  - `안랩 / [경력] ML/MLOps 엔지니어`
  - `셀바스AI / 음성인식 엔진/모델 개발자`

reverted_changes:
- 코드 revert는 없었다.
- notebook smoke가 만든 unsafe runtime은 채택하지 않았다.
  - `quality_score_100 = 76.5`
  - `staging_rows = 0`
  - `source_registry rows = 281 / 성공 279`

last_known_safe_state:
- growth cycle:
  - `run-collection-cycle-20260401230222`
- post-cycle bookkeeping restore:
  - `collect-company-evidence-20260401230047`
  - `screen-companies-20260401230932`
- automation snapshot:
  - `runtime/automation_status.json`
  - `master_rows = 103`
  - `approved_companies = 138`
  - `verified_source_success_count = 546`

last_successful_run_id:
- `screen-companies-20260401230932`

resume_next_step:
- 다음 시작점은 `master 103 / approved 138 / verified 546 / quality 99.52`다.
- `runtime/company_evidence_progress.json`은 `completed_full_scan = true`, `next_offset = 0` 상태다.
- `runtime/source_collection_progress.json`은 `policy_version = v4`, `next_source_offset = 247` 상태다.
- 다음 run은 `네오위즈홀딩스` 승인 이탈을 먼저 조사하고, 그 다음 `네이버/삼성커리어스`처럼 verified 대비 master 전환이 약한 html/direct hiring 패턴을 다뤄라.

failures:
- notebook smoke는 여전히 production runtime bookkeeping을 크게 흔든다.
- final restore 과정에서 `approved_company_count`가 `139 -> 138`로 1건 감소했고, 빠진 회사는 `네오위즈홀딩스`였다.

next_priorities:
- approved count 손실(`네오위즈홀딩스`)을 복구하라.
- verified source에서 놓치는 target-role recall을 더 보강하라.
- direct hiring html source 파싱을 보강해 `approved/source -> master` 번역률을 더 올려라.
- smoke isolation 또는 automatic restore를 코드화하라.

검증 결과:
- targeted pytest 통과
  - `pytest -q tests/test_jobs_market_v2.py -k test_job_role_classification`
- 전체 pytest 통과
  - `source .venv/bin/activate && pytest -q`
- notebook smoke 2개 통과
  - `runtime/notebook_smoke/00_source_screening.smoke.ipynb`
  - `runtime/notebook_smoke/01_bootstrap_population.smoke.ipynb`
- `doctor` 통과
- final `run-collection-cycle` 통과
- post-cycle published-state restore 통과
  - `collect-company-evidence --batch-size 200 --max-batches 20 --no-resume`
  - `screen-companies`

판정:
- 이번 단위는 `recall-driven master growth + bookkeeping recovery`로 채택한다.
- `quality_score_100 >= 99`를 유지한 채 `master_rows 94 -> 103`으로 올렸고, final handoff는 `published_company_state=true` 상태로 마무리했다.

## 작업 단위 21

이름: `v5 Active Source Pinning + Published-State Recovery`

목적:
- `approved/source growth`가 low-yield cursor 구간에서 `master` 증가로 끊기는 병목을 줄인다.
- 이미 생산성이 검증된 source를 cursor와 별도로 다시 확인해 신규 row 번역률을 올린다.
- validation 이후 흔들린 published company/source 상태를 다시 닫아 handoff 신뢰도를 높인다.

주요 코드:
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/collection.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py`

실행 명령:

```bash
pytest -q tests/test_jobs_market_v2.py -k 'prioritizes_ats_before_html_pages or prioritizes_unscanned_and_historically_active_ats_sources_before_zero_active_successes or prioritizes_active_and_unverified_before_zero_active_successes or interleaves_same_signal_ats_types or rechecks_historically_active_sources_while_cursor_advances or resets_progress_when_scan_policy_changes'
./scripts/setup_env.sh
./scripts/register_kernel.sh
pytest -q
./.venv/bin/jupyter nbconvert --to notebook --execute runtime/notebook_smoke/00_source_screening.smoke.ipynb --output 00_source_screening.executed.ipynb --output-dir runtime/notebook_smoke_exec --ExecutePreprocessor.timeout=600
./.venv/bin/jupyter nbconvert --to notebook --execute runtime/notebook_smoke/01_bootstrap_population.smoke.ipynb --output 01_bootstrap_population.executed.ipynb --output-dir runtime/notebook_smoke_exec --ExecutePreprocessor.timeout=600
python -m jobs_market_v2.cli doctor
python -m jobs_market_v2.cli run-collection-cycle
python -m jobs_market_v2.cli collect-company-evidence --batch-size 200 --max-batches 6
python -m jobs_market_v2.cli screen-companies
python -m jobs_market_v2.cli sync-sheets --target staging
python -m jobs_market_v2.cli sync-sheets --target master
```

before_metrics:
- `quality_score_100 = 99.52`
- `master_rows = 103`
- `staging_rows = 103`
- `active_jobs = 101`
- `approved_company_count = 138`
- `effective_verified_source_success_count = 546`
- `effective_screened_source_count = 556`
- `company_evidence.next_offset = 1200`
- `published_company_state = false`

after_metrics:
- validation cycle:
  - `doctor-20260401235535`
  - `run-collection-cycle-20260401235553`
- post-cycle published recovery:
  - `collect-company-evidence-20260402000318`
  - `screen-companies-20260402000749`
- final safe runtime:
  - `quality_score_100 = 99.47`
  - `master_rows = 105`
  - `staging_rows = 105`
  - `active_jobs = 103`
  - `approved_company_count = 140`
  - `source_registry verification_status=성공 = 545`
  - `screened_source_count = 555`
  - `company_evidence.completed_full_scan = true`
  - `company_evidence.next_offset = 0`
  - `source_collection_progress.policy_version = v5`
  - `source_collection_progress.next_source_offset = 255`
  - `master_row_delta_vs_run_start = +2`
  - `approved_company_delta_vs_run_start = +2`
  - `verified_source_success_delta_vs_run_start = -1`

kept_changes:
- source scan progress policy를 `v5`로 올려 stale cursor를 reset 가능하게 했다.
- `historically active source`를 `zero-active ATS`보다 먼저 보게 재정렬했다.
- 이미 지나간 cursor 앞쪽의 productive source를 매 run 다시 포함시키는 `active source pinning`을 추가했다.
- summary에 `cursor_selected_collectable_source_count`, `cursor_processed_collectable_source_count`, `pinned_collectable_source_count`를 추가했다.
- `master_rows 103 -> 105`, `approved_company_count 138 -> 140`까지 실제 성장으로 번역했다.
- post-cycle full company evidence publish + `screen-companies`로 final handoff를 `published_company_state=true`로 닫았다.

reverted_changes:
- 코드 revert는 없었다.
- `run-collection-cycle-20260401233616`의 no-growth intermediate state는 final safe state로 채택하지 않았다.

last_known_safe_state:
- validation cycle:
  - `run-collection-cycle-20260401235553`
- bookkeeping recovery:
  - `collect-company-evidence-20260402000318`
  - `screen-companies-20260402000749`
- final runtime snapshot:
  - `master_rows = 105`
  - `approved_company_count = 140`
  - `source_registry verification_status=성공 = 545`
  - `company_evidence.completed_full_scan = true`

last_successful_run_id:
- `screen-companies-20260402000749`

resume_next_step:
- 다음 시작점은 `master 105 / active 103 / approved 140 / verified 545 / screened sources 555 / quality 99.47`다.
- `runtime/company_evidence_progress.json`은 `completed_full_scan = true`, `next_offset = 0` 상태다.
- `runtime/source_collection_progress.json`은 `policy_version = v5`, `next_source_offset = 255` 상태다.
- 다음 run은 `offset 255` 이후에서도 active pinning이 성장 번역을 계속 유지하는지 먼저 확인하라.

failures:
- `published verified source success`는 `546 -> 545`로 1건 줄어 third-priority metric은 소폭 역행했다.
- notebook smoke는 여전히 runtime bookkeeping을 흔들어 post-cycle recovery가 필요했다.
- `html_link_catalog_url` timeout 10건은 계속 남아 있다.

next_priorities:
- `v5` pinning 비율과 cursor exploration 균형을 더 다듬어 `master > 105` 성장을 이어가라.
- html/direct hiring source에서 `approved/source -> master` 번역률을 더 올려라.
- `preferred / focus / requirements / hiring_track` recall을 quality 99 이상 유지 조건으로 더 보강하라.
- notebook smoke isolation 또는 automatic restore를 코드화하라.

검증 결과:
- targeted pytest 통과
  - `pytest -q tests/test_jobs_market_v2.py -k 'prioritizes_ats_before_html_pages or prioritizes_unscanned_and_historically_active_ats_sources_before_zero_active_successes or prioritizes_active_and_unverified_before_zero_active_successes or interleaves_same_signal_ats_types or rechecks_historically_active_sources_while_cursor_advances or resets_progress_when_scan_policy_changes'`
- 전체 pytest 통과
  - `pytest -q`
- notebook smoke 2개 통과
  - `runtime/notebook_smoke/00_source_screening.smoke.ipynb`
  - `runtime/notebook_smoke/01_bootstrap_population.smoke.ipynb`
- `doctor` 통과
- final `run-collection-cycle` 통과
- post-cycle published-state recovery 통과
  - `collect-company-evidence --batch-size 200 --max-batches 6`
  - `screen-companies`
  - `sync-sheets --target staging`
  - `sync-sheets --target master`

판정:
- 이번 단위는 `growth translation hardening + bookkeeping recovery`로 채택한다.
- `quality_score_100 >= 99`를 유지한 채 `master_rows 103 -> 105`, `approved_company_count 138 -> 140`까지 올렸고, final handoff는 다시 `published_company_state=true`로 마무리했다.

## 2026-04-02 00:50 KST - practical duplicate collapse hardening

before_metrics:
- `master_rows = 105`
- `same_content_groups = 3`
- `job_url_dupes = 0`
- `job_key_dupes = 0`

after_metrics:
- `doctor`: 통과
- `promote-staging`: 통과
- `sync-sheets --target staging`: 성공
- `sync-sheets --target master`: 성공
- final runtime:
  - `master_rows = 103`
  - `same_content_groups = 1`
  - `job_url_dupes = 0`
  - `job_key_dupes = 0`
  - `경력수준_표시 blank = 25`
  - `채용트랙_표시 blank = 32`
  - `직무초점_표시 blank = 4`
  - `주요업무_표시 blank = 5`
  - `자격요건_표시 blank = 5`
  - `우대사항_표시 blank = 14`

kept_changes:
- `quality.py` practical duplicate collapse 규칙을 `같은 회사 + 같은 직무 + 같은 본문` 기준으로 재구성했다.
- cosmetic title/summary 차이만 있는 동일본문 공고는 collapse 하고, 지역 차이는 유지하도록 했다.
- 실제 `master`에서 `마키나락스 AI Research Engineer (Junior)`와 `서울로보틱스 Director`를 제거했다.
- 회귀 테스트를 추가했다.
  - same-content duplicate collapse
  - location split keep
  - military-service track keep

reverted_changes:
- 없음

failures:
- `실질 중복 = 0`은 아직 아니다.
- 남은 same-content group은 `인터엑스 서울/울산` 1개다.
- `approved/source -> master` 번역률과 field recall 문제는 여전히 남아 있다.

next_priorities:
- `인터엑스 서울/울산` location split을 하나로 접을지, 별도 공고로 둘지 제품 정책 고정
- direct HTML hiring source 번역률 개선
- `채용트랙/경력/우대사항` recall 추가 개선

## 2026-04-02 01:20 KST - practical duplicate collapse + runs recovery

before_metrics:
- `master_rows = 103`
- `same_content_groups = 3`
- `job_url_dupes = 0`
- `job_key_dupes = 0`
- `runs.csv` parse error blocked `sync-sheets`

after_metrics:
- targeted duplicate/fallback pytest: 통과
- `doctor`: 통과
- `promote-staging`: 통과
- `sync-sheets --target master`: 성공
- `sync-sheets --target staging`: 성공
- final runtime:
  - `master_rows = 103`
  - `same_content_groups = 1`
  - `job_url_dupes = 0`
  - `job_key_dupes = 0`

kept_changes:
- `quality.py`
  - practical duplicate variant key를 location-only 보존 쪽으로 단순화
  - same-content duplicate 대표 선택 로직 추가
- `storage.py`
  - malformed csv line fallback + sparse garbage row drop 추가
- `tests/test_jobs_market_v2.py`
  - malformed csv fallback 회귀
  - same-content duplicate collapse/keep 회귀 보강
- `runtime/runs.csv`
  - 손상 fragment line 1개 제거

failures:
- `실질 중복 = 0`은 아직 아니다.
- 남은 same-content group은 `인터엑스 서울/울산` 1개다.
- `approved/source -> master` 번역률, direct HTML parsing, field recall은 계속 열린 상태다.

next_priorities:
- `인터엑스 서울/울산`을 collapse 할지 keep 할지 제품 정책 고정
- direct HTML hiring source 번역률 개선
- `채용트랙/경력/우대사항` recall 추가 개선

## 2026-04-02 01:35 KST - location-only duplicate collapse

before_metrics:
- `master_rows = 103`
- `same_content_groups = 1`

after_metrics:
- targeted duplicate/fallback pytest: 통과
- `doctor`: 통과
- `promote-staging`: 통과
- `sync-sheets --target master`: 성공
- `sync-sheets --target staging`: 성공
- final runtime:
  - `master_rows = 102`
  - `same_content_groups = 0`
  - `job_url_dupes = 0`
  - `job_key_dupes = 0`

kept_changes:
- location-only same-content pair도 collapse 하도록 `quality.py` practical duplicate policy를 강화했다.
- surviving title에는 merged location hint를 남기도록 했다.

next_priorities:
- direct HTML hiring source 번역률 개선
- `채용트랙/경력/우대사항` recall 추가 개선
- smoke / notebook isolation

## 2026-04-02 03:25 KST - HTML detail hydration

before_metrics:
- published `master_rows = 102`
- published `staging_rows = 103`
- published `verified_source_success_count = 545`
- published `html_page verified success = 482`
- published `html_page last_active_job_count > 0 = 1`

after_metrics:
- targeted html/source pytest 6개: 통과
- direct probe:
  - `네이버 공식 채용`
  - `[네이버랩스] Generative AI Research Engineer`
  - `주요업무/자격요건/우대사항` 복원 확인
  - `채용트랙_표시 = 박사 / 석사`
  - `구분요약_표시 = 경력 / 박사 / 석사`

kept_changes:
- `collection.py`
  - weak HTML listing metadata 판정 강화
  - same-host detail follow hydration 추가
  - generic detail page에서 `.detail_box`/`dl`/`table` 기반 본문/메타데이터 복원 추가
- `tests/test_jobs_market_v2.py`
  - weak listing -> detail hydration 회귀 추가

resolved:
- `html_page`가 detail URL만 찾고 본문은 못 읽던 코드 경로는 해결

partial:
- direct probe 수준에서는 효과 확인
- full published runtime에 다시 번역되는지는 fresh live run 재확인 필요

unresolved:
- `approved/source -> master` 번역률은 아직 blocker
- `automation_status.json` stale
- smoke / notebook isolation 미완

next_priorities:
- fresh live `update-incremental` 또는 `run-collection-cycle`로 HTML detail hydration이 published staging/master에 실제 반영되는지 확인
- reflected 이후 `master` / `html_page active>0` 증가를 다시 측정

## 2026-04-02 02:35 KST - generalized near-duplicate collapse + publish sync

before_metrics:
- published `master_rows = 102`
- published `staging_rows = 103`
- sheet export `master_rows = 102`
- duplicate complaint:
  - exact duplicate는 아니지만 `상세본문/섹션이 거의 같은 공고`가 시트에 남아 있었음

after_metrics:
- targeted duplicate pytest 5개: 통과
- `doctor`: 통과
- `promote-staging`: 통과
  - `dropped_low_quality_job_count = 5`
  - `promoted_job_count = 98`
- `sync-sheets --target master`: 성공
- final runtime:
  - `master_rows = 98`
  - `staging_rows = 98`
  - `sheet export master_rows = 98`
  - `job_url_dupes = 0`
  - `job_key_dupes = 0`
  - `refiltered master dropped rows = 0`

kept_changes:
- `quality.py`
  - heuristic same-content duplicate 외에 `near-practical-duplicate` collapse 확장
  - borderline pair에만 Gemini adjudication 경로 추가
- `gemini.py`
  - duplicate pair adjudication prompt/cache 추가
- `settings.py`
  - duplicate adjudication settings 추가
- `pipelines.py`
  - publish path가 settings/paths를 넘겨 duplicate adjudication을 실제로 사용하도록 연결

resolved:
- generalized near-duplicate collapse가 published runtime과 시트 export까지 반영됨
- stale `staging/master` mismatch 해소

partial:
- 현재 `master`에서 높은 유사도로 남는 pair는 `3개`
  - `당근` 학사 vs 전환형 인턴
  - `쿠팡` Staff vs Sr. Staff MLE
  - `몰로코` Senior vs II
- 이 3개는 현 정책상 별도 포지션으로 유지

unresolved:
- `approved/source -> master` 번역률이 여전히 최상위 blocker
- `경력수준/채용트랙/우대사항` recall 부족
- bookkeeping / smoke isolation 미해결

next_priorities:
- direct HTML hiring source에서 실제 `master` 증가를 더 만드는 작업
- `채용트랙_표시`, `경력수준_표시`, `우대사항_표시` 공란 축소
- runtime / bookkeeping / smoke isolation 정리

## 2026-04-02 02:56 KST - html growth translation v8

before_metrics:
- published `master_rows = 98`
- published `staging_rows = 98`
- `verified_success = 545`
- `html_page verified = 482`
- `html_page active-yielding = 3`

after_metrics:
- targeted html/scout/duplicate pytest 6개: 통과
- `doctor`: 통과
- `update-incremental`: 성공
  - `staging_job_count = 99`
  - `new_job_count = 1`
  - `net_job_delta = 1`
  - `net_active_job_delta = 1`
  - `source_scan_resume_strategy = policy_reset`
  - `source_scan_next_offset = 128`
- `promote-staging`: 성공
  - `promoted_job_count = 99`
- `sync-sheets --target master`: 성공
- final runtime:
  - `master_rows = 99`
  - `staging_rows = 99`
  - `sheet export master_rows = 99`
  - `quality_score_100 = 99.31`

kept_changes:
- `collection.py`
  - `v8` source-scan policy
  - strong listing candidate만 html scout 대상으로 유지
  - active pin limit 확대
  - strong hiring path는 content 신호가 약해도 Gemini listing probe 허용
  - detail-style href anchor도 Gemini probe payload에 포함
- `settings.py`
  - `gemini_html_listing_max_calls_per_run` 기본값 확대
- `tests/test_jobs_market_v2.py`
  - strong listing scout 회귀
  - detail href Gemini payload 회귀

resolved:
- duplicate/sync blocker 이후 growth engine이 다시 실제 `master` 증가로 번역됨
- published와 sheet export가 다시 일치

partial:
- 증가폭은 작지만 `98 -> 99`로 전진
- `policy v8` 리셋 후 다음 source cursor는 `128`

unresolved:
- `approved/source -> master` 번역률은 여전히 최상위 blocker
- low-active ATS/html source 보강 필요
- 품질 공란:
  - `경력수준_표시 blank = 23`
  - `채용트랙_표시 blank = 31`
  - `우대사항_표시 blank = 15`

next_priorities:
- low-active `greetinghr/recruiter/html_page`에서 active-yielding source 늘리기
- `채용트랙/경력/우대사항` recall 강화
- bookkeeping / smoke isolation 정리

## 작업 단위 16: User-Visible Duplicate Collapse

goal:
- 사람이 시트에서 보는 `상세본문_분석용`이 거의 같은 pair를 publish 단계에서 직접 collapse

implemented:
- `quality.py`
  - `visible_detail_seq` 추가
  - `visible_detail_seq >= 0.9` duplicate hard-collapse
  - `title_seq >= 0.98` strong title match fallback
- `gemini.py`
  - duplicate adjudication policy `service_duplicate_v3`
  - `visible_detail_seq` metric 전달
- `tests/test_jobs_market_v2.py`
  - 하이퍼커넥트/크래프톤 visible duplicate 회귀 추가

verification:
- targeted pytest 5개 통과
- `doctor` 통과
- `promote-staging`: `97`
- `sync-sheets --target master`: 성공
- `sync-sheets --target staging`: 성공
- final runtime:
  - `master_rows = 97`
  - `staging_rows = 97`
  - `sheet export master_rows = 97`
  - `sheet export staging_rows = 97`
  - `all_pairs_ge_0.9 = 0`
  - `하이퍼커넥트 pairs>=0.85 = 0`
  - `크래프톤 pairs>=0.85 = 0`

resolved:
- user-visible duplicate blocker를 publish와 sheet까지 반영

unresolved:
- `approved/source -> master` 번역률
- `경력수준/채용트랙/우대사항` blank
- bookkeeping / smoke isolation

## 작업 단위 17: Representative-Only Company Job Family Collapse

goal:
- 같은 회사의 같은 직군 family는 대표 1건만 남겨, 채용 준비 분석용 모집단으로 정리

implemented:
- `quality.py`
  - same title family면 본문 차이가 있어도 대표 1건 collapse
  - keep rank는 더 풍부한 본문/섹션을 가진 행을 선호
- `tests/test_jobs_market_v2.py`
  - 몰로코 same-family representative collapse 회귀
  - 서울로보틱스 level variant collapse 회귀

verification:
- targeted pytest 5개 통과
- `promote-staging`: `69`
- `sync-sheets --target master`: 성공
- `sync-sheets --target staging`: 성공
- final runtime:
  - `master_rows = 69`
  - `staging_rows = 69`
  - `sheet export master_rows = 69`
  - `sheet export staging_rows = 69`
  - `pairs_ge_0.85 = 0`
  - `경력수준_표시 blank = 15`
  - `채용트랙_표시 blank = 23`
  - `우대사항_표시 blank = 9`

resolved:
- requisition-level duplicate 집착이 아니라 analysis-representative dataset으로 정리

unresolved:
- `approved/source -> master` 번역률
- low-active direct HTML / ATS source recall
- bookkeeping / smoke isolation

## 작업 단위 18: Role-Salvage Growth Translation

goal:
- parsed jobs는 있는데 `job_role = NONE`이라 버려지던 source를 실제 `master` 증가로 번역

implemented:
- `constants.py`
  - `데이터분석`, `데이터사이언스`, `AI서비스개발` title signal 보강
- `gemini.py`
  - `maybe_salvage_job_role(...)` 추가
  - heuristic miss source에 대해 Gemini가 허용 직무 4개 중 하나만 반환하도록 제한
- `collection.py`
  - `normalize_job_payload(...)`에서 role miss 시 Gemini role salvage 시도
  - `collect_jobs_from_sources(...)`에 `gemini_role_salvage_max_calls_per_run` budget 연결
- `pipelines.py`
  - subset `registry_frame` incremental이 full `source_registry.csv`를 보존하도록 `_merge_updated_source_registry(...)` 추가
- `tests/test_jobs_market_v2.py`
  - role classification / role salvage / subset incremental merge 회귀 추가

verification:
- targeted pytest 통과
- live probe:
  - `신세계I&C recruiter` parsed `9`, accepted `2`
  - accepted jobs:
    - `데이터분석(데이터사이언스)` -> `데이터 사이언티스트`
    - `AI서비스개발` -> `인공지능 엔지니어`
- bounded publish:
  - `update-incremental` subset run
  - `promote-staging`
  - `sync-sheets --target staging`
  - `sync-sheets --target master`
- final runtime:
  - `master_rows = 71`
  - `staging_rows = 71`
  - `신세계I&C source last_active_job_count = 2`

resolved:
- `parsed > 0 but role miss`로 떨어지던 일부 recruiter source를 실제 `master` 증가로 번역
- full cycle 없이도 subset incremental로 안전하게 published 반영 가능

unresolved:
- `approved/source -> master` 전체 번역률은 여전히 낮음
- low-yield `html_page/greetinghr/recruiter` source 다수가 아직 0 active
- `채용트랙/경력/우대사항` blank

## 작업 단위 19: Generic HTML Detail Translation

goal:
- `listing page가 아니라 detail page 자체`인 html source를 실제 published growth로 번역

implemented:
- `collection.py`
  - generic single-detail page 판정과 fallback 추가
  - generic nav anchor만 있는 page에서 detail page 1건으로 변환
  - sparse detail block이면 full page content를 description으로 확장
  - collection path에서 Gemini analysis refinement 활성화
- `constants.py`
  - `AI개발자`, `AI 개발자`를 `인공지능 엔지니어` signal로 추가
- `gemini.py`
  - analysis refinement payload가 full `description_text` context를 유지하도록 수정
  - stale display cache invalidation (`v3`)
- `tests/test_jobs_market_v2.py`
  - generic nav link만 있어도 single detail page를 우선하는 회귀 추가

verification:
- targeted pytest 통과
- direct probe:
  - `데이터메이커 /job-openings/51,52,53` -> parsed `1/1/1`, accepted `3/3`
- bounded publish:
  - `데이터메이커 /job-openings/53` 단독 subset run
  - `quality_gate_passed = true`
  - `master_rows = 71 -> 72`
  - `staging_rows = 71 -> 72`
  - `master/staging` Google Sheets sync 성공

resolved:
- generic html detail source에서 실제 published growth `+1`을 만들었다
- exact listing/ATS 보강이 아니라 `detail page 직접 번역` 경로가 살아났다

unresolved:
- `데이터메이커 /job-openings/51,52`는 아직 sparse-detail quality가 낮아 published merge 시 gate를 흔든다
- 최상위 blocker는 여전히 `approved/source -> master` 번역률

## 작업 단위 20: Datamaker Three-Page Publish And Experience Recall

goal:
- `데이터메이커 /job-openings/51,52,53` 3건을 모두 published까지 올리고, body/title의 `n년 이상` 표현을 `경력수준_표시`로 반영

implemented:
- `presentation.py`
  - raw section extractor와 extracted section이 충돌할 때 raw section을 우선하는 보정 추가
  - `2년 이상의 ... 개발 경험`, `(2년 이상 / 계약직)` 같은 broad Korean year patterns를 경력 추론으로 해석
- `tests/test_jobs_market_v2.py`
  - alternate Korean heading recovery 회귀 수정
  - broad Korean year-pattern 경력 추론 회귀 추가

verification:
- targeted pytest 통과
- bounded publish:
  - `데이터메이커 /job-openings/51,52,53` subset run
  - `quality_gate_passed = true`
  - `master_rows = 72 -> 74`
  - `staging_rows = 72 -> 74`
  - `new_job_count = 2`
  - `changed_job_count = 1`
  - `master/staging` Google Sheets sync 성공
- post-publish metrics:
  - `경력수준_표시 blank = 18 -> 10`
  - `채용트랙_표시 blank = 26`
  - `우대사항_표시 blank = 10`
- published Datamaker rows:
  - `/51` -> `경력 2년+`
  - `/52` -> `경력 2년+`
  - `/53` -> `박사 / 석사`

resolved:
- `데이터메이커 /job-openings/51,52`도 published에 안전하게 올라왔다
- body/title 기반 `n년 이상` 패턴을 실제 display 경력수준으로 반영하게 됐다

unresolved:
- `approved/source -> master` 번역률은 여전히 낮다
- top zero-active targets는 `웹젠`, `메디픽셀`, `알서포트`, `오픈엣지테크놀로지`, `엔젤로보틱스`
- metadata blank의 큰 축은 아직 `당근`, `센드버드`, `크래프톤`

## 작업 단위 21: List-Like HTML Same-Prefix Detail Extraction

goal:
- `career/recruit` 같은 목록형 HTML 페이지에서 상단 네비가 아니라 실제 상세 공고 링크를 일반적으로 뽑아내기

implemented:
- `collection.py`
  - `_HTML_STRONG_LIST_PATH_HINTS`에 `/career/recruit` 추가
  - list-like base path 아래의 더 깊은 same-prefix detail path를 공고 후보로 인식
  - 같은 페이지에서 실제 detail 링크가 발견되면 `채용`, `채용공고`, self-link 같은 generic nav는 후처리로 제거
- `tests/test_jobs_market_v2.py`
  - `test_parse_html_jobs_extracts_same_prefix_detail_links_from_list_like_page` 추가

verification:
- targeted html parsing pytest 통과
- direct probe:
  - `https://www.rsupport.com/ko-kr/career/recruit` -> parsed `7`
  - parsed detail examples:
    - `.../career/recruit/back_end_dev/`
    - `.../career/recruit/front_end_dev/`

resolved:
- list-like same-prefix detail path를 못 잡아 generic nav만 남던 HTML archive 패턴을 일반화했다

unresolved:
- `알서포트`는 parsed `7`까지는 됐지만 AI/data accepted jobs는 `0`이다
- 즉 이번 패치는 parser 일반화에는 성공했지만 `master` 증분으로는 아직 번역되지 않았다

## 작업 단위 21: Zero-Active Source Translation via Skelterlabs

goal:
- `verified success but active 0` source 중 실제 AI/data 공고가 숨어 있는 케이스를 published growth로 번역

implemented:
- probe 결과를 기준으로 `스켈터랩스 https://www.skelterlabs.com/career`를 bounded subset target으로 선택
- collection/parser 변경 없이 current code path의 translated output을 runtime에 반영

verification:
- direct probe:
  - `parsed = 7`
  - `accepted = 2`로 예상했지만 live subset run에서 actual collected `4`
- bounded publish:
  - `update_incremental_pipeline(... registry_frame=subset_for_skelterlabs ...)`
  - `quality_gate_passed = true`
  - `master_rows = 74 -> 78`
  - `staging_rows = 74 -> 78`
  - `new_job_count = 4`
  - `net_active_job_delta = 4`
  - `master/staging` Google Sheets sync 성공
- post-publish:
  - `active_gt_0_sources = 34 -> 35`
  - published `스켈터랩스` rows `4`

resolved:
- `verified success / active 0`이던 source 하나를 실제 published growth `+4`로 번역
- current parser/runtime만으로도 zero-active source 재평가에서 성장 여지가 있다는 걸 입증

unresolved:
- 전체 자율 증분 수준으로 일반화되진 않았다
- `웹젠`처럼 fetch는 살아 있지만 AI/data role로 안 걸리는 source와, `오픈엣지`처럼 새로운 source family parser가 필요한 케이스가 섞여 있다

## 작업 단위 22: Flex Public API Translation for OpenEdges

goal:
- `careers.team / flex.team` 계열의 zero-active source를 public API 기반 parser로 translated growth로 바꾼다

implemented:
- `__NEXT_DATA__`에서 `customerIdHash`, `API_ENDPOINT`, `RECRUITING_SITE_ROOT_DOMAIN` 추출
- `flex.team/api-public/v2/recruiting/customers/{customerIdHash}/sites/job-descriptions`
- `.../job-descriptions/{jobDescriptionIdHash}/details`
- `LEXICAL_V1` content.data -> HTML renderer 추가
- `Neural Network Optimization Engineer` title이 `인공지능 엔지니어`로 분류되도록 signal 보강

verification:
- targeted pytest 통과
- direct probe:
  - `https://www.openedges.com/positions`
  - `parsed_jobs = 13`
  - `accepted_count = 1`
- bounded publish:
  - `update-incremental-20260402200234`
  - `master_rows = 78 -> 79`
  - `staging_rows = 78 -> 79`
  - `new_job_count = 1`
  - `net_active_job_delta = 1`
- post-merge:
  - `https://www.openedges.com/positions`
  - `last_active_job_count = 1`
  - `active_gt_0_sources = 36`
  - `OpenEdges published rows = 1`

resolved:
- `OpenEdges`는 더 이상 parser blocker가 아니다
- `careers.team / flex.team` 계열에 재사용 가능한 public API translation path를 확보했다

unresolved:
- 아직 이 family 전체가 자율 증분으로 일반화되진 않았다
- 다음 target은 `Angel Robotics /recruit/notice` RSC parser와 metadata blank 축소다

## 작업 단위 23: Embedded GreetingHR Translation for Devsisters

goal:
- `html_page`로 발견된 custom-domain GreetingHR 채용 사이트를 zero-active에서 active source로 번역한다

implemented:
- `fetch_source_content()`에 embedded GreetingHR 감지 경로 추가
  - `profiles.greetinghr.com`
  - `opening-attachments.greetinghr.com`
  - `__NEXT_DATA__`의 `openings` query 존재 시
  - 기존 `_fetch_greetinghr_source()`로 우회
- regression test 추가:
  - custom-domain GreetingHR가 `html_page`여도 GreetingHR fetcher로 라우팅되는지 검증

verification:
- targeted pytest 3개 통과
- direct probe:
  - `https://careers.devsisters.com`
  - `openings = 60`
  - AI/data family title 최소 `4`
  - live accepted `2`
- bounded publish:
  - `update-incremental-20260402201814`
  - `master_rows = 79 -> 81`
  - `staging_rows = 79 -> 81`
  - `new_job_count = 2`
  - `net_active_job_delta = 2`
- post-merge:
  - `https://careers.devsisters.com`
  - `last_active_job_count = 2`
  - `verified_sources = 546`
  - `active_gt_0_sources = 38`
  - `데브시스터즈 published rows = 2`

resolved:
- `데브시스터즈`는 더 이상 zero-active parser blocker가 아니다
- `html_page`로 발견된 custom-domain GreetingHR family에 재사용 가능한 translation path를 확보했다

unresolved:
- `노을` custom frontend는 아직 별도 family parser가 필요하다
- `Angel Robotics /recruit/notice` RSC parser도 아직 남아 있다

## 작업 단위 24: Embedded NineHire Translation for Visang

goal:
- `html_page`로 발견된 custom-domain NineHire 채용 사이트를 zero-active에서 active source로 번역한다

implemented:
- `career.visang.com`를 bounded subset incremental로 검증했다
- `embedded NineHire` 경로가 실제로 `accepted job`을 반환하는 것을 확인했다
- `collection.py` 내부 NineHire helper 중복 정의를 정리했다

verification:
- targeted pytest 2개 통과
  - `embedded_ninehire_custom_domain`
  - `fetch_ninehire_source_builds_jobs_from_public_api`
- bounded publish:
  - `update-incremental-20260402203604`
  - `master_rows = 81 -> 83`
  - `staging_rows = 81 -> 83`
  - `new_job_count = 1`
  - `net_active_job_delta = 1`
- post-merge:
  - `https://career.visang.com`
  - `last_active_job_count = 2`
  - `active_gt_0_sources = 39`

resolved:
- `비상교육 career.visang.com`은 더 이상 zero-active source가 아니다
- `embedded NineHire custom domain` family에 재사용 가능한 growth path를 확보했다

unresolved:
- `노을`은 same family지만 현재 공개 직무가 AI/data 성장에 직접 연결되지 않는다
- `Meissa / ByteSize`는 별도 custom frontend 해석이 더 필요하다

## 작업 단위 25: Angel Notice Family Publish Recovery

goal:
- `Angel Robotics /recruit/notice`에서 split된 `AI Researcher` row가 low-quality drop으로 사라지는 문제를 막고 published까지 반영한다

implemented:
- `collection.py`
  - family split 시 role-specific body만 남기지 않고 parent posting context를 함께 유지하도록 수정
  - `main_tasks`는 raw HTML이 아니라 plain text로 넘기도록 정리
- `tests/test_jobs_market_v2.py`
  - `next_rsc_notice_family_roles` 회귀를 parser/normalize 기준으로 보강

verification:
- targeted pytest 3개 통과
  - `embedded_ninehire_custom_domain`
  - `fetch_ninehire_source_builds_jobs_from_public_api`
  - `next_rsc_notice_family_roles`
- `doctor` 통과
- bounded publish:
  - `update-incremental-20260402204848`
  - `promote-staging-20260402204901`
  - `sync-sheets master/staging` 성공
  - `master_rows = 83 -> 84`
  - `staging_rows = 83 -> 84`
  - `new_job_count = 1`
  - `net_active_job_delta = 1`

resolved:
- `엔젤로보틱스`는 이제 `Data Scientist`와 `AI Researcher` 2건이 모두 published에 남는다
- family split row가 context loss 때문에 quality filter에서 사라지는 버그는 더 이상 blocker가 아니다

unresolved:
- `verified_sources = 546` 대비 `active_gt_0_sources = 39`로 성장 번역률은 아직 낮다
- 다음 growth 후보는 `Meissa`, `ByteSize`, `Suresoft`다

## 작업 단위 26: Angel English Bullet Preservation

goal:
- `Angel Robotics /recruit/notice` family split row의 영문 기술 bullet이 analysis/quality normalization 단계에서 사라지지 않게 해 published 2건을 안정적으로 유지한다

implemented:
- `presentation.py`
  - `build_analysis_fields(...)`에 raw `main_tasks` 기반 영문 technical bullet preservation fallback 추가
- `quality.py`
  - `normalize_job_analysis_fields(...)`에서 noisy/blank `주요업무_분석용`을 raw `main_tasks` fallback으로 복원
- `tests/test_jobs_market_v2.py`
  - `next_rsc_notice_family_roles` 회귀를 quality filter 통과 기준으로 강화

verification:
- targeted pytest 통과
- `doctor` 통과
- bounded subset publish for `https://www.angel-robotics.com/ko/recruit/notice`
  - published `Angel rows = 2`
  - `master = 84`
  - `staging = 84`

resolved:
- `AI Researcher`, `Data Scientist` 두 row가 모두 low-quality drop 없이 published에 남는다
- 영문 bullet 위주의 family split posting도 quality gate를 통과할 수 있다

unresolved:
- `approved/verified -> active_gt_0` 성장 번역률은 여전히 낮다
- metadata blank는 아직 남아 있다

## 작업 단위 27: Saramin Relay Follow + Gazzi Careers Detail Translation

goal:
- `알체라` Saramin relay family와 `가치랩스` custom careers family를 generic HTML 규칙만으로 active/master growth source로 번역한다

implemented:
- `collection.py`
  - `"/careers"` family를 strong list path로 인정해 same-host detail을 follow하도록 보강
  - `relay/view`, `rec_idx=`를 HTML detail URL hint로 추가해 Saramin relay posting을 actual detail candidate로 인정
  - `자세히 보기` 같은 generic detail CTA도 hiring row context가 있으면 detail follow 대상으로 인정
  - generic detail CTA 판정에서 줄바꿈 whitespace를 접어 실제 live HTML(`자세히\n보기`)도 잡히게 수정
  - generic detail page title 후보에 `.panel-heading h3`, `.panel-title`를 추가해 Gazzi 상세 페이지 제목을 `Careers`가 아니라 실제 공고 제목으로 읽게 수정
- `tests/test_jobs_market_v2.py`
  - same-prefix `/careers/<slug>` detail 추출 회귀
  - Saramin relay posting anchor 추출 회귀
  - generic detail CTA follow 회귀
  - panel heading title extraction 회귀

verification:
- targeted pytest 6개 통과
- live probe:
  - `https://gazzi.ai/careers`
    - `parsed = 1`
    - `accepted = 1`
    - accepted title:
      - `[병역특례 전문연구요원] AI 기술 및 응용솔루션 R&D 엔지니어 (1명)`
  - `https://www.alchera.ai/company/career`
    - `parsed = 16`
    - `accepted = 2`
    - accepted title:
      - `[알체라] AI 연구원 모집 (얼굴인식/위조판별)` x2 relay rows
- bounded publish:
  - `update-incremental-20260402234159`
  - `promote-staging-20260402234207`
  - `sync-sheets master/staging` 성공
  - `doctor` 성공
  - `master_rows = 100 -> 101`
  - `staging_rows = 100 -> 101`
  - `new_job_count = 1`
  - `net_active_job_delta = 1`
  - `quality_score_100 = 99.09`
- post-merge:
  - `https://gazzi.ai/careers`
    - `last_active_job_count = 1`
  - `https://www.alchera.ai/company/career`
    - `last_active_job_count = 2`
  - `active_gt_0_sources = 41 -> 43`
  - published `가치랩스 rows = 1`
  - published `알체라 rows = 0`

resolved:
- `가치랩스`는 더 이상 zero-active custom careers source가 아니다
- `알체라`는 더 이상 zero-active source가 아니다
- `Saramin relay`와 `generic /careers detail CTA` family에 재사용 가능한 translation path를 확보했다

unresolved:
- `알체라`는 source activation까지는 됐지만, 현재 relay rows 2건이 `master` publish로는 이어지지 않았다
- subset run 기준 `dropped_low_quality_job_count = 2`가 남아 있어 `active source -> master row` 번역률 문제는 계속 남는다
- metadata blank는 여전히 남아 있다

## 작업 단위 28: Display Blank Batch Closure

목표:
- 개별 회사별 수선이 아니라 `경력수준_표시`, `우대사항_표시` blank를 generic rule로 한 번에 닫는다.

변경:
- `presentation.py`
  - 강한 실무 신호(`실무 경험`, `제품 개선 경험`, `설계/구현/운영/배포`, `프로덕션 환경`, `상용 서비스`)를 `경력`으로 일반화했다.
  - 경험을 끝내 추론하지 못해도 signal text가 있으면 `경력수준_표시 = 미기재`, `경력근거_표시 = 표시기본값`으로 채운다.
  - `우대사항` heading에 `Bonus points`, `Would be a plus`, `What would make you stand out`를 추가했다.
  - 같은 계열 preferred heading이 반복되면 section을 합쳐 읽게 했다.
- `quality.py`
  - normalization 이후 `우대사항_표시`가 비면 `별도 우대사항 미기재`로 채운다.

검증:
- targeted pytest 6개 통과
- `promote-staging` 성공
- `sync-sheets master/staging` 성공
- `doctor` 성공

결과:
- `master_rows = 101`
- `staging_rows = 101`
- `quality_score_100 = 99.27`
- `경력수준_표시 blank = 12 -> 0`
- `우대사항_표시 blank = 15 -> 0`
- `채용트랙_표시 blank = 0 유지`
- `경력수준_표시 = 미기재` rows = `7`
- `우대사항_표시 = 별도 우대사항 미기재` rows = `15`

의미:
- 이번 턴은 `숫자를 조금 줄였다`가 아니라, display blank family를 배포형 policy로 닫은 작업이다.
- 이제 remaining blocker는 display blank가 아니라 `verified -> active -> master` 번역률이다.

## 작업 단위 29: ATS Source Canonicalization + Preferred Salvage

목표:
- `GreetingHR/Recruiter` source URL이 root와 `/about`류로 갈라져 지표가 찢어지는 family를 canonicalize한다.
- current published의 `상세본문`에서 구어형 우대 섹션을 다시 salvaging해 `별도 우대사항 미기재`를 줄인다.

변경:
- `utils.py`
  - `canonicalize_runtime_source_url()` 추가
  - `career.greetinghr.com`, `recruiter.co.kr`는 host root로 canonicalize
- `screening.py`
  - screening 단계에서 source_url canonicalize
  - duplicate source_url은 `last_active_job_count`, `source_quality_score`, `last_success_at` 기준으로 stronger row를 남기게 dedupe
- `collection.py`
  - normalized job row의 `source_url`도 canonical root를 쓰도록 통일
- `quality.py`
  - `source_url`를 normalization 때 canonicalize
  - `우대사항_분석용`이 비어 있으면 `상세본문_분석용`에서 `_PREFERRED_HEADING_PATTERNS`로 다시 salvage

검증:
- targeted pytest 통과
- `promote-staging` 성공
- `sync-sheets master/staging` 성공
- `doctor` 성공

결과:
- `source_registry_rows = 556 -> 551`
- `verified_sources = 541`
- `active_gt_0_sources = 42`
- `active_but_no_master_sources = 3 -> 2`
- remaining active-no-master:
  - `알체라`
  - `거대한에이아이실험실 주식회사`
- `인터엑스`는 source registry와 published source_url이 root 하나로 정리됨
- `우대사항_표시 = 별도 우대사항 미기재` rows = `15 -> 14`
- `경력수준_표시 blank = 0`, `우대사항_표시 blank = 0`, `채용트랙_표시 blank = 0` 유지

의미:
- 이번 턴의 본질은 `source_url family canonicalization`으로 배포형 지표 왜곡을 줄인 것과,
- raw 재수집 없이 current published detail에서 semantic preferred를 일부 복원한 것이다.

## 작업 단위 30: Same-Page Multi-Role Identity Migration

목표:
- `same-page multi-role custom html` family가 bare page URL 하나를 공유해 `job_key`가 충돌하거나, legacy plain-URL row가 carry-forward로 남는 문제를 generic rule로 닫는다.

변경:
- `collection.py`
  - `_extract_multi_role_html_jobs()`가 role title마다 `#role-{stable-slug}` fragment를 붙인 고유 `job_url`을 만들게 했다.
  - `서비스 개발자/Service Developer` 같은 일반 서비스 delivery title은, 본문이 `웹/모바일 서비스`, `React`, `Node.js`, `RESTful API`, `SQL/NoSQL`, `UX/UI` 위주이고 강한 AI work signal이 없으면 Gemini role salvage를 타지 않게 했다.
  - `merge_incremental()`에서 fragmentized same-page replacement가 존재하면, 예전 bare URL의 `미발견/검증실패보류` legacy row는 alias로 보고 드롭하게 했다.
- `test_jobs_market_v2.py`
  - same-page role split URL uniqueness 회귀
  - 일반 서비스 delivery title의 Gemini role salvage 차단 회귀
  - fragmentized replacement가 생겼을 때 legacy bare URL row를 드롭하는 merge 회귀

검증:
- targeted pytest 13개 통과
- Huge AI Lab bounded incremental 재실행 통과
- `promote-staging` 성공
- `sync-sheets --target master` 성공
- `sync-sheets --target staging` 성공
- `doctor` 성공

결과:
- `거대한에이아이실험실 주식회사` family는 published/master 기준으로 이제 `AI 개발자(AI Model Developer/Researcher)` 1건만 남는다.
- `source_url = https://hugeailab.com/recruit`의 legacy `서비스 개발자(Service Developer)` plain-URL row는 제거됐다.
- current safe state:
  - `master_rows = 103`
  - `staging_rows = 103`
  - `verified_sources = 541`
  - `active_gt_0_sources = 42`
  - `active_but_no_master_sources = 0`
  - `quality_score_100 = 99.3`
  - `경력수준_표시 blank = 0`
  - `우대사항_표시 blank = 0`
  - `채용트랙_표시 blank = 0`
  - `경력수준_표시 = 미기재` rows = `8`
  - `우대사항_표시 = 별도 우대사항 미기재` rows = `14`

의미:
- 이번 턴의 본질은 `Huge AI Lab 1건`을 고친 것이 아니라, `same-page multi-role family` 전체에 통하는 identity 규칙과 legacy migration 규칙을 넣은 것이다.
- 이로써 exact `active but no master` source blocker는 현재 `0`이 됐다.

## 작업 단위 31: Semantic Fallback Batch Refresh

목표:
- 남아 있던 `경력수준_표시 = 미기재`와 `우대사항_표시 = 별도 우대사항 미기재`를 개별 회사 수선이 아니라 family-level generic rule과 bounded refresh로 줄인다.

변경:
- `presentation.py`
  - 연구형 공고에서 `전문연구요원/병역특례/Research Engineer/Scientist/R&D` 신호와 `석사/박사/논문/저널/학회/모델 연구·개발·고도화` 신호가 함께 있으면 `경력`으로 승격하게 했다.
  - 경험레벨 추론이 source별 단일 필드에서 안 잡혀도, `title + requirements + preferred + detail`의 `종합신호`를 한 번 더 판정하게 했다.
- `test_jobs_market_v2.py`
  - numeric year가 없어도 연구형 track에서 `경력`으로 승격되는 회귀 2건 추가

검증:
- targeted pytest 16개 통과
- semantic fallback source 14개 bounded refresh 통과
- `promote-staging` 성공
- `sync-sheets --target master` 성공
- `sync-sheets --target staging` 성공
- `doctor` 성공

결과:
- bounded refresh summary:
  - `subset_source_count = 14`
  - `collected_job_count = 45`
  - `staging_job_count = 108`
  - `new_job_count = 6`
  - `changed_job_count = 1`
  - `quality_gate_passed = true`
- current safe state:
  - `master_rows = 108`
  - `staging_rows = 108`
  - `verified_sources = 541`
  - `active_gt_0_sources = 41`
  - `active_but_no_master_sources = 0`
  - `quality_score_100 = 99.07`
  - `경력수준_표시 blank = 0`
  - `우대사항_표시 blank = 0`
  - `채용트랙_표시 blank = 0`
  - `경력수준_표시 = 미기재` rows = `8 -> 5`
  - `우대사항_표시 = 별도 우대사항 미기재` rows = `14 -> 18`

의미:
- `미기재`는 실제로 줄었다. 즉 이번 규칙은 published master에서도 generic하게 먹었다.
- 반면 `별도 우대사항 미기재`는 줄지 않았고 오히려 source refresh로 `18`이 됐다.
- 따라서 남은 배포 blocker는 exact source coverage가 아니라, `preferred section이 실제로 없거나 flat detail에 묻히는 family`를 어떻게 다룰지에 더 가깝다.
## 2026-04-03 Task 32

- 목표: 배포 blocker를 끝까지 닫고 현재 코드 기준 production-safe state를 다시 확정한다.
- 수행:
  - full `pytest -q`에서 드러난 late regressions 4개를 모두 수정했다.
    - 연구형 `경력수준` 일반화 과적용 축소
    - `우대사항_표시` 기본값 정책에 맞춘 quality gate test 정리
    - `상세보기` CTA를 generic stub으로 분류해 Gemini HTML listing fallback 회귀 복구
    - incremental source-scan ordering에서 historically active source가 scout에 밀리던 순서 복구
  - full `pytest -q`를 다시 끝까지 통과시켰다.
  - `run-collection-cycle`를 실제로 재실행해 one-line 배포 경로를 검증했다.
  - 첫 full cycle 결과는 `english leak count > 0`, `quality_score_100 < 99`로 승격이 막혔다.
  - `gemini.needs_gemini_refinement`에 `영문 장문 display leak` heuristic을 추가해 Gemini refinement가 영문 display leak row까지 자동 복원하도록 일반화했다.
  - leak source 17개만 묶은 bounded `update_incremental_pipeline(..., registry_frame=subset, enable_source_scan_progress=False)`를 재실행했다.
  - 그 결과 quality gate가 다시 통과했고 `staging 123`, `quality_score_100 99.11`, `english_leak_count 0`을 회복했다.
  - 이후 `promote-staging`, `sync-sheets --target master`, `sync-sheets --target staging`, `doctor`를 다시 모두 통과시켰다.
- 현재 safe state:
  - `master = 123`
  - `staging = 123`
  - `quality_score_100 = 99.11`
  - `english_leak_count = 0`
  - `duplicate_job_url_count = 0`
  - `경력수준_표시 blank = 0`
  - `채용트랙_표시 blank = 0`
  - `우대사항_표시 blank = 0`
  - `verified_sources = 541`
  - `active_gt_0_sources = 41`

## 2026-04-03 Task 33

- 목표: source discovery automation을 더 만드는 대신, 공식 AI 채용 출처 4곳을 직접 붙여 production universe를 넓힌다.
- 직접 추가한 공식 source:
  - `업스테이지` -> `https://careers.upstage.ai`
  - `노타` -> `https://career.nota.ai/en/jobs`
  - `리벨리온` -> `https://rebellions.career.greetinghr.com`
  - `퓨리오사AI` -> `https://furiosa.ai/careers`
- 처리:
  - `config/manual_companies_seed.csv`, `config/manual_sources_seed.yaml`에 추가
  - runtime imports 반영
  - subset runtime merge로 source registry에 편입
  - bounded incremental + quality gate + promote + sheet sync 완료
- generic fix:
  - section-less AI posting에서 `주요업무/자격요건/직무초점` 복구
  - admin-only pool posting drop
  - flat custom html detail의 title/focus fallback 보강
- 결과:
  - `master = 160`
  - `staging = 160`
  - `quality_score_100 = 99.79`
  - `new_job_count = 37`
  - `verified_sources = 545`
  - `active_gt_0_sources = 41`
- 추가 주의:
  - direct subset merge 과정에서 `runtime/source_registry.csv`와 `output_samples/approved_sources.csv`가 4행 subset으로 잘리는 runtime 정합성 문제가 발생했다.
  - 이를 `runtime/source_registry_in_progress.csv` full registry에 신규 4개 source를 merge하는 방식으로 복구했다.
  - 복구 후 registry counts:
    - `source_registry_rows = 555`
    - `approved_sources_rows = 400`
  - `doctor` 재통과 확인

## 2026-04-03 Task 34

- 목표: 개별 회사 추가가 아니라 `리스트 자체`를 시드로 붙여 스타트업·중소·중견 모집단을 절대적으로 늘린다.
- 리스트 소스:
  - `KOREA AI Startups` 기업편람
  - `https://startups.koraia.org/company/list`
  - bounded 범위는 page `0..19`의 company detail `200`건
- 처리:
  - current `companies_registry`와 대조해 신규 회사만 추출
  - 법률/협회/플랫폼 도메인/오탐을 제외
  - AI 직무 가능성이 큰 산업군만 유지
  - `config/koraia_ai_company_batch_20260403.csv`에 `81`개 company seed 저장
  - `import-companies` -> `discover-companies` 수행
  - 이 81개만 subset으로 `discover_source_candidates` 실행
  - source 결과:
    - `source_candidates = 19`
    - `approved = 13`
    - `candidate = 6`
  - approved 13개만 bounded `update_incremental_pipeline(..., registry_frame=approved)`로 수집
- 결과:
  - `verified_source_success_count = 13`
  - `collected_job_count = 24`
  - `new_job_count = 19`
  - `staging = 179`
  - `master = 179` after `promote-staging`
  - `quality_score_100 = 99.80`
  - `sync-sheets(master/staging)` 성공
  - `doctor` 성공
- 실질적으로 붙은 대표 source:
  - `https://stradvision.career.greetinghr.com`
  - `https://impactive-ai.career.greetinghr.com`
  - `https://www.tsnlab.com/careers`
  - `https://www.exosystems.io/en/career`
  - `https://www.scatterlab.co.kr/ko/recruiting`

## 2026-04-03 Task 35

- 목표: 스타트업·중소·중견 공식 리스트를 추가로 직접 붙여 모집단을 넓힌다.
- 리스트 소스:
  - 서울 AI 허브 resident / graduate 공식 roster
  - `https://www.seoulaihub.kr/partner/partner.asp?scrID=0000000195&pageNum=2&subNum=1&ssubNum=1&page=1`
- 처리:
  - agent가 detail endpoint 구조를 먼저 확인
  - `scripts/extract_seoul_ai_hub_batch.py` 추가
  - roster `318`개 파싱 -> homepage host 확인 `283`개 -> 신규 company seed `218`개 저장
  - `config/seoul_ai_hub_company_batch_20260403.csv` import
  - `discover-companies` 재실행
  - `scripts/apply_company_batch_sources.py` 추가
  - 이 batch만 subset으로 source discovery / screening / incremental 수집 / promote / sheet sync / doctor까지 수행
- 결과:
  - `subset_company_count = 216`
  - `source_candidate_count = 20`
  - `approved_count = 14`
  - `candidate_count = 5`
  - `new_job_count = 4`
  - `master = 183`
  - `staging = 183`
  - `source_registry_rows = 592`

## 2026-04-03 Task 36

- 목표: 두 번째 bounded 리스트로 `KOREA AI STARTUP 100` 공식 company info slice를 붙인다.
- 리스트 소스:
  - `https://aistartuptop100.co.kr/page/s2/s1.php?sty=2023`
- 처리:
  - agent가 Cloudflare를 우회해 현재 visible slice `12`개 company/domain을 확인
  - `config/korea_ai_startup100_slice_20260403.csv` 생성
  - import + `discover-companies`
  - 같은 `scripts/apply_company_batch_sources.py`로 batch 적용
- 결과:
  - `verified_source_success_count = 5`
  - `verified_source_failure_count = 1`
  - `selected_collectable_source_count = 6`
  - `new_job_count = 0`
  - `master = 183` 유지
  - `staging = 183` 유지
  - `source_registry_rows = 597`
