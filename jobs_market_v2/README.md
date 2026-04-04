# jobs_market_v2

`jobs_market_v2`는 국내 채용시장 전체 모집단을 넓게 확보하기 위한 리콜 우선형 수집 코드베이스다. 공식 채용 페이지, 공개 ATS, JSON-LD, RSS, sitemap만 사용하며, 채용 포털 직접 수집과 우회성 접근은 금지한다.

운영 수집은 기본적으로 실웹(`live`) 기준이다. 테스트 fixture 기반 mock 수집은 테스트나 개발 검증에서만 `JOBS_MARKET_V2_USE_MOCK_SOURCES=true`로 명시적으로 켠다.

## 핵심 설계

- 0단계: 소스 적격성 판정 후 `approved_sources.csv`, `candidate_sources.csv`, `rejected_sources.csv` 생성
- 1단계: `approved + candidate` 소스로 첫 공고 스냅샷을 수집해 `staging`에 적재
- 2단계: 증분 갱신으로 신규, 유지, 변경, 미발견을 추적하고 품질 게이트 통과 시에만 `master` 반영
- 리콜 우선: 애매한 소스는 초반에 버리지 않고 `candidate`로 분리해 재검토 가능하게 유지

## 가상환경 생성 명령

```bash
cd jobs_market_v2
python3 -m venv .venv
source .venv/bin/activate
```

Windows PowerShell:

```powershell
cd jobs_market_v2
python -m venv .venv
.venv\Scripts\Activate.ps1
```

## 라이브러리 설치 명령

```bash
pip install --upgrade pip
pip install -e .
```

## kernel 등록 명령

```bash
./scripts/register_kernel.sh
```

## Jupyter Lab 실행 명령

```bash
./scripts/run_jupyter.sh
```

노트북에서 선택할 kernel 이름은 `jobs-market-v2`다.

## 0단계 노트북 실행법

1. 가상환경 활성화 후 `./scripts/run_jupyter.sh` 실행
2. `notebooks/00_source_screening.ipynb` 열기
3. 셀을 위에서 아래로 실행

주요 출력 파일:

- `output_samples/approved_sources.csv`
- `output_samples/candidate_sources.csv`
- `output_samples/rejected_sources.csv`

## 1단계 노트북 실행법

1. 0단계 노트북을 먼저 실행
2. `notebooks/01_bootstrap_population.ipynb` 열기
3. 셀을 위에서 아래로 실행

주요 출력 파일:

- `output_samples/first_snapshot_jobs.parquet`
- `runtime/staging_jobs.csv`
- `runtime/raw_detail.jsonl`

## 수동 기업/소스 입력법

기업 수동 입력:

```bash
python -m jobs_market_v2.cli import-companies config/manual_companies_seed.csv
```

소스 수동 입력:

```bash
python -m jobs_market_v2.cli import-sources config/manual_sources_seed.yaml
```

지원 형식은 `csv`, `yaml`, `yml`, `xlsx`다.

## verify-sources 사용법

```bash
python -m jobs_market_v2.cli discover-sources
python -m jobs_market_v2.cli verify-sources
```

## collect-jobs --dry-run 사용법

```bash
python -m jobs_market_v2.cli collect-jobs --dry-run
```

## update-incremental 사용법

```bash
python -m jobs_market_v2.cli update-incremental
```

## staging 확인법

```bash
python -m jobs_market_v2.cli build-coverage-report
python -m jobs_market_v2.cli sync-sheets --target staging
```

`runtime/staging_jobs.csv`와 `runtime/coverage_report.json`도 함께 확인하면 된다.

## master 승격법

```bash
python -m jobs_market_v2.cli promote-staging
```

품질 게이트를 통과하지 못하면 `master`는 갱신되지 않고 `staging`만 유지된다.

## daily 실행법

GitHub Actions의 `.github/workflows/jobs_market_v2_daily.yml`이 일일 상태복원 실행용이다. 이 워크플로는 GitHub-hosted runner의 휘발성 파일시스템을 그대로 믿지 않고, `automation-state` branch에 runtime state bundle을 저장/복원한 뒤 Python CLI를 실행한다.

필수 secret:

- `GOOGLE_SHEETS_SPREADSHEET_ID`
- `GOOGLE_SERVICE_ACCOUNT_JSON`

선택 secret:

- `GEMINI_API_KEY`
- `SLACK_WEBHOOK_URL`

운영 원칙:

- 워크플로는 하루 1회, 최대 60분만 실행된다.
- 실패해도 마지막 성공 runtime bundle은 유지되고, 다음 런이 그 상태에서 자동 복구 시도한다.
- Slack은 `3회 연속 실패`부터만 보낸다.
- GitHub repo에는 `jobs_market_v2` 실제 코드 트리가 그대로 올라가며, runner는 checkout된 코드를 직접 실행한다.

새 GitHub repo를 만든 뒤 최초 연결할 때는:

```bash
cd /Users/junheelee/Desktop/sctaper_p1/jobs_market_v2
cp config/github_actions_secrets.env.example .env.github-actions.local
./scripts/bootstrap_github_repo.sh https://github.com/<owner>/<repo>.git
./scripts/bootstrap_automation_state_branch.sh
```

그 다음 GitHub repo의 `Settings > Secrets and variables > Actions`에 아래 값을 넣는다.

- `GOOGLE_SHEETS_SPREADSHEET_ID`
- `GOOGLE_SERVICE_ACCOUNT_JSON`
- `GEMINI_API_KEY` 선택
- `SLACK_WEBHOOK_URL` 선택

## bounded production 배포

새 Google Sheet를 만들어 현재 상태를 그대로 이어서 배포할 때는:

1. 새 시트를 만든다.
2. 서비스 계정에 편집 권한을 준다.
3. `.env`의 `GOOGLE_SHEETS_SPREADSHEET_ID`를 새 시트 ID로 바꾼다.
4. `runtime`은 지우지 않는다.
5. 아래를 실행한다.

```bash
./scripts/run_production_cycle.sh
```

자세한 절차는 아래 문서를 본다.

- `docs/PRODUCTION_DEPLOY.md`

## doctor 실행 방법

```bash
python -m jobs_market_v2.cli doctor
```

## 권장 실행 순서

```bash
python -m jobs_market_v2.cli collect-company-seed-records
python -m jobs_market_v2.cli expand-company-candidates
python -m jobs_market_v2.cli discover-companies
python -m jobs_market_v2.cli collect-company-evidence
python -m jobs_market_v2.cli screen-companies
python -m jobs_market_v2.cli discover-sources
python -m jobs_market_v2.cli verify-sources
python -m jobs_market_v2.cli collect-jobs
python -m jobs_market_v2.cli build-coverage-report
python -m jobs_market_v2.cli promote-staging
python -m jobs_market_v2.cli sync-sheets --target staging
```

Google Sheets 확인 탭:

- `기업선정 탭`: 후보 기업 전체와 `기업버킷(approved/candidate/rejected)`, `후보시드유형`, `후보시드URL` 확인
- `기업근거 탭`: 기업별 `후보시드근거`, 공식도메인, 공식채용소스, 타깃직무공고 근거 확인
- `staging 탭`: 최신 공고 적재 결과 확인
- `master 탭`: 품질 게이트 통과 후 최종 반영본 확인

기업 후보군 입력 파일:

- `config/company_seed_sources.yaml`: 회사 목록 출처 레지스트리
- `config/company_seed_records.csv`: 출처가 있는 후보 레코드의 주 입력
- `runtime/company_seed_records_collected.csv`: company seed source 수집 결과
- `config/manual_sources_seed.yaml`: 공식 공개 채용 소스 기반 후보 입력
- `config/seed_company_inputs.yaml`: source-backed 후보 입력이 비어 있을 때만 쓰는 bootstrap fallback

## continue 반복 절차

`continue`는 무한 반복하지 않는다. 매 라운드는 아래 순서로 같은 기준으로 점검한다.

1. `company_candidates`, `company_evidence`, `approved_companies`를 보고 비어 있는 기업층과 직무 적합 근거를 먼저 확인한다.
2. 그 다음 `source_registry`와 `coverage_report`를 보고 비어 있는 기업층, 직무, 소스 유형을 확인한다.
3. 부족한 층을 메우는 `live` 공식 소스만 추가하거나, 공통 추출 규칙만 수정한다.
4. 아래 명령을 순서대로 실행한다.

```bash
python -m jobs_market_v2.cli collect-company-evidence
python -m jobs_market_v2.cli screen-companies
python -m jobs_market_v2.cli verify-sources
python -m jobs_market_v2.cli collect-jobs
python -m jobs_market_v2.cli build-coverage-report
python -m jobs_market_v2.cli promote-staging
python -m jobs_market_v2.cli sync-sheets --target staging
python -m jobs_market_v2.cli sync-sheets --target master
```

5. 아래 항목을 숫자로 기록한다.

- approved / candidate / rejected 기업 수
- 기업층별 approved 기업 수
- 기업별 evidence 수와 primary evidence 유형
- 후보시드URL이 있는 기업 수와 없는 기업 수
- verified approved source 수
- 기업층별 verified source 수
- 직무별 활성 공고 수
- `주요업무_표시`, `자격요건_표시`, `우대사항_표시`, `핵심기술_표시` 채움 수
- 회사 집중도와 층별 편중
- `update-incremental` 1회 실행 결과

6. 기준을 통과하지 못하면 다음 `continue`는 `새 회사별 예외`가 아니라 `기업 후보군 근거 계층`, `공통 계층`, `부족한 기업층 소스`를 보강하는 방식으로만 진행한다.

## 모집단 수집 전환 기준

아래 기준을 충족하면 `continue`를 멈추고 본격 모집단 수집으로 넘어간다.

- `verified approved source`가 `15개 이상`
- `대기업`, `스타트업`, `중견/중소`, `공공·연구기관`, `외국계 한국법인`, `지역기업` 6개 기업층이 모두 `실검증 성공 소스`를 1개 이상 보유
- `데이터 분석가`, `데이터 사이언티스트`, `인공지능 리서처`, `인공지능 엔지니어` 4개 직무가 모두 `0건 아님`
- `주요업무_표시` 채움률이 `85% 이상`
- `자격요건_표시` 채움률이 `90% 이상`
- `핵심기술_표시` 채움률이 `95% 이상`
- `영문 누수`가 `0`
- `update-incremental`을 `2회 연속` 실행했을 때 품질 게이트가 유지됨
- 특정 1개 회사에 과도하게 편중되지 않아야 하며, 해석 가능한 수준의 분산이 확보되어야 함

위 기준 중 하나라도 부족하면 `하드닝 계속`, 모두 충족하면 `본격 모집단 수집 시작`으로 판단한다.

## 본격 모집단 수집 시작 절차

전환 기준을 통과한 뒤에는 아래 절차로 운영한다.

1. `manual_sources_seed.yaml`, `manual_companies_seed.csv`, `approved_companies.csv`, 공통 추출 규칙을 잠근다.
2. `verify-sources`를 다시 실행해 당일 기준 verified source 집합을 확정한다.
3. `collect-jobs`로 기준 스냅샷을 만든다.
4. `build-coverage-report`와 Google Sheets `staging 탭`을 검토해 이상치를 마지막으로 확인한다.
5. 이상이 없으면 `promote-staging` 후 `master 탭`을 동기화한다.
6. 그 다음부터는 `update-incremental` 중심으로 운영하고, 새 소스 추가는 별도 검증 라운드로 분리한다.

## 본격 수집 중 수정 원칙

본격 모집단 수집에 들어간 뒤에는 비용과 회귀를 줄이기 위해 아래 원칙을 지킨다.

- 수집 중간에 parser를 자주 바꾸지 않는다.
- 새 회사 추가와 parser 수정은 같은 라운드에 무분별하게 섞지 않는다.
- `master`가 흔들릴 수 있는 변경은 `staging`에서 먼저 검증한다.
- Gemini는 공통 규칙으로 채우기 어려운 경우에만 보조적으로 사용한다.
- 특정 회사 하나만 맞추는 예외 규칙은 지양하고, 가능한 한 `ATS family` 또는 `공통 heading 패턴`으로 흡수한다.

## 작업 단위 적립

반복 작업은 `작업 단위`로 닫는다. 각 단위는 코드, 테스트, 산출물을 함께 남기고 다음 단위의 입력으로 사용한다.

- 작업 단위 1: `기업 후보군 / 근거 / 승인`
- 산출물:
  - `runtime/company_candidates.csv`
  - `runtime/company_evidence.csv`
  - `runtime/approved_companies.csv`
  - `runtime/candidate_companies.csv`
  - `runtime/rejected_companies.csv`
- 실행 명령:

```bash
python -m jobs_market_v2.cli discover-companies
python -m jobs_market_v2.cli collect-company-evidence
python -m jobs_market_v2.cli screen-companies
```

- 테스트 범위:
  - 기업 후보군 생성
  - 근거 적재
  - approved / candidate / rejected 분리
  - approved 기업 우선 source discovery 연결

세부 작업 기록은 [docs/WORK_UNITS.md](/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/docs/WORK_UNITS.md)에 누적한다.
