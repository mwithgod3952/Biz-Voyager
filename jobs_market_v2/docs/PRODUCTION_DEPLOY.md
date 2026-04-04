# Bounded Production Deploy

이 문서는 `jobs_market_v2`를 **현재 확보한 공식 출처 universe 안의 bounded production 운영**으로 배포하는 절차를 정리한다.

중요:
- 이 절차는 **현재 로컬 runtime 상태를 유지한 채** 새 Google Sheet에 연결하는 방식이다.
- `runtime`을 지우지 않는다.
- 즉, 지금까지 쌓아 둔 후보군/근거/source registry/master 상태를 새 시트에 이어서 반영한다.

## 배포 범위

현재 배포 가능한 범위:
- `run-collection-cycle` one-line 자동 운영
- 후보군 재확장
- shadow 자동 평가/승격
- candidate 재평가
- approved 재선별
- 공고 수집/증분
- `master` 반영
- Google Sheets 동기화

현재 배포 범위 밖:
- 웹 전체에서 한 번도 안 본 완전히 새로운 최상위 공식 카탈로그 도메인을 스스로 계속 찾는 절대적 완전자동화

즉, 현재 배포는 `bounded production`이다.

## 권장 방식

추천:
1. 새 Google Sheet를 만든다.
2. 서비스 계정에 시트 편집 권한을 준다.
3. 현재 로컬 runtime 상태는 유지한다.
4. 새 시트 ID만 바꿔서 배포한다.

비추천:
- 새 시트 + runtime 전체 삭제

이 방식은 더 깨끗해 보일 뿐, 실제 커버리지는 뒤로 간다.

## 1. 새 Google Sheet 준비

1. Google Sheet를 새로 만든다.
2. `GOOGLE_SERVICE_ACCOUNT_JSON` 안의 서비스 계정 이메일에 편집 권한을 준다.
3. 시트 ID를 복사한다.

시트에는 자동으로 아래 탭이 채워진다.
- `기업선정 탭`
- `기업근거 탭`
- `staging 탭`
- `master 탭`
- `source_registry 탭`
- `runs 탭`
- `errors 탭`

## 1-1. 새 GitHub repo 연결

새 빈 GitHub repo를 하나 만든 뒤, 로컬에서 아래를 실행한다.

```bash
cd /Users/junheelee/Desktop/sctaper_p1/jobs_market_v2
cp config/github_actions_secrets.env.example .env.github-actions.local
./scripts/bootstrap_github_repo.sh https://github.com/<owner>/<repo>.git
./scripts/bootstrap_automation_state_branch.sh
```

이 단계가 하는 일:
1. 현재 배포 파일들만 stage/commit 한다.
2. 새 `origin` remote를 연결한다.
3. 기본 branch를 push한다.
4. `automation-state` branch를 bootstrap한다.

주의:
- 이 스크립트는 배포 대상 경로만 올린다.
- 루트의 이상한 stray 파일은 commit 대상에 포함하지 않는다.

## 2. 환경 변수 설정

프로젝트 루트에서 `.env`를 준비한다.

최소 필수:

```dotenv
GOOGLE_SHEETS_SPREADSHEET_ID=새_시트_ID
GOOGLE_SERVICE_ACCOUNT_JSON='{"type":"service_account",...}'
JOBS_MARKET_V2_USE_MOCK_SOURCES=false
JOBS_MARKET_V2_ENABLE_FALLBACK_SOURCE_GUESS=false
```

선택:

```dotenv
GEMINI_API_KEY=...
JOBS_MARKET_V2_ENABLE_GEMINI_FALLBACK=false
JOBS_MARKET_V2_GEMINI_MODEL=gemini-2.5-flash
JOBS_MARKET_V2_GEMINI_MAX_CALLS_PER_RUN=8
JOBS_MARKET_V2_GEMINI_TIMEOUT_SECONDS=15
```

기본 원칙:
- `USE_MOCK_SOURCES`는 반드시 `false`
- `ENABLE_FALLBACK_SOURCE_GUESS`는 기본적으로 `false`
- production에서는 현재 live 공식 소스만 사용

## 3. runtime 유지 여부

현재 권장 배포는 **runtime 유지**다.

유지해야 하는 이유:
- `company_candidates`
- `company_evidence`
- `source_registry`
- `staging/master`
- seed source discovered/shadow/invalid cache
- company evidence progress

이 상태를 유지해야 지금까지 축적한 판단과 수집 이력이 새 시트로 이어진다.

## 4. 배포 전 체크

```bash
cd /Users/junheelee/Desktop/sctaper_p1/jobs_market_v2
source .venv/bin/activate
python -m jobs_market_v2.cli doctor
```

`doctor`가 통과해야 한다.

## 5. 최초 배포 실행

추천 명령:

```bash
cd /Users/junheelee/Desktop/sctaper_p1/jobs_market_v2
source .venv/bin/activate
python -m jobs_market_v2.cli run-collection-cycle
```

이 명령이 수행하는 것:
1. 후보군 재확장
2. company evidence 재수집
3. 기업 bucket 재선별
4. source 재탐색
5. source 검증
6. 공고 수집/증분
7. 품질 게이트
8. `master` 반영
9. Google Sheets 동기화

## 6. 배포 후 확인

새 시트에서 아래를 본다.

1. `기업선정 탭`
- 현재 후보군과 `approved/candidate/rejected`

2. `기업근거 탭`
- 회사별 근거 raw/evidence

3. `staging 탭`
- 최신 수집 상태

4. `master 탭`
- 최종 반영본

확인할 핵심 수치:
- `approved`
- `candidate`
- `staging/master` 행 수
- 최근 run의 `automation_ready`

## 7. 운영 방식

반복 운영 명령은 동일하다.

```bash
cd /Users/junheelee/Desktop/sctaper_p1/jobs_market_v2
source .venv/bin/activate
python -m jobs_market_v2.cli run-collection-cycle
```

현재 의미:
- 현재 확보한 공식 출처 universe 안에서는 무인 운영 가능
- 그 범위 안에서 후보군 재확장과 재평가가 계속 전진
- approved 기업 공고는 최신성 유지

## 7-1. GitHub Actions managed 운영

VM 없이 운영하려면 GitHub Actions를 스케줄러로만 쓰고, runtime state는 `automation-state` branch에 저장한다.

핵심 원칙:
- GitHub-hosted runner는 휘발성이므로 `runtime`을 그대로 믿지 않는다.
- GitHub repo에는 `jobs_market_v2` 실제 코드 트리를 그대로 두고, runner는 checkout된 코드를 직접 실행한다.
- 매 런 시작 전 `automation-state`의 `runtime_state.tar.gz`를 복원한다.
- 런이 성공하면 새 runtime bundle을 같은 branch에 덮어쓴다.
- 런이 실패하면 마지막 성공 bundle은 유지한다.
- 다음 런이 마지막 성공 상태에서 자동 복구 시도한다.

필수 secret:
- `GOOGLE_SHEETS_SPREADSHEET_ID`
- `GOOGLE_SERVICE_ACCOUNT_JSON`

선택 secret:
- `GEMINI_API_KEY`
- `SLACK_WEBHOOK_URL`

secret 값 초안은 아래 파일을 복사해서 정리하면 된다.

```bash
cp config/github_actions_secrets.env.example .env.github-actions.local
```

## 8. 완전 초기화가 필요한 경우

권장하지 않지만, 정말 처음부터 다시 시작하고 싶다면:
- 새 시트만 바꾸는 것으로는 부족하다
- runtime과 progress/cache도 비워야 한다

하지만 이건 현재 추천 경로가 아니다.

현재 추천 경로는:
- **새 시트**
- **runtime 유지**
- **one-line 운영 시작**

## 9. 최종 판단

현재 상태는 아래 의미에서 production 배포 가능하다.

- `현재 확보한 공식 출처 universe 안의 bounded production`

아직 별도 장기 과제인 것:
- `웹 전체 신규 최상위 공식 카탈로그 자동 발굴`

이 장기 과제는 현재 bounded production 개시를 막는 blocker로 보지 않는다.
