# Release Blockers

작성일: 2026-04-02

## 2026-04-02 23:30 KST update

### 해결됨

- 긴 description 본문 안의 조건부 마감 안내만으로 active 공고를 `closed`로 오인하던 경로를 줄였다.
  - `채용 완료 시 조기 마감`, `인재풀 활용` 같은 문구는 이제 description 내부에서 자동 `closed` 근거가 되지 않는다.
  - explicit `status / listing_context / title`의 닫힘 신호만 우선 닫힘으로 본다.
- `고위드 GreetingHR`는 실제 active source로 전환됐다.
  - source: `https://gowid.career.greetinghr.com`
  - before:
    - `parsed 11 / accepted 0`
    - `last_active_job_count = 0`
  - after:
    - `parsed 11 / accepted 1`
    - `last_active_job_count 0 -> 1`
  - publish/sync/doctor:
    - `update-incremental-20260402232623`
    - `promote-staging-20260402232625`
    - `sync-sheets --target master/staging` 성공
    - `doctor` 통과

### 미해결

- 이번 턴은 `active source` 전환에는 성공했지만 `master` 증분은 없었다.
  - current safe:
    - `master = 100`
    - `staging = 100`
    - `verified_source_success = 546`
    - `active_gt_0_sources = 41`
    - `quality_score_100 = 99.08`
- 즉 `verified_success -> active_gt_0`는 전진했지만, `active_gt_0 -> master` 성장 강도는 여전히 약하다.
- metadata blocker도 그대로 남아 있다.
  - `경력수준_표시 blank = 12`
  - `우대사항_표시 blank = 15`

### 다음 1순위

1. `알체라` 또는 `가치랩스`를 다음 실제 `master` 증가 source로 전환한다.
2. `당근/데이터메이커/뷰노/서울로보틱스`의 `경력수준_표시` blank를 줄인다.
3. `센드버드/몰로코/엔젤로보틱스`의 `우대사항_표시` blank를 줄인다.

## 2026-04-02 22:30 KST update

### 해결됨

- `메디컬에이아이` generic HTML detail hydration을 범용 경로로 보강했다.
  - `div.title + div.content` layout을 generic detail field pair로 읽는다.
  - `Recruit` 같은 shell title이 detail title 또는 listing title을 덮지 못하게 막았다.
  - `직무내용`, `직무 상세`, `지원 자격`을 실제 section heading으로 인식한다.
- `메디컬에이아이`는 실제 active growth source로 전환됐다.
  - source: `https://medicalai.com/en/recruit`
  - bounded publish 결과:
    - `master 98 -> 100`
    - `staging 98 -> 100`
    - `new_job_count = 2`
    - `net_active_job_delta = 2`
    - `active_gt_0_sources 39 -> 40`
    - `quality_score_100 = 99.08`
  - 시트 반영:
    - `master sheet export = 100`
    - `staging sheet export = 100`
  - `sync-sheets --target master/staging` 성공
  - `doctor` 통과

### 미해결

- 아직 `자율 증분 배포` 수준은 아니다.
  - current safe:
    - `master = 100`
    - `staging = 100`
    - `verified_source_success = 546`
    - `active_gt_0_sources = 40`
    - `quality_score_100 = 99.08`
- 즉 `verified_success -> active_gt_0 -> master` 번역률은 여전히 낮다.
- metadata blocker는 그대로 남아 있다.
  - `경력수준_표시 blank = 12`
  - `우대사항_표시 blank = 15`
- 특히 이번 턴은 growth는 성공했지만 `경력수준_표시`는 `11 -> 12`로 오히려 1 증가했다.
  - 따라서 metadata quality는 아직 release-ready라고 말할 수 없다.

### 다음 1순위

1. `알체라` 또는 `가치랩스`를 다음 `active source`로 전환한다.
2. `당근/데이터메이커/뷰노/서울로보틱스`의 `경력수준_표시` blank를 다시 줄인다.
3. `센드버드/몰로코/엔젤로보틱스`의 `우대사항_표시` blank를 줄인다.

## 2026-04-02 22:50 KST update

### 해결됨

- `채용트랙_표시 blank`를 schema 정책 차원에서 닫았다.
  - 특수 트랙 신호가 없으면 `일반채용`으로 기본 분류
  - `구분요약_표시`에는 기본값을 넣지 않아 요약 노이즈는 늘리지 않음
  - latest published:
    - `채용트랙_표시 blank = 0`
    - `hiring_track_blank_ratio = 0.0`
- blank hotspot source bounded refresh를 실제 published와 시트까지 반영했다.
  - 대상:
    - `당근`
    - `마키나락스`
    - `인터엑스`
    - `데브시스터즈`
    - `센드버드`
    - `카카오모빌리티`
    - `고위드`
  - 결과:
    - `master 91 -> 98`
    - `staging 91 -> 98`
    - `master sheet export = 98`
    - `staging sheet export = 98`
    - `quality_score_100 = 99.07`
    - `sync-sheets --target staging/master` 성공
    - `doctor` 통과

### 미해결

- 아직 `자율 증분 배포` 수준은 아니다.
  - current safe:
    - `master = 98`
    - `staging = 98`
    - `verified_source_success = 546`
    - `active_gt_0_sources = 39`
    - `quality_score_100 = 99.07`
- 즉 `verified_success -> active_gt_0 -> master` 번역률은 여전히 낮다.
- metadata blocker는 이제 아래 둘로 좁혀졌다.
  - `경력수준_표시 blank = 11`
  - `우대사항_표시 blank = 15`
- 다음 실제 growth 후보는 아직 publish 전이다.
  - `메디컬에이아이`
  - `알체라`
  - `가치랩스`

### 다음 1순위

1. `메디컬에이아이`를 active growth source로 전환한다.
2. `당근/데이터메이커/뷰노/서울로보틱스`의 `경력수준_표시` blank를 줄인다.
3. `센드버드/몰로코/엔젤로보틱스`의 `우대사항_표시` blank를 줄인다.

## 2026-04-02 22:40 KST update

### 해결됨

- `폴라리스오피스 GreetingHR`를 실제 active growth source로 전환했다.
  - source: `https://polarisofficerecruit.career.greetinghr.com`
  - `parsed 3 / accepted 1`
  - published 반영:
    - `master 85 -> 86`
    - `staging 85 -> 86`
    - `active_gt_0_sources 40 -> 41`
- Google Sheets sync timeout 1회로 전체가 바로 죽던 문제를 완화했다.
  - `src/jobs_market_v2/sheets.py`에 retry 추가
  - retry 회귀 테스트 통과
  - 재실행 결과 `sync-sheets --target master/staging` 둘 다 성공
- generic HTML source의 `closed notice false active` leak를 막았다.
  - `채용완료 / 채용 마감 / 지원 마감` 신호를 normalize 단계에서 감지해 drop
  - `Meissa` probe 결과:
    - `parsed 41 / accepted 0`
  - 즉 닫힌 AI/data notice를 active 공고로 잘못 세는 경로는 차단됐다.

### 미해결

- 아직 `자율 증분 배포` 수준은 아니다.
  - current safe:
    - `master = 86`
    - `staging = 86`
    - `verified_source_success = 546`
    - `active_gt_0_sources = 41`
    - `quality_score_100 = 99.02`
- 즉 `verified_success -> active_gt_0 -> master` 번역률은 여전히 낮다.
- metadata blank도 남아 있다.
  - `경력수준_표시 blank = 11`
  - `채용트랙_표시 blank = 32`
  - `우대사항_표시 blank = 14`
- `알체라`처럼 외부 채용호스트로 튀는 approved source는 아직 완전 일반화되지 않았다.

### 다음 1순위

1. `알체라` external recruit host follow를 닫아 `active_gt_0 source`를 하나 더 늘린다.
2. `greenhouse` metadata normalizer로 blank hotspot을 줄인다.
3. `verified_success -> active_gt_0` 전환률을 높이는 source family를 계속 발굴한다.

## 2026-04-02 03:35 KST update

### 해결됨

- over-aggressive near-duplicate collapse를 되돌리고, `명시적 차이(트랙/레벨/학위/계약/위치)`가 있으면 접지 않는 보수 정책으로 바꿨다.
- 잘못 빠졌던 published URL `5건`을 snapshot union으로 복구했다.
  - `마키나락스` 3건
  - `크래프톤` 1건
  - `클로버추얼패션` 1건
- 현재 published / sheet export는 다시 일치한다.
  - `runtime/master_jobs.csv = 104`
  - `runtime/staging_jobs.csv = 104`
  - `runtime/sheets_exports/master/master_탭.csv = 104`
  - `runtime/sheets_exports/staging/staging_탭.csv = 104`
- snapshot 대비 빠진 published URL은 현재 `0`이다.
- exact duplicate는 계속 `0`이다.
  - `job_url_dupes = 0`
  - `job_key_dupes = 0`
- Gemini duplicate adjudication은 이제 broad collapse가 아니라 `gray-zone audit` 용도로 좁게 쓰인다.
  - actual audit verdict:
    - `당근 학사/석사/전환형 인턴`: `distinct`
    - `몰로코 Senior / II`: `distinct`
    - `하이퍼커넥트 Senior / 전문연구요원`: `distinct`

### 부분 해결

- `상세본문_분석용`이 매우 비슷한 행들은 여전히 있다.
- 다만 지금은 `거의 같은 본문`만으로 접지 않고, explicit variant를 우선 보존한다.
- 즉 현재 정책은 `false positive 삭제를 막는 쪽`으로 다시 선회한 상태다.

### 미해결

- `gray-zone duplicate`는 아직 완전히 0이 아니다.
  - 가장 대표적인 남은 회색지대:
    - `쿠팡`
    - `Staff Machine Learning Engineer (Eats Search & Discovery)`
    - `Sr. Staff Machine Learning Engineer (Eats Search & Discovery)`
  - Gemini actual adjudication은 `None`으로 끝났고, 아직 자동 collapse 대상으로 고정하지 않았다.
- 서버 배포 blocker는 여전히 남아 있다.
  - `approved/source -> master` 번역률 약함
  - direct HTML hiring source recall 부족
  - metadata blank
    - `경력수준_표시 blank = 24`
    - `채용트랙_표시 blank = 31`
    - `우대사항_표시 blank = 15`
  - bookkeeping / smoke isolation 미완

## 판정

- 현재 상태는 **서버 배포/상시 자동운영 수준에 미달**한다.
- 이유는 자동화 부재가 아니라, 아래 `데이터 품질 / 증분 / 중복` blocker가 아직 남아 있기 때문이다.

## 해결됨

- `master_rows` 성장은 다시 살아났다.
  - `94 -> 105`
- `approved_company_count`도 다시 증가했다.
  - `138 -> 140`
- `quality_score_100`은 `99+`를 유지하고 있다.
  - latest safe: `99.47`
- 완전 동일 중복은 current `master` 기준으로 정리돼 있다.
  - `job_url_dupes = 0`
  - `job_key_dupes = 0`
- `실질 중복`도 일부 실제로 제거했다.
  - current `master`: `same_text_groups 3 -> 0`
  - 제거된 대표 케이스:
    - `마키나락스 / AI Research Engineer (Junior)` vs `[전문연구요원] AI Research Engineer`
    - `서울로보틱스 / Senior ...` vs `Director ...`
    - `인터엑스 / [서울] ...` vs `[울산] ...`
- 핵심 표시 필드는 비어 있지 않다.
  - `공고제목_표시 blank = 0`
  - `구분요약_표시 blank = 0`
  - `상세본문_분석용 blank = 0`
- `runs.csv` 깨진 조각 줄 때문에 시트 sync가 죽던 문제는 복구했다.
  - 손상 줄 삭제
  - `read_csv_or_empty()` fallback 추가
  - `sync-sheets --target staging/master` 재성공 확인

## 부분 해결

- 유사 공고 구분을 위한 표시 보강은 일부 들어갔다.
  - 제목 보조 구분
  - 구분요약
  - 일부 경력/트랙/직무초점 표시
- published state recovery 절차는 있다.
  - `run-collection-cycle`
  - `collect-company-evidence`
  - `screen-companies`
  - `sync-sheets`
- 그러나 위 둘 다 아직 완전 자동/완전 신뢰 수준은 아니다.

## 미해결

### 1. 실질 중복 제거

- 완전 동일 중복뿐 아니라 `동일 본문 practical duplicate`도 현재 `master` 기준으로는 `0`까지 줄였다.
- 따라서 이 항목은 더 이상 최상위 blocker가 아니다.
- 다만 앞으로도 direct HTML/ATS 변형에서 같은 유형이 새로 생기지 않도록 회귀 감시는 계속 필요하다.

### 2. `approved/source -> master` 번역률

- latest safe:
  - `approved = 140`
  - `verified_source_success = 545`
  - `master = 103`
- 승인/검증 규모에 비해 최종 공고 수가 아직 적다.

### 3. direct HTML hiring source 파싱

- ATS보다 direct HTML / direct hiring 계열에서 `master` 번역률이 약하다.
- 다만 이번 턴에서 `html detail follow`를 추가해, 리스트에서 detail URL만 찾고 본문을 못 읽던 구조는 코드상 보강했다.
- 대표 probe:
  - `네이버 공식 채용`
  - `[네이버랩스] Generative AI Research Engineer`
  - detail fetch 후 `주요업무/자격요건/우대사항/채용트랙(박사/석사)`까지 복원 가능함을 직접 확인
- 아직 blocker로 남기는 이유:
  - 이 보강이 **최신 published runtime 전체**에 다시 반영되어 `master` 증가로 번역되는 것까지는 아직 live 재검증을 끝내지 못했다.

### 4. 분석 필드 recall 부족

- current `master` 기준:
  - `경력수준_표시 blank = 25`
  - `채용트랙_표시 blank = 32`
  - `직무초점_표시 blank = 5`
  - `주요업무_표시 blank = 5`
  - `자격요건_표시 blank = 5`
  - `우대사항_표시 blank = 14`

### 5. bookkeeping / 운영 기록 동기화

- live 실행 현실과 `automation_status.json`이 한 템포 어긋난다.
- 실제 회차와 기록 상태가 항상 일치하지 않는다.
- `runs.csv`가 부분 쓰기되면 sync가 실패하는 경로는 이번에 완화했지만, 근본 원인인 동시/중단 쓰기 자체는 아직 완전히 해결되지 않았다.

### 6. smoke / notebook isolation

- smoke가 production runtime을 흔드는 문제가 아직 남아 있다.
- 복구는 가능하지만, 분리는 아직 완성되지 않았다.

## 다음 1순위

1. direct HTML hiring source에서 `approved/source -> master` 번역률을 올린다.
2. `채용트랙/경력/우대사항` recall을 높여 서비스형 가독성을 높인다.
3. bookkeeping과 smoke isolation을 서버 배포 기준으로 정리한다.

## 2026-04-02 02:35 KST update

### 해결됨

- generalized near-duplicate collapse를 published 경로에 실제 반영했다.
- `promote-staging` 성공:
  - `dropped_low_quality_job_count = 5`
  - `promoted_job_count = 98`
- `sync-sheets --target master` 성공
- published/runtime/sheet export가 다시 일치한다.
  - `runtime/master_jobs.csv = 98`
  - `runtime/staging_jobs.csv = 98`
  - `runtime/sheets_exports/master/master_탭.csv = 98`
- current published duplicate state:
  - `job_url_dupes = 0`
  - `job_key_dupes = 0`
  - deterministic re-filter on `master`:
    - `refiltered_rows = 98`
    - `dropped_on_master = 0`
- Gemini gray-zone duplicate adjudication을 `quality.py` publish 단계에 연결했다.
  - `enable_gemini_duplicate_adjudication = true`
  - cache file: `runtime/logs/gemini_duplicate_adjudication_cache.json`

### 부분 해결

- `누가 봐도 거의 같은 공고`는 상당수 제거됐다.
- 현재 `master`에서 높은 유사도로 남는 pair는 `3개`뿐이다.
  - `당근` 학사 vs 전환형 인턴
  - `쿠팡` Staff vs Sr. Staff MLE
  - `몰로코` Senior Applied Scientist vs Applied Scientist II
- 이 3개는 현 시점에선 `실질 중복`보다 `레벨/트랙이 다른 별도 포지션`으로 보는 쪽이 더 안전하다.

### 미해결

- 최상위 blocker는 이제 `중복`보다 `approved/source -> master` 번역률이다.
  - current published:
    - `approved = 140`
    - `verified_source_success = 555`
    - `master = 99`
- direct HTML hiring source는 핵심 parser gap을 메웠지만, published growth로 얼마나 이어지는지는 추가 cycle 검증이 더 필요하다.
- published 품질 공란:
  - `경력수준_표시 blank = 23`
  - `채용트랙_표시 blank = 31`
  - `직무초점_표시 blank = 3`
  - `주요업무_표시 blank = 6`
  - `자격요건_표시 blank = 5`
  - `우대사항_표시 blank = 15`
- bookkeeping / smoke isolation은 계속 미해결이다.

## 2026-04-02 02:56 KST update

### 해결됨

- `html_page` growth translation을 위해 source-scan policy를 수정했다.
  - strong listing candidate만 scout
  - active pin limit 확대
  - html listing Gemini budget 확대
  - strong listing path는 content 신호가 약해도 Gemini probe 허용
- 이 정책이 실제 published growth로 번역됐다.
  - `update-incremental-20260402025003`
  - `baseline_job_count = 98`
  - `staging_job_count = 99`
  - `new_job_count = 1`
  - `net_job_delta = 1`
  - `net_active_job_delta = 1`
- `promote-staging-20260402025519` 성공
  - `promoted_job_count = 99`
- `sync-sheets-20260402025538` 성공
  - Google Sheets sync 완료

### 부분 해결

- growth engine은 다시 전진했다.
  - `master 98 -> 99`
- `source_collection_progress.json`도 새 policy 기준으로 갱신됐다.
  - `policy_version = v8`
  - `source_scan_resume_strategy = policy_reset`
  - `source_scan_next_offset = 128`
- 다만 증가폭은 아직 작다.

### 미해결

- 최상위 blocker는 여전히 `approved/source -> master` 번역률이다.
  - `approved = 140`
  - `verified_source_success = 555`
  - `master = 99`
- published 품질 공란:
  - `경력수준_표시 blank = 23`
  - `채용트랙_표시 blank = 31`
  - `직무초점_표시 blank = 4`
  - `주요업무_표시 blank = 6`
  - `자격요건_표시 blank = 5`
  - `우대사항_표시 blank = 15`
- `quality_score_100 = 99.31`로 기준은 넘지만, 정보량은 아직 더 채워야 한다.

## 2026-04-02 04:05 KST 업데이트

### 해결됨

- `user-visible near-duplicate` 기준으로 남아 있던 pair를 다시 publish에 반영했다.
  - 하이퍼커넥트:
    - `Senior Machine Learning Engineer (Match Group AI)`
    - `Machine Learning Engineer (Match Group AI | 전문연구요원 편입/전직 가능)`
  - 크래프톤:
    - `[AI Research Div.] [전문연구요원] Research Scientist - Foundation Model (2년 이상)`
    - `[AI Research Div.] Research Scientist - Foundation Models (2년 이상 / 계약직)`
- 기준 변경:
  - 정규화 body score만 보지 않고, 사용자가 실제로 보는 `상세본문_분석용` 유사도를 duplicate signal로 직접 사용
  - 제목 stem mismatch가 있어도 `title_seq`가 매우 높으면 same-title-family로 취급
  - gray-zone pair는 Gemini duplicate adjudication payload에 `visible_detail_seq`를 함께 넘김
- 결과:
  - `promote-staging`: `98 -> 97`
  - `master_jobs.csv = 97`
  - `staging_jobs.csv = 97`
  - `master_탭.csv = 97`
  - `staging_탭.csv = 97`
  - `하이퍼커넥트 pairs>=0.85 = 0`
  - `크래프톤 pairs>=0.85 = 0`
  - `all_pairs_ge_0.9 = 0`

### 부분 해결

- exact duplicate는 계속 `0`이다.
  - `job_url_dupes = 0`
  - `job_key_dupes = 0`
- duplicate 기준은 이전보다 사용자-visible 본문과 더 잘 맞는다.

### 미해결

- 최상위 blocker는 여전히 `approved/source -> master` 번역률이다.
  - `approved = 140`
  - `verified_source_success = 555`
  - `master = 97`
- 품질 공란도 여전히 남아 있다.
  - `경력수준_표시 blank = 24`
  - `채용트랙_표시 blank = 31`
  - `우대사항_표시 blank = 15`
- bookkeeping / smoke isolation도 아직 미완이다.

## 2026-04-02 04:20 KST 업데이트

### 해결됨

- 분석 목적에 맞춰 duplicate 정책을 `대표 1건` 기준으로 전환했다.
  - 같은 회사
  - 같은 직무 family
  - 같은 제목 family 또는 매우 강한 title match
  인 경우, 레벨/트랙 차이가 있더라도 가장 정돈된 대표격 1건만 남긴다.
- 결과:
  - `promote-staging`: `97 -> 69`
  - `master_jobs.csv = 69`
  - `staging_jobs.csv = 69`
  - `master_탭.csv = 69`
  - `staging_탭.csv = 69`
  - `pairs_ge_0.85 = 0`
- 하이퍼커넥트, 크래프톤, 몰로코, 당근 등에서 같은 회사/같은 job family 변형은 대표 1건 정책으로 접혔다.

### 부분 해결

- metadata blank도 줄었다.
  - `경력수준_표시 blank = 15`
  - `채용트랙_표시 blank = 23`
  - `우대사항_표시 blank = 9`

### 미해결

- 서버 배포 blocker는 여전히 남아 있다.
  - `approved/source -> master` 번역률이 낮다
  - `approved = 140`
  - `verified_source_success = 555`
  - `master = 69`
- 즉 중복 정리는 많이 전진했지만, 성장 번역률과 direct HTML/ATS recall은 아직 미완이다.

## 2026-04-02 05:05 KST 업데이트

### 해결됨

- `parsed > 0 but role=NONE` 때문에 버리던 recruiter source를 실제로 살렸다.
  - `신세계I&C`
    - `[신세계아이앤씨] 데이터분석(데이터사이언스) 인재모집(채용 시 마감)` -> `데이터 사이언티스트`
    - `[신세계아이앤씨] AI서비스개발 직무 인재모집(채용 시 마감)` -> `인공지능 엔지니어`
- 코드 변경:
  - `constants.py`
    - `데이터분석`, `데이터사이언스`, `AI서비스개발` title signal 보강
  - `collection.py`
    - `classify_job_role` 보강
    - heuristic miss일 때 Gemini role salvage 경로 추가
    - collect loop에 `gemini_role_salvage_max_calls_per_run` budget 연결
  - `gemini.py`
    - `maybe_salvage_job_role(...)` 추가
  - `pipelines.py`
    - subset `registry_frame`로 incremental을 돌려도 full `source_registry.csv`를 보존하는 bounded merge helper 추가
- published 반영:
  - `update_incremental` subset run
  - `promote-staging`
  - `sync-sheets --target staging`
  - `sync-sheets --target master`
- 결과:
  - `master = 69 -> 71`
  - `staging = 69 -> 71`
  - `source_registry: 신세계I&C last_active_job_count = 2`
  - `master_탭.csv = 71`

### 부분 해결

- `API 활용 확대`는 시작했다.
  - duplicate adjudication에 이어 role salvage 경로까지 Gemini를 연결했다.
  - 이번 live probe에서는 `신세계I&C`가 heuristic으로 바로 잡혀 `gemini_calls_used = 0`이었지만, heuristic miss source를 위한 일반화 경로는 코드에 들어갔다.

### 미해결

- 최상위 blocker는 여전히 `approved/source -> master` 번역률이다.
  - `approved = 140`
  - `verified_source_success = 555`
  - `master = 71`
- direct HTML / greetinghr / recruiter의 저수율 source는 아직 많다.
  - `verified success = 545` 중 `active>0 source = 30` 수준
- metadata blank도 남아 있다.
  - `경력수준_표시 blank = 15`
  - `채용트랙_표시 blank = 24`
  - `우대사항_표시 blank = 10`

## 2026-04-02 06:20 KST

### 해결됨

- generic `html_page` detail source를 single-job detail로 직접 파싱하는 fallback을 추가했다.
  - `collection.py`
    - generic nav anchor만 있는 경우 `single detail page` 판정 후 대표 공고 1건으로 변환
    - `AI개발자` 계열 title signal 보강
    - sparse detail block일 때 full page 본문으로 확장
    - collection path에서 Gemini analysis refinement를 다시 활성화
  - `gemini.py`
    - analysis refinement payload가 `description_text` full context를 유지하도록 수정
    - stale display cache invalidation (`v3`)
- direct probe에서 `데이터메이커` detail pages가 실제 공고로 번역되기 시작했다.
  - `/job-openings/51` -> `AI개발자_프론트엔드` -> `인공지능 엔지니어`
  - `/job-openings/52` -> `AI개발자_백엔드` -> `인공지능 엔지니어`
  - `/job-openings/53` -> `AI개발자_ML개발자` -> `인공지능 엔지니어`
- bounded publish로 `데이터메이커 /job-openings/53` 1건을 실제 published까지 올렸다.
  - `master = 71 -> 72`
  - `staging = 71 -> 72`
  - `quality_gate_passed = true`
  - `promoted_job_count = 72`
  - Google Sheets `master/staging` sync 성공

### 부분 해결

- `데이터메이커 /job-openings/51,52`는 이제 direct probe 기준 `accepted`까지는 된다.
- 다만 이 두 건은 `자격요건/우대사항` recall이 아직 약해, 셋을 한 번에 bounded publish하면 dataset quality score를 `99` 아래로 끌어내린다.

### 미해결

- 여전히 최상위 blocker는 `approved/source -> master` 번역률이다.
  - `approved_sources = 520`
  - `verified_sources = 545`
  - `active_gt_0_sources = 34`
  - `master = 72`
- 즉 반복 실행은 가능하지만, 아직 `자율 증분 배포` 수준은 아니다.
- `데이터메이커 /job-openings/51,52`처럼 parsed/accepted는 되지만 metadata recall이 부족한 source를 더 끌어올려야 한다.

## 2026-04-02 07:00 KST

### 해결됨

- `데이터메이커 /job-openings/51,52,53` 3건을 모두 published까지 올렸다.
  - bounded subset run 결과:
    - `master = 72 -> 74`
    - `staging = 72 -> 74`
    - `new_job_count = 2`
    - `changed_job_count = 1`
    - `quality_gate_passed = true`
    - Google Sheets `master/staging` sync 성공
- `경력수준_표시` 추론을 `경력 5년 이상` 외의 더 흔한 한국어 패턴까지 확장했다.
  - `2년 이상의 웹 백엔드 개발 경험`
  - `(2년 이상 / 계약직)`
- 그 결과 published 기준으로:
  - `경력수준_표시 blank = 18 -> 10`
- Datamaker published rows:
  - `/51` -> `경력 2년+`
  - `/52` -> `경력 2년+`
  - `/53` -> `박사 / 석사`

### 부분 해결

- direct HTML detail translation은 이제 Datamaker 3건까지는 안정적으로 translated/published 된다.
- 다만 이는 아직 bounded subset 타격 결과이며, 전체 registry를 반복 실행했을 때 같은 비율로 자동 증분이 이어진다고 보긴 어렵다.

## 2026-04-02 20:51 KST

### 해결됨

- `Angel Robotics /recruit/notice` family split 2건이 모두 published에 남도록 quality path를 수정했다.
  - 영문 기술 bullet이 analysis/quality normalization에서 사라져 `AI Researcher` 또는 `Data Scientist`가 low-quality drop으로 빠지던 문제였다.
  - 현재 published:
    - `master = 84`
    - `staging = 84`
    - `Angel rows = 2`

### 안 됨

- 아직 배포 가능한 autonomous incremental 수준은 아니다.
  - `approved_sources = 521`
  - `verified_sources = 546`
  - `active_gt_0_sources = 39`
  - `master = 84`
- metadata blank:
  - `경력수준_표시 blank = 11`
  - `채용트랙_표시 blank = 30`
  - `우대사항_표시 blank = 14`

### 다음 1순위

- `Meissa / ByteSize / Suresoft`를 bounded subset으로 다시 검증해 `active_gt_0_source`를 늘린다
- `당근 / 센드버드 / 크래프톤` metadata blank를 줄여 quality margin을 더 확보한다

### 미해결

- 최상위 blocker는 여전히 `approved/source -> master` 번역률이다.
  - `verified_sources = 545`
  - `active_gt_0_sources = 34`
  - `master = 74`
- zero-active high-value targets:
  - `https://webzen.recruiter.co.kr/app/jobnotice/list`
  - `https://medipixel.io/careers/jobs`
  - `https://www.rsupport.com/ko-kr/career/recruit`
  - `https://www.openedges.com/positions`
  - `https://www.angel-robotics.com/ko/recruit/permanent`
- metadata blank 상위 병목:
  - `당근`
  - `센드버드`
  - `크래프톤`
  - 주된 축은 `경력수준_표시`, `채용트랙_표시`, `우대사항_표시`

## 2026-04-02 10:50 KST

### 해결됨

- `스켈터랩스 https://www.skelterlabs.com/career`를 bounded subset publish로 실제 성장으로 번역했다.
  - `master = 74 -> 78`
  - `staging = 74 -> 78`
  - `new_job_count = 4`
  - `net_active_job_delta = 4`
  - Google Sheets `master/staging` sync 성공
- post-publish runtime:
  - `verified_sources = 545`
  - `active_gt_0_sources = 35`
  - `master = 78`

### 부분 해결

- zero-active source 중 일부는 parser gap이 아니라 `AI/data role miss` 또는 `non-target job family` 문제라는 점을 구분했다.
  - `웹젠 recruiter`는 fetch 기준 `68 jobs`가 나오므로 fetch bug가 아니라 AI/data accept 문제에 가깝다.
- 즉 다음 패치는 `verified 0-active` 전부를 하나의 병목으로 다루면 안 되고,
  - `fetch OK / accept 0`
  - `parse 0`
  - `landing only`
  로 나눠야 한다.

### 미해결

- 아직 `자율 증분 배포` 수준은 아니다.
  - `verified_sources = 545`
  - `active_gt_0_sources = 35`
  - `master = 78`
- high-value remaining parser families:
  - `careers.team / flex.team` (`오픈엣지테크놀로지`)
  - zero-yield HTML listing/detail (`알서포트`는 parse gap이 있으나 AI/data 성장으로 바로 이어질 가능성은 낮음)
- metadata blank는 여전히 남아 있다.
  - `경력수준_표시 blank = 11`
  - `채용트랙_표시 blank = 27`
  - `우대사항_표시 blank = 11`

## 2026-04-02 07:20 KST

### 해결됨

- `career/recruit`형 HTML 아카이브에서 상단 네비 대신 실제 상세 공고 링크를 일반적으로 뽑는 parser 보강을 넣었다.
  - same-prefix detail path를 공고 후보로 인식
  - detail links가 있으면 `채용`, `채용공고`, self-link 같은 generic nav를 제거
- direct probe:
  - `https://www.rsupport.com/ko-kr/career/recruit` -> parsed `7`
  - 기존에는 `채용`, `채용공고` 같은 generic nav만 뽑히던 유형이었다

### 부분 해결

- parser 일반화는 성공했지만, `알서포트`는 현재 parsed `7` 중 AI/data accepted jobs가 `0`이다.
- 즉 이번 수정은 future growth 후보를 살리는 범용 개선이지만, 이번 회차의 `master` 증분에는 바로 번역되지 않았다.

### 미해결

- 여전히 자율 증분 배포 blocker는 `approved/source -> master` 번역률이다.
- `웹젠`은 현재 recruiter parser가 아니라 AI/data role 부재 쪽이 더 크다.
- `비상교육`도 recruiter no-state list는 나오지만 현재 시점에서는 오래된 closed notice가 주력이다.
- 현재 더 가치 있는 zero-active target은 여전히:
  - `https://medipixel.io/careers/jobs`
  - `https://www.openedges.com/positions`
  - `https://www.angel-robotics.com/ko/recruit/permanent`
  - 그리고 metadata blank 큰 축인 `당근`, `센드버드`, `크래프톤`

## 2026-04-02 20:10 KST

### 해결됨

- `careers.team / flex.team` public recruiting API parser를 추가했다.
  - `__NEXT_DATA__`에서 `customerIdHash`와 `RECRUITING_SITE_ROOT_DOMAIN`을 읽는다.
  - `https://flex.team/api-public/v2/recruiting/customers/{customerIdHash}/sites/job-descriptions`
  - `.../job-descriptions/{jobDescriptionIdHash}/details`
  - `LEXICAL_V1` detail payload를 HTML로 변환해 기존 section extraction 경로와 연결했다.
- `OpenEdges`는 더 이상 parser blocker가 아니다.
  - parsed `13`
  - accepted `1`
  - published `+1`
- published 상태와 시트 반영도 끝냈다.
  - `master = 79`
  - `staging = 79`
  - Google Sheets `master/staging` sync 성공
- source registry main state도 subset 결과를 병합해 맞췄다.
  - `https://www.openedges.com/positions`
  - `last_active_job_count = 1`
  - `active_gt_0_sources = 36`

### 부분 해결

- `OpenEdges`에서 실제 AI/data row 1건은 publish로 번역됐지만, `careers.team / flex.team` 계열 전체가 자율 증분으로 일반화됐다고 보기는 아직 이르다.
- `Angel Robotics`는 `/permanent`가 아니라 `/notice` RSC가 실제 공고 소스라는 점까지는 확인했다.

### 미해결

- 아직 `자율 증분 배포` 수준은 아니다.
  - `verified_sources = 545`
  - `active_gt_0_sources = 36`
  - `master = 79`
- metadata blank는 여전히 남아 있다.
  - `경력수준_표시 blank = 11`
  - `채용트랙_표시 blank = 27`
  - `우대사항_표시 blank = 11`
- 다음 growth 후보는 여전히:
  - `Angel Robotics /recruit/notice` RSC parser
  - `Medipixel`
  - `당근`, `센드버드`, `크래프톤` metadata blank 축소

## 2026-04-02 20:25 KST

### 해결됨

- `html_page`로 발견된 custom-domain GreetingHR 채용 사이트를 기존 GreetingHR fetcher로 자동 우회시키는 경로를 추가했다.
  - 대상 예: `https://careers.devsisters.com`
  - 감지 조건:
    - `profiles.greetinghr.com` 또는 `opening-attachments.greetinghr.com`
    - `__NEXT_DATA__`의 `openings` query 존재
- direct probe에서 `데브시스터즈`는 실제로 `openings 60`개를 노출하고, 그중 AI/data family가 최소 `4개`임을 확인했다.
  - `[기술본부] Data Engineer (BI/DW)`
  - `[기술본부] Machine Learning Engineer (경력)`
  - `[기술본부] Software Engineer, Data Platform`
  - `Data Analyst`
- live single-source collection:
  - `collected_job_count = 2`
  - accepted:
    - `https://careers.devsisters.com/ko/o/200580`
    - `https://careers.devsisters.com/ko/o/158824`
- bounded publish 및 시트 반영까지 완료했다.
  - `master = 79 -> 81`
  - `staging = 79 -> 81`
  - `new_job_count = 2`
  - `net_active_job_delta = 2`
  - `Google Sheets master/staging sync 성공`
- source registry main state도 반영됐다.
  - `https://careers.devsisters.com`
  - `last_active_job_count = 2`
  - `verified_sources = 546`
  - `active_gt_0_sources = 38`

### 부분 해결

- `데브시스터즈`처럼 embedded GreetingHR custom domain은 이제 growth translation path가 생겼다.
- 하지만 `노을`처럼 `__NEXT_DATA__`를 쓰는 다른 custom hiring frontend는 아직 family-level 일반화가 끝나지 않았다.

### 미해결

- 아직 `자율 증분 배포` 수준은 아니다.
  - `verified_sources = 546`
  - `active_gt_0_sources = 38`
  - `master = 81`
- metadata blank는 여전히 남아 있다.
  - `경력수준_표시 blank = 11`
  - `채용트랙_표시 blank = 29`
  - `우대사항_표시 blank = 12`
- 다음 growth 후보는:
  - `노을` custom frontend
  - `Angel Robotics /recruit/notice` RSC parser
  - `당근`, `센드버드`, `크래프톤` metadata blank 축소

## 2026-04-02 20:45 KST

### 해결됨

- `embedded NineHire custom domain` 경로는 실제 growth source로 검증됐다.
  - `https://career.visang.com`
  - `source_type = html_page`
  - `last_active_job_count = 0 -> 2`
- bounded incremental publish까지 완료했다.
  - `master = 81 -> 83`
  - `staging = 81 -> 83`
  - `new_job_count = 1`
  - `net_active_job_delta = 1`
  - `Google Sheets master/staging sync 성공`
- verified success 대비 실제 active source도 소폭 전진했다.
  - `active_gt_0_sources = 38 -> 39`
- code cleanup:
  - `collection.py` 내부 NineHire helper 중복 정의를 제거하고 실제 사용 경로만 남겼다.

### 부분 해결

- `노을` 자체는 NineHire API 경로를 확인했다.
  - `https://api.ninehire.com/identity-access/homepage/recruitments`
  - `https://api.ninehire.com/recruiting/job-posting?recruitmentId=...`
- 하지만 현재 공개 채용이 AI/data 직군으로 잘 번역되지 않아 `master` 성장 source로는 약하다.

### 미해결

- 아직 `자율 증분 배포` 수준은 아니다.
  - `verified_sources = 546`
  - `active_gt_0_sources = 39`
  - `master = 83`
- metadata blank는 여전히 남아 있다.
  - `경력수준_표시 blank = 11`
  - `채용트랙_표시 blank = 30`
  - `우대사항_표시 blank = 13`
- 다음 growth 후보는:
  - `careers.suresofttech.com`
  - `recruit.meissa.ai`
  - `career.thebytesize.ai/jobs`
  - `Angel Robotics /recruit/notice`

## 2026-04-02 20:49 KST

### 해결됨

- `Angel Robotics /recruit/notice` family split parser bug를 수정했다.
  - 문제: family row를 role별로 split하면서 role-specific body만 남겨 `AI Researcher`가 quality filter에서 탈락했다.
  - 수정: role-specific body 앞단은 유지하고, parent posting context를 함께 싣도록 바꿨다.
- bounded incremental publish까지 완료했다.
  - `master = 83 -> 84`
  - `staging = 83 -> 84`
  - `new_job_count = 1`
  - `changed_job_count = 1`
  - `net_active_job_delta = 1`
  - `Google Sheets master/staging sync 성공`
- published 결과:
  - `https://www.angel-robotics.com/ko/recruit/notice/10#data-데이터`
  - `https://www.angel-robotics.com/ko/recruit/notice/10#artificial-intelligence-인공지능`
- doctor와 targeted pytest도 통과했다.

### 안 됨

- 아직 `자율 증분 배포` 수준은 아니다.
  - `verified_sources = 546`
  - `active_gt_0_sources = 39`
  - `approved_sources = 521`
  - `master = 84`
- metadata blank는 여전히 남아 있다.
  - `경력수준_표시 blank = 11`
  - `채용트랙_표시 blank = 30`
  - `우대사항_표시 blank = 14`

### 다음 1순위

- `recruit.meissa.ai`
- `career.thebytesize.ai/jobs`
- `careers.suresofttech.com`
- `당근 / 센드버드 / 크래프톤` metadata blank 축소

## 2026-04-02 23:43 KST

### 해결됨

- `가치랩스`를 실제 growth source로 전환했다.
  - `https://gazzi.ai/careers`
  - custom careers 목록에서 `/careers/<slug>` detail follow가 generic 규칙으로 동작한다.
  - live probe:
    - `parsed = 1`
    - `accepted = 1`
  - bounded publish 결과:
    - `master = 100 -> 101`
    - `staging = 100 -> 101`
    - `new_job_count = 1`
    - `Google Sheets master/staging sync 성공`
- `알체라`도 zero-active source에서는 벗어났다.
  - `https://www.alchera.ai/company/career`
  - Saramin relay posting이 generic detail hint로 인식된다.
  - live probe:
    - `parsed = 16`
    - `accepted = 2`
  - source registry 반영:
    - `last_active_job_count = 0 -> 2`
- 전체 active source도 전진했다.
  - `active_gt_0_sources = 41 -> 43`
- 품질은 유지됐다.
  - `quality_score_100 = 99.09`

### 부분 해결

- `알체라`는 now `verified -> active` 번역까지는 된다.
- 하지만 현재 relay rows는 아직 `master` publish까지는 못 올라간다.
- 즉 이번 턴에서 확보한 건:
  - `가치랩스`: active + master growth
  - `알체라`: active translation only

### 미해결

- 아직 `자율 증분 배포` 수준은 아니다.
  - `verified_sources = 541`
  - `active_gt_0_sources = 42`
  - `master = 101`
- `active source -> master row` 번역률이 여전히 낮다.
  - 현재 exact source 기준 남은 `active but no master` source는 `2개`다.
    - `알체라`
    - `거대한에이아이실험실 주식회사`
- display blank 자체는 닫혔지만, `미기재/별도 우대사항 미기재`를 줄여야 하는 질적 과제는 남아 있다.
  - `경력수준_표시 blank = 0`
  - `우대사항_표시 blank = 0`
  - `채용트랙_표시 blank = 0`
  - `경력수준_표시 = 미기재` rows = `7`
  - `우대사항_표시 = 별도 우대사항 미기재` rows = `14`

### 다음 1순위

- `알체라`처럼 source는 살아났지만 publish quality에서 떨어지는 family를 publish-ready로 끌어올리는 것
- `verified -> active -> master` 번역 실패 family를 개별 사례가 아니라 패턴군 단위로 정리하는 것
- `미기재/별도 우대사항 미기재`로 남는 rows를 줄일 수 있는 generic inference를 더 보강하는 것

## 2026-04-03 00:xx KST

### 해결됨

- `same-page multi-role custom html` family의 identity migration을 generic rule로 닫았다.
  - role split 시 `job_url = {base_url}#role-{stable-slug}`를 부여한다.
  - old plain-URL row가 `미발견/검증실패보류` 상태로 carry-forward될 때, fragmentized replacement가 있으면 legacy alias로 드롭한다.
- `거대한에이아이실험실 주식회사 / https://hugeailab.com/recruit`는 published/master 기준으로 이제 `AI 개발자(AI Model Developer/Researcher)` 1건만 남는다.
- 일반 서비스 delivery title(`서비스 개발자/Service Developer`)이 본문의 약한 AI/Data 언급만으로 Gemini role salvage를 타던 오분류를 generic rule로 차단했다.
- bounded incremental, `promote-staging`, `sync-sheets master/staging`, `doctor`까지 모두 재통과했다.

### 현재 상태

- `master = 103`
- `staging = 103`
- `verified_sources = 541`
- `active_gt_0_sources = 42`
- `active_but_no_master_sources = 0`
- `quality_score_100 = 99.3`
- display blank:
  - `경력수준_표시 blank = 0`
  - `우대사항_표시 blank = 0`
  - `채용트랙_표시 blank = 0`
- semantic fallback:
  - `경력수준_표시 = 미기재` rows = `8`
  - `우대사항_표시 = 별도 우대사항 미기재` rows = `14`

### 아직 안 됨

- 아직 `무인 자율 증분 배포 가능`이라고는 말하지 않겠다.
- exact source blocker는 사라졌지만, 남은 blocker는 `semantic recovery`다.
  - `경력수준 미기재` family
    - 연구/학위형 공고인데 숫자 연차가 없는 케이스
    - experienced signal은 강하지만 `N년` 표기가 없는 케이스
  - `우대사항 미기재` family
    - ATS requirements-only 공고
    - flat custom/recruiter detail에서 preferred heading이 납작하게 섞인 케이스

### 다음 1순위

- `경력수준_표시 = 미기재` 8건을 family-level rule로 줄이기
- `우대사항_표시 = 별도 우대사항 미기재` 14건 중 flat detail segmentation으로 줄일 수 있는 family를 먼저 닫기
- 위 두 semantic fallback이 generic rule로 얼마나 줄어드는지 재측정한 뒤, 그때 배포 가능 여부를 다시 판정하기

## 2026-04-03 02:xx KST

### 해결됨

- `경력수준_표시 = 미기재` family에 대해 generic 연구형 경험 추론을 넣고, semantic fallback source 14개를 bounded refresh해서 published까지 반영했다.
  - `전문연구요원/병역특례/Research Engineer/Scientist/R&D` 신호와 `석사/박사/논문/저널/학회/모델 연구·개발·고도화` 신호를 함께 보면 `경력`으로 승격한다.
  - 단일 필드가 아니라 `title + requirements + preferred + detail`의 `종합신호`도 경험레벨 판정에 사용한다.
- Huge AI Lab family를 포함한 current published는 계속 정상이다.
  - `same-page multi-role` identity migration 적용 유지
  - `active_but_no_master_sources = 0` 유지
- `promote-staging`, `sync-sheets master/staging`, `doctor`를 다시 통과했다.

### 현재 상태

- `master = 108`
- `staging = 108`
- `verified_sources = 541`
- `active_gt_0_sources = 41`
- `active_but_no_master_sources = 0`
- `quality_score_100 = 99.07`
- display blank:
  - `경력수준_표시 blank = 0`
  - `우대사항_표시 blank = 0`
  - `채용트랙_표시 blank = 0`
- semantic fallback:
  - `경력수준_표시 = 미기재` rows = `5`
  - `우대사항_표시 = 별도 우대사항 미기재` rows = `18`

### 아직 안 됨

- 아직 `완전한 무인 자율 증분 배포`라고는 말하지 않겠다.
- exact source blocker는 닫혔고 `경력수준 미기재`도 줄었지만, 남은 핵심은 `우대사항 미기재 family`다.
- 이 값이 `14 -> 18`로 늘어난 이유는 source refresh로 새 valid rows가 들어왔기 때문이지 blank regression은 아니다.
- 따라서 남은 본질은:
  - 실제로 preferred section이 없는 ATS family
  - flat custom/recruiter detail에서 preferred가 requirements/detail에 섞인 family
  - noisy detail 때문에 preferred를 못 가르는 family

### 다음 1순위

- `우대사항_표시 = 별도 우대사항 미기재` 18건을 family-level로 분류하고,
  - 진짜 미기재는 유지
  - flat detail segmentation으로 복원 가능한 family만 줄인다
- 그 다음에야 `배포 가능` 판정을 다시 하는 것이 맞다
## 2026-04-03 Closeout

- 닫힌 blocker
  - exact/source-family parser regressions
  - display blank 3종
  - Huge AI Lab same-page collision
  - active-but-no-master source family
  - english display leak family
- 최종 검증
  - full `pytest -q` 통과
  - `run-collection-cycle` 실실행 완료
  - bounded leak-source refresh 후 `quality_gate_passed = true`
  - `promote-staging`, `sync-sheets(master/staging)`, `doctor` 재통과
- 현재 배포 기준 수치
  - `master = 123`
  - `staging = 123`
  - `quality_score_100 = 99.11`
  - `english_leak_count = 0`
  - `duplicate_job_url_count = 0`
  - `경력수준_표시 blank = 0`
  - `채용트랙_표시 blank = 0`
  - `우대사항_표시 blank = 0`
- 남은 residual은 quality gate blocker가 아니다.
  - `경력수준_표시 = 미기재` `6`
  - `우대사항_표시 = 별도 우대사항 미기재` `19`

## 2026-04-03 Direct Expansion Closeout

### 해결됨

- source discovery flow를 새로 만들지 않고, 공식 채용 출처 4곳을 production runtime에 직접 편입했다.
  - `업스테이지`
  - `노타`
  - `리벨리온`
  - `퓨리오사AI`
- 이 4개 source는 모두 현재 `verification_status = 성공`이다.
- bounded incremental + promote + sheet sync 이후:
  - `master = 160`
  - `staging = 160`
  - `quality_score_100 = 99.79`
  - `new_job_count = 37`
- registry subset overwrite 문제도 복구했다.
  - `runtime/source_registry.csv = 555 rows`
  - `output_samples/approved_sources.csv = 400 rows`
  - `verified_sources = 545`
  - `active_gt_0_sources = 41`

### 현재 판정

- 현재 runtime은 `기존 bounded production universe + 이번 4개 공식 source`가 함께 반영된 상태다.
- 즉 이번 확장은 기존 universe를 대체한 것이 아니라 실제로 덧붙인 것이다.
- `doctor` 재통과까지 확인했기 때문에 runtime 정합성도 현재 기준으로 정상이다.

## 2026-04-03 KORAIA List Closeout

### 해결됨

- 스타트업·중소·중견 모집단 확대를 위해 `KOREA AI Startups` 기업편람을 직접 시드로 편입했다.
- `20`페이지 `200`건을 bounded하게 훑었고, current registry에 없던 AI 관련 회사 `81`개를 company seed로 추가했다.
- 이 배치에서 실제 채용 source로 붙은 것은:
  - `19` discovered sources
  - `13` approved sources
  - `6` candidate sources
- approved 13개로 바로 incremental 수집을 돌려:
  - `new_job_count = 19`
  - `master = 179`
  - `staging = 179`
  - `quality_score_100 = 99.80`

### 현재 판정

- 이번 확장은 `회사 리스트 -> 회사 시드 -> source discovery -> job tracking`으로 실제 연결됐다.
- 즉 단순한 회사명 적재가 아니라, 이미 기존 수집 경로에 태워 추적 가능한 모집단으로 전환한 상태다.

## 2026-04-03 Additional Official List Closeout

### 해결됨

- `서울 AI 허브` 공식 resident / graduate roster를 production universe에 직접 편입했다.
- roster `318`개를 파싱했고, homepage host가 확인된 `283`개 중 current registry에 없던 `218`개를 company seed로 추가했다.
- 이 batch에서 실제로 붙은 것은:
  - `source_candidate_count = 20`
  - `approved_count = 14`
  - `candidate_count = 5`
  - `new_job_count = 4`
  - `master = 183`
  - `staging = 183`
- `KOREA AI STARTUP 100` 공식 company info slice도 bounded하게 붙였다.
  - visible slice `12`개 company/domain 기반
  - `verified_source_success_count = 5`
  - `selected_collectable_source_count = 6`
  - `new_job_count = 0`

### 현재 판정

- 현재 모집단 확장은 더 이상 단일 KORAIA 리스트에만 의존하지 않는다.
- `KORAIA + 서울 AI 허브 + KOREA AI STARTUP 100 slice`가 모두 기존 production 경로에 연결된 상태다.
- 현재 기준:
  - `master = 183`
  - `staging = 183`
  - `source_registry_rows = 597`
  - `verified_sources = 570`
  - `active_gt_0_sources = 49`
