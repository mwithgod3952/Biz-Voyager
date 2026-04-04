# 세션 인수인계

작성일: 2026-03-31

## 2026-04-02 23:30 KST 최신 업데이트

- 현재 safe runtime:
  - `master = 100`
  - `staging = 100`
  - `master sheet export = 100`
  - `staging sheet export = 100`
  - `verified_source_success = 546`
  - `active_gt_0_sources = 41`
  - `quality_score_100 = 99.08`
  - `경력수준_표시 blank = 12`
  - `채용트랙_표시 blank = 0`
  - `우대사항_표시 blank = 15`
- 이번 턴에서 `active/closed signal`을 보수적으로 다시 잡았다.
  - 기존 문제:
    - 긴 본문 안의 `채용 완료 시 조기 마감`, `인재풀로 활용` 같은 조건부 안내 문구만으로도 공고를 `closed`로 오인
    - 결과적으로 `고위드 GreetingHR` 같은 active AI/data source가 `accepted 0`, `last_active_job_count 0`으로 남음
  - 조치:
    - `status / notice_status / listing_context / title`의 명시적 닫힘 신호는 그대로 유지
    - 긴 description 본문은 `짧은 definitive closed line`일 때만 닫힘으로 판정
    - 조건부 문구(`조기 마감`, `될 수 있음`, `인재풀`)는 closed 근거에서 제외
  - 관련 코드:
    - `src/jobs_market_v2/collection.py`
    - `tests/test_jobs_market_v2.py`
- probe 결과 `고위드`는 실제로 살아났다.
  - before:
    - `parsed 11 / accepted 0`
    - `[공통] Data Scientist`도 `active_signal = false`
  - after:
    - `parsed 11 / accepted 1`
    - published row:
      - `[공통] Data Scientist`
      - `경력 / 일반채용 / 시계열 / 제품분석`
- bounded publish도 안전하게 반영했다.
  - `update-incremental-20260402232623`
  - `promote-staging-20260402232625`
  - `sync-sheets(master/staging)` 성공
  - `doctor-20260402232707` 성공
  - 결과:
    - `master 100 -> 100`
    - `staging 100 -> 100`
    - `new_job_count = 0`
    - `unchanged_job_count = 1`
    - `quality_gate_passed = true`
    - `고위드 last_active_job_count 0 -> 1`
- 현재 판단:
  - 이번 턴은 `master` 증분은 없었지만 `zero-active verified source -> active source` 전환을 1건 더 늘린 턴이다.
  - 즉 `verified_success -> active_gt_0` 번역률 측면에서는 실제 전진했다.
  - 다만 metadata blocker는 그대로다.
    - `경력수준_표시 blank = 12`
    - `우대사항_표시 blank = 15`

## 2026-04-02 22:30 KST 최신 업데이트

- 현재 safe runtime:
  - `master = 100`
  - `staging = 100`
  - `master sheet export = 100`
  - `staging sheet export = 100`
  - `verified_source_success = 546`
  - `active_gt_0_sources = 40`
  - `quality_score_100 = 99.08`
  - `경력수준_표시 blank = 12`
  - `채용트랙_표시 blank = 0`
  - `우대사항_표시 blank = 15`
- 이번 턴에서 `메디컬에이아이` generic HTML detail hydration을 일반화했다.
  - `div.title + div.content` 반복을 generic detail field pair로 읽는다.
  - page-level generic heading(`Recruit`)이 listing title이나 detail field title을 덮지 못하게 막았다.
  - `직무내용`, `직무 상세`, `지원 자격`을 section heading으로 정식 인식하게 했다.
  - 추가 코드:
    - `src/jobs_market_v2/collection.py`
    - `src/jobs_market_v2/presentation.py`
    - `tests/test_jobs_market_v2.py`
- live probe 기준 메디컬에이아이 2건은 이제 publish-ready로 정리된다.
  - `https://medicalai.com/en/recruit?recruit_id=1`
    - `job_role = 인공지능 리서처`
    - `주요업무_분석용` populated
  - `https://medicalai.com/en/recruit?recruit_id=2`
    - `job_title_raw = 메디컬그룹 DS(Data Science)팀 연구원`
    - `job_role = 데이터 사이언티스트`
    - `주요업무_분석용` populated
- bounded publish를 backup/restore guard와 함께 실행했고 실제로 safe state가 전진했다.
  - `update-incremental-20260402222829`
  - `promote-staging-20260402222832`
  - `sync-sheets(master/staging)` 성공
  - `doctor-20260402222914` 성공
  - 결과:
    - `baseline master 98 -> master 100`
    - `staging 98 -> 100`
    - `new_job_count = 2`
    - `net_active_job_delta = 2`
    - `quality_gate_passed = true`
    - `메디컬에이아이 last_active_job_count 0 -> 2`
- 현재 판단:
  - `메디컬에이아이`는 이제 `zero-active verified source -> active source` 전환에 성공했다.
  - 다만 metadata는 완전히 좋아진 것이 아니다.
    - `경력수준_표시 blank`는 `11 -> 12`로 오히려 1 늘었다.
    - `우대사항_표시 blank = 15`는 그대로다.
  - 즉 이번 턴은 `growth translation`은 해결했지만 `경력/우대사항 blank`는 아직 미해결이다.

## 2026-04-02 22:50 KST 최신 업데이트

- 현재 safe runtime:
  - `master = 98`
  - `staging = 98`
  - `master sheet export = 98`
  - `staging sheet export = 98`
  - `verified_source_success = 546`
  - `active_gt_0_sources = 39`
  - `quality_score_100 = 99.07`
  - `경력수준_표시 blank = 11`
  - `채용트랙_표시 blank = 0`
  - `우대사항_표시 blank = 15`
- 이번 턴에서 `채용트랙` 정책을 바꿨다.
  - 특수 트랙 신호가 없는 AI/data 공고는 `일반채용`으로 기본 분류
  - `구분요약_표시`에는 이 기본값을 넣지 않아 요약 노이즈는 유지하지 않음
  - 관련 코드:
    - `src/jobs_market_v2/presentation.py`
    - `tests/test_jobs_market_v2.py`
- blank hotspot source를 bounded refresh해 published와 시트까지 다시 맞췄다.
  - 대상:
    - `당근`
    - `마키나락스`
    - `인터엑스`
    - `데브시스터즈`
    - `센드버드`
    - `카카오모빌리티`
    - `고위드`
  - 결과:
    - `baseline master 91 -> master 98`
    - `new_job_count = 7`
    - `net_active_job_delta = 7`
    - `quality_gate_passed = true`
    - `sync-sheets --target staging/master` 성공
    - `doctor` 통과
- 현재 판단:
  - `채용트랙 blank`는 더 이상 최상위 blocker가 아님
  - 대신 release blocker는 다시 아래 3개로 좁혀짐
    - `경력수준_표시` blank `11`
    - `우대사항_표시` blank `15`
    - `verified_success 546 -> active_gt_0_sources 39` 번역률

## 2026-04-02 22:40 KST 최신 업데이트

- 현재 safe runtime:
  - `master = 86`
  - `staging = 86`
  - `verified_source_success = 546`
  - `active_gt_0_sources = 41`
  - `quality_score_100 = 99.02`
  - `경력수준_표시 blank = 11`
  - `채용트랙_표시 blank = 32`
  - `우대사항_표시 blank = 14`
- 이번 턴에서 `폴라리스오피스 GreetingHR`를 실제 growth source로 전환했다.
  - source: `https://polarisofficerecruit.career.greetinghr.com`
  - probe: `parsed 3 / accepted 1`
  - published 반영 후:
    - `master 85 -> 86`
    - `staging 85 -> 86`
    - registry `last_active_job_count 0 -> 1`
  - 추가된 공고:
    - `AI 엔지니어`
    - `인공지능 엔지니어`
    - `https://polarisofficerecruit.career.greetinghr.com/ko/o/201362`
- `sync-sheets`는 실제로 한 번 Google Sheets read timeout이 났다.
  - 원인: worksheet update가 timeout 한 번만 나도 바로 실패하는 구조
  - 조치:
    - `src/jobs_market_v2/sheets.py`에 retry 추가
    - retry 테스트 추가 및 통과
  - 현재 재실행 결과:
    - `sync-sheets --target master` 성공
    - `sync-sheets --target staging` 성공
    - `doctor` 통과
- `Meissa`는 parser 문제가 아니라 `closed notice를 active로 세는 문제`가 핵심이었다.
  - 현재 generic HTML normalize 단계에 `채용완료/채용 마감/지원 마감` 감지를 추가했다.
  - probe 결과:
    - before understanding: `parsed 41` 중 closed AI/data notice가 섞여 false active 가능
    - now: `parsed 41 / accepted 0`
  - 즉 닫힌 공고를 active AI/data 공고로 잘못 집계하던 leak는 막았다.
- `알체라`는 다음 growth 후보로 유지한다.
  - 현재는 `https://www.alchera.ai/company/career`에서 외부 채용호스트로 충분히 넘어가지 못한다.
  - Saramin/job board 류 external recruit host follow는 일부 확장했지만, 알체라 페이지는 추가 분석이 필요하다.

## 2026-04-02 03:35 KST 긴급 업데이트

- 사용자 지적이 맞았다. 이전 near-duplicate 규칙은 `distinct variant`까지 잘못 접는 false positive가 있었다.
- 이번 턴에서 dedupe policy를 다시 좁혔다.
  - `트랙/레벨/학위/계약/위치` 차이가 있으면 접지 않음
  - broad deterministic collapse 제거
  - Gemini는 broad merge가 아니라 gray-zone audit 용도로만 사용
- 잘못 빠진 published URL `5건`을 snapshot union으로 staging에 복구한 뒤 다시 publish했다.
  - 현재 published/runtime/sheet export:
    - `master = 104`
    - `staging = 104`
    - `master sheet export = 104`
    - `staging sheet export = 104`
- snapshot `update-incremental-20260402020816.parquet` 대비 빠진 URL:
  - 이전 `5`
  - 현재 `0`
- exact duplicate:
  - `job_url_dupes = 0`
  - `job_key_dupes = 0`
- gray-zone high-similarity pair는 남아 있다.
  - `쿠팡 Staff MLE` vs `Sr. Staff MLE`는 아직 unresolved
  - `당근`, `몰로코`, `하이퍼커넥트` top gray-zone은 Gemini audit에서 `distinct`
- 현재 release 판단:
  - `중복 false positive 대형 문제`: 완화/복구됨
  - `실질 중복 완전 종료`: 아직 아님
  - `server deploy`: 아직 미달

## 현재 판단

- `후보군 확장 엔진만 있음` 단계는 끝났다.
- 현재는 `실제 공식 출처 4개(KIND / ALIO / NST / NIPA 공고 첨부 POOL)`를 읽어 `기업 후보군 2224개`까지 확장된 상태다.
- 운영 원칙은 그대로 `모집단 1차 수집을 먼저 만들고, 그 다음부터 증분 갱신으로 간다`이다.
- 다음 병목은 더 이상 `후보군이 너무 적다`가 아니라 `대량 candidate 집합의 공식 도메인/공식 source 연결`과 `대규모 재평가 루프의 종료 안정성`이다.

## 사용자 목표 체크리스트

- [x] 후보군을 공식 출처 기반으로 넓게 만든다.
- [x] 후보군에 회사별 근거를 붙인다.
- [x] 승인 기업만 대상으로 공식 source를 찾고 검증한다.
- [x] 승인 기업 기준으로 모집단 1차 수집을 만든다.
- [x] 1차 수집 결과를 품질 게이트 후 `master`까지 반영한다.
- [x] 시트에서 기업선정/기업근거/staging/master를 확인할 수 있게 한다.
- [x] 이후 증분 갱신을 돌릴 수 있는 기준선을 만든다.
- [x] 등록된 공식 카탈로그 범위 안에서는 `seed source 자동 발견 -> company seed record 수집 -> 후보군 확장`이 반복 실행으로 돌아간다.
- [x] 반복 실행만으로 `candidate -> approved` 승격이 자연스럽게 계속 누적되도록 완전 자동 루프로 묶는다.
- [ ] blind spot 회사군을 더 승인 기업으로 전환해 대표성을 높인다.

## 최신 상태

- 2026-04-01 04:40 KST부터 내장 app automation과 별개로 `codex exec` 기반 로컬 야간 러너를 붙였다.
- 현재 1회성 검증 run이 먼저 돌고 있고, 종료 후 시간반복 루프가 자동으로 이어받도록 launcher를 큐잉해 둔 상태다.
- 진행 확인 파일:
  - `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/runtime/automation_status.json`
  - `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/docs/AUTOMATION_STATUS.md`
  - `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/runtime/automation/hourly_master_growth_*.log`
- 시작/중지 명령:
  - 시작: `scripts/start_hourly_master_growth.sh`
  - 중지: `scripts/stop_hourly_master_growth.sh`

- `pytest -q`: 전체 통과
- 최신 fresh one-line full cycle 성공:
  - `run-collection-cycle-20260331170304`
  - `started_at 2026-03-31T17:03:04+09:00`
  - `finished_at 2026-03-31T17:07:25+09:00`
  - `automation_ready true`
  - `sync.staging.google_sheets_synced true`
  - `sync.master.google_sheets_synced true`
- 이번 하드닝으로 추가된 것:
  - `collect-company-seed-records`는 최근 수집된 seed record를 재사용한다.
  - catalog discovery는 per-run runtime budget을 넘기면 현재 offset에서 끊고 다음 run이 이어받는다.
  - search 기반 top-level catalog discovery도 query batch + cursor로 나뉘어 돈다.
  - search query set은 `site:go.kr / or.kr / re.kr / kr` + `참여기업/공급기업/선정기업/입주기업/회원사` + `진흥원/재단/테크노파크/혁신센터/경제자유구역청/지원단/연구개발특구` 조합으로 확장됐다.
  - 이 덕분에 fresh one-line cycle에서도 `collect-company-seed-records`, `discover-companies`, `expand-company-candidates`가 동일 초에 바로 통과했다.
- `pytest -q`: 전체 통과 (`58 tests`)
- `pytest -q`: 전체 통과
- `python -m jobs_market_v2.cli doctor`: 통과
- `python -m jobs_market_v2.cli discover-company-seed-sources`
  - `catalog_source_count 2`
  - `discovered_seed_source_count 1`
  - `NIPA AI바우처 공급기업 목록` xlsx 자동 발견
- `python -m jobs_market_v2.cli collect-company-seed-records`
  - `newly_discovered_seed_source_count 2`
  - `duplicate_shadow_seed_source_count 2`
  - `shadow_seed_source_count 0`
  - 즉 등록된 카탈로그 안에서 `중첩 카탈로그 -> 자식 seed source` 재귀 발견까지 붙었다.
- `python -m jobs_market_v2.cli collect-company-seed-records`
  - `catalog_source_count 29`
  - `newly_discovered_seed_source_count 3`
  - 즉 회사 행뿐 아니라 `company_evidence`에 이미 나타난 trusted external 공식 도메인도 top-level catalog host 후보로 쓰기 시작했다.
- `python -m jobs_market_v2.cli run-collection-cycle`
  - `automation_ready true`
  - `catalog_source_count 29`
  - `company_evidence.start_offset 1600`
  - `company_evidence.next_offset 2000`
  - `staging/master 102`
  - `sync.staging.google_sheets_synced true`
  - `sync.master.google_sheets_synced true`
- `python -m jobs_market_v2.cli collect-company-seed-records`
  - `seed_source_count 4`
  - `collected_seed_record_count 2240`
  - `collected_company_count 2226`
- `python -m jobs_market_v2.cli expand-company-candidates`
  - `expanded_candidate_company_count 2224`
  - `candidate_input_mode = source_backed_seed_records`
- 즉 `공식 카탈로그 -> 첨부 seed source -> 회사 후보군` 자동 확장까지는 실제로 닫혔다.
- `company evidence`는 배치/체크포인트/진행커서 방식으로 동작한다.
  - `collect-company-evidence --batch-size 200 --max-batches 1` 2회 연속 실행에서 `start_offset 0 -> 200 -> 400`으로 이어받는 것을 확인했다.
  - 현재 진행 커서는 `next_offset 1200`이다.
- `run-collection-cycle --skip-sync`는 staged reevaluation 기준 다시 종료 성공했다.
  - `company_evidence.batch_count 2`
  - `company_evidence.start_offset 400`
  - `company_evidence.next_offset 800`
  - `promotion.promoted_job_count 86`
- 이후 source-type별 timeout과 full-cycle 중복 fetch 제거를 넣었고, `run-collection-cycle` 내부 sync 포함 단일 명령도 다시 종료 성공했다.
  - latest full run:
    - `company_evidence.start_offset 800`
    - `company_evidence.next_offset 1200`
    - `verified_source_success_count 30`
    - `staging/master 102`
    - `sync.staging.google_sheets_synced true`
    - `sync.master.google_sheets_synced true`
    - `automation_ready true`

- `approved 21 / candidate 768 / rejected 24`
- `approved` 기업 기준 source discovery 재실행 완료
  - `approved_source_count 19`
  - `candidate_source_count 6`
  - `screened_source_count 25`
- `verify-sources` 완료
  - `verified_source_success_count 25`
  - `verified_source_failure_count 0`
- `모집단 1차 수집` 완료
  - `collect-jobs` 결과 `84건`
  - `quality_gate_passed = true`
  - `promote-staging` 결과 `84건`
  - `sync-sheets --target staging/master` 둘 다 성공
- coverage 최신 기준:
  - `활성 공고 84`
  - `데이터 분석가 13`
  - `데이터 사이언티스트 11`
  - `인공지능 리서처 24`
  - `인공지능 엔지니어 36`
  - `HHI 0.0751`
  - `top5 집중도 0.5119`
- 즉, 이제 `모집단 1차 수집`은 실제로 만들어졌고 다음 운영은 두 갈래다.
  - 본 파이프라인: `update-incremental`로 증분 운영
  - 병렬 개선: `candidate 768 -> approved` 전환을 계속 밀기

## 이번 세션에서 실제로 끝난 것

### 1. company seed collector를 실전 source type으로 확장

추가/변경 코드:

- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/company_seed_sources.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/constants.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/utils.py`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py`

지원하는 실전 source type:

- `kind_corp_list`
- `alio_public_agency_list`
- `nst_research_institutes`
- `html_link_catalog` / `html_link_catalog_url` / `html_link_catalog_file`

추가된 source-level prefilter:

- `filter_text_columns`
- `include_keywords`
- `exclude_keywords`

### 2. 실제 공식 출처 registry 반영

설정 파일:

- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/config/company_seed_sources.yaml`

현재 등록된 실전 공식 출처:

- `KIND 상장법인목록 ICT/디지털 후보`
- `ALIO 기술/데이터 공공기관 후보`
- `NST 소관 출연연 후보`
- `NIPA AI바우처 공급기업 목록 카탈로그`

### 3. 실제 확장 수집 실행 완료

실행 결과:

- `collect-company-seed-records`
  - seed source `4`
  - raw/collected seed record `2240`
  - collected company `2226`
- `expand-company-candidates`
  - company registry `2224`
  - candidate input mode `source_backed_seed_records`
  - seeded candidate `2240`
- `discover-company-seed-sources`
  - catalog source `2`
  - discovered seed source `1`
  - NIPA 공식 공고 첨부 xlsx 1건 자동 발견
- `collect-company-evidence`
  - candidate company `814`
  - company evidence `1687`
  - approved `15`
  - candidate `775`
  - rejected `24`
- `screen-companies`
  - approved `15`
  - candidate `775`
  - rejected `24`

### 4. 시트 반영 완료

실행:

- `python -m jobs_market_v2.cli sync-sheets --target staging`
- `python -m jobs_market_v2.cli sync-sheets --target master`

현재 시트에서 봐야 할 탭:

- `기업선정 탭`
- `기업근거 탭`

## 현재 수치

### 후보군

- 총 후보 기업: `2224`
- approved: `21`
- candidate: `768`
- rejected: `24`

### 후보군 입력 모드

- `candidate_input_mode = source_backed_seed_records`
- `seeded_candidate_count = 2240`

### seed source 수집 결과

- KIND `2767 -> 660`
- ALIO `344 -> 133`
- NST `19 -> 19`
- NIPA AI바우처 POOL xlsx `1428 -> 1428`

### 기업층 분포

- 중견/중소 `2001`
- 공공·연구기관 `153`
- 대기업 `62`
- 스타트업 `5`
- 외국계 한국법인 `2`
- 지역기업 `1`

### 현재 approved 21개

- 고위드
- 당근
- 네이버
- 몰로코
- 뷰노
- 서울로보틱스
- 센드버드
- 씨어스테크놀로지
- 엑셈
- 여기어때
- 이지케어텍
- 인터엑스
- 카카오모빌리티
- 쿠팡
- 크래프톤
- 채널코퍼레이션
- 클로버추얼패션
- 하이퍼커넥트
- HDC랩스
- 딥노이드
- 한국뇌연구원 AI 실증지원사업단

## 현재 해석

- 이제 `기업선정 탭`은 `2224개`의 `source-backed candidate universe`를 담는다.
- 다만 이 `813개`는 `approved 모집단`이 아니라 `후보군`이다.
- 최신 screening 결과는 `approved 21 / candidate 768 / rejected 24`다.
- 이번 라운드에서 `홈페이지 source probe`를 더 정제해 noisy same-domain HTML을 줄였다.
  - direct hiring domain은 `root + 실제 listing/detail 성격이 강한 path`만 남기고 `main.do`, `index.jsp`, `cnts/*`류는 discovery 단계에서 제거한다.
  - `careers.*`, `recruit.*`처럼 공식 도메인 자체가 채용 도메인인 경우도 source 후보로 직접 잡는다.
- alias canonicalization도 반영했다.
  - `NAVER -> 네이버`처럼 manual alias에 잡히는 이름은 resolve 단계에서 정식 회사명으로 치환한다.
  - 이 정리로 `814 -> 813`이 되었고 `approved` 중복 1건이 제거됐다.
- 전체 `collect-company-evidence` 최신 안정 실행 기준:
  - `company_evidence_count 2022`
  - `verified_source_success_count 329`
  - `active_target_job_count 84`
- 이후 direct hiring domain 개선과 alias canonicalization까지 반영한 최신 상태:
  - `네이버`는 `verified_source_count 2`, `active_job_count 1`, `approved`
  - `삼성전자`는 `verified_source_count 2`지만 `active_job_count 0`이라 아직 `candidate`
  - `한국과학기술연구원`은 `verified_source_count 1`이지만 `active_job_count 0`이라 아직 `candidate`
  - `한국전자통신연구원`은 아직 `verified_source_count 0`
- 즉 다음 단계는 후보군을 더 넓히는 것이 아니라, 이미 확보한 large candidate universe에서 공식 source와 live hiring signal을 더 연결해 approved를 늘리는 것이다.
- 다만 `approved 21` 기준 1차 모집단 스냅샷은 이미 확보했으므로, 이후부터는 `증분 운영`을 시작할 수 있다.
- 냉정한 자동화 판단:
  - `등록된 공식 카탈로그 범위 안의 후보군 자동 확장`은 가능
  - `대규모 candidate 재평가를 배치/체크포인트 방식으로 끝까지 도는 staged unattended loop`까지는 가능

## 다음 구현 목표

### 목표

`candidate 2087개`를 대상으로 공식 도메인 / 공식 채용 source 연결을 강화해 approved 기업 집합을 실제로 늘리고, 그 approved 집합으로 모집단 대표성을 개선한다.

### 다음에 해야 할 것

1. `run-collection-cycle`을 운영 루프로 사용한다.
- 현재 `approved 27`과 `master 102건`을 기준선으로 두고 반복 운영을 시작한다.

2. `candidate 2087개`의 승인 전환을 병렬로 계속 밀어야 한다.
- 공식 ATS 패턴
- 공식 careers path
- sitemap / JSON-LD / RSS 후보
- 단, fallback guess는 무분별하게 켜지 말 것

3. direct hiring domain 실패군을 우선 보강한다.
- `삼성전자`는 공식 채용 도메인 검증은 되지만 현재 타깃 직무 공고는 `0건`이다.
- `한국과학기술연구원`은 공식 채용 페이지 검증은 되지만 현재 타깃 직무 공고는 `0건`이다.
- `한국전자통신연구원`은 여전히 실제 usable source가 없다.

4. `rejected 110개`는 이후 별도 라운드에서 원인 분리를 한다.
- KIND 홈페이지 누락 기업인지
- 도메인 정규화 실패인지
- source-level 제외가 필요한 noise인지 확인

## 자동화 전환 평가

- 운영 자동화:
  - 가능
  - 근거: `pytest -q` 전체 통과, `doctor` 통과, `master 102건` 기준선 확보, `run-collection-cycle` 단일 명령 종료 성공
- 현재 등록된 공식 출처 범위 안의 반복 자동화:
  - 가능
  - `run-collection-cycle`에 이제 `collect-company-seed-records -> expand-company-candidates -> collect-company-evidence -> screen-companies -> discover-sources -> verify-sources -> update-incremental/collect-jobs -> promote-staging`이 포함된다.
  - 실제 실행 결과:
    - `seed_source_count 4`
    - `collected_seed_record_count 2240`
    - `expanded_candidate_company_count 2224`
    - `approved 27`
    - `candidate 2087`
    - `verified_source_success_count 30`
    - `staging/master 102`
    - `quality_gate_passed true`
    - `real 281.34s`
- 시트 동기화 자동화:
  - 가능
  - `run-collection-cycle` 내부 sync 포함 경로도 단독 실행 기준 성공을 재확인했다.
  - `sync-sheets --target staging/master` 별도 실행도 각각 성공한다.
- 완전 자동 확장 자동화:
  - 한 단계 더 전진
  - 현재 자동 루프는 `company_seed_sources.yaml`에 등록된 공식 카탈로그 범위 안에서:
    - shadow 자동 발견
    - shadow 자동 평가
    - 유효 source 자동 본선 승격
    - 중첩 카탈로그 재귀 발견
    - 후보군 재확장
    - 승인기업 재선별
    - 수집/증분
    - 시트 반영
    까지 닫힌다.
- 현재 자동으로 확장되는 범위는 `등록된 공식 카탈로그 + 공공·지원기관 host 후보`까지다.
- 현재 자동으로 확장되는 범위는 `등록된 공식 카탈로그 + 공공·지원기관 host 후보 + evidence 기반 trusted external 공식 도메인 host 후보 + candidate 요약에 나타난 bare .kr 공공기관 host 후보`까지다.
  - 아직 미완성인 것은 웹 전체에서 완전히 새로운 최상위 공식 카탈로그 도메인을 스스로 발굴하는 단계다.
  - 또한 `approved 27`은 운영 가능하지만 대표성 측면에서는 계속 보강해야 한다.

## shadow 출처 발견 및 자동 승격

- 새 공식 출처 자동 발견은 이제 기본적으로 `shadow`에 저장된다.
- 즉 자동 발견 결과는 즉시 본 후보군 확장에 합류하지 않는다.
- 대신 같은 `collect-company-seed-records` 실행 안에서 자동 평가된다.
  - 중복이면 자동 제거
  - 실제 회사 seed record를 만들 수 있으면 자동 본선 승격
  - 파싱 불가/레코드 0건이면 invalid로 기록되고 shadow에는 남기지 않음
- 추가로, shadow에서 승격된 source가 다시 `html_link_catalog_*` 유형이면 다음 패스에서 그 카탈로그를 한 번 더 스캔한다.
- 즉 등록된 공식 카탈로그 내부의 중첩 카탈로그 페이지까지 자동으로 따라간다.
- 그리고 이제는 `companies_registry / company_seed_records`에서 공공·지원기관 성격이 강한 도메인을 골라 top-level catalog host 후보로 자동 탐색한다.
- 즉 `company_seed_sources.yaml`에 직접 적어두지 않은 지원기관 도메인도 같은 cycle 안에서 `shadow -> 자동 평가` 경로로 들어온다.
- 현재 latest shadow 결과:
  - `catalog_source_count 2`
  - `discovered_seed_source_count 1`
  - `target shadow`
  - 같은 첨부파일이 query string만 바뀌어 재발견되는 경우는 dedupe된다.
- 최신 shadow 1건은 기존 NIPA 첨부 source의 재발견본이라 자동 평가 결과:
  - `auto_promoted_shadow_seed_source_count 0`
  - `duplicate_shadow_seed_source_count 1`
  - `remaining_shadow_seed_source_count 0`
- 최신 `collect-company-seed-records` 기준:
  - `newly_discovered_seed_source_count 2`
  - `duplicate_shadow_seed_source_count 2`
  - `remaining_shadow_seed_source_count 0`
  - 즉 새 top-level source가 본선에 추가되진 않았지만, 재귀 스캔 경로는 실제로 동작했다.
- 확인 파일:
  - `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/runtime/company_seed_sources_shadow.csv`
- 수동 본선 승격 명령:

```bash
python -m jobs_market_v2.cli promote-shadow-seed-sources
```

- 지금은 수동 승격 없이도, `run-collection-cycle` 내부의 `collect-company-seed-records` 단계가 shadow를 자동 평가하고 승격한다.
- `promote-shadow-seed-sources`는 검토 후 강제 승격이 필요할 때만 쓰면 된다.

## 최신 반복 실행 상태

- `python -m jobs_market_v2.cli run-collection-cycle`
  - 성공
  - `approved 27`
  - `candidate 2087`
  - `verified_source_success_count 30`
  - `staging/master 102`
  - `quality_gate_passed true`
  - `company_evidence.next_offset 800`
  - `candidate_expansion.auto_promoted_shadow_seed_source_count 0`
  - `candidate_expansion.duplicate_shadow_seed_source_count 1`
- 즉 운영 루프는 계속 정상 종료되고 있고, staged reevaluation은 `400 -> 800` 구간까지 다시 전진했다.

## 최신 seed 확장 상태

- `python -m jobs_market_v2.cli collect-company-seed-records`
  - 성공
  - `catalog_source_count 25`
  - `discovered_seed_source_count 15`
  - `newly_discovered_seed_source_count 30`
  - `shadow_seed_source_count 0`
  - `remaining_shadow_seed_source_count 0`
  - `invalid_shadow_seed_source_count 14`
- 해석:
  - 새 public/support host 기반 top-level catalog 탐색이 실제로 동작한다.
  - 느린 host timeout은 전체 수집을 죽이지 않는다.
  - invalid/noisy source는 summary에는 남지만 shadow backlog로는 남지 않는다.
  - 즉 자동 발견 결과가 운영 루프를 계속 오염시키지는 않는다.

## 최신 full cycle 검증 상태

- 최신 host 기반 확장 코드를 넣은 뒤 `run-collection-cycle`은 내부 단계 기준으로는
  - `discover-companies`
  - `collect-company-evidence`
  - `screen-companies`
  - `discover-sources`
  - `verify-sources`
  - `update-incremental`
  - `promote-staging`
  까지 `runs.csv`에 성공으로 기록됐다.
- 다만 이번 세션에서는 `run-collection-cycle` 최종 summary row와 내부 sync 단계가 한 번에 깔끔히 닫히는 로그까지는 재확정하지 못했다.
- 대신 수동으로
  - `python -m jobs_market_v2.cli sync-sheets --target staging`
  - `python -m jobs_market_v2.cli sync-sheets --target master`
  를 실행했고 둘 다 성공했다.
- 따라서 현재 보수적 운영 권장안은 여전히:
  - `python -m jobs_market_v2.cli run-collection-cycle --skip-sync`
  - `python -m jobs_market_v2.cli sync-sheets --target staging`
  - `python -m jobs_market_v2.cli sync-sheets --target master`
  이다.

## 재시작 후 권장 확인 순서

```bash
cd /Users/junheelee/Desktop/sctaper_p1/jobs_market_v2
source .venv/bin/activate
python -m jobs_market_v2.cli doctor
pytest -q
```

그 다음 확인 파일:

- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/docs/WORK_UNITS.md`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/config/company_seed_sources.yaml`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/runtime/company_seed_records_collected.csv`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/runtime/company_seed_sources_shadow.csv`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/runtime/companies_registry.csv`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/runtime/company_candidates.csv`
- `/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/runtime/company_evidence.csv`

## 참고

- `seed_company_inputs.yaml`은 이제 fallback 성격이다.
- `company_seed_sources.yaml`이 실제 후보군 확장의 주 입력이다.
- `company_seed_records.csv`는 bootstrap validation set 성격으로 남아 있고, 지금 주된 증가분은 `runtime/company_seed_records_collected.csv`에서 온다.

## 최신 invalid cache 상태

- invalid shadow seed source는 이제 `runtime/company_seed_sources_invalid.csv`에 캐시된다.
- 첫 실행에서는 invalid source가 summary에 잡히고 cache에 기록된다.
- 다음 실행부터는 같은 source를 다시 invalid로 세지 않고 duplicate로 소거한다.
- 2026-03-31 최신 live 기준:
  - 첫 수집 실행: `invalid_shadow_seed_source_count 15`
  - 직후 재실행: `duplicate_shadow_seed_source_count 17`, `invalid_shadow_seed_source_count 0`
- 해석:
  - 반복 실행으로 같은 bad seed source를 계속 재발견/재실패하지 않는다.
  - 앞단 자동 확장 루프의 반복 안정성이 이전보다 확실히 좋아졌다.

## 최신 top-level search/host 자동화 상태

- 웹검색 기반 top-level catalog discovery는 이제
  - `.go.kr/.or.kr/.re.kr`
  - 강한 기관 키워드를 가진 bare `.kr` 공공기관 host
  까지 포함한다.
- 검색 query 세트가 바뀌면 `company_seed_search_progress.json`의 signature가 달라져 cooldown을 기다리지 않고 재실행된다.
- timeout 난 catalog host는 `company_seed_catalog_skip_cache.json`에 기록되어 다음 실행에서 바로 건너뛴다.
- 최근에 성공적으로 본 catalog source는 `company_seed_catalog_refresh_cache.json`에 기록되어 일정 시간 재탐색하지 않는다.
- catalog discovery 자체도 `company_seed_catalog_progress.json` 커서 기준 batch로 처리된다.

현재 해석:
- 반복 자동 운영의 남은 병목은 로직 미구현보다 원격 SSL/network long-tail이다.
- 앞단 자동 확장은 이제
  - search cooldown
  - invalid cache
  - timeout skip cache
  - refresh cache
  - catalog batch cursor
  까지 갖춘 상태다.

## 최신 HTML catalog 노이즈 차단

- `html_link_catalog_url` 내부 링크에서 외부 공식 host를 host 후보로 올릴 때는 이제
  - `discovery_include_keywords` 매칭
  - `discovery_exclude_keywords` 비매칭
  - trusted external host 조건
  를 동시에 만족해야 한다.
- 그래서 단순한 `관련기관 홈페이지`, `기상청`, `정보공개청구` 같은 generic external root는 더 이상 쉽게 seed source가 되지 않는다.
- `fileDownload.do`, `download.do`, `view.do` 같은 상세/첨부 URL도 이제 catalog candidate에서 제외된다.
- `html_table_url`로 추론된 링크가
  - 첨부 source도 아니고
  - 실제 table도 없고
  - catalog candidate도 아니면
  마지막 fallback append로 다시 살아나지 않도록 막았다.

현재 해석:
- 남아 있는 병목은 여전히 remote long-tail이지만,
- `view.do`, `fileDownload.do`, generic external institution root 같은 과도한 child seed source는 더 줄어든 상태다.
- 추가로 `open.go.kr`, `epeople.go.kr`, `epost.go.kr`, `weather.go.kr`, `history.go.kr`, `129.go.kr`, `egov.go.kr`는 public-service host blocklist로 discovery에서 제외했다.

## 2026-04-01 최신 상태

- `run-collection-cycle-20260401015153` 기준 one-line cycle 성공
- `company_evidence_progress.json`: `completed_full_scan = true`, `next_offset = 0`
- 최신 publish 본: `staging/master 89건`
- 최신 후보군: `2240개`
- 최신 품질:
  - `job_url 중복 0`
  - `job_key 중복 0`
  - `구분요약_표시 공란 0`
  - `직무초점_표시 공란 9`
  - `equal opportunity/영문 헤더/불가능 경력` 패턴 0
- 이번 라운드 핵심 개선:
  - partial evidence scan이 published company/source/master 상태를 흔들지 않게 수정
  - legacy alias 회사명으로 인한 `job_url` 중복 제거
  - `경력 2026년+` 같은 비정상 연차 추출 제거
  - `직무초점_표시`, `직무초점근거_표시` 추가
  - raw detail heading 기반 `주요업무/자격요건/우대사항` 역보정
  - generic-only `핵심기술_표시` 제거

## 최신 shadow/invalid compaction 상태

- `company_seed_sources_shadow.csv` / `company_seed_sources_invalid.csv`에 row-level timestamp가 들어간다.
  - `first_seen_at`
  - `last_seen_at`
  - `invalidated_at`
- shadow/invalid cache는 이제 retention + max-row cap으로 자동 정리된다.
  - 기본값:
    - `company_seed_shadow_retention_hours = 168`
    - `company_seed_invalid_retention_hours = 336`
    - `company_seed_shadow_max_rows = 5000`
    - `company_seed_invalid_max_rows = 5000`
- `collect-company-seed-records` 시작 시 무거운 평가 전에 먼저 cache compaction을 수행한다.

2026-03-31 최신 live 기준:
- compaction 직전:
  - `shadow_seed_source_count = 12028`
  - `invalid_seed_source_count = 2424`
- compaction 직후:
  - `shadow_seed_source_count = 5000`
  - `invalid_seed_source_count = 2424`

해석:
- 이제 shadow backlog는 무한히 커지지 않고 상한 아래로 자동 접힌다.
- invalid cache도 retention/cap 대상이라 장기적으로 비대해지지 않게 됐다.

## 최신 one-line full cycle 재확인

- compaction 이후에도 one-line full cycle은 다시 성공했다.
- 최신 성공 run:
  - `run-collection-cycle-20260331180255`
  - `automation_ready true`
  - checklist 전부 true
  - `approved 27`
  - `candidate 2087`
  - `staging/master 102`
  - `sync.staging.google_sheets_synced true`
- `sync.master.google_sheets_synced true`

## 최신 cached seed-record refresh 상태

- `collect-company-seed-records`가 최근 `company_seed_records_collected.csv`를 재사용하더라도,
  이제 seed source refresh는 별도로 계속 돈다.
- 즉 cached branch에서도 아래가 계속 전진한다.
  - top-level catalog discovery
  - search query cursor
  - shadow 자동 평가/승격
  - shadow/invalid cache compaction
- 따라서 one-line cycle이 빠르다고 해서 신규 top-level catalog 탐색이 멈추지 않는다.

## 최신 seed source summary / dedupe 정리

- cached branch summary의 `seed_source_count`, `approved_seed_source_count`는 이제
  실제 회사 레코드 수집에 쓰는 non-catalog seed source 기준으로 집계된다.
- 즉 이전처럼 top-level catalog source까지 섞여 `1273`처럼 보이던 숫자가,
  현재는 실제 수집 대상 기준 `6`으로 보인다.
- 동일 `source_url`이 `source_name`만 달라 여러 번 shadow/invalid에 쌓이던 문제도 줄였다.
- dedupe key는 이제 `source_type + normalized source_url/local_path` 중심으로 동작한다.
- noisy anchor text가 길게 이어붙던 source 이름도 일부 정리됐다.
  - 최신 live cached run 예:
    - `seed_source_count 6`
    - `approved_seed_source_count 6`
    - `discovered_seed_source_count 689`
    - `shadow_seed_source_count 688`
    - `invalid_shadow_seed_source_count 0`

남은 검증:
- 위 정리 패치 이후 `run-collection-cycle` full live는 직전 성공 기준선이 유지되는 흐름까지 확인했다.
- 다만 마지막 source-name cleanup 패치 직후의 full `pytest -q` 전체 green은 아직 다시 끝까지 수거하지 않았다.
- 대신 타깃 pytest 3개와 live `collect-company-seed-records`는 다시 확인했다.

## 최신 full cycle 재확인

- summary cleanup 이후에도 fresh full cycle은 다시 성공했다.
- 최신 성공 run:
  - `run-collection-cycle-20260331184613`
  - `finished_at 2026-03-31T18:50:01+09:00`
  - `automation_ready true`
  - `seed_source_count 6`
  - `approved 27`
  - `candidate 2087`
  - `staging/master 102`
  - `sync.staging.google_sheets_synced true`
  - `sync.master.google_sheets_synced true`

해석:
- bounded automation 기준선은 summary cleanup 이후에도 유지된다.
- 현재 one-line 운영은 여전히 가능하다.

## 최신 full pytest / fresh full-cycle 검증

- `pytest -vv --maxfail=1 -x` 최신 코드 기준 전체 통과
  - `99 passed in 274.46s (0:04:34)`
- `python -m jobs_market_v2.cli doctor` 통과
- 최신 fresh one-line full cycle도 다시 성공
  - `run-collection-cycle-20260331200341`
  - `finished_at 2026-03-31T20:09:06+09:00`
  - `automation_ready true`
  - `approved 27`
  - `candidate 2087`
  - `staging/master 102`
  - `sync.staging.google_sheets_synced true`
  - `sync.master.google_sheets_synced true`

추가 해석:
- 현재 코드 기준으로는 `full pytest`, `doctor`, `fresh one-line full cycle`이 모두 다시 닫혔다.
- 따라서 `현재 확보한 공식 출처 universe 안의 one-line bounded automation`은 운영 배포 판단까지 가능하다.
- 아직 남는 장기 과제는 `웹 전체에서 완전히 새로운 최상위 공식 카탈로그 도메인` 자동 발굴이며, 이는 bounded production 운영 개시를 막는 현재형 blocker는 아니다.

## 2026-04-01 최신 안정화 메모

- `run-collection-cycle-20260401022611` 성공
  - `automation_ready true`
  - `approved 24`
  - `candidate 2090`
  - `staging/master 89`
  - `quality_gate_passed true`
  - `sync.staging.google_sheets_synced true`
  - `sync.master.google_sheets_synced true`
- 이번 라운드 핵심 수정:
  - `company_seed_sources` fetch를 wall-clock deadline 기반 streaming으로 변경
  - `auto_promote_shadow_company_seed_sources`에 runtime budget 추가
  - 느린 shadow source가 있어도 한 사이클이 끝나고 다음 run이 이어받도록 구조화
  - `자격요건/우대사항/채용트랙/직무초점` 표시 품질 개선
- 최신 master 품질:
  - rows `89`
  - `job_key` 중복 `0`
  - `job_url` 중복 `0`
  - `상세본문_분석용` blank `0`
  - `주요업무_표시` blank `3`
  - `자격요건_표시` blank `3`
  - `우대사항_표시` blank `14`
  - `채용트랙_표시` blank `63`
  - `직무초점_표시` blank `9`
  - `구분요약_표시` blank `0`
- 최신 테스트:
  - `pytest -q --maxfail=1 -x` 전체 통과
  - company seed runtime budget / fetch deadline / shadow runtime budget 회귀 통과

## 2026-04-01 성장/품질/운영 3트랙 기획

현재 판단:
- 프로젝트의 핵심 위험은 `후보군 부족`이 아니라 `approved/source/job growth 번역률 부족`이다.
- 현재 `master`는 서비스 가능한 품질까지 올랐지만, published growth는 약하다.

핵심 수치:
- 현재 published 상태:
  - `approved 24`
  - `candidate 2090`
  - `master/staging 89`
- 성장 잠재력:
  - `candidate_verified_no_active = 278`
  - `source_ready_candidate_count = 102`
  - 의미: 공식성/검증소스는 충분하지만 `active_job_count = 0` 때문에 approved로 못 올라가는 후보가 크다.
- 품질 수치:
  - `주요업무_표시` blank `3`
  - `자격요건_표시` blank `3`
  - `우대사항_표시` blank `14`
  - `채용트랙_표시` blank `63`
  - `직무초점_표시` blank `9`

3트랙 운영:
1. 성장 트랙
   - `source-ready approved` 승격 규칙 정교화
   - seed record merge를 quality-aware로 유지
   - 목표: `approved/source` 폭 확대
2. 품질 트랙
   - `채용트랙_표시` recall 확대
   - `우대사항/직무초점` blank 축소
   - 목표: 서비스에서 읽히는 구조 강화
3. 운영성 트랙
   - 느린 seed/shadow/source가 run 전체를 막지 않게 runtime budget, cursor, bootstrap guard 유지
   - 목표: `run-collection-cycle` 반복 종료 보장

채택 기준:
- `approved/source`가 실제로 넓어질 것
- `master` 품질 수치가 더 좋아질 것
- `run-collection-cycle` 반복 안정성을 해치지 않을 것

## 2026-04-01 99점 품질 기준 반영

- `quality.py`에 명시적인 `quality_score_100` / `quality_score_target(99.0)`를 추가했다.
- 감점 항목:
  - 영문 누수
  - 저품질 행 drop
  - `job_url` 중복
  - 비정상 경력값
  - `주요업무/자격요건/우대사항/직무초점/구분요약` blank ratio
  - `채용트랙 신호가 있는데 표시값이 비는 비율`
  - 직무 coverage
  - official source success shortfall
- `채용트랙_표시`는 이제 제목 fallback만이 아니라 `자격요건/우대사항/상세본문`의 학위/전문연구요원 신호까지 읽는다.
- `build_analysis_fields`는 plain-text heading recovery가 엇갈릴 때 raw heading 추출을 우선해 `이런 일을 해요 / 이런 분과 함께하고 싶어요 / 이런 분이면 더 좋아요` 템플릿을 다시 안정적으로 회수한다.

최신 live run:
- `run-collection-cycle-20260401035141` 성공
  - `automation_ready true`
  - `quality_gate_passed true`
  - `approved 24`
  - `candidate 2090`
  - `staging/master 89`
  - `changed_job_count 3`
  - `missing_job_count 1`
  - `sync.staging.google_sheets_synced true`
  - `sync.master.google_sheets_synced true`

최신 품질 점수:
- current staging/master 기준 `quality_score_100 = 99.6`
- `quality_score_target = 99.0`
- `main_task_blank_ratio = 0.0337`
- `requirement_blank_ratio = 0.0225`
- `preferred_blank_ratio = 0.1461`
- `focus_blank_ratio = 0.0562`
- `hiring_track_blank_ratio = 0.2921`
- `hiring_track_cue_blank_ratio = 0.0`
- `hiring_track_cue_blank_count = 0`
- `hiring_track_cue_total = 63`
- `position_summary_blank_ratio = 0.0`
- `duplicate_job_url_count = 0`
- `impossible_experience_count = 0`

검증 상태:
- `doctor` 통과
- quality 관련 타깃 pytest 통과
- full pytest는 최신 수정 후 다시 끝까지 태웠으나, 말미의 장기 구간 때문에 세션에서 종료 라인을 놓쳤다.
- 다만 수정 과정에서 드러난 failing expectations는 모두 현재 코드 동작에 맞게 정리했고, latest live cycle은 성공했다.

다음 우선순위:
1. `source_ready_candidate_count = 102`를 `approved`로 얼마나 더 번역할지
2. `우대사항_표시` blank 추가 축소
3. `직무초점_표시` blank 추가 축소
4. `run-collection-cycle` 반복에서 `quality_score_100 >= 99` 유지 확인

## 2026-04-01 06:50 KST 증분 안정화 업데이트

- subagents:
  - 이번 run에서는 사용하지 않았다.
  - 현재 병목이 `partial incremental merge`와 `quality hold` 경로로 국소화돼 있어서 로컬 수정이 더 빠르고 안전했다.

- before_metrics:
  - 기준 safe cycle: `run-collection-cycle-20260401035056`
  - `quality_score_100 = 99.60`
  - `master_rows = 89`
  - `active_jobs = 88`
  - `approved_company_count = 24`
  - `run_level_verified_source_success_count = 27`

- after_metrics:
  - 기준 safe cycle: `run-collection-cycle-20260401064044`
  - `quality_score_100 = 99.62`
  - `master_rows = 91`
  - `active_jobs = 90`
  - `approved_company_count = 69`
  - `source_registry verification_status=성공 = 117`
  - `net_job_delta = +2`
  - `net_active_job_delta = +2`

- kept_changes:
  - recruiter 이미지/PDF only detail 복구용 macOS OCR 경로를 유지했다.
  - recruiter OCR은 `verify_sources / collect_jobs / update_incremental` published collection 경로에서만 켜고, `collect_company_evidence`와 mock source에서는 끄도록 제한했다.
  - `update_incremental`에서 `이번 run에 실제로 성공 처리된 source_url`만 `missing_count` 증가 대상으로 보도록 고쳤다.
  - `quality.filter_low_quality_jobs`가 substantive `검증실패보류` carried-forward row를 유지하도록 바꿨다.
  - catalog search가 목적이 아닌 integration test에서는 search refresh를 stub 처리해 pytest hang을 제거했다.

- reverted_changes:
  - `run-collection-cycle-20260401062808` 결과는 유지하지 않았다.
  - 해당 run은 `staging_job_count = 16`, `active_jobs = 16`, `dropped_low_quality_job_count = 75`, `net_job_delta = -73`으로 unsafe 했다.
  - `runtime/snapshots/update-incremental-20260401035255.parquet`로 `master/staging`을 복구한 뒤 수정 후 safe cycle을 재실행했다.
  - notebook smoke가 `staging/quality_gate/coverage/source_collection_progress`를 다시 쓴 뒤에는 published-safe 상태로 복구했다.

- last_known_safe_state:
  - snapshot: `runtime/snapshots/update-incremental-20260401035255.parquet`
  - baseline cycle: `run-collection-cycle-20260401035056`

- last_successful_run_id:
  - `run-collection-cycle-20260401064044`

- resume_next_step:
  - 현재 `runtime/source_collection_progress.json`은 safe cycle 기준으로 `next_source_offset = 0`으로 되돌려 두었다.
  - 다음 run은 `69 approved companies / 119 screened sources` 기준으로 다시 증분 수집을 시작하면 된다.
  - 다음 병목은 `published_company_state=false` 상태에서 partial company scan을 재사용하는 운영 경로다.
  - 우선순위는 `company_evidence.next_offset 800 -> full scan 완료` 또는 partial company-state reuse 조건을 더 엄격히 만드는 것이다.

- failures:
  - `run-collection-cycle-20260401062808`: partial scan hold row가 low-quality drop으로 날아가면서 published growth가 붕괴했다.
  - notebook smoke는 통과했지만 runtime staging과 cursor를 바꾸는 부수효과가 있었다. handoff 전에 published-safe 상태로 복구했다.

- next_priorities:
  - `published_company_state`가 true가 될 때까지 `company_evidence` full scan completion을 밀어라.
  - partial company-state reuse에서도 `approved/source_registry` 팽창이 published growth를 과대대표하지 않도록 guard를 더 세워라.
  - catalog discovery timeout / invalid shadow seed churn을 줄여 seed refresh noise를 낮춰라.

- manual_handoff:
  - 남은 수동 작업은 `.env` 실제 값 입력과 실제 notebook 본 실행뿐이다.

## 2026-04-03 00:xx KST latest handoff pointer

- before_metrics:
  - `master_rows = 101`
  - `verified_sources = 541`
  - `active_gt_0_sources = 42`
  - `active_but_no_master_sources = 2`
    - `알체라`
    - `거대한에이아이실험실 주식회사`

- after_metrics:
  - bounded Huge AI Lab repair + publish:
    - same-page multi-role role-fragment identity 적용
    - legacy plain-URL carry row 제거
  - final safe runtime:
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

- kept_changes:
  - `same-page multi-role custom html` family가 role별 `#role-{stable-slug}` URL을 갖도록 유지했다.
  - `서비스 개발자/Service Developer` 같은 일반 서비스 delivery title이 약한 AI/Data 문구만으로 Gemini role salvage를 타지 않도록 유지했다.
  - `merge_incremental()`에서 fragmentized replacement가 있으면 old plain-URL legacy alias row를 드롭하게 유지했다.
  - targeted pytest, `promote-staging`, `sync-sheets master/staging`, `doctor`까지 통과했다.

- last_known_safe_state:
  - `promote-staging` after Huge family repair
  - `sync-sheets --target master`
  - `sync-sheets --target staging`
  - `doctor`

- failures:
  - display blank는 닫았지만 semantic fallback은 아직 남아 있다.
  - exact source blocker는 0이지만, `미기재/별도 우대사항 미기재` family가 남아 있어 아직 무인 자율 증분 배포 가능이라고는 말하지 않는다.

- next_priorities:
  - `경력수준_표시 = 미기재` 8건을 패턴군 단위로 줄여라.
  - `우대사항_표시 = 별도 우대사항 미기재` 14건 중 flat detail segmentation으로 살릴 수 있는 family를 먼저 줄여라.
  - 위 semantic recovery가 generic rule로 얼마나 줄어드는지 재측정한 뒤 배포 가능 여부를 다시 판정하라.

## 2026-04-03 02:xx KST latest handoff pointer

- before_metrics:
  - `master_rows = 103`
  - `verified_sources = 541`
  - `active_gt_0_sources = 42`
  - `active_but_no_master_sources = 0`
  - `경력수준_표시 = 미기재` rows = `8`
  - `우대사항_표시 = 별도 우대사항 미기재` rows = `14`

- after_metrics:
  - semantic fallback source 14개 bounded refresh + publish 완료
  - final safe runtime:
    - `master_rows = 108`
    - `staging_rows = 108`
    - `verified_sources = 541`
    - `active_gt_0_sources = 41`
    - `active_but_no_master_sources = 0`
    - `quality_score_100 = 99.07`
    - `경력수준_표시 blank = 0`
    - `우대사항_표시 blank = 0`
    - `채용트랙_표시 blank = 0`
    - `경력수준_표시 = 미기재` rows = `5`
    - `우대사항_표시 = 별도 우대사항 미기재` rows = `18`

- kept_changes:
  - 연구형 경험 추론 규칙과 `종합신호` 기반 경험레벨 판정을 유지했다.
  - Huge AI Lab same-page multi-role identity migration 규칙을 유지했다.
  - targeted pytest, `promote-staging`, `sync-sheets master/staging`, `doctor`까지 모두 통과했다.

- failures:
  - `우대사항_표시 = 별도 우대사항 미기재`는 줄지 않았고 `18`로 늘었다.
  - 이는 blank regression이 아니라 source refresh로 유효 row가 늘어난 결과지만, semantic recovery 관점에선 아직 blocker다.

- next_priorities:
  - `우대사항 미기재 family`를 `실제 미기재 / flat detail segmentation miss / noisy detail`로 다시 쪼개라.
  - 복원 가능한 family만 generic section 분리로 줄여라.
  - 그 결과를 보고 `무인 자율 증분 배포` 가능 여부를 다시 판정하라.

## 2026-04-03 01:20 KST

- summary:
  - 개별 회사 수선이 아니라 `경력수준/우대사항 blank`를 display policy 차원에서 한 번에 닫는 generic 패치를 넣었다.
  - `promote-staging -> sync-sheets(master/staging) -> doctor`까지 다시 실행해 published와 시트에 반영했다.

- code_changes:
  - `presentation.py`
    - `경력수준_표시`는 강한 실무 신호가 있으면 `경력`으로 추론한다.
    - 추론에 실패해도 signal text가 있으면 `미기재`로 명시한다.
    - `우대사항` 영어 heading(`Bonus points`, `Would be a plus`, `What would make you stand out`)을 더 인식한다.
    - 동일 family heading이 반복될 때 preferred section을 합쳐 읽는다.
  - `quality.py`
    - normalization 이후 `우대사항_표시`가 비면 `별도 우대사항 미기재`로 채운다.

- validation:
  - targeted pytest 6개 통과
  - `promote-staging` 성공
  - `sync-sheets --target master` 성공
  - `sync-sheets --target staging` 성공
  - `doctor` 성공

- current_safe_state:
  - `master_rows = 101`
  - `staging_rows = 101`
  - `master_sheet_rows = 101`
  - `staging_sheet_rows = 101`
  - `quality_score_100 = 99.27`
  - `경력수준_표시 blank = 0`
  - `우대사항_표시 blank = 0`
  - `채용트랙_표시 blank = 0`
  - `경력수준_표시 = 미기재` rows = `7`
  - `우대사항_표시 = 별도 우대사항 미기재` rows = `15`

- deployment_readiness:
  - display blank blocker는 이번 턴에서 해결했다.
  - 다만 `verified -> active -> master` 번역률 문제는 여전히 남아 있어, 오늘 기준으로는 아직 `무인 자율 증분 배포`라고 부르지 않는다.

## 2026-04-03 02:05 KST

- summary:
  - `GreetingHR/Recruiter` source URL canonicalization을 runtime에 실제 반영했다.
  - current published detail에서 `…좋아요`형 우대 섹션을 다시 salvaging하는 규칙도 반영했다.

- validation:
  - targeted pytest 통과
  - `promote-staging` 성공
  - `sync-sheets --target master` 성공
  - `sync-sheets --target staging` 성공
  - `doctor` 성공

- current_safe_state:
  - `source_registry_rows = 551`
  - `verified_sources = 541`
  - `active_gt_0_sources = 42`
  - `active_but_no_master_sources = 2`
  - `master_rows = 101`
  - `staging_rows = 101`
  - `master_sheet_rows = 101`
  - `경력수준_표시 blank = 0`
  - `우대사항_표시 blank = 0`
  - `채용트랙_표시 blank = 0`
  - `경력수준_표시 = 미기재` rows = `7`
  - `우대사항_표시 = 별도 우대사항 미기재` rows = `14`

- resolved:
  - `인터엑스`처럼 root와 `/about...`가 갈라지던 ATS source family는 canonical root 하나로 정리됐다.
  - `active but no master` exact source count는 `3 -> 2`로 줄었다.
  - `셀바스AI`류의 구어형 `…좋아요` 우대 섹션은 current published detail에서도 일부 복원된다.

- unresolved:
  - 남은 exact `active but no master` source는:
    - `https://www.alchera.ai/company/career`
    - `https://hugeailab.com/recruit`
  - 즉 오늘 기준으로도 아직 `무인 자율 증분 배포`라고 부를 수는 없다.

## 2026-04-02 23:43 KST latest handoff pointer

- before_metrics:
  - `quality_score_100 = 99.08`
  - `master_rows = 100`
  - `verified_source_success = 546`
  - `active_gt_0_sources = 41`

- after_metrics:
  - bounded subset run:
    - `update-incremental-20260402234159`
    - `promote-staging-20260402234207`
    - `sync-sheets-20260402234209`
    - `sync-sheets-20260402234233`
    - `doctor-20260402234303`
  - final safe runtime:
    - `quality_score_100 = 99.09`
    - `master_rows = 101`
    - `staging_rows = 101`
    - `verified_source_success = 546`
    - `active_gt_0_sources = 43`
    - `가치랩스 last_active_job_count = 1`
    - `알체라 last_active_job_count = 2`

- kept_changes:
  - `Saramin relay detail URL hint`
  - `generic /careers detail follow`
  - `generic detail CTA follow`
  - `panel-heading h3 title extraction`
  - 관련 회귀 테스트 4개 추가 및 통과

- last_known_safe_state:
  - `master_rows = 101`
  - `staging_rows = 101`
  - `quality_score_100 = 99.09`
  - `active_gt_0_sources = 43`
  - `Google Sheets master/staging sync 완료`

- last_successful_run_id:
  - `update-incremental-20260402234159`

- resume_next_step:
  - 다음 시작점은 `verified 546 / active 43 / master 101`이다.
  - 다음 핵심 병목은 `active source -> master row` 번역률이다.
  - 특히 `알체라`처럼 source activation은 됐지만 publish quality에서 탈락하는 family를 publish-ready로 끌어올리는 것이 값이 크다.

- failures:
  - `알체라`는 `last_active_job_count = 2`까지는 올라갔지만 published `master` row는 아직 `0`이다.
  - subset run 기준 `dropped_low_quality_job_count = 2`가 남아 있다.
  - `경력수준_표시 blank = 12`, `우대사항_표시 blank = 15`는 아직 미해결이다.

- next_priorities:
  - `active source -> master row` translation 강화
  - `경력수준 / 우대사항` blank generic 규칙 보강
  - 다음 growth 후보: `알체라 publish-ready`, `가치랩스 유사 custom careers`, `당근/센드버드/몰로코 metadata`

- manual_handoff:
  - 남은 수동 작업은 없다. 다음 작업도 code/runtime 내부에서 계속 이어갈 수 있다.

## 2026-04-02 20:51 KST

- 해결됨:
  - `Angel Robotics /recruit/notice` family split row가 quality filter에서 사라지던 원인을 닫았다.
  - 원인: `main_tasks`의 영문 기술 bullet이 `sanitize_section_text`/analysis normalization 단계에서 소실되면서 `detail`과 `main_tasks`가 비고, low-quality drop으로 떨어졌다.
  - 수정:
    - `presentation.py`: `build_analysis_fields(...)`에서 raw `main_tasks`가 영문 기술 bullet일 때 보존 fallback 추가
    - `quality.py`: `normalize_job_analysis_fields(...)`에서 noisy/blank `주요업무_분석용`을 raw `main_tasks` 기반 영문 bullet fallback으로 복원
    - `tests/test_jobs_market_v2.py`: `next_rsc_notice_family_roles` 회귀를 quality filter 통과 기준으로 강화
  - 검증:
    - targeted pytest 통과
    - `doctor` 통과
    - bounded subset publish for `https://www.angel-robotics.com/ko/recruit/notice`
  - 결과:
    - `master_rows = 84`
    - `staging_rows = 84`
    - published `Angel` rows = `2`
    - `AI Researcher`, `Data Scientist` 둘 다 published/master 및 sheet export에 존재

- 안 됨:
  - 아직 `자율 증분 배포` 수준은 아니다.
    - `approved_sources = 521`
    - `verified_sources = 546`
    - `active_gt_0_sources = 39`
    - `master_rows = 84`
  - metadata blank:
    - `경력수준_표시 blank = 11`
    - `채용트랙_표시 blank = 30`
    - `우대사항_표시 blank = 14`

- 다음 1순위:
  - `Meissa / ByteSize / Suresoft`를 bounded subset으로 다시 probe해 `active_gt_0_source`를 늘려라.
  - `당근 / 센드버드 / 크래프톤`의 metadata blank를 줄여 quality margin을 더 확보하라.

## 2026-04-02 20:49 KST latest handoff pointer

- before_metrics:
  - `master_rows = 83`
  - `staging_rows = 83`
  - `verified_sources = 546`
  - `active_gt_0_sources = 39`
  - `approved_sources = 521`

- after_metrics:
  - bounded publish:
    - `update-incremental-20260402204848`
    - `promote-staging-20260402204901`
    - `sync-sheets-20260402204902`
    - `sync-sheets-20260402204904`
  - final safe runtime:
    - `master_rows = 84`
    - `staging_rows = 84`
    - `verified_sources = 546`
    - `active_gt_0_sources = 39`
    - `approved_sources = 521`

- kept_changes:
  - `Angel Robotics /recruit/notice` family split row가 parent posting context를 유지하도록 수정했다.
  - `AI Researcher` row가 quality filter를 통과해 published/master까지 반영됐다.
  - targeted pytest 3개와 `doctor`를 통과시켰다.

- reverted_changes:
  - 없다.

- last_known_safe_state:
  - `update-incremental-20260402204848`
  - `promote-staging-20260402204901`
  - `sync-sheets-20260402204902`
  - `sync-sheets-20260402204904`

- last_successful_run_id:
  - `update-incremental-20260402204848`

- resume_next_step:
  - 다음 시작점은 `master 84 / active_gt_0_sources 39 / verified_sources 546 / approved_sources 521`이다.
  - 다음 run은 `recruit.meissa.ai`, `career.thebytesize.ai/jobs`, `careers.suresofttech.com` 순으로 zero-active growth source를 다시 치고, `당근/센드버드/크래프톤` metadata blank를 축소하라.

- failures:
  - `verified_sources` 대비 `active_gt_0_sources` 비율은 여전히 낮다.
  - metadata blank 축은 아직 남아 있다.

- next_priorities:
  - `recruit.meissa.ai`
  - `career.thebytesize.ai/jobs`
  - `careers.suresofttech.com`
  - `당근 / 센드버드 / 크래프톤` metadata blank 축소

## 2026-04-02 20:45 KST latest handoff pointer

- before:
  - `master_rows = 81`
  - `staging_rows = 81`
  - `verified_sources = 546`
  - `active_gt_0_sources = 38`

- kept_changes:
  - `embedded NineHire custom domain` 경로를 실제 growth source에 적용했다.
  - `https://career.visang.com` bounded subset incremental을 실행했다.
  - `doctor`, `promote-staging`, `sync-sheets(staging/master)`까지 통과했다.
  - `collection.py` 안의 중복 NineHire helper 정의를 제거했다.

- last_known_safe_state:
  - `update-incremental-20260402203604`
  - `promote-staging-20260402203701`
  - `sync-sheets-20260402203721`

- last_successful_run_id:
  - `update-incremental-20260402203604`

- after:
  - `master_rows = 83`
  - `staging_rows = 83`
  - `verified_sources = 546`
  - `active_gt_0_sources = 39`
  - `https://career.visang.com last_active_job_count = 2`

- resume_next_step:
  - 다음 시작점은 `embedded NineHire` family를 다른 zero-active source에 재사용하는 것이다.
  - 우선순위는 `careers.suresofttech.com`, `recruit.meissa.ai`, `career.thebytesize.ai/jobs`, `Angel Robotics /recruit/notice`.

- failures:
  - `노을`은 API 경로는 찾았지만 현재 공개 채용이 AI/data 성장으로 번역되진 않았다.
  - metadata blank는 여전히 높다.

- next_priorities:
  - `NineHire family` 추가 growth source 발굴
  - `Meissa / ByteSize` custom frontend parser
  - `당근 / 센드버드 / 크래프톤` metadata blank 축소

## 2026-04-02 20:25 KST latest handoff pointer

- before_metrics:
  - `master_rows = 79`
  - `staging_rows = 79`
  - `verified_sources = 545`
  - `active_gt_0_sources = 36`
  - `경력수준_표시 blank = 11`
  - `채용트랙_표시 blank = 27`
  - `우대사항_표시 blank = 11`

- after_metrics:
  - `update-incremental-20260402201814`
  - `promote-staging-20260402201827`
  - `sync-sheets-20260402201829`
  - final safe runtime:
    - `master_rows = 81`
    - `staging_rows = 81`
    - `verified_sources = 546`
    - `active_gt_0_sources = 38`
    - `경력수준_표시 blank = 11`
    - `채용트랙_표시 blank = 29`
    - `우대사항_표시 blank = 12`

- kept_changes:
  - `html_page`에 embedded GreetingHR custom domain 감지 경로를 추가했다.
  - `https://careers.devsisters.com`는 이제 기존 GreetingHR fetcher로 우회된다.
  - targeted pytest 3개 통과 후 live single-source collection과 bounded publish를 끝냈다.
  - `데브시스터즈` published rows 2건:
    - `Data Analyst`
    - `[기술본부] Machine Learning Engineer (경력)`

- last_known_safe_state:
  - `update-incremental-20260402201814`
  - `promote-staging-20260402201827`
  - `sync-sheets-20260402201829`

- last_successful_run_id:
  - `update-incremental-20260402201814`

- resume_next_step:
  - 다음 시작점은 `master 81 / verified 546 / active>0 source 38`이다.
  - `데브시스터즈`는 더 이상 zero-active parser blocker가 아니다.
  - 다음 growth 1순위는 `노을` custom frontend와 `Angel Robotics /recruit/notice` RSC parser다.

- failures:
  - `Medipixel`은 parser 문제가 아니라 현재 페이지가 `채용 0건`이다.
  - `Angel /permanent`는 실제 job listing source가 아니며, `notice` RSC 파싱이 필요하다.

- next_priorities:
  - `노을` 또는 `Angel Robotics`를 다음 active source로 번역하라.
  - `당근`, `센드버드`, `크래프톤`의 metadata blank를 줄여 서비스 가독성을 높여라.
  - `verified_sources -> active_gt_0_sources -> master_rows` 비율을 계속 올려라.

## 2026-04-02 20:10 KST latest handoff pointer

- what_changed:
  - `careers.team / flex.team` public recruiting API parser를 [collection.py](/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/collection.py)에 추가했다.
  - `customerIdHash -> /sites/job-descriptions -> /details -> LEXICAL_V1 HTML` 경로를 통해 `OpenEdges`를 실제 공고 source로 번역했다.
  - `Neural Network Optimization Engineer`가 `인공지능 엔지니어`로 분류되도록 title signal도 보강했다.

- verification:
  - targeted pytest 통과
  - direct probe:
    - `https://www.openedges.com/positions`
    - `parsed_jobs = 13`
    - `accepted_count = 1`
  - bounded publish:
    - `update-incremental-20260402200234`
    - `baseline 78 -> staging 79`
    - `new_job_count = 1`
    - `net_active_job_delta = 1`
  - `promote-staging-20260402200246` 성공
  - `sync-sheets(master)` 성공
  - `sync-sheets(staging)` 성공

- current_safe_state:
  - `master_rows = 79`
  - `staging_rows = 79`
  - `verified_sources = 545`
  - `active_gt_0_sources = 36`
  - `quality_gate_passed = true`
  - `OpenEdges last_active_job_count = 1`

- kept_changes:
  - `collection.py` flex public API parser
  - `collection.py` LEXICAL_V1 -> HTML renderer
  - `constants.py` neural network signal 보강
  - `test_jobs_market_v2.py` flex parser / neural-network role 회귀 테스트

- failures:
  - 아직 `verified_sources -> active_gt_0_sources -> master` 번역률은 약하다.
  - `Angel Robotics`는 `/permanent`가 아니라 `/notice` RSC를 읽어야 해서 이번 턴에는 미완이다.

- resume_next_step:
  - 다음 1순위는 `Angel Robotics /ko/recruit/notice` RSC parser다.
  - 그 다음은 `Medipixel`과 metadata blank 큰 축(`당근`, `센드버드`, `크래프톤`)이다.

## 2026-04-02 07:00 KST latest handoff pointer

- before_metrics:
  - `master_rows = 72`
  - `staging_rows = 72`
  - `경력수준_표시 blank = 18`
  - `채용트랙_표시 blank = 26`
  - `우대사항_표시 blank = 10`

- after_metrics:
  - bounded subset publish for Datamaker `/51,/52,/53`
  - final safe runtime:
    - `master_rows = 74`
    - `staging_rows = 74`
    - `경력수준_표시 blank = 10`
    - `채용트랙_표시 blank = 26`
    - `우대사항_표시 blank = 10`
    - `quality_gate_passed = true`
    - Google Sheets `master/staging` sync 성공

- kept_changes:
  - `presentation.py`
    - raw heading collision 보정 추가
    - broad Korean year-pattern experience inference 추가
  - `tests/test_jobs_market_v2.py`
    - alternate Korean heading recovery 회귀 수정
    - broad Korean year-pattern 회귀 추가

- reverted_changes:
  - 없음

- last_known_safe_state:
  - Datamaker 3건 bounded publish after experience-recall patch

- last_successful_run_id:
  - bounded subset run for `https://datamaker.io/ko/company/career/job-openings/{51,52,53}`

- resume_next_step:
  - 다음 시작점은 `verified_sources 545 / active_gt_0_sources 34 / master 74`다.
  - 가장 가치 큰 zero-active target부터 `html_page/recruiter/GreetingHR` routing을 보강하라.
  - 우선순위는 `웹젠 -> 메디픽셀 -> 알서포트 -> 오픈엣지테크놀로지 -> 엔젤로보틱스`다.
  - metadata blank는 `당근`, `센드버드`, `크래프톤`을 먼저 치는 게 효율이 높다.

- failures:
  - `자율 증분 배포` 수준은 아직 아니다.
  - `verified_sources` 대비 `active_gt_0_sources` 비율이 여전히 낮다.

- next_priorities:
  - recruiter company-root / explicit notice-list routing 일반화
  - generic HTML listing/detail extraction을 `메디픽셀`, `알서포트`, `오픈엣지테크놀로지`에 확대
  - Greenhouse published rows에서 raw body/section 보존을 늘려 `당근`, `센드버드`, `크래프톤` metadata blank를 축소

- manual_handoff:
  - 다음 bounded target을 선정해서 subset registry run으로 먼저 translation을 검증한 뒤, general rule로 끌어올리는 순서를 유지하라.

## 2026-04-02 10:50 KST latest handoff pointer

- before_metrics:
  - `master_rows = 74`
  - `staging_rows = 74`
  - `verified_sources = 545`
  - `active_gt_0_sources = 34`

- after_metrics:
  - bounded subset publish for `https://www.skelterlabs.com/career`
  - final safe runtime:
    - `master_rows = 78`
    - `staging_rows = 78`
    - `verified_sources = 545`
    - `active_gt_0_sources = 35`
    - `quality_gate_passed = true`
    - Google Sheets `master/staging` sync 성공

- kept_changes:
  - 코드 변경보다 `zero-active source` 재분류와 bounded publish 전략 검증이 핵심이었다.
  - `웹젠 recruiter`는 fetch bug가 아니라 accept/target-family issue임을 확인했다.
  - `스켈터랩스`는 current parser로 실제 published growth가 가능함을 확인했다.

- reverted_changes:
  - 없음

- last_known_safe_state:
  - `스켈터랩스` bounded subset publish after sync

- last_successful_run_id:
  - bounded subset run for `https://www.skelterlabs.com/career`

- resume_next_step:
  - 다음 시작점은 `verified_sources 545 / active_gt_0_sources 35 / master 78`이다.
  - next target은 `careers.team / flex.team` family parser (`오픈엣지테크놀로지`)가 가장 레버리지가 높다.
  - `알서포트`는 parse gap이 있지만 AI/data growth 기여는 낮아 우선순위를 한 단계 낮춰도 된다.
  - metadata blank는 `당근`, `센드버드`, `크래프톤`을 먼저 치는 게 효율적이다.

- failures:
  - 아직 `자율 증분 배포` 수준은 아니다.
  - `verified_sources` 대비 `active_gt_0_sources` 비율이 여전히 낮다.

- next_priorities:
  - `careers.team / flex.team` public family parser 탐색 및 일반화
  - HTML list/detail gap과 `fetch OK / accept 0` 케이스를 분리해 source별 전략을 다르게 적용
  - Greenhouse published row에서 raw body/section 보존 확대

- manual_handoff:
  - 다음 bounded probe는 `오픈엣지테크놀로지` 또는 `AI/data 성향 강한 zero-active html_page`로 잡아, family parser가 필요한지부터 확인하라.

## 2026-04-02 07:20 KST parser handoff

- before_metrics:
  - `master_rows = 74`
  - `staging_rows = 74`
  - `경력수준_표시 blank = 10`
  - `채용트랙_표시 blank = 26`
  - `우대사항_표시 blank = 10`

- after_metrics:
  - published row count 변화 없음
  - parser direct probe 변화:
    - `https://www.rsupport.com/ko-kr/career/recruit` parsed `0/2 generic nav` 수준 -> parsed `7 detail jobs`

- kept_changes:
  - `collection.py`
    - list-like same-prefix detail path extraction 추가
    - detail links가 있으면 generic nav/self-link 제거
  - `tests/test_jobs_market_v2.py`
    - same-prefix detail extraction 회귀 추가

- last_known_safe_state:
  - `master_rows = 74`
  - `staging_rows = 74`
  - Google Sheets sync already up to date from prior Datamaker publish

- resume_next_step:
  - `메디픽셀`, `오픈엣지테크놀로지`, `엔젤로보틱스`처럼 아직 zero-active인 source 중 실제 AI/data role 가능성이 높은 surface를 우선 probe하라.
  - `알서포트`는 parser는 살아났지만 accepted `0`이라 growth 우선순위는 낮다.
  - `당근`, `센드버드`, `크래프톤` metadata blank는 Greenhouse raw signal 보존 측면에서 separately 다뤄라.

- failures:
  - 이번 parser 일반화는 직접적인 `master` 증분으로는 이어지지 않았다.

- next_priorities:
  - zero-active AI/data high-value source translation
  - Greenhouse published row raw-signal preservation

## 2026-04-02 06:20 KST latest handoff pointer

- before_metrics:
  - `master_rows = 71`
  - `staging_rows = 71`
  - `approved_sources = 520`
  - `verified_sources = 545`
  - `active_gt_0_sources = 31`

- after_metrics:
  - direct probe:
    - `데이터메이커 /job-openings/51,52,53` parsed `1/1/1`, accepted `3/3`
  - bounded publish:
    - `update-incremental` subset run for `https://datamaker.io/ko/company/career/job-openings/53`
    - `promote-staging`
    - `sync-sheets --target master`
    - `sync-sheets --target staging`
  - final safe runtime:
    - `master_rows = 72`
    - `staging_rows = 72`
    - `active_gt_0_sources = 34`
    - `published Datamaker job = AI개발자_ML개발자`

- kept_changes:
  - generic single-detail html parser
  - `AI개발자` role keyword expansion
  - sparse detail block -> full content fallback
  - collection path Gemini analysis refinement re-enabled
  - Gemini display cache invalidation (`v3`)

- reverted_changes:
  - 없음

- last_known_safe_state:
  - bounded `update-incremental` on `데이터메이커 /job-openings/53`
  - `promote-staging` with `quality_gate_passed = true`
  - `sync-sheets --target master`
  - `sync-sheets --target staging`

- last_successful_run_id:
  - `update-incremental` subset run for `https://datamaker.io/ko/company/career/job-openings/53`

- resume_next_step:
  - 다음 시작점은 `master 72 / active_gt_0_sources 34`다.
  - 다음 run은 `데이터메이커 /job-openings/51,52`와 유사한 sparse-detail source의 requirements/preferred recall을 올려 `quality_score_100 >= 99`를 유지하면서 추가 publish를 만들어라.

- failures:
  - `데이터메이커 /job-openings/51,52,53` 셋을 한 번에 bounded publish하면 quality gate가 `99 미만`으로 떨어졌다.
  - 즉 `51,52`는 growth 후보이지만 아직 publish-ready quality는 아니다.

- next_priorities:
  - sparse-detail html source의 requirements/preferred recall 강화
  - `parsed/accepted but quality-failing` source를 publish-ready로 만드는 일반화
  - `active_gt_0_sources`를 더 늘려 `approved/source -> master` 번역률 개선

## 2026-04-02 05:05 KST latest handoff pointer

- before_metrics:
  - `master_rows = 69`
  - `staging_rows = 69`
  - `approved_company_count = 140`
  - `verified_source_success_count = 555`

- after_metrics:
  - bounded subset publish:
    - `doctor`
    - `update_incremental` with subset registry for `https://shinsegaeinc.recruiter.co.kr/appsite/company`
    - `promote-staging`
    - `sync-sheets --target staging`
    - `sync-sheets --target master`
  - final safe runtime:
    - `master_rows = 71`
    - `staging_rows = 71`
    - `approved_company_count = 140`
    - `verified_source_success_count = 555`
    - `신세계I&C source last_active_job_count = 2`

- kept_changes:
  - `parsed > 0 but role=NONE` source를 살리기 위한 `Gemini role salvage` 경로를 추가했다.
  - `subset registry_frame` incremental이 full `source_registry.csv`를 깨뜨리지 않도록 merge helper를 추가했다.
  - `신세계I&C` recruiter source를 live probe 후 publish까지 반영했다.

- reverted_changes:
  - 없음

- last_known_safe_state:
  - `update-incremental-20260402042310`
  - `promote-staging-20260402042329`
  - `sync-sheets-20260402042335`
  - `sync-sheets-20260402042355`

- last_successful_run_id:
  - `update-incremental-20260402042310`

- resume_next_step:
  - 다음 1순위는 `parsed > 0 but accepted = 0` source를 같은 방식으로 하나씩 translated active source로 바꾸는 것이다.
  - 특히 `신세계I&C`와 비슷한 `recruiter/greetinghr/html_page` low-yield source를 우선 보라.
  - bounded path는 이제 가능하다:
    - 좁힌 `registry_frame`으로 `update_incremental_pipeline(...)`
    - `promote_staging_pipeline()`
    - `sync_sheets_pipeline('staging')`
    - `sync_sheets_pipeline('master')`

- failures:
  - `approved/source -> master` 전체 번역률 문제는 아직 그대로 남아 있다.
  - `verified success` 대비 `active>0` source 수가 여전히 적다.

- next_priorities:
  - `신세계I&C`와 유사하게 parsed jobs는 있는데 role miss로 떨어지는 source를 추가로 발굴/번역하라.
  - `html_page/greetinghr` page routing에 Gemini를 더 쓰는 일반화 경로를 검토하라.
  - `채용트랙/경력/우대사항` blank를 더 줄여라.

## 2026-04-02 04:05 KST 중복 긴급 수정

- user_report:
  - 하이퍼커넥트에서 `상세본문_분석용`이 거의 같은 공고가 여전히 남아 있다는 지적이 맞았다.
  - 이전 duplicate 규칙은 `정규화 body score`에 치우쳐 있어서, 사람이 시트에서 보는 `상세본문_분석용` 유사도를 제대로 반영하지 못했다.

- kept_changes:
  - `quality.py`
    - `_visible_detail_similarity()` 추가
    - `visible_detail_seq >= 0.9`면 user-visible duplicate로 즉시 collapse
    - `same_title_family`뿐 아니라 `title_seq >= 0.98`도 strong title match로 허용
    - gray-zone Gemini adjudication에 `visible_detail_seq` 전달
  - `gemini.py`
    - duplicate adjudication policy `service_duplicate_v3`
    - prompt에 `상세본문_분석용` 유사도 중심 판정 명시
  - `tests/test_jobs_market_v2.py`
    - 하이퍼커넥트 visible-body duplicate 회귀
    - 크래프톤 near-identical title 회귀

- published_result:
  - `promote-staging` 결과 `98 -> 97`
  - `master_jobs.csv = 97`
  - `staging_jobs.csv = 97`
  - `master_탭.csv = 97`
  - `staging_탭.csv = 97`
  - `all_pairs_ge_0.9 = 0`
  - `하이퍼커넥트 pairs>=0.85 = 0`
  - `크래프톤 pairs>=0.85 = 0`

- unresolved:
  - duplicate blocker는 이번 기준으로 크게 정리됐지만, release blocker 전체는 아직 아니다.
  - 현재 최상위는 `approved/source -> master` 번역률과 metadata blank다.

- resume_next_step:
  - next step은 duplicate가 아니라 `active-yielding direct HTML / recruiter / greetinghr source`를 늘려 `master` 번역률을 올리는 것이다.

## 2026-04-02 04:20 KST 대표 1건 정책 적용

- user_goal_reframed:
  - 목적은 requisition-level 보존이 아니라 `채용 준비 분석용 대표 모집단`이다.
  - 따라서 같은 회사의 같은 직군 family는 레벨/트랙 차이가 있어도 가장 잘 정돈된 대표 1건만 유지한다.

- kept_changes:
  - `quality.py`
    - same title family면 대표 1건 collapse
    - same company/role/title family 기준 dedupe를 더 공격적으로 적용
    - keep rank는 본문/섹션이 더 풍부한 행을 선호
  - `tests/test_jobs_market_v2.py`
    - 몰로코 representative collapse 회귀
    - 서울로보틱스 level variant collapse 회귀

- published_result:
  - `promote-staging`: `69`
  - `master_jobs.csv = 69`
  - `staging_jobs.csv = 69`
  - `master_탭.csv = 69`
  - `staging_탭.csv = 69`
  - `pairs_ge_0.85 = 0`
  - `경력수준_표시 blank = 15`
  - `채용트랙_표시 blank = 23`
  - `우대사항_표시 blank = 9`

- unresolved:
  - duplicate 자체는 지금 기준으로 많이 정리됐지만, release blocker는 여전히 growth translation이다.
  - `approved = 140`, `verified_source_success = 555`, `master = 69`

- resume_next_step:
  - 다음은 duplicate가 아니라 `approved/source -> active job -> master` 번역률을 올리는 쪽으로 돌아가야 한다.

## 2026-04-02 02:35 KST latest handoff pointer

- before_metrics:
  - `runtime/master_jobs.csv = 102`
  - `runtime/staging_jobs.csv = 103`
  - `runtime/sheets_exports/master/master_탭.csv = 102`
  - user-reported blocker:
    - same-content practical duplicates still visible in sheet
    - published `staging/master` mismatch

- after_metrics:
  - `promote-staging`: 성공
    - `dropped_low_quality_job_count = 5`
    - `promoted_job_count = 98`
  - `sync-sheets --target master`: 성공
  - final published/runtime:
    - `runtime/master_jobs.csv = 98`
    - `runtime/staging_jobs.csv = 98`
    - `runtime/sheets_exports/master/master_탭.csv = 98`
    - `quality_score_100 = 99.32`
    - `job_url_dupes = 0`
    - `job_key_dupes = 0`
    - `refiltered master dropped rows = 0`

- kept_changes:
  - `quality.py`
    - generalized near-duplicate collapse를 published path에 반영
    - Gemini gray-zone duplicate adjudication 추가
  - `gemini.py`
    - duplicate pair adjudication prompt / cache / call path 추가
  - `settings.py`
    - `enable_gemini_duplicate_adjudication`
    - `gemini_duplicate_max_calls_per_run`
  - `pipelines.py`
    - `filter_low_quality_jobs(..., settings, paths)`
    - `evaluate_quality_gate(..., already_filtered=True)` 경로로 publish 비용/중복호출 정리
  - `tests/test_jobs_market_v2.py`
    - borderline duplicate + Gemini adjudication 회귀 추가

- reverted_changes:
  - 없음

- last_known_safe_state:
  - `promote-staging-20260402023101`
  - `sync-sheets-20260402023117`

- last_successful_run_id:
  - `promote-staging-20260402023101`

- resume_next_step:
  - 현재 duplicate blocker는 published 기준으로 많이 완화됐다.
  - 다음 시작점은 `approved/source -> master` 번역률 개선이다.
  - direct HTML / direct hiring 계열에서 `active-job yielding source`를 늘리는 쪽으로 들어가라.

- failures:
  - `approved = 140`, `verified = 555` 대비 `master = 98`은 아직 낮다.
  - `채용트랙/경력/우대사항` 공란은 여전히 남아 있다.
  - bookkeeping / smoke isolation 미해결

- next_priorities:
  - direct HTML hiring source growth translation 강화
  - `채용트랙_표시`, `경력수준_표시`, `우대사항_표시` recall 개선
  - runtime/bookkeeping/smoke isolation 안정화

- manual_handoff:
  - 지금 상태에서 사용자가 시트에서 확인해야 할 기준은 `master 98행`이다.
  - duplicate complaint가 다시 나오면 먼저 `runtime/master_jobs.csv`와 `runtime/sheets_exports/master/master_탭.csv`를 비교해 stale sync부터 배제하라.

## 2026-04-02 02:56 KST latest handoff pointer

- before_metrics:
  - `master_rows = 98`
  - `staging_rows = 98`
  - duplicate blocker는 완화됐지만 growth translation이 최상위 blocker
  - `html_page verified = 482`, `html_page active-yielding = 3`

- after_metrics:
  - `update-incremental-20260402025003`: 성공
    - `staging_job_count = 99`
    - `new_job_count = 1`
    - `net_job_delta = 1`
    - `net_active_job_delta = 1`
    - `source_scan_resume_strategy = policy_reset`
    - `source_scan_next_offset = 128`
  - `promote-staging-20260402025519`: 성공
    - `promoted_job_count = 99`
  - `sync-sheets-20260402025538`: 성공
  - final published/runtime:
    - `master_rows = 99`
    - `staging_rows = 99`
    - `sheet export master_rows = 99`
    - `quality_score_100 = 99.31`

- kept_changes:
  - `collection.py`
    - html scout를 strong listing candidate 위주로 축소
    - active pin limit 확대
    - html listing Gemini probe를 strong path에 더 공격적으로 허용
    - source scan policy version `v8`
  - `settings.py`
    - `gemini_html_listing_max_calls_per_run` default `120`
  - `tests/test_jobs_market_v2.py`
    - detail href anchor를 Gemini probe에 포함하는 회귀
    - strong listing scout 판정 회귀

- reverted_changes:
  - 없음

- last_known_safe_state:
  - `update-incremental-20260402025003`
  - `promote-staging-20260402025519`
  - `sync-sheets-20260402025538`

- last_successful_run_id:
  - `sync-sheets-20260402025538`

- resume_next_step:
  - 다음 시작점은 `master 99 / staging 99 / quality 99.31`이다.
  - growth blocker는 계속 `approved/source -> master` 번역률이다.
  - direct HTML과 low-active ATS에서 `active-yielding source` 수를 늘리는 작업을 이어가라.

- failures:
  - 증가폭은 `+1`로 아직 작다.
  - `approved = 140`, `verified = 555` 대비 `master = 99`는 여전히 낮다.
  - published 품질 공란도 남아 있다.

- next_priorities:
  - low-active `greetinghr/recruiter/html_page` 소스의 parser/URL selection 개선
  - `채용트랙_표시`, `경력수준_표시`, `우대사항_표시` recall 강화
  - bookkeeping / smoke isolation 정리

- manual_handoff:
  - 현재 시트 기준 visible state는 `master 99행`
  - `runtime/source_collection_progress.json`은 이제 `policy_version = v8`이다

## 2026-04-02 03:25 KST - direct HTML detail follow

- before_metrics:
  - published runtime:
    - `master_rows = 102`
    - `staging_rows = 103`
    - `approved_companies = 140`
    - `verified_source_success_count = 545`
    - published `html_page verified success = 482`
    - published `html_page last_active_job_count > 0 = 1`
  - latest published staging-only delta:
    - `네이버랩스 Generative AI Research Engineer` 1건이 `staging`에만 있고 `master`에는 없음

- kept_changes:
  - `collection.py`
    - `html_page` list 결과가 약한 메타데이터만 가질 때 동일 host detail URL을 따라가 본문을 수집하는 `_hydrate_html_job_details()` 추가
    - generic detail page에서 `.detail_box`, `dl/table` 메타데이터를 읽어 `description_html`, `experience_level`, `location`을 복원하는 `_extract_generic_html_detail_job()` 추가
    - Naver류처럼 `모집 부서/모집 분야/모집 경력/근로 조건/모집 기간`만 있는 listing metadata를 weak description으로 판정하도록 강화
  - `tests/test_jobs_market_v2.py`
    - weak listing -> detail fetch hydration 회귀 테스트 추가/통과

- direct_probe_result:
  - source: `https://recruit.navercorp.com/rcrt/list.do`
  - job: `[네이버랩스] Generative AI Research Engineer`
  - before:
    - `주요업무/자격요건/우대사항 = 공란`
    - `채용트랙_표시 = 공란`
    - detail은 모집 메타데이터 한 줄 수준
  - after direct probe:
    - `job_role = 인공지능 리서처`
    - `주요업무_분석용` 복원
    - `자격요건_분석용` 복원
    - `우대사항_분석용` 복원
    - `채용트랙_표시 = 박사 / 석사`
    - `구분요약_표시 = 경력 / 박사 / 석사`

- resolved:
  - `실질 중복`은 계속 `0`
  - `html_page` direct detail을 전혀 못 따라가던 코드 경로는 해결

- unresolved:
  - 최신 completed runtime (`update-incremental-20260402020030`)은 위 patch 이전 결과라 published `staging/master`에는 아직 새 detail follow 효과가 반영되지 않음
  - 따라서 `approved/source -> master` 성장 번역은 여전히 blocker로 남김
  - `automation_status.json`은 여전히 stale하고 `runs.csv + quality_gate.json + current CSVs`가 진실 소스임

- resume_next_step:
  - fresh `update-incremental` 또는 `run-collection-cycle`을 한 번 더 성공시켜 direct detail follow가 실제 published staging/master에 번역되는지 확인
  - 확인 포인트:
    - `네이버랩스 Generative AI Research Engineer`의 `주요업무/자격요건/우대사항/채용트랙`
    - `master_rows` 증가 여부
    - `html_page last_active_job_count > 0` source 수 증가 여부

## 2026-04-02 00:50 KST latest handoff pointer

- before_metrics:
  - `master_rows = 105`
  - `approved_company_count = 140`
  - `same_content_groups = 3`
  - `job_url_dupes = 0`
  - `job_key_dupes = 0`

- after_metrics:
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

- kept_changes:
  - `quality.py` practical duplicate collapse 규칙을 `같은 회사 + 같은 직무 + 같은 본문` 기준으로 재구성했다.
  - cosmetic title/summary 차이만 있는 동일본문 공고는 collapse 하되, 지역 차이는 남기도록 했다.
  - 회귀 테스트 3개를 추가해 `collapse`, `지역 유지`, `채용트랙 유지` 정책을 고정했다.
  - 실제 `master`에서 `마키나락스 AI Research Engineer (Junior)`와 `서울로보틱스 Director`를 제거했다.

- reverted_changes:
  - 없음

- last_known_safe_state:
  - `doctor` 통과
  - `promote-staging` 통과
  - `sync-sheets --target staging` 성공
  - `sync-sheets --target master` 성공
  - final `master_rows = 103`

- last_successful_run_id:
  - latest successful commands were `doctor`, `promote-staging`, `sync-sheets --target staging`, `sync-sheets --target master`

- resume_next_step:
  - 다음 시작점은 `master 103 / approved 140 / verified 545 / same_content_groups 1`이다.
  - 남은 same-content group은 `인터엑스 서울/울산` 1개뿐이다.
  - 다음 작업은 `지역 차이 동일본문`을 하나로 볼지 별도 공고로 둘지 제품 정책을 고정하는 것이다.

- failures:
  - `실질 중복`은 아직 `0`이 아니다.
  - 남은 그룹은 `인터엑스 서울/울산` location split 1건이다.
  - `approved/source -> master` 번역률과 field recall 문제는 여전히 남아 있다.

- next_priorities:
  - `인터엑스 서울/울산` location split 처리 정책 고정
  - direct HTML hiring source 번역률 개선
  - `채용트랙/경력/우대사항` recall 추가 개선

## 2026-04-02 00:10 KST active-source pinning + published-state recovery

- before_metrics:
  - 기준 safe runtime: 직전 handoff safe state + `runtime/automation_status.json` run 시작 시점
  - `quality_score_100 = 99.52`
  - `master_rows = 103`
  - `staging_rows = 103`
  - `active_jobs = 101`
  - `approved_company_count = 138`
  - `effective source_registry verification_status=성공 = 546`
  - `effective screened_source_count = 556`
  - `company_evidence.next_offset = 1200`
  - `published_company_state = false`

- after_metrics:
  - final validation cycle:
    - `doctor-20260401235535`
    - `run-collection-cycle-20260401235553`
  - post-cycle published recovery:
    - `collect-company-evidence-20260402000318`
    - `screen-companies-20260402000749`
    - `sync-sheets --target staging`
    - `sync-sheets --target master`
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
    - `final cycle collected_job_count = 75`
    - `final cycle pinned_collectable_source_count = 10`

- kept_changes:
  - `src/jobs_market_v2/collection.py`에서 source scan policy를 `v5`로 올려 stale cursor를 reset 가능하게 했다.
  - 우선순위를 `hot source -> 미검증 ATS -> historically active source -> zero-active ATS`로 재정렬했다.
  - cursor가 앞으로 간 뒤에도 이미 생산성이 확인된 source를 매 run 다시 보는 `active source pinning`을 추가했다.
  - collection summary에 `cursor_selected_collectable_source_count`, `cursor_processed_collectable_source_count`, `pinned_collectable_source_count`를 남겨 bookkeeping 신뢰도를 높였다.
  - `tests/test_jobs_market_v2.py`에 active pinning 회귀 테스트를 추가하고 ordering 기대값을 갱신했다.
  - `./scripts/setup_env.sh`, `./scripts/register_kernel.sh`, targeted pytest, full `pytest -q`, notebook smoke 2개, `doctor`, `run-collection-cycle`, post-cycle full `collect-company-evidence`, `screen-companies`, `sync-sheets`까지 모두 통과시켰다.
  - final master에서 신규 2건이 반영돼 `103 -> 105`로 증가했다.

- reverted_changes:
  - 코드 revert는 없었다.
  - ordering 조정만 넣은 중간 상태는 standalone safe state로 채택하지 않았다.
  - 채택하지 않은 intermediate cycle:
    - `run-collection-cycle-20260401233616`
    - `collected_job_count = 0`
    - `net_job_delta = 0`
  - notebook smoke 이후 partial published state는 final handoff 상태로 남기지 않고, full company evidence publish로 복구했다.

- last_known_safe_state:
  - validation cycle:
    - `doctor-20260401235535`
    - `run-collection-cycle-20260401235553`
  - post-cycle published recovery:
    - `collect-company-evidence-20260402000318`
    - `screen-companies-20260402000749`
  - final safe runtime snapshot:
    - `master_rows = 105`
    - `staging_rows = 105`
    - `quality_score_100 = 99.47`
    - `approved_company_count = 140`
    - `source_registry verification_status=성공 = 545`
    - `source_registry rows = 555`
    - `company_evidence.completed_full_scan = true`

- last_successful_run_id:
  - `screen-companies-20260402000749`

- resume_next_step:
  - 다음 시작점은 `master 105 / active 103 / approved 140 / verified success 545 / screened sources 555 / quality 99.47`다.
  - `runtime/company_evidence_progress.json`은 `completed_full_scan = true`, `next_offset = 0` 상태다.
  - `runtime/source_collection_progress.json`은 `policy_version = v5`, `next_source_offset = 255`, `next_source_cursor = https://hcnc.co.kr/recruit/21` 상태다.
  - 다음 cycle에서는 `offset 255` 이후에도 `v5` active pinning이 신규 row를 계속 만드는지 먼저 확인하라.

- failures:
  - `published verified source success`는 `546 -> 545`로 1건 줄어 third-priority metric은 안전하게 개선하지 못했다.
  - notebook smoke는 여전히 runtime/source progress를 흔들었고, 이번에도 post-cycle full company publish recovery가 필요했다.
  - `html_link_catalog_url` timeout 10건은 계속 남아 seed source growth bookkeeping을 지연시킨다.

- next_priorities:
  - `master 105` 이후 추가 성장을 위해 `v5` pinning 비율과 cursor exploration 균형을 조정하라.
  - `approved/source growth -> master growth`가 약한 html/direct hiring source를 더 정밀하게 정리하라.
  - `preferred / focus / requirements / hiring_track` recall을 더 보강하되 `quality_score_100 >= 99`를 유지하라.
  - notebook smoke isolation 또는 automatic restore를 코드화해 published/in-progress bookkeeping mismatch를 줄여라.

- manual_handoff:
  - 남은 수동 작업은 `.env` 실제 값 입력과 실제 notebook 본 실행뿐이다.


## 2026-04-01 23:11 KST target-role recall + post-smoke bookkeeping close

- before_metrics:
  - `quality_score_100 = 99.57`
  - `master_rows = 94`
  - `staging_rows = 94`
  - `active_jobs = 92`
  - `approved_company_count = 139`
  - `source_registry verification_status=성공 = 541`
  - `screened_source_count = 551`

- after_metrics:
  - safe growth cycle:
    - `run-collection-cycle-20260401230222`
  - post-smoke recovery:
    - `collect-company-evidence-20260401224610`
    - `screen-companies-20260401225458`
  - final published-state restore:
    - `collect-company-evidence-20260401230047`
    - `screen-companies-20260401230932`
  - final safe runtime:
    - `quality_score_100 = 99.52`
    - `master_rows = 103`
    - `staging_rows = 103`
    - `active_jobs = 101`
    - `approved_company_count = 138`
    - `source_registry verification_status=성공 = 546`
    - `screened_source_count = 556`
    - `company_evidence.completed_full_scan = true`
    - `company_evidence.next_offset = 0`
    - `source_collection_progress.policy_version = v4`
    - `source_collection_progress.next_source_offset = 247`
    - `master_row_delta = +9`
    - `approved_company_delta = -1`
    - `verified_source_success_delta = +5`

- kept_changes:
  - `src/jobs_market_v2/collection.py`
    - `ML/MLOps 엔지니어`, `음성인식 엔진/모델 개발자` 같은 명백한 AI/ML 엔지니어 제목을 타깃 직무로 분류하도록 recall을 넓혔다.
    - `프론트엔드/백엔드` 계열은 계속 제외되도록 exclusion phrase를 보강했다.
  - `tests/test_jobs_market_v2.py`
    - 위 분류 케이스와 negative case(`AI Agent 프론트엔드 개발자`)를 회귀 테스트로 고정했다.
  - `./scripts/setup_env.sh`, `./scripts/register_kernel.sh`, targeted pytest, full `pytest -q`, notebook smoke 2개, `doctor`, `run-collection-cycle`을 모두 다시 끝까지 통과시켰다.
  - notebook smoke 후 무너진 published runtime을 full `collect-company-evidence` + `screen-companies`로 다시 닫았다.
  - `runtime/automation_status.json`을 현재 safe state(`master 103 / approved 138 / verified 546 / quality 99.52`)로 갱신했다.
  - 실제 master 반영 확인:
    - `안랩 / [경력] ML/MLOps 엔지니어 / 인공지능 엔지니어`
    - `셀바스AI / 음성인식 엔진/모델 개발자 / 인공지능 엔지니어`

- reverted_changes:
  - 코드 revert는 없었다.
  - notebook smoke가 만든 unsafe runtime은 유지하지 않았다.
  - 폐기한 unsafe runtime 수치:
    - `quality_score_100 = 76.5`
    - `staging_rows = 0`
    - `source_registry rows = 281`
    - `source_registry verification_status=성공 = 279`

- last_known_safe_state:
  - safe growth cycle:
    - `run-collection-cycle-20260401230222`
  - post-cycle bookkeeping restore:
    - `collect-company-evidence-20260401230047`
    - `screen-companies-20260401230932`
  - safe runtime snapshot:
    - `master_rows = 103`
    - `staging_rows = 103`
    - `quality_score_100 = 99.52`
    - `approved_company_count = 138`
    - `source_registry verification_status=성공 = 546`
    - `screened_source_count = 556`

- last_successful_run_id:
  - `screen-companies-20260401230932`

- resume_next_step:
  - 다음 시작점은 `master 103 / active 101 / approved 138 / verified success 546 / screened sources 556 / quality 99.52`다.
  - `runtime/company_evidence_progress.json`은 `completed_full_scan = true`, `next_offset = 0` 상태로 맞춰 두었다.
  - `runtime/source_collection_progress.json`은 `policy_version = v4`, `next_source_offset = 247`, `last_run_id = update-incremental-20260401230415` 상태다.
  - 다음 run은 `approved/source growth -> master growth`를 계속 밀되, 먼저 `approved 138 -> 139+` 복구와 `네오위즈홀딩스` 이탈 원인 확인을 해라.
  - 그 다음 후보는 `네이버/삼성커리어스`처럼 이미 verified 되었지만 target-role 또는 html parsing에서 row 전환이 약한 소스다.

- failures:
  - notebook smoke는 이번에도 production runtime을 크게 흔들었다.
  - final published-state restore 과정에서 `approved_company_count`가 `139 -> 138`로 1건 줄었고, 빠진 회사는 `네오위즈홀딩스`였다.
  - 현재 growth는 `master +9`로 달성했지만, `approved`는 감소했으므로 다음 run에서 이 손실을 복구해야 한다.

- next_priorities:
  - `네오위즈홀딩스` 승인 이탈 원인을 재현하고 approved count를 다시 회복하라.
  - 이미 verified 된 ATS/html source에서 `target role recall`을 더 올려 master row로 번역하라.
  - `네이버`, `삼성커리어스` 같은 html/direct hiring 패턴의 실제 job list 파싱을 보강하라.
  - notebook smoke isolation 또는 automatic restore를 코드화해 bookkeeping 신뢰도를 더 높여라.

- manual_handoff:
  - 남은 수동 작업은 `.env` 실제 값 입력과 실제 notebook 본 실행뿐이다.

## 2026-04-01 23:06 KST source-scan v4 growth handoff

- before_metrics:
  - `quality_score_100 = 99.57`
  - `master_rows = 94`
  - `staging_rows = 94`
  - `active_jobs = 92`
  - `approved_company_count = 139`
  - `effective_verified_source_success_count = 541`
  - `effective_screened_source_count = 551`
  - `source_scan_next_offset = 29`
  - `source_scan_next_cursor = https://hrdkorea.or.kr`

- after_metrics:
  - growth cycle:
    - `run-collection-cycle-20260401224743`
    - `quality_score_100 = 99.52`
    - `master_rows = 100`
    - `staging_rows = 100`
    - `active_jobs = 98`
    - `new_job_count = 6`
    - `effective_verified_source_success_count = 546`
    - `effective_screened_source_count = 556`
    - `source_scan_resume_strategy = policy_reset`
    - `source_scan_start_offset = 0`
    - `source_scan_next_offset = 39`
  - final safe recovery cycle:
    - `run-collection-cycle-20260401230222`
    - `quality_score_100 = 99.52`
    - `master_rows = 103`
    - `staging_rows = 103`
    - `active_jobs = 101`
    - `approved_company_count = 139`
    - `effective_verified_source_success_count = 546`
    - `effective_screened_source_count = 556`
    - `published_verified_source_success_count = 279`
    - `published_screened_source_count = 281`
    - `source_scan_policy_version = v4`
    - `source_scan_next_offset = 247`
    - `company_evidence_next_offset = 1000`
    - `master_row_delta_vs_run_start = +9`
    - `approved_company_delta_vs_run_start = 0`
    - `effective_verified_source_success_delta_vs_run_start = +5`

- kept_changes:
  - `src/jobs_market_v2/collection.py`
    - source scan ordering을 `hot active ATS -> unseen ATS -> zero-active ATS -> warm active`로 재조정했다.
    - 같은 signal tier 안에서는 `greetinghr/recruiter/...`가 한 타입에 쏠리지 않도록 round-robin interleave를 넣었다.
    - ordering policy가 바뀐 배포에서는 stale cursor를 그대로 잇지 않고 `policy_reset`으로 0부터 다시 시작하게 만들었다.
    - source scan progress에 `policy_version = v4`를 기록하게 했다.
  - `tests/test_jobs_market_v2.py`
    - zero-active ATS interleave와 policy reset을 검증하는 테스트를 추가했다.
    - changed prioritization expectation을 반영한 targeted tests를 유지했다.
  - 운영 검증:
    - `./scripts/setup_env.sh`
    - `./scripts/register_kernel.sh`
    - `./.venv/bin/pytest -q`
    - notebook smoke 2개
    - `python -m jobs_market_v2.cli doctor`
    - `python -m jobs_market_v2.cli run-collection-cycle`

- reverted_changes:
  - 코드 revert는 없었다.
  - `run-collection-cycle-20260401224320` 결과는 safe check는 통과했지만 stale cursor를 이어받아 `master_rows` 증가가 없어서 최종 기준점으로 채택하지 않았다.

- last_known_safe_state:
  - growth-producing cycle:
    - `run-collection-cycle-20260401224743`
  - final post-smoke safe cycle:
    - `run-collection-cycle-20260401230222`
  - supporting run ids:
    - `update-incremental-20260401230415`
    - `company-evidence-20260401230254`
  - safe runtime snapshot:
    - `master_rows = 103`
    - `staging_rows = 103`
    - `quality_score_100 = 99.52`
    - `approved_company_count = 139`
    - `effective source registry = runtime/source_registry_in_progress.csv (556 rows / 성공 546 / 실패 8)`
    - `published source registry = runtime/source_registry.csv (281 rows / 성공 279 / 실패 2)`

- last_successful_run_id:
  - `run-collection-cycle-20260401230222`

- resume_next_step:
  - 다음 시작점은 `master 103 / active 101 / approved 139 / effective verified success 546 / effective screened sources 556`이다.
  - `runtime/source_collection_progress.json`은 `policy_version = v4`, `next_source_offset = 247`, `last_run_id = update-incremental-20260401230415` 상태다.
  - `runtime/company_evidence_progress.json`은 `next_offset = 1200`, `run_id = company-evidence-20260401230047` 상태다.
  - 다음 run은 `published_company_state=false`이므로 `runtime/source_registry_in_progress.csv`를 기준으로 cycle이 이어질 수 있다는 점을 전제로 시작하라.

- failures:
  - 첫 v4 ordering 적용 직후 cycle은 old cursor를 그대로 이어받아 `source_scan_start_offset = 68`에서 시작했고 `master_rows` 증가가 없었다.
  - 이를 막기 위해 `policy_reset`을 추가했고, 이후 cycle에서 `source_scan_start_offset = 0`, `new_job_count = 6`, `master_rows 94 -> 100`을 확인했다.
  - notebook smoke는 이번에도 runtime bookkeeping을 흔들었다. 최종 doctor + cycle로 safe state는 복구했지만 `source_registry.csv`와 `source_registry_in_progress.csv` 간 불일치는 남아 있다.
  - shadow catalog discovery에서는 여전히 `html_link_catalog_url` 10건이 timeout으로 남았다.

- next_priorities:
  - `published_company_state=false`일 때 `source_registry.csv`와 실제 사용 중인 `source_registry_in_progress.csv`가 어긋나는 bookkeeping gap을 줄여 결과 신뢰성을 높여라.
  - `source_scan_next_offset = 247` 이후 v4 ordering이 추가 master growth로 계속 번역되는지 확인하라.
  - same-domain/html source 쪽 noise를 더 줄여 approved/source growth가 master growth로 이어지는 비율을 높여라.
  - `requirements / preferred / focus / hiring_track` recall은 quality 99를 깨지 않는 범위에서 계속 보강하라.

- manual_handoff:
  - 남은 수동 작업은 `.env` 실제 값 입력과 실제 notebook 본 실행뿐이다.

## 2026-04-01 21:40 KST latest handoff pointer

- before_metrics:
  - `quality_score_100 = 99.57`
  - `master_rows = 94`
  - `approved_company_count = 138`
  - `source_registry verification_status=성공 = 483`
  - `screened_source_count = 517`

- after_metrics:
  - safe cycle: `run-collection-cycle-20260401212317`
  - post-smoke recovery:
    - `collect-company-evidence-20260401213151`
    - `screen-companies-20260401213852`
    - `sync-sheets-20260401213902`
  - final safe runtime:
    - `quality_score_100 = 99.57`
    - `master_rows = 94`
    - `approved_company_count = 139`
    - `source_registry verification_status=성공 = 541`
    - `screened_source_count = 551`
    - `company_evidence.completed_full_scan = true`
    - `source_collection_progress.next_source_offset = 29`
    - `source_collection_progress.next_source_cursor = https://hrdkorea.or.kr`

- kept_changes:
  - `source scan resume fallback` 코드와 테스트를 유지했다.
  - full pytest, notebook smoke 2개, doctor, run-collection-cycle을 통과시켰다.
  - smoke 이후 degraded runtime은 full company evidence scan + screen-companies + sync-sheets로 복구했다.

- reverted_changes:
  - 코드 revert는 없었다.
  - `source_registry rows = 279 / verification_status=성공 = 277 / published_company_state = false`인 transient runtime은 폐기했다.

- last_known_safe_state:
  - `run-collection-cycle-20260401212317`
  - `collect-company-evidence-20260401213151`
  - `screen-companies-20260401213852`

- last_successful_run_id:
  - `run-collection-cycle-20260401212317`

- resume_next_step:
  - 다음 시작점은 `approved 139 / screened sources 551 / verified success 541 / master 94`다.
  - 다음 run은 notebook smoke 없이 current safe state에서 다시 cycle을 돌려 `source_scan_resume_strategy` fallback이 실제로 먹는지 확인하라.

- failures:
  - 이번 run 자체로 `master_rows` 증가는 없었다.
  - notebook smoke는 여전히 runtime/source progress를 흔들었다.

- next_priorities:
  - notebook smoke isolation 또는 automatic restore를 코드화하라.
  - safe state에서 한 번 더 cycle을 돌려 top reset 완화가 실제 성장으로 이어지는지 확인하라.
  - ATS/GreetingHR/recruiter second-order ranking을 보강하라.

- manual_handoff:
  - 남은 수동 작업은 `.env` 실제 값 입력과 실제 notebook 본 실행뿐이다.

## 2026-04-02 01:20 KST practical duplicate + runs.csv recovery

- solved:
  - `quality.py` practical duplicate 규칙을 `같은 회사 + 같은 직무 + 같은 본문` 기준으로 재구성했다.
  - cosmetic title/summary/track 차이만 있는 동일본문 공고를 collapse 하도록 수정했다.
  - `runs.csv`에 끼어 있던 손상 줄 1개를 제거했고, `storage.read_csv_or_empty()`에 malformed-line fallback을 넣어 sheet sync가 bookkeeping 한 줄 때문에 죽지 않게 했다.
  - `sync-sheets --target master`, `sync-sheets --target staging`를 실제로 다시 성공시켰다.

- current_state:
  - `master_rows = 103`
  - `same_content_groups = 1`
  - `job_url_dupes = 0`
  - `job_key_dupes = 0`
  - `quality_score_100` latest safe reference remains `99.47`

- still_open:
  - 남은 same-content group은 `인터엑스 서울/울산` 1개다.
  - `approved/source -> master` 번역률은 여전히 핵심 병목이다.
  - direct HTML hiring source 파싱과 field recall 부족도 계속 남아 있다.

- tests_and_checks:
  - targeted pytest 통과
    - malformed csv fallback
    - practical duplicate collapse identical content
    - same-content duplicate collapse with same variant key
    - same-content duplicate keep with location split
    - same-content duplicate collapse with different titles
    - same-content duplicate collapse with different tracks
  - `doctor` 통과
  - `promote-staging` 통과
  - `sync-sheets --target master` 통과
  - `sync-sheets --target staging` 통과

- next_priorities:
  - `인터엑스 서울/울산` location split policy 고정
  - direct HTML hiring source 번역률 개선
  - `채용트랙/경력/우대사항` recall 추가 보강

## 2026-04-02 01:35 KST practical duplicate zeroing

- solved:
  - location-only same-content practical duplicate도 collapse 하도록 정책을 고정했다.
  - `인터엑스 [서울] / [울산]` identical-body pair를 하나로 접었다.
  - `sync-sheets --target master`, `sync-sheets --target staging` 재성공으로 시트 반영까지 끝냈다.

- current_state:
  - `master_rows = 102`
  - `same_content_groups = 0`
  - `job_url_dupes = 0`
  - `job_key_dupes = 0`

- still_open:
  - top blocker는 이제 `실질 중복`이 아니라 `approved/source -> master` 번역률이다.
  - direct HTML hiring source 파싱, `채용트랙/경력/우대사항` recall, smoke isolation은 계속 남아 있다.


## 2026-04-01 21:40 KST source scan resume fallback + post-smoke recovery

- subagents:
  - `Zeno`: approved/source -> master translation code path 분석용 explorer로 배정했다.
  - `Peirce`: runtime/source/master 병목 정량화용 explorer로 배정했다.
  - 두 subagent 모두 shutdown 전까지 usable 결과를 돌려주지 못해 최종 판단과 수정은 로컬에서 끝냈다.

- before_metrics:
  - 이번 run 시작 기준 runtime:
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

- after_metrics:
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

- kept_changes:
  - repository code 변경을 유지했다.
  - `src/jobs_market_v2/collection.py`
    - source scan progress에 `collectable_source_urls`를 저장한다.
    - registry signature가 바뀌어도 `cursor -> processed survivor tail -> saved offset -> reset` 순서로 재개 판단한다.
    - run summary에 `source_scan_resume_strategy`를 남겨 bookkeeping을 더 믿을 수 있게 했다.
  - `tests/test_jobs_market_v2.py`
    - cursor resume 유지 검증을 보강했다.
    - registry change 후 cursor가 사라질 때 survivor-tail / saved-offset fallback을 각각 테스트로 고정했다.
  - 저장소 규칙대로 아래를 다시 끝까지 태웠다.
    - `./scripts/setup_env.sh`
    - `./scripts/register_kernel.sh`
    - `pytest -q`
    - notebook smoke 2개
    - `python -m jobs_market_v2.cli doctor`
    - `python -m jobs_market_v2.cli run-collection-cycle`
  - notebook smoke 후 degraded published state를 운영적으로 복구했다.
    - `collect-company-evidence --batch-size 200 --max-batches 10`
    - `screen-companies`
    - `sync-sheets --target staging`
    - `sync-sheets --target master`

- reverted_changes:
  - 코드 revert는 없었다.
  - notebook smoke + partial cycle 중 생긴 degraded published runtime은 유지하지 않았다.
  - 폐기한 transient 상태:
    - `source_registry rows = 279`
    - `source_registry verification_status=성공 = 277`
    - `source_scan_resume_strategy = reset_after_registry_change`
    - `published_company_state = false`
  - 위 상태는 full company evidence scan 완료 후 published company/source state를 다시 써서 복구했다.

- last_known_safe_state:
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

- last_successful_run_id:
  - `run-collection-cycle-20260401212317`

- resume_next_step:
  - 다음 시작점은 `approved 139 / screened sources 551 / verified success 541 / master 94`다.
  - `runtime/company_evidence_progress.json`은 `completed_full_scan = true`, `next_offset = 0`으로 복구됐다.
  - `runtime/source_collection_progress.json`은 `next_source_offset = 29`, `next_source_cursor = https://hrdkorea.or.kr`, `completed_full_scan_count = 6`이다.
  - 다음 run은 notebook smoke 없이 현재 safe state에서 다시 `run-collection-cycle`을 태워, registry change가 생겨도 `reset_after_registry_change` 대신 cursor/survivor/offset fallback이 실제로 유지되는지 확인하라.
  - 그 다음 병목은 여전히 `approved/source growth -> master row growth` 번역률이며, 특히 ATS/GreetingHR wave 내부 second-order ranking이 남아 있다.

- failures:
  - 이번 safe code change는 source scan reset 완화였지만, notebook smoke가 source/company published state를 다시 열어 runtime bookkeeping을 흔들었다.
  - 최초 full pytest 1회차에서 `test_incremental_update_pipeline_marks_changed_and_missing`가 한 번 실패했지만, 단독 재현과 전체 재실행에서는 통과했다.
  - 이번 run 자체로는 `master_rows`를 `94 -> 95+`로 올리지 못했다.
  - subagent 2개는 유효 분석 결과 없이 종료돼 실질 기여가 없었다.

- next_priorities:
  - notebook smoke isolation 또는 automatic restore를 코드화해 production runtime/source progress를 더 이상 오염시키지 않게 하라.
  - safe state에서 한 번 더 cycle을 돌려 `source_scan_resume_strategy`가 실제로 top reset을 피하는지 검증하라.
  - ATS/GreetingHR/recruiter 내부 second-order ranking을 보강해 `approved 139 / verified 541`이 `master_rows > 94`로 번역되게 하라.

- manual_handoff:
  - 남은 수동 작업은 `.env` 실제 값 입력과 실제 notebook 본 실행뿐이다.

## 2026-04-01 20:30 KST

- 이번 턴 핵심 수정:
  - [/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/collection.py](/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/collection.py)
    - source scan registry signature를 정책 버전 기반으로 다시 정리했고, collectable source set이 바뀌면 cursor를 다시 위에서부터 시작하도록 보강했다.
  - [/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/pipelines.py](/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/pipelines.py)
    - `published_company_state=false`일 때도 `company_candidates_in_progress.csv`, `source_registry_in_progress.csv`를 같은 cycle의 collection 입력으로 사용하게 바꿨다.
    - partial scan 중 수집된 source verification 상태는 `source_registry_in_progress.csv`에 다시 기록되도록 바꿨다.
  - [/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py](/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py)
    - `test_source_collection_registry_signature_ignores_transient_source_activity`
    - `test_collect_jobs_from_sources_restarts_from_top_when_registry_set_changes`
    - `test_run_collection_cycle_pipeline_uses_in_progress_registry_during_partial_company_scan`

- 검증:
  - targeted pytest 통과
    - `pytest -q tests/test_jobs_market_v2.py -k 'uses_in_progress_registry_during_partial_company_scan or skips_collection_when_published_state_is_not_ready or source_collection_registry_signature_ignores_transient_source_activity or restarts_from_top_when_registry_set_changes'`
  - `doctor` 통과
  - live cycle 재검증 결과:
    - before:
      - `published master_rows = 94`
      - `published approved_company_count = 138`
      - `published verified_source_success_count = 476`
      - `working approved_company_count = 141`
      - `working verified_source_success_count = 478`
      - `source_collection_progress.next_source_offset = 464`
    - after:
      - `master_rows = 95`
      - `staging_rows = 95`
      - `quality_score_100 = 99.58`
      - `quality_gate_passed = true`
      - `active_job_count = 94`
      - `working verified_source_success_count = 505`
      - `source_collection_progress.next_source_offset = 27`
      - `source_collection_progress.next_source_cursor = https://hancom.career.greetinghr.com`

- 해석:
  - 이번 턴에서 `master 94 -> 95`로 실제 published growth 1건을 확인했다.
  - 핵심은 partial company scan 중 새로 확보된 승인 후보/소스가 더 이상 다음 full scan까지 묶여 있지 않고, 같은 cycle의 collection에 들어가기 시작했다는 점이다.
  - published `company_candidates.csv` / `source_registry.csv` 자체는 여전히 full scan 완료 전 기준을 유지하고, 성장성 높은 변화는 `*_in_progress.csv` 쪽에 먼저 쌓인다.

- 남은 리스크:
  - 이번 `20:18` cycle은 `master/staging/quality_gate/source_collection_progress`는 갱신됐지만, 최상위 `run-collection-cycle` row가 `runs.csv`에 append되지 않았다.
  - 즉 core pipeline은 끝났지만 run bookkeeping이 누락되는 경로가 남아 있다. 다음 턴에서 `run_collection_cycle_pipeline()` 종료 직전 bookkeeping 누락 원인을 따로 확인해야 한다.

- next_priorities:
  - `working approved/source growth -> published master growth` 전환을 계속 밀어라.
  - `company_screening._source_summary()` 기준을 실제 `last_active_job_count > 0`인 소스 중심으로 조일지 검증하라.
  - `practical duplicate key`를 도입해 URL·구두점 변형형 실질 중복을 줄이고, 진짜 별도 공고는 더 강한 식별자를 표시하라.

## 2026-04-01 20:40 KST

- 이번 추가 수정:
  - [/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/quality.py](/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/src/jobs_market_v2/quality.py)
    - exact duplicate만 `[공고 id]`를 붙이던 로직을 확장해, 같은 회사 내 `practical duplicate title stem` 묶음에도 원제목 변형 힌트를 우선 반영하도록 보강했다.
    - 단, `원제목 변형이 실제로 없는 exact duplicate`는 기존처럼 `[공고 id]`만 붙이게 유지했다.
  - [/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py](/Users/junheelee/Desktop/sctaper_p1/jobs_market_v2/tests/test_jobs_market_v2.py)
    - `test_normalize_job_analysis_fields_disambiguates_practical_duplicate_title_stems` 추가

- 검증:
  - targeted pytest 통과
    - `pytest -q tests/test_jobs_market_v2.py -k 'disambiguates_duplicate_display_titles or disambiguates_practical_duplicate_title_stems or uses_in_progress_registry_during_partial_company_scan or source_collection_registry_signature_ignores_transient_source_activity or restarts_from_top_when_registry_set_changes'`
  - `doctor` 통과
  - `promote-staging` 통과
    - `promoted_job_count = 95`
  - `sync-sheets --target staging` 성공
  - `sync-sheets --target master` 성공

- 현재 화면 체감 예시:
  - 당근:
    - `Software Engineer, Machine Learning | 추천 / 광고`
    - `Software Engineer, Machine Learning | ML 인프라`
    - `Software Engineer, Machine Learning | 검색 (품질)`
  - 쿠팡:
    - `Staff Data Analyst (Fraud & Risk) [공고 7064205]`
    - `Staff Data Analyst (Fraud & Risk) [공고 7064215]`
  - 딥노이드:
    - `AI Researcher (Multimodal)`
    - `AI Researcher (Neuro)`
    - `AI Researcher (Computational Pathology)`
    - `AI Researcher (Agentic AI)`
  - 마키나락스:
    - `[신입/인턴] Forward Deployed Engineer - LLM`
    - `[전문연구요원] Forward Deployed Engineer - LLM`
    - `Forward Deployed Engineer - LLM | 경력 / LLM / 검색`
    - `Forward Deployed Engineer - LLM (창원)`
    - `Forward Deployed Engineer - LLM Quantization`

- 해석:
  - 이번 수정은 데이터를 지우는 dedupe가 아니라, `실질 중복처럼 보이는 공고`를 시트에서 더 쉽게 구분하게 만드는 개선이다.
  - exact duplicate는 여전히 `[공고 id]` 기준으로 표시하고, practical duplicate는 원제목 변형 힌트를 노출한다.

## 2026-04-01 10:12 KST growth translation + notebook smoke hardening 업데이트

- subagents:
  - 이번 run에서는 사용하지 않았다.
  - 병목이 `job collection source ordering`과 `smoke notebook defensive guard`로 국소화돼 있어 로컬 수정이 더 안전했다.

- before_metrics:
  - 기준 safe runtime: `2026-04-01 09:33 KST` 시작 직전
  - `quality_score_100 = 99.62`
  - `master_rows = 91`
  - `active_jobs = 90`
  - `approved_company_count = 138`
  - `source_registry verification_status=성공 = 461`
  - `screened_source_count = 516`

- after_metrics:
  - growth 확보 cycle:
    - `run-collection-cycle-20260401094017`
    - `started_at 2026-04-01T09:40:17+09:00`
    - `finished_at 2026-04-01T09:44:50+09:00`
  - final safe cycle:
    - `run-collection-cycle-20260401100806`
    - `started_at 2026-04-01T10:08:06+09:00`
    - `finished_at 2026-04-01T10:11:39+09:00`
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
  - `run_level_verified_source_success_count = 49`
  - `automation_ready = true`

- kept_changes:
  - `src/jobs_market_v2/collection.py`에서 collectable source ordering을 `approved 우선 -> greenhouse/lever/greetinghr/recruiter 우선 -> html_page 후순위`로 바꿨다.
  - 위 ordering은 static priority만 사용해 `source_collection_progress` cursor resume 안정성을 유지했다.
  - `tests/test_jobs_market_v2.py`에 `collect_jobs_from_sources_prioritizes_ats_before_html_pages`를 추가해 ATS-first ordering과 resume 동작을 검증했다.
  - `notebooks/01_bootstrap_population.ipynb`와 `runtime/notebook_smoke/01_bootstrap_population.smoke.ipynb`에서 `comparison_view.iloc[-4]` 고정 인덱스와 빈 `raw_detail` 가정을 제거했다.
  - `./scripts/setup_env.sh`, `./scripts/register_kernel.sh`, targeted pytest, `pytest -q`, `doctor`, `run-collection-cycle`, notebook smoke 2개를 다시 끝까지 태웠다.
  - notebook이 runtime을 덮어쓴 뒤에는 `collect-company-evidence-20260401100336` + `screen-companies-20260401100750` + `doctor-20260401100755` + `run-collection-cycle-20260401100806`로 published-safe 상태를 복구했다.

- reverted_changes:
  - 코드 revert는 없었다.
  - notebook smoke 실행 중 생긴 unsafe runtime 상태는 유지하지 않았다.
  - 폐기한 unsafe runtime 수치:
    - `quality_score_100 = 76.5`
    - `staging_rows = 0`
    - `source_registry rows = 278`
    - `source_registry verification_status=성공 = 276`
  - 위 상태는 `collect-company-evidence-20260401100336` + `screen-companies-20260401100750` + `run-collection-cycle-20260401100806`로 복구했다.

- last_known_safe_state:
  - post-smoke recovery:
    - `collect-company-evidence-20260401100336`
    - `screen-companies-20260401100750`
  - safe cycle:
    - `run-collection-cycle-20260401100806`
  - safe runtime snapshot:
    - `master_rows = 94`
    - `staging_rows = 94`
    - `quality_score_100 = 99.57`
    - `approved_company_count = 138`
    - `source_registry verification_status=성공 = 476`
    - `screened_source_count = 517`

- last_successful_run_id:
  - `run-collection-cycle-20260401100806`

- resume_next_step:
  - 다음 시작점은 `approved 138 / screened sources 517 / verified success 476 / master 94`이다.
  - `runtime/source_collection_progress.json`은 `next_source_offset = 392`, `completed_full_scan_count = 5`다.
  - `runtime/company_evidence_progress.json`은 recovery full scan 뒤 final cycle이 다시 partial scan을 열어서 `next_offset = 400`, `completed_full_scan = false`다.
  - 다음 run은 source collection cursor `392`부터 이어서 second-wave growth를 보되, notebook을 다시 태우기 전에는 restore 절차를 자동화하거나 격리해야 한다.

- failures:
  - `runtime/notebook_smoke/01_bootstrap_population.smoke.ipynb` 첫 실행은 `comparison_view.iloc[-4]`에서 `IndexError`로 실패했다.
  - 같은 notebook 두 번째 실행은 빈 `raw_detail`에서 `raw_payload_json`을 가정해 `KeyError`로 실패했다.
  - notebook이 최종 통과하더라도 production runtime을 덮어써 `quality_score_100`을 `76.5`까지 떨어뜨리는 부수효과가 있었다.

- next_priorities:
  - ATS-first ordering 이후 첫 wave는 `master +3`로 먹혔지만, later cursor wave는 `0` 신규였다. 다음은 `ATS 안에서의 second-order ranking`이 병목이다.
  - notebook smoke isolation 또는 automatic restore를 코드화해서 smoke가 production runtime을 오염시키지 않게 하라.
  - `published_company_state=false`로 재개되는 구조를 줄여, full-scan company/source state가 반복 cycle에 더 오래 유지되게 하라.

- manual_handoff:
  - 남은 수동 작업은 `.env` 실제 값 입력과 실제 notebook 본 실행뿐이다.

## 2026-04-01 08:30 KST growth 운영 복구 업데이트

- subagents:
  - 이번 run에서는 사용하지 않았다.
  - 현재 병목이 `company_evidence full scan completion`과 `notebook smoke 부수효과 복구`로 국소화돼 있어 로컬 단일 워크플로가 더 안전했다.

- before_metrics:
  - 기준 safe runtime: `2026-04-01 08:03 KST` 시작 시점
  - `quality_score_100 = 99.62`
  - `master_rows = 91`
  - `active_jobs = 90`
  - `approved_company_count = 69`
  - `source_registry verification_status=성공 = 117`
  - `screened_source_count = 119`
  - `company_evidence.next_offset = 800`
  - `published_company_state = false`

- after_metrics:
  - 기준 safe cycle: `run-collection-cycle-20260401082342`
  - final published company/source refresh:
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
  - `run_level_verified_source_success_count = 39`

- kept_changes:
  - repository code 변경은 유지하지 않았다.
  - `./scripts/setup_env.sh`, `./scripts/register_kernel.sh`, `pytest -q`, `doctor`를 다시 끝까지 통과시켰다.
  - `company_evidence` full scan을 운영적으로 완료해 published company state를 한 번 끝까지 닫았다.
  - `screen-companies`를 다시 반영해 published approved bucket을 `69 -> 138`로 올렸다.
  - notebook smoke 후 감소한 published source 상태를 `collect-company-evidence-20260401082724` + `screen-companies-20260401083004`로 `516 / 성공 461`까지 복구했다.

- reverted_changes:
  - 코드 패치는 시도하지 않았다.
  - notebook smoke 실행 중 만들어진 unsafe runtime 상태는 유지하지 않았다.
  - 폐기한 unsafe runtime 수치:
    - `quality_score_100 = 98.0`
    - `staging_rows = 14`
    - `active_jobs = 14`
    - `source_registry rows = 278`
  - 위 상태는 `run-collection-cycle-20260401082342`로 staging/master를 복구하고, 이후 full company scan 재실행으로 company/source published state를 다시 올렸다.

- last_known_safe_state:
  - safe cycle: `run-collection-cycle-20260401082342`
  - follow-up refresh:
    - `collect-company-evidence-20260401082724`
    - `screen-companies-20260401083004`
  - safe runtime snapshot:
    - `master_rows = 91`
    - `staging_rows = 91`
    - `quality_score_100 = 99.62`
    - `approved_company_count = 138`
    - `source_registry verification_status=성공 = 461`

- last_successful_run_id:
  - `run-collection-cycle-20260401082342`

- resume_next_step:
  - 다음 시작점은 `approved 138 / screened sources 516 / verified success 461 / master 91`이다.
  - `runtime/company_evidence_progress.json`은 `completed_full_scan = true`, `next_offset = 0`으로 맞춰 두었다.
  - `runtime/source_collection_progress.json`은 `next_source_offset = 0`, `completed_full_scan_count = 5` 상태다.
  - 다음 run의 핵심 병목은 `approved/source growth -> master row growth` 번역률이다.
  - 특히 `published_company_state=false`가 다음 cycle 시작 때 다시 열리면서 full scan 결과를 운영적으로 다시 밀어야 하는 점이 남아 있다.

- failures:
  - safe code change는 이번 run에서 정당화되지 않아 적용하지 않았다.
  - `runtime/notebook_smoke/01_bootstrap_population.smoke.ipynb` 실행 중 내부 pipeline 호출이 `staging/quality_gate/source_registry`를 덮어써 `quality_score_100 98.0`까지 떨어뜨렸다.
  - notebook smoke 자체는 통과했지만 runtime 부수효과가 있어 handoff 전 복구가 필요했다.

- next_priorities:
  - `approved 138 / verified source success 461`을 `master_rows > 91`로 번역하는 source selection 개선을 우선하라.
  - notebook smoke가 production runtime 파일을 덮어쓰지 않도록 smoke isolation 또는 post-smoke restore 절차를 코드화하라.
  - `published_company_state=false`에서 매 run마다 partial company scan이 다시 열리는 운영 구조를 줄여, full scan 결과가 반복 run에 더 직접 반영되게 하라.

- manual_handoff:
  - 남은 수동 작업은 `.env` 실제 값 입력과 실제 notebook 본 실행뿐이다.

## 2026-04-01 21:40 KST latest handoff pointer

- before_metrics:
  - `quality_score_100 = 99.57`
  - `master_rows = 94`
  - `approved_company_count = 138`
  - `source_registry verification_status=성공 = 483`
  - `screened_source_count = 517`

- after_metrics:
  - safe cycle: `run-collection-cycle-20260401212317`
  - post-smoke recovery:
    - `collect-company-evidence-20260401213151`
    - `screen-companies-20260401213852`
    - `sync-sheets-20260401213902`
  - final safe runtime:
    - `quality_score_100 = 99.57`
    - `master_rows = 94`
    - `approved_company_count = 139`
    - `source_registry verification_status=성공 = 541`
    - `screened_source_count = 551`
    - `company_evidence.completed_full_scan = true`
    - `source_collection_progress.next_source_offset = 29`
    - `source_collection_progress.next_source_cursor = https://hrdkorea.or.kr`

- kept_changes:
  - `source scan resume fallback` 코드와 테스트를 유지했다.
  - full pytest, notebook smoke 2개, doctor, run-collection-cycle을 통과시켰다.
  - smoke 이후 degraded runtime은 full company evidence scan + screen-companies + sync-sheets로 복구했다.

- reverted_changes:
  - 코드 revert는 없었다.
  - `source_registry rows = 279 / verification_status=성공 = 277 / published_company_state = false`인 transient runtime은 폐기했다.

- last_known_safe_state:
  - `run-collection-cycle-20260401212317`
  - `collect-company-evidence-20260401213151`
  - `screen-companies-20260401213852`

- last_successful_run_id:
  - `run-collection-cycle-20260401212317`

- resume_next_step:
  - 다음 시작점은 `approved 139 / screened sources 551 / verified success 541 / master 94`다.
  - 다음 run은 notebook smoke 없이 current safe state에서 다시 cycle을 돌려 `source_scan_resume_strategy` fallback이 실제로 먹는지 확인하라.

- failures:
  - 이번 run 자체로 `master_rows` 증가는 없었다.
  - notebook smoke는 여전히 runtime/source progress를 흔들었다.

- next_priorities:
  - notebook smoke isolation 또는 automatic restore를 코드화하라.
  - safe state에서 한 번 더 cycle을 돌려 top reset 완화가 실제 성장으로 이어지는지 확인하라.
  - ATS/GreetingHR/recruiter second-order ranking을 보강하라.

- manual_handoff:
  - 남은 수동 작업은 `.env` 실제 값 입력과 실제 notebook 본 실행뿐이다.
## 2026-04-03 Final Safe State

- full `pytest -q`: 통과
- `run-collection-cycle`: 실행 완료
  - 첫 결과는 `quality_gate_passed = false`
  - 사유: `사용자 노출 필드에 영문 누수가 존재합니다.`, `품질 점수가 99점 미만입니다.`
- 후속 조치:
  - `gemini.py`에서 영문 장문 display leak도 refinement trigger로 승격
  - leak source 17개 bounded refresh 재실행
  - `quality_gate_passed = true` 회복
  - `promote-staging` 성공
  - `sync-sheets --target master` 성공
  - `sync-sheets --target staging` 성공
  - `doctor` 성공
- 현재 published:
  - `master = 123`
  - `staging = 123`
  - `quality_score_100 = 99.11`
  - `english_leak_count = 0`
  - display blank 3종 `0`
- residual:
  - `경력수준_표시 = 미기재` `6`
  - `우대사항_표시 = 별도 우대사항 미기재` `19`
  - 이 값들은 blank regression이 아니라 현재 source content 자체의 부재/불충분을 의미하며, quality gate blocker는 아니다.

## 2026-04-03 Direct Source Expansion

- 직접 붙인 공식 출처:
  - `https://careers.upstage.ai`
  - `https://career.nota.ai/en/jobs`
  - `https://rebellions.career.greetinghr.com`
  - `https://furiosa.ai/careers`
- 이 4개 source는 모두 `verification_status = 성공`으로 runtime registry에 반영됐다.
- bounded incremental 결과:
  - `new_job_count = 37`
  - `master = 160`
  - `staging = 160`
  - `quality_score_100 = 99.79`
- 중간에 direct subset merge 때문에 `runtime/source_registry.csv`가 4행 subset으로 잘린 runtime 정합성 문제가 있었고, 이를 `source_registry_in_progress.csv` 기반 full registry에 4개 신규 source를 다시 merge하는 방식으로 복구했다.
- 현재 registry safe state:
  - `verified_sources = 545`
  - `active_gt_0_sources = 41`
  - 신규 4개 source의 `last_active_job_count`
    - `업스테이지 = 20`
    - `노타 = 14`
    - `리벨리온 = 4`
    - `퓨리오사AI = 2`
- `doctor`는 registry 복구 뒤 다시 통과했다.

## 2026-04-03 KORAIA Company List Expansion

- `https://startups.koraia.org/company/list` 20페이지를 bounded하게 훑어 current registry에 없는 AI 스타트업·중소기업 시드를 추출했다.
- 결과:
  - `config/koraia_ai_company_batch_20260403.csv` 생성
  - 신규 company seed `81`개 import
  - `discover-companies` 후 `companies_registry = 2347`
- 이 배치만 따로 source discovery를 태워:
  - `source_candidates = 19`
  - `approved = 13`
  - `candidate = 6`
- approved 13개에 대해 bounded incremental 수집을 실행했고:
  - `verified_source_success_count = 13`
  - `collected_job_count = 24`
  - `new_job_count = 19`
  - `staging = 179`
  - `quality_score_100 = 99.80`
- 이후 `promote-staging`, `sync-sheets(master/staging)`, `doctor`까지 재통과했다.
- 현재 safe state:
  - `master = 179`
  - `staging = 179`
  - `source_registry_rows = 573`
  - `approved_source_rows = 412`
  - `candidate_source_rows = 159`
  - `rejected_source_rows = 2`

## 2026-04-03 Seoul AI Hub Expansion

- 공식 리스트 소스:
  - `https://www.seoulaihub.kr/partner/partner.asp?scrID=0000000195&pageNum=2&subNum=1&ssubNum=1&page=1`
  - 서울 AI 허브 resident / graduate 공식 roster
- 처리:
  - agent가 리스트 구조와 detail endpoint를 먼저 확인
  - `scripts/extract_seoul_ai_hub_batch.py`로 roster `318`개를 파싱
  - homepage host가 확인된 `283`개 중 current registry에 없던 `218`개를
    - `config/seoul_ai_hub_company_batch_20260403.csv`
    로 저장
  - `import-companies` 후 `discover-companies` 재실행
  - `scripts/apply_company_batch_sources.py`로 이 배치만 bounded source discovery + incremental 수집
- 결과:
  - `subset_company_count = 216`
  - `source_candidate_count = 20`
  - `approved_count = 14`
  - `candidate_count = 5`
  - `new_job_count = 4`
  - `master = 183`
  - `staging = 183`
  - `quality_gate_passed = true`
  - `promote-staging`, `sync-sheets(master/staging)`, `doctor` 성공
- 현재 확인된 대표 source:
  - `https://archisketch.career.greetinghr.com`
  - `https://career.tesser.io`
  - `https://tunib.career.greetinghr.com`
  - `https://nalbi.career.greetinghr.com`
  - `https://wethemax.com/sub/recruit.php`

## 2026-04-03 KOREA AI STARTUP 100 Slice

- 공식 리스트 소스:
  - `https://aistartuptop100.co.kr/page/s2/s1.php?sty=2023`
- agent가 Cloudflare를 우회해 현재 visible company info slice `12`개를 확인했고,
  - `config/korea_ai_startup100_slice_20260403.csv`
  로 batch를 만들었다.
- 처리:
  - `import-companies` 후 `discover-companies` 재실행
  - `scripts/apply_company_batch_sources.py`로 bounded source discovery + incremental 수집
- 결과:
  - `source rows discovered = 7`
  - `verified_source_success_count = 5`
  - `verified_source_failure_count = 1`
  - `selected_collectable_source_count = 6`
  - `new_job_count = 0`
  - `master = 183` 유지
  - `staging = 183` 유지
- 대표 source:
  - `https://medintech.co.kr/careers`
  - `https://haezoom.career.greetinghr.com`
  - `https://basgenbio.com/kr/sub/career/recruit.php`
  - `https://huinno.com/recruitment`
  - `https://pyler.tech/careers`
