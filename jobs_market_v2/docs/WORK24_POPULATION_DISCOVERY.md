# Work24 Population Discovery Runbook

작성일: 2026-04-20

## 목적

Work24 공개 검색을 API 없이 사용하되, 단순 샘플 수집이 아니라 Biz Voyager가 상정한 4개 직군의 공개 검색 결과 전체 페이지를 확인해 모집단 후보를 만든다.

대상 직군:

- 데이터 분석가
- 데이터 사이언티스트
- 인공지능 리서처
- 인공지능 엔지니어

## 전체 페이지의 정의

여기서 "전체 페이지"는 Work24 사이트 전체가 아니라 `config/manual_sources_seed.yaml`에 정의된 4개 직군 검색 feed 각각의 결과 페이지 전체를 뜻한다.

각 feed는 다음 조건 중 하나가 발생할 때까지 page 1부터 순차 수집한다.

- `empty_page_stop`: 검색 결과가 없는 빈 페이지가 확인됨
- `stale_page_stop`: 페이지는 있으나 새로운 `wantedAuthNo`가 연속으로 나오지 않음
- `max_page_safety_cap`: 안전 상한에 도달함

정상적인 전체 순회는 `empty_page_stop` 또는 `stale_page_stop`으로 끝나야 한다. `max_page_safety_cap`이 나오면 전체 순회가 완료됐다고 보지 않고 검색 조건 또는 cap을 재검토한다.

## 저장 위치

영구 로직:

- 검색 seed: `config/manual_sources_seed.yaml`
- 경로와 환경 설정: `src/jobs_market_v2/settings.py`
- page 순회, 중복 추적, 종료 조건, 감사 로그: `src/jobs_market_v2/discovery.py`
- source registry snapshot 병합 정책: `src/jobs_market_v2/pipelines.py`
- Work24 목록 title 보정: `src/jobs_market_v2/collection.py`
- Work24 runtime 직무 재검수/비타깃 제거: `src/jobs_market_v2/quality.py`
- GitHub Actions stale snapshot 방어용 런타임 가드: `src/sitecustomize.py`

런타임 산출물:

- 원시 공고 모집단: `runtime/work24_population_jobs.csv`
- target 회사 후보: `runtime/work24_population_candidates.csv`
- 원천 기업 shadow pool: `runtime/work24_population_shadow_companies.csv`
- 페이지별 감사 로그: `runtime/work24_population_scan_log.csv`
- 후보 source: `output_samples/candidate_sources.csv`
- 운영 source registry: `runtime/source_registry.csv`

검증 고정 장치:

- `tests/test_jobs_market_v2.py`
- `README.md`
- `config/runtime_state_manifest.txt`

## 운영 명령

```bash
./.venv/bin/python -m jobs_market_v2.cli discover-work24-population
./.venv/bin/python -m jobs_market_v2.cli discover-companies
./.venv/bin/python -m jobs_market_v2.cli discover-sources
```

위 3개 명령은 Work24 공개 검색 결과를 모집단 후보로 만들고, 기존 회사 후보 및 source 후보 체계에 연결한다.

## 2026-04-20 최신 실행 결과

최신 `runtime/work24_population_scan_log.csv` 기준:

| Feed | 확인한 페이지 | 종료 사유 |
| --- | ---: | --- |
| 고용24 공개 채용검색 - 데이터 분석가 | 1, 2, 3 | empty_page_stop |
| 고용24 공개 채용검색 - 데이터 사이언티스트 | 1, 2 | empty_page_stop |
| 고용24 공개 채용검색 - 인공지능 리서처 | 1, 2 | empty_page_stop |
| 고용24 공개 채용검색 - 인공지능 엔지니어 | 1, 2 | empty_page_stop |

최신 산출물:

- `runtime/work24_population_jobs.csv`: 93 rows
- `runtime/work24_population_candidates.csv`: 16 rows
- `runtime/work24_population_shadow_companies.csv`: Work24 원천에서 발견한 전체 기업 pool. `promoted_candidate=false` 행은 아직 확정 후보가 아니라 상세 검증 대기 상태다.
- `output_samples/candidate_sources.csv`: 204 rows, Work24 fallback 16 rows
- `runtime/source_registry.csv`: 653 rows, Work24 fallback 16 rows

주의: `wc -l`은 CSV 본문 내 줄바꿈 때문에 실제 row 수보다 크게 보일 수 있다. row 수는 pandas/read_csv 기준으로 판단한다.

## 2026-04-20 Fallback Probe 결과

Work24 fallback source 16개를 master/staging 승격 없이 별도 probe로 수집했다.

산출물:

- probe 보고서: `runtime/work24_probe/work24_fallback_probe_report.json`
- 원시 수집 공고: `runtime/work24_probe/work24_fallback_probe_jobs_raw.csv`
- 필터 통과 공고: `runtime/work24_probe/work24_fallback_probe_jobs_filtered.csv`
- 드롭 공고: `runtime/work24_probe/work24_fallback_probe_jobs_dropped.csv`

결과:

| 항목 | 값 |
| --- | ---: |
| Work24 fallback source | 16 |
| source 수집 성공 | 16 |
| source 수집 실패 | 0 |
| raw jobs | 7 |
| filtered jobs | 3 |
| dropped jobs | 4 |
| quality gate | failed |

남은 실패 사유:

- 허용 직무 4개 중 2개 이상이 0건

해결된 사유:

- Work24 `관계없음` 경력값은 `경력무관`으로 표시한다.
- Work24 후보의 `company_tier`는 기업명/지역 신호로 `중견/중소`, `지역기업`, `공공·연구기관` 중 하나로 채운다.

해석:

- 이 probe는 Work24 fallback 16개만 따로 본 isolated smoke라 4개 직군 전체 coverage를 만족하지 못한다.
- 따라서 이 결과만으로 master/staging에 승격하지 않는다.
- 실제 자동화 승격 판단은 기존 master/staging과 병합된 전체 staging에서 수행한다.

## 다음 단계

다음 단계는 Work24 fallback source 16개를 바로 master에 올리는 것이 아니라, 제한 수집으로 상세 수집 가능성과 품질을 확인하는 것이다.

실행 순서:

1. `runtime/source_registry.csv`에서 `discovery_method == work24_limited_public_board_fallback`인 source만 분리한다.
2. 해당 subset에 대해 수집 smoke를 실행한다.
3. 수집된 jobs를 low-quality filter와 quality gate에 통과시킨다.
4. 통과한 경우에만 `update-incremental` 또는 staging promotion 후보로 다룬다.
5. 실패하면 `runtime/work24_population_jobs.csv`와 `runtime/work24_population_scan_log.csv`를 기준으로 검색어, LLM target 판단, Work24 상세 title parsing 중 어느 지점이 문제인지 먼저 수정한다.

승격 금지 조건:

- `max_page_safety_cap`이 하나라도 나온 경우
- Work24 fallback source의 수집 실패율이 높은 경우
- target 후보 중 비타깃 일반 개발, 영업, 사무, 디자인 공고가 quality gate를 통과하려는 경우
- 기존 master/staging count가 shrink guard에 걸리는 경우
