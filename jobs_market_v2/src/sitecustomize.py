"""Production runtime guards for GitHub Actions.

This module is loaded automatically by Python when the editable src path is
installed. It keeps the production workflow protected even when a stale source
snapshot is restored by Actions.
"""

from __future__ import annotations

import html
import json
import re
import sys
from urllib.parse import urljoin


def _install() -> None:
    import pandas as pd

    from jobs_market_v2 import collection, gemini, presentation, quality
    from jobs_market_v2.constants import ALLOWED_JOB_ROLES, JOB_COLUMNS
    from jobs_market_v2.models import GateResult
    from jobs_market_v2.presentation import build_analysis_fields, build_display_fields

    normalize = collection.normalize_whitespace
    normalize_role = collection._normalize_role_text
    has_phrase = collection._has_phrase
    has_any = collection._has_any_phrase
    old_classify = collection.classify_job_role
    old_merge_incremental = collection.merge_incremental
    old_evaluate_quality_gate = quality.evaluate_quality_gate

    def call_llm_for_html_jobs(payload: dict, settings) -> list[dict[str, str]]:
        prompt = f"""
다음 HTML 채용/모집 페이지 후보 정보에서 실제 채용 공고 링크만 골라 JSON으로 반환하라.

제약:
- 절대 새 링크를 만들지 마라
- 입력 anchors에 있는 url만 사용하라
- title은 anchors text를 바탕으로 정리하되 새 정보를 만들지 마라
- 채용 공고가 아니면 제외하라
- 결과가 없으면 빈 배열을 반환하라
- JSON만 반환하라

반환 형식:
{{"jobs":[{{"title":"","job_url":"","location":"","experience_level":""}}]}}

입력:
{json.dumps(payload, ensure_ascii=False)}
""".strip()
        response = gemini._call_json_llm(prompt=prompt, settings=settings, temperature=0.1, max_output_tokens=1024)
        if isinstance(response, dict):
            jobs = response.get("jobs") or response.get("items") or []
        elif isinstance(response, list):
            jobs = response
        else:
            jobs = []
        return jobs if isinstance(jobs, list) else []

    def extract_html_jobs_with_llm(content: str, base_url: str, *, settings=None, paths=None, budget=None) -> list[dict]:
        if not settings or not paths or not budget:
            return []
        if (
            not settings.enable_gemini_fallback
            or not gemini._active_llm_api_key(settings)
            or not gemini._active_llm_model(settings)
            or not budget.can_call()
        ):
            return []
        if not (
            collection._html_page_looks_like_hiring_page(content)
            or collection._html_source_has_hiring_path_signal({"source_url": base_url})
        ):
            return []

        payload, allowed_urls = collection._prepare_html_gemini_probe_payload(content, base_url)
        if not payload["anchors"]:
            return []

        cache_path = paths.logs_dir / "gemini_html_jobs_cache.json"
        cache: dict[str, list[dict[str, str]]] = {}
        if cache_path.exists():
            try:
                cache = json.loads(cache_path.read_text(encoding="utf-8"))
            except Exception:  # noqa: BLE001
                cache = {}
        cache_key = collection.stable_hash(
            [
                payload["base_url"],
                payload["page_title"],
                payload["page_excerpt"],
                json.dumps(payload["anchors"], ensure_ascii=False),
            ]
        )
        if cache_key not in cache:
            budget.consume()
            try:
                cache[cache_key] = call_llm_for_html_jobs(payload, settings)
                collection.atomic_write_text(cache_path, json.dumps(cache, ensure_ascii=False, indent=2))
            except Exception:  # noqa: BLE001
                return []

        jobs: list[dict] = []
        seen: set[str] = set()
        for item in cache.get(cache_key, []):
            if not isinstance(item, dict):
                continue
            title = normalize(item.get("title"))
            job_url = normalize(item.get("job_url"))
            if not title or not job_url:
                continue
            resolved_url = urljoin(base_url, job_url)
            if resolved_url not in allowed_urls or resolved_url in seen:
                continue
            seen.add(resolved_url)
            jobs.append(
                {
                    "title": title,
                    "description_html": f"<div>{html.escape(title)}</div>",
                    "job_url": resolved_url,
                    "location": normalize(item.get("location")),
                    "experience_level": normalize(item.get("experience_level")),
                }
            )
        return jobs

    def refine_jobs_with_llm(jobs: list[dict], settings, paths) -> list[dict]:
        if not settings.enable_gemini_fallback or not gemini._active_llm_api_key(settings) or not gemini._active_llm_model(settings):
            return jobs

        budget = gemini.GeminiBudget(max_calls=settings.gemini_max_calls_per_run)
        prioritized = sorted(
            ((index, collection._gemini_priority_score(job)) for index, job in enumerate(jobs)),
            key=lambda item: item[1],
            reverse=True,
        )

        for index, score in prioritized:
            if score <= 0 or not budget.can_call():
                break
            job = jobs[index]
            current_analysis = {
                "주요업무_분석용": job.get("주요업무_분석용", ""),
                "자격요건_분석용": job.get("자격요건_분석용", ""),
                "우대사항_분석용": job.get("우대사항_분석용", ""),
                "핵심기술_분석용": job.get("핵심기술_분석용", ""),
                "상세본문_분석용": job.get("상세본문_분석용", ""),
            }
            refined_analysis = gemini.maybe_refine_analysis_fields(
                {
                    "main_tasks": job.get("main_tasks"),
                    "requirements": job.get("requirements"),
                    "preferred": job.get("preferred"),
                    "core_skills": job.get("core_skills"),
                    "description_text": job.get("description_text"),
                },
                current_analysis,
                settings,
                paths,
                budget,
            )
            if refined_analysis != current_analysis:
                job.update(refined_analysis)
                job.update(build_display_fields(refined_analysis))
        return jobs

    ai_signals = (
        "machine learning",
        "ml",
        "mlops",
        "llm",
        "foundation model",
        "document ai",
        "computer vision",
        "vision language",
        "data platform",
        "compiler",
        "ai",
        "artificial intelligence",
        "인공지능",
        "머신러닝",
        "딥러닝",
        "엘엘엠",
        "브이엘엠",
        "엔피유",
        "엘피유",
        "지피유",
        "광학문자인식",
        "컴퓨터비전",
        "컴퓨터 비전",
        "자율주행",
        "adas",
        "에이디에이에스",
        "가속기",
        "런타임",
        "추론",
        "slam",
        "localization",
        "positioning",
    )
    analyst_titles = ("data analyst", "analytics analyst", "monetization analyst", "데이터 분석가", "데이터분석가", "분석가")
    data_engineer_titles = ("data engineer", "data analytics engineer", "analytics engineer", "데이터 엔지니어")
    data_scientist_titles = ("data scientist", "데이터 사이언티스트", "applied scientist", "머신러닝 사이언티스트")
    strict_non_target_titles = (
        "product manager",
        "growth manager",
        "operations manager",
        "risk manager",
        "business developer",
        "business development",
        "sales manager",
        "sales",
        "marketing",
        "marketer",
        "crm marketer",
        "recruiter",
        "recruiting",
        "coordinator",
        "planner",
        "consultant",
        "designer",
        "architect",
        "solution architect",
        "technical program manager",
        "enterprise manager",
        "strategy manager",
        "planning lead",
        "finance manager",
        "brand manager",
        "data manager",
        "program management",
        "pmo",
        "사업개발",
        "security engineer",
        "qa engineer",
        "quality engineer",
        "product validation engineer",
        "ssd product validation",
    )
    title_only_non_target = (
        "business analyst",
        "country manager",
        "project manager",
        "program manager",
        "cos manager",
        "ax manager",
        "applied ai project manager",
        "official website",
        "careers at",
        "talent pool",
        "채용공고",
        "인재채용",
        "인재 채용",
        "채용정보",
        "채용 정보",
        "채용홈페이지",
        "채용 홈페이지",
        "채용페이지",
        "채용 페이지",
        "각 부문 신입/경력 모집",
        "역할과 책임을 다합니다",
        "product researcher",
        "ux researcher",
        "user researcher",
        "market researcher",
        "research operations",
        "research ops",
        "security researcher",
        "offensive security researcher",
        "product owner",
        "기획자",
        "마케터",
        "서비스 기획",
        "프로젝트 개발자",
        "전자정부 프레임워크",
        "node.js 프로젝트 개발자",
        "인재풀 등록",
        "전문연구요원 및 산업기능요원",
        "산업기능요원",
        "전문연구요원 대규모",
        "대규모 채용",
        "멘토풀",
        "강사",
        "교육 전문",
        "process innovation",
        "it ax 전략",
        "ax 전략",
        "ax팀",
        "ax tech leader",
        "기술 리더",
        "sw검증",
        "software engineer launching platform",
        "team leader",
        "pm",
        "펄어비스",
    )
    simple_hard_exclude = (
        "backend",
        "back-end",
        "백엔드",
        "frontend",
        "front-end",
        "프론트엔드",
        "full stack",
        "fullstack",
        "full-stack",
        "풀스택",
        "cross platform",
        "크로스 플랫폼",
        "mobile developer",
        "web developer",
        "app developer",
        "server developer",
        "software engineer system",
        "firmware",
        "펌웨어",
        "fe junior",
        "fe developer",
        "fe 개발자",
    )
    simple_soft_titles = ("software engineer", "software developer", "platform software engineer", "system software engineer", "console")
    service_non_target_titles = (
        "frontend",
        "프론트엔드",
        "backend",
        "백엔드",
        "service developer",
        "service engineer",
        "software engineer",
        "platform engineer",
        "devops engineer",
        "application engineer",
        "integration engineer",
        "design engineer",
        "design verification engineer",
        "verification engineer",
        "dft engineer",
        "soc design engineer",
        "soc design verification",
    )
    service_allow = (
        "software engineer, machine learning",
        "machine learning software engineer",
        "machine learning platform engineer",
        "machine learning engineer",
        "ml engineer",
        "mlops engineer",
        "ai engineer",
        "ai research engineer",
        "deep learning engineer",
        "computer vision",
        "ai system software engineer",
        "data platform",
        "data pipeline",
    )
    researcher_titles = ("research scientist", "ai researcher", "ml researcher", "researcher", "리서처", "연구직", "연구원")
    research_engineer_titles = ("research engineer", "리서치 엔지니어", "연구 엔지니어")
    academic_terms = ("논문", "학회", "출판", "저널", "publication", "conference", "workshop", "journal", "박사", "phd", "benchmark")
    delivery_terms = ("설계", "구현", "개발", "구축", "운영", "배포", "서빙", "pipeline", "파이프라인", "제품", "서비스", "customer", "고객")
    weak_ai_software_titles = ("ai software engineer", "global software engineer", "applied ai technical engineer")
    statistical_analyst_titles = (
        "통계분석",
        "통계 분석",
        "빅데이터 분석",
        "데이터 분석 연구원",
        "리서치 통계분석",
    )
    robot_service_non_target_titles = (
        "robot service",
        "robot cs",
        "robot as",
        "cs as engineer",
        "서비스 cs as",
        "서비스 엔지니어",
        "로봇 서비스",
    )
    robot_service_non_target_body_terms = (
        "유지보수",
        "기술 지원",
        "정기 점검",
        "긴급 장애 대응",
        "장애 대응",
        "부품 교체",
        "입고 테스트",
        "서비스 매뉴얼",
    )
    field_support_non_target_titles = (
        "field application engineer",
        "field engineer",
        "field & system engineer",
        "field and system engineer",
        "field system engineer",
    )
    field_support_non_target_body_terms = (
        "technical support",
        "기술 지원",
        "운영 지원",
        "유지보수",
        "고객사",
        "고객 요청사항",
        "고객 대응",
        "현장",
        "온사이트",
        "on-site",
        "onsite",
        "설치",
        "설정",
        "연동",
        "트러블슈팅",
        "troubleshooting",
        "데모",
        "시연",
        "매뉴얼",
        "manual",
        "사용자 교육",
        "교육 프로그램",
    )
    strong_ai_work_terms = (
        "machine learning",
        "ml",
        "llm",
        "vlm",
        "computer vision",
        "document ai",
        "model training",
        "model serving",
        "data platform",
        "data pipeline",
        "npu",
        "gpu",
        "runtime",
        "inference",
        "머신러닝",
        "딥러닝",
        "엘엘엠",
        "브이엘엠",
        "컴퓨터비전",
        "문서",
        "모델 학습",
        "모델 훈련",
        "모델 서빙",
        "데이터 플랫폼",
        "데이터 파이프라인",
        "엔피유",
        "지피유",
        "런타임",
        "추론",
    )

    def target_signal(text: str) -> bool:
        if has_any(text, ai_signals + data_engineer_titles + data_scientist_titles):
            return True
        normalized = text if text.startswith(" ") else normalize_role(text)
        return any(term in normalized for term in ("엔피유", "엘피유", "지피유", "딥러닝", "컴퓨터비전", "자율주행", "가속기", "런타임"))

    def count_terms(text: str, terms: tuple[str, ...]) -> int:
        return sum(1 for term in terms if has_phrase(text, term) or (re.search(r"[가-힣]", term) and term in text))

    def classify_job_role(*texts: str | None) -> str:
        normalized_texts = [normalize(text) for text in texts]
        title_candidates = [text for text in normalized_texts[:2] if text]
        if not title_candidates:
            return ""
        primary_title = normalize_role(title_candidates[0])
        title_corpus = normalize_role(" ".join(title_candidates))
        body_corpus = normalize_role(" ".join(text for text in normalized_texts[2:] if text))
        corpus = normalize_role(" ".join(text for text in normalized_texts if text))
        if not corpus:
            return ""

        title_has_analyst = has_any(title_corpus, analyst_titles)
        if (
            has_any(title_corpus, field_support_non_target_titles)
            and has_any(body_corpus or corpus, field_support_non_target_body_terms)
        ):
            return ""
        if (
            has_any(title_corpus, robot_service_non_target_titles)
            and has_any(corpus, robot_service_non_target_body_terms)
            and not has_any(corpus, strong_ai_work_terms + ("인공지능", "머신러닝", "딥러닝", "기계학습", "llm", "컴퓨터 비전", "컴퓨터비전"))
        ):
            return ""
        if (
            has_any(title_corpus, statistical_analyst_titles)
            and not has_any(corpus, strong_ai_work_terms + ("인공지능", "머신러닝", "딥러닝", "기계학습", "llm", "컴퓨터 비전", "컴퓨터비전"))
        ):
            return "데이터 분석가"
        if has_any(title_corpus, title_only_non_target):
            return ""
        if has_any(title_corpus, ("adas application engineer", "application engineer")) and has_any(corpus, ("adas", "에이디에이에스", "컴퓨터 비전", "컴퓨터비전", "자율주행")):
            return "인공지능 엔지니어"
        if has_any(title_corpus, strict_non_target_titles) and not title_has_analyst:
            return ""
        if has_any(primary_title, weak_ai_software_titles) and not has_any(body_corpus, strong_ai_work_terms):
            return ""
        if has_any(title_corpus, simple_hard_exclude) and not has_any(title_corpus, ("compiler", "graph optimization")):
            return ""
        if has_any(title_corpus, simple_soft_titles) and not target_signal(corpus):
            return ""
        if has_any(primary_title, service_non_target_titles) and not title_has_analyst and not (has_any(primary_title, service_allow) or target_signal(body_corpus)):
            return ""

        if has_any(title_corpus, ("ai model production", "document ai")):
            return "인공지능 엔지니어"
        if has_phrase(primary_title, "ai devops") and has_any(corpus, ("배포", "클라우드", "쿠버네티스", "kubernetes", "인공지능", "llm")):
            return "인공지능 엔지니어"
        if has_phrase(title_corpus, "ai research div") and has_phrase(title_corpus, "internship") and count_terms(corpus, academic_terms) > 0:
            return "인공지능 리서처"
        if has_any(title_corpus, data_scientist_titles):
            return "데이터 사이언티스트"
        if has_any(title_corpus, data_engineer_titles) and not title_has_analyst:
            return "인공지능 엔지니어"
        if title_has_analyst:
            return "데이터 분석가"
        if has_any(title_corpus, ("연구직", "연구원")) and target_signal(corpus) and not has_any(title_corpus, ("developer", "engineer", "개발자", "엔지니어")):
            return "인공지능 리서처"
        if has_any(title_corpus, research_engineer_titles + researcher_titles):
            academic_score = count_terms(corpus, academic_terms)
            delivery_score = count_terms(corpus, delivery_terms)
            if has_any(title_corpus, ("research scientist", "researcher", "리서처")) and not has_any(title_corpus, ("developer", "engineer", "개발자", "엔지니어")):
                return "인공지능 리서처"
            return "인공지능 리서처" if academic_score >= delivery_score + 1 else "인공지능 엔지니어"
        if has_any(title_corpus, ("engineer", "엔지니어", "developer", "개발자")) and target_signal(corpus):
            return "인공지능 엔지니어"

        old_role = old_classify(*texts)
        if old_role in ALLOWED_JOB_ROLES:
            return old_role
        return ""

    def refresh_existing_job_role(row: dict) -> tuple[str, dict[str, str]]:
        analysis_fields = {
            "주요업무_분석용": normalize(row.get("주요업무_분석용")),
            "자격요건_분석용": normalize(row.get("자격요건_분석용")),
            "우대사항_분석용": normalize(row.get("우대사항_분석용")),
            "핵심기술_분석용": normalize(row.get("핵심기술_분석용")),
            "상세본문_분석용": normalize(row.get("상세본문_분석용")),
        }
        if not any(analysis_fields.values()):
            analysis_fields = build_analysis_fields(row)
        role = classify_job_role(
            row.get("job_title_raw"),
            row.get("공고제목_표시"),
            analysis_fields.get("주요업무_분석용", ""),
            analysis_fields.get("자격요건_분석용", ""),
            analysis_fields.get("우대사항_분석용", ""),
            analysis_fields.get("핵심기술_분석용", ""),
            analysis_fields.get("상세본문_분석용", ""),
        )
        return role, analysis_fields

    def refresh_merged_job_rows(frame: pd.DataFrame) -> pd.DataFrame:
        if frame.empty:
            return frame
        rows: list[dict] = []
        for row in frame.fillna("").to_dict(orient="records"):
            role, analysis_fields = refresh_existing_job_role(row)
            if role not in ALLOWED_JOB_ROLES:
                continue
            row["job_role"] = role
            row.update(analysis_fields)
            row.update(build_display_fields(row, analysis_fields=analysis_fields))
            rows.append({column: row.get(column, "") for column in JOB_COLUMNS})
        return pd.DataFrame(rows, columns=list(JOB_COLUMNS))

    def merge_incremental_with_role_refresh(*args, **kwargs) -> pd.DataFrame:
        merged = old_merge_incremental(*args, **kwargs)
        return refresh_merged_job_rows(merged)

    extra_focus = (
        ("문서AI", (re.compile(r"\bdocument ai\b", re.I), re.compile(r"\bocr\b", re.I), re.compile(r"광학문자인식"), re.compile(r"문서"))),
        ("AI반도체", (re.compile(r"\bnpu\b", re.I), re.compile(r"\bgpu\b", re.I), re.compile(r"\blpu\b", re.I), re.compile(r"엔피유"), re.compile(r"지피유"), re.compile(r"엘피유"), re.compile(r"가속기"), re.compile(r"반도체"))),
        ("검증", (re.compile(r"(?:모델|성능|품질|시스템|알고리즘|인공지능|AI)\s*검증", re.I), re.compile(r"테스트벤치"), re.compile(r"\bverification\b", re.I), re.compile(r"\bvalidation\b", re.I))),
        ("헬스케어", (re.compile(r"의료"), re.compile(r"임상"), re.compile(r"심전도"), re.compile(r"\becg\b", re.I))),
        ("자율주행", (re.compile(r"자율주행"), re.compile(r"\badas\b", re.I), re.compile(r"에이디에이에스"), re.compile(r"\bvslam\b", re.I), re.compile(r"측위"), re.compile(r"\blocalization\b", re.I), re.compile(r"\bpositioning\b", re.I), re.compile(r"센서\s*퓨전"), re.compile(r"센서\s*퓨젼"))),
        ("인프라", (re.compile(r"런타임"), re.compile(r"드라이버"), re.compile(r"\bsdk\b", re.I), re.compile(r"쿠버네티스"), re.compile(r"\bkubernetes\b", re.I), re.compile(r"\bci/cd\b", re.I))),
    )
    presentation._FOCUS_PATTERNS = extra_focus + presentation._FOCUS_PATTERNS

    role_inputs = ("job_title_raw", "공고제목_표시", "주요업무_분석용", "자격요건_분석용", "우대사항_분석용", "핵심기술_분석용", "상세본문_분석용")

    def semantic_role_audit(active_jobs: pd.DataFrame) -> dict[str, object]:
        mismatch_samples: list[dict[str, str]] = []
        non_target_samples: list[dict[str, str]] = []
        mismatch_count = 0
        non_target_count = 0
        for row in active_jobs.fillna("").to_dict(orient="records"):
            expected = classify_job_role(*[row.get(column, "") for column in role_inputs])
            current = normalize(row.get("job_role"))
            sample = {
                "company_name": normalize(row.get("company_name")),
                "job_title_raw": normalize(row.get("job_title_raw")),
                "job_role": current,
                "expected_role": expected,
                "job_url": normalize(row.get("job_url")),
            }
            if expected not in ALLOWED_JOB_ROLES:
                non_target_count += 1
                if len(non_target_samples) < 12:
                    non_target_samples.append(sample)
            elif current != expected:
                mismatch_count += 1
                if len(mismatch_samples) < 12:
                    mismatch_samples.append(sample)
        return {
            "role_mismatch_count": mismatch_count,
            "role_non_target_count": non_target_count,
            "role_mismatch_samples": mismatch_samples,
            "role_non_target_samples": non_target_samples,
        }

    def evaluate_quality_gate(staging_jobs: pd.DataFrame, source_registry: pd.DataFrame, *, settings=None, paths=None, already_filtered: bool = False) -> GateResult:
        result = old_evaluate_quality_gate(staging_jobs, source_registry, settings=settings, paths=paths, already_filtered=already_filtered)
        if "is_active" in staging_jobs.columns:
            active_jobs = staging_jobs[staging_jobs["is_active"].astype(str).str.lower().isin(["true", "1"])].copy()
        else:
            active_jobs = staging_jobs.copy()
        audit = semantic_role_audit(active_jobs)
        metrics = dict(result.metrics)
        metrics.update(audit)
        reasons = list(result.reasons)
        if audit["role_non_target_count"] and "최신 직무 분류 기준으로 비타깃 공고가 활성 행에 포함되어 있습니다." not in reasons:
            reasons.append("최신 직무 분류 기준으로 비타깃 공고가 활성 행에 포함되어 있습니다.")
        if audit["role_mismatch_count"] and "최신 직무 분류 기준과 다른 분류직무가 활성 행에 포함되어 있습니다." not in reasons:
            reasons.append("최신 직무 분류 기준과 다른 분류직무가 활성 행에 포함되어 있습니다.")
        passed = bool(result.passed and not audit["role_non_target_count"] and not audit["role_mismatch_count"])
        return GateResult(passed=passed, reasons=reasons, metrics=metrics)

    collection.classify_job_role = classify_job_role
    collection.merge_incremental = merge_incremental_with_role_refresh
    collection._call_gemini_for_html_jobs = call_llm_for_html_jobs
    collection._extract_html_jobs_with_gemini = extract_html_jobs_with_llm
    collection._refine_jobs_with_gemini = refine_jobs_with_llm
    collection._refresh_existing_job_role = refresh_existing_job_role
    collection._refresh_merged_job_rows = refresh_merged_job_rows
    quality.evaluate_quality_gate = evaluate_quality_gate


try:
    _install()
except Exception as exc:  # noqa: BLE001
    print(f"[jobs_market_v2] production guard failed to install: {exc}", file=sys.stderr)
