"""Job collection and incremental merge logic."""

from __future__ import annotations

import base64
import html
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic, time
from typing import Any, Callable
from urllib.parse import parse_qs, urlencode, urljoin, urlsplit, urlunsplit
from xml.etree import ElementTree

import httpx
import pandas as pd
from bs4 import BeautifulSoup
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from .constants import ALLOWED_JOB_ROLES, JOB_COLUMNS, RAW_DETAIL_COLUMNS, ROLE_KEYWORDS, SOURCE_REGISTRY_COLUMNS
from .gemini import (
    GeminiBudget,
    _active_llm_api_key,
    _active_llm_model,
    _call_json_llm,
    maybe_refine_analysis_fields,
    maybe_salvage_job_role,
    needs_gemini_refinement,
)
from .html_utils import clean_html_text, extract_sections_from_description
from .macos_ocr import extract_text_from_asset_urls
from .network import build_timeout
from .presentation import build_analysis_fields, build_display_fields, section_output_is_substantive
from .storage import atomic_write_text, coerce_bool
from .utils import canonicalize_runtime_source_url, dump_json, load_yaml, normalize_whitespace, stable_hash


_KOREA_LOCATION_TOKENS = (
    "korea",
    "south korea",
    "seoul",
    "대한민국",
    "한국",
    "서울",
    "판교",
    "성남",
    "수원",
    "인천",
    "대전",
    "대구",
    "광주",
    "울산",
    "부산",
    "jeju",
)
_CLOSED_JOB_STATUS_MARKERS = (
    "채용완료",
    "채용 완료",
    "채용마감",
    "채용 마감",
    "모집마감",
    "모집 마감",
    "지원마감",
    "지원 마감",
    "공고마감",
    "공고 마감",
    "application closed",
    "applications closed",
    "position closed",
    "position filled",
)
_OPEN_JOB_STATUS_MARKERS = (
    "채용중",
    "채용 중",
    "모집중",
    "모집 중",
    "지원가능",
    "지원 가능",
    "open position",
    "open positions",
    "open role",
    "open roles",
    "hiring",
    "recruiting",
)
_CLOSED_DESCRIPTION_EXCLUDE_PATTERNS = (
    "조기 마감",
    "조기마감",
    "될 수 있",
    "될수있",
    "인재풀",
    "참고 부탁",
    "참고 바랍니다",
)

_ROLE_TEXT_NORMALIZE_RE = re.compile(r"[^0-9a-z가-힣]+")
_ALPHA_HANGUL_BOUNDARY_RE = re.compile(r"(?<=[A-Za-z])(?=[가-힣])|(?<=[가-힣])(?=[A-Za-z])")

_AI_ENGINEER_SIGNAL_PHRASES = (
    "machine learning",
    "neural network",
    "ml",
    "mlops",
    "ml ops",
    "llm",
    "foundation model",
    "document ai",
    "computer vision",
    "speech recognition",
    "asr",
    "nlp",
    "natural language",
    "vision language",
    "vision language action",
    "perception",
    "adas",
    "slam",
    "localization",
    "positioning",
    "ai",
    "artificial intelligence",
    "인공지능",
    "머신러닝",
    "딥러닝",
    "엘엘엠",
    "브이엘엠",
    "음성인식",
    "자연어",
    "컴퓨터 비전",
    "컴퓨터비전",
    "엔피유",
    "엘피유",
    "지피유",
    "광학문자인식",
    "자율주행",
    "에이디에이에스",
    "센서 퓨전",
    "센서 퓨젼",
    "가속기",
    "런타임",
    "추론",
)

_AI_ENGINEER_EXCLUSION_PHRASES = (
    "qa",
    "quality assurance",
    "business developer",
    "business development",
    "product management",
    "product manager",
    "marketing",
    "marketer",
    "crm marketer",
    "growth manager",
    "operations manager",
    "risk manager",
    "sales manager",
    "sales",
    "recruiter",
    "recruiting",
    "coordinator",
    "planner",
    "consultant",
    "designer",
    "creative jobs",
    "creative",
    "p&c",
    "security engineer",
    "ai security",
    "dba",
    "database administrator",
    "backend software engineer",
    "software engineer backend",
    "backend engineer",
    "backend developer",
    "백엔드",
    "백엔드 개발자",
    "frontend software engineer",
    "software engineer frontend",
    "frontend engineer",
    "frontend developer",
    "프론트엔드",
    "프론트엔드 개발자",
    "full stack engineer",
    "fullstack engineer",
    "data engineer",
)

_NON_TARGET_TITLE_ONLY_PHRASES = (
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
)
_SERVICE_HARD_EXCLUSION_TITLE_PHRASES = (
    "software engineer system",
    "soc design verification",
    "design verification engineer",
    "verification engineer",
    "firmware",
    "펌웨어",
    "fe junior",
    "fe developer",
    "fe 개발자",
)
_SERVICE_SOFT_EXCLUSION_TITLE_PHRASES = (
    "software engineer system",
    "software engineer",
    "software developer",
    "system software engineer",
    "platform software engineer",
)
_SERVICE_ALLOW_TITLE_PHRASES = (
    "software engineer machine learning",
    "software engineer, machine learning",
    "machine learning software engineer",
    "machine learning platform engineer",
    "machine learning engineer",
    "ml engineer",
    "mlops engineer",
    "ai engineer",
    "ai system software engineer",
    "ai compiler engineer",
    "deep learning engineer",
)
_SERVICE_TARGET_WORK_PHRASES = (
    "machine learning",
    "ml",
    "mlops",
    "llm",
    "rag",
    "foundation model",
    "computer vision",
    "model training",
    "model serving",
    "model optimization",
    "inference",
    "ai 반도체",
    "npu",
    "gpu",
    "머신러닝",
    "딥러닝",
    "엘엘엠",
    "브이엘엠",
    "컴퓨터 비전",
    "컴퓨터비전",
    "모델 학습",
    "모델 훈련",
    "모델 서빙",
    "추론",
    "엔피유",
    "지피유",
    "인공지능 반도체",
)

_AI_ENGINEER_DELIVERY_PHRASES = (
    "engineer",
    "developer",
    "엔지니어",
    "개발자",
    "개발",
    "검증",
    "verification",
    "test engineer",
    "모델 개발자",
    "모델 엔지니어",
    "model developer",
    "model engineer",
)
_GENERAL_SERVICE_DELIVERY_TITLE_PHRASES = (
    "service developer",
    "service engineer",
    "서비스 개발자",
    "서비스 엔지니어",
)
_GENERAL_SERVICE_DELIVERY_BODY_PHRASES = (
    "web service",
    "mobile service",
    "웹 서비스",
    "웹/모바일 서비스",
    "ux/ui",
    "restful api",
    "react",
    "node.js",
    "django",
    "database",
    "sql",
    "nosql",
    "서비스 플랫폼",
    "서비스 성능",
    "프로젝트 관리",
)
_STRONG_AI_WORK_PHRASES = (
    "모델 개발",
    "모델링",
    "model development",
    "modeling",
    "model training",
    "training",
    "inference",
    "데이터사이언스",
    "data science",
    "ml model",
    "machine learning model",
    "딥러닝 모델",
    "머신러닝 모델",
    "computer vision",
    "nlp",
    "자연어 처리",
    "컴퓨터 비전",
    "llm",
)

_RESEARCH_SIGNAL_PHRASES = (
    "ai",
    "인공지능",
    "machine learning",
    "ml",
    "llm",
    "foundation model",
    "vision language",
    "vision language action",
    "computer vision",
    "document ai",
    "robotics",
    "slam",
    "localization",
    "positioning",
    "deep learning",
    "딥러닝",
    "머신러닝",
    "자율주행",
    "컴퓨터비전",
)

_ANALYST_TITLE_PHRASES = (
    "data analyst",
    "analytics analyst",
    "monetization analyst",
    "데이터 분석가",
    "데이터분석가",
    "분석가",
)

_DATA_ENGINEER_TITLE_PHRASES = (
    "data engineer",
    "data analytics engineer",
    "analytics engineer",
    "데이터 엔지니어",
)

_DATA_SCIENTIST_TITLE_PHRASES = (
    "data scientist",
    "데이터 사이언티스트",
    "applied scientist",
    "머신러닝 사이언티스트",
)

_ATTACHED_DATA_ANALYST_TITLE_PHRASES = (
    "데이터 분석가",
    "데이터분석가",
)

_ATTACHED_DATA_SCIENTIST_TITLE_PHRASES = (
    "데이터 사이언티스트",
    "데이터사이언티스트",
    "데이터 사이언스",
    "데이터사이언스",
)

_ANALYST_CONTEXT_PHRASES = (
    "데이터 분석",
    "데이터분석",
    "analytics",
    "analysis",
    "dashboard",
    "대시보드",
    "sql",
    "에스큐엘",
    "a/b test",
    "ab test",
    "experiment",
    "실험",
    "metric",
    "kpi",
    "지표",
    "cohort",
    "funnel",
    "reporting",
    "리포트",
)

_NON_TARGET_TITLE_PHRASES = (
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
    "creative jobs",
    "creative",
    "frontend",
    "backend",
    "service developer",
    "service engineer",
    "software engineer",
    "devops engineer",
    "platform engineer",
    "security engineer",
    "architect",
    "solution architect",
    "qa engineer",
    "quality engineer",
    "p&c",
    "hr",
)

_STRICT_NON_TARGET_TITLE_PHRASES = (
    "product manager",
    "growth manager",
    "operations manager",
    "risk manager",
    "business developer",
    "business development",
    "sales manager",
    "sales",
    "marketer",
    "crm marketer",
    "recruiter",
    "recruiting",
    "coordinator",
    "planner",
    "consultant",
    "designer",
    "creative jobs",
    "creative",
    "architect",
    "solution architect",
    "p&c",
    "hr",
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
    "변호사",
    "counsel",
    "security engineer",
    "qa engineer",
    "quality engineer",
    "ssd product validation",
    "product validation engineer",
)

_TITLE_ONLY_NON_TARGET_PHRASES = (
    "business analyst",
    "country manager",
    "project manager",
    "program manager",
    "qa engineering",
    "cos manager",
    "ax manager",
    "applied ai project manager",
    "official website",
    "careers at",
    "마케터",
    "기획자",
    "서비스기획",
    "인사기획",
    "경영기획",
    "상품 매니지먼트",
    "growth lead",
    "technical pm",
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
    "mechatronics r&d",
    "product owner",
    "기획/전략 담당",
    "기획/전략 담당자",
    "전략/운영 담당",
    "운영 담당",
    "운영 담당자",
    "전략기획",
    "영업총괄",
    "솔루션 영업",
    "전략구매",
    "세일즈 전략",
    "cx 운영",
    "운영 기획",
    "서비스 기획",
    "프로젝트 개발자",
    "전자정부 프레임워크",
    "node.js 프로젝트 개발자",
    "품질 관리",
    "operations specialist",
    "솔루션 컨설턴트",
    "인재풀 등록",
    "team leader",
    "pm",
    "구매",
    "펄어비스",
)

_WORK24_TITLE_AI_SIGNAL_PHRASES = (
    "ai",
    "인공지능",
    "vision",
    "비전",
)

_WORK24_TITLE_ALGORITHM_SIGNAL_PHRASES = (
    "algorithm",
    "알고리즘",
)

_WORK24_TITLE_AUTOMATION_SIGNAL_PHRASES = (
    "ai 자동화설계",
    "ai자동화설계",
    "인공지능 자동화설계",
    "인공지능자동화설계",
)

_WORK24_TITLE_AI_DELIVERY_PHRASES = (
    "개발",
    "개발자",
    "엔지니어",
    "앱개발",
    "앱 개발",
    "서비스개발",
    "서비스 개발",
    "설계",
    "구현",
)

_WORK24_TITLE_AI_EXCLUSION_PHRASES = (
    "ux",
    "ui",
    "ux ui",
    "ui ux",
    "designer",
    "디자이너",
    "frontend",
    "프론트엔드",
    "backend",
    "백엔드",
)

_SERVICE_NON_TARGET_TITLE_PHRASES = (
    "frontend",
    "프론트엔드",
    "backend",
    "back-end",
    "백엔드",
    "service developer",
    "service engineer",
    "software engineer",
    "platform engineer",
    "devops engineer",
    "application engineer",
    "integration engineer",
    "design engineer",
    "dft engineer",
    "soc design engineer",
)

_SERVICE_TARGET_ALLOWLIST_PHRASES = (
    "software engineer, machine learning",
    "machine learning software engineer",
    "machine learning engineer",
    "machine learning platform engineer",
    "ml engineer",
    "mlops engineer",
    "ml/mlops",
    "ai engineer",
    "applied ai engineer",
    "applied ai technical engineer",
    "ai research engineer",
    "deep learning engineer",
    "computer vision",
    "vision language",
    "robotics",
    "llm platform engineer",
    "ai platform engineer",
    "model serving",
    "model training",
    "npu software engineer",
    "gpu software engineer",
    "ai system software engineer",
    "data platform",
    "data pipeline",
)

_SIMPLE_DEVELOPER_HARD_EXCLUDE_TITLE_PHRASES = (
    "backend software engineer",
    "software engineer backend",
    "backend 개발자",
    "backend engineer",
    "backend developer",
    "back-end engineer",
    "back-end developer",
    "백엔드",
    "백엔드 개발자",
    "frontend software engineer",
    "software engineer frontend",
    "frontend engineer",
    "frontend developer",
    "front-end engineer",
    "front-end developer",
    "프론트엔드",
    "프론트엔드 개발자",
    "full stack engineer",
    "full stack",
    "full stack developer",
    "fullstack engineer",
    "fullstack",
    "fullstack developer",
    "full-stack",
    "풀스택",
    "cross platform",
    "크로스 플랫폼",
    "mobile developer",
    "ios developer",
    "android developer",
    "web developer",
    "app developer",
    "server developer",
)

_SIMPLE_DEVELOPER_SOFT_TITLE_PHRASES = (
    "software engineer",
    "software developer",
    "platform software engineer",
    "system software engineer",
    "forward deployed software engineer",
    "console",
)

_SIMPLE_DEVELOPER_TARGET_ALLOWLIST_PHRASES = (
    "machine learning",
    "ml ",
    "ml/",
    "llm",
    "model serving",
    "quantization",
    "npu",
    "gpu",
    "compiler",
    "inference",
    "firmware",
    "embedded",
    "sdk",
    "ai system software engineer",
    "ai platform",
    "data platform",
    "data pipeline",
    "데이터 플랫폼",
    "데이터 파이프라인",
    "ai 반도체",
    "반도체",
    "chip",
    "vision language",
    "computer vision",
    "컴퓨터 비전",
    "컴퓨터비전",
    "엘엘엠",
    "브이엘엠",
    "엔피유",
    "엘피유",
    "지피유",
    "딥러닝",
    "인공지능 가속",
    "광학문자인식",
    "자율주행",
    "에이디에이에스",
    "센서 퓨전",
    "센서 퓨젼",
    "가속기",
    "런타임",
    "추론",
    "모델 서빙",
    "모델 개발",
    "robotics",
    "slam",
    "localization",
    "positioning",
)

_DATA_SCIENTIST_CONTEXT_PHRASES = (
    "data science",
    "데이터 사이언스",
    "데이터사이언스",
    "applied scientist",
    "모델링",
    "modeling",
    "forecast",
    "예측",
    "causal inference",
    "인과 추론",
    "statistical modeling",
    "통계 모델",
)

_STATISTICAL_ANALYST_TITLE_PHRASES = (
    "통계분석",
    "통계 분석",
    "빅데이터 분석",
    "데이터 분석 연구원",
    "리서치 통계분석",
)

_ROBOT_SERVICE_NON_TARGET_TITLE_PHRASES = (
    "robot service",
    "robot cs",
    "robot as",
    "cs as engineer",
    "서비스 cs as",
    "서비스 엔지니어",
    "로봇 서비스",
)

_ROBOT_SERVICE_NON_TARGET_BODY_PHRASES = (
    "유지보수",
    "기술 지원",
    "정기 점검",
    "긴급 장애 대응",
    "장애 대응",
    "부품 교체",
    "입고 테스트",
    "서비스 매뉴얼",
)

_RESEARCHER_TITLE_PHRASES = (
    "research scientist",
    "ai researcher",
    "ml researcher",
    "researcher",
    "리서처",
    "연구직",
    "연구원",
    "postdoctoral researcher",
)

_MIXED_RESEARCHER_TITLE_PHRASES = (
    "developer researcher",
    "engineer researcher",
    "개발자 researcher",
    "개발자 리서처",
    "엔지니어 researcher",
    "엔지니어 리서처",
)

_RESEARCH_ENGINEER_TITLE_PHRASES = (
    "research engineer",
    "리서치 엔지니어",
    "연구 엔지니어",
)

_RESEARCH_ACADEMIC_PHRASES = (
    "논문",
    "학회",
    "출판",
    "저널",
    "publication",
    "publish",
    "published",
    "conference",
    "workshop",
    "journal",
    "박사",
    "phd",
    "postdoc",
    "제1저자",
    "교신저자",
    "first author",
    "benchmark",
    "sota",
    "state of the art",
)

_RESEARCH_WORK_PHRASES = (
    "research",
    "연구",
    "실험",
    "experiment",
    "가설",
    "methodology",
    "ablation",
    "evaluate",
    "evaluation",
)

_ENGINEER_DELIVERY_PHRASES = _AI_ENGINEER_DELIVERY_PHRASES + (
    "설계",
    "구현",
    "개발",
    "구축",
    "운영",
    "배포",
    "서빙",
    "pipeline",
    "파이프라인",
    "framework",
    "인프라",
    "infra",
    "infrastructure",
    "system",
    "시스템",
    "service",
    "서비스",
    "product",
    "제품",
    "workflow",
    "workflows",
    "agent",
    "agents",
    "integration",
    "통합",
    "build",
    "design",
    "implement",
    "deploy",
    "serve",
    "serving",
)

_HTML_LISTING_TEXT_HINTS = (
    "모집 부서",
    "모집 분야",
    "모집 경력",
    "모집 기간",
    "공개채용",
    "채용",
    "employment",
    "recruit",
    "career",
)

_HTML_LISTING_STRONG_TEXT_HINTS = (
    "모집 부서",
    "모집 분야",
    "모집 경력",
    "모집 기간",
    "공개채용",
)
_HTML_LISTING_METADATA_ONLY_HINTS = (
    "모집 부서",
    "모집 분야",
    "모집 경력",
    "근로 조건",
    "모집 기간",
)
_HTML_GEMINI_PROBE_TEXT_HINTS = (
    "채용",
    "공고",
    "모집",
    "recruit",
    "career",
    "job",
    "position",
    "engineer",
    "research",
    "ai",
    "data",
    "machine learning",
    "ml",
    "인공지능",
    "데이터",
    "연구",
    "개발",
)
_NEXT_RSC_PUSH_RE = re.compile(r"self\.__next_f\.push\(\[1,(.*)\]\)\s*$", flags=re.DOTALL)
_NEXT_RSC_CONTENT_MARKER_RE = re.compile(r"^([0-9a-z]+):T[0-9a-f]+,$")
_GENERIC_FAMILY_POSTING_HINTS = (
    "전문연구요원",
    "상시 모집",
    "상시모집",
    "상시 채용",
    "상시채용",
)
_HTML_GEMINI_MAX_ANCHORS = 80
_HTML_GENERIC_HIRING_NAV_HINTS = (
    "채용",
    "채용공고",
    "채용 공고",
    "채용홈페이지",
    "채용 홈페이지",
    "채용정보",
    "채용 정보",
    "인재채용",
    "인재 채용",
    "career",
    "careers",
    "jobs",
    "job board",
    "employment",
    "recruit",
)
_HTML_GENERIC_HIRING_NAV_EXCLUDE_HINTS = (
    "채용문의",
    "채용 문의",
    "채용절차",
    "채용 절차",
    "입사지원",
    "지원하기",
    "채용상담",
    "입찰공고",
    "faq",
    "복리후생",
    "인재상",
)
_HTML_GENERIC_DETAIL_CTA_HINTS = (
    "자세히 보기",
    "상세 보기",
    "상세보기",
    "view details",
    "read more",
    "learn more",
    "more",
)
_HTML_REDIRECT_FOLLOW_MAX_DEPTH = 2
_HTML_DETAIL_FETCH_LIMIT_PER_SOURCE = 12
_HTML_PRIORITY_PATH_HINTS = (
    "recruit",
    "rcrt",
    "career",
    "jobs",
    "job",
    "employment",
    "hire",
    "joinus",
    "join-us",
    "talent",
)
_HTML_STRONG_LIST_PATH_HINTS = (
    "/careers",
    "careers/",
    "recruit_list",
    "recruitment",
    "jobnotice/list",
    "job-posting",
    "jobposting",
    "open-position",
    "/jobs",
    "jobs/",
    "/career/recruit",
    "/career/job",
    "/recruit/list",
    "/recruit/job",
)
_HTML_DETAIL_URL_HINTS = (
    "articleNo=",
    "mode=view",
    "annoId=",
    "relay/view",
    "rec_idx=",
    "/view.do",
    "boardView",
    "board/view",
    "jobnotice",
    "recruit_detail",
    "recruitDetail",
)
_ATS_REDIRECT_HOST_HINTS = (
    "career.greetinghr.com",
    "recruiter.co.kr",
    "greenhouse.io",
    "lever.co",
    "saramin.co.kr",
    "jobkorea.co.kr",
    "wanted.co.kr",
    "jumpit.saramin.co.kr",
)
_GREETINGHR_DETAIL_PATH_RE = re.compile(r"/(?:[a-z]{2}/)?o/([0-9]+)")
_GREETINGHR_PAGE_PATH_HINTS = ("recruit", "career", "job", "opening")
_GREETINGHR_PAGE_PATH_EXCLUDE_HINTS = ("benefit", "process", "intro", "privacy", "privacypolicy")
_BYTESIZE_API_BASE_URL = "https://career-api.thebytesize.ai/career/api/v1"
_SARAMIN_API_BASE_URL = "https://oapi.saramin.co.kr/job-search"
_SARAMIN_API_DEFAULT_FIELDS = "posting-date expiration-date keyword-code"
_WORKNET_LIST_API_BASE_URL = "https://www.work24.go.kr/cm/openApi/call/wk/callOpenApiSvcInfo210L01.do"
_WORKNET_DETAIL_API_BASE_URL = "https://www.work24.go.kr/cm/openApi/call/wk/callOpenApiSvcInfo210D01.do"
_WORKNET_DEFAULT_KEYWORD = "데이터 분석|데이터 사이언티스트|데이터사이언스|머신러닝|인공지능"
_WORKNET_DEFAULT_DISPLAY = "50"
_WORKNET_DETAIL_LIMIT_QUERY_KEY = "detailLimit"
_WORK24_PUBLIC_SEARCH_BASE_URL = "https://www.work24.go.kr/wk/a/b/1200/retriveDtlEmpSrchList.do"
_WORK24_PUBLIC_SEARCH_POST_URL = "https://www.work24.go.kr/wk/a/b/1200/retriveDtlEmpSrchListInPost.do"
_WORK24_PUBLIC_DEFAULT_RESULT_COUNT = "50"
_WORK24_PUBLIC_DEFAULT_PAGE_LIMIT = "1"
_WORK24_PUBLIC_DEFAULT_SCAN_DEPTH = "1"
_WORK24_PUBLIC_DEFAULT_HOT_PAGE_LIMIT = "1"
_WORK24_PUBLIC_DEFAULT_DETAIL_LIMIT = "0"
_WORK24_PUBLIC_PROGRESS_FILE_NAME = "work24_public_progress.json"
_WORK24_PUBLIC_PROGRESS_VERSION = "v1"
_WORK24_PUBLIC_TRACKING_CACHE_VERSION = "v1"
_WORK24_PUBLIC_TRACKING_MAX_ITEMS = 25
_WORK24_PUBLIC_SEEN_AUTH_LIMIT = 5000
_SARAMIN_RELAY_PATH_HINTS = (
    "/zf_user/jobs/relay/view",
    "/job-search/view",
)
_SARAMIN_EMBEDDED_DETAIL_RE = re.compile(r"contents:\s*'([^']+)'")

_LEGAL_ENTITY_PREFIX_RE = re.compile(r"^(?:주식회사|㈜|\(주\))\s*")
_LEGAL_ENTITY_SUFFIX_RE = re.compile(r"\s*(?:\(주\)|㈜)$")
_RECRUITER_ATTACHMENT_EXTENSIONS = (".pdf", ".png", ".jpg", ".jpeg", ".webp")
_SOURCE_COLLECTION_BUCKET_PRIORITY = {
    "approved": 0,
    "candidate": 1,
}
_SOURCE_COLLECTION_TYPE_PRIORITY = {
    "saramin_api": 0,
    "worknet_api": 0,
    "work24_public_html": 0,
    "greenhouse": 1,
    "lever": 2,
    "greetinghr": 3,
    "recruiter": 4,
    "html_page": 5,
}
_SOURCE_COLLECTION_PROGRESS_POLICY_VERSION = "v8"
_SOURCE_COLLECTION_HOT_ACTIVE_JOB_COUNT = 4
_SOURCE_COLLECTION_ACTIVE_PIN_LIMIT = 24
_SOURCE_COLLECTION_HTML_SCOUT_LIMIT = 24


def _source_last_active_job_count(row: dict[str, Any]) -> int:
    value = row.get("last_active_job_count")
    try:
        return max(int(value or 0), 0)
    except (TypeError, ValueError):
        return 0


def _html_source_has_hiring_path_signal(row: dict[str, Any]) -> bool:
    source_url = normalize_whitespace(row.get("source_url")).lower()
    parsed = urlsplit(source_url)
    haystack = f"{parsed.path} {parsed.query}".lower()
    return any(hint in haystack for hint in _HTML_PRIORITY_PATH_HINTS)


def _html_source_collection_tiebreaker(row: dict[str, Any]) -> int:
    source_url = normalize_whitespace(row.get("source_url")).lower()
    parsed = urlsplit(source_url)
    haystack = f"{parsed.path} {parsed.query}".lower()
    if any(hint in haystack for hint in _HTML_STRONG_LIST_PATH_HINTS):
        return 0
    if "list" in haystack:
        return 1
    if any(hint in haystack for hint in ("career", "recruit", "jobs", "job", "rcrt")):
        return 2
    return 3


def _html_source_is_strong_listing_candidate(row: dict[str, Any]) -> bool:
    return _html_source_collection_tiebreaker(row) <= 1


def _source_collection_signal_priority(row: dict[str, Any]) -> tuple[int, int, int]:
    last_active_job_count = _source_last_active_job_count(row)
    verification_status = normalize_whitespace(row.get("verification_status"))
    source_type = normalize_whitespace(row.get("source_type"))
    type_priority = _SOURCE_COLLECTION_TYPE_PRIORITY.get(source_type, 9)
    has_last_success = bool(normalize_whitespace(row.get("last_success_at")))
    is_ats_source = type_priority < _SOURCE_COLLECTION_TYPE_PRIORITY["html_page"]
    is_hiring_html_source = source_type == "html_page" and _html_source_has_hiring_path_signal(row)
    if last_active_job_count >= _SOURCE_COLLECTION_HOT_ACTIVE_JOB_COUNT:
        return (0, -last_active_job_count, type_priority)
    if not has_last_success and is_ats_source:
        return (1, type_priority, 0)
    if last_active_job_count > 0:
        return (2, type_priority, 0)
    if verification_status == "성공" and is_ats_source and last_active_job_count == 0:
        return (3, type_priority, 0)
    if verification_status == "성공" and is_hiring_html_source and last_active_job_count == 0:
        return (4, _html_source_collection_tiebreaker(row), 0)
    if verification_status != "성공" and is_hiring_html_source:
        return (5, _html_source_collection_tiebreaker(row), 0)
    if verification_status != "성공":
        return (6, type_priority, 0)
    if is_ats_source:
        return (7, type_priority, 0)
    if is_hiring_html_source:
        return (8, _html_source_collection_tiebreaker(row), 0)
    return (9, type_priority, 0)


def _is_html_scout_candidate(row: dict[str, Any]) -> bool:
    return (
        normalize_whitespace(row.get("source_type")) == "html_page"
        and _html_source_has_hiring_path_signal(row)
        and _html_source_is_strong_listing_candidate(row)
        and _source_last_active_job_count(row) <= 0
    )


def _ordered_collectable_positions(rows: list[dict[str, Any]]) -> list[int]:
    sorted_positions = sorted(
        (index for index, row in enumerate(rows) if _is_collectable_source(row)),
        key=lambda index: _collectable_source_priority(rows[index]),
    )
    if not sorted_positions:
        return []

    grouped_positions: dict[tuple[int, int], dict[str, list[int]]] = {}
    group_order: list[tuple[int, int]] = []
    for position in sorted_positions:
        row = rows[position]
        signal_priority, _, _ = _source_collection_signal_priority(row)
        group_key = (
            _SOURCE_COLLECTION_BUCKET_PRIORITY.get(normalize_whitespace(row.get("source_bucket")), 9),
            signal_priority,
        )
        source_type = normalize_whitespace(row.get("source_type"))
        if group_key not in grouped_positions:
            grouped_positions[group_key] = {}
            group_order.append(group_key)
        grouped_positions[group_key].setdefault(source_type, []).append(position)

    ordered_positions: list[int] = []
    for group_key in group_order:
        type_groups = grouped_positions[group_key]
        ordered_types = sorted(type_groups, key=lambda value: (_SOURCE_COLLECTION_TYPE_PRIORITY.get(value, 9), value))
        while True:
            emitted = False
            for source_type in ordered_types:
                queue = type_groups[source_type]
                if not queue:
                    continue
                ordered_positions.append(queue.pop(0))
                emitted = True
            if not emitted:
                break
    return ordered_positions


def _select_incremental_collectable_positions(
    collectable_positions: list[int],
    rows: list[dict[str, Any]],
    *,
    start_offset: int,
    process_limit: int,
) -> tuple[list[int], list[int], list[int]]:
    if process_limit <= 0:
        cursor_positions = collectable_positions[start_offset:]
        return cursor_positions, cursor_positions, []

    forced_limit = min(max(process_limit // 10, 1), _SOURCE_COLLECTION_ACTIVE_PIN_LIMIT)
    forced_positions: list[int] = []
    forced_seen: set[int] = set()
    for position in collectable_positions:
        if len(forced_positions) >= forced_limit:
            break
        if not coerce_bool(rows[position].get("_always_refresh_source")):
            continue
        if position in forced_seen:
            continue
        forced_seen.add(position)
        forced_positions.append(position)

    cursor_positions = collectable_positions[start_offset : start_offset + process_limit]
    if not cursor_positions and not forced_positions:
        return cursor_positions, cursor_positions, forced_positions

    pinned_limit = min(process_limit // 5, _SOURCE_COLLECTION_ACTIVE_PIN_LIMIT)
    pinned_positions: list[int] = []
    pinned_seen: set[int] = set(forced_seen)
    if pinned_limit > 0 and start_offset > 0:
        for position in collectable_positions[:start_offset]:
            if _source_last_active_job_count(rows[position]) <= 0:
                continue
            if position in pinned_seen:
                continue
            pinned_seen.add(position)
            pinned_positions.append(position)
            if len(pinned_positions) >= pinned_limit:
                break

    scout_limit = min(max(process_limit // 3, 0), _SOURCE_COLLECTION_HTML_SCOUT_LIMIT)
    scout_positions: list[int] = []
    scout_seen: set[int] = set(pinned_seen)
    if scout_limit > 0 and start_offset > 0:
        for position in collectable_positions[start_offset:]:
            if position in scout_seen:
                continue
            if not _is_html_scout_candidate(rows[position]):
                continue
            scout_seen.add(position)
            scout_positions.append(position)
            if len(scout_positions) >= scout_limit:
                break

    remaining_capacity = max(process_limit - len(forced_positions) - len(pinned_positions) - len(scout_positions), 0)
    trimmed_cursor_positions: list[int] = []
    for position in cursor_positions:
        if len(trimmed_cursor_positions) >= remaining_capacity:
            break
        if position in scout_seen:
            continue
        trimmed_cursor_positions.append(position)
    cursor_selected_positions = scout_positions + trimmed_cursor_positions
    pinned_selected_positions = forced_positions + pinned_positions
    return pinned_selected_positions + cursor_selected_positions, cursor_selected_positions, pinned_selected_positions


def _normalize_role_text(value: str | None) -> str:
    text = normalize_whitespace(value).lower()
    if not text:
        return ""
    text = _ALPHA_HANGUL_BOUNDARY_RE.sub(" ", text)
    collapsed = normalize_whitespace(_ROLE_TEXT_NORMALIZE_RE.sub(" ", text))
    if not collapsed:
        return ""
    return f" {collapsed} "


def _has_phrase(text: str, phrase: str) -> bool:
    normalized_text = text if text.startswith(" ") else _normalize_role_text(text)
    normalized_phrase = _normalize_role_text(phrase)
    if not normalized_text or not normalized_phrase:
        return False
    return normalized_phrase in normalized_text


def _has_any_phrase(text: str, phrases: tuple[str, ...]) -> bool:
    return any(_has_phrase(text, phrase) for phrase in phrases)


def _count_phrase_hits(text: str, phrases: tuple[str, ...]) -> int:
    return sum(1 for phrase in phrases if _has_phrase(text, phrase))


def _has_attached_phrase(text: str, phrases: tuple[str, ...]) -> bool:
    normalized_text = text if text.startswith(" ") else _normalize_role_text(text)
    if not normalized_text:
        return False
    return any(
        normalized_phrase in normalized_text
        for phrase in phrases
        if (normalized_phrase := _normalize_role_text(phrase).strip())
    )


def _has_simple_developer_target_signal(text: str) -> bool:
    if _has_any_phrase(text, _SIMPLE_DEVELOPER_TARGET_ALLOWLIST_PHRASES):
        return True
    normalized_text = text if text.startswith(" ") else _normalize_role_text(text)
    # Korean particles often attach directly to signal nouns after cleanup
    # (for example, "엔피유가"). Keep this relaxed check scoped to the
    # simple-developer rescue allowlist so generic exclusions stay strict.
    return any(
        phrase.strip() in normalized_text
        for phrase in _SIMPLE_DEVELOPER_TARGET_ALLOWLIST_PHRASES
        if re.search(r"[가-힣]", phrase)
    )


def _is_simple_developer_title(title_corpus: str, body_corpus: str = "") -> bool:
    if not title_corpus:
        return False
    if _has_any_phrase(title_corpus, _SIMPLE_DEVELOPER_HARD_EXCLUDE_TITLE_PHRASES):
        return True
    if not _has_any_phrase(title_corpus, _SIMPLE_DEVELOPER_SOFT_TITLE_PHRASES):
        return False
    target_corpus = _normalize_role_text(f"{title_corpus} {body_corpus}")
    if _has_simple_developer_target_signal(target_corpus):
        return False
    return True


def _classify_ambiguous_research_engineer(
    title_corpus: str,
    body_corpus: str,
    corpus: str,
    *,
    prefer_title_researcher: bool = True,
) -> str:
    if prefer_title_researcher and _has_any_phrase(title_corpus, ("research scientist", "scientist", "researcher", "리서처")):
        return "인공지능 리서처"

    academic_score = _count_phrase_hits(corpus, _RESEARCH_ACADEMIC_PHRASES)
    academic_score += _count_phrase_hits(body_corpus, _RESEARCH_WORK_PHRASES)
    delivery_score = _count_phrase_hits(corpus, _ENGINEER_DELIVERY_PHRASES)

    if any(fragment in body_corpus for fragment in ("논문", "학회", "출판", "publication", "conference", "박사", "phd")):
        academic_score += 2
    if any(
        fragment in body_corpus
        for fragment in (
            "고객",
            "customer",
            "product",
            "제품",
            "service",
            "서비스",
            "pipeline",
            "파이프라인",
            "workflow",
            "agent",
            "배포",
            "서빙",
            "integration",
        )
    ):
        delivery_score += 2

    if delivery_score >= academic_score + 1:
        return "인공지능 엔지니어"
    if (
        academic_score >= delivery_score
        and any(fragment in corpus for fragment in ("논문", "학회", "출판", "publication", "conference"))
        and not any(
            fragment in body_corpus
            for fragment in ("제품", "서비스", "배포", "서빙", "파이프라인", "구축", "설계", "구현", "workflow", "agent")
        )
    ):
        return "인공지능 리서처"
    if academic_score >= delivery_score + 2:
        return "인공지능 리서처"
    return "인공지능 엔지니어"


def _classify_mixed_researcher_title(
    title_corpus: str,
    body_corpus: str,
    corpus: str,
) -> str:
    if _has_any_phrase(title_corpus, _NON_TARGET_TITLE_PHRASES):
        return ""
    if _has_any_phrase(title_corpus, _MIXED_RESEARCHER_TITLE_PHRASES):
        mixed_delivery_score = _count_phrase_hits(corpus, _ENGINEER_DELIVERY_PHRASES)
        mixed_academic_score = _count_phrase_hits(corpus, _RESEARCH_ACADEMIC_PHRASES)
        mixed_academic_score += _count_phrase_hits(body_corpus, _RESEARCH_WORK_PHRASES)
        if any(
            fragment in body_corpus
            for fragment in (
                "통합",
                "integration",
                "구현",
                "실행 가능",
                "온디바이스",
                "on device",
                "on-device",
                "추론 엔진",
                "inference engine",
            )
        ):
            mixed_delivery_score += 3
        if (
            _has_any_phrase(title_corpus, ("engineer", "developer", "엔지니어", "개발자"))
            and mixed_delivery_score >= mixed_academic_score
        ):
            return "인공지능 엔지니어"
        return _classify_ambiguous_research_engineer(
            title_corpus,
            body_corpus,
            corpus,
            prefer_title_researcher=False,
        )
    return "인공지능 리서처"


def classify_job_role(*texts: str | None) -> str:
    if not texts:
        return ""
    normalized_texts = [normalize_whitespace(text) for text in texts]
    title_candidates = [text for text in normalized_texts[:2] if text]
    if not title_candidates:
        return ""
    primary_title = _normalize_role_text(title_candidates[0])
    title_corpus = _normalize_role_text(" ".join(title_candidates))
    body_corpus = _normalize_role_text(" ".join(text for text in normalized_texts[2:] if text))
    corpus = _normalize_role_text(" ".join(text for text in normalized_texts if text))
    if not corpus:
        return ""
    if _is_simple_developer_title(title_corpus, body_corpus):
        return ""
    if (
        _has_any_phrase(title_corpus, _ROBOT_SERVICE_NON_TARGET_TITLE_PHRASES)
        and _has_any_phrase(corpus, _ROBOT_SERVICE_NON_TARGET_BODY_PHRASES)
        and not _has_any_phrase(corpus, _STRONG_AI_WORK_PHRASES + ("컴퓨터비전", "자율주행", "머신러닝", "딥러닝", "인공지능"))
    ):
        return ""
    if _has_any_phrase(title_corpus, _TITLE_ONLY_NON_TARGET_PHRASES):
        return ""
    if _has_attached_phrase(primary_title, _ATTACHED_DATA_SCIENTIST_TITLE_PHRASES):
        return "데이터 사이언티스트"
    if _has_attached_phrase(primary_title, _ATTACHED_DATA_ANALYST_TITLE_PHRASES):
        return "데이터 분석가"
    title_has_analyst_signal = _has_any_phrase(title_corpus, _ANALYST_TITLE_PHRASES)
    corpus_has_target_signal = _has_any_phrase(
        corpus,
        _AI_ENGINEER_SIGNAL_PHRASES
        + _RESEARCH_SIGNAL_PHRASES
        + _DATA_ENGINEER_TITLE_PHRASES
        + _DATA_SCIENTIST_TITLE_PHRASES,
    )
    if _has_any_phrase(title_corpus, _STRICT_NON_TARGET_TITLE_PHRASES) and not title_has_analyst_signal:
        return ""
    if (
        _has_any_phrase(primary_title, _SERVICE_NON_TARGET_TITLE_PHRASES)
        and not title_has_analyst_signal
        and not (
            _has_any_phrase(primary_title, _SERVICE_TARGET_ALLOWLIST_PHRASES)
            or _has_any_phrase(
                body_corpus,
                _AI_ENGINEER_SIGNAL_PHRASES
                + _RESEARCH_SIGNAL_PHRASES
                + _DATA_ENGINEER_TITLE_PHRASES
                + _DATA_SCIENTIST_TITLE_PHRASES,
            )
        )
    ):
        return ""
    if (
        not _has_any_phrase(title_corpus, _WORK24_TITLE_AI_EXCLUSION_PHRASES)
        and _has_any_phrase(title_corpus, _WORK24_TITLE_AI_DELIVERY_PHRASES)
        and (
            (
                _has_any_phrase(title_corpus, _WORK24_TITLE_AI_SIGNAL_PHRASES)
                and _has_any_phrase(title_corpus, _WORK24_TITLE_ALGORITHM_SIGNAL_PHRASES)
            )
            or _has_any_phrase(title_corpus, _WORK24_TITLE_AUTOMATION_SIGNAL_PHRASES)
        )
    ):
        return "인공지능 엔지니어"
    if _has_any_phrase(title_corpus, ("ai model production", "document ai")):
        return "인공지능 엔지니어"
    if (
        _has_any_phrase(title_corpus, _STATISTICAL_ANALYST_TITLE_PHRASES)
        and not _has_any_phrase(corpus, _STRONG_AI_WORK_PHRASES + ("인공지능", "머신러닝", "딥러닝", "기계학습", "llm", "컴퓨터 비전", "컴퓨터비전"))
    ):
        return "데이터 분석가"
    if (
        _has_phrase(primary_title, "ai devops")
        and _has_any_phrase(corpus, ("배포", "클라우드", "쿠버네티스", "kubernetes", "인공지능", "llm"))
    ):
        return "인공지능 엔지니어"
    if (
        _has_phrase(title_corpus, "ai research div")
        and _has_phrase(title_corpus, "internship")
        and _has_any_phrase(corpus, _RESEARCH_ACADEMIC_PHRASES + _RESEARCH_WORK_PHRASES + _RESEARCH_SIGNAL_PHRASES)
    ):
        return "인공지능 리서처"
    if _has_any_phrase(title_corpus, _DATA_SCIENTIST_TITLE_PHRASES):
        return "데이터 사이언티스트"
    if _has_any_phrase(title_corpus, _DATA_ENGINEER_TITLE_PHRASES) and not title_has_analyst_signal:
        return "인공지능 엔지니어"
    if (
        title_has_analyst_signal
        and not (
            _has_any_phrase(title_corpus, _AI_ENGINEER_SIGNAL_PHRASES)
            and _has_any_phrase(title_corpus, _AI_ENGINEER_DELIVERY_PHRASES)
        )
    ):
        return "데이터 분석가"
    if (
        _has_any_phrase(primary_title, _SERVICE_TARGET_ALLOWLIST_PHRASES)
        and _has_any_phrase(primary_title, _AI_ENGINEER_DELIVERY_PHRASES)
        and not _has_any_phrase(title_corpus, _RESEARCHER_TITLE_PHRASES + _RESEARCH_ENGINEER_TITLE_PHRASES)
    ):
        return "인공지능 엔지니어"
    if (
        _has_any_phrase(title_corpus, ("engineer", "엔지니어", "developer", "개발자"))
        and not title_has_analyst_signal
        and not _has_any_phrase(title_corpus, _RESEARCHER_TITLE_PHRASES + _RESEARCH_ENGINEER_TITLE_PHRASES)
        and corpus_has_target_signal
    ):
        return "인공지능 엔지니어"
    if any(_has_phrase(title_corpus, keyword) for keyword in ROLE_KEYWORDS["데이터 사이언티스트"]):
        return "데이터 사이언티스트"
    if _has_any_phrase(title_corpus, _RESEARCH_ENGINEER_TITLE_PHRASES):
        return _classify_ambiguous_research_engineer(title_corpus, body_corpus, corpus)
    if _has_phrase(primary_title, "postdoctoral researcher") and _has_any_phrase(corpus, _RESEARCH_SIGNAL_PHRASES):
        return "인공지능 리서처"
    if (
        _has_any_phrase(title_corpus, _RESEARCHER_TITLE_PHRASES)
        and _has_any_phrase(corpus, _RESEARCH_SIGNAL_PHRASES)
    ):
        return _classify_mixed_researcher_title(title_corpus, body_corpus, corpus)
    if (
        _has_any_phrase(corpus, ("데이터사이언스", "데이터 사이언스", "data science"))
        and _has_any_phrase(corpus, ("데이터분석", "데이터 분석", "analytics", "analysis"))
        and not _has_any_phrase(title_corpus, _NON_TARGET_TITLE_PHRASES)
    ):
        return "데이터 사이언티스트"
    if (
        _has_any_phrase(corpus, ("llm", "rag", "ai agent", "aiagent"))
        and _has_any_phrase(corpus, ("개발", "engineer", "developer", "엔지니어"))
    ):
        return "인공지능 엔지니어"
    for role in (
        "데이터 사이언티스트",
        "데이터 분석가",
        "인공지능 엔지니어",
    ):
        if any(_has_phrase(title_corpus, keyword) for keyword in ROLE_KEYWORDS[role]):
            return role
    if (
        _has_any_phrase(corpus, ("데이터분석", "데이터 분석", "analytics", "analysis"))
        and _has_any_phrase(corpus, _ANALYST_CONTEXT_PHRASES)
        and not _has_any_phrase(title_corpus, _RESEARCHER_TITLE_PHRASES + _RESEARCH_ENGINEER_TITLE_PHRASES)
        and not _has_any_phrase(title_corpus, _NON_TARGET_TITLE_PHRASES)
    ):
        return "데이터 분석가"
    if (
        _has_any_phrase(corpus, ("데이터사이언스", "데이터 사이언스", "data science", "applied scientist"))
        and _has_any_phrase(corpus, _DATA_SCIENTIST_CONTEXT_PHRASES + _AI_ENGINEER_SIGNAL_PHRASES)
        and not _has_any_phrase(title_corpus, _NON_TARGET_TITLE_PHRASES)
    ):
        return "데이터 사이언티스트"
    if not _has_any_phrase(primary_title, _AI_ENGINEER_EXCLUSION_PHRASES):
        if _has_phrase(primary_title, "ml ops engineer") or _has_phrase(primary_title, "mlops engineer"):
            return "인공지능 엔지니어"
        if (
            _has_any_phrase(primary_title, _AI_ENGINEER_SIGNAL_PHRASES)
            and _has_any_phrase(primary_title, _AI_ENGINEER_DELIVERY_PHRASES)
        ):
            return "인공지능 엔지니어"
        if (
            _has_any_phrase(corpus, _AI_ENGINEER_SIGNAL_PHRASES)
            and _has_any_phrase(primary_title, _AI_ENGINEER_DELIVERY_PHRASES)
        ):
            return "인공지능 엔지니어"
        if _has_phrase(primary_title, "forward deployed engineer") and _has_any_phrase(primary_title, ("ai", "llm", "machine learning", "foundation model")):
            return "인공지능 엔지니어"
        if _has_phrase(primary_title, "software engineer") and _has_any_phrase(primary_title, _AI_ENGINEER_SIGNAL_PHRASES):
            return "인공지능 엔지니어"
        if _has_phrase(primary_title, "platform engineer") and _has_any_phrase(primary_title, _AI_ENGINEER_SIGNAL_PHRASES):
            return "인공지능 엔지니어"
        if _has_phrase(primary_title, "director") and _has_any_phrase(primary_title, ("machine learning", "computer vision", "llm", "foundation model", "perception")):
            return "인공지능 엔지니어"
    if (
        _has_any_phrase(corpus, _RESEARCH_SIGNAL_PHRASES)
        and _has_any_phrase(corpus, _RESEARCHER_TITLE_PHRASES)
        and _count_phrase_hits(corpus, _RESEARCH_ACADEMIC_PHRASES) >= 1
        and not _has_any_phrase(title_corpus, _NON_TARGET_TITLE_PHRASES)
    ):
        return "인공지능 리서처"
    return ""


def _refresh_existing_job_role(row: dict[str, Any]) -> tuple[str, dict[str, str]]:
    analysis_fields = {
        "주요업무_분석용": normalize_whitespace(row.get("주요업무_분석용")),
        "자격요건_분석용": normalize_whitespace(row.get("자격요건_분석용")),
        "우대사항_분석용": normalize_whitespace(row.get("우대사항_분석용")),
        "핵심기술_분석용": normalize_whitespace(row.get("핵심기술_분석용")),
        "상세본문_분석용": normalize_whitespace(row.get("상세본문_분석용")),
    }
    if not any(analysis_fields.values()):
        analysis_fields = build_analysis_fields(row)
    refreshed_role = classify_job_role(
        row.get("job_title_raw"),
        row.get("공고제목_표시"),
        analysis_fields.get("주요업무_분석용", ""),
        analysis_fields.get("자격요건_분석용", ""),
        analysis_fields.get("우대사항_분석용", ""),
        analysis_fields.get("핵심기술_분석용", ""),
        analysis_fields.get("상세본문_분석용", ""),
    )
    return refreshed_role, analysis_fields


def _refresh_merged_job_rows(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame

    refreshed_rows: list[dict[str, Any]] = []
    for row in frame.fillna("").to_dict(orient="records"):
        refreshed_role, analysis_fields = _refresh_existing_job_role(row)
        if refreshed_role in ALLOWED_JOB_ROLES:
            row["job_role"] = refreshed_role
        else:
            continue

        display_fields = build_display_fields(row, analysis_fields=analysis_fields)
        row.update(analysis_fields)
        row.update(display_fields)
        refreshed_rows.append({column: row.get(column, "") for column in JOB_COLUMNS})

    return pd.DataFrame(refreshed_rows, columns=list(JOB_COLUMNS))


def _should_skip_general_service_role_salvage(
    title_raw: str | None,
    title_ko: str | None,
    *body_sections: str | None,
) -> bool:
    title_text = _normalize_role_text(" ".join(filter(None, (title_raw, title_ko))))
    if not title_text or not _has_any_phrase(title_text, _GENERAL_SERVICE_DELIVERY_TITLE_PHRASES):
        return False
    body_text = _normalize_role_text(" ".join(filter(None, body_sections)))
    if _is_simple_developer_title(title_text, body_text):
        return True
    if _has_any_phrase(title_text, _AI_ENGINEER_SIGNAL_PHRASES) or _has_any_phrase(title_text, _RESEARCH_SIGNAL_PHRASES):
        return False
    if not body_text:
        return False
    if _has_any_phrase(body_text, _STRONG_AI_WORK_PHRASES):
        return False
    return _has_any_phrase(body_text, _GENERAL_SERVICE_DELIVERY_BODY_PHRASES)


def normalize_experience_level(value: str | None) -> str:
    text = normalize_whitespace(value)
    lowered = text.lower()
    if not text:
        return ""
    if lowered in {"full-time", "full time", "part-time", "part time", "contract", "contractor", "regular", "permanent"}:
        return ""
    if "경력무관" in text or "경력 무관" in text or "관계없음" in text:
        return "경력무관"
    if "신입" in text or "new grad" in lowered or "entry" in lowered:
        return "신입"
    if "인턴" in text or "intern" in lowered:
        return "인턴"
    if "경력" in text or "experienced" in lowered or "career" in lowered:
        return "경력"
    return text


def canonicalize_company_name_for_jobs(value: str | None) -> str:
    text = normalize_whitespace(value)
    if not text:
        return ""
    text = _LEGAL_ENTITY_PREFIX_RE.sub("", text)
    text = _LEGAL_ENTITY_SUFFIX_RE.sub("", text)
    return normalize_whitespace(text)


def canonicalize_job_key(row: dict) -> str:
    job_url = normalize_whitespace(row.get("job_url"))
    if job_url:
        return stable_hash([job_url])
    return stable_hash(
        [
            canonicalize_company_name_for_jobs(row.get("company_name")),
            normalize_whitespace(row.get("source_url")),
            normalize_whitespace(row.get("job_title_raw")),
        ]
    )


def _section_value(raw_value: str | None, fallback: str | None = None) -> str:
    text = clean_html_text(raw_value)
    if text:
        return text
    return clean_html_text(fallback)


def _extract_location_text(raw_job: dict) -> str:
    candidates = [
        raw_job.get("location"),
        raw_job.get("location_raw"),
        raw_job.get("country"),
        raw_job.get("office"),
    ]
    values: list[str] = []
    for candidate in candidates:
        if isinstance(candidate, dict):
            value = candidate.get("name") or candidate.get("location")
        else:
            value = candidate
        normalized = normalize_whitespace(str(value or ""))
        if normalized:
            values.append(normalized)
    return " ".join(values)


def _is_korea_market_job(raw_job: dict) -> bool:
    location_text = _extract_location_text(raw_job).lower()
    if not location_text:
        return True
    return any(token in location_text for token in _KOREA_LOCATION_TOKENS)


def _description_contains_definitive_closed_marker(text: str | None) -> bool:
    normalized = normalize_whitespace(clean_html_text(text)).lower()
    if not normalized:
        return False
    for line in normalized.split("\n"):
        compact = normalize_whitespace(line)
        if not compact:
            continue
        if any(marker in compact for marker in _CLOSED_JOB_STATUS_MARKERS):
            if any(exclude in compact for exclude in _CLOSED_DESCRIPTION_EXCLUDE_PATTERNS):
                continue
            if len(compact) <= 80:
                return True
    return False


def _detect_job_active_signal(
    status: str | None = None,
    notice_status: str | None = None,
    listing_context: str | None = None,
    title: str | None = None,
    description_text: str | None = None,
) -> bool | None:
    status_like_values = [
        normalize_whitespace(clean_html_text(value)).lower()
        for value in (status, notice_status, listing_context, title)
        if normalize_whitespace(clean_html_text(value))
    ]
    description_value = normalize_whitespace(clean_html_text(description_text)).lower()
    normalized_values = status_like_values + ([description_value] if description_value else [])
    if not normalized_values:
        return None
    if any(marker in text for text in status_like_values for marker in _CLOSED_JOB_STATUS_MARKERS):
        return False
    if any(marker in text for text in normalized_values for marker in _OPEN_JOB_STATUS_MARKERS):
        return True
    if _description_contains_definitive_closed_marker(description_value):
        return False
    return None


def normalize_job_payload(
    raw_job: dict,
    source_row: dict,
    run_id: str,
    snapshot_date: str,
    collected_at: str,
    settings=None,
    paths=None,
    gemini_budget: GeminiBudget | None = None,
    refine_with_gemini: bool = True,
) -> tuple[dict, dict]:
    if not _is_korea_market_job(raw_job):
        return {}, {}

    description_html = raw_job.get("description_html") or raw_job.get("description") or ""
    sections = extract_sections_from_description(description_html)

    main_tasks = _section_value(raw_job.get("main_tasks") or raw_job.get("responsibilities"), sections.get("주요업무"))
    requirements = _section_value(raw_job.get("requirements"), sections.get("자격요건"))
    preferred = _section_value(raw_job.get("preferred"), sections.get("우대사항"))
    core_skills = _section_value(raw_job.get("core_skills") or raw_job.get("skills"), sections.get("핵심기술"))

    title_raw = normalize_whitespace(raw_job.get("title") or raw_job.get("job_title") or "")
    title_ko = normalize_whitespace(raw_job.get("title_ko") or "")
    experience_raw = normalize_experience_level(raw_job.get("experience_level") or raw_job.get("career_level"))
    description_text = clean_html_text(description_html)
    active_signal = _detect_job_active_signal(
        raw_job.get("status"),
        raw_job.get("notice_status"),
        raw_job.get("listing_context"),
        title_raw,
        description_text,
    )
    if active_signal is False:
        return {}, {}
    role = classify_job_role(
        title_raw,
        title_ko,
        main_tasks,
        requirements,
        preferred,
        core_skills,
        description_text,
    )
    if (
        role not in ALLOWED_JOB_ROLES
        and settings
        and paths
        and gemini_budget
        and not _should_skip_general_service_role_salvage(
            title_raw,
            title_ko,
            main_tasks,
            requirements,
            preferred,
            description_text,
        )
    ):
        role = maybe_salvage_job_role(
            {
                "title_raw": title_raw,
                "title_ko": title_ko,
                "main_tasks": main_tasks,
                "requirements": requirements,
                "preferred": preferred,
                "description_text": description_text,
                "source_type": normalize_whitespace(source_row.get("source_type")),
            },
            settings,
            paths,
            gemini_budget,
        )

    if role not in ALLOWED_JOB_ROLES:
        return {}, {}

    source_url = canonicalize_runtime_source_url(source_row.get("source_url"))
    source_type = normalize_whitespace(source_row.get("source_type"))
    company_name = normalize_whitespace(source_row.get("company_name"))
    company_name_hint = canonicalize_company_name_for_jobs(raw_job.get("company_name_hint"))
    if source_type in {"worknet_api", "work24_public_html", "saramin_api"} and company_name_hint:
        company_name = company_name_hint
    job_url = normalize_whitespace(raw_job.get("job_url") or "")
    if job_url:
        job_key = stable_hash([job_url])
    else:
        job_key = stable_hash(
            [
                company_name,
                source_url,
                title_raw,
            ]
        )
    change_hash = stable_hash([title_raw, description_html, requirements, preferred, core_skills, experience_raw])
    normalized = {
        "job_key": job_key,
        "change_hash": change_hash,
        "first_seen_at": collected_at,
        "last_seen_at": collected_at,
        "is_active": True,
        "missing_count": 0,
        "snapshot_date": snapshot_date,
        "run_id": run_id,
        "source_url": source_url,
        "source_bucket": source_row.get("source_bucket"),
        "source_name": source_row.get("source_name"),
        "company_name": company_name,
        "company_tier": source_row.get("company_tier"),
        "job_title_raw": title_raw,
        "experience_level_raw": experience_raw,
        "job_role": role,
        "job_url": job_url,
        "record_status": "신규",
        "job_title_ko": title_ko or title_raw,
        "experience_level_ko": raw_job.get("experience_level_ko") or experience_raw,
        "main_tasks": main_tasks,
        "requirements": requirements,
        "preferred": preferred,
        "core_skills": core_skills,
        "description_text": description_text,
        "description_html": description_html,
    }
    analysis_fields = build_analysis_fields(normalized)
    if refine_with_gemini and settings and paths and gemini_budget:
        analysis_fields = maybe_refine_analysis_fields(
            {
                "main_tasks": main_tasks,
                "requirements": requirements,
                "preferred": preferred,
                "core_skills": core_skills,
                "description_text": description_text,
            },
            analysis_fields,
            settings,
            paths,
            gemini_budget,
        )
    display_fields = build_display_fields(normalized, analysis_fields=analysis_fields)
    normalized.update(analysis_fields)
    normalized.update(display_fields)
    record = normalized.copy()
    raw_payload_for_storage = raw_job
    if source_type == "work24_public_html":
        raw_payload_for_storage = {
            key: raw_job.get(key, "")
            for key in (
                "title",
                "job_url",
                "location",
                "country",
                "experience_level",
                "company_name_hint",
                "listing_context",
                "worknet_wanted_auth_no",
                "work24_public_page",
                "work24_public_tracking_signal",
                "work24_llm_target_hint",
                "work24_llm_suggested_role",
                "work24_llm_reason",
                "work24_llm_tracking_error",
                "status",
                "source_compliance",
            )
            if normalize_whitespace(raw_job.get(key, ""))
        }
    raw_detail = {
        "run_id": run_id,
        "snapshot_date": snapshot_date,
        "job_key": job_key,
        "source_url": source_row.get("source_url"),
        "company_name": company_name,
        "job_title_raw": title_raw,
        "raw_payload_json": dump_json(raw_payload_for_storage),
    }
    raw_detail = {column: raw_detail.get(column, "") for column in RAW_DETAIL_COLUMNS}
    return record, raw_detail


def refresh_job_roles(frame: pd.DataFrame) -> pd.DataFrame:
    """Re-apply the current role taxonomy to previously collected job rows."""
    if frame.empty:
        return pd.DataFrame(columns=list(JOB_COLUMNS))

    refreshed_rows: list[dict[str, Any]] = []
    analysis_columns = (
        "주요업무_분석용",
        "자격요건_분석용",
        "우대사항_분석용",
        "핵심기술_분석용",
        "상세본문_분석용",
    )
    for row in frame.fillna("").to_dict(orient="records"):
        analysis_fields = {column: normalize_whitespace(row.get(column)) for column in analysis_columns}
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
        if role not in ALLOWED_JOB_ROLES:
            continue

        row["job_role"] = role
        row.update(analysis_fields)
        row.update(build_display_fields(row, analysis_fields=analysis_fields))
        refreshed_rows.append({column: row.get(column, "") for column in JOB_COLUMNS})

    return pd.DataFrame(refreshed_rows, columns=list(JOB_COLUMNS))


def _gemini_priority_score(job: dict) -> int:
    score = 0
    section_pairs = (
        ("main_tasks", "주요업무_분석용", 4),
        ("requirements", "자격요건_분석용", 4),
        ("preferred", "우대사항_분석용", 2),
        ("core_skills", "핵심기술_분석용", 2),
    )
    for raw_field, analysis_field, weight in section_pairs:
        if needs_gemini_refinement(job.get(raw_field), job.get(analysis_field)):
            score += weight
    if not normalize_whitespace(job.get("주요업무_분석용")) and normalize_whitespace(job.get("description_text")):
        score += 2
    return score


def _refine_jobs_with_gemini(jobs: list[dict], settings, paths) -> list[dict]:
    if not settings.enable_gemini_fallback or not _active_llm_api_key(settings) or not _active_llm_model(settings):
        return jobs

    budget = GeminiBudget(max_calls=settings.gemini_max_calls_per_run)
    prioritized = sorted(
        ((index, _gemini_priority_score(job)) for index, job in enumerate(jobs)),
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
        refined_analysis = maybe_refine_analysis_fields(
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
        if refined_analysis == current_analysis:
            continue
        job.update(refined_analysis)
        job.update(build_display_fields(job, analysis_fields=refined_analysis))
    return jobs


def _parse_json_jobs(content: str) -> list[dict]:
    payload = json.loads(content)
    if isinstance(payload, dict):
        if isinstance(payload.get("jobs"), list):
            return payload["jobs"]
        if payload.get("@type") == "JobPosting":
            return [payload]
    if isinstance(payload, list):
        return payload
    return []


def _extract_next_data_payload(content: str) -> dict:
    match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', content, flags=re.DOTALL)
    if not match:
        return {}
    try:
        payload = json.loads(match.group(1))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _extract_flex_recruiting_context(next_payload: dict, base_url: str) -> dict[str, str | bool]:
    props = next_payload.get("props") or {}
    page_props = props.get("pageProps") or {}
    recruiting_site = page_props.get("recruitingSiteResponse") or {}
    runtime_config = next_payload.get("runtimeConfig") or {}
    design = recruiting_site.get("design") or {}
    site_pages = design.get("siteDesignPages") or []
    customer_id_hash = normalize_whitespace(recruiting_site.get("customerIdHash"))
    subdomain = normalize_whitespace(recruiting_site.get("subdomain"))
    api_endpoint = normalize_whitespace(runtime_config.get("API_ENDPOINT")) or "https://flex.team"
    recruiting_site_root_domain = normalize_whitespace(runtime_config.get("RECRUITING_SITE_ROOT_DOMAIN")) or "careers.team"
    has_job_description_page = any(
        isinstance(page, dict) and normalize_whitespace(page.get("type")).upper() == "JOB_DESCRIPTION"
        for page in site_pages
    )

    public_base_url = ""
    if subdomain and recruiting_site_root_domain:
        public_base_url = f"https://{subdomain}.{recruiting_site_root_domain}"
    elif base_url:
        parts = urlsplit(base_url)
        if parts.scheme and parts.netloc:
            public_base_url = f"{parts.scheme}://{parts.netloc}"

    return {
        "customer_id_hash": customer_id_hash,
        "subdomain": subdomain,
        "api_endpoint": api_endpoint.rstrip("/"),
        "public_base_url": public_base_url.rstrip("/"),
        "has_job_description_page": has_job_description_page,
    }


def _find_first_block_by_type(node: Any, block_type: str) -> dict[str, Any]:
    if isinstance(node, dict):
        if normalize_whitespace(node.get("type")).lower() == block_type.lower():
            return node
        for value in node.values():
            match = _find_first_block_by_type(value, block_type)
            if match:
                return match
    elif isinstance(node, list):
        for value in node:
            match = _find_first_block_by_type(value, block_type)
            if match:
                return match
    return {}


def _extract_ninehire_recruiting_context(next_payload: dict, base_url: str) -> dict[str, Any]:
    props = next_payload.get("props") or {}
    page_props = props.get("pageProps") or {}
    homepage_props = page_props.get("homepageProps") or {}
    homepage = homepage_props.get("homepage") or {}
    info = homepage_props.get("info") or {}
    company_id = normalize_whitespace(info.get("companyId") or homepage.get("companyId"))
    job_posting_block = _find_first_block_by_type(homepage, "job_posting")
    public_base_url = ""
    if base_url:
        parts = urlsplit(base_url)
        if parts.scheme and parts.netloc:
            public_base_url = f"{parts.scheme}://{parts.netloc}"
    return {
        "company_id": company_id,
        "public_base_url": public_base_url.rstrip("/"),
        "job_posting_block": job_posting_block,
        "count_per_page": int(job_posting_block.get("countPerPage") or 25) if job_posting_block else 25,
        "order": normalize_whitespace(job_posting_block.get("orderBy") or "created_at_desc"),
        "fixed_recruitment_ids": job_posting_block.get("fixedRecruitmentIds") or [],
    }


def _fetch_flex_public_json(url: str, settings) -> dict[str, Any]:
    timeout_seconds, connect_timeout_seconds = _source_timeout_values(settings, "html_page")
    with httpx.Client(
        timeout=build_timeout(timeout_seconds, connect_timeout_seconds),
        follow_redirects=True,
        headers={"User-Agent": settings.user_agent},
    ) as client:
        response = client.get(url)
        response.raise_for_status()
        payload = response.json()
    return payload if isinstance(payload, dict) else {}


def _fetch_ninehire_public_json(url: str, settings, *, headers: dict[str, str] | None = None) -> dict[str, Any]:
    timeout_seconds, connect_timeout_seconds = _source_timeout_values(settings, "html_page")
    request_headers = {"User-Agent": settings.user_agent}
    if headers:
        request_headers.update(headers)
    with httpx.Client(
        timeout=build_timeout(timeout_seconds, connect_timeout_seconds),
        follow_redirects=True,
        headers=request_headers,
    ) as client:
        response = client.get(url)
        response.raise_for_status()
        payload = response.json()
    return payload if isinstance(payload, dict) else {}


def _format_ninehire_experience(recruitment: dict[str, Any]) -> str:
    career = recruitment.get("career") or {}
    career_type = normalize_whitespace(career.get("type"))
    career_range = career.get("range") or {}
    over = career_range.get("over")
    below = career_range.get("below")
    if career_type == "irrelevant":
        return "경력 무관"
    if career_type == "newcomer":
        return "신입"
    if over is not None and below is not None:
        try:
            return f"경력 {int(over)}~{int(below)}년"
        except Exception:  # noqa: BLE001
            return "경력"
    if over is not None:
        try:
            return f"경력 {int(over)}년 이상"
        except Exception:  # noqa: BLE001
            return "경력"
    if below is not None:
        try:
            return f"경력 {int(below)}년 이하"
        except Exception:  # noqa: BLE001
            return "경력"
    if career_type == "experienced":
        return "경력"
    return ""


def _render_flex_lexical_nodes(nodes: list[dict[str, Any]] | None) -> str:
    if not isinstance(nodes, list):
        return ""
    rendered: list[str] = []
    for node in nodes:
        if not isinstance(node, dict):
            continue
        rendered_node = _render_flex_lexical_node(node)
        if rendered_node:
            rendered.append(rendered_node)
    return "".join(rendered)


def _render_flex_lexical_node(node: dict[str, Any]) -> str:
    node_type = normalize_whitespace(node.get("type")).lower()
    children_html = _render_flex_lexical_nodes(node.get("children"))

    if node_type == "root":
        return children_html
    if node_type == "text":
        return html.escape(str(node.get("text") or ""))
    if node_type == "linebreak":
        return "<br/>"
    if node_type == "paragraph":
        return f"<p>{children_html}</p>"
    if node_type == "heading":
        tag = normalize_whitespace(node.get("tag")).lower() or "h3"
        if tag not in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            tag = "h3"
        return f"<{tag}>{children_html}</{tag}>"
    if node_type == "list":
        tag = normalize_whitespace(node.get("tag")).lower()
        if tag not in {"ul", "ol"}:
            tag = "ol" if normalize_whitespace(node.get("listType")).lower() in {"number", "ordered"} else "ul"
        return f"<{tag}>{children_html}</{tag}>"
    if node_type == "listitem":
        return f"<li>{children_html}</li>"
    if node_type == "quote":
        return f"<blockquote>{children_html}</blockquote>"
    if node_type == "link":
        href = normalize_whitespace(node.get("url"))
        if href:
            return f'<a href="{html.escape(href, quote=True)}">{children_html}</a>'
        return children_html
    return children_html


def _flex_content_to_html(content_payload: dict[str, Any]) -> str:
    if not isinstance(content_payload, dict):
        return ""
    schema = normalize_whitespace(content_payload.get("schema")).upper()
    raw_data = content_payload.get("data")
    if schema == "LEXICAL_V1" and raw_data:
        try:
            parsed = json.loads(raw_data if isinstance(raw_data, str) else json.dumps(raw_data, ensure_ascii=False))
        except json.JSONDecodeError:
            return ""
        root = parsed.get("root") if isinstance(parsed, dict) else {}
        return _render_flex_lexical_node(root) if isinstance(root, dict) else ""
    return normalize_whitespace(str(raw_data or ""))


def _extract_flex_job_description_jobs(content: str, base_url: str, *, settings=None) -> list[dict[str, Any]]:
    if not settings or not base_url:
        return []
    next_payload = _extract_next_data_payload(content)
    if not next_payload:
        return []
    context = _extract_flex_recruiting_context(next_payload, base_url)
    customer_id_hash = normalize_whitespace(context.get("customer_id_hash"))
    api_endpoint = normalize_whitespace(context.get("api_endpoint"))
    public_base_url = normalize_whitespace(context.get("public_base_url"))
    has_job_description_page = bool(context.get("has_job_description_page"))
    if not customer_id_hash or not api_endpoint or not public_base_url or not has_job_description_page:
        return []

    try:
        listing_payload = _fetch_flex_public_json(
            f"{api_endpoint}/api-public/v2/recruiting/customers/{customer_id_hash}/sites/job-descriptions",
            settings,
        )
    except Exception:  # noqa: BLE001
        return []

    listing_items = listing_payload.get("jobDescriptions")
    if not isinstance(listing_items, list):
        return []

    jobs: list[dict[str, Any]] = []
    for item in listing_items[:40]:
        if not isinstance(item, dict):
            continue
        job_id_hash = normalize_whitespace(item.get("jobDescriptionIdHash"))
        title = normalize_whitespace(item.get("title"))
        if not job_id_hash or not title:
            continue
        try:
            detail_payload = _fetch_flex_public_json(
                f"{api_endpoint}/api-public/v2/recruiting/customers/{customer_id_hash}/job-descriptions/{job_id_hash}/details",
                settings,
            )
        except Exception:  # noqa: BLE001
            detail_payload = {}
        description_html = _flex_content_to_html(detail_payload.get("content") or {})
        jobs.append(
            {
                "title": normalize_whitespace(detail_payload.get("title")) or title,
                "title_ko": "",
                "description_html": description_html,
                "job_url": f"{public_base_url}/job-descriptions/{job_id_hash}",
                "experience_level": "",
                "location": "",
                "country": "",
                "core_skills": "",
                "requirements": "",
                "preferred": "",
            }
        )
    return jobs


def _extract_next_rsc_chunks(content: str) -> list[str]:
    soup = BeautifulSoup(content, "lxml")
    chunks: list[str] = []
    for script in soup.find_all("script"):
        script_text = script.string or script.get_text()
        if "self.__next_f.push" not in script_text:
            continue
        match = _NEXT_RSC_PUSH_RE.search(script_text)
        if not match:
            continue
        try:
            payload = json.loads(match.group(1))
        except json.JSONDecodeError:
            continue
        if isinstance(payload, str):
            chunks.append(payload)
    return chunks


def _extract_json_array_after_key(text: str, key: str) -> list[dict[str, Any]]:
    key_index = text.find(key)
    if key_index < 0:
        return []
    fragment = text[key_index + len(key) :]
    decoder = json.JSONDecoder()
    try:
        parsed, _ = decoder.raw_decode(fragment)
    except json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


def _extract_next_rsc_content_map(chunks: list[str]) -> dict[str, str]:
    content_map: dict[str, str] = {}
    pending_ref = ""
    for chunk in chunks:
        marker_match = _NEXT_RSC_CONTENT_MARKER_RE.match(chunk)
        if marker_match:
            pending_ref = marker_match.group(1)
            continue
        if pending_ref:
            content_map[pending_ref] = chunk
            pending_ref = ""
    return content_map


def _looks_like_generic_family_posting(title: str | None) -> bool:
    normalized = normalize_whitespace(title)
    if not normalized:
        return False
    return any(hint in normalized for hint in _GENERIC_FAMILY_POSTING_HINTS)


def _infer_family_table_row_role(label: str, body_text: str) -> str:
    normalized_label = label or ""
    if _has_any_phrase(normalized_label, ("artificial intelligence", "인공지능", "ai")):
        if _has_any_phrase(body_text, ("reinforcement learning", "simulation", "diagnosis", "research")):
            return "인공지능 리서처"
        return "인공지능 엔지니어"
    if _has_any_phrase(normalized_label, ("data", "데이터")):
        return "데이터 사이언티스트"
    return ""


def _build_ninehire_jobs(
    recruitments: list[dict[str, Any]],
    *,
    public_base_url: str,
    detail_fetcher: Callable[[str], dict[str, Any]],
) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    for recruitment in recruitments:
        address_key = normalize_whitespace(recruitment.get("addressKey"))
        recruitment_id = normalize_whitespace(recruitment.get("recruitmentId"))
        if not address_key or not recruitment_id:
            continue
        job_url = urljoin(f"{public_base_url}/", f"job_posting/{address_key}")
        if not job_url or job_url in seen_urls:
            continue
        seen_urls.add(job_url)
        detail = detail_fetcher(recruitment_id)
        description_html = normalize_whitespace(detail.get("content") or detail.get("content_english"))
        locations = recruitment.get("jobLocations") or []
        location = ""
        if locations and isinstance(locations[0], dict):
            location = normalize_whitespace(locations[0].get("addressName") or locations[0].get("addressName_english"))
        title = normalize_whitespace(recruitment.get("externalTitle") or recruitment.get("title"))
        jobs.append(
            {
                "title": title,
                "description_html": description_html,
                "job_url": job_url,
                "experience_level": _format_ninehire_experience(recruitment),
                "location": location,
                "country": location,
            }
        )
    return jobs


def _role_hint_title(role: str) -> str:
    return {
        "인공지능 리서처": "AI Researcher",
        "인공지능 엔지니어": "AI Engineer",
        "데이터 사이언티스트": "Data Scientist",
        "데이터 분석가": "Data Analyst",
    }.get(role, role)


def _split_generic_family_posting(job: dict[str, Any]) -> list[dict[str, Any]]:
    title = normalize_whitespace(job.get("title"))
    description_html = job.get("description_html") or ""
    if not _looks_like_generic_family_posting(title) or not description_html:
        return [job]

    soup = BeautifulSoup(description_html, "lxml")
    table = soup.find("table")
    if not table:
        return [job]

    sections = extract_sections_from_description(description_html)
    requirements = _section_value(job.get("requirements"), sections.get("자격요건"))
    preferred = _section_value(job.get("preferred"), sections.get("우대사항"))

    def _wrap_section_html(section_title: str, section_text: str) -> str:
        normalized = normalize_whitespace(section_text)
        if not normalized:
            return ""
        paragraphs = "".join(
            f"<p>{html.escape(line)}</p>"
            for line in normalized.splitlines()
            if normalize_whitespace(line)
        )
        if not paragraphs:
            return ""
        return f"<h3>{html.escape(section_title)}</h3>{paragraphs}"

    split_jobs: list[dict[str, Any]] = []
    seen_roles: set[str] = set()
    for row in table.select("tr"):
        cells = row.find_all(["th", "td"])
        if len(cells) < 2:
            continue
        label = normalize_whitespace(clean_html_text(str(cells[0])))
        body_html = str(cells[1])
        body_text = clean_html_text(body_html)
        role = _infer_family_table_row_role(label, body_text)
        if role not in ALLOWED_JOB_ROLES or role in seen_roles:
            continue
        seen_roles.add(role)
        slug = re.sub(r"[^0-9a-z가-힣]+", "-", normalize_whitespace(label).lower()).strip("-") or role.lower()
        job_url = normalize_whitespace(job.get("job_url"))
        split_description_html = (
            f"<h3>{html.escape(label)}</h3>"
            f"<h3>주요 업무</h3>{body_html}"
            f"{_wrap_section_html('지원 자격', requirements)}"
            f"{_wrap_section_html('우대 사항', preferred)}"
        )
        split_jobs.append(
            {
                **job,
                "title": f"{title} - {_role_hint_title(role)} ({label})",
                "title_ko": label,
                "job_url": f"{job_url}#{slug}" if job_url else job_url,
                "main_tasks": body_text,
                "requirements": requirements,
                "preferred": preferred,
                "description_html": split_description_html,
            }
        )
    return split_jobs or [job]


def _extract_next_rsc_notice_jobs(content: str, base_url: str) -> list[dict[str, Any]]:
    if not base_url or "/recruit/notice" not in normalize_whitespace(urlsplit(base_url).path):
        return []
    chunks = _extract_next_rsc_chunks(content)
    if not chunks:
        return []
    list_items: list[dict[str, Any]] = []
    for chunk in chunks:
        list_items = _extract_json_array_after_key(chunk, '"list":')
        if list_items:
            break
    if not list_items:
        return []

    content_map = _extract_next_rsc_content_map(chunks)
    jobs: list[dict[str, Any]] = []
    for item in list_items:
        if not isinstance(item, dict):
            continue
        job_id = item.get("id")
        title = normalize_whitespace(item.get("title"))
        if not job_id or not title:
            continue
        content_ref = normalize_whitespace(str(item.get("content") or "")).lstrip("$")
        description_html = content_map.get(content_ref, "")
        base_job = {
            "title": normalize_whitespace(
                f"{normalize_whitespace(item.get('department'))} | {normalize_whitespace(item.get('business_place'))} {title}"
            ),
            "title_ko": "",
            "description_html": description_html,
            "job_url": f"{base_url.rstrip('/')}/{job_id}",
            "experience_level": "",
            "location": normalize_whitespace(item.get("business_place")),
            "country": normalize_whitespace(item.get("business_place")),
            "core_skills": "",
            "requirements": "",
            "preferred": "",
        }
        jobs.extend(_split_generic_family_posting(base_job))
    return jobs


def _extract_dehydrated_queries(next_payload: dict) -> list[dict]:
    props = next_payload.get("props") or {}
    page_props = props.get("pageProps") or {}
    dehydrated_state = page_props.get("dehydratedState") or props.get("dehydratedState") or {}
    queries = dehydrated_state.get("queries")
    return queries if isinstance(queries, list) else []


def _find_next_query_data(content: str, key_fragment: str) -> dict | list | None:
    payload = _extract_next_data_payload(content)
    for query in _extract_dehydrated_queries(payload):
        query_key = query.get("queryKey")
        if not query_key:
            continue
        if key_fragment not in json.dumps(query_key, ensure_ascii=False).lower():
            continue
        return query.get("state", {}).get("data")
    return None


def _source_root_url(url: str) -> str:
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, "", "", ""))


def _infer_source_type_from_url(url: str) -> str:
    parts = urlsplit(normalize_whitespace(url))
    host = parts.netloc.lower()
    if "oapi.saramin.co.kr" in host:
        return "saramin_api"
    if "work24.go.kr" in host and "/cm/openapi/call/wk/" in parts.path.lower():
        return "worknet_api"
    if "work24.go.kr" in host and "/wk/a/b/1200/retrivedtlempsrchlist" in parts.path.lower():
        return "work24_public_html"
    if "career.greetinghr.com" in host:
        return "greetinghr"
    if "recruiter.co.kr" in host:
        return "recruiter"
    if "greenhouse.io" in host:
        return "greenhouse"
    if "lever.co" in host:
        return "lever"
    return "html_page"


def _format_greetinghr_experience(position: dict) -> str:
    career = position.get("jobPositionCareer") or {}
    career_type = normalize_whitespace(career.get("careerType"))
    career_from = career.get("careerFrom")
    career_to = career.get("careerTo")
    if career_type == "NOT_MATTER":
        return "경력 무관"
    if career_from and career_to:
        return f"경력 {career_from}~{career_to}년"
    if career_from:
        return f"경력 {career_from}년 이상"
    if career_to:
        return f"경력 {career_to}년 이하"
    if career_type == "EXPERIENCED":
        return "경력"
    return ""


def _greetinghr_primary_position(opening: dict) -> dict:
    opening_job_position = opening.get("openingJobPosition") or {}
    positions = opening_job_position.get("openingJobPositions") or []
    return positions[0] if positions else {}


def _parse_greetinghr_detail_html(
    content: str,
    *,
    fallback_title: str,
    fallback_url: str,
    fallback_location: str,
    fallback_experience: str,
) -> dict:
    data = _find_next_query_data(content, "getopeningbyid")
    if not isinstance(data, dict):
        return {
            "title": fallback_title,
            "description_html": "",
            "job_url": fallback_url,
            "experience_level": fallback_experience,
            "location": fallback_location,
            "country": fallback_location,
        }
    opening_data = data.get("data") or {}
    openings_info = opening_data.get("openingsInfo") or {}
    title = normalize_whitespace(openings_info.get("title") or fallback_title)
    description_html = html.unescape(str(openings_info.get("detail") or ""))
    return {
        "title": title,
        "description_html": description_html,
        "job_url": fallback_url,
        "experience_level": fallback_experience,
        "location": fallback_location,
        "country": fallback_location,
    }


def _build_greetinghr_jobs_from_html(
    content: str,
    source_url: str,
    detail_fetcher: Callable[[str], str],
) -> list[dict]:
    openings_data = _find_next_query_data(content, "\"openings\"")
    openings = openings_data if isinstance(openings_data, list) else []
    jobs: list[dict] = []
    root_url = _source_root_url(source_url)
    seen_detail_urls: set[str] = set()
    for opening in openings:
        if not opening.get("deploy", True):
            continue
        opening_id = opening.get("openingId")
        if not opening_id:
            continue
        title = normalize_whitespace(opening.get("title"))
        position = _greetinghr_primary_position(opening)
        location = normalize_whitespace((position.get("workspacePlace") or {}).get("place"))
        experience = _format_greetinghr_experience(position)
        detail_url = f"{root_url}/ko/o/{opening_id}"
        if detail_url in seen_detail_urls:
            continue
        seen_detail_urls.add(detail_url)
        try:
            detail_html = detail_fetcher(detail_url)
        except Exception:  # noqa: BLE001
            detail_html = ""
        jobs.append(
            _parse_greetinghr_detail_html(
                detail_html,
                fallback_title=title,
                fallback_url=detail_url,
                fallback_location=location,
                fallback_experience=experience,
            )
        )
    if jobs:
        return jobs

    soup = BeautifulSoup(content, "lxml")
    for anchor in soup.select("a[href]"):
        href = normalize_whitespace(anchor.get("href"))
        if not href:
            continue
        resolved_url = urljoin(source_url, href)
        if urlsplit(resolved_url).netloc.lower() != urlsplit(root_url).netloc.lower():
            continue
        match = _GREETINGHR_DETAIL_PATH_RE.search(urlsplit(resolved_url).path)
        if not match:
            continue
        detail_url = f"{root_url}/ko/o/{match.group(1)}"
        if detail_url in seen_detail_urls:
            continue
        seen_detail_urls.add(detail_url)
        title = normalize_whitespace(anchor.get_text(" ", strip=True))
        try:
            detail_html = detail_fetcher(detail_url)
        except Exception:  # noqa: BLE001
            detail_html = ""
        jobs.append(
            _parse_greetinghr_detail_html(
                detail_html,
                fallback_title=title,
                fallback_url=detail_url,
                fallback_location="",
                fallback_experience="",
            )
        )
    return jobs


def _extract_greetinghr_candidate_page_urls(content: str, source_url: str) -> list[str]:
    soup = BeautifulSoup(content, "lxml")
    source_parts = urlsplit(source_url)
    source_host = source_parts.netloc.lower()
    candidates: list[tuple[int, str]] = []
    seen: set[str] = set()
    for anchor in soup.select("a[href]"):
        href = normalize_whitespace(anchor.get("href"))
        if not href:
            continue
        resolved_url = urljoin(source_url, href)
        parts = urlsplit(resolved_url)
        if parts.netloc.lower() != source_host:
            continue
        path = normalize_whitespace(parts.path.lower())
        if not path or _GREETINGHR_DETAIL_PATH_RE.search(path):
            continue
        if any(hint in path for hint in _GREETINGHR_PAGE_PATH_EXCLUDE_HINTS):
            continue
        if not any(hint in path for hint in _GREETINGHR_PAGE_PATH_HINTS):
            continue
        if resolved_url in seen:
            continue
        seen.add(resolved_url)
        priority = 1
        if "recruit" in path or "/job" in path or "opening" in path:
            priority = 0
        candidates.append((priority, resolved_url))
    candidates.sort()
    return [url for _, url in candidates]


def _extract_greetinghr_jobs_with_gemini_router(
    content: str,
    source_url: str,
    detail_fetcher: Callable[[str], str],
    *,
    settings=None,
    paths=None,
    budget: GeminiBudget | None = None,
) -> list[dict]:
    gemini_jobs = _extract_html_jobs_with_gemini(
        content,
        source_url,
        settings=settings,
        paths=paths,
        budget=budget,
    )
    if not gemini_jobs:
        return []

    root_url = _source_root_url(source_url)
    root_host = urlsplit(root_url).netloc.lower()
    seen_detail_urls: set[str] = set()
    candidate_page_urls: list[str] = []
    seen_candidate_urls: set[str] = set()
    jobs: list[dict] = []

    for item in gemini_jobs:
        resolved_url = urljoin(source_url, normalize_whitespace(item.get("job_url")))
        if not resolved_url:
            continue
        parts = urlsplit(resolved_url)
        if parts.netloc.lower() != root_host:
            continue
        match = _GREETINGHR_DETAIL_PATH_RE.search(parts.path)
        if match:
            detail_url = f"{root_url}/ko/o/{match.group(1)}"
            if detail_url in seen_detail_urls:
                continue
            seen_detail_urls.add(detail_url)
            title = normalize_whitespace(item.get("title"))
            try:
                detail_html = detail_fetcher(detail_url)
            except Exception:  # noqa: BLE001
                detail_html = ""
            jobs.append(
                _parse_greetinghr_detail_html(
                    detail_html,
                    fallback_title=title,
                    fallback_url=detail_url,
                    fallback_location=normalize_whitespace(item.get("location")),
                    fallback_experience=normalize_whitespace(item.get("experience_level")),
                )
            )
            continue

        if resolved_url in seen_candidate_urls:
            continue
        seen_candidate_urls.add(resolved_url)
        candidate_page_urls.append(resolved_url)

    if jobs:
        return jobs

    for candidate_url in candidate_page_urls[:3]:
        try:
            candidate_html = detail_fetcher(candidate_url)
        except Exception:  # noqa: BLE001
            continue
        jobs = _build_greetinghr_jobs_from_html(candidate_html, candidate_url, detail_fetcher)
        if jobs:
            return jobs
    return []


def _recruiter_page_text_hints(soup: BeautifulSoup) -> str:
    lines: list[str] = []
    for row in soup.select("table tbody tr"):
        th = normalize_whitespace(row.select_one("th").get_text(" ", strip=True) if row.select_one("th") else "")
        td_node = row.select_one("td")
        td = normalize_whitespace(td_node.get_text(" ", strip=True) if td_node else "")
        if not th:
            continue
        if any(token in th for token in ("공고명", "접수기간", "근무지", "근무지역", "근무장소")) and td:
            lines.append(f"{th}: {td}")
            continue
        if "첨부파일" in th and td_node:
            attachment_names = [
                normalize_whitespace(anchor.get_text(" ", strip=True))
                for anchor in td_node.select("a[href]")
                if normalize_whitespace(anchor.get_text(" ", strip=True))
            ]
            lines.extend(attachment_names[:3])
    return "\n".join(lines)


def _recruiter_asset_urls(description_html: str, soup: BeautifulSoup, detail_url: str) -> list[str]:
    asset_urls: list[str] = []
    seen: set[str] = set()

    for anchor in soup.select("a.fileWrapperView[href]"):
        href = normalize_whitespace(anchor.get("href"))
        name = normalize_whitespace(anchor.get_text(" ", strip=True)).lower()
        if not href:
            continue
        if not any(ext in f"{href.lower()} {name}" for ext in _RECRUITER_ATTACHMENT_EXTENSIONS):
            continue
        asset_url = urljoin(detail_url, href)
        if asset_url in seen:
            continue
        seen.add(asset_url)
        asset_urls.append(asset_url)

    description_soup = BeautifulSoup(description_html, "lxml")
    for image in description_soup.select("img[src]"):
        src = normalize_whitespace(image.get("src"))
        if not src or not any(ext in src.lower() for ext in _RECRUITER_ATTACHMENT_EXTENSIONS[1:]):
            continue
        asset_url = urljoin(detail_url, src)
        if asset_url in seen:
            continue
        seen.add(asset_url)
        asset_urls.append(asset_url)
    return asset_urls


def _recruiter_detail_needs_recovery(title: str, description_html: str, soup: BeautifulSoup) -> bool:
    if classify_job_role(title) not in ALLOWED_JOB_ROLES:
        return False
    has_attachment = bool(soup.select("a.fileWrapperView[href]"))
    description_text = clean_html_text(description_html)
    has_image_only_body = bool(BeautifulSoup(description_html, "lxml").select("img[src]")) and not section_output_is_substantive(description_text)
    return has_image_only_body or (has_attachment and not section_output_is_substantive(description_text))


def _recover_recruiter_detail_text(
    description_html: str,
    soup: BeautifulSoup,
    detail_url: str,
    title: str,
    paths,
    settings,
    *,
    enable_ocr_recovery: bool,
) -> str:
    if (
        paths is None
        or settings is None
        or not enable_ocr_recovery
        or bool(getattr(settings, "use_mock_sources", False))
        or not _recruiter_detail_needs_recovery(title, description_html, soup)
    ):
        return ""
    hint_text = _recruiter_page_text_hints(soup)
    asset_text = extract_text_from_asset_urls(
        _recruiter_asset_urls(description_html, soup, detail_url),
        user_agent=getattr(settings, "user_agent", "jobs-market-v2/0.1"),
        timeout_seconds=float(getattr(settings, "ats_source_timeout_seconds", 10.0)),
        connect_timeout_seconds=float(getattr(settings, "ats_source_connect_timeout_seconds", 3.0)),
    )
    parts: list[str] = []
    seen: set[str] = set()
    for part in (hint_text, asset_text):
        normalized = normalize_whitespace(part)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        parts.append(part)
    return "\n".join(parts)


_RECRUITER_RECOVERY_TERMINATION_PATTERNS = (
    re.compile(r"지원\s*시\s*참고사항"),
    re.compile(r"채용 관련 문의"),
    re.compile(r"문의 부탁"),
    re.compile(r"수탁연구사업\s*종료일"),
)
_RECRUITER_RECOVERY_DROP_PATTERNS = (
    re.compile(r"채용인원"),
    re.compile(r"접수기간"),
    re.compile(r"근무지"),
    re.compile(r"^\d+\s*제\d+차.*채용공고"),
)


def _sanitize_recruiter_recovered_text(recovered_text: str) -> str:
    if not recovered_text:
        return ""

    sanitized_lines: list[str] = []
    seen: set[str] = set()
    for raw_line in re.split(r"[\r\n]+|•", recovered_text):
        line = normalize_whitespace(raw_line)
        if not line:
            continue
        if any(pattern.search(line) for pattern in _RECRUITER_RECOVERY_TERMINATION_PATTERNS):
            break
        if any(pattern.search(line) for pattern in _RECRUITER_RECOVERY_DROP_PATTERNS):
            continue
        if line in seen:
            continue
        seen.add(line)
        sanitized_lines.append(line)
    return "\n".join(sanitized_lines)


def _append_recovered_text_to_description(description_html: str, recovered_text: str) -> str:
    if not recovered_text:
        return description_html
    escaped_lines = [
        html.escape(line)
        for line in _sanitize_recruiter_recovered_text(recovered_text).splitlines()
        if normalize_whitespace(line)
    ]
    if not escaped_lines:
        return description_html
    recovered_block = "<div>" + "<br>".join(escaped_lines) + "</div>"
    return "\n".join(part for part in (description_html, recovered_block) if part)


def _parse_recruiter_detail_html(
    content: str,
    *,
    fallback_title: str,
    detail_url: str,
    paths=None,
    settings=None,
    enable_ocr_recovery: bool = False,
) -> dict:
    soup = BeautifulSoup(content, "lxml")
    title_node = soup.select_one(".view-bbs-title")
    title = normalize_whitespace(title_node.get_text(" ", strip=True) if title_node else fallback_title)
    textarea = soup.select_one("textarea#jobnoticeContents")
    description_html = html.unescape(textarea.get_text() if textarea else "")
    location = ""
    for row in soup.select("table tbody tr"):
        th = normalize_whitespace(row.select_one("th").get_text(" ", strip=True) if row.select_one("th") else "")
        td = normalize_whitespace(row.select_one("td").get_text(" ", strip=True) if row.select_one("td") else "")
        if not th or not td:
            continue
        if any(token in th for token in ("근무지", "근무지역", "근무장소")):
            location = td
            break
    recovered_text = _recover_recruiter_detail_text(
        description_html,
        soup,
        detail_url,
        title,
        paths,
        settings,
        enable_ocr_recovery=enable_ocr_recovery,
    )
    return {
        "title": title,
        "description_html": _append_recovered_text_to_description(description_html, recovered_text),
        "job_url": detail_url,
        "experience_level": "",
        "location": location,
        "country": location,
    }


def _extract_recruiter_list_items(payload: dict) -> list[dict]:
    for key in ("list", "jobNoticeList", "data"):
        value = payload.get(key)
        if isinstance(value, list):
            return value
    return []


def _build_recruiter_jobs_from_payload(
    payload: dict,
    source_url: str,
    detail_fetcher: Callable[[str], str],
    *,
    paths=None,
    settings=None,
    enable_ocr_recovery: bool = False,
) -> list[dict]:
    jobs: list[dict] = []
    root_url = _source_root_url(source_url)
    for item in _extract_recruiter_list_items(payload):
        title = normalize_whitespace(item.get("jobnoticeName") or item.get("jobnoticeTitle") or item.get("title"))
        jobnotice_sn = item.get("jobnoticeSn")
        system_kind_code = item.get("systemKindCode") or "MRS2"
        if not title or not jobnotice_sn:
            continue
        detail_url = f"{root_url}/app/jobnotice/view?systemKindCode={system_kind_code}&jobnoticeSn={jobnotice_sn}"
        try:
            detail_html = detail_fetcher(detail_url)
        except Exception:  # noqa: BLE001
            detail_html = ""
        jobs.append(
            _parse_recruiter_detail_html(
                detail_html,
                fallback_title=title,
                detail_url=detail_url,
                paths=paths,
                settings=settings,
                enable_ocr_recovery=enable_ocr_recovery,
            )
        )
    return jobs


def _recruiter_notice_is_open(item: dict, now_ms: int) -> bool:
    if str(item.get("recruitEndYn") or "").upper() == "Y":
        return False
    receipt_state = normalize_whitespace(
        str(item.get("receiptState") or item.get("receiptStateName") or "")
    ).lower()
    if receipt_state and any(token in receipt_state for token in ("접수마감", "마감", "종료", "closed", "close")):
        return False
    try:
        deadline_count = item.get("deadlineCount")
        if deadline_count is not None and int(deadline_count) < 0:
            return False
    except (TypeError, ValueError):
        pass
    start = ((item.get("applyStartDate") or {}).get("time"))
    end = ((item.get("applyEndDate") or {}).get("time"))
    try:
        if start is not None and int(start) > now_ms:
            return False
        if end is not None and int(end) < now_ms:
            return False
    except (TypeError, ValueError):
        pass
    return True


def _metadata_value(metadata: list[dict] | None, key_fragment: str) -> str:
    if not metadata:
        return ""
    for item in metadata:
        name = normalize_whitespace(str(item.get("name") or "")).lower()
        if key_fragment not in name:
            continue
        value = item.get("value")
        if isinstance(value, list):
            return normalize_whitespace(" ".join(str(entry) for entry in value if entry))
        return normalize_whitespace(str(value or ""))
    return ""


def _parse_greenhouse_jobs(content: str) -> list[dict]:
    payload = json.loads(content)
    jobs = payload.get("jobs", []) if isinstance(payload, dict) else payload
    parsed_jobs: list[dict] = []
    for job in jobs or []:
        description_html = html.unescape(job.get("content") or "")
        parsed_jobs.append(
            {
                "title": job.get("title"),
                "description_html": description_html,
                "job_url": job.get("absolute_url"),
                "experience_level": _metadata_value(job.get("metadata"), "level"),
                "location": job.get("location"),
                "country": job.get("location", {}).get("name") if isinstance(job.get("location"), dict) else "",
            }
        )
    return parsed_jobs


def _build_lever_description(job: dict) -> str:
    parts = [job.get("opening"), job.get("description"), job.get("additional")]
    for section in job.get("lists") or []:
        heading = normalize_whitespace(str(section.get("text") or section.get("title") or ""))
        values = section.get("content") or section.get("body") or []
        body = "\n".join(normalize_whitespace(str(value)) for value in values if normalize_whitespace(str(value)))
        if heading or body:
            parts.append(f"<h2>{heading}</h2><div>{body}</div>")
    return "".join(str(part or "") for part in parts if part)


def _parse_lever_jobs(content: str) -> list[dict]:
    payload = json.loads(content)
    parsed_jobs: list[dict] = []
    for job in payload if isinstance(payload, list) else []:
        categories = job.get("categories") or {}
        parsed_jobs.append(
            {
                "title": job.get("text"),
                "description_html": _build_lever_description(job),
                "job_url": job.get("hostedUrl") or job.get("applyUrl"),
                "experience_level": categories.get("commitment"),
                "location": categories.get("location"),
                "country": job.get("country"),
            }
        )
    return parsed_jobs


def _extract_jsonld_jobs_from_html(content: str) -> list[dict]:
    soup = BeautifulSoup(content, "lxml")
    jobs: list[dict] = []
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            payload = json.loads(script.string or script.get_text())
        except json.JSONDecodeError:
            continue
        payloads = payload if isinstance(payload, list) else [payload]
        for item in payloads:
            if isinstance(item, dict) and item.get("@type") == "JobPosting":
                jobs.append(
                    {
                        "title": item.get("title"),
                        "description_html": item.get("description"),
                        "job_url": item.get("url"),
                        "requirements": item.get("qualifications"),
                        "responsibilities": item.get("responsibilities"),
                        "experience_level": item.get("experienceRequirements"),
                    }
                )
    return jobs


def _build_html_listing_jobs_from_anchors(content: str, base_url: str) -> list[dict]:
    soup = BeautifulSoup(content, "lxml")
    jobs: list[dict] = []
    seen_urls: set[str] = set()
    detail_urls: set[str] = set()
    base_parts = urlsplit(base_url)
    base_host = base_parts.netloc.lower()
    base_path = normalize_whitespace(base_parts.path.rstrip("/"))
    base_path_lower = base_path.lower()
    base_path_is_strong_list = any(hint in base_path_lower for hint in _HTML_STRONG_LIST_PATH_HINTS)

    for anchor in soup.find_all("a"):
        text = normalize_whitespace(anchor.get_text(" ", strip=True))
        href = normalize_whitespace(anchor.get("href"))
        onclick = normalize_whitespace(anchor.get("onclick"))
        if not text or len(text) > 240:
            continue

        job_url = ""
        resolved_url = ""
        same_prefix_detail_signal = False
        has_external_ats_href_signal = False
        lowered_text = text.lower()
        has_text_listing_signal = any(hint.lower() in lowered_text for hint in _HTML_LISTING_TEXT_HINTS)
        looks_like_generic_hiring_nav = _looks_like_generic_hiring_nav_title(text)
        has_strong_text_signal = any(hint.lower() in lowered_text for hint in _HTML_LISTING_STRONG_TEXT_HINTS)
        has_detail_href_signal = bool(href and any(token in href for token in _HTML_DETAIL_URL_HINTS))
        has_detail_onclick_signal = False
        if onclick:
            match = re.search(r"show\('([0-9]+)'\)", onclick)
            if match:
                has_detail_onclick_signal = True
                job_url = urljoin(base_url, f"/rcrt/view.do?annoId={match.group(1)}")
        if href and not href.startswith(("javascript:", "#", "mailto:", "tel:")):
            resolved_url = urljoin(base_url, href)
            resolved_parts = urlsplit(resolved_url)
            resolved_host = resolved_parts.netloc.lower()
            has_external_ats_href_signal = bool(
                resolved_host
                and resolved_host != base_host
                and any(hint in resolved_host for hint in _ATS_REDIRECT_HOST_HINTS)
            )
        if resolved_url and base_path_is_strong_list:
            resolved_parts = urlsplit(resolved_url)
            resolved_host = resolved_parts.netloc.lower()
            resolved_path = normalize_whitespace(resolved_parts.path.rstrip("/"))
            resolved_path_lower = resolved_path.lower()
            extra_path = ""
            if base_path and resolved_path_lower.startswith(f"{base_path_lower}/"):
                extra_path = resolved_path_lower[len(base_path_lower) + 1 :]
            if (
                resolved_host == base_host
                and resolved_path
                and resolved_path != base_path
                and extra_path
                and not extra_path.startswith("page/")
                and not extra_path.startswith("category/")
                and not extra_path.startswith("tag/")
                and "/" not in extra_path.strip("/")
                and not _looks_like_generic_hiring_nav_title(text)
            ):
                same_prefix_detail_signal = True
                has_detail_href_signal = True
        row_context_text = ""
        container = anchor
        for _ in range(3):
            if not getattr(container, "parent", None):
                break
            container = container.parent
            candidate_text = normalize_whitespace(container.get_text(" ", strip=True))
            if len(candidate_text) > len(row_context_text):
                row_context_text = candidate_text
        row_context_lowered = row_context_text.lower()
        has_row_hiring_context = any(hint.lower() in row_context_lowered for hint in _HTML_LISTING_TEXT_HINTS)
        has_detail_posting_signal = (
            (has_detail_href_signal or has_detail_onclick_signal)
            and (
                "모집" in text
                or "채용 중" in text
                or "채용 중" in row_context_text
            )
        )
        if len(text) < 8 and not (
            looks_like_generic_hiring_nav
            or has_detail_href_signal
            or has_detail_onclick_signal
            or has_external_ats_href_signal
        ):
            continue
        if not (
            has_text_listing_signal
            or looks_like_generic_hiring_nav
            or has_strong_text_signal
            or (
                (has_detail_href_signal or has_detail_onclick_signal)
                and (has_row_hiring_context or same_prefix_detail_signal or has_detail_posting_signal)
            )
        ):
            continue
        if not job_url and resolved_url:
            job_url = resolved_url
        if not job_url:
            continue
        if job_url in seen_urls:
            continue
        seen_urls.add(job_url)
        if same_prefix_detail_signal or has_detail_onclick_signal:
            detail_urls.add(job_url)

        title = text
        for marker in ("모집 부서", "모집 분야", "모집 경력", "모집 기간"):
            if marker in title:
                title = normalize_whitespace(title.split(marker, 1)[0])
                break

        jobs.append(
            {
                "title": title or text,
                "description_html": f"<div>{html.escape(text)}</div>",
                "job_url": job_url,
                "listing_context": row_context_text,
            }
        )
    if detail_urls:
        filtered_jobs: list[dict] = []
        for job in jobs:
            title = normalize_whitespace(job.get("title"))
            job_url = normalize_whitespace(job.get("job_url"))
            if job_url in detail_urls:
                filtered_jobs.append(job)
                continue
            if _looks_like_generic_hiring_nav_title(title):
                continue
            if job_url.rstrip("/") == base_url.rstrip("/"):
                continue
            filtered_jobs.append(job)
        jobs = filtered_jobs
    return jobs


def _html_page_looks_like_hiring_page(content: str) -> bool:
    normalized = normalize_whitespace(clean_html_text(content)).lower()
    if not normalized:
        return False
    return any(hint in normalized for hint in _HTML_GEMINI_PROBE_TEXT_HINTS)


def _looks_like_generic_hiring_nav_title(title: str | None) -> bool:
    normalized = normalize_whitespace(clean_html_text(title)).lower()
    if not normalized:
        return False
    normalized = normalized.lstrip("•·- ").strip()
    if len(normalized) > 40:
        return False
    if any(hint in normalized for hint in _HTML_GENERIC_HIRING_NAV_EXCLUDE_HINTS):
        return False
    if normalized in _HTML_GENERIC_HIRING_NAV_HINTS:
        return True
    return any(normalized.startswith(f"{hint} ") or normalized.endswith(f" {hint}") for hint in _HTML_GENERIC_HIRING_NAV_HINTS)


def _looks_like_generic_detail_cta_title(title: str | None) -> bool:
    normalized = " ".join(clean_html_text(title).split()).strip().lower()
    if not normalized:
        return False
    return normalized in {hint.lower() for hint in _HTML_GENERIC_DETAIL_CTA_HINTS}


def _extract_followable_html_redirect_urls(jobs: list[dict], *, base_url: str) -> list[str]:
    base_parts = urlsplit(base_url)
    base_host = base_parts.netloc.lower()
    base_path = normalize_whitespace(base_parts.path.rstrip("/"))
    candidates: list[tuple[int, str]] = []
    seen: set[str] = set()
    for job in jobs:
        title = job.get("title")
        resolved_url = urljoin(base_url, normalize_whitespace(job.get("job_url")))
        if not resolved_url:
            continue
        parts = urlsplit(resolved_url)
        resolved_host = parts.netloc.lower()
        is_external_ats = bool(resolved_host and resolved_host != base_host and any(hint in resolved_host for hint in _ATS_REDIRECT_HOST_HINTS))
        lowered_title = normalize_whitespace(clean_html_text(title)).lower()
        listing_context = normalize_whitespace(clean_html_text(job.get("listing_context"))).lower()
        has_detail_url_signal = any(hint.lower() in resolved_url.lower() for hint in _HTML_DETAIL_URL_HINTS)
        has_hiring_signal = any(hint.lower() in lowered_title for hint in _HTML_LISTING_TEXT_HINTS) or any(
            hint.lower() in listing_context for hint in _HTML_LISTING_TEXT_HINTS
        )
        is_generic_detail_cta = _looks_like_generic_detail_cta_title(title)
        if not _looks_like_generic_hiring_nav_title(title) and not (
            has_hiring_signal and (is_external_ats or is_generic_detail_cta)
        ) and not has_detail_url_signal:
            continue
        if resolved_host != base_host and not is_external_ats:
            continue
        resolved_path = normalize_whitespace(parts.path.rstrip("/"))
        if resolved_url == base_url or resolved_path == base_path:
            continue
        if resolved_url in seen:
            continue
        seen.add(resolved_url)
        priority = 1
        if is_external_ats:
            priority = 0
        if any(marker in lowered_title for marker in ("채용공고", "채용 공고", "job board", "jobs")):
            priority = 0
        candidates.append((priority, resolved_url))
    candidates.sort()
    return [url for _, url in candidates]


def _saramin_mobile_detail_url(url: str) -> str:
    normalized_url = normalize_whitespace(url)
    if not normalized_url:
        return ""
    parts = urlsplit(normalized_url)
    host = parts.netloc.lower()
    path = parts.path.lower()
    if "saramin.co.kr" not in host or not any(hint in path for hint in _SARAMIN_RELAY_PATH_HINTS):
        return ""
    rec_idx = normalize_whitespace((parse_qs(parts.query).get("rec_idx") or [""])[0])
    if not rec_idx:
        return ""
    return f"https://m.saramin.co.kr/job-search/view?rec_idx={rec_idx}"


def _extract_saramin_mobile_detail_job(content: str, *, job_url: str) -> dict[str, Any] | None:
    match = _SARAMIN_EMBEDDED_DETAIL_RE.search(content)
    if not match:
        return None
    try:
        description_html = base64.b64decode(match.group(1)).decode("utf-8", errors="ignore")
    except Exception:  # noqa: BLE001
        return None

    description_text = normalize_whitespace(clean_html_text(description_html))
    if not description_text:
        return None

    page_soup = BeautifulSoup(content, "lxml")
    detail_soup = BeautifulSoup(description_html, "lxml")
    title_candidates = (
        normalize_whitespace(detail_soup.select_one(".job-header__title").get_text(" ", strip=True) if detail_soup.select_one(".job-header__title") else ""),
        normalize_whitespace(page_soup.select_one('meta[property="og:title"]').get("content") if page_soup.select_one('meta[property="og:title"]') else ""),
        normalize_whitespace(page_soup.select_one('meta[name="title"]').get("content") if page_soup.select_one('meta[name="title"]') else ""),
        normalize_whitespace(page_soup.title.get_text(" ", strip=True) if page_soup.title else ""),
    )
    title = next((candidate for candidate in title_candidates if candidate), "")
    title = re.sub(r"\s*\(D-\d+\)\s*-\s*사람인$", "", title).strip()
    title = re.sub(r"\s*-\s*사람인$", "", title).strip()

    experience_level = ""
    location = ""
    for item in page_soup.select("dl.item_info"):
        label = normalize_whitespace(item.select_one(".tit").get_text(" ", strip=True) if item.select_one(".tit") else "")
        value = normalize_whitespace(item.select_one(".desc").get_text(" ", strip=True) if item.select_one(".desc") else "")
        if not label or not value:
            continue
        if "경력" in label and not experience_level:
            experience_level = value
        if any(token in label for token in ("지역", "근무지역", "근무지")) and not location:
            location = value

    sections = extract_sections_from_description(description_html)
    return {
        "title": title,
        "description_html": description_html,
        "requirements": normalize_whitespace(sections.get("자격요건")),
        "preferred": normalize_whitespace(sections.get("우대사항")),
        "job_url": normalize_whitespace(job_url),
        "location": location,
        "country": location,
        "experience_level": experience_level,
    }


def _extract_saramin_company_info_jobs(content: str, base_url: str) -> list[dict[str, Any]]:
    parts = urlsplit(normalize_whitespace(base_url))
    if "saramin.co.kr" not in parts.netloc.lower() or "/company-info/view-inner-recruit" not in parts.path.lower():
        return []

    soup = BeautifulSoup(content, "lxml")
    jobs: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    for item in soup.select(".list_item"):
        anchor = item.select_one(".job_tit a.str_tit[href]")
        if not anchor:
            continue
        job_url = urljoin(base_url, normalize_whitespace(anchor.get("href")))
        if not job_url or "/zf_user/jobs/relay/view" not in urlsplit(job_url).path.lower():
            continue
        if job_url in seen_urls:
            continue
        seen_urls.add(job_url)
        title = normalize_whitespace(anchor.get("title") or anchor.get_text(" ", strip=True))
        if not title:
            continue
        location = normalize_whitespace(
            item.select_one(".recruit_info .work_place").get_text(" ", strip=True)
            if item.select_one(".recruit_info .work_place")
            else ""
        )
        experience_level = normalize_whitespace(
            item.select_one(".recruit_info .career").get_text(" ", strip=True)
            if item.select_one(".recruit_info .career")
            else ""
        )
        sector_text = " ".join(
            normalize_whitespace(node.get_text(" ", strip=True))
            for node in item.select(".job_meta span")
            if normalize_whitespace(node.get_text(" ", strip=True))
        )
        listing_context = " ".join(part for part in (title, sector_text, location, experience_level) if part)
        jobs.append(
            {
                "title": title,
                "description_html": f"<div>{html.escape(title)}</div>",
                "job_url": job_url,
                "location": location,
                "experience_level": experience_level,
                "listing_context": listing_context,
            }
        )
    return jobs


def _xml_text(node: ElementTree.Element | None, path: str, default: str = "") -> str:
    if node is None:
        return default
    found = node.find(path)
    if found is None:
        return default
    return normalize_whitespace(html.unescape("".join(found.itertext())))


def _xml_texts(node: ElementTree.Element | None, path: str) -> list[str]:
    if node is None:
        return []
    return [normalize_whitespace(html.unescape("".join(found.itertext()))) for found in node.findall(path) if normalize_whitespace("".join(found.itertext()))]


def _html_section(title: str, value: str) -> str:
    text = normalize_whitespace(value)
    if not text:
        return ""
    return f"<h2>{html.escape(title)}</h2><p>{html.escape(text).replace(chr(10), '<br/>')}</p>"


def _join_nonempty(*values: str | None, separator: str = "\n") -> str:
    parts = [normalize_whitespace(value) for value in values if normalize_whitespace(value)]
    return separator.join(dict.fromkeys(parts))


def _worknet_detail_page_url(wanted_auth_no: str) -> str:
    auth_no = normalize_whitespace(wanted_auth_no)
    if not auth_no:
        return ""
    query = urlencode(
        {
            "wantedAuthNo": auth_no,
            "infoTypeCd": "VALIDATION",
            "infoTypeGroup": "tb_workinfoworknet",
        }
    )
    return f"https://www.work24.go.kr/wk/a/b/1500/empDetailAuthView.do?{query}"


def _worknet_query_value(url: str, key: str) -> str:
    parts = urlsplit(normalize_whitespace(url))
    return normalize_whitespace((parse_qs(parts.query, keep_blank_values=True).get(key) or [""])[0])


def _worknet_auth_key(url: str, settings) -> str:
    return normalize_whitespace(getattr(settings, "worknet_api_auth_key", "")) or _worknet_query_value(url, "authKey")


def _build_worknet_api_list_url(url: str, auth_key: str) -> str:
    normalized_url = normalize_whitespace(url) or _WORKNET_LIST_API_BASE_URL
    parts = urlsplit(normalized_url)
    query = parse_qs(parts.query, keep_blank_values=True)
    query.pop(_WORKNET_DETAIL_LIMIT_QUERY_KEY, None)
    query["authKey"] = [auth_key]
    query["callTp"] = ["L"]
    query["returnType"] = ["XML"]
    if not normalize_whitespace((query.get("startPage") or [""])[0]):
        query["startPage"] = ["1"]
    if not normalize_whitespace((query.get("display") or [""])[0]):
        query["display"] = [_WORKNET_DEFAULT_DISPLAY]
    if not normalize_whitespace((query.get("sortOrderBy") or [""])[0]):
        query["sortOrderBy"] = ["DESC"]
    if not normalize_whitespace((query.get("empTpGb") or [""])[0]):
        query["empTpGb"] = ["1"]
    if not normalize_whitespace((query.get("keyword") or [""])[0]) and not normalize_whitespace((query.get("occupation") or [""])[0]):
        query["keyword"] = [_WORKNET_DEFAULT_KEYWORD]
    return urlunsplit(("https", "www.work24.go.kr", "/cm/openApi/call/wk/callOpenApiSvcInfo210L01.do", urlencode(query, doseq=True), ""))


def _build_worknet_api_detail_url(url_or_auth_no: str, auth_key: str, *, info_svc: str = "VALIDATION") -> str:
    normalized = normalize_whitespace(url_or_auth_no)
    wanted_auth_no = _worknet_query_value(normalized, "wantedAuthNo") if normalized.startswith(("http://", "https://")) else normalized
    query = {
        "authKey": auth_key,
        "callTp": "D",
        "returnType": "XML",
        "wantedAuthNo": wanted_auth_no,
        "infoSvc": normalize_whitespace(info_svc) or "VALIDATION",
    }
    return urlunsplit(("https", "www.work24.go.kr", "/cm/openApi/call/wk/callOpenApiSvcInfo210D01.do", urlencode(query), ""))


def _worknet_detail_limit(url: str) -> int:
    raw_value = _worknet_query_value(url, _WORKNET_DETAIL_LIMIT_QUERY_KEY)
    try:
        return max(min(int(raw_value or _WORKNET_DEFAULT_DISPLAY), 100), 0)
    except ValueError:
        return int(_WORKNET_DEFAULT_DISPLAY)


def _fetch_worknet_xml(url: str, settings, *, accept: str = "application/xml") -> str:
    timeout_seconds, connect_timeout_seconds = _source_timeout_values(settings, "worknet_api")
    with httpx.Client(
        timeout=build_timeout(timeout_seconds, connect_timeout_seconds),
        follow_redirects=True,
        headers={
            "User-Agent": settings.user_agent,
            "Accept": accept,
        },
    ) as client:
        response = client.get(url)
        response.raise_for_status()
        return response.text


def _worknet_list_nodes(root: ElementTree.Element) -> list[ElementTree.Element]:
    nodes = root.findall(".//wanted")
    if root.tag == "wanted":
        nodes.insert(0, root)
    return nodes


def _worknet_detail_nodes(root: ElementTree.Element) -> list[ElementTree.Element]:
    nodes = root.findall(".//wantedDtl")
    if root.tag == "wantedDtl":
        nodes.insert(0, root)
    return nodes


def _build_worknet_list_job(node: ElementTree.Element) -> dict[str, Any]:
    wanted_auth_no = _xml_text(node, "wantedAuthNo")
    title = _xml_text(node, "title")
    company = _xml_text(node, "company")
    region = _xml_text(node, "region") or _join_nonempty(_xml_text(node, "basicAddr"), _xml_text(node, "detailAddr"), separator=" ")
    experience_level = _xml_text(node, "career")
    job_url = _xml_text(node, "wantedInfoUrl") or _worknet_detail_page_url(wanted_auth_no)
    listing_context = _join_nonempty(
        title,
        company,
        _xml_text(node, "indTpNm"),
        region,
        experience_level,
        _xml_text(node, "minEdubg"),
        _xml_text(node, "maxEdubg"),
        _xml_text(node, "salTpNm"),
        _xml_text(node, "sal"),
        _xml_text(node, "closeDt"),
        separator=" / ",
    )
    return {
        "title": title,
        "description_html": f"<div>{html.escape(listing_context)}</div>" if listing_context else "",
        "job_url": job_url,
        "location": region,
        "country": region,
        "experience_level": experience_level,
        "company_name_hint": company,
        "listing_context": listing_context,
        "worknet_wanted_auth_no": wanted_auth_no,
        "status": "active",
    }


def _build_worknet_detail_job(node: ElementTree.Element, fallback_job: dict[str, Any] | None = None) -> dict[str, Any]:
    fallback_job = fallback_job or {}
    corp = node.find("corpInfo") or node.find(".//corpInfo")
    wanted = node.find("wantedInfo") or node.find(".//wantedInfo")
    wanted_auth_no = _xml_text(node, "wantedAuthNo") or normalize_whitespace(fallback_job.get("worknet_wanted_auth_no"))
    company = _xml_text(corp, "corpNm") or normalize_whitespace(fallback_job.get("company_name_hint"))
    title = _xml_text(wanted, "wantedTitle") or normalize_whitespace(fallback_job.get("title"))
    jobs_name = _xml_text(wanted, "jobsNm")
    related_jobs = _xml_text(wanted, "relJobsNm")
    job_content = _xml_text(wanted, "jobCont")
    requirements = _join_nonempty(
        _xml_text(wanted, "enterTpNm"),
        _xml_text(wanted, "eduNm"),
        _xml_text(wanted, "major"),
        _xml_text(wanted, "certificate"),
        _xml_text(wanted, "compAbl"),
        _xml_text(wanted, "forLang"),
    )
    preferred = _join_nonempty(_xml_text(wanted, "pfCond"), _xml_text(wanted, "etcPfCond"))
    benefits = _join_nonempty(
        _xml_text(wanted, "etcWelfare"),
        _xml_text(wanted, "fourIns"),
        _xml_text(wanted, "retirepay"),
        _xml_text(wanted, "workdayWorkhrCont"),
    )
    keywords = _join_nonempty(*_xml_texts(wanted, ".//srchKeywordNm"), separator=", ")
    company_context = _join_nonempty(
        _xml_text(corp, "indTpCdNm"),
        _xml_text(corp, "busiCont"),
        _xml_text(corp, "busiSize"),
        separator=" / ",
    )
    main_tasks = _join_nonempty(jobs_name, related_jobs, job_content)
    description_html = "".join(
        part
        for part in (
            _html_section("회사정보", _join_nonempty(company, company_context, separator=" / ")),
            _html_section("주요업무", main_tasks),
            _html_section("자격요건", requirements),
            _html_section("우대사항", preferred),
            _html_section("근무조건", _join_nonempty(_xml_text(wanted, "empTpNm"), _xml_text(wanted, "salTpNm"), _xml_text(wanted, "workRegion"))),
            _html_section("복리후생", benefits),
            _html_section("전형/접수", _join_nonempty(_xml_text(wanted, "selMthd"), _xml_text(wanted, "rcptMthd"), _xml_text(wanted, "submitDoc"))),
            _html_section("기타안내", _xml_text(wanted, "etcHopeCont")),
        )
        if part
    )
    if not description_html and fallback_job.get("description_html"):
        description_html = normalize_whitespace(fallback_job.get("description_html"))
    location = _xml_text(wanted, "workRegion") or normalize_whitespace(fallback_job.get("location"))
    detail_url = _xml_text(wanted, "dtlRecrContUrl")
    job_url = detail_url or normalize_whitespace(fallback_job.get("job_url")) or _worknet_detail_page_url(wanted_auth_no)
    return {
        "title": title,
        "description_html": description_html,
        "main_tasks": main_tasks,
        "requirements": requirements,
        "preferred": preferred,
        "core_skills": keywords or _join_nonempty(_xml_text(wanted, "certificate"), _xml_text(wanted, "compAbl"), separator=", "),
        "job_url": job_url,
        "location": location,
        "country": location,
        "experience_level": _xml_text(wanted, "enterTpNm") or normalize_whitespace(fallback_job.get("experience_level")),
        "company_name_hint": company,
        "listing_context": _join_nonempty(title, jobs_name, related_jobs, keywords, location, separator=" / "),
        "worknet_wanted_auth_no": wanted_auth_no,
        "status": "active",
    }


def _parse_worknet_jobs(content: str) -> list[dict[str, Any]]:
    root = ElementTree.fromstring(content)
    detail_jobs = [_build_worknet_detail_job(node) for node in _worknet_detail_nodes(root)]
    if detail_jobs:
        return detail_jobs
    return [_build_worknet_list_job(node) for node in _worknet_list_nodes(root)]


def _fetch_worknet_api_source(url: str, settings) -> tuple[str, str]:
    auth_key = _worknet_auth_key(url, settings)
    if not auth_key:
        raise RuntimeError("Worknet API authKey is not configured")
    call_type = _worknet_query_value(url, "callTp").upper()
    if call_type == "D" or "callopenapisvcinfo210d01" in urlsplit(normalize_whitespace(url)).path.lower():
        detail_url = _build_worknet_api_detail_url(url, auth_key, info_svc=_worknet_query_value(url, "infoSvc") or "VALIDATION")
        jobs = _parse_worknet_jobs(_fetch_worknet_xml(detail_url, settings))
        return json.dumps({"jobs": jobs}, ensure_ascii=False), "application/json"

    list_url = _build_worknet_api_list_url(url, auth_key)
    list_xml = _fetch_worknet_xml(list_url, settings)
    listings = _parse_worknet_jobs(list_xml)
    hydrated_jobs: list[dict[str, Any]] = []
    for listing in listings[: _worknet_detail_limit(url)]:
        wanted_auth_no = normalize_whitespace(listing.get("worknet_wanted_auth_no"))
        if not wanted_auth_no:
            if listing.get("title") and listing.get("job_url"):
                hydrated_jobs.append(listing)
            continue
        try:
            detail_url = _build_worknet_api_detail_url(wanted_auth_no, auth_key)
            detail_jobs = _parse_worknet_jobs(_fetch_worknet_xml(detail_url, settings))
        except Exception:  # noqa: BLE001
            detail_jobs = []
        hydrated_jobs.extend(detail_jobs or [listing])
    return json.dumps({"jobs": hydrated_jobs}, ensure_ascii=False), "application/json"


def _work24_public_query_int(url: str, key: str, default: str, *, minimum: int = 0, maximum: int = 100) -> int:
    raw_value = _worknet_query_value(url, key)
    try:
        value = int(raw_value or default)
    except ValueError:
        value = int(default)
    return max(min(value, maximum), minimum)


def _work24_public_progress_path(paths) -> Path:
    return paths.runtime_dir / _WORK24_PUBLIC_PROGRESS_FILE_NAME


def _load_work24_public_progress(paths) -> dict[str, Any]:
    progress_path = _work24_public_progress_path(paths)
    if not progress_path.exists():
        return {"version": _WORK24_PUBLIC_PROGRESS_VERSION, "sources": {}}
    try:
        payload = json.loads(progress_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"version": _WORK24_PUBLIC_PROGRESS_VERSION, "sources": {}}
    if not isinstance(payload, dict):
        return {"version": _WORK24_PUBLIC_PROGRESS_VERSION, "sources": {}}
    if not isinstance(payload.get("sources"), dict):
        payload["sources"] = {}
    payload["version"] = _WORK24_PUBLIC_PROGRESS_VERSION
    return payload


def _write_work24_public_progress(paths, payload: dict[str, Any]) -> None:
    progress_path = _work24_public_progress_path(paths)
    progress_path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(progress_path, json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def _work24_public_source_key(url: str) -> str:
    normalized_url = normalize_whitespace(url) or _WORK24_PUBLIC_SEARCH_BASE_URL
    parts = urlsplit(normalized_url)
    query = parse_qs(parts.query, keep_blank_values=True)
    stable_query_keys = (
        "srcKeyword",
        "keyword",
        "siteClcd",
        "keywordWantedTitle",
        "keywordBusiNm",
        "keywordJobCont",
        "keywordStaAreaNm",
        "empTpGbcd",
        "sortField",
        "sortOrderBy",
    )
    stable_query = {key: query.get(key, [""])[0] for key in stable_query_keys if query.get(key)}
    return stable_hash(
        [
            parts.scheme or "https",
            parts.netloc or urlsplit(_WORK24_PUBLIC_SEARCH_BASE_URL).netloc,
            parts.path or urlsplit(_WORK24_PUBLIC_SEARCH_BASE_URL).path,
            json.dumps(stable_query, ensure_ascii=False, sort_keys=True),
        ]
    )


def _work24_public_auth_no(job: dict[str, Any]) -> str:
    return normalize_whitespace(job.get("worknet_wanted_auth_no")) or _work24_public_wanted_auth_no(normalize_whitespace(job.get("job_url")))


def _work24_public_page_plan(
    url: str,
    paths=None,
) -> tuple[list[int], dict[str, Any], str, int, set[str], int, int]:
    page_window = _work24_public_query_int(url, "pageLimit", _WORK24_PUBLIC_DEFAULT_PAGE_LIMIT, minimum=1, maximum=5)
    scan_depth_default = str(page_window)
    scan_depth = _work24_public_query_int(url, "scanDepth", scan_depth_default, minimum=1, maximum=50)
    hot_page_limit = _work24_public_query_int(
        url,
        "hotPageLimit",
        _WORK24_PUBLIC_DEFAULT_HOT_PAGE_LIMIT,
        minimum=0,
        maximum=min(page_window, scan_depth),
    )
    hot_pages = list(range(1, hot_page_limit + 1))
    source_key = _work24_public_source_key(url)
    progress = _load_work24_public_progress(paths) if paths is not None else {"version": _WORK24_PUBLIC_PROGRESS_VERSION, "sources": {}}
    source_state = progress.get("sources", {}).get(source_key, {}) if isinstance(progress.get("sources"), dict) else {}
    previous_seen = {
        normalize_whitespace(auth_no)
        for auth_no in source_state.get("seen_wanted_auth_nos", [])
        if normalize_whitespace(auth_no)
    }

    cursor_capacity = max(page_window - len(hot_pages), 0)
    cursor_pages: list[int] = []
    next_page = hot_page_limit + 1
    if cursor_capacity > 0 and scan_depth > hot_page_limit:
        try:
            cursor = int(source_state.get("next_page") or hot_page_limit + 1)
        except (TypeError, ValueError):
            cursor = hot_page_limit + 1
        if cursor <= hot_page_limit or cursor > scan_depth:
            cursor = hot_page_limit + 1
        page = cursor
        while len(cursor_pages) < cursor_capacity:
            if page > scan_depth:
                page = hot_page_limit + 1
            if page not in hot_pages and page not in cursor_pages:
                cursor_pages.append(page)
            page += 1
            if len(cursor_pages) >= max(scan_depth - hot_page_limit, 0):
                break
        next_page = page
        if next_page > scan_depth:
            next_page = hot_page_limit + 1

    pages = list(dict.fromkeys([*hot_pages, *cursor_pages]))
    if not pages:
        pages = [1]
        next_page = 1
    return pages, progress, source_key, next_page, previous_seen, scan_depth, hot_page_limit


def _build_work24_public_search_form(url: str, page: int) -> dict[str, str]:
    keyword = _worknet_query_value(url, "srcKeyword") or _worknet_query_value(url, "keyword") or _WORKNET_DEFAULT_KEYWORD
    result_count = str(_work24_public_query_int(url, "resultCnt", _WORK24_PUBLIC_DEFAULT_RESULT_COUNT, minimum=1, maximum=50))
    site_clcd = _worknet_query_value(url, "siteClcd") or "WORK"
    return {
        "currentPageNo": str(max(page, 1)),
        "pageIndex": str(max(page, 1)),
        "resultCnt": result_count,
        "sortOrderBy": _worknet_query_value(url, "sortOrderBy") or "DESC",
        "sortField": _worknet_query_value(url, "sortField") or "DATE",
        "siteClcd": site_clcd,
        "keyword": keyword,
        "srcKeyword": keyword,
        "keywordWantedTitle": _worknet_query_value(url, "keywordWantedTitle") or "Y",
        "keywordBusiNm": _worknet_query_value(url, "keywordBusiNm") or "N",
        "keywordJobCont": _worknet_query_value(url, "keywordJobCont") or "Y",
        "keywordStaAreaNm": _worknet_query_value(url, "keywordStaAreaNm") or "N",
        "empTpGbcd": _worknet_query_value(url, "empTpGbcd") or "1",
    }


def _fetch_work24_public_html(url: str, settings, *, data: dict[str, str] | None = None) -> str:
    timeout_seconds, connect_timeout_seconds = _source_timeout_values(settings, "work24_public_html")
    with httpx.Client(
        timeout=build_timeout(timeout_seconds, connect_timeout_seconds),
        follow_redirects=True,
        headers={
            "User-Agent": settings.user_agent,
            "Accept": "text/html,application/xhtml+xml",
        },
    ) as client:
        response = client.post(url, data=data) if data is not None else client.get(url)
        response.raise_for_status()
        return response.text


def _work24_public_wanted_auth_no(url: str) -> str:
    return _worknet_query_value(url, "wantedAuthNo")


def _work24_public_mobile_detail_url(job_url: str) -> str:
    absolute_url = urljoin(_WORK24_PUBLIC_SEARCH_BASE_URL, normalize_whitespace(job_url))
    parts = urlsplit(absolute_url)
    query = parse_qs(parts.query, keep_blank_values=True)
    query.setdefault("infoTypeCd", ["VALIDATION"])
    query.setdefault("infoTypeGroup", ["tb_workinfoworknet"])
    query["theWorkYn"] = ["Y"]
    return urlunsplit(("https", "m.work24.go.kr", parts.path, urlencode(query, doseq=True), ""))


def _work24_public_clip_text(value: str, limit: int = 1800) -> str:
    text = normalize_whitespace(clean_html_text(value))
    if len(text) <= limit:
        return text
    return f"{text[:limit].rstrip()}..."


def _work24_public_row_text(row: BeautifulSoup | Any, selector: str) -> str:
    node = row.select_one(selector) if row else None
    return normalize_whitespace(clean_html_text(str(node or "")))


def _parse_work24_public_list_jobs(content: str, base_url: str = _WORK24_PUBLIC_SEARCH_BASE_URL) -> list[dict[str, Any]]:
    soup = BeautifulSoup(content, "lxml")
    jobs: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    for anchor in soup.select("a[data-emp-detail], a[href*='empDetailAuthView.do']"):
        href = normalize_whitespace(anchor.get("href"))
        if not href:
            continue
        job_url = urljoin(base_url or _WORK24_PUBLIC_SEARCH_BASE_URL, href)
        if job_url in seen_urls:
            continue
        seen_urls.add(job_url)
        parts = urlsplit(job_url)
        query = parse_qs(parts.query, keep_blank_values=True)
        info_type_cd = normalize_whitespace((query.get("infoTypeCd") or [""])[0])
        info_type_group = normalize_whitespace((query.get("infoTypeGroup") or [""])[0])
        if info_type_cd and info_type_cd != "VALIDATION":
            continue
        if info_type_group and info_type_group != "tb_workinfoworknet":
            continue
        row = anchor.find_parent("tr")
        checkbox = row.select_one("input[id^='chkboxWantedAuthNo']") if row else None
        checkbox_parts = normalize_whitespace(checkbox.get("value") if checkbox else "").split("|")
        company = ""
        if len(checkbox_parts) >= 3:
            company = checkbox_parts[2]
        company = normalize_whitespace(company or _work24_public_row_text(row, ".cp_name"))
        checkbox_title = normalize_whitespace(checkbox_parts[3] if len(checkbox_parts) >= 4 else "")
        anchor_title = normalize_whitespace(anchor.get_text(" ", strip=True))
        title = (
            checkbox_title
            if checkbox_title and (not anchor_title or _work24_public_title_looks_like_company(anchor_title, company))
            else anchor_title or checkbox_title
        )
        member_text = _work24_public_row_text(row, "li.member")
        salary_text = _work24_public_row_text(row, "li.dollar")
        time_text = _work24_public_row_text(row, "li.time")
        location = _work24_public_row_text(row, "li.site")
        row_text = normalize_whitespace(clean_html_text(str(row or "")))
        deadline_match = re.search(r"마감일\s*:\s*([0-9]{4}-[0-9]{2}-[0-9]{2}|채용시까지)", row_text)
        registered_match = re.search(r"등록일\s*:\s*([0-9]{4}-[0-9]{2}-[0-9]{2})", row_text)
        listing_context = _join_nonempty(
            title,
            company,
            member_text,
            salary_text,
            time_text,
            location,
            f"마감일: {deadline_match.group(1)}" if deadline_match else "",
            f"등록일: {registered_match.group(1)}" if registered_match else "",
            separator=" / ",
        )
        jobs.append(
            {
                "title": title,
                "description_html": _html_section("채용정보", listing_context),
                "main_tasks": listing_context,
                "requirements": member_text,
                "job_url": job_url,
                "location": location,
                "country": location or "대한민국",
                "experience_level": member_text,
                "company_name_hint": company,
                "listing_context": listing_context,
                "worknet_wanted_auth_no": _work24_public_wanted_auth_no(job_url),
                "status": "active",
                "source_compliance": "work24_public_html: metadata-first, source-linked",
            }
        )
    return jobs


def _work24_public_table_value(soup: BeautifulSoup, label: str) -> str:
    for header in soup.find_all("th"):
        if normalize_whitespace(header.get_text(" ", strip=True)) != label:
            continue
        cell = header.find_next_sibling("td")
        if cell is not None:
            return _work24_public_clip_text(str(cell), limit=900)
    return ""


def _work24_public_em_value(soup: BeautifulSoup, label: str) -> str:
    for marker in soup.find_all("em"):
        if normalize_whitespace(marker.get_text(" ", strip=True)) != label:
            continue
        parent_text = normalize_whitespace(marker.parent.get_text(" ", strip=True) if marker.parent else "")
        if parent_text.startswith(label):
            return normalize_whitespace(parent_text[len(label) :])
        return parent_text
    return ""


def _work24_public_heading_body(soup: BeautifulSoup, label: str) -> str:
    for heading in soup.find_all(["strong", "h2", "h3", "h4"]):
        if normalize_whitespace(heading.get_text(" ", strip=True)) != label:
            continue
        parent = heading.parent
        if parent is None:
            continue
        text = normalize_whitespace(parent.get_text("\n", strip=True))
        if text.startswith(label):
            text = normalize_whitespace(text[len(label) :])
        if text:
            return _work24_public_clip_text(text, limit=2400)
    return ""


def _work24_public_detail_title(content: str, soup: BeautifulSoup) -> str:
    for selector in ("strong.title", ".emp_info .title", ".empInfo .title"):
        node = soup.select_one(selector)
        title = normalize_whitespace(node.get_text(" ", strip=True) if node else "")
        if title:
            return title
    match = re.search(r'const\s+WANTED_TITLE\s*=\s*"((?:\\.|[^"\\])*)"', content)
    if match:
        try:
            return normalize_whitespace(json.loads(f'"{match.group(1)}"'))
        except json.JSONDecodeError:
            return normalize_whitespace(match.group(1))
    return ""


def _work24_public_title_looks_like_company(title: str, company: str) -> bool:
    def company_key(value: str) -> str:
        key = re.sub(r"[^0-9a-z가-힣]+", "", canonicalize_company_name_for_jobs(value).casefold())
        for prefix in ("주식회사", "주"):
            if key.startswith(prefix) and len(key) > len(prefix):
                key = key[len(prefix) :]
        return key

    normalized_title = company_key(title)
    normalized_company = company_key(company)
    return bool(normalized_title and normalized_company and normalized_title == normalized_company)


def _parse_work24_public_detail_job(content: str, fallback_job: dict[str, Any] | None = None) -> dict[str, Any]:
    fallback_job = fallback_job or {}
    soup = BeautifulSoup(content, "lxml")
    wanted_auth_no = normalize_whitespace(
        (soup.select_one("input#wantedAuthNo") or {}).get("value", "") if soup.select_one("input#wantedAuthNo") else ""
    ) or normalize_whitespace(fallback_job.get("worknet_wanted_auth_no"))
    company = _work24_public_em_value(soup, "기업명") or normalize_whitespace(fallback_job.get("company_name_hint"))
    fallback_title = normalize_whitespace(fallback_job.get("title"))
    detail_title = _work24_public_detail_title(content, soup)
    title = (
        detail_title
        if detail_title and (not fallback_title or _work24_public_title_looks_like_company(fallback_title, company))
        else fallback_title or detail_title
    )
    location = _work24_public_table_value(soup, "근무 예정지") or normalize_whitespace(fallback_job.get("location"))
    experience = _work24_public_table_value(soup, "경력") or normalize_whitespace(fallback_job.get("experience_level"))
    education = _work24_public_table_value(soup, "학력")
    job_content = _work24_public_heading_body(soup, "직무내용")
    job_category = _join_nonempty(
        _work24_public_table_value(soup, "모집 직종"),
        _work24_public_table_value(soup, "관련 직종"),
        _work24_public_table_value(soup, "직종 키워드"),
        separator=" / ",
    )
    requirements = _join_nonempty(
        job_category,
        experience,
        education,
        _work24_public_table_value(soup, "자격 면허"),
        _work24_public_table_value(soup, "전공"),
    )
    preferred = _join_nonempty(
        _work24_public_table_value(soup, "우대조건"),
        _work24_public_table_value(soup, "기타 우대사항"),
        _work24_public_table_value(soup, "컴퓨터 활용 능력"),
        _work24_public_table_value(soup, "외국어 능력"),
    )
    core_skills = _join_nonempty(
        _work24_public_table_value(soup, "직종 키워드"),
        _work24_public_table_value(soup, "자격 면허"),
        _work24_public_table_value(soup, "컴퓨터 활용 능력"),
        separator=", ",
    )
    main_tasks = _join_nonempty(job_category, job_content)
    description_html = "".join(
        part
        for part in (
            _html_section("채용정보", _join_nonempty(title, company, location, separator=" / ")),
            _html_section("주요업무", main_tasks),
            _html_section("자격요건", requirements),
            _html_section("우대사항", preferred),
            _html_section("핵심기술", core_skills),
        )
        if part
    )
    if not description_html:
        description_html = normalize_whitespace(fallback_job.get("description_html"))
    job_url = normalize_whitespace(fallback_job.get("job_url"))
    return {
        **fallback_job,
        "title": title or normalize_whitespace(fallback_job.get("title")),
        "description_html": description_html,
        "main_tasks": main_tasks or normalize_whitespace(fallback_job.get("main_tasks")),
        "requirements": requirements or normalize_whitespace(fallback_job.get("requirements")),
        "preferred": preferred or normalize_whitespace(fallback_job.get("preferred")),
        "core_skills": core_skills or normalize_whitespace(fallback_job.get("core_skills")),
        "job_url": job_url or _worknet_detail_page_url(wanted_auth_no),
        "location": location,
        "country": location or normalize_whitespace(fallback_job.get("country")) or "대한민국",
        "experience_level": experience,
        "company_name_hint": company,
        "listing_context": _join_nonempty(title, company, job_category, location, separator=" / "),
        "worknet_wanted_auth_no": wanted_auth_no,
        "status": "active",
        "source_compliance": "work24_public_html: detail text clipped, source-linked, no full raw payload storage",
    }


def _parse_work24_public_jobs(content: str, base_url: str = _WORK24_PUBLIC_SEARCH_BASE_URL) -> list[dict[str, Any]]:
    if content.lstrip().startswith("{"):
        return _parse_json_jobs(content)
    list_jobs = _parse_work24_public_list_jobs(content, base_url=base_url)
    if list_jobs:
        return list_jobs
    detail_job = _parse_work24_public_detail_job(content)
    return [detail_job] if detail_job.get("title") and detail_job.get("job_url") else []


def _work24_public_listing_tracking_payload(listings: list[dict[str, Any]]) -> list[dict[str, str]]:
    payload: list[dict[str, str]] = []
    for listing in listings[:_WORK24_PUBLIC_TRACKING_MAX_ITEMS]:
        payload.append(
            {
                "worknet_wanted_auth_no": _work24_public_auth_no(listing),
                "title": normalize_whitespace(listing.get("title")),
                "company": normalize_whitespace(listing.get("company_name_hint")),
                "listing_context": normalize_whitespace(listing.get("listing_context")),
            }
        )
    return payload


def _work24_public_decision_is_target(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return normalize_whitespace(str(value)).lower() in {"1", "true", "yes", "y", "target", "대상", "맞음"}


def _call_llm_for_work24_public_tracking(payload: list[dict[str, str]], settings) -> dict[str, Any]:
    prompt = f"""
아래 고용24 공개 채용검색 목록 메타데이터만 보고, AI/데이터 직무 상세 수집 우선순위를 판정하라.

목적:
- 이 판단은 최종 직무 분류가 아니라 상세 페이지를 먼저 볼 후보를 고르는 추적 신호다.
- 데이터 분석가, 데이터 사이언티스트, 데이터 엔지니어, 인공지능 엔지니어, 인공지능 리서처에 가까우면 target_hint=true로 둔다.
- 일반 웹개발, 단순 앱/서버 개발, 영업, 디자인, 사무직은 target_hint=false로 둔다.

제약:
- 입력에 없는 사실을 만들지 마라.
- 불확실하면 confidence를 낮춰라.
- suggested_role은 아래 허용 직무 중 하나 또는 빈 문자열만 사용하라.
- JSON만 반환하라.

허용 직무:
{json.dumps(ALLOWED_JOB_ROLES, ensure_ascii=False)}

반환 형식:
{{"items":[{{"worknet_wanted_auth_no":"","target_hint":true,"suggested_role":"","confidence":0.0,"reason":""}}]}}

입력:
{json.dumps(payload, ensure_ascii=False)}
""".strip()
    response = _call_json_llm(prompt=prompt, settings=settings, temperature=0.05, max_output_tokens=2048)
    return response if isinstance(response, dict) else {}


def _track_work24_public_listings_with_llm(
    listings: list[dict[str, Any]],
    settings,
    paths=None,
    budget: GeminiBudget | None = None,
) -> list[dict[str, Any]]:
    tracked = [listing.copy() for listing in listings]
    if not tracked or not settings or not paths or not budget:
        return tracked
    if not getattr(settings, "enable_gemini_fallback", False):
        return tracked
    if not _active_llm_api_key(settings) or not _active_llm_model(settings):
        for listing in tracked:
            listing["work24_llm_tracking_error"] = "MissingLLMConfig"
        return tracked
    payload = _work24_public_listing_tracking_payload(tracked)
    payload = [item for item in payload if item.get("worknet_wanted_auth_no") and item.get("title")]
    if not payload:
        return tracked

    cache_path = paths.logs_dir / "work24_public_tracking_cache.json"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache: dict[str, Any] = {}
    if cache_path.exists():
        try:
            cache = json.loads(cache_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            cache = {}
    cache_key = stable_hash([_WORK24_PUBLIC_TRACKING_CACHE_VERSION, json.dumps(payload, ensure_ascii=False, sort_keys=True)])
    if cache_key not in cache:
        if not budget.can_call():
            return tracked
        budget.consume()
        try:
            cache[cache_key] = _call_llm_for_work24_public_tracking(payload, settings)
            atomic_write_text(cache_path, json.dumps(cache, ensure_ascii=False, indent=2, sort_keys=True))
        except Exception as exc:  # noqa: BLE001
            for listing in tracked:
                listing["work24_llm_tracking_error"] = type(exc).__name__
            return tracked

    decision_payload = cache.get(cache_key, {})
    items = decision_payload.get("items", []) if isinstance(decision_payload, dict) else []
    decisions: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        auth_no = normalize_whitespace(item.get("worknet_wanted_auth_no"))
        if auth_no:
            decisions[auth_no] = item

    allowed_roles = set(ALLOWED_JOB_ROLES)
    for listing in tracked:
        auth_no = _work24_public_auth_no(listing)
        decision = decisions.get(auth_no)
        if not decision:
            continue
        target_hint = _work24_public_decision_is_target(decision.get("target_hint"))
        listing["work24_llm_target_hint"] = "true" if target_hint else "false"
        suggested_role = normalize_whitespace(decision.get("suggested_role"))
        if suggested_role in allowed_roles:
            listing["work24_llm_suggested_role"] = suggested_role
        reason = normalize_whitespace(decision.get("reason"))
        confidence = normalize_whitespace(str(decision.get("confidence", "")))
        listing["work24_llm_reason"] = _join_nonempty(reason, f"confidence={confidence}" if confidence else "", separator=" / ")
    return tracked


def _work24_public_detail_priority(listing: dict[str, Any], original_index: int) -> tuple[int, int, int]:
    target_hint = normalize_whitespace(listing.get("work24_llm_target_hint")).lower()
    tracking_signal = normalize_whitespace(listing.get("work24_public_tracking_signal")).lower()
    heuristic_role = classify_job_role(
        normalize_whitespace(listing.get("title")),
        "",
        normalize_whitespace(listing.get("listing_context")),
        normalize_whitespace(listing.get("requirements")),
    )
    if target_hint == "true":
        tier = 0
    elif tracking_signal == "new":
        tier = 1
    elif heuristic_role:
        tier = 2
    elif target_hint == "false":
        tier = 4
    else:
        tier = 3
    try:
        page = int(listing.get("work24_public_page") or 999)
    except (TypeError, ValueError):
        page = 999
    return tier, page, original_index


def _update_work24_public_progress(
    paths,
    progress: dict[str, Any],
    *,
    source_key: str,
    source_url: str,
    next_page: int,
    scan_depth: int,
    hot_page_limit: int,
    selected_pages: list[int],
    listings: list[dict[str, Any]],
    hydrated_jobs: list[dict[str, Any]],
) -> None:
    sources = progress.setdefault("sources", {})
    source_state = sources.get(source_key, {}) if isinstance(sources.get(source_key, {}), dict) else {}
    previous_seen = [
        normalize_whitespace(auth_no)
        for auth_no in source_state.get("seen_wanted_auth_nos", [])
        if normalize_whitespace(auth_no)
    ]
    previous_seen_set = set(previous_seen)
    current_auths = list(dict.fromkeys(_work24_public_auth_no(job) for job in listings if _work24_public_auth_no(job)))
    new_auths = [auth_no for auth_no in current_auths if auth_no not in previous_seen_set]
    merged_seen = list(dict.fromkeys([*current_auths, *previous_seen]))[:_WORK24_PUBLIC_SEEN_AUTH_LIMIT]
    llm_target_hint_count = sum(1 for job in listings if normalize_whitespace(job.get("work24_llm_target_hint")).lower() == "true")
    llm_tracking_error_count = sum(1 for job in listings if normalize_whitespace(job.get("work24_llm_tracking_error")))
    sources[source_key] = {
        "version": _WORK24_PUBLIC_PROGRESS_VERSION,
        "source_url": source_url,
        "next_page": next_page,
        "scan_depth": scan_depth,
        "hot_page_limit": hot_page_limit,
        "last_pages": selected_pages,
        "last_seen_at": datetime.now(timezone.utc).isoformat(),
        "last_listing_count": len(listings),
        "last_detail_count": len(hydrated_jobs),
        "last_seen_wanted_auth_nos": current_auths,
        "new_wanted_auth_nos": new_auths,
        "llm_target_hint_count": llm_target_hint_count,
        "llm_tracking_error_count": llm_tracking_error_count,
        "seen_wanted_auth_nos": merged_seen,
    }
    _write_work24_public_progress(paths, progress)


def _fetch_work24_public_html_source(url: str, settings, *, paths=None) -> tuple[str, str]:
    normalized_url = normalize_whitespace(url) or _WORK24_PUBLIC_SEARCH_BASE_URL
    detail_limit = _work24_public_query_int(normalized_url, _WORKNET_DETAIL_LIMIT_QUERY_KEY, _WORK24_PUBLIC_DEFAULT_DETAIL_LIMIT, minimum=0, maximum=100)
    pages, progress, source_key, next_page, previous_seen, scan_depth, hot_page_limit = _work24_public_page_plan(normalized_url, paths=paths)
    jobs: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    for page in pages:
        form_data = _build_work24_public_search_form(normalized_url, page)
        list_html = _fetch_work24_public_html(_WORK24_PUBLIC_SEARCH_POST_URL, settings, data=form_data)
        for listing in _parse_work24_public_list_jobs(list_html, base_url=_WORK24_PUBLIC_SEARCH_BASE_URL):
            job_url = normalize_whitespace(listing.get("job_url"))
            if not job_url or job_url in seen_urls:
                continue
            seen_urls.add(job_url)
            auth_no = _work24_public_auth_no(listing)
            listing["work24_public_page"] = str(page)
            listing["work24_public_tracking_signal"] = "seen" if auth_no and auth_no in previous_seen else "new"
            jobs.append(listing)
    tracking_budget = GeminiBudget(max_calls=min(3, int(getattr(settings, "gemini_html_listing_max_calls_per_run", 0) or 0)))
    jobs = _track_work24_public_listings_with_llm(jobs, settings, paths=paths, budget=tracking_budget)
    detail_indexes = {
        index
        for index, _listing in sorted(
            enumerate(jobs),
            key=lambda item: _work24_public_detail_priority(item[1], item[0]),
        )[:detail_limit]
    }
    hydrated_jobs: list[dict[str, Any]] = []
    for index, listing in enumerate(jobs):
        if index not in detail_indexes:
            hydrated_jobs.append(listing)
            continue
        try:
            detail_html = _fetch_work24_public_html(_work24_public_mobile_detail_url(normalize_whitespace(listing.get("job_url"))), settings)
            hydrated_jobs.append(_parse_work24_public_detail_job(detail_html, fallback_job=listing))
        except Exception:  # noqa: BLE001
            hydrated_jobs.append(listing)
    if paths is not None:
        _update_work24_public_progress(
            paths,
            progress,
            source_key=source_key,
            source_url=normalized_url,
            next_page=next_page,
            scan_depth=scan_depth,
            hot_page_limit=hot_page_limit,
            selected_pages=pages,
            listings=jobs,
            hydrated_jobs=hydrated_jobs,
        )
    return json.dumps({"jobs": hydrated_jobs}, ensure_ascii=False), "application/json"


def _build_saramin_api_url(url: str, access_key: str) -> str:
    normalized_url = normalize_whitespace(url) or _SARAMIN_API_BASE_URL
    parts = urlsplit(normalized_url)
    query = parse_qs(parts.query, keep_blank_values=True)
    if not normalize_whitespace((query.get("access-key") or [""])[0]):
        query["access-key"] = [access_key]
    if not normalize_whitespace((query.get("count") or [""])[0]):
        query["count"] = ["110"]
    if not normalize_whitespace((query.get("sort") or [""])[0]):
        query["sort"] = ["pd"]
    if not normalize_whitespace((query.get("fields") or [""])[0]):
        query["fields"] = [_SARAMIN_API_DEFAULT_FIELDS]
    encoded_query = urlencode(query, doseq=True)
    return urlunsplit((parts.scheme or "https", parts.netloc or "oapi.saramin.co.kr", parts.path or "/job-search", encoded_query, ""))


def _fetch_saramin_api_json(url: str, settings) -> dict[str, Any]:
    access_key = normalize_whitespace(getattr(settings, "saramin_api_access_key", ""))
    if not access_key:
        raise RuntimeError("Saramin API access-key is not configured")
    request_url = _build_saramin_api_url(url, access_key)
    timeout_seconds, connect_timeout_seconds = _source_timeout_values(settings, "saramin_api")
    with httpx.Client(
        timeout=build_timeout(timeout_seconds, connect_timeout_seconds),
        follow_redirects=True,
        headers={
            "User-Agent": settings.user_agent,
            "Accept": "application/json",
        },
    ) as client:
        response = client.get(request_url)
        response.raise_for_status()
        payload = response.json()
    return payload if isinstance(payload, dict) else {}


def _extract_saramin_api_listings(payload: dict[str, Any]) -> list[dict[str, Any]]:
    jobs_payload = payload.get("jobs") if isinstance(payload, dict) else {}
    if isinstance(jobs_payload, dict):
        listings = jobs_payload.get("job")
        if isinstance(listings, list):
            return [listing for listing in listings if isinstance(listing, dict)]
        if isinstance(listings, dict):
            return [listings]
    return []


def _build_saramin_api_listing_job(listing: dict[str, Any]) -> dict[str, Any]:
    company_block = listing.get("company") or {}
    if isinstance(company_block.get("detail"), dict):
        company_block = company_block.get("detail") or {}
    position = listing.get("position") or {}
    title = normalize_whitespace(position.get("title"))
    job_url = normalize_whitespace(listing.get("url"))
    location = normalize_whitespace((position.get("location") or {}).get("name") if isinstance(position.get("location"), dict) else position.get("location"))
    experience = position.get("experience-level") or {}
    if isinstance(experience, dict):
        experience_level = normalize_whitespace(experience.get("name"))
    else:
        experience_level = normalize_whitespace(experience)
    keyword_text = normalize_whitespace(listing.get("keyword"))
    description_parts = [part for part in (title, keyword_text) if part]
    description_html = f"<div>{html.escape(' / '.join(description_parts))}</div>" if description_parts else ""
    return {
        "title": title,
        "description_html": description_html,
        "job_url": job_url,
        "location": location,
        "country": location,
        "experience_level": experience_level,
        "company_name_hint": normalize_whitespace(company_block.get("name")),
        "listing_context": " ".join(part for part in (title, keyword_text, location, experience_level) if part),
    }


def _fetch_saramin_api_source(url: str, settings) -> tuple[str, str]:
    payload = _fetch_saramin_api_json(url, settings)
    hydrated_jobs: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    for listing in _extract_saramin_api_listings(payload):
        active = normalize_whitespace(str(listing.get("active") or ""))
        if active and active != "1":
            continue
        relay_url = normalize_whitespace(listing.get("url"))
        if not relay_url or relay_url in seen_urls:
            continue
        seen_urls.add(relay_url)
        relay_payload = _fetch_saramin_relay_source(relay_url, settings)
        if relay_payload is not None:
            relay_content, relay_content_type = relay_payload
            relay_jobs = parse_jobs_from_payload(
                relay_content,
                relay_content_type,
                "json_api",
                base_url=relay_url,
                settings=settings,
            )
            if relay_jobs:
                hydrated_jobs.extend(relay_jobs)
                continue
        fallback_job = _build_saramin_api_listing_job(listing)
        if fallback_job.get("title") and fallback_job.get("job_url"):
            hydrated_jobs.append(fallback_job)
    return json.dumps({"jobs": hydrated_jobs}, ensure_ascii=False), "application/json"


def _extract_multi_role_html_jobs(content: str, base_url: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(content, "lxml")
    candidate_selectors = (".jobs .job", ".job-list .job", ".job")

    for selector in candidate_selectors:
        nodes = soup.select(selector)
        jobs: list[dict[str, Any]] = []
        seen_titles: set[str] = set()
        seen_job_urls: set[str] = set()
        for node in nodes:
            title_node = node.select_one("h3, h2, .job_tit, .job-title, .title")
            body_node = node.select_one(".job_con, .job-content, .content, .detail, .desc")
            title = normalize_whitespace(title_node.get_text(" ", strip=True) if title_node else "")
            body_html = str(body_node or node)
            body_text = normalize_whitespace(clean_html_text(body_html))
            has_section_signal = any(token in body_text for token in ("담당 업무", "자격 요건", "자격요건", "우대 사항", "우대사항", "주요업무"))
            if not title or (len(body_text) < 80 and not has_section_signal):
                continue
            lowered_title = title.lower()
            if lowered_title in seen_titles:
                continue
            seen_titles.add(lowered_title)
            job_url = normalize_whitespace(base_url)
            if job_url:
                slug = re.sub(r"[^0-9a-z가-힣]+", "-", lowered_title).strip("-")
                if not slug:
                    slug = stable_hash([title])[:12]
                candidate_job_url = f"{job_url}#role-{slug}"
                dedupe_suffix = 2
                while candidate_job_url in seen_job_urls:
                    candidate_job_url = f"{job_url}#role-{slug}-{dedupe_suffix}"
                    dedupe_suffix += 1
                job_url = candidate_job_url
                seen_job_urls.add(job_url)
            jobs.append(
                {
                    "title": title,
                    "description_html": f"<div><h2>{html.escape(title)}</h2>{body_html}</div>",
                    "job_url": job_url,
                    "location": "",
                    "experience_level": "",
                }
            )
        if len(jobs) >= 2:
            return jobs
    return []


def _fetch_saramin_relay_source(url: str, settings) -> tuple[str, str] | None:
    mobile_url = _saramin_mobile_detail_url(url)
    if not mobile_url:
        return None
    timeout_seconds, connect_timeout_seconds = _source_timeout_values(settings, "html_page")
    mobile_content, _ = _fetch_remote(
        mobile_url,
        timeout_seconds,
        settings.user_agent,
        connect_timeout_seconds=connect_timeout_seconds,
    )
    job = _extract_saramin_mobile_detail_job(mobile_content, job_url=url)
    if not job:
        return None
    return json.dumps({"jobs": [job]}, ensure_ascii=False), "application/json"


def _maybe_follow_html_redirect_jobs(
    jobs: list[dict],
    *,
    base_url: str,
    settings=None,
    paths=None,
    gemini_budget: GeminiBudget | None = None,
    redirect_depth: int = 0,
    visited_urls: set[str] | None = None,
) -> list[dict]:
    if not jobs or not settings or not paths:
        return []
    if redirect_depth >= _HTML_REDIRECT_FOLLOW_MAX_DEPTH:
        return []
    candidate_urls = _extract_followable_html_redirect_urls(jobs, base_url=base_url)
    if not candidate_urls:
        return []

    visited = set(visited_urls or set())
    visited.add(base_url)
    for candidate_url in candidate_urls[:3]:
        if candidate_url in visited:
            continue
        visited.add(candidate_url)
        try:
            redirected_content, redirected_content_type = fetch_source_content(
                candidate_url,
                paths,
                settings,
                _infer_source_type_from_url(candidate_url),
            )
        except Exception:  # noqa: BLE001
            continue
        redirected_jobs = parse_jobs_from_payload(
            redirected_content,
            redirected_content_type,
            _infer_source_type_from_url(candidate_url),
            base_url=candidate_url,
            settings=settings,
            paths=paths,
            gemini_budget=gemini_budget,
            redirect_depth=redirect_depth + 1,
            visited_urls=visited,
        )
        if any(not _html_job_looks_like_generic_stub(job) for job in redirected_jobs):
            return redirected_jobs
    return []


def _prepare_html_gemini_probe_payload(content: str, base_url: str) -> tuple[dict[str, Any], set[str]]:
    soup = BeautifulSoup(content, "lxml")
    page_title = normalize_whitespace(soup.title.get_text(" ", strip=True) if soup.title else "")
    anchors: list[dict[str, str]] = []
    allowed_urls: set[str] = set()
    seen: set[tuple[str, str]] = set()
    base_host = urlsplit(base_url).netloc.lower()

    for anchor in soup.find_all("a"):
        href = normalize_whitespace(anchor.get("href"))
        text = normalize_whitespace(anchor.get_text(" ", strip=True))
        if not href or not text:
            continue
        if href.startswith(("javascript:", "#", "mailto:", "tel:")):
            continue
        resolved_url = urljoin(base_url, href)
        resolved_host = urlsplit(resolved_url).netloc.lower()
        if resolved_host and base_host and resolved_host != base_host:
            continue
        if len(text) < 4 or len(text) > 240:
            continue
        key = (text, resolved_url)
        if key in seen:
            continue
        seen.add(key)
        lowered = text.lower()
        href_lower = resolved_url.lower()
        if len(anchors) >= _HTML_GEMINI_MAX_ANCHORS:
            break
        if not any(hint in lowered for hint in _HTML_GEMINI_PROBE_TEXT_HINTS) and not any(hint.lower() in href_lower for hint in _HTML_DETAIL_URL_HINTS):
            continue
        anchors.append({"text": text, "url": resolved_url})
        allowed_urls.add(resolved_url)

    excerpt = normalize_whitespace(clean_html_text(content))[:2500]
    return {
        "base_url": base_url,
        "page_title": page_title,
        "page_excerpt": excerpt,
        "anchors": anchors,
    }, allowed_urls


def _parse_gemini_html_jobs_response(text: str) -> list[dict[str, str]]:
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = stripped.strip("`")
        if "\n" in stripped:
            stripped = stripped.split("\n", 1)[1]
    if stripped.endswith("```"):
        stripped = stripped[:-3]
    payload = json.loads(stripped or "{}")
    if isinstance(payload, dict):
        jobs = payload.get("jobs") or payload.get("items") or []
    elif isinstance(payload, list):
        jobs = payload
    else:
        jobs = []
    return jobs if isinstance(jobs, list) else []


def _call_gemini_for_html_jobs(payload: dict[str, Any], settings) -> list[dict[str, str]]:
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

    response = _call_json_llm(prompt=prompt, settings=settings, temperature=0.1, max_output_tokens=1024)
    if isinstance(response, dict):
        jobs = response.get("jobs") or response.get("items") or []
    elif isinstance(response, list):
        jobs = response
    else:
        jobs = []
    return jobs if isinstance(jobs, list) else []


def _extract_html_jobs_with_gemini(
    content: str,
    base_url: str,
    *,
    settings=None,
    paths=None,
    budget: GeminiBudget | None = None,
) -> list[dict]:
    if not settings or not paths or not budget:
        return []
    if not settings.enable_gemini_fallback or not _active_llm_api_key(settings) or not _active_llm_model(settings) or not budget.can_call():
        return []
    if not (_html_page_looks_like_hiring_page(content) or _html_source_has_hiring_path_signal({"source_url": base_url})):
        return []

    payload, allowed_urls = _prepare_html_gemini_probe_payload(content, base_url)
    if not payload["anchors"]:
        return []

    cache_path = paths.logs_dir / "gemini_html_jobs_cache.json"
    cache: dict[str, list[dict[str, str]]] = {}
    if cache_path.exists():
        try:
            cache = json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            cache = {}
    cache_key = stable_hash([payload["base_url"], payload["page_title"], payload["page_excerpt"], json.dumps(payload["anchors"], ensure_ascii=False)])
    if cache_key not in cache:
        budget.consume()
        try:
            cache[cache_key] = _call_gemini_for_html_jobs(payload, settings)
            atomic_write_text(cache_path, json.dumps(cache, ensure_ascii=False, indent=2))
        except Exception:  # noqa: BLE001
            return []

    jobs: list[dict] = []
    seen: set[str] = set()
    for item in cache.get(cache_key, []):
        if not isinstance(item, dict):
            continue
        title = normalize_whitespace(item.get("title"))
        job_url = normalize_whitespace(item.get("job_url"))
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
                "location": normalize_whitespace(item.get("location")),
                "experience_level": normalize_whitespace(item.get("experience_level")),
            }
        )
    return jobs


def _parse_html_jobs(
    content: str,
    base_url: str = "",
    *,
    settings=None,
    paths=None,
    gemini_budget: GeminiBudget | None = None,
    redirect_depth: int = 0,
    visited_urls: set[str] | None = None,
) -> list[dict]:
    jsonld_jobs = _extract_jsonld_jobs_from_html(content)
    if jsonld_jobs:
        return jsonld_jobs

    flex_jobs = _extract_flex_job_description_jobs(content, base_url, settings=settings)
    if flex_jobs:
        return flex_jobs

    next_rsc_jobs = _extract_next_rsc_notice_jobs(content, base_url)
    if next_rsc_jobs:
        return next_rsc_jobs

    soup = BeautifulSoup(content, "lxml")
    single_detail_job = _maybe_extract_single_html_detail_job(content, base_url) if base_url else None
    multi_role_jobs = _extract_multi_role_html_jobs(content, base_url)
    if multi_role_jobs:
        return multi_role_jobs
    saramin_company_jobs = _extract_saramin_company_info_jobs(content, base_url)
    if saramin_company_jobs:
        redirected = _maybe_follow_html_redirect_jobs(
            saramin_company_jobs,
            base_url=base_url,
            settings=settings,
            paths=paths,
            gemini_budget=gemini_budget,
            redirect_depth=redirect_depth,
            visited_urls=visited_urls,
        )
        if redirected:
            return redirected
        return saramin_company_jobs
    jobs: list[dict] = []
    for card in soup.select("article.job-posting, article[data-role='job'], div.job-card"):
        job_link = card.select_one("a.job-link")
        jobs.append(
            {
                "title": card.select_one(".job-title").get_text(" ", strip=True) if card.select_one(".job-title") else "",
                "title_ko": card.select_one(".job-title-ko").get_text(" ", strip=True) if card.select_one(".job-title-ko") else "",
                "description_html": str(card.select_one(".job-description") or ""),
                "requirements": str(card.select_one(".requirements") or ""),
                "preferred": str(card.select_one(".preferred") or ""),
                "core_skills": str(card.select_one(".skills") or ""),
                "experience_level": card.select_one(".experience-level").get_text(" ", strip=True) if card.select_one(".experience-level") else "",
                "experience_level_ko": card.select_one(".experience-level-ko").get_text(" ", strip=True) if card.select_one(".experience-level-ko") else "",
                "job_url": card.get("data-job-url") or (job_link.get("href") if job_link else ""),
            }
        )
    if jobs:
        redirected = _maybe_follow_html_redirect_jobs(
            jobs,
            base_url=base_url,
            settings=settings,
            paths=paths,
            gemini_budget=gemini_budget,
            redirect_depth=redirect_depth,
            visited_urls=visited_urls,
        )
        if redirected:
            return redirected
        return jobs
    anchor_jobs = _build_html_listing_jobs_from_anchors(content, base_url)
    if anchor_jobs:
        all_generic_stubs = all(_html_job_looks_like_generic_stub(job) for job in anchor_jobs)
        if all_generic_stubs:
            if single_detail_job:
                return [single_detail_job]
            gemini_jobs = _extract_html_jobs_with_gemini(content, base_url, settings=settings, paths=paths, budget=gemini_budget)
            redirected = _maybe_follow_html_redirect_jobs(
                gemini_jobs,
                base_url=base_url,
                settings=settings,
                paths=paths,
                gemini_budget=gemini_budget,
                redirect_depth=redirect_depth,
                visited_urls=visited_urls,
            )
            if redirected:
                return redirected
            if gemini_jobs:
                return gemini_jobs
        redirected = _maybe_follow_html_redirect_jobs(
            anchor_jobs,
            base_url=base_url,
            settings=settings,
            paths=paths,
            gemini_budget=gemini_budget,
            redirect_depth=redirect_depth,
            visited_urls=visited_urls,
        )
        if redirected:
            return redirected
        return anchor_jobs
    if single_detail_job:
        return [single_detail_job]
    gemini_jobs = _extract_html_jobs_with_gemini(content, base_url, settings=settings, paths=paths, budget=gemini_budget)
    redirected = _maybe_follow_html_redirect_jobs(
        gemini_jobs,
        base_url=base_url,
        settings=settings,
        paths=paths,
        gemini_budget=gemini_budget,
        redirect_depth=redirect_depth,
        visited_urls=visited_urls,
    )
    if redirected:
        return redirected
    return gemini_jobs


def _html_job_description_looks_weak(job: dict[str, Any]) -> bool:
    description_html = normalize_whitespace(job.get("description_html"))
    description_text = clean_html_text(description_html)
    title = normalize_whitespace(job.get("title") or job.get("job_title"))
    extracted_sections = extract_sections_from_description(description_html)
    has_structured_sections = any(normalize_whitespace(value) for value in extracted_sections.values())
    if not description_text:
        return True
    lowered = description_text.lower()
    if title and normalize_whitespace(description_text) == title:
        return True
    metadata_hits = sum(1 for hint in _HTML_LISTING_METADATA_ONLY_HINTS if hint in description_text)
    if metadata_hits >= 3 and len(description_text) <= 220:
        return True
    if len(description_text) <= 80 and any(hint in lowered for hint in _HTML_GENERIC_HIRING_NAV_HINTS):
        return True
    if len(description_text) <= 120 and not has_structured_sections:
        return True
    return False


def _extract_detail_field_pairs(soup: BeautifulSoup) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    def _append_pair(label: str, value: str) -> None:
        normalized_label = normalize_whitespace(label)
        normalized_value = normalize_whitespace(value)
        if not normalized_label or not normalized_value:
            return
        key = (normalized_label.lower(), normalized_value.lower())
        if key in seen:
            return
        seen.add(key)
        pairs.append((normalized_label, normalized_value))

    for wrapper in soup.select("dl, table"):
        if wrapper.name == "dl":
            dt_nodes = wrapper.find_all("dt")
            dd_nodes = wrapper.find_all("dd")
            for dt_node, dd_node in zip(dt_nodes, dd_nodes):
                _append_pair(
                    dt_node.get_text(" ", strip=True),
                    dd_node.get_text("\n", strip=True),
                )
            continue
        for row in wrapper.select("tr"):
            _append_pair(
                row.select_one("th").get_text(" ", strip=True) if row.select_one("th") else "",
                row.select_one("td").get_text("\n", strip=True) if row.select_one("td") else "",
            )

    # Generic detail pages often expose job fields as repeated `div.title + div.content`.
    for label_node in soup.select(".title, .detail_title"):
        label = normalize_whitespace(label_node.get_text(" ", strip=True))
        if not label:
            continue
        sibling = label_node.find_next_sibling()
        while sibling is not None and getattr(sibling, "name", None) is None:
            sibling = sibling.next_sibling
        if sibling is None or not getattr(sibling, "name", None):
            continue
        sibling_classes = {normalize_whitespace(name).lower() for name in (sibling.get("class") or [])}
        if "content" not in sibling_classes and "detail_text" not in sibling_classes:
            continue
        _append_pair(label, sibling.get_text("\n", strip=True))
    return pairs


def _extract_generic_html_detail_job(content: str, detail_url: str, fallback_job: dict[str, Any]) -> dict[str, Any]:
    soup = BeautifulSoup(content, "lxml")
    detail_pairs = _extract_detail_field_pairs(soup)
    fallback_title = normalize_whitespace(fallback_job.get("title"))

    title_candidates = (
        normalize_whitespace(soup.select_one(".panel-heading h3").get_text(" ", strip=True) if soup.select_one(".panel-heading h3") else ""),
        normalize_whitespace(soup.select_one(".panel-title").get_text(" ", strip=True) if soup.select_one(".panel-title") else ""),
        normalize_whitespace(soup.select_one(".card_title").get_text(" ", strip=True) if soup.select_one(".card_title") else ""),
        normalize_whitespace(soup.select_one("h1").get_text(" ", strip=True) if soup.select_one("h1") else ""),
        normalize_whitespace(soup.select_one("h2").get_text(" ", strip=True) if soup.select_one("h2") else ""),
        normalize_whitespace(soup.title.get_text(" ", strip=True) if soup.title else ""),
        fallback_title,
    )
    title = next((candidate for candidate in title_candidates if candidate), "")
    if _looks_like_generic_hiring_nav_title(title) or _looks_like_generic_detail_cta_title(title):
        for label, value in detail_pairs:
            if re.search(r"(채용|모집)?\s*(포지션|직무)$", label):
                title = value
                break
        if (
            (_looks_like_generic_hiring_nav_title(title) or _looks_like_generic_detail_cta_title(title))
            and fallback_title
            and not _html_job_looks_like_generic_stub({"title": fallback_title, "job_url": detail_url})
        ):
            title = fallback_title

    experience_level = normalize_whitespace(fallback_job.get("experience_level"))
    location = normalize_whitespace(fallback_job.get("location"))
    for label, value in detail_pairs:
        if any(token in label for token in ("모집 경력", "경력", "experience")) and not experience_level:
            experience_level = value
        if any(token in label for token in ("근무지", "근무지역", "근무장소", "location")) and not location:
            location = value

    detail_blocks: list[str] = []
    seen_blocks: set[str] = set()
    for box in soup.select(".detail_box"):
        heading_node = box.select_one(".detail_title, h1, h2, h3, h4, h5, strong")
        body_node = box.select_one(".detail_text")
        heading = normalize_whitespace(heading_node.get_text(" ", strip=True) if heading_node else "")
        preferred_body = str(body_node) if body_node else ""
        preferred_text = clean_html_text(preferred_body)
        if len(normalize_whitespace(preferred_text)) >= 40:
            snippet = preferred_body
        else:
            snippet = str(box)
        key = normalize_whitespace(clean_html_text(snippet)).lower()
        if not key or key in seen_blocks:
            continue
        seen_blocks.add(key)
        detail_blocks.append(snippet)

    if not detail_blocks and detail_pairs:
        pair_sections: list[str] = []
        for label, value in detail_pairs:
            if not any(
                pattern.search(label)
                for pattern in (
                    re.compile(r"(직무|주요)\s*내용"),
                    re.compile(r"직무\s*상세"),
                    re.compile(r"(지원\s*자격|지원자격|자격요건|필수요건|우대사항|담당업무|주요업무)"),
                )
            ):
                continue
            pair_sections.append(
                "<div class=\"detail_box\">"
                f"<h4 class=\"detail_title\">{html.escape(label)}</h4>"
                f"<div class=\"detail_text\">{html.escape(value).replace(chr(10), '<br>')}</div>"
                "</div>"
            )
        if pair_sections:
            detail_blocks.append("\n".join(pair_sections))

    if not detail_blocks:
        for selector in ("main", "article", ".content", ".contents", ".view_cont", ".detail_cont"):
            node = soup.select_one(selector)
            if not node:
                continue
            text = clean_html_text(str(node))
            if len(normalize_whitespace(text)) < 120:
                continue
            detail_blocks.append(str(node))
            break

    description_html = "\n".join(detail_blocks) if detail_blocks else content
    extracted_sections = extract_sections_from_description(description_html)
    extracted_section_count = sum(
        1
        for key in ("주요업무", "자격요건", "우대사항")
        if normalize_whitespace(extracted_sections.get(key))
    )
    if detail_blocks and extracted_section_count < 2:
        full_text = normalize_whitespace(clean_html_text(content))
        selected_text = normalize_whitespace(clean_html_text(description_html))
        if len(full_text) >= len(selected_text) + 120:
            description_html = content
    return {
        **fallback_job,
        "title": title or fallback_job.get("title", ""),
        "job_url": normalize_whitespace(fallback_job.get("job_url")) or detail_url,
        "description_html": description_html,
        "experience_level": experience_level,
        "location": location,
        "country": location or normalize_whitespace(fallback_job.get("country")),
    }


def _html_job_looks_like_generic_stub(job: dict[str, Any]) -> bool:
    title = normalize_whitespace(job.get("title") or job.get("job_title"))
    job_url = normalize_whitespace(job.get("job_url"))
    if not title:
        return True
    if job_url.startswith(("mailto:", "tel:")):
        return True
    if "@" in title and "." in title:
        return True
    return _looks_like_generic_hiring_nav_title(title) or _looks_like_generic_detail_cta_title(title)


def _looks_like_single_html_detail_page(detail_url: str, detail_job: dict[str, Any]) -> bool:
    title = normalize_whitespace(detail_job.get("title"))
    if not title or _html_job_looks_like_generic_stub(detail_job):
        return False
    description_text = normalize_whitespace(clean_html_text(detail_job.get("description_html")))
    sections = extract_sections_from_description(detail_job.get("description_html"))
    substantive_sections = sum(
        1
        for key in ("주요업무", "자격요건", "우대사항")
        if normalize_whitespace(sections.get(key))
    )
    if len(description_text) < 80 and substantive_sections < 2:
        return False
    path = urlsplit(detail_url).path.lower()
    has_hiring_path_signal = any(
        token in path for token in ("career", "careers", "job", "jobs", "opening", "openings", "position", "positions", "recruit")
    )
    has_detail_path_signal = bool(re.search(r"/(?:[0-9]{2,}|[a-z0-9][a-z0-9_-]{7,})/?$", path))
    return substantive_sections >= 2 or (substantive_sections >= 1 and has_hiring_path_signal) or (has_hiring_path_signal and has_detail_path_signal)


def _maybe_extract_single_html_detail_job(content: str, detail_url: str) -> dict[str, Any] | None:
    candidate = _extract_generic_html_detail_job(
        content,
        detail_url,
        {
            "title": "",
            "job_url": detail_url,
            "description_html": "",
            "location": "",
            "experience_level": "",
            "country": "",
        },
    )
    if not _looks_like_single_html_detail_page(detail_url, candidate):
        return None
    return candidate


def _hydrate_html_job_details(
    jobs: list[dict[str, Any]],
    source_row: dict[str, Any],
    *,
    settings=None,
    paths=None,
) -> list[dict[str, Any]]:
    if normalize_whitespace(source_row.get("source_type")) != "html_page" or not settings or not paths:
        return jobs

    source_url = normalize_whitespace(source_row.get("source_url"))
    source_host = urlsplit(source_url).netloc.lower()
    hydrated: list[dict[str, Any]] = []
    fetched_count = 0

    for job in jobs:
        if fetched_count >= _HTML_DETAIL_FETCH_LIMIT_PER_SOURCE:
            hydrated.append(job)
            continue
        job_url = normalize_whitespace(job.get("job_url"))
        if (
            not job_url
            or job_url == source_url
            or urlsplit(job_url).netloc.lower() != source_host
            or not _html_job_description_looks_weak(job)
        ):
            hydrated.append(job)
            continue
        try:
            detail_content, _ = fetch_source_content(job_url, paths, settings, "html_page")
            hydrated.append(_extract_generic_html_detail_job(detail_content, job_url, job))
            fetched_count += 1
        except Exception:  # noqa: BLE001
            hydrated.append(job)
    return hydrated


def _parse_xml_jobs(content: str) -> list[dict]:
    root = ElementTree.fromstring(content)
    jobs: list[dict] = []
    for node in root.findall(".//job") + root.findall(".//item"):
        jobs.append(
            {
                "title": node.findtext("title", default=""),
                "title_ko": node.findtext("title_ko", default=""),
                "description_html": node.findtext("description_html", default=""),
                "requirements": node.findtext("requirements", default=""),
                "preferred": node.findtext("preferred", default=""),
                "core_skills": node.findtext("core_skills", default=""),
                "experience_level": node.findtext("experience_level", default=""),
                "experience_level_ko": node.findtext("experience_level_ko", default=""),
                "job_url": node.findtext("job_url", default=""),
            }
        )
    return jobs


def parse_jobs_from_payload(
    content: str,
    content_type: str,
    source_type: str,
    base_url: str = "",
    *,
    settings=None,
    paths=None,
    gemini_budget: GeminiBudget | None = None,
    redirect_depth: int = 0,
    visited_urls: set[str] | None = None,
) -> list[dict]:
    lowered_content_type = (content_type or "").lower()
    if source_type == "saramin_api":
        return _parse_json_jobs(content)
    if source_type == "worknet_api":
        if content.lstrip().startswith("<"):
            return _parse_worknet_jobs(content)
        return _parse_json_jobs(content)
    if source_type == "work24_public_html":
        return _parse_work24_public_jobs(content, base_url=base_url)
    if source_type == "greenhouse":
        return _parse_greenhouse_jobs(content)
    if source_type == "lever":
        return _parse_lever_jobs(content)
    if source_type in {"json_api", "jsonld", "greetinghr", "recruiter"} or "json" in lowered_content_type:
        return _parse_json_jobs(content)
    if source_type in {"rss", "sitemap"} or "xml" in lowered_content_type:
        return _parse_xml_jobs(content)
    return _parse_html_jobs(
        content,
        base_url=base_url,
        settings=settings,
        paths=paths,
        gemini_budget=gemini_budget,
        redirect_depth=redirect_depth,
        visited_urls=visited_urls,
    )


def _load_mock_mapping(paths) -> dict:
    return load_yaml(paths.mock_source_registry_path).get("sources", {})


def _source_timeout_values(settings, source_type: str) -> tuple[float, float]:
    if source_type == "html_page":
        total = getattr(settings, "html_source_timeout_seconds", None)
        connect = getattr(settings, "html_source_connect_timeout_seconds", None)
    elif source_type in {"saramin_api", "worknet_api", "work24_public_html", "greetinghr", "recruiter", "greenhouse", "lever", "json_api", "jsonld", "rss", "sitemap"}:
        total = getattr(settings, "ats_source_timeout_seconds", None)
        connect = getattr(settings, "ats_source_connect_timeout_seconds", None)
    else:
        total = None
        connect = None
    if total is None:
        total = getattr(settings, "timeout_seconds", 20.0)
    if connect is None:
        connect = getattr(settings, "connect_timeout_seconds", 5.0)
    return float(total), float(connect)


@retry(
    stop=stop_after_attempt(2),
    wait=wait_fixed(0.5),
    retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError)),
)
def _fetch_remote(
    url: str,
    timeout_seconds: float,
    user_agent: str,
    *,
    connect_timeout_seconds: float | None = None,
) -> tuple[str, str]:
    with httpx.Client(
        timeout=build_timeout(timeout_seconds, connect_timeout_seconds),
        follow_redirects=True,
        headers={"User-Agent": user_agent},
    ) as client:
        response = client.get(url)
        response.raise_for_status()
        return response.text, response.headers.get("content-type", "")


def _fetch_greetinghr_source(url: str, paths, settings) -> tuple[str, str]:
    timeout_seconds, connect_timeout_seconds = _source_timeout_values(settings, "greetinghr")
    with httpx.Client(
        timeout=build_timeout(timeout_seconds, connect_timeout_seconds),
        follow_redirects=True,
        headers={"User-Agent": settings.user_agent},
    ) as client:
        response = client.get(url)
        response.raise_for_status()

        def detail_fetcher(detail_url: str) -> str:
            detail_response = client.get(detail_url)
            detail_response.raise_for_status()
            return detail_response.text

        jobs = _build_greetinghr_jobs_from_html(response.text, str(response.url), detail_fetcher)
        if not jobs:
            for candidate_url in _extract_greetinghr_candidate_page_urls(response.text, str(response.url))[:3]:
                candidate_response = client.get(candidate_url)
                candidate_response.raise_for_status()
                jobs = _build_greetinghr_jobs_from_html(candidate_response.text, str(candidate_response.url), detail_fetcher)
                if jobs:
                    break
        if not jobs:
            greetinghr_gemini_budget = GeminiBudget(
                max_calls=min(int(getattr(settings, "gemini_html_listing_max_calls_per_run", 0) or 0), 2)
            )
            jobs = _extract_greetinghr_jobs_with_gemini_router(
                response.text,
                str(response.url),
                detail_fetcher,
                settings=settings,
                paths=paths,
                budget=greetinghr_gemini_budget,
            )
    return json.dumps({"jobs": jobs}, ensure_ascii=False), "application/json"


def _html_looks_like_embedded_greetinghr(content: str) -> bool:
    lowered = content.lower()
    if not any(
        marker in lowered
        for marker in (
            "profiles.greetinghr.com",
            "opening-attachments.greetinghr.com",
            "career.greetinghr",
        )
    ):
        return False
    openings = _find_next_query_data(content, "\"openings\"")
    return isinstance(openings, list) and bool(openings)


def _html_looks_like_embedded_ninehire(content: str) -> bool:
    lowered = content.lower()
    if "__next_data__" not in lowered or "ninehire" not in lowered:
        return False
    payload = _extract_next_data_payload(content)
    page = normalize_whitespace(payload.get("page"))
    if page.startswith("/job_posting/"):
        return False
    context = _extract_ninehire_recruiting_context(payload, "")
    return bool(context.get("company_id") and context.get("job_posting_block"))


def _fetch_ninehire_source(url: str, paths, settings) -> tuple[str, str]:
    timeout_seconds, connect_timeout_seconds = _source_timeout_values(settings, "html_page")
    headers = {
        "User-Agent": settings.user_agent,
        "Origin": _source_root_url(url),
        "Referer": url,
    }
    page_content, _ = _fetch_remote(
        url,
        timeout_seconds,
        settings.user_agent,
        connect_timeout_seconds=connect_timeout_seconds,
    )
    next_payload = _extract_next_data_payload(page_content)
    context = _extract_ninehire_recruiting_context(next_payload, url)
    company_id = normalize_whitespace(context.get("company_id"))
    if not company_id:
        return json.dumps({"jobs": []}, ensure_ascii=False), "application/json"
    page = 1
    count_per_page = max(1, int(context.get("count_per_page") or 25))
    order = normalize_whitespace(context.get("order") or "created_at_desc")
    fixed_recruitment_ids = context.get("fixed_recruitment_ids") or []
    api_base = "https://api.ninehire.com"
    recruitments: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    while True:
        params = [
            ("companyId", company_id),
            ("page", str(page)),
            ("countPerPage", str(count_per_page)),
            ("order", order),
        ]
        for fixed_recruitment_id in fixed_recruitment_ids:
            normalized = normalize_whitespace(fixed_recruitment_id)
            if normalized:
                params.append(("fixedRecruitmentIds", normalized))
        payload = _fetch_ninehire_public_json(
            f"{api_base}/identity-access/homepage/recruitments?{urlencode(params)}",
            settings,
            headers=headers,
        )
        raw_results = payload.get("results") if isinstance(payload, dict) else []
        results = raw_results if isinstance(raw_results, list) else []
        if not results:
            break
        for item in results:
            if not isinstance(item, dict):
                continue
            recruitment_id = normalize_whitespace(item.get("recruitmentId"))
            status = normalize_whitespace(item.get("status"))
            if not recruitment_id or status not in {"in_progress", "published", ""} or recruitment_id in seen_ids:
                continue
            seen_ids.add(recruitment_id)
            recruitments.append(item)
        total_count = int(payload.get("count") or len(recruitments)) if isinstance(payload, dict) else len(recruitments)
        if len(results) < count_per_page or len(seen_ids) >= total_count:
            break
        page += 1

    def detail_fetcher(recruitment_id: str) -> dict[str, Any]:
        return _fetch_ninehire_public_json(
            f"{api_base}/recruiting/job-posting?{urlencode({'recruitmentId': recruitment_id})}",
            settings,
            headers=headers,
        )

    jobs = _build_ninehire_jobs(
        recruitments,
        public_base_url=normalize_whitespace(context.get("public_base_url")),
        detail_fetcher=detail_fetcher,
    )
    return json.dumps({"jobs": jobs}, ensure_ascii=False), "application/json"


def _html_looks_like_bytesize_jobs_page(content: str, base_url: str) -> bool:
    parsed = urlsplit(normalize_whitespace(base_url))
    if "career.thebytesize.ai" not in parsed.netloc.lower():
        return False
    path = normalize_whitespace(parsed.path).lower()
    if "/jobs" not in path:
        return False
    lowered = (content or "").lower()
    return "bytesize careers" in lowered and '<div id="root"></div>' in lowered


def _fetch_bytesize_public_json(url: str, settings) -> dict[str, Any]:
    timeout_seconds, connect_timeout_seconds = _source_timeout_values(settings, "html_page")
    content, _ = _fetch_remote(
        url,
        timeout_seconds,
        settings.user_agent,
        connect_timeout_seconds=connect_timeout_seconds,
    )
    payload = json.loads(content)
    return payload if isinstance(payload, dict) else {}


def _format_bytesize_experience(job: dict[str, Any]) -> str:
    title = normalize_whitespace(job.get("title"))
    min_year = job.get("min_career_year")
    max_year = job.get("max_career_year")
    if "신입 가능" in title:
        return "신입 가능"
    if "신입" in title and "경력" in title:
        return "신입 / 경력"
    if "신입" in title:
        return "신입"
    try:
        min_year_int = int(min_year) if min_year is not None else None
    except (TypeError, ValueError):
        min_year_int = None
    try:
        max_year_int = int(max_year) if max_year is not None else None
    except (TypeError, ValueError):
        max_year_int = None
    if min_year_int is not None and max_year_int is not None:
        return f"{min_year_int}~{max_year_int}년"
    if min_year_int is not None:
        return f"{min_year_int}년 이상"
    if max_year_int is not None:
        return f"{max_year_int}년 이하"
    return ""


def _ocr_text_to_description_html(text: str) -> str:
    lines = [normalize_whitespace(line) for line in text.splitlines() if normalize_whitespace(line)]
    if not lines:
        return ""
    escaped = "<br>".join(html.escape(line) for line in lines)
    return f"<div>{escaped}</div>"


def _fetch_bytesize_source(url: str, paths, settings) -> tuple[str, str]:
    list_payload = _fetch_bytesize_public_json(f"{_BYTESIZE_API_BASE_URL}/jobs", settings)
    entries = list_payload.get("result") or []
    if not isinstance(entries, list):
        entries = []

    jobs: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict) or bool(entry.get("is_pool")):
            continue
        job_id = normalize_whitespace(str(entry.get("id") or ""))
        title = normalize_whitespace(entry.get("title"))
        if not job_id or not title:
            continue
        if classify_job_role(title) not in ALLOWED_JOB_ROLES:
            continue
        detail_payload = _fetch_bytesize_public_json(f"{_BYTESIZE_API_BASE_URL}/jobs/{job_id}", settings)
        detail = detail_payload.get("result") if isinstance(detail_payload, dict) else {}
        detail = detail if isinstance(detail, dict) else {}
        asset_urls = detail.get("images") if isinstance(detail.get("images"), list) else []
        recovered_text = extract_text_from_asset_urls(
            asset_urls,
            user_agent=getattr(settings, "user_agent", "jobs-market-v2/0.1"),
            timeout_seconds=float(getattr(settings, "ats_source_timeout_seconds", 10.0)),
            connect_timeout_seconds=float(getattr(settings, "ats_source_connect_timeout_seconds", 3.0)),
        )
        description_html = _ocr_text_to_description_html(recovered_text)
        job_url = urljoin(f"{url.rstrip('/')}/", f"application?id={job_id}")
        if not description_html or job_url in seen_urls:
            continue
        seen_urls.add(job_url)
        jobs.append(
            {
                "title": normalize_whitespace(detail.get("title")) or title,
                "description_html": description_html,
                "job_url": job_url,
                "experience_level": _format_bytesize_experience(detail or entry),
                "location": "",
                "country": "",
                "requirements": "",
                "preferred": "",
                "core_skills": "",
            }
        )
    return json.dumps({"jobs": jobs}, ensure_ascii=False), "application/json"


def _fetch_recruiter_source(url: str, paths, settings, *, enable_ocr_recovery: bool = False) -> tuple[str, str]:
    timeout_seconds, connect_timeout_seconds = _source_timeout_values(settings, "recruiter")
    with httpx.Client(
        timeout=build_timeout(timeout_seconds, connect_timeout_seconds),
        follow_redirects=True,
        headers={"User-Agent": settings.user_agent},
    ) as client:
        root_url = _source_root_url(url)
        list_url = urljoin(root_url, "/app/jobnotice/list.json")
        page = 1
        page_size = 100
        items: list[dict] = []
        payload_mode = "state10"
        raw_seen_count = 0
        now_ms = int(time() * 1000)
        while True:
            request_payload = {"page": page, "pageSize": page_size}
            if payload_mode == "state10":
                request_payload["jobnoticeStateCode"] = "10"
            response = client.post(list_url, data=request_payload)
            response.raise_for_status()
            payload = response.json()
            raw_page_items = _extract_recruiter_list_items(payload)
            page_items = [item for item in raw_page_items if _recruiter_notice_is_open(item, now_ms)]
            if page == 1 and payload_mode == "state10" and not raw_page_items:
                fallback_response = client.post(
                    list_url,
                    data={"page": page, "pageSize": page_size},
                )
                fallback_response.raise_for_status()
                payload = fallback_response.json()
                raw_page_items = _extract_recruiter_list_items(payload)
                page_items = [item for item in raw_page_items if _recruiter_notice_is_open(item, now_ms)]
                payload_mode = "no_state"
            if not page_items:
                break
            items.extend(page_items)
            raw_seen_count += len(raw_page_items)
            total_count = int(payload.get("totalCount") or payload.get("jobnoticeTotalCount") or len(items))
            if len(raw_page_items) < page_size or raw_seen_count >= total_count:
                break
            page += 1

        def detail_fetcher(detail_url: str) -> str:
            detail_response = client.get(detail_url)
            detail_response.raise_for_status()
            return detail_response.text

        jobs = _build_recruiter_jobs_from_payload(
            {"list": items},
            root_url,
            detail_fetcher,
            paths=paths,
            settings=settings,
            enable_ocr_recovery=enable_ocr_recovery,
        )
    return json.dumps({"jobs": jobs}, ensure_ascii=False), "application/json"


def fetch_source_content(
    url: str,
    paths,
    settings,
    source_type: str = "html_page",
    *,
    enable_recruiter_ocr_recovery: bool = False,
) -> tuple[str, str]:
    if settings.use_mock_sources:
        mock_mapping = _load_mock_mapping(paths)
        if url in mock_mapping:
            entry = mock_mapping[url]
            fixture_path = paths.root / entry["file"]
            return fixture_path.read_text(encoding="utf-8"), entry.get("content_type", "text/html")
    if source_type == "greetinghr":
        return _fetch_greetinghr_source(url, paths, settings)
    if source_type == "recruiter":
        return _fetch_recruiter_source(url, paths, settings, enable_ocr_recovery=enable_recruiter_ocr_recovery)
    if source_type == "saramin_api":
        return _fetch_saramin_api_source(url, settings)
    if source_type == "worknet_api":
        return _fetch_worknet_api_source(url, settings)
    if source_type == "work24_public_html":
        return _fetch_work24_public_html_source(url, settings, paths=paths)
    if source_type == "html_page":
        saramin_payload = _fetch_saramin_relay_source(url, settings)
        if saramin_payload is not None:
            return saramin_payload
    timeout_seconds, connect_timeout_seconds = _source_timeout_values(settings, source_type)
    content, content_type = _fetch_remote(
        url,
        timeout_seconds,
        settings.user_agent,
        connect_timeout_seconds=connect_timeout_seconds,
    )
    if source_type == "html_page" and _html_looks_like_embedded_greetinghr(content):
        return _fetch_greetinghr_source(url, paths, settings)
    if source_type == "html_page" and _html_looks_like_embedded_ninehire(content):
        return _fetch_ninehire_source(url, paths, settings)
    if source_type == "html_page" and _html_looks_like_bytesize_jobs_page(content, url):
        return _fetch_bytesize_source(url, paths, settings)
    return content, content_type


def _is_collectable_source(row: dict[str, Any]) -> bool:
    return row.get("source_bucket") in {"approved", "candidate"} and not coerce_bool(row.get("is_quarantined"))


def _collectable_source_priority(row: dict[str, Any]) -> tuple[int, int, int, int, str, str]:
    company_name = canonicalize_company_name_for_jobs(row.get("company_name")) or normalize_whitespace(row.get("company_name"))
    signal_priority, signal_tiebreaker, active_job_tiebreaker = _source_collection_signal_priority(row)
    return (
        _SOURCE_COLLECTION_BUCKET_PRIORITY.get(normalize_whitespace(row.get("source_bucket")), 9),
        signal_priority,
        signal_tiebreaker,
        active_job_tiebreaker,
        company_name,
        normalize_whitespace(row.get("source_url")),
    )


def _default_source_collection_progress() -> dict[str, Any]:
    return {
        "next_source_offset": 0,
        "next_source_cursor": "",
        "registry_signature": "",
        "policy_version": "",
        "completed_full_scan_count": 0,
        "collectable_source_urls": [],
    }


def _load_source_collection_progress(paths) -> dict[str, Any]:
    progress_path = paths.source_collection_progress_path
    if not progress_path.exists():
        return _default_source_collection_progress()
    try:
        payload = json.loads(progress_path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return _default_source_collection_progress()
    if not isinstance(payload, dict):
        return _default_source_collection_progress()
    return {**_default_source_collection_progress(), **payload}


def _write_source_collection_progress(paths, payload: dict[str, Any]) -> None:
    progress_path = paths.source_collection_progress_path
    atomic_write_text(progress_path, json.dumps(payload, ensure_ascii=False))


def _source_collection_registry_signature(rows: list[dict[str, Any]]) -> str:
    return stable_hash(
        [_SOURCE_COLLECTION_PROGRESS_POLICY_VERSION]
        + [
            "|".join(
                (
                    normalize_whitespace(row.get("source_bucket")),
                    normalize_whitespace(row.get("source_type")),
                    normalize_whitespace(row.get("source_url")),
                )
            )
            for row in rows
            if _is_collectable_source(row)
        ]
    )


def _resume_source_scan_offset(
    progress: dict[str, Any],
    collectable_urls: list[str],
    *,
    registry_signature_changed: bool,
) -> tuple[int, str]:
    if not collectable_urls:
        return 0, "empty"

    next_offset = int(progress.get("next_source_offset", 0) or 0)
    cursor = normalize_whitespace(progress.get("next_source_cursor"))

    if registry_signature_changed:
        if next_offset > 0 and cursor and cursor in collectable_urls:
            return collectable_urls.index(cursor), "cursor"
        previous_urls = [
            normalize_whitespace(url)
            for url in progress.get("collectable_source_urls", [])
            if normalize_whitespace(url)
        ]
        if next_offset > 0 and previous_urls:
            remaining_urls = previous_urls[next_offset:]
            for url in remaining_urls:
                if url in collectable_urls:
                    return collectable_urls.index(url), "surviving_remaining_cursor"
        if next_offset > 0:
            return next_offset, "offset_after_registry_change"
        return 0, "reset_after_registry_change"

    if next_offset <= 0:
        return 0, "reset"

    if cursor and cursor in collectable_urls:
        return collectable_urls.index(cursor), "cursor"

    if next_offset > 0:
        return next_offset, "offset"
    return 0, "reset"


def collect_jobs_from_sources(
    source_registry: pd.DataFrame,
    paths,
    settings,
    *,
    run_id: str,
    snapshot_date: str,
    collected_at: str,
    enable_source_scan_progress: bool = False,
    enable_recruiter_ocr_recovery: bool = False,
) -> tuple[pd.DataFrame, list[dict], pd.DataFrame, dict]:
    jobs: list[dict] = []
    raw_records: list[dict] = []
    verified_success = 0
    verified_failure = 0
    rows: list[dict[str, Any]] = []
    for row in source_registry.fillna("").to_dict(orient="records"):
        canonical_company_name = canonicalize_company_name_for_jobs(row.get("company_name"))
        if canonical_company_name:
            row = {**row, "company_name": canonical_company_name}
        rows.append(row)

    collectable_positions = _ordered_collectable_positions(rows)
    total_collectable = len(collectable_positions)
    registry_signature = _source_collection_registry_signature(rows)
    selected_positions = collectable_positions
    start_offset = 0
    next_offset = 0
    next_cursor = ""
    runtime_budget_seconds = 0.0
    runtime_limited = False
    registry_signature_changed = False
    completed_full_scan_count = 0
    resume_strategy = "full_registry"

    if enable_source_scan_progress:
        progress_exists = paths.source_collection_progress_path.exists()
        progress = _load_source_collection_progress(paths)
        completed_full_scan_count = int(progress.get("completed_full_scan_count", 0) or 0)
        registry_signature_changed = bool(progress.get("registry_signature")) and progress.get("registry_signature") != registry_signature
        collectable_urls = [normalize_whitespace(rows[position].get("source_url")) for position in collectable_positions]
        progress_policy_changed = progress_exists and normalize_whitespace(progress.get("policy_version")) != _SOURCE_COLLECTION_PROGRESS_POLICY_VERSION
        if progress_policy_changed:
            start_offset = 0
            resume_strategy = "policy_reset"
        else:
            start_offset, resume_strategy = _resume_source_scan_offset(
                progress,
                collectable_urls,
                registry_signature_changed=registry_signature_changed,
            )
        if start_offset >= total_collectable:
            start_offset = 0
            resume_strategy = "wrap_to_zero"
        batch_size = int(getattr(settings, "job_collection_source_batch_size", total_collectable) or total_collectable)
        max_batches = int(getattr(settings, "job_collection_source_max_batches_per_run", 1) or 1)
        process_limit = max(batch_size, 0) * max(max_batches, 0)
        runtime_budget_seconds = float(getattr(settings, "job_collection_max_runtime_seconds", 0.0) or 0.0)
        if process_limit > 0 and total_collectable > process_limit:
            selected_positions, cursor_selected_positions, pinned_positions = _select_incremental_collectable_positions(
                collectable_positions,
                rows,
                start_offset=start_offset,
                process_limit=process_limit,
            )
        else:
            selected_positions = collectable_positions[start_offset:]
            cursor_selected_positions = selected_positions
            pinned_positions = []
    else:
        cursor_selected_positions = selected_positions
        pinned_positions = []

    deadline = monotonic() + runtime_budget_seconds if runtime_budget_seconds > 0 else None
    updated_rows: dict[int, dict[str, Any]] = {}
    processed_count = 0
    cursor_selected_set = set(cursor_selected_positions)
    cursor_processed_count = 0
    html_listing_gemini_budget = GeminiBudget(
        max_calls=int(getattr(settings, "gemini_html_listing_max_calls_per_run", 12) or 12)
    )
    content_refine_gemini_budget = GeminiBudget(
        max_calls=int(getattr(settings, "gemini_role_salvage_max_calls_per_run", 0) or 0)
    )

    for position in selected_positions:
        row = rows[position]
        try:
            content, content_type = fetch_source_content(
                row["source_url"],
                paths,
                settings,
                row.get("source_type", "html_page"),
                enable_recruiter_ocr_recovery=enable_recruiter_ocr_recovery,
            )
            parsed_jobs = parse_jobs_from_payload(
                content,
                content_type,
                row.get("source_type", "html_page"),
                base_url=row.get("source_url", ""),
                settings=settings,
                paths=paths,
                gemini_budget=html_listing_gemini_budget,
            )
            if normalize_whitespace(row.get("source_type")) == "html_page":
                parsed_jobs = _hydrate_html_job_details(
                    parsed_jobs,
                    row,
                    settings=settings,
                    paths=paths,
                )
            active_count = 0
            for raw_job in parsed_jobs:
                normalized_job, raw_detail = normalize_job_payload(
                    raw_job,
                    row,
                    run_id,
                    snapshot_date,
                    collected_at,
                    settings=settings,
                    paths=paths,
                    gemini_budget=content_refine_gemini_budget,
                    refine_with_gemini=True,
                )
                if normalized_job:
                    active_count += 1
                    jobs.append(normalized_job)
                    raw_records.append(raw_detail)
            updated_rows[position] = {
                **row,
                "verification_status": "성공",
                "failure_count": 0,
                "last_success_at": collected_at,
                "last_active_job_count": active_count,
            }
            verified_success += 1
        except Exception as exc:  # noqa: BLE001
            updated_rows[position] = {
                **row,
                "verification_status": "실패",
                "failure_count": int(row.get("failure_count") or 0) + 1,
                "quarantine_reason": row.get("quarantine_reason") or "",
                "last_active_job_count": int(row.get("last_active_job_count") or 0),
            }
            verified_failure += 1
            raw_records.append(
                {
                    "run_id": run_id,
                    "snapshot_date": snapshot_date,
                    "job_key": "",
                    "source_url": row.get("source_url"),
                    "company_name": row.get("company_name"),
                    "job_title_raw": "",
                    "raw_payload_json": dump_json({"error": str(exc)}),
                }
            )
        processed_count += 1
        if position in cursor_selected_set:
            cursor_processed_count += 1
        if deadline is not None and monotonic() >= deadline:
            runtime_limited = (
                processed_count < len(selected_positions)
                or (start_offset + cursor_processed_count) < total_collectable
            )
            break

    if enable_source_scan_progress:
        actual_end_offset = start_offset + cursor_processed_count
        completed_full_source_scan = total_collectable == 0 or actual_end_offset >= total_collectable
        next_offset = 0 if completed_full_source_scan else actual_end_offset
        if total_collectable > 0 and completed_full_source_scan:
            completed_full_scan_count += 1
        if 0 < next_offset < total_collectable:
            next_cursor = normalize_whitespace(rows[collectable_positions[next_offset]].get("source_url"))
        _write_source_collection_progress(
            paths,
            {
                "next_source_offset": int(next_offset),
                "next_source_cursor": next_cursor,
                "registry_signature": registry_signature,
                "policy_version": _SOURCE_COLLECTION_PROGRESS_POLICY_VERSION,
                "completed_full_scan_count": int(completed_full_scan_count),
                "collectable_source_urls": collectable_urls,
                "last_run_id": run_id,
                "last_completed_at": collected_at,
            },
        )
    else:
        completed_full_source_scan = True
        next_offset = 0

    updated_sources = [updated_rows.get(index, row) for index, row in enumerate(rows)]
    deferred_count = max(len(selected_positions) - processed_count, 0)
    pending_count = 0 if not enable_source_scan_progress else max(total_collectable - (start_offset + cursor_processed_count), 0)

    refined_jobs = _refine_jobs_with_gemini(jobs, settings, paths)
    jobs_frame = pd.DataFrame(refined_jobs, columns=list(JOB_COLUMNS))
    if not jobs_frame.empty:
        jobs_frame = jobs_frame.drop_duplicates(subset=["job_key"], keep="last").reset_index(drop=True)
    updated_source_frame = pd.DataFrame(updated_sources)
    for column in SOURCE_REGISTRY_COLUMNS:
        if column not in updated_source_frame.columns:
            updated_source_frame[column] = None
    summary = {
        "collection_mode": "mock" if settings.use_mock_sources else "live",
        "collected_job_count": int(len(jobs_frame)),
        "verified_source_success_count": int(verified_success),
        "verified_source_failure_count": int(verified_failure),
        "source_scan_mode": "incremental_cursor" if enable_source_scan_progress else "full_registry",
        "total_collectable_source_count": int(total_collectable),
        "selected_collectable_source_count": int(len(selected_positions)),
        "processed_collectable_source_count": int(processed_count),
        "cursor_selected_collectable_source_count": int(len(cursor_selected_positions)),
        "cursor_processed_collectable_source_count": int(cursor_processed_count),
        "pinned_collectable_source_count": int(len(pinned_positions)),
        "deferred_collectable_source_count": int(deferred_count),
        "pending_collectable_source_count": int(pending_count),
        "source_scan_start_offset": int(start_offset),
        "source_scan_next_offset": int(next_offset),
        "source_scan_completed_full_pass_count": int(completed_full_scan_count),
        "completed_full_source_scan": bool(completed_full_source_scan),
        "source_scan_runtime_budget_seconds": float(runtime_budget_seconds),
        "source_scan_runtime_limited": bool(runtime_limited),
        "source_scan_registry_signature_changed": bool(registry_signature_changed),
        "source_scan_resume_strategy": resume_strategy,
    }
    return jobs_frame, raw_records, updated_source_frame[list(SOURCE_REGISTRY_COLUMNS)], summary


def _normalize_source_outcomes(source_statuses: set[str] | dict[str, str] | None) -> dict[str, str]:
    if not source_statuses:
        return {}
    if isinstance(source_statuses, set):
        return {
            normalize_whitespace(source_url): "success"
            for source_url in source_statuses
            if normalize_whitespace(source_url)
        }
    normalized: dict[str, str] = {}
    for source_url, outcome in source_statuses.items():
        normalized_url = normalize_whitespace(source_url)
        normalized_outcome = normalize_whitespace(outcome).lower()
        if not normalized_url or normalized_outcome not in {"success", "failure"}:
            continue
        normalized[normalized_url] = normalized_outcome
    return normalized


def merge_incremental(
    master_frame: pd.DataFrame,
    new_frame: pd.DataFrame,
    source_statuses: set[str] | dict[str, str] | None,
    run_id: str,
    snapshot_date: str,
    collected_at: str,
) -> pd.DataFrame:
    if master_frame.empty:
        bootstrap = new_frame.copy()
        if not bootstrap.empty:
            bootstrap["job_key"] = [canonicalize_job_key(row) for row in bootstrap.fillna("").to_dict(orient="records")]
            bootstrap = bootstrap.drop_duplicates(subset=["job_key"], keep="last").reset_index(drop=True)
        bootstrap["record_status"] = "신규"
        bootstrap["snapshot_date"] = snapshot_date
        bootstrap["run_id"] = run_id
        return bootstrap[list(JOB_COLUMNS)]

    source_outcomes = _normalize_source_outcomes(source_statuses)

    current_by_key = {}
    for row in master_frame.fillna("").to_dict(orient="records"):
        effective_key = canonicalize_job_key(row)
        row["job_key"] = effective_key
        current_by_key[effective_key] = row
    new_by_key = {}
    for row in new_frame.fillna("").to_dict(orient="records"):
        effective_key = canonicalize_job_key(row)
        row["job_key"] = effective_key
        new_by_key[effective_key] = row

    merged_rows: list[dict] = []
    for job_key, row in new_by_key.items():
        previous = current_by_key.get(job_key)
        if previous is None:
            row["record_status"] = "신규"
        else:
            row["first_seen_at"] = previous.get("first_seen_at") or row["first_seen_at"]
            row["missing_count"] = 0
            row["is_active"] = True
            row["record_status"] = "변경" if previous.get("change_hash") != row.get("change_hash") else "유지"
        row["last_seen_at"] = collected_at
        row["snapshot_date"] = snapshot_date
        row["run_id"] = run_id
        merged_rows.append({column: row.get(column, "") for column in JOB_COLUMNS})

    for job_key, previous in current_by_key.items():
        if job_key in new_by_key:
            continue
        carry = previous.copy()
        carry["run_id"] = run_id
        carry["snapshot_date"] = snapshot_date
        source_outcome = source_outcomes.get(normalize_whitespace(carry.get("source_url")))
        if source_outcome == "success":
            carry["missing_count"] = int(carry.get("missing_count") or 0) + 1
            carry["is_active"] = int(carry["missing_count"]) < 2
            carry["record_status"] = "미발견"
        elif source_outcome == "failure":
            carry["record_status"] = "검증실패보류"
        elif coerce_bool(carry.get("is_active")):
            carry["record_status"] = "유지"
        merged_rows.append({column: carry.get(column, "") for column in JOB_COLUMNS})

    merged = pd.DataFrame(merged_rows, columns=list(JOB_COLUMNS))
    if not merged.empty:
        fragmentized_job_bases = {
            (
                normalize_whitespace(row.get("source_url")),
                normalize_whitespace(row.get("job_url")).split("#", 1)[0],
                canonicalize_company_name_for_jobs(row.get("company_name")),
            )
            for row in merged.fillna("").to_dict(orient="records")
            if "#role-" in normalize_whitespace(row.get("job_url"))
        }
        if fragmentized_job_bases:
            filtered_rows: list[dict[str, Any]] = []
            for row in merged.fillna("").to_dict(orient="records"):
                job_url = normalize_whitespace(row.get("job_url"))
                if (
                    job_url
                    and "#role-" not in job_url
                    and normalize_whitespace(row.get("record_status")) in {"미발견", "검증실패보류"}
                    and (
                        normalize_whitespace(row.get("source_url")),
                        job_url,
                        canonicalize_company_name_for_jobs(row.get("company_name")),
                    )
                    in fragmentized_job_bases
                ):
                    continue
                filtered_rows.append({column: row.get(column, "") for column in JOB_COLUMNS})
            merged = pd.DataFrame(filtered_rows, columns=list(JOB_COLUMNS))
        merged = merged.drop_duplicates(subset=["job_key"], keep="last").reset_index(drop=True)
        merged = _refresh_merged_job_rows(merged)
    return merged.sort_values(["company_name", "job_role", "job_key"]).reset_index(drop=True)
