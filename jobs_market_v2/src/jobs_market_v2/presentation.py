"""Display and analysis text normalization helpers."""

from __future__ import annotations

import re

import pandas as pd

from .constants import DISPLAY_FIELDS
from .html_utils import extract_sections_from_description
from .utils import contains_english, has_hangul, normalize_whitespace


_LATIN_BLOCK_RE = re.compile(r"[A-Za-z][A-Za-z0-9\s&+/\-]*")
_LATIN_TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z0-9+#./_-]*")
_EMPTY_BRACKETS_RE = re.compile(r"[\(\[\{]\s*[\)\]\}]")
_SPACE_BEFORE_PUNCT_RE = re.compile(r"\s+([,.;:])")
_PUNCT_ONLY_LINE_RE = re.compile(r"^[,.;:/\-\"'“”‘’\s]+$")
_KOREAN_TOKEN_RE = re.compile(r"[가-힣0-9]{2,}")
_ENGLISH_WORD_RE = re.compile(r"[A-Za-z]{2,}")
_BRACKET_HEADING_LINE_RE = re.compile(r"^\[[가-힣0-9\s]+\]$")
_SHORT_HEADING_LINE_RE = re.compile(r"^[가-힣0-9\s/&()\-]{2,28}:$")
_EMOJI_RE = re.compile(r"[\U00010000-\U0010ffff]")
_ENGLISH_INTERN_RE = re.compile(r"\bintern(?:ship)?\b", flags=re.IGNORECASE)
_SPECIAL_TRACK_PATTERNS: tuple[tuple[str, tuple[re.Pattern[str], ...]], ...] = (
    (
        "전문연구요원",
        (
            re.compile(r"전문연구요원"),
            re.compile(r"병역특례"),
            re.compile(r"전문연\s*(?:지원\s*가능|지원가능|가능|편입|전직)"),
        ),
    ),
    (
        "계약직",
        (
            re.compile(r"계약직"),
            re.compile(r"기간제"),
            re.compile(r"\bfixed[- ]term\b", flags=re.IGNORECASE),
            re.compile(r"\bcontract(?:\s+(?:role|position|employment|employee))?\b", flags=re.IGNORECASE),
        ),
    ),
)
_DEGREE_TRACK_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("박사", re.compile(r"박사")),
    ("석사", re.compile(r"석사")),
    ("학사", re.compile(r"학사")),
)
_DEFAULT_HIRING_TRACK = "일반채용"
_DEFAULT_EXPERIENCE_DISPLAY = "미기재"
_DEFAULT_EXPERIENCE_SOURCE = "표시기본값"
_DEFAULT_PREFERRED_DISPLAY = "별도 우대사항 미기재"


def _acronym_pattern(token: str) -> re.Pattern[str]:
    return re.compile(rf"(?<![A-Za-z]){re.escape(token)}(?![A-Za-z])", flags=re.IGNORECASE)

_LETTER_TO_KOREAN = {
    "A": "에이",
    "B": "비",
    "C": "씨",
    "D": "디",
    "E": "이",
    "F": "에프",
    "G": "지",
    "H": "에이치",
    "I": "아이",
    "J": "제이",
    "K": "케이",
    "L": "엘",
    "M": "엠",
    "N": "엔",
    "O": "오",
    "P": "피",
    "Q": "큐",
    "R": "알",
    "S": "에스",
    "T": "티",
    "U": "유",
    "V": "브이",
    "W": "더블유",
    "X": "엑스",
    "Y": "와이",
    "Z": "지",
}

_TERM_REPLACEMENTS = (
    (re.compile(r"\bA/B\s*테스트\b", flags=re.IGNORECASE), "에이비 테스트"),
    (re.compile(r"\bPower\s*BI\b", flags=re.IGNORECASE), "파워비아이"),
    (re.compile(r"\bBusiness\s+Intelligence\b", flags=re.IGNORECASE), "비즈니스 인텔리전스"),
    (re.compile(r"\bMachine\s+Learning\b", flags=re.IGNORECASE), "머신러닝"),
    (re.compile(r"\bDeep\s+Learning\b", flags=re.IGNORECASE), "딥러닝"),
    (re.compile(r"\bData\s+Pipeline\b", flags=re.IGNORECASE), "데이터 파이프라인"),
    (re.compile(r"\bData\s+Warehouse\b", flags=re.IGNORECASE), "데이터 웨어하우스"),
    (re.compile(r"\bPrompt\s+Engineering\b", flags=re.IGNORECASE), "프롬프트 엔지니어링"),
    (re.compile(r"\bFine[\s-]?Tuning\b", flags=re.IGNORECASE), "파인튜닝"),
    (_acronym_pattern("SQL"), "에스큐엘"),
    (_acronym_pattern("API"), "에이피아이"),
    (_acronym_pattern("SDK"), "에스디케이"),
    (_acronym_pattern("DB"), "디비"),
    (_acronym_pattern("BI"), "비아이"),
    (_acronym_pattern("ETL"), "이티엘"),
    (_acronym_pattern("ELT"), "이엘티"),
    (_acronym_pattern("GPU"), "지피유"),
    (_acronym_pattern("CPU"), "씨피유"),
    (_acronym_pattern("AWS"), "에이더블유에스"),
    (_acronym_pattern("GCP"), "지씨피"),
    (re.compile(r"\bAzure\b", flags=re.IGNORECASE), "애저"),
    (re.compile(r"\bPython\b", flags=re.IGNORECASE), "파이썬"),
    (re.compile(r"\bPyTorch\b", flags=re.IGNORECASE), "파이토치"),
    (re.compile(r"\bTensorFlow\b", flags=re.IGNORECASE), "텐서플로"),
    (re.compile(r"\bSpark\b", flags=re.IGNORECASE), "스파크"),
    (re.compile(r"\bAirflow\b", flags=re.IGNORECASE), "에어플로"),
    (re.compile(r"\bDocker\b", flags=re.IGNORECASE), "도커"),
    (re.compile(r"\bKubernetes\b", flags=re.IGNORECASE), "쿠버네티스"),
    (re.compile(r"\bKafka\b", flags=re.IGNORECASE), "카프카"),
    (re.compile(r"\bHadoop\b", flags=re.IGNORECASE), "하둡"),
    (re.compile(r"\bTableau\b", flags=re.IGNORECASE), "태블로"),
    (re.compile(r"\bLooker\b", flags=re.IGNORECASE), "루커"),
    (re.compile(r"\bSnowflake\b", flags=re.IGNORECASE), "스노우플레이크"),
    (re.compile(r"\bDatabricks\b", flags=re.IGNORECASE), "데이터브릭스"),
    (re.compile(r"\bMLOps\b", flags=re.IGNORECASE), "엠엘옵스"),
    (_acronym_pattern("LLM"), "엘엘엠"),
    (_acronym_pattern("RAG"), "검색증강생성"),
    (_acronym_pattern("OCR"), "광학문자인식"),
    (_acronym_pattern("NLP"), "엔엘피"),
    (_acronym_pattern("CV"), "컴퓨터비전"),
    (_acronym_pattern("ML"), "머신러닝"),
    (_acronym_pattern("AI"), "인공지능"),
)

_CORE_SKILL_PATTERNS = (
    (re.compile(r"\bPython\b", flags=re.IGNORECASE), "파이썬"),
    (_acronym_pattern("SQL"), "에스큐엘"),
    (_acronym_pattern("HQL"), "에이치큐엘"),
    (re.compile(r"\bPyTorch\b", flags=re.IGNORECASE), "파이토치"),
    (re.compile(r"\bTensorFlow\b", flags=re.IGNORECASE), "텐서플로"),
    (re.compile(r"\bSpark\b", flags=re.IGNORECASE), "스파크"),
    (re.compile(r"\bAirflow\b", flags=re.IGNORECASE), "에어플로"),
    (re.compile(r"\bKafka\b", flags=re.IGNORECASE), "카프카"),
    (re.compile(r"\bHadoop\b", flags=re.IGNORECASE), "하둡"),
    (re.compile(r"\bTableau\b", flags=re.IGNORECASE), "태블로"),
    (re.compile(r"\bPower\s*BI\b", flags=re.IGNORECASE), "파워 비아이"),
    (re.compile(r"\bLooker\b", flags=re.IGNORECASE), "루커"),
    (re.compile(r"\bExcel\b", flags=re.IGNORECASE), "엑셀"),
    (_acronym_pattern("AWS"), "에이더블유에스"),
    (_acronym_pattern("GCP"), "지씨피"),
    (re.compile(r"\bAzure\b", flags=re.IGNORECASE), "애저"),
    (re.compile(r"\bDocker\b", flags=re.IGNORECASE), "도커"),
    (re.compile(r"\bKubernetes\b", flags=re.IGNORECASE), "쿠버네티스"),
    (re.compile(r"\bSnowflake\b", flags=re.IGNORECASE), "스노우플레이크"),
    (re.compile(r"\bDatabricks\b", flags=re.IGNORECASE), "데이터브릭스"),
    (re.compile(r"\bBigQuery\b", flags=re.IGNORECASE), "빅쿼리"),
    (re.compile(r"\bRedshift\b", flags=re.IGNORECASE), "레드시프트"),
    (_acronym_pattern("ETL"), "이티엘"),
    (_acronym_pattern("ELT"), "이엘티"),
    (re.compile(r"\bMLOps\b", flags=re.IGNORECASE), "엠엘옵스"),
    (re.compile(r"\bMLflow\b", flags=re.IGNORECASE), "엠엘플로"),
    (_acronym_pattern("LLM"), "엘엘엠"),
    (_acronym_pattern("RAG"), "검색증강생성"),
    (_acronym_pattern("NLP"), "엔엘피"),
    (_acronym_pattern("OCR"), "광학문자인식"),
    (_acronym_pattern("CV"), "컴퓨터비전"),
    (_acronym_pattern("GPU"), "지피유"),
    (_acronym_pattern("ML"), "머신러닝"),
    (_acronym_pattern("AI"), "인공지능"),
    (re.compile(r"\bComputer\s+Vision\b", flags=re.IGNORECASE), "컴퓨터비전"),
    (re.compile(r"머신러닝"), "머신러닝"),
    (re.compile(r"딥러닝"), "딥러닝"),
    (re.compile(r"엘엘엠"), "엘엘엠"),
    (re.compile(r"검색증강생성"), "검색증강생성"),
    (re.compile(r"멀티모달"), "멀티모달"),
    (re.compile(r"강화학습"), "강화학습"),
    (re.compile(r"인과\s*추론"), "인과 추론"),
    (re.compile(r"게임\s*이론"), "게임 이론"),
    (re.compile(r"추천\s*시스템"), "추천 시스템"),
    (re.compile(r"지피유"), "지피유"),
    (re.compile(r"\bGitHub\b", flags=re.IGNORECASE), "깃허브"),
    (re.compile(r"\bGit\b", flags=re.IGNORECASE), "깃"),
    (re.compile(r"\bLangChain\b", flags=re.IGNORECASE), "랭체인"),
    (re.compile(r"\bVector\s+DB\b", flags=re.IGNORECASE), "벡터 데이터베이스"),
    (re.compile(r"\bElasticsearch\b", flags=re.IGNORECASE), "엘라스틱서치"),
    (re.compile(r"\bRedis\b", flags=re.IGNORECASE), "레디스"),
)

_GENERIC_SECTION_TAILS = {
    "경험",
    "활용 경험",
    "테스트 경험",
    "경험 우대",
    "구축 경험",
    "운영 경험",
    "개발 경험",
}

_LEGAL_NOTICE_PATTERNS = (
    re.compile(r"공평한 기회"),
    re.compile(r"열려 있는 회사"),
    re.compile(r"개인정보"),
    re.compile(r"문서 반환"),
    re.compile(r"전형 절차"),
    re.compile(r"채용 절차"),
    re.compile(r"지원서"),
    re.compile(r"privacy notice", flags=re.IGNORECASE),
    re.compile(r"document return", flags=re.IGNORECASE),
    re.compile(r"recruitment process", flags=re.IGNORECASE),
    re.compile(r"application review", flags=re.IGNORECASE),
    re.compile(r"equal opportunit", flags=re.IGNORECASE),
    re.compile(r"국가보훈"),
    re.compile(r"보훈대상자"),
    re.compile(r"취업 보호 대상자"),
    re.compile(r"관계 법령"),
    re.compile(r"증빙서류"),
    re.compile(r"지원 시 고지"),
    re.compile(r"우대하오니"),
    re.compile(r"채용우대"),
)

_CTA_LINE_PATTERNS = (
    re.compile(r"자세히보기"),
    re.compile(r"소개 영상"),
    re.compile(r"궁금하다면"),
    re.compile(r"확인하기$"),
    re.compile(r"합류하면 담당할 업무예요"),
    re.compile(r"이런 분과 함께 하고 싶어요"),
    re.compile(r"이런 일을 함께합니다"),
    re.compile(r"이런 분을 찾습니다"),
    re.compile(r"이런 분이면 더 좋습니다"),
    re.compile(r"기업소개"),
    re.compile(r"담당업무"),
    re.compile(r"자격요건"),
    re.compile(r"우대사항"),
    re.compile(r"보유역량 및 필수요건"),
    re.compile(r"함께하게 될 .*소개합니다"),
    re.compile(r"소개합니다!?$"),
    re.compile(r"소개해요$"),
)

_SECTION_TERMINATION_PATTERNS = (
    re.compile(r"^근무 조건$"),
    re.compile(r"^복지 및 혜택$"),
    re.compile(r"^채용\s*절차$"),
    re.compile(r"^전형\s*절차$"),
    re.compile(r"^서류\s*반환\s*정책$"),
    re.compile(r"^기타사항$"),
    re.compile(r"^참고\s*사항$"),
    re.compile(r"^이렇게 합류해요$"),
    re.compile(r"^이렇게 근무해요$"),
    re.compile(r"^꼭 확인해 주세요$"),
    re.compile(r"^제출 서류"),
)

_MAIN_TASK_HEADING_PATTERNS = (
    re.compile(r"주요업무"),
    re.compile(r"담당업무"),
    re.compile(r"직무\s*내용"),
    re.compile(r"직무\s*상세"),
    re.compile(r"이런 일을 해요"),
    re.compile(r"이런 일을 함께 합니다"),
    re.compile(r"이런 일을 함께합니다"),
    re.compile(r"key responsibilities", flags=re.IGNORECASE),
    re.compile(r"responsibilities", flags=re.IGNORECASE),
    re.compile(r"responsibilit", flags=re.IGNORECASE),
    re.compile(r"what you(?:'| wi)ll do", flags=re.IGNORECASE),
    re.compile(r"mission of the role", flags=re.IGNORECASE),
    re.compile(r"about the role", flags=re.IGNORECASE),
)

_REQUIREMENTS_HEADING_PATTERNS = (
    re.compile(r"자격요건"),
    re.compile(r"지원\s*자격"),
    re.compile(r"지원자격"),
    re.compile(r"공통자격"),
    re.compile(r"필수요건"),
    re.compile(r"이런 분과 함께하고 싶어요"),
    re.compile(r"이런 분을 찾습니다"),
    re.compile(r"이런 분을 원해요"),
    re.compile(r"이런 분과 함께 하고 싶어요"),
    re.compile(r"requirements?", flags=re.IGNORECASE),
    re.compile(r"qualifications?", flags=re.IGNORECASE),
    re.compile(r"how do i know if the role is right for me", flags=re.IGNORECASE),
    re.compile(r"what we(?:'| a)re looking for", flags=re.IGNORECASE),
)

_PREFERRED_HEADING_PATTERNS = (
    re.compile(r"우대사항"),
    re.compile(r"선호(?:사항)?"),
    re.compile(r"이런 분이면 더 좋아요"),
    re.compile(r"이런 분이면 더 좋습니다"),
    re.compile(r"이런 분이라면 더 좋아요"),
    re.compile(r"보유하셨다면\s*좋아요"),
    re.compile(r"있(?:다면|으시면)\s*좋아요"),
    re.compile(r"경험(?:을)?\s*보유하셨다면\s*좋아요"),
    re.compile(r"preferred", flags=re.IGNORECASE),
    re.compile(r"preferred qualifications?", flags=re.IGNORECASE),
    re.compile(r"nice to have", flags=re.IGNORECASE),
    re.compile(r"plus if", flags=re.IGNORECASE),
    re.compile(r"bonus points?", flags=re.IGNORECASE),
    re.compile(r"would be a plus", flags=re.IGNORECASE),
    re.compile(r"you may be a good fit if", flags=re.IGNORECASE),
    re.compile(r"what would make you stand out", flags=re.IGNORECASE),
)

_STRONG_EXPERIENCE_SIGNAL_PATTERNS = (
    re.compile(r"실무\s*(?:개발|운영|분석|연구|구축)?\s*경험"),
    re.compile(r"제품을\s*개선해본\s*경험"),
    re.compile(r"비즈니스\s*성과를\s*창출한\s*경험"),
    re.compile(r"설계(?:·|/|,|\s)*(?:구현|운영|배포|구축|검증)"),
    re.compile(r"프로덕션\s*환경"),
    re.compile(r"상용\s*서비스"),
    re.compile(r"실제로\s*(?:설계|운영|개선|구축|리딩)"),
    re.compile(r"(?:운영|배포|구축|리딩|주도)\s*해본\s*경험"),
    re.compile(r"전\s*과정을\s*직접\s*수행한\s*경험"),
    re.compile(r"전사\s*주요\s*지표를\s*정의(?:하고| 및)\s*(?:관리|운영)"),
    re.compile(r"오너십을\s*갖고"),
    re.compile(r"\bproduction(?:ize|ized|izing)?\b", flags=re.IGNORECASE),
    re.compile(r"\bshipp?ed\b", flags=re.IGNORECASE),
    re.compile(r"\blaunched\b", flags=re.IGNORECASE),
    re.compile(r"\boperat(?:e|ed|ing)\b", flags=re.IGNORECASE),
    re.compile(r"\bowned\b", flags=re.IGNORECASE),
    re.compile(r"\bend-to-end\b", flags=re.IGNORECASE),
)
_RESEARCH_EXPERIENCE_TRACK_PATTERNS = (
    re.compile(r"전문연구요원"),
    re.compile(r"병역특례"),
    re.compile(r"\bresearch (?:engineer|scientist|researcher)\b", flags=re.IGNORECASE),
    re.compile(r"\bscientist\b", flags=re.IGNORECASE),
    re.compile(r"연구원"),
    re.compile(r"리서처"),
    re.compile(r"\br&d\b", flags=re.IGNORECASE),
    re.compile(r"연구개발"),
)
_RESEARCH_EXPERIENCE_WORK_PATTERNS = (
    re.compile(r"석사"),
    re.compile(r"박사"),
    re.compile(r"논문"),
    re.compile(r"저널"),
    re.compile(r"학회"),
    re.compile(r"게재"),
    re.compile(r"투고"),
    re.compile(r"모델\s*(?:연구|고도화|최적화)"),
    re.compile(r"알고리즘\s*(?:연구|고도화|최적화)"),
    re.compile(r"생체신호"),
    re.compile(r"연구\s*동향"),
    re.compile(r"실증"),
)

_DETAIL_PREFER_SECTION_PATTERNS = (
    re.compile(r"소개"),
    re.compile(r"자세히"),
    re.compile(r"인터뷰"),
    re.compile(r"성과"),
    re.compile(r"복지"),
    re.compile(r"근무\s*조건"),
    re.compile(r"채용\s*절차"),
    re.compile(r"전형\s*절차"),
    re.compile(r"꼭 확인"),
    re.compile(r"합류해요"),
    re.compile(r"20\d{2}년"),
)

_ACRONYM_ARTIFACT_TOKENS = {
    "엘아이엔케이",
    "비에이에스아이씨",
    "피알이에프이알알이디",
    "피알",
}

_CORE_SKILL_NOISE_PATTERNS = (
    re.compile(r"경력"),
    re.compile(r"년 이상"),
    re.compile(r"석사"),
    re.compile(r"박사"),
    re.compile(r"학위"),
    re.compile(r"우대"),
    re.compile(r"필수"),
    re.compile(r"문제 해결"),
    re.compile(r"협업"),
    re.compile(r"의사소통"),
    re.compile(r"커뮤니케이션"),
    re.compile(r"리더십"),
    re.compile(r"자율성"),
    re.compile(r"호기심"),
    re.compile(r"책임감"),
    re.compile(r"주도적"),
    re.compile(r"^영어$"),
    re.compile(r"^한국어$"),
)

_GENERIC_SIGNAL_TOKENS = {
    "경험",
    "활용",
    "테스트",
    "우대",
    "필요",
    "가능",
    "역량",
    "이해",
    "업무",
    "개발",
    "운영",
    "분석",
    "수행",
    "보유",
    "관련",
    "기반",
    "분야",
    "실무",
    "지원",
    "경력",
}

_TOKEN_SUFFIXES = (
    "해야합니다",
    "필요합니다",
    "우대합니다",
    "가능합니다",
    "수행합니다",
    "합니다",
    "됩니다",
    "입니다",
    "있습니다",
    "으로",
    "에서",
    "에게",
    "까지",
    "부터",
    "보다",
    "처럼",
    "이며",
    "하고",
    "하며",
    "해서",
    "하는",
    "되는",
    "적인",
    "관련",
    "경험이",
    "경험과",
    "경험을",
    "기술을",
    "역량을",
    "능력을",
    "업무를",
    "은",
    "는",
    "이",
    "가",
    "을",
    "를",
    "와",
    "과",
    "의",
    "에",
    "도",
    "만",
    "로",
)


def _localize_common_terms(text: str) -> str:
    localized = text
    for pattern, replacement in _TERM_REPLACEMENTS:
        localized = pattern.sub(replacement, localized)
    return localized


def _transliterate_acronym(token: str) -> str:
    parts = re.split(r"([/-])", token.upper())
    syllables: list[str] = []
    for part in parts:
        if not part or part in {"/", "-"}:
            continue
        if not all(character in _LETTER_TO_KOREAN for character in part):
            return ""
        syllables.append("".join(_LETTER_TO_KOREAN[character] for character in part))
    return "".join(syllables)


def _translate_english_token(token: str) -> str:
    if not token:
        return ""
    if re.fullmatch(r"[A-Z]{2,10}", token):
        return _transliterate_acronym(token)
    if re.fullmatch(r"[A-Z]{1,6}([/-][A-Z]{1,6})+", token):
        return _transliterate_acronym(token)
    return ""


def sanitize_name_or_title_text(value: str | None, *, unknown_name: bool = False, allow_english: bool = False) -> str:
    text = normalize_whitespace(value)
    if not text:
        return "한글명 미확인" if unknown_name else ""
    if allow_english:
        return text
    text = _LATIN_BLOCK_RE.sub(" ", text)
    text = normalize_whitespace(text)
    if contains_english(text):
        return "한글명 미확인" if unknown_name else ""
    if not has_hangul(text):
        return "한글명 미확인" if unknown_name else ""
    return text


def _coerce_section_input(value) -> str:
    if isinstance(value, (list, tuple, set)):
        parts = [normalize_whitespace(str(item)) for item in value if normalize_whitespace(str(item))]
        return "\n".join(parts)
    return normalize_whitespace(value)


def _looks_like_legal_notice_line(value: str | None) -> bool:
    text = normalize_whitespace(value)
    if not text:
        return False
    return any(pattern.search(text) for pattern in _LEGAL_NOTICE_PATTERNS)


def _looks_like_heading_artifact_line(value: str | None) -> bool:
    text = normalize_whitespace(value)
    if not text:
        return False
    if text in {"이런 업무를 해요", "이런분이 적합해요", "이런 분이면 더욱 좋아요"}:
        return True
    if _BRACKET_HEADING_LINE_RE.fullmatch(text):
        return True
    if _SHORT_HEADING_LINE_RE.fullmatch(text):
        return True
    tokens = [token for token in re.split(r"[\s,()/:-]+", text.strip("[] ")) if token]
    if tokens and len("".join(tokens)) <= 28 and all(token in _ACRONYM_ARTIFACT_TOKENS for token in tokens):
        return True
    return False


def _looks_like_cta_line(value: str | None) -> bool:
    text = normalize_whitespace(value)
    if not text:
        return False
    if any(pattern.search(text) for pattern in _CTA_LINE_PATTERNS):
        return True
    return False


def _looks_like_connector_fragment_line(value: str | None) -> bool:
    text = normalize_whitespace(value)
    if not text:
        return False
    if len(text) <= 6 and text[-1:] in {"은", "는", "이", "가", "을", "를", "의", "와", "과"}:
        return True
    return False


def _looks_like_section_termination_line(value: str | None) -> bool:
    text = normalize_whitespace(value)
    if not text:
        return False
    return any(pattern.search(text) for pattern in _SECTION_TERMINATION_PATTERNS)


def _display_signal_text(record: dict) -> str:
    parts = [
        record.get("job_title_ko"),
        record.get("job_title_raw"),
        record.get("experience_level_ko"),
        record.get("experience_level_raw"),
    ]
    normalized_parts: list[str] = []
    for part in parts:
        text = normalize_whitespace("" if part is None else str(part))
        if text and text.lower() != "nan":
            normalized_parts.append(text)
    return normalize_whitespace(" ".join(normalized_parts))


def _normalize_display_source_label(label: str) -> str:
    mapping = {
        "metadata": "구조화 메타데이터",
        "title": "공고제목",
        "main_tasks": "주요업무",
        "requirements": "자격요건",
        "preferred": "우대사항",
        "detail": "상세본문",
    }
    return mapping.get(label, label)


def _experience_from_text(text: str) -> str:
    lowered = text.lower()
    if not text:
        return ""
    if "전환형 인턴" in text:
        return "전환형 인턴"
    if "인턴" in text or _ENGLISH_INTERN_RE.search(lowered):
        return "인턴"
    if "principal" in lowered:
        return "프린시펄"
    if "staff" in lowered:
        return "스태프"
    if "senior" in lowered or re.search(r"\bsr\.?\b", lowered):
        return "시니어"
    if "lead" in lowered:
        return "리드"
    if "junior" in lowered:
        return "주니어"
    if "postdoctoral" in lowered or "postdoc" in lowered or "포닥" in text:
        return "포닥"
    year_patterns = (
        re.search(r"(?:경력|experience|experiences)\s*(\d{1,2})\s*(?:년|years?)\s*(?:이상|\+)?", text, flags=re.IGNORECASE),
        re.search(r"(\d{1,2})\s*(?:년|years?)\s*(?:이상|\+)\s*(?:의\s*)?(?:경력|experience)", text, flags=re.IGNORECASE),
        re.search(r"(?:경험|experience)\s*(\d{1,2})\s*(?:년|years?)\s*(?:이상|\+)?", text, flags=re.IGNORECASE),
        re.search(r"(\d{1,2})\s*(?:년|years?)\s*(?:이상|\+)\s*(?:의\s*)?(?:경험|experience)", text, flags=re.IGNORECASE),
        re.search(r"(\d{1,2})\s*(?:년|years?)\s*(?:이상|\+)(?:의)?[^\n]{0,48}?(?:경험|experience)", text, flags=re.IGNORECASE),
        re.search(r"(\d{1,2})\+\s*(?:년|years?)", text, flags=re.IGNORECASE),
        re.search(r"\(?(\d{1,2})\s*(?:년|years?)\s*(?:이상|\+)\)?", text, flags=re.IGNORECASE),
        re.search(r"at least\s+(\d{1,2})\s+years?\s+of\s+experience", lowered, flags=re.IGNORECASE),
    )
    for year_match in year_patterns:
        if not year_match:
            continue
        years = int(year_match.group(1))
        if years <= 0 or years >= 21:
            continue
        return f"경력 {years}년+"
    if (
        "신입" in text
        or "new grad" in lowered
        or "entry" in lowered
        or "recent graduate" in lowered
        or "fresh graduate" in lowered
        or "졸업 예정자" in text
        or "기졸업자" in text
        or "신규 졸업자" in text
    ):
        return "신입"
    if (
        _has_pattern_match(text, _RESEARCH_EXPERIENCE_TRACK_PATTERNS)
        and _has_pattern_match(text, _RESEARCH_EXPERIENCE_WORK_PATTERNS)
    ):
        return "경력"
    if "경력" in text or "experienced" in lowered or "career" in lowered:
        return "경력"
    if _has_strong_experience_signal(text):
        return "경력"
    return ""


def _has_strong_experience_signal(text: str) -> bool:
    if not text:
        return False
    lowered = text.lower()
    if (
        "신입" in text
        or "new grad" in lowered
        or "entry" in lowered
        or "recent graduate" in lowered
        or "fresh graduate" in lowered
        or "졸업 예정자" in text
        or "기졸업자" in text
        or "신규 졸업자" in text
        or "인턴" in text
        or _ENGLISH_INTERN_RE.search(lowered)
    ):
        return False
    return any(pattern.search(text) for pattern in _STRONG_EXPERIENCE_SIGNAL_PATTERNS)


def _has_pattern_match(text: str, patterns: tuple[re.Pattern[str], ...]) -> bool:
    return any(pattern.search(text) for pattern in patterns)


def _track_from_text(text: str, *, include_degrees: bool = False) -> str:
    tags: list[str] = []

    def add(tag: str) -> None:
        if tag and tag not in tags:
            tags.append(tag)

    for tag, patterns in _SPECIAL_TRACK_PATTERNS:
        if _has_pattern_match(text, patterns):
            add(tag)
    if re.search(r"전환형\s*인턴", text):
        add("전환형 인턴")
    elif "인턴" in text or _ENGLISH_INTERN_RE.search(text):
        add("인턴")
    if include_degrees:
        for degree, pattern in _DEGREE_TRACK_PATTERNS:
            if pattern.search(text):
                add(degree)
    return " / ".join(tags)


_FOCUS_PATTERNS: tuple[tuple[str, tuple[re.Pattern[str], ...]], ...] = (
    (
        "검색",
        (
            re.compile(r"검색"),
            re.compile(r"\bsearch\b", flags=re.IGNORECASE),
            re.compile(r"\bretrieval\b", flags=re.IGNORECASE),
            re.compile(r"\binformation retrieval\b", flags=re.IGNORECASE),
            re.compile(r"\bquery understanding\b", flags=re.IGNORECASE),
        ),
    ),
    (
        "추천",
        (
            re.compile(r"추천"),
            re.compile(r"\brecommendation\b", flags=re.IGNORECASE),
            re.compile(r"\branking\b", flags=re.IGNORECASE),
            re.compile(r"랭킹"),
            re.compile(r"\bpersonalization\b", flags=re.IGNORECASE),
            re.compile(r"개인화"),
        ),
    ),
    (
        "광고",
        (
            re.compile(r"광고"),
            re.compile(r"\bads?\b", flags=re.IGNORECASE),
            re.compile(r"\bctr\b", flags=re.IGNORECASE),
            re.compile(r"\bcvr\b", flags=re.IGNORECASE),
            re.compile(r"\bauction\b", flags=re.IGNORECASE),
        ),
    ),
    (
        "LLM",
        (
            re.compile(r"\bllm\b", flags=re.IGNORECASE),
            re.compile(r"\blarge language model\b", flags=re.IGNORECASE),
            re.compile(r"거대 언어 모델"),
            re.compile(r"프롬프트"),
            re.compile(r"에이전트"),
            re.compile(r"\bagent(?:ic)?\b", flags=re.IGNORECASE),
            re.compile(r"\brag\b", flags=re.IGNORECASE),
            re.compile(r"검색증강생성"),
        ),
    ),
    (
        "인프라",
        (
            re.compile(r"인프라"),
            re.compile(r"\bml infra\b", flags=re.IGNORECASE),
            re.compile(r"\bmlops\b", flags=re.IGNORECASE),
            re.compile(r"서빙"),
            re.compile(r"\bserving\b", flags=re.IGNORECASE),
            re.compile(r"\bplatform\b", flags=re.IGNORECASE),
            re.compile(r"\bpipeline\b", flags=re.IGNORECASE),
            re.compile(r"배포 시스템"),
        ),
    ),
    (
        "음성",
        (
            re.compile(r"음성"),
            re.compile(r"\bspeech\b", flags=re.IGNORECASE),
            re.compile(r"\bstt\b", flags=re.IGNORECASE),
            re.compile(r"\btts\b", flags=re.IGNORECASE),
            re.compile(r"\bvoice\b", flags=re.IGNORECASE),
            re.compile(r"\basr\b", flags=re.IGNORECASE),
        ),
    ),
    (
        "비전",
        (
            re.compile(r"\bvision\b", flags=re.IGNORECASE),
            re.compile(r"영상"),
            re.compile(r"이미지"),
            re.compile(r"\bcomputer vision\b", flags=re.IGNORECASE),
            re.compile(r"멀티모달"),
            re.compile(r"\bmultimodal\b", flags=re.IGNORECASE),
        ),
    ),
    (
        "로보틱스",
        (
            re.compile(r"로봇"),
            re.compile(r"\brobot\b", flags=re.IGNORECASE),
            re.compile(r"\brobotics\b", flags=re.IGNORECASE),
            re.compile(r"\bslam\b", flags=re.IGNORECASE),
            re.compile(r"\bmanipulation\b", flags=re.IGNORECASE),
            re.compile(r"\bvla\b", flags=re.IGNORECASE),
        ),
    ),
    (
        "리스크",
        (
            re.compile(r"\bfraud\b", flags=re.IGNORECASE),
            re.compile(r"\brisk\b", flags=re.IGNORECASE),
            re.compile(r"사기"),
            re.compile(r"리스크"),
            re.compile(r"\baudit\b", flags=re.IGNORECASE),
        ),
    ),
    (
        "제품분석",
        (
            re.compile(r"\bproduct analytics\b", flags=re.IGNORECASE),
            re.compile(r"제품 분석"),
            re.compile(r"\buser behavior\b", flags=re.IGNORECASE),
            re.compile(r"사용자 행동"),
            re.compile(r"지표"),
            re.compile(r"\bab test\b", flags=re.IGNORECASE),
            re.compile(r"\ba/b test\b", flags=re.IGNORECASE),
        ),
    ),
    (
        "성장분석",
        (
            re.compile(r"\bgrowth\b", flags=re.IGNORECASE),
            re.compile(r"마케팅"),
            re.compile(r"매출"),
            re.compile(r"\bfunnel\b", flags=re.IGNORECASE),
            re.compile(r"\bgrowth analytics\b", flags=re.IGNORECASE),
        ),
    ),
    (
        "데이터플랫폼",
        (
            re.compile(r"\bbigdata\b", flags=re.IGNORECASE),
            re.compile(r"\bbig data\b", flags=re.IGNORECASE),
            re.compile(r"\bdata platform\b", flags=re.IGNORECASE),
            re.compile(r"\bdata pipeline\b", flags=re.IGNORECASE),
            re.compile(r"데이터 파이프라인"),
        ),
    ),
    (
        "제조",
        (
            re.compile(r"제조"),
            re.compile(r"산업 인공지능"),
            re.compile(r"스마트팩토리"),
            re.compile(r"디지털 트윈"),
            re.compile(r"\bdigital twin\b", flags=re.IGNORECASE),
            re.compile(r"\bsmart factory\b", flags=re.IGNORECASE),
        ),
    ),
    (
        "시계열",
        (
            re.compile(r"시계열"),
            re.compile(r"수요\s*예측"),
            re.compile(r"예측 모델"),
            re.compile(r"\bforecast(?:ing)?\b", flags=re.IGNORECASE),
            re.compile(r"\bdemand forecasting\b", flags=re.IGNORECASE),
        ),
    ),
    (
        "최적화",
        (
            re.compile(r"최적화"),
            re.compile(r"강화학습"),
            re.compile(r"배차"),
            re.compile(r"경로 계획"),
            re.compile(r"가격 책정"),
            re.compile(r"\boptimization\b", flags=re.IGNORECASE),
            re.compile(r"\boperations research\b", flags=re.IGNORECASE),
            re.compile(r"\bscheduling\b", flags=re.IGNORECASE),
            re.compile(r"\bpricing\b", flags=re.IGNORECASE),
        ),
    ),
    (
        "공급망",
        (
            re.compile(r"공급망"),
            re.compile(r"재고"),
            re.compile(r"발주"),
            re.compile(r"\bscm\b", flags=re.IGNORECASE),
            re.compile(r"\bsupply chain\b", flags=re.IGNORECASE),
            re.compile(r"\binventory\b", flags=re.IGNORECASE),
        ),
    ),
    (
        "모빌리티",
        (
            re.compile(r"모빌리티"),
            re.compile(r"물류"),
            re.compile(r"\bmobility\b", flags=re.IGNORECASE),
            re.compile(r"\blogistics\b", flags=re.IGNORECASE),
        ),
    ),
    (
        "자율주행",
        (
            re.compile(r"자율주행"),
            re.compile(r"\bautonomous driving\b", flags=re.IGNORECASE),
            re.compile(r"\bautonomous vehicle\b", flags=re.IGNORECASE),
            re.compile(r"\bself-driving\b", flags=re.IGNORECASE),
        ),
    ),
)


def _focus_from_text(text: str) -> str:
    tags: list[str] = []

    def add(tag: str) -> None:
        if tag and tag not in tags:
            tags.append(tag)

    for tag, patterns in _FOCUS_PATTERNS:
        if _has_pattern_match(text, patterns):
            add(tag)
        if len(tags) >= 2:
            break
    return " / ".join(tags[:2])


def _infer_experience_display(record: dict, analysis_fields: dict[str, str] | None = None) -> tuple[str, str]:
    analysis_fields = analysis_fields or {}
    candidates = [
        ("metadata", _display_signal_text(record)),
        ("title", normalize_whitespace(_first_nonempty(record.get("job_title_ko"), record.get("job_title_raw")))),
        ("requirements", _first_nonempty(analysis_fields.get("자격요건_분석용"), record.get("requirements"))),
        ("preferred", _first_nonempty(analysis_fields.get("우대사항_분석용"), record.get("preferred"))),
        ("detail", _first_nonempty(analysis_fields.get("상세본문_분석용"), record.get("description_text"), record.get("description_html"))),
    ]
    for source, text in candidates:
        value = _experience_from_text(text)
        if value:
            return value, _normalize_display_source_label(source)
    combined_signal = normalize_whitespace(" ".join(text for _, text in candidates if normalize_whitespace(text)))
    combined_value = _experience_from_text(combined_signal)
    if combined_value:
        return combined_value, "종합신호"
    fallback_signal = _first_nonempty(
        _display_signal_text(record),
        normalize_whitespace(_first_nonempty(record.get("job_title_ko"), record.get("job_title_raw"))),
        _first_nonempty(analysis_fields.get("주요업무_분석용"), record.get("main_tasks")),
        _first_nonempty(analysis_fields.get("자격요건_분석용"), record.get("requirements")),
        _first_nonempty(analysis_fields.get("상세본문_분석용"), record.get("description_text"), record.get("description_html")),
    )
    if fallback_signal:
        return _DEFAULT_EXPERIENCE_DISPLAY, _DEFAULT_EXPERIENCE_SOURCE
    return "", ""


def _infer_hiring_track_display(record: dict, analysis_fields: dict[str, str] | None = None) -> tuple[str, str]:
    analysis_fields = analysis_fields or {}
    candidates = [
        ("metadata", _display_signal_text(record), True),
        ("title", normalize_whitespace(_first_nonempty(record.get("job_title_ko"), record.get("job_title_raw"))), True),
        ("requirements", _first_nonempty(analysis_fields.get("자격요건_분석용"), record.get("requirements")), True),
        ("preferred", _first_nonempty(analysis_fields.get("우대사항_분석용"), record.get("preferred")), True),
        ("detail", _first_nonempty(analysis_fields.get("상세본문_분석용"), record.get("description_text"), record.get("description_html")), True),
    ]
    tags: list[str] = []
    sources: list[str] = []
    for source, text, include_degrees in candidates:
        value = _track_from_text(text, include_degrees=include_degrees)
        if not value:
            continue
        source_label = _normalize_display_source_label(source)
        for item in value.split(" / "):
            item = normalize_whitespace(item)
            if item and item not in tags:
                tags.append(item)
                sources.append(source_label)
    if not tags:
        fallback_signal = _first_nonempty(
            normalize_whitespace(_first_nonempty(record.get("job_title_ko"), record.get("job_title_raw"))),
            _first_nonempty(analysis_fields.get("자격요건_분석용"), record.get("requirements")),
            _first_nonempty(analysis_fields.get("상세본문_분석용"), record.get("description_text"), record.get("description_html")),
        )
        if fallback_signal:
            return _DEFAULT_HIRING_TRACK, "기본추론"
        return "", ""
    source_labels: list[str] = []
    for label in sources:
        if label and label not in source_labels:
            source_labels.append(label)
    return " / ".join(tags[:3]), " / ".join(source_labels[:3])


def _infer_focus_display(record: dict, analysis_fields: dict[str, str] | None = None) -> tuple[str, str]:
    analysis_fields = analysis_fields or {}
    candidates = [
        ("title", normalize_whitespace(_first_nonempty(record.get("job_title_ko"), record.get("job_title_raw")))),
        ("requirements", _first_nonempty(analysis_fields.get("자격요건_분석용"), record.get("requirements"))),
        ("main_tasks", _first_nonempty(analysis_fields.get("주요업무_분석용"), record.get("main_tasks"))),
        ("preferred", _first_nonempty(analysis_fields.get("우대사항_분석용"), record.get("preferred"))),
        ("detail", _first_nonempty(analysis_fields.get("상세본문_분석용"), record.get("description_text"), record.get("description_html"))),
    ]
    tags: list[str] = []
    sources: list[str] = []
    for source, text in candidates:
        value = _focus_from_text(text)
        if not value:
            continue
        for item in value.split(" / "):
            item = normalize_whitespace(item)
            if item and item not in tags:
                tags.append(item)
                sources.append(_normalize_display_source_label(source))
            if len(tags) >= 2:
                break
        if len(tags) >= 2:
            break
    if not tags:
        return "", ""
    source_labels: list[str] = []
    for label in sources:
        if label and label not in source_labels:
            source_labels.append(label)
    return " / ".join(tags[:2]), " / ".join(source_labels[:2])


def _first_nonempty(*values: object) -> str:
    for value in values:
        text = normalize_whitespace("" if value is None else str(value))
        if text and text.lower() != "nan":
            return text
    return ""


def _build_position_summary_display(
    record: dict,
    experience_display: str,
    hiring_track_display: str,
    focus_display: str,
) -> str:
    tags: list[str] = []
    if experience_display and experience_display != _DEFAULT_EXPERIENCE_DISPLAY:
        tags.append(experience_display)
    if hiring_track_display:
        for item in hiring_track_display.split(" / "):
            item = normalize_whitespace(item)
            if item == _DEFAULT_HIRING_TRACK:
                continue
            if item and item not in tags:
                tags.append(item)
    if focus_display:
        for item in focus_display.split(" / "):
            item = normalize_whitespace(item)
            if item and item not in tags:
                tags.append(item)
    return " / ".join(tags)


def detail_prefers_structured_sections(
    detail: str | None,
    main_tasks: str | None,
    requirements: str | None,
    preferred: str | None,
) -> bool:
    detail_text = normalize_whitespace(detail)
    if not detail_text:
        return False
    structured = compose_detail_fallback(main_tasks, requirements, preferred)
    if not section_output_is_substantive(structured):
        return False
    return any(pattern.search(detail_text) for pattern in _DETAIL_PREFER_SECTION_PATTERNS)


def sanitize_section_text(value: str | None) -> str:
    text = _coerce_section_input(value)
    if not text:
        return ""

    raw_lines = [normalize_whitespace(line) for line in text.split("\n") if normalize_whitespace(line)]
    if any(has_hangul(line) for line in raw_lines):
        raw_lines = [line for line in raw_lines if has_hangul(line)]

    processed_lines: list[str] = []

    for raw_line in raw_lines:
        raw_line = _EMOJI_RE.sub(" ", raw_line).replace("™", " ")
        raw_line = raw_line.replace('"', " ").replace("'", " ").replace("“", " ").replace("”", " ")
        raw_line = normalize_whitespace(raw_line)
        if not raw_line:
            continue
        if _looks_like_section_termination_line(raw_line):
            break
        if _looks_like_legal_notice_line(raw_line):
            continue
        if _looks_like_cta_line(raw_line):
            continue
        if _looks_like_english_prose_line(raw_line):
            continue
        localized_line = _localize_common_terms(raw_line)
        replaced = _LATIN_TOKEN_RE.sub(lambda match: _translate_english_token(match.group(0)), localized_line)
        replaced = _EMPTY_BRACKETS_RE.sub(" ", replaced)
        replaced = _SPACE_BEFORE_PUNCT_RE.sub(r"\1", replaced)
        replaced = re.sub(r"\(\s*,\s*", "(", replaced)
        replaced = re.sub(r",\s*\)", ")", replaced)
        replaced = re.sub(r"(^|[\s(])[,/]+(?:\s+)?(?=[가-힣A-Za-z])", r"\1", replaced)
        replaced = re.sub(r",\s*,+", ", ", replaced)
        replaced = re.sub(r"/\s+(?=[가-힣A-Za-z])", " ", replaced)
        replaced = replaced.replace(" ,", " ").replace(" /", " ")
        replaced = normalize_whitespace(replaced)
        if not replaced or _PUNCT_ONLY_LINE_RE.fullmatch(replaced):
            continue
        if (
            _looks_like_legal_notice_line(replaced)
            or _looks_like_heading_artifact_line(replaced)
            or _looks_like_cta_line(replaced)
            or _looks_like_connector_fragment_line(replaced)
        ):
            continue
        if not has_hangul(replaced):
            continue
        processed_lines.append(replaced)

    return "\n".join(processed_lines)


def sanitize_core_skill_text(value: str | None) -> str:
    skills: list[str] = []
    seen: set[str] = set()
    raw_text = _coerce_section_input(value)
    if not raw_text:
        return ""
    raw_lines = [normalize_whitespace(line) for line in raw_text.split("\n") if normalize_whitespace(line)]
    for raw_line in raw_lines:
        localized_line = _localize_common_terms(raw_line)
        candidate = _LATIN_TOKEN_RE.sub(lambda match: _translate_english_token(match.group(0)), localized_line)
        candidate = _EMPTY_BRACKETS_RE.sub(" ", candidate)
        candidate = _SPACE_BEFORE_PUNCT_RE.sub(r"\1", candidate)
        candidate = normalize_whitespace(candidate)
        if not candidate:
            continue
        if _looks_like_legal_notice_line(candidate) or _looks_like_heading_artifact_line(candidate):
            continue
        if any(pattern.search(candidate) for pattern in _CORE_SKILL_NOISE_PATTERNS):
            continue
        if len(candidate) > 24:
            continue
        if candidate in seen:
            continue
        seen.add(candidate)
        skills.append(candidate)
    generic_only = {"인공지능", "머신러닝", "딥러닝", "에이전트"}
    if skills:
        specific_skills = [skill for skill in skills if skill not in generic_only]
        if specific_skills:
            skills = specific_skills
        elif all(skill in generic_only for skill in skills):
            return ""
    return "\n".join(skills)


def compose_detail_fallback(main_tasks: str | None, requirements: str | None, preferred: str | None) -> str:
    parts = []
    for part in (main_tasks, requirements, preferred):
        normalized_part = normalize_whitespace(part)
        if not normalized_part:
            continue
        if section_output_looks_noisy(normalized_part):
            continue
        parts.append(normalized_part)
    return "\n".join(parts)


def _looks_like_section_heading(line: str, patterns: tuple[re.Pattern[str], ...]) -> bool:
    normalized = normalize_whitespace(line)
    if not normalized:
        return False
    return any(pattern.search(normalized) for pattern in patterns)


def _extract_section_from_raw_detail(raw_text: str | None, patterns: tuple[re.Pattern[str], ...]) -> str:
    text = _coerce_section_input(raw_text)
    if not text:
        return ""
    raw_lines = [normalize_whitespace(line) for line in text.split("\n")]
    lines = [line for line in raw_lines if line]
    if not lines:
        return ""

    all_headings = _MAIN_TASK_HEADING_PATTERNS + _REQUIREMENTS_HEADING_PATTERNS + _PREFERRED_HEADING_PATTERNS
    collecting = False
    collected: list[str] = []
    for line in lines:
        if _looks_like_section_heading(line, patterns):
            collecting = True
            inline_match = re.search(r"[:\-–]\s*(.+)$", line)
            if inline_match:
                remainder = normalize_whitespace(inline_match.group(1))
                if remainder and not _looks_like_section_heading(remainder, all_headings):
                    collected.append(remainder)
            continue
        if collecting and _looks_like_section_heading(line, all_headings):
            if _looks_like_section_heading(line, patterns):
                inline_match = re.search(r"[:\-–]\s*(.+)$", line)
                if inline_match:
                    remainder = normalize_whitespace(inline_match.group(1))
                    if remainder and not _looks_like_section_heading(remainder, all_headings):
                        collected.append(remainder)
                continue
            break
        if collecting:
            collected.append(line)
    return sanitize_section_text("\n".join(collected))


def _preserve_english_task_lines(raw_text: str | None) -> str:
    text = _coerce_section_input(raw_text)
    if not text:
        return ""

    preserved: list[str] = []
    for raw_line in text.split("\n"):
        line = normalize_whitespace(raw_line)
        if not line:
            continue
        line = re.sub(r"^[\-*•]+\s*", "", line)
        line = normalize_whitespace(line)
        if not line or has_hangul(line):
            continue
        if _looks_like_legal_notice_line(line) or _looks_like_cta_line(line):
            continue
        if len(_ENGLISH_WORD_RE.findall(line)) < 3:
            continue
        localized_line = _localize_common_terms(line)
        replaced = _LATIN_TOKEN_RE.sub(
            lambda match: _translate_english_token(match.group(0)) or "영문토큰",
            localized_line,
        )
        replaced = replaced.replace("'", " ").replace("’", " ").replace("`", " ")
        replaced = _EMPTY_BRACKETS_RE.sub(" ", replaced)
        replaced = _SPACE_BEFORE_PUNCT_RE.sub(r"\1", replaced)
        replaced = normalize_whitespace(replaced)
        if not replaced or contains_english(replaced):
            continue
        if "영문토큰" in replaced or not has_hangul(replaced):
            continue
        preserved.append(replaced)

    joined = "\n".join(preserved)
    if not section_output_is_substantive(joined):
        return ""
    return joined


def _section_contains_competing_heading(text: str, patterns: tuple[re.Pattern[str], ...]) -> bool:
    normalized = _coerce_section_input(text)
    if not normalized:
        return False
    return any(_looks_like_section_heading(line, patterns) for line in normalized.split("\n") if normalize_whitespace(line))


def _normalized_section_equals(left: str | None, right: str | None) -> bool:
    left_normalized = sanitize_section_text(left)
    right_normalized = sanitize_section_text(right)
    if not left_normalized or not right_normalized:
        return False
    return left_normalized == right_normalized


def _looks_like_english_prose_line(line: str) -> bool:
    normalized = normalize_whitespace(line)
    if not normalized or has_hangul(normalized):
        return False
    english_words = _ENGLISH_WORD_RE.findall(normalized)
    if len(english_words) < 6:
        return False
    if "://" in normalized:
        return True
    if len(normalized) >= 45:
        return True
    return normalized.endswith((".", ":", "?", "!"))


def _build_loss_baseline(value: str | None) -> str:
    text = normalize_whitespace(value)
    if not text:
        return ""

    localized = _localize_common_terms(text)
    baseline_lines: list[str] = []
    for raw_line in localized.split("\n"):
        replaced = _LATIN_TOKEN_RE.sub(
            lambda match: _translate_english_token(match.group(0)) or "영문토큰",
            raw_line,
        )
        replaced = _EMPTY_BRACKETS_RE.sub(" ", replaced)
        replaced = _SPACE_BEFORE_PUNCT_RE.sub(r"\1", replaced)
        replaced = normalize_whitespace(replaced)
        if not replaced or _PUNCT_ONLY_LINE_RE.fullmatch(replaced):
            continue
        if has_hangul(replaced) or "영문토큰" in replaced:
            baseline_lines.append(replaced)
    return "\n".join(baseline_lines)


def _normalize_signal_token(token: str) -> str:
    normalized = token.strip()
    for suffix in _TOKEN_SUFFIXES:
        if normalized.endswith(suffix) and len(normalized) - len(suffix) >= 2:
            normalized = normalized[: -len(suffix)]
            break
    return normalized


def _extract_signal_tokens(text: str) -> set[str]:
    tokens: set[str] = set()
    for candidate in _KOREAN_TOKEN_RE.findall(text):
        token = _normalize_signal_token(candidate)
        if len(token) < 2:
            continue
        if token in _GENERIC_SIGNAL_TOKENS:
            continue
        tokens.add(token)
    return tokens


def _append_unique_skills(skills: list[str], seen: set[str], value: str) -> None:
    normalized = normalize_whitespace(value)
    if not normalized or normalized in seen:
        return
    skills.append(normalized)
    seen.add(normalized)


def extract_core_skills_from_text(*values: str | None) -> str:
    corpus = "\n".join(normalize_whitespace(value) for value in values if normalize_whitespace(value))
    if not corpus:
        return ""

    localized = _localize_common_terms(corpus)
    skills: list[str] = []
    seen: set[str] = set()

    for pattern, label in _CORE_SKILL_PATTERNS:
        if pattern.search(corpus) or label in localized:
            _append_unique_skills(skills, seen, label)

    for line in localized.split("\n"):
        normalized_line = normalize_whitespace(line)
        if not normalized_line:
            continue
        if "기술" in normalized_line or "스택" in normalized_line or "도구" in normalized_line:
            for pattern, label in _CORE_SKILL_PATTERNS:
                if pattern.search(normalized_line) or label in normalized_line:
                    _append_unique_skills(skills, seen, label)

    return "\n".join(skills)


def build_analysis_fields(record: dict) -> dict[str, str]:
    raw_detail = record.get("description_text") or record.get("description_html")
    main_tasks = sanitize_section_text(record.get("main_tasks"))
    requirements = sanitize_section_text(record.get("requirements"))
    preferred = sanitize_section_text(record.get("preferred"))
    preserved_english_main_tasks = _preserve_english_task_lines(record.get("main_tasks"))
    extracted_sections = extract_sections_from_description(raw_detail)
    raw_main_tasks = _extract_section_from_raw_detail(raw_detail, _MAIN_TASK_HEADING_PATTERNS)
    raw_requirements = _extract_section_from_raw_detail(raw_detail, _REQUIREMENTS_HEADING_PATTERNS)
    raw_preferred = _extract_section_from_raw_detail(raw_detail, _PREFERRED_HEADING_PATTERNS)
    if not main_tasks:
        main_tasks = sanitize_section_text(extracted_sections.get("주요업무"))
    if preserved_english_main_tasks and (
        not main_tasks or not section_output_is_substantive(main_tasks)
    ):
        main_tasks = preserved_english_main_tasks
    if raw_main_tasks and (
        not main_tasks
        or _section_contains_competing_heading(main_tasks, _REQUIREMENTS_HEADING_PATTERNS + _PREFERRED_HEADING_PATTERNS)
        or _normalized_section_equals(main_tasks, raw_requirements)
        or _normalized_section_equals(main_tasks, raw_preferred)
    ):
        main_tasks = raw_main_tasks
    if not requirements:
        requirements = sanitize_section_text(extracted_sections.get("자격요건"))
    if raw_requirements and (
        not requirements
        or _section_contains_competing_heading(requirements, _MAIN_TASK_HEADING_PATTERNS + _PREFERRED_HEADING_PATTERNS)
        or _normalized_section_equals(requirements, raw_main_tasks)
        or _normalized_section_equals(requirements, raw_preferred)
    ):
        requirements = raw_requirements
    if not preferred:
        preferred = sanitize_section_text(extracted_sections.get("우대사항"))
    if raw_preferred and (
        not preferred
        or len(normalize_whitespace(raw_preferred)) > len(normalize_whitespace(preferred))
        or _section_contains_competing_heading(preferred, _MAIN_TASK_HEADING_PATTERNS + _REQUIREMENTS_HEADING_PATTERNS)
        or _normalized_section_equals(preferred, raw_main_tasks)
        or _normalized_section_equals(preferred, raw_requirements)
    ):
        preferred = raw_preferred
    inferred_core_skills = extract_core_skills_from_text(
        record.get("core_skills"),
        record.get("requirements"),
        record.get("preferred"),
        record.get("main_tasks"),
        raw_detail,
        record.get("job_title_raw"),
    )
    detail = sanitize_section_text(raw_detail)
    if detail and not section_output_is_substantive(detail):
        detail = ""
    if detail and detail_prefers_structured_sections(detail, main_tasks, requirements, preferred):
        detail = ""
    structured_detail = compose_detail_fallback(main_tasks, requirements, preferred)
    if detail and section_loss_looks_high(structured_detail, detail):
        detail = ""
    if not detail:
        detail = structured_detail
    return {
        "주요업무_분석용": main_tasks,
        "자격요건_분석용": requirements,
        "우대사항_분석용": preferred,
        "핵심기술_분석용": sanitize_core_skill_text(record.get("core_skills") or inferred_core_skills),
        "상세본문_분석용": detail,
    }


def build_display_fields(record: dict, analysis_fields: dict[str, str] | None = None) -> dict[str, str]:
    analysis_fields = analysis_fields or build_analysis_fields(record)
    main_tasks_display = sanitize_section_text(analysis_fields.get("주요업무_분석용", ""))
    requirements_display = sanitize_section_text(analysis_fields.get("자격요건_분석용", ""))
    preferred_display = sanitize_section_text(analysis_fields.get("우대사항_분석용", ""))
    if not preferred_display:
        preferred_display = _DEFAULT_PREFERRED_DISPLAY
    core_skills_display = sanitize_core_skill_text(analysis_fields.get("핵심기술_분석용", ""))
    experience_display, experience_source = _infer_experience_display(record, analysis_fields)
    hiring_track_display, hiring_track_source = _infer_hiring_track_display(record, analysis_fields)
    focus_display, focus_source = _infer_focus_display(record, analysis_fields)
    position_summary = _build_position_summary_display(record, experience_display, hiring_track_display, focus_display)
    return {
        "회사명_표시": sanitize_name_or_title_text(record.get("company_name"), unknown_name=True),
        "소스명_표시": sanitize_name_or_title_text(record.get("source_name"), unknown_name=True),
        "공고제목_표시": sanitize_name_or_title_text(record.get("job_title_ko") or record.get("job_title_raw"), allow_english=True),
        "경력수준_표시": experience_display,
        "경력근거_표시": experience_source,
        "채용트랙_표시": hiring_track_display,
        "채용트랙근거_표시": hiring_track_source,
        "직무초점_표시": focus_display,
        "직무초점근거_표시": focus_source,
        "구분요약_표시": position_summary,
        "직무명_표시": sanitize_name_or_title_text(record.get("job_role")),
        "주요업무_표시": main_tasks_display,
        "자격요건_표시": requirements_display,
        "우대사항_표시": preferred_display,
        "핵심기술_표시": core_skills_display,
    }


def count_english_leaks(frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0
    leaks = 0
    for column in ("주요업무_표시", "자격요건_표시", "우대사항_표시", "핵심기술_표시"):
        if column not in frame.columns:
            continue
        leaks += int(frame[column].fillna("").astype(str).str.contains(r"[A-Za-z]").sum())
    return leaks


def section_loss_looks_high(raw_text: str | None, normalized_text: str | None) -> bool:
    raw = normalize_whitespace(raw_text)
    normalized = normalize_whitespace(normalized_text)
    if not raw:
        return False

    baseline = _build_loss_baseline(raw)
    if baseline and not normalized:
        return True
    if not baseline:
        return bool(raw and not normalized)

    if normalized in _GENERIC_SECTION_TAILS and normalized != baseline:
        return True
    if len(normalized) <= max(10, int(len(baseline) * 0.6)):
        return True

    baseline_lines = [line for line in baseline.split("\n") if line]
    normalized_lines = [line for line in normalized.split("\n") if line]
    if len(baseline_lines) >= 2 and len(normalized_lines) * 2 < len(baseline_lines):
        return True

    baseline_tokens = _extract_signal_tokens(baseline)
    if not baseline_tokens:
        return False
    normalized_tokens = _extract_signal_tokens(normalized)
    if "영문토큰" in baseline_tokens and "영문토큰" not in normalized_tokens:
        return True
    overlap_ratio = len(baseline_tokens & normalized_tokens) / len(baseline_tokens)
    if overlap_ratio < 0.6:
        return True
    return False


def section_output_looks_noisy(value: str | None) -> bool:
    text = normalize_whitespace(value)
    if not text:
        return False
    if "://" in text or ",," in text:
        return True

    lines = [line for line in text.split("\n") if normalize_whitespace(line)]
    if not lines:
        return False

    noisy_lines = 0
    for line in lines:
        compact = normalize_whitespace(line)
        hangul_tokens = _KOREAN_TOKEN_RE.findall(compact)
        punctuation_count = sum(character in "',.:;/()-" for character in compact)
        punctuation_ratio = punctuation_count / max(len(compact), 1)
        if len(compact) >= 40 and compact.count(" ") == 0:
            noisy_lines += 1
            continue
        if punctuation_ratio >= 0.18 and len(hangul_tokens) <= 2:
            noisy_lines += 1
            continue
        if re.search(r"(,\s*){2,}|/{2,}|::|'\s*\(", compact):
            noisy_lines += 1

    return noisy_lines >= max(1, len(lines) // 2)


def section_output_is_substantive(value: str | None) -> bool:
    text = normalize_whitespace(value)
    if not text:
        return False
    if _looks_like_legal_notice_line(text):
        return False
    if section_output_looks_noisy(text):
        return False
    lines = [line for line in text.split("\n") if normalize_whitespace(line)]
    if not lines:
        return False
    if len(text) < 40 and len(lines) <= 1:
        return False
    short_fragment_lines = sum(1 for line in lines if len(normalize_whitespace(line)) <= 12 and " " not in normalize_whitespace(line))
    if len(lines) >= 3 and short_fragment_lines >= len(lines) - 1:
        return False
    if all(_looks_like_heading_artifact_line(line) for line in lines):
        return False
    return True
