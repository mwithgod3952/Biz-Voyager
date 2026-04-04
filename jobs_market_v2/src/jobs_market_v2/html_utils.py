"""HTML cleaning and section extraction helpers."""

from __future__ import annotations

import re
from collections import defaultdict

from bs4 import BeautifulSoup, NavigableString, Tag

from .utils import normalize_whitespace


SECTION_ALIASES = {
    "주요업무": (
        "주요업무",
        "주요 업무",
        "담당업무",
        "담당 업무",
        "업무 내용",
        "이런 일을 해요",
        "직무 소개",
        "직무소개",
        "우리 팀과 함께할 미션을 소개합니다",
        "함께할 미션을 소개합니다",
        "미션을 소개합니다",
        "하게 될 일은 다음과 같습니다",
        "어떤 업무를 담당하나요",
        "이런 일을 하실 수 있어요",
        "합류하면 담당할 업무예요",
        "합류하면 담당할 업무",
        "합류하게 되면 이런 일을 하게 됩니다",
        "담당하실 업무에 대하여 소개 드립니다",
        "어떤 기회가 있나요",
        "합류하면 이런 일을 합니다",
        "이 포지션에서 하게 될 일",
        "job description",
        "responsibilities",
        "responsibility",
        "key responsibilities",
        "job responsibilities",
        "what you'll do",
        "what you’ll do",
        "what you will do",
        "about the role",
        "role overview",
        "position overview",
        "about this role",
        "the role",
        "role",
    ),
    "자격요건": (
        "자격요건",
        "자격 요건",
        "지원자격",
        "지원 자격",
        "공통자격",
        "공통 자격",
        "필수요건",
        "필수 요건",
        "이런 경험을 가진 분과 함께 성장하고 싶습니다",
        "이런 경험을 가진 분과 함께 성장하고 싶습니다 필수요건",
        "이런 분과 함께 하고 싶어요",
        "이런 분을 원해요",
        "이런 분을 찾고 있어요",
        "이런 동료를 기다립니다",
        "아래 경험 자격을 갖춘 분과 함께 일하고 싶습니다",
        "어떤 경력과 역량이 필요한가요",
        "이런 경험이 필요해요",
        "필수 조건",
        "requirements",
        "qualification",
        "qualifications",
        "candidate requirements",
        "basic qualifications",
        "minimum qualifications",
        "required qualifications",
        "must have",
        "about you",
        "who you are",
        "you have",
        "you need to have",
        "you might be this person if",
    ),
    "우대사항": (
        "우대사항",
        "우대 사항",
        "우대요건",
        "우대 요건",
        "선호",
        "선호사항",
        "선호 사항",
        "이런 경험들이 있다면 저희가 찾는 그 분입니다",
        "이런 경험들이 있다면 저희가 찾는 그 분입니다 우대요건",
        "이런 경험이 있으면 더 좋아요",
        "이런 분이면 더 좋아요",
        "이런 분이라면 더 좋아요",
        "이런 분이라면 더욱 좋습니다",
        "아래 경험 자격이 있다면 더욱 좋습니다",
        "이런 역량이 있으면 더 좋아요",
        "preferred",
        "preferred qualification",
        "preferred qualifications",
        "preferred experience",
        "preferred skills",
        "nice to have",
        "bonus points",
        "good to have",
        "plus if you have",
    ),
    "핵심기술": (
        "핵심기술",
        "핵심 기술",
        "핵심 기술 역량",
        "기술스택",
        "skills",
        "skill",
        "core technical proficiencies",
        "technical proficiencies",
        "technical skills",
        "tech stack",
        "tools",
        "stack",
        "technology stack",
    ),
}

_BLOCK_TAGS = ("h1", "h2", "h3", "h4", "p", "li", "dt", "dd", "div", "section", "article", "blockquote")
_LEAF_CONTAINER_TAGS = {"div", "section", "article"}
_HEADING_TAGS = {"h1", "h2", "h3", "h4", "dt", "blockquote"}
_HEADING_SEPARATOR_RE = re.compile(r"\s*[:\-–]\s*")
_HEADING_TRAILING_NOTE_RE = re.compile(
    r"^[!?.:\s\-–]*(?:\([^)]{1,20}\)|\[[^\]]{1,20}\]|\{[^}]{1,20}\})?[!?.:\s\-–]*$"
)
_SECTION_SEPARATOR_RE = re.compile(r"^[-_=]{3,}$|^\[(korean|english|한국어|영문)\]$", flags=re.IGNORECASE)
_FRAGMENT_LINE_RE = re.compile(r"^[A-Za-z가-힣0-9/&+().,:;'\"_-]{1,2}$")
_STOP_SECTION_HEADINGS = (
    "회사 소개",
    "about coupang",
    "about coupangeats",
    "about eats analytics",
    "about eats analytics team",
    "recruitment process",
    "application review",
    "details to consider",
    "privacy notice",
    "document return policy",
    "equal opportunities for all",
    "전형 절차",
    "전형 절차 및 안내 사항",
    "전형절차",
    "전형 과정",
    "채용 전형",
    "참고 사항",
    "참고해 주세요",
    "필요 서류",
    "근무지",
    "고용형태",
    "이렇게 근무해요",
    "안내 사항",
    "개인정보 처리방침",
    "서류 반환 정책",
)
_STOP_HEADING_REGEX_PATTERNS = (
    re.compile(r"전형\s*(과정|절차)"),
    re.compile(r"필요\s*서류"),
    re.compile(r"근무지"),
    re.compile(r"고용형태"),
    re.compile(r"안내\s*사항"),
)
_HEADING_REGEX_PATTERNS = {
    "주요업무": (
        re.compile(r"\bresponsibilit", flags=re.IGNORECASE),
        re.compile(r"\bwhat\s+(?:you|you'll|you will)\b", flags=re.IGNORECASE),
        re.compile(r"\bthe opportunity\b", flags=re.IGNORECASE),
        re.compile(r"\bthe impact\b", flags=re.IGNORECASE),
        re.compile(r"\bimpact you(?:'|’)ll\b", flags=re.IGNORECASE),
        re.compile(r"\bimpact you will\b", flags=re.IGNORECASE),
        re.compile(r"\bjob description\b", flags=re.IGNORECASE),
        re.compile(r"\brole overview\b", flags=re.IGNORECASE),
        re.compile(r"\bposition overview\b", flags=re.IGNORECASE),
        re.compile(r"직무\s*소개"),
        re.compile(r"이런\s*일을\s*해요"),
        re.compile(r"합류하(?:면|게\s*되면).{0,12}업무"),
        re.compile(r"담당하실\s*업무"),
    ),
    "자격요건": (
        re.compile(r"\bqualif", flags=re.IGNORECASE),
        re.compile(r"\brequirements?\b", flags=re.IGNORECASE),
        re.compile(r"\bwho you are\b", flags=re.IGNORECASE),
        re.compile(r"\babout you\b", flags=re.IGNORECASE),
        re.compile(r"\bwhat\s+(?:you|you'll|you will)\s+bring\b", flags=re.IGNORECASE),
        re.compile(r"\bwhat\s+(?:you|you'll|you will)\s+need\b", flags=re.IGNORECASE),
        re.compile(r"\bwhat (?:we|we're|we are) (?:look|looking)\b", flags=re.IGNORECASE),
        re.compile(r"\bwho (?:we|we're|we are) (?:look|looking)\b", flags=re.IGNORECASE),
        re.compile(r"\bwhat you need\b", flags=re.IGNORECASE),
        re.compile(r"\bmust have\b", flags=re.IGNORECASE),
        re.compile(r"\brole is right for me\b", flags=re.IGNORECASE),
        re.compile(r"이런\s*분을\s*찾고\s*있어요"),
        re.compile(r"이런\s*분을\s*원해요"),
        re.compile(r"이런\s*동료를\s*기다립니다"),
        re.compile(r"아래\s*경험\s*자격.{0,16}함께\s*일하고\s*싶습니다"),
        re.compile(r"\byou need to have\b", flags=re.IGNORECASE),
        re.compile(r"\byou might be this person if\b", flags=re.IGNORECASE),
        re.compile(r"공통\s*자격"),
    ),
    "우대사항": (
        re.compile(r"\bpreferred\b", flags=re.IGNORECASE),
        re.compile(r"\bnice to have\b", flags=re.IGNORECASE),
        re.compile(r"\bbonus points\b", flags=re.IGNORECASE),
        re.compile(r"\bgood to have\b", flags=re.IGNORECASE),
        re.compile(r"\bplus if\b", flags=re.IGNORECASE),
        re.compile(r"선호\s*(?:사항)?"),
        re.compile(r"이런\s*분이라면\s*더\s*좋아요"),
        re.compile(r"이런\s*분이라면\s*더욱\s*좋습니다"),
        re.compile(r"아래\s*경험\s*자격.{0,16}더욱\s*좋습니다"),
    ),
    "핵심기술": (
        re.compile(r"\bskills?\b", flags=re.IGNORECASE),
        re.compile(r"\btools?\b", flags=re.IGNORECASE),
        re.compile(r"\btech stack\b", flags=re.IGNORECASE),
        re.compile(r"\btechnical proficien", flags=re.IGNORECASE),
        re.compile(r"\bcore technical\b", flags=re.IGNORECASE),
    ),
}


def _is_fragment_line(line: str) -> bool:
    compact = normalize_whitespace(line).replace(" ", "")
    return bool(compact and (compact in {"<", ">", "/"} or _FRAGMENT_LINE_RE.fullmatch(compact)))


def _join_fragment_buffer(buffer: list[str]) -> str:
    tokens = [normalize_whitespace(item) for item in buffer if normalize_whitespace(item)]
    if not tokens:
        return ""

    cleaned_tokens: list[str] = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token == "<":
            probe = index + 1
            tag_tokens: list[str] = []
            while probe < len(tokens) and len(tag_tokens) < 12 and tokens[probe] != ">":
                tag_tokens.append(tokens[probe])
                probe += 1
            if probe < len(tokens) and tag_tokens and all(re.fullmatch(r"[/A-Za-z0-9]+", item) for item in tag_tokens):
                index = probe + 1
                continue
        if token not in {"<", ">", "/"}:
            cleaned_tokens.append(token)
        index += 1

    merged = "".join(cleaned_tokens)
    return normalize_whitespace(merged)


def _repair_fragmented_text(text: str) -> str:
    lines = [normalize_whitespace(line) for line in text.split("\n") if normalize_whitespace(line)]
    if not lines:
        return ""

    repaired_lines: list[str] = []
    buffer: list[str] = []

    def flush() -> None:
        nonlocal buffer
        if not buffer:
            return
        if len(buffer) >= 2:
            repaired = _join_fragment_buffer(buffer)
            if repaired:
                repaired_lines.append(repaired)
        else:
            repaired_lines.extend(buffer)
        buffer = []

    for line in lines:
        if _is_fragment_line(line):
            buffer.append(line)
            continue
        flush()
        repaired_lines.append(line)
    flush()

    return normalize_whitespace("\n".join(repaired_lines))


def clean_html_text(value: str | None) -> str:
    if not value:
        return ""
    soup = BeautifulSoup(value, "lxml")
    text = soup.get_text("\n", strip=True)
    return _repair_fragmented_text(text)


def _normalize_heading_text(text: str) -> str:
    cleaned = normalize_whitespace(text)
    cleaned = re.sub(r"[^\w가-힣\s:/\-–?!&+'’]", " ", cleaned)
    cleaned = normalize_whitespace(cleaned)
    return cleaned.strip(" :-–?!").lower()


def _canonical_heading(text: str) -> str | None:
    lowered = _normalize_heading_text(text)
    if not lowered:
        return None
    for canonical, aliases in SECTION_ALIASES.items():
        if any(alias.lower() == lowered for alias in aliases):
            return canonical
    for canonical, patterns in _HEADING_REGEX_PATTERNS.items():
        if any(pattern.search(lowered) for pattern in patterns):
            return canonical
    return None


def _match_heading_prefix(text: str) -> tuple[str | None, str]:
    normalized = normalize_whitespace(text)
    lowered = normalized.lower()
    if not normalized:
        return None, ""
    for canonical, aliases in SECTION_ALIASES.items():
        for alias in aliases:
            alias_lower = alias.lower()
            if lowered == alias_lower:
                return canonical, ""
            if lowered.startswith(alias_lower):
                tail = normalized[len(alias) :]
                if _HEADING_SEPARATOR_RE.match(tail):
                    remainder = _HEADING_SEPARATOR_RE.sub("", tail, count=1)
                    return canonical, normalize_whitespace(remainder)
                if not normalize_whitespace(tail) or _HEADING_TRAILING_NOTE_RE.fullmatch(tail):
                    return canonical, ""

    separator_match = re.search(r"[:\-–]\s*", normalized)
    if separator_match:
        heading_candidate = normalize_whitespace(normalized[: separator_match.start()])
        remainder = normalize_whitespace(normalized[separator_match.end() :])
        canonical = _canonical_heading(heading_candidate)
        if canonical and _looks_like_heading_candidate(heading_candidate):
            return canonical, remainder

    canonical = _canonical_heading(normalized)
    if canonical and _looks_like_heading_candidate(normalized):
        return canonical, ""
    return None, ""


def _looks_like_heading_candidate(text: str) -> bool:
    normalized = normalize_whitespace(text)
    if not normalized or len(normalized) > 160:
        return False
    if "," in normalized:
        return False
    if len(normalized.split()) > 18:
        return False
    return True


def _is_leaf_container(block: Tag) -> bool:
    if block.name not in _LEAF_CONTAINER_TAGS:
        return True
    return block.find(["h1", "h2", "h3", "h4", "p", "li", "dt", "dd"]) is None


def _is_heading_block(block: Tag, text: str) -> bool:
    if block.name in _HEADING_TAGS:
        return True
    if not text or len(text) > 120:
        return False
    children = list(block.children)
    if not children:
        return False
    return all(
        (
            isinstance(child, NavigableString)
            and not normalize_whitespace(str(child))
        )
        or (
            isinstance(child, Tag)
            and child.name in {"strong", "b", "span"}
            and normalize_whitespace(child.get_text(" ", strip=True))
        )
        for child in children
    )


def _extract_inline_heading(block: Tag) -> tuple[str, str]:
    heading_parts: list[str] = []
    remainder_parts: list[str] = []
    seen_heading = False
    in_remainder = False

    for child in block.children:
        if isinstance(child, NavigableString):
            text = normalize_whitespace(str(child))
            if not text:
                continue
            if seen_heading:
                in_remainder = True
                remainder_parts.append(text)
            else:
                heading_parts.append(text)
            continue

        if child.name == "br" and seen_heading:
            in_remainder = True
            continue

        child_text = normalize_whitespace(child.get_text(" ", strip=True))
        if not child_text:
            continue

        if not in_remainder and child.name in {"strong", "b", "span"}:
            heading_parts.append(child_text)
            seen_heading = True
            continue

        if seen_heading:
            in_remainder = True
            remainder_parts.append(child_text)
        else:
            heading_parts.append(child_text)

    if not seen_heading:
        return "", ""
    return normalize_whitespace(" ".join(heading_parts)), normalize_whitespace("\n".join(remainder_parts))


def _is_section_separator(text: str) -> bool:
    return bool(_SECTION_SEPARATOR_RE.fullmatch(normalize_whitespace(text)))


def _match_heading_from_lines(lines: list[str], start: int) -> tuple[str | None, str, int]:
    max_span = min(3, len(lines) - start)
    for span in range(max_span, 0, -1):
        chunk = [normalize_whitespace(lines[start + offset]) for offset in range(span)]
        if not all(chunk):
            continue
        if span > 1 and any(len(part) > 20 for part in chunk[:-1]):
            continue
        candidate = normalize_whitespace(" ".join(chunk))
        canonical, remainder = _match_heading_prefix(candidate)
        if canonical:
            return canonical, remainder, span
    return None, "", 0


def _match_stop_heading_from_lines(lines: list[str], start: int) -> int:
    max_span = min(3, len(lines) - start)
    for span in range(max_span, 0, -1):
        chunk = [normalize_whitespace(lines[start + offset]) for offset in range(span)]
        if not all(chunk):
            continue
        if span > 1 and any(len(part) > 20 for part in chunk[:-1]):
            continue
        candidate = _normalize_heading_text(" ".join(chunk))
        if any(candidate == heading or candidate.startswith(f"{heading} ") for heading in _STOP_SECTION_HEADINGS):
            return span
        if any(pattern.search(candidate) for pattern in _STOP_HEADING_REGEX_PATTERNS):
            return span
    return 0


def _is_stop_heading_text(text: str) -> bool:
    candidate = _normalize_heading_text(text)
    return any(candidate == heading or candidate.startswith(f"{heading} ") for heading in _STOP_SECTION_HEADINGS) or any(
        pattern.search(candidate) for pattern in _STOP_HEADING_REGEX_PATTERNS
    )


def _extract_sections_from_text_lines(description_html: str | None) -> dict[str, str]:
    sections = {key: "" for key in SECTION_ALIASES}
    lines = [line for line in clean_html_text(description_html).split("\n") if line]
    if not lines:
        return sections

    collected: dict[str, list[str]] = defaultdict(list)
    current_section: str | None = None
    index = 0

    while index < len(lines):
        line = normalize_whitespace(lines[index])
        if not line:
            index += 1
            continue
        if _is_section_separator(line):
            current_section = None
            index += 1
            continue

        stop_consumed = _match_stop_heading_from_lines(lines, index)
        if stop_consumed:
            current_section = None
            index += stop_consumed
            continue

        canonical, remainder, consumed = _match_heading_from_lines(lines, index)
        if canonical:
            current_section = canonical
            if remainder:
                collected[current_section].append(remainder)
            index += consumed
            continue

        if current_section:
            collected[current_section].append(line)
        index += 1

    for canonical in SECTION_ALIASES:
        sections[canonical] = normalize_whitespace("\n".join(collected.get(canonical, [])))
    return sections


def extract_sections_from_description(description_html: str | None) -> dict[str, str]:
    sections = {key: "" for key in SECTION_ALIASES}
    if not description_html:
        return sections

    soup = BeautifulSoup(description_html, "lxml")
    collected: dict[str, list[str]] = defaultdict(list)
    current_section: str | None = None

    for block in soup.find_all(_BLOCK_TAGS):
        if isinstance(block, Tag) and not _is_leaf_container(block):
            continue
        text = normalize_whitespace(block.get_text(" ", strip=True))
        if not text:
            continue
        if _is_section_separator(text):
            current_section = None
            continue
        if _is_stop_heading_text(text):
            current_section = None
            continue

        inline_heading, inline_remainder = _extract_inline_heading(block)
        heading_text = inline_heading or text
        canonical, remainder = _match_heading_prefix(heading_text)
        remainder = inline_remainder or remainder

        if canonical:
            current_section = canonical
            if remainder:
                collected[current_section].append(remainder)
            continue

        if inline_heading or _is_heading_block(block, text):
            current_section = None
            continue

        if current_section:
            collected[current_section].append(text)

    for canonical in SECTION_ALIASES:
        sections[canonical] = normalize_whitespace("\n".join(collected.get(canonical, [])))
    fallback_sections = _extract_sections_from_text_lines(description_html)
    for canonical, fallback_value in fallback_sections.items():
        if fallback_value and not sections.get(canonical):
            sections[canonical] = fallback_value
    return sections
