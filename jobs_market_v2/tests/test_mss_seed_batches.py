from jobs_market_v2.mss_seed_batches import (
    BABY_UNICORN_2023_NAMES_RAW,
    build_baby_unicorn_2023_rows,
    normalize_company_name,
    parse_dips_2023_names_from_text,
)


def test_normalize_company_name_strips_legal_wrappers() -> None:
    assert normalize_company_name("주식회사 모레") == "모레"
    assert normalize_company_name("(주)퀄리타스반도체") == "퀄리타스반도체"
    assert normalize_company_name("농업회사법인 푸디웜") == "푸디웜"
    assert normalize_company_name("아우토크립트(주)") == "아우토크립트"


def test_build_baby_unicorn_rows_has_expected_count() -> None:
    frame = build_baby_unicorn_2023_rows()
    assert len(frame) == len(BABY_UNICORN_2023_NAMES_RAW) == 51
    assert frame["company_name"].is_unique
    assert "MSS_BABY_UNICORN_2023" in frame["candidate_seed_type"].unique().tolist()


def test_parse_dips_names_from_text_extracts_all_section_entries() -> None:
    text = """
    시스템반도체(25개사)
    1 (주)굿인텔리전스2 래블업 주식회사
    바이오·헬스(45개사)
    1 주식회사 넥스아이2 주식회사 닥터테일
    미래 모빌리티(30개사)
    1 (주) 로비고스2 주식회사 로웨인
    친환경·에너지(25개사)
    1 (주) 그리너지2 주식회사 라잇루트
    로봇(25개사)
    1 (주) 딥인사이트2 주식회사 럭스로보
    시스템반도체(5개사) 1 주식회사 세미파이브
    바이오·헬스(13개사)
    1주식회사 리센스메디컬(RecensMedical, Inc.)
    미래 모빌리티(7개사) 1 주식회사 딥핑소스
    """
    # Simplified counts for a unit-sized fixture
    from jobs_market_v2.mss_seed_batches import SectionSpec, DIPS_2023_SECTION_SPECS

    original = tuple(DIPS_2023_SECTION_SPECS)
    try:
        import jobs_market_v2.mss_seed_batches as module

        module.DIPS_2023_SECTION_SPECS = (
            SectionSpec("시스템반도체(25개사)", 2),
            SectionSpec("바이오·헬스(45개사)", 2),
            SectionSpec("미래 모빌리티(30개사)", 2),
            SectionSpec("친환경·에너지(25개사)", 2),
            SectionSpec("로봇(25개사)", 2),
            SectionSpec("시스템반도체(5개사)", 1),
            SectionSpec("바이오·헬스(13개사)", 1),
            SectionSpec("미래 모빌리티(7개사)", 1),
        )
        names = parse_dips_2023_names_from_text(text)
        assert names == [
            "(주)굿인텔리전스",
            "래블업 주식회사",
            "주식회사 넥스아이",
            "주식회사 닥터테일",
            "(주) 로비고스",
            "주식회사 로웨인",
            "(주) 그리너지",
            "주식회사 라잇루트",
            "(주) 딥인사이트",
            "주식회사 럭스로보",
            "주식회사 세미파이브",
            "주식회사 리센스메디컬(RecensMedical, Inc.)",
            "주식회사 딥핑소스",
        ]
    finally:
        import jobs_market_v2.mss_seed_batches as module

        module.DIPS_2023_SECTION_SPECS = original
