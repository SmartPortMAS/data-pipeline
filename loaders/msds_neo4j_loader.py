"""
MSDS Neo4j 이관 모듈 — PostgreSQL JSONB → 지식 그래프 (Knowledge Graph)
==========================================================================
파이프라인 위치: PostgreSQL (msds_payload JSONB) → [이 모듈] → Neo4j

그래프 모델:
┌──────────────────────────────────────────────────────────────────────────┐
│  (:Chemical {id, name_ko, name_en, cas_no, un_no})                       │
│       │                              │                                    │
│  [:HAS_HAZARD]               [:INCOMPATIBLE_WITH]                        │
│       ↓                              ↓                                    │
│  (:HazardClass {name})    (:IncompatibleMaterial {name})                 │
│                                                                           │
│  데이터 출처:                                                             │
│  - Chemical     ← staging_msds_chemical 기본 식별자 컬럼                 │
│  - HazardClass  ← msds_payload.detail02 (B02: GHS 분류)                 │
│  - Incompatible ← msds_payload.detail10 (J코드 텍스트 키워드 추출)       │
└──────────────────────────────────────────────────────────────────────────┘

핵심 설계 원칙:
- PostgreSQL 서버사이드 Named Cursor로 대량 데이터 메모리 효율 보장.
- Neo4j UNWIND 배치 패턴으로 네트워크 왕복(round-trip) 최소화.
- MERGE + ON CREATE/MATCH SET으로 재실행 완전 멱등성(Idempotency) 보장.
- detail10 텍스트의 혼재금지 물질을 정규식 키워드 사전으로 엔티티화.

외부 의존 라이브러리:
    pip install psycopg2-binary neo4j python-dotenv

환경변수 (.env 또는 OS 환경변수):
    PG_HOST, PG_PORT, PG_DBNAME, PG_USER, PG_PASSWORD
    NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, NEO4J_DATABASE
"""

import logging
import os
import re

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from neo4j import GraphDatabase
from neo4j.exceptions import ClientError as Neo4jClientError
from neo4j.exceptions import ServiceUnavailable

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# 로깅 설정
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("msds_neo4j_loader")

# ─────────────────────────────────────────────────────────────────────────────
# DB 접속 설정 — 환경변수로 분리
# ─────────────────────────────────────────────────────────────────────────────
PG_CONFIG: dict = {
    "host":            os.getenv("PG_HOST",     "localhost"),
    "port":            int(os.getenv("PG_PORT", "5432")),
    "dbname":          os.getenv("PG_DBNAME",   "smartport"),
    "user":            os.getenv("PG_USER",     "postgres"),
    "password":        os.getenv("PG_PASSWORD", ""),
    "connect_timeout": 10,
    "options":         "-c search_path=public",
}

NEO4J_URI      = os.getenv("NEO4J_URI",      "bolt://localhost:7687")
NEO4J_USER     = os.getenv("NEO4J_USER",     "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")

# ─────────────────────────────────────────────────────────────────────────────
# 배치 사이즈 설정
# ─────────────────────────────────────────────────────────────────────────────
PG_FETCH_CHUNK   = 200  # PostgreSQL Named Cursor에서 한 번에 클라이언트로 가져올 행 수
NEO4J_BATCH_SIZE = 50   # Neo4j execute_write() 한 번에 처리할 레코드 수


# ─────────────────────────────────────────────────────────────────────────────
# ① HazardClass 추출 — detail02 (유해성·위험성) B02 코드
# ─────────────────────────────────────────────────────────────────────────────

# GHS 유해성 분류에 해당하는 msdsItemCode
# B02: GHS 분류 전문 (인화성 액체 구분2, 발암성 구분1B 등)
# KOSHA API는 B02 항목 하나에 전체 분류 텍스트를 '|'로 구분하여 반환함
_GHS_CLASSIFICATION_CODES: frozenset[str] = frozenset({"B02"})

# 적재 무의미 값 필터 (API 응답에서 빈 데이터를 나타내는 문자열들)
_NULL_VALUES: frozenset[str] = frozenset({"자료없음", "해당없음", "-", "", "N/A", "없음"})


def extract_hazard_classes(detail02_data: list[dict]) -> list[str]:
    """
    detail02 (유해성·위험성) 섹션의 data 배열에서 GHS 분류 항목 목록을 추출한다.

    처리 로직:
    1. msdsItemCode == 'B02' 인 항목의 itemDetail 텍스트를 선택.
    2. KOSHA API는 B02 하나에 여러 분류를 '|' 또는 개행으로 구분하므로 분리.
    3. 각 분류 문자열 앞뒤 공백·마침표 제거 후 필터링.
    4. 중복 제거 후 정렬된 리스트 반환.

    실제 API 응답 예시 (벤젠):
        {"msdsItemCode": "B02",
         "itemDetail": "인화성 액체 : 구분2|생식세포 변이원성 : 구분1B|발암성 : 구분1B"}
    → ["발암성 : 구분1B", "생식세포 변이원성 : 구분1B", "인화성 액체 : 구분2"]

    Args:
        detail02_data: msds_payload["detail02"]["data"] 리스트.

    Returns:
        Graph RAG HazardClass 노드명 목록 (중복 없음, 가나다 정렬).
    """
    if not isinstance(detail02_data, list):
        return []

    hazards: set[str] = set()

    for item in detail02_data:
        code:   str = (item.get("msdsItemCode") or "").strip()
        detail: str = (item.get("itemDetail")   or "").strip()

        if code not in _GHS_CLASSIFICATION_CODES:
            continue
        if not detail or detail in _NULL_VALUES:
            continue

        # '|' 또는 개행으로 구분된 여러 분류를 개별 노드로 분리
        for part in re.split(r"[|\n\r]", detail):
            cleaned = part.strip().rstrip(".")
            if cleaned and cleaned not in _NULL_VALUES:
                hazards.add(cleaned)

    return sorted(hazards)


# ─────────────────────────────────────────────────────────────────────────────
# ② IncompatibleMaterial 추출 — detail10 (안정성 및 반응성) J코드 키워드
# ─────────────────────────────────────────────────────────────────────────────

# 혼재금지 물질 표준명 → 매칭 정규식 패턴 목록
# 키: Neo4j IncompatibleMaterial 노드에 저장될 표준화 한글 엔티티명
# 값: 해당 엔티티를 발견하기 위한 정규식 패턴 리스트 (하나라도 매칭 시 노드 생성)
#
# 설계 원칙:
# - 너무 짧은 단어(물, 열 등)는 오탐을 방지하기 위해 경계(lookahead) 조건 추가.
# - 파생어·복합어가 많은 한국어 특성상 패턴을 유연하게 구성.
# - 이 사전이 Graph RAG의 핵심 엔티티 어휘(vocabulary)가 되므로 도메인 전문가와 지속 관리 필요.
INCOMPATIBLE_KEYWORD_DICT: dict[str, list[str]] = {
    # ── 산/염기류 ────────────────────────────────────────────────────────────
    "강산류":           [r"강산류?", r"진한\s*산", r"무기\s*산"],
    "강알칼리류":       [r"강알칼리류?", r"강염기류?", r"강한\s*염기"],
    "산화제":           [r"산화제", r"산화성\s*물질", r"산화성\s*가스"],
    "환원제":           [r"환원제", r"환원성\s*물질", r"강\s*환원제"],
    # ── 물/수분 — '물질', '물리' 등 파생어 오탐 방지 ─────────────────────────
    "물/수분":          [r"물(?!질|리|성|체|론|고|가)", r"수분", r"수증기", r"습기"],
    # ── 가연성 ──────────────────────────────────────────────────────────────
    "가연성물질":       [r"가연성\s*물질", r"인화성\s*물질", r"가연물"],
    # ── 할로겐족 ────────────────────────────────────────────────────────────
    "할로겐":           [r"할로겐"],
    "염소":             [r"액체\s*염소", r"염소\s*가스", r"염소(?!\s*계|\s*화)"],
    "플루오린(불소)":   [r"플루오린", r"불소", r"HF\b"],
    "브롬":             [r"브롬"],
    "요오드":           [r"요오드"],
    # ── 알칼리금속 ──────────────────────────────────────────────────────────
    "알칼리금속":       [r"알칼리\s*금속", r"나트륨(?!\s*이온|\s*클)", r"칼륨(?!\s*이온)"],
    # ── 구체적 무기산 ────────────────────────────────────────────────────────
    "질산":             [r"질산류?"],
    "황산":             [r"황산류?"],
    "염산":             [r"염산", r"염화\s*수소"],
    "과산화수소":       [r"과산화\s*수소"],
    "과산화물":         [r"과산화물", r"유기\s*과산화물"],
    # ── 유기물/용제 ─────────────────────────────────────────────────────────
    "유기물":           [r"유기물", r"유기\s*용제", r"유기\s*용매", r"유기\s*화합물"],
    # ── 금속류 ──────────────────────────────────────────────────────────────
    "금속류":           [r"금속류", r"금속\s*분말", r"구리\b", r"알루미늄\b", r"아연\b", r"철\s*분"],
    # ── 공기/산소 — '산소화', '산소기' 등 오탐 방지 ──────────────────────────
    "산소/공기":        [r"산소(?!화|기)", r"공기(?!\s*업|청)", r"대기\s*중"],
    # ── 암모니아/아민류 ──────────────────────────────────────────────────────
    "암모니아":         [r"암모니아"],
    "아민류":           [r"아민류?", r"1차\s*아민", r"2차\s*아민"],
    # ── 열/점화원 — 화학적 혼재 위험이 아닌 물리적 조건도 중요 엔티티 ─────────
    "열/점화원":        [r"열\s*[,·및]\s*스파크", r"점화원", r"고온", r"화염"],
    # ── 중합 반응 ────────────────────────────────────────────────────────────
    "중합반응물질":     [r"중합\s*반응", r"중합\s*촉매", r"퍼옥사이드"],
}

# detail10 J-prefix 코드 중 분해생성물(J07) 계열은
# IncompatibleMaterial 추출 대상에서 제외 (혼재금지 물질과 의미 구분)
_EXCLUDE_J_PREFIXES: tuple[str, ...] = ("J07",)

# 구체적 물질명 → 상위 카테고리 정규화 테이블
#
# 문제: INCOMPATIBLE_KEYWORD_DICT에 "황산", "염산" 같은 구체적 물질명이 있으면
#       extract_incompatible_keywords가 "황산" IncompatibleMaterial 노드를 생성한다.
#       그런데 황산(CAS 7664-93-9)의 IS_CLASSIFIED_AS는 "강산류" 노드를 가리키므로
#       공유 노드가 달라 혼재 탐지 쿼리가 실패한다.
#
#       화학물질A -[INCOMPATIBLE_WITH]→ "황산"   ← 이 노드
#       황산       -[IS_CLASSIFIED_AS]→ "강산류"  ← 저 노드  → 탐지 누락
#
# 해결: 추출 직후 구체적 물질명을 상위 카테고리명으로 치환하여 노드를 통일한다.
_MATERIAL_ALIAS: dict[str, str] = {
    "황산":           "강산류",
    "염산":           "강산류",
    "질산":           "강산류",
    "과산화수소":     "과산화물",
    "염소":           "할로겐",
    "브롬":           "할로겐",
    "요오드":         "할로겐",
    "플루오린(불소)": "할로겐",
}


def extract_incompatible_keywords(detail10_data: list[dict]) -> list[str]:
    """
    detail10 (안정성 및 반응성) 섹션의 data 배열에서 혼재금지 물질 키워드를 추출한다.

    처리 로직:
    1. J 코드(J01~J06, J08 등) 항목의 itemDetail 텍스트를 수집.
       J07(분해생성물) 계열은 별도 의미를 가지므로 제외.
    2. 수집된 텍스트 전체를 합쳐 INCOMPATIBLE_KEYWORD_DICT의 정규식으로 탐색.
    3. 매칭된 경우 딕셔너리의 표준 노드명(키)을 반환.
    4. 텍스트 문장 전체를 노드화하지 않고, 의미 있는 엔티티만 추출하는 것이 핵심.

    실제 API 응답 예시 (벤젠):
        {"msdsItemCode": "J01",
         "itemDetail": "강산류 및 산화제와 접촉 시 격렬히 반응함. 물과 서서히 반응."}
    → ["강산류", "물/수분", "산화제"]

    Graph RAG 활용 예시:
        벤젠-[:INCOMPATIBLE_WITH]->강산류
        톨루엔-[:INCOMPATIBLE_WITH]->강산류
        → "강산류와 혼재 불가 화물은?" 쿼리 시 벤젠, 톨루엔 동시 검색 가능

    Args:
        detail10_data: msds_payload["detail10"]["data"] 리스트.

    Returns:
        Graph RAG IncompatibleMaterial 노드명 목록 (중복 없음, 가나다 정렬).
    """
    if not isinstance(detail10_data, list):
        return []

    # J 코드(분해생성물 제외) 텍스트만 수집
    texts: list[str] = []
    for item in detail10_data:
        code:   str = (item.get("msdsItemCode") or "").strip()
        detail: str = (item.get("itemDetail")   or "").strip()

        if not code.startswith("J"):
            continue
        # 제외 코드(J07 계열) 필터
        if any(code.startswith(excl) for excl in _EXCLUDE_J_PREFIXES):
            continue
        if detail and detail not in _NULL_VALUES:
            texts.append(detail)

    if not texts:
        return []

    # 개별 텍스트를 하나로 합쳐 패턴 탐색 (문장 경계 무시, 키워드 존재 여부만 확인)
    combined: str = " ".join(texts)

    found: set[str] = set()
    for std_name, patterns in INCOMPATIBLE_KEYWORD_DICT.items():
        for pattern in patterns:
            # IGNORECASE: 영문 약어(HF 등) 대소문자 무관 매칭
            if re.search(pattern, combined, re.IGNORECASE):
                found.add(std_name)
                break  # 해당 노드명의 패턴 중 하나라도 매칭 → 다음 노드로 이동

    # 구체적 물질명 → 상위 카테고리로 치환하여 IS_CLASSIFIED_AS 노드와 일치시킴
    # 예: "황산" → "강산류"  (황산의 IS_CLASSIFIED_AS는 "강산류"를 가리키므로)
    return sorted({_MATERIAL_ALIAS.get(name, name) for name in found})


# ─────────────────────────────────────────────────────────────────────────────
# ③ IS_CLASSIFIED_AS 추출 — 이 화학물질 자체가 어떤 혼재금지 카테고리에 속하는가
# ─────────────────────────────────────────────────────────────────────────────
#
# 핵심 아이디어:
#   벤젠 -[:INCOMPATIBLE_WITH]-> "강산류"
#   황산 -[:IS_CLASSIFIED_AS]--> "강산류"   ← 황산 자체가 강산류임을 명시
#   → 이 두 관계를 이용해 "벤젠 + 황산은 혼재 불가" 를 그래프에서 자동 추론 가능
#
# 2단계 감지 전략:
#   1) GHS 분류 텍스트(detail02 B02)로 자동 감지  → 인화성/산화성 계열
#   2) CAS번호 기반 수동 보완                      → GHS로 잡히지 않는 무기산·강염기

# GHS HazardClass 텍스트 패턴 → 이 물질이 속하는 IncompatibleMaterial 카테고리
# 예: "인화성 액체 : 구분2" 포함 → 이 물질은 "가연성물질" 카테고리에 해당
_HAZARD_TEXT_TO_CATEGORY: list[tuple[str, str]] = [
    (r"인화성\s*(액체|가스|고체|에어로졸)", "가연성물질"),
    (r"가연성\s*액체",                       "가연성물질"),
    (r"산화성\s*(액체|가스|고체)",           "산화제"),
    (r"자기\s*반응성\s*물질",               "산화제"),
    (r"유기\s*과산화물",                     "과산화물"),
]

# CAS번호 → 수동 카테고리 보완 매핑
# GHS 분류 텍스트만으로 감지되지 않는 물질(무기산, 강염기 등)을 보완
_CAS_CATEGORY_SUPPLEMENT: dict[str, list[str]] = {
    # ── 강산류 ──────────────────────────────────────────────────────────────
    "7664-93-9":  ["강산류"],                    # 황산 (H2SO4) — 대표적 강산
    # ── 강알칼리·아민류 ──────────────────────────────────────────────────────
    "7664-41-7":  ["암모니아", "강알칼리류"],    # 암모니아 — 수용액은 강염기
    "102-71-6":   ["아민류"],                    # 트리에탄올아민 — 아민 계열
    "26471-62-5": ["아민류", "가연성물질"],      # TDI — 이소시아네이트(아민 반응성)
    # ── 할로겐 함유 ─────────────────────────────────────────────────────────
    "75-01-4":    ["가연성물질", "할로겐"],      # 염화비닐 (Cl 함유)
    "79-01-6":    ["가연성물질", "할로겐"],      # 트리클로로에틸렌 (Cl 함유)
    # ── 극저온/산화 특성 ─────────────────────────────────────────────────────
    "1333-74-0":  ["가연성물질", "산소/공기"],   # 액화수소 — 극저온·산화제 역할
}


def extract_chemical_classifications(
    cas_no: str,
    hazard_classes: list[str],
) -> list[str]:
    """
    이 화학물질 자체가 속하는 IncompatibleMaterial 카테고리를 추출한다.
    IS_CLASSIFIED_AS 관계 생성에 사용된다.

    예시:
        황산(cas=7664-93-9) → ["강산류"]
        가솔린(GHS="인화성 액체:구분2") → ["가연성물질"]
        암모니아 → ["강알칼리류", "암모니아"]

    Args:
        cas_no:         CAS 번호 (수동 보완 조회용).
        hazard_classes: extract_hazard_classes()가 반환한 GHS 분류 문자열 목록.

    Returns:
        이 물질이 속하는 IncompatibleMaterial 카테고리명 목록 (중복 없음, 정렬).
    """
    found: set[str] = set()

    # Step 1: GHS 분류 텍스트로 자동 감지
    combined = " ".join(hazard_classes)
    for pattern, category in _HAZARD_TEXT_TO_CATEGORY:
        if re.search(pattern, combined, re.IGNORECASE):
            found.add(category)

    # Step 2: CAS 기반 수동 보완 (GHS 텍스트로 잡히지 않는 물질 처리)
    for category in _CAS_CATEGORY_SUPPLEMENT.get(cas_no or "", []):
        found.add(category)

    return sorted(found)


# ─────────────────────────────────────────────────────────────────────────────
# Neo4j 스키마 — 제약조건(Constraint) 및 인덱스
# ─────────────────────────────────────────────────────────────────────────────

# Neo4j 4.4+ / 5.x 기준 Cypher 구문
# IF NOT EXISTS: 멱등성 보장 (재실행 안전)
_NEO4J_CONSTRAINT_STMTS: list[str] = [
    # Chemical 노드: id 기준 유일성 (PK 역할)
    "CREATE CONSTRAINT chem_id_unique IF NOT EXISTS "
    "FOR (c:Chemical) REQUIRE c.id IS UNIQUE",

    # HazardClass 노드: name 기준 유일성
    # 동일 유해성 분류는 단 하나의 노드로 수렴 → 화학물질 간 공유 노드 형성
    "CREATE CONSTRAINT hazard_class_name_unique IF NOT EXISTS "
    "FOR (h:HazardClass) REQUIRE h.name IS UNIQUE",

    # IncompatibleMaterial 노드: name 기준 유일성
    # "강산류" 노드 하나에 N개 화학물질이 INCOMPATIBLE_WITH 관계로 연결됨
    "CREATE CONSTRAINT incompatible_material_name_unique IF NOT EXISTS "
    "FOR (m:IncompatibleMaterial) REQUIRE m.name IS UNIQUE",
]


def ensure_neo4j_schema(driver) -> None:
    """
    Neo4j 제약조건과 인덱스를 멱등성 있게 생성한다.
    Neo4j 4.x 구형 버전(IF NOT EXISTS 미지원)에서는 이미 존재 오류를 조용히 무시.
    """
    with driver.session(database=NEO4J_DATABASE) as session:
        for stmt in _NEO4J_CONSTRAINT_STMTS:
            try:
                session.run(stmt)
            except Neo4jClientError as e:
                # 이미 동일한 제약조건이 존재하는 경우 (Neo4j 4.x 호환)
                if "already exists" in str(e).lower() or "equivalent" in str(e).lower():
                    logger.debug("제약조건 이미 존재 (스킵): %s...", stmt[:50])
                else:
                    raise  # 그 외 오류는 상위로 전파
    logger.info("Neo4j 스키마(제약조건) 확인/생성 완료")


# ─────────────────────────────────────────────────────────────────────────────
# Cypher 쿼리 정의 — UNWIND 배치 패턴
# ─────────────────────────────────────────────────────────────────────────────

# ── Chemical 노드 MERGE ──────────────────────────────────────────────────────
# ON CREATE: 최초 생성 시에만 created_at 기록
# ON MATCH:  이미 존재하는 경우 식별자 정보 갱신 + updated_at 기록
_CYPHER_MERGE_CHEMICAL = """
UNWIND $batch AS row
MERGE (c:Chemical {id: row.chem_id})
ON CREATE SET
    c.name_ko    = row.name_ko,
    c.name_en    = row.name_en,
    c.cas_no     = row.cas_no,
    c.un_no      = row.un_no,
    c.created_at = datetime()
ON MATCH SET
    c.name_ko    = row.name_ko,
    c.name_en    = row.name_en,
    c.cas_no     = row.cas_no,
    c.un_no      = row.un_no,
    c.updated_at = datetime()
"""

# ── HazardClass 노드 + HAS_HAZARD 관계 MERGE ────────────────────────────────
# 동일 hazard_name이 여러 화학물질에 등장해도 HazardClass 노드는 하나만 생성됨
# → 이것이 지식 그래프의 핵심: "발암성 구분1B" 노드에 벤젠, 가솔린이 모두 연결
_CYPHER_MERGE_HAZARD = """
UNWIND $batch AS row
MATCH (c:Chemical {id: row.chem_id})
UNWIND row.hazards AS hazard_name
MERGE (h:HazardClass {name: hazard_name})
ON CREATE SET h.created_at = datetime()
MERGE (c)-[r:HAS_HAZARD]->(h)
ON CREATE SET r.created_at = datetime()
"""

# ── IncompatibleMaterial 노드 + INCOMPATIBLE_WITH 관계 MERGE ────────────────
# "강산류" 하나의 노드에 N개 화학물질이 연결 → "강산류와 혼재 불가 화물 목록" 즉시 조회
_CYPHER_MERGE_INCOMPATIBLE = """
UNWIND $batch AS row
MATCH (c:Chemical {id: row.chem_id})
UNWIND row.incompatibles AS material_name
MERGE (m:IncompatibleMaterial {name: material_name})
ON CREATE SET m.created_at = datetime()
MERGE (c)-[r:INCOMPATIBLE_WITH]->(m)
ON CREATE SET r.created_at = datetime()
"""

# ── IS_CLASSIFIED_AS 관계 MERGE ──────────────────────────────────────────────
# 이 화학물질 자체가 어떤 IncompatibleMaterial 카테고리에 속하는지 명시
# 예: 황산 -[:IS_CLASSIFIED_AS]-> "강산류"
#
# 이 관계가 있어야 아래 쿼리가 가능:
#   MATCH (a:Chemical)-[:INCOMPATIBLE_WITH]->(m)<-[:IS_CLASSIFIED_AS]-(b:Chemical)
#   → a와 b가 같은 화물에 있으면 혼재 불가 자동 탐지
#
# IncompatibleMaterial 노드는 MERGE로 생성 (INCOMPATIBLE_WITH 배치 이전에
# 해당 카테고리 노드가 없을 수도 있으므로 안전하게 MERGE 사용)
_CYPHER_MERGE_IS_CLASSIFIED_AS = """
UNWIND $batch AS row
MATCH (c:Chemical {id: row.chem_id})
UNWIND row.classifications AS category_name
MERGE (m:IncompatibleMaterial {name: category_name})
ON CREATE SET m.created_at = datetime()
MERGE (c)-[r:IS_CLASSIFIED_AS]->(m)
ON CREATE SET r.created_at = datetime()
"""


# ─────────────────────────────────────────────────────────────────────────────
# Neo4j 트랜잭션 함수 — session.execute_write()에 전달되는 단위 함수
# ─────────────────────────────────────────────────────────────────────────────

def _tx_merge_chemical(tx, batch: list[dict]) -> None:
    """Chemical 노드 배치 MERGE 트랜잭션 함수."""
    tx.run(_CYPHER_MERGE_CHEMICAL, batch=batch)


def _tx_merge_hazard(tx, batch: list[dict]) -> None:
    """HazardClass 노드 + HAS_HAZARD 관계 배치 MERGE 트랜잭션 함수."""
    tx.run(_CYPHER_MERGE_HAZARD, batch=batch)


def _tx_merge_incompatible(tx, batch: list[dict]) -> None:
    """IncompatibleMaterial 노드 + INCOMPATIBLE_WITH 관계 배치 MERGE 트랜잭션 함수."""
    tx.run(_CYPHER_MERGE_INCOMPATIBLE, batch=batch)


def _tx_merge_is_classified_as(tx, batch: list[dict]) -> None:
    """IS_CLASSIFIED_AS 관계 배치 MERGE 트랜잭션 함수."""
    tx.run(_CYPHER_MERGE_IS_CLASSIFIED_AS, batch=batch)


# ─────────────────────────────────────────────────────────────────────────────
# 배치 쓰기 실행 — 3단계 Neo4j 쿼리 순서 보장
# ─────────────────────────────────────────────────────────────────────────────

def _write_batch_to_neo4j(driver, rows: list[dict]) -> None:
    """
    행 목록을 3단계 Cypher 트랜잭션으로 Neo4j에 기록한다.

    실행 순서가 중요:
      1) Chemical 노드 먼저 MERGE (뒤따르는 MATCH의 대상 노드 보장)
      2) HazardClass 노드 + HAS_HAZARD 관계 MERGE
      3) IncompatibleMaterial 노드 + INCOMPATIBLE_WITH 관계 MERGE

    빈 hazards/incompatibles 배치는 Neo4j UNWIND에 빈 리스트가 전달되어
    아무 작업도 하지 않으므로 별도 스킵 분기 없이도 안전하지만,
    네트워크 왕복을 절약하기 위해 명시적으로 필터링한다.

    Args:
        driver: Neo4j GraphDatabase.driver() 인스턴스 (thread-safe).
        rows:   변환된 레코드 목록 (chem_id, hazards, incompatibles 포함).
    """
    if not rows:
        return

    # 관계가 있는 행만 필터 (불필요한 트랜잭션 절약)
    hazard_rows          = [r for r in rows if r.get("hazards")]
    incompatible_rows    = [r for r in rows if r.get("incompatibles")]
    classification_rows  = [r for r in rows if r.get("classifications")]

    with driver.session(database=NEO4J_DATABASE) as session:
        # ── Step 1: Chemical 노드 ─────────────────────────────────────────
        session.execute_write(_tx_merge_chemical, batch=rows)

        # ── Step 2: HazardClass + HAS_HAZARD ──────────────────────────────
        if hazard_rows:
            session.execute_write(_tx_merge_hazard, batch=hazard_rows)

        # ── Step 3: IncompatibleMaterial + INCOMPATIBLE_WITH ──────────────
        if incompatible_rows:
            session.execute_write(_tx_merge_incompatible, batch=incompatible_rows)

        # ── Step 4: IS_CLASSIFIED_AS (이 물질 자체의 카테고리) ──────────────
        # Step 3 이후 실행 → IncompatibleMaterial 노드가 이미 존재하는 상태
        if classification_rows:
            session.execute_write(_tx_merge_is_classified_as, batch=classification_rows)


# ─────────────────────────────────────────────────────────────────────────────
# 메인 이관 함수 — PostgreSQL Cursor → Neo4j 배치 MERGE
# ─────────────────────────────────────────────────────────────────────────────

def transfer_msds_to_neo4j(
    pg_conn,
    neo4j_driver,
    batch_size: int = NEO4J_BATCH_SIZE,
) -> None:
    """
    PostgreSQL staging_msds_chemical 테이블의 전체 데이터를 Neo4j로 이관한다.

    메모리 효율을 위해 PostgreSQL 서버사이드 Named Cursor를 사용하여
    전체 결과를 한 번에 클라이언트 메모리에 올리지 않고 chunk 단위로 스트리밍한다.

    Args:
        pg_conn:        psycopg2 연결 객체 (이미 연결된 상태).
        neo4j_driver:   neo4j.GraphDatabase.driver() 반환 객체.
        batch_size:     Neo4j execute_write 당 처리 레코드 수.
    """
    # quality_flag='OK' 인 정상 레코드만 대상 (MISSING_KEY 제외)
    _SELECT_SQL = """
        SELECT
            chem_id,
            cas_no,
            un_no,
            name_ko,
            name_en,
            msds_payload
        FROM staging_msds_chemical
        WHERE quality_flag = 'OK'
        ORDER BY chem_id
    """

    # Named Cursor (서버사이드): 결과 전체를 클라이언트로 한 번에 전송하지 않음
    # RealDictCursor: psycopg2가 JSONB 컬럼을 Python dict로 자동 역직렬화
    with pg_conn.cursor(
        name="msds_neo4j_fetch_cursor",
        cursor_factory=psycopg2.extras.RealDictCursor,
    ) as cur:
        cur.itersize = PG_FETCH_CHUNK  # 서버 → 클라이언트 chunk 크기
        cur.execute(_SELECT_SQL)

        batch:           list[dict] = []
        total_processed: int        = 0
        total_hazards:   int        = 0
        total_incomp:    int        = 0

        for pg_row in cur:
            # psycopg2 RealDictCursor는 JSONB → dict 자동 변환
            payload: dict = pg_row["msds_payload"] or {}

            # ── detail02 → HazardClass 후보 추출 ──────────────────────────
            d02_data = payload.get("detail02", {}).get("data", [])
            hazards  = extract_hazard_classes(d02_data)

            # ── detail10 → IncompatibleMaterial 키워드 추출 ───────────────
            d10_data      = payload.get("detail10", {}).get("data", [])
            incompatibles = extract_incompatible_keywords(d10_data)

            # ── 이 물질 자체의 카테고리 → IS_CLASSIFIED_AS 관계용 ──────────
            # GHS 분류 텍스트 + CAS번호 기반 2단계 감지
            cas_no          = pg_row["cas_no"] or ""
            classifications = extract_chemical_classifications(cas_no, hazards)

            batch.append({
                "chem_id":         pg_row["chem_id"],
                "cas_no":          cas_no,
                "un_no":           pg_row["un_no"]   or "",
                "name_ko":         pg_row["name_ko"] or "",
                "name_en":         pg_row["name_en"] or "",
                "hazards":         hazards,
                "incompatibles":   incompatibles,
                "classifications": classifications,  # IS_CLASSIFIED_AS 용
            })

            total_hazards += len(hazards)
            total_incomp  += len(incompatibles)

            # batch_size 도달 → Neo4j 쓰기 후 배치 초기화
            if len(batch) >= batch_size:
                _write_batch_to_neo4j(neo4j_driver, batch)
                total_processed += len(batch)
                logger.info(
                    "Neo4j MERGE 완료: 누적 %d건 처리 (HazardClass 누계: %d, Incompatible 누계: %d)",
                    total_processed, total_hazards, total_incomp,
                )
                batch.clear()

        # 루프 종료 후 잔여 배치 처리
        if batch:
            _write_batch_to_neo4j(neo4j_driver, batch)
            total_processed += len(batch)

    logger.info(
        "Neo4j 이관 완료 — Chemical 노드: %d개 / HazardClass 관계: %d건 / "
        "IncompatibleMaterial 관계: %d건",
        total_processed, total_hazards, total_incomp,
    )


# ─────────────────────────────────────────────────────────────────────────────
# 오케스트레이터 — 연결 생성 및 전체 파이프라인 실행
# ─────────────────────────────────────────────────────────────────────────────

def run_neo4j_transfer() -> None:
    """
    PostgreSQL → Neo4j 이관 파이프라인 전체를 실행한다.

    실행 순서:
      1. PostgreSQL 연결
      2. Neo4j Driver 초기화 및 연결 검증
      3. Neo4j 제약조건/인덱스 보장
      4. 배치 이관 실행
      5. 모든 연결 종료 (finally 보장)
    """
    pg_conn      = None
    neo4j_driver = None

    try:
        # ── PostgreSQL 연결 (Named Cursor는 트랜잭션 내에서만 동작) ──
        pg_conn = psycopg2.connect(**PG_CONFIG)
        pg_conn.autocommit = False
        logger.info(
            "PostgreSQL 연결 완료: %s:%s/%s",
            PG_CONFIG["host"], PG_CONFIG["port"], PG_CONFIG["dbname"],
        )

        # ── Neo4j Driver 초기화 ────────────────────────────────────────────
        # GraphDatabase.driver()는 thread-safe, session은 thread-unsafe
        neo4j_driver = GraphDatabase.driver(
            NEO4J_URI,
            auth=(NEO4J_USER, NEO4J_PASSWORD),
            # 연결 풀 설정: 프로덕션 환경에 맞게 조정 가능
            max_connection_pool_size=10,
            connection_timeout=15,
        )

        # 연결 유효성 검증 (실제 네트워크 연결 시도)
        neo4j_driver.verify_connectivity()
        logger.info("Neo4j 연결 확인 완료: %s (DB: %s)", NEO4J_URI, NEO4J_DATABASE)

        # ── 스키마 보장 ────────────────────────────────────────────────────
        ensure_neo4j_schema(neo4j_driver)

        # ── 이관 실행 ──────────────────────────────────────────────────────
        transfer_msds_to_neo4j(pg_conn, neo4j_driver)

    except ServiceUnavailable as e:
        logger.error("Neo4j 연결 불가: %s", e)
        raise
    except psycopg2.OperationalError as e:
        logger.error("PostgreSQL 연결 불가: %s", e)
        raise
    except Exception:
        logger.exception("Neo4j 이관 중 치명적 오류 발생")
        raise
    finally:
        if neo4j_driver:
            neo4j_driver.close()
            logger.info("Neo4j Driver 종료")
        if pg_conn and not pg_conn.closed:
            pg_conn.close()
            logger.info("PostgreSQL 연결 종료")


# ─────────────────────────────────────────────────────────────────────────────
# 단독 실행 진입점
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_neo4j_transfer()
