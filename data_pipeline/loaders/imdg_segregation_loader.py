# -*- coding: utf-8 -*-
"""
IMDG Code 공인 일반 격리표(General Segregation Table) Neo4j 이관 모듈
==========================================================================
파이프라인 위치: PostgreSQL(msds_chemical.msds_payload) -> [이 모듈] -> Neo4j

msds_neo4j_loader.py가 만드는 INCOMPATIBLE_WITH/IS_CLASSIFIED_AS 그래프는
MSDS 원문 텍스트(J코드, 안정성·반응성 섹션)를 정규식으로 훑어 만든 근사치다
(msds_neo4j_loader.py 상단 주석 참고). 이 모듈은 그것과는 다른, IMDG Code
Chapter 7.2의 "일반 격리표"(공인 국제 규정)를 별도 레이어로 추가한다.

두 신호는 서로 다른 근거를 가지므로 하나로 합치지 않고 병행한다:
  - INCOMPATIBLE_WITH: 이 화학물질 특유의 반응성 위험 (MSDS 텍스트 마이닝)
  - HAS_IMDG_CLASS -> SEGREGATE: 위험물 대분류(Class) 간 공인 국제 격리 규정

UN Class(IMDG Class) 데이터 출처:
    KOSHA MSDS 14번 항목("운송에 필요한 정보", detail14)의 msdsItemCode="N06"
    ("운송에서의 위험성 등급")에 이미 공식 IMDG Class 값이 들어있다
    (예: 벤젠 -> "3"). 별도 UN번호->Class 매핑 데이터를 구할 필요 없이
    기존에 수집된 MSDS 원문에서 바로 추출한다.

일반 격리표 데이터 출처:
    IMDG Code Chapter 7.2 "Segregation Table" (전 세계 위험물 운송 교육 자료에
    널리 재수록되는 공개된 규정표. 표 자체의 구조와 수치는 IMO가 정한 공식
    규정이며 저작권으로 보호되는 것은 IMDG Code 원문 조항 서술이다).
    코드 의미: 1=Away from(수평 3m 이상), 2=Separated from(다른 격창),
    3=Separated by a complete compartment or hold from,
    4=Separated longitudinally by an intervening complete compartment or
    hold from. X=일반 규정 없음(개별 위험물목록 확인 필요).

    아래 IMDG_GENERAL_SEGREGATION_TABLE은 전체 17개 Class 조합을 담고 있지만,
    이 로더는 우리 34종 화물에 실제로 등장하는 Class만 ImdgClass 노드로
    만든다(현재: 2.1, 2.3, 3, 6.1, 8, 9 — 6종). 새 화학물질이 추가돼 새로운
    Class가 등장하면 자동으로 해당 Class 노드도 함께 생성된다.

알려진 한계:
    - "격리 코드 -> RiskLevel" 매핑(1→주의, 2→위험, 3·4→배정불가)은 IMDG의
      물리적 이격 거리 규정을 안전관제 4단계 등급 체계로 옮긴 것으로, IMDG
      자체가 정한 공식 환산표가 아니라 이번 구현에서 내린 해석이다
      (rule_engine.py에 문서화).
    - "particular provisions"(개별 위험물목록의 화물별 특칙)는 반영하지
      않는다 — 일반 격리표(class 단위)만 반영.

외부 의존 라이브러리:
    pip install psycopg2-binary neo4j python-dotenv

환경변수: msds_neo4j_loader.py와 동일 (POSTGRES_*, NEO4J_*)
"""

import logging
import os

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from neo4j import GraphDatabase
from neo4j.exceptions import ClientError as Neo4jClientError
from neo4j.exceptions import ServiceUnavailable

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("imdg_segregation_loader")

_REQUIRED_VARS = [
    "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD",
    "NEO4J_URI", "NEO4J_USER", "NEO4J_PASSWORD", "NEO4J_DATABASE",
]
_missing = [k for k in _REQUIRED_VARS if not os.getenv(k)]
if _missing:
    raise RuntimeError(f"DB 접속정보가 없습니다. .env에 설정하세요: {', '.join(_missing)}")

PG_CONFIG: dict = {
    "host":            os.environ["POSTGRES_HOST"],
    "port":            int(os.environ["POSTGRES_PORT"]),
    "dbname":          os.environ["POSTGRES_DB"],
    "user":            os.environ["POSTGRES_USER"],
    "password":        os.environ["POSTGRES_PASSWORD"],
    "connect_timeout": 10,
    "options":         "-c search_path=public",
}

NEO4J_URI      = os.environ["NEO4J_URI"]
NEO4J_USER     = os.environ["NEO4J_USER"]
NEO4J_PASSWORD = os.environ["NEO4J_PASSWORD"]
NEO4J_DATABASE = os.environ["NEO4J_DATABASE"]

_NULL_CLASS_VALUES = frozenset({"해당없음", "-", "", None})

# ─────────────────────────────────────────────────────────────────────────────
# IMDG Code Chapter 7.2 일반 격리표 (전체 17개 Class, 출처: IMDG Code 공식
# Segregation Table의 공개 재수록본 — 표 구조/수치는 IMO 공식 규정).
# 대칭 행렬이므로 한쪽 방향만 채우고 로더가 양방향으로 적재한다.
# ─────────────────────────────────────────────────────────────────────────────
_CLASSES: list[str] = [
    "1.1", "1.3", "1.4", "2.1", "2.2", "2.3", "3", "4.1", "4.2", "4.3",
    "5.1", "5.2", "6.1", "6.2", "7", "8", "9",
]

# 원문 표 순서 그대로 (행: _CLASSES 순서, 열: _CLASSES 순서). "*"는 폭발물 등급
# 간 특칙(이번 데이터셋에 Class 1이 없어 사용되지 않음)이라 X로 취급한다.
_RAW_ROWS: list[str] = [
    "*  *  *  4  2  2  4  4  4  4  4  4  2  4  2  4  X",  # 1.1,1.2,1.5
    "*  *  *  4  2  2  4  3  3  4  4  4  2  4  2  2  X",  # 1.3,1.6
    "*  *  *  2  1  1  2  2  2  2  2  2  X  4  2  2  X",  # 1.4
    "4  4  2  X  X  X  2  1  2  2  2  2  X  4  2  1  X",  # 2.1
    "2  2  1  X  X  X  1  X  1  X  X  1  X  2  1  X  X",  # 2.2
    "2  2  1  X  X  X  2  X  2  X  X  2  X  2  1  X  X",  # 2.3
    "4  4  2  2  1  2  X  X  2  2  2  2  X  3  2  X  X",  # 3
    "4  3  2  1  X  X  X  X  1  X  1  2  X  3  2  1  X",  # 4.1
    "4  3  2  2  1  2  2  1  X  1  2  2  1  3  2  1  X",  # 4.2
    "4  4  2  2  X  X  2  X  1  X  2  2  X  2  2  1  X",  # 4.3
    "4  4  2  2  X  X  2  1  2  2  X  2  1  3  1  2  X",  # 5.1
    "4  4  2  2  1  2  2  2  2  2  2  X  1  3  2  2  X",  # 5.2
    "2  2  X  X  X  X  X  X  1  X  1  1  X  1  X  X  X",  # 6.1
    "4  4  4  4  2  2  3  3  3  2  3  3  1  X  3  3  X",  # 6.2
    "2  2  2  2  1  1  2  2  2  2  1  2  X  3  X  2  X",  # 7
    "4  2  2  1  X  X  X  1  1  1  2  2  X  3  2  X  X",  # 8
    "X  X  X  X  X  X  X  X  X  X  X  X  X  X  X  X  X",  # 9
]


def _build_segregation_table() -> dict[tuple[str, str], str]:
    """(class_a, class_b) -> 격리코드("1"~"4"). X/* 조합은 아예 딕셔너리에 안 넣는다."""
    table: dict[tuple[str, str], str] = {}
    for i, row_str in enumerate(_RAW_ROWS):
        values = row_str.split()
        for j, code in enumerate(values):
            if code in ("X", "*"):
                continue
            class_a, class_b = _CLASSES[i], _CLASSES[j]
            table[(class_a, class_b)] = code
    return table


def _build_no_segregation_pairs() -> set[tuple[str, str]]:
    """공인 표상 "X"(격리 불필요 — 모호함이 아니라 확정된 안전 답변)인 (class_a, class_b)
    조합. "*"(폭발물류 특칙, 이번 34~36종 화물에는 Class 1이 없어 해당 없음)는 제외한다
    — "*"는 "불필요"가 아니라 "표에서 별도 규정 참조"라 X와 성격이 달라, 안전측으로
    여기 포함하지 않고 여전히 미확인으로 남겨 둔다."""
    pairs: set[tuple[str, str]] = set()
    for i, row_str in enumerate(_RAW_ROWS):
        values = row_str.split()
        for j, code in enumerate(values):
            if code == "X":
                pairs.add((_CLASSES[i], _CLASSES[j]))
    return pairs


IMDG_GENERAL_SEGREGATION_TABLE: dict[tuple[str, str], str] = _build_segregation_table()
IMDG_NO_SEGREGATION_PAIRS: set[tuple[str, str]] = _build_no_segregation_pairs()


def _normalize_class(raw: str | None) -> str | None:
    """MSDS N06 값을 IMDG Class 표기로 정규화. "2.1"은 그대로, 소분류 없는 경우 대비."""
    if raw in _NULL_CLASS_VALUES:
        return None
    return raw.strip()


_NEO4J_CONSTRAINT_STMTS: list[str] = [
    "CREATE CONSTRAINT imdg_class_code_unique IF NOT EXISTS "
    "FOR (i:ImdgClass) REQUIRE i.code IS UNIQUE",
]

# 2026-08-21 추가 — "X"(공인 표상 격리 불필요) 조합도 명시적으로 그래프에 남긴다.
# 배경: 안전관제 에이전트는 "SEGREGATE 관계 없음"을 "코드 X인지, 이 Class 조합이
# 그래프에 아예 없는지 구분 못 함"으로 보고 최소 주의로 격상하는 fail-safe를 쓴다
# (backend/app/agents/safety/rule_engine.py의 compute_imdg_unconfirmed_floor).
# 그런데 실측 확인 결과 이 프로젝트가 다루는 34~36종 대부분이 Class "3"(인화성
# 액체) 하나에 몰려 있고(19종), IMDG 공인 표는 **모든 Class가 자기 자신과는
# 예외 없이 X**다(같은 등급끼리는 격리 불필요 — 17개 Class 대각선 전부 확인).
# 즉 "같은 Class끼리"는 절대 모호하지 않은 확정된 안전 답변인데, 기존 구조로는
# 이것도 "미확인"으로 오분류돼 같은 Class 화물끼리(예: 벤젠-가솔린, 둘 다
# Class 3)마다 불필요한 "주의" 경고가 스팸처럼 발생했다. 이 관계를 명시적으로
# 적재해서, 안전관제 에이전트가 "SEGREGATE도 없고 이 관계도 없는" 진짜 커버리지
# 밖 조합만 미확인으로 다루게 한다.
_CYPHER_MERGE_NO_SEGREGATION_REQUIRED = """
UNWIND $batch AS row
MATCH (a:ImdgClass {code: row.class_a})
MATCH (b:ImdgClass {code: row.class_b})
MERGE (a)-[r1:NO_SEGREGATION_REQUIRED]->(b)
ON CREATE SET r1.created_at = datetime()
MERGE (b)-[r2:NO_SEGREGATION_REQUIRED]->(a)
ON CREATE SET r2.created_at = datetime()
"""


def ensure_neo4j_schema(driver) -> None:
    with driver.session(database=NEO4J_DATABASE) as session:
        for stmt in _NEO4J_CONSTRAINT_STMTS:
            try:
                session.run(stmt)
            except Neo4jClientError as e:
                if "already exists" in str(e).lower() or "equivalent" in str(e).lower():
                    logger.debug("제약조건 이미 존재 (스킵): %s...", stmt[:50])
                else:
                    raise
    logger.info("Neo4j 스키마(제약조건) 확인/생성 완료")


# ─────────────────────────────────────────────────────────────────────────────
# PostgreSQL -> IMDG Class 추출
# ─────────────────────────────────────────────────────────────────────────────

def fetch_chemical_imdg_classes(pg_conn) -> list[dict]:
    """msds_chemical.msds_payload.detail14의 N06(운송에서의 위험성 등급)을 추출한다."""
    with pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT chem_id, name_ko,
                   (SELECT item->>'itemDetail'
                    FROM jsonb_array_elements(msds_payload->'detail14'->'data') item
                    WHERE item->>'msdsItemCode' = 'N06') AS imdg_class_raw
            FROM msds_chemical
        """)
        rows = cur.fetchall()

    result = []
    for row in rows:
        imdg_class = _normalize_class(row["imdg_class_raw"])
        if imdg_class is None:
            logger.debug("IMDG Class 없음(운송 비대상으로 간주): %s (%s)", row["chem_id"], row["name_ko"])
            continue
        result.append({"chem_id": row["chem_id"], "imdg_class": imdg_class})
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Cypher 쿼리
# ─────────────────────────────────────────────────────────────────────────────

_CYPHER_SET_CHEMICAL_CLASS = """
UNWIND $batch AS row
MATCH (c:Chemical {id: row.chem_id})
MERGE (i:ImdgClass {code: row.imdg_class})
ON CREATE SET i.created_at = datetime()
SET c.imdg_class = row.imdg_class
MERGE (c)-[r:HAS_IMDG_CLASS]->(i)
ON CREATE SET r.created_at = datetime()
"""

# 양방향 관계를 명시적으로 두 개 만들어 조회 시 방향 UNION 없이 한 방향
# MATCH만으로 격리 대상을 찾을 수 있게 한다 (berth_neo4j_loader.py의
# ADJACENT_TO와 동일한 패턴).
_CYPHER_MERGE_SEGREGATE = """
UNWIND $batch AS row
MATCH (a:ImdgClass {code: row.class_a})
MATCH (b:ImdgClass {code: row.class_b})
MERGE (a)-[r1:SEGREGATE {code: row.seg_code}]->(b)
ON CREATE SET r1.created_at = datetime()
MERGE (b)-[r2:SEGREGATE {code: row.seg_code}]->(a)
ON CREATE SET r2.created_at = datetime()
"""


def _tx_set_chemical_class(tx, batch: list[dict]) -> None:
    tx.run(_CYPHER_SET_CHEMICAL_CLASS, batch=batch)


def _tx_merge_segregate(tx, batch: list[dict]) -> None:
    tx.run(_CYPHER_MERGE_SEGREGATE, batch=batch)


def _tx_merge_no_segregation_required(tx, batch: list[dict]) -> None:
    tx.run(_CYPHER_MERGE_NO_SEGREGATION_REQUIRED, batch=batch)


def transfer_imdg_segregation_to_neo4j(pg_conn, neo4j_driver) -> None:
    chem_rows = fetch_chemical_imdg_classes(pg_conn)
    if not chem_rows:
        logger.warning("IMDG Class가 있는 화학물질이 없습니다.")
        return

    present_classes = {row["imdg_class"] for row in chem_rows}

    # 실제 데이터에 등장하는 Class 조합만 SEGREGATE 관계로 적재 (X/미등장 Class 제외)
    segregate_batch = [
        {"class_a": a, "class_b": b, "seg_code": code}
        for (a, b), code in IMDG_GENERAL_SEGREGATION_TABLE.items()
        if a in present_classes and b in present_classes
    ]
    no_segregation_batch = [
        {"class_a": a, "class_b": b}
        for (a, b) in IMDG_NO_SEGREGATION_PAIRS
        if a in present_classes and b in present_classes
    ]

    with neo4j_driver.session(database=NEO4J_DATABASE) as session:
        session.execute_write(_tx_set_chemical_class, chem_rows)
        if segregate_batch:
            session.execute_write(_tx_merge_segregate, segregate_batch)
        if no_segregation_batch:
            session.execute_write(_tx_merge_no_segregation_required, no_segregation_batch)

    logger.info(
        "IMDG 격리 그래프 이관 완료: Chemical %d개 (Class %d종), SEGREGATE 쌍 %d개, "
        "NO_SEGREGATION_REQUIRED(공인 X) 쌍 %d개",
        len(chem_rows), len(present_classes), len(segregate_batch), len(no_segregation_batch),
    )


def run_imdg_segregation_transfer() -> None:
    pg_conn = None
    neo4j_driver = None
    try:
        pg_conn = psycopg2.connect(**PG_CONFIG)
        pg_conn.autocommit = False
        logger.info(
            "PostgreSQL 연결 완료: %s:%s/%s",
            PG_CONFIG["host"], PG_CONFIG["port"], PG_CONFIG["dbname"],
        )

        neo4j_driver = GraphDatabase.driver(
            NEO4J_URI,
            auth=(NEO4J_USER, NEO4J_PASSWORD),
            max_connection_pool_size=10,
            connection_timeout=15,
        )
        neo4j_driver.verify_connectivity()
        logger.info("Neo4j 연결 확인 완료: %s (DB: %s)", NEO4J_URI, NEO4J_DATABASE)

        ensure_neo4j_schema(neo4j_driver)
        transfer_imdg_segregation_to_neo4j(pg_conn, neo4j_driver)

    except ServiceUnavailable as e:
        logger.error("Neo4j 연결 불가: %s", e)
        raise
    except psycopg2.OperationalError as e:
        logger.error("PostgreSQL 연결 불가: %s", e)
        raise
    except Exception:
        logger.exception("IMDG 격리 그래프 이관 중 치명적 오류 발생")
        raise
    finally:
        if neo4j_driver:
            neo4j_driver.close()
            logger.info("Neo4j Driver 종료")
        if pg_conn and not pg_conn.closed:
            pg_conn.close()
            logger.info("PostgreSQL 연결 종료")


if __name__ == "__main__":
    run_imdg_segregation_transfer()
