# -*- coding: utf-8 -*-
"""
벌크 액체화학물질 호환성 그룹(Compatibility Group) Neo4j 이관 모듈
==========================================================================
파이프라인 위치: PostgreSQL(msds_chemical.cas_no) -> [이 모듈] -> Neo4j

msds_neo4j_loader.py(INCOMPATIBLE_WITH/IS_CLASSIFIED_AS, MSDS 텍스트 마이닝)와
imdg_segregation_loader.py(SEGREGATE, IMDG Code Chapter 7.2 공인 격리표)에 이은
세 번째 독립 신호 — 산적(bulk) 액체화학물질 전용 호환성 그룹 분류(반응성 그룹
1~22 / 화물 그룹 30~43 구조, 국제 업계에서 널리 참조되는 산적 화학물질 운송
호환성 차트를 재구성). 세 신호는 근거가 서로 다르므로 하나로 합치지 않고
병행한다.

★ 이 파일이 이 참조 데이터의 유일한 원본(source of truth)이다. 처음에는
  backend에도 같은 데이터를 정적 Python dict로 복제해 뒀었으나(안전관제
  에이전트의 "빠른 경로"), 챗봇(GraphRAG)이 Neo4j만 조회하다 보니 그 dict가
  안 보여 "안전관제 에이전트는 위험하다고 막는 조합인데 챗봇은 정보 없음이라
  답하는" 불일치가 생겼다(2026-08-21 발견 — 신규 31개 판정 조합 중 10개,
  32%가 이 문제에 해당했다). 이후 안전관제 에이전트(backend/app/agents/safety/
  bulk_compatibility.py, graph_queries.py)도 이 그래프를 조회하도록 전환해
  "판정 로직의 권위는 하나"라는 원칙에 맞췄다 — 이제 데이터를 바꿀 곳은 이
  파일 하나뿐이다. 수정 후에는 이 로더를 재실행해 그래프를 갱신할 것.

알려진 한계:
    - 그룹 번호 체계(반응성그룹 1~22/화물그룹 30~43)의 원출처는 미국 해안경비대
      (USCG) 46 CFR Part 150 계열의 호환성 차트다. 국내에도 한국해사위험물검사원
      (KOMDI)이 운영하는 "산적케미칼 격리 툴(STBC)"이라는 유사 체계가 실재하나,
      이 데이터가 STBC의 분류와 수치까지 정확히 일치하는지는 별도로 대조되지
      않았다 — 참고 신호로만 다룰 것.
    - 36종 화학물질 커버리지 밖(신규 화학물질)은 자동으로 매핑되지 않는다.

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
logger = logging.getLogger("bulk_compatibility_neo4j_loader")

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

# ─────────────────────────────────────────────────────────────────────────────
# 참조 데이터 — backend/app/agents/safety/bulk_compatibility.py와 동일(위 docstring 참고)
# ─────────────────────────────────────────────────────────────────────────────

GROUP_TYPE_REACTIVE = "reactive"
GROUP_TYPE_CARGO = "cargo"
GROUP_TYPE_SPECIAL = "special"

# CAS번호 -> (그룹번호, 그룹구분, 그룹명)
CAS_TO_GROUP: dict[str, tuple[int, str, str]] = {
    "7664-93-9": (2, GROUP_TYPE_REACTIVE, "Sulfuric Acids"),
    "7664-41-7": (6, GROUP_TYPE_REACTIVE, "Ammonia"),
    "102-71-6": (8, GROUP_TYPE_REACTIVE, "Alkanolamines"),
    "26471-62-5": (12, GROUP_TYPE_REACTIVE, "Isocyanates"),
    "107-13-1": (15, GROUP_TYPE_REACTIVE, "Substituted Allyls"),
    "75-56-9": (16, GROUP_TYPE_REACTIVE, "Alkylene Oxides"),
    "67-64-1": (18, GROUP_TYPE_REACTIVE, "Ketones"),
    "67-63-0": (20, GROUP_TYPE_REACTIVE, "Alcohols, Glycols"),
    "67-56-1": (20, GROUP_TYPE_REACTIVE, "Alcohols, Glycols"),
    "64-17-5": (20, GROUP_TYPE_REACTIVE, "Alcohols, Glycols"),
    "107-21-1": (20, GROUP_TYPE_REACTIVE, "Alcohols, Glycols"),
    "74-85-1": (30, GROUP_TYPE_CARGO, "Olefins"),
    "115-07-1": (30, GROUP_TYPE_CARGO, "Olefins"),
    "100-42-5": (30, GROUP_TYPE_CARGO, "Olefins"),
    "106-99-0": (30, GROUP_TYPE_CARGO, "Olefins"),
    "74-82-8": (31, GROUP_TYPE_CARGO, "Paraffins"),
    "74-98-6": (31, GROUP_TYPE_CARGO, "Paraffins"),
    "106-97-8": (31, GROUP_TYPE_CARGO, "Paraffins"),
    "71-43-2": (32, GROUP_TYPE_CARGO, "Aromatic Hydrocarbons"),
    "108-88-3": (32, GROUP_TYPE_CARGO, "Aromatic Hydrocarbons"),
    "1330-20-7": (32, GROUP_TYPE_CARGO, "Aromatic Hydrocarbons"),
    "106-42-3": (32, GROUP_TYPE_CARGO, "Aromatic Hydrocarbons"),
    "86290-81-5": (33, GROUP_TYPE_CARGO, "Misc. Hydrocarbon Mixtures"),
    "8030-30-6": (33, GROUP_TYPE_CARGO, "Misc. Hydrocarbon Mixtures"),
    "8002-05-9": (33, GROUP_TYPE_CARGO, "Misc. Hydrocarbon Mixtures"),
    "68334-30-5": (33, GROUP_TYPE_CARGO, "Misc. Hydrocarbon Mixtures"),
    "8006-61-9": (33, GROUP_TYPE_CARGO, "Misc. Hydrocarbon Mixtures"),
    "8008-20-6": (33, GROUP_TYPE_CARGO, "Misc. Hydrocarbon Mixtures"),
    "68476-33-5": (33, GROUP_TYPE_CARGO, "Misc. Hydrocarbon Mixtures"),
    "8052-42-4": (33, GROUP_TYPE_CARGO, "Misc. Hydrocarbon Mixtures"),
    "97280-83-6": (34, GROUP_TYPE_CARGO, "Esters"),
    "75-01-4": (35, GROUP_TYPE_CARGO, "Vinyl Halides"),
    "79-01-6": (36, GROUP_TYPE_CARGO, "Halogenated Hydrocarbons"),
    "9082-00-2": (40, GROUP_TYPE_CARGO, "Glycol Ethers"),
    "109-99-9": (41, GROUP_TYPE_CARGO, "Ethers"),
    "1333-74-0": (0, GROUP_TYPE_SPECIAL, "Unassigned Gas"),
}

INCOMPATIBLE_GROUP_PAIRS: frozenset[frozenset[int]] = frozenset({
    frozenset({2, 30}), frozenset({2, 20}), frozenset({2, 40}), frozenset({2, 41}),
    frozenset({2, 15}), frozenset({2, 8}), frozenset({2, 16}), frozenset({2, 12}),
    frozenset({2, 6}), frozenset({2, 18}),
    frozenset({12, 20}), frozenset({12, 40}), frozenset({12, 6}), frozenset({12, 8}),
    frozenset({12, 15}), frozenset({12, 16}), frozenset({12, 18}),
    frozenset({15, 16}), frozenset({15, 6}), frozenset({16, 6}), frozenset({16, 8}),
})

_EXCEPTION_SAFE: frozenset[frozenset[str]] = frozenset({
    frozenset({"102-71-6", "107-13-1"}),
})
_EXCEPTION_BLOCKED: frozenset[frozenset[str]] = frozenset({
    frozenset({"9082-00-2", "107-13-1"}),
})

_GROUP_NAMES: dict[int, tuple[str, str]] = {
    num: (gtype, name) for num, gtype, name in CAS_TO_GROUP.values()
}

# ─────────────────────────────────────────────────────────────────────────────
# PostgreSQL -> chem_id/cas_no 추출 (이 참조 축의 커버리지에 있는 것만)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_chemical_cas(pg_conn) -> list[dict]:
    with pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT chem_id, cas_no FROM msds_chemical WHERE cas_no IS NOT NULL")
        rows = cur.fetchall()
    return [dict(row) for row in rows if row["cas_no"] in CAS_TO_GROUP]


# ─────────────────────────────────────────────────────────────────────────────
# Cypher 쿼리
# ─────────────────────────────────────────────────────────────────────────────

_NEO4J_CONSTRAINT_STMTS: list[str] = [
    "CREATE CONSTRAINT compat_group_no_unique IF NOT EXISTS "
    "FOR (g:CompatibilityGroup) REQUIRE g.group_no IS UNIQUE",
]


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


_CYPHER_SET_CHEMICAL_GROUP = """
UNWIND $batch AS row
MATCH (c:Chemical {id: row.chem_id})
MERGE (g:CompatibilityGroup {group_no: row.group_no})
ON CREATE SET g.group_type = row.group_type, g.name = row.group_name, g.created_at = datetime()
MERGE (c)-[r:IN_COMPATIBILITY_GROUP]->(g)
ON CREATE SET r.created_at = datetime()
"""

# 양방향으로 두 관계를 만들어 조회 시 방향 UNION 없이 한 방향 MATCH만으로
# 불호환 여부를 찾을 수 있게 한다(imdg_segregation_loader.py의 SEGREGATE와 동일 패턴).
_CYPHER_MERGE_INCOMPATIBLE_GROUP = """
UNWIND $batch AS row
MATCH (a:CompatibilityGroup {group_no: row.group_a})
MATCH (b:CompatibilityGroup {group_no: row.group_b})
MERGE (a)-[r1:INCOMPATIBLE_WITH_GROUP]->(b)
ON CREATE SET r1.created_at = datetime()
MERGE (b)-[r2:INCOMPATIBLE_WITH_GROUP]->(a)
ON CREATE SET r2.created_at = datetime()
"""

_CYPHER_MERGE_SAFE_EXCEPTION = """
UNWIND $batch AS row
MATCH (a:Chemical {id: row.chem_id_a})
MATCH (b:Chemical {id: row.chem_id_b})
MERGE (a)-[r1:BULK_COMPAT_SAFE_EXCEPTION]->(b)
ON CREATE SET r1.created_at = datetime()
MERGE (b)-[r2:BULK_COMPAT_SAFE_EXCEPTION]->(a)
ON CREATE SET r2.created_at = datetime()
"""

_CYPHER_MERGE_BLOCKED_EXCEPTION = """
UNWIND $batch AS row
MATCH (a:Chemical {id: row.chem_id_a})
MATCH (b:Chemical {id: row.chem_id_b})
MERGE (a)-[r1:BULK_COMPAT_BLOCKED_EXCEPTION]->(b)
ON CREATE SET r1.created_at = datetime()
MERGE (b)-[r2:BULK_COMPAT_BLOCKED_EXCEPTION]->(a)
ON CREATE SET r2.created_at = datetime()
"""


def _tx_set_chemical_group(tx, batch: list[dict]) -> None:
    tx.run(_CYPHER_SET_CHEMICAL_GROUP, batch=batch)


def _tx_merge_incompatible_group(tx, batch: list[dict]) -> None:
    tx.run(_CYPHER_MERGE_INCOMPATIBLE_GROUP, batch=batch)


def _tx_merge_safe_exception(tx, batch: list[dict]) -> None:
    tx.run(_CYPHER_MERGE_SAFE_EXCEPTION, batch=batch)


def _tx_merge_blocked_exception(tx, batch: list[dict]) -> None:
    tx.run(_CYPHER_MERGE_BLOCKED_EXCEPTION, batch=batch)


def transfer_bulk_compatibility_to_neo4j(pg_conn, neo4j_driver) -> None:
    chem_rows = fetch_chemical_cas(pg_conn)
    if not chem_rows:
        logger.warning("호환성 그룹 참조 데이터에 매칭되는 화학물질이 없습니다.")
        return

    cas_to_chem_id = {row["cas_no"]: row["chem_id"] for row in chem_rows}

    group_batch = [
        {
            "chem_id": row["chem_id"],
            "group_no": CAS_TO_GROUP[row["cas_no"]][0],
            "group_type": CAS_TO_GROUP[row["cas_no"]][1],
            "group_name": CAS_TO_GROUP[row["cas_no"]][2],
        }
        for row in chem_rows
    ]

    present_groups = {CAS_TO_GROUP[row["cas_no"]][0] for row in chem_rows}
    incompatible_batch = []
    for pair in INCOMPATIBLE_GROUP_PAIRS:
        a, b = tuple(pair)  # 항상 서로 다른 정수 2개(frozenset 구성 방식상 보장됨)
        if a in present_groups and b in present_groups:
            incompatible_batch.append({"group_a": a, "group_b": b})

    safe_exception_batch = []
    for pair in _EXCEPTION_SAFE:
        a, b = tuple(pair)
        if a in cas_to_chem_id and b in cas_to_chem_id:
            safe_exception_batch.append({"chem_id_a": cas_to_chem_id[a], "chem_id_b": cas_to_chem_id[b]})

    blocked_exception_batch = []
    for pair in _EXCEPTION_BLOCKED:
        a, b = tuple(pair)
        if a in cas_to_chem_id and b in cas_to_chem_id:
            blocked_exception_batch.append({"chem_id_a": cas_to_chem_id[a], "chem_id_b": cas_to_chem_id[b]})

    with neo4j_driver.session(database=NEO4J_DATABASE) as session:
        session.execute_write(_tx_set_chemical_group, group_batch)
        if incompatible_batch:
            session.execute_write(_tx_merge_incompatible_group, incompatible_batch)
        if safe_exception_batch:
            session.execute_write(_tx_merge_safe_exception, safe_exception_batch)
        if blocked_exception_batch:
            session.execute_write(_tx_merge_blocked_exception, blocked_exception_batch)

    logger.info(
        "벌크 호환성그룹 그래프 이관 완료: Chemical %d개 (그룹 %d종), "
        "INCOMPATIBLE_WITH_GROUP 쌍 %d개, 예외(안전) %d건, 예외(차단) %d건",
        len(chem_rows), len(present_groups), len(incompatible_batch),
        len(safe_exception_batch), len(blocked_exception_batch),
    )


def run_bulk_compatibility_transfer() -> None:
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
        transfer_bulk_compatibility_to_neo4j(pg_conn, neo4j_driver)

    except ServiceUnavailable as e:
        logger.error("Neo4j 연결 불가: %s", e)
        raise
    except psycopg2.OperationalError as e:
        logger.error("PostgreSQL 연결 불가: %s", e)
        raise
    except Exception:
        logger.exception("벌크 호환성그룹 그래프 이관 중 치명적 오류 발생")
        raise
    finally:
        if neo4j_driver:
            neo4j_driver.close()
            logger.info("Neo4j Driver 종료")
        if pg_conn and not pg_conn.closed:
            pg_conn.close()
            logger.info("PostgreSQL 연결 종료")


if __name__ == "__main__":
    run_bulk_compatibility_transfer()
