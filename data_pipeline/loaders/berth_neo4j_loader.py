# -*- coding: utf-8 -*-
"""
선석(Berth) Neo4j 이관 모듈 — PostgreSQL(upa_berth_facility) -> 지식 그래프
==========================================================================
파이프라인 위치: PostgreSQL (upa_berth_facility) -> [이 모듈] -> Neo4j

msds_neo4j_loader.py와 동일한 설계 원칙(MERGE 멱등성, UNWIND 배치, 환경변수
기본값 없음)을 따른다. 이 모듈이 만드는 그래프는 스케줄링 에이전트가 선석
후보를 조회하는 데 쓰인다.

그래프 모델:
┌──────────────────────────────────────────────────────────────────────────┐
│  (:Berth {id, wharf_name, port_name, depth_m, length_m, ...})            │
│       │                              │                                    │
│  [:HANDLES]                   [:ADJACENT_TO]                             │
│       ↓                              ↓                                    │
│  (:CargoCategory {name})         (:Berth)  (다른 선석, 상호 관계)          │
│                                                                           │
│  데이터 출처:                                                             │
│  - Berth         <- upa_berth_facility (함현우 담당 data-pipeline UPA 로더가 │
│                      이미 적재한 테이블. 이 모듈은 읽기만 한다)             │
│  - CargoCategory <- handling_cargo_name 콤마 분리 (예: "잡화, 액체화학")   │
│  - ADJACENT_TO   <- PILOT_ADJACENT_PAIRS 수동 큐레이션 (아래 설명)         │
└──────────────────────────────────────────────────────────────────────────┘

CargoCategory 노드 이름은 cargo_category_loader.py가 Chemical.cargo_category에
쓰는 값("원유"/"유류"/"액체화학")과 동일한 문자열을 그대로 쓴다. 두 로더가 같은
문자열 상수를 쓰기 때문에, 스케줄링 에이전트는 화학물질의 cargo_category
속성값으로 바로 `MATCH (b:Berth)-[:HANDLES]->(:CargoCategory {name: $category})`
조회가 가능하다.

알려진 한계 (문서화):
    - ADJACENT_TO는 좌표 기반 자동 계산이 아니다. 수집된 69개 선석 중 다수가
      좌표 결측(MISSING_COORDINATE)이고 부두 폴리곤 데이터도 없어 전체 자동
      인접성 계산은 이번 범위 밖이다. 대신 수행계획서(예상 시나리오, 14p)가
      지정한 5개 파일럿 선석군(정일1·2, OTK1·2, 현대오일신항1·2, SK5~8,
      북신항 에너지부두)만 PILOT_ADJACENT_PAIRS에 수동으로 등록했다.
      나머지 선석은 인접 선석 정보 없이 그래프에만 존재한다.
    - handling_cargo_name이 "잡화"/"컨테이너"/"광석" 등인 비액체 선석도 그대로
      Berth/CargoCategory 노드로 적재된다(스케줄링 에이전트가 액체화학 계열
      3개 카테고리로만 필터링하므로 조회 결과에는 영향 없음).
    - 동일 wharf_name이 서로 다른 부두(민유/국유 등)로 중복 등록된 경우
      (수집 데이터 기준 "SK2부두" 1건) wharf_se_name을 id에 덧붙여 구분한다.

외부 의존 라이브러리:
    pip install psycopg2-binary neo4j python-dotenv

환경변수 (.env 또는 OS 환경변수, 기본값 없음 — 미설정 시 에러):
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
    NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, NEO4J_DATABASE
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
logger = logging.getLogger("berth_neo4j_loader")

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

# 수행계획서 14p "시뮬레이션 적용 대상 부두"가 지정한 5개 파일럿 선석군.
# wharf_name은 upa_berth_facility_stg.csv 실제 값과 정확히 일치해야 한다.
PILOT_ADJACENT_PAIRS: list[tuple[str, str]] = [
    ("정일1부두", "정일2부두"),
    ("OTK1부두", "OTK2부두"),
    ("현대오일터미널 신항1부두", "현대오일터미널 신항2부두"),
    ("SK5부두", "SK6부두"),
    ("SK6부두", "SK7부두"),
    ("SK7부두", "SK8부두"),
    ("북신항 에너지부두", "신항북방파제 에너지부두"),
]

_NEO4J_CONSTRAINT_STMTS: list[str] = [
    "CREATE CONSTRAINT berth_id_unique IF NOT EXISTS "
    "FOR (b:Berth) REQUIRE b.id IS UNIQUE",
    "CREATE CONSTRAINT cargo_category_name_unique IF NOT EXISTS "
    "FOR (cat:CargoCategory) REQUIRE cat.name IS UNIQUE",
]


def ensure_neo4j_schema(driver) -> None:
    """Berth/CargoCategory 제약조건을 멱등성 있게 생성한다."""
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
# PostgreSQL -> 배치 행 변환
# ─────────────────────────────────────────────────────────────────────────────

def _split_cargo_categories(handling_cargo_name: str | None) -> list[str]:
    """"잡화, 액체화학" 같은 콤마 구분 텍스트를 개별 카테고리 목록으로 분리."""
    if not handling_cargo_name:
        return []
    return [token.strip() for token in handling_cargo_name.split(",") if token.strip()]


def fetch_berth_rows(pg_conn) -> list[dict]:
    """upa_berth_facility 전체를 읽어 Neo4j 배치 행으로 변환한다.

    동일 wharf_name이 여러 부두 운영주체로 중복 등록된 경우(SK2부두 등)
    wharf_se_name을 id에 덧붙여 구분한다.
    """
    with pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT wharf_name, port_name, length_m, depth_m, berth_capacity,
                   handling_cargo_name, wharf_se_name, port_operator_name,
                   latitude, longitude
            FROM upa_berth_facility
            WHERE wharf_name IS NOT NULL AND wharf_name <> ''
        """)
        raw_rows = cur.fetchall()

    seen_names: dict[str, int] = {}
    for row in raw_rows:
        seen_names[row["wharf_name"]] = seen_names.get(row["wharf_name"], 0) + 1

    batch: list[dict] = []
    for row in raw_rows:
        name = row["wharf_name"]
        berth_id = name if seen_names[name] == 1 else f"{name}({row['wharf_se_name'] or '?'})"
        batch.append({
            "berth_id": berth_id,
            "wharf_name": name,
            "port_name": row["port_name"],
            "length_m": row["length_m"],
            "depth_m": row["depth_m"],
            "berth_capacity": row["berth_capacity"],
            "handling_cargo_name": row["handling_cargo_name"],
            "wharf_se_name": row["wharf_se_name"],
            "port_operator_name": row["port_operator_name"],
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "categories": _split_cargo_categories(row["handling_cargo_name"]),
        })
    return batch


# ─────────────────────────────────────────────────────────────────────────────
# Cypher 쿼리
# ─────────────────────────────────────────────────────────────────────────────

_CYPHER_MERGE_BERTH = """
UNWIND $batch AS row
MERGE (b:Berth {id: row.berth_id})
ON CREATE SET
    b.wharf_name          = row.wharf_name,
    b.port_name           = row.port_name,
    b.length_m            = row.length_m,
    b.depth_m             = row.depth_m,
    b.berth_capacity      = row.berth_capacity,
    b.handling_cargo_name = row.handling_cargo_name,
    b.wharf_se_name       = row.wharf_se_name,
    b.port_operator_name  = row.port_operator_name,
    b.latitude            = row.latitude,
    b.longitude           = row.longitude,
    b.created_at          = datetime()
ON MATCH SET
    b.wharf_name          = row.wharf_name,
    b.port_name           = row.port_name,
    b.length_m            = row.length_m,
    b.depth_m             = row.depth_m,
    b.berth_capacity      = row.berth_capacity,
    b.handling_cargo_name = row.handling_cargo_name,
    b.wharf_se_name       = row.wharf_se_name,
    b.port_operator_name  = row.port_operator_name,
    b.latitude            = row.latitude,
    b.longitude           = row.longitude,
    b.updated_at          = datetime()
"""

_CYPHER_MERGE_HANDLES = """
UNWIND $batch AS row
MATCH (b:Berth {id: row.berth_id})
UNWIND row.categories AS category_name
MERGE (cat:CargoCategory {name: category_name})
ON CREATE SET cat.created_at = datetime()
MERGE (b)-[r:HANDLES]->(cat)
ON CREATE SET r.created_at = datetime()
"""

# 양방향 관계를 명시적으로 두 개 만들어, 조회 시 방향 UNION 없이
# 한 방향 MATCH만으로 인접 선석을 찾을 수 있게 한다.
_CYPHER_MERGE_ADJACENT = """
UNWIND $batch AS row
MATCH (a:Berth {id: row.berth_a})
MATCH (b:Berth {id: row.berth_b})
MERGE (a)-[r1:ADJACENT_TO]->(b)
ON CREATE SET r1.created_at = datetime()
MERGE (b)-[r2:ADJACENT_TO]->(a)
ON CREATE SET r2.created_at = datetime()
"""


def _tx_merge_berth(tx, batch: list[dict]) -> None:
    tx.run(_CYPHER_MERGE_BERTH, batch=batch)


def _tx_merge_handles(tx, batch: list[dict]) -> None:
    tx.run(_CYPHER_MERGE_HANDLES, batch=batch)


def _tx_merge_adjacent(tx, batch: list[dict]) -> None:
    tx.run(_CYPHER_MERGE_ADJACENT, batch=batch)


def _resolve_pilot_pairs(batch: list[dict]) -> list[dict]:
    """PILOT_ADJACENT_PAIRS의 wharf_name을 실제 berth_id로 해석한다.

    중복 wharf_name(berth_id가 "이름(구분)" 형태로 바뀐 경우)은 파일럿 목록에
    없으므로 단순 1:1 매핑으로 충분하다. 매칭 실패(수집 데이터에 해당
    wharf_name이 없는 경우)는 경고만 남기고 스킵한다 — 수집 범위가 달라져도
    로더가 죽지 않도록.
    """
    wharf_to_id = {row["wharf_name"]: row["berth_id"] for row in batch}
    resolved: list[dict] = []
    for name_a, name_b in PILOT_ADJACENT_PAIRS:
        id_a, id_b = wharf_to_id.get(name_a), wharf_to_id.get(name_b)
        if not id_a or not id_b:
            logger.warning("파일럿 인접쌍 매칭 실패(수집 데이터에 없음): %s <-> %s", name_a, name_b)
            continue
        resolved.append({"berth_a": id_a, "berth_b": id_b})
    return resolved


def transfer_berths_to_neo4j(pg_conn, neo4j_driver) -> None:
    """upa_berth_facility 전체를 읽어 Berth/CargoCategory/ADJACENT_TO로 적재한다."""
    batch = fetch_berth_rows(pg_conn)
    if not batch:
        logger.warning("upa_berth_facility에 적재할 행이 없습니다.")
        return

    adjacent_batch = _resolve_pilot_pairs(batch)
    handles_batch = [row for row in batch if row["categories"]]

    with neo4j_driver.session(database=NEO4J_DATABASE) as session:
        session.execute_write(_tx_merge_berth, batch)
        if handles_batch:
            session.execute_write(_tx_merge_handles, handles_batch)
        if adjacent_batch:
            session.execute_write(_tx_merge_adjacent, adjacent_batch)

    logger.info(
        "Berth 이관 완료: Berth %d개, HANDLES 대상 %d개, ADJACENT_TO 쌍 %d개",
        len(batch), len(handles_batch), len(adjacent_batch),
    )


def run_berth_transfer() -> None:
    """PostgreSQL(upa_berth_facility) -> Neo4j 이관 파이프라인 전체를 실행한다."""
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
        transfer_berths_to_neo4j(pg_conn, neo4j_driver)

    except ServiceUnavailable as e:
        logger.error("Neo4j 연결 불가: %s", e)
        raise
    except psycopg2.OperationalError as e:
        logger.error("PostgreSQL 연결 불가: %s", e)
        raise
    except Exception:
        logger.exception("Berth 이관 중 치명적 오류 발생")
        raise
    finally:
        if neo4j_driver:
            neo4j_driver.close()
            logger.info("Neo4j Driver 종료")
        if pg_conn and not pg_conn.closed:
            pg_conn.close()
            logger.info("PostgreSQL 연결 종료")


if __name__ == "__main__":
    run_berth_transfer()
