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

import csv
import logging
import math
import os
import re
from collections import defaultdict
from pathlib import Path

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

# 온산 MVP(feature/onsan-mvp) 이식: 기상분석 에이전트가 berth_weather_threshold를
# 부두그룹 단위로 조회하므로, 개별 선석(wharf_name)이 어느 부두그룹에 속하는지
# 매핑이 필요하다. wharf_name은 upa_berth_facility_stg.csv 실제 값과 정확히
# 일치해야 한다(2026-07-19 수집분 기준 확인됨). 이 매핑에 없는 선석은
# berth_group=None으로 남고, 기상분석 에이전트는 전역 폴백 임계값을 쓴다.
ONSAN_BERTH_GROUP_MAP: dict[str, str] = {
    "OTK1부두": "OTK1/2부두(처용리)",
    "OTK2부두": "OTK1/2부두(처용리)",
    "정일1부두": "정일1/2부두(산암리)",
    "정일2부두": "정일1/2부두(산암리)",
    "UTK부두": "UTK부두(처용리)",
    "대한유화부두": "대한유화부두(처용리)",
    "효성부두": "효성부두(산암리)",
    "S-Oil 1부두": "S-Oil1~4부두(산암리/원산리)",
    "S-Oil 2부두": "S-Oil1~4부두(산암리/원산리)",
    "S-Oil 3부두": "S-Oil1~4부두(산암리/원산리)",
    "S-Oil 4부두": "S-Oil1~4부두(산암리/원산리)",
    "석유공사부이": "한국석유공사원유부이",
}

# 온산 MVP가 정의한 온산 스코프(액체화물 12부두 + 부이 3기). 좌표 거리 기반
# ADJACENT_TO/SUBSTITUTABLE_WITH 자동 계산과 스케줄링 에이전트의 배정 대상
# 범위(graph_queries._CYPHER_FIND_ELIGIBLE_BERTHS)를 이 범위로 한정할 때 쓴다
# — 울산항 전체 69개 선석 중 무관한 조합(예: 컨테이너부두 vs 벌크부두)까지
# 계산하지 않기 위함. 이 스코프 밖 선석은 그래프 적재 자체에는 영향 없다.
#
# 달포부두(유류, 울산항만공사)는 원천 시설데이터(ulsan_berth_spec_seed.csv)
# 기준으로도 온산항 소속인데 최초 큐레이션에서 누락돼 있었다
ONSAN_SCOPE_WHARF_NAMES: set[str] = {
    "OTK1부두", "OTK2부두", "정일1부두", "정일2부두", "UTK부두", "대한유화부두",
    "효성부두", "달포부두", "S-Oil 1부두", "S-Oil 2부두", "S-Oil 3부두", "S-Oil 4부두",
    "S-Oil부이", "S-Oil&오일허브 부이", "석유공사부이",
}

# 온산 스코프 선석의 인접(ADJACENT_TO) 판정 임계 거리(m). onsan_mvp/scripts/
# build_adjacency.py와 동일(처용리/산암리 클러스터가 약 2.5km 떨어져 있어 이
# 임계값으로는 자연히 클러스터를 넘지 않는다).
ONSAN_ADJACENCY_THRESHOLD_M = 500.0

_NEO4J_CONSTRAINT_STMTS: list[str] = [
    "CREATE CONSTRAINT berth_id_unique IF NOT EXISTS "
    "FOR (b:Berth) REQUIRE b.id IS UNIQUE",
    "CREATE CONSTRAINT cargo_category_name_unique IF NOT EXISTS "
    "FOR (cat:CargoCategory) REQUIRE cat.name IS UNIQUE",
    # 온산 MVP(feature/onsan-mvp) 이식: 정박지 대기 모델에 쓰는 노드.
    "CREATE CONSTRAINT anchorage_id_unique IF NOT EXISTS "
    "FOR (a:Anchorage) REQUIRE a.id IS UNIQUE",
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

# 취급화물 큐레이션 보정은 mart.berth_handling_cargo 뷰가 정본이다.
#
# 예전에는 이 파일에 파이썬 dict(HANDLING_CARGO_OVERRIDES)로 들고 있었는데,
# 그러면 Neo4j 를 읽는 스케줄링 에이전트와 SQL 을 읽는 화면이 서로 다른 값을
# 보게 된다 — 실제로 가스부두가 그래프에서는 '가스', 선석 배정현황 화면에서는
# '유류' 로 나왔다(2026-08-20 실측). 같은 기준이 두 군데 살아 있으면 갈라진다.
# 보정 근거와 목록은 data-pipeline/mart_views.sql 12절 참고.


def _split_cargo_categories(handling_cargo_name: str | None) -> list[str]:
    """"잡화, 액체화학" 같은 콤마 구분 텍스트를 개별 카테고리 목록으로 분리."""
    if not handling_cargo_name:
        return []
    return [token.strip() for token in handling_cargo_name.split(",") if token.strip()]


def fetch_berth_rows(pg_conn) -> list[dict]:
    """berth × wharf 를 읽어 Neo4j 배치 행으로 변환한다.

    ★ (2026-09-20) 출처를 upa_berth_facility -> berth/wharf 로 교체했다.

      예전에는 upa_berth_facility(부두 단위 68행)를 Berth 노드로 만들었다.
      그런데 그 표는 **선석 단위 제원을 부두 하나로 뭉갠 것**이라 문제가 있었다:
        - 접안능력이 68행 중 2행만 채워져 있어 배정 판정에 못 썼다
        - 같은 부두 안에서 선석별 접안능력이 8배까지 차이 난다
          (2부두: 1선석 40,000 / 2선석 20,000 / 3선석 5,000 DWT)
        - wharf_name UPSERT 로 SK2부두 1행이 조용히 유실돼 있었다(실측 68 vs 69)

      이제 berth(118 선석) × wharf(65 부두)를 읽는다. berth_id 가 이미 유일하므로
      옛 `wharf_se_name` 접미사 우회도 필요 없다.
      설계 근거: docs/11_선석제원_재설계_설계문서.md

    취급화물 보정(mart.berth_handling_cargo)은 record_uid 가 아니라 **wharf_name**
    으로 조인한다 — 새 berth 에는 record_uid 가 없고, 보정은 원래 부두 단위다.
    """
    with pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT b.berth_id, b.wharf_name, b.berth_no,
                   b.length_m, b.length_basis,
                   b.water_depth_m, b.capacity_value, b.unload_value,
                   b.quay_structure, b.operator_name AS berth_operator_name,
                   w.port_name, w.latitude, w.longitude,
                   w.port_name AS wharf_port_name,
                   w.berth_count, w.berthing_vessel_count,
                   w.total_quay_length_m,
                   w.min_water_depth_m, w.min_capacity_dwt, w.max_capacity_dwt,
                   w.facility_cd, w.facility_sub_code,
                   COALESCE(bhc.handling_cargo_name, b.handling_cargo_name)
                       AS handling_cargo_name
            FROM berth b
            JOIN wharf w ON w.wharf_name = b.wharf_name
            LEFT JOIN (
                SELECT DISTINCT ON (wharf_name) wharf_name, handling_cargo_name
                FROM mart.berth_handling_cargo
            ) bhc ON bhc.wharf_name = b.wharf_name
            WHERE b.berth_id IS NOT NULL AND b.berth_id <> ''
            ORDER BY b.wharf_name, b.berth_no NULLS FIRST
        """)
        raw_rows = cur.fetchall()

    batch: list[dict] = []
    for row in raw_rows:
        name = row["wharf_name"]
        batch.append({
            "berth_id": row["berth_id"],
            "wharf_name": name,
            "berth_no": row["berth_no"],
            "port_name": row["port_name"],
            # 선석 단위 제원. 확정 못 한 값은 NULL 로 둔다 — 부두 집계값을 선석
            # 값인 척 채우지 않는다(그게 거짓 안전을 만든다).
            "length_m": row["length_m"],
            "length_basis": row["length_basis"],
            "depth_m": row["water_depth_m"],
            "berth_capacity": row["capacity_value"]
                              or ONSAN_BERTH_CAPACITY_DWT.get(name),
            "unload_capacity": row["unload_value"],
            "quay_structure": row["quay_structure"],
            # 부두 단위 값(같은 부두의 선석들이 공유한다)
            "wharf_berth_count": row["berth_count"],
            "berth_vessel_count": row["berthing_vessel_count"],
            "wharf_total_quay_length_m": row["total_quay_length_m"],
            "wharf_min_depth_m": row["min_water_depth_m"],
            "wharf_min_capacity_dwt": row["min_capacity_dwt"],
            "wharf_max_capacity_dwt": row["max_capacity_dwt"],
            "facility_cd": row["facility_cd"],
            "facility_sub_code": row["facility_sub_code"],
            "handling_cargo_name": row["handling_cargo_name"],
            "port_operator_name": row["berth_operator_name"],
            # 좌표는 부두 단위다(웹에 선석 좌표가 없다). 같은 부두의 선석들은
            # 같은 좌표를 갖는다 — ADJACENT_TO 거리계산에서 서로 인접으로
            # 잡히는데, 실제로 맞닿아 있으므로 의도한 결과다.
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "categories": _split_cargo_categories(row["handling_cargo_name"]),
            "berth_group": ONSAN_BERTH_GROUP_MAP.get(name),
            "onsan_scope": name in ONSAN_SCOPE_WHARF_NAMES,
        })
    return batch


# ─────────────────────────────────────────────────────────────────────────────
# Cypher 쿼리
# ─────────────────────────────────────────────────────────────────────────────

_CYPHER_MERGE_BERTH = """
UNWIND $batch AS row
// 부두(Wharf)는 선석의 부모다. 좌표·항 이름은 부두의 속성이고, 선석은 제원을
// 갖는다 — 한쪽에 몰아넣으면 복제되거나 뭉개진다(설계문서 §5).
MERGE (w:Wharf {name: row.wharf_name})
SET w.port_name            = row.port_name,
    w.latitude             = row.latitude,
    w.longitude            = row.longitude,
    w.berth_count          = row.wharf_berth_count,
    w.berthing_vessel_count= row.berth_vessel_count,
    w.total_quay_length_m  = row.wharf_total_quay_length_m,
    w.min_water_depth_m    = row.wharf_min_depth_m,
    w.min_capacity_dwt     = row.wharf_min_capacity_dwt,
    w.max_capacity_dwt     = row.wharf_max_capacity_dwt,
    w.facility_cd          = row.facility_cd,
    w.facility_sub_code    = row.facility_sub_code,
    w.onsan_scope          = row.onsan_scope,
    w.updated_at           = datetime()
MERGE (b:Berth {id: row.berth_id})
ON CREATE SET
    b.wharf_name          = row.wharf_name,
    b.berth_no            = row.berth_no,
    b.port_name           = row.port_name,
    b.length_m            = row.length_m,
    b.length_basis        = row.length_basis,
    b.depth_m             = row.depth_m,
    b.berth_capacity      = row.berth_capacity,
    b.unload_capacity     = row.unload_capacity,
    b.quay_structure      = row.quay_structure,
    b.handling_cargo_name = row.handling_cargo_name,
    b.port_operator_name  = row.port_operator_name,
    b.latitude            = row.latitude,
    b.longitude           = row.longitude,
    b.berth_group         = row.berth_group,
    b.onsan_scope         = row.onsan_scope,
    b.facility_cd         = row.facility_cd,
    b.facility_sub_code   = row.facility_sub_code,
    b.created_at          = datetime()
ON MATCH SET
    b.wharf_name          = row.wharf_name,
    b.berth_no            = row.berth_no,
    b.port_name           = row.port_name,
    b.length_m            = row.length_m,
    b.length_basis        = row.length_basis,
    b.depth_m             = row.depth_m,
    b.berth_capacity      = row.berth_capacity,
    b.unload_capacity     = row.unload_capacity,
    b.quay_structure      = row.quay_structure,
    b.handling_cargo_name = row.handling_cargo_name,
    b.port_operator_name  = row.port_operator_name,
    b.latitude            = row.latitude,
    b.longitude           = row.longitude,
    b.berth_group         = row.berth_group,
    b.onsan_scope         = row.onsan_scope,
    b.facility_cd         = row.facility_cd,
    b.facility_sub_code   = row.facility_sub_code,
    b.updated_at          = datetime()
MERGE (b)-[:PART_OF]->(w)
"""

# 이 선석이 더 이상 취급하지 않는 카테고리 관계를 먼저 끊는다.
#
# MERGE 는 추가만 하고 지우지 않는다. 그래서 취급화물이 바뀌면(원천 갱신이나
# HANDLING_CARGO_OVERRIDES 보정) 옛 관계가 그대로 남아 두 카테고리를 동시에
# 취급하는 것처럼 보였다 — 2026-08-18 실측: 석유공사부이를 '원유'로 보정한 뒤에도
# '유류' 관계가 남아, 제품유 후보에서 빼려던 목적이 그대로 무산됐다.
# 재적재가 몇 번을 돌아도 같은 결과가 되도록(멱등) 배치에 있는 선석만 정리한다.
_CYPHER_PRUNE_HANDLES = """
UNWIND $batch AS row
MATCH (b:Berth {id: row.berth_id})-[r:HANDLES]->(cat:CargoCategory)
WHERE NOT cat.name IN row.categories
DELETE r
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
# distance_m은 좌표 기반 계산 쌍에만 있다(온산 MVP 이식) — 수동 큐레이션
# PILOT_ADJACENT_PAIRS는 null로 남는다(거리 근거가 없다는 뜻을 그대로 보존).
_CYPHER_MERGE_ADJACENT = """
UNWIND $batch AS row
MATCH (a:Berth {id: row.berth_a})
MATCH (b:Berth {id: row.berth_b})
MERGE (a)-[r1:ADJACENT_TO]->(b)
ON CREATE SET r1.created_at = datetime(), r1.distance_m = row.distance_m
ON MATCH SET r1.distance_m = coalesce(row.distance_m, r1.distance_m)
MERGE (b)-[r2:ADJACENT_TO]->(a)
ON CREATE SET r2.created_at = datetime(), r2.distance_m = row.distance_m
ON MATCH SET r2.distance_m = coalesce(row.distance_m, r2.distance_m)
"""


def _tx_merge_berth(tx, batch: list[dict]) -> None:
    tx.run(_CYPHER_MERGE_BERTH, batch=batch)


def _tx_merge_handles(tx, batch: list[dict]) -> None:
    tx.run(_CYPHER_PRUNE_HANDLES, batch=batch)
    tx.run(_CYPHER_MERGE_HANDLES, batch=batch)


def _tx_merge_adjacent(tx, batch: list[dict]) -> None:
    tx.run(_CYPHER_MERGE_ADJACENT, batch=batch)


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """두 위경도(WGS84) 사이의 거리(m). onsan_mvp/scripts/build_adjacency.py와 동일 공식."""
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def compute_adjacent_pairs_by_distance(
    batch: list[dict],
    *,
    scope_wharf_names: set[str] = ONSAN_SCOPE_WHARF_NAMES,
    threshold_m: float = ONSAN_ADJACENCY_THRESHOLD_M,
) -> list[dict]:
    """좌표 거리 기반 ADJACENT_TO 쌍을 계산한다 (온산 MVP 이식,
    onsan_mvp/scripts/build_adjacency.py의 haversine 로직).

    PILOT_ADJACENT_PAIRS(수동 큐레이션)를 대체하는 게 아니라 추가한다 — 이 함수는
    scope_wharf_names로 범위를 한정해서, 울산항 전체 69개 선석 전부에 대해
    O(n^2) 거리 계산을 하지 않는다. 좌표 결측 선석(S-Oil 부이 2기, 석유공사부이 —
    해상 계류점이라 안벽 좌표가 없음)은 계산에서 자연히 제외된다.
    """
    have_coords = [
        row for row in batch
        if row["wharf_name"] in scope_wharf_names
        and row["latitude"] is not None
        and row["longitude"] is not None
    ]
    pairs: list[dict] = []
    for i in range(len(have_coords)):
        for j in range(i + 1, len(have_coords)):
            a, b = have_coords[i], have_coords[j]
            distance = haversine_m(a["latitude"], a["longitude"], b["latitude"], b["longitude"])
            if distance <= threshold_m:
                pairs.append({"berth_a": a["berth_id"], "berth_b": b["berth_id"], "distance_m": round(distance, 1)})
    return pairs


def _resolve_pilot_pairs(batch: list[dict]) -> list[dict]:
    """PILOT_ADJACENT_PAIRS(부두명 쌍)를 berth_id 쌍으로 편다.

    ★ (2026-09-20) 이제 Berth 노드가 **선석 단위**라 부두 하나에 선석이 여럿이다.
      예전처럼 `{wharf_name: berth_id}` 사전을 만들면 부두당 마지막 선석 하나만
      남아, 큐레이션한 인접 관계가 대부분 소실된다.
      부두 A·B 가 인접이면 **A 의 모든 선석과 B 의 모든 선석**이 인접이므로
      곱집합으로 편다.
    """
    by_wharf: dict[str, list[str]] = {}
    for row in batch:
        by_wharf.setdefault(row["wharf_name"], []).append(row["berth_id"])

    resolved: list[dict] = []
    for name_a, name_b in PILOT_ADJACENT_PAIRS:
        ids_a, ids_b = by_wharf.get(name_a), by_wharf.get(name_b)
        if not ids_a or not ids_b:
            logger.warning("파일럿 인접쌍 매칭 실패(수집 데이터에 없음): %s <-> %s", name_a, name_b)
            continue
        for ia in ids_a:
            for ib in ids_b:
                resolved.append({"berth_a": ia, "berth_b": ib, "distance_m": None})
    return resolved


def _merge_adjacent_pair_sources(*pair_lists: list[dict]) -> list[dict]:
    """여러 출처(수동 큐레이션 + 좌표 계산)의 인접쌍을 (berth_a, berth_b) 무순서 기준으로 dedup.

    같은 쌍이 여러 출처에 있으면 distance_m이 있는(=좌표로 계산된) 쪽을 우선한다.
    """
    merged: dict[frozenset, dict] = {}
    for pairs in pair_lists:
        for pair in pairs:
            key = frozenset((pair["berth_a"], pair["berth_b"]))
            existing = merged.get(key)
            if existing is None or (existing.get("distance_m") is None and pair.get("distance_m") is not None):
                merged[key] = pair
    return list(merged.values())


def transfer_berths_to_neo4j(pg_conn, neo4j_driver) -> None:
    """upa_berth_facility 전체를 읽어 Berth/CargoCategory/ADJACENT_TO로 적재한다.

    ADJACENT_TO는 두 출처를 합친다 — 기존 PILOT_ADJACENT_PAIRS(수행계획서 5개
    선석군 수동 큐레이션)와, 온산 MVP 이식으로 추가된 좌표 거리 기반 자동 계산
    (compute_adjacent_pairs_by_distance, ONSAN_SCOPE_WHARF_NAMES 범위 한정).
    같은 쌍이 겹치면 거리 정보가 있는 쪽을 남긴다. 기존 5개 선석군 데이터는
    그대로 보존되고(DETACH DELETE 없음), 온산 스코프만 추가된다.
    """
    batch = fetch_berth_rows(pg_conn)
    if not batch:
        logger.warning("berth 테이블에 적재할 행이 없습니다.")
        return

    # ★ (2026-09-20) 옛 구조를 먼저 깨끗이 지운다.
    #   부두 단위 Berth(68개)와 선석 단위 Berth(118개)는 id 체계가 달라
    #   MERGE 로는 옛 노드가 그대로 남는다. 남으면 스케줄링·조회가 두 체계를
    #   동시에 보게 되어 조용히 틀린 결과가 나온다.
    #   DETACH DELETE 로 관계까지 정리한 뒤 새로 만든다 — ADJACENT_TO/HANDLES 는
    #   아래에서 전부 재생성되므로 손실이 없다.
    with neo4j_driver.session(database=NEO4J_DATABASE) as session:
        removed = session.run(
            "MATCH (b:Berth) DETACH DELETE b RETURN count(b) AS c"
        ).single()["c"]
        removed_w = session.run(
            "MATCH (w:Wharf) DETACH DELETE w RETURN count(w) AS c"
        ).single()["c"]
    logger.info("옛 구조 제거: Berth %d개, Wharf %d개", removed, removed_w)

    pilot_pairs = _resolve_pilot_pairs(batch)
    distance_pairs = compute_adjacent_pairs_by_distance(batch)
    adjacent_batch = _merge_adjacent_pair_sources(pilot_pairs, distance_pairs)
    handles_batch = [row for row in batch if row["categories"]]

    with neo4j_driver.session(database=NEO4J_DATABASE) as session:
        session.execute_write(_tx_merge_berth, batch)
        if handles_batch:
            session.execute_write(_tx_merge_handles, handles_batch)
        if adjacent_batch:
            session.execute_write(_tx_merge_adjacent, adjacent_batch)

    logger.info(
        "Berth 이관 완료: Berth %d개, HANDLES 대상 %d개, "
        "ADJACENT_TO 쌍 %d개(파일럿 %d + 좌표계산 %d, 중복 제거 후)",
        len(batch), len(handles_batch), len(adjacent_batch), len(pilot_pairs), len(distance_pairs),
    )


# ─────────────────────────────────────────────────────────────────────────────
# cargo_kind 세분화 적재 (2026-08-19 신설)
#
# 07_입항승인_선석확정_설계문서.md §4.1.1이 지적한 공백: handling_cargo_name
# (upa_berth_facility, 이 파일이 이미 읽는 실시간 소스)에는 원유/유류/액체화학
# 3개 토큰만 있고(cargo_category_loader.py 주석에서 라이브 조회로 재확인),
# 더 세밀한 값(케미칼류/LPG/비료원료 등)은 data/seed/ulsan_berth_spec_seed.csv의
# cargo_kind 컬럼에만 있는데 이걸 Neo4j에 적재하는 로더가 없었다. 이 절이
# 그 로더다.
#
# [범위를 일부러 좁혔다 — 반드시 읽을 것]
# 이 로더는 Berth 노드에 cargo_kind_detail 속성(문자열 배열)만 추가한다.
# 기존 HANDLES/CargoCategory 관계나 find_eligible_berths()의 매칭 로직은
# 건드리지 않는다. 이유: HANDLES의 카테고리 이름은 cargo_category_loader.py가
# "화물(=화학물질) 쪽"에서 원유/유류/액체화학 3개로만 분류한 값과 반드시
# 일치해야 매칭이 성립하는데(같은 문자열 상수를 공유하는 게 이 그래프 모델의
# 전제, 이 파일 상단 docstring 참고), 화물 쪽에 케미칼류/LPG/비료원료로
# 분류하는 로직 자체가 없다. 여기서 HANDLES에 세분화 카테고리를 추가해도
# 그 카테고리로 분류되는 화물이 하나도 없어 죽은 엣지가 될 뿐이다(실제로
# 매칭에 쓰이지 않음). 그래서 "조회 가능하게 적재"까지만 하고, 실제 배정
# 게이트로 쓰는 건 화물 쪽 5-tier 분류기가 생긴 뒤의 별도 작업으로 남긴다.
#
# 매칭: CSV facility_name -> Neo4j Berth.wharf_name. 두 표기 체계가 정확히
# 같지 않을 수 있어(mart.facility_alias가 이미 겪은 문제와 동일 종류) 정규화
# 후 비교한다 — mart.norm_berth()(mart_views.sql)와 같은 규칙(괄호 제거,
# 공백 제거, 소문자화, 끝자리 숫자 제거)을 Python으로 재현했다. SK2부두처럼
# 한 wharf_name이 berth_id 여러 개로 갈라진 경우(운영주체 구분) 전부에 같은
# cargo_kind_detail을 적용한다 — CSV 자체가 그 이상 세밀하게 구분하지 않는다.
# ─────────────────────────────────────────────────────────────────────────────

CARGO_KIND_SEED_CSV = Path(__file__).resolve().parents[2] / "data" / "seed" / "ulsan_berth_spec_seed.csv"


def _normalize_wharf_name(name: str) -> str:
    """mart.norm_berth()(mart_views.sql)와 동일 규칙의 Python 재현.

    SQL 함수를 Python에서 그대로 호출할 수 없어(다른 프로세스) 규칙만 복제한다
    — 규칙이 바뀌면 두 곳 다 고쳐야 한다는 뜻이지만, 이 정도로 안정된(2026-08-11
    확정) 정규화 규칙을 매번 DB 왕복으로 확인할 정도는 아니라고 판단했다.
    """
    s = re.sub(r"\(.*\)", "", name)
    s = re.sub(r"\s+", "", s)
    s = s.lower()
    return re.sub(r"[0-9]+$", "", s)


def fetch_cargo_kind_rows(csv_path: Path = CARGO_KIND_SEED_CSV) -> list[dict]:
    """ulsan_berth_spec_seed.csv를 읽어 (정규화 부두명, cargo_kind 목록) 행으로 변환.

    cargo_kind 셀은 '·'로 여러 값을 함께 적어 둔 경우가 있다(예: '유류·케미칼류',
    복합 취급 부두) — 그대로 배열로 쪼갠다.
    """
    if not csv_path.exists():
        logger.warning("cargo_kind 시드 CSV 없음: %s — 이 단계 스킵", csv_path)
        return []
    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = []
        for r in reader:
            name = (r.get("facility_name") or "").strip()
            kind = (r.get("cargo_kind") or "").strip()
            if not name or not kind:
                continue
            rows.append({
                "facility_name": name,
                "norm_name": _normalize_wharf_name(name),
                "cargo_kinds": [k.strip() for k in kind.split("·") if k.strip()],
            })
        return rows


_CYPHER_SET_CARGO_KIND_DETAIL = """
UNWIND $batch AS row
MATCH (b:Berth {id: row.berth_id})
SET b.cargo_kind_detail    = row.cargo_kinds,
    b.cargo_kind_source    = 'ulsan_berth_spec_seed.csv',
    b.cargo_kind_updated_at = datetime()
"""


def _tx_set_cargo_kind_detail(tx, batch: list[dict]) -> None:
    tx.run(_CYPHER_SET_CARGO_KIND_DETAIL, batch=batch)


# 해수청 시드 CSV(`ulsan_berth_spec_seed.csv`)의 부두 표기 -> 우리 부두명.
#
# 우리 부두명의 정본은 **UPA 공공 API** 다(설계문서 §5). 해수청 액체화물현황은
# 표기가 달라서 _normalize_wharf_name 만으로는 안 붙는다. PORT-MIS 와는 무관한
# 경로다 — PORT-MIS 이름은 portmis_facility_map 이 따로 다룬다.
#
# 키는 _normalize_wharf_name() 을 적용한 형태다. 값이 여러 개인 것은 해수청이
# 한 줄로 묶어 적은 부두를 API 가 나눠 적기 때문이다(1:N).
CARGO_KIND_WHARF_ALIAS: dict[str, list[str]] = {
    "용잠부두": ["용잠1부두", "용잠2부두"],
    "sk부이ii": ["SK2부이"],
    "sk부이iii": ["SK3부이"],
    "정일스톨트헤븐신항3~5부두": [
        "정일스톨트헤븐 울산신항3부두",
        "정일스톨트헤븐 울산신항4부두",
        "정일스톨트헤븐 울산신항5부두",
    ],
    # 웹 상세가 신항2부두(270m)와 일치해 그쪽으로 확정했다(설계문서 §16).
    "현대오일터미널신항부두": ["현대오일터미널 신항2부두"],
    "ls니꼬신항부두": ["LS MNM 신항부두"],  # LS니꼬 -> LS MnM 사명 변경
    # 'S-Oil&오일허브 부이'는 공공 API 에만 있고 웹 상세가 없어 우리 berth 에
    # 아예 없다. 별칭으로 해결할 수 있는 건이 아니라 여기 넣지 않는다.
}


def transfer_cargo_kind_to_neo4j(pg_conn, neo4j_driver, *, csv_path: Path = CARGO_KIND_SEED_CSV) -> None:
    """cargo_kind 세분화 값을 이미 적재된 Berth 노드에 속성으로 추가한다.

    Berth 노드 자체는 만들지 않는다(transfer_berths_to_neo4j가 이미 했어야
    함) — CSV에만 있고 upa_berth_facility에는 없는 부두명이 매칭 실패로
    빠지는 것과, 애초에 Berth 노드가 없어 MATCH가 실패하는 것을 같은 로그로
    구분할 수 있게 하기 위해 별도 단계로 둔다.
    """
    csv_rows = fetch_cargo_kind_rows(csv_path)
    if not csv_rows:
        return

    berths = fetch_berth_rows(pg_conn)
    by_norm_name: dict[str, list[str]] = defaultdict(list)
    for berth in berths:
        by_norm_name[_normalize_wharf_name(berth["wharf_name"])].append(berth["berth_id"])

    by_wharf_name: dict[str, list[str]] = defaultdict(list)
    for berth in berths:
        by_wharf_name[berth["wharf_name"]].append(berth["berth_id"])

    batch: list[dict] = []
    unmatched: list[str] = []
    for row in csv_rows:
        berth_ids = by_norm_name.get(row["norm_name"])
        if not berth_ids:
            # 정규화로 안 붙으면 별칭 사전을 본다. 규칙을 늘리지 않고 사전으로
            # 관리한다 — 규칙을 늘리면 다른 부두까지 잘못 묶기 시작한다.
            berth_ids = [
                bid
                for wname in CARGO_KIND_WHARF_ALIAS.get(row["norm_name"], [])
                for bid in by_wharf_name.get(wname, [])
            ]
        if not berth_ids:
            unmatched.append(row["facility_name"])
            continue
        for berth_id in berth_ids:
            batch.append({"berth_id": berth_id, "cargo_kinds": row["cargo_kinds"]})

    if unmatched:
        logger.warning(
            "cargo_kind 매칭 실패(Neo4j Berth에 없음, %d/%d건): %s",
            len(unmatched), len(csv_rows), unmatched,
        )
    if not batch:
        logger.warning("cargo_kind 적용 대상 Berth 없음 — 적재 스킵")
        return

    with neo4j_driver.session(database=NEO4J_DATABASE) as session:
        session.execute_write(_tx_set_cargo_kind_detail, batch)

    logger.info(
        "cargo_kind 세분화 적재 완료: %d개 Berth 노드(CSV %d행 중 매칭 %d행)",
        len(batch), len(csv_rows), len(csv_rows) - len(unmatched),
    )


_CYPHER_MERGE_SUBSTITUTABLE = """
UNWIND $batch AS row
MATCH (a:Berth {id: row.berth_a})
MATCH (b:Berth {id: row.berth_b})
MERGE (a)-[r:SUBSTITUTABLE_WITH]->(b)
ON CREATE SET r.operator = row.operator, r.shared_products = row.shared_products,
              r.to_max_dwt = row.to_max_dwt, r.to_depth_m = row.to_depth_m,
              r.created_at = datetime()
ON MATCH SET r.operator = row.operator, r.shared_products = row.shared_products,
             r.to_max_dwt = row.to_max_dwt, r.to_depth_m = row.to_depth_m,
             r.updated_at = datetime()
"""


def compute_substitutability_pairs(batch: list[dict]) -> list[dict]:
    """같은 운영사(port_operator_name) + 취급화물 카테고리가 겹치는 선석 쌍을
    SUBSTITUTABLE_WITH 후보로 계산한다 (온산 MVP 이식,
    onsan_mvp/scripts/build_substitutability.py의 통찰 그대로 적용: 액체화물
    부두는 파이프라인이 특정 탱크단지로 고정 연결된 전용부두라, 대체는 사실상
    같은 운영사 안에서만 가능하다). ADJACENT_TO(물리적 인접=혼재위험)와는 완전히
    다른 관계이자 방향성 엣지 — A -> B가 있어도 B -> A의 제원 조건은 다를 수
    있어 개별 계산한다.
    """
    by_operator: dict[str, list[dict]] = defaultdict(list)
    for row in batch:
        if row["port_operator_name"] and row["categories"]:
            by_operator[row["port_operator_name"]].append(row)

    pairs: list[dict] = []
    for operator, rows in by_operator.items():
        for a in rows:
            for b in rows:
                if a["berth_id"] == b["berth_id"]:
                    continue
                shared = sorted(set(a["categories"]) & set(b["categories"]))
                if not shared:
                    continue
                # upa_berth_facility.berth_capacity가 대부분 NULL이라(실측 확인,
                # transfer_anchorage_to_neo4j와 동일 문제) 온산 부두는 팀원
                # PDF값(ONSAN_BERTH_CAPACITY_DWT)으로 보강 — 아니면 DWT 게이트
                # (scheduling.service.resolve_berth_assignment)가 항상 통과되어
                # 사실상 무의미해진다.
                to_max_dwt = b["berth_capacity"] or ONSAN_BERTH_CAPACITY_DWT.get(b["wharf_name"])
                pairs.append({
                    "berth_a": a["berth_id"],
                    "berth_b": b["berth_id"],
                    "operator": operator,
                    "shared_products": "/".join(shared),
                    "to_max_dwt": to_max_dwt,
                    "to_depth_m": b["depth_m"],
                })
    return pairs


def _tx_merge_substitutable(tx, batch: list[dict]) -> None:
    tx.run(_CYPHER_MERGE_SUBSTITUTABLE, batch=batch)


def transfer_substitutability_to_neo4j(pg_conn, neo4j_driver) -> None:
    """upa_berth_facility 기준으로 SUBSTITUTABLE_WITH 관계를 계산해 추가 적재한다.

    팀원 CSV(onsan_berth_substitutability.csv)를 그대로 옮기지 않고 이미 있는
    실데이터(upa_berth_facility)에서 같은 방식으로 재계산한다 — 9장 비교분석에서
    확인된 대로 팀원 CSV의 대상 데이터가 upa_berth_facility와 겹치므로, 새 CSV를
    또 만들지 않는다.
    """
    batch = fetch_berth_rows(pg_conn)
    if not batch:
        logger.warning("berth 테이블에 적재할 행이 없습니다.")
        return


    pairs = compute_substitutability_pairs(batch)
    if not pairs:
        logger.warning("계산된 SUBSTITUTABLE_WITH 쌍이 없습니다.")
        return

    with neo4j_driver.session(database=NEO4J_DATABASE) as session:
        session.execute_write(_tx_merge_substitutable, pairs)

    logger.info("SUBSTITUTABLE_WITH 이관 완료: %d쌍(방향성 엣지)", len(pairs))


_TONNAGE_NUM_UNIT_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(만톤|천톤)")


def _tonnage_to_value(num: str, unit: str) -> float:
    n = float(num)
    return n * 10000 if unit == "만톤" else n * 1000


def parse_tonnage_bounds(remark: str | None) -> tuple[float | None, float | None]:
    """'3만톤 이하', '2만톤 초과 ~ 5만톤 이하' 같은 자유 텍스트를 (하한, 상한) 톤수로 근사 변환.

    upa_anchorage.remark 실측 표기(이하/이상/초과/~급/급이하) 전부를 대상으로
    확인했다. "이상"/"초과"가 있는 구간은 하한, 그 외(이하/급/단독표기)는 상한으로
    취급한다 — 팀원 assign_anchorage()의 이산 등급표 대신 실데이터 remark 텍스트를
    직접 파싱해서, 팀원 CSV에 없던 정박지(M1~M7, T1~T3 등)까지 자동으로 커버한다.
    """
    if not remark:
        return None, None
    lower: float | None = None
    upper: float | None = None
    for segment in remark.split("~"):
        m = _TONNAGE_NUM_UNIT_RE.search(segment)
        if not m:
            continue
        value = _tonnage_to_value(m.group(1), m.group(2))
        if "이상" in segment or "초과" in segment:
            lower = value
        else:
            upper = value
    return lower, upper


_CYPHER_MERGE_ANCHORAGE = """
UNWIND $batch AS row
MERGE (a:Anchorage {id: row.anchorage_id})
ON CREATE SET a.name = row.name, a.tonnage_rule = row.tonnage_rule,
              a.tonnage_lower = row.tonnage_lower, a.tonnage_upper = row.tonnage_upper,
              a.anchorage_type = row.anchorage_type,
              a.latitude = row.latitude, a.longitude = row.longitude,
              a.created_at = datetime()
ON MATCH SET a.name = row.name, a.tonnage_rule = row.tonnage_rule,
             a.tonnage_lower = row.tonnage_lower, a.tonnage_upper = row.tonnage_upper,
             a.anchorage_type = row.anchorage_type,
             a.latitude = row.latitude, a.longitude = row.longitude,
             a.updated_at = datetime()
"""

_CYPHER_MERGE_FALLBACK_ANCHORAGE = """
UNWIND $batch AS row
MATCH (b:Berth {id: row.berth_id})
MATCH (a:Anchorage {id: row.anchorage_id})
MERGE (b)-[r:FALLBACK_ANCHORAGE]->(a)
ON CREATE SET r.created_at = datetime()
"""


def fetch_anchorage_rows(pg_conn) -> list[dict]:
    """upa_anchorage(정박지 경계를 폴리곤 정점 다수로 저장)를 anchorage_name별
    중심점 1개로 집계한다.

    이 로더가 쓰는 '정박지 대표 좌표 1점' 모델과 upa_anchorage의 폴리곤 모델이
    달라서, 정점들의 단순 평균(centroid)을 대표 좌표로 쓴다 — 폴리곤 형상을
    그대로 보존하는 게 목적이 아니라 "이 정박지가 대략 어디 있는가"만 있으면
    되므로 이 근사로 충분하다.
    """
    with pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT anchorage_name, remark, anchorage_type, latitude, longitude
            FROM upa_anchorage
            WHERE anchorage_name IS NOT NULL AND anchorage_name <> ''
        """)
        raw_rows = cur.fetchall()

    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in raw_rows:
        grouped[row["anchorage_name"]].append(row)

    batch: list[dict] = []
    for name, rows in grouped.items():
        coords = [
            (r["latitude"], r["longitude"]) for r in rows
            if r["latitude"] is not None and r["longitude"] is not None
        ]
        lat = sum(c[0] for c in coords) / len(coords) if coords else None
        lon = sum(c[1] for c in coords) / len(coords) if coords else None
        remark = next((r["remark"] for r in rows if r["remark"]), None)
        anchorage_type = next((r["anchorage_type"] for r in rows if r["anchorage_type"]), None)
        lower, upper = parse_tonnage_bounds(remark)
        batch.append({
            "anchorage_id": name,
            "name": name,
            "tonnage_rule": remark,
            "tonnage_lower": lower,
            "tonnage_upper": upper,
            "anchorage_type": anchorage_type,
            "latitude": lat,
            "longitude": lon,
        })
    return batch


# onsan_berth_master.csv(입항정보 PDF)의 접안능력_DWT — upa_berth_facility.berth_capacity
# (brthdCapVl)가 온산 12부두 전부 NULL이라(실측 확인됨, 울산항 전체 69개 중
# 이 필드가 채워진 곳은 2곳뿐) 정박지 배정용 DWT를 이걸로 보강한다. 이 보강이
# 없으면 온산 부두 전부가 "DWT 미상"으로 정박지 폴백 자체를 계산할 수 없다.
ONSAN_BERTH_CAPACITY_DWT: dict[str, float] = {
    "OTK1부두": 40000, "OTK2부두": 10000, "UTK부두": 30000, "대한유화부두": 80000,
    "정일1부두": 40000, "정일2부두": 40000, "효성부두": 30000, "달포부두": 3000,
    "S-Oil 1부두": 50000, "S-Oil 2부두": 120000, "S-Oil 3부두": 50000, "S-Oil 4부두": 30000,
    "석유공사부이": 325000, "S-Oil부이": 350000, "S-Oil&오일허브 부이": 325000,
}

# 원유 VLCC(부이, 32만~35만톤)는 물리적으로 E/B 정박지에 수용 불가 — 실제로
# 항상 부이직접/외해대기다. tonnage_rule 텍스트(예: E3 "2만톤 이상")에는 상한이
# 없어 파싱만으로는 이 물리적 상한을 알 수 없으므로, 팀원 build_anchorage_
# assignment.py의 값을 그대로 가져온다(도메인 지식, 텍스트 파싱으로 복원 불가).
VLCC_BUOY_DWT = 150000

# "벙커링전용"(BUNKER_RING) 정박지는 급유 목적이라 일반 접안 대기 배정 대상이
# 아니다(팀원 모델의 E/W 계열만 정박지 대기로 쓰는 것과 동일 구분).
_GENERAL_WAITING_ANCHORAGE_TYPES = {"POLYGON", "CIRCLE"}


def assign_fallback_anchorage(dwt: float | None, anchorages: list[dict]) -> dict | None:
    """선박 DWT에 맞는 일반 대기 정박지를 고른다.

    상한(tonnage_upper)이 있으면 그 이내, 하한(tonnage_lower)만 있으면(예: E3
    "2만톤 이상") 그 이상인 정박지도 후보로 포함한다 — 이전 버전은 상한 없는
    정박지를 아예 후보에서 제외해 대형선 배정이 전부 실패하는 버그가 있었다.
    후보가 여럿이면 상한이 있는(더 타이트한) 쪽을 우선한다. VLCC급(DWT>=15만톤)
    이거나 벙커링 전용 정박지밖에 없으면 None(부이직접/외해대기)을 반환한다.
    """
    if dwt is None or dwt >= VLCC_BUOY_DWT:
        return None
    candidates = [
        a for a in anchorages
        if a.get("anchorage_type") in _GENERAL_WAITING_ANCHORAGE_TYPES
        and (a["tonnage_lower"] is not None or a["tonnage_upper"] is not None)
        and (a["tonnage_upper"] is None or dwt <= a["tonnage_upper"])
        and (a["tonnage_lower"] is None or dwt >= a["tonnage_lower"])
    ]
    if not candidates:
        return None
    return min(candidates, key=lambda a: a["tonnage_upper"] if a["tonnage_upper"] is not None else float("inf"))


def _tx_merge_anchorage(tx, batch: list[dict]) -> None:
    tx.run(_CYPHER_MERGE_ANCHORAGE, batch=batch)


def _tx_merge_fallback_anchorage(tx, batch: list[dict]) -> None:
    tx.run(_CYPHER_MERGE_FALLBACK_ANCHORAGE, batch=batch)


def transfer_anchorage_to_neo4j(pg_conn, neo4j_driver) -> None:
    """upa_anchorage(이미 매일 수집되는 정박지 GIS)를 Anchorage 노드로 적재하고,
    berth_capacity(DWT 근사) 기준으로 각 Berth의 FALLBACK_ANCHORAGE를 계산해 연결한다.

    팀원 CSV(onsan_anchorage.csv, onsan_berth_anchorage_fallback.csv)를 새로
    만들지 않고 이미 있는 upa_anchorage/upa_berth_facility에서 계산한다 — 9장
    비교분석에서 확인된 대로 anchorage_name(E1/E2/E3/W1 등)과 tonnage_rule 텍스트가
    팀원 CSV와 그대로 일치한다.
    """
    anchorages = fetch_anchorage_rows(pg_conn)
    if not anchorages:
        logger.warning("upa_anchorage에 적재할 행이 없습니다.")
        return

    berths = fetch_berth_rows(pg_conn)
    fallback_batch = []
    for berth in berths:
        # upa_berth_facility.berth_capacity가 대부분 NULL이라(실측 확인: 울산항
        # 전체 69개 중 2개만 값 있음) 온산 부두는 팀원 PDF값(ONSAN_BERTH_CAPACITY_DWT)으로 보강.
        dwt = berth["berth_capacity"] or ONSAN_BERTH_CAPACITY_DWT.get(berth["wharf_name"])
        chosen = assign_fallback_anchorage(dwt, anchorages)
        if chosen is not None:
            fallback_batch.append({"berth_id": berth["berth_id"], "anchorage_id": chosen["anchorage_id"]})

    with neo4j_driver.session(database=NEO4J_DATABASE) as session:
        session.execute_write(_tx_merge_anchorage, anchorages)
        if fallback_batch:
            session.execute_write(_tx_merge_fallback_anchorage, fallback_batch)

    logger.info(
        "Anchorage 이관 완료: Anchorage %d개, FALLBACK_ANCHORAGE 엣지 %d개 (DWT 미상/매칭 실패 %d개 제외)",
        len(anchorages), len(fallback_batch), len(berths) - len(fallback_batch),
    )


def run_berth_transfer(*, include_onsan_extensions: bool = True) -> None:
    """PostgreSQL(upa_berth_facility/upa_anchorage) -> Neo4j 이관 파이프라인 전체를 실행한다.

    include_onsan_extensions=True(기본값)면 온산 MVP 이식분(좌표기반 ADJACENT_TO
    추가·SUBSTITUTABLE_WITH·Anchorage/FALLBACK_ANCHORAGE)까지 함께 적재한다.
    전부 MERGE 기반 추가적재라 기존 데이터(전국이 아니라 울산항 전체 69개 선석,
    9장 참고)를 지우지 않는다 — DETACH DELETE 없음.
    """
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
        # 정적 CSV 보강이라 include_onsan_extensions 스코프와 무관하게 항상 실행.
        # CSV가 없거나 매칭 실패해도 경고만 남기고 계속 진행한다(fetch_cargo_kind_rows/
        # transfer_cargo_kind_to_neo4j 둘 다 fail-soft).
        transfer_cargo_kind_to_neo4j(pg_conn, neo4j_driver)

        if include_onsan_extensions:
            transfer_substitutability_to_neo4j(pg_conn, neo4j_driver)
            transfer_anchorage_to_neo4j(pg_conn, neo4j_driver)

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
