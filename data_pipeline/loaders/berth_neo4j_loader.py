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
# ADJACENT_TO/SUBSTITUTABLE_WITH 자동 계산을 이 범위로 한정할 때 쓴다 — 울산항
# 전체 69개 선석 중 무관한 조합(예: 컨테이너부두 vs 벌크부두)까지 계산하지
# 않기 위함. 이 스코프 밖 선석은 그래프 적재 자체에는 영향 없다.
ONSAN_SCOPE_WHARF_NAMES: set[str] = {
    "OTK1부두", "OTK2부두", "정일1부두", "정일2부두", "UTK부두", "대한유화부두",
    "효성부두", "S-Oil 1부두", "S-Oil 2부두", "S-Oil 3부두", "S-Oil 4부두",
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

# ─────────────────────────────────────────────────────────────────────────────
# 취급화물 큐레이션 보정 (원천 시설데이터의 라벨이 실제 운영과 어긋나는 선석)
#
# 원천의 handling_cargo_name 은 대분류라, 물리적으로 전혀 다른 설비를 같은
# '유류'로 묶어 놓은 곳이 있다. 그대로 두면 스케줄링 에이전트가 실제로는
# 댈 수 없는 선석을 후보로 올린다(2026-08-18 실측 확인).
#
#  · 석유공사부이 — 원천 '유류'. 그러나 수심 27 m 해상 계류점(SPM)이고 운영사가
#    한국석유공사(원유비축기지)다. 같은 데이터의 다른 부이 4기(SK2·SK3·S-Oil·
#    S-Oil&오일허브)는 전부 '원유'로 라벨돼 있고, 우리 선석 큐레이션
#    (frontend geoUtils.ONSAN_BERTHS)도 이 부이를 '원유'로 적고 있다.
#    '유류'로 두면 흘수 6 m 짜리 제품유 운반선이 VLCC용 부이에 배정된다.
#
#  · 가스부두 / SK1부두 / SK2부두(SK가스㈜ 운영분) — 원천 '유류'. 운영사가
#    SK가스㈜인 LPG 터미널이다. LPG 운반선은 가압·냉동 탱크와 증기환수 배관이
#    있는 전용 터미널에만 댈 수 있어, 일반 석유제품 부두와 같은 카테고리로
#    묶으면 안 된다. 실제로 지금 재항 중인 가스선(가스 프리웨이·에코 가스·
#    HENRIETTA KOSAN 등)이 전부 이 경로로 잘못 매칭됐다.
#    ※ 'SK2부두'는 SK가스㈜(수심 7.5)와 SK에너지㈜(수심 8.0) 두 곳이 같은
#      이름을 쓴다 — 그래서 키를 (선석명, 운영사)로 잡는다.
#
# 원천 테이블을 직접 고치지 않는 이유: 수집기가 다시 돌면 되돌아간다.
# 보정은 여기 한 곳에만 두고, 근거를 남겨 팀이 검토할 수 있게 한다.
# ─────────────────────────────────────────────────────────────────────────────
HANDLING_CARGO_OVERRIDES: dict[tuple[str, str], str] = {
    ("석유공사부이", "한국석유공사"): "원유",
    ("가스부두", "SK가스㈜"): "가스",
    ("SK1부두", "SK가스㈜"): "가스",
    ("SK2부두", "SK가스㈜"): "가스",
}


def _handling_cargo(row: dict) -> str | None:
    """원천 취급화물명에 큐레이션 보정을 적용한다."""
    key = (row.get("wharf_name"), row.get("port_operator_name"))
    return HANDLING_CARGO_OVERRIDES.get(key, row.get("handling_cargo_name"))


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
                   unload_capacity, berth_vessel_count, handling_cargo_name,
                   wharf_se_name, port_operator_name, latitude, longitude
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
            # ONSAN_BERTH_CAPACITY_DWT 보정을 노드 속성 자체에 baked-in 한다
            # (2026-08-19) — 예전엔 compute_substitutability_pairs/
            # assign_fallback_anchorage 두 함수가 호출 시점마다 각자
            # `berth_capacity or ONSAN_BERTH_CAPACITY_DWT.get(...)`를 따로
            # 적용했는데, Berth.berth_capacity 속성 자체는 raw 값(69개 중
            # 대부분 NULL, 07 문서 §4.1.1)이라 08 설계문서의 부이 게이트
            # (§4.1.3-A, VLCC_BUOY_DWT_THRESHOLD 비교)처럼 새로 이 속성을
            # 직접 읽는 소비자는 보정을 못 받는 문제가 있었다. 로더 단계에서
            # 한 번만 보정해 두면 이후 모든 소비자(기존 두 함수 포함, 그쪽의
            # `or` 폴백은 이미 보정된 값에 적용돼도 무해함)가 같은 값을 본다.
            "berth_capacity": row["berth_capacity"] or ONSAN_BERTH_CAPACITY_DWT.get(name),
            # 08_스케줄링_전면재설계_자동배정_설계문서.md §5.2.1-B — 소프트 가중치
            # (타이브레이커)로만 쓴다. 결측이 많아(온산 액체화학 선석 다수는 있지만
            # SK1~8·S-Oil 1~3 등은 NULL) 하드 게이트로 쓰지 않는다.
            "unload_capacity": row["unload_capacity"],
            # 2026-08-19 — Neo4j Berth 노드의 동시접안 슬롯 수. brthdVslCntVl(동시접안
            # 가능 척수)이 이미 UPA 원본에 있었다. backend의 berth_assignment는 이제
            # (berth 테이블 없이) upa_berth_facility.berth_vessel_count를 직접 읽는다.
            "berth_vessel_count": row["berth_vessel_count"],
            # 큐레이션 오버라이드 적용값 — HANDLES 관계(아래 categories)와 같은
            # 값을 쓴다. raw 값을 그대로 두면 가스부두 등에서 "취급화물: 유류"로
            # 표시돼(대시보드 BerthInfo) 실제 HANDLES("가스")와 화면 표시가
            # 어긋난다.
            "handling_cargo_name": _handling_cargo(row),
            "wharf_se_name": row["wharf_se_name"],
            "port_operator_name": row["port_operator_name"],
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "categories": _split_cargo_categories(_handling_cargo(row)),
            "berth_group": ONSAN_BERTH_GROUP_MAP.get(name),
            # 스케줄링 에이전트가 온산 선석을 우선 배정하는 근거가 되는 값.
            # 백엔드는 coalesce(b.onsan_scope, false)로 읽는데(scheduling/
            # graph_queries.py) 이 속성을 만드는 곳이 없어 69개 선석 전부
            # false 였고, 그 결과 온산 우선 정렬이 한 번도 발동하지 못했다
            # (실측 2026-08-15: 에탄올 요청에 '신항남방파제 T/S부두'(온산 밖)가
            #  1순위로 배정됨 — 7/26 회의 요청 4번이 이 원인으로 남아 있었다).
            "onsan_scope": name in ONSAN_SCOPE_WHARF_NAMES,
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
    b.unload_capacity     = row.unload_capacity,
    b.handling_cargo_name = row.handling_cargo_name,
    b.wharf_se_name       = row.wharf_se_name,
    b.port_operator_name  = row.port_operator_name,
    b.latitude            = row.latitude,
    b.longitude           = row.longitude,
    b.berth_group         = row.berth_group,
    b.onsan_scope         = row.onsan_scope,
    b.created_at          = datetime()
ON MATCH SET
    b.wharf_name          = row.wharf_name,
    b.port_name           = row.port_name,
    b.length_m            = row.length_m,
    b.depth_m             = row.depth_m,
    b.berth_capacity      = row.berth_capacity,
    b.unload_capacity     = row.unload_capacity,
    b.handling_cargo_name = row.handling_cargo_name,
    b.wharf_se_name       = row.wharf_se_name,
    b.port_operator_name  = row.port_operator_name,
    b.latitude            = row.latitude,
    b.longitude           = row.longitude,
    b.berth_group         = row.berth_group,
    b.onsan_scope         = row.onsan_scope,
    b.updated_at          = datetime()
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
    O(n^2) 거리 계산을 하지 않는다. 좌표 결측 선석(S-Oil 부이 2기, 석유공사부이,
    달포부두 등)은 계산에서 자연히 제외된다.
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
        resolved.append({"berth_a": id_a, "berth_b": id_b, "distance_m": None})
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
        logger.warning("upa_berth_facility에 적재할 행이 없습니다.")
        return

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

    batch: list[dict] = []
    unmatched: list[str] = []
    for row in csv_rows:
        berth_ids = by_norm_name.get(row["norm_name"])
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
        logger.warning("upa_berth_facility에 적재할 행이 없습니다.")
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
    "정일1부두": 40000, "정일2부두": 40000, "효성부두": 30000,
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
