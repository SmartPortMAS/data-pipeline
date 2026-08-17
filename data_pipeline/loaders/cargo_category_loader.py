# -*- coding: utf-8 -*-
"""
화학물질(Chemical) -> 선석 취급화물 대분류(cargo_category) 매핑 이관 모듈
==========================================================================
파이프라인 위치: (수동 큐레이션 매핑) -> [이 모듈] -> Neo4j Chemical.cargo_category

목적:
    선석(upa_berth_facility.handling_cargo_name)은 "원유/유류/액체화학" 같은
    대분류 텍스트만 갖고 있고, 개별 화학물질명이나 UN번호를 갖고 있지 않다.
    스케줄링 에이전트가 "이 화물을 취급 가능한 선석"을 찾으려면 Chemical 쪽에도
    같은 대분류가 있어야 Neo4j에서 한 번의 MATCH로 조인할 수 있다.

    이 모듈은 msds_neo4j_loader.py 가 이미 만든 Chemical 노드에 cargo_category
    속성만 추가로 SET 한다 (신규 노드/관계 생성 없음, 기존 그래프에 안전하게 추가).

알려진 한계 (문서화):
    - CHEM_CATEGORY_MAP은 34종 화학물질의 name_ko를 사람이 직접 3개 대분류
      (원유/유류/액체화학)로 분류한 수동 매핑이다. KOSHA MSDS의 공인 분류
      체계가 아니라 이번 구현에서 정한 근사치다 (msds_neo4j_loader.py의
      INCOMPATIBLE_KEYWORD_DICT와 같은 성격의 한계).
    - 수소/암모니아/메테인/프로페인/부탄 같은 가스류는 upa_berth_facility에
      별도 "가스" 카테고리가 존재하지 않아(수집된 69개 선석 기준) 가장 가까운
      기존 대분류(유류 또는 액체화학)로 편입시켰다. 실제 가스 전용 터미널
      취급 여부와는 무관한 편의적 분류다.
    - 새 화학물질이 MSDS 파이프라인에 추가되면 이 딕셔너리도 수동으로 갱신해야
      한다 (자동 추론 로직 없음).

외부 의존 라이브러리:
    pip install psycopg2-binary neo4j python-dotenv

환경변수: msds_neo4j_loader.py와 동일 (POSTGRES_*, NEO4J_*)
"""

import logging
import os

from dotenv import load_dotenv
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("cargo_category_loader")

_REQUIRED_VARS = ["NEO4J_URI", "NEO4J_USER", "NEO4J_PASSWORD", "NEO4J_DATABASE"]
_missing = [k for k in _REQUIRED_VARS if not os.getenv(k)]
if _missing:
    raise RuntimeError(f"DB 접속정보가 없습니다. .env에 설정하세요: {', '.join(_missing)}")

NEO4J_URI = os.environ["NEO4J_URI"]
NEO4J_USER = os.environ["NEO4J_USER"]
NEO4J_PASSWORD = os.environ["NEO4J_PASSWORD"]
NEO4J_DATABASE = os.environ["NEO4J_DATABASE"]

# 선석(upa_berth_facility.handling_cargo_name)에 실제로 등장하는 값과 맞춘
# 3개 대분류. 값은 chem_id (msds_chemical.chem_id / Neo4j Chemical.id) 목록.
CARGO_CATEGORIES: dict[str, list[str]] = {
    "원유": [
        "000751",  # 석유(PETROLEUM)
    ],
    "유류": [
        "000756",  # 케로젠
        "000973",  # 디젤 연료
        "000976",  # 연료, 잔사유(FUEL OIL, RESIDUAL)
        "016420",  # 가솔린
        "031533",  # 가솔린(GASOLINE) — 같은 휘발유의 별도 MSDS 등재분(CAS 86290-81-5).
                   #   화물 manifest 의 UN1203 이 실제로 이 chem_id 로 매칭된다(재항 69척,
                   #   2026-08-17 실측 — 재항 화물 중 최다). 여기 빠져 있어서 가솔린선
                   #   전부가 스케줄링 422(카테고리 미지정) → 종합 판정 보류로 죽었다.
        "016495",  # 러버 솔벤트
        "016558",  # 아스팔트
        "002524",  # 프로필렌
        "015390",  # 메테인
        "015420",  # 프로페인
        "000247",  # 부탄
    ],
    "액체화학": [
        "000034",  # 에탄올
        "000045",  # 에틸렌
        "000218",  # 트리에탄올아민
        "000233",  # p-크실렌
        "000557",  # 수소
        "000891",  # 톨루엔-2,4/2,6-디이소시아네이트
        "001008",  # 벤젠
        "001010",  # 염화비닐
        "001027",  # 스티렌
        "001032",  # 톨루엔
        "001041",  # 테트라하이드로푸란
        "001049",  # 황산
        "001065",  # 이소프로필 알코올
        "001067",  # 아세톤
        "001077",  # 크실렌
        "001093",  # 에틸렌 글리콜
        "001140",  # 트리클로로에틸렌
        "001143",  # 아크릴로니트릴
        "001151",  # 메틸 알코올
        "001162",  # 1,2-에폭시프로판
        "001167",  # 1,3-부타디엔
        "001174",  # 암모니아
        "017853",  # 폴리에테르 폴리올 수지
        "018391",  # Dodecene, branched
    ],
}

# chem_id -> category 역방향 조회용 (다른 모듈에서 재사용 가능하도록 공개)
CHEM_ID_TO_CATEGORY: dict[str, str] = {
    chem_id: category
    for category, chem_ids in CARGO_CATEGORIES.items()
    for chem_id in chem_ids
}

_CYPHER_SET_CATEGORY = """
UNWIND $batch AS row
MATCH (c:Chemical {id: row.chem_id})
SET c.cargo_category = row.category
"""


def _tx_set_category(tx, batch: list[dict]) -> None:
    tx.run(_CYPHER_SET_CATEGORY, batch=batch)


def assign_cargo_categories(driver) -> int:
    """CARGO_CATEGORIES 매핑대로 Chemical.cargo_category 속성을 설정한다.

    존재하지 않는 chem_id는 MATCH가 실패해 조용히 스킵된다(멱등, 안전).
    Returns:
        SET을 시도한 (chem_id, category) 매핑 건수.
    """
    batch = [
        {"chem_id": chem_id, "category": category}
        for chem_id, category in CHEM_ID_TO_CATEGORY.items()
    ]
    with driver.session(database=NEO4J_DATABASE) as session:
        session.execute_write(_tx_set_category, batch)
    logger.info("Chemical.cargo_category 설정 완료: %d건", len(batch))
    return len(batch)


def run_cargo_category_assignment() -> None:
    """단독 실행 진입점: Neo4j에 연결해 cargo_category를 설정한다."""
    neo4j_driver = None
    try:
        neo4j_driver = GraphDatabase.driver(
            NEO4J_URI,
            auth=(NEO4J_USER, NEO4J_PASSWORD),
            max_connection_pool_size=10,
            connection_timeout=15,
        )
        neo4j_driver.verify_connectivity()
        logger.info("Neo4j 연결 확인 완료: %s (DB: %s)", NEO4J_URI, NEO4J_DATABASE)
        assign_cargo_categories(neo4j_driver)
    except ServiceUnavailable as e:
        logger.error("Neo4j 연결 불가: %s", e)
        raise
    finally:
        if neo4j_driver:
            neo4j_driver.close()
            logger.info("Neo4j Driver 종료")


if __name__ == "__main__":
    run_cargo_category_assignment()
