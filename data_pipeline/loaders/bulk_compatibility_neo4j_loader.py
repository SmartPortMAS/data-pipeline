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
    # ── CAS가 CFR Table 1에 그대로 없어 자동 대조에서 빠진 4종 (2026-09-16) ──
    # Table 1은 이성질체 혼합물을 모(母)명칭 아래 묶고 CAS도 대표값 하나만 싣는다
    # ("unless otherwise indicated, isomers or mixtures of isomers of a particular
    #  cargo are assigned to the same group" — Appendix II). 그래서 우리가 쓰는
    # 혼합물 CAS로는 매칭되지 않는다. 명칭으로 원문을 찾아 그룹을 확정했다.
    "1330-20-7":  (32, GROUP_TYPE_CARGO, "Aromatic Hydrocarbons"),
    #   크실렌(혼합 이성질체). 원문 "Dimethylbenzene, see Xylenes"로 상호참조되고
    #   본 항목 "Xylenes ... 32 ... 106-42-3"이 그룹 32다.
    "26471-62-5": (12, GROUP_TYPE_REACTIVE, "Isocyanates"),
    #   톨루엔-2,4/2,6-디이소시아네이트(혼합). 원문 "Toluene diisocyanate ... 12 ... 584-84-9".
    "97280-83-6": (30, GROUP_TYPE_CARGO, "Olefins"),
    #   ★ 기존 데이터 오류 정정 — Dodecene, branched 를 34 Esters 로 두고 있었다.
    #   도데센은 에스터가 아니라 올레핀이다. 원문 "Dodecene (all isomers) ... 30 ...
    #   25378-22-7"로 그룹 30이 확정된다. CFR 대조가 아니었으면 드러나지 않았을
    #   분류 오류다.
    "1333-74-0":  (0, GROUP_TYPE_SPECIAL, "Unassigned Gas"),
    #   수소. CFR Table 1에 항목 자체가 없다(산적 액체화물 범위 밖). 프로젝트
    #   자체 큐레이션을 유지하되 근거가 CFR이 아님을 명시한다.
    # ── 2026-09-16 전면 재작성 — 근거를 46 CFR Part 150 원문으로 교체 ────────
    # 출처: 46 CFR Ch. I (10-1-24 Edition) Part 150
    #       · Table 1 to Part 150 "Alphabetical List of Cargoes" (화물명/그룹번호/CAS)
    #       · Table 2 to Part 150 "Grouping of Cargoes" (그룹 번호·명칭 정본)
    #       govinfo.gov CFR-2024-title46-vol5-part150.pdf 에서 CAS 824건을 추출해
    #       msds_chemical 151종과 CAS로 대조했다(직접 매칭 113종 + 동명이의 해소 4종).
    #
    # ★ 왜 다시 썼나 — 이전 매핑은 화학물질명을 보고 사람이 분류한 근사치였고,
    #   CFR 원문과 대조하니 실제로 어긋나는 것이 있었다(실측):
    #     · 헥실렌(592-41-6)   우리=20 Alcohols  →  CFR=30 Olefins
    #       ("헥실렌"은 hexylene glycol이 아니라 Hexene(all isomers)이다)
    #     · 다이에틸렌 글리콜(111-46-6)      우리=20  →  CFR=40 Glycol Ethers
    #     · 에틸렌글리콜모노부틸에테르아세테이트(112-07-2) 우리=40 → CFR=34 Esters
    #     · 프로필렌글리콜모노메틸에테르아세테이트(108-65-6) 우리=40 → CFR=34 Esters
    #   그룹 '번호 체계'는 원문과 정확히 일치함을 확인했다(2=Sulfuric Acids,
    #   32=Aromatic Hydrocarbons, 36=Halogenated Hydrocarbons, 40=Glycol Ethers …).
    #
    # ★ 여기 없는 24종은 의도적으로 비워 둔다 — CFR상 그룹은 확정됐지만
    #   그 그룹이 INCOMPATIBLE_GROUP_PAIRS에 없어서다. 관계 없이 넣으면
    #   "판정 대상인데 절대 충돌하지 않는" 화물이 되어 오히려 위험하다.
    #   보류 목록(CFR 확정 그룹):
    #     1 인산 / 3 질산 / 4 유기산 4종 / 5 수산화나트륨·칼륨 /
    #     7 지방족아민 3종 / 9 방향족아민 2종 / 10 디메틸포름아미드 /
    #     11 무수초산 / 14 아크릴레이트 4종 / 17 에피클로로히드린 /
    #     21 페놀 / 37 아디포니트릴 / 42 니트로화합물 2종
    #   → Figure 1(Compatibility Chart) 매트릭스를 확보해 쌍을 함께 추가할 것.
    "624-92-0": (0, GROUP_TYPE_SPECIAL, "Unassigned Gas"),  # 디메틸디설파이드
    "7704-34-9": (0, GROUP_TYPE_SPECIAL, "Unassigned Gas"),  # 황(SULFUR)
    "7664-93-9": (2, GROUP_TYPE_REACTIVE, "Sulfuric Acids"),  # 황산
    "7664-41-7": (6, GROUP_TYPE_REACTIVE, "Ammonia"),  # 암모니아
    "105-59-9": (8, GROUP_TYPE_REACTIVE, "Alkanolamines"),  # N-메틸다이에탄올아민
    "111-42-2": (8, GROUP_TYPE_REACTIVE, "Alkanolamines"),  # 디에탄올아민
    "141-43-5": (8, GROUP_TYPE_REACTIVE, "Alkanolamines"),  # 에탄올아민
    "102-71-6": (8, GROUP_TYPE_REACTIVE, "Alkanolamines"),  # 트리에탄올아민
    "101-68-8": (12, GROUP_TYPE_REACTIVE, "Isocyanates"),  # 4,4&#39;-메틸렌 디(비스)페닐 디이소시아네이트
    "9016-87-9": (12, GROUP_TYPE_REACTIVE, "Isocyanates"),  # 아이소사이안산 폴리메틸렌 폴리페닐렌에스터
    "126-98-7": (15, GROUP_TYPE_REACTIVE, "Substituted Allyls"),  # 메틸 아크릴로니트릴
    "107-13-1": (15, GROUP_TYPE_REACTIVE, "Substituted Allyls"),  # 아크릴로니트릴
    "107-18-6": (15, GROUP_TYPE_REACTIVE, "Substituted Allyls"),  # 알릴 알콜
    "75-56-9": (16, GROUP_TYPE_REACTIVE, "Alkylene Oxides"),  # 1,2-에폭시프로판
    "1675-54-3": (16, GROUP_TYPE_REACTIVE, "Alkylene Oxides"),  # 비스페놀 A 다이글리시딜 에테르
    "108-83-8": (18, GROUP_TYPE_REACTIVE, "Ketones"),  # 디이소부틸케톤
    "78-93-3": (18, GROUP_TYPE_REACTIVE, "Ketones"),  # 메틸 에틸 케톤
    "108-10-1": (18, GROUP_TYPE_REACTIVE, "Ketones"),  # 메틸 이소부틸 케톤
    "108-94-1": (18, GROUP_TYPE_REACTIVE, "Ketones"),  # 시클로헥사논
    "67-64-1": (18, GROUP_TYPE_REACTIVE, "Ketones"),  # 아세톤
    "107-88-0": (20, GROUP_TYPE_REACTIVE, "Alcohols, Glycols"),  # 1,3-부탄디올
    "143-08-8": (20, GROUP_TYPE_REACTIVE, "Alcohols, Glycols"),  # 1-노난올
    "71-36-3": (20, GROUP_TYPE_REACTIVE, "Alcohols, Glycols"),  # n-부틸알코올
    "71-23-8": (20, GROUP_TYPE_REACTIVE, "Alcohols, Glycols"),  # 노말-프로필 알콜
    "67-56-1": (20, GROUP_TYPE_REACTIVE, "Alcohols, Glycols"),  # 메틸 알코올
    "25103-58-6": (20, GROUP_TYPE_REACTIVE, "Alcohols, Glycols"),  # 삼차-도데실 메르캅탄(tert-DODECYL MERCAPT
    "112-92-5": (20, GROUP_TYPE_REACTIVE, "Alcohols, Glycols"),  # 스테아릴 알코올
    "108-93-0": (20, GROUP_TYPE_REACTIVE, "Alcohols, Glycols"),  # 시클로헥사놀
    "64-17-5": (20, GROUP_TYPE_REACTIVE, "Alcohols, Glycols"),  # 에탄올
    "107-21-1": (20, GROUP_TYPE_REACTIVE, "Alcohols, Glycols"),  # 에틸렌 글리콜
    "111-87-5": (20, GROUP_TYPE_REACTIVE, "Alcohols, Glycols"),  # 옥타놀-1
    "78-83-1": (20, GROUP_TYPE_REACTIVE, "Alcohols, Glycols"),  # 이소부틸 알코올
    "67-63-0": (20, GROUP_TYPE_REACTIVE, "Alcohols, Glycols"),  # 이소프로필 알코올
    "57-55-6": (20, GROUP_TYPE_REACTIVE, "Alcohols, Glycols"),  # 프로필렌 글리콜
    "106-99-0": (30, GROUP_TYPE_CARGO, "Olefins"),  # 1,3-부타디엔
    "106-98-9": (30, GROUP_TYPE_CARGO, "Olefins"),  # 1-부텐
    "111-66-0": (30, GROUP_TYPE_CARGO, "Olefins"),  # 1-옥텐
    "25167-70-8": (30, GROUP_TYPE_CARGO, "Olefins"),  # 다이아이소뷰틸렌
    "77-73-6": (30, GROUP_TYPE_CARGO, "Olefins"),  # 디시클로펜타디엔
    "100-42-5": (30, GROUP_TYPE_CARGO, "Olefins"),  # 스티렌
    "74-85-1": (30, GROUP_TYPE_CARGO, "Olefins"),  # 에틸렌
    "16219-75-3": (30, GROUP_TYPE_CARGO, "Olefins"),  # 에틸리덴 노보르닌
    "78-79-5": (30, GROUP_TYPE_CARGO, "Olefins"),  # 이소프렌(2-메틸-1,3-부타디엔)
    "115-07-1": (30, GROUP_TYPE_CARGO, "Olefins"),  # 프로필렌
    "592-41-6": (30, GROUP_TYPE_CARGO, "Olefins"),  # 헥실렌
    "74-82-8": (31, GROUP_TYPE_CARGO, "Paraffins"),  # 메테인
    "106-97-8": (31, GROUP_TYPE_CARGO, "Paraffins"),  # 부탄
    "110-82-7": (31, GROUP_TYPE_CARGO, "Paraffins"),  # 시클로헥산
    "74-84-0": (31, GROUP_TYPE_CARGO, "Paraffins"),  # 에탄
    "74-98-6": (31, GROUP_TYPE_CARGO, "Paraffins"),  # 프로페인
    "110-54-3": (31, GROUP_TYPE_CARGO, "Paraffins"),  # 헥산
    "142-82-5": (31, GROUP_TYPE_CARGO, "Paraffins"),  # 헵탄
    "106-42-3": (32, GROUP_TYPE_CARGO, "Aromatic Hydrocarbons"),  # p-크실렌
    "71-43-2": (32, GROUP_TYPE_CARGO, "Aromatic Hydrocarbons"),  # 벤젠
    "100-41-4": (32, GROUP_TYPE_CARGO, "Aromatic Hydrocarbons"),  # 에틸벤젠
    "98-82-8": (32, GROUP_TYPE_CARGO, "Aromatic Hydrocarbons"),  # 큐멘
    "108-88-3": (32, GROUP_TYPE_CARGO, "Aromatic Hydrocarbons"),  # 톨루엔
    "8006-61-9": (33, GROUP_TYPE_CARGO, "Misc. Hydrocarbon Mixtures"),  # 가솔린
    "68334-30-5": (33, GROUP_TYPE_CARGO, "Misc. Hydrocarbon Mixtures"),  # 디젤 연료
    "8030-30-6": (33, GROUP_TYPE_CARGO, "Misc. Hydrocarbon Mixtures"),  # 러버 솔벤트
    "8002-05-9": (33, GROUP_TYPE_CARGO, "Misc. Hydrocarbon Mixtures"),  # 석유(PETROLEUM)
    "8052-41-3": (33, GROUP_TYPE_CARGO, "Misc. Hydrocarbon Mixtures"),  # 스토다드 솔벤트
    "8052-42-4": (33, GROUP_TYPE_CARGO, "Misc. Hydrocarbon Mixtures"),  # 아스팔트
    "68476-33-5": (33, GROUP_TYPE_CARGO, "Misc. Hydrocarbon Mixtures"),  # 연료, 잔사유(FUEL OIL, RESIDUAL)
    "8008-20-6": (33, GROUP_TYPE_CARGO, "Misc. Hydrocarbon Mixtures"),  # 케로젠
    "3648-20-2": (34, GROUP_TYPE_CARGO, "Esters"),  # 1,2-Benzenedicarboxylic acid diu
    "28553-12-0": (34, GROUP_TYPE_CARGO, "Esters"),  # 다이아이소노닐 프탈산(DIISONONYL PHTHALATE
    "117-81-7": (34, GROUP_TYPE_CARGO, "Esters"),  # 디(2-에틸헥실)프탈레이트
    "6422-86-2": (34, GROUP_TYPE_CARGO, "Esters"),  # 디옥틸 테레프탈산(DIOCTYL TEREPHTHALATE)
    "109-21-7": (34, GROUP_TYPE_CARGO, "Esters"),  # 뷰틸 뷰틸산
    "763-69-9": (34, GROUP_TYPE_CARGO, "Esters"),  # 에틸 베타-에톡시프로피온산(ETHYL BETA-ETHOXY
    "112-07-2": (34, GROUP_TYPE_CARGO, "Esters"),  # 에틸렌글리콜모노부틸에테르아세테이트
    "556-67-2": (34, GROUP_TYPE_CARGO, "Esters"),  # 옥타메틸사이클로테트라실록산
    "123-86-4": (34, GROUP_TYPE_CARGO, "Esters"),  # 초산 부틸
    "108-65-6": (34, GROUP_TYPE_CARGO, "Esters"),  # 프로필렌 글리콜 모노메틸 에테르 아세트산
    "75-01-4": (35, GROUP_TYPE_CARGO, "Vinyl Halides"),  # 염화비닐
    "75-09-2": (36, GROUP_TYPE_CARGO, "Halogenated Hydrocarbons"),  # 디클로로메탄
    "107-06-2": (36, GROUP_TYPE_CARGO, "Halogenated Hydrocarbons"),  # 이염화에틸렌
    "67-66-3": (36, GROUP_TYPE_CARGO, "Halogenated Hydrocarbons"),  # 트리클로로메탄
    "79-01-6": (36, GROUP_TYPE_CARGO, "Halogenated Hydrocarbons"),  # 트리클로로에틸렌
    "127-18-4": (36, GROUP_TYPE_CARGO, "Halogenated Hydrocarbons"),  # 퍼클로로에틸렌
    "109-86-4": (40, GROUP_TYPE_CARGO, "Glycol Ethers"),  # 2-메톡시에탄올
    "111-76-2": (40, GROUP_TYPE_CARGO, "Glycol Ethers"),  # 2-부톡시에탄올
    "111-46-6": (40, GROUP_TYPE_CARGO, "Glycol Ethers"),  # 다이에틸렌 글리콜
    "111-77-3": (40, GROUP_TYPE_CARGO, "Glycol Ethers"),  # 다이에틸렌 글리콜 모노메틸 에테르
    "111-90-0": (40, GROUP_TYPE_CARGO, "Glycol Ethers"),  # 디에틸렌 글리콜 모노에틸 에테르
    "34590-94-8": (40, GROUP_TYPE_CARGO, "Glycol Ethers"),  # 디프로필렌 글리콜메틸 에테르
    "25265-71-8": (40, GROUP_TYPE_CARGO, "Glycol Ethers"),  # 옥시비스프로판올(OXYBISPROPANOL)
    "9082-00-2": (40, GROUP_TYPE_CARGO, "Glycol Ethers"),  # 폴리에테르 폴리올 수지
    "25791-96-2": (40, GROUP_TYPE_CARGO, "Glycol Ethers"),  # 폴리프로필렌 트리올(POLYPROPYLENE TRIOL)
    "107-98-2": (40, GROUP_TYPE_CARGO, "Glycol Ethers"),  # 프로필렌 글리콜 모노메틸 에테르
    "1634-04-4": (41, GROUP_TYPE_CARGO, "Ethers"),  # 메틸삼차 부틸에테르
    "109-99-9": (41, GROUP_TYPE_CARGO, "Ethers"),  # 테트라하이드로푸란
    # ── 2026-09-16 2차 추가 (24종) — Figure 1 매트릭스 확보로 보류 해제 ──────
    # 1차에서는 이 24종의 그룹(1·3·4·5·7·9·10·11·14·17·21·37·42)이
    # INCOMPATIBLE_GROUP_PAIRS에 없어 "판정 대상인데 절대 충돌하지 않는" 상태가
    # 되므로 일부러 비워 두었다. 아래 Figure 1 전체 매트릭스(120쌍)를 원문에서
    # 추출해 넣으면서 그 제약이 풀려 함께 등록한다.
    "7664-38-2": (1, GROUP_TYPE_REACTIVE, "Non-Oxidizing Mineral Acids"),  # 인산
    "7697-37-2": (3, GROUP_TYPE_REACTIVE, "Nitric Acids"),  # 질산
    "583-91-5": (4, GROUP_TYPE_REACTIVE, "Organic Acids"),  # 2-Hydroxy-4-(methylthio)butano
    "79-10-7": (4, GROUP_TYPE_REACTIVE, "Organic Acids"),  # 아크릴산
    "64-19-7": (4, GROUP_TYPE_REACTIVE, "Organic Acids"),  # 초산
    "79-09-4": (4, GROUP_TYPE_REACTIVE, "Organic Acids"),  # 프로피온산
    "1310-58-3": (5, GROUP_TYPE_REACTIVE, "Caustics"),  # 수산화 칼륨
    "1310-73-2": (5, GROUP_TYPE_REACTIVE, "Caustics"),  # 수산화나트륨
    "107-15-3": (7, GROUP_TYPE_REACTIVE, "Aliphatic Amines"),  # 1,2-디아미노에탄
    "111-40-0": (7, GROUP_TYPE_REACTIVE, "Aliphatic Amines"),  # 디에틸렌 트리아민
    "124-09-4": (7, GROUP_TYPE_REACTIVE, "Aliphatic Amines"),  # 헥사메틸렌디아민
    "872-50-4": (9, GROUP_TYPE_REACTIVE, "Aromatic Amines"),  # 1-메틸-2-피롤리디논
    "62-53-3": (9, GROUP_TYPE_REACTIVE, "Aromatic Amines"),  # 아닐린
    "68-12-2": (10, GROUP_TYPE_REACTIVE, "Amides"),  # 디메틸포름아미드
    "108-24-7": (11, GROUP_TYPE_REACTIVE, "Organic Anhydrides"),  # 무수초산
    "103-11-7": (14, GROUP_TYPE_REACTIVE, "Acrylates"),  # 2-에틸헥실 아크릴산
    "141-32-2": (14, GROUP_TYPE_REACTIVE, "Acrylates"),  # 노말-부틸아크릴레이트
    "80-62-6": (14, GROUP_TYPE_REACTIVE, "Acrylates"),  # 메틸메타크릴레이트
    "140-88-5": (14, GROUP_TYPE_REACTIVE, "Acrylates"),  # 에틸 아크릴레이트
    "106-89-8": (17, GROUP_TYPE_REACTIVE, "Epichlorohydrins"),  # 에피클로로히드린
    "108-95-2": (21, GROUP_TYPE_REACTIVE, "Phenols, Cresols"),  # 페놀
    "111-69-3": (37, GROUP_TYPE_CARGO, "Nitriles"),  # 아디포니트릴
    "108-03-2": (42, GROUP_TYPE_CARGO, "Nitrocompounds"),  # 1-니트로프로판
    "79-24-3": (42, GROUP_TYPE_CARGO, "Nitrocompounds"),  # 니트로에탄
}

INCOMPATIBLE_GROUP_PAIRS: frozenset[frozenset[int]] = frozenset({
    # ── Figure 1 to Part 150 "Compatibility Chart" 전체 (120쌍) ────────────
    # 출처: 46 CFR Ch. I (10-1-24 Edition) Pt. 150, Fig. 1
    #       govinfo CFR-2024-title46-vol5-part150.pdf 3쪽에 내장된 TIFF
    #       (3088x2692, 1bit)를 추출해 격자 좌표로 X 표시를 기계 판독했다.
    #       판독 검증: (a) 반응성×반응성 구간 대칭성 위반 0건,
    #                  (b) 검출 누락 3칸(행16 열4·5, 행17 열6)은 확대 육안 확인 후 보정.
    #
    # 규정상 성질(Appendix II 원문):
    #   · 1~22 = Reactive Groups, 30~43 = Cargo Groups
    #   · "Cargo Groups do not react hazardously with one another."
    #     → 화물그룹끼리는 비호환 쌍이 없는 것이 정상이다. 가솔린(33)·벤젠(32)이
    #       서로 충돌하지 않는 것은 결함이 아니라 규정 그대로다.
    frozenset({1, 2}), frozenset({1, 5}), frozenset({1, 6}), frozenset({1, 7}),
    frozenset({1, 8}), frozenset({1, 9}), frozenset({1, 10}), frozenset({1, 11}),
    frozenset({1, 12}), frozenset({1, 13}), frozenset({1, 16}), frozenset({1, 17}),
    frozenset({2, 3}), frozenset({2, 4}), frozenset({2, 5}), frozenset({2, 6}),
    frozenset({2, 7}), frozenset({2, 8}), frozenset({2, 9}), frozenset({2, 10}),
    frozenset({2, 11}), frozenset({2, 12}), frozenset({2, 13}), frozenset({2, 14}),
    frozenset({2, 15}), frozenset({2, 16}), frozenset({2, 17}), frozenset({2, 18}),
    frozenset({2, 19}), frozenset({2, 20}), frozenset({2, 21}), frozenset({2, 22}),
    frozenset({2, 30}), frozenset({2, 34}), frozenset({2, 37}), frozenset({2, 40}),
    frozenset({2, 41}), frozenset({2, 43}), frozenset({3, 5}), frozenset({3, 6}),
    frozenset({3, 7}), frozenset({3, 8}), frozenset({3, 9}), frozenset({3, 10}),
    frozenset({3, 11}), frozenset({3, 12}), frozenset({3, 13}), frozenset({3, 14}),
    frozenset({3, 15}), frozenset({3, 16}), frozenset({3, 17}), frozenset({3, 18}),
    frozenset({3, 19}), frozenset({3, 20}), frozenset({3, 21}), frozenset({3, 30}),
    frozenset({3, 32}), frozenset({3, 33}), frozenset({3, 34}), frozenset({3, 35}),
    frozenset({3, 41}), frozenset({4, 5}), frozenset({4, 6}), frozenset({4, 7}),
    frozenset({4, 8}), frozenset({4, 12}), frozenset({4, 16}), frozenset({4, 17}),
    frozenset({5, 11}), frozenset({5, 12}), frozenset({5, 16}), frozenset({5, 17}),
    frozenset({5, 19}), frozenset({5, 20}), frozenset({5, 21}), frozenset({5, 22}),
    frozenset({5, 42}), frozenset({6, 10}), frozenset({6, 11}), frozenset({6, 12}),
    frozenset({6, 13}), frozenset({6, 16}), frozenset({6, 17}), frozenset({6, 19}),
    frozenset({6, 42}), frozenset({7, 11}), frozenset({7, 12}), frozenset({7, 13}),
    frozenset({7, 14}), frozenset({7, 15}), frozenset({7, 16}), frozenset({7, 17}),
    frozenset({7, 18}), frozenset({7, 19}), frozenset({7, 20}), frozenset({7, 21}),
    frozenset({7, 22}), frozenset({7, 38}), frozenset({7, 42}), frozenset({8, 11}),
    frozenset({8, 12}), frozenset({8, 13}), frozenset({8, 14}), frozenset({8, 15}),
    frozenset({8, 16}), frozenset({8, 17}), frozenset({8, 19}), frozenset({8, 38}),
    frozenset({8, 42}), frozenset({9, 11}), frozenset({9, 12}), frozenset({9, 19}),
    frozenset({9, 42}), frozenset({10, 12}), frozenset({10, 21}), frozenset({12, 20}),
    frozenset({12, 22}), frozenset({12, 40}), frozenset({12, 43}), frozenset({22, 35}),

    # ── 아래 5쌍은 Figure 1에 없다 — 출처 추적 결과 (2026-09-16) ────────────
    # 이전 구현의 21쌍 중 16쌍은 Figure 1과 일치했다. 나머지 5쌍을 Appendix I
    # (Exceptions to the Chart) 원문까지 대조한 결과는 다음과 같다.
    #
    # [출처 특정됨 — 개별 물질 예외를 그룹 단위로 일반화한 것]
    #   {12, 15} ← Appendix I(b) "Allyl alcohol (15) is not compatible with
    #              Group 12, Isocyanates."
    #              → 규정이 막는 것은 **알릴 알콜**(15의 한 구성원) × 그룹12이지
    #                "모든 치환알릴 × 모든 이소시아네이트"가 아니다.
    #   {12, 18} ← Appendix I(b) "Cyclohexanone/Cyclohexanol mixture (18) is not
    #              compatible with Group 12, Isocyanates."
    #              → 마찬가지로 **시클로헥사논/시클로헥사놀 혼합물** 한정이다.
    #
    # [출처 없음 — Figure 1에도 Appendix I(a)(b)에도 해당 조합이 없다]
    #   {6, 15} · {12, 16} · {15, 16}
    #
    # 다섯 쌍 모두 지우지 않는다 — 차트보다 더 막는 것은 fail-safe 방향이라,
    # 근거 없이 '안전'을 넓히는 것보다 낫다. 다만 과잉 차단이므로:
    #   · {12,15}·{12,18}은 그룹쌍이 아니라 _EXCEPTION_BLOCKED의 물질쌍으로
    #     옮기는 것이 규정에 맞다(알릴 알콜 CAS 107-18-6, 시클로헥사논 108-94-1).
    #     지금 옮기지 않는 이유는 판정이 느슨해지는 변경이라 별도 검토가 필요해서다.
    #   · 나머지 3쌍은 출처를 계속 찾거나, 못 찾으면 제거를 검토할 것.
    frozenset({6, 15}), frozenset({12, 15}), frozenset({12, 16}),
    frozenset({12, 18}), frozenset({15, 16}),
})

# ── Appendix I to Part 150 "Exceptions to the Chart" (2026-09-16 근거 확인) ──
# (a) 차트상 비호환이지만 시험 결과 위험하지 않다고 확인된 조합.
#     원문: "Acrylonitrile (15) ... Compatible with: Triethanolamine (8)."
#     기존 데이터가 원문과 정확히 일치함을 확인했다.
_EXCEPTION_SAFE: frozenset[frozenset[str]] = frozenset({
    frozenset({"102-71-6", "107-13-1"}),  # 트리에탄올아민(8) × 아크릴로니트릴(15)
})
# (b) 차트상 호환이지만 위험하다고 확인된 조합.
#     원문: "Glycol Ethers (Group 40) are not compatible with Acrylonitrile (Group 15)"
#
#     ★ 2026-09-16 수정 — 규정은 **글리콜에테르 그룹 전체** × 아크릴로니트릴을
#       막는데, 기존 데이터는 폴리에테르 폴리올(9082-00-2) 한 종만 담고 있어
#       나머지 글리콜에테르가 통과하고 있었다(과소 차단). 벌크 매핑이 CFR 기반으로
#       늘면서 그룹 40 소속이 1종 -> 10종이 되어 누락 범위가 그만큼 커졌다.
#       원문대로 10종 전부를 등록한다.
_EXCEPTION_BLOCKED: frozenset[frozenset[str]] = frozenset({
    frozenset({"109-86-4", "107-13-1"}),
    frozenset({"111-76-2", "107-13-1"}),
    frozenset({"111-46-6", "107-13-1"}),
    frozenset({"111-77-3", "107-13-1"}),
    frozenset({"111-90-0", "107-13-1"}),
    frozenset({"34590-94-8", "107-13-1"}),
    frozenset({"25265-71-8", "107-13-1"}),
    frozenset({"9082-00-2", "107-13-1"}),
    frozenset({"25791-96-2", "107-13-1"}),
    frozenset({"107-98-2", "107-13-1"}),
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


_CYPHER_PRUNE_STALE_GROUPS = """
UNWIND $rows AS row
MATCH (c:Chemical {id: row.chem_id})-[r:IN_COMPATIBILITY_GROUP]->(g:CompatibilityGroup)
WHERE g.group_no <> row.group_no
DELETE r
"""

_CYPHER_PRUNE_UNMAPPED = """
MATCH (c:Chemical)-[r:IN_COMPATIBILITY_GROUP]->(:CompatibilityGroup)
WHERE NOT c.id IN $mapped_ids
DELETE r
"""


def _tx_prune_stale(tx, rows: list[dict], mapped_ids: list[str]) -> None:
    """이 참조 데이터에서 빠졌거나 그룹이 바뀐 화물의 옛 관계를 지운다.

    [2026-09-16 추가] 이 로더는 MERGE만 하고 있어 재적재해도 옛 관계가 그대로
    남았다. 실제로 그 때문에 한 화물이 서로 다른 두 그룹에 동시에 걸린 상태가
    생겼다(실측 4종 — 예: 헥실렌이 30 Olefins와 20 Alcohols 양쪽). 그러면
    양쪽 그룹의 비호환 관계가 모두 적용돼, 근거가 틀린 쪽에서도 충돌이 나온다.
    berth_neo4j_loader가 HANDLES에 대해 하는 것과 같은 정리를 여기서도 한다.
    """
    tx.run(_CYPHER_PRUNE_STALE_GROUPS, rows=rows)
    tx.run(_CYPHER_PRUNE_UNMAPPED, mapped_ids=mapped_ids)


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
        # 옛 관계 정리를 SET보다 먼저 한다 — 이 참조 데이터에서 빠졌거나 그룹이
        # 바뀐 화물의 IN_COMPATIBILITY_GROUP을 지워, 재적재가 몇 번을 돌아도
        # 같은 결과가 되도록(멱등) 만든다.
        session.execute_write(
            _tx_prune_stale,
            [{"chem_id": r["chem_id"], "group_no": r["group_no"]} for r in group_batch],
            [r["chem_id"] for r in group_batch],
        )
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
