# -*- coding: utf-8 -*-
"""msds_chemical 구조화 컬럼 백필 — msds_payload(JSONB)에서 컬럼으로 승격.

왜 별도 모듈인가
---------------
이 백필은 원래 Alembic 0007 마이그레이션 안에만 있었다. 마이그레이션은 1회만
실행되므로, 그 뒤에 새로 수집·적재한 행은 구조화 컬럼이 전부 NULL로 남는다.

실제로 그 일이 났다 (2026-09-13, 151종 재수집):
    [5] 인화점: 숫자 0 / 원문 0 / 전체 151
msds_pg_loader 는 기본 컬럼 + msds_payload 만 INSERT 하고, 승격은 하지 않는다.
적재 경로에 백필이 없으면 "DB를 비우고 다시 받을 때마다" 조용히 사라진다.

그래서 적재 직후 항상 돌 수 있도록 떼어냈다. 마이그레이션의 SQL과 같은 식을
쓰되(결과가 달라지면 안 된다), 여러 번 돌려도 같은 결과가 나온다(멱등).

실행: py -m data_pipeline.loaders.msds_structured_backfill
"""
import logging
import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)

# (컬럼, 섹션, KOSHA msdsItemCode) — Alembic 0007 의 _COLUMNS 와 같아야 한다.
COLUMNS: list[tuple[str, str, str]] = [
    ("flash_point_text", "detail09", "I14"),
    ("boiling_point_text", "detail09", "I12"),
    ("vapor_pressure_text", "detail09", "I22"),
    ("specific_gravity_text", "detail09", "I28"),
    ("packing_group", "detail14", "N08"),
    ("ems_fire", "detail14", "N1202"),
    ("ems_spill", "detail14", "N1204"),
    ("signal_word", "detail02", "B0404"),
    ("exposure_limit_kr", "detail08", "H0202"),
]

# KOSHA 가 "값 없음"을 나타내는 문자열. NULL 로 정규화하지 않으면
# "인화점: 자료없음"이 값처럼 프롬프트에 실려 LLM 이 근거로 인용한다.
NULL_VALUES = ("자료없음", "해당없음", "-", "", "N/A", "없음")

# 원문에 HTML 엔티티가 그대로 들어있다(예: "&lt; 20 ℃"). 이중 이스케이프된 값도
# 있어 2회 적용한다(실측 최대 깊이).
_UNESCAPE = [("&lt;", "<"), ("&gt;", ">"), ("&amp;", "&"), ("&quot;", '"')]
_UNESCAPE_PASSES = 2


def _item_expr(section: str, code: str) -> str:
    expr = (
        f"btrim((SELECT i->>'itemDetail' "
        f"FROM jsonb_array_elements(msds_payload->'{section}'->'data') i "
        f"WHERE i->>'msdsItemCode' = '{code}' LIMIT 1))"
    )
    for _ in range(_UNESCAPE_PASSES):
        for entity, char in _UNESCAPE:
            expr = f"replace({expr}, '{entity}', '{char}')"
    return expr


def _dsn() -> str:
    return (
        f"host={os.getenv('POSTGRES_HOST', 'localhost')} "
        f"port={os.getenv('POSTGRES_PORT', '5433')} "
        f"dbname={os.getenv('POSTGRES_DB', 'smartport')} "
        f"user={os.getenv('POSTGRES_USER', 'smartport')} "
        f"password={os.getenv('POSTGRES_PASSWORD', 'smartportmas')}"
    )


def backfill(conn=None) -> dict:
    """구조화 컬럼을 채운다. 채워진 행 수를 컬럼별로 돌려준다."""
    own = conn is None
    conn = conn or psycopg2.connect(_dsn())
    filled: dict[str, int] = {}
    try:
        cur = conn.cursor()
        null_list = ", ".join(f"'{v}'" for v in NULL_VALUES)
        for name, section, code in COLUMNS:
            expr = _item_expr(section, code)
            cur.execute(
                f"UPDATE msds_chemical SET {name} = "
                f"CASE WHEN coalesce({expr}, '') IN ({null_list}) THEN NULL "
                f"ELSE {expr} END "
                f"WHERE jsonb_typeof(msds_payload->'{section}'->'data') = 'array'"
            )
            cur.execute(f"SELECT count(*) FROM msds_chemical WHERE {name} IS NOT NULL")
            filled[name] = cur.fetchone()[0]

        # flash_point_celsius — 원문에서 첫 숫자를 뽑는다. "&lt; 20 ℃", "-1~565 ℃"
        # 처럼 부등호·범위·출처가 섞여 있어 항상 성공하지는 않는다. 실패하면 NULL 로
        # 두고 원문을 쓴다 — 억지 파싱값으로 잘못된 수치를 답변에 싣지 않기 위해서다.
        cur.execute(
            "UPDATE msds_chemical SET flash_point_celsius = "
            "(substring(flash_point_text FROM '-?[0-9]+\\.?[0-9]*'))::float "
            "WHERE flash_point_text ~ '-?[0-9]+\\.?[0-9]*'"
        )
        cur.execute("SELECT count(*) FROM msds_chemical WHERE flash_point_celsius IS NOT NULL")
        filled["flash_point_celsius"] = cur.fetchone()[0]
        conn.commit()
        cur.close()
    finally:
        if own:
            conn.close()
    return filled


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    result = backfill()
    total = max(result.values()) if result else 0
    for col, n in result.items():
        print(f"  {col:<24} {n}행")
    print(f"[완료] 구조화 컬럼 {len(result)}개 백필")
