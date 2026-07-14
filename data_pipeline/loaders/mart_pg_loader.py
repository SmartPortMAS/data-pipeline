# -*- coding: utf-8 -*-
"""
통합 마트(ulsan_vessel_mart) → PostgreSQL 적재기

create_mart.py 가 생성한 data/mart/ulsan_vessel_mart.csv 를 적재한다.
마트 행은 선박 위치 스냅샷(UPA 항내 선박위치 — 기본 / 레거시 AIS — 폴백)을
기준으로 PORT-MIS·기상·UPA·MSDS 를 enrich 한 것이므로, record_uid 해시로
dedup 하며 이력을 누적한다 (같은 내용 재적재 시 무시).

스키마 소유권: 마트는 파이프라인이 산출하는 파생 테이블이므로 backend(Alembic)
소유가 아니라 auto_create=True 로 파이프라인이 직접 생성한다.
(추후 backend 조회 API가 붙으면 Alembic 이관 검토)

스키마 변경 대응: 위치 소스 교체 등으로 마트 컬럼셋이 바뀌면 기존 테이블과
INSERT 컬럼이 어긋나 UndefinedColumn 오류가 난다. 마트는 CSV에서 언제든
재생성 가능한 파생 테이블이므로, 컬럼셋 불일치를 감지하면 테이블을 DROP 하고
새 스키마로 재적재한다 (원본 staging 테이블들은 건드리지 않는다).

실행:
  python -m data_pipeline.loaders.mart_pg_loader
"""
import os

import pandas as pd
from sqlalchemy import inspect, text

from data_pipeline.common_pg_loader import get_engine, load_all

MART_DIR = "data/mart"

TABLE_MAP = {
    "ulsan_vessel_mart.csv": ("ulsan_vessel_mart", ["record_uid"]),
}


def _drop_if_schema_changed() -> None:
    """CSV 컬럼셋과 기존 마트 테이블 컬럼셋이 다르면 테이블을 DROP 한다."""
    engine = get_engine()
    insp = inspect(engine)
    for fname, (table, _ucols) in TABLE_MAP.items():
        path = os.path.join(MART_DIR, fname)
        if not os.path.exists(path) or not insp.has_table(table):
            continue
        csv_cols = set(pd.read_csv(path, nrows=0).columns) | {"record_uid"}
        db_cols = {c["name"] for c in insp.get_columns(table)}
        if csv_cols != db_cols:
            print(f"  - 마트 스키마 변경 감지: {table} DROP 후 재생성 "
                  f"(CSV {len(csv_cols)}컬럼 vs DB {len(db_cols)}컬럼)")
            with engine.begin() as conn:
                conn.execute(text(f'DROP TABLE "{table}";'))


def load() -> None:
    _drop_if_schema_changed()
    load_all(TABLE_MAP, staging_dir=MART_DIR, auto_create=True)


if __name__ == "__main__":
    load()
