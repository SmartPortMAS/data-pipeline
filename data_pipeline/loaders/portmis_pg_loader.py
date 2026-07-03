# -*- coding: utf-8 -*-
"""
PORT-MIS staging → PostgreSQL 적재기

자연키(callsgn + entry_year + entry_count)로 upsert한다 — 같은 입출항 건이 필드
갱신을 동반해 재수집되어도 새 행으로 쌓이지 않고 최신 상태로 덮어써진다.

테이블은 backend(Alembic)가 소유한다. 이 모듈은 insert만 한다 — 먼저
backend에서 `alembic upgrade head`를 실행해 두어야 한다.

실행:
  python -m data_pipeline.loaders.portmis_pg_loader
"""
from data_pipeline.common_pg_loader import load_all

TABLE_MAP = {
    "portmis_vessel_stg.csv": ("portmis_vessel", ["callsgn", "entry_year", "entry_count"]),
}


def load() -> None:
    load_all(TABLE_MAP, auto_create=False)


if __name__ == "__main__":
    load()
