# -*- coding: utf-8 -*-
"""
vessel_spec_stg.csv → PostgreSQL 적재기

자연키(callsgn)로 upsert한다 — 선박 제원은 자주 바뀌지 않으므로 이력이 아니라
callsgn당 최신 1행만 유지한다.

테이블은 backend(Alembic)가 소유한다(0013_create_vessel_spec.py). 이 모듈은
insert만 한다 — 먼저 backend에서 `alembic upgrade head`를 실행해 두어야 한다.

실행:
  python -m data_pipeline.loaders.vessel_spec_pg_loader
"""
from data_pipeline.common_pg_loader import load_all

TABLE_MAP = {
    "vessel_spec_stg.csv": ("vessel_spec", ["callsgn"]),
}


def load() -> None:
    load_all(TABLE_MAP, auto_create=False)


if __name__ == "__main__":
    load()
