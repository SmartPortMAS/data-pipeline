# -*- coding: utf-8 -*-
"""
AIS staging → PostgreSQL 적재기

- ais_vessel_position: 시계열 스냅샷 누적 (record_uid 해시로 dedup, 이력 보존)
- ais_vessel_static:   mmsi 기준 최신 1행만 유지 (목적지 등 변경 시 덮어씀)

테이블은 backend(Alembic)가 소유한다. 이 모듈은 insert만 한다 — 먼저
backend에서 `alembic upgrade head`를 실행해 두어야 한다.

실행:
  python -m data_pipeline.loaders.ais_pg_loader
"""
from data_pipeline.common_pg_loader import load_all

TABLE_MAP = {
    "ais_vessel_position_stg.csv": ("ais_vessel_position", ["record_uid"]),
    "ais_vessel_static_stg.csv": ("ais_vessel_static", ["mmsi"]),
}


def load() -> None:
    load_all(TABLE_MAP, auto_create=False)


if __name__ == "__main__":
    load()
