# -*- coding: utf-8 -*-
"""
파고(wave) staging → PostgreSQL 적재기

자연키(station_id + observed_at_utc)로 upsert한다.

테이블은 backend(Alembic)가 소유한다. 이 모듈은 insert만 한다 — 먼저
backend에서 `alembic upgrade head`를 실행해 두어야 한다.

실행:
  python -m data_pipeline.loaders.wave_pg_loader
"""
from data_pipeline.common_pg_loader import load_all

TABLE_MAP = {
    "wave_obs_stg.csv": ("wave_obs", ["station_id", "observed_at_utc"]),
}


def load() -> None:
    load_all(TABLE_MAP, auto_create=False)


if __name__ == "__main__":
    load()
