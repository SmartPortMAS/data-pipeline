# -*- coding: utf-8 -*-
"""
조위 예보(tide_forecast) staging → PostgreSQL 적재기

자연키(station_id + predicted_at_utc)로 upsert한다. 예보는 매번 다시 받으므로
같은 시각이 반복해서 들어오는데, 조석 상수가 갱신되면 최신값이 이긴다.

테이블은 backend(Alembic)가 소유한다. 이 모듈은 insert만 한다 — 먼저
backend에서 `alembic upgrade head`(0024 이상)를 실행해 두어야 한다.

실행:
  python -m data_pipeline.loaders.tide_forecast_pg_loader
"""
from data_pipeline.common_pg_loader import load_all

TABLE_MAP = {
    "tide_forecast_stg.csv": ("tide_forecast", ["station_id", "predicted_at_utc"]),
}


def load() -> None:
    load_all(TABLE_MAP, auto_create=False)


if __name__ == "__main__":
    load()
