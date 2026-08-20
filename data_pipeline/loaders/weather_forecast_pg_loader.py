# -*- coding: utf-8 -*-
"""
단기예보(weather_forecast) staging → PostgreSQL 적재기

자연키(nx + ny + fcst_at_utc)로 upsert한다. 같은 미래 시각에 대한 예보가 재발표될
때마다(하루 8회) 최신 값으로 덮어써진다 — 발표가 최신일수록 더 정확한 예보이므로
과거 예보값을 이력으로 남기지 않고 항상 "지금 알 수 있는 가장 최신 예보"만 유지한다.

테이블은 backend(Alembic)가 소유한다. 이 모듈은 insert만 한다 — 먼저
backend에서 `alembic upgrade head`를 실행해 두어야 한다.

실행:
  python -m data_pipeline.loaders.weather_forecast_pg_loader
"""
from data_pipeline.common_pg_loader import load_all

TABLE_MAP = {
    "weather_forecast_stg.csv": ("weather_forecast", ["nx", "ny", "fcst_at_utc"]),
}


def load() -> None:
    load_all(TABLE_MAP, auto_create=False)


if __name__ == "__main__":
    load()
