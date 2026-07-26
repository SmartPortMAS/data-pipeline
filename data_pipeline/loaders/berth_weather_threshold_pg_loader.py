# -*- coding: utf-8 -*-
"""부두그룹별 기상 임계값 1회성 시드 로더 (온산 MVP 이식분).

파이프라인 위치: data/seed/berth_weather_thresholds_seed.csv -> [이 모듈] -> PostgreSQL

다른 로더들과 달리 API 수집이 아니다 — 값 자체가 각 터미널 입항정보 PDF 9.8절을
사람이 옮겨 적은 정적 데이터라(온산MVP_이식_코드변화예측_backend_datapipeline_20260725.md
9장 참고), 반복 수집이 필요 없는 1회성 시드다. 그래서 run_pipeline.py의 자동
DOMAINS에는 넣지 않고 필요할 때(신규 부두그룹 추가 등) 수동 실행한다.

테이블(`berth_weather_threshold`)은 backend(Alembic)가 소유한다
(alembic/versions/0004_create_berth_weather_threshold.py). 이 모듈은 insert(upsert)만
한다 — 먼저 backend에서 `alembic upgrade head`를 실행해 두어야 한다.

실행:
    python -m data_pipeline.loaders.berth_weather_threshold_pg_loader
"""
import os

import pandas as pd

from data_pipeline.common_pg_loader import get_engine, upsert_dataframe

SEED_CSV = os.path.join("data", "seed", "berth_weather_thresholds_seed.csv")
TABLE_NAME = "berth_weather_threshold"
UNIQUE_COLS = ["berth_group"]

# CSV(한글 헤더, 팀원 원본 컬럼명)를 DB 컬럼명(영문)으로 변환.
# 원본 CSV는 그대로 두고(팀원 문서와 1:1 대조 가능하게) 로더에서만 rename한다.
COLUMN_MAP = {
    "중단_풍속_ms": "stop_wind_ms",
    "중단_파고_m": "stop_wave_m",
    "이안_풍속_ms": "unberth_wind_ms",
    "이안_파고_m": "unberth_wave_m",
    "호스분리_풍속_ms": "disconnect_wind_ms",
    "호스분리_파고_m": "disconnect_wave_m",
    "기타조건": "extra_conditions",
}


def load(seed_csv: str = SEED_CSV) -> int:
    df = pd.read_csv(seed_csv, encoding="utf-8-sig")
    df = df.rename(columns=COLUMN_MAP)
    df["collected_at_utc"] = pd.Timestamp.utcnow()

    engine = get_engine()
    n = upsert_dataframe(engine, TABLE_NAME, df, UNIQUE_COLS, auto_create=False)
    print(f"[OK] {os.path.basename(seed_csv)} -> {TABLE_NAME} ({n} rows upsert)")
    return n


if __name__ == "__main__":
    load()
