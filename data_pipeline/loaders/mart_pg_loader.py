# -*- coding: utf-8 -*-
"""
통합 마트(ulsan_vessel_mart) → PostgreSQL 적재기

create_mart.py 가 생성한 data/mart/ulsan_vessel_mart.csv 를 적재한다.
마트 행은 AIS 위치 스냅샷을 기준으로 제원·PORT-MIS·기상·UPA·MSDS 를
enrich 한 것이므로, ais_vessel_position 과 동일하게 record_uid 해시로
dedup 하며 이력을 누적한다 (같은 내용 재적재 시 무시).

스키마 소유권: 마트는 파이프라인이 산출하는 파생 테이블이므로 backend(Alembic)
소유가 아니라 auto_create=True 로 파이프라인이 직접 생성한다.
(추후 backend 조회 API가 붙으면 Alembic 이관 검토)

실행:
  python -m data_pipeline.loaders.mart_pg_loader
"""
from data_pipeline.common_pg_loader import load_all

MART_DIR = "data/mart"

TABLE_MAP = {
    "ulsan_vessel_mart.csv": ("ulsan_vessel_mart", ["record_uid"]),
}


def load() -> None:
    load_all(TABLE_MAP, staging_dir=MART_DIR, auto_create=True)


if __name__ == "__main__":
    load()
