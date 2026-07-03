# -*- coding: utf-8 -*-
"""
울산항만공사(UPA) staging → PostgreSQL 적재기 (담당: 함현우)

전처리 결과(data/staging/*.csv)를 PostgreSQL(RDS 등)에 적재한다.
- 인프라 독립적: APScheduler/Lambda/수동 실행 어디서든 함수 호출만 하면 됨
- 멱등(idempotent) 적재: 같은 데이터를 다시 넣어도 중복이 쌓이지 않음
  (메타 컬럼 collected_at_utc 를 제외한 모든 컬럼으로 record_uid 해시를 만들고,
   record_uid 기준 UPSERT → 동일 행은 갱신, 새 행만 INSERT)

엔진 생성/자동 테이블 생성/UPSERT 로직은 common_pg_loader.py 로 공통화되어
AIS/PORTMIS/tide/wave/weather 로더와 공유한다 (data_pipeline/loaders/*_pg_loader.py).

준비:
  pip install sqlalchemy psycopg2-binary pandas
  환경변수로 접속정보 지정 (둘 중 하나):
    1) DATABASE_URL=postgresql+psycopg2://user:pass@host:5432/dbname
    2) PG_HOST / PG_PORT / PG_USER / PG_PASSWORD / PG_DBNAME

실행:
  python -m data_pipeline.upa.upa_loader
"""
from data_pipeline.common_pg_loader import load_all as _load_all

# staging 파일명 -> (DB 테이블명, upsert 유니크 키)
# UPA 6개 테이블은 전부 record_uid(전체 행 해시) 방식으로 이력 없이 dedup 적재한다.
TABLE_MAP = {
    "upa_vessel_position_stg.csv": ("upa_vessel_position", ["record_uid"]),
    "upa_port_call_stg.csv": ("upa_port_call", ["record_uid"]),
    "upa_cargo_manifest_stg.csv": ("upa_cargo_manifest", ["record_uid"]),
    "upa_unload_record_stg.csv": ("upa_unload_record", ["record_uid"]),
    "upa_berth_facility_stg.csv": ("upa_berth_facility", ["record_uid"]),
    "upa_anchorage_stg.csv": ("upa_anchorage", ["record_uid"]),
}


def load_all(staging_dir: str = "data/staging") -> None:
    """staging 폴더의 모든 UPA staging CSV 를 PostgreSQL 에 적재."""
    _load_all(TABLE_MAP, staging_dir=staging_dir)


if __name__ == "__main__":
    load_all()
