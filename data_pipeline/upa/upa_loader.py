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
from sqlalchemy import text

from data_pipeline.common_pg_loader import get_engine, load_all as _load_all

# staging 파일명 -> (DB 테이블명, upsert 유니크 키)
#
# 2026-07 키 체계 변경 (이틀에 걸친 수집 시 중복 적재 문제 조치):
# record_uid(전체 행 해시)는 원천 시스템 갱신시각(updated_at_utc, job_at_utc)
# 등이 재수집 때마다 바뀌면 같은 논리 레코드를 새 행으로 다시 쌓는다.
# → upa_config 의 key_cols(자연키) 기준 UPSERT 로 전환: 같은 키는 최신 값으로
#   갱신되고, 새 키만 INSERT 된다. (기존 테이블의 과거 중복은 로더가 유니크
#   인덱스 생성 시점에 최신 1행만 남기고 자동 정리한다)
#
# 예외 — upa_cargo_manifest 는 record_uid 유지: 자연키 후보(bl_no 등)가
# 결측 가능해 유니크 인덱스로 쓸 수 없다. bzentyCd 확보 후 키 확정 예정.
TABLE_MAP = {
    # 2026-08 MMSI-First: callsgn → vessel_uid.
    # callsgn 은 결측 가능해 유니크 인덱스에서 NULL 이 서로 다른 값으로 취급되고,
    # ON CONFLICT 가 걸리지 않아 폴링마다 중복 행이 쌓였다 (실증 확인).
    "upa_vessel_position_stg.csv": ("upa_vessel_position", ["vessel_uid", "received_at_utc"]),
    # 입항 건(port_call_id) 하나에 입항·접안·이안·출항 이벤트가 comm_count 로 나뉘어
    # 여러 행 온다. 키를 port_call_id 만으로 두면 이벤트가 1행으로 합쳐져
    # "언제 접안했고 언제 이안했는지"가 사라진다 → 이벤트 단위 복합키로 보존한다.
    # (선박별 입항 1건만 필요한 소비자는 DISTINCT ON (port_call_id) 로 쓰면 된다)
    "upa_port_call_stg.csv": ("upa_port_call", ["port_call_id", "comm_count"]),
    "upa_cargo_manifest_stg.csv": ("upa_cargo_manifest", ["record_uid"]),
    "upa_berth_facility_stg.csv": ("upa_berth_facility", ["wharf_name"]),
    "upa_anchorage_stg.csv": ("upa_anchorage", ["anchorage_name"]),
}


# upa_cargo_manifest는 전 행 is_synthetic=True — 실제로 존재하는 안정적 개체가
# 아니라 매 실행마다 무작위로 새로 지어내는 "가짜 스냅샷 하나"다(bzentyCd 미확보로
# 실데이터 수집 불가라 gen_cargo_manifest.py가 대체). record_uid가 행 전체 해시라
# 내용이 랜덤으로 바뀔 때마다 "새 행"으로 잡혀, UPSERT를 그대로 쓰면 과거 실행분이
# 안 지워지고 계속 쌓인다(실측: 한 화물명 오탈자 수정 후 재생성했더니 예전 오탈자
# 행 32건이 새 행과 나란히 남아있었음, 2026-08-17). 그래서 이 표만 예외적으로
# "적재 전 전체 교체"로 다룬다 — 실제 API 데이터 표(upa_port_call 등)는 자연키
# UPSERT가 맞으므로 건드리지 않는다.
#
# ★ 알려진 한계(2026-08-17, 의도적으로 안 고침): 아래 TRUNCATE는 이 함수 자체의
# 트랜잭션으로 즉시 커밋되고, 실제 재적재(_load_all)는 그 뒤 별도 트랜잭션에서
# 일어난다 — 그 사이 짧게 이 표가 비어 있는 창이 생긴다(mart.berth_current_cargo가
# 그 순간 조회되면 "인접 화물 없음"으로 잘못 읽힐 수 있음). 완전히 없애려면
# common_pg_loader.upsert_dataframe()이 외부 트랜잭션을 받아써서 TRUNCATE+INSERT를
# 하나로 묶어야 하는데, 그건 다른 7개 테이블이 같이 쓰는 공용 로더라 범위가 커서
# 보류함. gen_cargo_manifest.py는 사람이 수동으로만 실행하고(자동 스케줄 없음,
# run_pipeline.py DOMAINS·upa_scheduler.py 어디에도 없음) 실행 빈도가 낮아
# 지금은 감수하기로 함.
def _replace_cargo_manifest(engine) -> None:
    with engine.begin() as conn:
        # 신규 환경(첫 실행)에서는 아직 테이블이 없을 수 있다 — TRUNCATE는
        # DROP TABLE과 달리 IF EXISTS 구문이 없어 존재 여부를 먼저 확인한다.
        exists = conn.execute(text("SELECT to_regclass('public.upa_cargo_manifest')")).scalar()
        if exists:
            conn.execute(text("TRUNCATE TABLE upa_cargo_manifest"))


def load_all(staging_dir: str = "data/staging") -> None:
    """staging 폴더의 모든 UPA staging CSV 를 PostgreSQL 에 적재."""
    _replace_cargo_manifest(get_engine())
    _load_all(TABLE_MAP, staging_dir=staging_dir)


if __name__ == "__main__":
    load_all()
