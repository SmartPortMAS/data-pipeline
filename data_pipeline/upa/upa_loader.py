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
#
# 2026-07 키 체계 변경 (이틀에 걸친 수집 시 중복 적재 문제 조치):
# record_uid(전체 행 해시)는 원천 시스템 갱신시각(updated_at_utc, job_at_utc)
# 등이 재수집 때마다 바뀌면 같은 논리 레코드를 새 행으로 다시 쌓는다.
# → upa_config 의 key_cols(자연키) 기준 UPSERT 로 전환: 같은 키는 최신 값으로
#   갱신되고, 새 키만 INSERT 된다. (기존 테이블의 과거 중복은 로더가 유니크
#   인덱스 생성 시점에 최신 1행만 남기고 자동 정리한다)
#
# upa_cargo_manifest 는 bl_no 키(2026-09-26, backend alembic 0031). 합성 생성기 v4 가
# bl_no 를 입항 건 키로 결정적으로 만든다(SYN-{콜사인}_{연도}_{횟수}-{순번}).
TABLE_MAP = {
    # 2026-08 MMSI-First: callsgn → vessel_uid.
    # callsgn 은 결측 가능해 유니크 인덱스에서 NULL 이 서로 다른 값으로 취급되고,
    # ON CONFLICT 가 걸리지 않아 폴링마다 중복 행이 쌓였다 (실증 확인).
    #
    # 2026-09-13: received_at_utc 를 키에서 뺐다 — 선박당 최신 1행만 유지한다.
    # 이 테이블의 이력을 읽는 코드가 없고(소비처는 전부 최신 1행만 꺼내 쓴다),
    # 접안 판정도 '정박(계류) + 선석 400m 이내' 순간 판정으로 성립한다.
    # 이력 축적은 물리 마트(ulsan_vessel_mart)가 맡는다. 상세 근거는
    # backend/alembic/versions/0018_vessel_position_latest_only.py 참고.
    "upa_vessel_position_stg.csv": ("upa_vessel_position", ["vessel_uid"]),
    # 입항 건(port_call_id) 하나에 입항·접안·이안·출항 이벤트가 comm_count 로 나뉘어
    # 여러 행 온다. 키를 port_call_id 만으로 두면 이벤트가 1행으로 합쳐져
    # "언제 접안했고 언제 이안했는지"가 사라진다 → 이벤트 단위 복합키로 보존한다.
    # (선박별 입항 1건만 필요한 소비자는 DISTINCT ON (port_call_id) 로 쓰면 된다)
    "upa_port_call_stg.csv": ("upa_port_call", ["port_call_id", "comm_count"]),
    "upa_cargo_manifest_stg.csv": ("upa_cargo_manifest", ["bl_no"]),
    "upa_berth_facility_stg.csv": ("upa_berth_facility", ["wharf_name"]),
    # (2026-09-20) anchorage_name -> (facility_code, index_no).
    # 정박지 하나가 폴리곤 정점 여러 행으로 오는데(E3 는 41행) 이름을 키로 두면
    # 1행만 남는다. 그 결과 (a) berth_neo4j_loader 의 centroid 계산이 정점 1개로
    # 무너지고 (b) 같은 이름의 TEXT 행(remark 없음)이 POLYGON 행(제한 있음)을
    # 덮어써 **톤급 제한이 사라진다** — 실측상 E3·M1~M7 8곳이 그랬다.
    # 상세 근거는 upa_config.ANCHRG_INFO 주석 참고.
    "upa_anchorage_stg.csv": ("upa_anchorage", ["facility_code", "index_no"]),
}


def load_all(staging_dir: str = "data/staging") -> None:
    """staging 폴더의 모든 UPA staging CSV 를 PostgreSQL 에 적재."""
    # (2026-09-24) UPA 표 5종은 backend Alembic 0028 이 만든다 — 여기서는 적재만 한다.
    # 표가 없으면 "backend 에서 alembic upgrade head 먼저" 오류가 난다. 컬럼을 늘리거나
    # 바꿀 때는 backend 마이그레이션이 먼저다(CSV 에만 새 컬럼이 있으면 INSERT 가 실패한다).
    _load_all(TABLE_MAP, staging_dir=staging_dir, auto_create=False)


if __name__ == "__main__":
    load_all()
