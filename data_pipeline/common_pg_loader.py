# -*- coding: utf-8 -*-
"""
staging CSV → PostgreSQL 공통 적재 로직 (정본)

upa/upa_loader.py 에서 쓰던 엔진 생성/자동 테이블 생성/UPSERT 로직을 도메인 무관하게
뽑아낸 모듈. AIS/PORTMIS/tide/wave/weather/UPA 로더가 모두 이 함수들을 공유한다.

인프라 독립적: APScheduler/Lambda/수동 실행 어디서든 함수 호출만 하면 됨.

스키마 소유권 (auto_create 플래그):
  - auto_create=True  (기본값, UPA 로더): 테이블이 없으면 DataFrame 스키마로 자동 생성.
    data-pipeline이 스키마를 직접 관리하는 레거시 방식 — 타입 추론이 부정확할 수 있고
    마이그레이션 이력이 안 남는 단점이 있다. 새 도메인에는 권장하지 않는다.
  - auto_create=False (AIS/PORTMIS/tide/wave/weather/MSDS): 테이블은 backend(Alembic)가
    소유한다. 이 모듈은 insert(upsert)만 수행하고, 테이블이 없으면 명확한 에러를 낸다
    (먼저 backend에서 `alembic upgrade head` 실행 필요).

준비:
  pip install sqlalchemy psycopg2-binary pandas
  환경변수로 접속정보 지정 (둘 중 하나, .env 파일 컨벤션과 동일). 기본값 없음 —
  아무것도 설정 안 하면 에러를 낸다 (하드코딩된 자격증명 fallback으로 조용히
  엉뚱한 DB에 붙는 걸 방지):
    1) DATABASE_URL=postgresql+psycopg2://user:pass@host:5432/dbname
    2) POSTGRES_HOST / POSTGRES_PORT / POSTGRES_USER / POSTGRES_PASSWORD / POSTGRES_DB

Upsert 키 지정 방식 (TABLE_MAP 의 unique_cols):
  - ["record_uid"] : 메타 컬럼을 제외한 전체 행 내용 해시로 dedup.
    같은 내용이 재수집되면 무시되고, 내용이 바뀌면 새 행이 쌓인다 (이력 누적용).
    예: AIS 선박위치처럼 시계열 스냅샷을 계속 쌓아야 하는 테이블.
  - [실제 컬럼, ...] : 그 컬럼(들)을 유니크 키로 삼아 ON CONFLICT UPDATE.
    같은 키의 행은 항상 최신 값으로 덮어써진다 (최신 상태 유지용).
    예: PORTMIS 입출항 건(callsgn+entry_year+entry_count), 관측소 시계열
    (station_id+observed_at_utc), 선박 정적 정보 최신값(mmsi).
"""
import glob
import hashlib
import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine, make_url

load_dotenv()

STAGING_DIR = "data/staging"

# 해시(record_uid)에서 제외할 컬럼 (실행마다/재수집마다 바뀌는 값)
# - collected_at_utc: 우리 수집 시각
# - updated_at_utc / job_at_utc: 원천 시스템(관제 등)의 갱신·배치 시각.
#   내용이 같아도 재수집 때마다 바뀌므로 해시에 넣으면 같은 논리 레코드가
#   이틀에 걸친 수집에서 다른 record_uid 로 중복 적재된다.
VOLATILE_COLS = ["collected_at_utc", "updated_at_utc", "job_at_utc"]


def get_engine() -> Engine:
    """환경변수에서 PostgreSQL 접속 엔진 생성. 기본값 없음 — 미설정 시 명확히 에러."""
    return create_engine(_database_url())


def pg_conninfo() -> str:
    """psycopg2.connect() 에 넘길 접속 문자열. get_engine() 과 같은 규칙으로 만든다.

    psycopg2 를 직접 쓰는 스크립트들이 예전엔 각자 POSTGRES_* 를 읽으면서
    localhost:5433 · 비밀번호까지 기본값으로 박아 두었다. 그러면 운영 서버의 .env 에
    값이 빠졌을 때 에러 대신 **조용히 엉뚱한 DB 로** 붙는다. 규칙을 여기 하나로 모은다.
    """
    return make_url(_database_url()).set(drivername="postgresql").render_as_string(hide_password=False)


def _database_url() -> str:
    """DATABASE_URL 우선, 없으면 POSTGRES_* 5개로 조립. 하나라도 없으면 에러."""
    url = os.getenv("DATABASE_URL")
    if not url:
        required = ["POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB"]
        missing = [k for k in required if not os.getenv(k)]
        if missing:
            raise RuntimeError(
                f"DB 접속정보가 없습니다. DATABASE_URL 또는 {', '.join(missing)}를 .env에 설정하세요."
            )
        host = os.environ["POSTGRES_HOST"]
        port = os.environ["POSTGRES_PORT"]
        user = os.environ["POSTGRES_USER"]
        pw = os.environ["POSTGRES_PASSWORD"]
        db = os.environ["POSTGRES_DB"]
        url = f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}"
    return url


def add_record_uid(df: pd.DataFrame) -> pd.DataFrame:
    """변동 컬럼을 제외한 전체 컬럼으로 행 고유 해시(record_uid)를 만든다."""
    df = df.copy()
    cols = [c for c in df.columns if c not in VOLATILE_COLS and c != "record_uid"]
    joined = df[cols].astype(str).fillna("").agg("|".join, axis=1)
    df["record_uid"] = joined.map(lambda x: hashlib.md5(x.encode("utf-8")).hexdigest())
    # 같은 배치 안의 완전 중복 제거
    df = df.drop_duplicates(subset=["record_uid"], keep="last").reset_index(drop=True)
    return df


def _ensure_table(engine: Engine, table: str, df: pd.DataFrame, unique_cols: list, auto_create: bool) -> None:
    """테이블 존재를 보장한다.

    auto_create=True 면 없을 때 DataFrame 스키마로 생성 + 유니크 인덱스 생성(레거시 방식).
    auto_create=False 면 테이블이 backend(Alembic) 소유이므로 생성하지 않고,
    없으면 마이그레이션을 먼저 적용하라는 에러를 낸다.
    """
    insp = inspect(engine)
    if insp.has_table(table):
        return

    if not auto_create:
        raise RuntimeError(
            f'테이블 "{table}"이 존재하지 않습니다. 이 테이블은 backend(Alembic)가 소유합니다. '
            f"backend에서 `alembic upgrade head`를 먼저 실행하세요."
        )

    df.head(0).to_sql(table, engine, index=False)
    col_list = ", ".join(f'"{c}"' for c in unique_cols)
    with engine.begin() as conn:
        conn.execute(text(
            f'CREATE UNIQUE INDEX IF NOT EXISTS "{table}_uidx" '
            f'ON "{table}" ({col_list})'
        ))
    print(f"  - 테이블 생성: {table} (유니크 키: {unique_cols})")


def _ensure_unique_index(engine: Engine, table: str, unique_cols: list) -> None:
    """기존 테이블에 unique_cols 를 커버하는 유니크 인덱스를 보장한다.

    ON CONFLICT (unique_cols) 는 해당 컬럼 조합의 유니크 인덱스가 있어야 동작한다.
    TABLE_MAP 의 키를 record_uid → 자연키로 바꾸는 등 키 체계가 변경되면
    기존 테이블에는 새 키의 인덱스가 없으므로 여기서 만들어 준다.
    인덱스 생성 전, 키 변경 이전에 쌓인 중복 행을 정리한다
    (같은 키 중 collected_at_utc 최신 1행만 유지 — 재수집 중복 정리와 동일 의미).
    """
    insp = inspect(engine)
    target = set(unique_cols)
    for idx in insp.get_indexes(table):
        if idx.get("unique") and set(idx.get("column_names") or []) == target:
            return
    pk = insp.get_pk_constraint(table)
    if pk and set(pk.get("constrained_columns") or []) == target:
        return

    col_list = ", ".join(f'"{c}"' for c in unique_cols)
    has_collected = any(c["name"] == "collected_at_utc" for c in insp.get_columns(table))
    order_by = 'ORDER BY collected_at_utc DESC NULLS LAST, ctid DESC' if has_collected else 'ORDER BY ctid DESC'
    idx_name = f"{table}_uidx__" + "__".join(unique_cols)
    with engine.begin() as conn:
        dedup_sql = (
            f'DELETE FROM "{table}" d USING ('
            f'  SELECT ctid, row_number() OVER (PARTITION BY {col_list} {order_by}) AS rn'
            f'  FROM "{table}"'
            f') r WHERE d.ctid = r.ctid AND r.rn > 1;'
        )
        deleted = conn.execute(text(dedup_sql)).rowcount
        conn.execute(text(
            f'CREATE UNIQUE INDEX IF NOT EXISTS "{idx_name}" ON "{table}" ({col_list})'
        ))
    if deleted:
        print(f"  - 키 체계 변경: {table} 기존 중복 {deleted}행 정리 후 유니크 인덱스 생성 ({unique_cols})")
    else:
        print(f"  - 유니크 인덱스 생성: {table} ({unique_cols})")


def upsert_dataframe(
    engine: Engine, table: str, df: pd.DataFrame, unique_cols: list, auto_create: bool = True,
) -> int:
    """임시 테이블 경유 UPSERT (ON CONFLICT unique_cols)."""
    if df.empty:
        return 0

    # 배치 내 키 중복 제거 — 같은 INSERT 문 안에 동일 유니크 키가 두 번 들어가면
    # PostgreSQL 이 CardinalityViolation("cannot affect row a second time")을 낸다.
    #
    # 이 방어는 원래 load_csv() 에만 있었다. 그런데 upsert_dataframe() 은
    # 공개 함수라 CSV 를 거치지 않는 호출자(전처리 결과를 DataFrame 째로 넣는
    # 코드, 테스트, 마트 적재기)가 직접 부른다. 그 경로는 무방비였다.
    # 방어는 우회 가능한 상위가 아니라 실제로 SQL 을 만드는 이 계층에 있어야 한다.
    # (load_csv 의 기존 dedup 은 그대로 두어도 무해하다 — 여기서 한 번 더 걸린다)
    if unique_cols and all(c in df.columns for c in unique_cols):
        before = len(df)
        df = df.drop_duplicates(subset=unique_cols, keep="last").reset_index(drop=True)
        if len(df) < before:
            print(f"  - 배치 내 키 중복 {before - len(df)}행 제거 ({table}, 키: {unique_cols})")

    _ensure_table(engine, table, df, unique_cols, auto_create)
    _ensure_unique_index(engine, table, unique_cols)

    cols = list(df.columns)
    tmp = f"_tmp_{table}"
    df.to_sql(tmp, engine, if_exists="replace", index=False)

    collist = ", ".join(f'"{c}"' for c in cols)
    conflict_cols = ", ".join(f'"{c}"' for c in unique_cols)
    updates = ", ".join(f'"{c}"=EXCLUDED."{c}"' for c in cols if c not in unique_cols)
    sql = (
        f'INSERT INTO "{table}" ({collist}) '
        f'SELECT {collist} FROM "{tmp}" '
        f'ON CONFLICT ({conflict_cols}) DO UPDATE SET {updates};'
    )
    with engine.begin() as conn:
        conn.execute(text(sql))
        conn.execute(text(f'DROP TABLE IF EXISTS "{tmp}";'))
    return len(df)


# ---------------------------------------------------------------------------
# 앞자리 0 이 의미를 갖는 식별자 컬럼 — 반드시 문자열로 읽는다.
#
# [2026-09-21] pandas 는 '001128' 을 정수 1128 로 추론한다. 그대로 적재하면
# MSDS chem_id 조인이 통째로 빈다(실측: mart.cargo_msds 의 msds_matched 757행 중
# 0행). 값이 사라진 게 아니라 '앞자리 0 이 사라져' 안 맞는 거라, NULL 검사로는
# 안 잡히고 조인 결과만 조용히 0 이 된다 — 가장 찾기 어려운 종류의 결함이다.
#
# gen_cargo_manifest 가 dg_un_no 를 문자열로 강제하는 것과 같은 이유이고
# (그쪽 주석 참고), 방어는 CSV 를 만드는 쪽이 아니라 **읽는 이 계층**에도
# 있어야 한다. 쓰는 쪽이 여럿이기 때문이다.
#
# 여기 없는 컬럼이 같은 함정에 빠지면 증상이 똑같다 — 새 코드성 컬럼을 추가할 때
# 앞자리 0 이 있을 수 있으면 이 목록에 먼저 넣을 것.
#
# [2026-09-22] 접미사로 맞춘다 — 정확히 일치시키다 실제로 뚫렸다.
#   PORT-MIS 는 같은 코드를 입·출항으로 나눠 보내서 컬럼명이
#   `arrival_facility_sub_code` · `departure_facility_sub_code` 다. 목록에는
#   접두사 없는 `facility_sub_code` 만 있었고, pandas dtype 은 키가 정확히
#   같아야 적용되므로 두 컬럼 다 보호 밖이었다.
#
#   결과(실측 2026-09-22, 현재 staging CSV):
#       arrival_facility_sub_code    int64    [1, 3, 2, 12, 32, 5]     ← '01' 의 0 이 날아감
#       departure_facility_sub_code  float64  [2.0, 1.0, 12.0, ...]    ← NaN 이 섞여 실수화
#   원천 raw 는 줄곧 '01'·'02'·'12' 로 0 패딩해서 보낸다(raw JSON 확인). 즉
#   손상은 전적으로 이 지점에서 생겼고, DB 에도 9/20 이후 수집분 182건이 비패딩으로
#   들어가 portmis_facility_map(전부 0패딩) 과 조인이 끊겼다.
#
#   접미사 매칭이면 앞으로 붙을 arrival_/departure_/prev_ 같은 접두사 변형이
#   자동으로 덮인다. 부분 문자열이 아니라 접미사인 이유는 `mmsi` 가 우연히 들어간
#   다른 이름(예: `mmsi_source`)까지 문자열로 굳히지 않기 위해서다.
# ---------------------------------------------------------------------------
TEXT_ID_COLUMNS = (
    "chem_id",          # MSDS 물질 ID — 6자리 제로패딩 (예: 001128)
    "cas_no",           # CAS 번호 — 하이픈 포함이지만 방어적으로 고정
    "dg_un_no",         # UN 번호 — 4자리
    "callsgn",          # 호출부호 — 숫자만인 국내선이 있다 (예: 010511)
    "facility_code", "facility_cd", "facility_sub_code",
    "port_code", "station_id",
    # ※ mmsi 는 일부러 넣지 않는다. 여기 있었는데 upa_vessel_position.mmsi 가
    #   bigint 라, 문자열로 굳힌 임시표를 INSERT ... SELECT 하면서
    #   "column mmsi is of type bigint but expression is of type text" 로 깨졌다.
    #   실측: 2026-09-21 20:06 부터 [vessel] 도메인이 5분마다 66회 연속 실패 —
    #   선박위치가 이 프로젝트에서 가장 실시간성이 중요한 피드인데 10시간 멈췄다.
    #   MMSI 는 MID 가 2~7 로 시작해 앞자리 0 이 없고, 저장 타입도 정수라 애초에
    #   0 패딩을 보존할 수 없다 — 이 목록에 있을 이유가 없다.
)


def _text_dtypes(csv_path: str) -> dict:
    """헤더를 먼저 읽어, TEXT_ID_COLUMNS 로 끝나는 컬럼을 전부 문자열로 지정한다."""
    header = pd.read_csv(csv_path, nrows=0).columns
    return {c: str for c in header
            if any(c == t or c.endswith("_" + t) for t in TEXT_ID_COLUMNS)}


def load_csv(
    engine: Engine, csv_path: str, table: str, unique_cols: list,
    auto_create: bool = True, row_filter=None,
) -> int:
    df = pd.read_csv(csv_path, dtype=_text_dtypes(csv_path))
    if df.empty:
        print(f"[SKIP] {csv_path} empty")
        return 0
    # 적재 직전 행 필터 — 원천(수집기)이 이미 고쳐졌지만 배포가 늦어 옛 산출물이
    # 계속 내려오는 기간에, 잘못된 행이 DB로 재유입되는 것을 막는 방어선.
    if row_filter is not None:
        df = row_filter(df)
        if df.empty:
            print(f"[SKIP] {csv_path} 필터 후 0행")
            return 0
    for col in df.columns:
        if col.endswith("_utc"):
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
    if unique_cols == ["record_uid"]:
        df = add_record_uid(df)
    else:
        # batch dedup: same unique key twice in one INSERT
        # causes ON CONFLICT DO UPDATE CardinalityViolation
        df = df.drop_duplicates(subset=unique_cols, keep="last").reset_index(drop=True)
    n = upsert_dataframe(engine, table, df, unique_cols, auto_create)
    print(f"[OK] {os.path.basename(csv_path)} -> {table} ({n} rows upsert)")
    return n


def load_all(
    table_map: dict, staging_dir: str = STAGING_DIR, auto_create: bool = True, row_filter=None,
) -> None:
    """staging 폴더의 CSV 를 table_map 에 맞춰 PostgreSQL 에 적재한다.

    Args:
        table_map: {csv 파일명: (테이블명, unique_cols)}
        staging_dir: staging CSV 디렉터리.
        auto_create: True면 테이블 없을 때 DataFrame 스키마로 자동 생성(레거시).
            False면 backend(Alembic) 소유 테이블이 이미 존재한다고 가정하고 insert만 한다.
    """
    engine = get_engine()
    total = 0
    for csv_path in sorted(glob.glob(os.path.join(staging_dir, "*.csv"))):
        fname = os.path.basename(csv_path)
        entry = table_map.get(fname)
        if not entry:
            continue  # table_map 에 없는 파일은 건너뜀
        table, unique_cols = entry
        total += load_csv(engine, csv_path, table, unique_cols, auto_create, row_filter=row_filter)
    print(f"[DONE] 총 {total} rows 적재 완료")
