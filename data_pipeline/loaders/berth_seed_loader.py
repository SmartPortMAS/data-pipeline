# -*- coding: utf-8 -*-
"""wharf/berth 시드 CSV → PostgreSQL 적재.

정적 참조 데이터다. 스케줄러에 걸지 않는다 — 부두 제원은 거의 변하지 않고,
자동 재수집은 검수된 시드를 덮어쓸 위험만 만든다. 시드를 갱신했을 때 사람이
직접 실행한다.

선행 조건: alembic upgrade head (0021 wharf/berth · 0022 portmis_facility_map)

적재 방식은 **전체 교체**다. 시드 파일이 정본이므로, 파일에서 빠진 행은
DB에서도 없어야 한다. 자연키 UPSERT를 쓰면 이름이 바뀐 옛 행이 남아 두 표기가
공존하게 된다(기존 upa_berth_facility가 겪은 문제).

berth 를 먼저 지우고 wharf 를 지운다 — FK 방향 때문이다.

실행:
  python -m data_pipeline.loaders.berth_seed_loader
"""
from __future__ import annotations

import os
import sys

import pandas as pd
from sqlalchemy import text

from data_pipeline.common_pg_loader import get_engine

WHARF_CSV = "data/seed/wharf_seed.csv"
BERTH_CSV = "data/seed/berth_seed.csv"
MAP_CSV = "data/seed/portmis_facility_map.csv"

# 시드 CSV 컬럼 -> DB 컬럼. 시드에만 있고 DB에 없는 진단용 컬럼은 여기서 걸러진다.
WHARF_COLS = [
    "wharf_name", "name_source", "facility_cd", "facility_sub_code", "port_name", "address",
    "built_year", "built_by", "latitude", "longitude", "berth_count",
    "berthing_vessel_count", "min_water_depth_m", "max_water_depth_m",
    "min_capacity_dwt", "max_capacity_dwt", "min_length_m", "max_length_m",
    "total_quay_length_m",
    "operator_count", "spec_spread_flag", "depth_floored",
]
BERTH_COLS = [
    "berth_id", "wharf_name", "berth_no", "berth_name", "ownership_prefix",
    "length_m", "length_basis", "quay_structure", "water_depth_m", "water_depth_is_range",
    "capacity_value", "capacity_unit", "capacity_suspect",
    "unload_value", "unload_unit", "handling_cargo_name", "operator_name",
    "quay_length_raw", "water_depth_raw", "capacity_raw", "unload_raw",
    "port_code", "spec_source", "name_source",
]
MAP_COLS = ["facility_cd", "facility_sub_code", "facility_nm", "wharf_name",
            "berth_id", "match_level", "confidence", "gap_reason"]
INT_COLS = {"built_year", "berth_count", "berthing_vessel_count",
            "operator_count", "berth_no"}


# 앞자리 0이 의미를 갖는 코드 컬럼. dtype 을 지정하지 않으면 pandas 가 '01' 을
# 정수 1 로 읽어 PORT-MIS 의 '01' 과 조인이 빗나간다(실측으로 확인).
STR_COLS = {"facility_cd", "facility_sub_code", "berth_id", "wharf_name"}


def _prepare(path: str, cols: list[str]) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="utf-8-sig",
                     dtype={c: str for c in STR_COLS})
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]
    # pandas 가 결측 때문에 float 으로 읽은 정수 컬럼을 nullable Int64 로 되돌린다
    # (그대로 두면 built_year 가 1996.0 으로 들어간다).
    for c in INT_COLS & set(df.columns):
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    return df.where(pd.notna(df), None)


def load_all() -> None:
    for p in (WHARF_CSV, BERTH_CSV):
        if not os.path.exists(p):
            sys.exit(f"{p} 없음 — 먼저 build_berth_seed 를 실행할 것")

    wharf = _prepare(WHARF_CSV, WHARF_COLS)
    berth = _prepare(BERTH_CSV, BERTH_COLS)
    fmap = _prepare(MAP_CSV, MAP_COLS) if os.path.exists(MAP_CSV) else None
    if fmap is None:
        print(f"[WARN] {MAP_CSV} 없음 — 감사 뷰가 배정과 연결되지 않는다")

    # 시드가 서로 어긋난 채 적재되면 FK 위반으로 중간에 터진다. 미리 막는다.
    orphans = set(berth["wharf_name"].dropna()) - set(wharf["wharf_name"].dropna())
    if orphans:
        sys.exit(f"berth 의 wharf_name 중 wharf 에 없는 값: {sorted(orphans)}")

    engine = get_engine()
    with engine.begin() as conn:
        exists = conn.execute(text("SELECT to_regclass('public.wharf')")).scalar()
        if not exists:
            sys.exit("wharf 테이블 없음 — alembic upgrade head 를 먼저 실행할 것")
        # TRUNCATE + INSERT 를 한 트랜잭션으로 묶는다. 중간에 빈 창이 생기면
        # 그 순간 감사 뷰가 '제원 없음'으로 잘못 읽힌다.
        conn.execute(text("TRUNCATE TABLE portmis_facility_map, berth, wharf"))
        wharf.to_sql("wharf", conn, if_exists="append", index=False)
        berth.to_sql("berth", conn, if_exists="append", index=False)
        if fmap is not None:
            # 매핑이 가리키는 선석이 시드에 없으면 FK 로 터진다 — 미리 거른다.
            unknown_w = set(fmap["wharf_name"].dropna()) - set(wharf["wharf_name"].dropna())
            if unknown_w:
                sys.exit(f"매핑의 wharf_name 중 wharf 에 없는 값: {sorted(unknown_w)}")
            known = set(berth["berth_id"].dropna())
            bad = set(fmap["berth_id"].dropna()) - known
            if bad:
                sys.exit(f"매핑의 berth_id 중 berth 에 없는 값: {sorted(bad)}")
            fmap.to_sql("portmis_facility_map", conn, if_exists="append", index=False)

    n_map = 0 if fmap is None else len(fmap)
    print(f"[DONE] wharf {len(wharf)}행 · berth {len(berth)}행 · 매핑 {n_map}행 적재")


if __name__ == "__main__":
    load_all()
