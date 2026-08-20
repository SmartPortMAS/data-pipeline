# -*- coding: utf-8 -*-
"""
PORT-MIS staging → PostgreSQL 적재기

자연키(callsgn + entry_year + entry_count)로 upsert한다 — 같은 입출항 건이 필드
갱신을 동반해 재수집되어도 새 행으로 쌓이지 않고 최신 상태로 덮어써진다.

테이블은 backend(Alembic)가 소유한다. 이 모듈은 insert만 한다 — 먼저
backend에서 `alembic upgrade head`를 실행해 두어야 한다.

실행:
  python -m data_pipeline.loaders.portmis_pg_loader
"""
from data_pipeline.common_pg_loader import load_all

TABLE_MAP = {
    "portmis_vessel_stg.csv": ("portmis_vessel", ["callsgn", "entry_year", "entry_count"]),
}


def _drop_invalid_port_code(df):
    """prtAgCd=300 행 제거 — PORT-MIS 에 온산항 전용 항만청 코드는 실재하지 않는다.

    수집기는 이미 300 조회를 없앴지만(b79665c), 그 코드가 EC2 에 배포되기 전까지는
    S3 staging 에 300 으로 라벨된 행(실제로는 대산항 배)이 계속 내려온다. 여기서
    걸러야 매시간 cloud_pull 이 로컬 DB 를 재오염시키지 않는다(2026-08-20 실측:
    대산 185건이 '온산항' 라벨로 적재돼 있었다). 재배포 후에는 걸릴 행이 없어
    무해하며, 300 은 애초에 유효한 코드가 아니므로 영구히 두어도 안전하다.
    """
    if "port_agency_cd" not in df.columns:
        return df
    bad = df["port_agency_cd"].astype(str).str.strip() == "300"
    if bad.any():
        print(f"  [필터] prtAgCd=300 (실재하지 않는 코드, 대산항 오염) {int(bad.sum())}행 제외")
    return df[~bad].reset_index(drop=True)


def load() -> None:
    load_all(TABLE_MAP, auto_create=False, row_filter=_drop_invalid_port_code)


if __name__ == "__main__":
    load()
