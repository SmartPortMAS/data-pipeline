# -*- coding: utf-8 -*-
"""portmis_vessel 의 계선시설 서브코드를 raw JSON 에서 채운다 (1회성 백필).

왜 필요한가
----------
`arrival_facility_sub_code` / `departure_facility_sub_code` 는 마이그레이션 0019
(2026-09-13)로 **뒤늦게** 추가된 컬럼이다. 그 전에 적재된 행은 값이 NULL 인데,
정박지·선석 감사가 이 코드로 조인하므로 NULL 이면 판정이 전부 UNKNOWN 이 된다
(실측: 363행 전부 NULL → mart.berth_audit 조인 0건).

전처리기를 다시 돌리는 방법은 쓸 수 없다. `preprocess_portmis()` 는 **오늘 이후
입항만** 남기는 증분 파이프라인이라(실측: '입항일 필터 704 -> 0행') 과거 raw 를
다시 넣으면 0행이 된다. 그건 버그가 아니라 설계다.

그래서 raw JSON 에서 자연키로 직접 UPDATE 한다. 새 컬럼이 추가됐을 때의 통상적인
백필이며, 앞으로 수집되는 건은 전처리기가 정상적으로 채운다.

실행 (1회):
  python -m data_pipeline.checks.backfill_portmis_subcode
"""
from __future__ import annotations

import glob
import json
import os

from sqlalchemy import create_engine, text

RAW_GLOB = "data/raw/portmis/*.json"


def collect_from_raw() -> dict[tuple[str, str, str], dict]:
    """자연키 -> 서브코드. 같은 항차가 여러 파일·신고차수로 오므로 마지막 값을 쓴다."""
    out: dict[tuple[str, str, str], dict] = {}

    def walk(o):
        if isinstance(o, list):
            for x in o:
                walk(x)
        elif isinstance(o, dict):
            if o.get("clsgn") and o.get("etryptYear") and o.get("etryptCo") is not None:
                key = (str(o["clsgn"]), str(o["etryptYear"]), str(o["etryptCo"]))
                cur = out.setdefault(key, {})
                for pre in ("arrival", "departure"):
                    cd = o.get(f"{pre}_laidupFcltyCd")
                    sub = o.get(f"{pre}_laidupFcltySubCd")
                    if cd and sub not in (None, ""):
                        # cd 와 sub 는 **같은 레코드에서 짝으로** 가져온다.
                        cur[f"{pre}_cd"] = str(cd)
                        cur[f"{pre}_facility_sub_code"] = str(sub)
                v = o.get("arrival_reqstSeNm")
                if v:
                    cur["arrival_report_type"] = str(v)
            for v in o.values():
                walk(v)

    for f in sorted(glob.glob(RAW_GLOB)):
        with open(f, encoding="utf-8") as fh:
            walk(json.load(fh))
    return out


def main() -> None:
    data = collect_from_raw()
    print(f"[IN ] raw 에서 자연키 {len(data)}건 수집")
    engine = create_engine(os.environ["DATABASE_URL"])
    # ★ 서브코드는 **시설코드가 일치할 때만** 쓴다.
    #
    #   자연키만 보고 넣으면 안 된다. 같은 항차라도 신고 차수(최초/변경/최종)마다
    #   배정 시설이 바뀔 수 있는데, DB 행의 cd 는 옛 적재분이고 raw 에서 고른 sub 는
    #   최신분이면 **둘이 어긋난 조합**이 만들어진다.
    #   실측: 그렇게 넣었더니 DB 에 WAE/11 · WAE/32 가 생겼다. raw 의 WAE 는
    #   01·02·03 뿐이고 11 은 MBU(SK1부두), 32 는 다른 시설의 서브코드였다.
    #   존재하지 않는 조합이라 조인이 빗나가고, 더 나쁘게는 **엉뚱한 부두에
    #   붙을 수도** 있다.
    #
    #   그래서 cd 가 같은 경우에만 채우고, 어긋나면 NULL 로 둔다(UNKNOWN 이 정직하다).
    stmt = text("""
        UPDATE portmis_vessel SET
            arrival_facility_sub_code = CASE
                WHEN :acd IS NOT NULL AND arrival_facility_cd = :acd
                    THEN COALESCE(:a, arrival_facility_sub_code)
                ELSE arrival_facility_sub_code END,
            departure_facility_sub_code = CASE
                WHEN :dcd IS NOT NULL AND departure_facility_cd = :dcd
                    THEN COALESCE(:d, departure_facility_sub_code)
                ELSE departure_facility_sub_code END,
            arrival_report_type = COALESCE(:r, arrival_report_type)
        WHERE callsgn = :cs AND entry_year = :yr AND entry_count = :cnt
    """)
    updated = 0
    with engine.begin() as conn:
        for (cs, yr, cnt), v in data.items():
            res = conn.execute(stmt, {
                "cs": cs, "yr": int(yr), "cnt": int(cnt),
                "a": v.get("arrival_facility_sub_code"),
                "acd": v.get("arrival_cd"),
                "d": v.get("departure_facility_sub_code"),
                "dcd": v.get("departure_cd"),
                "r": v.get("arrival_report_type"),
            })
            updated += res.rowcount or 0
        left = conn.execute(text(
            "SELECT count(*) FROM portmis_vessel "
            "WHERE arrival_facility_cd IS NOT NULL AND arrival_facility_sub_code IS NULL"
        )).scalar()
    print(f"[OUT] {updated}행 갱신 · 아직 서브코드 없는 행 {left}")


if __name__ == "__main__":
    main()
