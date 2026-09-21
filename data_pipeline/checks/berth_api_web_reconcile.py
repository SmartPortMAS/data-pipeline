# -*- coding: utf-8 -*-
"""UPA 공공 API(부두 단위) ↔ UPA 웹(선석 단위) 대사(reconciliation).

왜 필요한가
----------
두 자료는 같은 울산항 시설을 **다른 입도로** 적는다.

  공공 API(`upa_berth_facility`)  69행 / 고유 이름 68개 — 여러 선석을 한 행으로 겹침
  UPA 웹 부두현황                117행                  — 선석을 하나씩 나눔

우리 시드는 웹을 선석으로 쓰고 API 에서 **좌표와 시설코드만** 가져온다. 그
연결이 이름 매칭이므로, 정말로 같은 시설끼리 붙었는지 확인해야 한다. 안 붙거나
잘못 붙으면 좌표가 엉뚱한 부두에 달린다(접안판정이 좌표에 의존한다).

무엇을 보는가
------------
1. 이름 집합 대사 — 한쪽에만 있는 시설
2. 겹친 값의 정체 — API 의 length_m/depth_m/berth_vessel_count 가 웹 선석들의
   합인지 최대인지 대표값인지. 규칙을 모른 채 API 값을 쓰면 의미가 뒤섞인다.
3. 1:N 붕괴 — 웹의 서로 다른 부두가 API 의 한 행에 몰려 있는 경우

실행:
  python -m data_pipeline.checks.berth_api_web_reconcile
"""
from __future__ import annotations

import os
import re
import sys

import pandas as pd

API_STG = "data/staging/upa_berth_facility_stg.csv"
WHARF_CSV = "data/seed/wharf_seed.csv"
BERTH_CSV = "data/seed/berth_seed.csv"
OUT = "data/seed/_api_web_reconcile.csv"

STRIP = re.compile(r"[\s\-_()·\"'“”]")


def norm(s) -> str:
    """이름 대조용. **숫자는 지우지 않는다** — 부두 번호가 의미다."""
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    return STRIP.sub("", str(s)).lower()


def main() -> None:
    for p in (API_STG, WHARF_CSV, BERTH_CSV):
        if not os.path.exists(p):
            sys.exit(f"{p} 없음")

    api = pd.read_csv(API_STG, encoding="utf-8-sig", dtype=str)
    api.columns = [c.strip("﻿") for c in api.columns]
    wharf = pd.read_csv(WHARF_CSV, encoding="utf-8-sig")
    berth = pd.read_csv(BERTH_CSV, encoding="utf-8-sig")
    for c in ("length_m", "depth_m", "berth_vessel_count", "berth_capacity"):
        api[c] = pd.to_numeric(api[c], errors="coerce")

    api["k"] = api.wharf_name.map(norm)
    wharf["k"] = wharf.wharf_name.map(norm)
    berth["k"] = berth.wharf_name.map(norm)

    A, W = set(api.k), set(wharf.k)
    print(f"[IN ] API {len(api)}행 / 고유이름 {api.k.nunique()}  ·  웹 부두 {len(wharf)} / 선석 {len(berth)}")
    print(f"[SET] 교집합 {len(A & W)} · API 에만 {len(A - W)} · 웹에만 {len(W - A)}")

    rows: list[dict] = []

    for k in sorted(A - W):
        for _, r in api[api.k == k].iterrows():
            rows.append({"kind": "API_ONLY", "name": r.wharf_name,
                         "detail": f"{r.fcltCd}/{r.fcltSubCd} · 웹 부두현황에 대응 이름 없음 "
                                   f"(좌표·코드를 붙일 곳이 없다)"})
    for k in sorted(W - A):
        r = wharf[wharf.k == k].iloc[0]
        rows.append({"kind": "WEB_ONLY", "name": r.wharf_name,
                     "detail": f"선석 {r.berth_count}개 · API 에 없어 좌표·시설코드 미확보"})

    # --- 겹친 값의 정체 ----------------------------------------------------
    # API 한 행 vs 그 부두의 웹 선석들. 합/최대/최소 중 무엇과 맞는지 센다.
    tally = {"length_SUM": 0, "length_MAX": 0, "length_NEITHER": 0,
             "depth_MIN": 0, "depth_MAX": 0, "depth_NEITHER": 0,
             "count_EQ": 0, "count_NE": 0}
    for k in sorted(A & W):
        arow = api[api.k == k].iloc[0]
        kids = berth[berth.k == k]
        wrow = wharf[wharf.k == k].iloc[0]

        lens = kids.length_m.dropna()
        if pd.notna(arow.length_m) and len(lens):
            # 허용오차는 build_berth_seed._classify_quay_length 와 맞춘다.
            # API 가 반올림된 총연장을 싣는 경우가 있다(2부두 602 vs 선석합 600).
            if abs(arow.length_m - lens.sum()) < 2.5:
                tally["length_SUM"] += 1
            elif abs(arow.length_m - lens.max()) < 1.5:
                tally["length_MAX"] += 1
            else:
                tally["length_NEITHER"] += 1
                rows.append({"kind": "LEN_MISMATCH", "name": arow.wharf_name,
                             "detail": f"API {arow.length_m:g}m vs 웹 선석 합 {lens.sum():g} / "
                                       f"최대 {lens.max():g} ({len(kids)}선석)"})

        deps = kids.water_depth_m.dropna()
        if pd.notna(arow.depth_m) and len(deps):
            if abs(arow.depth_m - deps.min()) < 0.05:
                tally["depth_MIN"] += 1
            elif abs(arow.depth_m - deps.max()) < 0.05:
                tally["depth_MAX"] += 1
            else:
                tally["depth_NEITHER"] += 1
                # 빌더가 이미 API 쪽(얕은 값)으로 끌어내렸는지 확인한다.
                # 보정된 건을 미해결처럼 보여주면 리포트를 잘못 읽게 된다.
                fixed = (
                    pd.notna(wrow.min_water_depth_m)
                    and abs(float(wrow.min_water_depth_m) - float(arow.depth_m)) < 0.05
                )
                rows.append({"kind": "DEPTH_MISMATCH_FIXED" if fixed else "DEPTH_MISMATCH",
                             "name": arow.wharf_name,
                             "detail": f"API {arow.depth_m:g}m vs 웹 {deps.min():g}~{deps.max():g}m"
                                       + (" — wharf.min_water_depth_m 은 API 값으로 보정됨"
                                          if fixed else " — **미보정**")})

        if pd.notna(arow.berth_vessel_count):
            if int(arow.berth_vessel_count) == int(wrow.berth_count):
                tally["count_EQ"] += 1
            else:
                tally["count_NE"] += 1
                rows.append({"kind": "COUNT_MISMATCH", "name": arow.wharf_name,
                             "detail": f"API 척수 {arow.berth_vessel_count:g} vs 웹 선석수 {wrow.berth_count}"})

    # --- 1:N 붕괴 ----------------------------------------------------------
    # 웹의 서로 다른 부두가 API 한 행에 몰리면 좌표가 한쪽에만 붙는다.
    dup = api[api.k.duplicated(keep=False)]
    for k, g in dup.groupby("k"):
        rows.append({"kind": "API_DUP_ROW", "name": g.iloc[0].wharf_name,
                     "detail": f"API 에 같은 이름 {len(g)}행 — 어느 것이 정본인지 알 수 없다"})

    rep = pd.DataFrame(rows, columns=["kind", "name", "detail"])
    rep.to_csv(OUT, index=False, encoding="utf-8-sig")

    print("\n=== 겹친 값의 정체 (교집합 기준) ===")
    print(f"  안벽길이  선석 합={tally['length_SUM']}  최대={tally['length_MAX']}  둘 다 아님={tally['length_NEITHER']}")
    print(f"  수심      최소={tally['depth_MIN']}  최대={tally['depth_MAX']}  둘 다 아님={tally['depth_NEITHER']}")
    print(f"  척수      웹 선석수와 일치={tally['count_EQ']}  불일치={tally['count_NE']}")
    print(f"\n[OUT] {len(rep)}건 -> {OUT}")
    if len(rep):
        for k, n in rep["kind"].value_counts().items():
            print(f"       {k}: {n}")


if __name__ == "__main__":
    main()
