# -*- coding: utf-8 -*-
"""선석 제원 두 출처 대조 — 해수청 시드 vs UPA 웹.

두 정부 출처가 같은 부두를 다르게 적고 있다.

  data/seed/ulsan_berth_spec_seed.csv  울산지방해양수산청 「울산항시설현황」 (35부두)
  data/seed/wharf_seed.csv             UPA 웹 부두현황에서 집계 (64부두)

어느 쪽을 정본으로 삼을지 고르기 전에, **실제로 몇 건이 얼마나 어긋나는지**를
먼저 본다. 차이가 작으면 고민할 것이 없고, 크면 근거를 보고 정할 수 있다.

수심은 양쪽 모두 "가장 얕은 선석" 기준으로 맞춰 비교한다
(해수청 depth_min_m ↔ UPA min_water_depth_m).

접안능력은 해수청이 max_dwt(부두 최대)라 UPA 쪽도 max_capacity_dwt와 비교한다.
단, 감사에 실제로 쓰는 값은 MIN이므로 이 비교는 "출처가 서로 맞는가"만 본다.

실행:
  python -m data_pipeline.checks.berth_spec_crosscheck
"""
from __future__ import annotations

import os
import re
import sys

import pandas as pd

HAESUCHEONG = "data/seed/ulsan_berth_spec_seed.csv"
UPA_WHARF = "data/seed/wharf_seed.csv"
OUT = "data/seed/_spec_crosscheck.csv"

# 표기가 다른 같은 부두. 자동 규칙을 늘리는 대신 사전으로 관리한다
# (규칙을 늘리면 다른 부두까지 잘못 묶기 시작한다).
ALIAS = {
    "정일스톨트헤븐신항3~5부두": None,      # UPA는 3·4·5부두로 분리 — 1:N이라 비교 보류
    "ls니꼬신항부두": "lsmnm신항부두",
    "현대오일터미널신항부두": None,          # UPA는 신항1·2부두로 분리
    "soil부이": "soil신부이",
    "석유공사부이": "한국석유공사부이",
}


def norm(s) -> str:
    """이름 대조용. 숫자는 지우지 않는다 — 부두 번호가 의미다."""
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    return re.sub(r"[\s\-_()·\"']", "", str(s)).lower()


def _diff(a, b) -> tuple[float | None, str]:
    """차이와 판정. 양쪽 다 있어야 비교한다."""
    if pd.isna(a) or pd.isna(b):
        return None, "ONE_SIDE_ONLY"
    d = float(a) - float(b)
    if abs(d) < 1e-9:
        return 0.0, "SAME"
    return d, "DIFF"


def main() -> None:
    for p in (HAESUCHEONG, UPA_WHARF):
        if not os.path.exists(p):
            sys.exit(f"{p} 없음")

    hs = pd.read_csv(HAESUCHEONG, encoding="utf-8-sig")
    up = pd.read_csv(UPA_WHARF, encoding="utf-8-sig")
    print(f"[IN ] 해수청 {len(hs)}행 · UPA {len(up)}행")

    up["_k"] = up["wharf_name"].map(norm)
    up_lut = {k: g.iloc[0] for k, g in up.groupby("_k")}

    rows: list[dict] = []
    matched = 0
    for _, h in hs.iterrows():
        k = norm(h["facility_name"])
        k = ALIAS.get(k, k)
        if k is None:
            rows.append({"wharf_name": h["facility_name"], "field": "-", "verdict": "SPLIT_1_TO_N",
                         "haesucheong": "", "upa": "", "diff": "",
                         "note": "UPA 쪽이 여러 부두로 분리 — 1:1 비교 불가"})
            continue
        u = up_lut.get(k)
        if u is None:
            rows.append({"wharf_name": h["facility_name"], "field": "-", "verdict": "NO_UPA_MATCH",
                         "haesucheong": "", "upa": "", "diff": "", "note": "UPA 웹 목록에 없음"})
            continue
        matched += 1
        pairs = [
            ("berth_count", h.get("berth_count"), u.get("berth_count")),
            ("depth_min_m", h.get("depth_min_m"), u.get("min_water_depth_m")),
            ("max_dwt", h.get("max_dwt"), u.get("max_capacity_dwt")),
            ("quay_length_m", h.get("quay_length_m"), u.get("max_length_m")),
        ]
        for field, a, b in pairs:
            d, verdict = _diff(a, b)
            rows.append({
                "wharf_name": h["facility_name"], "field": field, "verdict": verdict,
                "haesucheong": a, "upa": b, "diff": d,
                "note": "quay_length는 해수청=부두합계 / UPA=선석최대라 원래 다를 수 있음"
                        if field == "quay_length_m" else "",
            })

    rep = pd.DataFrame(rows)
    rep.to_csv(OUT, index=False, encoding="utf-8-sig")

    print(f"[MATCH] 이름 매칭 {matched}/{len(hs)}")
    print(f"[OUT] {OUT}")
    print("\n=== 판정 분포 ===")
    print(rep["verdict"].value_counts().to_string())

    # 감사에 실제로 쓰는 값(수심·접안능력)의 불일치만 따로 보여준다.
    key = rep[(rep.verdict == "DIFF") & (rep.field.isin(["depth_min_m", "max_dwt", "berth_count"]))]
    if len(key):
        print(f"\n=== 감사 기준값 불일치 {len(key)}건 ===")
        print(key[["wharf_name", "field", "haesucheong", "upa", "diff"]].to_string(index=False))
    else:
        print("\n감사 기준값 불일치 없음")


if __name__ == "__main__":
    main()
