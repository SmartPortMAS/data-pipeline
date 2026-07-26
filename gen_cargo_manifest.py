# -*- coding: utf-8 -*-
"""
upa_cargo_manifest_stg.csv 근거기반 합성 샘플 생성기 (뷰 검증용) — v2
====================================================================
※ 실제 UPA 화물 API(getIntgCagInfo)는 업체코드(bzentyCd) 필수라 자동수집이 막혀
   있다(활용신청은 승인됐으나 조회에 쓸 bzentyCd 값 자체가 없음 — UPA 확인 중).
   그동안 mart 뷰 3종(port_call_overview·cargo_msds·dashboard_current)이
   upa_cargo_manifest 부재로 생성되지 않는다.

v1과의 차이 (핵심 개선):
  v1은 액체선 여부만 보고 화물을 "액체화물 목록 중 무작위"로 배정했다.
  v2는 PORT-MIS 가 실제로 신고한 **선종(ship_kind_category)** 을 그대로 반영해,
  그 선종 계열에 맞는 화물만 배정한다 — 즉 "원유운반선"이라 신고된 배에는
  원유만, "LPG운반선"에는 LPG만 배정된다. bl_no·수량 등 개별 화물 건 자체는
  여전히 합성(가짜)이지만, "이 배가 어떤 계열의 화물을 실었나"는 더 이상
  무작위가 아니라 PORT-MIS 실신고 데이터에 근거한다.

여전히 합성(가짜)인 것: bl_no, MRN, 정확한 수량, 정확한 화물명(같은 계열 내 랜덤).
실제(real)인 것: callsgn·선박명(실제 staging) + 선종(PORT-MIS 실신고) + 화물계열 매핑
+ UN번호(실제 코드).
→ is_synthetic=True 로 표기하고, 팀 공유 시 "화물 상세는 가짜, 화물 대분류는 실선종
기반"이라고 명확히 설명할 것.

실행: (data-pipeline 폴더에서)  py gen_cargo_manifest.py
출력: data/staging/upa_cargo_manifest_stg.csv
"""
import os
import random

import pandas as pd

random.seed(20260725)
STAGING = os.path.join("data", "staging")
OUT = os.path.join(STAGING, "upa_cargo_manifest_stg.csv")
# 공유·커밋용 복사본. data/staging 은 .gitignore 제외 대상이라 커밋이 안 되므로,
# 팀 공유용은 samples/ 에 _synthetic 접미사를 붙여 따로 떨군다.
SHARE_DIR = "samples"
SHARE_OUT = os.path.join(SHARE_DIR, "upa_cargo_manifest_stg_synthetic.csv")
TARGET_ROWS = 365

# 선종 대분류(ship_kind_category, PORT-MIS 실신고) → 그 계열에 맞는 화물만 배정.
# (화물명, UN번호) — UN번호는 실제 코드, MSDS 조인용.
CARGO_BY_SHIP_KIND = {
    "원유운반선":         [("원유", "1267")],
    "석유제품운반선":     [("나프타", "1268"), ("경유", "1202"), ("휘발유", "1203"), ("등유", "1223")],
    "석유제품/케미칼겸용": [("나프타", "1268"), ("벤젠", "1114"), ("톨루엔", "1294")],
    "케미칼운반선":       [("벤젠", "1114"), ("톨루엔", "1294"), ("자일렌", "1307"),
                          ("메탄올", "1230"), ("스티렌", "2055"), ("아세톤", "1090")],
    "케미칼가스운반선":   [("프로필렌", "1077"), ("부타디엔", "1010")],
    "LPG운반선":          [("LPG", "1075"), ("프로판", "1978"), ("부탄", "1011")],
    "LNG운반선":          [("LNG", "1972")],
    "기타유조선":         [("석유제품(기타)", "1268")],
}
# 비액체 선종 → 화물(UN번호 없음)
CARGO_BY_DRY_KIND = {
    "산물선": "곡물/광석", "양곡운반선": "양곡", "원목운반선": "원목",
    "광석운반선": "광석", "석탄운반선": "석탄", "시멘트운반선": "시멘트",
    "자동차운반선": "자동차", "철강제운반선": "철강재", "모래운반선": "모래",
    "냉동냉장선": "냉동화물", "일반화물선": "일반잡화", "풀컨테이너선": "컨테이너화물",
    "세미컨테이너선": "컨테이너화물",
}
DEFAULT_DRY_CARGO = "일반잡화"

# 액체선 계열이지만 PORT-MIS 매칭이 안 돼 선종을 모르는 배(위치데이터만 있는 callsgn)
# 에 한해서만 쓰는 폴백 — 추정 표시(estimated=True)로 구분.
FALLBACK_LIQUID_CARGO = [("원유", "1267"), ("나프타", "1268"), ("벤젠", "1114"), ("LPG", "1075")]

FACILITIES = ["제3부두", "제4부두", "SK2부두", "SK8부두", "S-Oil 1부두", "OTK1부두",
              "정일1부두", "가스부두", "온산1부두", "신항일반부두"]


def _read(name):
    p = os.path.join(STAGING, name)
    return pd.read_csv(p, encoding="utf-8-sig") if os.path.exists(p) else None


def build_vessel_pool():
    """실제 staging 에서 (callsgn, vessel_name, ship_kind_category, is_liquid, estimated) 목록을 만든다.

    ship_kind_category 는 PORT-MIS 가 실제로 신고한 값 — 여기서 화물계열을 정한다.
    위치데이터에만 있고 PORT-MIS 매칭이 안 되는 callsgn 은 선종을 모르므로
    estimated=True 로 표시(화물도 추정 폴백에서 배정).
    """
    pool = []
    pm = _read("portmis_vessel_stg.csv")
    if pm is not None and "callsgn" in pm.columns:
        pm = pm.dropna(subset=["callsgn"]).drop_duplicates(subset=["callsgn"])
        for _, r in pm.iterrows():
            cs = str(r["callsgn"]).strip()
            if not cs or cs.lower() == "nan":
                continue
            liq = str(r.get("is_liquid_cargo_vessel", "")).lower() == "true"
            cat = str(r.get("ship_kind_category", "") or "")
            pool.append((cs, str(r.get("vessel_name", "") or ""), cat, liq, False))
    # 위치데이터 callsgn 보강 — PORT-MIS 미매칭(선종 모름) → estimated=True
    upa = _read("upa_vessel_position_stg.csv")
    if upa is not None and "callsgn" in upa.columns:
        have = {c for c, _, _, _, _ in pool}
        u = upa.dropna(subset=["callsgn"]).drop_duplicates(subset=["callsgn"])
        for _, r in u.iterrows():
            cs = str(r["callsgn"]).strip()
            if not cs or cs.lower() == "nan" or cs in have:
                continue
            pool.append((cs, str(r.get("vessel_name", "") or ""), "", False, True))
    return pool


def pick_cargo(ship_kind_category: str, is_liquid: bool, estimated: bool):
    """선종 대분류에 맞는 화물 (화물명, UN번호, cargo_basis) 반환."""
    if not estimated and ship_kind_category in CARGO_BY_SHIP_KIND:
        cargo, un = random.choice(CARGO_BY_SHIP_KIND[ship_kind_category])
        return cargo, un, "PORT-MIS 실선종 기반"
    if not estimated and ship_kind_category in CARGO_BY_DRY_KIND:
        return CARGO_BY_DRY_KIND[ship_kind_category], None, "PORT-MIS 실선종 기반"
    if estimated and is_liquid:
        cargo, un = random.choice(FALLBACK_LIQUID_CARGO)
        return cargo, un, "선종미상(위치데이터 전용)-추정"
    return DEFAULT_DRY_CARGO, None, "선종미상-추정"


def gen_rows(pool):
    rows = []
    liquids = [v for v in pool if v[3]]
    others = [v for v in pool if not v[3]]
    ordered = liquids + others
    if not ordered:
        ordered = [(f"TEST{i:03d}", f"샘플선박{i}", "", i % 3 == 0, True) for i in range(50)]

    i = 0
    while len(rows) < TARGET_ROWS:
        cs, vname, cat, liq, estimated = ordered[i % len(ordered)]
        i += 1
        n_items = random.randint(1, 3)
        for _ in range(n_items):
            cargo, un_no, basis = pick_cargo(cat, liq, estimated)
            wton = round(random.uniform(500, 50000) if un_no else random.uniform(100, 20000), 1)
            rows.append({
                "port_code": "KRUSN",
                "ptent_yr": "2026",
                "voyage_no": f"{random.randint(1, 200):03d}",
                "callsgn": cs,
                "vessel_name": vname,
                "vessel_type_name": cat or ("유조선(추정)" if liq else "화물선(추정)"),
                "vessel_nationality_code": "KR",
                "vessel_nationality_name": "대한민국",
                "mrn_no": f"MRN{random.randint(10000, 99999)}",
                "bl_no": f"BL{random.randint(100000, 999999)}",
                "master_bl_no": f"MBL{random.randint(10000, 99999)}",
                "io_se_code": random.choice(["I", "O"]),
                "io_se_name": random.choice(["수입", "수출"]),
                "facility_name": random.choice(FACILITIES),
                "cargo_se_name": "액체" if un_no else "일반",
                "cargo_name_raw": cargo,
                "dg_un_no": un_no,
                "cargo_basis": basis,  # ★ 이 화물 배정이 실선종 기반인지 추정인지 표시
                "package_type_name": "벌크" if un_no else "포장",
                "unload_method_name": "펌프" if un_no else "크레인",
                "vol_ton_unit_name": "TON",
                "vol_ton": wton,
                "weight_ton": wton,
                "pod_name": "울산",
                "pol_name": random.choice(["SINGAPORE", "DALIAN", "휴스턴", "여수"]),
                "arrival_at_utc": "2026-07-24 00:00:00+00:00",
                "customs_progress_status_name": "반입",
            })
            if len(rows) >= TARGET_ROWS:
                break
    return rows


def main():
    pool = build_vessel_pool()
    src = "실제 staging callsgn" if pool else "폴백 가짜 callsgn"
    n_real_kind = sum(1 for _, _, _, _, est in pool if not est)
    rows = gen_rows(pool)
    df = pd.DataFrame(rows)
    df["source_system"] = "UPA_SYNTHETIC"
    df["source_table"] = "IntgCagInfo(SYNTHETIC)"
    df["collected_at_utc"] = pd.Timestamp.now("UTC")
    df["quality_flag"] = "OK"
    df["is_synthetic"] = True

    os.makedirs(STAGING, exist_ok=True)
    df.to_csv(OUT, index=False, encoding="utf-8-sig")
    # 공유·커밋용 복사본 (samples/, _synthetic 접미사)
    os.makedirs(SHARE_DIR, exist_ok=True)
    df.to_csv(SHARE_OUT, index=False, encoding="utf-8-sig")

    liq = df[df["dg_un_no"].notna()]
    real_basis = df[df["cargo_basis"] == "PORT-MIS 실선종 기반"]
    print(f"[생성 완료] {OUT}")
    print(f"[공유용 사본] {SHARE_OUT}  ← 팀 공유/커밋은 이 파일")
    print(f"  총 {len(df)}행  (키 출처: {src}, 실선종 확인 선박 {n_real_kind}척/{len(pool)}척)")
    print(f"  위험물(UN번호 有) 화물: {len(liq)}행 / 고유 선박 {df['callsgn'].nunique()}척")
    print(f"  화물 배정 근거 - 실선종 기반: {len(real_basis)}행 / 추정: {len(df) - len(real_basis)}행")
    print(f"  위험물 UN번호 종류: {sorted(liq['dg_un_no'].dropna().unique())}")
    print("  ※ 합성 데이터입니다 (is_synthetic=True). bl_no/수량은 가짜이나,")
    print("     화물 대분류는 PORT-MIS 실신고 선종(cargo_basis 컬럼 참고) 기반입니다.")


if __name__ == "__main__":
    main()
