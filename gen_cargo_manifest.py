# -*- coding: utf-8 -*-
"""
합성 화물 manifest 생성기 (WBS 1.5 / 2.4)
=========================================
UPA 통합화물 API(getIntgCagInfo)는 업체코드(bzentyCd)가 필수라 자동수집이 불가하다
(활용신청 승인·인증키 정상, 조회 키 값만 부재 — UPA 회신 대기). 그동안 mart 화물
뷰와 안전판정 시나리오를 검증할 수 있도록 합성 manifest 를 만든다.

산출물
------
  1) v2 — 정상 케이스 (WBS 1.5 "스키마 정의서 기준 재작성")
     · upa_cargo_manifest 테이블 정의(mart_views.sql)의 39개 컬럼을 순서까지 맞춰
       빠짐없이 채운다. 이전 판은 7개 컬럼이 누락돼 적재 시 스키마가 어긋났다.
     · 화물은 무작위가 아니라 **PORT-MIS 실신고 선종**에 근거해 배정한다.
       (원유운반선 → 원유, LPG운반선 → LPG 계열 …)

  2) v3 — v2 + 위반 케이스 주입 (WBS 2.4)
     · 멘토 피드백 "틀린 데이터를 넣어도 된다 → 판정이 걸리는 장면이 필요하다" 반영.
     · 시스템이 실제로 경고를 띄우도록 의도적 위반 6종을 심는다.

  3) violation_scenarios.csv — 주입한 위반의 기계 판독용 목록
     (위반유형 · 대상 bl_no/선박/부두 · 근거 · 기대 판정)

  4) violation_weather_obs_synthetic.csv — 기상 초과 시나리오용 관측 행
     (화물 manifest 컬럼이 아니므로 별도 파일로 분리)

주의
----
  · 전 행 is_synthetic=True. 실데이터가 아니다.
  · bl_no·MRN·수량은 합성값이며, 화물 대분류만 실선종에 근거한다(cargo_basis 참고).
  · 혼재금지 판정 규칙(SEGREGATION_RULES)은 실무상 대표 조합만 담은 **잠정** 표다.
    IMDG Code 7.2.4 segregation table 원문 대조는 WBS 2.6 에서 확정할 것.

실행
----
  py gen_cargo_manifest.py                # v2·v3 모두 생성, 파이프라인엔 v3 반영
  py gen_cargo_manifest.py --no-violations # 파이프라인엔 v2(정상)만 반영
"""
import argparse
import csv
import os
import random

import pandas as pd

random.seed(20260727)
STAGING = os.path.join("data", "staging")
SHARE_DIR = "samples"
OUT_PIPELINE = os.path.join(STAGING, "upa_cargo_manifest_stg.csv")
OUT_V2 = os.path.join(SHARE_DIR, "upa_cargo_manifest_stg_v2_synthetic.csv")
OUT_V3 = os.path.join(SHARE_DIR, "upa_cargo_manifest_stg_v3_synthetic.csv")
OUT_SCENARIO = os.path.join(SHARE_DIR, "violation_scenarios.csv")
OUT_WEATHER = os.path.join(SHARE_DIR, "violation_weather_obs_synthetic.csv")
TARGET_ROWS = 365

# ---------------------------------------------------------------------------
# 스키마 정본 — mart_views.sql 의 CREATE TABLE upa_cargo_manifest 와 1:1 일치.
# 컬럼 순서까지 맞춰 두면 적재 시 스키마 불일치가 나지 않는다. (WBS 1.5)
# ---------------------------------------------------------------------------
SCHEMA_COLUMNS = [
    "port_code", "ptent_yr", "voyage_no", "callsgn", "vessel_name",
    "vessel_type_name", "vessel_nationality_code", "vessel_nationality_name",
    "mrn_no", "bl_no", "master_bl_no", "io_se_code", "io_se_name",
    "facility_name", "cargo_se_name", "cargo_name_raw", "dg_un_no",
    "cargo_basis", "package_type_name", "unload_method_name",
    "vol_ton_unit_name", "vol_ton", "weight_ton", "vol_size", "weight_size",
    "bulk_vol_size", "bulk_weight_size", "container_count",
    "pod_name", "pol_name", "ldud_port_name", "last_dest_port_name",
    "arrival_at_utc", "customs_progress_status_name",
    "source_system", "source_table", "collected_at_utc",
    "quality_flag", "is_synthetic",
]

# ---------------------------------------------------------------------------
# 선종(PORT-MIS 실신고) → 화물 계열.
# UN 번호·IMDG 등급·용기등급은 KOSHA MSDS 실측값(mart.msds_flat 로 검증).
#   (화물명, UN번호, IMDG등급, 용기등급)
# ---------------------------------------------------------------------------
CARGO_BY_SHIP_KIND = {
    "원유운반선":          [("석유(PETROLEUM)", "1993", "3", "해당없음")],
    "석유제품운반선":      [("러버 솔벤트", "1268", "3", "Ⅰ"), ("디젤 연료", "1202", "3", "Ⅲ"),
                            ("가솔린", "1203", "3", "Ⅱ"), ("케로젠", "1223", "3", "Ⅲ")],
    "석유제품/케미칼겸용":  [("러버 솔벤트", "1268", "3", "Ⅰ"), ("벤젠", "1114", "3", "Ⅱ"),
                            ("톨루엔", "1294", "3", "Ⅱ")],
    "케미칼운반선":        [("벤젠", "1114", "3", "Ⅱ"), ("톨루엔", "1294", "3", "Ⅱ"),
                            ("크실렌", "1307", "3", "Ⅲ"), ("메틸 알코올", "1230", "3", "Ⅱ"),
                            ("스티렌", "2055", "3", "Ⅲ"), ("아크릴로니트릴", "1093", "3", "Ⅰ"),
                            ("테트라하이드로푸란", "2056", "3", "Ⅱ"),
                            ("1,2-에폭시프로판", "1280", "3", "Ⅰ")],
    "케미칼가스운반선":    [("프로필렌", "1077", "2.1", "-"), ("1,3-부타디엔", "1010", "2.1", "-")],
    "LPG운반선":           [("프로페인", "1978", "2.1", "해당없음"), ("부탄", "1011", "2.1", "-")],
    "LNG운반선":           [("메테인", "1971", "2.1", "해당없음")],
    "기타유조선":          [("석유(PETROLEUM)", "1993", "3", "해당없음")],
}
CARGO_BY_DRY_KIND = {
    "산물선": "곡물/광석", "양곡운반선": "양곡", "원목운반선": "원목",
    "광석운반선": "광석", "석탄운반선": "석탄", "시멘트운반선": "시멘트",
    "자동차운반선": "자동차", "철강제운반선": "철강재", "모래운반선": "모래",
    "냉동냉장선": "냉동화물", "일반화물선": "일반잡화", "풀컨테이너선": "컨테이너화물",
    "세미컨테이너선": "컨테이너화물",
}
DEFAULT_DRY_CARGO = "일반잡화"
FALLBACK_LIQUID = [("석유(PETROLEUM)", "1993", "3", "해당없음"),
                   ("벤젠", "1114", "3", "Ⅱ"),
                   ("프로페인", "1978", "2.1", "해당없음")]

# 액체화물 부두 / 일반 부두 (울산항 계류시설 코드표의 취급화물 기준)
LIQUID_FACILITIES = [
    "정일1부두", "정일2부두", "OTK1부두", "OTK2부두", "UTK부두", "UTT부두",
    "S-Oil 1부두", "S-Oil 2부두", "S-Oil 3부두", "S-Oil 4부두",
    "SK1부두", "SK2부두", "SK3부두", "SK4부두", "SK5부두", "SK6부두",
    "SK7부두", "SK8부두", "가스부두", "효성부두", "대한유화부두", "용잠부두",
    "현대오일터미널 신항1부두", "현대오일터미널 신항2부두",
]
DRY_FACILITIES = ["1부두", "5부두", "7부두", "8부두", "신항일반부두", "온산1부두", "염포부두"]

# ---------------------------------------------------------------------------
# 인접 선석쌍 — berth_neo4j_loader.PILOT_ADJACENT_PAIRS 와 동일(ADJACENT_TO).
# 혼재금지는 "인접 선석에서 비혼재 등급을 동시 취급"할 때 걸린다.
# ---------------------------------------------------------------------------
ADJACENT_PAIRS = [
    ("정일1부두", "정일2부두"),
    ("OTK1부두", "OTK2부두"),
    ("현대오일터미널 신항1부두", "현대오일터미널 신항2부두"),
    ("SK5부두", "SK6부두"),
    ("SK6부두", "SK7부두"),
    ("SK7부두", "SK8부두"),
]

# ---------------------------------------------------------------------------
# 혼재금지 규칙 (IMDG 등급 조합)
#   ※ 실무상 격리가 요구되는 대표 조합만 담은 **잠정** 표.
#     IMDG Code 7.2.4 segregation table 원문 대조는 WBS 2.6 에서 확정한다.
#     현재 MSDS 34종에 존재하는 등급: 2.1 / 2.3 / 3 / 6.1 / 8 / 9
# ---------------------------------------------------------------------------
SEGREGATION_RULES = [
    ("8", "3", "부식성(산) ↔ 인화성 액체 — 산이 유기물과 반응해 발열·발화 위험"),
    ("8", "2.1", "부식성(산) ↔ 인화성 가스 — 산 누출 시 가스 인화 위험"),
    ("2.3", "2.1", "독성 가스 ↔ 인화성 가스 — 동시 누출 시 중독·인화 복합 위험"),
    ("5.1", "3", "산화성 물질 ↔ 인화성 액체 — 산화제가 연소를 급격히 촉진"),
]

# 위반 케이스 전용 화물 (평상시 배정 목록에는 없는 등급을 의도적으로 투입)
VIOLATION_CARGO = {
    "황산":     ("황산", "1830", "8", "Ⅱ"),
    "암모니아": ("암모니아", "1005", "2.3", "-"),
}

# SK 부두 수심(m) — UPA 부두 명세. 흘수 초과 판정용.
BERTH_DEPTH_M = {
    "SK1부두": 7.5, "SK2부두": 8.0, "SK3부두": 12.0, "SK4부두": 10.0,
    "SK5부두": 11.0, "SK6부두": 15.0, "SK7부두": 15.0, "SK8부두": 18.0,
}

# ---------------------------------------------------------------------------
# 낡은 staging 감지용 표식
#
# 2026-07 이전 portmis_preprocessor 는 선종코드 매핑이 공식 CODE BOOK 과 어긋나
# 있었다(51=일반화물선·52=화물/시멘트선·53=목재선 …). 그 시절 staging 을 그대로
# 쓰면 "석유제품운반선"이 "화물/시멘트선"으로 남아 있어 화물이 거의 배정되지
# 않는다 — 조용히 잘못된 CSV 가 나오므로 여기서 잡아 경고한다.
# ---------------------------------------------------------------------------
STALE_CATEGORY_MARKERS = {
    "화물/시멘트선", "목재선", "LPG선", "LNG선", "기타화물선", "냉동화물선",
}


# ===========================================================================
# 공통 유틸
# ===========================================================================
def _read(name):
    p = os.path.join(STAGING, name)
    return pd.read_csv(p, encoding="utf-8-sig") if os.path.exists(p) else None


def check_staging_freshness(pm):
    """PORT-MIS staging 이 구 선종매핑으로 만들어진 것인지 검사한다.

    낡은 staging 을 쓰면 액체화물선이 "화물/시멘트선" 등으로 남아 있어 화물이
    거의 배정되지 않는다. 조용히 잘못된 CSV 가 나오는 걸 막기 위해 경고한다.
    """
    if pm is None or "ship_kind_category" not in pm.columns:
        return
    found = STALE_CATEGORY_MARKERS & set(pm["ship_kind_category"].dropna().unique())
    if not found:
        return
    n = int(pm["ship_kind_category"].isin(found).sum())
    print("!" * 70)
    print("[경고] PORT-MIS staging 이 낡았습니다 — 구 선종매핑으로 생성된 파일입니다.")
    print(f"       구 라벨 발견: {', '.join(sorted(found))}  ({n}행)")
    print("       구 매핑은 51=일반화물선·52=화물/시멘트선·53=목재선 이었고,")
    print("       공식 CODE BOOK 기준으로는 51=원유·52=석유제품·53=케미칼 입니다.")
    print("       → 이대로 생성하면 액체화물이 거의 배정되지 않습니다.")
    print()
    print("       [해결] PORT-MIS 를 다시 수집·전처리한 뒤 이 스크립트를 재실행하세요:")
    print("         py -m data_pipeline.run_pipeline portmis --skip-db \\")
    print("            --start YYYYMMDD --end YYYYMMDD")
    print("!" * 70)
    print()


def build_vessel_pool():
    """실제 staging → (callsgn, vessel_name, ship_kind_category, is_liquid, estimated).

    ship_kind_category 는 PORT-MIS 실신고 값 — 여기서 화물 계열이 정해진다.
    위치데이터에만 있고 PORT-MIS 매칭이 안 된 callsgn 은 선종을 모르므로
    estimated=True 로 표시한다(화물도 추정 폴백에서 배정).
    """
    pool = []
    pm = _read("portmis_vessel_stg.csv")
    check_staging_freshness(pm)
    if pm is not None and "callsgn" in pm.columns:
        pm = pm.dropna(subset=["callsgn"]).drop_duplicates(subset=["callsgn"])
        for _, r in pm.iterrows():
            cs = str(r["callsgn"]).strip()
            if not cs or cs.lower() == "nan":
                continue
            liq = str(r.get("is_liquid_cargo_vessel", "")).lower() == "true"
            cat = str(r.get("ship_kind_category", "") or "")
            pool.append((cs, str(r.get("vessel_name", "") or ""), cat, liq, False))
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


def pick_cargo(cat, is_liquid, estimated):
    """선종 대분류 → (화물명, UN번호, IMDG등급, 용기등급, 배정근거)."""
    if not estimated and cat in CARGO_BY_SHIP_KIND:
        name, un, imdg, pg = random.choice(CARGO_BY_SHIP_KIND[cat])
        return name, un, imdg, pg, "PORT-MIS 실선종 기반"
    if not estimated and cat in CARGO_BY_DRY_KIND:
        return CARGO_BY_DRY_KIND[cat], None, None, None, "PORT-MIS 실선종 기반"
    if estimated and is_liquid:
        name, un, imdg, pg = random.choice(FALLBACK_LIQUID)
        return name, un, imdg, pg, "선종미상(위치데이터 전용)-추정"
    return DEFAULT_DRY_CARGO, None, None, None, "선종미상-추정"


def make_row(callsgn, vessel_name, cat, cargo, un_no, basis, facility, seq):
    """SCHEMA_COLUMNS 전 항목을 채운 1행 (WBS 1.5)."""
    wton = round(random.uniform(500, 50000) if un_no else random.uniform(100, 20000), 1)
    return {
        "port_code": "KRUSN",
        "ptent_yr": "2026",
        "voyage_no": f"{random.randint(1, 200):03d}",
        "callsgn": callsgn,
        "vessel_name": vessel_name,
        "vessel_type_name": cat or ("유조선(추정)" if un_no else "화물선(추정)"),
        "vessel_nationality_code": "KR",
        "vessel_nationality_name": "대한민국",
        "mrn_no": f"MRN{random.randint(10000, 99999)}",
        "bl_no": f"BL{seq:06d}",
        "master_bl_no": f"MBL{random.randint(10000, 99999)}",
        "io_se_code": random.choice(["I", "O"]),
        "io_se_name": random.choice(["수입", "수출"]),
        "facility_name": facility,
        "cargo_se_name": "액체" if un_no else "일반",
        "cargo_name_raw": cargo,
        "dg_un_no": un_no,
        "cargo_basis": basis,
        "package_type_name": "벌크" if un_no else "포장",
        "unload_method_name": "펌프" if un_no else "크레인",
        "vol_ton_unit_name": "TON",
        "vol_ton": wton,
        "weight_ton": wton,
        "vol_size": None,
        "weight_size": None,
        "bulk_vol_size": round(wton * 1.1, 1) if un_no else None,
        "bulk_weight_size": wton if un_no else None,
        "container_count": None,
        "pod_name": "울산",
        "pol_name": random.choice(["SINGAPORE", "DALIAN", "휴스턴", "여수", "ONSAN"]),
        "ldud_port_name": None,
        "last_dest_port_name": None,
        "arrival_at_utc": "2026-07-27 00:00:00+00:00",
        "customs_progress_status_name": "반입",
        "source_system": "UPA_SYNTHETIC",
        "source_table": "IntgCagInfo(SYNTHETIC)",
        "collected_at_utc": pd.Timestamp.now("UTC").isoformat(),
        "quality_flag": "OK",
        "is_synthetic": True,
    }


# ===========================================================================
# v2 — 정상 케이스 (WBS 1.5)
# ===========================================================================
def build_v2(pool):
    liquids = [v for v in pool if v[3]]
    others = [v for v in pool if not v[3]]
    ordered = liquids + others
    if not ordered:
        ordered = [(f"TEST{i:03d}", f"샘플선박{i}", "", i % 3 == 0, True) for i in range(50)]

    rows, i, seq = [], 0, 1
    while len(rows) < TARGET_ROWS:
        cs, vname, cat, liq, est = ordered[i % len(ordered)]
        i += 1
        for _ in range(random.randint(1, 3)):
            cargo, un, _imdg, _pg, basis = pick_cargo(cat, liq, est)
            facility = random.choice(LIQUID_FACILITIES if un else DRY_FACILITIES)
            rows.append(make_row(cs, vname, cat, cargo, un, basis, facility, seq))
            seq += 1
            if len(rows) >= TARGET_ROWS:
                break
    return rows


# ===========================================================================
# v3 — 위반 케이스 주입 (WBS 2.4)
# ===========================================================================
def inject_violations(rows):
    """v2 위에 의도적 위반을 심고 시나리오 목록을 함께 반환한다.

    위반 행의 bl_no 는 BL9xxxxx 대역으로 분리해 정상 행과 구분되게 한다.
    """
    scenarios = []
    seq = 900001

    def add(cargo_tuple, callsgn, vessel_name, facility, basis):
        nonlocal seq
        name, un, _imdg, _pg = cargo_tuple
        r = make_row(callsgn, vessel_name, "케미칼운반선", name, un, basis, facility, seq)
        rows.append(r)
        seq += 1
        return r

    # ── V-SEG-01 : 혼재금지 (부식성 산 ↔ 인화성 액체, 인접 선석) ────────────
    a, b = ADJACENT_PAIRS[0]                       # 정일1부두 / 정일2부두
    r1 = add(VIOLATION_CARGO["황산"], "VIO001", "SULFURIC PIONEER", a, "위반주입-혼재금지")
    r2 = add(("벤젠", "1114", "3", "Ⅱ"), "VIO002", "BENZENE STAR", b, "위반주입-혼재금지")
    scenarios.append({
        "violation_id": "V-SEG-01", "violation_type": "혼재금지(IMDG 격리)",
        "target_bl_no": f"{r1['bl_no']},{r2['bl_no']}",
        "target_callsgn": f"{r1['callsgn']},{r2['callsgn']}",
        "target_facility": f"{a},{b}",
        "detail": f"인접 선석 {a}(황산 UN1830/Class 8) ↔ {b}(벤젠 UN1114/Class 3) 동시 취급",
        "rule_basis": SEGREGATION_RULES[0][2],
        "expected_judgement": "혼재금지 경고 — 인접 선석 동시 취급 불가",
    })

    # ── V-SEG-02 : 혼재금지 (독성 가스 ↔ 인화성 가스, 인접 선석) ────────────
    a2, b2 = ADJACENT_PAIRS[4]                     # SK6부두 / SK7부두
    r3 = add(VIOLATION_CARGO["암모니아"], "VIO003", "AMMONIA CARRIER", a2, "위반주입-혼재금지")
    r4 = add(("프로필렌", "1077", "2.1", "-"), "VIO004", "PROPYLENE GAS", b2, "위반주입-혼재금지")
    scenarios.append({
        "violation_id": "V-SEG-02", "violation_type": "혼재금지(IMDG 격리)",
        "target_bl_no": f"{r3['bl_no']},{r4['bl_no']}",
        "target_callsgn": f"{r3['callsgn']},{r4['callsgn']}",
        "target_facility": f"{a2},{b2}",
        "detail": f"인접 선석 {a2}(암모니아 UN1005/Class 2.3) ↔ {b2}(프로필렌 UN1077/Class 2.1) 동시 취급",
        "rule_basis": SEGREGATION_RULES[2][2],
        "expected_judgement": "혼재금지 경고 — 독성가스·인화성가스 인접 취급 불가",
    })

    # ── V-DG-01 : 위험물 UN 번호 누락 (MSDS 매칭 실패 유도) ─────────────────
    r5 = add(("벤젠", None, None, None), "VIO005", "UNKNOWN CHEM", "UTK부두",
             "위반주입-UN번호누락")
    r5["cargo_se_name"] = "액체"          # 액체화물인데 UN 번호만 비어 있는 상태
    r5["package_type_name"] = "벌크"
    r5["unload_method_name"] = "펌프"
    r5["quality_flag"] = "MISSING_DG_CODE"
    scenarios.append({
        "violation_id": "V-DG-01", "violation_type": "위험물 정보 누락",
        "target_bl_no": r5["bl_no"], "target_callsgn": r5["callsgn"],
        "target_facility": "UTK부두",
        "detail": "화물명은 벤젠(인화성 액체)인데 dg_un_no 미기재 → MSDS 매칭 불가",
        "rule_basis": "위험물은 UN 번호 신고 의무 — 미기재 시 안전판정 근거 확보 불가",
        "expected_judgement": "판정불가 경고 — UN 번호 보완 요청",
    })

    # ── V-PKG-01 : 포장·하역방식 부적합 ────────────────────────────────────
    r6 = add(("아크릴로니트릴", "1093", "3", "Ⅰ"), "VIO006", "ACRYLO TRADER",
             "정일1부두", "위반주입-포장부적합")
    r6["package_type_name"] = "드럼(일반)"     # 용기등급 Ⅰ 인데 일반 포장
    r6["unload_method_name"] = "크레인"        # 액체인데 크레인 하역
    scenarios.append({
        "violation_id": "V-PKG-01", "violation_type": "포장·하역방식 부적합",
        "target_bl_no": r6["bl_no"], "target_callsgn": r6["callsgn"],
        "target_facility": "정일1부두",
        "detail": "아크릴로니트릴(UN1093/Class 3/용기등급 Ⅰ)을 일반 드럼·크레인 하역으로 신고",
        "rule_basis": "용기등급 Ⅰ(고위험)은 전용 용기·펌프 이송 필요",
        "expected_judgement": "포장기준 위반 경고",
    })

    # ── V-DRF-01 : 흘수 초과 (부두 수심 대비) ──────────────────────────────
    #   화물 manifest 컬럼이 아니므로 시나리오 정의로 남긴다.
    r7 = add(("석유(PETROLEUM)", "1993", "3", "해당없음"), "VIO007", "DEEP DRAFT VLCC",
             "SK1부두", "위반주입-흘수초과")
    scenarios.append({
        "violation_id": "V-DRF-01", "violation_type": "흘수 초과",
        "target_bl_no": r7["bl_no"], "target_callsgn": r7["callsgn"],
        "target_facility": "SK1부두",
        "detail": f"SK1부두 수심 {BERTH_DEPTH_M['SK1부두']}m 대비 선박 흘수 11.0m 배정",
        "rule_basis": "선박 흘수 < 부두 수심(안전여유 포함) 이어야 접안 가능 — 좌초 위험",
        "expected_judgement": "접안 불가 경고 — 대체 선석(SK8, 수심 18m) 제안",
    })

    # ── V-WX-01 : 기상 임계 초과 (violation_weather_obs 와 연동) ────────────
    r8 = add(("벤젠", "1114", "3", "Ⅱ"), "VIO008", "WEATHER RISK",
             "정일1부두", "위반주입-기상초과")
    scenarios.append({
        "violation_id": "V-WX-01", "violation_type": "기상 임계 초과",
        "target_bl_no": r8["bl_no"], "target_callsgn": r8["callsgn"],
        "target_facility": "정일1부두",
        "detail": "정일1/2부두 하역중단 기준(풍속 17.0m/s·파고 1.0m) 대비 풍속 19.5m/s·파고 1.8m",
        "rule_basis": "data/seed/berth_weather_thresholds_seed.csv — 정일1/2부두(산암리)",
        "expected_judgement": "하역중단 경고 — 이안 기준(19m/s)도 초과",
    })

    return rows, scenarios


def build_weather_violation_rows():
    """기상 초과 시나리오(V-WX-01)용 합성 관측 행."""
    return [{
        "observed_at_utc": "2026-07-27 06:00:00+00:00",
        "wind_dir_deg": 225,
        "wind_speed_ms": 19.5,
        "air_temp_c": 28.0,
        "humidity_pct": 82,
        "air_pressure_hpa": 998.0,
        "visibility_m": 1200,
        "wave_height_sig_m": 1.8,
        "violation_id": "V-WX-01",
        "note": "정일1/2부두 기준(중단 풍속17·파고1.0 / 이안 풍속19) 초과",
        "is_synthetic": True,
    }]


# ===========================================================================
def write_csv(rows, path, columns):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    df = pd.DataFrame(rows)
    for c in columns:                       # 누락 컬럼 보강 후 정본 순서로 정렬
        if c not in df.columns:
            df[c] = None
    df = df[columns]
    df.to_csv(path, index=False, encoding="utf-8-sig")
    return df


def main():
    ap = argparse.ArgumentParser(description="합성 화물 manifest 생성 (WBS 1.5/2.4)")
    ap.add_argument("--no-violations", action="store_true",
                    help="파이프라인에는 v2(정상)만 반영 (v3 파일은 그대로 생성)")
    args = ap.parse_args()

    pool = build_vessel_pool()
    n_real = sum(1 for _, _, _, _, est in pool if not est)
    src = "실제 staging callsgn" if pool else "폴백 가짜 callsgn"

    v2_rows = build_v2(pool)
    df_v2 = write_csv([dict(r) for r in v2_rows], OUT_V2, SCHEMA_COLUMNS)

    v3_rows, scenarios = inject_violations([dict(r) for r in v2_rows])
    df_v3 = write_csv(v3_rows, OUT_V3, SCHEMA_COLUMNS)

    target = df_v2 if args.no_violations else df_v3
    os.makedirs(STAGING, exist_ok=True)
    target.to_csv(OUT_PIPELINE, index=False, encoding="utf-8-sig")

    os.makedirs(SHARE_DIR, exist_ok=True)
    with open(OUT_SCENARIO, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "violation_id", "violation_type", "target_bl_no", "target_callsgn",
            "target_facility", "detail", "rule_basis", "expected_judgement"])
        w.writeheader()
        w.writerows(scenarios)

    pd.DataFrame(build_weather_violation_rows()).to_csv(
        OUT_WEATHER, index=False, encoding="utf-8-sig")

    liq = df_v3[df_v3["dg_un_no"].notna()]
    real_basis = df_v3[df_v3["cargo_basis"] == "PORT-MIS 실선종 기반"]
    print("=" * 70)
    print("합성 화물 manifest 생성 완료 (WBS 1.5 / 2.4)")
    print("=" * 70)
    print(f"  [v2 정상]     {OUT_V2}  ({len(df_v2)}행)")
    print(f"  [v3 위반포함] {OUT_V3}  ({len(df_v3)}행)")
    print(f"  [시나리오]    {OUT_SCENARIO}  ({len(scenarios)}건)")
    print(f"  [기상 초과]   {OUT_WEATHER}")
    print(f"  [파이프라인]  {OUT_PIPELINE}  "
          f"({'v2 정상' if args.no_violations else 'v3 위반포함'})")
    print()
    print(f"  스키마 컬럼   : {len(SCHEMA_COLUMNS)}개 (mart_views.sql 정의와 1:1)")
    print(f"  키 출처       : {src} — 실선종 확인 {n_real}척 / 전체 {len(pool)}척")
    print(f"  위험물 화물   : {len(liq)}행 (UN번호 有)")
    print(f"  화물 배정근거 : 실선종 {len(real_basis)}행 / 추정 {len(df_v3) - len(real_basis)}행")
    print()
    print("  주입된 위반:")
    for s in scenarios:
        print(f"    {s['violation_id']:<10} {s['violation_type']:<20} → {s['expected_judgement']}")
    print()
    print("  ※ 전 행 is_synthetic=True — 실데이터 아님. bl_no/수량은 합성값이며")
    print("    화물 대분류만 PORT-MIS 실신고 선종에 근거함(cargo_basis 참고).")


if __name__ == "__main__":
    main()
