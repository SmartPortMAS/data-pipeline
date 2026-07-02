"""
preprocess_portmis.py
======================
collect_portmis.py로 수집된 PORT-MIS 선박 입출항 원본 데이터를
전처리하여 data/staging/ 에 CSV로 저장합니다.

PORT-MIS 응답 컬럼 → 표준 컬럼 매핑:
    prtAgCd          → port_agency_cd       (항만청 코드: 820=울산, 300=온산)
    prtAgNm          → port_agency_nm       (항만청 명칭)
    etryptYear       → entry_year           (입항 연도)
    etryptCo         → entry_count          (입항 횟수: 연간 누적)
    clsgn            → callsgn              (호출부호 ← AIS callsgn과 조인 키)
    vsslNm           → vessel_name          (선박명)
    vsslNltyCd       → nationality_cd       (국적 코드)
    vsslNltyNm       → nationality_nm       (국적 명칭)
    vsslKndCd        → ship_kind_cd         (선종 코드)
    vsslKndNm        → ship_kind_nm         (선종 명칭)
    etryptPurpsCd    → entry_purpose_cd     (입항 목적 코드)
    etryptPurpsNm    → entry_purpose_nm     (입항 목적 명칭)
    frstDpmprtNatPrtCd → origin_port_cd     (최초 출발항 코드)
    frstDpmprtPrtNm    → origin_port_nm     (최초 출발항 명)
    prvsDpmprtNatPrtCd → prev_port_cd       (직전 출발항 코드)
    prvsDpmprtPrtNm    → prev_port_nm       (직전 출발항 명)
    nxlnptNatPrtCd   → next_port_cd         (다음 입항 예정 코드)
    nxlnptPrtNm      → next_port_nm         (다음 입항 예정 명)
    dstnNatPrtCd     → dest_port_cd         (최종 목적지 코드)
    dstnPrtNm        → dest_port_nm         (최종 목적지 명)
    tkoffPrrrnDt     → departure_sched_utc  (출항 예정 일시, 입항 선박용)
    dstnEtryptDt     → dest_arrival_utc     (목적지 입항 예정 일시, 출항 선박용)
"""

import os
import sys
import glob
import json
import pandas as pd
import numpy as np

# common_utils.py 경로 추가
src_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")
if src_dir not in sys.path:
    sys.path.append(src_dir)

import common_utils as cu

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_PORTMIS_DIR = os.path.join(BASE_DIR, "data", "raw", "portmis")
STAGING_DIR = os.path.join(BASE_DIR, "data", "staging")

# PORT-MIS → 표준 컬럼명 매핑
COLUMN_MAP = {
    "prtAgCd":           "port_agency_cd",
    "prtAgNm":           "port_agency_nm",
    "etryptYear":        "entry_year",
    "etryptCo":          "entry_count",
    "clsgn":             "callsgn",
    "vsslNm":            "vessel_name",
    "vsslNltyCd":        "nationality_cd",
    "vsslNltyNm":        "nationality_nm",
    "vsslKndCd":         "ship_kind_cd",
    "vsslKndNm":         "ship_kind_nm",
    "etryptPurpsCd":     "entry_purpose_cd",
    "etryptPurpsNm":     "entry_purpose_nm",
    "frstDpmprtNatPrtCd":"origin_port_cd",
    "frstDpmprtPrtNm":   "origin_port_nm",
    "prvsDpmprtNatPrtCd":"prev_port_cd",
    "prvsDpmprtPrtNm":   "prev_port_nm",
    "nxlnptNatPrtCd":    "next_port_cd",
    "nxlnptPrtNm":       "next_port_nm",
    "dstnNatPrtCd":      "dest_port_cd",
    "dstnPrtNm":         "dest_port_nm",
    "tkoffPrrrnDt":      "departure_sched_utc",
    "dstnEtryptDt":      "dest_arrival_utc",
    "details":           "_details_raw",         # 파싱 후 제거
}

# 선종 코드 → 대분류 라벨 (울산항 주요 선종)
SHIP_KIND_CATEGORY = {
    "51": "일반화물선",
    "52": "화물/시멘트선",
    "53": "목재선",
    "54": "냉동화물선",
    "55": "LPG선",        # 액체화물
    "56": "LNG선",        # 액체화물
    "57": "유조선",       # 액체화물
    "58": "화학제품선",   # 액체화물
    "59": "기타화물선",
    "60": "예인선",
    "61": "여객선",
    "62": "고속여객선",
    "91": "급수선",
    "92": "급유선",
    "93": "부선",
    "94": "크레인선",
    "95": "작업선",
}

# 액체화물(탱커/가스) 선종 코드 세트
LIQUID_CARGO_KIND_CODES = {"55", "56", "57", "58"}


def preprocess_portmis():
    print("=== PORT-MIS 선박입출항 데이터 전처리 시작 ===")

    # 1. raw 파일 목록 수집 (가장 최신 파일 우선)
    raw_files = sorted(glob.glob(os.path.join(RAW_PORTMIS_DIR, "portmis_vessel_*.json")), reverse=True)
    if not raw_files:
        print(f"[오류] 처리할 파일이 없습니다. 경로: {RAW_PORTMIS_DIR}")
        return

    all_dfs = []
    for raw_path in raw_files:
        with open(raw_path, encoding="utf-8") as f:
            data = json.load(f)
        if not data:
            print(f"  [건너뜀] 빈 파일: {raw_path}")
            continue
        df = pd.DataFrame(data)
        print(f"  로드: {os.path.basename(raw_path)} ({len(df)}건)")
        all_dfs.append(df)

    if not all_dfs:
        print("[오류] 유효한 데이터가 없습니다.")
        return

    df = pd.concat(all_dfs, ignore_index=True)
    print(f"\n  총 원본 행 수: {len(df)}")

    # 2. 컬럼명 표준화
    df = cu.standardize_column_names(df, COLUMN_MAP)

    # 3. 불필요 컬럼 제거
    if "_details_raw" in df.columns:
        df = df.drop(columns=["_details_raw"])

    # 4. 결측값 표준화
    df = cu.normalize_nulls(df)

    # 5. 공통 메타데이터 추가
    df = cu.add_common_metadata(
        df,
        source_system="PORTMIS",
        source_table="VsslEtrynd5_Info5",
        is_synthetic=False
    )

    # 6. 숫자형 변환
    df = cu.to_numeric_safe(df, ["entry_year", "entry_count", "ship_kind_cd"])

    # 7. 기본키 결측 검증 (호출부호 필수)
    df = cu.flag_missing_key(df, ["callsgn"])

    # 8. 선종 대분류 라벨 추가
    df["ship_kind_category"] = (
        df["ship_kind_cd"]
        .astype(str)
        .str.strip()
        .map(SHIP_KIND_CATEGORY)
        .fillna("기타/불명")
    )

    # 9. 액체화물선 여부 플래그
    df["is_liquid_cargo_vessel"] = (
        df["ship_kind_cd"]
        .astype(str)
        .str.strip()
        .isin(LIQUID_CARGO_KIND_CODES)
    )

    # 10. 국내/국제 항로 여부 판단
    # origin_port_cd가 'KR'로 시작하면 국내 항로
    df["is_domestic_voyage"] = (
        df["origin_port_cd"]
        .fillna("")
        .str.startswith("KR")
    )

    # 11. 항만청 이름 코드 기반으로 명시적 레이블링
    port_cd_map = {"820": "울산항", "300": "온산항"}
    df["port_agency_label"] = (
        df["port_agency_cd"]
        .astype(str)
        .map(port_cd_map)
        .fillna("기타")
    )

    # 12. 요약 출력
    print(f"\n  전처리 후 행 수       : {len(df)}")
    print(f"  울산항 건수           : {(df['port_agency_cd'].astype(str) == '820').sum()}")
    print(f"  온산항 건수           : {(df['port_agency_cd'].astype(str) == '300').sum()}")
    print(f"  액체화물선 건수       : {df['is_liquid_cargo_vessel'].sum()}")
    print(f"  국내 항로 건수        : {df['is_domestic_voyage'].sum()}")
    print(f"  키 결측(MISSING_KEY)  : {(df['quality_flag'] == 'MISSING_KEY').sum()}")

    # 13. 선종 분포 출력
    print("\n  선종별 분포:")
    kind_dist = df["ship_kind_category"].value_counts()
    for kind, cnt in kind_dist.items():
        print(f"    {kind}: {cnt}건")

    # 14. staging 저장
    os.makedirs(STAGING_DIR, exist_ok=True)
    output_path = os.path.join(STAGING_DIR, "portmis_vessel_stg.csv")
    cu.save_staging_csv(df, output_path)
    print(f"\n  Staging 저장 완료: {output_path}")
    print("=" * 50)


if __name__ == "__main__":
    preprocess_portmis()
