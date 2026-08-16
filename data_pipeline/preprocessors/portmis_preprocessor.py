"""
preprocess_portmis.py
======================
collect_portmis.py로 수집된 PORT-MIS 선박 입출항 원본 데이터를
전처리하여 data/staging/ 에 CSV로 저장합니다.

PORT-MIS 응답 컬럼 → 표준 컬럼 매핑:
    prtAgCd          → port_agency_cd       (항만청 코드: 820=울산)
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

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import common_preprocessing as cu

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
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

# 선종 코드 → 대분류 라벨
# 출처: 해양수산부 PORT-MIS CODE BOOK "03.해사안전(19종) > 가.선박 > 1.선박용도코드"
# (사용자 제공 공식 문서, 2026-07). 이전엔 실측 데이터에 나타난 코드만으로 매핑을
# 추정했으나(51=일반화물선 등으로 오인), 이 공식 전체표로 완전히 교체한다.
SHIP_KIND_CATEGORY = {
    # 여객선
    "11": "여객선", "12": "화객선",
    # 화물선
    "21": "산물선", "22": "양곡운반선", "23": "원목운반선", "24": "광석운반선",
    "25": "석탄운반선", "26": "시멘트운반선", "27": "자동차운반선",
    "28": "핫코일운반선", "29": "철강제운반선", "31": "모래운반선",
    "32": "냉동냉장선", "33": "폐기물운반선", "39": "일반화물선",
    "41": "풀컨테이너선", "42": "세미컨테이너선",
    # 유조선 (액체화물)
    "51": "원유운반선", "52": "석유제품운반선", "53": "케미칼운반선",
    "54": "케미칼가스운반선", "55": "LPG운반선", "56": "LNG운반선",
    "57": "석유제품/케미칼겸용", "59": "기타유조선",
    # 예선
    "61": "견인용예선", "62": "이접안용예선", "63": "압항예선",
    "64": "예선", "69": "기타예선",
    # 부선 (73~75 는 액체화물을 나르는 부선 — 별도 플래그로 취급, 아래 참고)
    "70": "부선", "71": "모래운반용부선", "72": "철강재운반용부선",
    "73": "원유운반용부선", "74": "석유제품운반용부선", "75": "화공약품운반용부선",
    "76": "일반화물운반용부선", "77": "공사작업용부선", "79": "기타부선",
    # 기타선
    "81": "관공선", "82": "경찰정", "83": "군함", "91": "연근해어선",
    "92": "원양어선", "93": "급유선", "94": "급수선", "95": "용달선(통선)",
    "96": "준설선", "97": "유람선", "98": "도선", "99": "기타선",
}

# 액체화물 '본선' 선종 코드 (유조선 카테고리 51~59) — 공식 코드표 기준.
LIQUID_CARGO_KIND_CODES = {"51", "52", "53", "54", "55", "56", "57", "59"}

# 액체화물을 나르는 '부선(바지)' 코드 — 본선과는 성격이 달라(비자항·예인) 별도 플래그.
# 본선 집계에 합칠지는 팀 판단 필요 (관제 목적상 보통 별도 취급).
LIQUID_CARGO_BARGE_CODES = {"73", "74", "75"}

# 인화성 액체를 싣지만 '화물선'은 아닌 지원선(급유선 93).
#
# 급유선은 다른 배에 연료를 공급하는 서비스 선박이라, 공식 CODE BOOK 도
# 유조선(5x)이 아니라 '기타선' 그룹에 배치한다. 따라서 is_liquid_cargo_vessel
# (=액체화물 '본선')에는 넣지 않는다 — 넣으면 하역 스케줄링 대상 척수가
# 부풀려지고 공식 분류와도 어긋난다.
#
# 다만 인화성 유류를 적재한 채 탱커 옆에 접근하므로 **안전관제 관점에서는
# 무시하면 안 된다**. 그래서 별도 플래그로 표시해 두고, 화면·에이전트가
# 목적에 따라 선택적으로 포함할 수 있게 한다.
#   - 하역 스케줄링 통계 → 제외 (기본)
#   - 화재·인화 위험 관제 → 포함 검토
# 본선 집계 합산 여부는 팀 판단 필요.
BUNKERING_VESSEL_CODES = {"93"}

# 선종명(텍스트) 기반 보강 — 코드 체계가 또 바뀌어도 이름으로 액체화물을 포착.
# '급유'는 의도적으로 넣지 않는다(위 BUNKERING_VESSEL_CODES 주석 참고).
# '급수선'(94)·'용달선'(95) 등 다른 지원선도 이 키워드에 걸리지 않는다.
LIQUID_NAME_KEYWORDS = ("유조", "원유", "석유", "케미칼", "화학", "탱커",
                        "tanker", "LPG", "LNG", "가스", "액체", "황산")


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
    # 코드 세트 OR 선종명 키워드로 판정 — 코드 체계 변동에 견고.
    _cd = df["ship_kind_cd"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    _by_code = _cd.isin(LIQUID_CARGO_KIND_CODES)
    if "ship_kind_nm" in df.columns:
        _pat = "|".join(LIQUID_NAME_KEYWORDS)
        _by_name = df["ship_kind_nm"].astype(str).str.contains(_pat, case=False, na=False)
    else:
        _by_name = False
    df["is_liquid_cargo_vessel"] = _by_code | _by_name

    # 9-1. 액체화물 부선(바지) 여부 — 본선과 성격이 달라(비자항) 별도 플래그로 분리.
    # 관제·통계에서 본선에 합산할지는 팀 판단 필요.
    df["is_liquid_cargo_barge"] = _cd.isin(LIQUID_CARGO_BARGE_CODES)

    # 9-2. 급유선(지원선) 여부 — 인화성 유류를 싣지만 화물 '본선'은 아니다.
    # 하역 스케줄링 통계에서는 제외하되, 화재·인화 위험 관제에서는 참조 가능.
    df["is_bunkering_vessel"] = _cd.isin(BUNKERING_VESSEL_CODES)

    # 10. 국내/국제 항로 여부 판단
    # origin_port_cd가 'KR'로 시작하면 국내 항로
    df["is_domestic_voyage"] = (
        df["origin_port_cd"]
        .fillna("")
        .str.startswith("KR")
    )

    # 11. 항만청 이름 코드 기반으로 명시적 레이블링
    port_cd_map = {"820": "울산항"}
    df["port_agency_label"] = (
        df["port_agency_cd"]
        .astype(str)
        .map(port_cd_map)
        .fillna("기타")
    )

    # 12. 요약 출력
    print(f"\n  전처리 후 행 수       : {len(df)}")
    print(f"  울산항 건수           : {(df['port_agency_cd'].astype(str) == '820').sum()}")
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
