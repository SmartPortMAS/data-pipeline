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

    (2026-08-20) collect_portmis.py가 <details><detail>(입항/출항 이벤트별)를
    arrival_/departure_ 접두로 이미 평탄화해서 넘긴다 — 예전엔 이 블록 전체가
    파싱 없이 버려졌다(raw XML 직접 확인으로 재현·수정, 07 문서 §4.4.1 참고):
    arrival_etryptDt      → arrival_at_utc       (실제 입항 일시)
    arrival_laidupFcltyCd → arrival_facility_cd  (공식 배정 계선시설 코드)
    arrival_laidupFcltyNm → arrival_facility_nm  (공식 배정 계선시설 명)
    arrival_tkoffPrrrnDt  → departure_sched_utc  (입항 시점에 신고한 출항 "예정" 일시)
    arrival_grtg          → gross_tonnage        (총톤수)
    arrival_satmntEntrpsNm→ agency_name          (선박대리점)
    departure_tkoffDt        → departure_at_utc      (실제 출항 일시)
    departure_laidupFcltyCd  → departure_facility_cd (출항 시점 계선시설 코드)
    departure_laidupFcltyNm  → departure_facility_nm (출항 시점 계선시설 명)
    departure_dstnEtryptDt   → dest_arrival_utc      (목적지 입항 예정 일시)

    (2026-09-13) raw에는 있었으나 그동안 COLUMN_MAP 누락으로 버려지던 필드 추가
    (backend 0019 마이그레이션과 세트 — 저 표에 컬럼이 먼저 있어야 함):
    arrival_laidupFcltySubCd   → arrival_facility_sub_code   (계선시설 서브코드.
                                  upa_port_call.facility_spec_sub_code와 같은 체계)
    departure_laidupFcltySubCd→ departure_facility_sub_code
    arrival_intrlGrtg          → intrl_gross_tonnage  (국제총톤수, gross_tonnage와 다른 값)
    arrival_ldadngFrghtClCd    → cargo_class_code      (화물명세 코드)
    arrival_ldadngTon          → cargo_onboard_ton     (입항 시 적재 중인 화물톤수)
    arrival_trnpdtTon          → cargo_transship_ton   (환적톤수)
    arrival_landngFrghtTon     → cargo_unload_ton      (양하화물톤 - 입항상세)
    departure_ldFrghtTon       → cargo_load_ton        (적하화물톤 - 출항상세)

    선원수(crewCo 등)·도선여부(piltgYn)는 의도적으로 제외 — 판정에 쓸 용도가
    없고(선원수) 액체화물선 특성상 변별력이 낮다(도선여부, 거의 상수 예상).
"""

import os
import sys
import glob
import re
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
    # 입항 이벤트(collect_portmis.py의 arrival_ 접두) — details 파싱 복원(2026-08-20)
    "arrival_etryptDt":        "arrival_at_utc",
    "arrival_laidupFcltyCd":   "arrival_facility_cd",
    "arrival_laidupFcltyNm":   "arrival_facility_nm",
    "arrival_tkoffPrrrnDt":    "departure_sched_utc",
    "arrival_grtg":            "gross_tonnage",
    "arrival_satmntEntrpsNm":  "agency_name",
    # 입항 신고구분(최초/변경/최종) — 입항 예정 창 수집(2026-09-17)으로 미래 신고가
    # 들어오므로, 지금 값이 예정인지 확정인지 구분하려고 보존한다(alembic 0018).
    "arrival_reqstSeNm":       "arrival_report_type",
    # 출항 이벤트(departure_ 접두) — 아직 출항 전이면 전부 결측으로 남는다(정상)
    "departure_tkoffDt":       "departure_at_utc",
    "departure_laidupFcltyCd": "departure_facility_cd",
    "departure_laidupFcltyNm": "departure_facility_nm",
    "departure_dstnEtryptDt":  "dest_arrival_utc",
    # (2026-09-13) 시설 서브코드 + 화물톤수 — backend 0019 마이그레이션과 세트.
    # 선원수·도선여부는 의도적으로 제외(모듈 docstring 참고).
    "arrival_laidupFcltySubCd":   "arrival_facility_sub_code",
    "departure_laidupFcltySubCd": "departure_facility_sub_code",
    "arrival_intrlGrtg":          "intrl_gross_tonnage",
    "arrival_ldadngFrghtClCd":    "cargo_class_code",
    "arrival_ldadngTon":          "cargo_onboard_ton",
    "arrival_trnpdtTon":          "cargo_transship_ton",
    "arrival_landngFrghtTon":     "cargo_unload_ton",
    "departure_ldFrghtTon":       "cargo_load_ton",
}

# tz 오프셋이 붙어 오는 필드(예: "2026-06-01T00:10:00+09:00") — parse_datetime_utc가
# 그대로 처리 가능.
_OFFSET_AWARE_DATETIME_COLS = ["arrival_at_utc", "departure_at_utc"]

# tz 오프셋 없이 오는 필드(예: "2026-06-01 04:30:00") — PORT-MIS는 한국 기관이라
# 이 값들은 KST 기준이다. 그대로 UTC로 해석하면 9시간이 밀린다(2026-08-20 확인 —
# arrival_at_utc/departure_at_utc와 달리 이 두 필드만 원본에 오프셋이 없다).
_NAIVE_KST_DATETIME_COLS = ["departure_sched_utc", "dest_arrival_utc"]


def _parse_naive_kst_to_utc(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    df = df.copy()
    for col in columns:
        if col not in df.columns:
            continue
        parsed = pd.to_datetime(df[col], errors="coerce")
        if hasattr(parsed.dt, "tz") and parsed.dt.tz is not None:
            # 이미 tz-aware로 파싱됐으면(예상 밖 포맷) 그대로 UTC 변환만 한다.
            df[col] = parsed.dt.tz_convert("UTC")
        else:
            df[col] = parsed.dt.tz_localize(
                "Asia/Seoul", ambiguous="NaT", nonexistent="NaT"
            ).dt.tz_convert("UTC")
    return df

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

    # 1. raw 파일 목록 수집 — 오래된 파일부터 (2026-09-17 수정)
    #
    # 예전엔 최신 파일을 먼저 합쳤는데(reverse=True), 적재기는 같은 자연키가 여러 번
    # 나오면 "마지막" 행을 남긴다(common_pg_loader.load_csv keep="last"). 그래서 같은
    # 항차가 여러 파일에 있으면 가장 오래된 값이 최신 값을 덮어썼다. 입항 예정 창을
    # 매시 다시 조회하면 같은 항차가 최초 -> 변경 -> 최종으로 여러 파일에 남으므로
    # 이 순서가 곧 정확도다. 아래 3-1에서 최신 1행만 남긴다.
    #
    # 정렬 기준은 파일명 전체가 아니라 (종료일, 시작일, 수정 시각)이다. 시작일은
    # 겹침 때문에 뒤로 갈 수 있어 이름순이 실행 순서와 어긋난다(실측: 오늘 실행한
    # 20260820_20260920 이 8/24 실행한 20260821_20260824 보다 앞에 와서 옛 값이
    # 이겼다). 종료일은 실행일(+3일)을 따라 늘기만 한다.
    def _collected_order(path):
        m = re.search(r"portmis_vessel_(\d{8})_(\d{8})\.json$", path)
        start, end = (m.group(1), m.group(2)) if m else ("", "")
        return (end, start, os.path.getmtime(path))

    raw_files = sorted(
        glob.glob(os.path.join(RAW_PORTMIS_DIR, "portmis_vessel_*.json")),
        key=_collected_order,
    )
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

    # 2-1. COLUMN_MAP에 없는 원본 필드 제거 — details 파싱 복원(2026-08-20)으로
    # arrival_/departure_ 접두 원본 필드가 20여 개 추가로 들어오는데, 그중 승무원
    # 수·예선/도선 여부·화물톤수 세부내역·선박신고번호(mrNum) 등은 지금 이 표(DB)
    # 스키마에 없다. 이 표는 backend(Alembic) 소유라(auto_create=False) 여기서
    # 컬럼을 새로 만들 수 없으므로, COLUMN_MAP에서 의도적으로 고른 필드만 남기고
    # 나머지는 버린다(필요해지면 백엔드 마이그레이션과 함께 다시 추가할 것).
    # 예전 raw 파일에 남아있던 미파싱 "details" 원문 컬럼도 이 필터로 같이 걸러진다.
    _known_cols = set(COLUMN_MAP.values())
    df = df[[c for c in df.columns if c in _known_cols]]

    # 3. 결측값 표준화
    df = cu.normalize_nulls(df)

    # 3-1. 같은 항차(호출부호+입항연도+입항횟수)는 가장 나중에 수집한 행만 남긴다.
    # 위 1번에서 오래된 파일부터 합쳤으므로 keep="last" = 최신 신고.
    _key = [c for c in ("callsgn", "entry_year", "entry_count") if c in df.columns]
    if len(_key) == 3:
        _before = len(df)
        df = df.drop_duplicates(subset=_key, keep="last").reset_index(drop=True)
        if len(df) < _before:
            print(f"  같은 항차 중복 {_before - len(df)}행 -> 최신 수집분만 유지")

    # 4. 공통 메타데이터 추가
    df = cu.add_common_metadata(
        df,
        source_system="PORTMIS",
        source_table="VsslEtrynd5_Info5",
        is_synthetic=False
    )

    # 5. 숫자형 변환
    df = cu.to_numeric_safe(df, [
        "entry_year", "entry_count", "ship_kind_cd", "gross_tonnage",
        "intrl_gross_tonnage", "cargo_onboard_ton", "cargo_transship_ton",
        "cargo_unload_ton", "cargo_load_ton",
    ])

    # 5-1. 시각 파싱 — arrival_at_utc/departure_at_utc는 원본에 +09:00 오프셋이
    # 붙어 오고(parse_datetime_utc가 그대로 처리), departure_sched_utc/
    # dest_arrival_utc는 오프셋 없이 KST naive로 오므로 별도 변환이 필요하다
    # (모듈 docstring, 2026-08-20 details 파싱 복원 참고).
    df = cu.parse_datetime_utc(df, _OFFSET_AWARE_DATETIME_COLS)
    df = _parse_naive_kst_to_utc(df, _NAIVE_KST_DATETIME_COLS)

    # 6. 기본키 결측 검증 (호출부호 필수)
    df = cu.flag_missing_key(df, ["callsgn"])

    # 8. 선종 대분류 라벨 추가
    df["ship_kind_category"] = (
        df["ship_kind_cd"]
        .astype(str)
        .str.strip()
        .map(SHIP_KIND_CATEGORY)
        .fillna("기타/불명")
    )

    # 9. 액체화물선 '본선' 여부 플래그
    # 코드 세트 OR 선종명 키워드로 판정 — 코드 체계 변동에 견고.
    _cd = df["ship_kind_cd"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    _by_code = _cd.isin(LIQUID_CARGO_KIND_CODES)
    if "ship_kind_nm" in df.columns:
        _pat = "|".join(LIQUID_NAME_KEYWORDS)
        _by_name = df["ship_kind_nm"].astype(str).str.contains(_pat, case=False, na=False)
    else:
        _by_name = False

    # 부선(73~75)은 이름 키워드에 걸려도 본선으로 세지 않는다.
    #
    # 이 가드가 없으면 아래 주석(9-1)이 선언한 "부선은 본선과 분리한다"가 이름 분기
    # 하나로 무너진다. 실제로 그랬다 —
    #     73 원유운반용부선   → 이름에 '원유' → 본선으로 샘  ❌
    #     74 석유제품운반용부선 → 이름에 '석유' → 본선으로 샘 ❌
    #     75 화공약품운반용부선 → 키워드 없음  → 안 샘        ✅
    # 같은 부선 3종이 임의로 쪼개져, 코드가 정본인데 이름이 그걸 뒤집는 상태였다.
    # (급유선 93 에 대해 :113 이 걱정한 "척수가 부풀려진다"가 부선에서 발생)
    _is_barge = _cd.isin(LIQUID_CARGO_BARGE_CODES)
    df["is_liquid_cargo_vessel"] = (_by_code | _by_name) & ~_is_barge

    # 9-1. 액체화물 부선(바지) 여부 — 본선과 성격이 달라(비자항) 별도 플래그로 분리.
    # 관제·통계에서 본선에 합산할지는 팀 판단 필요.
    df["is_liquid_cargo_barge"] = _cd.isin(LIQUID_CARGO_BARGE_CODES)

    # 9-2. 급유선(지원선) 여부 — 인화성 유류를 싣지만 화물 '본선'은 아니다.
    # 하역 스케줄링 통계에서는 제외하되, 화재·인화 위험 관제에서는 참조 가능.
    df["is_bunkering_vessel"] = _cd.isin(BUNKERING_VESSEL_CODES)

    # 10. 국내/국제 항로 여부 판단 — '직전' 출발항 기준
    #
    # 예전에는 origin_port_cd(frstDpmprtNatPrtCd = 최초 출발항)를 썼는데, 그건
    # 이번 항차의 국내/국제가 아니라 그 배가 애초에 어디서 출발했는지다.
    # 실측(raw 121행) 최초 ≠ 직전이 13건:
    #     KMTC JAKARTA : 최초 HKHKG(홍콩) / 직전 KRPUS(부산)
    #       → 이번 구간은 부산→울산 연안인데 '국제'로 판정됐다
    #     스타 파이오니아 : 최초 KRKAN(광양) / 직전 KRPUS(부산)
    # 이번 항차를 보려면 prev_port_cd(prvsDpmprtNatPrtCd = 직전 출발항)가 맞다.
    # 두 컬럼 모두 이미 수집·적재되어 있다(COLUMN_MAP 참고).
    #
    # 결측은 False(국제)로 접지 않고 None 으로 둔다 — 모르는 것을 단정하지 않는다는
    # 이 파이프라인 원칙(quality_flag·identity_confidence 와 같은 취급)에 맞춘다.
    _prev = df["prev_port_cd"] if "prev_port_cd" in df.columns else pd.Series(index=df.index, dtype=object)
    _prev_s = _prev.astype("string").str.strip()
    df["is_domestic_voyage"] = _prev_s.str.startswith("KR").astype("boolean")

    # 11. 항만청 이름 코드 기반으로 명시적 레이블링
    port_cd_map = {"820": "울산항"}
    df["port_agency_label"] = (
        df["port_agency_cd"]
        .astype(str)
        .map(port_cd_map)
        .fillna("기타")
    )

    # 11-B. 입항일 필터 — 수집일(KST) 당일 입항분만 남긴다.
    #
    # [2026-09-13] PORT-MIS API 의 조회기간 파라미터(--start/--end)가 실제로는
    # 입항일 필터가 아니다. 20260912~20260915 로 요청해도 8/19 입항분까지 딸려
    # 온다(실측 409행 중 217행, 53% 가 요청기간 밖). 신고 접수일 기준으로 주는
    # 것으로 보이는데, 신고는 입항 전에 미리 하므로 과거 건이 섞인다.
    #
    # 그래서 기간 한정은 우리 쪽에서 한다. 기준은 "수집일 당일 이후 입항":
    #
    #   · 과거분을 버리는 이유 — 이미 들어온 배는 배정 대상이 아니다. 재항 여부는
    #     upa_vessel_position(실시간)과 upa_port_call(접안 이벤트)로 판단한다.
    #   · 미래분을 남기는 이유 — 선석 배정은 미리 해야 한다. 오늘치만 받으면
    #     내일·모레 들어올 배(실측 42척/17척)를 못 보고 당일치기 배정이 된다.
    #
    # 상한은 따로 두지 않는다 — 수집 시 --start/--end 로 요청한 범위가 곧 상한이다.
    # 하루 1회 수집이므로 매일 그날 이후분이 갱신·추가되어 누적된다
    # (적재는 callsgn+entry_year+entry_count UPSERT).
    #
    # ★ 날짜 비교는 KST 로 한다. arrival_at_utc 는 UTC 로 저장되므로 UTC 날짜로
    #   자르면 KST 00:00~09:00 입항분(그날 새벽에 들어온 배)이 전날로 분류돼
    #   통째로 빠진다.
    target_kst = pd.Timestamp.now(tz="Asia/Seoul").normalize().date()
    before = len(df)
    arr_kst = pd.to_datetime(df["arrival_at_utc"], errors="coerce", utc=True).dt.tz_convert("Asia/Seoul")
    df = df[arr_kst.dt.date >= target_kst].copy()
    print(f"\n  입항일 필터({target_kst} KST 이후): {before} -> {len(df)}행")

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
