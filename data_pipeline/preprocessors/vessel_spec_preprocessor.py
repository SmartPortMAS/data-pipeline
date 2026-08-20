# -*- coding: utf-8 -*-
"""
vessel_spec_preprocessor.py
==================
vessel_spec_collector.py로 수집된 원본(data/raw/vessel_spec/*.json)을 전처리하여
data/staging/vessel_spec_stg.csv로 저장합니다.

원본 구조가 다른 collector들과 다르다 — 선박 1척당 1회 조회 결과를
{queried_clsgn, matched, error, items:[...]} 형태로 감싸서 저장한다(§9
3-3 vessel_spec_collector.py 참고). 이 전처리기는 matched=True인 행만 펼쳐서
(items[0]) 표준 컬럼으로 변환한다 — matched=False/error 행은 "조회했지만 없음"이라는
정보 자체가 유효하므로 조용히 버리지 않고 콘솔에 집계만 출력한다(커버리지 실측,
08_스케줄링_전면재설계_자동배정_설계문서.md §5.2.1-A).

응답 컬럼 → 표준 컬럼 매핑 (2026-08-19 실제 API 응답으로 확인, 추정 아님):
    clsgn        → callsgn                (호출부호, PK)
    ibobprt      → inout_port_se          (내외항구분)
    vsslNo       → vessel_no              (선박번호)
    imoNo        → imo_no                 (IMO번호)
    vsslKorNm    → vessel_kor_name        (선박한글명)
    vsslEngNm    → vessel_eng_name        (선박영문명)
    vsslKnd      → vessel_kind            (선박종류)
    vsslNlty     → vessel_nationality     (선박국적)
    tonEdycSe    → ton_edyc_se            (톤수증서구분 코드)
    tonEdycSeNm  → ton_edyc_se_name       (톤수증서구분명)
    intrlGrtg    → intrl_gross_tonnage    (국제총톤수)
    grtg         → gross_tonnage          (총톤수)
    ntng         → net_tonnage            (순톤수)
    vsslTotLt    → loa_m                  (선박총길이 ← 안벽길이 게이트 핵심 값)
    shdth        → beam_m                 (선박너비)
    vsslDrft     → draught_m              (선박흘수)
    vsslLt       → registered_length_m    (선박길이, 등록길이 계열로 추정)
    vsslDp       → depth_m                (선박깊이, molded depth)
    brbtSe       → bareboat_charter_se    (나용선구분 코드)
    brbtSeNm     → bareboat_charter_se_name
    nvgShapCd    → operation_shape_cd     (운항형태 코드)
    nvgShapNm    → operation_shape_name
    vsslCnstrDt  → built_at               (선박건조일시, 원본 포맷 미확인이라 문자열 보존)
    befClsgn     → prev_callsgn           (이전호출부호)
    nwshipAt     → is_new_ship            (신조선여부, 원본 문자열 보존)
"""

import glob
import json
import os
import sys

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import common_preprocessing as cu  # noqa: E402

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw", "vessel_spec")
STAGING_DIR = os.path.join(BASE_DIR, "data", "staging")
OUT_PATH = os.path.join(STAGING_DIR, "vessel_spec_stg.csv")

COLUMN_MAP = {
    "clsgn": "callsgn",
    "ibobprt": "inout_port_se",
    "vsslNo": "vessel_no",
    "imoNo": "imo_no",
    "vsslKorNm": "vessel_kor_name",
    "vsslEngNm": "vessel_eng_name",
    "vsslKnd": "vessel_kind",
    "vsslNlty": "vessel_nationality",
    "tonEdycSe": "ton_edyc_se",
    "tonEdycSeNm": "ton_edyc_se_name",
    "intrlGrtg": "intrl_gross_tonnage",
    "grtg": "gross_tonnage",
    "ntng": "net_tonnage",
    "vsslTotLt": "loa_m",
    "shdth": "beam_m",
    "vsslDrft": "draught_m",
    "vsslLt": "registered_length_m",
    "vsslDp": "depth_m",
    "brbtSe": "bareboat_charter_se",
    "brbtSeNm": "bareboat_charter_se_name",
    "nvgShapCd": "operation_shape_cd",
    "nvgShapNm": "operation_shape_name",
    "vsslCnstrDt": "built_at",
    "befClsgn": "prev_callsgn",
    "nwshipAt": "is_new_ship",
}

NUMERIC_COLS = [
    "intrl_gross_tonnage", "gross_tonnage", "net_tonnage",
    "loa_m", "beam_m", "draught_m", "registered_length_m", "depth_m",
]


def _flatten_raw(raw_records: list[dict]) -> tuple[list[dict], int, int, int]:
    """{queried_clsgn, matched, error, items} 목록에서 matched 행만 펼친다.

    Returns:
        (펼쳐진 item dict 목록, 조회 총수, 매칭 수, 오류 수) — 마지막 세 값은
        커버리지 집계 출력용(§5.2.1-A가 요구하는 실측 수치를 전처리 단계에서도
        재확인할 수 있게 남긴다).
    """
    flat: list[dict] = []
    total = matched = errored = 0
    for r in raw_records:
        total += 1
        if r.get("error"):
            errored += 1
        if r.get("matched") and r.get("items"):
            matched += 1
            item = dict(r["items"][0])  # 호출부호 1건당 결과는 통상 1건(collector 주석 참고)
            flat.append(item)
    return flat, total, matched, errored


def preprocess_vessel_spec() -> pd.DataFrame | None:
    print("=== 선박제원정보(vessel_spec) 전처리 시작 ===")

    raw_files = sorted(glob.glob(os.path.join(RAW_DIR, "vessel_spec_*.json")))
    if not raw_files:
        print(f"[오류] 처리할 파일이 없습니다. 경로: {RAW_DIR}")
        return None

    all_flat: list[dict] = []
    total_sum = matched_sum = error_sum = 0
    for raw_path in raw_files:
        with open(raw_path, encoding="utf-8") as f:
            raw_records = json.load(f)
        flat, total, matched, errored = _flatten_raw(raw_records)
        print(f"  로드: {os.path.basename(raw_path)} (조회 {total} / 매칭 {matched} / 오류 {errored})")
        all_flat.extend(flat)
        total_sum += total
        matched_sum += matched
        error_sum += errored

    if not all_flat:
        print("[오류] 매칭된 행이 없습니다.")
        return None

    df = pd.DataFrame(all_flat)
    df = cu.standardize_column_names(df, COLUMN_MAP)
    df = cu.normalize_nulls(df)
    df = df.drop_duplicates(subset=["callsgn"], keep="last")  # 여러 raw 파일 걸친 재조회는 최신값 유지
    df = cu.to_numeric_safe(df, NUMERIC_COLS)
    df = cu.add_common_metadata(
        df, source_system="MOF_OPENAPI", source_table="SicsVsslManp3_Info3", is_synthetic=False,
    )

    os.makedirs(STAGING_DIR, exist_ok=True)
    cu.save_staging_csv(df, OUT_PATH)

    rate = (matched_sum / total_sum * 100) if total_sum else 0.0
    print(f"  누적 커버리지: 조회 {total_sum} / 매칭 {matched_sum} (매칭률 {rate:.1f}%) / 오류 {error_sum}")
    print(f"  최종 staging 행 수(callsgn distinct): {len(df)}")
    return df


if __name__ == "__main__":
    preprocess_vessel_spec()
