"""
MSDS 데이터 전처리기
raw → staging: data/staging/msds_chemical_stg.csv

출처: 공공데이터포탈 안전보건공단 MSDS API
raw 구조: list_info + detail01~16 중첩 → msdsItemCode 기준 flat 변환
"""

import glob
import json
import os
import re
import sys

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from common_preprocessing import (
    add_common_metadata,
    flag_missing_key,
    normalize_nulls,
    save_staging_csv,
    to_numeric_safe,
)

RAW_DIR = "data/raw/msds"
STAGING_DIR = "data/staging"


def _extract_number(text) -> float | None:
    if pd.isna(text):
        return None
    matches = re.findall(r"[-+]?\d+\.?\d*", str(text))
    return float(matches[0]) if matches else None


def _item_by_code(data, code: str) -> str:
    """msdsItemCode 정확 일치 → itemDetail 반환"""
    if not isinstance(data, list):
        return ""
    for item in data:
        if item.get("msdsItemCode") == code:
            return (item.get("itemDetail") or "").strip()
    return ""


def _items_by_prefix(data, prefix: str) -> str:
    """msdsItemCode가 prefix로 시작하는 항목 모두 결합 (|)"""
    if not isinstance(data, list):
        return ""
    parts = [
        (item.get("itemDetail") or "").strip()
        for item in data
        if item.get("msdsItemCode", "").startswith(prefix)
        and (item.get("itemDetail") or "").strip()
    ]
    return "|".join(parts)


def _flatten_record(record: dict) -> dict:
    """중첩된 MSDS 레코드를 단일 flat dict로 변환 (msdsItemCode 기반)"""
    flat = {
        "chem_id":      record.get("_chem_id", ""),
        "query_name":   record.get("_query_name", ""),
        "collected_at": record.get("_collected_at_utc", ""),
    }

    # list_info: 화학물질 기본 식별 정보
    # API 반환 필드명: chemNameKor, chemEngNm, casNo, unNo, chemId, enNo, keNo
    list_info = record.get("list_info", {})
    flat["cargo_name_raw"] = list_info.get("chemNameKor", "")
    flat["cargo_name_en"]  = list_info.get("chemEngNm", "")
    flat["cas_no"]         = list_info.get("casNo", "")
    flat["dg_un_no"]       = list_info.get("unNo", "")   # 운송정보보다 list_info가 더 안정적

    # detail09: 물리화학적 특성
    # I14=인화점, I12=끓는점, I22=증기압, I28=비중
    d09 = record.get("detail09", {}).get("data", [])
    flat["flash_point_text"]      = _item_by_code(d09, "I14")
    flat["boiling_point_text"]    = _item_by_code(d09, "I12")
    flat["vapor_pressure_text"]   = _item_by_code(d09, "I22")
    flat["specific_gravity_text"] = _item_by_code(d09, "I28")
    flat["flash_point_celsius"]   = _extract_number(flat["flash_point_text"])

    # detail02: 유해성·위험성
    # B02=분류, B0404=신호어, B0406=유해·위험문구
    d02 = record.get("detail02", {}).get("data", [])
    flat["ghs_hazard"]  = _item_by_code(d02, "B02")
    flat["signal_word"] = _item_by_code(d02, "B0404")
    flat["h_statements"] = _items_by_prefix(d02, "B0406")

    # detail14: 운송 정보
    # N06=위험성등급(IMDG), N08=용기등급, N1202=화재EMS, N1204=유출EMS
    d14 = record.get("detail14", {}).get("data", [])
    flat["imdg_class"]    = _item_by_code(d14, "N06")
    flat["packing_group"] = _item_by_code(d14, "N08")
    flat["ems_fire"]      = _item_by_code(d14, "N1202")
    flat["ems_spill"]     = _item_by_code(d14, "N1204")

    # detail04: 응급조치요령 (E로 시작하는 코드들 결합)
    d04 = record.get("detail04", {}).get("data", [])
    flat["emergency_response"] = _items_by_prefix(d04, "E")

    # detail05: 폭발·화재 대처 (F로 시작)
    d05 = record.get("detail05", {}).get("data", [])
    flat["fire_response"] = _items_by_prefix(d05, "F")

    # detail06: 누출사고 대처 (S로 시작)
    d06 = record.get("detail06", {}).get("data", [])
    flat["spill_response"] = _items_by_prefix(d06, "S")

    # detail10: 안정성 및 반응성
    d10 = record.get("detail10", {}).get("data", [])
    flat["incompatible_materials"] = _items_by_prefix(d10, "J")
    flat["decomposition_products"] = _items_by_prefix(d10, "J07")

    # detail11: 독성 정보
    d11 = record.get("detail11", {}).get("data", [])
    flat["toxicity_info"] = _items_by_prefix(d11, "K")

    # detail15: 법적 규제현황
    d15 = record.get("detail15", {}).get("data", [])
    flat["legal_regulation"] = _items_by_prefix(d15, "O")

    return flat


def load_latest_raw() -> list:
    files = sorted(glob.glob(os.path.join(RAW_DIR, "msds_chemical_*_raw.json")))
    if not files:
        raise FileNotFoundError(f"raw 파일이 없습니다: {RAW_DIR}")
    latest = files[-1]
    print(f"[로드] {latest}")
    with open(latest, encoding="utf-8") as f:
        payload = json.load(f)
    return payload.get("data", [])


def preprocess_msds() -> None:
    os.makedirs(STAGING_DIR, exist_ok=True)

    raw_records = load_latest_raw()
    print(f"  raw 레코드: {len(raw_records)}건")

    # 중첩 구조 → flat
    flat_records = [_flatten_record(r) for r in raw_records]
    df = pd.DataFrame(flat_records)

    # 결측값 정규화
    df = normalize_nulls(df)

    # 숫자형 변환
    df = to_numeric_safe(df, ["flash_point_celsius"])

    # 공통 메타데이터
    df = add_common_metadata(
        df,
        source_system="KOSHA_MSDS_API",
        source_table="getChemDetail",
        is_synthetic=False,
    )

    # 기본키 결측 체크
    df = flag_missing_key(df, ["chem_id", "cargo_name_raw"])

    # 최종 컬럼 정리
    final_cols = [
        "chem_id", "cargo_name_raw", "cargo_name_en", "cas_no",
        "flash_point_celsius", "flash_point_text",
        "boiling_point_text", "vapor_pressure_text", "specific_gravity_text",
        "dg_un_no", "imdg_class", "packing_group",
        "ems_fire", "ems_spill",
        "ghs_hazard", "signal_word", "h_statements",
        "emergency_response", "fire_response", "spill_response",
        "incompatible_materials", "decomposition_products",
        "toxicity_info", "legal_regulation",
        "source_system", "source_table", "collected_at_utc",
        "quality_flag", "is_synthetic",
    ]
    out_cols = [c for c in final_cols if c in df.columns]
    df_out = df[out_cols].drop_duplicates(subset=["chem_id"])

    save_staging_csv(df_out, os.path.join(STAGING_DIR, "msds_chemical_stg.csv"))


if __name__ == "__main__":
    preprocess_msds()
