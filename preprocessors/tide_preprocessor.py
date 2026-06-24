"""
조위 데이터 전처리기
raw → staging: data/staging/tide_obs_stg.csv

출처: 공공데이터포탈 국립해양조사원 GetDTRecentApiService
관측소: 울산 조위관측소 (DT_0020)

응답 필드 매핑:
  bscTdlvHgt → tide_level_cm      조위 (기본수준면 기준, cm)
  wndrct     → wind_dir_deg       풍향 (deg)
  wspd       → wind_speed_ms      풍속 (m/s)
  maxMmntWspd→ gust_ms            최대순간풍속 (m/s)
  artmp      → air_temp_c         기온 (℃)
  atmpr      → air_pressure_hpa   기압 (hPa)
  wtem       → sea_temp_c         수온 (℃)
  slntQty    → salinity_psu       염분 (psu)
  crdir      → current_dir_deg    유향 (deg)
  crsp       → current_speed_cms  유속 (cm/s)
"""

import glob
import json
import os
import sys

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from common_preprocessing import (
    add_common_metadata,
    flag_missing_key,
    normalize_nulls,
    parse_datetime_kst_to_utc,
    save_staging_csv,
    to_numeric_safe,
)

RAW_DIR = "data/raw/tide"
STAGING_DIR = "data/staging"

COLUMN_MAP = {
    "_obs_code":    "station_id",
    "_obs_name":    "station_name",
    "_latitude":    "latitude",
    "_longitude":   "longitude",
    "obsrvnDt":     "observed_at_kst",
    "bscTdlvHgt":   "tide_level_cm",
    "wndrct":       "wind_dir_deg",
    "wspd":         "wind_speed_ms",
    "maxMmntWspd":  "gust_ms",
    "artmp":        "air_temp_c",
    "atmpr":        "air_pressure_hpa",
    "wtem":         "sea_temp_c",
    "slntQty":      "salinity_psu",
    "crdir":        "current_dir_deg",
    "crsp":         "current_speed_cms",
}

NUMERIC_COLS = [
    "tide_level_cm",
    "wind_dir_deg", "wind_speed_ms", "gust_ms",
    "air_temp_c", "air_pressure_hpa", "sea_temp_c",
    "salinity_psu",
    "current_dir_deg", "current_speed_cms",
    "latitude", "longitude",
]

FINAL_COLS = [
    "station_id", "station_name",
    "latitude", "longitude",
    "observed_at_utc",
    "tide_level_cm",
    "wind_dir_deg", "wind_speed_ms", "gust_ms",
    "air_temp_c", "air_pressure_hpa", "sea_temp_c",
    "salinity_psu",
    "current_dir_deg", "current_speed_cms",
    "source_system", "source_table", "collected_at_utc",
    "quality_flag", "is_synthetic",
]


def load_all_raw() -> pd.DataFrame:
    files = sorted(glob.glob(os.path.join(RAW_DIR, "tide_obs_*_raw.json")))
    if not files:
        raise FileNotFoundError(f"raw 파일이 없습니다: {RAW_DIR}")

    all_records = []
    for f_path in files:
        with open(f_path, encoding="utf-8") as f:
            payload = json.load(f)
        records = payload.get("data", [])
        all_records.extend(records)
        print(f"  [로드] {f_path}  ({len(records)}건)")

    return pd.DataFrame(all_records)


def validate_tide(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "tide_level_cm" in df.columns:
        invalid = ~df["tide_level_cm"].between(-50, 500)
        df.loc[invalid & df["tide_level_cm"].notna(), "quality_flag"] = "INVALID_TIDE_LEVEL"
    if "wind_speed_ms" in df.columns:
        invalid = df["wind_speed_ms"] < 0
        df.loc[invalid & df["wind_speed_ms"].notna(), "quality_flag"] = "INVALID_WIND_SPEED"
    if "wind_dir_deg" in df.columns:
        invalid = ~df["wind_dir_deg"].between(0, 360)
        df.loc[invalid & df["wind_dir_deg"].notna(), "quality_flag"] = "INVALID_WIND_DIR"
    return df


def preprocess_tide() -> None:
    os.makedirs(STAGING_DIR, exist_ok=True)

    df = load_all_raw()
    print(f"  raw 전체 레코드: {len(df)}건")

    # API 응답에 있는 컬럼만 매핑
    rename = {k: v for k, v in COLUMN_MAP.items() if k in df.columns}
    df = df.rename(columns=rename)

    # 결측값 정규화
    df = normalize_nulls(df)

    # 숫자형 변환
    df = to_numeric_safe(df, NUMERIC_COLS)

    # KST → UTC
    if "observed_at_kst" in df.columns:
        df = parse_datetime_kst_to_utc(df, ["observed_at_kst"])
        df = df.rename(columns={"observed_at_kst": "observed_at_utc"})

    # 공통 메타데이터
    df = add_common_metadata(
        df,
        source_system="KDPA_KHOA_dtRecent",
        source_table="GetDTRecentApiService",
        is_synthetic=False,
    )

    # 기본키 결측 체크
    df = flag_missing_key(df, ["station_id", "observed_at_utc"])

    # 값 범위 검증
    df = validate_tide(df)

    # 최종 컬럼 정리 및 중복 제거
    out_cols = [c for c in FINAL_COLS if c in df.columns]
    df_out = df[out_cols].drop_duplicates(subset=["station_id", "observed_at_utc"])

    save_staging_csv(df_out, os.path.join(STAGING_DIR, "tide_obs_stg.csv"))

    print(f"\n  [품질 플래그 분포]")
    print(df_out["quality_flag"].value_counts().to_string())
    print(f"\n  레코드 수: {len(df_out)}건")


if __name__ == "__main__":
    preprocess_tide()
