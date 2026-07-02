"""
항만기상 데이터 전처리기
raw → staging: data/staging/weather_obs_stg.csv

출처: 해양수산부 항만기상정보시스템 (marineweather.nmpnt.go.kr)
관측소: 울산항동방파제서단등대 (1041519)
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
    flag_ulsan_bbox,
    normalize_nulls,
    save_staging_csv,
    standardize_column_names,
    to_numeric_safe,
)

RAW_DIR = "data/raw/weather"
STAGING_DIR = "data/staging"

COLUMN_MAP = {
    "MMSI_CODE":       "station_id",
    "MMSI_NM":         "station_name",
    "MMAF_CODE":       "agency_code",
    "MMAF_NM":         "agency_name",
    "DATETIME":        "observed_at_raw",
    "WIND_DIRECT":     "wind_dir_deg",
    "WIND_SPEED":      "wind_speed_ms",
    "AIR_TEMPERATURE": "air_temp_c",
    "HUMIDITY":        "humidity_pct",
    "AIR_PRESSURE":    "air_pressure_hpa",
    "HORIZON_VISIBL":  "visibility_m",
    "LATITUDE":        "latitude",
    "LONGITUDE":       "longitude",
}

NO_DATA_VALUES = {"데이터없음", "미제공", ""}


def _parse_observed_at(val) -> pd.Timestamp | None:
    """YYYYMMDDHHMMSS → UTC Timestamp (관측값은 KST 기준)"""
    if pd.isna(val):
        return None
    s = str(val).strip()
    if len(s) < 14:
        return None
    try:
        dt_kst = pd.Timestamp(
            year=int(s[0:4]), month=int(s[4:6]), day=int(s[6:8]),
            hour=int(s[8:10]), minute=int(s[10:12]), second=int(s[12:14]),
            tz="Asia/Seoul",
        )
        return dt_kst.tz_convert("UTC")
    except Exception:
        return None


def load_all_raw() -> pd.DataFrame:
    files = sorted(glob.glob(os.path.join(RAW_DIR, "weather_obs_*_raw.json")))
    if not files:
        raise FileNotFoundError(f"raw 파일이 없습니다: {RAW_DIR}")

    all_records = []
    for f_path in files:
        with open(f_path, encoding="utf-8") as f:
            payload = json.load(f)
        records = payload.get("data", [])
        for r in records:
            r["_source_file"] = os.path.basename(f_path)
        all_records.extend(records)
        print(f"  [로드] {f_path}  ({len(records)}건)")

    return pd.DataFrame(all_records)


def _clean_no_data(df: pd.DataFrame) -> pd.DataFrame:
    """'데이터없음', '미제공' → NaN"""
    return df.replace(NO_DATA_VALUES, pd.NA)


def validate_wind(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "wind_dir_deg" in df.columns:
        invalid = ~df["wind_dir_deg"].between(0, 360)
        df.loc[invalid & df["wind_dir_deg"].notna(), "quality_flag"] = "INVALID_WIND_DIR"
    if "wind_speed_ms" in df.columns:
        invalid = df["wind_speed_ms"] < 0
        df.loc[invalid & df["wind_speed_ms"].notna(), "quality_flag"] = "INVALID_WIND_SPEED"
    return df


def preprocess_weather() -> None:
    os.makedirs(STAGING_DIR, exist_ok=True)

    df = load_all_raw()
    print(f"  raw 전체 레코드: {len(df)}건")

    # 1. '데이터없음'/'미제공' → NaN
    df = _clean_no_data(df)

    # 2. 컬럼 표준화
    df = standardize_column_names(df, COLUMN_MAP)

    # 3. 결측값 정규화
    df = normalize_nulls(df)

    # 4. 관측시각 파싱 (YYYYMMDDHHMMSS → UTC)
    df["observed_at_utc"] = df["observed_at_raw"].apply(_parse_observed_at)
    df = df.drop(columns=["observed_at_raw"], errors="ignore")

    # 5. 숫자형 변환
    df = to_numeric_safe(df, [
        "wind_dir_deg", "wind_speed_ms", "air_temp_c",
        "humidity_pct", "air_pressure_hpa", "visibility_m",
        "latitude", "longitude",
    ])

    # 6. 공통 메타데이터
    df = add_common_metadata(
        df,
        source_system="MMPNT_MARINEWEATHER",
        source_table="openWeatherNow",
        is_synthetic=False,
    )

    # 7. 기본키 결측 체크
    df = flag_missing_key(df, ["station_id", "observed_at_utc"])

    # 8. 좌표 범위 검증
    df = flag_ulsan_bbox(df)

    # 9. 풍향·풍속 검증
    df = validate_wind(df)

    # 10. 최종 컬럼 정리
    final_cols = [
        "station_id", "station_name", "agency_code", "agency_name",
        "latitude", "longitude",
        "observed_at_utc",
        "wind_dir_deg", "wind_speed_ms",
        "air_temp_c", "humidity_pct",
        "air_pressure_hpa", "visibility_m",
        "source_system", "source_table", "collected_at_utc",
        "quality_flag", "is_synthetic",
    ]
    out_cols = [c for c in final_cols if c in df.columns]
    df_out = df[out_cols].drop_duplicates(subset=["station_id", "observed_at_utc"])

    save_staging_csv(df_out, os.path.join(STAGING_DIR, "weather_obs_stg.csv"))


if __name__ == "__main__":
    preprocess_weather()
