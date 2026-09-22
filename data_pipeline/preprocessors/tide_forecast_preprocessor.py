"""
조위 예보 전처리기
raw → staging: data/staging/tide_forecast_stg.csv

출처: 공공데이터포털 국립해양조사원 조석예보 GetTideFcstHghLwApiService
관측소: 울산 조위관측소 (DT_0020)

응답 필드 매핑:
  predcDt     → predicted_at_kst → predicted_at_utc   예보 시각
  predcTdlvVl → tide_level_cm                         예측 조위 (기본수준면 기준, cm)
  extrSe      → extr_type                             극값 구분 코드
              → extr_kind                             고조/저조 (파생)

tide_obs 와 같은 단위·기준면(cm, 기본수준면)이라 두 표를 바로 이어 쓸 수 있다.
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

RAW_DIR = "data/raw/tide_forecast"
STAGING_DIR = "data/staging"

COLUMN_MAP = {
    "_obs_code":    "station_id",
    "_obs_name":    "station_name",
    "_latitude":    "latitude",
    "_longitude":   "longitude",
    "predcDt":      "predicted_at_kst",
    "predcTdlvVl":  "tide_level_cm",
    "extrSe":       "extr_type",
}

# 1=제1고조 2=제1저조 3=제2고조 4=제2저조 (수집기 docstring 의 실측 근거 참조)
EXTR_KIND = {"1": "고조", "2": "저조", "3": "고조", "4": "저조"}

NUMERIC_COLS = ["tide_level_cm", "latitude", "longitude"]

FINAL_COLS = [
    "station_id", "station_name",
    "latitude", "longitude",
    "predicted_at_utc",
    "tide_level_cm",
    "extr_type", "extr_kind",
    "source_system", "source_table", "collected_at_utc",
    "quality_flag", "is_synthetic",
]


def load_all_raw() -> pd.DataFrame:
    files = sorted(glob.glob(os.path.join(RAW_DIR, "tide_forecast_*_raw.json")))
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


def validate_tide_forecast(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "tide_level_cm" in df.columns:
        # tide_obs 와 같은 범위 기준을 쓴다 (validate_tide)
        invalid = ~df["tide_level_cm"].between(-50, 500)
        df.loc[invalid & df["tide_level_cm"].notna(), "quality_flag"] = "INVALID_TIDE_LEVEL"
    if "extr_type" in df.columns:
        # 모르는 코드가 오면 고조/저조를 단정하지 않고 플래그만 남긴다
        unknown = ~df["extr_type"].isin(EXTR_KIND)
        df.loc[unknown & df["extr_type"].notna(), "quality_flag"] = "UNKNOWN_EXTR_TYPE"
    return df


def preprocess_tide_forecast() -> None:
    os.makedirs(STAGING_DIR, exist_ok=True)

    df = load_all_raw()
    print(f"  raw 전체 레코드: {len(df)}건")

    rename = {k: v for k, v in COLUMN_MAP.items() if k in df.columns}
    df = df.rename(columns=rename)

    df = normalize_nulls(df)
    df = to_numeric_safe(df, NUMERIC_COLS)

    # 극값 구분 코드는 문자열로 고정한다 (to_numeric_safe 대상이 아님)
    if "extr_type" in df.columns:
        df["extr_type"] = df["extr_type"].astype("string").str.strip()
        df["extr_kind"] = df["extr_type"].map(EXTR_KIND)

    # KST → UTC
    if "predicted_at_kst" in df.columns:
        df = parse_datetime_kst_to_utc(df, ["predicted_at_kst"])
        df = df.rename(columns={"predicted_at_kst": "predicted_at_utc"})

    df = add_common_metadata(
        df,
        source_system="KDPA_KHOA_tideFcstHghLw",
        source_table="GetTideFcstHghLwApiService",
        is_synthetic=False,
    )

    df = flag_missing_key(df, ["station_id", "predicted_at_utc"])
    df = validate_tide_forecast(df)

    out_cols = [c for c in FINAL_COLS if c in df.columns]
    df_out = df[out_cols].drop_duplicates(subset=["station_id", "predicted_at_utc"])

    save_staging_csv(df_out, os.path.join(STAGING_DIR, "tide_forecast_stg.csv"))

    print(f"\n  [품질 플래그 분포]")
    print(df_out["quality_flag"].value_counts().to_string())
    print(f"\n  [극값 분포]")
    print(df_out["extr_kind"].value_counts().to_string())
    print(f"\n  레코드 수: {len(df_out)}건")


if __name__ == "__main__":
    preprocess_tide_forecast()
