"""
파고 데이터 전처리기
raw → staging: data/staging/wave_obs_stg.csv

출처: 기상청 APIHUB KMA BUOY API
관측소: STN 22189 울산 부이
수집 항목: 유의파고·최대파고·평균파고, 파주기, 파향,
           풍향·풍속(듀얼센서)·돌풍, 기온, 해수온도, 기압, 습도
"""

import glob
import json
import os
import sys
from datetime import timezone

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from common_preprocessing import (
    add_common_metadata,
    flag_missing_key,
    normalize_nulls,
    save_staging_csv,
    to_numeric_safe,
)

RAW_DIR = "data/raw/wave"
STAGING_DIR = "data/staging"

NUMERIC_COLS = [
    "wind_dir1_deg", "wind_speed1_ms", "gust1_ms",
    "wind_dir2_deg", "wind_speed2_ms", "gust2_ms",
    "air_pressure_hpa", "humidity_pct", "air_temp_c", "sea_temp_c",
    "wave_height_max_m", "wave_height_sig_m", "wave_height_avg_m",
    "wave_period_s", "wave_dir_deg",
]

FINAL_COLS = [
    "station_id", "station_name",
    "observed_at_utc",
    "wave_height_sig_m", "wave_height_max_m", "wave_height_avg_m",
    "wave_period_s", "wave_dir_deg",
    "wind_dir1_deg", "wind_speed1_ms", "gust1_ms",
    "wind_dir2_deg", "wind_speed2_ms", "gust2_ms",
    "air_temp_c", "sea_temp_c", "air_pressure_hpa", "humidity_pct",
    "source_system", "source_table", "collected_at_utc",
    "quality_flag", "is_synthetic",
]


def _parse_tm_kst_to_utc(val) -> pd.Timestamp | None:
    """YYYYMMDDHHMI (KST) → UTC Timestamp"""
    if pd.isna(val):
        return None
    s = str(val).strip()
    if len(s) < 12:
        return None
    try:
        dt_kst = pd.Timestamp(
            year=int(s[0:4]), month=int(s[4:6]), day=int(s[6:8]),
            hour=int(s[8:10]), minute=int(s[10:12]),
            tz="Asia/Seoul",
        )
        return dt_kst.tz_convert("UTC")
    except Exception:
        return None


def load_all_raw() -> pd.DataFrame:
    files = sorted(glob.glob(os.path.join(RAW_DIR, "wave_obs_*_raw.json")))
    if not files:
        raise FileNotFoundError(f"raw 파일이 없습니다: {RAW_DIR}")

    all_records = []
    for f_path in files:
        with open(f_path, encoding="utf-8") as f:
            payload = json.load(f)
        records = payload.get("data", [])
        stn_info = payload.get("station", {})
        for r in records:
            r["station_id"] = stn_info.get("stn_id", r.get("stn_id", ""))
            r["station_name"] = stn_info.get("stn_name", "")
        all_records.extend(records)
        print(f"  [로드] {f_path}  ({len(records)}건)")

    return pd.DataFrame(all_records)


def validate_wave(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "wave_height_sig_m" in df.columns:
        invalid = ~df["wave_height_sig_m"].between(0, 20)
        df.loc[invalid & df["wave_height_sig_m"].notna(), "quality_flag"] = "INVALID_WAVE_HEIGHT"
    if "wave_dir_deg" in df.columns:
        invalid = ~df["wave_dir_deg"].between(0, 360)
        df.loc[invalid & df["wave_dir_deg"].notna(), "quality_flag"] = "INVALID_WAVE_DIR"
    if "wind_speed1_ms" in df.columns:
        invalid = df["wind_speed1_ms"] < 0
        df.loc[invalid & df["wind_speed1_ms"].notna(), "quality_flag"] = "INVALID_WIND_SPEED"
    return df


def preprocess_wave() -> None:
    os.makedirs(STAGING_DIR, exist_ok=True)

    df = load_all_raw()
    print(f"  raw 전체 레코드: {len(df)}건")

    # 결측값 정규화
    df = normalize_nulls(df)

    # 숫자형 변환
    df = to_numeric_safe(df, NUMERIC_COLS)

    # KST → UTC 변환
    df["observed_at_utc"] = df["tm_kst"].apply(_parse_tm_kst_to_utc)

    # 공통 메타데이터
    df = add_common_metadata(
        df,
        source_system="KMA_APIHUB_BUOY",
        source_table="kma_buoy",
        is_synthetic=False,
    )

    # 기본키 결측 체크
    df = flag_missing_key(df, ["station_id", "observed_at_utc"])

    # 값 범위 검증
    df = validate_wave(df)

    # 최종 컬럼 정리 및 중복 제거
    out_cols = [c for c in FINAL_COLS if c in df.columns]
    df_out = df[out_cols].drop_duplicates(subset=["station_id", "observed_at_utc"])

    save_staging_csv(df_out, os.path.join(STAGING_DIR, "wave_obs_stg.csv"))

    print(f"\n  [품질 플래그 분포]")
    print(df_out["quality_flag"].value_counts().to_string())
    print(f"\n  레코드 수: {len(df_out)}건")


if __name__ == "__main__":
    preprocess_wave()
