"""
기상청 단기예보 전처리기
raw → staging: data/staging/weather_forecast_stg.csv

출처: 공공데이터포털 VilageFcstInfoService_2.0/getVilageFcst (울산항 격자 nx=102 ny=84)

raw는 (카테고리 x 예보시각) 조합마다 한 행이다 (예: WSD/20260719/0600 한 행,
WAV/20260719/0600 한 행, ...). 이 전처리기는 (nx, ny, fcstDate, fcstTime) 기준으로
피벗해 "예보 시각 하나당 한 행 x 카테고리별 컬럼" 형태로 만든다.
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
    save_staging_csv,
    to_numeric_safe,
)

RAW_DIR = "data/raw/weather_forecast"
STAGING_DIR = "data/staging"

# 기상분석 에이전트가 실제로 쓰는 항목 위주로 선별(전체 14종 중 6종). WSD=풍속(m/s),
# WAV=파고(m)가 rule_engine 판단에 쓰이는 핵심 항목이고 나머지 4개는 참고용이다.
# TMN/TMX(일 최저·최고기온)는 하루 중 특정 시각에만 값이 오는 항목이라(3시간 간격
# 예보 시각 전체에 없음) 이번 스코프에서는 제외했다.
CATEGORY_MAP = {
    "WSD": "wind_speed_ms",      # 풍속 (m/s) — rule_engine 임계값(14m/s) 판단에 사용
    "WAV": "wave_height_m",      # 파고 (m)   — rule_engine 임계값(1.5m) 판단에 사용
    "TMP": "air_temp_c",         # 1시간 기온 (℃) — 참고용
    "PTY": "precip_type_code",   # 강수형태 코드값 (0:없음 1:비 2:비/눈 3:눈 4:소나기 등) — 참고용
    "SKY": "sky_code",           # 하늘상태 코드값 (1:맑음 3:구름많음 4:흐림) — 참고용
    "POP": "precip_prob_pct",    # 강수확률 (%) — 참고용
}


def _parse_yyyymmddhhmm_kst_to_utc(date_col: pd.Series, time_col: pd.Series) -> pd.Series:
    """YYYYMMDD + HHMM(KST) -> UTC Timestamp."""
    combined = date_col.astype(str).str.zfill(8) + time_col.astype(str).str.zfill(4)
    dt = pd.to_datetime(combined, format="%Y%m%d%H%M", errors="coerce")
    return dt.dt.tz_localize("Asia/Seoul", nonexistent="NaT", ambiguous="NaT").dt.tz_convert("UTC")


def load_all_raw() -> pd.DataFrame:
    files = sorted(glob.glob(os.path.join(RAW_DIR, "weather_forecast_*_raw.json")))
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


def preprocess_weather_forecast() -> None:
    os.makedirs(STAGING_DIR, exist_ok=True)

    df = load_all_raw()
    print(f"  raw 전체 레코드(카테고리 x 예보시각): {len(df)}건")

    # 관심 카테고리만 남긴다
    df = df[df["category"].isin(CATEGORY_MAP.keys())].copy()

    # (nx, ny, baseDate, baseTime, fcstDate, fcstTime) 그룹당 한 행으로 피벗
    index_cols = ["nx", "ny", "baseDate", "baseTime", "fcstDate", "fcstTime"]
    pivoted = df.pivot_table(
        index=index_cols,
        columns="category",
        values="fcstValue",
        aggfunc="first",
    ).reset_index()
    pivoted.columns.name = None
    pivoted = pivoted.rename(columns=CATEGORY_MAP)

    # 시각 파싱 (KST -> UTC)
    pivoted["fcst_at_utc"] = _parse_yyyymmddhhmm_kst_to_utc(pivoted["fcstDate"], pivoted["fcstTime"])
    pivoted["base_at_utc"] = _parse_yyyymmddhhmm_kst_to_utc(pivoted["baseDate"], pivoted["baseTime"])
    pivoted = pivoted.drop(columns=["baseDate", "baseTime", "fcstDate", "fcstTime"])

    # 숫자형 변환
    numeric_cols = [c for c in CATEGORY_MAP.values() if c in pivoted.columns]
    pivoted = to_numeric_safe(pivoted, ["nx", "ny", *numeric_cols])

    # 공통 메타데이터
    pivoted = add_common_metadata(
        pivoted,
        source_system="KMA_VILAGE_FCST",
        source_table="getVilageFcst",
        is_synthetic=False,
    )

    # 기본키 결측 체크: 이 예보가 어느 격자·어느 미래 시각인지는 반드시 있어야 함
    pivoted = flag_missing_key(pivoted, ["nx", "ny", "fcst_at_utc"])

    final_cols = [
        "nx", "ny", "base_at_utc", "fcst_at_utc",
        "wind_speed_ms", "wave_height_m", "air_temp_c",
        "precip_type_code", "sky_code", "precip_prob_pct",
        "source_system", "source_table", "collected_at_utc",
        "quality_flag", "is_synthetic",
    ]
    out_cols = [c for c in final_cols if c in pivoted.columns]
    df_out = pivoted[out_cols].drop_duplicates(subset=["nx", "ny", "fcst_at_utc"], keep="last")

    save_staging_csv(df_out, os.path.join(STAGING_DIR, "weather_forecast_stg.csv"))


if __name__ == "__main__":
    preprocess_weather_forecast()
