"""
기상청 단기예보 전처리기
raw → staging: data/staging/weather_forecast_stg.csv

출처: 공공데이터포털 VilageFcstInfoService_2.0/getVilageFcst
      (격자는 수집기 FORECAST_GRIDS — 온산항 부두 103,82 · 울산 시내 102,84. 격자는 행마다
       nx·ny 로 남으므로 여기서는 격자 수와 무관하게 그대로 처리한다)

raw는 (카테고리 x 예보시각) 조합마다 한 행이다 (예: WSD/20260719/0600 한 행,
WAV/20260719/0600 한 행, ...). 이 전처리기는 (nx, ny, fcstDate, fcstTime) 기준으로
피벗해 "예보 시각 하나당 한 행 x 카테고리별 컬럼" 형태로 만든다.
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
    save_staging_csv,
    to_numeric_safe,
)

RAW_DIR = "data/raw/weather_forecast"
STAGING_DIR = "data/staging"

# 기상분석 에이전트가 실제로 쓰는 항목 위주로 선별(전체 14종 중 7종). WSD=풍속(m/s),
# WAV=파고(m)가 rule_engine 판단에 쓰이는 핵심 항목이고 나머지는 참고용이다.
# TMN/TMX(일 최저·최고기온)는 하루 중 특정 시각에만 값이 오는 항목이라(3시간 간격
# 예보 시각 전체에 없음) 이번 스코프에서는 제외했다.
# PCP(강수량)는 원래 여기 없었다 — raw에는 있지만 값이 구간 텍스트("1mm 미만",
# "16.0mm", "강수없음" 등 혼재, 실측 확인)라 저장을 미뤄뒀던 항목. rule_engine
# 판정에는 아직 안 쓴다(임계값 출처 미검증) — precip_mm으로 참고용 저장만 한다.
CATEGORY_MAP = {
    "WSD": "wind_speed_ms",      # 풍속 (m/s) — rule_engine 임계값(14m/s) 판단에 사용
    "WAV": "wave_height_m",      # 파고 (m)   — rule_engine 임계값(1.5m) 판단에 사용
    "TMP": "air_temp_c",         # 1시간 기온 (℃) — 참고용
    "PTY": "precip_type_code",   # 강수형태 코드값 (0:없음 1:비 2:비/눈 3:눈 4:소나기 등) — 참고용
    "SKY": "sky_code",           # 하늘상태 코드값 (1:맑음 3:구름많음 4:흐림) — 참고용
    "POP": "precip_prob_pct",    # 강수확률 (%) — 참고용
    "PCP": "precip_mm",          # 강수량 (mm, 정규화됨) — 참고용, rule_engine 미사용
}

_PCP_NUMBER_RE = re.compile(r"(\d+(?:\.\d+)?)")


def normalize_pcp_mm(raw: object) -> float | None:
    """기상청 PCP(강수량) 원문을 mm 숫자로 정규화한다.

    KMA getVilageFcst의 PCP는 깔끔한 mm 숫자가 아니라 구간 텍스트로 온다(실측
    확인: '강수없음', '1mm 미만', '16.0mm', '4.0mm' 등이 한 응답 안에 혼재하고,
    "N.0~M.0mm" 범위 표기도 공식 문서화되어 있다). precip_mm은 rule_engine
    판정에 쓰지 않는 참고용 값이라 다음 근사 규칙으로 정규화한다:
      - '강수없음' -> 0.0
      - 'N ~ M mm' 범위 -> 중간값 (N+M)/2
      - 'N mm 미만' -> N의 절반 (0과 N 사이의 대략적 추정치)
      - 'Nmm'/순수 숫자('0', '1.3' 등, 모의 데이터 포함) -> N 그대로
    파싱 자체가 안 되는 값은 None(결측)으로 남긴다 — 임의 추정값을 만들지 않는다.
    """
    if raw is None:
        return None
    text = str(raw).strip()
    if text == "" or text.lower() in ("nan", "none"):
        return None
    if "없음" in text:
        return 0.0
    if "~" in text:
        numbers = _PCP_NUMBER_RE.findall(text)
        if len(numbers) == 2:
            return (float(numbers[0]) + float(numbers[1])) / 2
    if "미만" in text:
        numbers = _PCP_NUMBER_RE.findall(text)
        if numbers:
            return float(numbers[0]) / 2
    numbers = _PCP_NUMBER_RE.findall(text)
    if numbers:
        return float(numbers[0])
    return None


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

    # PCP(강수량)는 구간 텍스트라 일반 숫자 변환(to_numeric_safe) 전에 먼저
    # 정규화한다 — 그냥 to_numeric에 넣으면 '강수없음'/'1mm 미만' 등이 전부
    # NaN이 되어버린다.
    if "precip_mm" in pivoted.columns:
        pivoted["precip_mm"] = pivoted["precip_mm"].map(normalize_pcp_mm)

    # 숫자형 변환
    numeric_cols = [c for c in CATEGORY_MAP.values() if c in pivoted.columns and c != "precip_mm"]
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
        "precip_type_code", "sky_code", "precip_prob_pct", "precip_mm",
        "source_system", "source_table", "collected_at_utc",
        "quality_flag", "is_synthetic",
    ]
    out_cols = [c for c in final_cols if c in pivoted.columns]
    df_out = pivoted[out_cols].drop_duplicates(subset=["nx", "ny", "fcst_at_utc"], keep="last")

    save_staging_csv(df_out, os.path.join(STAGING_DIR, "weather_forecast_stg.csv"))


if __name__ == "__main__":
    preprocess_weather_forecast()
