"""
기상청 BUOY 파고 수집기 — KMA APIHUB
API: https://apihub.kma.go.kr/api/typ01/url/kma_buoy.php
저장 위치: data/raw/wave/wave_obs_<날짜>_raw.json

관측소: STN 22189 (울산 부이)
수집 항목: 유의파고·최대파고·평균파고, 파주기, 파향,
           풍향·풍속(듀얼센서)·돌풍, 기온, 해수온도, 기압, 습도
수집 주기: 매 30분마다
"""

import json
import os
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv

load_dotenv()

AUTH_KEY = os.getenv("KMA_BUOY_AUTH_KEY", "")
BASE_URL = "https://apihub.kma.go.kr/api/typ01/url/kma_buoy.php"
RAW_DIR = "data/raw/wave"

# 울산 부이 지점
ULSAN_STN = "22189"

# 컬럼 순서 (help=1 문서 기준)
COLUMNS = [
    "tm_kst", "stn_id",
    "wind_dir1_deg", "wind_speed1_ms", "gust1_ms",
    "wind_dir2_deg", "wind_speed2_ms", "gust2_ms",
    "air_pressure_hpa", "humidity_pct", "air_temp_c", "sea_temp_c",
    "wave_height_max_m", "wave_height_sig_m", "wave_height_avg_m",
    "wave_period_s", "wave_dir_deg",
]
MISSING = {"-99", "-99.0", "-99.00"}


def _parse_line(line: str) -> dict | None:
    parts = line.split()
    if len(parts) < len(COLUMNS):
        return None
    rec = {}
    for col, val in zip(COLUMNS, parts):
        rec[col] = None if val in MISSING else val
    return rec


def fetch_buoy(stn: str = ULSAN_STN) -> list[dict]:
    """현재 시각 기준 부이 관측 1회 조회 (tm 생략 → 최신 자료)"""
    r = requests.get(
        BASE_URL,
        params={"stn": stn, "authKey": AUTH_KEY},
        timeout=15,
    )
    r.raise_for_status()

    records = []
    for line in r.text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        rec = _parse_line(line)
        if rec and rec.get("stn_id") == stn:
            records.append(rec)
    return records


def collect_wave_raw() -> None:
    os.makedirs(RAW_DIR, exist_ok=True)
    now_kst = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=9)
    today = now_kst.strftime("%Y%m%d")
    output_path = os.path.join(RAW_DIR, f"wave_obs_{today}_raw.json")

    collected_at_utc = datetime.now(timezone.utc).replace(tzinfo=None).isoformat() + "Z"

    print(f"[파고 수집] STN {ULSAN_STN} 울산 부이")
    records = fetch_buoy(ULSAN_STN)
    print(f"  → {len(records)}건")

    # 기존 파일에 누적 (하루 동안 10분 간격으로 호출 시)
    existing = []
    if os.path.exists(output_path):
        with open(output_path, encoding="utf-8") as f:
            existing = json.load(f).get("data", [])

    # tm_kst 기준 중복 제거 후 병합
    seen = {r["tm_kst"] for r in existing if r.get("tm_kst")}
    new_records = [r for r in records if r.get("tm_kst") not in seen]
    all_records = existing + new_records

    payload = {
        "collected_at_utc": collected_at_utc,
        "source": "KMA_APIHUB_BUOY",
        "station": {"stn_id": ULSAN_STN, "stn_name": "울산"},
        "record_count": len(all_records),
        "data": all_records,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"  [저장] {output_path}  (누적 {len(all_records)}건, 신규 {len(new_records)}건)")


if __name__ == "__main__":
    collect_wave_raw()
