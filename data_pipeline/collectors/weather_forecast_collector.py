"""
기상청 단기예보 수집기 — 공공데이터포털 VilageFcstInfoService_2.0/getVilageFcst
API: http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst
저장 위치: data/raw/weather_forecast/weather_forecast_<발표시각>_raw.json

격자: 울산항 nx=102, ny=84 (기상청 고유 격자좌표계, 위경도 아님)
수집 항목: 이 API 응답에 포함된 전체 카테고리 중 기상분석 에이전트가 쓰는
           WSD(풍속)·WAV(파고)를 포함해 TMP·PTY·SKY·POP 등도 원본 그대로 저장한다
           (선별은 전처리 단계에서).
수집 주기: 하루 8회 발표(02·05·08·11·14·17·20·23시, KST). 발표마다 이후 최대
           3일치를 3시간 간격으로 제공한다.

관측(observation)이 아니라 예측(forecast) 데이터다 — weather_obs/wave_obs와
혼동하지 않도록 raw 저장 경로와 컬럼 접두어를 분리했다.
"""

import json
import os
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("KMA_API_KEY", "")
BASE_URL = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst"
RAW_DIR = "data/raw/weather_forecast"

# 울산항 격자좌표 (위경도 아님, 기상청 고유 좌표계)
ULSAN_PORT_NX = 102
ULSAN_PORT_NY = 84

KST = timezone(timedelta(hours=9))

# 하루 8회 발표 시각 (KST, HHMM)
BASE_TIMES = ["0200", "0500", "0800", "1100", "1400", "1700", "2000", "2300"]

# 발표 후 실제로 조회 가능해지기까지의 여유 시간. 발표 직후(수 분~수십 분)에는
# 자료가 아직 준비되지 않아 빈 응답이 오는 경우가 있어 보수적으로 40분을 둔다.
PUBLISH_DELAY = timedelta(minutes=40)


def latest_base_datetime(now_kst: datetime) -> tuple[str, str]:
    """현재 시각(KST) 기준으로 이미 발표·조회 가능한 가장 최근 baseDate/baseTime을 계산한다."""
    candidates = []
    for day_offset in (0, -1):
        day = (now_kst + timedelta(days=day_offset)).date()
        for t in BASE_TIMES:
            hh, mm = int(t[:2]), int(t[2:])
            candidates.append(datetime(day.year, day.month, day.day, hh, mm, tzinfo=KST))

    available = [c for c in candidates if c + PUBLISH_DELAY <= now_kst]
    if not available:
        raise RuntimeError(f"조회 가능한 발표 시각을 찾을 수 없습니다 (now_kst={now_kst})")

    chosen = max(available)
    return chosen.strftime("%Y%m%d"), chosen.strftime("%H%M")


def fetch_forecast(nx: int = ULSAN_PORT_NX, ny: int = ULSAN_PORT_NY) -> list[dict]:
    """가장 최근 발표분의 예보 항목 전체(item 배열)를 가져온다."""
    now_kst = datetime.now(KST)
    base_date, base_time = latest_base_datetime(now_kst)

    params = {
        "serviceKey": API_KEY,
        "dataType": "JSON",
        "numOfRows": "1000",
        "pageNo": "1",
        "base_date": base_date,
        "base_time": base_time,
        "nx": nx,
        "ny": ny,
    }
    resp = requests.get(BASE_URL, params=params, timeout=15)
    resp.raise_for_status()
    body = resp.json()

    header = body.get("response", {}).get("header", {})
    result_code = header.get("resultCode", "")
    if result_code != "00":
        raise RuntimeError(f"getVilageFcst 오류: {header.get('resultMsg', result_code)}")

    items = body.get("response", {}).get("body", {}).get("items", {}).get("item", [])
    return items


def collect_weather_forecast_raw() -> str:
    os.makedirs(RAW_DIR, exist_ok=True)

    print(f"[단기예보 수집] 울산항 격자 nx={ULSAN_PORT_NX} ny={ULSAN_PORT_NY}")
    items = fetch_forecast()
    print(f"  -> {len(items)}건 (카테고리 x 예보시각 조합 전체)")

    collected_at_utc = datetime.now(timezone.utc).isoformat()
    base_date = items[0]["baseDate"] if items else ""
    base_time = items[0]["baseTime"] if items else ""

    payload = {
        "collected_at_utc": collected_at_utc,
        "source": "KMA_VILAGE_FCST",
        "grid": {"nx": ULSAN_PORT_NX, "ny": ULSAN_PORT_NY, "name": "울산항"},
        "base_date": base_date,
        "base_time": base_time,
        "record_count": len(items),
        "data": items,
    }

    output_path = os.path.join(RAW_DIR, f"weather_forecast_{base_date}_{base_time}_raw.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"  [저장] {output_path}")
    return output_path


if __name__ == "__main__":
    collect_weather_forecast_raw()
