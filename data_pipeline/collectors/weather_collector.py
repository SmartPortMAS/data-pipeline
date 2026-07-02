"""
항만기상 실시간 수집기 — 해양수산부 항만기상정보시스템
API: http://marineweather.nmpnt.go.kr:8001/openWeatherNow.do
저장 위치: data/raw/weather/weather_obs_<날짜>_raw.json

관측소: 울산항동방파제서단등대 (울산청 104 / 지점 1041519)
수집 항목: 풍향, 풍속, 기온, 습도, 기압, 시정
수집 주기: 실시간 (DATETIME 기준)
"""

import json
import os
from datetime import datetime, timezone, timedelta

import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("MMAF_API_KEY", "")
BASE_URL = "http://marineweather.nmpnt.go.kr:8001/openWeatherNow.do"
RAW_DIR = "data/raw/weather"

STATION = {
    "mmaf": "104",
    "mmaf_name": "울산청",
    "mmsi": "1041519",
    "mmsi_name": "울산항동방파제서단등대",
    "latitude": 35.4665,
    "longitude": 129.399889,
}


def fetch_weather_now() -> dict:
    """항만기상 현재 관측값 조회 (dataType=2: 전항목)"""
    params = {
        "serviceKey": API_KEY,
        "resultType": "json",
        "mmaf": STATION["mmaf"],
        "mmsi": STATION["mmsi"],
        "dataType": "1",
    }
    try:
        resp = requests.get(BASE_URL, params=params, timeout=10)
        resp.raise_for_status()
        body = resp.json()

        status = body.get("result", {}).get("status", "")
        if status != "OK":
            msg = body.get("result", {}).get("message", "")
            print(f"  [항만기상 API] status={status} message={msg}")
            return {}

        records = body.get("result", {}).get("recordset", [])
        return records[0] if records else {}

    except requests.exceptions.RequestException as e:
        print(f"  [항만기상 API 오류] {e}")
        return {}
    except (KeyError, ValueError) as e:
        print(f"  [파싱 오류] {e}")
        return {}


def collect_weather_raw() -> None:
    os.makedirs(RAW_DIR, exist_ok=True)
    KST = timezone(timedelta(hours=9))
    now_utc = datetime.now(timezone.utc)
    collected_at_utc = now_utc.isoformat()
    today = now_utc.astimezone(KST).strftime("%Y%m%d")

    print(f"[항만기상 수집] {STATION['mmsi_name']} ({STATION['mmsi']})")

    record = fetch_weather_now()

    if not record:
        print("  -> 데이터 없음")
        return

    obs_time = record.get("DATETIME", "")
    print(f"  -> 관측시각: {obs_time}")
    print(f"  -> 풍향: {record.get('WIND_DIRECT')}° / 풍속: {record.get('WIND_SPEED')} m/s")
    print(f"  -> 기온: {record.get('AIR_TEMPERATURE')}°C / 습도: {record.get('HUMIDITY')}%")
    print(f"  -> 기압: {record.get('AIR_PRESSURE')} hPa / 시정: {record.get('HORIZON_VISIBL')} m")

    output_path = os.path.join(RAW_DIR, f"weather_obs_{today}_raw.json")

    # 당일 파일이 이미 있으면 records 배열에 append
    if os.path.exists(output_path):
        with open(output_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        payload["data"].append(record)
        payload["record_count"] = len(payload["data"])
        payload["last_collected_utc"] = collected_at_utc
    else:
        payload = {
            "collected_at_utc": collected_at_utc,
            "last_collected_utc": collected_at_utc,
            "source": "MMPNT_MARINEWEATHER",
            "station": STATION,
            "record_count": 1,
            "data": [record],
        }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"  [저장] {output_path}")


if __name__ == "__main__":
    collect_weather_raw()
