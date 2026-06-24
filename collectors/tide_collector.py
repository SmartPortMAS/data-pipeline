"""
조위 실시간 수집기 — 공공데이터포탈 국립해양조사원
API: https://apis.data.go.kr/1192136/dtRecent/GetDTRecentApiService
저장 위치: data/raw/tide/tide_obs_<날짜>_raw.json

관측소: 울산 조위관측소 (DT_0020)

응답 항목: obsrvnDt, bscTdlvHgt(조위), wndrct(풍향), wspd(풍속),
           maxMmntWspd(최대순간풍속), artmp(기온), atmpr(기압),
           wtem(수온), slntQty(염분), crdir(유향), crsp(유속)
"""

import json
import os
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

import requests
from dotenv import load_dotenv

load_dotenv()

_KHOA_API_KEY = os.getenv("KHOA_API_KEY", "")
API_KEY_ENCODED = quote(_KHOA_API_KEY, safe="")
BASE_URL = "https://apis.data.go.kr/1192136/dtRecent/GetDTRecentApiService"
RAW_DIR = "data/raw/tide"

ULSAN_STN = {
    "obs_code":  "DT_0020",
    "obs_name":  "울산",
    "latitude":  35.50194,
    "longitude": 129.38722,
}


def fetch_recent(obs_code: str, req_date: str | None = None, interval_min: int = 10) -> list:
    """GetDTRecentApiService 단일 호출 → item 리스트 반환

    Args:
        obs_code:     관측소 코드 (예: DT_0020)
        req_date:     요청일자 YYYYMMDD, None이면 현재일자
        interval_min: 시간 간격(분), 기본 60, 최댓값 60
    """
    params = {
        "serviceKey": API_KEY_ENCODED,
        "type":       "json",
        "obsCode":    obs_code,
        "min":        interval_min,
        "numOfRows":  300,
        "pageNo":     1,
    }
    if req_date:
        params["reqDate"] = req_date

    # serviceKey는 이미 URL 인코딩된 값이므로 직접 URL 조합
    qs = "&".join(
        f"{k}={v}" if k == "serviceKey" else f"{k}={v}"
        for k, v in params.items()
    )
    url = f"{BASE_URL}?{qs}"

    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    body = resp.json()

    result_code = body.get("header", {}).get("resultCode", "")
    if result_code != "00":
        msg = body.get("header", {}).get("resultMsg", "")
        raise RuntimeError(f"API 오류: resultCode={result_code} msg={msg}")

    items = body.get("body", {}).get("items", {}).get("item", [])
    if isinstance(items, dict):
        items = [items]
    return items or []


def collect_tide_raw(days_back: int = 0) -> None:
    os.makedirs(RAW_DIR, exist_ok=True)
    now_kst = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=9)
    collected_at_utc = datetime.now(timezone.utc).replace(tzinfo=None).isoformat() + "Z"

    for d in range(days_back + 1):
        target = (now_kst - timedelta(days=d)).strftime("%Y%m%d")
        output_path = os.path.join(RAW_DIR, f"tide_obs_{target}_raw.json")

        print(f"[조위 수집] {ULSAN_STN['obs_name']} ({ULSAN_STN['obs_code']}) {target}")

        try:
            items = fetch_recent(
                obs_code=ULSAN_STN["obs_code"],
                req_date=target,
                interval_min=10,
            )
        except Exception as e:
            print(f"  [오류] {e}")
            continue

        for item in items:
            item.update({
                "_obs_code":  ULSAN_STN["obs_code"],
                "_obs_name":  ULSAN_STN["obs_name"],
                "_latitude":  ULSAN_STN["latitude"],
                "_longitude": ULSAN_STN["longitude"],
            })

        print(f"  → {len(items)}건")

        # 기존 파일에 누적 (obsrvnDt 기준 중복 제거)
        existing = []
        if os.path.exists(output_path):
            with open(output_path, encoding="utf-8") as f:
                existing = json.load(f).get("data", [])

        seen = {r.get("obsrvnDt") for r in existing if r.get("obsrvnDt")}
        new_items = [r for r in items if r.get("obsrvnDt") not in seen]
        all_data = existing + new_items

        payload = {
            "collected_at_utc": collected_at_utc,
            "source":           "KDPA_KHOA_dtRecent",
            "query_date_kst":   target,
            "station":          ULSAN_STN,
            "record_count":     len(all_data),
            "is_synthetic":     False,
            "data":             all_data,
        }

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        print(f"  [저장] {output_path}  (누적 {len(all_data)}건, 신규 {len(new_items)}건)")


if __name__ == "__main__":
    collect_tide_raw(days_back=0)
