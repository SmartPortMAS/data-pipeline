"""
조위 예보 수집기 — 공공데이터포털 국립해양조사원 조석예보
API: https://apis.data.go.kr/1192136/tideFcstHghLw/GetTideFcstHghLwApiService
저장 위치: data/raw/tide_forecast/tide_forecast_<날짜>_raw.json

관측소: 울산 조위관측소 (DT_0020) — tide_collector 와 같은 지점

왜 필요한가
  tide_obs 는 실측 전용이라 과거만 있다. 판정에 쓰는 "입항 예정 시각의 예상조위"와
  "체류 중 최저조"는 둘 다 미래 구간이라 실측으로 잴 수 없다.

응답 항목: predcDt(예보시각 KST), predcTdlvVl(예측조위 cm), extrSe(극값구분),
           obsvtrNm(관측소명), lat, lot

extrSe 는 1=제1고조 · 2=제1저조 · 3=제2고조 · 4=제2저조.
명세 문서(HWP)를 구할 수 없어 2026-09-20~10-01 울산 응답 45개 극값을 시간순
이웃과 대조해 확정했다 — 1·3 은 전부 극대, 2·4 는 전부 극소로 예외가 없었다.

하루 4건(간혹 3건)만 온다. 연속 곡선이 아니라 극값만이므로 임의 시각의 조위는
소비 측에서 보간한다.
"""

import json
import os
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

import requests
from dotenv import load_dotenv

load_dotenv()

_KHOA_API_KEY = os.getenv("KHOA_API_KEY", "")
API_KEY_ENCODED = quote(_KHOA_API_KEY, safe="")
BASE_URL = "https://apis.data.go.kr/1192136/tideFcstHghLw/GetTideFcstHghLwApiService"
RAW_DIR = "data/raw/tide_forecast"

ULSAN_STN = {
    "obs_code":  "DT_0020",
    "obs_name":  "울산",
    "latitude":  35.50194,
    "longitude": 129.38722,
}

# 며칠치를 받아둘지. 입항 판정은 72시간 앞을 보고(arrivals/upcoming), 체류 구간이
# 그 뒤로 더 이어지므로 여유를 둔다. API 는 12일 앞까지 응답하는 것을 확인했다.
DEFAULT_DAYS_AHEAD = 7


#: 게이트웨이 5xx 재시도. 1회면 충분하다 — 2026-09-22 새벽 관측에서 8일 요청 중
#: 5일이 504 였고, 같은 요청을 잠시 뒤 다시 보내면 정상 응답이 왔다. 포털 쪽
#: 순간 과부하라 간격을 길게 둘 이유는 없고, 하루 8회 호출이라 부하도 아니다.
_RETRY_5XX_DELAY_SEC = 1.0


def _safe_err(exc: Exception) -> str:
    """로그에 남겨도 되는 실패 문구.

    ★ 예외 문구를 그대로 쓰면 안 된다. requests 의 HTTPError 는 메시지에 **요청
      URL 전체**를 담는데, 이 API 는 serviceKey 를 쿼리스트링으로 받으므로
      인증키가 통째로 로그에 남는다. 상태 코드와 예외 이름만 남긴다.

      같은 결함이 backend 에도 있었다(twin.py 가 실패 사유에 httpx 예외 문구를
      넣어 serviceKey 가 API 응답으로 나갔다). backend 는 이 API 를 직접 부르지
      않게 되면서 없어졌고, 호출 지점은 이제 여기 하나다.
    """
    if isinstance(exc, requests.RequestException):
        # requests 계열만 URL 을 물고 있다. 상태 코드 + 예외 이름으로 줄인다.
        status = getattr(getattr(exc, "response", None), "status_code", None)
        return f"HTTP {status} ({type(exc).__name__})" if status else type(exc).__name__
    # 우리가 만든 문구(resultCode/resultMsg)는 키를 담지 않으므로 그대로 남긴다 —
    # 여기까지 지워 버리면 "왜 실패했는지"가 로그에서 사라진다.
    return f"{type(exc).__name__}: {exc}"


def fetch_forecast(obs_code: str, req_date: str) -> list:
    """GetTideFcstHghLwApiService 단일 호출 → item 리스트 반환

    5xx 는 한 번 더 시도한다(`_RETRY_5XX_DELAY_SEC`). 4xx·JSON 오류는 다시 물어도
    같은 답이 오므로 재시도하지 않는다.

    Args:
        obs_code: 관측소 코드 (예: DT_0020)
        req_date: 요청일자 YYYYMMDD
    """
    params = {
        "serviceKey": API_KEY_ENCODED,
        "type":       "json",
        "obsCode":    obs_code,
        "reqDate":    req_date,
        "numOfRows":  50,
        "pageNo":     1,
    }
    # serviceKey는 이미 URL 인코딩된 값이므로 직접 URL 조합 (tide_collector 와 동일)
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    url = f"{BASE_URL}?{qs}"

    for attempt in (1, 2):
        resp = requests.get(url, timeout=15)
        if resp.status_code >= 500 and attempt == 1:
            print(f"  [재시도] HTTP {resp.status_code} — {_RETRY_5XX_DELAY_SEC}초 뒤 한 번 더")
            time.sleep(_RETRY_5XX_DELAY_SEC)
            continue
        break

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


def collect_tide_forecast_raw(days_ahead: int = DEFAULT_DAYS_AHEAD) -> None:
    """오늘부터 days_ahead 일 뒤까지 하루 단위로 받아 날짜별 파일로 저장한다."""
    os.makedirs(RAW_DIR, exist_ok=True)
    now_kst = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=9)
    collected_at_utc = datetime.now(timezone.utc).replace(tzinfo=None).isoformat() + "Z"

    for d in range(days_ahead + 1):
        target = (now_kst + timedelta(days=d)).strftime("%Y%m%d")
        output_path = os.path.join(RAW_DIR, f"tide_forecast_{target}_raw.json")

        print(f"[조위예보 수집] {ULSAN_STN['obs_name']} ({ULSAN_STN['obs_code']}) {target}")

        try:
            items = fetch_forecast(obs_code=ULSAN_STN["obs_code"], req_date=target)
        except Exception as e:
            # 예외 문구를 그대로 찍지 않는다 — serviceKey 가 로그로 샌다(_safe_err).
            print(f"  [오류] {_safe_err(e)}")
            continue

        for item in items:
            item.update({
                "_obs_code":  ULSAN_STN["obs_code"],
                "_obs_name":  ULSAN_STN["obs_name"],
                "_latitude":  ULSAN_STN["latitude"],
                "_longitude": ULSAN_STN["longitude"],
            })

        print(f"  → {len(items)}건")

        # 예보는 조화분해 결과라 같은 날짜를 다시 받아도 값이 같다. 누적이 아니라
        # 통째로 덮어쓴다 — 조석 상수가 갱신되면 최신값이 이기는 게 맞다.
        payload = {
            "collected_at_utc": collected_at_utc,
            "source":           "KDPA_KHOA_tideFcstHghLw",
            "query_date_kst":   target,
            "station":          ULSAN_STN,
            "record_count":     len(items),
            "is_synthetic":     False,
            "data":             items,
        }

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        print(f"  [저장] {output_path}  ({len(items)}건)")


if __name__ == "__main__":
    collect_tide_forecast_raw()
