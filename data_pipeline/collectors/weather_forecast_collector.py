"""
기상청 단기예보 수집기 — 공공데이터포털 VilageFcstInfoService_2.0/getVilageFcst
API: http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst
저장 위치: data/raw/weather_forecast/weather_forecast_<발표시각>_raw.json

수집 항목: 이 API 응답에 포함된 전체 카테고리 중 기상분석 에이전트가 쓰는
           WSD(풍속)·WAV(파고)를 포함해 TMP·PTY·SKY·POP 등도 원본 그대로 저장한다
           (선별은 전처리 단계에서).
수집 주기: 하루 8회 발표(02·05·08·11·14·17·20·23시, KST). 발표마다 이후 최대
           3일치를 3시간 간격으로 제공한다.

관측(observation)이 아니라 예측(forecast) 데이터다 — weather_obs/wave_obs와
혼동하지 않도록 raw 저장 경로와 컬럼 접두어를 분리했다.

───────────────────────────────────────────────────────────────────────────
[2026-09-21] 격자 정정 — nx=102, ny=84 는 울산항이 아니었다
───────────────────────────────────────────────────────────────────────────
증상: `weather_forecast.wave_height_m` 408행이 **전부 0.0** 이었다(실측).
      raw JSON 8개 파일 594개 WAV 값도 전부 문자열 '0' — 전처리 버그가 아니라
      API 가 실제로 0 을 돌려주고 있었다.

원인: 격자가 틀렸다. 기상청 공식 DFS(Lambert Conformal Conic) 변환으로
      울산항 좌표(35.4665N, 129.399889E — weather_collector.STATION 과 동일)를
      환산하면 **nx=103, ny=82** 다. 기존 (102, 84) 의 격자 중심은
      35.5502N / 129.3278E 로 **울산항에서 11.4 km 북서쪽 내륙**이다.
      내륙 격자는 WAV(파고)가 정의되지 않아 KMA 가 0 으로 응답한다.

실측 대조(2026-09-21 17:00 발표, 같은 호출·같은 시각):
      (102,84) 기존   WAV 0.0 ~ 0.0   WSD 0.2 ~  5.5   ← 내륙
      (103,82) 울산항  WAV 0.5 ~ 1.0   WSD 0.2 ~  7.1
      (104,82) 정박지E1 WAV 0.5 ~ 2.0   WSD 0.2 ~  8.5
      (105,82) 정박지E2 WAV 0.5 ~ 2.5   WSD 0.4 ~ 10.0

      ★ 파고보다 풍속이 더 심각하다. 풍속은 berth_weather_threshold 의
        하드 게이트(중단 12~17 m/s)에 직접 들어가는데, 내륙 격자는 해상보다
        최대 4.5 m/s 낮게 나온다 — 중단 판정이 조용히 누락된다.

조치: 격자를 좌표에서 계산한다(하드코딩 금지). 부두 판정용 울산항 격자와
      정박지/진입수로 판정용 해상 격자를 **둘 다** 수집한다 —
      weather_forecast 의 자연키가 (nx, ny, fcst_at_utc) 라 다중 격자가
      전처리·적재까지 그대로 흐른다(스키마 변경 불필요).
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

KST = timezone(timedelta(hours=9))

# ---------------------------------------------------------------------------
# 기상청 DFS 격자 변환 (Lambert Conformal Conic) — KMA 공개 알고리즘
#
# 격자를 손으로 적지 않고 좌표에서 계산한다. (102, 84) 를 손으로 적었다가
# 11.4 km 떨어진 내륙 격자를 두 달간 수집한 것이 이 함수를 만든 이유다
# (모듈 docstring 참고). 좌표는 다른 수집기·DB 와 대조 가능한 값만 쓴다.
# ---------------------------------------------------------------------------
_DFS = dict(RE=6371.00877, GRID=5.0, SLAT1=30.0, SLAT2=60.0,
            OLON=126.0, OLAT=38.0, XO=43, YO=136)


def latlon_to_grid(lat: float, lon: float) -> tuple[int, int]:
    """위경도 → 기상청 단기예보 격자 (nx, ny)."""
    import math

    deg = math.pi / 180.0
    re_ = _DFS["RE"] / _DFS["GRID"]
    sl1, sl2 = _DFS["SLAT1"] * deg, _DFS["SLAT2"] * deg
    olon, olat = _DFS["OLON"] * deg, _DFS["OLAT"] * deg

    sn = math.log(math.cos(sl1) / math.cos(sl2)) / math.log(
        math.tan(math.pi * 0.25 + sl2 * 0.5) / math.tan(math.pi * 0.25 + sl1 * 0.5))
    sf = (math.tan(math.pi * 0.25 + sl1 * 0.5) ** sn) * math.cos(sl1) / sn
    ro = re_ * sf / (math.tan(math.pi * 0.25 + olat * 0.5) ** sn)
    ra = re_ * sf / (math.tan(math.pi * 0.25 + lat * deg * 0.5) ** sn)

    theta = lon * deg - olon
    if theta > math.pi:
        theta -= 2.0 * math.pi
    if theta < -math.pi:
        theta += 2.0 * math.pi
    theta *= sn

    return (int(ra * math.sin(theta) + _DFS["XO"] + 0.5),
            int(ro - ra * math.cos(theta) + _DFS["YO"] + 0.5))


# 수집 지점 — 판정 용도가 다르므로 둘 다 받는다.
#   울산항   : 부두 하역/이안 판정용. 좌표는 weather_collector.STATION 과 동일
#              (울산항동방파제서단등대) — 항만기상 실측과 같은 지점이라 대조 가능.
#   정박지E2 : 정박지·진입수로 판정용 해상 격자. upa_anchorage 의 E2 폴리곤
#              중심(TEXT 라벨행 제외)에서 계산했다.
FORECAST_POINTS = [
    ("울산항", 35.466500, 129.399889),
    ("정박지해상", 35.435390, 129.464320),
]

# 하위 호환 — 기존 호출부가 참조하던 이름. 이제 계산값이다.
ULSAN_PORT_NX, ULSAN_PORT_NY = latlon_to_grid(35.466500, 129.399889)

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
    """FORECAST_POINTS 의 모든 격자를 한 파일에 모아 저장한다.

    응답 item 마다 nx/ny 가 들어 있고, 전처리기가 (nx, ny, ...) 로 피벗하며
    적재 자연키도 (nx, ny, fcst_at_utc) 라 격자를 늘려도 뒤 단계는 그대로다.
    """
    os.makedirs(RAW_DIR, exist_ok=True)

    all_items: list[dict] = []
    grids: list[dict] = []
    errors: list[str] = []
    for name, lat, lon in FORECAST_POINTS:
        nx, ny = latlon_to_grid(lat, lon)
        print(f"[단기예보 수집] {name} ({lat:.5f}, {lon:.5f}) -> 격자 nx={nx} ny={ny}")
        # 격자 하나가 실패해도 나머지는 저장한다(dev 2026-09-21). 정박지 격자가
        # 빠졌다고 부두 판정까지 끊기면 안 된다 — 전부 실패했을 때만 예외를 올린다.
        try:
            items = fetch_forecast(nx=nx, ny=ny)
        except Exception as e:  # noqa: BLE001
            print(f"  [실패] {e}")
            errors.append(f"{name}({nx},{ny}) {e}")
            continue
        wav = [i["fcstValue"] for i in items if i.get("category") == "WAV"]
        print(f"  -> {len(items)}건 | WAV {len(wav)}건 "
              f"(값 {sorted(set(wav))[:6] if wav else '없음'})")
        # 내륙 격자를 다시 집는 사고를 조용히 넘기지 않는다. 경고만 하고 계속
        # 진행한다 — 실제로 파고 0 이 정상인 날(잔잔한 날)도 있기 때문이다.
        if wav and set(wav) == {"0"}:
            print(f"  [경고] {name} 격자의 WAV 가 전부 0 이다. 내륙 격자일 수 있다 "
                  f"— 모듈 docstring 의 2026-09-21 격자 정정 참고")
        all_items.extend(items)
        grids.append({"nx": nx, "ny": ny, "name": name,
                      "latitude": lat, "longitude": lon})

    if not all_items:
        raise RuntimeError("단기예보 수집 전부 실패: " + "; ".join(errors))

    collected_at_utc = datetime.now(timezone.utc).isoformat()
    base_date = all_items[0]["baseDate"] if all_items else ""
    base_time = all_items[0]["baseTime"] if all_items else ""

    payload = {
        "collected_at_utc": collected_at_utc,
        "source": "KMA_VILAGE_FCST",
        "grid": grids[0] if grids else {},   # 하위 호환(단일 격자 시절 형식)
        "grids": grids,
        "base_date": base_date,
        "base_time": base_time,
        "record_count": len(all_items),
        "data": all_items,
    }
    items = all_items

    output_path = os.path.join(RAW_DIR, f"weather_forecast_{base_date}_{base_time}_raw.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"  [저장] {output_path}")
    return output_path


if __name__ == "__main__":
    collect_weather_forecast_raw()
