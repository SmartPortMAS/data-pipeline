"""
기상청 BUOY 파고 수집기 — KMA APIHUB
API: https://apihub.kma.go.kr/api/typ01/url/kma_buoy.php
저장 위치: data/raw/wave/wave_obs_<날짜>_raw.json

관측소: STN 22189 (울산 부이)
수집 항목: 유의파고·최대파고·평균파고, 파주기, 파향,
           풍향·풍속(듀얼센서)·돌풍, 기온, 해수온도, 기압, 습도
수집 주기: 매 30분마다

과거 조회(백필)
  `tm=YYYYMMDDHHMI` 를 주면 그 시각의 관측을 돌려준다(실측 2026-09-20 확인).
  한 번 호출하면 **직전 30분 자료와 해당 정시 자료 2건**이 온다 —
  tm=202609091200 -> [202609091130, 202609091200]. 그래서 **정시마다 한 번씩만**
  부르면 30분 간격 전체가 덮인다(하루 24회).

  구간 조회(tm1/tm2)는 쓸 수 없다 — stn 과 같이 주면 무시되고 최신값만 오고,
  stn 없이 주면 504 로 끊긴다(둘 다 2026-09-20 실측).
"""

import json
import os
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

from data_pipeline.common_http import get_with_retry

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


def fetch_buoy(stn: str = ULSAN_STN, tm: str | None = None,
               timeout: int = 30, retries: int = 3) -> list[dict]:
    """부이 관측 1회 조회.

    tm 생략 -> 최신 자료. tm="YYYYMMDDHHMI" -> 그 시각 자료(직전 30분 포함 2건).

    과거 조회는 현재값 조회보다 느려서 이따금 읽기 시간이 초과된다(실측: 20초
    타임아웃에서 간헐 실패, 60초로 올리면 통과). 백필은 수백 번 호출하므로
    한 번 끊겼다고 그날치를 통째로 버리지 않도록 짧게 재시도한다.
    """
    params = {"stn": stn, "authKey": AUTH_KEY}
    if tm:
        params["tm"] = tm

    # 재시도는 공통 헬퍼로(2026-09-24). 예전 루프는 마지막 실패에서 requests 예외를
    # 그대로 올려, 문구에 담긴 요청 URL 과 함께 authKey 가 로그로 샜다.
    r = get_with_retry(BASE_URL, params=params, timeout=timeout, retries=retries,
                       label=f"KMA 부이 stn={stn}" + (f" tm={tm}" if tm else ""))

    records = []
    for line in r.text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        rec = _parse_line(line)
        if rec and rec.get("stn_id") == stn:
            records.append(rec)
    return records


def _write_day(day: str, records: list[dict], collected_at_utc: str) -> None:
    """하루치 레코드를 그 날짜의 raw 파일에 tm_kst 기준으로 누적 병합한다."""
    output_path = os.path.join(RAW_DIR, f"wave_obs_{day}_raw.json")

    existing = []
    if os.path.exists(output_path):
        with open(output_path, encoding="utf-8") as f:
            existing = json.load(f).get("data", [])

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


def collect_wave_raw(days_back: int = 0) -> None:
    """부이 관측 수집.

    days_back=0 (기본) 이면 지금 시각의 최신 자료만 받는다 — 매시 정시 수집의 동작이다.
    days_back=N 이면 **오늘 포함 N+1 일**을 정시마다 조회해 과거를 메운다.

    왜 정시마다인가
      한 번 호출에 [직전 30분, 해당 정시] 2건이 오므로(모듈 docstring 참고) 정시
      24회면 하루 48건이 전부 덮인다. 30분마다 부르면 호출만 두 배가 되고 얻는
      자료는 같다.

    레코드는 **자기 tm_kst 의 날짜** 파일에 넣는다 — tm=YYYYMMDD0000 호출은 전날
    23:30 자료를 함께 주므로, 호출한 날짜로 넣으면 그 한 건이 엉뚱한 날에 쌓인다.
    """
    os.makedirs(RAW_DIR, exist_ok=True)
    now_kst = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=9)
    collected_at_utc = datetime.now(timezone.utc).replace(tzinfo=None).isoformat() + "Z"

    if days_back <= 0:
        print(f"[파고 수집] STN {ULSAN_STN} 울산 부이 (최신)")
        records = fetch_buoy(ULSAN_STN)
        print(f"  → {len(records)}건")
        _write_day(now_kst.strftime("%Y%m%d"), records, collected_at_utc)
        return

    # ── 백필 ──
    by_day: dict[str, list[dict]] = {}
    calls = errors = 0
    for d in range(days_back + 1):
        target = (now_kst - timedelta(days=d)).strftime("%Y%m%d")
        day_hits = 0
        for hh in range(24):
            tm = f"{target}{hh:02d}00"
            if datetime.strptime(tm, "%Y%m%d%H%M") > now_kst:
                continue  # 아직 오지 않은 시각
            calls += 1
            try:
                for rec in fetch_buoy(ULSAN_STN, tm=tm, timeout=60):
                    stamp = rec.get("tm_kst") or ""
                    if len(stamp) >= 8:
                        by_day.setdefault(stamp[:8], []).append(rec)
                        day_hits += 1
            except Exception as e:
                errors += 1
                print(f"  [오류] tm={tm} {type(e).__name__}")
        print(f"[파고 백필] {target} → {day_hits}건")

    for day in sorted(by_day):
        # tm_kst 중복 제거 (정시 호출이 겹쳐 같은 레코드가 두 번 올 수 있다)
        uniq = {r["tm_kst"]: r for r in by_day[day] if r.get("tm_kst")}
        _write_day(day, list(uniq.values()), collected_at_utc)
    print("")
    print(f"[백필 완료] 호출 {calls}회 · 오류 {errors}회 · 날짜 {len(by_day)}일")


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="부이 파고 수집 (--days-back 으로 과거 백필)")
    ap.add_argument("--days-back", type=int, default=0,
                    help="오늘 포함 며칠 전까지 정시마다 메울지 (기본 0 = 최신만)")
    collect_wave_raw(days_back=ap.parse_args().days_back)
