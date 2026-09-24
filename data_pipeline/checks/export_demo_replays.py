# -*- coding: utf-8 -*-
"""시연 재생 데이터 추출 — 실제로 있었던 날을 세 시점 판정으로 다시 보여준다.

만드는 것: frontend/public/demo/replays.json (프론트 "입항 예정 · 검증" 화면이 읽음)

  1) 조위로 판정이 바뀐 선박 — GINGA MARGAY 8/18 OTK1부두 (적합 -> 주의)
  2) 표 수심으로는 불가능한데 실제 접안한 선박 — SEA DRAGON 9/11 S-Oil 1부두 (확인 요청)
  3) 외해 너울 기간의 입항 변화 — 8/18~9/16 일별 액체화물선 입항 수 × 외해 부이 파고

판정 규칙은 backtest_false_alarm.py 의 S3 단계와 같다(조위 반영 흘수 여유 1.0m,
체류 중 최저 여유 >= 1.0 적합 · 0~1.0 주의 · < 0 확인 요청). 흘수·선석·입출항 시각은
그 스크립트의 --json 결과를 입력으로 받는다 — 같은 표본을 두 번 계산하지 않는다.

조위: "입항 전" 단계는 운영 시 조석 예보를 써야 하지만, 과거 날짜의 예보 기록이 없어
재생에서는 그 시각의 실측 조위를 쓴다. 화면에 그렇게 표시한다.

실행 (data-pipeline 폴더):
  python -m data_pipeline.checks.backtest_false_alarm --json data/state/backtest_rows.json
  python -m data_pipeline.checks.export_demo_replays --rows data/state/backtest_rows.json
"""
import argparse
import collections
import datetime as dt
import json
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ROOT_DIR = os.path.dirname(BASE_DIR)
sys.path.insert(0, BASE_DIR)

KST = dt.timezone(dt.timedelta(hours=9))
UTC = dt.timezone.utc
MARGIN_M = 1.0
OUT_PATH = os.path.join(ROOT_DIR, "frontend", "public", "demo", "replays.json")
LIQ_WORDS = ("석유제품", "케미칼", "LPG", "LNG", "원유", "유조", "급유")
REPORT_RANK = {"최초": 0, "변경": 1, "최종": 2}

# 시연에 쓰는 선박 — (호출부호가 아니라) 이름과 입항일로 표본에서 찾는다
CASES = [
    dict(id="ginga-margay-0818", name="GINGA MARGAY", date="2026-08-18",
         headline="접안 때는 적합, 하역 중 저조에서 주의로 바뀐 날"),
    dict(id="sea-dragon-0911", name="SEA DRAGON", date="2026-09-11",
         headline="표 수심으로는 불가능한데 실제로 접안한 날 — 단정하지 않고 확인을 요청"),
]
SWELL = dict(id="swell-0904-0911", start="2026-08-18", end="2026-09-16", threshold_m=2.0)


def _level_for_margin(m):
    if m is None:
        return "판정불가"
    if m >= MARGIN_M:
        return "적합"
    if m >= 0:
        return "주의"
    return "부적합"


def _iso_kst(t):
    return t.astimezone(KST).isoformat(timespec="minutes")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", required=True, help="backtest_false_alarm.py --json 결과")
    ap.add_argument("--out", default=OUT_PATH)
    args = ap.parse_args()

    import psycopg2

    # 접속정보는 data-pipeline/.env 에서 — 기본값(localhost 등)은 두지 않는다.
    from dotenv import load_dotenv
    load_dotenv(os.path.join(BASE_DIR, ".env"))
    from data_pipeline.collectors import portmis_collector as pm
    from data_pipeline.common_pg_loader import pg_conninfo

    pg = psycopg2.connect(pg_conninfo())
    q = pg.cursor()
    rows = json.load(open(args.rows, encoding="utf-8"))
    q.execute("""SELECT berth_group, stop_wind_ms, unberth_wind_ms, disconnect_wind_ms FROM berth_weather_threshold""")
    wind_thr = {r[0]: r[1:] for r in q.fetchall()}

    replays = []
    for case in CASES:
        row = next((r for r in rows if str(r["name"]).strip() == case["name"] and r["at"].startswith(case["date"])), None)
        if row is None:
            print(f"[건너뜀] 표본에 없음: {case['name']} {case['date']}")
            continue
        at = dt.datetime.fromisoformat(row["at"]).astimezone(UTC)
        dep = dt.datetime.fromisoformat(row["dep"]).astimezone(UTC)
        chart = row["chart_margin"]

        q.execute("""SELECT observed_at_utc, tide_level_cm / 100.0 FROM tide_obs
                     WHERE tide_level_cm IS NOT NULL AND observed_at_utc BETWEEN %s AND %s ORDER BY 1""",
                  (at - dt.timedelta(hours=3), dep + dt.timedelta(hours=1)))
        tide = [(t, float(v)) for t, v in q.fetchall()]
        q.execute("""
            SELECT observed_at_utc, wind_speed_ms FROM weather_obs WHERE wind_speed_ms IS NOT NULL AND observed_at_utc BETWEEN %s AND %s
            UNION ALL SELECT observed_at_utc, wind_speed_ms FROM tide_obs WHERE wind_speed_ms IS NOT NULL AND observed_at_utc BETWEEN %s AND %s
            ORDER BY 1""", (at - dt.timedelta(hours=3), dep + dt.timedelta(hours=1)) * 2)
        wind = [(t, float(v)) for t, v in q.fetchall()]
        thr = wind_thr.get(row["group"]) or wind_thr.get("__GLOBAL_DEFAULT__")

        def tide_at(t):
            before = [v for tt, v in tide if tt <= t]
            return before[-1] if before else None

        def wind_max(t0, t1):
            vals = [v for tt, v in wind if t0 <= tt <= t1]
            return max(vals) if vals else None

        stay = [(t, v) for t, v in tide if at <= t <= dep]
        low_t, low_v = min(stay, key=lambda x: x[1]) if stay else (at, tide_at(at))
        arr_tide = tide_at(at)
        m_arr = chart + arr_tide if arr_tide is not None else None
        m_low = chart + low_v if low_v is not None else None
        completed = True  # 표본은 완료 접안

        low_level = _level_for_margin(m_low)
        if low_level == "부적합" and completed:
            low_level = "확인요청"

        w_arr = wind_max(at - dt.timedelta(hours=1), at)
        w_stay = wind_max(at, dep)
        stop = thr[0] if thr else None

        stages = [
            dict(
                key="pre_arrival", label="입항 전", at=_iso_kst(at - dt.timedelta(hours=12)),
                trigger="PORT-MIS 입항 신고 · 사전배정 계류시설",
                level=_level_for_margin(m_arr),
                facts=[
                    f"사전배정 {row['berth']} · 표 수심 {row['depth']} m",
                    f"흘수 {row['draught']} m ({row['draught_basis']}) → 표 수심 여유 {chart:+.2f} m",
                    f"입항 예정 시각 조위 {arr_tide:+.2f} m → 여유 {m_arr:+.2f} m" if m_arr is not None else "조위 없음",
                ],
                basis_note="재생에서는 입항 시각의 실측 조위를 사용 (운영 시 조석 예보)",
                action=None if _level_for_margin(m_arr) == "적합" else "입항 시각 조정 · 대체 선석 검토",
                recipient="선석 운영 주체",
            ),
            dict(
                key="pre_berthing", label="접안 직전", at=_iso_kst(at),
                trigger="항내 진입 · 접안",
                level=_level_for_margin(m_arr),
                facts=[
                    f"접안 시각 조위 {arr_tide:+.2f} m → 흘수 여유 {m_arr:+.2f} m" if m_arr is not None else "조위 없음",
                    f"직전 1시간 최대 풍속 {w_arr:.1f} m/s · 하역중단 기준 {stop} m/s" if w_arr is not None and stop else "풍속 관측 없음",
                ],
                action=None if _level_for_margin(m_arr) == "적합" else "정박지 대기 · 입항 보류",
                recipient="VTS",
            ),
            dict(
                key="cargo_ops", label="하역 중", at=_iso_kst(low_t),
                trigger="체류 중 최저 조위",
                level=low_level,
                facts=[
                    f"최저 조위 {low_v:+.2f} m ({low_t.astimezone(KST):%m-%d %H:%M}) → 흘수 여유 {m_low:+.2f} m" if m_low is not None else "조위 없음",
                    f"체류 중 최대 풍속 {w_stay:.1f} m/s · 하역중단 기준 {stop} m/s" if w_stay is not None and stop else "풍속 관측 없음",
                ],
                action={"적합": None, "주의": "저조 전후 흘수·하역량 확인 · 하역 보류 검토",
                        "확인요청": "수심·흘수 데이터 확인 요청 (표 수심 또는 보고 흘수 불일치 가능)",
                        "부적합": "하역 보류"}.get(low_level),
                recipient="터미널 안전관리자",
                gate="LOCKED" if low_level in ("부적합",) else ("CAUTION" if low_level in ("주의", "확인요청") else "OPEN"),
            ),
        ]

        series = []
        for t, v in tide:
            if t < at - dt.timedelta(hours=2) or t > dep + dt.timedelta(minutes=30):
                continue
            series.append(dict(t=_iso_kst(t), tide_m=round(v, 2), margin_m=round(chart + v, 2)))

        replays.append(dict(
            id=case["id"], kind="vessel", headline=case["headline"],
            vessel=dict(name=row["name"], call_sign=row["cs"], kind=row["kind"]),
            berth=dict(name=row["berth"], depth_m=row["depth"], handling=row["handling"], group=row["group"]),
            draught=dict(m=row["draught"], basis=row["draught_basis"]),
            arrival=_iso_kst(at), departure=_iso_kst(dep),
            margin_threshold_m=MARGIN_M, chart_margin_m=round(chart, 2),
            stages=stages, tide_series=series,
            source="PORT-MIS 입항 신고 · UPA 선박위치 흘수 · 국립해양조사원 울산 조위(DT_0020) · 기상청 관측",
        ))
        print(f"[OK] {case['id']}: {len(series)} 조위 점, 단계 {[s['level'] for s in stages]}")

    # ── 외해 너울 ──
    start = dt.date.fromisoformat(SWELL["start"])
    end = dt.date.fromisoformat(SWELL["end"])
    raw = pm.fetch_vessel_entries("820", start.strftime("%Y%m%d"), (end + dt.timedelta(days=1)).strftime("%Y%m%d"))
    ev = {}
    for x in raw:
        if not any(w in str(x.get("vsslKndNm")) for w in LIQ_WORDS):
            continue
        key = (x.get("clsgn"), x.get("etryptYear"), x.get("etryptCo"))
        rank = REPORT_RANK.get(x.get("arrival_reqstSeNm"), 0)
        if key in ev and ev[key][0] > rank:
            continue
        try:
            a = dt.datetime.fromisoformat(str(x.get("arrival_etryptDt")))
        except ValueError:
            continue
        ev[key] = (rank, a.astimezone(KST).date())
    arrivals = collections.Counter(d for _, d in ev.values())
    q.execute("""SELECT (observed_at_utc AT TIME ZONE 'Asia/Seoul')::date, avg(wave_height_sig_m), max(wave_height_sig_m)
                 FROM wave_obs WHERE (observed_at_utc AT TIME ZONE 'Asia/Seoul')::date BETWEEN %s AND %s GROUP BY 1""",
              (start, end))
    wave = {d: (a, m) for d, a, m in q.fetchall()}
    days = []
    d = start
    while d <= end:
        a, m = wave.get(d, (None, None))
        days.append(dict(date=d.isoformat(), arrivals=arrivals.get(d, 0),
                         wave_avg_m=round(float(a), 2) if a is not None else None,
                         wave_max_m=round(float(m), 1) if m is not None else None,
                         swell=a is not None and float(a) >= SWELL["threshold_m"]))
        d += dt.timedelta(days=1)
    sw = [x["arrivals"] for x in days if x["swell"]]
    nm = [x["arrivals"] for x in days if not x["swell"] and x["wave_avg_m"] is not None]
    peak = max(days, key=lambda x: x["arrivals"])
    replays.append(dict(
        id=SWELL["id"], kind="swell",
        headline="외해 너울 기간엔 입항이 줄었다가, 끝나자 몰렸다",
        window=dict(start=SWELL["start"], end=SWELL["end"]), threshold_m=SWELL["threshold_m"],
        summary=dict(swell_days=len(sw), swell_avg=round(sum(sw) / len(sw), 1) if sw else None,
                     normal_days=len(nm), normal_avg=round(sum(nm) / len(nm), 1) if nm else None,
                     peak_date=peak["date"], peak_arrivals=peak["arrivals"]),
        days=days,
        stages=[
            dict(key="pre_arrival", label="입항 전", level="주의", recipient="선석 운영 주체 · VTS",
                 action="외해 너울 예보 시 입항 예정 선박에 입항 시각 재조정 권고",
                 facts=["외해 부이(22189) 일평균 유의파고 2.0 m 이상", "항내 부두 하역 판정에는 쓰지 않는다 (외해 관측)"]),
            dict(key="after_swell", label="해소 직후", level="주의", recipient="선석 운영 주체",
                 action="입항 몰림 — 선석 혼잡 경고",
                 facts=[f"{peak['date'][5:].replace('-', '/')} 액체화물선 입항 {peak['arrivals']}척 (기간 최대)"]),
        ],
        caveat="원인을 너울로 단정할 수는 없다(기상특보에 따른 입출항 통제 등 가능). 외해 조건이 영향을 주는 곳이 부두 하역이 아니라 입항 단계라는 판단과 맞는다.",
        source="PORT-MIS 입항 신고(신고구분 최신본) · 기상청 울산 해양기상부이(22189)",
        queried_at=dt.datetime.now(KST).isoformat(timespec="minutes"),
    ))
    print(f"[OK] {SWELL['id']}: 너울 {len(sw)}일 평균 {replays[-1]['summary']['swell_avg']} · 평상 {len(nm)}일 평균 {replays[-1]['summary']['normal_avg']} · 최대 {peak['date']} {peak['arrivals']}척")

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(dict(generated_at=dt.datetime.now(KST).isoformat(timespec="minutes"), replays=replays),
                  f, ensure_ascii=False, indent=1)
    print(f"저장: {args.out}")


if __name__ == "__main__":
    main()
