# -*- coding: utf-8 -*-
"""오경보 백테스트 — 실제로 접안·하역을 마친 기록에 판정 규칙을 적용한다.

왜 이 지표인가
  사고(정답) 데이터가 없어 "판정이 맞았나"는 잴 수 없다. 대신 **실제로 문제없이
  접안을 마친 배**는 적어도 "부적합"이 나오면 안 되는 표본이다. 여기에 규칙을
  돌려 몇 %를 막았는지(오경보율)를 잰다. 과잉 차단만 재고 놓친 위험은 못 잰다.
  규칙을 바꿀 때마다 같은 표본으로 다시 돌리면 판정 회귀 테스트가 된다.

표본
  PORT-MIS 입항 신고(최종본) 중 액체화물선 · 온산 액체 선석에 배정 · 입항 시각이
  지난 것. 계류시설 하위코드(선석 번호)가 API 응답에만 있어 DB 가 아니라 API 로 받는다.

단계 (2026-09-17 실측: 53% -> 12% -> 3.7% -> 2.2%)
  S0 현행 규칙   선종<->선석 취급 · 흘수 여유 1.0m(표 수심) · 점유 초과 · 입항 시각 기상(풍속+파고)
  S1 착시 제거   선종<->취급은 정보로(선종은 이번 화물이 아니다)
                 파고는 외해 부이(22189)라 원유부이 선석에만 적용, 항내 부두는 풍속만
  S2 점유 강등   PORT-MIS 출항 시각이 예정값이라 동시 계류를 과대 계산 -> 정보로
  S3 조위 반영   가용수심 = 표 수심 + 조위. 체류 중 최저 여유로 판정
                   >= 1.0m 해소 · 0~1.0m 주의 · < 0m 인데 실제 접안 -> 확인 요청(데이터 불일치)

기상 단계 판정은 backend/app/agents/weather/rule_engine.py 의 _level_for_metric 과 같다
(3시간 넘은 관측은 판단불가). 흘수는 입항 ±48h UPA 실측(>0) 우선, 없으면 선박제원 최대흘수.

실행 (data-pipeline 폴더에서, Docker 의 PostgreSQL·Neo4j 가 켜져 있어야 함)
  python -m data_pipeline.checks.backtest_false_alarm
  python -m data_pipeline.checks.backtest_false_alarm --days 30 --json out.json
"""
import argparse
import bisect
import collections
import datetime as dt
import json
import os
import re
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ROOT_DIR = os.path.dirname(BASE_DIR)
sys.path.insert(0, BASE_DIR)

KST = dt.timezone(dt.timedelta(hours=9))
UTC = dt.timezone.utc
MARGIN_M = 1.0                      # 스케줄링 에이전트 draught_margin_m 기본값
STALE = dt.timedelta(hours=3)       # weather rule_engine MAX_STALENESS
LIQ_CATEGORY = {
    "원유운반선": "원유", "석유제품 운반선": "유류", "기타 유조선": "유류", "급유선": "유류",
    "케미칼 운반선": "액체화학", "LPG 운반선": "가스", "LNG 운반선": "가스",
}
SEV = {"정상": 0, "하역중단": 1, "이안": 2, "호스분리": 3, "판단불가": 4}
REPORT_RANK = {"최초": 0, "변경": 1, "최종": 2}


def _env(path: str) -> dict:
    try:
        text = open(path, encoding="utf-8").read()
    except OSError:
        return {}
    return {k: v.strip().strip('"') for k, v in re.findall(r"^([A-Z0-9_]+)=(.*)$", text, re.M)}


def _iso(v):
    if not v:
        return None
    s = str(v)
    try:
        if "T" in s:
            return dt.datetime.fromisoformat(s)
        return dt.datetime.fromisoformat(s.replace(" ", "T")).replace(tzinfo=KST)  # 오프셋 없는 값은 KST
    except ValueError:
        return None


def _level(value, stale, stop, unberth, disconnect):
    if stop is None and unberth is None and disconnect is None:
        return "정상"
    if value is None or stale:
        return "판단불가"
    if disconnect is not None and value >= disconnect:
        return "호스분리"
    if unberth is not None and value >= unberth:
        return "이안"
    if stop is not None and value >= stop:
        return "하역중단"
    return "정상"


class Series:
    """관측 시계열 — 시각 이전 최신값 조회와 구간 최솟값."""

    def __init__(self, rows):
        self.rows = [r for r in rows if r[1] is not None]
        self.times = [r[0] for r in self.rows]

    def at(self, t):
        i = bisect.bisect_right(self.times, t) - 1
        if i < 0:
            return None, True
        obs_t, v = self.rows[i]
        return float(v), (t - obs_t) > STALE

    def min_between(self, t0, t1):
        i, j = bisect.bisect_left(self.times, t0), bisect.bisect_right(self.times, t1)
        vals = [(float(self.rows[k][1]), self.rows[k][0]) for k in range(i, j)]
        return min(vals) if vals else (None, None)


def main() -> None:
    ap = argparse.ArgumentParser(description="완료 접안 대비 오경보 백테스트")
    ap.add_argument("--days", type=int, default=30, help="표본 기간(일). 기본 30")
    ap.add_argument("--json", help="행 단위 결과를 저장할 경로")
    args = ap.parse_args()

    import psycopg2
    from neo4j import GraphDatabase

    penv = _env(os.path.join(BASE_DIR, ".env"))
    benv = _env(os.path.join(ROOT_DIR, "backend", ".env"))
    os.environ.setdefault("PORT_MIS_API_KEY", penv.get("PORT_MIS_API_KEY", ""))
    from data_pipeline.collectors import portmis_collector as pm

    now = dt.datetime.now(KST)
    pg = psycopg2.connect(
        host=penv.get("POSTGRES_HOST", "localhost"), port=int(penv.get("POSTGRES_PORT", "5433")),
        dbname=penv.get("POSTGRES_DB", "smartport"), user=penv.get("POSTGRES_USER", "smartport"),
        password=penv.get("POSTGRES_PASSWORD", ""),
    )
    q = pg.cursor()

    # ── 온산 액체 선석 ──
    q.execute("""
        SELECT b."fcltCd", b."fcltSubCd", b.wharf_name, b.depth_m, h.handling_cargo_name, b.berth_vessel_count
        FROM upa_berth_facility b
        LEFT JOIN mart.berth_handling_cargo h
          ON h.wharf_name = b.wharf_name AND h.port_operator_name IS NOT DISTINCT FROM b.port_operator_name
        WHERE b.port_name = '온산항'
    """)
    berths = {}
    for cd, sub, name, depth, handling, cap in q.fetchall():
        if handling and any(w in handling for w in ("원유", "유류", "액체화학", "가스")):
            berths[(cd, int(sub or 1))] = dict(name=name, depth=depth, handling=handling, cap=cap or 1)

    def resolve_berth(cd, sub):
        try:
            sub = int(sub)
        except (TypeError, ValueError):
            sub = 1
        if (cd, sub) in berths:
            return berths[(cd, sub)]
        if (cd, 1) in berths and berths[(cd, 1)]["cap"] > 1:   # S-Oil 4부두 01/02/03 = 한 행·3척
            return berths[(cd, 1)]
        return None

    drv = GraphDatabase.driver(
        benv.get("NEO4J_URI", "bolt://localhost:7687"),
        auth=(benv.get("NEO4J_USER", benv.get("NEO4J_USERNAME", "neo4j")), benv.get("NEO4J_PASSWORD", "")),
    )
    with drv.session() as s:
        group_of = {r["w"]: r["g"] for r in s.run("MATCH (b:Berth) RETURN b.wharf_name AS w, b.berth_group AS g")}
    drv.close()
    q.execute("""SELECT berth_group, stop_wind_ms, stop_wave_m, unberth_wind_ms, unberth_wave_m,
                        disconnect_wind_ms, disconnect_wave_m FROM berth_weather_threshold""")
    thresholds = {r[0]: r[1:] for r in q.fetchall()}

    # ── 관측 시계열 ──
    since = now - dt.timedelta(days=args.days + 2)
    q.execute("""
        SELECT observed_at_utc, wind_speed_ms FROM weather_obs WHERE wind_speed_ms IS NOT NULL AND observed_at_utc > %s
        UNION ALL SELECT observed_at_utc, wind_speed_ms FROM tide_obs WHERE wind_speed_ms IS NOT NULL AND observed_at_utc > %s
        UNION ALL SELECT observed_at_utc, wind_speed1_ms FROM wave_obs WHERE wind_speed1_ms IS NOT NULL AND observed_at_utc > %s
        ORDER BY 1""", (since, since, since))
    wind = Series(q.fetchall())
    # weather/service.py 와 같다 — 최신 행의 유의파고
    q.execute("SELECT observed_at_utc, wave_height_sig_m FROM wave_obs WHERE observed_at_utc > %s ORDER BY 1", (since,))
    wave = Series(q.fetchall())
    q.execute("""SELECT observed_at_utc, tide_level_cm / 100.0 FROM tide_obs
                 WHERE tide_level_cm IS NOT NULL AND observed_at_utc > %s ORDER BY 1""", (since,))
    tide = Series(q.fetchall())

    def weather_at(group, at, use_wave):
        t = thresholds.get(group) or thresholds.get("__GLOBAL_DEFAULT__")
        w, w_stale = wind.at(at)
        a = _level(w, w_stale, t[0], t[2], t[4])
        if not use_wave:
            return a
        v, v_stale = wave.at(at)
        b = _level(v, v_stale, t[1], t[3], t[5])
        return max(a, b, key=SEV.get)

    def draught_for(cs, at):
        q.execute("""SELECT draught FROM upa_vessel_position WHERE upper(btrim(callsgn)) = %s AND draught > 0
                     AND received_at_utc BETWEEN %s AND %s
                     ORDER BY abs(extract(epoch FROM received_at_utc - %s)) LIMIT 1""",
                  (cs, at - dt.timedelta(hours=48), at + dt.timedelta(hours=48), at))
        r = q.fetchone()
        if r:
            return float(r[0]), "실측"
        q.execute("SELECT draught_m FROM vessel_spec WHERE upper(btrim(callsgn)) = %s AND draught_m > 0 LIMIT 1", (cs,))
        r = q.fetchone()
        return (float(r[0]), "제원최대") if r else (None, "없음")

    # ── PORT-MIS 표본 (신고 최종본) ──
    raw = pm.fetch_vessel_entries(
        "820", (now - dt.timedelta(days=args.days)).strftime("%Y%m%d"), now.strftime("%Y%m%d"))
    events = {}
    for x in raw:
        kind = x.get("vsslKndNm")
        cat = LIQ_CATEGORY.get(kind)
        if not cat:
            continue
        key = (x.get("clsgn"), x.get("etryptYear"), x.get("etryptCo"))
        rank = REPORT_RANK.get(x.get("arrival_reqstSeNm"), 0)
        if key in events and events[key]["_rank"] > rank:
            continue
        at = _iso(x.get("arrival_etryptDt"))
        if not at or at > now:
            continue
        dep = _iso(x.get("departure_tkoffDt")) or _iso(x.get("arrival_tkoffPrrrnDt")) or at + dt.timedelta(hours=24)
        berth = resolve_berth(x.get("arrival_laidupFcltyCd"), x.get("arrival_laidupFcltySubCd"))
        if not berth:
            continue
        events[key] = dict(
            _rank=rank, cs=str(x.get("clsgn") or "").upper().strip(), name=x.get("vsslNm"), kind=kind,
            cat=cat, at=at, dep=min(max(dep, at), now), berth=berth,
        )
    sample = list(events.values())

    by_berth = collections.defaultdict(list)
    for e in sample:
        by_berth[e["berth"]["name"]].append(e)
    for lst in by_berth.values():
        for e in lst:
            e["concurrent"] = sum(1 for o in lst if o["at"] <= e["at"] < o["dep"])

    # ── 판정 ──
    rows = []
    for e in sample:
        b = e["berth"]
        at_utc, dep_utc = e["at"].astimezone(UTC), e["dep"].astimezone(UTC)
        group = group_of.get(b["name"])
        is_buoy = "부이" in b["name"]
        d, basis = draught_for(e["cs"], at_utc)

        chart_margin = (b["depth"] - d) if (d is not None and b["depth"] is not None) else None
        handles = e["cat"] in (b["handling"] or "")
        occupancy_over = e["concurrent"] > b["cap"]
        wx_s0 = weather_at(group, at_utc, use_wave=True)
        wx_s1 = weather_at(group, at_utc, use_wave=is_buoy)
        wx_alarm = ("하역중단", "이안", "호스분리")

        # S3 — 조위 반영 흘수 여유 (체류 중 최저)
        tide_verdict, tide_arr_margin, tide_min_margin, tide_min_at = None, None, None, None
        if chart_margin is not None and chart_margin < MARGIN_M:
            t_arr, t_stale = tide.at(at_utc)
            t_min, t_min_at = tide.min_between(at_utc, dep_utc)
            if t_arr is not None and not t_stale:
                tide_arr_margin = chart_margin + t_arr
                low = min(t_arr, t_min) if t_min is not None else t_arr
                tide_min_at = t_min_at if (t_min is not None and t_min <= t_arr) else at_utc
                tide_min_margin = chart_margin + low
                if tide_min_margin >= MARGIN_M:
                    tide_verdict = "해소"
                elif tide_min_margin >= 0:
                    tide_verdict = "주의"
                else:
                    tide_verdict = "확인요청"
            else:
                tide_verdict = "주의(조위 없음)"

        s0 = [f for f, on in [
            ("선종<->취급", not handles),
            (f"흘수여유({basis})", chart_margin is not None and chart_margin < MARGIN_M),
            ("점유초과", occupancy_over),
            (f"기상:{wx_s0}", wx_s0 in wx_alarm),
        ] if on]
        s1 = [f for f in s0 if not f.startswith(("선종", "기상"))] + ([f"기상:{wx_s1}"] if wx_s1 in wx_alarm else [])
        s2 = [f for f in s1 if f != "점유초과"]
        s3 = [f for f in s2 if not f.startswith("흘수")]
        if tide_verdict and tide_verdict != "해소":
            s3.append(f"흘수:{tide_verdict}")

        rows.append(dict(
            at=e["at"], dep=e["dep"], name=e["name"], cs=e["cs"], kind=e["kind"], berth=b["name"],
            depth=b["depth"], handling=b["handling"], group=group, draught=d, draught_basis=basis,
            chart_margin=chart_margin, tide_arrival_margin=tide_arr_margin,
            tide_min_margin=tide_min_margin, tide_min_at=tide_min_at, tide_verdict=tide_verdict,
            concurrent=e["concurrent"], cap=b["cap"], weather_s0=wx_s0, weather_s1=wx_s1,
            s0=s0, s1=s1, s2=s2, s3=s3,
        ))

    n = len(rows)

    def pct(k):
        return f"{k}/{n} ({100 * k / n:.1f}%)" if n else "0/0"

    print(f"\n오경보 백테스트 — 온산 액체 선석 완료 입항 {n}건 (지난 {args.days}일, 신고 최종본)")
    print(f"실행 {now:%Y-%m-%d %H:%M} KST\n")
    print("■ 축별 (S0 현행 규칙)")
    for label, fn in [
        ("선종<->선석 취급 불일치", lambda r: "선종<->취급" in r["s0"]),
        ("입항 시각 기상 하역중단 이상", lambda r: any(f.startswith("기상") for f in r["s0"])),
        ("점유 초과", lambda r: "점유초과" in r["s0"]),
        ("흘수 여유 1.0m 미달 (표 수심)", lambda r: any(f.startswith("흘수") for f in r["s0"])),
        ("흘수 정보 없음 -> 판정불가", lambda r: r["draught_basis"] == "없음"),
    ]:
        print(f"  {label:30s} {pct(sum(map(fn, rows)))}")

    print("\n■ 단계별 오경보율 (예외 1개 이상)")
    for stage, label in [("s0", "S0 현행 규칙"), ("s1", "S1 선종·외해 파고 착시 제거"),
                         ("s2", "S2 점유 초과 -> 정보"), ("s3", "S3 조위 반영 흘수(체류 중 최저)")]:
        print(f"  {label:30s} {pct(sum(1 for r in rows if r[stage]))}")

    verdicts = collections.Counter(r["tide_verdict"] for r in rows if r["tide_verdict"])
    print(f"\n■ S3 흘수 판정 내역: {dict(verdicts)} · 부적합 0 (완료 접안이라 음수 여유는 확인요청)")
    for r in sorted((r for r in rows if r["tide_verdict"]), key=lambda r: r["at"]):
        low = f"{r['tide_min_margin']:+.2f}m" if r["tide_min_margin"] is not None else "-"
        arr = f"{r['tide_arrival_margin']:+.2f}m" if r["tide_arrival_margin"] is not None else "-"
        low_at = r["tide_min_at"].astimezone(KST).strftime("%m-%d %H:%M") if r["tide_min_at"] else "-"
        print(f"  {r['at']:%m-%d %H:%M} {str(r['name'])[:16]:16s} -> {r['berth']:12s} "
              f"흘수 {r['draught']}({r['draught_basis']}) 표 여유 {r['chart_margin']:+.2f}m · "
              f"접안 {arr} · 최저 {low} ({low_at}) => {r['tide_verdict']}")

    left = [r for r in rows if r["s3"]]
    if left:
        print("\n■ S3 이후 남은 예외")
        for r in sorted(left, key=lambda r: r["at"]):
            print(f"  {r['at']:%m-%d %H:%M} {str(r['name'])[:16]:16s} -> {r['berth']:12s} {r['s3']}")

    if args.json:
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=1, default=str)
        print(f"\n행 단위 결과 저장: {args.json}")


if __name__ == "__main__":
    main()
