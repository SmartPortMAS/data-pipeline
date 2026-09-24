# -*- coding: utf-8 -*-
"""
수집 파이프라인 통합 스케줄러 (APScheduler)

`run_pipeline.py` 의 도메인들을 **각 원천 API 의 실제 갱신 주기에 맞춰** 자동 실행한다.
지금까지 파이프라인은 사람이 손으로 돌렸고, 그래서 `berth_assignment` /
`anchorage_queue` / `scheduling_exclusion` 이 0행이었다(실측 2026-09-21 —
조건에 맞는 후보는 59척이나 대기 중이었다).

    실행:
        cd data-pipeline
        py -m data_pipeline.pipeline_scheduler              # 전체
        py -m data_pipeline.pipeline_scheduler --only vessel,portmis,tide
        py -m data_pipeline.pipeline_scheduler --dry-run    # 일정만 출력하고 종료
        py -m data_pipeline.pipeline_scheduler --no-startup-run

    중지: Ctrl+C

───────────────────────────────────────────────────────────────────────────────
주기 산정 근거 — 전부 실측이다. "10분쯤이겠지"로 정한 값은 하나도 없다.
───────────────────────────────────────────────────────────────────────────────

  tide (KHOA 조위 실측)           10분
      연속 관측 간 간격 최빈값 = 10분 (5,842/5,844 구간 = 99.97%).
      GetDTRecentApiService 는 한 번에 최근 구간을 통째로 주므로 한 번 놓쳐도
      다음 호출이 메운다.

  wave (KMA 부이 파고)            30분
      연속 관측 간 간격 최빈값 = 30분 (1,729/1,731 = 99.9%).
      수집기 docstring 대로 1회 호출에 정시+직전 30분 2건이 온다.
      10분마다 불러도 같은 값만 다시 받는다 — 호출만 3배가 된다.

  weather (항만기상 실측)         10분
      openWeatherNow.do 의 DATETIME 이 10분 단위다(실측 20260921191000).
      ★ 이 API 는 **현재값 1건만** 준다(records[0]). 과거 조회 파라미터가 없어
        백필이 불가능하다 — 즉 이 표의 행 수는 곧 스케줄러 실행 횟수다.
        weather_obs 가 30행뿐이었던 이유가 이것이다(로더 버그가 아니다).

  vessel (UPA 선박위치)            5분
      응답 item 의 updtTm(원천 갱신시각)을 4분 간격으로 5회 연속 조회한 실측:
          19:52:52 조회 -> 최신 updtTm 19:40:08 (경과 12.7분)
          19:56:54 조회 -> 최신 updtTm 19:45:08 (경과 11.8분)
          20:00:57 조회 -> 최신 updtTm 19:50:08 (경과 10.8분)
          20:05:00 조회 -> 최신 updtTm 20:00:08 (경과  4.9분)
          20:09:02 조회 -> 최신 updtTm 20:05:08 (경과  3.9분)
      최신 시각이 :00 :05 :10 … 로 떨어지고 초까지 :08 로 고정이다
      — UPA 가 **5분 단위 배치**로 게시한다는 뜻이다. 그래서 수집도 5분.
      ★ 2.5분 간격 조회에서는 590척 중 0척이 바뀌었다 — 5분보다 짧게 부르면
        같은 배치를 다시 받을 뿐이다.

      게시 지연은 **일정하지 않다: 실측 3.9 ~ 12.7분.** 즉 받는 값은 항상
      과거 위치이고, 얼마나 과거인지는 그때그때 다르다. 신선도가 필요한
      판정(mart.vessel_latest_position.presence_state, berth_occupancy_live)은
      수집 주기가 아니라 received_at_utc 를 직접 봐야 한다 — 5분마다 부른다고
      5분 내 위치가 보장되지 않는다.

      호출량: 1회 약 6페이지 x 288회/일 = 약 1,700회. UPA 일일 한도 10만 회의 2%.

  portmis (PORT-MIS 입출항신고)   10분
      신고가 들어오는 대로 반영되는 행정 기록이라 고정 주기가 없다.
      선석 자동 추천(arrival_watcher, backend 10분 주기)의 입력이므로
      그 주기보다 느리면 추천이 한 사이클씩 밀린다. 10분으로 맞춘다.

  weather_forecast (KMA 단기예보) 발표시각 cron (02·05·08·11·14·17·20·23시 + 45분)
      하루 8회 발표다. 그 사이에 다시 불러 봐야 같은 발표분이 온다.
      발표 직후에는 자료가 준비되지 않아 빈 응답이 오므로
      collector.PUBLISH_DELAY(40분)보다 여유를 둔 :45 에 실행한다.

  tide_forecast (KHOA 조석예보)   하루 1회 (03:10)
      한 번 호출에 7일치(DEFAULT_DAYS_AHEAD)가 온다. API 자체는 12일까지 응답한다.
      천문조 예보라 하루 사이에 값이 바뀌지 않는다.

  vessel_spec (해수부 선박제원)   하루 1회 (03:30)
      LOA·흘수·톤수는 선박 개조 전까지 불변이다. portmis staging 의 callsgn 을
      입력으로 쓰므로 portmis 가 이미 돌아간 시각에 배치한다.

  port_call (UPA 운항이력)        하루 1회 (04:20)
      사후 이력이다(관제 '현재' 판정에서는 배제됨 — realtime_views_v2.sql).
      선박 1척당 1회 호출이라 약 550회가 필요해 짧은 주기로 돌릴 대상이 아니다.

  upa_master (부두/정박지 마스터) 하루 1회 (04:40)
      계류시설·정박지 GIS. 시설이 신설·폐쇄될 때만 바뀐다.

  mart (구체화뷰 갱신)            1시간 (매시 :55)
      파생 도메인이라 원천이 갱신된 뒤에 돌아야 의미가 있다.
      mart.facility_alias 구체화뷰를 REFRESH 한다 — 사전이 낡으면 선석 매칭이
      틀어지므로 이 도메인의 본체가 이 갱신이다.
      [2026-09-21] 물리 마트(ulsan_vessel_mart) 생성·적재 단계는 빠졌다.
      읽는 곳이 한 곳도 없었고 같은 결합을 mart.* 뷰가 이미 한다 —
      표는 alembic 0027 이 DROP 했다(run_pipeline.run_mart 주석 참고).

  ※ raw 원본은 남기지 않는다(2026-09-24). 각 회차가 끝나면 그 회차가 쓴 raw 파일만
    지운다 — 이미 있던 파일은 그대로 둔다. data/raw/upa 를 같이 쓰는 vessel ·
    port_call · upa_master 는 한 번에 하나만 돈다. 그래서 매일 04:20 port_call 이 도는
    동안 vessel 5분 회차가 밀리거나 건너뛸 수 있다. 근거는 raw_cleanup.py docstring.

  ※ MSDS 는 넣지 않았다. 물질 151종 × 16섹션 배치라 몇 분씩 걸리고,
    원천(KOSHA)이 상시 갱신되는 자료가 아니다. 필요할 때 수동 실행한다.

───────────────────────────────────────────────────────────────────────────────
동작 원칙
───────────────────────────────────────────────────────────────────────────────
  · max_instances=1  — 같은 도메인이 겹쳐 돌지 않는다. 앞선 실행이 길어지면
                       다음 회차는 건너뛴다(수집은 멱등이라 건너뛰어도 안전하다).
  · coalesce=True    — 스케줄러가 멈췄다 살아나도 밀린 회차를 몰아 돌리지 않고
                       1회만 실행한다.
  · misfire_grace_time — 주기의 절반. 그만큼 늦어지면 그 회차는 버린다.
  · 한 도메인이 실패해도 스케줄러는 계속 돈다. 직전 raw/staging 이 남아 있어
    소비 측은 낡은 값을 보게 되므로, 실패는 로그에 남기되 멈추지 않는다.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

try:
    from dotenv import load_dotenv

    load_dotenv(os.path.join(BASE_DIR, ".env"))
except ImportError:  # pragma: no cover
    pass

LOG_DIR = os.path.join(BASE_DIR, "data", "logs")
logger = logging.getLogger("pipeline_scheduler")


# ---------------------------------------------------------------------------
# 일정표 — (도메인, 트리거종류, 트리거인자, 설명)
#
# 값의 근거는 모듈 docstring 에 전부 적혀 있다. 여기 숫자를 바꾸려면
# 그쪽 근거도 같이 고칠 것 — 근거 없는 주기가 다시 생기지 않게.
# ---------------------------------------------------------------------------
SCHEDULE: list[dict] = [
    # ── 실시간 계열 ────────────────────────────────────────────────────────
    {"domain": "vessel",  "trigger": "interval", "kwargs": {"minutes": 5},
     "desc": "UPA 선박위치 — updtTm 5분 배치 게시(실측), 게시지연 3.9~12.7분"},
    {"domain": "portmis", "trigger": "interval", "kwargs": {"minutes": 10},
     "desc": "PORT-MIS 입출항신고 — backend arrival_watcher(10분)와 동기"},
    {"domain": "tide",    "trigger": "interval", "kwargs": {"minutes": 10},
     "desc": "KHOA 조위 실측 — 관측간격 실측 10분"},
    {"domain": "weather", "trigger": "interval", "kwargs": {"minutes": 10},
     "desc": "항만기상 실측 — DATETIME 10분 단위, 1회 1행(백필 불가)"},
    {"domain": "wave",    "trigger": "interval", "kwargs": {"minutes": 30},
     "desc": "KMA 부이 파고 — 관측간격 실측 30분"},

    # ── 발표 주기 계열 ─────────────────────────────────────────────────────
    {"domain": "weather_forecast", "trigger": "cron",
     "kwargs": {"hour": "2,5,8,11,14,17,20,23", "minute": 45},
     "desc": "KMA 단기예보 — 하루 8회 발표 + 준비지연 40분 여유"},

    # ── 일 1회 계열 ────────────────────────────────────────────────────────
    {"domain": "tide_forecast", "trigger": "cron", "kwargs": {"hour": 3, "minute": 10},
     "desc": "KHOA 조석예보 — 1회 호출에 7일치(API 상한 12일)"},
    {"domain": "vessel_spec",   "trigger": "cron", "kwargs": {"hour": 3, "minute": 30},
     "desc": "해수부 선박제원 — 거의 불변. portmis staging 뒤에 실행"},
    {"domain": "port_call",     "trigger": "cron", "kwargs": {"hour": 4, "minute": 20},
     "desc": "UPA 운항이력 — 사후 이력, 선박당 1회 호출(약 550회)"},
    {"domain": "upa_master",    "trigger": "cron", "kwargs": {"hour": 4, "minute": 40},
     "desc": "부두/정박지 마스터 — 시설 신설·폐쇄 시에만 변동"},

    # ── 파생 ───────────────────────────────────────────────────────────────
    {"domain": "mart", "trigger": "cron", "kwargs": {"minute": 55},
     "desc": "통합 마트 + 구체화뷰 REFRESH — 원천 갱신 뒤 매시 실행"},
]

# 시작 시 한 번 즉시 돌릴 도메인. 일 1회짜리를 기동 때마다 돌리면 호출량만 늘어난다.
STARTUP_RUN = ["vessel", "portmis", "tide", "weather", "wave", "weather_forecast"]


# ---------------------------------------------------------------------------
# 도메인 실행기
# ---------------------------------------------------------------------------
def _run_upa_master() -> None:
    """부두/정박지 마스터 — run_pipeline.DOMAINS 에 없어 여기서 엮는다.

    upa_scheduler.py(예시 스크립트)는 전처리까지만 하고 DB 적재를 하지 않았다.
    여기서는 적재까지 간다 — 그래야 upa_berth_facility / upa_anchorage 가 실제로
    갱신된다.
    """
    from data_pipeline.common_pg_loader import load_all
    from data_pipeline.upa.upa_collector import UpaClient
    from data_pipeline.upa.upa_loader import TABLE_MAP as UPA_TABLE_MAP
    from data_pipeline.upa.upa_preprocess import run_pipeline as run_upa_preprocess

    client = UpaClient()
    berth_raw = client.collect_berth_facility()
    anch_raw = client.collect_anchorage()
    run_upa_preprocess("berth_facility", [berth_raw])
    run_upa_preprocess("anchorage", [anch_raw])
    load_all({
        k: UPA_TABLE_MAP[k]
        for k in ("upa_berth_facility_stg.csv", "upa_anchorage_stg.csv")
        if k in UPA_TABLE_MAP
    })


def _resolve(domain: str):
    from data_pipeline import run_pipeline as rp

    if domain == "upa_master":
        return _run_upa_master
    fn = rp.DOMAINS.get(domain)
    if fn is None:
        raise KeyError(f"알 수 없는 도메인: {domain}")
    return fn


def run_domain(domain: str) -> None:
    """도메인 1회 실행. 실패해도 예외를 밖으로 던지지 않는다(스케줄러 유지)."""
    from contextlib import nullcontext

    from data_pipeline.raw_cleanup import UPA_DOMAINS, UPA_LOCK, discard_raw

    started = time.monotonic()
    logger.info("[%s] 시작", domain)
    try:
        with (UPA_LOCK if domain in UPA_DOMAINS else nullcontext()), discard_raw(domain):
            _resolve(domain)()
    except Exception:  # noqa: BLE001
        logger.exception("[%s] 실패 — 직전 raw/staging 유지, 다음 회차 재시도", domain)
    else:
        logger.info("[%s] 완료 (%.1fs)", domain, time.monotonic() - started)


# ---------------------------------------------------------------------------
def _grace_seconds(entry: dict) -> int:
    """미스파이어 허용 시간 = 주기의 절반. cron 은 넉넉히 10분."""
    if entry["trigger"] == "interval":
        return max(60, int(entry["kwargs"].get("minutes", 10) * 60 / 2))
    return 600


def _setup_logging(verbose: bool) -> None:
    from logging.handlers import RotatingFileHandler

    os.makedirs(LOG_DIR, exist_ok=True)
    # 파일 로그는 10MB x 5개에서 돈다. 예전 FileHandler 는 끝없이 커졌다 — 상시
    # 도는 운영 EC2 에서는 디스크를 채운다. (systemd 로 돌리면 stdout 은 journald 에도 남는다)
    handlers = [
        logging.StreamHandler(sys.stdout),
        RotatingFileHandler(os.path.join(LOG_DIR, "pipeline_scheduler.log"),
                            maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"),
    ]
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
    )
    # APScheduler 의 내부 INFO 로그(잡 추가/실행)는 시끄러워서 WARNING 으로 낮춘다.
    logging.getLogger("apscheduler").setLevel(logging.WARNING)


def build_scheduler(entries: list[dict], scheduler_cls=None):
    from apscheduler.schedulers.blocking import BlockingScheduler

    sched = (scheduler_cls or BlockingScheduler)(timezone="Asia/Seoul")
    for e in entries:
        sched.add_job(
            run_domain,
            e["trigger"],
            args=[e["domain"]],
            id=e["domain"],
            name=e["desc"],
            max_instances=1,
            coalesce=True,
            misfire_grace_time=_grace_seconds(e),
            **e["kwargs"],
        )
    return sched


def _describe(e: dict) -> str:
    if e["trigger"] == "interval":
        return f"{e['kwargs']['minutes']}분마다"
    k = e["kwargs"]
    hour = k.get("hour", "*")
    return f"매일 {hour}시 {k.get('minute', 0):02d}분" if hour != "*" else f"매시 {k['minute']:02d}분"


def main() -> int:
    ap = argparse.ArgumentParser(description="수집 파이프라인 통합 스케줄러")
    ap.add_argument("--only", type=str, default=None,
                    help="쉼표로 구분한 도메인만 스케줄 (예: vessel,portmis)")
    ap.add_argument("--dry-run", action="store_true", help="일정만 출력하고 종료")
    ap.add_argument("--no-startup-run", action="store_true",
                    help="기동 직후 1회 즉시 실행을 하지 않는다")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    _setup_logging(args.verbose)

    entries = SCHEDULE
    if args.only:
        want = {s.strip() for s in args.only.split(",") if s.strip()}
        unknown = want - {e["domain"] for e in SCHEDULE}
        if unknown:
            print(f"[중단] 알 수 없는 도메인: {', '.join(sorted(unknown))}")
            print(f"       사용 가능: {', '.join(e['domain'] for e in SCHEDULE)}")
            return 2
        entries = [e for e in SCHEDULE if e["domain"] in want]

    print("=" * 76)
    print(" 수집 파이프라인 스케줄 (Asia/Seoul)")
    print("=" * 76)
    for e in entries:
        print(f"  {e['domain']:18} {_describe(e):16} {e['desc']}")
    print("=" * 76)

    if args.dry_run:
        print(" --dry-run: 실행하지 않고 종료합니다.")
        return 0

    sched = build_scheduler(entries)

    if not args.no_startup_run:
        startup = [e["domain"] for e in entries if e["domain"] in STARTUP_RUN]
        if startup:
            logger.info("기동 직후 1회 실행: %s", ", ".join(startup))
            for d in startup:
                run_domain(d)

    logger.info("스케줄러 시작 — 종료는 Ctrl+C")
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("스케줄러 종료")
    return 0


if __name__ == "__main__":
    sys.exit(main())
