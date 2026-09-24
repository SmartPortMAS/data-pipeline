# -*- coding: utf-8 -*-
"""수집 회차가 만든 raw 파일을 그 회차가 끝나면 지운다 — raw 원본을 남기지 않는다.

2026-09-24 결정: raw 원본은 어디에도(운영 EC2·로컬 PC) 남기지 않는다. S3 보관 경로도
없앴다. 단, **이미 있던 raw 파일은 그대로 둔다** — 그래서 "며칠 지난 파일 삭제"가
아니라 "이번 회차가 만들거나 고친 파일만 삭제"다(수정 시각 >= 회차 시작).

왜 이렇게 해도 되는가
--------------------
  · 수집 → 전처리 → 적재가 한 회차 안에서 끝난다. raw 는 그 사이의 중간 산출물이다.
    전처리가 staging CSV 를 만들고 로더가 DB 에 넣은 뒤에는 읽는 곳이 없다.
  · DB 는 영향이 없다 — 로더는 UPSERT 만 하고 지우지 않는다.
  · portmis 는 raw 파일명으로 "어디까지 받았나"를 추정하는데, 파일이 없으면 어제부터
    다시 받는다(resolve_incremental_start_date). 조회 창이 겹치므로 누락이 없다.
  · 조위·기상 실황 수집기는 날짜별 파일에 이어 붙이지만, 지워진 뒤에는 새 파일에 이번
    회차분만 들어간다 — 이미 적재된 행은 DB 에 있다.

도메인마다 자기 파일만 지운다
---------------------------
스케줄러는 도메인을 동시에 돌린다(스레드). 폴더 전체를 지우면 다른 도메인이 방금 쓰고
아직 읽지 않은 파일이 사라질 수 있다. 그래서 도메인 → 파일 패턴을 따로 둔다.
UPA 는 vessel · port_call · upa_master 가 같은 data/raw/upa 를 쓰고, vessel 과 port_call 은
**같은 파일**(upa_vessel_position_raw.json)까지 쓴다 — 그래서 셋은 UPA_LOCK 으로 한 번에
하나만 돈다(pipeline_scheduler.run_domain).

msds 는 넣지 않았다 — 스케줄러 밖 수동 배치이고, raw 전체가 한 번의 전처리 입력이다.
끄는 법(디버깅용): .env 에 KEEP_RAW=1.
"""
from __future__ import annotations

import fnmatch
import os
import threading
import time
from contextlib import contextmanager

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")

# 도메인 → (data/raw 아래 폴더, 파일 패턴들)
DOMAIN_RAW: dict[str, tuple[str, tuple[str, ...]]] = {
    "tide":             ("tide",             ("tide_obs_*",)),
    "tide_forecast":    ("tide_forecast",    ("tide_forecast_*",)),
    "wave":             ("wave",             ("wave_obs_*",)),
    "weather":          ("weather",          ("weather_obs_*",)),
    "weather_forecast": ("weather_forecast", ("weather_forecast_*",)),
    "portmis":          ("portmis",          ("portmis_vessel_*",)),
    "vessel_spec":      ("vessel_spec",      ("vessel_spec_*",)),
    "vessel":           ("upa",              ("upa_vessel_position_*",)),
    "port_call":        ("upa",              ("upa_vessel_position_*", "upa_vessel_nvgt_*")),
    "upa_master":       ("upa",              ("upa_berth_facility_*", "upa_anchorage_*")),
}

# data/raw/upa 를 쓰는 도메인은 한 번에 하나만 돈다(위 docstring).
UPA_DOMAINS = frozenset({"vessel", "port_call", "upa_master"})
UPA_LOCK = threading.Lock()


def _enabled() -> bool:
    return os.getenv("KEEP_RAW", "").strip() not in {"1", "true", "yes"}


def remove_written_since(domain: str, since: float) -> int:
    """domain 의 raw 파일 중 수정 시각이 since 이후인 것만 지운다. 지운 개수 반환."""
    spec = DOMAIN_RAW.get(domain)
    if spec is None:
        return 0
    sub, patterns = spec
    folder = os.path.join(RAW_DIR, sub)
    if not os.path.isdir(folder):
        return 0
    removed = 0
    for name in os.listdir(folder):
        if not any(fnmatch.fnmatch(name, p) for p in patterns):
            continue
        path = os.path.join(folder, name)
        try:
            if os.path.getmtime(path) >= since:
                os.remove(path)
                removed += 1
        except FileNotFoundError:
            continue
    return removed


@contextmanager
def discard_raw(domain: str):
    """with 블록 안에서 domain 이 쓴 raw 를 블록이 끝나면 지운다(성공·실패 모두).

    실패해도 지운다 — 다음 회차는 어차피 새로 받는다. 남겨 두면 "raw 를 남기지 않는다"가
    실패할 때마다 깨진다.
    """
    # 파일시스템 시각 해상도(ext4 ns, NTFS 100ns)와 무관하게 이번 회차 파일을 확실히
    # 포함하도록 1초 앞당긴다. 1초 전에 다른 회차가 같은 패턴 파일을 썼을 수는 없다 —
    # 같은 도메인은 max_instances=1 이고, UPA 는 잠금으로 직렬화된다.
    started = time.time() - 1.0
    try:
        yield
    finally:
        if _enabled():
            n = remove_written_since(domain, started)
            if n:
                print(f"  - [{domain}] raw {n}개 삭제 (보관하지 않음)")
