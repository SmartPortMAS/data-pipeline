# -*- coding: utf-8 -*-
"""UPA 부두현황 웹페이지 → 선석 제원 원본 CSV.

왜 웹인가: 선석 제원(접안능력·안벽길이)은 공공 API(getGisBaseHrbrFcltDtlInfo)에서
대부분 결측이라 배정 감사(길이/수심 게이트)의 기준값으로 쓸 수 없다. UPA가 웹으로
공개하는 부두현황이 같은 항목을 선석 단위로 온전히 제공한다.

산출물은 `data/seed/berth_web_raw.csv` — **원본 증거 파일이다. 손으로 고치지 말 것.**
파싱·보정은 build_berth_seed.py가 맡고, 그 결과만 시드로 커밋된다. 그래야
"이 수심은 원천값인가 누가 고친 값인가"에 답할 수 있다(감사 근거의 최소 요건).

이 데이터는 정적으로 다룬다 — 스케줄러에 걸지 않고 사람이 필요할 때만 실행한다.
부두 제원은 거의 변하지 않고, 자동 재수집은 검수된 시드를 덮어쓸 위험만 만든다.

실행:
  python -m data_pipeline.upa.berth_web_scraper
"""
from __future__ import annotations

import csv
import os
import re
import sys
import time
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

BASE = "https://www.upa.or.kr/safe/portInfo"
LIST_URL = f"{BASE}/list.do?mid=0200000000"

# 목록 페이지의 gubn_code 가 항(港) 구분이다. 실측으로 확인:
#   1 본항 62 + 2 온산항 31 + 3 울산신항 23 + 4 미포항 1 = 117 (전체와 정확히 일치)
# 상세 페이지에는 항 구분이 없어서, 이 목록들을 따로 읽어야 port_name 을 얻는다.
# (공공 API 쪽 port_name 은 이름 매칭에 성공한 부두에만 붙어 커버리지가 낮다)
PORT_GUBUN = {1: "울산본항", 2: "온산항", 3: "울산신항", 4: "미포항"}
DETAIL_URL = f"{BASE}/detail.do?mid=0200000000&port_code={{code}}"
OUT_PATH = "data/seed/berth_web_raw.csv"

# 남의 서버다. 목록 1회 + 상세 117회면 충분하므로 여유있게 쉬어간다.
DELAY_SEC = 1.0
TIMEOUT = 20
RETRIES = 3

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; ulsan-berth-seed-builder/1.0)"}

# 목록 표의 컬럼 순서(번호/부두·선석명/소재지/운영사/준공연도).
LIST_COLS = ["seq", "list_name", "list_address", "list_operator", "list_built_year"]


def _get(url: str) -> BeautifulSoup:
    last = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            r.raise_for_status()
            # 서버가 charset을 명시하지 않는 경우가 있어 본문에서 추정한다.
            r.encoding = r.apparent_encoding or "utf-8"
            return BeautifulSoup(r.text, "lxml")
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"요청 실패({RETRIES}회): {url}") from last


def fetch_list() -> list[dict]:
    """목록 페이지에서 행 + port_code를 뽑는다(전 건이 한 페이지에 있다)."""
    soup = _get(LIST_URL)
    rows: list[dict] = []
    for tr in soup.select("table.tb tr"):
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue
        # port_code는 행 안의 링크/onclick 어디에 있든 문자열로 잡는다.
        m = re.search(r"PORT\d+", tr.decode())
        if not m:
            continue
        rec = {"port_code": m.group(0)}
        rec.update({k: tds[i].get_text(" ", strip=True) for i, k in enumerate(LIST_COLS)})
        rows.append(rec)
    return rows


def fetch_port_gubun() -> dict[str, str]:
    """port_code -> 항 이름. gubn_code 별 목록을 읽어 만든다.

    합계가 전체 건수와 맞는지는 호출부에서 확인한다 — 안 맞으면 구분이 겹치거나
    빠진 것이라 조용히 넘기면 안 된다.
    """
    out: dict[str, str] = {}
    for code, name in PORT_GUBUN.items():
        soup = _get(f"{LIST_URL}&gubn_code={code}")
        found = 0
        for tr in soup.select("table.tb tr"):
            m = re.search(r"PORT\d+", tr.decode())
            if m and tr.find_all("td"):
                out[m.group(0)] = name
                found += 1
        print(f"  [GUBUN] {name}: {found}건")
        time.sleep(DELAY_SEC)
    return out


def fetch_detail(port_code: str) -> dict:
    """상세 페이지의 라벨→값을 그대로 돌려준다.

    위치(인덱스)가 아니라 **라벨 기준**으로 읽는다 — 항목이 빠진 페이지가 있어도
    다른 값이 한 칸씩 밀려 들어가지 않게 하려는 것. 못 보던 라벨은 버리지 않고
    그대로 컬럼이 된다(원본 보존 원칙).
    """
    soup = _get(DETAIL_URL.format(code=port_code))
    for tag in soup(["script", "style"]):
        tag.decompose()
    target = None
    for ul in soup.select("ul.cont-dlist"):
        if any("부두/선석명" in li.get_text() for li in ul.find_all("li", recursive=False)):
            target = ul
            break
    if target is None:
        return {}
    out: dict[str, str] = {}
    for li in target.find_all("li", recursive=False):
        tit = li.select_one("span.tit")
        det = li.select_one("div.details")
        if not tit or not det:
            continue
        out[tit.get_text(" ", strip=True)] = det.get_text(" ", strip=True)
    return out


def main() -> None:
    listing = fetch_list()
    print(f"[LIST] {len(listing)}건")
    if not listing:
        sys.exit("목록 파싱 실패 — 페이지 구조가 바뀌었을 수 있다")

    gubun = fetch_port_gubun()
    missing = [r["port_code"] for r in listing if r["port_code"] not in gubun]
    if missing:
        # 어느 항에도 안 잡힌 부두가 있으면 구분 체계가 바뀐 것이다. 멈추지는
        # 않되(제원 수집 자체는 유효하다) 드러낸다.
        print(f"  [WARN] 항 구분 미상 {len(missing)}건: {missing[:5]}")

    records: list[dict] = []
    labels: list[str] = []          # 최초 등장 순서를 유지한다(컬럼 순서 = 페이지 순서)
    fetched_at = datetime.now(timezone.utc).isoformat()

    for i, row in enumerate(listing, 1):
        detail = fetch_detail(row["port_code"])
        if not detail:
            print(f"  [WARN] 상세 없음: {row['port_code']} {row.get('list_name')}")
        for k in detail:
            if k not in labels:
                labels.append(k)
        rec = dict(row)
        rec.update(detail)
        rec["port_gubun"] = gubun.get(row["port_code"])
        rec["fetched_at_utc"] = fetched_at
        records.append(rec)
        if i % 20 == 0:
            print(f"  ... {i}/{len(listing)}")
        time.sleep(DELAY_SEC)

    cols = ["port_code", *LIST_COLS, "port_gubun", *labels, "fetched_at_utc"]
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(records)

    print(f"[DONE] {len(records)}행 -> {OUT_PATH}")
    print(f"[LABELS] {len(labels)}종: {labels}")
    # 항목 구성이 페이지마다 다른지 드러낸다 — 파싱 규칙을 세우기 전에 알아야 한다.
    missing = {l: sum(1 for r in records if not r.get(l)) for l in labels}
    for l, n in missing.items():
        if n:
            print(f"  [VARY] '{l}' 결측 {n}행")


if __name__ == "__main__":
    main()
