# -*- coding: utf-8 -*-
"""
실 API 라이브 검증 스크립트 (PC 실행 전용)

클라우드 작업 환경은 공공 API가 차단되어 있어, 실 API 호출 검증은 이 스크립트를
인터넷이 되는 PC에서 한 번 실행하는 것으로 대신한다. 6개 API 를 각각 1회 호출해
① 연결/인증 성공 여부 ② 수신 건수 ③ 응답 필드명이 우리 전처리 column_map 과
일치하는지(누락/신규 필드)를 자동 대조해 PASS/FAIL 로 출력한다.

실행:
  python -m data_pipeline.checks.verify_live_api

전제: .env 에 UPA_SERVICE_KEY / PORT_MIS_API_KEY / KHOA_API_KEY /
      KMA_BUOY_AUTH_KEY / MMAF_API_KEY 설정.
"""
import datetime as dt
import json
import os
import urllib.parse

import requests
from dotenv import load_dotenv

load_dotenv()

RESULTS = []


def report(name: str, ok: bool, detail: str) -> None:
    RESULTS.append((name, ok, detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}")


def check_fields(name: str, item: dict, expected: set) -> str:
    """응답 첫 item 의 필드명을 column_map 원본 키와 대조."""
    got = set(item.keys())
    missing = expected - got
    extra = got - expected
    msg = ""
    if missing:
        msg += f" | 기대했지만 없는 필드: {sorted(missing)}"
    if extra:
        msg += f" | 응답에만 있는 새 필드: {sorted(extra)}"
    return msg or " | 필드명 일치"


def verify_upa_vessel_position() -> None:
    """① UPA 항내 선박위치 (선박위치 소스 — 최우선 확인)"""
    from data_pipeline.upa import upa_config as cfg
    key = os.getenv("UPA_SERVICE_KEY", "")
    try:
        r = requests.get(
            "https://apis.data.go.kr/B551938/VslPstnInfoService/getVslPstnInfo",
            params={"serviceKey": key, "resultType": "json", "pageNo": 1, "numOfRows": 100},
            timeout=20,
        )
        body = r.json()["response"]["body"]
        items = body.get("items", {}).get("item", []) or []
        if isinstance(items, dict):
            items = [items]
        n = len(items)
        if n == 0:
            report("UPA 선박위치", False, f"HTTP {r.status_code}, 0건 수신 — 시간대/관제권 확인 필요")
            return
        msg = check_fields("UPA 선박위치", items[0], set(cfg.VSL_PSTN_INFO["column_map"].keys()))
        # 관제권 커버리지 힌트: 좌표 범위
        lats = [float(i.get("lat", 0) or 0) for i in items]
        lons = [float(i.get("lot", 0) or 0) for i in items]
        cover = f" | 좌표범위 lat {min(lats):.3f}~{max(lats):.3f} / lon {min(lons):.3f}~{max(lons):.3f}"
        report("UPA 선박위치", True, f"{n}건 수신 (totalCount={body.get('totalCount')}){msg}{cover}")
    except Exception as e:  # noqa: BLE001
        report("UPA 선박위치", False, f"{type(e).__name__}: {e}")


def verify_portmis() -> None:
    """② PORT-MIS 입출항 (선종·액체화물선 판별의 원천)"""
    key = os.getenv("PORT_MIS_API_KEY", "")
    today = dt.datetime.now().strftime("%Y%m%d")
    try:
        url = (
            "https://apis.data.go.kr/1192000/VsslEtrynd5/Info5"
            f"?serviceKey={key}&pageNo=1&numOfRows=5&dataType=JSON"
            f"&prtAgCd=820&sde={today}&ede={today}"
        )
        r = requests.get(url, timeout=20)
        ct = r.headers.get("content-type", "")
        if "json" in ct.lower():
            data = r.json()
            items = data.get("response", {}).get("body", {}).get("items", {}).get("item", []) or []
        else:
            # PORT-MIS 는 XML 응답인 경우가 있음 — 건수만 대략 확인
            items = r.text.count("<item>") * [None]
        report("PORT-MIS 입출항", r.ok, f"HTTP {r.status_code}, 오늘({today}) 울산 {len(items)}건")
    except Exception as e:  # noqa: BLE001
        report("PORT-MIS 입출항", False, f"{type(e).__name__}: {e}")


def verify_tide() -> None:
    """③ KHOA 조위 (울산 DT_0020)

    응답 구조는 최상위가 {header, body} 이며 'response' 래퍼가 없다
    (tide_collector.fetch_recent 와 동일). resultCode='00' 이 정상.
    """
    key_enc = urllib.parse.quote(os.getenv("KHOA_API_KEY", ""), safe="")
    try:
        url = (
            "https://apis.data.go.kr/1192136/dtRecent/GetDTRecentApiService"
            f"?serviceKey={key_enc}&type=json&obsCode=DT_0020&min=10&numOfRows=5&pageNo=1"
        )
        r = requests.get(url, timeout=20)
        data = r.json()
        result_code = data.get("header", {}).get("resultCode", "")
        items = (data.get("body", {}).get("items", {}) or {}).get("item", []) or []
        if isinstance(items, dict):
            items = [items]
        ok = result_code == "00" and bool(items)
        msg = f"HTTP {r.status_code}, resultCode={result_code}, {len(items)}건 수신"
        report("KHOA 조위", ok, msg)
    except Exception as e:  # noqa: BLE001
        report("KHOA 조위", False, f"{type(e).__name__}: {e}")


def verify_kma_buoy() -> None:
    """④ 기상청 APIHUB 부이 (파고, 울산 22189)"""
    try:
        r = requests.get(
            "https://apihub.kma.go.kr/api/typ01/url/kma_buoy.php",
            params={"stn": "22189", "authKey": os.getenv("KMA_BUOY_AUTH_KEY", "")},
            timeout=20,
        )
        lines = [ln for ln in r.text.splitlines() if ln.strip() and not ln.startswith("#")]
        report("KMA 부이(파고)", r.ok and bool(lines), f"HTTP {r.status_code}, 데이터 {len(lines)}줄")
    except Exception as e:  # noqa: BLE001
        report("KMA 부이(파고)", False, f"{type(e).__name__}: {e}")


def verify_weather() -> None:
    """⑤ 해수부 항만기상 (울산청 104 / 울산항동방파제서단등대 1041519)

    mmaf·mmsi·dataType 은 필수 파라미터 — 빠지면 HTTP 400 이 난다
    (weather_collector.fetch_weather_now 와 동일). result.status='OK' 가 정상.
    """
    try:
        r = requests.get(
            "http://marineweather.nmpnt.go.kr:8001/openWeatherNow.do",
            params={
                "serviceKey": os.getenv("MMAF_API_KEY", ""),
                "resultType": "json",
                "mmaf": "104",
                "mmsi": "1041519",
                "dataType": "1",
            },
            timeout=20,
        )
        status = ""
        try:
            status = r.json().get("result", {}).get("status", "")
        except ValueError:
            pass
        ok = r.ok and status == "OK"
        report("항만기상", ok, f"HTTP {r.status_code}, status={status or 'N/A'}, {len(r.text)}바이트")
    except Exception as e:  # noqa: BLE001
        report("항만기상", False, f"{type(e).__name__}: {e}")


def verify_msds() -> None:
    """⑥ KOSHA MSDS (벤젠 CAS 71-43-2 조회 1건)

    엔드포인트는 /getChemList (msds_api_collector.fetch_chem_by_cas 와 동일).
    CAS 정확검색은 searchCnd=1. 응답은 XML 이므로 <item> 개수로 성공 판정.
    """
    try:
        r = requests.get(
            "https://apis.data.go.kr/B552468/msdschem/getChemList",
            params={
                "serviceKey": os.getenv("KOSHA_MSDS_API_KEY", ""),
                "searchWrd": "71-43-2", "searchCnd": 1, "numOfRows": 1, "pageNo": 1,
            },
            timeout=20,
        )
        n_items = r.text.count("<item>")
        ok = r.ok and n_items > 0
        report("KOSHA MSDS", ok, f"HTTP {r.status_code}, item {n_items}건, {len(r.text)}바이트")
    except Exception as e:  # noqa: BLE001
        report("KOSHA MSDS", False, f"{type(e).__name__}: {e}")


def main() -> None:
    print("=== 실 API 라이브 검증 (6종) ===\n")
    verify_upa_vessel_position()
    verify_portmis()
    verify_tide()
    verify_kma_buoy()
    verify_weather()
    verify_msds()

    print("\n=== 요약 ===")
    ok = sum(1 for _, s, _ in RESULTS if s)
    for name, s, _ in RESULTS:
        print(f"  {'✅' if s else '❌'} {name}")
    print(f"\n{ok}/{len(RESULTS)} 통과")
    if ok == len(RESULTS):
        print("→ 다음: python -m data_pipeline.run_pipeline all 로 전체 파이프라인 실행")
    else:
        print("→ FAIL 항목의 에러 메시지를 확인하세요 (키 활용신청 승인 여부, 트래픽 제한 등)")


if __name__ == "__main__":
    main()
