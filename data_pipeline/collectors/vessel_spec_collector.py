# -*- coding: utf-8 -*-
"""
vessel_spec_collector.py
==================
해양수산부 선박제원정보 API(SicsVsslManp3/info3)를 활용해 선박 제원(선박총길이·
너비·흘수·톤수 등)을 호출부호(clsgn) 단위로 조회·수집합니다.

배경 (08_스케줄링_전면재설계_자동배정_설계문서.md §5.2.1-A)
------------------------------------------------------------
선석 쪽 안벽길이(upa_berth_facility.length_m)는 이미 있지만, 선박 쪽 전장(LOA) 데이터가
없어서 "안벽 길이 ↔ 선박 전장" 게이트를 걸 수 없었다. 한 번 있었던 선박 길이·폭
(ais_vessel_static)은 2026-08-16에 품질 문제(ship_type 전량 NaN)로 이미 삭제됐고,
PORT-MIS 원본에도 길이 필드가 없다 — 이 공백을 메우기 위한 신규 수집기다.

이 API는 portmis_collector.py처럼 날짜범위로 한 번에 긁는 구조가 아니라, 선박 1척당
1회 조회(clsgn 또는 vsslNm 검색)하는 구조다. 그래서 "먼저 우리가 이미 추적 중인
호출부호 목록을 만들고, 그 목록을 순회하며 조회"하는 방식을 쓴다 — UPA 통합화물
API(bzentyCd 업체코드 필수, 그 값 자체를 몰라 영구 수집 불가)와 달리, 여기서 필요한
키(호출부호)는 이미 PORT-MIS/AIS/UPA 위치 데이터로 전량 확보돼 있다.

1차 실측(2026-08-19, 37/37 매칭)이 강한 신호를 보여 파이프라인에 편입했다 —
`vessel_spec_preprocessor.py`(전처리) → `vessel_spec_pg_loader.py`(PG 적재,
`vessel_spec` 테이블, backend Alembic 0013)로 이어진다. 다만 안벽 길이 게이트
자체(find_eligible_berths 연결)는 아직 하드 필터로 켜지 않았다 — §9 3-5(표본 확대
재검증) 통과 후 3-6에서 연결한다. `python -m data_pipeline.run_pipeline vessel_spec`
로 전체 파이프라인(수집→전처리→적재)을 한 번에 실행할 수 있다.

응답 필드 (사용자 제공 API 명세, 2026-08-19 확인 — 추정 아님):
    ibobprt      : 내외항구분
    clsgn        : 호출부호 ← 조회 키, 기존 callsgn과 동일 조인 키
    vsslNo       : 선박번호
    imoNo        : IMO번호 ← 호출부호보다 안정적인 보조 조인 키
    vsslKorNm    : 선박한글명
    vsslEngNm    : 선박영문명
    vsslKnd      : 선박종류
    vsslNlty     : 선박국적 ← 국적 필드 존재 = 외국적선도 대상일 정황 근거(확정 아님)
    tonEdycSe    : 톤수증서구분 / tonEdycSeNm : 톤수증서구분명
    intrlGrtg    : 국제총톤수 / grtg : 총톤수 / ntng : 순톤수
    vsslTotLt    : 선박총길이 ← 이번 게이트가 필요로 하는 값(LOA)
    shdth        : 선박너비
    vsslDrft     : 선박흘수 ← 부수 소득(기존 흘수 소스 교차검증/보강 후보, 이번 스코프 아님)
    vsslLt       : 선박길이(등록길이, Lpp 계열로 추정 — vsslTotLt와는 다른 값일 수 있음)
    vsslDp       : 선박깊이
    brbtSe/Nm    : 나용선구분
    nvgShapCd/Nm : 운항형태
    vsslCnstrDt  : 선박건조일시
    befClsgn     : 이전호출부호
    nwshipAt     : 신조선여부

사용법:
    python data_pipeline/collectors/vessel_spec_collector.py [--limit N] [--source PATH]

    --limit  : 테스트용 상한(예: 20) — 지정하면 그만큼만 조회 후 종료. 기본값: 전체.
    --source : 조회할 호출부호 목록의 출처 CSV. 기본값: data/staging/portmis_vessel_stg.csv
               (callsgn 컬럼, distinct). 이미 PORT-MIS로 확인된 울산항 재항 선박 전체를
               가장 잘 대표하는 목록이라 기본으로 삼는다.

출력:
    data/raw/vessel_spec/vessel_spec_<YYYYMMDD_HHMMSS>.json
    (조회한 매 호출부호에 대해 {queried_clsgn, matched, items:[...]} 형태로 기록 —
     매칭 실패도 그대로 남겨야 나중에 커버리지를 정확히 계산할 수 있다)

콘솔에 마지막으로 "조회 N척 / 매칭 M척 (비율%)" 요약을 출력한다 — 이게
§5.2.1-A 결론이 요구하는 커버리지 실측값이다.
"""

import argparse
import csv
import datetime
import json
import os
import time
import urllib.parse
import xml.etree.ElementTree as ET

from dotenv import load_dotenv

from data_pipeline.common_http import FetchError, get_with_retry

load_dotenv()

# ── 서비스키 ──────────────────────────────────────────────────────────────
# 같은 기관(해양수산부, provider 1192000) API라 계정 서비스키가 공유될 가능성이
# 높지만, data.go.kr은 API별로 별도 활용신청이 필요하다 — 전용 키가 따로 있으면
# VSSL_SPEC_API_KEY를 쓰고, 없으면 PORT_MIS_API_KEY로 폴백한다(.env에 새 항목을
# 추가하지 않아도 바로 시도해볼 수 있게).
SERVICE_KEY = os.getenv("VSSL_SPEC_API_KEY") or os.getenv("PORT_MIS_API_KEY")
if not SERVICE_KEY:
    raise RuntimeError(
        "API 키가 없습니다. .env에 VSSL_SPEC_API_KEY(전용) 또는 "
        "PORT_MIS_API_KEY(같은 기관 계정 폴백)를 설정하세요."
    )

BASE_URL = "http://apis.data.go.kr/1192000/SicsVsslManp3/Info3"
# ★ 대소문자 주의 — 마지막 세그먼트는 소문자 "info3"가 아니라 "Info3"다(PORT-MIS의
# "VsslEtrynd5/Info5"와 같은 표기 관행). 실측: "info3"는 NO_OPENAPI_SERVICE_ERROR(12)로
# 즉시 거부되고, "Info3"만 정상 응답한다(2026-08-19 직접 호출로 확인, 활용신청은 이미
# 승인돼 있어 PORT_MIS_API_KEY를 그대로 재사용 가능함도 함께 확인됨).

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw", "vessel_spec")
DEFAULT_SOURCE = os.path.join(BASE_DIR, "data", "staging", "portmis_vessel_stg.csv")

REQUEST_TIMEOUT_SEC = 15
REQUEST_INTERVAL_SEC = 0.15  # 척당 1회 호출 × 수백 척 — 외부 API에 과도한 연타를 피한다


def load_distinct_callsigns(source_csv: str) -> list[str]:
    """source_csv의 callsgn 컬럼에서 중복 제거한 호출부호 목록을 읽는다."""
    if not os.path.exists(source_csv):
        raise FileNotFoundError(
            f"{source_csv} 가 없습니다. 먼저 portmis 수집을 실행했는지 확인하세요"
            "(python -m data_pipeline.run_pipeline portmis --skip-db)."
        )
    seen: dict[str, None] = {}
    with open(source_csv, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if "callsgn" not in (reader.fieldnames or []):
            raise ValueError(f"{source_csv} 에 callsgn 컬럼이 없습니다.")
        for row in reader:
            cs = (row.get("callsgn") or "").strip()
            if cs and cs.lower() != "nan":
                seen[cs] = None
    return list(seen.keys())


def fetch_vessel_spec(clsgn: str) -> tuple[list[dict], str | None]:
    """단일 호출부호로 선박제원정보를 조회한다.

    Returns:
        (items, error_message). error_message가 None이 아니면 API 레벨 오류(예: 활용신청
        미승인, 키 오류)다 — "조회했지만 결과 없음"(정상, 매칭 실패)과는 구분해야 커버리지
        실측이 정확해진다.
    """
    params = {
        "serviceKey": SERVICE_KEY,
        "pageNo": "1",
        "numOfRows": "10",  # 호출부호 1건당 결과는 보통 1건 — 여유만 둠
        "dataType": "JSON",
        "clsgn": clsgn,
    }
    query_str = urllib.parse.urlencode({k: v for k, v in params.items() if k != "serviceKey"})
    full_url = f"{BASE_URL}?serviceKey={SERVICE_KEY}&{query_str}"

    try:
        content = get_with_retry(
            full_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=REQUEST_TIMEOUT_SEC,
            label="선박제원 Info3",
        ).content
    except FetchError as e:  # 문구에 URL·인증키가 없다
        return [], f"네트워크 오류: {e}"

    try:
        root = ET.fromstring(content)
    except ET.ParseError as e:
        # dataType=JSON을 줘도 이 계열 API는 XML을 돌려주는 경우가 있다(portmis_collector.py와
        # 동일 관찰) — 그래도 파싱이 실패하면 응답 자체가 JSON이거나 오류 페이지라는 뜻이므로
        # 원문을 그대로 보여준다.
        snippet = content[:200].decode("utf-8", errors="replace")
        return [], f"XML 파싱 실패({e}) — 응답 앞부분: {snippet!r}"

    result_code_elem = root.find(".//resultCode")
    result_code = result_code_elem.text if result_code_elem is not None else None
    if result_code not in ("00", "0000", None):
        result_msg_elem = root.find(".//resultMsg")
        result_msg = result_msg_elem.text if result_msg_elem is not None else ""
        return [], f"API 오류 응답: {result_code} - {result_msg}"

    items = root.findall(".//item")
    records = [{child.tag: child.text for child in item} for item in items]
    return records, None


def collect_vessel_specs(callsigns: list[str]) -> list[dict]:
    """호출부호 목록을 순회하며 선박제원을 조회하고, 매칭 여부와 함께 전부 기록한다."""
    results: list[dict] = []
    matched_count = 0
    error_count = 0

    print(f"=== 선박제원정보 수집 시작 (조회 대상 {len(callsigns)}척) ===")
    for i, clsgn in enumerate(callsigns, start=1):
        items, error = fetch_vessel_spec(clsgn)
        matched = bool(items) and error is None
        if matched:
            matched_count += 1
        if error:
            error_count += 1

        results.append({
            "queried_clsgn": clsgn,
            "matched": matched,
            "error": error,
            "items": items,
        })

        if i <= 5 or i % 50 == 0 or error:
            status = "매칭" if matched else ("오류" if error else "결과없음")
            print(f"  [{i}/{len(callsigns)}] {clsgn}: {status}" + (f" — {error}" if error else ""))

        # 처음 몇 건에서 API 키/활용신청 관련 오류가 연속되면 조기 중단해 낭비를 줄인다.
        if i == 5 and error_count == 5:
            print("  [중단] 처음 5건 전부 오류입니다 — API 키 또는 활용신청 상태를 먼저 "
                  "확인하세요(data.go.kr 마이페이지에서 '해양수산부_선박제원정보' 활용신청 "
                  "승인 여부).")
            break

        time.sleep(REQUEST_INTERVAL_SEC)

    total_queried = len(results)
    rate = (matched_count / total_queried * 100) if total_queried else 0.0
    print("=== 수집 완료 ===")
    print(f"  조회 {total_queried}척 / 매칭 {matched_count}척 (매칭률 {rate:.1f}%) "
          f"/ 오류 {error_count}척")
    return results


def collect_and_save(source: str = DEFAULT_SOURCE, limit: int | None = None) -> str:
    """수집 + raw JSON 저장을 한 번에 — run_pipeline.py와 CLI(main)가 공유하는 진입점."""
    callsigns = load_distinct_callsigns(source)
    if limit:
        callsigns = callsigns[:limit]

    results = collect_vessel_specs(callsigns)

    os.makedirs(RAW_DIR, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(RAW_DIR, f"vessel_spec_{timestamp}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False, default=str)
    print(f"  저장 경로: {out_path}")
    return out_path


def main() -> None:
    ap = argparse.ArgumentParser(description="해양수산부 선박제원정보 수집기 (커버리지 실측용)")
    ap.add_argument("--limit", type=int, default=None, help="테스트용 상한 척수(예: 20)")
    ap.add_argument("--source", type=str, default=DEFAULT_SOURCE,
                     help="호출부호 출처 CSV (callsgn 컬럼, 기본: portmis_vessel_stg.csv)")
    args = ap.parse_args()

    collect_and_save(source=args.source, limit=args.limit)


if __name__ == "__main__":
    main()
