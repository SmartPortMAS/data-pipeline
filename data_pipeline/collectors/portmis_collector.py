"""
portmis_collector.py
==================
해양수산부 선박운항정보 API (PORT-MIS, VsslEtrynd5)를 활용하여
울산항(prtAgCd=820) 입출항 기록을 수집하고
data/raw/portmis/ 에 저장합니다.

(2026-08-16) 온산항을 별도 prtAgCd=300으로 조회하던 로직 제거 — PORT-MIS에는
온산항 전용 항만청 코드가 실제로 존재하지 않는다(300은 잘못된 값이었음).
온산항은 울산항(820) 조회 결과에 포함되어 들어온다.

사용법:
    python data_pipeline/collectors/portmis_collector.py [--start YYYYMMDD] [--end YYYYMMDD]

    --start : 조회 시작일. 생략하면 "자동 이어붙이기"(아래 설명) — 오늘이 아니다.
    --end   : 조회 종료일. 기본값: 오늘 + LOOKAHEAD_DAYS(3) — 입항 예정 신고 포함 (2026-09-17)

    --start를 생략하면 DB(portmis_vessel)의 마지막 수집일 하루 전부터 자동으로 이어서
    수집한다(2026-09-26, 예전엔 raw 파일명을 이력으로 썼다 — raw 는 회차마다 지우므로
    더 이상 이력이 될 수 없다). DB 가 비어 있으면 BACKFILL_DAYS(30)일 전부터 받는다.
    이어붙일 때 마지막 수집일을 **하루 겹쳐서** 다시 수집한다 — 그날 오전에
    수집이 돌았는데 오후에 추가로 입항 신고가 들어온 경우를 놓치지 않기 위해서다.
    portmis_pg_loader가 자연키(callsgn+entry_year+entry_count)로 upsert하므로 겹쳐
    수집해도 중복 행이 쌓이지 않는다 — 안전하게 겹칠 수 있다.

출력:
    data/raw/portmis/portmis_vessel_<YYYYMMDD>_<YYYYMMDD>.json

PORT-MIS 응답 필드 설명 (실제 API 검증 완료 기준):
    prtAgCd          : 항만청 코드 (820=울산)
    prtAgNm          : 항만청 명칭
    etryptYear       : 입항 연도
    etryptCo         : 입항 횟수 (선박별 연간 누적)
    clsgn            : 호출부호 (Call Sign) ← AIS Static 데이터의 callsgn과 조인 키
    vsslNm           : 선박명
    vsslNltyCd       : 국적 코드 (KR=한국, 등)
    vsslNltyNm       : 국적 명칭
    vsslKndCd        : 선종 코드. 실측 대조(vsslKndNm 기준, 2026-08-15):
                       51=원유운반선 52=석유제품 운반선 53=케미칼 운반선
                       55=LPG 운반선 59=기타 유조선 / 26=시멘트운반선
                       ※ 예전 주석에 "52=화물/시멘트"라고 적혀 있었으나 틀렸다.
                          52 는 이 프로젝트의 주력 선종(석유제품 운반선)이고,
                          시멘트는 26 이다. 코드 의미는 반드시 같은 행의 이름
                          컬럼(vsslKndNm)과 대조할 것 — prtAgCd 오인과 같은 실패다.
    vsslKndNm        : 선종 명칭 ← 코드 의미를 검증할 때 이 컬럼을 본다
    etryptPurpsCd    : 입항 목적 코드. 실측 대조(etryptPurpsNm 기준):
                       1=양적하 2=양하 3=적하 8=급유 10=단순경유 99=기타
                       ※ 예전 주석의 "03=하역"은 부정확하다(3 은 '적하').
    etryptPurpsNm    : 입항 목적 명칭
    frstDpmprtNatPrtCd : 최초 출발항 국가+항만 코드 (예: KRPUS=한국부산)
    frstDpmprtPrtNm    : 최초 출발항 명칭
    prvsDpmprtNatPrtCd : 직전 출발항 국가+항만 코드
    prvsDpmprtPrtNm    : 직전 출발항 명칭
    nxlnptNatPrtCd   : 다음 입항 예정 국가+항만 코드
    nxlnptPrtNm      : 다음 입항 예정 항만 명칭
    dstnNatPrtCd     : 최종 목적지 국가+항만 코드
    dstnPrtNm        : 최종 목적지 항만 명칭
    (2025.03 추가) tkoffPrrrnDt  : 출항 예정 일시 (입항 선박에 한함)
    (2025.03 추가) dstnEtryptDt : 목적지 입항 예정 일시 (출항 선박에 한함)
"""

import urllib.parse
import xml.etree.ElementTree as ET
import json
import os
import datetime
import argparse

from dotenv import load_dotenv

from data_pipeline.common_http import get_with_retry

load_dotenv()

# ----- 설정 -----
# backend/.env 의 PORT_MIS_API_KEY 와 같은 변수명(프로젝트 공통 컨벤션)
if not os.getenv("PORT_MIS_API_KEY"):
    raise RuntimeError("PORT_MIS_API_KEY가 없습니다. .env에 설정하세요.")
SERVICE_KEY = os.environ["PORT_MIS_API_KEY"]
BASE_URL = "http://apis.data.go.kr/1192000/VsslEtrynd5/Info5"

# 울산항(820)만 조회 — 온산항 전용 코드(300)는 실재하지 않는 잘못된 값이었음
PORT_CODES = {
    "820": "울산항",
}

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RAW_PORTMIS_DIR = os.path.join(BASE_DIR, "data", "raw", "portmis")

MAX_ROWS_PER_PAGE = 100  # API 최대 허용 건수 (페이징)

# (2026-09-17) 입항 예정 창 — 매 수집마다 오늘부터 며칠 뒤까지 함께 조회한다.
#
# 예전엔 "마지막 수집일 ~ 오늘"만 조회해서, 아직 입항하지 않은 선박의 입항 신고
# (사전배정 계류시설 포함)를 한 번도 받지 못했다. 그런데 API 는 미래 날짜 조회에
# 입항 예정 신고를 돌려준다(실측 9/17: 오늘~+3일 115건, 입항 시각이 미래인 건 92건,
# 그중 액체화물선 65척 전부 계류시설 기재). SafeBerth 의 "입항 전" 판정은 이
# 데이터가 없으면 성립하지 않는다.
#
# 같은 항차가 최초 -> 변경 -> 최종 신고로 바뀌므로 창을 매번 다시 조회해 덮어쓴다.
LOOKAHEAD_DAYS = 3

# 자동 이어붙이기가 거슬러 올라가는 최대 일수. 합성 화물(gen_cargo_manifest.WINDOW_DAYS)과 같다.
BACKFILL_DAYS = 30

# ---------------------------------------------------------------------------
# <item> 아래 <details><detail>...</detail><detail>...</detail></details> 파싱
# (2026-08-20 신설)
#
# 예전엔 `{child.tag: child.text for child in item}`로 item의 "직계 자식"만
# 평탄화했다. <details>도 직계 자식이라 걸리긴 하는데, ElementTree에서 자식
# 엘리먼트가 있는 태그의 `.text`는 "여는 태그 다음, 첫 자식 앞"의 공백 문자열일
# 뿐이다 — 그래서 `details`는 항상 `"\n    "` 같은 빈 문자열로만 잡히고, 그 안의
# 실제 값(입출항 실측 시각, 공식 배정 계선시설, 출항예정시각, 화물톤수, 예선/도선
# 여부, 승무원 수, 선박대리점 등 이벤트당 20여 개 필드)은 통째로 버려지고 있었다
# — raw XML을 직접 열어 재현·확인함(2026-08-20).
#
# <detail> 하나는 "입항" 아니면 "출항" 이벤트 하나를 나타낸다(<etryndNm> 값으로
# 구분). 같은 태그명(예: laidupFcltyCd)이 입항/출항 양쪽에 다 나오므로, 어느
# 이벤트인지 접두어로 구분해서 평탄화한다 — arrival_etryptDt(실제 입항시각),
# departure_tkoffDt(실제 출항시각) 등. 흥미로운 점: 입항 detail 안에
# tkoffPrrrnDt(출항 "예정" 시각)가 이미 들어있다 — 즉 배가 들어오는 순간 이미
# 출항 예정시각을 선사가 신고한다(다만 실측 출항시각과 다를 수 있는 "예정"값).
# 아직 출항 전인 항차는 <detail>이 입항 1개뿐일 수 있다.
# ---------------------------------------------------------------------------
_ETRYND_PREFIX = {"입항": "arrival_", "출항": "departure_"}


def _parse_details(details_el) -> dict:
    """<details> 하위 <detail>(입항/출항 이벤트별)을 arrival_/departure_ 접두로 평탄화."""
    out: dict = {}
    if details_el is None:
        return out
    for detail_el in details_el.findall("detail"):
        etryndnm_el = detail_el.find("etryndNm")
        prefix = _ETRYND_PREFIX.get(etryndnm_el.text if etryndnm_el is not None else None)
        if prefix is None:
            continue  # "입항"/"출항" 외 값(모르는 이벤트 종류)은 안전하게 건너뛴다
        for field in detail_el:
            if field.tag == "etryndNm":
                continue
            out[f"{prefix}{field.tag}"] = field.text
    return out


def find_last_collected_date() -> datetime.date | None:
    """DB portmis_vessel 에 마지막으로 적재된 수집일(KST). 비었거나 DB 에 못 붙으면 None.

    (2026-09-26) 예전엔 data/raw/portmis/ 의 파일명을 수집 이력으로 썼다. 그런데 raw 는
    회차가 끝나면 지운다(raw_cleanup.py) — 파일이 없으니 항상 "어제부터"였고, 반대로
    DB 를 새로 만들면 남은 옛 파일 때문에 "이미 받았다"고 판단해 빈 DB 를 채우지 않았다.
    "어디까지 받았나"의 정답은 DB 에 있다.
    """
    from sqlalchemy import text

    from data_pipeline.common_pg_loader import get_engine

    try:
        with get_engine().connect() as conn:
            last = conn.execute(text(
                "SELECT max(collected_at_utc) AT TIME ZONE 'Asia/Seoul' FROM portmis_vessel"
            )).scalar()
    except Exception as e:  # noqa: BLE001 — DB 없이도 수집은 돌아야 한다
        print(f"  (DB 수집 이력 조회 불가: {str(e)[:120]})")
        return None
    return last.date() if last else None


def resolve_incremental_start_date() -> str:
    """--start 생략 시 쓸 시작일 — "자동 이어붙이기".

    마지막 수집일 하루 전부터 겹쳐 받는다. 어떤 경우에도 어제보다 늦게 시작하지 않고
    (어제 들어온 최종 신고·출항 기록까지 다시 받는다), BACKFILL_DAYS 보다 멀리 가지 않는다.
    DB 가 비어 있으면(새로 만든 DB) BACKFILL_DAYS 전부터 받는다 — 합성 화물이 최근
    30일 입항 건을 보기 때문이다(gen_cargo_manifest.WINDOW_DAYS).
    upsert 라 겹쳐 받아도 중복이 쌓이지 않는다.
    """
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    earliest = today - datetime.timedelta(days=BACKFILL_DAYS)
    last = find_last_collected_date()
    if last is None:
        return earliest.strftime("%Y%m%d")
    start = min(last - datetime.timedelta(days=1), yesterday)
    return max(start, earliest).strftime("%Y%m%d")


def resolve_lookahead_end_date() -> str:
    """--end 생략 시 쓸 종료일 — 오늘 + LOOKAHEAD_DAYS (입항 예정 신고 포함)."""
    return (datetime.date.today() + datetime.timedelta(days=LOOKAHEAD_DAYS)).strftime("%Y%m%d")


def fetch_vessel_entries(port_code: str, start_date: str, end_date: str) -> list:
    """특정 항만청 코드, 날짜 범위의 선박 입출항 전체 데이터를 페이징으로 수집."""
    all_records = []
    page = 1

    while True:
        params = {
            "serviceKey": SERVICE_KEY,
            "pageNo": str(page),
            "numOfRows": str(MAX_ROWS_PER_PAGE),
            "dataType": "JSON",
            "prtAgCd": port_code,
            "sde": start_date,
            "ede": end_date,
        }

        query_str = urllib.parse.urlencode({k: v for k, v in params.items() if k != "serviceKey"})
        full_url = f"{BASE_URL}?serviceKey={SERVICE_KEY}&{query_str}"

        # 네트워크 실패는 재시도 후 예외로 올린다(아래 except 로 삼키지 않는다).
        # 예전에는 실패한 페이지에서 break 해 "받은 데까지"를 정상처럼 저장했고,
        # 스케줄러에는 성공으로 남았다. 조회 창이 매 회차 겹치므로 이번 회차를
        # 통째로 실패 처리해도 다음 회차(10분 뒤)가 같은 구간을 다시 받는다.
        content = get_with_retry(
            full_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15,
            label=f"PORT-MIS Info5 prtAgCd={port_code} page={page}",
        ).content

        try:
            root = ET.fromstring(content)

            # 응답 결과 코드 확인
            result_code_elem = root.find(".//resultCode")
            result_code = result_code_elem.text if result_code_elem is not None else "???"
            if result_code != "00":
                result_msg_elem = root.find(".//resultMsg")
                result_msg = result_msg_elem.text if result_msg_elem is not None else ""
                print(f"  [경고] API 오류 응답: {result_code} - {result_msg}")
                break

            # totalCount 파악
            total_count_elem = root.find(".//totalCount")
            total_count = int(total_count_elem.text) if total_count_elem is not None else 0
            num_of_rows_elem = root.find(".//numOfRows")
            num_of_rows = int(num_of_rows_elem.text) if num_of_rows_elem is not None else 0

            if total_count == 0:
                print(f"  조회 결과 없음 (항만코드={port_code}, {start_date}~{end_date})")
                break

            # item 파싱
            items = root.findall(".//item")
            for item in items:
                record = {child.tag: child.text for child in item if child.tag != "details"}
                record.update(_parse_details(item.find("details")))
                all_records.append(record)

            print(f"  [페이지 {page}] {len(items)}건 수집 (누계 {len(all_records)}/{total_count}건)")

            # 마지막 페이지 판단
            if len(all_records) >= total_count or num_of_rows == 0:
                break

            page += 1

        except Exception as e:
            print(f"  [오류] 항만코드={port_code}, 페이지={page}: {e}")
            break

    return all_records


def collect_portmis(start_date: str, end_date: str):
    """울산항의 입출항 기록을 수집하여 JSON 파일로 저장."""
    os.makedirs(RAW_PORTMIS_DIR, exist_ok=True)
    output_filename = f"portmis_vessel_{start_date}_{end_date}.json"
    output_path = os.path.join(RAW_PORTMIS_DIR, output_filename)

    all_records = []

    print(f"=== PORT-MIS 선박운항정보 수집 시작 ===")
    print(f"  조회 기간 : {start_date} ~ {end_date}")
    print(f"  저장 경로 : {output_path}")
    print("-------------------------------------------")

    for port_code, port_name in PORT_CODES.items():
        print(f"\n[{port_name} | prtAgCd={port_code}] 조회 중...")
        records = fetch_vessel_entries(port_code, start_date, end_date)
        all_records.extend(records)
        print(f"  → {len(records)}건 수집 완료")

    # JSON 저장
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_records, f, indent=4, ensure_ascii=False, default=str)

    print(f"\n=== 수집 완료 ===")
    print(f"  전체 레코드 : {len(all_records)}건")
    print(f"  저장 경로   : {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PORT-MIS 울산항 선박 입출항 수집기")
    parser.add_argument(
        "--start", type=str, default=None,
        help=f"조회 시작일 (YYYYMMDD). 생략하면 DB 마지막 수집일 전날부터 자동 이어붙이기(DB 가 비었으면 {BACKFILL_DAYS}일 전)",
    )
    parser.add_argument(
        "--end", type=str, default=resolve_lookahead_end_date(),
        help=f"조회 종료일 (YYYYMMDD). 기본: 오늘+{LOOKAHEAD_DAYS}일 (입항 예정 신고 포함)",
    )
    args = parser.parse_args()

    start_date = args.start
    if start_date is None:
        start_date = resolve_incremental_start_date()
        reason = f"DB 수집 이력 없음 → {BACKFILL_DAYS}일 전({start_date})부터" \
            if find_last_collected_date() is None \
            else f"DB 마지막 수집일 전날({start_date})부터 겹쳐서 이어붙임"
        print(f"[자동 이어붙이기] --start 생략됨 — {reason}")

    collect_portmis(start_date=start_date, end_date=args.end)
