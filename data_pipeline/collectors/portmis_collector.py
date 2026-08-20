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
    --end   : 조회 종료일. 기본값: 오늘

    (2026-08-19) --start를 생략하면 마지막으로 수집해 둔 raw 파일의 종료일부터 자동으로
    이어서 수집한다 — 매번 날짜를 손으로 갱신하지 않아도 "그날그날 새로 입항한 것만"
    누적된다. 별도 체크포인트 파일을 두지 않고, data/raw/portmis/ 안의 기존 파일명
    (portmis_vessel_<start>_<end>.json) 자체를 이력으로 쓴다 — 그 디렉터리에 있는 모든
    end_date 중 최댓값을 다음 시작일로 삼는다. 이력이 전혀 없으면(최초 실행) 오늘부터
    시작한다. 이어붙일 때 마지막 종료일을 **하루 겹쳐서** 다시 수집한다 — 그날 오전에
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

import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
import json
import os
import re
import datetime
import argparse

from dotenv import load_dotenv

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

_RAW_FILENAME_RE = re.compile(r"^portmis_vessel_(\d{8})_(\d{8})\.json$")

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


def find_last_collected_end_date() -> str | None:
    """data/raw/portmis/ 에 이미 있는 raw 파일들 중 가장 늦은 end_date를 찾는다.

    별도 체크포인트 파일을 두지 않는다 — 파일명(portmis_vessel_<start>_<end>.json)
    자체가 "언제까지 수집했는지"를 이미 담고 있으므로, 그 디렉터리를 그대로 상태로
    쓴다(tide_collector.py가 날짜별 파일명으로 멱등성을 표현하는 것과 같은 원칙).
    """
    if not os.path.isdir(RAW_PORTMIS_DIR):
        return None
    end_dates = [
        m.group(2)
        for name in os.listdir(RAW_PORTMIS_DIR)
        if (m := _RAW_FILENAME_RE.match(name))
    ]
    return max(end_dates) if end_dates else None


def resolve_incremental_start_date() -> str:
    """--start 생략 시 쓸 시작일 — "자동 이어붙이기".

    마지막 수집 종료일을 하루 겹쳐서(포함) 다시 시작한다 — 그날 수집 이후에 추가로
    들어온 입항 신고를 놓치지 않기 위한 보수적 겹침이며, upsert라 중복 걱정이 없다
    (모듈 docstring 참고). 수집 이력이 전혀 없으면(최초 실행) 오늘부터 시작한다.
    """
    last_end = find_last_collected_end_date()
    if last_end is None:
        return datetime.datetime.now().strftime("%Y%m%d")
    return last_end


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

        try:
            req = urllib.request.Request(full_url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=15) as response:
                content = response.read()

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
    today_str = datetime.datetime.now().strftime("%Y%m%d")
    parser.add_argument(
        "--start", type=str, default=None,
        help="조회 시작일 (YYYYMMDD). 생략하면 마지막 수집 종료일부터 자동 이어붙이기(최초 실행이면 오늘)",
    )
    parser.add_argument("--end", type=str, default=today_str, help="조회 종료일 (YYYYMMDD). 기본: 오늘")
    args = parser.parse_args()

    start_date = args.start
    if start_date is None:
        start_date = resolve_incremental_start_date()
        reason = "이전 수집 이력 없음 → 오늘부터" if find_last_collected_end_date() is None \
            else f"마지막 수집 종료일({start_date})부터 겹쳐서 이어붙임"
        print(f"[자동 이어붙이기] --start 생략됨 — {reason}")

    collect_portmis(start_date=start_date, end_date=args.end)
