"""
collect_portmis.py
==================
해양수산부 선박운항정보 API (PORT-MIS, VsslEtrynd5)를 활용하여
울산항(prtAgCd=820) + 온산항(prtAgCd=300) 입출항 기록을 수집하고
data/raw/portmis/ 에 저장합니다.

사용법:
    python utils/collect_portmis.py [--start YYYYMMDD] [--end YYYYMMDD]

    --start : 조회 시작일. 기본값: 오늘
    --end   : 조회 종료일. 기본값: 오늘

출력:
    data/raw/portmis/portmis_vessel_<YYYYMMDD>_<YYYYMMDD>.json

PORT-MIS 응답 필드 설명 (실제 API 검증 완료 기준):
    prtAgCd          : 항만청 코드 (820=울산, 300=온산)
    prtAgNm          : 항만청 명칭
    etryptYear       : 입항 연도
    etryptCo         : 입항 횟수 (선박별 연간 누적)
    clsgn            : 호출부호 (Call Sign) ← AIS Static 데이터의 callsgn과 조인 키
    vsslNm           : 선박명
    vsslNltyCd       : 국적 코드 (KR=한국, 등)
    vsslNltyNm       : 국적 명칭
    vsslKndCd        : 선종 코드 (52=화물/시멘트 등)
    vsslKndNm        : 선종 명칭
    etryptPurpsCd    : 입항 목적 코드 (03=하역 등)
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
import datetime
import argparse

# ----- 설정 -----
SERVICE_KEY = "219d0702262c964cf52fac4bd6a84fc313bfba3026af157bcbd6ba82c8f0010a"
BASE_URL = "http://apis.data.go.kr/1192000/VsslEtrynd5/Info5"

# 울산항(820) + 온산항(300) 모두 조회
PORT_CODES = {
    "820": "울산항",
    "300": "온산항",
}

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_PORTMIS_DIR = os.path.join(BASE_DIR, "data", "raw", "portmis")

MAX_ROWS_PER_PAGE = 100  # API 최대 허용 건수 (페이징)


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
                record = {child.tag: child.text for child in item}
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
    """울산항 + 온산항의 입출항 기록을 수집하여 JSON 파일로 저장."""
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
    parser = argparse.ArgumentParser(description="PORT-MIS 울산/온산항 선박 입출항 수집기")
    today_str = datetime.datetime.now().strftime("%Y%m%d")
    parser.add_argument("--start", type=str, default=today_str, help="조회 시작일 (YYYYMMDD). 기본: 오늘")
    parser.add_argument("--end", type=str, default=today_str, help="조회 종료일 (YYYYMMDD). 기본: 오늘")
    args = parser.parse_args()

    collect_portmis(start_date=args.start, end_date=args.end)
