"""
MSDS API 수집기 — 공공데이터포탈 안전보건공단 화학물질안전보건자료
API: https://apis.data.go.kr/B552468/msdschem/
저장 위치: data/raw/msds/msds_chemical_<날짜>_raw.json

수집 흐름:
  1. /getChemList (searchCnd=1, CAS번호 검색) → chemId 1건 확보
  2. /getChemDetail01 ~ /getChemDetail16 → chemId별 16개 섹션 수집
"""

import json
import os
import time
import xml.etree.ElementTree as ET
from datetime import datetime

import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY_DECODED = os.getenv("KOSHA_MSDS_API_KEY", "")
BASE_URL = "https://apis.data.go.kr/B552468/msdschem"
RAW_DIR = "data/raw/msds"

# 울산항 취급 화학물질 — (한국명, CAS번호)
# CAS번호로 정확히 1건 조회하므로 부분일치 오류 없음
ULSAN_CARGO_CHEMICALS = [
    # 정유제품 — KOSHA DB 확인된 CAS
    ("나프타",           "64741-41-9"),   # chemId=000910
    ("가솔린",           "8006-61-9"),    # chemId=016420
    ("경유",             "64742-86-5"),   # chemId=000957 수소탈황 경유
    ("등유",             "8020-83-5"),    # chemId=016476 탈취 등유
    # BTX 방향족
    ("벤젠",             "71-43-2"),      # chemId=001008
    ("톨루엔",           "108-88-3"),     # chemId=001032
    ("자일렌",           "1330-20-7"),    # chemId=001077
    ("스티렌",           "100-42-5"),     # chemId=001027
    # 알코올류
    ("메탄올",           "67-56-1"),      # chemId=001151
    ("에탄올",           "64-17-5"),      # chemId=000034
    ("이소프로판올",     "67-63-0"),      # chemId=001065
    # 지방족 / 케톤
    ("헥산",           "110-54-3"),     # chemId=001044
    ("사이클로헥산",     "110-82-7"),     # chemId=001042
    ("아세톤",           "67-64-1"),      # chemId=001067
    ("메틸에틸케톤",     "78-93-3"),      # chemId=001080
    # LPG / 에폭사이드 / 무기화학
    ("프로판",           "74-98-6"),      # chemId=015420
    ("부탄",             "106-97-8"),     # chemId=000247
    ("프로필렌옥사이드", "75-56-9"),      # chemId=001162
    ("황산",             "7664-93-9"),    # chemId=001049
    ("암모니아",         "7664-41-7"),    # chemId=001174
]

# 16개 섹션 엔드포인트
DETAIL_ENDPOINTS = [
    ("detail01", "/getChemDetail01", "화학제품과 회사에 관한 정보"),
    ("detail02", "/getChemDetail02", "유해성·위험성"),
    ("detail03", "/getChemDetail03", "구성성분의 명칭 및 함유량"),
    ("detail04", "/getChemDetail04", "응급조치요령"),
    ("detail05", "/getChemDetail05", "폭발·화재시 대처방법"),
    ("detail06", "/getChemDetail06", "누출사고시 대처방법"),
    ("detail07", "/getChemDetail07", "취급 및 저장방법"),
    ("detail08", "/getChemDetail08", "노출방지 및 개인보호구"),
    ("detail09", "/getChemDetail09", "물리화학적 특성"),
    ("detail10", "/getChemDetail10", "안정성 및 반응성"),
    ("detail11", "/getChemDetail11", "독성에 관한 정보"),
    ("detail12", "/getChemDetail12", "환경에 미치는 영향"),
    ("detail13", "/getChemDetail13", "폐기시 주의사항"),
    ("detail14", "/getChemDetail14", "운송에 필요한 정보"),
    ("detail15", "/getChemDetail15", "법적 규제현황"),
    ("detail16", "/getChemDetail16", "그 밖의 참고사항"),
]


def _xml_to_items(xml_text: str) -> list:
    try:
        root = ET.fromstring(xml_text)
        items_el = root.find(".//items")
        if items_el is None:
            return []
        return [
            {child.tag: (child.text or "").strip() for child in item}
            for item in items_el.findall("item")
        ]
    except ET.ParseError:
        return []


def fetch_chem_by_cas(cas_no: str) -> dict | None:
    """CAS번호로 정확한 chemId 1건 조회 (searchCnd=1)"""
    r = requests.get(
        f"{BASE_URL}/getChemList",
        params={
            "serviceKey": API_KEY_DECODED,
            "searchWrd": cas_no,
            "searchCnd": 1,
            "numOfRows": 1,
            "pageNo": 1,
        },
        timeout=10,
    )
    r.raise_for_status()
    items = _xml_to_items(r.text)
    return items[0] if items else None


def fetch_chem_detail(chem_id: str, endpoint: str) -> list:
    """chemId로 특정 섹션 전체 항목 조회 (msdsItemCode별 다건)"""
    r = requests.get(
        f"{BASE_URL}{endpoint}",
        params={
            "serviceKey": API_KEY_DECODED,
            "chemId": chem_id,
            "numOfRows": 100,
            "pageNo": 1,
        },
        timeout=10,
    )
    r.raise_for_status()
    return _xml_to_items(r.text)


def collect_all_msds_raw() -> None:
    os.makedirs(RAW_DIR, exist_ok=True)
    today = datetime.utcnow().strftime("%Y%m%d")
    output_path = os.path.join(RAW_DIR, f"msds_chemical_{today}_raw.json")

    collected_at = datetime.utcnow().isoformat() + "Z"
    all_results = []
    errors = []

    total = len(ULSAN_CARGO_CHEMICALS)
    print(f"[MSDS 수집 시작] {total}개 화학물질 (CAS번호 기준 1건씩)")

    for i, (name, cas_no) in enumerate(ULSAN_CARGO_CHEMICALS, 1):
        print(f"  [{i}/{total}] {name} (CAS {cas_no})")

        try:
            list_item = fetch_chem_by_cas(cas_no)
        except Exception as e:
            print(f"    목록 조회 오류: {e}")
            errors.append({"name": name, "cas_no": cas_no, "reason": str(e)})
            time.sleep(0.5)
            continue

        if not list_item:
            print(f"    결과 없음")
            errors.append({"name": name, "cas_no": cas_no, "reason": "조회 결과 없음"})
            time.sleep(0.3)
            continue

        chem_id = list_item.get("chemId", "")
        print(f"    chemId={chem_id} 상세 수집 중...")

        record = {
            "_query_name": name,
            "_cas_no": cas_no,
            "_chem_id": chem_id,
            "_collected_at_utc": collected_at,
            "list_info": list_item,
        }

        for key, endpoint, section_name in DETAIL_ENDPOINTS:
            try:
                detail = fetch_chem_detail(chem_id, endpoint)
            except Exception as e:
                detail = {}
                print(f"      {endpoint} 오류: {e}")
            record[key] = {"section": section_name, "data": detail}
            time.sleep(0.15)

        all_results.append(record)
        time.sleep(0.3)

    payload = {
        "collected_at_utc": collected_at,
        "source": "KOSHA_MSDS_API",
        "total_chemicals": total,
        "record_count": len(all_results),
        "error_count": len(errors),
        "errors": errors,
        "data": all_results,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"\n[완료] {output_path}  ({len(all_results)}건 수집 / {len(errors)}건 실패)")


if __name__ == "__main__":
    collect_all_msds_raw()
