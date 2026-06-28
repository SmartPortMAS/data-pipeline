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

# 울산항 주요 수출입 액체화물 — (한국명, CAS번호)
# CAS번호로 정확히 1건 조회하므로 부분일치 오류 없음
# 출처: 울산항 물동량 통계 및 MSDS 공개 자료 (2026년 기준)
ULSAN_CARGO_CHEMICALS = [
    # ── 1. 원유 및 정유·석유제품 (Crude & Refined Oil) ──────────────────────
    # 울산항 전체 물동량의 최대 비중을 차지하는 항목군
    ("원유",             "8002-05-9"),    # Crude Oil
    ("경유",             "68334-30-5"),   # Diesel Fuel — 수출입량 상위 1위 품목
    ("휘발유",           "8006-61-9"),    # Light Gasoline
    ("등유",             "8008-20-6"),    # Kerosine
    ("나프타",           "8030-30-6"),    # Naphtha (직류 나프타)
    ("벙커유/잔사유",    "68476-33-5"),   # Fuel Oil, Residual (Bunker)
    ("아스팔트",         "8052-42-4"),    # Asphalt

    # ── 2. BTX 및 기초 석유화학 유분 (Basic Petrochemicals) ─────────────────
    # 울산 석유화학단지 핵심 원료 — 액체화물 비중 약 12~13%
    ("벤젠",             "71-43-2"),      # Benzene
    ("톨루엔",           "108-88-3"),     # Toluene
    ("자일렌/크실렌",    "1330-20-7"),    # Xylene (혼합 이성질체)
    ("파라자일렌",       "106-42-3"),     # p-Xylene — 국내 제조·수출 핵심 품목
    ("에틸렌",           "74-85-1"),      # Ethylene
    ("프로필렌",         "115-07-1"),     # Propylene
    ("스티렌",           "100-42-5"),     # Styrene
    ("부타디엔",         "106-99-0"),     # Butadiene

    # ── 3. 알코올 및 글리콜류 (Alcohols & Glycols) ──────────────────────────
    # 상업용 탱크터미널에서 대량 저장·취급
    ("메탄올",           "67-56-1"),      # Methanol
    ("에탄올",           "64-17-5"),      # Ethanol
    ("에틸렌글리콜",     "107-21-1"),     # Ethylene Glycol (EG) — 석유화학 핵심 물질
    ("이소프로판올",     "67-63-0"),      # Isopropanol (IPA)

    # ── 4. 액체 가스 화물 (Liquefied Gases) ─────────────────────────────────
    # 전용 가스 부두 및 에너지 터미널 취급
    ("LPG(프로판)",      "74-98-6"),      # Propane
    ("LPG(부탄)",        "106-97-8"),     # Butane
    ("LNG/메탄",         "74-82-8"),      # Methane / LNG
    ("액화수소",         "1333-74-0"),    # Liquid Hydrogen

    # ── 5. 석유화학 중간체 및 화공품 (Intermediates & Specialty Chemicals) ───
    # 울산항 MSDS 직접 확인 물질 포함
    ("아크릴로니트릴",          "107-13-1"),   # Acrylonitrile (AN)
    ("프로필렌옥사이드",        "75-56-9"),    # Propylene Oxide (PO)
    ("염화비닐",                "75-01-4"),    # Vinyl Chloride (VCM)
    ("톨루엔디이소시아네이트",  "26471-62-5"), # Toluene Diisocyanate (TDI)
    ("테트라하이드로푸란",      "109-99-9"),   # Tetrahydrofuran (THF)
    ("트리에테르 폴리올 수지",  "9082-00-2"),  # Triol Polyether Polyol
    ("도데센",                  "97280-83-6"), # Dodecene (Tri-n-butene)
    ("트리에탄올아민",          "102-71-6"),   # Triethanolamine (TEA)
    ("트리클로로에틸렌",        "79-01-6"),    # Trichloroethylene (TCE)

    # ── 6. 무기화학물질 및 기타 (Inorganics & Others) ───────────────────────
    ("황산",             "7664-93-9"),    # Sulfuric Acid
    ("암모니아",         "7664-41-7"),    # Ammonia
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
