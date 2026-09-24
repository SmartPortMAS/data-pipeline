"""
MSDS API 수집기 — 공공데이터포탈 안전보건공단 화학물질안전보건자료
API: https://apis.data.go.kr/B552468/msdschem/
저장 위치: data/raw/msds/msds_chemical_<날짜>_raw.json

수집 흐름:
  1. /getChemList (searchCnd=1, CAS번호 검색) → chemId 1건 확보
  2. /getChemDetail01 ~ /getChemDetail16 → chemId별 16개 섹션 수집
"""

import csv
import json
import os
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime

from dotenv import load_dotenv

from data_pipeline.common_http import get_with_retry

load_dotenv()

API_KEY_DECODED = os.getenv("KOSHA_MSDS_API_KEY", "")
BASE_URL = "https://apis.data.go.kr/B552468/msdschem"
RAW_DIR = "data/raw/msds"

# ---------------------------------------------------------------------------
# 수집 대상 화학물질 — data/reference/ 의 CSV 두 개에서 읽는다.
#
# [2026-09-13] 하드코딩 34종 목록을 제거했다. 울산항만공사 MSDS 정리본(169행)을
# 확보하면서 목록이 151종으로 늘었고, 코드에 박아두면 목록 변경이 코드 변경이 된다.
#
#   ulsan_msds_curated.csv   울산항만공사 원본 169행 (CAS 정정 2건 반영)
#                            169행 != 169종 — 같은 CAS를 공유하는 28행이 있어
#                            고유 CAS 는 141종이다(같은 물질의 다른 상품명).
#                            MSDS 는 CAS 단위로 발급되므로 CAS 로 중복을 제거한다.
#                            (제거 안 하면 같은 MSDS 가 여러 건 들어가고,
#                             UN번호로 조인하는 화물 판정이 그만큼 부풀려진다)
#   petroleum_products.csv   원본 목록에 없는 석유제품·가스 10종.
#                            원본이 케미컬 탱커 취급 물질 위주라 원유·경유·나프타
#                            등이 빠져 있는데, S-Oil/SK/현대오일터미널 선석의
#                            주력 화물이라 우리 관제 범위에서는 필수다.
#
# CAS 정정 이력(원본은 cas_no_original 컬럼에 보존):
#   #95  헥실렌            124-09-4 -> 592-41-6  (124-09-4는 헥사메틸렌디아민 #93)
#   #158 삼차-부틸알콜      76-65-0  -> 75-65-0   (76-65-0은 존재하지 않는 번호)
# ---------------------------------------------------------------------------
# 경로는 이 모듈 위치에서 계산한다 — cwd 에 의존하면 어디서 실행하느냐에 따라
# 조용히 다른 파일을 읽거나(최악) FileNotFoundError 가 난다.
_PKG_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
REFERENCE_DIR = os.path.join(_PKG_ROOT, "data", "reference")
CURATED_CSV = os.path.join(REFERENCE_DIR, "ulsan_msds_curated.csv")
PETROLEUM_CSV = os.path.join(REFERENCE_DIR, "petroleum_products.csv")

_CAS_RE = re.compile(r"^\d{2,7}-\d{2}-\d$")


def load_target_chemicals() -> list[tuple[str, str]]:
    """(표시명, CAS) 목록. CAS 기준 중복 제거, 원본 등장 순서 유지.

    한 행에 CAS 가 여러 개인 경우(#74 에탄/프로판)는 구분자로 쪼개 각각 담는다.
    형식에 맞지 않는 값(#125 "71-41-0 외")은 유효한 토큰만 취한다.
    """
    seen: dict[str, str] = {}

    def add(name: str, raw: str) -> None:
        for token in re.split(r"[,/]", raw or ""):
            token = token.strip()
            if _CAS_RE.match(token):
                seen.setdefault(token, name)

    with open(CURATED_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            label = f"{row['name_ko']}({row['name_en']})"
            add(label, row["cas_no"])

    with open(PETROLEUM_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            add(row["name_ko"], row["cas_no"])

    return [(name, cas) for cas, name in seen.items()]


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
    r = get_with_retry(
        f"{BASE_URL}/getChemList",
        params={
            "serviceKey": API_KEY_DECODED,
            "searchWrd": cas_no,
            "searchCnd": 1,
            "numOfRows": 1,
            "pageNo": 1,
        },
        timeout=10,
        label="KOSHA MSDS",
    )
    items = _xml_to_items(r.text)
    return items[0] if items else None


def fetch_chem_detail(chem_id: str, endpoint: str) -> list:
    """chemId로 특정 섹션 전체 항목 조회 (msdsItemCode별 다건)"""
    r = get_with_retry(
        f"{BASE_URL}{endpoint}",
        params={
            "serviceKey": API_KEY_DECODED,
            "chemId": chem_id,
            "numOfRows": 100,
            "pageNo": 1,
        },
        timeout=10,
        label="KOSHA MSDS",
    )
    return _xml_to_items(r.text)


def collect_all_msds_raw() -> None:
    os.makedirs(RAW_DIR, exist_ok=True)
    today = datetime.utcnow().strftime("%Y%m%d")
    output_path = os.path.join(RAW_DIR, f"msds_chemical_{today}_raw.json")

    collected_at = datetime.utcnow().isoformat() + "Z"
    all_results = []
    errors = []

    targets = load_target_chemicals()
    total = len(targets)
    print(f"[MSDS 수집 시작] {total}개 화학물질 (CAS번호 기준 1건씩)")

    for i, (name, cas_no) in enumerate(targets, 1):
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
