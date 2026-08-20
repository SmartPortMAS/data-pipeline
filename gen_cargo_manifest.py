# -*- coding: utf-8 -*-
"""
합성 화물 manifest 생성기 (WBS 1.5 / 2.4)
=========================================
UPA 통합화물 API(getIntgCagInfo)는 업체코드(bzentyCd)가 필수라 자동수집이 불가하다
(활용신청 승인·인증키 정상, 조회 키 값만 부재 — UPA 회신 대기). 그동안 mart 화물
뷰와 안전판정 시나리오를 검증할 수 있도록 합성 manifest 를 만든다.

산출물
------
  1) v2 — 정상 케이스 (WBS 1.5 "스키마 정의서 기준 재작성")
     · upa_cargo_manifest 테이블 정의(mart_views.sql)의 39개 컬럼을 순서까지 맞춰
       빠짐없이 채운다. 이전 판은 7개 컬럼이 누락돼 적재 시 스키마가 어긋났다.
     · 화물은 무작위가 아니라 **PORT-MIS 실신고 선종**에 근거해 배정한다.
       (원유운반선 → 원유, LPG운반선 → LPG 계열 …)

  2) v3 — v2 + 위반 케이스 주입 (WBS 2.4)
     · 멘토 피드백 "틀린 데이터를 넣어도 된다 → 판정이 걸리는 장면이 필요하다" 반영.
     · 시스템이 실제로 경고를 띄우도록 의도적 위반 6종을 심는다.

  3) violation_scenarios.csv — 주입한 위반의 기계 판독용 목록
     (위반유형 · 대상 bl_no/선박/부두 · 근거 · 기대 판정)

  4) violation_weather_obs_synthetic.csv — 기상 초과 시나리오용 관측 행
     (화물 manifest 컬럼이 아니므로 별도 파일로 분리)

주의
----
  · 전 행 is_synthetic=True. 실데이터가 아니다.
  · bl_no·MRN·수량은 합성값이며, 화물 대분류만 실선종에 근거한다(cargo_basis 참고).
  · (화물명, UN, IMDG Class, 용기등급) 조합은 자유롭게 만들지 않는다.
    data_pipeline/reference/imdg_dgl.py 의 DGL 참조표를 유일한 출처로 삼고,
    생성 직후 전 행을 강제 검증한다. 불일치가 하나라도 있으면 생성이 실패한다.
    → 교차검증 지적 "합성 위험물 데이터는 화학적 교차검증 없으면 가짜" 대응.
  · 부차위험(subsidiary risk)은 스키마 39컬럼에 칸이 없어 manifest 에 담지 않는다.
    판정 시점에 imdg_dgl.subsidiary_risks() / mart.msds_flat 으로 조회할 것.
    예) 메탄올 UN1230 은 Class 3 이지만 부차위험 6.1(독성)이 있다.
  · 혼재금지 규칙(SEGREGATION_RULES)의 등급 조합은 IMDG 7.2 일반격리표 정본과
    일치하지만, 그것을 **인접 선석 동시하역**에 적용하는 것은 규정이 아니라
    보수적 스크리닝 차용이다. rule_authority 컬럼으로 그 사실을 명시한다.

실행
----
  py gen_cargo_manifest.py                 # 정상+위반 생성, 파이프라인 반영
  py gen_cargo_manifest.py --no-violations # 파이프라인엔 정상 케이스만 반영
"""
import argparse
import csv
import os
import random
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from data_pipeline.reference import imdg_dgl  # noqa: E402

# DB 접속정보(.env)를 읽는다. 없으면 _read_db 가 staging 으로 조용히 폴백한다.
try:
    from dotenv import load_dotenv

    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except ImportError:
    pass

random.seed(20260727)
STAGING = os.path.join("data", "staging")
SHARE_DIR = "samples"
OUT_PIPELINE = os.path.join(STAGING, "upa_cargo_manifest_stg.csv")
# 정상 365행만 담던 v2 파일은 v3 의 cargo_basis='PORT-MIS 실선종 기반' 부분집합과
# 동일해 중복이므로 제거했다. 정상만 필요하면 v3 에서 필터링할 것.
OUT_V3 = os.path.join(SHARE_DIR, "upa_cargo_manifest_stg_v3_synthetic.csv")
OUT_SCENARIO = os.path.join(SHARE_DIR, "violation_scenarios.csv")
OUT_WEATHER = os.path.join(SHARE_DIR, "violation_weather_obs_synthetic.csv")
# 액체화물선은 전수 배정하므로 총 행 수는 선박 수에 따라 정해진다(고정 목표 없음).
# 일반화물선은 표본이라 최소 이만큼만 채운다 — 혼재 판정 대상이 아니라서 전수가 필요 없다.
DRY_SAMPLE_MIN = 60

# ---------------------------------------------------------------------------
# 스키마 정본 — mart_views.sql 의 CREATE TABLE upa_cargo_manifest 와 1:1 일치.
# 컬럼 순서까지 맞춰 두면 적재 시 스키마 불일치가 나지 않는다. (WBS 1.5)
#
# ★ 2026-08-04 확정 — bzentyCd(업체코드)는 영구 미확보 (멘토 승인, 재추진 없음).
#   즉 getIntgCagInfo/getInprtCagDclrInfo 는 앞으로도 호출 불가능하다.
#   그럼에도 39컬럼 전부를 유지하는 이유:
#     - 아래 컬럼 중 io_se_code/pod_name/mrn_no 등 27개는 마트 뷰 어디서도
#       실제로 조회하지 않는다(전수 확인함). 안 써도 존재 자체가 문제를 만들지
#       않는다 — 전부 NULL 로 채워질 뿐이다.
#     - UPA 화물 API 응답 필드와 1:1 매핑된 스펙이라, 지금 줄였다가 나중에
#       (수동 화물 신고서 입력 등 대체 경로가 생기면) 다시 늘리는 것보다,
#       "매핑 자리는 유지하되 값은 영구 NULL"이 더 안전하다.
#   컬럼을 줄이면 이 파일 + mart_views.sql 의 CREATE TABLE 두 곳을 같이 고치고
#   재검증해야 하는데, 실제로 얻는 이득(로직 변화)이 없어 지금 시점엔 손대지
#   않는다. 컬럼이 왜 비어 있는지 몰라 헷갈리는 게 목적이면, 삭제가 아니라
#   이 주석으로 답한다.
# ---------------------------------------------------------------------------
SCHEMA_COLUMNS = [
    "port_code", "ptent_yr", "voyage_no", "callsgn", "vessel_name",
    "vessel_type_name", "vessel_nationality_code", "vessel_nationality_name",
    "mrn_no", "bl_no", "master_bl_no", "io_se_code", "io_se_name",
    "facility_name", "cargo_se_name", "cargo_name_raw", "dg_un_no",
    "cargo_basis", "package_type_name", "unload_method_name",
    "vol_ton_unit_name", "vol_ton", "weight_ton", "vol_size", "weight_size",
    "bulk_vol_size", "bulk_weight_size", "container_count",
    "pod_name", "pol_name", "ldud_port_name", "last_dest_port_name",
    "arrival_at_utc", "customs_progress_status_name",
    "source_system", "source_table", "collected_at_utc",
    "quality_flag", "is_synthetic",
]

# ---------------------------------------------------------------------------
# 선종(PORT-MIS 실신고) → 화물 계열.
#
# ★ 값을 여기 직접 적지 않는다. imdg_dgl.SHIP_KIND_ALLOWED_UN 이 정한 UN 번호만
#   쓰고, 화물명·Class·용기등급은 DGL 참조표에서 끌어온다. 손으로 적으면
#   UN↔Class↔PG 삼각관계가 어긋나도 아무도 모른 채 CSV 가 나온다.
#
# [정정 이력 2026-08-02 — DGL 대조로 드러난 실제 오류 3건]
#   1) 원유운반선에 UN1993(FLAMMABLE LIQUID, N.O.S. — 총칭 엔트리)을 쓰고 있었다.
#      원유의 고유 엔트리는 UN1267(PETROLEUM CRUDE OIL)이다. 총칭 UN 을 쓰면
#      MSDS 매칭이 엉뚱한 물질로 붙는다.
#   2) LNG운반선에 UN1971(METHANE, COMPRESSED — 압축가스)을 쓰고 있었다.
#      LNG 는 냉동액화이므로 UN1972(METHANE, REFRIGERATED LIQUID)가 맞다.
#   3) 메탄올(UN1230)의 부차위험 6.1(독성)이 어디에도 없었다. 스키마에 칸이
#      없으므로 DGL 참조표에 담고 판정 시점에 조회하도록 했다.
#   → 세 건 모두 "39컬럼 정합"만으로는 잡히지 않는 결함이다. 스키마 정합과
#     내용 정합은 별개라는 것이 이 정정의 교훈이다.
# ---------------------------------------------------------------------------
def _cargo_options(ship_kind: str) -> list:
    """선종 → [(화물명, UN, Class, 용기등급)] — 전부 DGL 참조표에서 유도."""
    out = []
    for un in imdg_dgl.SHIP_KIND_ALLOWED_UN.get(ship_kind, ()):
        e = imdg_dgl.DGL[un]
        pg = {"I": "Ⅰ", "II": "Ⅱ", "III": "Ⅲ"}.get(
            e.packing_groups[0] if e.packing_groups else "", "해당없음")
        out.append((e.psn_ko, e.un_no, e.imdg_class, pg))
    return out


CARGO_BY_SHIP_KIND = {
    k: _cargo_options(k) for k in imdg_dgl.SHIP_KIND_ALLOWED_UN
}
CARGO_BY_DRY_KIND = {
    "산물선": "곡물/광석", "양곡운반선": "양곡", "원목운반선": "원목",
    "광석운반선": "광석", "석탄운반선": "석탄", "시멘트운반선": "시멘트",
    "자동차운반선": "자동차", "철강제운반선": "철강재", "모래운반선": "모래",
    "냉동냉장선": "냉동화물", "일반화물선": "일반잡화", "풀컨테이너선": "컨테이너화물",
    "세미컨테이너선": "컨테이너화물",
}
DEFAULT_DRY_CARGO = "일반잡화"
# 선종을 모르는 액체선 폴백. 물질이 특정되지 않았으므로 총칭 엔트리 UN1993 이
# 여기서는 오히려 정확한 선택이다(원유운반선에 쓰면 틀리지만, 미상 선박에는 맞다).
FALLBACK_LIQUID = [_cargo_options("기타유조선")[i] for i in (0, 1)] + \
                  [_cargo_options("케미칼운반선")[1]]

# 액체화물 부두 / 일반 부두 (울산항 계류시설 코드표의 취급화물 기준)
LIQUID_FACILITIES = [
    "정일1부두", "정일2부두", "OTK1부두", "OTK2부두", "UTK부두", "UTT부두",
    "S-Oil 1부두", "S-Oil 2부두", "S-Oil 3부두", "S-Oil 4부두",
    "SK1부두", "SK2부두", "SK3부두", "SK4부두", "SK5부두", "SK6부두",
    "SK7부두", "SK8부두", "가스부두", "효성부두", "대한유화부두",
    "용잠1부두", "용잠2부두",
    "현대오일터미널 신항1부두", "현대오일터미널 신항2부두",
]
DRY_FACILITIES = ["1부두", "5부두", "7부두", "8부두", "신항일반부두", "온산1부두", "염포부두"]

# 부두별 동시접안 가능 척수(berth.max_concurrent_vessels 실측값, 2026-08-17 대조).
# 목록에 없으면 1로 본다. random.choice만 쓰면 24~31개 부두 중 특정 한 곳에
# 우연히 몰릴 수 있는데(실측: 서로 다른 배 16척이 한 부두에 동시 배정된 사례 —
# 실제로는 부두 하나에 유조선이 1~5척밖에 못 붙는다), 이 표를 기준으로 용량 대비
# 사용률이 가장 낮은 곳부터 채우면 최소한 특정 부두에만 비현실적으로 쌓이지는
# 않는다(TARGET_ROWS가 전체 용량 합보다 많아 초과 배정 자체는 남는다).
FACILITY_CAPACITY: dict[str, int] = {
    "정일1부두": 2, "정일2부두": 2, "OTK1부두": 2, "OTK2부두": 2, "UTK부두": 2, "UTT부두": 1,
    "S-Oil 1부두": 2, "S-Oil 2부두": 3, "S-Oil 3부두": 2, "S-Oil 4부두": 3,
    "SK1부두": 2, "SK2부두": 4, "SK3부두": 1, "SK4부두": 3, "SK5부두": 5, "SK6부두": 1,
    "SK7부두": 1, "SK8부두": 1, "가스부두": 3, "효성부두": 1, "대한유화부두": 2,
    "용잠1부두": 1, "용잠2부두": 1,
    "현대오일터미널 신항1부두": 1, "현대오일터미널 신항2부두": 1,
    "1부두": 1, "5부두": 1, "7부두": 1, "8부두": 2, "신항일반부두": 2, "온산1부두": 1, "염포부두": 3,
}


def _pick_facility(candidates: list, usage: dict) -> str:
    """용량 대비 사용률(usage/capacity)이 가장 낮은 후보를 고르고 사용량을 1 늘린다."""
    best = min(candidates, key=lambda f: usage.get(f, 0) / FACILITY_CAPACITY.get(f, 1))
    usage[best] = usage.get(best, 0) + 1
    return best


# ---------------------------------------------------------------------------
# 화물-선석 적합성 정합 (2026-08-19,
# 08_스케줄링_전면재설계_자동배정_설계문서.md §4.1.5)
#
# 기존 _pick_facility는 화물 종류와 무관하게 LIQUID_FACILITIES 24곳을 한
# 목록으로 놓고 용량 대비 사용률만 봤다 — 즉 "가솔린이 액체화학 전용 부두에"
# 같은 조합도 통계적으로 발생했다. 실제 스케줄링 로직(cargo_category_loader.py)이
# 화물을 원유/유류/액체화학 3개로 가르고 선석의 handling_cargo_name과 대조하므로,
# 합성 데이터도 같은 기준으로 걸러야 새 알고리즘 검증이 의미 있다.
#
# FACILITY_CATEGORY: LIQUID_FACILITIES 각 선석의 실제 handling_cargo_name
# (2026-08-19 라이브 DB 직접 조회, upa_berth_facility).
FACILITY_CATEGORY: dict[str, str] = {
    "정일1부두": "액체화학", "정일2부두": "액체화학", "OTK1부두": "액체화학",
    "OTK2부두": "액체화학", "UTK부두": "액체화학", "UTT부두": "액체화학",
    "S-Oil 1부두": "유류", "S-Oil 2부두": "유류", "S-Oil 3부두": "유류", "S-Oil 4부두": "액체화학",
    "SK1부두": "유류", "SK2부두": "유류", "SK3부두": "유류", "SK4부두": "유류",
    "SK5부두": "유류", "SK6부두": "유류", "SK7부두": "유류", "SK8부두": "유류",
    "가스부두": "유류", "효성부두": "액체화학", "대한유화부두": "액체화학",
    "용잠1부두": "액체화학", "용잠2부두": "액체화학",
    "현대오일터미널 신항1부두": "유류", "현대오일터미널 신항2부두": "액체화학",
}

# UN번호 -> 카테고리. imdg_dgl.SHIP_KIND_ALLOWED_UN/VIOLATION_ONLY_UN에 실제
# 등장하는 UN 전체를 cargo_category_loader.py의 CARGO_CATEGORIES(chem_id 기준)와
# 같은 물질명 기준으로 대응시켰다 — 손으로 새로 분류하지 않고 이미 있는 정본을
# 따른 것뿐이다. 1993(총칭 인화성액체, 미상 폴백)은 특정 물질이 아니라서
# 안전측으로 "유류"에 근사한다(원유는 아니고, 특정 액체화학 물질도 아님).
UN_TO_CATEGORY: dict[str, str] = {
    "1267": "원유",                                       # 석유(원유)
    "1202": "유류", "1203": "유류", "1223": "유류", "1268": "유류", "1993": "유류",
    "1011": "유류", "1978": "유류", "1972": "유류", "1077": "유류", "1005": "유류",
    "1114": "액체화학", "1294": "액체화학", "1093": "액체화학", "1230": "액체화학",
    "1280": "액체화학", "1307": "액체화학", "2055": "액체화학", "2056": "액체화학",
    "1010": "액체화학", "1830": "액체화학",
}


def _eligible_facilities(candidates: list, un_no: str | None) -> list:
    """카테고리 적합성으로 후보를 좁힌다. 매칭 정보가 없으면(건화물, 미분류 UN)
    원래 후보 전체로 폴백한다 — 합성 데이터가 아예 안 나오는 것보다는 낫다."""
    if not un_no:
        return candidates
    category = UN_TO_CATEGORY.get(str(un_no))
    if not category:
        return candidates
    narrowed = [f for f in candidates if FACILITY_CATEGORY.get(f) == category]
    return narrowed or candidates

# ---------------------------------------------------------------------------
# 인접 선석쌍 — berth_neo4j_loader.PILOT_ADJACENT_PAIRS 와 동일(ADJACENT_TO).
# 혼재금지는 "인접 선석에서 비혼재 등급을 동시 취급"할 때 걸린다.
# ---------------------------------------------------------------------------
ADJACENT_PAIRS = [
    ("정일1부두", "정일2부두"),
    ("OTK1부두", "OTK2부두"),
    ("현대오일터미널 신항1부두", "현대오일터미널 신항2부두"),
    ("SK5부두", "SK6부두"),
    ("SK6부두", "SK7부두"),
    ("SK7부두", "SK8부두"),
]

# ---------------------------------------------------------------------------
# 혼재금지 규칙 (IMDG Class 조합)
#
# 정본: data_pipeline/loaders/imdg_segregation_loader.py 의
#       IMDG_GENERAL_SEGREGATION_TABLE (IMDG Code Chapter 7.2 일반 격리표).
#       그 모듈은 neo4j 패키지를 import 하므로 여기서는 직접 참조하지 않고,
#       우리 34종에 실제로 등장하는 Class 조합만 발췌해 둔다.
#       ※ 표를 고칠 일이 생기면 반드시 정본 쪽을 고치고 여기로 옮겨 적을 것.
#
# 격리 코드: 1=Away from(수평 3m 이상)  2=Separated from(다른 격창)
#            3=Separated by a complete compartment  4=Separated longitudinally
#            X=일반 규정 없음(개별 위험물목록 확인 필요)
#
# [정정 이력 2026-07-31]
#   초기 잠정 표에는 8↔3(부식성↔인화성액체), 2.3↔2.1(독성가스↔인화성가스)을
#   혼재금지로 넣었으나, 공식 표와 대조하니 **둘 다 X(일반 규정 없음)** 이었다.
#   실무 직관에 기댄 추정이었고 근거가 없어 제거한다. 우리 34종에 등장하는
#   Class(2.1 / 2.3 / 3 / 6.1 / 8 / 9) 중 일반 격리 규정이 있는 조합은 아래 3개뿐.
#
# ★★ [적용 범위에 대한 정직한 고지 — 2026-08-02 추가] ★★
#   아래 등급 조합과 격리코드는 IMDG 7.2 일반격리표 원문과 일치한다. 그러나
#   IMDG 7.2 는 **선박 내부(화물창·갑판)의 적재(stowage)** 를 규율하는 해상
#   규정이지, 부두 A 와 부두 B 사이의 거리를 규율하는 규정이 **아니다**.
#
#   국내 법령 계보도 마찬가지다:
#     「선박안전법」 제41조
#       → 「위험물 선박운송 및 저장규칙」(해수부령) 제20조 (위험물의 격리)
#         → 「위험물 선박운송 기준」(해수부 고시) 별표19 (격리 기준)
#     — 이 계보 전체가 '선박에 실은 위험물' 을 대상으로 한다.
#
#   따라서 본 프로젝트는 이 표를 **법적 위반 판정**에 쓰지 않는다.
#   "인접 선석에서 동시 하역 중인 두 화물이, 만약 한 배에 실렸다면 격리 대상인가"
#   를 묻는 **보수적 스크리닝 트리거**로만 쓴다. 트리거가 걸리면 관제사에게
#   "확인 필요"를 띄우는 것이지 "위법"이라고 말하지 않는다.
#   이 구분을 데이터에도 남기려고 violation_scenarios.csv 에 rule_authority
#   컬럼을 두어 '법정근거' 와 '차용-스크리닝' 을 구분한다.
#
#   액체 벌크 터미널의 실제 동시작업(SIMOPS) 기준은 일반표가 아니라
#   ISGOTT(OCIMF/ICS) 기반의 터미널별 운영규정으로 정해진다. 확정 근거가
#   필요하면 울산항 각 터미널 운영규정을 입수해야 한다(미확보 — 한계로 보고).
# ---------------------------------------------------------------------------
SEGREGATION_RULES = [
    ("2.1", "3", "2", "인화성 가스 ↔ 인화성 액체 — Separated from(다른 격창)"),
    ("2.3", "3", "2", "독성 가스 ↔ 인화성 액체 — Separated from(다른 격창)"),
    ("2.1", "8", "1", "인화성 가스 ↔ 부식성 물질 — Away from(수평 3m 이상)"),
]

# 규칙 권위 등급 — violation_scenarios.csv 의 rule_authority 컬럼에 쓴다.
AUTHORITY_STATUTE = "법정근거"          # 국내 법령·고시에 직접 근거
AUTHORITY_BORROWED = "차용-스크리닝"    # 해상 규정을 항만 상황에 보수적으로 차용
AUTHORITY_OPERATIONAL = "운영기준"      # 터미널·항만 운영 실무 기준

# 위반 케이스 전용 화물 — DGL 참조표에서 유도(손으로 적지 않는다)
def _v(un: str):
    e = imdg_dgl.DGL[un]
    pg = {"I": "Ⅰ", "II": "Ⅱ", "III": "Ⅲ"}.get(
        e.packing_groups[0] if e.packing_groups else "", "해당없음")
    return (e.psn_ko, e.un_no, e.imdg_class, pg)


VIOLATION_CARGO = {
    "황산":     _v("1830"),
    "암모니아": _v("1005"),
    "프로페인": _v("1978"),
}

# UKC(Under Keel Clearance, 용골하 여유수심) 요구 비율.
#   실무 관행값(흘수의 10%)이며 법정 수치가 아니다. 항만·선종·해저저질에 따라
#   달라지므로 울산항 각 터미널 운영규정으로 확정해야 한다(미확보 — 한계로 보고).
UKC_RATIO = 0.10

# 울산항 부두 수심(m) — 출처: 울산지방해양수산청 「울산항시설현황」
#   ★ 이 값은 **해도기준면(Chart Datum) 기준 수심**이다. 그 시각의 실제
#     가용수심은 여기에 조위(tide_obs.tide_level_cm)를 더해야 나온다.
#     흘수 판정에 조위를 빼먹으면 만조에만 접안 가능한 배를 영구 불가로 오판한다.
#
#   정본은 data/seed/ulsan_berth_spec_seed.csv (선석수·안벽길이·DWT·운영주체 포함).
#   mart_views.sql 의 berth_draught_check 와 반드시 같은 값이어야 한다 —
#   둘이 갈리면 생성한 위반 시나리오와 실제 판정이 어긋난다.
#
#   선석별 수심이 다른 부두는 **안전측 최소값**을 쓴다. 부두명까지만 알고
#   몇 번 선석인지는 모르므로, 깊은 쪽을 쓰면 착저인 배를 OK 로 오판한다.
#     SK2부두 7.5(중력식 1선석)/8(잔교식 4선석) → 7.5
#     SK5부두 원문 '7-11' 범위(5선석)          → 7
#   부이(수심 27m)와 3부두(원문 '9,12' 로 모호)는 제외한다.
BERTH_DEPTH_M = {
    # 본항
    "4부두": 11.0, "6부두": 12.0, "용잠부두": 7.0, "가스부두": 7.5, "UTT부두": 11.0,
    "SK1부두": 7.5, "SK2부두": 7.5, "SK3부두": 12.0, "SK4부두": 10.0,
    "SK5부두": 7.0, "SK6부두": 15.0, "SK7부두": 15.0, "SK8부두": 18.0,
    # 온산항
    "효성부두": 12.0, "달포부두": 7.0, "UTK부두": 12.0, "대한유화부두": 12.0,
    "OTK1부두": 11.0, "OTK2부두": 9.0,
    "S-Oil 1부두": 11.0, "S-Oil 2부두": 15.5, "S-Oil 3부두": 14.0, "S-Oil 4부두": 12.0,
    "정일1부두": 11.0, "정일2부두": 12.5,
    # 울산신항
    "정일스톨트헤븐 신항 3~5부두": 14.0, "현대오일터미널 신항부두": 14.0,
    "LS니꼬 신항부두": 14.0, "UTK 신항부두": 14.0,
}

# ---------------------------------------------------------------------------
# 낡은 staging 감지용 표식
#
# 2026-07 이전 portmis_preprocessor 는 선종코드 매핑이 공식 CODE BOOK 과 어긋나
# 있었다(51=일반화물선·52=화물/시멘트선·53=목재선 …). 그 시절 staging 을 그대로
# 쓰면 "석유제품운반선"이 "화물/시멘트선"으로 남아 있어 화물이 거의 배정되지
# 않는다 — 조용히 잘못된 CSV 가 나오므로 여기서 잡아 경고한다.
# ---------------------------------------------------------------------------
STALE_CATEGORY_MARKERS = {
    "화물/시멘트선", "목재선", "LPG선", "LNG선", "기타화물선", "냉동화물선",
}


# ===========================================================================
# 공통 유틸
# ===========================================================================
def _read(name):
    """staging CSV 를 읽는다. 없으면 None.

    staging 은 **직전 1회 수집분**이다(수집기가 매시 덮어쓴다). 누적 이력이 필요한
    곳은 _read_db 를 쓴다 — 아래 주석 참고.
    """
    p = os.path.join(STAGING, name)
    return pd.read_csv(p, encoding="utf-8-sig") if os.path.exists(p) else None


def _read_db(sql):
    """로컬 DB 에서 읽는다. 접속이 안 되면 None (staging 폴백).

    [2026-08-15 — 왜 DB 도 보게 했나]
    선박 목록을 staging 에서만 만들면 그 시각에 수집된 배만 화물을 받는다.
    실측: staging PORT-MIS 130행(액체화물선 103) vs DB 누적 682행(액체화물선 403,
    고유 277척). 즉 staging 만 보면 액체화물선의 3분의 1 정도만 덮인다.

    화물 배정의 근거는 "그 배의 선종"이지 "이번 수집에 잡혔는지"가 아니므로,
    누적된 선종 정보를 쓰는 편이 맞다. DB 가 없는 환경(수집 전용 EC2 등)에서는
    조용히 staging 으로 돌아간다 — 그쪽은 DB 에 접속하지 않는 것이 설계다.
    """
    try:
        from sqlalchemy import create_engine
        url = os.getenv("DATABASE_URL")
        if not url:
            host = os.getenv("POSTGRES_HOST"); port = os.getenv("POSTGRES_PORT")
            db = os.getenv("POSTGRES_DB"); user = os.getenv("POSTGRES_USER")
            pw = os.getenv("POSTGRES_PASSWORD")
            if not all([host, port, db, user, pw]):
                return None
            url = f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}"
        url = url.replace("+asyncpg", "+psycopg2")
        return pd.read_sql(sql, create_engine(url))
    except Exception as e:
        print(f"  (DB 조회 불가 - staging 으로 진행: {str(e)[:70]})")
        return None


def check_staging_freshness(pm, allow_stale: bool = False):
    """PORT-MIS staging 이 구 선종매핑으로 만들어진 것인지 검사한다.

    낡은 staging 을 쓰면 액체화물선이 "화물/시멘트선" 등으로 남아 있어 화물이
    거의 배정되지 않는다.

    [2026-08-02 강화] 예전에는 경고만 찍고 그대로 생성했다. 그 결과 실제로
    "정상 6행 / 추정 359행" 짜리 사실상 쓸모없는 CSV 가 조용히 만들어져 기존
    샘플을 덮어썼다. 경고를 눈으로 읽고 넘어가는 것에 의존하면 안 된다.
    이제는 **생성을 중단**하고, 정말 필요하면 --allow-stale 로 명시하게 한다.
    """
    if pm is None or "ship_kind_category" not in pm.columns:
        return
    found = STALE_CATEGORY_MARKERS & set(pm["ship_kind_category"].dropna().unique())
    if not found:
        return
    n = int(pm["ship_kind_category"].isin(found).sum())
    msg = [
        "!" * 70,
        "[중단] PORT-MIS staging 이 낡았습니다 — 구 선종매핑으로 생성된 파일입니다.",
        f"       구 라벨 발견: {', '.join(sorted(found))}  ({n}행)",
        "       구 매핑은 51=일반화물선·52=화물/시멘트선·53=목재선 이었고,",
        "       공식 CODE BOOK 기준으로는 51=원유·52=석유제품·53=케미칼 입니다.",
        "       → 이대로 생성하면 액체화물이 거의 배정되지 않아 무의미한 CSV 가 나옵니다.",
        "",
        "       [해결] PORT-MIS 를 다시 수집·전처리한 뒤 이 스크립트를 재실행하세요:",
        "         py -m data_pipeline.run_pipeline portmis --skip-db \\",
        "            --start YYYYMMDD --end YYYYMMDD",
        "",
        "       그래도 지금 상태로 생성하려면: py gen_cargo_manifest.py --allow-stale",
        "!" * 70,
    ]
    if allow_stale:
        msg[1] = "[경고] --allow-stale 지정 — 낡은 staging 으로 그대로 생성합니다."
        print("\n".join(msg) + "\n")
        return
    raise SystemExit("\n".join(msg))


def build_vessel_pool(allow_stale: bool = False):
    """실제 staging → (callsgn, vessel_name, ship_kind_category, is_liquid, estimated).

    ship_kind_category 는 PORT-MIS 실신고 값 — 여기서 화물 계열이 정해진다.
    위치데이터에만 있고 PORT-MIS 매칭이 안 된 callsgn 은 선종을 모르므로
    estimated=True 로 표시한다(화물도 추정 폴백에서 배정).
    """
    pool = []
    # 선종은 누적 정보라 DB 를 우선 본다(없으면 staging). 신선도 검사는 staging 기준
    # 으로만 의미가 있으므로 그대로 둔다 — 수집이 멈췄는지 알려주는 장치다.
    pm_stg = _read("portmis_vessel_stg.csv")
    check_staging_freshness(pm_stg, allow_stale)
    pm = _read_db(
        "SELECT DISTINCT ON (callsgn) callsgn, vessel_name, ship_kind_category,"
        " is_liquid_cargo_vessel FROM portmis_vessel"
        " WHERE callsgn IS NOT NULL AND btrim(callsgn) <> ''"
        " ORDER BY callsgn, collected_at_utc DESC"
    )
    if pm is None:
        pm = pm_stg
    if pm is not None and "callsgn" in pm.columns:
        pm = pm.dropna(subset=["callsgn"]).drop_duplicates(subset=["callsgn"])
        for _, r in pm.iterrows():
            cs = str(r["callsgn"]).strip()
            if not cs or cs.lower() == "nan":
                continue
            liq = str(r.get("is_liquid_cargo_vessel", "")).lower() == "true"
            cat = str(r.get("ship_kind_category", "") or "")
            pool.append((cs, str(r.get("vessel_name", "") or ""), cat, liq, False))
    upa = _read_db(
        "SELECT DISTINCT ON (callsgn) callsgn, vessel_name FROM upa_vessel_position"
        " WHERE callsgn IS NOT NULL AND btrim(callsgn) <> ''"
        " ORDER BY callsgn, received_at_utc DESC"
    )
    if upa is None:
        upa = _read("upa_vessel_position_stg.csv")
    if upa is not None and "callsgn" in upa.columns:
        have = {c for c, _, _, _, _ in pool}
        u = upa.dropna(subset=["callsgn"]).drop_duplicates(subset=["callsgn"])
        for _, r in u.iterrows():
            cs = str(r["callsgn"]).strip()
            if not cs or cs.lower() == "nan" or cs in have:
                continue
            pool.append((cs, str(r.get("vessel_name", "") or ""), "", False, True))
    return pool


def pick_cargo(cat, is_liquid, estimated):
    """선종 대분류 → (화물명, UN번호, IMDG등급, 용기등급, 배정근거)."""
    if not estimated and cat in CARGO_BY_SHIP_KIND:
        name, un, imdg, pg = random.choice(CARGO_BY_SHIP_KIND[cat])
        return name, un, imdg, pg, "PORT-MIS 실선종 기반"
    if not estimated and cat in CARGO_BY_DRY_KIND:
        return CARGO_BY_DRY_KIND[cat], None, None, None, "PORT-MIS 실선종 기반"
    if estimated and is_liquid:
        name, un, imdg, pg = random.choice(FALLBACK_LIQUID)
        return name, un, imdg, pg, "선종미상(위치데이터 전용)-추정"
    return DEFAULT_DRY_CARGO, None, None, None, "선종미상-추정"


# io_se_code(I/O)와 io_se_name(수입/수출)이 각자 독립 random.choice() 로 뽑혀
# 375행 중 180행이 코드-이름 모순이었다(I인데 수출, O인데 수입). 하나를 정하고
# 나머지는 파생시켜야 한다. pod_name(양하항)/pol_name(적하항)도 마찬가지로
# 수출입 방향과 무관하게 pod_name="울산" 고정이라, 수출 화물인데 도착항이
# 울산으로 찍히는 오류가 있었다 — 수입이면 울산이 도착지(pod), 수출이면
# 울산이 출발지(pol)여야 한다.
IO_SE_NAME_BY_CODE = {"I": "수입", "O": "수출"}
FOREIGN_PORTS = ["SINGAPORE", "DALIAN", "휴스턴", "여수", "ONSAN"]


def make_row(callsgn, vessel_name, cat, cargo, un_no, basis, facility, seq):
    """SCHEMA_COLUMNS 전 항목을 채운 1행 (WBS 1.5)."""
    wton = round(random.uniform(500, 50000) if un_no else random.uniform(100, 20000), 1)
    io_se_code = random.choice(["I", "O"])
    return {
        "port_code": "KRUSN",
        "ptent_yr": "2026",
        "voyage_no": f"{random.randint(1, 200):03d}",
        "callsgn": callsgn,
        "vessel_name": vessel_name,
        "vessel_type_name": cat or ("유조선(추정)" if un_no else "화물선(추정)"),
        "vessel_nationality_code": "KR",
        "vessel_nationality_name": "대한민국",
        "mrn_no": f"MRN{random.randint(10000, 99999)}",
        "bl_no": f"BL{seq:06d}",
        "master_bl_no": f"MBL{random.randint(10000, 99999)}",
        "io_se_code": io_se_code,
        "io_se_name": IO_SE_NAME_BY_CODE[io_se_code],
        "facility_name": facility,
        "cargo_se_name": "액체" if un_no else "일반",
        "cargo_name_raw": cargo,
        "dg_un_no": un_no,
        "cargo_basis": basis,
        "package_type_name": "벌크" if un_no else "포장",
        "unload_method_name": "펌프" if un_no else "크레인",
        "vol_ton_unit_name": "TON",
        "vol_ton": wton,
        "weight_ton": wton,
        "vol_size": None,
        "weight_size": None,
        "bulk_vol_size": round(wton * 1.1, 1) if un_no else None,
        "bulk_weight_size": wton if un_no else None,
        "container_count": None,
        "pod_name": "울산" if io_se_code == "I" else random.choice(FOREIGN_PORTS),
        "pol_name": random.choice(FOREIGN_PORTS) if io_se_code == "I" else "울산",
        "ldud_port_name": None,
        "last_dest_port_name": None,
        "arrival_at_utc": "2026-07-27 00:00:00+00:00",
        "customs_progress_status_name": "반입",
        "source_system": "UPA_SYNTHETIC",
        "source_table": "IntgCagInfo(SYNTHETIC)",
        "collected_at_utc": pd.Timestamp.now("UTC").isoformat(),
        "quality_flag": "OK",
        "is_synthetic": True,
    }


# ===========================================================================
# v2 — 정상 케이스 (WBS 1.5)
# ===========================================================================
def build_v2(pool):
    """정상 케이스. **액체화물선은 한 척도 빠짐없이** 화물을 갖는다.

    [2026-08-15 개정 — 왜 행 수 목표를 버렸나]
    예전에는 TARGET_ROWS(365행)를 채울 때까지만 선박 목록을 돌았다. 그러면 앞쪽
    선박만 화물을 받고 뒤쪽은 빈손으로 남는데, 그 경계에 아무 의미가 없다 —
    같은 '석유제품 운반선'인데 목록 순서 때문에 한 척은 화물이 있고 한 척은 없다.

    실측(2026-08-15): PORT-MIS 액체화물선 277척 중 화물이 배정된 건 98척뿐이었고,
    화면에서는 "액체화물선인데 뭘 싣는지 모르는 배"로 보였다. 합성 데이터를 쓰기로
    한 이상 같은 선종을 다르게 대할 근거가 없다.

    그래서 기준을 "행 수"에서 "선박 커버리지"로 바꿨다. 액체화물선 전원에게 1~3건을
    배정하고, 일반화물선은 표본 성격이므로 종전처럼 일부만 채운다(혼재 판정 대상이
    아니라서 전수가 필요 없다).

    배정 근거(cargo_basis)와 IMDG 참조표 교차검증은 그대로다 — 커버리지를 늘린 것이지
    근거를 느슨하게 한 것이 아니다.
    """
    liquids = [v for v in pool if v[3]]
    others = [v for v in pool if not v[3]]
    ordered = liquids + others
    if not ordered:
        ordered = [(f"TEST{i:03d}", f"샘플선박{i}", "", i % 3 == 0, True) for i in range(50)]

    rows, i, seq = [], 0, 1
    facility_usage: dict[str, int] = {}
    while len(rows) < TARGET_ROWS:
        cs, vname, cat, liq, est = ordered[i % len(ordered)]
        i += 1
        for _ in range(random.randint(1, 3)):
            cargo, un, _imdg, _pg, basis = pick_cargo(cat, liq, est)
            base_candidates = LIQUID_FACILITIES if un else DRY_FACILITIES
            facility = _pick_facility(_eligible_facilities(base_candidates, un), facility_usage)
            rows.append(make_row(cs, vname, cat, cargo, un, basis, facility, seq))
            seq += 1

    # ② 일반화물선 — 표본만. 혼재 판정 대상이 아니라 전수가 필요 없고,
    #    액체화물 관제 화면에서 비중이 커지면 오히려 주제가 흐려진다.
    dry_quota = max(DRY_SAMPLE_MIN, len(rows) // 4)
    for cs, vname, cat, liq, est in others[:dry_quota]:
        cargo, un, _imdg, _pg, basis = pick_cargo(cat, liq, est)
        facility = random.choice(LIQUID_FACILITIES if un else DRY_FACILITIES)
        rows.append(make_row(cs, vname, cat, cargo, un, basis, facility, seq))
        seq += 1

    return rows


# ===========================================================================
# v3 — 위반 케이스 주입 (WBS 2.4)
# ===========================================================================
def inject_violations(rows):
    """v2 위에 의도적 위반을 심고 시나리오 목록을 함께 반환한다.

    위반 행의 bl_no 는 BL9xxxxx 대역으로 분리해 정상 행과 구분되게 한다.
    """
    scenarios = []
    seq = 900001

    def add(cargo_tuple, callsgn, vessel_name, facility, basis):
        nonlocal seq
        name, un, _imdg, _pg = cargo_tuple
        r = make_row(callsgn, vessel_name, "케미칼운반선", name, un, basis, facility, seq)
        rows.append(r)
        seq += 1
        return r

    # ── V-SEG-01 : 혼재금지 — 인화성 가스(2.1) ↔ 인화성 액체(3), 코드 2 ─────
    #    SK5(유류) ↔ SK6(LPG·유류) 는 실제 인접 선석이고 화물-부두 조합도 자연스럽다.
    a, b = ADJACENT_PAIRS[3]                       # SK5부두 / SK6부두
    r1 = add(_v("1203"), "VIO001", "GASOLINE STAR", a, "위반주입-혼재금지")
    r2 = add(VIOLATION_CARGO["프로페인"], "VIO002", "PROPANE CARRIER", b, "위반주입-혼재금지")
    scenarios.append({
        "violation_id": "V-SEG-01", "violation_type": "혼재금지(IMDG 격리)",
        "target_bl_no": f"{r1['bl_no']},{r2['bl_no']}",
        "target_callsgn": f"{r1['callsgn']},{r2['callsgn']}",
        "target_facility": f"{a},{b}",
        "detail": f"인접 선석 {a}(가솔린 UN1203/Class 3) ↔ {b}(프로페인 UN1978/Class 2.1) 동시 취급",
        "rule_basis": f"IMDG 7.2 일반격리표 Class 2.1↔3 = 코드 2 — {SEGREGATION_RULES[0][3]} / ※ 선내 적재 규정을 인접 선석 스크리닝에 차용(법정 위반판정 아님)",
        "rule_authority": AUTHORITY_BORROWED,
        "expected_judgement": "확인필요 경고 — 한 선박에 함께 실렸다면 Separated from(다른 격창) 대상. 인접 선석 동시하역 가부는 터미널 운영규정으로 확인",
    })

    # ── V-SEG-02 : 혼재금지 — 인화성 가스(2.1) ↔ 부식성(8), 코드 1 ──────────
    a2, b2 = ADJACENT_PAIRS[4]                     # SK6부두 / SK7부두
    r3 = add(VIOLATION_CARGO["프로페인"], "VIO003", "LPG PIONEER", a2, "위반주입-혼재금지")
    r4 = add(VIOLATION_CARGO["황산"], "VIO004", "SULFURIC TRADER", b2, "위반주입-혼재금지")
    scenarios.append({
        "violation_id": "V-SEG-02", "violation_type": "혼재금지(IMDG 격리)",
        "target_bl_no": f"{r3['bl_no']},{r4['bl_no']}",
        "target_callsgn": f"{r3['callsgn']},{r4['callsgn']}",
        "target_facility": f"{a2},{b2}",
        "detail": f"인접 선석 {a2}(프로페인 UN1978/Class 2.1) ↔ {b2}(황산 UN1830/Class 8) 동시 취급",
        "rule_basis": f"IMDG 7.2 일반격리표 Class 2.1↔8 = 코드 1 — {SEGREGATION_RULES[2][3]} / ※ 선내 적재 규정을 인접 선석 스크리닝에 차용(법정 위반판정 아님)",
        "rule_authority": AUTHORITY_BORROWED,
        "expected_judgement": "확인필요 경고 — 한 선박에 함께 실렸다면 Away from(수평 3m 이상) 대상",
    })

    # ── V-SEG-03 : 혼재금지 — 독성 가스(2.3) ↔ 인화성 액체(3), 코드 2 ───────
    a3, b3 = ADJACENT_PAIRS[5]                     # SK7부두 / SK8부두
    r9 = add(VIOLATION_CARGO["암모니아"], "VIO009", "AMMONIA CARRIER", a3, "위반주입-혼재금지")
    r10 = add(_v("1993"), "VIO010", "CRUDE RUNNER", b3,
              "위반주입-혼재금지")
    scenarios.append({
        "violation_id": "V-SEG-03", "violation_type": "혼재금지(IMDG 격리)",
        "target_bl_no": f"{r9['bl_no']},{r10['bl_no']}",
        "target_callsgn": f"{r9['callsgn']},{r10['callsgn']}",
        "target_facility": f"{a3},{b3}",
        "detail": f"인접 선석 {a3}(암모니아 UN1005/Class 2.3) ↔ {b3}(석유 UN1993/Class 3) 동시 취급",
        "rule_basis": f"IMDG 7.2 일반격리표 Class 2.3↔3 = 코드 2 — {SEGREGATION_RULES[1][3]} / ※ 선내 적재 규정을 인접 선석 스크리닝에 차용(법정 위반판정 아님)",
        "rule_authority": AUTHORITY_BORROWED,
        "expected_judgement": "확인필요 경고 — 한 선박에 함께 실렸다면 Separated from(다른 격창) 대상",
    })

    # ── V-DG-01 : 위험물 UN 번호 누락 (MSDS 매칭 실패 유도) ─────────────────
    r5 = add((imdg_dgl.DGL["1114"].psn_ko, None, None, None), "VIO005", "UNKNOWN CHEM", "UTK부두",
             "위반주입-UN번호누락")
    r5["cargo_se_name"] = "액체"          # 액체화물인데 UN 번호만 비어 있는 상태
    r5["package_type_name"] = "벌크"
    r5["unload_method_name"] = "펌프"
    r5["quality_flag"] = "MISSING_DG_CODE"
    scenarios.append({
        "violation_id": "V-DG-01", "violation_type": "위험물 정보 누락",
        "target_bl_no": r5["bl_no"], "target_callsgn": r5["callsgn"],
        "target_facility": "UTK부두",
        "detail": "화물명은 벤젠(인화성 액체)인데 dg_un_no 미기재 → MSDS 매칭 불가",
        "rule_basis": "IMO Res. MSC.150(77) — MARPOL Annex I 화물·선박연료유 MSDS 제공 권고 / 「위험물 선박운송 및 저장규칙」 제18조(선장의 의무) 위험물 명세서 기재사항 확인. 미기재 시 MSDS 매칭 불가 → 안전판정 근거 확보 불가",
        "rule_authority": AUTHORITY_STATUTE,
        "expected_judgement": "판정불가 경고 — UN 번호 보완 요청",
    })

    # ── V-PKG-01 : 포장·하역방식 부적합 ────────────────────────────────────
    r6 = add(_v("1093"), "VIO006", "ACRYLO TRADER",
             "정일1부두", "위반주입-포장부적합")
    r6["package_type_name"] = "드럼(일반)"     # 용기등급 Ⅰ 인데 일반 포장
    r6["unload_method_name"] = "크레인"        # 액체인데 크레인 하역
    scenarios.append({
        "violation_id": "V-PKG-01", "violation_type": "포장·하역방식 부적합",
        "target_bl_no": r6["bl_no"], "target_callsgn": r6["callsgn"],
        "target_facility": "정일1부두",
        "detail": "아크릴로니트릴(UN1093/Class 3/용기등급 Ⅰ)을 일반 드럼·크레인 하역으로 신고",
        "rule_basis": "「위험물 선박운송 및 저장규칙」 제20조·「위험물 선박운송 기준」 — 용기등급 Ⅰ(고위험)은 전용 용기·펌프 이송 필요",
        "rule_authority": AUTHORITY_STATUTE,
        "expected_judgement": "포장기준 위반 경고",
    })

    # ── V-DRF-01 : 흘수 초과 (조위 반영 가용수심·UKC 기준) ──────────────────
    #
    #   [실무 반영 정정 2026-08-02]
    #   기존 판정식은 "선박 흘수 < 부두 수심" 이었다. 이건 실무와 다르다.
    #     (1) BERTH_DEPTH_M 은 **해도기준면(Chart Datum) 기준 수심**이다.
    #         실제 그 시각의 가용수심 = 해도수심 + 조위(tide_level).
    #         울산항 대조차는 약 0.3~0.5 m 수준이지만, 대형 유조선은 이 여유로
    #         접안 시각을 조정한다. 조위를 빼면 만조에만 들어올 수 있는 배를
    #         "영구 접안불가"로 잘못 판정한다.
    #     (2) 실무는 흘수와 수심이 같아도 되는 게 아니라 **UKC(Under Keel
    #         Clearance, 용골하 여유수심)** 를 요구한다. 통상 흘수의 10% 또는
    #         최소 여유(항만·선종별로 다름) 중 큰 값을 쓴다.
    #   → 아래 시나리오는 이 두 요소를 명시한 판정식으로 다시 쓴다.
    #     실제 계산은 mart.berth_draught_check 뷰가 수행한다(mart_views.sql).
    draft = 11.0
    depth = BERTH_DEPTH_M["SK1부두"]
    tide_m = 0.4                       # 시나리오 시각의 조위(합성값)
    ukc_req = round(draft * UKC_RATIO, 2)
    available = round(depth + tide_m, 2)
    r7 = add(_v("1993"), "VIO007", "DEEP DRAFT VLCC",
             "SK1부두", "위반주입-흘수초과")
    scenarios.append({
        "violation_id": "V-DRF-01", "violation_type": "흘수 초과(UKC 부족)",
        "target_bl_no": r7["bl_no"], "target_callsgn": r7["callsgn"],
        "target_facility": "SK1부두",
        "detail": (f"SK1부두 해도수심 {depth}m + 조위 {tide_m}m = 가용수심 {available}m, "
                   f"선박 흘수 {draft}m → UKC {round(available - draft, 2)}m "
                   f"(요구 {ukc_req}m 미달, 실제로는 착저)"),
        "rule_basis": (f"가용수심 = 해도수심 + 조위. UKC = 가용수심 − 흘수 ≥ "
                       f"흘수×{UKC_RATIO:.0%}(관행값, 터미널 규정으로 확정 필요)"),
        "rule_authority": AUTHORITY_OPERATIONAL,
        "expected_judgement": "접안 불가 경고 — 대체 선석(SK8, 해도수심 18m) 제안",
    })

    # ── V-WX-01 : 기상 임계 초과 (violation_weather_obs 와 연동) ────────────
    #   [실무 반영] 풍속만 보지 않는다. 액체부두는 **풍향**이 이안풍(offshore)
    #   이면 계류삭 장력이 급증하고, 시정(해무)이 나쁘면 도선 자체가 중단된다.
    #   울산항은 해무 발생이 잦아 시정이 실질적 병목이다.
    r8 = add(_v("1114"), "VIO008", "WEATHER RISK",
             "정일1부두", "위반주입-기상초과")
    scenarios.append({
        "violation_id": "V-WX-01", "violation_type": "기상 임계 초과",
        "target_bl_no": r8["bl_no"], "target_callsgn": r8["callsgn"],
        "target_facility": "정일1부두",
        "detail": ("정일1/2부두 하역중단 기준(풍속 17.0m/s·파고 1.0m) 대비 "
                   "풍속 19.5m/s·파고 1.8m, 풍향 225°(이안풍), 시정 1,200m(해무)"),
        "rule_basis": "data/seed/berth_weather_thresholds_seed.csv — 정일1/2부두(산암리)",
        "rule_authority": AUTHORITY_OPERATIONAL,
        "expected_judgement": "하역중단 경고 — 이안 기준(19m/s)도 초과, 시정 부족으로 도선 제한",
    })

    return rows, scenarios


def build_weather_violation_rows():
    """기상 초과 시나리오(V-WX-01)용 합성 관측 행."""
    return [{
        "observed_at_utc": "2026-07-27 06:00:00+00:00",
        "wind_dir_deg": 225,
        "wind_speed_ms": 19.5,
        "air_temp_c": 28.0,
        "humidity_pct": 82,
        "air_pressure_hpa": 998.0,
        "visibility_m": 1200,
        "wave_height_sig_m": 1.8,
        "violation_id": "V-WX-01",
        "note": "정일1/2부두 기준(중단 풍속17·파고1.0 / 이안 풍속19) 초과",
        "is_synthetic": True,
    }]


# ===========================================================================
def enforce_dgl(rows) -> None:
    """
    생성된 전 행의 (화물명, UN, Class, 용기등급) 조합을 DGL 참조표로 검증한다.
    하나라도 어긋나면 예외를 던져 **CSV 자체가 나오지 않게** 한다.

    조용히 틀린 합성 데이터가 유통되는 것이 이 프로젝트에서 가장 위험하다.
    스키마 39컬럼이 맞아도 내용이 틀리면 아무 의미가 없기 때문이다.
    """
    problems = []
    for r in rows:
        un = r.get("dg_un_no")
        if not un:
            continue                      # 비위험물 또는 UN 누락 시나리오(V-DG-01)
        # manifest 에 Class·PG 컬럼이 없으므로 DGL 값으로 역검증한다:
        # UN 이 참조표에 있고, 화물명이 그 UN 의 국문 통용명과 일치하는가.
        e = imdg_dgl.lookup(un)
        if e is None:
            problems.append(f"{r['bl_no']}: UN{un} 이 DGL 참조표에 없음")
            continue
        if r.get("cargo_name_raw") and r["cargo_name_raw"] != e.psn_ko:
            problems.append(
                f"{r['bl_no']}: UN{e.un_no} 의 통용명은 '{e.psn_ko}' 인데 "
                f"'{r['cargo_name_raw']}' 로 기재됨")
    if problems:
        raise SystemExit(
            "[DGL 검증 실패] 합성 데이터를 생성하지 않는다.\n  - "
            + "\n  - ".join(problems[:20])
            + (f"\n  ... 외 {len(problems) - 20}건" if len(problems) > 20 else ""))


def write_csv(rows, path, columns):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    df = pd.DataFrame(rows)
    for c in columns:                       # 누락 컬럼 보강 후 정본 순서로 정렬
        if c not in df.columns:
            df[c] = None
    df = df[columns]
    # UN 번호는 반드시 문자열로 쓴다. 숫자로 두면 NULL 이 하나만 섞여도 pandas 가
    # float 으로 올려 '1294.0' 이 되고, 4자리 UN 조인이 통째로 깨진다.
    if "dg_un_no" in df.columns:
        df["dg_un_no"] = df["dg_un_no"].apply(
            lambda v: "" if pd.isna(v) else str(v).split(".")[0])
    df.to_csv(path, index=False, encoding="utf-8-sig")
    return df


def main():
    ap = argparse.ArgumentParser(description="합성 화물 manifest 생성 (WBS 1.5/2.4)")
    ap.add_argument("--no-violations", action="store_true",
                    help="파이프라인에는 정상 케이스만 반영")
    ap.add_argument("--allow-stale", action="store_true",
                    help="PORT-MIS staging 이 낡아도 생성 강행(권장하지 않음)")
    args = ap.parse_args()

    pool = build_vessel_pool(args.allow_stale)
    n_real = sum(1 for _, _, _, _, est in pool if not est)
    src = "실제 staging callsgn" if pool else "폴백 가짜 callsgn"

    normal_rows = build_v2(pool)
    v3_rows, scenarios = inject_violations([dict(r) for r in normal_rows])

    # ★ CSV 를 쓰기 전에 DGL 검증. 실패하면 여기서 멈춘다.
    enforce_dgl(v3_rows)

    df_v3 = write_csv(v3_rows, OUT_V3, SCHEMA_COLUMNS)
    _is_violation = df_v3["cargo_basis"].astype(str).str.startswith("위반주입-")
    df_normal = df_v3[~_is_violation]

    target = df_normal if args.no_violations else df_v3
    os.makedirs(STAGING, exist_ok=True)
    target.to_csv(OUT_PIPELINE, index=False, encoding="utf-8-sig")

    os.makedirs(SHARE_DIR, exist_ok=True)
    with open(OUT_SCENARIO, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "violation_id", "violation_type", "target_bl_no", "target_callsgn",
            "target_facility", "detail", "rule_basis", "rule_authority",
            "expected_judgement"])
        w.writeheader()
        w.writerows(scenarios)

    pd.DataFrame(build_weather_violation_rows()).to_csv(
        OUT_WEATHER, index=False, encoding="utf-8-sig")

    liq = df_v3[df_v3["dg_un_no"].astype(str).str.strip() != ""]
    real_basis = df_v3[df_v3["cargo_basis"] == "PORT-MIS 실선종 기반"]
    print("=" * 70)
    print("합성 화물 manifest 생성 완료 (WBS 1.5 / 2.4)")
    print("=" * 70)
    print(f"  [정상+위반]   {OUT_V3}  ({len(df_v3)}행 "
          f"= 정상 {len(df_normal)} + 위반 {len(df_v3) - len(df_normal)})")
    print(f"  [시나리오]    {OUT_SCENARIO}  ({len(scenarios)}건)")
    print(f"  [기상 초과]   {OUT_WEATHER}")
    print(f"  [파이프라인]  {OUT_PIPELINE}  "
          f"({'정상만' if args.no_violations else '정상+위반'})")
    print("  ※ 정상 케이스만 필요하면 cargo_basis 가 '위반주입-' 으로 시작하지 않는 행만 필터")
    print()
    print(f"  스키마 컬럼   : {len(SCHEMA_COLUMNS)}개 (mart_views.sql 정의와 1:1)")
    print(f"  DGL 검증      : 통과 (UN↔화물명 정합 {len(liq)}행)")
    _todo = imdg_dgl.unverified_entries()
    if _todo:
        print(f"  ※ DGL 참조표 {len(_todo)}종은 아직 IMDG Code 원문 미대조 - "
              f"py -m data_pipeline.checks.check_dgl_consistency 로 MSDS 대조 가능")
    print(f"  키 출처       : {src} - 실선종 확인 {n_real}척 / 전체 {len(pool)}척")
    print(f"  위험물 화물   : {len(liq)}행 (UN번호 有)")
    print(f"  화물 배정근거 : 실선종 {len(real_basis)}행 / 추정 {len(df_v3) - len(real_basis)}행")
    print()
    print("  주입된 위반:")
    for s in scenarios:
        print(f"    {s['violation_id']:<10} {s['violation_type']:<20} → {s['expected_judgement']}")
    print()
    print("  ※ 전 행 is_synthetic=True - 실데이터 아님. bl_no/수량은 합성값이며")
    print("    화물 대분류만 PORT-MIS 실신고 선종에 근거함(cargo_basis 참고).")


# 이 스크립트는 CP949 콘솔(윈도우 기본)에서 돌아간다. 진단 출력에 em-dash 같은
# 문자가 하나만 섞여도 UnicodeEncodeError 로 죽고, 하필 생성이 끝난 뒤라 결과 요약만
# 못 보게 된다. 출력 단계에서 인코딩을 UTF-8 로 바꿔 그 함정을 없앤다.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

if __name__ == "__main__":
    main()
