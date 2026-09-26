# -*- coding: utf-8 -*-
"""
합성 화물 manifest 생성기 (WBS 1.5 / 2.4)
=========================================
UPA 통합화물 API(getIntgCagInfo)는 업체코드(bzentyCd)가 필수라 자동수집이 불가하다
(활용신청 승인·인증키 정상, 조회 키 값만 부재 — UPA 회신 대기). 그동안 mart 화물
뷰와 안전판정 시나리오를 검증할 수 있도록 합성 manifest 를 만든다.

★ v4 (2026-09-25) — 정상 행은 이제 PORT-MIS **입항 건 단위**로 결정적으로 만든다.
  같은 입항 건(콜사인+입항연도+입항횟수)은 몇 번 돌려도 같은 화물이고, 한 번 적재된
  입항 건은 다시 뽑지 않는다. 규칙과 근거는 아래 'v4' 절 주석. 아래 1) v2 설명은
  이력으로 남긴다 — 선종 기반 무작위 배정·부두 균등 배분은 v4 에서 폐기됐다.

산출물
------
  1) v2 — 정상 케이스 (폐기, 이력) (WBS 1.5 "스키마 정의서 기준 재작성")
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
  py gen_cargo_manifest.py --dry-run       # 생성·검증·요약만 (파일/DB 미기록)
  py gen_cargo_manifest.py                 # 정상+위반 생성, staging CSV 기록
  py gen_cargo_manifest.py --load          # + DB upa_cargo_manifest 에 bl_no 키로 UPSERT
  (자동 실행: pipeline_scheduler 의 cargo 도메인이 10분마다 --load 와 같은 일을 한다)
  py gen_cargo_manifest.py --no-violations # 위반 주입 행 제외
"""
import argparse
import csv
import hashlib
import os
import random
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from data_pipeline.reference import imdg_dgl  # noqa: E402
from data_pipeline.reference import ulsan_terminals  # noqa: E402

# DB 접속정보(.env)를 읽는다. v4 는 DB 가 필수다(없으면 중단).
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
# 스키마 정본 — (2026-09-24부터) backend alembic 0028(alembic/sql/0028_upa_tables.sql)의
# upa_cargo_manifest 와 맞춘다. 예전 정본이던 mart_views.sql 의 CREATE TABLE 껍데기는
# 이관 전 흔적이다. 컬럼 순서까지 맞춰 두면 적재 시 스키마 불일치가 나지 않는다. (WBS 1.5)
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
#   컬럼을 줄이면 이 파일 + backend alembic 마이그레이션 두 곳을 같이 고치고
#   재검증해야 하는데, 실제로 얻는 이득(로직 변화)이 없어 지금 시점엔 손대지
#   않는다. 컬럼이 왜 비어 있는지 몰라 헷갈리는 게 목적이면, 삭제가 아니라
#   이 주석으로 답한다.
# ---------------------------------------------------------------------------
SCHEMA_COLUMNS = [
    "port_code", "ptent_yr", "voyage_no", "callsgn", "vessel_name",
    "vessel_type_name", "vessel_nationality_code", "vessel_nationality_name",
    "mrn_no", "bl_no", "master_bl_no", "io_se_code", "io_se_name",
    "facility_name", "cargo_se_name", "cargo_name_raw", "dg_un_no", "chem_id",
    "cargo_basis", "package_type_name", "unload_method_name",
    "vol_ton_unit_name", "vol_ton", "weight_ton", "vol_size", "weight_size",
    "bulk_vol_size", "bulk_weight_size", "container_count",
    "pod_name", "pol_name", "ldud_port_name", "last_dest_port_name",
    "arrival_at_utc", "customs_progress_status_name",
    "source_system", "source_table", "collected_at_utc",
    "quality_flag", "is_synthetic",
]

# ---------------------------------------------------------------------------
# [v4 — 2026-09-25] 입항 건 단위 결정적 생성
#
# 왜 바꿨나 (실측 근거는 모두 2026-09-25 로컬 DB):
#   · v3 는 콜사인마다 난수로 화물을 뽑고 부두도 용량 균등 배분으로 정했다.
#     두 번 생성하면 같은 배 305척 중 69척(23%)의 화물이 완전히 달라졌고,
#     부두는 실제 입항 부두와 7.5%만 일치했다. 원유 44행은 전부 액체화학 부두에 갔다.
#   · 화물의 키가 콜사인이라 30일 안에 두 번 들어온 배(액체선 약 100척)는
#     지난 항차 화물과 이번 항차 화물이 섞였다.
#
# 규칙:
#   ① 키 = PORT-MIS 입항 건 (callsgn, entry_year, entry_count).
#      난수 시드 = 이 키의 해시 → 몇 번 돌려도 같은 입항 건엔 같은 화물.
#      이미 DB 에 있는 v4 입항 건은 다시 뽑지 않는다(처음 본 시점에 고정).
#      선박위치(upa_vessel_position)가 먼저 알려 준 항차도 같은 키로 합성한다
#      (2026-09-26, load_position_calls) — cargo_basis 에 '입항건=위치(...)' 로 남긴다.
#   ② 화물 계열 = PORT-MIS 대표화물 코드(ldadngFrghtClCd, HS 2자리 — Port-MIS
#      이용코드집 p.81·외항선 신고서 작성요령 "대표화물 2자리 code").
#      코드가 없거나 대응 물질이 없으면(38) 선종 허용 목록(imdg_dgl.SHIP_KIND_ALLOWED_UN).
#   ③ 액화가스선 ↔ 일반 탱커는 물리적으로 화물을 바꿔 실을 수 없으므로 가스 UN 을 가른다.
#   ④ 선석 조건 — 부두명은 berth(정본)로 해소한다.
#        터미널 취급품목(ulsan_terminals.TERMINAL_CARGO_UN, berth.operator_name 으로 연결)
#        > 액체 취급 분류(berth 의 주요취급화물 텍스트 ∪ upa_berth_facility 분류)
#        > 선석 미정/비액체 부두면 조건 없음.
#      두 원천이 자주 어긋난다(UTK 신항부두: berth '목재, 펄프' / UPA '액체화학').
#      한쪽만 액체라고 적은 다목적 부두를 놓치지 않으려고 합집합을 쓴다.
#   ⑤ 종수 = 1 ~ min(선종 상한, 후보 수) 균등. 상한은 대형선 자료 기준이라
#      울산 연안선엔 과대할 수 있다 → cargo_basis 에 '종수가정' 으로 남긴다.
#   ⑥ 선종(또는 대표화물코드) ∩ 선석 조건이 비면 **선종을 따른다**(2026-09-27).
#      선종은 그 배가 물리적으로 실을 수 있는 것이고, 선석 분류는 거칠다(실측:
#      SK1부두 '가스'만, S-OIL3부두 '유류'만 취급으로 돼 있는데 케미칼선이 붙는다).
#      예전엔 여기서 '물질 미특정'을 만들어 133 입항 건(10%)이 화물 없이 판정불가였다.
#      cargo_basis 에 '선석분류 충돌→선종 우선' 으로 남긴다.
#   ⑦ 선종 후보까지 비면 '물질 미특정' 1행(UN 없음) — 지어내지 않는다.
# ---------------------------------------------------------------------------
V4_SOURCE_TABLE = "IntgCagInfo(SYNTHETIC v4)"
WINDOW_DAYS = 30

# HS 2자리 대표화물 코드 → DGL 참조표 UN. 호(4자리) 배정은 코드집 p.89~92 품명 기준
# (예: 벤젠·톨루엔·크실렌은 2707 벤조올·톨루올·크실올 과 2902 환식탄화수소 양쪽).
# 38(각종 화학공업생산품)은 DGL 참조표에 대응 물질이 없어 비워 둔다.
HS_FAMILY_UN: dict[str, frozenset] = {
    "27": frozenset({"1267", "1202", "1203", "1223", "1268",          # 2709·2710
                     "1978", "1011", "1972", "1077", "1010",          # 2711
                     "1114", "1294", "1307"}),                        # 2707
    "28": frozenset({"1830", "1005"}),                                # 2807·2814
    "29": frozenset({"1114", "1294", "1307", "2055",                  # 2902
                     "1230", "1280", "1093", "2056",                  # 2905·2910·2926·2932
                     "1077", "1010"}),                                # 2901 (프로필렌·부타디엔)
    "38": frozenset(),
}

GAS_UN = frozenset({"1978", "1011", "1972", "1077", "1010", "1005"})
GAS_SHIP_KINDS = frozenset({"LPG운반선", "LNG운반선", "케미칼가스운반선"})
LNG_SHIP_KIND, LNG_UN = "LNG운반선", "1972"

# 선종 허용 목록에 없는 액체 관련 선종. 급유선은 DGL 참조표에 있는 연료유만.
SHIP_KIND_EXTRA_UN: dict[str, tuple] = {"급유선": ("1202", "1268")}

# 선석 액체 취급 분류 → UN. 키워드는 berth(웹 '주요취급화물')와 UPA 분류 양쪽 표기.
BERTH_CATEGORY_UN: dict[str, frozenset] = {
    "원유": frozenset({"1267"}),
    "유류": frozenset({"1202", "1203", "1223", "1268", "1993"}),
    "액체화학": frozenset({"1114", "1294", "1093", "1230", "1280", "1307", "2055", "2056", "1830"}),
    "가스": frozenset({"1978", "1011", "1972", "1077", "1010", "1005"}),
}
BERTH_CATEGORY_KEYWORDS: list[tuple[str, str]] = [
    ("원유", "원유"),
    ("유류", "유류"), ("석유정제품", "유류"), ("연료", "유류"),
    ("액체화학", "액체화학"), ("화학공업생산품", "액체화학"), ("유기화합물", "액체화학"),
    ("케미칼", "액체화학"), ("액체화물", "액체화학"),
    ("가스", "가스"),
]

# berth.operator_name → ulsan_terminals.TERMINAL_CARGO_UN 키.
# 한 부두에 운영사가 여럿이면(6부두 등) 어느 선석인지 모르므로 연결하지 않는다.
OPERATOR_TERMINAL: list[tuple[str, str]] = [
    ("정일스톨트헤븐", "JEONGIL_STOLTHAVEN"),
    ("태영인더스트리", "TAEYOUNG"),
    ("오드펠", "ODFJELL"),
    ("유나이티드터미널코리아", "UTK"),
    ("효성", "HYOSUNG"),
    ("현대오일터미널", "HYUNDAI_OIL"),
]

# 한 입항 건에 싣는 화물 종수 상한 (근거: 원유 3계통 직송배관 / MR 탱커 사양 6 grades /
# LPG 대형선 2-grade / LNG 단일). 케미칼·급유선은 후보 수까지. 기타유조선은 근거 없어
# 석유제품운반선 값을 빌린다(가정).
PARCEL_MAX: dict[str, int] = {
    "원유운반선": 3, "석유제품운반선": 6, "석유제품/케미칼겸용": 6, "기타유조선": 6,
    "LPG운반선": 2, "케미칼가스운반선": 2, "LNG운반선": 1,
}

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


# ===========================================================================
# 공통 유틸
# ===========================================================================
def _read_db(sql, params=None):
    """로컬 DB 에서 읽는다. 접속이 안 되면 None.

    v4 는 staging 폴백이 없다 — 입항 건·선석 정본·VTS 이력이 모두 DB 에만 있고,
    그중 하나라도 빠진 채 생성하면 "같은 입항 건 = 같은 화물" 이 깨진다.
    """
    try:
        from sqlalchemy import create_engine, text
        url = os.getenv("DATABASE_URL")
        if not url:
            host = os.getenv("POSTGRES_HOST"); port = os.getenv("POSTGRES_PORT")
            db = os.getenv("POSTGRES_DB"); user = os.getenv("POSTGRES_USER")
            pw = os.getenv("POSTGRES_PASSWORD")
            if not all([host, port, db, user, pw]):
                return None
            url = f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}"
        url = url.replace("+asyncpg", "+psycopg2")
        with create_engine(url).connect() as conn:
            return pd.read_sql(text(sql), conn, params=params or {})
    except Exception as e:
        print(f"  (DB 조회 불가: {str(e)[:120]})")
        return None


# io_se_code(I/O)와 io_se_name(수입/수출)은 하나를 정하고 나머지를 파생시킨다.
# 수입이면 울산이 도착지(pod), 수출이면 울산이 출발지(pol).
IO_SE_NAME_BY_CODE = {"I": "수입", "O": "수출"}
FOREIGN_PORTS = ["SINGAPORE", "DALIAN", "휴스턴", "여수", "ONSAN"]


def make_row(callsgn, vessel_name, cat, cargo, un_no, basis, facility, seq):
    """위반 주입(v3 시나리오)용 1행. 정상 행은 make_call_row 를 쓴다."""
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
# v4 — 입항 건 단위 결정적 생성
# ===========================================================================
def _norm(s) -> str:
    return "" if s is None or (isinstance(s, float) and pd.isna(s)) else str(s).strip()


def call_key(cs, year, count) -> str:
    return f"{_norm(cs).upper()}_{int(year)}_{int(count):03d}"


def _rng_for(key: str) -> random.Random:
    """입항 건 키 → 고정 시드. 생성 순서·다른 배의 유무와 무관하게 같은 값."""
    return random.Random(int(hashlib.sha256(key.encode("utf-8")).hexdigest()[:16], 16))


def load_calls() -> pd.DataFrame:
    """최근 WINDOW_DAYS 일 안에 입항(예정 포함)한 액체화물선·급유선의 PORT-MIS 입항 건."""
    df = _read_db(f"""
        SELECT upper(btrim(callsgn)) AS cs, entry_year, entry_count, vessel_name,
               ship_kind_category, is_liquid_cargo_vessel, is_bunkering_vessel,
               cargo_class_code, arrival_at_utc,
               arrival_facility_cd, arrival_facility_sub_code, arrival_facility_nm
        FROM portmis_vessel
        WHERE arrival_at_utc >= now() - interval '{WINDOW_DAYS} days'
          AND (is_liquid_cargo_vessel OR is_bunkering_vessel)
          AND nullif(btrim(callsgn), '') IS NOT NULL
          AND entry_year IS NOT NULL AND entry_count IS NOT NULL
    """)
    if df is None:
        raise SystemExit("[중단] PORT-MIS 입항 건을 DB 에서 읽지 못했다 — v4 는 DB 가 필수다.")
    df["key_src"] = "PORT-MIS"
    return df.sort_values(["cs", "arrival_at_utc"]).reset_index(drop=True)


def load_position_calls(pm_keys: set) -> pd.DataFrame:
    """선박위치(upa_vessel_position)에만 있는 입항 건 — PORT-MIS 에 아직 없는 항차.

    [2026-09-26] 위치 API 의 입항횟수(vyg)는 PORT-MIS 의 입항횟수와 같은 키다(로컬 DB
    실측: 둘 다 값이 있는 307척 중 279척 일치). 그런데 위치 API 가 새 항차를 먼저
    알려 주는 경우가 있다(28척, 대부분 +1). 판정은 그 항차 키로 화물을 찾으므로
    PORT-MIS 입항 건만 합성하면 그 배는 '화물 없음'이 된다.

    선종은 배의 속성이라 항차가 바뀌어도 같다 — 같은 콜사인의 가장 최근 PORT-MIS
    기록에서 가져온다. 대표화물 코드는 항차마다 다르므로 가져오지 않는다(선종 기준).
    입항 시각은 위치에 처음 잡힌 시각으로 대신한다. 나중에 PORT-MIS 신고가 들어와도
    키가 같아 load_frozen_rows 가 고정하므로 화물은 바뀌지 않는다.
    """
    pos = _read_db(f"""
        SELECT upper(btrim(callsgn)) AS cs, ptent_yr::int AS entry_year,
               voyage_no::int AS entry_count, min(received_at_utc) AS arrival_at_utc
        FROM upa_vessel_position
        WHERE received_at_utc >= now() - interval '{WINDOW_DAYS} days'
          AND nullif(btrim(callsgn), '') IS NOT NULL
          AND ptent_yr IS NOT NULL AND voyage_no IS NOT NULL
        GROUP BY 1, 2, 3
    """)
    kinds = _read_db("""
        SELECT DISTINCT ON (upper(btrim(callsgn))) upper(btrim(callsgn)) AS cs, vessel_name,
               ship_kind_category, is_liquid_cargo_vessel, is_bunkering_vessel
        FROM portmis_vessel
        WHERE nullif(btrim(callsgn), '') IS NOT NULL
        ORDER BY upper(btrim(callsgn)), arrival_at_utc DESC NULLS LAST
    """)
    if pos is None or kinds is None or pos.empty:
        return pd.DataFrame()
    df = pos.merge(kinds, on="cs", how="inner")
    df = df[(df["is_liquid_cargo_vessel"] == True) | (df["is_bunkering_vessel"] == True)]  # noqa: E712
    df = df[[call_key(r.cs, r.entry_year, r.entry_count) not in pm_keys for r in df.itertuples()]]
    for col in ("cargo_class_code", "arrival_facility_cd", "arrival_facility_sub_code", "arrival_facility_nm"):
        df[col] = None
    df["key_src"] = "위치(PORT-MIS 미신고)"
    return df.reset_index(drop=True)


def load_berth_profiles() -> dict:
    """부두명(berth 정본) → {cats, cat_src, terminal}.

    액체 취급 분류는 berth(웹 '주요취급화물' 텍스트)와 upa_berth_facility(API 분류)의
    합집합. 운영사는 berth.operator_name 으로 터미널 취급품목 표에 연결한다.
    """
    b = _read_db("""
        SELECT wharf_name,
               string_agg(DISTINCT coalesce(handling_cargo_name, ''), ' / ') AS b_cargo,
               array_agg(DISTINCT operator_name) FILTER (WHERE operator_name IS NOT NULL) AS b_ops
        FROM berth GROUP BY wharf_name
    """)
    u = _read_db("""
        SELECT wharf_name, string_agg(DISTINCT coalesce(handling_cargo_name, ''), ' / ') AS u_cargo
        FROM upa_berth_facility GROUP BY wharf_name
    """)
    if b is None:
        raise SystemExit("[중단] berth 표를 읽지 못했다.")
    u_map = dict(zip(u["wharf_name"], u["u_cargo"])) if u is not None else {}

    def cats_of(text: str) -> set:
        return {cat for kw, cat in BERTH_CATEGORY_KEYWORDS if kw in (text or "")}

    prof = {}
    for _, r in b.iterrows():
        bc, uc = cats_of(r["b_cargo"]), cats_of(u_map.get(r["wharf_name"], ""))
        src = []
        if bc: src.append("berth")
        if uc: src.append("UPA")
        ops = [o for o in (r["b_ops"] or []) if _norm(o)]
        terminal = None
        if len(ops) == 1:
            for kw, key in OPERATOR_TERMINAL:
                if kw in ops[0] and ulsan_terminals.cargo_un_for_terminal(key):
                    terminal = key
                    break
        prof[r["wharf_name"]] = {"cats": bc | uc, "cat_src": "+".join(src), "terminal": terminal}
    return prof


def load_facility_resolvers():
    """PORT-MIS 시설코드·시설명 → berth 부두명, 그리고 VTS 접안 이력."""
    fmap = _read_db("""
        SELECT facility_cd, coalesce(facility_sub_code, '') AS sub, wharf_name
        FROM portmis_facility_map WHERE wharf_name IS NOT NULL
    """)
    alias = _read_db("""
        SELECT a.source_name, a.wharf_name FROM mart.facility_alias a
        WHERE a.facility_type = 'BERTH'
          AND EXISTS (SELECT 1 FROM berth b WHERE b.wharf_name = a.wharf_name)
    """)
    vts = _read_db(f"""
        SELECT upper(btrim(u.callsgn)) AS cs, u.arrival_at_utc AS t, a.wharf_name
        FROM upa_port_call u
        JOIN mart.facility_alias a ON a.source_name = u.facility_name AND a.facility_type = 'BERTH'
        WHERE u.io_vts_name IN ('입항', '접안', '이선')
          AND u.arrival_at_utc >= now() - interval '{WINDOW_DAYS + 5} days'
          AND EXISTS (SELECT 1 FROM berth b WHERE b.wharf_name = a.wharf_name)
    """)
    fmap_d = {(r.facility_cd, _norm(r.sub)): r.wharf_name for r in (fmap.itertuples() if fmap is not None else [])}
    alias_d = dict(zip(alias["source_name"], alias["wharf_name"])) if alias is not None else {}
    vts_by_cs = {cs: g.sort_values("t") for cs, g in vts.groupby("cs")} if vts is not None else {}
    return fmap_d, alias_d, vts_by_cs


def resolve_wharf(call, next_arrival, fmap_d, alias_d, vts_by_cs):
    """입항 건의 선석(berth 부두명)과 근거. 못 찾으면 (None, '선석 미정').

    PORT-MIS 가 선석을 적어 준 경우가 1순위다. 정박지 신고(예정·대기)면
    그 입항 건 기간(입항 1일 전 ~ 다음 입항 건 전) 안의 VTS 첫 접안 선석을 쓴다.
    """
    nm = _norm(call.arrival_facility_nm)
    if nm and "정박지" not in nm:
        w = fmap_d.get((_norm(call.arrival_facility_cd), _norm(call.arrival_facility_sub_code)))
        if w:
            return w, "PORT-MIS 선석"
        w = alias_d.get(nm)
        if w:
            return w, "PORT-MIS 선석"
    g = vts_by_cs.get(call.cs)
    if g is not None and pd.notna(call.arrival_at_utc):
        lo = call.arrival_at_utc - pd.Timedelta(days=1)
        hi = next_arrival if next_arrival is not None and pd.notna(next_arrival) \
            else call.arrival_at_utc + pd.Timedelta(days=WINDOW_DAYS)
        hit = g[(g["t"] >= lo) & (g["t"] < hi)]
        if not hit.empty:
            return hit.iloc[0]["wharf_name"], "VTS 접안"
    return None, "선석 미정"


def candidates_for(call, wharf, prof):
    """(후보 UN 집합, 근거 문자열들)."""
    kind = _norm(call.ship_kind_category)
    code = _norm(call.cargo_class_code).split(".")[0]
    # 코드가 있어도 대응 물질이 없으면(38 각종 화학공업생산품) 선종 목록으로 간다 — 규칙 ②
    if HS_FAMILY_UN.get(code):
        base, why = set(HS_FAMILY_UN[code]), [f"대표화물코드 {code}"]
    else:
        base = set(imdg_dgl.SHIP_KIND_ALLOWED_UN.get(kind, ())) | set(SHIP_KIND_EXTRA_UN.get(kind, ()))
        why = [f"선종({kind or '미상'})"]
    base = (base & GAS_UN) if kind in GAS_SHIP_KINDS else (base - GAS_UN)
    # LNG(UN1972, 극저온 메탄)는 LNG선만 싣는다. 대표화물코드 27 은 가스 계열 전체라
    # LPG선·케미칼가스선에도 LNG 가 뽑혔고, LNG 는 MSDS 가 없어 판정불가가 됐다(9/27 실측 3건).
    if kind != LNG_SHIP_KIND:
        base.discard(LNG_UN)
    else:
        base = {LNG_UN}
    # 급유선은 대표화물코드가 27(광물성연료)이어도 싣는 건 연료유다 — 계열 전체
    # (가솔린·벤젠…)로 넓히면 급유선이 5종을 싣는 비현실적 결과가 나온다(실측).
    if kind in SHIP_KIND_EXTRA_UN:
        base &= set(SHIP_KIND_EXTRA_UN[kind])

    p = prof.get(wharf) if wharf else None
    if p and p["terminal"]:
        cand = base & set(ulsan_terminals.cargo_un_for_terminal(p["terminal"]))
        why.append(f"터미널 취급품목({p['terminal']})")
    elif p and p["cats"]:
        allowed = set().union(*(BERTH_CATEGORY_UN[c] for c in p["cats"]))
        cand = base & allowed
        why.append(f"선석분류({'·'.join(sorted(p['cats']))}/{p['cat_src']})")
    else:
        cand = base
        if wharf:
            why.append("선석조건 없음")        # 선석 미정은 resolve_wharf 근거가 따로 붙는다
    if not cand and base and p and (p["terminal"] or p["cats"]):
        cand = base                             # 규칙 ⑥ — 선석 조건과 충돌하면 선종을 따른다
        why.append("선석분류 충돌→선종 우선")
    return cand, why


def make_call_row(call, wharf, un, parcel_no, n_parcels, basis, rng):
    e = imdg_dgl.DGL.get(un) if un else None
    wton = round(rng.uniform(500, 30000), 1)
    io = rng.choice(["I", "O"])
    key = call_key(call.cs, call.entry_year, call.entry_count)
    return {
        "port_code": "KRUSN",
        "ptent_yr": str(int(call.entry_year)),
        "voyage_no": f"{int(call.entry_count):03d}",
        "callsgn": call.cs,
        "vessel_name": _norm(call.vessel_name),
        "vessel_type_name": _norm(call.ship_kind_category),
        "vessel_nationality_code": None,
        "vessel_nationality_name": None,
        "mrn_no": f"SYN{key}",
        "bl_no": f"SYN-{key}-{parcel_no}",
        "master_bl_no": None,
        "io_se_code": io,
        "io_se_name": IO_SE_NAME_BY_CODE[io],
        "facility_name": wharf or _norm(call.arrival_facility_nm) or None,
        "cargo_se_name": "액체",
        "cargo_name_raw": e.psn_ko if e else "물질 미특정",
        "dg_un_no": un,
        "cargo_basis": basis,
        "package_type_name": "벌크",
        "unload_method_name": "펌프",
        "vol_ton_unit_name": "TON",
        "vol_ton": wton, "weight_ton": wton,
        "vol_size": None, "weight_size": None,
        "bulk_vol_size": round(wton * 1.1, 1), "bulk_weight_size": wton,
        "container_count": None,
        "pod_name": "울산" if io == "I" else None,
        "pol_name": None if io == "I" else "울산",
        "ldud_port_name": None, "last_dest_port_name": None,
        "arrival_at_utc": call.arrival_at_utc.isoformat() if pd.notna(call.arrival_at_utc) else None,
        "customs_progress_status_name": None,
        "source_system": "UPA_SYNTHETIC",
        "source_table": V4_SOURCE_TABLE,
        "collected_at_utc": pd.Timestamp.now("UTC").isoformat(),
        "quality_flag": "OK" if un else "UNSPECIFIED_SUBSTANCE",
        "is_synthetic": True,
    }


def load_frozen_rows():
    """이미 적재된 v4 행. 이 입항 건들은 다시 뽑지 않는다(처음 본 시점에 고정).

    [2026-09-27] 단, 화물을 하나도 특정하지 못한 입항 건(모든 행의 chem_id 가 없음 —
    '물질 미특정'이거나 MSDS 없는 UN)은 고정하지 않는다. 규칙 ⑥(선종 우선)과 LNG 제한을
    이미 본 입항 건에도 적용하기 위해서다. 화물이 정해진 입항 건은 그대로 둔다.
    """
    cols = ", ".join(c for c in SCHEMA_COLUMNS if c != "chem_id")
    df = _read_db(f"""
        SELECT {cols} FROM upa_cargo_manifest m
        WHERE source_table = :st
          AND EXISTS (SELECT 1 FROM upa_cargo_manifest k
                      WHERE k.source_table = m.source_table
                        AND k.callsgn = m.callsgn AND k.ptent_yr = m.ptent_yr
                        AND k.voyage_no = m.voyage_no
                        AND nullif(btrim(k.chem_id::text), '') IS NOT NULL)
    """, {"st": V4_SOURCE_TABLE})
    if df is None or df.empty:
        return [], set()
    keys = {call_key(r.callsgn, r.ptent_yr, r.voyage_no) for r in df.itertuples()}
    return df.to_dict("records"), keys


def build_v4():
    calls = load_calls()
    pm_keys = {call_key(r.cs, r.entry_year, r.entry_count) for r in calls.itertuples()}
    pos_calls = load_position_calls(pm_keys)
    if not pos_calls.empty:
        calls = pd.concat([calls, pos_calls], ignore_index=True)
    prof = load_berth_profiles()
    fmap_d, alias_d, vts_by_cs = load_facility_resolvers()
    frozen_rows, frozen_keys = load_frozen_rows()

    rows, stats = list(frozen_rows), {"calls": 0, "frozen": 0, "new": 0, "unspecified": 0,
                                      "wharf_src": {}, "family_src": {"code": 0, "shipkind": 0},
                                      "key_src": {}}
    for cs, g in calls.groupby("cs", sort=False):
        g = g.sort_values("arrival_at_utc").reset_index(drop=True)
        for i, call in enumerate(g.itertuples()):
            stats["calls"] += 1
            key = call_key(call.cs, call.entry_year, call.entry_count)
            if key in frozen_keys:
                stats["frozen"] += 1
                continue
            nxt = g.loc[i + 1, "arrival_at_utc"] if i + 1 < len(g) else None
            wharf, wsrc = resolve_wharf(call, nxt, fmap_d, alias_d, vts_by_cs)
            stats["wharf_src"][wsrc] = stats["wharf_src"].get(wsrc, 0) + 1
            stats["key_src"][call.key_src] = stats["key_src"].get(call.key_src, 0) + 1
            cand, why = candidates_for(call, wharf, prof)
            if call.key_src != "PORT-MIS":
                why = why + [f"입항건={call.key_src}"]
            stats["family_src"]["code" if why[0].startswith("대표화물코드") else "shipkind"] += 1
            rng = _rng_for(key)
            kind = _norm(call.ship_kind_category)
            if not cand:
                basis = "·".join(why + [wsrc, "물질 미특정"])
                rows.append(make_call_row(call, wharf, None, 1, 1, basis, rng))
                stats["unspecified"] += 1
            else:
                cap = min(PARCEL_MAX.get(kind, len(cand)), len(cand))
                k = rng.randint(1, cap)
                chosen = rng.sample(sorted(cand), k)
                basis = "·".join(why + [wsrc, f"종수가정 {k}/{cap}"])
                for n, un in enumerate(chosen, 1):
                    rows.append(make_call_row(call, wharf, un, n, k, basis, rng))
            stats["new"] += 1
    return rows, stats


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


# ---------------------------------------------------------------------------
# UN 번호 → MSDS chem_id 확정 매핑
#
# [2026-09-21] 왜 만들었나 — UN 번호 조인이 두 가지로 깨지고 있었다.
#
#   (1) 다대일(fan-out). UN 번호는 화학물질 식별자가 아니라 **운송 분류 코드**라
#       한 UN 에 여러 물질이 붙는다. mart.cargo_msds 가 UN 으로 LEFT JOIN 하면
#       매니페스트 588행이 710행으로 불어난다(실측). 게다가 arrival_watcher 는
#       그중 `LIMIT 1` 로 하나를 집는데 ORDER BY 가 없다 —
#       UN1993 화물의 안전판정이 '석유'로 갈지 '옥타메틸사이클로테트라실록산'
#       으로 갈지가 우연에 달려 있었다. 인화점·IMDG등급이 달라 판정이 바뀐다.
#         UN1993 -> 4개, UN3082 -> 6개, UN3295/1986 -> 4개, UN1268/1307 -> 2개
#
#   (2) 원유가 영영 안 붙는다. KOSHA MSDS 는 석유(PETROLEUM, CAS 8002-05-9)에
#       **UN1993**(총칭 N.O.S.)을 부여했다. 반면 이 생성기는 2026-08-02 정정에서
#       원유의 고유 엔트리 **UN1267**(PETROLEUM CRUDE OIL)로 바꿨다(모듈 상단
#       정정이력 참고). 양쪽 다 각자 맞지만 UN 으로는 만나지 않는다.
#       결과: 원유운반선 20척 전부 chem_id 확보 실패 → arrival_watcher 가
#       "화물 미식별"로 건너뜀. LNG운반선 2척도 같다(메탄 CAS 74-82-8 미수집).
#       울산항은 원유항인데 원유선이 자동 추천에서 통째로 빠져 있었다.
#
# 그래서 UN 조인을 버리고 **생성 시점에 chem_id 를 못 박는다.** 화물을 고른 쪽이
# 어느 물질인지 알고 있으므로, 그 지식을 소비 측에서 다시 추측하게 두지 않는다.
#
# 값의 근거: mart.msds_flat 전수 조회(2026-09-21). 후보가 둘 이상인 UN 은
# 아래에 선택 이유를 남긴다. 후보가 하나뿐인 UN 은 기계적 매칭이다.
# ---------------------------------------------------------------------------
UN_TO_CHEM_ID: dict[str, str] = {
    # ── 후보 1개 (기계적 매칭) ────────────────────────────────────────────
    "1005": "001174",   # 암모니아(무수)
    "1010": "001167",   # 1,3-부타디엔
    "1011": "000247",   # 부탄
    "1077": "002524",   # 프로필렌
    "1093": "001143",   # 아크릴로니트릴
    "1114": "001008",   # 벤젠
    "1202": "000973",   # 디젤 연료
    "1203": "016420",   # 가솔린
    "1223": "000756",   # 케로젠
    "1230": "001151",   # 메틸 알코올(메탄올)
    "1280": "001162",   # 1,2-에폭시프로판(산화프로필렌)
    "1294": "001032",   # 톨루엔
    "1830": "001049",   # 황산
    "1978": "015420",   # 프로페인
    "2055": "001027",   # 스티렌
    "2056": "001041",   # 테트라하이드로푸란

    # ── 후보 2개 이상 — 선택 이유 명시 ────────────────────────────────────
    # 1268 후보: 001128 스토다드 솔벤트(CAS 8052-41-3) / 016495 러버 솔벤트(8030-30-6)
    #   둘 다 석유계 혼합용제다. 스토다드 솔벤트가 '석유증류물(기타)'에 대응하는
    #   표준 엔트리이고 러버 솔벤트는 고무공업용 협의 제품이라 전자를 쓴다.
    "1268": "001128",
    # 1307 후보: 000233 p-크실렌(CAS 106-42-3) / 001077 크실렌(CAS 1330-20-7)
    #   매니페스트의 화물명이 '크실렌'(혼합 이성질체)이다. CAS 1330-20-7 이
    #   혼합 크실렌이고 106-42-3 은 파라 이성질체 단일 물질이라 전자를 쓴다.
    "1307": "001077",
    # 1993 후보: 000751 석유 / 000454 옥타메틸사이클로테트라실록산 /
    #            000852 에틸리덴 노보르닌 / 002093 뷰틸 뷰틸산
    #   UN1993 은 N.O.S.(품명 미지정) 총칭 엔트리다. 이 생성기가 UN1993 을
    #   쓰는 곳은 '선종미상 액체선 폴백'과 '인화성 액체(기타)' 두 가지로, 둘 다
    #   "석유계 인화성 액체인데 물질이 특정되지 않음"을 뜻한다. KOSHA 자신이
    #   석유(PETROLEUM)에 UN1993 을 부여했으므로 그 물질을 대표로 쓴다.
    "1993": "000751",

    # ── UN 으로는 절대 안 붙는 항목 (이 표가 존재하는 진짜 이유) ───────────
    # 원유. MSDS 에는 '석유(PETROLEUM)' CAS 8002-05-9 로 **존재한다** —
    # 8002-05-9 는 원유(crude petroleum)의 CAS 다. 다만 KOSHA 가 UN 을 1993 으로
    # 기록해 UN1267 조인으로는 닿지 않는다. 여기서 직접 이어 준다.
    "1267": "000751",
}

# MSDS 151종에 대응 물질이 없는 UN. 조용히 NULL 이 되지 않도록 명시한다.
# LNG(메탄, CAS 74-82-8)는 수집 대상 169→151 선별에서 빠졌다.
# 울산항 LNG운반선은 실측 2척으로 물량이 작지만, '없어서 비었다'와
# '매핑을 안 해서 비었다'는 구분되어야 한다.
UN_WITHOUT_MSDS: dict[str, str] = {
    "1972": "메탄(냉동액화)/LNG — CAS 74-82-8 이 MSDS 151종에 없음",
}


def chem_id_for(un_no) -> str:
    """UN 번호 → chem_id. 매핑이 없으면 빈 문자열.

    표에 없는 UN 이 나오면 조용히 넘기지 않고 경고한다 — 새 화물을 추가하고
    매핑을 빠뜨리면 그 화물은 안전판정에서 통째로 빠지기 때문이다.
    """
    import re

    m = re.search(r"([0-9]{4})", str(un_no or ""))
    if not m:
        return ""
    un = m.group(1)
    if un in UN_TO_CHEM_ID:
        return UN_TO_CHEM_ID[un]
    if un in UN_WITHOUT_MSDS:
        return ""
    print(f"  [경고] UN{un} 의 chem_id 매핑이 없습니다 — "
          f"이 화물은 MSDS 근거 없이 적재됩니다. UN_TO_CHEM_ID 에 추가하세요.")
    return ""


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
        # chem_id 는 **최종 확정된 dg_un_no** 에서 파생한다. 위반 주입이
        # dg_un_no 를 바꾸는 경우(UN번호 누락 위반 등)도 여기서 함께 반영된다 —
        # make_row 시점에 넣으면 위반 주입 뒤 값이 어긋난다.
        df["chem_id"] = df["dg_un_no"].apply(chem_id_for)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    return df


def load_manifest_to_db() -> None:
    """staging 의 적하목록 **한 파일만** DB 에 bl_no 키로 UPSERT 한다.

    upa_loader.load_all() 은 staging 의 UPA 파일 전부를 적재하므로 쓰지 않는다 —
    data/staging 에 남은 오래된 선박위치·입항이력 CSV 가 최신 DB 행을 덮어쓸 수 있다.
    (2026-09-26) 예전엔 적재 전에 표를 TRUNCATE 했다. 이제 bl_no 가 입항 건 키로
    결정적이라 UPSERT 한다(backend alembic 0031) — 표가 비는 순간이 없다.
    고정된 입항 건(load_frozen_rows)은 DB 값 그대로 다시 실리므로 갱신돼도 같은 값이다.
    """
    from data_pipeline.common_pg_loader import load_all
    from data_pipeline.upa.upa_loader import TABLE_MAP

    fname = os.path.basename(OUT_PIPELINE)
    load_all({fname: TABLE_MAP[fname]}, staging_dir=os.path.dirname(OUT_PIPELINE), auto_create=False)


def main():
    ap = argparse.ArgumentParser(description="합성 화물 manifest 생성 v4 (입항 건 단위, 결정적)")
    ap.add_argument("--no-violations", action="store_true",
                    help="위반 주입 행(가짜 콜사인 VIO*)을 빼고 정상 행만")
    ap.add_argument("--dry-run", action="store_true",
                    help="생성·검증·요약만 하고 파일/DB 에 쓰지 않는다")
    ap.add_argument("--load", action="store_true",
                    help="생성 후 DB upa_cargo_manifest 에 bl_no 키로 UPSERT")
    args = ap.parse_args()
    run(no_violations=args.no_violations, dry_run=args.dry_run, load=args.load)


def run(no_violations=False, dry_run=False, load=False, write_samples=True):
    """생성 → 검증 → staging CSV → (load 면) DB UPSERT.

    pipeline_scheduler 의 cargo 도메인은 write_samples=False 로 부른다 — samples/ 는
    git 에 올라가는 공유용 사본이라 운영 서버에서 주기적으로 고쳐 쓰지 않는다.
    """
    # 위반 주입 행(make_row)은 모듈 전역 random 을 쓴다. 스케줄러처럼 한 프로세스에서
    # 여러 번 부르면 난수 상태가 이어져 같은 bl_no 의 수량·항차가 회차마다 바뀐다.
    random.seed(20260727)
    normal_rows, st = build_v4()
    rows = [dict(r) for r in normal_rows]
    scenarios = []
    if not no_violations:
        rows, scenarios = inject_violations(rows)

    enforce_dgl(rows)                              # ★ 실패하면 여기서 멈춘다

    df = pd.DataFrame(rows)
    liquid = df[df["dg_un_no"].fillna("").astype(str).str.strip() != ""]
    v4 = df[df["source_table"] == V4_SOURCE_TABLE]
    per_call = v4.groupby(["callsgn", "ptent_yr", "voyage_no"]).size()
    print("=" * 70)
    print("합성 화물 manifest v4 — 입항 건 단위 결정적 생성")
    print("=" * 70)
    print(f"  대상 입항 건    : {st['calls']}  (최근 {WINDOW_DAYS}일, 액체화물선·급유선)")
    print(f"    · 기존 고정   : {st['frozen']}   · 신규 생성 : {st['new']}")
    print(f"  선석 근거       : {st['wharf_src']}")
    print(f"  입항 건 출처    : {st['key_src']}  (신규 생성분 기준)")
    print(f"  화물 계열 근거  : 대표화물코드 {st['family_src']['code']} / 선종 {st['family_src']['shipkind']}")
    print(f"  물질 미특정     : {st['unspecified']} 입항 건")
    print(f"  v4 행           : {len(v4)}  (입항 건당 화물 평균 {per_call.mean():.2f}, 최대 {per_call.max()})")
    print(f"  위반 주입 행    : {len(df) - len(v4)}  ({'제외' if no_violations else '포함'})")
    print(f"  UN 번호 있는 행 : {len(liquid)}   DGL 검증 통과")

    if dry_run:
        print("\n  [dry-run] 파일·DB 에 쓰지 않았다.")
        return

    write_csv(rows, OUT_PIPELINE, SCHEMA_COLUMNS)
    if write_samples:
        os.makedirs(SHARE_DIR, exist_ok=True)
        write_csv(rows, OUT_V3, SCHEMA_COLUMNS)
        with open(OUT_SCENARIO, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=[
                "violation_id", "violation_type", "target_bl_no", "target_callsgn",
                "target_facility", "detail", "rule_basis", "rule_authority",
                "expected_judgement"])
            w.writeheader()
            w.writerows(scenarios)
        pd.DataFrame(build_weather_violation_rows()).to_csv(OUT_WEATHER, index=False, encoding="utf-8-sig")
    print(f"\n  [파이프라인] {OUT_PIPELINE}")

    if load:
        load_manifest_to_db()


# 이 스크립트는 CP949 콘솔(윈도우 기본)에서 돌아간다. 진단 출력에 em-dash 같은
# 문자가 하나만 섞여도 UnicodeEncodeError 로 죽고, 하필 생성이 끝난 뒤라 결과 요약만
# 못 보게 된다. 출력 단계에서 인코딩을 UTF-8 로 바꿔 그 함정을 없앤다.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

if __name__ == "__main__":
    main()
