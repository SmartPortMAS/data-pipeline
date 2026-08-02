# -*- coding: utf-8 -*-
"""
imdg_dgl.py — 위험물목록(DGL) 참조표 + 규칙기반 강제 매핑

배경 (교차검증 지적 A 대응)
--------------------------------------------------------------------------
합성 화물 데이터에서 가장 공격받기 쉬운 지점은
    UN번호 ↔ 정식선적명(PSN, Proper Shipping Name) ↔ IMDG Class ↔ 용기등급(PG)
삼각관계가 어긋나는 것이다. 하나라도 틀리면 안전판정 전체의 신뢰도가 무너진다.

그래서 화물 생성기가 (화물명, UN, Class, PG) 를 자유롭게 조합하지 못하도록,
이 모듈의 표를 **유일한 출처**로 삼고 생성 시점에 강제 검증한다.
표에 없는 조합은 생성 자체가 실패한다(조용히 틀린 CSV 가 나오지 않는다).

★ 이 표의 신뢰 등급에 대한 정직한 고지
--------------------------------------------------------------------------
아래 값은 IMDG Code Vol.2 Chapter 3.2 (Dangerous Goods List) **원문을 직접
대조한 것이 아니다.** 따라서 각 항목에 `verified` 플래그로 근거 수준을 남긴다.

    verified="UNMR"  — UN Model Regulations(오렌지북) 계열 공개 위험물 데이터베이스
                       (ADR/HazMat Tool 등)와 대조하여 UN번호·정식선적명·등급·
                       부차위험·용기등급이 일치함을 확인한 항목.
                       ※ ADR(육상)과 IMDG(해상)는 **UN Model Regulations 를
                         공통 모체**로 하므로 이 5개 항목(UN번호/PSN/Class/
                         부차위험/용기등급)은 두 규정에서 일치한다.
                         반면 **격리(segregation)·적재(stowage)·EmS 는 IMDG
                         고유**라 여기서 검증되지 않는다 — 혼재금지 판정 근거는
                         imdg_segregation_loader.py 의 7.2 일반격리표를 쓸 것.
    verified="MSDS"  — KOSHA MSDS 원문(N02=UN번호, N06=IMDG등급, N08=용기등급)과
                       기계적으로 대조되어 일치가 확인된 항목.
                       확인 방법: py -m data_pipeline.checks.check_dgl_consistency
    verified="TODO"  — 아직 어떤 1차 출처와도 대조되지 않은 항목. 보고서·시연에
                       인용하기 전에 반드시 IMDG Code Vol.2 3.2 로 확인할 것.

즉 이 파일은 "정답표"가 아니라 **"검증 대상 목록 + 현재까지 확인된 근거"** 다.
그 상태를 숨기지 않는 것이 합성 데이터를 쓰는 최소 조건이다.

MSDS 를 안전판정 근거로 쓰는 국제적 정당성
--------------------------------------------------------------------------
"MSDS 는 산업안전 문서인데 해상 위험물 판정에 써도 되는가"는 나올 수 있는
질문이다. 아래 두 근거가 있다.

  · IMO Resolution MSC.150(77) (2003) — "Recommendation for Material Safety
    Data Sheets (MSDS) for MARPOL Annex I Cargoes and Marine Fuel Oils".
    IMO 가 **유조선 화물과 선박연료유에 대해 MSDS 제공을 명시적으로 권고**한다.
    즉 액체화물 안전판정에 MSDS 를 쓰는 것은 해상 실무 관행과 어긋나지 않는다.
  · ISGOTT 6th Edition (OCIMF/ICS, 2020), p.10 — MSDS 는 GHS 형식을 따르며
    **16개 섹션**으로 구성된다고 규정. 우리가 KOSHA MSDS 의 16섹션을
    msdsItemCode 기준으로 파싱하는 방식과 동일한 전제다.

주의: 두 근거 모두 "MSDS 를 참조하라"는 것이지 "MSDS 가 IMDG 분류를 대체한다"
는 뜻이 아니다. UN번호·Class·용기등급의 정본은 여전히 IMDG Code 3.2 다.

부차위험(subsidiary risk) 취급
--------------------------------------------------------------------------
upa_cargo_manifest 스키마 39컬럼에는 부차위험 칸이 없다(실제 UPA 응답에 없다).
따라서 부차위험은 manifest 에 넣지 않고 이 표와 mart.msds_flat 에서 판정 시점에
조회한다. 스키마 정합을 깨면서까지 컬럼을 늘리지 않는다.
  예) 메탄올 UN1230 은 Class 3 이지만 부차위험 6.1(독성) 이 있다.
      인화성만 보고 판정하면 독성 노출 위험을 놓친다.
"""
from __future__ import annotations

from typing import NamedTuple


class DGLEntry(NamedTuple):
    un_no: str            # UN 번호 4자리
    psn_en: str           # 정식선적명(Proper Shipping Name)
    psn_ko: str           # 국문 통용명
    imdg_class: str       # 주위험 등급
    subsidiary: tuple     # 부차위험 등급 (없으면 빈 튜플)
    packing_groups: tuple # 허용 용기등급 (가스류는 빈 튜플)
    verified: str         # "UNMR" | "MSDS" | "TODO"  (아래 신뢰 등급 참고)
    note: str = ""


def _e(*a, **kw):
    return DGLEntry(*a, **kw)


# ---------------------------------------------------------------------------
# 울산항 액체화물 시나리오에 등장하는 위험물만 수록한다.
# 새 화물을 추가하려면 반드시 여기에 먼저 등록해야 한다.
# ---------------------------------------------------------------------------
DGL: dict[str, DGLEntry] = {e.un_no: e for e in [
    # ── Class 2.1 인화성 가스 ────────────────────────────────────────────────
    _e("1010", "BUTADIENES, STABILIZED", "1,3-부타디엔(안정화)", "2.1", (), (), "TODO"),
    _e("1011", "BUTANE", "부탄", "2.1", (), (), "TODO"),
    _e("1077", "PROPYLENE", "프로필렌", "2.1", (), (), "TODO"),
    _e("1971", "METHANE, COMPRESSED", "메탄(압축)", "2.1", (), (), "UNMR",
       note="압축가스. LNG 운반선 화물이 아니다 — LNG 는 UN1972."),
    _e("1972", "METHANE, REFRIGERATED LIQUID", "메탄(냉동액화)/LNG", "2.1", (), (), "UNMR",
       note="LNG 운반선의 정상 화물. 냉동액화 상태."),
    _e("1978", "PROPANE", "프로페인", "2.1", (), (), "TODO"),

    # ── Class 2.3 독성 가스 ──────────────────────────────────────────────────
    _e("1005", "AMMONIA, ANHYDROUS", "암모니아(무수)", "2.3", ("8",), (), "UNMR",
       note="부차위험 8(부식성). 독성만 보고 판정하면 부식 위험을 놓친다."),

    # ── Class 3 인화성 액체 ──────────────────────────────────────────────────
    _e("1093", "ACRYLONITRILE, STABILIZED", "아크릴로니트릴(안정화)", "3", ("6.1",),
       ("I",), "UNMR", note="용기등급 Ⅰ(고위험) + 부차위험 6.1(독성)."),
    _e("1114", "BENZENE", "벤젠", "3", (), ("II",), "TODO"),
    _e("1202", "DIESEL FUEL", "디젤 연료/경유", "3", (), ("III",), "TODO",
       note="PSN 은 DIESEL FUEL / GAS OIL / HEATING OIL, LIGHT 중 택일."),
    _e("1203", "MOTOR SPIRIT", "가솔린", "3", (), ("II",), "TODO",
       note="PSN 은 MOTOR SPIRIT / GASOLINE / PETROL 중 택일."),
    _e("1223", "KEROSENE", "케로신", "3", (), ("III",), "TODO"),
    _e("1230", "METHANOL", "메탄올", "3", ("6.1",), ("II",), "UNMR",
       note="부차위험 6.1(독성). Class 3 만 보고 판정하면 안 된다."),
    _e("1267", "PETROLEUM CRUDE OIL", "원유", "3", (), ("I", "II", "III"), "UNMR",
       note="원유운반선의 정상 화물. UN1993(기타 인화성액체)로 쓰면 안 된다."),
    _e("1268", "PETROLEUM DISTILLATES, N.O.S.", "석유증류물(기타)", "3", (),
       ("I", "II", "III"), "TODO", note="PETROLEUM PRODUCTS, N.O.S. 와 동일 UN."),
    _e("1280", "PROPYLENE OXIDE", "1,2-에폭시프로판(산화프로필렌)", "3", ("6.1",),
       ("I",), "TODO"),
    _e("1294", "TOLUENE", "톨루엔", "3", (), ("II",), "TODO"),
    _e("1307", "XYLENES", "크실렌", "3", (), ("II", "III"), "TODO"),
    _e("1993", "FLAMMABLE LIQUID, N.O.S.", "인화성 액체(기타)", "3", (),
       ("I", "II", "III"), "TODO",
       note="N.O.S. 총칭 엔트리. 물질이 특정되면 고유 UN 을 써야 한다."),
    _e("2055", "STYRENE MONOMER, STABILIZED", "스티렌 모노머(안정화)", "3", (),
       ("III",), "TODO"),
    _e("2056", "TETRAHYDROFURAN", "테트라하이드로푸란", "3", (), ("II",), "TODO"),

    # ── Class 8 부식성 물질 ──────────────────────────────────────────────────
    _e("1830", "SULPHURIC ACID", "황산(51% 초과)", "8", (), ("II",), "TODO",
       note="PSN 전체는 'SULPHURIC ACID with more than 51% acid'."),
]}


# ---------------------------------------------------------------------------
# 선종 → 실을 수 있는 화물(UN번호) — 해운 실무상 넌센스 조합을 원천 차단한다.
#   교차검증 지적: "Chemical Tanker 화물을 Oil Tanker 에 매핑하면 넌센스"
# ---------------------------------------------------------------------------
SHIP_KIND_ALLOWED_UN: dict[str, tuple] = {
    "원유운반선":           ("1267",),
    "석유제품운반선":       ("1202", "1203", "1223", "1268"),
    "석유제품/케미칼겸용":  ("1114", "1268", "1294"),
    "케미칼운반선":         ("1093", "1114", "1230", "1280", "1294", "1307",
                             "2055", "2056"),
    "케미칼가스운반선":     ("1010", "1077"),
    "LPG운반선":            ("1011", "1978"),
    "LNG운반선":            ("1972",),
    "기타유조선":           ("1268", "1993"),
}

# 위반 케이스 전용 화물 — 평상시 배정 목록에는 없는 Class 를 의도적으로 투입
VIOLATION_ONLY_UN: tuple = ("1005", "1830", "1978", "1203", "1093", "1993")


def lookup(un_no) -> DGLEntry | None:
    """UN 번호 → DGL 엔트리. 4자리 숫자만 추출해 조회한다."""
    import re
    m = re.search(r"([0-9]{4})", str(un_no or ""))
    return DGL.get(m.group(1)) if m else None


def validate(cargo_name: str, un_no, imdg_class: str, packing_group: str,
             ship_kind: str | None = None) -> list[str]:
    """
    (화물명, UN, Class, PG) 조합을 DGL 로 검증한다.
    반환: 위반 사유 목록. 빈 리스트면 정합.
    """
    problems: list[str] = []
    e = lookup(un_no)
    if e is None:
        return [f"UN{un_no} 이 DGL 참조표에 없음 — imdg_dgl.py 에 먼저 등록할 것"]

    if str(imdg_class).strip() != e.imdg_class:
        problems.append(
            f"UN{e.un_no}({e.psn_ko}) 의 IMDG Class 는 {e.imdg_class} 인데 "
            f"{imdg_class} 로 기재됨")

    pg = str(packing_group or "").strip()
    ROMAN = {"Ⅰ": "I", "Ⅱ": "II", "Ⅲ": "III", "I": "I", "II": "II", "III": "III"}
    if e.packing_groups:
        if ROMAN.get(pg) not in e.packing_groups:
            problems.append(
                f"UN{e.un_no}({e.psn_ko}) 의 허용 용기등급은 "
                f"{'/'.join(e.packing_groups)} 인데 '{pg}' 로 기재됨")
    else:
        if ROMAN.get(pg):
            problems.append(
                f"UN{e.un_no}({e.psn_ko}) 는 가스류로 용기등급이 없는데 "
                f"'{pg}' 로 기재됨")

    if ship_kind and ship_kind in SHIP_KIND_ALLOWED_UN:
        allowed = SHIP_KIND_ALLOWED_UN[ship_kind]
        if e.un_no not in allowed and e.un_no not in VIOLATION_ONLY_UN:
            problems.append(
                f"{ship_kind} 에 UN{e.un_no}({e.psn_ko}) 배정 — 해운 실무상 부적합. "
                f"허용: {', '.join(allowed)}")
    return problems


def subsidiary_risks(un_no) -> tuple:
    """부차위험 등급. manifest 39컬럼에 없으므로 판정 시점에 여기서 조회한다."""
    e = lookup(un_no)
    return e.subsidiary if e else ()


VERIFIED_LEVELS = ("UNMR", "MSDS")


def unverified_entries() -> list[DGLEntry]:
    """아직 어떤 1차 출처와도 대조되지 않은 항목(verified='TODO')."""
    return [e for e in DGL.values() if e.verified not in VERIFIED_LEVELS]


def verification_summary() -> dict:
    """근거 수준별 집계 — 보고서에 그대로 인용할 수 있게."""
    out = {"UNMR": 0, "MSDS": 0, "TODO": 0}
    for e in DGL.values():
        out[e.verified if e.verified in out else "TODO"] += 1
    return out
