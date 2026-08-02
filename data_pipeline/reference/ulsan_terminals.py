# -*- coding: utf-8 -*-
"""
ulsan_terminals.py — 울산항 액체화물 터미널 제원 참조표

출처
--------------------------------------------------------------------------
울산항만공사(UPA) 「Ulsan Port, Connect UP — 액체화물의 새로운 Connect를 만듭니다」
액체화물 터미널 소개 브로슈어 (www.upa.or.kr, 기업지원센터 발간).
**항만공사가 직접 발간한 공식 자료**이며, 각 터미널이 제출한 제원을 담고 있다.

이 표가 채우는 공백
--------------------------------------------------------------------------
그동안 "미확보 — 문서를 구해야 한다"로 보고했던 항목 중 아래가 실제로 확보됐다.

  (1) 선석 제원  — 흘수 제한 / 최대 DWT / 동시접안 척수
      → mart.berth_draught_check 의 부두 명세를 SK 8개 너머로 확장할 근거.
        (기존에는 SK 부두 수심 8개만 갖고 있었다)

  (2) 하역 펌프 레이트 — 시간당 처리량(kl/h)
      → "하역 소요시간을 산출할 수 없어 스케줄링이 성립하지 않는다"던
        미해결 항목의 첫 실측 근거. 소요시간 ≈ 물량 ÷ 레이트.

  (3) 터미널별 실제 취급화물 — 물질명 수준
      → 기존 계선시설 코드표는 "액체화학 / 유류" 같은 대분류뿐이었다.
        여기서는 Methanol·Xylene·Benzene·Styrene Monomer 처럼 UN 번호로
        직접 연결되는 물질명이 나온다. 합성 화물의 부두 배정을 추정이 아니라
        **실제 터미널 취급품목**에 근거하게 만들 수 있다.

  (4) 벙커링·STS(선박간 이고작업)·블렌딩 설비 보유 여부
      → AIS 로 안 잡히는 ③계층(소형 급유선·부선)이 **어디서** 움직이는지
        좁혀준다. 벙커링 설비가 있는 터미널 주변이 그 활동 구역이다.

주의 — 이 표의 한계
--------------------------------------------------------------------------
  · 브로슈어는 홍보 자료다. 수치는 대표값이며 선석별 상세 제원(선석 길이,
    선석별 개별 흘수 제한)은 여전히 없다. 확정하려면 각 터미널 운영규정 또는
    UPA 선석시설 API(collect_berth_facility)가 필요하다.
  · 펌프 레이트는 현대오일터미널 한 곳만 명시적 수치를 준다. 나머지는
    "MR size 36시간 내 처리" 같은 간접 표현이라 그대로 옮겨 적고 파생값은
    만들지 않는다.
  · 발간 시점의 값이므로 증설·개편이 반영되지 않을 수 있다.
"""
from __future__ import annotations

from typing import NamedTuple


class Terminal(NamedTuple):
    key: str                 # 내부 식별자
    name_ko: str
    name_en: str
    cargo_raw: str           # 브로슈어 '취급화물' 원문
    tank_count: int | None   # 탱크 기수
    capacity_kl: int | None  # 총 저장능력 (kl 환산; cbm 은 1:1 로 본다)
    berth_count: int | None  # 전용 부두/선석 수
    max_vessels_alongside: int | None   # 동시접안 가능 척수
    max_dwt: int | None      # 최대 접안 선박 DWT
    draught_limit_m: float | None       # 흘수 제한(m)
    load_rate_klh: int | None           # 선적(적하) 레이트 kl/h
    discharge_rate_klh: int | None      # 하역(양하) 레이트 kl/h
    has_bunkering: bool      # 벙커링 설비 보유
    has_sts: bool            # 선박간 이고작업(STS) 가능
    has_blending: bool       # 블렌딩 설비/허가
    has_vcu: bool            # VCU(증기연소장치) 등 VOC 저감설비
    note: str = ""


# ---------------------------------------------------------------------------
# UPA 브로슈어 수록 10개 터미널 (목차 순서)
# ---------------------------------------------------------------------------
TERMINALS: dict[str, Terminal] = {t.key: t for t in [
    Terminal(
        "HYUNDAI_OIL", "현대오일터미널", "HYUNDAI Oil Terminal",
        "FUEL OIL / SLURRY OIL / ULSD / KEROSENE / GASOIL / BASEOIL",
        35, 280_000, None, None, None, None,
        load_rate_klh=1_000, discharge_rate_klh=2_400,
        has_bunkering=False, has_sts=False, has_blending=False, has_vcu=False,
        note="중질유 최대 70℃ 가열 heating 설비. IFR(Inner Floating Roof)로 고증기압 화물 저장. "
             "★브로슈어 전체에서 유일하게 시간당 처리량을 명시한 터미널."),
    Terminal(
        "JEONGIL_STOLTHAVEN", "정일스톨트헤븐울산", "JEONGIL STOLTHAVEN ULSAN",
        "M.X, Methanol, Ethanol, Gas oil, Gasoline, A.N, HMD 등 액체 케미칼 및 석유류",
        235, 1_577_500, 7, None, 100_000, None,
        load_rate_klh=None, discharge_rate_klh=None,
        has_bunkering=False, has_sts=False, has_blending=True, has_vcu=True,
        note="동북아 상업용 액체터미널 중 최대 규모. 자가 부두 7곳. 탱크 550kl~32,000kl. "
             "Tank Lorry 출하 holds 53. 자동 계측, 드럼 작업, 질소·스팀 공급. "
             "ISO14001/45001/9001, CDI-T, PSM P등급."),
    Terminal(
        "UTK", "유나이티드터미널코리아", "United Terminal Korea Limited",
        "휘발유, 등유, 경유 등 석유제품 / 바이오디젤, 바이오중유 등 대체연료 / 에탄올, 벤젠, 톨루엔 등",
        64, 468_450, 2, 3, None, None,
        load_rate_klh=None, discharge_rate_klh=None,
        has_bunkering=True, has_sts=True, has_blending=True, has_vcu=True,
        note="탱크 1,100~20,000kl. 종합보세구역(보세화물 취급·Blending). "
             "MR size 선박 화물 36시간 내 처리 가능(→ 레이트 역산 가능). "
             "★선박간 이고작업(STS) 가능 + 유량계 Bunkering 지원 — ③계층 활동 구역."),
    Terminal(
        "TAEYOUNG", "태영인더스트리", "TAEYOUNG INDUSTRY",
        "Propylene Oxide, Styrene Monomer, Xylene, Benzene, Ethanol, MPG, MEA, Base oil, MEK 등",
        110, 260_000, 3, 4, None, None,
        load_rate_klh=None, discharge_rate_klh=None,
        has_bunkering=False, has_sts=False, has_blending=False, has_vcu=True,
        note="탱크 300~10,000kl. 울산석유화학단지 고객사와 지하배관 18열 연결. "
             "전용부두 3선석 + 최대 4척 동시접안. 화재예방 워터커튼, 부식방지 전기방식. "
             "KOSHA18001, ISO9001/14001, CDI-T."),
    Terminal(
        "VOPAK", "한국보팍터미날", "VOPAK TERMINALS KOREA",
        "Methanol, Xylene, MEG(Mono Ethylene Glycol), Styrene 등 다양한 액체화물과 가스제품",
        145, 278_600, 3, 3, None, 11.0,
        load_rate_klh=None, discharge_rate_klh=None,
        has_bunkering=False, has_sts=False, has_blending=False, has_vcu=True,
        note="★브로슈어에서 유일하게 흘수 제한(Draught 11.0 m)을 명시. Berths for vessels: 3. "
             "탱크 500~7,000cbm. Access: Barge / Pipeline / Truck / Vessel — **Barge 접근 명시**. "
             "울산국가산업단지 고객사 연결 송유관 13개. IBC/Drum filling. ISO14001/9001:2015, CDI-T."),
    Terminal(
        "KPX_GLOBAL", "KPX글로벌", "KPX Global",
        "알콜류, 용제류, MONOMER류, 무기산, 연료유, Diisocyanate 등 유독물·위험물을 포함한 모든 액체화물",
        26, 80_000, None, None, None, None,
        load_rate_klh=None, discharge_rate_klh=None,
        has_bunkering=False, has_sts=False, has_blending=False, has_vcu=True,
        note="stainless 재질 탱크 6기 포함. Truck bay 8개, Mass Flow meter, Batch control system. "
             "전 탱크 질소 BLANKETING. FOAM/HYDRANT. ACTIVE CARBON·BIO FILTER·COMBUSTION UNIT. "
             "탱크 OVERFLOW 방지 LEVEL 감지장치 + 긴급차단밸브. "
             "★'유독물·위험물 포함 모든 액체화물' — 취급 위험물 범위가 가장 넓다."),
    Terminal(
        "ODFJELL", "오드펠터미널코리아", "ODFJELL TERMINALS KOREA",
        "석유 정제 및 액체화학제품류",
        85, 313_710, None, None, 50_000, None,
        load_rate_klh=None, discharge_rate_klh=None,
        has_bunkering=False, has_sts=False, has_blending=True, has_vcu=True,
        note="Inner Floating Roof, Drum Stuffing Facility. VCU·IFR·카본 흡착 시스템·Scrubber. "
             "자가부두 운영. ★국내 최초 Blending/mixing 허가 획득. "
             "ISO9001/14001/45001, CDI-T ATTESTATION."),
    Terminal(
        "ULSAN_ENERGY", "울산에너지터미널", "ULSAN ENERGY TERMINAL",
        "High Sulfur Fuel Oil, Low Sulfur Fuel Oil, Marine Diesel Oil, Marine Gas Oil 등",
        11, 275_200, None, None, None, None,
        load_rate_klh=None, discharge_rate_klh=None,
        has_bunkering=True, has_sts=False, has_blending=False, has_vcu=False,
        note="★취급화물이 전부 선박용 연료유(HSFO/LSFO/MDO/MGO) — 사실상 벙커링 전용. "
             "보온탱크(Heating) 36,700KL×3, 23,700×1, 21,000×2, 2,500×2 / 비보온 36,700×2, 21,000×1. "
             "부두연결 지하배관 14인치×3Line(Heating Cable), 12인치×3Line. "
             "레이더 게이지, Air(N2) Sparging×8탱크, 트럭출하장 6대 동시. PSM, 안전표준절차 23개."),
    Terminal(
        "HYOSUNG", "효성화학", "HYOSUNG CHEMICAL",
        "위험물 제4류, 에틸렌 가스",
        13, 25_000, None, None, None, None,
        load_rate_klh=None, discharge_rate_klh=None,
        has_bunkering=False, has_sts=False, has_blending=False, has_vcu=False,
        note="위험물 저장탱크 12기(25,000kl) + 에틸렌 저장탱크 1기(7,500톤). "
             "지하이송배관 7개 라인(온산~석유화학공단). "
             "★'위험물 제4류'는 「위험물안전관리법」 분류(인화성 액체)로 IMDG Class 3 과 대응. "
             "ISO14001/9001/45001, KOSHA18001, IATF16949."),
    Terminal(
        "ONSAN_TANK", "온산탱크터미널", "ONSAN TANK TERMINAL",
        "GASOLINE, GASOIL, KEROSENE, BIO FUEL OIL, VLSFO, LSMGO, 기타 석유제품 및 액체 케미칼 등",
        11, 99_890, 2, None, None, None,
        load_rate_klh=None, discharge_rate_klh=None,
        has_bunkering=True, has_sts=False, has_blending=True, has_vcu=True,
        note="탱크 13,000KL×5, 6,500KL×5, 2,390KL×1. S-OIL·JSTT 와 배관 연결. "
             "종합보세구역(보세화물 취급·Blending). "
             "★Bunkering 작업에 최적화된 설비 + 유량계 설치 — ③계층 활동 구역. "
             "'2개의 부두는 날씨의 영향이 적어 선박작업에 용이'. PSM.")
]}


# ---------------------------------------------------------------------------
# 파생 조회
# ---------------------------------------------------------------------------
def bunkering_terminals() -> list[Terminal]:
    """벙커링 설비 보유 터미널 — 소형 급유선(③계층)의 주 활동 구역."""
    return [t for t in TERMINALS.values() if t.has_bunkering]


def sts_terminals() -> list[Terminal]:
    """선박간 이고작업(STS) 가능 터미널 — 부선·소형선 접현 발생 지점."""
    return [t for t in TERMINALS.values() if t.has_sts]


def blending_terminals() -> list[Terminal]:
    """
    블렌딩 설비·허가 보유 터미널.

    ★ 안전판정상 중요: 블렌딩은 화물 특성 자체를 바꾼다(인화점·옥탄가·증기압).
      ISGOTT 6th ed. ch.12.1.6 및 SOLAS 제VI장 제5-2규칙이 규율하며,
      블렌딩 전후로 MSDS 가 달라질 수 있으므로 UN 번호를 고정값으로 보면 안 된다.
    """
    return [t for t in TERMINALS.values() if t.has_blending]


def draught_limits() -> dict:
    """터미널별 흘수 제한(m). 확보된 것만 반환 — 없는 곳은 키가 없다."""
    return {t.key: t.draught_limit_m
            for t in TERMINALS.values() if t.draught_limit_m is not None}


def pump_rates() -> dict:
    """
    터미널별 하역/선적 레이트(kl/h). 확보된 것만.

    소요시간(시간) ≈ 물량(kl) ÷ 레이트(kl/h)
    ※ 접·이안, 호스 연결, 검량, 서류 시간은 제외된 순수 이송 시간이다.
      실제 선석 점유시간은 이보다 길다 — 스케줄링에 쓸 때 여유를 둘 것.
    """
    return {t.key: {"load_klh": t.load_rate_klh, "discharge_klh": t.discharge_rate_klh}
            for t in TERMINALS.values()
            if t.load_rate_klh or t.discharge_rate_klh}


def estimate_transfer_hours(volume_kl: float, terminal_key: str,
                            direction: str = "discharge") -> float | None:
    """
    물량 → 순수 이송 소요시간(h). 레이트가 확보되지 않은 터미널은 None.

    None 을 0 이나 평균값으로 대체하지 않는다 — 모르는 것을 아는 척하면
    스케줄이 실제보다 낙관적으로 나온다.
    """
    t = TERMINALS.get(terminal_key)
    if t is None:
        return None
    rate = t.discharge_rate_klh if direction == "discharge" else t.load_rate_klh
    if not rate or not volume_kl:
        return None
    return round(float(volume_kl) / rate, 2)


# ---------------------------------------------------------------------------
# 터미널 취급화물 → UN 번호 (imdg_dgl 에 등재된 것만 연결)
#   브로슈어의 물질명을 우리 DGL 참조표와 이어 붙인다. 이러면 합성 화물의
#   부두 배정이 추정이 아니라 **실제 터미널 취급품목**에 근거하게 된다.
#   ※ 브로슈어에 있으나 DGL 미등재인 물질(MEG·MPG·MEA·MEK·A.N·HMD·
#     Diisocyanate·에틸렌 등)은 의도적으로 비워 둔다. 지어내지 않는다.
# ---------------------------------------------------------------------------
TERMINAL_CARGO_UN: dict[str, tuple] = {
    "HYUNDAI_OIL":        ("1202", "1223", "1268"),          # GASOIL, KEROSENE, 석유제품
    "JEONGIL_STOLTHAVEN": ("1230", "1307", "1202", "1203"),  # Methanol, M.X(자일렌), Gas oil, Gasoline
    "UTK":                ("1203", "1223", "1202", "1114", "1294"),  # 휘발유·등유·경유·벤젠·톨루엔
    "TAEYOUNG":           ("1280", "2055", "1307", "1114"),  # Propylene Oxide, Styrene, Xylene, Benzene
    "VOPAK":              ("1230", "1307", "2055"),          # Methanol, Xylene, Styrene
    "KPX_GLOBAL":         ("1230", "1993"),                  # 알콜류 + 유독물 포함 광범위
    "ODFJELL":            ("1268", "1114", "1294"),          # 석유정제·액체화학
    "ULSAN_ENERGY":       ("1202",),                         # MDO/MGO 계열
    "HYOSUNG":            ("1993",),                         # 위험물 제4류(인화성 액체) 총칭
    "ONSAN_TANK":         ("1203", "1202", "1223"),          # GASOLINE, GASOIL, KEROSENE
}


def cargo_un_for_terminal(terminal_key: str) -> tuple:
    return TERMINAL_CARGO_UN.get(terminal_key, ())


def summary() -> str:
    """콘솔 요약 — 확보/미확보를 한눈에."""
    lines = [f"울산항 액체화물 터미널 {len(TERMINALS)}개 (UPA 공식 브로슈어)"]
    lines.append(f"  총 저장능력 : {sum(t.capacity_kl or 0 for t in TERMINALS.values()):,} kl")
    lines.append(f"  총 탱크     : {sum(t.tank_count or 0 for t in TERMINALS.values()):,} 기")
    lines.append(f"  흘수 제한 확보 : {len(draught_limits())} / {len(TERMINALS)} 곳")
    lines.append(f"  펌프 레이트 확보: {len(pump_rates())} / {len(TERMINALS)} 곳")
    lines.append(f"  벙커링 설비   : {', '.join(t.name_ko for t in bunkering_terminals())}")
    lines.append(f"  STS 가능      : {', '.join(t.name_ko for t in sts_terminals()) or '(없음)'}")
    lines.append(f"  블렌딩        : {', '.join(t.name_ko for t in blending_terminals())}")
    return "\n".join(lines)


if __name__ == "__main__":
    print(summary())
