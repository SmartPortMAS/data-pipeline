# -*- coding: utf-8 -*-
"""berth_web_raw.csv (+ UPA API staging) -> wharf/berth 시드 CSV.

입력
  data/seed/berth_web_raw.csv              선석 제원 원본(웹). 손대지 않는 증거 파일
  data/staging/upa_berth_facility_stg.csv  API staging. 여기서 **좌표와 시설코드**를 얻는다
  data/seed/berth_code_map.csv             (선택) 사람이 검수한 매핑·보정. 자동 결과를 이긴다

출력
  data/seed/wharf_seed.csv                 부두 — 위치/운영 주체
  data/seed/berth_seed.csv                 선석 — 제원(감사 기준값)
  data/seed/_build_report.csv              미매칭·충돌 진단. 커밋 대상 아님

왜 둘로 나누는가: 좌표·운영사·소재지는 부두의 속성이고 안벽길이·수심·접안능력은
선석의 속성이다. 한 표에 합치면 부두 속성을 선석마다 복제하거나(불일치 발생)
선석 제원을 부두로 집계해야 하는데, 후자는 MAX(접안능력)가 "없는 능력"을 만들어
감사에서 거짓 안전(false OK)을 낳는다.

왜 upa_config.HRBR_FCLT_INFO 를 고치지 않는가: fcltCd/fcltSubCd 는 column_map 에
없어도 staging CSV 에 원래 이름 그대로 실려 온다. 기존 파이프라인을 건드리지 않고
여기서 읽는 편이 부작용이 없다.

원본 문자열(*_raw)을 끝까지 들고 간다 — 파싱 규칙은 반드시 틀린 케이스가 나오고,
그때 재수집 없이 고칠 수 있어야 한다.

실행:
  python -m data_pipeline.upa.build_berth_seed
"""
from __future__ import annotations

import os
import re
import sys

import pandas as pd

SEED_DIR = "data/seed"
WEB_RAW = f"{SEED_DIR}/berth_web_raw.csv"
API_STG = "data/staging/upa_berth_facility_stg.csv"
CODE_MAP = f"{SEED_DIR}/berth_code_map.csv"
OUT_WHARF = f"{SEED_DIR}/wharf_seed.csv"
OUT_BERTH = f"{SEED_DIR}/berth_seed.csv"
OUT_REPORT = f"{SEED_DIR}/_build_report.csv"

# 웹 상세 페이지의 라벨(원문). 위치가 아니라 이 이름으로 읽는다.
L_NAME, L_ADDR = "부두/선석명", "소재지"
L_YEAR, L_BY = "준공년도", "준공주체"
L_QUAY, L_DEPTH = "안벽길이", "수심"
L_CAP, L_UNLOAD = "접안능력", "하역능력"
L_OPER, L_CARGO = "운영사", "주요취급화물"


# ---------------------------------------------------------------------------
# 파싱 — 전부 "못 읽으면 None". 추측해서 채우지 않는다(감사 기준값이므로)
# ---------------------------------------------------------------------------
def _present(v) -> bool:
    """값이 실제로 있는가. pandas 의 NaN 은 truthy 라 `if v:` 로는 걸러지지 않는다."""
    if v is None:
        return False
    try:
        if pd.isna(v):
            return False
    except (TypeError, ValueError):
        pass
    return str(v).strip() != ""


def _num(s) -> float | None:
    """문자열에서 첫 번째 수치를 뽑는다(천단위 콤마 허용)."""
    if not _present(s):
        return None
    m = re.search(r"(\d[\d,]*(?:\.\d+)?)", str(s))
    return float(m.group(1).replace(",", "")) if m else None


def parse_quay(raw) -> tuple[float | None, str | None]:
    """'830m(잔교식)' -> (830.0, '잔교식'). 구조형식은 괄호 안 전부를 보존한다.

    ★ 수치가 여러 개면 **선석 단위 길이를 알 수 없다**. 6부두 원문이
      '990m 390m(잔교식) 600m(중력식)' 인데, 990 은 부두 전체 연장이고
      390·600 은 구조별 구간이다(5개 선석에 어떻게 나뉘는지는 안 적혀 있다).

      첫 수치(990)를 선석 길이로 쓰면 `min_length_m` 이 990m 이 되어 어떤
      대형선이든 길이 게이트를 통과한다 — 감사에서 가장 피해야 할 **거짓 안전**이다.
      그래서 미상(None)으로 둔다. 소비처에서는 `length_verdict='UNKNOWN'` 이 되며,
      이는 "모르는 것을 안전으로 간주하지 않는다"는 이 프로젝트의 원칙과 같다.
    """
    if not _present(raw):
        return None, None
    m = re.search(r"\(([^)]*)\)", str(raw))
    structure = m.group(1).strip() if m else None
    nums = re.findall(r"\d[\d,]*(?:\.\d+)?", str(raw))
    if len(nums) != 1:
        return None, structure
    return float(nums[0].replace(",", "")), structure


def parse_depth(raw) -> tuple[float | None, bool]:
    """'11.5m' -> (11.5, False). '9~11m' 같은 범위는 **최솟값**을 쓴다.

    수심은 감사에서 흘수와 비교하는 값이라, 범위를 만나면 보수적인 쪽(얕은 쪽)을
    택해야 한다. 최댓값을 쓰면 실제로는 못 들어가는 배를 통과시킨다.
    기존 mart.berth_draught_check 의 '안전측 최소값 원칙'과 같은 규칙이다.
    """
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None, False
    nums = [float(x.replace(",", "")) for x in re.findall(r"\d[\d,]*(?:\.\d+)?", str(raw))]
    if not nums:
        return None, False
    return min(nums), len(nums) > 1


def parse_capacity(raw) -> tuple[float | None, str | None, int | None]:
    """'40,000DWT / 1척' -> (40000.0, 'DWT', 1).

    단위를 고정하지 않고 읽는 이유: 표기가 DWT가 아닐 수 있고, 단위를 잘못 가정한
    수치가 감사에 들어가면 조용히 틀린 판정이 나온다. 실제로 원문이 '15.5m'
    (수심값이 접안능력 칸에 들어간 UPA 쪽 입력오류)인 행이 있어, 단위가 DWT가
    아니면 SUSPECT 로 걸러낸다.

    ★ '/N척'은 **그 선석의 동시접안 수가 아니다.** 실측하면 6부두는 1~5선석이
      모두 '30,000DWT / 4척'으로 같은 값을 반복한다 — 부두 전체의 척수를 선석마다
      복사해 둔 것이다. 그래서 이 값은 berth 가 아니라 wharf 로 올린다.
    """
    entries = parse_capacity_entries(raw)
    if not entries:
        # 단위가 DWT가 아닌 경우(수심값 오입력 등)를 드러내기 위해 단위만 회수한다.
        if not _present(raw):
            return None, None, None
        m = re.search(r"(\d[\d,]*(?:\.\d+)?)\s*([A-Za-z]+|톤|t)", str(raw))
        return None, (m.group(2).strip() if m else None), None
    # 항목이 여러 개면 그 선석 하나의 값이 아니다(부두 구성 목록) — §함정1 참조.
    if len(entries) > 1:
        return None, "DWT", sum(c or 0 for _, c in entries) or None
    value, count = entries[0]
    return value, "DWT", count


CAP_ENTRY_RE = re.compile(r"(\d[\d,]*(?:\.\d+)?)\s*DWT\s*/?\s*(\d+)?\s*척?", re.IGNORECASE)


def parse_capacity_entries(raw) -> list[tuple[float, int | None]]:
    """'2,000DWT / 2척4,000DWT / 1척' -> [(2000.0, 2), (4000.0, 1)].

    ★ 접안능력 칸이 그 선석 하나의 값이 아닌 경우가 있다. SK5부두는 5개 선석
      페이지가 모두 '2,000DWT/2척 4,000DWT/1척 5,000DWT/1척 15,000DWT/1척'을
      똑같이 싣는다 — 척수 합(2+1+1+1=5)이 선석 수와 같다. 즉 **부두의 선석
      구성표**를 선석마다 복사해 둔 것이다.

      그래서 항목을 전부 읽어 둔다. 항목이 하나뿐일 때만 그 선석의 값으로
      확정하고, 여러 개면 선석 단위로는 미상 처리한 뒤 부두 집계에만 쓴다.
      첫 항목만 읽으면 SK5부두가 2,000DWT(실제 최소 2,000·최대 15,000)로
      잡혀 감사 기준이 통째로 틀어진다.
    """
    if not _present(raw):
        return []
    out: list[tuple[float, int | None]] = []
    for m in CAP_ENTRY_RE.finditer(str(raw)):
        out.append((float(m.group(1).replace(",", "")),
                    int(m.group(2)) if m.group(2) else None))
    return out


def parse_unload(raw) -> tuple[float | None, str | None]:
    """'9,425천 톤' -> (9425.0, '천 톤'), '16,000Bbls' -> (16000.0, 'Bbls').

    '천 톤'을 1000배로 펴지 않는다 — 기간(연간/일간)이 불명이라 단위를 바꾸면
    정밀도를 지어내는 셈이 된다. 차원이 다른 값들이므로 수치 비교에 쓰지 말 것.
    """
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None, None
    s = str(raw).strip()
    m = re.search(r"(\d[\d,]*(?:\.\d+)?)\s*(.*)$", s)
    if not m:
        return None, None
    unit = re.sub(r"\s+", " ", m.group(2)).strip() or None
    return float(m.group(1).replace(",", "")), unit


PREFIX_RE = re.compile(r"^\((국|민|공)\)\s*")
BERTH_RE = re.compile(r"^(?P<wharf>.+?)\s*(?P<no>\d+)\s*선석$")


def split_name(raw) -> tuple[str, int | None, str | None]:
    """'(국)SK1부두1선석' -> ('SK1부두', 1, '국').

    선석 접미사가 없으면 선석이 하나인 부두로 본다(berth_no=None) — 예외 규칙이
    아니라 자식이 하나인 정상 케이스다.
    """
    s = str(raw).strip()
    pm = PREFIX_RE.match(s)
    prefix = pm.group(1) if pm else None
    s = PREFIX_RE.sub("", s)
    bm = BERTH_RE.match(s)
    if bm:
        return bm.group("wharf").strip(), int(bm.group("no")), prefix
    return s, None, prefix


def norm(s) -> str:
    """이름 대조용 정규화. **숫자는 절대 지우지 않는다** — 선석/부두 번호가 의미다.

    (기존 mart.norm_berth 는 끝자리 숫자를 지웠다. 그건 부두 단위로 뭉개기
    위한 규칙이라 선석 단위 모델과 공존할 수 없다.)

    따옴표도 지운다 — 웹은 '신항"컨"부두', 공공 API 는 '신항컨부두'로 적는다.
    이걸 빼먹으면 그 부두만 좌표·시설코드를 못 받는다(실측으로 확인).
    """
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    return re.sub(r"[\s\-_()·\"'“”]", "", str(s)).lower()


OVERRIDES = f"{SEED_DIR}/berth_spec_overrides.csv"
EXTRA = f"{SEED_DIR}/berth_extra_seed.csv"

# 같은 시설을 웹과 공공 API 가 다르게 적는 경우. 여기서 붙여 주지 않으면
# **좌표를 못 받는다**(웹에는 좌표가 없다) — 접안판정이 좌표에 의존하므로
# 단순한 표기 문제로 끝나지 않는다.
#   키=웹 부두명(정규화), 값=API wharf_name(정규화)
WEB_TO_API = {
    "ls니꼬신항부두": "lsmnm신항부두",       # LS니꼬 → LS MnM (사명 변경)
    "한국석유공사부이": "석유공사부이",        # 약칭 차이
    # 웹은 '현대오일터미널 신항부두' 하나인데 API 는 신항1·2부두 두 행이다(둘 다
    # MBN/12). **안벽길이로 어느 쪽인지 확정했다** — 웹 270m = API 신항2부두 270m
    # (신항1부두는 210m). PORT-MIS 도 MBN/12 를 '현대오일터미널신항2부두'로 적고
    # 배정이 32건으로 안벽 시설 중 가장 많다. 1부두로 붙이면 210m 기준으로
    # 길이 게이트가 잘못 판정된다.
    "현대오일터미널신항부두": "현대오일터미널신항2부두",
    # 웹 '용잠3부두'(155m) 와 API '용잠2부두'(155m) 는 같은 선석이다 — 안벽길이로
    # 확인했다(웹 용잠1부두 88m = API 용잠1부두 88m). 번호만 다르게 매긴 것이다.
    "용잠3부두": "용잠2부두",
    # 웹 'S-Oil 신부이' = API·해수청 'S-Oil부이'. **접안능력으로 확정했다** —
    # 양쪽 다 350,000DWT 이고, 인접한 'S-Oil&오일허브 부이'는 325,000DWT 라
    # 헷갈릴 여지가 없다. PORT-MIS 도 MUD/01·02 를 'S-OIL부이 01/02' 로 적는다.
    "soil신부이": "soil부이",
}


def _classify_quay_length(berth: pd.DataFrame, api: pd.DataFrame,
                          report: list[dict]) -> tuple[pd.DataFrame, dict]:
    """웹의 안벽길이가 **선석별 값인지 부두 총연장인지** 값 단위로 가른다.

    웹은 둘을 같은 칸에 같은 형식으로 싣는다:

        일반부두  선석 7개 · 일곱 페이지 모두 '679m' → 공공 API 도 679  (총연장)
        SK1부두  선석 2개 · 두 페이지 모두 '130m'   → 공공 API 는 260  (= 130×2, 선석별)
        UTK부두  1선석 '240m' · 2선석 '246m'        →                  (선석별)

    ★ 핵심 판별 근거는 **같은 값이 몇 개 선석에 반복되는가** 다.
      부두 총연장을 복사해 실었다면 그 부두의 선석들이 **같은 값**을 가질 수밖에
      없다. 값이 선석마다 다르면 그건 웹이 선석별로 적었다는 뜻이다.

      초판은 "API 와 정확히 맞아떨어지지 않으면 버린다"로 했는데 과했다.
      UTK부두는 웹이 240m·246m 로 명확히 선석별 값을 주는데도, 합(486)이 API(287)와
      다르다는 이유로 둘 다 버렸다. **웹에만 있는 정보를 API 로 검산할 수 없다고
      해서 폐기하면, 원천이 하나뿐인 값은 영영 못 쓴다.**

      그래서 API 는 '전부 같은 값'이라 판단이 안 서는 경우에만 쓴다:
        API == 그 값        -> 총연장을 복사한 것    -> 선석 길이 미상
        API == 그 값 × 개수 -> 선석별 값             -> 그대로 사용
        그 외/ API 없음      -> 판단 근거 없음        -> 미상(보수적)
    """
    berth["length_basis"] = None
    wharf_total: dict[str, float] = {}
    alut: dict[str, float] = {}
    if not api.empty:
        tmp = api.copy()
        tmp["_k"] = tmp["wharf_name"].map(norm)
        tmp["_len"] = pd.to_numeric(tmp.get("length_m"), errors="coerce")
        for k, g in tmp.groupby("_k"):
            v = g["_len"].dropna()
            if len(v):
                alut[k] = float(v.iloc[0])

    for wn, grp in berth.groupby("wharf_name"):
        vals = grp["length_m"].dropna()
        if not len(vals):
            continue
        a = alut.get(norm(wn)) or alut.get(WEB_TO_API.get(norm(wn), ""))
        wharf_total[wn] = a if a is not None else float(vals.max())

        counts = vals.round(2).value_counts()
        for idx in grp.index:
            v = berth.at[idx, "length_m"]
            if pd.isna(v):
                continue
            n_same = int(counts.get(round(float(v), 2), 0))
            if n_same <= 1:
                # 그 선석만 가진 값 — 복사된 총연장일 수 없다.
                berth.at[idx, "length_basis"] = "BERTH"
                continue
            if a is not None and abs(a - float(v)) < 1.5:
                berth.at[idx, "length_basis"] = "WHARF_TOTAL"
                berth.at[idx, "length_m"] = None
            elif a is not None and abs(a - float(v) * n_same) < 2.5:
                berth.at[idx, "length_basis"] = "BERTH"
            else:
                berth.at[idx, "length_basis"] = "AMBIGUOUS"
                berth.at[idx, "length_m"] = None

        kinds = set(berth.loc[grp.index, "length_basis"].dropna())
        if kinds - {"BERTH"}:
            report.append({"kind": "QUAY_NOT_BERTH_LEVEL", "port_code": "", "name": str(wn),
                           "detail": f"선석 {len(grp)}개 · 웹 값 {sorted(set(vals))} · "
                                     f"API {a} -> {sorted(kinds)}"})
    return berth, wharf_total


def _floor_depth_with_api(wharf: pd.DataFrame, api: pd.DataFrame,
                          report: list[dict]) -> pd.DataFrame:
    """부두 최저수심을 공공 API 값으로 **더 얕은 쪽으로만** 끌어내린다.

    왜 필요한가: 웹이 얕은 선석의 수심을 빠뜨린 경우가 있다. 실측 대조 —

        부두        API   웹     해수청 일반현황
        2부두        9    12     9~12     ← 웹이 9m 선석을 누락
        용연부두      12    14    12~14
        신항컨부두    12    14    12~14
        SK5부두      7    11     7~11

    네 건 모두 **API 가 옳고 웹이 깊은 쪽만 적어 놨다.** 그리고 교집합 58개 중
    52개에서 API 수심 = 웹 선석들의 최솟값이었다 — 즉 API 의 겹친 수심값은
    그 부두의 **최저수심**이다. 그렇다면 API 가 더 얕게 말할 때는 웹이 놓친
    선석이 있다고 보는 것이 맞다.

    **얕은 쪽으로만** 옮기고 깊은 쪽으로는 절대 올리지 않는다. 감사에서 수심을
    실제보다 깊게 잡으면 착저 위험이 있는 배를 통과시킨다(거짓 안전).
    """
    if api.empty:
        return wharf
    tmp = api.copy()
    tmp["_k"] = tmp["wharf_name"].map(norm)
    tmp["_d"] = pd.to_numeric(tmp.get("depth_m"), errors="coerce")
    dlut: dict[str, float] = {}
    for k, g in tmp.groupby("_k"):
        v = g["_d"].dropna()
        if len(v):
            dlut[k] = float(v.min())

    wharf["depth_floored"] = False
    for i, w in wharf.iterrows():
        a = dlut.get(norm(w["wharf_name"]))
        if a is None:
            a = dlut.get(WEB_TO_API.get(norm(w["wharf_name"]), ""))
        cur = w["min_water_depth_m"]
        if a is None or pd.isna(cur) or a >= cur:
            continue
        wharf.at[i, "min_water_depth_m"] = a
        # ★ 이 플래그가 켜지면 **그 부두의 선석별 수심을 믿을 수 없다**는 뜻이다.
        #   웹이 얕은 선석을 빠뜨렸는데 어느 선석인지는 알 수 없으므로, 감사는
        #   그 부두의 어느 선석이든 부두 최저수심으로 판정해야 한다.
        #   실측: 2부두는 웹이 3선석 모두 12m 라 하지만 해수청 원본은 9~12m 다.
        wharf.at[i, "depth_floored"] = True
        report.append({"kind": "DEPTH_FLOORED_BY_API", "port_code": "", "name": w["wharf_name"],
                       "detail": f"웹 최저 {cur:g}m -> API {a:g}m (웹이 얕은 선석을 누락)"})
    return wharf


def _load_extra(report: list[dict]) -> list[dict]:
    """UPA 웹 부두현황에 없는 부두를 다른 출처로 보충한다.

    웹 목록(117건)이 울산항 전부는 아니다. PORT-MIS 가 배정하는 부두 중 일부가
    빠져 있고, 그중에는 공공 API(`upa_berth_facility`)에는 있는 것도 있다.
    보충하지 않으면 그 부두의 배정은 영영 UNKNOWN 으로 남아 감사가 성립하지 않는다.

    **계획·공사 중 시설의 예정 제원은 넣지 않는다.** 준공 전 수치는 운영 제원이
    아니고, 감사 기준값에 섞이면 근거 없는 판정이 된다(예: 북신항 에너지부두는
    보도상 접안능력이 계획 단계에서 바뀌었다).
    """
    if not os.path.exists(EXTRA):
        return []
    df = pd.read_csv(EXTRA, encoding="utf-8-sig")
    out: list[dict] = []
    for _, r in df.iterrows():
        d = {k: (None if pd.isna(v) else v) for k, v in r.items()}
        d.setdefault("spec_source", "UPA_API")
        d["_port_gubun"] = d.get("port_name")
        cap = d.get("capacity_value")
        d["_cap_values"] = [float(cap)] if _present(cap) else []
        d["_wharf_vessel_count"] = d.get("concurrent_vessels")
        out.append(d)
        report.append({"kind": "EXTRA_SEED", "port_code": "", "name": str(d.get("berth_id")),
                       "detail": f"웹 미수록 부두를 보충 ({d.get('spec_source')})"})
    print(f"[EXTRA] 웹 미수록 보충 {len(out)}건")
    return out


def _apply_overrides(wharf: pd.DataFrame, berth: pd.DataFrame,
                     report: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """출처 대조로 확정한 보정값을 적용한다.

    UPA 웹이 늘 옳지는 않다. 울산지방해양수산청 「울산항시설현황」과 대조하면
    접안능력에서 어긋나는 건이 나오고, 일부는 UPA 쪽 오기로 판단된다
    (예: SK4부두 '1,000DWT' — 수심 10m·안벽 228m 에 1,000DWT 는 물리적으로
    맞지 않는다. 원본은 10,000DWT).

    자동 규칙으로 처리하지 않는 이유: 어느 출처가 옳은지는 건별로 다르다.
    3부두·달포부두는 UPA 웹이 맞고(해수청 원본도 같다), SK4부두는 해수청이
    맞다. "항상 큰 값" 같은 규칙을 만들면 근거 없이 한쪽으로 쏠린다.

    파일이 없으면 아무 것도 하지 않는다 — 보정은 선택 사항이다.
    """
    if not os.path.exists(OVERRIDES):
        print(f"[OVR] {OVERRIDES} 없음 — 보정 없이 진행")
        return wharf, berth
    ov = pd.read_csv(OVERRIDES, encoding="utf-8-sig", dtype=str)
    applied = 0
    for _, o in ov.iterrows():
        scope, key, col, val = o["scope"], o["key"], o["column"], o["value"]
        df, keycol = (wharf, "wharf_name") if scope == "wharf" else (berth, "berth_id")
        mask = df[keycol] == key
        if not mask.any():
            report.append({"kind": "OVERRIDE_NO_TARGET", "port_code": "", "name": key,
                           "detail": f"{scope}.{col} — 대상 행 없음"})
            continue
        if col not in df.columns:
            report.append({"kind": "OVERRIDE_NO_COLUMN", "port_code": "", "name": key,
                           "detail": f"{scope}.{col} — 컬럼 없음"})
            continue
        before = df.loc[mask, col].iloc[0]
        try:
            cast = float(str(val).replace(",", ""))
        except ValueError:
            cast = val
        df.loc[mask, col] = cast
        applied += 1
        report.append({"kind": "OVERRIDE_APPLIED", "port_code": "", "name": key,
                       "detail": f"{col}: {before} -> {val} ({o.get('source', '')})"})
    print(f"[OVR] 보정 {applied}건 적용")
    return wharf, berth


# ---------------------------------------------------------------------------
def main() -> None:
    if not os.path.exists(WEB_RAW):
        sys.exit(f"{WEB_RAW} 없음 — 먼저 berth_web_scraper 를 실행할 것")
    web = pd.read_csv(WEB_RAW, encoding="utf-8-sig", dtype=str)
    print(f"[IN ] web  {len(web)}행")

    api = pd.DataFrame()
    if os.path.exists(API_STG):
        api = pd.read_csv(API_STG, encoding="utf-8-sig", dtype=str)
        api.columns = [c.strip("﻿") for c in api.columns]
        print(f"[IN ] api  {len(api)}행")
    else:
        print(f"[WARN] {API_STG} 없음 — 좌표·시설코드 없이 진행한다")

    report: list[dict] = []
    rows: list[dict] = []
    for _, r in web.iterrows():
        name_raw = r.get(L_NAME) or r.get("list_name") or ""
        wharf_name, berth_no, prefix = split_name(name_raw)
        length_m, structure = parse_quay(r.get(L_QUAY))
        depth_m, depth_ranged = parse_depth(r.get(L_DEPTH))
        cap_v, cap_u, concurrent = parse_capacity(r.get(L_CAP))
        ul_v, ul_u = parse_unload(r.get(L_UNLOAD))

        berth_id = f"{wharf_name}-{berth_no}선석" if berth_no else wharf_name
        rows.append({
            "berth_id": berth_id,
            "wharf_name": wharf_name,
            "berth_no": berth_no,
            "berth_name": PREFIX_RE.sub("", str(name_raw).strip()),
            "ownership_prefix": prefix,
            "length_m": length_m,
            "quay_structure": structure,
            "water_depth_m": depth_m,
            "water_depth_is_range": depth_ranged,
            "capacity_value": cap_v if cap_u == "DWT" else None,
            "capacity_unit": cap_u,
            # 단위가 DWT가 아니면 접안능력이 아니다 — 값을 쓰지 않고 표시만 남긴다.
            "capacity_suspect": cap_u is not None and cap_u != "DWT",
            "_wharf_vessel_count": concurrent,   # 부두 속성. 아래에서 wharf 로 옮긴다
            # 부두 집계용 — 항목이 여러 개인 행도 모든 값을 살려 둔다.
            "_cap_values": [v for v, _ in parse_capacity_entries(r.get(L_CAP))],
            "unload_value": ul_v,
            "unload_unit": ul_u,
            "handling_cargo_name": r.get(L_CARGO),
            "operator_name": r.get(L_OPER),
            "address": r.get(L_ADDR),
            "built_year": _num(r.get(L_YEAR)),
            "built_by": r.get(L_BY),
            # 원본 보존
            "quay_length_raw": r.get(L_QUAY),
            "water_depth_raw": r.get(L_DEPTH),
            "capacity_raw": r.get(L_CAP),
            "unload_raw": r.get(L_UNLOAD),
            "port_code": r.get("port_code"),
            "_port_gubun": r.get("port_gubun"),
            "spec_source": "UPA_WEB",
        })
        n_entries = len(parse_capacity_entries(r.get(L_CAP)))
        for label, val in ((L_QUAY, length_m), (L_DEPTH, depth_m), (L_CAP, cap_v)):
            # 원문이 **있는데** 못 읽은 경우만 파싱 실패다. 원문 자체가 비어 있는
            # 것은 정상이다(예: 부이는 안벽이 없어 안벽길이가 없다).
            if not (_present(r.get(label)) and val is None):
                continue
            # 접안능력이 구성 목록형이라 선석 단위로 확정 못 한 것은 실패가 아니다
            # — 값은 부두 집계(min/max_capacity_dwt)로 정상 반영된다.
            if label == L_CAP and n_entries > 1:
                kind = "CAP_MULTI_ENTRY"
            elif label == L_QUAY:
                kind = "QUAY_AMBIGUOUS"   # 수치가 여러 개 — 선석 길이 미상(안전측)
            else:
                kind = "PARSE_FAIL"
            report.append({"kind": kind, "port_code": r.get("port_code"),
                           "name": str(name_raw), "detail": f"{label}={r.get(label)!r}"})

    # --- 웹 부두현황에 없는 부두를 보충한다 -------------------------------
    # PORT-MIS 가 실제로 배정하는데 UPA 웹 117건에 실리지 않은 부두가 있다
    # (예: 북신항 액체부두 — 공공 API 에는 MBN/31 로 있다). 없는 채로 두면
    # 그 배정은 영영 UNKNOWN 이라 감사 자체가 안 된다.
    # 출처가 다르므로 spec_source 로 구분해 둔다.
    rows.extend(_load_extra(report))

    berth = pd.DataFrame(rows)
    # 이름 정본 표시. API 와 매칭되면 UPA_API 로 덮인다(아래).
    berth["name_source"] = "UPA_WEB"
    # ★ (fcltCd, fcltSubCd)는 **부두**를 가리킨다(선석이 아니다). 실측 확인:
    #   MDU/1..8 = SK1..SK8부두, MDS/1..3 = S-Oil 1..3부두, MBN/16 = LS MNM 신항부두.
    #   즉 fcltCd 는 부두군(MDU=SK계열, MBN=신항), fcltSubCd 가 그 안의 부두다.
    #   따라서 이 코드는 wharf 의 키이고, PORTMIS 배정도 부두까지만 식별된다.
    berth["_facility_cd"] = None
    berth["_facility_sub_code"] = None

    # --- 시설코드·좌표를 API에서 가져다 붙인다 (이름 대조) --------------------
    if not api.empty:
        api = api.copy()
        api["_k"] = api["wharf_name"].map(norm)
        # 같은 이름이 여러 행이면 코드를 확정할 수 없다 — 붙이지 않고 드러낸다.
        dups = set(api["_k"][api["_k"].duplicated(keep=False)])
        lut = {k: g.iloc[0] for k, g in api.groupby("_k") if k not in dups}
        for k in sorted(dups):
            report.append({"kind": "API_DUP_NAME", "port_code": "", "name": k,
                           "detail": "API staging 에 같은 이름이 여러 행 — 코드 자동연결 보류"})
        for i, br in berth.iterrows():
            # 선석명 전체 -> 부두명 -> 별칭 순으로 시도한다.
            hit = lut.get(norm(br["berth_name"]))
            if hit is None:
                hit = lut.get(norm(br["wharf_name"]))
            if hit is None:
                hit = lut.get(WEB_TO_API.get(norm(br["wharf_name"]), ""))
            if hit is None:
                report.append({"kind": "NO_API_MATCH", "port_code": br["port_code"],
                               "name": br["berth_name"], "detail": "시설코드·좌표 미확보"})
                continue
            berth.at[i, "_facility_cd"] = hit.get("fcltCd")
            berth.at[i, "_facility_sub_code"] = hit.get("fcltSubCd")
            berth.at[i, "_lat"] = hit.get("latitude")
            berth.at[i, "_lon"] = hit.get("longitude")
            berth.at[i, "_port_name"] = hit.get("port_name")
            # ★ 부두 이름의 정본은 공공 API 다. 웹은 선석 상세를 보태는 쪽이다.
            #   표기가 다르면(LS니꼬신항부두 ↔ LS MNM 신항부두) API 표기로 맞춘다.
            api_name = hit.get("wharf_name")
            if _present(api_name) and str(api_name) != str(br["wharf_name"]):
                report.append({"kind": "WHARF_RENAMED_TO_API", "port_code": br["port_code"],
                               "name": br["wharf_name"], "detail": f"-> {api_name}"})
            if _present(api_name):
                berth.at[i, "wharf_name"] = api_name
                berth.at[i, "name_source"] = "UPA_API"

    # --- 안벽길이가 선석별 값인지 부두 총연장인지 판정한다 ------------------
    berth, wharf_total = _classify_quay_length(berth, api, report)

    # --- 부두 이름이 API 표기로 바뀌었으므로 berth_id 를 다시 만든다 --------
    # berth_id 는 wharf_name 에서 파생되는 값이라, 이름을 바꿨으면 같이 바뀌어야
    # 한다. 그대로 두면 'LS니꼬신항부두-1선석' 이 'LS MNM 신항부두' 아래 매달린다.
    berth["berth_id"] = [
        f"{w}-{int(n)}선석" if pd.notna(n) else str(w)
        for w, n in zip(berth["wharf_name"], berth["berth_no"])
    ]
    dup = berth["berth_id"][berth["berth_id"].duplicated(keep=False)]
    for d in sorted(set(dup)):
        report.append({"kind": "BERTH_ID_COLLISION", "port_code": "", "name": d,
                       "detail": "이름 통일 후 berth_id 가 겹친다 — 원본 이름 확인 필요"})

    # --- 사람이 검수한 매핑이 자동 결과를 이긴다 ----------------------------
    if os.path.exists(CODE_MAP):
        cm = pd.read_csv(CODE_MAP, encoding="utf-8-sig", dtype=str).set_index("berth_id")
        applied = 0
        for i, br in berth.iterrows():
            if br["berth_id"] in cm.index:
                for col, val in cm.loc[br["berth_id"]].dropna().items():
                    berth.at[i, col] = val
                applied += 1
        print(f"[MAP] 수기 매핑 {applied}건 적용")
    else:
        print(f"[MAP] {CODE_MAP} 없음 — 자동 매칭 결과만 사용")

    # --- 부두 테이블 --------------------------------------------------------
    def _mode(s: pd.Series):
        s = s.dropna()
        return s.mode().iloc[0] if len(s) else None

    has_lat = "_lat" in berth.columns
    has_port = "_port_name" in berth.columns
    g = berth.groupby("wharf_name", dropna=False)
    wharf = pd.DataFrame({
        "wharf_name": list(g.groups.keys()),
        # PORTMIS(arrival_facility_cd + sub_code) 와 붙는 조인 키. 부두 단위다.
        "facility_cd": g["_facility_cd"].agg(_mode).values,
        "facility_sub_code": g["_facility_sub_code"].agg(_mode).values,
        # 항 이름도 공공 API 가 정본이고, 없을 때만 웹 목록의 gubn_code 로 메운다.
        # 둘이 어긋나는 실측 사례: 석유공사부이 — 웹 gubn 은 울산신항, API 는 온산항
        # (해수청 시설현황도 온산항이다). 커버리지는 웹이 넓지만 정확도는 API 가 낫다.
        "port_name": [
            (_mode(g.get_group(k)["_port_name"]) if "_port_name" in berth.columns else None)
            or _mode(g.get_group(k)["_port_gubun"])
            for k in g.groups
        ],
        "address": g["address"].agg(_mode).values,
        # 웹 '접안능력'의 '/N척' — 부두 전체 척수(선석마다 같은 값이 반복된다)
        "berthing_vessel_count": g["_wharf_vessel_count"].agg(_mode).values,
        # ★ 감사 기준값은 **최솟값**으로 집계한다. PORTMIS 는 부두까지만 알려주므로
        #   어느 선석인지 모른 채 판정해야 하고, 최댓값을 쓰면 실제로는 못 들어가는
        #   배를 통과시킨다(거짓 안전). 기존 mart.berth_draught_check 의
        #   '안전측 최소값 원칙'과 같은 규칙을 실측 데이터로 계산한 것.
        "min_water_depth_m": g["water_depth_m"].min().values,
        "max_water_depth_m": g["water_depth_m"].max().values,
        # 접안능력은 개별 선석 값이 아니라 **그 부두에 나타난 모든 항목**에서
        # 집계한다(구성 목록형 표기를 놓치지 않기 위함).
        "min_capacity_dwt": g["_cap_values"].agg(
            lambda s: min([v for lst in s for v in lst], default=None)).values,
        "max_capacity_dwt": g["_cap_values"].agg(
            lambda s: max([v for lst in s for v in lst], default=None)).values,
        # 선석별로 확정된 값만 집계한다. 총연장을 복사해 둔 부두는 여기서 빠지고
        # total_quay_length_m 으로만 남는다(설계문서 §7 함정3).
        "min_length_m": g["length_m"].min().values,
        "max_length_m": g["length_m"].max().values,
        "total_quay_length_m": [wharf_total.get(w) for w in g.groups],
        # operator_name 은 여기 없다 — 실측 결과 같은 부두의 선석마다 운영사가
        # 다른 경우가 있었다(6부두: 고려항만·울산항6,7부두운영·한국보팍터미날).
        # 즉 운영사는 부두가 아니라 **선석**의 속성이다. berth 쪽에만 둔다.
        "name_source": g["name_source"].agg(_mode).values,
        "operator_count": g["operator_name"].nunique().values,
        "spec_spread_flag": g["_cap_values"].agg(
            lambda x: len({v for lst in x for v in lst}) > 1).values,
        "built_year": g["built_year"].agg(_mode).values,
        "built_by": g["built_by"].agg(_mode).values,
        "latitude": g["_lat"].agg(_mode).values if has_lat else None,
        "longitude": g["_lon"].agg(_mode).values if has_lat else None,
        "berth_count": g.size().values,
    })
    # 부두 속성이 선석마다 엇갈리면 대표값으로 뭉개지 말고 드러낸다.
    # (operator_name 은 애초에 선석 속성으로 옮겼으므로 여기서 보지 않는다.)
    for col in ("address", "built_by"):
        for wn, grp in g:
            vals = sorted(set(grp[col].dropna()))
            if len(vals) > 1:
                report.append({"kind": "WHARF_ATTR_CONFLICT", "port_code": "",
                               "name": str(wn), "detail": f"{col}: {vals}"})

    # --- 공공 API 의 수심으로 하한을 보정한다 -----------------------------
    wharf = _floor_depth_with_api(wharf, api, report)

    # --- 출처 대조로 확정한 보정을 마지막에 덮어쓴다 ----------------------
    # 자동 파싱 결과가 아니라 **사람이 원본을 확인해 내린 판단**이다.
    # 근거·출처가 파일에 함께 남아 diff 로 추적된다(설계문서 §13).
    wharf, berth = _apply_overrides(wharf, berth, report)

    berth = berth.drop(columns=[c for c in (
        "_lat", "_lon", "_port_name", "_facility_cd", "_facility_sub_code",
        "_wharf_vessel_count", "_cap_values", "_port_gubun") if c in berth.columns])
    os.makedirs(SEED_DIR, exist_ok=True)
    wharf.to_csv(OUT_WHARF, index=False, encoding="utf-8-sig")
    berth.to_csv(OUT_BERTH, index=False, encoding="utf-8-sig")
    pd.DataFrame(report, columns=["kind", "port_code", "name", "detail"]).to_csv(
        OUT_REPORT, index=False, encoding="utf-8-sig")

    print(f"[OUT] wharf {len(wharf)}행 -> {OUT_WHARF}")
    print(f"[OUT] berth {len(berth)}행 -> {OUT_BERTH}")
    print(f"[OUT] report {len(report)}건 -> {OUT_REPORT}")
    if report:
        for k, n in pd.DataFrame(report)["kind"].value_counts().items():
            print(f"       {k}: {n}")
    print(f"[BASIS] 안벽길이 근거: {berth['length_basis'].value_counts().to_dict()}")
    print(f"[COVER] berth  수심 {berth['water_depth_m'].notna().sum()}/{len(berth)}  "
          f"안벽길이 {berth['length_m'].notna().sum()}/{len(berth)}  "
          f"접안능력 {berth['capacity_value'].notna().sum()}/{len(berth)}")
    print(f"[COVER] wharf  시설코드 {wharf['facility_sub_code'].notna().sum()}/{len(wharf)}  "
          f"좌표 {wharf['latitude'].notna().sum() if has_lat else 0}/{len(wharf)}")
    spread = int(wharf["spec_spread_flag"].fillna(False).sum())
    print(f"[SPREAD] 선석별 접안능력이 서로 다른 부두: {spread}/{len(wharf)} "
          f"— 부두 단위로 집계하면 이만큼이 뭉개진다")


if __name__ == "__main__":
    main()
