# -*- coding: utf-8 -*-
"""PORT-MIS 계선시설 레지스트리 → wharf/berth 매핑표.

왜 별도 매핑이 필요한가
----------------------
배정의 정본은 `portmis_vessel.arrival_laidupFcltyCd + SubCd` 다. 처음에는 이
코드가 UPA 부두현황 API(`upa_berth_facility.fcltCd/fcltSubCd`)와 같은 체계라고
보고 그대로 조인하려 했다. **실측하면 아니다.**

  PORT-MIS : MBU/01 = 'SK2부두 01',  MBU/11 = 'SK1부두 11'
  UPA      : MDU/02 = 'SK2부두',     MDU/01 = 'SK1부두'

3글자 접두 공간은 공유하지만 **배정이 다르다**(SK 계열이 PORT-MIS 는 MBU,
UPA 는 MDU). 그대로 조인하면 대부분 빗나가고, 일부는 우연히 맞아 더 나쁘다.

그래서 PORT-MIS 가 실제로 쓰는 코드를 **PORT-MIS 원본에서 직접 수집**해
우리 wharf/berth 에 이름으로 연결한다. 조인 키의 정본을 배정의 정본과 같은
곳에서 가져오는 것이다.

PORT-MIS 의 입도는 시설마다 다르다
--------------------------------
  선석 단위 : '자동차부두 01' · '자동차부두 02' · '자동차부두 03'
  부두 단위 : 'SK3부두' · 'S-OIL1부두' · 'OTK부두'

운영상 배정 단위가 그대로 등록된 것으로 보인다(선석이 하나뿐인 부두는 부두가
곧 선석이다). 그래서 매핑도 두 가지가 나온다 — `berth_id` 가 잡히면 선석 단위
감사가 가능하고, `wharf_name` 만 잡히면 그 부두의 최악값(MIN)으로 판정한다.

이름 끝의 숫자는 SubCd 를 그대로 옮긴 것이지 반드시 선석 번호는 아니다
(SK1부두는 11·12 로 등록돼 있는데 선석은 2개다). 그래서 번호를 직접 믿지 않고,
**부두 내 시설 개수와 선석 개수가 같을 때만** 순서대로 대응시킨다.
개수가 다르면 사람이 판단하도록 남긴다.

출력
  data/seed/portmis_facility_map.csv   커밋 대상. 수기 보정도 여기에 한다
  data/seed/_portmis_map_report.csv    진단

실행:
  python -m data_pipeline.upa.build_portmis_facility_map
"""
from __future__ import annotations

import glob
import json
import os
import re
import sys

import pandas as pd

RAW_GLOB = "data/raw/portmis/*.json"
BERTH_CSV = "data/seed/berth_seed.csv"
WHARF_CSV = "data/seed/wharf_seed.csv"
OUT_MAP = "data/seed/portmis_facility_map.csv"
OUT_REPORT = "data/seed/_portmis_map_report.csv"

ANCHORAGE_STG = "data/staging/upa_anchorage_stg.csv"

TRAIL_NO = re.compile(r"\s*0*(\d{1,2})\s*$")


def norm(s) -> str:
    """이름 대조용. 숫자는 남기되 공백·기호·따옴표는 지운다.

    PORT-MIS 는 'S-OIL3부두'·'엘에스엠앤엠신항부두'처럼 붙여쓰거나 한글로 풀어
    적고, UPA 웹은 'S-Oil 3부두'·'LS니꼬신항부두'로 적는다. 공백 제거만으로
    붙는 것은 여기서 붙이고, 표기 자체가 다른 것은 수기 보정 대상으로 남긴다.
    """
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    return re.sub(r"[\s\-_()·\"'“”]", "", str(s)).lower()


def load_registry() -> pd.DataFrame:
    """PORT-MIS raw JSON에서 (코드, 서브코드, 시설명) 고유 목록 + 입항 배정 건수.

    배정 건수를 같이 세는 이유: 못 붙인 시설이 생겼을 때 **무엇부터 손볼지**를
    정하려면 그 시설에 배가 얼마나 들어오는지 알아야 한다. 배정이 없는 부두는
    좌표·코드가 비어 있어도 감사에 아무 영향이 없다.

    ★ 레코드가 아니라 **입항건**을 센다. raw 파일은 수집 구간이 겹치고
      (20260820_20260820 과 20260820_20260821 등), 같은 항차가 신고 차수
      (최초/변경/최종)마다 다시 실린다. 그대로 세면 1078건이 나오는데 실제
      고유 입항건은 698건이다 — 1.5배 부풀려진다.
      자연키는 clsgn(호출부호) + etryptYear + etryptCo 다.
    """
    rows: list[tuple[str, str, str]] = []

    def walk(o):
        if isinstance(o, list):
            for x in o:
                walk(x)
        elif isinstance(o, dict):
            call_key = f"{o.get('clsgn')}|{o.get('etryptYear')}|{o.get('etryptCo')}"
            for pre in ("arrival", "departure"):
                cd = o.get(f"{pre}_laidupFcltyCd")
                if cd:
                    rows.append((cd, o.get(f"{pre}_laidupFcltySubCd"),
                                 o.get(f"{pre}_laidupFcltyNm"),
                                 pre == "arrival", call_key))
            for v in o.values():
                walk(v)

    files = glob.glob(RAW_GLOB)
    if not files:
        sys.exit(f"{RAW_GLOB} 없음 — PORT-MIS 원본 수집이 먼저다")
    for f in files:
        with open(f, encoding="utf-8") as fh:
            walk(json.load(fh))
    raw = pd.DataFrame(rows, columns=["facility_cd", "facility_sub_code",
                                      "facility_nm", "is_arrival", "call_key"]).dropna(
        subset=["facility_cd", "facility_sub_code", "facility_nm"])
    # 입항건 단위로 접은 뒤 센다. 시설이 신고 차수마다 바뀐 경우는 마지막 값을
    # 쓴다(최종 신고에 가깝다).
    arr = raw[raw.is_arrival].drop_duplicates("call_key", keep="last")
    cnt = arr.groupby(["facility_cd", "facility_sub_code"]).size().rename("arrival_count")
    df = (raw.drop(columns=["is_arrival", "call_key"]).drop_duplicates()
          .join(cnt, on=["facility_cd", "facility_sub_code"])
          .sort_values(["facility_cd", "facility_sub_code"]))
    df["arrival_count"] = df["arrival_count"].fillna(0).astype(int)
    print(f"[IN ] PORT-MIS 원본 {len(files)}개 · 레코드 {len(raw)} → "
          f"고유 입항건 {len(arr)} · 고유 계선시설 {len(df)}종")
    return df.reset_index(drop=True)


def main() -> None:
    reg = load_registry()
    berth = pd.read_csv(BERTH_CSV, encoding="utf-8-sig")
    wharf = pd.read_csv(WHARF_CSV, encoding="utf-8-sig")

    # 정박지 판별은 **추측하지 않고 정박지 레지스트리와 대조한다.**
    #   초판은 코드 접두어로 갈랐다(W*/MQ*). W* 는 맞았지만 MQ* 가 틀렸다 —
    #   MQP-01 은 '미포부두 01', MQP-03 은 '현중미포만의장안벽' 으로 정박지가
    #   아니라 안벽이다. 접두어로 추측하면 이런 것이 조용히 감사에서 빠진다.
    #   upa_anchorage.facility_code 는 'WAE-02' 결합형이라 PORT-MIS 의
    #   (cd, sub) 를 같은 형태로 만들어 대조하면 된다.
    anc_codes: set[str] = set()
    if os.path.exists(ANCHORAGE_STG):
        anc = pd.read_csv(ANCHORAGE_STG, encoding="utf-8-sig", dtype=str)
        anc.columns = [c.strip("﻿") for c in anc.columns]
        anc_codes = set(anc["facility_code"].dropna())
    else:
        print(f"[WARN] {ANCHORAGE_STG} 없음 — 정박지를 가려낼 수 없다")
    reg["_code"] = reg.facility_cd + "-" + reg.facility_sub_code.astype(str).str.zfill(2)
    reg["is_anchorage"] = reg["_code"].isin(anc_codes)
    print(f"[ANC ] 정박지로 판별 {int(reg.is_anchorage.sum())}건 "
          f"(정박지 레지스트리 {len(anc_codes)}종)")
    # 시설명에서 부두명을 얻는다 — 끝의 숫자는 SubCd 를 옮긴 것이라 떼어낸다.
    reg["wharf_guess"] = reg.facility_nm.map(lambda x: TRAIL_NO.sub("", str(x)).strip())

    berth_by_wharf = {w: g.sort_values("berth_no", na_position="first")
                      for w, g in berth.groupby("wharf_name")}
    wlut: dict[str, str] = {}
    for _, w in wharf.iterrows():
        wlut[norm(w["wharf_name"])] = w["wharf_name"]
    # 부두명 표기가 다른 경우를 흡수한다. 자동 규칙을 늘리지 않고 사전으로 둔다.
    ALIAS = {
        "soil1부두": "S-Oil 1부두", "soil2부두": "S-Oil 2부두",
        "soil3부두": "S-Oil 3부두", "soil4부두": "S-Oil 4부두",
        "엘에스엠앤엠신항부두": "LS MNM 신항부두",
        "신항컨테이너부두": "신항컨부두",
        "신항남방파제t/s부두": "신항남방파제 T/S부두",
        "신항북방파제t/s부두": "신항북방파제 T/S부두",
        # 부두 이름의 정본은 공공 API 다(build_berth_seed). 아래 대상 이름은
        # 모두 API 표기이며, 웹 표기로 적으면 매칭이 빗나간다.
        "현대오일터미널신항2부두": "현대오일터미널 신항2부두",
        "soil부이": "S-Oil부이",
        # UPA 공공 API 가 MDW/2 를 OTK1부두로 적는다 — PORT-MIS 의 'OTK부두'와 같은 시설.
        "otk부두": "OTK1부두",
        # '유화1부두'(PORT-MIS) = '대한유화부두'(UPA 웹). 약칭 표기 차이.
        "유화1부두": "대한유화부두",
    }

    # 이름만으로는 어느 부두인지 못 고르는 건들. 시설코드로 직접 지정한다.
    #   PORT-MIS 는 용잠 2개 시설을 '용잠부두 01' · '용잠부두 02' 로 적지만
    #   우리 표기(=API 정본)는 '용잠1부두'(88m) · '용잠2부두'(155m) 다.
    #   PORT-MIS 는 등장 순서대로 01·02 로 적으므로 순서대로 대응시킨다.
    #   부이는 'SK부이 02' 로 적히는데 우리 표기는 'SK2부이' 다. 끝 숫자를 떼면
    #   부두명이 'SK부이' 가 되어 버려 이름으로는 어느 부이인지 못 고른다.
    FACILITY_WHARF = {
        ("MBY", "01"): "용잠1부두", ("MBY", "03"): "용잠2부두",
        ("MUY", "02"): "SK2부이", ("MUY", "03"): "SK3부이",
    }

    # 제원 자료를 어디서도 구할 수 없다고 **확인된** 시설.
    #
    # 처음에는 장생포호안을 "안벽이 아니니 제외" 로 빼 두었는데, 실측하니 입항
    # 배정이 124건으로 안벽 시설 중 두 번째로 많았다. 조용히 빼면 감사 대상의
    # 큰 덩어리가 없는 셈이 된다. 그래서 빼지 않고 **사유를 붙여 표에 남긴다** —
    # 판정은 UNKNOWN 이되, "왜 모르는지"가 데이터에 남아야 한다.
    #
    # 이 목록에 있으면 '알려진 공백'(더 할 일 없음)이고, 없으면서 안 붙은 것은
    # '미처리'(작업 대기열)다. 둘을 섞으면 대기열이 줄지 않는다.
    KNOWN_GAPS = {
        "장생포호안": "UPA 웹 부두현황·공공 API·해수청 시설현황 어디에도 없음. "
                   "호안(護岸)이라 안벽 제원 공시 대상이 아닌 것으로 보인다",
        "이진소형선부두": "세 출처 모두에 없음. 소형선 전용이라 제원 공시 대상이 아닌 듯",
        "북신항 에너지부두02": "공사·계획 단계. 보도상 접안능력이 계획에서 변경됐고 "
                          "준공 전 수치는 감사 기준으로 쓸 수 없다",
        "북신항 에너지부두04": "공사·계획 단계(위와 같음)",
    }
    # 조선소 의장안벽(艤裝岸壁). UPA 부두현황·해수청 시설현황은 **상업 항만시설**만
    # 싣는다 — 조선소가 자사 건조선박을 대는 안벽은 공시 대상이 아니다. 그래서
    # 세 출처 어디에도 없고, 앞으로도 생기지 않는다.
    # (초판은 이들을 MQ* 접두어만 보고 정박지로 오분류해 아예 빼 두었었다.)
    for nm in ("현중미포만의장안벽", "현중해양의장안벽"):
        KNOWN_GAPS[nm] = "조선소 의장안벽 — 상업 항만시설이 아니라 제원 공시 대상이 아님"
    for n in range(1, 10):
        KNOWN_GAPS[f"현대미포의장안벽 0{n}"] = (
            "조선소 의장안벽 — 상업 항만시설이 아니라 제원 공시 대상이 아님")

    report: list[dict] = []
    out: list[dict] = []

    # 정박지만 뺀다. 나머지는 붙든 못 붙든 **전부 표에 남긴다** — 못 붙인 것을
    # 목록에서 지우면 무엇이 빠졌는지 알 수 없게 된다.
    targets = reg[~reg.is_anchorage]
    for wg, grp in targets.groupby("wharf_guess"):
        key = norm(wg)
        wname = wlut.get(key) or ALIAS.get(key)

        # 시설코드로 직접 지정한 건이 있으면 그것이 이름 매칭을 이긴다.
        direct = {(r.facility_cd, str(r.facility_sub_code)) for _, r in grp.iterrows()}
        if any(k in FACILITY_WHARF for k in direct):
            for _, r in grp.iterrows():
                w = FACILITY_WHARF.get((r.facility_cd, str(r.facility_sub_code)))
                if w is None:
                    continue
                kid = berth_by_wharf.get(w, pd.DataFrame())
                out.append({"facility_cd": r.facility_cd, "facility_sub_code": r.facility_sub_code,
                            "facility_nm": r.facility_nm, "wharf_name": w,
                            "berth_id": kid.iloc[0].berth_id if len(kid) == 1 else None,
                            "match_level": "BERTH" if len(kid) == 1 else "WHARF",
                            "confidence": "MANUAL", "arrival_count": int(r.arrival_count)})
            continue

        if wname is None:
            # 못 붙였다고 행을 버리지 않는다. '확인된 공백'인지 '아직 미처리'인지
            # 구분해 남겨야 대기열이 관리된다.
            for _, r in grp.iterrows():
                reason = KNOWN_GAPS.get(str(r.facility_nm))
                out.append({"facility_cd": r.facility_cd,
                            "facility_sub_code": r.facility_sub_code,
                            "facility_nm": r.facility_nm, "wharf_name": None,
                            "berth_id": None,
                            "match_level": "KNOWN_GAP" if reason else "UNMAPPED",
                            "confidence": "CONFIRMED_NO_SOURCE" if reason else "TODO",
                            "gap_reason": reason, "arrival_count": int(r.arrival_count)})
                report.append({"kind": "KNOWN_GAP" if reason else "UNMAPPED",
                               "facility": f"{r.facility_cd}/{r.facility_sub_code}",
                               "name": r.facility_nm,
                               "detail": reason or "우리 시드에 대응 부두 없음 — 확인 필요"})
            continue
        kids = berth_by_wharf.get(wname, pd.DataFrame())
        n_fac, n_berth = len(grp), len(kids)
        grp = grp.sort_values("facility_sub_code")
        if n_fac == n_berth and n_berth > 0:
            # 개수가 같을 때만 순서대로 대응 — 선석 단위 감사가 가능해진다.
            for (_, r), (_, b) in zip(grp.iterrows(), kids.iterrows()):
                out.append({"facility_cd": r.facility_cd, "facility_sub_code": r.facility_sub_code,
                            "facility_nm": r.facility_nm, "wharf_name": wname,
                            "berth_id": b.berth_id, "match_level": "BERTH",
                            "confidence": "AUTO_COUNT_MATCH", "arrival_count": int(r.arrival_count)})
        else:
            # 개수가 다르면 어느 선석인지 특정할 수 없다. 부두 단위로만 붙이고,
            # 감사는 그 부두의 최악값(MIN)으로 판정한다.
            for _, r in grp.iterrows():
                out.append({"facility_cd": r.facility_cd, "facility_sub_code": r.facility_sub_code,
                            "facility_nm": r.facility_nm, "wharf_name": wname,
                            "berth_id": None, "match_level": "WHARF",
                            "confidence": "COUNT_MISMATCH", "arrival_count": int(r.arrival_count)})
            report.append({"kind": "COUNT_MISMATCH", "facility": grp.iloc[0].facility_cd,
                           "name": wname, "detail": f"PORT-MIS 시설 {n_fac}건 vs 우리 선석 {n_berth}건"})

    full = pd.DataFrame(out)
    # arrival_count 는 **커밋되는 매핑 파일에 넣지 않는다.** 수집 창에 따라
    # 달라지는 파생 통계라, 정적 시드에 넣으면 즉시 낡고 파일이 매번 흔들린다.
    # 공백 우선순위를 볼 때만 필요하므로 진단 리포트와 화면 출력에만 쓴다.
    # (DB 에서 항상 정확한 값이 필요하면 portmis_vessel 을 직접 세면 된다 —
    #  berth_audit_views.sql 하단의 mart.berth_facility_traffic 참고)
    m = pd.DataFrame(out, columns=["facility_cd", "facility_sub_code", "facility_nm",
                                   "wharf_name", "berth_id", "match_level", "confidence",
                                   "gap_reason"])
    m.to_csv(OUT_MAP, index=False, encoding="utf-8-sig")
    for _, r in full.iterrows():
        if r["match_level"] in ("KNOWN_GAP", "UNMAPPED"):
            report.append({"kind": f"{r['match_level']}_TRAFFIC",
                           "facility": f"{r['facility_cd']}/{r['facility_sub_code']}",
                           "name": r["facility_nm"],
                           "detail": f"입항배정 {int(r['arrival_count'])}건 (수집 창 기준)"})
    pd.DataFrame(report, columns=["kind", "facility", "name", "detail"]).to_csv(
        OUT_REPORT, index=False, encoding="utf-8-sig")

    total_berth_fac = int((~reg.is_anchorage).sum())
    print(f"[OUT] {len(m)}/{total_berth_fac} -> {OUT_MAP}")
    for lv, g in full.groupby("match_level"):
        print(f"       {lv}: {len(g)}건 · 입항배정 {int(g.arrival_count.sum())}건")
    gaps = full[full.match_level.isin(["KNOWN_GAP", "UNMAPPED"])].sort_values(
        "arrival_count", ascending=False)
    if len(gaps):
        print("\n[GAP] 붙지 않은 시설 — 배정 많은 순")
        for _, r in gaps.iterrows():
            print(f"       {r.facility_cd}/{r.facility_sub_code} {r.facility_nm} "
                  f"· 배정 {r.arrival_count}건 · {r.match_level}")
    print(f"[OUT] 진단 {len(report)}건 -> {OUT_REPORT}")
    if report:
        for k, n in pd.DataFrame(report)["kind"].value_counts().items():
            print(f"       {k}: {n}")


if __name__ == "__main__":
    main()
