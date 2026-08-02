# -*- coding: utf-8 -*-
"""
diag_callsgn_gap.py — callsgn 결측 구조 진단

목적
  UPA getVslPstnInfo 의 callsgn 결측이 "간헐적(intermittent)"인지
  "체계적(systematic)"인지 판정한다. 둘은 처방이 정반대다.

    간헐적 = 어떤 스냅샷엔 있고 어떤 스냅샷엔 없다
             → Stateful Filling(과거 신호 끌어오기)으로 회복 가능
    체계적 = 그 선박은 아예 정적신호(AIS Msg 5/24)를 송출하지 않는다
             → 끌어올 과거가 없다. 다른 소스가 필요하다

  그리고 결측 선박이 우리 목표(울산항 액체화물선 식별)에 실제로 영향을
  주는지를 흘수·선명·MMSI 포맷으로 교차 판정한다.

실행
    cd data-pipeline
    py -m data_pipeline.checks.diag_callsgn_gap                # 저장된 raw 전체
    py -m data_pipeline.checks.diag_callsgn_gap --live 3 --interval 360
        → 6분 간격으로 3회 라이브 수집 후 진단 (정적신호 주기가 6분이므로
          최소 2회 이상 떠 있어야 '간헐 결측'을 관측할 수 있다)

출력
  콘솔 리포트 + data/staging/diag_callsgn_gap.csv
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sys
import time
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw", "upa")
OUT_CSV = os.path.join(BASE_DIR, "data", "staging", "diag_callsgn_gap.csv")

# ---------------------------------------------------------------------------
# MMSI 포맷 판정 — ITU-R M.585 (MMSI 할당 규칙)
#   MIDxxxxxx  (9자리, 앞 3자리 MID) : 일반 선박국
#   00MIDxxxx                        : 해안국(Coast station)
#   98MIDxxxx                        : 모선에 부속된 보조선(craft associated
#                                      with a parent ship) — 예선·통선 등
#   99MIDxxxx                        : 항행보조시설(AtoN) — 부표·등표
#   970/972/974                      : AIS-SART / MOB / EPIRB
#   한국 MID = 440, 441
# 위 특수 포맷은 정의상 '액체화물을 싣는 상선'일 수 없다 → 즉시 배제 가능.
# ---------------------------------------------------------------------------
def classify_mmsi(mmsi: str) -> str:
    m = str(mmsi or "").strip()
    if not m.isdigit():
        return "형식오류"
    if m.startswith("00"):
        return "해안국(00MID)"
    if m.startswith("98"):
        return "모선부속선(98MID)"
    if m.startswith("99"):
        return "항행보조시설(99MID)"
    if m[:3] in ("970", "972", "974"):
        return "조난장치(SART/MOB/EPIRB)"
    if len(m) != 9:
        return f"비표준자리수({len(m)})"
    if m[:3] in ("440", "441"):
        return "일반선박(한국)"
    return f"일반선박(MID {m[:3]})"


# 액체화물선이 아닐 가능성이 매우 높은 선명 키워드
#   관공선·소방정·순찰선·예부선·통선 등. 이들은 SOLAS Class A AIS 의무
#   대상이 아니거나 정적신호를 제한적으로만 송출한다.
SERVICE_NAME_KEYWORDS = (
    "KCG", "COAST GUARD", "해경", "SOBANG", "소방", "FIRE",
    "SOONCHAL", "순찰", "PATROL", "PILOT", "도선", "TUG", "예선",
    "POLICE", "경찰", "NAVY", "군", "SURVEY", "측량", "DREDG", "준설",
)

# 상선(액체화물선 포함)이 통상 갖는 최소 흘수(m).
#   울산항 액체부두 수심이 7.5~18m 이고, 접안하는 유조선/케미칼선은
#   실측상 흘수 6m 이상. 3m 미만이면 소형 서비스선일 확률이 매우 높다.
MERCHANT_MIN_DRAUGHT_M = 3.0


def _load_raw_snapshots(paths: list) -> list:
    """(스냅샷라벨, [관측...]) 리스트로 정규화."""
    snaps = []
    for p in sorted(paths):
        try:
            with open(p, encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:  # noqa: BLE001
            print(f"  [건너뜀] {os.path.basename(p)} — {e}")
            continue
        items = data
        if isinstance(data, dict):
            body = data.get("response", {}).get("body", data.get("body", {}))
            items = body.get("items", data.get("items", []))
            if isinstance(items, dict):
                items = items.get("item", [])
        if not isinstance(items, list):
            continue
        snaps.append((os.path.basename(p), items))
    return snaps


def _collect_live(rounds: int, interval_s: int) -> list:
    from dotenv import load_dotenv

    load_dotenv(os.path.join(BASE_DIR, ".env"))
    from data_pipeline.upa.upa_collector import UpaClient

    client = UpaClient()
    snaps = []
    for i in range(rounds):
        items = client.fetch_all("VslPstnInfoService/getVslPstnInfo", num_of_rows=100)
        label = f"live#{i + 1}"
        print(f"  {label}: {len(items)}건 수집")
        snaps.append((label, items))
        if i < rounds - 1:
            print(f"  ... {interval_s}초 대기 (AIS 정적신호 주기 6분)")
            time.sleep(interval_s)
    return snaps


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", type=int, default=0,
                    help="라이브 수집 횟수 (0이면 저장된 raw 파일 사용)")
    ap.add_argument("--interval", type=int, default=360,
                    help="라이브 수집 간격(초). 기본 360 = 6분")
    args = ap.parse_args()

    print("=" * 74)
    print(" callsgn 결측 구조 진단 — 간헐적(회복가능) vs 체계적(회복불가)")
    print("=" * 74)

    if args.live > 0:
        snaps = _collect_live(args.live, args.interval)
    else:
        paths = glob.glob(os.path.join(RAW_DIR, "*vessel_position*.json"))
        if not paths:
            print(f"[FAIL] {RAW_DIR} 에 선박위치 raw 파일이 없습니다.")
            print("       --live 3 옵션으로 라이브 수집하거나, 먼저 수집을 돌리세요.")
            return 1
        snaps = _load_raw_snapshots(paths)

    if len(snaps) < 2:
        print(f"\n[주의] 스냅샷이 {len(snaps)}개뿐입니다. '간헐 결측' 판정에는")
        print("       서로 다른 시점의 스냅샷이 최소 2개 필요합니다.")
        print("       --live 3 --interval 360 으로 다시 돌리는 것을 권합니다.\n")

    # ── 선박(MMSI)별로 관측 집계 ────────────────────────────────────────────
    obs = defaultdict(lambda: {"snaps_seen": set(), "snaps_with_cs": set(),
                               "callsgns": set(), "names": set(),
                               "draughts": [], "n": 0})
    total_obs = 0
    obs_missing_cs = 0
    for label, items in snaps:
        for it in items:
            total_obs += 1
            mmsi = str(it.get("mmsiNo") or it.get("mmsi") or "").strip()
            cs = (it.get("callsgn") or "").strip()
            nm = (it.get("vslNm") or it.get("vessel_name") or "").strip()
            drft = it.get("drft", it.get("draught"))
            if not cs:
                obs_missing_cs += 1
            if not mmsi:
                continue
            r = obs[mmsi]
            r["n"] += 1
            r["snaps_seen"].add(label)
            if cs:
                r["snaps_with_cs"].add(label)
                r["callsgns"].add(cs.upper())
            if nm:
                r["names"].add(nm)
            try:
                if drft is not None and str(drft) != "":
                    r["draughts"].append(float(drft))
            except (TypeError, ValueError):
                pass

    n_vessels = len(obs)
    always, partial, never = [], [], []
    for mmsi, r in obs.items():
        if len(r["snaps_with_cs"]) == len(r["snaps_seen"]) and r["snaps_with_cs"]:
            always.append(mmsi)
        elif r["snaps_with_cs"]:
            partial.append(mmsi)
        else:
            never.append(mmsi)

    print(f"\n[1] 관측 규모")
    print(f"  스냅샷 수      : {len(snaps)}개  ({', '.join(l for l, _ in snaps[:4])}"
          f"{' ...' if len(snaps) > 4 else ''})")
    print(f"  총 관측(행)    : {total_obs:,}건")
    print(f"  그중 callsgn 결측 : {obs_missing_cs:,}건 "
          f"({100.0 * obs_missing_cs / max(total_obs, 1):.1f}%)")
    print(f"  고유 MMSI      : {n_vessels:,}척")

    print(f"\n[2] ★ 핵심 판정 — 결측이 간헐적인가 체계적인가")
    print(f"  항상 callsgn 있음        : {len(always):>5}척")
    print(f"  일부 스냅샷만 있음       : {len(partial):>5}척  "
          f"← Stateful Filling 이 회복하는 대상")
    print(f"  한 번도 없음             : {len(never):>5}척  "
          f"← 끌어올 과거가 없음(체계적 결측)")
    if len(snaps) >= 2:
        if partial:
            print(f"\n  → 간헐 결측이 {len(partial)}척 존재. Stateful Filling 이 실제로 기여한다.")
        if never:
            print(f"  → 그러나 {len(never)}척은 정적신호 자체를 송출하지 않는다.")
            print(f"     이 선박들은 채우기로 해결되지 않는다. MMSI-First 로 남기고")
            print(f"     선종은 별도 소스(AIS ShipStaticData / PORT-MIS / 계선시설코드)로 판정해야 한다.")

    # ── 결측 선박이 액체화물선일 수 있는가 ──────────────────────────────────
    print(f"\n[3] callsgn 없는 {len(never)}척이 우리 목표(액체화물선)에 영향을 주는가")
    fmt_bucket = defaultdict(int)
    kw_hit, small_draught, unknown = 0, 0, []
    for mmsi in never:
        r = obs[mmsi]
        fmt_bucket[classify_mmsi(mmsi)] += 1
        name = next(iter(r["names"]), "")
        up = name.upper()
        hit = any(k in up for k in SERVICE_NAME_KEYWORDS)
        maxd = max(r["draughts"]) if r["draughts"] else None
        small = maxd is not None and maxd < MERCHANT_MIN_DRAUGHT_M
        if hit:
            kw_hit += 1
        if small:
            small_draught += 1
        if not hit and not small:
            unknown.append((mmsi, name, maxd))

    print(f"  MMSI 포맷 분포:")
    for k, v in sorted(fmt_bucket.items(), key=lambda x: -x[1]):
        print(f"    {k:<28} {v:>5}척")
    print(f"  선명이 관공선/소방정/순찰선/예선 키워드    : {kw_hit:>5}척")
    print(f"  최대 흘수 < {MERCHANT_MIN_DRAUGHT_M}m (소형 서비스선 추정) : {small_draught:>5}척")
    print(f"  위 두 근거 모두 없음(추가 확인 필요)       : {len(unknown):>5}척")
    if unknown:
        print(f"    ↓ 상위 15척 — 이 목록만 사람이 눈으로 확인하면 된다")
        for mmsi, nm, d in sorted(unknown, key=lambda x: -(x[2] or 0))[:15]:
            print(f"      MMSI {mmsi}  흘수 {('%.1f' % d) if d else '  -  '}m  {nm}")

    # ── CSV 저장 ────────────────────────────────────────────────────────────
    import csv
    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
    with open(OUT_CSV, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["mmsi", "callsgn_pattern", "mmsi_format", "vessel_name",
                    "max_draught_m", "snaps_seen", "snaps_with_callsgn",
                    "callsgns", "service_keyword_hit", "likely_not_liquid"])
        for bucket, mmsis in (("항상있음", always), ("일부만있음", partial),
                              ("한번도없음", never)):
            for mmsi in mmsis:
                r = obs[mmsi]
                nm = next(iter(r["names"]), "")
                maxd = max(r["draughts"]) if r["draughts"] else None
                hit = any(k in nm.upper() for k in SERVICE_NAME_KEYWORDS)
                small = maxd is not None and maxd < MERCHANT_MIN_DRAUGHT_M
                fmt = classify_mmsi(mmsi)
                special = fmt not in ("일반선박(한국)",) and not fmt.startswith("일반선박(MID")
                w.writerow([mmsi, bucket, fmt, nm,
                            f"{maxd:.1f}" if maxd is not None else "",
                            len(r["snaps_seen"]), len(r["snaps_with_cs"]),
                            ";".join(sorted(r["callsgns"])),
                            hit, hit or small or special])
    print(f"\n[4] 상세 결과 저장 → {OUT_CSV}")

    print("\n" + "=" * 74)
    if partial:
        print(" 결론: 간헐 결측 존재 → Stateful Filling 유지 + MMSI-First 병행 필요")
    elif never:
        print(" 결론: 결측은 전부 체계적 → Stateful Filling 만으로는 0척 회복.")
        print("       MMSI-First(뷰에서 안 버리기) + 선종 대체소스가 본 해법.")
    else:
        print(" 결론: 결측 없음")
    print("=" * 74)
    return 0


if __name__ == "__main__":
    sys.exit(main())
