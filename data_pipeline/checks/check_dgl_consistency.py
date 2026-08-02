# -*- coding: utf-8 -*-
"""
check_dgl_consistency.py — DGL 참조표를 KOSHA MSDS 원문과 대조

교차검증 지적 A("합성 위험물 데이터는 화학적/규제적 교차검증 없으면 가짜")에 대한
직접 대응이다. imdg_dgl.py 의 하드코딩 표를 사람이 눈으로 믿는 대신, 이미 DB 에
적재된 **KOSHA MSDS 원문**과 기계적으로 대조한다.

대조 근거 (KOSHA MSDS 16개 섹션의 안정 키):
    N02 = UN 번호
    N06 = 운송에서의 위험성 등급 (IMDG Class)
    N08 = 용기등급 (Packing Group)
이 값들은 우리가 만든 것이 아니라 KOSHA 가 배포하는 공식 MSDS 원문이다.
따라서 "LLM 이 지어낸 값"이라는 공격에 대한 1차 방어선이 된다.

한계 — 반드시 함께 보고할 것:
    KOSHA MSDS 는 IMDG Code 원문 자체가 아니라 국내 물질안전보건자료다.
    둘이 일치하면 신뢰도가 크게 올라가지만, 최종 근거는 여전히
    IMDG Code Vol.2 Chapter 3.2 이다. MSDS 에 없는 물질은 여기서 검증되지
    않으므로 imdg_dgl.py 의 verified 플래그가 TODO 로 남는다.

실행:
    py -m data_pipeline.checks.check_dgl_consistency
"""
from __future__ import annotations

import os
import re
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, BASE_DIR)

from data_pipeline.reference.imdg_dgl import DGL, unverified_entries  # noqa: E402

ROMAN = {"Ⅰ": "I", "Ⅱ": "II", "Ⅲ": "III", "I": "I", "II": "II", "III": "III",
         "1": "I", "2": "II", "3": "III"}


def _norm_un(v) -> str | None:
    m = re.search(r"([0-9]{4})", str(v or ""))
    return m.group(1) if m else None


def _norm_class(v) -> str | None:
    """'3', 'Class 3', '제3급', '2.1' 등에서 등급만 뽑는다."""
    m = re.search(r"([1-9](?:\.[1-9])?)", str(v or ""))
    return m.group(1) if m else None


def _norm_pg(v) -> str | None:
    s = str(v or "").strip()
    m = re.search(r"(Ⅰ|Ⅱ|Ⅲ|III|II|I|[123])", s)
    return ROMAN.get(m.group(1)) if m else None


def main() -> int:
    print("=" * 74)
    print(" DGL 참조표 ↔ KOSHA MSDS 원문 대조")
    print("=" * 74)

    try:
        from dotenv import load_dotenv
        from sqlalchemy import create_engine, text
        load_dotenv(os.path.join(BASE_DIR, ".env"))
        url = (f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:"
               f"{os.environ['POSTGRES_PASSWORD']}@{os.environ['POSTGRES_HOST']}:"
               f"{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}")
        eng = create_engine(url)
        with eng.connect() as cx:
            rows = cx.execute(text("""
                SELECT dg_un_no, name_ko, imdg_class, packing_group
                FROM mart.msds_flat
                WHERE dg_un_no IS NOT NULL
            """)).fetchall()
    except Exception as e:  # noqa: BLE001
        print(f"[건너뜀] DB 조회 실패 — {type(e).__name__}: {e}")
        print("         mart.msds_flat 이 있는 PostgreSQL 에 연결되어야 한다.")
        print("         (mart_views.sql 실행 + .env 의 POSTGRES_* 설정 확인)")
        _report_unverified()
        return 2

    msds: dict[str, list] = {}
    for un, nm, cls, pg in rows:
        key = _norm_un(un)
        if key:
            msds.setdefault(key, []).append((nm, _norm_class(cls), _norm_pg(pg)))

    print(f"\nMSDS 에서 UN 번호를 가진 화학물질: {len(msds)}종")
    print(f"DGL 참조표 등재: {len(DGL)}종\n")

    matched, mismatched, absent = [], [], []
    for un, e in sorted(DGL.items()):
        if un not in msds:
            absent.append(e)
            continue
        ok, detail = False, []
        for nm, cls, pg in msds[un]:
            cls_ok = (cls is None) or (cls == e.imdg_class)
            pg_ok = (pg is None) or (not e.packing_groups) or (pg in e.packing_groups)
            if cls_ok and pg_ok:
                ok = True
                break
            detail.append(f"MSDS '{nm}' → Class {cls}, PG {pg}")
        (matched if ok else mismatched).append((e, detail))

    print(f"[1] 일치         : {len(matched):>3}종")
    print(f"[2] 불일치       : {len(mismatched):>3}종  ← 반드시 확인")
    print(f"[3] MSDS 에 없음 : {len(absent):>3}종  ← IMDG Code 원문 대조 필요")

    if mismatched:
        print("\n--- [2] 불일치 상세 ---")
        for e, detail in mismatched:
            print(f"  UN{e.un_no} {e.psn_ko}")
            print(f"    DGL 표 : Class {e.imdg_class}, PG {'/'.join(e.packing_groups) or '-'}")
            for d in detail:
                print(f"    {d}")

    if absent:
        print("\n--- [3] MSDS 에 없어 미검증 ---")
        for e in absent:
            print(f"  UN{e.un_no} {e.psn_ko:<24} (Class {e.imdg_class})")

    if matched:
        print("\n--- [1] 일치 (imdg_dgl.py 의 verified 를 'MSDS' 로 올려도 되는 항목) ---")
        for e, _ in matched:
            print(f"  UN{e.un_no} {e.psn_ko}")

    _report_unverified()

    print("\n" + "=" * 74)
    if mismatched:
        print(f" 결과: FAIL — 불일치 {len(mismatched)}종. 합성 데이터를 쓰기 전에 해소할 것.")
        return 1
    print(f" 결과: PASS — 불일치 0. 단, MSDS 미수록 {len(absent)}종은 여전히 IMDG 원문 대조 필요.")
    print("=" * 74)
    return 0


def _report_unverified() -> None:
    todo = unverified_entries()
    if todo:
        print(f"\n[고지] imdg_dgl.py 에서 아직 1차 출처 대조가 끝나지 않은 항목 {len(todo)}종.")
        print("       보고서·시연에 인용하기 전 IMDG Code Vol.2 Chapter 3.2 로 확인할 것.")


if __name__ == "__main__":
    sys.exit(main())
