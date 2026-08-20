# -*- coding: utf-8 -*-
"""
restage_derived_columns.py — 원천 재수집 없이 파생 컬럼만 다시 계산

문제 상황
--------------------------------------------------------------------------
PORT-MIS 선종코드 매핑을 공식 CODE BOOK 으로 정정(커밋 fea6c1d)하기 **전에**
만들어진 staging 파일이 남아 있으면, 파일 안의 ship_kind_category 는
구 매핑 결과("화물/시멘트선", "목재선" …)를 그대로 들고 있다.
그 staging 으로 합성 화물을 만들면 액체화물이 거의 배정되지 않는다.

핵심은 이것이다 — **원본 코드(ship_kind_cd)와 원본 선종명(ship_kind_nm)은
staging 안에 그대로 살아 있다.** 틀린 것은 그 코드로부터 계산한 파생 컬럼뿐이다.
따라서 API 를 다시 부를 필요 없이, 현재 전처리기의 매핑으로 파생 컬럼만
다시 계산하면 된다.

다시 계산하는 컬럼
    ship_kind_category      선종 대분류 라벨
    is_liquid_cargo_vessel  액체화물 본선 여부
    is_liquid_cargo_barge   액체화물 부선 여부
    is_bunkering_vessel     급유선 여부

원본 컬럼(ship_kind_cd, ship_kind_nm, callsgn …)은 손대지 않는다.

실행
    py -m data_pipeline.checks.restage_derived_columns            # 미리보기
    py -m data_pipeline.checks.restage_derived_columns --apply    # 실제 반영
"""
from __future__ import annotations

import argparse
import os
import shutil
import sys

import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, BASE_DIR)

from data_pipeline.preprocessors.portmis_preprocessor import (  # noqa: E402
    BUNKERING_VESSEL_CODES,
    LIQUID_CARGO_BARGE_CODES,
    LIQUID_CARGO_KIND_CODES,
    LIQUID_NAME_KEYWORDS,
    SHIP_KIND_CATEGORY,
)

STG = os.path.join(BASE_DIR, "data", "staging", "portmis_vessel_stg.csv")


def recompute(df: pd.DataFrame) -> pd.DataFrame:
    """portmis_preprocessor 의 8·9·9-1·9-2 단계와 동일한 계산."""
    cd = (df["ship_kind_cd"].astype(str).str.strip()
          .str.replace(r"\.0$", "", regex=True))
    out = df.copy()
    out["ship_kind_category"] = cd.map(SHIP_KIND_CATEGORY).fillna("기타/불명")
    by_code = cd.isin(LIQUID_CARGO_KIND_CODES)
    if "ship_kind_nm" in out.columns:
        pat = "|".join(LIQUID_NAME_KEYWORDS)
        by_name = out["ship_kind_nm"].astype(str).str.contains(pat, case=False, na=False)
    else:
        by_name = False
    out["is_liquid_cargo_vessel"] = by_code | by_name
    out["is_liquid_cargo_barge"] = cd.isin(LIQUID_CARGO_BARGE_CODES)
    out["is_bunkering_vessel"] = cd.isin(BUNKERING_VESSEL_CODES)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="실제로 파일을 덮어쓴다")
    args = ap.parse_args()

    if not os.path.exists(STG):
        print(f"[FAIL] {STG} 가 없습니다.")
        return 1

    df = pd.read_csv(STG, encoding="utf-8-sig", dtype=str)
    if "ship_kind_cd" not in df.columns:
        print("[FAIL] ship_kind_cd 컬럼이 없어 재계산할 수 없습니다.")
        return 1

    new = recompute(df)

    print("=" * 70)
    print(" PORT-MIS staging 파생 컬럼 재계산")
    print("=" * 70)
    print(f"\n대상 행수: {len(df):,}\n")

    before = df.get("ship_kind_category", pd.Series(dtype=str)).value_counts()
    after = new["ship_kind_category"].value_counts()
    keys = sorted(set(before.index) | set(after.index),
                  key=lambda k: -int(after.get(k, 0)))
    print(f"  {'선종 대분류':<20} {'변경 전':>8} {'변경 후':>8}")
    print("  " + "-" * 40)
    for k in keys:
        b, a = int(before.get(k, 0)), int(after.get(k, 0))
        mark = "  ←" if b != a else ""
        print(f"  {k:<20} {b:>8} {a:>8}{mark}")

    def _truth(s):
        return s.astype(str).str.lower().isin(("true", "1", "t")).sum() if s is not None else 0

    print()
    for col in ("is_liquid_cargo_vessel", "is_liquid_cargo_barge", "is_bunkering_vessel"):
        b = _truth(df[col]) if col in df.columns else 0
        a = int(new[col].sum())
        print(f"  {col:<24} {b:>8} → {a:>8}")

    if not args.apply:
        print("\n※ 미리보기입니다. 실제 반영하려면 --apply 를 붙이세요.")
        return 0

    backup = STG + ".bak"
    shutil.copy2(STG, backup)
    new.to_csv(STG, index=False, encoding="utf-8-sig")
    print(f"\n[적용] {STG}")
    print(f"[백업] {backup}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
