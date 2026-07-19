# -*- coding: utf-8 -*-
"""
울산항 bbox 조정 검증 스크립트 (PR 근거 자동 생성)

무엇을 하나:
  data/staging 의 좌표 staging CSV들을 읽어, "현재 bbox"와 "제안 bbox" 각각의
  커버리지를 비교 출력한다. PR 설명에 붙일 근거 표가 그대로 나온다.

제안 근거 (2026-07-12, 실측 517건 분석):
  - 현재 bbox(35.30~35.58/129.18~129.52)는 부두·정박지 100% 커버하지만
    UPA 항내 선박위치의 27.5%를 OUT_OF_ULSAN_BBOX 로 오분류
    (실제 선박은 북쪽 35.85 정자 앞바다, 동쪽 129.77 외해 접근수역까지 분포)
  - 제안 bbox는 부두·정박지 100% + 선박위치 98.6% 커버
    (남는 1.4%는 방어진 외해 초입 — 진짜 범위 밖이므로 플래그 정상 동작)
  - AIS 의 OUT_OF_BBOX 다발은 수집박스가 부산 포함 광역이라 정상 동작임 (별개 사안)

실행 (data-pipeline 레포 루트에서, 전처리 실행 후):
  python -m data_pipeline.checks.check_bbox_coverage
"""
import glob
import os

import pandas as pd

# 현재 값 (common_preprocessing.py 의 기존 상수)
OLD = {"lat_min": 35.30, "lat_max": 35.58, "lon_min": 129.18, "lon_max": 129.52}
# 제안 값 (실측 99% 커버 기준, 0.01도 라운딩)
NEW = {"lat_min": 35.18, "lat_max": 35.82, "lon_min": 129.22, "lon_max": 129.76}

STAGING_DIR = "data/staging"
# 좌표가 있는 staging (있는 것만 검사)
TARGETS = [
    "upa_vessel_position_stg.csv",
    "upa_berth_facility_stg.csv",
    "upa_anchorage_stg.csv",
    "ais_vessel_position_stg.csv",
]


def coverage(lat: pd.Series, lon: pd.Series, box: dict) -> float:
    inbox = lat.between(box["lat_min"], box["lat_max"]) & lon.between(box["lon_min"], box["lon_max"])
    return inbox.mean() * 100


def main() -> None:
    print("=" * 72)
    print("울산항 bbox 조정 검증 — 현재 vs 제안")
    print(f"  현재: lat {OLD['lat_min']}~{OLD['lat_max']} / lon {OLD['lon_min']}~{OLD['lon_max']}")
    print(f"  제안: lat {NEW['lat_min']}~{NEW['lat_max']} / lon {NEW['lon_min']}~{NEW['lon_max']}")
    print("=" * 72)

    found = False
    for fname in TARGETS:
        path = os.path.join(STAGING_DIR, fname)
        if not os.path.exists(path):
            continue
        df = pd.read_csv(path)
        if "latitude" not in df.columns or "longitude" not in df.columns:
            continue
        lat = pd.to_numeric(df["latitude"], errors="coerce")
        lon = pd.to_numeric(df["longitude"], errors="coerce")
        ok = lat.notna() & lon.notna() & (lat != 0) & (lon != 0)
        lat, lon = lat[ok], lon[ok]
        if lat.empty:
            continue
        found = True
        old_c, new_c = coverage(lat, lon, OLD), coverage(lat, lon, NEW)
        diff = new_c - old_c
        print(f"\n[{fname}] 유효좌표 {len(lat)}건")
        print(f"  실측 범위: lat {lat.min():.4f}~{lat.max():.4f} / lon {lon.min():.4f}~{lon.max():.4f}")
        print(f"  커버리지:  현재 {old_c:5.1f}%  →  제안 {new_c:5.1f}%  ({'+' if diff >= 0 else ''}{diff:.1f}%p)")

    if not found:
        print(f"\n{STAGING_DIR} 에 좌표 staging 이 없습니다. 먼저 전처리를 실행하세요.")
        return

    print("\n" + "=" * 72)
    print("판정 가이드: 부두·정박지 100% 유지 + 선박위치 커버리지 대폭 상승이면 제안 채택")
    print("주의: AIS는 수집박스(부산 포함 광역) 특성상 제안 bbox 밖도 정상입니다.")
    print("=" * 72)


if __name__ == "__main__":
    main()
