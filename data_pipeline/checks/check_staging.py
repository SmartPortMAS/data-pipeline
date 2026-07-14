# -*- coding: utf-8 -*-
"""
staging 산출물 검증 스크립트

선박 위치(UPA 항내 선박위치 — 기본 / 레거시 AIS — 폴백)와 PORT-MIS staging 의
품질 분포를 출력한다. 핵심 확인 항목:
  - 위치 데이터의 quality_flag 분포 (bbox 오탐 여부)
  - 위치 ↔ PORT-MIS callsgn 조인 성공률 및 액체화물선(Tanker) 식별 수
    (구 aisstream 경로에서 ship_type 전부 NaN → Tanker 0척이던 문제의 검증 지점)

실행: python -m data_pipeline.checks.check_staging
"""
import os

import pandas as pd

STAGING = 'data/staging'


def _exists(name: str) -> bool:
    return os.path.exists(os.path.join(STAGING, name))


def _read(name: str) -> pd.DataFrame:
    return pd.read_csv(os.path.join(STAGING, name), encoding='utf-8-sig')


df_portmis = _read('portmis_vessel_stg.csv') if _exists('portmis_vessel_stg.csv') else None

# ---------------------------------------------------------------------------
# 1. 선박 위치 — UPA 항내 선박위치 (기본 소스)
# ---------------------------------------------------------------------------
if _exists('upa_vessel_position_stg.csv'):
    df_upa = _read('upa_vessel_position_stg.csv')
    print("=== 선박 위치 데이터 (upa_vessel_position_stg.csv / UPA getVslPstnInfo) ===")
    print(f"총 레코드: {len(df_upa)}건")
    if 'is_synthetic' in df_upa.columns:
        real = df_upa[~df_upa['is_synthetic'].astype(str).str.lower().eq('true')]
        print(f"실제 수집 데이터: {len(real)}건 (가상 제외)")
    print("quality_flag 분포:")
    print(df_upa['quality_flag'].value_counts().to_string())
    print()
    for key in ('callsgn', 'mmsi', 'imo_no'):
        if key in df_upa.columns:
            missing = df_upa[key].isna().sum()
            print(f"{key} 결측: {missing}건 / {len(df_upa)}건")
    # 핵심 검증: PORT-MIS 조인으로 선종(액체화물선) 식별
    if df_portmis is not None and 'callsgn' in df_upa.columns:
        upa_cs = df_upa['callsgn'].astype(str).str.strip().str.upper()
        pm = df_portmis.copy()
        pm['callsgn_clean'] = pm['callsgn'].astype(str).str.strip().str.upper()
        pm = pm.drop_duplicates(subset=['callsgn_clean'], keep='last')
        joined = pd.DataFrame({'callsgn_clean': upa_cs}).merge(pm, on='callsgn_clean', how='left')
        matched = joined['ship_kind_nm'].notna().sum()
        print()
        print(f"PORT-MIS 조인 성공: {matched}건 / {len(df_upa)}건 "
              f"({matched / max(len(df_upa), 1) * 100:.0f}%)")
        if 'is_liquid_cargo_vessel' in joined.columns:
            liquid = joined['is_liquid_cargo_vessel'].astype(str).str.lower().eq('true').sum()
            print(f"액체화물선(Tanker) 식별: {liquid}건  ← 구 AIS ship_type NaN 문제 검증 지점")
    print()
else:
    print("[SKIP] upa_vessel_position_stg.csv 없음 — `run_pipeline vessel` 먼저 실행")
    print()

# ---------------------------------------------------------------------------
# 2. 선박 위치 — 레거시 AIS (aisstream, 폴백/비교용)
# ---------------------------------------------------------------------------
if _exists('ais_vessel_position_stg.csv'):
    df_pos = _read('ais_vessel_position_stg.csv')
    print("=== [레거시] AIS 위치 데이터 (ais_vessel_position_stg.csv) ===")
    print(f"총 레코드: {len(df_pos)}건")
    real = df_pos[~df_pos['is_synthetic'].astype(str).str.lower().eq('true')]
    print(f"실제 수집 데이터: {len(real[real['quality_flag'] != 'MISSING_KEY'])}건 (샘플 제외)")
    print("quality_flag 분포:")
    print(df_pos['quality_flag'].value_counts().to_string())
    print()
    print("ulsan_bound 태그 여부 (컬럼 존재시):")
    if 'ulsan_bound' in df_pos.columns:
        print(df_pos['ulsan_bound'].value_counts().to_string())
    else:
        print("  ulsan_bound 컬럼 없음")
    print()

if _exists('ais_vessel_static_stg.csv'):
    df_static = _read('ais_vessel_static_stg.csv')
    print("=== [레거시] AIS 제원 데이터 (ais_vessel_static_stg.csv) ===")
    print(f"총 레코드: {len(df_static)}건")
    if 'Destination' in df_static.columns:
        dest_not_null = df_static[df_static['Destination'].notna() & (df_static['Destination'] != '')]
        print(f"Destination 있는 선박: {len(dest_not_null)}건")
    if 'ulsan_bound' in df_static.columns:
        ulsan = df_static[df_static['ulsan_bound'].astype(str).str.lower().eq('true')]
        print(f"울산 향 (ulsan_bound=True): {len(ulsan)}건")
        if len(ulsan) > 0:
            print(ulsan[['mmsi', 'vessel_name', 'Destination']].to_string(index=False))
    print()

# ---------------------------------------------------------------------------
# 3. PORT-MIS 입출항
# ---------------------------------------------------------------------------
if df_portmis is not None:
    print("=== PORT-MIS 입출항 데이터 (portmis_vessel_stg.csv) ===")
    print(f"총 레코드: {len(df_portmis)}건")
    print("quality_flag 분포:")
    print(df_portmis['quality_flag'].value_counts().to_string())
    print()
    print(f"호출부호(callsgn) 결측 (MISSING_KEY): {(df_portmis['quality_flag'] == 'MISSING_KEY').sum()}건")
    print()
    print("항만청(port_agency_label)별 건수:")
    if 'port_agency_label' in df_portmis.columns:
        print(df_portmis['port_agency_label'].value_counts().to_string())
    else:
        print("  port_agency_label 컬럼 없음")
    if 'is_liquid_cargo_vessel' in df_portmis.columns:
        liquid = df_portmis[df_portmis['is_liquid_cargo_vessel'].astype(str).str.lower().eq('true')]
        print(f"액체화물선 (is_liquid_cargo_vessel=True): {len(liquid)}건")
else:
    print("[SKIP] portmis_vessel_stg.csv 없음")
