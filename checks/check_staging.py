import pandas as pd

df_pos = pd.read_csv('data/staging/ais_vessel_position_stg.csv', encoding='utf-8-sig')
df_static = pd.read_csv('data/staging/ais_vessel_static_stg.csv', encoding='utf-8-sig')

print("=== AIS 위치 데이터 (ais_vessel_position_stg.csv) ===")
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
print("=== AIS 제원 데이터 (ais_vessel_static_stg.csv) ===")
print(f"총 레코드: {len(df_static)}건")
if 'Destination' in df_static.columns:
    dest_not_null = df_static[df_static['Destination'].notna() & (df_static['Destination'] != '')]
    print(f"Destination 있는 선박: {len(dest_not_null)}건")
if 'ulsan_bound' in df_static.columns:
    ulsan = df_static[df_static['ulsan_bound'].astype(str).str.lower().eq('true')]
    print(f"울산 향 (ulsan_bound=True): {len(ulsan)}건")
    if len(ulsan) > 0:
        print(ulsan[['mmsi', 'vessel_name', 'Destination']].to_string(index=False))
