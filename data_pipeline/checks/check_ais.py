import json

# Static 데이터에서 ulsan_bound 선박 확인
with open('data/raw/ais/ais_static_20260618_real.json', encoding='utf-8') as f:
    static = json.load(f)

ulsan_ships = [s for s in static if s.get('ulsan_bound')]
print("=== 수집 결과 요약 ===")
print(f"전체 Static 레코드: {len(static)}건")
print(f"울산 향 선박 (ulsan_bound): {len(ulsan_ships)}건")
print()
if ulsan_ships:
    print("[★ ULSAN BOUND 선박 목록]")
    for s in ulsan_ships:
        print(f"  MMSI={s['mmsiNo']} | {s['vslNm']} | Dest={s['Destination']} | Type={s['ShipType']}")
else:
    all_dest = [(s['mmsiNo'], s['vslNm'], s.get('Destination','')) for s in static]
    print("[Destination 샘플 (처음 10건)]")
    for row in all_dest[:10]:
        print(f"  MMSI={row[0]} | {row[1]} | Dest={row[2]!r}")

print()
# Position 데이터 ulsan_bound 통계
with open('data/raw/ais/ais_position_20260618_real.json', encoding='utf-8') as f:
    pos = json.load(f)

ulsan_pos = [p for p in pos if p.get('ulsan_bound')]
print(f"전체 Position 레코드: {len(pos)}건")
print(f"울산 향 Position 레코드: {len(ulsan_pos)}건")
