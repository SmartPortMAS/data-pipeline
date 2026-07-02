import os
import json

def main():
    # 저장 경로 설정
    raw_dir = r"C:\Users\user\.gemini\antigravity-ide\scratch\ulsan_port_control\data\raw\ais"
    os.makedirs(raw_dir, exist_ok=True)
    
    # 1. 가상 AIS 위치 데이터 (Dynamic)
    # 정상 범위: Lat 35.30 ~ 35.58, Lon 129.18 ~ 129.52
    position_data = [
        # (1) 정상 데이터 1: 울산항 내, 정상 속도
        {
            "MMSI": "440123456",
            "lat": 35.45,
            "lot": 129.35,
            "sog": 12.5,
            "cog": 180.0,
            "nvgtStts": 0,  # Under Way
            "TimeUtc": "2026-06-01T08:00:00Z"
        },
        # (2) 정상 데이터 2: 울산항 내, 정박 중 (낮은 속도)
        {
            "MMSI": "440987654",
            "lat": 35.38,
            "lot": 129.42,
            "sog": 0.1,
            "cog": 90.0,
            "nvgtStts": 1,  # Anchored
            "TimeUtc": "2026-06-01T08:05:00Z"
        },
        # (3) 울산항 영역 외 데이터 (OUT_OF_ULSAN_BBOX)
        {
            "MMSI": "440111222",
            "lat": 34.90,  # 범위 미달
            "lot": 128.50,  # 범위 미달
            "sog": 15.0,
            "cog": 270.0,
            "nvgtStts": 0,
            "TimeUtc": "2026-06-01T08:10:00Z"
        },
        # (4) 속도 오류 데이터 (INVALID_SPEED)
        {
            "MMSI": "440333444",
            "lat": 35.50,
            "lot": 129.30,
            "sog": -5.0,  # 음수 속도
            "cog": 45.0,
            "nvgtStts": 0,
            "TimeUtc": "2026-06-01T08:15:00Z"
        },
        # (5) 침로 오류 데이터 (INVALID_COURSE)
        {
            "MMSI": "440555666",
            "lat": 35.52,
            "lot": 129.28,
            "sog": 8.0,
            "cog": 400.0,  # 360 초과
            "nvgtStts": 0,
            "TimeUtc": "2026-06-01T08:20:00Z"
        },
        # (6) 기본키 결측 데이터 (MISSING_KEY)
        {
            "MMSI": "None",  # 결측 표현
            "lat": 35.40,
            "lot": 129.30,
            "sog": 10.0,
            "cog": 120.0,
            "nvgtStts": 0,
            "TimeUtc": "2026-06-01T08:25:00Z"
        },
        # (7) 좌표 결측 데이터 (MISSING_COORDINATE - nan)
        {
            "MMSI": "440777888",
            "lat": "NaN",
            "lot": 129.30,
            "sog": 10.0,
            "cog": 120.0,
            "nvgtStts": 0,
            "TimeUtc": "2026-06-01T08:30:00Z"
        },
        # (8) 좌표 오류 데이터 (MISSING_COORDINATE - 0.0)
        {
            "MMSI": "440999000",
            "lat": 0.0,
            "lot": 0.0,
            "sog": 5.0,
            "cog": 150.0,
            "nvgtStts": 0,
            "TimeUtc": "2026-06-01T08:35:00Z"
        }
    ]

    # 2. 가상 AIS 선박 제원 데이터 (Static)
    # ship_type: 80-89는 Tanker (액체화물선), 70-79는 Cargo (일반화물선), 60-69는 Passenger 등
    static_data = [
        # (1) 유류운반선 (Tanker) - 전처리 대상 포함
        {
            "mmsiNo": "440123456",
            "imoNo": "9123456",
            "callsgn": "D7AAA",
            "vslNm": "ULSAN OILSTAR",
            "ShipType": 80,  # Tanker (인화성 액체)
            "Length": 180,
            "Width": 32,
            "Draught": 10.5,
            "TimeUtc": "2026-06-01T00:00:00Z"
        },
        # (2) 케미컬운반선 (Tanker) - 전처리 대상 포함
        {
            "mmsiNo": "440987654",
            "imoNo": "9987654",
            "callsgn": "D7BBB",
            "vslNm": "PACIFIC CHEM",
            "ShipType": 84,  # Tanker (유해 액체화학물질)
            "Length": 120,
            "Width": 20,
            "Draught": 7.8,
            "TimeUtc": "2026-06-01T00:05:00Z"
        },
        # (3) 일반화물선 (Cargo) - 필터링에서 제외되거나 속성 분리
        {
            "mmsiNo": "440111222",
            "imoNo": "9111222",
            "callsgn": "D7CCC",
            "vslNm": "HANJIN BULK",
            "ShipType": 70,  # Cargo (일반 벌크)
            "Length": 220,
            "Width": 32,
            "Draught": 12.0,
            "TimeUtc": "2026-06-01T00:10:00Z"
        },
        # (4) 여객선 (Passenger) - 필터링에서 제외
        {
            "mmsiNo": "440333444",
            "imoNo": "9333444",
            "callsgn": "D7DDD",
            "vslNm": "ULSAN FERRY",
            "ShipType": 60,  # Passenger
            "Length": 90,
            "Width": 15,
            "Draught": 4.5,
            "TimeUtc": "2026-06-01T00:15:00Z"
        },
        # (5) 기본키 결측 제원 데이터
        {
            "mmsiNo": "None",
            "imoNo": "9555666",
            "callsgn": "D7EEE",
            "vslNm": "MISSING MMSI BOAT",
            "ShipType": 81,
            "Length": 100,
            "Width": 18,
            "Draught": 6.0,
            "TimeUtc": "2026-06-01T00:20:00Z"
        }
    ]

    # JSON 파일 저장
    pos_path = os.path.join(raw_dir, "ais_position_20260601_raw.json")
    with open(pos_path, "w", encoding="utf-8") as f:
        json.dump(position_data, f, indent=4, ensure_ascii=False)
        
    static_path = os.path.join(raw_dir, "ais_static_20260601_raw.json")
    with open(static_path, "w", encoding="utf-8") as f:
        json.dump(static_data, f, indent=4, ensure_ascii=False)

    print(f"가상 raw 데이터 생성 완료:")
    print(f" - {pos_path}")
    print(f" - {static_path}")

if __name__ == "__main__":
    main()
