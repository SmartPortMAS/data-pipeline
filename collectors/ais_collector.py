"""
collect_real_ais.py
===================
울산항 실제 AIS 위치/제원 데이터 수집 스크립트 (aisstream.io 기반)

사용법:
    python utils/collect_real_ais.py [--minutes 5]

    --minutes : 수집 시간(분). 기본값 5분.
    Ctrl+C로 조기 종료 가능. 종료 시 그때까지 수집된 데이터를 저장합니다.

출력:
    data/raw/ais/ais_position_<YYYYMMDD>_real.json   (AIS Dynamic 위치)
    data/raw/ais/ais_static_<YYYYMMDD>_real.json     (AIS Static 제원)

의존성:
    pip install websockets
"""

import asyncio
import json
import os
import sys
import datetime
import argparse

# ----- 설정 -----
AISSTREAM_API_KEY = "3460701b4937d1fe1d2e0c4f3562fdd04c9d592b"

# 울산항 인근 해상 광역 Bounding Box (울산 향하는 항해 중인 선박 포함)
# [[남서위도, 남서경도], [북동위도, 북동경도]]
ULSAN_BBOX = [[34.00, 128.00], [36.50, 130.50]]

# 울산 향하는 선박 필터링 키워드 (Destination 필드 기준, 다양한 표기 포함)
# 실제 수집 데이터 기반 검증 완료: KRULS, KRUSN, KR USN, ULSAN, USN
ULSAN_DEST_KEYWORDS = ["ULSAN", "KRULS", "KRUSN", "KR USN", "USN", "KR ULS"]

# 저장 경로
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_AIS_DIR = os.path.join(BASE_DIR, "data", "raw", "ais")


async def collect_ais(duration_minutes: int = 5):
    """울산항 BBox 기준으로 AISStream에서 실시간 AIS 데이터를 수신하여 파일로 저장."""
    try:
        import websockets
    except ImportError:
        print("[ERROR] 'websockets' 패키지가 없습니다. 아래 명령을 먼저 실행하세요:")
        print("        pip install websockets")
        sys.exit(1)

    os.makedirs(RAW_AIS_DIR, exist_ok=True)
    # KST (UTC+9) 기준으로 파일명 생성
    kst_now = datetime.datetime.utcnow() + datetime.timedelta(hours=9)
    today_str = kst_now.strftime("%Y%m%d")

    pos_file_path = os.path.join(RAW_AIS_DIR, f"ais_position_{today_str}_real.json")
    static_file_path = os.path.join(RAW_AIS_DIR, f"ais_static_{today_str}_real.json")

    pos_records = []
    static_records = []
    ulsan_bound_mmsi = set()  # Destination으로 울산 향이 확인된 MMSI 집합

    end_time = datetime.datetime.utcnow() + datetime.timedelta(minutes=duration_minutes)

    uri = "wss://stream.aisstream.io/v0/stream"

    # websockets 연결 옵션: 핑/퐁으로 연결 유지, 10초 닫기 대기
    CONNECT_KWARGS = {
        "ping_interval": 20,
        "ping_timeout": 10,
        "close_timeout": 10,
        "additional_headers": {"User-Agent": "ulsan-ais-collector/1.0"},
    }

    print(f"=== AISStream 울산항 실시간 AIS 수집 시작 ===")
    print(f"  BoundingBox : {ULSAN_BBOX}")
    print(f"  수집 예정 시간 : {duration_minutes}분")
    print(f"  종료 예정 UTC : {end_time.strftime('%H:%M:%S')}")
    print(f"  위치 저장 경로 : {pos_file_path}")
    print(f"  제원 저장 경로 : {static_file_path}")
    print("----------------------------------------------")

    try:
        while datetime.datetime.utcnow() < end_time:
            try:
                print(f"[INFO] wss://stream.aisstream.io/v0/stream 연결 시도 중...")
                async with websockets.connect(uri, **CONNECT_KWARGS) as websocket:
                    subscribe_message = {
                        "APIKey": AISSTREAM_API_KEY,
                        "BoundingBoxes": [ULSAN_BBOX],
                        # PositionReport: 위치/속도/침로/항해상태
                        # ShipStaticData: 선박명/선종/IMO/흘수
                        "FilterMessageTypes": ["PositionReport", "ShipStaticData"]
                    }
                    await websocket.send(json.dumps(subscribe_message))
                    print("[INFO] 구독 요청 전송 완료. 데이터 수신 중...")

                    while datetime.datetime.utcnow() < end_time:
                        try:
                            message_str = await asyncio.wait_for(websocket.recv(), timeout=10.0)
                        except asyncio.TimeoutError:
                            # 데이터가 없는 구간은 건너뜀
                            continue

                        message_json = json.loads(message_str)
                        msg_type = message_json.get("MessageType")
                        meta = message_json.get("MetaData", {})
                        mmsi = meta.get("MMSI")
                        time_utc = meta.get("time_utc")

                        # ── A. 위치 정보 (Dynamic) ──────────────────────────────────────
                        if msg_type == "PositionReport":
                            pos = message_json.get("Message", {}).get("PositionReport", {})
                            record = {
                                "MMSI": str(mmsi),
                                "lat": pos.get("Latitude"),
                                "lot": pos.get("Longitude"),
                                "sog": pos.get("Sog"),        # Speed Over Ground (kn)
                                "cog": pos.get("Cog"),        # Course Over Ground (deg)
                                "nvgtStts": pos.get("NavigationalStatus"),  # 0=UnderWay, 1=Anchored, 5=Moored
                                "TimeUtc": time_utc,
                                "ulsan_bound": str(mmsi) in ulsan_bound_mmsi  # 울산 향 선박 여부
                            }
                            pos_records.append(record)

                            # 진행 상황 실시간 출력
                            elapsed = (end_time - datetime.datetime.utcnow()).seconds // 60
                            ulsan_tag = " [★ ULSAN BOUND]" if record["ulsan_bound"] else ""
                            print(
                                f"[POS] MMSI={mmsi} | "
                                f"Lat={record['lat']:.4f}, Lon={record['lot']:.4f} | "
                                f"SOG={record['sog']}kn | NavStatus={record['nvgtStts']} | "
                                f"남은시간={elapsed}분 | 울산향{len(ulsan_bound_mmsi)}선 | 총{len(pos_records)}건"
                                + ulsan_tag
                            )

                            # 10건마다 중간 저장
                            if len(pos_records) % 10 == 0:
                                _save_json(pos_records, pos_file_path)

                        # ── B. 제원 정보 (Static) ────────────────────────────────────────
                        elif msg_type == "ShipStaticData":
                            static = message_json.get("Message", {}).get("ShipStaticData", {})
                            dim = static.get("Dimension", {})
                            destination = static.get("Destination", "") or ""
                            # 울산 향 여부 판단 (Destination 필드 직접 확인)
                            is_ulsan_bound = any(
                                kw in destination.upper()
                                for kw in ULSAN_DEST_KEYWORDS
                            )
                            record = {
                                "mmsiNo": str(mmsi),
                                "imoNo": str(static.get("ImoNumber", "")),
                                "callsgn": static.get("CallSign", "").strip(),
                                "vslNm": meta.get("ShipName", "").strip(),
                                # 길이 = A(전방거리) + B(후방거리), 너비 = C + D
                                "Length": (dim.get("A", 0) or 0) + (dim.get("B", 0) or 0),
                                "Width": (dim.get("C", 0) or 0) + (dim.get("D", 0) or 0),
                                "ShipType": static.get("ShipType"),       # 80~89: Tanker
                                "Draught": static.get("MaximumStaticDraft"),
                                "Destination": destination.strip(),
                                "ulsan_bound": is_ulsan_bound,
                                "TimeUtc": time_utc
                            }
                            static_records.append(record)
                            # 울산 향 선박 MMSI 세트에 등록
                            if is_ulsan_bound:
                                ulsan_bound_mmsi.add(str(mmsi))
                            ulsan_tag = " [★ ULSAN BOUND]" if is_ulsan_bound else ""
                            print(
                                f"[STATIC] MMSI={mmsi} | "
                                f"Name={record['vslNm']} | "
                                f"Type={record['ShipType']} | "
                                f"Draught={record['Draught']}m | 총{len(static_records)}건"
                            )
                            _save_json(static_records, static_file_path)
            except (websockets.exceptions.ConnectionClosed, asyncio.TimeoutError) as e:
                print(f"[경고] WebSocket 연결이 종료되거나 제한시간이 초과되었습니다: {e}. 3초 후 재연결을 시도합니다.")
                await asyncio.sleep(3)
            except Exception as e:
                print(f"[오류] 연결 중 오류 발생: {type(e).__name__}: {e}. 3초 후 재연결을 시도합니다.")
                await asyncio.sleep(3)
    except KeyboardInterrupt:
        print("\n[INFO] 사용자 종료 요청.")

    # 최종 저장
    _save_json(pos_records, pos_file_path)
    _save_json(static_records, static_file_path)

    print("\n=== 수집 완료 ===")
    print(f"  AIS 위치 레코드 : {len(pos_records)}건  →  {pos_file_path}")
    print(f"  AIS 제원 레코드 : {len(static_records)}건  →  {static_file_path}")


def _save_json(records: list, path: str):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=4, ensure_ascii=False, default=str)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="울산항 실시간 AIS 수집기")
    parser.add_argument("--minutes", type=int, default=5, help="수집 시간(분). 기본값: 5")
    args = parser.parse_args()

    asyncio.run(collect_ais(duration_minutes=args.minutes))
