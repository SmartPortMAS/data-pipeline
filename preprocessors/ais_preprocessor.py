import os
import sys
import json
import pandas as pd
import numpy as np

# common_utils.py가 위치한 src 폴더를 path에 추가하여 모듈을 가져올 수 있도록 설정
src_dir = os.path.dirname(os.path.abspath(__file__))
if src_dir not in sys.path:
    sys.path.append(src_dir)

import common_utils as cu

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STAGING_DIR = os.path.join(BASE_DIR, "data", "staging")

def preprocess_ais_position():
    print("1. AIS 위치 데이터 (Dynamic) 전처리 시작...")
    output_dir = STAGING_DIR
    
    # 1. raw 데이터 로드 (모든 ais_position_*.json 파일 탐색 및 병합)
    import glob
    raw_ais_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "raw", "ais")
    raw_files = sorted(glob.glob(os.path.join(raw_ais_dir, "ais_position_*.json")), reverse=True)
    if not raw_files:
        print(f"처리할 AIS 위치 파일을 찾을 수 없습니다.")
        return
        
    all_dfs = []
    for raw_path in raw_files:
        try:
            with open(raw_path, encoding="utf-8") as f:
                data = json.load(f)
            if not data:
                continue
            df = pd.DataFrame(data)
            all_dfs.append(df)
            print(f" - 로드: {os.path.basename(raw_path)} ({len(df)}건)")
        except Exception as e:
            print(f" - [오류] {os.path.basename(raw_path)} 로드 실패: {e}")
            
    if not all_dfs:
        print("[오류] 유효한 AIS 위치 데이터가 없습니다.")
        return
        
    df = pd.concat(all_dfs, ignore_index=True)
    print(f" - 원본 총 행 개수: {len(df)}")
    
    # 2. 원본 컬럼명 → 표준 컬럼명 매핑
    column_map = {
        "MMSI": "mmsi",
        "lat": "latitude",
        "lot": "longitude",
        "sog": "sog",
        "cog": "cog",
        "nvgtStts": "nav_status_code",
        "TimeUtc": "received_at_utc"
    }
    
    # 3. 공통 전처리 적용
    df = cu.standardize_column_names(df, column_map)
    df = cu.normalize_nulls(df)
    
    # 공통 메타데이터 추가
    df = cu.add_common_metadata(
        df,
        source_system="AISSTREAM",
        source_table="PositionReport",
        is_synthetic=False
    )
    
    # 숫자형 변환
    df = cu.to_numeric_safe(df, ["mmsi", "latitude", "longitude", "sog", "cog", "nav_status_code"])
    
    # 시간 파싱 (AIS는 이미 UTC이므로 parse_datetime_utc 적용)
    df = cu.parse_datetime_utc(df, ["received_at_utc"])
    
    # 기본키 결측 검증 (MMSI와 수신시각이 필수 키)
    df = cu.flag_missing_key(df, ["mmsi", "received_at_utc"])
    
    # 울산항 범위 검증
    df = cu.flag_ulsan_bbox(df)
    
    # 속도 및 침로 검증
    df = cu.validate_speed_course(df)
    
    # 4. AIS 특화 전처리: 상태 판단 (정박/접안/운항)
    # (sog가 0.5 미만이고 nav_status_code가 1(Anchored)이거나, 혹은 단순 속도로 판단)
    df["nav_status_category"] = "UNKNOWN"
    
    # 조건 설정
    is_anchored = (df["nav_status_code"] == 1) | ((df["sog"] < 0.5) & (df["nav_status_code"] != 5))
    is_moored = (df["nav_status_code"] == 5) | ((df["sog"] < 0.2) & (~is_anchored))
    is_underway = df["sog"] >= 0.5
    
    df.loc[is_anchored, "nav_status_category"] = "ANCHORED"
    df.loc[is_moored, "nav_status_category"] = "MOORED"
    df.loc[is_underway, "nav_status_category"] = "UNDER_WAY"
    
    # 결측이 있는 경우 UNKNOWN 유지
    df.loc[df["sog"].isna(), "nav_status_category"] = "UNKNOWN"

    # 4-2. 선박 제원(static) 전처리 결과를 바탕으로 위치 데이터의 ulsan_bound 동적 갱신
    static_path = os.path.join(output_dir, "ais_vessel_static_stg.csv")
    ulsan_bound_mmsis = set()
    if os.path.exists(static_path):
        try:
            df_st = pd.read_csv(static_path, encoding="utf-8-sig")
            ulsan_mmsi_series = df_st[df_st["ulsan_bound"].astype(str).str.lower().eq("true")]["mmsi"]
            ulsan_bound_mmsis = set(ulsan_mmsi_series.dropna().astype(float).astype(int).astype(str))
            print(f" - static 데이터 기반 울산 향 MMSI {len(ulsan_bound_mmsis)}개 확인")
        except Exception as e:
            print(f" - [경고] static 전처리 결과 로드 오류: {e}")

    # MMSI를 안전하게 int -> str 형변환하여 ulsan_bound 여부 매핑
    df["mmsi_str"] = df["mmsi"].fillna(-1).astype(float).astype(int).astype(str)
    df["ulsan_bound"] = df["mmsi_str"].isin(ulsan_bound_mmsis)
    df = df.drop(columns=["mmsi_str"])

    # 5. staging 파일 저장
    os.makedirs(output_dir, exist_ok=True)
    
    output_path = os.path.join(output_dir, "ais_vessel_position_stg.csv")
    cu.save_staging_csv(df, output_path)
    print(f" - Staging 파일 저장 완료: {output_path}")
    print(df[["mmsi", "latitude", "longitude", "sog", "quality_flag", "nav_status_category"]].head(10))
    print("-" * 50)


def preprocess_ais_static():
    print("2. AIS 선박 제원 데이터 (Static) 전처리 시작...")
    output_dir = STAGING_DIR
    
    # 1. raw 데이터 로드 (모든 ais_static_*.json 파일 탐색 및 병합)
    import glob
    raw_ais_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "raw", "ais")
    raw_files = sorted(glob.glob(os.path.join(raw_ais_dir, "ais_static_*.json")), reverse=True)
    if not raw_files:
        print(f"처리할 AIS 제원 파일을 찾을 수 없습니다.")
        return
        
    all_dfs = []
    for raw_path in raw_files:
        try:
            with open(raw_path, encoding="utf-8") as f:
                data = json.load(f)
            if not data:
                continue
            df = pd.DataFrame(data)
            all_dfs.append(df)
            print(f" - 로드: {os.path.basename(raw_path)} ({len(df)}건)")
        except Exception as e:
            print(f" - [오류] {os.path.basename(raw_path)} 로드 실패: {e}")
            
    if not all_dfs:
        print("[오류] 유효한 AIS 제원 데이터가 없습니다.")
        return
        
    df = pd.concat(all_dfs, ignore_index=True)
    print(f" - 원본 총 행 개수: {len(df)}")
    
    # 2. 원본 컬럼명 → 표준 컬럼명 매핑
    column_map = {
        "mmsiNo": "mmsi",
        "imoNo": "imo_no",
        "callsgn": "callsgn",
        "vslNm": "vessel_name",
        "ShipType": "ship_type",
        "Length": "length",
        "Width": "width",
        "Draught": "draught",
        "TimeUtc": "collected_at_utc"
    }
    
    # 3. 공통 전처리 적용
    df = cu.standardize_column_names(df, column_map)
    df = cu.normalize_nulls(df)
    
    # 공통 메타데이터 추가
    df = cu.add_common_metadata(
        df,
        source_system="AISSTREAM",
        source_table="VesselStaticDraft",
        is_synthetic=False
    )
    
    # 숫자형 변환
    df = cu.to_numeric_safe(df, ["mmsi", "imo_no", "ship_type", "length", "width", "draught"])
    
    # 시간 파싱
    df = cu.parse_datetime_utc(df, ["collected_at_utc"])
    
    # 기본키 결측 검증 (MMSI 가 필수 키)
    df = cu.flag_missing_key(df, ["mmsi"])
    
    # 흘수 검증 (흘수가 음수이거나 비정상적으로 큰 값 30m 초과 시 INVALID_DRAUGHT 표시)
    if "draught" in df.columns:
        invalid_draught = (df["draught"] < 0) | (df["draught"] > 30)
        df.loc[invalid_draught & df["draught"].notna(), "quality_flag"] = "INVALID_DRAUGHT"
        
    # 4. AIS 특화 전처리: 선종 필터링 및 액체화물선 여부 라벨링
    # AIS 선종 코드 80-89: Tanker (액체화물선/유조선 등)
    # 80: Tanker, 81: Tanker - Hazardous category A, 등등
    df["is_liquid_cargo_vessel"] = df["ship_type"].between(80, 89)
    print(f" - 액체화물선(Tanker) 개수: {df['is_liquid_cargo_vessel'].sum()} / {len(df)}")
    
    # PORT-MIS 호출부호 로드하여 울산행 판별에 활용
    portmis_path = os.path.join(output_dir, "portmis_vessel_stg.csv")
    portmis_callsigns = set()
    if os.path.exists(portmis_path):
        try:
            df_pm = pd.read_csv(portmis_path, encoding="utf-8-sig")
            portmis_callsigns = set(df_pm["callsgn"].dropna().astype(str).str.strip().str.upper())
            print(f" - PORT-MIS 호출부호 {len(portmis_callsigns)}개 로드 완료 (울산 대조용)")
        except Exception as e:
            print(f" - [경고] PORT-MIS 데이터 로드 중 오류: {e}")

    # ulsan_bound 플래그 재판단 (Destination 텍스트 + PORT-MIS 호출부호 매칭)
    ulsan_keywords = ["ULSAN", "KRULS", "KRUSN", "KR USN", "USN", "KR ULS"]
    dest_ulsan = df["Destination"].fillna("").astype(str).str.upper().apply(
        lambda x: any(kw in x for kw in ulsan_keywords)
    )
    call_ulsan = df["callsgn"].fillna("").astype(str).str.strip().str.upper().isin(portmis_callsigns)
    
    # 둘 중 하나라도 만족하면 ulsan_bound = True
    df["ulsan_bound"] = dest_ulsan | call_ulsan
    print(f" - 울산 향(ulsan_bound) 최종 판단: {df['ulsan_bound'].sum()}척 (Destination 기준: {dest_ulsan.sum()}척, PORT-MIS 호출부호 매칭 기준: {call_ulsan.sum()}척)")
    
    # 5. staging 파일 저장
    os.makedirs(output_dir, exist_ok=True)
    
    output_path = os.path.join(output_dir, "ais_vessel_static_stg.csv")
    cu.save_staging_csv(df, output_path)
    print(f" - Staging 파일 저장 완료: {output_path}")
    print(df[["mmsi", "imo_no", "vessel_name", "ship_type", "is_liquid_cargo_vessel", "quality_flag"]].head(10))
    print("-" * 50)


def main():
    # static 전처리 결과를 position 전처리에서 참조하므로 호출 순서를 정렬
    preprocess_ais_static()
    preprocess_ais_position()
    print("전체 AIS 데이터 전처리 완료!")

if __name__ == "__main__":
    main()
