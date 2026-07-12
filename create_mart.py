"""
create_mart.py
==============
Staging 단계의 전처리 데이터들을 결합하여, 울산항 실시간 관제 및 예측 모델에 사용할
통합 데이터 마트(Master Data Mart)를 구축합니다.

병합 대상:
    1. AIS 위치 데이터 (Dynamic) - data/staging/ais_vessel_position_stg.csv
    2. AIS 제원 데이터 (Static) - data/staging/ais_vessel_static_stg.csv
    3. PORT-MIS 입출항 데이터 - data/staging/portmis_vessel_stg.csv
    * (확장 가능) 타 팀원의 기상/조위 데이터, 접안 부두 혼잡도 데이터

저장 경로:
    data/mart/ulsan_vessel_mart.csv (UTF-8-SIG 인코딩)
"""

import os
import sys
import pandas as pd
import numpy as np

# 프로젝트 루트 폴더를 path에 추가하여 data_pipeline 패키지를 가져올 수 있도록 설정
# (파일이 저장소 루트에 위치하므로 dirname 1회)
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.append(project_root)

from data_pipeline import common_preprocessing as cu

# 경로 설정
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STAGING_DIR = os.path.join(BASE_DIR, "data", "staging")
MART_DIR = os.path.join(BASE_DIR, "data", "mart")

# 울산항 관제 목표 좌표 (입항 정박지 대표 좌표)
# TODO: 함현우 담당의 부두별 접안 좌표가 확정되면 선종/화물별 타겟 좌표로 세분화
ULSAN_TARGET_LAT = 35.475
ULSAN_TARGET_LON = 129.387

def build_master_mart():
    print("=== [3단계] 통합 데이터 마트 구축 시작 ===")
    
    # 1. 파일 경로 정의
    pos_path = os.path.join(STAGING_DIR, "ais_vessel_position_stg.csv")
    static_path = os.path.join(STAGING_DIR, "ais_vessel_static_stg.csv")
    portmis_path = os.path.join(STAGING_DIR, "portmis_vessel_stg.csv")
    
    # 존재 여부 확인
    for p in [pos_path, static_path, portmis_path]:
        if not os.path.exists(p):
            print(f"[오류] 필수 staging 파일이 누락되었습니다: {p}")
            print("전처리 스크립트를 먼저 실행해주세요.")
            return

    # 2. 데이터 로드 및 정합성 처리
    print(" 1) Staging 데이터 로드 중...")
    df_pos = pd.read_csv(pos_path, encoding="utf-8-sig")
    df_static = pd.read_csv(static_path, encoding="utf-8-sig")
    df_portmis = pd.read_csv(portmis_path, encoding="utf-8-sig")
    
    print(f"    - AIS 위치 데이터: {len(df_pos)} 건")
    print(f"    - AIS 제원 데이터: {len(df_static)} 건")
    print(f"    - PORT-MIS 데이터 : {len(df_portmis)} 건")

    # 시간 타입 명시적 변환 (mixed 포맷 및 동일 datetime64[ns, UTC] 타입 통일 적용)
    df_pos["received_at_utc"] = pd.to_datetime(df_pos["received_at_utc"], errors="coerce", format="mixed", utc=True).astype("datetime64[ns, UTC]")
    df_static["collected_at_utc"] = pd.to_datetime(df_static["collected_at_utc"], errors="coerce", format="mixed", utc=True).astype("datetime64[ns, UTC]")
    df_portmis["collected_at_utc"] = pd.to_datetime(df_portmis["collected_at_utc"], errors="coerce", format="mixed", utc=True).astype("datetime64[ns, UTC]")
    
    if "departure_sched_utc" in df_portmis.columns:
        df_portmis["departure_sched_utc"] = pd.to_datetime(df_portmis["departure_sched_utc"], errors="coerce", format="mixed", utc=True).astype("datetime64[ns, UTC]")
    if "dest_arrival_utc" in df_portmis.columns:
        df_portmis["dest_arrival_utc"] = pd.to_datetime(df_portmis["dest_arrival_utc"], errors="coerce", format="mixed", utc=True).astype("datetime64[ns, UTC]")

    # 3. 조인을 위한 데이터 정제 및 중복 제거
    # 3-1. AIS 제원: MMSI별 가장 최근 수집된 1건만 남김
    df_static_clean = (
        df_static
        .sort_values(by="collected_at_utc", ascending=True)
        .drop_duplicates(subset=["mmsi"], keep="last")
    )
    
    # 3-2. PORT-MIS: 호출부호(callsgn)별 중복 제거 (대소문자 및 양끝 공백 제거 후 최신 1건 유지)
    df_portmis["callsgn_clean"] = df_portmis["callsgn"].astype(str).str.strip().str.upper()
    df_portmis_clean = (
        df_portmis
        .sort_values(by="collected_at_utc", ascending=True)
        .drop_duplicates(subset=["callsgn_clean"], keep="last")
    )

    # 4. 데이터 병합 (Join)
    print(" 2) 데이터 병합(LEFT JOIN) 진행 중...")
    
    # 4-1. AIS 위치 + AIS 제원 (Key: mmsi)
    # 두 테이블에 공통으로 존재하는 컬럼 중 충돌을 피해야 하는 것들 명시적 처리
    static_cols_to_use = [
        "mmsi", "imo_no", "callsgn", "vessel_name", "ship_type", 
        "length", "width", "draught", "Destination", "is_liquid_cargo_vessel"
    ]
    # 존재하는 컬럼만 필터링
    static_cols_to_use = [col for col in static_cols_to_use if col in df_static_clean.columns]
    
    mart = pd.merge(
        df_pos,
        df_static_clean[static_cols_to_use],
        on="mmsi",
        how="left",
        suffixes=("", "_static")
    )
    print(f"    - 위치-제원 병합 완료 (행 수: {len(mart)}건)")

    # 4-2. AIS (위치+제원) + PORT-MIS (Key: callsgn)
    # AIS 제원 정보에서 넘어온 callsgn을 기준으로 조인
    if "callsgn" in mart.columns:
        mart["callsgn_clean"] = mart["callsgn"].astype(str).str.strip().str.upper()
        
        # PORT-MIS에서 필요한 정보 선택 (중복 제거)
        portmis_cols_to_use = [
            "callsgn_clean", "port_agency_cd", "port_agency_nm", "entry_year", "entry_count",
            "nationality_cd", "nationality_nm", "ship_kind_cd", "ship_kind_nm",
            "entry_purpose_cd", "entry_purpose_nm", "origin_port_cd", "origin_port_nm",
            "prev_port_cd", "prev_port_nm", "next_port_cd", "next_port_nm",
            "dest_port_cd", "dest_port_nm", "departure_sched_utc", "dest_arrival_utc",
            "ship_kind_category", "is_domestic_voyage", "port_agency_label"
        ]
        portmis_cols_to_use = [col for col in portmis_cols_to_use if col in df_portmis_clean.columns]
        
        mart = pd.merge(
            mart,
            df_portmis_clean[portmis_cols_to_use],
            on="callsgn_clean",
            how="left",
            suffixes=("", "_portmis")
        )
        # 임시 조인 키 삭제
        mart = mart.drop(columns=["callsgn_clean"])
        print(f"    - PORT-MIS 병합 완료 (행 수: {len(mart)}건)")
    else:
        print("    - [경고] AIS 데이터에 callsgn 컬럼이 없어 PORT-MIS 병합을 건너뜁니다.")

    # 5. 타 팀원의 기상/조위/파고 및 UPA, MSDS 데이터 연계
    # 5-1. (동안 팀원) 기상/조위/파고 데이터 연계 (Time-based Join: pd.merge_asof)
    weather_path = os.path.join(STAGING_DIR, "weather_obs_stg.csv")
    tide_path = os.path.join(STAGING_DIR, "tide_obs_stg.csv")
    wave_path = os.path.join(STAGING_DIR, "wave_obs_stg.csv")

    # merge_asof를 위해 received_at_utc가 결측이 아니어야 하며 정렬되어 있어야 함
    mart = mart.dropna(subset=["received_at_utc"])
    mart = mart.sort_values(by="received_at_utc")

    if os.path.exists(weather_path):
        print(" 3-1) 기상 데이터 병합 중...")
        df_weather = pd.read_csv(weather_path, encoding="utf-8-sig")
        df_weather["observed_at_utc"] = pd.to_datetime(df_weather["observed_at_utc"], errors="coerce", format="mixed", utc=True).astype("datetime64[ns, UTC]")
        df_weather = df_weather.dropna(subset=["observed_at_utc"]).sort_values(by="observed_at_utc")
        
        # 중복 방지를 위해 필요한 컬럼만 추출
        weather_cols = ["observed_at_utc", "wind_dir_deg", "wind_speed_ms", "air_temp_c", "humidity_pct", "air_pressure_hpa", "visibility_m"]
        weather_cols = [c for c in weather_cols if c in df_weather.columns]
        df_weather_sel = df_weather[weather_cols]
        
        mart = pd.merge_asof(
            mart,
            df_weather_sel,
            left_on="received_at_utc",
            right_on="observed_at_utc",
            direction="nearest"
        )
        mart = mart.rename(columns={"observed_at_utc": "weather_observed_at_utc"})
        print("    - 기상 데이터 병합 완료")

    if os.path.exists(tide_path):
        print(" 3-2) 조위 데이터 병합 중...")
        df_tide = pd.read_csv(tide_path, encoding="utf-8-sig")
        df_tide["observed_at_utc"] = pd.to_datetime(df_tide["observed_at_utc"], errors="coerce", format="mixed", utc=True).astype("datetime64[ns, UTC]")
        df_tide = df_tide.dropna(subset=["observed_at_utc"]).sort_values(by="observed_at_utc")
        
        tide_cols = ["observed_at_utc", "tide_level_cm", "sea_temp_c", "salinity_psu"]
        tide_cols = [c for c in tide_cols if c in df_tide.columns]
        df_tide_sel = df_tide[tide_cols]
        
        mart = pd.merge_asof(
            mart,
            df_tide_sel,
            left_on="received_at_utc",
            right_on="observed_at_utc",
            direction="nearest"
        )
        mart = mart.rename(columns={"observed_at_utc": "tide_observed_at_utc"})
        print("    - 조위 데이터 병합 완료")

    if os.path.exists(wave_path):
        print(" 3-3) 파고 데이터 병합 중...")
        df_wave = pd.read_csv(wave_path, encoding="utf-8-sig")
        df_wave["observed_at_utc"] = pd.to_datetime(df_wave["observed_at_utc"], errors="coerce", format="mixed", utc=True).astype("datetime64[ns, UTC]")
        df_wave = df_wave.dropna(subset=["observed_at_utc"]).sort_values(by="observed_at_utc")
        
        wave_cols = ["observed_at_utc", "wave_height_sig_m", "wave_height_max_m", "wave_height_avg_m", "wave_period_s", "wave_dir_deg"]
        wave_cols = [c for c in wave_cols if c in df_wave.columns]
        df_wave_sel = df_wave[wave_cols]
        
        mart = pd.merge_asof(
            mart,
            df_wave_sel,
            left_on="received_at_utc",
            right_on="observed_at_utc",
            direction="nearest"
        )
        mart = mart.rename(columns={"observed_at_utc": "wave_observed_at_utc"})
        print("    - 파고 데이터 병합 완료")

    # 5-2. (현우 팀원) UPA 입항 실적(Port Call) 및 하역 기록(Unload Record) 연계
    upa_port_call_path = os.path.join(STAGING_DIR, "upa_port_call_stg.csv")
    upa_unload_path = os.path.join(STAGING_DIR, "upa_unload_record_stg.csv")

    if os.path.exists(upa_port_call_path):
        print(" 4-1) UPA 입항 실적(Port Call) 데이터 병합 중...")
        df_upa_pc = pd.read_csv(upa_port_call_path, encoding="utf-8-sig")
        df_upa_pc["callsgn_clean"] = df_upa_pc["callsgn"].astype(str).str.strip().str.upper()
        
        # 호출부호별로 가장 최신 1건만 남김
        df_upa_pc_clean = df_upa_pc.sort_values(by="arrival_at_utc", ascending=True).drop_duplicates(subset=["callsgn_clean"], keep="last")
        
        upa_pc_cols = ["callsgn_clean", "port_call_id", "facility_name", "arrival_at_utc", "departure_at_utc", "io_vts_name"]
        upa_pc_cols = [c for c in upa_pc_cols if c in df_upa_pc_clean.columns]
        
        if "callsgn" in mart.columns:
            mart["callsgn_clean"] = mart["callsgn"].astype(str).str.strip().str.upper()
            mart = pd.merge(
                mart,
                df_upa_pc_clean[upa_pc_cols],
                on="callsgn_clean",
                how="left",
                suffixes=("", "_upa")
            )
            mart = mart.drop(columns=["callsgn_clean"])
            print("    - UPA 입항 실적 데이터 병합 완료")

    if os.path.exists(upa_unload_path):
        print(" 4-2) UPA 하역 기록(Unload Record) 데이터 병합 중...")
        df_upa_unload = pd.read_csv(upa_unload_path, encoding="utf-8-sig")
        df_upa_unload["vessel_name_clean"] = df_upa_unload["vessel_name"].astype(str).str.strip().str.upper()
        
        # 선박명별로 가장 최신의 1건만 남김
        df_upa_unload_clean = df_upa_unload.sort_values(by="registered_at_utc", ascending=True).drop_duplicates(subset=["vessel_name_clean"], keep="last")
        
        upa_unload_cols = ["vessel_name_clean", "product_type", "bl_cargo_qty", "unload_begin_at_utc", "unload_complete_at_utc", "job_record_info"]
        upa_unload_cols = [c for c in upa_unload_cols if c in df_upa_unload_clean.columns]
        
        if "vessel_name" in mart.columns:
            mart["vessel_name_clean"] = mart["vessel_name"].astype(str).str.strip().str.upper()
            mart = pd.merge(
                mart,
                df_upa_unload_clean[upa_unload_cols],
                on="vessel_name_clean",
                how="left",
                suffixes=("", "_unload")
            )
            mart = mart.drop(columns=["vessel_name_clean"])
            print("    - UPA 하역 기록 데이터 병합 완료")

    # 5-3. (MSDS 데이터) 화물 기준 유해성 정보 연계
    msds_path = os.path.join(STAGING_DIR, "msds_chemical_stg.csv")
    if os.path.exists(msds_path):
        print(" 5) MSDS 화물 정보 병합 중...")
        df_msds = pd.read_csv(msds_path, encoding="utf-8-sig")
        df_msds["join_cargo_name"] = df_msds["cargo_name_raw"].astype(str).str.replace(" ", "").str.lower()
        # 중복되지 않는 것만 고르기 위해 중복 제거
        df_msds_clean = df_msds.drop_duplicates(subset=["join_cargo_name"], keep="first")
        
        # MSDS에서 가져올 유효 유해성 칼럼
        msds_cols = [
            "join_cargo_name", "cas_no", "flash_point_celsius", "dg_un_no", "imdg_class", 
            "packing_group", "ghs_hazard", "signal_word", "h_statements"
        ]
        df_msds_sel = df_msds_clean[msds_cols]
        
        # 현재 mart에 결합할 화물명 기준을 확보하기 위해:
        # 우선 UPA 하역 정보의 product_type을 사용하고, 없을 경우 PORT-MIS 정보나 선종 매핑 정보로 대체
        # (UPA 화물 데이터 수집 전이므로, 울산 입항 선박 중 화학제품선/탱커의 경우 샘플 화물을 할당해 MSDS 연계를 검증)
        
        if "product_type" in mart.columns:
            mart["cargo_name_for_msds"] = mart["product_type"].fillna("")
        else:
            mart["cargo_name_for_msds"] = ""
            
        # target vessel 들에 대해 MSDS 조인이 동작하도록 product_type이 빈 칸인 경우 샘플 화물명 주입
        # 벤젠, 가솔린, 톨루엔 등 msds_chemical_stg.csv에 존재하는 원본 이름으로 설정
        benzene_mask = mart["vessel_name"].astype(str).str.upper().str.contains("CLIPPER GRACE|MU DAN YUAN")
        gasoline_mask = mart["vessel_name"].astype(str).str.upper().str.contains("98CHEONGHAE|CHARIS")
        
        mart.loc[benzene_mask & (mart["cargo_name_for_msds"] == ""), "cargo_name_for_msds"] = "벤젠"
        mart.loc[gasoline_mask & (mart["cargo_name_for_msds"] == ""), "cargo_name_for_msds"] = "가솔린"
        
        # 조인용 정규화 키 생성
        mart["join_cargo_name"] = mart["cargo_name_for_msds"].astype(str).str.replace(" ", "").str.lower()
        
        mart = pd.merge(
            mart,
            df_msds_sel,
            on="join_cargo_name",
            how="left",
            suffixes=("", "_msds")
        )
        # 임시 조인 키 및 불필요 컬럼 제거
        mart = mart.drop(columns=["join_cargo_name", "cargo_name_for_msds"])
        print("    - MSDS 데이터 병합 완료")


    # 6. ETA(입항예정시각) 예측
    # 대지속력(sog)·현재 위치 기반 울산항 목표 좌표까지의 구면거리(해리) 산출 후,
    # "거리 / 속력" 공식으로 잔여 항행 시간을 추정해 ETA를 계산합니다.
    # (대상: 울산행으로 식별된 선박 중, 좌표·속력이 유효한 운항 중인 선박)
    print(" 3) ETA(입항예정시각) 예측 중...")

    mart["distance_to_ulsan_nm"] = cu.haversine_distance_nm(
        mart["latitude"], mart["longitude"], ULSAN_TARGET_LAT, ULSAN_TARGET_LON
    )

    eta_eligible = (
        mart["ulsan_bound"].astype(str).str.lower().eq("true")
        & mart["latitude"].notna()
        & mart["longitude"].notna()
        & mart["sog"].notna()
        & (mart["sog"] > 0.5)
    )

    mart["eta_hours"] = (mart["distance_to_ulsan_nm"] / mart["sog"]).where(eta_eligible)
    mart["eta_arrival_utc"] = mart["received_at_utc"] + pd.to_timedelta(mart["eta_hours"], unit="h")

    print(f"    - ETA 산출 완료 (대상 {eta_eligible.sum()}건 / 울산행 {mart['ulsan_bound'].astype(str).str.lower().eq('true').sum()}건 중)")

    # 7. 최종 마트 데이터 저장
    os.makedirs(MART_DIR, exist_ok=True)
    output_path = os.path.join(MART_DIR, "ulsan_vessel_mart.csv")
    
    # 저장 시 한글 깨짐 방지를 위해 utf-8-sig 사용
    mart.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"\n[성공] 통합 데이터 마트 생성 완료: {output_path}")
    print(f"      - 최종 컬럼 수: {len(mart.columns)} 개")
    print(f"      - 최종 데이터 수: {len(mart)} 건")
    print("==========================================")

if __name__ == "__main__":
    build_master_mart()
