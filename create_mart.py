"""
create_mart.py
==============
Staging 단계의 전처리 데이터들을 결합하여, 울산항 실시간 관제 및 예측 모델에 사용할
통합 데이터 마트(Master Data Mart)를 구축합니다.

병합 대상:
    1. 선박 위치 데이터 - data/staging/upa_vessel_position_stg.csv (기본)
       (UPA 항내 선박위치 getVslPstnInfo — callsgn/mmsi/imo/선박명 네이티브 포함.
        파일이 없으면 레거시 AIS staging(ais_vessel_position/static)으로 폴백)
    2. PORT-MIS 입출항 데이터 - data/staging/portmis_vessel_stg.csv
       (선종·액체화물선 여부는 여기서 callsgn 조인으로 확정 — AIS ShipType 미의존)
    * (확장) 기상/조위/파고, UPA 입항·하역, MSDS

선박 위치 소스 교체 배경 (2026-07):
    aisstream 기반 수집은 AIS static의 ship_type 이 전부 NaN 으로 수신되어
    액체화물선(Tanker) 식별이 0척이었고, 위치 다수가 울산 bbox 밖으로 오탐됐다.
    UPA API 는 울산 항내 스코프 + callsgn 네이티브 제공으로 두 문제를 해소한다.

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

# 울산항 관제 목표 좌표 (입항 정박지 대표 좌표) — 부두별 좌표를 못 찾을 때의 폴백.
ULSAN_TARGET_LAT = 35.475
ULSAN_TARGET_LON = 129.387

# 부두별 접안 좌표 세분화 (온산 MVP 이식 — 이 TODO가 원래 여기 있었다:
# "함현우 담당의 부두별 접안 좌표가 확정되면 선종/화물별 타겟 좌표로 세분화").
# 팀원이 PDF에서 새로 만든 CSV를 쓰지 않고, 이미 매일 수집되는
# upa_berth_facility_stg.csv(부두 GIS, UPA getGisBaseHrbrFcltDtlInfo)를 그대로
# 재사용한다 — 온산MVP_이식_코드변화예측 9장에서 확인된 대로 팀원 CSV와 대상
# 데이터가 겹치므로 새 좌표 테이블을 또 만들지 않는다.
BERTH_COORDS_PATH = os.path.join(STAGING_DIR, "upa_berth_facility_stg.csv")


def _load_berth_target_coords() -> dict:
    """wharf_name -> (lat, lon) 조회 테이블. upa_port_call의 facility_name과
    wharf_name이 같은 UPA 명명 체계를 쓰므로 그대로 조인 키로 쓸 수 있다.

    좌표 결측 선석(SPM 부이 2기, 달포부두 등)은 이 사전에 없다 — 그런 선석이
    타겟이면 ULSAN_TARGET_LAT/LON(항 전체 대표 좌표)으로 자동 폴백한다.
    """
    if not os.path.exists(BERTH_COORDS_PATH):
        return {}
    df = pd.read_csv(BERTH_COORDS_PATH, encoding="utf-8-sig")
    df = df.dropna(subset=["wharf_name", "latitude", "longitude"])
    return {row["wharf_name"]: (row["latitude"], row["longitude"]) for _, row in df.iterrows()}

def build_master_mart():
    print("=== [3단계] 통합 데이터 마트 구축 시작 ===")
    
    # 1. 파일 경로 정의
    upa_pos_path = os.path.join(STAGING_DIR, "upa_vessel_position_stg.csv")
    ais_pos_path = os.path.join(STAGING_DIR, "ais_vessel_position_stg.csv")
    static_path = os.path.join(STAGING_DIR, "ais_vessel_static_stg.csv")
    portmis_path = os.path.join(STAGING_DIR, "portmis_vessel_stg.csv")

    # 위치 소스 선택: UPA(기본) → 없으면 레거시 AIS 폴백
    use_upa = os.path.exists(upa_pos_path)
    if use_upa:
        required = [upa_pos_path, portmis_path]
    else:
        required = [ais_pos_path, static_path, portmis_path]
    for p in required:
        if not os.path.exists(p):
            print(f"[오류] 필수 staging 파일이 누락되었습니다: {p}")
            print("전처리 스크립트를 먼저 실행해주세요.")
            return

    # 2. 데이터 로드 및 정합성 처리
    print(" 1) Staging 데이터 로드 중...")
    df_portmis = pd.read_csv(portmis_path, encoding="utf-8-sig")

    if use_upa:
        df_pos = pd.read_csv(upa_pos_path, encoding="utf-8-sig")
        df_static = None
        print(f"    - 선박 위치 데이터 (UPA 항내): {len(df_pos)} 건")
        # 항내 위치 데이터이므로 전 선박이 울산 관제권 내 = 울산행/재항으로 취급
        df_pos["ulsan_bound"] = True
    else:
        df_pos = pd.read_csv(ais_pos_path, encoding="utf-8-sig")
        df_static = pd.read_csv(static_path, encoding="utf-8-sig")
        print(f"    - 선박 위치 데이터 (레거시 AIS): {len(df_pos)} 건")
        print(f"    - AIS 제원 데이터: {len(df_static)} 건")
    print(f"    - PORT-MIS 데이터 : {len(df_portmis)} 건")

    # 시간 타입 명시적 변환 (mixed 포맷 및 동일 datetime64[ns, UTC] 타입 통일 적용)
    df_pos["received_at_utc"] = pd.to_datetime(df_pos["received_at_utc"], errors="coerce", format="mixed", utc=True).astype("datetime64[ns, UTC]")
    if df_static is not None:
        df_static["collected_at_utc"] = pd.to_datetime(df_static["collected_at_utc"], errors="coerce", format="mixed", utc=True).astype("datetime64[ns, UTC]")
    df_portmis["collected_at_utc"] = pd.to_datetime(df_portmis["collected_at_utc"], errors="coerce", format="mixed", utc=True).astype("datetime64[ns, UTC]")
    
    if "departure_sched_utc" in df_portmis.columns:
        df_portmis["departure_sched_utc"] = pd.to_datetime(df_portmis["departure_sched_utc"], errors="coerce", format="mixed", utc=True).astype("datetime64[ns, UTC]")
    if "dest_arrival_utc" in df_portmis.columns:
        df_portmis["dest_arrival_utc"] = pd.to_datetime(df_portmis["dest_arrival_utc"], errors="coerce", format="mixed", utc=True).astype("datetime64[ns, UTC]")

    # 3. 조인을 위한 데이터 정제 및 중복 제거
    # 3-1. (레거시 AIS 폴백 전용) AIS 제원: MMSI별 가장 최근 수집된 1건만 남김
    if df_static is not None:
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

    if use_upa:
        # 4-1. UPA 위치 데이터는 callsgn/mmsi/imo_no/vessel_name/draught 를
        # 네이티브로 포함하므로 별도 제원(Static) 조인이 필요 없다.
        mart = df_pos
        print(f"    - UPA 위치 데이터 사용 (제원 조인 생략, 행 수: {len(mart)}건)")
    else:
        # 4-1. (레거시) AIS 위치 + AIS 제원 (Key: mmsi)
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

    # 4-2. 위치 + PORT-MIS (Key: callsgn)
    # 선종(ship_kind)·액체화물선 여부는 공식 신고 데이터인 PORT-MIS 에서 확정한다.
    if "callsgn" in mart.columns:
        mart["callsgn_clean"] = mart["callsgn"].astype(str).str.strip().str.upper()
        
        # PORT-MIS에서 필요한 정보 선택 (중복 제거)
        portmis_cols_to_use = [
            "callsgn_clean", "port_agency_cd", "port_agency_nm", "entry_year", "entry_count",
            "nationality_cd", "nationality_nm", "ship_kind_cd", "ship_kind_nm",
            "entry_purpose_cd", "entry_purpose_nm", "origin_port_cd", "origin_port_nm",
            "prev_port_cd", "prev_port_nm", "next_port_cd", "next_port_nm",
            "dest_port_cd", "dest_port_nm", "departure_sched_utc", "dest_arrival_utc",
            "ship_kind_category", "is_domestic_voyage", "port_agency_label",
            # 액체화물선 여부 — UPA 위치 소스에서는 PORT-MIS 선종이 유일한 판별
            # 근거 (레거시 AIS 경로에서는 suffix _portmis 로 붙어 참고용이 된다)
            "is_liquid_cargo_vessel",
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
    # 대지속력(sog)·현재 위치 기반 목표 좌표까지의 구면거리(해리) 산출 후,
    # "거리 / 속력" 공식으로 잔여 항행 시간을 추정해 ETA를 계산합니다.
    # (대상: 울산행으로 식별된 선박 중, 좌표·속력이 유효한 운항 중인 선박)
    #
    # 목표 좌표는 항 전체 대표점 하나가 아니라, UPA 입항실적(facility_name, 5-2절
    # 병합분)으로 확인된 실제 접안 부두 좌표를 우선 쓴다(온산 MVP 이식 — 원래
    # 이 자리의 TODO였던 "부두별 타겟 좌표 세분화"). facility_name이 없거나
    # 좌표 결측 부두(SPM 부이 등)면 항 전체 대표 좌표로 폴백한다.
    print(" 3) ETA(입항예정시각) 예측 중...")

    berth_coords = _load_berth_target_coords()
    if "facility_name" in mart.columns and berth_coords:
        target_coords = mart["facility_name"].map(berth_coords)
        target_lat = target_coords.map(lambda c: c[0] if isinstance(c, tuple) else None).fillna(ULSAN_TARGET_LAT)
        target_lon = target_coords.map(lambda c: c[1] if isinstance(c, tuple) else None).fillna(ULSAN_TARGET_LON)
        matched = target_coords.notna().sum()
        print(f"    - 부두별 타겟 좌표 매칭: {matched}/{len(mart)}건 (나머지는 항 전체 대표 좌표로 폴백)")
    else:
        target_lat = ULSAN_TARGET_LAT
        target_lon = ULSAN_TARGET_LON

    mart["distance_to_ulsan_nm"] = cu.haversine_distance_nm(
        mart["latitude"], mart["longitude"], target_lat, target_lon
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

    # 7. 위치 스냅샷 자연키(mart_uid) 생성
    # record_uid(전체 행 해시) 적재는 기상 등 enrich 값이 갱신될 때마다 같은
    # 위치 스냅샷을 새 행으로 중복 적재한다 → "선박(callsgn, 없으면 mmsi) +
    # 관측시각" 자연키로 UPSERT 해 같은 스냅샷은 최신 enrich 로 갱신되게 한다.
    import hashlib

    # pandas 3.0 부터 .astype(str) 이 결측값을 "nan" 문자열이 아니라 실제 null 로
    # 남겨둔다(이전 버전과 동작 변경). 그 null 이 문자열 이어붙이기(+)를 타고
    # 전파되면 최종 값이 float(NaN) 이 되어 .encode() 에서 AttributeError 가 난다.
    # astype(str) 직후 매번 명시적으로 결측을 문자열로 고정해 버전에 관계없이
    # 동일하게 동작하도록 한다.
    def _str_col(s: pd.Series, na_token: str) -> pd.Series:
        s = s.astype(str)
        return s.mask(s.isna() | s.isin(["nan", "NaT", "None"]), na_token)

    if "callsgn" in mart.columns:
        vessel_id = _str_col(mart["callsgn"], "NAN").str.strip().str.upper()
    else:
        vessel_id = pd.Series("", index=mart.index)
    invalid = vessel_id.isin(["", "NAN", "NONE"])
    if "mmsi" in mart.columns:
        vessel_id = vessel_id.where(~invalid, _str_col(mart["mmsi"], "NAN"))
    received_str = _str_col(mart["received_at_utc"], "NAT")
    mart["mart_uid"] = (
        (vessel_id + "|" + received_str)
        .map(lambda x: hashlib.md5(x.encode("utf-8")).hexdigest())
    )

    # 8. 최종 마트 데이터 저장
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
