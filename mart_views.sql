-- ===========================================================================
-- mart_views.sql v2 — 통합 마트 뷰 6종 (키 JOIN 기반 '한 줄 조회')
-- ===========================================================================
-- 13종 staging 테이블이 한 PostgreSQL 에 모인 뒤, 에이전트·대시보드·FastAPI 가
-- 바로 쓸 수 있도록 키 JOIN 으로 묶은 조회 전용 뷰 6개를 만든다.
--
-- v2 (설계 초안 인수·통합): 김동안 설계 초안(뷰 6개 구조)과 기존 v1(뷰 4개)을
-- 통합 — v1 의 dashboard_current 안에 숨어 있던 최신 위치/화물-MSDS 로직을
-- vessel_latest_position / cargo_msds 뷰로 분리해 중복 CTE 를 제거했고,
-- create_mart.py 에만 있던 거리·ETA 산출을 동일 공식(하버사인, R=3440.065)으로
-- dashboard_current 에 통합했다. 실제 적재 테이블 컬럼명·타입 대조 완료.
--
-- 실행: DBeaver(5433) 에서 이 파일 전체 실행 (또는 psql -f mart_views.sql)
-- 검증: mart_views_check.sql 의 검증 쿼리 5종이 모두 PASS 여야 한다.
--
-- 물리 마트(ulsan_vessel_mart, create_mart.py)와의 역할 구분:
--   뷰(여기)        = "지금" 상태 조회 — 대시보드·에이전트 실시간 서빙용
--   물리 마트 테이블 = 스냅샷 이력 축적 — 시계열 분석·모델 학습용
--   (동일 조인 로직의 이중화가 아니라 서빙/학습 용도 분리다)
--
-- 연계 키 (회의 합의):
--   callsgn          — 선박: 위치 ↔ PORT-MIS ↔ UPA (upper/trim 정규화 후 조인)
--   dg_un_no         — 화물 ↔ MSDS 위험물
--   bl_no            — 하역 ↔ 화물
--   observed_at_utc  — 기상 최신값
--   식별 우선순위     — imo → mmsi → callsgn (vessel_key)
--
-- 뷰 계층 (의존 순서대로 생성):
--   1. mart.vessel_identity        선박 식별 마스터 (1척 = 1행)
--   2. mart.vessel_latest_position 선박별 최신 위치 1행 (UPA 우선, AIS 보강)
--   3. mart.port_call_overview     입항 통합 (입항건당 대표 1행)
--   4. mart.cargo_msds             화물 ↔ MSDS (★안전관제 핵심)
--   5. mart.weather_now            환경 최신 1행 (기상+조위+파고 스냅샷)
--   6. mart.dashboard_current      '한 줄 조회' — 대시보드·에이전트 진입점
-- ===========================================================================

CREATE SCHEMA IF NOT EXISTS mart;

-- ---------------------------------------------------------------------------
-- 0. 선행 조건: 참조 테이블 존재 보장 (빈 테이블이라도)
--
-- PostgreSQL 은 CREATE VIEW 시점에 참조 테이블의 "존재"를 검사한다(데이터는 안
-- 읽는다). 따라서 테이블이 아예 없으면 뷰 등록 자체가 실패한다.
--
-- upa_cargo_manifest 는 UPA 통합화물 API(getIntgCagInfo)가 업체코드(bzentyCd)
-- 필수라 아직 자동수집이 불가해 테이블이 없는 상태다. 그 결과 화물과 직접
-- 관련이 없는 port_call_overview 까지 (5개 부가 컬럼을 LEFT JOIN 한다는 이유로)
-- 함께 생성 실패하고, 이를 참조하는 dashboard_current 까지 연쇄로 죽었다.
--
-- 아래 CREATE TABLE IF NOT EXISTS 로 "빈 껍데기"를 보장하면:
--   - 뷰 6종이 화물 데이터 없이도 정상 등록된다 (화물 컬럼만 NULL)
--   - 나중에 실데이터(또는 합성 샘플)가 적재되면 뷰 수정 없이 그대로 채워진다
--   - 이미 테이블이 있으면 이 구문은 아무 일도 하지 않는다 (기존 데이터 안전)
--
-- 컬럼 구성은 upa_config.py 의 INTG_CAG_INFO / INPRT_CAG_DCLR_INFO column_map
-- 합집합 + 공통 메타데이터를 따른다 (적재 시 스키마 불일치 방지).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS upa_cargo_manifest (
    port_code                    text,
    ptent_yr                     text,
    voyage_no                    text,
    callsgn                      text,
    vessel_name                  text,
    vessel_type_name             text,
    vessel_nationality_code      text,
    vessel_nationality_name      text,
    mrn_no                       text,
    bl_no                        text,
    master_bl_no                 text,
    io_se_code                   text,
    io_se_name                   text,
    facility_name                text,
    cargo_se_name                text,
    cargo_name_raw               text,          -- MSDS 매핑 입력
    dg_un_no                     text,          -- 위험물 UN 번호 (MSDS 조인키)
    cargo_basis                  text,          -- 합성 샘플의 화물 배정 근거 표기
    package_type_name            text,
    unload_method_name           text,
    vol_ton_unit_name            text,
    vol_ton                      double precision,
    weight_ton                   double precision,
    vol_size                     double precision,
    weight_size                  double precision,
    bulk_vol_size                double precision,
    bulk_weight_size             double precision,
    container_count              double precision,
    pod_name                     text,          -- 양하항
    pol_name                     text,          -- 적하항
    ldud_port_name               text,          -- 양적하 항구명 (내항화물)
    last_dest_port_name          text,
    arrival_at_utc               timestamptz,
    customs_progress_status_name text,
    source_system                text,
    source_table                 text,
    collected_at_utc             timestamptz,
    quality_flag                 text,
    is_synthetic                 boolean
);

-- ---------------------------------------------------------------------------
-- 1. mart.vessel_identity — 선박 식별 마스터 (1척 = 1행)
--    위치(UPA)와 PORT-MIS 를 callsgn 으로 묶고, 식별 우선순위 imo → mmsi →
--    callsgn 에 따라 대표 키(vessel_key)를 만든다. 선종·액체화물선 여부는
--    공식 신고 데이터인 PORT-MIS 를 정본으로 삼는다.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.vessel_identity AS
WITH pos_latest AS (
    SELECT DISTINCT ON (upper(trim(callsgn)))
           upper(trim(callsgn))            AS callsgn,
           mmsi,
           imo_no::bigint                  AS imo_no,
           vessel_name,
           received_at_utc                 AS last_position_at_utc
    FROM upa_vessel_position
    WHERE callsgn IS NOT NULL
    ORDER BY upper(trim(callsgn)), received_at_utc DESC
),
pm_latest AS (
    SELECT DISTINCT ON (upper(trim(callsgn)))
           upper(trim(callsgn))            AS callsgn,
           vessel_name                     AS vessel_name_portmis,
           nationality_cd,
           nationality_nm,
           ship_kind_cd,
           ship_kind_nm,
           ship_kind_category,
           is_liquid_cargo_vessel,
           port_agency_label
    FROM portmis_vessel
    WHERE callsgn IS NOT NULL
    ORDER BY upper(trim(callsgn)), collected_at_utc DESC
)
SELECT
    -- 식별 우선순위: imo → mmsi → callsgn
    COALESCE(p.imo_no::text, p.mmsi::text, c.callsgn)   AS vessel_key,
    c.callsgn,
    p.imo_no,
    p.mmsi,
    COALESCE(p.vessel_name, m.vessel_name_portmis)      AS vessel_name,
    m.nationality_cd,
    m.nationality_nm,
    m.ship_kind_cd,
    m.ship_kind_nm,
    m.ship_kind_category,
    m.is_liquid_cargo_vessel,
    m.port_agency_label,
    p.last_position_at_utc,
    (p.callsgn IS NOT NULL)                             AS has_position,
    (m.callsgn IS NOT NULL)                             AS has_portmis
FROM (SELECT callsgn FROM pos_latest
      UNION
      SELECT callsgn FROM pm_latest) c
LEFT JOIN pos_latest p ON p.callsgn = c.callsgn
LEFT JOIN pm_latest  m ON m.callsgn = c.callsgn;

-- ---------------------------------------------------------------------------
-- 2. mart.vessel_latest_position — 선박별 최신 위치 1행
--    UPA 항내 선박위치(기본 소스)를 우선하되, 레거시 AIS 관측이 더 최신이면
--    그것을 쓴다 (mmsi → vessel_identity 로 callsgn 역매핑). position_source
--    컬럼으로 어느 소스의 관측인지 표시한다.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.vessel_latest_position AS
WITH unified AS (
    SELECT upper(trim(callsgn))       AS callsgn,
           mmsi,
           latitude,
           longitude,
           sog,
           cog,
           nav_status_code::text      AS nav_status_code,
           received_at_utc,
           quality_flag,
           'UPA'                      AS position_source
    FROM upa_vessel_position
    WHERE callsgn IS NOT NULL

    UNION ALL

    SELECT vi.callsgn,
           a.mmsi,
           a.latitude,
           a.longitude,
           a.sog,
           a.cog,
           a.nav_status_code::text,
           a.received_at_utc,
           a.quality_flag,
           'AIS'
    FROM ais_vessel_position a
    LEFT JOIN (
        SELECT DISTINCT ON (mmsi) mmsi, callsgn
        FROM mart.vessel_identity
        WHERE mmsi IS NOT NULL
        ORDER BY mmsi
    ) vi ON vi.mmsi = a.mmsi
)
SELECT DISTINCT ON (COALESCE(callsgn, mmsi::text))
       COALESCE(callsgn, mmsi::text)  AS vessel_pos_key,
       callsgn,
       mmsi,
       latitude,
       longitude,
       sog,
       cog,
       nav_status_code,
       received_at_utc,
       quality_flag,
       position_source
FROM unified
ORDER BY COALESCE(callsgn, mmsi::text), received_at_utc DESC;

-- ---------------------------------------------------------------------------
-- 3. mart.port_call_overview — 입항 통합 (입항건당 대표 1행)
--    PORT-MIS 입출항 신고 + UPA 운항관제(접안 부두·일시) + UPA 화물 manifest
--    요약(품목 수 / B/L 수 / 위험물 UN 번호)을 callsgn 으로 결합한다.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.port_call_overview AS
WITH pm_latest AS (
    SELECT DISTINCT ON (upper(trim(callsgn)))
           upper(trim(callsgn))            AS callsgn,
           entry_year,
           entry_count,
           entry_purpose_cd,
           entry_purpose_nm,
           origin_port_nm,
           prev_port_nm,
           next_port_nm,
           dest_port_nm,
           is_domestic_voyage,
           port_agency_label
    FROM portmis_vessel
    WHERE callsgn IS NOT NULL
    ORDER BY upper(trim(callsgn)), collected_at_utc DESC
),
pc_latest AS (
    SELECT DISTINCT ON (upper(trim(callsgn)))
           upper(trim(callsgn))            AS callsgn,
           port_call_id,
           arrival_at_utc,
           departure_at_utc,
           facility_name,
           io_vts_name
    FROM upa_port_call
    WHERE callsgn IS NOT NULL
    ORDER BY upper(trim(callsgn)), arrival_at_utc DESC
),
cargo_sum AS (
    SELECT upper(trim(callsgn))            AS callsgn,
           count(*)                        AS cargo_item_count,
           count(DISTINCT bl_no)           AS bl_count,
           count(*) FILTER (WHERE dg_un_no IS NOT NULL)        AS dg_cargo_count,
           string_agg(DISTINCT dg_un_no::text, ',')            AS dg_un_nos,
           string_agg(DISTINCT cargo_name_raw, ' | ')          AS cargo_names
    FROM upa_cargo_manifest
    WHERE callsgn IS NOT NULL
    GROUP BY upper(trim(callsgn))
)
SELECT
    pm.callsgn,
    pm.entry_year,
    pm.entry_count,
    pm.entry_purpose_nm,
    pm.origin_port_nm,
    pm.prev_port_nm,
    pm.next_port_nm,
    pm.dest_port_nm,
    pm.is_domestic_voyage,
    pm.port_agency_label,
    pc.port_call_id,
    pc.arrival_at_utc,
    pc.departure_at_utc,
    pc.facility_name,
    pc.io_vts_name,
    cs.cargo_item_count,
    cs.bl_count,
    cs.dg_cargo_count,
    cs.dg_un_nos,
    cs.cargo_names
FROM pm_latest pm
LEFT JOIN pc_latest pc ON pc.callsgn = pm.callsgn
LEFT JOIN cargo_sum cs ON cs.callsgn = pm.callsgn;

-- ---------------------------------------------------------------------------
-- 4. mart.cargo_msds — 화물 ↔ MSDS 위험물 (★안전관제 핵심, 화물 행 단위)
--    UPA 화물 manifest 의 dg_un_no ↔ msds_chemical.dg_un_no 조인으로
--    화물 1건마다 인화점·IMDG 등급·GHS 정보를 붙인다. UN 번호가 없는 화물은
--    msds_matched=false 로 남는다 (일반 화물 or 코드 미기재 — 후자는 bzentyCd
--    확보로 통합화물 API 조회가 가능해지면 자동으로 채워진다).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.cargo_msds AS
SELECT
    upper(trim(cm.callsgn))        AS callsgn,
    cm.bl_no,
    cm.cargo_name_raw,
    cm.dg_un_no,
    ms.chem_id,
    ms.cas_no,
    ms.flash_point_celsius,
    ms.imdg_class,
    ms.packing_group,
    ms.signal_word,
    ms.ghs_hazard,
    ms.h_statements,
    (ms.chem_id IS NOT NULL)       AS msds_matched
FROM upa_cargo_manifest cm
LEFT JOIN msds_chemical ms
  ON cm.dg_un_no IS NOT NULL
 -- UN 번호 표기 편차 정규화 후 조인: "UN1972" / "1972" / "1972.0"(숫자형 적재
 -- 잔재) 를 모두 "1972" 로 통일. 정규화 없이 원문 비교하면 매칭 누락 발생.
 AND nullif(regexp_replace(upper(trim(ms.dg_un_no::text)), '^UN|\.0$', '', 'g'), '')
   = nullif(regexp_replace(upper(trim(cm.dg_un_no::text)), '^UN|\.0$', '', 'g'), '');

-- ---------------------------------------------------------------------------
-- 5. mart.weather_now — 환경 최신 (항상 정확히 1행)
--    기상·조위·파고 각 최신 관측 1건을 옆으로 붙인다 (observed_at_utc 기준).
--    앵커(1행) 기준 LEFT JOIN — 세 관측 테이블 중 일부가 비어 있어도(수집 전·
--    장애) 뷰가 0행이 되지 않고 해당 소스만 NULL 로 남는다 (CROSS JOIN 이었다면
--    한 테이블만 비어도 대시보드 환경 컬럼 전체가 사라진다).
--    울산 단일 관측 지점 전제 — 다지점 수집으로 바뀌면 station_id 필터 추가.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.weather_now AS
SELECT
    w.observed_at_utc      AS weather_observed_at_utc,
    w.wind_dir_deg,
    w.wind_speed_ms,
    w.air_temp_c,
    w.humidity_pct,
    w.air_pressure_hpa,
    w.visibility_m,
    t.observed_at_utc      AS tide_observed_at_utc,
    t.tide_level_cm,
    t.sea_temp_c,
    t.salinity_psu,
    v.observed_at_utc      AS wave_observed_at_utc,
    v.wave_height_sig_m,
    v.wave_height_max_m,
    v.wave_period_s,
    v.wave_dir_deg
FROM (SELECT 1) AS anchor
LEFT JOIN (SELECT * FROM weather_obs ORDER BY observed_at_utc DESC LIMIT 1) w ON TRUE
LEFT JOIN (SELECT * FROM tide_obs ORDER BY observed_at_utc DESC LIMIT 1) t ON TRUE
LEFT JOIN (SELECT * FROM wave_obs ORDER BY observed_at_utc DESC LIMIT 1) v ON TRUE;

-- ---------------------------------------------------------------------------
-- 6. mart.dashboard_current — '한 줄 조회' (대시보드·에이전트 진입점)
--    선박별 최신 위치 1행에 식별·입항·위험물 요약·환경 최신을 전부 결합.
--    거리·ETA 는 create_mart.py 와 동일 공식(하버사인, 지구반경 3440.065 해리,
--    목표 좌표 35.475/129.387)으로 산출 — FastAPI/에이전트는 이 뷰만 조회한다.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.dashboard_current AS
WITH msds_by_vessel AS (
    SELECT callsgn,
           min(flash_point_celsius)                    AS min_flash_point_c,
           string_agg(DISTINCT imdg_class, ',')        AS imdg_classes,
           string_agg(DISTINCT signal_word, ',')       AS ghs_signal_words,
           count(*) FILTER (WHERE msds_matched)        AS msds_matched_count
    FROM mart.cargo_msds
    GROUP BY callsgn
)
SELECT
    -- [선박 식별]
    vi.vessel_key,
    p.callsgn,
    vi.imo_no,
    vi.mmsi,
    vi.vessel_name,
    vi.ship_kind_nm,
    vi.is_liquid_cargo_vessel,
    vi.nationality_nm,
    -- [최신 위치·상태]
    p.latitude,
    p.longitude,
    p.sog,
    p.nav_status_code,
    p.received_at_utc,
    p.position_source,
    p.quality_flag,
    -- [거리·ETA — 울산항 목표 좌표 기준]
    d.distance_to_ulsan_nm,
    CASE WHEN p.sog > 0.5 THEN d.distance_to_ulsan_nm / p.sog END AS eta_hours,
    -- [입항·접안]
    pco.arrival_at_utc,
    pco.departure_at_utc,
    pco.facility_name,
    pco.entry_purpose_nm,
    pco.dest_port_nm,
    -- [화물·위험물]
    pco.cargo_item_count,
    pco.dg_cargo_count,
    pco.dg_un_nos,
    mv.min_flash_point_c,
    mv.imdg_classes,
    mv.ghs_signal_words,
    mv.msds_matched_count,
    -- [환경 최신 — 모든 행에 동일 부착]
    wn.wind_speed_ms,
    wn.visibility_m,
    wn.tide_level_cm,
    wn.wave_height_sig_m,
    wn.weather_observed_at_utc
FROM mart.vessel_latest_position p
CROSS JOIN LATERAL (
    SELECT CASE
        WHEN p.latitude IS NOT NULL AND p.longitude IS NOT NULL THEN
            3440.065 * 2 * asin(sqrt(
                power(sin(radians((35.475 - p.latitude) / 2)), 2)
                + cos(radians(p.latitude)) * cos(radians(35.475))
                  * power(sin(radians((129.387 - p.longitude) / 2)), 2)
            ))
    END AS distance_to_ulsan_nm
) d
LEFT JOIN mart.vessel_identity     vi  ON vi.callsgn = p.callsgn
LEFT JOIN mart.port_call_overview  pco ON pco.callsgn = p.callsgn
LEFT JOIN msds_by_vessel           mv  ON mv.callsgn = p.callsgn
LEFT JOIN mart.weather_now         wn  ON TRUE;
