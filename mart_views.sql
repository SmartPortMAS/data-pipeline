-- ===========================================================================
-- mart_views.sql — 통합 마트 뷰 (키 JOIN 기반 '한 줄 조회')
-- ===========================================================================
-- 13종 staging 테이블이 한 PostgreSQL 에 모인 뒤, 에이전트·대시보드가 바로 쓸 수
-- 있도록 키 JOIN 으로 묶은 조회 전용 뷰 4개를 만든다. 원본 테이블은 건드리지
-- 않으며(뷰는 저장 공간 0), staging 이 갱신되면 뷰 결과도 즉시 따라온다.
--
-- 실행: DBeaver 에서 이 파일 전체 실행 (또는 psql -f mart_views.sql)
-- 필요 테이블: upa_vessel_position, portmis_vessel, upa_port_call,
--             upa_cargo_manifest, upa_unload_record, weather_obs, tide_obs,
--             wave_obs, msds_chemical
--             (없는 테이블이 있으면 해당 뷰만 실패한다 — 도메인 적재 후 재실행)
--
-- 연계 키 (회의 합의):
--   callsgn          — 선박: 위치 ↔ PORT-MIS ↔ UPA (upper/trim 정규화 후 조인)
--   dg_un_no         — 화물 ↔ MSDS 위험물
--   bl_no            — 하역 ↔ 화물
--   observed_at_utc  — 기상 3종 최신값
--   식별 우선순위     — imo → mmsi → callsgn (vessel_key)
--
-- 뷰 계층:
--   1. mart.vessel_identity     선박 식별 (1척 = 1행)
--   2. mart.port_call_overview  입항 통합 (입항 건 + 화물·위험물 요약)
--   3. mart.weather_now         환경 최신 (기상+조위+파고 최신 1행)
--   4. mart.dashboard_current   한 줄 조회 (최신 위치 기준 전 소스 결합)
-- ===========================================================================

CREATE SCHEMA IF NOT EXISTS mart;

-- ---------------------------------------------------------------------------
-- 1. mart.vessel_identity — 선박 식별 (1척 = 1행)
--    위치(UPA)와 PORT-MIS 를 callsgn 으로 묶고, 식별 우선순위 imo → mmsi →
--    callsgn 에 따라 대표 키(vessel_key)를 만든다. 선종·액체화물선 여부는
--    공식 신고 데이터인 PORT-MIS 를 정본으로 삼는다.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.vessel_identity AS
WITH pos_latest AS (
    SELECT DISTINCT ON (upper(trim(callsgn)))
           upper(trim(callsgn))            AS callsgn,
           mmsi,
           imo_no,
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
-- 2. mart.port_call_overview — 입항 통합 (선박별 최신 입항 건 1행)
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
-- 3. mart.weather_now — 환경 최신 (항상 1행)
--    기상·조위·파고 각 최신 관측 1건을 옆으로 붙인다 (observed_at_utc 기준).
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
FROM (SELECT * FROM weather_obs ORDER BY observed_at_utc DESC LIMIT 1) w
CROSS JOIN (SELECT * FROM tide_obs ORDER BY observed_at_utc DESC LIMIT 1) t
CROSS JOIN (SELECT * FROM wave_obs ORDER BY observed_at_utc DESC LIMIT 1) v;

-- ---------------------------------------------------------------------------
-- 4. mart.dashboard_current — 한 줄 조회 (선박별 최신 위치 1행 = 대시보드 1행)
--    최신 위치 + 선박 식별 + 입항 통합 + 위험물(MSDS) 요약 + 환경 최신을
--    전부 결합한다. 에이전트·대시보드는 이 뷰 하나만 조회하면 된다.
--    위험물 요약: 화물 manifest 의 dg_un_no ↔ msds_chemical.dg_un_no 조인으로
--    인화점 최솟값·IMDG 등급·GHS 신호어를 선박 단위로 집계.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.dashboard_current AS
WITH pos_latest AS (
    SELECT DISTINCT ON (upper(trim(callsgn)))
           upper(trim(callsgn))            AS callsgn,
           latitude,
           longitude,
           sog,
           cog,
           heading,
           draught,
           nav_status_code,
           received_at_utc,
           quality_flag
    FROM upa_vessel_position
    WHERE callsgn IS NOT NULL
    ORDER BY upper(trim(callsgn)), received_at_utc DESC
),
msds_by_vessel AS (
    SELECT upper(trim(cm.callsgn))         AS callsgn,
           min(ms.flash_point_celsius)     AS min_flash_point_c,
           string_agg(DISTINCT ms.imdg_class, ',')     AS imdg_classes,
           string_agg(DISTINCT ms.signal_word, ',')    AS ghs_signal_words,
           count(DISTINCT ms.chem_id)      AS msds_matched_count
    FROM upa_cargo_manifest cm
    JOIN msds_chemical ms
      ON ms.dg_un_no::text = cm.dg_un_no::text
    WHERE cm.dg_un_no IS NOT NULL
    GROUP BY upper(trim(cm.callsgn))
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
    p.quality_flag,
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
    -- [환경 최신 — 모든 행에 동일 부착]
    wn.wind_speed_ms,
    wn.visibility_m,
    wn.tide_level_cm,
    wn.wave_height_sig_m,
    wn.weather_observed_at_utc
FROM pos_latest p
LEFT JOIN mart.vessel_identity    vi ON vi.callsgn = p.callsgn
LEFT JOIN mart.port_call_overview pco ON pco.callsgn = p.callsgn
LEFT JOIN msds_by_vessel          mv ON mv.callsgn = p.callsgn
LEFT JOIN mart.weather_now        wn ON TRUE;
