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
-- ===========================================================================
-- 키 체계 (2026-08 MMSI-First 전환 이후) — ★ 여기가 자주 혼동된다
-- ===========================================================================
-- 키는 "고유키"와 "조인키"가 서로 다른 역할을 한다. 하나로 착각하면 안 된다.
--
--   [고유키 / 식별키]  vessel_uid  — "이 배가 그 배인가"
--       COALESCE(mmsi::text, 'CS:'||upper(btrim(callsgn)))
--       결측 0%인 MMSI 가 1순위. 우리 시스템이 선박을 세는 단위.
--       AIS 동적신호(Msg 1/2/3/18)에 실려오므로 위치가 있으면 항상 있다.
--       → mart.vessel_identity, mart.vessel_latest_position 의 파티션 키
--
--   [조인키 / 연계키]  callsgn     — "이 배의 서류가 어느 것인가"
--       PORT-MIS 입출항신고 · UPA 화물 manifest · 하역기록은 모두 호출부호로
--       발급된다. MMSI 로는 이 서류들을 찾을 수 없다 (원천에 MMSI 컬럼 자체가
--       없다). 따라서 서류 계열 조인은 앞으로도 callsgn 이다.
--       AIS 정적신호(Msg 5/24)에 실려오므로 미송출 선박은 결측(약 31%).
--
--   [표시키]           vessel_key  — 화면·로그용 대표 식별자
--       imo → mmsi → callsgn 우선순위로 고른 사람이 읽기 좋은 값.
--       조인에 쓰지 말 것 (우선순위가 바뀌면 값이 바뀐다).
--
--   그 외 도메인 조인키:
--       dg_un_no         — 화물 ↔ MSDS 위험물
--       bl_no            — 하역 ↔ 화물
--       facility_name    — 입항 ↔ 선석 제원(수심)
--       observed_at_utc  — 기상·조위 최신값
--
--   ★ 요약: 배를 "세는" 것은 vessel_uid, 배의 "서류를 찾는" 것은 callsgn.
--     그래서 dashboard_current 는 식별 조인만 vessel_uid 로 걸고
--     (관공선도 화면에 남기려고), 입항·화물 조인은 callsgn 을 유지한다.
--
-- 뷰 계층 (의존 순서대로 생성) — 각 단계의 조인키를 함께 표기:
--   1. mart.vessel_identity        선박 식별 마스터 (1척 = 1행)
--        위치 --vessel_uid--> 자기 자신 집계,  PORT-MIS --callsgn--> 선종
--   2. mart.vessel_latest_position 선박별 최신 위치 1행 (UPA 우선, AIS 보강)
--        UPA∪AIS --vessel_uid--> 최신 1행,  upa_port_call --callsgn--> 출항확정
--   3. mart.port_call_overview     입항 통합 (입항건당 대표 1행)
--        PORT-MIS ∙ UPA운항 ∙ 화물 전부 --callsgn-->
--   4. mart.cargo_msds             화물 ↔ MSDS (★안전관제 핵심)
--        화물 --dg_un_no--> MSDS
--   5. mart.weather_now            환경 최신 1행 (기상+조위+파고+조류 스냅샷)
--        --observed_at_utc--> 각 관측 최신값
--   5-1. mart.berth_draught_check  조위 반영 가용수심 · UKC 판정
--        입항 --facility_name--> 선석 수심,  입항 --callsgn--> 흘수
--   6. mart.dashboard_current      '한 줄 조회' — 대시보드·에이전트 진입점
--        위치 --vessel_uid--> 식별 / 위치 --callsgn--> 입항·화물 / 기상 CROSS
--   7. mart.pipeline_health        수집기 생존 신호 (조인 없음, 단일 집계행)
--   8. mart.berth_current_cargo    선석별 현재 취급 화물 → chem_id (백엔드 소비 계약)
--        재항선박 --callsgn--> 화물 --dg_un_no--> MSDS --> chem_id
--        ★ backend/app/agents/scheduling/category_map.py 의 카테고리 대표값
--          근사를 대체한다 (그 파일 주석이 이 뷰를 기다리고 있다)
--   (+ mart.msds_flat             msds_chemical JSONB 평탄화 — cargo_msds 가 사용)
-- ===========================================================================

CREATE SCHEMA IF NOT EXISTS mart;

-- ---------------------------------------------------------------------------
-- 0-A. 기존 뷰 제거 (재실행 안전성)
--
-- ★ 왜 DROP 이 필요한가 — CREATE OR REPLACE VIEW 의 제약
--   PostgreSQL 의 CREATE OR REPLACE VIEW 는 "기존 컬럼 목록 뒤에 새 컬럼을
--   덧붙이는" 변경만 허용한다. 컬럼을 중간에 끼워 넣거나, 순서·이름·타입을
--   바꾸면 다음과 같이 실패한다:
--       ERROR: cannot change name of view column "nav_status_code" to "heading"
--
--   실제로 그 사고가 났다 (2026-08-10, PR #23 리뷰에서 재현):
--   vessel_latest_position 에 heading/draught 를 sog·cog 옆(중간)에 추가했더니
--   신규 DB 에서는 통과하고 기존 DB 에서만 실패했다. 만든 사람은 신규 DB 라
--   못 보고, 받는 사람은 기존 DB 라 바로 깨지는 — 가장 늦게 발견되는 형태다.
--
--   해법으로 "새 컬럼은 항상 맨 뒤에 붙이기" 규칙을 지킬 수도 있지만,
--   그러면 cog 옆에 있어야 할 heading 이 파일 맨 끝에 홀로 떨어져 가독성이
--   나빠지고, 규칙을 한 번만 어겨도 같은 사고가 반복된다.
--   뷰는 데이터를 갖지 않으므로(정의만 있음) 지웠다 다시 만드는 비용이 0 이다.
--   구조적으로 막는 쪽을 택한다.
--
-- ★ CASCADE 를 쓰지 않는 이유
--   CASCADE 는 이 뷰에 의존하는 "우리가 모르는 객체"(백엔드가 만든 뷰 등)까지
--   조용히 같이 지운다. 여기서는 의존 역순으로 명시 삭제만 하고, 외부 의존이
--   있으면 에러로 드러나게 둔다 — 조용한 파괴보다 시끄러운 실패가 낫다.
--
-- ★ 삭제 순서 = 생성 역순 (의존하는 쪽을 먼저 지운다)
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS mart.berth_current_cargo;
DROP VIEW IF EXISTS mart.pipeline_health;
DROP VIEW IF EXISTS mart.dashboard_current;
DROP VIEW IF EXISTS mart.berth_draught_check;
DROP VIEW IF EXISTS mart.weather_now;
DROP VIEW IF EXISTS mart.cargo_msds;
DROP VIEW IF EXISTS mart.msds_flat;
DROP VIEW IF EXISTS mart.port_call_overview;
DROP VIEW IF EXISTS mart.vessel_latest_position;
DROP VIEW IF EXISTS mart.vessel_identity;

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
    -- ★ record_uid 는 upa_loader.TABLE_MAP 이 이 테이블의 UPSERT 자연키로 쓴다.
    --   (bl_no 등 자연키 후보가 결측 가능해 전체 행 해시를 키로 쓰는 예외 테이블)
    --   여기 껍데기에 이 컬럼이 빠져 있으면, 뷰가 먼저 테이블을 만든 환경에서
    --   로더가 "column record_uid does not exist" 로 죽는다 (실증 확인).
    --   스키마 소유권이 Alembic / 파이프라인 auto_create / 이 파일 3곳으로
    --   갈라져 있어 생기는 문제다. 일원화 전까지는 여기서 맞춰 둔다.
    record_uid                   text,
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
--
--    [MMSI-First + Stateful Filling]  (callsgn 결측 31% 대응)
--    UPA getVslPstnInfo 는 AIS 동적신호(위치)를 기반으로 하고, callsgn 은
--    정적신호(Msg Type 5)에서 온다. 정적신호는 6분 주기라 스냅샷에 따라
--    비거나(간헐 결측), 아예 송출하지 않는 선박(관공선·소방정·순찰선·예부선
--    등 150척)도 있다. 따라서:
--
--      (a) 파티션 키를 callsgn 이 아니라 MMSI 로 둔다 → callsgn 이 한 번도
--          없는 선박도 뷰에서 사라지지 않는다 (MMSI-First).
--      (b) callsgn/IMO/선명은 "그 MMSI 의 가장 최근 non-null 값"을 끌어와
--          채운다 (Stateful Filling / Last Known Position).
--          first_value(...) OVER (PARTITION BY vessel_uid
--                                 ORDER BY (x IS NOT NULL) DESC, received_at_utc DESC)
--          → boolean 은 false < true 이므로 DESC 를 주면 non-null 행이 먼저
--            오고, 그중 가장 최신 행의 값이 선택된다.
--      (c) callsgn_source 로 출처를 표기한다:
--            'current' = 최신 관측에 실제로 실려온 값
--            'filled'  = 과거 신호에서 끌어온 값 (정적신호 일시 누락)
--            NULL      = 한 번도 받은 적 없음 (MMSI 로만 식별)
--
--    ★ 주의: 여기서 앞으로 끌어오는 것은 "정적 식별정보"(호출부호·IMO·선명)
--      뿐이다. 위치(위경도·SOG·COG)는 절대 채워 넣지 않는다. 떠난 배가 옛
--      좌표로 남아 있으면 안전관제상 오히려 위험하다. 위치는 계속
--      mart.vessel_latest_position 이 관측된 시각 그대로 보여준다.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.vessel_identity AS
WITH pos_src AS (
    SELECT
        -- 선박 고유키: MMSI 우선, MMSI 조차 없으면 callsgn 으로 대체
        COALESCE(mmsi::text, 'CS:' || upper(btrim(callsgn)))  AS vessel_uid,
        mmsi,
        nullif(upper(btrim(callsgn)), '')                     AS callsgn,
        imo_no::bigint                                        AS imo_no,
        nullif(btrim(vessel_name), '')                        AS vessel_name,
        received_at_utc
    FROM upa_vessel_position
    WHERE mmsi IS NOT NULL
       OR nullif(btrim(callsgn), '') IS NOT NULL
),
pos_filled AS (
    SELECT
        vessel_uid,
        mmsi,
        received_at_utc,
        callsgn                                        AS callsgn_observed,
        first_value(callsgn)     OVER w_callsgn        AS callsgn_lkp,
        first_value(imo_no)      OVER w_imo            AS imo_no_lkp,
        first_value(vessel_name) OVER w_name           AS vessel_name_lkp
    FROM pos_src
    WINDOW
        w_callsgn AS (PARTITION BY vessel_uid
                      ORDER BY (callsgn     IS NOT NULL) DESC, received_at_utc DESC),
        w_imo     AS (PARTITION BY vessel_uid
                      ORDER BY (imo_no      IS NOT NULL) DESC, received_at_utc DESC),
        w_name    AS (PARTITION BY vessel_uid
                      ORDER BY (vessel_name IS NOT NULL) DESC, received_at_utc DESC)
),
pos_latest AS (
    -- vessel_uid 당 1행: 시각은 "최신 관측 시각", 식별정보는 위에서 채운 값
    SELECT DISTINCT ON (vessel_uid)
           vessel_uid,
           callsgn_lkp                     AS callsgn,
           mmsi,
           imo_no_lkp                      AS imo_no,
           vessel_name_lkp                 AS vessel_name,
           received_at_utc                 AS last_position_at_utc,
           CASE
               WHEN callsgn_observed IS NOT NULL THEN 'current'
               WHEN callsgn_lkp      IS NOT NULL THEN 'filled'
               ELSE NULL
           END                             AS callsgn_source
    FROM pos_filled
    ORDER BY vessel_uid, received_at_utc DESC
),
pm_latest AS (
    SELECT DISTINCT ON (upper(btrim(callsgn)))
           upper(btrim(callsgn))           AS callsgn,
           vessel_name                     AS vessel_name_portmis,
           nationality_cd,
           nationality_nm,
           ship_kind_cd,
           ship_kind_nm,
           ship_kind_category,
           is_liquid_cargo_vessel,
           port_agency_label
    FROM portmis_vessel
    WHERE nullif(btrim(callsgn), '') IS NOT NULL
    ORDER BY upper(btrim(callsgn)), collected_at_utc DESC
),
base AS (
    -- ① 위치 신호가 있는 선박 (callsgn 유무와 무관하게 전부 포함)
    SELECT vessel_uid, callsgn, mmsi, imo_no, vessel_name,
           last_position_at_utc, callsgn_source, TRUE AS has_position
    FROM pos_latest

    UNION ALL

    -- ② PORT-MIS 신고만 있고 아직 항내 위치가 안 잡힌 선박 (입항 예정 등)
    SELECT 'CS:' || m.callsgn, m.callsgn, NULL::bigint, NULL::bigint, NULL::text,
           NULL::timestamptz, 'portmis'::text, FALSE
    FROM pm_latest m
    WHERE NOT EXISTS (SELECT 1 FROM pos_latest p WHERE p.callsgn = m.callsgn)
)
SELECT
    -- 식별 우선순위: imo → mmsi → callsgn
    COALESCE(b.imo_no::text, b.mmsi::text, b.callsgn)   AS vessel_key,
    b.callsgn,
    b.imo_no,
    b.mmsi,
    COALESCE(b.vessel_name, m.vessel_name_portmis)      AS vessel_name,
    m.nationality_cd,
    m.nationality_nm,
    m.ship_kind_cd,
    m.ship_kind_nm,
    m.ship_kind_category,
    m.is_liquid_cargo_vessel,
    m.port_agency_label,
    b.last_position_at_utc,
    b.has_position,
    (m.callsgn IS NOT NULL)                             AS has_portmis,
    -- ↓ 신규 컬럼 (CREATE OR REPLACE 제약상 반드시 맨 뒤에 추가할 것)
    b.callsgn_source,
    b.vessel_uid,
    -- -----------------------------------------------------------------------
    -- identity_confidence — 식별 신뢰도
    --
    -- ★ "정적신호가 없는 선박 = 안전한 선박" 이 아니다.
    --   울산·온산 같은 액체화물 허브에는 대형 유조선 옆에 붙어 화물을 옮겨 싣는
    --   소형 급유선(bunker vessel), 액체 부선(barge), 300GT 미만 연안 케미칼선이
    --   상시 움직인다. 이들은 Class B 를 쓰거나 AIS 의무 대상이 아니어서 선종이
    --   확인되지 않는다. 이 선박들을 "액체화물선 아님"으로 단정해 목록에서
    --   지워버리면, 실제로는 위험물을 실은 배가 관제 화면에서 사라진다.
    --
    --   그래서 판정을 "액체/비액체" 2값이 아니라 아래 3상태로 남긴다.
    --   소비 계층(대시보드·에이전트)은 UNIDENTIFIED 를 '안전'이 아니라
    --   '미확인 위험'으로 취급하고, 관제사에게 VTS 교신 확인을 유도해야 한다.
    --
    --     CONFIRMED    PORT-MIS 입출항신고로 선종이 확정됨 (정본)
    --     PARTIAL      신고는 없으나 정적신호(callsgn/IMO)로 식별은 됨
    --     UNIDENTIFIED 정적신호 수신 이력 자체가 없음 — MMSI 만 아는 상태
    -- -----------------------------------------------------------------------
    CASE
        WHEN m.callsgn IS NOT NULL                     THEN 'CONFIRMED'
        WHEN b.callsgn IS NOT NULL OR b.imo_no IS NOT NULL THEN 'PARTIAL'
        ELSE 'UNIDENTIFIED'
    END                                                 AS identity_confidence
FROM base b
LEFT JOIN pm_latest m ON m.callsgn = b.callsgn;

-- ---------------------------------------------------------------------------
-- 2. mart.vessel_latest_position — 선박별 최신 위치 1행
--    UPA 항내 선박위치(기본 소스)를 우선하되, 레거시 AIS 관측이 더 최신이면
--    그것을 쓴다 (mmsi → vessel_identity 로 callsgn 역매핑). position_source
--    컬럼으로 어느 소스의 관측인지 표시한다.
--
--    [MMSI-First] 예전에는 callsgn 없는 UPA 관측을 통째로 버려서 관공선·
--    소방정·순찰선 등이 지도에서 사라졌다. 이제 MMSI 를 1차 키로 쓰고
--    callsgn 은 vessel_identity 에서 역매핑해 보강만 한다.
--    ★ 위치값 자체는 Stateful Filling 대상이 아니다. 신호가 끊긴 배는 마지막
--      관측 시각(received_at_utc)과 함께 그대로 남고, 좌표를 만들어내지
--      않는다. 소비 측은 now() - received_at_utc 로 신선도를 판단할 것.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.vessel_latest_position AS
WITH unified AS (
    SELECT COALESCE(mmsi::text, 'CS:' || upper(btrim(callsgn))) AS vessel_uid,
           nullif(upper(btrim(callsgn)), '')  AS callsgn,
           mmsi,
           latitude,
           longitude,
           sog,
           cog,
           heading,
           draught,
           nav_status_code::text      AS nav_status_code,
           received_at_utc,
           quality_flag,
           'UPA'                      AS position_source
    FROM upa_vessel_position
    WHERE mmsi IS NOT NULL
       OR nullif(btrim(callsgn), '') IS NOT NULL

    UNION ALL

    SELECT a.mmsi::text,
           vi.callsgn,
           a.mmsi,
           a.latitude,
           a.longitude,
           a.sog,
           a.cog,
           -- AIS(레거시 웹소켓) 응답에는 heading/draught 가 없다(ais_vessel_position
           -- 테이블 자체에 컬럼 없음, Alembic 0002 참조). UPA 소스가 기본이라
           -- 실사용에 지장 없지만, 값 없음과 0 을 혼동하지 않도록 명시적으로 NULL.
           NULL::double precision     AS heading,
           NULL::double precision     AS draught,
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
    WHERE a.mmsi IS NOT NULL
),
-- 서류상 재항 여부 — 출항은 "추정"하지 않고 upa_port_call 로 "확정"한다.
-- departure_at_utc 가 NULL 인 최신 입항 건이 있으면 아직 항내에 있다는 신고 상태.
still_in_port AS (
    SELECT DISTINCT upper(btrim(callsgn)) AS callsgn
    FROM upa_port_call
    WHERE nullif(btrim(callsgn), '') IS NOT NULL
      AND departure_at_utc IS NULL
),
departed_doc AS (
    SELECT upper(btrim(callsgn)) AS callsgn, max(departure_at_utc) AS departed_at_utc
    FROM upa_port_call
    WHERE nullif(btrim(callsgn), '') IS NOT NULL
      AND departure_at_utc IS NOT NULL
    GROUP BY 1
),
latest AS (
    SELECT DISTINCT ON (u.vessel_uid)
           u.vessel_uid                          AS vessel_pos_key,
           -- 관측에 callsgn 이 안 실렸으면 vessel_identity 의 Last Known 값으로 보강
           COALESCE(u.callsgn, vi2.callsgn)      AS callsgn,
           u.mmsi,
           u.latitude,
           u.longitude,
           u.sog,
           u.cog,
           u.heading,
           u.draught,
           u.nav_status_code,
           u.received_at_utc,
           u.quality_flag,
           u.position_source
    FROM unified u
    LEFT JOIN mart.vessel_identity vi2 ON vi2.vessel_uid = u.vessel_uid
    ORDER BY u.vessel_uid, u.received_at_utc DESC
)
SELECT l.*,
       -- ↓ 신규 컬럼 (CREATE OR REPLACE 제약상 반드시 맨 뒤에 추가할 것)
       -- -----------------------------------------------------------------------
       -- 원시 신선도. 임계값을 박아 넣지 않은 값이라 소비자가 각자 기준을
       -- 적용할 수 있다 (지도는 30분, 안전 에이전트는 더 엄격하게).
       -- -----------------------------------------------------------------------
       round(EXTRACT(EPOCH FROM (now() - l.received_at_utc)) / 60.0)::int
                                                 AS position_age_min,
       -- -----------------------------------------------------------------------
       -- presence_state — 화면 표시용 존재 상태
       --
       -- ★ "신호가 없다"와 "배가 떠났다"는 다른 사건이다.
       --   6시간 침묵의 원인은 실제 출항 / 접안 중 AIS 차단 / 트랜스폰더 고장 /
       --   구조물 전파음영 / 우리 수집기 장애 최소 5가지다. 이걸 전부
       --   DEPARTED 로 부르고 화면에서 지우면, 트랜스폰더가 죽은 채 선석에
       --   붙어 있는 배가 지도에서 사라진다. 안전관제에서 이건 결함이다.
       --
       --   그래서 출항은 신호 부재로 추론하지 않고 upa_port_call 의
       --   departure_at_utc(서류상 확정 정보)로만 판정한다.
       --
       --     PRESENT    30분 이내 관측 — 정상 표시
       --     STALE      30분~6시간 침묵 — 회색 반투명, "N분 전" 배지
       --     NO_SIGNAL  6시간 초과 침묵 + 출항 기록 없음
       --                → ★숨기지 말 것. 별도 목록으로 관제사에게 노출.
       --                  (서류상 재항 + 신호 소실 = 전화를 걸어야 하는 상황)
       --     DEPARTED   출항 신고 확정 — 지도에서 제외해도 안전
       --
       --   임계값 30분/6시간은 현재 실측 근거가 없는 잠정값이다.
       --   mart_views_check.sql 의 관측간격 분위수 쿼리로 재산정할 것.
       -- -----------------------------------------------------------------------
       CASE
           WHEN d.departed_at_utc IS NOT NULL
                AND d.departed_at_utc >= l.received_at_utc      THEN 'DEPARTED'
           WHEN l.received_at_utc > now() - interval '30 min'    THEN 'PRESENT'
           WHEN l.received_at_utc > now() - interval '6 hour'    THEN 'STALE'
           ELSE 'NO_SIGNAL'
       END                                       AS presence_state,
       -- 서류상 아직 항내인가 (출항신고 없는 입항 건 보유)
       (s.callsgn IS NOT NULL)                   AS doc_still_in_port,
       d.departed_at_utc,
       -- -----------------------------------------------------------------------
       -- signal_health — 안전 에이전트용. 지도 표시보다 엄격하다.
       -- 하역 중인 배가 12분(6분 폴링 2회분) 침묵하면 이미 이상 상황이다.
       -- 지도 기준(30분)으로는 그 29분 동안 정상 아이콘으로 보인다.
       -- -----------------------------------------------------------------------
       CASE
           WHEN l.received_at_utc > now() - interval '12 min'    THEN 'OK'
           WHEN l.received_at_utc > now() - interval '30 min'    THEN 'DEGRADED'
           ELSE 'LOST'
       END                                       AS signal_health
FROM latest l
LEFT JOIN still_in_port s ON s.callsgn = l.callsgn
LEFT JOIN departed_doc  d ON d.callsgn = l.callsgn;

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
-- 3-1. mart.msds_flat — msds_chemical(JSONB) → 안전관제용 평면 뷰
--
--   스키마 합의(2026-07): msds_chemical 은 KOSHA 원본 16개 섹션을
--   msds_payload(JSONB)에 무손실 보존한다(로더 설계 원칙 유지). 대신 소비자
--   (mart 뷰·백엔드 API·LLM 에이전트)가 각자 JSONB 경로를 파고들면 파싱 규칙이
--   곳곳에 중복·분기되므로, **평탄화 규칙의 단일 정본**을 이 뷰 한 곳에 둔다.
--   → 원본 보존(JSONB)과 소비 편의(평면 컬럼)를 둘 다 취하는 구조.
--   → 필요한 항목이 늘면 이 뷰만 고치면 된다 (Alembic 마이그레이션 불필요).
--
--   추출 키는 KOSHA 가 부여하는 msdsItemCode(안정 코드)를 쓴다 — 한글 라벨이
--   바뀌어도 깨지지 않는다.
--     I14=인화점  I32=자연발화온도  I26=증기밀도
--     N02=UN번호  N06=운송위험성등급(IMDG)  N08=용기등급
--     B02=유해성분류  B0402=그림문자  B0404=신호어  B0406=유해·위험문구(H)
--
--   전제: msds_chemical 테이블은 backend(Alembic) 소유이며 이미 존재한다.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.msds_flat AS
WITH item AS (
    SELECT m.chem_id,
           it->>'msdsItemCode' AS item_code,
           it->>'itemDetail'   AS item_detail
    FROM msds_chemical m
    CROSS JOIN LATERAL jsonb_each(m.msds_payload) AS d(key, val)
    CROSS JOIN LATERAL jsonb_array_elements(
        CASE WHEN jsonb_typeof(d.val->'data') = 'array'
             THEN d.val->'data' ELSE '[]'::jsonb END) AS it
    WHERE d.key LIKE 'detail%'
),
clean AS (
    -- HTML 이스케이프 복원 + "값 없음"을 뜻하는 표기를 전부 NULL 로 정규화.
    --
    -- KOSHA MSDS 는 값이 없을 때 표기가 항목마다 다르다(실측 34종 기준):
    --   '자료없음'  — 인화점(I14) 등에서 사용
    --   '해당없음'  — IMDG 등급(N06) 4건, 용기등급(N08) 8건. 비위험물이라
    --                 해당 분류 자체가 없다는 뜻이므로 NULL 이 맞다.
    --   '-'         — 용기등급(N08) 6건. 가스류처럼 용기등급 개념이 없는 경우.
    -- 이걸 문자열로 두면 imdg_class='해당없음' 같은 값이 소비자(백엔드·LLM)에게
    -- 실제 등급처럼 보이고, 격리 판정 조인에서도 의미 없는 키가 된다.
    SELECT chem_id, item_code,
           nullif(btrim(replace(replace(replace(replace(
               item_detail, '&lt;', '<'), '&gt;', '>'), '&amp;', '&'), '&quot;', '"')),
               '') AS v0
    FROM item
),
clean2 AS (
    SELECT chem_id, item_code,
           CASE WHEN v0 IN ('자료없음', '자료 없음', '해당없음', '해당 없음',
                            '정보없음', '-', '–', 'N/A')
                THEN NULL ELSE v0 END AS v
    FROM clean
),
pick AS (
    SELECT chem_id,
           max(v) FILTER (WHERE item_code = 'I14')   AS flash_point_raw,
           max(v) FILTER (WHERE item_code = 'I32')   AS autoignition_raw,
           max(v) FILTER (WHERE item_code = 'I26')   AS vapor_density_raw,
           max(v) FILTER (WHERE item_code = 'N02')   AS un_no_msds,
           max(v) FILTER (WHERE item_code = 'N06')   AS imdg_class,
           max(v) FILTER (WHERE item_code = 'N08')   AS packing_group,
           max(v) FILTER (WHERE item_code = 'B02')   AS ghs_hazard,
           max(v) FILTER (WHERE item_code = 'B0402') AS ghs_pictogram,
           max(v) FILTER (WHERE item_code = 'B0404') AS signal_word,
           max(v) FILTER (WHERE item_code = 'B0406') AS h_statements
    FROM clean2 GROUP BY chem_id
)
SELECT
    m.chem_id,
    m.cas_no,
    m.name_ko,
    m.name_en,
    -- UN 번호: 로더 컬럼 우선, 비어 있으면 payload(N02)로 보강
    COALESCE(nullif(btrim(m.un_no), ''), p.un_no_msds)                  AS dg_un_no,
    p.flash_point_raw,
    -- 인화점 숫자화: '℃' 앞부분만 잘라(뒤쪽 압력값 오인 방지) 첫 실수를 취한다.
    --   '-37 ℃|※출처'           → -37
    --   '11.11 ℃|※출처'         → 11.11
    --   '< -40 ℃ (ca. 101.325)'  → -40   (℃ 앞만 보므로 101.325 무시)
    --   '58~66 ℃'                → 58    (범위는 하한 = 보수적, 낮을수록 위험)
    --   '<  ℃ (c.c.)' / '(인화성 가스)' → NULL (숫자 없음)
    (regexp_match(split_part(p.flash_point_raw, '℃', 1),
                  '(-?[0-9]+(?:\.[0-9]+)?)'))[1]::numeric              AS flash_point_celsius,
    p.imdg_class,
    p.packing_group,
    p.signal_word,
    p.h_statements,
    p.ghs_pictogram,
    p.ghs_hazard,
    p.autoignition_raw,
    p.vapor_density_raw
FROM msds_chemical m
LEFT JOIN pick p ON p.chem_id = m.chem_id;

-- ---------------------------------------------------------------------------
-- 4. mart.cargo_msds — 화물 ↔ MSDS 위험물 (★안전관제 핵심, 화물 행 단위)
--    UPA 화물 manifest 의 dg_un_no ↔ mart.msds_flat.dg_un_no 조인으로
--    화물 1건마다 인화점·IMDG 등급·GHS 정보를 붙인다. UN 번호가 없는 화물은
--    msds_matched=false 로 남는다 (일반 화물 or 코드 미기재 — 후자는 bzentyCd
--    확보로 통합화물 API 조회가 가능해지면 자동으로 채워진다).
--    ※ msds_chemical 을 직접 읽지 않고 mart.msds_flat 을 경유한다 — JSONB
--      평탄화 규칙을 한 곳에서만 관리하기 위함 (3-1 참고).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.cargo_msds AS
SELECT
    upper(trim(cm.callsgn))        AS callsgn,
    cm.bl_no,
    -- 접안 시설명 — 혼재금지(IMDG 격리) 판정에 필수.
    -- "인접 선석에서 비혼재 등급을 동시 취급 중인가"를 판정하려면 화물이 어느
    -- 부두에 있는지 알아야 한다 (Neo4j ADJACENT_TO 선석쌍과 대조).
    cm.facility_name,
    -- 포장·하역방식 — 용기등급 대비 적정성 판정용
    -- (예: 용기등급 Ⅰ 화물을 일반 드럼·크레인으로 신고한 경우)
    cm.cargo_se_name,
    cm.package_type_name,
    cm.unload_method_name,
    cm.cargo_name_raw,
    cm.dg_un_no,
    ms.chem_id,
    ms.cas_no,
    ms.name_ko                     AS msds_name_ko,
    ms.flash_point_celsius,
    ms.imdg_class,
    ms.packing_group,
    ms.signal_word,
    ms.ghs_hazard,
    ms.h_statements,
    (ms.chem_id IS NOT NULL)       AS msds_matched,
    -- 합성 데이터 여부를 반드시 노출한다. bzentyCd 미확보로 화물은 당분간
    -- 합성 샘플을 쓰는데, 뷰에서 이 플래그를 감추면 소비자(백엔드·대시보드·
    -- LLM)가 실데이터로 오인한다. 뷰에서 걸러내지 않는 이유는 시연에 합성
    -- 데이터가 필요하기 때문 — 걸러내는 대신 "표시"해서 판단을 넘긴다.
    cm.is_synthetic,
    cm.cargo_basis
FROM upa_cargo_manifest cm
LEFT JOIN mart.msds_flat ms
  ON cm.dg_un_no IS NOT NULL
 -- UN 번호 정규화 후 조인.
 -- UN 번호는 국제 표준상 항상 4자리 숫자(0004~3548)라, 표기가 어떻게 오든
 -- 첫 4자리 숫자만 뽑으면 안전하게 정규화된다. 실무 표기 편차 실측:
 --   "1972" / "UN1972" / "un1972" / "UN 1972" / "UN-1972" / "(UN1972)"
 --   "1972.0" / "1972.00"  ← 숫자형으로 적재됐다가 문자열화된 잔재
 -- 이전 방식('^UN|\.0$' 치환)은 공백·괄호·하이픈·소수점 2자리를 놓쳤다.
 -- (숫자만 남기는 '[^0-9]' 방식은 "1972.0"을 "19720"으로 만들어 쓸 수 없다.)
 AND nullif((regexp_match(ms.dg_un_no::text, '([0-9]{4})'))[1], '')
   = nullif((regexp_match(cm.dg_un_no::text, '([0-9]{4})'))[1], '');

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
    v.wave_dir_deg,
    -- ↓ 실무 반영 추가 (CREATE OR REPLACE 제약상 맨 뒤에 붙인다)
    --   돌풍(gust): 계류삭 장력은 평균풍속이 아니라 순간최대풍속에 끊어진다.
    --   조류(current): 액체부두 접·이안에서 조류는 풍속만큼 중요한 제약이다.
    --     특히 울산 본항·온산 수로는 창·낙조류 방향이 접안 조종에 직접 영향.
    t.gust_ms,
    t.current_speed_cms,
    t.current_dir_deg
FROM (SELECT 1) AS anchor
-- "가장 최근 행" 이 아니라 "그 지표가 실제로 관측된 가장 최근 행" 을 쓴다.
--
-- 원천(MMAF openWeatherNow, 울산항동방파제서단등대)이 관측값 없이 시각만 있는 행을
-- 계속 보낼 때가 있다. 2026-08-11 실측: 최근 167행 중 풍속이 있는 행은 20건뿐이고
-- 08-10 12:40 이후로는 전부 비어 있었다. 최신 행만 집으면 풍속이 NULL 이 되고,
-- 화면(mock-server)은 그걸 "실데이터 없음" 으로 보고 mock 기상으로 넘어간다 —
-- 즉 실관측이 있는데도 대시보드가 가짜 기상을 띄우게 된다.
--
-- 값이 오래됐다는 사실 자체는 숨기지 않는다: 각 observed_at_utc 가 그 값이 실제로
-- 측정된 시각이므로, 화면은 그걸로 신선도(is_stale)를 그대로 판단할 수 있다.
LEFT JOIN (SELECT * FROM weather_obs WHERE wind_speed_ms IS NOT NULL
           ORDER BY observed_at_utc DESC LIMIT 1) w ON TRUE
LEFT JOIN (SELECT * FROM tide_obs WHERE tide_level_cm IS NOT NULL
           ORDER BY observed_at_utc DESC LIMIT 1) t ON TRUE
LEFT JOIN (SELECT * FROM wave_obs WHERE wave_height_sig_m IS NOT NULL
           ORDER BY observed_at_utc DESC LIMIT 1) v ON TRUE;

-- ---------------------------------------------------------------------------
-- 5-1. mart.berth_draught_check — 조위 반영 가용수심 · UKC 판정
--
-- [왜 필요한가 — 실무와 어긋나던 부분]
--   기존 흘수 판정은 "선박 흘수 < 부두 수심" 이었다. 항만 실무는 그렇지 않다.
--
--   (1) 부두 명세의 수심은 **해도기준면(Chart Datum) 기준**이다. 해도기준면은
--       약최저저조위이므로, 실제 그 시각의 가용수심은 항상 그보다 크다.
--           가용수심 = 해도수심 + 그 시각 조위
--       조위를 빼먹으면 "만조 때만 접안 가능한 배"를 영구 접안불가로 오판한다.
--       실제로 대형 유조선은 이 조위 여유에 맞춰 접안 시각을 잡는다.
--
--   (2) 흘수와 가용수심이 같으면 되는 게 아니다. 선체 아래에 **UKC(Under Keel
--       Clearance, 용골하 여유수심)** 가 남아야 한다. 항주파·선체 침하(squat)·
--       해저 기복·측심 오차를 흡수하는 여유다.
--
--   ※ UKC 요구치는 법정 수치가 아니다. 여기서는 통상 관행값(흘수의 10%)을
--     기본으로 두되, 터미널·항만별로 다르므로 파라미터로 분리해 두었다.
--     울산항 각 터미널 운영규정을 입수해 확정해야 한다(미확보 — 한계로 보고).
--
--   ※ AIS drft(최대정적흘수)는 선원이 입력하는 값이며 트림(선수·선미 흘수 차)을
--     반영하지 않는다. 실제 최대흘수는 이보다 클 수 있다 — 보수적으로 볼 것.
--
-- 판정:
--   OK           UKC 여유 충족
--   MARGINAL     양수이나 요구 UKC 미달 — 조위창(tidal window) 조정 필요
--   NOT_ALLOWED  가용수심 ≤ 흘수 — 착저 위험, 접안 불가
--   UNKNOWN      흘수 또는 부두 수심 미상
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.berth_draught_check AS
WITH tide AS (
    SELECT tide_level_cm, observed_at_utc
    FROM tide_obs ORDER BY observed_at_utc DESC LIMIT 1
),
berth AS (
    -- -----------------------------------------------------------------------
    -- 울산항 부두 제원 (해도기준면 수심 m)
    --
    -- 출처: 울산지방해양수산청 「울산항시설현황」 (본항 15부두·부이 2기 /
    --       온산항 12부두·부이 3기 / 울산신항 6부두 — 총 35부두 67선석)
    --       원본: data/seed/ulsan_berth_spec_seed.csv (선석수·안벽길이·DWT 포함)
    --
    -- ★ 안전측 최소값 원칙
    --   같은 부두명에 선석별 수심이 다른 경우가 있다. 우리 데이터(upa_port_call)
    --   는 부두명까지만 알고 몇 번 선석인지는 모르므로, 가장 얕은 수심을 쓴다.
    --   깊은 쪽을 쓰면 실제로는 착저인 배를 OK 로 오판할 수 있다.
    --     SK2부두 : 중력식 7.5m(1선석) + 잔교식 8m(4선석) → 7.5 적용
    --     SK5부두 : 원문 '7-11' 범위(5선석)              → 7   적용
    --
    -- ★ 제외 대상
    --   부이(SK부이II·III, S-Oil 부이, 석유공사부이 등 수심 27m)는 접안이 아니라
    --   해상 계류라 안벽 UKC 개념이 다르다. 여기 목록에서 뺀다.
    --   3부두는 원문 수심 표기가 '9,12' 로 모호해(9m/12m 인지 9.12m 인지) 제외.
    --   → 목록에 없는 부두는 chart_depth_m NULL → draught_verdict 'UNKNOWN' 이 된다.
    --     "모르는 것을 안전으로 간주하지 않는다"는 이 프로젝트 원칙과 같다.
    -- -----------------------------------------------------------------------
    SELECT * FROM (VALUES
        -- 본항
        ('4부두',        11.0), ('6부두',        12.0), ('용잠부두',      7.0),
        ('가스부두',      7.5), ('UTT부두',      11.0),
        ('SK1부두',       7.5), ('SK2부두',       7.5), ('SK3부두',      12.0),
        ('SK4부두',      10.0), ('SK5부두',       7.0), ('SK6부두',      15.0),
        ('SK7부두',      15.0), ('SK8부두',      18.0),
        -- 온산항
        ('효성부두',     12.0), ('달포부두',      7.0), ('UTK부두',      12.0),
        ('대한유화부두', 12.0), ('OTK1부두',     11.0), ('OTK2부두',      9.0),
        ('S-Oil 1부두',  11.0), ('S-Oil 2부두',  15.5), ('S-Oil 3부두',  14.0),
        ('S-Oil 4부두',  12.0), ('정일1부두',    11.0), ('정일2부두',    12.5),
        -- 울산신항
        ('정일스톨트헤븐 신항 3~5부두', 14.0),
        ('현대오일터미널 신항부두',     14.0),
        ('LS니꼬 신항부두',             14.0),
        ('UTK 신항부두',                14.0)
    ) AS t(facility_name, chart_depth_m)
),
vessel AS (
    SELECT DISTINCT ON (upper(btrim(callsgn)))
           upper(btrim(callsgn)) AS callsgn, draught, received_at_utc
    FROM upa_vessel_position
    WHERE nullif(btrim(callsgn), '') IS NOT NULL AND draught IS NOT NULL
    ORDER BY upper(btrim(callsgn)), received_at_utc DESC
),
pc AS (
    -- 선석 배정은 UPA 운항관제(upa_port_call)에 있다. port_call_overview 는
    -- PORT-MIS 신고를 기준행으로 삼으므로, 신고가 아직 없는 배(접안은 했으나
    -- 신고 미연계)가 빠진다. 흘수 판정은 접안 사실만 있으면 해야 하므로
    -- 여기서는 upa_port_call 을 직접 본다.
    SELECT DISTINCT ON (upper(btrim(callsgn)))
           upper(btrim(callsgn)) AS callsgn, facility_name, arrival_at_utc
    FROM upa_port_call
    WHERE nullif(btrim(callsgn), '') IS NOT NULL
      AND departure_at_utc IS NULL          -- 아직 접안 중인 건만
    ORDER BY upper(btrim(callsgn)), arrival_at_utc DESC
)
SELECT
    pc.callsgn,
    pc.facility_name,
    b.chart_depth_m,
    (SELECT tide_level_cm FROM tide) / 100.0            AS tide_level_m,
    b.chart_depth_m + COALESCE((SELECT tide_level_cm FROM tide), 0) / 100.0
                                                        AS available_depth_m,
    v.draught                                           AS vessel_draught_m,
    round((b.chart_depth_m + COALESCE((SELECT tide_level_cm FROM tide), 0) / 100.0
           - v.draught)::numeric, 2)                    AS ukc_m,
    round((v.draught * 0.10)::numeric, 2)               AS ukc_required_m,
    CASE
        WHEN v.draught IS NULL OR b.chart_depth_m IS NULL THEN 'UNKNOWN'
        WHEN (b.chart_depth_m + COALESCE((SELECT tide_level_cm FROM tide), 0) / 100.0)
             <= v.draught                                THEN 'NOT_ALLOWED'
        WHEN (b.chart_depth_m + COALESCE((SELECT tide_level_cm FROM tide), 0) / 100.0
              - v.draught) < v.draught * 0.10            THEN 'MARGINAL'
        ELSE 'OK'
    END                                                 AS draught_verdict,
    (SELECT observed_at_utc FROM tide)                  AS tide_observed_at_utc,
    v.received_at_utc                                   AS draught_observed_at_utc,
    pc.arrival_at_utc
FROM pc
-- ★ LEFT JOIN 이어야 한다. INNER JOIN 이면 위 berth 목록에 없는 부두(제원 미확보,
--   부이, 신규 부두)에 접안한 선박이 판정 결과에서 통째로 사라진다. 그러면
--   "위험하지 않다"가 아니라 "아예 안 보인다"가 되어 UNIDENTIFIED·NO_SIGNAL 을
--   살려둔 이 프로젝트 원칙과 정면으로 어긋난다.
--   목록에 없으면 chart_depth_m 이 NULL 이 되고 draught_verdict 는 'UNKNOWN' 이다.
LEFT JOIN berth  b ON b.facility_name = pc.facility_name
LEFT JOIN vessel v ON v.callsgn = pc.callsgn;

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
    -- vessel_identity 에 아직 없는 선박(레거시 AIS 관측만 있는 경우)도 최소한
    -- 위치 소스가 들고 있는 MMSI/키는 살려 둔다 — NULL 행이 생기지 않게.
    COALESCE(vi.vessel_key, p.vessel_pos_key)           AS vessel_key,
    p.callsgn,
    vi.imo_no,
    COALESCE(vi.mmsi, p.mmsi)                           AS mmsi,
    vi.vessel_name,
    vi.ship_kind_nm,
    vi.is_liquid_cargo_vessel,
    vi.nationality_nm,
    -- [최신 위치·상태]
    p.latitude,
    p.longitude,
    p.sog,
    p.cog,
    p.heading,
    p.draught,
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
    wn.weather_observed_at_utc,
    -- ↓ 신규 컬럼 (CREATE OR REPLACE 제약상 반드시 맨 뒤에 추가할 것)
    -- 식별 신뢰도 — UNIDENTIFIED 는 '안전'이 아니라 '미확인 위험'으로 표시할 것.
    vi.identity_confidence,
    vi.callsgn_source,
    -- 풍향·돌풍: 이안풍(offshore)이면 계류삭 장력이 급증하고, 계류 판단은
    -- 평균풍속이 아니라 순간최대풍속(gust)으로 한다.
    wn.wind_dir_deg,
    wn.gust_ms,
    -- 조류: 액체부두 접·이안 조종의 직접 제약.
    wn.current_speed_cms,
    wn.current_dir_deg,
    -- -----------------------------------------------------------------------
    -- 존재 상태 — 프론트 진입점이 여기이므로 반드시 노출해야 한다.
    --   프론트 기본 필터: presence_state = 'PRESENT'
    --   'STALE'     회색 반투명 + position_age_min 배지
    --   'NO_SIGNAL' ★숨기지 말 것. doc_still_in_port=true 이면
    --               "서류상 재항인데 신호 소실" — 관제사 확인 대상.
    --   'DEPARTED'  출항신고 확정 — 지도에서 제외해도 안전
    -- -----------------------------------------------------------------------
    p.position_age_min,
    p.presence_state,
    p.doc_still_in_port,
    p.signal_health
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
-- [MMSI-First] 식별 조인을 callsgn 이 아니라 vessel_uid(=MMSI 기반)로 건다.
-- callsgn 을 한 번도 송출하지 않는 관공선·소방정·예부선도 선명·MMSI 를 달고
-- 대시보드에 표시되어야 하기 때문이다. 반대로 입항신고·화물은 callsgn 이
-- 있어야만 존재하는 정보라 그대로 callsgn 조인을 유지한다.
LEFT JOIN mart.vessel_identity     vi  ON vi.vessel_uid = p.vessel_pos_key
LEFT JOIN mart.port_call_overview  pco ON pco.callsgn = p.callsgn
LEFT JOIN msds_by_vessel           mv  ON mv.callsgn = p.callsgn
LEFT JOIN mart.weather_now         wn  ON TRUE;

-- ---------------------------------------------------------------------------
-- 7. mart.pipeline_health — 수집기 생존 신호 (관측성)
--
-- ★ 왜 필요한가
--   수집기가 7시간 죽으면 모든 선박이 NO_SIGNAL 로 떨어지고 지도가 텅 빈다.
--   화면상으로는 "울산항에 배가 한 척도 없음"과 구별되지 않는다.
--   프론트는 pipeline_state='DEGRADED' 일 때 지도를 비우는 대신
--   "수집 지연 — 표시 정보가 최신이 아닙니다" 배너를 띄워야 한다.
--
--   collected_at_utc(우리 수집 시각)와 received_at_utc(UPA 갱신 시각)를
--   분리해 둔 덕분에 추가 수집 없이 계산된다.
--     - collect_age_min 이 크다        → 우리 파이프라인 장애
--     - source_age_min 만 크다         → UPA 쪽 갱신 지연 (우리는 정상)
--   이 둘을 구분해야 장애 대응 대상이 정해진다.
--
-- ★ 2026-08-04 수정 — 초록불 오탐 제거
--   이전 판은 pipeline_state 를 collected_at_utc 하나로만 판정했다. 그 결과
--   "옛날 raw 파일을 다시 전처리하기만 해도" OK 가 떴다. 실측 재현:
--       collect_age_min 1분(방금 돌림) / source_age_min 31,011분(21.5일 전 데이터)
--       → pipeline_state = 'OK'          ← 3주 전 위치를 정상이라고 표시
--   관제사가 초록 배지를 보고 안심하는데 화면에는 3주 전 선박이 떠 있는
--   상황이 가능하다. 이 뷰가 막으려던 사고와 정확히 같은 종류다.
--
--   그래서 상태를 2개(OK/DEGRADED)에서 4개로 나누고, 원인별로 대응 주체가
--   갈리도록 diagnosis 문장을 함께 낸다. 판정 순서가 중요하다 —
--   수집기가 죽었으면 원천 신선도는 볼 필요도 없으므로 COLLECTOR_DOWN 이 먼저다.
--
--     OK              둘 다 최신
--     COLLECTOR_DOWN  우리 수집기가 120분 이상 안 돎       → 데이터 담당
--     STALE_SOURCE    수집은 도는데 원천이 180분 이상 낡음 → UPA 문의 / 옛 raw 재처리 의심
--     NO_DATA         적재 데이터 자체가 없음              → 초기 상태
--
-- ★ 2026-08-10 임계값 재산정 (20분/60분 → 120분/180분)
--   이전 값(20분/60분)은 로컬에서 6분 주기로 폴링하던 시절 기준이었다.
--   AWS 무인 수집기(PR #22)로 넘어가면서 수집 주기가 **매시 정각 1회**가 됐고,
--   그 결과 정상 운영 중에도 장애로 표시되는 반대 방향 오탐이 생겼다.
--     실측(PR #23 리뷰): 마지막 수집 27분 경과 — 정상인데 COLLECTOR_DOWN
--
--   이 뷰는 원래 "초록불 오탐"(장애인데 정상이라 표시)을 막으려고 만든 것인데,
--   반대로 "빨간불 오탐"(정상인데 장애라 표시)이 잦으면 관제사가 배지를
--   무시하게 되어 결국 같은 곳으로 간다. 두 방향 다 막아야 의미가 있다.
--
--   새 값의 근거 — 수집 주기 60분 기준:
--     COLLECTOR_DOWN 120분 = 정각 수집을 2회 연속 놓침. 1회 실패는 일시적
--       네트워크 오류로도 발생하므로 즉시 장애로 부르지 않는다.
--     STALE_SOURCE  180분 = 원천 신선도는 "수집 주기(최대 60분 지연) + 원천
--       자체 갱신 지연"의 합이라 수집기 기준보다 넉넉해야 한다. 60분으로 두면
--       매시 수집 직전(59분 경과)마다 STALE_SOURCE 가 깜빡인다.
--
--   ※ 수집 주기가 다시 바뀌면 이 두 값도 같이 바꿔야 한다. 근거는
--     mart_views_check.sql 의 관측간격 분위수 쿼리로 다시 만들 수 있다.
--
--   ※ 프론트 규약: pipeline_state <> 'OK' 이면 지도를 비우는 대신 배너를
--     띄운다. COLLECTOR_DOWN 과 STALE_SOURCE 는 문구가 달라야 한다 —
--     전자는 "우리 시스템 점검 중", 후자는 "원천 데이터 갱신 지연"이다.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.pipeline_health AS
SELECT
    max(collected_at_utc)                                                  AS last_collect_at_utc,
    round(EXTRACT(EPOCH FROM (now() - max(collected_at_utc))) / 60.0)::int AS collect_age_min,
    max(received_at_utc)                                                   AS last_source_at_utc,
    round(EXTRACT(EPOCH FROM (now() - max(received_at_utc))) / 60.0)::int  AS source_age_min,
    count(*) FILTER (WHERE collected_at_utc > now() - interval '10 min')   AS rows_last_10min,
    count(DISTINCT vessel_uid)                                             AS vessel_uid_total,
    CASE
        WHEN max(collected_at_utc) IS NULL                        THEN 'NO_DATA'
        WHEN max(collected_at_utc) < now() - interval '120 min'   THEN 'COLLECTOR_DOWN'
        WHEN max(received_at_utc)  < now() - interval '180 min'   THEN 'STALE_SOURCE'
        ELSE 'OK'
    END                                                                    AS pipeline_state,
    -- ↓ 신규 컬럼 (CREATE OR REPLACE 제약상 반드시 맨 뒤에 추가할 것)
    -- 관제사·개발자가 "그래서 뭘 해야 하나"를 배지 하나로 알 수 있게 한다.
    CASE
        WHEN max(collected_at_utc) IS NULL
            THEN '적재 데이터 없음 — 파이프라인을 한 번도 돌리지 않았거나 테이블이 비어 있음'
        WHEN max(collected_at_utc) < now() - interval '120 min'
            THEN '우리 수집기가 안 돌고 있음(정각 수집 2회 이상 누락) — EC2 스케줄러/run_pipeline 상태 확인'
        WHEN max(received_at_utc) < now() - interval '180 min'
            THEN '수집은 도는데 원천 데이터가 낡음 — UPA 갱신 지연 또는 옛 raw 재처리 의심'
        ELSE '정상'
    END                                                                    AS diagnosis
FROM upa_vessel_position;

-- ---------------------------------------------------------------------------
-- 8. mart.berth_current_cargo — 선석별 "지금 취급 중인 화물" (백엔드 소비 계약)
--
-- ★ 왜 필요한가 — 백엔드의 알려진 한계 #1 을 없애는 뷰다.
--   backend/app/agents/scheduling/category_map.py 는 인접 선석의 화물을 몰라서
--   카테고리 대표 1종으로 근사하고 있다:
--       원유 → 석유(000751) / 유류 → 디젤(000973) / 액체화학 → 벤젠(001008)
--   그 파일 주석도 "upa_cargo_manifest 파이프라인이 생기면 이 모듈 대신 실제
--   최근 하역 기록을 조회하도록 교체해야 한다"고 적어두었다. 이 뷰가 그 대체물이다.
--
--   실제 영향(그 문서 인용): "정일1부두에 실제로는 톨루엔이 있는데도 시스템은
--   '액체화학이니까 벤젠이 있다고 치자'고 판단한다" — 즉 혼재금지 판정이 엉뚱한
--   화물 조합으로 이뤄진다. 이 뷰를 쓰면 실제 신고 화물로 판정하게 된다.
--
-- ★ 출력 계약 — 스케줄링 에이전트가 그대로 쓸 수 있는 모양
--   AdjacentCargo(berth_name=facility_name, cargo=CargoRef(chem_id=chem_id))
--   CargoRef 는 chem_id 또는 cas_no 중 하나면 되므로 둘 다 내보낸다.
--   (백엔드는 un_no 로 조회하지 않는다 — msds_context.py 는 cas_no/chem_id 만 쓴다.
--    un_no 는 msds_chemical 의 표시용 컬럼이고 WHERE 절에 등장하지 않는다.
--    화물 manifest 에는 UN 번호만 있으므로, UN → chem_id 변환이 이 뷰의 핵심이다.)
--
-- ★ "지금"의 정의
--   upa_port_call 에 입항 기록이 있고 departure_at_utc 가 NULL 인 선박 = 재항 중.
--   출항 신고가 확정된 배의 화물은 그 선석에 없으므로 제외한다.
--   (vessel_latest_position 의 presence_state 와 같은 원칙 — 출항은 서류로 확정)
--
-- ★ 한계 (숨기지 않는다)
--   - is_synthetic=true 인 화물이 섞여 있다. bzentyCd(업체코드) 미확보로 UPA
--     통합화물 API 를 못 부르는 동안의 합성 샘플이다. 소비자가 판단하도록 그대로
--     노출한다. 실화물만 원하면 WHERE is_synthetic = false 를 붙일 것.
--   - MSDS 에 없는 UN 번호는 chem_id 가 NULL 이다. 이 행을 버리지 않는다 —
--     "위험물이 있는데 정체를 모른다"는 것이 "화물이 없다"보다 중요한 정보다.
--     소비자는 chem_id IS NULL 을 '미확인 위험'으로 다뤄야 한다.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.berth_current_cargo AS
WITH in_port AS (
    -- 재항 중인 선박 (출항 신고 없음) — 선박당 최신 입항 건 1개
    SELECT DISTINCT ON (upper(btrim(callsgn)))
           upper(btrim(callsgn)) AS callsgn,
           facility_name,
           arrival_at_utc
    FROM upa_port_call
    WHERE nullif(btrim(callsgn), '') IS NOT NULL
      AND departure_at_utc IS NULL
    ORDER BY upper(btrim(callsgn)), arrival_at_utc DESC
)
SELECT DISTINCT
       COALESCE(cm.facility_name, ip.facility_name) AS facility_name,
       ip.callsgn,
       cm.chem_id,
       cm.cas_no,
       cm.dg_un_no,
       COALESCE(cm.msds_name_ko, cm.cargo_name_raw)  AS cargo_name,
       cm.imdg_class,
       cm.packing_group,
       cm.msds_matched,
       cm.is_synthetic,
       cm.cargo_basis,
       ip.arrival_at_utc
FROM in_port ip
JOIN mart.cargo_msds cm ON cm.callsgn = ip.callsgn
WHERE cm.dg_un_no IS NOT NULL;   -- 위험물 화물만 (혼재금지 판정 대상)
