-- ===========================================================================
-- mart_views_check.sql — mart 뷰 검증 쿼리 5종
-- ===========================================================================
-- mart_views.sql 실행 후 이 파일을 실행한다. 모든 행의 result 가 'PASS' 여야
-- 뷰 계약 확정 조건을 충족한다. (DBeaver 에서 전체 실행 또는 psql -f)
-- ===========================================================================

-- [검증 1] 뷰 6종이 모두 생성되었는가
SELECT '1. 뷰 6종 생성' AS check_name,
       CASE WHEN count(*) = 6 THEN 'PASS' ELSE 'FAIL (' || count(*) || '개)' END AS result
FROM information_schema.views
WHERE table_schema = 'mart'
  AND table_name IN ('vessel_identity', 'vessel_latest_position',
                     'port_call_overview', 'cargo_msds',
                     'weather_now', 'dashboard_current');

-- [검증 2] vessel_identity — 선박 1척 = 1행 (callsgn 중복 0, vessel_key 결측 0)
SELECT '2. 식별 마스터 유일성' AS check_name,
       CASE WHEN dup = 0 AND null_key = 0 THEN 'PASS'
            ELSE 'FAIL (중복 ' || dup || ', 키결측 ' || null_key || ')' END AS result
FROM (
    SELECT count(*) - count(DISTINCT callsgn)          AS dup,
           count(*) FILTER (WHERE vessel_key IS NULL)  AS null_key
    FROM mart.vessel_identity
) t;

-- [검증 3] vessel_latest_position — 선박당 최신 1행 보장
SELECT '3. 최신 위치 선박당 1행' AS check_name,
       CASE WHEN count(*) = count(DISTINCT vessel_pos_key) THEN 'PASS'
            ELSE 'FAIL' END AS result
FROM mart.vessel_latest_position;

-- [검증 4] cargo_msds — UN 번호 보유 화물의 MSDS 매칭 (0건이면 조인키 점검)
SELECT '4. 위험물 MSDS 매칭' AS check_name,
       CASE WHEN dg_total = 0 THEN 'PASS (위험물 화물 없음)'
            WHEN matched > 0 THEN 'PASS (' || matched || '/' || dg_total || ' 매칭)'
            ELSE 'FAIL (UN번호 ' || dg_total || '건 모두 미매칭 — dg_un_no 조인키 점검)' END AS result
FROM (
    SELECT count(*) FILTER (WHERE dg_un_no IS NOT NULL)                   AS dg_total,
           count(*) FILTER (WHERE dg_un_no IS NOT NULL AND msds_matched)  AS matched
    FROM mart.cargo_msds
) t;

-- [검증 5] dashboard_current — 진입점 조회 가능 + 환경 스냅샷 최신성
SELECT '5. 한 줄 조회 정합' AS check_name,
       CASE WHEN dash_rows = pos_rows
             AND weather_rows = 1
             AND weather_is_latest
            THEN 'PASS (' || dash_rows || '척)'
            ELSE 'FAIL (dash ' || dash_rows || ' vs pos ' || pos_rows
                 || ', weather_now ' || weather_rows || '행, 최신성 ' || weather_is_latest || ')'
       END AS result
FROM (
    SELECT (SELECT count(*) FROM mart.dashboard_current)          AS dash_rows,
           (SELECT count(*) FROM mart.vessel_latest_position)     AS pos_rows,
           (SELECT count(*) FROM mart.weather_now)                AS weather_rows,
           (SELECT weather_observed_at_utc FROM mart.weather_now)
             = (SELECT max(observed_at_utc) FROM weather_obs)     AS weather_is_latest
) t;
