-- ===========================================================================
-- mart_views_check.sql — mart 뷰 검증 쿼리 9종
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

-- [검증 2] vessel_identity — 선박 1척 = 1행 (vessel_uid 중복 0, vessel_key 결측 0)
--   ※ MMSI-First 전환 이후 callsgn 은 NULL 일 수 있다(정적신호를 아예 송출하지
--     않는 관공선·소방정·예부선 등). 따라서 유일성 판정 기준을 callsgn 이 아니라
--     vessel_uid(MMSI 우선 고유키)로 바꾼다. 예전 기준을 그대로 두면 NULL 이
--     DISTINCT 집계에서 빠져 항상 FAIL 로 오탐한다.
SELECT '2. 식별 마스터 유일성' AS check_name,
       CASE WHEN dup = 0 AND null_key = 0 THEN 'PASS'
            ELSE 'FAIL (중복 ' || dup || ', 키결측 ' || null_key || ')' END AS result
FROM (
    SELECT count(*) - count(DISTINCT vessel_uid)       AS dup,
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

-- [검증 6] MMSI-First — 위치신호가 있는 선박은 callsgn 유무와 무관하게 전부 남는가
--   UPA getVslPstnInfo 는 callsgn 결측 약 31%. 예전 뷰는 이 선박들을 통째로
--   버려서 관공선·소방정·순찰선이 화면에서 사라졌다. 아래는 원천 테이블의
--   고유 선박 수와 뷰의 위치보유 선박 수가 일치하는지 본다.
SELECT '6. MMSI-First 누락 없음' AS check_name,
       CASE WHEN src = viewed THEN 'PASS (' || viewed || '척)'
            ELSE 'FAIL (원천 ' || src || '척 → 뷰 ' || viewed || '척, '
                 || (src - viewed) || '척 유실)' END AS result
FROM (
    SELECT (SELECT count(DISTINCT COALESCE(mmsi::text, 'CS:' || upper(btrim(callsgn))))
              FROM upa_vessel_position
             WHERE mmsi IS NOT NULL OR nullif(btrim(callsgn), '') IS NOT NULL) AS src,
           (SELECT count(*) FROM mart.vessel_identity WHERE has_position)      AS viewed
) t;

-- [검증 7] Stateful Filling — callsgn 출처 표기가 정상 범위인가
--   'current' = 최신 관측에 실려온 값 / 'filled' = 과거 신호에서 끌어온 값
--   'portmis' = 위치는 아직 없고 입항신고만 있는 선박 / NULL = 수신 이력 없음
--   ★ filled 는 "정적신호 일시 누락"을 메운 것이므로 존재해도 정상이지만,
--     전체의 절반을 넘으면 정적신호 수집 주기 자체를 의심해야 한다.
SELECT '7. callsgn 출처 표기' AS check_name,
       CASE WHEN bad_label > 0
              THEN 'FAIL (허용외 라벨 ' || bad_label || '건)'
            WHEN pos_rows > 0 AND filled::numeric / pos_rows > 0.5
              THEN 'FAIL (filled 비중 ' || round(100.0 * filled / pos_rows, 1)
                   || '% — 정적신호 수집 주기 점검)'
            ELSE 'PASS (current ' || current_cnt || ' / filled ' || filled
                 || ' / 이력없음 ' || never || ')' END AS result
FROM (
    SELECT count(*) FILTER (WHERE callsgn_source NOT IN
                                  ('current', 'filled', 'portmis'))       AS bad_label,
           count(*) FILTER (WHERE has_position)                           AS pos_rows,
           count(*) FILTER (WHERE callsgn_source = 'current')             AS current_cnt,
           count(*) FILTER (WHERE callsgn_source = 'filled')              AS filled,
           count(*) FILTER (WHERE callsgn_source IS NULL)                 AS never
    FROM mart.vessel_identity
) t;

-- [검증 8] 미확인 위험(Unknown Risk) 상태가 살아 있는가
--   "정적신호가 없는 선박 = 안전"이 아니다. 액체 부선·소형 급유선·300GT 미만
--   연안 케미칼선은 AIS 의무 대상이 아니거나 Class B 라 선종이 확인되지 않는다.
--   이들을 목록에서 지우면 위험물 실은 배가 관제 화면에서 사라진다.
--   따라서 identity_confidence 3상태가 모두 정의되어 있고, UNIDENTIFIED 가
--   dashboard 까지 전달되는지 확인한다.
SELECT '8. 미확인 위험 상태 보존' AS check_name,
       CASE WHEN bad > 0 THEN 'FAIL (허용외 상태 ' || bad || '건)'
            WHEN vi_unknown <> dash_unknown
              THEN 'FAIL (identity ' || vi_unknown || ' vs dashboard '
                   || dash_unknown || ' — 대시보드로 전달 누락)'
            ELSE 'PASS (확정 ' || confirmed || ' / 부분 ' || partial
                 || ' / 미확인 ' || vi_unknown || ')' END AS result
FROM (
    SELECT (SELECT count(*) FROM mart.vessel_identity
             WHERE identity_confidence NOT IN
                   ('CONFIRMED', 'PARTIAL', 'UNIDENTIFIED'))              AS bad,
           (SELECT count(*) FROM mart.vessel_identity
             WHERE identity_confidence = 'CONFIRMED')                     AS confirmed,
           (SELECT count(*) FROM mart.vessel_identity
             WHERE identity_confidence = 'PARTIAL')                       AS partial,
           (SELECT count(*) FROM mart.vessel_identity
             WHERE identity_confidence = 'UNIDENTIFIED' AND has_position)  AS vi_unknown,
           (SELECT count(*) FROM mart.dashboard_current
             WHERE identity_confidence = 'UNIDENTIFIED')                  AS dash_unknown
) t;

-- [검증 9] 흘수 판정에 조위가 실제로 반영되는가
--   부두 명세 수심은 해도기준면 기준이므로 그 시각 가용수심 = 해도수심 + 조위.
--   조위를 빼먹으면 만조에만 접안 가능한 배를 영구 접안불가로 오판한다.
--   available_depth_m 이 chart_depth_m 과 항상 같다면 조위가 안 붙은 것이다.
SELECT '9. 조위 반영 흘수 판정' AS check_name,
       CASE WHEN rows_total = 0
              THEN 'PASS (접안 중 선박 없음 — 판정 대상 0건)'
            WHEN tide_applied = 0
              THEN 'FAIL (조위가 반영되지 않음 — tide_obs 적재 확인)'
            WHEN bad_verdict > 0
              THEN 'FAIL (허용외 판정값 ' || bad_verdict || '건)'
            ELSE 'PASS (' || rows_total || '건 판정, 조위 반영 ' || tide_applied || '건)'
       END AS result
FROM (
    SELECT count(*)                                                     AS rows_total,
           count(*) FILTER (WHERE available_depth_m IS DISTINCT FROM chart_depth_m)
                                                                        AS tide_applied,
           count(*) FILTER (WHERE draught_verdict NOT IN
                            ('OK', 'MARGINAL', 'NOT_ALLOWED', 'UNKNOWN')) AS bad_verdict
    FROM mart.berth_draught_check
) t;
