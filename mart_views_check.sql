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
       -- ★ "MSDS 테이블이 비어 있음"과 "조인키가 깨짐"은 원인도 대응도 다르다.
       --   전자는 수집 미완료(MSDS API 키·수집 배치), 후자는 dg_un_no 정규화 문제다.
       --   구분하지 않으면 수집만 안 됐는데 조인 코드를 뒤지게 된다.
       CASE WHEN dg_total  = 0 THEN 'PASS (위험물 화물 없음)'
            WHEN msds_rows = 0 THEN 'SKIP (MSDS 미적재 — 화물 UN번호 '
                                    || dg_total || '건 대기중. 조인키 문제 아님)'
            WHEN matched > 0 THEN 'PASS (' || matched || '/' || dg_total || ' 매칭)'
            ELSE 'FAIL (MSDS ' || msds_rows || '행 적재됐는데 UN번호 ' || dg_total
                 || '건 모두 미매칭 — dg_un_no 조인키 점검)' END AS result
FROM (
    SELECT count(*) FILTER (WHERE dg_un_no IS NOT NULL)                   AS dg_total,
           count(*) FILTER (WHERE dg_un_no IS NOT NULL AND msds_matched)  AS matched,
           (SELECT count(*) FROM msds_chemical)                           AS msds_rows
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

-- [검증 10] 적재 계층 MMSI-First — vessel_uid 결측·중복 없음
--   PostgreSQL 유니크 인덱스는 NULL 을 서로 다른 값으로 취급한다.
--   적재 자연키에 NULL 가능 컬럼(callsgn)을 쓰면 ON CONFLICT 가 걸리지 않아
--   폴링마다 중복 행이 쌓인다. vessel_uid 는 항상 채워져야 한다.
SELECT '10. 적재키 vessel_uid 무결성' AS check_name,
       CASE WHEN total = 0            THEN 'PASS (적재 데이터 없음)'
            WHEN uid_null > 0         THEN 'FAIL (vessel_uid 결측 ' || uid_null || '행)'
            WHEN dup_rows > 0         THEN 'FAIL (동일 키 중복 ' || dup_rows || '행 — 유니크 인덱스 확인)'
            ELSE 'PASS (' || total || '행, 고유선박 ' || uids || '척, 중복 0)'
       END AS result
FROM (
    SELECT count(*)                                          AS total,
           count(*) FILTER (WHERE vessel_uid IS NULL)        AS uid_null,
           count(DISTINCT vessel_uid)                        AS uids,
           (SELECT COALESCE(sum(c - 1), 0) FROM (
                SELECT count(*) AS c FROM upa_vessel_position
                GROUP BY vessel_uid, received_at_utc HAVING count(*) > 1) d) AS dup_rows
    FROM upa_vessel_position
) t;

-- [검증 11] 출항은 서류로 확정, 신호 소실과 분리되었는가
--   신호 침묵을 DEPARTED 로 부르면 트랜스폰더가 죽은 채 접안 중인 배가
--   지도에서 사라진다. DEPARTED 는 upa_port_call.departure_at_utc 근거로만
--   나와야 하고, 근거 없는 침묵은 NO_SIGNAL 이어야 한다.
SELECT '11. 출항 판정 근거' AS check_name,
       CASE WHEN total = 0             THEN 'PASS (위치 데이터 없음)'
            WHEN bad_state > 0         THEN 'FAIL (허용외 상태값 ' || bad_state || '건)'
            WHEN dep_no_doc > 0        THEN 'FAIL (서류 근거 없는 DEPARTED ' || dep_no_doc || '건)'
            ELSE 'PASS (총 ' || total || '척 / PRESENT ' || n_present
                 || ' · STALE ' || n_stale || ' · NO_SIGNAL ' || n_nosig
                 || ' · DEPARTED ' || n_dep || ')'
       END AS result
FROM (
    SELECT count(*)                                                        AS total,
           count(*) FILTER (WHERE presence_state NOT IN
                ('PRESENT','STALE','NO_SIGNAL','DEPARTED'))                AS bad_state,
           count(*) FILTER (WHERE presence_state = 'DEPARTED'
                              AND departed_at_utc IS NULL)                 AS dep_no_doc,
           count(*) FILTER (WHERE presence_state = 'PRESENT')              AS n_present,
           count(*) FILTER (WHERE presence_state = 'STALE')                AS n_stale,
           count(*) FILTER (WHERE presence_state = 'NO_SIGNAL')            AS n_nosig,
           count(*) FILTER (WHERE presence_state = 'DEPARTED')             AS n_dep
    FROM mart.vessel_latest_position
) t;

-- [검증 12] 수집기 생존 신호가 계산되는가
--   전면 침묵(수집기 장애)과 "항만이 비었다"를 구별하기 위한 뷰.
--
--   ★ 초록불 오탐 방지가 이 검증의 핵심이다. 이전 판은 collected_at_utc 만 보고
--     판정해서, 옛 raw 를 재처리하기만 해도 OK 가 떴다(원천 21.5일 전인데 OK).
--     아래 단언은 "수집은 최신인데 원천만 낡은" 상태가 OK 로 새어나가지 않는지를
--     직접 확인한다 — 이 조합이면 반드시 STALE_SOURCE 여야 한다.
SELECT '12. 수집기 생존 신호' AS check_name,
       CASE WHEN pipeline_state IS NULL
              THEN 'FAIL (pipeline_health 계산 불가)'
            WHEN pipeline_state NOT IN ('OK', 'COLLECTOR_DOWN', 'STALE_SOURCE', 'NO_DATA')
              THEN 'FAIL (허용외 상태값: ' || pipeline_state || ')'
            -- 초록불 오탐: 원천이 60분 이상 낡았는데 OK 라고 하면 판정 순서가 깨진 것
            WHEN pipeline_state = 'OK' AND source_age_min > 60
              THEN 'FAIL (원천 ' || source_age_min || '분 낡았는데 OK — STALE_SOURCE 여야 함)'
            -- 수집기가 죽었는데 OK 라고 하는 경우
            WHEN pipeline_state = 'OK' AND collect_age_min > 20
              THEN 'FAIL (수집 ' || collect_age_min || '분 중단인데 OK — COLLECTOR_DOWN 이어야 함)'
            WHEN pipeline_state = 'NO_DATA'
              THEN 'PASS (적재 데이터 없음 — NO_DATA)'
            ELSE 'PASS (' || pipeline_state || ', 수집경과 ' || collect_age_min
                 || '분 / 원천경과 ' || source_age_min || '분 — ' || diagnosis || ')'
       END AS result
FROM mart.pipeline_health;

-- ---------------------------------------------------------------------------
-- [참고 A] presence_state 임계값 실측 근거 산출 쿼리
--
--   ★ 30분/6시간은 아직 실측 근거가 없는 잠정값이다.
--     "6분 폴링이니까 30분"은 논리가 아니다. 아래로 실제 관측 간격 분포를
--     구하고, p99 를 넘는 지점을 PRESENT 경계로 잡아야 근거 있는 값이 된다.
--     실데이터가 충분히 쌓인 뒤 재실행해 임계값을 갱신할 것.
--
--   해석: p99 가 11분이면 → "실측 p99 11분, 여유 2.7배로 30분" 이 된다.
--         max_gap 이 6시간을 넘으면 NO_SIGNAL 경계도 함께 올려야 한다.
-- ---------------------------------------------------------------------------
WITH gaps AS (
    SELECT vessel_uid,
           EXTRACT(EPOCH FROM (received_at_utc
             - lag(received_at_utc) OVER (PARTITION BY vessel_uid
                                          ORDER BY received_at_utc))) / 60 AS gap_min
    FROM upa_vessel_position
)
SELECT 'A. 관측간격 분포(임계값 근거)'                                        AS check_name,
       count(*)                                                              AS n_gaps,
       count(DISTINCT vessel_uid)                                            AS n_vessels,
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY gap_min)::numeric, 1) AS p50_min,
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY gap_min)::numeric, 1) AS p95_min,
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY gap_min)::numeric, 1) AS p99_min,
       round(max(gap_min)::numeric, 1)                                       AS max_gap_min
FROM gaps
WHERE gap_min IS NOT NULL AND gap_min > 0;
