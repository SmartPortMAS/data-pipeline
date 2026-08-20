-- ===========================================================================
-- upa_vessel_position 에 vessel_uid / vessel_uid_source 컬럼 추가 (2026-08-04)
-- ===========================================================================
-- [왜 수동 SQL인가]
-- upa_* 테이블은 backend(Alembic) 소유가 아니라 data-pipeline 이 auto_create=True 로
-- 직접 만든 레거시 테이블이다(common_pg_loader.py 의 스키마 소유권 주석 참조).
-- 그래서 Alembic 마이그레이션 대상이 아니다.
--
-- 로더는 "테이블이 없으면" DataFrame 스키마로 생성하지만, "이미 있는 테이블에
-- 컬럼을 추가"하지는 않는다. MMSI-First 전환(2026-08, PR #18)으로 적재 자연키가
-- callsgn → vessel_uid 로 바뀌면서, 기존 DB 에는 없는 컬럼을 UPSERT 키로 쓰게 되어
-- 적재가 실패했다. 신규 설치는 로더가 알아서 만들므로 이 파일이 필요 없다.
--
-- [실행 대상] 2026-08-04 이전에 upa_vessel_position 을 이미 적재한 로컬/서버 DB
-- [실행 방법]
--   docker exec -e PGPASSWORD=... smartport-postgres-dev \
--     psql -U <user> -d <db> -f /tmp/2026-08-04_add_vessel_uid.sql
--
-- [실행 후] data_pipeline.run_pipeline vessel 을 돌리면 로더가
--   (vessel_uid, received_at_utc) 유니크 인덱스를 만들면서 기존 중복 행을 정리한다.
--   실측: 2,783행 → 중복 639행 정리 → 고유 선박 846척
-- ===========================================================================

ALTER TABLE upa_vessel_position
  ADD COLUMN IF NOT EXISTS vessel_uid        text,
  ADD COLUMN IF NOT EXISTS vessel_uid_source text;

-- 백필 규칙은 common_preprocessing.create_vessel_uid() 와 동일해야 한다.
-- 여기서 다르게 채우면 같은 배가 적재 경로에 따라 다른 키를 갖게 된다.

-- 1순위: MMSI (결측률 0% 관측). numeric_cols 를 거치며 float 이 되므로 정수화한다.
UPDATE upa_vessel_position
   SET vessel_uid        = trunc(mmsi::numeric)::bigint::text,
       vessel_uid_source = 'MMSI'
 WHERE vessel_uid IS NULL
   AND mmsi IS NOT NULL;

-- 2순위: 'CS:' + 호출부호
UPDATE upa_vessel_position
   SET vessel_uid        = 'CS:' || upper(btrim(callsgn)),
       vessel_uid_source = 'CALLSIGN'
 WHERE vessel_uid IS NULL
   AND callsgn IS NOT NULL
   AND btrim(callsgn) <> '';

-- 3순위(ANON 해시)는 SQL 로 채우지 않는다.
-- create_vessel_uid() 는 sha1(latitude|longitude|received_at_utc) 를 쓰는데,
-- PostgreSQL 기본 설치에는 sha1 이 없고(pgcrypto 필요) 문자열 표현이 pandas 와
-- 달라질 위험이 있다. 키가 어긋나면 같은 관측이 중복 적재된다.
-- MMSI·호출부호가 모두 없는 행이 있으면 전처리를 다시 태워서 채울 것.
-- (2026-08-04 실행 시점 기준 해당 행 0건)

-- 검증
SELECT vessel_uid_source,
       count(*)                  AS 행수,
       count(DISTINCT vessel_uid) AS 고유선박
  FROM upa_vessel_position
 GROUP BY 1
 ORDER BY 1;

SELECT count(*) AS uid_없음_행수
  FROM upa_vessel_position
 WHERE vessel_uid IS NULL;   -- 0 이어야 한다
