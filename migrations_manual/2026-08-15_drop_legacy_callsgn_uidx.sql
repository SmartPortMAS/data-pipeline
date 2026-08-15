-- ===========================================================================
-- upa_vessel_position: 레거시 유니크 인덱스 제거 (callsgn + received_at_utc)
-- ===========================================================================
-- 실행: docker exec -i smartport-postgres-dev psql -U smartport -d smartport
--         < migrations_manual/2026-08-15_drop_legacy_callsgn_uidx.sql
--
-- [왜 지우는가]
-- 2026-08-04 에 MMSI-First 식별자(vessel_uid)를 도입하면서
-- upa_vessel_position_uidx__vessel_uid__received_at_utc 를 새로 만들었는데,
-- **대체 대상이던 옛 인덱스를 지우지 않았다.** 그래서 두 유니크 제약이 공존했고,
-- 옛 것이 먼저 걸려 적재가 통째로 실패하고 있었다.
--
-- 옛 인덱스는 "호출부호가 같고 관측시각이 같으면 같은 배"라고 본다. 그런데 UPA
-- 선박위치는 호출부호 결측이 흔하고(약 31%), 빈 문자열이나 '500' 같은 값이 여러
-- 배에 동시에 들어온다. 즉 **서로 다른 배가 같은 키로 뭉개진다.**
--
-- 2026-08-15 실측 (staging 519행):
--     callsgn=''    2026-08-15 00:46:24  ->  CHO KAWANG NO.(mmsi 440011690)
--                                            1 DEA YANG HO (mmsi 440411950)
--     callsgn='500' 2026-08-15 00:46:25  ->  EOHANG DONGHAE(mmsi 440049870)
--                                            DAESU HO      (mmsi 440223380)
--
-- 네 척 다 MMSI 가 다르므로 vessel_uid 는 서로 다르다 — 새 인덱스로는 정상
-- 구분된다. 옛 인덱스만 이들을 중복으로 보고 UniqueViolation 을 던졌다.
--
-- [피해]
-- 선박 위치 적재가 2026-08-14 20:50 이후 전량 실패. S3 에는 최신 관측이 계속
-- 쌓이는데 로컬 DB 와 대시보드 지도만 5시간 가까이 과거에 머물러 있었다.
-- cloud_pull 이 도메인별로 예외를 삼키고 다음 도메인을 계속 적재하는 구조라
-- (한 도메인이 죽어도 나머지는 살린다는 의도) 겉으로는 "성공"으로 보였다.
--
-- [남기는 것]
--   upa_vessel_position_uidx                              (record_uid)
--   upa_vessel_position_uidx__vessel_uid__received_at_utc (vessel_uid, received_at_utc)
-- 이 둘로 중복 방지는 충분하다. vessel_uid 는 MMSI -> 호출부호 -> 익명해시
-- 순으로 채워지므로 호출부호가 없어도 배를 구분한다.
-- ===========================================================================

BEGIN;

DROP INDEX IF EXISTS public.upa_vessel_position_uidx__callsgn__received_at_utc;

-- 확인: 남은 유니크 인덱스가 record_uid / vessel_uid 두 개인지
SELECT indexname
FROM pg_indexes
WHERE tablename = 'upa_vessel_position'
ORDER BY indexname;

COMMIT;
