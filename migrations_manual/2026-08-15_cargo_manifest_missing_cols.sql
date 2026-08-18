-- ===========================================================================
-- upa_cargo_manifest: 생성기 스키마에는 있는데 테이블에 없던 컬럼 6종 추가
-- ===========================================================================
-- 실행: docker exec -i smartport-postgres-dev psql -U smartport -d smartport
--         < migrations_manual/2026-08-15_cargo_manifest_missing_cols.sql
--
-- [왜 필요한가]
-- gen_cargo_manifest.py 는 39컬럼(공공데이터 화물정보 스키마 정의서와 1:1)을
-- 만드는데 테이블에는 33컬럼만 있었다. 적재 시 이 오류로 전량 실패한다:
--
--     column "vol_size" of relation "upa_cargo_manifest" does not exist
--
-- 지금까지 드러나지 않은 이유는 기존 CSV(365행)가 우연히 짧은 컬럼 집합이었기
-- 때문이고, 생성 범위를 넓히면서 전체 스키마로 쓰기 시작하자 바로 걸렸다.
--
-- 이 테이블은 data-pipeline 이 auto_create 로 만든 것이라(backend Alembic 소유가
-- 아니다) 여기 수동 마이그레이션으로 맞춘다 — vessel_uid 때와 같은 경로다.
--
-- 추가하는 컬럼은 전부 수량·항구 표기용 보조 필드다. 판정(혼재·흘수·기상)에는
-- 쓰이지 않으므로 기존 행이 NULL 로 남아도 판정 결과가 달라지지 않는다.
-- ===========================================================================

BEGIN;

ALTER TABLE public.upa_cargo_manifest
    ADD COLUMN IF NOT EXISTS vol_size            text,  -- 용적(원문 표기 그대로)
    ADD COLUMN IF NOT EXISTS weight_size         text,  -- 중량(원문 표기)
    ADD COLUMN IF NOT EXISTS bulk_vol_size       text,  -- 산적 용적
    ADD COLUMN IF NOT EXISTS bulk_weight_size    text,  -- 산적 중량
    ADD COLUMN IF NOT EXISTS ldud_port_name      text,  -- 양적항
    ADD COLUMN IF NOT EXISTS last_dest_port_name text,  -- 최종 목적항
    ADD COLUMN IF NOT EXISTS container_count     text;  -- 컨테이너 개수(액체화물엔 빈 값)

-- 확인: 39컬럼이 되었는지
SELECT count(*) AS columns_now
FROM information_schema.columns
WHERE table_name = 'upa_cargo_manifest';

COMMIT;
