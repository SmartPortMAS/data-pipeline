"""
MSDS PostgreSQL 적재 모듈 — JSONB 기반 마스터 스테이징 DB
==========================================================
파이프라인 위치: [수집기] → raw JSON → [이 모듈] → PostgreSQL

전략:
- 수집기(msds_api_collector.py)가 생성한 raw record에서
  기본 식별자(chem_id, cas_no 등)는 일반 컬럼으로 꺼내고,
  detail01 ~ detail16 의 16개 섹션 구조체 전체는 msds_payload (JSONB) 단일 컬럼에 보존.
- ON CONFLICT (chem_id) DO UPDATE 로 재실행 시 멱등성(Idempotency) 보장.
- 팀 공통 메타데이터 규칙 (common_preprocessing.py §7) 준수.

외부 의존 라이브러리:
    pip install psycopg2-binary python-dotenv

환경변수 (.env 또는 OS 환경변수):
    PG_HOST, PG_PORT, PG_DBNAME, PG_USER, PG_PASSWORD
"""

import glob
import json
import logging
import os
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# 로깅 설정
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("msds_pg_loader")

# ─────────────────────────────────────────────────────────────────────────────
# DB 접속 설정 — 환경변수로 분리하여 프로덕션 배포 시 secrets manager 연동 가능
# ─────────────────────────────────────────────────────────────────────────────
PG_CONFIG: dict = {
    "host":            os.getenv("PG_HOST",     "localhost"),
    "port":            int(os.getenv("PG_PORT", "5432")),
    "dbname":          os.getenv("PG_DBNAME",   "smartport"),
    "user":            os.getenv("PG_USER",     "postgres"),
    "password":        os.getenv("PG_PASSWORD", ""),
    "connect_timeout": 10,
    "options":         "-c search_path=public",  # 스키마 명시
}

# ─────────────────────────────────────────────────────────────────────────────
# 파이프라인 상수
# ─────────────────────────────────────────────────────────────────────────────
RAW_DIR    = "data/raw/msds"
BATCH_SIZE = 50  # 한 번에 커밋할 최대 레코드 수

# 팀 공통 메타데이터 (common_preprocessing.py add_common_metadata() 규칙 동일)
SOURCE_SYSTEM = "KOSHA_MSDS_API"
SOURCE_TABLE  = "getChemDetail"

# 수집기가 생성하는 16개 섹션 키
SECTION_KEYS: list[str] = [f"detail{i:02d}" for i in range(1, 17)]


# ─────────────────────────────────────────────────────────────────────────────
# DDL 정의 — 테이블, 제약조건, 인덱스
# ─────────────────────────────────────────────────────────────────────────────
_DDL_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS staging_msds_chemical (
    -- ── 기본 식별자 컬럼 ──────────────────────────────────────────────────
    chem_id          VARCHAR(20)   NOT NULL,     -- 안전보건공단 화학물질 고유 ID (PK)
    cas_no           VARCHAR(50),                 -- CAS 등록번호 (부분 UNIQUE 인덱스)
    un_no            VARCHAR(20),                 -- UN 번호 (위험물 운송 코드)
    name_ko          VARCHAR(500),                -- 한국어 화학물질명
    name_en          VARCHAR(500),                -- 영문 화학물질명

    -- ── 팀 공통 메타데이터 컬럼 (§7 규칙 준수) ────────────────────────────
    source_system    VARCHAR(50)   NOT NULL DEFAULT 'KOSHA_MSDS_API',
    source_table     VARCHAR(50)   NOT NULL DEFAULT 'getChemDetail',
    collected_at_utc TIMESTAMPTZ   NOT NULL,      -- 수집 시각 (UTC)
    quality_flag     VARCHAR(20)   NOT NULL DEFAULT 'OK',  -- OK | MISSING_KEY
    is_synthetic     BOOLEAN       NOT NULL DEFAULT FALSE,

    -- ── MSDS 원본 섹션 데이터 (JSONB) ──────────────────────────────────────
    -- detail01 ~ detail16 전체 구조체를 유실 없이 보존
    -- GIN 인덱스로 내부 키·값 full-text 검색 지원
    msds_payload     JSONB         NOT NULL,

    -- ── 제약조건 ────────────────────────────────────────────────────────────
    CONSTRAINT pk_staging_msds_chem_id PRIMARY KEY (chem_id)
);
"""

# cas_no가 있는 경우에만 UNIQUE 보장 (NULL/빈 값 제외하는 부분 인덱스)
_DDL_IDX_CAS = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_stg_msds_cas_no
    ON staging_msds_chemical (cas_no)
    WHERE cas_no IS NOT NULL AND cas_no <> '';
"""

# JSONB 내부 키·값 검색을 위한 GIN 인덱스 (msdsItemCode, itemDetail 조회 등)
_DDL_IDX_GIN = """
CREATE INDEX IF NOT EXISTS idx_stg_msds_payload_gin
    ON staging_msds_chemical USING GIN (msds_payload);
"""

# 적재 시각 범위 조회 최적화 (증분 수집 시 유용)
_DDL_IDX_COLLECTED = """
CREATE INDEX IF NOT EXISTS idx_stg_msds_collected_at
    ON staging_msds_chemical (collected_at_utc DESC);
"""


def ensure_table_exists(conn: psycopg2.extensions.connection) -> None:
    """
    staging_msds_chemical 테이블과 필요한 인덱스를 멱등성 있게 생성한다.
    이미 존재하는 경우 IF NOT EXISTS 구문으로 안전하게 스킵된다.
    """
    with conn.cursor() as cur:
        cur.execute(_DDL_CREATE_TABLE)
        cur.execute(_DDL_IDX_CAS)
        cur.execute(_DDL_IDX_GIN)
        cur.execute(_DDL_IDX_COLLECTED)
    conn.commit()
    logger.info("테이블/인덱스 확인 완료: staging_msds_chemical")


# ─────────────────────────────────────────────────────────────────────────────
# 레코드 변환 — raw dict → PostgreSQL 행(row) 매핑
# ─────────────────────────────────────────────────────────────────────────────

def _build_row(record: dict) -> dict:
    """
    수집기(msds_api_collector.py)가 생성한 raw record 1건을
    PostgreSQL 컬럼 딕셔너리로 변환한다.

    raw record 구조 (수집기 참고):
        {
            "_query_name":      "벤젠",
            "_cas_no":          "71-43-2",
            "_chem_id":         "001008",
            "_collected_at_utc": "2026-06-26T02:37:35Z",
            "list_info":        { chemNameKor, chemEngNm, casNo, unNo, ... },
            "detail01":         { "section": "화학제품과 회사에 관한 정보", "data": [...] },
            ...
            "detail16":         { "section": "그 밖의 참고사항",           "data": [...] },
        }

    Returns:
        PostgreSQL 컬럼명 → 파이썬 값 딕셔너리
    """
    list_info: dict = record.get("list_info") or {}

    # 기본 식별자: 수집기 내부 필드(_xxx) 우선, 없으면 list_info fallback
    chem_id: str        = (record.get("_chem_id") or list_info.get("chemId") or "").strip()
    cas_no:  str | None = (record.get("_cas_no")  or list_info.get("casNo")  or None)
    un_no:   str | None = (list_info.get("unNo")   or None)
    name_ko: str | None = (list_info.get("chemNameKor") or None)
    name_en: str | None = (list_info.get("chemEngNm")   or None)

    # msds_payload: detail01 ~ detail16 섹션 전체 + list_info (원본 보존)
    # 수집기의 _xxx 내부 메타 필드는 포함하지 않음 (일반 컬럼으로 분리됨)
    payload: dict = {}
    if list_info:
        payload["list_info"] = list_info
    for key in SECTION_KEYS:
        section = record.get(key)
        if section is not None:
            payload[key] = section

    # quality_flag: chem_id와 name_ko 중 하나라도 없으면 MISSING_KEY
    quality_flag: str = "OK" if (chem_id and name_ko) else "MISSING_KEY"

    return {
        "chem_id":          chem_id,
        "cas_no":           cas_no,
        "un_no":            un_no,
        "name_ko":          name_ko,
        "name_en":          name_en,
        "source_system":    SOURCE_SYSTEM,
        "source_table":     SOURCE_TABLE,
        "collected_at_utc": datetime.now(tz=timezone.utc),
        "quality_flag":     quality_flag,
        "is_synthetic":     False,
        # JSON 직렬화: ensure_ascii=False로 한글 유지,
        # SQL의 ::jsonb 캐스트로 PostgreSQL JSONB 타입에 바인딩
        "msds_payload":     json.dumps(payload, ensure_ascii=False),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Upsert SQL — ON CONFLICT로 재실행 멱등성 보장
# ─────────────────────────────────────────────────────────────────────────────
_UPSERT_SQL = """
INSERT INTO staging_msds_chemical (
    chem_id, cas_no, un_no, name_ko, name_en,
    source_system, source_table, collected_at_utc,
    quality_flag, is_synthetic, msds_payload
) VALUES (
    %(chem_id)s,
    %(cas_no)s,
    %(un_no)s,
    %(name_ko)s,
    %(name_en)s,
    %(source_system)s,
    %(source_table)s,
    %(collected_at_utc)s,
    %(quality_flag)s,
    %(is_synthetic)s,
    %(msds_payload)s::jsonb      -- 문자열 → JSONB 타입 명시 캐스트
)
ON CONFLICT (chem_id) DO UPDATE SET
    -- 식별자 컬럼 갱신 (CAS 번호 정정 대응)
    cas_no           = EXCLUDED.cas_no,
    un_no            = EXCLUDED.un_no,
    name_ko          = EXCLUDED.name_ko,
    name_en          = EXCLUDED.name_en,
    -- 메타데이터는 항상 최신 실행 기준으로 덮어씀
    source_system    = EXCLUDED.source_system,
    source_table     = EXCLUDED.source_table,
    collected_at_utc = EXCLUDED.collected_at_utc,
    quality_flag     = EXCLUDED.quality_flag,
    -- JSONB 페이로드 전체 교체 (|| 연산자로 병합이 아닌 완전 교체)
    msds_payload     = EXCLUDED.msds_payload
;
"""


def upsert_to_postgresql(
    conn: psycopg2.extensions.connection,
    record: dict,
) -> None:
    """
    수집기 raw record 1건을 PostgreSQL에 Upsert한다.

    커밋(commit)은 호출자가 직접 제어한다.
    배치 커밋 패턴을 지원하기 위해 이 함수 내에서는 commit을 수행하지 않는다.

    Args:
        conn:   psycopg2 연결 객체. autocommit=False 상태여야 한다.
        record: 수집기가 생성한 raw dict (list_info + detail01~16 포함).

    Raises:
        ValueError:       chem_id가 없는 레코드 (스킵 신호).
        psycopg2.Error:   DB 오류. 호출자에서 conn.rollback() 처리 필요.
    """
    row = _build_row(record)

    if not row["chem_id"]:
        name = record.get("_query_name", "UNKNOWN")
        logger.warning("chem_id 없는 레코드 스킵 — query_name=%s", name)
        raise ValueError(f"chem_id 없음: {name}")

    with conn.cursor() as cur:
        cur.execute(_UPSERT_SQL, row)

    logger.debug("Upsert 완료: chem_id=%s  quality_flag=%s", row["chem_id"], row["quality_flag"])


# ─────────────────────────────────────────────────────────────────────────────
# 오케스트레이터 — raw JSON 파일 → PostgreSQL 배치 적재
# ─────────────────────────────────────────────────────────────────────────────

def run_pg_load(raw_json_path: str | None = None) -> None:
    """
    raw JSON 파일을 읽어 staging_msds_chemical 테이블에 배치 Upsert를 수행한다.

    Args:
        raw_json_path:
            특정 raw JSON 파일 경로. None이면 RAW_DIR 내 날짜 기준
            최신 파일(msds_chemical_YYYYMMDD_raw.json)을 자동으로 선택.

    Raises:
        FileNotFoundError: raw 파일이 존재하지 않을 때.
        psycopg2.Error:    DB 연결 또는 치명적 오류 발생 시.
    """
    # ── 1. 적재 대상 파일 결정 ─────────────────────────────────────────────
    if raw_json_path is None:
        files = sorted(glob.glob(os.path.join(RAW_DIR, "msds_chemical_*_raw.json")))
        if not files:
            raise FileNotFoundError(f"raw MSDS 파일을 찾을 수 없습니다: {RAW_DIR}")
        raw_json_path = files[-1]  # 날짜 내림차순 정렬 → 최신 파일

    logger.info("적재 대상 파일: %s", raw_json_path)

    with open(raw_json_path, encoding="utf-8") as f:
        payload = json.load(f)

    records: list[dict] = payload.get("data", [])
    total = len(records)
    logger.info("총 레코드: %d건", total)

    if total == 0:
        logger.warning("적재할 레코드가 없습니다.")
        return

    # ── 2. PostgreSQL 연결 및 스키마 보장 ─────────────────────────────────
    conn = psycopg2.connect(**PG_CONFIG)
    conn.autocommit = False  # 명시적 트랜잭션 제어
    logger.info("PostgreSQL 연결 완료: %s:%s/%s", PG_CONFIG["host"], PG_CONFIG["port"], PG_CONFIG["dbname"])

    try:
        ensure_table_exists(conn)

        success_cnt = 0
        skip_cnt    = 0
        error_cnt   = 0

        for i, record in enumerate(records, 1):
            chem_id = record.get("_chem_id", "?")

            try:
                upsert_to_postgresql(conn, record)
                success_cnt += 1

            except ValueError:
                # chem_id 없음 → 데이터 문제, 스킵
                skip_cnt += 1
                continue

            except psycopg2.Error as e:
                # DB 오류 → 현재 트랜잭션만 롤백 후 계속 진행
                conn.rollback()
                logger.error("[%d/%d] Upsert 실패 chem_id=%s: %s", i, total, chem_id, e)
                error_cnt += 1
                continue

            # BATCH_SIZE마다 중간 커밋 (메모리 효율 + 부분 장애 복구)
            if success_cnt % BATCH_SIZE == 0:
                conn.commit()
                logger.info("중간 커밋: %d/%d 처리 완료", i, total)

        # 잔여 레코드 최종 커밋
        conn.commit()
        logger.info(
            "PostgreSQL 적재 완료 — 성공: %d건 / 스킵: %d건 / 실패: %d건",
            success_cnt, skip_cnt, error_cnt,
        )

    except Exception:
        conn.rollback()
        logger.exception("치명적 오류 발생 — 전체 롤백")
        raise

    finally:
        conn.close()
        logger.info("PostgreSQL 연결 종료")


# ─────────────────────────────────────────────────────────────────────────────
# 단독 실행 진입점
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_pg_load()
