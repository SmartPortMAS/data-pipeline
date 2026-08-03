"""
MSDS PostgreSQL 적재 모듈 — JSONB 기반 마스터 DB
==========================================================
파이프라인 위치: [수집기] → raw JSON → [이 모듈] → PostgreSQL

전략:
- 수집기(msds_api_collector.py)가 생성한 raw record에서
  기본 식별자(chem_id, cas_no 등)는 일반 컬럼으로 꺼내고,
  detail01 ~ detail16 의 16개 섹션 구조체 전체는 msds_payload (JSONB) 단일 컬럼에 보존.
- ON CONFLICT (chem_id) DO UPDATE 로 재실행 시 멱등성(Idempotency) 보장.
- 팀 공통 메타데이터 규칙 (common_preprocessing.py §7) 준수.

스키마 소유권:
- msds_chemical 테이블/인덱스는 backend(Alembic, backend/alembic/versions/)가 소유한다.
  backend가 동일 테이블에 실시간 조회+적재(lazy fetch-and-cache)도 하기 때문에
  스키마를 두 곳에서 관리하면 드리프트가 생긴다.
- 이 모듈은 테이블이 이미 존재한다고 가정하고 insert(upsert)만 수행한다.
  로컬 개발 등에서 백엔드 마이그레이션을 먼저 적용해야 한다:
      cd backend && alembic upgrade head

외부 의존 라이브러리:
    pip install psycopg2-binary python-dotenv

환경변수 (.env 또는 OS 환경변수, 기본값 없음 — 미설정 시 에러):
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
"""

import glob
import html
import json
import logging
import os
import re
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
# 기본값 없음: 하드코딩된 자격증명 fallback으로 조용히 엉뚱한 DB에 붙는 걸 방지.
# ─────────────────────────────────────────────────────────────────────────────
_REQUIRED_PG_VARS = ["POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"]
_missing = [k for k in _REQUIRED_PG_VARS if not os.getenv(k)]
if _missing:
    raise RuntimeError(f"DB 접속정보가 없습니다. .env에 설정하세요: {', '.join(_missing)}")

PG_CONFIG: dict = {
    "host":            os.environ["POSTGRES_HOST"],
    "port":            int(os.environ["POSTGRES_PORT"]),
    "dbname":          os.environ["POSTGRES_DB"],
    "user":            os.environ["POSTGRES_USER"],
    "password":        os.environ["POSTGRES_PASSWORD"],
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
# 테이블/인덱스 정의는 backend(Alembic)가 소유한다.
# backend/alembic/versions/0001_create_msds_chemical.py 참고.
# 이 모듈은 스키마를 생성하지 않고 insert(upsert)만 수행한다.
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

    row = {
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
    row.update(_extract_structured_values(record))
    return row


# ─────────────────────────────────────────────────────────────────────────────
# 정형 값 컬럼 추출 (backend Alembic 0007)
# ─────────────────────────────────────────────────────────────────────────────
# msds_payload 안에 묻혀 있던 "값 하나짜리" 항목을 일반 컬럼으로도 적재한다.
# 챗봇이 인화점·용기등급 같은 확정된 값을 벡터 검색(근사)이 아니라 컬럼 조회
# (결정적)로 답하기 위한 것 — 0007 마이그레이션 주석에 배경이 있다.
#
# 코드 매핑은 msds_preprocessor._flatten_record()와 동일하게 유지해야 한다.
# 그쪽은 분석용 staging CSV를, 이쪽은 서비스 테이블을 만든다.
_STRUCTURED_ITEMS: dict[str, tuple[str, str]] = {
    "flash_point_text":      ("detail09", "I14"),
    "boiling_point_text":    ("detail09", "I12"),
    "vapor_pressure_text":   ("detail09", "I22"),
    "specific_gravity_text": ("detail09", "I28"),
    "packing_group":         ("detail14", "N08"),
    "ems_fire":              ("detail14", "N1202"),
    "ems_spill":             ("detail14", "N1204"),
    "signal_word":           ("detail02", "B0404"),
    "exposure_limit_kr":     ("detail08", "H0202"),
}

# KOSHA가 값 없음을 나타내는 문자열. NULL로 정규화하지 않으면 "인화점: 자료없음"이
# 값처럼 프롬프트에 실려 LLM이 근거로 인용한다.
_NULL_VALUES: frozenset[str] = frozenset({"자료없음", "해당없음", "-", "", "N/A", "없음"})

# 원문에 HTML 엔티티가 그대로 들어있다(예: 인화점 "&lt; 20 ℃").
# 두 번 이상 이스케이프된 값도 실제로 있어서(detail08의 "&amp;lt;19.6%") 더 이상
# 변하지 않을 때까지 반복해서 푼다. 상한은 병리적 입력 방어용이고 실측 최대는 2회다.
# backend의 app/agents/chatbot/chunking.unescape()와 동작이 같아야 한다 — 같은 원문에서
# 컬럼과 청크가 서로 다른 문자열을 만들면 근거 대조가 안 된다.
_MAX_UNESCAPE_PASSES = 4


def _unescape(text: str) -> str:
    for _ in range(_MAX_UNESCAPE_PASSES):
        decoded = html.unescape(text)
        if decoded == text:
            return decoded
        text = decoded
    return text


def _item_detail(record: dict, section: str, code: str) -> str | None:
    """지정 섹션에서 msdsItemCode가 일치하는 itemDetail을 꺼내 정규화한다."""
    data = (record.get(section) or {}).get("data")
    if not isinstance(data, list):
        return None
    for item in data:
        if (item.get("msdsItemCode") or "").strip() != code:
            continue
        value = _unescape((item.get("itemDetail") or "").strip())
        return None if value in _NULL_VALUES else value
    return None


def _extract_structured_values(record: dict) -> dict:
    values = {name: _item_detail(record, sec, code)
              for name, (sec, code) in _STRUCTURED_ITEMS.items()}

    # 인화점 섭씨값 — "-11 ℃", "&lt; 20 ℃"(→ "< 20 ℃"), "-1~565 ℃"처럼 부등호·범위·
    # 단위·출처가 섞여 있어 첫 숫자만 취한다. 뽑히지 않으면 NULL로 두고 원문
    # (flash_point_text)을 쓰게 한다 — 억지 파싱값으로 잘못된 수치를 답변에 싣는
    # 것보다 값이 없다고 하는 편이 안전하다.
    text = values.get("flash_point_text")
    match = re.search(r"-?\d+\.?\d*", text) if text else None
    values["flash_point_celsius"] = float(match.group()) if match else None
    return values


# ─────────────────────────────────────────────────────────────────────────────
# Upsert SQL — ON CONFLICT로 재실행 멱등성 보장
# ─────────────────────────────────────────────────────────────────────────────
_UPSERT_SQL = """
INSERT INTO msds_chemical (
    chem_id, cas_no, un_no, name_ko, name_en,
    source_system, source_table, collected_at_utc,
    quality_flag, is_synthetic, msds_payload,
    -- 정형 값 컬럼 (Alembic 0007)
    flash_point_text, flash_point_celsius, boiling_point_text,
    vapor_pressure_text, specific_gravity_text,
    packing_group, ems_fire, ems_spill, signal_word, exposure_limit_kr
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
    %(msds_payload)s::jsonb,     -- 문자열 → JSONB 타입 명시 캐스트
    %(flash_point_text)s,
    %(flash_point_celsius)s,
    %(boiling_point_text)s,
    %(vapor_pressure_text)s,
    %(specific_gravity_text)s,
    %(packing_group)s,
    %(ems_fire)s,
    %(ems_spill)s,
    %(signal_word)s,
    %(exposure_limit_kr)s
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
    msds_payload     = EXCLUDED.msds_payload,
    -- 정형 값도 payload와 같은 원본에서 나오므로 함께 교체한다.
    -- 값이 사라진 항목(자료없음으로 바뀜)은 NULL로 덮어써야 옛 값이 남지 않는다.
    flash_point_text      = EXCLUDED.flash_point_text,
    flash_point_celsius   = EXCLUDED.flash_point_celsius,
    boiling_point_text    = EXCLUDED.boiling_point_text,
    vapor_pressure_text   = EXCLUDED.vapor_pressure_text,
    specific_gravity_text = EXCLUDED.specific_gravity_text,
    packing_group         = EXCLUDED.packing_group,
    ems_fire              = EXCLUDED.ems_fire,
    ems_spill             = EXCLUDED.ems_spill,
    signal_word           = EXCLUDED.signal_word,
    exposure_limit_kr     = EXCLUDED.exposure_limit_kr
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
    raw JSON 파일을 읽어 msds_chemical 테이블에 배치 Upsert를 수행한다.
    테이블/인덱스는 backend(Alembic)가 소유하므로, 이 함수 실행 전에
    backend에서 `alembic upgrade head`가 적용되어 있어야 한다.

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

    # ── 2. PostgreSQL 연결 (테이블은 backend Alembic이 이미 생성해 둔 상태여야 함) ──
    conn = psycopg2.connect(**PG_CONFIG)
    conn.autocommit = False  # 명시적 트랜잭션 제어
    logger.info("PostgreSQL 연결 완료: %s:%s/%s", PG_CONFIG["host"], PG_CONFIG["port"], PG_CONFIG["dbname"])

    try:
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
