# -*- coding: utf-8 -*-
"""S3 → 로컬 DB 적재 — AWS 수집기가 올린 staging 을 내려받아 적재한다.

[역할 분담 — 보안 설계의 핵심]
  EC2(클라우드) : 수집·전처리 → S3 (공공 API 키만 보유, DB 접속정보 없음)
  이 스크립트(로컬) : S3 → data/staging 다운로드 → 로컬 PostgreSQL 적재
  → DB·Neo4j 비밀번호는 클라우드에 올라가지 않는다.

필요 설정 (data-pipeline/.env 에 추가):
  AWS_ACCESS_KEY_ID=...        # IAM 사용자 smartport-local 의 액세스 키
  AWS_SECRET_ACCESS_KEY=...
  SMARTPORT_S3_BUCKET=smartport-collector-hwham
  (POSTGRES_* 는 기존 그대로 사용)

실행: python cloud_pull.py          # 루트의 데이터_클라우드받기.bat 이 이걸 부른다
"""
import io
import json
import os
import re
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

from dotenv import load_dotenv  # noqa: E402

load_dotenv(os.path.join(BASE_DIR, ".env"))

REGION = os.getenv("AWS_DEFAULT_REGION", "ap-northeast-2")
BUCKET = os.getenv("SMARTPORT_S3_BUCKET")


def download(s3) -> int:
    """s3://BUCKET/staging,mart → data/staging,data/mart. 받은 파일 수 반환."""
    n = 0
    for prefix, local_dir in (("staging/", "data/staging"),
                              ("mart/", "data/mart")):
        os.makedirs(os.path.join(BASE_DIR, local_dir), exist_ok=True)
        token = None
        while True:
            kw = {"Bucket": BUCKET, "Prefix": prefix}
            if token:
                kw["ContinuationToken"] = token
            resp = s3.list_objects_v2(**kw)
            for obj in resp.get("Contents", []):
                name = os.path.basename(obj["Key"])
                if not name:
                    continue
                dest = os.path.join(BASE_DIR, local_dir, name)
                s3.download_file(BUCKET, obj["Key"], dest)
                n += 1
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
    return n


def collector_health(s3) -> None:
    """클라우드 수집기의 마지막 성공 시각을 보여준다 (없으면 조용히 넘어감)."""
    try:
        buf = io.BytesIO()
        s3.download_fileobj(BUCKET, "health/last_run.json", buf)
        h = json.loads(buf.getvalue())
        print(f"  클라우드 수집기 마지막 실행: {h.get('last_run_utc')} (exit={h.get('exit_code')})")
    except Exception:
        print("  (클라우드 수집기 생존 신호 없음 - 아직 EC2 첫 실행 전인지 확인)")


# ── 선박위치 이력 재생 ────────────────────────────────────────────────
#
# 다른 도메인(조위·파고·기상·PORT-MIS)은 원천 API 가 기간 조회를 지원하거나
# staging 이 누적본이라, 최신 staging 만 받아도 밀린 구간이 채워진다.
# 선박위치는 다르다 — API 가 선박당 마지막 한 점만 주므로 staging 은 "그 시각의
# 한 장"이고, PC 가 꺼져 있던 시각의 장은 다시 받을 방법이 없었다
# (2026-09-17 실측: 최근 30일 중 13일 이력 없음).
#
# EC2 가 매시 raw/upa/upa_vessel_position_YYYYMMDDTHHZ_raw.json 을 따로 남기므로
# (upa_collector.save_raw hourly=True), 여기서 아직 안 넣은 장만 골라 시각 순으로
# 전처리 -> UPSERT 한다. 키가 (vessel_uid, received_at_utc) 라 여러 번 넣어도
# 중복이 쌓이지 않는다.
#
# 기존 날짜 스냅샷(YYYYMMDD, 하루 마지막 실행분)도 같이 재생한다 — 시각별 파일이
# 생기기 전 기간에서 건질 수 있는 유일한 조각이다. 날짜 스냅샷은 그날 동안 계속
# 덮어써지므로 "넣었는지"를 파일명이 아니라 ETag 로 기록해, 바뀌면 다시 넣는다.
POSITION_RAW_RE = re.compile(r"upa_vessel_position_(\d{8}(?:T\d{2}Z)?)_raw\.json$")
POSITION_STATE = os.path.join(BASE_DIR, "data", "state", "position_raw_loaded.json")
POSITION_BATCH = 48  # 한 번에 전처리할 장 수 (장당 약 260KB)


def _read_position_state() -> dict:
    try:
        with open(POSITION_STATE, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def _write_position_state(state: dict) -> None:
    os.makedirs(os.path.dirname(POSITION_STATE), exist_ok=True)
    tmp = POSITION_STATE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=0, sort_keys=True)
    os.replace(tmp, POSITION_STATE)  # 쓰다 죽어도 기존 기록이 깨지지 않게


def backfill_vessel_positions(s3) -> int:
    """S3 의 선박위치 원본 중 아직 적재하지 않은 장을 시각 순으로 적재한다.

    반환: 이번에 적재한 장 수.
    """
    import shutil
    import tempfile

    from data_pipeline.common_pg_loader import get_engine, load_csv
    from data_pipeline.upa.upa_loader import TABLE_MAP
    from data_pipeline.upa.upa_preprocess import run_pipeline

    state = _read_position_state()
    pending: list[tuple[str, str, str]] = []  # (정렬키, S3 key, etag)
    token = None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": "raw/upa/upa_vessel_position_"}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for obj in resp.get("Contents", []):
            m = POSITION_RAW_RE.search(obj["Key"])
            if not m:
                continue  # 최신 고정본(upa_vessel_position_raw.json)은 staging 경로가 이미 적재
            etag = obj["ETag"].strip('"')
            if state.get(obj["Key"]) == etag:
                continue
            # 날짜 스냅샷은 "그날 마지막 실행분"이므로 시각 스냅샷보다 뒤에 오게 정렬
            stamp = m.group(1)
            sort_key = stamp if "T" in stamp else stamp + "T99Z"
            pending.append((sort_key, obj["Key"], etag))
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")

    if not pending:
        print("  새로 받을 선박위치 원본 없음")
        return 0

    pending.sort()
    span = [k.replace("T99Z", "") for k in (pending[0][0], pending[-1][0])]
    print(f"  적재 대기 {len(pending)}장 ({span[0]} ~ {span[1]})")
    table, keys = TABLE_MAP["upa_vessel_position_stg.csv"]
    engine = get_engine()
    done = 0
    for i in range(0, len(pending), POSITION_BATCH):
        batch = pending[i:i + POSITION_BATCH]
        work = tempfile.mkdtemp(prefix="pos_backfill_")
        try:
            paths = []
            for _, key, _ in batch:
                dest = os.path.join(work, os.path.basename(key))
                s3.download_file(BUCKET, key, dest)
                paths.append(dest)
            df = run_pipeline("vessel_position", paths, staging_dir=work)
            if not df.empty:
                load_csv(engine, os.path.join(work, "upa_vessel_position_stg.csv"), table, keys)
            # 배치가 끝까지 성공했을 때만 기록한다 — 중간에 죽으면 다음 실행에서 다시 넣는다
            for _, key, etag in batch:
                state[key] = etag
            _write_position_state(state)
            done += len(batch)
        finally:
            shutil.rmtree(work, ignore_errors=True)
    print(f"  선박위치 원본 {done}장 적재")
    return done


def load_to_db(s3) -> list[str]:
    """staging → 로컬 DB. 실패한 도메인 이름 목록을 반환한다(없으면 빈 목록)."""
    from data_pipeline.upa.upa_loader import load_all as upa_load
    from data_pipeline.loaders import (
        tide_pg_loader, wave_pg_loader, weather_pg_loader,
        weather_forecast_pg_loader, portmis_pg_loader, mart_pg_loader,
    )
    steps = [
        ("UPA (선박위치·입출항·시설)", upa_load),
        ("조위", tide_pg_loader.load),
        ("파고", wave_pg_loader.load),
        ("기상 관측", weather_pg_loader.load),
        ("단기예보", weather_forecast_pg_loader.load),
        ("PORT-MIS", portmis_pg_loader.load),
        ("선박위치 이력 (시각별 원본 재생)", lambda: backfill_vessel_positions(s3)),
        ("마트", mart_pg_loader.load),  # 파생 테이블 — 반드시 마지막
    ]
    failed: list[str] = []
    for name, fn in steps:
        try:
            print(f"\n=== 적재: {name} ===")
            fn()
        except Exception as e:  # 한 도메인이 죽어도 나머지는 적재한다
            failed.append(name)
            print(f"[실패] {name}: {e}")
    # 문자 주의: 이 스크립트는 작업 스케줄러가 CP949 콘솔로 돌린다.
    # em-dash 같은 문자를 쓰면 여기서 UnicodeEncodeError 로 죽고, 하필 적재가
    # 다 끝난 뒤라 "무엇이 실패했는지"만 못 보게 된다 (2026-08-15 실증).
    if failed:
        print(f"\n적재 완료 - 실패 {len(failed)}건: {', '.join(failed)}")
        return failed
    print("\n적재 완료 - 전 도메인 성공")
    return []


# ── 두 가지 실패를 구분한다 ────────────────────────────────────────────
#
# (1) 로컬 DB 가 아예 꺼져 있음  — 노트북에서 Docker 를 안 켠 것뿐이다.
#     수집기 문제가 아니므로 조용히 건너뛰고 정상 종료한다. Docker 를 켜는
#     날 다음 정시에 저절로 따라잡는다. 대부분 도메인은 최신 staging 만으로
#     밀린 구간이 채워지고, 선박위치만 예외라 시각별 원본을 재생한다
#     (backfill_vessel_positions 참고 — "재생할 필요 없다"던 예전 가정이
#     선박위치 이력 손실의 원인이었다, 2026-09-17).
# (2) DB 는 살아 있는데 특정 도메인 적재가 실패  — 진짜 이상이다.
#     이때는 예전처럼 종료코드를 1 로 남겨 스케줄러에 실패로 보고한다.
#
# 이 구분이 없으면 노트북을 안 켠 날마다 매시 실패 창이 떠서, 정작 진짜
# 실패가 섞여 들어와도 알아채지 못한다(2026-08-28: 사흘간 매시 실패 보고).
def db_reachable(timeout: float = 3.0) -> bool:
    import socket  # noqa: PLC0415

    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", "5433"))
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def main() -> None:
    if not BUCKET:
        sys.exit("SMARTPORT_S3_BUCKET 이 .env 에 없습니다 - aws/설치가이드.md 6단계 참조")

    if not db_reachable():
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5433")
        print(f"로컬 DB({host}:{port})가 꺼져 있어 이번 적재를 건너뜁니다.")
        print("  수집기 문제가 아닙니다 - EC2 는 계속 수집해 S3 에 쌓고 있습니다.")
        print("  Docker Desktop 을 켜두면 다음 정시(매시 35분)에 자동으로 따라잡습니다.")
        return

    import boto3

    s3 = boto3.client("s3", region_name=REGION)
    print(f"S3 버킷: {BUCKET}")
    collector_health(s3)
    print("다운로드 중...")
    n = download(s3)
    print(f"  {n}개 파일 수신")
    if n == 0:
        sys.exit("받은 파일이 없습니다 - EC2 수집기가 돌았는지 확인 (aws/설치가이드.md 8단계)")
    failed = load_to_db(s3)
    # 한 도메인이라도 실패하면 종료코드를 0 이 아닌 값으로 남긴다.
    # 작업 스케줄러가 매시 무인 실행하므로, 성공으로 보고되면 아무도 모른다 —
    # 실제로 선박 위치 적재가 5시간 동안 실패하는 동안 스케줄러는 "성공"이었다.
    if failed:
        sys.exit(f"적재 실패 {len(failed)}건: {', '.join(failed)}")


if __name__ == "__main__":
    main()
