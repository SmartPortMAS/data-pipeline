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


def load_to_db() -> None:
    """staging → 로컬 DB. run_pipeline 의 적재 단계와 동일한 로더를 쓴다."""
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
        ("마트", mart_pg_loader.load),  # 파생 테이블 — 반드시 마지막
    ]
    fails = 0
    for name, fn in steps:
        try:
            print(f"\n=== 적재: {name} ===")
            fn()
        except Exception as e:  # 한 도메인이 죽어도 나머지는 적재한다
            fails += 1
            print(f"[실패] {name}: {e}")
    # 콘솔이 CP949 라 em-dash 같은 문자에서 UnicodeEncodeError 가 난다 (적재는 이미 끝난 뒤라 더 아깝다)
    if fails:
        print(f"\n적재 완료 - 실패 {fails}건 (위 로그 확인)")
    else:
        print("\n적재 완료 - 전 도메인 성공")


def main() -> None:
    if not BUCKET:
        sys.exit("SMARTPORT_S3_BUCKET 이 .env 에 없습니다 - aws/설치가이드.md 6단계 참조")
    import boto3

    s3 = boto3.client("s3", region_name=REGION)
    print(f"S3 버킷: {BUCKET}")
    collector_health(s3)
    print("다운로드 중...")
    n = download(s3)
    print(f"  {n}개 파일 수신")
    if n == 0:
        sys.exit("받은 파일이 없습니다 - EC2 수집기가 돌았는지 확인 (aws/설치가이드.md 8단계)")
    load_to_db()


if __name__ == "__main__":
    main()
