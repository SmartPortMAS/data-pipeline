#!/bin/bash
# ===========================================================================
# EC2 정시 수집 잡 — 수집·전처리(staging CSV)까지만 하고 S3 로 올린다
# ===========================================================================
# DB 적재는 하지 않는다(--skip-db). 이 설계가 보안의 핵심이다:
#   클라우드에는 공공 API 키만 있고, DB·Neo4j 비밀번호는 아예 올라오지 않는다.
#   적재는 로컬에서 데이터_클라우드받기.bat 이 S3 를 내려받아 수행한다.
#
# cron 이 매시 정각에 이 스크립트를 부른다 (ec2_user_data.sh 가 등록).
# 로그: /opt/smartport/logs/collect_YYYYMMDD.log
# ===========================================================================
set -u
BASE=/opt/smartport
REPO=$BASE/data-pipeline
LOG=$BASE/logs/collect_$(date +%Y%m%d).log
source $BASE/collector.conf   # BUCKET= 이 파일에 있다 (user_data 가 생성)

cd $REPO
{
  echo "===== $(date -u +%FT%TZ) 수집 시작 ====="

  # 수집 + 전처리만 (staging CSV 생성). 포털 장애 시에도 도메인별 독립 실행이라
  # 성공한 도메인의 staging 은 남는다.
  python3.11 -m data_pipeline.run_pipeline all --skip-db
  RC=$?

  # staging/mart/raw 를 S3 로. staging 은 --delete 로 원본과 동일하게 유지,
  # raw 는 이력 보존을 위해 추가만 한다.
  aws s3 sync data/staging "s3://$BUCKET/staging" --delete --only-show-errors
  aws s3 sync data/mart    "s3://$BUCKET/mart"    --only-show-errors
  aws s3 sync data/raw     "s3://$BUCKET/raw"     --only-show-errors

  # 수집기 생존 신호 — 로컬 mart.pipeline_health 와 별개로, 클라우드 수집기
  # 자체의 마지막 성공 시각을 남긴다 (대시보드 신선도 배지가 소비 가능)
  echo "{\"last_run_utc\": \"$(date -u +%FT%TZ)\", \"exit_code\": $RC, \"host\": \"$(hostname)\"}" > /tmp/last_run.json
  aws s3 cp /tmp/last_run.json "s3://$BUCKET/health/last_run.json" --only-show-errors

  echo "===== $(date -u +%FT%TZ) 종료 (exit=$RC) ====="
} >> "$LOG" 2>&1

# 로그 7일 보관
find $BASE/logs -name "collect_*.log" -mtime +7 -delete
