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

  # -------------------------------------------------------------------------
  # 코드 자동 갱신 — S3 의 zip 이 바뀌었으면 받아서 덮어쓴다.
  #
  # 예전에는 이 단계가 user_data(최초 부팅 1회)에만 있어서, S3 에 새 zip 을
  # 올려도 EC2 는 부팅 때 풀어둔 코드로 계속 돌았다. "로컬·Git 은 고쳤는데
  # EC2 만 옛 코드"인 상태가 배포할 때마다 생겼고, 반영하려면 인스턴스를 새로
  # 띄워야 했다. 이제 zip 만 올리면 다음 정각에 자동 반영된다.
  #
  # ETag 비교로 바뀐 경우에만 내려받는다 — 매시 12MB 를 다시 받을 이유가 없고,
  # 같은 zip 을 덮어쓰다 실행 중 파일이 꼬이는 일도 피한다.
  # 갱신 실패는 수집을 막지 않는다(|| true) — 코드가 낡은 채로라도 수집은 돈다.
  # -------------------------------------------------------------------------
  ETAG_NEW=$(aws s3api head-object --bucket "$BUCKET" --key code/data-pipeline.zip \
             --query ETag --output text 2>/dev/null || echo "")
  ETAG_OLD=$(cat $BASE/code.etag 2>/dev/null || echo "")
  if [ -n "$ETAG_NEW" ] && [ "$ETAG_NEW" != "$ETAG_OLD" ]; then
    echo "코드 갱신 감지 (ETag $ETAG_OLD -> $ETAG_NEW) - 새 zip 적용"
    aws s3 cp "s3://$BUCKET/code/data-pipeline.zip" /tmp/dp.zip --only-show-errors && {
      unzip -oq /tmp/dp.zip -d $BASE || true
      # zip 루트 폴더명이 달라도 data-pipeline 으로 통일 (user_data 와 동일 처리)
      [ -d $BASE/data-pipeline ] || mv $BASE/data-pipeline-* $BASE/data-pipeline
      # 코드가 바뀌어도 API 키는 유지되어야 한다 - .env 를 SSM 에서 다시 받는다
      # (zip 에는 .env 가 없다. 덮어쓰기로 지워지진 않지만, 만약을 대비해 재확인)
      [ -f $REPO/.env ] || aws ssm get-parameter --region ap-northeast-2 \
          --name /smartport/collector-env --with-decryption \
          --query Parameter.Value --output text > $REPO/.env
      echo "$ETAG_NEW" > $BASE/code.etag
      echo "코드 갱신 완료"
    } || echo "코드 갱신 실패 - 기존 코드로 계속"
  fi

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
