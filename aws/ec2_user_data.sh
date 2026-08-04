#!/bin/bash
# ===========================================================================
# EC2 최초 부팅 스크립트 (user data) — 인스턴스 시작 시 1회 자동 실행
# ===========================================================================
# 사용법: EC2 시작 화면 맨 아래 "고급 세부 정보 > 사용자 데이터"에 이 파일
# 전체를 붙여넣는다. 붙여넣기 전에 아래 BUCKET 한 줄만 본인 버킷명으로 수정.
#
# 전제 (설치가이드.md 의 1~4단계):
#   1) S3 버킷 생성 + data-pipeline.zip 업로드
#   2) SSM Parameter Store 에 /smartport/collector-env (SecureString) 등록
#   3) IAM 역할(smartport-collector-role)에 iam_policy.json 정책 연결
#   4) 이 인스턴스에 그 IAM 역할을 붙여서 시작
# ===========================================================================
set -eux

# ★★★ 여기만 수정 — 본인이 만든 버킷 이름 ★★★
BUCKET=smartport-collector-hwham

BASE=/opt/smartport
mkdir -p $BASE/logs

# 1) 파이썬 + cron (Amazon Linux 2023 은 cron 이 기본 미설치)
dnf install -y python3.11 python3.11-pip cronie unzip
systemctl enable --now crond

# 2) 파이프라인 코드 (S3 에 올려둔 zip)
aws s3 cp "s3://$BUCKET/code/data-pipeline.zip" /tmp/dp.zip
unzip -o /tmp/dp.zip -d $BASE
# zip 루트 폴더명이 달라도 data-pipeline 으로 통일
[ -d $BASE/data-pipeline ] || mv $BASE/data-pipeline-* $BASE/data-pipeline

# 3) 의존성 — 수집·전처리에 필요한 것만 (DB 드라이버는 로더 임포트용 최소 포함)
python3.11 -m pip install --no-cache-dir \
    pandas numpy requests python-dotenv sqlalchemy psycopg2-binary

# 4) API 키 — SSM SecureString 하나에 .env 전문이 들어 있다.
#    DB·Neo4j 접속정보는 여기 없어야 정상이다 (클라우드는 DB 에 접속하지 않는다).
aws ssm get-parameter --region ap-northeast-2 \
    --name /smartport/collector-env --with-decryption \
    --query Parameter.Value --output text > $BASE/data-pipeline/.env
chmod 600 $BASE/data-pipeline/.env

# 5) 수집 스크립트 + 버킷 설정
cp $BASE/data-pipeline/aws/collect_and_sync.sh $BASE/collect_and_sync.sh
sed -i 's/\r$//' $BASE/collect_and_sync.sh   # Windows 에서 zip 됐어도 CRLF 제거
chmod +x $BASE/collect_and_sync.sh
echo "BUCKET=$BUCKET" > $BASE/collector.conf

# 6) 매시 정각 실행 등록 + 즉시 1회 실행 (설치 검증을 겸한다)
echo "0 * * * * root $BASE/collect_and_sync.sh" > /etc/cron.d/smartport-collect
$BASE/collect_and_sync.sh || true

echo "SETUP DONE $(date -u +%FT%TZ)" >> $BASE/logs/setup.log
