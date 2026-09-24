#!/usr/bin/env bash
# 수집 EC2 에서 실행되는 배포 스크립트. GitHub Actions(deploy.yml)가 SSH stdin 으로 넘겨 실행한다.
# 서버에서 손으로 돌려도 된다:  SHA=<커밋> bash deploy/deploy.sh
#
# 환경변수
#   APP_DIR   data-pipeline 체크아웃 경로   (기본 /home/ubuntu/data-pipeline)
#   SERVICE   systemd 서비스 이름            (기본 smartport-pipeline)
#   SHA       배포할 커밋. 비우면 origin/main 최신
#
# 스키마(표·뷰)는 backend Alembic 이 만든다. 여기서는 마이그레이션을 하지 않는다 —
# backend 쪽 `alembic upgrade head` 가 먼저 RDS 에 적용돼 있어야 로더가 돈다.
#
# 기동 확인: 이 서비스에는 HTTP 헬스체크가 없다. 재시작 후 STABLE_SEC 동안 active 이고
# systemd 가 한 번도 자동 재시작하지 않았으면(NRestarts=0) 성공으로 본다 — import 오류·
# .env 누락처럼 기동 직후 죽는 실패는 여기서 잡힌다. 수집 API 실패는 잡지 못한다
# (스케줄러는 도메인 실패를 로그만 남기고 계속 돈다).

main() {
  set -euo pipefail

  APP_DIR="${APP_DIR:-/home/ubuntu/data-pipeline}"
  SERVICE="${SERVICE:-smartport-pipeline}"
  STABLE_SEC="${STABLE_SEC:-20}"
  export PYTHONUTF8=1

  cd "$APP_DIR"
  local py="$APP_DIR/.venv/bin/python"

  [ -f .env ] || die ".env 가 없습니다: $APP_DIR/.env  (.env.prod.example 참고)"
  [ -x "$py" ] || die "venv 가 없습니다: $py  (deploy/README.md 의 서버 준비 참고)"

  local prev target
  prev="$(git rev-parse HEAD)"
  git fetch --quiet --prune origin main
  target="${SHA:-$(git rev-parse origin/main)}"

  log "현재 $(short "$prev") → 배포 $(short "$target")"
  git reset --hard --quiet "$target"

  log "의존성 설치"
  "$py" -m pip install --quiet --disable-pip-version-check -r requirements.txt

  log "재시작: $SERVICE"
  sudo systemctl restart "$SERVICE"

  if wait_stable; then
    log "배포 완료: $(short "$target")"
    return 0
  fi

  sudo journalctl -u "$SERVICE" -n 60 --no-pager >&2 || true
  log "기동 확인 실패 — 이전 커밋 $(short "$prev") 로 되돌립니다"
  git reset --hard --quiet "$prev"
  "$py" -m pip install --quiet --disable-pip-version-check -r requirements.txt
  sudo systemctl restart "$SERVICE"
  if wait_stable; then
    die "새 버전 기동 실패, 이전 버전으로 복구했습니다."
  fi
  die "새 버전 기동 실패, 이전 버전 복구도 실패했습니다. 서버를 직접 확인하세요."
}

wait_stable() {
  sleep "$STABLE_SEC"
  local state restarts
  state="$(systemctl is-active "$SERVICE" || true)"
  restarts="$(systemctl show -p NRestarts --value "$SERVICE" || echo "?")"
  log "상태: $state, 자동 재시작 횟수: $restarts"
  [ "$state" = "active" ] && [ "$restarts" = "0" ]
}

short() { printf '%s' "${1:0:7}"; }
log() { printf '[deploy] %s\n' "$*"; }
die() { printf '[deploy] 실패: %s\n' "$*" >&2; exit 1; }

# 본문을 함수로 감싸 끝에서 부른다 — bash 가 전체를 다 읽은 뒤 실행하므로,
# 실행 중에 git reset 이 이 파일을 바꿔도 영향이 없다.
main "$@"
