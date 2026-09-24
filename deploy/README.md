# 수집 EC2 배포 (GitHub Actions)

수집 스케줄러(`pipeline_scheduler`)를 **수집 전용 EC2** 에서 systemd 로 상시 띄우고, 수집한 데이터를
**RDS 에 바로 적재**한다. backend EC2 와는 다른 서버다.

```
공공 API ──▶ [수집 EC2] pipeline_scheduler ──▶ RDS PostgreSQL ◀── [backend EC2]
               수집 → 전처리 → 적재 (5~30분 주기, 일 1회 계열 포함)
```

2026-09-24 에 바뀐 것:
- **S3 경로 폐기.** 예전 구조(수집 EC2 → S3 → 로컬 `cloud_pull.py` → 로컬 DB)와 `aws/` 스크립트를 지웠다.
- **raw 원본을 남기지 않는다.** 회차가 끝나면 그 회차가 쓴 raw 파일을 지운다(`data_pipeline/raw_cleanup.py`).
  이미 있던 raw 파일은 건드리지 않는다. 로컬 PC 에서 돌려도 같다. 디버깅으로 남기려면 `.env` 에 `KEEP_RAW=1`.
- **스키마는 backend Alembic 이 만든다.** 이 배포는 마이그레이션을 하지 않는다. RDS 에 backend 의
  `alembic upgrade head`(0028 `upa_*` · 0029 `mart`)가 먼저 적용돼 있어야 로더가 돈다.

```
main 머지 → Actions: Deploy (EC2)
  1. CI (ci.yml)    의존성 설치 · 스케줄러 경로 import · 스케줄 표(--dry-run) · 스크립트 문법
  2. SSH ──▶ 수집 EC2   deploy/deploy.sh
                          git reset --hard <커밋> → pip install → systemctl restart
                          20초 뒤 active 이고 자동 재시작 0회인지 확인 ─ 아니면 이전 커밋으로 복구
```

> 기동 확인은 "프로세스가 죽지 않았다"까지다. 이 서비스엔 HTTP 헬스체크가 없고, 스케줄러는
> 도메인 수집이 실패해도 로그만 남기고 계속 돈다. **수집이 실제로 되는지는 로그로 본다**(4절).

---

## 1. 수집 EC2 준비 (한 번만)

Ubuntu, 사용자 `ubuntu`, 경로 `/home/ubuntu/data-pipeline` 기준. 다르게 쓰면 서비스 파일과
GitHub 변수(3절)를 같이 바꾼다.

### 1-1. 인스턴스 · 네트워크
- **Elastic IP** 를 붙인다(재시작해도 IP 가 바뀌지 않게 — 배포 SSH 시크릿이 IP 에 묶인다).
- 보안 그룹 인바운드: **22 만** — GitHub Actions 러너가 접속한다(러너 IP 가 고정이 아니라 `0.0.0.0/0`,
  키 인증만). 이 서버는 외부에 서비스를 열지 않는다.
- **RDS 보안 그룹**: 인바운드 PostgreSQL 5432 에 **수집 EC2 의 보안 그룹**을 추가한다. RDS 와 같은 VPC 면
  RDS 주소가 사설 IP 로 풀리므로 탄력적 IP 가 아니라 보안 그룹으로 허용해야 한다(backend 때와 같은 이유).
- 수집 API 는 아웃바운드만 쓴다(기본 보안 그룹은 아웃바운드 전체 허용).

### 1-2. Python 3.10 · 코드
로컬·CI 가 3.10 이다. Ubuntu 24.04 기본은 3.12 라 3.10 을 따로 깐다(backend EC2 와 같은 방법).
3.12 에서 `requirements.txt` 가 설치·동작하는지는 확인하지 않았다.

```bash
sudo apt update && sudo apt install -y git curl
sudo add-apt-repository -y ppa:deadsnakes/ppa && sudo apt update
sudo apt install -y python3.10 python3.10-venv

git clone https://github.com/SmartPortMAS/data-pipeline.git ~/data-pipeline   # public 레포
cd ~/data-pipeline && git checkout main
python3.10 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

### 1-3. .env
로컬에 보관한 `data-pipeline/.env.prod`(`.env.prod.example` 기준)를 서버에 넣는다.
```bash
# 로컬 PC (Git Bash)
scp -i <키.pem> data-pipeline/.env.prod ubuntu@<수집EC2>:/home/ubuntu/data-pipeline/.env
# 서버
cd ~/data-pipeline && sed -i 's/\r$//; s/^[[:space:]]*//' .env && chmod 600 .env
cut -d= -f1 .env | grep -v '^#' | grep -v '^$'      # 키 이름만 확인(값은 출력 안 함)
```
- 공공 API 키 7종(`UPA_SERVICE_KEY` · `PORT_MIS_API_KEY` · `KHOA_API_KEY` · `KMA_API_KEY` ·
  `KMA_BUOY_AUTH_KEY` · `MMAF_API_KEY` · `KOSHA_MSDS_API_KEY`)
- DB: `DATABASE_URL`(`postgresql+psycopg2://...`) **와** `POSTGRES_*` 를 둘 다 RDS 로
  — backend 는 `+asyncpg`, 여기는 `+psycopg2` 다. 주소·계정은 같다.
- 선박제원 전용 키(`VSSL_SPEC_API_KEY`)가 없으면 `PORT_MIS_API_KEY` 로 대신한다.
- Neo4j 값은 스케줄러가 쓰지 않는다(수동 적재기 전용). 있어도 된다.

RDS 접속 확인:
```bash
nc -zv -w 5 <RDS 엔드포인트> 5432      # succeeded
```

### 1-4. systemd
```bash
sudo cp ~/data-pipeline/deploy/smartport-pipeline.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable smartport-pipeline
```
`enable` 만 한다. 첫 배포(`deploy.sh` 의 restart)가 켠다. 손으로 먼저 돌려 보려면
`sudo systemctl start smartport-pipeline && journalctl -u smartport-pipeline -f`.

**RDS 에 backend 마이그레이션이 먼저 적용돼 있어야 한다.** 표가 없으면 로더가
"backend 에서 alembic upgrade head 를 먼저 실행하세요" 로 실패한다(스케줄러는 계속 돈다).

---

## 2. GitHub Actions → 수집 EC2 SSH 키

서버에서:
```bash
ssh-keygen -t ed25519 -f ~/gha_deploy -N "" -C "github-actions-deploy-pipeline"
cat ~/gha_deploy.pub >> ~/.ssh/authorized_keys
cat ~/gha_deploy                                         # → EC2_SSH_KEY
awk -v h=<Elastic IP> '{print h, $1, $2}' /etc/ssh/ssh_host_*_key.pub    # → EC2_KNOWN_HOSTS
rm ~/gha_deploy ~/gha_deploy.pub
```

## 3. GitHub 설정 (`SmartPortMAS/data-pipeline` → Settings → Secrets and variables → Actions)

| 종류 | 이름 | 값 |
|---|---|---|
| Secret | `EC2_HOST` | 수집 EC2 Elastic IP |
| Secret | `EC2_USER` | `ubuntu` |
| Secret | `EC2_SSH_KEY` | 2절 개인키 전체(`-----BEGIN` ~ `END-----`) |
| Secret | `EC2_KNOWN_HOSTS` | 2절 `awk` 출력 전체. 각 줄 맨 앞 IP 가 `EC2_HOST` 와 글자까지 같아야 한다 |
| Variable | `EC2_APP_DIR` | 선택. 기본 `/home/ubuntu/data-pipeline` |
| Variable | `EC2_SERVICE` | 선택. 기본 `smartport-pipeline` |

backend 레포 시크릿과 **이름은 같지만 값은 수집 EC2 것**이다(레포마다 따로 저장된다).
API 키·DB 비밀번호는 GitHub 에 넣지 않는다 — 서버 `.env` 에만 있다.

---

## 4. 운영

**배포:** dev → main 머지. 수동 재배포는 Actions → Deploy (EC2) → Run workflow(브랜치 `main`).

**수집이 되는지 보기**
```bash
journalctl -u smartport-pipeline -f                      # 실시간
journalctl -u smartport-pipeline --since "1 hour ago" | grep -E "완료|실패"
tail -f ~/data-pipeline/data/logs/pipeline_scheduler.log # 같은 내용, 10MB x 5 회전
```
`[vessel] 완료`, `[portmis] 완료` … 가 주기대로 찍히면 된다. `실패` 뒤 traceback 이 원인이다.

**알려진 동작**
- 기동 직후 실시간 계열(vessel·portmis·tide·weather·wave·weather_forecast)을 1회 돌린다 — 배포마다 API 호출이 한 번 더 나간다.
- `vessel` · `port_call` · `upa_master` 는 같은 raw 폴더를 써서 한 번에 하나만 돈다. 매일 04:20 `port_call`
  (선박당 1회 호출, 약 550회) 동안 `vessel` 5분 회차가 밀리거나 건너뛸 수 있다.
- MSDS 는 스케줄에 없다(수동 배치).

**자주 나는 실패**
| 증상 | 원인 |
|---|---|
| Actions `Host key verification failed` / `Connection timed out` / `Permission denied (publickey)` | backend 배포 때와 같다 — `EC2_KNOWN_HOSTS` IP 불일치 / 보안 그룹 22 / `authorized_keys` |
| `[deploy] 상태: activating/failed` 또는 자동 재시작 > 0 | 기동 직후 죽음 — 로그에 찍힌 journalctl 출력. 흔한 건 `.env` 누락 키(`PORT_MIS_API_KEY` 는 import 시점에 필수) |
| 로그에 `테이블 "..."이 존재하지 않습니다` | RDS 에 backend 마이그레이션 미적용 |
| 로그에 DB `timeout` | RDS 보안 그룹에 수집 EC2 보안 그룹 미추가 |

---

## 5. 확인하지 않은 것 (2026-09-24 작성 시점)

- **실제 수집 EC2·GitHub 러너에서 돌려 보지 않았다.** 로컬에서 확인한 것: CI 단계(더미 키로 import ·
  `--dry-run` · `bash -n`), `deploy.sh` 를 가짜 python/sudo/systemctl 로 두 시나리오(정상 · 기동 후 반복
  재시작 → 이전 커밋 복구), `raw_cleanup` 을 가짜 파일로(기존 파일 유지 · 회차 파일 삭제 · 다른 도메인 파일 유지 · `KEEP_RAW=1`).
- 수집 전 과정(API → RDS)을 서버에서 한 회차 돌려 본 적이 없다. 첫 배포 뒤 4절 로그로 확인할 것.
- 인스턴스 사양(메모리)이 pandas 전처리에 충분한지 모른다. `port_call`·`portmis` 가 가장 무겁다.
