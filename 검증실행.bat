@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo ============================================
echo   데이터 파이프라인 자동 검증 (더블클릭 실행용)
echo ============================================

if not exist ".env" (
  echo [오류] 이 파일을 data-pipeline 폴더 안에 넣고 실행하세요. (.env 파일이 안 보임)
  pause
  exit /b 1
)

REM Python 실행기 찾기 — Windows 는 py 런처만 있고 python 이 없는 경우가 흔하다.
REM (예전 판은 python 만 확인해서, py 만 있는 PC 에서 조용히 멈췄다)
set PY=
where py >nul 2>nul && set PY=py
if not defined PY where python >nul 2>nul && set PY=python
if not defined PY (
  echo [오류] Python 이 없습니다. https://python.org 에서 설치 후 다시 더블클릭하세요.
  echo        ※ 설치 시 "Add python.exe to PATH" 체크 필수
  pause
  exit /b 1
)
echo [실행기] %PY%

echo.
echo [1/5] 최신 코드 받는 중...
git pull origin dev

echo.
echo [2/5] 필요 라이브러리 설치 중... (처음 한 번만 오래 걸림)
%PY% -m pip install --quiet requests python-dotenv pandas sqlalchemy psycopg2-binary

echo.
echo [3/5] 실 API 6종 검증 중... (20초 정도)
%PY% -m data_pipeline.checks.verify_live_api > 검증결과.txt 2>&1

echo.
echo [4/5] callsgn 결측 구조 진단 중...
%PY% -m data_pipeline.checks.diag_callsgn_gap >> 검증결과.txt 2>&1

echo.
echo [5/5] DGL(위험물목록) 참조표 대조 중...
%PY% -m data_pipeline.checks.check_dgl_consistency >> 검증결과.txt 2>&1

type 검증결과.txt

echo.
echo ============================================
echo  위 결과가 "검증결과.txt" 파일로도 저장됐습니다.
echo  그 파일 내용을 채팅에 붙여넣어 주세요.
echo ============================================
pause
