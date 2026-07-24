@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo ============================================
echo   실 API 6종 자동 검증 (더블클릭 실행용)
echo ============================================

if not exist ".env" (
  echo [오류] 이 파일을 data-pipeline 폴더 안에 넣고 실행하세요. (.env 파일이 안 보임)
  pause
  exit /b 1
)

where python >nul 2>nul
if errorlevel 1 (
  echo [오류] Python이 없습니다. https://python.org 에서 설치 후 다시 더블클릭하세요.
  echo        ※ 설치 시 "Add python.exe to PATH" 체크 필수
  pause
  exit /b 1
)

echo [1/3] 최신 코드 받는 중...
git pull origin dev

echo [2/3] 필요 라이브러리 설치 중... (처음 한 번만 오래 걸림)
python -m pip install --quiet requests python-dotenv pandas sqlalchemy psycopg2-binary

echo [3/3] 실 API 6종 검증 중... (20초 정도)
python -m data_pipeline.checks.verify_live_api > 검증결과.txt 2>&1
type 검증결과.txt

echo.
echo ============================================
echo  위 결과가 "검증결과.txt" 파일로도 저장됐습니다.
echo  그 파일 내용을 채팅에 붙여넣어 주세요.
echo ============================================
pause
