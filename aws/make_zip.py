# -*- coding: utf-8 -*-
"""EC2 업로드용 data-pipeline.zip 생성 (루트의 aws_zip_만들기.bat 이 호출).

PowerShell Compress-Archive 를 쓰지 않는 이유(2026-08-05 실증):
경로 구분자를 백슬래시(\\)로 넣어서 리눅스 unzip 이 경고(종료코드 1)를 내고,
EC2 부팅 스크립트(set -e)가 그 경고만으로 중단됐다. 한글 파일명도 깨졌다.
Python zipfile 은 항상 슬래시(/)와 UTF-8 파일명 플래그를 쓰므로 안전하다.

제외: .env(비밀키) · data/(수집물) · .git · __pycache__ · zip 자신
"""
import os
import zipfile

BASE = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SRC = os.path.join(BASE, "data-pipeline")
DEST = os.path.join(BASE, "data-pipeline.zip")

EXCLUDE_DIRS = {".git", "data", "__pycache__", ".pytest_cache"}
EXCLUDE_FILES = {".env"}


def main() -> None:
    n = 0
    with zipfile.ZipFile(DEST, "w", zipfile.ZIP_DEFLATED) as z:
        for root, dirs, files in os.walk(SRC):
            dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]
            for f in files:
                if f in EXCLUDE_FILES or f.endswith(".pyc"):
                    continue
                full = os.path.join(root, f)
                rel = os.path.relpath(full, os.path.dirname(SRC))
                # 핵심: 아크명(arcname)의 구분자를 슬래시로 강제
                z.write(full, rel.replace(os.sep, "/"))
                n += 1
    size = os.path.getsize(DEST)
    print(f"생성 완료: {DEST}")
    print(f"파일 {n}개 / {size:,} bytes")
    # 자가 검증 — 백슬래시·.env 가 들어갔으면 여기서 바로 실패시킨다
    with zipfile.ZipFile(DEST) as z:
        names = z.namelist()
    assert not any("\\" in x for x in names), "백슬래시 경로 발견 — 업로드 금지"
    assert not any(x.endswith(".env") for x in names), ".env 포함 — 업로드 금지"
    print("검증 통과: 구분자 슬래시 / .env 미포함")


if __name__ == "__main__":
    main()
