# -*- coding: utf-8 -*-
"""MSDS 적재 무결성 점검.

수집·적재 직후 1회 돌려서 "몇 종이 들어왔고 무엇이 비었는지"를 기록으로 남긴다.
나중에 "151종인데 왜 140종밖에 없지"를 헤매지 않기 위한 것이다.

점검 항목
--------
  1. 적재 건수 vs 수집 대상 수      — 누락분의 CAS 를 찍는다
  2. chem_id 중복                   — MSDS 1건이 여러 행으로 들어오면 판정이 부풀려진다
  3. UN번호 결측률                  — UN 없는 물질은 화물 풀에서 빠진다(IMDG 비대상)
  4. UN번호 중복                    — ★ 화물 조인이 UN 기준이라 1건이 N건으로 부풀려진다
                                       (예: UN1307 을 크실렌과 p-크실렌이 정당하게 공유)
  5. 인화점 파싱률                  — flash_point_celsius 가 숫자로 들어왔는가

실행: py -m data_pipeline.checks.msds_integrity
"""
import os
import sys

import psycopg2
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from data_pipeline.collectors.msds_api_collector import load_target_chemicals  # noqa: E402

load_dotenv()


def _dsn() -> str:
    return (
        f"host={os.getenv('POSTGRES_HOST', 'localhost')} "
        f"port={os.getenv('POSTGRES_PORT', '5433')} "
        f"dbname={os.getenv('POSTGRES_DB', 'smartport')} "
        f"user={os.getenv('POSTGRES_USER', 'smartport')} "
        f"password={os.getenv('POSTGRES_PASSWORD', 'smartportmas')}"
    )


def main() -> int:
    targets = {cas: name for name, cas in load_target_chemicals()}
    conn = psycopg2.connect(_dsn())
    cur = conn.cursor()
    fail = 0

    cur.execute("SELECT count(*), count(DISTINCT chem_id), count(DISTINCT cas_no) FROM msds_chemical")
    rows, ids, cas_n = cur.fetchone()
    print(f"[1] 적재 {rows}행 / chem_id {ids}개 / cas_no {cas_n}개  (수집 대상 {len(targets)}종)")

    cur.execute("SELECT cas_no FROM msds_chemical WHERE cas_no IS NOT NULL")
    got = {r[0] for r in cur.fetchall()}
    missing = [c for c in targets if c not in got]
    if missing:
        fail += 1
        print(f"    !! 누락 {len(missing)}종")
        for c in missing:
            print(f"       {c:<14} {targets[c]}")
    else:
        print("    OK - 누락 없음")

    print(f"[2] chem_id 중복: {rows - ids}건", end="  ")
    if rows != ids:
        fail += 1
        cur.execute(
            "SELECT chem_id, count(*) FROM msds_chemical GROUP BY 1 HAVING count(*) > 1"
        )
        print("!!")
        for cid, n in cur.fetchall():
            print(f"       chem_id={cid} {n}행")
    else:
        print("OK")

    cur.execute("SELECT count(*) FILTER (WHERE un_no IS NULL OR un_no = ''), count(*) FROM msds_chemical")
    nul, tot = cur.fetchone()
    print(f"[3] UN번호 결측: {nul}/{tot} ({100 * nul / tot if tot else 0:.1f}%)")
    print("    (UN 없는 물질은 IMDG 대상이 아니므로 화물 풀에서 제외된다 — 정상)")

    cur.execute(
        """SELECT un_no, count(*), string_agg(coalesce(name_ko, cas_no), ', ')
           FROM msds_chemical WHERE un_no IS NOT NULL AND un_no <> ''
           GROUP BY 1 HAVING count(*) > 1 ORDER BY 2 DESC"""
    )
    dups = cur.fetchall()
    print(f"[4] UN번호 중복: {len(dups)}종")
    if dups:
        print("    ★ 화물 조인이 UN 기준이라 해당 화물 1건이 MSDS N건으로 부풀려진다.")
        for un, n, names in dups:
            print(f"       UN{un} x{n} : {names}")

    cur.execute(
        """SELECT count(*) FILTER (WHERE flash_point_celsius IS NOT NULL),
                  count(*) FILTER (WHERE flash_point_text IS NOT NULL AND flash_point_text <> ''),
                  count(*) FROM msds_chemical"""
    )
    num, txt, tot = cur.fetchone()
    print(f"[5] 인화점: 숫자 {num} / 원문 {txt} / 전체 {tot}")

    cur.close()
    conn.close()
    print()
    print("=== 점검 " + ("실패 (위 !! 항목 확인)" if fail else "통과") + " ===")
    return 1 if fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
