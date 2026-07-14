# -*- coding: utf-8 -*-
"""
수집 → 전처리 → DB 적재를 한 번에 묶어 실행하는 오케스트레이터.

지금까지는 각 단계를 사람이 순서대로 따로 실행해야 했다
(collector → preprocessor → pg_loader, 도메인당 명령어 3개).
이 스크립트는 그 3단계를 한 번의 명령으로 이어서 실행한다.

실행:
    python -m data_pipeline.run_pipeline tide
    python -m data_pipeline.run_pipeline wave
    python -m data_pipeline.run_pipeline weather
    python -m data_pipeline.run_pipeline vessel
    python -m data_pipeline.run_pipeline portmis [--start YYYYMMDD --end YYYYMMDD]
    python -m data_pipeline.run_pipeline mart
    python -m data_pipeline.run_pipeline all
    python -m data_pipeline.run_pipeline ais [--minutes 5]   # 레거시 (all 미포함)

선박 위치 소스 (2026-07 교체):
    vessel = UPA 항내 선박위치 getVslPstnInfo (기본, all 포함)
    ais    = aisstream.io 웹소켓 (레거시 — AIS static ship_type 미수신으로
             액체화물선 식별 불가 + 관측 다수가 울산 bbox 밖 문제로 기본 흐름에서
             제외. 비교 검증 등 필요 시 단독 실행만 가능)

MSDS는 여기 포함하지 않는다 — 배치 수집이 물질 30여 종 × 16섹션으로 몇 분씩 걸리고,
DB 적재 대상 테이블도(chem_id upsert) 조회 lazy-fetch 캐시(backend)와 공유하는 등
성격이 달라 필요하면 별도로 chain한다.
"""

import argparse
import asyncio
import datetime


def run_tide() -> None:
    from data_pipeline.collectors.tide_collector import collect_tide_raw
    from data_pipeline.loaders.tide_pg_loader import load as load_tide
    from data_pipeline.preprocessors.tide_preprocessor import preprocess_tide

    print("=== [tide] 1/3 수집 ===")
    collect_tide_raw(days_back=0)
    print("=== [tide] 2/3 전처리 ===")
    preprocess_tide()
    print("=== [tide] 3/3 DB 적재 ===")
    load_tide()


def run_wave() -> None:
    from data_pipeline.collectors.wave_collector import collect_wave_raw
    from data_pipeline.loaders.wave_pg_loader import load as load_wave
    from data_pipeline.preprocessors.wave_preprocessor import preprocess_wave

    print("=== [wave] 1/3 수집 ===")
    collect_wave_raw()
    print("=== [wave] 2/3 전처리 ===")
    preprocess_wave()
    print("=== [wave] 3/3 DB 적재 ===")
    load_wave()


def run_weather() -> None:
    from data_pipeline.collectors.weather_collector import collect_weather_raw
    from data_pipeline.loaders.weather_pg_loader import load as load_weather
    from data_pipeline.preprocessors.weather_preprocessor import preprocess_weather

    print("=== [weather] 1/3 수집 ===")
    collect_weather_raw()
    print("=== [weather] 2/3 전처리 ===")
    preprocess_weather()
    print("=== [weather] 3/3 DB 적재 ===")
    load_weather()


def run_ais(minutes: int = 5) -> None:
    from data_pipeline.collectors.ais_collector import collect_ais
    from data_pipeline.loaders.ais_pg_loader import load as load_ais
    from data_pipeline.preprocessors.ais_preprocessor import (
        preprocess_ais_position,
        preprocess_ais_static,
    )

    print(f"=== [ais] 1/3 수집 ({minutes}분) ===")
    asyncio.run(collect_ais(duration_minutes=minutes))
    print("=== [ais] 2/3 전처리 ===")
    # static이 position 전처리 결과를 참조하므로 순서 고정 (ais_preprocessor.main()과 동일)
    preprocess_ais_static()
    preprocess_ais_position()
    print("=== [ais] 3/3 DB 적재 ===")
    load_ais()


def run_vessel() -> None:
    """선박위치: UPA 항내 선박위치정보(getVslPstnInfo) 수집 → 전처리 → DB 적재.

    aisstream 웹소켓 수집(run_ais)을 대체한다. UPA API는 callsgn/mmsi/imo를
    네이티브로 제공해 PORT-MIS 선종(액체화물선 여부)과 확정 조인이 가능하고,
    울산 항내 스코프라 bbox 오탐도 구조적으로 없다.
    수집·전처리는 upa 모듈(담당: 함현우)을 재사용하고, 여기서는 위치 API 1종만 chain한다.
    """
    from data_pipeline.common_pg_loader import load_all
    from data_pipeline.upa.upa_collector import UpaClient
    from data_pipeline.upa.upa_loader import TABLE_MAP as UPA_TABLE_MAP
    from data_pipeline.upa.upa_preprocess import run_pipeline as run_upa_preprocess

    print("=== [vessel] 1/3 수집 (UPA 항내 선박위치) ===")
    raw_path = UpaClient().collect_vessel_position()
    print("=== [vessel] 2/3 전처리 ===")
    run_upa_preprocess("vessel_position", [raw_path])
    print("=== [vessel] 3/3 DB 적재 ===")
    # 유니크 키는 upa_loader TABLE_MAP 정의를 단일 정본으로 재사용
    load_all({"upa_vessel_position_stg.csv": UPA_TABLE_MAP["upa_vessel_position_stg.csv"]})


def run_portmis(start_date: str | None = None, end_date: str | None = None) -> None:
    from data_pipeline.collectors.portmis_collector import collect_portmis
    from data_pipeline.loaders.portmis_pg_loader import load as load_portmis
    from data_pipeline.preprocessors.portmis_preprocessor import preprocess_portmis

    today = datetime.datetime.now().strftime("%Y%m%d")
    start_date = start_date or today
    end_date = end_date or today

    print(f"=== [portmis] 1/3 수집 ({start_date} ~ {end_date}) ===")
    collect_portmis(start_date=start_date, end_date=end_date)
    print("=== [portmis] 2/3 전처리 ===")
    preprocess_portmis()
    print("=== [portmis] 3/3 DB 적재 ===")
    load_portmis()


def run_mart() -> None:
    """staging 산출물을 결합해 통합 마트를 만들고 DB에 적재한다.

    수집 단계가 없는 파생 도메인 — ais/portmis(필수) 및 tide/wave/weather/UPA/MSDS
    (있으면 병합) staging CSV가 먼저 준비되어 있어야 한다. `all` 실행 시
    다른 도메인들이 끝난 뒤 마지막에 실행되도록 DOMAINS 순서를 유지할 것.
    """
    # create_mart.py는 저장소 루트에 있다 (python -m 실행 시 루트가 sys.path에 포함됨)
    from create_mart import build_master_mart
    from data_pipeline.loaders.mart_pg_loader import load as load_mart

    print("=== [mart] 1/2 통합 마트 생성 ===")
    build_master_mart()
    print("=== [mart] 2/2 DB 적재 ===")
    load_mart()


DOMAINS = {
    "tide": run_tide,
    "wave": run_wave,
    "weather": run_weather,
    "vessel": run_vessel,  # 선박위치 (UPA getVslPstnInfo) — 구 ais 도메인 대체
    "portmis": run_portmis,
    "mart": run_mart,  # 파생 도메인 — 반드시 마지막 (staging 산출물 필요)
}

# 레거시 도메인: 단독 실행만 가능, `all` 에는 포함되지 않는다.
LEGACY_DOMAINS = {
    "ais": run_ais,  # aisstream.io 웹소켓 (vessel 로 대체됨 — 비교 검증용)
}


def main() -> None:
    parser = argparse.ArgumentParser(description="수집 → 전처리 → DB 적재 파이프라인 실행")
    parser.add_argument(
        "domain",
        choices=[*DOMAINS.keys(), *LEGACY_DOMAINS.keys(), "all"],
        help="실행할 도메인 (all 은 레거시 도메인 제외)",
    )
    parser.add_argument("--minutes", type=int, default=5, help="[ais 전용] 수집 시간(분)")
    parser.add_argument("--start", type=str, default=None, help="[portmis 전용] 조회 시작일 YYYYMMDD")
    parser.add_argument("--end", type=str, default=None, help="[portmis 전용] 조회 종료일 YYYYMMDD")
    args = parser.parse_args()

    targets = list(DOMAINS.keys()) if args.domain == "all" else [args.domain]

    for name in targets:
        try:
            if name == "ais":
                run_ais(minutes=args.minutes)
            elif name == "portmis":
                run_portmis(start_date=args.start, end_date=args.end)
            elif name in LEGACY_DOMAINS:
                LEGACY_DOMAINS[name]()
            else:
                DOMAINS[name]()
        except Exception as e:  # noqa: BLE001
            print(f"[ERROR] {name} 파이프라인 실패: {e}")
            if args.domain != "all":
                raise

    print("\n=== 전체 파이프라인 완료 ===")


if __name__ == "__main__":
    main()
