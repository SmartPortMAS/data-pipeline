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
    python -m data_pipeline.run_pipeline weather_forecast
    python -m data_pipeline.run_pipeline ais [--minutes 5]
    python -m data_pipeline.run_pipeline portmis [--start YYYYMMDD --end YYYYMMDD]
    python -m data_pipeline.run_pipeline all

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


def run_weather_forecast() -> None:
    from data_pipeline.collectors.weather_forecast_collector import collect_weather_forecast_raw
    from data_pipeline.loaders.weather_forecast_pg_loader import load as load_weather_forecast
    from data_pipeline.preprocessors.weather_forecast_preprocessor import preprocess_weather_forecast

    print("=== [weather_forecast] 1/3 수집 ===")
    collect_weather_forecast_raw()
    print("=== [weather_forecast] 2/3 전처리 ===")
    preprocess_weather_forecast()
    print("=== [weather_forecast] 3/3 DB 적재 ===")
    load_weather_forecast()


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


DOMAINS = {
    "tide": run_tide,
    "wave": run_wave,
    "weather": run_weather,
    "weather_forecast": run_weather_forecast,
    "ais": run_ais,
    "portmis": run_portmis,
}


def main() -> None:
    parser = argparse.ArgumentParser(description="수집 → 전처리 → DB 적재 파이프라인 실행")
    parser.add_argument("domain", choices=[*DOMAINS.keys(), "all"], help="실행할 도메인")
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
            else:
                DOMAINS[name]()
        except Exception as e:  # noqa: BLE001
            print(f"[ERROR] {name} 파이프라인 실패: {e}")
            if args.domain != "all":
                raise

    print("\n=== 전체 파이프라인 완료 ===")


if __name__ == "__main__":
    main()
