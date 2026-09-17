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

# --skip-db 로 켜지면 DB 적재 단계를 건너뛴다.
# backend(Alembic) 미구축 상태에서는 대부분 도메인의 테이블이 없어 DB 적재가
# 실패하므로, 수집·전처리(staging CSV)만 돌려 검증할 때 사용한다.
SKIP_DB = False


def _load(fn) -> None:
    """DB 적재 단계 실행 (SKIP_DB 면 건너뜀)."""
    if SKIP_DB:
        print("    (--skip-db: DB 적재 건너뜀 — staging CSV까지만 생성)")
        return
    fn()


def run_tide() -> None:
    from data_pipeline.collectors.tide_collector import collect_tide_raw
    from data_pipeline.loaders.tide_pg_loader import load as load_tide
    from data_pipeline.preprocessors.tide_preprocessor import preprocess_tide

    print("=== [tide] 1/3 수집 ===")
    collect_tide_raw(days_back=0)
    print("=== [tide] 2/3 전처리 ===")
    preprocess_tide()
    print("=== [tide] 3/3 DB 적재 ===")
    _load(load_tide)


def run_wave() -> None:
    from data_pipeline.collectors.wave_collector import collect_wave_raw
    from data_pipeline.loaders.wave_pg_loader import load as load_wave
    from data_pipeline.preprocessors.wave_preprocessor import preprocess_wave

    print("=== [wave] 1/3 수집 ===")
    collect_wave_raw()
    print("=== [wave] 2/3 전처리 ===")
    preprocess_wave()
    print("=== [wave] 3/3 DB 적재 ===")
    _load(load_wave)


def run_weather() -> None:
    from data_pipeline.collectors.weather_collector import collect_weather_raw
    from data_pipeline.loaders.weather_pg_loader import load as load_weather
    from data_pipeline.preprocessors.weather_preprocessor import preprocess_weather

    print("=== [weather] 1/3 수집 ===")
    collect_weather_raw()
    print("=== [weather] 2/3 전처리 ===")
    preprocess_weather()
    print("=== [weather] 3/3 DB 적재 ===")
    _load(load_weather)


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
    _load(load_ais)


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
    _load(lambda: load_all({"upa_vessel_position_stg.csv": UPA_TABLE_MAP["upa_vessel_position_stg.csv"]}))


def run_port_call() -> None:
    """입출항: UPA 운항정보(getVtsBaseVslNvgtInfo) 수집 → 전처리 → DB 적재.

    이 API는 callsgn(호출부호)이 필수라 "오늘 입항 전체"를 한 번에 못 받는다.
    대신 **항차(vyg)는 생략 가능**해서 `호출부호 + 입항연도`만 주면 그 선박의
    올해 입출항 이력이 통째로 온다(2026-07-26 실측: 1척 조회에 183건).
    호출부호는 파라미터 없이 조회되는 선박위치 API에서 얻는다.

        선박위치(전체 조회) → 호출부호 N개 → 운항정보 N회 호출 → upa_port_call UPSERT

    호출량은 항내 선박 수(약 550척) = 일일 허용 10만 회의 0.5% 수준이라 여유롭다.
    수집·전처리 함수(collect_vessel_nvgt / preprocess_vessel_nvgt)는 이미 upa
    모듈에 있고, 여기서는 일일 파이프라인에 연결만 한다.
    """
    import json as _json

    from data_pipeline.common_pg_loader import load_all
    from data_pipeline.upa.upa_collector import (
        UpaClient,
        _extract_items,
        derive_nvgt_targets_from_position,
    )
    from data_pipeline.upa.upa_loader import TABLE_MAP as UPA_TABLE_MAP
    from data_pipeline.upa.upa_preprocess import run_pipeline as run_upa_preprocess

    year = datetime.datetime.now().strftime("%Y")
    client = UpaClient()

    print("=== [port_call] 1/4 호출부호 확보 (선박위치) ===")
    pos_path = client.collect_vessel_position()
    with open(pos_path, encoding="utf-8") as f:
        targets = derive_nvgt_targets_from_position(_extract_items(_json.load(f)), year)
    # vyg 는 비워 보낸다 — 특정 항차가 아니라 그 선박의 연간 이력 전체를 받기 위함
    for t in targets:
        t["vyg"] = None
    if not targets:
        print("[port_call] 항내 선박이 없어 건너뜁니다.")
        return

    print(f"=== [port_call] 2/4 운항정보 수집 (대상 {len(targets)}척) ===")
    raw_path = client.collect_vessel_nvgt(targets)
    print("=== [port_call] 3/4 전처리 ===")
    run_upa_preprocess("vessel_nvgt", [raw_path])
    print("=== [port_call] 4/4 DB 적재 ===")
    load_all({"upa_port_call_stg.csv": UPA_TABLE_MAP["upa_port_call_stg.csv"]})


def run_portmis(start_date: str | None = None, end_date: str | None = None) -> None:
    from data_pipeline.collectors.portmis_collector import (
        collect_portmis,
        resolve_incremental_start_date,
        resolve_lookahead_end_date,
    )
    from data_pipeline.loaders.portmis_pg_loader import load as load_portmis
    from data_pipeline.preprocessors.portmis_preprocessor import preprocess_portmis

    # --start를 안 주면 "오늘부터"가 아니라 "마지막 수집 이후로 이어붙이기"가 기본이다
    # (2026-08-19) — 매일 이 명령을 그대로 재실행해도 그날그날 새로 입항한 건만
    # 자동으로 누적되도록 하기 위함(portmis_collector.py 모듈 docstring 참고).
    # 종료일 기본값은 오늘이 아니라 오늘+3일 — 입항 예정 신고까지 받는다(2026-09-17).
    start_date = start_date or resolve_incremental_start_date()
    end_date = end_date or resolve_lookahead_end_date()

    print(f"=== [portmis] 1/3 수집 ({start_date} ~ {end_date}) ===")
    collect_portmis(start_date=start_date, end_date=end_date)
    print("=== [portmis] 2/3 전처리 ===")
    preprocess_portmis()
    print("=== [portmis] 3/3 DB 적재 ===")
    _load(load_portmis)


def _refresh_materialized_views() -> None:
    """mart 스키마의 구체화 뷰를 최신 적재분 기준으로 갱신한다.

    일반 뷰와 달리 MATERIALIZED VIEW 는 조회 시점에 다시 계산되지 않는다 —
    만들어진 순간의 결과를 그대로 들고 있다가, REFRESH 를 해줘야 갱신된다.

    ★ 갱신하지 않으면 무슨 일이 생기나
      mart.facility_alias 는 upa_port_call / upa_cargo_manifest 의 시설명을
      DISTINCT 로 훑어 만든 사전이다. 새 입항 기록이 들어오면서 처음 보는
      시설명이 등장해도, 갱신 전까지 사전에는 없다. 그 시설은 선석 매칭에서
      조용히 빠지고(backend dashboard.py 가 이 사전을 경유해 조인한다),
      화면에는 "점유 중인데 여유"로 뜬다 — facility_alias 가 없앴어야 할
      바로 그 오표시가 형태만 바꿔 되살아난다.

      뷰 정의(mart_views.sql)를 다시 실행할 때는 DROP+CREATE 라 자동으로
      최신이 되지만, 그건 사람이 수동으로 돌리는 작업이다. 매 수집마다
      SQL 파일을 다시 실행하게 만들 수는 없으므로 여기서 갱신한다.

    없는 경우(뷰 미적용 DB)는 조용히 넘어간다 — 파이프라인 적재 자체는
    이미 끝난 뒤이고, 뷰가 없다는 건 mart_views.sql 을 아직 안 돌렸다는
    뜻이지 적재 실패가 아니다.
    """
    from sqlalchemy import text

    from data_pipeline.common_pg_loader import get_engine

    engine = get_engine()
    with engine.begin() as conn:
        exists = conn.execute(text(
            "SELECT 1 FROM pg_matviews WHERE schemaname = 'mart' AND matviewname = 'facility_alias'"
        )).scalar()
        if not exists:
            print("  - mart.facility_alias 없음 — 갱신 건너뜀 (mart_views.sql 미적용 DB)")
            return
        conn.execute(text("REFRESH MATERIALIZED VIEW mart.facility_alias"))
        n = conn.execute(text("SELECT count(*) FROM mart.facility_alias")).scalar()
        unmapped = conn.execute(text(
            "SELECT count(*) FROM mart.facility_alias WHERE facility_type = 'UNMAPPED'"
        )).scalar()
    print(f"  - mart.facility_alias 갱신 완료 ({n}종, 미매핑 {unmapped}종)")


def run_vessel_spec() -> None:
    """해양수산부 선박제원정보 수집 — portmis_vessel_stg.csv의 distinct callsgn을
    순회 조회한다(08_스케줄링_전면재설계_자동배정_설계문서.md §5.2.1-A). portmis
    도메인이 먼저 돌아 staging이 최신이어야 의미가 있으므로 DOMAINS 순서상 portmis
    바로 뒤에 둔다."""
    from data_pipeline.collectors.vessel_spec_collector import collect_and_save
    from data_pipeline.loaders.vessel_spec_pg_loader import load as load_vessel_spec
    from data_pipeline.preprocessors.vessel_spec_preprocessor import preprocess_vessel_spec

    print("=== [vessel_spec] 1/3 수집 ===")
    collect_and_save()
    print("=== [vessel_spec] 2/3 전처리 ===")
    preprocess_vessel_spec()
    print("=== [vessel_spec] 3/3 DB 적재 ===")
    _load(load_vessel_spec)


def run_mart() -> None:
    """staging 산출물을 결합해 통합 마트를 만들고 DB에 적재한다.

    수집 단계가 없는 파생 도메인 — ais/portmis(필수) 및 tide/wave/weather/UPA/MSDS
    (있으면 병합) staging CSV가 먼저 준비되어 있어야 한다. `all` 실행 시
    다른 도메인들이 끝난 뒤 마지막에 실행되도록 DOMAINS 순서를 유지할 것.
    """
    # create_mart.py는 저장소 루트에 있다 (python -m 실행 시 루트가 sys.path에 포함됨)
    from create_mart import build_master_mart
    from data_pipeline.loaders.mart_pg_loader import load as load_mart

    print("=== [mart] 1/3 통합 마트 생성 ===")
    build_master_mart()
    print("=== [mart] 2/3 DB 적재 ===")
    _load(load_mart)
    print("=== [mart] 3/3 구체화 뷰 갱신 ===")
    if SKIP_DB:
        print("  - --skip-db: 갱신 건너뜀")
    else:
        try:
            _refresh_materialized_views()
        except Exception as e:  # noqa: BLE001
            # 적재는 이미 끝났다. 갱신 실패로 전체를 실패로 만들지 않되,
            # 조용히 넘기지도 않는다 — 사전이 낡으면 선석 매칭이 틀어진다.
            print(f"  [WARN] 구체화 뷰 갱신 실패: {e}")


DOMAINS = {
    "tide": run_tide,
    "wave": run_wave,
    "weather": run_weather,
    "weather_forecast": run_weather_forecast,
    "vessel": run_vessel,  # 선박위치 (UPA getVslPstnInfo) — 구 ais 도메인 대체
    "port_call": run_port_call,  # 입출항 이력 (UPA getVtsBaseVslNvgtInfo) — 선박위치 뒤에 실행
    "portmis": run_portmis,
    "vessel_spec": run_vessel_spec,  # portmis 뒤에 실행 — 그 staging의 callsgn을 씀
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
    parser.add_argument(
        "--skip-db",
        action="store_true",
        help="DB 적재 단계를 건너뛰고 수집·전처리(staging CSV)까지만 실행 "
             "(backend Alembic 미구축 시 검증용)",
    )
    args = parser.parse_args()

    global SKIP_DB
    SKIP_DB = args.skip_db
    if SKIP_DB:
        print("[안내] --skip-db: DB 적재를 건너뜁니다. staging CSV 생성까지만 수행.\n")

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
