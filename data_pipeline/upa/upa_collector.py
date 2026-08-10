# -*- coding: utf-8 -*-
"""
울산항만공사(UPA) Open API 수집기 (담당: 함현우)

공공데이터포털에서 발급받은 serviceKey 로 6종 API 를 호출하여
원본 응답(JSON)을 data/raw/upa/ 에 그대로 저장한다. (원본 보존 원칙)
저장된 raw 는 upa_preprocess 모듈이 전처리한다.

[수집 난이도 2단계]
  (A) 전체 조회형 : 필수 파라미터가 없어 한 번에 목록 수집 가능
      - 선박위치(getVslPstnInfo)
      - 하역정보(getUnloadRcdInfo)
      - 부두(getGisBaseHrbrFcltDtlInfo)
      - 정박지(getGisBaseAnchrgDtlInfo)
  (B) 키 지정형  : callsgn/연도/항차(+업체코드)가 필수 → 대상 선박을 알아야 조회
      - 운항정보(getVtsBaseVslNvgtInfo)  : callsgn, ptentVtsYr 필수
      - 통합화물(getIntgCagInfo)         : bzentyCd, callsgn, prtCd, ptentYr, vyg 필수
      - 내항화물(getInprtCagDclrInfo)    : 〃
  → (B)는 보통 (A)의 선박위치에서 얻은 callsgn 목록으로 반복 호출한다.

사용 전 준비:
  pip install requests
  환경변수 또는 .env 에 UPA_SERVICE_KEY 설정 (공공데이터포털 '일반 인증키(Decoding)')
"""
import os
import json
import time
import datetime as dt

import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "http://apis.data.go.kr/B551938"
RAW_DIR = "data/raw/upa"

# API 키별 (엔드포인트 경로, raw 파일 접두어)
ENDPOINTS = {
    "vessel_position": ("VslPstnInfoService/getVslPstnInfo", "upa_vessel_position"),
    "vessel_nvgt":     ("VslNvgtInfoService/getVtsBaseVslNvgtInfo", "upa_vessel_nvgt"),
    "intg_cargo":      ("IntgCagInfoService/getIntgCagInfo", "upa_intg_cargo"),
    "inprt_cargo":     ("IntgCagInfoService/getInprtCagDclrInfo", "upa_inprt_cargo"),
    "unload_record":   ("UnloadRcdInfoService/getUnloadRcdInfo", "upa_unload_record"),
    "berth_facility":  ("GisBaseHrbrFcltDtlInfoService/getGisBaseHrbrFcltDtlInfo", "upa_berth_facility"),
    "anchorage":       ("GisBaseHrbrFcltDtlInfoService/getGisBaseAnchrgDtlInfo", "upa_anchorage"),
}


class UpaClient:
    """울산항만공사 Open API 클라이언트."""

    # 호출 간 최소 간격(초). data.go.kr 은 초당 호출 한도가 있는데, 운항정보는
    # 선박 1척당 1회 호출이라 334척이면 334회를 연속으로 던지게 된다. 간격 없이
    # 쏘면 몇 회 만에 한도에 걸려 이후 전량 429 가 된다(실측: 334척 100% 실패).
    # 아래 간격만큼 스스로 늦춰서 한도에 걸리지 않게 한다.
    DEFAULT_MIN_INTERVAL = 0.4

    # 연속 실패가 이 횟수를 넘으면 남은 대상을 포기한다. 키가 이미 막힌 상태에서
    # 나머지를 계속 두드려도 전부 실패하며, 재시도 대기(2+4+8초)까지 곱해져
    # 334척이면 1시간 넘게 헛돈다. 빨리 멈추고 원인을 알리는 편이 낫다.
    CONSECUTIVE_FAILURE_LIMIT = 8

    def __init__(self, service_key: str = None, timeout: int = 20, max_retry: int = 3,
                 min_interval: float = None):
        self.service_key = service_key or os.getenv("UPA_SERVICE_KEY")
        if not self.service_key:
            raise ValueError(
                "serviceKey 가 없습니다. 환경변수 UPA_SERVICE_KEY 를 설정하거나 "
                "UpaClient(service_key='...') 로 직접 넘기세요."
            )
        self.timeout = timeout
        self.max_retry = max_retry
        self.min_interval = (
            self.DEFAULT_MIN_INTERVAL if min_interval is None else min_interval
        )
        self._last_call_at = 0.0
        self.session = requests.Session()

    # ----- 저수준 호출 -------------------------------------------------------
    def _throttle(self) -> None:
        """직전 호출로부터 min_interval 이 지날 때까지 기다린다."""
        if self.min_interval <= 0:
            return
        elapsed = time.monotonic() - self._last_call_at
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)

    @staticmethod
    def _retry_wait(exc, attempt: int) -> float:
        """재시도 대기 시간(초).

        429(Too Many Requests)는 일반 오류보다 길게 쉰다 — 즉시 재시도하면
        한도를 더 밀어 올려 상황이 나빠진다. 서버가 Retry-After 를 주면 그 값을
        우선한다(표준 헤더이므로 있으면 그게 정답이다).
        """
        resp = getattr(exc, "response", None)
        if resp is not None and resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            if retry_after:
                try:
                    return min(float(retry_after), 60.0)
                except ValueError:
                    pass
            return min(5 * (2 ** attempt), 60.0)   # 10, 20, 40s
        return 2 ** attempt                        # 2, 4, 8s

    def _get(self, endpoint: str, params: dict) -> dict:
        """단일 페이지 호출. JSON dict 반환. 실패 시 예외."""
        url = f"{BASE_URL}/{endpoint}"
        q = {
            "serviceKey": self.service_key,
            "resultType": "json",
            **params,
        }
        last_err = None
        for attempt in range(1, self.max_retry + 1):
            try:
                self._throttle()
                resp = self.session.get(url, params=q, timeout=self.timeout)
                self._last_call_at = time.monotonic()
                resp.raise_for_status()
                # data.go.kr 는 오류 시 XML/HTML 을 주기도 한다 → JSON 파싱 시도
                try:
                    return resp.json()
                except json.JSONDecodeError:
                    raise RuntimeError(f"JSON 아님(인증키/파라미터 확인): {resp.text[:200]}")
            except Exception as e:  # noqa: BLE001
                last_err = e
                if attempt == self.max_retry:
                    break
                wait = self._retry_wait(e, attempt)
                print(f"[RETRY] {endpoint} 시도 {attempt} 실패: {e} -> {wait:.0f}s 후 재시도")
                time.sleep(wait)
        raise RuntimeError(f"{endpoint} 호출 실패: {last_err}")

    def fetch_all(self, endpoint: str, params: dict = None, num_of_rows: int = 100,
                  max_pages: int = 100) -> list:
        """페이지를 끝까지 넘기며 item 을 모두 모은다."""
        params = dict(params or {})
        items = []
        page = 1
        while page <= max_pages:
            data = self._get(endpoint, {**params, "pageNo": page, "numOfRows": num_of_rows})
            page_items = _extract_items(data)
            if not page_items:
                break
            items.extend(page_items)
            total = _extract_total(data)
            if total is not None and len(items) >= total:
                break
            if len(page_items) < num_of_rows:
                break
            page += 1
        return items

    # ----- 저장 --------------------------------------------------------------
    def save_raw(self, prefix: str, items: list, with_date: bool = True) -> str:
        """
        raw 저장. 두 개를 만든다.
          1) 날짜 스냅샷  : upa_xxx_20260618_raw.json  (이력 보존)
          2) 최신 고정본  : upa_xxx_raw.json           (전처리가 읽는 파일)
        반환값은 전처리가 읽는 최신 고정본 경로.
        """
        os.makedirs(RAW_DIR, exist_ok=True)
        payload = {
            "response": {
                "header": {"resultCode": "00", "resultMsg": "NORMAL SERVICE."},
                "body": {"items": {"item": items}, "totalCount": len(items)},
            }
        }
        latest = os.path.join(RAW_DIR, f"{prefix}_raw.json")
        with open(latest, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        if with_date:
            today = dt.datetime.now().strftime("%Y%m%d")
            snap = os.path.join(RAW_DIR, f"{prefix}_{today}_raw.json")
            with open(snap, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        return latest

    # ----- (A) 전체 조회형 ---------------------------------------------------
    def collect_vessel_position(self) -> str:
        ep, prefix = ENDPOINTS["vessel_position"]
        items = self.fetch_all(ep)
        print(f"[COLLECT] 선박위치 {len(items)}건")
        return self.save_raw(prefix, items)

    def collect_unload_record(self, unload_prt_nm: str = None, vsl_nm: str = None) -> str:
        ep, prefix = ENDPOINTS["unload_record"]
        params = {}
        if unload_prt_nm:
            params["unloadPrtNm"] = unload_prt_nm
        if vsl_nm:
            params["vslNm"] = vsl_nm
        items = self.fetch_all(ep, params)
        print(f"[COLLECT] 하역정보 {len(items)}건")
        return self.save_raw(prefix, items)

    def collect_berth_facility(self, prt_nm: str = None) -> str:
        ep, prefix = ENDPOINTS["berth_facility"]
        params = {"prtNm": prt_nm} if prt_nm else {}
        items = self.fetch_all(ep, params)
        print(f"[COLLECT] 부두 {len(items)}건")
        return self.save_raw(prefix, items)

    def collect_anchorage(self, anchrg_nm: str = None) -> str:
        ep, prefix = ENDPOINTS["anchorage"]
        params = {"anchrgNm": anchrg_nm} if anchrg_nm else {}
        items = self.fetch_all(ep, params)
        print(f"[COLLECT] 정박지 {len(items)}건")
        return self.save_raw(prefix, items)

    # ----- (B) 키 지정형 -----------------------------------------------------
    def collect_vessel_nvgt(self, targets: list) -> str:
        """
        운항정보 수집. targets = [{"callsgn":..., "ptentVtsYr":..., "vyg":...(선택)}, ...]
        보통 collect_vessel_position 결과의 callsgn 목록으로 만든다.
        """
        ep, prefix = ENDPOINTS["vessel_nvgt"]
        all_items = []
        ok_count = 0
        fail_count = 0
        consecutive_fail = 0
        aborted = False

        for idx, t in enumerate(targets, 1):
            params = {"callsgn": t["callsgn"], "ptentVtsYr": t["ptentVtsYr"]}
            if t.get("vyg"):
                params["vyg"] = t["vyg"]
            try:
                all_items.extend(self.fetch_all(ep, params))
                ok_count += 1
                consecutive_fail = 0
            except Exception as e:  # noqa: BLE001
                fail_count += 1
                consecutive_fail += 1
                print(f"[WARN] ({idx}/{len(targets)}) 운항정보 {t['callsgn']} 실패: {e}")
                # 연속 실패가 임계를 넘으면 키가 이미 막힌 것으로 보고 중단한다.
                # 나머지를 계속 두드려도 전부 실패하고, 재시도 대기까지 곱해져
                # 수백 척이면 1시간 넘게 헛돈다.
                if consecutive_fail >= self.CONSECUTIVE_FAILURE_LIMIT:
                    aborted = True
                    print(
                        f"\n[ABORT] 연속 {consecutive_fail}회 실패 — 남은 "
                        f"{len(targets) - idx}척을 건너뜁니다.\n"
                        f"        429(Too Many Requests)가 계속된다면 UPA 호출 한도가 "
                        f"소진된 상태입니다.\n"
                        f"        조치: (1) 시간을 두고(일일 한도면 자정 이후) 재실행 "
                        f"(2) data.go.kr 에서 트래픽 증량 신청\n"
                        f"        (3) 다른 팀원이 같은 키로 동시 수집 중인지 확인\n"
                    )
                    break

            if idx % 50 == 0 and not aborted:
                print(f"  ... 진행 {idx}/{len(targets)}척 (성공 {ok_count} / 실패 {fail_count})")

        status = "중단됨" if aborted else "완료"
        print(
            f"[COLLECT] 운항정보 {len(all_items)}건 "
            f"(대상 {len(targets)}척 중 성공 {ok_count} / 실패 {fail_count}, {status})"
        )
        return self.save_raw(prefix, all_items)

    def collect_intg_cargo(self, targets: list) -> str:
        """
        통합화물 수집. targets 각 항목 필수키:
          bzentyCd, callsgn, prtCd, ptentYr, vyg
        """
        ep, prefix = ENDPOINTS["intg_cargo"]
        return self._collect_cargo(ep, prefix, targets, "통합화물")

    def collect_inprt_cargo(self, targets: list) -> str:
        ep, prefix = ENDPOINTS["inprt_cargo"]
        return self._collect_cargo(ep, prefix, targets, "내항화물")

    def _collect_cargo(self, ep, prefix, targets, label):
        all_items = []
        required = ("bzentyCd", "callsgn", "prtCd", "ptentYr", "vyg")
        for t in targets:
            missing = [k for k in required if not t.get(k)]
            if missing:
                print(f"[WARN] {label} 건너뜀(필수누락 {missing}): {t}")
                continue
            params = {k: t[k] for k in required}
            try:
                all_items.extend(self.fetch_all(ep, params))
            except Exception as e:  # noqa: BLE001
                print(f"[WARN] {label} {t} 실패: {e}")
        print(f"[COLLECT] {label} {len(all_items)}건 (대상 {len(targets)})")
        return self.save_raw(prefix, all_items)


# ===========================================================================
# 헬퍼
# ===========================================================================
def _extract_items(data) -> list:
    if not isinstance(data, dict):
        return []
    body = data.get("response", {}).get("body", {})
    items = body.get("items") if isinstance(body, dict) else None
    if isinstance(items, dict):
        item = items.get("item", [])
    else:
        item = items
    if item is None:
        return []
    return [item] if isinstance(item, dict) else list(item)


def _extract_total(data):
    try:
        return int(data["response"]["body"]["totalCount"])
    except (KeyError, TypeError, ValueError):
        return None


def derive_nvgt_targets_from_position(position_items: list, year: str) -> list:
    """선박위치 결과에서 운항정보 조회용 (callsgn, ptentVtsYr, vyg) 목록을 만든다."""
    seen = set()
    targets = []
    for it in position_items:
        cs = (it.get("callsgn") or "").strip()
        if not cs or cs in seen:
            continue
        seen.add(cs)
        targets.append({"callsgn": cs, "ptentVtsYr": year, "vyg": (it.get("vyg") or "").strip() or None})
    return targets


# ===========================================================================
# 수집 + 전처리 한 번에
# ===========================================================================
def collect_and_preprocess(service_key: str = None, year: str = None,
                           cargo_targets: list = None):
    """
    실전 수집 흐름 예시.
      1) 전체 조회형 4종 수집(선박위치/하역/부두/정박지)
      2) 선박위치에서 callsgn 뽑아 운항정보 수집
      3) (선택) cargo_targets 가 있으면 화물정보 수집
      4) 전처리 실행 -> data/staging/*.csv
    """
    from . import upa_preprocess as upa  # 지연 import (pandas 의존 분리)

    year = year or dt.datetime.now().strftime("%Y")
    client = UpaClient(service_key)

    # 1) 전체 조회형
    pos_path = client.collect_vessel_position()
    client.collect_unload_record()
    client.collect_berth_facility()
    client.collect_anchorage()

    # 2) 운항정보 (선박위치 callsgn 기반)
    with open(pos_path, encoding="utf-8") as f:
        pos_items = _extract_items(json.load(f))
    nvgt_targets = derive_nvgt_targets_from_position(pos_items, year)
    if nvgt_targets:
        client.collect_vessel_nvgt(nvgt_targets)

    # 3) 화물정보 (업체코드 등 필수 → 호출측에서 targets 제공해야 함)
    if cargo_targets:
        client.collect_intg_cargo(cargo_targets)

    # 4) 전처리
    print("\n[PREPROCESS] staging 생성 시작")
    base = RAW_DIR
    upa.run_pipeline("vessel_position", [f"{base}/upa_vessel_position_raw.json"])
    upa.run_pipeline("unload_record", [f"{base}/upa_unload_record_raw.json"])
    upa.run_pipeline("berth_facility", [f"{base}/upa_berth_facility_raw.json"])
    upa.run_pipeline("anchorage", [f"{base}/upa_anchorage_raw.json"])
    if os.path.exists(f"{base}/upa_vessel_nvgt_raw.json"):
        upa.run_pipeline("vessel_nvgt", [f"{base}/upa_vessel_nvgt_raw.json"])
    intg = f"{base}/upa_intg_cargo_raw.json"
    inprt = f"{base}/upa_inprt_cargo_raw.json"
    if os.path.exists(intg) or os.path.exists(inprt):
        upa.run_cargo_combined(
            intg_paths=[intg] if os.path.exists(intg) else [],
            inprt_paths=[inprt] if os.path.exists(inprt) else [],
        )
    print("[DONE] 수집 + 전처리 완료")


if __name__ == "__main__":
    # 환경변수 UPA_SERVICE_KEY 필요
    collect_and_preprocess()
