# -*- coding: utf-8 -*-
"""수집기 공통 HTTP GET — 일시 장애 재시도 + 인증키가 새지 않는 오류 문구.

왜 필요한가
-----------
재시도가 수집기마다 제각각이었다(2026-09-24 점검). UPA·파고만 제대로 있었고,
기상 실황·조위·PORT-MIS·단기예보·선박제원·MSDS 는 한 번 실패하면 그 회차를
버렸다. 특히 기상 실황은 과거 조회가 불가능한 유일한 도메인이라(현재값 1건만
준다) 놓친 10분은 영구히 사라진다.

재시도 대상 / 비대상
--------------------
  · 재시도: 연결 오류, 타임아웃, 응답 끊김, 429, 5xx — 잠시 뒤 같은 요청이 성공할 수 있는 것.
    (tide_forecast 2026-09-22 실측: 8일 요청 중 5일이 504, 다시 보내면 정상)
  · 재시도 안 함: 그 밖의 4xx, 응답 본문의 resultCode 오류(키 미등록·파라미터 오류)
    — 다시 물어도 같은 답이 온다. 이 판정은 각 수집기가 한다.

인증키 누출 방지
----------------
requests 의 예외 문구에는 **요청 URL 전체**가 들어간다. data.go.kr 계열은
serviceKey 를 쿼리스트링으로 받으므로 예외를 그대로 print/raise 하면 키가 로그와
raw JSON(오류 목록)에 남는다. 그래서 FetchError 는 상태 코드·예외 이름만 담고,
원 예외와의 연결도 끊는다(`from None`) — traceback 에 원 예외가 따라 나오지 않게.
"""
from __future__ import annotations

import time

import requests

RETRY_STATUS = frozenset({429, 500, 502, 503, 504})
DEFAULT_RETRIES = 3
_BACKOFF_SEC = (2.0, 4.0, 8.0)  # 시도 1, 2, 3 실패 후 대기
_MAX_RETRY_AFTER_SEC = 60.0


class FetchError(RuntimeError):
    """재시도 끝에 실패. 문구에 URL·인증키가 없다."""


def safe_err(exc: BaseException) -> str:
    """로그에 남겨도 되는 실패 문구 — requests 계열은 상태 코드 + 예외 이름만."""
    if isinstance(exc, requests.RequestException):
        status = getattr(getattr(exc, "response", None), "status_code", None)
        return f"HTTP {status} ({type(exc).__name__})" if status else type(exc).__name__
    return f"{type(exc).__name__}: {exc}"


def _wait_sec(resp: requests.Response | None, attempt: int) -> float:
    if resp is not None and resp.status_code == 429:
        retry_after = resp.headers.get("Retry-After")
        if retry_after:
            try:
                return min(float(retry_after), _MAX_RETRY_AFTER_SEC)
            except ValueError:
                pass
        return _MAX_RETRY_AFTER_SEC / 2  # 한도 초과를 바로 다시 두드리지 않는다
    return _BACKOFF_SEC[min(attempt - 1, len(_BACKOFF_SEC) - 1)]


def get_with_retry(
    url: str,
    *,
    params: dict | None = None,
    headers: dict | None = None,
    timeout: float = 15,
    retries: int = DEFAULT_RETRIES,
    label: str = "",
) -> requests.Response:
    """GET 을 최대 retries 회 시도하고 2xx 응답을 돌려준다.

    url 에 serviceKey 가 이미 인코딩돼 들어 있어도 된다(requests 는 %XX 를 다시
    인코딩하지 않는다). 실패하면 FetchError — 문구에 URL 이 없다.
    """
    name = label or url.split("?", 1)[0].rsplit("/", 1)[-1]
    for attempt in range(1, retries + 1):
        resp = None
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
        except requests.RequestException as e:  # 연결·타임아웃·응답 끊김 등
            err = safe_err(e)
        else:
            if resp.status_code not in RETRY_STATUS:
                try:
                    resp.raise_for_status()
                except requests.HTTPError as e:  # 재시도해도 같은 4xx
                    raise FetchError(f"{name} 실패: {safe_err(e)}") from None
                return resp
            err = f"HTTP {resp.status_code}"

        if attempt == retries:
            raise FetchError(f"{name} 실패 ({retries}회 시도): {err}")
        wait = _wait_sec(resp, attempt)
        print(f"  [재시도] {name} {attempt}/{retries} 실패: {err} -> {wait:.0f}초 뒤 다시")
        time.sleep(wait)

    raise AssertionError("unreachable")  # pragma: no cover
