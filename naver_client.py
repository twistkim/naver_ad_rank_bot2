import time
import hmac
import base64
import hashlib
import requests
import json  # 상단에 없으면 추가

from typing import Dict, Any, Optional, Tuple

from config import (
    NAVER_API_BASE, NAVER_API_KEY, NAVER_SECRET_KEY, NAVER_CUSTOMER_ID,
    HTTP_TIMEOUT, HTTP_RETRY, HTTP_RETRY_BACKOFF
)
from utils import jitter_sleep



def _normalize_stats_params(params: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not params:
        return params
    p = dict(params)

    # ✅ ids: list/tuple -> "id1,id2,id3" 문자열로
    if "ids" in p and isinstance(p["ids"], (list, tuple)):
        p["ids"] = ",".join(p["ids"])

    # ✅ fields: list -> JSON string
    if "fields" in p and isinstance(p["fields"], (list, tuple)):
        p["fields"] = json.dumps(list(p["fields"]), ensure_ascii=False)

    # ✅ timeRange: dict -> JSON string
    if "timeRange" in p and isinstance(p["timeRange"], dict):
        p["timeRange"] = json.dumps(p["timeRange"], ensure_ascii=False)

    # ✅ timeIncrement: int -> str (안전)
    if "timeIncrement" in p and isinstance(p["timeIncrement"], int):
        p["timeIncrement"] = str(p["timeIncrement"])

    return p

def _signature(timestamp_ms: str, method: str, uri: str, secret_key: str) -> str:
    # "{timestamp}.{method}.{uri}" -> HMAC-SHA256 -> base64
    message = f"{timestamp_ms}.{method}.{uri}"
    digest = hmac.new(
        secret_key.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256
    ).digest()
    return base64.b64encode(digest).decode("utf-8")


def _headers(method: str, uri: str) -> Dict[str, str]:
    ts = str(int(time.time() * 1000))
    sig = _signature(ts, method.upper(), uri, NAVER_SECRET_KEY)
    return {
        "Content-Type": "application/json; charset=UTF-8",
        "X-Timestamp": ts,
        "X-API-KEY": NAVER_API_KEY,
        "X-Customer": str(NAVER_CUSTOMER_ID),
        "X-Signature": sig,
    }


def _safe_parse_response(r: requests.Response) -> Tuple[Any, str]:
    """
    returns: (parsed_obj_or_text_or_none, raw_text_snippet)
    """
    txt = (r.text or "").strip()
    snippet = txt[:2000] if txt else ""
    if not txt:
        return None, ""
    # content-type 힌트가 json이면 json 우선
    ctype = (r.headers.get("Content-Type") or "").lower()
    if "json" in ctype:
        try:
            return r.json(), snippet
        except Exception:
            return txt, snippet
    # content-type이 애매해도 json 시도
    try:
        return r.json(), snippet
    except Exception:
        return txt, snippet


def request_json(method: str, uri: str, params: Optional[Dict[str, Any]] = None) -> Any:
    """
    method: GET/POST/PUT/DELETE
    uri: like "/ncc/campaigns", "/stats"
    params:
      - GET: querystring
      - others: JSON body
    """
    method_u = method.upper()
    url = f"{NAVER_API_BASE}{uri}"

    last_err: Optional[Exception] = None

    for attempt in range(1, HTTP_RETRY + 1):
        try:
            headers = _headers(method_u, uri)

            if method_u == "GET":
                if uri == "/stats":
                    params = _normalize_stats_params(params)
                r = requests.get(url, headers=headers, params=params, timeout=HTTP_TIMEOUT)
            else:
                r = requests.request(method_u, url, headers=headers, json=params, timeout=HTTP_TIMEOUT)

            data, snippet = _safe_parse_response(r)

            # ✅ 재시도 대상: 429 / 5xx (네트워크/일시 장애)
            if r.status_code == 429 or r.status_code >= 500:
                last_err = RuntimeError(f"HTTP {r.status_code} {url}: {snippet}")
                jitter_sleep(HTTP_RETRY_BACKOFF * attempt, 0.6)
                continue

            # ✅ 4xx는 보통 “요청이 잘못됨”이라 재시도해도 소용 없음
            if r.status_code >= 400:
                raise RuntimeError(f"HTTP {r.status_code} {url}: {data}")

            return data

        except Exception as e:
            last_err = e
            # 네트워크 오류 등은 재시도 가치 있음
            if attempt < HTTP_RETRY:
                jitter_sleep(HTTP_RETRY_BACKOFF * attempt, 0.6)

    raise last_err if last_err else RuntimeError(f"Unknown error calling {url}")