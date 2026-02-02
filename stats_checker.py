from typing import Dict, List, Any
from datetime import date
import json

from naver_client import request_json
from utils import chunked, jitter_sleep
from config import MAX_IDS_PER_CALL

def fetch_stats_by_keyword_ids(keyword_ids: List[str]) -> List[Dict[str, Any]]:
    """
    /stats 호출.

    중요:
    - ids는 JSON 배열이 아니라 콤마로 연결된 문자열("id1,id2,...") 형태로 보내는 것이 가장 호환이 좋다.
    - mixed ids(예: grp-/cmp- 등)가 섞이면 11001이 날 수 있으므로 nkw-만 남긴다.
    - breakdown=pcMblTp 조합이 계정/필드에 따라 11001이 날 수 있어 fallback(분해 없이 최소 필드)로 재시도한다.

    Response는 계정/타입에 따라 구조가 달라질 수 있어, 일단 raw list로 받는다.
    """

    # keyword id만 남김
    keyword_ids = [x for x in keyword_ids if isinstance(x, str) and x.startswith("nkw-")]
    if not keyword_ids:
        return []

    today = date.today().isoformat()
    time_range = {"since": today, "until": today}

    # 1차 시도 필드(너무 욕심내면 11001이 날 수 있어 최소로)
    fields_primary = ["impCnt", "clkCnt", "avgRnk"]
    # fallback 필드(가장 호환 좋은 최소 조합)
    fields_fallback = ["impCnt", "avgRnk"]

    all_rows: List[Dict[str, Any]] = []

    for batch in chunked(keyword_ids, MAX_IDS_PER_CALL):
        ids_str = ",".join(batch)

        # 1) PC/모바일 분해 시도
        params = {
            "ids": ids_str,
            "fields": json.dumps(fields_primary, ensure_ascii=False),
            "timeRange": json.dumps(time_range, ensure_ascii=False),
            "breakdown": "pcMblTp",  # PC/모바일 구분
        }

        try:
            data = request_json("GET", "/stats", params=params)
        except RuntimeError as e:
            # 11001 힌트: breakdown/fields 조합이 지원되지 않는 경우가 많아 fallback
            if "11001" not in str(e):
                raise

            params2 = {
                "ids": ids_str,
                "fields": json.dumps(fields_fallback, ensure_ascii=False),
                "timeRange": json.dumps(time_range, ensure_ascii=False),
                # breakdown 없이 재시도
            }
            data = request_json("GET", "/stats", params=params2)

        # 응답 정규화: 보통 list이지만 dict로 오는 경우도 있어서 data 키를 우선 확인
        rows: Any
        if isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
            rows = data["data"]
        else:
            rows = data

        if isinstance(rows, list):
            all_rows.extend(rows)
        else:
            # 예상 밖이면 그대로 감싸서 남김
            all_rows.append({"_raw": rows})

        jitter_sleep(0.05, 0.15)

    return all_rows

def summarize_by_keyword(raw_rows: List[Dict[str, Any]], id_to_keyword: Dict[str, str]) -> Dict[str, Dict[str, Dict[str, float]]]:
    """
    결과를 keyword 텍스트 기준으로 합치기.
    반환 형태:
      {
        "인터넷": {
           "PC": {"imp": 123, "clk": 4, "avgRnk": 1.12},
           "MOBILE": {"imp": 77, "clk": 1, "avgRnk": 1.35}
        },
        ...
      }

    avgRnk는 (노출 가중 평균)으로 합산.
    """
    acc: Dict[str, Dict[str, Dict[str, float]]] = {}

    for row in raw_rows:
        rid = row.get("id") or row.get("nccKeywordId") or row.get("keywordId")
        if not rid:
            continue

        kw = id_to_keyword.get(rid)
        if not kw:
            continue

        # breakdown 키가 "pcMblTp"로 오기도 하고, "pcMblTp" 값이 다른 키로 오기도 함
        # 실전에서는 row를 찍어보며 맞춰야 하는데, 보통 아래 중 하나.
        dev = (row.get("pcMblTp") or row.get("pcMblTpNm") or row.get("pcMblTpType") or "").upper()
        if not dev:
            # 일부 응답은 dev 값이 "PC"/"MOBILE" 등이 아닐 수 있어 fallback
            dev = (row.get("device") or row.get("type") or "UNKNOWN").upper()

        imp = float(row.get("impCnt") or 0)
        clk = float(row.get("clkCnt") or 0)
        avg = row.get("avgRnk")
        avg = float(avg) if avg is not None and avg != "" else None

        acc.setdefault(kw, {}).setdefault(dev, {"imp": 0.0, "clk": 0.0, "avgRnk": 0.0, "_w": 0.0})

        acc[kw][dev]["imp"] += imp
        acc[kw][dev]["clk"] += clk

        # avgRnk 가중합: 노출 기준
        if avg is not None and imp > 0:
            acc[kw][dev]["avgRnk"] += avg * imp
            acc[kw][dev]["_w"] += imp

    # finalize
    out: Dict[str, Dict[str, Dict[str, float]]] = {}
    for kw, devs in acc.items():
        out[kw] = {}
        for dev, m in devs.items():
            w = m.get("_w", 0.0)
            out[kw][dev] = {
                "imp": m["imp"],
                "clk": m["clk"],
                "avgRnk": (m["avgRnk"] / w) if w > 0 else None
            }
    return out