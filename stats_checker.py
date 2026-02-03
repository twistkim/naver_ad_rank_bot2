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

# stats_checker.py

from typing import Dict, Any

def summarize_by_keyword(rows, id_to_keyword: Dict[str, str]) -> Dict[str, Any]:
    """
    API /stats rows 예시:
      {
        'avgRnk': 1,
        'id': '...',
        'clkCnt': 243,
        'impCnt': 345,
        'breakdowns': [{'name': '모바일', 'avgRnk': 1, 'clkCnt': 243, 'impCnt': 345}]
      }

    반환:
      {
        '키워드': {
           'PC': {'avgRnk': ..., 'imp': ...},
           'MOBILE': {'avgRnk': ..., 'imp': ...}
        }
      }
    """
    out: Dict[str, Any] = {}

    def normalize_dev(name: str):
        n = (name or "").strip().lower()
        if "모바일" in n or "mobile" in n:
            return "MOBILE"
        if n == "pc" or "desktop" in n or "데스크" in n or "피씨" in n:
            return "PC"
        return None

    for r in rows or []:
        kid = r.get("id")
        kw = id_to_keyword.get(kid)
        if not kw:
            continue

        st = out.setdefault(kw, {
            "PC": {"avgRnk": None, "imp": 0},
            "MOBILE": {"avgRnk": None, "imp": 0},
        })

        # breakdowns가 있으면 breakdowns 우선
        bds = r.get("breakdowns") or []
        if bds:
            for b in bds:
                dev = normalize_dev(b.get("name", ""))
                if not dev:
                    continue
                imp = b.get("impCnt")
                if imp is None:
                    imp = b.get("imp", 0)
                avg = b.get("avgRnk")

                # 같은 키워드/디바이스에 여러 row가 올 수 있으면,
                # 노출이 더 큰 값으로 최신 스냅샷을 대표시키자.
                if imp > (st[dev].get("imp") or 0):
                    st[dev]["imp"] = int(imp or 0)
                    st[dev]["avgRnk"] = avg
        else:
            # breakdowns가 없으면 전체 row를 그대로 저장(디바이스 미상)
            # → 일단 모바일/PC에 넣기 애매하니, imp가 있으면 둘 다 채우지 않고 패스
            pass

    return out