from typing import Dict, List, Any
from utils import ensure_dir, read_json, write_json
from naver_client import request_json

CACHE_PATH = "cache/keyword_map.json"

def _norm_kw(s: str) -> str:
    return (s or "").strip()

def load_keywords_csv(path: str = "keywords.csv") -> List[str]:
    import csv
    kws: List[str] = []
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if "keyword" not in reader.fieldnames:
            raise ValueError("keywords.csv must have 'keyword' column")
        for row in reader:
            k = _norm_kw(row.get("keyword"))
            if k:
                kws.append(k)
    # 중복 제거(순서 유지)
    seen = set()
    out = []
    for k in kws:
        if k in seen:
            continue
        seen.add(k)
        out.append(k)
    return out

def load_keywords_txt(path: str = "keywords.txt") -> List[str]:
    kws: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            k = _norm_kw(line)
            if k:
                kws.append(k)

    # 중복 제거(순서 유지)
    seen = set()
    out = []
    for k in kws:
        if k in seen:
            continue
        seen.add(k)
        out.append(k)
    return out

def build_keyword_map(force_refresh: bool = False) -> Dict[str, List[Dict[str, Any]]]:
    """
    returns:
      {
        "인터넷": [
            {"id":"nkw-a001-...","keyword":"인터넷","adGroupId":"grp-...","campaignId":"cmp-..."}
        ],
        ...
      }
    """
    ensure_dir("cache")

    if not force_refresh:
        cached = read_json(CACHE_PATH, default=None)
        if isinstance(cached, dict) and cached.get("_meta"):
            return cached

    # 1) campaigns
    campaigns = request_json("GET", "/ncc/campaigns")

    kw_map: Dict[str, List[Dict[str, Any]]] = {}

    for c in campaigns:
        cid = c.get("nccCampaignId")
        if not cid:
            continue

        # 2) adgroups
        adgroups = request_json("GET", "/ncc/adgroups", params={"nccCampaignId": cid})

        for g in adgroups:
            gid = g.get("nccAdgroupId")
            if not gid:
                continue

            # 3) keywords (per adgroup)
            keywords = request_json("GET", "/ncc/keywords", params={"nccAdgroupId": gid})

            for kw in keywords:
                kid = kw.get("nccKeywordId")
                ktxt = _norm_kw(kw.get("keyword"))
                if not kid or not ktxt:
                    continue

                kw_map.setdefault(ktxt, []).append({
                    "id": kid,
                    "keyword": ktxt,
                    "adGroupId": gid,
                    "campaignId": cid,
                })

    wrapped = {
        "_meta": {"version": 1},
        "map": kw_map
    }
    write_json(CACHE_PATH, wrapped)
    return wrapped