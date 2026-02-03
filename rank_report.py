#!/usr/bin/env python3
# rank_report.py
# - Read snapshot JSON (e.g., out/ranks_latest.json)
# - Produce summary report JSON (counts, buckets, top/bottom lists)
#
# Usage:
#   python rank_report.py
#   python rank_report.py --input out/ranks_latest.json --outdir out --min-imp 1 --top 50

import argparse
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


def utc_ts_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def bucket_rank(avg_rnk: Optional[float]) -> str:
    """Bucket by avg rank. None => 'none'."""
    if avg_rnk is None:
        return "none"
    try:
        r = float(avg_rnk)
    except Exception:
        return "invalid"

    if r <= 1:
        return "1"
    if r <= 3:
        return "2-3"
    if r <= 5:
        return "4-5"
    if r <= 10:
        return "6-10"
    if r <= 20:
        return "11-20"
    if r <= 50:
        return "21-50"
    if r <= 100:
        return "51-100"
    return "100+"


def get_dev(d: Dict[str, Any], dev: str) -> Dict[str, Any]:
    # dev 키가 "PC"/"MOBILE" 또는 케이스 변형일 수 있어서 방어
    return d.get(dev) or d.get(dev.lower()) or d.get(dev.upper()) or {}


def safe_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def safe_int(x: Any) -> int:
    try:
        return int(float(x))
    except Exception:
        return 0


def build_report(snapshot: Dict[str, Any], min_imp: int, top_n: int) -> Dict[str, Any]:
    meta = snapshot.get("_meta", {}) if isinstance(snapshot, dict) else {}
    kws = snapshot.get("keywords", {}) if isinstance(snapshot, dict) else {}
    missing = snapshot.get("missing_in_account", []) if isinstance(snapshot, dict) else []

    # stats
    devs = ["PC", "MOBILE"]
    counts = {
        "total_keywords": len(kws) if isinstance(kws, dict) else 0,
        "missing_in_account": len(missing) if isinstance(missing, list) else 0,
    }

    # per-device aggregates
    per_dev = {}
    top_lists: Dict[str, List[Dict[str, Any]]] = {d: [] for d in devs}
    bottom_lists: Dict[str, List[Dict[str, Any]]] = {d: [] for d in devs}

    # buckets
    buckets: Dict[str, Dict[str, int]] = {d: {} for d in devs}

    # Gather ranks for sorting
    ranks_for_sort: Dict[str, List[Tuple[str, float, int]]] = {d: [] for d in devs}  # (kw, avg, imp)

    for kw, info in (kws.items() if isinstance(kws, dict) else []):
        if not isinstance(info, dict):
            continue

        for dev in devs:
            dev_data = get_dev(info, dev)
            avg = safe_float(dev_data.get("avgRnk"))
            imp = safe_int(dev_data.get("imp"))

            b = bucket_rank(avg)
            buckets[dev][b] = buckets[dev].get(b, 0) + 1

            # candidate for sorting only if meets min_imp and avg exists
            if avg is not None and imp >= min_imp:
                ranks_for_sort[dev].append((kw, avg, imp))

    # Compute per-dev stats
    for dev in devs:
        arr = ranks_for_sort[dev]
        arr_sorted = sorted(arr, key=lambda x: x[1])  # avg ascending (best first)

        top_items = arr_sorted[:top_n]
        bot_items = list(reversed(arr_sorted[-top_n:])) if arr_sorted else []

        top_lists[dev] = [{"keyword": k, "avgRnk": a, "imp": i} for (k, a, i) in top_items]
        bottom_lists[dev] = [{"keyword": k, "avgRnk": a, "imp": i} for (k, a, i) in bot_items]

        # avg of avgRnk (only where exists)
        avg_values = [a for (_, a, _) in arr]
        mean_avg = sum(avg_values) / len(avg_values) if avg_values else None

        per_dev[dev] = {
            "rank_items_count": len(arr),
            "mean_avgRnk": mean_avg,
            "buckets": dict(sorted(buckets[dev].items(), key=lambda kv: kv[0])),
        }

    report = {
        "_meta": {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "input_meta": meta,
            "min_imp_filter": min_imp,
            "top_n": top_n,
        },
        "counts": counts,
        "per_device": per_dev,
        "top": top_lists,
        "bottom": bottom_lists,
        "missing_in_account_sample": missing[:50] if isinstance(missing, list) else [],
    }
    return report


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="out/ranks_latest.json", help="snapshot json path")
    ap.add_argument("--outdir", default="out", help="output directory")
    ap.add_argument("--min-imp", type=int, default=1, help="only include items with imp >= min_imp for ranking lists")
    ap.add_argument("--top", type=int, default=50, help="top/bottom N items per device")
    args = ap.parse_args()

    snap_path = args.input
    outdir = args.outdir
    min_imp = args.min_imp
    top_n = args.top

    snapshot = load_json(snap_path)
    report = build_report(snapshot, min_imp=min_imp, top_n=top_n)

    # outputs
    ts = utc_ts_compact()
    out_latest = os.path.join(outdir, "report_latest.json")
    out_hist = os.path.join(outdir, f"report_{ts}.json")

    save_json(out_latest, report)
    save_json(out_hist, report)

    # console summary
    print(f"[OK] wrote: {out_latest}")
    print(f"[OK] wrote: {out_hist}")

    counts = report.get("counts", {})
    print(f"- total keywords in snapshot: {counts.get('total_keywords')}")
    print(f"- missing_in_account: {counts.get('missing_in_account')}")

    per_dev = report.get("per_device", {})
    for dev in ["PC", "MOBILE"]:
        d = per_dev.get(dev, {})
        print(f"\n[{dev}]")
        print(f"  - rank_items_count (avgRnk exists & imp>={min_imp}): {d.get('rank_items_count')}")
        print(f"  - mean_avgRnk: {d.get('mean_avgRnk')}")
        b = d.get("buckets", {})
        # show key buckets in stable order
        order = ["1", "2-3", "4-5", "6-10", "11-20", "21-50", "51-100", "100+", "none", "invalid"]
        line = "  - buckets: " + ", ".join([f"{k}={b.get(k,0)}" for k in order if k in b or k in ("none","invalid")])
        print(line)


if __name__ == "__main__":
    main()