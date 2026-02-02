from typing import Dict, Any, List
from utils import setup_logger
from keyword_map import load_keywords_txt, build_keyword_map
from stats_checker import fetch_stats_by_keyword_ids, summarize_by_keyword
from state_store import load_state, save_state
from slack_notify import send_slack
from config import RANK_THRESHOLD, MIN_IMP, STREAK_THRESHOLD

LOGGER = setup_logger()

def is_top_like(imp: float, avg_rnk: float) -> bool:
    if avg_rnk is None:
        return False
    if imp < MIN_IMP:
        return False
    return avg_rnk <= RANK_THRESHOLD

def main():
    # 1) 입력 키워드 로드
    wanted_keywords = load_keywords_txt("keywords.txt")
    LOGGER.info(f"Loaded {len(wanted_keywords)} keywords from keywords.txt")

    # 2) 계정 키워드ID 매핑(캐시)
    km = build_keyword_map(force_refresh=True)
    kw_map = km.get("map", {})
    LOGGER.info(f"Keyword map loaded: {len(kw_map)} unique keywords in account cache")

    # 3) 요청 키워드 중 계정에 존재하는 키워드만 추리기
    id_to_keyword: Dict[str, str] = {}
    keyword_ids: List[str] = []
    missing: List[str] = []

    for kw in wanted_keywords:
        entries = kw_map.get(kw)
        if not entries:
            missing.append(kw)
            continue
        for e in entries:
            kid = e["id"]
            keyword_ids.append(kid)
            id_to_keyword[kid] = kw

    if missing:
        LOGGER.warning(f"Missing in account (not found by API): {len(missing)} e.g. {missing[:10]}")

    if not keyword_ids:
        LOGGER.error("No keyword IDs to check. Stop.")
        return

    LOGGER.info(f"Checking keyword IDs: {len(keyword_ids)}")

    # 4) /stats 조회
    rows = fetch_stats_by_keyword_ids(keyword_ids)
    LOGGER.info(f"/stats rows received: {len(rows)}")

    summary = summarize_by_keyword(rows, id_to_keyword)

    # 5) state 로드
    state = load_state()

    # 6) 연속(2회) 판정 + Slack
    alerts = []

    for kw, devs in summary.items():
        st = state.setdefault(kw, {
            "PC": {"streak": 0, "last_avgRnk": None, "last_imp": 0},
            "MOBILE": {"streak": 0, "last_avgRnk": None, "last_imp": 0},
        })

        for dev_key in ["PC", "MOBILE"]:
            # API가 dev를 "PC"/"MOBILE"이 아닌 형태로 줄 수도 있어
            dev_data = devs.get(dev_key) or devs.get(dev_key.lower()) or devs.get(dev_key.upper())
            if not dev_data:
                continue

            imp = dev_data.get("imp") or 0
            avg = dev_data.get("avgRnk")

            top_like = is_top_like(imp, avg)

            if top_like:
                st[dev_key]["streak"] = int(st[dev_key]["streak"]) + 1
            else:
                st[dev_key]["streak"] = 0

            st[dev_key]["last_avgRnk"] = avg
            st[dev_key]["last_imp"] = imp

            if st[dev_key]["streak"] >= STREAK_THRESHOLD:
                alerts.append((kw, dev_key, st[dev_key]["streak"], avg, imp))
                # 스팸 방지: 알림 후 streak 리셋(원하면 유지로 바꿀 수 있음)
                st[dev_key]["streak"] = 0

    # 7) 저장
    save_state(state)

    # 8) Slack 전송
    if alerts:
        lines = ["🚨 *네이버 키워드 상단(1위급) 고착 감지* (API avgRnk 기준)"]
        for kw, dev, streak, avg, imp in alerts[:50]:
            lines.append(f"- `{kw}` [{dev}] : streak={streak}, avgRnk={avg}, imp={int(imp)} (기준: avgRnk<={RANK_THRESHOLD}, imp>={MIN_IMP})")
        msg = "\n".join(lines)
        send_slack(msg)
        LOGGER.info(f"Sent Slack alerts: {len(alerts)}")
    else:
        LOGGER.info("No alerts.")

if __name__ == "__main__":
    main()