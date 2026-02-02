import requests
from config import SLACK_WEBHOOK_URL

def send_slack(text: str) -> None:
    if not SLACK_WEBHOOK_URL:
        return
    requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=10)