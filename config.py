import os
from dotenv import load_dotenv

load_dotenv()

NAVER_API_BASE = os.getenv("NAVER_API_BASE", "https://api.naver.com").rstrip("/")
NAVER_API_KEY = os.getenv("NAVER_API_KEY", "").strip()
NAVER_SECRET_KEY = os.getenv("NAVER_SECRET_KEY", "").strip()
NAVER_CUSTOMER_ID = os.getenv("NAVER_CUSTOMER_ID", "").strip()

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()

RANK_THRESHOLD = float(os.getenv("RANK_THRESHOLD", "1.5"))
MIN_IMP = int(os.getenv("MIN_IMP", "30"))
STREAK_THRESHOLD = int(os.getenv("STREAK_THRESHOLD", "2"))

MAX_IDS_PER_CALL = int(os.getenv("MAX_IDS_PER_CALL", "200"))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "20"))
HTTP_RETRY = int(os.getenv("HTTP_RETRY", "3"))
HTTP_RETRY_BACKOFF = float(os.getenv("HTTP_RETRY_BACKOFF", "1.5"))