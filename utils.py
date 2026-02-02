import os
import json
import time
import random
import logging
from typing import Any, Dict, List

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def setup_logger() -> logging.Logger:
    ensure_dir("logs")
    logger = logging.getLogger("naver_ad_rank_bot2")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fh = logging.FileHandler("logs/app.log", encoding="utf-8")
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        logger.addHandler(sh)
    return logger

def chunked(lst: List[Any], size: int) -> List[List[Any]]:
    return [lst[i:i+size] for i in range(0, len(lst), size)]

def jitter_sleep(base: float = 0.15, spread: float = 0.25) -> None:
    time.sleep(base + random.random() * spread)

def read_json(path: str, default: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except Exception:
        return default

def write_json(path: str, data: Any) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)