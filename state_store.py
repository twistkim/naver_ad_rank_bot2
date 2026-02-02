from typing import Dict, Any
from utils import read_json, write_json

STATE_PATH = "state.json"

def load_state() -> Dict[str, Any]:
    return read_json(STATE_PATH, default={})

def save_state(state: Dict[str, Any]) -> None:
    write_json(STATE_PATH, state)