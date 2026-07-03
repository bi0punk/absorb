import json
from pathlib import Path
from typing import Any

from . import constants


def read_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    try:
        with open(str(path), encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return default


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(str(path), "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def load_source_state() -> dict[str, dict]:
    return read_json(constants.SOURCE_STATE_FILE, {})


def save_source_state(data: dict[str, dict]) -> None:
    write_json(constants.SOURCE_STATE_FILE, data)


def get_source_state_entry(profile_url: str) -> dict:
    state = load_source_state()
    return state.get(profile_url, {})


def infer_latest_shortcode_from_summary(profile_url: str) -> str:
    from .sources import extract_source_username
    username = extract_source_username(profile_url)
    summary = read_json(constants.SUMMARY_FILE, [])
    for post in summary:
        if post.get("source_username") == username:
            shortcode = post.get("shortcode", "")
            if shortcode:
                return shortcode
    return ""


def get_last_known_shortcode(profile_url: str) -> str:
    entry = get_source_state_entry(profile_url)
    return entry.get("latest_visible_shortcode", "")


def update_source_state(
    profile_url: str,
    latest_visible_shortcode: str,
    latest_visible_kind: str,
) -> None:
    state = load_source_state()
    if profile_url not in state:
        state[profile_url] = {}
    state[profile_url]["latest_visible_shortcode"] = latest_visible_shortcode
    state[profile_url]["latest_visible_kind"] = latest_visible_kind
    save_source_state(state)


def merge_payloads(existing: list[dict], new_items: list[dict]) -> list[dict]:
    seen: set = set()
    result: list[dict] = []
    for item in existing + new_items:
        shortcode = item.get("shortcode", "")
        if shortcode and shortcode not in seen:
            seen.add(shortcode)
            result.append(item)
    return result


def estimate_max_scrolls(target_new_count: int | None, collect_all: bool) -> int:
    if collect_all or target_new_count is None:
        return 200
    return min(target_new_count * 3, 200)
