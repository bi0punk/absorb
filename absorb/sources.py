import re
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from . import constants


def normalize_profile_url(raw_value: str) -> str:
    raw_value = raw_value.strip().rstrip("/")
    m = re.search(r"(?:https?://)?(?:www\.)?instagram\.com/([^/?]+)", raw_value)
    if m:
        return f"https://www.instagram.com/{m.group(1)}/"
    raw_value = re.sub(r"^https?://", "", raw_value)
    raw_value = re.sub(r"^www\.", "", raw_value)
    if raw_value.startswith("instagram.com/"):
        return f"https://www.{raw_value}"
    if "/" in raw_value:
        return f"https://www.instagram.com/{raw_value.split('/')[0]}/"
    return f"https://www.instagram.com/{raw_value}/"


def extract_source_username(profile_url: str) -> str:
    return profile_url.rstrip("/").rsplit("/", 1)[-1]


def build_source_metadata(profile_url: str) -> dict[str, str]:
    username = extract_source_username(profile_url)
    return {
        "profile_url": profile_url,
        "source_username": username,
        "source_label": f"@{username}",
    }


def sanitize_source_dirname(raw_value: str) -> str:
    sanitized = re.sub(r"[^a-zA-Z0-9_\-.]", "_", raw_value)
    sanitized = sanitized.strip("._")
    if sanitized in (".", "..", ""):
        return "source"
    return sanitized


def get_source_storage_dir(profile_url: str) -> Path:
    username = extract_source_username(profile_url)
    return constants.BASE_DIR / sanitize_source_dirname(username)


def parse_positive_limit(raw_value: Any, fallback: int = constants.DEFAULT_LIMIT) -> int:
    try:
        val = int(raw_value)
    except (ValueError, TypeError):
        return fallback
    if val < 1:
        return fallback
    return val


def parse_content_mode(raw_value: str | None, fallback: str = "both") -> str:
    if not raw_value:
        return fallback
    val = str(raw_value).strip().lower()
    if val in ("both", "post", "reel"):
        return val
    return fallback


def build_content_mode_label(content_mode: str) -> str:
    labels = {"both": "Posts + Reels", "post": "Posts only", "reel": "Reels only"}
    return labels.get(content_mode, content_mode)


def get_profile_link_selector(content_mode: str) -> str:
    selectors = {
        "post": 'a[href*="/p/"]',
        "reel": 'a[href*="/reel/"]',
        "both": 'a[href*="/p/"], a[href*="/reel/"]',
    }
    return selectors.get(content_mode, selectors["both"])


def split_raw_source_entries(raw_values: Iterable[str]) -> list[str]:
    entries: list[str] = []
    for raw in raw_values:
        if not raw or not raw.strip():
            continue
        entries.extend(re.split(r"[,;\n]+", raw))
    return [e.strip() for e in entries if e.strip()]


def parse_profile_sources(raw_values: Iterable[str]) -> list[str]:
    entries = split_raw_source_entries(raw_values)
    seen: set = set()
    result: list[str] = []
    for e in entries:
        norm = normalize_profile_url(e)
        if norm not in seen:
            seen.add(norm)
            result.append(norm)
    return result


def parse_source_jobs(
    raw_values: Iterable[str], default_limit: int = constants.DEFAULT_LIMIT
) -> list[dict]:
    entries = split_raw_source_entries(raw_values)
    seen: set = set()
    result: list[dict] = []
    for e in entries:
        parts = e.split("|")
        raw_url = parts[0].strip()
        limit = parse_positive_limit(parts[1].strip() if len(parts) > 1 else None, default_limit)
        norm = normalize_profile_url(raw_url)
        if norm not in seen:
            seen.add(norm)
            result.append({"profile_url": norm, "limit": limit})
    return result


def format_source_job_arg(profile_url: str, limit: int) -> str:
    return f"{profile_url}|{limit}"


def parse_cli_sources_and_limit(argv: list[str]) -> tuple[list[str], int]:
    limit: int = constants.DEFAULT_LIMIT
    sources: list[str] = []
    for arg in argv:
        if arg.startswith("--"):
            continue
        if arg.isdigit():
            limit = parse_positive_limit(arg)
        else:
            sources.append(arg)
    return sources, limit


def parse_cli_jobs(argv: list[str]) -> tuple[list[dict], int | None, str]:
    jobs: list[dict] = []
    global_limit: int | None = None
    content_mode: str = "both"
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg == "--content-mode" and i + 1 < len(argv):
            content_mode = parse_content_mode(argv[i + 1])
            i += 2
            continue
        if arg.startswith("--"):
            i += 1
            continue
        if arg.isdigit():
            global_limit = parse_positive_limit(arg)
        else:
            parts = arg.split("|")
            raw_url = parts[0].strip()
            limit = parse_positive_limit(parts[1].strip() if len(parts) > 1 else None, constants.DEFAULT_LIMIT)
            norm = normalize_profile_url(raw_url)
            jobs.append({"profile_url": norm, "limit": limit})
        i += 1
    return jobs, global_limit, content_mode
