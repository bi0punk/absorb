import re
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from . import constants


def normalize_profile_url(raw_value: str) -> str:
    value = (raw_value or "").strip()
    if not value:
        return ""

    value = value.rstrip("/")
    if not value:
        return ""

    if value.startswith("@"):
        value = value[1:]

    if "instagram.com" not in value and not value.startswith("http"):
        value = f"https://www.instagram.com/{value}"
    elif value.startswith("www.instagram.com/"):
        value = f"https://{value}"
    elif value.startswith("instagram.com/"):
        value = f"https://www.{value}"

    match = re.search(r"instagram\.com/([^/?#]+)/?", value, re.IGNORECASE)
    if match:
        username = match.group(1)
        return f"https://www.instagram.com/{username}/"

    return value + "/"


def extract_source_username(profile_url: str) -> str:
    value = (profile_url or "").strip()
    if not value:
        return ""

    match = re.search(r"instagram\.com/([^/?#]+)/?", value, re.IGNORECASE)
    if match:
        return match.group(1).strip().lower()

    if value.startswith("@"):
        return value[1:].strip().lower()

    return value.strip().strip("/").lower()


def build_source_metadata(profile_url: str) -> dict[str, str]:
    normalized_url = normalize_profile_url(profile_url) if profile_url else ""
    username = extract_source_username(normalized_url or profile_url)
    return {
        "profile_url": normalized_url or profile_url or "",
        "source_username": username,
        "source_label": f"@{username}" if username else "",
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
        val = int(str(raw_value).strip())
    except (ValueError, TypeError):
        return fallback
    if val < 1:
        return fallback
    return min(val, constants.MAX_LIMIT)


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
    normalized = parse_content_mode(content_mode)
    if normalized == "post":
        return 'a[href*="/p/"]'
    if normalized == "reel":
        return 'a[href*="/reel/"]'
    return 'a[href*="/p/"], a[href*="/reel/"]'


def split_raw_source_entries(raw_values: Iterable[str]) -> list[str]:
    items: list[str] = []
    for raw in raw_values:
        if raw is None:
            continue
        for part in re.split(r"[\n,;]+", str(raw)):
            token = part.strip()
            if token:
                items.append(token)
    return items


def parse_profile_sources(raw_values: Iterable[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()

    for part in split_raw_source_entries(raw_values):
        url = normalize_profile_url(part)
        if not url or url in seen:
            continue
        seen.add(url)
        normalized.append(url)

    return normalized


def parse_source_jobs(
    raw_values: Iterable[str], default_limit: int = constants.DEFAULT_LIMIT
) -> list[dict[str, int | str]]:
    jobs_by_url: dict[str, dict[str, int | str]] = {}
    default_limit = parse_positive_limit(default_limit, constants.DEFAULT_LIMIT)

    for token in split_raw_source_entries(raw_values):
        profile_token = token
        source_limit = default_limit

        match = re.match(r"^(.*?)(?:\s*(?:=|\|)\s*(\d+))$", token)
        if match:
            profile_token = match.group(1).strip()
            source_limit = parse_positive_limit(match.group(2), default_limit)

        profile_url = normalize_profile_url(profile_token)
        if not profile_url:
            continue

        jobs_by_url[profile_url] = {
            "profile_url": profile_url,
            "limit": source_limit,
        }

    return list(jobs_by_url.values())


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
