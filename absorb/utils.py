import random
import time
from datetime import UTC, date, datetime

from .constants import MANUAL_LOG_FILE, RUNTIME_LOG_FILE


def _ts() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def append_manual_log(message: str, ignore_scheduler: bool = True) -> None:
    if ignore_scheduler:
        import os
        if os.environ.get("SCRAPER_RUN_CONTEXT") == "scheduler":
            return
    MANUAL_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(str(MANUAL_LOG_FILE), "a", encoding="utf-8") as f:
        f.write(f"[{_ts()}] {message}\n")


def append_runtime_log(message: str) -> None:
    RUNTIME_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(str(RUNTIME_LOG_FILE), "a", encoding="utf-8") as f:
        f.write(f"[{_ts()}] {message}\n")


def log(message: str) -> None:
    print(message, flush=True)
    append_runtime_log(message)


def log_section(title: str) -> None:
    sep = "=" * 70
    log("")
    log(sep)
    log(f"  {title}")
    log(sep)


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def local_today() -> date:
    import os
    tz_str = os.environ.get("TZ", "America/Santiago")
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(tz_str)
    except Exception:
        tz = UTC
    return datetime.now(tz).date()


def build_effective_date_bounds(
    date_from: date | None, date_to: date | None
) -> tuple[date | None, date | None]:
    effective_from = date_from
    effective_to = date_to
    if effective_from and effective_to and effective_from > effective_to:
        effective_from, effective_to = effective_to, effective_from
    return effective_from, effective_to


def format_post_date_log(
    post_date_value: date | None,
    date_from: date | None,
    date_to: date | None,
) -> str:
    parts = []
    if post_date_value:
        parts.append(f"post_date={post_date_value.isoformat()}")
    if date_from:
        parts.append(f"from={date_from.isoformat()}")
    if date_to:
        parts.append(f"to={date_to.isoformat()}")
    return " ".join(parts)


def existing_post_label(payload: dict | None, shortcode: str) -> str:
    if payload is None:
        return f"no cache for #{shortcode}"
    status = payload.get("status", "unknown")
    return f"[status={status}] #{shortcode}"


def random_delay(min_sec: float, max_sec: float, label: str = "") -> float:
    delay = random.uniform(min_sec, max_sec)
    import os
    factor_str = os.environ.get("SCRAPER_DELAY_FACTOR", "1.0")
    try:
        factor = float(factor_str)
    except (ValueError, TypeError):
        factor = 1.0
    profile = os.environ.get("SCRAPER_BEHAVIOR_PROFILE", "balanced")
    if profile == "fast":
        min_sec = max(min_sec * 0.5, 0.1)
        max_sec = max(max_sec * 0.5, 0.2)
        delay = random.uniform(min_sec, max_sec) * factor * 0.5
    elif profile == "conservative":
        min_sec = min(min_sec * 3.0, 60.0)
        max_sec = min(max_sec * 3.0, 120.0)
        delay = random.uniform(min_sec, max_sec) * factor
    else:
        delay = random.uniform(min_sec, max_sec) * factor
    time.sleep(delay)
    return delay
