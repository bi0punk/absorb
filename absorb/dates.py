import re
from datetime import date, datetime


def parse_iso_date(raw_value: str | None) -> date | None:
    if not raw_value:
        return None
    try:
        return date.fromisoformat(raw_value.strip())
    except (ValueError, TypeError):
        return None


def parse_compact_date(raw_value: str | None) -> date | None:
    if not raw_value:
        return None
    val = raw_value.strip()
    if not re.match(r"^\d{6}$", val):
        return None
    try:
        day = int(val[0:2])
        month = int(val[2:4])
        year = int(val[4:6]) + 2000
        return date(year, month, day)
    except (ValueError, TypeError):
        return None


def validate_date_range(date_from: date | None, date_to: date | None) -> None:
    if date_from is not None and date_to is not None and date_from > date_to:
        raise ValueError(f"date_from ({date_from}) is after date_to ({date_to})")


def parse_post_date_from_iso(raw_value: str | None) -> date | None:
    if not raw_value:
        return None
    try:
        dt = datetime.fromisoformat(raw_value)
        return dt.date()
    except (ValueError, TypeError):
        try:
            return date.fromisoformat(raw_value.strip()[:10])
        except (ValueError, TypeError):
            return None


def match_post_date(
    post_date_value: date | None,
    date_from: date | None,
    date_to: date | None,
) -> bool:
    if post_date_value is None:
        return True
    if date_from and post_date_value < date_from:
        return False
    return not (date_to and post_date_value > date_to)


def should_stop_after_candidate(
    post_date_value: date | None, date_from: date | None
) -> bool:
    if post_date_value is None or date_from is None:
        return False
    return post_date_value < date_from


def build_mode_label(
    date_from: date | None, date_to: date | None
) -> str:
    if date_from and date_to:
        return f"from {date_from.isoformat()} to {date_to.isoformat()}"
    if date_from:
        return f"from {date_from.isoformat()}"
    if date_to:
        return f"up to {date_to.isoformat()}"
    return "all available dates"


def build_source_execution_label(
    job: dict,
    collect_all_by_date: bool,
    scheduler_all_new: bool,
) -> str:
    label = job.get("profile_url", "?")
    if scheduler_all_new:
        label += " [scheduler-all-new]"
    elif collect_all_by_date:
        label += " [collect-all-by-date]"
    return label
