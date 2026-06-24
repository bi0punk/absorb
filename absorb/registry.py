import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from . import constants


def get_registry_connection() -> sqlite3.Connection:
    constants.REGISTRY_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(constants.REGISTRY_DB))
    conn.row_factory = sqlite3.Row
    return conn


def init_registry() -> None:
    conn = get_registry_connection()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS processed_posts (
            shortcode TEXT PRIMARY KEY,
            kind TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'processed',
            profile_url TEXT NOT NULL,
            source_username TEXT NOT NULL,
            source_label TEXT NOT NULL,
            post_dir TEXT NOT NULL,
            analysis_path TEXT NOT NULL,
            post_iso TEXT NOT NULL DEFAULT '',
            original_source TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_processed_status ON processed_posts(status);
    """)
    conn.commit()
    conn.close()


def find_post_dir_in_registry(shortcode: str) -> Optional[Path]:
    conn = get_registry_connection()
    row = conn.execute(
        "SELECT post_dir FROM processed_posts WHERE shortcode = ?", (shortcode,)
    ).fetchone()
    conn.close()
    return Path(row["post_dir"]) if row else None


def find_post_dir_on_disk(shortcode: str) -> Optional[Path]:
    for p in constants.BASE_DIR.rglob(f"*{shortcode}"):
        if p.is_dir():
            return p
    return None


def locate_post_dir(shortcode: str) -> Optional[Path]:
    return find_post_dir_in_registry(shortcode) or find_post_dir_on_disk(shortcode)


def expected_post_dir(shortcode: str, profile_url: str) -> Path:
    from .sources import extract_source_username, sanitize_source_dirname
    username = extract_source_username(profile_url)
    return constants.BASE_DIR / sanitize_source_dirname(username) / f"-{shortcode}"


def expected_analysis_path(shortcode: str, profile_url: str) -> Path:
    return expected_post_dir(shortcode, profile_url) / f"{shortcode}{constants.ANALYSIS_SUFFIX}"


def read_json_file(path: Path) -> Any:
    if not path.exists():
        return None
    try:
        with open(str(path), "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def infer_payload_status(payload: Optional[Dict]) -> str:
    if payload is None:
        return ""
    status = payload.get("status", "")
    return status if status in ("processed", "downloaded") else ""


def find_analysis_path(
    shortcode: str, statuses: Optional[Iterable[str]] = None
) -> Optional[Path]:
    for p in constants.BASE_DIR.rglob(f"{shortcode}{constants.ANALYSIS_SUFFIX}"):
        if p.is_file():
            return p
    return None


def upsert_registry_record(
    shortcode: str,
    kind: str,
    profile_url: str,
    source_username: str,
    source_label: str,
    post_dir: str,
    analysis_path: str,
    status: str = "processed",
    post_iso: str = "",
    original_source: str = "",
) -> None:
    conn = get_registry_connection()
    conn.execute(
        """INSERT OR REPLACE INTO processed_posts
           (shortcode, kind, status, profile_url, source_username, source_label,
            post_dir, analysis_path, post_iso, original_source)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (shortcode, kind, status, profile_url, source_username, source_label,
         post_dir, analysis_path, post_iso, original_source),
    )
    conn.commit()
    conn.close()


def bootstrap_registry_from_disk() -> int:
    count = 0
    for analysis_file in constants.BASE_DIR.rglob(f"*{constants.ANALYSIS_SUFFIX}"):
        if not analysis_file.is_file():
            continue
        payload = read_json_file(analysis_file)
        if not payload:
            continue
        shortcode = payload.get("shortcode", "")
        if not shortcode:
            continue
        post_dir = str(analysis_file.parent)
        kind = payload.get("kind", "post")
        status = payload.get("status", "processed")
        from .sources import extract_source_username
        profile_url = payload.get("profile_url", "")
        source_username = payload.get("source_username", extract_source_username(profile_url))
        source_label = payload.get("source_label", f"@{source_username}")
        post_iso = payload.get("post_iso", "")
        upsert_registry_record(
            shortcode, kind, profile_url, source_username, source_label,
            post_dir, str(analysis_file), status, post_iso,
        )
        count += 1
    return count


def load_processed_shortcodes() -> Set[str]:
    conn = get_registry_connection()
    rows = conn.execute(
        "SELECT shortcode FROM processed_posts WHERE status = 'processed'"
    ).fetchall()
    conn.close()
    return {r["shortcode"] for r in rows}


def find_payload(shortcode: str, statuses: Optional[Iterable[str]] = None) -> Optional[Dict]:
    analysis_path = find_analysis_path(shortcode, statuses)
    if not analysis_path:
        return None
    return read_json_file(analysis_path)


def find_cached_payload(shortcode: str) -> Optional[Dict]:
    return find_payload(shortcode, ["processed"])


def find_downloaded_payload(shortcode: str) -> Optional[Dict]:
    return find_payload(shortcode, ["downloaded"])
