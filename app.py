#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import builtins
import json
import os
import re
import sqlite3
import subprocess
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

import cv2
import pytesseract
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

BASE_DIR = Path("data_instagram")
STATE_FILE = "ig_state.json"
OCR_LANG = "spa"
TESSERACT_CONFIG = "--oem 3 --psm 6"
REGISTRY_DB = BASE_DIR / "registry.sqlite3"
ANALYSIS_SUFFIX = ".analysis.json"
DEFAULT_LIMIT = 5
MAX_LIMIT = 200
MANUAL_LOG_FILE = BASE_DIR / "manual_run.log"
IS_SCHEDULER_RUN = os.environ.get("SCRAPER_RUN_CONTEXT") == "scheduler"

try:
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except Exception:
    pass


def print(*args, **kwargs):
    kwargs.setdefault("flush", True)
    return builtins.print(*args, **kwargs)


def append_manual_log(message: str) -> None:
    if IS_SCHEDULER_RUN:
        return
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    with MANUAL_LOG_FILE.open("a", encoding="utf-8") as fh:
        fh.write(message.rstrip() + "\n")


def log(message: str) -> None:
    print(message)
    append_manual_log(message)


def reset_manual_log() -> None:
    if IS_SCHEDULER_RUN:
        return
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    MANUAL_LOG_FILE.write_text("", encoding="utf-8")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


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


def build_source_metadata(profile_url: str) -> Dict[str, str]:
    normalized_url = normalize_profile_url(profile_url) if profile_url else ""
    username = extract_source_username(normalized_url or profile_url)
    return {
        "profile_url": normalized_url or profile_url or "",
        "source_username": username,
        "source_label": f"@{username}" if username else "",
    }


def parse_positive_limit(raw_value, fallback: int = DEFAULT_LIMIT) -> int:
    try:
        value = int(str(raw_value).strip())
    except Exception:
        value = fallback
    return max(1, min(value, MAX_LIMIT))


def split_raw_source_entries(raw_values: Iterable[str]) -> List[str]:
    items: List[str] = []
    for raw in raw_values:
        if raw is None:
            continue
        parts = re.split(r"[\n,;]+", str(raw))
        for part in parts:
            token = part.strip()
            if token:
                items.append(token)
    return items


def parse_profile_sources(raw_values: Iterable[str]) -> List[str]:
    normalized: List[str] = []
    seen: Set[str] = set()

    for part in split_raw_source_entries(raw_values):
        url = normalize_profile_url(part)
        if not url or url in seen:
            continue
        seen.add(url)
        normalized.append(url)

    return normalized


def parse_source_jobs(raw_values: Iterable[str], default_limit: int = DEFAULT_LIMIT) -> List[Dict[str, int | str]]:
    jobs_by_url: Dict[str, Dict[str, int | str]] = {}
    default_limit = parse_positive_limit(default_limit, DEFAULT_LIMIT)

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
    return f"{normalize_profile_url(profile_url)}={parse_positive_limit(limit)}"


def parse_cli_sources_and_limit(argv: List[str]) -> Tuple[List[str], int]:
    if not argv:
        return [], DEFAULT_LIMIT

    raw_sources = list(argv)
    limit = DEFAULT_LIMIT

    last_token = raw_sources[-1].strip()
    if re.fullmatch(r"\d+", last_token):
        limit = int(last_token)
        raw_sources = raw_sources[:-1]

    if not raw_sources:
        return [], parse_positive_limit(limit)

    return parse_profile_sources(raw_sources), parse_positive_limit(limit)


def parse_cli_jobs(argv: List[str]) -> Tuple[List[Dict[str, int | str]], Optional[int], str]:
    if not argv:
        return [], None, "per_source"

    if any(re.search(r"(?:=|\|)\s*\d+$", token.strip()) for token in argv):
        jobs = parse_source_jobs(argv, default_limit=DEFAULT_LIMIT)
        return jobs, None, "per_source"

    profile_urls, limit = parse_cli_sources_and_limit(argv)
    jobs = [{"profile_url": url, "limit": limit} for url in profile_urls]
    return jobs, limit, "shared_total"


def parse_iso_date(raw_value: str | None) -> Optional[date]:
    value = str(raw_value or "").strip()
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"Fecha inválida: {value}. Usa formato YYYY-MM-DD.") from exc


def validate_date_range(date_from: Optional[date], date_to: Optional[date]) -> None:
    if date_from and date_to and date_from > date_to:
        raise ValueError("La fecha desde no puede ser mayor que la fecha hasta.")


def parse_post_date_from_iso(raw_value: str | None) -> Optional[date]:
    value = str(raw_value or "").strip()
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def match_post_date(post_date_value: Optional[date], date_from: Optional[date], date_to: Optional[date]) -> bool:
    if not post_date_value:
        return False
    if date_from and post_date_value < date_from:
        return False
    if date_to and post_date_value > date_to:
        return False
    return True


def should_stop_after_candidate(post_date_value: Optional[date], date_from: Optional[date]) -> bool:
    if not post_date_value:
        return False
    return bool(date_from and post_date_value < date_from)


def build_mode_label(date_from: Optional[date], date_to: Optional[date]) -> str:
    if date_from and date_to:
        return f"desde {date_from.isoformat()} hasta {date_to.isoformat()}"
    if date_from:
        return f"desde {date_from.isoformat()} a la fecha"
    if date_to:
        return f"hasta {date_to.isoformat()}"
    return "solo nuevos"


def fetch_post_datetime(context, kind: str, shortcode: str) -> Optional[str]:
    post_url = (
        f"https://www.instagram.com/{kind}/{shortcode}/"
        if kind == "reel"
        else f"https://www.instagram.com/p/{shortcode}/"
    )
    page = context.new_page()
    try:
        page.goto(post_url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_selector("time[datetime]", timeout=12000)
        iso_value = page.locator("time[datetime]").first.get_attribute("datetime")
        return (iso_value or "").strip() or None
    except Exception as exc:
        log(f"[WARN] No se pudo leer fecha para {kind}:{shortcode} -> {exc}")
        return None
    finally:
        page.close()


def build_context(browser):
    if Path(STATE_FILE).exists():
        return browser.new_context(
            storage_state=STATE_FILE,
            viewport={"width": 1400, "height": 1800},
            locale="es-CL",
            timezone_id="America/Santiago",
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
        )
    return browser.new_context(
        viewport={"width": 1400, "height": 1800},
        locale="es-CL",
        timezone_id="America/Santiago",
        user_agent=(
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
    )


def save_debug_artifacts(page, prefix="debug_instagram"):
    Path("debug").mkdir(exist_ok=True)
    html_path = Path("debug") / f"{prefix}.html"
    png_path = Path("debug") / f"{prefix}.png"

    html_path.write_text(page.content(), encoding="utf-8")
    page.screenshot(path=str(png_path), full_page=True)

    return html_path, png_path


def dismiss_cookie_banner(page):
    candidates = [
        "button:has-text('Permitir todas las cookies')",
        "button:has-text('Aceptar')",
        "button:has-text('Allow all cookies')",
        "button:has-text('Allow essential and optional cookies')",
    ]
    for selector in candidates:
        try:
            if page.locator(selector).first.is_visible(timeout=1500):
                page.locator(selector).first.click()
                time.sleep(1)
                return
        except Exception:
            pass


# ── Registro local / deduplicación ───────────────────────────────────────────

def get_registry_connection() -> sqlite3.Connection:
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(REGISTRY_DB)
    conn.row_factory = sqlite3.Row
    return conn


def init_registry() -> None:
    with get_registry_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_posts (
                shortcode TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                profile_url TEXT,
                post_url TEXT,
                post_dir TEXT,
                analysis_json_path TEXT,
                image_path TEXT,
                status TEXT NOT NULL DEFAULT 'processed',
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                processed_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_processed_posts_status
            ON processed_posts(status)
            """
        )
        conn.commit()


def expected_post_dir(shortcode: str) -> Path:
    return BASE_DIR / f"-{shortcode}"


def expected_analysis_path(shortcode: str) -> Path:
    return expected_post_dir(shortcode) / f"{shortcode}{ANALYSIS_SUFFIX}"


def read_json_file(path: Path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def upsert_registry_record(
    shortcode: str,
    kind: str,
    profile_url: str = "",
    post_url: str = "",
    post_dir: str = "",
    analysis_json_path: str = "",
    image_path: str = "",
    status: str = "processed",
    processed_at: str = "",
) -> None:
    now = utc_now_iso()
    with get_registry_connection() as conn:
        conn.execute(
            """
            INSERT INTO processed_posts (
                shortcode, kind, profile_url, post_url, post_dir,
                analysis_json_path, image_path, status,
                first_seen_at, last_seen_at, processed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(shortcode) DO UPDATE SET
                kind = excluded.kind,
                profile_url = CASE
                    WHEN excluded.profile_url != '' THEN excluded.profile_url
                    ELSE processed_posts.profile_url
                END,
                post_url = CASE
                    WHEN excluded.post_url != '' THEN excluded.post_url
                    ELSE processed_posts.post_url
                END,
                post_dir = CASE
                    WHEN excluded.post_dir != '' THEN excluded.post_dir
                    ELSE processed_posts.post_dir
                END,
                analysis_json_path = CASE
                    WHEN excluded.analysis_json_path != '' THEN excluded.analysis_json_path
                    ELSE processed_posts.analysis_json_path
                END,
                image_path = CASE
                    WHEN excluded.image_path != '' THEN excluded.image_path
                    ELSE processed_posts.image_path
                END,
                status = excluded.status,
                last_seen_at = excluded.last_seen_at,
                processed_at = CASE
                    WHEN excluded.processed_at != '' THEN excluded.processed_at
                    ELSE processed_posts.processed_at
                END
            """,
            (
                shortcode,
                kind,
                profile_url,
                post_url,
                post_dir,
                analysis_json_path,
                image_path,
                status,
                now,
                now,
                processed_at,
            ),
        )
        conn.commit()


def bootstrap_registry_from_disk() -> int:
    synced = 0
    for json_file in BASE_DIR.rglob(f"*{ANALYSIS_SUFFIX}"):
        payload = read_json_file(json_file)
        if not payload:
            continue

        shortcode = payload.get("shortcode")
        kind = payload.get("kind")
        if not shortcode or not kind:
            continue

        upsert_registry_record(
            shortcode=shortcode,
            kind=kind,
            profile_url=payload.get("profile_url", ""),
            post_url=payload.get("post_url", ""),
            post_dir=str(json_file.parent),
            analysis_json_path=str(json_file),
            image_path=payload.get("image_path", ""),
            status="processed",
            processed_at=payload.get("processed_at", ""),
        )
        synced += 1
    return synced


def load_processed_shortcodes() -> Set[str]:
    with get_registry_connection() as conn:
        rows = conn.execute(
            """
            SELECT shortcode
            FROM processed_posts
            WHERE status = 'processed'
            """
        ).fetchall()
    return {row["shortcode"] for row in rows if row["shortcode"]}


def find_cached_payload(shortcode: str) -> Optional[Dict]:
    direct_path = expected_analysis_path(shortcode)
    payload = read_json_file(direct_path)
    if payload:
        return payload

    with get_registry_connection() as conn:
        row = conn.execute(
            """
            SELECT analysis_json_path
            FROM processed_posts
            WHERE shortcode = ? AND status = 'processed'
            LIMIT 1
            """,
            (shortcode,),
        ).fetchone()

    if not row:
        return None

    analysis_path = row["analysis_json_path"]
    if not analysis_path:
        return None

    return read_json_file(Path(analysis_path))


def merge_payloads(existing: List[Dict], new_items: List[Dict]) -> List[Dict]:
    merged: Dict[str, Dict] = {}

    for item in existing:
        shortcode = item.get("shortcode")
        if shortcode:
            merged[shortcode] = item

    for item in new_items:
        shortcode = item.get("shortcode")
        if shortcode:
            merged[shortcode] = item

    def sort_key(item: Dict):
        ts = item.get("processed_at") or item.get("updated_at") or ""
        return (ts, item.get("shortcode", ""))

    return sorted(merged.values(), key=sort_key, reverse=True)


def estimate_max_scrolls(target_new_count: int) -> int:
    target = max(1, target_new_count)
    return min(80, max(12, target * 4))


def extract_shortcodes_from_profile(
    profile_url: str,
    target_new_count: int = 5,
    known_shortcodes: Optional[Set[str]] = None,
    headless: bool = True,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> List[Dict]:
    found: Dict[str, Dict] = {}
    candidates: List[Dict] = []
    blocked = known_shortcodes or set()
    max_scrolls = estimate_max_scrolls(target_new_count)
    stale_rounds = 0
    stop_due_to_date = False

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = build_context(browser)
        page = context.new_page()

        log(f"[INFO] Abriendo perfil: {profile_url}")
        if date_from or date_to:
            log(f"[INFO] Filtro temporal activo: {build_mode_label(date_from, date_to)}")
        page.goto(profile_url, wait_until="domcontentloaded", timeout=60000)
        time.sleep(3)

        dismiss_cookie_banner(page)

        try:
            page.wait_for_selector('a[href*="/p/"], a[href*="/reel/"]', timeout=12000)
        except PlaywrightTimeoutError:
            html_path, png_path = save_debug_artifacts(page, "sin_posts_visibles")
            log("[WARN] No aparecieron links /p/ o /reel/ en el timeout inicial.")
            log(f"[WARN] HTML guardado en: {html_path}")
            log(f"[WARN] Screenshot guardado en: {png_path}")

        for scroll_idx in range(max_scrolls):
            prev_found = len(found)
            prev_candidates = len(candidates)
            added_this_scroll = 0

            hrefs = page.locator('a[href*="/p/"], a[href*="/reel/"]').evaluate_all(
                "(els) => els.map(e => e.getAttribute('href')).filter(Boolean)"
            )

            for href in hrefs:
                match = re.match(
                    r"^(?:https?://(?:www\.)?instagram\.com)?/"
                    r"(?:[^/]+/)?"
                    r"(p|reel)/([A-Za-z0-9_-]+)/?",
                    href,
                )
                if not match:
                    continue

                kind = match.group(1)
                shortcode = match.group(2)
                if shortcode in found:
                    continue

                item = {
                    "kind": kind,
                    "shortcode": shortcode,
                    "href": href,
                    "post_datetime": "",
                    "post_date": "",
                }
                found[shortcode] = item

                if shortcode in blocked:
                    log(f"[SKIP] post saltado existe -> {kind}:{shortcode}")
                    continue

                if date_from or date_to:
                    post_datetime = fetch_post_datetime(context, kind, shortcode)
                    post_date_value = parse_post_date_from_iso(post_datetime)
                    item["post_datetime"] = post_datetime or ""
                    item["post_date"] = post_date_value.isoformat() if post_date_value else ""

                    if should_stop_after_candidate(post_date_value, date_from):
                        stop_due_to_date = True
                        log(
                            f"[INFO] Se alcanzó el límite inferior de fecha con {kind}:{shortcode} "
                            f"({item['post_date'] or 'sin fecha'})."
                        )
                        continue

                    if not match_post_date(post_date_value, date_from, date_to):
                        log(
                            f"[INFO] Post fuera de rango -> {kind}:{shortcode} "
                            f"fecha={item['post_date'] or 'desconocida'}"
                        )
                        continue

                candidates.append(item)
                added_this_scroll += 1
                log(
                    f"[INFO] Candidato nuevo agregado -> {kind}:{shortcode}"
                    + (f" fecha={item['post_date']}" if item.get("post_date") else "")
                )

                if len(candidates) >= target_new_count:
                    break

            log(
                "[INFO] "
                f"Scroll {scroll_idx + 1}/{max_scrolls} -> detectados: {len(found)}, "
                f"nuevos candidatos válidos: {len(candidates)}/{target_new_count}, "
                f"agregados en este scroll: {added_this_scroll}"
            )

            if len(candidates) >= target_new_count or stop_due_to_date:
                break

            if len(found) == prev_found and len(candidates) == prev_candidates:
                stale_rounds += 1
            else:
                stale_rounds = 0

            if stale_rounds >= 5:
                log("[WARN] No aparecieron más posts nuevos tras varios scrolls. Se corta exploración.")
                break

            page.mouse.wheel(0, 3500)
            time.sleep(2)

        if not found:
            html_path, png_path = save_debug_artifacts(page, "resultado_cero")
            log(f"[WARN] Sin resultados. Revisa: {html_path}")
            log(f"[WARN] Sin resultados. Revisa: {png_path}")

        context.close()
        browser.close()

    return candidates[:target_new_count]


def run_instaloader_for_shortcode(kind: str, shortcode: str) -> Path:
    target = f"-{shortcode}"
    outdir = BASE_DIR / target
    outdir.mkdir(parents=True, exist_ok=True)

    cmd = [
        "instaloader",
        "--no-videos",
        "--no-video-thumbnails",
        "--dirname-pattern", "data_instagram/{target}",
        "--filename-pattern", "{date_utc}_UTC",
        "--",
        target,
    ]

    log(f"[INFO] Descargando {kind}:{shortcode} con Instaloader...")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        raise RuntimeError(
            f"Instaloader falló para {shortcode}\nSTDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}"
        )

    if result.stdout.strip():
        log(result.stdout.strip())
    if result.stderr.strip():
        log("[WARN] STDERR Instaloader:")
        log(result.stderr.strip())

    return outdir


def find_latest_image(post_dir: Path) -> Optional[Path]:
    candidates = []
    for pattern in ("*.jpg", "*.jpeg", "*.png"):
        candidates.extend(post_dir.glob(pattern))

    if not candidates:
        return None

    return sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)[0]


def read_caption_for_image(image_path: Path) -> str:
    txt_path = image_path.with_suffix(".txt")
    if txt_path.exists():
        return txt_path.read_text(encoding="utf-8", errors="ignore").strip()
    return ""


def preprocess_image(image_path: str):
    img = cv2.imread(image_path)
    if img is None:
        raise FileNotFoundError(f"No se pudo abrir la imagen: {image_path}")

    img = cv2.resize(img, None, fx=2.0, fy=2.0, interpolation=cv2.INTER_CUBIC)
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    gray = cv2.GaussianBlur(gray, (3, 3), 0)

    proc = cv2.adaptiveThreshold(
        gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 31, 11
    )
    return img, proc


def clean_text(text: str) -> str:
    text = text.replace("\x0c", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def run_ocr(image_path: Path) -> Dict:
    original, processed = preprocess_image(str(image_path))

    text_original = pytesseract.image_to_string(original, lang=OCR_LANG, config=TESSERACT_CONFIG)
    text_processed = pytesseract.image_to_string(processed, lang=OCR_LANG, config=TESSERACT_CONFIG)

    text_original = clean_text(text_original)
    text_processed = clean_text(text_processed)
    best = text_processed if len(text_processed) >= len(text_original) else text_original

    preproc_path = image_path.with_name(image_path.stem + "_preproc.jpg")
    cv2.imwrite(str(preproc_path), processed)

    return {
        "ocr_original": text_original,
        "ocr_processed": text_processed,
        "ocr_best": best,
        "preprocessed_image": str(preproc_path),
    }


def process_shortcode(
    kind: str,
    shortcode: str,
    profile_url: str = "",
    post_datetime: str = "",
    post_date: str = "",
) -> Dict:
    post_dir = run_instaloader_for_shortcode(kind, shortcode)

    image_path = find_latest_image(post_dir)
    if not image_path:
        raise FileNotFoundError(f"No encontré imagen descargada en {post_dir}")

    caption = read_caption_for_image(image_path)
    ocr = run_ocr(image_path)
    processed_at = utc_now_iso()
    source_meta = build_source_metadata(profile_url)

    payload = {
        "kind": kind,
        "shortcode": shortcode,
        "profile_url": source_meta["profile_url"],
        "source_username": source_meta["source_username"],
        "source_label": source_meta["source_label"],
        "processed_at": processed_at,
        "post_datetime": post_datetime,
        "post_date": post_date,
        "post_url": (
            f"https://www.instagram.com/{kind}/{shortcode}/"
            if kind == "reel"
            else f"https://www.instagram.com/p/{shortcode}/"
        ),
        "image_path": str(image_path),
        "caption": caption,
        "ocr_best": ocr["ocr_best"],
        "merged_text": "\n\n".join([x for x in [caption, ocr["ocr_best"]] if x.strip()]).strip(),
        "preprocessed_image": ocr["preprocessed_image"],
    }

    out_json = post_dir / f"{shortcode}{ANALYSIS_SUFFIX}"
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    upsert_registry_record(
        shortcode=shortcode,
        kind=kind,
        profile_url=source_meta["profile_url"],
        post_url=payload["post_url"],
        post_dir=str(post_dir),
        analysis_json_path=str(out_json),
        image_path=str(image_path),
        status="processed",
        processed_at=processed_at,
    )
    return payload


def process_source(
    profile_url: str,
    target_new_count: int,
    blocked_shortcodes: Set[str],
    results: List[Dict],
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> Dict[str, int]:
    target_new_count = parse_positive_limit(target_new_count)
    stats = {"processed": 0, "skipped": 0, "failed": 0}

    posts = extract_shortcodes_from_profile(
        profile_url,
        target_new_count=target_new_count,
        known_shortcodes=blocked_shortcodes,
        headless=True,
        date_from=date_from,
        date_to=date_to,
    )
    if not posts:
        log(f"[WARN] No encontré posts candidatos válidos en {profile_url}")
        return stats

    detected = [
        f"{p['kind']}:{p['shortcode']}" + (f"@{p.get('post_date')}" if p.get('post_date') else "")
        for p in posts
    ]
    log(f"[INFO] Candidatos detectados en la fuente: {detected}")

    for item in posts:
        if stats["processed"] >= target_new_count:
            break

        shortcode = item["shortcode"]
        kind = item["kind"]

        if shortcode in blocked_shortcodes:
            stats["skipped"] += 1
            log(f"[SKIP] post saltado existe -> {kind}:{shortcode}")
            continue

        cached_payload = find_cached_payload(shortcode)
        if cached_payload:
            blocked_shortcodes.add(shortcode)
            upsert_registry_record(
                shortcode=shortcode,
                kind=kind,
                profile_url=profile_url,
                post_url=cached_payload.get("post_url", ""),
                post_dir=str(expected_post_dir(shortcode)),
                analysis_json_path=str(expected_analysis_path(shortcode)),
                image_path=cached_payload.get("image_path", ""),
                status="processed",
                processed_at=cached_payload.get("processed_at", ""),
            )
            stats["skipped"] += 1
            log(f"[SKIP] post saltado existe -> {kind}:{shortcode}")
            continue

        try:
            payload = process_shortcode(
                kind,
                shortcode,
                profile_url=profile_url,
                post_datetime=item.get("post_datetime", ""),
                post_date=item.get("post_date", ""),
            )
            blocked_shortcodes.add(shortcode)
            results.append(payload)
            stats["processed"] += 1
            log(
                f"[OK] Procesado {kind}:{shortcode} -> progreso fuente "
                f"{stats['processed']}/{target_new_count}"
            )
        except Exception as exc:
            blocked_shortcodes.add(shortcode)
            stats["failed"] += 1
            log(f"[ERROR] Falló {kind}:{shortcode} -> {exc}")

    if stats["processed"] < target_new_count:
        log(
            f"[WARN] La fuente {profile_url} no alcanzó la meta. "
            f"Solicitados: {target_new_count}, nuevos obtenidos: {stats['processed']}."
        )

    return stats


def run_scrape_jobs(
    source_jobs: List[Dict[str, int | str]],
    shared_total_limit: Optional[int] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> Dict[str, int]:
    if not source_jobs:
        raise ValueError("No se recibieron fuentes para procesar.")

    BASE_DIR.mkdir(parents=True, exist_ok=True)
    validate_date_range(date_from, date_to)
    reset_manual_log()
    run_prefix = "programada" if IS_SCHEDULER_RUN else "manual"
    log(f"[INFO] Inicio de ejecución {run_prefix}. Modo temporal: {build_mode_label(date_from, date_to)}")
    init_registry()
    synced = bootstrap_registry_from_disk()
    if synced:
        log(f"[INFO] Registro local sincronizado con {synced} análisis existentes.")

    blocked_shortcodes = set(load_processed_shortcodes())
    results: List[Dict] = []
    total_processed = 0
    total_skipped = 0
    total_failed = 0

    if shared_total_limit is not None:
        shared_total_limit = parse_positive_limit(shared_total_limit)
        profile_urls = [str(job["profile_url"]) for job in source_jobs]
        log(f"[INFO] Fuentes normalizadas: {profile_urls}")
        log(f"[INFO] Objetivo global de nuevos posts: {shared_total_limit}")

        for idx, profile_url in enumerate(profile_urls, start=1):
            if total_processed >= shared_total_limit:
                break

            remaining = shared_total_limit - total_processed
            log(f"[INFO] Fuente {idx}/{len(profile_urls)} -> {profile_url}")
            log(f"[INFO] Faltan {remaining} posts nuevos por procesar en modo global.")
            source_stats = process_source(profile_url, remaining, blocked_shortcodes, results, date_from=date_from, date_to=date_to)
            total_processed += source_stats["processed"]
            total_skipped += source_stats["skipped"]
            total_failed += source_stats["failed"]
    else:
        log("[INFO] Ejecutando en modo por fuente.")
        for idx, job in enumerate(source_jobs, start=1):
            profile_url = str(job["profile_url"])
            source_limit = parse_positive_limit(job["limit"])
            log(f"[INFO] Fuente {idx}/{len(source_jobs)} -> {profile_url}")
            log(f"[INFO] Objetivo para esta fuente: {source_limit} posts nuevos.")
            source_stats = process_source(profile_url, source_limit, blocked_shortcodes, results, date_from=date_from, date_to=date_to)
            total_processed += source_stats["processed"]
            total_skipped += source_stats["skipped"]
            total_failed += source_stats["failed"]

    summary_path = BASE_DIR / "summary_latest_posts.json"
    existing_summary = read_json_file(summary_path) or []
    merged_summary = merge_payloads(existing_summary, results)
    summary_path.write_text(json.dumps(merged_summary, ensure_ascii=False, indent=2), encoding="utf-8")

    log(
        "[INFO] Resultado del lote -> "
        f"nuevos: {total_processed}, "
        f"reutilizados/omitidos: {total_skipped}, "
        f"fallidos: {total_failed}, "
        f"total resumen: {len(merged_summary)}"
    )

    if shared_total_limit is not None and total_processed < shared_total_limit:
        log(
            f"[WARN] No se alcanzó el objetivo global completo. "
            f"Solicitados: {shared_total_limit}, obtenidos: {total_processed}."
        )
        log("[WARN] Causas típicas: pocos posts nuevos visibles, perfil muy corto o fallas de descarga.")

    log(f"[OK] Resumen guardado en {summary_path}")

    return {
        "processed": total_processed,
        "skipped": total_skipped,
        "failed": total_failed,
        "summary_total": len(merged_summary),
    }


def parse_cli_options(argv: List[str]) -> Tuple[List[str], Optional[date], Optional[date]]:
    remaining: List[str] = []
    date_from: Optional[date] = None
    date_to: Optional[date] = None
    idx = 0

    while idx < len(argv):
        token = argv[idx]
        if token == "--date-from":
            if idx + 1 >= len(argv):
                raise ValueError("Falta valor para --date-from")
            date_from = parse_iso_date(argv[idx + 1])
            idx += 2
            continue
        if token == "--date-to":
            if idx + 1 >= len(argv):
                raise ValueError("Falta valor para --date-to")
            date_to = parse_iso_date(argv[idx + 1])
            idx += 2
            continue
        remaining.append(token)
        idx += 1

    validate_date_range(date_from, date_to)
    return remaining, date_from, date_to


def main() -> None:
    try:
        argv, date_from, date_to = parse_cli_options(sys.argv[1:])
    except ValueError as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)

    source_jobs, shared_total_limit, mode = parse_cli_jobs(argv)
    if not source_jobs:
        print("Uso global: python app.py https://www.instagram.com/biobiochile/ https://www.instagram.com/cnnchile/ 20")
        print("Uso por fuente: python app.py @biobiochile=30 @cnnchile=50 @latercera=20")
        print("Opciones fecha: --date-from YYYY-MM-DD --date-to YYYY-MM-DD")
        sys.exit(1)

    print(f"[INFO] Modo detectado: {mode}")
    if date_from or date_to:
        print(f"[INFO] Filtro temporal CLI: {build_mode_label(date_from, date_to)}")
    run_scrape_jobs(
        source_jobs,
        shared_total_limit=shared_total_limit,
        date_from=date_from,
        date_to=date_to,
    )


if __name__ == "__main__":
    main()
