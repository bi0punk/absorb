#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import builtins
import json
import os
import random
import re
import sqlite3
import subprocess
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Set, Tuple

import cv2
import pytesseract
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# ── Directorios y archivos ────────────────────────────────────────────────────
BASE_DIR = Path("data_instagram")
STATE_FILE = "ig_state.json"
OCR_LANG = "spa"
TESSERACT_CONFIG = "--oem 3 --psm 6"
REGISTRY_DB = BASE_DIR / "registry.sqlite3"
ANALYSIS_SUFFIX = ".analysis.json"
DEFAULT_LIMIT = 5
MAX_LIMIT = 200
MANUAL_LOG_FILE = BASE_DIR / "manual_run.log"
SOURCE_STATE_FILE = BASE_DIR / "source_state.json"
IS_SCHEDULER_RUN = os.environ.get("SCRAPER_RUN_CONTEXT") == "scheduler"

# ── Visión en tiempo real del browser ─────────────────────────────────────────
# SCRAPER_HEADLESS=1  → browser invisible (scheduler / CI)
# Sin la variable    → browser VISIBLE para depuración manual
_HEADLESS_RAW = os.environ.get("SCRAPER_HEADLESS", "").strip()
BROWSER_HEADLESS: bool = _HEADLESS_RAW in ("1", "true", "yes")

_LIVE_SCREENSHOT_RAW = os.environ.get("SCRAPER_SCREENSHOT_DIR", "").strip()
LIVE_SCREENSHOT_DIR: Optional[Path] = Path(_LIVE_SCREENSHOT_RAW).expanduser() if _LIVE_SCREENSHOT_RAW else None

# ── Tiempos de espera (segundos) ──────────────────────────────────────────────
# Entre fuentes de scraping
DELAY_BETWEEN_SOURCES_MIN = 8
DELAY_BETWEEN_SOURCES_MAX = 18

# Entre procesamiento de cada post (descarga + OCR)
DELAY_BETWEEN_POSTS_MIN = 4
DELAY_BETWEEN_POSTS_MAX = 10

# Tras cada scroll, espera base antes de leer nuevos hrefs
DELAY_AFTER_SCROLL_MIN = 2.5
DELAY_AFTER_SCROLL_MAX = 5.0

# Tras abrir el perfil (carga inicial)
DELAY_PROFILE_LOAD_MIN = 3.0
DELAY_PROFILE_LOAD_MAX = 6.0

# Espera máxima para que aparezcan posts nuevos tras un scroll (seg)
DELAY_SCROLL_CONTENT_TIMEOUT = 8.0

# Tras aceptar cookie banner
DELAY_AFTER_COOKIE = 1.5

# Tras abrir Chromium y crear la pestaña
DELAY_BROWSER_BOOT_MIN = 1.8
DELAY_BROWSER_BOOT_MAX = 3.8
DELAY_AFTER_NEW_PAGE_MIN = 0.8
DELAY_AFTER_NEW_PAGE_MAX = 1.8

# Tras cerrar un modal/banner o estabilizar la vista
DELAY_AFTER_MODAL_CLOSE = 1.2
DELAY_PROFILE_SETTLE_MIN = 1.0
DELAY_PROFILE_SETTLE_MAX = 2.5
OVERLAY_DISMISS_PASSES = 3

# Número de reintentos para process_shortcode
MAX_RETRIES_PER_POST = 2
RETRY_WAIT_MIN = 5
RETRY_WAIT_MAX = 12

try:
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except Exception:
    pass


def print(*args, **kwargs):
    kwargs.setdefault("flush", True)
    return builtins.print(*args, **kwargs)


# ── Logging ───────────────────────────────────────────────────────────────────

def _ts() -> str:
    """Timestamp compacto UTC para logs."""
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def append_manual_log(message: str) -> None:
    if IS_SCHEDULER_RUN:
        return
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    with MANUAL_LOG_FILE.open("a", encoding="utf-8") as fh:
        fh.write(message.rstrip() + "\n")


def log(message: str) -> None:
    print(message)
    append_manual_log(message)


def log_section(title: str) -> None:
    """Separador visual para marcar inicio de sección importante."""
    line = "─" * 70
    log(f"\n{line}")
    log(f"  {title}")
    log(line)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def save_live_screenshot(page, label: str = "", scroll_idx: int = 0, extra: str = "") -> None:
    """
    Guarda un screenshot en LIVE_SCREENSHOT_DIR junto a un status.json.
    Solo actúa si la variable de entorno SCRAPER_SCREENSHOT_DIR está configurada.
    Silencia cualquier excepción para no afectar el flujo principal.
    """
    if not LIVE_SCREENSHOT_DIR or not str(LIVE_SCREENSHOT_DIR):
        return
    try:
        LIVE_SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
        screenshot_path = LIVE_SCREENSHOT_DIR / "current.png"
        # Captura solo el viewport (más rápido que full_page=True)
        page.screenshot(path=str(screenshot_path), full_page=False, timeout=8000)
        status = {
            "label": label,
            "extra": extra,
            "scroll_idx": scroll_idx,
            "timestamp": utc_now_iso(),
            "screenshot_file": "current.png",
        }
        (LIVE_SCREENSHOT_DIR / "status.json").write_text(
            json.dumps(status, ensure_ascii=False), encoding="utf-8"
        )
    except Exception:
        pass  # Nunca interrumpir el scraping por fallos de screenshot


def reset_manual_log() -> None:
    if IS_SCHEDULER_RUN:
        return
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    MANUAL_LOG_FILE.write_text("", encoding="utf-8")


# ── Utilidades de tiempo ──────────────────────────────────────────────────────

def random_delay(min_sec: float, max_sec: float, label: str = "") -> float:
    """Espera un tiempo aleatorio dentro del rango dado y logea el motivo."""
    wait = round(random.uniform(min_sec, max_sec), 2)
    if label:
        log(f"[WAIT] {_ts()} ⏳ {label} ({wait}s)")
    time.sleep(wait)
    return wait


# ── Normalización de URLs y fuentes ──────────────────────────────────────────

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


def sanitize_source_dirname(raw_value: str) -> str:
    value = re.sub(r"[^a-zA-Z0-9._-]+", "_", str(raw_value or "").strip().lower())
    value = value.strip("._-")
    return value or "sin_fuente"


def get_source_storage_dir(profile_url: str = "") -> Path:
    meta = build_source_metadata(profile_url)
    dirname = sanitize_source_dirname(meta.get("source_username", "") or "sin_fuente")
    return BASE_DIR / dirname


def parse_positive_limit(raw_value, fallback: int = DEFAULT_LIMIT) -> int:
    try:
        value = int(str(raw_value).strip())
    except Exception:
        value = fallback
    return max(1, min(value, MAX_LIMIT))


def parse_content_mode(raw_value: str | None, fallback: str = "both") -> str:
    value = str(raw_value or fallback).strip().lower()
    aliases = {
        "both": "both",
        "all": "both",
        "ambos": "both",
        "post": "post",
        "posts": "post",
        "image": "post",
        "images": "post",
        "p": "post",
        "reel": "reel",
        "reels": "reel",
        "r": "reel",
    }
    normalized = aliases.get(value)
    if not normalized:
        raise ValueError("Modo de contenido inválido. Usa: both, post o reel.")
    return normalized


def build_content_mode_label(content_mode: str) -> str:
    normalized = parse_content_mode(content_mode)
    return {
        "both": "posts y reels",
        "post": "solo posts",
        "reel": "solo reels",
    }[normalized]


def get_profile_link_selector(content_mode: str) -> str:
    normalized = parse_content_mode(content_mode)
    if normalized == "post":
        return 'a[href*="/p/"]'
    if normalized == "reel":
        return 'a[href*="/reel/"]'
    return 'a[href*="/p/"], a[href*="/reel/"]'


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


def parse_compact_date(raw_value: str | None) -> Optional[date]:
    value = str(raw_value or "").strip()
    if not value:
        return None
    try:
        return datetime.strptime(value, "%d%m%y").date()
    except ValueError as exc:
        raise ValueError(f"Fecha inválida: {value}. Usa formato ddmmaa, por ejemplo 010326.") from exc


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
        return f"entre {date_from.isoformat()} y {date_to.isoformat()}"
    if date_from:
        return f"desde hoy hasta {date_from.isoformat()}"
    if date_to:
        return f"hasta {date_to.isoformat()}"
    return "solo nuevos"


def build_source_execution_label(job: Dict[str, int | str], collect_all_by_date: bool = False, scheduler_all_new: bool = False) -> str:
    source_meta = build_source_metadata(str(job.get("profile_url", "")))
    source_label = source_meta.get("source_label") or str(job.get("profile_url", ""))
    if collect_all_by_date:
        return f"{source_label} (sin límite por cantidad; corte por fecha)"
    if scheduler_all_new:
        return f"{source_label} (solo nuevos, sin cuota fija)"
    return f"{source_label} (límite={job.get('limit', '?')})"


# ── Fechas de posts individuales ──────────────────────────────────────────────

def fetch_post_datetime(context, kind: str, shortcode: str) -> Optional[str]:
    post_url = (
        f"https://www.instagram.com/{kind}/{shortcode}/"
        if kind == "reel"
        else f"https://www.instagram.com/p/{shortcode}/"
    )
    log(f"[FETCH] {_ts()} 🕐 Consultando fecha de post {kind}:{shortcode} → {post_url}")
    page = context.new_page()
    try:
        page.goto(post_url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_selector("time[datetime]", timeout=12000)
        iso_value = page.locator("time[datetime]").first.get_attribute("datetime")
        result = (iso_value or "").strip() or None
        if result:
            log(f"[FETCH] {_ts()} ✓ Fecha obtenida {kind}:{shortcode} → {result}")
        else:
            log(f"[FETCH] {_ts()} ⚠ Sin fecha encontrada para {kind}:{shortcode}")
        return result
    except Exception as exc:
        log(f"[WARN] {_ts()} No se pudo leer fecha para {kind}:{shortcode} → {exc}")
        return None
    finally:
        page.close()


# ── Navegador / contexto ──────────────────────────────────────────────────────

def build_context(browser):
    log(f"[BROWSER] {_ts()} Creando contexto de navegador (state={'cargado' if Path(STATE_FILE).exists() else 'sin sesión'})")
    kwargs = dict(
        viewport={"width": 1400, "height": 1800},
        locale="es-CL",
        timezone_id="America/Santiago",
        user_agent=(
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
    )
    if Path(STATE_FILE).exists():
        return browser.new_context(storage_state=STATE_FILE, **kwargs)
    return browser.new_context(**kwargs)


def save_debug_artifacts(page, prefix="debug_instagram"):
    Path("debug").mkdir(exist_ok=True)
    html_path = Path("debug") / f"{prefix}.html"
    png_path = Path("debug") / f"{prefix}.png"

    html_path.write_text(page.content(), encoding="utf-8")
    page.screenshot(path=str(png_path), full_page=True)

    return html_path, png_path


def dismiss_cookie_banner(page) -> bool:
    candidates = [
        "button:has-text('Permitir todas las cookies')",
        "button:has-text('Aceptar')",
        "button:has-text('Allow all cookies')",
        "button:has-text('Allow essential and optional cookies')",
    ]
    for selector in candidates:
        try:
            locator = page.locator(selector).first
            if locator.count() and locator.is_visible(timeout=1500):
                log(f"[BROWSER] {_ts()} 🍪 Banner de cookies detectado, aceptando…")
                locator.click(timeout=4000)
                time.sleep(DELAY_AFTER_COOKIE)
                log(f"[BROWSER] {_ts()} ✓ Cookie banner aceptado.")
                return True
        except Exception:
            pass
    return False


def _click_first_visible_selector(page, selectors: List[str], description: str, wait_after: float = DELAY_AFTER_MODAL_CLOSE) -> bool:
    """Hace click en el primer selector visible. Devuelve True si actuó."""
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            if locator.count() and locator.is_visible(timeout=1200):
                log(f"[BROWSER] {_ts()} 🪟 {description} → {selector}")
                locator.click(timeout=4000)
                time.sleep(wait_after)
                return True
        except Exception:
            pass
    return False


def dismiss_transient_overlays(page, source_label: str = "", passes: int = OVERLAY_DISMISS_PASSES) -> int:
    """
    Intenta cerrar banners/modales de Instagram que a veces tapan el perfil:
    cookies, "Ahora no", login popups y botones X/Cerrar.
    """
    total_actions = 0
    source_suffix = f" en {source_label}" if source_label else ""

    not_now_selectors = [
        "[role='dialog'] button:has-text('Ahora no')",
        "[role='dialog'] button:has-text('Ahora no, gracias')",
        "[role='dialog'] button:has-text('No ahora')",
        "[role='dialog'] button:has-text('Not Now')",
        "button:has-text('Ahora no')",
        "button:has-text('Not Now')",
    ]
    close_x_selectors = [
        "[role='dialog'] button[aria-label='Cerrar']",
        "[role='dialog'] button[aria-label='Close']",
        "[role='dialog'] button:has(svg[aria-label='Cerrar'])",
        "[role='dialog'] button:has(svg[aria-label='Close'])",
        "button[aria-label='Cerrar']",
        "button[aria-label='Close']",
        "button:has(svg[aria-label='Cerrar'])",
        "button:has(svg[aria-label='Close'])",
    ]

    for attempt in range(1, max(1, passes) + 1):
        acted = False

        if dismiss_cookie_banner(page):
            total_actions += 1
            acted = True

        if _click_first_visible_selector(page, not_now_selectors, f"Modal cerrado con 'Ahora no'{source_suffix}"):
            total_actions += 1
            acted = True

        if _click_first_visible_selector(page, close_x_selectors, f"Modal/banner cerrado con X{source_suffix}"):
            total_actions += 1
            acted = True

        if not acted:
            try:
                if page.locator("[role='dialog']").count():
                    page.keyboard.press("Escape")
                    time.sleep(DELAY_AFTER_MODAL_CLOSE)
                    log(f"[BROWSER] {_ts()} ⌨ Overlay descartado con Escape{source_suffix}.")
                    total_actions += 1
                    acted = True
            except Exception:
                pass

        if not acted:
            break

        if attempt < passes:
            random_delay(DELAY_PROFILE_SETTLE_MIN, DELAY_PROFILE_SETTLE_MAX,
                         f"Asentando UI tras cerrar banners{source_suffix}")

    return total_actions


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


def find_post_dir_in_registry(shortcode: str) -> Optional[Path]:
    try:
        with get_registry_connection() as conn:
            row = conn.execute(
                """
                SELECT post_dir
                FROM processed_posts
                WHERE shortcode = ? AND post_dir != ''
                ORDER BY
                    CASE status
                        WHEN 'processed' THEN 0
                        WHEN 'downloaded' THEN 1
                        ELSE 2
                    END,
                    last_seen_at DESC
                LIMIT 1
                """,
                (shortcode,),
            ).fetchone()
    except sqlite3.OperationalError:
        return None

    if not row:
        return None

    raw_path = str(row["post_dir"] or "").strip()
    if not raw_path:
        return None

    path = Path(raw_path)
    return path if path.exists() else None


def find_post_dir_on_disk(shortcode: str) -> Optional[Path]:
    for candidate in BASE_DIR.rglob(f"-{shortcode}"):
        if candidate.is_dir():
            return candidate
    return None


def locate_post_dir(shortcode: str) -> Optional[Path]:
    return find_post_dir_in_registry(shortcode) or find_post_dir_on_disk(shortcode)


def expected_post_dir(shortcode: str, profile_url: str = "") -> Path:
    if profile_url:
        return get_source_storage_dir(profile_url) / f"-{shortcode}"

    located = locate_post_dir(shortcode)
    if located:
        return located

    return BASE_DIR / f"-{shortcode}"


def expected_analysis_path(shortcode: str, profile_url: str = "") -> Path:
    return expected_post_dir(shortcode, profile_url=profile_url) / f"{shortcode}{ANALYSIS_SUFFIX}"


def read_json_file(path: Path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def find_analysis_path(shortcode: str, statuses: Optional[Iterable[str]] = None) -> Optional[Path]:
    registry_row = None
    try:
        with get_registry_connection() as conn:
            if statuses:
                placeholders = ",".join("?" for _ in statuses)
                registry_row = conn.execute(
                    f"""
                    SELECT analysis_json_path
                    FROM processed_posts
                    WHERE shortcode = ? AND status IN ({placeholders})
                    ORDER BY last_seen_at DESC
                    LIMIT 1
                    """,
                    (shortcode, *tuple(statuses)),
                ).fetchone()
            else:
                registry_row = conn.execute(
                    """
                    SELECT analysis_json_path
                    FROM processed_posts
                    WHERE shortcode = ?
                    ORDER BY
                        CASE status
                            WHEN 'processed' THEN 0
                            WHEN 'downloaded' THEN 1
                            ELSE 2
                        END,
                        last_seen_at DESC
                    LIMIT 1
                    """,
                    (shortcode,),
                ).fetchone()
    except sqlite3.OperationalError:
        registry_row = None

    if registry_row:
        analysis_json_path = str(registry_row["analysis_json_path"] or "").strip()
        if analysis_json_path:
            path = Path(analysis_json_path)
            if path.exists():
                return path

    for candidate in BASE_DIR.rglob(f"{shortcode}{ANALYSIS_SUFFIX}"):
        if candidate.is_file():
            return candidate

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

        payload_has_ocr = bool(str(payload.get("ocr_best", "") or "").strip())
        payload_processed_at = str(payload.get("processed_at", "") or "").strip()

        upsert_registry_record(
            shortcode=shortcode,
            kind=kind,
            profile_url=payload.get("profile_url", ""),
            post_url=payload.get("post_url", ""),
            post_dir=str(json_file.parent),
            analysis_json_path=str(json_file),
            image_path=payload.get("image_path", ""),
            status="processed" if (payload_has_ocr or payload_processed_at) else "downloaded",
            processed_at=payload_processed_at,
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


def find_payload(shortcode: str, statuses: Optional[Iterable[str]] = None) -> Optional[Dict]:
    analysis_path = find_analysis_path(shortcode, statuses=statuses)
    if not analysis_path:
        return None
    return read_json_file(analysis_path)


def find_cached_payload(shortcode: str) -> Optional[Dict]:
    payload = find_payload(shortcode, statuses=("processed",))
    if not payload:
        return None

    payload["ocr_best"] = str(payload.get("ocr_best", "") or "")
    return payload


def find_downloaded_payload(shortcode: str) -> Optional[Dict]:
    payload = find_payload(shortcode, statuses=("downloaded", "processed"))
    if not payload:
        return None

    image_path = Path(str(payload.get("image_path", "") or ""))
    if not image_path.exists():
        return None

    payload.setdefault("caption", "")
    payload.setdefault("ocr_best", "")
    payload.setdefault("merged_text", payload.get("caption", ""))
    return payload


# ── Estado por fuente ─────────────────────────────────────────────────────────

def load_source_state() -> Dict[str, Dict]:
    data = read_json_file(SOURCE_STATE_FILE)
    return data if isinstance(data, dict) else {}


def save_source_state(data: Dict[str, Dict]) -> None:
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    SOURCE_STATE_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def get_source_state_entry(profile_url: str) -> Dict:
    key = normalize_profile_url(profile_url)
    state = load_source_state()
    entry = state.get(key)
    return entry if isinstance(entry, dict) else {}


def infer_latest_shortcode_from_summary(profile_url: str) -> str:
    normalized = normalize_profile_url(profile_url)
    summary_path = BASE_DIR / "summary_latest_posts.json"
    data = read_json_file(summary_path)
    if not isinstance(data, list):
        return ""

    items = []
    for item in data:
        if not isinstance(item, dict):
            continue
        if normalize_profile_url(item.get("profile_url", "")) != normalized:
            continue
        shortcode = str(item.get("shortcode", "") or "").strip()
        if not shortcode:
            continue
        processed_at = str(item.get("processed_at", "") or "")
        items.append((processed_at, shortcode))

    if not items:
        return ""

    items.sort(reverse=True)
    return items[0][1]


def get_last_known_shortcode(profile_url: str) -> str:
    entry = get_source_state_entry(profile_url)
    shortcode = str(entry.get("latest_visible_shortcode", "") or "").strip()
    if shortcode:
        return shortcode
    return infer_latest_shortcode_from_summary(profile_url)


def update_source_state(profile_url: str, latest_visible_shortcode: str = "", latest_visible_kind: str = "") -> None:
    normalized = normalize_profile_url(profile_url)
    if not normalized or not latest_visible_shortcode:
        return

    state = load_source_state()
    state[normalized] = {
        "profile_url": normalized,
        "source_username": extract_source_username(normalized),
        "source_label": build_source_metadata(normalized).get("source_label", ""),
        "latest_visible_shortcode": latest_visible_shortcode,
        "latest_visible_kind": latest_visible_kind,
        "updated_at": utc_now_iso(),
    }
    save_source_state(state)


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


def estimate_max_scrolls(target_new_count: Optional[int], collect_all: bool = False) -> int:
    if collect_all or target_new_count is None:
        return 600
    target = max(1, int(target_new_count))
    return min(120, max(12, target * 4))


# ── Extracción de shortcodes del perfil ──────────────────────────────────────

def _wait_for_new_content(page, prev_count: int, selector: str, timeout: float = DELAY_SCROLL_CONTENT_TIMEOUT) -> int:
    """
    Espera activamente a que aparezcan más posts tras un scroll.
    Retorna el conteo actual de hrefs detectados.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        time.sleep(0.6)
        hrefs = page.locator(selector).evaluate_all(
            "(els) => els.map(e => e.getAttribute('href')).filter(Boolean)"
        )
        current = len(hrefs)
        if current > prev_count:
            return current
    return prev_count


def _scroll_profile_page(page, collect_all_matching: bool = False) -> None:
    """
    Hace un scroll robusto en el perfil. En modo por fecha empuja más fuerte
    para seguir cargando posts históricos.
    """
    if collect_all_matching:
        page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
    else:
        page.mouse.wheel(0, 3500)


def extract_shortcodes_from_profile(
    profile_url: str,
    target_new_count: Optional[int] = 5,
    known_shortcodes: Optional[Set[str]] = None,
    headless: bool = False,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    stop_at_shortcode: str = "",
    collect_all_matching: bool = False,
    content_mode: str = "both",
    on_candidate: Optional[Callable[[Dict], bool]] = None,
) -> Dict:
    found: Dict[str, Dict] = {}
    candidates: List[Dict] = []
    accepted_candidates = 0
    blocked = known_shortcodes or set()
    max_scrolls = estimate_max_scrolls(target_new_count, collect_all=collect_all_matching)
    stale_rounds = 0
    stop_due_to_date = False
    stop_due_to_boundary = False
    latest_visible_shortcode = ""
    latest_visible_kind = ""
    last_logged_scan_date = ""
    stale_round_limit = 12 if collect_all_matching else 5
    target_label = "todos los coincidentes" if collect_all_matching or target_new_count is None else str(target_new_count)
    source_meta = build_source_metadata(profile_url)
    source_label = source_meta.get("source_label") or profile_url
    content_mode = parse_content_mode(content_mode)
    content_selector = get_profile_link_selector(content_mode)

    log_section(f"EXTRAYENDO FUENTE: {source_label}")
    log(f"[SOURCE] {_ts()} 🌐 URL perfil: {profile_url}")
    log(f"[SOURCE] {_ts()} 🎯 Objetivo: {target_label} posts nuevos")
    log(f"[SOURCE] {_ts()} 🧩 Tipo de contenido: {build_content_mode_label(content_mode)}")
    log(f"[SOURCE] {_ts()} 📜 Scrolls máximos: {max_scrolls}")
    if date_from or date_to:
        log(f"[SOURCE] {_ts()} 📅 Filtro temporal: {build_mode_label(date_from, date_to)}")
    if stop_at_shortcode:
        log(f"[SOURCE] {_ts()} 🔖 Corte por slug conocido: {stop_at_shortcode}")
    if blocked:
        log(f"[SOURCE] {_ts()} 🔒 Posts bloqueados (ya procesados): {len(blocked)}")

    with sync_playwright() as p:
        log(f"[BROWSER] {_ts()} 🚀 Iniciando Chromium para {source_label}…")
        browser = p.chromium.launch(headless=headless)
        random_delay(DELAY_BROWSER_BOOT_MIN, DELAY_BROWSER_BOOT_MAX,
                     f"Esperando que el navegador termine de abrir en {source_label}")

        context = build_context(browser)
        page = context.new_page()
        random_delay(DELAY_AFTER_NEW_PAGE_MIN, DELAY_AFTER_NEW_PAGE_MAX,
                     f"Preparando pestaña nueva en {source_label}")

        log(f"[NAV] {_ts()} ➡ Navegando a perfil: {profile_url}")
        page.goto(profile_url, wait_until="domcontentloaded", timeout=60000)
        save_live_screenshot(page, label=f"Cargando perfil {source_label}", scroll_idx=0,
                             extra="Esperando contenido inicial…")

        # Espera aleatoria carga inicial del perfil
        random_delay(DELAY_PROFILE_LOAD_MIN, DELAY_PROFILE_LOAD_MAX,
                     f"Cargando perfil {source_label}")

        overlay_actions = dismiss_transient_overlays(page, source_label=source_label, passes=OVERLAY_DISMISS_PASSES)
        if overlay_actions == 0:
            log(f"[NAV] {_ts()} ℹ Sin banners/modales visibles al abrir {source_label}.")

        random_delay(DELAY_PROFILE_SETTLE_MIN, DELAY_PROFILE_SETTLE_MAX,
                     f"Estabilizando vista del perfil {source_label}")

        save_live_screenshot(page, label=f"Perfil abierto: {source_label}", scroll_idx=0,
                             extra="Buscando posts en el DOM…")

        log(f"[NAV] {_ts()} ⏳ Esperando que aparezcan posts en {source_label}…")
        try:
            page.wait_for_selector(content_selector, timeout=12000)
            initial_hrefs = page.locator(content_selector).count()
            log(f"[NAV] {_ts()} ✓ Posts iniciales detectados en DOM: {initial_hrefs}")
        except PlaywrightTimeoutError:
            overlay_retry_actions = dismiss_transient_overlays(page, source_label=source_label, passes=2)
            if overlay_retry_actions:
                random_delay(DELAY_PROFILE_SETTLE_MIN, DELAY_PROFILE_SETTLE_MAX,
                             f"Reintentando tras cerrar overlays en {source_label}")
                try:
                    page.wait_for_selector(content_selector, timeout=8000)
                    initial_hrefs = page.locator(content_selector).count()
                    log(f"[NAV] {_ts()} ✓ Posts detectados tras limpiar overlays: {initial_hrefs}")
                except PlaywrightTimeoutError:
                    html_path, png_path = save_debug_artifacts(page, "sin_posts_visibles")
                    log(f"[WARN] {_ts()} No aparecieron links visibles para el filtro {build_content_mode_label(content_mode)} en el timeout inicial.")
                    log(f"[WARN] {_ts()} HTML guardado en: {html_path}")
                    log(f"[WARN] {_ts()} Screenshot guardado en: {png_path}")
            else:
                html_path, png_path = save_debug_artifacts(page, "sin_posts_visibles")
                log(f"[WARN] {_ts()} No aparecieron links visibles para el filtro {build_content_mode_label(content_mode)} en el timeout inicial.")
                log(f"[WARN] {_ts()} HTML guardado en: {html_path}")
                log(f"[WARN] {_ts()} Screenshot guardado en: {png_path}")

        for scroll_idx in range(max_scrolls):
            dismiss_transient_overlays(page, source_label=source_label, passes=1)
            prev_found = len(found)
            prev_candidates = len(candidates)
            prev_accepted_candidates = accepted_candidates
            added_this_scroll = 0

            save_live_screenshot(
                page,
                label=f"Scroll {scroll_idx + 1}/{max_scrolls} — {source_label}",
                scroll_idx=scroll_idx,
                extra=f"Candidatos: {len(candidates)}/{target_label} | Detectados: {len(found)}",
            )

            hrefs = page.locator(content_selector).evaluate_all(
                "(els) => els.map(e => e.getAttribute('href')).filter(Boolean)"
            )

            log(f"[SCROLL] {_ts()} 🔍 Scroll {scroll_idx + 1}/{max_scrolls} "
                f"| hrefs en DOM: {len(hrefs)} | candidatos: {len(candidates)}/{target_label}")

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
                if content_mode == "post" and kind != "p":
                    continue
                if content_mode == "reel" and kind != "reel":
                    continue
                if shortcode in found:
                    continue

                if not latest_visible_shortcode:
                    latest_visible_shortcode = shortcode
                    latest_visible_kind = kind
                    log(f"[INFO] {_ts()} 📌 Primer post visible de {source_label}: {kind}:{shortcode}")

                item = {
                    "kind": kind,
                    "shortcode": shortcode,
                    "href": href,
                    "post_datetime": "",
                    "post_date": "",
                }
                found[shortcode] = item

                if stop_at_shortcode and shortcode == stop_at_shortcode:
                    stop_due_to_boundary = True
                    log(f"[INFO] {_ts()} 🔖 Slug límite alcanzado → {kind}:{shortcode}. No hay más nuevos en esta fuente.")
                    break

                if shortcode in blocked:
                    log(f"[SKIP] {_ts()} ♻ Ya procesado → {kind}:{shortcode}")
                    continue

                if date_from or date_to:
                    log(f"[DATE] {_ts()} 📆 Verificando fecha de post {kind}:{shortcode}…")
                    post_datetime = fetch_post_datetime(context, kind, shortcode)
                    post_date_value = parse_post_date_from_iso(post_datetime)
                    item["post_datetime"] = post_datetime or ""
                    item["post_date"] = post_date_value.isoformat() if post_date_value else ""

                    if item["post_date"] and item["post_date"] != last_logged_scan_date:
                        last_logged_scan_date = item["post_date"]
                        log(f"[DATE] {_ts()} 🗓 Scrapeando fecha {item['post_date']} en {source_label}…")

                    if should_stop_after_candidate(post_date_value, date_from):
                        stop_due_to_date = True
                        log(
                            f"[STOP] {_ts()} 📅 Límite histórico alcanzado → {kind}:{shortcode} "
                            f"({item['post_date'] or 'sin fecha'}) anterior a {date_from}."
                        )
                        break

                    if not match_post_date(post_date_value, date_from, date_to):
                        log(
                            f"[SKIP] {_ts()} 📅 Post fuera de rango de fechas → {kind}:{shortcode} "
                            f"(fecha={item['post_date'] or 'desconocida'})"
                        )
                        continue

                candidates.append(item)
                added_this_scroll += 1
                handled_now = True
                if on_candidate is not None:
                    try:
                        handled_now = bool(on_candidate(item))
                    except Exception as exc:
                        handled_now = False
                        log(f"[ERROR] {_ts()} ❌ Falló el manejo inmediato del candidato {kind}:{shortcode} → {exc}")
                if handled_now:
                    accepted_candidates += 1
                log(
                    f"[OK] {_ts()} ✅ Candidato válido → {kind}:{shortcode}"
                    + (f" | fecha={item['post_date']}" if item.get("post_date") else "")
                    + (" | acción inmediata completada" if handled_now else " | acción inmediata no completada")
                    + f" | candidatos válidos: {len(candidates)}/{target_label}"
                    + f" | manejados: {accepted_candidates}/{target_label}"
                )

                if not collect_all_matching and target_new_count is not None and accepted_candidates >= target_new_count:
                    break

            log(
                f"[SCROLL] {_ts()} 📊 Resumen scroll {scroll_idx + 1}: "
                f"detectados={len(found)} | válidos={len(candidates)}/{target_label} | "
                f"manejados={accepted_candidates}/{target_label} | nuevos en este scroll={added_this_scroll} | sin-cambio-rondas={stale_rounds}"
            )

            if stop_due_to_boundary or stop_due_to_date:
                log(f"[STOP] {_ts()} 🛑 Deteniendo exploración de {source_label} (razón: {'boundary' if stop_due_to_boundary else 'fecha'})")
                break
            if not collect_all_matching and target_new_count is not None and accepted_candidates >= target_new_count:
                log(f"[STOP] {_ts()} 🎯 Meta alcanzada ({accepted_candidates}/{target_label}) en {source_label}")
                break

            if len(found) == prev_found and len(candidates) == prev_candidates and accepted_candidates == prev_accepted_candidates:
                stale_rounds += 1
                log(f"[SCROLL] {_ts()} ⚠ Sin nuevos posts (ronda sin cambios {stale_rounds}/{stale_round_limit})")
                save_live_screenshot(
                    page,
                    label=f"⚠ Sin nuevos posts — scroll {scroll_idx + 1}",
                    scroll_idx=scroll_idx,
                    extra=f"Ronda sin cambios: {stale_rounds}/{stale_round_limit} | DOM hrefs: {len(hrefs)}",
                )
            else:
                stale_rounds = 0

            if stale_rounds >= stale_round_limit:
                log(f"[WARN] {_ts()} 🛑 Sin nuevos posts tras {stale_round_limit} scrolls consecutivos en {source_label}. Terminando exploración.")
                save_live_screenshot(
                    page,
                    label=f"🛑 DETENIDO — sin nuevos posts en {source_label}",
                    scroll_idx=scroll_idx,
                    extra=f"{stale_round_limit} scrolls consecutivos sin cambios. DOM tiene {len(hrefs)} hrefs totales.",
                )
                break

            # Scroll y espera activa a que cargue contenido nuevo
            dismiss_transient_overlays(page, source_label=source_label, passes=1)
            log(f"[SCROLL] {_ts()} ↕ Ejecutando scroll en {source_label}…")
            _scroll_profile_page(page, collect_all_matching=collect_all_matching)

            # Espera activa: detecta si aparecen nuevos hrefs
            new_href_count = _wait_for_new_content(page, len(hrefs), content_selector, timeout=DELAY_SCROLL_CONTENT_TIMEOUT)
            if new_href_count > len(hrefs):
                log(f"[SCROLL] {_ts()} ✓ Nuevos elementos detectados en DOM tras scroll: {new_href_count} (antes: {len(hrefs)})")
                save_live_screenshot(
                    page,
                    label=f"✓ Nuevos posts cargados — scroll {scroll_idx + 1}",
                    scroll_idx=scroll_idx,
                    extra=f"DOM hrefs: {len(hrefs)} → {new_href_count}",
                )
            else:
                log(f"[SCROLL] {_ts()} ℹ Sin nuevos elementos tras scroll (DOM estable: {new_href_count})")
                save_live_screenshot(
                    page,
                    label=f"ℹ DOM estable tras scroll {scroll_idx + 1}",
                    scroll_idx=scroll_idx,
                    extra=f"hrefs en DOM: {new_href_count} (sin cambio)",
                )

            # Pausa aleatoria adicional entre scrolls
            random_delay(DELAY_AFTER_SCROLL_MIN, DELAY_AFTER_SCROLL_MAX,
                         f"Espera entre scrolls en {source_label}")


        log_section(f"FIN EXTRACCIÓN: {source_label} — {accepted_candidates} manejados / {len(candidates)} candidatos / {len(found)} detectados")
        browser.close()

    return {
        "posts": candidates,
        "latest_visible_shortcode": latest_visible_shortcode,
        "latest_visible_kind": latest_visible_kind,
        "stop_due_to_boundary": stop_due_to_boundary,
        "stop_due_to_date": stop_due_to_date,
        "detected_total": len(found),
        "accepted_total": accepted_candidates,
    }


# ── Descarga e imagen ─────────────────────────────────────────────────────────

def build_post_url(kind: str, shortcode: str) -> str:
    return (
        f"https://www.instagram.com/{kind}/{shortcode}/"
        if kind == "reel"
        else f"https://www.instagram.com/p/{shortcode}/"
    )


def write_analysis_payload(post_dir: Path, shortcode: str, payload: Dict) -> Path:
    out_json = post_dir / f"{shortcode}{ANALYSIS_SUFFIX}"
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_json


def run_instaloader_for_shortcode(kind: str, shortcode: str, profile_url: str = "") -> Path:
    source_dir = get_source_storage_dir(profile_url)
    target = f"-{shortcode}"
    outdir = source_dir / target
    outdir.mkdir(parents=True, exist_ok=True)

    dirname_pattern = str(source_dir / "{target}")

    cmd = [
        "instaloader",
        "--no-videos",
        "--no-video-thumbnails",
        "--dirname-pattern", dirname_pattern,
        "--filename-pattern", "{date_utc}_UTC",
        "--",
        target,
    ]

    log(f"[DOWNLOAD] {_ts()} ⬇ Descargando {kind}:{shortcode} con Instaloader…")
    log(f"[DOWNLOAD] {_ts()} 🗂 Fuente destino: {source_dir}")
    log(f"[DOWNLOAD] {_ts()} 📂 Directorio destino: {outdir}")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        raise RuntimeError(
            f"Instaloader falló para {shortcode}\nSTDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}"
        )

    if result.stdout.strip():
        log(result.stdout.strip())
    if result.stderr.strip():
        log(f"[WARN] {_ts()} STDERR Instaloader:")
        log(result.stderr.strip())

    log(f"[DOWNLOAD] {_ts()} ✓ Descarga completada: {kind}:{shortcode}")
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


# ── OCR ───────────────────────────────────────────────────────────────────────

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
    log(f"[OCR] {_ts()} 🔎 Ejecutando OCR sobre: {image_path.name}")
    original, processed = preprocess_image(str(image_path))

    text_original = pytesseract.image_to_string(original, lang=OCR_LANG, config=TESSERACT_CONFIG)
    text_processed = pytesseract.image_to_string(processed, lang=OCR_LANG, config=TESSERACT_CONFIG)

    text_original = clean_text(text_original)
    text_processed = clean_text(text_processed)
    best = text_processed if len(text_processed) >= len(text_original) else text_original

    preproc_path = image_path.with_name(image_path.stem + "_preproc.jpg")
    cv2.imwrite(str(preproc_path), processed)

    log(f"[OCR] {_ts()} ✓ OCR completado | original={len(text_original)} chars | procesado={len(text_processed)} chars | mejor={len(best)} chars")

    return {
        "ocr_original": text_original,
        "ocr_processed": text_processed,
        "ocr_best": best,
        "preprocessed_image": str(preproc_path),
    }


# ── Procesamiento de un post ──────────────────────────────────────────────────

def build_download_payload(
    kind: str,
    shortcode: str,
    profile_url: str = "",
    post_datetime: str = "",
    post_date: str = "",
    post_dir: Optional[Path] = None,
    image_path: Optional[Path] = None,
    caption: str = "",
) -> Dict:
    source_meta = build_source_metadata(profile_url)
    safe_post_dir = post_dir or expected_post_dir(shortcode, profile_url=profile_url)
    safe_image_path = image_path or find_latest_image(safe_post_dir)

    return {
        "kind": kind,
        "shortcode": shortcode,
        "profile_url": source_meta["profile_url"],
        "source_username": source_meta["source_username"],
        "source_label": source_meta["source_label"],
        "downloaded_at": utc_now_iso(),
        "processed_at": "",
        "post_datetime": post_datetime,
        "post_date": post_date,
        "post_url": build_post_url(kind, shortcode),
        "post_dir": str(safe_post_dir),
        "image_path": str(safe_image_path) if safe_image_path else "",
        "caption": caption,
        "ocr_best": "",
        "merged_text": caption.strip(),
        "preprocessed_image": "",
    }


def download_shortcode(
    kind: str,
    shortcode: str,
    profile_url: str = "",
    post_datetime: str = "",
    post_date: str = "",
) -> Dict:
    post_url = build_post_url(kind, shortcode)
    log(f"[POST] {_ts()} 📥 Preparando descarga {kind}:{shortcode} | URL: {post_url}")

    post_dir = run_instaloader_for_shortcode(kind, shortcode, profile_url=profile_url)

    image_path = find_latest_image(post_dir)
    if not image_path:
        raise FileNotFoundError(f"No encontré imagen descargada en {post_dir}")

    log(f"[POST] {_ts()} 🖼 Imagen encontrada: {image_path.name} ({image_path.stat().st_size // 1024} KB)")

    caption = read_caption_for_image(image_path)
    if caption:
        log(f"[POST] {_ts()} 📝 Caption leído ({len(caption)} chars)")
    else:
        log(f"[POST] {_ts()} ℹ Sin caption disponible para {kind}:{shortcode}")

    payload = build_download_payload(
        kind=kind,
        shortcode=shortcode,
        profile_url=profile_url,
        post_datetime=post_datetime,
        post_date=post_date,
        post_dir=post_dir,
        image_path=image_path,
        caption=caption,
    )

    out_json = write_analysis_payload(post_dir, shortcode, payload)
    log(f"[POST] {_ts()} 💾 Metadata de descarga guardada: {out_json}")

    upsert_registry_record(
        shortcode=shortcode,
        kind=kind,
        profile_url=payload["profile_url"],
        post_url=payload["post_url"],
        post_dir=str(post_dir),
        analysis_json_path=str(out_json),
        image_path=str(image_path),
        status="downloaded",
        processed_at="",
    )
    return payload


def enrich_payload_with_ocr(payload: Dict) -> Dict:
    shortcode = str(payload.get("shortcode", "") or "").strip()
    if not shortcode:
        raise ValueError("Payload sin shortcode para OCR.")

    image_path = Path(str(payload.get("image_path", "") or ""))
    if not image_path.exists():
        raise FileNotFoundError(f"No existe la imagen para OCR: {image_path}")

    post_dir = locate_post_dir(shortcode) or image_path.parent
    ocr = run_ocr(image_path)
    processed_at = utc_now_iso()

    enriched = dict(payload)
    enriched["processed_at"] = processed_at
    enriched["ocr_best"] = ocr["ocr_best"]
    enriched["preprocessed_image"] = ocr["preprocessed_image"]
    enriched["merged_text"] = "\n\n".join(
        [x for x in [str(enriched.get("caption", "") or ""), ocr["ocr_best"]] if x.strip()]
    ).strip()
    enriched.setdefault("post_dir", str(post_dir))

    out_json = write_analysis_payload(post_dir, shortcode, enriched)
    log(f"[OCR] {_ts()} 💾 Análisis OCR guardado: {out_json}")

    upsert_registry_record(
        shortcode=shortcode,
        kind=str(enriched.get("kind", "") or "p"),
        profile_url=str(enriched.get("profile_url", "") or ""),
        post_url=str(enriched.get("post_url", "") or ""),
        post_dir=str(post_dir),
        analysis_json_path=str(out_json),
        image_path=str(image_path),
        status="processed",
        processed_at=processed_at,
    )
    return enriched


def download_shortcode_with_retry(
    kind: str,
    shortcode: str,
    profile_url: str = "",
    post_datetime: str = "",
    post_date: str = "",
) -> Dict:
    last_exc: Exception = RuntimeError("Sin reintentos de descarga")
    for attempt in range(1, MAX_RETRIES_PER_POST + 2):
        try:
            return download_shortcode(kind, shortcode, profile_url, post_datetime, post_date)
        except Exception as exc:
            last_exc = exc
            if attempt <= MAX_RETRIES_PER_POST:
                log(f"[RETRY] {_ts()} ⚠ Intento {attempt}/{MAX_RETRIES_PER_POST + 1} falló en descarga para {kind}:{shortcode} → {exc}")
                random_delay(
                    RETRY_WAIT_MIN,
                    RETRY_WAIT_MAX,
                    f"Esperando antes del reintento de descarga {attempt + 1} para {kind}:{shortcode}",
                )
            else:
                log(f"[RETRY] {_ts()} ❌ Todos los intentos de descarga fallaron para {kind}:{shortcode}")
    raise last_exc


def ocr_payload_with_retry(payload: Dict) -> Dict:
    shortcode = str(payload.get("shortcode", "") or "").strip()
    kind = str(payload.get("kind", "") or "post").strip()
    last_exc: Exception = RuntimeError("Sin reintentos de OCR")
    for attempt in range(1, MAX_RETRIES_PER_POST + 2):
        try:
            return enrich_payload_with_ocr(payload)
        except Exception as exc:
            last_exc = exc
            if attempt <= MAX_RETRIES_PER_POST:
                log(f"[RETRY] {_ts()} ⚠ Intento {attempt}/{MAX_RETRIES_PER_POST + 1} falló en OCR para {kind}:{shortcode} → {exc}")
                random_delay(
                    RETRY_WAIT_MIN,
                    RETRY_WAIT_MAX,
                    f"Esperando antes del reintento OCR {attempt + 1} para {kind}:{shortcode}",
                )
            else:
                log(f"[RETRY] {_ts()} ❌ Todos los intentos de OCR fallaron para {kind}:{shortcode}")
    raise last_exc


# ── Procesamiento de una fuente ───────────────────────────────────────────────

def process_source(
    profile_url: str,
    target_new_count: Optional[int],
    acquired_shortcodes: Set[str],
    results: List[Dict],
    pending_ocr: List[Dict],
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    stop_at_shortcode: str = "",
    collect_all_matching: bool = False,
    source_index: int = 0,
    source_total: int = 0,
    content_mode: str = "both",
) -> Dict[str, int]:
    requested_count = parse_positive_limit(target_new_count) if target_new_count is not None else None
    stats = {
        "downloaded": 0,
        "queued_existing": 0,
        "already_processed": 0,
        "skipped": 0,
        "failed": 0,
    }
    source_meta = build_source_metadata(profile_url)
    source_label = source_meta.get("source_label") or profile_url
    content_mode = parse_content_mode(content_mode)

    log(f"\n[SOURCE] {_ts()} 🚀 Iniciando fuente {source_index}/{source_total}: {source_label} ({profile_url})")
    log(f"[SOURCE] {_ts()} 🧩 Filtro de contenido activo: {build_content_mode_label(content_mode)}")
    log(f"[SOURCE] {_ts()} 🪜 Flujo activo: detectar candidato válido → descargar inmediato + caption → OCR diferido por lote")

    def handle_candidate_immediately(item: Dict) -> bool:
        shortcode = item["shortcode"]
        kind = item["kind"]
        post_date_label = item.get("post_date") or "sin fecha"

        log(f"\n[POST] {_ts()} 📌 Candidato válido detectado en {source_label}: {kind}:{shortcode} | fecha={post_date_label}")

        if shortcode in acquired_shortcodes:
            stats["skipped"] += 1
            log(f"[SKIP] {_ts()} ♻ Ya visto en esta ejecución → {kind}:{shortcode}")
            return False

        cached_payload = find_cached_payload(shortcode)
        if cached_payload:
            acquired_shortcodes.add(shortcode)
            results.append(cached_payload)
            upsert_registry_record(
                shortcode=shortcode,
                kind=kind,
                profile_url=str(cached_payload.get("profile_url", "") or profile_url),
                post_url=cached_payload.get("post_url", ""),
                post_dir=str(locate_post_dir(shortcode) or expected_post_dir(shortcode, profile_url=profile_url)),
                analysis_json_path=str(find_analysis_path(shortcode, statuses=("processed",)) or expected_analysis_path(shortcode, profile_url=profile_url)),
                image_path=cached_payload.get("image_path", ""),
                status="processed",
                processed_at=str(cached_payload.get("processed_at", "") or ""),
            )
            stats["already_processed"] += 1
            log(f"[SKIP] {_ts()} 📂 Caché OCR disponible, reutilizando → {kind}:{shortcode}")
            return True

        downloaded_payload = find_downloaded_payload(shortcode)
        if downloaded_payload and not str(downloaded_payload.get("ocr_best", "") or "").strip():
            acquired_shortcodes.add(shortcode)
            pending_ocr.append(downloaded_payload)
            results.append(downloaded_payload)
            stats["queued_existing"] += 1
            log(f"[QUEUE] {_ts()} ♻ Descarga previa detectada, se agenda OCR posterior → {kind}:{shortcode}")
            return True

        try:
            payload = download_shortcode_with_retry(
                kind,
                shortcode,
                profile_url=profile_url,
                post_datetime=item.get("post_datetime", ""),
                post_date=item.get("post_date", ""),
            )
            acquired_shortcodes.add(shortcode)
            pending_ocr.append(payload)
            results.append(payload)
            stats["downloaded"] += 1
            log(
                f"[OK] {_ts()} ✅ Post descargado de inmediato y encolado para OCR: {kind}:{shortcode} | "
                f"progreso descargas nuevas {stats['downloaded']}/{requested_count if requested_count is not None else 'todos'}"
            )
            if requested_count is None or collect_all_matching or (stats["downloaded"] < requested_count):
                random_delay(
                    DELAY_BETWEEN_POSTS_MIN,
                    DELAY_BETWEEN_POSTS_MAX,
                    f"Pausa tras descarga inmediata en {source_label}",
                )
            return True
        except Exception as exc:
            stats["failed"] += 1
            log(f"[ERROR] {_ts()} ❌ Falló descarga inmediata para {kind}:{shortcode} → {exc}")
            return False

    extraction = extract_shortcodes_from_profile(
        profile_url,
        target_new_count=requested_count,
        known_shortcodes=acquired_shortcodes,
        headless=BROWSER_HEADLESS,
        date_from=date_from,
        date_to=date_to,
        stop_at_shortcode=stop_at_shortcode,
        collect_all_matching=collect_all_matching,
        content_mode=content_mode,
        on_candidate=handle_candidate_immediately,
    )
    posts = extraction.get("posts", [])
    latest_visible_shortcode = extraction.get("latest_visible_shortcode", "")
    latest_visible_kind = extraction.get("latest_visible_kind", "")

    if latest_visible_shortcode:
        update_source_state(profile_url, latest_visible_shortcode, latest_visible_kind)
        log(f"[SOURCE] {_ts()} 💾 Estado de fuente actualizado: último slug={latest_visible_shortcode}")

    if not posts:
        if extraction.get("stop_due_to_boundary"):
            log(f"[INFO] {_ts()} ℹ Sin posts nuevos para {source_label}. Último slug coincide con el guardado.")
        else:
            log(f"[WARN] {_ts()} ⚠ No se encontraron posts candidatos en {source_label}")
        return stats

    detected = [
        f"{p['kind']}:{p['shortcode']}" + (f"@{p.get('post_date')}" if p.get("post_date") else "")
        for p in posts
    ]
    if detected:
        log(f"[SOURCE] {_ts()} 📋 Candidatos válidos detectados y manejados en línea en {source_label}: {detected}")
    log(
        f"[SOURCE] {_ts()} 📦 Resumen manejo inmediato en {source_label}: "
        f"descargados={stats['downloaded']} | reutilizados-descargados={stats['queued_existing']} | "
        f"ya-procesados={stats['already_processed']} | fallidos={stats['failed']}"
    )

    handled_total = extraction.get("accepted_total", 0)
    if requested_count is not None and not collect_all_matching and handled_total < requested_count:
        log(
            f"[WARN] {_ts()} ⚠ {source_label} no alcanzó la meta de candidatos manejados. "
            f"Solicitados: {requested_count}, manejados: {handled_total}, descargados nuevos: {stats['downloaded']}."
        )

    log(
        f"[SOURCE] {_ts()} 🏁 Fin fuente {source_label}: "
        f"manejados={handled_total} | descargados={stats['downloaded']} | pendientes OCR reaprovechados={stats['queued_existing']} | "
        f"ya procesados={stats['already_processed']} | omitidos={stats['skipped']} | fallidos={stats['failed']}"
    )
    return stats


# ── Lote de fuentes ───────────────────────────────────────────────────────────

def run_scrape_jobs(
    source_jobs: List[Dict[str, int | str]],
    shared_total_limit: Optional[int] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    scheduler_all_new: bool = False,
    content_mode: str = "both",
) -> Dict[str, int]:
    if not source_jobs:
        raise ValueError("No se recibieron fuentes para procesar.")

    BASE_DIR.mkdir(parents=True, exist_ok=True)
    validate_date_range(date_from, date_to)
    content_mode = parse_content_mode(content_mode)
    reset_manual_log()
    collect_all_by_date = bool(date_from or date_to)
    if collect_all_by_date or scheduler_all_new:
        source_jobs = [
            {"profile_url": str(job.get("profile_url", "")).strip()}
            for job in source_jobs
            if str(job.get("profile_url", "")).strip()
        ]
        shared_total_limit = None

    run_prefix = "programada" if IS_SCHEDULER_RUN else "manual"
    log_section(f"INICIO DE EJECUCIÓN {run_prefix.upper()} — {utc_now_iso()}")
    log(f"[RUN] {_ts()} 🗓 Modo temporal: {build_mode_label(date_from, date_to)}")
    log(f"[RUN] {_ts()} 🧩 Tipo de contenido: {build_content_mode_label(content_mode)}")
    log(f"[RUN] {_ts()} 📡 Fuentes recibidas: {len(source_jobs)}")
    log(f"[RUN] {_ts()} 🪜 Pipeline: validación de fecha → descarga/caption por fuente → OCR masivo al final")
    for i, job in enumerate(source_jobs, 1):
        log(f"[RUN] {_ts()}   {i}. {build_source_execution_label(job, collect_all_by_date=collect_all_by_date, scheduler_all_new=scheduler_all_new)}")
    if scheduler_all_new:
        log(f"[RUN] {_ts()} 🔄 Modo scheduler: se buscarán todos los posts nuevos por fuente hasta el slug conocido.")

    init_registry()
    synced = bootstrap_registry_from_disk()
    if synced:
        log(f"[RUN] {_ts()} 🗄 Registro local sincronizado con {synced} análisis existentes en disco.")

    processed_shortcodes = set(load_processed_shortcodes())
    acquired_shortcodes = set(processed_shortcodes)
    log(f"[RUN] {_ts()} 🔒 Total posts ya procesados en registro: {len(processed_shortcodes)}")

    results: List[Dict] = []
    pending_ocr: List[Dict] = []
    total_downloaded = 0
    total_queued_existing = 0
    total_already_processed = 0
    total_skipped = 0
    total_failed_download = 0
    total_ocr_processed = 0
    total_ocr_failed = 0

    def write_summary_snapshot() -> int:
        summary_path = BASE_DIR / "summary_latest_posts.json"
        existing_summary = read_json_file(summary_path) or []
        merged_summary = merge_payloads(existing_summary, results)
        summary_path.write_text(json.dumps(merged_summary, ensure_ascii=False, indent=2), encoding="utf-8")
        return len(merged_summary)

    if shared_total_limit is not None and not collect_all_by_date and not scheduler_all_new:
        shared_total_limit = parse_positive_limit(shared_total_limit)
        profile_urls = [str(job["profile_url"]) for job in source_jobs]
        log(f"[RUN] {_ts()} 🌐 Modo global: objetivo total {shared_total_limit} posts nuevos entre {len(profile_urls)} fuentes")

        for idx, profile_url in enumerate(profile_urls, start=1):
            if total_downloaded >= shared_total_limit:
                log(f"[RUN] {_ts()} 🎯 Objetivo global de descarga alcanzado ({total_downloaded}/{shared_total_limit}). Deteniendo etapa 1.")
                break

            remaining = shared_total_limit - total_downloaded
            log(f"\n[RUN] {_ts()} ▶ Fuente {idx}/{len(profile_urls)}: {profile_url} | faltan {remaining} descargas globales")

            source_stats = process_source(
                profile_url,
                remaining,
                acquired_shortcodes,
                results,
                pending_ocr,
                date_from=date_from,
                date_to=date_to,
                source_index=idx,
                source_total=len(profile_urls),
                content_mode=content_mode,
            )
            total_downloaded += source_stats["downloaded"]
            total_queued_existing += source_stats["queued_existing"]
            total_already_processed += source_stats["already_processed"]
            total_skipped += source_stats["skipped"]
            total_failed_download += source_stats["failed"]

            if idx < len(profile_urls) and total_downloaded < shared_total_limit:
                random_delay(
                    DELAY_BETWEEN_SOURCES_MIN,
                    DELAY_BETWEEN_SOURCES_MAX,
                    f"Pausa entre fuente {idx} y {idx + 1} de {len(profile_urls)}",
                )
    else:
        if collect_all_by_date:
            log(f"[RUN] {_ts()} 🌐 Modo por fecha: sin límite por cantidad, corte por fecha por cada fuente.")
        elif scheduler_all_new:
            log(f"[RUN] {_ts()} 🌐 Modo scheduler por fuente: solo nuevos, sin cuota fija.")
        else:
            log(f"[RUN] {_ts()} 🌐 Modo por fuente (cuotas independientes)")

        if collect_all_by_date:
            log(f"[RUN] {_ts()} 📅 Modo temporal con fecha: scrapeo total sin límite por cantidad ({build_mode_label(date_from, date_to)}).")

        for idx, job in enumerate(source_jobs, start=1):
            profile_url = str(job["profile_url"])
            configured_limit = parse_positive_limit(job.get("limit"), DEFAULT_LIMIT)
            stop_at_shortcode = get_last_known_shortcode(profile_url) if scheduler_all_new else ""
            target_for_source = None if (collect_all_by_date or scheduler_all_new) else configured_limit
            meta = build_source_metadata(profile_url)
            source_label = meta.get("source_label") or profile_url

            log(f"\n[RUN] {_ts()} ▶ Fuente {idx}/{len(source_jobs)}: {source_label}")
            if scheduler_all_new:
                log(f"[RUN] {_ts()} 🔄 Objetivo: todos los nuevos. Último slug: {stop_at_shortcode or '-'}")
            elif collect_all_by_date:
                log(f"[RUN] {_ts()} 📅 Objetivo: todos los posts que coincidan con {build_mode_label(date_from, date_to)}.")
            else:
                log(f"[RUN] {_ts()} 🎯 Objetivo: {configured_limit} posts nuevos.")

            source_stats = process_source(
                profile_url,
                target_for_source,
                acquired_shortcodes,
                results,
                pending_ocr,
                date_from=date_from,
                date_to=date_to,
                stop_at_shortcode=stop_at_shortcode,
                collect_all_matching=bool(collect_all_by_date or scheduler_all_new),
                source_index=idx,
                source_total=len(source_jobs),
                content_mode=content_mode,
            )
            total_downloaded += source_stats["downloaded"]
            total_queued_existing += source_stats["queued_existing"]
            total_already_processed += source_stats["already_processed"]
            total_skipped += source_stats["skipped"]
            total_failed_download += source_stats["failed"]

            if idx < len(source_jobs):
                random_delay(
                    DELAY_BETWEEN_SOURCES_MIN,
                    DELAY_BETWEEN_SOURCES_MAX,
                    f"Pausa entre fuente {idx} ({source_label}) y la siguiente",
                )

    summary_count_after_download = write_summary_snapshot()
    log_section(f"ETAPA 1 COMPLETADA — DESCARGAS Y METADATA — {utc_now_iso()}")
    log(f"[RUN] {_ts()} ⬇ Descargas nuevas      : {total_downloaded}")
    log(f"[RUN] {_ts()} ♻ Descargas previas usadas: {total_queued_existing}")
    log(f"[RUN] {_ts()} 🗄 Ya procesados con OCR : {total_already_processed}")
    log(f"[RUN] {_ts()} 🚫 Omitidos             : {total_skipped}")
    log(f"[RUN] {_ts()} ❌ Fallos de descarga   : {total_failed_download}")
    log(f"[RUN] {_ts()} 📄 Resumen parcial      : {summary_count_after_download}")

    if pending_ocr:
        log_section(f"ETAPA 2 — OCR MASIVO POSTERIOR A DESCARGAS — {utc_now_iso()}")
        log(f"[RUN] {_ts()} 🧠 OCR pendiente para {len(pending_ocr)} posts descargados.")

        for idx, payload in enumerate(pending_ocr, start=1):
            shortcode = str(payload.get("shortcode", "") or "").strip()
            kind = str(payload.get("kind", "") or "post").strip()
            log(f"\n[OCR] {_ts()} 📌 OCR {idx}/{len(pending_ocr)} → {kind}:{shortcode}")
            try:
                enriched = ocr_payload_with_retry(payload)
                results.append(enriched)
                total_ocr_processed += 1
                log(f"[OK] {_ts()} ✅ OCR completado para {kind}:{shortcode} | progreso {total_ocr_processed}/{len(pending_ocr)}")
            except Exception as exc:
                total_ocr_failed += 1
                log(f"[ERROR] {_ts()} ❌ Falló OCR para {kind}:{shortcode} → {exc}")

            if idx < len(pending_ocr):
                random_delay(
                    DELAY_BETWEEN_POSTS_MIN,
                    DELAY_BETWEEN_POSTS_MAX,
                    f"Pausa entre OCR {idx} y {idx + 1}",
                )
    else:
        log(f"[RUN] {_ts()} ℹ No hubo posts pendientes para OCR en esta ejecución.")

    summary_path = BASE_DIR / "summary_latest_posts.json"
    summary_total = write_summary_snapshot()

    log_section(f"RESULTADO FINAL — {utc_now_iso()}")
    log(f"[RUN] {_ts()} ✅ Descargas nuevas     : {total_downloaded}")
    log(f"[RUN] {_ts()} ♻ Descargas reusadas   : {total_queued_existing}")
    log(f"[RUN] {_ts()} 🗄 Ya con OCR en caché  : {total_already_processed}")
    log(f"[RUN] {_ts()} 🔎 OCR completados      : {total_ocr_processed}")
    log(f"[RUN] {_ts()} ❌ Fallos descarga      : {total_failed_download}")
    log(f"[RUN] {_ts()} ❌ Fallos OCR           : {total_ocr_failed}")
    log(f"[RUN] {_ts()} 🚫 Omitidos             : {total_skipped}")
    log(f"[RUN] {_ts()} 📄 Total en resumen     : {summary_total}")
    log(f"[RUN] {_ts()} 💾 Resumen guardado en  : {summary_path}")

    if shared_total_limit is not None and not collect_all_by_date and not scheduler_all_new and total_downloaded < shared_total_limit:
        log(
            f"[WARN] {_ts()} ⚠ No se alcanzó el objetivo global de descargas. "
            f"Solicitados: {shared_total_limit}, obtenidos: {total_downloaded}."
        )
        log(f"[WARN] {_ts()} Causas típicas: pocos posts nuevos, perfil muy corto o fallas de descarga.")

    return {
        "processed": total_ocr_processed + total_already_processed,
        "downloaded": total_downloaded,
        "queued_existing": total_queued_existing,
        "skipped": total_skipped,
        "failed": total_failed_download + total_ocr_failed,
        "ocr_failed": total_ocr_failed,
        "summary_total": summary_total,
    }




# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_cli_options(argv: List[str]) -> Tuple[List[str], Optional[date], Optional[date], bool, str]:
    remaining: List[str] = []
    date_from: Optional[date] = None
    date_to: Optional[date] = None
    scheduler_all_new = False
    content_mode = "both"
    idx = 0

    while idx < len(argv):
        token = argv[idx]
        if token in {"--until", "--since"}:
            if idx + 1 >= len(argv):
                raise ValueError(f"Falta valor para {token}")
            date_from = parse_compact_date(argv[idx + 1])
            idx += 2
            continue
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
        if token == "--scheduler-all-new":
            scheduler_all_new = True
            idx += 1
            continue
        if token == "--content-mode":
            if idx + 1 >= len(argv):
                raise ValueError("Debes indicar un valor después de --content-mode (both, post o reel).")
            content_mode = parse_content_mode(argv[idx + 1])
            idx += 2
            continue
        remaining.append(token)
        idx += 1

    validate_date_range(date_from, date_to)
    return remaining, date_from, date_to, scheduler_all_new, content_mode


def main() -> None:
    try:
        argv, date_from, date_to, scheduler_all_new, content_mode = parse_cli_options(sys.argv[1:])
    except ValueError as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)

    source_jobs, shared_total_limit, mode = parse_cli_jobs(argv)
    if not source_jobs:
        print("Uso global: python app.py https://www.instagram.com/biobiochile/ https://www.instagram.com/cnnchile/ 20")
        print("Uso por fuente: python app.py @biobiochile=30 @cnnchile=50 @latercera=20")
        print("Uso histórico hasta fecha objetivo: python app.py --until 010326 @biobiochile @cnnchile")
        print("Filtrar contenido: python app.py --content-mode post @biobiochile=20")
        print("Modo scheduler interno: python app.py --scheduler-all-new @biobiochile @cnnchile")
        sys.exit(1)

    print(f"[INFO] {_ts()} Modo detectado: {mode}")
    if date_from or date_to:
        print(f"[INFO] {_ts()} Filtro temporal CLI: {build_mode_label(date_from, date_to)}")
    if scheduler_all_new:
        print(f"[INFO] {_ts()} Flag scheduler-all-new activa.")
    print(f"[INFO] {_ts()} Filtro de contenido CLI: {build_content_mode_label(content_mode)}")

    run_scrape_jobs(
        source_jobs,
        shared_total_limit=shared_total_limit,
        date_from=date_from,
        date_to=date_to,
        scheduler_all_new=scheduler_all_new,
        content_mode=content_mode,
    )


if __name__ == "__main__":
    main()
