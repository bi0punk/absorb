#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Dict, Optional

import cv2
import pytesseract
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

BASE_DIR = Path("data_instagram")
STATE_FILE = "ig_state.json"
OCR_LANG = "spa"
TESSERACT_CONFIG = "--oem 3 --psm 6"


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


def extract_shortcodes_from_profile(profile_url: str, limit: int = 5, headless: bool = True) -> List[Dict]:
    found: Dict[str, Dict] = {}
    max_scrolls = 10

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = build_context(browser)
        page = context.new_page()

        print(f"[INFO] Abriendo perfil: {profile_url}")
        page.goto(profile_url, wait_until="domcontentloaded", timeout=60000)
        time.sleep(3)

        dismiss_cookie_banner(page)

        # Primer intento: esperar que aparezcan links de posts o reels
        try:
            page.wait_for_selector('a[href*="/p/"], a[href*="/reel/"]', timeout=12000)
        except PlaywrightTimeoutError:
            html_path, png_path = save_debug_artifacts(page, "sin_posts_visibles")
            print("[WARN] No aparecieron links /p/ o /reel/ en el timeout inicial.")
            print(f"[WARN] HTML guardado en: {html_path}")
            print(f"[WARN] Screenshot guardado en: {png_path}")

        for scroll_idx in range(max_scrolls):
            hrefs = page.locator('a[href*="/p/"], a[href*="/reel/"]').evaluate_all(
                "(els) => els.map(e => e.getAttribute('href')).filter(Boolean)"
            )

            for href in hrefs:
                m = re.match(
                        r"^(?:https?://(?:www\.)?instagram\.com)?/"
                        r"(?:[^/]+/)?"
                        r"(p|reel)/([A-Za-z0-9_-]+)/?",
                        href
                    )
                if not m:
                    continue

                kind = m.group(1)
                shortcode = m.group(2)

                if shortcode not in found:
                    found[shortcode] = {
                        "kind": kind,
                        "shortcode": shortcode,
                        "href": href,
                    }

            print(f"[INFO] Scroll {scroll_idx+1}/{max_scrolls} -> shortcodes detectados: {len(found)}")

            if len(found) >= limit:
                break

            page.mouse.wheel(0, 3500)
            time.sleep(2)

        if not found:
            html_path, png_path = save_debug_artifacts(page, "resultado_cero")
            print(f"[WARN] Sin resultados. Revisa: {html_path}")
            print(f"[WARN] Sin resultados. Revisa: {png_path}")

        context.close()
        browser.close()

    return list(found.values())[:limit]


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

    print(f"[INFO] Descargando {kind}:{shortcode} con Instaloader...")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        raise RuntimeError(
            f"Instaloader falló para {shortcode}\nSTDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}"
        )

    if result.stdout.strip():
        print(result.stdout.strip())
    if result.stderr.strip():
        print("[WARN] STDERR Instaloader:")
        print(result.stderr.strip())

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


def process_shortcode(kind: str, shortcode: str) -> Dict:
    post_dir = run_instaloader_for_shortcode(kind, shortcode)

    image_path = find_latest_image(post_dir)
    if not image_path:
        raise FileNotFoundError(f"No encontré imagen descargada en {post_dir}")

    caption = read_caption_for_image(image_path)
    ocr = run_ocr(image_path)

    payload = {
        "kind": kind,
        "shortcode": shortcode,
        "post_url": f"https://www.instagram.com/{kind}/{shortcode}/" if kind == "reel" else f"https://www.instagram.com/p/{shortcode}/",
        "image_path": str(image_path),
        "caption": caption,
        "ocr_best": ocr["ocr_best"],
        "merged_text": "\n\n".join([x for x in [caption, ocr["ocr_best"]] if x.strip()]).strip(),
        "preprocessed_image": ocr["preprocessed_image"],
    }

    out_json = post_dir / f"{shortcode}.analysis.json"
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def main():
    if len(sys.argv) < 2:
        print("Uso: python app.py https://www.instagram.com/biobiochile/ 5")
        sys.exit(1)

    profile_url = sys.argv[1].strip()
    limit = int(sys.argv[2]) if len(sys.argv) >= 3 else 5

    BASE_DIR.mkdir(parents=True, exist_ok=True)

    posts = extract_shortcodes_from_profile(profile_url, limit=limit, headless=True)
    if not posts:
        print("[ERROR] No encontré shortcodes visibles en el perfil.")
        print("[ERROR] Haz login una vez con login_instagram.py y vuelve a probar.")
        sys.exit(2)

    print(f"[INFO] Detectados: {[f'{p['kind']}:{p['shortcode']}' for p in posts]}")

    results = []
    for item in posts:
        try:
            payload = process_shortcode(item["kind"], item["shortcode"])
            results.append(payload)
            print(f"[OK] Procesado {item['kind']}:{item['shortcode']}")
        except Exception as e:
            print(f"[ERROR] Falló {item['kind']}:{item['shortcode']} -> {e}")

    summary_path = BASE_DIR / "summary_latest_posts.json"
    summary_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] Resumen guardado en {summary_path}")


if __name__ == "__main__":
    main()