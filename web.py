#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
web.py  –  Interfaz web para visualizar resultados del scraper de Instagram.
Corre con:  python web.py
Accede en:  http://localhost:5000
"""

import json
import mimetypes
import subprocess
import sys
from pathlib import Path

from flask import (
    Flask,
    Response,
    abort,
    jsonify,
    render_template,
    request,
    send_file,
)

from app import MAX_LIMIT, DEFAULT_LIMIT, parse_profile_sources

# ── Config ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path("data_instagram")
SUMMARY_F = BASE_DIR / "summary_latest_posts.json"
APP_SCRIPT = "app.py"               # no tocar
LOGIN_SCRIPT = "login_instagram.py"  # no tocar

app = Flask(__name__)


# ── Utilidades ──────────────────────────────────────────────────────────────────

def load_summary() -> list:
    """Lee el JSON de resumen; devuelve lista vacía si no existe."""
    if SUMMARY_F.exists():
        try:
            return json.loads(SUMMARY_F.read_text(encoding="utf-8"))
        except Exception:
            return []
    return []


def load_all_posts() -> list:
    """
    Combina el summary con cualquier análisis individual que exista en disco,
    para que la vista muestre todo aunque el summary esté desactualizado.
    """
    by_code: dict = {p["shortcode"]: p for p in load_summary()}

    for json_file in sorted(BASE_DIR.rglob("*.analysis.json")):
        try:
            data = json.loads(json_file.read_text(encoding="utf-8"))
            sc = data.get("shortcode")
            if sc and sc not in by_code:
                by_code[sc] = data
        except Exception:
            pass

    return list(by_code.values())


def get_post(shortcode: str) -> dict | None:
    for post in load_all_posts():
        if post.get("shortcode") == shortcode:
            return post
    return None


# ── Rutas ──────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    posts = load_all_posts()
    has_state = Path("ig_state.json").exists()
    return render_template("index.html", posts=posts, has_state=has_state, max_limit=MAX_LIMIT)


@app.route("/post/<shortcode>")
def post_detail(shortcode: str):
    post = get_post(shortcode)
    if not post:
        abort(404)
    return render_template("detail.html", post=post)


@app.route("/image/<path:rel_path>")
def serve_image(rel_path: str):
    """Sirve imágenes desde data_instagram/ de forma segura."""
    img_path = (BASE_DIR / rel_path).resolve()
    if not str(img_path).startswith(str(BASE_DIR.resolve())):
        abort(403)
    if not img_path.exists():
        abort(404)
    mime, _ = mimetypes.guess_type(str(img_path))
    return send_file(img_path, mimetype=mime or "image/jpeg")


@app.route("/api/posts")
def api_posts():
    return jsonify(load_all_posts())


@app.route("/api/post/<shortcode>")
def api_post(shortcode: str):
    post = get_post(shortcode)
    if not post:
        return jsonify({"error": "not found"}), 404
    return jsonify(post)


@app.route("/run", methods=["POST"])
def run_scraper():
    """
    Lanza app.py en segundo plano y hace stream de stdout/stderr al cliente
    vía Server-Sent Events para mostrar el progreso en tiempo real.
    """
    profile_urls_raw = request.form.get("profile_urls", "").strip()
    profile_url_single = request.form.get("profile_url", "").strip()
    limit_raw = request.form.get("limit", str(DEFAULT_LIMIT)).strip()

    sources = parse_profile_sources([profile_urls_raw, profile_url_single])
    if not sources:
        return jsonify({"error": "Debes ingresar al menos una fuente de Instagram."}), 400

    try:
        limit_int = max(1, min(int(limit_raw), MAX_LIMIT))
    except ValueError:
        limit_int = DEFAULT_LIMIT

    def generate():
        cmd = [sys.executable, APP_SCRIPT, *sources, str(limit_int)]
        yield f"data: ▶ Ejecutando: {' '.join(cmd)}\n\n"
        yield f"data: [INFO] Fuentes recibidas: {sources}\n\n"
        yield f"data: [INFO] Objetivo solicitado: {limit_int} posts nuevos\n\n"

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        if proc.stdout is not None:
            for line in proc.stdout:
                line = line.rstrip()
                if line:
                    yield f"data: {line}\n\n"

        proc.wait()
        if proc.returncode == 0:
            yield "data: ✅ Proceso completado exitosamente.\n\n"
        else:
            yield f"data: ❌ El proceso terminó con código {proc.returncode}.\n\n"
        yield "data: __DONE__\n\n"

    return Response(generate(), mimetype="text/event-stream")


@app.route("/login")
def login_info():
    has_state = Path("ig_state.json").exists()
    return render_template("login.html", has_state=has_state)


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    print("=" * 60)
    print("  Instagram Scraper Dashboard")
    print("  http://localhost:5000")
    print("=" * 60)
    app.run(debug=True, host="0.0.0.0", port=5002, threaded=True)
