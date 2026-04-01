#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
web.py  –  Interfaz web para visualizar resultados del scraper de Instagram.
Corre con:  python web.py
Accede en:  http://localhost:5000
"""

import json
import mimetypes
import os
import signal
import subprocess
import sys
from datetime import datetime, timezone
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

from app import (
    MAX_LIMIT,
    DEFAULT_LIMIT,
    build_source_metadata,
    format_source_job_arg,
    parse_iso_date,
    parse_positive_limit,
    validate_date_range,
    parse_profile_sources,
    parse_source_jobs,
)

# ── Config ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path("data_instagram")
SUMMARY_F = BASE_DIR / "summary_latest_posts.json"
APP_SCRIPT = Path(__file__).with_name("app.py")
LOGIN_SCRIPT = Path(__file__).with_name("login_instagram.py")
SCHEDULER_SCRIPT = Path(__file__).with_name("scheduler.py")
SCHEDULER_CONFIG_FILE = BASE_DIR / "scheduler_config.json"
SCHEDULER_STATUS_FILE = BASE_DIR / "scheduler_status.json"
SCHEDULER_PID_FILE = BASE_DIR / "scheduler.pid"
SCHEDULER_LOG_FILE = BASE_DIR / "scheduler.log"
MANUAL_LOG_FILE = BASE_DIR / "manual_run.log"

app = Flask(__name__)


# ── Utilidades ──────────────────────────────────────────────────────────────────

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def read_json_file(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def normalize_post_source(post: dict) -> dict:
    if not isinstance(post, dict):
        return post

    normalized = dict(post)
    source_meta = build_source_metadata(normalized.get("profile_url", ""))

    if source_meta["profile_url"] and not normalized.get("profile_url"):
        normalized["profile_url"] = source_meta["profile_url"]
    if source_meta["source_username"] and not normalized.get("source_username"):
        normalized["source_username"] = source_meta["source_username"]
    if source_meta["source_label"] and not normalized.get("source_label"):
        normalized["source_label"] = source_meta["source_label"]

    return normalized


def load_summary() -> list:
    if SUMMARY_F.exists():
        try:
            data = json.loads(SUMMARY_F.read_text(encoding="utf-8"))
            return [normalize_post_source(item) for item in data if isinstance(item, dict)]
        except Exception:
            return []
    return []


def load_all_posts() -> list:
    by_code: dict = {p["shortcode"]: p for p in load_summary()}

    for json_file in sorted(BASE_DIR.rglob("*.analysis.json")):
        try:
            data = json.loads(json_file.read_text(encoding="utf-8"))
            sc = data.get("shortcode")
            if sc and sc not in by_code:
                by_code[sc] = normalize_post_source(data)
        except Exception:
            pass

    return list(by_code.values())


def get_post(shortcode: str) -> dict | None:
    for post in load_all_posts():
        if post.get("shortcode") == shortcode:
            return post
    return None


def default_scheduler_config() -> dict:
    return {
        "enabled": False,
        "interval_minutes": 15,
        "default_limit": DEFAULT_LIMIT,
        "source_jobs": [],
        "updated_at": "",
    }


def load_scheduler_config() -> dict:
    config = read_json_file(SCHEDULER_CONFIG_FILE, default_scheduler_config())
    if "interval_minutes" not in config:
        config["interval_minutes"] = 15
    if "default_limit" not in config:
        config["default_limit"] = DEFAULT_LIMIT
    if "source_jobs" not in config:
        config["source_jobs"] = []
    if "enabled" not in config:
        config["enabled"] = False
    return config


def save_scheduler_config(config: dict) -> dict:
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    current = default_scheduler_config()
    current.update(config)
    current["interval_minutes"] = max(1, int(current.get("interval_minutes", 15) or 15))
    current["default_limit"] = parse_positive_limit(current.get("default_limit", DEFAULT_LIMIT))
    current["updated_at"] = utc_now_iso()
    SCHEDULER_CONFIG_FILE.write_text(json.dumps(current, ensure_ascii=False, indent=2), encoding="utf-8")
    return current


def read_scheduler_status() -> dict:
    return read_json_file(SCHEDULER_STATUS_FILE, {})


def read_scheduler_pid() -> int | None:
    try:
        return int(SCHEDULER_PID_FILE.read_text(encoding="utf-8").strip())
    except Exception:
        return None


def process_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def scheduler_is_running() -> bool:
    pid = read_scheduler_pid()
    if pid and process_alive(pid):
        return True
    if SCHEDULER_PID_FILE.exists():
        SCHEDULER_PID_FILE.unlink(missing_ok=True)
    return False


def tail_text_file(path: Path, max_lines: int = 80) -> str:
    if not path.exists():
        return ""
    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        return "\n".join(lines[-max_lines:])
    except Exception:
        return ""


def scheduler_snapshot() -> dict:
    config = load_scheduler_config()
    status = read_scheduler_status()
    running = scheduler_is_running()
    return {
        "running": running,
        "config": config,
        "status": status,
        "log_tail": tail_text_file(SCHEDULER_LOG_FILE, max_lines=80),
    }


def manual_log_tail(max_lines: int = 120) -> str:
    return tail_text_file(MANUAL_LOG_FILE, max_lines=max_lines)


def parse_jobs_from_request() -> tuple[list[dict], int, str, str, str]:
    profile_specs_raw = request.form.get("profile_specs", "").strip()
    profile_urls_raw = request.form.get("profile_urls", "").strip()
    profile_url_single = request.form.get("profile_url", "").strip()
    default_limit = parse_positive_limit(request.form.get("default_limit", DEFAULT_LIMIT), DEFAULT_LIMIT)

    raw_combined = profile_specs_raw or "\n".join([profile_urls_raw, profile_url_single]).strip()
    jobs = parse_source_jobs([raw_combined], default_limit=default_limit)
    date_from = request.form.get("date_from", "").strip()
    date_to = request.form.get("date_to", "").strip()
    if date_from:
        parse_iso_date(date_from)
    if date_to:
        parse_iso_date(date_to)
    return jobs, default_limit, raw_combined, date_from, date_to


def build_run_command_args(source_jobs: list[dict], date_from: str = "", date_to: str = "") -> list[str]:
    args: list[str] = []
    if date_from:
        args.extend(["--date-from", date_from])
    if date_to:
        args.extend(["--date-to", date_to])
    args.extend(format_source_job_arg(job["profile_url"], int(job["limit"])) for job in source_jobs)
    return args


def launch_scheduler_process() -> None:
    with open(os.devnull, "w", encoding="utf-8") as devnull:
        subprocess.Popen(
            [sys.executable, str(SCHEDULER_SCRIPT)],
            stdout=devnull,
            stderr=devnull,
            cwd=str(SCHEDULER_SCRIPT.parent),
            start_new_session=True,
        )


# ── Rutas ──────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    posts = load_all_posts()
    has_state = Path("ig_state.json").exists()
    scheduler = scheduler_snapshot()
    return render_template(
        "index.html",
        posts=posts,
        has_state=has_state,
        max_limit=MAX_LIMIT,
        scheduler=scheduler,
        manual_log_tail=manual_log_tail(),
        today_iso=datetime.now().date().isoformat(),
    )


@app.route("/post/<shortcode>")
def post_detail(shortcode: str):
    post = get_post(shortcode)
    if not post:
        abort(404)
    return render_template("detail.html", post=post)


@app.route("/image/<path:rel_path>")
def serve_image(rel_path: str):
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


@app.route("/api/scheduler_status")
def api_scheduler_status():
    return jsonify(scheduler_snapshot())


@app.route("/api/manual_log")
def api_manual_log():
    return jsonify({"log_tail": manual_log_tail()})


@app.route("/run", methods=["POST"])
def run_scraper():
    try:
        source_jobs, default_limit, raw_combined, date_from, date_to = parse_jobs_from_request()
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    if not source_jobs:
        return jsonify({"error": "Debes ingresar al menos una fuente de Instagram."}), 400

    def generate():
        cmd = [sys.executable, str(APP_SCRIPT), *build_run_command_args(source_jobs, date_from=date_from, date_to=date_to)]
        yield f"data: ▶ Ejecutando: {' '.join(cmd)}\n\n"
        yield f"data: [INFO] Especificación recibida: {raw_combined}\n\n"
        yield f"data: [INFO] Límite por defecto para líneas sin cuota: {default_limit}\n\n"
        yield f"data: [INFO] Fuentes programadas: {source_jobs}\n\n"
        yield f"data: [INFO] Filtro temporal solicitado: desde={date_from or '-'} hasta={date_to or '-'}\n\n"

        env = dict(os.environ)
        env["PYTHONUNBUFFERED"] = "1"
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            cwd=str(APP_SCRIPT.parent),
            env=env,
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


@app.route("/scheduler/start", methods=["POST"])
def scheduler_start():
    try:
        source_jobs, default_limit, _, _, _ = parse_jobs_from_request()
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    if not source_jobs:
        return jsonify({"error": "Debes ingresar al menos una fuente con cuota para programar."}), 400

    interval_minutes = max(1, int(request.form.get("interval_minutes", 15) or 15))
    config = save_scheduler_config(
        {
            "enabled": True,
            "interval_minutes": interval_minutes,
            "default_limit": default_limit,
            "source_jobs": source_jobs,
        }
    )

    if not scheduler_is_running():
        launch_scheduler_process()
        message = "Scheduler iniciado."
    else:
        message = "Scheduler ya estaba corriendo. Configuración actualizada."

    return jsonify({"ok": True, "message": message, "scheduler": scheduler_snapshot(), "config": config})


@app.route("/scheduler/stop", methods=["POST"])
def scheduler_stop():
    config = load_scheduler_config()
    config["enabled"] = False
    save_scheduler_config(config)

    pid = read_scheduler_pid()
    if pid and process_alive(pid):
        try:
            os.kill(pid, signal.SIGTERM)
            message = f"Scheduler detenido. pid={pid}"
        except OSError as exc:
            message = f"No se pudo detener el scheduler: {exc}"
    else:
        SCHEDULER_PID_FILE.unlink(missing_ok=True)
        message = "Scheduler ya estaba detenido."

    return jsonify({"ok": True, "message": message, "scheduler": scheduler_snapshot()})


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
    app.run(debug=True, host="0.0.0.0", port=5000, threaded=True)
