#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Dict, List

BASE_DIR = Path("data_instagram")
CONFIG_FILE = BASE_DIR / "scheduler_config.json"
STATUS_FILE = BASE_DIR / "scheduler_status.json"
PID_FILE = BASE_DIR / "scheduler.pid"
LOG_FILE = BASE_DIR / "scheduler.log"
APP_SCRIPT = Path(__file__).with_name("app.py")
APP_TZ = ZoneInfo("America/Santiago")

stop_requested = False


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def future_iso(minutes: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(minutes=minutes)).replace(microsecond=0).isoformat()


def interval_config_to_minutes(config: Dict) -> int:
    unit = str(config.get("interval_unit", "minutes") or "minutes").strip().lower()
    value = int(config.get("interval_value", config.get("interval_minutes", 15)) or 15)
    value = max(1, value)
    if unit == "hours":
        return value * 60
    return value




def parse_daily_times(raw_values) -> list[str]:
    values = raw_values or []
    if isinstance(values, str):
        values = [part.strip() for part in values.split(',') if part.strip()]
    normalized = []
    for raw in values:
        token = str(raw or '').strip()
        if not token:
            continue
        parts = token.split(':', 1)
        if len(parts) != 2:
            continue
        try:
            hh = int(parts[0])
            mm = int(parts[1])
        except Exception:
            continue
        if 0 <= hh <= 23 and 0 <= mm <= 59:
            normalized.append(f"{hh:02d}:{mm:02d}")
    return sorted(set(normalized))


def now_local() -> datetime:
    return datetime.now(APP_TZ)


def future_iso_from_dt(dt_obj: datetime) -> str:
    return dt_obj.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def next_daily_run_at(daily_times: list[str]) -> datetime:
    times = parse_daily_times(daily_times)
    current = now_local()
    today = current.date()
    candidates = []
    for token in times:
        hh, mm = map(int, token.split(':'))
        candidates.append(datetime(today.year, today.month, today.day, hh, mm, tzinfo=APP_TZ))
    future_candidates = [dt for dt in candidates if dt > current]
    if future_candidates:
        return min(future_candidates)
    first_hh, first_mm = map(int, times[0].split(':'))
    tomorrow = today + timedelta(days=1)
    return datetime(tomorrow.year, tomorrow.month, tomorrow.day, first_hh, first_mm, tzinfo=APP_TZ)


def wait_until(target_dt: datetime) -> bool:
    while not stop_requested:
        remaining = (target_dt - now_local()).total_seconds()
        if remaining <= 0:
            return True
        time.sleep(min(1.0, max(0.1, remaining)))
        config = load_config()
        if not bool(config.get('enabled', False)):
            return False
    return False

def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def load_config() -> Dict:
    return load_json(
        CONFIG_FILE,
        {
            "enabled": False,
            "schedule_mode": "interval",
            "interval_minutes": 15,
            "interval_value": 15,
            "interval_unit": "minutes",
            "daily_times": [],
            "content_mode": "both",
            "source_jobs": [],
            "updated_at": "",
        },
    )


def write_status(**kwargs) -> None:
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    current = load_json(STATUS_FILE, {})
    current.update(kwargs)
    current["updated_at"] = utc_now_iso()
    STATUS_FILE.write_text(json.dumps(current, ensure_ascii=False, indent=2), encoding="utf-8")


def process_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def read_pid() -> int | None:
    try:
        return int(PID_FILE.read_text(encoding="utf-8").strip())
    except Exception:
        return None


def scheduler_running() -> bool:
    pid = read_pid()
    return bool(pid and process_alive(pid))


def cleanup_stale_pid() -> None:
    pid = read_pid()
    if pid and not process_alive(pid):
        PID_FILE.unlink(missing_ok=True)


def handle_signal(signum, frame):
    del signum, frame
    global stop_requested
    stop_requested = True


def source_job_args(source_jobs: List[Dict]) -> List[str]:
    args: List[str] = []
    for job in source_jobs:
        profile_url = str(job.get("profile_url", "")).strip()
        if profile_url:
            args.append(profile_url)
    return args


def append_log(line: str) -> None:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with LOG_FILE.open("a", encoding="utf-8") as fh:
        fh.write(line.rstrip() + "\n")


def run_cycle(config: Dict) -> int:
    jobs = source_job_args(config.get("source_jobs", []))
    if not jobs:
        append_log(f"[{utc_now_iso()}] [WARN] Scheduler sin fuentes configuradas.")
        return 0

    content_mode = str(config.get("content_mode", "both") or "both").strip().lower()
    cmd = [sys.executable, str(APP_SCRIPT), "--content-mode", content_mode, "--scheduler-all-new", *jobs]
    append_log("=" * 80)
    append_log(f"[{utc_now_iso()}] [INFO] Inicio de ciclo programado")
    append_log(f"[{utc_now_iso()}] [INFO] Comando: {' '.join(cmd)}")

    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"
    env["SCRAPER_RUN_CONTEXT"] = "scheduler"
    env["SCRAPER_HEADLESS"] = "1"   # scheduler siempre corre sin ventana

    with LOG_FILE.open("a", encoding="utf-8") as fh:
        proc = subprocess.run(
            cmd,
            stdout=fh,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=str(APP_SCRIPT.parent),
            env=env,
        )

    append_log(f"[{utc_now_iso()}] [INFO] Fin de ciclo programado. exit_code={proc.returncode}")
    return proc.returncode


def main() -> int:
    global stop_requested
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    cleanup_stale_pid()

    if scheduler_running():
        print("Scheduler ya está en ejecución.")
        return 1

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    PID_FILE.write_text(str(os.getpid()), encoding="utf-8")
    write_status(state="starting", pid=os.getpid(), started_at=utc_now_iso())
    append_log(f"[{utc_now_iso()}] [INFO] Scheduler iniciado. pid={os.getpid()}")

    try:
        while not stop_requested:
            config = load_config()
            enabled = bool(config.get("enabled", False))
            schedule_mode = str(config.get("schedule_mode", "interval") or "interval").strip().lower()
            interval_minutes = max(1, interval_config_to_minutes(config))
            interval_value = int(config.get("interval_value", config.get("interval_minutes", 15)) or 15)
            interval_unit = str(config.get("interval_unit", "minutes") or "minutes")
            daily_times = parse_daily_times(config.get("daily_times", []))

            if not enabled:
                write_status(state="disabled", pid=os.getpid(), interval_minutes=interval_minutes, interval_value=interval_value, interval_unit=interval_unit, schedule_mode=schedule_mode, daily_times=daily_times)
                append_log(f"[{utc_now_iso()}] [INFO] Scheduler deshabilitado desde configuración. Saliendo.")
                break

            if schedule_mode == 'daily_times' and daily_times:
                next_run_dt = next_daily_run_at(daily_times)
                write_status(state="waiting", pid=os.getpid(), interval_minutes=interval_minutes, interval_value=interval_value, interval_unit=interval_unit, schedule_mode=schedule_mode, daily_times=daily_times, next_run_at=future_iso_from_dt(next_run_dt), source_jobs=config.get("source_jobs", []))
                append_log(f"[{utc_now_iso()}] [INFO] Scheduler en modo horario fijo. Próxima ejecución local={next_run_dt.isoformat()}")
                if not wait_until(next_run_dt):
                    stop_requested = True
                    break
            else:
                write_status(state="running", pid=os.getpid(), interval_minutes=interval_minutes, interval_value=interval_value, interval_unit=interval_unit, schedule_mode='interval', daily_times=daily_times, last_run_started_at=utc_now_iso(), next_run_at=future_iso(interval_minutes), source_jobs=config.get("source_jobs", []))

            write_status(state="running", pid=os.getpid(), interval_minutes=interval_minutes, interval_value=interval_value, interval_unit=interval_unit, schedule_mode=schedule_mode, daily_times=daily_times, last_run_started_at=utc_now_iso(), source_jobs=config.get("source_jobs", []))
            exit_code = run_cycle(config)

            if schedule_mode == 'daily_times' and daily_times:
                next_run_at = future_iso_from_dt(next_daily_run_at(daily_times))
            else:
                next_run_at = future_iso(interval_minutes)

            write_status(state="sleeping", pid=os.getpid(), interval_minutes=interval_minutes, interval_value=interval_value, interval_unit=interval_unit, schedule_mode=schedule_mode, daily_times=daily_times, last_run_finished_at=utc_now_iso(), last_exit_code=exit_code, next_run_at=next_run_at, source_jobs=config.get("source_jobs", []))

            if schedule_mode == 'daily_times' and daily_times:
                continue

            sleep_seconds = interval_minutes * 60
            for _ in range(sleep_seconds):
                if stop_requested:
                    break
                time.sleep(1)
                config = load_config()
                if not bool(config.get("enabled", False)):
                    stop_requested = True
                    break

        write_status(state="stopped", pid=os.getpid(), stopped_at=utc_now_iso())
        append_log(f"[{utc_now_iso()}] [INFO] Scheduler detenido.")
        return 0
    finally:
        PID_FILE.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
