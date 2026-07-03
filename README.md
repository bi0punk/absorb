# absorb

[![CI](https://github.com/bi0punk/absorb/actions/workflows/ci.yml/badge.svg)](https://github.com/bi0punk/absorb/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Instagram scraper with OCR capabilities. Automates browsing of Instagram profiles, downloads posts and reels, runs OCR (Spanish) on images, and persists results in SQLite with a web viewer and scheduler.

## Tabla de contenidos

- [Características](#características)
- [Stack](#stack)
- [Arquitectura](#arquitectura)
- [Requisitos](#requisitos)
- [Instalación](#instalación)
- [Uso](#uso)
- [Tests](#tests)
- [CI](#ci)
- [Datos](#datos)
- [Limitaciones y roadmap](#limitaciones-y-roadmap)
- [Licencia](#licencia)

## Características

- Scraping de perfiles/reels de Instagram vía Playwright (navegación por flecha nativa).
- Descarga de posts (imágenes/video) con reintentos y deduplicación por shortcode.
- OCR en español (Tesseract) con preprocesamiento OpenCV.
- Persistencia en SQLite (registro de posts + state por fuente).
- Web viewer Flask para navegar resultados.
- Scheduler periódico (configurable por fuente) con zonas horarias.

## Stack

- **Lenguaje**: Python 3.12+
- **Scraping**: Playwright (Chromium)
- **OCR**: pytesseract + opencv-python (preprocesamiento)
- **Web**: Flask + Jinja2
- **DB**: SQLite
- **Auth**: `login_instagram.py` (helper de login con Playwright)
- **Calidad**: ruff (lint), pytest

## Arquitectura

```
┌──────────────┐   schedule   ┌──────────────┐
│ scheduler.py  │ ───────────► │  app.py        │  (Playwright)
└──────────────┘              │  scraper       │
                              └───────┬────────┘
                                      │ descarga + OCR
                                      ▼
┌──────────────┐   query    ┌──────────────────┐
│  web.py       │ ◄──────── │  absorb/          │  (SQLite registry)
│  Flask viewer │           │  sources/dates/   │
└──────────────┘           │  registry/state   │
                           └──────────────────┘
```

- **absorb/**: paquete core reutilizable (sources, dates, registry, state, utils, constants). Pure Python (sqlite3/json/re), sin deps pesadas → testeable en CI.
- **app.py**: scraper principal (Playwright + OCR).
- **web.py**: Flask viewer.
- **scheduler.py**: orquestador periódico.
- **login_instagram.py**: helper de autenticación.

## Requisitos

- Python 3.12+
- Chromium (vía `playwright install chromium`)
- Tesseract OCR (`tesseract-ocr` + datos `spa`)

## Instalación

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
# Tesseract (Debian/Ubuntu):
sudo apt-get install -y tesseract-ocr tesseract-ocr-spa
```

## Uso

```bash
# Login interactivo (guarda sesión en data_instagram/)
python login_instagram.py

# Scraper (ver --help para opciones de fecha/fuentes)
python app.py <profile_url> [--date-from YYYY-MM-DD] [--until YYYY-MM-DD]

# Web viewer
python web.py

# Scheduler periódico (config en data_instagram/scheduler.json)
python scheduler.py
```

## Tests

```bash
pytest -q
```

120 tests cubren el paquete `absorb/` (sources, dates, registry, state) — pure Python, sin Playwright/OCR/DB externa. Usan SQLite efímera y tmp_path.

## CI

GitHub Actions (`.github/workflows/ci.yml`) sobre Python 3.12:

- **lint** — `ruff check .`
- **test** — instala Flask + pytest y corre los tests del paquete `absorb/` (no requiere Playwright/Tesseract/Chromium).

## Datos

- `data_instagram/` — todos los artefactos runtime (gitignored): SQLite registry, estado por fuente, posts descargados, análisis OCR, sesión de login, config del scheduler.

## Limitaciones y roadmap

- **Limitación**: depende del DOM de Instagram (frágil a cambios de la plataforma).
- **Limitación**: sin tests del scraper end-to-end (requiere sesión real + Chromium).
- **Roadmap**: tests con fixtures HTML para el scraper, exportación CSV/JSON, dashboard de métricas.

## Licencia

MIT — ver [LICENSE](LICENSE).
