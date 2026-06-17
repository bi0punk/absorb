# absorb

Instagram scraper with OCR capabilities. Automates browsing of Instagram profiles, downloads posts and reels, runs OCR (Spanish) on images, and persists results in SQLite with a web viewer and scheduler.

## Stack

Python 3, Flask, Playwright, OpenCV, Tesseract OCR, SQLite

## Installation

```bash
pip install -r requirements.txt
playwright install chromium
```

## Usage

```bash
# Run the scraper
python app.py

# Launch web UI
python web.py

# Run with scheduler
python scheduler.py
```

## Structure

```
absorb/
├── app.py              # Main scraper
├── web.py              # Flask web viewer
├── scheduler.py        # Periodic scheduler
├── login_instagram.py  # Instagram auth helper
├── requirements.txt
└── templates/
```

## License

MIT
