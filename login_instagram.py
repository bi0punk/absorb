#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from playwright.sync_api import sync_playwright

STATE_FILE = "ig_state.json"

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()

    page.goto("https://www.instagram.com/", wait_until="domcontentloaded")
    print("\nInicia sesión manualmente en Instagram.")
    print("Cuando ya estés dentro del feed o perfil, presiona ENTER aquí en la terminal...\n")
    input()

    context.storage_state(path=STATE_FILE)
    print(f"[OK] Estado guardado en {STATE_FILE}")

    browser.close()