# absorb — Instagram Scraper v2

## Ajustes v2.9

- Los posts ahora se guardan por fuente en directorios tipo `data_instagram/<fuente>/-<shortcode>/`.
- El pipeline quedó dividido en dos etapas reales:
  1. detectar candidato válido, validar fecha y descargarlo de inmediato con imagen + caption/metadata
  2. ejecutar OCR al final, una vez terminadas todas las descargas de todas las fuentes
- Si existía una descarga previa sin OCR completo, se reutiliza y se agenda para la etapa final de OCR.
- La UI ahora permite redimensionar el panel lateral y los cuadros de logs para revisar mejor salidas largas.

## Novedades v2.5

### Web manual simplificada: solo por fecha
La interfaz web ya no mezcla modo por cantidad con modo histórico. Ahora el botón **Ejecutar ahora** siempre trabaja con una fecha objetivo `ddmmaa` y recorre cada fuente desde hoy hacia atrás hasta ese corte.

- No se usa límite por defecto.
- No se muestran cupos en la UI manual.
- Si pegas líneas antiguas como `@medio=30`, la web toma solo la fuente y descarta la cuota.

### Selector de contenido se mantiene
Sigue disponible el selector para:
- `Posts + reels`
- `Solo posts`
- `Solo reels`

### Limpieza interna
- La capa web deja de enviar y almacenar límites que no aplican al modo manual histórico.
- En ejecuciones por fecha o scheduler, el backend sanea las fuentes para trabajar solo con `profile_url`, sin cuotas residuales.

## Novedades v2.3

### Sin límite por cantidad si hay fecha
Cuando activas un filtro temporal (`--until`, `--since`, `--date-from` o `--date-to`), el scraper ignora por completo los cupos y procesa todos los posts que coincidan con el rango temporal. Esto evita que un backfill por fecha quede truncado por un límite numérico.

### Selector de contenido: posts, reels o ambos
Se agregó `--content-mode` en CLI y un selector `Contenido` en la web.

Ejemplos:

```bash
python app.py --content-mode post @biobiochile=20
python app.py --content-mode reel --until 010316 @biobiochile @cnnchile
python app.py --content-mode both @biobiochile=30 @cnnchile=50
```

En la interfaz web y en el scheduler puedes elegir:
- `Posts + reels`
- `Solo posts`
- `Solo reels`

El scheduler respeta esa selección en cada ciclo programado.

---

Scraper de Instagram con OCR, deduplicación local, soporte multi-fuente, backfill histórico y programador periódico orientado a nuevos.

---

## Novedades v2.1

### 🎯 Corte histórico por fecha objetivo
Ahora el flujo principal de backfill histórico queda expresado como un corte claro:

- `--until ddmmaa`
- desde hoy hacia atrás
- una fuente primero y luego la siguiente
- detención inmediata al cruzar la fecha objetivo

Ejemplo:

```bash
python app.py --until 010316 @biobiochile @cnnchile
```

También se mantiene `--since` como alias compatible para no romper automatizaciones previas, pero la interfaz y la documentación ya muestran `--until`, que describe mejor el comportamiento real.
- En la interfaz web, la fecha histórica aplica solo a la ejecución manual. El scheduler sigue funcionando exclusivamente en modo "solo nuevos".

### 🐞 Correcciones incluidas
- Se corrigió un bug de `LIVE_SCREENSHOT_DIR` no definido.
- Se corrigió el corte interno del scraping: al alcanzar el slug conocido o una fecha más antigua que el límite, la exploración de la fuente se detiene de inmediato en lugar de seguir consumiendo posts del mismo lote.


### ⏳ Tiempos de espera configurables
Todos los delays son aleatorios dentro de rangos para simular comportamiento humano y evitar bloqueos.

| Constante | Rango por defecto | Cuándo aplica |
|---|---|---|
| `DELAY_BETWEEN_SOURCES_MIN/MAX` | 8–18 s | Entre cada fuente de scraping |
| `DELAY_BETWEEN_POSTS_MIN/MAX` | 4–10 s | Entre el procesamiento de cada post |
| `DELAY_AFTER_SCROLL_MIN/MAX` | 2.5–5 s | Tras cada scroll en el perfil |
| `DELAY_PROFILE_LOAD_MIN/MAX` | 3–6 s | Tras abrir el perfil (carga inicial) |
| `DELAY_SCROLL_CONTENT_TIMEOUT` | 8 s | Espera activa a que llegue contenido tras scroll |
| `DELAY_AFTER_COOKIE` | 1.5 s | Tras cerrar banner de cookies |
| `RETRY_WAIT_MIN/MAX` | 5–12 s | Entre reintentos de un post fallido |

Puedes ajustar estos valores directamente en la sección de constantes de `app.py`.

### 🔁 Reintentos automáticos por post
`MAX_RETRIES_PER_POST = 2` (configurable). Si un post falla por error transitorio (red, Instaloader, OCR), se reintenta antes de marcarlo como fallido.

### 🔍 Espera activa tras scroll
En lugar de solo `time.sleep(N)` fijo después de cada scroll, el scraper ahora:
1. Hace el scroll.
2. Espera activamente hasta que el DOM muestre más hrefs (polling cada 0.6 s, timeout de 8 s).
3. Agrega una pausa aleatoria adicional sobre esa espera.

Esto evita que el scraper lea los hrefs antes de que Instagram cargue el nuevo lote de posts.

### 📋 Logs detallados
Cada acción loguea exactamente qué está haciendo, con timestamp `HH:MM:SS` y emoji para lectura rápida:

```
[SOURCE] 14:23:01 🌐 URL perfil: https://www.instagram.com/biobiochile/
[SOURCE] 14:23:01 🎯 Objetivo: 30 posts nuevos
[SOURCE] 14:23:01 📜 Scrolls máximos: 120
[NAV]    14:23:04 ✓ Posts iniciales detectados en DOM: 12
[SCROLL] 14:23:07 🔍 Scroll 1/120 | hrefs en DOM: 12 | candidatos: 0/30
[OK]     14:23:08 ✅ Candidato agregado → p:ABC123 | total candidatos: 1/30
[WAIT]   14:23:09 ⏳ Espera entre scrolls en @biobiochile (3.42s)
[SCROLL] 14:23:12 ✓ Nuevos elementos detectados en DOM tras scroll: 24 (antes: 12)
[POST]   14:24:01 📋 Procesando p:ABC123 | URL: https://...
[DOWNLOAD] 14:24:02 ⬇ Descargando p:ABC123 con Instaloader…
[OCR]    14:24:05 🔎 Ejecutando OCR sobre: 2026-03-01_UTC.jpg
[OCR]    14:24:07 ✓ OCR completado | original=142 chars | procesado=198 chars | mejor=198 chars
[OK]     14:24:07 ✅ Post procesado: p:ABC123 | progreso fuente 1/30
[WAIT]   14:24:07 ⏳ Pausa entre posts en @biobiochile (6.18s)
[WAIT]   14:31:22 ⏳ Pausa entre fuente 1 (@biobiochile) y la siguiente (12.4s)
```

### 🔒 Logs de bloqueo/skip explícitos
```
[SKIP] 14:23:10 ♻ Ya procesado (bloqueado) → p:XYZ789
[SKIP] 14:23:11 📂 Caché disponible, reutilizando → p:DEF456
```

### 🍪 Detección de cookie banner con log
```
[BROWSER] 14:23:04 🍪 Banner de cookies detectado, aceptando…
[BROWSER] 14:23:05 ✓ Cookie banner aceptado.
```

### 📊 Separadores visuales de sección
```
──────────────────────────────────────────────────────────────────────
  EXTRAYENDO FUENTE: @biobiochile
──────────────────────────────────────────────────────────────────────
```

---

## Qué sigue soportando (sin cambios de interfaz)

- Deduplicación local con `data_instagram/registry.sqlite3`.
- Reutilización de análisis existentes.
- Soporte multi-fuente en una sola corrida.
- Entrada como `@usuario`, URL completa o `usuario=cuota`.
- Modo histórico `--until ddmmaa` (alias compatible: `--since`).
- Scheduler periódico comparando por slug conocido.
- Logs separados para ejecución manual y programada.

---

## Formatos de entrada (sin cambios)

```bash
# Por fuente con cuotas independientes
python app.py @biobiochile=30 @cnnchile=50 @latercera=20

# Modo global compartido
python app.py https://www.instagram.com/biobiochile/ https://www.instagram.com/cnnchile/ 20

# Histórico desde hoy hacia atrás hasta una fecha objetivo (formato ddmmaa)
python app.py --until 010316 @biobiochile @cnnchile

# Filtrar por contenido
python app.py --content-mode post @biobiochile=30
python app.py --content-mode reel --until 010316 @cnnchile

# Scheduler interno (no usar directamente)
python app.py --content-mode both --scheduler-all-new @biobiochile @cnnchile
```

---

## Ajustar tiempos de espera

Edita las constantes al inicio de `app.py`:

```python
# Entre fuentes de scraping
DELAY_BETWEEN_SOURCES_MIN = 8
DELAY_BETWEEN_SOURCES_MAX = 18

# Entre posts
DELAY_BETWEEN_POSTS_MIN = 4
DELAY_BETWEEN_POSTS_MAX = 10

# Tras cada scroll
DELAY_AFTER_SCROLL_MIN = 2.5
DELAY_AFTER_SCROLL_MAX = 5.0

# Espera activa tras scroll para que cargue DOM
DELAY_SCROLL_CONTENT_TIMEOUT = 8.0

# Reintentos por post
MAX_RETRIES_PER_POST = 2
```

---

## Uso desde la web

```bash
python web.py
# → http://localhost:5000
```

---

## Archivos importantes

| Archivo | Descripción |
|---|---|
| `data_instagram/registry.sqlite3` | Registro de posts procesados |
| `data_instagram/source_state.json` | Último slug visible por fuente |
| `data_instagram/summary_latest_posts.json` | Resumen de todos los posts |
| `data_instagram/manual_run.log` | Log de ejecución manual |
| `data_instagram/scheduler_config.json` | Configuración del scheduler |
| `data_instagram/scheduler_status.json` | Estado actual del scheduler |
| `data_instagram/scheduler.log` | Log del scheduler |


## Ajustes v2.4

- La interfaz web queda únicamente en modo manual por fecha.
- El campo de fuentes en web ya no trabaja con cupos; si vienen, se ignoran.
- Los logs y la preparación interna eliminan cuotas residuales cuando el corte es por fecha o cuando corre el scheduler.


## Ajustes v2.6

- Corregido un fallo en modo histórico por fecha que detenía la corrida al entrar a la primera fuente.
- Corregida la espera de nuevos elementos tras scroll en perfiles de Instagram.
- Mejorado el scroll histórico para seguir cargando posts hasta alcanzar realmente la fecha objetivo.
- En modo por fecha ahora se loguea el avance por día detectado, por ejemplo: `Scrapeando fecha 2026-04-01`.
- Se amplió la tolerancia a scrolls sin cambio en modo histórico para evitar cortes prematuros.


## Mejoras v2.7

- Esperas adicionales y controladas al abrir el navegador y la pestaña.
- Cierre automático de overlays frecuentes de Instagram: cookies, "Ahora no", popups de login y botones X/Cerrar.
- Reintento de detección de posts después de limpiar overlays.
