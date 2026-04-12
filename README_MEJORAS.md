# ABSORB – versión mejorada

## Cambios aplicados

1. **Scraping histórico robusto por fecha**
   - Se mantiene el flujo `hoy -> fecha objetivo`.
   - Corte por fecha por fuente.
   - Reuso de payload existente para evitar reprocesamiento.

2. **Idempotencia / no duplicados**
   - Si el shortcode ya existe en cache, disco o registry SQLite, el post se omite o se reutiliza.
   - Se evita volver a descargar/OCR cuando ya existe evidencia previa.

3. **Scheduler mejorado**
   - Modo por intervalo: cada N minutos/horas.
   - Nuevo modo por horarios fijos: `HH:MM, HH:MM, ...` para ejecutar varias veces al día tipo cron.

4. **Logging ampliado**
   - `data_instagram/manual_run.log`: corrida manual.
   - `data_instagram/scheduler.log`: scheduler.
   - `data_instagram/runtime.log`: actividad global del scraper.
   - `data_instagram/events.jsonl`: eventos estructurados JSONL.
   - `data_instagram/web.log`: acciones del dashboard Flask.

5. **Panel web mejorado**
   - Estado del scheduler más claro.
   - Soporte para configurar horarios diarios.
   - Nuevo bloque con log general del scraper.

6. **Pacing configurable**
   - Variable `SCRAPER_DELAY_FACTOR` para acelerar o desacelerar pausas.
   - Variable `SCRAPER_BEHAVIOR_PROFILE` con perfiles `fast`, `balanced`, `conservative`.

## Nota importante sobre “comportamiento humano”

Se mejoró el pacing con jitter y pausas variables, pero **no** se agregaron mecanismos agresivos de evasión, fingerprint spoofing ni bypass deliberado de protecciones de plataforma.

## Variables de entorno útiles

```bash
export SCRAPER_DELAY_FACTOR=0.7
export SCRAPER_BEHAVIOR_PROFILE=balanced
export SCRAPER_HEADLESS=1
```

## Scheduler por horarios fijos

Ejemplo en la web:

- Modo: `Horarios fijos`
- Horarios: `08:00, 13:00, 19:30`

## Validación rápida

```bash
python -m py_compile app.py web.py scheduler.py
```

## Ejecución

```bash
python web.py
```

## Login manual

```bash
python login_instagram.py
```
