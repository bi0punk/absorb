# absorb

Scraper de Instagram con OCR, deduplicación local, soporte multi-fuente y ejecución programada.

## Qué quedó soportado

- Deduplicación local con registro persistente en `data_instagram/registry.sqlite3`.
- Reutilización de análisis existentes para no reprocesar posts viejos.
- Asociación explícita de cada post con su fuente.
- Soporte para múltiples fuentes en una sola corrida.
- Modo por fuente con cuota independiente:
  - ejemplo: fuente 1 = 30, fuente 2 = 50, fuente 3 = 20.
- El flujo procesa una fuente completa y luego pasa a la siguiente.
- Los posts repetidos no descuentan cupo: el scraper sigue avanzando hasta reunir los nuevos reales de esa fuente, si existen suficientes posts visibles.
- Programador periódico para revisar todas las fuentes cada N minutos y extraer solo nuevos.
- Filtro histórico opcional por fecha real del post: desde una fecha, hasta una fecha o rango.
- Log manual persistente en `data_instagram/manual_run.log` y mensaje explícito `post saltado existe` cuando un post ya fue descargado antes.

## Formatos de entrada

### CLI clásico, objetivo global compartido

```bash
python app.py https://www.instagram.com/biobiochile/ https://www.instagram.com/cnnchile/ 20
```

En este modo intenta reunir 20 posts nuevos en total entre todas las fuentes.

### CLI por fuente, con cuotas independientes

```bash
python app.py @biobiochile=30 @cnnchile=50 @latercera=20
```

También acepta URLs completas:

```bash
python app.py https://www.instagram.com/biobiochile/=30 https://www.instagram.com/cnnchile/=50
```

### CLI con filtro histórico por fecha

Desde una fecha a hoy:

```bash
python app.py --date-from 2026-03-01 @biobiochile=30 @cnnchile=20
```

Hasta una fecha específica:

```bash
python app.py --date-to 2026-03-01 @biobiochile=50
```

Rango cerrado:

```bash
python app.py --date-from 2026-02-01 --date-to 2026-03-01 @biobiochile=100
```

## Uso desde la web

```bash
python web.py
```

Luego abre:

```text
http://localhost:5000
```

### Ejecución manual

En el panel lateral puedes pegar líneas como:

```text
@biobiochile=30
@cnnchile=50
@latercera=20
```

Si una línea no trae cuota, usa el valor de **Límite por defecto**.

Además puedes usar **Desde fecha** y **Hasta fecha** para backfill histórico. Si dejas ambos vacíos, trabaja solo con posts nuevos.

### Programador

En el mismo panel puedes:

- definir las fuentes y sus cuotas;
- indicar cada cuántos minutos revisar;
- iniciar el programador;
- detenerlo;
- ver el estado y el log reciente.

Archivos del programador:

- `data_instagram/scheduler_config.json`
- `data_instagram/scheduler_status.json`
- `data_instagram/scheduler.pid`
- `data_instagram/scheduler.log`

## Flujo operativo

1. Lee las fuentes configuradas.
2. Para cada fuente, busca posts visibles y detecta shortcodes.
3. Omite los ya procesados.
4. Sigue haciendo scroll hasta intentar completar la cuota nueva real de esa fuente.
5. Descarga el post nuevo, corre OCR y guarda el análisis.
6. Pasa a la fuente siguiente.
7. Fusiona el resumen sin perder histórico.
8. Registra lo que va haciendo en el log manual o en el log del scheduler.
9. Si el programador está activo, repite ese ciclo cada N minutos.

## Notas

- Si una fuente no alcanza su cuota, normalmente será por falta de posts nuevos visibles o por fallas puntuales de descarga.
- El programador usa la misma lógica del scraper manual, por lo que también respeta deduplicación y asociación por fuente.

## Logs

- Ejecución manual: `data_instagram/manual_run.log`
- Scheduler: `data_instagram/scheduler.log`

Cuando un post ya existe localmente, no se vuelve a descargar y queda registrado como:

```text
[SKIP] post saltado existe -> p:ABC123
```
