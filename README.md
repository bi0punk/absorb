# absorb

Scraper simple para extraer datos desde perfiles públicos de Instagram.

## Mejoras aplicadas

Se mantuvo el flujo original y se agregaron mejoras de bajo riesgo:

- Deduplicación local con registro persistente en `data_instagram/registry.sqlite3`.
- Si un post ya fue procesado antes, no se vuelve a scrapear ni a pasar por OCR.
- El resumen `summary_latest_posts.json` ahora se fusiona sin perder histórico por sobrescritura.
- El conteo ahora respeta la cantidad de **posts nuevos** solicitados.
- Si aparecen posts repetidos, se saltan y el scraper sigue avanzando hasta reunir los nuevos necesarios, si existen suficientes posts visibles.
- Soporte para **múltiples fuentes** en una sola corrida.
- Entrada manual para la cantidad a scrapear desde la interfaz web.
- Normalización de fuentes: acepta URL completa, `@usuario` o nombre simple.

## Flujo actual

1. Detecta shortcodes visibles del perfil.
2. Revisa si ya existen localmente.
3. Los repetidos no cuentan para la meta de nuevos posts.
4. Sigue haciendo scroll hasta completar la cantidad de nuevos solicitada o hasta agotar lo visible.
5. Si el post es nuevo, descarga, procesa OCR y guarda análisis.
6. Actualiza el resumen consolidado.

## Uso por CLI

Una sola fuente:

```bash
python app.py https://www.instagram.com/biobiochile/ 5
```

Múltiples fuentes:

```bash
python app.py https://www.instagram.com/biobiochile/ https://www.instagram.com/cnnchile/ 20
```

También acepta usuarios simples:

```bash
python app.py @biobiochile @latercera 10
```

## Uso desde la web

```bash
python web.py
```

Luego abre:

```text
http://localhost:5000
```

En el panel de scraping puedes:

- ingresar una o varias fuentes, una por línea;
- definir manualmente la cantidad de posts nuevos;
- ejecutar una corrida consolidada sobre varias cuentas.

## Nota operativa

Si la meta no se cumple exactamente, normalmente será por una de estas razones:

- la cuenta no tiene suficientes posts nuevos visibles;
- Instagram dejó de entregar más enlaces tras varios scrolls;
- falló una descarga puntual del post.
