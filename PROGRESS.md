# HLTV CS2 Data Pipeline — Estado del Proyecto

**Última actualización:** 2026-04-21  
**Estado:** Pipeline end-to-end completo. Todos los componentes funcionales.

---

## Stack completo

| Componente | Tecnología | Estado |
|---|---|---|
| Scraping | Python + Playwright + BeautifulSoup | ✅ Funcional |
| Raw layer | Parquet (local) | ✅ Funcional |
| Transformaciones | dbt-core + dbt-duckdb | ✅ Funcional |
| Warehouse local | DuckDB (`warehouse/hltv.duckdb`) | ✅ Funcional |
| Warehouse cloud | PostgreSQL en Supabase (Session Pooler) | ✅ Funcional — 2.833 filas |
| Orquestación | Apache Airflow 2.11.2 | ✅ Instalado — DAG `hltv_pipeline` configurado |
| Dashboard | Streamlit + Plotly | ✅ Funcional (localhost:8501) |

---

## Estado por capa

### 1. Raw Layer — Parquet

#### Partidas (`raw_layer/matches/`)
- **300 partidas únicas** scrapeadas de `https://www.hltv.org/results`
- Rango de fechas: **2026-04-05 a 2026-04-16**
- 4 archivos Parquet (3 runs de `--pages 3` + 1 run de `--pages 1`)
- Columnas: `match_id, match_url, team1_name, team2_name, team1_score, team2_score, match_date, event_name, event_tier, match_format, map_name, map_index, scraped_at`

#### Estadísticas de jugadores (`raw_layer/player_stats/`)
- **283 partidas scrapeadas** (2.830 filas aprox) — run completo con `--headless false`
- **17 partidas fallidas permanentemente** — bloqueadas por Cloudflare en ambos intentos
- Columnas: `match_id, player_name, team_name, kills, deaths, assists, adr, kast_pct, rating, hs_pct, map_context, scraped_at`
- `assists` y `hs_pct` son siempre 0 (no están en la vista `all` de HLTV)
- **Nota operativa:** `--headless true` es detectado por Cloudflare → usar siempre `--headless false`

### 2. Staging — dbt (views en DuckDB)

| Modelo | Ubicación | Estado | Tests |
|---|---|---|---|
| `stg_match_results` | `models/staging/` | ✅ | 9/9 PASS |
| `stg_player_stats` | `models/staging/` | ✅ | 6/6 PASS |

**Transformaciones aplicadas en staging:**
- Dedup por `match_id` / `(match_id, player_name)` → `ROW_NUMBER() OVER (...) = 1`
- `event_tier`: `'Other'` → `'a-tier'`, luego `MAX() OVER (PARTITION BY event_name)` para normalizar por torneo
- `match_format`: `'unknown'` → `NULL`
- `match_date`: `COALESCE(match_date, DATE(scraped_at))` para rellenar el bloque "featured"
- `kd_ratio`: `kills::float / NULLIF(deaths, 0)`

### 3. Marts — dbt (tabla en DuckDB)

| Modelo | Ubicación | Estado |
|---|---|---|
| `fct_match_performance` | `models/marts/` | ✅ |

**Grain:** una fila por jugador por partido.  
**JOIN:** `stg_player_stats INNER JOIN stg_match_results ON match_id`  
**Columna clave:** `is_winner BOOLEAN = (team_name = match_winner)`

**Filas actuales:** 2.833 filas · 283 partidas únicas · 10 jugadores por partida.

---

## Lo que falta — en orden de prioridad

### ~~Paso 1 — Scraper de jugadores completo~~ ✅ COMPLETADO (2026-04-21)
283/300 partidas scrapeadas. 17 bloqueadas por Cloudflare permanentemente.

### ~~Paso 2 — Refrescar dbt~~ ✅ COMPLETADO (2026-04-21)
`dbt run` + `dbt test` — 15/15 PASS. `fct_match_performance` tiene 2.833 filas.

### ~~Paso 5 — Streamlit dashboard~~ ✅ COMPLETADO (2026-04-21)
`dashboard/app.py` funcional en `localhost:8501`. 4 secciones: resumen, top jugadores, ganadores vs perdedores, winrate por equipo.

### ~~Paso 3 — Supabase / PostgreSQL~~ ✅ COMPLETADO (2026-04-21)
- Proyecto en Supabase: `kdnayfzqndwveghpdqmu`
- **Usar Session Pooler** (IPv4) — dirección directa es IPv6-only y no funciona desde redes sin IPv6
- Credenciales en `.env` como `SUPABASE_URL` + componentes derivados
- Publicación via `scripts/publish_to_supabase.py` (truncate + bulk insert)
- dbt `--target prod` NO funciona (staging lee Parquet que no existe en PG) — arquitectura: transformar en DuckDB → publicar mart a Supabase
- Dashboard Streamlit lee de Supabase si `SUPABASE_URL` está en `.env`, o de DuckDB como fallback

### ~~Paso 4 — Airflow~~ ✅ COMPLETADO (2026-04-21)
- Airflow 2.11.2 instalado (Python 3.13, constraints-3.12)
- `AIRFLOW_HOME` → `orchestration/airflow_home/`
- DAG `hltv_pipeline` detectado — 3 tasks: `scrape_match_results >> scrape_player_stats >> dbt_run`
- Schedule: `0 9 1,15 * *` (9am los días 1 y 15)
- Usuario admin creado (user: `admin`, pass: `admin`)
- Para arrancar: ejecutar `orchestration/start_airflow.bat` → http://localhost:8080
- **Nota Windows:** Airflow no soporta Windows oficialmente — usar `SequentialExecutor`

### Paso 5 — Streamlit dashboard
```bash
.venv/Scripts/streamlit run dashboard/app.py
```
Archivo a crear: `dashboard/app.py`  
Fuente de datos: conectar a `warehouse/hltv.duckdb` con `duckdb.connect()`

---

## Comandos de referencia rápida

```bash
# Activar entorno
cd E:/ProyectoHLTV
# (en bash) source .venv/Scripts/activate
# (en cmd)  .venv\Scripts\activate

# Scraper partidas (1 página = ~100 partidas)
.venv/Scripts/python -m ingestion.scrapers.results_scraper --pages 3 --headless false

# Scraper jugadores (todas las partidas pendientes)
.venv/Scripts/python -m ingestion.scrapers.player_stats_scraper --headless false

# Scraper jugadores (solo N partidas, para testing)
.venv/Scripts/python -m ingestion.scrapers.player_stats_scraper --limit 5 --headless false

# dbt
cd dbt_project
../.venv/Scripts/dbt run --profiles-dir .
../.venv/Scripts/dbt test --profiles-dir .
../.venv/Scripts/dbt run --select stg_player_stats fct_match_performance --profiles-dir .

# Inspeccionar datos en DuckDB
.venv/Scripts/python -c "
import duckdb
con = duckdb.connect('warehouse/hltv.duckdb')
print(con.execute('SELECT * FROM main_marts.fct_match_performance LIMIT 5').df())
"

# Verificar Parquet de partidas
.venv/Scripts/python -c "
import pandas as pd, glob
df = pd.concat([pd.read_parquet(f) for f in glob.glob('raw_layer/matches/*.parquet')])
print(df.drop_duplicates('match_id').shape, df.match_date.dropna().min(), df.match_date.dropna().max())
"
```

---

## Decisiones técnicas importantes

### Por qué Playwright en lugar de requests
HLTV sirve un JS challenge de Cloudflare en todas las rutas. `requests` recibe 403 siempre. Playwright lanza un Chromium real que resuelve el challenge. Se enmascara `navigator.webdriver` vía `add_init_script` para evitar detección.

### Por qué wait_for_selector antes de page.content()
`page.goto(wait_until="domcontentloaded")` no es suficiente — el contenido de HLTV se renderiza por JS después de la carga inicial. Sin esperar el selector, `page.content()` devuelve el HTML antes del render y BeautifulSoup encuentra 0 elementos.
- Resultados: `wait_for_selector("div.result-con")`
- Jugadores: `wait_for_selector("table.totalstats")`

### Selectores CSS reales de HLTV (verificados contra HTML live)

**Página `/results`:**
```
Contenedor principal : div.results-all          ← hay MÚLTIPLES en la página
Sublist por día      : div.results-sublist       ← dentro de cada results-all
Header de fecha      : span.standard-headline    ← es <span>, NO <div>
  Texto ejemplo      : "Results for April 16th 2026"
Fila de partido      : div.result-con
  Link               : a.a-reset[href]           ← href contiene el match_id
  Estructura interna : div.result > table > tbody > tr
  Equipo 1           : td.team-cell (primero) > div.line-align > div.team
  Equipo 2           : td.team-cell (segundo) > div.line-align > div.team
  Score              : td.result-score > span.score-won + span.score-lost
  Evento             : td.event > span.event-name
  Estrellas/tier     : td.star-cell > div.map-and-stars > div.stars > i.fa-star
    0 estrellas → a-tier
    1-2 estrellas → s-tier
    3 estrellas → major
    i.fa-fire / i.gloffire → major (alternativo)
  Formato            : div.map.map-text (dentro de div.map-and-stars)
```

**Página de detalle de partido (`/matches/{id}/...`):**
```
Stats totales        : div#all-content
  2 tablas por equipo: table.totalstats
    Header equipo    : tbody > tr[0] > a.teamName
    Filas jugador    : tbody > tr[1:]
      Nickname       : span.player-nick
      K-D            : td.kd.traditional-data → texto "33-16"
      ADR            : td.adr.traditional-data → float
      KAST%          : td.kast.traditional-data → "90.0%" strip %
      Rating         : td.rating → float
      (assists/hs%   : NO disponibles en vista agregada)

Stats por mapa       : div#{map_id}-content (ej: div#226622-content)
  Map names          : div.box-headline > div.dynamic-map-name-full
```

### Por qué DuckDB como warehouse local
- Instalación cero (archivo `.duckdb`)
- Lee Parquet nativamente con `read_parquet('glob/*.parquet')`
- `dbt-duckdb` permite transformar directamente sobre los Parquet sin ETL previo
- Para producción se cambia el perfil dbt a `dbt-postgres` apuntando a Supabase

### Estructura de Parquet

**`raw_layer/matches/results_offset{NNNN}_{timestamp}.parquet`**
- Offset en nombre → paginación idempotente
- Timestamp → múltiples runs no sobreescriben
- `stg_match_results` deduplica con `ROW_NUMBER() OVER (PARTITION BY match_id ORDER BY scraped_at DESC)`

**`raw_layer/player_stats/player_stats_{match_id}_{timestamp}.parquet`**
- Un archivo por partido scrapeado
- Skip automático: si existe `player_stats_{match_id}_*.parquet`, no re-scrapeamos

### Tier mapping HLTV
El tier se determina contando `<i class="fa fa-star star">` dentro de `div.stars`. La clase del elemento es `"fa fa-star star"` (3 clases), pero `find_all("i", class_="fa-star")` funciona porque BeautifulSoup hace matching parcial de clases CSS.

---

## Estructura de archivos

```
E:/ProyectoHLTV/
├── .venv/                          # entorno virtual Python
├── ingestion/
│   ├── scrapers/
│   │   ├── results_scraper.py      # ✅ scraper /results
│   │   └── player_stats_scraper.py # ✅ scraper /matches/{id}
│   ├── schemas/
│   │   ├── match.py                # ✅ Pydantic RawMatchResult
│   │   └── player_stats.py         # ✅ Pydantic RawPlayerStats
│   └── utils/
│       └── http_client.py          # ✅ Playwright client (anti-bot)
├── raw_layer/
│   ├── matches/                    # ✅ 300 partidas en Parquet
│   └── player_stats/               # ⚠️  solo 5 partidas
├── dbt_project/
│   ├── dbt_project.yml             # ✅
│   ├── profiles.yml                # ✅ DuckDB local
│   ├── packages.yml                # ✅ dbt_utils
│   └── models/
│       ├── staging/
│       │   ├── stg_match_results.sql   # ✅ view
│       │   ├── stg_player_stats.sql    # ✅ view
│       │   ├── sources.yml
│       │   └── schema.yml              # ✅ 15 tests
│       └── marts/
│           └── fct_match_performance.sql # ✅ table
├── warehouse/
│   └── hltv.duckdb                 # ✅ base de datos DuckDB
├── orchestration/
│   ├── airflow_home/               # ✅ AIRFLOW_HOME (SQLite DB, logs, config)
│   ├── dags/
│   │   └── hltv_pipeline.py        # ✅ DAG: scrape_results >> scrape_players >> dbt_run
│   └── start_airflow.bat           # ✅ Arranca scheduler + webserver
├── dashboard/
│   └── app.py                      # ✅ Streamlit (4 secciones, Plotly)
├── debug/                          # scripts de diagnóstico (no borrar)
│   ├── dump_results_html.py
│   ├── dump_html.py
│   └── dump_match_html.py
└── PROGRESS.md                     # este archivo
```
