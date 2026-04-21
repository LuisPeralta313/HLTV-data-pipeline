# HLTV CS2 Data Pipeline

> End-to-end data engineering pipeline that scrapes professional CS2 match statistics from HLTV, transforms them with dbt, and serves them via a Streamlit dashboard backed by Supabase.

![Python](https://img.shields.io/badge/Python-3.13-blue?logo=python)
![dbt](https://img.shields.io/badge/dbt-1.11-orange?logo=dbt)
![Airflow](https://img.shields.io/badge/Airflow-2.11-red?logo=apacheairflow)
![Streamlit](https://img.shields.io/badge/Streamlit-dashboard-brightgreen?logo=streamlit)
![Supabase](https://img.shields.io/badge/Supabase-PostgreSQL-3ecf8e?logo=supabase)

---

## Architecture

```
HLTV.org
    │
    │  Playwright (anti-bot, headless=false)
    ▼
┌─────────────────────────────┐
│   ingestion/scrapers/       │
│   results_scraper.py        │  → raw_layer/matches/*.parquet
│   player_stats_scraper.py   │  → raw_layer/player_stats/*.parquet
└─────────────────────────────┘
    │
    │  dbt-duckdb (local warehouse)
    ▼
┌─────────────────────────────┐
│   dbt_project/models/       │
│   staging/                  │  stg_match_results (view)
│     stg_match_results       │  stg_player_stats  (view)
│     stg_player_stats        │
│   marts/                    │  fct_match_performance (table)
│     fct_match_performance   │  2,833 rows · 283 matches · 878 players
└─────────────────────────────┘
    │                    │
    │  DuckDB (local)    │  scripts/publish_to_supabase.py
    ▼                    ▼
warehouse/           Supabase (PostgreSQL cloud)
hltv.duckdb          fct_match_performance
    │                    │
    └────────┬───────────┘
             │
    dashboard/app.py  (Streamlit + Plotly)
             │
    http://localhost:8501

Orchestration: Apache Airflow DAG hltv_pipeline
  scrape_results → scrape_players → dbt_run
  Schedule: 9 AM on the 1st and 15th of each month
```

---

## Tech Stack

| Tool | Role | Why |
|---|---|---|
| **Playwright** | Scraping | HLTV serves a Cloudflare JS challenge — real Chromium is the only reliable bypass |
| **BeautifulSoup4** | HTML parsing | Lightweight CSS-selector-based parsing after JS render |
| **Parquet** | Raw layer | Columnar, compressed, append-friendly; each scrape run is a separate file |
| **DuckDB** | Local warehouse | Zero-install, reads Parquet natively, perfect dbt target for local dev |
| **dbt-core** | Transformations | Dedup, type casting, KPI columns, 15 data tests — all in SQL |
| **Supabase** | Cloud PostgreSQL | Free-tier managed Postgres with REST API and dashboard |
| **Streamlit** | Dashboard | Rapid prototyping; reads from Supabase or DuckDB via env flag |
| **Apache Airflow** | Orchestration | Scheduled DAG to refresh data on the 1st and 15th of each month |

---

## Quickstart

### Prerequisites
- Python 3.11+
- Git

### 1. Clone and set up environment
```bash
git clone https://github.com/LuisPeralta313/HLTV-data-pipeline.git
cd HLTV-data-pipeline
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

### 2. Configure secrets
```bash
cp .env.example .env
# Edit .env and fill in your SUPABASE_URL (Session Pooler connection string)
```

### 3. Scrape data
```bash
# Step 1 — match results (~300 matches, ~3 min)
python -m ingestion.scrapers.results_scraper --pages 3 --headless false

# Step 2 — player stats (~40-70 min, skip already-scraped automatically)
python -m ingestion.scrapers.player_stats_scraper --headless false
```

> **Note:** Always use `--headless false`. HLTV's Cloudflare protection blocks headless Chromium.

### 4. Transform with dbt
```bash
cd dbt_project
../.venv/Scripts/dbt run --profiles-dir .
../.venv/Scripts/dbt test --profiles-dir .
cd ..
```

### 5. Publish to Supabase
```bash
python scripts/publish_to_supabase.py
```

### 6. Run the dashboard
```bash
streamlit run dashboard/app.py
# Open http://localhost:8501
```

### 7. (Optional) Start Airflow
```bash
# Windows: double-click orchestration/start_airflow.bat
# Then open http://localhost:8080  (admin / admin)
```

---

## Project Structure

```
HLTV-data-pipeline/
├── ingestion/
│   ├── scrapers/
│   │   ├── results_scraper.py       # Scrapes /results pages → Parquet
│   │   └── player_stats_scraper.py  # Scrapes /matches/{id} → Parquet
│   ├── schemas/
│   │   ├── match.py                 # Pydantic model: RawMatchResult
│   │   └── player_stats.py          # Pydantic model: RawPlayerStats
│   └── utils/
│       └── http_client.py           # Playwright client with Cloudflare bypass
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_match_results.sql   # Dedup, type cast, tier normalization
│   │   │   ├── stg_player_stats.sql    # Dedup, KD ratio
│   │   │   ├── sources.yml
│   │   │   └── schema.yml              # 15 data tests
│   │   └── marts/
│   │       └── fct_match_performance.sql  # JOIN: player × match, is_winner flag
│   ├── profiles.yml                 # dev=DuckDB, prod=Supabase (via env vars)
│   └── dbt_project.yml
├── orchestration/
│   ├── dags/
│   │   └── hltv_pipeline.py         # Airflow DAG: scrape → transform → publish
│   └── start_airflow.bat            # Windows launcher for scheduler + webserver
├── scripts/
│   └── publish_to_supabase.py       # DuckDB mart → Supabase bulk upsert
├── dashboard/
│   └── app.py                       # Streamlit: 4 sections, reads Supabase or DuckDB
├── .env.example                     # Template — copy to .env and fill secrets
├── .gitignore
├── PROGRESS.md                      # Development log
└── README.md
```

---

## Current Data

| Metric | Value |
|---|---|
| Matches scraped | 283 / 300 (17 blocked by Cloudflare) |
| Date range | 2026-04-05 → 2026-04-16 |
| Rows in `fct_match_performance` | 2,833 |
| Unique players | 878 |
| Events covered | Major, S-Tier, A-Tier |
| dbt tests passing | 15 / 15 |

---

## Dashboard Sections

1. **General summary** — total matches, unique players, date range, most frequent event
2. **Top 10 players by rating** — min. 3 matches, filterable by event tier
3. **Winners vs Losers** — avg rating, ADR, KAST% side-by-side bar charts
4. **Team winrate** — teams with 5+ matches, sorted by win percentage

---

## Environment Variables

Copy `.env.example` to `.env` and fill in:

| Variable | Description |
|---|---|
| `SUPABASE_URL` | Full Supabase Session Pooler connection string |

> The individual components (`SUPABASE_HOST`, `SUPABASE_USER`, etc.) are derived automatically by `scripts/publish_to_supabase.py` and `dashboard/app.py`.

---

## License

MIT
