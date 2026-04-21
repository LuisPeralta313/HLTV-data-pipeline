"""
Reads fct_match_performance from local DuckDB and upserts it into Supabase (PostgreSQL).
Run after: dbt run --target dev
"""

import os
import sys
from pathlib import Path

import duckdb
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

DUCK_DB   = str(Path(__file__).resolve().parents[1] / "warehouse" / "hltv.duckdb")
MART      = "main_marts.fct_match_performance"
PG_TABLE  = "fct_match_performance"
PG_SCHEMA = "public"

# ---------------------------------------------------------------------------
# 1. Read from DuckDB
# ---------------------------------------------------------------------------
print("Reading from DuckDB…")
con = duckdb.connect(DUCK_DB, read_only=True)
df  = con.execute(f"SELECT * FROM {MART}").df()
con.close()
print(f"  {len(df):,} rows loaded")

# ---------------------------------------------------------------------------
# 2. Connect to Supabase
# ---------------------------------------------------------------------------
pg = psycopg2.connect(
    host=os.environ["SUPABASE_HOST"],
    port=int(os.environ["SUPABASE_PORT"]),
    user=os.environ["SUPABASE_USER"],
    password=os.environ["SUPABASE_PW"],
    dbname=os.environ["SUPABASE_DB"],
    sslmode="require",
    connect_timeout=15,
)
pg.autocommit = False
cur = pg.cursor()

# ---------------------------------------------------------------------------
# 3. Create table if not exists (matches DuckDB schema)
# ---------------------------------------------------------------------------
cur.execute(f"""
CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.{PG_TABLE} (
    match_id        BIGINT,
    player_name     TEXT,
    team_name       TEXT,
    kills           INTEGER,
    deaths          INTEGER,
    assists         INTEGER,
    rating          FLOAT,
    kast_pct        FLOAT,
    adr             FLOAT,
    hs_pct          FLOAT,
    map_context     TEXT,
    kd_ratio        FLOAT,
    match_date      DATE,
    event_name      TEXT,
    event_tier      TEXT,
    match_format    TEXT,
    team1_name      TEXT,
    team2_name      TEXT,
    match_winner    TEXT,
    is_winner       BOOLEAN,
    scraped_at      TIMESTAMPTZ,
    PRIMARY KEY (match_id, player_name)
);
""")

# ---------------------------------------------------------------------------
# 4. Truncate and bulk insert (simpler than upsert for full refresh)
# ---------------------------------------------------------------------------
print(f"Truncating {PG_SCHEMA}.{PG_TABLE}…")
cur.execute(f"TRUNCATE TABLE {PG_SCHEMA}.{PG_TABLE}")

cols    = list(df.columns)
rows    = [tuple(r) for r in df.itertuples(index=False, name=None)]
sql     = f"INSERT INTO {PG_SCHEMA}.{PG_TABLE} ({', '.join(cols)}) VALUES %s"

print(f"Inserting {len(rows):,} rows into Supabase…")
execute_values(cur, sql, rows, page_size=500)
pg.commit()

cur.close()
pg.close()
print("Done. Supabase table updated.")
