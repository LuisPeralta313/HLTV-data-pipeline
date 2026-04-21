"""
Scraper for per-player statistics on HLTV match detail pages.

For each match in raw_layer/matches/*.parquet it:
  1. Navigates to the match_url already stored in the Parquet
  2. Waits for 'table.stats-table' to confirm JS render is complete
  3. Parses the player stats table  ← HTML parser goes here after diagnosis
  4. Saves results to raw_layer/player_stats/player_stats_{match_id}_{ts}.parquet

CLI usage:
    python -m ingestion.scrapers.player_stats_scraper --limit 5
    python -m ingestion.scrapers.player_stats_scraper --limit 5 --headless false
"""

from __future__ import annotations

import argparse
import glob
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
from bs4 import BeautifulSoup, Tag
from loguru import logger

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ingestion.utils.http_client import HLTVClient
from ingestion.schemas.player_stats import RawPlayerStats


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

RAW_MATCHES_GLOB  = str(Path(__file__).resolve().parents[2] / "raw_layer" / "matches" / "*.parquet")
RAW_STATS_PATH    = Path(__file__).resolve().parents[2] / "raw_layer" / "player_stats"
STATS_TABLE_SEL   = "table.totalstats"    # selector to wait for before page.content()


# ---------------------------------------------------------------------------
# Parquet I/O
# ---------------------------------------------------------------------------

def _load_match_urls() -> pd.DataFrame:
    """Read all match Parquets and return a deduplicated match_id + match_url table."""
    files = sorted(glob.glob(RAW_MATCHES_GLOB))
    if not files:
        raise FileNotFoundError(f"No Parquet files found at {RAW_MATCHES_GLOB}")
    df = pd.concat([pd.read_parquet(f, columns=["match_id", "match_url"]) for f in files])
    return df.drop_duplicates("match_id").reset_index(drop=True)


def _already_scraped() -> set[str]:
    """Return set of match_ids that already have a stats Parquet on disk."""
    done: set[str] = set()
    for f in RAW_STATS_PATH.glob("player_stats_*.parquet"):
        # filename pattern: player_stats_{match_id}_{timestamp}.parquet
        parts = f.stem.split("_")
        if len(parts) >= 3:
            done.add(parts[2])
    return done


def _save_to_parquet(records: list[RawPlayerStats], match_id: str) -> Path:
    RAW_STATS_PATH.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out = RAW_STATS_PATH / f"player_stats_{match_id}_{ts}.parquet"
    pd.DataFrame([r.model_dump() for r in records]).to_parquet(out, index=False, engine="pyarrow")
    logger.info("Saved {} player rows → {}", len(records), out)
    return out


# ---------------------------------------------------------------------------
# HTML parser
# ---------------------------------------------------------------------------

def _parse_kd(cell: Tag) -> tuple[int, int]:
    """Parse a 'kd' cell with text like '33-16' → (33, 16)."""
    text = cell.get_text(strip=True)
    if "-" in text:
        parts = text.split("-")
        try:
            return int(parts[0]), int(parts[1])
        except (ValueError, IndexError):
            pass
    return 0, 0


def _parse_player_row(row: Tag, team_name: str, match_id: str, scraped_at: str) -> Optional[RawPlayerStats]:
    """
    Extract one player's stats from a tbody <tr>.

    Verified selectors from live HTML dump:
      nickname : span.player-nick
      kills/deaths : td.kd.traditional-data  → text "33-16"
      adr          : td.adr.traditional-data → float
      kast_pct     : td.kast.traditional-data → "90.0%" strip %
      rating       : td.rating               → float
      assists      : not present in aggregate table → default 0
    """
    nick_span = row.find("span", class_="player-nick")
    if nick_span is None:
        return None
    player_name = nick_span.get_text(strip=True)

    # K-D cell: must have BOTH 'kd' and 'traditional-data' (skip eco-adjusted-data)
    kd_cell = row.find("td", class_=lambda c: c and "kd" in c and "traditional-data" in c)
    kills, deaths = _parse_kd(kd_cell) if kd_cell else (0, 0)

    # ADR
    adr_cell = row.find("td", class_=lambda c: c and "adr" in c and "traditional-data" in c)
    adr = float(adr_cell.get_text(strip=True) or 0) if adr_cell else 0.0

    # KAST — strip trailing %
    kast_cell = row.find("td", class_=lambda c: c and "kast" in c and "traditional-data" in c)
    kast_pct = float(kast_cell.get_text(strip=True).rstrip("%") or 0) if kast_cell else 0.0

    # Rating (HLTV Rating 2.0 / 3.0 — field name unchanged in our schema)
    rating_cell = row.find("td", class_="rating")
    rating = float(rating_cell.get_text(strip=True) or 0) if rating_cell else 0.0

    try:
        return RawPlayerStats(
            match_id=match_id,
            team_name=team_name,
            player_name=player_name,
            kills=kills,
            deaths=deaths,
            assists=0,      # not present in series-aggregate table
            rating=rating,
            kast_pct=kast_pct,
            adr=adr,
            hs_pct=0.0,     # not present in series-aggregate table
            map_context="all",
            scraped_at=scraped_at,
        )
    except Exception as exc:
        logger.warning("Could not build RawPlayerStats for {}: {}", player_name, exc)
        return None


def _parse_team_table(table: Tag, match_id: str, scraped_at: str) -> list[RawPlayerStats]:
    """
    Parse one team's totalstats table.

    Structure (confirmed from live dump):
      tbody
        tr[0]  → header row: a.teamName contains the team name
        tr[1:] → one row per player (5 players)
    """
    tbody = table.find("tbody")
    if tbody is None:
        return []

    rows = tbody.find_all("tr")
    if len(rows) < 2:
        return []

    # Team name from first row
    team_link = rows[0].find("a", class_="teamName")
    team_name = team_link.get_text(strip=True) if team_link else "Unknown"

    records: list[RawPlayerStats] = []
    for row in rows[1:]:           # skip header row
        record = _parse_player_row(row, team_name, match_id, scraped_at)
        if record:
            records.append(record)

    return records


def _parse_player_stats(html: str, match_id: str, scraped_at: str) -> list[RawPlayerStats]:
    """
    Parse a match detail page and return one RawPlayerStats per player.

    Targets div#all-content (series aggregate stats, map_context='all').
    div#all-content contains exactly 2 table.totalstats:
      tables[0] = team 1
      tables[1] = team 2

    Structure confirmed from live HTML dump (see debug/match_detail.html).
    """
    soup = BeautifulSoup(html, "lxml")

    all_content = soup.find("div", id="all-content")
    if all_content is None:
        logger.error("match_id={}: could not find div#all-content", match_id)
        return []

    tables = all_content.find_all("table", class_="totalstats")
    logger.debug("match_id={}: found {} totalstats tables in #all-content", match_id, len(tables))

    if not tables:
        logger.warning("match_id={}: no totalstats tables found", match_id)
        return []

    records: list[RawPlayerStats] = []
    for table in tables:
        records.extend(_parse_team_table(table, match_id, scraped_at))

    logger.debug("match_id={}: parsed {} player rows", match_id, len(records))
    return records


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def scrape_player_stats(
    limit: Optional[int] = None,
    headless: bool = True,
    skip_scraped: bool = True,
) -> list[Path]:
    """
    Iterate match URLs from raw_layer/matches/ and scrape player stats for each.

    Args:
        limit:        Max number of matches to process (None = all).
        headless:     Run Playwright headless.
        skip_scraped: Skip matches that already have a stats file on disk.

    Returns:
        List of Parquet paths written in this run.
    """
    matches = _load_match_urls()
    logger.info("Loaded {} unique matches from Parquet", len(matches))

    if skip_scraped:
        done = _already_scraped()
        matches = matches[~matches["match_id"].astype(str).isin(done)]
        logger.info("{} matches remaining after skipping already-scraped", len(matches))

    if limit:
        matches = matches.head(limit)
        logger.info("Limiting to first {} matches (--limit flag)", limit)

    written: list[Path] = []

    with HLTVClient(headless=headless) as client:
        for _, row in matches.iterrows():
            match_id  = str(row["match_id"])
            match_url = str(row["match_url"])

            logger.info("Scraping player stats for match_id={} — {}", match_id, match_url)
            try:
                html = client.get(match_url, wait_for_selector=STATS_TABLE_SEL)
                scraped_at = datetime.now(timezone.utc).isoformat()
                records = _parse_player_stats(html, match_id, scraped_at)
                if records:
                    written.append(_save_to_parquet(records, match_id))
                else:
                    logger.warning("match_id={} yielded 0 player rows", match_id)
            except NotImplementedError:
                raise
            except Exception as exc:
                logger.error("Failed match_id={}: {}", match_id, exc)
                continue

    return written


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape HLTV per-player match stats")
    parser.add_argument("--limit",    type=int,  default=None,
                        help="Max matches to process (default: all)")
    parser.add_argument("--headless", type=lambda x: x.lower() != "false", default=True,
                        help="Headless browser (default true). Pass 'false' to see it.")
    parser.add_argument("--no-skip",  action="store_true",
                        help="Re-scrape matches even if stats file already exists")
    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="INFO")
    logger.add(
        Path(__file__).resolve().parents[2] / "logs" / "player_stats_scraper.log",
        rotation="10 MB", level="DEBUG",
    )

    files = scrape_player_stats(
        limit=args.limit,
        headless=args.headless,
        skip_scraped=not args.no_skip,
    )
    print(f"\nDone. {len(files)} Parquet file(s) written:")
    for f in files:
        print(f"  {f}")
