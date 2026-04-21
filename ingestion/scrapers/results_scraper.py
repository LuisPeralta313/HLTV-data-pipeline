"""
Scraper for https://www.hltv.org/results

Extracts the match results listing (one entry per series) and saves
each page's results to a date-partitioned Parquet file under raw_layer/.

CLI usage:
    python -m ingestion.scrapers.results_scraper --pages 3
    python -m ingestion.scrapers.results_scraper --pages 1 --headless false
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import pandas as pd
from bs4 import BeautifulSoup, Tag
from dateutil import parser as dateutil_parser
from loguru import logger

# Project root on sys.path so relative imports work when run as __main__
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ingestion.utils.http_client import HLTVClient
from ingestion.schemas.match import RawMatchResult, EventTier, MatchFormat


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "https://www.hltv.org"
RESULTS_URL = f"{BASE_URL}/results"
RESULTS_PER_PAGE = 100          # HLTV uses offset=0, 100, 200 …
RAW_LAYER_PATH = Path(__file__).resolve().parents[2] / "raw_layer" / "matches"


# ---------------------------------------------------------------------------
# Tier detection
# ---------------------------------------------------------------------------

_STAR_TO_TIER: dict[int, EventTier] = {
    0: EventTier.A_TIER,
    1: EventTier.S_TIER,
    2: EventTier.S_TIER,
    3: EventTier.MAJOR,    # 3 stars = Major (e.g. IEM Rio, PGL Major)
}


def _parse_event_tier(event_cell: Tag) -> EventTier:
    """
    Count i.fa-star elements inside div.stars to derive the event tier.
    If div.stars is absent → 0 stars → A-tier.
    A fire icon (<i class="...fa-fire..."> or "gloffire") signals a Major.
    """
    stars_div = event_cell.find("div", class_="stars")
    if stars_div is None:
        # No stars container → treat as 0 stars
        return EventTier.A_TIER

    # Majors use a fire/gloffire icon instead of stars
    if stars_div.find("i", class_=re.compile(r"fa-fire|gloffire")):
        return EventTier.MAJOR

    # Count filled stars: class="fa-star" (NOT "fa-star-o" which is the empty variant)
    filled = stars_div.find_all("i", class_="fa-star")
    return _STAR_TO_TIER.get(len(filled), EventTier.OTHER)


# ---------------------------------------------------------------------------
# Format detection
# ---------------------------------------------------------------------------

def _parse_format(result_div: Tag) -> MatchFormat:
    """
    Look for the map/format cell using two selectors in priority order:
      1. div.map.map-text  (element that has BOTH classes)
      2. div.map-text      (fallback — single class)
    Text values: 'bo1', 'bo3', 'bo5', or a map name like 'de_dust2' (→ bo1).
    """
    map_cell = (
        result_div.select_one("div.map.map-text")
        or result_div.select_one("div.map-text")
    )
    if map_cell is None:
        return MatchFormat.UNKNOWN

    text = map_cell.get_text(strip=True).lower()
    if "bo5" in text:
        return MatchFormat.BO5
    if "bo3" in text:
        return MatchFormat.BO3
    if "bo1" in text:
        return MatchFormat.BO1
    # A specific map name implies a bo1 series
    if text.startswith("de_") or text.startswith("cs_"):
        return MatchFormat.BO1
    return MatchFormat.UNKNOWN


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------

def _parse_hltv_date(raw: str) -> Optional[date]:
    """
    Parse the text from div.standard-headline into a date.

    Examples of raw strings HLTV produces:
      'Results for April 16th 2026'  → date(2026, 4, 16)
      'featured'                      → None  (no date for featured block)

    dateutil.parser with fuzzy=True silently ignores unknown tokens
    ('Results', 'for', ordinal suffixes) and extracts the date parts.
    """
    clean = raw.strip().lower()
    if not clean or clean == "featured":
        return None
    try:
        return dateutil_parser.parse(raw, fuzzy=True).date()
    except (ValueError, OverflowError):
        logger.warning("Could not parse date string: '{}'", raw)
        return None


# ---------------------------------------------------------------------------
# Core HTML parsing
# ---------------------------------------------------------------------------

def _parse_results_page(html: str, scraped_at: str) -> list[RawMatchResult]:
    """
    Parse a rendered /results HTML page and return a list of RawMatchResult.

    Actual HLTV HTML structure (confirmed from dump):

    <div class="results-all">
      <div class="results-sublist">
        <div class="standard-headline">Results for April 16th 2026</div>
        <!-- OR no headline at all for the "featured" block -->
        <div class="result-con">
          <a class="a-reset" href="/matches/2378573/...">
            <div class="result">
              <div class="team-cell"><div class="team">Navi</div></div>
              <div class="result-score">
                <span class="score-won">2</span>
                -
                <span class="score-lost">1</span>
              </div>
              <div class="team-cell"><div class="team">Vitality</div></div>
              <div class="map map-text">bo3</div>   <!-- or div.map-text -->
              <div class="event">
                <div class="stars"><i class="fa fa-star"></i></div>
                <span class="event-name">ESL Pro League S19</span>
              </div>
            </div>
          </a>
        </div>
      </div>
    </div>
    """
    soup = BeautifulSoup(html, "lxml")
    result_cons = soup.select("div.result-con")
    logger.debug(f"DEBUG _parse_results_page: found {len(result_cons)} div.result-con in received HTML")
    results: list[RawMatchResult] = []

    all_containers = soup.find_all("div", class_="results-all")
    if not all_containers:
        logger.error("Could not find any <div class='results-all'> — page structure may have changed")
        return results

    sublists = [s for c in all_containers for s in c.find_all("div", class_="results-sublist")]
    logger.debug(
        "results-all containers: {} | total sublists: {}",
        len(all_containers),
        len(sublists),
    )

    current_date: Optional[date] = None

    for i, sublist in enumerate(sublists):

        # --- Date header (div.standard-headline) ---
        # If absent, this is the "featured" block → date stays None
        headline = sublist.find(class_="standard-headline")  # <span> not <div>
        headline_text = headline.get_text(strip=True) if headline else "featured"
        current_date = _parse_hltv_date(headline_text)
        cons_in_sublist = sublist.find_all("div", class_="result-con")
        logger.debug(
            "Sublist[{}] headline='{}' date={} | result-con inside: {}",
            i, headline_text, current_date, len(cons_in_sublist),
        )

        # --- Match rows ---
        for result_con in sublist.find_all("div", class_="result-con"):
            try:
                record = _parse_single_result(result_con, current_date, scraped_at)
                if record:
                    results.append(record)
            except Exception as exc:
                logger.warning("Skipping malformed result-con: {}", exc)

    return results


def _parse_single_result(
    result_con: Tag,
    match_date: Optional[date],
    scraped_at: str,
) -> Optional[RawMatchResult]:
    """
    Extract one RawMatchResult from a single result-con <div>.

    Actual DOM (confirmed from live dump):
      result-con > a.a-reset > div.result > table > tbody > tr
        td.team-cell  > div.line-align.team1 > div.team[.team-won]
        td.result-score > span.score-won + span.score-lost
        td.team-cell  > div.line-align.team2 > div.team
        td.event      > span.event-name
        td.star-cell  > div.map-and-stars > div.stars + div.map.map-text
    """
    # --- Match URL and ID ---
    link: Optional[Tag] = result_con.find("a", class_="a-reset")
    if link is None:
        return None
    href = link.get("href", "")
    m = re.search(r"/matches/(\d+)/", href)
    if not m:
        return None
    match_id = int(m.group(1))
    match_url = BASE_URL + href

    result_div = link.find("div", class_="result")
    if result_div is None:
        return None

    # --- Teams: <td class="team-cell"> (not div) ---
    team_cells = result_div.find_all(class_="team-cell")
    if len(team_cells) < 2:
        return None
    team1_el = team_cells[0].find(class_="team")
    team2_el = team_cells[1].find(class_="team")
    if not team1_el or not team2_el:
        return None
    team1_name = team1_el.get_text(strip=True)
    team2_name = team2_el.get_text(strip=True)

    # --- Score: <td class="result-score"> (not div) ---
    score_cell = result_div.find(class_="result-score")
    team1_score, team2_score = None, None
    if score_cell:
        won_span  = score_cell.find("span", class_="score-won")
        lost_span = score_cell.find("span", class_="score-lost")
        # score-won is always first span; winner is always team1 in HLTV listing
        if won_span and lost_span:
            team1_score = won_span.get_text(strip=True)
            team2_score = lost_span.get_text(strip=True)

    # --- Format: div.map.map-text inside td.star-cell > div.map-and-stars ---
    match_format = _parse_format(result_div)

    # --- Event name: <td class="event"> (not div) ---
    event_cell = result_div.find(class_="event")
    event_name = ""
    if event_cell:
        name_span = event_cell.find("span", class_="event-name")
        event_name = name_span.get_text(strip=True) if name_span else ""

    # --- Tier: stars live in td.star-cell, not in td.event ---
    star_cell = result_div.find(class_="star-cell")
    event_tier = _parse_event_tier(star_cell) if star_cell else EventTier.A_TIER

    return RawMatchResult(
        match_id=match_id,
        match_url=match_url,
        team1_name=team1_name,
        team2_name=team2_name,
        team1_score=team1_score,
        team2_score=team2_score,
        match_date=match_date,
        event_name=event_name,
        event_tier=event_tier,
        match_format=match_format,
        scraped_at=scraped_at,
    )


# ---------------------------------------------------------------------------
# Parquet writer
# ---------------------------------------------------------------------------

def _save_to_parquet(records: list[RawMatchResult], page_offset: int) -> Path:
    """
    Persist a list of RawMatchResult to a date-partitioned Parquet file.
    Filename encodes the scrape timestamp and page offset for idempotent reruns.
    """
    RAW_LAYER_PATH.mkdir(parents=True, exist_ok=True)

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"results_offset{page_offset:04d}_{ts}.parquet"
    out_path = RAW_LAYER_PATH / filename

    df = pd.DataFrame([r.model_dump() for r in records])
    df.to_parquet(out_path, index=False, engine="pyarrow")
    logger.info("Saved {} rows → {}", len(df), out_path)
    return out_path


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def scrape_results(
    n_pages: int = 1,
    headless: bool = True,
    start_offset: int = 0,
) -> list[Path]:
    """
    Scrape *n_pages* pages of HLTV results (100 matches each) starting at
    *start_offset* and save each page to its own Parquet file.

    Returns a list of Parquet file paths that were written.
    """
    written_files: list[Path] = []

    with HLTVClient(headless=headless) as client:
        for page_num in range(n_pages):
            offset = start_offset + page_num * RESULTS_PER_PAGE
            url = f"{RESULTS_URL}?offset={offset}" if offset > 0 else RESULTS_URL

            logger.info("Scraping page {} / {} — offset={}", page_num + 1, n_pages, offset)
            html = client.get(url, wait_for_selector="div.result-con")

            scraped_at = datetime.utcnow().isoformat()
            records = _parse_results_page(html, scraped_at)

            if not records:
                logger.warning("No records parsed on page {} — stopping early", page_num + 1)
                break

            logger.info("Parsed {} match results from offset={}", len(records), offset)
            path = _save_to_parquet(records, offset)
            written_files.append(path)

    return written_files


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape HLTV match results")
    parser.add_argument(
        "--pages", type=int, default=1,
        help="Number of result pages to scrape (100 matches each)"
    )
    parser.add_argument(
        "--offset", type=int, default=0,
        help="Starting offset (default 0 = most recent)"
    )
    parser.add_argument(
        "--headless", type=lambda x: x.lower() != "false", default=True,
        help="Run browser headless (default true). Pass 'false' to see the browser."
    )
    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="INFO")
    logger.add(
        Path(__file__).resolve().parents[2] / "logs" / "results_scraper.log",
        rotation="10 MB",
        level="DEBUG",
    )

    files = scrape_results(
        n_pages=args.pages,
        headless=args.headless,
        start_offset=args.offset,
    )
    print(f"\nDone. {len(files)} Parquet file(s) written:")
    for f in files:
        print(f"  {f}")
