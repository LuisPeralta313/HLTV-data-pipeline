"""
Pydantic models for raw match result data extracted from HLTV /results.
These represent the raw layer — minimal cleaning, no business logic.
"""

from __future__ import annotations

from datetime import date
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class EventTier(str, Enum):
    MAJOR = "Major"
    S_TIER = "S-tier"
    A_TIER = "A-tier"
    OTHER = "Other"


class MatchFormat(str, Enum):
    BO1 = "bo1"
    BO3 = "bo3"
    BO5 = "bo5"
    UNKNOWN = "unknown"


class RawMatchResult(BaseModel):
    """
    One row per map played. For bo3/bo5 series the match_id is shared
    across all maps; map_index disambiguates them.
    On the /results listing page HLTV shows only the series-level score,
    so map and map_index are None until the individual match page is scraped.
    """

    # Identifiers
    match_id: int = Field(..., description="Numeric ID from HLTV URL, e.g. 2378573")
    match_url: str = Field(..., description="Full HLTV URL to the match page")

    # Teams & score
    team1_name: str
    team2_name: str
    team1_score: Optional[int] = None   # maps won in the series
    team2_score: Optional[int] = None

    # Match metadata
    match_date: Optional[date] = None
    event_name: str
    event_tier: EventTier = EventTier.OTHER
    match_format: MatchFormat = MatchFormat.UNKNOWN

    # Map-level (populated in a later scraping stage)
    map_name: Optional[str] = None
    map_index: Optional[int] = None     # 1-based position in the series

    # Housekeeping
    scraped_at: Optional[str] = None    # ISO-8601 timestamp

    @field_validator("match_id", mode="before")
    @classmethod
    def coerce_match_id(cls, v):
        return int(v)

    @field_validator("team1_score", "team2_score", mode="before")
    @classmethod
    def coerce_score(cls, v):
        if v is None or v == "":
            return None
        try:
            return int(v)
        except (ValueError, TypeError):
            return None

    class Config:
        use_enum_values = True
