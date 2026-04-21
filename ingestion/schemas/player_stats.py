"""
Pydantic model for raw per-player statistics extracted from a match detail page.
One row per player per match. For bo3/bo5 series this represents the aggregate
stats shown in the HLTV match page (not per-map breakdown).
"""

from __future__ import annotations

from typing import Optional
from pydantic import BaseModel, Field, field_validator


class RawPlayerStats(BaseModel):
    match_id:   str
    team_name:  str
    player_name: str

    kills:   int
    deaths:  int
    assists: int

    rating:   float = Field(..., description="HLTV Rating 2.0")
    kast_pct: float = Field(..., description="KAST percentage, e.g. 74.5")
    adr:      float = Field(..., description="Average Damage per Round")
    hs_pct:   float = Field(..., description="Headshot percentage, e.g. 42.0")

    map_context: str = "all"   # "all" = series aggregate; future: map name
    scraped_at: str            # ISO-8601 UTC

    @field_validator("kills", "deaths", "assists", mode="before")
    @classmethod
    def coerce_int(cls, v):
        if v is None or v == "" or v == "-":
            return 0
        return int(str(v).strip())

    @field_validator("rating", "kast_pct", "adr", "hs_pct", mode="before")
    @classmethod
    def coerce_float(cls, v):
        if v is None or v == "" or v == "-":
            return 0.0
        # Strip trailing % if present
        return float(str(v).strip().rstrip("%"))

    class Config:
        use_enum_values = True
