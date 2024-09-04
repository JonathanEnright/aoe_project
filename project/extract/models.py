from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import date


class WeeklyDump(BaseModel):
    start_date: date
    end_date: Optional[date] = None
    num_matches: int
    num_players: Optional[int] = None
    matches_url: str
    players_url: str
    match_checksum: Optional[str] = None
    player_checksum: Optional[str] = None


class ApiSchema(BaseModel):
    db_dumps: List[WeeklyDump]
    total_matches: Optional[int] = None
    total_players: Optional[int] = None
