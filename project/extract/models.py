from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import date


# -----------------------------------------------------------------------------
# Aoedumps schema
# -----------------------------------------------------------------------------


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


# -----------------------------------------------------------------------------
# RelicAPI schema
# -----------------------------------------------------------------------------


class RelicResult(BaseModel):
    code: int
    message: str


class Member(BaseModel):
    profile_id: int
    name: Optional[str] = None
    alias: str
    personal_statgroup_id: Optional[int] = None
    xp: Optional[int] = None
    level: Optional[int] = None
    leaderboardregion_id: Optional[int] = None
    country: str


class StatGroup(BaseModel):
    id: int
    name: Optional[str] = None
    type: Optional[int] = None
    members: List[Member]


class LeaderboardStat(BaseModel):
    statgroup_id: int
    leaderboard_id: Optional[int] = None
    wins: Optional[int] = None
    losses: Optional[int] = None
    streak: Optional[int] = None
    disputes: Optional[int] = None
    drops: Optional[int] = None
    rank: int
    ranktotal: Optional[int] = None
    ranklevel: Optional[int] = None
    rating: Optional[int] = None
    regionrank: Optional[int] = None
    regionranktotal: Optional[int] = None
    lastmatchdate: int
    highestrank: Optional[int] = None
    highestranklevel: Optional[int] = None
    highestrating: Optional[int] = None


class RelicResponse(BaseModel):
    result: RelicResult
    statGroups: List[StatGroup]
    leaderboardStats: List[LeaderboardStat]
    rankTotal: Optional[int] = None
