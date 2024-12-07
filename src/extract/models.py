from pydantic import BaseModel, model_validator
from typing import List, Optional
from datetime import date, timedelta, datetime
import logging

logger = logging.getLogger(__name__)

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

    @model_validator(mode="after")
    def validate_and_get_message(self) -> "WeeklyDump":
        if self.num_matches == 0:
            msg = f"File {self.start_date} is present but is empty!"
            logger.warning(msg)
        return self


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
    profile_id: Optional[int | float] = None
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
    statGroups: List[StatGroup]
    leaderboardStats: List[LeaderboardStat]


# -------------------------------Aoestats .parquet schemas


class Players(BaseModel):
    civ: str
    game_id: int
    match_rating_diff: float
    new_rating: int
    old_rating: int
    profile_id: Optional[int | float] = None
    replay_summary_raw: Optional[str | float] = None
    team: int
    winner: bool


class Matches(BaseModel):
    avg_elo: float
    duration: timedelta
    game_id: int
    game_speed: str
    game_type: str
    irl_duration: timedelta
    leaderboard: str
    map: str
    mirror: bool
    num_players: int
    patch: int
    raw_match_type: Optional[int | float] = None
    replay_enhanced: bool
    started_timestamp: datetime
    starting_age: str
    team_0_elo: float
    team_1_elo: float
