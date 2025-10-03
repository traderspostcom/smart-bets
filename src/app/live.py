from fastapi import APIRouter, Query
from pydantic import BaseModel
import pandas as pd
from pathlib import Path
from typing import Optional

picks_live_router = APIRouter()

class LivePick(BaseModel):
    event_id: str
    sport_key: str
    home_team: str
    away_team: str
    book_key: str
    home_q_novig: float
    consensus_home_q: float
    edge_vs_consensus: float

@picks_live_router.get("/picks_live")
def picks_live(
    # We compute the default dynamically based on `source`.
    min_abs_edge: Optional[float] = Query(
        None, ge=0, le=1, description="Minimum absolute edge vs consensus."
    ),
    limit: int = Query(
        50, ge=1, le=200, description="Max rows to return (default 50)."
    ),
    source: str = Query(
        "fullgame",
        pattern="^(fullgame|firsthalf)$",
        description="fullgame -> market_baselines_h2h.csv; firsthalf -> market_baselines_firsthalf.csv",
    ),
):
    """
    Show books deviating from consensus:
      - source=fullgame   -> data/processed/market_baselines_h2h.csv  (default min_abs_edge = 0.015)
      - source=firsthalf  -> data/processed/market_baselines_firsthalf.csv (default min_abs_edge = 0.020)
    """
    # Dynamic default threshold
    if min_abs_edge is None:
        min_abs_edge = 0.015 if source == "fullgame" else 0.020

    path = (
        Path("./data/processed/market_baselines_h2h.csv")
        if source == "fullgame"
        else Path("./data/processed/market_baselines_firsthalf.csv")
    )
    if not path.exists():
        return {"picks": [], "note": f"Missing file: {path}"}

    df = pd.read_csv(path)

    # consensus: median home no-vig across books per
