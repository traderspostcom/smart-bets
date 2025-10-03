from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from pathlib import Path
import pandas as pd
import os

picks_live_router = APIRouter()

# ---------- Env-configurable defaults ----------
# Change these in Render → Environment → Add Environment Variable
DEF_FULLGAME_EDGE = float(os.getenv("PICKS_FULLGAME_MIN_EDGE", "0.015"))   # 1.5%
DEF_FIRSTHALF_EDGE = float(os.getenv("PICKS_FIRSTHALF_MIN_EDGE", "0.010")) # 1.0%
DEF_LIMIT = int(os.getenv("PICKS_DEFAULT_LIMIT", "50"))                    # max rows
MIN_CONSENSUS_BOOKS = int(os.getenv("PICKS_MIN_CONSENSUS_BOOKS", "1"))    # require >= N books in consensus
MAX_AGE_MINUTES = int(os.getenv("PICKS_MAX_AGE_MINUTES", "0"))            # 0 = ignore freshness

class LivePick(BaseModel):
    event_id: str
    sport_key: str
    home_team: str
    away_team: str
    book_key: str
    home_q_novig: float
    consensus_home_q: float
    edge_vs_consensus: float

def _load_df(source: str) -> pd.DataFrame:
    path = (
        Path("./data/processed/market_baselines_h2h.csv")
        if source == "fullgame"
        else Path("./data/processed/market_baselines_firsthalf.csv")
    )
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    return pd.read_csv(path)

@picks_live_router.get("/picks_live")
def picks_live(
    min_abs_edge: Optional[float] = Query(
        None, ge=0, le=1, description="Minimum absolute edge vs consensus. If omitted, uses env-configured default."
    ),
    limit: int = Query(
        None, ge=1, le=200, description="Max rows to return. If omitted, uses env-configured default."
    ),
    source: str = Query(
        "fullgame",
        pattern="^(fullgame|firsthalf)$",
        description="fullgame -> h2h file; firsthalf -> first-half/F5 file",
    ),
):
    """
    Returns rows where a book deviates from the event consensus (median of books).

    Defaults (override via env on Render):
      - PICKS_FULLGAME_MIN_EDGE   (default 0.015)
      - PICKS_FIRSTHALF_MIN_EDGE  (default 0.010)
      - PICKS_DEFAULT_LIMIT       (default 50)
      - PICKS_MIN_CONSENSUS_BOOKS (default 1)
      - PICKS_MAX_AGE_MINUTES     (default 0 = ignore)
    """
    try:
        # dynamic defaults from env
        edge_default = DEF_FULLGAME_EDGE if source == "fullgame" else DEF_FIRSTHALF_EDGE
        edge_thresh = float(edge_default if min_abs_edge is None else min_abs_edge)
        row_limit = int(DEF_LIMIT if limit is None else limit)

        df = _load_df(source)

        # Optional freshness filter (only if a last_update column is present)
        if MAX_AGE_MINUTES > 0 and "last_update" in df.columns:
            try:
                ts = pd.to_datetime(df["last_update"], errors="coerce", utc=True)
                cutoff = pd.Timestamp.utcnow() - pd.Timedelta(minutes=MAX_AGE_MINUTES)
                df = df[ts >= cutoff]
            except Exception:
                # If parsing fails, just skip freshness filtering silently
                pass

        # Require a minimum number of books contributing to the event consensus
