from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from pathlib import Path
import pandas as pd
import os

picks_live_router = APIRouter()

# ---------- Env-configurable defaults ----------
DEF_FULLGAME_EDGE = float(os.getenv("PICKS_FULLGAME_MIN_EDGE", "0.015"))   # 1.5%
DEF_FIRSTHALF_EDGE = float(os.getenv("PICKS_FIRSTHALF_MIN_EDGE", "0.010")) # 1.0%
DEF_LIMIT = int(os.getenv("PICKS_DEFAULT_LIMIT", "50"))
MIN_CONSENSUS_BOOKS = int(os.getenv("PICKS_MIN_CONSENSUS_BOOKS", "1"))
MAX_AGE_MINUTES = int(os.getenv("PICKS_MAX_AGE_MINUTES", "0"))  # 0 = ignore

class LivePick(BaseModel):
    event_id: str
    sport_key: str
    home_team: str
    away_team: str
    book_key: str
    home_q_novig: float
    consensus_home_q: float
    edge_vs_consensus: float

def _path_for(source: str) -> Path:
    if source == "fullgame":
        return Path("./data/processed/market_baselines_h2h.csv")
    else:
        return Path("./data/processed/market_baselines_firsthalf.csv")

def _load_df(source: str) -> pd.DataFrame:
    path = _path_for(source)
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    return pd.read_csv(path)

@picks_live_router.get("/picks_live")
def picks_live(
    min_abs_edge: Optional[float] = Query(None, ge=0, le=1, description="Minimum absolute edge vs consensus."),
    limit: Optional[int] = Query(None, ge=1, le=200, description="Max rows to return."),
    source: str = Query("fullgame", pattern="^(fullgame|firsthalf)$", description="Which baseline file to use."),
):
    """
    Returns rows where a book deviates from the event consensus (median of books).
    Defaults come from environment variables on Render.
    """
    # dynamic defaults from env
    edge_default = DEF_FULLGAME_EDGE if source == "fullgame" else DEF_FIRSTHALF_EDGE
    edge_thresh = float(edge_default if min_abs_edge is None else min_abs_edge)
    row_limit = int(DEF_LIMIT if limit is None else limit)

    try:
        df = _load_df(source)
    except FileNotFoundError as e:
        return JSONResponse(content={"picks": [], "note": str(e)})

    # Optional freshness filter if column exists and MAX_AGE_MINUTES > 0
    if MAX_AGE_MINUTES > 0 and "last_update" in df.columns:
        ts = pd.to_datetime(df["last_update"], errors="coerce", utc=True)
        cutoff = pd.Timestamp.utcnow() - pd.Timedelta(minutes=MAX_AGE_MINUTES)
        df = df[ts >= cutoff]

    # Require a minimum number of books contributing to the consensus
    if MIN_CONSENSUS_BOOKS > 1 and "event_id" in df.columns:
        counts = df.groupby("event_id").size()
        keep_ids = set(counts[counts >= MIN_CONSENSUS_BOOKS].index)
        df = df[df["event_id"].isin(keep_ids)]

    if df.empty:
        return JSONResponse(content={"picks": [], "note": "No rows after filters (consensus depth / freshness)."})

    # Build consensus (median home no-vig per event)
    cons = df.groupby("event_id", as_index=False)["home_q_novig"].median()
    cons = cons.rename(columns={"home_q_novig": "consensus_home_q"})
    merged = df.merge(cons, on="event_id", how="left")

    # Edge vs consensus and abs edge
    merged["edge_vs_consensus"] = merged["consensus_home_q"] - merged["home_q_novig"]
    merged["abs_edge"] = merged["edge_vs_consensus"].abs()

    # Filter and sort
    filtered = merged[merged["abs_edge"] >= edge_thresh]
    filtered = filtered.sort_values("abs_edge", ascending=False).head(row_limit)

    out: List[Dict[str, Any]] = []
    for row in filtered.itertuples():
        out.append(
            LivePick(
                event_id=str(row.event_id),
                sport_key=row.sport_key,
                home_team=row.home_team,
                away_team=row.away_team,
                book_key=row.book_key,
                home_q_novig=float(row.home_q_novig),
                consensus_home_q=float(row.consensus_home_q),
                edge_vs_consensus=float(row.edge_vs_consensus),
            ).model_dump()
        )

    return JSONResponse(content={"picks": out})
