from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from pathlib import Path
import pandas as pd

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
        None, ge=0, le=1, description="Minimum absolute edge vs consensus."
    ),
    limit: int = Query(50, ge=1, le=200, description="Max rows to return."),
    source: str = Query(
        "fullgame",
        pattern="^(fullgame|firsthalf)$",
        description="fullgame -> h2h file; firsthalf -> first-half/F5 file",
    ),
):
    """
    Returns rows where a book deviates from the event consensus (median of books).
    Defaults: fullgame=1.5% edge, firsthalf=2.0% edge.
    """
    try:
        # dynamic default
        edge = 0.015 if source == "fullgame" else 0.020
        if min_abs_edge is None:
            min_abs_edge = edge

        df = _load_df(source)

        # consensus: median home no-vig across books per event
        cons = (
            df.groupby("event_id", as_index=False)["home_q_novig"]
            .median()
            .rename(columns={"home_q_novig": "consensus_home_q"})
        )
        merged = df.merge(cons, on="event_id", how="left")
        merged["edge_vs_consensus"] = merged["consensus_home_q"] - merged["home_q_novig"]

        # sort by abs(edge) desc
        merged = merged.reindex(
            merged["edge_vs_consensus"].abs().sort_values(ascending=False).index
        )

        out: List[Dict[str, Any]] = []
        for row in merged.itertuples():
            if abs(row.edge_vs_consensus) < float(min_abs_edge):
                continue
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
            if len(out) >= limit:
                break

        return JSONResponse(content={"picks": out})
    except FileNotFoundError as e:
        # File missing after deploy; clear message
        return JSONResponse(content={"picks": [], "note": str(e)})
    except Exception as e:
        # Never return None; surface a safe error note
        return JSONResponse(content={"picks": [], "error": f"{e.__class__.__name__}: {e}"})
