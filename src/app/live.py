from fastapi import APIRouter, Query
from pydantic import BaseModel
import pandas as pd
from pathlib import Path

picks_live_router = APIRouter()

class LivePick(BaseModel):
    event_id: str
    sport_key: str
    home_team: str
    away_team: str
    book_key: str
    home_q_novig: float
    consensus_home_q: float
    edge_vs_consensus: float  # positive => this book's home prob is lower than consensus (potential value on home)

@picks_live_router.get("/picks_live")
def picks_live(min_abs_edge: float = Query(0.03, ge=0, le=1), limit: int = Query(25, ge=1, le=200)):
    """Surface home moneyline opportunities where a book's no-vig differs from consensus."""
    path = Path("./data/processed/market_baselines_h2h.csv")
    if not path.exists():
        return {"picks": [], "note": "Run odds pull + baseline first."}

    df = pd.read_csv(path)

    # consensus: median home_q_novig across books per event
    cons = df.groupby("event_id", as_index=False)["home_q_novig"].median().rename(columns={"home_q_novig":"consensus_home_q"})
    merged = df.merge(cons, on="event_id", how="left")
    merged["edge_vs_consensus"] = merged["consensus_home_q"] - merged["home_q_novig"]

    # Keep the largest absolute deviations (both positive and negative)
    out = []
    for row in merged.reindex(merged["edge_vs_consensus"].abs().sort_values(ascending=False).index).itertuples():
        if abs(row.edge_vs_consensus) < min_abs_edge:
            continue
        out.append(LivePick(
            event_id=str(row.event_id),
            sport_key=row.sport_key,
            home_team=row.home_team,
            away_team=row.away_team,
            book_key=row.book_key,
            home_q_novig=float(row.home_q_novig),
            consensus_home_q=float(row.consensus_home_q),
            edge_vs_consensus=float(row.edge_vs_consensus),
        ).model_dump())
        if len(out) >= limit:
            break

    return {"picks": out}
