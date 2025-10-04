from fastapi import FastAPI, Query
from pydantic import BaseModel
from pathlib import Path
import pandas as pd

app = FastAPI(title="Smart Bets", version="1.0.0")

DATA_DIR = Path("data")
PROCESSED = DATA_DIR / "processed"

class PicksResponse(BaseModel):
    picks: list

def _load_csv_safe(path: Path) -> pd.DataFrame | None:
    try:
        if not path.exists() or path.stat().st_size == 0:
            return None
        return pd.read_csv(path)
    except Exception:
        return None

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/picks_live", response_model=PicksResponse)
def picks_live(
    source: str = Query("fullgame"),  # "fullgame" or "firsthalf"
    min_abs_edge: float = Query(0.02, ge=0.0, le=1.0),
    limit: int = Query(20, ge=1, le=500),
):
    if source == "firsthalf":
        path = PROCESSED / "market_baselines_firsthalf.csv"
    else:
        path = PROCESSED / "market_baselines_h2h.csv"

    df = _load_csv_safe(path)
    if df is None or df.empty:
        note = f"Missing or empty file: {path.name}"
        return {"picks": [], "note": note}

    required = {"event_id", "sport_key", "home_team", "away_team", "book_key", "home_q_novig", "consensus_home_q"}
    if not required.issubset(df.columns):
        missing = list(required - set(df.columns))
        return {"picks": [], "note": f"CSV missing required columns: {missing}"}

    df = df.copy()
    df["edge_vs_consensus"] = df["home_q_novig"] - df["consensus_home_q"]
    df = df[df["edge_vs_consensus"].abs() >= min_abs_edge]
    df = df.sort_values(by="edge_vs_consensus", ascending=False)

    out = df[["event_id","sport_key","home_team","away_team","book_key","home_q_novig","consensus_home_q","edge_vs_consensus"]] \
            .head(limit).to_dict(orient="records")

    return {"picks": out}

from .live import picks_live_router
from .admin import admin_router

app.include_router(picks_live_router)
app.include_router(admin_router)
