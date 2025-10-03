from fastapi import APIRouter, Query
from pydantic import BaseModel
from pathlib import Path
import os
import pandas as pd
import time

picks_live_router = APIRouter()

# --- Robust path resolution ---
# repo_root = .../smart-bets (two parents up from src/app/live.py)
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PROCESSED = REPO_ROOT / "data" / "processed"

# Optional override via env var (kept for flexibility)
PROCESSED_DIR = Path(os.getenv("PROCESSED_DIR", DEFAULT_PROCESSED))

FULLGAME_FILE = PROCESSED_DIR / "market_baselines_h2h.csv"
FIRSTHALF_FILE = PROCESSED_DIR / "market_baselines_firsthalf.csv"

# Filtering knobs (read from env, but with safe defaults)
MIN_CONSENSUS_BOOKS = int(os.getenv("PICKS_MIN_CONSENSUS_BOOKS", "2"))
LIVE_MAX_AGE_MIN = int(os.getenv("LIVE_MAX_AGE_MIN", os.getenv("LIVE_MAX_AGE_MINUTES", "120")))
BOOKS_ALLOWED = [b.strip() for b in os.getenv("BOOKS_ALLOWED", "").split(",") if b.strip()]

class LivePick(BaseModel):
    event_id: str
    sport_key: str
    home_team: str
    away_team: str
    book_key: str
    home_q_novig: float
    consensus_home_q: float
    edge_vs_consensus: float

class PicksResponse(BaseModel):
    picks: list[LivePick] = []
    note: str | None = None

def _load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    if path.stat().st_size == 0:
        raise FileNotFoundError(f"Empty file: {path}")
    return pd.read_csv(path)

def _apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    # Optional: restrict to allowed books
    if BOOKS_ALLOWED:
        df = df[df["book_key"].isin(BOOKS_ALLOWED)]

    # Consensus depth
    # Expect the baseline to include a per-event consensus count if available.
    # If not, we approximate by counting unique books per (event_id, side).
    if "consensus_depth" in df.columns:
        df = df[df["consensus_depth"] >= MIN_CONSENSUS_BOOKS]
    else:
        depth = (
            df.groupby(["event_id", "home_team", "away_team"])["book_key"]
            .nunique()
            .rename("depth")
            .reset_index()
        )
        df = df.merge(depth, on=["event_id", "home_team", "away_team"], how="left")
        df = df[df["depth"] >= MIN_CONSENSUS_BOOKS]

    # Freshness
    # If baseline includes an updated timestamp per row, use it.
    # Otherwise skip freshness filter (the baseline job itself is recent).
    now_ts = time.time()
    if "updated_ts" in df.columns:  # seconds epoch recommended
        max_age_sec = LIVE_MAX_AGE_MIN * 60
        df = df[(now_ts - df["updated_ts"].astype(float)) <= max_age_sec]

    return df

def _build_picks(df: pd.DataFrame, limit: int) -> list[LivePick]:
    # Compute edge vs consensus if needed
    if "edge_vs_consensus" not in df.columns:
        if {"home_q_novig", "consensus_home_q"}.issubset(df.columns):
            df["edge_vs_consensus"] = df["consensus_home_q"] - df["home_q_novig"]
        else:
            df["edge_vs_consensus"] = 0.0  # fallback

    # Sort by absolute edge, biggest first
    df = df.sort_values(by="edge_vs_consensus", key=lambda s: s.abs(), ascending=False)

    out = []
    for _, row in df.head(limit).iterrows():
        try:
            out.append(LivePick(
                event_id=str(row.get("event_id", "")),
                sport_key=str(row.get("sport_key", "")),
                home_team=str(row.get("home_team", "")),
                away_team=str(row.get("away_team", "")),
                book_key=str(row.get("book_key", "")),
                home_q_novig=float(row.get("home_q_novig", 0.0)),
                consensus_home_q=float(row.get("consensus_home_q", 0.0)),
                edge_vs_consensus=float(row.get("edge_vs_consensus", 0.0)),
            ))
        except Exception:
            # skip malformed rows safely
            continue
    return out

@picks_live_router.get("/picks_live", response_model=PicksResponse)
def picks_live(
    source: str = Query(default="fullgame", pattern="^(fullgame|firsthalf)$"),
    min_abs_edge: float = Query(0.02, ge=0, le=1),
    limit: int = Query(25, ge=1, le=200),
):
    """
    Live picks endpoint backed by CSV baselines.
    - source=fullgame    -> market_baselines_h2h.csv
    - source=firsthalf   -> market_baselines_firsthalf.csv
    """
    csv_path = FULLGAME_FILE if source == "fullgame" else FIRSTHALF_FILE

    try:
        df = _load_csv(csv_path)
    except FileNotFoundError as e:
        return PicksResponse(picks=[], note=f"Missing or empty file: {csv_path.name}")

    # Basic required columns check
    required = {"event_id", "sport_key", "home_team", "away_team", "book_key", "home_q_novig", "consensus_home_q"}
    if not required.issubset(df.columns):
        missing = ", ".join(sorted(list(required - set(df.columns))))
        return PicksResponse(picks=[], note=f"{csv_path.name} missing columns: {missing}")

    # Filters
    df = _apply_filters(df)

    # Edge threshold
    if "edge_vs_consensus" not in df.columns:
        df["edge_vs_consensus"] = df["consensus_home_q"] - df["home_q_novig"]
    df = df[df["edge_vs_consensus"].abs() >= min_abs_edge]

    if df.empty:
        return PicksResponse(picks=[], note="No rows after filters (consensus depth / freshness / edge).")

    picks = _build_picks(df, limit)
    return PicksResponse(picks=picks)
