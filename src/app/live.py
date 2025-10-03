# src/app/live.py
from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from fastapi import APIRouter, Query

picks_live_router = APIRouter()

# -----------------------------
# Helpers: env + per-sport overrides
# -----------------------------
def _csv_env(name: str, default_csv: str = "") -> List[str]:
    raw = os.getenv(name, default_csv).strip()
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]

def _int_env(name: str, default_val: int) -> int:
    try:
        return int(os.getenv(name, str(default_val)))
    except Exception:
        return default_val

def _float_env(name: str, default_val: float) -> float:
    try:
        return float(os.getenv(name, str(default_val)))
    except Exception:
        return default_val

def _get_per_sport(name_base: str, sport_key: Optional[str], fallback_val):
    """
    Look up env override like NAME__sport_key, else NAME.
    Example: LIVE_MAX_AGE_MIN__baseball_mlb; falls back to LIVE_MAX_AGE_MIN.
    """
    if sport_key:
        specific = f"{name_base}__{sport_key}"
        if specific in os.environ and str(os.environ[specific]).strip() != "":
            typ = type(fallback_val)
            try:
                if typ is int:
                    return int(os.environ[specific])
                if typ is float:
                    return float(os.environ[specific])
                if typ is list:
                    return _csv_env(specific)
                return os.environ[specific]
            except Exception:
                # if cast fails, fall back
                pass
    return fallback_val

# -----------------------------
# Config (globals with optional per-sport overrides)
# -----------------------------
# Global defaults (can be overridden globally or per sport)
GLOBAL_ALLOWED_BOOKS = _csv_env("LIVE_ALLOWED_BOOKS", "")  # empty = all books
GLOBAL_MIN_CONSENSUS = _int_env("LIVE_MIN_CONSENSUS_BOOKS", 2)
GLOBAL_MAX_AGE_MIN = _int_env("LIVE_MAX_AGE_MIN", 120)
GLOBAL_MIN_ABS_EDGE = _float_env("DEFAULT_MIN_ABS_EDGE", 0.015)
GLOBAL_LIMIT = _int_env("DEFAULT_LIMIT", 20)

# Input files
PROCESSED_DIR = Path("data/processed")
FULLGAME_FILE = PROCESSED_DIR / "market_baselines_h2h.csv"
FIRSTHALF_FILE = PROCESSED_DIR / "market_baselines_firsthalf.csv"

# -----------------------------
# Core loader
# -----------------------------
def _load_baseline(source: str) -> Optional[pd.DataFrame]:
    f = FIRSTHALF_FILE if source == "firsthalf" else FULLGAME_FILE
    if not f.exists():
        return None
    try:
        df = pd.read_csv(f)
    except Exception:
        return None
    # Normalize expected columns if present
    # Required columns we use below:
    #   event_id, sport_key, home_team, away_team, book_key,
    #   home_q_novig, consensus_home_q
    missing = [c for c in ["event_id","sport_key","home_team","away_team","book_key","home_q_novig","consensus_home_q"] if c not in df.columns]
    if missing:
        # Try to adapt if file has slightly different names (no-op if not found)
        # Keep it simple; return None if truly incompatible.
        return None
    return df

# -----------------------------
# Endpoint
# -----------------------------
@picks_live_router.get("/picks_live")
def picks_live(
    source: str = Query("fullgame", pattern="^(fullgame|firsthalf)$", description="fullgame or firsthalf"),
    sport: Optional[str] = Query(None, description="Optional sport_key filter, e.g. baseball_mlb"),
    min_abs_edge: float = Query(None, ge=0, le=1, description="Override min |edge_vs_consensus|"),
    limit: int = Query(None, ge=1, le=200, description="Max rows to return"),
):
    """
    Returns rows from processed baselines with filters:
    - Allowed books (global or per-sport)
    - Min consensus books (global or per-sport)
    - Max age minutes (global or per-sport) — enforced via file mtime
    - Min absolute edge
    - Optional sport filter
    """
    df = _load_baseline("firsthalf" if source == "firsthalf" else "fullgame")
    if df is None or df.empty:
        return {"picks": [], "note": f"Missing or empty file: {('market_baselines_firsthalf.csv' if source=='firsthalf' else 'market_baselines_h2h.csv')}"}

    # If sport query provided, filter early
    if sport:
        df = df[df["sport_key"] == sport]
        if df.empty:
            return {"picks": [], "note": f"No rows for sport={sport} after initial filter."}

    # Resolve per-sport overrides (if sport filter set, use that; otherwise use globals)
    # NOTE: When multiple sports are present in one response, we’ll use global defaults.
    min_consensus = _get_per_sport("LIVE_MIN_CONSENSUS_BOOKS", sport, GLOBAL_MIN_CONSENSUS)
    max_age_min = _get_per_sport("LIVE_MAX_AGE_MIN", sport, GLOBAL_MAX_AGE_MIN)
    allowed_books = _get_per_sport("LIVE_ALLOWED_BOOKS", sport, GLOBAL_ALLOWED_BOOKS)

    # Freshness gate (based on file mtime); if too old for the requested sport, return none.
    try:
        fpath = FIRSTHALF_FILE if source == "firsthalf" else FULLGAME_FILE
        age_minutes = max(0, int((pd.Timestamp.utcnow() - pd.Timestamp(fpath.stat().st_mtime, unit="s", tz="UTC")).total_seconds() // 60))
        if age_minutes > max_age_min:
            return {"picks": [], "note": f"Snapshot too old ({age_minutes} min > max {max_age_min}); run admin refresh."}
    except Exception:
        pass  # if we can’t compute age, we don’t block

    # Book filter (optional)
    if isinstance(allowed_books, list) and allowed_books:
        df = df[df["book_key"].isin(allowed_books)]
        if df.empty:
            return {"picks": [], "note": "No rows after allowed_books filter."}

    # Min consensus: count number of distinct books contributing to the event in this df
    # Assumes each row is one (event_id, book_key) quote.
    depth = df.groupby("event_id")["book_key"].nunique().rename("consensus_depth")
    df = df.merge(depth, left_on="event_id", right_index=True, how="left")
    df = df[df["consensus_depth"] >= int(min_consensus)]
    if df.empty:
        return {"picks": [], "note": "No rows after consensus depth filter."}

    # Compute edge vs consensus if not present; otherwise trust existing column
    if "edge_vs_consensus" not in df.columns:
        df["edge_vs_consensus"] = df["home_q_novig"] - df["consensus_home_q"]

    # Min absolute edge (use per-request override if provided, else global default)
    min_edge = GLOBAL_MIN_ABS_EDGE if (min_abs_edge is None) else float(min_abs_edge)
    df["abs_edge"] = df["edge_vs_consensus"].abs()
    df = df[df["abs_edge"] >= float(min_edge)]
    if df.empty:
        return {"picks": [], "note": "No rows after edge filter."}

    # Sort and trim
    max_rows = GLOBAL_LIMIT if (limit is None) else int(limit)
    df = df.sort_values("abs_edge", ascending=False).head(max_rows)

    # Select fields for response
    cols = [
        "event_id",
        "sport_key",
        "home_team",
        "away_team",
        "book_key",
        "home_q_novig",
        "consensus_home_q",
        "edge_vs_consensus",
    ]
    # keep only existing cols (defensive)
    cols = [c for c in cols if c in df.columns]
    out = df[cols].to_dict(orient="records")
    return {"picks": out}
