# src/features/make_baseline_from_odds.py
import os
import sys
import json
from datetime import datetime, timezone
from typing import List

import pandas as pd


RAW_IN = "data/raw/odds_latest.csv"
OUT = "data/processed/market_baselines_h2h.csv"


def _get_env_csv(name: str, default: str = "") -> List[str]:
    v = os.getenv(name, default)
    if not v:
        return []
    return [x.strip().lower() for x in v.split(",") if x.strip()]


def american_to_prob(odds: float) -> float:
    """
    Convert American odds to implied probability (with vig).
    +150 -> 100/(150+100)
    -150 -> 150/(150+100)
    """
    if pd.isna(odds):
        return float("nan")
    try:
        o = float(odds)
    except Exception:
        return float("nan")
    if o > 0:
        return 100.0 / (o + 100.0)
    else:
        return (-o) / ((-o) + 100.0)


def prob_to_american(p: float) -> int:
    """
    Convert probability to fair American odds.
    p in (0,1). If p < 0.5 => positive odds; else negative.
    """
    p = float(p)
    p = min(max(p, 1e-9), 1 - 1e-9)
    if p < 0.5:
        return int(round((1 - p) / p * 100))
    else:
        return int(round(-p / (1 - p) * 100))


def first_existing(df: pd.DataFrame, candidates: List[str]) -> str:
    for c in candidates:
        if c in df.columns:
            return c
    raise KeyError(f"None of the expected columns found: {candidates}. Present: {list(df.columns)}")


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Map flexible raw schemas into a common set
    col_map = {}

    # Identity columns
    col_map["event_id"] = first_existing(df, ["event_id", "id", "event_key"])
    col_map["sport_key"] = first_existing(df, ["sport_key", "sport"])
    col_map["commence_time"] = first_existing(df, ["commence_time", "start_time", "commence_time_utc"])
    col_map["home_team"] = first_existing(df, ["home_team", "home", "team_home"])
    col_map["away_team"] = first_existing(df, ["away_team", "away", "team_away"])

    # Book and prices (American ML)
    col_map["book"] = first_existing(df, ["book", "book_key", "bookmaker_key", "bookmaker"])
    col_map["home_price"] = first_existing(df, ["price_home", "home_price", "home_odds", "home_ml", "price_home_moneyline"])
    col_map["away_price"] = first_existing(df, ["price_away", "away_price", "away_odds", "away_ml", "price_away_moneyline"])

    # Optional updated timestamp
    updated_candidates = [c for c in ["last_update", "updated_at", "fetched_at", "pulled_at"] if c in df.columns]
    col_map["updated_at"] = updated_candidates[0] if updated_candidates else None

    out = pd.DataFrame({
        "event_id": df[col_map["event_id"]],
        "sport_key": df[col_map["sport_key"]],
        "commence_time": df[col_map["commence_time"]],
        "home_team": df[col_map["home_team"]],
        "away_team": df[col_map["away_team"]],
        "book": df[col_map["book"]].astype(str).str.lower(),
        "home_price": pd.to_numeric(df[col_map["home_price"]], errors="coerce"),
        "away_price": pd.to_numeric(df[col_map["away_price"]], errors="coerce"),
    })

    if col_map["updated_at"]:
        out["updated_at"] = df[col_map["updated_at"]]
    else:
        out["updated_at"] = pd.NaT

    return out


def apply_allowlists(df: pd.DataFrame) -> pd.DataFrame:
    books_allowed = _get_env_csv("BOOKS_ALLOWED", "")
    if books_allowed:
        df = df[df["book"].isin(books_allowed)].copy()
    sports_allowed = _get_env_csv("SPORTS_ALLOWED", "")
    if sports_allowed:
        df = df[df["sport_key"].astype(str).str.lower().isin(sports_allowed)].copy()
    return df


def build_consensus(df: pd.DataFrame) -> pd.DataFrame:
    # Implied probs per row
    df["p_home_raw"] = df["home_price"].apply(american_to_prob)
    df["p_away_raw"] = df["away_price"].apply(american_to_prob)

    # Drop rows missing either side
    df = df.dropna(subset=["p_home_raw", "p_away_raw"]).copy()

    # De-vig at row level so home+away=1 by book/event
    s = df["p_home_raw"] + df["p_away_raw"]
    df["p_home_nv"] = df["p_home_raw"] / s
    df["p_away_nv"] = df["p_away_raw"] / s

    # Aggregate across books per event
    grp_cols = ["event_id", "sport_key", "commence_time", "home_team", "away_team"]
    agg = df.groupby(grp_cols).agg(
        consensus_home_q=("p_home_nv", "mean"),
        consensus_away_q=("p_away_nv", "mean"),
        num_books=("book", "nunique"),
        books_used=("book", lambda x: ";".join(sorted(set(map(str, x))))),
        last_updated_utc=("updated_at", "max"),
    ).reset_index()

    # Fair odds from consensus probs
    agg["consensus_home_fair_odds"] = agg["consensus_home_q"].apply(prob_to_american)
    agg["consensus_away_fair_odds"] = agg["consensus_away_q"].apply(prob_to_american)

    # Enforce minimum book count
    min_books = int(os.getenv("LIVE_MIN_BOOKS", "3"))
    agg = agg[agg["num_books"] >= max(1, min_books)].copy()

    # Sort stable & normalize time column
    try:
        agg["commence_time"] = pd.to_datetime(agg["commence_time"], utc=True, errors="coerce")
    except Exception:
        pass
    agg = agg.sort_values(["commence_time", "home_team"], na_position="last").copy()

    # Write
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    agg.to_csv(OUT, index=False)

    return agg


def main():
    if not os.path.exists(RAW_IN):
        print(json.dumps({"ok": False, "error": f"Missing input file {RAW_IN}"}))
        sys.exit(1)

    df_raw = pd.read_csv(RAW_IN)
    df = normalize_columns(df_raw)
    df = apply_allowlists(df)
    out = build_consensus(df)

    msg = {
        "ok": True,
        "written": OUT,
        "rows": int(out.shape[0]),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    print(f"Wrote {OUT} with {out.shape[0]} rows")
    print(json.dumps(msg))


if __name__ == "__main__":
    main()
