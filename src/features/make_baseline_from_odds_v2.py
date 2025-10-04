# src/features/make_baseline_from_odds_v2.py
"""
Builds no-vig consensus baselines from long-form odds CSV produced by src.etl.pull_odds_to_csv.

Input:  data/raw/odds_latest.csv  (long form)
Expected columns (from pull_odds_to_csv):
  ['event_id','sport_key','commence_time','home_team','away_team',
   'book_key','book_title','last_update','market_key','outcome_name','price','point']

Output: data/processed/market_baselines_h2h.csv with at least:
  event_id, sport_key, commence_time, home_team, away_team,
  consensus_home_q, consensus_away_q,
  consensus_home_fair_odds, consensus_away_fair_odds,
  num_books, books_used, last_updated_utc
"""

from __future__ import annotations

import os
import math
import pandas as pd
from typing import Iterable

RAW_PATH = "data/raw/odds_latest.csv"
OUT_PATH = "data/processed/market_baselines_h2h.csv"


def _american_to_prob(ml: float | int) -> float:
    """
    Convert American odds to implied probability (no vig removal here).
    +150 -> 100/(150+100); -150 -> 150/(150+100)
    """
    try:
        ml = float(ml)
    except Exception:
        return float("nan")

    if ml > 0:
        return 100.0 / (ml + 100.0)
    elif ml < 0:
        ml = abs(ml)
        return ml / (ml + 100.0)
    else:
        return float("nan")


def _prob_to_american(q: float) -> float:
    """Convert probability to no-vig fair American odds."""
    if q <= 0 or q >= 1:
        return float("nan")
    dec = 1.0 / q
    # american
    if dec >= 2.0:
        return (dec - 1.0) * 100.0
    else:
        return -100.0 / (dec - 1.0)


def _filter_allowlists(df: pd.DataFrame) -> pd.DataFrame:
    # Books allowlist
    books_env = os.getenv("BOOKS_ALLOWED", "")
    books = [b.strip() for b in books_env.split(",") if b.strip()] if books_env else []
    if books:
        df = df[df["book_key"].isin(books)].copy()

    # Sports allowlist (optional)
    sports_env = os.getenv("SPORTS_ALLOWED", "")
    sports = [s.strip() for s in sports_env.split(",") if s.strip()] if sports_env else []
    if sports:
        df = df[df["sport_key"].isin(sports)].copy()

    return df


def _latest_update_iso(series: Iterable[str]) -> str:
    try:
        ts = pd.to_datetime(series, utc=True, errors="coerce")
        mx = ts.max()
        if pd.isna(mx):
            return ""
        return mx.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return ""


def build_consensus() -> pd.DataFrame:
    if not os.path.exists(RAW_PATH) or os.path.getsize(RAW_PATH) == 0:
        raise FileNotFoundError(f"Missing or empty file: {RAW_PATH}")

    df = pd.read_csv(RAW_PATH)
    needed = {
        "event_id", "sport_key", "commence_time", "home_team", "away_team",
        "book_key", "book_title", "last_update", "market_key", "outcome_name", "price"
    }
    missing = needed.difference(df.columns)
    if missing:
        raise ValueError(f"{RAW_PATH} missing columns: {sorted(list(missing))}")

    # Only full-game moneyline (h2h)
    df = df[df["market_key"] == "h2h"].copy()
    if df.empty:
        return pd.DataFrame(columns=[
            "event_id","sport_key","commence_time","home_team","away_team",
            "consensus_home_q","consensus_away_q",
            "consensus_home_fair_odds","consensus_away_fair_odds",
            "num_books","books_used","last_updated_utc"
        ])

    df = _filter_allowlists(df)

    # Map outcome_name to side: home vs away (compare to team names)
    df["side"] = None
    df.loc[df["outcome_name"] == df["home_team"], "side"] = "home"
    df.loc[df["outcome_name"] == df["away_team"], "side"] = "away"
    df = df[df["side"].isin(["home", "away"])].copy()

    # Convert American odds to implied probabilities
    df["q"] = df["price"].map(_american_to_prob)

    # Drop rows with invalid probs
    df = df[pd.notna(df["q"]) & (df["q"] > 0) & (df["q"] < 1)].copy()
    if df.empty:
        return pd.DataFrame(columns=[
            "event_id","sport_key","commence_time","home_team","away_team",
            "consensus_home_q","consensus_away_q",
            "consensus_home_fair_odds","consensus_away_fair_odds",
            "num_books","books_used","last_updated_utc"
        ])

    # Compute consensus per event across allowlisted books
    # We average probabilities by side; also collect books_used & num_books per event.
    group_keys = ["event_id", "sport_key", "commence_time", "home_team", "away_team"]
    # consensus by side
    side_mean = (
        df.groupby(group_keys + ["side"])
          .agg(consensus_q=("q", "mean"),
               books_used=("book_key", lambda x: ",".join(sorted(set(x)))),
               num_books=("book_key", lambda x: len(set(x))),
               last_updated_utc=("last_update", _latest_update_iso))
          .reset_index()
    )

    # pivot back to home/away columns
    pivot = side_mean.pivot_table(
        index=group_keys,
        columns="side",
        values=["consensus_q", "books_used", "num_books", "last_updated_utc"],
        aggfunc="first"
    )

    # Flatten columns
    pivot.columns = [f"{a}_{b}" for a, b in pivot.columns.to_flat_index()]
    pivot = pivot.reset_index()

    # Ensure required columns exist
    for col in ["consensus_q_home", "consensus_q_away"]:
        if col not in pivot.columns:
            pivot[col] = pd.NA

    # Rename to expected output names
    out = pivot.rename(columns={
        "consensus_q_home": "consensus_home_q",
        "consensus_q_away": "consensus_away_q",
        "books_used_home": "books_used",       # same set for both sides after groupby; prefer home col if present
        "num_books_home": "num_books",
        "last_updated_utc_home": "last_updated_utc",
    }).copy()

    # If books_used/num_books missing on _home, try _away
    if "books_used" not in out or out["books_used"].isna().all():
        if "books_used_away" in pivot.columns:
            out["books_used"] = pivot["books_used_away"]
    if "num_books" not in out or out["num_books"].isna().all():
        if "num_books_away" in pivot.columns:
            out["num_books"] = pivot["num_books_away"]
    if "last_updated_utc" not in out or out["last_updated_utc"].isna().all():
        if "last_updated_utc_away" in pivot.columns:
            out["last_updated_utc"] = pivot["last_updated_utc_away"]

    # Fair odds from consensus probabilities
    out["consensus_home_fair_odds"] = out["consensus_home_q"].map(_prob_to_american)
    out["consensus_away_fair_odds"] = out["consensus_away_q"].map(_prob_to_american)

    # Order/keep columns
    cols = [
        "event_id", "sport_key", "commence_time", "home_team", "away_team",
        "consensus_home_q", "consensus_away_q",
        "consensus_home_fair_odds", "consensus_away_fair_odds",
        "num_books", "books_used", "last_updated_utc",
    ]
    # Include only those that exist
    cols = [c for c in cols if c in out.columns]
    out = out[cols].copy()

    return out


def main():
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    df = build_consensus()
    df.to_csv(OUT_PATH, index=False)
    print(f"Wrote {OUT_PATH} with {len(df)} rows")


if __name__ == "__main__":
    main()
