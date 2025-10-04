
# src/features/make_baseline_first_half_v2.py
"""
Builds no-vig consensus baselines for **1st-half / F5** from long-form CSV
written by: src.etl.pull_period_odds_to_csv

Input:  data/raw/odds_periods_latest.csv
Columns expected:
  ['event_id','sport_key','commence_time','home_team','away_team',
   'book_key','book_title','last_update','market_key','outcome_name','price','point']

Output: data/processed/market_baselines_firsthalf.csv with:
  event_id, sport_key, commence_time, home_team, away_team,
  consensus_home_q, consensus_away_q,
  consensus_home_fair_odds, consensus_away_fair_odds,
  num_books, books_used, last_updated_utc
"""

from __future__ import annotations

import os
import pandas as pd
from typing import Iterable

RAW_PATH = "data/raw/odds_periods_latest.csv"
OUT_PATH = "data/processed/market_baselines_firsthalf.csv"


def _american_to_prob(ml):
    try:
        ml = float(ml)
    except Exception:
        return float("nan")
    if ml > 0:
        return 100.0 / (ml + 100.0)
    elif ml < 0:
        ml = abs(ml)
        return ml / (ml + 100.0)
    return float("nan")


def _prob_to_american(q: float):
    if q <= 0 or q >= 1:
        return float("nan")
    dec = 1.0 / q
    return (dec - 1.0) * 100.0 if dec >= 2.0 else -100.0 / (dec - 1.0)


def _filter_allowlists(df: pd.DataFrame) -> pd.DataFrame:
    books_env = os.getenv("BOOKS_ALLOWED", "")
    books = [b.strip() for b in books_env.split(",") if b.strip()] if books_env else []
    if books:
        df = df[df["book_key"].isin(books)].copy()

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


def _map_side(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map outcome_name -> {'home','away'} for period/F5 markets.
    Providers often use labels like 'Home', 'Away', 'F5 Home', '1st Half Away', etc.
    Fallback to team-name equality.
    """
    lab = df["outcome_name"].astype(str).str.strip()
    lab_cf = lab.str.casefold()

    is_home_like = (
        lab_cf.eq("home")
        | lab_cf.str.contains("home")
        | lab_cf.str.contains("1st half home")
        | lab_cf.str.contains("first half home")
        | lab_cf.str.contains("f5 home")
    )
    is_away_like = (
        lab_cf.eq("away")
        | lab_cf.str.contains("away")
        | lab_cf.str.contains("1st half away")
        | lab_cf.str.contains("first half away")
        | lab_cf.str.contains("f5 away")
    )

    side = pd.Series(pd.NA, index=df.index, dtype="object")
    side[is_home_like] = "home"
    side[is_away_like] = "away"

    # Fallback: exact team name match
    side[df["outcome_name"] == df["home_team"]] = "home"
    side[df["outcome_name"] == df["away_team"]] = "away"

    df = df.copy()
    df["side"] = side
    return df[df["side"].isin(["home", "away"])]


def build_consensus() -> pd.DataFrame:
    if not os.path.exists(RAW_PATH) or os.path.getsize(RAW_PATH) == 0:
        # Ensure the processed file exists even if raw is missing
        os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
        pd.DataFrame(columns=[
            "event_id","sport_key","commence_time","home_team","away_team",
            "consensus_home_q","consensus_away_q",
            "consensus_home_fair_odds","consensus_away_fair_odds",
            "num_books","books_used","last_updated_utc",
        ]).to_csv(OUT_PATH, index=False)
        print(f"Wrote {OUT_PATH} with 0 rows (no raw file)")
        return pd.read_csv(OUT_PATH)

    df = pd.read_csv(RAW_PATH)

    needed = {
        "event_id", "sport_key", "commence_time", "home_team", "away_team",
        "book_key", "book_title", "last_update", "market_key", "outcome_name", "price"
    }
    missing = needed.difference(df.columns)
    if missing:
        raise ValueError(f"{RAW_PATH} missing columns: {sorted(list(missing))}")

    # Keep only moneyline-ish markets for periods/F5 (be generous)
    keep_markets = {"h2h", "moneyline", "ml", "h2h_1h", "h2h_h1", "h2h_first_half", "f5", "ml_f5"}
    df = df[df["market_key"].astype(str).str.lower().isin(keep_markets)].copy()

    if df.empty:
        os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
        empty = pd.DataFrame(columns=[
            "event_id","sport_key","commence_time","home_team","away_team",
            "consensus_home_q","consensus_away_q",
            "consensus_home_fair_odds","consensus_away_fair_odds",
            "num_books","books_used","last_updated_utc",
        ])
        empty.to_csv(OUT_PATH, index=False)
        print(f"Wrote {OUT_PATH} with 0 rows (no matching markets)")
        return empty

    df = _filter_allowlists(df)
    df = _map_side(df)

    # Convert to implied probabilities and drop invalids
    df["q"] = df["price"].map(_american_to_prob)
    df = df[pd.notna(df["q"]) & (df["q"] > 0) & (df["q"] < 1)].copy()

    if df.empty:
        os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
        empty = pd.DataFrame(columns=[
            "event_id","sport_key","commence_time","home_team","away_team",
            "consensus_home_q","consensus_away_q",
            "consensus_home_fair_odds","consensus_away_fair_odds",
            "num_books","books_used","last_updated_utc",
        ])
        empty.to_csv(OUT_PATH, index=False)
        print(f"Wrote {OUT_PATH} with 0 rows (after side/prob filtering)")
        return empty

    keys = ["event_id", "sport_key", "commence_time", "home_team", "away_team"]
    side_mean = (
        df.groupby(keys + ["side"])
          .agg(consensus_q=("q", "mean"),
               books_used=("book_key", lambda x: ",".join(sorted(set(x)))),
               num_books=("book_key", lambda x: len(set(x))),
               last_updated_utc=("last_update", _latest_update_iso))
          .reset_index()
    )

    pivot = side_mean.pivot_table(
        index=keys,
        columns="side",
        values=["consensus_q", "books_used", "num_books", "last_updated_utc"],
        aggfunc="first"
    )
    pivot.columns = [f"{a}_{b}" for a, b in pivot.columns.to_flat_index()]
    pivot = pivot.reset_index()

    for col in ["consensus_q_home", "consensus_q_away"]:
        if col not in pivot.columns:
            pivot[col] = pd.NA

    out = pivot.rename(columns={
        "consensus_q_home": "consensus_home_q",
        "consensus_q_away": "consensus_away_q",
        "books_used_home": "books_used",
        "num_books_home": "num_books",
        "last_updated_utc_home": "last_updated_utc",
    }).copy()

    if "books_used" not in out or out["books_used"].isna().all():
        if "books_used_away" in pivot.columns:
            out["books_used"] = pivot["books_used_away"]
    if "num_books" not in out or out["num_books"].isna().all():
        if "num_books_away" in pivot.columns:
            out["num_books"] = pivot["num_books_away"]
    if "last_updated_utc" not in out or out["last_updated_utc"].isna().all():
        if "last_updated_utc_away" in pivot.columns:
            out["last_updated_utc"] = pivot["last_updated_utc_away"]

    out["consensus_home_fair_odds"] = out["consensus_home_q"].map(_prob_to_american)
    out["consensus_away_fair_odds"] = out["consensus_away_q"].map(_prob_to_american)

    cols = [
        "event_id","sport_key","commence_time","home_team","away_team",
        "consensus_home_q","consensus_away_q",
        "consensus_home_fair_odds","consensus_away_fair_odds",
        "num_books","books_used","last_updated_utc",
    ]
    cols = [c for c in cols if c in out.columns]
    out = out[cols].copy()

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    out.to_csv(OUT_PATH, index=False)
    print(f"Wrote {OUT_PATH} with {len(out)} rows")
    return out


def main():
    build_consensus()


if __name__ == "__main__":
    main()
