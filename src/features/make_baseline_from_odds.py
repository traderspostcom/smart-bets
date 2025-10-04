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


def _normalize_wide(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map 'wide' schema (one row per event-book with home/away ML columns) into a canonical table.
    """
    col_map = {
        "event_id": first_existing(df, ["event_id", "id", "event_key"]),
        "sport_key": first_existing(df, ["sport_key", "sport"]),
        "commence_time": first_existing(df, ["commence_time", "start_time", "commence_time_utc"]),
        "home_team": first_existing(df, ["home_team", "home", "team_home"]),
        "away_team": first_existing(df, ["away_team", "away", "team_away"]),
        "book": first_existing(df, ["book", "book_key", "bookmaker_key", "bookmaker"]),
        "home_price": first_existing(df, ["price_home", "home_price", "home_odds", "home_ml", "price_home_moneyline"]),
        "away_price": first_existing(df, ["price_away", "away_price", "away_odds", "away_ml", "price_away_moneyline"]),
    }
    updated = [c for c in ["last_update", "updated_at", "fetched_at", "pulled_at"] if c in df.columns]
    updated_col = updated[0] if updated else None

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
    out["updated_at"] = df[updated_col] if updated_col else pd.NaT
    return out


def _normalize_long(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map 'long' schema (multiple rows per event-book with columns like outcome_name, price)
    into a canonical wide per-event-book row with home_price/away_price.
    Expected columns seen in error: book_key, book_title, last_update, market_key, outcome_name, price, point
    """
    # Required id columns
    event_id = first_existing(df, ["event_id", "id", "event_key"])
    sport_key = first_existing(df, ["sport_key", "sport"])
    commence_time = first_existing(df, ["commence_time", "start_time", "commence_time_utc"])
    home_team = first_existing(df, ["home_team", "home", "team_home"])
    away_team = first_existing(df, ["away_team", "away", "team_away"])

    # Book columns
    book_col = None
    for c in ["book", "book_key", "bookmaker_key", "bookmaker"]:
        if c in df.columns:
            book_col = c
            break
    if not book_col:
        # fall back to title if key missing
        book_col = first_existing(df, ["book_title"])

    # Price columns
    price_col = first_existing(df, ["price", "american_price", "odds"])
    outcome_col = first_existing(df, ["outcome_name", "outcome", "side", "team"])

    # Keep only h2h market if present
    if "market_key" in df.columns:
        df = df[df["market_key"].astype(str).str.lower().eq("h2h") | df["market_key"].isna()].copy()

    keep = [
        event_id, sport_key, commence_time, home_team, away_team,
        book_col, outcome_col, price_col
    ]
    if "last_update" in df.columns:
        keep.append("last_update")
    df2 = df[keep].copy()

    # Standardize column names
    df2 = df2.rename(columns={
        event_id: "event_id",
        sport_key: "sport_key",
        commence_time: "commence_time",
        home_team: "home_team",
        away_team: "away_team",
        book_col: "book",
        outcome_col: "outcome_name",
        price_col: "price"
    })
    df2["book"] = df2["book"].astype(str).str.lower()

    # Determine side = home/away based on outcome_name
    def _side(row):
        val = str(row["outcome_name"]).strip().lower()
        h = str(row["home_team"]).strip().lower()
        a = str(row["away_team"]).strip().lower()
        if val in ("home", "h", "1", "home_team"):
            return "home"
        if val in ("away", "a", "2", "away_team"):
            return "away"
        # If the outcome is the team name, match it
        if val == h:
            return "home"
        if val == a:
            return "away"
        # ignore draws/pushes for h2h
        if val in ("draw", "tie", "x"):
            return None
        # best effort string contains
        if val and h and val in h:
            return "home"
        if val and a and val in a:
            return "away"
        return None

    df2["side"] = df2.apply(_side, axis=1)
    df2 = df2[df2["side"].isin(["home", "away"])].copy()

    # Pivot to wide: one row per event-book with home_price & away_price
    df2["price"] = pd.to_numeric(df2["price"], errors="coerce")
    idx_cols = ["event_id", "sport_key", "commence_time", "home_team", "away_team", "book"]
    wide = df2.pivot_table(index=idx_cols, columns="side", values="price", aggfunc="first").reset_index()
    wide = wide.rename(columns={"home": "home_price", "away": "away_price"})
    wide["updated_at"] = pd.NaT  # use last_update if you want, but optional for now
    return wide


def normalize_any(df: pd.DataFrame) -> pd.DataFrame:
    cols = set(df.columns)
    wide_needed = {"price_home", "home_price", "home_odds", "home_ml", "price_home_moneyline"} & cols
    long_hint = {"outcome_name", "price"} <= cols
    try:
        if wide_needed:
            return _normalize_wide(df)
        if long_hint:
            return _normalize_long(df)
        # Fallback: try long
        return _normalize_long(df)
    except KeyError as e:
        # Last resort: raise a clear error
        raise


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
    df = df[(s > 0) & s.notna()].copy()
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
        last_updated_utc=("updated_at", "max") if "updated_at" in df.columns else ("home_price", "size"),
    ).reset_index()

    # Convert last_updated_utc if it accidentally came from wrong agg
    if "last_updated_utc" in agg.columns and agg["last_updated_utc"].dtype != "datetime64[ns, UTC]":
        try:
            agg["last_updated_utc"] = pd.to_datetime(agg["last_updated_utc"], utc=True, errors="coerce")
        except Exception:
            pass

    # Fair odds from consensus probs
    agg["consensus_home_fair_odds"] = agg["consensus_home_q"].apply(prob_to_american)
    agg["consensus_away_fair_odds"] = agg["consensus_away_q"].apply(prob_to_american)

    # Enforce minimum book count
    min_books = int(os.getenv("LIVE_MIN_BOOKS", "3"))
    agg = agg[agg["num_books"] >= max(1, min_books)].copy()

    # Sort & write
    try:
        agg["commence_time"] = pd.to_datetime(agg["commence_time"], utc=True, errors="coerce")
    except Exception:
        pass
    agg = agg.sort_values(["commence_time", "home_team"], na_position="last").copy()

    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    agg.to_csv(OUT, index=False)
    return agg


def main():
    if not os.path.exists(RAW_IN):
        print(json.dumps({"ok": False, "error": f"Missing input file {RAW_IN}"}))
        sys.exit(1)

    df_raw = pd.read_csv(RAW_IN)
    df = normalize_any(df_raw)
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
