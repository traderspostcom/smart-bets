# src/app/live.py
import os
from typing import Optional

import pandas as pd
from fastapi import APIRouter, Query, Header, HTTPException
from fastapi.responses import JSONResponse

# NOTE: This module only defines a router.
# DO NOT create a FastAPI() app here and DO NOT import this module from itself.

router = APIRouter()

RAW_DIR = "data/raw"
PROC_DIR = "data/processed"
FULLGAME_PATH = os.path.join(PROC_DIR, "market_baselines_h2h.csv")
FIRSTHALF_PATH = os.path.join(PROC_DIR, "market_baselines_firsthalf.csv")
CRON_TOKEN = os.getenv("CRON_TOKEN", "")

def _need_auth(x_cron_token: Optional[str]) -> None:
    if not CRON_TOKEN:
        raise HTTPException(status_code=500, detail="CRON_TOKEN not set")
    if x_cron_token != CRON_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")

def _read_csv_or_note(path: str) -> pd.DataFrame:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        raise FileNotFoundError(f"Missing or empty file: {os.path.basename(path)}")
    return pd.read_csv(path)

def _apply_freshness_filter(df: pd.DataFrame) -> pd.DataFrame:
    def _get_max_age_minutes(sk: Optional[str]) -> int:
        if not sk:
            return int(os.getenv("LIVE_MAX_AGE_MINUTES", "60"))
        sk_upper = str(sk).upper()
        per_key = f"LIVE_MAX_AGE_MINUTES__{sk_upper.replace('-', '_')}"
        return int(os.getenv(per_key, os.getenv("LIVE_MAX_AGE_MINUTES", "60")))

    if "last_updated_utc" in df.columns:
        try:
            ts = pd.to_datetime(df["last_updated_utc"], utc=True, errors="coerce")
            now = pd.Timestamp.utcnow()
            age_min = (now - ts).dt.total_seconds() / 60.0
            if "sport_key" in df.columns:
                def _row_keep(row):
                    try:
                        cap = _get_max_age_minutes(row.get("sport_key"))
                    except Exception:
                        cap = int(os.getenv("LIVE_MAX_AGE_MINUTES", "60"))
                    return 0 <= row["_age_min"] <= cap
                tmp = df.copy()
                tmp["_age_min"] = age_min
                df = tmp[tmp.apply(_row_keep, axis=1)].drop(columns=["_age_min"])
            else:
                max_age = int(os.getenv("LIVE_MAX_AGE_MINUTES", "60"))
                df = df.loc[(age_min >= 0) & (age_min <= max_age)].copy()
        except Exception:
            pass
    return df

@router.get("/admin/peek_csv")
def admin_peek_csv(
    which: str = Query("fullgame", pattern="^(fullgame|firsthalf)$"),
    x_cron_token: Optional[str] = Header(None),
):
    _need_auth(x_cron_token)
    path = FULLGAME_PATH if which == "fullgame" else FIRSTHALF_PATH
    if not os.path.exists(path):
        return JSONResponse({"ok": False, "error": f"missing {path}"}, status_code=404)
    try:
        df = pd.read_csv(path, nrows=200)
        return {
            "ok": True,
            "path": path,
            "columns": list(df.columns),
            "rows_read": int(df.shape[0]),
            "sample_rows": df.head(5).to_dict(orient="records"),
        }
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e), "path": path}, status_code=500)

@router.get("/picks_live")
def picks_live(
    source: Optional[str] = Query(None, pattern="^(firsthalf)$"),
    min_abs_edge: float = 0.02,
    limit: int = 20,
):
    path = FIRSTHALF_PATH if source == "firsthalf" else FULLGAME_PATH

    try:
        df = _read_csv_or_note(path)
    except FileNotFoundError as e:
        return {"picks": [], "note": str(e)}

    needed = {"consensus_home_q", "consensus_away_q"}
    if not needed.issubset(set(df.columns)):
        return {
            "picks": [],
            "note": f"{os.path.basename(path)} missing columns: "
                    f"{', '.join(sorted(list(needed.difference(set(df.columns)))))}",
            "columns_present": list(df.columns),
        }

    min_books = int(os.getenv("LIVE_MIN_BOOKS", "3"))
    if "num_books" in df.columns:
        df = df[df["num_books"] >= max(1, min_books)].copy()

    df = _apply_freshness_filter(df)

    df["edge_home_abs"] = (df["consensus_home_q"] - 0.5).abs()
    df = df[df["edge_home_abs"] >= float(min_abs_edge)]

    cols_keep = [
        "event_id", "sport_key", "commence_time",
        "home_team", "away_team",
        "consensus_home_q", "consensus_away_q",
        "consensus_home_fair_odds", "consensus_away_fair_odds",
        "num_books", "books_used", "last_updated_utc", "edge_home_abs",
    ]
    cols_exist = [c for c in cols_keep if c in df.columns]
    df = df[cols_exist].copy()

    if "edge_home_abs" in df.columns:
        df = df.sort_values(["edge_home_abs", "commence_time"], ascending=[False, True])
    elif "commence_time" in df.columns:
        df = df.sort_values(["commence_time"], ascending=[True])

    if limit > 0:
        df = df.head(limit)

    return {"picks": df.to_dict(orient="records")}
