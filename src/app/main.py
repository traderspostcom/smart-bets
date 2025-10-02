from fastapi import FastAPI, Query
from pydantic import BaseModel
import os, json
from pathlib import Path
import pandas as pd
import numpy as np
from joblib import load

app = FastAPI(title="Smart Bets API")

# --- helpers ---
def american_to_decimal(A: int) -> float:
    return (100/abs(A))+1 if A < 0 else (A/100)+1

def kelly_fraction(p: float, b: float) -> float:
    f = (b*p - (1-p)) / b
    return max(0.0, min(1.0, f))

def load_artifacts():
    artifacts = Path("./data/model_artifacts")
    processed = Path("./data/processed")

    model = load(artifacts / "model.joblib")
    cal_path = artifacts / "calibration.joblib"
    cal = load(cal_path) if cal_path.exists() else None

    feats = pd.read_csv(processed / "features.csv")
    return model, cal, feats

def build_picks(min_edge=0.015, kelly_frac=0.25, max_risk_per_market=0.02, daily_risk_cap=0.08, bankroll=100000.0):
    model, cal, df = load_artifacts()
    feature_cols = [
        "home_rating","away_rating","rest_diff","travel_miles","weather_wind","market_implied_q_novig"
    ]
    probs = model.predict_proba(df[feature_cols].values)[:,1]
    if cal is not None:
        probs = np.array([float(cal.predict([p])[0]) for p in probs])

    # approximate no-vig baseline from toy data columns
    q_vig = 1.0/df["price_home_decimal"].values
    q_away_vig = 1.0 - q_vig
    hold = q_vig + q_away_vig
    q_novig = q_vig / hold

    edges = probs - q_novig
    df_out = df[["game_id","book","price_home_american","price_home_decimal"]].copy()
    df_out["p_model"] = probs
    df_out["q_novig"] = q_novig
    df_out["edge"] = edges

    picks = []
    risk_used = 0.0
    cap_daily = daily_risk_cap * bankroll
    cap_market = max_risk_per_market * bankroll

    for row in df_out.sort_values("edge", ascending=False).itertuples():
        if row.edge < min_edge or risk_used >= cap_daily:
            continue
        b = row.price_home_decimal - 1.0
        f = kelly_frac * kelly_fraction(row.p_model, b)
        stake = min(f * bankroll, cap_market, cap_daily - risk_used)
        if stake <= 0: 
            continue
        risk_used += stake
        picks.append({
            "game_id": int(row.game_id),
            "book": row.book,
            "price_american": int(row.price_home_american),
            "p_model": float(row.p_model),
            "q_novig": float(row.q_novig),
            "edge": float(row.edge),
            "stake": round(float(stake), 2),
            "kelly_fraction": round(float(f), 4)
        })
    return picks

# --- API routes ---
@app.get("/health")
def health():
    return {"ok": True}

class PicksResponse(BaseModel):
    picks: list

@app.get("/picks", response_model=PicksResponse)
def picks(
    min_edge: float = Query(0.015, ge=0, le=1),
    kelly_frac: float = Query(0.25, ge=0, le=1),
    max_risk_per_market: float = Query(0.02, ge=0, le=1),
    daily_risk_cap: float = Query(0.08, ge=0, le=1),
    bankroll: float = Query(100000.0, ge=0),
):
    data = build_picks(min_edge, kelly_frac, max_risk_per_market, daily_risk_cap, bankroll)
    return {"picks": data}

from .live import picks_live_router
app.include_router(picks_live_router)

from .admin import admin_router
app.include_router(admin_router)
