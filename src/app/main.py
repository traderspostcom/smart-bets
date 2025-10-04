from fastapi import FastAPI, Query
from pydantic import BaseModel
from pathlib import Path
from subprocess import run, PIPE
import os

# Routers (already in your repo)
from .live import picks_live_router
from .admin import admin_router

app = FastAPI(title="Smart Bets")

# ---------- health ----------
@app.get("/health")
def health():
    return {"ok": True}

# ---------- optional /picks (kept compatible; returns a note if build_picks is unavailable) ----------
class PicksResponse(BaseModel):
    picks: list

def _try_build_picks(min_edge: float, kelly_frac: float, max_risk_per_market: float, daily_risk_cap: float, bankroll: float):
    try:
        # Defer import so the app can still boot if training artifacts aren't present
        from src.bet.engine import build_picks  # type: ignore
        return build_picks(min_edge, kelly_frac, max_risk_per_market, daily_risk_cap, bankroll)
    except Exception as e:
        return {"note": f"build_picks unavailable: {e.__class__.__name__}: {e}"}

@app.get("/picks", response_model=PicksResponse)
def picks(
    min_edge: float = Query(0.015, ge=0, le=1),
    kelly_frac: float = Query(0.25, ge=0, le=1),
    max_risk_per_market: float = Query(0.02, ge=0, le=1),
    daily_risk_cap: float = Query(0.08, ge=0, le=1),
    bankroll: float = Query(100000.0, ge=0),
):
    data = _try_build_picks(min_edge, kelly_frac, max_risk_per_market, daily_risk_cap, bankroll)
    # If build_picks returned a dict with a note, wrap it so the schema matches
    if isinstance(data, dict):
        return {"picks": [data]}
    return {"picks": data}

# Mount routers
app.include_router(picks_live_router)
app.include_router(admin_router)

# ---------- helper ----------
def _run(cmd: list[str]):
    """Run a subprocess and log last 4000 chars of stdout/stderr; never crash server."""
    p = run(cmd, stdout=PIPE, stderr=PIPE, text=True)
    out, err = p.stdout[-4000:], p.stderr[-4000:]
    if p.returncode != 0:
        print(f"[BOOTSTRAP] ERROR {p.returncode} running: {' '.join(cmd)}\nSTDOUT:\n{out}\nSTDERR:\n{err}")
    else:
        print(f"[BOOTSTRAP] OK: {' '.join(cmd)}\n{out}")
    return p.returncode, out, err

def _ensure_files_on_start():
    """
    Zero-cost bootstrap: if CSVs are missing after a deploy, rebuild safe subsets so /picks_live works.
    - Full-game baseline: MLB only, markets=h2h
    - First-half baseline: MLB F5 only, capped in its module
    """
    data_dir = Path("./data")
    processed = data_dir / "processed"
    processed.mkdir(parents=True, exist_ok=True)

    regions = os.getenv("ODDS_API_REGIONS", "us,eu")

    need_full = not (processed / "market_baselines_h2h.csv").exists()
    need_first = not (processed / "market_baselines_firsthalf.csv").exists()

    # Build MLB full-game (cheap, one sport)
    if need_full:
        _run([
            "python", "-m", "src.etl.pull_odds_to_csv",
            "--sports", "baseball_mlb",
            "--regions", regions,
            "--markets", "h2h",
        ])
        _run(["python", "-m", "src.features.make_baseline_from_odds"])

    # Build MLB F5 (period odds)
    if need_first:
        _run([
            "python", "-m", "src.etl.pull_period_odds_to_csv",
            "--sports", "baseball_mlb",
            "--regions", regions,
            # puller itself is capped via its default; we can add --max_events "30" if needed
        ])
        _run(["python", "-m", "src.features.make_baseline_first_half"])

# ---------- startup hook ----------
@app.on_event("startup")
def bootstrap_on_start():
    # You can disable this by setting BOOTSTRAP_ON_START=0
    if os.getenv("BOOTSTRAP_ON_START", "1") not in ("0", "false", "False"):
        try:
            _ensure_files_on_start()
        except Exception as e:
            # Never block the app from starting
            print(f"[BOOTSTRAP] Exception during startup: {e.__class__.__name__}: {e}")

# touch 2025-10-03T21:41:57
