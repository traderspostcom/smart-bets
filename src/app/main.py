# src/app/main.py
import os
import sys
import subprocess
from datetime import datetime, timezone
from typing import Optional, List

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse

from .live import router as live_router  # only the router, no self-imports

# IMPORTANT: uvicorn loads this 'app'
app = FastAPI(title="Smart Bets")
app.include_router(live_router)

CRON_TOKEN = os.getenv("CRON_TOKEN", "")
RAW_DIR = "data/raw"
PROC_DIR = "data/processed"
FULLGAME_PATH = os.path.join(PROC_DIR, "market_baselines_h2h.csv")
FIRSTHALF_PATH = os.path.join(PROC_DIR, "market_baselines_firsthalf.csv")


def _need_auth(x_cron_token: Optional[str]) -> None:
    if not CRON_TOKEN:
        raise HTTPException(status_code=500, detail="CRON_TOKEN not set")
    if x_cron_token != CRON_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")


def _run(cmd: List[str]) -> dict:
    proc = subprocess.run(cmd, capture_output=True, text=True)
    return {
        "cmd": " ".join(cmd),
        "returncode": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }


@app.get("/health")
def health():
    return {"ok": True}


# Debug: prove which modules this instance will call
@app.get("/admin/which_builder")
def which_builder(x_cron_token: Optional[str] = Header(None)):
    _need_auth(x_cron_token)
    return {
        "ok": True,
        "fullgame_builder": "src.features.make_baseline_from_odds_v2",
        "firsthalf_builder": "src.features.make_baseline_first_half",
    }


@app.get("/admin/list_files")
def admin_list_files(x_cron_token: Optional[str] = Header(None)):
    _need_auth(x_cron_token)
    raw = [os.path.join(RAW_DIR, f) for f in sorted(os.listdir(RAW_DIR))] if os.path.isdir(RAW_DIR) else []
    processed = [os.path.join(PROC_DIR, f) for f in sorted(os.listdir(PROC_DIR))] if os.path.isdir(PROC_DIR) else []
    return {"ok": True, "files": {"raw": raw, "processed": processed, "model_artifacts": []}}


@app.get("/admin/debug_paths")
def admin_debug_paths(x_cron_token: Optional[str] = Header(None)):
    _need_auth(x_cron_token)
    return {
        "ok": True,
        "debug": {
            "cwd": os.getcwd(),
            "env_keys": sorted([k for k in os.environ.keys() if k in {
                "ODDS_API_HOST", "BOOKS_ALLOWED", "ODDS_API_MARKETS", "ODDS_API_KEY",
                "SPORTS_ALLOWED", "ODDS_API_REGIONS", "CRON_TOKEN", "LIVE_MIN_BOOKS"
            }]),
            "paths": {
                "raw_dir": RAW_DIR,
                "processed_dir": PROC_DIR,
                "raw_dir_exists": os.path.isdir(RAW_DIR),
                "processed_dir_exists": os.path.isdir(PROC_DIR),
                "fullgame_path": FULLGAME_PATH,
                "firsthalf_path": FIRSTHALF_PATH,
                "fullgame_size": os.path.getsize(FULLGAME_PATH) if os.path.exists(FULLGAME_PATH) else 0,
                "firsthalf_size": os.path.getsize(FIRSTHALF_PATH) if os.path.exists(FIRSTHALF_PATH) else 0,
            },
            "time": int(datetime.now(timezone.utc).timestamp()),
        },
    }


@app.post("/admin/refresh_fullgame_safe")
def refresh_fullgame_safe(
    sport: Optional[str] = None, x_cron_token: Optional[str] = Header(None)
):
    _need_auth(x_cron_token)

    sports = ["baseball_mlb", "basketball_nba", "americanfootball_nfl", "americanfootball_ncaaf", "icehockey_nhl"]
    if sport:
        sports = [sport]

    steps = []
    # Pull odds
    steps.append(_run([
        sys.executable, "-m", "src.etl.pull_odds_to_csv",
        "--sports", *sports,
        "--regions", os.getenv("ODDS_API_REGIONS", "us,eu"),
        "--markets", os.getenv("ODDS_API_MARKETS", "h2h"),
    ]))
    # Build baseline with the new long/wide-tolerant builder
    steps.append(_run([sys.executable, "-m", "src.features.make_baseline_from_odds_v2"]))

    ok = all(s["returncode"] == 0 for s in steps)
    if not ok:
        failing = next((s for s in steps if s["returncode"] != 0), steps[-1])
        return JSONResponse(
            {"ok": False, "step": "baseline" if "make_baseline" in failing["cmd"] else "pull", **failing},
            status_code=500
        )
    return {"ok": True, "steps": steps}


@app.post("/admin/refresh_firsthalf")
def refresh_firsthalf(
    sport: Optional[str] = None, x_cron_token: Optional[str] = Header(None)
):
    _need_auth(x_cron_token)

    sports = ["baseball_mlb", "basketball_nba", "americanfootball_nfl", "americanfootball_ncaaf"]
    if sport:
        sports = [sport]

    steps = []
    steps.append(_run([
        sys.executable, "-m", "src.etl.pull_period_odds_to_csv",
        "--sports", *sports,
        "--regions", os.getenv("ODDS_API_REGIONS", "us,eu"),
    ]))
    steps.append(_run([sys.executable, "-m", "src.features.make_baseline_first_half"]))

    ok = all(s["returncode"] == 0 for s in steps)
    if not ok:
        failing = next((s for s in steps if s["returncode"] != 0), steps[-1])
        return JSONResponse(
            {"ok": False, "step": "baseline" if "make_baseline" in failing["cmd"] else "pull", **failing},
            status_code=500
        )
    return {"ok": True, "steps": steps}
