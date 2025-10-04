# src/app/admin.py
import os
import sys
import subprocess
from typing import Optional, List

from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import JSONResponse

admin_router = APIRouter()

CRON_TOKEN = os.getenv("CRON_TOKEN", "")

def _need_auth(x_cron_token: Optional[str]) -> None:
    if not CRON_TOKEN:
        raise HTTPException(status_code=500, detail="CRON_TOKEN not set")
    if x_cron_token != CRON_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")

def _run_py_module(module: str, args: List[str]) -> tuple[int, str, str, str]:
    cmd = [sys.executable, "-m", module] + args
    proc = subprocess.run(cmd, capture_output=True, text=True)
    return proc.returncode, proc.stdout, proc.stderr, " ".join(cmd)

RAW_DIR = "data/raw"
PROC_DIR = "data/processed"
FULLGAME_PATH = os.path.join(PROC_DIR, "market_baselines_h2h.csv")
FIRSTHALF_PATH = os.path.join(PROC_DIR, "market_baselines_firsthalf.csv")

# === Debug: tell us which builder module this router will call ===
@admin_router.get("/admin/which_builder")
def which_builder(x_cron_token: Optional[str] = Header(None)):
    _need_auth(x_cron_token)
    return {
        "ok": True,
        "fullgame_builder": "src.features.make_baseline_from_odds_v2",
        "firsthalf_builder": "src.features.make_baseline_first_half",  # unchanged for now
    }

@admin_router.get("/admin/list_files")
def admin_list_files(x_cron_token: Optional[str] = Header(None)):
    _need_auth(x_cron_token)
    raw = [os.path.join(RAW_DIR, f) for f in sorted(os.listdir(RAW_DIR))] if os.path.isdir(RAW_DIR) else []
    processed = [os.path.join(PROC_DIR, f) for f in sorted(os.listdir(PROC_DIR))] if os.path.isdir(PROC_DIR) else []
    return {"ok": True, "files": {"raw": raw, "processed": processed, "model_artifacts": []}}

@admin_router.post("/admin/refresh_fullgame_safe")
def refresh_fullgame_safe(
    sport: Optional[str] = None,
    x_cron_token: Optional[str] = Header(None),
):
    _need_auth(x_cron_token)

    sports = ["baseball_mlb", "basketball_nba", "americanfootball_nfl", "americanfootball_ncaaf", "icehockey_nhl"]
    if sport:
        sports = [sport]

    # Pull odds
    code1, out1, err1, cmd1 = _run_py_module(
        "src.etl.pull_odds_to_csv",
        ["--sports", *sports, "--regions", os.getenv("ODDS_API_REGIONS", "us,eu"),
         "--markets", os.getenv("ODDS_API_MARKETS", "h2h")]
    )
    if code1 != 0:
        return JSONResponse({"ok": False, "step": "pull", "code": code1, "stdout": out1, "stderr": err1, "cmd": cmd1}, status_code=500)

    # Build baseline (v2 long/wide tolerant)
    code2, out2, err2, cmd2 = _run_py_module(
        "src.features.make_baseline_from_odds_v2",
        []
    )
    if code2 != 0:
        return JSONResponse({"ok": False, "step": "baseline", "code": code2, "stdout": out2, "stderr": err2, "cmd": cmd2}, status_code=500)

    return {"ok": True, "steps": [
        {"cmd": cmd1, "returncode": code1, "stdout": out1, "stderr": err1},
        {"cmd": cmd2, "returncode": code2, "stdout": out2, "stderr": err2},
    ]}

@admin_router.post("/admin/refresh_firsthalf")
def refresh_firsthalf(
    sport: Optional[str] = None,
    x_cron_token: Optional[str] = Header(None),
):
    _need_auth(x_cron_token)

    sports = ["baseball_mlb", "basketball_nba", "americanfootball_nfl", "americanfootball_ncaaf"]
    if sport:
        sports = [sport]

    code1, out1, err1, cmd1 = _run_py_module(
        "src.etl.pull_period_odds_to_csv",
        ["--sports", *sports, "--regions", os.getenv("ODDS_API_REGIONS", "us,eu")]
    )
    if code1 != 0:
        return JSONResponse({"ok": False, "step": "pull", "code": code1, "stdout": out1, "stderr": err1, "cmd": cmd1}, status_code=500)

    code2, out2, err2, cmd2 = _run_py_module(
        "src.features.make_baseline_first_half",
        []
    )
    if code2 != 0:
        return JSONResponse({"ok": False, "step": "baseline", "code": code2, "stdout": out2, "stderr": err2, "cmd": cmd2}, status_code=500)

    return {"ok": True, "steps": [
        {"cmd": cmd1, "returncode": code1, "stdout": out1, "stderr": err1},
        {"cmd": cmd2, "returncode": code2, "stdout": out2, "stderr": err2},
    ]}
