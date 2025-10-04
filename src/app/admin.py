from fastapi import APIRouter, Header, HTTPException, Query
from pathlib import Path
from subprocess import run, PIPE
import os
import time

admin_router = APIRouter()

DATA_DIR = Path("data")
RAW = DATA_DIR / "raw"
PROCESSED = DATA_DIR / "processed"

def _require_token(x_cron_token: str | None):
    expected = os.getenv("CRON_TOKEN")
    if not expected or not x_cron_token or x_cron_token != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")

def _run_py_module(module: str, args: list[str], timeout: int = 180):
    cmd = ["python", "-m", module, *args]
    p = run(cmd, stdout=PIPE, stderr=PIPE, text=True, timeout=timeout)
    return p.returncode, p.stdout[-4000:], p.stderr[-4000:], cmd

def _size_or_zero(p: Path) -> int:
    try:
        return p.stat().st_size if p.exists() else 0
    except Exception:
        return 0

@admin_router.get("/admin/list_files")
def list_files(x_cron_token: str | None = Header(default=None)):
    _require_token(x_cron_token)
    raw_files = [str(p) for p in RAW.glob("*") if p.is_file()]
    proc_files = [str(p) for p in PROCESSED.glob("*") if p.is_file()]
    return {"ok": True, "files": {"raw": raw_files, "processed": proc_files, "model_artifacts": []}}

@admin_router.get("/admin/debug_paths")
def debug_paths(x_cron_token: str | None = Header(default=None)):
    _require_token(x_cron_token)
    info = {
        "cwd": os.getcwd(),
        "env_keys": [k for k in os.environ.keys() if k in {
            "CRON_TOKEN", "ODDS_API_KEY", "ODDS_API_HOST", "ODDS_API_REGIONS",
            "ODDS_API_MARKETS", "SPORTS_ALLOWED", "BOOKS_ALLOWED",
            "LIVE_MAX_AGE_MINUTES", "LIVE_MIN_BOOKS"
        }],
        "paths": {
            "raw_dir": str(RAW), "processed_dir": str(PROCESSED),
            "raw_dir_exists": RAW.exists(), "processed_dir_exists": PROCESSED.exists(),
            "fullgame_path": str(PROCESSED / "market_baselines_h2h.csv"),
            "firsthalf_path": str(PROCESSED / "market_baselines_firsthalf.csv"),
            "fullgame_size": _size_or_zero(PROCESSED / "market_baselines_h2h.csv"),
            "firsthalf_size": _size_or_zero(PROCESSED / "market_baselines_firsthalf.csv"),
        },
        "time": int(time.time()),
    }
    return {"ok": True, "debug": info}

@admin_router.post("/admin/refresh_fullgame_safe")
def refresh_fullgame_safe(
    x_cron_token: str | None = Header(default=None),
    sport: str | None = Query(default=None),
):
    _require_token(x_cron_token)

    sports = [
        "baseball_mlb",
        "basketball_nba",
        "americanfootball_nfl",
        "americanfootball_ncaaf",
        "icehockey_nhl",
    ]
    if sport:
        sports = [sport]

    regions = os.getenv("ODDS_API_REGIONS", "us,eu")
    markets = "h2h"

    code1, out1, err1, cmd1 = _run_py_module(
        "src.etl.pull_odds_to_csv",
        ["--sports", *sports, "--regions", regions, "--markets", markets]
    )
    if code1 != 0:
        return {"ok": False, "step": "pull_odds", "code": code1, "stdout": out1, "stderr": err1, "cmd": " ".join(cmd1)}

    code2, out2, err2, cmd2 = _run_py_module(
        "src.features.make_baseline_from_odds_v2",
        []
    )
    if code2 != 0:
        return {"ok": False, "step": "baseline", "code": code2, "stdout": out2, "stderr": err2, "cmd": " ".join(cmd2)}

    return {"ok": True, "steps": [
        {"cmd": " ".join(cmd1), "stdout": out1, "stderr": err1},
        {"cmd": " ".join(cmd2), "stdout": out2, "stderr": err2},
    ]}

@admin_router.post("/admin/refresh_firsthalf")
def refresh_firsthalf(
    x_cron_token: str | None = Header(default=None),
    sport: str | None = Query(default=None),
):
    _require_token(x_cron_token)

    sports = [
        "baseball_mlb",
        "basketball_nba",
        "americanfootball_nfl",
        "americanfootball_ncaaf",
    ]
    if sport:
        sports = [sport]

    regions = os.getenv("ODDS_API_REGIONS", "us,eu")

    code1, out1, err1, cmd1 = _run_py_module(
        "src.etl.pull_period_odds_to_csv",
        ["--sports", *sports, "--regions", regions]
    )
    if code1 != 0:
        return {"ok": False, "step": "pull_period_odds", "code": code1, "stdout": out1, "stderr": err1, "cmd": " ".join(cmd1)}

    code2, out2, err2, cmd2 = _run_py_module(
        "src.features.make_baseline_first_half",
        []
    )
    if code2 != 0:
        return {"ok": False, "step": "baseline_firsthalf", "code": code2, "stdout": out2, "stderr": err2, "cmd": " ".join(cmd2)}

    return {"ok": True, "steps": [
        {"cmd": " ".join(cmd1), "stdout": out1, "stderr": err1},
        {"cmd": " ".join(cmd2), "stdout": out2, "stderr": err2},
    ]}
