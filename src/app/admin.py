from fastapi import APIRouter, Header, HTTPException
from subprocess import run, PIPE
from pathlib import Path
import os

admin_router = APIRouter()

def _require_token(x_cron_token: str | None):
    expected = os.getenv("CRON_TOKEN")
    if not expected or not x_cron_token or x_cron_token != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")

def _run(cmd: list[str]):
    p = run(cmd, stdout=PIPE, stderr=PIPE, text=True)
    return p.returncode, p.stdout[-4000:], p.stderr[-4000:]

@admin_router.get("/admin/list_files")
def list_files(x_cron_token: str | None = Header(default=None)):
    """
    List files under ./data to see what's actually on disk.
    """
    _require_token(x_cron_token)
    root = Path("./data")
    out = {}
    for sub in ["raw", "processed", "model_artifacts"]:
        p = root / sub
        if p.exists():
            out[sub] = sorted([str(x.relative_to(root)) for x in p.rglob("*") if x.is_file()])[:500]
        else:
            out[sub] = []
    return {"ok": True, "files": out}

@admin_router.post("/admin/refresh")
def refresh(x_cron_token: str | None = Header(default=None)):
    """
    Refresh FULL-GAME markets (moneyline only).
    """
    _require_token(x_cron_token)

    regions = os.getenv("ODDS_API_REGIONS", "us,eu")
    sports = [
        "americanfootball_nfl",
        "icehockey_nhl",
        "baseball_mlb",
        "basketball_nba",
        "americanfootball_ncaaf",
    ]
    cmd1 = ["python", "-m", "src.etl.pull_odds_to_csv",
            "--sports", *sports,
            "--regions", regions,
            "--markets", "h2h"]
    cmd2 = ["python", "-m", "src.features.make_baseline_from_odds"]

    code1, out1, err1 = _run(cmd1)
    if code1 != 0:
        return {"ok": False, "step": "pull_odds", "code": code1, "stdout": out1, "stderr": err1}

    code2, out2, err2 = _run(cmd2)
    if code2 != 0:
        return {"ok": False, "step": "baseline", "code": code2, "stdout": out2, "stderr": err2}

    return {"ok": True, "steps": [
        {"cmd": " ".join(cmd1), "stdout": out1, "stderr": err1},
        {"cmd": " ".join(cmd2), "stdout": out2, "stderr": err2},
    ]}

@admin_router.post("/admin/refresh_firsthalf")
def refresh_firsthalf(x_cron_token: str | None = Header(default=None)):
    """
    Refresh FIRST-HALF / F5 markets (currently MLB F5 for stability).
    """
    _require_token(x_cron_token)

    regions = os.getenv("ODDS_API_REGIONS", "us,eu")
    sports = ["baseball_mlb"]

    cmd1 = ["python", "-m", "src.etl.pull_period_odds_to_csv",
            "--sports", *sports,
            "--regions", regions,
            "--max_events", "30"]
    cmd2 = ["python", "-m", "src.features.make_baseline_first_half"]

    code1, out1, err1 = _run(cmd1)
    if code1 != 0:
        return {"ok": False, "step": "pull_period_odds", "code": code1, "stdout": out1, "stderr": err1}

    code2, out2, err2 = _run(cmd2)
    if code2 != 0:
        return {"ok": False, "step": "baseline_firsthalf", "code": code2, "stdout": out2, "stderr": err2}

    return {"ok": True, "steps": [
        {"cmd": " ".join(cmd1), "stdout": out1, "stderr": err1},
        {"cmd": " ".join(cmd2), "stdout": out2, "stderr": err2},
    ]}

# touch for redeploy
