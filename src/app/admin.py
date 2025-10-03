from fastapi import APIRouter, Header, HTTPException
from subprocess import run, CalledProcessError, PIPE
import os

admin_router = APIRouter()

def _require_token(x_cron_token: str | None):
    expected = os.getenv("CRON_TOKEN")
    if not expected or not x_cron_token or x_cron_token != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")

def _run(cmd: list[str]):
    p = run(cmd, stdout=PIPE, stderr=PIPE, text=True)
    return p.returncode, p.stdout[-4000:], p.stderr[-4000:]

@admin_router.post("/admin/refresh")
def refresh(x_cron_token: str | None = Header(default=None)):
    _require_token(x_cron_token)

    sports = [
        "americanfootball_nfl",
        "icehockey_nhl",
        "baseball_mlb",
        "basketball_nba",
        "americanfootball_ncaaf",
    ]
    cmd1 = ["python", "-m", "src.etl.pull_odds_to_csv", "--sports", *sports]
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
    _require_token(x_cron_token)

    # Pull first-half (H1) for NFL/NCAAF/NBA and F5 for MLB
    sports = [
        "basketball_nba",
        "americanfootball_nfl",
        "americanfootball_ncaaf",
        "baseball_mlb",
    ]
    cmd1 = ["python", "-m", "src.etl.pull_period_odds_to_csv", "--sports", *sports]
    cmd2 = ["python", "-m", "src.features.make_baseline_first_half"]

    # Reuse the helper for detailed outputs
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

