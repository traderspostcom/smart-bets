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
    if p.returncode != 0:
        raise CalledProcessError(p.returncode, cmd, output=p.stdout, stderr=p.stderr)
    return {"cmd": " ".join(cmd), "stdout": p.stdout[-1000:], "stderr": p.stderr[-1000:]}

@admin_router.post("/admin/refresh")
def refresh(x_cron_token: str | None = Header(default=None)):
    _require_token(x_cron_token)

    # Pull odds for NFL/NHL/MLB/NBA/NCAAF (you can tune later)
    sports = [
        "americanfootball_nfl",
        "icehockey_nhl",
        "baseball_mlb",
        "basketball_nba",
        "americanfootball_ncaaf",
    ]
    cmd1 = ["python", "-m", "src.etl.pull_odds_to_csv", "--sports", *sports]
    cmd2 = ["python", "-m", "src.features.make_baseline_from_odds"]

    out1 = _run(cmd1)
    out2 = _run(cmd2)
    return {"ok": True, "steps": [out1, out2]}
