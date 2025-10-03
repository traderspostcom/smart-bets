from fastapi import APIRouter, Header, HTTPException, Query
from subprocess import run, PIPE
from pathlib import Path
import os

admin_router = APIRouter()

# ---------- Auth ----------
def _require_token(x_cron_token: str | None):
    expected = os.getenv("CRON_TOKEN")
    if not expected or not x_cron_token or x_cron_token != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")

# ---------- Helpers ----------
def _run(cmd: list[str]):
    """
    Run a subprocess and capture the last 4000 chars of stdout/stderr so we can
    debug issues without crashing the app.
    """
    p = run(cmd, stdout=PIPE, stderr=PIPE, text=True)
    return p.returncode, p.stdout[-4000:], p.stderr[-4000:]

# ---------- Inspect files ----------
@admin_router.get("/admin/list_files")
def list_files(x_cron_token: str | None = Header(default=None)):
    _require_token(x_cron_token)
    root = Path("./data")
    out = {}
    for sub in ["raw", "processed", "model_artifacts"]:
        p = root / sub
        if p.exists():
            out[sub] = sorted(
                [str(x.relative_to(root)) for x in p.rglob("*") if x.is_file()]
            )[:500]
        else:
            out[sub] = []
    return {"ok": True, "files": out}

# ---------- Safe full-game refresh (one sport) ----------
@admin_router.post("/admin/refresh_fullgame_safe")
def refresh_fullgame_safe(
    x_cron_token: str | None = Header(default=None),
    sport: str = Query("baseball_mlb", description="One sport key, e.g. baseball_mlb, basketball_nba"),
):
    """
    Safest refresh: one sport at a time, full-game moneyline only.
    Avoids long runs / crashes / credit spikes.
    """
    _require_token(x_cron_token)

    regions = os.getenv("ODDS_API_REGIONS", "us,eu")
    cmd1 = [
        "python", "-m", "src.etl.pull_odds_to_csv",
        "--sports", sport,
        "--regions", regions,
        "--markets", "h2h",
    ]
    cmd2 = ["python", "-m", "src.features.make_baseline_from_odds"]

    code1, out1, err1 = _run(cmd1)
    if code1 != 0:
        return {"ok": False, "step": "pull_odds", "code": code1, "stdout": out1, "stderr": err1}

    code2, out2, err2 = _run(cmd2)
    if code2 != 0:
        return {"ok": False, "step": "baseline", "code": code2, "stdout": out2, "stderr": err2}

    return {
        "ok": True,
        "steps": [
            {"cmd": " ".join(cmd1), "stdout": out1, "stderr": err1},
            {"cmd": " ".join(cmd2), "stdout": out2, "stderr": err2},
        ],
    }

# ---------- First-half / F5 refresh (bounded) ----------
@admin_router.post("/admin/refresh_firsthalf")
def refresh_firsthalf(
    x_cron_token: str | None = Header(default=None),
    sport: str = Query("baseball_mlb", description="Supported: baseball_mlb for F5"),
    max_events: int = Query(30, ge=1, le=100),
):
    """
    Period markets refresh (default MLB F5). Bounded by max_events to avoid timeouts/credit spikes.
    """
    _require_token(x_cron_token)

    regions = os.getenv("ODDS_API_REGIONS", "us,eu")
    cmd1 = [
        "python", "-m", "src.etl.pull_period_odds_to_csv",
        "--sports", sport,
        "--regions", regions,
        "--max_events", str(max_events),
    ]
    cmd2 = ["python", "-m", "src.features.make_baseline_first_half"]

    code1, out1, err1 = _run(cmd1)
    if code1 != 0:
        return {"ok": False, "step": "pull_period_odds", "code": code1, "stdout": out1, "stderr": err1}

    code2, out2, err2 = _run(cmd2)
    if code2 != 0:
        return {"ok": False, "step": "baseline_firsthalf", "code": code2, "stdout": out2, "stderr": err2}

    return {
        "ok": True,
        "steps": [
            {"cmd": " ".join(cmd1), "stdout": out1, "stderr": err1},
            {"cmd": " ".join(cmd2), "stdout": out2, "stderr": err2},
        ],
    }
