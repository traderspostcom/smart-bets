# src/app/admin.py
from fastapi import APIRouter, Header, HTTPException
from subprocess import run, PIPE
import os
from typing import List

admin_router = APIRouter()

# ---------- helpers ----------
def _require_token(x_cron_token: str | None):
    expected = os.getenv("CRON_TOKEN")
    if not expected or not x_cron_token or x_cron_token != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")

def _run(cmd: list[str]) -> tuple[int, str, str]:
    p = run(cmd, stdout=PIPE, stderr=PIPE, text=True)
    return p.returncode, p.stdout[-4000:], p.stderr[-4000:]

def _parse_csv_env(key: str) -> List[str]:
    raw = os.getenv(key, "") or ""
    return [s.strip() for s in raw.split(",") if s.strip()]

def _sports_allowed(defaults: List[str]) -> List[str]:
    allowed = _parse_csv_env("SPORTS_ALLOWED")
    return allowed if allowed else defaults

def _filter_books_if_set(input_csv: str):
    """
    Applies BOOKS_ALLOWED allowlist to the processed baselines CSV.
    Writes back in place. If BOOKS_ALLOWED is empty, it's a no-op.
    """
    books = os.getenv("BOOKS_ALLOWED", "").strip()
    if books == "":
        return {"filtered": False, "rows": None, "note": "BOOKS_ALLOWED empty; no filter applied."}

    code, out, err = _run([
        "python", "-m", "src.features.filter_books", input_csv, input_csv
    ])
    return {"filtered": code == 0, "stdout": out, "stderr": err}

# ---------- endpoints ----------

@admin_router.post("/admin/refresh")  # legacy full-game (kept for compatibility)
def refresh_legacy(x_cron_token: str | None = Header(default=None)):
    return refresh_fullgame_safe(x_cron_token=x_cron_token)

@admin_router.post("/admin/refresh_fullgame_safe")
def refresh_fullgame_safe(x_cron_token: str | None = Header(default=None)):
    _require_token(x_cron_token)

    # Full-game H2H for allowed sports
    sports = _sports_allowed([
        "americanfootball_nfl",
        "icehockey_nhl",
        "baseball_mlb",
        "basketball_nba",
        "americanfootball_ncaaf",
    ])

    # Pull odds (moneyline) into raw
    cmd1 = ["python", "-m", "src.etl.pull_odds_to_csv", "--sports", *sports]
    code1, out1, err1 = _run(cmd1)
    if code1 != 0:
        return {"ok": False, "step": "pull_odds", "code": code1, "stdout": out1, "stderr": err1}

    # Build baselines
    cmd2 = ["python", "-m", "src.features.make_baseline_from_odds"]
    code2, out2, err2 = _run(cmd2)
    if code2 != 0:
        return {"ok": False, "step": "baseline_h2h", "code": code2, "stdout": out2, "stderr": err2}

    # Apply book allowlist (if any)
    filt = _filter_books_if_set("data/processed/market_baselines_h2h.csv")

    return {
        "ok": True,
        "steps": [
            {"cmd": " ".join(cmd1), "stdout": out1, "stderr": err1},
            {"cmd": " ".join(cmd2), "stdout": out2, "stderr": err2},
            {"book_filter": filt},
        ],
    }

@admin_router.post("/admin/refresh_firsthalf")  # first-half/F5 legacy route (this is the one you use)
def refresh_firsthalf(x_cron_token: str | None = Header(default=None)):
    _require_token(x_cron_token)

    sports = _sports_allowed([
        "basketball_nba",
        "americanfootball_nfl",
        "americanfootball_ncaaf",
        "baseball_mlb",
    ])

    # Pull period odds (H1/F5) into raw
    cmd1 = ["python", "-m", "src.etl.pull_period_odds_to_csv", "--sports", *sports]
    code1, out1, err1 = _run(cmd1)
    if code1 != 0:
        return {"ok": False, "step": "pull_period_odds", "code": code1, "stdout": out1, "stderr": err1}

    # Build first-half/F5 baselines
    cmd2 = ["python", "-m", "src.features.make_baseline_first_half"]
    code2, out2, err2 = _run(cmd2)
    if code2 != 0:
        return {"ok": False, "step": "baseline_firsthalf", "code": code2, "stdout": out2, "stderr": err2}

    # Apply book allowlist (if any)
    filt = _filter_books_if_set("data/processed/market_baselines_firsthalf.csv")

    return {
        "ok": True,
        "steps": [
            {"cmd": " ".join(cmd1), "stdout": out1, "stderr": err1},
            {"cmd": " ".join(cmd2), "stdout": out2, "stderr": err2},
            {"book_filter": filt},
        ],
    }

@admin_router.get("/admin/list_files")
def list_files(x_cron_token: str | None = Header(default=None)):
    _require_token(x_cron_token)
    def _ls(p): 
        try:
            return sorted(os.listdir(p))
        except Exception:
            return []
    return {
        "ok": True,
        "files": {
            "raw": _ls("data/raw"),
            "processed": _ls("data/processed"),
            "model_artifacts": _ls("data/model_artifacts"),
        }
    }
