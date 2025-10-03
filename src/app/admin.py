# src/app/admin.py
from __future__ import annotations

import os
import sys
from pathlib import Path
from subprocess import run, PIPE
from typing import Dict, List, Tuple, Optional

from fastapi import APIRouter, Header, HTTPException

# ---------- Router ----------
admin_router = APIRouter()


# ---------- Helpers ----------
def _require_token(x_cron_token: Optional[str]) -> None:
    expected = os.getenv("CRON_TOKEN")
    if not expected or not x_cron_token or x_cron_token != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")


def _run(mod_and_args: List[str]) -> Tuple[int, str, str]:
    """
    Run a Python module with the SAME interpreter that runs FastAPI (your venv).
    Example: _run(["-m", "src.etl.pull_odds_to_csv", "--sports", "baseball_mlb"])
    """
    cmd = [sys.executable, *mod_and_args]
    p = run(cmd, stdout=PIPE, stderr=PIPE, text=True)
    return p.returncode, p.stdout[-4000:], p.stderr[-4000:]


def _csv_env(name: str, default_csv: str = "") -> List[str]:
    raw = os.getenv(name, default_csv).strip()
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]


def _list_files_under(dir_path: Path) -> List[str]:
    if not dir_path.exists():
        return []
    out: List[str] = []
    for p in sorted(dir_path.rglob("*")):
        if p.is_file():
            rel = p.as_posix()
            out.append(rel)
    return out


# ---------- Defaults / env controls ----------
# Regions for the Odds API pulls (default to US + EU so you get Pinnacle)
ODDS_API_REGIONS = os.getenv("ODDS_API_REGIONS", "us,eu")

# Which sports to pull if not provided (you can override via SPORTS_ALLOWED env)
DEFAULT_SPORTS = [
    "americanfootball_nfl",
    "icehockey_nhl",
    "baseball_mlb",
    "basketball_nba",
    "americanfootball_ncaaf",
]

SPORTS_ALLOWED = _csv_env("SPORTS_ALLOWED") or DEFAULT_SPORTS

# Optional: restrict consensus to certain books in post-processing steps (not enforced here;
# the feature scripts can use BOOKS_ALLOWED too if you wired that up there)
BOOKS_ALLOWED = _csv_env("BOOKS_ALLOWED")  # empty means "all books"


# ---------- Admin endpoints ----------
@admin_router.post("/admin/refresh_fullgame_safe")
def refresh_fullgame_safe(x_cron_token: Optional[str] = Header(default=None)):
    """
    Pulls full-game moneyline odds (h2h) for allowed sports, then builds the consensus baseline file:
      data/processed/market_baselines_h2h.csv
    """
    _require_token(x_cron_token)

    sports = SPORTS_ALLOWED  # already a list
    steps: List[Dict[str, str]] = []

    # 1) Pull odds snapshot -> data/raw/odds_latest.csv
    cmd1 = [
        "-m",
        "src.etl.pull_odds_to_csv",
        "--sports",
        *sports,
        "--regions",
        ODDS_API_REGIONS,
        "--markets",
        "h2h",
    ]
    code1, out1, err1 = _run(cmd1)
    if code1 != 0:
        return {"ok": False, "step": "pull_odds", "code": code1, "stdout": out1, "stderr": err1}
    steps.append({"cmd": " ".join(cmd1), "stdout": out1, "stderr": err1})

    # 2) Build no-vig consensus baselines -> data/processed/market_baselines_h2h.csv
    cmd2 = ["-m", "src.features.make_baseline_from_odds"]
    code2, out2, err2 = _run(cmd2)
    if code2 != 0:
        return {"ok": False, "step": "baseline_h2h", "code": code2, "stdout": out2, "stderr": err2}
    steps.append({"cmd": " ".join(cmd2), "stdout": out2, "stderr": err2})

    return {"ok": True, "steps": steps}


@admin_router.post("/admin/refresh_firsthalf")
def refresh_firsthalf(x_cron_token: Optional[str] = Header(default=None)):
    """
    Pulls period markets and builds the first-half (NBA/NFL/NCAAF) and F5 (MLB) baseline file:
      data/processed/market_baselines_firsthalf.csv
    """
    _require_token(x_cron_token)

    # The period puller decides which period markets to request per sport
    # (e.g., NBA/NFL/NCAAF -> h2h_h1, MLB -> h2h_1st_5_innings)
    sports = [s for s in SPORTS_ALLOWED if s in {
        "basketball_nba", "americanfootball_nfl", "americanfootball_ncaaf", "baseball_mlb"
    }]

    steps: List[Dict[str, str]] = []

    # 1) Pull period odds snapshot -> data/raw/odds_periods_latest.csv
    cmd1 = [
        "-m",
        "src.etl.pull_period_odds_to_csv",
        "--sports",
        *sports,
        "--regions",
        ODDS_API_REGIONS,
    ]
    code1, out1, err1 = _run(cmd1)
    if code1 != 0:
        return {"ok": False, "step": "pull_period_odds", "code": code1, "stdout": out1, "stderr": err1}
    steps.append({"cmd": " ".join(cmd1), "stdout": out1, "stderr": err1})

    # 2) Build first-half / F5 baselines -> data/processed/market_baselines_firsthalf.csv
    cmd2 = ["-m", "src.features.make_baseline_first_half"]
    code2, out2, err2 = _run(cmd2)
    if code2 != 0:
        return {"ok": False, "step": "baseline_firsthalf", "code": code2, "stdout": out2, "stderr": err2}
    steps.append({"cmd": " ".join(cmd2), "stdout": out2, "stderr": err2})

    return {"ok": True, "steps": steps}


@admin_router.get("/admin/list_files")
def list_files(x_cron_token: Optional[str] = Header(default=None)):
    """
    Quick inventory for debugging what exists on disk inside the Render container.
    """
    _require_token(x_cron_token)

    root = Path(".")
    raw = _list_files_under(root / "data" / "raw")
    processed = _list_files_under(root / "data" / "processed")
    artifacts = _list_files_under(root / "data" / "model_artifacts")

    return {
        "ok": True,
        "files": {
            "raw": raw,
            "processed": processed,
            "model_artifacts": artifacts,
        },
    }
