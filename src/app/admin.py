# src/app/admin.py
from __future__ import annotations

import os
import sys
import json
import subprocess
from typing import List, Dict, Any, Optional

from fastapi import APIRouter, Header, HTTPException

admin_router = APIRouter()

# --- Auth helper (require x-cron-token on all /admin/*) -------------------------
def _require_token(x_cron_token: Optional[str]):
    want = os.getenv("CRON_TOKEN", "")
    if not want:
        # If no CRON_TOKEN configured, allow (useful for local dev)
        return
    if x_cron_token != want:
        raise HTTPException(status_code=401, detail="unauthorized")

# --- Subprocess helpers ---------------------------------------------------------
def _run_py_module(module_str: str, args: List[str]) -> Dict[str, Any]:
    cmd = [sys.executable, "-m", module_str, *args]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    return {
        "cmd": " ".join(cmd),
        "returncode": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }

def _sanitize_markets(env_val: str) -> str:
    # Remove blanks & trailing commas. Only allow known keys.
    allow = {"h2h", "spreads", "totals"}
    parts = [p.strip() for p in (env_val or "").split(",") if p.strip()]
    parts = [p for p in parts if p in allow]
    return ",".join(parts)

# --- Introspection endpoints ----------------------------------------------------
@admin_router.get("/admin/which_builder")
def which_builder(x_cron_token: Optional[str] = Header(None)):
    _require_token(x_cron_token)
    return {
        "ok": True,
        "fullgame_builder": "src.features.make_baseline_from_odds_v2",
        "firsthalf_builder": "src.features.make_baseline_first_half_v2",
    }

@admin_router.get("/admin/list_files")
def list_files(x_cron_token: Optional[str] = Header(None)):
    _require_token(x_cron_token)
    paths = {
        "raw": [],
        "processed": [],
        "model_artifacts": [],
    }
    for p in ("data/raw", "data/processed", "models", "model_artifacts"):
        if os.path.isdir(p):
            for root, _, files in os.walk(p):
                for f in files:
                    rel = os.path.join(root, f)
                    if rel.startswith("data/raw"):
                        paths["raw"].append(rel)
                    elif rel.startswith("data/processed"):
                        paths["processed"].append(rel)
                    elif rel.startswith("models") or rel.startswith("model_artifacts"):
                        paths["model_artifacts"].append(rel)
    return {"ok": True, "files": paths}

@admin_router.get("/admin/debug_paths")
def debug_paths(x_cron_token: Optional[str] = Header(None)):
    _require_token(x_cron_token)
    raw_dir = "data/raw"
    processed_dir = "data/processed"
    fullgame_path = os.path.join(processed_dir, "market_baselines_h2h.csv")
    firsthalf_path = os.path.join(processed_dir, "market_baselines_firsthalf.csv")
    def _size(p): 
        try: return os.path.getsize(p)
        except: return 0
    return {
        "ok": True,
        "debug": {
            "cwd": os.getcwd(),
            "env_keys": sorted([k for k in os.environ.keys() if k in {
                "ODDS_API_HOST","ODDS_API_KEY","ODDS_API_REGIONS","ODDS_API_MARKETS",
                "BOOKS_ALLOWED","SPORTS_ALLOWED","CRON_TOKEN"
            }]),
            "paths": {
                "raw_dir": raw_dir,
                "processed_dir": processed_dir,
                "raw_dir_exists": os.path.isdir(raw_dir),
                "processed_dir_exists": os.path.isdir(processed_dir),
                "fullgame_path": fullgame_path,
                "firsthalf_path": firsthalf_path,
                "fullgame_size": _size(fullgame_path),
                "firsthalf_size": _size(firsthalf_path),
            },
            "time": int(__import__("time").time()),
        }
    }

# --- Build pipelines ------------------------------------------------------------
@admin_router.post("/admin/refresh_fullgame_safe")
def refresh_fullgame_safe(x_cron_token: Optional[str] = Header(None)):
    _require_token(x_cron_token)

    odds_api_markets = _sanitize_markets(os.getenv("ODDS_API_MARKETS", "h2h,spreads,totals"))
    if not odds_api_markets:
        odds_api_markets = "h2h"  # fallback

    # 1) Pull odds (long form)
    args_pull = [
        "--sports", "baseball_mlb", "basketball_nba", "americanfootball_nfl", "americanfootball_ncaaf", "icehockey_nhl",
        "--regions", os.getenv("ODDS_API_REGIONS", "us,eu"),
        "--markets", odds_api_markets,
    ]
    step1 = _run_py_module("src.etl.pull_odds_to_csv", args_pull)
    if step1["returncode"] != 0:
        return {"ok": False, "step": "pull", **step1}

    # 2) Build full-game baselines (v2)
    step2 = _run_py_module("src.features.make_baseline_from_odds_v2", [])
    if step2["returncode"] != 0:
        return {"ok": False, "step": "baseline", **step2}

    return {"ok": True, "steps": [step1, step2]}

@admin_router.post("/admin/refresh_firsthalf")
def refresh_firsthalf(x_cron_token: Optional[str] = Header(None)):
    _require_token(x_cron_token)

    # 1) Pull period/F5 odds
    args_pull = [
        "--sports", "baseball_mlb", "basketball_nba", "americanfootball_nfl", "americanfootball_ncaaf",
        "--regions", os.getenv("ODDS_API_REGIONS", "us,eu"),
    ]
    step1 = _run_py_module("src.etl.pull_period_odds_to_csv", args_pull)
    if step1["returncode"] != 0:
        return {"ok": False, "step": "pull", **step1}

    # 2) Build first-half/F5 baselines (v2)
    step2 = _run_py_module("src.features.make_baseline_first_half_v2", [])
    if step2["returncode"] != 0:
        return {"ok": False, "step": "baseline", **step2}

    return {"ok": True, "steps": [step1, step2]}

# --- NEW: Peek into first-half CSV to diagnose filters --------------------------
@admin_router.get("/admin/peek_firsthalf_sample")
def peek_firsthalf_sample(limit: int = 10, x_cron_token: Optional[str] = Header(None)):
    _require_token(x_cron_token)

    import pandas as pd

    path = "data/processed/market_baselines_firsthalf.csv"
    if not os.path.exists(path):
        return {"ok": False, "exists": False, "note": "missing file"}

    try:
        df = pd.read_csv(path)
    except Exception as e:
        return {"ok": False, "exists": True, "note": f"read failed: {e}"}

    n = len(df)
    cols = list(df.columns)

    # Summaries that help explain why /picks_live may return []
    summary = {}
    for key in ("sport_key",):
        if key in df.columns:
            summary[f"counts_by_{key}"] = df[key].value_counts(dropna=False).to_dict()

    for key in ("num_books",):
        if key in df.columns:
            summary["num_books_min"] = float(df[key].min()) if len(df) else None
            summary["num_books_max"] = float(df[key].max()) if len(df) else None

    if "books_used" in df.columns:
        # show top distinct book-sets (trimmed)
        top_sets = (
            df["books_used"]
            .fillna("")
            .value_counts()
            .head(10)
            .to_dict()
        )
        summary["top_books_used_sets"] = top_sets

    head_rows = df.head(limit).to_dict(orient="records")

    return {
        "ok": True,
        "exists": True,
        "rows": n,
        "columns": cols,
        "summary": summary,
        "sample": head_rows,
    }
