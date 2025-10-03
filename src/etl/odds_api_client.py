# src/etl/odds_api_client.py
from __future__ import annotations

import os
import json
import time
import errno
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests


class OddsAPIError(Exception):
    pass


# ---------- Environment / defaults ----------
ODDS_API_HOST = os.getenv("ODDS_API_HOST", "https://api.the-odds-api.com")
ODDS_API_KEY = os.getenv("ODDS_API_KEY", "")

# Default regions/markets if caller doesn’t pass any
DEFAULT_REGIONS = os.getenv("ODDS_API_REGIONS", "us,eu")
DEFAULT_MARKETS = os.getenv("ODDS_API_MARKETS", "h2h")
DEFAULT_ODDS_FORMAT = os.getenv("ODDS_API_FORMAT", "american")

# Budget lock
BUDGET_PATH = Path(os.getenv("ODDS_BUDGET_STORE", "/tmp/odds_budget.json"))
DAILY_BUDGET = int(os.getenv("ODDS_DAILY_BUDGET", "0") or "0")  # 0 = disabled
CRON_TZ = os.getenv("CRON_TZ", "UTC")


# ---------- Budget helpers ----------
def _tzinfo_from_name(name: str):
    try:
        from zoneinfo import ZoneInfo  # Python 3.9+
        return ZoneInfo(name)
    except Exception:
        return timezone.utc  # fallback


def _today_key(now_ts: Optional[float] = None) -> str:
    tz = _tzinfo_from_name(CRON_TZ)
    dt = datetime.fromtimestamp(now_ts or time.time(), tz=tz)
    return dt.strftime("%Y-%m-%d")


def _load_budget() -> Dict[str, Any]:
    if not BUDGET_PATH.exists():
        return {"date": _today_key(), "count": 0}
    try:
        with open(BUDGET_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {"date": _today_key(), "count": 0}
        return data
    except Exception:
        return {"date": _today_key(), "count": 0}


def _save_budget(data: Dict[str, Any]) -> None:
    try:
        BUDGET_PATH.parent.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    with open(BUDGET_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f)


def _reset_if_new_day(state: Dict[str, Any]) -> Dict[str, Any]:
    today = _today_key()
    if state.get("date") != today:
        return {"date": today, "count": 0}
    return state


def _can_spend(n: int) -> bool:
    if DAILY_BUDGET <= 0:
        return True
    state = _reset_if_new_day(_load_budget())
    return (state.get("count", 0) + n) <= DAILY_BUDGET


def _spend(n: int) -> None:
    if DAILY_BUDGET <= 0:
        return
    state = _reset_if_new_day(_load_budget())
    state["count"] = int(state.get("count", 0)) + n
    _save_budget(state)


def get_budget_status() -> Dict[str, Any]:
    state = _reset_if_new_day(_load_budget())
    return {
        "date": state["date"],
        "used": state["count"],
        "limit": DAILY_BUDGET,
        "remaining": None if DAILY_BUDGET <= 0 else max(0, DAILY_BUDGET - state["count"]),
        "store": str(BUDGET_PATH),
        "tz": CRON_TZ,
    }


# ---------- Core HTTP ----------
def _require_key():
    if not ODDS_API_KEY:
        raise OddsAPIError("Set ODDS_API_KEY in your environment or .env file.")


def _url(path: str) -> str:
    host = ODDS_API_HOST.rstrip("/")
    path = path if path.startswith("/") else f"/{path}"
    return f"{host}{path}"


def _get(path: str, params: Dict[str, Any]) -> requests.Response:
    _require_key()
    if not _can_spend(1):
        status = get_budget_status()
        raise OddsAPIError(
            f"Daily request budget exceeded: used={status['used']} limit={status['limit']} date={status['date']} tz={status['tz']}"
        )
    r = requests.get(_url(path), params=params, timeout=30)
    _spend(1)  # count it regardless of status
    if r.status_code != 200:
        raise OddsAPIError(f"{path} failed: {r.status_code} {r.text}")
    return r


# ---------- Public functions ----------
def list_sports(all: bool = False) -> List[Dict[str, Any]]:
    params = {"apiKey": ODDS_API_KEY}
    if all:
        params["all"] = "true"
    r = _get("/v4/sports", params)
    return r.json()  # type: ignore


def get_odds(
    sport_key: str,
    regions: Optional[str] = None,
    markets: Optional[str] = None,
    odds_format: Optional[str] = None,
    date_format: str = "iso",
    bookmakers: Optional[str] = None,
    event_ids: Optional[str] = None,
) -> List[Dict[str, Any]]:
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": regions or DEFAULT_REGIONS,
        "markets": markets or DEFAULT_MARKETS,
        "oddsFormat": odds_format or DEFAULT_ODDS_FORMAT,
        "dateFormat": date_format,
    }
    if bookmakers:
        params["bookmakers"] = bookmakers
    if event_ids:
        params["eventIds"] = event_ids

    r = _get(f"/v4/sports/{sport_key}/odds", params)
    data = r.json()
    if not isinstance(data, list):
        raise OddsAPIError("Unexpected response for /odds")
    return data  # type: ignore


def get_event_odds(
    sport_key: str,
    event_id: str,
    regions: Optional[str] = None,
    markets: Optional[str] = None,
    odds_format: Optional[str] = None,
    date_format: str = "iso",
    bookmakers: Optional[str] = None,
) -> Dict[str, Any]:
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": regions or DEFAULT_REGIONS,
        "markets": markets or DEFAULT_MARKETS,
        "oddsFormat": odds_format or DEFAULT_ODDS_FORMAT,
        "dateFormat": date_format,
    }
    if bookmakers:
        params["bookmakers"] = bookmakers

    r = _get(f"/v4/sports/{sport_key}/events/{event_id}/odds", params)
    data = r.json()
    if not isinstance(data, dict):
        raise OddsAPIError("Unexpected response for /events/{id}/odds")
    return data  # type: ignore
