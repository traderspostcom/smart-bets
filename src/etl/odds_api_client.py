import os
import requests
from typing import List, Dict

# ===== Config & credit budget =====
ODDS_API_HOST = os.getenv("ODDS_API_HOST", "https://api.the-odds-api.com")
ODDS_API_KEY = os.getenv("ODDS_API_KEY")

# Hard caps so a run can't burn credits unexpectedly
MAX_REQUESTS_PER_RUN = int(os.getenv("ODDS_MAX_REQUESTS_PER_RUN", "40"))  # total requests allowed per process
MIN_REMAINING_REQUIRED = int(os.getenv("ODDS_MIN_REMAINING", "5"))        # abort if provider shows fewer remaining

_requests_used = 0  # module-level counter

class OddsAPIError(Exception):
    pass

def _check_key():
    if not ODDS_API_KEY:
        raise OddsAPIError("Set ODDS_API_KEY in your environment or .env file.")

def _enforce_budget():
    global _requests_used
    if _requests_used >= MAX_REQUESTS_PER_RUN:
        raise OddsAPIError(
            f"Budget exceeded: used {_requests_used} >= MAX_REQUESTS_PER_RUN={MAX_REQUESTS_PER_RUN}"
        )

def _record(resp: requests.Response):
    """Increment local counter and stop if provider credits are very low."""
    global _requests_used
    _requests_used += 1
    try:
        remaining = int(resp.headers.get("x-requests-remaining", "999999"))
    except ValueError:
        remaining = 999999
    if remaining < MIN_REMAINING_REQUIRED:
        raise OddsAPIError(
            f"Low remaining credits: {remaining} < MIN_REMAINING_REQUIRED={MIN_REMAINING_REQUIRED}"
        )

def _get(url: str, params: Dict) -> requests.Response:
    _enforce_budget()
    r = requests.get(url, params=params, timeout=30)
    _record(r)
    return r

# ===== Public client functions =====

def get_sports(all_sports: bool = False) -> List[Dict]:
    _check_key()
    params = {"apiKey": ODDS_API_KEY}
    if all_sports:
        params["all"] = "true"
    url = f"{ODDS_API_HOST}/v4/sports/"
    r = _get(url, params)
    if r.status_code != 200:
        raise OddsAPIError(f"/sports failed: {r.status_code} {r.text}")
    return r.json()

def get_odds(
    sport_key: str,
    regions: str = "us",
    markets: str = "h2h,spreads,totals",
    odds_format: str = "american",
):
    """
    Full-game markets only. Period markets like h2h_h1 are NOT supported here.
    """
    _check_key()
    url = f"{ODDS_API_HOST}/v4/sports/{sport_key}/odds/"
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": regions,
        "markets": markets,
        "oddsFormat": odds_format,
    }
    r = _get(url, params)
    if r.status_code != 200:
        raise OddsAPIError(f"/odds failed: {r.status_code} {r.text}")
    return r.json()

def get_event_odds(
    sport_key: str,
    event_id: str,
    regions: str,
    markets: str,
    odds_format: str = "american",
):
    """
    Event-level odds for PERIOD markets (e.g., h2h_h1, h2h_1st_5_innings).
    """
    _check_key()
    url = f"{ODDS_API_HOST}/v4/sports/{sport_key}/events/{event_id}/odds/"
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": regions,
        "markets": markets,
        "oddsFormat": odds_format,
    }
    r = _get(url, params)
    if r.status_code != 200:
        raise OddsAPIError(f"/events/{event_id}/odds failed: {r.status_code} {r.text}")
    return r.json()
