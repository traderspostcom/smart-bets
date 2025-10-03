import os
import requests
from typing import List, Dict, Optional

# === Config & Budget ===
ODDS_API_HOST = os.getenv("ODDS_API_HOST", "https://api.the-odds-api.com")
ODDS_API_KEY = os.getenv("ODDS_API_KEY")

# Hard caps to protect your credits; tune via env on Render/local
MAX_REQUESTS_PER_RUN = int(os.getenv("ODDS_MAX_REQUESTS_PER_RUN", "40"))   # total requests allowed in a single run
MIN_REMAINING_REQUIRED = int(os.getenv("ODDS_MIN_REMAINING", "5"))         # abort if provider says remaining < this

# Module-level counters
_REQUESTS_USED_THIS_RUN = 0

class OddsAPIError(Exception):
    pass

def _check_key():
    if not ODDS_API_KEY:
        raise OddsAPIError("Set ODDS_API_KEY in your environment or .env file.")

def _enforce_budget():
    global _REQUESTS_USED_THIS_RUN
    if _REQUESTS_USED_THIS_RUN >= MAX_REQUESTS_PER_RUN:
        raise OddsAPIError(
            f"Budget exceeded: used {_REQUESTS_USED_THIS_RUN} >= MAX_REQUESTS_PER_RUN={MAX_REQUESTS_PER_RUN}"
        )

def _record_and_guard(resp: requests.Response):
    """Record usage from headers and guard against low remaining credits."""
    global _REQUESTS_USED_THIS_RUN
    # Provider returns these headers; be tolerant if missing.
    try:
        used = int(resp.headers.get("x-requests-used", "0"))
        remaining = int(resp.headers.get("x-requests-remaining", "999999"))
    except ValueError:
        used, remaining = 0, 999999

    # We don't always get cumulative 'used', so increment our own budget counter regardless.
    _REQUESTS_USED_THIS_RUN += 1

    # Abort early if remaining is below threshold
    if remaining < MIN_REMAINING_REQUIRED:
        raise OddsAPIError(
            f"Provider shows low remaining credits ({remaining}) < MIN_REMAINING={MIN_REMAINING_REQUIRED}"
        )

def _get(url: str, params: Dict) -> requests.Response:
    _enforce_budget()
    r = requests.get(url, params=params, timeout=30)
    _record_and_guard(r)
    return r

# ========== Public Client Functions ==========

def get_sports(all_sports: bool = False) -> List[Dict]:
    _check_key()
    params = {"apiKey": ODDS_API_KEY}
    if all_sports:
        params["all"] = "true"
    url = f"{ODDS_AP_
