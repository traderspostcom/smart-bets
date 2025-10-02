
import os
import requests
from typing import List, Dict

ODDS_API_HOST = os.getenv("ODDS_API_HOST", "https://api.the-odds-api.com")
ODDS_API_KEY = os.getenv("ODDS_API_KEY")

class OddsAPIError(Exception):
    pass

def _check_key():
    if not ODDS_API_KEY:
        raise OddsAPIError("Set ODDS_API_KEY in your environment or .env file.")

def get_sports(all_sports: bool=False) -> List[Dict]:
    _check_key()
    params = {"apiKey": ODDS_API_KEY}
    if all_sports:
        params["all"] = "true"
    url = f"{ODDS_API_HOST}/v4/sports/"
    r = requests.get(url, params=params, timeout=30)
    if r.status_code != 200:
        raise OddsAPIError(f"/sports failed: {r.status_code} {r.text}")
    return r.json()

def get_odds(sport_key: str, regions: str="us", markets: str="h2h,spreads,totals",
             odds_format: str="american"):
    _check_key()
    url = f"{ODDS_API_HOST}/v4/sports/{sport_key}/odds/"
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": regions,
        "markets": markets,
        "oddsFormat": odds_format
    }
    r = requests.get(url, params=params, timeout=30)
    if r.status_code != 200:
        raise OddsAPIError(f"/odds failed: {r.status_code} {r.text}")
    r.quota = {
        "x-requests-remaining": r.headers.get("x-requests-remaining"),
        "x-requests-used": r.headers.get("x-requests-used"),
        "x-requests-last": r.headers.get("x-requests-last"),
    }
    return r.json()

def get_historical_odds(sport_key: str, date_iso: str, regions: str="us",
                        markets: str="h2h,spreads,totals", odds_format: str="american"):
    _check_key()
    url = f"{ODDS_API_HOST}/v4/historical/sports/{sport_key}/odds/"
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": regions,
        "markets": markets,
        "oddsFormat": odds_format,
        "date": date_iso
    }
    r = requests.get(url, params=params, timeout=60)
    if r.status_code != 200:
        raise OddsAPIError(f"/historical/odds failed: {r.status_code} {r.text}")
    r.quota = {
        "x-requests-remaining": r.headers.get("x-requests-remaining"),
        "x-requests-used": r.headers.get("x-requests-used"),
        "x-requests-last": r.headers.get("x-requests-last"),
    }
    return r.json()
