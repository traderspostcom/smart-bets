
from typing import Dict, List, Any

def flatten_odds_event(ev: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = []
    event_id = ev.get("id")
    sport_key = ev.get("sport_key")
    commence_time = ev.get("commence_time")
    home = ev.get("home_team")
    away = ev.get("away_team")
    for bm in ev.get("bookmakers", []):
        book_key = bm.get("key")
        book_title = bm.get("title")
        last_update = bm.get("last_update")
        for mkt in bm.get("markets", []):
            market_key = mkt.get("key")
            for outcome in mkt.get("outcomes", []):
                rows.append({
                    "event_id": event_id,
                    "sport_key": sport_key,
                    "commence_time": commence_time,
                    "home_team": home,
                    "away_team": away,
                    "book_key": book_key,
                    "book_title": book_title,
                    "last_update": last_update,
                    "market_key": market_key,
                    "outcome_name": outcome.get("name"),
                    "price": outcome.get("price"),
                    "point": outcome.get("point"),
                })
    return rows
