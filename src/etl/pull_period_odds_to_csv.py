import os
import argparse
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd

from .odds_api_client import get_odds, get_event_odds

# Period markets we want to pull per sport
PERIOD_MARKETS = {
    "basketball_nba": ["h2h_h1"],            # NBA 1st half moneyline
    "americanfootball_nfl": ["h2h_h1"],      # NFL 1st half moneyline
    "americanfootball_ncaaf": ["h2h_h1"],    # NCAAF 1st half moneyline
    "baseball_mlb": ["h2h_1st_5_innings"],   # MLB First 5 innings moneyline
    # Optional: NHL 1st period
    # "icehockey_nhl": ["h2h_p1"],
}

def flatten(ev: Dict[str, Any], sport_key: str, bm: Dict[str, Any], mkt: Dict[str, Any], outcome: Dict[str, Any], period_key: str):
    return {
        "event_id": ev.get("id"),
        "sport_key": sport_key,
        "commence_time": ev.get("commence_time"),
        "home_team": ev.get("home_team"),
        "away_team": ev.get("away_team"),
        "book_key": bm.get("key"),
        "book_title": bm.get("title"),
        "last_update": bm.get("last_update"),
        "market_key": period_key,
        "outcome_name": outcome.get("name"),
        "price": outcome.get("price"),
        "point": outcome.get("point"),
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sports", nargs="*", default=list(PERIOD_MARKETS.keys()))
    ap.add_argument("--regions", default=os.getenv("ODDS_API_REGIONS", "us,eu"))
    ap.add_argument("--odds_format", default=os.getenv("ODDS_API_FORMAT", "american"))
    ap.add_argument("--out", default=os.getenv("DATA_DIR", "./data") + "/raw/odds_periods_latest.csv")
    ap.add_argument("--max_events", type=int, default=30, help="Max events per sport to process (prevents timeouts).")
    args = ap.parse_args()

    rows: List[Dict[str, Any]] = []
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    for sk in args.sports:
        # 1) Get upcoming events (use simple h2h to enumerate event IDs cheaply)
        try:
            events = get_odds(sk, regions=args.regions, markets="h2h", odds_format=args.odds_format)
        except Exception as e:
            print(f"[WARN] get_odds failed for {sk}: {e}")
            continue

        # Cap number of events to avoid long server calls
        events = (events or [])[: max(0, args.max_events)]

        # 2) For each event, request the specific period markets
        for ev in events:
            ev_id = ev.get("id")
            if not ev_id:
                continue
            for mkt_key in PERIOD_MARKETS.get(sk, []):
                try:
                    data = get_event_odds(sk, ev_id, regions=args.regions, markets=mkt_key, odds_format=args.odds_format)
                except Exception as e:
                    print(f"[WARN] get_event_odds failed for {sk} {ev_id} {mkt_key}: {e}")
                    continue

                # Response: event dict with bookmakers -> markets -> outcomes
                for bm in data.get("bookmakers", []) or []:
                    for m in bm.get("markets", []) or []:
                        for outc in m.get("outcomes", []) or []:
                            rows.append(flatten(data, sk, bm, m, outc, mkt_key))

    df = pd.DataFrame(rows)
    df.to_csv(out_path, index=False)
    print(f"Wrote {out_path} with {len(df)} rows.")

if __name__ == "__main__":
    main()
