import os, argparse
import pandas as pd
from pathlib import Path
from .odds_api_client import get_odds, get_event_odds

# Period markets we want to pull per sport
PERIOD_MARKETS = {
    "basketball_nba": ["h2h_h1"],            # NBA 1st half moneyline
    "americanfootball_nfl": ["h2h_h1"],      # NFL 1st half moneyline
    "americanfootball_ncaaf": ["h2h_h1"],    # NCAAF 1st half moneyline
    "baseball_mlb": ["h2h_1st_5_innings"],   # MLB First 5 innings moneyline
    # If you want NHL 1st period: uncomment next line
    # "icehockey_nhl": ["h2h_p1"],
}

def flatten(ev, sport_key, bm, mkt, outcome, period_key):
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
    args = ap.parse_args()

    rows = []
    for sk in args.sports:
        # 1) Get upcoming events (any market, we use h2h to list event IDs cheaply)
        events = get_odds(sk, regions=args.regions, markets="h2h", odds_format=args.odds_format)

        # 2) For each event, request the specific period market(s)
        for ev in events:
            for mkt_key in PERIOD_MARKETS.get(sk, []):
                data = get_event_odds(sk, ev["id"], regions=args.regions, markets=mkt_key, odds_format=args.odds_format)
                # Same structure: event + bookmakers -> markets -> outcomes
                for bm in data.get("bookmakers", []):
                    for m in bm.get("markets", []):
                        for outc in m.get("outcomes", []):
                            rows.append(flatten(data, sk, bm, m, outc, mkt_key))

    df = pd.DataFrame(rows)
    Path(os.path.dirname(args.out)).mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False)
    print(f"Wrote {args.out} with {len(df)} rows.")

if __name__ == "__main__":
    main()
