
import os, argparse
import pandas as pd
from pathlib import Path
from .odds_api_client import get_odds
from .normalize import flatten_odds_event

SPORT_KEYS_DEFAULT = [
    "americanfootball_nfl",
    "americanfootball_ncaaf",
    "icehockey_nhl",
    "baseball_mlb",
    "basketball_nba",
]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sports", nargs="*", default=SPORT_KEYS_DEFAULT)
    ap.add_argument("--regions", default=os.getenv("ODDS_API_REGIONS", "us,us2"))
    ap.add_argument("--markets", default=os.getenv("ODDS_API_MARKETS", "h2h,spreads,totals"))
    ap.add_argument("--odds_format", default=os.getenv("ODDS_API_FORMAT", "american"))
    ap.add_argument("--out", default=os.getenv("DATA_DIR", "./data") + "/raw/odds_latest.csv")
    args = ap.parse_args()

    all_rows = []
    for sk in args.sports:
        events = get_odds(sk, regions=args.regions, markets=args.markets, odds_format=args.odds_format)
        for ev in events:
            all_rows.extend(flatten_odds_event(ev))

    df = pd.DataFrame(all_rows)
    Path(os.path.dirname(args.out)).mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False)
    print(f"Wrote {args.out} with {len(df)} rows.")

if __name__ == "__main__":
    main()
