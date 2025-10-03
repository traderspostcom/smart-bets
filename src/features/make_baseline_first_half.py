import os
import pandas as pd
from pathlib import Path

def american_to_decimal(A):
    return (100/abs(A))+1 if A < 0 else (A/100)+1

def novig_two_way(dec_prices):
    inv = [1.0/p for p in dec_prices]
    hold = sum(inv)
    return [v/hold for v in inv]

def main():
    data_dir = Path(os.getenv("DATA_DIR", "./data"))
    raw = data_dir / "raw"
    processed = data_dir / "processed"
    processed.mkdir(parents=True, exist_ok=True)

    # period odds pulled by pull_period_odds_to_csv.py
    src = raw / "odds_periods_latest.csv"
    if not src.exists():
        raise FileNotFoundError(f"Missing {src}. Run: python -m src.etl.pull_period_odds_to_csv")

    df = pd.read_csv(src)

    # Keep our target period markets: 1st half (h2h_h1) and MLB F5 (h2h_1st_5_innings)
    df = df[df["market_key"].isin(["h2h_h1", "h2h_1st_5_innings"])].copy()

    # Label side by team name
    def side(row):
        if row["outcome_name"] == row["home_team"]:
            return "home"
        if row["outcome_name"] == row["away_team"]:
            return "away"
        return "other"

    df["side"] = df.apply(side, axis=1)
    df = df[df["side"].isin(["home", "away"])]

    # Convert American to decimal
    df["price_decimal"] = df["price"].apply(american_to_decimal)

    rows = []
    for (event_id, book_key, market_key), sub in df.groupby(["event_id", "book_key", "market_key"]):
        sub = sub.set_index("side")
        if not {"home", "away"}.issubset(sub.index):
            continue
        home_dec = float(sub.loc["home", "price_decimal"])
        away_dec = float(sub.loc["away", "price_decimal"])
        q_home, q_away = novig_two_way([home_dec, away_dec])

        rows.append({
            "event_id": event_id,
            "sport_key": sub.loc["home", "sport_key"],
            "book_key": book_key,
            "market_key": market_key,
            "home_team": sub.loc["home", "home_team"],
            "away_team": sub.loc["away", "away_team"],
            "home_price_decimal": home_dec,
            "away_price_decimal": away_dec,
            "home_q_novig": q_home,
            "away_q_novig": q_away,
        })

    out = pd.DataFrame(rows)
    out_path = processed / "market_baselines_firsthalf.csv"
    out.to_csv(out_path, index=False)
    print(f"Wrote {out_path} with {len(out)} rows")

if __name__ == "__main__":
    main()
