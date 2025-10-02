
import os
import pandas as pd
from pathlib import Path

def main():
    data_dir = Path(os.getenv("DATA_DIR", "./data"))
    raw = data_dir / "raw"
    processed = data_dir / "processed"
    processed.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(raw / "toy_games.csv")
    df['market_implied_q_novig'] = df['price_home_decimal'].apply(lambda d: (1.0/d) / ((1.0/d) + (1 - (1.0/d))))
    feats = df[[
        'game_id','home_rating','away_rating','rest_diff','travel_miles','weather_wind',
        'market_implied_q_novig','home_win','price_home_american','price_home_decimal','book'
    ]].copy()
    feats['target'] = feats['home_win']
    feats.to_csv(processed / "features.csv", index=False)
    print(f"Wrote {processed/'features.csv'} with {len(feats)} rows")

if __name__ == "__main__":
    main()
