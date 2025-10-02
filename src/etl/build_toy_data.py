
import os, json, random, math
import pandas as pd
from pathlib import Path

def build_games(n=2000, seed=7):
    rng = random.Random(seed)
    rows = []
    for i in range(n):
        home = rng.randint(0, 99)
        away = (home + rng.randint(1, 99)) % 100
        home_rating = rng.gauss(0, 1)
        away_rating = rng.gauss(0, 1)
        rest_diff = rng.randint(-3, 3)
        travel_miles = max(0, int(rng.gauss(500, 300)))
        weather_wind = max(0, rng.randint(0, 25))
        logit = 0.35 + 0.9*(home_rating - away_rating) + 0.05*rest_diff - 0.0003*travel_miles - 0.01*weather_wind
        p_true = 1/(1+math.exp(-logit))
        result = 1 if rng.random() < p_true else 0

        p_book_vigged = min(0.98, max(0.02, p_true + rng.gauss(0, 0.05)))
        def prob_to_decimal(p): return 1.0 / p
        d_home = prob_to_decimal(p_book_vigged)
        price_home_decimal = d_home
        if p_book_vigged >= 0.5:
            price_home_american = int(round(-100 * (p_book_vigged / (1 - p_book_vigged))))
        else:
            price_home_american = int(round(100 * ((1 - p_book_vigged) / p_book_vigged)))

        rows.append(dict(
            game_id=i,
            home_team=f"T{home}", away_team=f"T{away}",
            home_rating=home_rating, away_rating=away_rating,
            rest_diff=rest_diff, travel_miles=travel_miles, weather_wind=weather_wind,
            home_win=result, p_true=p_true,
            book="ToyBook", price_home_american=price_home_american, price_home_decimal=price_home_decimal
        ))
    return pd.DataFrame(rows)

def main():
    data_dir = Path(os.getenv("DATA_DIR", "./data"))
    raw = data_dir / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    df = build_games()
    df.to_csv(raw / "toy_games.csv", index=False)
    print(f"Wrote {raw/'toy_games.csv'} with {len(df)} rows")

if __name__ == "__main__":
    main()
