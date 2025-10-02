
import os
import pandas as pd
from pathlib import Path

def american_to_decimal(A):
    return (100/abs(A))+1 if A<0 else (A/100)+1

def novig_two_way(prices):
    q = [1.0/p for p in prices]
    hold = sum(q)
    return [qi/hold for qi in q]

def main():
    data_dir = Path(os.getenv("DATA_DIR", "./data"))
    raw = data_dir / "raw"
    processed = data_dir / "processed"
    processed.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(raw / "odds_latest.csv")
    h2h = df[df["market_key"]=="h2h"].copy()
    def label_side(row):
        if row["outcome_name"] == row["home_team"]:
            return "home"
        elif row["outcome_name"] == row["away_team"]:
            return "away"
        else:
            return "other"
    h2h["side"] = h2h.apply(label_side, axis=1)
    h2h = h2h[h2h["side"].isin(["home","away"])]
    h2h["price_decimal"] = h2h["price"].apply(american_to_decimal)

    groups = []
    for (event, book), sub in h2h.groupby(["event_id","book_key"]):
        sub = sub.set_index("side")
        if not {"home","away"}.issubset(sub.index): continue
        decs = [sub.loc["home","price_decimal"], sub.loc["away","price_decimal"]]
        q_home, q_away = novig_two_way(decs)
        groups.append({
            "event_id": event,
            "book_key": book,
            "home_price_decimal": decs[0],
            "away_price_decimal": decs[1],
            "home_q_novig": q_home,
            "away_q_novig": q_away,
            "home_team": sub.loc["home","home_team"],
            "away_team": sub.loc["away","away_team"],
            "sport_key": sub.loc["home","sport_key"],
        })
    out = pd.DataFrame(groups)
    out_path = processed / "market_baselines_h2h.csv"
    out.to_csv(out_path, index=False)
    print(f"Wrote {out_path} with {len(out)} rows")

if __name__ == "__main__":
    main()
