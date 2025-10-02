
import os, argparse, yaml
import pandas as pd
import numpy as np
from pathlib import Path
from joblib import load

def american_to_decimal(A):
    return (100/abs(A))+1 if A<0 else (A/100)+1

def decimal_to_prob(d):
    return 1.0/d

def kelly_fraction(p, b):
    f = (b*p - (1-p)) / b
    return max(0.0, min(1.0, f))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--config', required=True)
    args = ap.parse_args()
    with open(args.config, 'r') as f:
        cfg = yaml.safe_load(f)

    processed = Path(cfg['paths']['processed'])
    artifacts = Path(cfg['paths']['artifacts'])
    df = pd.read_csv(processed/'features.csv')

    model = load(artifacts/'model.joblib')
    if (artifacts/'calibration.joblib').exists():
        cal = load(artifacts/'calibration.joblib')
        def calibrate(p): return float(cal.predict([p])[0])
    else:
        def calibrate(p): return p

    feature_cols = cfg['model']['features']
    probs = model.predict_proba(df[feature_cols].values)[:,1]
    probs = np.array([calibrate(p) for p in probs])

    q_vig = 1.0/df['price_home_decimal'].values
    q_away_vig = 1.0 - q_vig
    hold = q_vig + q_away_vig
    q_novig = q_vig / hold

    edges = probs - q_novig
    df_out = df[['game_id','book','price_home_american','price_home_decimal']].copy()
    df_out['p_model'] = probs
    df_out['q_novig'] = q_novig
    df_out['edge'] = edges

    bankroll = 100000.0
    min_edge = cfg['betting']['min_edge']
    kfrac = cfg['betting']['kelly_fraction']
    max_risk_per_market = cfg['betting']['max_risk_per_market']
    daily_risk_cap = cfg['betting']['daily_risk_cap']

    picks = []
    risk_used = 0.0
    cap_daily = daily_risk_cap * bankroll
    cap_market = max_risk_per_market * bankroll

    for row in df_out.sort_values('edge', ascending=False).itertuples():
        if row.edge < min_edge or risk_used >= cap_daily:
            continue
        b = row.price_home_decimal - 1.0
        f = kfrac * kelly_fraction(row.p_model, b)
        stake = min(f * bankroll, cap_market, cap_daily - risk_used)
        if stake <= 0: continue
        risk_used += stake
        picks.append({
            "game_id": row.game_id,
            "book": row.book,
            "price_american": int(row.price_home_american),
            "p_model": float(row.p_model),
            "q_novig": float(row.q_novig),
            "edge": float(row.edge),
            "stake": round(float(stake),2),
            "kelly_fraction": round(float(f),4)
        })

    out_path = Path("data") / "model_artifacts" / "picks.json"
    Path("data/model_artifacts").mkdir(parents=True, exist_ok=True)
    import json
    with open(out_path, "w") as f:
        json.dump(picks, f, indent=2)
    print(f"Wrote {out_path} with {len(picks)} picks")
    if picks[:5]:
        print("Top 5 picks preview:")
        for p in picks[:5]: print(p)

if __name__ == "__main__":
    main()
