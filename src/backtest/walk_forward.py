
import os, argparse, yaml
import pandas as pd
import numpy as np
from pathlib import Path
from joblib import load
from sklearn.metrics import brier_score_loss

def settle(result, price_decimal, stake):
    if result == 1:
        return stake * (price_decimal - 1.0)
    elif result == 0:
        return -stake
    else:
        return 0.0

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

    df = df.sort_values('game_id').reset_index(drop=True)
    split = int(len(df)*0.8)
    train_df, test_df = df.iloc[:split], df.iloc[split:]

    model = load(artifacts/'model.joblib')
    from joblib import load as jload
    if (artifacts/'calibration.joblib').exists():
        cal = jload(artifacts/'calibration.joblib')
        def calibrate(p): return float(cal.predict([p])[0])
    else:
        def calibrate(p): return p

    feature_cols = cfg['model']['features']
    p_test = model.predict_proba(test_df[feature_cols].values)[:,1]
    p_test = np.array([calibrate(p) for p in p_test])

    q_vig = 1.0/test_df['price_home_decimal'].values
    hold = q_vig + (1 - q_vig)
    q_novig = q_vig / hold

    edges = p_test - q_novig
    min_edge = cfg['betting']['min_edge']
    kfrac = cfg['betting']['kelly_fraction']

    bankroll = 100000.0
    pnl_hist = []
    for i, row in test_df.iterrows():
        p = p_test[i - split]
        edge = edges[i - split]
        if edge < min_edge:
            continue
        price_dec = row['price_home_decimal']
        b = price_dec - 1.0
        stake = bankroll * kfrac * kelly_fraction(p, b)
        pnl = settle(row['home_win'], price_dec, stake)
        bankroll += pnl
        pnl_hist.append(pnl)

    roi = (bankroll - 100000.0) / 100000.0
    br = brier_score_loss(test_df['home_win'], p_test)
    print(f"Backtest: ROI={roi:.3%}  Brier={br:.4f}  bets={len(pnl_hist)}  final_bankroll={bankroll:,.2f}")

if __name__ == "__main__":
    main()
