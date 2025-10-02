
import os, argparse, yaml
import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.metrics import log_loss, brier_score_loss
from sklearn.model_selection import TimeSeriesSplit
from xgboost import XGBClassifier
from joblib import dump

def load_config(path):
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--config', required=True)
    args = ap.parse_args()

    cfg = load_config(args.config)
    data_dir = Path(cfg['paths']['processed'])
    artifacts = Path(cfg['paths']['artifacts'])
    artifacts.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(data_dir/'features.csv')
    feature_cols = cfg['model']['features']
    X = df[feature_cols].values
    y = df[cfg['model']['target']].values

    tscv = TimeSeriesSplit(n_splits=5)
    oof = np.zeros(len(df))
    for fold, (tr, va) in enumerate(tscv.split(X)):
        model = XGBClassifier(
            n_estimators=cfg['model']['params']['n_estimators'],
            max_depth=cfg['model']['params']['max_depth'],
            learning_rate=cfg['model']['params']['learning_rate'],
            subsample=cfg['model']['params']['subsample'],
            colsample_bytree=cfg['model']['params']['colsample_bytree'],
            objective='binary:logistic',
            eval_metric='logloss',
            n_jobs=4
        )
        model.fit(X[tr], y[tr])
        oof[va] = model.predict_proba(X[va])[:,1]

    ll = log_loss(y, oof)
    br = brier_score_loss(y, oof)
    print(f"OOF logloss={ll:.4f}  brier={br:.4f}")

    final_model = XGBClassifier(
        n_estimators=cfg['model']['params']['n_estimators'],
        max_depth=cfg['model']['params']['max_depth'],
        learning_rate=cfg['model']['params']['learning_rate'],
        subsample=cfg['model']['params']['subsample'],
        colsample_bytree=cfg['model']['params']['colsample_bytree'],
        objective='binary:logistic',
        eval_metric='logloss',
        n_jobs=4
    )
    final_model.fit(X, y)

    dump(final_model, artifacts/'model.joblib')
    np.save(artifacts/'oof_probs.npy', oof)
    print(f"Saved model + oof to {artifacts}")

if __name__ == "__main__":
    main()
