
import os, argparse, yaml
import numpy as np
import pandas as pd
from pathlib import Path
from sklearn.isotonic import IsotonicRegression
from joblib import dump

def load_config(path):
    with open(path, 'r') as f:
        import yaml
        return yaml.safe_load(f)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--config', required=True)
    args = ap.parse_args()

    cfg = load_config(args.config)
    artifacts = Path(cfg['paths']['artifacts'])
    oof = np.load(artifacts/'oof_probs.npy')
    df = pd.read_csv(Path(cfg['paths']['processed'])/'features.csv')
    y = df[cfg['model']['target']].values

    ir = IsotonicRegression(out_of_bounds='clip')
    cal = ir.fit(oof, y)
    dump(cal, artifacts/'calibration.joblib')
    print(f"Saved isotonic calibration to {artifacts}")

if __name__ == "__main__":
    main()
