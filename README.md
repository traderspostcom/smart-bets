
# edge-betting-starter

A tiny, pragmatic starter repo for AI-driven sports betting focused on probability estimation, edge filtering (no-vig), and risk-managed execution.

## TL;DR quickstart

```bash
# 1) Create and activate a virtual env (Python 3.10+ recommended)
python -m venv .venv && source .venv/bin/activate

# 2) Install deps
pip install -r requirements.txt

# 3) Configure
cp .env.example .env
cp config/config.example.yaml config/config.yaml

# 4) Run a tiny E2E dry run on toy data
python -m src.etl.build_toy_data
python -m src.features.make_features
python -m src.model.train --config config/config.yaml
python -m src.model.calibrate --config config/config.yaml
python -m src.backtest.walk_forward --config config/config.yaml

# 5) Simulate picking bets from today's toy odds
python -m src.bet.engine --config config/config.yaml
```

> This starter ships with **toy data generators** so you can execute the pipeline end-to-end without external feeds. Replace with your real ETL as you integrate odds & stats providers.

## Project layout

- `src/etl/` — ETL & toy data builders
- `src/features/` — feature construction, no leakage
- `src/model/` — model train & calibration
- `src/bet/` — bet selection with no-vig comparison & fractional Kelly
- `src/backtest/` — walk-forward backtest simulator
- `config/` — configuration (paths, thresholds, model params)
- `data/` — raw/processed/artifacts (gitignored by default)

## Core ideas baked in

- Optimize **log loss/Brier** and **calibrate**.
- Compare to **no-vig market** baseline.
- Only bet when **edge > threshold**; size with **fractional Kelly**.
- **Walk-forward** only; no data leakage.
- Persist snapshots for auditability.

## Next steps

- Replace toy ETL with real feeds.
- Add CLV tracking vs actual closing prices.
- Expand to props & lower-liquidity markets first.

## Hooking up The Odds API

Set your credentials in `.env`:

```
ODDS_API_KEY=your_key_here
ODDS_API_HOST=https://api.the-odds-api.com
ODDS_API_REGIONS=us,us2
ODDS_API_MARKETS=h2h,spreads,totals
```

Pull live odds for NFL/NCAAF/NHL/MLB/NBA and normalize to CSV:

```
python -m src.etl.pull_odds_to_csv
```

Build a quick baseline moneyline dataset from the odds dump (no-vig conversion per event/book):

```
python -m src.features.make_baseline_from_odds
```
