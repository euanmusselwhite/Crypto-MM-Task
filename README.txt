# Crypto Market Making — Technical Exercise

## Objective
Implement a small Python project that ingests real-time market data, maintains a Level 2 order book, measures liquidity (spreads for specific sizes), and runs a basic market-making simulation with simple risk controls.

---

## Scope → Files

- **Connectivity check**
  - `01_heartbeat.py` — Connects to Coinbase Advanced Trade WebSocket and prints heartbeats.

- **Market data subscriptions (BTC-USD)**
  - `02_trade_feed.py` — Executed trades (time & sales).
  - `03_order_book.py` — Level 2 order book: snapshot + incremental updates; prints best bid/ask and top N levels.

- **Liquidity analytics**
  - `04_spreads_kpis.py` — Computes spreads (in bps) for sizes **0.1 / 1 / 5 / 10 BTC** by walking book depth; prints rolling **avg / median / min / max** over 5 minutes.

- **Strategy: basic market making**
  - `05_market_maker.py` — Quotes around mid with a defined half-spread and inventory-based skew, clamped to top-of-book to encourage fills in simulation. Uses public trade prints to simulate passive fills. Tracks position, average entry, exposure, realized/unrealized/total P&L. Enforces risk limits:
    - Max notional exposure: **$1,000,000**
    - Max loss: **$100,000**

- **Bonus / polish**
  - `06_mm_with_logging.py` — Same as above, plus CSV logging:
    - `data/fills_log.csv` — one row per fill
    - `data/pnl_log.csv` — periodic status (mid, bb/ba, position, exposure, P&L)

---

## Project Structure
crypto_mm_project/
├─ README.md
├─ requirements.txt
├─ .gitignore
├─ 01_heartbeat.py
├─ 02_trade_feed.py
├─ 03_order_book.py
├─ 04_spreads_kpis.py
├─ 05_market_maker.py
├─ 06_mm_with_logging.py
└─ data/
└─ .gitkeep

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt