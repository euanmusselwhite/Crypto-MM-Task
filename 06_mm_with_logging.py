"""
Task
---------------------
Add CSV logging to the basic BTC-USD market-making simulation:
- Log every fill to data/fills_log.csv
- Log periodic status (mid, bb/ba, position, exposure, P&L) to data/pnl_log.csv

Notes
-----
- Keeps: quoting around mid (with inventory skew), clamped to top-of-book, passive fill simulation,
  risk limits (max notional = $1,000,000; max loss = $100,000).
"""
from __future__ import annotations

import csv
import json
import os
import ssl
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import certifi
from websocket import WebSocketApp

# ----------------------------- Config -----------------------------------------
WS_URL = "wss://advanced-trade-ws.coinbase.com"
PRODUCT_ID = "BTC-USD"

# Quoting params
QUOTE_SIZE_BTC = 0.10
BASE_HALF_SPREAD_BPS = 0.1
SKEW_BPS_AT_MAX = 8.0
SKEW_INVENTORY_SCALE_BTC = 10.0

# Risk limits
MAX_NOTIONAL_USD = 1_000_000.0
MAX_LOSS_USD = 100_000.0

# Logging / UI
PRINT_INTERVAL_SEC = 2.0
DATA_DIR = "data"
FILLS_CSV = os.path.join(DATA_DIR, "fills_log.csv")
PNL_CSV = os.path.join(DATA_DIR, "pnl_log.csv")


# --------------------------- Order book model ---------------------------------
class OrderBook:
    """Maintains price->size for bids and asks from Level 2 snapshot/updates."""

    def __init__(self, product_id: str) -> None:
        self.product_id = product_id
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.last_update_ts: float = 0.0

    def handle_snapshot(self, updates: List[dict]) -> None:
        self.bids.clear()
        self.asks.clear()
        for u in updates:
            side = u.get("side")
            px = float(u.get("price_level"))
            qty = float(u.get("new_quantity"))
            if qty <= 0:
                continue
            (self.bids if side == "bid" else self.asks)[px] = qty
        self.last_update_ts = time.time()

    def handle_update(self, updates: List[dict]) -> None:
        for u in updates:
            side = u.get("side")
            px = float(u.get("price_level"))
            qty = float(u.get("new_quantity"))
            book = self.bids if side == "bid" else self.asks
            if qty == 0.0:
                book.pop(px, None)
            else:
                book[px] = qty
        self.last_update_ts = time.time()

    def best_bid(self) -> Optional[float]:
        return max(self.bids) if self.bids else None

    def best_ask(self) -> Optional[float]:
        return min(self.asks) if self.asks else None

    def mid(self) -> Optional[float]:
        bb, ba = self.best_bid(), self.best_ask()
        if bb is None or ba is None:
            return None
        return 0.5 * (bb + ba)


# ------------------------------ Trades buffer ---------------------------------
@dataclass
class Trade:
    ts: float
    side: str      # maker side ("BUY"/"SELL")
    price: float
    size: float

class TradeBuffer:
    """
    Collects trades and lets the caller drain only the NEW trades each loop.
    Avoids double-counting the same prints.
    """

    def __init__(self, maxlen: int = 5000) -> None:
        self.trades: List[Trade] = []
        self.maxlen = maxlen
        self._cursor = 0  # index of the next unread trade

    def add_from_ws(self, events: list) -> None:
        now = time.time()
        for ev in events:
            for t in ev.get("trades", []) or []:
                self.trades.append(
                    Trade(
                        ts=now,
                        side=t.get("side"),
                        price=float(t.get("price")),
                        size=float(t.get("size")),
                    )
                )
        # Trim and fix cursor
        if len(self.trades) > self.maxlen:
            drop = len(self.trades) - self.maxlen
            self.trades = self.trades[drop:]
            self._cursor = max(0, self._cursor - drop)

    def drain_new(self) -> List[Trade]:
        new = self.trades[self._cursor :]
        self._cursor = len(self.trades)
        return new


# --------------------------------- Risk ---------------------------------------
@dataclass
class RiskState:
    position_btc: float = 0.0
    avg_entry: float = 0.0       # VWAP of open position
    realized_pnl: float = 0.0

    def exposure_usd(self, mid: float) -> float:
        return (self.position_btc * mid) if mid else 0.0

    def unrealized_pnl(self, mid: float) -> float:
        if self.position_btc == 0 or not mid:
            return 0.0
        return (mid - self.avg_entry) * self.position_btc

    def total_pnl(self, mid: float) -> float:
        return self.realized_pnl + self.unrealized_pnl(mid)

    def apply_fill(self, qty_btc: float, price: float) -> None:
        """
        qty_btc > 0 => bought at price
        qty_btc < 0 => sold  at price
        Updates avg_entry and realized_pnl using simple moving-average logic.
        """
        pos0 = self.position_btc

        # Add in same direction (increase long or increase short)
        if (pos0 >= 0 and qty_btc >= 0) or (pos0 <= 0 and qty_btc <= 0):
            new_pos = pos0 + qty_btc
            if new_pos != 0:
                self.avg_entry = (self.avg_entry * abs(pos0) + price * abs(qty_btc)) / abs(new_pos)
            else:
                self.avg_entry = 0.0
            self.position_btc = new_pos
            return

        # Closing or flipping
        if abs(qty_btc) <= abs(pos0):
            closed = qty_btc
            pnl = (price - self.avg_entry) * closed
            self.realized_pnl += pnl
            self.position_btc = pos0 + qty_btc
            if self.position_btc == 0:
                self.avg_entry = 0.0
        else:
            # Flip: close all old, open remainder at price
            closed = -pos0
            pnl = (price - self.avg_entry) * closed
            self.realized_pnl += pnl
            self.position_btc = pos0 + qty_btc
            self.avg_entry = price


# -------------------------------- Quoting -------------------------------------
@dataclass
class Quotes:
    bid_px: float
    ask_px: float
    size_btc: float

@dataclass
class MMParams:
    base_half_spread_bps: float
    skew_bps_at_max: float
    quote_size_btc: float
    skew_scale_btc: float

class MarketMaker:
    """Computes quotes around mid with linear inventory skew."""

    def __init__(self, p: MMParams) -> None:
        self.p = p
        self.last_quotes: Optional[Quotes] = None

    def compute_quotes(self, mid: float, position_btc: float) -> Optional[Quotes]:
        if not mid or mid <= 0:
            return None
        half = self.p.base_half_spread_bps / 10_000.0
        pos_ratio = max(-1.0, min(1.0, position_btc / self.p.skew_scale_btc))
        skew = self.p.skew_bps_at_max / 10_000.0 * pos_ratio

        bid = mid * (1.0 - half - max(0.0, skew))
        ask = mid * (1.0 + half - min(0.0, skew))
        if bid >= ask:  # safety
            bid = mid * (1.0 - 0.5 * half)
            ask = mid * (1.0 + 0.5 * half)

        q = Quotes(bid_px=bid, ask_px=ask, size_btc=self.p.quote_size_btc)
        self.last_quotes = q
        return q

    def simulate_fills(self, prints: List[Trade], q: Quotes) -> List[Tuple[float, float]]:
        """
        Passive fill model per side:
          - Track bid fills and ask fills independently.
          - Return up to two fills: [(+qty_btc, vwap_bid), (-qty_btc, vwap_ask)].
        """
        buy_filled = 0.0; buy_cost = 0.0
        sell_filled = 0.0; sell_proceeds = 0.0

        for tr in prints:
            px, sz = tr.price, tr.size
            if px <= q.bid_px and buy_filled < q.size_btc:
                take = min(sz, q.size_btc - buy_filled)
                if take > 0:
                    buy_filled += take
                    buy_cost += take * q.bid_px
            if px >= q.ask_px and sell_filled < q.size_btc:
                take = min(sz, q.size_btc - sell_filled)
                if take > 0:
                    sell_filled += take
                    sell_proceeds += take * q.ask_px

        fills: List[Tuple[float, float]] = []
        if buy_filled > 0:
            fills.append((+buy_filled, buy_cost / buy_filled))
        if sell_filled > 0:
            fills.append((-sell_filled, sell_proceeds / sell_filled))
        return fills


# --------------------------- WebSocket client ---------------------------------
class WSClient:
    """Subscribe to level2 + market_trades; dispatch; auto-reconnect."""

    def __init__(self, url, on_message, on_error=None) -> None:
        self.url = url
        self.on_message_cb = on_message
        self.on_error_cb = on_error
        self.ws: Optional[WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None
        self._stop = False

    def _on_open(self, ws: WebSocketApp) -> None:
        for m in (
            {"type": "subscribe", "channel": "level2", "product_ids": [PRODUCT_ID]},
            {"type": "subscribe", "channel": "market_trades", "product_ids": [PRODUCT_ID]},
        ):
            ws.send(json.dumps(m))

    def _on_message(self, ws: WebSocketApp, message_text: str) -> None:
        try:
            msg = json.loads(message_text)
            ch = msg.get("channel")
            evs = msg.get("events", []) or []
            if ch:
                self.on_message_cb({"channel": ch, "events": evs})
        except Exception as e:
            if self.on_error_cb:
                self.on_error_cb(e)

    def _on_error(self, ws: WebSocketApp, error: Exception) -> None:
        if self.on_error_cb:
            self.on_error_cb(error)

    def _on_close(self, ws: WebSocketApp, code: int, reason: str) -> None:
        if not self._stop:
            delay = 1.0
            while not self._stop:
                time.sleep(delay)
                try:
                    self._connect()
                    return
                except Exception:
                    delay = min(delay * 2, 60.0)

    def _connect(self) -> None:
        self.ws = WebSocketApp(
            self.url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        self.ws.run_forever(
            ping_interval=20,
            ping_timeout=10,
            sslopt={"ca_certs": certifi.where(), "cert_reqs": ssl.CERT_REQUIRED},
        )

    def start(self) -> None:
        self._stop = False
        self._thread = threading.Thread(target=self._connect, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop = True
        try:
            if self.ws:
                self.ws.close()
        finally:
            if self._thread:
                self._thread.join(timeout=3)


# ------------------------------ CSV utilities ---------------------------------
def _ensure_csv(path: str, header: List[str]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path):
        with open(path, "w", newline="") as f:
            csv.writer(f).writerow(header)

def _iso(ts: Optional[float] = None) -> str:
    return datetime.fromtimestamp(ts or time.time(), tz=timezone.utc).isoformat()


# --------------------------------- Runner -------------------------------------
def main() -> None:
    ob = OrderBook(PRODUCT_ID)
    tb = TradeBuffer()
    risk = RiskState()
    mm = MarketMaker(
        MMParams(
            base_half_spread_bps=BASE_HALF_SPREAD_BPS,
            skew_bps_at_max=SKEW_BPS_AT_MAX,
            quote_size_btc=QUOTE_SIZE_BTC,
            skew_scale_btc=SKEW_INVENTORY_SCALE_BTC,
        )
    )

    # Prepare CSV files
    _ensure_csv(
        FILLS_CSV,
        ["ts_utc", "side", "qty_btc", "price", "bid_px", "ask_px", "mid",
         "pos_btc_after", "avg_entry", "realized_pnl", "unrealized_pnl", "total_pnl"],
    )
    _ensure_csv(
        PNL_CSV,
        ["ts_utc", "mid", "best_bid", "best_ask", "pos_btc", "avg_entry",
         "exposure_usd", "realized_pnl", "unrealized_pnl", "total_pnl"],
    )

    def on_msg(m: Dict[str, Any]) -> None:
        ch = m["channel"]
        if ch in ("level2", "l2_data"):
            for ev in m["events"]:
                typ = ev.get("type")
                ups = ev.get("updates", []) or []
                if typ == "snapshot":
                    ob.handle_snapshot(ups)
                elif typ == "update":
                    ob.handle_update(ups)
        elif ch == "market_trades":
            tb.add_from_ws(m["events"])

    def on_err(e: Exception) -> None:
        print("WebSocket error:", e)

    ws = WSClient(WS_URL, on_msg, on_err)
    ws.start()

    try:
        print("Market-making simulation with CSV logging… Ctrl+C to stop.")
        last_print = 0.0
        risk_blocked_last: Optional[bool] = None

        while True:
            time.sleep(0.1)

            mid = ob.mid()
            if not mid:
                continue
            bb, ba = ob.best_bid(), ob.best_ask()
            if bb is None or ba is None:
                continue

            # Risk gate
            exposure = abs(risk.exposure_usd(mid))
            total_pnl = risk.total_pnl(mid)
            allowed = (exposure <= MAX_NOTIONAL_USD) and (total_pnl >= -MAX_LOSS_USD)

            if allowed:
                q = mm.compute_quotes(mid, risk.position_btc)
                if q:
                    # Clamp to top-of-book to increase fill probability.
                    q = Quotes(bid_px=bb, ask_px=ba, size_btc=q.size_btc)

                    # Use only NEW trades since last loop (no double-counting)
                    new_prints = tb.drain_new()
                    fills = mm.simulate_fills(new_prints, q)
                    for qty, px in fills:
                        risk.apply_fill(qty, px)
                        side = "BUY" if qty > 0 else "SELL"
                        print(f"[FILL] side={side:<4} qty_btc={qty:+.6f} @ {px:.2f} "
                              f"(bid={q.bid_px:.2f}, ask={q.ask_px:.2f})")

                        # CSV: write fill row
                        with open(FILLS_CSV, "a", newline="") as f:
                            w = csv.writer(f)
                            w.writerow([
                                _iso(), side, f"{qty:.8f}", f"{px:.2f}",
                                f"{q.bid_px:.2f}", f"{q.ask_px:.2f}", f"{mid:.2f}",
                                f"{risk.position_btc:.8f}", f"{risk.avg_entry:.2f}",
                                f"{risk.realized_pnl:.2f}", f"{risk.unrealized_pnl(mid):.2f}",
                                f"{risk.total_pnl(mid):.2f}",
                            ])
            else:
                if risk_blocked_last is not True:
                    reason = []
                    if exposure > MAX_NOTIONAL_USD:
                        reason.append(f"exposure ${exposure:,.0f} > ${MAX_NOTIONAL_USD:,.0f}")
                    if total_pnl < -MAX_LOSS_USD:
                        reason.append(f"loss ${-total_pnl:,.0f} > ${MAX_LOSS_USD:,.0f}")
                    print("[RISK] quoting paused:", "; ".join(reason))
                risk_blocked_last = True

            # Periodic status line + CSV P&L row
            now = time.time()
            if now - last_print >= PRINT_INTERVAL_SEC:
                print(
                    f"mid={mid:.2f} | bb={bb:.2f} ba={ba:.2f} | "
                    f"pos={risk.position_btc:+.6f} BTC | avg={risk.avg_entry:.2f} | "
                    f"exp=${risk.exposure_usd(mid):,.0f} | "
                    f"real=${risk.realized_pnl:,.0f} unrl=${risk.unrealized_pnl(mid):,.0f} "
                    f"tot=${risk.total_pnl(mid):,.0f}"
                )
                with open(PNL_CSV, "a", newline="") as f:
                    w = csv.writer(f)
                    w.writerow([
                        _iso(), f"{mid:.2f}", f"{bb:.2f}", f"{ba:.2f}",
                        f"{risk.position_btc:.8f}", f"{risk.avg_entry:.2f}",
                        f"{risk.exposure_usd(mid):.2f}", f"{risk.realized_pnl:.2f}",
                        f"{risk.unrealized_pnl(mid):.2f}", f"{risk.total_pnl(mid):.2f}",
                    ])
                last_print = now
                risk_blocked_last = allowed

    except KeyboardInterrupt:
        pass
    finally:
        ws.stop()


if __name__ == "__main__":
    main()
