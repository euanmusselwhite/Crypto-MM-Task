"""
Task
----
From the BTC-USD Level 2 order book, compute spreads for target sizes
(0.1, 1, 5, 10 BTC) and report rolling statistics (avg/median/min/max, in bps).

Notes
-----
- Pure Python (no pandas/numpy). Rolling window = 300s (5 minutes).
- Spread definition for a size S:
    worst_buy  = price reached when consuming S from the ask side (low→high)
    worst_sell = price reached when consuming S from the bid side (high→low)
    spread_abs = worst_sell - worst_buy
    spread_bps = (spread_abs / mid) * 10_000, where mid = (best_bid + best_ask)/2
"""

from __future__ import annotations

import json
import threading
import time
import ssl
from typing import Dict, List, Tuple, Optional

import certifi
from websocket import WebSocketApp

WS_URL = "wss://advanced-trade-ws.coinbase.com"
PRODUCT_ID = "BTC-USD"
TARGET_SIZES = [0.1, 1.0, 5.0, 10.0]
ROLLING_WINDOW_SEC = 300  # 5 minutes
PRINT_INTERVAL_SEC = 5.0  # print KPI table every 5s


# --------------------------- Order book model ---------------------------------
class OrderBook:
    """
    Maintains price->size maps for bids and asks.
    Coinbase L2 messages provide absolute 'new_quantity' at 'price_level';
    a zero quantity removes the level.
    """

    def __init__(self, product_id: str) -> None:
        self.product_id = product_id
        self.bids: Dict[float, float] = {}  # price -> size
        self.asks: Dict[float, float] = {}  # price -> size
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

    def depth_price_for_size(self, side: str, size: float) -> Optional[float]:
        """
        Return the worst price to fully fill 'size' from one side of the book.
        side='buy'  -> consume asks from low→high
        side='sell' -> consume bids from high→low
        """
        if side == "buy":
            levels = sorted(self.asks.items(), key=lambda kv: kv[0])
        else:
            levels = sorted(self.bids.items(), key=lambda kv: kv[0], reverse=True)

        need = size
        worst = None
        for px, qty in levels:
            if need <= 0:
                break
            take = min(qty, need)
            need -= take
            worst = px
        return worst if need <= 0 else None


# --------------------------- Rolling stats window -----------------------------
class RollingStats:
    """Store (ts, bps) per size; trim older than window; compute summary."""

    def __init__(self, window_sec: int) -> None:
        self.window = window_sec
        self.data: Dict[float, List[Tuple[float, float]]] = {s: [] for s in TARGET_SIZES}

    def add(self, size: float, bps: float) -> None:
        now = time.time()
        self.data[size].append((now, bps))
        self._trim(size, now)

    def _trim(self, size: float, now: float) -> None:
        cutoff = now - self.window
        self.data[size] = [row for row in self.data[size] if row[0] >= cutoff]

    @staticmethod
    def _median(vals: List[float]) -> Optional[float]:
        n = len(vals)
        if n == 0:
            return None
        vals_sorted = sorted(vals)
        mid = n // 2
        if n % 2:
            return vals_sorted[mid]
        return 0.5 * (vals_sorted[mid - 1] * 1.0 + vals_sorted[mid] * 1.0)

    def summary(self) -> List[Dict[str, Optional[float]]]:
        out: List[Dict[str, Optional[float]]] = []
        for s in TARGET_SIZES:
            bps_vals = [v for _, v in self.data[s]]
            if not bps_vals:
                out.append({"size_btc": s, "count": 0, "avg": None, "median": None, "min": None, "max": None})
                continue
            n = len(bps_vals)
            avg = sum(bps_vals) / n
            med = self._median(bps_vals)
            out.append({"size_btc": s, "count": n, "avg": avg, "median": med, "min": min(bps_vals), "max": max(bps_vals)})
        return out


# --------------------------- WebSocket client ---------------------------------
class WSClient:
    """Subscribe to 'level2'; dispatch messages; auto-reconnect."""

    def __init__(self, url, on_message, on_error=None) -> None:
        self.url = url
        self.on_message_cb = on_message
        self.on_error_cb = on_error
        self.ws: Optional[WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None
        self._stop = False

    def _on_open(self, ws: WebSocketApp) -> None:
        sub = {"type": "subscribe", "channel": "level2", "product_ids": [PRODUCT_ID]}
        ws.send(json.dumps(sub))

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


# ------------------------------- Runner ---------------------------------------
def _print_table(rows: List[Dict[str, Optional[float]]]) -> None:
    # Header
    print("\nSpreads (bps), rolling 5m")
    print(f"{'Size (BTC)':>10}  {'N':>5}  {'Avg':>8}  {'Med':>8}  {'Min':>8}  {'Max':>8}")
    # Rows
    for r in rows:
        size = f"{r['size_btc']:.1f}"
        n = f"{r['count']}"
        fmt = lambda x: "" if x is None else f"{x:0.2f}"
        print(f"{size:>10}  {n:>5}  {fmt(r['avg']):>8}  {fmt(r['median']):>8}  {fmt(r['min']):>8}  {fmt(r['max']):>8}")


def main() -> None:
    ob = OrderBook(PRODUCT_ID)
    stats = RollingStats(ROLLING_WINDOW_SEC)

    def on_msg(m: Dict[str, any]) -> None:
        # Maintain the book
        if m["channel"] in ("level2", "l2_data"):
            for ev in m["events"]:
                typ = ev.get("type")
                ups = ev.get("updates", []) or []
                if typ == "snapshot":
                    ob.handle_snapshot(ups)
                elif typ == "update":
                    ob.handle_update(ups)

            # After each update batch, attempt to compute spreads at current mid.
            mid = ob.mid()
            if not mid:
                return
            for s in TARGET_SIZES:
                worst_buy = ob.depth_price_for_size("buy", s)
                worst_sell = ob.depth_price_for_size("sell", s)
                if worst_buy is None or worst_sell is None:
                    # insufficient depth available to fill size 's'
                    continue
                spread_abs = worst_buy - worst_sell
                spread_bps = (spread_abs / mid) * 10_000
                stats.add(s, spread_bps)

    def on_err(e: Exception) -> None:
        print("WebSocket error:", e)

    ws = WSClient(WS_URL, on_msg, on_err)
    ws.start()

    try:
        print("Computing spread KPIs… Ctrl+C to stop.")
        last_print = 0.0
        while True:
            time.sleep(0.1)
            now = time.time()
            if now - last_print >= PRINT_INTERVAL_SEC:
                _print_table(stats.summary())
                last_print = now
    except KeyboardInterrupt:
        pass
    finally:
        ws.stop()


if __name__ == "__main__":
    main()