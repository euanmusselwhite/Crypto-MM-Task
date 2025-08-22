"""
Task
----
Subscribe to BTC-USD Level 2 order book and maintain:
- Snapshot + incremental updates
- Display best bid/ask and top N levels per side
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
TOP_N = 5  # number of levels to print per side


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

    # Snapshot: full state load
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

    # Incremental: absolute updates (set or remove levels)
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

    def top_n(self, n: int = TOP_N) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        bids = sorted(self.bids.items(), key=lambda kv: kv[0], reverse=True)[:n]
        asks = sorted(self.asks.items(), key=lambda kv: kv[0])[:n]
        return bids, asks


# --------------------------- WebSocket client ---------------------------------
class WSClient:
    """Light wrapper around WebSocketApp: subscribe to level2, dispatch, auto-reconnect."""

    def __init__(self, url, on_message, on_error=None) -> None:
        self.url = url
        self.on_message_cb = on_message
        self.on_error_cb = on_error
        self.ws: Optional[WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None
        self._stop = False

    def _on_open(self, ws: WebSocketApp) -> None:
        # Subscribe to Level 2 for the chosen product.
        # Some Coinbase docs use channel name "level2"; older examples show "l2_data".
        # Subscribe to "level2"; handler below accepts either channel name when parsing.
        sub = {"type": "subscribe", "channel": "level2", "product_ids": [PRODUCT_ID]}
        ws.send(json.dumps(sub))

    def _on_message(self, ws: WebSocketApp, message_text: str) -> None:
        try:
            msg = json.loads(message_text)
            channel = msg.get("channel")
            events = msg.get("events", []) or []
            if channel:
                self.on_message_cb({"channel": channel, "events": events})
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
def _print_book(ob: OrderBook) -> None:
    bb = ob.best_bid()
    ba = ob.best_ask()
    if bb is None or ba is None:
        print("Waiting for snapshot/update…")
        return

    print(f"\n{PRODUCT_ID} — Best Bid: {bb:.2f} | Best Ask: {ba:.2f}")
    bids, asks = ob.top_n(TOP_N)
    # Format: Price x Size (align decimals reasonably)
    print(f"Top {TOP_N} bids:")
    for px, sz in bids:
        print(f"  {px:>12.2f}  x  {sz:.6f}")
    print(f"Top {TOP_N} asks:")
    for px, sz in asks:
        print(f"  {px:>12.2f}  x  {sz:.6f}")


def main() -> None:
    ob = OrderBook(PRODUCT_ID)

    def on_msg(m: Dict[str, any]) -> None:
        ch = m["channel"]
        if ch in ("level2", "l2_data"):
            for ev in m["events"]:
                typ = ev.get("type")
                ups = ev.get("updates", []) or []
                if typ == "snapshot":
                    ob.handle_snapshot(ups)
                elif typ == "update":
                    ob.handle_update(ups)

    def on_err(e: Exception) -> None:
        print("WebSocket error:", e)

    ws = WSClient(WS_URL, on_msg, on_err)
    ws.start()

    try:
        print("Building Level 2 order book… Ctrl+C to stop.")
        last_print = 0.0
        while True:
            time.sleep(0.1)
            now = time.time()
            if now - last_print >= 1.0:  # throttle output to once per second
                _print_book(ob)
                last_print = now
    except KeyboardInterrupt:
        pass
    finally:
        ws.stop()


if __name__ == "__main__":
    main()