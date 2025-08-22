"""
02_trade_feed.py
================
Subscribe to BTC/USD and manage: Trade feed (executed trades / time & sales)
This File:
- Opens a PUBLIC WebSocket connection to Coinbase Advanced Trade (no API key needed).
- Subscribes ONLY to the 'market_trades' channel for BTC-USD.
- Prints each executed trade as it arrives (fields: side, price, size).
"""
from __future__ import annotations

import json
import threading
import time
import ssl
from typing import Callable, Dict, Any, Optional

import certifi
from websocket import WebSocketApp

WS_URL = "wss://advanced-trade-ws.coinbase.com"
PRODUCT_ID = "BTC-USD"


class WSClient:
    """Light wrapper around WebSocketApp with auto-reconnect and message dispatch."""

    def __init__(
        self,
        url: str,
        on_message: Callable[[Dict[str, Any]], None],
        on_error: Optional[Callable[[Exception], None]] = None,
    ) -> None:
        self.url = url
        self.on_message_cb = on_message
        self.on_error_cb = on_error
        self.ws: Optional[WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None
        self._stop = False

    # WebSocket callbacks ------------------------------------------------------

    def _on_open(self, ws: WebSocketApp) -> None:
        sub = {"type": "subscribe", "channel": "market_trades", "product_ids": [PRODUCT_ID]}
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

    # Control ------------------------------------------------------------------

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


# Business logic ---------------------------------------------------------------

def on_message(msg: Dict[str, Any]) -> None:
    if msg["channel"] != "market_trades":
        return
    for ev in msg["events"]:
        for tr in ev.get("trades", []):
            side = tr.get("side")      # "BUY" / "SELL"
            price = tr.get("price")    # string
            size = tr.get("size")      # string
            print(f"[TRADE] {PRODUCT_ID} side={side} price={price} size={size}")


def on_error(e: Exception) -> None:
    print("WebSocket error:", e)


if __name__ == "__main__":
    client = WSClient(WS_URL, on_message, on_error)
    client.start()
    try:
        print("Streaming executed trades… Ctrl+C to stop.")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        client.stop()