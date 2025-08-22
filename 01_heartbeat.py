"""
01_heartbeat.py
Purpose: Open a live WebSocket connection to Coinbase Advanced Trade and print
         heartbeat messages to confirm connectivity.
Run:
python3 01_heartbeat.py

Stop with Ctrl+C.
"""

import json
import threading
import time
import ssl               #for SSL options
import certifi           #uses a known-good CA bundle
from typing import Callable, Dict, Any, Optional

from websocket import WebSocketApp

WS_URL = "wss://advanced-trade-ws.coinbase.com"


class WSClient:
    """
    Minimal WebSocket client wrapper:
      - Opens the connection
      - Subscribes to requested channels
      - Dispatches incoming messages to a user-supplied callback
      - Attempts automatic reconnection if the socket drops
    """

    def __init__(
        self,
        url: str,
        on_message: Callable[[Dict[str, Any]], None],
        on_error: Optional[Callable[[Exception], None]] = None,
    ):
        # Store configuration and callbacks.
        self.url = url
        self.on_message_cb = on_message     # Called on each inbound message
        self.on_error_cb = on_error         # Called on socket/library errors

        # Lazy-initialized runtime attributes.
        self.ws: Optional[WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None
        self._stop: bool = False

    # ---- WebSocketApp callbacks ----

    def _on_open(self, ws: WebSocketApp) -> None:
        # Subscribe to 'heartbeats' to get periodic keep-alive messages.
        ws.send(json.dumps({"type": "subscribe", "channel": "heartbeats"}))

    def _on_message(self, ws: WebSocketApp, message_text: str) -> None:
        # Parse JSON and forward a compact dict to the user callback.
        try:
            msg = json.loads(message_text)
            channel = msg.get("channel")
            events = msg.get("events", []) or []
            if channel:
                self.on_message_cb({"channel": channel, "events": events, "raw": msg})
        except Exception as e:
            if self.on_error_cb:
                self.on_error_cb(e)

    def _on_error(self, ws: WebSocketApp, error: Exception) -> None:
        if self.on_error_cb:
            self.on_error_cb(error)

    def _on_close(self, ws: WebSocketApp, code: int, reason: str) -> None:
        # Reconnect with exponential backoff, unless a stop was requested.
        if not self._stop:
            delay = 1.0
            while not self._stop:
                time.sleep(delay)
                try:
                    self._connect()
                    return
                except Exception:
                    delay = min(delay * 2, 60.0)

    # ---- Control methods ----

    def _connect(self) -> None:
        # Create the WebSocketApp and enter its event loop with explicit SSL options.
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
            sslopt={
                "ca_certs": certifi.where(),        # trusted CA bundle
                "cert_reqs": ssl.CERT_REQUIRED,     # require valid certs
            },
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
        except Exception:
            pass
        if self._thread:
            self._thread.join(timeout=3)


# ---- Message handlers ----

def on_message(msg: Dict[str, Any]) -> None:
    if msg["channel"] == "heartbeats":
        print("❤️  heartbeat received")


def on_error(e: Exception) -> None:
    print("WebSocket error:", e)


# ---- Entry point ----

if __name__ == "__main__":
    client = WSClient(WS_URL, on_message, on_error)
    client.start()
    try:
        print("Connected… waiting for heartbeats. Press Ctrl+C to stop.")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping…")
    finally:
        client.stop()

