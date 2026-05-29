"""
OKX V5 WebSocket public stream.

URL:
  Mainnet: wss://ws.okx.com:8443/ws/v5/public
  Testnet: wss://wspap.okx.com:8443/ws/v5/public

Особенности:
  • Подписка: {"op": "subscribe", "args": [{"channel": "tickers", "instId": "BTC-USDT-SWAP"}]}
  • Heartbeat: каждые 30 сек шлём строку "ping" (не JSON!), получаем "pong"
  • Закрывает соединение после 30 сек без активности
"""

import asyncio
import json
import logging
import time
from typing import Callable, Optional

try:
    import websockets
    from websockets.exceptions import ConnectionClosed
    HAS_WS = True
except ImportError:
    HAS_WS = False
    websockets = None
    ConnectionClosed = Exception

from exchange_client import to_okx_symbol, from_okx_symbol

log = logging.getLogger("PHANTOM_ws")


class ExchangeWSStream:
    """OKX public WebSocket stream."""

    PING_INTERVAL = 25   # OKX рекомендует ping не реже 30 сек

    def __init__(self, testnet: bool = True):
        self.url = ("wss://wspap.okx.com:8443/ws/v5/public"
                    if testnet
                    else "wss://ws.okx.com:8443/ws/v5/public")
        self.testnet = testnet
        self.ws = None
        self.connected = False
        self.subscribed_symbols: set = set()
        self.unsupported_symbols: set = set()
        self.last_message_time = 0.0
        self.tickers: dict = {}      # symbol → ticker data
        self.on_ticker: Optional[Callable[[dict], None]] = None
        self.on_disconnect: Optional[Callable[[], None]] = None
        self._stop = False
        self._reconnect_attempt = 0

    async def connect(self):
        if not HAS_WS:
            log.warning("websockets not installed — WS отключен")
            return False
        try:
            self.ws = await websockets.connect(
                self.url,
                ping_interval=None,
                close_timeout=5,
            )
            self.connected = True
            self._reconnect_attempt = 0
            self.last_message_time = time.monotonic()
            log.info(f"OKX WS connected to {self.url}")
            return True
        except Exception as e:
            log.error(f"OKX WS connect failed: {e}")
            self.connected = False
            return False

    async def subscribe(self, symbols: list):
        if not self.ws or not self.connected: return
        symbols_to_sub = [s for s in symbols if s not in self.unsupported_symbols]
        if not symbols_to_sub: return
        # OKX требует список args, каждый с channel + instId
        args = [{"channel": "tickers", "instId": to_okx_symbol(s)}
                for s in symbols_to_sub]
        msg = {"op": "subscribe", "args": args}
        try:
            await self.ws.send(json.dumps(msg))
            self.subscribed_symbols.update(symbols_to_sub)
            log.info(f"OKX WS подписался на {len(symbols_to_sub)} тикеров")
        except Exception as e:
            log.warning(f"OKX WS subscribe error: {e}")

    async def unsubscribe(self, symbols: list):
        if not self.ws or not self.connected: return
        args = [{"channel": "tickers", "instId": to_okx_symbol(s)}
                for s in symbols]
        msg = {"op": "unsubscribe", "args": args}
        try:
            await self.ws.send(json.dumps(msg))
            self.subscribed_symbols.difference_update(symbols)
        except Exception as e:
            log.warning(f"OKX WS unsubscribe error: {e}")

    async def _send_ping(self):
        """OKX ping = строка 'ping', НЕ JSON!"""
        while self.connected and not self._stop:
            try:
                await asyncio.sleep(self.PING_INTERVAL)
                if self.ws:
                    await self.ws.send("ping")
            except Exception:
                break

    async def _handle_message(self, raw_msg):
        """Обработка входящего сообщения."""
        # OKX pong
        if isinstance(raw_msg, str) and raw_msg == "pong":
            self.last_message_time = time.monotonic()
            return

        try:
            msg = json.loads(raw_msg) if isinstance(raw_msg, str) else raw_msg
        except Exception:
            return

        # Subscription confirmation/error
        if msg.get("event") == "subscribe":
            return  # OK
        if msg.get("event") == "error":
            code = msg.get("code", "")
            error_msg = msg.get("msg", "")
            log.warning(f"OKX WS error {code}: {error_msg}")
            # Если ошибка про конкретный инструмент — извлечём
            arg = msg.get("arg", {})
            if isinstance(arg, dict):
                inst_id = arg.get("instId", "")
                if inst_id and ("not found" in error_msg.lower()
                                or "not exist" in error_msg.lower()):
                    sym = from_okx_symbol(inst_id)
                    self.subscribed_symbols.discard(sym)
                    self.unsupported_symbols.add(sym)
                    log.info(f"Символ {sym} не поддерживается на OKX, исключён")
            return

        # Ticker data
        arg = msg.get("arg", {})
        if isinstance(arg, dict) and arg.get("channel") == "tickers":
            data_list = msg.get("data", [])
            for data in data_list:
                inst_id = data.get("instId", "")
                if not inst_id: continue
                symbol = from_okx_symbol(inst_id)
                try:
                    last = float(data.get("last", 0) or 0)
                    open24 = float(data.get("open24h", 0) or 0)
                    pct_change = (last - open24) / open24 if open24 > 0 else 0
                    cached = self.tickers.get(symbol, {})
                    cached.update({
                        "symbol": symbol,
                        "lastPrice": last,
                        "markPrice": last,
                        "indexPrice": float(data.get("idxPx", last) or last),
                        "highPrice24h": float(data.get("high24h", 0) or 0),
                        "lowPrice24h": float(data.get("low24h", 0) or 0),
                        "turnover24h": float(data.get("volCcy24h", 0) or 0),
                        "volume24h": float(data.get("vol24h", 0) or 0),
                        "price24hPcnt": pct_change,
                        "fundingRate": cached.get("fundingRate", 0.0001),
                        "_updated_at": time.time(),
                    })
                    self.tickers[symbol] = cached
                    if self.on_ticker:
                        try: self.on_ticker(cached)
                        except Exception as e: log.warning(f"on_ticker: {e}")
                except Exception as e:
                    log.warning(f"Parse ticker {symbol}: {e}")

    async def listen(self):
        """Основной цикл приёма."""
        ping_task = None
        try:
            while not self._stop:
                if not self.connected:
                    delay = min(60, 2 ** min(self._reconnect_attempt, 6))
                    self._reconnect_attempt += 1
                    log.info(f"OKX WS reconnect через {delay}s (попытка {self._reconnect_attempt})")
                    await asyncio.sleep(delay)
                    if not await self.connect(): continue
                    if self.subscribed_symbols:
                        await self.subscribe(list(self.subscribed_symbols))
                    if ping_task and not ping_task.done(): ping_task.cancel()
                    ping_task = asyncio.create_task(self._send_ping())

                try:
                    msg_raw = await asyncio.wait_for(self.ws.recv(), timeout=300)
                    self.last_message_time = time.monotonic()
                    await self._handle_message(msg_raw)
                except asyncio.TimeoutError:
                    log.warning("OKX WS timeout 300s — переподключение")
                    self.connected = False
                    if self.ws:
                        try: await self.ws.close()
                        except Exception: pass
                except (ConnectionClosed, Exception) as e:
                    log.warning(f"OKX WS error: {e}")
                    self.connected = False
                    if self.on_disconnect:
                        try: self.on_disconnect()
                        except Exception: pass
        finally:
            if ping_task and not ping_task.done(): ping_task.cancel()
            if self.ws:
                try: await self.ws.close()
                except Exception: pass
            self.connected = False

    async def stop(self):
        self._stop = True
        if self.ws:
            try: await self.ws.close()
            except Exception: pass
        self.connected = False

    def get_ticker(self, symbol: str) -> dict:
        return self.tickers.get(symbol, {})

    def get_all_tickers(self) -> list:
        return list(self.tickers.values())

    @property
    def is_stale(self) -> bool:
        if not self.connected: return True
        return (time.monotonic() - self.last_message_time) > 60
