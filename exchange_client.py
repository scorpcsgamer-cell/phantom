"""
OKX V5 API Client — адаптер для PHANTOM.

Документация: https://www.okx.com/docs-v5/en/

Главные отличия от Bybit:
  • Аутентификация: 3 заголовка (api-key, signature, passphrase)
  • Подпись: HMAC-SHA256 + base64
  • Символы: BTC-USDT-SWAP (perpetual) вместо BTCUSDT
  • Категория: instType=SWAP вместо category=linear
  • Position mode: net_mode (one-way) или long_short_mode (hedge)
  • Размер позиции: измеряется в контрактах (sz), не в монетах
  • Tick size, lot size: возвращаются в одном /public/instruments

WebSocket public:
  Mainnet: wss://ws.okx.com:8443/ws/v5/public
  Testnet: wss://wspap.okx.com:8443/ws/v5/public  (с ?brokerId=9999)

REST:
  Mainnet: https://www.okx.com
  Testnet: https://www.okx.com (с заголовком x-simulated-trading: 1)
"""

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional

import httpx

from network_safety import RateLimiter, CircuitBreaker, NetworkWatchdog, retry_with_backoff


log = logging.getLogger("PHANTOM")


# ─────────────────────────────────────────────────
# Маппинг символов: BTCUSDT (Bybit-style) → BTC-USDT-SWAP (OKX-style)
# ─────────────────────────────────────────────────
def to_okx_symbol(sym: str) -> str:
    """BTCUSDT → BTC-USDT-SWAP."""
    if sym.endswith("-SWAP"): return sym  # уже в OKX-формате
    if sym.endswith("USDT"):
        base = sym[:-4]
        return f"{base}-USDT-SWAP"
    return sym  # неизвестный формат, не трогаем

def from_okx_symbol(sym: str) -> str:
    """BTC-USDT-SWAP → BTCUSDT."""
    if sym.endswith("-USDT-SWAP"):
        return sym[:-10] + "USDT"
    if sym.endswith("USDT"):
        return sym
    return sym


class ExchangeClient:
    """REST + auth для OKX V5 API."""

    def __init__(self, api_key: str, api_secret: str, passphrase: str,
                 testnet: bool = True):
        self.key = api_key
        self.secret = api_secret
        self.passphrase = passphrase
        self.testnet = testnet
        # OKX использует тот же URL для testnet, но с заголовком x-simulated-trading:1
        self.base = "https://www.okx.com"
        self._http: Optional[httpx.AsyncClient] = None
        self.rate_limiter = RateLimiter(max_per_second=8)
        self.circuit_breaker = CircuitBreaker(error_threshold=10,
                                                window_seconds=60,
                                                cooldown_seconds=60)
        self.watchdog = NetworkWatchdog(
            alert_after_seconds=120,
            panic_close_after_seconds=600
        )
        self.time_sync: Optional[TimeSync] = None

    async def init(self):
        self._http = httpx.AsyncClient(
            timeout=15,
            limits=httpx.Limits(max_connections=30, max_keepalive_connections=10),
        )
        # Time sync через wrapper-объект совместимый с интерфейсом TimeSync
        self.time_sync = ExchangeTimeSync(self)

    async def close(self):
        if self._http:
            await self._http.aclose()
            self._http = None

    def _ts_iso(self) -> str:
        """OKX использует ISO 8601 с миллисекундами в UTC."""
        if self.time_sync:
            ts_ms = self.time_sync.now_ms()
        else:
            ts_ms = int(time.time() * 1000)
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        # OKX ожидает формат типа: 2024-01-01T00:00:00.000Z
        return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(dt.microsecond/1000):03d}Z"

    def _sign(self, ts_iso: str, method: str, request_path: str, body: str) -> str:
        """OKX подпись: base64(hmac-sha256(secret, ts + method + path + body))."""
        message = f"{ts_iso}{method.upper()}{request_path}{body}"
        signature = hmac.new(
            self.secret.encode(),
            message.encode(),
            hashlib.sha256
        ).digest()
        return base64.b64encode(signature).decode()

    def _headers(self, ts_iso: str, sign: str) -> dict:
        h = {
            "OK-ACCESS-KEY": self.key,
            "OK-ACCESS-SIGN": sign,
            "OK-ACCESS-TIMESTAMP": ts_iso,
            "OK-ACCESS-PASSPHRASE": self.passphrase,
            "Content-Type": "application/json",
        }
        if self.testnet:
            h["x-simulated-trading"] = "1"
        return h

    async def _request(self, method: str, path: str,
                        params: dict = None, body_json: dict = None) -> dict:
        if not self._http: return {}
        if not self.circuit_breaker.can_attempt():
            log.warning(f"Circuit breaker OPEN — {path} skipped")
            return {}
        await self.rate_limiter.acquire()

        # Подготовка query string
        if params:
            qs = "&".join(f"{k}={v}" for k, v in params.items())
            request_path = f"{path}?{qs}"
        else:
            request_path = path

        body_str = json.dumps(body_json) if body_json else ""

        async def do():
            ts_iso = self._ts_iso()
            sign = self._sign(ts_iso, method, request_path, body_str)
            headers = self._headers(ts_iso, sign)
            url = f"{self.base}{request_path}"
            if method == "GET":
                r = await self._http.get(url, headers=headers)
            else:
                r = await self._http.post(url, headers=headers, content=body_str)
            return r.json()

        try:
            result = await retry_with_backoff(do, max_attempts=3, base_delay=1.0)
            if result is None:
                self.circuit_breaker.record_error()
                return {}
            self.circuit_breaker.record_success()
            self.watchdog.heartbeat()
            return result
        except Exception as e:
            self.circuit_breaker.record_error()
            log.warning(f"{method} {path} error: {e}")
            return {}

    async def get(self, path: str, params: dict = None) -> dict:
        return await self._request("GET", path, params=params)

    async def post(self, path: str, body: dict = None) -> dict:
        return await self._request("POST", path, body_json=body)

    # ── Market Data ─────────────────────────────────
    async def get_klines(self, symbol: str, interval: str = "15", limit: int = 200) -> list:
        """
        Возвращает свечи в формате Bybit: [[time, o, h, l, c, vol, turnover], ...]
        OKX bar values: 1m, 3m, 5m, 15m, 30m, 1H, 2H, 4H, 1D
        """
        bar_map = {"1": "1m", "3": "3m", "5": "5m", "15": "15m", "30": "30m",
                   "60": "1H", "120": "2H", "240": "4H", "D": "1D"}
        bar = bar_map.get(interval, "15m")
        okx_sym = to_okx_symbol(symbol)
        r = await self.get("/api/v5/market/candles",
            {"instId": okx_sym, "bar": bar, "limit": str(limit)})
        if r.get("code") != "0":
            return []
        # OKX возвращает: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
        # Bybit формат: [t, o, h, l, c, v, turnover]
        result = []
        for k in r.get("data", []):
            # OKX уже возвращает newest-first, как Bybit
            result.append([k[0], k[1], k[2], k[3], k[4], k[5], k[7] if len(k)>7 else "0"])
        return result

    async def get_tickers_all(self) -> list:
        """Все SWAP тикеры USDT-pair."""
        r = await self.get("/api/v5/market/tickers", {"instType": "SWAP"})
        if r.get("code") != "0": return []
        result = []
        for t in r.get("data", []):
            inst_id = t.get("instId", "")
            if not inst_id.endswith("-USDT-SWAP"):
                continue
            symbol = from_okx_symbol(inst_id)
            try:
                last = float(t.get("last", 0) or 0)
                high24 = float(t.get("high24h", 0) or 0)
                low24 = float(t.get("low24h", 0) or 0)
                vol_ccy_quote = float(t.get("volCcy24h", 0) or 0)  # объём в USDT
                # Изменение 24ч — OKX даёт open24h
                open24 = float(t.get("open24h", 0) or 0)
                pct_change = (last - open24) / open24 if open24 > 0 else 0
                result.append({
                    "symbol": symbol,
                    "lastPrice": str(last),
                    "highPrice24h": str(high24),
                    "lowPrice24h": str(low24),
                    "turnover24h": str(vol_ccy_quote),
                    "volume24h": str(t.get("vol24h", 0)),
                    "price24hPcnt": str(pct_change),
                    "fundingRate": "0.0001",  # будет обновлено отдельно
                })
            except Exception: continue
        return result

    async def get_ticker(self, symbol: str) -> dict:
        okx_sym = to_okx_symbol(symbol)
        r = await self.get("/api/v5/market/ticker", {"instId": okx_sym})
        if r.get("code") != "0": return {}
        data = r.get("data", [])
        if not data: return {}
        t = data[0]
        return {
            "symbol": symbol,
            "lastPrice": t.get("last"),
            "highPrice24h": t.get("high24h"),
            "lowPrice24h": t.get("low24h"),
            "turnover24h": t.get("volCcy24h"),
            "volume24h": t.get("vol24h"),
        }

    async def get_instruments_info_all(self) -> dict:
        """
        Возвращает dict {symbol: {tick_size, qty_step, min_order_qty, max_leverage, ct_val}}
        Важно: у OKX позиции считаются в контрактах (sz), а не в монетах!
        ct_val — это сколько монет в одном контракте.
        """
        r = await self.get("/api/v5/public/instruments", {"instType": "SWAP"})
        if r.get("code") != "0": return {}
        out = {}
        for item in r.get("data", []):
            try:
                inst_id = item.get("instId", "")
                if not inst_id.endswith("-USDT-SWAP"): continue
                symbol = from_okx_symbol(inst_id)
                out[symbol] = {
                    "instId": inst_id,
                    "tick_size": float(item.get("tickSz", "0.0001")),
                    "qty_step": float(item.get("lotSz", "1")),    # шаг в контрактах
                    "min_order_qty": float(item.get("minSz", "1")),
                    "max_leverage": float(item.get("lever", "100")),
                    "ct_val": float(item.get("ctVal", "1")),       # монет в 1 контракте
                    "ct_val_ccy": item.get("ctValCcy", ""),
                }
            except Exception as e:
                log.warning(f"Parse {inst_id}: {e}")
        return out

    # ── Account ─────────────────────────────────────
    async def get_balance(self) -> float:
        """USDT total equity = свободный + заблокированный в маржу + нереализованный PnL.

        Используем `eq` (полный капитал в этой валюте) вместо `availBal`
        (доступного остатка), потому что при открытых позициях с isolated
        margin часть средств блокируется. Если показывать availBal — будет
        казаться, что баланс упал на размер маржи, хотя это просто залог.
        """
        r = await self.get("/api/v5/account/balance", {"ccy": "USDT"})
        if r.get("code") != "0": return 0.0
        try:
            for acc in r.get("data", []):
                for item in acc.get("details", []):
                    if item.get("ccy") == "USDT":
                        # Приоритет: eq (total equity) → cashBal → availBal
                        for field in ("eq", "cashBal", "availBal"):
                            val = item.get(field)
                            if val:
                                return float(val)
                        return 0.0
        except Exception as e:
            log.warning(f"get_balance parse: {e}")
        return 0.0

    async def get_algo_orders(self) -> dict:
        """Получить активные attached SL/TP по позициям (algo orders).

        OKX хранит SL/TP не в позиции, а отдельным algo-ордером. Чтобы
        reconciler корректно проверял что защита есть на бирже — нам нужно
        вытянуть oco/conditional algo-ордера и сматчить их по instId.

        Возвращает: {symbol: {"sl": float, "tp": float, "algoId": str}}
        """
        out: dict = {}
        # OKX /trade/orders-algo-pending принимает один ordType за вызов.
        # attachAlgoOrds c одновременным sl+tp создаются как 'oco'.
        # Но если только один из них — могут быть 'conditional'.
        for ord_type in ("oco", "conditional"):
            try:
                r = await self.get("/api/v5/trade/orders-algo-pending",
                                   {"ordType": ord_type, "instType": "SWAP"})
                if r.get("code") != "0":
                    continue
                for a in r.get("data", []):
                    inst_id = a.get("instId", "")
                    symbol  = from_okx_symbol(inst_id)
                    try:    sl = float(a.get("slTriggerPx") or 0)
                    except: sl = 0.0
                    try:    tp = float(a.get("tpTriggerPx") or 0)
                    except: tp = 0.0
                    # Если по символу уже что-то записано — обновим непустыми значениями
                    cur = out.setdefault(symbol, {"sl": 0.0, "tp": 0.0, "algoId": ""})
                    if sl > 0: cur["sl"] = sl
                    if tp > 0: cur["tp"] = tp
                    if a.get("algoId"): cur["algoId"] = a["algoId"]
            except Exception as e:
                log.debug(f"get_algo_orders {ord_type}: {e}")
        return out

    async def get_positions(self) -> list:
        """Возвращает позиции в Bybit-совместимом формате,
        обогащённые SL/TP из связанных algo-ордеров (OKX хранит их отдельно)."""
        r = await self.get("/api/v5/account/positions", {"instType": "SWAP"})
        if r.get("code") != "0": return []
        # Параллельно подтянем все active algo orders для маппинга SL/TP
        algo_map = await self.get_algo_orders()
        result = []
        for p in r.get("data", []):
            try:
                pos_size = float(p.get("pos", 0) or 0)
                if pos_size == 0: continue  # нулевая позиция
                inst_id = p.get("instId", "")
                symbol = from_okx_symbol(inst_id)
                pos_side = p.get("posSide", "net")  # long, short, net
                # Для one-way (net) определяем сторону по знаку pos
                if pos_side == "net":
                    side = "Buy" if pos_size > 0 else "Sell"
                    abs_size = abs(pos_size)
                else:
                    side = "Buy" if pos_side == "long" else "Sell"
                    abs_size = abs(pos_size)
                # SL/TP из связанных algo-ордеров (если есть)
                algo = algo_map.get(symbol, {})
                result.append({
                    "symbol": symbol,
                    "side": side,
                    "size": str(abs_size),
                    "avgPrice": p.get("avgPx", "0"),
                    "markPrice": p.get("markPx", "0"),
                    "leverage": p.get("lever", "1"),
                    "stopLoss":   str(algo.get("sl", 0.0)),
                    "takeProfit": str(algo.get("tp", 0.0)),
                    "liqPrice": p.get("liqPx", "0"),
                    "positionIdx": 0 if pos_side == "net" else (1 if pos_side == "long" else 2),
                    "createdTime": p.get("cTime", ""),
                    "algoId": algo.get("algoId", ""),
                })
            except Exception as e:
                log.warning(f"Parse position: {e}")
        return result

    # ── Trading ─────────────────────────────────────
    async def set_leverage(self, symbol: str, lev: int) -> bool:
        """OKX: устанавливаем плечо для инструмента."""
        okx_sym = to_okx_symbol(symbol)
        # mgnMode: isolated — работает в Futures mode и выше
        r = await self.post("/api/v5/account/set-leverage", {
            "instId": okx_sym,
            "lever": str(lev),
            "mgnMode": "isolated",
            "posSide": "net",
        })
        return r.get("code") == "0"

    async def place_order(self, symbol: str, side: str, qty_str: str,
                          sl: Optional[str] = None, tp: Optional[str] = None,
                          reduce_only: bool = False,
                          position_idx: int = 0) -> dict:
        """
        Размещает рыночный ордер.
        side: "Buy" or "Sell"
        qty_str: размер в КОНТРАКТАХ (важно!)
        sl/tp: цены SL/TP, если заданы — добавляются как attached algo orders
        """
        okx_sym = to_okx_symbol(symbol)
        order_side = "buy" if side.lower() in ("buy", "long") else "sell"

        body = {
            "instId": okx_sym,
            "tdMode": "isolated",         # маржинальный режим (isolated — работает в Futures mode и выше)
            "side": order_side,
            "ordType": "market",
            "sz": qty_str,                # размер в контрактах
        }
        if reduce_only:
            body["reduceOnly"] = "true"

        # Для one-way mode posSide не нужен; для hedge — нужен
        # Здесь предполагаем one-way (как в нашем боте)
        body["posSide"] = "net"

        # SL/TP через attachAlgoOrds
        if sl or tp:
            algo = {}
            if sl:
                algo["slTriggerPx"] = sl
                algo["slOrdPx"] = "-1"  # market
            if tp:
                algo["tpTriggerPx"] = tp
                algo["tpOrdPx"] = "-1"
            if algo:
                body["attachAlgoOrds"] = [algo]

        r = await self.post("/api/v5/trade/order", body)
        # ── ДИАГНОСТИКА: логируем тело запроса и полный сырой ответ OKX ──
        log.error(f"[OKX-DEBUG] REQUEST BODY: {body}")
        log.error(f"[OKX-DEBUG] RAW RESPONSE: {r}")
        # Конвертируем формат ответа в Bybit-совместимый
        code = r.get("code", "1")
        if code == "0" and r.get("data"):
            order_data = r["data"][0]
            inner_code = order_data.get("sCode", "0")
            if inner_code == "0":
                log.info(f"✅ {side} {qty_str} {symbol}")
                return {
                    "retCode": 0,
                    "retMsg": "OK",
                    "result": {"orderId": order_data.get("ordId", "")},
                }
            else:
                log.error(f"❌ {symbol}: {inner_code} {order_data.get('sMsg', '')}")
                return {
                    "retCode": int(inner_code) if inner_code.isdigit() else 1,
                    "retMsg": order_data.get("sMsg", ""),
                    "result": {},
                }
        else:
            # ── НОВОЕ: достаём реальную причину из data[0], если есть ──
            inner_msg = ""
            inner_code = ""
            data_list = r.get("data") or []
            if data_list and isinstance(data_list, list) and isinstance(data_list[0], dict):
                inner_code = data_list[0].get("sCode", "")
                inner_msg = data_list[0].get("sMsg", "")
            full_msg = f"code={code} msg={r.get('msg','')}"
            if inner_code or inner_msg:
                full_msg += f" | inner_sCode={inner_code} sMsg={inner_msg}"
            log.error(f"❌ {symbol}: {full_msg}")
            return {
                "retCode": int(inner_code) if inner_code and inner_code.isdigit() and inner_code != "0"
                           else (int(code) if code.isdigit() else 1),
                "retMsg": inner_msg or r.get("msg", ""),
                "result": {},
            }

    async def close_market(self, symbol: str, side: str, qty_str: str,
                           position_idx: int = 0) -> dict:
        """Закрывает позицию рыночным ордером."""
        # В OKX можно использовать /trade/close-position
        okx_sym = to_okx_symbol(symbol)
        r = await self.post("/api/v5/trade/close-position", {
            "instId": okx_sym,
            "mgnMode": "isolated",
            "posSide": "net",
        })
        code = r.get("code", "1")
        if code == "0":
            log.info(f"✅ Закрыта позиция {symbol}")
            return {"retCode": 0, "retMsg": "OK", "result": {}}
        return {"retCode": int(code) if code.isdigit() else 1,
                "retMsg": r.get("msg", ""), "result": {}}

    # ── Algo Order Amend (изменение SL/TP без переоткрытия позиции) ─────────
    async def amend_algo_order(self, symbol: str, algo_id: str,
                                new_sl: Optional[float] = None,
                                new_tp: Optional[float] = None) -> dict:
        """
        Изменяет цены SL и/или TP у существующего algo-ордера через OKX
        /api/v5/trade/amend-algos. Если algo_id пустой — пытаемся найти его
        автоматически через get_algo_orders.

        Возвращает Bybit-совместимый dict: {retCode, retMsg, result}.

        ВАЖНО: этот метод — единственный валидный способ "подтянуть" SL.
        Любая запись в local_pos["sl"] без вызова amend_algo_order приведёт
        к рассинхрону с биржей (см. историю бага SL drift).
        """
        if not algo_id:
            algos = await self.get_algo_orders()
            algo_id = algos.get(symbol, {}).get("algoId", "")
            if not algo_id:
                log.error(f"[AMEND] {symbol}: algoId не найден, amend невозможен")
                return {"retCode": 1, "retMsg": "algoId not found", "result": {}}

        okx_sym = to_okx_symbol(symbol)
        body = {"instId": okx_sym, "algoId": algo_id}
        if new_sl is not None and new_sl > 0:
            body["newSlTriggerPx"] = f"{new_sl}"
            body["newSlOrdPx"] = "-1"   # market
        if new_tp is not None and new_tp > 0:
            body["newTpTriggerPx"] = f"{new_tp}"
            body["newTpOrdPx"] = "-1"
        if "newSlTriggerPx" not in body and "newTpTriggerPx" not in body:
            return {"retCode": 1, "retMsg": "nothing to amend", "result": {}}

        r = await self.post("/api/v5/trade/amend-algos", body)
        code = r.get("code", "1")
        data_list = r.get("data") or []
        inner_code = ""
        inner_msg = ""
        if data_list and isinstance(data_list, list) and isinstance(data_list[0], dict):
            inner_code = data_list[0].get("sCode", "")
            inner_msg = data_list[0].get("sMsg", "")

        if code == "0" and (not inner_code or inner_code == "0"):
            log.info(f"[AMEND] ✅ {symbol} SL→{new_sl} TP→{new_tp} (algoId={algo_id})")
            return {"retCode": 0, "retMsg": "OK",
                    "result": {"algoId": algo_id, "sl": new_sl, "tp": new_tp}}
        full_msg = f"code={code} msg={r.get('msg','')}"
        if inner_code or inner_msg:
            full_msg += f" | inner_sCode={inner_code} sMsg={inner_msg}"
        log.error(f"[AMEND] ❌ {symbol}: {full_msg}")
        return {
            "retCode": int(inner_code) if inner_code and inner_code.isdigit() and inner_code != "0"
                       else (int(code) if code.isdigit() else 1),
            "retMsg": inner_msg or r.get("msg", ""),
            "result": {},
        }

    async def place_algo_order(self, symbol: str, position_side: str,
                                 qty_str: str,
                                 sl: Optional[float] = None,
                                 tp: Optional[float] = None) -> dict:
        """Создать standalone algo-order (SL/TP) для уже открытой позиции.

        Используется для восстановления когда attachAlgoOrds не создались при
        открытии позиции (например после OKX 51050 ошибки на 4-й попытке).

        Args:
            symbol: e.g. "NEARUSDT"
            position_side: "Buy" (long) или "Sell" (short) — направление ПОЗИЦИИ.
                           Закрывающий ордер будет противоположной стороны.
            qty_str: размер в КОНТРАКТАХ как строка
            sl: цена stop-loss (опционально)
            tp: цена take-profit (опционально)

        Returns:
            {"retCode": 0, ...} при успехе или ошибка в Bybit-совместимом формате.
        """
        if not sl and not tp:
            return {"retCode": 1, "retMsg": "Ни SL ни TP не заданы"}

        okx_sym = to_okx_symbol(symbol)
        # Закрывающая сторона противоположна позиции
        close_side = "sell" if position_side.lower() in ("buy", "long") else "buy"

        # Тип ордера: oco = одновременно SL + TP, conditional = только один из них
        ord_type = "oco" if (sl and tp) else "conditional"

        body = {
            "instId": okx_sym,
            "tdMode": "isolated",
            "side": close_side,
            "posSide": "net",
            "ordType": ord_type,
            "sz": qty_str,
            "reduceOnly": "true",   # критично: только закрывает, не открывает новое
        }
        if sl:
            body["slTriggerPx"] = self._fmt_price(symbol, sl)
            body["slOrdPx"] = "-1"   # -1 = market при срабатывании
        if tp:
            body["tpTriggerPx"] = self._fmt_price(symbol, tp)
            body["tpOrdPx"] = "-1"

        r = await self.post("/api/v5/trade/order-algo", body)
        log.info(f"[PLACE-ALGO] {symbol}: body={body}")
        log.info(f"[PLACE-ALGO] {symbol}: response={r}")

        # Конвертация в Bybit-совместимый формат
        code = r.get("code", "1")
        if code == "0" and r.get("data"):
            algo_data = r["data"][0]
            inner_code = algo_data.get("sCode", "0")
            if inner_code == "0":
                log.info(f"✅ [PLACE-ALGO] {symbol}: SL={sl}, TP={tp}, "
                         f"algoId={algo_data.get('algoId', '')}")
                return {
                    "retCode": 0, "retMsg": "OK",
                    "result": {"algoId": algo_data.get("algoId", "")},
                }
            else:
                log.error(f"❌ [PLACE-ALGO] {symbol}: {inner_code} "
                          f"{algo_data.get('sMsg', '')}")
                return {
                    "retCode": int(inner_code) if inner_code.isdigit() else 1,
                    "retMsg": algo_data.get("sMsg", "Unknown error"),
                }
        return {
            "retCode": int(code) if code.isdigit() else 1,
            "retMsg": r.get("msg", "Unknown error"),
        }

    def _fmt_price(self, symbol: str, price: float) -> str:
        """Форматирование цены — для совместимости с _fmt_price если есть InstrumentRegistry.
        Если регистра нет — возвращает строку с разумной точностью."""
        # Если у клиента есть instruments — используем round_price
        if hasattr(self, "instruments") and self.instruments:
            return self.instruments.round_price(symbol, price)
        # Fallback: 6 значащих цифр для копеечных альтов, 2 для обычных
        if price < 0.01:
            return f"{price:.8f}"
        return f"{price:.6g}"

    async def update_sl_tp(self, symbol: str,
                            sl: Optional[float] = None,
                            tp: Optional[float] = None,
                            verify: bool = True) -> bool:
        """
        Высокоуровневая обёртка: меняет SL/TP на бирже и (опционально)
        проверяет что изменение прижилось через повторный get_algo_orders.

        Returns:
            True — биржа подтвердила новое значение
            False — amend не сработал или verify не подтвердил

        Вызывать ВМЕСТО прямой записи в local_pos["sl"].
        """
        # 1. Достаём текущий algoId
        algos = await self.get_algo_orders()
        info = algos.get(symbol, {})
        algo_id = info.get("algoId", "")
        if not algo_id:
            log.warning(f"[UPDATE-SLTP] {symbol}: алго-ордер не найден на бирже, "
                        f"возможно SL/TP не были выставлены при открытии")
            return False

        # 2. Шлём amend
        r = await self.amend_algo_order(symbol, algo_id, new_sl=sl, new_tp=tp)
        if r.get("retCode") != 0:
            return False

        if not verify:
            return True

        # 3. Verify: даём бирже ~600ms на обработку, потом сверяем
        await asyncio.sleep(0.6)
        algos2 = await self.get_algo_orders()
        info2 = algos2.get(symbol, {})
        ok = True
        if sl is not None and sl > 0:
            ex_sl = info2.get("sl", 0.0)
            # допуск 0.5% — OKX может округлить до tick_size
            if ex_sl <= 0 or abs(ex_sl - sl) / max(sl, 1e-9) > 0.005:
                log.error(f"[VERIFY] {symbol} SL: ожидали {sl}, на бирже {ex_sl}")
                ok = False
        if tp is not None and tp > 0:
            ex_tp = info2.get("tp", 0.0)
            if ex_tp <= 0 or abs(ex_tp - tp) / max(tp, 1e-9) > 0.005:
                log.error(f"[VERIFY] {symbol} TP: ожидали {tp}, на бирже {ex_tp}")
                ok = False
        if ok:
            log.info(f"[VERIFY] ✅ {symbol} SL/TP подтверждены биржей")
        return ok


# ─────────────────────────────────────────────────
# Time Sync для OKX
# ─────────────────────────────────────────────────
class ExchangeTimeSync:
    """Синхронизация времени с OKX. OKX endpoint: /api/v5/public/time"""

    def __init__(self, client: ExchangeClient):
        self.client = client
        self.offset_ms: int = 0
        self.last_sync: float = 0.0
        self.sync_interval_s: int = 600
        self._sync_in_progress: bool = False

    async def sync(self) -> bool:
        if self._sync_in_progress: return False
        self._sync_in_progress = True
        self.last_sync = time.monotonic()
        try:
            t_local_before = int(time.time() * 1000)
            r = await self.client.get("/api/v5/public/time")
            t_local_after = int(time.time() * 1000)
            if r.get("code") == "0":
                rtt = t_local_after - t_local_before
                data = r.get("data", [{}])
                t_server = int(data[0].get("ts", "0")) if data else 0
                if t_server == 0: return False
                t_local_mid = (t_local_before + t_local_after) // 2
                self.offset_ms = t_server - t_local_mid
                if abs(self.offset_ms) > 5000:
                    log.warning(f"Большой clock skew OKX: {self.offset_ms}ms (RTT={rtt}ms)")
                else:
                    log.info(f"Время OKX синхронизировано: offset={self.offset_ms:+d}ms (RTT={rtt}ms)")
                return True
            return False
        except Exception as e:
            log.warning(f"OKX time sync error: {e}")
            return False
        finally:
            self._sync_in_progress = False

    def now_ms(self) -> int:
        return int(time.time() * 1000) + self.offset_ms

    def needs_resync(self) -> bool:
        if self._sync_in_progress: return False
        if self.last_sync == 0: return True
        return (time.monotonic() - self.last_sync) >= self.sync_interval_s

    @property
    def is_critical_skew(self) -> bool:
        return abs(self.offset_ms) > 4000
