"""
PHANTOM v1.0 — OKX Crypto Futures Trading Bot
============================================================
Автономный торговый бот для криптовалютных фьючерсов на OKX.

Возможности:
  ✓ Multi-tier стратегия по 50 топ-парам
  ✓ 16 технических индикаторов в анализе
  ✓ 5 торговых стратегий (trend, scalp, breakout, dca, mean_reversion)
  ✓ Position Mode detection (one-way / hedge)
  ✓ Liquidation distance monitor
  ✓ SL/TP drift detection
  ✓ Order Health throttling (5 ошибок/мин → пауза)
  ✓ Multi-level Partial Take-Profit
  ✓ Telegram alerts
  ✓ Walk-forward validation
  ✓ Time sync (защита от clock skew)
  ✓ Real-time WebSocket data
  ✓ Auto state persistence + restore
  ✓ Network panic-close protection
  ✓ Drawdown guard

Модули проекта:
  bot_server.py            — главный сервер (FastAPI)
  exchange_client.py       — клиент биржи (OKX)
  ws_stream.py             — WebSocket public stream
  position_mode.py         — определение режима позиций
  fees_funding.py          — комиссии и funding rate
  state_store.py           — персистентность JSON
  network_safety.py        — rate limit, circuit breaker, watchdog
  reconciler.py            — синхронизация с биржей
  liquidation_monitor.py   — мониторинг ликвидации
  order_health.py          — троттлинг критических ошибок
  partial_tp.py            — multi-level take-profit
  telegram_notifier.py     — Telegram алерты
  walk_forward.py          — walk-forward валидация (CLI)
  backtest.py              — бэктест-движок (CLI)

Требования:
    pip install -r requirements.txt

Запуск:
    python bot_server.py
"""

import asyncio
import contextlib
import hashlib
import hmac
import json
import logging
import logging.handlers
import math
import os
import time
import warnings
from collections import deque
from datetime import datetime, timezone
from typing import Optional

# Подавляем pandas FutureWarning (косметика, не влияет на работу)
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=PendingDeprecationWarning)
# Подавляем через переменную окружения тоже (для надёжности)
os.environ.setdefault("PYTHONWARNINGS", "ignore::FutureWarning,ignore::DeprecationWarning")

import numpy as np
import pandas as pd
# Дополнительно отключаем chained assignment warning в pandas
pd.set_option('mode.chained_assignment', None)
try:
    pd.set_option('future.no_silent_downcasting', True)
except Exception: pass
import uvicorn
import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from contextlib import asynccontextmanager

# Локальные модули (OKX edition)
from fees_funding         import FeeCalculator, FundingTracker
from state_store          import StateManager
from network_safety       import RateLimiter, CircuitBreaker, NetworkWatchdog, retry_with_backoff
from ws_stream import ExchangeWSStream as WSStream, HAS_WS  # alias для совместимости
from reconciler           import Reconciler
from position_mode    import PositionModeManager
from liquidation_monitor  import LiquidationMonitor
from order_health         import OrderHealthMonitor, CRITICAL_ERROR_CODES
from telegram_notifier    import TelegramNotifier
from exchange_client           import ExchangeClient
from partial_tp           import PartialTPManager, PartialTPConfig, PartialTPState
from volume_anomaly       import (
    VolumeAnomalyDetector,
    VolumeDropGuard,
    VolumeDivergenceIndicator,
)
# ── Phase 1 modules (Fib+Div strategy) ──────────────────────────────
# Эти модули реализуют новую стратегию: multi-TF trend filter + Fib retracement
# entry + RSI/MACD дивергенция как подтверждение. Включается через
# cfg.USE_FIB_STRATEGY=true в .env. Если false — работает старая логика.
from trend_filter        import MultiTFTrendFilter, check_trend
from fib_engine          import detect_fib_setup, get_tp_ladder, FibSetup
from divergence          import check_divergences
from volatility          import pick_adaptive_tf
from meme_strategy       import (
    is_meme as is_meme_symbol,
    check_meme_setup,
    MemeSetup,
    DEFAULT_MEMES,
)

load_dotenv()

# ──────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────
_log_file = logging.handlers.RotatingFileHandler(
    "phantom_bot.log", maxBytes=5_000_000, backupCount=5, encoding="utf-8"
)
_log_file.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
_console = logging.StreamHandler()
_console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logging.basicConfig(level=logging.INFO, handlers=[_log_file, _console])
log = logging.getLogger("PHANTOM")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("websockets").setLevel(logging.WARNING)

# ──────────────────────────────────────────────
# ТОП-50 АЛЬТКОИНОВ
# ──────────────────────────────────────────────
# UNIVERSE_SYMBOLS — расширенный список (78 пар).
# На testnet OKX часть пар недоступна — бот при старте отфильтрует существующие.
# Имя TOP50_SYMBOLS сохранено для обратной совместимости со старым кодом.
TOP50_SYMBOLS = [
    # Majors (5)
    "BTCUSDT", "ETHUSDT", "BNBUSDT",  "SOLUSDT",  "XRPUSDT",
    # Large caps (15)
    "ADAUSDT", "AVAXUSDT","DOGEUSDT", "TRXUSDT",  "LINKUSDT",
    "DOTUSDT", "LTCUSDT", "ATOMUSDT", "ETCUSDT",  "XLMUSDT",
    "BCHUSDT", "FILUSDT", "ICPUSDT",  "NEARUSDT", "TONUSDT",
    # Mid caps L2 / DeFi anchors (15)
    "OPUSDT",  "ARBUSDT", "INJUSDT",  "SUIUSDT",  "JUPUSDT",
    "AAVEUSDT","MKRUSDT", "COMPUSDT", "CRVUSDT",  "LDOUSDT",
    "STXUSDT", "IMXUSDT", "APEUSDT",  "ALGOUSDT", "BLURUSDT",
    # DeFi / infra / gaming (15)
    "FETUSDT", "GRTUSDT", "DYDXUSDT", "ENSUSDT",  "MASKUSDT",
    "GMXUSDT", "RUNEUSDT","GMTUSDT",  "MINAUSDT", "FLOWUSDT",
    "AXSUSDT", "MANAUSDT","GALAUSDT", "SANDUSDT", "ANKRUSDT",
    # Narratives / newer listings (10)
    "ORDIUSDT","WLDUSDT", "TIAUSDT",  "SEIUSDT",  "PYTHUSDT",
    "POLUSDT", "UNIUSDT", "APTUSDT",  "WIFUSDT",  "RENDERUSDT",
    # Older majors / extended (8)
    "NEOUSDT", "EOSUSDT", "DASHUSDT", "ZECUSDT",  "IOTAUSDT",
    "HBARUSDT","THETAUSDT","CFXUSDT",
    # Memes (10)
    "PEPEUSDT","FLOKIUSDT","BONKUSDT","SHIBUSDT", "BOMEUSDT",
    "MEMEUSDT","POPCATUSDT","MEWUSDT","GOATUSDT","ACTUSDT",
]

SYMBOL_TIERS = {
    "tier1": ["BTCUSDT","ETHUSDT"],
    "tier2": ["BNBUSDT","SOLUSDT","XRPUSDT","ADAUSDT","AVAXUSDT",
              "DOGEUSDT","TRXUSDT","LINKUSDT","DOTUSDT","LTCUSDT",
              "BCHUSDT","ATOMUSDT","NEARUSDT","TONUSDT","FILUSDT"],
    "tier4": ["PEPEUSDT","FLOKIUSDT","BONKUSDT","SHIBUSDT","WIFUSDT",
              "BOMEUSDT","MEMEUSDT","POPCATUSDT","MEWUSDT","GOATUSDT","ACTUSDT"],
}
_t124 = set(SYMBOL_TIERS["tier1"]+SYMBOL_TIERS["tier2"]+SYMBOL_TIERS["tier4"])
SYMBOL_TIERS["tier3"] = [s for s in TOP50_SYMBOLS if s not in _t124]

TIER_MULT    = {"tier1":1.5, "tier2":1.0, "tier3":0.7, "tier4":0.4}
MAX_PER_TIER = {"tier1":2,   "tier2":4,   "tier3":4,   "tier4":2}

def get_tier(symbol: str) -> str:
    for t, syms in SYMBOL_TIERS.items():
        if symbol in syms: return t
    return "tier3"

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────
class Config:
    # OKX API (вместо Bybit)
    OKX_API_KEY:       str   = os.getenv("OKX_API_KEY", "")
    OKX_API_SECRET:    str   = os.getenv("OKX_API_SECRET", "")
    OKX_PASSPHRASE:    str   = os.getenv("OKX_PASSPHRASE", "")
    OKX_TESTNET:       bool  = os.getenv("OKX_TESTNET", "true").lower() == "true"
    # Алиасы для обратной совместимости со старым кодом
    API_KEY:           str   = OKX_API_KEY
    API_SECRET:        str   = OKX_API_SECRET
    TESTNET:           bool  = OKX_TESTNET
    # Risk
    RISK_PER_TRADE:    float = float(os.getenv("RISK_PER_TRADE",    "1.0"))
    MAX_DAILY_LOSS:    float = float(os.getenv("MAX_DAILY_LOSS",    "5.0"))
    MAX_DRAWDOWN_STOP: float = float(os.getenv("MAX_DRAWDOWN_STOP", "15.0"))
    LEVERAGE:          int   = int(os.getenv("LEVERAGE",            "5"))
    MAX_POSITIONS:     int   = int(os.getenv("MAX_POSITIONS",       "10"))
    # Защита от over-trading: максимум открытий в час.
    # Помогает не сжечь депозит на комиссиях если стратегия даёт частые ложные сигналы.
    MAX_TRADES_PER_HOUR: int = int(os.getenv("MAX_TRADES_PER_HOUR", "4"))
    SL_PCT:            float = float(os.getenv("SL_PCT",            "3.0"))
    TP_PCT:            float = float(os.getenv("TP_PCT",            "6.0"))
    TRAIL_PCT:         float = float(os.getenv("TRAIL_PCT",         "2.0"))
    SIGNAL_CONFIDENCE: float = float(os.getenv("SIGNAL_CONFIDENCE", "68.0"))
    COOLDOWN_AFTER_SL: int   = int(os.getenv("COOLDOWN_AFTER_SL",   "30"))
    # Scanner
    SCAN_INTERVAL:     int   = int(os.getenv("SCAN_INTERVAL",       "30"))
    # Warm-up phase: после старта бот несколько полных циклов сканера
    # только наблюдает рынок, не открывая позиций. Это защищает от:
    # 1) торговли по старым сигналам из state.json
    # 2) открытия пачки позиций в первые секунды
    # 3) неустоявшегося определения режима рынка
    WARMUP_SCANS:      int   = int(os.getenv("WARMUP_SCANS",        "3"))
    # Signal persistence: сигнал должен повториться N раз подряд прежде
    # чем по нему открыть позицию. Фильтрует шум и однократные ложные сигналы.
    SIGNAL_CONFIRM_TICKS: int = int(os.getenv("SIGNAL_CONFIRM_TICKS", "2"))
    MIN_VOLUME_USDT:   float = float(os.getenv("MIN_VOLUME_USDT",   "3000000"))
    MIN_ATR_PCT:       float = float(os.getenv("MIN_ATR_PCT",       "1.0"))
    STRATEGY:          str   = os.getenv("STRATEGY",                "trend")
    # Авто-переключение стратегий:
    # если STRATEGY=auto — бот сам выбирает по режиму рынка
    # иначе — используется фиксированная стратегия из STRATEGY
    AUTO_STRATEGY_REASSESS_S: int = int(os.getenv("AUTO_STRATEGY_REASSESS_S", "300"))
    ACTIVE_SYMBOLS:    list  = TOP50_SYMBOLS.copy()
    # Internal
    BALANCE_REFRESH_S:    int = 60
    RECONCILE_INTERVAL_S: int = int(os.getenv("RECONCILE_INTERVAL_S", "300"))
    PERSIST_INTERVAL_S:   int = int(os.getenv("PERSIST_INTERVAL_S",   "30"))
    LIQ_CHECK_INTERVAL_S: int = int(os.getenv("LIQ_CHECK_INTERVAL_S", "60"))
    # Network safety
    PANIC_CLOSE_AFTER_S:  int = int(os.getenv("PANIC_CLOSE_AFTER_S",  "600"))
    # Funding
    AVOID_FUNDING_MIN:    int = int(os.getenv("AVOID_FUNDING_MIN",    "5"))
    # Slippage
    SLIPPAGE_PCT:       float = float(os.getenv("SLIPPAGE_PCT",       "0.05"))
    # WebSocket
    USE_WEBSOCKET:       bool = os.getenv("USE_WEBSOCKET", "true").lower() == "true"
    AUTO_RESTORE:        bool = os.getenv("AUTO_RESTORE",  "true").lower() == "true"
    # Position mode
    AUTO_SWITCH_POSITION_MODE: bool = os.getenv("AUTO_SWITCH_POSITION_MODE", "false").lower() == "true"
    REQUIRE_ONE_WAY_MODE:      bool = os.getenv("REQUIRE_ONE_WAY_MODE",      "true").lower() == "true"
    # Liquidation
    LIQ_CRITICAL_PCT:     float = float(os.getenv("LIQ_CRITICAL_PCT",  "30.0"))
    LIQ_EMERGENCY_PCT:    float = float(os.getenv("LIQ_EMERGENCY_PCT", "15.0"))
    AUTO_RESTORE_SL_TP:    bool = os.getenv("AUTO_RESTORE_SL_TP", "true").lower() == "true"
    # Order health
    ORDER_HEALTH_THRESHOLD:  int = int(os.getenv("ORDER_HEALTH_THRESHOLD",  "5"))
    ORDER_HEALTH_WINDOW_S:   int = int(os.getenv("ORDER_HEALTH_WINDOW_S",   "60"))
    ORDER_HEALTH_PAUSE_S:    int = int(os.getenv("ORDER_HEALTH_PAUSE_S",    "300"))
    # Telegram
    TELEGRAM_BOT_TOKEN:    str  = os.getenv("TELEGRAM_BOT_TOKEN",    "")
    TELEGRAM_CHAT_ID:      str  = os.getenv("TELEGRAM_CHAT_ID",      "")
    TELEGRAM_ENABLED:      bool = os.getenv("TELEGRAM_ENABLED", "false").lower() == "true"
    # Daily summary
    DAILY_SUMMARY_HOUR_UTC: int = int(os.getenv("DAILY_SUMMARY_HOUR_UTC", "0"))
    # Partial TP (v1.0 NEW)
    ENABLE_PARTIAL_TP:        bool = os.getenv("ENABLE_PARTIAL_TP", "false").lower() == "true"
    TP_LEVELS:                str  = os.getenv("TP_LEVELS", "3:50,6:30,10:20")
    MOVE_SL_TO_BE_AFTER_TP1:  bool = os.getenv("MOVE_SL_TO_BE_AFTER_TP1", "true").lower() == "true"
    TRAIL_AFTER_LAST_TP:      bool = os.getenv("TRAIL_AFTER_LAST_TP", "true").lower() == "true"
    TRAIL_PCT_AFTER_ALL_TP:   float = float(os.getenv("TRAIL_PCT_AFTER_ALL_TP", "1.5"))
    # ── Volume Anomaly Modules (v1.1 NEW) ──────────────────────
    # 1) Z-score детектор: блокирует вход при аномальных всплесках/обвалах объёма
    VOLUME_ANOMALY_ENABLED:   bool  = os.getenv("VOLUME_ANOMALY_ENABLED", "true").lower() == "true"
    VOLUME_Z_WINDOW:          int   = int(os.getenv("VOLUME_Z_WINDOW", "50"))
    VOLUME_Z_THRESHOLD:       float = float(os.getenv("VOLUME_Z_THRESHOLD", "3.0"))
    # 2) Drop Guard: защита открытых позиций от пересыхания ликвидности
    VOLUME_DROP_GUARD_ENABLED: bool  = os.getenv("VOLUME_DROP_GUARD_ENABLED", "true").lower() == "true"
    VOLUME_DROP_FACTOR:        float = float(os.getenv("VOLUME_DROP_FACTOR", "3.0"))
    VOLUME_DROP_ACTION:        str   = os.getenv("VOLUME_DROP_ACTION", "alert")  # alert|tighten_sl|close
    VOLUME_DROP_TIGHTEN_PCT:   float = float(os.getenv("VOLUME_DROP_TIGHTEN_PCT", "1.0"))
    # 3) Price/Volume Divergence: 17-й индикатор для голосования
    VOLUME_DIVERGENCE_ENABLED: bool  = os.getenv("VOLUME_DIVERGENCE_ENABLED", "true").lower() == "true"
    VOLUME_DIV_LOOKBACK:       int   = int(os.getenv("VOLUME_DIV_LOOKBACK", "10"))

    # ── Phase 1: Fib + Div strategy (NEW) ──────────────────────
    # Главный переключатель: false (default) = старая логика "топ движущихся",
    # true = новая стратегия multi-TF trend + Fib + RSI/MACD дивергенция.
    USE_FIB_STRATEGY:          bool  = os.getenv("USE_FIB_STRATEGY", "false").lower() == "true"
    # Trend filter (4h + 1h)
    TREND_SLOPE_THRESHOLD:     float = float(os.getenv("TREND_SLOPE_THRESHOLD", "0.001"))
    TREND_CACHE_TTL_S:         int   = int(os.getenv("TREND_CACHE_TTL_S", "300"))
    # Fib engine
    FIB_TOLERANCE_PCT:         float = float(os.getenv("FIB_TOLERANCE_PCT", "0.3"))
    FIB_SWING_ATR_MULT:        float = float(os.getenv("FIB_SWING_ATR_MULT", "1.5"))
    # Divergence
    DIV_LOOKBACK:              int   = int(os.getenv("DIV_LOOKBACK", "30"))
    # Adaptive timeframe (можно отключить — будем использовать только 15m)
    USE_ADAPTIVE_TF:           bool  = os.getenv("USE_ADAPTIVE_TF", "true").lower() == "true"
    FALLBACK_TF:               str   = os.getenv("FALLBACK_TF", "15m")
    # Meme strategy (отдельная логика для PEPE/FLOKI/...)
    MEME_STRATEGY_ENABLED:     bool  = os.getenv("MEME_STRATEGY_ENABLED", "true").lower() == "true"
    MEME_RISK_PCT:             float = float(os.getenv("MEME_RISK_PCT", "0.75"))
    MEME_SL_PCT:               float = float(os.getenv("MEME_SL_PCT", "1.0"))
    MEME_TP_PCT:               float = float(os.getenv("MEME_TP_PCT", "2.0"))
    MEME_MAX_CONCURRENT:       int   = int(os.getenv("MEME_MAX_CONCURRENT", "1"))
    # Correlation guard (max 2 в одну сторону, лимит риска портфеля)
    CORR_MAX_SAME_DIRECTION:   int   = int(os.getenv("CORR_MAX_SAME_DIRECTION", "2"))
    CORR_MAX_PORTFOLIO_RISK:   float = float(os.getenv("CORR_MAX_PORTFOLIO_RISK", "4.5"))
    # Circuit breaker: 3 SL подряд → пауза N часов
    SL_STREAK_THRESHOLD:       int   = int(os.getenv("SL_STREAK_THRESHOLD", "3"))
    SL_STREAK_PAUSE_HOURS:     float = float(os.getenv("SL_STREAK_PAUSE_HOURS", "4.0"))

cfg = Config()

# ──────────────────────────────────────────────
# UTILS
# ──────────────────────────────────────────────
def floor_step(value: float, step: float) -> float:
    if step <= 0: return value
    return math.floor(value / step) * step

def round_step(value: float, step: float) -> float:
    if step <= 0: return value
    return round(value / step) * step

def step_decimals(step: float) -> int:
    s = f"{step:.10f}".rstrip("0")
    if "." not in s: return 0
    return len(s.split(".")[1])

# ──────────────────────────────────────────────
# BYBIT CLIENT
# ──────────────────────────────────────────────
class ExchangeClientWrapper:
    """Wrapper над ExchangeClient для обратной совместимости с остальным кодом бота.

    Все методы возвращают данные в Bybit-совместимом формате,
    чтобы InstrumentRegistry, MarketScanner, Reconciler и т.д. работали без изменений.
    """

    def __init__(self):
        self._client: Optional[ExchangeClient] = None
        # Прокси для совместимости
        self.rate_limiter = None
        self.circuit_breaker = None
        self.watchdog = None
        self.time_sync = None

    async def init(self):
        self._client = ExchangeClient(
            api_key=cfg.OKX_API_KEY,
            api_secret=cfg.OKX_API_SECRET,
            passphrase=cfg.OKX_PASSPHRASE,
            testnet=cfg.OKX_TESTNET
        )
        await self._client.init()
        # Прокси-атрибуты
        self.rate_limiter = self._client.rate_limiter
        self.circuit_breaker = self._client.circuit_breaker
        self.watchdog = self._client.watchdog
        self.time_sync = self._client.time_sync

    async def close(self):
        if self._client:
            await self._client.close()

    # Generic GET/POST для совместимости (например, position_mode.py их использует)
    async def get(self, ep: str, params: dict = None) -> dict:
        if not self._client: return {}
        return await self._client.get(ep, params)

    async def post(self, ep: str, body: dict = None) -> dict:
        if not self._client: return {}
        return await self._client.post(ep, body)

    # Market data
    async def get_klines(self, symbol: str, interval="15", limit=200) -> list:
        return await self._client.get_klines(symbol, interval, limit)

    async def get_tickers_all(self) -> list:
        return await self._client.get_tickers_all()

    async def get_ticker(self, symbol: str) -> dict:
        return await self._client.get_ticker(symbol)

    async def get_instruments_info_all(self) -> dict:
        return await self._client.get_instruments_info_all()

    # Account
    async def get_balance(self) -> float:
        return await self._client.get_balance()

    async def get_positions(self) -> list:
        return await self._client.get_positions()

    # Trading
    async def set_leverage(self, symbol: str, lev: int) -> bool:
        return await self._client.set_leverage(symbol, lev)

    async def place_order(self, symbol: str, side: str, qty_str: str,
                          sl: Optional[str] = None, tp: Optional[str] = None,
                          reduce_only: bool = False,
                          position_idx: int = 0) -> dict:
        return await self._client.place_order(symbol, side, qty_str,
                                                sl=sl, tp=tp,
                                                reduce_only=reduce_only,
                                                position_idx=position_idx)

    async def close_market(self, symbol: str, side: str, qty_str: str,
                           position_idx: int = 0) -> dict:
        return await self._client.close_market(symbol, side, qty_str,
                                                 position_idx=position_idx)

    async def update_sl_tp(self, symbol: str,
                            sl: Optional[float] = None,
                            tp: Optional[float] = None,
                            verify: bool = True) -> bool:
        """Делегат к OKXClient.update_sl_tp — закрывает баг SL drift.
        Без этой прокидки tighten_sl и trailing молча падают с AttributeError."""
        return await self._client.update_sl_tp(symbol, sl=sl, tp=tp, verify=verify)

    async def amend_algo_order(self, symbol: str, algo_id: str,
                                new_sl: Optional[float] = None,
                                new_tp: Optional[float] = None) -> dict:
        return await self._client.amend_algo_order(symbol, algo_id,
                                                     new_sl=new_sl, new_tp=new_tp)

# ──────────────────────────────────────────────
# INSTRUMENT REGISTRY
# ──────────────────────────────────────────────
class InstrumentRegistry:
    def __init__(self):
        self.data: dict = {}

    async def load(self, client: ExchangeClientWrapper):
        info_map = await client.get_instruments_info_all()
        for sym, item in info_map.items():
            try:
                # OKX возвращает уже в плоском формате
                self.data[sym] = {
                    "tick_size":    float(item.get("tick_size", 0.0001)),
                    "qty_step":     float(item.get("qty_step", 1)),         # шаг контрактов
                    "min_order_qty":float(item.get("min_order_qty", 1)),    # мин. контрактов
                    "max_leverage": float(item.get("max_leverage", 100)),
                    "ct_val":       float(item.get("ct_val", 1)),           # монет в контракте
                    "instId":       item.get("instId", ""),
                }
            except Exception as e:
                log.warning(f"Parse instrument {sym}: {e}")
        log.info(f"Загружено {len(self.data)} инструментов OKX")

    def get(self, symbol: str) -> dict:
        return self.data.get(symbol, {
            "tick_size":0.0001,"qty_step":1,
            "min_order_qty":1,"max_leverage":50,
            "ct_val":1,
        })

    def round_qty(self, symbol: str, qty: float) -> tuple:
        info = self.get(symbol)
        step = info["qty_step"]
        rounded = floor_step(qty, step)
        if rounded < info["min_order_qty"]:
            return None, 0.0
        decimals = step_decimals(step)
        return f"{rounded:.{decimals}f}", rounded

    def round_price(self, symbol: str, price: float) -> str:
        info = self.get(symbol)
        step = info["tick_size"]
        rounded = round_step(price, step)
        decimals = step_decimals(step)
        return f"{rounded:.{decimals}f}"

# ──────────────────────────────────────────────
# MARKET SCANNER
# ──────────────────────────────────────────────
class MarketScanner:
    def __init__(self, client: ExchangeClientWrapper, ws_stream: Optional[WSStream] = None):
        self.client = client
        self.ws     = ws_stream
        self.ranked: list = []
        self.last_scan_time: float = 0

    async def scan(self) -> list:
        if self.ws and self.ws.connected and not self.ws.is_stale:
            tickers = self.ws.get_all_tickers()
        else:
            tickers = await self.client.get_tickers_all()
        if not tickers: return self.ranked
        tmap = {t["symbol"]: t for t in tickers}
        ranked = []
        for sym in cfg.ACTIVE_SYMBOLS:
            t = tmap.get(sym)
            if not t: continue
            try:
                vol24 = float(t.get("turnover24h",0) or 0)
                chg24 = float(t.get("price24hPcnt",0) or 0) * 100
                price = float(t.get("lastPrice",0) or 0)
                hi24  = float(t.get("highPrice24h",price) or price or 1)
                lo24  = float(t.get("lowPrice24h",price)  or price or 1)
                if vol24 < cfg.MIN_VOLUME_USDT or price <= 0: continue
                atr_pct = (hi24-lo24)/max(lo24,1e-9)*100
                if atr_pct < cfg.MIN_ATR_PCT: continue
                vol_score  = min(40, vol24/max(cfg.MIN_VOLUME_USDT,1)*8)
                chg_score  = min(35, abs(chg24)*3.5)
                volt_score = min(25, atr_pct*2.5)
                score = vol_score+chg_score+volt_score
                ranked.append({
                    "symbol":sym,"score":round(score,1),
                    "vol24h":vol24,"chg24h":round(chg24,2),
                    "price":price,"atr_pct":round(atr_pct,2),
                    "tier":get_tier(sym),
                    "funding_rate": float(t.get("fundingRate",0) or 0)*100,
                })
            except Exception: continue
        ranked.sort(key=lambda x: x["score"], reverse=True)
        self.ranked = ranked
        self.last_scan_time = time.time()
        return ranked

# ──────────────────────────────────────────────
# INDICATORS
# ──────────────────────────────────────────────
class Ind:
    @staticmethod
    def ema(s: pd.Series, p: int) -> pd.Series:
        return s.ewm(span=p, adjust=False).mean()
    @staticmethod
    def rsi(s: pd.Series, p=14) -> float:
        d = s.diff()
        g = d.clip(lower=0).rolling(p).mean()
        l_ = (-d.clip(upper=0)).rolling(p).mean()
        if g.iloc[-1] == 0 and l_.iloc[-1] == 0: return 50.0
        rs = g / l_.replace(0,1e-9)
        v = (100 - 100/(1+rs)).iloc[-1]
        return float(v) if not (math.isnan(v) or math.isinf(v)) else 50.0
    @staticmethod
    def macd(s: pd.Series, f=12, sl=26, sg=9):
        m = Ind.ema(s,f) - Ind.ema(s,sl)
        sg_ = Ind.ema(m, sg)
        h = m - sg_
        def _safe(x):
            v = float(x); return v if not (math.isnan(v) or math.isinf(v)) else 0.0
        return _safe(m.iloc[-1]), _safe(sg_.iloc[-1]), _safe(h.iloc[-1])
    @staticmethod
    def bollinger(s: pd.Series, p=20, std=2):
        mid = s.rolling(p).mean(); sig = s.rolling(p).std().fillna(0)
        up = mid + std*sig; lo = mid - std*sig
        denom = (up-lo).replace(0,1e-9)
        pb = (s-lo)/denom; w = (up-lo)/mid.replace(0,1e-9)*100
        def _s(x, d=0):
            v = float(x); return v if not (math.isnan(v) or math.isinf(v)) else d
        return _s(up.iloc[-1]), _s(mid.iloc[-1]), _s(lo.iloc[-1]), _s(pb.iloc[-1],0.5), _s(w.iloc[-1])
    @staticmethod
    def atr(h,l,c,p=14):
        tr = pd.concat([h-l,(h-c.shift()).abs(),(l-c.shift()).abs()],axis=1).max(axis=1)
        v = tr.rolling(p).mean().iloc[-1]
        return float(v) if not (math.isnan(v) or math.isinf(v)) else 0.0
    @staticmethod
    def adx(h,l,c,p=14):
        pdm = h.diff().clip(lower=0).fillna(0)
        ndm = (-l.diff()).clip(lower=0).fillna(0)
        tr = pd.concat([h-l,(h-c.shift()).abs(),(l-c.shift()).abs()],axis=1).max(axis=1)
        atr_ = tr.rolling(p).mean().replace(0,1e-9)
        pdi = 100*pdm.rolling(p).mean()/atr_
        ndi = 100*ndm.rolling(p).mean()/atr_
        dx = 100*(pdi-ndi).abs()/(pdi+ndi+1e-9)
        v = dx.rolling(p).mean().iloc[-1]
        return float(v) if not (math.isnan(v) or math.isinf(v)) else 20.0
    @staticmethod
    def stoch_rsi(s: pd.Series, p=14, sk=3, sd=3):
        d = s.diff()
        g = d.clip(lower=0).rolling(p).mean()
        l_ = (-d.clip(upper=0)).rolling(p).mean().replace(0,1e-9)
        rs = g/l_
        rsi_s = (100-100/(1+rs)).fillna(50)
        mn = rsi_s.rolling(p).min(); mx = rsi_s.rolling(p).max()
        k = 100*(rsi_s-mn)/(mx-mn+1e-9)
        k = k.rolling(sk).mean()
        d_ = k.rolling(sd).mean()
        kv = float(k.iloc[-1]); dv = float(d_.iloc[-1])
        return (kv if not (math.isnan(kv) or math.isinf(kv)) else 50.0,
                dv if not (math.isnan(dv) or math.isinf(dv)) else 50.0)
    @staticmethod
    def cci(h,l,c,p=20):
        tp=(h+l+c)/3; ma=tp.rolling(p).mean()
        md=tp.rolling(p).apply(lambda x: np.mean(np.abs(x-x.mean())),raw=False)
        denom=(0.015*md).replace(0,1e-9)
        v=((tp-ma)/denom).iloc[-1]
        return float(v) if not (math.isnan(v) or math.isinf(v)) else 0.0
    @staticmethod
    def obv_slope(c,v):
        if len(c)<5: return 0.0
        dir_ = c.diff().apply(lambda x: 1 if x>0 else (-1 if x<0 else 0))
        obv = (dir_*v).cumsum()
        return float(obv.iloc[-1]-obv.iloc[-5])
    @staticmethod
    def vwap(h,l,c,v):
        tp=(h+l+c)/3; cv=v.cumsum().replace(0,1e-9)
        return float((tp*v).cumsum().iloc[-1]/cv.iloc[-1])
    @staticmethod
    def ichimoku(h,l):
        def _s(s, d=0):
            v=float(s.iloc[-1]) if len(s) else d
            return v if not (math.isnan(v) or math.isinf(v)) else d
        t = (h.rolling(9).max()+l.rolling(9).min())/2
        k = (h.rolling(26).max()+l.rolling(26).min())/2
        sa = ((t+k)/2).shift(26)
        sb = ((h.rolling(52).max()+l.rolling(52).min())/2).shift(26)
        return {"tenkan":_s(t),"kijun":_s(k),"sa":_s(sa),"sb":_s(sb)}
    @staticmethod
    def williams_r(h,l,c,p=14):
        hh=h.rolling(p).max(); ll=l.rolling(p).min()
        denom=(hh-ll).replace(0,1e-9)
        v=(-100*(hh-c)/denom).iloc[-1]
        return float(v) if not (math.isnan(v) or math.isinf(v)) else -50.0
    @staticmethod
    def mfi(h,l,c,v,p=14):
        tp=(h+l+c)/3; mf=tp*v
        pos=mf.where(tp>tp.shift(),0).rolling(p).sum()
        neg=mf.where(tp<tp.shift(),0).rolling(p).sum().replace(0,1e-9)
        val=(100-100/(1+pos/neg)).iloc[-1]
        return float(val) if not (math.isnan(val) or math.isinf(val)) else 50.0
    @staticmethod
    def supertrend(h,l,c,p=10,mult=3.0):
        atr_=Ind.atr(h,l,c,p)
        if atr_==0: return "neut"
        hl2=(h+l)/2; lower=hl2-mult*atr_
        return "bull" if float(c.iloc[-1])>float(lower.iloc[-1]) else "bear"
    @staticmethod
    def compute_all(raw: list) -> dict:
        if len(raw) < 60: return {}
        data = list(reversed(raw))
        df = pd.DataFrame(data, columns=["time","open","high","low","close","volume","turnover"])
        # Конвертируем числовые колонки (используем .loc чтобы избежать FutureWarning)
        numeric_cols = ["open","high","low","close","volume"]
        df.loc[:, numeric_cols] = df[numeric_cols].apply(
            lambda c: pd.to_numeric(c, errors="coerce")
        )
        df = df.dropna()
        if len(df) < 60: return {}
        h,l,c,v = df.high, df.low, df.close, df.volume
        rsi_v = Ind.rsi(c)
        mv,msig,mhist = Ind.macd(c)
        bup,bmid,blo,pb,bw = Ind.bollinger(c)
        atr_v = Ind.atr(h,l,c)
        adx_v = Ind.adx(h,l,c)
        cci_v = Ind.cci(h,l,c)
        sk,sd = Ind.stoch_rsi(c)
        obv_v = Ind.obv_slope(c,v)
        vwap_v= Ind.vwap(h,l,c,v)
        ichi  = Ind.ichimoku(h,l)
        wr_v  = Ind.williams_r(h,l,c)
        mfi_v = Ind.mfi(h,l,c,v)
        e20=float(Ind.ema(c,20).iloc[-1])
        e50=float(Ind.ema(c,50).iloc[-1])
        e200=float(Ind.ema(c,200).iloc[-1])
        st=Ind.supertrend(h,l,c)
        price=float(c.iloc[-1])
        avg_v=float(v.rolling(20).mean().iloc[-1] or 1)
        if avg_v<=0: avg_v=1
        vr=float(v.iloc[-1])/avg_v
        atr_pct=atr_v/price*100 if price>0 else 0
        candles=[
            {"t":int(r.time),"o":float(r.open),"h":float(r.high),
             "l":float(r.low),"c":float(r.close),"v":float(r.volume)}
            for _,r in df.tail(100).iterrows()
        ]
        return dict(price=price,rsi=rsi_v,
            macd=mv,macd_signal=msig,macd_hist=mhist,
            bb_upper=bup,bb_mid=bmid,bb_lower=blo,bb_pct=pb,bb_width=bw,
            atr=atr_v,atr_pct=atr_pct,adx=adx_v,cci=cci_v,
            stoch_k=sk,stoch_d=sd,obv_slope=obv_v,vwap=vwap_v,ichimoku=ichi,
            williams_r=wr_v,mfi=mfi_v,
            ema20=e20,ema50=e50,ema200=e200,
            supertrend=st,vol_ratio=vr,candles=candles)

# ──────────────────────────────────────────────
# SIGNAL ENGINE
# ──────────────────────────────────────────────
class Signal:
    @staticmethod
    def analyze(ind: dict, strategy: str = "trend") -> dict:
        if not ind:
            return {"signal":"wait","confidence":0,"bull":0,"bear":0,"total":0,"details":[]}
        # ── Volume Anomaly: блокировка входа (v1.1) ──
        # Аномальный всплеск (spike) часто = pump-and-dump → не лезем за движением.
        # Аномальный обвал (drop) = пересохшая ликвидность → ордера будут с проскальзыванием.
        vol_anomaly = ind.get("vol_anomaly", "normal")
        if vol_anomaly == "spike":
            return {"signal":"wait","confidence":0,"bull":0,"bear":0,"total":0,
                    "details":[{"name":f"Volume Spike z={ind.get('vol_z',0):.1f} BLOCKED",
                                "signal":"neut"}]}
        if vol_anomaly == "drop":
            return {"signal":"wait","confidence":0,"bull":0,"bear":0,"total":0,
                    "details":[{"name":f"Volume Drop z={ind.get('vol_z',0):.1f} BLOCKED",
                                "signal":"neut"}]}
        price = ind["price"]
        bull=bear=0; details=[]
        def add(name,d,w=1):
            nonlocal bull,bear
            if d=="bull": bull+=w; details.append({"name":name,"signal":"bull"})
            elif d=="bear": bear+=w; details.append({"name":name,"signal":"bear"})
            else: details.append({"name":name,"signal":"neut"})
        rsi=ind["rsi"]
        if   rsi<20: add("RSI Extreme OS","bull",3)
        elif rsi<30: add("RSI Oversold","bull",2)
        elif rsi<40: add("RSI Mildly OS","bull",1)
        elif rsi>80: add("RSI Extreme OB","bear",3)
        elif rsi>70: add("RSI Overbought","bear",2)
        elif rsi>60: add("RSI Mildly OB","bear",1)
        else: add("RSI Neutral","neut")
        mh=ind["macd_hist"]
        if   mh>0 and ind["macd"]>ind["macd_signal"]: add("MACD Bull X","bull",2)
        elif mh<0 and ind["macd"]<ind["macd_signal"]: add("MACD Bear X","bear",2)
        elif mh>0: add("MACD+","bull")
        elif mh<0: add("MACD-","bear")
        else: add("MACD Flat","neut")
        e20,e50,e200=ind["ema20"],ind["ema50"],ind["ema200"]
        if   e20>e50>e200: add("EMA Full Bull","bull",2)
        elif e20<e50<e200: add("EMA Full Bear","bear",2)
        elif e20>e50: add("EMA20>EMA50","bull")
        else: add("EMA20<EMA50","bear")
        adx=ind["adx"]
        if   adx>30: add("ADX Strong","bull" if e20>e50 else "bear",2)
        elif adx>20: add("ADX Mod","bull" if e20>e50 else "bear")
        else: add("ADX Weak","neut")
        if   price<ind["bb_lower"]: add("BB Below","bull",2)
        elif price>ind["bb_upper"]: add("BB Above","bear",2)
        elif ind["bb_pct"]<0.2: add("BB Low","bull")
        elif ind["bb_pct"]>0.8: add("BB High","bear")
        else: add("BB Mid","neut")
        sk,sd=ind["stoch_k"],ind["stoch_d"]
        if   sk<15 and sk>sd: add("StochRSI OS X","bull",2)
        elif sk>85 and sk<sd: add("StochRSI OB X","bear",2)
        elif sk<25: add("StochRSI Low","bull")
        elif sk>75: add("StochRSI High","bear")
        else: add("StochRSI Neut","neut")
        cci=ind["cci"]
        if   cci<-150: add("CCI Extreme OS","bull",2)
        elif cci<-100: add("CCI OS","bull")
        elif cci>150:  add("CCI Extreme OB","bear",2)
        elif cci>100:  add("CCI OB","bear")
        else: add("CCI Neut","neut")
        add("OBV","bull" if ind["obv_slope"]>0 else "bear")
        add("VWAP","bull" if price>ind["vwap"] else "bear")
        ichi=ind["ichimoku"]
        ctop=max(ichi["sa"],ichi["sb"]); cbot=min(ichi["sa"],ichi["sb"])
        if   price>ctop and ichi["tenkan"]>ichi["kijun"]: add("Ichi Bull","bull",2)
        elif price<cbot and ichi["tenkan"]<ichi["kijun"]: add("Ichi Bear","bear",2)
        elif ichi["sa"]>ichi["sb"]: add("Cloud Green","bull")
        else: add("Cloud Red","bear")
        wr=ind["williams_r"]
        if   wr<-85: add("Williams OS","bull")
        elif wr>-15: add("Williams OB","bear")
        else: add("Williams Neut","neut")
        mfi=ind["mfi"]
        if   mfi<20: add("MFI OS","bull")
        elif mfi>80: add("MFI OB","bear")
        else: add("MFI Neut","neut")
        st_dir=ind["supertrend"]
        if st_dir in ("bull","bear"): add("SuperTrend",st_dir)
        else: add("SuperTrend Flat","neut")
        vr=ind["vol_ratio"]
        if   vr>2.5: add("High Vol","bull" if bull>bear else "bear",2)
        elif vr>1.5: add("Above Vol","bull" if bull>bear else "bear")
        # ── Volume Divergence (17-й индикатор, v1.1) ──
        # Цена растёт, объём падает → bear (выдыхающееся ралли).
        # Цена падает, объём падает → bull (продавцы выдыхаются).
        vol_div = ind.get("vol_div", "neutral")
        if   vol_div == "bull_div": add("Vol Divergence Bull","bull",2)
        elif vol_div == "bear_div": add("Vol Divergence Bear","bear",2)
        if strategy=="scalp" and ind["bb_width"]>6: add("BB Too Wide","neut")
        elif strategy=="breakout" and vr>2.5 and adx>30:
            add("Breakout Confirm","bull" if bull>bear else "bear",3)
        elif strategy=="dca" and rsi<40: add("DCA Zone","bull",2)
        elif strategy=="trend" and st_dir==("bull" if e20>e50 else "bear"):
            add("Trend+ST","bull" if e20>e50 else "bear",2)
        elif strategy=="mean_reversion":
            # Mean reversion: ловим возврат к центру BB. Отскок от верхней — медвежий,
            # от нижней — бычий. Усиление при подтверждении CCI и Williams%R.
            if   price <= ind["bb_lower"] and cci < -100 and wr < -85:
                add("MR Bottom","bull",3)
            elif price >= ind["bb_upper"] and cci >  100 and wr > -15:
                add("MR Top","bear",3)
            elif ind["bb_pct"] < 0.25 and rsi < 40:
                add("MR Lower Zone","bull",1)
            elif ind["bb_pct"] > 0.75 and rsi > 60:
                add("MR Upper Zone","bear",1)
        total=bull+bear
        conf=round(max(bull,bear)/total*100) if total>0 else 0
        if conf>=cfg.SIGNAL_CONFIDENCE and total>=6:
            signal="buy" if bull>bear else "sell"
        else:
            signal="wait"
        return {"signal":signal,"confidence":conf,
                "bull":bull,"bear":bear,"total":total,"details":details}

# ──────────────────────────────────────────────
# MARKET REGIME CLASSIFIER
# ──────────────────────────────────────────────
class MarketRegime:
    """Классификатор режима рынка по индикаторам BTC + ETH.

    Идея: BTC и ETH — это «температура» крипты. Если они сильно трендят —
    весь рынок в тренде. Если они в боковике — лучше mean_reversion. И т.д.

    Возвращает один из 5 режимов и предлагает соответствующую стратегию.
    """
    # Маппинг: режим рынка → лучшая стратегия для него
    REGIME_TO_STRATEGY = {
        "strong_trend":  "trend",        # ADX>30, EMA выстроены — едем по тренду
        "trending":      "trend",        # ADX 20-30, EMA направление чёткое
        "volatile":      "breakout",     # широкие BB, высокий volume — пробои
        "ranging":       "scalp",        # узкие BB, ADX<20 — скальп от уровней
        "oversold":      "dca",          # экстремальная перепроданность — усреднение
        "mean_revert":   "mean_reversion",  # боковик с откатами от BB
    }

    @staticmethod
    def classify(btc_ind: dict, eth_ind: dict = None) -> dict:
        """Возвращает {regime, strategy, reason, confidence}."""
        if not btc_ind or "rsi" not in btc_ind:
            return {"regime": "unknown", "strategy": "trend",
                    "reason": "нет данных", "confidence": 0}
        # Соберём ключевые показатели BTC (главный сигнал)
        adx = btc_ind.get("adx", 20)
        rsi = btc_ind.get("rsi", 50)
        bb_w = btc_ind.get("bb_width", 3)
        e20 = btc_ind.get("ema20", 0)
        e50 = btc_ind.get("ema50", 0)
        e200 = btc_ind.get("ema200", 0)
        vol_ratio = btc_ind.get("vol_ratio", 1)
        # ETH как подтверждение (если есть)
        eth_adx  = eth_ind.get("adx", adx) if eth_ind else adx
        eth_rsi  = eth_ind.get("rsi", rsi) if eth_ind else rsi

        # 1. Экстремальная перепроданность по BTC + ETH → DCA
        if rsi < 25 and eth_rsi < 30:
            return {"regime": "oversold", "strategy": "dca",
                    "reason": f"BTC RSI {rsi:.0f}, ETH RSI {eth_rsi:.0f} — перепроданы",
                    "confidence": 85}

        # 2. Сильный тренд: ADX > 30, EMA выстроены, объём подтверждает
        avg_adx = (adx + eth_adx) / 2
        ema_aligned_bull = e20 > e50 > e200
        ema_aligned_bear = e20 < e50 < e200
        if avg_adx > 30 and (ema_aligned_bull or ema_aligned_bear):
            return {"regime": "strong_trend", "strategy": "trend",
                    "reason": f"ADX {avg_adx:.0f}, EMA выстроены"
                              f"{' ↑' if ema_aligned_bull else ' ↓'}",
                    "confidence": 90}

        # 3. Высокая волатильность — пробои
        # Широкие BB + высокий объём = вероятен пробой
        if bb_w > 5 and vol_ratio > 1.8:
            return {"regime": "volatile", "strategy": "breakout",
                    "reason": f"BB width {bb_w:.1f}%, vol×{vol_ratio:.1f}",
                    "confidence": 75}

        # 4. Умеренный тренд
        if avg_adx > 20 and (e20 > e50 or e20 < e50):
            return {"regime": "trending", "strategy": "trend",
                    "reason": f"ADX {avg_adx:.0f}, направление "
                              f"{'↑' if e20 > e50 else '↓'}",
                    "confidence": 70}

        # 5. Узкий боковик — лучше mean reversion если есть откаты
        if avg_adx < 20 and bb_w < 2.5:
            return {"regime": "mean_revert", "strategy": "mean_reversion",
                    "reason": f"ADX {avg_adx:.0f}, BB width {bb_w:.1f}% — узкий боковик",
                    "confidence": 70}

        # 6. Дефолт: широкий боковик — скальп
        return {"regime": "ranging", "strategy": "scalp",
                "reason": f"ADX {avg_adx:.0f}, BB width {bb_w:.1f}% — нейтрально",
                "confidence": 60}



# ──────────────────────────────────────────────
# STRATEGY SCORER — рейтинг 5 стратегий по индикаторам BTC
# ──────────────────────────────────────────────
class StrategyScorer:
    """Считает оценку 0..100 для каждой из 5 стратегий
    по индикаторам BTC (как proxy всего рынка).

    Идея: чем лучше текущая рыночная картина соответствует
    типу стратегии, тем выше скор. Каждая стратегия имеет
    свой профиль идеальных условий.
    """

    @staticmethod
    def _clamp(v: float, lo: float = 0, hi: float = 100) -> float:
        return max(lo, min(hi, v))

    @staticmethod
    def score_all(ind: dict) -> dict:
        """Возвращает {trend, scalp, breakout, dca, mean_reversion}: 0..100"""
        if not ind:
            return {"trend": 0, "scalp": 0, "breakout": 0,
                    "dca": 0, "mean_reversion": 0}

        rsi      = ind.get("rsi", 50)
        adx      = ind.get("adx", 20)
        bb_width = ind.get("bb_width", 4)
        bb_pct   = ind.get("bb_pct", 0.5)
        atr_pct  = ind.get("atr_pct", 1.0)
        vol_ratio = ind.get("vol_ratio", 1.0)
        e20      = ind.get("ema20", 0)
        e50      = ind.get("ema50", 0)
        e200     = ind.get("ema200", 0)
        macd_h   = ind.get("macd_hist", 0)
        stoch_k  = ind.get("stoch_k", 50)
        cci      = ind.get("cci", 0)
        wr       = ind.get("williams_r", -50)
        price    = ind.get("price", 0)

        # Тренд выстроен? (буст +1, ломанный -0.5)
        ema_aligned_up   = (e20 > e50 > e200) if e200 > 0 else (e20 > e50)
        ema_aligned_down = (e20 < e50 < e200) if e200 > 0 else (e20 < e50)
        ema_aligned      = ema_aligned_up or ema_aligned_down

        # ── 1. TREND FOLLOWING ──
        # Любит: высокий ADX (>25), EMA выстроены, MACD подтверждает,
        # умеренный объём, не экстремальный RSI
        s_trend = 0
        s_trend += StrategyScorer._clamp((adx - 15) * 2.0, 0, 50)          # ADX вклад до 50
        s_trend += 25 if ema_aligned else 0                                 # тренд выстроен
        s_trend += 15 if (macd_h > 0 and ema_aligned_up) or \
                          (macd_h < 0 and ema_aligned_down) else 0          # MACD согласен
        s_trend += 10 if 30 <= rsi <= 70 else 0                             # RSI не в экстремуме

        # ── 2. SCALPING ──
        # Любит: узкие BB (тихий рынок), низкий ADX, средний RSI,
        # цена близко к середине, не слишком волатильно
        s_scalp = 0
        s_scalp += StrategyScorer._clamp(50 - bb_width * 6, 0, 50)          # узкие BB
        s_scalp += StrategyScorer._clamp(40 - adx, 0, 25)                   # низкий ADX
        s_scalp += 15 if 40 <= rsi <= 60 else 5                              # RSI у центра
        s_scalp += 10 if atr_pct < 2.0 else 0                                # низкая волатильность

        # ── 3. BREAKOUT ──
        # Любит: всплеск объёма, расширяющиеся BB, ADX>25, цена у границы BB
        s_breakout = 0
        s_breakout += StrategyScorer._clamp((vol_ratio - 1.0) * 25, 0, 40)   # объёмный всплеск
        s_breakout += StrategyScorer._clamp((adx - 20) * 1.5, 0, 25)        # ADX начал расти
        s_breakout += StrategyScorer._clamp(bb_width * 3, 0, 20)             # BB расширены
        s_breakout += 15 if (bb_pct > 0.85 or bb_pct < 0.15) else 0          # у края канала

        # ── 4. DCA BOT ──
        # Любит: перепроданность (RSI<35), низкие позиции в BB, отрицательный CCI
        # Не любит сильный медвежий тренд
        s_dca = 0
        s_dca += StrategyScorer._clamp((40 - rsi) * 2.2, 0, 40)              # RSI низкий
        s_dca += StrategyScorer._clamp((0.4 - bb_pct) * 75, 0, 25)           # ниже середины BB
        s_dca += StrategyScorer._clamp(-cci * 0.2, 0, 20)                    # CCI отрицательный
        s_dca += 15 if stoch_k < 30 else (10 if stoch_k < 50 else 0)         # StochRSI низкий
        # штраф если медвежий тренд сильный — ловить падающие ножи опасно
        if ema_aligned_down and adx > 30:
            s_dca = max(0, s_dca - 25)

        # ── 5. MEAN REVERSION ──
        # Любит: цена у границ BB, экстремальные CCI/Williams%R,
        # низкий ADX (нет тренда), широкие BB
        s_mr = 0
        # Расстояние bb_pct от центра 0.5 — чем дальше, тем лучше
        dist_from_center = abs(bb_pct - 0.5)
        s_mr += StrategyScorer._clamp(dist_from_center * 100, 0, 30)         # цена у края
        s_mr += StrategyScorer._clamp((abs(cci) - 50) * 0.3, 0, 25)          # CCI экстремум
        s_mr += 15 if (wr < -80 or wr > -20) else 0                          # Williams экстремум
        s_mr += StrategyScorer._clamp((25 - adx) * 1.2, 0, 20)               # ADX слабый
        s_mr += StrategyScorer._clamp(bb_width * 1.5, 0, 10)                 # BB достаточно широкие

        return {
            "trend":          int(StrategyScorer._clamp(s_trend)),
            "scalp":          int(StrategyScorer._clamp(s_scalp)),
            "breakout":       int(StrategyScorer._clamp(s_breakout)),
            "dca":            int(StrategyScorer._clamp(s_dca)),
            "mean_reversion": int(StrategyScorer._clamp(s_mr)),
        }


class PositionManager:
    def __init__(self):
        self.positions: dict = {}
        self.trade_hist = deque(maxlen=2000)
        self.daily_pnl: float = 0.0
        self.total_pnl: float = 0.0
        self.wins: int = 0
        self.losses: int = 0
        self.equity_hist = deque(maxlen=2000)
        self.best_trade: float = 0.0
        self.worst_trade: float = 0.0
        self.cooldowns: dict = {}
        self.fees_total: float = 0.0
        self.funding_total: float = 0.0
        self._daily_reset_date = datetime.now(timezone.utc).date()
        self.trades_today: int = 0

    def reset_daily_if_needed(self):
        today = datetime.now(timezone.utc).date()
        if today != self._daily_reset_date:
            log.info(f"Сброс дневного PnL: {self._daily_reset_date} → {today}")
            self.daily_pnl = 0.0
            self.trades_today = 0
            self._daily_reset_date = today

    def add(self, symbol, side, entry, qty, size_usdt, sl, tp, lev, strat, oid):
        self.positions[symbol] = {
            "symbol":symbol,"side":side,"entry":entry,"qty":qty,
            "size_usdt":size_usdt,"sl":sl,"tp":tp,"leverage":lev,
            "strategy":strat,"order_id":oid,"tier":get_tier(symbol),
            "open_time":datetime.now(timezone.utc).isoformat(),
            "current_price":entry,"pnl":0.0,
            "liq_price":0.0, "liq_distance_pct":100.0,
        }

    def update_pnl(self, symbol, price):
        if symbol not in self.positions: return
        p = self.positions[symbol]
        if p["entry"]<=0: return
        diff = price-p["entry"] if p["side"]=="Buy" else p["entry"]-price
        p["pnl"] = (diff/p["entry"])*p["size_usdt"]
        p["current_price"] = price

    def record_close(self, symbol, gross_pnl, reason):
        p = self.positions.pop(symbol, {})
        notional = p.get("size_usdt", 0)
        fees = FeeCalculator.round_trip_fee(notional, both_taker=True)
        net_pnl = gross_pnl - fees
        self.fees_total += fees
        self.daily_pnl += net_pnl
        self.total_pnl += net_pnl
        self.trades_today += 1
        if net_pnl > 0:
            self.wins += 1
            self.best_trade = max(self.best_trade, net_pnl)
        else:
            self.losses += 1
            self.worst_trade = min(self.worst_trade, net_pnl)
        if reason == "STOP-LOSS":
            self.cooldowns[symbol] = time.time() + cfg.COOLDOWN_AFTER_SL*60
        self.trade_hist.append({
            "symbol":symbol,"side":p.get("side",""),"entry":p.get("entry",0),
            "pnl":round(net_pnl,4),"pnl_gross":round(gross_pnl,4),"fees":round(fees,4),
            "reason":reason,"strategy":p.get("strategy",""),"tier":p.get("tier",""),
            "time":datetime.now(timezone.utc).isoformat(),
        })
        return net_pnl

    def is_in_cooldown(self, symbol: str) -> bool:
        until = self.cooldowns.get(symbol, 0)
        if until and time.time() < until: return True
        if until and time.time() >= until: self.cooldowns.pop(symbol, None)
        return False

    @property
    def win_rate(self):
        t = self.wins+self.losses
        return self.wins/t*100 if t else 0.0

    def tier_count(self, tier: str) -> int:
        return sum(1 for p in self.positions.values() if p.get("tier")==tier)

    def restore_from_state(self, state: dict):
        positions = state.get("positions", {})
        if positions:
            self.positions = positions
            log.info(f"Восстановлено {len(positions)} позиций")
        history = state.get("trade_history", [])
        if history:
            self.trade_hist = deque(history, maxlen=2000)
        equity = state.get("equity_history", [])
        if equity: self.equity_hist = deque(equity, maxlen=2000)
        cd = state.get("cooldowns", {})
        if cd: self.cooldowns = cd
        m = state.get("metrics", {})
        if m:
            self.wins = m.get("wins", 0)
            self.losses = m.get("losses", 0)
            self.total_pnl = m.get("total_pnl", 0.0)
            self.daily_pnl = m.get("daily_pnl", 0.0)
            self.best_trade = m.get("best_trade", 0.0)
            self.worst_trade = m.get("worst_trade", 0.0)
            self.fees_total = m.get("fees_total", 0.0)
            self.funding_total = m.get("funding_total", 0.0)
            drd = m.get("daily_reset_date")
            if drd:
                try: self._daily_reset_date = datetime.fromisoformat(drd).date()
                except Exception: pass

    def to_state_dict(self, start_balance, peak_balance, max_dd):
        return {
            "positions_dict": dict(self.positions),
            "trade_history":  list(self.trade_hist),
            "equity_history": list(self.equity_hist),
            "cooldowns":      dict(self.cooldowns),
            "metrics": {
                "wins": self.wins, "losses": self.losses,
                "total_pnl": self.total_pnl, "daily_pnl": self.daily_pnl,
                "best_trade": self.best_trade, "worst_trade": self.worst_trade,
                "fees_total": self.fees_total, "funding_total": self.funding_total,
                "start_balance": start_balance, "peak_balance": peak_balance,
                "max_drawdown": max_dd,
                "last_save_time": datetime.now(timezone.utc).isoformat(),
                "daily_reset_date": self._daily_reset_date.isoformat(),
            }
        }

# ──────────────────────────────────────────────
# TRADING BOT
# ──────────────────────────────────────────────
class TradingBot:
    def __init__(self):
        self.client       = ExchangeClientWrapper()
        self.instruments  = InstrumentRegistry()
        self.pm           = PositionManager()
        self.scanner: MarketScanner = None
        self.ws_stream: Optional[WSStream] = None
        self.state_mgr    = StateManager(state_dir="state")
        self.fee_calc     = FeeCalculator
        self.funding_tracker = FundingTracker()
        self.reconciler:  Optional[Reconciler] = None
        self.position_mode = PositionModeManager(self.client)
        self.liq_monitor:  Optional[LiquidationMonitor] = None
        self.order_health: Optional[OrderHealthMonitor] = None
        self.telegram     = TelegramNotifier(
            bot_token=cfg.TELEGRAM_BOT_TOKEN,
            chat_id=cfg.TELEGRAM_CHAT_ID,
            enabled=cfg.TELEGRAM_ENABLED
        )
        # Partial TP manager (v1.0)
        self.partial_tp_config = PartialTPConfig.from_env_string(
            cfg.TP_LEVELS,
            move_sl_to_be_after_tp1=cfg.MOVE_SL_TO_BE_AFTER_TP1,
            trail_after_last_tp=cfg.TRAIL_AFTER_LAST_TP,
            trail_pct_after_all_tp=cfg.TRAIL_PCT_AFTER_ALL_TP,
        )
        # Force disable if ENABLE_PARTIAL_TP=false
        if not cfg.ENABLE_PARTIAL_TP:
            self.partial_tp_config.enabled = False
        self.partial_tp = PartialTPManager(self, self.partial_tp_config)
        # ── Volume Anomaly Modules (v1.1) ──
        self.vol_detector = VolumeAnomalyDetector(
            window=cfg.VOLUME_Z_WINDOW,
            z_threshold=cfg.VOLUME_Z_THRESHOLD,
        ) if cfg.VOLUME_ANOMALY_ENABLED else None
        self.vol_drop_guard = VolumeDropGuard(
            drop_factor=cfg.VOLUME_DROP_FACTOR,
            action=cfg.VOLUME_DROP_ACTION,
            cooldown_sec=300,
            min_bars_required=24,
        ) if cfg.VOLUME_DROP_GUARD_ENABLED else None
        self.vol_divergence = VolumeDivergenceIndicator(
            lookback=cfg.VOLUME_DIV_LOOKBACK,
            slope_threshold=0.005,
        ) if cfg.VOLUME_DIVERGENCE_ENABLED else None
        # ── Phase 1: Fib + Div strategy modules ────────────────────
        if cfg.USE_FIB_STRATEGY:
            self.trend_filter = MultiTFTrendFilter(
                self.client,
                slope_threshold=cfg.TREND_SLOPE_THRESHOLD,
                cache_ttl_sec=cfg.TREND_CACHE_TTL_S,
            )
            log.info(f"[Phase1] Fib+Div strategy ENABLED. trend slope thr={cfg.TREND_SLOPE_THRESHOLD}, "
                     f"fib tol={cfg.FIB_TOLERANCE_PCT}%, div lookback={cfg.DIV_LOOKBACK}")
        else:
            self.trend_filter = None
        # Circuit breaker для SL streak
        self._sl_streak = 0
        self._circuit_breaker_until: float = 0.0
        # Slippage помощник (для partial_tp)
        self.cfg_slippage_pct = cfg.SLIPPAGE_PCT
        # State
        self.running: bool = False
        self.paused:  bool = False
        self.balance: float = 0.0
        self.start_bal: float = 0.0
        self.peak_bal:  float = 0.0
        self.drawdown:  float = 0.0
        self.max_dd:    float = 0.0
        self.strategy   = cfg.STRATEGY
        self.logs       = deque(maxlen=500)
        self.market_data: dict = {}
        self.scan_results: list = []
        self.ws_clients: list = []
        # ── Multi-TF heatmap cache: {symbol: {"tf_4h": str, "tf_1h": str,
        #     "tf_15m": str, "regime": str, "ts": float}}. Заполняется в
        #     analyze_with_fib_strategy при каждом цикле.
        self.trend_heatmap: dict = {}
        # ── Rejection counters: {reason_short: int}. Скользящее окно — раз в
        #     5 минут все счётчики делятся пополам (грубый exponential decay).
        self.reject_counts: dict = {}
        self._reject_last_decay: float = time.time()
        self._loop_task:    Optional[asyncio.Task] = None
        self._persist_task: Optional[asyncio.Task] = None
        self._recon_task:   Optional[asyncio.Task] = None
        self._liq_task:     Optional[asyncio.Task] = None
        self._daily_task:   Optional[asyncio.Task] = None
        self._price_task:   Optional[asyncio.Task] = None
        self._tick = 0
        self._last_balance_refresh: float = 0
        self._ready = False
        # ── Авто-блокировка пар при определённых ошибках OKX
        # 51155 — compliance restriction (например JUP в некоторых юрисдикциях)
        # 51202 — market order amount exceeds maximum (testnet даёт малые лимиты)
        # 51000–51009 — параметры/режим аккаунта (тоже нет смысла повторять)
        self.blocked_symbols: set = set()
        # Память о причинах блокировки — чтобы один раз залогировать в человеческом виде
        self._block_reasons: dict = {}
        # Rate-limit открытий: храним timestamps последних открытий
        # для проверки MAX_TRADES_PER_HOUR
        self._recent_opens: list = []
        # ── Warm-up Phase
        # При старте бот не торгует пока не пройдёт WARMUP_SCANS полных циклов сканера.
        # Защищает от торговли по протухшим сигналам из state.json и от лавины ордеров
        # в первые секунды после рестарта.
        self._scans_done: int = 0
        self._warmup_started_at: float = 0  # время начала warm-up для отображения
        # ── Signal Persistence
        # Храним историю последних N сигналов по каждому символу.
        # Открываем позицию только если сигнал повторился SIGNAL_CONFIRM_TICKS раз подряд.
        self._signal_history: dict = {}  # {symbol: [list of recent signals]}
        # ── Авто-переключение стратегий
        # Если STRATEGY=auto, бот сам выбирает по режиму рынка.
        # Иначе режим = manual, стратегия зафиксирована.
        self.auto_strategy_mode: bool = (cfg.STRATEGY.lower() == "auto")
        self.market_regime: dict = {"regime": "unknown", "strategy": cfg.STRATEGY,
                                    "reason": "ожидание данных", "confidence": 0}
        self._last_regime_check: float = 0
        # Если включён auto — стартуем с trend (безопасный дефолт), пока не классифицируем рынок
        if self.auto_strategy_mode:
            self.strategy = "trend"

    def log(self, icon, msg, level="info"):
        entry = {"time":datetime.now().strftime("%H:%M:%S"),
                 "icon":icon,"msg":msg,"level":level}
        self.logs.appendleft(entry)
        log.info(f"{icon} {msg}")
        try:
            asyncio.get_running_loop()
            asyncio.create_task(self._broadcast({"type":"log","data":entry}))
        except RuntimeError: pass

    async def _broadcast(self, data):
        dead = []
        for ws in list(self.ws_clients):
            try: await ws.send_json(data)
            except Exception: dead.append(ws)
        for ws in dead:
            if ws in self.ws_clients: self.ws_clients.remove(ws)

    async def alert_critical(self, message: str, severity: str = "critical"):
        """Отправить алерт в Telegram (если настроен)."""
        if self.telegram.enabled:
            await self.telegram.send(message, severity=severity)

    def _maybe_reassess_strategy(self):
        """Если включён auto-режим, периодически пересматриваем стратегию
        на основе классификатора режима рынка по BTC+ETH.

        Вызывается из run_loop. Не делает ничего в manual режиме.
        Защита от слишком частого переключения: интервал AUTO_STRATEGY_REASSESS_S.
        """
        if not self.auto_strategy_mode:
            return
        # Не переключаем стратегию если уже есть открытые позиции —
        # это может ухудшить менеджмент рисков. Дождёмся пока их закроют.
        if self.pm.positions:
            return
        now = time.time()
        if now - self._last_regime_check < cfg.AUTO_STRATEGY_REASSESS_S:
            return
        self._last_regime_check = now
        # Берём данные BTC и ETH
        btc = self.market_data.get("BTCUSDT", {}).get("indicators", {})
        eth = self.market_data.get("ETHUSDT", {}).get("indicators", {})
        if not btc:
            return  # ещё нет данных
        regime = MarketRegime.classify(btc, eth or None)
        old_strategy = self.strategy
        self.market_regime = regime
        new_strategy = regime["strategy"]
        if new_strategy != old_strategy:
            self.strategy = new_strategy
            self.log("🔀", f"Auto-strategy: {old_strategy} → {new_strategy} "
                           f"({regime['regime']}, conf {regime['confidence']}%) "
                           f"— {regime['reason']}", "info")
        else:
            # Молча обновляем reason/confidence в state
            log.debug(f"Regime unchanged: {regime['regime']} ({regime['reason']})")

    # ── Init ──────────────────────────────────
    async def initialize(self):
        await self.client.init()
        await self.telegram.init()
        self.log("🌐", "Подключение к Bybit API...", "info")

        # Time sync (критично!)
        sync_ok = await self.client.time_sync.sync()
        if not sync_ok:
            self.log("⚠", "Не удалось синхронизировать время с Bybit", "warn")
        elif self.client.time_sync.is_critical_skew:
            self.log("⚠", f"Большой clock skew: {self.client.time_sync.offset_ms}ms", "warn")

        # Инструменты
        try:
            await self.instruments.load(self.client)
            self.log("📋", f"Инструментов: {len(self.instruments.data)}", "info")
            # ── Фильтруем ACTIVE_SYMBOLS: оставляем только те, что реально есть на бирже.
            # На testnet OKX некоторых пар (POL, UNI, APT, SEI, TIA, WIF, PYTH и др.) нет —
            # без фильтра WebSocket валит подписку с ошибкой 60018 для каждой.
            available = set(self.instruments.data.keys())
            original  = list(cfg.ACTIVE_SYMBOLS)
            cfg.ACTIVE_SYMBOLS = [s for s in original if s in available]
            missing = [s for s in original if s not in available]
            if missing:
                self.log("ℹ", f"Не на бирже ({len(missing)}): "
                              f"{', '.join(s.replace('USDT','') for s in missing[:8])}"
                              f"{'…' if len(missing) > 8 else ''}",
                         "info")
        except Exception as e:
            self.log("⚠", f"Instruments: {e}", "warn")

        # Position mode (one-way / hedge)
        if cfg.API_KEY and cfg.REQUIRE_ONE_WAY_MODE:
            ok, msg = await self.position_mode.ensure_one_way(
                auto_switch=cfg.AUTO_SWITCH_POSITION_MODE
            )
            if ok:
                self.log("✅", f"Position mode: {msg}", "info")
            else:
                self.log("🛑", f"Position mode: {msg}", "risk")
                self.log("🛑", "БОТ НЕ ЗАПУЩЕН — переключите режим вручную", "risk")
                await self.alert_critical(f"Position mode error: {msg}")
                return  # не продолжаем инициализацию

        # Баланс
        if cfg.API_KEY and cfg.API_SECRET:
            self.balance = await self.client.get_balance()
            if self.balance > 0:
                self.log("✅", f"Баланс: ${self.balance:.2f} USDT", "info")
            else:
                self.log("⚠", "Баланс=0. Проверьте API ключи", "warn")
                self.balance = 1000.0
        else:
            self.balance = 1000.0
            self.log("⚠", "API ключи не заданы → Demo $1000", "warn")

        self.start_bal = self.balance
        self.peak_bal  = self.balance
        self.pm.equity_hist.append(self.balance)
        self._last_balance_refresh = time.time()

        # WebSocket
        if cfg.USE_WEBSOCKET and HAS_WS:
            self.ws_stream = WSStream(testnet=cfg.TESTNET)
            self.scanner = MarketScanner(self.client, self.ws_stream)
            connected = await self.ws_stream.connect()
            if connected:
                await self.ws_stream.subscribe(cfg.ACTIVE_SYMBOLS)
                asyncio.create_task(self.ws_stream.listen())
                self.log("📡", f"WS подключён, подписан {len(cfg.ACTIVE_SYMBOLS)} тикеров", "info")
            else:
                self.log("⚠", "WS не подключился → REST", "warn")
        else:
            self.scanner = MarketScanner(self.client, None)

        # Reconciler / Liquidation / Order Health
        self.reconciler  = Reconciler(self)
        self.liq_monitor = LiquidationMonitor(
            self,
            critical_distance_pct=cfg.LIQ_CRITICAL_PCT,
            emergency_distance_pct=cfg.LIQ_EMERGENCY_PCT,
        )
        self.order_health = OrderHealthMonitor(
            self,
            error_threshold=cfg.ORDER_HEALTH_THRESHOLD,
            window_seconds=cfg.ORDER_HEALTH_WINDOW_S,
            pause_seconds=cfg.ORDER_HEALTH_PAUSE_S,
        )

        # Auto-restore
        if cfg.AUTO_RESTORE and self.state_mgr.has_saved_state():
            try:
                state = self.state_mgr.load_full_snapshot()
                self.pm.restore_from_state(state)
                m = state.get("metrics", {})
                if m.get("start_balance", 0) > 0:
                    self.start_bal = m["start_balance"]
                    self.peak_bal  = m.get("peak_balance", self.balance)
                    self.max_dd    = m.get("max_drawdown", 0.0)
                self.log("💾", f"Restore: {len(self.pm.positions)} поз, "
                                f"{self.pm.wins+self.pm.losses} сделок", "info")
            except Exception as e:
                self.log("⚠", f"Restore: {e}", "warn")
            try:
                rec = await self.reconciler.reconcile()
                if rec["fixed"]:
                    self.log("🔄", f"Reconcile: {len(rec['fixed'])} fixed", "info")
            except Exception as e:
                self.log("⚠", f"Reconcile: {e}", "warn")

            # ── Re-anchor start_bal если нет позиций и баланс сильно отличается.
            # Защита от случая: пользователь докинул testnet USDT через faucet,
            # позиций после reconcile не осталось, но в state_store сохранён
            # старый start_balance — отображение PnL/ROI становится бессмысленным.
            if not self.pm.positions and self.balance > 0 and self.start_bal > 0:
                drift_pct = abs(self.balance - self.start_bal) / self.start_bal * 100
                if drift_pct > 20:
                    self.log("🔁", f"Re-anchor baseline: ${self.start_bal:.2f} → ${self.balance:.2f} "
                                   f"(дрифт {drift_pct:.0f}%, нет открытых позиций)", "info")
                    self.start_bal = self.balance
                    self.peak_bal  = self.balance
                    self.max_dd    = 0.0

        # Logs
        be_pct = self.fee_calc.break_even_pct(cfg.LEVERAGE)
        self.log("💰", f"Fees: {self.fee_calc.TAKER_FEE*100:.3f}% | "
                       f"Break-even {cfg.LEVERAGE}× = {be_pct:.2f}%", "info")
        self.log("🛡", f"Risk {cfg.RISK_PER_TRADE}% | SL {cfg.SL_PCT}% | TP {cfg.TP_PCT}% | "
                       f"Lev {cfg.LEVERAGE}× | CD {cfg.COOLDOWN_AFTER_SL}m", "info")
        self.log("🚨", f"Liq alert {cfg.LIQ_CRITICAL_PCT}% | Emergency {cfg.LIQ_EMERGENCY_PCT}%", "info")
        if self.telegram.enabled:
            self.log("📱", "Telegram alerts: enabled", "info")
        self.log("🤖", f"PHANTOM v1.0 ready | "
                       f"{'TESTNET' if cfg.TESTNET else 'MAINNET'}", "info")
        self._ready = True

        # Уведомление о запуске
        await self.telegram.notify_bot_started(
            balance=self.balance,
            mode="TESTNET" if cfg.TESTNET else "MAINNET"
        )

        # Фоновые задачи
        self._persist_task = asyncio.create_task(self._persistence_loop())
        self._recon_task   = asyncio.create_task(self._reconciliation_loop())
        self._liq_task     = asyncio.create_task(self._liquidation_loop())
        self._daily_task   = asyncio.create_task(self._daily_summary_loop())
        self._price_task   = asyncio.create_task(self._price_tick_loop())

    async def _price_tick_loop(self):
        """Раз в 3 секунды шлём дашборду живые цены из WS-стрима для пар чарт-табов
        и для всех открытых позиций. Это дешёвая отдельная рассылка — нужна чтобы
        цены и PnL обновлялись плавно, не дожидаясь следующего скана (30 сек)."""
        chart_syms = {"BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","XRPUSDT"}
        while True:
            try:
                await asyncio.sleep(3)
                if not self.ws_stream or not self.ws_clients:
                    continue
                interesting = (chart_syms & set(cfg.ACTIVE_SYMBOLS)) | set(self.pm.positions.keys())
                prices = {}
                for sym in interesting:
                    t = self.ws_stream.tickers.get(sym, {})
                    px = t.get("lastPrice") or t.get("price") or t.get("last")
                    if px:
                        try: prices[sym] = float(px)
                        except (TypeError, ValueError): pass
                if not prices:
                    continue
                # Заодно пересчитаем PnL открытых позиций по живым ценам
                pos_pnl = {}
                for sym, p in self.pm.positions.items():
                    px = prices.get(sym)
                    if not px: continue
                    self.pm.update_pnl(sym, px)
                    pos_pnl[sym] = round(p.get("pnl", 0.0), 2)
                await self._broadcast({
                    "type": "price_tick",
                    "data": {"prices": prices, "position_pnl": pos_pnl,
                             "ts": int(time.time()*1000)}
                })
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.debug(f"price_tick_loop: {e}")

    async def shutdown(self):
        try: await self._persist_now()
        except Exception as e: log.warning(f"Final persist: {e}")
        for task in (self._loop_task, self._persist_task, self._recon_task,
                     self._liq_task, self._daily_task, self._price_task):
            if task and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
        if self.ws_stream: await self.ws_stream.stop()
        await self.telegram.close()
        await self.client.close()

    # ── Persistence loop ──────────────────────
    async def _persist_now(self):
        snap = self.pm.to_state_dict(self.start_bal, self.peak_bal, self.max_dd)
        self.state_mgr.save_full_snapshot(snap)

    async def _persistence_loop(self):
        try:
            while True:
                await asyncio.sleep(cfg.PERSIST_INTERVAL_S)
                try: await self._persist_now()
                except Exception as e: log.warning(f"Persist: {e}")
        except asyncio.CancelledError: pass

    async def _reconciliation_loop(self):
        try:
            while True:
                await asyncio.sleep(cfg.RECONCILE_INTERVAL_S)
                if self.running and not self.paused and self.reconciler:
                    try:
                        rec = await self.reconciler.reconcile(log_silent=True)
                        if rec["fixed"]:
                            self.log("🔄", f"Recon: {len(rec['fixed'])}", "warn")
                    except Exception as e: log.warning(f"Recon: {e}")
        except asyncio.CancelledError: pass

    async def _liquidation_loop(self):
        """Периодическая проверка дистанции до ликвидации и SL/TP drift."""
        try:
            while True:
                await asyncio.sleep(cfg.LIQ_CHECK_INTERVAL_S)
                if not self.running or self.paused or not self.liq_monitor: continue
                if not self.pm.positions: continue
                try:
                    res = await self.liq_monitor.check_all()
                    # Emergency closes
                    for sym in res.get("emergency_closes", []):
                        self.log("🚨", f"{sym}: EMERGENCY CLOSE — близко к ликвидации", "risk")
                        await self.close_position(sym, "LIQ-EMERGENCY")
                        await self.alert_critical(
                            f"⚠️ Emergency close {sym}: ликвидация близко!"
                        )
                    # Alerts
                    for alert in res.get("alerts", []):
                        await self.telegram.send(
                            f"⚠️ {alert['symbol']}: {alert['distance_pct']}% до ликвидации "
                            f"(liq=${alert['liq_price']:.6g})",
                            severity="warning"
                        )
                    # SL/TP drift — попробуем восстановить
                    if cfg.AUTO_RESTORE_SL_TP:
                        for drift in res.get("sl_tp_drift", []):
                            sym = drift["symbol"]
                            if any("отсутствует" in i for i in drift["issues"]):
                                ok = await self.liq_monitor.restore_missing_sl_tp(sym)
                                if ok:
                                    self.log("🔧", f"SL/TP восстановлены: {sym}", "info")
                except Exception as e:
                    log.warning(f"Liquidation loop: {e}")
        except asyncio.CancelledError: pass

    async def _daily_summary_loop(self):
        """Ежедневная сводка в Telegram."""
        try:
            while True:
                # Спим до следующего часа summary (UTC)
                now = datetime.now(timezone.utc)
                target = now.replace(hour=cfg.DAILY_SUMMARY_HOUR_UTC,
                                       minute=0, second=0, microsecond=0)
                if target <= now:
                    target = target.replace(day=target.day) + \
                             pd.Timedelta(days=1).to_pytimedelta()
                wait_seconds = (target - now).total_seconds()
                await asyncio.sleep(min(wait_seconds, 3600))
                # Проверим, дошло ли время
                if datetime.now(timezone.utc).hour == cfg.DAILY_SUMMARY_HOUR_UTC:
                    if self.telegram.enabled:
                        stats = {
                            "trades_today": self.pm.trades_today,
                            "win_rate":     self.pm.win_rate,
                            "daily_pnl":    self.pm.daily_pnl,
                            "total_pnl":    self.pm.total_pnl,
                            "balance":      self.balance,
                            "roi_pct":      (self.balance-self.start_bal)/self.start_bal*100 if self.start_bal>0 else 0,
                            "drawdown":     self.drawdown,
                            "positions":    len(self.pm.positions),
                        }
                        await self.telegram.notify_daily_summary(stats)
                    await asyncio.sleep(3600)  # не повторять в этом часу
        except asyncio.CancelledError: pass

    # ── Analyze / open / close ────────────────
    async def analyze_symbol(self, symbol: str) -> dict:
        try:
            klines = await self.client.get_klines(symbol, interval="15", limit=200)
            if not klines: return {}
            ind = Ind.compute_all(klines)
            if not ind: return {}

            # ── Volume Anomaly Modules (v1.1) ──
            # Эти модули требуют raw Series close/volume, поэтому строим маленький DF
            # отдельно (компромисс: дублируем парсинг, зато compute_all остаётся чистым)
            need_vol_modules = (
                self.vol_detector is not None
                or self.vol_divergence is not None
                or (self.vol_drop_guard is not None and symbol in self.pm.positions)
            )
            ind.setdefault("vol_anomaly", "normal")
            ind.setdefault("vol_z", 0.0)
            ind.setdefault("vol_div", "neutral")
            if need_vol_modules:
                try:
                    data = list(reversed(klines))
                    df = pd.DataFrame(data, columns=[
                        "time","open","high","low","close","volume","turnover"])
                    df.loc[:, ["close","volume"]] = df[["close","volume"]].apply(
                        lambda c: pd.to_numeric(c, errors="coerce"))
                    df = df.dropna()
                    close_s = df["close"]
                    vol_s   = df["volume"]
                    # 1) Z-score детектор
                    if self.vol_detector is not None:
                        va = self.vol_detector.check(vol_s)
                        ind["vol_anomaly"] = va["anomaly"]
                        ind["vol_z"]       = va["z_score"]
                    # 2) Divergence (17-й индикатор)
                    if self.vol_divergence is not None:
                        vd = self.vol_divergence.check(close_s, vol_s)
                        ind["vol_div"]         = vd["signal"]
                        ind["vol_price_slope"] = vd["price_slope"]
                        ind["vol_vol_slope"]   = vd["vol_slope"]
                    # 3) Drop guard — только для символов с открытой позицией
                    if self.vol_drop_guard is not None and symbol in self.pm.positions:
                        last_vol = float(vol_s.iloc[-1]) if len(vol_s) else 0.0
                        self.vol_drop_guard.update_volume(symbol, last_vol)
                        action = self.vol_drop_guard.evaluate(symbol)
                        if action:
                            ind["vol_drop_action"] = action
                except Exception as e:
                    log.warning(f"volume modules {symbol}: {e}")

            sig = Signal.analyze(ind, self.strategy)
            return {"symbol":symbol,"indicators":ind,"signal":sig}
        except Exception as e:
            log.warning(f"analyze {symbol}: {e}")
            return {}

    # ── Phase 1: новая стратегия Fib + Div ────────────────────────────
    # Включается через cfg.USE_FIB_STRATEGY=true. Если выключено — этот
    # метод не вызывается, работает старая analyze_symbol.
    async def analyze_with_fib_strategy(self, symbol: str) -> dict:
        """Анализ символа по стратегии Phase 1: trend filter → Fib → divergence.

        ВАЖНО: индикаторы и свечи 15m считаются ВСЕГДА (для отображения на
        дашборде), независимо от того, разрешает ли стратегия вход. На
        rejection возвращается стаб с signal="wait", но full indicators.
        Только при катастрофическом сбое (нет данных) возвращается {}.
        """
        # ── Шаг 0: baseline с полным набором индикаторов (15m) ──
        # Используем analyze_symbol чтобы получить полную compute_all + volume
        # modules; стратегия из analyze_symbol игнорируется — её сигнал
        # перетрётся стаб-сигналом и затем (опционально) Phase 1.
        base = await self.analyze_symbol(symbol)
        if not base:
            return {}
        # Перетираем сигнал на стаб — сейчас решит Phase 1 pipeline
        base["signal"] = {"signal":"wait", "confidence":0,
                          "bull":0, "bear":0, "total":0, "details":[]}

        try:
            # ── Ветка для мемов: отдельная логика, без trend/fib/div ──
            is_meme = cfg.MEME_STRATEGY_ENABLED and is_meme_symbol(symbol)
            if is_meme:
                # Для мемов используем фиксированный 5m TF — там объёмные спайки
                # лучше видны
                klines_meme = await self.client.get_klines(symbol, interval="5", limit=100)
                if not klines_meme:
                    return base
                meme_setup = check_meme_setup(klines_meme)
                if meme_setup is None:
                    return base
                # Лог сигнала ПЕРЕД формированием решения
                log.info(
                    f"[SIGNAL] {symbol} MEME {meme_setup.side}: "
                    f"RSI={meme_setup.rsi:.1f} vol_ratio={meme_setup.volume_ratio:.1f}x "
                    f"close={meme_setup.last_close} open={meme_setup.last_open}"
                )
                # Унифицируем формат с Signal.analyze: signal=buy/sell в lowercase
                signal_str = "buy" if meme_setup.side == "Buy" else "sell"
                sig = {
                    "signal": signal_str,
                    "confidence": 75.0,   # фиксированная для мемов
                    "bull": 1 if signal_str == "buy" else 0,
                    "bear": 1 if signal_str == "sell" else 0,
                    "total": 1,
                    "details": [{"name": "meme", "signal": "bull" if signal_str == "buy" else "bear"}],
                    "reasons": [f"meme: RSI={meme_setup.rsi:.0f}, vol×{meme_setup.volume_ratio:.1f}"],
                }
                # Мерджим: оставляем все индикаторы из base + meme metadata
                base["indicators"]["_meme_setup"] = meme_setup
                base["indicators"]["strategy"] = "meme"
                base["signal"] = sig
                return base

            # ── Обычная ветка: trend → adaptive TF → fib → divergence ──

            # 1. Trend filter (multi-TF)
            trend = await self.trend_filter.check(symbol)

            # ── Заполняем кэш Multi-TF Heatmap (для дашборда) ──
            try:
                tf4 = trend.get("details", {}).get("tf_4h", {})
                tf1 = trend.get("details", {}).get("tf_1h", {})
                # Грубое 15m направление по EMA20/EMA50 из base.indicators
                bi = base.get("indicators", {})
                e20 = bi.get("ema20"); e50 = bi.get("ema50")
                if e20 and e50:
                    tf15 = "up" if e20 > e50 * 1.001 else "down" if e20 < e50 * 0.999 else "flat"
                else:
                    tf15 = "unknown"
                self.trend_heatmap[symbol] = {
                    "tf_4h": tf4.get("direction", "unknown"),
                    "tf_1h": tf1.get("direction", "unknown"),
                    "tf_15m": tf15,
                    "regime": trend.get("regime", "unknown"),
                    "long_allowed":  bool(trend.get("long_allowed")),
                    "short_allowed": bool(trend.get("short_allowed")),
                    "ts": time.time(),
                }
            except Exception:
                pass

            if not (trend["long_allowed"] or trend["short_allowed"]):
                # Тренд против или unknown — не входим
                # (MultiTFTrendFilter уже пишет в лог конкретную причину,
                # здесь молчим чтобы не дублировать)
                self._bump_reject("trend_blocked")
                return base
            allowed_side = "Buy" if trend["long_allowed"] else "Sell"

            # 2. Adaptive TF выбор. Берём 1h klines (они уже подгружаются в кеш
            # trend_filter, но мы их не сохраняем — повторяем запрос с малым limit).
            if cfg.USE_ADAPTIVE_TF:
                klines_1h = await self.client.get_klines(symbol, interval="1H", limit=50)
                tf_info = pick_adaptive_tf(klines_1h)
                entry_tf = tf_info["tf"]
            else:
                tf_info = {"tf": cfg.FALLBACK_TF, "regime": "manual"}
                entry_tf = cfg.FALLBACK_TF

            # OKX интервалы: 1m, 5m, 15m, 1h → нужно конвертировать в API формат
            interval_map = {"1m": "1m", "5m": "5m", "15m": "15m", "1h": "1H"}
            api_interval = interval_map.get(entry_tf, "15m")

            # 3. Klines на entry TF
            klines_entry = await self.client.get_klines(symbol, interval=api_interval, limit=200)
            if not klines_entry or len(klines_entry) < 80:
                log.info(f"[REJECT] {symbol} {allowed_side}: мало баров на entry TF "
                          f"({len(klines_entry) if klines_entry else 0}/80, tf={entry_tf})")
                self._bump_reject("no_bars")
                return base

            # 4. Fib setup
            fib = detect_fib_setup(
                klines_entry,
                tolerance_pct=cfg.FIB_TOLERANCE_PCT,
                min_atr_mult=cfg.FIB_SWING_ATR_MULT,
            )
            if fib is None:
                log.info(f"[REJECT] {symbol} {allowed_side}: fib swing не найден "
                          f"(tf={entry_tf}, bars={len(klines_entry)})")
                self._bump_reject("no_fib_swing")
                return base
            if fib.setup_type is None:
                # Свинг есть, но цена не на 0.5/0.618. Логируем расстояние —
                # увидим насколько близко мы были.
                log.info(f"[REJECT] {symbol} {allowed_side}: fib свинг есть, но цена "
                          f"не на entry уровне (до ближайшего {fib.distance_to_nearest:.2f}%, "
                          f"tol={cfg.FIB_TOLERANCE_PCT}%)")
                self._bump_reject("fib_far_from_level")
                return base

            # 5. Проверка направления: trend и fib должны совпадать
            fib_side = "Buy" if fib.setup_type == "long_retrace" else "Sell"
            if fib_side != allowed_side:
                log.info(f"[REJECT] {symbol}: конфликт trend({allowed_side}) vs "
                          f"fib({fib_side})")
                self._bump_reject("trend_fib_conflict")
                return base

            # 6. Divergence confirmation
            div = check_divergences(klines_entry, lookback=cfg.DIV_LOOKBACK)
            expected_dir = "long" if allowed_side == "Buy" else "short"
            if div["confirmed"] != expected_dir:
                # И-логика не сработала — самая частая причина отказа в норме
                log.info(f"[REJECT] {symbol} {allowed_side}: divergence не подтверждена "
                          f"(rsi={div['rsi_div']}, macd={div['macd_div']}, "
                          f"нужно оба = {'bullish' if expected_dir=='long' else 'bearish'})")
                self._bump_reject("divergence_unconfirmed")
                return base

            # ── Все три фильтра согласны: входим ──
            # Подробный лог сигнала (это и есть тот лог что Сергей просил
            # — чтобы видеть ПОЧЕМУ открыли позицию).
            tf_4h = trend["details"]["tf_4h"]
            tf_1h = trend["details"]["tf_1h"]
            log.info(
                f"[SIGNAL] {symbol} {allowed_side}: "
                f"trend(4h={tf_4h.get('direction','?')} slope={tf_4h.get('slope',0):.4f}, "
                f"1h={tf_1h.get('direction','?')} slope={tf_1h.get('slope',0):.4f}) | "
                f"fib({fib.on_level} of swing {fib.swing_low:.6g}→{fib.swing_high:.6g}) | "
                f"div(rsi={div['rsi_div']}, macd={div['macd_div']}) | "
                f"TF={entry_tf} regime={tf_info.get('regime','?')} "
                f"ATR%={tf_info.get('atr_pct',0):.2f}"
            )

            # Phase 1 одобрила вход: мерджим метаданные стратегии в base.indicators
            # (НЕ заменяем base.indicators — там лежит compute_all с полным
            #  набором RSI/MACD/EMA/BB/etc., нужным для отображения и
            #  partial_tp/risk-кода).
            base["indicators"]["strategy"] = "fib_div"
            base["indicators"]["tf_used"]  = entry_tf
            base["indicators"]["atr_pct"]  = tf_info.get("atr_pct", base["indicators"].get("atr_pct", 0.0))
            base["indicators"]["_fib_setup"] = fib
            # Унифицируем формат с Signal.analyze: signal=buy/sell в lowercase
            signal_str = "buy" if allowed_side == "Buy" else "sell"
            base["signal"] = {
                "signal": signal_str,
                "confidence": 80.0,   # фиксированная высокая — три фильтра согласны
                "bull": 3 if signal_str == "buy" else 0,
                "bear": 3 if signal_str == "sell" else 0,
                "total": 3,
                "details": [
                    {"name": "trend",     "signal": "bull" if signal_str=="buy" else "bear"},
                    {"name": "fib",       "signal": "bull" if signal_str=="buy" else "bear"},
                    {"name": "div",       "signal": "bull" if signal_str=="buy" else "bear"},
                ],
                "reasons": [
                    f"trend 4h+1h {trend['regime']}",
                    f"fib {fib.on_level} retrace",
                    f"div rsi+macd {div['confirmed']}",
                ],
            }
            return base

        except Exception as e:
            log.warning(f"analyze_with_fib_strategy {symbol}: {e}")
            return base  # сохраняем индикаторы даже при сбое pipeline

    def calc_size_qty(self, symbol: str, price: float) -> tuple:
        mult = TIER_MULT[get_tier(symbol)]
        risk_amt = self.balance * (cfg.RISK_PER_TRADE/100) * mult
        sl_dec = cfg.SL_PCT/100
        if sl_dec<=0 or price<=0: return None,0,0,0,0

        # Расчёт размера в МОНЕТАХ
        qty_coins_raw = risk_amt / (price * sl_dec)
        max_qty_coins = (self.balance * cfg.LEVERAGE * 0.95) / price
        qty_coins_raw = min(qty_coins_raw, max_qty_coins)

        # КОНВЕРТАЦИЯ В КОНТРАКТЫ (OKX-специфика)
        info = self.instruments.get(symbol)
        ct_val = info.get("ct_val", 1)  # сколько монет в 1 контракте
        if ct_val <= 0: ct_val = 1
        qty_contracts = qty_coins_raw / ct_val

        # Округление по qty_step (для OKX это шаг контрактов)
        qty_str, qty_f = self.instruments.round_qty(symbol, qty_contracts)
        if qty_str is None or qty_f<=0: return None,0,0,0,0

        # size_usdt — реальная позиция в USDT
        actual_coins = qty_f * ct_val
        size_usdt = actual_coins * price
        sl_dist = price * cfg.SL_PCT/100
        tp_dist = price * cfg.TP_PCT/100
        return qty_str, qty_f, size_usdt, sl_dist, tp_dist

    def can_open(self, symbol: str) -> tuple:
        # ── Warm-up Phase: первые WARMUP_SCANS циклов сканера только наблюдаем.
        # Сигналы из state.json могут быть устаревшими, индикаторы ещё не пересчитаны
        # на свежих свечах, классификатор режима рынка ещё не получил свежих BTC/ETH.
        if self._scans_done < cfg.WARMUP_SCANS:
            return False, f"Warm-up {self._scans_done}/{cfg.WARMUP_SCANS}"
        if symbol in self.blocked_symbols:
            return False, f"Заблокирована: {self._block_reasons.get(symbol, 'ошибка')}"
        if symbol in self.pm.positions: return False, "Уже в позиции"
        if self.pm.is_in_cooldown(symbol):
            until = self.pm.cooldowns[symbol]
            mins = max(0, int((until-time.time())/60))
            return False, f"CD {mins}m"
        if len(self.pm.positions) >= cfg.MAX_POSITIONS:
            return False, f"Лимит {cfg.MAX_POSITIONS}"
        # Rate-limit: не больше MAX_TRADES_PER_HOUR открытий за последние 60 минут.
        # Чистим старые записи, считаем оставшиеся.
        now = time.time()
        self._recent_opens = [t for t in self._recent_opens if now - t < 3600]
        if len(self._recent_opens) >= cfg.MAX_TRADES_PER_HOUR:
            oldest_ago = int((now - self._recent_opens[0]) / 60)
            return False, f"Rate-limit: {cfg.MAX_TRADES_PER_HOUR}/час (через {60-oldest_ago}m)"
        tier = get_tier(symbol)
        if self.pm.tier_count(tier) >= MAX_PER_TIER.get(tier,3):
            return False, f"Лимит {tier}"
        if self.pm.daily_pnl < -self.start_bal*cfg.MAX_DAILY_LOSS/100:
            return False, "Дневной лимит"
        if self.drawdown >= cfg.MAX_DRAWDOWN_STOP:
            return False, "Просадка"
        if self.funding_tracker.is_near_funding(threshold_seconds=cfg.AVOID_FUNDING_MIN*60):
            return False, "Funding скоро"
        if self.client.watchdog.silence_seconds() > 60:
            return False, "Сеть"
        if self.order_health and self.order_health.is_paused:
            return False, f"Order health pause {self.order_health.remaining_pause_seconds()}s"
        return True, ""

    def _correlation_guard(self, symbol: str, side: str) -> tuple[bool, str]:
        """Корреляционная защита перед открытием новой позиции.

        Проверяет:
          1. Не более CORR_MAX_SAME_DIRECTION позиций в одном направлении
             (long_count или short_count). Это страхует от ситуации
             "BTC падает, все альты падают → все наши long в минусе".
          2. Не более MEME_MAX_CONCURRENT мемов одновременно.
          3. Суммарный риск всех открытых ≤ CORR_MAX_PORTFOLIO_RISK% капитала.
             Риск каждой позиции = (entry - sl) / entry × 100.
          4. Circuit breaker: если за последние часы было SL_STREAK_THRESHOLD
             убыточных закрытий подряд — пауза SL_STREAK_PAUSE_HOURS часов.

        Args:
            symbol: новый символ для открытия (для проверки на мем)
            side: "Buy" или "Sell" — направление новой позиции

        Returns:
            (True, "") если можно открывать, (False, reason) если нет.
        """
        # 0. Circuit breaker
        if self._circuit_breaker_until > time.time():
            remaining_min = int((self._circuit_breaker_until - time.time()) / 60)
            return False, f"Circuit breaker: ещё {remaining_min}m"

        # 1. Max same-direction
        long_count = sum(1 for p in self.pm.positions.values() if p.get("side") == "Buy")
        short_count = sum(1 for p in self.pm.positions.values() if p.get("side") == "Sell")
        if side == "Buy" and long_count >= cfg.CORR_MAX_SAME_DIRECTION:
            return False, f"Лимит long: уже {long_count}, max {cfg.CORR_MAX_SAME_DIRECTION}"
        if side == "Sell" and short_count >= cfg.CORR_MAX_SAME_DIRECTION:
            return False, f"Лимит short: уже {short_count}, max {cfg.CORR_MAX_SAME_DIRECTION}"

        # 2. Max meme positions
        if is_meme_symbol(symbol):
            meme_count = sum(1 for s in self.pm.positions.keys() if is_meme_symbol(s))
            if meme_count >= cfg.MEME_MAX_CONCURRENT:
                return False, f"Лимит мемов: уже {meme_count}, max {cfg.MEME_MAX_CONCURRENT}"

        # 3. Portfolio risk
        total_risk_pct = 0.0
        for p in self.pm.positions.values():
            entry = p.get("entry", 0.0) or 0.0
            sl = p.get("sl", 0.0) or 0.0
            if entry > 0 and sl > 0:
                risk_pct = abs(entry - sl) / entry * 100
                total_risk_pct += risk_pct
        # Прибавляем риск новой позиции (используем дефолт SL_PCT или меньший для мема)
        new_risk_pct = cfg.MEME_SL_PCT if is_meme_symbol(symbol) else cfg.SL_PCT
        if total_risk_pct + new_risk_pct > cfg.CORR_MAX_PORTFOLIO_RISK:
            return False, (f"Лимит portfolio risk: уже {total_risk_pct:.1f}%, "
                           f"+{new_risk_pct:.1f}% превысит {cfg.CORR_MAX_PORTFOLIO_RISK:.1f}%")

        return True, ""

    def _record_sl_for_streak(self):
        """Вызывается из close_position при SL. Если streak >= threshold —
        активируется circuit breaker."""
        self._sl_streak += 1
        if self._sl_streak >= cfg.SL_STREAK_THRESHOLD:
            self._circuit_breaker_until = time.time() + cfg.SL_STREAK_PAUSE_HOURS * 3600
            log.warning(f"🛑 Circuit breaker: {self._sl_streak} SL подряд → "
                        f"пауза {cfg.SL_STREAK_PAUSE_HOURS}ч")
            self.log("🛑", f"Circuit breaker: {self._sl_streak} SL подряд, "
                            f"пауза {cfg.SL_STREAK_PAUSE_HOURS}ч", "risk")

    def _reset_sl_streak(self):
        """Сбрасывается при любом профитном закрытии (TP, manual win)."""
        if self._sl_streak > 0:
            log.info(f"SL streak сброшен (был {self._sl_streak})")
        self._sl_streak = 0

    def _record_signal(self, symbol: str, signal: str):
        """Запоминаем последний сигнал по символу. История обрезается
        до SIGNAL_CONFIRM_TICKS — сколько подряд подтверждений требуется."""
        max_keep = max(cfg.SIGNAL_CONFIRM_TICKS, 1)
        history = self._signal_history.setdefault(symbol, [])
        history.append(signal)
        # Держим только последние N
        if len(history) > max_keep:
            del history[:-max_keep]

    def _bump_reject(self, reason: str):
        """Инкремент счётчика отказов для дашборда. Раз в 5 минут все
        счётчики делятся пополам — exponential decay, чтобы видеть свежую
        картину, а не накопленную с начала запуска."""
        now = time.time()
        if now - self._reject_last_decay > 300:
            self.reject_counts = {k: v * 0.5 for k, v in self.reject_counts.items() if v >= 1}
            self._reject_last_decay = now
        self.reject_counts[reason] = self.reject_counts.get(reason, 0) + 1

    def _signal_confirmed(self, symbol: str, signal: str) -> bool:
        """True, если последние SIGNAL_CONFIRM_TICKS подряд сигналов по символу
        одинаковые и совпадают с текущим. Защита от однократных шумовых сигналов."""
        n = max(cfg.SIGNAL_CONFIRM_TICKS, 1)
        history = self._signal_history.get(symbol, [])
        if len(history) < n:
            return False  # Ещё не накопили достаточно повторений
        return all(s == signal for s in history[-n:])

    def _maybe_block_symbol(self, symbol: str, ret_code: int, ret_msg: str):
        """Если ошибка системная (юрисдикция/лимит размера) — блокируем пару
        до перезапуска, чтобы не флудить логи одинаковыми ошибками."""
        # Коды OKX: 51155 — compliance, 51202 — max amount, 51000-9 — account mode
        block_codes = {51155, 51202, 51000, 51001, 51002, 51008, 51010}
        if ret_code in block_codes and symbol not in self.blocked_symbols:
            short = (ret_msg or "")[:60]
            self.blocked_symbols.add(symbol)
            self._block_reasons[symbol] = f"OKX {ret_code}"
            self.log("🚫", f"{symbol} заблокирована до рестарта: {ret_code} — {short}", "warn")

        # 51050 = "TP should be higher than primary order price". Это значит
        # цена сильно ушла за наш расчётный TP (fib swing устарел). Ставим
        # cooldown на символ чтобы дать рынку устаканиться, а нам — пересчитать
        # свежий fib swing в следующем цикле сканера.
        if ret_code == 51050:
            self.pm.cooldowns[symbol] = time.time() + 300  # 5 минут
            self.log("⏳", f"{symbol}: TP устарел (51050), cooldown 5 мин", "warn")

    async def open_position(self, symbol: str, side: str, ind: dict):
        ok, reason = self.can_open(symbol)
        if not ok:
            # Раньше отказ был молчаливым — невозможно понять, почему сигнал
            # есть, а позиция не открывается. Теперь логируем причину (но не
            # флудим: warm-up и "уже в позиции" — рутина, пишем на debug-уровне
            # через reject-счётчик; остальное — явным INFO).
            routine = reason.startswith(("Warm-up", "Уже в позиции", "CD "))
            if not routine:
                log.info(f"[BLOCK] {symbol} {side}: {reason}")
            self._bump_reject(f"can_open: {reason.split(':')[0].split('(')[0].strip()}")
            return
        price = ind.get("price", 0)
        if price<=0:
            log.info(f"[BLOCK] {symbol} {side}: нет цены (price={price})")
            self._bump_reject("no_price")
            return

        qty_str, qty_f, size, sl_d, tp_d = self.calc_size_qty(symbol, price)
        if qty_str is None:
            log.info(f"[BLOCK] {symbol} {side}: calc_size_qty вернул None "
                     f"(price={price:.6g}, bal={self.balance:.2f}) — "
                     f"размер меньше min lot или округлился в 0")
            self._bump_reject("size_too_small")
            return

        bside = "Buy" if side=="buy" else "Sell"
        slip = cfg.SLIPPAGE_PCT/100
        entry_eff = price * (1+slip if side=="buy" else 1-slip)
        sl = entry_eff - sl_d if side=="buy" else entry_eff + sl_d
        tp = entry_eff + tp_d if side=="buy" else entry_eff - tp_d

        # ── Фикс OKX 51050 "TP should be higher than primary order price" ──
        # Корень: на market order OKX считает "primary price" по фактической
        # fill-цене, не по нашему расчётному price. На тонкой testnet ликвидности
        # fill может прыгнуть на 2-3% выше нашего price → TP оказывается ниже.
        # Защита: TP должен быть как минимум на (TP_PCT_MIN_GAP) % от текущей
        # цены. Если расчётный TP меньше этого порога — значит fib swing
        # устарел (цена ушла за пределы swing range), отказываемся от входа.
        TP_PCT_MIN_GAP = 0.5  # min зазор между TP и текущей ценой, в %
        min_gap = price * (TP_PCT_MIN_GAP / 100)
        if side == "buy":
            if tp - price < min_gap:
                log.info(
                    f"[REJECT] {symbol} Buy: TP {tp:.6g} слишком близко к "
                    f"текущей цене {price:.6g} (зазор {(tp-price)/price*100:.2f}% < "
                    f"{TP_PCT_MIN_GAP}%). Fib swing устарел — пропускаем."
                )
                return
        else:
            if price - tp < min_gap:
                log.info(
                    f"[REJECT] {symbol} Sell: TP {tp:.6g} слишком близко к "
                    f"текущей цене {price:.6g} (зазор {(price-tp)/price*100:.2f}% < "
                    f"{TP_PCT_MIN_GAP}%). Fib swing устарел — пропускаем."
                )
                return

        sl_str = self.instruments.round_price(symbol, sl)
        # При включённом partial_tp основной TP на бирже не ставим
        # (ставим только через локальное отслеживание)
        use_partial = self.partial_tp_config.enabled
        tp_str = None if use_partial else self.instruments.round_price(symbol, tp)
        tier = get_tier(symbol)

        pos_idx = self.position_mode.position_idx
        await self.client.set_leverage(symbol, cfg.LEVERAGE)
        r = await self.client.place_order(symbol, bside, qty_str, sl_str, tp_str,
                                            position_idx=pos_idx)
        # Order Health analysis
        if self.order_health:
            self.order_health.record_order_response(r, symbol)

        # ── ДИАГНОСТИКА: при ошибке логируем полный ответ OKX ──
        # (нужно чтобы понимать причины "All operations failed")
        if r.get("retCode") != 0:
            log.error(f"[ORDER-DEBUG] {symbol} side={bside} qty={qty_str} "
                      f"sl={sl_str} tp={tp_str} pos_idx={pos_idx}")
            log.error(f"[ORDER-DEBUG] FULL RESPONSE: {r}")
            # Авто-блокировка пары при «системных» ошибках (юрисдикция,
            # размер, режим аккаунта). Чтобы каждый цикл не повторять.
            self._maybe_block_symbol(symbol, r.get("retCode", 0), r.get("retMsg", ""))

        if r.get("retCode")==0:
            oid = r.get("result",{}).get("orderId","")
            self.pm.add(symbol, bside, price, qty_f, size,
                        float(sl_str), float(tp_str) if tp_str else 0.0,
                        cfg.LEVERAGE, self.strategy, oid)
            # Записываем время открытия для rate-limit
            self._recent_opens.append(time.time())
            # Привяжем partial_tp состояние к позиции
            if use_partial:
                self.pm.positions[symbol]["partial_tp"] = \
                    PartialTPState.init_for_position(self.partial_tp_config)
                self.pm.positions[symbol]["initial_qty"] = qty_f
            arrow = "▲" if side=="buy" else "▼"
            tp_info = "PartialTP" if use_partial else f"TP {tp_str}"
            self.log("📈" if side=="buy" else "📉",
                f"{arrow} {bside} {symbol}[{tier}] @ ${price:.6g} | "
                f"SL {sl_str} {tp_info} | qty={qty_str}",
                "buy" if side=="buy" else "sell")
            await self.telegram.notify_position_opened(
                symbol, bside, price, size, float(sl_str),
                float(tp_str) if tp_str else 0.0
            )

    async def close_position(self, symbol: str, reason: str):
        pos = self.pm.positions.get(symbol)
        if not pos: return
        qty_str, _ = self.instruments.round_qty(symbol, pos["qty"])
        if qty_str is None: qty_str = str(pos["qty"])
        await self.client.close_market(symbol, pos["side"], qty_str,
                                          position_idx=self.position_mode.position_idx)

        price = (self.market_data.get(symbol,{})
                 .get("indicators",{}).get("price", pos["entry"]))
        if price<=0 or pos["entry"]<=0:
            gross_pnl = 0.0
        else:
            slip = cfg.SLIPPAGE_PCT/100
            if pos["side"]=="Buy":
                exit_eff = price * (1-slip)
                diff = exit_eff - pos["entry"]
            else:
                exit_eff = price * (1+slip)
                diff = pos["entry"] - exit_eff
            gross_pnl = (diff/pos["entry"]) * pos["size_usdt"]

        net_pnl = self.pm.record_close(symbol, gross_pnl, reason)
        self.balance += net_pnl
        self.pm.equity_hist.append(self.balance)
        if self.balance > self.peak_bal: self.peak_bal = self.balance
        if self.peak_bal > 0:
            self.drawdown = (self.peak_bal-self.balance)/self.peak_bal*100
            self.max_dd = max(self.max_dd, self.drawdown)
        icon = "✅" if net_pnl>=0 else "❌"
        pnls = f"+${net_pnl:.2f}" if net_pnl>=0 else f"-${abs(net_pnl):.2f}"
        self.log(icon,
            f"{pos['side']} {symbol} [{reason}] PnL:{pnls} | Bal:${self.balance:.2f}",
            "buy" if net_pnl>=0 else "sell")
        await self.telegram.notify_position_closed(symbol, net_pnl, reason, self.balance)
        # Сброс истории объёмов для drop-guard (v1.1)
        if self.vol_drop_guard is not None:
            self.vol_drop_guard.reset(symbol)
        # ── Phase 1: circuit breaker tracking ──
        # SL → streak++; любой профит (TP, manual win) → reset.
        # Когда streak >= threshold — _record_sl_for_streak активирует паузу.
        if cfg.USE_FIB_STRATEGY:
            if net_pnl < 0 and reason in ("STOP-LOSS", "MANUAL-STOP", "TIME-STOP"):
                self._record_sl_for_streak()
            elif net_pnl >= 0:
                self._reset_sl_streak()

    async def _update_sl_safe(self, sym: str, pos: dict, new_sl: float,
                                reason: str = "") -> bool:
        """
        Безопасное обновление SL: сначала шлём amend на биржу с verify,
        и ТОЛЬКО при успехе пишем в local pos["sl"].

        Это закрывает корень бага "SL drift": раньше local менялся, биржа нет.
        Возвращает True если оба источника обновились, False — иначе.
        """
        try:
            new_sl_rounded = float(self.instruments.round_price(sym, new_sl))
            ok = await self.client.update_sl_tp(sym, sl=new_sl_rounded, verify=True)
            if ok:
                pos["sl"] = new_sl_rounded
                return True
            # Биржа не подтвердила — local НЕ трогаем
            log.warning(f"[SL-UPDATE] {sym} {reason}: amend на бирже не подтверждён, "
                        f"local SL={pos['sl']:.6g} оставлен прежним")
            return False
        except Exception as e:
            log.error(f"[SL-UPDATE] {sym} {reason}: исключение при amend: {e}")
            return False

    async def check_positions(self):
        for sym, pos in list(self.pm.positions.items()):
            md = self.market_data.get(sym,{})
            price = md.get("indicators",{}).get("price")
            if not price and self.ws_stream:
                t = self.ws_stream.get_ticker(sym)
                price = t.get("lastPrice") if t else None
            if not price or price<=0: continue
            self.pm.update_pnl(sym, price)

            # ── Volume Drop Guard (v1.1) ──
            # Действие установлено в analyze_symbol, если средний объём за 1ч
            # упал в N раз ниже среднего за 4ч. Защита от пересыхания ликвидности.
            ind = md.get("indicators", {})
            drop_action = ind.get("vol_drop_action")
            if drop_action and self.vol_drop_guard is not None:
                stats = self.vol_drop_guard.stats(sym)
                ratio = stats.get("ratio", 0)
                if drop_action == "alert":
                    self.log("⚠", f"{sym} объём пересыхает (4h/1h={ratio:.1f}x)", "warn")
                    await self.telegram.send(
                        f"⚠ Volume drop on {sym}\nRatio (4h/1h): {ratio:.1f}",
                        severity="alert", silent=True
                    )
                elif drop_action == "tighten_sl":
                    pct = cfg.VOLUME_DROP_TIGHTEN_PCT / 100.0
                    if pos["side"] == "Buy":
                        new_sl = price * (1 - pct)
                        if new_sl > pos["sl"]:
                            old_sl = pos["sl"]
                            ok = await self._update_sl_safe(sym, pos, new_sl,
                                                             reason="vol-drop-tighten")
                            if ok:
                                self.log("⚠", f"{sym} объём упал {ratio:.1f}x — SL→{pos['sl']:.6g}",
                                         "warn")
                                await self.telegram.send(
                                    f"⚠ {sym}: объём упал в {ratio:.1f}x, SL подтянут к {pos['sl']:.6g}",
                                    severity="alert", silent=True)
                    else:
                        new_sl = price * (1 + pct)
                        if new_sl < pos["sl"]:
                            old_sl = pos["sl"]
                            ok = await self._update_sl_safe(sym, pos, new_sl,
                                                             reason="vol-drop-tighten")
                            if ok:
                                self.log("⚠", f"{sym} объём упал {ratio:.1f}x — SL→{pos['sl']:.6g}",
                                         "warn")
                                await self.telegram.send(
                                    f"⚠ {sym}: объём упал в {ratio:.1f}x, SL подтянут к {pos['sl']:.6g}",
                                    severity="alert", silent=True)
                elif drop_action == "close":
                    self.log("🔴", f"{sym} закрытие: объём упал в {ratio:.1f}x", "warn")
                    await self.telegram.send(
                        f"🔴 {sym} закрыт: пересыхание объёма (4h/1h={ratio:.1f}x)",
                        severity="critical")
                    await self.close_position(sym, "VOL-DROP")
                    continue
                # Чистим флаг чтобы не зациклить (cooldown в guard сам не даст повтора)
                ind.pop("vol_drop_action", None)

            # Сначала — проверка partial TP уровней
            if self.partial_tp_config.enabled and pos.get("partial_tp"):
                ptp_result = await self.partial_tp.check_and_execute(sym, pos, price)
                # Логируем сработавшие уровни
                for trig in ptp_result.get("triggered", []):
                    pnl_str = f"+${trig['pnl']:.2f}" if trig['pnl']>=0 else f"-${abs(trig['pnl']):.2f}"
                    self.log("🎯",
                        f"{sym} TP{trig['level_idx']+1} (+{trig['level_pct']}%): "
                        f"закрыто {trig['qty']:.6g} | PnL {pnl_str}",
                        "buy")
                    await self.telegram.send(
                        f"🎯 {sym} TP{trig['level_idx']+1}: +{trig['level_pct']}% "
                        f"| Closed {trig['qty']:.6g} | PnL {pnl_str}",
                        severity="trade", silent=True
                    )
                # Если позиция полностью закрыта partial TP — удаляем
                ptp_state = pos.get("partial_tp", {})
                # Используем порог 1% от initial_qty для учёта округлений
                initial_q = pos.get("initial_qty", pos.get("qty", 1))
                if (ptp_state.get("all_triggered") or
                    ptp_state.get("remaining_qty", 1) <= initial_q * 0.01):
                    self._finalize_partial_close(sym, pos, "PARTIAL-TP-COMPLETE")
                    continue

            # Стандартная проверка SL и trailing
            if pos["side"]=="Buy":
                if price<=pos["sl"]: await self.close_position(sym,"STOP-LOSS"); continue
                # TP проверяем только если partial_tp выключен
                if not self.partial_tp_config.enabled and pos["tp"]>0 and price>=pos["tp"]:
                    await self.close_position(sym,"TAKE-PROFIT"); continue
                # Trailing: после всех partial TP или по обычной логике
                ptp_done = pos.get("partial_tp", {}).get("all_triggered", False)
                if ptp_done:
                    trail_pct = self.partial_tp_config.trail_pct_after_all_tp
                    if price>pos["entry"]:
                        new_sl = price*(1-trail_pct/100)
                        if new_sl>pos["sl"]:
                            await self._update_sl_safe(sym, pos, new_sl, reason="trail-post-ptp")
                elif price>pos["entry"]*1.015:
                    new_sl = price*(1-cfg.TRAIL_PCT/100)
                    if new_sl>pos["sl"]:
                        await self._update_sl_safe(sym, pos, new_sl, reason="trail")
            else:
                if price>=pos["sl"]: await self.close_position(sym,"STOP-LOSS"); continue
                if not self.partial_tp_config.enabled and pos["tp"]>0 and price<=pos["tp"]:
                    await self.close_position(sym,"TAKE-PROFIT"); continue
                ptp_done = pos.get("partial_tp", {}).get("all_triggered", False)
                if ptp_done:
                    trail_pct = self.partial_tp_config.trail_pct_after_all_tp
                    if price<pos["entry"]:
                        new_sl = price*(1+trail_pct/100)
                        if new_sl<pos["sl"]:
                            await self._update_sl_safe(sym, pos, new_sl, reason="trail-post-ptp")
                elif price<pos["entry"]*0.985:
                    new_sl = price*(1+cfg.TRAIL_PCT/100)
                    if new_sl<pos["sl"]:
                        await self._update_sl_safe(sym, pos, new_sl, reason="trail")

    def _finalize_partial_close(self, symbol: str, pos: dict, reason: str):
        """Завершаем позицию закрытую через partial TP (для статистики)."""
        # PnL уже накоплен в process partial closes
        ptp = pos.get("partial_tp", {})
        total_realized = sum(l.get("realized_pnl", 0) for l in ptp.get("levels", []))
        # Запишем в историю как одну сделку
        self.pm.trade_hist.append({
            "symbol": symbol, "side": pos.get("side",""),
            "entry": pos.get("entry",0),
            "pnl": round(total_realized, 4),
            "pnl_gross": round(total_realized, 4),  # уже за вычетом fees
            "fees": 0,  # уже учтены в каждом partial
            "reason": reason,
            "strategy": pos.get("strategy",""),
            "tier": pos.get("tier",""),
            "time": datetime.now(timezone.utc).isoformat(),
            "partial_tp_summary": PartialTPManager.get_summary(pos),
        })
        if total_realized > 0:
            self.pm.wins += 1
            self.pm.best_trade = max(self.pm.best_trade, total_realized)
        else:
            self.pm.losses += 1
            self.pm.worst_trade = min(self.pm.worst_trade, total_realized)
        self.pm.trades_today += 1
        self.pm.positions.pop(symbol, None)
        self.log("✅", f"{symbol} полностью закрыт через partial TP. "
                       f"Total PnL: ${total_realized:+.2f}", "buy" if total_realized>=0 else "sell")

    async def check_guard(self):
        if self.drawdown >= cfg.MAX_DRAWDOWN_STOP and self.running:
            self.log("🛑", f"DD-GUARD! {self.drawdown:.2f}% ≥ {cfg.MAX_DRAWDOWN_STOP}%", "risk")
            await self.alert_critical(
                f"⚠️ DRAWDOWN GUARD: {self.drawdown:.2f}% — закрываю всё!"
            )
            for sym in list(self.pm.positions.keys()):
                await self.close_position(sym, "DD-GUARD")
            self.running = False

    async def check_network(self):
        wd = self.client.watchdog
        if wd.should_alert():
            self.log("⚠", f"Сеть: {wd.silence_seconds():.0f}с тишины", "warn")
            await self.alert_critical(
                f"Network silence: {wd.silence_seconds():.0f}s — не получаем данные с Bybit"
            )
        if wd.should_panic_close() and self.pm.positions:
            self.log("🚨", "NETWORK PANIC: закрываю всё", "risk")
            await self.alert_critical("🚨 NETWORK PANIC: closing all positions")
            for sym in list(self.pm.positions.keys()):
                await self.close_position(sym, "NET-PANIC")
            self.running = False

    # ── Main loop ─────────────────────────────
    async def run_loop(self):
        self.log("🚀", "Цикл запущен", "info")
        try:
            while self.running:
                try:
                    self.pm.reset_daily_if_needed()
                    if not self.paused:
                        # Сканер
                        if self._tick % 3 == 0:
                            results = await self.scanner.scan()
                            self.scan_results = results
                            top = [s["symbol"].replace("USDT","") for s in results[:5]]
                            if top: self.log("🔭", f"ТОП: {' · '.join(top)}", "info")
                            # ── Warm-up: засчитываем полный цикл сканера
                            if self._scans_done < cfg.WARMUP_SCANS:
                                self._scans_done += 1
                                if self._warmup_started_at == 0:
                                    self._warmup_started_at = time.time()
                                if self._scans_done == cfg.WARMUP_SCANS:
                                    elapsed = time.time() - self._warmup_started_at
                                    self.log("🔥", f"Warm-up закончен за {elapsed:.0f}с — "
                                                   f"торговля разрешена", "info")
                                else:
                                    self.log("🔥", f"Warm-up {self._scans_done}/{cfg.WARMUP_SCANS} "
                                                   f"— наблюдаем рынок, торговля заблокирована",
                                             "info")

                        # Funding
                        if self.ws_stream:
                            for sym, t in self.ws_stream.tickers.items():
                                if "fundingRate" in t:
                                    self.funding_tracker.update_rate(sym, t["fundingRate"])

                        # Анализ
                        # ВСЕ активные пары анализируются на каждом цикле — иначе
                        # дашборд не получает индикаторы/свечи для большинства
                        # символов. Решение о ВХОДЕ по-прежнему принимает Phase 1
                        # pipeline (на rejection возвращает base без сигнала).
                        targets = list(set(cfg.ACTIVE_SYMBOLS) | set(self.pm.positions.keys()))
                        if targets:
                            # ── Phase 1 переключатель ──────────────────
                            # При USE_FIB_STRATEGY=true вызываем новый pipeline
                            # (trend + fib + div). При false — старая логика.
                            if cfg.USE_FIB_STRATEGY and self.trend_filter is not None:
                                analyze_fn = self.analyze_with_fib_strategy
                            else:
                                analyze_fn = self.analyze_symbol
                            # Бьём на батчи по 15 чтобы не упереться в OKX rate-limit
                            # (по умолчанию 20 req/2s на endpoint). 15 пар × 1-2
                            # запроса = ~30 запросов на батч с задержкой 0.5s.
                            BATCH = 15
                            for i in range(0, len(targets), BATCH):
                                batch = targets[i:i+BATCH]
                                results = await asyncio.gather(
                                    *[analyze_fn(s) for s in batch],
                                    return_exceptions=True)
                                for r in results:
                                    if isinstance(r, dict) and r.get("symbol"):
                                        self.market_data[r["symbol"]] = r
                                # Небольшая пауза между батчами
                                if i + BATCH < len(targets):
                                    await asyncio.sleep(0.5)

                        # Если включён auto-выбор стратегии — пересматриваем
                        # на основе свежих индикаторов BTC/ETH
                        self._maybe_reassess_strategy()

                        await self.check_positions()
                        await self.check_guard()
                        await self.check_network()

                        # ── Запись истории сигналов для persistence-фильтра.
                        # Делаем это всегда (даже во время warm-up), чтобы к моменту
                        # окончания warm-up у нас была история подтверждений.
                        for sym in targets:
                            d = self.market_data.get(sym, {})
                            sig_value = d.get("signal", {}).get("signal", "wait")
                            self._record_signal(sym, sig_value)

                        if self.running and not self.paused:
                            for sym in targets:
                                d = self.market_data.get(sym,{})
                                sig = d.get("signal",{})
                                ind = d.get("indicators",{})
                                if sig.get("signal") in ("buy","sell") and ind:
                                    # Persistence: открываем только если сигнал
                                    # повторился SIGNAL_CONFIRM_TICKS раз подряд
                                    if not self._signal_confirmed(sym, sig["signal"]):
                                        continue
                                    # ── Phase 1: корреляционная защита ──
                                    # Проверяем только если включена новая стратегия —
                                    # в старой логике этой защиты не было, не ломаем поведение
                                    if cfg.USE_FIB_STRATEGY:
                                        side = "Buy" if sig["signal"] == "buy" else "Sell"
                                        ok, reason = self._correlation_guard(sym, side)
                                        if not ok:
                                            log.info(f"[CORR] {sym} {side} заблокирован: {reason}")
                                            self._bump_reject("correlation_limit")
                                            continue
                                    await self.open_position(sym, sig["signal"], ind)

                        if cfg.API_KEY and time.time()-self._last_balance_refresh > cfg.BALANCE_REFRESH_S:
                            rb = await self.client.get_balance()
                            if rb>0: self.balance = rb
                            self._last_balance_refresh = time.time()

                        await self._broadcast({"type":"state","data":self.get_state()})
                        self._tick += 1

                    sleep_s = 1 if self.paused else cfg.SCAN_INTERVAL
                    await asyncio.sleep(sleep_s)
                except asyncio.CancelledError: raise
                except Exception as e:
                    log.exception(f"Loop iter: {e}")
                    await asyncio.sleep(5)
        except asyncio.CancelledError:
            log.info("Loop cancelled")
        finally:
            log.info("Loop stopped")

    # ── State ─────────────────────────────────
    def get_state(self) -> dict:
        pnl = self.balance - self.start_bal
        pnl_pct = pnl/self.start_bal*100 if self.start_bal>0 else 0
        return {
            "balance":round(self.balance,2),
            "start_balance":round(self.start_bal,2),
            "total_pnl":round(pnl,2),
            "total_pnl_pct":round(pnl_pct,2),
            "daily_pnl":round(self.pm.daily_pnl,2),
            "drawdown":round(self.drawdown,2),
            "max_drawdown":round(self.max_dd,2),
            "wins":self.pm.wins,"losses":self.pm.losses,
            "win_rate":round(self.pm.win_rate,1),
            "total_trades":self.pm.wins+self.pm.losses,
            "trades_today":self.pm.trades_today,
            "running":self.running,"paused":self.paused,
            "ready":self._ready,"strategy":self.strategy,
            "testnet":cfg.TESTNET,
            "positions":list(self.pm.positions.values()),
            "equity_history":list(self.pm.equity_hist)[-200:],
            "scan_results":self.scan_results[:50],
            "market_data":{
                sym:{
                    "symbol":sym,"signal":d.get("signal",{}),
                    "price":d.get("indicators",{}).get("price",0),
                    # candles нужны дашборду для рисования графика, но они большие.
                    # Включаем только для пар которые есть в табах графика
                    # и обрезаем до последних 80 свечей.
                    "indicators": (
                        # Исключаем `candles` и все приватные ключи (`_fib_setup`,
                        # `_meme_setup`) — это объекты Python, не JSON-сериализуемые.
                        {**{k:v for k,v in d.get("indicators",{}).items()
                            if k != "candles" and not k.startswith("_")},
                         "candles": d.get("indicators",{}).get("candles", [])[-80:]}
                        if sym in ("BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","XRPUSDT")
                        else {k:v for k,v in d.get("indicators",{}).items()
                              if k != "candles" and not k.startswith("_")}
                    ),
                } for sym,d in self.market_data.items()
            },
            "logs":list(self.logs)[:80],
            "trade_history":list(self.pm.trade_hist)[-100:],
            "best_trade":round(self.pm.best_trade,2),
            "worst_trade":round(self.pm.worst_trade,2),
            "active_symbols_count":len(cfg.ACTIVE_SYMBOLS),
            "scan_count":len(self.scan_results),
            "tiers":{t:self.pm.tier_count(t) for t in TIER_MULT},
            "cooldowns":{s:int(t-time.time())
                         for s,t in self.pm.cooldowns.items() if t>time.time()},
            "fees_total":round(self.pm.fees_total,2),
            # ── Multi-TF Heatmap для дашборда. Отдаём только пары, по которым
            #    есть свежие данные (за последние 10 минут).
            "trend_heatmap": {
                sym: {k: v for k, v in d.items() if k != "ts"}
                for sym, d in self.trend_heatmap.items()
                if time.time() - d.get("ts", 0) < 600
            },
            # ── Rejection counters за последний 5-минутный декей-цикл
            "reject_counts": {k: int(round(v)) for k, v in self.reject_counts.items() if v >= 1},
            "ws_status":{
                "connected": bool(self.ws_stream and self.ws_stream.connected),
                "stale": bool(self.ws_stream and self.ws_stream.is_stale),
                "subscribed": len(self.ws_stream.subscribed_symbols) if self.ws_stream else 0,
            },
            "circuit_breaker": self.client.circuit_breaker.state if self.client.circuit_breaker else "unknown",
            "network_silence_s": int(self.client.watchdog.silence_seconds()),
            "next_funding_s": self.funding_tracker.estimate_next_funding_seconds(),
            "position_mode": self.position_mode.current_mode,
            "order_health": {
                "paused": self.order_health.is_paused if self.order_health else False,
                "remaining_s": self.order_health.remaining_pause_seconds() if self.order_health else 0,
                "errors_in_window": len(self.order_health.errors) if self.order_health else 0,
            },
            "telegram_enabled": self.telegram.enabled,
            "time_offset_ms": self.client.time_sync.offset_ms if self.client.time_sync else 0,
            "config": {
                "risk_per_trade":    cfg.RISK_PER_TRADE,
                "sl_pct":            cfg.SL_PCT,
                "tp_pct":            cfg.TP_PCT,
                "trail_pct":         cfg.TRAIL_PCT,
                "leverage":          cfg.LEVERAGE,
                "max_positions":     cfg.MAX_POSITIONS,
                "max_daily_loss":    cfg.MAX_DAILY_LOSS,
                "max_drawdown_stop": cfg.MAX_DRAWDOWN_STOP,
                "cooldown_after_sl": cfg.COOLDOWN_AFTER_SL,
                "scan_interval":     cfg.SCAN_INTERVAL,
                "min_volume_usdt":   cfg.MIN_VOLUME_USDT,
                "signal_confidence": cfg.SIGNAL_CONFIDENCE,
            },
            # Авто-выбор стратегии: режим рынка (для отображения на дашборде)
            "auto_strategy": self.auto_strategy_mode,
            "market_regime": self.market_regime,
            # Рейтинги 5 стратегий (0..100) по индикаторам BTC — для дашборда
            "strategy_scores": StrategyScorer.score_all(
                self.market_data.get("BTCUSDT", {}).get("indicators", {})
            ),
            # Warm-up status: дашборд может показывать прогресс
            "warmup": {
                "active": self._scans_done < cfg.WARMUP_SCANS,
                "scans_done": self._scans_done,
                "scans_total": cfg.WARMUP_SCANS,
            },
            "partial_tp": {
                "enabled": self.partial_tp_config.enabled,
                "levels": [
                    {"pct": l.pct, "share": int(l.close_share*100)}
                    for l in self.partial_tp_config.levels
                ] if self.partial_tp_config.enabled else [],
                "move_sl_to_be_after_tp1": self.partial_tp_config.move_sl_to_be_after_tp1,
            },
        }

    # ── Controls ──────────────────────────────
    async def start(self):
        if not self._ready: return {"ok":False,"msg":"Не готов"}
        if self.running:    return {"ok":False,"msg":"Уже запущен"}
        # Если WebSocket был остановлен предыдущим Stop — поднимаем заново.
        # Без этого после Stop → Start не будет live-данных и сигналы не работают.
        # Используем атрибут .connected (он есть) вместо .running (которого нет).
        if self.ws_stream and not self.ws_stream.connected:
            try:
                connected = await self.ws_stream.connect()
                if connected:
                    await self.ws_stream.subscribe(cfg.ACTIVE_SYMBOLS)
                    asyncio.create_task(self.ws_stream.listen())
                    self.log("📡", f"WS переподключён, подписан "
                                    f"{len(cfg.ACTIVE_SYMBOLS)} тикеров", "info")
            except Exception as e:
                self.log("⚠", f"WS restart: {e}", "warn")
        # Сбрасываем warm-up счётчик чтобы при Start заново прогрелись —
        # это важно, потому что после длительного Stop сигналы устарели.
        self._scans_done = 0
        self._warmup_started_at = 0
        self._signal_history.clear()
        self.running=True; self.paused=False
        self._loop_task = asyncio.create_task(self.run_loop())
        self.log("▶","Запущен","info")
        return {"ok":True}

    async def pause(self):
        self.paused = not self.paused
        self.log("⏸" if self.paused else "▶",
                 "Пауза" if self.paused else "Продолжение", "warn")
        return {"ok":True,"paused":self.paused}

    async def stop(self):
        self.running=False; self.paused=False
        if self._loop_task and not self._loop_task.done():
            self._loop_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._loop_task
        for sym in list(self.pm.positions.keys()):
            await self.close_position(sym, "MANUAL-STOP")
        # Финальный persist уже после закрытия позиций.
        # Это важно: если persist делать раньше, то state.json
        # сохранит ещё не закрытые позиции, и reconciler при следующем
        # старте начнёт их «восстанавливать» — выглядит странно.
        await self._persist_now()
        # Останавливаем WebSocket-стрим явно, чтобы он не пытался
        # переподключиться после Stop. Без этого WS живёт своей жизнью
        # и в логах после Stop появляются reconnect-сообщения.
        if self.ws_stream:
            with contextlib.suppress(Exception):
                await self.ws_stream.stop()
        self.log("⏹","Остановлен","warn")
        await self.telegram.notify_bot_stopped("manual")
        return {"ok":True}

    async def emergency_stop(self):
        await self.stop()
        self.log("🚨","АВАРИЙНАЯ ОСТАНОВКА","risk")
        await self.alert_critical("🚨 EMERGENCY STOP triggered")
        return {"ok":True}

    async def close_symbol(self, symbol: str):
        await self.close_position(symbol,"MANUAL")
        return {"ok":True}

    async def reconcile_now(self):
        if not self.reconciler: return {"ok":False}
        rec = await self.reconciler.reconcile()
        return {"ok":True,"result":rec}

    def update_config(self, data: dict):
        for k,v in data.items():
            uk = k.upper()
            if hasattr(cfg, uk):
                cur = getattr(cfg, uk)
                try:
                    if isinstance(cur, bool):     setattr(cfg, uk, bool(v))
                    elif isinstance(cur, int) and not isinstance(cur, bool):
                                                  setattr(cfg, uk, int(v))
                    elif isinstance(cur, float):  setattr(cfg, uk, float(v))
                    elif isinstance(cur, list):
                        setattr(cfg, uk, v if isinstance(v, list) else str(v).split(","))
                    else: setattr(cfg, uk, v)
                except (ValueError,TypeError): log.warning(f"Bad type {uk}={v}")
        self.strategy = cfg.STRATEGY
        self.log("⚙", f"Config: {list(data.keys())}", "info")
        return {"ok":True}

# ──────────────────────────────────────────────
# FASTAPI
# ──────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    await bot.initialize()
    yield
    await bot.shutdown()

app = FastAPI(title="PHANTOM v1.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
bot = TradingBot()

@app.get("/")
async def index():
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.html")
    if os.path.exists(path): return FileResponse(path)
    return JSONResponse({"error":"HTML not found"}, status_code=404)

@app.get("/api/state")
async def api_state(): return JSONResponse(bot.get_state())
@app.post("/api/start")
async def api_start(): return await bot.start()
@app.post("/api/pause")
async def api_pause(): return await bot.pause()
@app.post("/api/stop")
async def api_stop(): return await bot.stop()
@app.post("/api/emergency")
async def api_emergency(): return await bot.emergency_stop()
@app.post("/api/close/{symbol}")
async def api_close(symbol: str): return await bot.close_symbol(symbol)
@app.post("/api/config")
async def api_config(data: dict): return bot.update_config(data)
@app.post("/api/reconcile")
async def api_reconcile(): return await bot.reconcile_now()
@app.get("/api/scan")
async def api_scan():
    r = await bot.scanner.scan()
    return {"results":r,"total":len(r)}
@app.get("/api/symbols")
async def api_symbols():
    return {"all":TOP50_SYMBOLS,"active":cfg.ACTIVE_SYMBOLS,
            "tiers":SYMBOL_TIERS,"tier_limits":MAX_PER_TIER,
            "tier_multipliers":TIER_MULT}
@app.get("/api/klines/{symbol}")
async def api_klines(symbol: str, interval: str = "15", limit: int = 200):
    klines = await bot.client.get_klines(symbol, interval, limit)
    return {"symbol":symbol,"klines":klines}
@app.get("/api/ticker/{symbol}")
async def api_ticker(symbol: str): return await bot.client.get_ticker(symbol)
@app.get("/api/logs")
async def api_logs(): return {"logs":list(bot.logs)}
@app.get("/api/instruments")
async def api_instruments(): return {"instruments":bot.instruments.data}

@app.get("/api/health")
async def api_health():
    return {
        "ready": bot._ready,
        "running": bot.running,
        "paused": bot.paused,
        "ws_connected": bool(bot.ws_stream and bot.ws_stream.connected),
        "circuit_breaker": bot.client.circuit_breaker.state,
        "network_silence_s": int(bot.client.watchdog.silence_seconds()),
        "positions": len(bot.pm.positions),
        "balance": bot.balance,
        "position_mode": bot.position_mode.current_mode,
        "order_health_paused": bot.order_health.is_paused if bot.order_health else False,
        "time_offset_ms": bot.client.time_sync.offset_ms if bot.client.time_sync else 0,
        "telegram_enabled": bot.telegram.enabled,
    }

@app.websocket("/ws")
async def ws_ep(ws: WebSocket):
    await ws.accept()
    bot.ws_clients.append(ws)
    try:
        await ws.send_json({"type":"state","data":bot.get_state()})
        while True:
            msg = await ws.receive_json()
            cmd = msg.get("cmd","")
            if   cmd=="start":     await bot.start()
            elif cmd=="pause":     await bot.pause()
            elif cmd=="stop":      await bot.stop()
            elif cmd=="emergency": await bot.emergency_stop()
            elif cmd=="close":     await bot.close_symbol(msg.get("symbol",""))
            elif cmd=="config":    bot.update_config(msg.get("data",{}))
            elif cmd=="reconcile": await bot.reconcile_now()
            elif cmd=="ping":      await ws.send_json({"type":"pong"})
            # После любой команды управления (кроме ping) сразу
            # рассылаем актуальный state всем подключённым клиентам,
            # чтобы дашборд мгновенно отразил Start/Stop/Pause.
            if cmd and cmd != "ping":
                await bot._broadcast({"type":"state","data":bot.get_state()})
    except WebSocketDisconnect: pass
    except Exception as e: log.warning(f"WS: {e}")
    finally:
        if ws in bot.ws_clients: bot.ws_clients.remove(ws)

if __name__ == "__main__":
    print("""
╔═══════════════════════════════════════════════════════╗
║                                                       ║
║          ██████╗ ██╗  ██╗ █████╗ ███╗   ██╗████████╗  ║
║          ██╔══██╗██║  ██║██╔══██╗████╗  ██║╚══██╔══╝  ║
║          ██████╔╝███████║███████║██╔██╗ ██║   ██║     ║
║          ██╔═══╝ ██╔══██║██╔══██║██║╚██╗██║   ██║     ║
║          ██║     ██║  ██║██║  ██║██║ ╚████║   ██║     ║
║          ╚═╝     ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═══╝   ╚═╝     ║
║                                                       ║
║   PHANTOM v1.0 — OKX Crypto Futures Bot               ║
║                                                       ║
║   ✓ 50 trading pairs        ✓ 16 indicators           ║
║   ✓ 5 strategies            ✓ Multi-level TP          ║
║   ✓ Liquidation monitor     ✓ Order health throttle   ║
║   ✓ Telegram alerts         ✓ Walk-forward validation ║
║   ✓ Network panic close     ✓ Drawdown guard          ║
║                                                       ║
║   Dashboard:  http://localhost:8000                   ║
║   Health:     http://localhost:8000/api/health        ║
║   Backtest:   python backtest.py --help               ║
║   Walk-Fwd:   python walk_forward.py --help           ║
╚═══════════════════════════════════════════════════════╝
    """)
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")
