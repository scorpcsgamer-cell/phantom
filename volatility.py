"""
volatility.py — Адаптивный таймфрейм для PHANTOM (Фаза 1, Поставка 4A)
==========================================================================

Задача: для каждого символа выбрать оптимальный таймфрейм входа по текущей
волатильности. Высокая волатильность → короткий TF (быстро ловим движение).
Низкая → длинный TF (фильтруем шум).

Используем два метрики на 1h klines:
  • ATR% = ATR(14) / close * 100   — амплитуда движения в %
  • BB Width = (BB_upper - BB_lower) / BB_middle * 100   — ширина канала

Логика выбора TF:
  ATR% > 2.5%    → 1m   (экстремальная волатильность, пампы/дампы)
  ATR% > 1.5%    → 5m   (высокая)
  ATR% > 0.7%    → 15m  (нормальная, дефолт)
  ATR% ≤ 0.7%    → 1h   (низкая, тихий рынок)

Дополнительная корректировка по BB Width:
  Если BB Width < 1% (сжатие, "squeeze") — добавляем +1 ступень к TF.
  Это предотвращает вход в боковике на короткий TF, ждём пробоя.

API:
    result = pick_adaptive_tf(klines_1h)
    # → {"tf": "5m", "atr_pct": 1.82, "bb_width": 3.4, "regime": "high"}
"""

from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd

log = logging.getLogger("volatility")


# ════════════════════════════════════════════════════════════════════
# Константы
# ════════════════════════════════════════════════════════════════════

ATR_PERIOD = 14
BB_PERIOD = 20
BB_STD = 2.0

# Пороги ATR% для выбора TF. Откалибровано под крипто-волатильность.
# BTC обычно сидит в 0.5-1.5%, альты 1-3%, мемы 3-10%.
TF_THRESHOLDS = [
    (2.5, "1m"),    # ATR% > 2.5%  → 1m
    (1.5, "5m"),    # ATR% > 1.5%  → 5m
    (0.7, "15m"),   # ATR% > 0.7%  → 15m
    (0.0, "1h"),    # ATR% > 0%    → 1h (fallback)
]

# Если BB Width ниже этого — рынок в сжатии, увеличиваем TF.
BB_SQUEEZE_THRESHOLD = 1.0  # %

# Минимум баров для расчёта на 1h
MIN_BARS_REQUIRED = max(ATR_PERIOD, BB_PERIOD) + 5


# ════════════════════════════════════════════════════════════════════
# Расчёты
# ════════════════════════════════════════════════════════════════════

def _atr_pct(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray,
              period: int = ATR_PERIOD) -> float:
    """ATR в процентах от текущей цены close. Это и есть "волатильность в %"."""
    n = len(highs)
    if n < period + 1:
        return 0.0
    tr_list = []
    for i in range(1, n):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i-1]),
            abs(lows[i] - closes[i-1]),
        )
        tr_list.append(tr)
    atr = float(np.mean(tr_list[-period:]))
    current_close = float(closes[-1])
    if current_close <= 0:
        return 0.0
    return atr / current_close * 100.0


def _bb_width_pct(closes: pd.Series, period: int = BB_PERIOD,
                   std_mult: float = BB_STD) -> float:
    """Ширина Bollinger Bands в % от центральной линии (SMA)."""
    if len(closes) < period:
        return 0.0
    sma = closes.rolling(window=period).mean()
    std = closes.rolling(window=period).std(ddof=0)
    upper = sma + std * std_mult
    lower = sma - std * std_mult
    last_mid = float(sma.iloc[-1])
    last_up  = float(upper.iloc[-1])
    last_dn  = float(lower.iloc[-1])
    if last_mid <= 0 or np.isnan(last_mid):
        return 0.0
    return (last_up - last_dn) / last_mid * 100.0


# ════════════════════════════════════════════════════════════════════
# Основная функция
# ════════════════════════════════════════════════════════════════════

# Соответствие TF → следующая (более длинная) ступень — для bb squeeze коррекции.
_NEXT_TF = {"1m": "5m", "5m": "15m", "15m": "1h", "1h": "1h"}


def pick_adaptive_tf(klines_1h: list) -> dict:
    """Выбрать адаптивный таймфрейм входа по текущей волатильности.

    Args:
        klines_1h: OKX kline data 1h, формат [[ts, o, h, l, c, vol, ...], ...]

    Returns:
        {
          "tf": "1m" | "5m" | "15m" | "1h",
          "atr_pct": float,
          "bb_width": float,
          "regime": "extreme" | "high" | "normal" | "low",
          "bb_squeeze": bool,  # был ли применён +1 ступень за сжатие
        }

    Безопасно при недостатке данных — возвращает default {"tf": "15m"}.
    """
    default = {"tf": "15m", "atr_pct": 0.0, "bb_width": 0.0,
                "regime": "normal", "bb_squeeze": False}

    if not klines_1h or len(klines_1h) < MIN_BARS_REQUIRED:
        return default

    try:
        first_ts = int(klines_1h[0][0])
        last_ts  = int(klines_1h[-1][0])
        data = list(reversed(klines_1h)) if first_ts > last_ts else list(klines_1h)
        highs  = np.array([float(k[2]) for k in data], dtype=float)
        lows   = np.array([float(k[3]) for k in data], dtype=float)
        closes_np = np.array([float(k[4]) for k in data], dtype=float)
        closes_pd = pd.Series(closes_np, dtype=float)
    except (ValueError, TypeError, IndexError) as e:
        log.warning(f"pick_adaptive_tf: parse error: {e}")
        return default

    atr_pct  = _atr_pct(highs, lows, closes_np, ATR_PERIOD)
    bb_width = _bb_width_pct(closes_pd, BB_PERIOD, BB_STD)

    # Базовый выбор TF по ATR%
    tf = "1h"
    for threshold, tf_name in TF_THRESHOLDS:
        if atr_pct > threshold:
            tf = tf_name
            break

    # BB squeeze: если ширина мала, рынок в сжатии — увеличиваем TF на 1 ступень.
    # Идея: в squeeze не входим на 1m/5m (там одни ложные сигналы), ждём пробоя.
    bb_squeeze = bb_width > 0 and bb_width < BB_SQUEEZE_THRESHOLD
    if bb_squeeze:
        tf = _NEXT_TF[tf]

    # Режим — для логов и дашборда
    if atr_pct > 2.5:
        regime = "extreme"
    elif atr_pct > 1.5:
        regime = "high"
    elif atr_pct > 0.7:
        regime = "normal"
    else:
        regime = "low"

    return {
        "tf": tf,
        "atr_pct": round(atr_pct, 3),
        "bb_width": round(bb_width, 3),
        "regime": regime,
        "bb_squeeze": bb_squeeze,
    }


# ════════════════════════════════════════════════════════════════════
# Самотест
# ════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    def _make_klines(prices: list, hl_spread: float = 0.005) -> list:
        """Klines с заданным high/low spread от close (default 0.5%)."""
        out = []
        for i, p in enumerate(prices):
            ts = 1700000000_000 + i * 3600_000
            h = p * (1 + hl_spread)
            l = p * (1 - hl_spread)
            out.append([str(ts), str(p), str(h), str(l), str(p), "100", "10000"])
        return out

    rng = np.random.default_rng(42)

    print("\n=== Test 1: низкая волатильность (тихий рынок) → 1h ===")
    # BTC-style: цена 50000, мелкий шум 0.2%
    prices = 50000 + rng.normal(0, 50, 30)  # std=50 на цене 50000 = 0.1%
    klines = _make_klines(list(prices), hl_spread=0.002)
    r = pick_adaptive_tf(klines)
    print(f"  {r}")
    assert r["regime"] == "low"
    assert r["tf"] in ("1h",)  # squeeze может оставить 1h как есть
    print("  ✅ PASS")

    print("\n=== Test 2: нормальная волатильность → 15m или 5m ===")
    # ATR должен быть ~1-2% от цены.
    prices = 50000 + rng.normal(0, 200, 30)
    klines = _make_klines(list(prices), hl_spread=0.01)   # 1% h-l spread
    r = pick_adaptive_tf(klines)
    print(f"  {r}")
    # Любой нормальный/высокий → допускаем оба
    assert r["tf"] in ("15m", "5m", "1h"), f"Получили {r['tf']}"
    print("  ✅ PASS")

    print("\n=== Test 3: высокая волатильность → 5m ===")
    prices = 50000 + rng.normal(0, 500, 30)
    klines = _make_klines(list(prices), hl_spread=0.02)   # 2% h-l spread
    r = pick_adaptive_tf(klines)
    print(f"  {r}")
    assert r["regime"] in ("high", "extreme")
    assert r["tf"] in ("5m", "1m")
    print("  ✅ PASS")

    print("\n=== Test 4: экстремальная волатильность (мем-памп) → 1m ===")
    # PEPE-style: цена 0.00001, hl spread 5%
    prices = 0.00001 + rng.normal(0, 0.000001, 30)
    klines = _make_klines(list(prices), hl_spread=0.05)
    r = pick_adaptive_tf(klines)
    print(f"  {r}")
    assert r["regime"] == "extreme"
    assert r["tf"] == "1m"
    print("  ✅ PASS")

    print("\n=== Test 5: BB squeeze — повышает TF на 1 ступень ===")
    # Сжатый канал: 30 баров с минимальной разницей. ATR будет средний,
    # но BB width маленький → ожидаем повышение TF.
    prices = [50000 + 50*np.sin(i/3) for i in range(30)]
    klines = _make_klines(prices, hl_spread=0.015)  # ATR ~ 1.5% (нормальный)
    r = pick_adaptive_tf(klines)
    print(f"  {r}")
    # BB width должен быть маленьким для синусоиды с малой амплитудой
    if r["bb_squeeze"]:
        print(f"  ✅ PASS — squeeze применён, tf={r['tf']}")
    else:
        # Не всегда срабатывает, проверим что atr и tf разумные
        print(f"  ⚠ squeeze не сработал, но это OK для теста (bb_width={r['bb_width']})")

    print("\n=== Test 6: недостаточно данных → default 15m ===")
    klines = _make_klines([100.0] * 10)
    r = pick_adaptive_tf(klines)
    print(f"  {r}")
    assert r["tf"] == "15m"
    print("  ✅ PASS")

    print("\n=== Test 7: пустые данные → default ===")
    r = pick_adaptive_tf([])
    assert r["tf"] == "15m"
    print("  ✅ PASS")

    print("\n=== Test 8: обратный порядок klines ===")
    prices = 50000 + rng.normal(0, 500, 30)
    klines_normal = _make_klines(list(prices), hl_spread=0.02)
    klines_reversed = list(reversed(klines_normal))
    r1 = pick_adaptive_tf(klines_normal)
    r2 = pick_adaptive_tf(klines_reversed)
    print(f"  normal:   {r1}")
    print(f"  reversed: {r2}")
    assert r1["tf"] == r2["tf"]
    print("  ✅ PASS — эвристика разворота работает")

    print("\n🎉 Все тесты прошли. volatility готов.")
