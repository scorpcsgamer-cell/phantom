"""
divergence.py — RSI / MACD дивергенции для PHANTOM (Фаза 1, Поставка 3)
==========================================================================

Задача: после того как trend_filter сказал "тренд up, long разрешён" и
fib_engine сказал "цена на 0.618" — этот модуль подтверждает или отклоняет
вход через дивергенцию.

Что такое КЛАССИЧЕСКАЯ дивергенция (используется здесь):
  • Bullish (сигнал к long, разворот вверх):
      Цена:     low2 < low1 (новый минимум ниже предыдущего)
      RSI/MACD: low2 > low1 (минимум на индикаторе ВЫШЕ предыдущего)
      → цена "выдыхается" на падении, медведи теряют силу.

  • Bearish (сигнал к short, разворот вниз):
      Цена:     high2 > high1 (новый максимум выше предыдущего)
      RSI/MACD: high2 < high1 (максимум на индикаторе НИЖЕ предыдущего)
      → цена "выдыхается" на росте, быки теряют силу.

И-логика (по спеке Сергея):
  RSI div ≠ "none" AND MACD div ≠ "none" AND обе в одну сторону
  → confirmed_direction (long / short)
  Иначе → None (без подтверждения вход блокируется).

API:
    result = check_divergences(klines, lookback=30)
    # → {
    #   "rsi_div":  "bullish" | "bearish" | "none",
    #   "macd_div": "bullish" | "bearish" | "none",
    #   "confirmed": "long" | "short" | None,
    #   "details": {...},
    # }
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd

log = logging.getLogger("divergence")


# ════════════════════════════════════════════════════════════════════
# Константы
# ════════════════════════════════════════════════════════════════════

RSI_PERIOD = 14
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

# Окно поиска экстремумов — последние N баров. Реалистично 30 для 15m / 1h:
# покрывает ~7-30 часов истории.
DEFAULT_LOOKBACK = 30

# Pivot lookback для поиска локальных экстремумов на индикаторах.
# Меньше чем в fib_engine (5), потому что индикаторы более "шумные".
INDICATOR_PIVOT_LB = 3

# Минимум баров для расчёта: MACD slow + signal + lookback + запас.
MIN_BARS_REQUIRED = MACD_SLOW + MACD_SIGNAL + DEFAULT_LOOKBACK + 20


# ════════════════════════════════════════════════════════════════════
# Расчёт RSI и MACD
# ════════════════════════════════════════════════════════════════════

def _rsi(closes: pd.Series, period: int = RSI_PERIOD) -> pd.Series:
    """Стандартный RSI через Wilder smoothing (как в TradingView)."""
    delta = closes.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    # Wilder = EMA с alpha = 1/period
    avg_gain = gain.ewm(alpha=1.0/period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1.0/period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi.fillna(50.0)   # начальные NaN → нейтральные 50


def _macd_histogram(closes: pd.Series,
                     fast: int = MACD_FAST, slow: int = MACD_SLOW,
                     signal: int = MACD_SIGNAL) -> pd.Series:
    """MACD histogram = MACD - signal line. Именно гистограмму используем
    для дивергенций, потому что она лучше показывает momentum."""
    ema_fast = closes.ewm(span=fast,  adjust=False, min_periods=fast).mean()
    ema_slow = closes.ewm(span=slow,  adjust=False, min_periods=slow).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False,
                                  min_periods=signal).mean()
    return (macd_line - signal_line).fillna(0.0)


# ════════════════════════════════════════════════════════════════════
# Pivot detection на 1D массиве (упрощённый из fib_engine)
# ════════════════════════════════════════════════════════════════════

def _find_pivots_1d(values: np.ndarray, lookback: int = INDICATOR_PIVOT_LB,
                     ) -> tuple[list, list]:
    """Найти pivot highs и lows на 1D массиве (для индикаторов).

    Используем тот же подход что в fib_engine: значение pivot high, если
    оно >= всех слева и > всех справа в окне lookback.

    Returns:
        (pivot_highs, pivot_lows) — списки (index, value), отсортировано по idx.
    """
    n = len(values)
    pivot_highs = []
    pivot_lows = []
    if n < 2 * lookback + 1:
        return pivot_highs, pivot_lows

    for i in range(lookback, n - lookback):
        left  = values[i - lookback : i]
        right = values[i + 1 : i + lookback + 1]
        if values[i] >= left.max() and values[i] > right.max():
            pivot_highs.append((i, float(values[i])))
        if values[i] <= left.min() and values[i] < right.min():
            pivot_lows.append((i, float(values[i])))

    return pivot_highs, pivot_lows


# ════════════════════════════════════════════════════════════════════
# Детекция дивергенции по одному индикатору
# ════════════════════════════════════════════════════════════════════

@dataclass
class DivResult:
    """Результат проверки дивергенции по одному индикатору."""
    direction: str          # "bullish" / "bearish" / "none"
    price_idx_1: int = -1
    price_idx_2: int = -1
    price_val_1: float = 0.0
    price_val_2: float = 0.0
    ind_val_1: float = 0.0
    ind_val_2: float = 0.0

    def to_dict(self) -> dict:
        if self.direction == "none":
            return {"direction": "none"}
        return {
            "direction":   self.direction,
            "price_idx_1": self.price_idx_1,
            "price_idx_2": self.price_idx_2,
            "price_val_1": round(self.price_val_1, 8),
            "price_val_2": round(self.price_val_2, 8),
            "ind_val_1":   round(self.ind_val_1, 6),
            "ind_val_2":   round(self.ind_val_2, 6),
        }


def _check_classic_divergence(prices_high: np.ndarray, prices_low: np.ndarray,
                                indicator: np.ndarray,
                                lookback: int = DEFAULT_LOOKBACK,
                                ) -> DivResult:
    """Проверить классическую дивергенцию между ценой и индикатором.

    Алгоритм:
      1. Сужаем массивы до последних `lookback` баров (если данных больше).
      2. Находим pivot highs и lows на ЦЕНЕ (отдельно highs для bearish,
         lows для bullish).
      3. Берём последние две пары:
         • Для bullish: два последних price_low, сравниваем индикатор в них.
         • Для bearish: два последних price_high, сравниваем индикатор в них.
      4. Если соответствует определению классической дивергенции — возвращаем.

    Returns:
        DivResult с direction = "bullish" / "bearish" / "none".
    """
    n = len(prices_high)
    if n < 2 * INDICATOR_PIVOT_LB + 4:
        return DivResult("none")

    # Сужаем до последних lookback баров (но сохраняем абсолютные индексы).
    start = max(0, n - lookback)
    win_high = np.array([prices_high[i] for i in range(start, n)])
    win_low  = np.array([prices_low[i]  for i in range(start, n)])

    # Pivots для bearish — на массиве highs. Pivots для bullish — на массиве lows.
    # Каждый _find_pivots_1d возвращает (highs, lows), но мы используем:
    # из работы на highs — только pivot_highs, из работы на lows — только pivot_lows.
    p_highs_rel, _ = _find_pivots_1d(win_high, lookback=INDICATOR_PIVOT_LB)
    _, p_lows_rel  = _find_pivots_1d(win_low,  lookback=INDICATOR_PIVOT_LB)
    p_highs_abs = [(i + start, v) for i, v in p_highs_rel]
    p_lows_abs  = [(i + start, v) for i, v in p_lows_rel]

    # === Bullish divergence: price low2 < low1, indicator low2 > low1 ===
    if len(p_lows_abs) >= 2:
        # Две последние pivot low (в хронологическом порядке)
        (idx1, low1), (idx2, low2) = p_lows_abs[-2], p_lows_abs[-1]
        if low2 < low1:   # цена сделала новый минимум
            # Сравниваем индикатор в тех же точках
            ind1 = float(indicator[idx1])
            ind2 = float(indicator[idx2])
            if ind2 > ind1:
                return DivResult(
                    direction="bullish",
                    price_idx_1=idx1, price_idx_2=idx2,
                    price_val_1=low1, price_val_2=low2,
                    ind_val_1=ind1, ind_val_2=ind2,
                )

    # === Bearish divergence: price high2 > high1, indicator high2 < high1 ===
    if len(p_highs_abs) >= 2:
        (idx1, high1), (idx2, high2) = p_highs_abs[-2], p_highs_abs[-1]
        if high2 > high1:
            ind1 = float(indicator[idx1])
            ind2 = float(indicator[idx2])
            if ind2 < ind1:
                return DivResult(
                    direction="bearish",
                    price_idx_1=idx1, price_idx_2=idx2,
                    price_val_1=high1, price_val_2=high2,
                    ind_val_1=ind1, ind_val_2=ind2,
                )

    return DivResult("none")


# ════════════════════════════════════════════════════════════════════
# Главная функция: проверка обеих дивергенций с И-логикой
# ════════════════════════════════════════════════════════════════════

def check_divergences(klines: list, lookback: int = DEFAULT_LOOKBACK) -> dict:
    """Проверить классические дивергенции RSI и MACD.

    Args:
        klines: OKX kline data [[ts, o, h, l, c, vol, ...], ...]
        lookback: окно поиска экстремумов в барах (default 30)

    Returns:
        {
          "rsi_div":   "bullish" | "bearish" | "none",
          "macd_div":  "bullish" | "bearish" | "none",
          "confirmed": "long" | "short" | None,   # И-логика обеих
          "details": {
              "rsi":  {...},
              "macd": {...},
              "bars_used": int,
          }
        }
    """
    empty = {
        "rsi_div": "none", "macd_div": "none", "confirmed": None,
        "details": {"rsi": {}, "macd": {}, "bars_used": 0}
    }

    if not klines or len(klines) < MIN_BARS_REQUIRED:
        return empty

    # Парсинг + разворот
    try:
        first_ts = int(klines[0][0])
        last_ts  = int(klines[-1][0])
        data = list(reversed(klines)) if first_ts > last_ts else list(klines)
        highs  = np.array([float(k[2]) for k in data], dtype=float)
        lows   = np.array([float(k[3]) for k in data], dtype=float)
        closes = pd.Series([float(k[4]) for k in data], dtype=float)
    except (ValueError, TypeError, IndexError) as e:
        log.warning(f"check_divergences: parse error: {e}")
        return empty

    n = len(closes)

    # Расчёт индикаторов
    rsi  = _rsi(closes, RSI_PERIOD)
    macd = _macd_histogram(closes, MACD_FAST, MACD_SLOW, MACD_SIGNAL)

    rsi_arr  = rsi.values
    macd_arr = macd.values

    rsi_div  = _check_classic_divergence(highs, lows, rsi_arr,  lookback)
    macd_div = _check_classic_divergence(highs, lows, macd_arr, lookback)

    # И-логика: confirmed только если ОБА показывают одно направление
    confirmed = None
    if rsi_div.direction == "bullish" and macd_div.direction == "bullish":
        confirmed = "long"
    elif rsi_div.direction == "bearish" and macd_div.direction == "bearish":
        confirmed = "short"

    return {
        "rsi_div":   rsi_div.direction,
        "macd_div":  macd_div.direction,
        "confirmed": confirmed,
        "details": {
            "rsi":  rsi_div.to_dict(),
            "macd": macd_div.to_dict(),
            "bars_used": n,
        }
    }


# ════════════════════════════════════════════════════════════════════
# Самотест
# ════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    def _make_klines(prices: list, ts_start: int = 1700000000_000,
                      ts_step_ms: int = 3600_000) -> list:
        """Klines в OKX формате: [ts, o, h, l, c, vol, volCcy]"""
        out = []
        for i, p in enumerate(prices):
            ts = ts_start + i * ts_step_ms
            out.append([str(ts), str(p), str(p*1.005), str(p*0.995),
                        str(p), "100", "10000"])
        return out

    # Bullish дивергенция: два явных минимума в окне.
    # Второй минимум ниже первого (price low2 < low1),
    # но за это время цена сильно упала, потом отскочила, потом упала меньше →
    # RSI на втором минимуме выше (выдохся импульс падения).
    print("\n=== Test 1: bullish divergence (цена ↓, индикатор ↑) ===")
    # Структура:
    #   warm-up 80 баров на 100, чтобы MACD устаканился
    #   спад 100→85 (резкий, 15 баров) → low #1
    #   отскок 85→95 (10 баров)
    #   спад 95→82 (плавный, 20 баров) → low #2 (ниже)
    #   подтверждение 82→84 (5 баров)
    prices = []
    prices += [100.0] * 80
    prices += list(np.linspace(100, 85, 15))   # резкий спад → low1 ~85 @ idx ~94
    prices += list(np.linspace(85, 95, 10))    # отскок
    prices += list(np.linspace(95, 82, 20))    # плавный спад → low2 ~82 @ idx ~124
    prices += list(np.linspace(82, 84, 5))     # подтверждение
    klines = _make_klines(prices)
    # lookback=60 покрывает оба экстремума (от idx 70 до конца)
    r = check_divergences(klines, lookback=60)
    print(f"  rsi_div={r['rsi_div']}, macd_div={r['macd_div']}, "
          f"confirmed={r['confirmed']}")
    print(f"  RSI details: {r['details']['rsi']}")
    print(f"  MACD details: {r['details']['macd']}")
    # Хотя бы один индикатор должен показать bullish
    assert r["rsi_div"] == "bullish" or r["macd_div"] == "bullish", \
        f"Ожидали bullish хотя бы в одном индикаторе"
    print("  ✅ PASS")

    print("\n=== Test 2: bearish divergence (цена ↑, индикатор ↓) ===")
    prices = []
    prices += [100.0] * 80
    prices += list(np.linspace(100, 115, 15))   # резкий рост → high1
    prices += list(np.linspace(115, 105, 10))   # откат
    prices += list(np.linspace(105, 118, 20))   # плавный рост → high2
    prices += list(np.linspace(118, 116, 5))
    klines = _make_klines(prices)
    r = check_divergences(klines, lookback=60)
    print(f"  rsi_div={r['rsi_div']}, macd_div={r['macd_div']}, "
          f"confirmed={r['confirmed']}")
    print(f"  RSI: {r['details']['rsi']}")
    print(f"  MACD: {r['details']['macd']}")
    assert r["rsi_div"] == "bearish" or r["macd_div"] == "bearish"
    print("  ✅ PASS")

    print("\n=== Test 3: чистый тренд без дивергенции (плавный рост) ===")
    # Цена и индикатор оба растут — дивергенции нет
    prices = list(np.linspace(100, 130, 100))
    klines = _make_klines(prices)
    r = check_divergences(klines, lookback=30)
    print(f"  rsi_div={r['rsi_div']}, macd_div={r['macd_div']}, "
          f"confirmed={r['confirmed']}")
    assert r["confirmed"] is None  # не должно быть подтверждения
    print("  ✅ PASS — нет ложного срабатывания на чистом тренде")

    print("\n=== Test 4: недостаточно данных → none ===")
    klines = _make_klines([100.0] * 20)
    r = check_divergences(klines)
    assert r["rsi_div"] == "none"
    assert r["macd_div"] == "none"
    assert r["confirmed"] is None
    print("  ✅ PASS")

    print("\n=== Test 5: пустые данные → none (не падаем) ===")
    r = check_divergences([])
    assert r["confirmed"] is None
    print("  ✅ PASS")

    print("\n=== Test 6: И-логика — конфликт rsi/macd → confirmed=None ===")
    # Если rsi и macd показывают разные направления — confirmed=None.
    # Проверяем это явно: сначала собираем результаты, потом валидируем логику.
    prices = []
    prices += [100.0] * 80
    prices += list(np.linspace(100, 85, 15))
    prices += list(np.linspace(85, 95, 10))
    prices += list(np.linspace(95, 82, 20))
    prices += [83.0] * 5
    klines = _make_klines(prices)
    r = check_divergences(klines, lookback=60)
    print(f"  rsi_div={r['rsi_div']}, macd_div={r['macd_div']}, "
          f"confirmed={r['confirmed']}")
    # Валидация И-логики:
    if r["rsi_div"] == r["macd_div"] == "bullish":
        assert r["confirmed"] == "long"
    elif r["rsi_div"] == r["macd_div"] == "bearish":
        assert r["confirmed"] == "short"
    else:
        assert r["confirmed"] is None, \
            f"И-логика нарушена: rsi={r['rsi_div']} macd={r['macd_div']} но confirmed={r['confirmed']}"
    print("  ✅ PASS — И-логика работает корректно")

    print("\n=== Test 7: обратный порядок klines (OKX дефолт) ===")
    prices = []
    prices += [100.0] * 80
    prices += list(np.linspace(100, 85, 15))
    prices += list(np.linspace(85, 95, 10))
    prices += list(np.linspace(95, 82, 20))
    prices += list(np.linspace(82, 84, 5))
    klines_normal = _make_klines(prices)
    klines_reversed = list(reversed(klines_normal))
    r1 = check_divergences(klines_normal, lookback=60)
    r2 = check_divergences(klines_reversed, lookback=60)
    print(f"  normal:   rsi={r1['rsi_div']}, macd={r1['macd_div']}")
    print(f"  reversed: rsi={r2['rsi_div']}, macd={r2['macd_div']}")
    assert r1["rsi_div"] == r2["rsi_div"]
    assert r1["macd_div"] == r2["macd_div"]
    print("  ✅ PASS — эвристика разворота работает корректно")

    print("\n🎉 Все 7 тестов прошли. divergence готов.")
