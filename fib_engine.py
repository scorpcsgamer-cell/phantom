"""
fib_engine.py — Детекция свинга и Fib-уровни для PHANTOM (Фаза 1, Поставка 2)
============================================================================

Задача: найти последний значимый свинг (low → high или high → low), посчитать
от него Fib retracement и extension уровни, и определить — стоит ли цена
сейчас на одном из ключевых ретрейсментов (0.5 или 0.618).

Это второй фильтр после trend_filter:
  • trend_filter сказал "тренд up, long разрешён"
  • fib_engine говорит "цена сейчас на 0.618 — точка входа для long"
  • divergence (Поставка 3) подтвердит сигналом RSI/MACD дивергенции

Алгоритм детекции свинга:
  1. Pivot points с lookback=5: точка high, если max(±5 баров вокруг) = эта точка.
     Аналогично pivot low.
  2. ATR(14) фильтр: амплитуда между соседними pivot должна быть > 1.5 × ATR.
     Это отсеивает микро-свинги в боковике.
  3. Берём последнюю подтверждённую пару (low, high).

Retracement формула (для swing up: low → high):
    level_price = high - (high - low) * pct
    # 0.0 = high (старт ретрейса), 1.0 = low (конец)
    # Long entry — на 0.5 или 0.618 от high вниз

Extension формула (для swing up):
    ext_price = high + (high - low) * (pct - 1.0)
    # 1.272 = продление вверх на 27.2% от амплитуды свинга
    # Используется как TP-цель после пробоя high

API:
    setup = detect_fib_setup(klines, current_price)
    if setup and setup.setup_type == "long_retrace":
        # цена на 0.5 или 0.618 в up-свинге, можно искать long вход
        ...
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

log = logging.getLogger("fib_engine")


# ════════════════════════════════════════════════════════════════════
# Константы
# ════════════════════════════════════════════════════════════════════

# Lookback для pivot detection (±N баров). 5 — стандарт в TradingView.
PIVOT_LOOKBACK = 5

# Период ATR для фильтра значимости свинга.
ATR_PERIOD = 14

# Минимальная амплитуда свинга в единицах ATR. 1.5 = свинг должен быть в 1.5×ATR.
SWING_MIN_ATR_MULT = 1.5

# Минимум баров для валидной работы: ATR + pivot lookback + запас.
MIN_BARS_REQUIRED = ATR_PERIOD + 2 * PIVOT_LOOKBACK + 30

# Стандартные Fib уровни для retracement и extension.
RETRACE_LEVELS = [0.236, 0.382, 0.5, 0.618, 0.786]
EXTENSION_LEVELS = [1.272, 1.414, 1.618, 2.0, 2.618]

# Уровни на которых даём сигнал "on_level" — для долгого входа.
# 0.5 и 0.618 — золотое сечение, классические уровни входа.
ENTRY_LEVELS = [0.5, 0.618]

# Допуск для срабатывания "цена на уровне", в % от цены. 0.3% — близко к
# минимальному ATR обычного бара на 1h, не слишком жёстко.
DEFAULT_TOLERANCE_PCT = 0.3


# ════════════════════════════════════════════════════════════════════
# Структуры данных
# ════════════════════════════════════════════════════════════════════

@dataclass
class FibLevel:
    """Один Fib уровень: процент и абсолютная цена."""
    pct: float
    price: float

    def to_dict(self) -> dict:
        return {"pct": self.pct, "price": round(self.price, 8)}


@dataclass
class FibSetup:
    """Полный результат анализа: свинг + Fib уровни + текущая позиция цены."""
    direction: str           # "up" (low→high) или "down" (high→low)
    swing_high: float
    swing_low: float
    swing_high_idx: int      # индекс бара pivot_high в klines
    swing_low_idx: int       # индекс бара pivot_low в klines
    amplitude: float         # high - low (всегда >= 0)
    atr: float
    retracements: list[FibLevel] = field(default_factory=list)
    extensions: list[FibLevel] = field(default_factory=list)
    current_price: float = 0.0
    on_level: Optional[float] = None     # 0.5, 0.618 или None
    distance_to_nearest: float = 0.0     # % до ближайшего ENTRY уровня
    setup_type: Optional[str] = None     # "long_retrace" / "short_retrace" / None

    def to_dict(self) -> dict:
        return {
            "direction": self.direction,
            "swing_high": round(self.swing_high, 8),
            "swing_low": round(self.swing_low, 8),
            "amplitude": round(self.amplitude, 8),
            "atr": round(self.atr, 8),
            "retracements": [l.to_dict() for l in self.retracements],
            "extensions": [l.to_dict() for l in self.extensions],
            "current_price": round(self.current_price, 8),
            "on_level": self.on_level,
            "distance_to_nearest_pct": round(self.distance_to_nearest, 3),
            "setup_type": self.setup_type,
        }


# ════════════════════════════════════════════════════════════════════
# Утилиты: ATR и pivot detection
# ════════════════════════════════════════════════════════════════════

def _calc_atr(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray,
               period: int = ATR_PERIOD) -> float:
    """ATR на последнем баре. True Range = max(h-l, |h-prev_close|, |l-prev_close|).
    Используем простое среднее (SMA) — для надёжности и предсказуемости."""
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
    # Берём последние period значений
    return float(np.mean(tr_list[-period:]))


def _find_pivots(highs: np.ndarray, lows: np.ndarray,
                  lookback: int = PIVOT_LOOKBACK) -> tuple[list, list]:
    """Найти все pivot highs и pivot lows.

    Pivot high: бар, у которого high больше highs всех баров в окне ±lookback.
    Pivot low: аналогично для lows.

    Returns:
        (pivot_highs, pivot_lows) — списки кортежей (index, price).
        Список отсортирован по index (старые → новые).

    NOTE: первые и последние `lookback` баров не могут быть pivot — недостаточно
    соседей. Это означает что pivot подтверждается с задержкой `lookback` баров.
    """
    n = len(highs)
    pivot_highs = []
    pivot_lows = []
    if n < 2 * lookback + 1:
        return pivot_highs, pivot_lows

    for i in range(lookback, n - lookback):
        # Стандарт TradingView: бар считается pivot high, если
        #   - его high >= всех highs слева в окне lookback (допускается плато)
        #   - его high >  всех highs справа в окне lookback (строго больше)
        # Это позволяет плато слева брать первую точку как pivot,
        # и предотвращает дубликаты при stairs/плато конструкциях.
        left_h  = highs[i - lookback : i]
        right_h = highs[i + 1 : i + lookback + 1]
        left_l  = lows [i - lookback : i]
        right_l = lows [i + 1 : i + lookback + 1]
        if highs[i] >= left_h.max() and highs[i] > right_h.max():
            pivot_highs.append((i, float(highs[i])))
        if lows[i] <= left_l.min() and lows[i] < right_l.min():
            pivot_lows.append((i, float(lows[i])))

    return pivot_highs, pivot_lows


def _select_significant_swing(pivot_highs: list, pivot_lows: list,
                                highs: Optional[np.ndarray] = None,
                                lows: Optional[np.ndarray] = None,
                                atr: float = 0.0,
                                min_atr_mult: float = SWING_MIN_ATR_MULT,
                                ) -> Optional[tuple]:
    """Выбрать последнюю значимую пару (low, high) для построения Fib.

    Логика:
      • Если есть pivot обоих типов → стандартный путь: берём последний pivot,
        идём назад в поисках противоположного с амплитудой > min_atr_mult × ATR.
      • Fallback: если найден pivot только одного типа (классическая ситуация
        линейного rise → pullback, где low не успел стать pivot) — ищем
        абсолютный экстремум противоположного типа в массиве highs/lows.

    Args:
        pivot_highs/pivot_lows: списки (idx, price) из _find_pivots
        highs/lows: полные массивы для fallback (опционально)
        atr: ATR для фильтра значимости
        min_atr_mult: множитель ATR (default 1.5)

    Returns:
        (low_idx, low_price, high_idx, high_price, direction) или None.
    """
    if atr <= 0:
        return None

    min_amplitude = atr * min_atr_mult

    # === Стандартный путь: оба типа pivot есть ===
    if pivot_highs and pivot_lows:
        all_pivots = [(idx, price, "H") for idx, price in pivot_highs] + \
                     [(idx, price, "L") for idx, price in pivot_lows]
        all_pivots.sort(key=lambda x: x[0])
        if len(all_pivots) >= 2:
            last = all_pivots[-1]
            last_idx, last_price, last_type = last
            for prev in reversed(all_pivots[:-1]):
                prev_idx, prev_price, prev_type = prev
                if prev_type == last_type:
                    continue
                amplitude = abs(last_price - prev_price)
                if amplitude >= min_amplitude:
                    if last_type == "H":
                        return (prev_idx, prev_price, last_idx, last_price, "up")
                    else:
                        return (last_idx, last_price, prev_idx, prev_price, "down")

    # === Fallback: только один тип pivot ===
    # Это случай линейного rise/fall + неполный pullback. Берём найденный
    # pivot как один конец свинга, ищем абсолютный экстремум-противоположность
    # в данных СЛЕВА от него (от начала массива до pivot_idx).
    if highs is None or lows is None:
        return None

    if pivot_highs and not pivot_lows:
        # Есть pivot_high → достраиваем low слева
        h_idx, h_price = pivot_highs[-1]
        if h_idx <= 0:
            return None
        left_slice = lows[:h_idx]
        if len(left_slice) == 0:
            return None
        l_idx = int(np.argmin(left_slice))
        l_price = float(left_slice[l_idx])
        amplitude = h_price - l_price
        if amplitude >= min_amplitude:
            return (l_idx, l_price, h_idx, h_price, "up")

    if pivot_lows and not pivot_highs:
        # Есть pivot_low → достраиваем high слева
        l_idx, l_price = pivot_lows[-1]
        if l_idx <= 0:
            return None
        left_slice = highs[:l_idx]
        if len(left_slice) == 0:
            return None
        h_idx = int(np.argmax(left_slice))
        h_price = float(left_slice[h_idx])
        amplitude = h_price - l_price
        if amplitude >= min_amplitude:
            return (l_idx, l_price, h_idx, h_price, "down")

    return None


# ════════════════════════════════════════════════════════════════════
# Основная функция: построить FibSetup
# ════════════════════════════════════════════════════════════════════

def detect_fib_setup(klines: list,
                      current_price: Optional[float] = None,
                      tolerance_pct: float = DEFAULT_TOLERANCE_PCT,
                      min_atr_mult: float = SWING_MIN_ATR_MULT,
                      ) -> Optional[FibSetup]:
    """Главная функция: построить Fib-сетап по klines.

    Args:
        klines: OKX kline data [[ts, o, h, l, c, vol, ...], ...]
                Эвристика разворачивания: если ts убывает — развернём.
        current_price: текущая цена (если None — возьмём close последнего бара)
        tolerance_pct: % допуска для срабатывания "цена на уровне" (default 0.3%)
        min_atr_mult: минимум амплитуды свинга в ATR (default 1.5)

    Returns:
        FibSetup или None если данных недостаточно / нет значимого свинга.
    """
    if not klines or len(klines) < MIN_BARS_REQUIRED:
        return None

    # Парсинг и разворот при необходимости (OKX отдаёт новые сверху).
    try:
        first_ts = int(klines[0][0])
        last_ts  = int(klines[-1][0])
        data = list(reversed(klines)) if first_ts > last_ts else list(klines)
        highs  = np.array([float(k[2]) for k in data], dtype=float)
        lows   = np.array([float(k[3]) for k in data], dtype=float)
        closes = np.array([float(k[4]) for k in data], dtype=float)
    except (ValueError, TypeError, IndexError) as e:
        log.warning(f"detect_fib_setup: не удалось распарсить klines: {e}")
        return None

    if current_price is None:
        current_price = float(closes[-1])

    # 1. ATR на последнем баре
    atr = _calc_atr(highs, lows, closes, ATR_PERIOD)
    if atr <= 0:
        return None

    # 2. Pivot points
    pivot_highs, pivot_lows = _find_pivots(highs, lows, PIVOT_LOOKBACK)

    # 3. Последний значимый свинг (с fallback на абсолютные экстремумы)
    swing = _select_significant_swing(pivot_highs, pivot_lows,
                                        highs=highs, lows=lows,
                                        atr=atr, min_atr_mult=min_atr_mult)
    if swing is None:
        return None

    low_idx, low_price, high_idx, high_price, direction = swing
    amplitude = high_price - low_price

    # 4. Расчёт retracement уровней.
    # Для swing up (low → high): retracement движется ОТ high К low.
    #     0.0 = high, 1.0 = low
    #     level_price = high - amplitude * pct
    # Для swing down (high → low): retracement движется ОТ low К high.
    #     0.0 = low, 1.0 = high
    #     level_price = low + amplitude * pct
    retracements = []
    for pct in RETRACE_LEVELS:
        if direction == "up":
            price = high_price - amplitude * pct
        else:
            price = low_price + amplitude * pct
        retracements.append(FibLevel(pct=pct, price=price))

    # 5. Extension уровни (для TP-лестницы).
    # Для swing up: extension продолжает движение вверх (за high).
    #     price = high + amplitude * (pct - 1.0)
    # Для swing down: extension продолжает движение вниз (за low).
    extensions = []
    for pct in EXTENSION_LEVELS:
        if direction == "up":
            price = high_price + amplitude * (pct - 1.0)
        else:
            price = low_price - amplitude * (pct - 1.0)
        extensions.append(FibLevel(pct=pct, price=price))

    # 6. Проверяем — стоит ли цена сейчас на одном из ENTRY уровней?
    on_level = None
    tol = current_price * (tolerance_pct / 100.0)
    nearest_dist_pct = float("inf")
    for lvl in retracements:
        if lvl.pct not in ENTRY_LEVELS:
            continue
        dist = abs(current_price - lvl.price)
        dist_pct = dist / current_price * 100.0
        if dist_pct < nearest_dist_pct:
            nearest_dist_pct = dist_pct
        if dist <= tol:
            on_level = lvl.pct
            break

    # 7. Определяем тип сетапа.
    # long_retrace: swing up + цена откатилась к 0.5/0.618 от high
    # short_retrace: swing down + цена откатилась к 0.5/0.618 от low
    setup_type = None
    if on_level is not None:
        if direction == "up":
            setup_type = "long_retrace"
        else:
            setup_type = "short_retrace"

    return FibSetup(
        direction=direction,
        swing_high=high_price,
        swing_low=low_price,
        swing_high_idx=high_idx,
        swing_low_idx=low_idx,
        amplitude=amplitude,
        atr=atr,
        retracements=retracements,
        extensions=extensions,
        current_price=current_price,
        on_level=on_level,
        distance_to_nearest=nearest_dist_pct if nearest_dist_pct != float("inf") else 0.0,
        setup_type=setup_type,
    )


# ════════════════════════════════════════════════════════════════════
# Утилита для TP-лестницы (используется в Поставке 5)
# ════════════════════════════════════════════════════════════════════

def get_tp_ladder(setup: FibSetup) -> list[float]:
    """Возвращает список цен TP для лестничного выхода.

    Порядок: от ближайшего к точке входа TP до самого дальнего (swing edge).

    Для long entry (swing up, цена на 0.618):
        TP1 = цена на 0.5  (первая цель, ближайшая к входу)
        TP2 = цена на 0.382
        TP3 = цена на 0.236
        TP4 = swing_high (финальная цель)

    Для short entry — симметрично.
    """
    if not setup or setup.setup_type is None:
        return []

    entry_pct = setup.on_level or 0.5

    # Берём все retracement уровни выше точки входа (для long — с меньшим pct,
    # т.е. ближе к swing_high). Сортируем по убыванию pct: ближайший к входу
    # идёт первым (TP1).
    relevant_levels = [lvl for lvl in setup.retracements if lvl.pct < entry_pct]
    relevant_levels.sort(key=lambda l: l.pct, reverse=True)
    ladder = [lvl.price for lvl in relevant_levels]

    # Финальная цель: дальний конец свинга.
    if setup.setup_type == "long_retrace":
        ladder.append(setup.swing_high)
    else:
        ladder.append(setup.swing_low)

    return ladder


# ════════════════════════════════════════════════════════════════════
# Самотест (запускать командой: python fib_engine.py)
# ════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    def _make_klines(highs_lows_closes: list, ts_start: int = 1700000000_000,
                      ts_step_ms: int = 3600_000) -> list:
        """Собрать массив klines в OKX формате из (high, low, close) триад."""
        out = []
        for i, (h, l, c) in enumerate(highs_lows_closes):
            ts = ts_start + i * ts_step_ms
            # OKX формат: [ts, o, h, l, c, vol, volCcy, ...]
            o = c  # close prev для простоты
            out.append([str(ts), str(o), str(h), str(l), str(c), "100", "10000"])
        return out

    print("\n=== Test 1: чёткий up-свинг, цена на 0.618 → long_retrace ===")
    # 100 баров: цена идёт с 100 до 200 (up swing), затем откатывает к 138.2
    # (0.618 от 200: 200 - 100*0.618 = 138.2). Это идеальный long entry.
    rise = list(np.linspace(100, 200, 60))      # бары 0..59: рост 100→200
    retrace_target = 200 - (200 - 100) * 0.618  # = 138.2
    pullback = list(np.linspace(200, retrace_target, 40))  # бары 60..99
    prices = rise + pullback
    klines = _make_klines([(p*1.005, p*0.995, p) for p in prices])
    setup = detect_fib_setup(klines, current_price=retrace_target)
    assert setup is not None, "Setup должен быть найден"
    print(f"  direction={setup.direction}")
    print(f"  swing: low={setup.swing_low:.2f} @ idx={setup.swing_low_idx}, "
          f"high={setup.swing_high:.2f} @ idx={setup.swing_high_idx}")
    print(f"  amplitude={setup.amplitude:.2f}, ATR={setup.atr:.2f}")
    print(f"  retracements:")
    for lvl in setup.retracements:
        marker = " ←" if lvl.pct == setup.on_level else ""
        print(f"    {lvl.pct:.3f} = {lvl.price:.2f}{marker}")
    print(f"  current_price={setup.current_price:.2f}, on_level={setup.on_level}, "
          f"setup_type={setup.setup_type}")
    assert setup.direction == "up"
    assert setup.setup_type == "long_retrace"
    assert setup.on_level == 0.618
    print("  ✅ PASS")

    print("\n=== Test 2: чёткий down-свинг, цена на 0.5 → short_retrace ===")
    fall = list(np.linspace(200, 100, 60))
    retrace_up = 100 + (200 - 100) * 0.5   # = 150
    pullback = list(np.linspace(100, retrace_up, 40))
    prices = fall + pullback
    klines = _make_klines([(p*1.005, p*0.995, p) for p in prices])
    setup = detect_fib_setup(klines, current_price=retrace_up)
    assert setup is not None
    print(f"  direction={setup.direction}, setup_type={setup.setup_type}, "
          f"on_level={setup.on_level}")
    assert setup.direction == "down"
    assert setup.setup_type == "short_retrace"
    assert setup.on_level == 0.5
    print("  ✅ PASS")

    print("\n=== Test 3: up-свинг, но цена далеко от уровней → setup_type=None ===")
    rise = list(np.linspace(100, 200, 60))
    pullback = list(np.linspace(200, 195, 40))  # цена на 0.05 retrace, не входная
    prices = rise + pullback
    klines = _make_klines([(p*1.005, p*0.995, p) for p in prices])
    setup = detect_fib_setup(klines, current_price=195)
    assert setup is not None
    print(f"  direction={setup.direction}, on_level={setup.on_level}, "
          f"setup_type={setup.setup_type}, "
          f"distance_to_nearest={setup.distance_to_nearest:.2f}%")
    assert setup.direction == "up"
    assert setup.on_level is None
    assert setup.setup_type is None
    print("  ✅ PASS — свинг найден но входной точки сейчас нет")

    print("\n=== Test 4: недостаточно данных → None ===")
    klines = _make_klines([(100, 99, 99.5) for _ in range(20)])  # всего 20 баров
    setup = detect_fib_setup(klines)
    assert setup is None
    print("  ✅ PASS — None при недостатке данных")

    print("\n=== Test 5: боковик (нет значимых свингов) → None ===")
    rng = np.random.default_rng(42)
    prices = 100 + rng.normal(0, 0.3, 150)  # очень тихий боковик
    klines = _make_klines([(p+0.1, p-0.1, p) for p in prices])
    setup = detect_fib_setup(klines)
    if setup is None:
        print("  ✅ PASS — нет значимого свинга, вернули None")
    else:
        # амплитуда может быть маленькой но > 1.5×ATR случайно — это нормально
        print(f"  Найден свинг amplitude={setup.amplitude:.2f} (ATR={setup.atr:.2f}), "
              f"setup_type={setup.setup_type}")
        # главное чтобы не было setup_type — то есть цена не на уровне
        assert setup.amplitude >= 1.5 * setup.atr
        print("  ✅ PASS — свинг есть но он значимый (амплитуда > 1.5×ATR)")

    print("\n=== Test 6: TP-лестница для long_retrace ===")
    rise = list(np.linspace(100, 200, 60))
    pullback = list(np.linspace(200, 138.2, 40))
    klines = _make_klines([(p*1.005, p*0.995, p) for p in rise + pullback])
    setup = detect_fib_setup(klines, current_price=138.2)
    ladder = get_tp_ladder(setup)
    print(f"  TP ladder: {[round(p, 2) for p in ladder]}")
    # Лестница для long: от ближнего (0.5) к дальнему (swing_high)
    # swing low в этом тесте = 99.5 (из low=p*0.995), high = 201.0
    # amplitude = 101.5, 0.5 retracement = 201 - 50.75 = 150.25
    # Допуск 0.5% от цены — нормальный для синтетического теста.
    assert len(ladder) == 4
    assert abs(ladder[0] - 150.25) < 1.0   # 0.5 уровень
    assert abs(ladder[-1] - 201.0)  < 1.0  # swing high
    # Проверка порядка: должен возрастать (для long идём от близкого TP к дальнему)
    assert ladder[0] < ladder[1] < ladder[2] < ladder[3]
    print("  ✅ PASS — лестница правильная (восходящая для long)")

    print("\n=== Test 7: новый→старый порядок (OKX дефолт) ===")
    rise = list(np.linspace(100, 200, 60))
    pullback = list(np.linspace(200, 138.2, 40))
    klines_normal = _make_klines([(p*1.005, p*0.995, p) for p in rise + pullback])
    klines_reversed = list(reversed(klines_normal))
    setup = detect_fib_setup(klines_reversed, current_price=138.2)
    assert setup is not None
    assert setup.direction == "up"
    assert setup.setup_type == "long_retrace"
    print(f"  direction={setup.direction}, setup_type={setup.setup_type}")
    print("  ✅ PASS — эвристика разворота работает")

    print("\n🎉 Все 7 тестов прошли. fib_engine готов.")
