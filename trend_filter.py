"""
trend_filter.py — Multi-TF тренд-фильтр для PHANTOM (Фаза 1, Поставка 1)
==========================================================================

Задача: ответить на единственный вопрос — "разрешён ли вход в long или short
по этому символу прямо сейчас?". Это ПЕРВЫЙ фильтр перед Fib и дивергенцией:
любая стратегия входа применяется только в направлении тренда.

Логика "жёсткого" варианта (согласовано с Сергеем):
  • 4h таймфрейм: EMA50 vs EMA200, наклон EMA50
      - long_4h:  EMA50 > EMA200 и slope(EMA50) > +threshold
      - short_4h: EMA50 < EMA200 и slope(EMA50) < -threshold
  • 1h таймфрейм: EMA20 vs EMA50, наклон EMA20
      - long_1h:  EMA20 > EMA50  и slope(EMA20) > +threshold
      - short_1h: EMA20 < EMA50  и slope(EMA20) < -threshold
  • Вход разрешён только если оба TF согласны (long_4h AND long_1h)

API (чистая функция):
    result = check_trend(klines_4h, klines_1h, slope_threshold=0.001)
    # → {"long_allowed": bool, "short_allowed": bool, "regime": str, "details": {...}}

Также есть высокоуровневый класс `MultiTFTrendFilter` с TTL-кешом:
он принимает exchange client и сам подгружает klines, кешируя на 5 минут.
Нужен, потому что 33 символа × 2 таймфрейма = 66 запросов на цикл, что
быстро упрётся в rate limit OKX (~20 req/sec на public).

Использование в bot_server.py (будет в Поставке 5, интеграция):
    self.trend_filter = MultiTFTrendFilter(self.client, slope_threshold=0.001)
    ...
    trend = await self.trend_filter.check(symbol)
    if signal_side == "Buy" and not trend["long_allowed"]:
        return  # тренд против — не входим
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd

log = logging.getLogger("trend_filter")


# ════════════════════════════════════════════════════════════════════
# Константы и пороги
# ════════════════════════════════════════════════════════════════════

# Минимум баров для расчёта на каждом таймфрейме.
# 4h: было 220 (для адекватной EMA200), снижено до 100. На OKX testnet и
# для новых альтов 220 баров 4h истории (36+ дней) часто недоступны.
# С 100 барами EMA200 будет менее точной первое время, но это лучше чем
# вообще не торговать. На mainnet можно вернуть к 220 если хочется точности.
# 1h: EMA50 требует хотя бы ~60 баров.
MIN_BARS_4H = 100
MIN_BARS_1H = 60

# Длина окна для расчёта slope (наклон EMA).
# slope считается на последних SLOPE_LOOKBACK барах.
SLOPE_LOOKBACK = 10

# TTL кеша для класса MultiTFTrendFilter. 4h тренд не меняется за 5 мин,
# 1h меняется медленно. Значение в секундах.
CACHE_TTL_SEC = 300


# ════════════════════════════════════════════════════════════════════
# Утилиты: EMA и нормализованный slope
# ════════════════════════════════════════════════════════════════════

def _ema(series: pd.Series, span: int) -> pd.Series:
    """Exponential Moving Average через pandas ewm."""
    return series.ewm(span=span, adjust=False, min_periods=span).mean()


def _normalized_slope(series: pd.Series, lookback: int = SLOPE_LOOKBACK) -> float:
    """OLS slope на последних lookback значениях, нормированный по среднему.
    Возвращает: % изменения за бар. Положительный = рост, отрицательный = падение.
    Например, 0.001 = +0.1% за бар.
    """
    if len(series) < lookback:
        return 0.0
    y = series.iloc[-lookback:].values.astype(float)
    # отбрасываем NaN из неполной EMA в начале
    mask = ~np.isnan(y)
    if mask.sum() < 3:
        return 0.0
    y = y[mask]
    x = np.arange(len(y), dtype=float)
    x_mean = x.mean()
    denom = ((x - x_mean) ** 2).sum()
    if denom < 1e-12:
        return 0.0
    slope = ((x - x_mean) * (y - y.mean())).sum() / denom
    norm = max(abs(y.mean()), 1e-9)
    return float(slope / norm)


# ════════════════════════════════════════════════════════════════════
# Чистая функция: проверка тренда по одному таймфрейму
# ════════════════════════════════════════════════════════════════════

@dataclass
class TFResult:
    """Результат анализа одного таймфрейма."""
    direction: str       # "up" / "down" / "flat" / "unknown"
    long_ok: bool
    short_ok: bool
    fast_ema: float
    slow_ema: float
    slope: float
    bars_used: int

    def to_dict(self) -> dict:
        return {
            "direction": self.direction,
            "fast_ema": round(self.fast_ema, 8),
            "slow_ema": round(self.slow_ema, 8),
            "slope": round(self.slope, 6),
            "bars_used": self.bars_used,
        }


def _check_single_tf(closes: pd.Series, fast_span: int, slow_span: int,
                      min_bars: int, slope_threshold: float) -> TFResult:
    """Проверка одного таймфрейма: EMA(fast) vs EMA(slow) + slope EMA(fast).

    Args:
        closes: Series close-цен (старая → новая)
        fast_span: span быстрой EMA (50 для 4h, 20 для 1h)
        slow_span: span медленной EMA (200 для 4h, 50 для 1h)
        min_bars: минимум баров для валидного решения
        slope_threshold: порог наклона за бар (например 0.001 = +0.1%/бар)

    Returns:
        TFResult с long_ok / short_ok / direction.
    """
    n = len(closes)
    if n < min_bars:
        return TFResult("unknown", False, False, 0.0, 0.0, 0.0, n)

    fast = _ema(closes, fast_span)
    slow = _ema(closes, slow_span)

    fast_last = float(fast.iloc[-1])
    slow_last = float(slow.iloc[-1])
    slope = _normalized_slope(fast, lookback=SLOPE_LOOKBACK)

    if np.isnan(fast_last) or np.isnan(slow_last):
        return TFResult("unknown", False, False, 0.0, 0.0, 0.0, n)

    long_ok  = (fast_last > slow_last) and (slope > slope_threshold)
    short_ok = (fast_last < slow_last) and (slope < -slope_threshold)

    if long_ok:
        direction = "up"
    elif short_ok:
        direction = "down"
    else:
        direction = "flat"

    return TFResult(direction, long_ok, short_ok, fast_last, slow_last, slope, n)


def check_trend(klines_4h: list, klines_1h: list,
                 slope_threshold: float = 0.001) -> dict:
    """Главная функция — multi-TF тренд проверка.

    Args:
        klines_4h: OKX klines на 4h, формат [[ts, o, h, l, c, vol, ...], ...]
                   (старые → новые ИЛИ новые → старые — мы развернём)
        klines_1h: OKX klines на 1h, тот же формат
        slope_threshold: порог наклона за бар (default 0.001 = 0.1%/бар)

    Returns:
        {
          "long_allowed":  bool,   # разрешён ли long вход
          "short_allowed": bool,   # разрешён ли short вход
          "regime":        str,    # "up" / "down" / "flat" / "unknown"
          "details": {
              "tf_4h":     {...},  # детали 4h анализа
              "tf_1h":     {...},  # детали 1h анализа
              "agreement": bool,   # согласие таймфреймов
          }
        }

    Безопасно при пустых/коротких данных — вернёт all False, regime="unknown".
    """
    empty = {
        "long_allowed": False, "short_allowed": False,
        "regime": "unknown", "details": {
            "tf_4h": {}, "tf_1h": {}, "agreement": False,
        }
    }

    # Конвертация klines → pd.Series close-цен.
    # Кандл из OKX приходит как [ts, o, h, l, c, vol, ...]. Индекс 4 = close.
    # OKX возвращает в порядке новые→старые, но мы для верности развернём.
    def _to_close_series(klines: list) -> Optional[pd.Series]:
        if not klines:
            return None
        try:
            # Эвристика: если первый ts > последнего → новые→старые, развернём
            first_ts = int(klines[0][0])
            last_ts  = int(klines[-1][0])
            data = list(reversed(klines)) if first_ts > last_ts else list(klines)
            closes = pd.Series([float(k[4]) for k in data], dtype=float)
            return closes
        except (ValueError, TypeError, IndexError) as e:
            log.warning(f"check_trend: не удалось распарсить klines: {e}")
            return None

    closes_4h = _to_close_series(klines_4h)
    closes_1h = _to_close_series(klines_1h)
    if closes_4h is None or closes_1h is None:
        return empty

    tf_4h = _check_single_tf(closes_4h, fast_span=50, slow_span=200,
                              min_bars=MIN_BARS_4H, slope_threshold=slope_threshold)
    tf_1h = _check_single_tf(closes_1h, fast_span=20, slow_span=50,
                              min_bars=MIN_BARS_1H, slope_threshold=slope_threshold)

    long_allowed  = tf_4h.long_ok  and tf_1h.long_ok
    short_allowed = tf_4h.short_ok and tf_1h.short_ok
    agreement = (tf_4h.direction == tf_1h.direction
                 and tf_4h.direction in ("up", "down"))

    if long_allowed:
        regime = "up"
    elif short_allowed:
        regime = "down"
    elif tf_4h.direction == "unknown" or tf_1h.direction == "unknown":
        regime = "unknown"
    else:
        regime = "flat"

    return {
        "long_allowed":  long_allowed,
        "short_allowed": short_allowed,
        "regime":        regime,
        "details": {
            "tf_4h":     tf_4h.to_dict(),
            "tf_1h":     tf_1h.to_dict(),
            "agreement": agreement,
        }
    }


# ════════════════════════════════════════════════════════════════════
# Высокоуровневый класс с кешом — для использования в bot_server
# ════════════════════════════════════════════════════════════════════

class MultiTFTrendFilter:
    """Обёртка над check_trend с TTL-кешом и автозагрузкой klines.

    Зачем кеш: 33 символа × 2 таймфрейма = 66 API запросов на цикл анализа.
    Без кеша это упрётся в rate limit OKX (~20 req/sec public). 4h тренд
    реально не меняется чаще раза в 30 минут, 1h — раза в 10. TTL=5 мин
    даёт фактор экономии ~10× при сохранении актуальности.
    """

    def __init__(self, exchange_client, slope_threshold: float = 0.001,
                  cache_ttl_sec: int = CACHE_TTL_SEC,
                  stats_log_interval_sec: int = 300,
                  stats_min_cached: int = 5):
        """
        stats_log_interval_sec: как часто (сек) писать сводный [TREND STATS]
            лог по всем символам в кеше. 0 = отключить. Дефолт 300 (5 мин)
            совпадает с TTL кеша, так что каждая сводка отражает свежий снимок.
        stats_min_cached: минимум символов в кеше прежде чем начать писать
            сводку. На старте бота кеш пуст, и писать "1 sym" бесполезно.
        """
        self.client = exchange_client
        self.slope_threshold = slope_threshold
        self.cache_ttl = cache_ttl_sec
        # cache[symbol] = (timestamp, result_dict)
        self._cache: dict[str, tuple[float, dict]] = {}
        # Параметры периодической диагностической сводки
        self.stats_log_interval_sec = stats_log_interval_sec
        self.stats_min_cached = stats_min_cached
        self._last_stats_log: float = 0.0

    async def check(self, symbol: str, force: bool = False) -> dict:
        """Получить результат тренд-проверки по символу. Кеш на TTL секунд.

        Args:
            symbol: e.g. "BTCUSDT"
            force: игнорировать кеш и запросить заново
        """
        now = time.monotonic()
        if not force and symbol in self._cache:
            ts, result = self._cache[symbol]
            if now - ts < self.cache_ttl:
                return result

        try:
            # OKX intervals: "4H" и "1H" (в Bybit было "240" и "60")
            # Адаптер exchange_client должен принимать строковые названия
            klines_4h = await self.client.get_klines(symbol, interval="4H", limit=250)
            klines_1h = await self.client.get_klines(symbol, interval="1H", limit=80)
        except Exception as e:
            log.warning(f"MultiTFTrendFilter[{symbol}]: ошибка загрузки klines: {e}")
            # Возвращаем "unknown" — это безопасно: бот не будет входить
            return {
                "long_allowed": False, "short_allowed": False,
                "regime": "unknown",
                "details": {"error": str(e)}
            }

        # Диагностика: первый вызов по символу логируем явно — увидим что
        # OKX реально отдаёт и хватает ли баров для тренд-анализа.
        n4h = len(klines_4h) if klines_4h else 0
        n1h = len(klines_1h) if klines_1h else 0
        result = check_trend(klines_4h, klines_1h, self.slope_threshold)

        # Если результат unknown — это блокирует все входы по символу. Логируем
        # причину чтобы Сергей мог увидеть в логе что не так.
        if result["regime"] == "unknown":
            log.info(
                f"[TREND] {symbol}: unknown regime "
                f"(bars 4h={n4h}/{MIN_BARS_4H}, 1h={n1h}/{MIN_BARS_1H}). "
                f"Вход заблокирован."
            )
        elif result["regime"] == "flat":
            # Не unknown но flat — тренд есть на одном TF и отсутствует на другом
            # ИЛИ оба flat. Тоже блокирует, но это нормальное состояние рынка.
            tf_4h_dir = result["details"]["tf_4h"].get("direction", "?")
            tf_1h_dir = result["details"]["tf_1h"].get("direction", "?")
            log.info(f"[TREND] {symbol}: flat (4h={tf_4h_dir}, 1h={tf_1h_dir})")
        # При up/down ничего не пишем — будет [SIGNAL] потом

        self._cache[symbol] = (now, result)
        # Периодическая сводка по всем символам в кеше — полезна когда хочется
        # понять состояние рынка в целом без чтения 33 отдельных строк лога.
        self._maybe_log_stats(now)
        return result

    def _maybe_log_stats(self, now: float) -> None:
        """Если прошло stats_log_interval_sec с последней сводки — пишем её.

        Формат строки:
          [TREND STATS] N sym | regime: up=K (sym1,sym2) down=K (sym1,sym2)
            flat=K unknown=K | 4h up/dn/fl=K/K/K | 1h up/dn/fl=K/K/K

        Сводка строится ТОЛЬКО по свежим записям кеша (age < cache_ttl).
        Перечисляются конкретные символы только для up и down — это редкие
        состояния и их важно видеть. flat/unknown показываем числом.
        """
        if self.stats_log_interval_sec <= 0:
            return  # отключено

        # Собираем свежие записи кеша
        fresh: list[tuple[str, dict]] = [
            (sym, res) for sym, (ts, res) in self._cache.items()
            if now - ts < self.cache_ttl
        ]
        if len(fresh) < self.stats_min_cached:
            return  # кеш ещё прогревается, не пишем

        if now - self._last_stats_log < self.stats_log_interval_sec:
            return  # рано

        self._last_stats_log = now

        # Подсчёт по итоговому regime
        ups:      list[str] = []
        downs:    list[str] = []
        flats:    int = 0
        unknowns: int = 0
        # Отдельно по TF — полезно для диагностики (видно почему мало long_allowed)
        tf4h_up = tf4h_dn = tf4h_fl = 0
        tf1h_up = tf1h_dn = tf1h_fl = 0

        for sym, res in fresh:
            regime = res.get("regime", "unknown")
            if regime == "up":
                ups.append(sym)
            elif regime == "down":
                downs.append(sym)
            elif regime == "flat":
                flats += 1
            else:
                unknowns += 1

            det = res.get("details", {}) or {}
            d4 = (det.get("tf_4h") or {}).get("direction", "?")
            d1 = (det.get("tf_1h") or {}).get("direction", "?")
            if d4 == "up":   tf4h_up += 1
            elif d4 == "down": tf4h_dn += 1
            elif d4 == "flat": tf4h_fl += 1
            if d1 == "up":   tf1h_up += 1
            elif d1 == "down": tf1h_dn += 1
            elif d1 == "flat": tf1h_fl += 1

        ups_str   = f"up={len(ups)} ({','.join(ups)})"     if ups   else "up=0"
        downs_str = f"down={len(downs)} ({','.join(downs)})" if downs else "down=0"

        log.info(
            f"[TREND STATS] {len(fresh)} sym | regime: {ups_str} {downs_str} "
            f"flat={flats} unknown={unknowns} | "
            f"4h up/dn/fl={tf4h_up}/{tf4h_dn}/{tf4h_fl} | "
            f"1h up/dn/fl={tf1h_up}/{tf1h_dn}/{tf1h_fl}"
        )

    def invalidate(self, symbol: Optional[str] = None) -> None:
        """Сбросить кеш — по символу или весь."""
        if symbol is None:
            self._cache.clear()
        else:
            self._cache.pop(symbol, None)

    def stats(self) -> dict:
        """Размер кеша для дашборда/отладки."""
        now = time.monotonic()
        fresh = sum(1 for ts, _ in self._cache.values()
                    if now - ts < self.cache_ttl)
        return {"cached_symbols": len(self._cache), "fresh": fresh,
                "ttl_sec": self.cache_ttl}


# ════════════════════════════════════════════════════════════════════
# Самотест (запускать командой:  python trend_filter.py)
# ════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")

    def _make_klines(prices: list, ts_start: int = 1700000000_000,
                      ts_step_ms: int = 4 * 3600 * 1000) -> list:
        """Собрать массив klines в OKX формате [ts, o, h, l, c, vol, ...]."""
        out = []
        for i, p in enumerate(prices):
            ts = ts_start + i * ts_step_ms
            out.append([str(ts), str(p), str(p*1.001), str(p*0.999),
                        str(p), "100", "10000"])
        return out

    print("\n=== Test 1: чёткий аптренд (BTC-style рост) ===")
    # 250 баров плавного роста: 30000 → 60000
    prices_4h_up = list(np.linspace(30000, 60000, 250))
    prices_1h_up = list(np.linspace(55000, 60000, 80))
    klines_4h = _make_klines(prices_4h_up)
    klines_1h = _make_klines(prices_1h_up, ts_step_ms=3600_000)
    r = check_trend(klines_4h, klines_1h)
    print(f"  long_allowed={r['long_allowed']}, short_allowed={r['short_allowed']}, "
          f"regime={r['regime']}")
    print(f"  4h: {r['details']['tf_4h']}")
    print(f"  1h: {r['details']['tf_1h']}")
    assert r["long_allowed"] is True
    assert r["short_allowed"] is False
    assert r["regime"] == "up"
    print("  ✅ PASS")

    print("\n=== Test 2: чёткий даунтренд ===")
    prices_4h_down = list(np.linspace(60000, 30000, 250))
    prices_1h_down = list(np.linspace(35000, 30000, 80))
    klines_4h = _make_klines(prices_4h_down)
    klines_1h = _make_klines(prices_1h_down, ts_step_ms=3600_000)
    r = check_trend(klines_4h, klines_1h)
    print(f"  long_allowed={r['long_allowed']}, short_allowed={r['short_allowed']}, "
          f"regime={r['regime']}")
    assert r["long_allowed"] is False
    assert r["short_allowed"] is True
    assert r["regime"] == "down"
    print("  ✅ PASS")

    print("\n=== Test 3: боковик (нет тренда) ===")
    rng = np.random.default_rng(42)
    prices_flat = 50000 + rng.normal(0, 100, 250)  # шум вокруг 50k
    klines_4h = _make_klines(list(prices_flat))
    klines_1h = _make_klines(list(prices_flat[-80:]), ts_step_ms=3600_000)
    r = check_trend(klines_4h, klines_1h)
    print(f"  long_allowed={r['long_allowed']}, short_allowed={r['short_allowed']}, "
          f"regime={r['regime']}")
    assert r["long_allowed"] is False
    assert r["short_allowed"] is False
    print("  ✅ PASS")

    print("\n=== Test 4: конфликт TF (4h up, 1h down) — должен блокировать оба ===")
    klines_4h = _make_klines(list(np.linspace(30000, 60000, 250)))
    klines_1h = _make_klines(list(np.linspace(60000, 55000, 80)),
                              ts_step_ms=3600_000)
    r = check_trend(klines_4h, klines_1h)
    print(f"  long_allowed={r['long_allowed']}, short_allowed={r['short_allowed']}, "
          f"regime={r['regime']}")
    print(f"  4h direction={r['details']['tf_4h']['direction']}, "
          f"1h direction={r['details']['tf_1h']['direction']}")
    assert r["long_allowed"] is False
    assert r["short_allowed"] is False
    assert r["regime"] == "flat"   # таймфреймы не согласны = не торгуем
    print("  ✅ PASS")

    print("\n=== Test 5: недостаточно данных → unknown ===")
    r = check_trend(_make_klines([50000]*50), _make_klines([50000]*20))
    print(f"  regime={r['regime']}, long={r['long_allowed']}")
    assert r["regime"] == "unknown"
    assert r["long_allowed"] is False
    assert r["short_allowed"] is False
    print("  ✅ PASS")

    print("\n=== Test 6: пустые klines → unknown (не падаем) ===")
    r = check_trend([], [])
    assert r["regime"] == "unknown"
    assert r["long_allowed"] is False
    print("  ✅ PASS")

    print("\n=== Test 7: новый→старый порядок (как реально отдаёт OKX) ===")
    # OKX обычно отдаёт новые сверху. Проверяем, что эвристика разворота работает.
    prices_up = list(np.linspace(30000, 60000, 250))
    klines_4h_reversed = list(reversed(_make_klines(prices_up)))
    klines_1h_reversed = list(reversed(_make_klines(
        list(np.linspace(55000, 60000, 80)), ts_step_ms=3600_000)))
    r = check_trend(klines_4h_reversed, klines_1h_reversed)
    print(f"  long_allowed={r['long_allowed']}, regime={r['regime']}")
    assert r["long_allowed"] is True
    print("  ✅ PASS — эвристика разворота работает корректно")

    print("\n🎉 Все 7 тестов прошли. Модуль готов к интеграции (Поставка 5).")
