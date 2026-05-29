"""
volume_anomaly.py — три модуля защиты по объёму для PHANTOM (OKX edition)
==========================================================================

Содержимое:
  1. VolumeAnomalyDetector     — Z-score объёма за 50 баров (блокировка входа)
  2. VolumeDropGuard           — мониторинг падения объёма на открытых позициях
  3. VolumeDivergenceIndicator — 17-й индикатор: дивергенция цена/объём

Все три модуля независимы и могут включаться/выключаться по отдельности
через Config (см. секцию VOLUME_* в .env).
"""

from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

log = logging.getLogger("volume_anomaly")


# ════════════════════════════════════════════════════════════════════
# 1. VOLUME ANOMALY DETECTOR (Z-score по 50 барам)
# ════════════════════════════════════════════════════════════════════
class VolumeAnomalyDetector:
    """
    Статистический детектор аномалий объёма через rolling Z-score.

    Принцип:
        z = (current_volume - mean_50bars) / std_50bars
        |z| > 3  → аномалия

    Действие:
        spike (z > +3)   — резкий всплеск, возможен pump-and-dump → блокировать вход
        drop  (z < -2)   — пересыхание ликвидности → блокировать вход
        normal           — обычный режим, торгуем

    Асимметрия: drop срабатывает раньше (~2σ), потому что низкий объём
    опаснее для исполнения ордеров и SL/TP.
    """

    def __init__(self, window: int = 50, z_threshold: float = 3.0):
        self.window = window
        self.z_threshold = z_threshold
        self.drop_threshold = -z_threshold * 0.66  # ≈ -2σ при threshold=3

    def check(self, volume: pd.Series) -> dict:
        """
        Возвращает:
            {
                "z_score": float,
                "anomaly": "spike" | "drop" | "normal",
                "current": float,
                "mean": float,
                "std": float,
            }
        """
        empty = {"z_score": 0.0, "anomaly": "normal",
                 "current": 0.0, "mean": 0.0, "std": 0.0}

        if volume is None or len(volume) < self.window + 1:
            if volume is not None and len(volume) > 0:
                empty["current"] = float(volume.iloc[-1])
            return empty

        # baseline без текущего бара (чтобы он не смазывал собственный score)
        baseline = volume.iloc[-(self.window + 1):-1]
        current = float(volume.iloc[-1])
        mean = float(baseline.mean())
        std = float(baseline.std(ddof=0))

        if std < 1e-9 or mean < 1e-9:
            return {"z_score": 0.0, "anomaly": "normal",
                    "current": current, "mean": mean, "std": std}

        z = (current - mean) / std

        if z >= self.z_threshold:
            anomaly = "spike"
        elif z <= self.drop_threshold:
            anomaly = "drop"
        else:
            anomaly = "normal"

        return {"z_score": round(z, 2), "anomaly": anomaly,
                "current": current, "mean": mean, "std": std}


# ════════════════════════════════════════════════════════════════════
# 2. VOLUME DROP GUARD (защита открытых позиций)
# ════════════════════════════════════════════════════════════════════
@dataclass
class _SymbolVolHistory:
    bars_1h: deque = field(default_factory=lambda: deque(maxlen=12))   # 12 × 5min
    bars_4h: deque = field(default_factory=lambda: deque(maxlen=48))   # 48 × 5min
    last_action_ts: float = 0.0


class VolumeDropGuard:
    """
    Мониторит объём по уже открытым позициям. Если средний объём за 1ч
    падает в N раз ниже среднего за 4ч — выполняет защитное действие.

    Действия:
        "alert"        — только лог + Telegram (по желанию извне)
        "tighten_sl"   — подтянуть SL к рынку (вернуть % из конфига)
        "close"        — экстренно закрыть позицию

    Cooldown защищает от срабатывания каждый цикл — действие применяется
    не чаще, чем раз в `cooldown_sec` секунд на символ.
    """

    ACTION_NONE = None
    ACTION_ALERT = "alert"
    ACTION_TIGHTEN_SL = "tighten_sl"
    ACTION_CLOSE = "close"

    def __init__(
        self,
        drop_factor: float = 3.0,
        action: str = "tighten_sl",
        cooldown_sec: int = 300,
        min_bars_required: int = 24,
        max_sane_ratio: float = 50.0,
    ):
        if action not in (self.ACTION_ALERT, self.ACTION_TIGHTEN_SL, self.ACTION_CLOSE):
            raise ValueError(f"Unknown action: {action}")
        self.drop_factor = drop_factor
        self.action = action
        self.cooldown_sec = cooldown_sec
        self.min_bars_required = min_bars_required
        # max_sane_ratio: если 4h_avg / 1h_avg больше этого числа, это не падение
        # объёма, а ошибка данных (вероятно, в bars_4h попало кумулятивное
        # 24h значение из tickers вместо per-bar volume из klines).
        # См. лог-баг: "drop 12328.0x" на ALGOUSDT.
        self.max_sane_ratio = max_sane_ratio
        self._history: dict[str, _SymbolVolHistory] = {}

    def update_volume(self, symbol: str, bar_volume: float) -> None:
        """Запись объёма последнего закрытого бара в буферы.

        Защита от outliers: если новое значение в 100+ раз больше уже
        накопленной медианы — отбрасываем и логируем. Это означает что
        вызывающий код подал не 5-минутный объём, а кумулятивный (24h
        ticker volume), что портит всю статистику.
        """
        if bar_volume is None or bar_volume < 0:
            return
        v = float(bar_volume)
        h = self._history.setdefault(symbol, _SymbolVolHistory())

        # Sanity check: outlier protection
        if len(h.bars_4h) >= 6:
            sorted_vals = sorted(h.bars_4h)
            median = sorted_vals[len(sorted_vals) // 2]
            if median > 1e-9 and v > median * 100:
                log.warning(
                    f"[VolumeDropGuard] {symbol}: отброшен outlier vol={v:.0f} "
                    f"(медиана={median:.0f}, ratio={v/median:.0f}x). "
                    f"Проверь источник данных в bot_server.py — вероятно подаётся "
                    f"24h ticker volume вместо per-bar."
                )
                return

        h.bars_1h.append(v)
        h.bars_4h.append(v)

    def evaluate(self, symbol: str) -> Optional[str]:
        """
        Возвращает строку-действие, если падение зафиксировано,
        иначе None. Учитывает cooldown.
        """
        h = self._history.get(symbol)
        if not h or len(h.bars_4h) < self.min_bars_required:
            return None
        if time.time() - h.last_action_ts < self.cooldown_sec:
            return None

        avg_1h = sum(h.bars_1h) / max(len(h.bars_1h), 1)
        avg_4h = sum(h.bars_4h) / max(len(h.bars_4h), 1)
        if avg_4h < 1e-9 or avg_1h < 1e-9:
            return None

        ratio = avg_4h / avg_1h
        # Вторая линия защиты: даже после update_volume sanity-check
        # возможны крайние ratio при смешанных таймфреймах. Не реагируем.
        if ratio > self.max_sane_ratio:
            log.error(
                f"[VolumeDropGuard] {symbol}: ratio={ratio:.0f}x превышает разумный "
                f"порог {self.max_sane_ratio}x — это ошибка данных, action не применяется. "
                f"avg_1h={avg_1h:.0f}, avg_4h={avg_4h:.0f}, bars={len(h.bars_4h)}"
            )
            return None
        if ratio >= self.drop_factor:
            h.last_action_ts = time.time()
            log.warning(
                f"[VolumeDropGuard] {symbol}: 1h avg vol={avg_1h:.0f} "
                f"vs 4h avg={avg_4h:.0f} (drop {ratio:.1f}x) → action={self.action}"
            )
            return self.action
        return None

    def reset(self, symbol: str) -> None:
        """Очистить историю символа (вызывается при закрытии позиции)."""
        self._history.pop(symbol, None)

    def stats(self, symbol: str) -> dict:
        """Для дашборда / отладки."""
        h = self._history.get(symbol)
        if not h:
            return {"avg_1h": 0.0, "avg_4h": 0.0, "ratio": 0.0, "bars": 0}
        avg_1h = sum(h.bars_1h) / max(len(h.bars_1h), 1)
        avg_4h = sum(h.bars_4h) / max(len(h.bars_4h), 1)
        return {
            "avg_1h": round(avg_1h, 2),
            "avg_4h": round(avg_4h, 2),
            "ratio": round(avg_4h / avg_1h, 2) if avg_1h > 0 else 0.0,
            "bars": len(h.bars_4h),
        }


# ════════════════════════════════════════════════════════════════════
# 3. PRICE/VOLUME DIVERGENCE (17-й индикатор)
# ════════════════════════════════════════════════════════════════════
class VolumeDivergenceIndicator:
    """
    Классическая дивергенция цены и объёма за `lookback` баров.

    Считаем нормализованный наклон (slope/mean) для price и volume:
        bear_div : price slope > +T  AND  volume slope < -T
                   (рост на падающем объёме — слабое ралли, риск разворота вниз)
        bull_div : price slope < -T  AND  volume slope < -T
                   (падение на угасающем объёме — продавцы выдыхаются)
        neutral  : всё остальное

    Возвращает голос для Signal.analyze:
        bear_div → +1 голос "bear" (Volume Divergence Bear)
        bull_div → +1 голос "bull" (Volume Divergence Bull)
    """

    def __init__(self, lookback: int = 10, slope_threshold: float = 0.005):
        self.lookback = lookback
        self.slope_threshold = slope_threshold

    @staticmethod
    def _normalized_slope(series: pd.Series) -> float:
        """OLS slope, нормированный по среднему ⇒ %-в-бар."""
        if len(series) < 3:
            return 0.0
        y = series.values.astype(float)
        x = np.arange(len(y), dtype=float)
        x_mean = x.mean()
        denom = ((x - x_mean) ** 2).sum()
        if denom < 1e-12:
            return 0.0
        slope = ((x - x_mean) * (y - y.mean())).sum() / denom
        norm = max(abs(y.mean()), 1e-9)
        return slope / norm

    def check(self, close: pd.Series, volume: pd.Series) -> dict:
        if (close is None or volume is None
                or len(close) < self.lookback or len(volume) < self.lookback):
            return {"signal": "neutral", "price_slope": 0.0, "vol_slope": 0.0}

        c = close.iloc[-self.lookback:]
        v = volume.iloc[-self.lookback:]
        ps = self._normalized_slope(c)
        vs = self._normalized_slope(v)
        T = self.slope_threshold

        if ps > T and vs < -T:
            sig = "bear_div"
        elif ps < -T and vs < -T:
            sig = "bull_div"
        else:
            sig = "neutral"

        return {"signal": sig,
                "price_slope": round(ps, 5),
                "vol_slope": round(vs, 5)}


# ════════════════════════════════════════════════════════════════════
# Самотест (запускать командой:  python volume_anomaly.py)
# ════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")

    rng = np.random.default_rng(42)

    # 1. AnomalyDetector — нормальный + spike + drop
    print("\n=== Test 1: VolumeAnomalyDetector ===")
    det = VolumeAnomalyDetector(window=50, z_threshold=3.0)

    base = pd.Series(rng.normal(1000, 50, 60))
    print("Normal:", det.check(base))

    spike = base.copy(); spike.iloc[-1] = 5000
    print("Spike :", det.check(spike))

    drop = base.copy(); drop.iloc[-1] = 100
    print("Drop  :", det.check(drop))

    # 2. DropGuard — симулируем 4 часа нормального объёма, потом пересыхание
    print("\n=== Test 2: VolumeDropGuard ===")
    guard = VolumeDropGuard(drop_factor=3.0, action="tighten_sl",
                            cooldown_sec=0, min_bars_required=24)
    for i in range(48):
        guard.update_volume("BTCUSDT", 1000.0)
    print("After 4h normal:", guard.evaluate("BTCUSDT"))    # None
    for i in range(12):
        guard.update_volume("BTCUSDT", 100.0)               # 1h просели в 10×
    print("After 1h drop  :", guard.evaluate("BTCUSDT"))    # tighten_sl
    print("Stats          :", guard.stats("BTCUSDT"))

    # 3. Divergence — bear (цена растёт, объём падает)
    print("\n=== Test 3: VolumeDivergenceIndicator ===")
    div = VolumeDivergenceIndicator(lookback=10, slope_threshold=0.005)
    price_up = pd.Series([100, 101, 102, 103, 104, 105, 106, 107, 108, 109])
    vol_dn = pd.Series([1000, 950, 900, 850, 800, 750, 700, 650, 600, 550])
    print("Bear div:", div.check(price_up, vol_dn))

    price_dn = pd.Series([110, 109, 108, 107, 106, 105, 104, 103, 102, 101])
    print("Bull div:", div.check(price_dn, vol_dn))

    print("Neutral :", div.check(price_up, pd.Series([1000]*10)))

    print("\nAll tests passed ✓")
