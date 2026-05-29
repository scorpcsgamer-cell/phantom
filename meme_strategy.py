"""
meme_strategy.py — Отдельная стратегия для мемов (Фаза 1, Поставка 4B)
==========================================================================

Зачем отдельно: на PEPE/FLOKI/BONK/SHIB/DOGE технический анализ через Fib
работает плохо — нет нормальной структуры свингов, ликвидность тонкая,
объёмы спайково меняются. Зато мемы хорошо реагируют на резкие сдвиги
объёма + RSI экстремумы (oversold/overbought).

Логика входа:
  Long (для контр-памп охоты):
    • Volume последнего бара > 3 × среднего объёма последних N баров
    • RSI < 25 (глубокий oversold)
    • Цена закрылась выше open (зелёная свеча — есть отскок)

  Short:
    • Volume spike (та же логика)
    • RSI > 75 (overbought)
    • Цена закрылась ниже open (красная свеча — начало отката)

Жёсткие параметры из спеки:
  • Риск на позицию: 0.75% (в 2 раза меньше стандарта)
  • SL: 1.0% (вместо 1.5%)
  • Max 1 мем одновременно (контролируется в bot_server, не здесь)

Универсум мемов (можно конфигурировать):
  По умолчанию: PEPE, FLOKI, BONK, SHIB, DOGE.

API:
    is_meme(symbol)          # "PEPEUSDT" → True
    check_meme_setup(klines)  # возвращает сетап или None
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd

log = logging.getLogger("meme_strategy")


# ════════════════════════════════════════════════════════════════════
# Константы
# ════════════════════════════════════════════════════════════════════

# Стандартный набор мемов. Можно расширить при необходимости.
DEFAULT_MEMES = frozenset({
    "PEPE", "FLOKI", "BONK", "SHIB", "DOGE",
    "WIF", "BOME", "MEW", "POPCAT",   # современные мемы 2024-2025
})

# Параметры сигнала
VOLUME_LOOKBACK = 20          # окно для среднего объёма
VOLUME_SPIKE_MULT = 3.0       # порог: vol_last > 3 × avg
RSI_PERIOD = 14
RSI_OVERSOLD = 25.0
RSI_OVERBOUGHT = 75.0

# Risk-параметры для мемов (отдельные от стандарта)
MEME_RISK_PCT = 0.75
MEME_SL_PCT = 1.0
MEME_TP_PCT = 2.0   # консервативный RR 1:2

MIN_BARS_REQUIRED = max(VOLUME_LOOKBACK, RSI_PERIOD) + 5


# ════════════════════════════════════════════════════════════════════
# Утилиты
# ════════════════════════════════════════════════════════════════════

def is_meme(symbol: str, custom_set: Optional[set] = None) -> bool:
    """Проверить — мем или нет. Сравнивает по базовому имени без USDT/USDC.

    Args:
        symbol: "PEPEUSDT", "PEPE-USDT-SWAP", "PEPE" — любой формат.
        custom_set: переопределить дефолтный список.

    Returns:
        True если символ в списке мемов.
    """
    if not symbol:
        return False
    memes = custom_set if custom_set is not None else DEFAULT_MEMES
    # Берём первую часть до тире или удаляем суффикс USDT/USDC
    base = symbol.split("-")[0]
    for suf in ("USDT", "USDC", "BUSD"):
        if base.endswith(suf) and base != suf:
            base = base[:-len(suf)]
            break
    return base.upper() in {s.upper() for s in memes}


def _rsi_last(closes: pd.Series, period: int = RSI_PERIOD) -> float:
    """RSI последнего бара через Wilder smoothing."""
    if len(closes) < period + 1:
        return 50.0
    delta = closes.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1.0/period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1.0/period, adjust=False, min_periods=period).mean()
    last_gain = float(avg_gain.iloc[-1])
    last_loss = float(avg_loss.iloc[-1])
    if last_loss <= 0:
        return 100.0 if last_gain > 0 else 50.0
    rs = last_gain / last_loss
    return 100.0 - (100.0 / (1.0 + rs))


# ════════════════════════════════════════════════════════════════════
# Структура сетапа
# ════════════════════════════════════════════════════════════════════

@dataclass
class MemeSetup:
    side: str            # "Buy" / "Sell"
    rsi: float
    volume_ratio: float  # vol_last / avg_vol
    last_close: float
    last_open: float
    risk_pct: float = MEME_RISK_PCT
    sl_pct: float = MEME_SL_PCT
    tp_pct: float = MEME_TP_PCT

    def to_dict(self) -> dict:
        return {
            "side": self.side,
            "rsi": round(self.rsi, 2),
            "volume_ratio": round(self.volume_ratio, 2),
            "last_close": self.last_close,
            "last_open": self.last_open,
            "risk_pct": self.risk_pct,
            "sl_pct": self.sl_pct,
            "tp_pct": self.tp_pct,
        }


# ════════════════════════════════════════════════════════════════════
# Главная функция
# ════════════════════════════════════════════════════════════════════

def check_meme_setup(klines: list) -> Optional[MemeSetup]:
    """Проверить — есть ли сейчас сигнал для мем-входа.

    Args:
        klines: OKX kline data [[ts, o, h, l, c, vol, ...], ...]

    Returns:
        MemeSetup или None если сигнала нет.
    """
    if not klines or len(klines) < MIN_BARS_REQUIRED:
        return None

    try:
        first_ts = int(klines[0][0])
        last_ts  = int(klines[-1][0])
        data = list(reversed(klines)) if first_ts > last_ts else list(klines)
        opens   = np.array([float(k[1]) for k in data], dtype=float)
        closes  = np.array([float(k[4]) for k in data], dtype=float)
        volumes = np.array([float(k[5]) for k in data], dtype=float)
    except (ValueError, TypeError, IndexError) as e:
        log.warning(f"check_meme_setup: parse error: {e}")
        return None

    closes_pd = pd.Series(closes, dtype=float)

    # 1. Volume spike: текущий объём против среднего предыдущих VOLUME_LOOKBACK
    if len(volumes) < VOLUME_LOOKBACK + 1:
        return None
    avg_vol = float(np.mean(volumes[-(VOLUME_LOOKBACK+1):-1]))   # без текущего
    if avg_vol <= 0:
        return None
    vol_ratio = float(volumes[-1] / avg_vol)
    if vol_ratio < VOLUME_SPIKE_MULT:
        return None  # нет всплеска — не наш кейс

    # 2. RSI
    rsi = _rsi_last(closes_pd, RSI_PERIOD)

    # 3. Свеча
    last_open = float(opens[-1])
    last_close = float(closes[-1])
    is_green = last_close > last_open
    is_red = last_close < last_open

    # 4. Решение
    if rsi < RSI_OVERSOLD and is_green:
        # Глубокий oversold + зелёная свеча → контр-памп вверх (long)
        return MemeSetup(side="Buy", rsi=rsi, volume_ratio=vol_ratio,
                          last_close=last_close, last_open=last_open)
    if rsi > RSI_OVERBOUGHT and is_red:
        # Overbought + красная → начало отката вниз (short)
        return MemeSetup(side="Sell", rsi=rsi, volume_ratio=vol_ratio,
                          last_close=last_close, last_open=last_open)

    return None


# ════════════════════════════════════════════════════════════════════
# Самотест
# ════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    print("\n=== Test 1: is_meme — определение мем-токенов ===")
    cases = [
        ("PEPEUSDT", True),
        ("PEPE-USDT-SWAP", True),
        ("PEPE", True),
        ("pepe", True),
        ("FLOKIUSDT", True),
        ("BONKUSDT", True),
        ("BTCUSDT", False),
        ("ETHUSDT", False),
        ("SOLUSDT", False),
        ("", False),
    ]
    for sym, expected in cases:
        got = is_meme(sym)
        ok = "✅" if got == expected else "❌"
        print(f"  {ok} is_meme('{sym}') = {got} (ожидали {expected})")
        assert got == expected, f"is_meme('{sym}')"
    print("  ✅ PASS")

    def _make_klines(opens: list, closes: list, volumes: list) -> list:
        """Klines с заданными open/close/volume per bar."""
        assert len(opens) == len(closes) == len(volumes)
        out = []
        for i, (o, c, v) in enumerate(zip(opens, closes, volumes)):
            ts = 1700000000_000 + i * 300_000  # 5m бары
            h = max(o, c) * 1.001
            l = min(o, c) * 0.999
            out.append([str(ts), str(o), str(h), str(l), str(c), str(v), "10000"])
        return out

    print("\n=== Test 2: oversold + green + volume spike → long ===")
    # 25 баров: первые 20 — нейтральные с объёмом 100, потом падение,
    # потом резкий отскок с объёмом 500 и зелёной свечой.
    opens = [100.0] * 19 + [99.0, 96.0, 92.0, 88.0, 84.0, 81.0]   # падение
    closes = [99.0] * 19 + [96.0, 92.0, 88.0, 84.0, 81.0, 85.0]   # последняя зелёная
    volumes = [100.0] * 19 + [120.0, 130.0, 140.0, 150.0, 160.0, 500.0]
    klines = _make_klines(opens, closes, volumes)
    setup = check_meme_setup(klines)
    if setup is None:
        print(f"  ⚠ setup=None — проверь rsi и volume")
        # debug
        from meme_strategy import _rsi_last
        rsi = _rsi_last(pd.Series(closes))
        avg_vol = np.mean(volumes[-21:-1])
        print(f"  debug: rsi={rsi:.1f}, vol_ratio={volumes[-1]/avg_vol:.2f}")
    assert setup is not None, "Ожидали setup при падении + отскоке с объёмом"
    print(f"  {setup.to_dict()}")
    assert setup.side == "Buy"
    assert setup.rsi < 30
    assert setup.volume_ratio >= 3.0
    print("  ✅ PASS")

    print("\n=== Test 3: overbought + red + volume spike → short ===")
    opens = [100.0] * 19 + [102.0, 105.0, 108.0, 112.0, 116.0, 119.0]
    closes = [102.0] * 19 + [105.0, 108.0, 112.0, 116.0, 119.0, 115.0]  # last red
    volumes = [100.0] * 19 + [120.0, 130.0, 140.0, 150.0, 160.0, 500.0]
    klines = _make_klines(opens, closes, volumes)
    setup = check_meme_setup(klines)
    assert setup is not None
    print(f"  {setup.to_dict()}")
    assert setup.side == "Sell"
    assert setup.rsi > 70
    print("  ✅ PASS")

    print("\n=== Test 4: нет volume spike → None ===")
    # Тот же сценарий как test 2, но без всплеска объёма
    opens = [100.0] * 19 + [99.0, 96.0, 92.0, 88.0, 84.0, 81.0]
    closes = [99.0] * 19 + [96.0, 92.0, 88.0, 84.0, 81.0, 85.0]
    volumes = [100.0] * 24 + [110.0]   # вял объём, всего 25 значений
    klines = _make_klines(opens, closes, volumes)
    setup = check_meme_setup(klines)
    assert setup is None, "Без spike не должно быть сигнала"
    print("  ✅ PASS — без volume spike → None")

    print("\n=== Test 5: volume spike, но RSI нейтральный (50) → None ===")
    # Объём прыгнул, но цена в боковике, RSI ~ 50
    opens = [100.0] * 24 + [100.5]
    closes = [100.5, 100.0] * 12 + [100.5]
    volumes = [100.0] * 24 + [500.0]
    klines = _make_klines(opens, closes, volumes)
    setup = check_meme_setup(klines)
    assert setup is None, "Нейтральный RSI не должен давать сигнал"
    print("  ✅ PASS")

    print("\n=== Test 6: oversold + RED свеча → None (нет подтверждения отскока) ===")
    # RSI < 25, volume spike, но свеча красная — отскока не подтверждено
    opens = [100.0] * 19 + [99.0, 96.0, 92.0, 88.0, 84.0, 81.0]
    closes = [99.0] * 19 + [96.0, 92.0, 88.0, 84.0, 81.0, 78.0]  # last red!
    volumes = [100.0] * 19 + [120.0, 130.0, 140.0, 150.0, 160.0, 500.0]
    klines = _make_klines(opens, closes, volumes)
    setup = check_meme_setup(klines)
    assert setup is None, "Падение продолжается — не входим"
    print("  ✅ PASS — красная свеча на oversold НЕ даёт сигнала")

    print("\n=== Test 7: недостаточно данных → None ===")
    klines = _make_klines([100.0]*10, [100.0]*10, [100.0]*10)
    assert check_meme_setup(klines) is None
    print("  ✅ PASS")

    print("\n=== Test 8: обратный порядок klines ===")
    opens = [100.0] * 19 + [99.0, 96.0, 92.0, 88.0, 84.0, 81.0]
    closes = [99.0] * 19 + [96.0, 92.0, 88.0, 84.0, 81.0, 85.0]
    volumes = [100.0] * 19 + [120.0, 130.0, 140.0, 150.0, 160.0, 500.0]
    klines_normal = _make_klines(opens, closes, volumes)
    klines_reversed = list(reversed(klines_normal))
    s1 = check_meme_setup(klines_normal)
    s2 = check_meme_setup(klines_reversed)
    assert s1 is not None and s2 is not None
    assert s1.side == s2.side
    assert abs(s1.rsi - s2.rsi) < 0.01
    print(f"  normal:   side={s1.side}, rsi={s1.rsi:.2f}")
    print(f"  reversed: side={s2.side}, rsi={s2.rsi:.2f}")
    print("  ✅ PASS — эвристика разворота работает")

    print("\n🎉 Все 8 тестов прошли. meme_strategy готов.")
