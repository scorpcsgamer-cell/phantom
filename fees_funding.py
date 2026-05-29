"""
Модуль расчёта комиссий и funding rate для Bybit USDT-фьючерсов.

Bybit fees (на дату Apr 2026):
  Maker: 0.02% (можно обнулить через API discount)
  Taker: 0.055%

Funding rate: каждые 8 часов (00:00, 08:00, 16:00 UTC)
  Размер варьируется ±0.01% (типично) до ±0.75% (экстремально)
"""

import time
from datetime import datetime, timezone, timedelta


class FeeCalculator:
    """Считает реальные комиссии при входе/выходе."""

    # Стандартные тарифы Bybit для VIP 0
    MAKER_FEE = 0.0002   # 0.02%
    TAKER_FEE = 0.00055  # 0.055%

    @classmethod
    def round_trip_fee(cls, notional_usdt: float, both_taker: bool = True) -> float:
        """
        Полная комиссия за вход + выход (round-trip).
        Market orders → taker fee на обе стороны.
        """
        rate = cls.TAKER_FEE if both_taker else cls.MAKER_FEE
        return notional_usdt * rate * 2  # вход + выход

    @classmethod
    def entry_fee(cls, notional_usdt: float, taker: bool = True) -> float:
        rate = cls.TAKER_FEE if taker else cls.MAKER_FEE
        return notional_usdt * rate

    @classmethod
    def break_even_pct(cls, leverage: int = 1, taker: bool = True) -> float:
        """
        % движения цены, нужный чтобы покрыть комиссии.
        При плече 5x и taker fees: 0.055% × 2 × 5 = 0.55% движения цены.
        """
        rate = cls.TAKER_FEE if taker else cls.MAKER_FEE
        return rate * 2 * leverage * 100  # в процентах

    @classmethod
    def adjust_pnl_for_fees(cls, gross_pnl: float, notional_usdt: float,
                             taker: bool = True) -> float:
        """Из валового PnL вычитаем комиссии за round-trip."""
        return gross_pnl - cls.round_trip_fee(notional_usdt, taker)


class FundingTracker:
    """
    Отслеживает funding rate и оценивает суммарные затраты/доходы
    от funding для открытых позиций.

    Funding происходит в 00:00, 08:00, 16:00 UTC.
    Для LONG позиции: при положительной ставке вы ПЛАТИТЕ
                       при отрицательной — ПОЛУЧАЕТЕ
    Для SHORT — наоборот.
    """

    FUNDING_HOURS_UTC = (0, 8, 16)
    FUNDING_INTERVAL_SECONDS = 8 * 3600

    def __init__(self):
        self.last_rates: dict = {}   # symbol → last_funding_rate
        self.paid: dict = {}         # symbol → total paid/received

    def update_rate(self, symbol: str, rate: float):
        """Обновляем сохранённую ставку funding."""
        self.last_rates[symbol] = rate

    def estimate_next_funding_seconds(self) -> int:
        """Сколько секунд до следующего funding."""
        now = datetime.now(timezone.utc)
        # Найдём ближайший час из FUNDING_HOURS_UTC
        for h in self.FUNDING_HOURS_UTC:
            target = now.replace(hour=h, minute=0, second=0, microsecond=0)
            if target > now:
                return int((target - now).total_seconds())
        # Если все часы прошли — следующий в 00:00 завтра
        target = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        return int((target - now).total_seconds())

    def is_near_funding(self, threshold_seconds: int = 300) -> bool:
        """True если до funding осталось < 5 минут (по умолчанию)."""
        return self.estimate_next_funding_seconds() <= threshold_seconds

    def estimate_funding_cost(self, symbol: str, side: str,
                               notional_usdt: float) -> float:
        """
        Оценка стоимости funding на следующем расчёте.
        Возвращает положительное число = вы заплатите,
                  отрицательное число = вы получите.
        """
        rate = self.last_rates.get(symbol, 0.0001)  # default 0.01%
        if side == "Buy":  # LONG
            return notional_usdt * rate
        else:              # SHORT
            return -notional_usdt * rate

    def track_payment(self, symbol: str, amount: float):
        """Учесть фактический funding payment."""
        self.paid[symbol] = self.paid.get(symbol, 0) + amount

    def total_paid(self) -> float:
        """Суммарно потрачено на funding по всем парам."""
        return sum(self.paid.values())
