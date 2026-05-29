"""
Order Health: обработка частичных исполнений и троттлинг критических ошибок.

Партиал-филлы (partial fills) на market orders маловероятны, но возможны
на низколиквидных альткоинах. Если ордер исполнен частично:
  - Реальная qty отличается от заявленной
  - Размер позиции меньше расчётного
  - SL/TP могут быть выставлены некорректно

Критические ошибки API (insufficient margin, qty exceeds max и т.д.):
  - Если за 60 секунд произошло >= 5 таких ошибок → пауза бота на 5 минут
  - Это защита от каскадных проблем (например, маржа упала ниже порога)
"""

import logging
import time
from collections import deque
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from bot_server import ExchangeClientWrapper, TradingBot

log = logging.getLogger("PHANTOM.orderhealth")


# OKX V5 error codes → category
# Адаптер exchange_client.place_order пробрасывает OKX sCode в retCode,
# поэтому здесь должны быть OKX-коды, а не Bybit (раньше тут были 110007
# и т.д. — они никогда не срабатывали, троттлинг был мёртвым).
# Список: https://www.okx.com/docs-v5/en/#error-code-rest
CRITICAL_ERROR_CODES = {
    # Margin / balance issues
    51008:  "Insufficient USDT balance / position margin",
    51009:  "Insufficient USDT margin for derivatives",
    51131:  "Insufficient balance to place order",
    51136:  "Insufficient leverage / margin tier",
    58110:  "Withdrawal/transfer limit exceeded",
    # Position size / risk limit
    51004:  "Order amount exceeds current tier limit",
    51010:  "Order amount too small",
    51020:  "Position size exceeds current tier limit",
    51169:  "Order quantity exceeds the maximum",
    # Invalid params / state
    51119:  "Order placement failed",
    51400:  "Cancel order failed",
    51401:  "Cancellation failed: order does not exist or already filled",
    # Account state issues
    50006:  "Account is frozen / restricted",
    50026:  "System error / partial outage",
    # Position mode
    51022:  "Position mode mismatch",
    # Rate limiting (если упёрлись в API лимит — тоже временный стоп)
    50011:  "Rate limit exceeded",
    50013:  "System is busy, please try again later",
}


class OrderHealthMonitor:
    """Отслеживает критические ошибки и применяет troттлинг."""

    def __init__(self, bot: "TradingBot",
                 error_threshold: int = 5,
                 window_seconds: int = 60,
                 pause_seconds: int = 300):
        self.bot              = bot
        self.error_threshold  = error_threshold
        self.window_seconds   = window_seconds
        self.pause_seconds    = pause_seconds
        self.errors           = deque(maxlen=200)   # (timestamp, code, msg)
        self.paused_until     = 0.0

    def record_order_response(self, response: dict, symbol: str = "?") -> bool:
        """
        Анализирует ответ Bybit на place_order. Возвращает:
          True  — ошибка зафиксирована
          False — ошибки нет
        """
        if not response: return False
        code = response.get("retCode", 0)
        if code == 0: return False
        msg = response.get("retMsg", "")
        # Записываем только критические ошибки (для троттлинга)
        if code in CRITICAL_ERROR_CODES:
            self.errors.append({
                "ts": time.monotonic(),
                "code": code, "msg": msg, "symbol": symbol,
            })
            self._cleanup_old_errors()
            log.warning(f"[ORDER-HEALTH] {symbol} critical error {code}: {msg}")
            # Проверим threshold
            if len(self.errors) >= self.error_threshold:
                self._trigger_pause(code)
            return True
        return False

    def _cleanup_old_errors(self):
        cutoff = time.monotonic() - self.window_seconds
        while self.errors and self.errors[0]["ts"] < cutoff:
            self.errors.popleft()

    def _trigger_pause(self, last_code: int):
        """Активируем паузу бота."""
        self.paused_until = time.monotonic() + self.pause_seconds
        codes = list(set(e["code"] for e in self.errors))
        log.error(f"🛑 ORDER HEALTH: {len(self.errors)} ошибок за {self.window_seconds}с "
                  f"(коды: {codes}). Бот на паузе {self.pause_seconds}с")
        # Сообщим боту чтоб он временно перестал открывать позиции
        if hasattr(self.bot, "log"):
            self.bot.log("🛑", f"Order Health: пауза {self.pause_seconds}с "
                                f"({len(self.errors)} ошибок API за {self.window_seconds}с)", "risk")
        # Опциональный callback для алертов
        if hasattr(self.bot, "alert_critical"):
            try:
                import asyncio
                asyncio.create_task(
                    self.bot.alert_critical(
                        f"Order Health Pause: {len(self.errors)} critical errors. Codes: {codes}",
                        severity="critical"
                    )
                )
            except Exception: pass

    @property
    def is_paused(self) -> bool:
        if self.paused_until == 0: return False
        if time.monotonic() < self.paused_until: return True
        # Снимаем паузу
        self.paused_until = 0
        self.errors.clear()
        log.info("[ORDER-HEALTH] Пауза снята, ошибки очищены")
        return False

    def remaining_pause_seconds(self) -> int:
        if not self.is_paused: return 0
        return max(0, int(self.paused_until - time.monotonic()))

    def reset(self):
        self.errors.clear()
        self.paused_until = 0


def detect_partial_fill(order_response: dict, requested_qty: float) -> Optional[float]:
    """
    Проверяет partial fill по OKX-ответу на market order.

    OKX market order для SWAP в isolated mode обычно исполняется полностью,
    но при низкой ликвидности (мемы, тонкие пары) возможен частичный fill.

    Адаптер exchange_client.place_order сейчас не возвращает fillSz и accFillSz,
    но в OKX raw response data[0] эти поля есть. Мы аккуратно вытаскиваем их:

    Returns:
        None  — если partial fill не обнаружен или данных нет
        float — реальная исполненная qty, если она < requested_qty * 0.99

    ВАЖНО: для надёжной детекции рекомендуется параллельно делать reconcile
    через 2-3 секунды после ордера — exchange.get_positions() даст точную qty.
    """
    if not order_response: return None

    # Адаптер сворачивает успех в retCode=0, но может не пробросить fillSz.
    # Если есть raw_data — попробуем оттуда, иначе считаем что fill полный.
    result = order_response.get("result", {}) or {}
    raw = result.get("raw") or {}
    data = raw.get("data") or []
    if not data or not isinstance(data, list): return None
    first = data[0] if isinstance(data[0], dict) else {}

    try:
        # OKX поля для market order: accFillSz (накопленный fill),
        # fillSz (последний fill). Для market они обычно совпадают.
        filled = first.get("accFillSz") or first.get("fillSz")
        if filled is None or filled == "":
            return None
        filled_f = float(filled)
        if filled_f <= 0:
            return None
        if filled_f < requested_qty * 0.99:
            log.warning(f"[ORDER-HEALTH] Partial fill: requested={requested_qty}, "
                        f"filled={filled_f} ({filled_f/requested_qty*100:.1f}%)")
            return filled_f
    except (ValueError, TypeError):
        return None
    return None
