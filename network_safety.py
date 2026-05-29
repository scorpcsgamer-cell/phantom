"""
Модуль сетевой устойчивости.

Включает:
  • Retry с exponential backoff
  • Rate limiter (защита от превышения лимитов Bybit)
  • Circuit breaker (выключение бота при многократных ошибках API)
  • Watchdog (детектор обрывов связи)
"""

import asyncio
import logging
import time
from collections import deque
from typing import Callable, Awaitable, Optional, Any

log = logging.getLogger("APEX.net")


# ─────────────────────────────────────────────────
# RATE LIMITER
# ─────────────────────────────────────────────────
class RateLimiter:
    """
    Token bucket для соблюдения лимитов Bybit.
    Bybit V5: ~10 req/sec для market data, до 50 req/sec для trade endpoints.
    Ставим консервативный лимит 8 req/sec для безопасности.
    """

    def __init__(self, max_per_second: int = 8):
        self.max_per_sec = max_per_second
        self.tokens      = float(max_per_second)
        self.last_refill = time.monotonic()
        self._lock       = asyncio.Lock()

    async def acquire(self):
        """Блокирует, пока не появится токен."""
        async with self._lock:
            while True:
                now      = time.monotonic()
                elapsed  = now - self.last_refill
                self.tokens = min(self.max_per_sec,
                                  self.tokens + elapsed * self.max_per_sec)
                self.last_refill = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                # Ждём пока пополнится хотя бы 1 токен
                wait = (1 - self.tokens) / self.max_per_sec
                await asyncio.sleep(wait)


# ─────────────────────────────────────────────────
# RETRY DECORATOR
# ─────────────────────────────────────────────────
async def retry_with_backoff(
    func: Callable[..., Awaitable],
    *args,
    max_attempts: int = 4,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    retry_on_codes: tuple = (10002, 10006, 10018),  # rate limit / timeout / IP banned
    **kwargs
) -> Optional[Any]:
    """
    Вызывает async-функцию с retry и exponential backoff.
    Возвращает результат или None при провале всех попыток.
    """
    delay = base_delay
    last_err = None
    for attempt in range(1, max_attempts + 1):
        try:
            result = await func(*args, **kwargs)
            # Bybit API: проверим retCode для retryable errors
            if isinstance(result, dict):
                code = result.get("retCode")
                if code in retry_on_codes:
                    raise RuntimeError(f"Bybit retCode {code}: {result.get('retMsg','')}")
            return result
        except Exception as e:
            last_err = e
            if attempt < max_attempts:
                log.warning(f"Попытка {attempt}/{max_attempts} провалилась: {e}, "
                            f"retry через {delay:.1f}s")
                await asyncio.sleep(delay)
                delay = min(delay * 2, max_delay)
            else:
                log.error(f"Все {max_attempts} попыток провалились: {e}")
    return None


# ─────────────────────────────────────────────────
# CIRCUIT BREAKER
# ─────────────────────────────────────────────────
class CircuitBreaker:
    """
    Если за N секунд произошло M ошибок — открываем "circuit",
    запросы блокируются на cooldown_seconds. Защищает от каскада ошибок.

    States:
      CLOSED   = всё работает нормально
      OPEN     = слишком много ошибок, блокируем запросы
      HALF_OPEN = после cooldown пробуем 1 запрос
    """

    CLOSED    = "closed"
    OPEN      = "open"
    HALF_OPEN = "half_open"

    def __init__(self, error_threshold: int = 10,
                 window_seconds: int = 60,
                 cooldown_seconds: int = 60):
        self.error_threshold  = error_threshold
        self.window_seconds   = window_seconds
        self.cooldown_seconds = cooldown_seconds
        self.errors           = deque(maxlen=100)
        self.state            = self.CLOSED
        self.opened_at        = 0.0

    def record_success(self):
        if self.state == self.HALF_OPEN:
            log.info("Circuit breaker: HALF_OPEN → CLOSED (recovery)")
            self.state = self.CLOSED
            self.errors.clear()

    def record_error(self):
        now = time.monotonic()
        self.errors.append(now)
        # Удалим старые ошибки за пределами окна
        cutoff = now - self.window_seconds
        while self.errors and self.errors[0] < cutoff:
            self.errors.popleft()

        if (self.state == self.CLOSED and
            len(self.errors) >= self.error_threshold):
            log.error(f"Circuit breaker: CLOSED → OPEN "
                      f"(>{self.error_threshold} ошибок за {self.window_seconds}s)")
            self.state = self.OPEN
            self.opened_at = now
        elif self.state == self.HALF_OPEN:
            log.warning("Circuit breaker: HALF_OPEN → OPEN (re-failure)")
            self.state = self.OPEN
            self.opened_at = now

    def can_attempt(self) -> bool:
        if self.state == self.CLOSED:
            return True
        if self.state == self.OPEN:
            if time.monotonic() - self.opened_at >= self.cooldown_seconds:
                log.info("Circuit breaker: OPEN → HALF_OPEN (cooldown elapsed)")
                self.state = self.HALF_OPEN
                return True
            return False
        return True   # HALF_OPEN — пробуем

    @property
    def is_open(self) -> bool:
        return self.state == self.OPEN


# ─────────────────────────────────────────────────
# WATCHDOG
# ─────────────────────────────────────────────────
class NetworkWatchdog:
    """
    Следит за временем последнего успешного запроса.
    Если связь пропала на N минут — алертит и закрывает все позиции
    (по требованию).
    """

    def __init__(self, alert_after_seconds: int = 120,
                 panic_close_after_seconds: int = 600):
        self.alert_after  = alert_after_seconds
        self.panic_after  = panic_close_after_seconds
        self.last_success = time.monotonic()
        self.alerted      = False

    def heartbeat(self):
        """Вызывать после каждого успешного API запроса."""
        self.last_success = time.monotonic()
        self.alerted = False

    def silence_seconds(self) -> float:
        return time.monotonic() - self.last_success

    def should_alert(self) -> bool:
        s = self.silence_seconds()
        if s >= self.alert_after and not self.alerted:
            self.alerted = True
            return True
        return False

    def should_panic_close(self) -> bool:
        return self.silence_seconds() >= self.panic_after
