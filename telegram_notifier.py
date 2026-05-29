"""
Telegram Notifier: отправка критических алертов и daily summary в Telegram.

Настройка:
  1. Создайте бота через @BotFather → получите BOT_TOKEN
  2. Получите свой chat_id через @userinfobot
  3. В .env установите:
     TELEGRAM_BOT_TOKEN=...
     TELEGRAM_CHAT_ID=...
     TELEGRAM_ENABLED=true

Уведомления:
  • Bot started/stopped
  • Position opened/closed (важные)
  • Drawdown guard сработал
  • Network panic-close
  • Liquidation warning
  • Order health pause
  • Daily summary в 00:00 UTC
"""

import asyncio
import logging
import os
import time
from collections import deque
from typing import Optional

try:
    import httpx
    HAS_HTTPX = True
except ImportError:
    HAS_HTTPX = False
    httpx = None

log = logging.getLogger("PHANTOM.telegram")


class TelegramNotifier:
    """Async-отправитель сообщений в Telegram."""

    SEVERITY_ICONS = {
        "info":     "ℹ️",
        "success":  "✅",
        "warning":  "⚠️",
        "critical": "🚨",
        "trade":    "💰",
        "summary":  "📊",
    }

    def __init__(self, bot_token: str = "", chat_id: str = "",
                 enabled: bool = False, throttle_seconds: int = 5):
        self.bot_token = bot_token or os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.chat_id   = chat_id or os.getenv("TELEGRAM_CHAT_ID", "")
        self.enabled   = enabled and bool(self.bot_token) and bool(self.chat_id) and HAS_HTTPX
        self.throttle  = throttle_seconds
        self._last_sent_at = {}  # severity → timestamp (anti-flood)
        self._client: Optional["httpx.AsyncClient"] = None
        self._queue: deque = deque(maxlen=100)
        self._send_task: Optional[asyncio.Task] = None

        if not HAS_HTTPX and (self.bot_token or self.chat_id):
            log.warning("Telegram requires httpx — install with: pip install httpx")
        if self.enabled:
            log.info(f"Telegram notifier enabled (chat_id={self.chat_id[:6]}...)")

    async def init(self):
        if not self.enabled: return
        self._client = httpx.AsyncClient(timeout=8)
        self._send_task = asyncio.create_task(self._sender_loop())

    async def close(self):
        if self._send_task and not self._send_task.done():
            self._send_task.cancel()
            try: await self._send_task
            except (asyncio.CancelledError, Exception): pass
        if self._client:
            await self._client.aclose()
            self._client = None

    async def send(self, message: str, severity: str = "info",
                    silent: bool = False) -> bool:
        """
        Добавляет сообщение в очередь отправки.
        severity: info | success | warning | critical | trade | summary
        silent: True = тихое уведомление (без звука)
        """
        if not self.enabled: return False
        # Anti-flood: одинаковые severity не чаще throttle_seconds
        now = time.monotonic()
        last = self._last_sent_at.get(severity, 0)
        if now - last < self.throttle and severity != "critical":
            return False
        self._last_sent_at[severity] = now

        icon = self.SEVERITY_ICONS.get(severity, "")
        text = f"{icon} {message}" if icon else message
        self._queue.append({"text": text, "silent": silent})
        return True

    async def send_now(self, message: str, severity: str = "info",
                        silent: bool = False) -> bool:
        """Срочная отправка без очереди (для критических случаев)."""
        if not self.enabled or not self._client: return False
        icon = self.SEVERITY_ICONS.get(severity, "")
        text = f"{icon} {message}" if icon else message
        return await self._do_send(text, silent)

    async def _do_send(self, text: str, silent: bool = False) -> bool:
        if not self._client: return False
        try:
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            r = await self._client.post(url, json={
                "chat_id": self.chat_id,
                "text": text[:4000],   # Telegram max 4096
                "parse_mode": "HTML",
                "disable_notification": silent,
                "disable_web_page_preview": True,
            })
            if r.status_code == 200:
                return True
            if r.status_code == 429:
                # Rate limited
                ra = r.json().get("parameters", {}).get("retry_after", 30)
                log.warning(f"Telegram rate limit, retry after {ra}s")
                await asyncio.sleep(ra)
                return False
            log.warning(f"Telegram API status {r.status_code}: {r.text[:200]}")
            return False
        except Exception as e:
            log.warning(f"Telegram send failed: {e}")
            return False

    async def _sender_loop(self):
        """Фоновая задача отправки из очереди."""
        try:
            while True:
                await asyncio.sleep(0.5)
                if not self._queue: continue
                msg = self._queue.popleft()
                await self._do_send(msg["text"], msg["silent"])
                # Bybit Telegram API ограничивает 30 сообщений/сек, мы шлём 2/сек
                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            pass

    # ── Helpers для типичных событий ─────────────
    async def notify_bot_started(self, balance: float, mode: str = "TESTNET"):
        await self.send(f"<b>PHANTOM запущен</b>\n"
                         f"Режим: {mode}\nБаланс: ${balance:.2f}",
                         severity="success", silent=True)

    async def notify_bot_stopped(self, reason: str = "manual"):
        await self.send(f"<b>Бот остановлен</b> ({reason})", severity="warning")

    async def notify_position_opened(self, symbol: str, side: str,
                                       price: float, size_usdt: float,
                                       sl: float, tp: float):
        msg = (f"<b>Открыта позиция</b>\n"
               f"{side} {symbol} @ ${price:.6g}\n"
               f"Размер: ${size_usdt:.2f}\n"
               f"SL: ${sl:.6g}  TP: ${tp:.6g}")
        await self.send(msg, severity="trade", silent=True)

    async def notify_position_closed(self, symbol: str, pnl: float,
                                       reason: str, balance: float):
        emoji = "✅" if pnl >= 0 else "❌"
        sign = "+" if pnl >= 0 else ""
        msg  = (f"<b>{emoji} Закрыта {symbol}</b>\n"
                f"PnL: {sign}${pnl:.2f}\n"
                f"Причина: {reason}\n"
                f"Баланс: ${balance:.2f}")
        await self.send(msg, severity="trade", silent=True)

    async def notify_critical(self, message: str):
        await self.send(message, severity="critical", silent=False)

    async def notify_daily_summary(self, stats: dict):
        msg = (f"<b>📊 Daily Summary</b>\n"
               f"━━━━━━━━━━━━━━━━━\n"
               f"Сделок:    {stats.get('trades_today', 0)}\n"
               f"Win Rate:  {stats.get('win_rate', 0):.1f}%\n"
               f"PnL день:  ${stats.get('daily_pnl', 0):+.2f}\n"
               f"PnL всего: ${stats.get('total_pnl', 0):+.2f}\n"
               f"Баланс:    ${stats.get('balance', 0):.2f}\n"
               f"ROI:       {stats.get('roi_pct', 0):+.2f}%\n"
               f"Drawdown:  {stats.get('drawdown', 0):.2f}%\n"
               f"Позиций:   {stats.get('positions', 0)}")
        await self.send(msg, severity="summary", silent=True)
