"""
Position Mode Manager для OKX.

OKX поддерживает два режима для perpetual:
  • net_mode (one-way) — одна позиция на пару
  • long_short_mode (hedge) — отдельные long и short позиции

В отличие от Bybit, OKX не требует переключения для каждой пары —
режим устанавливается на уровне аккаунта.

Бот работает только в net_mode (one-way).
"""

import logging
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from bot_server_okx import ExchangeClientWrapper

log = logging.getLogger("PHANTOM.posmode")


class PositionModeManager:
    """Определяет и управляет режимом позиций на OKX."""

    MODE_ONE_WAY = "one_way"
    MODE_HEDGE = "hedge"
    MODE_UNKNOWN = "unknown"

    def __init__(self, client: "ExchangeClientWrapper"):
        self.client = client
        self.current_mode: str = self.MODE_UNKNOWN
        self.position_idx: int = 0

    async def detect_mode(self) -> str:
        """
        Определяет режим через /api/v5/account/config.
        В ответе есть поле 'posMode': 'net_mode' или 'long_short_mode'.
        """
        try:
            r = await self.client.get("/api/v5/account/config")
            if r.get("code") == "0":
                data = r.get("data", [])
                if data:
                    pos_mode = data[0].get("posMode", "")
                    if pos_mode == "net_mode":
                        self.current_mode = self.MODE_ONE_WAY
                        self.position_idx = 0
                        log.info("OKX: Net Mode (one-way) подтверждён")
                        return self.current_mode
                    elif pos_mode == "long_short_mode":
                        self.current_mode = self.MODE_HEDGE
                        log.info("OKX: Long/Short Mode (hedge) обнаружен")
                        return self.current_mode
            log.warning(f"Не удалось определить режим OKX: {r.get('msg', 'no data')}")
            return self.MODE_UNKNOWN
        except Exception as e:
            log.error(f"OKX detect_mode error: {e}")
            return self.MODE_UNKNOWN

    async def ensure_one_way(self, auto_switch: bool = False) -> tuple:
        """Убеждается что режим one-way."""
        mode = await self.detect_mode()
        if mode == self.MODE_ONE_WAY:
            return True, "OKX Net Mode (One-Way) подтверждён"
        if mode == self.MODE_UNKNOWN:
            return False, "Не удалось определить режим OKX (проверьте API ключи и passphrase)"
        if mode == self.MODE_HEDGE:
            if not auto_switch:
                return False, ("OKX в Hedge Mode (Long/Short). Переключите вручную: "
                                "Setting → Position Mode → Net mode. "
                                "Или установите AUTO_SWITCH_POSITION_MODE=true")
            success = await self._switch_to_one_way()
            if success:
                self.current_mode = self.MODE_ONE_WAY
                self.position_idx = 0
                return True, "Успешно переключён в OKX Net Mode"
            return False, "Не удалось переключить режим OKX автоматически"
        return False, f"Неизвестное состояние: {mode}"

    async def _switch_to_one_way(self) -> bool:
        """Переключение в net_mode на OKX."""
        try:
            # Сначала проверим что нет открытых позиций
            r = await self.client.get("/api/v5/account/positions",
                                       {"instType": "SWAP"})
            if r.get("code") == "0":
                positions = [p for p in r.get("data", [])
                             if float(p.get("pos", 0) or 0) != 0]
                if positions:
                    log.error(f"Нельзя переключить mode — есть {len(positions)} открытых позиций")
                    return False

            # Переключаем
            r = await self.client.post("/api/v5/account/set-position-mode",
                                        {"posMode": "net_mode"})
            if r.get("code") == "0":
                log.info("OKX position mode переключён на net_mode")
                return True
            log.error(f"OKX set-position-mode failed: {r.get('msg')}")
            return False
        except Exception as e:
            log.error(f"OKX switch_to_one_way exception: {e}")
            return False
