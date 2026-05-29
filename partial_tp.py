"""
Partial Take-Profit Manager: многоуровневая фиксация прибыли.

Концепция:
  Вместо одного общего TP на 100% позиции, бот закрывает позицию
  по частям при достижении нескольких уровней прибыли.

  Пример конфигурации (TP_LEVELS=3:50,6:30,10:20):
    • При +3% → закрыть 50% позиции (TP1)
    • При +6% → закрыть ещё 30% (TP2)
    • При +10% → закрыть остаток 20% (TP3)

  Дополнительно:
    • После TP1 — SL переносится в break-even (точку входа)
    • После TP3 — оставшаяся часть управляется только trailing-стопом

Преимущества:
  ✅ Гарантированная фиксация части прибыли при движении в плюс
  ✅ Психологически легче выдерживать просадки
  ✅ Защита от ситуации "цена дошла до +5%, потом откатилась в SL"
  ✅ Остаток позиции "ловит" большие движения

Минусы:
  ⚠️ Средний выигрыш меньше чем при одиночном TP
  ⚠️ Больше комиссий (несколько закрытий вместо одного)
  ⚠️ Требует более сложной логики отслеживания
"""

import logging
import time
from dataclasses import dataclass, field, asdict
from typing import List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from bot_server import TradingBot

log = logging.getLogger("PHANTOM.partial_tp")


@dataclass
class TPLevel:
    """Один уровень частичной фиксации."""
    pct: float           # % движения цены от входа
    close_share: float   # доля позиции к закрытию (0.0-1.0)
    triggered: bool = False
    triggered_at: Optional[float] = None  # timestamp
    triggered_price: float = 0.0
    closed_qty: float = 0.0
    realized_pnl: float = 0.0


@dataclass
class PartialTPConfig:
    """Конфигурация partial TP для позиции."""
    enabled: bool = True
    levels: List[TPLevel] = field(default_factory=list)
    move_sl_to_be_after_tp1: bool = True
    trail_after_last_tp: bool = True
    # Если все уровни сработали — добавочный trailing на остаток
    trail_pct_after_all_tp: float = 1.5

    @classmethod
    def from_env_string(cls, levels_str: str, **kwargs) -> "PartialTPConfig":
        """
        Парсит строку формата "3:50,6:30,10:20" в список уровней.
        """
        levels = []
        if not levels_str:
            return cls(enabled=False, **kwargs)
        try:
            for part in levels_str.split(","):
                part = part.strip()
                if not part: continue
                pct_str, share_str = part.split(":")
                levels.append(TPLevel(
                    pct=float(pct_str.strip()),
                    close_share=float(share_str.strip()) / 100.0
                ))
            # Проверка: сумма close_share должна быть ≈ 1.0
            total = sum(l.close_share for l in levels)
            if abs(total - 1.0) > 0.01:
                log.warning(f"TP_LEVELS sum = {total*100:.0f}% (≠ 100%). "
                            f"Последний уровень будет скорректирован.")
                if levels:
                    levels[-1].close_share += (1.0 - total)
            # Сортируем по pct по возрастанию
            levels.sort(key=lambda l: l.pct)
            return cls(enabled=True, levels=levels, **kwargs)
        except Exception as e:
            log.error(f"Ошибка парсинга TP_LEVELS='{levels_str}': {e}")
            return cls(enabled=False, **kwargs)


class PartialTPState:
    """Состояние partial TP для одной позиции (хранится в position dict)."""

    @staticmethod
    def init_for_position(config: PartialTPConfig) -> dict:
        """Создаёт начальное состояние partial TP при открытии позиции."""
        if not config.enabled or not config.levels:
            return {"enabled": False, "levels": []}
        return {
            "enabled": True,
            "levels": [asdict(level) for level in config.levels],
            "move_sl_to_be_after_tp1": config.move_sl_to_be_after_tp1,
            "trail_after_last_tp": config.trail_after_last_tp,
            "trail_pct_after_all_tp": config.trail_pct_after_all_tp,
            "all_triggered": False,
            "remaining_qty": None,  # установится при первом обновлении
        }


class PartialTPManager:
    """Управляет частичной фиксацией прибыли."""

    def __init__(self, bot: "TradingBot", config: PartialTPConfig):
        self.bot = bot
        self.config = config

    def calculate_pnl_pct(self, position: dict, current_price: float) -> float:
        """% движения цены от входа в направлении позиции."""
        entry = position.get("entry", 0)
        if entry <= 0 or current_price <= 0: return 0.0
        if position.get("side") == "Buy":
            return (current_price - entry) / entry * 100
        else:
            return (entry - current_price) / entry * 100

    async def check_and_execute(self, symbol: str, position: dict,
                                  current_price: float) -> dict:
        """
        Проверяет partial TP уровни и закрывает части позиции при необходимости.

        Возвращает: {
            "triggered": [{level_idx, qty, pnl}, ...],
            "sl_moved_to_be": bool,
            "all_done": bool,
        }
        """
        result = {"triggered": [], "sl_moved_to_be": False, "all_done": False}

        ptp_state = position.get("partial_tp")
        if not ptp_state or not ptp_state.get("enabled"):
            return result

        levels = ptp_state.get("levels", [])
        if not levels: return result

        pnl_pct = self.calculate_pnl_pct(position, current_price)
        if pnl_pct <= 0:
            return result  # позиция в убытке, не закрываем

        # Если ещё не сохранили initial qty — сохраним
        if ptp_state.get("remaining_qty") is None:
            ptp_state["remaining_qty"] = position["qty"]

        # Проверяем каждый уровень по порядку
        for idx, level in enumerate(levels):
            if level["triggered"]: continue
            if pnl_pct < level["pct"]: break  # уровни отсортированы, дальше не сработают

            # Срабатывает!
            close_share = level["close_share"]
            close_qty = position["qty"] * close_share
            # Округляем по qty_step
            qty_str, qty_f = self.bot.instruments.round_qty(symbol, close_qty)
            if qty_str is None or qty_f <= 0:
                log.warning(f"[Partial-TP] {symbol} TP{idx+1}: qty {close_qty} меньше min")
                # Помечаем уровень как triggered чтобы не повторять
                level["triggered"] = True
                level["triggered_at"] = time.time()
                continue

            # Если осталось мало — закрываем всё что есть
            remaining = ptp_state.get("remaining_qty", position["qty"])
            if qty_f > remaining * 0.95:
                qty_f = remaining
                qty_str = self.bot.instruments.round_qty(symbol, qty_f)[0] or qty_str

            log.info(f"[Partial-TP] {symbol} TP{idx+1}: +{pnl_pct:.2f}% ≥ {level['pct']}%, "
                     f"закрываю {close_share*100:.0f}% ({qty_str})")

            # Размещаем reduce-only ордер
            success, pnl = await self._close_partial(symbol, position, qty_f, current_price)
            if success:
                level["triggered"] = True
                level["triggered_at"] = time.time()
                level["triggered_price"] = current_price
                level["closed_qty"] = qty_f
                level["realized_pnl"] = pnl
                ptp_state["remaining_qty"] = max(0, remaining - qty_f)
                # Уменьшаем qty в основной позиции
                position["qty"] = ptp_state["remaining_qty"]
                position["size_usdt"] = position["qty"] * position["entry"]
                result["triggered"].append({
                    "level_idx": idx,
                    "level_pct": level["pct"],
                    "qty": qty_f,
                    "pnl": pnl,
                    "price": current_price,
                })

                # После TP1 → SL в break-even
                if idx == 0 and ptp_state.get("move_sl_to_be_after_tp1"):
                    new_sl = position["entry"]
                    if position["side"] == "Buy" and new_sl > position["sl"]:
                        position["sl"] = new_sl
                        result["sl_moved_to_be"] = True
                        await self._update_sl_on_exchange(symbol, new_sl)
                        log.info(f"[Partial-TP] {symbol} SL → break-even ${new_sl:.6g}")
                    elif position["side"] == "Sell" and new_sl < position["sl"]:
                        position["sl"] = new_sl
                        result["sl_moved_to_be"] = True
                        await self._update_sl_on_exchange(symbol, new_sl)
                        log.info(f"[Partial-TP] {symbol} SL → break-even ${new_sl:.6g}")

        # Все уровни отработали?
        all_triggered = all(l["triggered"] for l in levels)
        if all_triggered and not ptp_state.get("all_triggered"):
            ptp_state["all_triggered"] = True
            result["all_done"] = True
            log.info(f"[Partial-TP] {symbol}: все TP уровни отработали, "
                     f"остаток qty={ptp_state['remaining_qty']:.6g}")

        return result

    async def _close_partial(self, symbol: str, position: dict,
                              qty: float, current_price: float) -> tuple:
        """
        Закрывает часть позиции через market reduce-only ордер.
        Возвращает (success, realized_pnl_net).
        """
        try:
            qty_str, qty_f = self.bot.instruments.round_qty(symbol, qty)
            if qty_str is None: return False, 0.0
            pos_idx = self.bot.position_mode.position_idx
            r = await self.bot.client.close_market(
                symbol, position["side"], qty_str, position_idx=pos_idx
            )
            if r.get("retCode") != 0:
                log.error(f"[Partial-TP] {symbol}: close failed: "
                          f"{r.get('retCode')} {r.get('retMsg')}")
                return False, 0.0

            # PnL gross
            slip = self.bot.cfg_slippage_pct / 100 if hasattr(self.bot, 'cfg_slippage_pct') else 0.0005
            entry = position["entry"]
            if position["side"] == "Buy":
                exit_eff = current_price * (1 - slip)
                diff = exit_eff - entry
            else:
                exit_eff = current_price * (1 + slip)
                diff = entry - exit_eff
            notional = qty_f * entry
            gross_pnl = (diff / entry) * notional
            # Fees
            from fees_funding import FeeCalculator
            fees = notional * FeeCalculator.TAKER_FEE  # только закрытие, открытие уже учтено отдельно
            net_pnl = gross_pnl - fees

            # Обновляем балансы и метрики бота
            self.bot.balance += net_pnl
            self.bot.pm.fees_total += fees
            self.bot.pm.daily_pnl += net_pnl
            self.bot.pm.total_pnl += net_pnl
            self.bot.pm.equity_hist.append(self.bot.balance)

            return True, net_pnl
        except Exception as e:
            log.error(f"[Partial-TP] _close_partial {symbol}: {e}")
            return False, 0.0

    async def _update_sl_on_exchange(self, symbol: str, new_sl: float):
        """Обновляет stopLoss на бирже через trading-stop endpoint."""
        try:
            sl_str = self.bot.instruments.round_price(symbol, new_sl)
            r = await self.bot.client.post("/v5/position/trading-stop", {
                "category": "linear",
                "symbol": symbol,
                "stopLoss": sl_str,
                "positionIdx": self.bot.position_mode.position_idx,
            })
            if r.get("retCode") == 0:
                log.info(f"[Partial-TP] {symbol} SL обновлён на бирже: {sl_str}")
            else:
                log.warning(f"[Partial-TP] {symbol} SL update failed: "
                            f"{r.get('retCode')} {r.get('retMsg')}")
        except Exception as e:
            log.error(f"[Partial-TP] update_sl_on_exchange {symbol}: {e}")

    @staticmethod
    def get_summary(position: dict) -> dict:
        """Краткая сводка состояния partial TP для отображения."""
        ptp = position.get("partial_tp")
        if not ptp or not ptp.get("enabled"):
            return {"enabled": False}
        levels = ptp.get("levels", [])
        triggered_count = sum(1 for l in levels if l["triggered"])
        total_realized = sum(l.get("realized_pnl", 0) for l in levels)
        return {
            "enabled": True,
            "total_levels": len(levels),
            "triggered_count": triggered_count,
            "remaining_qty": ptp.get("remaining_qty", 0),
            "realized_pnl": round(total_realized, 4),
            "all_done": ptp.get("all_triggered", False),
            "levels": [{
                "pct": l["pct"],
                "share": int(l["close_share"] * 100),
                "triggered": l["triggered"],
                "pnl": round(l.get("realized_pnl", 0), 4),
            } for l in levels],
        }
