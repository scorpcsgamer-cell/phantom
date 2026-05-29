"""
Liquidation Monitor: следит за дистанцией позиции до цены ликвидации.

Bybit для каждой открытой позиции возвращает поле liqPrice.
Если markPrice приближается к liqPrice ближе чем на N% — алерт + опционально
закрытие позиции (защита от ликвидации, которая дороже SL).

Также проверяет статус SL/TP ордеров на бирже:
  - Если бот поставил SL/TP, а на бирже их нет (биржа отменила) → восстанавливает.
  - Если SL/TP сильно изменены вручную пользователем → лог-предупреждение.
"""

import logging
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from bot_server import TradingBot

log = logging.getLogger("PHANTOM.liq")


class LiquidationMonitor:
    """Следит за дистанцией до ликвидации и состоянием SL/TP."""

    def __init__(self, bot: "TradingBot",
                 critical_distance_pct: float = 30.0,
                 emergency_distance_pct: float = 15.0):
        """
        critical_distance_pct: при дистанции менее этого % — алерт
        emergency_distance_pct: при дистанции менее этого — экстренное закрытие
        """
        self.bot = bot
        self.critical_pct  = critical_distance_pct
        self.emergency_pct = emergency_distance_pct
        self.last_check    = 0.0
        self.alerted_symbols: set = set()
        self.sl_tp_drift_log: list = []   # лог расхождений с биржей

    async def check_all(self) -> dict:
        """
        Проверяет все открытые позиции:
          1. Дистанцию до liqPrice
          2. Соответствие SL/TP с биржевыми
        Возвращает: {alerts: [...], emergency_closes: [...], sl_tp_drift: [...]}
        """
        result = {"alerts": [], "emergency_closes": [], "sl_tp_drift": []}
        if not self.bot.pm.positions:
            return result

        try:
            ex_positions = await self.bot.client.get_positions()
            ex_map = {p["symbol"]: p for p in ex_positions}

            for symbol, local_pos in list(self.bot.pm.positions.items()):
                ex_pos = ex_map.get(symbol)
                if not ex_pos:
                    continue   # обработает Reconciler

                liq_price  = float(ex_pos.get("liqPrice", 0) or 0)
                mark_price = float(ex_pos.get("markPrice", 0) or 0)
                ex_sl      = float(ex_pos.get("stopLoss", 0) or 0)
                ex_tp      = float(ex_pos.get("takeProfit", 0) or 0)
                side       = ex_pos.get("side", local_pos.get("side"))

                # ── 1. Liquidation distance ─────────────────
                if liq_price > 0 and mark_price > 0:
                    if side == "Buy":
                        # для LONG: liqPrice ниже mark, дистанция = (mark - liq) / mark
                        dist = (mark_price - liq_price) / mark_price * 100
                    else:
                        # для SHORT: liqPrice выше mark
                        dist = (liq_price - mark_price) / mark_price * 100

                    local_pos["liq_price"] = liq_price
                    local_pos["liq_distance_pct"] = round(dist, 2)

                    if dist <= self.emergency_pct:
                        log.error(f"🚨 {symbol}: ликвидация ОЧЕНЬ близко "
                                  f"({dist:.1f}% до liq @ ${liq_price:.6g})")
                        result["emergency_closes"].append(symbol)
                    elif dist <= self.critical_pct:
                        if symbol not in self.alerted_symbols:
                            log.warning(f"⚠️ {symbol}: близко к ликвидации "
                                        f"({dist:.1f}% до liq @ ${liq_price:.6g})")
                            self.alerted_symbols.add(symbol)
                        result["alerts"].append({
                            "symbol": symbol, "distance_pct": round(dist,2),
                            "liq_price": liq_price, "mark_price": mark_price,
                        })
                    else:
                        # вышли из зоны риска — снимем алерт-флаг
                        self.alerted_symbols.discard(symbol)

                # ── 2. SL/TP drift ──────────────────────────
                local_sl = local_pos.get("sl", 0)
                local_tp = local_pos.get("tp", 0)
                drift = []

                if local_sl > 0 and ex_sl == 0:
                    drift.append(f"SL отсутствует на бирже")
                elif local_sl > 0 and ex_sl > 0:
                    if abs(ex_sl - local_sl) / local_sl > 0.01:  # > 1% drift
                        drift.append(f"SL drift: local={local_sl:.6g}, exchange={ex_sl:.6g}")

                if local_tp > 0 and ex_tp == 0:
                    drift.append(f"TP отсутствует на бирже")
                elif local_tp > 0 and ex_tp > 0:
                    if abs(ex_tp - local_tp) / local_tp > 0.01:
                        drift.append(f"TP drift: local={local_tp:.6g}, exchange={ex_tp:.6g}")

                if drift:
                    msg = f"{symbol}: {'; '.join(drift)}"
                    log.warning(f"[SL/TP] {msg}")
                    result["sl_tp_drift"].append({"symbol": symbol, "issues": drift})
                    self.sl_tp_drift_log.append({
                        "time": time.time(), "symbol": symbol, "issues": drift,
                    })
                    # Trim log to 200 records
                    if len(self.sl_tp_drift_log) > 200:
                        self.sl_tp_drift_log = self.sl_tp_drift_log[-200:]

            self.last_check = time.time()
            return result
        except Exception as e:
            log.error(f"LiquidationMonitor.check_all: {e}")
            return result

    async def restore_missing_sl_tp(self, symbol: str) -> bool:
        """Восстановить SL/TP на бирже если они отсутствуют.

        Стратегия (OKX-specific):
          1. Пробуем update_sl_tp — изменит существующий algo если он есть,
             но просто с неправильными ценами.
          2. Если update вернул False — значит algo вообще нет на бирже
             (типичная ситуация после OKX 51050 на открытии). Тогда создаём
             новый standalone algo через place_algo_order.

        Returns:
            True если на бирже теперь есть валидные SL/TP, иначе False.
        """
        local_pos = self.bot.pm.positions.get(symbol)
        if not local_pos:
            return False
        try:
            sl = local_pos.get("sl", 0)
            tp = local_pos.get("tp", 0)
            if sl <= 0 or tp <= 0:
                log.warning(f"[SL/TP] {symbol}: нет валидных local SL/TP "
                            f"(sl={sl}, tp={tp}), не могу восстановить")
                return False

            side = local_pos.get("side", "Buy")
            qty  = local_pos.get("qty", 0)
            if not qty:
                log.warning(f"[SL/TP] {symbol}: нет qty позиции, не могу "
                            f"создать алго-ордер")
                return False
            # OKX берёт sz в контрактах = local_pos["qty"]
            qty_str = str(qty)

            # Попытка 1: update существующего algo (если он есть но с неверными ценами)
            ok = await self.bot.client.update_sl_tp(symbol, sl=sl, tp=tp, verify=True)
            if ok:
                log.info(f"[SL/TP] ✅ Обновлены для {symbol}: SL={sl}, TP={tp}")
                return True

            # Попытка 2: algo вообще нет → создаём новый standalone
            log.info(f"[SL/TP] {symbol}: algo не найден, создаю новый "
                     f"(SL={sl}, TP={tp}, qty={qty_str})")
            r = await self.bot.client.place_algo_order(
                symbol, position_side=side, qty_str=qty_str, sl=sl, tp=tp
            )
            if r.get("retCode") == 0:
                log.info(f"[SL/TP] ✅ Восстановлены для {symbol}: SL={sl}, TP={tp}")
                return True
            else:
                log.warning(f"[SL/TP] ❌ Не удалось восстановить {symbol}: "
                            f"{r.get('retCode')} {r.get('retMsg')}")
                return False
        except Exception as e:
            log.error(f"restore_missing_sl_tp {symbol}: {e}")
            return False
