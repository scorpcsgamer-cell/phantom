"""
Reconciliation модуль: синхронизирует локальные позиции бота
с реальным состоянием на бирже.

Запускается:
  • При старте бота (восстановление после перезапуска)
  • Периодически (каждые 5 минут) для контроля рассинхрона
  • После каждого размещённого ордера

Что проверяется:
  1. Если позиция есть локально, но НЕТ на бирже → удалить локальную
     (вероятно, SL/TP сработали пока бот был оффлайн)
  2. Если позиция есть на бирже, но НЕТ локально → добавить локально
     (восстановление состояния после краха)
  3. Если qty/SL/TP отличаются → обновить локальные данные с биржи
"""

import logging
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from bot_server import TradingBot

log = logging.getLogger("PHANTOM.recon")


class Reconciler:
    """Сверяет локальное состояние позиций с биржей."""

    def __init__(self, bot: "TradingBot"):
        self.bot = bot
        self.last_run: float = 0
        self.last_drift: list = []   # последние выявленные расхождения

    async def reconcile(self, log_silent: bool = False) -> dict:
        """
        Запустить сверку. Возвращает {orphans, missing, fixed}.

        orphans — локальные позиции, которых нет на бирже
        missing — биржевые позиции, которых нет локально
        fixed   — успешно исправленные расхождения
        """
        result = {"orphans": [], "missing": [], "fixed": []}
        try:
            # Получаем реальные позиции с биржи
            exchange_positions = await self.bot.client.get_positions()
            exchange_map = {p["symbol"]: p for p in exchange_positions}
            local_map    = dict(self.bot.pm.positions)

            # 1. Orphans: локальные без биржевого аналога
            #    Накапливаем в список, лог одной строкой в конце —
            #    чтобы при восстановлении после рестарта не пугать пользователя
            #    стеной из 9 ворнингов.
            for symbol, local_pos in local_map.items():
                if symbol not in exchange_map:
                    result["orphans"].append(symbol)
                    self.bot.pm.positions.pop(symbol, None)
                    result["fixed"].append(f"removed_local:{symbol}")
            if result["orphans"] and not log_silent:
                short = ", ".join(s.replace("USDT","") for s in result["orphans"][:6])
                more  = f" + ещё {len(result['orphans'])-6}" if len(result["orphans"]) > 6 else ""
                log.info(f"[RECON] Закрытых на бирже: {len(result['orphans'])} ({short}{more}). "
                         f"Локальная копия очищена.")

            # 2. Missing: биржевые без локального аналога
            for symbol, ex_pos in exchange_map.items():
                if symbol not in local_map:
                    result["missing"].append(symbol)
                    if not log_silent:
                        log.warning(f"[RECON] Missing: {symbol} на бирже, но не локально. Восстанавливаю.")
                    self._restore_position_from_exchange(symbol, ex_pos)
                    result["fixed"].append(f"restored_local:{symbol}")

            # 3. Drift в qty / SL / TP
            # ВАЖНО (фикс бага SL drift):
            #   qty: биржа — источник истины (если позиция частично закрылась,
            #        мы должны это увидеть и обновить локально).
            #   SL/TP: НЕ перезаписываем local значениями биржи! Раньше код
            #        делал local_pos["sl"] = ex_sl, что затирало любую попытку
            #        VolumeDropGuard или trailing-логики "подтянуть" SL.
            #        Теперь: drift только логируется. Если local изменился,
            #        обязанность отправить amend лежит на том коде, который
            #        меняет local_pos["sl"] (через client.update_sl_tp()).
            for symbol, local_pos in self.bot.pm.positions.items():
                ex_pos = exchange_map.get(symbol)
                if not ex_pos: continue
                ex_qty = float(ex_pos.get("size", 0) or 0)
                ex_sl  = float(ex_pos.get("stopLoss") or 0)
                ex_tp  = float(ex_pos.get("takeProfit") or 0)

                if abs(ex_qty - local_pos["qty"]) / max(local_pos["qty"], 1e-9) > 0.01:
                    if not log_silent:
                        log.warning(f"[RECON] {symbol} qty drift: local={local_pos['qty']}, exchange={ex_qty}")
                    local_pos["qty"] = ex_qty
                    result["fixed"].append(f"sync_qty:{symbol}")

                # SL/TP drift — только лог, без перезаписи.
                # Auto-fix через amend делается отдельно в auto_fix_sltp_drift()
                if ex_sl > 0 and abs(ex_sl - local_pos["sl"]) / max(local_pos["sl"], 1e-9) > 0.005:
                    if not log_silent:
                        log.warning(f"[RECON] {symbol} SL drift: local={local_pos['sl']}, exchange={ex_sl} "
                                    f"(не перезаписываю — local источник истины для SL)")
                    result["fixed"].append(f"drift_sl:{symbol}")
                if ex_tp > 0 and abs(ex_tp - local_pos["tp"]) / max(local_pos["tp"], 1e-9) > 0.005:
                    if not log_silent:
                        log.warning(f"[RECON] {symbol} TP drift: local={local_pos['tp']}, exchange={ex_tp}")
                    result["fixed"].append(f"drift_tp:{symbol}")

            self.last_run = time.time()
            self.last_drift = result["fixed"]
            return result

        except Exception as e:
            log.error(f"Reconcile error: {e}")
            return result

    def _restore_position_from_exchange(self, symbol: str, ex_pos: dict):
        """Восстановить локальную позицию из биржевых данных."""
        try:
            side  = ex_pos.get("side", "Buy")     # Bybit: "Buy" or "Sell"
            qty   = float(ex_pos.get("size", 0) or 0)
            entry = float(ex_pos.get("avgPrice", 0) or ex_pos.get("entryPrice", 0) or 0)
            lev   = int(float(ex_pos.get("leverage", 1) or 1))
            sl    = float(ex_pos.get("stopLoss") or 0)
            tp    = float(ex_pos.get("takeProfit") or 0)
            mark  = float(ex_pos.get("markPrice", entry) or entry)

            # Если SL/TP не выставлены — используем дефолты из конфига бота
            sl_pct_default = getattr(self.bot, "cfg_sl_pct", None)
            tp_pct_default = getattr(self.bot, "cfg_tp_pct", None)
            if sl_pct_default is None or tp_pct_default is None:
                # Fallback: пробуем достать через cfg в bot_server
                try:
                    from bot_server import cfg
                    sl_pct_default = cfg.SL_PCT
                    tp_pct_default = cfg.TP_PCT
                except Exception:
                    sl_pct_default = 3.0
                    tp_pct_default = 6.0
            if sl <= 0:
                sl = entry * (1 - sl_pct_default/100) if side == "Buy" else entry * (1 + sl_pct_default/100)
            if tp <= 0:
                tp = entry * (1 + tp_pct_default/100) if side == "Buy" else entry * (1 - tp_pct_default/100)

            notional = qty * entry
            self.bot.pm.add(
                symbol=symbol, side=side, entry=entry, qty=qty,
                size_usdt=notional, sl=sl, tp=tp,
                lev=lev, strat="restored", oid=ex_pos.get("createdTime", "?"),
            )
            log.info(f"[RECON] Восстановлена позиция: {side} {symbol} @ ${entry:.6g}, "
                     f"qty={qty}, SL={sl}, TP={tp}")
        except Exception as e:
            log.error(f"Не удалось восстановить позицию {symbol}: {e}")

    async def auto_fix_sltp_drift(self) -> dict:
        """
        Идёт по всем открытым позициям и для каждой, где local SL/TP отличается
        от exchange — отправляет amend на биржу, чтобы биржа догнала local.

        Это правильное направление синхронизации: local — намерение, biржа — факт.
        Используется после tighten_sl/trailing/multi-tp изменений в local_pos.

        Возвращает: {amended: [symbol], failed: [symbol]}.
        """
        out = {"amended": [], "failed": []}
        try:
            ex_positions = await self.bot.client.get_positions()
            ex_map = {p["symbol"]: p for p in ex_positions}
            for symbol, local_pos in list(self.bot.pm.positions.items()):
                ex_pos = ex_map.get(symbol)
                if not ex_pos: continue
                local_sl = float(local_pos.get("sl", 0) or 0)
                local_tp = float(local_pos.get("tp", 0) or 0)
                ex_sl    = float(ex_pos.get("stopLoss") or 0)
                ex_tp    = float(ex_pos.get("takeProfit") or 0)

                need_sl = local_sl > 0 and (ex_sl <= 0 or
                          abs(ex_sl - local_sl) / max(local_sl, 1e-9) > 0.005)
                need_tp = local_tp > 0 and (ex_tp <= 0 or
                          abs(ex_tp - local_tp) / max(local_tp, 1e-9) > 0.005)
                if not (need_sl or need_tp):
                    continue

                ok = await self.bot.client.update_sl_tp(
                    symbol=symbol,
                    sl=local_sl if need_sl else None,
                    tp=local_tp if need_tp else None,
                    verify=True,
                )
                if ok:
                    out["amended"].append(symbol)
                else:
                    out["failed"].append(symbol)
            return out
        except Exception as e:
            log.error(f"auto_fix_sltp_drift error: {e}")
            return out
