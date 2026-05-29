"""
Бэктест-движок для проверки стратегий на исторических данных.

Использует:
  • Исторические свечи через Bybit API (или локальный CSV)
  • Индикаторы и сигнальный движок из bot_server.py
  • Имитацию SL/TP/Trailing с учётом комиссий и slippage

CLI запуск:
    python backtest.py --symbol BTCUSDT --strategy trend --days 90
    python backtest.py --symbols TOP50 --days 30 --report
"""

import argparse
import asyncio
import csv
import json
import math
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

try:
    import httpx
except ImportError:
    httpx = None
import pandas as pd
import numpy as np

# Подключаем бот-логику
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from bot_server import (
    Ind, Signal, TOP50_SYMBOLS, get_tier, TIER_MULT, MAX_PER_TIER, cfg
)
from fees_funding import FeeCalculator
from partial_tp import PartialTPConfig


# ─────────────────────────────────────────────────
# DATA LOADER
# ─────────────────────────────────────────────────
class DataLoader:
    """Скачивает исторические свечи с Bybit и кэширует их локально."""

    BYBIT_BASE = "https://api.bybit.com"

    def __init__(self, cache_dir: str = "backtest_cache"):
        self.cache = Path(cache_dir)
        self.cache.mkdir(exist_ok=True)
        if httpx is None:
            raise RuntimeError("httpx не установлен — pip install httpx")
        self._client = httpx.AsyncClient(timeout=15)

    async def close(self):
        await self._client.aclose()

    def _cache_path(self, symbol: str, interval: str, days: int) -> Path:
        return self.cache / f"{symbol}_{interval}_{days}d.csv"

    async def get_klines_paginated(self, symbol: str, interval: str = "15",
                                    days: int = 30) -> list:
        """
        Скачиваем свечи постранично (Bybit лимит 1000 свечей за запрос).
        Возвращает список candles в формате [t,o,h,l,c,v,turn].
        """
        cached = self._cache_path(symbol, interval, days)
        if cached.exists():
            df = pd.read_csv(cached)
            return df.values.tolist()

        end_ts = int(time.time() * 1000)
        if interval.isdigit():
            ms_per_candle = int(interval) * 60 * 1000
        elif interval == "D":
            ms_per_candle = 86_400_000
        else:
            ms_per_candle = 60_000

        total_needed = days * 86_400_000 // ms_per_candle
        all_klines = []
        cursor_end = end_ts

        while len(all_klines) < total_needed:
            try:
                r = await self._client.get(
                    f"{self.BYBIT_BASE}/v5/market/kline",
                    params={"category": "linear", "symbol": symbol,
                             "interval": interval, "limit": 1000,
                             "end": cursor_end}
                )
                data = r.json()
                if data.get("retCode") != 0:
                    print(f"  ERROR {symbol}: {data.get('retMsg')}")
                    break
                klines = data["result"]["list"]
                if not klines: break
                all_klines.extend(klines)
                # Старейшая свеча в этой пачке
                oldest_ts = int(klines[-1][0])
                if oldest_ts <= end_ts - days * 86_400_000:
                    break
                cursor_end = oldest_ts - 1
                await asyncio.sleep(0.15)   # rate limit
            except Exception as e:
                print(f"  EXCEPTION {symbol}: {e}")
                break

        # Bybit возвращает newest first → разворачиваем для хронологии
        all_klines.reverse()
        # Сохраним в кэш
        try:
            df = pd.DataFrame(all_klines, columns=["time","open","high","low","close","volume","turnover"])
            df.to_csv(cached, index=False)
        except Exception as e:
            print(f"  Cache write error: {e}")
        return all_klines


# ─────────────────────────────────────────────────
# BACKTEST ENGINE
# ─────────────────────────────────────────────────
class BacktestEngine:
    def __init__(self,
                 initial_balance: float = 1000,
                 risk_per_trade: float = 1.0,
                 leverage: int = 5,
                 sl_pct: float = 3.0,
                 tp_pct: float = 6.0,
                 trail_pct: float = 2.0,
                 strategy: str = "trend",
                 signal_confidence: float = 68.0,
                 max_positions: int = 10,
                 cooldown_minutes: int = 30,
                 use_fees: bool = True,
                 slippage_pct: float = 0.05,
                 partial_tp: str = "",         # "3:50,6:30,10:20" или ""
                 move_sl_to_be: bool = True,
                 trail_pct_after_all_tp: float = 1.5):
        self.balance         = initial_balance
        self.start_balance   = initial_balance
        self.risk_per_trade  = risk_per_trade
        self.leverage        = leverage
        self.sl_pct          = sl_pct
        self.tp_pct          = tp_pct
        self.trail_pct       = trail_pct
        self.strategy        = strategy
        self.signal_conf     = signal_confidence
        self.max_positions   = max_positions
        self.cooldown_min    = cooldown_minutes
        self.use_fees        = use_fees
        self.slippage_pct    = slippage_pct
        # Partial TP
        self.partial_tp_cfg = PartialTPConfig.from_env_string(
            partial_tp,
            move_sl_to_be_after_tp1=move_sl_to_be,
            trail_pct_after_all_tp=trail_pct_after_all_tp,
        )
        if not partial_tp:
            self.partial_tp_cfg.enabled = False

        # Перепишем cfg из переданных параметров
        cfg.SIGNAL_CONFIDENCE = signal_confidence
        cfg.SL_PCT  = sl_pct
        cfg.TP_PCT  = tp_pct

        self.positions:  dict = {}
        self.trades:     list = []
        self.equity:     list = [initial_balance]
        self.peak_balance     = initial_balance
        self.max_drawdown     = 0.0
        self.cooldowns:  dict = {}
        self.fees_total       = 0.0

    def _can_open(self, symbol: str, ts: int) -> bool:
        if symbol in self.positions: return False
        if len(self.positions) >= self.max_positions: return False
        if symbol in self.cooldowns and self.cooldowns[symbol] > ts:
            return False
        tier = get_tier(symbol)
        tier_count = sum(1 for p in self.positions.values() if p["tier"] == tier)
        if tier_count >= MAX_PER_TIER.get(tier, 3): return False
        return True

    def _open_position(self, symbol: str, side: str, price: float, ts: int):
        if not self._can_open(symbol, ts): return
        if price <= 0: return

        tier_mult = TIER_MULT[get_tier(symbol)]
        risk_amt  = self.balance * (self.risk_per_trade / 100) * tier_mult
        sl_dec    = self.sl_pct / 100
        if sl_dec <= 0: return
        notional  = risk_amt / sl_dec
        max_notional = self.balance * self.leverage * 0.95
        notional = min(notional, max_notional)
        qty = notional / price
        if qty <= 0: return

        entry_price = price * (1 + self.slippage_pct/100) if side == "buy" \
                      else price * (1 - self.slippage_pct/100)

        sl = entry_price * (1 - sl_dec) if side == "buy" else entry_price * (1 + sl_dec)
        tp_dec = self.tp_pct / 100
        tp = entry_price * (1 + tp_dec) if side == "buy" else entry_price * (1 - tp_dec)

        fee = FeeCalculator.entry_fee(notional) if self.use_fees else 0
        self.balance -= fee
        self.fees_total += fee

        pos = {
            "symbol": symbol, "side": side, "entry": entry_price,
            "qty": qty, "initial_qty": qty, "notional": notional,
            "sl": sl, "tp": tp, "tier": get_tier(symbol),
            "open_ts": ts, "max_favorable": entry_price,
            "partial_realized_pnl": 0.0,
            "partial_levels_triggered": [False] * len(self.partial_tp_cfg.levels),
            "partial_tp_done": False,
        }
        self.positions[symbol] = pos

    def _check_partial_tp(self, symbol: str, candle: dict, ts: int):
        """Проверяем срабатывание partial TP уровней. Возвращает True если позиция полностью закрыта."""
        if not self.partial_tp_cfg.enabled or not self.partial_tp_cfg.levels:
            return False
        pos = self.positions.get(symbol)
        if not pos or pos.get("partial_tp_done"): return False

        h, l = candle["high"], candle["low"]
        # Высокая/низкая цена этой свечи
        if pos["side"] == "buy":
            best_price = h
        else:
            best_price = l

        # PnL %
        if pos["side"] == "buy":
            pnl_pct = (best_price - pos["entry"]) / pos["entry"] * 100
        else:
            pnl_pct = (pos["entry"] - best_price) / pos["entry"] * 100

        if pnl_pct <= 0: return False

        any_triggered = False
        for idx, level in enumerate(self.partial_tp_cfg.levels):
            if pos["partial_levels_triggered"][idx]: continue
            if pnl_pct < level.pct: break

            # Уровень сработал — закрываем close_share от initial_qty
            close_qty = pos["initial_qty"] * level.close_share
            if close_qty > pos["qty"]: close_qty = pos["qty"]

            # Цена при которой сработал TP
            trig_price = pos["entry"] * (1 + level.pct/100) if pos["side"]=="buy" \
                         else pos["entry"] * (1 - level.pct/100)
            # Slippage на выходе
            if pos["side"] == "buy":
                exit_eff = trig_price * (1 - self.slippage_pct/100)
                diff = exit_eff - pos["entry"]
            else:
                exit_eff = trig_price * (1 + self.slippage_pct/100)
                diff = pos["entry"] - exit_eff

            partial_notional = close_qty * pos["entry"]
            pnl_gross = (diff / pos["entry"]) * partial_notional
            exit_fee = partial_notional * FeeCalculator.TAKER_FEE if self.use_fees else 0
            self.fees_total += exit_fee
            pnl_net = pnl_gross - exit_fee

            self.balance += pnl_net
            self.equity.append(self.balance)
            if self.balance > self.peak_balance: self.peak_balance = self.balance

            pos["partial_levels_triggered"][idx] = True
            pos["partial_realized_pnl"] += pnl_net
            pos["qty"] -= close_qty
            pos["notional"] = pos["qty"] * pos["entry"]
            any_triggered = True

            self.trades.append({
                "symbol": symbol, "side": pos["side"],
                "entry": pos["entry"], "exit": exit_eff,
                "qty": close_qty, "notional": partial_notional,
                "pnl_gross": round(pnl_gross, 4),
                "pnl_net":   round(pnl_net, 4),
                "fees":      round(exit_fee, 4),
                "reason": f"PARTIAL-TP{idx+1}",
                "open_ts": pos["open_ts"], "close_ts": ts,
                "duration_min": (ts - pos["open_ts"]) // 60000,
                "tier": pos["tier"],
            })

            # После TP1 — SL в break-even
            if idx == 0 and self.partial_tp_cfg.move_sl_to_be_after_tp1:
                pos["sl"] = pos["entry"]

            # Все уровни?
            if all(pos["partial_levels_triggered"]):
                pos["partial_tp_done"] = True
                break

        # Если все TP сработали и qty = 0 — позиция закрыта
        if pos["partial_tp_done"] and pos["qty"] <= pos["initial_qty"] * 0.01:
            self.positions.pop(symbol, None)
            return True
        return False

    def _close_position(self, symbol: str, exit_price: float,
                         reason: str, ts: int):
        pos = self.positions.pop(symbol)
        # Slippage на выходе
        if pos["side"] == "buy":
            exit_eff = exit_price * (1 - self.slippage_pct/100)
            diff = exit_eff - pos["entry"]
        else:
            exit_eff = exit_price * (1 + self.slippage_pct/100)
            diff = pos["entry"] - exit_eff

        pnl_gross = (diff / pos["entry"]) * pos["notional"]
        # Комиссия выхода
        exit_fee = FeeCalculator.entry_fee(pos["notional"]) if self.use_fees else 0
        self.fees_total += exit_fee
        pnl_net = pnl_gross - exit_fee

        self.balance += pnl_net
        self.equity.append(self.balance)
        if self.balance > self.peak_balance:
            self.peak_balance = self.balance
        dd = (self.peak_balance - self.balance) / self.peak_balance * 100
        if dd > self.max_drawdown: self.max_drawdown = dd

        if reason == "STOP-LOSS":
            self.cooldowns[symbol] = ts + self.cooldown_min * 60_000

        self.trades.append({
            "symbol": symbol, "side": pos["side"],
            "entry": pos["entry"], "exit": exit_eff,
            "qty": pos["qty"], "notional": pos["notional"],
            "pnl_gross": round(pnl_gross, 4),
            "pnl_net":   round(pnl_net, 4),
            "fees":      round(exit_fee + (pos["notional"] * FeeCalculator.TAKER_FEE if self.use_fees else 0), 4),
            "reason": reason,
            "open_ts": pos["open_ts"], "close_ts": ts,
            "duration_min": (ts - pos["open_ts"]) // 60000,
            "tier": pos["tier"],
        })

    def _check_positions(self, symbol: str, candle: dict, ts: int):
        """Проверяем SL/TP по high/low свечи."""
        if symbol not in self.positions: return
        # Сначала — partial TP (если включён)
        if self.partial_tp_cfg.enabled:
            closed = self._check_partial_tp(symbol, candle, ts)
            if closed: return
        if symbol not in self.positions: return  # вдруг partial всё закрыл
        pos = self.positions[symbol]
        h, l = candle["high"], candle["low"]

        if pos["side"] == "buy":
            # SL и TP могут сработать в одной свече — приоритет худшему случаю (SL)
            if l <= pos["sl"]:
                self._close_position(symbol, pos["sl"], "STOP-LOSS", ts); return
            # TP проверяем только если partial TP отключён
            if not self.partial_tp_cfg.enabled and h >= pos["tp"]:
                self._close_position(symbol, pos["tp"], "TAKE-PROFIT", ts); return
            # Trailing
            if h > pos["max_favorable"]:
                pos["max_favorable"] = h
                # Trail после всех partial TP
                ptp_done = pos.get("partial_tp_done", False)
                if ptp_done:
                    new_sl = h * (1 - self.partial_tp_cfg.trail_pct_after_all_tp/100)
                    if new_sl > pos["sl"]: pos["sl"] = new_sl
                elif h > pos["entry"] * 1.015:
                    new_sl = h * (1 - self.trail_pct/100)
                    if new_sl > pos["sl"]: pos["sl"] = new_sl
        else:
            if h >= pos["sl"]:
                self._close_position(symbol, pos["sl"], "STOP-LOSS", ts); return
            if not self.partial_tp_cfg.enabled and l <= pos["tp"]:
                self._close_position(symbol, pos["tp"], "TAKE-PROFIT", ts); return
            if l < pos["max_favorable"]:
                pos["max_favorable"] = l
                ptp_done = pos.get("partial_tp_done", False)
                if ptp_done:
                    new_sl = l * (1 + self.partial_tp_cfg.trail_pct_after_all_tp/100)
                    if new_sl < pos["sl"]: pos["sl"] = new_sl
                elif l < pos["entry"] * 0.985:
                    new_sl = l * (1 + self.trail_pct/100)
                    if new_sl < pos["sl"]: pos["sl"] = new_sl

    def run_on_symbol(self, symbol: str, candles_raw: list, verbose: bool = False):
        """
        Прогоняет стратегию на свечах одного символа.
        candles_raw: список [t,o,h,l,c,v,turn] (chronological order!)
        """
        if len(candles_raw) < 200:
            print(f"  [{symbol}] Недостаточно свечей ({len(candles_raw)} < 200)")
            return

        # Конвертируем в numeric DataFrame
        df = pd.DataFrame(candles_raw, columns=["time","open","high","low","close","volume","turnover"])
        for col in ["open","high","low","close","volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna().sort_values("time").reset_index(drop=True)

        for i in range(200, len(df)):
            window = df.iloc[i-199:i+1]   # 200 последних свечей
            ts = int(window.iloc[-1]["time"])
            current = {
                "open":  float(window.iloc[-1]["open"]),
                "high":  float(window.iloc[-1]["high"]),
                "low":   float(window.iloc[-1]["low"]),
                "close": float(window.iloc[-1]["close"]),
            }

            # Сначала проверяем SL/TP открытой позиции
            if symbol in self.positions:
                self._check_positions(symbol, current, ts)

            # Затем — сигнал на новый вход
            if symbol not in self.positions:
                # Готовим формат для compute_all (newest first как от Bybit)
                raw = window[["time","open","high","low","close","volume","turnover"]].values.tolist()
                raw_reversed = list(reversed(raw))
                ind = Ind.compute_all(raw_reversed)
                if ind:
                    sig = Signal.analyze(ind, self.strategy)
                    if sig["signal"] == "buy":
                        self._open_position(symbol, "buy", current["close"], ts)
                    elif sig["signal"] == "sell":
                        self._open_position(symbol, "sell", current["close"], ts)

        # Закрываем оставшиеся позиции по последней цене
        if symbol in self.positions:
            last_price = float(df.iloc[-1]["close"])
            self._close_position(symbol, last_price, "END-OF-TEST", int(df.iloc[-1]["time"]))

    # ── Метрики ──────────────────────────────
    def metrics(self) -> dict:
        if not self.trades:
            return {"trades": 0, "no_data": True}
        wins   = [t for t in self.trades if t["pnl_net"] > 0]
        losses = [t for t in self.trades if t["pnl_net"] <= 0]
        total_win  = sum(t["pnl_net"] for t in wins)
        total_loss = abs(sum(t["pnl_net"] for t in losses))
        pnl_total  = sum(t["pnl_net"] for t in self.trades)
        roi        = (self.balance - self.start_balance) / self.start_balance * 100
        wr         = len(wins) / len(self.trades) * 100 if self.trades else 0
        pf         = total_win / total_loss if total_loss > 0 else float("inf")
        avg_win    = total_win / len(wins) if wins else 0
        avg_loss   = -total_loss / len(losses) if losses else 0
        avg_dur    = sum(t["duration_min"] for t in self.trades) / len(self.trades)
        # Sharpe (упрощённый daily Sharpe)
        equity_changes = []
        for i in range(1, len(self.equity)):
            equity_changes.append((self.equity[i] - self.equity[i-1]) / self.equity[i-1])
        if equity_changes and np.std(equity_changes) > 0:
            sharpe = (np.mean(equity_changes) / np.std(equity_changes)) * math.sqrt(252)
        else:
            sharpe = 0
        # Sortino
        neg = [r for r in equity_changes if r < 0]
        sortino = (np.mean(equity_changes) / np.std(neg) * math.sqrt(252)) if neg and np.std(neg) > 0 else 0

        return {
            "initial_balance": self.start_balance,
            "final_balance":   round(self.balance, 2),
            "roi_pct":         round(roi, 2),
            "trades_total":    len(self.trades),
            "wins":            len(wins),
            "losses":          len(losses),
            "win_rate_pct":    round(wr, 2),
            "profit_factor":   round(pf, 2) if pf != float("inf") else "inf",
            "avg_win":         round(avg_win, 2),
            "avg_loss":        round(avg_loss, 2),
            "max_drawdown_pct":round(self.max_drawdown, 2),
            "avg_duration_min":round(avg_dur, 1),
            "sharpe":          round(sharpe, 2),
            "sortino":         round(sortino, 2),
            "fees_paid":       round(self.fees_total, 2),
            "best_trade":      round(max((t["pnl_net"] for t in self.trades), default=0), 2),
            "worst_trade":     round(min((t["pnl_net"] for t in self.trades), default=0), 2),
        }

    def export_trades_csv(self, path: str):
        if not self.trades: return
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=self.trades[0].keys())
            w.writeheader()
            w.writerows(self.trades)


# ─────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────
def print_report(metrics: dict, label: str = ""):
    print(f"\n{'='*60}")
    if label: print(f"  {label}")
    print(f"{'='*60}")
    if metrics.get("no_data"):
        print("  Нет сделок"); return
    print(f"  Стартовый баланс:   ${metrics['initial_balance']:.2f}")
    print(f"  Финальный баланс:   ${metrics['final_balance']:.2f}")
    print(f"  ROI:                {metrics['roi_pct']:+.2f}%")
    print(f"  Сделок:             {metrics['trades_total']}")
    print(f"  Win rate:           {metrics['win_rate_pct']:.1f}%  "
          f"({metrics['wins']}W / {metrics['losses']}L)")
    print(f"  Profit Factor:      {metrics['profit_factor']}")
    print(f"  Avg Win / Loss:     ${metrics['avg_win']:.2f} / ${metrics['avg_loss']:.2f}")
    print(f"  Best / Worst:       ${metrics['best_trade']:.2f} / ${metrics['worst_trade']:.2f}")
    print(f"  Max Drawdown:       {metrics['max_drawdown_pct']:.2f}%")
    print(f"  Sharpe / Sortino:   {metrics['sharpe']} / {metrics['sortino']}")
    print(f"  Avg duration:       {metrics['avg_duration_min']:.0f} мин")
    print(f"  Комиссии:           ${metrics['fees_paid']:.2f}")


async def run_cli():
    parser = argparse.ArgumentParser(description="APEX Backtest Engine")
    parser.add_argument("--symbol", default=None, help="Один символ (напр. BTCUSDT)")
    parser.add_argument("--symbols", default=None, help="TOP50 или список через запятую")
    parser.add_argument("--days", type=int, default=30, help="Глубина истории, дни")
    parser.add_argument("--interval", default="15", help="Таймфрейм (1, 5, 15, 60, 240, D)")
    parser.add_argument("--strategy", default="trend",
                        choices=["trend","scalp","breakout","dca","mean_reversion"])
    parser.add_argument("--balance", type=float, default=1000)
    parser.add_argument("--risk", type=float, default=1.0)
    parser.add_argument("--leverage", type=int, default=5)
    parser.add_argument("--sl", type=float, default=3.0)
    parser.add_argument("--tp", type=float, default=6.0)
    parser.add_argument("--confidence", type=float, default=68.0)
    parser.add_argument("--no-fees", action="store_true", help="Отключить комиссии (для сравнения)")
    parser.add_argument("--partial-tp", default="",
                        help="Уровни partial TP: '3:50,6:30,10:20'. Пусто = одиночный TP")
    parser.add_argument("--export", default=None, help="Сохранить trades в CSV")
    args = parser.parse_args()

    if args.symbols == "TOP50":
        symbols = TOP50_SYMBOLS
    elif args.symbols:
        symbols = args.symbols.split(",")
    elif args.symbol:
        symbols = [args.symbol]
    else:
        symbols = ["BTCUSDT"]

    print(f"\n🔬 BACKTEST: {len(symbols)} символ(ов), {args.days} дней, "
          f"стратегия={args.strategy}, TF={args.interval}m")

    loader = DataLoader()
    engine = BacktestEngine(
        initial_balance=args.balance,
        risk_per_trade=args.risk,
        leverage=args.leverage,
        sl_pct=args.sl,
        tp_pct=args.tp,
        signal_confidence=args.confidence,
        strategy=args.strategy,
        use_fees=not args.no_fees,
        partial_tp=args.partial_tp,
    )

    for i, sym in enumerate(symbols):
        print(f"  [{i+1}/{len(symbols)}] Загрузка {sym}...")
        candles = await loader.get_klines_paginated(sym, args.interval, args.days)
        if not candles:
            print(f"    Нет данных для {sym}"); continue
        print(f"    {len(candles)} свечей, прогон стратегии...")
        engine.run_on_symbol(sym, candles)

    await loader.close()

    metrics = engine.metrics()
    print_report(metrics, f"СТРАТЕГИЯ: {args.strategy.upper()}, "
                          f"{args.days}d, {len(symbols)} пар")

    # По тирам
    by_tier = defaultdict(list)
    for t in engine.trades:
        by_tier[t["tier"]].append(t["pnl_net"])
    if by_tier:
        print(f"\n  PnL по тирам:")
        for tier in ("tier1","tier2","tier3","tier4"):
            if tier in by_tier:
                pnls = by_tier[tier]
                print(f"    {tier}: {len(pnls)} сделок, "
                      f"PnL ${sum(pnls):+.2f}, "
                      f"средн ${sum(pnls)/len(pnls):+.2f}")

    if args.export:
        engine.export_trades_csv(args.export)
        print(f"\n  Сделки сохранены в {args.export}")


if __name__ == "__main__":
    asyncio.run(run_cli())
