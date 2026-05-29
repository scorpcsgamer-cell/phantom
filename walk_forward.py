"""
Walk-Forward Validation: проверка робастности стратегии.

Стандартная процедура:
  1. Берём N дней истории (например, 90)
  2. Делим на окна train (e.g. 30 дней) и test (15 дней)
  3. Двигаем окно вперёд с шагом step (например, 15 дней)
  4. На каждом шаге:
     a) Прогон стратегии на train
     b) Прогон той же стратегии на test
     c) Сравнение метрик
  5. Если метрики на test существенно хуже train — стратегия overfit-ed

CLI:
    python walk_forward.py --symbol BTCUSDT --days 180 --train 30 --test 15 --step 15
"""

import argparse
import asyncio
import os
import sys
import statistics
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from backtest import DataLoader, BacktestEngine, print_report
    from bot_server import TOP50_SYMBOLS
    HAS_DEPS = True
except ImportError as e:
    HAS_DEPS = False
    _IMPORT_ERROR = str(e)


def split_candles_chronologically(candles: list, train_days: int,
                                    test_days: int, step_days: int,
                                    interval_minutes: int = 15) -> list:
    """
    Разбивает свечи на список (train_chunk, test_chunk) кортежей,
    двигая окно вперёд с шагом step_days.
    """
    if not candles: return []
    candles_per_day = (24 * 60) // max(interval_minutes, 1)
    train_size = train_days * candles_per_day
    test_size  = test_days  * candles_per_day
    step_size  = step_days  * candles_per_day

    windows = []
    start = 0
    while start + train_size + test_size <= len(candles):
        train = candles[start:start + train_size]
        test  = candles[start + train_size:start + train_size + test_size]
        windows.append((train, test))
        start += step_size
    return windows


def run_backtest_on_chunk(chunk: list, **engine_kwargs) -> dict:
    engine = BacktestEngine(**engine_kwargs)
    engine.run_on_symbol(engine_kwargs.get("_symbol", "TEST"), chunk)
    return engine.metrics()


async def walk_forward_run(symbol: str, days: int, train_days: int,
                            test_days: int, step_days: int,
                            interval: str, strategy: str,
                            **engine_kwargs) -> dict:
    if not HAS_DEPS:
        return {"error": f"Dependencies missing: {_IMPORT_ERROR}"}

    loader = DataLoader()
    print(f"\n📥 Загрузка {symbol} за {days} дней...")
    candles = await loader.get_klines_paginated(symbol, interval, days)
    await loader.close()

    if not candles:
        return {"error": "No data"}

    print(f"   Получено {len(candles)} свечей")

    interval_minutes = int(interval) if interval.isdigit() else 60
    windows = split_candles_chronologically(candles, train_days, test_days,
                                              step_days, interval_minutes)
    if not windows:
        return {"error": "Недостаточно данных для walk-forward"}

    print(f"\n🔬 Walk-Forward: {len(windows)} окон")
    print(f"   train={train_days}d, test={test_days}d, step={step_days}d\n")

    train_results = []
    test_results  = []

    for i, (train_chunk, test_chunk) in enumerate(windows):
        train_metrics = run_backtest_on_chunk(
            train_chunk, _symbol=symbol, strategy=strategy, **engine_kwargs
        )
        test_metrics = run_backtest_on_chunk(
            test_chunk, _symbol=symbol, strategy=strategy, **engine_kwargs
        )

        train_roi = train_metrics.get("roi_pct", 0)
        test_roi  = test_metrics.get("roi_pct", 0)
        train_wr  = train_metrics.get("win_rate_pct", 0)
        test_wr   = test_metrics.get("win_rate_pct", 0)
        train_pf  = train_metrics.get("profit_factor", 0)
        test_pf   = test_metrics.get("profit_factor", 0)

        train_results.append(train_metrics)
        test_results.append(test_metrics)

        print(f"   Окно {i+1}: train ROI={train_roi:+.2f}% WR={train_wr:.1f}%  "
              f"|  test ROI={test_roi:+.2f}% WR={test_wr:.1f}%")

    # Aggregate
    avg_train_roi = statistics.mean(r.get("roi_pct", 0) for r in train_results)
    avg_test_roi  = statistics.mean(r.get("roi_pct", 0) for r in test_results)
    avg_train_wr  = statistics.mean(r.get("win_rate_pct", 0) for r in train_results)
    avg_test_wr   = statistics.mean(r.get("win_rate_pct", 0) for r in test_results)

    test_rois = [r.get("roi_pct", 0) for r in test_results]
    test_roi_std = statistics.stdev(test_rois) if len(test_rois) >= 2 else 0
    test_positive = sum(1 for r in test_rois if r > 0)

    # Robustness verdict
    overfitting = avg_train_roi - avg_test_roi
    if overfitting > 20:
        verdict = "🚨 ВЫСОКИЙ OVERFITTING — стратегия не работает на новых данных"
    elif overfitting > 10:
        verdict = "⚠️ Средний overfitting — стратегия частично переобучена"
    elif test_positive < len(test_rois) // 2:
        verdict = "⚠️ Меньше половины test-окон прибыльные"
    elif avg_test_roi > 0 and test_roi_std < 15:
        verdict = "✅ Стратегия робастная — стабильная на out-of-sample"
    else:
        verdict = "🤔 Смешанные результаты, нужны дополнительные тесты"

    summary = {
        "symbol":          symbol,
        "windows":         len(windows),
        "avg_train_roi":   round(avg_train_roi, 2),
        "avg_test_roi":    round(avg_test_roi, 2),
        "avg_train_wr":    round(avg_train_wr, 2),
        "avg_test_wr":     round(avg_test_wr, 2),
        "test_roi_std":    round(test_roi_std, 2),
        "test_positive":   f"{test_positive}/{len(test_rois)}",
        "overfitting_gap": round(overfitting, 2),
        "verdict":         verdict,
    }
    return summary


def print_walk_forward_report(s: dict):
    if "error" in s:
        print(f"\n❌ Ошибка: {s['error']}")
        return
    print("\n" + "=" * 60)
    print(f"  WALK-FORWARD RESULT: {s['symbol']}")
    print("=" * 60)
    print(f"  Окон:                    {s['windows']}")
    print(f"  Средний ROI на train:    {s['avg_train_roi']:+.2f}%")
    print(f"  Средний ROI на test:     {s['avg_test_roi']:+.2f}%")
    print(f"  Overfitting gap:         {s['overfitting_gap']:+.2f}%")
    print(f"  Win Rate train/test:     {s['avg_train_wr']:.1f}% / {s['avg_test_wr']:.1f}%")
    print(f"  Test ROI σ (std dev):    {s['test_roi_std']:.2f}%")
    print(f"  Прибыльные test-окна:    {s['test_positive']}")
    print(f"\n  Вердикт:  {s['verdict']}")
    print("=" * 60)


async def cli():
    parser = argparse.ArgumentParser(description="Walk-Forward Validation")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--days",     type=int, default=180)
    parser.add_argument("--train",    type=int, default=30)
    parser.add_argument("--test",     type=int, default=15)
    parser.add_argument("--step",     type=int, default=15)
    parser.add_argument("--interval", default="15")
    parser.add_argument("--strategy", default="trend",
                        choices=["trend","scalp","breakout","dca","mean_reversion"])
    parser.add_argument("--balance",  type=float, default=1000)
    parser.add_argument("--risk",     type=float, default=1.0)
    parser.add_argument("--leverage", type=int, default=5)
    parser.add_argument("--sl",       type=float, default=3.0)
    parser.add_argument("--tp",       type=float, default=6.0)
    parser.add_argument("--confidence", type=float, default=68.0)
    args = parser.parse_args()

    if not HAS_DEPS:
        print(f"❌ Не хватает зависимостей: {_IMPORT_ERROR}")
        print("   Установите: pip install -r requirements.txt")
        return

    summary = await walk_forward_run(
        symbol=args.symbol, days=args.days,
        train_days=args.train, test_days=args.test, step_days=args.step,
        interval=args.interval, strategy=args.strategy,
        initial_balance=args.balance, risk_per_trade=args.risk,
        leverage=args.leverage, sl_pct=args.sl, tp_pct=args.tp,
        signal_confidence=args.confidence,
    )
    print_walk_forward_report(summary)


if __name__ == "__main__":
    asyncio.run(cli())
