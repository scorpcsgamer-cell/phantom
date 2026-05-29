# PHANTOM Phase 1 — Fib + Divergence Strategy

## Что включено

Новая стратегия входа: **multi-TF тренд (4h+1h) + Fib retracement + RSI/MACD дивергенция**. Старая логика «топ движущихся» сохранена как fallback, переключается одним флагом.

## Файлы

Положи рядом с `bot_server.py`:
- `trend_filter.py` — Multi-TF тренд-фильтр
- `fib_engine.py` — Детекция свинга + Fib уровни
- `divergence.py` — RSI/MACD классическая дивергенция
- `volatility.py` — Адаптивный таймфрейм
- `meme_strategy.py` — Отдельная логика для PEPE/FLOKI/...
- `bot_server.py` — обновлённый (интеграция)

Также убедись что у тебя последние версии из предыдущих фикcов:
- `exchange_client.py` (с `update_sl_tp`)
- `reconciler.py`, `order_health.py`, `volume_anomaly.py`

## Как включить новую стратегию

В `.env` добавь:

```bash
# ── Phase 1: Fib + Divergence strategy ────────────────────────────
USE_FIB_STRATEGY=true          # ГЛАВНЫЙ ПЕРЕКЛЮЧАТЕЛЬ

# Trend filter (если жёстко — мало сигналов 1-3/день, как договаривались)
TREND_SLOPE_THRESHOLD=0.001    # минимальный наклон EMA за бар
TREND_CACHE_TTL_S=300          # кеш на 5 мин

# Fib engine
FIB_TOLERANCE_PCT=0.3          # допуск "цена на уровне" 0.3%
FIB_SWING_ATR_MULT=1.5         # минимум амплитуды свинга в ATR

# Divergence
DIV_LOOKBACK=30                # окно поиска экстремумов

# Adaptive TF
USE_ADAPTIVE_TF=true           # бот выбирает 1m/5m/15m/1h по волатильности
FALLBACK_TF=15m                # если USE_ADAPTIVE_TF=false

# Meme strategy (для PEPE, FLOKI, BONK, SHIB, DOGE, WIF, BOME, MEW, POPCAT)
MEME_STRATEGY_ENABLED=true
MEME_RISK_PCT=0.75             # риск 0.75% вместо стандартных 1%
MEME_SL_PCT=1.0
MEME_TP_PCT=2.0
MEME_MAX_CONCURRENT=1          # max 1 мем в портфеле

# Корреляционная защита (то что согласовали как "агрессивно но безопасно")
CORR_MAX_SAME_DIRECTION=2      # max 2 позиции в одном направлении (long или short)
CORR_MAX_PORTFOLIO_RISK=4.5    # суммарный риск всех открытых ≤ 4.5%

# Circuit breaker (3 SL подряд → пауза 4 часа)
SL_STREAK_THRESHOLD=3
SL_STREAK_PAUSE_HOURS=4
```

Чтобы **откатиться к старой стратегии** в любой момент — поставь `USE_FIB_STRATEGY=false` и перезапусти.

## Что появится в логе

При новой стратегии ты увидишь **подробные SIGNAL-строки** перед каждым ордером — то что просил «лог сигнала с причинами»:

```
[SIGNAL] BTCUSDT Buy: trend(4h=up slope=0.0023, 1h=up slope=0.0018) | fib(0.618 of swing 95→110) | div(rsi=bullish, macd=bullish) | TF=15m regime=normal ATR%=1.20
```

Расшифровка:
- `trend(4h=up...)` — оба таймфрейма согласны на up
- `fib(0.618 of swing 95→110)` — цена откатилась к 0.618 от свинга 95→110
- `div(rsi=bullish, macd=bullish)` — обе дивергенции совпали (И-логика)
- `TF=15m regime=normal` — адаптивный TF выбрал 15-минутный

Для мемов формат другой:
```
[SIGNAL] PEPEUSDT MEME Buy: RSI=22.5 vol_ratio=4.3x close=0.0000037 open=0.0000035
```

При блокировке корреляционной защитой:
```
[CORR] ADAUSDT Buy заблокирован: Лимит long: уже 2, max 2
[CORR] FLOKIUSDT Buy заблокирован: Лимит мемов: уже 1, max 1
[CORR] DOTUSDT Buy заблокирован: Лимит portfolio risk: уже 3.5%, +1.5% превысит 4.5%
```

Circuit breaker (3 SL подряд):
```
🛑 Circuit breaker: 3 SL подряд → пауза 4.0ч
[CORR] BTCUSDT Buy заблокирован: Circuit breaker: ещё 234m
```

## Чего ожидать в первые часы

- **Мало сигналов**. Жёсткий фильтр — это норма 1-3 сделки в день. Если сидишь час и нет сигнала — это правильное поведение, не баг.
- **Warm-up 3 цикла** (~5 мин). Первые 5 минут бот молчит.
- **Trend cache TTL 5 мин**: первый цикл будет медленный (66 запросов klines), дальше быстро.

## Что наблюдать в логе

**Должно появиться:**
- `[Phase1] Fib+Div strategy ENABLED. trend slope thr=0.001, fib tol=0.3%, div lookback=30` — на старте
- `[SIGNAL]` строки перед каждым ордером
- При срабатывании защит — `[CORR]` или 🛑 Circuit breaker

**НЕ должно быть:**
- AttributeError про `update_sl_tp` (если есть — значит файлы exchange_client.py / bot_server.py не обновлены)
- Открытие позиций без `[SIGNAL]` строки

## Валидация на TESTNET (как договаривались)

1. Запусти бот с `USE_FIB_STRATEGY=true` на 3-5 дней
2. Цели для проверки:
   - Каждое открытие должно сопровождаться `[SIGNAL]` строкой
   - Должны срабатывать защиты: при попытке открыть 3-й long в одном направлении → `[CORR] Лимит long`
   - SL drift не должен возникать (это уже починили в фазе 0)
3. Если за 5 дней нет сигналов вообще — фильтр слишком жёсткий, ослабь `TREND_SLOPE_THRESHOLD` до `0.0005`

## Что НЕ изменилось (важно)

Telegram, partial_tp, liquidation_monitor, networksafety, recon — всё работает как было. Я их не трогал.

## Если что-то сломалось

1. `USE_FIB_STRATEGY=false` в `.env` → перезапуск → откат на старую логику
2. Пришли мне лог последних 30 минут — разберём
