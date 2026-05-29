# PHANTOM bot — Phase 0 changelog (исправление критических багов)

Дата: 19.05.2026
Затронуто 4 файла: `exchange_client.py`, `reconciler.py`, `volume_anomaly.py`, `order_health.py`.
Telegram не трогался по твоей просьбе.

---

## 1. `exchange_client.py` — добавлены два метода (САМОЕ ВАЖНОЕ)

### `amend_algo_order(symbol, algo_id, new_sl=None, new_tp=None)`
Дёргает OKX endpoint `POST /api/v5/trade/amend-algos`. Это **единственный валидный способ изменить SL/TP на бирже без переоткрытия позиции**. Раньше его в коде не было.

### `update_sl_tp(symbol, sl=None, tp=None, verify=True)`
Высокоуровневая обёртка: сама находит algoId через `get_algo_orders`, шлёт amend, и через 600 мс делает **verify-callback** — повторно читает algo-ордер с биржи и проверяет, что новое значение прижилось (допуск 0.5% на округление до tick_size).

### Что менять в `bot_server.py`
Найди все места, где идёт **прямая запись** в `local_pos["sl"]` или `local_pos["tp"]` (например, в реакции на `VolumeDropGuard.evaluate() == "tighten_sl"`, в trailing-логике, в partial_tp).

**Было** (плохо — биржа не знает):
```python
local_pos["sl"] = new_sl
```

**Должно стать**:
```python
ok = await self.client.update_sl_tp(symbol, sl=new_sl, verify=True)
if ok:
    local_pos["sl"] = new_sl
else:
    log.warning(f"{symbol}: не удалось обновить SL на бирже, local оставлен прежним")
```

Это закрывает корень бага «SL drift».

---

## 2. `reconciler.py` — две правки

### a) В `reconcile()` убран код, который перезаписывал local SL/TP значениями биржи
Раньше: если `local_pos["sl"] != ex_sl`, делал `local_pos["sl"] = ex_sl`. Это **затирало** любую попытку tighten_sl/trailing, потому что эти попытки писали в local, но не доходили до биржи (см. пункт 1).

Теперь: drift только **логируется** как warning. `local_pos` — источник истины для SL/TP (это намерение бота). `qty` синхронизируется как раньше, потому что там биржа действительно источник истины (позиция могла частично закрыться).

### b) Новый метод `auto_fix_sltp_drift()`
Идёт по всем позициям и для каждой, где local SL/TP отличается от biржи, отправляет `update_sl_tp` (правильное направление: local → exchange).

### Что менять в `bot_server.py`
В главном loop, **после** тика VolumeDropGuard / trailing / partial_tp, добавь:
```python
if iteration % 5 == 0:  # каждые ~5 минут
    await self.reconciler.auto_fix_sltp_drift()
```
Это страховка на случай если update_sl_tp не дошёл с первого раза (сеть моргнула, OKX был занят).

---

## 3. `volume_anomaly.py` — два слоя защиты от outliers

В лог попадало `drop 12328.0x`, `7169.9x`, `6168.2x` — это явно ошибка данных, а не реальное падение объёма.

### Слой 1: на вход `update_volume()`
Если новое значение в **>100× больше медианы** уже накопленных баров — отбрасываем и пишем warning с указанием где искать причину.

### Слой 2: на выходе `evaluate()`
Если ratio `avg_4h / avg_1h > max_sane_ratio` (по умолчанию 50.0) — не возвращаем action, пишем error.

### Что менять в `bot_server.py`
**Найди где вызывается `volume_drop_guard.update_volume(symbol, ???)`** — это и есть корень бага. Скорее всего туда подаётся:
- Поле `volume24h` или `turnover24h` из `get_tickers_all()` (это **24-часовой кумулятивный объём** — нельзя)
- Вместо: индекс **`5`** или **`6`** из массива `get_klines(symbol, "5", limit=1)` (per-bar volume)

Правильная подача:
```python
klines = await self.client.get_klines(symbol, "5", limit=1)
if klines:
    bar_volume = float(klines[0][5])   # vol в контрактах (per-bar)
    volume_drop_guard.update_volume(symbol, bar_volume)
```

Запусти бот после фикса — если warning-сообщения «отброшен outlier» появляются, значит баг подачи **ещё не исправлен**, но защита держит. Если их нет — корень исправлен.

---

## 4. `order_health.py` — две правки

### a) `CRITICAL_ERROR_CODES`: заменены Bybit-коды на OKX-коды
Раньше там были `110007`, `110045`, `30086` — это **Bybit**. Адаптер `place_order` пробрасывает OKX inner `sCode` в `retCode`, поэтому Bybit-коды никогда не совпадали и троттлинг был мёртв.

Теперь коды OKX: `51008`, `51009`, `51131`, `51020`, `50006`, `50011` и т.д. Источник: https://www.okx.com/docs-v5/en/#error-code-rest. Если в логах увидишь критичные коды, которых нет в словаре — допиши.

### b) `detect_partial_fill()`: заглушка → реальная проверка
Раньше всегда возвращала `None`. Теперь пытается достать `accFillSz`/`fillSz` из OKX raw response (если есть). Полная надёжность всё равно через reconcile, но базовая детекция работает.

### Что менять в `bot_server.py`
Ничего, если `OrderHealthMonitor` уже создаётся и `record_order_response` вызывается после каждого ордера. Если не вызывается — добавь в обёртку place_order:
```python
order_health.record_order_response(response, symbol=symbol)
if order_health.is_paused:
    log.warning(f"Order Health: бот на паузе, пропускаем сигналы")
    continue
```

---

## Чек-лист перед запуском на TESTNET

- [ ] Заменить 4 файла на исправленные версии
- [ ] В `bot_server.py` найти все прямые записи `local_pos["sl"] = ...` → заменить на `client.update_sl_tp(...)`
- [ ] В `bot_server.py` найти вызов `volume_drop_guard.update_volume(...)` → убедиться что подаётся per-bar volume из klines (индекс 5), а не 24h ticker volume
- [ ] Запустить бот, открыть позицию, дождаться срабатывания VolumeDropGuard → в логах должно быть `[AMEND] ✅` и `[VERIFY] ✅`
- [ ] В логах НЕ должно появляться `drop 12328.0x` или другие безумные числа. Если появляются `отброшен outlier` — корень в bot_server.py ещё не починен, но защита работает.

После этого можно переходить к Фазе 1 (новая стратегия: trend filter + Fib + Div). Для этого мне понадобится `bot_server.py`.
