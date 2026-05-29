# PHANTOM v1.0

```
██████╗ ██╗  ██╗ █████╗ ███╗   ██╗████████╗ ██████╗ ███╗   ███╗
██╔══██╗██║  ██║██╔══██╗████╗  ██║╚══██╔══╝██╔═══██╗████╗ ████║
██████╔╝███████║███████║██╔██╗ ██║   ██║   ██║   ██║██╔████╔██║
██╔═══╝ ██╔══██║██╔══██║██║╚██╗██║   ██║   ██║   ██║██║╚██╔╝██║
██║     ██║  ██║██║  ██║██║ ╚████║   ██║   ╚██████╔╝██║ ╚═╝ ██║
╚═╝     ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═══╝   ╚═╝    ╚═════╝ ╚═╝     ╚═╝

         AUTONOMOUS CRYPTO FUTURES BOT • OKX
```

## 📦 Что это

Полноценный автономный торговый бот для криптовалютных фьючерсов на бирже OKX.

### Возможности:
- ✅ **Multi-tier стратегия** — 50 топ-пар разделены на 4 тира с разными лимитами
- ✅ **16 технических индикаторов** — RSI, MACD, EMA, ADX, Bollinger, StochRSI, CCI, ATR, OBV, VWAP, Ichimoku, Williams %R, MFI, SuperTrend, Volume, BB Width
- ✅ **5 торговых стратегий** — Trend, Scalp, Breakout, DCA, Mean Reversion
- ✅ **Multi-level Partial Take-Profit** — закрытие позиции по частям
- ✅ **8 уровней защиты от слива** депозита
- ✅ **WebSocket real-time** данные
- ✅ **Telegram алерты** с daily summary
- ✅ **Auto state persistence** с auto-restore
- ✅ **Walk-forward validation** для проверки робастности
- ✅ **Backtest engine** на исторических данных

---

## 🚀 Быстрый старт

### Требования
- **Python 3.10+** ([скачать](https://python.org))
- **OKX аккаунт** ([регистрация](https://www.okx.com/))
- **Demo Trading** (виртуальные USDT для тестирования)

### Настройка (5 минут)

#### 1. Скопируйте файлы
Поместите все файлы в папку `C:\phantom\` (или другую без пробелов и кириллицы).

#### 2. Получите API ключи OKX

1. Войдите в OKX → переключитесь в **Demo Trading** (правый верхний угол)
2. Перейдите в **Profile → API**: https://www.okx.com/account/my-api
3. Создайте **V5 API Key** с правами:
   - ☑ **Read**
   - ☑ **Trade**
   - ❌ **Withdraw** (НЕ ставьте!)
4. **Придумайте Passphrase** (это третий секрет, помимо key и secret)
5. Сохраните 3 значения: API Key, Secret, Passphrase

#### 3. Заполните `.env`

Откройте `.env` в Блокноте, впишите:
```env
OKX_API_KEY=ваш_ключ
OKX_API_SECRET=ваш_секрет
OKX_PASSPHRASE=ваш_passphrase
OKX_TESTNET=true
```

#### 4. Запустите бота

Двойной клик на **`start.bat`**

При первом запуске:
- Создастся виртуальное окружение Python
- Установятся зависимости (~150 MB, 3-5 минут)
- Откроется браузер с дашбордом http://localhost:8000

---

## 📁 Структура проекта

```
phantom/
├── bot_server.py            ← Главный сервер
├── exchange_client.py       ← OKX API клиент
├── ws_stream.py             ← WebSocket stream
├── position_mode.py         ← Position mode manager
├── partial_tp.py            ← Multi-level take-profit
├── liquidation_monitor.py   ← Защита от ликвидации
├── order_health.py          ← Throttling критических ошибок
├── telegram_notifier.py     ← Telegram алерты
├── reconciler.py            ← Sync с биржей
├── state_store.py           ← Персистентность
├── network_safety.py        ← Rate limiter, circuit breaker
├── fees_funding.py          ← Расчёт комиссий
├── backtest.py              ← Бэктест-движок (CLI)
├── walk_forward.py          ← Walk-forward validation (CLI)
├── dashboard.html           ← Веб-интерфейс
├── .env                     ← Конфигурация
├── start.bat                ← Запуск (Windows)
├── requirements.txt         ← Python зависимости
└── README.md                ← Эта документация
```

---

## 🛡 Защиты от слива депозита

| # | Защита | Параметр |
|---|--------|----------|
| 1 | **Stop-Loss** на каждой сделке | `SL_PCT=3.0` |
| 2 | **Risk per trade** | `RISK_PER_TRADE=1.0` |
| 3 | **Daily Loss Limit** | `MAX_DAILY_LOSS=5.0` |
| 4 | **Drawdown Guard** (главная!) | `MAX_DRAWDOWN_STOP=15.0` |
| 5 | **Liquidation Monitor** | `LIQ_EMERGENCY_PCT=15.0` |
| 6 | **Position Limits** | `MAX_POSITIONS=10` |
| 7 | **Network Watchdog** | `PANIC_CLOSE_AFTER_S=600` |
| 8 | **Order Health Throttle** | `ORDER_HEALTH_THRESHOLD=5` |

При **дефолтных параметрах** максимально возможная потеря ≈ **15%** от депозита.

---

## 🧪 Бэктест и Walk-Forward

### Бэктест на исторических данных:
```powershell
cd C:\phantom
venv\Scripts\activate
python backtest.py --symbol BTCUSDT --days 90
python backtest.py --symbols TOP50 --days 30
python backtest.py --symbol ETHUSDT --days 60 --partial-tp 3:50,6:30,10:20
```

### Walk-forward validation:
```powershell
python walk_forward.py --symbol BTCUSDT --days 180 --train 30 --test 15 --step 15
```

---

## ⚙ Все параметры `.env`

См. файл `.env` — все параметры с комментариями.

---

## 🎯 Roadmap к mainnet

### Этап 1 — Demo Trading (4-6 недель)
- Запуск с `OKX_TESTNET=true`
- Депозит 10000+ виртуальных USDT
- Минимум 100 сделок для статистики

### Этап 2 — Backtest + Walk-Forward
```powershell
python backtest.py --symbols TOP50 --days 90
python walk_forward.py --symbol BTCUSDT --days 180
```

Критерии для перехода на mainnet:
- Win Rate ≥ 50%
- Profit Factor ≥ 1.3
- Max Drawdown ≤ 10%
- Walk-Forward overfitting gap < 10%

### Этап 3 — Mainnet с минимальным депозитом
```env
OKX_TESTNET=false
RISK_PER_TRADE=0.5
LEVERAGE=3
MAX_POSITIONS=5
```

Депозит **$100-200** в первые 3 месяца.

### Этап 4 — Масштабирование
Только если за 3 месяца ROI > +20% и нет существенных проблем.

---

## ⚠️ Дисклеймер

Торговля криптовалютными фьючерсами с плечом несёт **высокий риск полной потери депозита**.

**Бот не гарантирует прибыль.** Используйте только средства, потерю которых готовы принять.

Перед mainnet-запуском **обязательно**:
1. Бэктест на 90+ днях ✅
2. Walk-forward validation ✅
3. Минимум 4 недели на Demo Trading ✅
4. Минимальный депозит $100-200 ✅
5. `RISK_PER_TRADE` ≤ 1%, `LEVERAGE` ≤ 5x ✅

Автор не несёт ответственности за торговые убытки.

---

## 🔧 Troubleshooting

### "Python is not recognized"
Python не в PATH. Переустановите с галочкой **☑ Add Python to PATH**.

### "Module not found: numpy"
Зависимости не установились:
```powershell
venv\Scripts\activate
pip install -r requirements.txt
```

### "Position mode: not one_way"
- Войдите в OKX → Settings → Position Mode → **Net Mode**
- Закройте все открытые позиции
- Перезапустите бот

### "Invalid signature" / "401 Unauthorized"
Проверьте `.env`:
- API ключи скопированы без пробелов
- Passphrase точно тот, что вы указали при создании ключа
- Не перепутали testnet/mainnet ключи

### "ERR_CONNECTION_REFUSED"
Бот не запустился или упал. Проверьте логи в PowerShell.

---

## 📞 Поддержка

При проблемах присылайте:
- Скриншот ошибки в PowerShell
- Файл `phantom_bot.log` (последние 100-500 строк)
- Файл `.env` с **замазанными ключами** (XXX вместо реальных значений)

---

**Удачной торговли! 🚀**
