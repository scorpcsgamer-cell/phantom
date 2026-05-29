"""
Модуль персистентности: сохраняет состояние бота на диск,
восстанавливает после перезапуска.

Сохраняется:
  • Открытые позиции (с SL/TP/trailing данными)
  • История сделок (для статистики)
  • Equity curve
  • Cooldowns
  • PnL метрики
  • Стартовый/пиковый баланс

Файлы:
  state/positions.json     — текущие открытые позиции
  state/trade_history.json — история закрытых сделок
  state/metrics.json        — метрики (PnL, wins, losses, drawdown)
  state/cooldowns.json      — активные cooldowns
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

log = logging.getLogger("PHANTOM.state")


class StateStore:
    """Атомарная запись JSON-файлов с резервным копированием."""

    def __init__(self, state_dir: str = "state"):
        self.dir = Path(state_dir)
        self.dir.mkdir(exist_ok=True)

    def _path(self, name: str) -> Path:
        return self.dir / f"{name}.json"

    def save(self, name: str, data) -> bool:
        """Атомарная запись: сначала во временный файл, потом rename."""
        try:
            target = self._path(name)
            tmp    = target.with_suffix(".json.tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)
            # Атомарная замена (на Windows тоже работает)
            os.replace(tmp, target)
            return True
        except Exception as e:
            log.error(f"Не удалось сохранить {name}: {e}")
            return False

    def load(self, name: str, default=None):
        """Загружает с диска. Если файла нет — возвращает default."""
        path = self._path(name)
        if not path.exists():
            return default
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            log.warning(f"Не удалось загрузить {name}: {e}, использую default")
            # Резервная копия повреждённого файла
            backup = path.with_suffix(f".json.broken.{int(time.time())}")
            try:
                os.rename(path, backup)
                log.info(f"Повреждённый файл сохранён как {backup}")
            except Exception:
                pass
            return default

    def exists(self, name: str) -> bool:
        return self._path(name).exists()


class StateManager:
    """
    Высокоуровневый интерфейс для бота: сохранение/восстановление
    всего состояния позиции и метрик.
    """

    def __init__(self, state_dir: str = "state"):
        self.store = StateStore(state_dir)
        self._dirty = False

    def mark_dirty(self):
        self._dirty = True

    # ── Positions ───────────────────────────
    def save_positions(self, positions: dict) -> bool:
        return self.store.save("positions", positions)

    def load_positions(self) -> dict:
        return self.store.load("positions", default={})

    # ── Trade history ────────────────────────
    def save_trade_history(self, trades: list) -> bool:
        # Ограничиваем последними 5000 сделок
        return self.store.save("trade_history", list(trades)[-5000:])

    def load_trade_history(self) -> list:
        return self.store.load("trade_history", default=[])

    # ── Metrics ──────────────────────────────
    def save_metrics(self, metrics: dict) -> bool:
        return self.store.save("metrics", metrics)

    def load_metrics(self) -> dict:
        return self.store.load("metrics", default={
            "wins": 0, "losses": 0,
            "total_pnl": 0.0, "daily_pnl": 0.0,
            "best_trade": 0.0, "worst_trade": 0.0,
            "start_balance": 0.0, "peak_balance": 0.0,
            "max_drawdown": 0.0,
            "fees_total": 0.0, "funding_total": 0.0,
            "last_save_time": None,
            "daily_reset_date": None,
        })

    # ── Equity curve ─────────────────────────
    def save_equity(self, equity_history: list) -> bool:
        # Храним до 10000 точек (примерно неделя при тике 1 минута)
        return self.store.save("equity", list(equity_history)[-10000:])

    def load_equity(self) -> list:
        return self.store.load("equity", default=[])

    # ── Cooldowns ────────────────────────────
    def save_cooldowns(self, cooldowns: dict) -> bool:
        # Сохраняем только активные (not expired)
        active = {k: v for k, v in cooldowns.items() if v > time.time()}
        return self.store.save("cooldowns", active)

    def load_cooldowns(self) -> dict:
        data = self.store.load("cooldowns", default={})
        # Фильтруем устаревшие
        return {k: v for k, v in data.items() if v > time.time()}

    # ── Snapshot (полный) ────────────────────
    def save_full_snapshot(self, bot_state: dict) -> bool:
        """Сохранить всё состояние одним вызовом."""
        try:
            self.save_positions(bot_state.get("positions_dict", {}))
            self.save_trade_history(bot_state.get("trade_history", []))
            self.save_metrics(bot_state.get("metrics", {}))
            self.save_equity(bot_state.get("equity_history", []))
            self.save_cooldowns(bot_state.get("cooldowns", {}))
            return True
        except Exception as e:
            log.error(f"Snapshot save failed: {e}")
            return False

    def load_full_snapshot(self) -> dict:
        """Загрузить всё состояние одним вызовом."""
        return {
            "positions":      self.load_positions(),
            "trade_history":  self.load_trade_history(),
            "metrics":        self.load_metrics(),
            "equity_history": self.load_equity(),
            "cooldowns":      self.load_cooldowns(),
        }

    def has_saved_state(self) -> bool:
        return any(self.store.exists(name) for name in
                   ("positions", "metrics", "trade_history"))
