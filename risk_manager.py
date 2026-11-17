from datetime import datetime, timezone


class RiskManager:
    def __init__(self, config):
        self.config = config
        self.day_start_equity = config.initial_balance
        self.current_day = datetime.now(timezone.utc).date()
        self.circuit_breaker_tripped = False

    def check_daily_reset(self, current_equity: float):
        today = datetime.now(timezone.utc).date()
        if today != self.current_day:
            self.current_day = today
            self.day_start_equity = current_equity
            self.circuit_breaker_tripped = False

    def can_trade(self, current_equity: float, open_positions_count: int) -> tuple[bool, str]:
        self.check_daily_reset(current_equity)

        if current_equity <= 0:
            return False, "Account balance is $0.00 — waiting for deposit"

        if self.day_start_equity <= 0:
            self.day_start_equity = current_equity

        if self.circuit_breaker_tripped:
            return False, "Circuit Breaker Tripped: Halted until next UTC day"

        drawdown = (self.day_start_equity - current_equity) / self.day_start_equity
        if drawdown >= self.config.max_daily_drawdown_pct:
            self.circuit_breaker_tripped = True
            return False, f"Circuit Breaker Tripped: Daily drawdown {drawdown*100:.2f}%"

        if open_positions_count >= self.config.max_open_positions:
            return False, "Max concurrent positions reached"

        return True, "OK"

    def calculate_position_size(self, equity: float, entry_price: float, stop_loss: float, risk_pct: float | None = None) -> float:
        """
        Calculates exact asset quantity so a stop-loss hit equals `risk_pct` (or global `risk_per_trade_pct`) of equity.
        """
        if equity <= 0 or entry_price <= 0:
            return 0.0

        active_risk_pct = risk_pct if risk_pct is not None else self.config.risk_per_trade_pct
        risk_dollars = equity * active_risk_pct
        stop_distance = abs(entry_price - stop_loss)
        if stop_distance <= 0:
            return 0.0

        qty = risk_dollars / stop_distance
        # Cap notional exposure per trade at 2.5x account equity to avoid liquidation traps
        max_notional = equity * 2.5
        if (qty * entry_price) > max_notional:
            qty = max_notional / entry_price

        return round(qty, 4)