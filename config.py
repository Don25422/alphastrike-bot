import os
from dataclasses import dataclass, field
from typing import Dict, List, Any
from dotenv import load_dotenv

load_dotenv()


@dataclass
class BotConfig:
    exchange_id: str = "bybit"
    paper_trading: bool = True    # Default fallback; overridden by persisted UI mode in alphastrike.db

    # API Credentials loaded safely from .env
    api_key: str = os.getenv("BYBIT_API_KEY", "")
    api_secret: str = os.getenv("BYBIT_API_SECRET", "")

    initial_balance: float = 100000.0

    # Top 6 Backtested High-Beta Perpetual Pairs
    watchlist: List[str] = field(default_factory=lambda: [
        "AVAX/USDT:USDT",
        "SOL/USDT:USDT",
        "ETH/USDT:USDT",
        "LINK/USDT:USDT",
        "SUI/USDT:USDT",
        "DOGE/USDT:USDT",
        "XAU/USDT:USDT",
        "XRP/USDT:USDT",
        "AAVE/USDT:USDT"
    ])

    # Per-pair custom overrides: {"SOL/USDT:USDT": {"risk_per_trade_pct": 0.015, "reward_to_risk": 3.0, "enabled": True}}
    pair_overrides: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    # Timeframes
    htf_timeframe: str = "4h"
    ltf_timeframe: str = "1h"

    # Asymmetric Risk Parameters (Global Defaults)
    risk_per_trade_pct: float = 0.01      # 1.0% risk per trade
    reward_to_risk: float = 2.5           # 2.5R runner target (with 50% partial at 1.2R)
    breakeven_trigger_r: float = 1.2
    max_open_positions: int = 3
    max_daily_drawdown_pct: float = 0.05  # 5% hard daily circuit breaker

    # Fee Simulation (Maker / Taker)
    maker_fee: float = 0.0002
    taker_fee: float = 0.00055

    def get_pair_params(self, symbol: str) -> Dict[str, Any]:
        ov = self.pair_overrides.get(symbol, {})
        return {
            "enabled": ov.get("enabled", symbol in self.watchlist),
            "risk_per_trade_pct": float(ov.get("risk_per_trade_pct", self.risk_per_trade_pct)),
            "reward_to_risk": float(ov.get("reward_to_risk", self.reward_to_risk)),
            "is_custom": symbol in self.pair_overrides and len(self.pair_overrides[symbol]) > 0,
        }