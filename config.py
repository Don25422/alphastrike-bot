import os
from dataclasses import dataclass, field
from typing import List
from dotenv import load_dotenv

load_dotenv()


@dataclass
class BotConfig:
    exchange_id: str = "bybit"
    paper_trading: bool = True    # Keep True for paper soak test; set False for testnet/live orders
    use_testnet: bool = os.getenv("USE_TESTNET", "true").lower() == "true"

    # API Credentials loaded safely from .env
    api_key: str = os.getenv("BYBIT_API_KEY", "")
    api_secret: str = os.getenv("BYBIT_API_SECRET", "")

    initial_balance: float = 100000.0

    # Top 6 Backtested High-Beta Perpetual Pairs (+45.94% 90-day basket)
    watchlist: List[str] = field(default_factory=lambda: [
        "AVAX/USDT:USDT",
        "SOL/USDT:USDT",
        "ETH/USDT:USDT",
        "LINK/USDT:USDT",
        "SUI/USDT:USDT",
        "DOGE/USDT:USDT"
    ])

    # Timeframes
    htf_timeframe: str = "4h"
    ltf_timeframe: str = "1h"

    # Asymmetric Risk Parameters
    risk_per_trade_pct: float = 0.01      # 1.0% risk per trade
    reward_to_risk: float = 2.5           # 2.5R runner target (with 50% partial at 1.2R)
    breakeven_trigger_r: float = 1.2
    max_open_positions: int = 3
    max_daily_drawdown_pct: float = 0.05  # 5% hard daily circuit breaker

    # Fee Simulation (Maker / Taker)
    maker_fee: float = 0.0002
    taker_fee: float = 0.00055