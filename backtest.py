import os
import time
import ccxt
import pandas as pd
from datetime import datetime, timedelta, timezone
from config import BotConfig
from strategy import evaluate_smc_setup
from risk_manager import RiskManager


def fetch_historical_candles(exchange, symbol: str, timeframe: str, days: int, use_cache: bool = True) -> pd.DataFrame:
    os.makedirs("data_cache", exist_ok=True)
    safe_sym = symbol.replace("/", "_").replace(":", "_")
    cache_file = f"data_cache/{exchange.id}_{safe_sym}_{timeframe}_{days}d.csv"

    if use_cache and os.path.exists(cache_file):
        return pd.read_csv(cache_file)

    since = exchange.parse8601((datetime.now(timezone.utc) - timedelta(days=days)).isoformat())
    all_ohlcv = []

    while True:
        for attempt in range(5):
            try:
                batch = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=1000)
                break
            except (ccxt.RateLimitExceeded, ccxt.NetworkError):
                wait = 2 * (attempt + 1)
                print(f"  Rate limit hit on {symbol} ({timeframe}), waiting {wait}s...")
                time.sleep(wait)
        else:
            raise RuntimeError(f"Failed to fetch {symbol} after 5 retries")

        if not batch:
            break
        all_ohlcv.extend(batch)
        last_ts = batch[-1][0]
        if len(batch) < 1000 or last_ts >= exchange.milliseconds() - 60_000:
            break
        since = last_ts + 1
        time.sleep(0.4)  # Respect Bybit public endpoint rate limits

    df = pd.DataFrame(all_ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df = df.drop_duplicates(subset=["timestamp"]).reset_index(drop=True)
    df.to_csv(cache_file, index=False)
    return df


def run_backtest(days: int = 90):
    config = BotConfig()
    exchange = getattr(ccxt, config.exchange_id)({
        "enableRateLimit": True,
        "rateLimit": 400,
        "options": {"defaultType": "swap"}
    })
    risk = RiskManager(config)

    balance = config.initial_balance
    peak_balance = balance
    max_drawdown_pct = 0.0

    total_wins = 0
    total_losses = 0
    total_be = 0
    gross_profit = 0.0
    gross_loss = 0.0
    pair_stats = []

    print(f"Fetching {days} days of historical data from {config.exchange_id.upper()} (with local cache)...")

    for symbol in config.watchlist:
        print(f"Backtesting {symbol}...")
        htf_df = fetch_historical_candles(exchange, symbol, config.htf_timeframe, days + 35)
        ltf_df = fetch_historical_candles(exchange, symbol, config.ltf_timeframe, days)
        time.sleep(0.5)

        if len(ltf_df) < 100 or len(htf_df) < 205:
            print(f"  Skipping {symbol}: insufficient candle history")
            continue

        position = None
        p_wins, p_losses, p_be = 0, 0, 0
        p_net_pnl = 0.0

        for i in range(60, len(ltf_df) - 1):
            curr_candle = ltf_df.iloc[i]
            curr_ts = curr_candle["timestamp"]

            # 1. Manage active position with Partial TP + Trailing Stop
            if position:
                high = curr_candle["high"]
                low = curr_candle["low"]
                r_dist = position["risk_dist"]

                if position["side"] == "LONG":
                    if not position["partial_taken"] and high >= position["entry"] + (1.2 * r_dist):
                        partial_qty = position["qty"] * 0.5
                        partial_exit = position["entry"] + (1.2 * r_dist)
                        pnl = (partial_exit - position["entry"]) * partial_qty - (partial_exit * partial_qty * config.maker_fee)
                        balance += pnl
                        p_net_pnl += pnl
                        gross_profit += pnl
                        position["qty"] -= partial_qty
                        position["sl"] = position["entry"]
                        position["partial_taken"] = True

                    if position["partial_taken"]:
                        new_trail = high - (1.5 * r_dist)
                        if new_trail > position["sl"]:
                            position["sl"] = new_trail

                    if low <= position["sl"]:
                        exit_price = position["sl"]
                        pnl = (exit_price - position["entry"]) * position["qty"] - (exit_price * position["qty"] * config.taker_fee)
                        balance += pnl
                        p_net_pnl += pnl
                        if position["partial_taken"]:
                            total_wins += 1
                            p_wins += 1
                            if pnl > 0:
                                gross_profit += pnl
                        else:
                            total_losses += 1
                            p_losses += 1
                            gross_loss += abs(pnl)
                        position = None

                    elif high >= position["tp"]:
                        exit_price = position["tp"]
                        pnl = (exit_price - position["entry"]) * position["qty"] - (exit_price * position["qty"] * config.maker_fee)
                        balance += pnl
                        p_net_pnl += pnl
                        total_wins += 1
                        p_wins += 1
                        gross_profit += pnl
                        position = None

                else:  # SHORT
                    if not position["partial_taken"] and low <= position["entry"] - (1.2 * r_dist):
                        partial_qty = position["qty"] * 0.5
                        partial_exit = position["entry"] - (1.2 * r_dist)
                        pnl = (position["entry"] - partial_exit) * partial_qty - (partial_exit * partial_qty * config.maker_fee)
                        balance += pnl
                        p_net_pnl += pnl
                        gross_profit += pnl
                        position["qty"] -= partial_qty
                        position["sl"] = position["entry"]
                        position["partial_taken"] = True

                    if position["partial_taken"]:
                        new_trail = low + (1.5 * r_dist)
                        if new_trail < position["sl"]:
                            position["sl"] = new_trail

                    if high >= position["sl"]:
                        exit_price = position["sl"]
                        pnl = (position["entry"] - exit_price) * position["qty"] - (exit_price * position["qty"] * config.taker_fee)
                        balance += pnl
                        p_net_pnl += pnl
                        if position["partial_taken"]:
                            total_wins += 1
                            p_wins += 1
                            if pnl > 0:
                                gross_profit += pnl
                        else:
                            total_losses += 1
                            p_losses += 1
                            gross_loss += abs(pnl)
                        position = None

                    elif low <= position["tp"]:
                        exit_price = position["tp"]
                        pnl = (position["entry"] - exit_price) * position["qty"] - (exit_price * position["qty"] * config.maker_fee)
                        balance += pnl
                        p_net_pnl += pnl
                        total_wins += 1
                        p_wins += 1
                        gross_profit += pnl
                        position = None

                if balance > peak_balance:
                    peak_balance = balance
                dd = (peak_balance - balance) / peak_balance
                if dd > max_drawdown_pct:
                    max_drawdown_pct = dd
                continue

            # 2. Evaluate new setup on zero look-ahead slice
            htf_slice = htf_df[htf_df["timestamp"] <= curr_ts].tail(210)
            ltf_slice = ltf_df.iloc[i - 59 : i + 1]

            signal = evaluate_smc_setup(symbol, htf_slice, ltf_slice, config.reward_to_risk)
            if signal:
                qty = risk.calculate_position_size(balance, signal.entry_price, signal.stop_loss)
                if qty > 0:
                    entry_fee = (signal.entry_price * qty) * config.maker_fee
                    balance -= entry_fee
                    p_net_pnl -= entry_fee
                    position = {
                        "side": signal.side,
                        "entry": signal.entry_price,
                        "sl": signal.stop_loss,
                        "tp": signal.take_profit,
                        "qty": qty,
                        "risk_dist": abs(signal.entry_price - signal.stop_loss),
                        "partial_taken": False
                    }

        pair_stats.append((symbol, p_wins + p_losses + p_be, p_wins, p_losses, p_net_pnl))

    total_trades = total_wins + total_losses + total_be
    win_rate = (total_wins / total_trades * 100) if total_trades > 0 else 0.0
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else float("inf")
    net_return_pct = ((balance - config.initial_balance) / config.initial_balance) * 100

    print("\n" + "=" * 56)
    print(f"ALPHASTRIKE {days}-DAY BACKTEST REPORT (PARTIAL + TRAIL)")
    print("=" * 56)
    for sym, trades, w, l, pnl in pair_stats:
        print(f"  {sym:<15} | Trades: {trades:<3} | W/L: {w}/{l:<3} | PnL: ${pnl:+,.2f}")
    print("-" * 56)
    print(f"Starting Balance : ${config.initial_balance:,.2f}")
    print(f"Final Balance    : ${balance:,.2f} ({net_return_pct:+.2f}%)")
    print(f"Total Trades     : {total_trades} (Wins: {total_wins} | Losses: {total_losses})")
    print(f"Win Rate         : {win_rate:.1f}%")
    print(f"Profit Factor    : {profit_factor:.2f}")
    print(f"Max Drawdown     : {max_drawdown_pct * 100:.2f}%")
    print("=" * 56)


if __name__ == "__main__":
    run_backtest(days=360)