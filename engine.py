import asyncio
import json
import time
import traceback
from collections import deque
from datetime import datetime, timezone
from typing import Deque, Dict
import ccxt.async_support as ccxt
import pandas as pd
from strategy import evaluate_smc_setup
from risk_manager import RiskManager
from database import TradeDatabase


class BybitRateLimiter:
    """Enforces <= 20 REST requests/sec (well below Bybit's 30 req/sec limit) with async locking."""

    def __init__(self, max_per_second: int = 20):
        self.max_per_second = max_per_second
        self.timestamps: Deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            while self.timestamps and now - self.timestamps[0] >= 1.0:
                self.timestamps.popleft()
            if len(self.timestamps) >= self.max_per_second:
                sleep_for = 1.0 - (now - self.timestamps[0]) + 0.02
                if sleep_for > 0:
                    await asyncio.sleep(sleep_for)
            self.timestamps.append(time.monotonic())


class TradingEngine:
    def __init__(self, config):
        self.config = config
        self.limiter = BybitRateLimiter(max_per_second=20)
        self.exchange = self._build_exchange(paper_trading=config.paper_trading)
        self.db = TradeDatabase(config.initial_balance)
        self._hydrate_persistent_config()
        self.balance = self._load_initial_mode_balance()
        self.risk = RiskManager(config)
        self.risk.day_start_equity = self.balance
        self.positions = self.db.load_open_positions(mode=self.active_mode)
        self.latest_prices: Dict[str, float] = {}
        self.mark_prices: Dict[str, float] = {}
        self.funding_rates: Dict[str, float] = {}
        self.price_history: Dict[str, list] = {}
        self.pair_telemetry: Dict[str, str] = {}
        self.available_markets = []
        self.logs = []
        self.is_running = False
        self._loop_active = False
        self._ohlcv_cache = {}
        self._last_block_log = 0.0
        self._last_scan_log = 0.0

    @property
    def active_mode(self) -> str:
        return "paper" if self.config.paper_trading else "live"

    def _hydrate_persistent_config(self):
        """Restores watchlist, global risk parameters, and per-pair overrides from SQLite on boot."""
        saved_watchlist = self.db.get_setting("watchlist", None)
        if saved_watchlist:
            try:
                parsed = json.loads(saved_watchlist)
                if isinstance(parsed, list) and len(parsed) > 0:
                    self.config.watchlist = parsed
            except Exception:
                pass

        saved_risk = self.db.get_setting("global_risk_settings", None)
        if saved_risk:
            try:
                r = json.loads(saved_risk)
                if "risk_per_trade_pct" in r:
                    self.config.risk_per_trade_pct = float(r["risk_per_trade_pct"])
                if "max_open_positions" in r:
                    self.config.max_open_positions = int(r["max_open_positions"])
                if "reward_to_risk" in r:
                    self.config.reward_to_risk = float(r["reward_to_risk"])
            except Exception:
                pass

        saved_overrides = self.db.get_setting("pair_overrides", None)
        if saved_overrides:
            try:
                ov = json.loads(saved_overrides)
                if isinstance(ov, dict):
                    self.config.pair_overrides = ov
            except Exception:
                pass

    def persist_config(self):
        self.db.set_setting("watchlist", json.dumps(self.config.watchlist))
        self.db.set_setting(
            "global_risk_settings",
            json.dumps({
                "risk_per_trade_pct": self.config.risk_per_trade_pct,
                "max_open_positions": self.config.max_open_positions,
                "reward_to_risk": self.config.reward_to_risk,
            }),
        )
        self.db.set_setting("pair_overrides", json.dumps(self.config.pair_overrides))

    async def _safe_call(self, coro_fn, *args, retries: int = 3, **kwargs):
        """Executes Bybit API calls with rate-limit throttling and exponential backoff."""
        for attempt in range(retries):
            await self.limiter.acquire()
            try:
                return await coro_fn(*args, **kwargs)
            except ccxt.RateLimitExceeded:
                backoff = 0.6 * (2 ** attempt)
                self.log(f"Bybit rate limit hit — backing off {backoff:.1f}s")
                await asyncio.sleep(backoff)
            except ccxt.NetworkError:
                if attempt == retries - 1:
                    raise
                await asyncio.sleep(0.35 * (2 ** attempt))

    def _load_initial_mode_balance(self) -> float:
        if self.config.paper_trading:
            saved_paper = self.db.get_setting("paper_balance", None)
            if saved_paper is not None:
                return round(float(saved_paper), 2)
            bal = self.db.get_current_balance(mode="paper")
            self.db.set_setting("paper_balance", str(bal))
            return bal
        return self.db.get_current_balance(mode="live")

    def get_wallet_history(self, mode: str) -> list:
        raw = self.db.get_setting(f"wallet_ledger_{mode}", "[]")
        try:
            items = json.loads(raw)
            return items if isinstance(items, list) else []
        except Exception:
            return []

    def _record_wallet_event(self, mode: str, action: str, amount: float, balance_after: float, note: str):
        items = self.get_wallet_history(mode)
        items.insert(0, {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "action": action.upper(),
            "amount": round(float(amount), 2),
            "balance_after": round(float(balance_after), 2),
            "note": note,
        })
        self.db.set_setting(f"wallet_ledger_{mode}", json.dumps(items[:50]))

    def _build_exchange(self, paper_trading: bool):
        exchange_params = {
            "enableRateLimit": True,
            "rateLimit": 100,
            "timeout": 30000,
            "options": {
                "defaultType": "swap",
                "recvWindow": 5000,
                "accountType": "UNIFIED",
            },
        }
        if not paper_trading:
            if not self.config.api_key or not self.config.api_secret:
                raise ValueError("Missing BYBIT_API_KEY or BYBIT_API_SECRET in .env for live trading!")
            exchange_params["apiKey"] = self.config.api_key
            exchange_params["secret"] = self.config.api_secret
        return getattr(ccxt, self.config.exchange_id)(exchange_params)

    def record_price_tick(self, symbol: str, price: float, mark_price: float = None, funding_rate: float = None):
        self.latest_prices[symbol] = price
        if mark_price and mark_price > 0:
            self.mark_prices[symbol] = mark_price
        if funding_rate is not None:
            self.funding_rates[symbol] = funding_rate

        hist = self.price_history.setdefault(symbol, [])
        if not hist or hist[-1] != price:
            hist.append(price)
            if len(hist) > 45:
                del hist[0]

    def log(self, msg: str):
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        entry = f"[{ts}] {msg}"
        print(entry)
        self.logs.insert(0, entry)
        self.logs = self.logs[:100]

    async def load_tradeable_markets(self):
        try:
            markets = await self._safe_call(self.exchange.load_markets)
            linear_usdt = []
            for sym, m in markets.items():
                if (
                    m.get("active", True)
                    and m.get("swap", False)
                    and m.get("linear", False)
                    and m.get("settle") == "USDT"
                    and sym.endswith(":USDT")
                ):
                    linear_usdt.append({
                        "symbol": sym,
                        "short": m.get("base", sym.split("/")[0]),
                        "name": f"{m.get('base', sym.split('/')[0])} Perpetual"
                    })
            linear_usdt.sort(key=lambda x: x["short"])
            self.available_markets = linear_usdt
            self.log(f"Loaded {len(self.available_markets)} tradeable Bybit USDT perpetual markets.")
        except Exception as e:
            self.log(f"Market catalog sync warning: {e}")

    async def sync_batch_tickers(self):
        """Fetches Last Price, Mark Price, and Funding Rate for all watchlist pairs in 1 batched API call."""
        symbols = list(set(self.config.watchlist + list(self.positions.keys())))
        if not symbols:
            return
        try:
            tickers = await self._safe_call(self.exchange.fetch_tickers, symbols)
            for sym, ticker in tickers.items():
                info = ticker.get("info") or {}
                last_px = float(ticker.get("last") or info.get("lastPrice") or 0.0)
                mark_px = float(info.get("markPrice") or last_px)
                funding = float(info.get("fundingRate") or 0.0)
                if last_px > 0:
                    self.record_price_tick(sym, last_px, mark_price=mark_px, funding_rate=funding)
        except Exception as e:
            self.log(f"Batch ticker sync note: {type(e).__name__}: {e}")

    async def sync_live_balance(self):
        if self.config.paper_trading:
            return
        try:
            bal = await self._safe_call(self.exchange.fetch_balance, {"type": "unified"})
            usdt_info = bal.get("USDT") or {}
            usdt_total = usdt_info.get("total", 0.0)
            self.balance = round(float(usdt_total or 0.0), 2)
        except Exception as e:
            self.balance = 0.0
            self.log(f"Balance sync warning (defaulted to $0.00): {e}")

    async def switch_mode(self, mode: str) -> str:
        mode = mode.lower().strip()
        if mode not in ("paper", "live"):
            raise ValueError("Mode must be 'paper' or 'live'.")
        if self.positions:
            raise ValueError("Close all open positions before switching trading modes.")
        old_exchange = self.exchange
        self._last_block_log = 0.0
        self._last_scan_log = 0.0
        if mode == "paper":
            self.config.paper_trading = True
            self.exchange = self._build_exchange(paper_trading=True)
            if old_exchange:
                await old_exchange.close()
            self.positions = self.db.load_open_positions(mode="paper")
            saved_paper_bal = self.db.get_setting("paper_balance", None)
            if saved_paper_bal is not None:
                self.balance = round(float(saved_paper_bal), 2)
            else:
                self.balance = self.db.get_current_balance(mode="paper")
                self.db.set_setting("paper_balance", str(self.balance))
            self.risk.day_start_equity = self.balance
            self.log(f"MODE SWITCHED -> PAPER TRADING | Balance: ${self.balance:,.2f}")
            return "PAPER TRADING"

        if not self.config.api_key or not self.config.api_secret:
            raise ValueError("Missing BYBIT_API_KEY or BYBIT_API_SECRET in .env!")
        new_exchange = self._build_exchange(paper_trading=False)
        try:
            await new_exchange.load_markets(True)
        except Exception as e:
            await new_exchange.close()
            raise ValueError(f"Failed to connect to Bybit Mainnet: {e}")
        self.config.paper_trading = False
        self.exchange = new_exchange
        if old_exchange:
            await old_exchange.close()
        self.positions = self.db.load_open_positions(mode="live")
        self.balance = 0.0
        await self.sync_live_balance()
        self.risk.day_start_equity = self.balance
        self.log(f"MODE SWITCHED -> LIVE MAINNET | Synced Unified USDT Balance: ${self.balance:,.2f}")
        return "LIVE MAINNET"

    async def adjust_wallet(self, action: str, amount: float) -> float:
        if amount <= 0:
            raise ValueError("Amount must be greater than 0.")
        if self.config.paper_trading:
            if action == "deposit":
                self.balance = round(self.balance + amount, 2)
            elif action == "withdraw":
                if amount > self.balance:
                    raise ValueError("Withdrawal exceeds available paper balance.")
                self.balance = round(self.balance - amount, 2)
            else:
                raise ValueError("Invalid action.")
            self.db.set_setting("paper_balance", str(self.balance))
            self.risk.day_start_equity = self.balance
            self._record_wallet_event(
                mode="paper",
                action=action,
                amount=amount,
                balance_after=self.balance,
                note=f"Paper {action.capitalize()}",
            )
            self.log(f"PAPER WALLET {action.upper()} | ${amount:,.2f} -> New Balance: ${self.balance:,.2f}")
            return self.balance

        from_acct = "FUND" if action == "deposit" else "UNIFIED"
        to_acct = "UNIFIED" if action == "deposit" else "FUND"
        await self._safe_call(self.exchange.transfer, "USDT", amount, from_acct, to_acct)
        await self.sync_live_balance()
        self.risk.day_start_equity = self.balance
        self._record_wallet_event(
            mode="live",
            action=action,
            amount=amount,
            balance_after=self.balance,
            note=f"{from_acct} -> {to_acct}",
        )
        self.log(f"BYBIT INTERNAL TRANSFER ({from_acct} -> {to_acct}) | ${amount:,.2f} USDT | Unified Balance: ${self.balance:,.2f}")
        return self.balance

    async def fetch_df(self, symbol: str, timeframe: str, limit: int = 210) -> pd.DataFrame:
        ttl = 300 if timeframe == "4h" else 60
        cache_key = (symbol, timeframe)
        now = time.time()
        if cache_key in self._ohlcv_cache:
            cached_at, cached_df = self._ohlcv_cache[cache_key]
            if now - cached_at < ttl:
                if timeframe == "1h" and not cached_df.empty and symbol not in self.latest_prices:
                    self.record_price_tick(symbol, float(cached_df["close"].iloc[-1]))
                return cached_df

        ohlcv = await self._safe_call(self.exchange.fetch_ohlcv, symbol, timeframe=timeframe, limit=limit)
        df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
        self._ohlcv_cache[cache_key] = (now, df)
        if timeframe == "1h" and not df.empty:
            if symbol not in self.price_history or len(self.price_history[symbol]) < 5:
                self.price_history[symbol] = [float(x) for x in df["close"].tail(30).tolist()]
            if symbol not in self.latest_prices:
                self.record_price_tick(symbol, float(df["close"].iloc[-1]))
        return df

    def _compute_pair_telemetry(self, symbol: str, htf_df: pd.DataFrame, ltf_df: pd.DataFrame):
        """Determines exact plain-English SMC scanner stage for Pair Cards."""
        if htf_df.empty or ltf_df.empty or len(htf_df) < 20 or len(ltf_df) < 15:
            self.pair_telemetry[symbol] = "syncing_candles"
            return

        htf_hi = float(htf_df["high"].tail(12).max())
        htf_lo = float(htf_df["low"].tail(12).min())
        htf_close = float(htf_df["close"].iloc[-1])
        bullish_bias = htf_close >= (htf_hi + htf_lo) / 2.0

        ltf_recent = ltf_df.iloc[-15:-2]
        ltf_swing_hi = float(ltf_recent["high"].max())
        ltf_swing_lo = float(ltf_recent["low"].min())
        prev_bar = ltf_df.iloc[-2]
        curr_bar = ltf_df.iloc[-1]

        if bullish_bias:
            swept_low = float(prev_bar["low"]) < ltf_swing_lo or float(curr_bar["low"]) < ltf_swing_lo
            if swept_low:
                self.pair_telemetry[symbol] = "fvg_waiting: Bullish sweep detected — confirming FVG entry"
            else:
                self.pair_telemetry[symbol] = "sweep_waiting: Bullish 4H trend — watching 1H sell-side liquidity"
        else:
            swept_high = float(prev_bar["high"]) > ltf_swing_hi or float(curr_bar["high"]) > ltf_swing_hi
            if swept_high:
                self.pair_telemetry[symbol] = "fvg_waiting: Bearish sweep detected — confirming FVG entry"
            else:
                self.pair_telemetry[symbol] = "sweep_waiting: Bearish 4H trend — watching 1H buy-side liquidity"

    async def place_live_entry(self, symbol: str, side: str, qty: float, sl: float, tp: float):
        order_side = "buy" if side == "LONG" else "sell"
        formatted_qty = float(self.exchange.amount_to_precision(symbol, qty))
        formatted_sl = float(self.exchange.price_to_precision(symbol, sl))
        formatted_tp = float(self.exchange.price_to_precision(symbol, tp))
        # Use MarkPrice for Stop-Loss trigger to prevent single-exchange wick stop-outs
        params = {
            "stopLoss": {"triggerPrice": formatted_sl, "triggerBy": "MarkPrice"},
            "takeProfit": {"triggerPrice": formatted_tp, "triggerBy": "LastPrice"},
        }
        order = await self._safe_call(
            self.exchange.create_order,
            symbol=symbol,
            type="market",
            side=order_side,
            amount=formatted_qty,
            params=params,
        )
        fill_price = float(order.get("average") or order.get("price") or 0.0)
        return formatted_qty, fill_price, formatted_sl, formatted_tp

    async def update_live_stop_loss(self, symbol: str, new_sl: float):
        if self.config.paper_trading:
            return
        try:
            formatted_sl = float(self.exchange.price_to_precision(symbol, new_sl))
            await self._safe_call(
                self.exchange.private_post_v5_position_trading_stop,
                {
                    "category": "linear",
                    "symbol": self.exchange.market_id(symbol),
                    "stopLoss": str(formatted_sl),
                    "slTriggerBy": "MarkPrice",
                    "positionIdx": 0,
                },
            )
        except Exception as e:
            self.log(f"Live SL sync warning on {symbol}: {e}")

    async def close_live_portion(self, symbol: str, side: str, qty: float):
        close_side = "sell" if side == "LONG" else "buy"
        formatted_qty = float(self.exchange.amount_to_precision(symbol, qty))
        if formatted_qty <= 0:
            return
        await self._safe_call(
            self.exchange.create_order,
            symbol=symbol,
            type="market",
            side=close_side,
            amount=formatted_qty,
            params={"reduceOnly": True},
        )

    async def close_single_position(self, symbol: str, reason: str = "MANUAL CLOSE"):
        pos = self.positions.get(symbol)
        if not pos:
            return False
        exit_price = pos.get("current_price") or self.latest_prices.get(symbol) or pos["entry"]
        if not self.config.paper_trading:
            try:
                await self.close_live_portion(symbol, pos["side"], pos["qty"])
                await self._safe_call(self.exchange.cancel_all_orders, symbol)
            except Exception as e:
                self.log(f"Live manual close warning on {symbol}: {e}")

        gross_pnl = (exit_price - pos["entry"]) * pos["qty"] if pos["side"] == "LONG" else (pos["entry"] - exit_price) * pos["qty"]
        fee = (exit_price * pos["qty"]) * self.config.taker_fee
        net_pnl = round(gross_pnl - fee, 2)
        self.balance = round(self.balance + net_pnl, 2)
        if self.config.paper_trading:
            self.db.set_setting("paper_balance", str(self.balance))

        closed = {
            "symbol": symbol,
            "side": pos["side"],
            "entry": pos["entry"],
            "exit": exit_price,
            "qty": pos["qty"],
            "pnl": net_pnl,
            "reason": reason,
            "closed_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        }
        self.db.record_trade(closed, self.balance, mode=self.active_mode)
        self.db.remove_open_position(symbol, mode=self.active_mode)
        del self.positions[symbol]
        self.log(f"CLOSED {pos['side']} {symbol} ({reason}) | Net PnL: ${net_pnl:+,.2f}")
        return True

    async def manage_open_positions(self):
        if not self.positions:
            return
        for symbol in list(self.positions.keys()):
            pos = self.positions[symbol]
            last_px = self.latest_prices.get(symbol)
            if not last_px:
                continue
            mark_px = self.mark_prices.get(symbol, last_px)
            pos["current_price"] = last_px
            pos["mark_price"] = mark_px
            r_dist = pos["risk_dist"]

            if pos["side"] == "LONG":
                if not pos["partial_taken"] and mark_px >= pos["entry"] + (1.2 * r_dist):
                    partial_qty = round(pos["qty"] * 0.5, 4)
                    if not self.config.paper_trading:
                        await self.close_live_portion(symbol, "LONG", partial_qty)
                    partial_pnl = round((last_px - pos["entry"]) * partial_qty - (last_px * partial_qty * self.config.maker_fee), 2)
                    self.balance = round(self.balance + partial_pnl, 2)
                    if self.config.paper_trading:
                        self.db.set_setting("paper_balance", str(self.balance))
                    self.db.record_trade({
                        "symbol": symbol,
                        "side": "LONG (50%)",
                        "entry": pos["entry"],
                        "exit": last_px,
                        "qty": partial_qty,
                        "pnl": partial_pnl,
                        "reason": "PARTIAL +1.2R",
                        "closed_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                    }, self.balance, mode=self.active_mode)
                    pos["qty"] = round(pos["qty"] - partial_qty, 4)
                    pos["sl"] = pos["entry"]
                    pos["partial_taken"] = True
                    pos["be_locked"] = True
                    self.db.upsert_open_position(pos, mode=self.active_mode)
                    await self.update_live_stop_loss(symbol, pos["sl"])
                    self.log(f"PARTIAL TP (+1.2R) | {symbol} banked ${partial_pnl:+,.2f} — Runner trailing!")

                if pos["partial_taken"]:
                    raw_trail = mark_px - (1.5 * r_dist)
                    try:
                        new_trail = float(self.exchange.price_to_precision(symbol, raw_trail))
                    except Exception:
                        new_trail = round(raw_trail, 6)

                    if new_trail > pos["sl"]:
                        pos["sl"] = new_trail
                        self.db.upsert_open_position(pos, mode=self.active_mode)
                        await self.update_live_stop_loss(symbol, pos["sl"])

                unrealized = (mark_px - pos["entry"]) * pos["qty"]
                hit_tp = last_px >= pos["tp"]
                hit_sl = mark_px <= pos["sl"]  # Mark-price stop-loss check
            else:  # SHORT
                if not pos["partial_taken"] and mark_px <= pos["entry"] - (1.2 * r_dist):
                    partial_qty = round(pos["qty"] * 0.5, 4)
                    if not self.config.paper_trading:
                        await self.close_live_portion(symbol, "SHORT", partial_qty)
                    partial_pnl = round((pos["entry"] - last_px) * partial_qty - (last_px * partial_qty * self.config.maker_fee), 2)
                    self.balance = round(self.balance + partial_pnl, 2)
                    if self.config.paper_trading:
                        self.db.set_setting("paper_balance", str(self.balance))
                    self.db.record_trade({
                        "symbol": symbol,
                        "side": "SHORT (50%)",
                        "entry": pos["entry"],
                        "exit": last_px,
                        "qty": partial_qty,
                        "pnl": partial_pnl,
                        "reason": "PARTIAL +1.2R",
                        "closed_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                    }, self.balance, mode=self.active_mode)
                    pos["qty"] = round(pos["qty"] - partial_qty, 4)
                    pos["sl"] = pos["entry"]
                    pos["partial_taken"] = True
                    pos["be_locked"] = True
                    self.db.upsert_open_position(pos, mode=self.active_mode)
                    await self.update_live_stop_loss(symbol, pos["sl"])
                    self.log(f"PARTIAL TP (+1.2R) | {symbol} banked ${partial_pnl:+,.2f} — Runner trailing!")

                if pos["partial_taken"]:
                    raw_trail = mark_px + (1.5 * r_dist)
                    try:
                        new_trail = float(self.exchange.price_to_precision(symbol, raw_trail))
                    except Exception:
                        new_trail = round(raw_trail, 6)

                    if new_trail < pos["sl"]:
                        pos["sl"] = new_trail
                        self.db.upsert_open_position(pos, mode=self.active_mode)
                        await self.update_live_stop_loss(symbol, pos["sl"])

                unrealized = (pos["entry"] - mark_px) * pos["qty"]
                hit_tp = last_px <= pos["tp"]
                hit_sl = mark_px >= pos["sl"]  # Mark-price stop-loss check

            pos["unrealized_pnl"] = round(unrealized, 2)
            if hit_tp or hit_sl:
                exit_reason = "TAKE PROFIT" if hit_tp else ("TRAIL STOP" if pos["partial_taken"] else "MARK STOP LOSS")
                exit_price = pos["tp"] if hit_tp else pos["sl"]
                if not self.config.paper_trading:
                    try:
                        await self.close_live_portion(symbol, pos["side"], pos["qty"])
                        await self._safe_call(self.exchange.cancel_all_orders, symbol)
                    except Exception as e:
                        self.log(f"Live close cleanup note on {symbol}: {e}")

                gross_pnl = (exit_price - pos["entry"]) * pos["qty"] if pos["side"] == "LONG" else (pos["entry"] - exit_price) * pos["qty"]
                fee = (exit_price * pos["qty"]) * self.config.taker_fee
                net_pnl = round(gross_pnl - fee, 2)
                self.balance = round(self.balance + net_pnl, 2)
                if self.config.paper_trading:
                    self.db.set_setting("paper_balance", str(self.balance))

                closed = {
                    "symbol": symbol,
                    "side": pos["side"],
                    "entry": pos["entry"],
                    "exit": exit_price,
                    "qty": pos["qty"],
                    "pnl": net_pnl,
                    "reason": exit_reason,
                    "closed_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                }
                self.db.record_trade(closed, self.balance, mode=self.active_mode)
                self.db.remove_open_position(symbol, mode=self.active_mode)
                del self.positions[symbol]
                self.log(f"CLOSED {pos['side']} {symbol} ({exit_reason}) | Net PnL: ${net_pnl:+,.2f}")
                
    async def scan_markets(self):
        if datetime.now(timezone.utc).second < 3:
            return
        allowed, status = self.risk.can_trade(self.balance, len(self.positions))
        if not allowed:
            now = time.time()
            if now - self._last_block_log >= 60:
                self._last_block_log = now
                self.log(f"SCANNER PAUSED | {status}")
            return

        scanned_count = 0
        for symbol in list(self.config.watchlist):
            pair_cfg = self.config.get_pair_params(symbol)
            if not pair_cfg["enabled"]:
                self.pair_telemetry[symbol] = "paused"
                continue

            try:
                htf_df = await self.fetch_df(symbol, self.config.htf_timeframe, limit=210)
                ltf_df = await self.fetch_df(symbol, self.config.ltf_timeframe, limit=60)
                self._compute_pair_telemetry(symbol, htf_df, ltf_df)
                scanned_count += 1

                if symbol in self.positions or len(self.positions) >= self.config.max_open_positions:
                    continue

                signal = evaluate_smc_setup(symbol, htf_df, ltf_df, pair_cfg["reward_to_risk"])
                if signal:
                    self.pair_telemetry[symbol] = f"entry_ready: {signal.side} SMC setup triggered"
                    qty = self.risk.calculate_position_size(
                        self.balance,
                        signal.entry_price,
                        signal.stop_loss,
                        risk_pct=pair_cfg["risk_per_trade_pct"]
                    )
                    if qty <= 0:
                        continue
                    if self.config.paper_trading:
                        entry_price = signal.entry_price
                        sl_price = signal.stop_loss
                        tp_price = signal.take_profit
                        entry_fee = round((entry_price * qty) * self.config.maker_fee, 2)
                        self.balance = round(self.balance - entry_fee, 2)
                        self.db.set_setting("paper_balance", str(self.balance))
                    else:
                        qty, fill_price, sl_price, tp_price = await self.place_live_entry(
                            symbol, signal.side, qty, signal.stop_loss, signal.take_profit
                        )
                        entry_price = fill_price if fill_price > 0 else signal.entry_price

                    self.positions[symbol] = {
                        "symbol": symbol,
                        "side": signal.side,
                        "entry": entry_price,
                        "current_price": entry_price,
                        "mark_price": entry_price,
                        "sl": sl_price,
                        "tp": tp_price,
                        "qty": qty,
                        "risk_dist": abs(entry_price - sl_price),
                        "unrealized_pnl": 0.0,
                        "be_locked": False,
                        "partial_taken": False,
                        "reason": signal.reason
                    }
                    self.db.upsert_open_position(self.positions[symbol], mode=self.active_mode)
                    self.log(
                        f"OPEN {signal.side} {symbol} @ {entry_price:.4f} | Qty: {qty} | "
                        f"Risk: {pair_cfg['risk_per_trade_pct']*100:.2f}% | R:R: {pair_cfg['reward_to_risk']}R"
                    )
            except Exception as e:
                self.log(f"Scan retry queued for {symbol}: {type(e).__name__}: {e}")
            await asyncio.sleep(0.2)

        if scanned_count > 0 and (time.time() - self._last_scan_log >= 60):
            self._last_scan_log = time.time()
            mode_tag = "PAPER" if self.config.paper_trading else "LIVE"
            self.log(
                f"SCANNER HEARTBEAT ({mode_tag}) | Evaluated 4H/1H SMC setups across "
                f"{scanned_count} pairs — awaiting valid breakout trigger."
            )

    async def run_loop(self):
        if self._loop_active:
            return
        self._loop_active = True
        self.is_running = True
        try:
            await self.load_tradeable_markets()
            if not self.config.paper_trading:
                await self.sync_live_balance()
                self.risk.day_start_equity = self.balance
            mode_label = "PAPER" if self.config.paper_trading else "LIVE MAINNET"
            self.log(
                f"AlphaStrike v4 Engine Started ({mode_label}) | Balance: ${self.balance:,.2f} | "
                f"Watchlist: {len(self.config.watchlist)} pairs"
            )
            while self.is_running:
                try:
                    if not self.config.paper_trading and self.balance <= 0:
                        await self.sync_live_balance()
                        if self.balance > 0:
                            self.risk.day_start_equity = self.balance
                            self.log(f"LIVE DEPOSIT DETECTED | New Unified Balance: ${self.balance:,.2f}")
                    await self.sync_batch_tickers()
                    await self.manage_open_positions()
                    await self.scan_markets()
                except ccxt.NetworkError as e:
                    self.log(f"Transient network hiccup (auto-recovering): {type(e).__name__}")
                except Exception as e:
                    self.log(f"Engine loop error: {type(e).__name__}: {e}")
                    traceback.print_exc()
                await asyncio.sleep(10)
        finally:
            self._loop_active = False

    def emergency_kill(self):
        self.is_running = False
        for symbol, pos in list(self.positions.items()):
            if not self.config.paper_trading:
                asyncio.create_task(self.close_live_portion(symbol, pos["side"], pos["qty"]))
            self.db.remove_open_position(symbol, mode=self.active_mode)
        self.positions.clear()
        self.log("EMERGENCY KILL SWITCH ACTIVATED — Engine halted & positions flattened.")