import asyncio
import time
import ccxt.async_support as ccxt
import pandas as pd
from datetime import datetime, timezone
from strategy import evaluate_smc_setup
from risk_manager import RiskManager
from database import TradeDatabase


class TradingEngine:
    def __init__(self, config):
        self.config = config
        exchange_params = {
            "enableRateLimit": True,
            "rateLimit": 150,
            "timeout": 30000,
            "options": {"defaultType": "swap"}
        }
        if not config.paper_trading:
            if not config.api_key or not config.api_secret:
                raise ValueError("Missing BYBIT_API_KEY or BYBIT_API_SECRET in .env for live trading!")
            exchange_params["apiKey"] = config.api_key
            exchange_params["secret"] = config.api_secret

        self.exchange = getattr(ccxt, config.exchange_id)(exchange_params)
        if not config.paper_trading and config.use_testnet:
            self.exchange.set_sandbox_mode(True)

        self.db = TradeDatabase(config.initial_balance)
        self.balance = self.db.get_current_balance()
        self.risk = RiskManager(config)
        self.risk.day_start_equity = self.balance
        self.positions = self.db.load_open_positions()
        self.latest_prices = {}
        self.price_history = {}
        self.available_markets = []
        self.logs = []
        self.is_running = False
        self._loop_active = False
        self._ohlcv_cache = {}

    def record_price_tick(self, symbol: str, price: float):
        self.latest_prices[symbol] = price
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
            markets = await self.exchange.load_markets()
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

    async def sync_live_balance(self):
        if self.config.paper_trading:
            return
        try:
            bal = await self.exchange.fetch_balance()
            usdt_total = bal.get("USDT", {}).get("total")
            if usdt_total is not None:
                self.balance = round(float(usdt_total), 2)
        except Exception as e:
            self.log(f"Balance sync warning: {e}")

    async def fetch_df(self, symbol: str, timeframe: str, limit: int = 210) -> pd.DataFrame:
        ttl = 300 if timeframe == "4h" else 60
        cache_key = (symbol, timeframe)
        now = time.time()

        if cache_key in self._ohlcv_cache:
            cached_at, cached_df = self._ohlcv_cache[cache_key]
            if now - cached_at < ttl:
                if timeframe == "1h" and not cached_df.empty:
                    self.record_price_tick(symbol, float(cached_df["close"].iloc[-1]))
                return cached_df

        ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
        self._ohlcv_cache[cache_key] = (now, df)

        if timeframe == "1h" and not df.empty:
            if symbol not in self.price_history or len(self.price_history[symbol]) < 5:
                self.price_history[symbol] = [float(x) for x in df["close"].tail(30).tolist()]
            self.record_price_tick(symbol, float(df["close"].iloc[-1]))

        await asyncio.sleep(0.35)
        return df

    async def place_live_entry(self, symbol: str, side: str, qty: float, sl: float, tp: float):
        order_side = "buy" if side == "LONG" else "sell"
        formatted_qty = float(self.exchange.amount_to_precision(symbol, qty))
        formatted_sl = float(self.exchange.price_to_precision(symbol, sl))
        formatted_tp = float(self.exchange.price_to_precision(symbol, tp))

        params = {
            "stopLoss": {"triggerPrice": formatted_sl},
            "takeProfit": {"triggerPrice": formatted_tp},
        }
        order = await self.exchange.create_order(
            symbol=symbol,
            type="market",
            side=order_side,
            amount=formatted_qty,
            params=params
        )
        fill_price = float(order.get("average") or order.get("price") or 0.0)
        return formatted_qty, fill_price, formatted_sl, formatted_tp

    async def update_live_stop_loss(self, symbol: str, new_sl: float):
        if self.config.paper_trading:
            return
        try:
            formatted_sl = float(self.exchange.price_to_precision(symbol, new_sl))
            await self.exchange.private_post_v5_position_trading_stop({
                "category": "linear",
                "symbol": self.exchange.market_id(symbol),
                "stopLoss": str(formatted_sl),
                "positionIdx": 0
            })
        except Exception as e:
            self.log(f"Live SL sync warning on {symbol}: {e}")

    async def close_live_portion(self, symbol: str, side: str, qty: float):
        close_side = "sell" if side == "LONG" else "buy"
        formatted_qty = float(self.exchange.amount_to_precision(symbol, qty))
        if formatted_qty <= 0:
            return
        await self.exchange.create_order(
            symbol=symbol,
            type="market",
            side=close_side,
            amount=formatted_qty,
            params={"reduceOnly": True}
        )

    async def close_single_position(self, symbol: str, reason: str = "MANUAL CLOSE"):
        pos = self.positions.get(symbol)
        if not pos:
            return False

        exit_price = pos.get("current_price") or self.latest_prices.get(symbol) or pos["entry"]
        if not self.config.paper_trading:
            try:
                await self.close_live_portion(symbol, pos["side"], pos["qty"])
                await self.exchange.cancel_all_orders(symbol)
            except Exception as e:
                self.log(f"Live manual close warning on {symbol}: {e}")

        gross_pnl = (exit_price - pos["entry"]) * pos["qty"] if pos["side"] == "LONG" else (pos["entry"] - exit_price) * pos["qty"]
        fee = (exit_price * pos["qty"]) * self.config.taker_fee
        net_pnl = round(gross_pnl - fee, 2)

        self.balance = round(self.balance + net_pnl, 2)
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
        self.db.record_trade(closed, self.balance)
        self.db.remove_open_position(symbol)
        del self.positions[symbol]
        self.log(f"CLOSED {pos['side']} {symbol} ({reason}) | Net PnL: ${net_pnl:+,.2f}")
        return True

    async def manage_open_positions(self):
        if not self.positions:
            return

        for symbol in list(self.positions.keys()):
            pos = self.positions[symbol]
            ticker = await self.exchange.fetch_ticker(symbol)
            price = float(ticker["last"])
            self.record_price_tick(symbol, price)
            pos["current_price"] = price
            r_dist = pos["risk_dist"]

            if pos["side"] == "LONG":
                if not pos["partial_taken"] and price >= pos["entry"] + (1.2 * r_dist):
                    partial_qty = round(pos["qty"] * 0.5, 4)
                    if not self.config.paper_trading:
                        await self.close_live_portion(symbol, "LONG", partial_qty)
                    partial_pnl = round((price - pos["entry"]) * partial_qty - (price * partial_qty * self.config.maker_fee), 2)
                    self.balance = round(self.balance + partial_pnl, 2)
                    self.db.record_trade({
                        "symbol": symbol,
                        "side": "LONG (50%)",
                        "entry": pos["entry"],
                        "exit": price,
                        "qty": partial_qty,
                        "pnl": partial_pnl,
                        "reason": "PARTIAL +1.2R",
                        "closed_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                    }, self.balance)
                    pos["qty"] = round(pos["qty"] - partial_qty, 4)
                    pos["sl"] = pos["entry"]
                    pos["partial_taken"] = True
                    pos["be_locked"] = True
                    self.db.upsert_open_position(pos)
                    await self.update_live_stop_loss(symbol, pos["sl"])
                    self.log(f"PARTIAL TP (+1.2R) | {symbol} banked ${partial_pnl:+,.2f} — Runner trailing!")

                if pos["partial_taken"]:
                    new_trail = round(price - (1.5 * r_dist), 4)
                    if new_trail > pos["sl"]:
                        pos["sl"] = new_trail
                        self.db.upsert_open_position(pos)
                        await self.update_live_stop_loss(symbol, pos["sl"])

                unrealized = (price - pos["entry"]) * pos["qty"]
                hit_tp = price >= pos["tp"]
                hit_sl = price <= pos["sl"]

            else:  # SHORT
                if not pos["partial_taken"] and price <= pos["entry"] - (1.2 * r_dist):
                    partial_qty = round(pos["qty"] * 0.5, 4)
                    if not self.config.paper_trading:
                        await self.close_live_portion(symbol, "SHORT", partial_qty)
                    partial_pnl = round((pos["entry"] - price) * partial_qty - (price * partial_qty * self.config.maker_fee), 2)
                    self.balance = round(self.balance + partial_pnl, 2)
                    self.db.record_trade({
                        "symbol": symbol,
                        "side": "SHORT (50%)",
                        "entry": pos["entry"],
                        "exit": price,
                        "qty": partial_qty,
                        "pnl": partial_pnl,
                        "reason": "PARTIAL +1.2R",
                        "closed_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                    }, self.balance)
                    pos["qty"] = round(pos["qty"] - partial_qty, 4)
                    pos["sl"] = pos["entry"]
                    pos["partial_taken"] = True
                    pos["be_locked"] = True
                    self.db.upsert_open_position(pos)
                    await self.update_live_stop_loss(symbol, pos["sl"])
                    self.log(f"PARTIAL TP (+1.2R) | {symbol} banked ${partial_pnl:+,.2f} — Runner trailing!")

                if pos["partial_taken"]:
                    new_trail = round(price + (1.5 * r_dist), 4)
                    if new_trail < pos["sl"]:
                        pos["sl"] = new_trail
                        self.db.upsert_open_position(pos)
                        await self.update_live_stop_loss(symbol, pos["sl"])

                unrealized = (pos["entry"] - price) * pos["qty"]
                hit_tp = price <= pos["tp"]
                hit_sl = price >= pos["sl"]

            pos["unrealized_pnl"] = round(unrealized, 2)

            if hit_tp or hit_sl:
                exit_reason = "TAKE PROFIT" if hit_tp else ("TRAIL STOP" if pos["partial_taken"] else "STOP LOSS")
                exit_price = pos["tp"] if hit_tp else pos["sl"]
                if not self.config.paper_trading:
                    try:
                        await self.close_live_portion(symbol, pos["side"], pos["qty"])
                        await self.exchange.cancel_all_orders(symbol)
                    except Exception as e:
                        self.log(f"Live close cleanup note on {symbol}: {e}")

                gross_pnl = (exit_price - pos["entry"]) * pos["qty"] if pos["side"] == "LONG" else (pos["entry"] - exit_price) * pos["qty"]
                fee = (exit_price * pos["qty"]) * self.config.taker_fee
                net_pnl = round(gross_pnl - fee, 2)

                self.balance = round(self.balance + net_pnl, 2)
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
                self.db.record_trade(closed, self.balance)
                self.db.remove_open_position(symbol)
                del self.positions[symbol]
                self.log(f"CLOSED {pos['side']} {symbol} ({exit_reason}) | Net PnL: ${net_pnl:+,.2f}")

            await asyncio.sleep(0.15)

    async def scan_markets(self):
        if datetime.now(timezone.utc).second < 3:
            return

        allowed, status = self.risk.can_trade(self.balance, len(self.positions))
        if not allowed:
            return

        for symbol in list(self.config.watchlist):
            pair_cfg = self.config.get_pair_params(symbol)
            if not pair_cfg["enabled"]:
                continue
            if symbol in self.positions or len(self.positions) >= self.config.max_open_positions:
                continue
            try:
                htf_df = await self.fetch_df(symbol, self.config.htf_timeframe, limit=210)
                ltf_df = await self.fetch_df(symbol, self.config.ltf_timeframe, limit=60)
                signal = evaluate_smc_setup(symbol, htf_df, ltf_df, pair_cfg["reward_to_risk"])

                if signal:
                    stop_distance = abs(signal.entry_price - signal.stop_loss)
                    if stop_distance <= 0:
                        continue
                    risk_dollars = self.balance * pair_cfg["risk_per_trade_pct"]
                    qty = round(risk_dollars / stop_distance, 4)
                    if qty <= 0:
                        continue

                    if self.config.paper_trading:
                        entry_price = signal.entry_price
                        sl_price = signal.stop_loss
                        tp_price = signal.take_profit
                        entry_fee = round((entry_price * qty) * self.config.maker_fee, 2)
                        self.balance = round(self.balance - entry_fee, 2)
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
                        "sl": sl_price,
                        "tp": tp_price,
                        "qty": qty,
                        "risk_dist": abs(entry_price - sl_price),
                        "unrealized_pnl": 0.0,
                        "be_locked": False,
                        "partial_taken": False,
                        "reason": signal.reason
                    }
                    self.db.upsert_open_position(self.positions[symbol])
                    self.log(
                        f"OPEN {signal.side} {symbol} @ {entry_price:.4f} | Qty: {qty} | "
                        f"Risk: {pair_cfg['risk_per_trade_pct']*100:.2f}% | R:R: {pair_cfg['reward_to_risk']}R"
                    )
            except Exception as e:
                self.log(f"Scan retry queued for {symbol}: {type(e).__name__}")
            await asyncio.sleep(0.5)

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

            mode_label = "PAPER" if self.config.paper_trading else ("TESTNET" if self.config.use_testnet else "LIVE MAINNET")
            self.log(f"AlphaStrike v4 Engine Started ({mode_label}) | Balance: ${self.balance:,.2f} | Watchlist: {len(self.config.watchlist)} pairs")

            while self.is_running:
                try:
                    await self.manage_open_positions()
                    await self.scan_markets()
                except Exception as e:
                    self.log(f"Transient network hiccup (auto-recovering): {type(e).__name__}")
                await asyncio.sleep(15)
        finally:
            self._loop_active = False

    def emergency_kill(self):
        self.is_running = False
        for symbol, pos in list(self.positions.items()):
            if not self.config.paper_trading:
                asyncio.create_task(self.close_live_portion(symbol, pos["side"], pos["qty"]))
            self.db.remove_open_position(symbol)
        self.positions.clear()
        self.log("EMERGENCY KILL SWITCH ACTIVATED — Engine halted & positions flattened.")