import asyncio
from contextlib import asynccontextmanager
from typing import List, Optional
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from config import BotConfig
from engine import TradingEngine

config = BotConfig()
engine = TradingEngine(config)


def get_active_mode_key() -> str:
    return "paper" if config.paper_trading else "live"


class SettingsUpdate(BaseModel):
    risk_per_trade: Optional[float] = None
    max_open_positions: Optional[int] = None
    reward_to_risk: Optional[float] = None
    watchlist: Optional[List[str]] = None


class WatchlistAction(BaseModel):
    symbol: str
    action: str  # "add" or "remove"


class PairSettingsUpdate(BaseModel):
    symbol: str
    enabled: Optional[bool] = None
    risk_per_trade: Optional[float] = None
    reward_to_risk: Optional[float] = None
    reset_to_global: bool = False


class ClosePositionRequest(BaseModel):
    symbol: str


class ModeRequest(BaseModel):
    mode: str  # "paper" or "live"


class WalletRequest(BaseModel):
    action: str  # "deposit", "withdraw", or "sync"
    amount: float = 0.0


@asynccontextmanager
async def lifespan(app: FastAPI):
    saved_mode = engine.db.get_setting("trading_mode", "paper")
    if saved_mode not in ("paper", "live"):
        saved_mode = "paper"
    await engine.switch_mode(saved_mode)
    task = asyncio.create_task(engine.run_loop())
    yield
    engine.is_running = False
    task.cancel()


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def get_dashboard():
    with open("static/index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())


@app.get("/api/markets")
async def get_available_markets():
    if not engine.available_markets:
        await engine.load_tradeable_markets()
    return {"markets": engine.available_markets}


@app.post("/api/mode")
async def api_switch_mode(payload: ModeRequest):
    target = payload.mode.strip().lower()
    if target not in ("paper", "live"):
        raise HTTPException(status_code=400, detail="Mode must be 'paper' or 'live'.")
    try:
        label = await engine.switch_mode(target)
        engine.db.set_setting("trading_mode", target)
        if not engine._loop_active:
            asyncio.create_task(engine.run_loop())
        return {"status": "ok", "mode": label, "balance": engine.balance}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/wallet")
async def api_wallet_action(payload: WalletRequest):
    try:
        if payload.action == "sync":
            await engine.sync_live_balance()
            engine.risk.day_start_equity = engine.balance
            engine.log(f"WALLET SYNCED | Current Balance: ${engine.balance:,.2f}")
            return {"status": "ok", "balance": engine.balance}
        new_bal = await engine.adjust_wallet(payload.action, payload.amount)
        return {"status": "ok", "balance": new_bal}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/watchlist")
async def modify_watchlist(payload: WatchlistAction):
    sym = payload.symbol.strip().upper()
    if "/" not in sym:
        sym = f"{sym}/USDT:USDT"
    elif not sym.endswith(":USDT"):
        sym = f"{sym}:USDT"

    valid_symbols = {m["symbol"] for m in engine.available_markets}
    if payload.action == "add":
        if valid_symbols and sym not in valid_symbols:
            raise HTTPException(status_code=400, detail=f"{sym} is not an active Bybit USDT perpetual contract.")
        if sym not in config.watchlist:
            config.watchlist.append(sym)
        if sym in config.pair_overrides:
            config.pair_overrides[sym]["enabled"] = True
        engine.persist_config()
        engine.log(f"WATCHLIST ADDED | {sym} added to active scanner ({len(config.watchlist)} pairs)")
        return {"status": "added", "symbol": sym, "watchlist": config.watchlist}

    elif payload.action == "remove":
        if len(config.watchlist) <= 1 and sym in config.watchlist:
            raise HTTPException(status_code=400, detail="Cannot remove the last remaining watchlist pair.")
        if sym in config.watchlist:
            config.watchlist.remove(sym)
        config.pair_overrides.pop(sym, None)
        engine.pair_telemetry.pop(sym, None)
        engine.persist_config()
        engine.log(f"WATCHLIST REMOVED | {sym} removed from scanner ({len(config.watchlist)} pairs)")
        return {"status": "removed", "symbol": sym, "watchlist": config.watchlist}

    raise HTTPException(status_code=400, detail="Invalid action. Use 'add' or 'remove'.")


@app.post("/api/kill")
async def kill_bot():
    engine.emergency_kill()
    return {"status": "killed"}


@app.post("/api/start")
async def start_bot():
    if not engine._loop_active:
        engine.is_running = True
        asyncio.create_task(engine.run_loop())
    return {"status": "running", "is_running": engine.is_running}


@app.post("/api/settings")
async def update_settings(payload: SettingsUpdate):
    if payload.risk_per_trade is not None:
        clamped_risk = max(0.001, min(0.25, payload.risk_per_trade))
        config.risk_per_trade_pct = clamped_risk
        engine.risk.config.risk_per_trade_pct = clamped_risk
    if payload.max_open_positions is not None:
        clamped_max = max(1, min(20, payload.max_open_positions))
        config.max_open_positions = clamped_max
        engine.risk.config.max_open_positions = clamped_max
    if payload.reward_to_risk is not None:
        config.reward_to_risk = max(1.0, min(10.0, payload.reward_to_risk))
    if payload.watchlist is not None and len(payload.watchlist) > 0:
        config.watchlist = payload.watchlist
        for sym in config.watchlist:
            if sym in config.pair_overrides and "enabled" in config.pair_overrides[sym]:
                config.pair_overrides[sym]["enabled"] = True

    engine.persist_config()
    engine.log(
        f"CUSTOM SETTINGS APPLIED | Risk: {config.risk_per_trade_pct * 100:.2f}% | "
        f"Max Pos: {config.max_open_positions} | R:R: {config.reward_to_risk}R"
    )
    return {"status": "ok"}


@app.post("/api/pair-settings")
async def update_pair_settings(payload: PairSettingsUpdate):
    sym = payload.symbol
    if payload.reset_to_global:
        config.pair_overrides.pop(sym, None)
        if sym not in config.watchlist:
            config.watchlist.append(sym)
        engine.persist_config()
        engine.log(f"PAIR RESET | {sym} restored to global parameters")
        return {"status": "reset", "symbol": sym}

    ov = config.pair_overrides.setdefault(sym, {})
    if payload.enabled is not None:
        ov["enabled"] = payload.enabled
    if payload.risk_per_trade is not None:
        ov["risk_per_trade_pct"] = max(0.001, min(0.25, payload.risk_per_trade))
    if payload.reward_to_risk is not None:
        ov["reward_to_risk"] = max(1.0, min(10.0, payload.reward_to_risk))

    engine.persist_config()
    params = config.get_pair_params(sym)
    engine.log(
        f"PAIR UPDATED | {sym} -> {'ACTIVE' if params['enabled'] else 'PAUSED'} | "
        f"Risk: {params['risk_per_trade_pct']*100:.2f}% | Target: {params['reward_to_risk']}R"
    )
    return {"status": "ok", "symbol": sym, "params": params}


@app.post("/api/close-position")
async def close_single_position(payload: ClosePositionRequest):
    closed = await engine.close_single_position(payload.symbol, reason="MANUAL PAIR CLOSE")
    return {"status": "closed" if closed else "not_found", "symbol": payload.symbol}


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            mode_key = get_active_mode_key()
            unrealized_total = sum(p["unrealized_pnl"] for p in engine.positions.values())
            mode_label = "PAPER TRADING" if config.paper_trading else "LIVE MAINNET"
            pair_settings = {sym: config.get_pair_params(sym) for sym in config.watchlist}
            raw_trades = engine.db.get_recent_trades(60, mode=mode_key)
            clean_trades = [t for t in raw_trades if t.get("symbol") != "WALLET/USDT"]
            payload = {
                "is_running": engine.is_running,
                "mode": mode_label,
                "balance": round(engine.balance, 2),
                "equity": round(engine.balance + unrealized_total, 2),
                "unrealized_pnl": round(unrealized_total, 2),
                "stats": engine.db.get_performance_stats(mode=mode_key),
                "equity_curve": engine.db.get_equity_curve(mode=mode_key),
                "positions": list(engine.positions.values()),
                "history": clean_trades,
                "wallet_history": engine.get_wallet_history(mode=mode_key),
                "latest_prices": engine.latest_prices,
                "mark_prices": engine.mark_prices,
                "funding_rates": engine.funding_rates,
                "price_history": engine.price_history,
                "pair_telemetry": engine.pair_telemetry,
                "market_count": len(engine.available_markets),
                "settings": {
                    "risk_per_trade": config.risk_per_trade_pct,
                    "max_open_positions": config.max_open_positions,
                    "reward_to_risk": config.reward_to_risk,
                    "watchlist": config.watchlist,
                },
                "pair_settings": pair_settings,
            }
            await ws.send_json(payload)
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        pass