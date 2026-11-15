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


@asynccontextmanager
async def lifespan(app: FastAPI):
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
        engine.log(f"WATCHLIST ADDED | {sym} added to active scanner ({len(config.watchlist)} pairs)")
        return {"status": "added", "symbol": sym, "watchlist": config.watchlist}

    elif payload.action == "remove":
        if len(config.watchlist) <= 1 and sym in config.watchlist:
            raise HTTPException(status_code=400, detail="Cannot remove the last remaining watchlist pair.")
        if sym in config.watchlist:
            config.watchlist.remove(sym)
        config.pair_overrides.pop(sym, None)
        engine.log(f"WATCHLIST REMOVED | {sym} removed from scanner ({len(config.watchlist)} pairs)")
        return {"status": "removed", "symbol": sym, "watchlist": config.watchlist}

    raise HTTPException(status_code=400, detail="Invalid action. Use 'add' or 'remove'.")


@app.post("/api/kill")
async def kill_bot():
    engine.emergency_kill()
    return {"status": "killed"}


@app.post("/api/start")
async def start_bot():
    if not engine.is_running:
        asyncio.create_task(engine.run_loop())
    return {"status": "running"}


@app.post("/api/settings")
async def update_settings(payload: SettingsUpdate):
    if payload.risk_per_trade is not None:
        clamped_risk = max(0.001, min(0.05, payload.risk_per_trade))
        config.risk_per_trade_pct = clamped_risk
        engine.risk.config.risk_per_trade_pct = clamped_risk

    if payload.max_open_positions is not None:
        clamped_max = max(1, min(10, payload.max_open_positions))
        config.max_open_positions = clamped_max
        engine.risk.config.max_open_positions = clamped_max

    if payload.reward_to_risk is not None:
        config.reward_to_risk = max(1.0, min(5.0, payload.reward_to_risk))

    if payload.watchlist is not None and len(payload.watchlist) > 0:
        config.watchlist = payload.watchlist
        for sym in config.watchlist:
            if sym in config.pair_overrides and "enabled" in config.pair_overrides[sym]:
                config.pair_overrides[sym]["enabled"] = True

    engine.log(
        f"GLOBAL SETTINGS | Risk: {config.risk_per_trade_pct * 100:.2f}% | "
        f"Max Pos: {config.max_open_positions} | R:R: {config.reward_to_risk}R | "
        f"Active Pairs: {len(config.watchlist)}"
    )
    return {"status": "ok"}


@app.post("/api/pair-settings")
async def update_pair_settings(payload: PairSettingsUpdate):
    sym = payload.symbol
    if payload.reset_to_global:
        config.pair_overrides.pop(sym, None)
        if sym not in config.watchlist:
            config.watchlist.append(sym)
        engine.log(f"PAIR RESET | {sym} restored to global parameters")
        return {"status": "reset", "symbol": sym}

    ov = config.pair_overrides.setdefault(sym, {})
    if payload.enabled is not None:
        ov["enabled"] = payload.enabled

    if payload.risk_per_trade is not None:
        ov["risk_per_trade_pct"] = max(0.001, min(0.05, payload.risk_per_trade))

    if payload.reward_to_risk is not None:
        ov["reward_to_risk"] = max(1.0, min(5.0, payload.reward_to_risk))

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
            unrealized_total = sum(p["unrealized_pnl"] for p in engine.positions.values())
            mode_label = (
                "PAPER TRADING"
                if config.paper_trading
                else ("BYBIT TESTNET" if config.use_testnet else "LIVE MAINNET")
            )
            pair_settings = {sym: config.get_pair_params(sym) for sym in config.watchlist}
            payload = {
                "is_running": engine.is_running,
                "mode": mode_label,
                "balance": round(engine.balance, 2),
                "equity": round(engine.balance + unrealized_total, 2),
                "unrealized_pnl": round(unrealized_total, 2),
                "stats": engine.db.get_performance_stats(),
                "equity_curve": engine.db.get_equity_curve(),
                "positions": list(engine.positions.values()),
                "history": engine.db.get_recent_trades(50),
                "logs": engine.logs[:25],
                "latest_prices": engine.latest_prices,
                "price_history": engine.price_history,
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