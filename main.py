import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from config import BotConfig
from engine import TradingEngine

config = BotConfig()
engine = TradingEngine(config)


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


@app.post("/api/kill")
async def kill_bot():
    engine.emergency_kill()
    return {"status": "killed"}


@app.post("/api/start")
async def start_bot():
    if not engine.is_running:
        asyncio.create_task(engine.run_loop())
    return {"status": "running"}


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
            payload = {
                "is_running": engine.is_running,
                "mode": mode_label,
                "balance": round(engine.balance, 2),
                "equity": round(engine.balance + unrealized_total, 2),
                "unrealized_pnl": round(unrealized_total, 2),
                "stats": engine.db.get_performance_stats(),
                "equity_curve": engine.db.get_equity_curve(),
                "positions": list(engine.positions.values()),
                "history": engine.db.get_recent_trades(20),
                "logs": engine.logs[:25],
            }
            await ws.send_json(payload)
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        pass