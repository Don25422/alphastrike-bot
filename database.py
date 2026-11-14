import sqlite3
from datetime import datetime, timezone
from typing import List, Dict

DB_PATH = "alphastrike.db"


class TradeDatabase:
    def __init__(self, initial_balance: float, db_path: str = DB_PATH):
        self.db_path = db_path
        self.initial_balance = initial_balance
        self._init_tables()

    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_tables(self):
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    entry REAL NOT NULL,
                    exit REAL NOT NULL,
                    qty REAL NOT NULL,
                    pnl REAL NOT NULL,
                    reason TEXT NOT NULL,
                    closed_at TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS equity_curve (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    equity REAL NOT NULL
                )
            """)
            count = conn.execute("SELECT COUNT(*) FROM equity_curve").fetchone()[0]
            if count == 0:
                now = datetime.now(timezone.utc).strftime("%m-%d %H:%M")
                conn.execute(
                    "INSERT INTO equity_curve (timestamp, equity) VALUES (?, ?)",
                    (now, round(self.initial_balance, 2))
                )

    def get_current_balance(self) -> float:
        with self._connect() as conn:
            row = conn.execute("SELECT SUM(pnl) AS total_pnl FROM trades").fetchone()
            total_pnl = row["total_pnl"] if row["total_pnl"] is not None else 0.0
            return round(self.initial_balance + total_pnl, 2)

    def record_trade(self, trade: Dict, new_balance: float):
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO trades (symbol, side, entry, exit, qty, pnl, reason, closed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trade["symbol"],
                    trade["side"],
                    trade["entry"],
                    trade["exit"],
                    trade["qty"],
                    trade["pnl"],
                    trade["reason"],
                    trade["closed_at"],
                ),
            )
            now = datetime.now(timezone.utc).strftime("%m-%d %H:%M")
            conn.execute(
                "INSERT INTO equity_curve (timestamp, equity) VALUES (?, ?)",
                (now, round(new_balance, 2)),
            )

    def get_recent_trades(self, limit: int = 25) -> List[Dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]

    def get_equity_curve(self, limit: int = 60) -> List[Dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT timestamp, equity FROM equity_curve ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(r) for r in reversed(rows)]

    def get_performance_stats(self) -> Dict:
        with self._connect() as conn:
            rows = conn.execute("SELECT pnl FROM trades").fetchall()
            if not rows:
                return {"total_trades": 0, "win_rate": 0.0, "net_pnl": 0.0}
            pnls = [r["pnl"] for r in rows]
            wins = sum(1 for p in pnls if p > 0)
            return {
                "total_trades": len(pnls),
                "win_rate": round((wins / len(pnls)) * 100, 1),
                "net_pnl": round(sum(pnls), 2),
            }