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
        conn = sqlite3.connect(self.db_path, timeout=15.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    def _init_tables(self):
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)
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
                    closed_at TEXT NOT NULL,
                    mode TEXT DEFAULT 'paper'
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS equity_curve (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    equity REAL NOT NULL,
                    mode TEXT DEFAULT 'paper'
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS open_positions (
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    entry REAL NOT NULL,
                    sl REAL NOT NULL,
                    tp REAL NOT NULL,
                    qty REAL NOT NULL,
                    risk_dist REAL NOT NULL,
                    be_locked INTEGER NOT NULL,
                    partial_taken INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    mode TEXT DEFAULT 'paper',
                    PRIMARY KEY (symbol, mode)
                )
            """)

            # Safe schema migration for existing alphastrike.db files
            trade_cols = [r["name"] for r in conn.execute("PRAGMA table_info(trades)").fetchall()]
            if "mode" not in trade_cols:
                conn.execute("ALTER TABLE trades ADD COLUMN mode TEXT DEFAULT 'paper'")

            eq_cols = [r["name"] for r in conn.execute("PRAGMA table_info(equity_curve)").fetchall()]
            if "mode" not in eq_cols:
                conn.execute("ALTER TABLE equity_curve ADD COLUMN mode TEXT DEFAULT 'paper'")

            pos_cols = [r["name"] for r in conn.execute("PRAGMA table_info(open_positions)").fetchall()]
            if "mode" not in pos_cols:
                conn.execute("ALTER TABLE open_positions ADD COLUMN mode TEXT DEFAULT 'paper'")

            # Default to 'paper' on first boot if not previously set; migrate any old 'testnet' setting to 'live'
            conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('trading_mode', 'paper')")
            conn.execute("UPDATE settings SET value = 'live' WHERE key = 'trading_mode' AND value = 'testnet'")

            for m in ("paper", "live"):
                count = conn.execute(
                    "SELECT COUNT(*) FROM equity_curve WHERE mode = ?", (m,)
                ).fetchone()[0]
                if count == 0:
                    now = datetime.now(timezone.utc).strftime("%m-%d %H:%M")
                    start_eq = round(self.initial_balance, 2) if m == "paper" else 0.0
                    conn.execute(
                        "INSERT INTO equity_curve (timestamp, equity, mode) VALUES (?, ?, ?)",
                        (now, start_eq, m),
                    )

    def get_setting(self, key: str, default: str = None):
        with self._connect() as conn:
            row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
            return row["value"] if row else default

    def set_setting(self, key: str, value: str):
        with self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                (key, str(value)),
            )

    def get_current_balance(self, mode: str = "paper") -> float:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT SUM(pnl) AS total_pnl FROM trades WHERE mode = ?", (mode,)
            ).fetchone()
            total_pnl = row["total_pnl"] if row["total_pnl"] is not None else 0.0
            base = self.initial_balance if mode == "paper" else 0.0
            return round(base + total_pnl, 2)

    def upsert_open_position(self, pos: Dict, mode: str = "paper"):
        with self._connect() as conn:
            conn.execute("""
                INSERT INTO open_positions (symbol, side, entry, sl, tp, qty, risk_dist, be_locked, partial_taken, reason, mode)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, mode) DO UPDATE SET
                    sl=excluded.sl,
                    qty=excluded.qty,
                    be_locked=excluded.be_locked,
                    partial_taken=excluded.partial_taken
            """, (
                pos["symbol"], pos["side"], pos["entry"], pos["sl"], pos["tp"],
                pos["qty"], pos["risk_dist"], int(pos["be_locked"]),
                int(pos["partial_taken"]), pos["reason"], mode
            ))

    def remove_open_position(self, symbol: str, mode: str = "paper"):
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM open_positions WHERE symbol = ? AND mode = ?",
                (symbol, mode),
            )

    def load_open_positions(self, mode: str = "paper") -> Dict[str, Dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM open_positions WHERE mode = ?", (mode,)
            ).fetchall()
            positions = {}
            for r in rows:
                positions[r["symbol"]] = {
                    "symbol": r["symbol"],
                    "side": r["side"],
                    "entry": r["entry"],
                    "current_price": r["entry"],
                    "mark_price": r["entry"],
                    "sl": r["sl"],
                    "tp": r["tp"],
                    "qty": r["qty"],
                    "risk_dist": r["risk_dist"],
                    "unrealized_pnl": 0.0,
                    "be_locked": bool(r["be_locked"]),
                    "partial_taken": bool(r["partial_taken"]),
                    "reason": r["reason"]
                }
            return positions

    def record_trade(self, trade: Dict, new_balance: float, mode: str = "paper"):
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO trades (symbol, side, entry, exit, qty, pnl, reason, closed_at, mode)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    mode,
                ),
            )
            now = datetime.now(timezone.utc).strftime("%m-%d %H:%M")
            conn.execute(
                "INSERT INTO equity_curve (timestamp, equity, mode) VALUES (?, ?, ?)",
                (now, round(new_balance, 2), mode),
            )

    def get_recent_trades(self, limit: int = 25, mode: str = "paper") -> List[Dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM trades WHERE mode = ? ORDER BY id DESC LIMIT ?",
                (mode, limit),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_equity_curve(self, limit: int = 60, mode: str = "paper") -> List[Dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT timestamp, equity FROM equity_curve WHERE mode = ? ORDER BY id DESC LIMIT ?",
                (mode, limit),
            ).fetchall()
            return [dict(r) for r in reversed(rows)]

    def get_performance_stats(self, mode: str = "paper") -> Dict:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT pnl FROM trades WHERE mode = ?", (mode,)
            ).fetchall()
            if not rows:
                return {
                    "total_trades": 0,
                    "win_rate": 0.0,
                    "net_pnl": 0.0,
                    "total_pnl": 0.0,
                    "profit_factor": 0.0,
                    "wins": 0,
                    "losses": 0,
                }
            pnls = [float(r["pnl"]) for r in rows]
            wins = [p for p in pnls if p > 0]
            losses = [p for p in pnls if p <= 0]
            gross_profit = sum(wins)
            gross_loss = abs(sum(losses))
            pf = (gross_profit / gross_loss) if gross_loss > 0 else (gross_profit if gross_profit > 0 else 0.0)
            net = round(sum(pnls), 2)
            return {
                "total_trades": len(pnls),
                "win_rate": round((len(wins) / len(pnls)) * 100, 1),
                "net_pnl": net,
                "total_pnl": net,
                "profit_factor": round(pf, 2),
                "wins": len(wins),
                "losses": len(losses),
            }