import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import Optional

@dataclass
class TradeSignal:
    symbol: str
    side: str            # "LONG" or "SHORT"
    entry_price: float
    stop_loss: float
    take_profit: float
    reason: str

def compute_adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
    up_move = df["high"].diff()
    down_move = -df["low"].diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - df["close"].shift()).abs(),
        (df["low"] - df["close"].shift()).abs()
    ], axis=1).max(axis=1)

    atr = tr.ewm(alpha=1 / period, adjust=False).mean()
    plus_di = 100 * pd.Series(plus_dm, index=df.index).ewm(alpha=1 / period, adjust=False).mean() / atr
    minus_di = 100 * pd.Series(minus_dm, index=df.index).ewm(alpha=1 / period, adjust=False).mean() / atr
    dx = (100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan))
    return dx.ewm(alpha=1 / period, adjust=False).mean()

def prepare_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ema_50"] = df["close"].ewm(span=50, adjust=False).mean()
    df["ema_200"] = df["close"].ewm(span=200, adjust=False).mean()
    df["adx"] = compute_adx(df, 14)

    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - df["close"].shift()).abs(),
        (df["low"] - df["close"].shift()).abs()
    ], axis=1).max(axis=1)
    df["atr"] = tr.rolling(14).mean()

    # 24-hour Donchian Breakout Channels (prior 24 closed candles)
    df["donchian_high"] = df["high"].shift(1).rolling(24).max()
    df["donchian_low"] = df["low"].shift(1).rolling(24).min()
    df["vol_ma"] = df["volume"].rolling(24).mean()
    return df

def evaluate_smc_setup(
    symbol: str,
    htf_df: pd.DataFrame,
    ltf_df: pd.DataFrame,
    rr_ratio: float = 2.5
) -> Optional[TradeSignal]:
    """
    Evaluates market data using a multi-timeframe approach with scoring confluence
    and structural risk management.
    """
    if len(ltf_df) < 40 or len(htf_df) < 200:
        return None

    htf = prepare_indicators(htf_df)
    ltf = prepare_indicators(ltf_df)

    htf_last = htf.iloc[-1]
    c_prev = ltf.iloc[-2]  # Closed candle for signal evaluation
    c_curr = ltf.iloc[-1]  # Current live candle for execution price

    # Macro trend filters
    htf_bullish = (htf_last["close"] > htf_last["ema_50"]) and (htf_last["ema_50"] > htf_last["ema_200"])
    htf_bearish = (htf_last["close"] < htf_last["ema_50"]) and (htf_last["ema_50"] < htf_last["ema_200"])
    
    atr = c_prev["atr"]
    if pd.isna(atr) or atr <= 0:
        return None

    # ==========================
    # BULLISH SETUP (LONG)
    # ==========================
    if htf_bullish and c_prev["adx"] > 20:
        score = 0
        if c_prev["close"] > c_prev["donchian_high"]: score += 2
        if c_prev["close"] > c_prev["open"]: score += 1
        if c_prev["volume"] > (c_prev["vol_ma"] * 1.2): score += 1
        if c_prev["adx"] > 25: score += 1

        # Requires accumulated confluence score >= 3 out of 5
        if score >= 3:
            entry = float(c_curr["close"])
            # Combine ATR volatility buffer with local swing low market structure
            local_support = ltf["low"].iloc[-4:-1].min()
            sl = float(min(entry - (1.5 * atr), local_support - (0.2 * atr)))
            
            risk_dist = entry - sl
            if risk_dist <= 0:
                return None
                
            tp = float(entry + (risk_dist * rr_ratio))
            
            return TradeSignal(
                symbol=symbol,
                side="LONG",
                entry_price=entry,
                stop_loss=sl,
                take_profit=tp,
                reason=f"Smart Breakout LONG (Score {score}/5, ADX {c_prev['adx']:.1f})"
            )

    # ==========================
    # BEARISH SETUP (SHORT)
    # ==========================
    if htf_bearish and c_prev["adx"] > 20:
        score = 0
        if c_prev["close"] < c_prev["donchian_low"]: score += 2
        if c_prev["close"] < c_prev["open"]: score += 1
        if c_prev["volume"] > (c_prev["vol_ma"] * 1.2): score += 1
        if c_prev["adx"] > 25: score += 1

        if score >= 3:
            entry = float(c_curr["close"])
            # Combine ATR volatility buffer with local swing high market structure
            local_resistance = ltf["high"].iloc[-4:-1].max()
            sl = float(max(entry + (1.5 * atr), local_resistance + (0.2 * atr)))
            
            risk_dist = sl - entry
            if risk_dist <= 0:
                return None
                
            tp = float(entry - (risk_dist * rr_ratio))
            
            return TradeSignal(
                symbol=symbol,
                side="SHORT",
                entry_price=entry,
                stop_loss=sl,
                take_profit=tp,
                reason=f"Smart Breakdown SHORT (Score {score}/5, ADX {c_prev['adx']:.1f})"
            )

    return None

# Backwards compatibility alias
evaluate_smart_breakout = evaluate_smc_setup