import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import Optional

@dataclass
class TradeSignal:
    symbol: str
    side: str          # "LONG" or "SHORT"
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
    df["ema_50"] = df["close"].ewm(span=50, adjust=False).mean()
    df["ema_200"] = df["close"].ewm(span=200, adjust=False).mean()
    df["adx"] = compute_adx(df, 14)

    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - df["close"].shift()).abs(),
        (df["low"] - df["close"].shift()).abs()
    ], axis=1).max(axis=1)
    df["atr"] = tr.rolling(14).mean()

    # 24-hour Donchian Breakout Channels (prior 24 closed 1h candles)
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
    htf = prepare_indicators(htf_df.copy())
    ltf = prepare_indicators(ltf_df.copy())

    if len(ltf) < 40 or len(htf) < 200:
        return None

    htf_last = htf.iloc[-1]
    c_prev = ltf.iloc[-2]  # Closed breakout candle
    c_curr = ltf.iloc[-1]

    # Macro 4H alignment + ADX > 22 ensures market is trending, not stuck in sideways chop
    htf_bullish = (htf_last["close"] > htf_last["ema_50"]) and (htf_last["ema_50"] > htf_last["ema_200"])
    htf_bearish = (htf_last["close"] < htf_last["ema_50"]) and (htf_last["ema_50"] < htf_last["ema_200"])
    strong_trend = c_prev["adx"] > 22

    atr = c_prev["atr"]
    if pd.isna(atr) or atr <= 0 or not strong_trend:
        return None

    # BULLISH MOMENTUM BREAKOUT:
    # 1H candle closes decisively above 24-hour high with strong volume surge
    if htf_bullish:
        breakout_up = c_prev["close"] > c_prev["donchian_high"]
        strong_body = c_prev["close"] > c_prev["open"]
        volume_surge = c_prev["volume"] > (c_prev["vol_ma"] * 1.4)

        if breakout_up and strong_body and volume_surge:
            entry = float(c_curr["close"])
            sl = float(entry - (2.0 * atr))
            risk_dist = entry - sl
            tp = entry + (risk_dist * rr_ratio)
            return TradeSignal(
                symbol=symbol,
                side="LONG",
                entry_price=entry,
                stop_loss=sl,
                take_profit=tp,
                reason=f"24H Breakout LONG (ADX {c_prev['adx']:.1f})"
            )

    # BEARISH MOMENTUM BREAKOUT:
    # 1H candle closes decisively below 24-hour low with strong volume surge
    if htf_bearish:
        breakout_down = c_prev["close"] < c_prev["donchian_low"]
        strong_body = c_prev["close"] < c_prev["open"]
        volume_surge = c_prev["volume"] > (c_prev["vol_ma"] * 1.4)

        if breakout_down and strong_body and volume_surge:
            entry = float(c_curr["close"])
            sl = float(entry + (2.0 * atr))
            risk_dist = sl - entry
            tp = entry - (risk_dist * rr_ratio)
            return TradeSignal(
                symbol=symbol,
                side="SHORT",
                entry_price=entry,
                stop_loss=sl,
                take_profit=tp,
                reason=f"24H Breakdown SHORT (ADX {c_prev['adx']:.1f})"
            )

    return None